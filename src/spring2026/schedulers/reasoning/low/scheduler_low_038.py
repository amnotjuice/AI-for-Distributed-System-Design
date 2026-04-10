# policy_key: scheduler_low_038
# reasoning_effort: low
# model: gpt-5.2-2025-12-11
# llm_cost: 0.047527
# generation_seconds: 47.69
# generated_at: 2026-04-09T21:32:58.360370
@register_scheduler_init(key="scheduler_low_038")
def scheduler_low_038_init(s):
    """
    Priority-weighted, failure-averse scheduler.

    Core ideas:
      1) Weighted fairness across priorities using a simple deficit-round-robin:
         QUERY > INTERACTIVE > BATCH, but BATCH always accumulates credit to ensure progress.
      2) Failure-averse RAM sizing: start with conservative RAM and exponentially back off on failure
         for the specific (pipeline, op) to reduce repeated OOM-like failures (which are very costly).
      3) Pool-aware placement: prefer a "high-priority" pool (pool 0) for QUERY/INTERACTIVE when
         available, and prefer other pools for BATCH; fall back to best-fit by available resources.
      4) Single-op assignments to improve responsiveness and reduce head-of-line blocking.
    """
    s.waiting = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }
    # Per (pipeline_id, op_key): {"ram": last_ram, "cpu": last_cpu, "fails": int}
    s.op_hints = {}
    # Deficit round-robin credits
    s.credits = {
        Priority.QUERY: 0,
        Priority.INTERACTIVE: 0,
        Priority.BATCH_PIPELINE: 0,
    }
    s.weights = {
        Priority.QUERY: 10,
        Priority.INTERACTIVE: 5,
        Priority.BATCH_PIPELINE: 1,
    }
    s.tick = 0


@register_scheduler(key="scheduler_low_038")
def scheduler_low_038(s, results, pipelines):
    """
    Scheduler step.

    Returns:
      (suspensions, assignments)

    Notes:
      - We do not actively preempt because the minimal public interface does not expose
        a reliable list of running containers to suspend. Instead, we focus on (a) correct
        admission and (b) avoiding repeated failures via RAM backoff.
      - "Failure" is treated as a signal to increase RAM for the same op on retry. This
        aggressively reduces repeated penalties (720s for failed/incomplete pipelines).
    """
    s.tick += 1

    def _prio(p):
        return p.priority

    def _op_key(op):
        # Try common identifiers, otherwise fall back to a stable-ish string
        for attr in ("operator_id", "op_id", "id", "name"):
            if hasattr(op, attr):
                return str(getattr(op, attr))
        return str(op)

    def _pipeline_done_or_failed(p):
        status = p.runtime_status()
        if status.is_pipeline_successful():
            return True
        # If any operators failed, the pipeline might still be retryable in the simulator,
        # but the example baseline drops pipelines with failures. We do NOT drop here;
        # we allow retries (via ASSIGNABLE_STATES which includes FAILED) with larger RAM.
        return False

    def _next_assignable_op(p):
        status = p.runtime_status()
        op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
        return op_list[0] if op_list else None

    def _normalize_priority(pri):
        # Defensive: some traces might use equivalent enums/strings; handle known values only.
        if pri in (Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE):
            return pri
        return Priority.BATCH_PIPELINE

    def _add_pipeline(p):
        pri = _normalize_priority(p.priority)
        s.waiting[pri].append(p)

    def _record_result(res):
        # Update hints only on failures: treat as likely OOM/underprovision, bump RAM.
        if not res.failed():
            return
        # We may see failures for many reasons; backing off RAM is still a safe move given
        # the high penalty for failures/incompletes in the objective.
        for op in getattr(res, "ops", []) or []:
            key = (getattr(res, "pipeline_id", None), _op_key(op))
            # If pipeline_id isn't present on result, fall back to op-only key bucket
            if key[0] is None:
                key = ("_unknown_pipeline_", _op_key(op))
            hint = s.op_hints.get(key, {"ram": max(1.0, float(getattr(res, "ram", 0) or 0)), "cpu": float(getattr(res, "cpu", 0) or 0), "fails": 0})
            hint["fails"] = int(hint.get("fails", 0)) + 1
            last_ram = float(hint.get("ram", 0) or 0)
            # Exponential backoff (cap later by pool max)
            hint["ram"] = max(last_ram, max(1.0, float(getattr(res, "ram", 0) or last_ram))) * (1.7 if hint["fails"] <= 2 else 1.35)
            # Keep cpu hint as-is (we prioritize fixing OOM-like failures first)
            s.op_hints[key] = hint

    def _pool_preference_order(priority):
        # Prefer pool 0 for high priority; prefer non-zero pools for batch when available.
        n = s.executor.num_pools
        if n <= 1:
            return [0]
        if priority in (Priority.QUERY, Priority.INTERACTIVE):
            return [0] + [i for i in range(1, n)]
        return [i for i in range(1, n)] + [0]

    def _pick_pool_for(priority, ram_need, cpu_need):
        # Choose first pool (by preference) that can fit. Otherwise choose the pool with
        # the most available RAM (OOM avoidance) and then CPU.
        best_fit = None
        best_score = None
        for pid in _pool_preference_order(priority):
            pool = s.executor.pools[pid]
            if pool.avail_cpu_pool >= cpu_need and pool.avail_ram_pool >= ram_need:
                return pid
            # Track fallback
            score = (pool.avail_ram_pool, pool.avail_cpu_pool)
            if best_score is None or score > best_score:
                best_score = score
                best_fit = pid
        return best_fit

    def _target_cpu(pool, priority):
        # CPU policy: give more CPU to queries/interactive to reduce latency,
        # but avoid letting batch consume the entire pool.
        avail = float(pool.avail_cpu_pool)
        maxc = float(pool.max_cpu_pool)
        if avail <= 0:
            return 0.0

        if priority == Priority.QUERY:
            # Burst hard for query latency; still keep at least 1 CPU granularity.
            return max(1.0, min(avail, maxc))
        if priority == Priority.INTERACTIVE:
            # Strong burst, but slightly gentler to allow queries to co-exist.
            return max(1.0, min(avail, maxc * 0.75))
        # Batch: throttle to preserve headroom; allow burst only if pool is mostly idle.
        if avail >= maxc * 0.80:
            return max(1.0, min(avail, maxc * 0.60))
        return max(1.0, min(avail, maxc * 0.35))

    def _target_ram(pool, priority, op_hint_ram=None):
        # RAM policy: prioritize completion (avoid OOM) over tight packing.
        # Start conservative-high for QUERY/INTERACTIVE; batch is moderate but safe.
        avail = float(pool.avail_ram_pool)
        maxr = float(pool.max_ram_pool)
        if avail <= 0:
            return 0.0

        base_frac = 0.0
        if priority == Priority.QUERY:
            base_frac = 0.70
        elif priority == Priority.INTERACTIVE:
            base_frac = 0.60
        else:
            base_frac = 0.50

        base = min(avail, maxr * base_frac)

        if op_hint_ram is None or op_hint_ram <= 0:
            return max(1.0, base)

        # If we have a hint (after failures), honor it as much as possible.
        return max(1.0, min(avail, min(maxr, float(op_hint_ram))))

    def _refill_credits():
        # DRR: accumulate credits each tick; cap to avoid runaway.
        for pri, w in s.weights.items():
            s.credits[pri] = min(50, int(s.credits.get(pri, 0)) + int(w))

    def _choose_next_priority():
        # Pick the highest "value" class with credit and queued pipelines.
        # Tie-break: higher weight first to protect dominant score contributors.
        candidates = []
        for pri in (Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE):
            if s.waiting[pri] and s.credits.get(pri, 0) > 0:
                candidates.append(pri)
        if not candidates:
            return None
        candidates.sort(key=lambda x: (s.weights[x], s.credits.get(x, 0)), reverse=True)
        return candidates[0]

    def _rotate_queue(pri):
        # FIFO rotation (keep ordering stable, but allow skipping blocked pipelines)
        q = s.waiting[pri]
        if q:
            q.append(q.pop(0))

    # Ingest new pipelines
    for p in pipelines:
        _add_pipeline(p)

    # Update hints based on results
    for r in results:
        _record_result(r)

    # Early exit only if nothing to do
    if not pipelines and not results and all(len(q) == 0 for q in s.waiting.values()):
        return [], []

    suspensions = []
    assignments = []

    # Add credits each tick to ensure even batch eventually runs
    _refill_credits()

    # Attempt to fill each pool with at most one new assignment per tick to keep the system responsive.
    # (This avoids one class flooding all pools in a single step.)
    for _ in range(s.executor.num_pools):
        pri = _choose_next_priority()
        if pri is None:
            break

        # Find a runnable pipeline within this priority class (skip completed)
        chosen_pipeline = None
        chosen_op = None

        # Scan up to the queue length; rotate to avoid permanent skipping
        qlen = len(s.waiting[pri])
        for _i in range(qlen):
            p = s.waiting[pri][0]
            _rotate_queue(pri)

            if _pipeline_done_or_failed(p):
                # Remove completed pipelines from waiting
                # (We don't drop failed; failures can become assignable again via FAILED state)
                if p.runtime_status().is_pipeline_successful():
                    # Remove all instances (should be one)
                    try:
                        s.waiting[pri].remove(p)
                    except Exception:
                        pass
                continue

            op = _next_assignable_op(p)
            if op is None:
                continue

            chosen_pipeline = p
            chosen_op = op
            break

        if chosen_pipeline is None:
            # No runnable pipelines in this class; drain its credit to move on
            s.credits[pri] = 0
            continue

        # Derive op-specific hints
        pid = chosen_pipeline.pipeline_id
        ok = _op_key(chosen_op)
        hint = s.op_hints.get((pid, ok), None)
        hint_ram = float(hint["ram"]) if hint and "ram" in hint else None

        # Pick a pool that can fit; compute needs per-pool (cpu/ram depend on pool)
        best_pid = None
        best_cpu = 0.0
        best_ram = 0.0

        # We pick the first pool that can accommodate our "desired" cpu/ram; otherwise fallback.
        # To reduce failure risk, RAM is the primary fit dimension.
        for pid_try in _pool_preference_order(pri):
            pool = s.executor.pools[pid_try]
            cpu_need = _target_cpu(pool, pri)
            ram_need = _target_ram(pool, pri, hint_ram)

            if cpu_need <= 0 or ram_need <= 0:
                continue

            if pool.avail_cpu_pool >= cpu_need and pool.avail_ram_pool >= ram_need:
                best_pid, best_cpu, best_ram = pid_try, cpu_need, ram_need
                break

        if best_pid is None:
            # Fallback: pick the pool with most RAM headroom, and size within its avail.
            pid_try = _pick_pool_for(pri, 1.0, 1.0)
            if pid_try is None:
                break
            pool = s.executor.pools[pid_try]
            cpu_need = max(1.0, min(float(pool.avail_cpu_pool), _target_cpu(pool, pri)))
            ram_need = max(1.0, min(float(pool.avail_ram_pool), _target_ram(pool, pri, hint_ram)))
            if pool.avail_cpu_pool < 1.0 or pool.avail_ram_pool < 1.0:
                break
            best_pid, best_cpu, best_ram = pid_try, cpu_need, ram_need

        # Final guard
        pool = s.executor.pools[best_pid]
        if pool.avail_cpu_pool < best_cpu or pool.avail_ram_pool < best_ram:
            # Can't place; reduce credit a bit to avoid spinning.
            s.credits[pri] = max(0, int(s.credits.get(pri, 0)) - 1)
            continue

        assignments.append(
            Assignment(
                ops=[chosen_op],
                cpu=best_cpu,
                ram=best_ram,
                priority=chosen_pipeline.priority,
                pool_id=best_pid,
                pipeline_id=chosen_pipeline.pipeline_id,
            )
        )

        # Spend credit proportional to weight: higher priority spends more per dispatch,
        # but still retains preference due to higher refill weight.
        s.credits[pri] = max(0, int(s.credits.get(pri, 0)) - int(max(1, s.weights[pri] // 2)))

    return suspensions, assignments
