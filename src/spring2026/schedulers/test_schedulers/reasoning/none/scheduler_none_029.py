# policy_key: scheduler_none_029
# reasoning_effort: none
# model: gpt-5.2-2025-12-11
# llm_cost: 0.065503
# generation_seconds: 40.05
# generated_at: 2026-04-09T22:07:47.472091
@register_scheduler_init(key="scheduler_none_029")
def scheduler_none_029_init(s):
    """Priority-aware, failure-averse scheduler.

    Core ideas (kept intentionally simple/stable):
      1) Priority queues: always consider QUERY first, then INTERACTIVE, then BATCH.
      2) Conservative sizing to reduce OOM/failures (which are heavily penalized):
         - Track per-operator RAM "floor" from observed failures; next attempt increases RAM.
         - Allocate a modest CPU slice per op to improve concurrency & reduce head-of-line blocking.
      3) Light admission control:
         - Keep a small safety buffer of pool resources (esp. RAM) to absorb bursts and reduce OOM risk.
      4) No aggressive preemption by default (to avoid churn/wasted work). Optionally preempt only
         when high-priority work is waiting and there is no headroom at all.
    """
    # Queues store Pipeline objects; we de-duplicate by pipeline_id.
    s.q_query = []
    s.q_interactive = []
    s.q_batch = []
    s.known_pipeline_ids = set()

    # Per-operator learned minimum RAM to avoid repeated OOM. Keyed by (pipeline_id, op_id).
    s.op_ram_floor = {}  # (pid, op_id) -> ram

    # Backoff counters for repeated failures.
    s.op_fail_count = {}  # (pid, op_id) -> int

    # Basic knobs
    s.ram_growth_factor = 1.6          # multiplicative RAM increase on failure
    s.ram_growth_additive = 0.5        # additive bump (in same units as pool RAM) on failure
    s.max_ram_fraction = 0.95          # don't allocate more than this fraction of pool RAM to one op
    s.cpu_slice_query = 0.5            # fraction of pool CPU offered to one QUERY op
    s.cpu_slice_interactive = 0.5      # fraction for INTERACTIVE
    s.cpu_slice_batch = 0.5            # fraction for BATCH
    s.min_cpu = 1e-6                   # avoid zero CPU assignment if pool has tiny float CPU

    # Safety buffers to keep some headroom; reduces OOM risk & makes room for bursts.
    s.safety_ram_frac = 0.10
    s.safety_cpu_frac = 0.05

    # Optional limited preemption when completely stuck.
    s.enable_preemption = True


def _priority_rank(p):
    # Smaller rank = higher priority
    if p.priority == Priority.QUERY:
        return 0
    if p.priority == Priority.INTERACTIVE:
        return 1
    return 2


def _enqueue_pipeline(s, p):
    if p.pipeline_id in s.known_pipeline_ids:
        return
    s.known_pipeline_ids.add(p.pipeline_id)
    if p.priority == Priority.QUERY:
        s.q_query.append(p)
    elif p.priority == Priority.INTERACTIVE:
        s.q_interactive.append(p)
    else:
        s.q_batch.append(p)


def _iter_queues_in_priority_order(s):
    # Yield (queue_list, priority)
    yield s.q_query, Priority.QUERY
    yield s.q_interactive, Priority.INTERACTIVE
    yield s.q_batch, Priority.BATCH_PIPELINE


def _cleanup_pipeline_if_terminal(s, p):
    status = p.runtime_status()
    if status.is_pipeline_successful():
        # remove bookkeeping to prevent growth
        s.known_pipeline_ids.discard(p.pipeline_id)
        return True
    # If any operator failed, we still want to retry with larger RAM; don't drop pipeline here.
    return False


def _first_ready_op(p):
    status = p.runtime_status()
    # Only schedule ops whose parents are complete, prefer at most one op to limit per-pipeline burstiness.
    op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
    return op_list


def _op_key(pipeline_id, op):
    # op expected to have stable id-like attribute; fall back to str(op)
    op_id = getattr(op, "operator_id", None)
    if op_id is None:
        op_id = getattr(op, "op_id", None)
    if op_id is None:
        op_id = getattr(op, "id", None)
    if op_id is None:
        op_id = str(op)
    return (pipeline_id, op_id)


def _update_from_results(s, results):
    # Learn from failures to reduce future 720s penalties from failing pipelines.
    for r in results:
        if not getattr(r, "ops", None):
            continue
        # We only learn from explicit failures.
        if not r.failed():
            continue

        # Conservative: treat any failure as a signal to increase RAM floor for the involved ops.
        # If error contains "oom", it's likely RAM; otherwise could be other, but we still backoff slightly.
        err = str(getattr(r, "error", "")).lower()
        oom_like = ("oom" in err) or ("out of memory" in err) or ("memory" in err)

        for op in r.ops:
            key = _op_key(getattr(r, "pipeline_id", None) or getattr(getattr(r, "pipeline", None), "pipeline_id", None) or "unknown", op)
            # The simulator's ExecutionResult doesn't always include pipeline_id; if missing, approximate by op identity.
            # Use a stable fallback to still provide some backoff behavior.
            if key[0] == "unknown":
                key = ("unknown", key[1])

            prev_fail = s.op_fail_count.get(key, 0) + 1
            s.op_fail_count[key] = prev_fail

            prev_floor = s.op_ram_floor.get(key, 0.0)
            # Use reported RAM as baseline when available; otherwise bump relative to prev_floor.
            base = float(getattr(r, "ram", 0.0) or prev_floor or 0.0)

            # Backoff schedule: geometric + small additive bump, slightly stronger on OOM-like errors.
            factor = s.ram_growth_factor * (1.1 if oom_like else 1.0)
            bump = s.ram_growth_additive * (1.0 if oom_like else 0.5)
            new_floor = max(prev_floor, base * factor + bump)

            s.op_ram_floor[key] = new_floor


def _pick_cpu_slice(s, priority, pool_cpu):
    if priority == Priority.QUERY:
        frac = s.cpu_slice_query
    elif priority == Priority.INTERACTIVE:
        frac = s.cpu_slice_interactive
    else:
        frac = s.cpu_slice_batch
    cpu = max(s.min_cpu, pool_cpu * frac)
    # Do not exceed available pool cpu
    return min(cpu, pool_cpu)


def _reserve_headroom(s, pool):
    # Keep some safety headroom unallocated to reduce OOM & burst contention.
    safe_cpu = max(0.0, float(pool.max_cpu_pool) * s.safety_cpu_frac)
    safe_ram = max(0.0, float(pool.max_ram_pool) * s.safety_ram_frac)
    return safe_cpu, safe_ram


def _maybe_preempt_for_high_priority(s, pool_id, pool, want_priority):
    # Very conservative preemption: only if absolutely no available headroom beyond safety,
    # and high-priority work exists. Preempt lowest priority running containers first.
    if not s.enable_preemption:
        return []

    # If there is some headroom, don't preempt.
    safe_cpu, safe_ram = _reserve_headroom(s, pool)
    if pool.avail_cpu_pool > safe_cpu and pool.avail_ram_pool > safe_ram:
        return []

    # We need access to running containers; the simulator API isn't specified beyond Suspend.
    # Try best-effort: look for pool.containers if present with fields (priority, container_id, state).
    containers = getattr(pool, "containers", None)
    if not containers:
        return []

    # Determine which priorities can be preempted
    if want_priority == Priority.QUERY:
        preemptable = {Priority.INTERACTIVE, Priority.BATCH_PIPELINE}
    elif want_priority == Priority.INTERACTIVE:
        preemptable = {Priority.BATCH_PIPELINE}
    else:
        return []

    suspensions = []
    # Sort preempt candidates by lowest priority first.
    def prio_value(c):
        pr = getattr(c, "priority", Priority.BATCH_PIPELINE)
        if pr == Priority.BATCH_PIPELINE:
            return 2
        if pr == Priority.INTERACTIVE:
            return 1
        return 0

    # Suspend a small number to reduce churn; 1 at a time.
    cand = [c for c in containers if getattr(c, "priority", None) in preemptable]
    cand.sort(key=prio_value, reverse=True)

    if cand:
        c = cand[0]
        cid = getattr(c, "container_id", None) or getattr(c, "id", None)
        if cid is not None:
            suspensions.append(Suspend(container_id=cid, pool_id=pool_id))
    return suspensions


@register_scheduler(key="scheduler_none_029")
def scheduler_none_029_scheduler(s, results, pipelines):
    """
    Scheduling step:
      - Ingest new pipelines into priority queues.
      - Update RAM backoff floors from failures.
      - For each pool, try to assign ready operators in strict priority order.
      - Assign only one op per scheduling decision per pool per tick to avoid overcommitting.
    """
    for p in pipelines:
        _enqueue_pipeline(s, p)

    # Early exit if nothing changed; keeps simulator fast.
    if not pipelines and not results:
        return [], []

    _update_from_results(s, results)

    suspensions = []
    assignments = []

    # Maintain queues by rotating pipelines: fairness within same priority (round-robin).
    # We'll attempt a bounded scan to find a runnable op.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]

        # Best-effort preemption if a high-priority queue is non-empty and pool is completely congested.
        if s.q_query:
            suspensions.extend(_maybe_preempt_for_high_priority(s, pool_id, pool, Priority.QUERY))
        elif s.q_interactive:
            suspensions.extend(_maybe_preempt_for_high_priority(s, pool_id, pool, Priority.INTERACTIVE))

        # Recompute available after potential preemption (sim may apply next tick; still proceed conservatively).
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)

        safe_cpu, safe_ram = _reserve_headroom(s, pool)
        effective_cpu = max(0.0, avail_cpu - safe_cpu)
        effective_ram = max(0.0, avail_ram - safe_ram)

        if effective_cpu <= 0.0 or effective_ram <= 0.0:
            continue

        assigned_this_pool = False

        for q, pr in _iter_queues_in_priority_order(s):
            if assigned_this_pool:
                break
            if not q:
                continue

            # Scan a few pipelines to find a runnable op; rotate to preserve fairness.
            scan = min(len(q), 8)
            for _ in range(scan):
                p = q.pop(0)

                # Drop completed pipelines from queues.
                if _cleanup_pipeline_if_terminal(s, p):
                    continue

                op_list = _first_ready_op(p)
                if not op_list:
                    # Not ready now; push back.
                    q.append(p)
                    continue

                op = op_list[0]
                key = _op_key(p.pipeline_id, op)

                # Choose RAM: learned floor if available; otherwise allocate a conservative share.
                learned_floor = float(s.op_ram_floor.get(key, 0.0))
                # Default RAM: enough to avoid obvious OOMs; pick min(half pool, effective_ram)
                default_ram = min(effective_ram, float(pool.max_ram_pool) * 0.50)
                ram = max(learned_floor, default_ram)
                ram = min(ram, effective_ram, float(pool.max_ram_pool) * s.max_ram_fraction)

                # If even the minimum attempt cannot fit, requeue and try lower priorities? No—better to
                # keep strict priority; but rotate to avoid starvation within same class.
                if ram <= 0.0:
                    q.append(p)
                    continue

                cpu = _pick_cpu_slice(s, pr, effective_cpu)

                if cpu <= 0.0:
                    q.append(p)
                    continue

                # If resources insufficient for this attempt, requeue and try next pipeline (same priority).
                if cpu > effective_cpu + 1e-12 or ram > effective_ram + 1e-12:
                    q.append(p)
                    continue

                assignments.append(
                    Assignment(
                        ops=op_list,
                        cpu=cpu,
                        ram=ram,
                        priority=p.priority,
                        pool_id=pool_id,
                        pipeline_id=p.pipeline_id,
                    )
                )

                # Put pipeline back to end for RR within its priority.
                q.append(p)

                assigned_this_pool = True
                break

    return suspensions, assignments
