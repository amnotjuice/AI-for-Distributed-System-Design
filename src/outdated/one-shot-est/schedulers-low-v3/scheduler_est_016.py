# policy_key: scheduler_est_016
# reasoning_effort: low
# model: gpt-5.2-2025-12-11
# llm_cost: 0.039969
# generation_seconds: 36.25
# generated_at: 2026-04-03T01:14:54.189477
@register_scheduler_init(key="scheduler_est_016")
def scheduler_est_016_init(s):
    """Priority-aware, low-risk improvement over naive FIFO.

    Goals:
      - Reduce tail latency for high-priority pipelines by (a) priority queues and
        (b) small resource reservations for high priority when they are waiting.
      - Handle RAM uncertainty cheaply via aggressive allocation from estimate +
        OOM-triggered retry with multiplicative backoff.
      - Keep the policy simple: no preemption; only admission/placement/sizing.

    State:
      - waiting_{priority}: FIFO queues per priority
      - ram_backoff[(pipeline_id, op_key)] = multiplier (>=1.0) applied to RAM ask after OOM
      - rr_cursor_{priority}: round-robin cursor per priority queue to avoid head-of-line blocking
    """
    s.waiting_by_prio = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }
    s.rr_cursor = {
        Priority.QUERY: 0,
        Priority.INTERACTIVE: 0,
        Priority.BATCH_PIPELINE: 0,
    }
    s.ram_backoff = {}  # (pipeline_id, op_key) -> float


@register_scheduler(key="scheduler_est_016")
def scheduler_est_016(s, results, pipelines):
    """
    Scheduler step:
      1) Enqueue new pipelines by priority.
      2) Update RAM backoff for OOM failures (retry later with more RAM).
      3) For each pool, assign runnable operators, prioritizing QUERY > INTERACTIVE > BATCH.
         - If any high-priority work is waiting, reserve a small fraction of pool resources
           from being consumed by lower priority assignments.
         - Allocate CPU: give more to higher priorities but avoid taking the whole pool
           unless it's batch and nothing else is waiting.
         - Allocate RAM: use op.estimate.mem_peak_gb if present; otherwise a small default.
           On OOM, increase backoff multiplier for that op.
    """
    # --- Helpers (kept local to avoid imports at module scope) ---
    def _prio_rank(p):
        if p == Priority.QUERY:
            return 0
        if p == Priority.INTERACTIVE:
            return 1
        return 2

    def _op_key(op):
        # Prefer stable ids if present; otherwise fallback to object identity.
        for attr in ("op_id", "operator_id", "id", "name"):
            if hasattr(op, attr):
                try:
                    v = getattr(op, attr)
                    if callable(v):
                        v = v()
                    if v is not None:
                        return (attr, str(v))
                except Exception:
                    pass
        return ("pyid", str(id(op)))

    def _is_oom_error(err):
        if err is None:
            return False
        msg = str(err).lower()
        return ("oom" in msg) or ("out of memory" in msg) or ("cuda out of memory" in msg)

    def _get_est_mem_gb(op):
        # Estimator may attach op.estimate.mem_peak_gb
        try:
            est = getattr(op, "estimate", None)
            if est is None:
                return None
            v = getattr(est, "mem_peak_gb", None)
            if v is None:
                return None
            v = float(v)
            if v <= 0:
                return None
            return v
        except Exception:
            return None

    def _queue_has_runnable(prio):
        # Conservative check: if any pipeline exists, we treat as potentially runnable.
        # (More precise runnable detection happens when attempting to schedule.)
        return len(s.waiting_by_prio[prio]) > 0

    def _pop_next_pipeline_round_robin(prio):
        q = s.waiting_by_prio[prio]
        if not q:
            return None
        # Round-robin to reduce HOL blocking due to pipelines waiting on dependencies.
        idx = s.rr_cursor[prio] % len(q)
        p = q.pop(idx)
        # Next time start from same idx (since list shrunk) to preserve rotation fairness.
        s.rr_cursor[prio] = idx
        return p

    def _push_pipeline(prio, pipeline):
        s.waiting_by_prio[prio].append(pipeline)

    def _pipeline_done_or_failed(pipeline):
        status = pipeline.runtime_status()
        if status.is_pipeline_successful():
            return True
        # If any operator is FAILED, we still keep it (we want retries). But if the pipeline
        # is irrecoverable, the simulator likely marks it as failed differently; we don't
        # have that signal here, so we only drop on success.
        return False

    def _pick_runnable_ops(pipeline):
        status = pipeline.runtime_status()
        return status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)

    def _cpu_share_for(prio, pool_avail_cpu, high_waiting):
        # Give more CPU to higher priorities, but keep some headroom when high priority is waiting.
        # Values are conservative to avoid starving concurrency.
        if pool_avail_cpu <= 0:
            return 0

        # If any QUERY/INTERACTIVE waiting, cap BATCH CPU grabs to avoid blocking latency.
        if prio == Priority.BATCH_PIPELINE and high_waiting:
            return max(1, int(pool_avail_cpu * 0.50))

        if prio == Priority.QUERY:
            return max(1, int(pool_avail_cpu * 0.80))
        if prio == Priority.INTERACTIVE:
            return max(1, int(pool_avail_cpu * 0.65))
        return max(1, int(pool_avail_cpu * 0.90))

    def _ram_ask_for(pipeline_id, op, pool_avail_ram):
        # Aggressive allocation close to estimate; rely on OOM retries to correct underestimates.
        est = _get_est_mem_gb(op)
        base = 1.0 if est is None else max(0.5, est * 1.05)  # tiny headroom over estimate
        mult = s.ram_backoff.get((pipeline_id, _op_key(op)), 1.0)
        ask = base * mult

        # Never ask for more than currently available; scheduler can't allocate beyond pool headroom.
        # Also avoid asking for ~0 which can cause immediate OOM.
        ask = max(0.25, min(float(pool_avail_ram), float(ask)))
        return ask

    # --- Enqueue new pipelines ---
    for p in pipelines:
        _push_pipeline(p.priority, p)

    # --- Process results: update RAM backoff on OOM failures ---
    if results:
        for r in results:
            try:
                if r.failed() and _is_oom_error(r.error):
                    if r.ops:
                        op = r.ops[0]
                        # pipeline_id might not exist on result; use op's pipeline if present, else None.
                        # In Eudoxia Assignment includes pipeline_id, but ExecutionResult may not.
                        # We conservatively key by ("unknown", op_key) if needed.
                        pid = getattr(r, "pipeline_id", None)
                        if pid is None:
                            pid = getattr(op, "pipeline_id", None)
                        if pid is None:
                            pid = "unknown"
                        k = (pid, _op_key(op))
                        prev = s.ram_backoff.get(k, 1.0)
                        # Multiplicative increase; modest to avoid over-reserving after one noisy OOM.
                        s.ram_backoff[k] = min(16.0, max(prev * 1.6, prev + 0.5))
            except Exception:
                # Never let bookkeeping errors break scheduling.
                pass

    # Early exit if nothing changed and no new pipelines arrived.
    if not pipelines and not results:
        return [], []

    suspensions = []  # no preemption in this version
    assignments = []

    # Detect if high priority waiting (used to reserve some capacity from batch).
    high_waiting = _queue_has_runnable(Priority.QUERY) or _queue_has_runnable(Priority.INTERACTIVE)

    # --- Scheduling loop across pools ---
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = int(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)

        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # If high priority is waiting, reserve a small fixed fraction from batch consumption.
        # (We don't hard reserve from QUERY/INTERACTIVE themselves.)
        reserve_cpu_for_high = int(pool.max_cpu_pool * 0.10) if high_waiting else 0
        reserve_ram_for_high = float(pool.max_ram_pool) * 0.10 if high_waiting else 0.0

        made_progress = True
        # Continue assigning until we can no longer fit anything useful.
        while made_progress:
            made_progress = False

            # Try priorities in order. This is the primary latency improvement.
            for prio in (Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE):
                # For batch, keep reservation if high priority waiting.
                eff_avail_cpu = avail_cpu
                eff_avail_ram = avail_ram
                if prio == Priority.BATCH_PIPELINE and high_waiting:
                    eff_avail_cpu = max(0, avail_cpu - reserve_cpu_for_high)
                    eff_avail_ram = max(0.0, avail_ram - reserve_ram_for_high)

                if eff_avail_cpu <= 0 or eff_avail_ram <= 0:
                    continue

                # Find a pipeline with a runnable op; requeue pipelines that aren't runnable yet.
                # Bound the scan to current queue length to avoid infinite loops.
                qlen = len(s.waiting_by_prio[prio])
                if qlen == 0:
                    continue

                scanned = 0
                selected = None
                selected_ops = None

                while scanned < qlen:
                    p = _pop_next_pipeline_round_robin(prio)
                    scanned += 1
                    if p is None:
                        break

                    # Drop completed pipelines.
                    if _pipeline_done_or_failed(p):
                        continue

                    ops = _pick_runnable_ops(p)
                    if ops:
                        selected = p
                        selected_ops = ops[:1]  # keep atomic step: one operator at a time
                        break

                    # Not runnable yet; requeue.
                    _push_pipeline(prio, p)

                if selected is None or not selected_ops:
                    continue

                op = selected_ops[0]
                cpu = _cpu_share_for(prio, eff_avail_cpu, high_waiting)
                if cpu <= 0:
                    # Put back and move on.
                    _push_pipeline(prio, selected)
                    continue

                ram = _ram_ask_for(selected.pipeline_id, op, eff_avail_ram)
                if ram <= 0:
                    _push_pipeline(prio, selected)
                    continue

                # Ensure we don't exceed current pool availability (hard constraint).
                if cpu > avail_cpu or ram > avail_ram:
                    # Can't fit now; requeue and try other priority/pool work.
                    _push_pipeline(prio, selected)
                    continue

                assignments.append(
                    Assignment(
                        ops=selected_ops,
                        cpu=cpu,
                        ram=ram,
                        priority=selected.priority,
                        pool_id=pool_id,
                        pipeline_id=selected.pipeline_id,
                    )
                )

                # Requeue the pipeline for future operators.
                _push_pipeline(prio, selected)

                # Update available resources for further packing this tick.
                avail_cpu -= int(cpu)
                avail_ram -= float(ram)
                made_progress = True
                break  # re-start priority order with updated availability

    return suspensions, assignments
