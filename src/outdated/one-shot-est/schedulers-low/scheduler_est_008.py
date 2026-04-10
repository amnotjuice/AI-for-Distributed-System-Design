# policy_key: scheduler_est_008
# reasoning_effort: low
# model: gpt-5.2-2025-12-11
# llm_cost: 0.041278
# generation_seconds: 38.40
# generated_at: 2026-03-31T23:50:18.810422
@register_scheduler_init(key="scheduler_est_008")
def scheduler_est_008_init(s):
    """
    Priority-aware FIFO with simple RAM-flooring from estimates and conservative batch throttling.

    Incremental improvements over naive FIFO:
      1) Maintain per-priority queues so QUERY/INTERACTIVE run first.
      2) Allocate RAM using op.estimate.mem_peak_gb as a floor (with small safety multiplier).
      3) Avoid letting BATCH consume the entire pool when higher-priority work is waiting
         (simple headroom reservation; no preemption required).
      4) On apparent OOM failures, increase the future RAM floor for the specific operator.
    """
    s.waiting = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }
    # Per-operator RAM inflation after OOM-like failures: (pipeline_id, op_key) -> multiplier
    s.oom_ram_mult = {}
    # Per-pipeline FIFO order tracking (helps avoid always picking same pipeline when many exist)
    s.round_robin_cursor = {
        Priority.QUERY: 0,
        Priority.INTERACTIVE: 0,
        Priority.BATCH_PIPELINE: 0,
    }


@register_scheduler(key="scheduler_est_008")
def scheduler_est_008(s, results: List["ExecutionResult"], pipelines: List["Pipeline"]) -> Tuple[List["Suspend"], List["Assignment"]]:
    """
    Scheduler tick:
      - Ingest new pipelines into per-priority queues.
      - Learn from failures (OOM => inflate RAM next attempt).
      - For each pool, schedule as many ready operators as resources permit:
          * Choose next runnable op from highest priority queue first.
          * For BATCH, cap usable resources if higher-priority work is waiting.
          * Allocate CPU with a small per-op cap to improve latency and avoid monopolization.
          * Allocate RAM >= estimated peak * safety * learned multiplier (when possible).
    """
    # --- Helpers (kept inside to avoid imports at module scope) ---
    def _prio_order():
        # Highest to lowest
        return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]

    def _has_high_prio_waiting():
        return bool(s.waiting[Priority.QUERY] or s.waiting[Priority.INTERACTIVE])

    def _op_key(pipeline, op):
        # Try a stable identifier if present; fall back to id(op).
        # (Within the simulator, operator objects are typically stable across retries.)
        return (pipeline.pipeline_id, getattr(op, "op_id", id(op)))

    def _looks_like_oom(err):
        if not err:
            return False
        try:
            msg = str(err).lower()
        except Exception:
            return False
        return ("oom" in msg) or ("out of memory" in msg) or ("memory" in msg and "exceed" in msg)

    def _enqueue_pipeline(p):
        # Avoid duplicates by only enqueuing if not already present in any queue.
        for q in s.waiting.values():
            if p in q:
                return
        s.waiting[p.priority].append(p)

    def _drop_if_done_or_failed(p):
        st = p.runtime_status()
        # If any operator is FAILED we treat the pipeline as failed terminally
        # (baseline behavior), unless the simulator expects retries; we keep it conservative.
        has_failures = st.state_counts[OperatorState.FAILED] > 0
        if st.is_pipeline_successful() or has_failures:
            return True
        return False

    def _next_runnable_from_queue(priority):
        """
        Round-robin scan within a priority queue to find a pipeline with a runnable operator.
        Returns (pipeline, [op]) or (None, []).
        """
        q = s.waiting[priority]
        if not q:
            return None, []

        n = len(q)
        if n == 0:
            return None, []

        start = s.round_robin_cursor[priority] % n
        for i in range(n):
            idx = (start + i) % n
            p = q[idx]

            if _drop_if_done_or_failed(p):
                continue

            st = p.runtime_status()
            ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
            if ops:
                # Move cursor forward so the next search starts after this pipeline
                s.round_robin_cursor[priority] = idx + 1
                return p, ops

        return None, []

    def _remove_pipeline_once(p):
        q = s.waiting[p.priority]
        # Remove a single occurrence if present
        try:
            q.remove(p)
        except ValueError:
            pass

    def _requeue_pipeline(p):
        # Put it at the end of its priority queue
        _remove_pipeline_once(p)
        s.waiting[p.priority].append(p)

    def _ram_floor_gb(pipeline, op, pool_avail_ram):
        """
        Determine RAM allocation, using estimate as a floor and inflating on OOM history.
        Extra RAM is considered "free" (no perf cost), so we allocate as much as reasonable,
        but never exceed pool availability.
        """
        est = None
        try:
            est = getattr(getattr(op, "estimate", None), "mem_peak_gb", None)
        except Exception:
            est = None

        # Base floor: if estimate missing, use a small conservative guess to reduce immediate OOMs.
        base_floor = float(est) if (est is not None) else 0.5

        # Safety factor and OOM multiplier
        safety = 1.20
        mult = s.oom_ram_mult.get(_op_key(pipeline, op), 1.0)

        floor = base_floor * safety * mult

        # If floor is tiny (e.g., 0), make it at least 0.25GB to avoid zero-ram requests.
        floor = max(floor, 0.25)

        # Allocate: prefer to give more RAM (up to what's available), but at least floor if possible.
        if pool_avail_ram >= floor:
            return pool_avail_ram
        else:
            # Not enough to satisfy floor; still allocate what's available (may OOM, but avoids deadlock).
            return max(0.0, pool_avail_ram)

    def _cpu_alloc(priority, pool, avail_cpu):
        """
        CPU allocation tuned for latency:
          - For QUERY/INTERACTIVE, give a larger share (up to pool max).
          - For BATCH, cap per-op CPU so it doesn't monopolize.
        """
        # Minimum CPU slice; if the simulator allows fractional, keep float.
        min_cpu = 1.0

        # Per-op caps by class (fraction of pool max)
        if priority == Priority.QUERY:
            cap_frac = 1.00
        elif priority == Priority.INTERACTIVE:
            cap_frac = 0.75
        else:
            cap_frac = 0.50

        cap = max(min_cpu, float(pool.max_cpu_pool) * cap_frac)
        return max(0.0, min(avail_cpu, cap))

    # --- Ingest new pipelines ---
    for p in pipelines:
        _enqueue_pipeline(p)

    # --- Learn from results (simple OOM inflation) ---
    if results:
        for r in results:
            if hasattr(r, "failed") and r.failed() and _looks_like_oom(getattr(r, "error", None)):
                for op in getattr(r, "ops", []) or []:
                    # We don't have pipeline_id on ExecutionResult in the provided interface,
                    # so we cannot reliably map back to pipeline_id here. Instead, inflate by operator object id.
                    # This still helps because the operator objects are typically stable within a pipeline.
                    key = ("_unknown_pipeline", getattr(op, "op_id", id(op)))
                    prev = s.oom_ram_mult.get(key, 1.0)
                    s.oom_ram_mult[key] = min(prev * 1.5, 16.0)

    # Early exit if nothing new to do
    if not pipelines and not results:
        return [], []

    suspensions: List["Suspend"] = []
    assignments: List["Assignment"] = []

    # --- Schedule per pool ---
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)
        if avail_cpu <= 0.0 or avail_ram <= 0.0:
            continue

        # If higher-priority work exists, throttle batch by reserving headroom.
        high_waiting = _has_high_prio_waiting()
        if high_waiting:
            # Keep a small fraction for QUERY/INTERACTIVE arrivals within this tick.
            reserve_cpu = max(1.0, float(pool.max_cpu_pool) * 0.25)
            reserve_ram = max(0.5, float(pool.max_ram_pool) * 0.25)
        else:
            reserve_cpu = 0.0
            reserve_ram = 0.0

        # Keep scheduling until resources exhausted or no runnable work
        made_progress = True
        while made_progress:
            made_progress = False

            # Try priorities in order; for batch, respect reservation if high-priority is waiting
            for prio in _prio_order():
                # Apply batch throttling when high-priority exists
                eff_avail_cpu = avail_cpu
                eff_avail_ram = avail_ram
                if prio == Priority.BATCH_PIPELINE and high_waiting:
                    eff_avail_cpu = max(0.0, avail_cpu - reserve_cpu)
                    eff_avail_ram = max(0.0, avail_ram - reserve_ram)

                if eff_avail_cpu <= 0.0 or eff_avail_ram <= 0.0:
                    continue

                pipeline, ops = _next_runnable_from_queue(prio)
                if not pipeline or not ops:
                    continue

                op = ops[0]

                # RAM: use estimate as floor, allocate generously (up to remaining effective RAM)
                ram = _ram_floor_gb(pipeline, op, eff_avail_ram)

                # CPU: cap per op for fairness/latency
                cpu = _cpu_alloc(prio, pool, eff_avail_cpu)

                # If we can't allocate meaningful CPU/RAM, skip
                if cpu <= 0.0 or ram <= 0.0:
                    continue

                assignments.append(
                    Assignment(
                        ops=ops,
                        cpu=cpu,
                        ram=ram,
                        priority=pipeline.priority,
                        pool_id=pool_id,
                        pipeline_id=pipeline.pipeline_id,
                    )
                )

                # Update local available resources
                avail_cpu -= cpu
                avail_ram -= ram

                # Rotate the pipeline to the back of its queue (simple fairness within priority)
                _requeue_pipeline(pipeline)

                made_progress = True
                break  # re-evaluate from highest priority with updated resources

            # Stop if pool is effectively full
            if avail_cpu <= 0.0 or avail_ram <= 0.0:
                break

    return suspensions, assignments
