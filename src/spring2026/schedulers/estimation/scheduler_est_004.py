# policy_key: scheduler_est_004
# reasoning_effort: low
# model: gpt-5.2-2025-12-11
# llm_cost: 0.037974
# generation_seconds: 35.22
# generated_at: 2026-03-31T23:47:35.993902
@register_scheduler_init(key="scheduler_est_004")
def scheduler_est_004_init(s):
    """
    Priority-aware FIFO with basic right-sizing and OOM backoff.

    Improvements over naive FIFO:
    - Priority queues: always try to schedule higher priority work first.
    - RAM floor: allocate at least the operator memory estimate (if present) to avoid avoidable OOMs.
    - OOM backoff: if an op fails, increase its RAM floor and retry (bounded).
    - Simple fairness: aging promotion for long-waiting batch pipelines.
    - Avoid "one op eats the pool": cap per-op CPU to leave headroom for additional low-cost latency wins.
    """
    # Separate queues to reduce head-of-line blocking from long batch pipelines.
    s.q_hi = []   # Priority.QUERY / Priority.INTERACTIVE
    s.q_lo = []   # Priority.BATCH_PIPELINE (and anything else)
    s.tick = 0

    # Track enqueue tick for simple aging fairness.
    s.enqueued_at = {}  # pipeline_id -> tick

    # Track per-operator RAM floors; key by (pipeline_id, op_object_id).
    s.op_ram_floor_gb = {}

    # Track retry counts to prevent infinite OOM retry loops.
    s.op_retries = {}  # (pipeline_id, op_object_id) -> int

    # Small constants; tuned to be safe/simple.
    s.max_retries = 3
    s.oom_growth = 1.6  # multiply RAM floor after failure
    s.min_ram_gb = 0.25  # avoid allocating tiny fractions if estimates are missing
    s.aging_ticks = 50  # after this many ticks, let batch get scheduled even if hi keeps arriving


@register_scheduler(key="scheduler_est_004")
def scheduler_est_004_scheduler(s, results, pipelines):
    """
    Scheduling loop:
    1) Ingest new pipelines into priority queues.
    2) Use execution results to adjust RAM floors on failures.
    3) For each pool, repeatedly schedule ready ops:
       - Prefer hi priority, but allow aging-promoted batch.
       - Allocate RAM = max(estimate, learned floor), capped by pool availability.
       - Allocate CPU with a priority-based cap (to reduce tail latency by enabling concurrency).
    """
    # ---- helpers (defined inside to keep file self-contained) ----
    def _is_hi_priority(pri):
        return pri in (Priority.QUERY, Priority.INTERACTIVE)

    def _queue_for(pri):
        return s.q_hi if _is_hi_priority(pri) else s.q_lo

    def _purge_terminal(p):
        st = p.runtime_status()
        if st.is_pipeline_successful():
            return True
        # If any operator failed and we have no retry plan, we may drop later; for now keep pipeline.
        return False

    def _first_ready_op(p):
        st = p.runtime_status()
        # Only consider ops whose parents are complete.
        ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        if not ops:
            return None
        return ops[0]

    def _op_key(pipeline, op):
        return (pipeline.pipeline_id, id(op))

    def _base_estimate_ram(op):
        # Use estimate as a RAM floor when available.
        est = None
        try:
            est = getattr(getattr(op, "estimate", None), "mem_peak_gb", None)
        except Exception:
            est = None
        if est is None:
            return s.min_ram_gb
        try:
            est_f = float(est)
        except Exception:
            return s.min_ram_gb
        return max(est_f, s.min_ram_gb)

    def _desired_ram(pipeline, op, avail_ram):
        key = _op_key(pipeline, op)
        learned_floor = s.op_ram_floor_gb.get(key, 0.0)
        est_floor = _base_estimate_ram(op)
        floor = max(learned_floor, est_floor)

        # "Extra RAM is free", but pool RAM is finite; allocate as much as possible up to the pool's available RAM.
        # Keep a tiny reserve to avoid packing fragmentation across multiple assignments.
        reserve = 0.0
        alloc = min(avail_ram - reserve, max(floor, s.min_ram_gb))
        if alloc <= 0:
            return 0.0
        return alloc

    def _cpu_cap_for_priority(pool, pri):
        # Cap per-op CPU to enable concurrency and reduce latency interference.
        # QUERY/INTERACTIVE get more CPU; batch gets less to avoid starving hi-pri tail.
        if pri == Priority.QUERY:
            frac = 0.75
        elif pri == Priority.INTERACTIVE:
            frac = 0.60
        else:
            frac = 0.35
        # At least 1 vCPU if available.
        return max(1.0, pool.max_cpu_pool * frac)

    def _desired_cpu(pool, pri, avail_cpu):
        cap = _cpu_cap_for_priority(pool, pri)
        return max(0.0, min(avail_cpu, cap))

    def _pick_next_pipeline():
        # Aging: if batch has waited long enough, allow it to run even if hi queue is non-empty.
        if s.q_lo:
            # Find oldest batch pipeline (by enqueue time).
            oldest_lo = None
            oldest_tick = None
            for p in s.q_lo:
                t = s.enqueued_at.get(p.pipeline_id, s.tick)
                if oldest_tick is None or t < oldest_tick:
                    oldest_tick = t
                    oldest_lo = p
            if oldest_lo is not None and (s.tick - oldest_tick) >= s.aging_ticks:
                # Pop that specific pipeline from lo queue.
                idx = s.q_lo.index(oldest_lo)
                return s.q_lo.pop(idx)

        # Default: strict priority.
        if s.q_hi:
            return s.q_hi.pop(0)
        if s.q_lo:
            return s.q_lo.pop(0)
        return None

    def _requeue_pipeline(p):
        q = _queue_for(p.priority)
        q.append(p)

    # ---- ingest new pipelines ----
    for p in pipelines:
        if p.pipeline_id not in s.enqueued_at:
            s.enqueued_at[p.pipeline_id] = s.tick
        _queue_for(p.priority).append(p)

    # ---- process results (learn from failures) ----
    # We avoid sophisticated parsing and assume any failure means "try more RAM next time" up to a limit.
    for r in results:
        if not getattr(r, "failed", lambda: False)():
            continue
        # Increase RAM floors for all ops in the failed assignment.
        try:
            ops = list(getattr(r, "ops", []) or [])
        except Exception:
            ops = []

        for op in ops:
            # We may not have pipeline_id here; best effort via r.ops objects:
            # use a pseudo key with pipeline_id unavailable; fall back to op id only.
            # However ExecutionResult exposes container_id/pool_id, not pipeline_id. We'll attempt to use op.pipeline_id if present.
            pid = getattr(op, "pipeline_id", None)
            if pid is None:
                # Can't safely learn without pipeline_id; skip.
                continue
            key = (pid, id(op))
            prev = s.op_ram_floor_gb.get(key, 0.0)
            # If we know what RAM was allocated, use it as a baseline; otherwise rely on estimate.
            base = 0.0
            try:
                base = float(getattr(r, "ram", 0.0) or 0.0)
            except Exception:
                base = 0.0
            new_floor = max(prev, base) * s.oom_growth if max(prev, base) > 0 else prev + s.min_ram_gb
            s.op_ram_floor_gb[key] = max(new_floor, prev, s.min_ram_gb)
            s.op_retries[key] = int(s.op_retries.get(key, 0)) + 1

    # ---- early exit if nothing to do ----
    if not pipelines and not results and not s.q_hi and not s.q_lo:
        s.tick += 1
        return [], []

    suspensions = []
    assignments = []

    # ---- schedule per pool ----
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool

        # Small guard.
        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # Try to fill the pool with multiple assignments if possible.
        # We keep a small local buffer of pipelines popped but not schedulable, to requeue after.
        local_defer = []

        # Hard cap iterations to avoid infinite loops on unschedulable queues.
        for _ in range(256):
            if avail_cpu <= 0 or avail_ram <= 0:
                break

            p = _pick_next_pipeline()
            if p is None:
                break

            # Drop completed pipelines.
            if _purge_terminal(p):
                continue

            op = _first_ready_op(p)
            if op is None:
                # Not ready yet; keep it around.
                local_defer.append(p)
                continue

            # Retry bounding: if we've retried too much, stop scheduling this pipeline (treat as terminal).
            k = _op_key(p, op)
            if int(s.op_retries.get(k, 0)) > s.max_retries:
                # Do not requeue; avoid infinite thrash.
                continue

            # Compute resource request.
            ram = _desired_ram(p, op, avail_ram)
            cpu = _desired_cpu(pool, p.priority, avail_cpu)

            # If we can't fit minimum RAM or any CPU, defer.
            if ram <= 0 or cpu <= 0:
                local_defer.append(p)
                break

            # Ensure RAM at least the floor; if floor itself doesn't fit, defer.
            floor = max(s.op_ram_floor_gb.get(k, 0.0), _base_estimate_ram(op))
            if floor > avail_ram:
                local_defer.append(p)
                # If hi-priority can't fit, no point continuing in this pool.
                # (We avoid preemption in this version.)
                if _is_hi_priority(p.priority):
                    break
                continue

            # Assign single op per container for atomicity and simplicity.
            assignment = Assignment(
                ops=[op],
                cpu=cpu,
                ram=max(ram, floor),
                priority=p.priority,
                pool_id=pool_id,
                pipeline_id=p.pipeline_id,
            )
            assignments.append(assignment)

            # Update available resources for further packing.
            avail_cpu -= cpu
            avail_ram -= max(ram, floor)

            # Requeue the pipeline for future ops.
            local_defer.append(p)

        # Put deferred pipelines back into their queues, preserving approximate FIFO within their priority class.
        for p in local_defer:
            _requeue_pipeline(p)

    s.tick += 1
    return suspensions, assignments
