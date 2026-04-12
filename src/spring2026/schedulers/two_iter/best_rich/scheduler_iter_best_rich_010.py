# policy_key: scheduler_iter_best_rich_010
# context: rich
# model: gpt-5.2-2025-12-11
# llm_cost: 0.052053
# generation_seconds: 46.06
# generated_at: 2026-04-12T00:26:49.220167
@register_scheduler_init(key="scheduler_iter_best_rich_010")
def scheduler_iter_best_rich_010_init(s):
    """Priority-aware scheduler focused on improving weighted latency with small, robust steps.

    Improvements over naive FIFO / prior iteration:
      - Priority queues (QUERY > INTERACTIVE > BATCH) with round-robin fairness within each class.
      - Pool affinity: if multiple pools exist, reserve pool 0 primarily for QUERY/INTERACTIVE to protect latency;
        batch prefers non-zero pools but can opportunistically use pool 0 when high-priority backlog is empty.
      - Adaptive per-operator sizing:
          * Start with small RAM allocations to avoid chronic over-allocation (which reduces concurrency).
          * On OOM: increase RAM estimate aggressively for that operator.
          * On timeout: increase CPU estimate moderately for that operator.
          * On success: slowly decay RAM/CPU estimates to reclaim resources.
      - Headroom reservation: when high-priority backlog exists, avoid consuming the last slice of CPU/RAM with batch.

    Intentionally not included yet:
      - Preemption/suspensions (insufficient guaranteed visibility into running containers in the provided interface).
    """
    # Per-priority FIFO with rotation.
    s.q_query = []
    s.q_interactive = []
    s.q_batch = []

    # Per-operator resource estimates keyed by (pipeline_id, op_id-ish).
    s.op_ram_est = {}  # bytes/units consistent with pool.ram
    s.op_cpu_est = {}  # vCPU

    # Track pipelines we should stop trying (non-retryable failures).
    s.dead_pipelines = set()

    def _op_key(op, pipeline_id):
        oid = getattr(op, "op_id", None)
        if oid is None:
            oid = getattr(op, "operator_id", None)
        if oid is None:
            oid = getattr(op, "node_id", None)
        if oid is None:
            oid = id(op)
        return (pipeline_id, oid)

    s._op_key = _op_key


@register_scheduler(key="scheduler_iter_best_rich_010")
def scheduler_iter_best_rich_010_scheduler(s, results, pipelines):
    """Adaptive priority scheduler (latency-focused) with OOM/timeout-aware resizing."""

    def _is_oom(err) -> bool:
        if err is None:
            return False
        msg = str(err).lower()
        return ("oom" in msg) or ("out of memory" in msg) or ("killed" in msg and "memory" in msg)

    def _is_timeout(err) -> bool:
        if err is None:
            return False
        msg = str(err).lower()
        return "timeout" in msg or "timed out" in msg or "time limit" in msg

    def _enqueue(p):
        if p.pipeline_id in s.dead_pipelines:
            return
        if p.priority == Priority.QUERY:
            s.q_query.append(p)
        elif p.priority == Priority.INTERACTIVE:
            s.q_interactive.append(p)
        else:
            s.q_batch.append(p)

    def _backlog_counts():
        return (len(s.q_query), len(s.q_interactive), len(s.q_batch))

    def _has_high_backlog():
        return len(s.q_query) > 0 or len(s.q_interactive) > 0

    def _pick_next_pipeline(allowed_priorities):
        # Rotate within each eligible queue; bounded scan to avoid infinite loops.
        # allowed_priorities is an ordered list (highest first).
        prio_to_q = {
            Priority.QUERY: s.q_query,
            Priority.INTERACTIVE: s.q_interactive,
            Priority.BATCH_PIPELINE: s.q_batch,
        }
        for pr in allowed_priorities:
            q = prio_to_q.get(pr, None)
            if not q:
                continue
            n = len(q)
            for _ in range(n):
                p = q.pop(0)
                if p.pipeline_id in s.dead_pipelines:
                    continue
                st = p.runtime_status()
                if st.is_pipeline_successful():
                    continue

                op_list = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
                if op_list:
                    return p, op_list
                # Not runnable yet; rotate.
                q.append(p)
        return None, None

    def _default_cpu_for(priority):
        # Conservative but not tiny; timeouts suggest CPU may need to be non-trivial.
        if priority == Priority.QUERY:
            return 2.0
        if priority == Priority.INTERACTIVE:
            return 3.0
        return 4.0  # batch

    def _base_ram_frac_for(priority):
        # Much smaller than previous version to avoid chronic over-allocation and improve concurrency.
        if priority == Priority.QUERY:
            return 0.05
        if priority == Priority.INTERACTIVE:
            return 0.07
        return 0.10

    def _cap_cpu_for(priority, pool_max_cpu):
        # Keep caps so one op doesn't monopolize a pool; still allow higher caps for batch.
        if priority == Priority.QUERY:
            return min(4.0, pool_max_cpu * 0.35)
        if priority == Priority.INTERACTIVE:
            return min(6.0, pool_max_cpu * 0.45)
        return min(10.0, pool_max_cpu * 0.70)

    def _cap_ram_for(priority, pool_max_ram):
        # Upper bound per-container RAM share (prevents a single batch op from grabbing everything).
        if priority == Priority.QUERY:
            return pool_max_ram * 0.25
        if priority == Priority.INTERACTIVE:
            return pool_max_ram * 0.35
        return pool_max_ram * 0.60

    def _size_op(p, op, pool, pool_id, avail_cpu, avail_ram):
        # Build an initial estimate from:
        #   - per-operator learned estimate if available
        #   - otherwise small fraction of pool capacity based on priority
        # Then clamp to caps, available resources, and reasonable minimums.

        op_key = s._op_key(op, p.pipeline_id)

        # RAM
        base_ram = pool.max_ram_pool * _base_ram_frac_for(p.priority)
        ram_est = s.op_ram_est.get(op_key, base_ram)
        # Ensure we always request at least a tiny amount if the simulator expects >0.
        ram_min = min(avail_ram, pool.max_ram_pool) * 0.01
        if ram_min <= 0:
            ram_min = 0.0

        ram_cap = min(_cap_ram_for(p.priority, pool.max_ram_pool), avail_ram)
        ram = max(ram_min, min(ram_est, ram_cap))

        # CPU
        base_cpu = _default_cpu_for(p.priority)
        cpu_est = s.op_cpu_est.get(op_key, base_cpu)

        cpu_cap = min(_cap_cpu_for(p.priority, pool.max_cpu_pool), avail_cpu)
        # If backlog is large, bias toward smaller per-container CPU to increase concurrency.
        qn, in_, bn = _backlog_counts()
        total_backlog = qn + in_ + bn
        if total_backlog > 0:
            # More backlog -> more splitting. Keep QUERY less affected.
            if p.priority == Priority.BATCH_PIPELINE:
                split = 1.0 + min(3.0, total_backlog / 2000.0)
                cpu_est = max(1.0, cpu_est / split)
            elif p.priority == Priority.INTERACTIVE:
                split = 1.0 + min(2.0, total_backlog / 4000.0)
                cpu_est = max(1.0, cpu_est / split)

        cpu = min(max(1.0, cpu_est), cpu_cap)

        # If we can't give at least 1 vCPU or any RAM, signal inability.
        if cpu < 0.99 or ram <= 0.0:
            return 0.0, 0.0

        return cpu, ram

    # Ingest new pipelines
    for p in pipelines:
        _enqueue(p)

    if not pipelines and not results:
        return [], []

    # Update resource estimates from results (OOM/timeout/success)
    for r in results:
        # We may not have pipeline_id on result; best-effort.
        rid_pid = getattr(r, "pipeline_id", None)

        # If non-OOM failure and pipeline_id is available, stop retrying that pipeline.
        if r.failed() and not _is_oom(r.error) and not _is_timeout(r.error):
            if rid_pid is not None:
                s.dead_pipelines.add(rid_pid)

        # Update per-op estimates based on error type.
        for op in (r.ops or []):
            op_key = s._op_key(op, rid_pid)
            if op_key[0] is None:
                # Fall back to op identity only (still useful within a run).
                op_key = (None, op_key[1])

            if r.failed():
                if _is_oom(r.error):
                    # OOM: raise RAM aggressively (use allocated ram as a floor).
                    prev = s.op_ram_est.get(op_key, max(1.0, getattr(r, "ram", 1.0)))
                    floor = getattr(r, "ram", prev)
                    nxt = max(floor * 2.0, prev * 2.0)
                    s.op_ram_est[op_key] = nxt
                elif _is_timeout(r.error):
                    # Timeout: likely under-provisioned CPU (or queueing); increase CPU moderately.
                    prev = s.op_cpu_est.get(op_key, max(1.0, getattr(r, "cpu", 1.0)))
                    floor = getattr(r, "cpu", prev)
                    nxt = max(floor * 1.5, prev * 1.3)
                    s.op_cpu_est[op_key] = nxt
                else:
                    # Unknown failure: do not amplify; leave estimates unchanged.
                    pass
            else:
                # Success: slowly decay estimates to reclaim resources and improve concurrency.
                # This directly addresses the prior run's "allocated high, consumed low" signal.
                if op_key in s.op_ram_est:
                    s.op_ram_est[op_key] = max(1.0, s.op_ram_est[op_key] * 0.92)
                if op_key in s.op_cpu_est:
                    s.op_cpu_est[op_key] = max(1.0, s.op_cpu_est[op_key] * 0.95)

    suspensions = []
    assignments = []

    # Scheduling: fill pools
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool

        # Determine which priorities this pool should prefer.
        # If multiple pools exist, pool 0 is "latency pool" for QUERY/INTERACTIVE.
        if s.executor.num_pools > 1:
            if pool_id == 0:
                preferred = [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]
            else:
                preferred = [Priority.BATCH_PIPELINE, Priority.INTERACTIVE, Priority.QUERY]
        else:
            preferred = [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]

        # Headroom reservation: if high-priority backlog exists, don't let batch consume the last chunk.
        # (Prevents high-priority arrivals from waiting behind batch because pool is packed.)
        reserve_cpu = 0.0
        reserve_ram = 0.0
        if _has_high_backlog():
            reserve_cpu = pool.max_cpu_pool * 0.10
            reserve_ram = pool.max_ram_pool * 0.10

        # Fill the pool with multiple small-to-moderate assignments for better concurrency.
        # Bound iterations per tick to avoid pathologically long loops when queues are huge.
        step_budget = 64
        while step_budget > 0 and avail_cpu > 1.0 and avail_ram > 0.0:
            step_budget -= 1

            # If this is a non-latency pool and there's high-priority backlog, allow INTERACTIVE spillover
            # but keep QUERY mostly on pool 0 unless others are idle.
            allowed = list(preferred)
            if s.executor.num_pools > 1 and pool_id != 0 and _has_high_backlog():
                allowed = [Priority.BATCH_PIPELINE, Priority.INTERACTIVE]

            p, op_list = _pick_next_pipeline(allowed)
            if not p:
                break

            # If we're about to schedule batch while we should preserve headroom, check reservation.
            if p.priority == Priority.BATCH_PIPELINE and _has_high_backlog():
                if avail_cpu <= reserve_cpu or avail_ram <= reserve_ram:
                    # Put it back and stop scheduling batch on this pool for now.
                    _enqueue(p)
                    break

            op = op_list[0]
            cpu, ram = _size_op(p, op, pool, pool_id, avail_cpu, avail_ram)
            if cpu <= 0.0 or ram <= 0.0:
                _enqueue(p)
                break

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

            avail_cpu -= cpu
            avail_ram -= ram

            # Round-robin: rotate pipeline to back so other pipelines can make progress.
            _enqueue(p)

    return suspensions, assignments
