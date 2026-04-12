# policy_key: scheduler_iter_best_rich_006
# context: rich
# model: gpt-5.2-2025-12-11
# llm_cost: 0.048105
# generation_seconds: 39.89
# generated_at: 2026-04-12T00:24:15.831132
@register_scheduler_init(key="scheduler_iter_best_rich_006")
def scheduler_iter_best_rich_006_init(s):
    """Priority-aware scheduler focused on reducing weighted latency with small, robust improvements.

    Improvements over naive FIFO (and prior simple priority FIFO):
      - Strict priority order: QUERY > INTERACTIVE > BATCH, with round-robin fairness within each class.
      - High-priority reservations: never let BATCH consume the last slice of CPU/RAM in a pool.
      - Lower initial RAM asks (to avoid excessive RAM reservation / low effective concurrency),
        with fast exponential backoff on OOM to converge to adequate sizing.
      - Moderate minimum CPU per op (avoid pathological tiny allocations that can drive timeouts).
      - Pool-aware: within each pool, schedule high priority first; batch only if headroom remains.

    Notes:
      - No preemption: simulator interface provided doesn¡¯t expose a safe/standard way to enumerate
        running low-priority containers for suspension decisions.
      - OOM detection is best-effort via error string matching.
    """
    # Per-priority queues (pipelines). Pipelines are rotated for round-robin fairness.
    s.q_query = []
    s.q_interactive = []
    s.q_batch = []

    # Track pipelines we should stop scheduling (non-OOM repeated failures).
    s.dead_pipelines = set()

    # Per-operator RAM multiplier for OOM retries; key uses object identity (works within a sim run).
    # op_key -> mult (1, 2, 4, ...)
    s.op_ram_mult = {}

    # Per-operator failure counters to stop futile retries on repeated non-OOM failures.
    s.op_fail_count = {}

    def _op_key(op):
        oid = getattr(op, "op_id", None)
        if oid is None:
            oid = getattr(op, "operator_id", None)
        if oid is None:
            oid = id(op)
        return oid

    s._op_key = _op_key


@register_scheduler(key="scheduler_iter_best_rich_006")
def scheduler_iter_best_rich_006_scheduler(s, results, pipelines):
    """
    Scheduler tick:
      1) Enqueue new pipelines into per-priority queues.
      2) Update OOM-driven RAM multipliers based on recent failures.
      3) For each pool, schedule in phases (QUERY, INTERACTIVE, then BATCH) while respecting
         per-pool reservations for high-priority work.
    """
    # ---------------- Helpers ----------------
    def _is_oom_error(err) -> bool:
        if err is None:
            return False
        msg = str(err).lower()
        return ("oom" in msg) or ("out of memory" in msg) or ("memoryerror" in msg)

    def _enqueue_pipeline(p):
        if p.pipeline_id in s.dead_pipelines:
            return
        if p.priority == Priority.QUERY:
            s.q_query.append(p)
        elif p.priority == Priority.INTERACTIVE:
            s.q_interactive.append(p)
        else:
            s.q_batch.append(p)

    def _pop_next_runnable_from_queue(q):
        # Round-robin scan for a runnable pipeline; returns (pipeline, [op]) or (None, None)
        if not q:
            return None, None
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
            # Not runnable yet, rotate.
            q.append(p)
        return None, None

    def _requeue_pipeline(p):
        # Keep pipeline in its priority queue for future ops.
        _enqueue_pipeline(p)

    def _ram_backoff_for_op(op):
        return s.op_ram_mult.get(s._op_key(op), 1.0)

    def _compute_request(priority, pool, op, avail_cpu, avail_ram):
        # Goal: reduce RAM over-reservation to increase concurrency, while ensuring decent CPU to avoid timeouts.
        max_cpu = pool.max_cpu_pool
        max_ram = pool.max_ram_pool

        # Base RAM fractions intentionally low; rely on OOM backoff to find true needs.
        if priority == Priority.QUERY:
            base_ram_frac = 0.05
            cpu_cap = min(6.0, max_cpu * 0.60)
            min_cpu = 1.0
        elif priority == Priority.INTERACTIVE:
            base_ram_frac = 0.07
            cpu_cap = min(8.0, max_cpu * 0.70)
            min_cpu = 1.0
        else:
            base_ram_frac = 0.09
            cpu_cap = min(10.0, max_cpu * 0.80)
            min_cpu = 0.5

        # CPU: give at least min_cpu if possible, but avoid leaving unusable tiny fragments.
        cpu = min(avail_cpu, cpu_cap)
        if cpu < min_cpu:
            # If we can¡¯t provide the minimum, don¡¯t schedule this op in this pool right now.
            return 0.0, 0.0

        # RAM: start small; scale up quickly after OOM.
        mult = _ram_backoff_for_op(op)
        ram = max_ram * base_ram_frac * mult
        ram = min(ram, avail_ram, max_ram)
        # Avoid scheduling with negligible RAM.
        if ram <= 0.01:
            return 0.0, 0.0

        return cpu, ram

    def _has_pending_high_priority():
        # A cheap proxy: if queues non-empty, there is likely demand soon.
        # Even if not runnable at this instant, reserving a small headroom reduces latency spikes.
        return (len(s.q_query) > 0) or (len(s.q_interactive) > 0)

    # ---------------- Ingest new pipelines ----------------
    for p in pipelines:
        _enqueue_pipeline(p)

    if not pipelines and not results:
        return [], []

    # ---------------- Process results ----------------
    for r in results:
        if not r.failed():
            continue

        if _is_oom_error(r.error):
            # Increase RAM multiplier for the failed operator(s).
            for op in (r.ops or []):
                k = s._op_key(op)
                cur = s.op_ram_mult.get(k, 1.0)
                nxt = cur * 2.0
                if nxt > 32.0:
                    nxt = 32.0
                s.op_ram_mult[k] = nxt
        else:
            # Non-OOM failure: count and eventually stop retrying that operator by marking its pipeline dead
            # if we can infer it. We generally keep this conservative to avoid dropping good work.
            for op in (r.ops or []):
                k = s._op_key(op)
                s.op_fail_count[k] = s.op_fail_count.get(k, 0) + 1
            # If result contains pipeline_id in this simulator build, use it.
            pid = getattr(r, "pipeline_id", None)
            if pid is not None:
                s.dead_pipelines.add(pid)

    suspensions = []
    assignments = []

    # ---------------- Scheduling per pool ----------------
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool

        # Per-pool reservations: keep a slice for high-priority to reduce queueing delays.
        # These are modest to avoid starving batch while still protecting interactive latency.
        reserve_cpu_for_hp = max(0.0, pool.max_cpu_pool * 0.15)
        reserve_ram_for_hp = max(0.0, pool.max_ram_pool * 0.10)

        # If we already have high-priority demand somewhere, enforce reservations more strictly.
        # Otherwise allow batch to use more of the pool.
        hp_demand = _has_pending_high_priority()

        # Fill the pool while meaningful headroom exists.
        # Use phases to keep strict priority; within phase use round-robin.
        while avail_cpu > 0.01 and avail_ram > 0.01:
            made_assignment = False

            # Phase 1: QUERY
            p, op_list = _pop_next_runnable_from_queue(s.q_query)
            if p is not None:
                cpu, ram = _compute_request(p.priority, pool, op_list[0], avail_cpu, avail_ram)
                if cpu > 0.0 and ram > 0.0:
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
                    _requeue_pipeline(p)
                    made_assignment = True
                else:
                    # Can't fit now; rotate back and try other work.
                    _requeue_pipeline(p)

            if made_assignment:
                continue

            # Phase 2: INTERACTIVE
            p, op_list = _pop_next_runnable_from_queue(s.q_interactive)
            if p is not None:
                cpu, ram = _compute_request(p.priority, pool, op_list[0], avail_cpu, avail_ram)
                if cpu > 0.0 and ram > 0.0:
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
                    _requeue_pipeline(p)
                    made_assignment = True
                else:
                    _requeue_pipeline(p)

            if made_assignment:
                continue

            # Phase 3: BATCH (only if we won't eat into reserved headroom for high priority)
            if hp_demand:
                if avail_cpu <= reserve_cpu_for_hp or avail_ram <= reserve_ram_for_hp:
                    break

            p, op_list = _pop_next_runnable_from_queue(s.q_batch)
            if p is None:
                break

            # For batch, also avoid consuming reserved headroom if hp demand exists.
            cpu, ram = _compute_request(p.priority, pool, op_list[0], avail_cpu, avail_ram)
            if cpu > 0.0 and ram > 0.0:
                if hp_demand:
                    # Ensure post-allocation still keeps reservation.
                    if (avail_cpu - cpu) < reserve_cpu_for_hp or (avail_ram - ram) < reserve_ram_for_hp:
                        # Can't place this batch op without harming high-priority latency.
                        _requeue_pipeline(p)
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
                _requeue_pipeline(p)
                made_assignment = True
            else:
                _requeue_pipeline(p)
                break

            if not made_assignment:
                break

    return suspensions, assignments
