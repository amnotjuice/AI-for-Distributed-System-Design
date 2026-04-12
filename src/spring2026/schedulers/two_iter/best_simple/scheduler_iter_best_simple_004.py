# policy_key: scheduler_iter_best_simple_004
# context: simple
# model: gpt-5.2-2025-12-11
# llm_cost: 0.042165
# generation_seconds: 31.48
# generated_at: 2026-04-11T22:40:52.296426
@register_scheduler_init(key="scheduler_iter_best_simple_004")
def scheduler_iter_best_simple_004_init(s):
    """Priority-aware, reservation-based scheduler to reduce weighted latency.

    Incremental improvements over prior version:
      - Strict priority scheduling (QUERY > INTERACTIVE > BATCH) rather than mixing all in one loop.
      - Soft per-pool reservations: keep CPU/RAM headroom for higher priorities; throttle BATCH usage.
      - Priority-aware pool choice: place high-priority work on the pool with most free headroom.
      - Keep simple OOM backoff (RAM multiplier per operator) with conservative caps.
      - Round-robin within each priority queue for fairness and to avoid head-of-line blocking.

    Intentionally still simple:
      - No preemption (not enough runtime visibility guaranteed by the template).
      - No runtime prediction/SRPT (not available in the provided interfaces).
    """
    s.q_query = []
    s.q_interactive = []
    s.q_batch = []

    # Adaptive RAM multiplier per operator key (to mitigate repeated OOMs)
    s.op_ram_mult = {}

    # Pipelines we will ignore (e.g., repeated non-OOM failures, if detectable)
    s.dead_pipelines = set()

    def _op_key(op, pipeline_id=None):
        oid = getattr(op, "op_id", None)
        if oid is None:
            oid = getattr(op, "operator_id", None)
        if oid is None:
            oid = id(op)
        return (pipeline_id, oid)

    s._op_key = _op_key


@register_scheduler(key="scheduler_iter_best_simple_004")
def scheduler_iter_best_simple_004_scheduler(s, results, pipelines):
    """Schedule high-priority ops quickly by reserving headroom and throttling batch."""
    # --- helpers ---
    def _is_oom_error(err) -> bool:
        if err is None:
            return False
        msg = str(err).lower()
        return ("oom" in msg) or ("out of memory" in msg)

    def _enqueue(p):
        if p.pipeline_id in s.dead_pipelines:
            return
        if p.priority == Priority.QUERY:
            s.q_query.append(p)
        elif p.priority == Priority.INTERACTIVE:
            s.q_interactive.append(p)
        else:
            s.q_batch.append(p)

    def _pop_runnable_from_queue(q):
        # Round-robin scan: return (pipeline, [op]) or (None, None)
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
            # Not runnable yet; rotate to back
            q.append(p)
        return None, None

    def _pool_headroom(pool):
        return (pool.avail_cpu_pool * pool.avail_ram_pool)

    def _size_for(priority, pool, op, pipeline_id, cpu_budget, ram_budget):
        # Small, conservative allocations for fast start; favor latency over throughput.
        max_cpu = pool.max_cpu_pool
        max_ram = pool.max_ram_pool

        # CPU caps: keep QUERY snappy, INTERACTIVE moderate, BATCH small to reduce interference.
        if priority == Priority.QUERY:
            cpu_cap = min(4.0, max_cpu * 0.35)
            ram_base_frac = 0.20
        elif priority == Priority.INTERACTIVE:
            cpu_cap = min(6.0, max_cpu * 0.45)
            ram_base_frac = 0.28
        else:
            cpu_cap = min(2.0, max_cpu * 0.20)
            ram_base_frac = 0.22

        # RAM multiplier from previous OOMs (if any)
        op_key = s._op_key(op, pipeline_id)
        mult = s.op_ram_mult.get(op_key, 1.0)
        if mult < 1.0:
            mult = 1.0
        if mult > 16.0:
            mult = 16.0

        cpu = min(cpu_budget, cpu_cap)
        ram = min(ram_budget, max_ram * ram_base_frac * mult)

        # Avoid tiny allocations that just create churn
        if cpu < 0.01 or ram < 0.01:
            return 0.0, 0.0
        return cpu, ram

    # --- ingest new pipelines ---
    for p in pipelines:
        _enqueue(p)

    if not pipelines and not results:
        return [], []

    # --- process results: adjust RAM on OOM failures ---
    for r in results:
        if not r.failed():
            continue
        if _is_oom_error(r.error):
            for op in (r.ops or []):
                # We may not have pipeline_id on result; use None to still learn per-op object/id.
                op_key = s._op_key(op, None)
                cur = s.op_ram_mult.get(op_key, 1.0)
                nxt = cur * 2.0
                if nxt > 16.0:
                    nxt = 16.0
                s.op_ram_mult[op_key] = nxt
        else:
            pid = getattr(r, "pipeline_id", None)
            if pid is not None:
                s.dead_pipelines.add(pid)

    suspensions = []
    assignments = []

    # If there is any high-priority demand, reserve more headroom (throttle batch).
    hi_demand = bool(s.q_query or s.q_interactive)

    # Reservations are fractions of pool max resources we try not to consume with BATCH.
    # This reduces the chance that high-priority arrivals must wait for batch to finish.
    if hi_demand:
        reserve_cpu_frac = 0.45
        reserve_ram_frac = 0.45
    else:
        reserve_cpu_frac = 0.20
        reserve_ram_frac = 0.20

    # Choose pool order: for high priority, pick most headroom first.
    pool_ids_by_headroom = list(range(s.executor.num_pools))
    pool_ids_by_headroom.sort(key=lambda i: _pool_headroom(s.executor.pools[i]), reverse=True)

    # 1) Schedule QUERY first
    for pool_id in pool_ids_by_headroom:
        pool = s.executor.pools[pool_id]
        while pool.avail_cpu_pool > 0.01 and pool.avail_ram_pool > 0.01:
            p, op_list = _pop_runnable_from_queue(s.q_query)
            if not p:
                break
            op = op_list[0]
            cpu, ram = _size_for(p.priority, pool, op, p.pipeline_id, pool.avail_cpu_pool, pool.avail_ram_pool)
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
            _enqueue(p)

    # 2) Then INTERACTIVE
    for pool_id in pool_ids_by_headroom:
        pool = s.executor.pools[pool_id]
        while pool.avail_cpu_pool > 0.01 and pool.avail_ram_pool > 0.01:
            p, op_list = _pop_runnable_from_queue(s.q_interactive)
            if not p:
                break
            op = op_list[0]
            cpu, ram = _size_for(p.priority, pool, op, p.pipeline_id, pool.avail_cpu_pool, pool.avail_ram_pool)
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
            _enqueue(p)

    # 3) Finally BATCH, but only use resources above reservations
    # For batch, we pick pools with MOST available headroom first as well, but respect reserve.
    for pool_id in pool_ids_by_headroom:
        pool = s.executor.pools[pool_id]
        # Compute budgets after reservation. If pool is already below reserve, don't schedule batch here.
        cpu_reserve = pool.max_cpu_pool * reserve_cpu_frac
        ram_reserve = pool.max_ram_pool * reserve_ram_frac

        while True:
            cpu_budget = pool.avail_cpu_pool - cpu_reserve
            ram_budget = pool.avail_ram_pool - ram_reserve
            if cpu_budget <= 0.01 or ram_budget <= 0.01:
                break

            p, op_list = _pop_runnable_from_queue(s.q_batch)
            if not p:
                break

            op = op_list[0]
            cpu, ram = _size_for(p.priority, pool, op, p.pipeline_id, cpu_budget, ram_budget)
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
            _enqueue(p)

    return suspensions, assignments
