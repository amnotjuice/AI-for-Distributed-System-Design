# policy_key: scheduler_iter_best_simple_012
# context: simple
# model: gpt-5.2-2025-12-11
# llm_cost: 0.043201
# generation_seconds: 37.53
# generated_at: 2026-04-11T22:45:46.671381
@register_scheduler_init(key="scheduler_iter_best_simple_012")
def scheduler_iter_best_simple_012_init(s):
    """Priority-first, reserve-based scheduler to reduce weighted latency.

    Incremental improvements over the previous attempt:
      - Much more aggressive priority handling: always try QUERY first, then INTERACTIVE, then BATCH.
      - Pool headroom reservation: do not start lower-priority work if it would eat into a reserved
        CPU/RAM buffer intended for imminent QUERY/INTERACTIVE arrivals (since we do not preempt yet).
      - Better sizing for high priority: give QUERY/INTERACTIVE more CPU (finish sooner) and a bit
        more RAM (fewer OOM retries), while keeping BATCH conservative.
      - Queue discipline: FIFO for QUERY/INTERACTIVE (minimize tail), round-robin for BATCH (fairness).

    Still intentionally simple:
      - No preemption (requires reliable visibility/control of running containers beyond this template).
      - No runtime prediction (not available in the provided interface).
    """
    # FIFO queues for high-priority work; RR for batch.
    s.q_query = []
    s.q_interactive = []
    s.q_batch = []

    # Drop pipelines we decide are hopeless (non-OOM failures) to avoid wasting scheduling effort.
    s.dead_pipelines = set()

    # Per-operator RAM backoff on OOM: (pipeline_id, op_idish) -> multiplier.
    s.op_ram_mult = {}

    def _op_key(op, pipeline_id):
        oid = getattr(op, "op_id", None)
        if oid is None:
            oid = getattr(op, "operator_id", None)
        if oid is None:
            oid = id(op)
        return (pipeline_id, oid)

    s._op_key = _op_key


@register_scheduler(key="scheduler_iter_best_simple_012")
def scheduler_iter_best_simple_012_scheduler(s, results, pipelines):
    """Priority-first scheduling with reserved headroom and OOM-aware RAM backoff."""
    # --- helpers ---
    def _is_oom_error(err) -> bool:
        if err is None:
            return False
        msg = str(err).lower()
        return ("oom" in msg) or ("out of memory" in msg) or ("killed" in msg and "memory" in msg)

    def _enqueue_pipeline(p):
        if p.pipeline_id in s.dead_pipelines:
            return
        if p.priority == Priority.QUERY:
            s.q_query.append(p)
        elif p.priority == Priority.INTERACTIVE:
            s.q_interactive.append(p)
        else:
            s.q_batch.append(p)

    def _pipeline_done_or_dead(p) -> bool:
        if p.pipeline_id in s.dead_pipelines:
            return True
        st = p.runtime_status()
        return st.is_pipeline_successful()

    def _get_one_runnable_from_queue(q, rotate: bool, scan_limit: int):
        # Returns (pipeline, [op]) or (None, None)
        n = min(len(q), scan_limit)
        for _ in range(n):
            p = q.pop(0)
            if _pipeline_done_or_dead(p):
                continue
            st = p.runtime_status()
            op_list = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
            if op_list:
                return p, op_list
            # Not runnable yet
            if rotate:
                q.append(p)
            else:
                # For FIFO queues, keep ordering stable: push back but do not keep cycling aggressively.
                q.append(p)
        return None, None

    def _reserve_for_priority(pool, prio):
        # Reserve headroom to keep the pool responsive to high priority arrivals.
        # Since we do not preempt, this is the main lever to reduce weighted latency.
        max_cpu = pool.max_cpu_pool
        max_ram = pool.max_ram_pool

        # Reserve for QUERY when considering INTERACTIVE/BATCH.
        # Reserve for INTERACTIVE when considering BATCH.
        if prio == Priority.QUERY:
            return 0.0, 0.0
        if prio == Priority.INTERACTIVE:
            # Leave space for at least one quick QUERY op.
            return max(1.0, 0.20 * max_cpu), 0.20 * max_ram
        # prio == BATCH_PIPELINE: leave space for both QUERY and INTERACTIVE bursts.
        return max(2.0, 0.35 * max_cpu), 0.35 * max_ram

    def _size_for(priority, pool, op, avail_cpu, avail_ram, pipeline_id):
        # CPU: give more to higher priority to finish faster; cap to prevent monopolization.
        max_cpu = pool.max_cpu_pool
        max_ram = pool.max_ram_pool

        if priority == Priority.QUERY:
            cpu_cap = min(6.0, max(1.0, 0.60 * max_cpu))
            ram_frac = 0.30
        elif priority == Priority.INTERACTIVE:
            cpu_cap = min(8.0, max(1.0, 0.70 * max_cpu))
            ram_frac = 0.28
        else:
            cpu_cap = min(6.0, max(1.0, 0.50 * max_cpu))
            ram_frac = 0.18

        cpu = min(avail_cpu, cpu_cap)

        # RAM: prioritize fewer OOM retries for high priority, be conservative for batch.
        base_ram = max_ram * ram_frac
        op_key = s._op_key(op, pipeline_id)
        mult = s.op_ram_mult.get(op_key, 1.0)
        ram = min(avail_ram, max_ram, base_ram * mult)

        return cpu, ram

    # --- ingest new pipelines ---
    for p in pipelines:
        _enqueue_pipeline(p)

    if not pipelines and not results:
        return [], []

    # --- process results (OOM backoff / drop hard failures) ---
    for r in results:
        if not r.failed():
            continue

        if _is_oom_error(r.error):
            # Increase RAM multiplier for the failing op(s), so the retry is more likely to succeed.
            for op in (r.ops or []):
                pid = getattr(r, "pipeline_id", None)
                op_key = s._op_key(op, pid)
                if op_key[0] is None:
                    op_key = (None, op_key[1])

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

    # --- scheduling: per pool, fill with QUERY, then INTERACTIVE (with reserve), then BATCH (with reserve) ---
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool

        # We schedule multiple ops per pool per tick, but guard against tiny leftover fragmentation.
        while avail_cpu > 0.01 and avail_ram > 0.01:
            made_assignment = False

            # 1) QUERY: always attempt first, no reserve needed.
            p, op_list = _get_one_runnable_from_queue(s.q_query, rotate=False, scan_limit=32)
            if p and op_list:
                op = op_list[0]
                cpu, ram = _size_for(p.priority, pool, op, avail_cpu, avail_ram, p.pipeline_id)
                if cpu > 0.01 and ram > 0.01:
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
                    _enqueue_pipeline(p)  # keep it moving; next ops may become runnable later
                    made_assignment = True
                    continue
                else:
                    _enqueue_pipeline(p)

            # 2) INTERACTIVE: only if we can keep a QUERY reserve.
            p, op_list = _get_one_runnable_from_queue(s.q_interactive, rotate=False, scan_limit=64)
            if p and op_list:
                res_cpu, res_ram = _reserve_for_priority(pool, Priority.INTERACTIVE)
                op = op_list[0]
                cpu, ram = _size_for(p.priority, pool, op, avail_cpu, avail_ram, p.pipeline_id)

                # Enforce reserve: do not consume into the reserved headroom.
                if cpu > 0.01 and ram > 0.01 and (avail_cpu - cpu) >= res_cpu and (avail_ram - ram) >= res_ram:
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
                    _enqueue_pipeline(p)
                    made_assignment = True
                    continue
                else:
                    _enqueue_pipeline(p)

            # 3) BATCH: only if we can keep a larger reserve (QUERY+INTERACTIVE buffer).
            p, op_list = _get_one_runnable_from_queue(s.q_batch, rotate=True, scan_limit=128)
            if p and op_list:
                res_cpu, res_ram = _reserve_for_priority(pool, Priority.BATCH_PIPELINE)
                op = op_list[0]
                cpu, ram = _size_for(p.priority, pool, op, avail_cpu, avail_ram, p.pipeline_id)

                if cpu > 0.01 and ram > 0.01 and (avail_cpu - cpu) >= res_cpu and (avail_ram - ram) >= res_ram:
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
                    _enqueue_pipeline(p)
                    made_assignment = True
                else:
                    _enqueue_pipeline(p)

            if not made_assignment:
                break

    return suspensions, assignments
