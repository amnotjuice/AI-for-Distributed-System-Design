# policy_key: scheduler_iter_best_rich_007
# context: rich
# model: gpt-5.2-2025-12-11
# llm_cost: 0.051899
# generation_seconds: 34.00
# generated_at: 2026-04-12T00:24:49.832234
@register_scheduler_init(key="scheduler_iter_best_rich_007")
def scheduler_iter_best_rich_007_init(s):
    """Priority-aware scheduler focused on reducing weighted latency (iterative improvement).

    Improvements over prior version:
      - Reserve capacity for high-priority (QUERY/INTERACTIVE); throttle BATCH when HP backlog exists.
      - Reduce baseline RAM allocations (fix pathological over-allocation) + keep OOM backoff.
      - Add timeout-aware CPU backoff (many failures were timeouts) to speed up slow ops.
      - Add gentle decay of backoff multipliers on success to avoid permanent bloat.
      - Keep round-robin fairness within each priority queue; avoid spinning on unrunnable pipelines.

    Constraints:
      - No preemption yet (no reliable API for enumerating running containers in provided template).
      - Uses only fields shown in the provided interfaces; degrades gracefully if some fields absent.
    """
    s.q_query = []
    s.q_interactive = []
    s.q_batch = []

    # op resource backoff multipliers
    s.op_ram_mult = {}   # op_key -> mult
    s.op_cpu_mult = {}   # op_key -> mult

    # For bounded scanning
    s.max_scan_per_queue = 64

    def _op_key(op, pipeline_id):
        oid = getattr(op, "op_id", None)
        if oid is None:
            oid = getattr(op, "operator_id", None)
        if oid is None:
            oid = id(op)
        # pipeline_id may be None for some ExecutionResult objects; still stable enough within run
        return (pipeline_id, oid)

    s._op_key = _op_key


@register_scheduler(key="scheduler_iter_best_rich_007")
def scheduler_iter_best_rich_007_scheduler(s, results, pipelines):
    """See init docstring for overview."""
    def _is_oom_error(err) -> bool:
        if err is None:
            return False
        msg = str(err).lower()
        return ("oom" in msg) or ("out of memory" in msg)

    def _is_timeout_error(err) -> bool:
        if err is None:
            return False
        msg = str(err).lower()
        return ("timeout" in msg) or ("timed out" in msg) or ("deadline" in msg)

    def _enqueue(p):
        if p.priority == Priority.QUERY:
            s.q_query.append(p)
        elif p.priority == Priority.INTERACTIVE:
            s.q_interactive.append(p)
        else:
            s.q_batch.append(p)

    def _has_hp_backlog():
        # HP backlog exists if any QUERY/INTERACTIVE pipeline currently has an assignable op.
        for q in (s.q_query, s.q_interactive):
            if not q:
                continue
            n = min(len(q), s.max_scan_per_queue)
            for i in range(n):
                p = q[i]
                st = p.runtime_status()
                if st.is_pipeline_successful():
                    continue
                if st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]:
                    return True
        return False

    def _pop_next_from_queue(q, require_parents_complete=True):
        # Round-robin pop of the next pipeline in q that currently has an assignable op.
        if not q:
            return None, None
        n = min(len(q), s.max_scan_per_queue)
        for _ in range(n):
            p = q.pop(0)
            st = p.runtime_status()
            if st.is_pipeline_successful():
                continue
            op_list = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=require_parents_complete)[:1]
            if op_list:
                return p, op_list
            # Not runnable yet; rotate to back
            q.append(p)
        return None, None

    def _baseline_caps(priority, pool):
        # CPU caps: favor QUERY/INTERACTIVE to reduce timeouts and weighted latency.
        # RAM baselines lowered aggressively to reduce "allocated %" while relying on OOM backoff.
        max_cpu = pool.max_cpu_pool
        max_ram = pool.max_ram_pool

        if priority == Priority.QUERY:
            cpu_cap = min(6.0, max_cpu * 0.60)
            ram_base = max_ram * 0.06
            cpu_floor = 1.0
        elif priority == Priority.INTERACTIVE:
            cpu_cap = min(8.0, max_cpu * 0.70)
            ram_base = max_ram * 0.10
            cpu_floor = 1.0
        else:  # BATCH_PIPELINE
            cpu_cap = min(10.0, max_cpu * 0.80)
            ram_base = max_ram * 0.18
            cpu_floor = 0.5

        return cpu_floor, cpu_cap, ram_base

    def _size_op(priority, pool, op, pipeline_id, avail_cpu, avail_ram):
        cpu_floor, cpu_cap, ram_base = _baseline_caps(priority, pool)

        op_key = s._op_key(op, pipeline_id)
        ram_mult = s.op_ram_mult.get(op_key, 1.0)
        cpu_mult = s.op_cpu_mult.get(op_key, 1.0)

        # Apply multipliers; clamp to pool and availability
        cpu_req = min(cpu_cap * cpu_mult, pool.max_cpu_pool, avail_cpu)
        # Enforce a small floor to avoid pathological "tiny CPU => long runtimes => timeouts"
        if cpu_req < cpu_floor:
            cpu_req = min(avail_cpu, cpu_floor)

        ram_req = min(ram_base * ram_mult, pool.max_ram_pool, avail_ram)
        # Ensure non-zero requests (avoid busy-looping)
        if ram_req < 0.01:
            ram_req = min(avail_ram, 0.01)

        return cpu_req, ram_req

    # ---- ingest new pipelines ----
    for p in pipelines:
        _enqueue(p)

    # Early exit
    if not pipelines and not results:
        return [], []

    # ---- process results: adaptive backoff/decay ----
    for r in results:
        ops = r.ops or []
        pid = getattr(r, "pipeline_id", None)

        if r.failed():
            if _is_oom_error(r.error):
                # Increase RAM for this op; lightly decrease CPU (often OOM is RAM sizing, not CPU).
                for op in ops:
                    k = s._op_key(op, pid)
                    cur = s.op_ram_mult.get(k, 1.0)
                    nxt = cur * 2.0
                    if nxt > 32.0:
                        nxt = 32.0
                    s.op_ram_mult[k] = nxt

                    ccur = s.op_cpu_mult.get(k, 1.0)
                    if ccur > 1.0:
                        s.op_cpu_mult[k] = max(1.0, ccur * 0.9)
            elif _is_timeout_error(r.error):
                # Increase CPU for this op (timeouts dominated failures in prior run).
                # Also slightly increase RAM to reduce risk of borderline memory causing slowdowns.
                for op in ops:
                    k = s._op_key(op, pid)
                    cur = s.op_cpu_mult.get(k, 1.0)
                    nxt = cur * 1.6
                    if nxt > 6.0:
                        nxt = 6.0
                    s.op_cpu_mult[k] = nxt

                    rcur = s.op_ram_mult.get(k, 1.0)
                    if rcur < 4.0:
                        s.op_ram_mult[k] = min(4.0, rcur * 1.25)
            else:
                # Unknown failure: modestly increase CPU once; avoid runaway.
                for op in ops:
                    k = s._op_key(op, pid)
                    cur = s.op_cpu_mult.get(k, 1.0)
                    if cur < 2.0:
                        s.op_cpu_mult[k] = 2.0
        else:
            # Success: gently decay multipliers so we don't keep over-provisioning forever.
            for op in ops:
                k = s._op_key(op, pid)
                rcur = s.op_ram_mult.get(k, 1.0)
                ccur = s.op_cpu_mult.get(k, 1.0)
                if rcur > 1.0:
                    s.op_ram_mult[k] = max(1.0, rcur * 0.95)
                if ccur > 1.0:
                    s.op_cpu_mult[k] = max(1.0, ccur * 0.93)

    suspensions = []
    assignments = []

    # ---- scheduling: prioritize QUERY/INTERACTIVE; throttle BATCH under HP backlog ----
    hp_backlog = _has_hp_backlog()

    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool

        # Pool-level reservations: keep some headroom for QUERY/INTERACTIVE arrivals.
        # Reservations are stronger when there is current HP backlog.
        if hp_backlog:
            reserve_cpu = max(0.0, pool.max_cpu_pool * 0.15)
            reserve_ram = max(0.0, pool.max_ram_pool * 0.10)
        else:
            reserve_cpu = max(0.0, pool.max_cpu_pool * 0.08)
            reserve_ram = max(0.0, pool.max_ram_pool * 0.06)

        # Phase 1: schedule as much QUERY then INTERACTIVE as possible
        for q in (s.q_query, s.q_interactive):
            while avail_cpu > 0.01 and avail_ram > 0.01:
                p, op_list = _pop_next_from_queue(q, require_parents_complete=True)
                if not p:
                    break
                op = op_list[0]
                cpu, ram = _size_op(p.priority, pool, op, p.pipeline_id, avail_cpu, avail_ram)
                if cpu <= 0.01 or ram <= 0.01:
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
                _enqueue(p)

        # Phase 2: schedule BATCH only if there's slack beyond reservations (or no HP backlog)
        while avail_cpu > 0.01 and avail_ram > 0.01:
            if hp_backlog and (avail_cpu <= reserve_cpu or avail_ram <= reserve_ram):
                break

            p, op_list = _pop_next_from_queue(s.q_batch, require_parents_complete=True)
            if not p:
                break

            op = op_list[0]
            # For batch, if HP backlog exists, be extra conservative with CPU to avoid interfering.
            cpu, ram = _size_op(p.priority, pool, op, p.pipeline_id, avail_cpu, avail_ram)
            if hp_backlog:
                cpu = min(cpu, max(0.5, pool.max_cpu_pool * 0.20), avail_cpu)

            if cpu <= 0.01 or ram <= 0.01:
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
            _enqueue(p)

    return suspensions, assignments
