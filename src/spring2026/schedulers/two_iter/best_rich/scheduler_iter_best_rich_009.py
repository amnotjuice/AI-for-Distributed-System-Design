# policy_key: scheduler_iter_best_rich_009
# context: rich
# model: gpt-5.2-2025-12-11
# llm_cost: 0.051409
# generation_seconds: 36.86
# generated_at: 2026-04-12T00:26:03.153854
@register_scheduler_init(key="scheduler_iter_best_rich_009")
def scheduler_iter_best_rich_009_init(s):
    """Incremental priority-aware scheduler focused on weighted latency.

    Improvements over the previous priority FIFO variant:
      - Much smaller initial RAM asks (reduce wasted allocation; increase parallelism).
      - OOM-triggered RAM backoff per operator signature (exponential, capped).
      - Timeout-triggered CPU backoff per operator signature (multiplicative, capped).
      - Soft reservations: batch cannot consume the last slice of pool resources,
        preserving headroom for QUERY/INTERACTIVE arrivals (no preemption available).
      - Priority order enforced inside each pool: QUERY > INTERACTIVE > BATCH,
        with round-robin fairness within each class.

    Intentionally still simple:
      - No suspensions/preemption (not enough visibility into running set in the template).
      - No runtime estimation (ExecutionResult interface here doesn¡¯t expose durations).
    """
    s.q_query = []
    s.q_interactive = []
    s.q_batch = []

    # Per-operator adaptive multipliers (keyed by a stable-ish operator signature).
    s.op_ram_mult = {}  # op_sig -> mult
    s.op_cpu_mult = {}  # op_sig -> mult

    # If a pipeline has repeated non-resource failures, we can stop re-queuing it.
    s.dead_pipelines = set()

    def _op_sig_from_op(op, priority=None):
        # Prefer explicit ids if present; else fall back to Python object id.
        oid = getattr(op, "op_id", None)
        if oid is None:
            oid = getattr(op, "operator_id", None)
        if oid is None:
            oid = id(op)
        return (priority, oid)

    s._op_sig_from_op = _op_sig_from_op


@register_scheduler(key="scheduler_iter_best_rich_009")
def scheduler_iter_best_rich_009_scheduler(s, results, pipelines):
    """Priority-first, reservation-based packing with OOM/timeout adaptive sizing."""
    # -------- helper functions (no imports) --------
    def _is_oom_error(err) -> bool:
        if err is None:
            return False
        msg = str(err).lower()
        return ("oom" in msg) or ("out of memory" in msg)

    def _is_timeout_error(err) -> bool:
        if err is None:
            return False
        msg = str(err).lower()
        return ("timeout" in msg) or ("timed out" in msg) or ("time out" in msg)

    def _enqueue(p):
        if p.pipeline_id in s.dead_pipelines:
            return
        if p.priority == Priority.QUERY:
            s.q_query.append(p)
        elif p.priority == Priority.INTERACTIVE:
            s.q_interactive.append(p)
        else:
            s.q_batch.append(p)

    def _next_runnable_from_queue(q):
        # Round-robin scan to find a pipeline with a parent-ready assignable op.
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
            # Not runnable yet; rotate to back.
            q.append(p)
        return None, None

    def _base_caps(priority, pool):
        # CPU caps: keep queries snappy, interactive responsive, batch opportunistic.
        # RAM bases: start small to reduce wasted allocation; rely on OOM backoff to converge.
        max_cpu = pool.max_cpu_pool
        max_ram = pool.max_ram_pool

        if priority == Priority.QUERY:
            base_cpu = min(6.0, max_cpu * 0.60)
            base_ram = max_ram * 0.05
            cpu_cap = min(10.0, max_cpu * 0.85)
            ram_cap = max_ram * 0.35
        elif priority == Priority.INTERACTIVE:
            base_cpu = min(8.0, max_cpu * 0.65)
            base_ram = max_ram * 0.08
            cpu_cap = min(12.0, max_cpu * 0.90)
            ram_cap = max_ram * 0.45
        else:  # BATCH_PIPELINE
            base_cpu = min(12.0, max_cpu * 0.75)
            base_ram = max_ram * 0.12
            cpu_cap = min(16.0, max_cpu * 0.95)
            ram_cap = max_ram * 0.65

        # Avoid tiny containers that risk pathological slowdowns and timeouts.
        base_cpu = max(base_cpu, 1.0)
        base_ram = max(base_ram, max_ram * 0.01)

        return base_cpu, base_ram, cpu_cap, ram_cap

    def _size_op(priority, pool, op, avail_cpu, avail_ram, reserve_cpu, reserve_ram):
        # Enforce soft reservations (no preemption available): lower priorities can¡¯t eat all headroom.
        eff_cpu = avail_cpu - reserve_cpu
        eff_ram = avail_ram - reserve_ram
        if eff_cpu <= 0.01 or eff_ram <= 0.01:
            return 0.0, 0.0

        base_cpu, base_ram, cpu_cap, ram_cap = _base_caps(priority, pool)

        sig = s._op_sig_from_op(op, priority=priority)
        ram_mult = s.op_ram_mult.get(sig, 1.0)
        cpu_mult = s.op_cpu_mult.get(sig, 1.0)

        cpu = base_cpu * cpu_mult
        ram = base_ram * ram_mult

        # Clamp to caps and current effective availability.
        cpu = min(cpu, cpu_cap, eff_cpu)
        ram = min(ram, ram_cap, eff_ram)

        # If we can¡¯t give at least ~1 vCPU and a sliver of RAM, don¡¯t schedule.
        if cpu < 0.99 or ram <= 0.01:
            return 0.0, 0.0
        return cpu, ram

    def _try_schedule_one_from_queue(q, pool_id, pool, avail_cpu, avail_ram, reserve_cpu, reserve_ram, priority):
        p, op_list = _next_runnable_from_queue(q)
        if not p:
            return None, avail_cpu, avail_ram

        op = op_list[0]
        cpu, ram = _size_op(priority, pool, op, avail_cpu, avail_ram, reserve_cpu, reserve_ram)
        if cpu <= 0.0 or ram <= 0.0:
            # Put it back (fairness) and signal no fit at this moment.
            _enqueue(p)
            return None, avail_cpu, avail_ram

        a = Assignment(
            ops=op_list,
            cpu=cpu,
            ram=ram,
            priority=p.priority,
            pool_id=pool_id,
            pipeline_id=p.pipeline_id,
        )

        # Rotate pipeline to back for RR fairness within its class.
        _enqueue(p)
        return a, avail_cpu - cpu, avail_ram - ram

    # -------- ingest new pipelines --------
    for p in pipelines:
        _enqueue(p)

    if not pipelines and not results:
        return [], []

    # -------- process execution results to adapt sizing --------
    for r in results:
        if not r.failed():
            continue

        # Update multipliers based on failure type.
        # Key by operator signature + priority; result provides r.ops and r.priority.
        for op in (r.ops or []):
            sig = s._op_sig_from_op(op, priority=r.priority)

            if _is_oom_error(r.error):
                cur = s.op_ram_mult.get(sig, 1.0)
                nxt = cur * 2.0
                if nxt > 32.0:
                    nxt = 32.0
                s.op_ram_mult[sig] = nxt

                # On OOM, also slightly bump CPU to shorten retried attempts and reduce timeout risk.
                ccur = s.op_cpu_mult.get(sig, 1.0)
                cnxt = ccur * 1.10
                if cnxt > 3.0:
                    cnxt = 3.0
                s.op_cpu_mult[sig] = cnxt

            elif _is_timeout_error(r.error):
                # Timeouts likely indicate too little CPU or too much queueing; we can only control CPU here.
                cur = s.op_cpu_mult.get(sig, 1.0)
                nxt = cur * 1.35
                if nxt > 4.0:
                    nxt = 4.0
                s.op_cpu_mult[sig] = nxt

                # Keep RAM conservative on timeout (don¡¯t inflate waste); at most a tiny bump.
                rcur = s.op_ram_mult.get(sig, 1.0)
                rnxt = rcur * 1.05
                if rnxt > 2.0:
                    rnxt = 2.0
                s.op_ram_mult[sig] = rnxt

            else:
                # Unknown failure: do not thrash by endlessly retrying; if pipeline_id is available, mark dead.
                pid = getattr(r, "pipeline_id", None)
                if pid is not None:
                    s.dead_pipelines.add(pid)

    suspensions = []
    assignments = []

    # -------- schedule per pool, with soft reservations --------
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool

        # Reservations: keep headroom for higher priority arrivals.
        # - Batch must leave enough for (potential) QUERY+INTERACTIVE bursts.
        # - Interactive must leave a smaller slice for QUERY.
        reserve_for_hi_cpu = max(1.0, pool.max_cpu_pool * 0.18)
        reserve_for_hi_ram = pool.max_ram_pool * 0.10
        reserve_for_query_cpu = max(0.5, pool.max_cpu_pool * 0.06)
        reserve_for_query_ram = pool.max_ram_pool * 0.04

        # Keep placing while we can make progress.
        while avail_cpu > 0.99 and avail_ram > 0.01:
            made = False

            # 1) QUERY: no reservations (can use everything available).
            a, avail_cpu2, avail_ram2 = _try_schedule_one_from_queue(
                s.q_query, pool_id, pool, avail_cpu, avail_ram, reserve_cpu=0.0, reserve_ram=0.0, priority=Priority.QUERY
            )
            if a:
                assignments.append(a)
                avail_cpu, avail_ram = avail_cpu2, avail_ram2
                made = True

            # 2) INTERACTIVE: keep some headroom for QUERY.
            if not made:
                a, avail_cpu2, avail_ram2 = _try_schedule_one_from_queue(
                    s.q_interactive,
                    pool_id,
                    pool,
                    avail_cpu,
                    avail_ram,
                    reserve_cpu=reserve_for_query_cpu,
                    reserve_ram=reserve_for_query_ram,
                    priority=Priority.INTERACTIVE,
                )
                if a:
                    assignments.append(a)
                    avail_cpu, avail_ram = avail_cpu2, avail_ram2
                    made = True

            # 3) BATCH: keep headroom for QUERY+INTERACTIVE to protect weighted latency.
            if not made:
                a, avail_cpu2, avail_ram2 = _try_schedule_one_from_queue(
                    s.q_batch,
                    pool_id,
                    pool,
                    avail_cpu,
                    avail_ram,
                    reserve_cpu=reserve_for_hi_cpu,
                    reserve_ram=reserve_for_hi_ram,
                    priority=Priority.BATCH_PIPELINE,
                )
                if a:
                    assignments.append(a)
                    avail_cpu, avail_ram = avail_cpu2, avail_ram2
                    made = True

            if not made:
                break

    return suspensions, assignments
