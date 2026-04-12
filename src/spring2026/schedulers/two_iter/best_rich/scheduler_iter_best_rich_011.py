# policy_key: scheduler_iter_best_rich_011
# context: rich
# model: gpt-5.2-2025-12-11
# llm_cost: 0.058871
# generation_seconds: 44.64
# generated_at: 2026-04-12T00:27:33.865484
@register_scheduler_init(key="scheduler_iter_best_rich_011")
def scheduler_iter_best_rich_011_init(s):
    """Priority-aware, latency-optimized scheduler (iteration 2).

    Improvements over prior simple priority FIFO:
      - Reservation-aware admission: keep CPU/RAM headroom for QUERY/INTERACTIVE so BATCH can't
        fully crowd them out (helps weighted latency).
      - Adaptive per-operator sizing:
          * RAM: start small; on OOM double; on success remember a "good" RAM and reuse it.
          * CPU: on timeout increase CPU multiplier; on success decay multiplier.
      - Queueing: strict priority with round-robin within each class to avoid starvation within class.

    Intentional scope control:
      - No container preemption (not enough stable introspection in the minimal interface).
      - No complex DAG lookahead; assign at most one runnable op per pipeline per decision.
    """
    s.q_query = []
    s.q_interactive = []
    s.q_batch = []

    # Per-operator learned hints (keyed by (pipeline_id, op_id_like))
    s.op_ram_hint = {}      # "good" RAM observed on success (ExecutionResult.ram)
    s.op_ram_mult = {}      # backoff multiplier on OOM (1,2,4,... capped)
    s.op_cpu_mult = {}      # scale-up multiplier on timeouts (1,1.5,2,... capped)

    # Avoid retry storms for clearly non-recoverable failures (best-effort; result may not expose pid)
    s.dead_pipelines = set()

    def _op_key(op, pipeline_id):
        oid = getattr(op, "op_id", None)
        if oid is None:
            oid = getattr(op, "operator_id", None)
        if oid is None:
            oid = id(op)
        return (pipeline_id, oid)

    s._op_key = _op_key


@register_scheduler(key="scheduler_iter_best_rich_011")
def scheduler_iter_best_rich_011_scheduler(s, results, pipelines):
    """See init docstring for the policy overview."""
    def _is_oom(err) -> bool:
        if err is None:
            return False
        msg = str(err).lower()
        return ("oom" in msg) or ("out of memory" in msg)

    def _is_timeout(err) -> bool:
        if err is None:
            return False
        msg = str(err).lower()
        return "timeout" in msg or "timed out" in msg

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
        # Round-robin scan: pop front; if not runnable, push back.
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
            q.append(p)
        return None, None

    def _has_waiting(priority) -> bool:
        if priority == Priority.QUERY:
            return len(s.q_query) > 0
        if priority == Priority.INTERACTIVE:
            return len(s.q_interactive) > 0
        return len(s.q_batch) > 0

    def _reserve_fractions():
        # Dynamic reservations based on which high-priority work is waiting.
        # Keep these modest to avoid collapsing throughput, but sufficient to stop
        # batch from fully occupying the pool.
        cpu_res_q = 0.0
        ram_res_q = 0.0
        cpu_res_i = 0.0
        ram_res_i = 0.0

        if _has_waiting(Priority.QUERY):
            cpu_res_q = 0.20
            ram_res_q = 0.10
        if _has_waiting(Priority.INTERACTIVE):
            cpu_res_i = 0.25
            ram_res_i = 0.15

        # Cap total reservation so we still run something even when queues build up.
        cpu_res_total = min(cpu_res_q + cpu_res_i, 0.60)
        ram_res_total = min(ram_res_q + ram_res_i, 0.50)
        return (cpu_res_q, ram_res_q, cpu_res_i, ram_res_i, cpu_res_total, ram_res_total)

    def _size_op(priority, pool, op, pipeline_id, avail_cpu, avail_ram, cpu_reserved_left, ram_reserved_left):
        # Base fractions: start RAM relatively low (previous policy over-allocated),
        # rely on OOM backoff + success hint to converge.
        if priority == Priority.QUERY:
            base_cpu_frac = 0.55
            base_ram_frac = 0.05
            cpu_cap_abs = 12.0
        elif priority == Priority.INTERACTIVE:
            base_cpu_frac = 0.65
            base_ram_frac = 0.08
            cpu_cap_abs = 16.0
        else:  # batch
            base_cpu_frac = 0.80
            base_ram_frac = 0.12
            cpu_cap_abs = 32.0

        op_key = s._op_key(op, pipeline_id)

        # CPU: use a priority-based fraction of pool, scaled by timeout backoff.
        cpu_mult = s.op_cpu_mult.get(op_key, 1.0)
        cpu_target = min(pool.max_cpu_pool * base_cpu_frac * cpu_mult, cpu_cap_abs, pool.max_cpu_pool)
        cpu = min(avail_cpu, cpu_target)

        # RAM: prefer remembered successful RAM (slightly padded), else small fraction of pool;
        # scale by OOM multiplier.
        ram_mult = s.op_ram_mult.get(op_key, 1.0)
        ram_hint = s.op_ram_hint.get(op_key, None)
        if ram_hint is not None:
            ram_target = ram_hint * 1.10
        else:
            ram_target = pool.max_ram_pool * base_ram_frac
        ram_target *= ram_mult
        ram = min(avail_ram, ram_target, pool.max_ram_pool)

        # Avoid allocating vanishing amounts that just thrash; also respect reservation for higher prio:
        # when scheduling lower priority, do not consume reserved headroom.
        if priority == Priority.BATCH_PIPELINE:
            cpu = min(cpu, max(0.0, avail_cpu - cpu_reserved_left))
            ram = min(ram, max(0.0, avail_ram - ram_reserved_left))

        # Enforce minimum useful allocations (heuristic).
        if cpu < 0.25:
            cpu = 0.0
        if ram < (0.01 * pool.max_ram_pool):
            ram = 0.0

        return cpu, ram

    # Ingest new pipelines
    for p in pipelines:
        _enqueue(p)

    if not pipelines and not results:
        return [], []

    # Process results: learn from failures/successes
    for r in results:
        # Best-effort pipeline id (may not exist on result)
        r_pid = getattr(r, "pipeline_id", None)

        if r.failed():
            # If non-OOM/non-timeout, consider it non-retryable (best effort).
            if not _is_oom(r.error) and not _is_timeout(r.error):
                if r_pid is not None:
                    s.dead_pipelines.add(r_pid)
                continue

            for op in (r.ops or []):
                op_key = s._op_key(op, r_pid)

                if _is_oom(r.error):
                    cur = s.op_ram_mult.get(op_key, 1.0)
                    nxt = cur * 2.0
                    if nxt > 16.0:
                        nxt = 16.0
                    s.op_ram_mult[op_key] = nxt
                    # If we had a hint that was too small, bump it a bit to converge faster.
                    if op_key in s.op_ram_hint and s.op_ram_hint[op_key] < (getattr(r, "ram", 0.0) or 0.0):
                        s.op_ram_hint[op_key] = getattr(r, "ram", s.op_ram_hint[op_key])

                if _is_timeout(r.error):
                    cur = s.op_cpu_mult.get(op_key, 1.0)
                    nxt = cur * 1.5
                    if nxt > 6.0:
                        nxt = 6.0
                    s.op_cpu_mult[op_key] = nxt

        else:
            # On success: record RAM used as a good hint; decay multipliers to reduce waste.
            for op in (r.ops or []):
                op_key = s._op_key(op, r_pid)
                if getattr(r, "ram", None) is not None:
                    s.op_ram_hint[op_key] = r.ram

                if op_key in s.op_ram_mult:
                    s.op_ram_mult[op_key] = max(1.0, s.op_ram_mult[op_key] * 0.8)
                    if s.op_ram_mult[op_key] <= 1.01:
                        s.op_ram_mult[op_key] = 1.0

                if op_key in s.op_cpu_mult:
                    s.op_cpu_mult[op_key] = max(1.0, s.op_cpu_mult[op_key] * 0.85)
                    if s.op_cpu_mult[op_key] <= 1.01:
                        s.op_cpu_mult[op_key] = 1.0

    suspensions = []
    assignments = []

    # Schedule per pool
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool
        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        cpu_res_q, ram_res_q, cpu_res_i, ram_res_i, cpu_res_total, ram_res_total = _reserve_fractions()

        # Convert reservation fractions to absolute "reserved left" values for this pool.
        # Reserved-left is only enforced against BATCH; QUERY/INTERACTIVE can use full avail.
        cpu_reserved_left = pool.max_cpu_pool * cpu_res_total
        ram_reserved_left = pool.max_ram_pool * ram_res_total

        # Fill the pool with repeated decisions until we can't place more.
        # Strict priority order: QUERY -> INTERACTIVE -> BATCH.
        while avail_cpu > 0.01 and avail_ram > 0.01:
            picked = False

            # Try QUERY
            p, op_list = _pop_runnable_from_queue(s.q_query)
            if p is not None:
                op = op_list[0]
                cpu, ram = _size_op(p.priority, pool, op, p.pipeline_id, avail_cpu, avail_ram, 0.0, 0.0)
                if cpu > 0 and ram > 0:
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
                    picked = True
                else:
                    # Can't place now; rotate back and stop trying to avoid spinning.
                    _enqueue(p)
                    break

            if picked:
                continue

            # Try INTERACTIVE
            p, op_list = _pop_runnable_from_queue(s.q_interactive)
            if p is not None:
                op = op_list[0]
                cpu, ram = _size_op(p.priority, pool, op, p.pipeline_id, avail_cpu, avail_ram, 0.0, 0.0)
                if cpu > 0 and ram > 0:
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
                    picked = True
                else:
                    _enqueue(p)
                    break

            if picked:
                continue

            # Try BATCH (only consume beyond reserved headroom)
            p, op_list = _pop_runnable_from_queue(s.q_batch)
            if p is None:
                break

            op = op_list[0]
            cpu, ram = _size_op(
                p.priority, pool, op, p.pipeline_id, avail_cpu, avail_ram, cpu_reserved_left, ram_reserved_left
            )
            if cpu > 0 and ram > 0:
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
                picked = True
            else:
                # Not enough non-reserved headroom; keep batch in queue and stop filling this pool
                # so we don't erode the reservation.
                _enqueue(p)
                break

    return suspensions, assignments
