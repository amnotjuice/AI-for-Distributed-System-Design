# policy_key: scheduler_iter_best_simple_008
# context: simple
# model: gpt-5.2-2025-12-11
# llm_cost: 0.046785
# generation_seconds: 32.83
# generated_at: 2026-04-11T22:43:15.735546
@register_scheduler_init(key="scheduler_iter_best_simple_008")
def scheduler_iter_best_simple_008_init(s):
    """Priority-first, latency-oriented scheduler with simple reservations and OOM-aware RAM backoff.

    Improvements vs naive FIFO and prior iteration:
      - Strict priority ordering (QUERY > INTERACTIVE > BATCH) with round-robin within each class.
      - Pool headroom reservations: avoid letting BATCH consume the last slice of CPU/RAM if higher
        priorities are waiting (reduces queueing/tail latency for high-priority arrivals).
      - More aggressive sizing for high priority (finish sooner), more conservative for batch.
      - OOM-aware RAM multiplier per (pipeline, op) with exponential backoff; non-OOM failures are dropped.
      - Duplicate-avoidance for queued pipelines via a membership set.

    Intentionally still simple:
      - No preemption (requires reliable visibility into running containers/containers list).
      - No runtime prediction / SRPT (not available in the minimal interface).
    """
    s.q_query = []
    s.q_interactive = []
    s.q_batch = []

    # Track membership to prevent queue blow-ups due to repeated re-enqueue.
    s.in_q_query = set()
    s.in_q_interactive = set()
    s.in_q_batch = set()

    # Pipelines with non-OOM failures we stop retrying.
    s.dead_pipelines = set()

    # Adaptive RAM multiplier per operator: (pipeline_id, op_idish) -> multiplier
    s.op_ram_mult = {}

    def _op_key(op, pipeline_id):
        oid = getattr(op, "op_id", None)
        if oid is None:
            oid = getattr(op, "operator_id", None)
        if oid is None:
            oid = id(op)
        return (pipeline_id, oid)

    s._op_key = _op_key


@register_scheduler(key="scheduler_iter_best_simple_008")
def scheduler_iter_best_simple_008_scheduler(s, results, pipelines):
    """See init docstring for overview."""
    # -------- helpers --------
    def _is_oom_error(err) -> bool:
        if err is None:
            return False
        msg = str(err).lower()
        return ("oom" in msg) or ("out of memory" in msg) or ("killed" in msg and "memory" in msg)

    def _sets_for_priority(prio):
        if prio == Priority.QUERY:
            return s.q_query, s.in_q_query
        if prio == Priority.INTERACTIVE:
            return s.q_interactive, s.in_q_interactive
        return s.q_batch, s.in_q_batch

    def _enqueue(p):
        if p is None:
            return
        if p.pipeline_id in s.dead_pipelines:
            return
        q, in_q = _sets_for_priority(p.priority)
        if p.pipeline_id in in_q:
            return
        q.append(p)
        in_q.add(p.pipeline_id)

    def _dequeue_front(q, in_q):
        p = q.pop(0)
        in_q.discard(p.pipeline_id)
        return p

    def _has_waiting_high_prio():
        return len(s.q_query) > 0 or len(s.q_interactive) > 0

    def _pop_next_runnable(allowed_priorities):
        # Round-robin scan within each allowed priority queue; return first runnable pipeline+op_list.
        # If not runnable now, rotate it to the back.
        for prio in allowed_priorities:
            q, in_q = _sets_for_priority(prio)
            if not q:
                continue
            n = len(q)
            for _ in range(n):
                p = _dequeue_front(q, in_q)
                if p.pipeline_id in s.dead_pipelines:
                    continue
                st = p.runtime_status()
                if st.is_pipeline_successful():
                    continue
                op_list = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
                if op_list:
                    return p, op_list
                # Not runnable yet, rotate.
                _enqueue(p)
        return None, None

    def _reserve_for_high_prio(pool):
        # Reservation sizes are small but enough to absorb bursts without letting batch fill the pool.
        # Clamp to pool max to avoid nonsense.
        max_cpu = pool.max_cpu_pool
        max_ram = pool.max_ram_pool
        # Keep at least 1 vCPU and 8% RAM free when any high-priority work is waiting.
        res_cpu = min(1.0, max_cpu * 0.25)
        res_ram = max_ram * 0.08
        if res_cpu < 0:
            res_cpu = 0.0
        if res_ram < 0:
            res_ram = 0.0
        return res_cpu, res_ram

    def _size_for(priority, pool, op, pipeline_id, avail_cpu, avail_ram):
        max_cpu = pool.max_cpu_pool
        max_ram = pool.max_ram_pool

        # CPU sizing:
        # - For QUERY: give a bigger slice to finish quickly (reduces weighted latency).
        # - For INTERACTIVE: moderate slice.
        # - For BATCH: conservative slice to avoid harming high-priority tail latency.
        if priority == Priority.QUERY:
            cpu_cap = min(max(2.0, max_cpu * 0.60), max_cpu)
            ram_frac = 0.25
        elif priority == Priority.INTERACTIVE:
            cpu_cap = min(max(2.0, max_cpu * 0.45), max_cpu)
            ram_frac = 0.22
        else:
            cpu_cap = min(max(1.0, max_cpu * 0.30), max_cpu)
            ram_frac = 0.35

        # Apply OOM backoff for RAM. (RAM beyond minimum doesn't speed up, but avoids retries.)
        op_key = s._op_key(op, pipeline_id)
        mult = s.op_ram_mult.get(op_key, 1.0)

        cpu = min(avail_cpu, cpu_cap)
        if cpu < 0:
            cpu = 0.0

        base_ram = max_ram * ram_frac
        ram = base_ram * mult
        ram = min(ram, avail_ram, max_ram)
        if ram < 0:
            ram = 0.0

        # Enforce small minimums to avoid allocating tiny, unrealistic slices.
        if cpu > 0.0 and cpu < 0.5:
            cpu = min(avail_cpu, 0.5)
        if ram > 0.0 and ram < max_ram * 0.02:
            ram = min(avail_ram, max_ram * 0.02)

        return cpu, ram

    # -------- ingest new pipelines --------
    for p in pipelines:
        _enqueue(p)

    if not pipelines and not results:
        return [], []

    # -------- process results for OOM backoff / failure handling --------
    for r in results:
        if not r.failed():
            continue

        # OOM: increase RAM multiplier for the failed operators to reduce future retries.
        if _is_oom_error(r.error):
            # If we can't recover pipeline_id from result, we still try to key by (None, op_idish).
            pid = getattr(r, "pipeline_id", None)
            for op in (r.ops or []):
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

    # -------- scheduling: fill pools, but protect high-priority headroom --------
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]

        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool

        # Continue assigning while we have meaningful capacity.
        while avail_cpu > 0.01 and avail_ram > 0.01:
            # Decide what priorities are allowed to consume resources right now.
            # If high-priority work exists, allow it freely; restrict BATCH from consuming reserved headroom.
            high_waiting = _has_waiting_high_prio()
            res_cpu, res_ram = _reserve_for_high_prio(pool)

            allowed = [Priority.QUERY, Priority.INTERACTIVE]
            if not high_waiting:
                allowed.append(Priority.BATCH_PIPELINE)

            # First try to schedule high priorities; if none runnable and no high waiting, schedule batch.
            p, op_list = _pop_next_runnable(allowed)
            if not p:
                # If nothing runnable among high prio, we may still run batch (even if high_waiting) only if
                # we can do so without consuming reserved headroom.
                if high_waiting:
                    # No runnable high prio now (likely dependencies), avoid chewing reserved headroom.
                    break
                p, op_list = _pop_next_runnable([Priority.BATCH_PIPELINE])
                if not p:
                    break

            # Apply reservations only when scheduling batch and high-priority is waiting.
            eff_avail_cpu = avail_cpu
            eff_avail_ram = avail_ram
            if p.priority == Priority.BATCH_PIPELINE and high_waiting:
                eff_avail_cpu = max(0.0, avail_cpu - res_cpu)
                eff_avail_ram = max(0.0, avail_ram - res_ram)

            if eff_avail_cpu <= 0.01 or eff_avail_ram <= 0.01:
                # Can't schedule this class without violating reservations.
                _enqueue(p)
                break

            op = op_list[0]
            cpu, ram = _size_for(p.priority, pool, op, p.pipeline_id, eff_avail_cpu, eff_avail_ram)

            if cpu <= 0.01 or ram <= 0.01:
                # Not enough headroom; put it back and stop on this pool to avoid spin.
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

            # Update remaining headroom and re-enqueue pipeline for RR fairness.
            avail_cpu -= cpu
            avail_ram -= ram
            _enqueue(p)

    return suspensions, assignments
