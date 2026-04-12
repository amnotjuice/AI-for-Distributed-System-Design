# policy_key: scheduler_iter_best_simple_018
# context: simple
# model: gpt-5.2-2025-12-11
# llm_cost: 0.047065
# generation_seconds: 33.07
# generated_at: 2026-04-11T22:49:30.614835
@register_scheduler_init(key="scheduler_iter_best_simple_018")
def scheduler_iter_best_simple_018_init(s):
    """Priority-first scheduler tuned to reduce weighted latency.

    Improvements over naive FIFO and prior simple priority version:
      1) Strict priority gating: do not schedule BATCH if any QUERY/INTERACTIVE runnable exists.
      2) Better sizing for high-priority: give more CPU (finish sooner) and more RAM (avoid OOM retries).
      3) Learn per-operator RAM hints from past attempts:
         - On OOM-like failure: increase RAM hint aggressively.
         - On success: record RAM used as a stable hint for next time.
      4) Round-robin fairness within each priority queue to avoid head-of-line blocking.

    Intentionally not implementing preemption yet (not enough guaranteed visibility into running containers
    from the provided interface).
    """
    # Per-priority queues (round-robin)
    s.q_query = []
    s.q_interactive = []
    s.q_batch = []

    # Track pipelines that we should stop considering (non-OOM failures, if identifiable)
    s.dead_pipelines = set()

    # Per-operator RAM hint (absolute value, not multiplier): op_key -> ram_amount
    s.op_ram_hint = {}

    # Per-operator OOM backoff multiplier to apply on top of hints: op_key -> multiplier
    s.op_oom_mult = {}

    def _op_key(op, pipeline_id):
        # Prefer stable operator ids if present.
        oid = getattr(op, "op_id", None)
        if oid is None:
            oid = getattr(op, "operator_id", None)
        if oid is None:
            oid = getattr(op, "node_id", None)
        if oid is None:
            oid = id(op)
        return (pipeline_id, oid)

    s._op_key = _op_key


@register_scheduler(key="scheduler_iter_best_simple_018")
def scheduler_iter_best_simple_018_scheduler(s, results, pipelines):
    """Priority gating + learned RAM hints to reduce OOM churn and weighted latency."""
    def _is_oom_error(err) -> bool:
        if err is None:
            return False
        msg = str(err).lower()
        return ("oom" in msg) or ("out of memory" in msg) or ("killed" in msg and "memory" in msg)

    def _enqueue(p):
        if p.pipeline_id in s.dead_pipelines:
            return
        if p.priority == Priority.QUERY:
            s.q_query.append(p)
        elif p.priority == Priority.INTERACTIVE:
            s.q_interactive.append(p)
        else:
            s.q_batch.append(p)

    def _has_runnable_in_queue(q) -> bool:
        # Bounded scan; rotate items to preserve RR order.
        n = len(q)
        for _ in range(n):
            p = q.pop(0)
            if p.pipeline_id in s.dead_pipelines:
                continue
            st = p.runtime_status()
            if st.is_pipeline_successful():
                continue
            ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
            q.append(p)
            if ops:
                return True
        return False

    def _pop_runnable_from_queue(q):
        # Return (pipeline, [op]) or (None, None), with RR rotation.
        n = len(q)
        for _ in range(n):
            p = q.pop(0)
            if p.pipeline_id in s.dead_pipelines:
                continue
            st = p.runtime_status()
            if st.is_pipeline_successful():
                continue
            ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
            if ops:
                return p, ops
            q.append(p)
        return None, None

    def _choose_next_pipeline_and_op():
        # Strict priority: QUERY > INTERACTIVE > BATCH, but gate BATCH if any higher priority runnable exists.
        p, ops = _pop_runnable_from_queue(s.q_query)
        if p:
            return p, ops
        p, ops = _pop_runnable_from_queue(s.q_interactive)
        if p:
            return p, ops

        # Gate batch if there exists any runnable high-priority work (even if not popped due to RR ordering).
        if _has_runnable_in_queue(s.q_query) or _has_runnable_in_queue(s.q_interactive):
            return None, None

        return _pop_runnable_from_queue(s.q_batch)

    def _size_for(priority, pool, op, pipeline_id, avail_cpu, avail_ram):
        # CPU sizing: give more CPU to high-priority to reduce latency, but still cap to allow parallelism.
        max_cpu = pool.max_cpu_pool
        max_ram = pool.max_ram_pool

        if priority == Priority.QUERY:
            cpu_cap = min(max_cpu, 8.0)
            # Prefer finishing quickly; don't overly fragment CPU.
            cpu_floor = 2.0
            base_ram_frac = 0.30
        elif priority == Priority.INTERACTIVE:
            cpu_cap = min(max_cpu, 12.0)
            cpu_floor = 2.0
            base_ram_frac = 0.38
        else:
            cpu_cap = min(max_cpu, 16.0)
            cpu_floor = 1.0
            base_ram_frac = 0.45

        # Pick CPU: as much as possible up to cap, but ensure we leave some headroom for additional ops
        # when the pool is large. This reduces tail latency for concurrent queries.
        # Heuristic: never take more than 70% of currently available CPU for a single assignment.
        cpu = min(avail_cpu * 0.70, cpu_cap, avail_cpu)
        if cpu < cpu_floor and avail_cpu >= cpu_floor:
            cpu = cpu_floor
        # If pool is tiny, take what we can.
        if cpu <= 0:
            cpu = 0

        # RAM sizing: start from (a) learned hint, else (b) fraction of pool max,
        # then apply OOM multiplier, then clamp to available.
        op_key = s._op_key(op, pipeline_id)
        hint = s.op_ram_hint.get(op_key, None)
        if hint is None:
            hint = max_ram * base_ram_frac

        mult = s.op_oom_mult.get(op_key, 1.0)
        ram = hint * mult

        # Ensure we don't request more than pool/available; and don't request trivially small RAM.
        ram = min(ram, max_ram, avail_ram)
        if ram <= 0:
            ram = 0

        return cpu, ram, op_key

    # Ingest new pipelines
    for p in pipelines:
        _enqueue(p)

    # Early exit if nothing changed
    if not pipelines and not results:
        return [], []

    # Learn from results (RAM hints + OOM backoff)
    for r in results:
        # If we can't attribute results to ops, skip learning.
        op_list = r.ops or []
        if not op_list:
            continue

        # Try to determine pipeline_id for keying; if not present, fall back to None.
        pid = getattr(r, "pipeline_id", None)

        if r.failed():
            if _is_oom_error(r.error):
                # On OOM: increase multiplier and also bump absolute hint to at least last attempted RAM * 1.5
                for op in op_list:
                    op_key = s._op_key(op, pid)
                    cur_mult = s.op_oom_mult.get(op_key, 1.0)
                    new_mult = cur_mult * 2.0
                    if new_mult > 32.0:
                        new_mult = 32.0
                    s.op_oom_mult[op_key] = new_mult

                    attempted_ram = getattr(r, "ram", None)
                    if attempted_ram is not None and attempted_ram > 0:
                        cur_hint = s.op_ram_hint.get(op_key, 0.0)
                        bumped = attempted_ram * 1.5
                        if bumped > cur_hint:
                            s.op_ram_hint[op_key] = bumped
            else:
                # Non-OOM failure: if pipeline_id available, stop considering it to avoid wasted retries.
                if pid is not None:
                    s.dead_pipelines.add(pid)
        else:
            # On success: record RAM used as a stable hint and reset OOM multiplier.
            used_ram = getattr(r, "ram", None)
            if used_ram is not None and used_ram > 0:
                for op in op_list:
                    op_key = s._op_key(op, pid)
                    # Keep the max to be conservative (avoid OOM if sizes fluctuate)
                    prev = s.op_ram_hint.get(op_key, 0.0)
                    if used_ram > prev:
                        s.op_ram_hint[op_key] = used_ram
                    s.op_oom_mult[op_key] = 1.0

    suspensions = []
    assignments = []

    # Fill pools
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool

        # Keep packing while meaningful resources remain.
        while avail_cpu > 0.01 and avail_ram > 0.01:
            p, op_list = _choose_next_pipeline_and_op()
            if not p:
                break

            op = op_list[0]
            cpu, ram, _op_key = _size_for(p.priority, pool, op, p.pipeline_id, avail_cpu, avail_ram)

            # If we cannot fit this op now, rotate pipeline back and stop for this pool to avoid spinning.
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

            # Update local headroom
            avail_cpu -= cpu
            avail_ram -= ram

            # Round-robin fairness: put pipeline back to its priority queue
            _enqueue(p)

    return suspensions, assignments
