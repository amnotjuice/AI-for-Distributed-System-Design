# policy_key: scheduler_iter_best_simple_019
# context: simple
# model: gpt-5.2-2025-12-11
# llm_cost: 0.041059
# generation_seconds: 40.67
# generated_at: 2026-04-11T22:50:11.289977
@register_scheduler_init(key="scheduler_iter_best_simple_019")
def scheduler_iter_best_simple_019_init(s):
    """Priority-aware, latency-optimized scheduler (incremental improvement).

    Improvements over the previous attempt:
      1) Stronger priority isolation via *explicit reservation* of CPU/RAM for QUERY work:
         - If QUERY backlog exists, we keep headroom by limiting INTERACTIVE/BATCH packing.
      2) Higher parallelism for latency: smaller, adaptive per-op CPU targets for QUERY when backlog is high.
      3) OOM-aware RAM backoff *plus* success-based RAM reuse:
         - On success, remember the RAM that worked for the operator and re-use it as a floor.
         - On OOM-like failures, exponentially increase RAM multiplier for that operator.
      4) Simple aging within each priority class (FIFO by arrival into the scheduler queues).

    Intentionally still avoids preemption (not enough reliable visibility of running containers in the template).
    """
    # Tick counter for deterministic, stable aging if needed later.
    s.tick = 0

    # Per-priority waiting queues. We keep (enqueue_tick, pipeline) for FIFO/aging.
    s.q_query = []
    s.q_interactive = []
    s.q_batch = []

    # Operator RAM learning:
    # - mult: exponential backoff when OOM occurs
    # - last_good_ram: floor to reduce repeated OOMs after one success
    s.op_ram_mult = {}       # op_key -> float
    s.op_last_good_ram = {}  # op_key -> float

    # Pipeline arrival tick (for stability if pipelines get re-enqueued).
    s.pipeline_first_seen = {}  # pipeline_id -> tick

    def _op_key(op):
        # Prefer stable operator identifiers if present; else fall back to object identity.
        oid = getattr(op, "op_id", None)
        if oid is None:
            oid = getattr(op, "operator_id", None)
        if oid is None:
            oid = id(op)
        return oid

    s._op_key = _op_key


@register_scheduler(key="scheduler_iter_best_simple_019")
def scheduler_iter_best_simple_019_scheduler(s, results, pipelines):
    """Priority-first scheduling with reserved headroom for QUERY, adaptive CPU sizing, and RAM learning."""
    # Local import only inside function (per instructions).
    from math import isfinite

    s.tick += 1

    def _is_oom_error(err) -> bool:
        if err is None:
            return False
        msg = str(err).lower()
        return ("oom" in msg) or ("out of memory" in msg) or ("killed" in msg and "memory" in msg)

    def _enqueue(p):
        # Record first-seen tick for stable ordering if needed.
        if p.pipeline_id not in s.pipeline_first_seen:
            s.pipeline_first_seen[p.pipeline_id] = s.tick

        item = (s.pipeline_first_seen[p.pipeline_id], p)
        if p.priority == Priority.QUERY:
            s.q_query.append(item)
        elif p.priority == Priority.INTERACTIVE:
            s.q_interactive.append(item)
        else:
            s.q_batch.append(item)

    def _cleanup_and_pick_op(q):
        """Pop the next runnable (pipeline, op_list) from a single queue; rotate non-runnable to back."""
        if not q:
            return None, None, None

        n = len(q)
        for _ in range(n):
            enq_tick, p = q.pop(0)
            st = p.runtime_status()
            if st.is_pipeline_successful():
                continue

            op_list = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
            if op_list:
                return enq_tick, p, op_list

            # Not runnable yet; rotate to back.
            q.append((enq_tick, p))

        return None, None, None

    def _has_query_backlog() -> bool:
        # True if any query pipeline currently has an assignable op ready (or likely soon).
        # Cheap check: non-empty queue. (We avoid heavy scanning.)
        return len(s.q_query) > 0

    def _cpu_target_for(priority, pool, query_backlog_len):
        # Heuristic CPU targets to trade off latency (parallelism) vs per-op speed.
        max_cpu = float(pool.max_cpu_pool)

        if priority == Priority.QUERY:
            # If many queries, push toward 1 vCPU to increase concurrency.
            if query_backlog_len >= max(4, int(max_cpu)):
                return 1.0
            return min(2.0, max(1.0, max_cpu * 0.125))
        if priority == Priority.INTERACTIVE:
            return min(4.0, max(2.0, max_cpu * 0.20))
        # Batch: allow bigger chunks only when not starving higher priority
        return min(8.0, max(2.0, max_cpu * 0.35))

    def _reserve_for_queries(pool, query_backlog_present: bool):
        # When queries exist, reserve a small fixed share of CPU/RAM so we can admit queries quickly.
        if not query_backlog_present:
            return 0.0, 0.0
        reserve_cpu = min(2.0, float(pool.max_cpu_pool) * 0.25)
        reserve_ram = float(pool.max_ram_pool) * 0.20
        return reserve_cpu, reserve_ram

    def _ram_request(priority, pool, avail_ram, op):
        # Baseline RAM fractions; bigger for batch to reduce OOM churn, but bounded by learning.
        max_ram = float(pool.max_ram_pool)
        if priority == Priority.QUERY:
            base = max_ram * 0.12
        elif priority == Priority.INTERACTIVE:
            base = max_ram * 0.20
        else:
            base = max_ram * 0.32

        opk = s._op_key(op)
        mult = s.op_ram_mult.get(opk, 1.0)
        last_good = s.op_last_good_ram.get(opk, 0.0)

        # Prefer last known good RAM to avoid repeating failures; then apply multiplier.
        ram = max(base, last_good) * mult

        # Sanity clamps.
        if not isfinite(ram) or ram <= 0:
            ram = base
        ram = min(ram, avail_ram, max_ram)
        return ram

    # Ingest new pipelines.
    for p in pipelines:
        _enqueue(p)

    # Process results to learn RAM needs.
    for r in results:
        # Learn from both success and failure (especially OOM).
        ops = (r.ops or [])
        if r.failed():
            if _is_oom_error(r.error):
                for op in ops:
                    opk = s._op_key(op)
                    cur = s.op_ram_mult.get(opk, 1.0)
                    nxt = cur * 2.0
                    if nxt > 16.0:
                        nxt = 16.0
                    s.op_ram_mult[opk] = nxt
            # Non-OOM failures: don't special-case; pipeline logic will handle failed state.
        else:
            # On success, record the RAM that worked for these ops; also gently decay multiplier.
            for op in ops:
                opk = s._op_key(op)
                if getattr(r, "ram", None) is not None:
                    worked = float(r.ram)
                    if isfinite(worked) and worked > 0:
                        prev = s.op_last_good_ram.get(opk, 0.0)
                        # Keep the max of seen "worked" RAM to reduce oscillation.
                        if worked > prev:
                            s.op_last_good_ram[opk] = worked
                # Mild decay so we don't stay over-provisioned forever.
                cur = s.op_ram_mult.get(opk, 1.0)
                if cur > 1.0:
                    s.op_ram_mult[opk] = max(1.0, cur * 0.85)

    # Early exit if nothing changed.
    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    query_backlog_present = _has_query_backlog()
    query_backlog_len = len(s.q_query)

    # Schedule per pool: always try QUERY then INTERACTIVE then BATCH, with reservations.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)

        reserve_cpu, reserve_ram = _reserve_for_queries(pool, query_backlog_present)

        # Keep placing while we have headroom.
        while avail_cpu > 0.01 and avail_ram > 0.01:
            picked = None

            # Priority order selection: try to pick a runnable op from highest priority queue first.
            for q in (s.q_query, s.q_interactive, s.q_batch):
                enq_tick, p, op_list = _cleanup_and_pick_op(q)
                if p is None:
                    continue

                prio = p.priority

                # If not QUERY, respect reserved headroom when queries are waiting.
                if query_backlog_present and prio != Priority.QUERY:
                    # If we would dip into reserved capacity, don't schedule lower priorities.
                    if (avail_cpu - reserve_cpu) <= 0.01 or (avail_ram - reserve_ram) <= 0.01:
                        # Put it back and stop filling this pool for now.
                        q.append((enq_tick, p))
                        picked = None
                        break

                picked = (q, enq_tick, p, op_list)
                break

            if not picked:
                break

            q, enq_tick, p, op_list = picked
            op = op_list[0]

            # Size the assignment.
            cpu_tgt = _cpu_target_for(p.priority, pool, query_backlog_len)
            cpu = min(avail_cpu, cpu_tgt)

            ram = _ram_request(p.priority, pool, avail_ram, op)

            # If we can't allocate meaningful resources, requeue and stop on this pool.
            if cpu <= 0.01 or ram <= 0.01:
                q.append((enq_tick, p))
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

            # Update local headroom.
            avail_cpu -= cpu
            avail_ram -= ram

            # Requeue pipeline (for fairness / next ops) at the back of its same priority queue.
            q.append((enq_tick, p))

    return suspensions, assignments
