# policy_key: scheduler_iter_best_simple_015
# context: simple
# model: gpt-5.2-2025-12-11
# llm_cost: 0.044209
# generation_seconds: 38.15
# generated_at: 2026-04-11T22:47:28.407615
@register_scheduler_init(key="scheduler_iter_best_simple_015")
def scheduler_iter_best_simple_015_init(s):
    """Priority-first scheduler tuned to reduce weighted latency (simple, incremental improvements).

    Improvements over the previous iteration:
      - Strict priority with "leftovers for batch": never schedule BATCH if QUERY/INTERACTIVE runnable.
      - Smaller, backlog-aware CPU caps for high-priority to increase concurrency (reduce queueing delay).
      - OOM handling switched to absolute per-op RAM targets (based on last attempted RAM), not a pool-fraction multiplier.
      - Pool selection: try to place high-priority work on the pool with the most headroom first.
      - Round-robin fairness within each priority queue to avoid head-of-line blocking.

    Intentionally still simple:
      - No preemption (not enough supported surface in the provided template).
      - One operator per assignment (consistent with the starter policy).
    """
    s.q_query = []
    s.q_interactive = []
    s.q_batch = []

    # Per-operator absolute RAM target for retries after OOM: op_key -> ram
    s.op_ram_target = {}

    # Track repeated non-OOM failures per pipeline to stop wasting cycles (best-effort).
    s.pipeline_fail_count = {}

    def _op_key(op, pipeline_id):
        oid = getattr(op, "op_id", None)
        if oid is None:
            oid = getattr(op, "operator_id", None)
        if oid is None:
            oid = id(op)
        return (pipeline_id, oid)

    s._op_key = _op_key


@register_scheduler(key="scheduler_iter_best_simple_015")
def scheduler_iter_best_simple_015_scheduler(s, results, pipelines):
    """Latency-oriented, priority-aware scheduler with OOM RAM backoff and backlog-aware CPU sizing."""
    # --- helpers ---
    def _is_oom_error(err) -> bool:
        if err is None:
            return False
        msg = str(err).lower()
        return ("oom" in msg) or ("out of memory" in msg) or ("memory" in msg and ("exceed" in msg or "limit" in msg))

    def _enqueue_pipeline(p):
        if p.priority == Priority.QUERY:
            s.q_query.append(p)
        elif p.priority == Priority.INTERACTIVE:
            s.q_interactive.append(p)
        else:
            s.q_batch.append(p)

    def _queue_for_priority(pri):
        if pri == Priority.QUERY:
            return s.q_query
        if pri == Priority.INTERACTIVE:
            return s.q_interactive
        return s.q_batch

    def _count_runnable_in_queue(q):
        # Approximate backlog pressure; bounded scan for efficiency.
        cnt = 0
        n = len(q)
        scan = n if n < 12 else 12
        for i in range(scan):
            p = q[i]
            st = p.runtime_status()
            if st.is_pipeline_successful():
                continue
            ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
            if ops:
                cnt += 1
        return cnt

    def _pop_next_runnable_from_queue(q):
        # Round-robin: pop front, if not runnable rotate to back.
        if not q:
            return None, None
        n = len(q)
        for _ in range(n):
            p = q.pop(0)
            st = p.runtime_status()

            # Drop completed pipelines eagerly.
            if st.is_pipeline_successful():
                continue

            # If pipeline has failed ops and we have seen repeated non-OOM failures, drop it.
            fail_cnt = s.pipeline_fail_count.get(p.pipeline_id, 0)
            if fail_cnt >= 3:
                # Don't keep it around.
                continue

            op_list = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
            if op_list:
                return p, op_list

            # Not runnable yet; rotate.
            q.append(p)
        return None, None

    def _candidate_pools_by_headroom():
        # Prefer pools with more available CPU; tie-break with RAM.
        idxs = list(range(s.executor.num_pools))
        idxs.sort(
            key=lambda i: (s.executor.pools[i].avail_cpu_pool, s.executor.pools[i].avail_ram_pool),
            reverse=True,
        )
        return idxs

    def _size_cpu(priority, pool, backlog_runnable, avail_cpu):
        # Backlog-aware CPU sizing: when multiple high-priority ops are waiting,
        # keep per-op CPU smaller to increase parallelism and reduce queueing latency.
        max_cpu = pool.max_cpu_pool

        if priority == Priority.QUERY:
            base = 2.0 if backlog_runnable >= 2 else 4.0
            cap = min(base, max_cpu * 0.50, 8.0)
        elif priority == Priority.INTERACTIVE:
            base = 3.0 if backlog_runnable >= 2 else 6.0
            cap = min(base, max_cpu * 0.60, 10.0)
        else:
            # Batch only uses leftovers; can be larger, but don't monopolize completely.
            cap = min(12.0, max_cpu * 0.75)

        cpu = min(avail_cpu, cap)
        if cpu < 0:
            cpu = 0.0
        return cpu

    def _size_ram(priority, pool, op, pipeline_id, avail_ram):
        # Use absolute per-op RAM target if known (after OOM); otherwise choose a small default.
        max_ram = pool.max_ram_pool
        op_key = s._op_key(op, pipeline_id)

        # Default RAM: keep small to improve concurrency; OOM will quickly raise targets.
        if priority == Priority.QUERY:
            default_ram = max_ram * 0.12
        elif priority == Priority.INTERACTIVE:
            default_ram = max_ram * 0.18
        else:
            default_ram = max_ram * 0.30

        target = s.op_ram_target.get(op_key, default_ram)

        # Clamp to pool and current availability.
        ram = min(target, max_ram, avail_ram)
        if ram < 0:
            ram = 0.0
        return ram

    # --- ingest pipelines ---
    for p in pipelines:
        _enqueue_pipeline(p)

    if not pipelines and not results:
        return [], []

    # --- process results for OOM backoff / failure accounting ---
    for r in results:
        if not r.failed():
            # On success, do nothing; keeping RAM targets is usually beneficial (avoid future OOM retries).
            continue

        if _is_oom_error(r.error):
            # Increase RAM target based on the last attempted RAM. We may not know pipeline_id here;
            # update using best-effort keys. If pipeline_id isn't on result, use (None, op_id) fallback.
            for op in (r.ops or []):
                pid = getattr(r, "pipeline_id", None)
                op_key = s._op_key(op, pid)
                if op_key[0] is None:
                    op_key = (None, op_key[1])

                cur = s.op_ram_target.get(op_key, max(1.0, float(getattr(r, "ram", 1.0))))
                last = float(getattr(r, "ram", cur))
                # Backoff: at least double the last attempted RAM, and at least 1.5x current target.
                nxt = max(last * 2.0, cur * 1.5)
                # Hard cap to avoid absurd targets.
                if hasattr(s.executor, "pools") and s.executor.pools:
                    # Use largest pool max RAM as a safe ceiling in multi-pool setups.
                    ceiling = max(pool.max_ram_pool for pool in s.executor.pools)
                    if nxt > ceiling:
                        nxt = ceiling
                s.op_ram_target[op_key] = nxt
        else:
            # Non-OOM failure: increment failure count if pipeline_id is available.
            pid = getattr(r, "pipeline_id", None)
            if pid is not None:
                s.pipeline_fail_count[pid] = s.pipeline_fail_count.get(pid, 0) + 1

    suspensions = []
    assignments = []

    # --- scheduling strategy ---
    # Strict priority: schedule QUERY first, then INTERACTIVE, then BATCH.
    # Additionally, only schedule BATCH if no runnable QUERY/INTERACTIVE exists at scheduling time.
    pool_order = _candidate_pools_by_headroom()

    # Precompute runnable backlog signals (cheap, approximate).
    q_backlog = _count_runnable_in_queue(s.q_query)
    i_backlog = _count_runnable_in_queue(s.q_interactive)

    # First pass: schedule high priority across best pools.
    for pool_id in pool_order:
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool

        while avail_cpu > 0.01 and avail_ram > 0.01:
            # Try QUERY then INTERACTIVE.
            p, op_list = _pop_next_runnable_from_queue(s.q_query)
            backlog = q_backlog
            if not p:
                p, op_list = _pop_next_runnable_from_queue(s.q_interactive)
                backlog = i_backlog
            if not p:
                break

            op = op_list[0]
            cpu = _size_cpu(p.priority, pool, backlog, avail_cpu)
            ram = _size_ram(p.priority, pool, op, p.pipeline_id, avail_ram)

            # If we can't allocate, rotate pipeline back and stop for this pool (headroom too small).
            if cpu <= 0.01 or ram <= 0.01:
                _enqueue_pipeline(p)
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
            # Rotate pipeline back for fairness.
            _enqueue_pipeline(p)

    # Second pass: schedule BATCH only if there is no runnable high-priority work.
    # This reduces interference and improves weighted latency when weights favor high priority.
    # (We re-check runnable existence now, not relying solely on backlog counts.)
    def _any_runnable(q):
        if not q:
            return False
        n = len(q)
        scan = n if n < 20 else 20
        for i in range(scan):
            p = q[i]
            st = p.runtime_status()
            if st.is_pipeline_successful():
                continue
            if st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True):
                return True
        return False

    if not _any_runnable(s.q_query) and not _any_runnable(s.q_interactive):
        for pool_id in pool_order:
            pool = s.executor.pools[pool_id]
            avail_cpu = pool.avail_cpu_pool
            avail_ram = pool.avail_ram_pool

            while avail_cpu > 0.01 and avail_ram > 0.01:
                p, op_list = _pop_next_runnable_from_queue(s.q_batch)
                if not p:
                    break

                op = op_list[0]
                cpu = _size_cpu(p.priority, pool, backlog_runnable=0, avail_cpu=avail_cpu)
                ram = _size_ram(p.priority, pool, op, p.pipeline_id, avail_ram)

                if cpu <= 0.01 or ram <= 0.01:
                    _enqueue_pipeline(p)
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
                _enqueue_pipeline(p)

    return suspensions, assignments
