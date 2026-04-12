# policy_key: scheduler_iter_best_simple_003
# context: simple
# model: gpt-5.2-2025-12-11
# llm_cost: 0.058867
# generation_seconds: 34.38
# generated_at: 2026-04-11T22:40:20.810095
@register_scheduler_init(key="scheduler_iter_best_simple_003")
def scheduler_iter_best_simple_003_init(s):
    """Priority-first, latency-oriented scheduler with pool reservation + strict gating.

    Incremental improvements over the prior priority-queue version:
      - Strict priority gating: never schedule BATCH if any QUERY/INTERACTIVE runnable exists.
      - Pool reservation: keep headroom in each pool for QUERY/INTERACTIVE to reduce queueing delay.
      - Simple pool preference: prefer running high-priority work in a designated "fast lane" pool.
      - OOM-aware RAM backoff retained, but keyed robustly via operator object identity.

    Intentionally still simple:
      - No preemption (requires visibility/control of running containers beyond provided template).
      - No runtime prediction; uses conservative sizing heuristics.
    """
    s.q_query = []
    s.q_interactive = []
    s.q_batch = []

    # Mark pipelines to stop considering (non-OOM repeated failures, if pipeline_id available).
    s.dead_pipelines = set()

    # OOM backoff: (pipeline_id, op_identity) -> multiplier
    s.op_ram_mult = {}

    # Track last-seen priority arrival time ticks (optional future aging; kept for stability).
    s._tick = 0

    def _op_ident(op):
        # Prefer stable ids if present; else use object id.
        oid = getattr(op, "op_id", None)
        if oid is None:
            oid = getattr(op, "operator_id", None)
        if oid is None:
            oid = id(op)
        return oid

    s._op_ident = _op_ident


@register_scheduler(key="scheduler_iter_best_simple_003")
def scheduler_iter_best_simple_003_scheduler(s, results, pipelines):
    """Scheduler step: strict priority + headroom reservation to reduce weighted latency."""
    s._tick += 1

    # --- helpers ---
    def _is_oom_error(err) -> bool:
        if err is None:
            return False
        msg = str(err).lower()
        return ("oom" in msg) or ("out of memory" in msg) or ("memory" in msg and ("exceed" in msg or "limit" in msg))

    def _enqueue_pipeline(p):
        if p.pipeline_id in s.dead_pipelines:
            return
        if p.priority == Priority.QUERY:
            s.q_query.append(p)
        elif p.priority == Priority.INTERACTIVE:
            s.q_interactive.append(p)
        else:
            s.q_batch.append(p)

    def _pipeline_has_runnable_op(p) -> bool:
        st = p.runtime_status()
        if st.is_pipeline_successful():
            return False
        op_list = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        return bool(op_list)

    def _pop_next_runnable_from_queue(q):
        # Round-robin scan: find first runnable; rotate non-runnable to back.
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

    def _has_any_runnable_high_pri() -> bool:
        # Cheap check: scan queues; bounded by queue sizes.
        for q in (s.q_query, s.q_interactive):
            for p in q:
                if p.pipeline_id in s.dead_pipelines:
                    continue
                if _pipeline_has_runnable_op(p):
                    return True
        return False

    def _pool_preference_order(priority):
        # "Fast lane": pool 0 preferred for QUERY/INTERACTIVE if multiple pools.
        if s.executor.num_pools <= 1:
            return [0]
        if priority in (Priority.QUERY, Priority.INTERACTIVE):
            return [0] + [i for i in range(1, s.executor.num_pools)]
        # Batch prefers non-fast-lane pools first to reduce interference.
        return [i for i in range(1, s.executor.num_pools)] + [0]

    def _headroom_reserved(pool, priority):
        # Reserve resources in each pool for high-priority arrivals.
        # This helps without preemption by preventing batch from consuming the last headroom.
        if priority == Priority.BATCH_PIPELINE:
            # If there is any high-priority runnable work, reserve aggressively.
            if _has_any_runnable_high_pri():
                return pool.max_cpu_pool * 0.50, pool.max_ram_pool * 0.50
            # Otherwise reserve a small amount to keep interactive responsiveness.
            return pool.max_cpu_pool * 0.15, pool.max_ram_pool * 0.15
        # For high priority, no reservation (they *are* the reservation users).
        return 0.0, 0.0

    def _size_for(pool, priority, op, pipeline_id, avail_cpu, avail_ram):
        # Aim: finish QUERY/INTERACTIVE quickly while still leaving some parallelism;
        # keep BATCH allocations smaller (especially when high priority exists).
        max_cpu = pool.max_cpu_pool
        max_ram = pool.max_ram_pool

        if priority == Priority.QUERY:
            cpu_cap = min(max(2.0, max_cpu * 0.50), 16.0)
            ram_frac = 0.25
        elif priority == Priority.INTERACTIVE:
            cpu_cap = min(max(2.0, max_cpu * 0.40), 16.0)
            ram_frac = 0.30
        else:
            # Batch: small slices to avoid blocking (and to reduce wasted work on OOM retries).
            cpu_cap = min(max(1.0, max_cpu * 0.20), 8.0)
            ram_frac = 0.25

        cpu = min(avail_cpu, cpu_cap)

        # OOM backoff multiplier per operator.
        op_key = (pipeline_id, s._op_ident(op))
        mult = s.op_ram_mult.get(op_key, 1.0)

        ram = min(avail_ram, max_ram * ram_frac * mult, max_ram)

        return cpu, ram

    # --- ingest new pipelines ---
    for p in pipelines:
        _enqueue_pipeline(p)

    # Early exit if nothing changed.
    if not pipelines and not results:
        return [], []

    # --- process results: OOM backoff / dead pipeline marking ---
    for r in results:
        if not r.failed():
            continue

        # If it's an OOM, increase RAM multiplier for those ops so retries have a better chance.
        if _is_oom_error(r.error):
            # Use operator object identity and try to recover pipeline_id if present on result.
            pid = getattr(r, "pipeline_id", None)
            for op in (r.ops or []):
                key = (pid, s._op_ident(op))
                cur = s.op_ram_mult.get(key, 1.0)
                nxt = cur * 2.0
                if nxt > 16.0:
                    nxt = 16.0
                s.op_ram_mult[key] = nxt
        else:
            pid = getattr(r, "pipeline_id", None)
            if pid is not None:
                s.dead_pipelines.add(pid)

    suspensions = []
    assignments = []

    # --- scheduling: strict priority gating + pool reservations ---
    # We schedule in global priority order, but respect pool preference to reduce interference.
    # We iterate priorities and attempt to place work into pools in preferred order.
    priorities = [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]
    prio_to_queue = {
        Priority.QUERY: s.q_query,
        Priority.INTERACTIVE: s.q_interactive,
        Priority.BATCH_PIPELINE: s.q_batch,
    }

    # Precompute if there is any runnable high-priority work (for batch gating).
    high_pri_runnable = _has_any_runnable_high_pri()

    for prio in priorities:
        # Strict gating: if any high-priority runnable exists, don't schedule batch at all.
        if prio == Priority.BATCH_PIPELINE and high_pri_runnable:
            continue

        q = prio_to_queue[prio]
        if not q:
            continue

        for pool_id in _pool_preference_order(prio):
            pool = s.executor.pools[pool_id]

            # Effective available resources after reservation (only relevant for batch).
            reserve_cpu, reserve_ram = _headroom_reserved(pool, prio)
            eff_avail_cpu = max(0.0, pool.avail_cpu_pool - reserve_cpu)
            eff_avail_ram = max(0.0, pool.avail_ram_pool - reserve_ram)

            # Keep scheduling into this pool while we can.
            while eff_avail_cpu > 0.01 and eff_avail_ram > 0.01 and q:
                p, op_list = _pop_next_runnable_from_queue(q)
                if not p:
                    break

                op = op_list[0]
                cpu, ram = _size_for(pool, prio, op, p.pipeline_id, eff_avail_cpu, eff_avail_ram)

                # If we can't allocate a meaningful slice, put it back and stop on this pool.
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

                # Update effective headroom for additional placements.
                eff_avail_cpu -= cpu
                eff_avail_ram -= ram

                # Fairness within the same priority: rotate pipeline back.
                _enqueue_pipeline(p)

    return suspensions, assignments
