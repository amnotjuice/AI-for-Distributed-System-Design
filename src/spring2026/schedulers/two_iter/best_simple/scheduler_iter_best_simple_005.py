# policy_key: scheduler_iter_best_simple_005
# context: simple
# model: gpt-5.2-2025-12-11
# llm_cost: 0.039323
# generation_seconds: 34.68
# generated_at: 2026-04-11T22:41:26.979009
@register_scheduler_init(key="scheduler_iter_best_simple_005")
def scheduler_iter_best_simple_005_init(s):
    """Iteration 2: priority-first + lighter footprints + batch admission control.

    Goals: reduce weighted latency (QUERY/INTERACTIVE) by:
      - Strict priority ordering (QUERY > INTERACTIVE > BATCH).
      - Smaller initial RAM allocations with OOM-driven RAM backoff to increase concurrency.
      - Prevent BATCH from consuming headroom when high-priority work is waiting (simple admission control).
      - Chunked CPU allocation so one op doesn't monopolize a pool and block latency-sensitive ops.

    Non-goals (kept simple intentionally):
      - No explicit preemption (needs reliable visibility into running containers beyond given interface).
      - No runtime prediction (ExecutionResult interface does not expose runtimes in the prompt).
    """
    s.q_query = []
    s.q_interactive = []
    s.q_batch = []

    # RAM backoff per operator "signature" (best-effort stable key).
    # key -> multiplier (1, 2, 4, ...)
    s.op_ram_mult = {}

    # Track pipelines we should stop retrying (non-OOM failures if detectable).
    s.dead_pipelines = set()

    def _op_key(op, pipeline_id=None):
        # Prefer stable ids if present; else object id.
        oid = getattr(op, "op_id", None)
        if oid is None:
            oid = getattr(op, "operator_id", None)
        if oid is None:
            oid = getattr(op, "name", None)
        if oid is None:
            oid = id(op)
        # Include pipeline_id when available to reduce cross-pipeline pollution.
        return (pipeline_id, oid)

    s._op_key = _op_key


@register_scheduler(key="scheduler_iter_best_simple_005")
def scheduler_iter_best_simple_005_scheduler(s, results, pipelines):
    """Priority-aware scheduler with conservative batch admission and OOM-adaptive RAM sizing."""
    # ---------------- helpers ----------------
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

    def _has_waiting_high_prio() -> bool:
        return bool(s.q_query) or bool(s.q_interactive)

    def _pop_next_runnable_from_queue(q):
        # Round-robin scan: pop front, if not runnable now, push back.
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

    def _pop_next_runnable_pipeline(allow_batch: bool):
        # Strict priority: QUERY then INTERACTIVE then (optionally) BATCH.
        p, ops = _pop_next_runnable_from_queue(s.q_query)
        if p:
            return p, ops
        p, ops = _pop_next_runnable_from_queue(s.q_interactive)
        if p:
            return p, ops
        if allow_batch:
            p, ops = _pop_next_runnable_from_queue(s.q_batch)
            if p:
                return p, ops
        return None, None

    def _ram_target_fraction(priority) -> float:
        # Smaller starting footprints increase concurrency and reduce latency.
        # OOM backoff will increase RAM only when necessary.
        if priority == Priority.QUERY:
            return 0.06
        if priority == Priority.INTERACTIVE:
            return 0.08
        return 0.10

    def _cpu_chunk(priority, pool_max_cpu) -> float:
        # Chunk CPU to avoid a single op taking the whole pool.
        # Favor QUERY slightly to finish quickly once admitted.
        if priority == Priority.QUERY:
            return min(4.0, max(1.0, pool_max_cpu * 0.35))
        if priority == Priority.INTERACTIVE:
            return min(6.0, max(1.0, pool_max_cpu * 0.30))
        return min(6.0, max(1.0, pool_max_cpu * 0.25))

    def _size_for(pool, priority, op, pipeline_id, avail_cpu, avail_ram):
        # CPU: allocate in chunks, never more than available.
        cpu = min(avail_cpu, _cpu_chunk(priority, pool.max_cpu_pool))
        if cpu < 0.01:
            cpu = 0.0

        # RAM: start small; increase only after OOM signals.
        base = pool.max_ram_pool * _ram_target_fraction(priority)
        op_key = s._op_key(op, pipeline_id)
        mult = s.op_ram_mult.get(op_key, 1.0)
        ram = base * mult

        # Clamp to available/pool max; if tiny, treat as infeasible.
        ram = min(ram, avail_ram, pool.max_ram_pool)
        if ram < 0.01:
            ram = 0.0

        return cpu, ram

    def _batch_allowed_in_pool(pool, avail_cpu, avail_ram) -> bool:
        # Admission control for batch: if high-priority exists, keep a reserve.
        if not _has_waiting_high_prio():
            return True

        # Reserve enough headroom to place at least one QUERY op quickly.
        # This is a simple guard against batch filling the pool right before a query arrives.
        cpu_reserve = min(2.0, pool.max_cpu_pool * 0.15)
        ram_reserve = pool.max_ram_pool * 0.08
        return (avail_cpu - cpu_reserve) > 0.01 and (avail_ram - ram_reserve) > 0.01

    # ---------------- ingest new pipelines ----------------
    for p in pipelines:
        _enqueue_pipeline(p)

    if not pipelines and not results:
        return [], []

    # ---------------- process results (OOM backoff + dead pipeline) ----------------
    for r in results:
        if not r.failed():
            continue

        if _is_oom_error(r.error):
            # Increase RAM multiplier for failed ops.
            for op in (r.ops or []):
                op_key = s._op_key(op, None)
                cur = s.op_ram_mult.get(op_key, 1.0)
                nxt = cur * 2.0
                if nxt > 32.0:
                    nxt = 32.0
                s.op_ram_mult[op_key] = nxt
        else:
            # Best-effort: if pipeline_id is attached, stop retrying it.
            pid = getattr(r, "pipeline_id", None)
            if pid is not None:
                s.dead_pipelines.add(pid)

    suspensions = []
    assignments = []

    # ---------------- main scheduling loop ----------------
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool

        # Keep placing work in this pool while there's headroom.
        # Use local tracking to avoid oversubscribing within this tick.
        while avail_cpu > 0.01 and avail_ram > 0.01:
            allow_batch = _batch_allowed_in_pool(pool, avail_cpu, avail_ram)
            p, op_list = _pop_next_runnable_pipeline(allow_batch=allow_batch)
            if not p:
                break

            op = op_list[0]
            cpu, ram = _size_for(pool, p.priority, op, p.pipeline_id, avail_cpu, avail_ram)

            # If we can't place this op, requeue and stop trying to fill this pool right now.
            # (Avoid spinning; next tick resources/state may change.)
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

            # Round-robin fairness within the same priority class.
            _enqueue_pipeline(p)

    return suspensions, assignments
