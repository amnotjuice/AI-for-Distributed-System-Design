# policy_key: scheduler_iter_median_rich_017
# context: rich
# model: gpt-5.2-2025-12-11
# llm_cost: 0.069864
# generation_seconds: 37.80
# generated_at: 2026-04-12T01:56:57.413730
@register_scheduler_init(key="scheduler_iter_median_rich_017")
def scheduler_iter_median_rich_017_init(s):
    """Iteration 2: Priority queues + per-operator RAM learning + conservative default RAM.

    Directional changes vs previous attempt:
    - Reduce unnecessary RAM hoarding (mean allocated >> mean consumed in stats) by starting small.
    - Learn RAM requirements per operator from OOM failures using ExecutionResult (op-level hints).
    - Keep strict priority ordering (QUERY > INTERACTIVE > BATCH), but avoid batch starving by
      allowing batch on "non-reserved" pools and when high-priority queues are empty.
    - Maintain simple round-robin within each priority queue for fairness.

    Notes / constraints:
    - We avoid suspensions/preemption because the minimal public API doesn't expose running
      containers safely beyond ExecutionResult callbacks.
    - We schedule one ready op per pipeline per pick to reduce head-of-line blocking.
    """
    s.queues = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }
    s.rr_cursor = {
        Priority.QUERY: 0,
        Priority.INTERACTIVE: 0,
        Priority.BATCH_PIPELINE: 0,
    }

    # Operator-level RAM hints learned from OOMs.
    # key: op_signature (stable-ish string), value: hinted_ram (float)
    s.op_ram_hint = {}

    # Pipeline-level failure handling (non-OOM failures are not retried).
    # key: pipeline_id, value: bool
    s.pipeline_drop = {}

    # Coarse per-priority RAM multipliers (only bumped if we see many OOMs without op signature).
    s.pri_ram_mult = {
        Priority.QUERY: 1.0,
        Priority.INTERACTIVE: 1.0,
        Priority.BATCH_PIPELINE: 1.0,
    }


@register_scheduler(key="scheduler_iter_median_rich_017")
def scheduler_iter_median_rich_017_scheduler(s, results, pipelines):
    """Priority-aware scheduler with per-op RAM backoff and pool reservation.

    High-level:
    1) Enqueue new pipelines into priority queues.
    2) From results, update per-op RAM hints on OOM; drop pipelines on non-OOM failures (best-effort).
    3) For each pool, schedule runnable ops by priority; if multiple pools, reserve pool 0 primarily
       for QUERY+INTERACTIVE (batch spills into it only when HP is empty).
    """
    # -----------------------------
    # Helpers
    # -----------------------------
    def _is_oom_error(err):
        if err is None:
            return False
        msg = str(err).lower()
        return ("oom" in msg) or ("out of memory" in msg) or ("killed" in msg and "memory" in msg)

    def _op_sig(op):
        # Try to create a stable signature for the operator object without assuming attributes.
        # If op has common identifiers, prefer them; else fallback to string.
        for attr in ("op_id", "operator_id", "id", "name"):
            if hasattr(op, attr):
                try:
                    v = getattr(op, attr)
                    if v is not None:
                        return f"{attr}:{v}"
                except Exception:
                    pass
        return str(op)

    def _priority_order():
        return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]

    def _enqueue(p):
        s.queues[p.priority].append(p)
        if p.pipeline_id not in s.pipeline_drop:
            s.pipeline_drop[p.pipeline_id] = False

    def _pipeline_done_or_drop(p):
        st = p.runtime_status()
        if st.is_pipeline_successful():
            return True
        # If marked as non-retriable and has any failures, drop it.
        if s.pipeline_drop.get(p.pipeline_id, False) and st.state_counts.get(OperatorState.FAILED, 0) > 0:
            return True
        return False

    def _pop_rr(pri):
        q = s.queues[pri]
        if not q:
            return None

        n = len(q)
        start = s.rr_cursor[pri] % max(1, n)

        for k in range(n):
            idx = (start + k) % n
            p = q[idx]
            if _pipeline_done_or_drop(p):
                q.pop(idx)
                # adjust cursor if needed
                if idx < start:
                    start -= 1
                n -= 1
                if n <= 0:
                    s.rr_cursor[pri] = 0
                    return None
                continue

            # keep, advance cursor
            s.rr_cursor[pri] = (idx + 1) % max(1, len(q))
            return p
        return None

    def _has_hp_backlog():
        for pri in (Priority.QUERY, Priority.INTERACTIVE):
            for p in s.queues[pri]:
                if _pipeline_done_or_drop(p):
                    continue
                st = p.runtime_status()
                ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
                if ops:
                    return True
        return False

    def _base_cpu(pool, pri):
        # Favor concurrency for latency: smaller CPU slices for high-priority work.
        if pri == Priority.QUERY:
            return max(1.0, min(2.0, pool.max_cpu_pool * 0.20))
        if pri == Priority.INTERACTIVE:
            return max(1.0, min(3.0, pool.max_cpu_pool * 0.25))
        # batch can use more when available
        return max(1.0, min(pool.max_cpu_pool * 0.50, 8.0))

    def _base_ram(pool, pri):
        # Start small to avoid over-allocation (previous run allocated ~94% with only ~30% consumed).
        if pri == Priority.QUERY:
            return max(1.0, pool.max_ram_pool * 0.04)
        if pri == Priority.INTERACTIVE:
            return max(1.0, pool.max_ram_pool * 0.06)
        return max(1.0, pool.max_ram_pool * 0.08)

    def _choose_ram_for_op(pool, pri, op, avail_ram):
        # Use learned per-op hint if present; otherwise use conservative base.
        hint = s.op_ram_hint.get(_op_sig(op), 0.0)
        req = max(_base_ram(pool, pri) * s.pri_ram_mult.get(pri, 1.0), hint)
        # Clamp to pool/availability; must be >= 1
        req = max(1.0, min(req, avail_ram, pool.max_ram_pool))
        return req

    def _choose_cpu(pool, pri, avail_cpu):
        req = _base_cpu(pool, pri)
        req = max(1.0, min(req, avail_cpu, pool.max_cpu_pool))
        return req

    def _pool_allows_priority(pool_id, pri, hp_backlog):
        # If we have multiple pools, reserve pool 0 primarily for high-priority work.
        # Batch can still use pool 0 when there's no HP backlog.
        if s.executor.num_pools <= 1:
            return True
        if pool_id == 0:
            if pri == Priority.BATCH_PIPELINE and hp_backlog:
                return False
            return True
        # Non-zero pools accept all priorities
        return True

    # -----------------------------
    # 1) Enqueue new pipelines
    # -----------------------------
    for p in pipelines:
        _enqueue(p)

    # Early exit
    if not pipelines and not results:
        return [], []

    # -----------------------------
    # 2) Learn from results
    # -----------------------------
    # Update per-op RAM hints on OOM using the RAM that was allocated when it failed.
    # If we can't get ops or ram, fall back to bumping a coarse per-priority multiplier.
    for r in results:
        if hasattr(r, "failed") and r.failed():
            oom = _is_oom_error(getattr(r, "error", None))
            if oom:
                learned_any = False
                try:
                    ram_used = float(getattr(r, "ram", 0.0) or 0.0)
                except Exception:
                    ram_used = 0.0

                ops = getattr(r, "ops", None) or []
                for op in ops:
                    sig = _op_sig(op)
                    prev = s.op_ram_hint.get(sig, 0.0)
                    # Backoff: at least 1.5x what we tried, and grow exponentially on repeated OOMs.
                    target = max(prev * 1.7, ram_used * 1.5, 1.0)
                    s.op_ram_hint[sig] = target
                    learned_any = True

                if not learned_any:
                    # Coarse bump for this priority if we couldn't learn op-level.
                    pri = getattr(r, "priority", None)
                    if pri in s.pri_ram_mult:
                        s.pri_ram_mult[pri] = min(s.pri_ram_mult[pri] * 1.25, 16.0)
            else:
                # Non-OOM failures are likely logic/timeouts; don't retry indefinitely.
                # We can't reliably map result->pipeline_id, so we mark *pipelines that currently
                # show FAILED ops* as non-retriable in a later cleanup pass.
                pass

    # Best-effort marking of non-OOM failures as drop (lazy: if a pipeline has FAILED ops now,
    # and we saw any non-OOM failures in this tick, drop it).
    saw_non_oom = False
    for r in results:
        if hasattr(r, "failed") and r.failed() and (not _is_oom_error(getattr(r, "error", None))):
            saw_non_oom = True
            break
    if saw_non_oom:
        for pri in _priority_order():
            for p in list(s.queues[pri]):
                st = p.runtime_status()
                if st.state_counts.get(OperatorState.FAILED, 0) > 0:
                    s.pipeline_drop[p.pipeline_id] = True

    # -----------------------------
    # 3) Schedule
    # -----------------------------
    suspensions = []
    assignments = []

    hp_backlog = _has_hp_backlog()

    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool

        # Attempt to fill the pool greedily with small slices, prioritizing latency.
        # Limit loop to prevent pathological spins when nothing is runnable.
        spins = 0
        while avail_cpu > 0 and avail_ram > 0 and spins < 200:
            spins += 1
            scheduled_any = False

            for pri in _priority_order():
                if not _pool_allows_priority(pool_id, pri, hp_backlog):
                    continue

                p = _pop_rr(pri)
                if p is None:
                    continue

                # Find one runnable op (parents complete) to reduce HoL blocking.
                st = p.runtime_status()
                op_list = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
                if not op_list:
                    # Not runnable now; keep it for later.
                    s.queues[pri].append(p)
                    continue

                op = op_list[0]
                cpu_req = _choose_cpu(pool, pri, avail_cpu)
                ram_req = _choose_ram_for_op(pool, pri, op, avail_ram)

                # If we cannot fit minimal resources, put pipeline back and stop trying on this pool.
                if cpu_req <= 0 or ram_req <= 0 or cpu_req > avail_cpu or ram_req > avail_ram:
                    s.queues[pri].append(p)
                    # If we can't fit even a minimal op, further scheduling is unlikely.
                    spins = 10**9
                    break

                assignments.append(
                    Assignment(
                        ops=op_list,
                        cpu=cpu_req,
                        ram=ram_req,
                        priority=p.priority,
                        pool_id=pool_id,
                        pipeline_id=p.pipeline_id,
                    )
                )

                avail_cpu -= cpu_req
                avail_ram -= ram_req

                # Requeue pipeline for future ops
                s.queues[pri].append(p)

                scheduled_any = True
                break  # re-start from highest priority after each placement

            if not scheduled_any:
                break

    return suspensions, assignments
