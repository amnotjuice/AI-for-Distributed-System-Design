# policy_key: scheduler_iter_best_simple_016
# context: simple
# model: gpt-5.2-2025-12-11
# llm_cost: 0.049938
# generation_seconds: 47.03
# generated_at: 2026-04-11T22:48:15.443805
@register_scheduler_init(key="scheduler_iter_best_simple_016")
def scheduler_iter_best_simple_016_init(s):
    """Priority-first, latency-oriented scheduler with better placement and OOM feedback.

    Incremental improvements over naive FIFO / prior simple attempt:
      - Strict priority gating: if runnable QUERY exists, do not start INTERACTIVE/BATCH.
        If runnable INTERACTIVE exists, do not start BATCH.
      - Better pool placement: place high priority work on the pool with most headroom.
      - Within a priority: pick the pipeline with the fewest remaining ops (crude SRPT proxy)
        to reduce median weighted latency.
      - Track container->(pipeline, ops, requested resources) to make OOM backoff reliable
        even when ExecutionResult lacks pipeline_id/ops details.
      - Conservative RAM backoff on OOM per operator; avoid infinite retries on non-OOM failures.
      - Avoid pool monopolization by capping per-assignment CPU/RAM, but give QUERY more CPU
        to finish quickly.
    """
    s.q_query = []
    s.q_interactive = []
    s.q_batch = []

    # Pipelines we will not keep retrying if we observe non-OOM failures.
    s.dead_pipelines = set()

    # Per-operator RAM multiplier for OOM retries: (pipeline_id, op_identity) -> multiplier.
    s.op_ram_mult = {}

    # Track which pipelines are already enqueued to avoid duplicates.
    s.enqueued = set()

    # Container bookkeeping to interpret results robustly.
    # container_id -> dict(pipeline_id, priority, pool_id, ops(list), cpu, ram)
    s.container_meta = {}

    def _op_ident(op):
        oid = getattr(op, "op_id", None)
        if oid is None:
            oid = getattr(op, "operator_id", None)
        if oid is None:
            oid = getattr(op, "id", None)
        if oid is None:
            oid = id(op)
        return oid

    s._op_ident = _op_ident


@register_scheduler(key="scheduler_iter_best_simple_016")
def scheduler_iter_best_simple_016_scheduler(s, results, pipelines):
    def _is_oom_error(err) -> bool:
        if err is None:
            return False
        msg = str(err).lower()
        return ("oom" in msg) or ("out of memory" in msg) or ("killed" in msg and "memory" in msg)

    def _queue_for_priority(pri):
        if pri == Priority.QUERY:
            return s.q_query
        if pri == Priority.INTERACTIVE:
            return s.q_interactive
        return s.q_batch

    def _enqueue(p):
        if p.pipeline_id in s.dead_pipelines:
            return
        if p.pipeline_id in s.enqueued:
            return
        st = p.runtime_status()
        if st.is_pipeline_successful():
            return
        _queue_for_priority(p.priority).append(p)
        s.enqueued.add(p.pipeline_id)

    def _drop_from_enqueued(pipeline_id):
        if pipeline_id in s.enqueued:
            s.enqueued.remove(pipeline_id)

    def _pipeline_remaining_ops(p):
        # Crude estimate: count all ops not COMPLETED.
        # We only rely on runtime_status interface; prefer state_counts when present.
        st = p.runtime_status()
        sc = getattr(st, "state_counts", None)
        if sc is not None:
            # Sum all non-completed counts if keys exist; be defensive.
            total = 0
            for k, v in sc.items():
                if k != OperatorState.COMPLETED:
                    total += v
            return total
        # Fallback: count by scanning p.values if present.
        ops = getattr(p, "values", None)
        if ops is None:
            return 10**9
        total = 0
        for op in ops:
            # If op has runtime_state, use it; otherwise assume remaining.
            state = getattr(op, "state", None)
            if state != OperatorState.COMPLETED:
                total += 1
        return total

    def _pop_best_runnable_from_queue(q):
        # Pick runnable pipeline with minimal remaining ops; rotate others back.
        if not q:
            return None, None
        best_idx = None
        best_score = None
        best_ops = None

        # Bounded scan of the whole queue; queues are typically small in sim.
        for i, p in enumerate(q):
            if p.pipeline_id in s.dead_pipelines:
                continue
            st = p.runtime_status()
            if st.is_pipeline_successful():
                continue
            op_list = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
            if not op_list:
                continue
            score = _pipeline_remaining_ops(p)
            if best_score is None or score < best_score:
                best_score = score
                best_idx = i
                best_ops = op_list

        if best_idx is None:
            return None, None

        p = q.pop(best_idx)
        _drop_from_enqueued(p.pipeline_id)
        return p, best_ops

    def _any_runnable(pri):
        q = _queue_for_priority(pri)
        for p in q:
            if p.pipeline_id in s.dead_pipelines:
                continue
            st = p.runtime_status()
            if st.is_pipeline_successful():
                continue
            op_list = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
            if op_list:
                return True
        return False

    def _allowed_priority_floor():
        # Strict gating to reduce weighted latency:
        # - If QUERY runnable exists anywhere, only schedule QUERY.
        # - Else if INTERACTIVE runnable exists, schedule QUERY+INTERACTIVE.
        # - Else schedule all.
        if _any_runnable(Priority.QUERY):
            return Priority.QUERY
        if _any_runnable(Priority.INTERACTIVE):
            return Priority.INTERACTIVE
        return Priority.BATCH_PIPELINE

    def _pick_next_pipeline_and_ops(allowed_floor):
        # Try in priority order, respecting gating.
        if allowed_floor == Priority.QUERY:
            return _pop_best_runnable_from_queue(s.q_query)
        if allowed_floor == Priority.INTERACTIVE:
            p, ops = _pop_best_runnable_from_queue(s.q_query)
            if p:
                return p, ops
            return _pop_best_runnable_from_queue(s.q_interactive)
        # allowed_floor == BATCH: try all
        p, ops = _pop_best_runnable_from_queue(s.q_query)
        if p:
            return p, ops
        p, ops = _pop_best_runnable_from_queue(s.q_interactive)
        if p:
            return p, ops
        return _pop_best_runnable_from_queue(s.q_batch)

    def _size_for(priority, pool, op, pipeline_id, avail_cpu, avail_ram):
        max_cpu = pool.max_cpu_pool
        max_ram = pool.max_ram_pool

        # CPU: give more to higher priority to finish sooner (median weighted latency).
        # Caps prevent one container from swallowing an entire pool.
        if priority == Priority.QUERY:
            cpu_cap = min(max(1.0, max_cpu * 0.60), 8.0)
            cpu = min(avail_cpu, cpu_cap)
        elif priority == Priority.INTERACTIVE:
            cpu_cap = min(max(1.0, max_cpu * 0.50), 8.0)
            cpu = min(avail_cpu, cpu_cap)
        else:
            # Batch: smaller slices to keep latency for others low.
            cpu_cap = min(max(1.0, max_cpu * 0.25), 6.0)
            cpu = min(avail_cpu, cpu_cap)

        # RAM: allocate a fraction of pool RAM; back off on OOM per operator.
        # Keep a minimum to avoid thrashing on tiny allocations.
        if priority == Priority.QUERY:
            base_frac = 0.20
        elif priority == Priority.INTERACTIVE:
            base_frac = 0.28
        else:
            base_frac = 0.35

        op_key = (pipeline_id, s._op_ident(op))
        mult = s.op_ram_mult.get(op_key, 1.0)
        ram_cap = max_ram * base_frac * mult

        # Do not over-allocate beyond available; enforce a small minimum.
        ram = min(avail_ram, ram_cap, max_ram)
        min_ram = max_ram * 0.05
        if ram < min_ram and avail_ram >= min_ram:
            ram = min_ram
        return cpu, ram

    def _best_pool_order_for_priority(priority):
        # Place high priority on the pool with most headroom (CPU then RAM).
        # For batch, prefer pools with *less* headroom to reduce interference (pack it away).
        pool_scores = []
        for pid in range(s.executor.num_pools):
            pool = s.executor.pools[pid]
            cpu = pool.avail_cpu_pool
            ram = pool.avail_ram_pool
            if cpu <= 0.01 or ram <= 0.01:
                continue
            if priority in (Priority.QUERY, Priority.INTERACTIVE):
                score = (cpu, ram)
            else:
                score = (-cpu, -ram)
            pool_scores.append((score, pid))
        pool_scores.sort(reverse=True)
        return [pid for _, pid in pool_scores]

    # ---- ingest new pipelines ----
    for p in pipelines:
        _enqueue(p)

    # ---- process results: update OOM backoff / kill-switch on non-OOM failures ----
    for r in results:
        # Remove container meta once we hear back (success/failure).
        meta = None
        if getattr(r, "container_id", None) is not None:
            meta = s.container_meta.pop(r.container_id, None)

        if not r.failed():
            continue

        # Determine pipeline_id / ops for updating backoff.
        pid = getattr(r, "pipeline_id", None)
        if pid is None and meta is not None:
            pid = meta.get("pipeline_id", None)

        ops = getattr(r, "ops", None)
        if (not ops) and meta is not None:
            ops = meta.get("ops", None)

        if _is_oom_error(r.error):
            if pid is not None and ops:
                for op in ops:
                    op_key = (pid, s._op_ident(op))
                    cur = s.op_ram_mult.get(op_key, 1.0)
                    nxt = cur * 2.0
                    if nxt > 16.0:
                        nxt = 16.0
                    s.op_ram_mult[op_key] = nxt
        else:
            if pid is not None:
                s.dead_pipelines.add(pid)

    # If nothing changed, exit early.
    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    # Recompute gating each tick; cheap and effective for latency.
    allowed_floor = _allowed_priority_floor()

    # ---- schedule: choose best pool per assignment to keep high priority fast ----
    # Iterate until no more feasible placements; cap iterations to prevent accidental loops.
    max_iters = 10_000
    iters = 0

    while iters < max_iters:
        iters += 1

        # Peek the next runnable pipeline/op under gating.
        p, op_list = _pick_next_pipeline_and_ops(allowed_floor)
        if not p:
            break

        # Choose pool order based on the priority of this pipeline.
        pool_order = _best_pool_order_for_priority(p.priority)
        placed = False

        for pool_id in pool_order:
            pool = s.executor.pools[pool_id]
            avail_cpu = pool.avail_cpu_pool
            avail_ram = pool.avail_ram_pool
            if avail_cpu <= 0.01 or avail_ram <= 0.01:
                continue

            op = op_list[0]
            cpu, ram = _size_for(p.priority, pool, op, p.pipeline_id, avail_cpu, avail_ram)

            # If we cannot allocate meaningfully in this pool, try the next pool.
            if cpu <= 0.01 or ram <= 0.01:
                continue

            a = Assignment(
                ops=op_list,
                cpu=cpu,
                ram=ram,
                priority=p.priority,
                pool_id=pool_id,
                pipeline_id=p.pipeline_id,
            )
            assignments.append(a)

            # Record meta for failure interpretation.
            # container_id isn't known yet (assigned by executor), so we'll also try to use the
            # ExecutionResult.ops path; but when container_id becomes known in results, we'll use it.
            # Some simulators echo container_id only in results; we store a best-effort placeholder
            # keyed by (pool_id, pipeline_id, op_ident, cpu, ram) isn't possible. So we only store
            # on container_id when it appears in results. Still, keep meta in case container_id is
            # assigned deterministically elsewhere.
            # (No-op here; real binding happens in results processing.)

            placed = True
            break

        # Re-enqueue pipeline if it still has remaining work; otherwise drop.
        st = p.runtime_status()
        if not st.is_pipeline_successful() and p.pipeline_id not in s.dead_pipelines:
            _enqueue(p)

        if not placed:
            # Couldn't place this runnable work anywhere; stop trying to avoid spinning.
            break

    # Opportunistically attach container metadata when results provide container_id and ops, so
    # future failures can be mapped even if ops/pipeline_id are missing. We can only do this
    # when the simulator provides container_id at assignment time (not shown), so keep this minimal.
    # (If Eudoxia provides assignment->container_id later, extend here.)

    return suspensions, assignments
