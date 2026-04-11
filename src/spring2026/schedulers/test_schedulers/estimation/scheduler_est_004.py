# policy_key: scheduler_est_004
# reasoning_effort: low
# exp: estimation
# model: gpt-5.2-2025-12-11
# llm_cost: 0.041692
# generation_seconds: 57.64
# generated_at: 2026-04-10T07:49:12.828965
@register_scheduler_init(key="scheduler_est_004")
def scheduler_est_004_init(s):
    """Memory-aware, priority-protecting scheduler.

    Core ideas:
    - Maintain per-priority queues (QUERY > INTERACTIVE > BATCH) to minimize weighted latency.
    - Use op.estimate.mem_peak_gb as a hint to (a) pick a feasible pool and (b) right-size RAM to reduce OOMs.
    - On suspected OOM failures, retry the same operator with an exponential RAM boost (bounded), instead of dropping the pipeline.
    - Prevent starvation by simple aging for batch pipelines and by allowing batch work only when higher-priority backlog is small.
    """
    from collections import deque

    s.q_query = deque()
    s.q_interactive = deque()
    s.q_batch = deque()

    # Track pipelines we've seen to avoid enqueuing duplicates.
    s._seen_pipeline_ids = set()

    # Per-(pipeline_id, op_id) retry state.
    s._op_attempts = {}     # (pipeline_id, op_key) -> int
    s._op_ram_boost = {}    # (pipeline_id, op_key) -> float

    # Simple aging for batch: count ticks waited in queue.
    s._batch_wait_ticks = {}  # pipeline_id -> int
    s._tick = 0


@register_scheduler(key="scheduler_est_004")
def scheduler_est_004_scheduler(s, results, pipelines):
    from collections import defaultdict
    import math

    s._tick += 1

    def _priority_rank(pri):
        # Lower is higher priority.
        if pri == Priority.QUERY:
            return 0
        if pri == Priority.INTERACTIVE:
            return 1
        return 2

    def _enqueue_pipeline(p):
        if p.pipeline_id in s._seen_pipeline_ids:
            return
        s._seen_pipeline_ids.add(p.pipeline_id)
        if p.priority == Priority.QUERY:
            s.q_query.append(p)
        elif p.priority == Priority.INTERACTIVE:
            s.q_interactive.append(p)
        else:
            s.q_batch.append(p)
            s._batch_wait_ticks[p.pipeline_id] = 0

    def _is_oom_error(err):
        if err is None:
            return False
        msg = str(err).lower()
        return ("oom" in msg) or ("out of memory" in msg) or ("memory" in msg and "exceed" in msg)

    def _op_key(pipeline_id, op):
        # Use id(op) for a stable key during a simulation run.
        return (pipeline_id, id(op))

    def _get_est_mem_gb(op):
        est = getattr(op, "estimate", None)
        val = None if est is None else getattr(est, "mem_peak_gb", None)
        try:
            if val is None:
                return None
            val = float(val)
            if val < 0:
                return None
            return val
        except Exception:
            return None

    def _attempts(pipeline_id, op):
        return s._op_attempts.get(_op_key(pipeline_id, op), 0)

    def _boost(pipeline_id, op):
        return s._op_ram_boost.get(_op_key(pipeline_id, op), 1.0)

    def _bump_retry_state(pipeline_id, op, oom):
        k = _op_key(pipeline_id, op)
        s._op_attempts[k] = s._op_attempts.get(k, 0) + 1

        # Exponential backoff on RAM for OOM; mild increase otherwise.
        cur = s._op_ram_boost.get(k, 1.0)
        if oom:
            # Double quickly but cap to avoid runaway.
            s._op_ram_boost[k] = min(cur * 2.0, 16.0)
        else:
            s._op_ram_boost[k] = min(cur * 1.25, 4.0)

    def _pipeline_done_or_hopeless(p):
        st = p.runtime_status()
        if st.is_pipeline_successful():
            return True

        # If there are failures, we will *try* to retry, but cap retries per-op to avoid infinite churn.
        failed_ops = st.get_ops([OperatorState.FAILED], require_parents_complete=False)
        for op in failed_ops:
            att = _attempts(p.pipeline_id, op)
            # Allow more retries for OOM-ish behavior; fewer otherwise.
            # We don't always know if it was OOM from the operator alone; use attempts cap as guardrail.
            if att <= 5:
                return False
        # Too many failures: treat as hopeless and stop scheduling it.
        return True

    def _pick_assignable_op(p):
        st = p.runtime_status()
        # Only schedule when dependencies are satisfied.
        ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        if not ops:
            return None

        # Prefer smallest estimated memory first within a pipeline to reduce fragmentation/headroom issues.
        # For None estimates, place them after known-small ops.
        def key_fn(op):
            est = _get_est_mem_gb(op)
            att = _attempts(p.pipeline_id, op)
            # Penalize repeated retries to avoid thrashing.
            return (math.inf if est is None else est, att)

        ops_sorted = sorted(ops, key=key_fn)
        return ops_sorted[0]

    def _compute_ram_request(pool, p, op):
        # Conservative but not too wasteful:
        # - If estimate exists: allocate estimate * safety_margin * retry_boost
        # - Else: allocate a moderate default fraction of pool RAM (varies by priority).
        est = _get_est_mem_gb(op)
        b = _boost(p.pipeline_id, op)

        if p.priority == Priority.QUERY:
            safety = 1.35
            default_frac = 0.40
        elif p.priority == Priority.INTERACTIVE:
            safety = 1.30
            default_frac = 0.45
        else:
            safety = 1.20
            default_frac = 0.55

        if est is not None:
            req = est * safety * b
            # Clamp to [small_min, pool.max_ram_pool] and also not more than currently available.
            small_min = 0.5
            req = max(req, small_min)
        else:
            # Unknown memory: avoid tiny allocations that risk OOM; scale with retries a bit.
            req = pool.max_ram_pool * default_frac * min(1.0 + 0.15 * _attempts(p.pipeline_id, op), 1.6)

        return min(req, pool.avail_ram_pool)

    def _compute_cpu_request(pool, p):
        # Favor latency for high priority by giving a decent chunk of CPU but leave room for concurrency.
        if p.priority == Priority.QUERY:
            target = max(1.0, 0.60 * pool.max_cpu_pool)
        elif p.priority == Priority.INTERACTIVE:
            target = max(1.0, 0.50 * pool.max_cpu_pool)
        else:
            target = max(1.0, 0.35 * pool.max_cpu_pool)
        return min(target, pool.avail_cpu_pool)

    def _fits_pool(pool, p, op):
        # Quick feasibility using estimate as hint.
        est = _get_est_mem_gb(op)
        if est is None:
            # Unknown: require some headroom; don't schedule if pool is extremely tight.
            return pool.avail_ram_pool >= max(0.5, 0.10 * pool.max_ram_pool) and pool.avail_cpu_pool >= 1.0

        # Apply a small margin and retry boost to reduce OOM chance.
        need = est * 1.10 * _boost(p.pipeline_id, op)
        return (need <= pool.avail_ram_pool) and (pool.avail_cpu_pool >= 1.0)

    # Enqueue new pipelines.
    for p in pipelines:
        _enqueue_pipeline(p)

    # Update aging for batch pipelines.
    # (Keep it simple: age only while they remain queued and unfinished.)
    for p in list(s.q_batch):
        if p.pipeline_id in s._batch_wait_ticks:
            s._batch_wait_ticks[p.pipeline_id] += 1

    # Process results: detect OOM failures and bump retry state.
    # Also, if pipeline is completed/hopeless, later cleanup will avoid requeue.
    for r in results:
        if r is None:
            continue
        if hasattr(r, "failed") and r.failed():
            oom = _is_oom_error(getattr(r, "error", None))
            # Update state for each op in the failed container.
            for op in getattr(r, "ops", []) or []:
                _bump_retry_state(getattr(r, "pipeline_id", None) or getattr(getattr(op, "pipeline_id", None), "pipeline_id", None) or getattr(op, "pipeline_id", None) or getattr(getattr(op, "pipeline", None), "pipeline_id", None) or None, op, oom)  # best-effort
            # The above pipeline_id inference is unreliable; also bump using available pipeline_id on result if present.
            # If result doesn't carry pipeline_id, we still can bump by scanning queues below on selection.

    # Because ExecutionResult may not include pipeline_id, do a second pass:
    # If any operator is in FAILED state, assume it may have just failed and bump its retry state modestly.
    # (This keeps the policy robust to schema differences.)
    for q in (s.q_query, s.q_interactive, s.q_batch):
        for p in list(q):
            st = p.runtime_status()
            failed_ops = st.get_ops([OperatorState.FAILED], require_parents_complete=False)
            for op in failed_ops:
                k = _op_key(p.pipeline_id, op)
                # Only bump once per tick per failed op to avoid repeated inflation.
                last_bump = s.__dict__.setdefault("_last_failed_bump_tick", {})
                if last_bump.get(k) == s._tick:
                    continue
                last_bump[k] = s._tick
                # Assume OOM is more likely; conservative increase helps completion rate.
                _bump_retry_state(p.pipeline_id, op, oom=True)

    suspensions = []
    assignments = []

    # Helper: pop next viable pipeline (with batch aging), without dropping unless done/hopeless.
    def _pop_next_pipeline(prefer_batch=False):
        # If prefer_batch is True, allow an aged batch pipeline to jump ahead of interactive
        # after a long wait (aging threshold).
        aging_threshold = 25

        # Build candidates in priority order, but allow batch promotion if very old.
        batch_promote = None
        if s.q_batch:
            # Find the oldest batch pipeline.
            oldest = max(s.q_batch, key=lambda p: s._batch_wait_ticks.get(p.pipeline_id, 0))
            if s._batch_wait_ticks.get(oldest.pipeline_id, 0) >= aging_threshold:
                batch_promote = oldest

        if batch_promote is not None and prefer_batch:
            # Remove and return promoted batch.
            s.q_batch.remove(batch_promote)
            return batch_promote

        for q in (s.q_query, s.q_interactive, s.q_batch):
            while q:
                p = q.popleft()
                if _pipeline_done_or_hopeless(p):
                    # Cleanup.
                    s._batch_wait_ticks.pop(p.pipeline_id, None)
                    continue
                return p
        return None

    # Decide whether to allow batch scheduling when high priority backlog exists.
    def _high_priority_backlog():
        return len(s.q_query) + len(s.q_interactive)

    # Main packing loop: iterate pools and assign multiple single-op containers while resources allow.
    # This improves throughput while still protecting high-priority latency.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]

        # If pool is empty-ish but there is high priority backlog, don't waste time on batch.
        allow_batch = True
        if _high_priority_backlog() > 0:
            # Only allow batch if we have lots of headroom.
            allow_batch = (pool.avail_cpu_pool >= 0.60 * pool.max_cpu_pool) and (pool.avail_ram_pool >= 0.60 * pool.max_ram_pool)

        # Try to fill pool with several assignments.
        # Limit per tick per pool to avoid overly aggressive packing.
        max_assignments_this_pool = 6
        made = 0

        # Small optimization: build a local list of candidate pipelines in a stable order.
        # We requeue pipelines we couldn't place (e.g., mem doesn't fit) to avoid dropping.
        requeue_later = []

        while made < max_assignments_this_pool:
            if pool.avail_cpu_pool < 1.0 or pool.avail_ram_pool <= 0.1:
                break

            # Choose next pipeline:
            # - Always prefer query, then interactive
            # - For batch, only if allowed or if batch has aged significantly (prefer_batch flag)
            prefer_batch = allow_batch
            p = _pop_next_pipeline(prefer_batch=prefer_batch)
            if p is None:
                break

            if (p.priority == Priority.BATCH_PIPELINE) and not allow_batch:
                # Can't run batch now; defer.
                requeue_later.append(p)
                continue

            op = _pick_assignable_op(p)
            if op is None:
                # No runnable op yet, keep pipeline around.
                requeue_later.append(p)
                continue

            # If op doesn't fit this pool, try other pools by deferring to later in this tick:
            # put back in a temp list; it will be requeued after pool iteration.
            if not _fits_pool(pool, p, op):
                requeue_later.append(p)
                continue

            cpu_req = _compute_cpu_request(pool, p)
            ram_req = _compute_ram_request(pool, p, op)

            # Final guard: ensure we don't assign trivially-small RAM for known-large estimate.
            est = _get_est_mem_gb(op)
            if est is not None:
                if ram_req < min(pool.avail_ram_pool, est * 1.05):
                    # Not enough headroom right now; defer.
                    requeue_later.append(p)
                    continue

            # Create assignment (single operator per container to reduce blast radius on OOM).
            assignments.append(
                Assignment(
                    ops=[op],
                    cpu=cpu_req,
                    ram=ram_req,
                    priority=p.priority,
                    pool_id=pool_id,
                    pipeline_id=p.pipeline_id,
                )
            )
            made += 1

            # Keep pipeline around for remaining ops.
            requeue_later.append(p)

            # Update local pool availability optimistically (the simulator will also account for it,
            # but this prevents over-issuing in the same tick).
            pool.avail_cpu_pool -= cpu_req
            pool.avail_ram_pool -= ram_req

            # If we just scheduled a high-priority op, tighten batch admission for the remainder.
            if p.priority in (Priority.QUERY, Priority.INTERACTIVE):
                allow_batch = False

        # Requeue deferred pipelines to the appropriate queues, preserving relative order.
        for p in requeue_later:
            if _pipeline_done_or_hopeless(p):
                s._batch_wait_ticks.pop(p.pipeline_id, None)
                continue
            if p.priority == Priority.QUERY:
                s.q_query.append(p)
            elif p.priority == Priority.INTERACTIVE:
                s.q_interactive.append(p)
            else:
                s.q_batch.append(p)

    return suspensions, assignments
