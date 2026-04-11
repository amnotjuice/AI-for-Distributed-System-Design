# policy_key: scheduler_est_011
# reasoning_effort: low
# exp: estimation
# model: gpt-5.2-2025-12-11
# llm_cost: 0.046984
# generation_seconds: 61.30
# generated_at: 2026-04-10T07:55:28.664219
@register_scheduler_init(key="scheduler_est_011")
def scheduler_est_011_init(s):
    """Memory-aware, priority-first, low-churn scheduler.

    Core ideas:
    - Maintain separate queues per priority (query, interactive, batch) and avoid starvation via aging.
    - Use op.estimate.mem_peak_gb (when available) to:
        * avoid obviously infeasible placements (reduce OOM/fail=720s penalties),
        * choose best-fit pools to pack memory efficiently,
        * size RAM with a conservative safety factor (higher for high priority, higher on retries).
    - Greedily fill each pool with multiple small/medium assignments per tick when possible,
      but reserve preference for high priority to minimize weighted latency.
    """
    from collections import deque

    s.q_query = deque()
    s.q_interactive = deque()
    s.q_batch = deque()

    # Per-pipeline bookkeeping
    s.enqueued_at = {}  # pipeline_id -> tick first seen (for aging)
    s.last_seen_tick = 0

    # Per-operator retry tracking (id(op) -> retry_count)
    s.op_retries = {}

    # If we observe any failure for a pipeline, we avoid aggressive downsizing later.
    s.pipeline_had_failure = set()


@register_scheduler(key="scheduler_est_011")
def scheduler_est_011_scheduler(s, results, pipelines):
    """
    Scheduler step:
    - Ingest new pipelines into priority queues.
    - Update retry counts based on failures.
    - For each pool, repeatedly pick the best next operator to run that fits the pool.
    - Choose pool placements using memory best-fit with estimator hints.
    """
    from collections import defaultdict

    # ----------------------------
    # Helpers (defined inside to avoid global imports)
    # ----------------------------
    def _prio_weight(prio):
        # Higher means more important for the objective.
        if prio == Priority.QUERY:
            return 10
        if prio == Priority.INTERACTIVE:
            return 5
        return 1

    def _queue_for_priority(prio):
        if prio == Priority.QUERY:
            return s.q_query
        if prio == Priority.INTERACTIVE:
            return s.q_interactive
        return s.q_batch

    def _all_queues():
        return (s.q_query, s.q_interactive, s.q_batch)

    def _pipeline_done_or_failed(p):
        st = p.runtime_status()
        if st.is_pipeline_successful():
            return True
        # If any operator is FAILED, we still want to keep trying (drop is costly),
        # but many templates treat FAILED as terminal. We keep the pipeline unless
        # it is completely successful; failures get retried by assigning again.
        return False

    def _get_ready_ops(p, max_ops=1):
        st = p.runtime_status()
        # Only schedule ops that are ready w.r.t parents.
        ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        if not ops:
            return []
        return ops[:max_ops]

    def _est_mem_gb(op):
        est = getattr(op, "estimate", None)
        if est is None:
            return None
        return getattr(est, "mem_peak_gb", None)

    def _pool_max_ram():
        mx = 0.0
        for i in range(s.executor.num_pools):
            mx = max(mx, float(s.executor.pools[i].max_ram_pool))
        return mx

    def _best_pool_for_op(op, candidate_pool_ids, prefer_best_fit=True):
        """Choose a pool where op likely fits, using estimate as a hint (best-fit on RAM)."""
        est_mem = _est_mem_gb(op)
        best = None
        best_score = None

        for pool_id in candidate_pool_ids:
            pool = s.executor.pools[pool_id]
            avail_ram = float(pool.avail_ram_pool)
            avail_cpu = float(pool.avail_cpu_pool)
            if avail_ram <= 0 or avail_cpu <= 0:
                continue

            # If we have an estimate, skip pools that are very likely infeasible.
            # Use a small slack factor because estimate is noisy and we don't want to deadlock.
            if est_mem is not None:
                # "Likely fit" check: require estimate <= avail_ram * 1.05 (small slack).
                if est_mem > avail_ram * 1.05:
                    continue

            # Scoring: best-fit (min leftover RAM) to pack memory efficiently.
            if prefer_best_fit and est_mem is not None:
                leftover = avail_ram - est_mem
                score = (leftover, -avail_cpu)  # prefer lower leftover, then more cpu
            else:
                # Otherwise just choose the pool with most available RAM (more forgiving).
                score = (-avail_ram, -avail_cpu)

            if best is None or score < best_score:
                best = pool_id
                best_score = score

        return best

    def _ram_request(op, pool_id, pipeline_priority):
        """Compute RAM request (GB) with safety factor and retry escalation."""
        pool = s.executor.pools[pool_id]
        max_ram = float(pool.max_ram_pool)
        avail_ram = float(pool.avail_ram_pool)

        est_mem = _est_mem_gb(op)
        retries = s.op_retries.get(id(op), 0)

        # Base safety factor by priority (higher priority -> more conservative to avoid 720s penalty).
        if pipeline_priority == Priority.QUERY:
            base_sf = 1.45
        elif pipeline_priority == Priority.INTERACTIVE:
            base_sf = 1.30
        else:
            base_sf = 1.15

        # Retry escalation (assume failures may be OOM): multiplicative bump.
        # Keep it moderate to avoid locking the cluster on repeated failures.
        retry_sf = 1.0 + 0.35 * min(retries, 4)

        # If pipeline has seen any failure, be slightly more conservative.
        pipeline_sf = 1.10 if pipeline_priority != Priority.BATCH_PIPELINE else 1.05

        # Minimum container size "floor" to avoid pathological tiny allocations.
        # (RAM beyond minimum doesn't speed up, but too small increases OOM risk.)
        if pipeline_priority == Priority.QUERY:
            floor = 0.10 * max_ram
        elif pipeline_priority == Priority.INTERACTIVE:
            floor = 0.08 * max_ram
        else:
            floor = 0.05 * max_ram

        if est_mem is None:
            # No estimate: choose a conservative-but-not-blocking default.
            if pipeline_priority == Priority.QUERY:
                target = 0.55 * max_ram
            elif pipeline_priority == Priority.INTERACTIVE:
                target = 0.45 * max_ram
            else:
                target = 0.30 * max_ram
        else:
            target = est_mem * base_sf * retry_sf * pipeline_sf
            target = max(target, floor)

        # Never exceed pool max, and never request more than available (assignment must fit).
        target = min(target, max_ram)
        target = min(target, avail_ram)

        # Ensure strictly positive (some sims may treat 0 as invalid).
        return max(0.001, target)

    def _cpu_request(pool_id, pipeline_priority):
        """CPU request (vCPU) tuned to reduce high-priority latency without starving others."""
        pool = s.executor.pools[pool_id]
        avail_cpu = float(pool.avail_cpu_pool)
        max_cpu = float(pool.max_cpu_pool)

        if avail_cpu <= 0:
            return 0.0

        # For high priority, allocate more CPU to reduce runtime (sublinear scaling but still helps latency).
        if pipeline_priority == Priority.QUERY:
            target = min(avail_cpu, max(1.0, 0.90 * max_cpu))
        elif pipeline_priority == Priority.INTERACTIVE:
            target = min(avail_cpu, max(1.0, 0.70 * max_cpu))
        else:
            target = min(avail_cpu, max(1.0, 0.50 * max_cpu))

        # Avoid allocating the entire pool to batch if we can keep some headroom.
        if pipeline_priority == Priority.BATCH_PIPELINE and avail_cpu > 1.0:
            target = min(target, avail_cpu - 0.5)

        return max(1.0, target) if avail_cpu >= 1.0 else avail_cpu

    def _effective_score(pipeline, now_tick):
        """Higher score => schedule sooner. Uses priority weight + simple aging."""
        base = _prio_weight(pipeline.priority)

        first = s.enqueued_at.get(pipeline.pipeline_id, now_tick)
        age = max(0, now_tick - first)

        # Aging: slow but steady; prevents starvation without hurting high-pri too much.
        # Query/interactive already dominate; batch needs more aging to ensure progress.
        if pipeline.priority == Priority.BATCH_PIPELINE:
            age_boost = age / 15.0
        else:
            age_boost = age / 30.0

        # If pipeline had failures, slightly boost to retry sooner (avoid 720s penalties).
        fail_boost = 0.6 if pipeline.pipeline_id in s.pipeline_had_failure else 0.0

        return base + age_boost + fail_boost

    def _pop_best_pipeline_candidate(now_tick, scan_limit=24):
        """Select one pipeline to consider next, without fully sorting the whole queues."""
        best_p = None
        best_q = None
        best_score = None

        # Prefer high-priority queues, but allow aging to promote older lower-priority work.
        for q in _all_queues():
            # Scan only the head window to keep decisions cheap/deterministic.
            n = min(len(q), scan_limit)
            for i in range(n):
                p = q[i]
                if _pipeline_done_or_failed(p):
                    continue
                sc = _effective_score(p, now_tick)
                if best_p is None or sc > best_score:
                    best_p = p
                    best_q = q
                    best_score = sc

        if best_p is None:
            return None, None

        # Remove best_p from its queue (deque supports remove, O(n) within the small scan window).
        best_q.remove(best_p)
        return best_p, best_q

    # ----------------------------
    # Tick / bookkeeping
    # ----------------------------
    s.last_seen_tick = getattr(s, "last_seen_tick", 0) + 1
    now_tick = s.last_seen_tick

    # Ingest new pipelines
    for p in pipelines:
        q = _queue_for_priority(p.priority)
        q.append(p)
        if p.pipeline_id not in s.enqueued_at:
            s.enqueued_at[p.pipeline_id] = now_tick

    # Update retry counters based on results (assume failure implies we should increase RAM next time)
    for r in results:
        if hasattr(r, "failed") and r.failed():
            # Mark pipeline had failure (prioritize retries a bit)
            # Note: r may not contain pipeline_id; use priority-based boost only if missing.
            # We can still bump retries for the specific op objects included in r.ops.
            for op in getattr(r, "ops", []) or []:
                s.op_retries[id(op)] = s.op_retries.get(id(op), 0) + 1

            # Best-effort: if pipeline_id is present in the result, mark it.
            pid = getattr(r, "pipeline_id", None)
            if pid is not None:
                s.pipeline_had_failure.add(pid)

    # Early exit if nothing to do
    if not pipelines and not results:
        # We still might have queued work; do not early exit solely on empty deltas.
        pass

    suspensions = []
    assignments = []

    # Build a stable list of pool_ids to consider
    pool_ids = list(range(s.executor.num_pools))

    # Main packing loop: repeatedly try to schedule work into pools with headroom.
    # We do multiple rounds to fill pools, but cap rounds to avoid long decision time.
    max_rounds = 64
    rounds = 0

    while rounds < max_rounds:
        rounds += 1

        # Identify pools with resources
        runnable_pools = []
        for pool_id in pool_ids:
            pool = s.executor.pools[pool_id]
            if float(pool.avail_cpu_pool) > 0 and float(pool.avail_ram_pool) > 0:
                runnable_pools.append(pool_id)
        if not runnable_pools:
            break

        # Pick next pipeline to schedule from queues
        p, origin_q = _pop_best_pipeline_candidate(now_tick)
        if p is None:
            break

        # Fetch next ready op (single-op scheduling to keep placement predictable)
        ready_ops = _get_ready_ops(p, max_ops=1)
        if not ready_ops:
            # Not ready: requeue and continue
            origin_q.append(p)
            continue

        op = ready_ops[0]

        # Choose the best pool for this op.
        # Prefer best-fit packing; if estimator absent, fall back to most available RAM.
        chosen_pool = _best_pool_for_op(op, runnable_pools, prefer_best_fit=True)
        if chosen_pool is None:
            # If no pool "likely fits" by estimate, try a last-chance placement on the largest-RAM pool
            # (estimate is noisy; otherwise we can deadlock on overestimates).
            # Only do this if that pool actually has some RAM available.
            max_ram = -1.0
            fallback_pool = None
            for pool_id in runnable_pools:
                pool = s.executor.pools[pool_id]
                if float(pool.avail_ram_pool) > max_ram:
                    max_ram = float(pool.avail_ram_pool)
                    fallback_pool = pool_id

            if fallback_pool is None or max_ram <= 0:
                origin_q.append(p)
                continue

            chosen_pool = fallback_pool

        # Size resources
        cpu_req = _cpu_request(chosen_pool, p.priority)
        ram_req = _ram_request(op, chosen_pool, p.priority)

        # Final feasibility check
        pool = s.executor.pools[chosen_pool]
        if cpu_req <= 0 or ram_req <= 0:
            origin_q.append(p)
            continue
        if cpu_req > float(pool.avail_cpu_pool) + 1e-9 or ram_req > float(pool.avail_ram_pool) + 1e-9:
            # Not enough resources right now; requeue.
            origin_q.append(p)
            continue

        # Create the assignment
        assignments.append(
            Assignment(
                ops=[op],
                cpu=cpu_req,
                ram=ram_req,
                priority=p.priority,
                pool_id=chosen_pool,
                pipeline_id=p.pipeline_id,
            )
        )

        # Requeue pipeline (it likely has more ops to run later)
        origin_q.append(p)

    return suspensions, assignments
