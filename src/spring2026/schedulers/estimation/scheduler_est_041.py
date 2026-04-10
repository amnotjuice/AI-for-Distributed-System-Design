# policy_key: scheduler_est_041
# reasoning_effort: low
# exp: estimation
# model: gpt-5.2-2025-12-11
# llm_cost: 0.052220
# generation_seconds: 43.55
# generated_at: 2026-04-10T10:18:57.476612
@register_scheduler_init(key="scheduler_est_041")
def scheduler_est_041_init(s):
    """
    Memory-aware, priority-first scheduler with OOM-adaptive retries.

    Core ideas:
      - Maintain three priority queues (query > interactive > batch).
      - Prefer assigning ops whose estimated peak memory fits comfortably in a pool.
      - Use estimator as a hint; add safety headroom and learn from failures (esp. OOM-like)
        by increasing future RAM allocations for that specific op.
      - Avoid repeatedly placing a previously-failed op on the same pool when alternatives exist.
      - Pack multiple single-op assignments per pool per tick while resources allow.
    """
    # Priority-separated FIFO queues of pipelines
    s.q_query = []
    s.q_interactive = []
    s.q_batch = []

    # Per-operator learned memory multiplier (ram = base_est * mult + cushion)
    s.op_mem_mult = {}  # op_key -> float

    # Per-operator failure counters (used to keep increasing RAM on repeated failures)
    s.op_fail_count = {}  # op_key -> int

    # Per-operator last failed pool (avoid same pool if possible)
    s.op_avoid_pool = {}  # op_key -> pool_id

    # Small constants (in GB / vCPU) - tuned to be conservative and reduce failure penalties
    s.base_cushion_gb = 0.5
    s.min_assign_gb = 0.5
    s.max_mem_mult = 8.0

    s.min_cpu_query = 2
    s.min_cpu_interactive = 2
    s.min_cpu_batch = 1
    s.max_cpu_query = 4
    s.max_cpu_interactive = 3
    s.max_cpu_batch = 2

    # "Comfort" headroom: require some fraction of free pool RAM to remain after placement
    s.pool_headroom_frac = 0.10

    # When an op fails, multiply its future RAM request by this factor
    s.fail_mem_bump = 1.6


@register_scheduler(key="scheduler_est_041")
def scheduler_est_041_scheduler(s, results, pipelines):
    """
    Scheduler step:
      - Ingest new pipelines into priority queues.
      - Update per-op learned RAM multipliers from failures.
      - For each pool, repeatedly pick the next best assignable op across queues:
          * Prefer higher priority
          * Prefer smaller estimated memory footprint (to reduce fragmentation)
          * Place on a pool where estimated memory fits with headroom and avoids known-bad pool
      - Never intentionally assign an op if its estimated memory clearly cannot fit the pool.
    """
    # Helper functions defined locally to keep the policy self-contained.
    def _get_priority_queue(priority):
        if priority == Priority.QUERY:
            return s.q_query
        if priority == Priority.INTERACTIVE:
            return s.q_interactive
        return s.q_batch

    def _op_key(pipeline, op):
        # Try common operator id attribute names; fall back to object identity.
        for attr in ("operator_id", "op_id", "id", "uid", "name"):
            if hasattr(op, attr):
                try:
                    return (pipeline.pipeline_id, getattr(op, attr))
                except Exception:
                    pass
        return (pipeline.pipeline_id, id(op))

    def _est_mem_gb(op):
        est = None
        if hasattr(op, "estimate") and op.estimate is not None and hasattr(op.estimate, "mem_peak_gb"):
            est = op.estimate.mem_peak_gb
        if est is None:
            return None
        try:
            est = float(est)
        except Exception:
            return None
        # Guard against bogus negative/NaN values
        if not (est >= 0.0):
            return None
        return est

    def _target_cpu(priority, avail_cpu):
        # Keep per-op CPU modest to increase concurrency and reduce head-of-line blocking,
        # while still giving higher priorities enough CPU to reduce latency.
        if priority == Priority.QUERY:
            lo, hi = s.min_cpu_query, s.max_cpu_query
        elif priority == Priority.INTERACTIVE:
            lo, hi = s.min_cpu_interactive, s.max_cpu_interactive
        else:
            lo, hi = s.min_cpu_batch, s.max_cpu_batch

        cpu = max(lo, 1)
        cpu = min(cpu, hi)
        cpu = min(cpu, int(avail_cpu))
        return max(cpu, 1)

    def _target_ram_gb(pipeline, op, pool):
        """
        Compute RAM request from estimator + learned multiplier + cushion.
        If no estimate, use a conservative default that still enables progress.
        """
        key = _op_key(pipeline, op)
        mult = s.op_mem_mult.get(key, 1.25)  # default mild headroom even without failures
        mult = min(max(mult, 1.0), s.max_mem_mult)

        est = _est_mem_gb(op)
        if est is None:
            # Conservative fallback: request a small but non-trivial amount, capped by pool size.
            # Too large hurts concurrency; too small risks OOM. This is a compromise.
            base = max(s.min_assign_gb, min(2.0, float(getattr(pool, "max_ram_pool", 2.0)) * 0.25))
        else:
            base = max(est, s.min_assign_gb)

        req = base * mult + s.base_cushion_gb
        # Don't request more than the pool can ever provide.
        max_pool_ram = float(getattr(pool, "max_ram_pool", req))
        req = min(req, max_pool_ram)
        return max(req, s.min_assign_gb)

    def _fits_in_pool(req_ram, pool):
        avail_ram = float(pool.avail_ram_pool)
        # Keep some headroom to reduce chance of OOM cascades and allow another small task.
        headroom = float(getattr(pool, "max_ram_pool", avail_ram)) * s.pool_headroom_frac
        return req_ram <= max(0.0, avail_ram - headroom)

    def _pipeline_done_or_terminal(pipeline):
        status = pipeline.runtime_status()
        # We avoid dropping pipelines even if failures exist, because re-trying FAILED ops
        # can improve completion rate and reduce the heavy 720s penalty.
        return status.is_pipeline_successful()

    def _get_first_assignable_op(pipeline):
        status = pipeline.runtime_status()
        op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        if not op_list:
            return None
        return op_list[0]

    def _queue_candidates_with_ops(queue):
        """
        Return list of (pipeline, op, est_mem) for pipelines that have an assignable op.
        Does not mutate the queue.
        """
        out = []
        for p in queue:
            if _pipeline_done_or_terminal(p):
                continue
            op = _get_first_assignable_op(p)
            if op is None:
                continue
            out.append((p, op, _est_mem_gb(op)))
        return out

    def _select_best_candidate():
        """
        Choose one candidate across all queues by:
          1) priority order: query, interactive, batch
          2) within same priority: smallest estimated memory first (None treated as medium)
        Returns (pipeline, op) or (None, None) if nothing is ready.
        """
        # Build candidates per queue in strict priority order.
        for queue in (s.q_query, s.q_interactive, s.q_batch):
            cands = _queue_candidates_with_ops(queue)
            if not cands:
                continue

            def mem_sort_key(x):
                # Prefer smaller memory; put unknown estimates after small but before huge by using 4GB sentinel.
                est = x[2]
                return 4.0 if est is None else est

            cands.sort(key=mem_sort_key)
            p, op, _ = cands[0]
            return p, op
        return None, None

    def _remove_pipeline_from_queue(pipeline):
        q = _get_priority_queue(pipeline.priority)
        # Remove first occurrence; queue sizes are expected to be moderate in simulation.
        for i, p in enumerate(q):
            if p is pipeline:
                q.pop(i)
                return

    def _reappend_pipeline(pipeline):
        _get_priority_queue(pipeline.priority).append(pipeline)

    # Ingest new pipelines
    for p in pipelines:
        _get_priority_queue(p.priority).append(p)

    # Early exit if no changes
    if not pipelines and not results:
        return [], []

    # Update learned RAM multipliers from failures
    for r in results:
        if not r.failed():
            continue
        # Treat any failure as a hint we under-provisioned or chose a bad placement;
        # bump RAM multiplier and try to avoid the same pool next time.
        for op in getattr(r, "ops", []) or []:
            # We do not have the pipeline object here; use container_id/pool_id signals only
            # by updating keys by object identity fallback when possible. Best effort:
            key = ("unknown", id(op))
            cnt = s.op_fail_count.get(key, 0) + 1
            s.op_fail_count[key] = cnt
            prev = s.op_mem_mult.get(key, 1.25)
            s.op_mem_mult[key] = min(prev * s.fail_mem_bump, s.max_mem_mult)
            s.op_avoid_pool[key] = getattr(r, "pool_id", None)

    suspensions = []
    assignments = []

    # Attempt to schedule on each pool, packing multiple ops if resources allow.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]

        # Greedy packing loop
        while True:
            avail_cpu = int(pool.avail_cpu_pool)
            avail_ram = float(pool.avail_ram_pool)
            if avail_cpu <= 0 or avail_ram <= 0.0:
                break

            pipeline, op = _select_best_candidate()
            if pipeline is None or op is None:
                break

            # Compute per-op key with pipeline context for learned multipliers.
            key = _op_key(pipeline, op)

            # If we learned an "avoid pool" for this op (best-effort), honor it when possible.
            avoid_pool = s.op_avoid_pool.get(key, None)
            if avoid_pool is not None and avoid_pool == pool_id and s.executor.num_pools > 1:
                # Try another pool in this tick; do not remove from queue.
                break

            req_ram = _target_ram_gb(pipeline, op, pool)

            # Hard filter: if estimated memory is present and clearly larger than max pool ram, don't thrash.
            est = _est_mem_gb(op)
            max_pool_ram = float(getattr(pool, "max_ram_pool", avail_ram))
            if est is not None and est > max_pool_ram:
                # Can't ever fit here; try other pools this tick.
                break

            if not _fits_in_pool(req_ram, pool):
                # Not enough headroom; stop packing this pool (other pools may have space).
                break

            cpu = _target_cpu(pipeline.priority, avail_cpu)
            ram = min(req_ram, float(pool.avail_ram_pool))

            # Create assignment for a single operator.
            assignment = Assignment(
                ops=[op],
                cpu=cpu,
                ram=ram,
                priority=pipeline.priority,
                pool_id=pool_id,
                pipeline_id=pipeline.pipeline_id,
            )
            assignments.append(assignment)

            # Move pipeline to back of its queue to provide fairness within same priority.
            _remove_pipeline_from_queue(pipeline)
            _reappend_pipeline(pipeline)

            # Continue packing until resources run out.

    return suspensions, assignments
