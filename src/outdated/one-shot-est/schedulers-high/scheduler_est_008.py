# policy_key: scheduler_est_008
# reasoning_effort: high
# model: gpt-5.2-2025-12-11
# llm_cost: 0.069334
# generation_seconds: 151.86
# generated_at: 2026-03-31T18:35:33.486179
@register_scheduler_init(key="scheduler_est_008")
def scheduler_est_008_init(s):
    """
    Priority-aware, estimate-driven scheduler with conservative headroom.

    Incremental improvements over naive FIFO:
      1) Separate queues per priority (QUERY > INTERACTIVE > BATCH).
      2) Use op.estimate.mem_peak_gb to right-size RAM and reduce OOMs.
      3) Retry OOM failures with increasing RAM multiplier (instead of dropping pipelines with failures).
      4) Maintain CPU/RAM headroom so low-priority work doesn't destroy high-priority latency.
      5) Multi-pool placement: high-priority prefers the emptiest pool; batch prefers packing.

    Notes:
      - No preemption (kept simple and robust without relying on executor internals).
      - One op per container assignment; multiple assignments per tick as capacity allows.
    """
    from collections import deque

    # Per-priority round-robin queues of pipeline_ids
    s.queues = {
        Priority.QUERY: deque(),
        Priority.INTERACTIVE: deque(),
        Priority.BATCH_PIPELINE: deque(),
    }
    s.in_queue = set()  # pipeline_ids currently enqueued (to avoid duplicates)
    s.pipeline_map = {}  # pipeline_id -> Pipeline
    s.dead_pipelines = set()  # permanently failed pipelines (non-OOM failures)

    # Track which pipeline an op belonged to (so we can react to failures)
    s.op_to_pipeline = {}  # id(op) -> pipeline_id

    # OOM retry control (multiplier applied to estimated peak memory)
    s.base_ram_mult = 1.20
    s.max_ram_mult = 8.00
    s.ram_mult_by_pipeline = {}  # pipeline_id -> multiplier

    # Small floor to avoid "0 GB" corner cases
    s.min_ram_gb = 0.25

    # Keep headroom for high priority to reduce queueing and tail latency
    s.high_reserve_frac_cpu = 0.25
    s.high_reserve_frac_ram = 0.25

    # Conservative CPU caps per op to avoid dumping an entire pool into one container by default
    s.cpu_caps = {
        Priority.QUERY: 8.0,
        Priority.INTERACTIVE: 4.0,
        Priority.BATCH_PIPELINE: 2.0,
    }

    # Limit repeated scans to avoid spending too much time rotating queues
    s.max_queue_scan_factor = 2  # scan up to factor * len(queue) items per scheduling attempt

    # Per-pipeline cap per tick (prevents one pipeline from monopolizing a tick)
    s.max_ops_per_pipeline_per_tick = {
        Priority.QUERY: 2,
        Priority.INTERACTIVE: 1,
        Priority.BATCH_PIPELINE: 1,
    }

    # Cache global maxima for "can this ever fit?" checks
    s.max_pool_ram = 0.0
    s.max_pool_cpu = 0.0
    for i in range(s.executor.num_pools):
        pool = s.executor.pools[i]
        s.max_pool_ram = max(s.max_pool_ram, float(pool.max_ram_pool))
        s.max_pool_cpu = max(s.max_pool_cpu, float(pool.max_cpu_pool))


@register_scheduler(key="scheduler_est_008")
def scheduler_est_008(s, results, pipelines):
    """
    See init docstring for the policy overview.
    """
    # ----------------------------
    # Helpers (kept nested to avoid top-level imports)
    # ----------------------------
    def _queue_for_priority(prio):
        # Defensive: if a new priority exists, treat it as batch.
        return s.queues.get(prio, s.queues[Priority.BATCH_PIPELINE])

    def _enqueue_pipeline(p):
        pid = p.pipeline_id
        if pid in s.dead_pipelines:
            return
        # Always keep the latest pipeline object
        s.pipeline_map[pid] = p
        if pid in s.in_queue:
            return
        st = p.runtime_status()
        if st.is_pipeline_successful():
            return
        _queue_for_priority(p.priority).append(pid)
        s.in_queue.add(pid)

    def _error_is_oom(err):
        if not err:
            return False
        msg = str(err).lower()
        # Keep this heuristic simple and robust across different error strings.
        return ("oom" in msg) or ("out of memory" in msg) or ("out-of-memory" in msg)

    def _pipeline_ram_mult(pipeline_id, priority):
        # Slightly higher starting slack for higher priority to reduce OOM retries impacting latency.
        base = s.base_ram_mult
        if priority == Priority.QUERY:
            base *= 1.10
        elif priority == Priority.INTERACTIVE:
            base *= 1.05
        return s.ram_mult_by_pipeline.get(pipeline_id, base)

    def _bump_ram_mult(pipeline_id):
        cur = s.ram_mult_by_pipeline.get(pipeline_id, s.base_ram_mult)
        # Multiplicative bump converges quickly and is stable.
        s.ram_mult_by_pipeline[pipeline_id] = min(s.max_ram_mult, cur * 1.50)

    def _estimate_op_ram_gb(op, mult):
        est = None
        # op.estimate.mem_peak_gb is advertised by Eudoxia; still guard in case of missing fields.
        est_obj = getattr(op, "estimate", None)
        if est_obj is not None:
            est = getattr(est_obj, "mem_peak_gb", None)
        if est is None:
            est = s.min_ram_gb
        try:
            est = float(est)
        except Exception:
            est = s.min_ram_gb
        return max(s.min_ram_gb, est * float(mult))

    def _pool_score(avail_cpu, avail_ram):
        # Simple, unitless score; good enough for ordering pools.
        return float(avail_cpu) + float(avail_ram)

    def _high_backlog():
        return (len(s.queues[Priority.QUERY]) + len(s.queues[Priority.INTERACTIVE])) > 0

    def _total_backlog():
        return len(s.queues[Priority.QUERY]) + len(s.queues[Priority.INTERACTIVE]) + len(s.queues[Priority.BATCH_PIPELINE])

    def _compute_cpu_alloc(priority, avail_cpu, pool_max_cpu, allow_scale_up):
        cap = float(s.cpu_caps.get(priority, 2.0))
        if allow_scale_up:
            cap = float(pool_max_cpu)
        cpu = min(float(avail_cpu), cap)
        if cpu < 1.0:
            return 0.0
        return cpu

    def _try_schedule_one_from_priority(
        priority,
        pool_id,
        avail_cpu,
        avail_ram,
        pool_max_cpu,
        pool_max_ram,
        planned_op_ids,
        scheduled_ops_per_pipeline,
        enforce_high_headroom_for_batch,
        high_reserve_cpu,
        high_reserve_ram,
    ):
        """
        Attempts to create one Assignment for the given priority on the given pool
        while respecting capacity and (optionally) high-priority headroom.

        Returns: (assignment_or_None, new_avail_cpu, new_avail_ram)
        """
        q = _queue_for_priority(priority)
        if not q:
            return None, avail_cpu, avail_ram

        # For batch, enforce keeping headroom *unless* no high backlog.
        def _effective_avail(cpu, ram):
            if enforce_high_headroom_for_batch and priority == Priority.BATCH_PIPELINE:
                return max(0.0, float(cpu) - high_reserve_cpu), max(0.0, float(ram) - high_reserve_ram)
            return float(cpu), float(ram)

        eff_cpu, eff_ram = _effective_avail(avail_cpu, avail_ram)
        if eff_cpu < 1.0 or eff_ram < s.min_ram_gb:
            return None, avail_cpu, avail_ram

        # Scan a bounded number of items to find something runnable that fits.
        scan_limit = max(1, min(len(q) * s.max_queue_scan_factor, len(q) + 16))

        for _ in range(scan_limit):
            if not q:
                break

            pipeline_id = q.popleft()
            s.in_queue.discard(pipeline_id)

            p = s.pipeline_map.get(pipeline_id)
            if p is None:
                continue
            if pipeline_id in s.dead_pipelines:
                continue

            st = p.runtime_status()
            if st.is_pipeline_successful():
                # Cleanup lazily.
                s.pipeline_map.pop(pipeline_id, None)
                s.ram_mult_by_pipeline.pop(pipeline_id, None)
                continue

            # Per-tick anti-monopoly for a single pipeline.
            if scheduled_ops_per_pipeline.get(pipeline_id, 0) >= s.max_ops_per_pipeline_per_tick.get(priority, 1):
                q.append(pipeline_id)
                s.in_queue.add(pipeline_id)
                continue

            # Pick one ready op whose parents are done.
            ready_ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True) or []
            op = None
            for candidate in ready_ops:
                if id(candidate) not in planned_op_ids:
                    op = candidate
                    break

            if op is None:
                # Nothing runnable right now; requeue.
                q.append(pipeline_id)
                s.in_queue.add(pipeline_id)
                continue

            # RAM allocation from estimate + multiplier.
            mult = _pipeline_ram_mult(pipeline_id, priority)
            ram_need = _estimate_op_ram_gb(op, mult)

            # If it can never fit on any pool, mark as dead to avoid infinite churn.
            if ram_need > float(s.max_pool_ram) + 1e-9:
                s.dead_pipelines.add(pipeline_id)
                continue

            # If it doesn't fit on this pool, requeue (maybe another pool can take it).
            if ram_need > float(pool_max_ram) + 1e-9:
                q.append(pipeline_id)
                s.in_queue.add(pipeline_id)
                continue

            # Refresh effective avail (may differ for batch).
            eff_cpu, eff_ram = _effective_avail(avail_cpu, avail_ram)
            if ram_need > eff_ram + 1e-9:
                q.append(pipeline_id)
                s.in_queue.add(pipeline_id)
                continue

            # CPU allocation: allow scale-up when there is little/no backlog to reduce latency.
            allow_scale_up = (_total_backlog() <= 1)
            cpu_alloc = _compute_cpu_alloc(priority, eff_cpu, pool_max_cpu, allow_scale_up=allow_scale_up)
            if cpu_alloc < 1.0:
                q.append(pipeline_id)
                s.in_queue.add(pipeline_id)
                continue

            # Create the assignment.
            a = Assignment(
                ops=[op],
                cpu=cpu_alloc,
                ram=ram_need,
                priority=priority,
                pool_id=pool_id,
                pipeline_id=pipeline_id,
            )

            # Record mappings so results can update the right pipeline multiplier.
            planned_op_ids.add(id(op))
            s.op_to_pipeline[id(op)] = pipeline_id
            scheduled_ops_per_pipeline[pipeline_id] = scheduled_ops_per_pipeline.get(pipeline_id, 0) + 1

            # Requeue pipeline for potential future ops.
            q.append(pipeline_id)
            s.in_queue.add(pipeline_id)

            return a, float(avail_cpu) - float(cpu_alloc), float(avail_ram) - float(ram_need)

        return None, avail_cpu, avail_ram

    # ----------------------------
    # Ingest new pipelines
    # ----------------------------
    for p in pipelines:
        _enqueue_pipeline(p)

    # Early exit if nothing to react to (keeps simulator fast)
    if not pipelines and not results:
        return [], []

    # ----------------------------
    # Process results: retry OOM, drop non-OOM failures
    # ----------------------------
    for r in results:
        # Clear op->pipeline mappings for completed attempts (success or failure).
        for op in getattr(r, "ops", []) or []:
            op_id = id(op)
            pipeline_id = s.op_to_pipeline.pop(op_id, None)
            if pipeline_id is None:
                continue

            if r.failed():
                if _error_is_oom(getattr(r, "error", None)):
                    _bump_ram_mult(pipeline_id)
                else:
                    # Non-OOM failure: mark pipeline dead to prevent infinite retries.
                    s.dead_pipelines.add(pipeline_id)

    # ----------------------------
    # Scheduling
    # ----------------------------
    suspensions = []  # no preemption in this incremental policy
    assignments = []

    # Local view of pool availability so we can place multiple assignments per tick.
    avail_cpu = {}
    avail_ram = {}
    max_cpu = {}
    max_ram = {}
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu[pool_id] = float(pool.avail_cpu_pool)
        avail_ram[pool_id] = float(pool.avail_ram_pool)
        max_cpu[pool_id] = float(pool.max_cpu_pool)
        max_ram[pool_id] = float(pool.max_ram_pool)

    # Compute headroom to protect high priority latency.
    # If there's no high backlog, let batch use everything (avoid wasted capacity).
    enforce_headroom = _high_backlog()
    high_reserve_cpu_by_pool = {pid: s.high_reserve_frac_cpu * max_cpu[pid] for pid in range(s.executor.num_pools)}
    high_reserve_ram_by_pool = {pid: s.high_reserve_frac_ram * max_ram[pid] for pid in range(s.executor.num_pools)}

    planned_op_ids = set()
    scheduled_ops_per_pipeline = {}

    # Pool ordering:
    # - High priority: schedule on emptiest pools first (max available score).
    # - Batch: pack into most utilized pools first (min available score) to keep a "fast lane".
    high_pool_order = sorted(range(s.executor.num_pools),
                             key=lambda pid: _pool_score(avail_cpu[pid], avail_ram[pid]),
                             reverse=True)
    batch_pool_order = sorted(range(s.executor.num_pools),
                              key=lambda pid: _pool_score(avail_cpu[pid], avail_ram[pid]),
                              reverse=False)

    # 1) Schedule QUERY and INTERACTIVE first
    for pool_id in high_pool_order:
        # Try to fill the pool with high-priority work, but naturally limited by CPU caps and queue availability.
        while True:
            made = False
            for prio in (Priority.QUERY, Priority.INTERACTIVE):
                a, new_cpu, new_ram = _try_schedule_one_from_priority(
                    priority=prio,
                    pool_id=pool_id,
                    avail_cpu=avail_cpu[pool_id],
                    avail_ram=avail_ram[pool_id],
                    pool_max_cpu=max_cpu[pool_id],
                    pool_max_ram=max_ram[pool_id],
                    planned_op_ids=planned_op_ids,
                    scheduled_ops_per_pipeline=scheduled_ops_per_pipeline,
                    enforce_high_headroom_for_batch=False,
                    high_reserve_cpu=high_reserve_cpu_by_pool[pool_id],
                    high_reserve_ram=high_reserve_ram_by_pool[pool_id],
                )
                if a is not None:
                    assignments.append(a)
                    avail_cpu[pool_id], avail_ram[pool_id] = new_cpu, new_ram
                    made = True
                    break
            if not made:
                break

    # 2) Schedule BATCH, preserving headroom if high backlog exists
    for pool_id in batch_pool_order:
        while True:
            a, new_cpu, new_ram = _try_schedule_one_from_priority(
                priority=Priority.BATCH_PIPELINE,
                pool_id=pool_id,
                avail_cpu=avail_cpu[pool_id],
                avail_ram=avail_ram[pool_id],
                pool_max_cpu=max_cpu[pool_id],
                pool_max_ram=max_ram[pool_id],
                planned_op_ids=planned_op_ids,
                scheduled_ops_per_pipeline=scheduled_ops_per_pipeline,
                enforce_high_headroom_for_batch=enforce_headroom,
                high_reserve_cpu=high_reserve_cpu_by_pool[pool_id],
                high_reserve_ram=high_reserve_ram_by_pool[pool_id],
            )
            if a is None:
                break
            assignments.append(a)
            avail_cpu[pool_id], avail_ram[pool_id] = new_cpu, new_ram

    return suspensions, assignments
