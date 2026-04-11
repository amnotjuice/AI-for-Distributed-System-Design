# policy_key: scheduler_est_025
# reasoning_effort: low
# model: gpt-5.2-2025-12-11
# llm_cost: 0.040844
# generation_seconds: 45.05
# generated_at: 2026-04-01T00:01:31.072948
@register_scheduler_init(key="scheduler_est_025")
def scheduler_est_025_init(s):
    """Priority-aware, estimate-guided scheduler (incremental improvement over naive FIFO).

    Main ideas (kept intentionally simple):
      1) Maintain per-priority FIFO queues (QUERY > INTERACTIVE > BATCH).
      2) Use op.estimate.mem_peak_gb as a RAM floor to reduce OOM risk; allocate extra RAM generously.
      3) If high-priority work is waiting, keep a small headroom slice so batch doesn't consume everything.
      4) On failures, retry a few times with increased RAM floor (pipeline-level backoff) instead of dropping.
    """
    from collections import deque

    s.q_query = deque()
    s.q_interactive = deque()
    s.q_batch = deque()

    # Track which pipelines are currently enqueued to avoid duplicates
    s.enqueued_pipeline_ids = set()

    # Simple failure handling / retry boosting (pipeline-level, not per-op)
    s.pipeline_retry_count = {}   # pipeline_id -> int
    s.pipeline_ram_boost = {}     # pipeline_id -> float multiplier applied to mem estimate

    # Tunables (small, obvious improvements first)
    s.max_retries = 3
    s.initial_ram_boost = 1.0
    s.ram_boost_on_fail = 1.8
    s.max_ram_boost = 8.0

    # Keep headroom for high-priority if any is waiting
    s.reserve_frac_cpu_for_high = 0.25
    s.reserve_frac_ram_for_high = 0.25

    # Default RAM floor if estimate missing (GB)
    s.default_mem_floor_gb = 1.0


@register_scheduler(key="scheduler_est_025")
def scheduler_est_025_scheduler(s, results, pipelines):
    """
    Priority-aware multi-queue scheduler with estimate-guided RAM floors and simple retry logic.

    Returns:
      (suspensions, assignments)
    """
    # Helper functions are defined inside to keep the policy self-contained.
    def _get_priority_queue(priority):
        # Priority enum values assumed to exist per spec.
        if priority == Priority.QUERY:
            return s.q_query
        if priority == Priority.INTERACTIVE:
            return s.q_interactive
        return s.q_batch  # Priority.BATCH_PIPELINE (or any other) defaults to batch queue

    def _enqueue_pipeline(p):
        if p.pipeline_id in s.enqueued_pipeline_ids:
            return
        _get_priority_queue(p.priority).append(p)
        s.enqueued_pipeline_ids.add(p.pipeline_id)
        s.pipeline_retry_count.setdefault(p.pipeline_id, 0)
        s.pipeline_ram_boost.setdefault(p.pipeline_id, s.initial_ram_boost)

    def _dequeue_pipeline(p):
        # We don't remove from deque here (we pop from left), only clear the membership marker.
        s.enqueued_pipeline_ids.discard(p.pipeline_id)

    def _pipeline_done_or_give_up(p):
        status = p.runtime_status()
        if status.is_pipeline_successful():
            return True
        # Give up if too many failed operators accumulated and retries exceeded.
        # We keep this conservative; simulator may allow retries because FAILED is assignable.
        if s.pipeline_retry_count.get(p.pipeline_id, 0) > s.max_retries:
            return True
        return False

    def _has_any_high_waiting():
        return (len(s.q_query) > 0) or (len(s.q_interactive) > 0)

    def _pick_next_pipeline():
        # Strict priority: QUERY > INTERACTIVE > BATCH
        if s.q_query:
            return s.q_query.popleft()
        if s.q_interactive:
            return s.q_interactive.popleft()
        if s.q_batch:
            return s.q_batch.popleft()
        return None

    def _mem_estimate_gb(op):
        est = getattr(op, "estimate", None)
        if est is None:
            return None
        return getattr(est, "mem_peak_gb", None)

    def _ram_request_gb(pipeline, op, avail_ram, pool):
        # Use estimate as floor; extra RAM is "free" (just reduces capacity for other ops).
        est_gb = _mem_estimate_gb(op)
        if est_gb is None:
            floor = s.default_mem_floor_gb
        else:
            floor = max(float(est_gb), 0.0)

        boost = s.pipeline_ram_boost.get(pipeline.pipeline_id, s.initial_ram_boost)
        floor *= boost
        if floor <= 0:
            floor = s.default_mem_floor_gb

        # Allocate generously but don't consume everything if high-priority work is waiting.
        # For high priority we can be more aggressive.
        if pipeline.priority in (Priority.QUERY, Priority.INTERACTIVE):
            target = avail_ram  # try to finish fast / avoid OOM
        else:
            # For batch, avoid monopolizing RAM when high-priority work is queued.
            if _has_any_high_waiting():
                reserve = s.reserve_frac_ram_for_high * pool.max_ram_pool
                target = max(0.0, avail_ram - reserve)
                target = max(target, floor)
            else:
                target = avail_ram

        return min(avail_ram, max(floor, min(target, avail_ram)))

    def _cpu_request(pipeline, avail_cpu, pool):
        # CPU affects speed; keep it simple:
        # - High priority: take all available CPU in the pool.
        # - Batch: if high-priority waiting, cap batch to avoid blocking responsiveness.
        if avail_cpu <= 0:
            return 0

        if pipeline.priority in (Priority.QUERY, Priority.INTERACTIVE):
            return avail_cpu

        # batch
        if _has_any_high_waiting():
            reserve = s.reserve_frac_cpu_for_high * pool.max_cpu_pool
            cap = max(1, int(max(0, avail_cpu - reserve)))
            return max(1, min(avail_cpu, cap))
        return avail_cpu

    # In this incremental version we do not preempt because we don't have a reliable
    # way (from the provided interface) to enumerate running containers to suspend.
    suspensions = []
    assignments = []

    # Ingest new pipelines
    for p in pipelines:
        _enqueue_pipeline(p)

    # Process results: on failures, increase RAM boost and retry count for the pipeline.
    # We rely on FAILED being assignable; we do not drop immediately.
    for r in results:
        if not r.failed():
            continue
        # Best-effort: identify pipeline by r.ops[0].pipeline_id is not guaranteed.
        # We only have r.ops (operator objects). We try to read pipeline_id off the op if present;
        # otherwise we can't attribute precisely and just skip boosting.
        pid = None
        if getattr(r, "ops", None):
            op0 = r.ops[0] if r.ops else None
            if op0 is not None:
                pid = getattr(op0, "pipeline_id", None)

        if pid is None:
            # Cannot attribute failure; skip boosting.
            continue

        s.pipeline_retry_count[pid] = s.pipeline_retry_count.get(pid, 0) + 1
        prev = s.pipeline_ram_boost.get(pid, s.initial_ram_boost)
        s.pipeline_ram_boost[pid] = min(s.max_ram_boost, prev * s.ram_boost_on_fail)

    # Early exit if no changes
    if not pipelines and not results and not s.enqueued_pipeline_ids:
        return suspensions, assignments

    # Schedule per pool: fill capacity with strict-priority ready ops, multiple assignments per pool if possible.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool

        # Keep scheduling while resources exist and there's work.
        while avail_cpu > 0 and avail_ram > 0 and (s.q_query or s.q_interactive or s.q_batch):
            pipeline = _pick_next_pipeline()
            if pipeline is None:
                break

            # If pipeline is already completed or we decided to give up, drop it.
            if _pipeline_done_or_give_up(pipeline):
                _dequeue_pipeline(pipeline)
                continue

            status = pipeline.runtime_status()

            # Choose at most one ready operator (simple, incremental improvement).
            op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
            if not op_list:
                # Not currently schedulable; requeue at end to avoid spinning.
                _get_priority_queue(pipeline.priority).append(pipeline)
                continue

            # Compute resource request
            cpu_req = _cpu_request(pipeline, avail_cpu, pool)
            if cpu_req <= 0:
                # No CPU; requeue and move to next pool.
                _get_priority_queue(pipeline.priority).appendleft(pipeline)
                break

            ram_req = _ram_request_gb(pipeline, op_list[0], avail_ram, pool)
            if ram_req <= 0:
                _get_priority_queue(pipeline.priority).appendleft(pipeline)
                break

            # If we can't fit the floor, don't assign; requeue and stop for this pool.
            # (extra RAM is optional, but floor is required for OOM safety)
            est_gb = _mem_estimate_gb(op_list[0])
            floor = (s.default_mem_floor_gb if est_gb is None else max(float(est_gb), 0.0))
            floor *= s.pipeline_ram_boost.get(pipeline.pipeline_id, s.initial_ram_boost)
            floor = max(floor, s.default_mem_floor_gb)
            if avail_ram < floor:
                _get_priority_queue(pipeline.priority).appendleft(pipeline)
                break

            assignments.append(
                Assignment(
                    ops=op_list,
                    cpu=cpu_req,
                    ram=ram_req,
                    priority=pipeline.priority,
                    pool_id=pool_id,
                    pipeline_id=pipeline.pipeline_id,
                )
            )

            # Consume pool availability for additional placements in this tick.
            avail_cpu -= cpu_req
            avail_ram -= ram_req

            # Requeue the pipeline for subsequent operators.
            # (We keep it enqueued until completion/give-up.)
            _get_priority_queue(pipeline.priority).append(pipeline)

    return suspensions, assignments
