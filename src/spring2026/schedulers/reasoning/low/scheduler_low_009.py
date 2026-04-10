# policy_key: scheduler_low_009
# reasoning_effort: low
# model: gpt-5.2-2025-12-11
# llm_cost: 0.054891
# generation_seconds: 39.78
# generated_at: 2026-04-09T21:08:55.176308
@register_scheduler_init(key="scheduler_low_009")
def scheduler_low_009_init(s):
    """
    Priority- and completion-aware scheduler.

    Goals:
      - Minimize weighted end-to-end latency with strong emphasis on QUERY and INTERACTIVE.
      - Avoid failures (each failure is extremely costly), by biasing RAM upward after any failure.
      - Avoid starvation via simple aging for BATCH (and all classes).
      - Keep implementation simple/stable: no aggressive preemption; allocate at most a few ops per assignment.

    Core ideas:
      1) Weighted priority queue with aging: QUERY > INTERACTIVE > BATCH, but older pipelines rise.
      2) Conservative RAM: allocate a fraction of pool RAM; on any failure, increase per-pipeline RAM factor.
      3) Slight multi-op batching for high priority: run up to 2 ready ops from the same pipeline when possible.
    """
    s.waiting_queue = []  # list of Pipeline
    s.enqueued_at_tick = {}  # pipeline_id -> tick when first seen (or last re-enqueued)
    s.ram_factor = {}  # pipeline_id -> multiplicative factor to reduce OOM likelihood after failures
    s.fail_count = {}  # pipeline_id -> number of observed failures
    s.tick = 0  # logical time for aging


def _prio_weight(priority):
    # Larger means more important.
    if priority == Priority.QUERY:
        return 1000
    if priority == Priority.INTERACTIVE:
        return 500
    return 100  # Priority.BATCH_PIPELINE (or others)


def _get_result_pipeline_id(r):
    # Best-effort extraction of pipeline_id from ExecutionResult.
    # Different simulators attach pipeline_id at different places; try common patterns.
    pid = getattr(r, "pipeline_id", None)
    if pid is not None:
        return pid
    ops = getattr(r, "ops", None) or []
    if ops:
        op0 = ops[0]
        pid = getattr(op0, "pipeline_id", None)
        if pid is not None:
            return pid
        # Some models store it as op.pipeline.pipeline_id
        pipeline_obj = getattr(op0, "pipeline", None)
        if pipeline_obj is not None:
            return getattr(pipeline_obj, "pipeline_id", None)
    return None


def _compute_request(priority, pool, avail_cpu, avail_ram, ram_factor):
    """
    Choose CPU/RAM per assignment.
    - RAM: prioritize avoiding OOM and costly 720s penalties; high priority gets larger fractions.
    - CPU: provide decent parallelism, but avoid monopolizing the pool completely.
    """
    if priority == Priority.QUERY:
        base_ram_frac = 0.70
        base_cpu_frac = 0.80
        min_cpu = 1
    elif priority == Priority.INTERACTIVE:
        base_ram_frac = 0.60
        base_cpu_frac = 0.70
        min_cpu = 1
    else:
        base_ram_frac = 0.40
        base_cpu_frac = 0.55
        min_cpu = 1

    # Target based on pool capacity, then clamp to availability.
    target_ram = int(max(1, pool.max_ram_pool * base_ram_frac))
    target_cpu = int(max(min_cpu, pool.max_cpu_pool * base_cpu_frac))

    # Apply per-pipeline RAM safety multiplier (increases after failures).
    target_ram = int(max(1, target_ram * max(1.0, ram_factor)))

    # Clamp to current available resources.
    req_ram = int(min(avail_ram, target_ram))
    req_cpu = int(min(avail_cpu, target_cpu))

    # Avoid zero allocations.
    if req_cpu <= 0 or req_ram <= 0:
        return 0, 0
    return req_cpu, req_ram


def _max_ops_for_priority(priority):
    # Small batching to reduce end-to-end latency for high-priority pipelines.
    if priority == Priority.QUERY:
        return 2
    if priority == Priority.INTERACTIVE:
        return 2
    return 1


@register_scheduler(key="scheduler_low_009")
def scheduler_low_009(s, results, pipelines):
    """
    Scheduler step:
      - Ingest new pipelines into a single global waiting queue.
      - Update per-pipeline RAM factor on failures to reduce future OOM/penalty risk.
      - For each pool, repeatedly select the best next pipeline (priority + aging),
        then assign up to N ready ops with a conservative resource request.
    """
    s.tick += 1

    # Incorporate new arrivals.
    for p in pipelines:
        s.waiting_queue.append(p)
        if p.pipeline_id not in s.enqueued_at_tick:
            s.enqueued_at_tick[p.pipeline_id] = s.tick
        s.ram_factor.setdefault(p.pipeline_id, 1.0)
        s.fail_count.setdefault(p.pipeline_id, 0)

    # React to results (especially failures) by increasing RAM factor for that pipeline.
    for r in results or []:
        if hasattr(r, "failed") and r.failed():
            pid = _get_result_pipeline_id(r)
            if pid is not None:
                s.fail_count[pid] = s.fail_count.get(pid, 0) + 1
                # Aggressive ramp to avoid repeated 720s penalties:
                # 1st fail: 1.5x, 2nd: 2.25x, 3rd: 3.0x (capped)
                prev = s.ram_factor.get(pid, 1.0)
                bumped = min(3.0, max(prev * 1.5, 1.5))
                s.ram_factor[pid] = bumped
                # Give it a "fresh" enqueue time so it doesn't get buried after requeue.
                s.enqueued_at_tick[pid] = min(s.enqueued_at_tick.get(pid, s.tick), s.tick)

    # Early exit if nothing to do.
    if not pipelines and not results:
        return [], []

    suspensions = []  # No preemption in this policy to avoid churn and wasted work.
    assignments = []

    # Clean queue: drop completed/failed pipelines; keep active ones.
    active = []
    for p in s.waiting_queue:
        st = p.runtime_status()
        # If any operators are FAILED, we still keep it: simulator may allow retries by re-assigning FAILED ops.
        # But if pipeline is successful, drop.
        if st.is_pipeline_successful():
            continue
        active.append(p)
        s.ram_factor.setdefault(p.pipeline_id, 1.0)
        s.fail_count.setdefault(p.pipeline_id, 0)
        s.enqueued_at_tick.setdefault(p.pipeline_id, s.tick)
    s.waiting_queue = active

    # Build an ordered list each tick based on priority + aging.
    def pipeline_rank(p):
        base = _prio_weight(p.priority)
        age = max(0, s.tick - s.enqueued_at_tick.get(p.pipeline_id, s.tick))
        # Aging is modest so high priority stays protected, but batch makes progress.
        return base + age * 2

    # For each pool, schedule as much as we can.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = int(pool.avail_cpu_pool)
        avail_ram = int(pool.avail_ram_pool)
        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # Repeatedly pick best runnable pipeline for this pool.
        # To reduce O(n^2) churn, we do a bounded number of placements per pool per tick.
        placements = 0
        max_placements = 4

        while placements < max_placements and avail_cpu > 0 and avail_ram > 0 and s.waiting_queue:
            # Sort by rank descending; stable selection each iteration.
            s.waiting_queue.sort(key=pipeline_rank, reverse=True)

            picked_index = None
            picked_ops = None
            picked_pipeline = None

            # Find first pipeline with ready ops.
            for i, p in enumerate(s.waiting_queue):
                st = p.runtime_status()

                # Prefer PENDING and FAILED (retryable) ops whose parents are complete.
                max_ops = _max_ops_for_priority(p.priority)
                op_list = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:max_ops]
                if op_list:
                    picked_index = i
                    picked_ops = op_list
                    picked_pipeline = p
                    break

            if picked_pipeline is None:
                break

            # Compute request for the picked pipeline.
            rf = s.ram_factor.get(picked_pipeline.pipeline_id, 1.0)
            req_cpu, req_ram = _compute_request(
                picked_pipeline.priority, pool, avail_cpu, avail_ram, rf
            )

            # If we can't fit even minimal resources, stop scheduling in this pool.
            if req_cpu <= 0 or req_ram <= 0:
                break

            assignments.append(
                Assignment(
                    ops=picked_ops,
                    cpu=req_cpu,
                    ram=req_ram,
                    priority=picked_pipeline.priority,
                    pool_id=pool_id,
                    pipeline_id=picked_pipeline.pipeline_id,
                )
            )

            # Update available resources for additional placements.
            avail_cpu -= req_cpu
            avail_ram -= req_ram
            placements += 1

            # Move picked pipeline to the back to reduce head-of-line repeated selection in same tick.
            # (It will bubble back up if it remains highest-ranked next iteration.)
            s.waiting_queue.append(s.waiting_queue.pop(picked_index))

    return suspensions, assignments
