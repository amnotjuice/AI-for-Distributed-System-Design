# policy_key: scheduler_est_043
# reasoning_effort: low
# model: gpt-5.2-2025-12-11
# llm_cost: 0.039024
# generation_seconds: 38.44
# generated_at: 2026-04-01T00:13:23.983274
@register_scheduler_init(key="scheduler_est_043")
def scheduler_est_043_init(s):
    """Priority-aware FIFO with simple headroom reservation + OOM-aware memory floors.

    Improvements over naive FIFO:
      1) Priority queues: always schedule QUERY/INTERACTIVE before BATCH.
      2) Headroom reservation: avoid letting BATCH consume the entire pool, preserving latency for new high-priority arrivals.
      3) OOM retry with increased RAM floor: if an op fails (likely OOM), raise its per-op RAM floor and retry.
      4) Conservative single-op-per-pool-per-tick: keeps behavior simple and stable while improving tail latency.

    Notes:
      - We do not rely on inspecting running containers beyond pool availability (portable across simulator variants).
      - RAM is treated as a safety constraint; extra RAM is "free" but limited by pool availability.
      - CPU is the performance lever; for high-priority we allocate aggressively, for batch we cap to preserve headroom.
    """
    # FIFO queues per priority class
    s.q_query = []
    s.q_interactive = []
    s.q_batch = []

    # Track pipelines by id (lets us re-enqueue on failures without losing references)
    s.pipelines_by_id = {}

    # Per-operator RAM floors (GB) learned from failures; key by object identity (stable within simulation)
    s.op_ram_floor_gb = {}

    # Reservation fractions per pool (keep capacity available for high-priority work)
    s.reserve_cpu_frac = 0.30
    s.reserve_ram_frac = 0.30

    # Batch CPU cap per assignment (avoid batch taking entire pool even when no headroom is needed)
    s.batch_cpu_cap_frac = 0.60

    # A small minimum RAM floor to avoid pathologically tiny allocations
    s.min_ram_gb = 0.25


def _enqueue_pipeline(s, pipeline):
    """Enqueue pipeline into the appropriate FIFO queue if not already present."""
    # Avoid duplicated enqueues by using a simple presence check across queues.
    # (O(n) but queues are typically not huge in the simulator; keeps code simple.)
    pid = pipeline.pipeline_id
    for q in (s.q_query, s.q_interactive, s.q_batch):
        for p in q:
            if p.pipeline_id == pid:
                return

    if pipeline.priority == Priority.QUERY:
        s.q_query.append(pipeline)
    elif pipeline.priority == Priority.INTERACTIVE:
        s.q_interactive.append(pipeline)
    else:
        s.q_batch.append(pipeline)


def _pick_next_pipeline_with_ready_op(s, queue):
    """Pop pipelines FIFO until one has a ready assignable op; return (pipeline, op_list) or (None, None).

    Pipelines that are complete are dropped; pipelines without a ready op are requeued.
    """
    if not queue:
        return None, None

    requeue = []
    chosen_pipeline = None
    chosen_ops = None

    while queue:
        p = queue.pop(0)
        status = p.runtime_status()

        # Drop completed pipelines
        if status.is_pipeline_successful():
            continue

        # Get the next ready operator (parents complete)
        op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
        if op_list:
            chosen_pipeline = p
            chosen_ops = op_list
            break

        # Not ready yet; requeue
        requeue.append(p)

    # Preserve FIFO for those we skipped
    if requeue:
        queue.extend(requeue)

    return chosen_pipeline, chosen_ops


def _compute_ram_request_gb(s, op, avail_ram_gb, priority):
    """Compute RAM request using estimator + learned floors, with a little safety slack when possible."""
    est = getattr(getattr(op, "estimate", None), "mem_peak_gb", None)
    floor = s.op_ram_floor_gb.get(op, 0.0)

    base = max(s.min_ram_gb, float(est) if est is not None else 0.0, float(floor))

    # Give some slack to reduce OOM retries; more slack for high-priority work.
    slack_mult = 1.35 if priority in (Priority.QUERY, Priority.INTERACTIVE) else 1.20
    target = base * slack_mult

    # Extra RAM is "free" but bounded by availability.
    # Ensure we never request less than base; if base doesn't fit, request whatever is available (will likely OOM/retry).
    if avail_ram_gb <= 0:
        return 0.0
    if base > avail_ram_gb:
        return float(avail_ram_gb)
    return float(min(avail_ram_gb, target))


def _compute_cpu_request(s, pool, avail_cpu, priority, is_batch):
    """Compute CPU request. High priority: aggressive. Batch: capped to preserve headroom."""
    if avail_cpu <= 0:
        return 0

    # Allocate integer-ish CPU if the simulator expects that; but keep as numeric type (can be float in some sims).
    max_cpu = pool.max_cpu_pool

    if is_batch:
        cap = max(1.0, max_cpu * s.batch_cpu_cap_frac)
        return min(avail_cpu, cap)

    # QUERY/INTERACTIVE: allocate most available CPU to minimize latency.
    return min(avail_cpu, max_cpu)


@register_scheduler(key="scheduler_est_043")
def scheduler_est_043(s, results, pipelines):
    """Priority-aware scheduler with reserved headroom and OOM-aware retries."""
    # Ingest new pipelines
    for p in pipelines:
        s.pipelines_by_id[p.pipeline_id] = p
        _enqueue_pipeline(s, p)

    # Process results: on failure, increase RAM floor for failed ops and ensure pipeline is queued for retry
    for r in results:
        if r.failed():
            # Heuristic: if it failed, likely OOM or underprovisioning; raise RAM floor for those ops.
            # Use observed allocated RAM as a lower bound, then increase further for next retry.
            bump_mult = 1.50
            for op in getattr(r, "ops", []) or []:
                prev = s.op_ram_floor_gb.get(op, 0.0)
                observed = float(getattr(r, "ram", 0.0) or 0.0)
                new_floor = max(prev, observed * bump_mult, s.min_ram_gb)
                s.op_ram_floor_gb[op] = new_floor

            # Re-enqueue the owning pipeline so failed ops can be retried
            pid = getattr(r, "pipeline_id", None)
            if pid is not None and pid in s.pipelines_by_id:
                _enqueue_pipeline(s, s.pipelines_by_id[pid])

    # Early exit: no new work and no results to react to
    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    # One assignment per pool per tick (simple and stable)
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool

        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # Compute reserved headroom for high priority
        reserve_cpu = pool.max_cpu_pool * s.reserve_cpu_frac
        reserve_ram = pool.max_ram_pool * s.reserve_ram_frac

        # Try scheduling priority classes in order; batch only uses leftover beyond reservation
        picked = None  # (pipeline, op_list)
        picked_priority = None
        allowed_cpu = avail_cpu
        allowed_ram = avail_ram

        # 1) QUERY
        p, op_list = _pick_next_pipeline_with_ready_op(s, s.q_query)
        if p is not None:
            picked = (p, op_list)
            picked_priority = Priority.QUERY
            allowed_cpu = avail_cpu
            allowed_ram = avail_ram

        # 2) INTERACTIVE
        if picked is None:
            p, op_list = _pick_next_pipeline_with_ready_op(s, s.q_interactive)
            if p is not None:
                picked = (p, op_list)
                picked_priority = Priority.INTERACTIVE
                allowed_cpu = avail_cpu
                allowed_ram = avail_ram

        # 3) BATCH (only if we can preserve headroom)
        if picked is None:
            # If we're already below reservation, don't start batch work.
            cpu_for_batch = avail_cpu - reserve_cpu
            ram_for_batch = avail_ram - reserve_ram
            if cpu_for_batch > 0 and ram_for_batch > 0:
                p, op_list = _pick_next_pipeline_with_ready_op(s, s.q_batch)
                if p is not None:
                    picked = (p, op_list)
                    picked_priority = p.priority
                    allowed_cpu = cpu_for_batch
                    allowed_ram = ram_for_batch

        if picked is None:
            continue

        pipeline, op_list = picked
        op = op_list[0]

        # Size resources
        is_batch = pipeline.priority == Priority.BATCH_PIPELINE
        cpu_req = _compute_cpu_request(s, pool, allowed_cpu, pipeline.priority, is_batch=is_batch)
        ram_req = _compute_ram_request_gb(s, op, allowed_ram, pipeline.priority)

        # If we cannot allocate meaningful resources, requeue and skip
        if cpu_req <= 0 or ram_req <= 0:
            _enqueue_pipeline(s, pipeline)
            continue

        assignment = Assignment(
            ops=op_list,
            cpu=cpu_req,
            ram=ram_req,
            priority=pipeline.priority,
            pool_id=pool_id,
            pipeline_id=pipeline.pipeline_id,
        )
        assignments.append(assignment)

        # Requeue pipeline for subsequent operators (if any); preserves FIFO-ish progression
        _enqueue_pipeline(s, pipeline)

    return suspensions, assignments
