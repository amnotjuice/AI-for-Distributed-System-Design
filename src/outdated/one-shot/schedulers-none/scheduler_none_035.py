# policy_key: scheduler_none_035
# reasoning_effort: none
# model: gpt-5.2-2025-12-11
# llm_cost: 0.038653
# generation_seconds: 37.67
# generated_at: 2026-03-14T02:06:50.963409
@register_scheduler_init(key="scheduler_none_035")
def scheduler_none_035_init(s):
    """Priority-aware FIFO with gentle improvements over naive baseline.

    Improvements over the example FIFO:
      1) Separate waiting queues per priority to reduce head-of-line blocking.
      2) Reserve a small fraction of each pool for high-priority (QUERY/INTERACTIVE).
      3) Basic OOM-aware retry: if an op fails with OOM at some RAM, bump RAM next time for that op.
      4) Light admission shaping: assign smaller "starter" CPU to interactive/query to reduce contention.

    The policy stays intentionally simple and robust:
      - No complex preemption logic (avoids churn without needing container inventory APIs).
      - One assignment per pool per tick (like the baseline), but with priority selection and sizing.
    """
    # Per-priority FIFO queues
    s.waiting_by_prio = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }

    # Remember last known good/bad RAM per (pipeline_id, op_id) to avoid repeated OOMs.
    # We store only an increasing lower-bound to keep logic monotone and safe.
    s.op_ram_floor = {}  # (pipeline_id, op_id) -> ram_floor

    # Keep last observed assignment sizes for quick reference (optional but helpful)
    s.last_assign = {}  # (pipeline_id, op_id) -> (cpu, ram)

    # Tuning knobs (conservative defaults)
    s.reserve_hi_frac_cpu = 0.25  # reserve for QUERY/INTERACTIVE (combined)
    s.reserve_hi_frac_ram = 0.25
    s.min_cpu_slice = 1.0         # don't assign less than this if available
    s.min_ram_slice = 0.25        # in "pool units" - we cap by availability anyway
    s.hi_cpu_cap_frac = 0.5       # cap interactive/query cpu per assignment to reduce interference
    s.batch_cpu_cap_frac = 1.0    # batch can take more if available
    s.oom_ram_bump_factor = 1.5   # multiplicative bump on OOM
    s.oom_ram_bump_add = 0.5      # additive bump to ensure progress when small
    s.max_attempt_ram_frac = 1.0  # never request > 100% of pool max RAM for a single op


def _prio_rank(priority):
    # Lower is higher priority
    if priority == Priority.QUERY:
        return 0
    if priority == Priority.INTERACTIVE:
        return 1
    return 2  # BATCH_PIPELINE


def _iter_priorities_high_to_low():
    return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]


def _extract_op_id(op):
    # Best-effort stable identifier for per-op state.
    # Many simulators use .op_id; fallback to .operator_id; finally object's id.
    return getattr(op, "op_id", getattr(op, "operator_id", id(op)))


def _is_oom_error(err):
    if err is None:
        return False
    msg = str(err).lower()
    return ("oom" in msg) or ("out of memory" in msg) or ("memoryerror" in msg)


def _pool_reservation(pool, prio):
    # Return (cpu_reserved, ram_reserved) to keep free for high-priority work.
    if prio in (Priority.QUERY, Priority.INTERACTIVE):
        return 0.0, 0.0
    # For batch, keep some headroom for high priority arrivals.
    return (pool.max_cpu_pool * 0.25, pool.max_ram_pool * 0.25)


def _compute_request(s, pool, pipeline, op, avail_cpu, avail_ram):
    """Choose CPU/RAM request for a single op given pool availability and priority."""
    prio = pipeline.priority
    op_id = _extract_op_id(op)
    key = (pipeline.pipeline_id, op_id)

    # Apply reservation for batch so it doesn't consume all headroom.
    cpu_res, ram_res = _pool_reservation(pool, prio)
    cpu_budget = max(0.0, avail_cpu - cpu_res)
    ram_budget = max(0.0, avail_ram - ram_res)
    if cpu_budget <= 0 or ram_budget <= 0:
        return 0.0, 0.0

    # Priority-aware CPU caps: start smaller for interactive/query to reduce interference.
    if prio in (Priority.QUERY, Priority.INTERACTIVE):
        cpu_cap = max(s.min_cpu_slice, pool.max_cpu_pool * s.hi_cpu_cap_frac)
    else:
        cpu_cap = max(s.min_cpu_slice, pool.max_cpu_pool * s.batch_cpu_cap_frac)

    # Default "starter" CPU: give a fair slice, not the entire pool.
    # This reduces tail latency under contention and improves overall responsiveness.
    if prio == Priority.QUERY:
        cpu_req = min(cpu_budget, cpu_cap, max(s.min_cpu_slice, pool.max_cpu_pool * 0.25))
    elif prio == Priority.INTERACTIVE:
        cpu_req = min(cpu_budget, cpu_cap, max(s.min_cpu_slice, pool.max_cpu_pool * 0.35))
    else:
        # Batch: can take more, but still not necessarily all (keeps room for others in multi-tenant).
        cpu_req = min(cpu_budget, cpu_cap, max(s.min_cpu_slice, pool.max_cpu_pool * 0.6))

    # RAM request: start with a conservative fraction of available,
    # but enforce any known floor from prior OOMs.
    ram_floor = s.op_ram_floor.get(key, 0.0)

    if prio in (Priority.QUERY, Priority.INTERACTIVE):
        ram_guess = max(s.min_ram_slice, pool.max_ram_pool * 0.25)
    else:
        ram_guess = max(s.min_ram_slice, pool.max_ram_pool * 0.35)

    ram_req = min(ram_budget, max(ram_floor, ram_guess))

    # Hard cap by pool max (single-op cannot exceed pool capacity)
    ram_req = min(ram_req, pool.max_ram_pool * s.max_attempt_ram_frac)
    cpu_req = min(cpu_req, pool.max_cpu_pool)

    # Ensure strictly positive if possible
    if cpu_req <= 0 or ram_req <= 0:
        return 0.0, 0.0
    return cpu_req, ram_req


def _enqueue_pipeline(s, pipeline):
    prio = pipeline.priority
    if prio not in s.waiting_by_prio:
        # Unknown priority: treat as batch
        prio = Priority.BATCH_PIPELINE
    s.waiting_by_prio[prio].append(pipeline)


def _dequeue_next_runnable_pipeline(s):
    """Pick the next pipeline to consider, prioritizing higher priority queues.

    We do not guarantee strict fairness, but batch pipelines will run when
    there is no high-priority runnable work or when reservations prevent batch from starving.
    """
    for prio in _iter_priorities_high_to_low():
        q = s.waiting_by_prio[prio]
        while q:
            p = q.pop(0)
            status = p.runtime_status()
            # Drop completed pipelines
            if status.is_pipeline_successful():
                continue
            # If there are failed ops, we still keep pipeline to allow retry logic
            return p
    return None


def _requeue_pipeline_front(s, pipeline):
    prio = pipeline.priority
    if prio not in s.waiting_by_prio:
        prio = Priority.BATCH_PIPELINE
    s.waiting_by_prio[prio].insert(0, pipeline)


def _requeue_pipeline_back(s, pipeline):
    prio = pipeline.priority
    if prio not in s.waiting_by_prio:
        prio = Priority.BATCH_PIPELINE
    s.waiting_by_prio[prio].append(pipeline)


@register_scheduler(key="scheduler_none_035")
def scheduler_none_035(s, results, pipelines):
    """
    Priority-aware scheduler with small incremental improvements:
      - per-priority FIFO queues
      - headroom reservation for high priority
      - OOM-aware RAM bump retry
      - priority-aware right-sizing (starter CPU/RAM) to improve latency under contention
    """
    # Incorporate new arrivals
    for p in pipelines:
        _enqueue_pipeline(s, p)

    # Process execution results to learn from OOMs (RAM floor increases monotonically)
    for r in results:
        if r is None:
            continue
        if not getattr(r, "failed", lambda: False)():
            continue
        if not _is_oom_error(getattr(r, "error", None)):
            continue

        # Raise RAM floor for each op in the failed container
        for op in getattr(r, "ops", []) or []:
            op_id = _extract_op_id(op)
            # We cannot always recover pipeline_id from result; use stored pipeline_id if present
            # ExecutionResult often has .pipeline_id, but it's not guaranteed in the prompt.
            pipeline_id = getattr(r, "pipeline_id", None)
            if pipeline_id is None:
                # Fallback: do not update if pipeline_id missing
                continue
            key = (pipeline_id, op_id)

            prev = s.op_ram_floor.get(key, 0.0)
            base = max(prev, float(getattr(r, "ram", 0.0) or 0.0))
            bumped = max(base + s.oom_ram_bump_add, base * s.oom_ram_bump_factor)
            # Don't exceed pool maximum if we can access it
            pool_id = getattr(r, "pool_id", None)
            if pool_id is not None and 0 <= pool_id < s.executor.num_pools:
                bumped = min(bumped, s.executor.pools[pool_id].max_ram_pool * s.max_attempt_ram_frac)
            s.op_ram_floor[key] = max(prev, bumped)

    # Early exit if nothing to do
    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    # Schedule at most one assignment per pool per tick (keeps behavior close to baseline)
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool
        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # Try a few pipelines to find a runnable op for this pool
        # (prevents getting stuck on a pipeline whose next ops aren't ready)
        tried = 0
        max_try = 25

        chosen = None
        chosen_ops = None
        chosen_cpu = 0.0
        chosen_ram = 0.0

        while tried < max_try:
            tried += 1
            pipeline = _dequeue_next_runnable_pipeline(s)
            if pipeline is None:
                break

            status = pipeline.runtime_status()

            # Get at most one runnable op whose parents are complete
            op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
            if not op_list:
                # Not runnable yet; requeue behind peers of same priority to avoid spinning
                _requeue_pipeline_back(s, pipeline)
                continue

            op = op_list[0]
            cpu_req, ram_req = _compute_request(s, pool, pipeline, op, avail_cpu, avail_ram)

            # If we cannot fit due to reservations/budgets, requeue and try other pipelines.
            if cpu_req <= 0 or ram_req <= 0 or cpu_req > avail_cpu or ram_req > avail_ram:
                _requeue_pipeline_back(s, pipeline)
                continue

            chosen = pipeline
            chosen_ops = op_list
            chosen_cpu = cpu_req
            chosen_ram = ram_req
            break

        if chosen is None:
            continue

        # Create the assignment
        assignment = Assignment(
            ops=chosen_ops,
            cpu=chosen_cpu,
            ram=chosen_ram,
            priority=chosen.priority,
            pool_id=pool_id,
            pipeline_id=chosen.pipeline_id,
        )
        assignments.append(assignment)

        # Requeue the pipeline (front) so it can continue quickly if it has more ready ops later.
        # This helps interactive pipelines finish sooner once admitted.
        _requeue_pipeline_front(s, chosen)

        # Only one assignment per pool per tick (baseline-like)
        continue

    return suspensions, assignments
