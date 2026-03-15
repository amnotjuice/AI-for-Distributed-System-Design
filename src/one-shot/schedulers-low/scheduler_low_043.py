# policy_key: scheduler_low_043
# reasoning_effort: low
# model: gpt-5.2-2025-12-11
# llm_cost: 0.053860
# generation_seconds: 56.09
# generated_at: 2026-03-14T02:35:38.967884
@register_scheduler_init(key="scheduler_low_043")
def scheduler_low_043_init(s):
    """
    Priority-aware, small-step improvement over naive FIFO.

    Key changes vs naive:
    - Maintain per-priority FIFO queues (QUERY > INTERACTIVE > BATCH_PIPELINE).
    - Right-size per-operator CPU/RAM (avoid giving an entire pool to one op).
    - Simple headroom reservation: keep some CPU/RAM available so high-priority
      work is less likely to queue behind batch.
    - Failure-aware retries: if an operator fails with an OOM-like error, retry it
      with increased RAM; otherwise mark it as non-retryable to avoid thrashing.
    """
    from collections import deque

    # Waiting pipelines by priority
    s.q_query = deque()
    s.q_interactive = deque()
    s.q_batch = deque()

    # Per-operator hints (keyed by id(op)) learned from outcomes
    s.op_ram_hint = {}          # id(op) -> suggested RAM
    s.op_cpu_hint = {}          # id(op) -> suggested CPU
    s.op_no_retry = set()       # id(op) -> do not retry if it failed non-OOM

    # Tuning knobs (small, safe improvements over naive)
    s.reserve_frac_cpu = 0.25   # keep this fraction for potential high-priority arrivals
    s.reserve_frac_ram = 0.25

    # Default sizing by priority (fractions of pool maxima)
    s.default_cpu_frac = {
        Priority.QUERY: 0.60,
        Priority.INTERACTIVE: 0.50,
        Priority.BATCH_PIPELINE: 0.25,
    }
    s.default_ram_frac = {
        Priority.QUERY: 0.60,
        Priority.INTERACTIVE: 0.50,
        Priority.BATCH_PIPELINE: 0.30,
    }

    # Minimum allocations (avoid issuing unusable tiny containers)
    s.min_cpu = 1.0
    s.min_ram = 0.25  # in "RAM units" consistent with simulator config

    # Limits to prevent over-allocating a single op (improves parallelism)
    s.max_cpu_per_op_frac = 0.80
    s.max_ram_per_op_frac = 0.80


@register_scheduler(key="scheduler_low_043")
def scheduler_low_043_scheduler(s, results, pipelines):
    """
    Priority queues + headroom reservation + OOM-aware RAM backoff.

    Scheduling outline per tick:
    1) Incorporate new pipelines into the appropriate priority queue.
    2) Learn from execution results:
       - If failed and looks like OOM => increase RAM hint for that op and allow retry.
       - If failed and not OOM => mark op as non-retryable (avoid endless retries).
    3) Assign ready operators:
       - Always schedule higher priority first.
       - Allow batch only if it does not consume reserved headroom while there is
         any queued high-priority work.
       - Right-size CPU/RAM using per-op hints if present, else defaults.
       - Place on pools in order of most available resources.
    """
    from collections import deque

    def _looks_like_oom(err):
        if err is None:
            return False
        txt = str(err).lower()
        # Broad match (simulators and real systems vary in wording)
        return ("oom" in txt) or ("out of memory" in txt) or ("killed" in txt and "memory" in txt) or ("memoryerror" in txt)

    def _queue_for_priority(prio):
        if prio == Priority.QUERY:
            return s.q_query
        if prio == Priority.INTERACTIVE:
            return s.q_interactive
        return s.q_batch

    def _any_high_prio_waiting():
        return (len(s.q_query) > 0) or (len(s.q_interactive) > 0)

    def _clamp(x, lo, hi):
        return max(lo, min(hi, x))

    def _pick_ready_op(pipeline):
        status = pipeline.runtime_status()
        # Do not schedule completed pipelines
        if status.is_pipeline_successful():
            return None

        # Prefer ready ops whose parents are complete
        ready = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        if not ready:
            return None

        # Filter out ops we decided are non-retryable (typically non-OOM failures)
        for op in ready:
            if id(op) in s.op_no_retry:
                continue
            return op
        return None

    def _compute_request(pool, prio, op):
        # If we learned a hint for this op, use it; else default fractions of pool max.
        max_cpu = float(pool.max_cpu_pool)
        max_ram = float(pool.max_ram_pool)

        default_cpu = max_cpu * float(s.default_cpu_frac.get(prio, 0.25))
        default_ram = max_ram * float(s.default_ram_frac.get(prio, 0.30))

        cpu = float(s.op_cpu_hint.get(id(op), default_cpu))
        ram = float(s.op_ram_hint.get(id(op), default_ram))

        # Limit per-op size to avoid monopolizing a pool and to improve latency via parallelism
        cpu = _clamp(cpu, s.min_cpu, max_cpu * s.max_cpu_per_op_frac)
        ram = _clamp(ram, s.min_ram, max_ram * s.max_ram_per_op_frac)
        return cpu, ram

    # 1) Enqueue new pipelines by priority
    for p in pipelines:
        _queue_for_priority(p.priority).append(p)

    # Early exit if no new info
    if not pipelines and not results:
        return [], []

    # 2) Learn from results (RAM backoff on OOM)
    for r in results:
        # r.ops is a list of operator objects
        if not getattr(r, "ops", None):
            continue
        failed = False
        try:
            failed = r.failed()
        except Exception:
            failed = bool(getattr(r, "error", None))

        if not failed:
            # On success, we keep hints as-is (conservative; avoids oscillations).
            continue

        is_oom = _looks_like_oom(getattr(r, "error", None))
        for op in r.ops:
            op_id = id(op)
            if is_oom:
                # Increase RAM hint aggressively to stop repeated OOMs.
                prev = float(s.op_ram_hint.get(op_id, float(getattr(r, "ram", 0.0) or 0.0)))
                base = float(getattr(r, "ram", 0.0) or prev or 0.0)
                # If base is unknown/zero, still bump to a reasonable minimum.
                if base <= 0:
                    base = max(s.min_ram, 1.0)
                s.op_ram_hint[op_id] = max(prev, base * 2.0)
                # Do not mark no-retry on OOM.
                if op_id in s.op_no_retry:
                    s.op_no_retry.discard(op_id)
            else:
                # Non-OOM failures likely won't be fixed by resizing: avoid infinite churn.
                s.op_no_retry.add(op_id)

    suspensions = []
    assignments = []

    # 3) Assign ready work with priority and headroom.
    # Order pools by "most headroom" each tick to reduce fragmentation effects.
    pool_order = list(range(s.executor.num_pools))
    pool_order.sort(
        key=lambda pid: (s.executor.pools[pid].avail_ram_pool, s.executor.pools[pid].avail_cpu_pool),
        reverse=True,
    )

    # Build an ordered list of (priority, queue) to schedule from
    prio_plan = [
        (Priority.QUERY, s.q_query),
        (Priority.INTERACTIVE, s.q_interactive),
        (Priority.BATCH_PIPELINE, s.q_batch),
    ]

    # For each pool, try to pack multiple small assignments (better tail latency than "one big op")
    for pool_id in pool_order:
        pool = s.executor.pools[pool_id]
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)
        if avail_cpu < s.min_cpu or avail_ram < s.min_ram:
            continue

        # Compute reserved headroom only if any high-priority work exists anywhere.
        # This is intentionally simple; it avoids batch consuming everything.
        has_high = _any_high_prio_waiting()
        reserved_cpu = float(pool.max_cpu_pool) * s.reserve_frac_cpu if has_high else 0.0
        reserved_ram = float(pool.max_ram_pool) * s.reserve_frac_ram if has_high else 0.0

        # Try to make several assignments per pool per tick; cap to avoid long loops.
        max_assignments_this_pool = 8
        made = 0

        while made < max_assignments_this_pool:
            did_assign = False

            for prio, q in prio_plan:
                if not q:
                    continue

                # Enforce headroom only when scheduling batch and high-priority exists.
                if prio == Priority.BATCH_PIPELINE and has_high:
                    if (avail_cpu - reserved_cpu) < s.min_cpu or (avail_ram - reserved_ram) < s.min_ram:
                        continue

                # Round-robin within the queue: take one pipeline, inspect, then requeue if not runnable.
                pipeline = q.popleft()
                status = pipeline.runtime_status()

                # Drop completed pipelines; keep others
                if status.is_pipeline_successful():
                    continue

                # If pipeline has failed ops, we still may retry them if they are assignable (FAILED is assignable).
                op = _pick_ready_op(pipeline)

                if op is None:
                    # Not runnable yet (parents not complete or only non-retryable failures), keep it in queue.
                    q.append(pipeline)
                    continue

                req_cpu, req_ram = _compute_request(pool, prio, op)

                # If batch, respect headroom
                effective_avail_cpu = avail_cpu
                effective_avail_ram = avail_ram
                if prio == Priority.BATCH_PIPELINE and has_high:
                    effective_avail_cpu = max(0.0, avail_cpu - reserved_cpu)
                    effective_avail_ram = max(0.0, avail_ram - reserved_ram)

                # Clamp request to what we can actually provide now
                cpu = min(req_cpu, effective_avail_cpu)
                ram = min(req_ram, effective_avail_ram)

                if cpu < s.min_cpu or ram < s.min_ram:
                    # Can't fit now; put pipeline back and try others
                    q.append(pipeline)
                    continue

                assignment = Assignment(
                    ops=[op],
                    cpu=cpu,
                    ram=ram,
                    priority=pipeline.priority,
                    pool_id=pool_id,
                    pipeline_id=pipeline.pipeline_id,
                )
                assignments.append(assignment)

                # Update local avail after placing this assignment
                avail_cpu -= cpu
                avail_ram -= ram
                made += 1
                did_assign = True

                # Put the pipeline back for future ops (it may still have more work)
                q.append(pipeline)

                break  # move back to highest priority again

            if not did_assign:
                break

            if avail_cpu < s.min_cpu or avail_ram < s.min_ram:
                break

    return suspensions, assignments
