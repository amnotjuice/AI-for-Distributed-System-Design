# policy_key: scheduler_est_047
# reasoning_effort: low
# model: gpt-5.2-2025-12-11
# llm_cost: 0.042118
# generation_seconds: 45.17
# generated_at: 2026-04-01T00:16:06.609255
@register_scheduler_init(key="scheduler_est_047")
def scheduler_est_047_init(s):
    """Priority-aware FIFO with basic right-sizing from memory estimates.

    Improvements over naive FIFO:
      1) Separate waiting queues per priority; always attempt higher priorities first.
      2) Avoid giving a single op the entire pool: cap CPU per assignment by priority to improve concurrency/latency.
      3) Use op.estimate.mem_peak_gb as a RAM floor (with a small safety margin) to reduce OOMs.
      4) On failures (treated as likely OOM), increase the per-op RAM floor and retry (FAILED is assignable).

    Notes:
      - This version intentionally avoids preemption because the container enumeration API is not specified here.
      - It also keeps complexity modest: single-step admission/placement per pool with simple reservation for batch.
    """
    # FIFO queues per priority
    s.waiting_query = []
    s.waiting_interactive = []
    s.waiting_batch = []

    # Per-operator adaptive RAM floor (GB), keyed by Python object id(op)
    s.op_ram_floor_gb = {}

    # Small knobs (tuned conservatively)
    s.safety_mem_mult_hi = 1.15   # a bit more headroom for latency-sensitive work
    s.safety_mem_mult_lo = 1.05   # minimal headroom for batch
    s.min_cpu_per_op = 1.0

    # CPU caps as fractions of a pool, to avoid one task monopolizing the pool
    s.cpu_cap_frac = {
        Priority.QUERY: 0.75,
        Priority.INTERACTIVE: 0.60,
        Priority.BATCH_PIPELINE: 0.40,
    }

    # Reserve some capacity so batch can't fully crowd out sudden interactive/query arrivals
    s.reserve_frac_for_hi = 0.20  # 20% reserved (soft reservation enforced by batch admission gating)


def _enqueue_by_priority(s, pipeline):
    if pipeline.priority == Priority.QUERY:
        s.waiting_query.append(pipeline)
    elif pipeline.priority == Priority.INTERACTIVE:
        s.waiting_interactive.append(pipeline)
    else:
        s.waiting_batch.append(pipeline)


def _pop_next_pipeline(s, prefer_priorities):
    """Pop next pipeline from the first non-empty queue among prefer_priorities."""
    for pr in prefer_priorities:
        if pr == Priority.QUERY and s.waiting_query:
            return s.waiting_query.pop(0)
        if pr == Priority.INTERACTIVE and s.waiting_interactive:
            return s.waiting_interactive.pop(0)
        if pr == Priority.BATCH_PIPELINE and s.waiting_batch:
            return s.waiting_batch.pop(0)
    return None


def _peek_has_work(pipeline):
    status = pipeline.runtime_status()
    if status.is_pipeline_successful():
        return False
    # Keep pipelines even with failures; FAILED ops are assignable and we may retry with more RAM
    return True


def _get_next_assignable_op(pipeline):
    status = pipeline.runtime_status()
    ops = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
    if not ops:
        return None
    return ops[0]


def _ram_floor_for_op_gb(s, op):
    """Return an adaptive RAM floor (GB) using estimator + learned increases."""
    est = getattr(getattr(op, "estimate", None), "mem_peak_gb", None)
    base = None
    if est is not None:
        try:
            base = float(est)
        except Exception:
            base = None

    learned = s.op_ram_floor_gb.get(id(op), None)
    if base is None and learned is None:
        # If we have no estimate, start with a modest default; failures will increase this.
        return 2.0
    if base is None:
        return float(learned)
    if learned is None:
        return float(base)
    return float(max(base, learned))


def _cpu_cap_for_priority(s, pool, priority):
    cap_frac = s.cpu_cap_frac.get(priority, 0.5)
    cap = pool.max_cpu_pool * cap_frac
    if cap < s.min_cpu_per_op:
        cap = s.min_cpu_per_op
    return cap


@register_scheduler(key="scheduler_est_047")
def scheduler_est_047(s, results, pipelines):
    """
    Priority-first, estimate-aware scheduler.

    Core loop (per tick):
      - Ingest new pipelines into priority queues.
      - Use last-tick results to raise per-op RAM floors on failures.
      - For each pool, schedule as many ops as fit:
          * Always prefer QUERY > INTERACTIVE > BATCH.
          * Batch admission is gated so it can't consume the last reserved slice of resources.
          * Allocate RAM >= estimated/learned floor with small headroom.
          * Allocate CPU <= per-priority cap to improve concurrency/latency.
    """
    # Ingest new pipelines
    for p in pipelines:
        _enqueue_by_priority(s, p)

    # Learn from failures: increase RAM floor and retry
    for r in results:
        if not getattr(r, "failed", lambda: False)():
            continue
        # Treat failures as "likely OOM": increase learned floor for the involved ops.
        # We can't reliably parse error strings across environments, so be conservative.
        bump_from = None
        try:
            bump_from = float(getattr(r, "ram", None))
        except Exception:
            bump_from = None

        for op in getattr(r, "ops", []) or []:
            cur = s.op_ram_floor_gb.get(id(op), None)
            # Increase floor by 50%, bounded below by 2GB.
            if bump_from is not None:
                new_floor = max(2.0, bump_from * 1.5)
            else:
                # If we don't know the prior allocation, still bump meaningfully.
                if cur is None:
                    new_floor = 3.0
                else:
                    new_floor = max(2.0, float(cur) * 1.5)

            if cur is None:
                s.op_ram_floor_gb[id(op)] = new_floor
            else:
                s.op_ram_floor_gb[id(op)] = max(float(cur), new_floor)

    # Early exit if nothing changed that would affect decisions
    if not pipelines and not results:
        return [], []

    suspensions = []  # no preemption in this iteration
    assignments = []

    # Helper to requeue pipelines we inspected but didn't run
    requeue = []

    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]

        # Current free capacity in the pool
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)

        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # Soft reservation: keep some headroom for high priority by limiting batch scheduling
        reserve_cpu = float(pool.max_cpu_pool) * float(s.reserve_frac_for_hi)
        reserve_ram = float(pool.max_ram_pool) * float(s.reserve_frac_for_hi)

        # Schedule multiple ops per pool until we run out of capacity or no runnable work
        # Priority order: QUERY > INTERACTIVE > BATCH
        priority_order = [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]

        # To avoid an infinite loop if pipelines are blocked (parents not complete), cap attempts.
        max_attempts = (len(s.waiting_query) + len(s.waiting_interactive) + len(s.waiting_batch) + 8)
        attempts = 0

        while attempts < max_attempts and avail_cpu > 0 and avail_ram > 0:
            attempts += 1

            pipeline = _pop_next_pipeline(s, priority_order)
            if pipeline is None:
                break

            if not _peek_has_work(pipeline):
                continue

            op = _get_next_assignable_op(pipeline)
            if op is None:
                # Nothing runnable now (likely waiting on parents); keep it around.
                requeue.append(pipeline)
                continue

            # Batch gating: don't let batch consume the reserved slice needed for high-priority bursts
            if pipeline.priority == Priority.BATCH_PIPELINE:
                if (avail_cpu - reserve_cpu) < s.min_cpu_per_op or (avail_ram - reserve_ram) < 0.5:
                    # Not enough non-reserved capacity; try later.
                    requeue.append(pipeline)
                    continue

            # Compute RAM request: floor from estimate/learning + small safety
            ram_floor = _ram_floor_for_op_gb(s, op)
            safety = s.safety_mem_mult_hi if pipeline.priority in (Priority.QUERY, Priority.INTERACTIVE) else s.safety_mem_mult_lo
            ram_req = ram_floor * safety

            # If we can't fit this op now, requeue (do not block other work of same priority forever)
            if ram_req > avail_ram:
                requeue.append(pipeline)
                continue

            # Compute CPU request: capped per priority to avoid monopolization, but at least 1 CPU
            cpu_cap = _cpu_cap_for_priority(s, pool, pipeline.priority)
            cpu_req = min(avail_cpu, cpu_cap)
            if cpu_req < s.min_cpu_per_op:
                # Can't schedule anything meaningful right now
                requeue.append(pipeline)
                break

            # Assign exactly one op to improve latency and interleave across pipelines
            assignment = Assignment(
                ops=[op],
                cpu=cpu_req,
                ram=ram_req,
                priority=pipeline.priority,
                pool_id=pool_id,
                pipeline_id=pipeline.pipeline_id,
            )
            assignments.append(assignment)

            # Consume resources locally for additional placements this tick
            avail_cpu -= cpu_req
            avail_ram -= ram_req

            # Keep the pipeline in the system; subsequent ops may become runnable later
            requeue.append(pipeline)

    # Put pipelines back to their queues preserving FIFO by appending in the order we touched them
    for p in requeue:
        _enqueue_by_priority(s, p)

    return suspensions, assignments
