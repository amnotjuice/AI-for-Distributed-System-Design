# policy_key: scheduler_low_038
# reasoning_effort: low
# model: gpt-5.2-2025-12-11
# llm_cost: 0.045317
# generation_seconds: 59.02
# generated_at: 2026-03-14T02:31:50.885518
@register_scheduler_init(key="scheduler_low_038")
def scheduler_low_038_init(s):
    """Priority-aware FIFO with small, safe improvements over naive FIFO.

    Improvements (kept deliberately simple/robust):
      1) Global priority ordering: QUERY > INTERACTIVE > BATCH, instead of pure arrival FIFO.
      2) One-op-at-a-time per pool (like the baseline), but choose the best next op by priority.
      3) Basic OOM retry: if an operator fails with an OOM-like error, retry it with more RAM next time.
         Non-OOM failures are treated as non-retryable and will not be rescheduled.
      4) Gentle right-sizing: cap per-op CPU/RAM to a fraction of pool capacity to reduce head-of-line blocking.
    """
    s.waiting_pipelines = []          # pipelines we've seen and are still active
    s.pipeline_seq = {}              # pipeline_id -> sequence number (arrival order tie-break)
    s._next_seq = 0
    s.op_ram_mult = {}               # id(op) -> RAM multiplier (OOM backoff)
    s.op_oom_retries = {}            # id(op) -> number of OOM retries so far
    s.op_nonretryable_failed = set() # id(op) that failed with a non-OOM error
    s.max_oom_retries = 3
    s.max_ram_mult = 8.0


def _priority_rank(priority):
    # Lower rank is "more important"
    if priority == Priority.QUERY:
        return 0
    if priority == Priority.INTERACTIVE:
        return 1
    return 2  # Priority.BATCH_PIPELINE and any others


def _is_oom_error(err) -> bool:
    if err is None:
        return False
    msg = str(err).lower()
    # Keep this heuristic broad; simulator errors may vary
    return ("oom" in msg) or ("out of memory" in msg) or ("memory" in msg and "exceed" in msg)


def _pool_preference_order(num_pools: int, priority):
    # Small improvement: give highest priority first shot at pool 0 (often treated as "primary"),
    # but still allow spillover to other pools.
    if num_pools <= 1:
        return [0]
    if priority in (Priority.QUERY, Priority.INTERACTIVE):
        return list(range(num_pools))  # 0,1,2,...
    return list(range(num_pools - 1, -1, -1))  # prefer non-primary pools for batch


def _compute_allocation(s, pool, op, pipeline_priority):
    # Start with available resources, but cap to reduce head-of-line blocking and allow concurrency.
    # This is intentionally conservative to avoid creating new failure modes.
    max_cpu = pool.max_cpu_pool
    max_ram = pool.max_ram_pool

    avail_cpu = pool.avail_cpu_pool
    avail_ram = pool.avail_ram_pool

    # Priority-based caps: higher priority gets a larger share of a pool.
    if pipeline_priority == Priority.QUERY:
        cpu_cap_frac, ram_cap_frac = 0.75, 0.80
    elif pipeline_priority == Priority.INTERACTIVE:
        cpu_cap_frac, ram_cap_frac = 0.60, 0.70
    else:
        cpu_cap_frac, ram_cap_frac = 0.50, 0.60

    cpu_cap = max(1.0, max_cpu * cpu_cap_frac)
    ram_cap = max(1.0, max_ram * ram_cap_frac)

    cpu = min(avail_cpu, cpu_cap)
    ram = min(avail_ram, ram_cap)

    # Apply OOM backoff multiplier for this operator if needed.
    mult = s.op_ram_mult.get(id(op), 1.0)
    ram = min(avail_ram, ram * mult)

    # Ensure we don't emit zero allocations.
    cpu = max(1.0, cpu)
    ram = max(1.0, ram)
    return cpu, ram


@register_scheduler(key="scheduler_low_038")
def scheduler_low_038(s, results, pipelines):
    """
    Scheduler step:
      - ingest new pipelines
      - update OOM/non-OOM failure signals from results
      - pick ready operators in global priority order and assign at most one op per pool per tick
    """
    # Ingest new pipelines; remember stable arrival order for tie-breaking
    for p in pipelines:
        if p.pipeline_id not in s.pipeline_seq:
            s.pipeline_seq[p.pipeline_id] = s._next_seq
            s._next_seq += 1
        s.waiting_pipelines.append(p)

    # Update failure tracking based on the latest execution results
    for r in results:
        if not getattr(r, "failed", lambda: False)():
            continue
        # Results include ops; track per-op retry policy keyed by object identity.
        for op in getattr(r, "ops", []) or []:
            op_key = id(op)
            if _is_oom_error(getattr(r, "error", None)):
                # Increase RAM multiplier and allow retry up to a limit
                prev = s.op_ram_mult.get(op_key, 1.0)
                s.op_ram_mult[op_key] = min(s.max_ram_mult, prev * 2.0)
                s.op_oom_retries[op_key] = s.op_oom_retries.get(op_key, 0) + 1
                if s.op_oom_retries[op_key] > s.max_oom_retries:
                    # Too many OOMs; stop retrying to avoid infinite loops.
                    s.op_nonretryable_failed.add(op_key)
            else:
                # Treat other failures as non-retryable.
                s.op_nonretryable_failed.add(op_key)

    # Early exit: if nothing changed, do nothing
    if not pipelines and not results:
        return [], []

    # Collect candidate (pipeline, op) pairs that are ready to run
    candidates = []
    still_waiting = []
    for p in s.waiting_pipelines:
        status = p.runtime_status()

        if status.is_pipeline_successful():
            continue  # drop completed pipelines

        # Find ready ops (including FAILED so we can retry OOM-failed operators)
        ready_ops = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True) or []
        # Filter out ops we decided are non-retryable
        ready_ops = [op for op in ready_ops if id(op) not in s.op_nonretryable_failed]

        # Keep the pipeline around regardless; it may have ops that become ready later
        still_waiting.append(p)

        if not ready_ops:
            continue

        # Small simplification: schedule at most one ready op per pipeline at a time
        op = ready_ops[0]
        candidates.append((p, op))

    s.waiting_pipelines = still_waiting

    # Sort by priority then FIFO arrival order
    candidates.sort(key=lambda po: (_priority_rank(po[0].priority), s.pipeline_seq.get(po[0].pipeline_id, 0)))

    suspensions = []
    assignments = []

    if not candidates:
        return suspensions, assignments

    # We'll do one assignment per pool per tick (as in the baseline), but pick best op globally.
    # Iterate pools in a priority-aware preference order for each candidate.
    used_candidates = set()

    # Build a quick list of pools with some capacity
    pools_with_capacity = []
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        if pool.avail_cpu_pool > 0 and pool.avail_ram_pool > 0:
            pools_with_capacity.append(pool_id)

    if not pools_with_capacity:
        return suspensions, assignments

    # Greedy: for each pool, pick the best remaining candidate for that pool.
    for pool_id in pools_with_capacity:
        pool = s.executor.pools[pool_id]
        if pool.avail_cpu_pool <= 0 or pool.avail_ram_pool <= 0:
            continue

        chosen = None

        # Prefer candidates whose priority "likes" this pool ordering; still simple/greedy.
        # We do this by scanning candidates in priority order and picking the first that considers this pool acceptable.
        for idx, (p, op) in enumerate(candidates):
            if idx in used_candidates:
                continue
            # Pool preference is only a soft hint; accept any pool. Keep the hook anyway for gradual improvements.
            pref_order = _pool_preference_order(s.executor.num_pools, p.priority)
            if pool_id in pref_order:
                chosen = (idx, p, op)
                break

        if chosen is None:
            continue

        idx, p, op = chosen

        cpu, ram = _compute_allocation(s, pool, op, p.priority)

        # If we can't fit minimal (>=1) allocations, skip.
        if cpu <= 0 or ram <= 0 or cpu > pool.avail_cpu_pool or ram > pool.avail_ram_pool:
            continue

        assignments.append(
            Assignment(
                ops=[op],
                cpu=cpu,
                ram=ram,
                priority=p.priority,
                pool_id=pool_id,
                pipeline_id=p.pipeline_id,
            )
        )
        used_candidates.add(idx)

    return suspensions, assignments
