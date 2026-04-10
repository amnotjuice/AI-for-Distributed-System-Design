# policy_key: scheduler_none_003
# reasoning_effort: none
# model: gpt-5.2-2025-12-11
# llm_cost: 0.033543
# generation_seconds: 45.86
# generated_at: 2026-03-12T21:23:11.210654
@register_scheduler_init(key="scheduler_none_003")
def scheduler_none_003_init(s):
    """Priority-aware FIFO with conservative sizing + OOM backoff.

    Small, incremental improvements over naive FIFO:
      1) Maintain per-priority queues; always try higher priority first (QUERY > INTERACTIVE > BATCH_PIPELINE).
      2) Avoid "give everything to one op": use a per-op CPU/RAM cap and keep headroom.
      3) On OOM failure, increase the per-(pipeline, op) RAM guess and retry later (bounded).
      4) Basic anti-starvation: batch pipelines slowly gain "age" and can be considered after some waiting.

    Notes:
      - No preemption yet (keeps churn low; improves tail latency mainly via priority + headroom).
      - Placement: try to schedule high priority on the pool with most headroom.
    """
    # Per-priority FIFO queues (store pipeline_ids; actual objects kept in map)
    s.q_query = []
    s.q_interactive = []
    s.q_batch = []

    # pipeline_id -> Pipeline (latest reference)
    s.pipelines_by_id = {}

    # (pipeline_id, op_id_str) -> ram_guess
    s.ram_guess = {}

    # (pipeline_id, op_id_str) -> consecutive_ooms
    s.oom_count = {}

    # pipeline_id -> age ticks (for mild aging / anti-starvation)
    s.age = {}

    # Conservative global defaults (can be tuned in later versions)
    s.default_ram_guess_frac = 0.25  # start with 25% of pool RAM (bounded by caps)
    s.default_cpu_guess_frac = 0.50  # start with 50% of pool CPU (bounded by caps)
    s.max_cpu_per_op_frac = 0.75     # never allocate more than 75% of pool CPU to one op
    s.max_ram_per_op_frac = 0.75     # never allocate more than 75% of pool RAM to one op
    s.min_cpu_per_op = 1.0           # at least 1 vCPU if available
    s.min_ram_per_op = 0.5           # at least 0.5 GB (units depend on simulator config)
    s.keep_headroom_frac = 0.10      # keep 10% pool resources unallocated if possible

    # Hard limit to avoid infinite retries on pathological OOMs
    s.max_oom_retries = 5

    # How quickly batch ages into consideration (ticks)
    s.batch_aging_threshold = 8


def _prio_rank(priority):
    # Smaller is higher priority
    if priority == Priority.QUERY:
        return 0
    if priority == Priority.INTERACTIVE:
        return 1
    return 2  # Priority.BATCH_PIPELINE and any others


def _enqueue_pipeline(s, p):
    pid = p.pipeline_id
    s.pipelines_by_id[pid] = p
    if pid not in s.age:
        s.age[pid] = 0

    if p.priority == Priority.QUERY:
        s.q_query.append(pid)
    elif p.priority == Priority.INTERACTIVE:
        s.q_interactive.append(pid)
    else:
        s.q_batch.append(pid)


def _pop_next_candidate_pid(s):
    # Strict priority first, but allow mild aging for batch after threshold.
    # If batch has waited long enough, let it be considered after interactive if no query.
    if s.q_query:
        return s.q_query.pop(0)

    # If there are no interactive, take batch.
    if not s.q_interactive and s.q_batch:
        return s.q_batch.pop(0)

    # If interactive exists, mostly prefer it, but allow aged batch to slip in sometimes.
    if s.q_interactive:
        # Find an aged batch at the front-ish (scan limited) if it has waited.
        scan = min(5, len(s.q_batch))
        for i in range(scan):
            pid = s.q_batch[i]
            if s.age.get(pid, 0) >= s.batch_aging_threshold:
                # Pop that batch pid
                s.q_batch.pop(i)
                return pid
        return s.q_interactive.pop(0)

    if s.q_batch:
        return s.q_batch.pop(0)

    return None


def _requeue_pid(s, pid):
    p = s.pipelines_by_id.get(pid)
    if p is None:
        return
    if p.priority == Priority.QUERY:
        s.q_query.append(pid)
    elif p.priority == Priority.INTERACTIVE:
        s.q_interactive.append(pid)
    else:
        s.q_batch.append(pid)


def _op_key(pipeline_id, op):
    # Try to use stable identifiers; fall back to repr(op)
    opid = getattr(op, "op_id", None)
    if opid is None:
        opid = getattr(op, "operator_id", None)
    if opid is None:
        opid = getattr(op, "id", None)
    if opid is None:
        opid = repr(op)
    return (pipeline_id, str(opid))


def _is_oom_error(err):
    if not err:
        return False
    msg = str(err).lower()
    return ("oom" in msg) or ("out of memory" in msg) or ("memory" in msg and "exceed" in msg)


def _choose_pool_order(s, pipeline_priority):
    # High priority: try the pool with most available RAM first (reduces OOM risk and queueing)
    # Low priority: try the pool with most available CPU first (throughput-friendly)
    pool_ids = list(range(s.executor.num_pools))

    def pool(i):
        return s.executor.pools[i]

    if pipeline_priority in (Priority.QUERY, Priority.INTERACTIVE):
        pool_ids.sort(key=lambda i: (pool(i).avail_ram_pool, pool(i).avail_cpu_pool), reverse=True)
    else:
        pool_ids.sort(key=lambda i: (pool(i).avail_cpu_pool, pool(i).avail_ram_pool), reverse=True)
    return pool_ids


def _size_for_op(s, pool, pipeline_priority, ok, avail_cpu, avail_ram):
    # Compute CPU/RAM request for this op on this pool, with caps + headroom.
    # Use per-op RAM guess if present; otherwise conservative fraction of pool.
    max_cpu = max(0.0, pool.max_cpu_pool)
    max_ram = max(0.0, pool.max_ram_pool)

    # Keep headroom when possible
    headroom_cpu = max_cpu * s.keep_headroom_frac
    headroom_ram = max_ram * s.keep_headroom_frac
    usable_cpu = max(0.0, avail_cpu - headroom_cpu)
    usable_ram = max(0.0, avail_ram - headroom_ram)

    # Priority bias: queries get a bit more CPU to cut latency; batch gets less to avoid hogging.
    if pipeline_priority == Priority.QUERY:
        cpu_bias = 1.0
    elif pipeline_priority == Priority.INTERACTIVE:
        cpu_bias = 0.85
    else:
        cpu_bias = 0.65

    # Base guesses from pool size (not from current avail) to be stable
    base_cpu = max(s.min_cpu_per_op, max_cpu * s.default_cpu_guess_frac * cpu_bias)
    base_ram = max(s.min_ram_per_op, max_ram * s.default_ram_guess_frac)

    # Apply learned RAM guess if any (per op)
    learned_ram = s.ram_guess.get(ok)
    if learned_ram is not None:
        base_ram = max(base_ram, learned_ram)

    # Cap per op to avoid monopolizing the pool
    cap_cpu = max(s.min_cpu_per_op, max_cpu * s.max_cpu_per_op_frac)
    cap_ram = max(s.min_ram_per_op, max_ram * s.max_ram_per_op_frac)

    req_cpu = min(cap_cpu, base_cpu, usable_cpu if usable_cpu > 0 else avail_cpu)
    req_ram = min(cap_ram, base_ram, usable_ram if usable_ram > 0 else avail_ram)

    # If headroom made it too small but resources exist, fall back to using raw avail
    if req_cpu < s.min_cpu_per_op and avail_cpu >= s.min_cpu_per_op:
        req_cpu = s.min_cpu_per_op
    if req_ram < s.min_ram_per_op and avail_ram >= s.min_ram_per_op:
        req_ram = s.min_ram_per_op

    # Final clamp to actual available
    req_cpu = max(0.0, min(req_cpu, avail_cpu))
    req_ram = max(0.0, min(req_ram, avail_ram))

    return req_cpu, req_ram


@register_scheduler(key="scheduler_none_003")
def scheduler_none_003(s, results, pipelines):
    """
    Priority-aware FIFO scheduling with OOM backoff and simple anti-starvation.

    Returns:
      suspensions: none (no preemption yet)
      assignments: at most one operator per pool per tick (simple, stable behavior)
    """
    # Ingest new pipelines
    for p in pipelines:
        _enqueue_pipeline(s, p)

    # Early exit if nothing to do
    if not pipelines and not results:
        return [], []

    # Update ages (waiting time)
    for pid in s.q_query + s.q_interactive + s.q_batch:
        s.age[pid] = s.age.get(pid, 0) + 1

    # Learn from results (mainly OOM -> increase RAM guess)
    for r in results:
        if r is None or not getattr(r, "failed", lambda: False)():
            continue

        # Try to find pipeline id from result (fallback: skip if unknown)
        pipeline_id = getattr(r, "pipeline_id", None)
        if pipeline_id is None:
            # If the simulator doesn't provide pipeline_id in results, we can't safely learn per-op.
            # Still, we can do a coarse global backoff by ignoring.
            continue

        # Try to identify the op
        ops = getattr(r, "ops", None) or []
        if not ops:
            continue
        op = ops[0]
        ok = _op_key(pipeline_id, op)

        if _is_oom_error(getattr(r, "error", None)):
            # Exponential-ish backoff on RAM guess, bounded by pool max in sizing
            prev = s.ram_guess.get(ok, max(s.min_ram_per_op, float(getattr(r, "ram", 0.0) or 0.0)))
            if prev <= 0:
                prev = s.min_ram_per_op
            # Increase by 1.6x plus a small constant to escape tiny guesses
            new_guess = prev * 1.6 + s.min_ram_per_op
            s.ram_guess[ok] = new_guess
            s.oom_count[ok] = s.oom_count.get(ok, 0) + 1

    suspensions = []
    assignments = []

    # One op per pool per tick; try to fill each pool independently
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool
        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # Choose next runnable op by considering candidates in priority order.
        # We rotate through pipelines until we either schedule one op on this pool or exhaust queues.
        attempts = 0
        max_attempts = len(s.q_query) + len(s.q_interactive) + len(s.q_batch)
        if max_attempts == 0:
            continue

        # Since we're iterating pools, we don't want pool_id ordering to bias priorities too much.
        # For high priority, try best pool first by reordering pool traversal; but we keep it simple:
        # within this pool, just pick best candidate pipeline.
        scheduled = False
        deferred = []

        while attempts < max_attempts:
            pid = _pop_next_candidate_pid(s)
            if pid is None:
                break
            attempts += 1

            p = s.pipelines_by_id.get(pid)
            if p is None:
                continue

            status = p.runtime_status()
            has_failures = status.state_counts[OperatorState.FAILED] > 0

            # Drop completed pipelines; drop pipelines with non-retriable failures (keep behavior similar to baseline)
            if status.is_pipeline_successful() or has_failures:
                continue

            # Get at most one ready op
            op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
            if not op_list:
                # Not ready, keep it in circulation
                deferred.append(pid)
                continue

            op = op_list[0]
            ok = _op_key(pid, op)

            # If we've OOM'd too many times on this op, stop retrying to avoid infinite loops.
            if s.oom_count.get(ok, 0) >= s.max_oom_retries:
                # Treat as permanently failed: do not requeue.
                continue

            # Size request for this pool
            req_cpu, req_ram = _size_for_op(s, pool, p.priority, ok, avail_cpu, avail_ram)

            # If we can't meet minimums, defer and try another pipeline
            if req_cpu < s.min_cpu_per_op or req_ram < s.min_ram_per_op:
                deferred.append(pid)
                continue

            assignment = Assignment(
                ops=[op],
                cpu=req_cpu,
                ram=req_ram,
                priority=p.priority,
                pool_id=pool_id,
                pipeline_id=pid,
            )
            assignments.append(assignment)
            scheduled = True

            # Reset age once we made progress on this pipeline
            s.age[pid] = 0
            break

        # Requeue deferred pipelines in FIFO order
        for pid in deferred:
            _requeue_pid(s, pid)

        # If we scheduled something, don't also schedule another op in this pool this tick (stability)
        if scheduled:
            continue

    return suspensions, assignments
