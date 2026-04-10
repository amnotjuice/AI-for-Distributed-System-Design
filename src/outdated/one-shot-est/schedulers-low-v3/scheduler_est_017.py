# policy_key: scheduler_est_017
# reasoning_effort: low
# model: gpt-5.2-2025-12-11
# llm_cost: 0.042741
# generation_seconds: 38.22
# generated_at: 2026-04-03T01:15:32.408225
@register_scheduler_init(key="scheduler_est_017")
def scheduler_est_017_init(s):
    """
    Priority-aware FIFO with small, safe improvements over naive FIFO:

    1) Three separate FIFO queues by priority (QUERY > INTERACTIVE > BATCH).
    2) Soft reservation: keep a small CPU/RAM headroom in each pool so batch
       can't consume everything and block high-priority arrivals.
    3) Simple OOM retry sizing: if an op fails (OOM or any failure), retry it
       with a RAM multiplier (exponential backoff) up to pool max RAM.
    4) Conservative CPU sizing for high-priority ops to improve concurrency and
       tail latency under contention; batch uses more CPU when available.

    Notes:
    - Avoids relying on non-guaranteed executor internals (preemption requires
      enumerating running containers, which may not exist in the API).
    """
    s.q_query = []         # List[Pipeline]
    s.q_interactive = []   # List[Pipeline]
    s.q_batch = []         # List[Pipeline]

    # Track per-operator retry sizing based on observed failures.
    # Keyed by (pipeline_id, op_object_id) to avoid assuming op has stable IDs.
    s.op_fail_count = {}   # Dict[Tuple[str,int], int]

    # Optional: remember last assigned RAM for debugging/heuristics.
    s.op_last_ram = {}     # Dict[Tuple[str,int], float]

    # Monotonic tie-breaker to preserve FIFO order within priority.
    s._enqueue_seq = 0
    s._seq = {}            # Dict[int, int] pipeline_object_id -> seq


def _pkey(priority):
    # Lower is better for sorting.
    if priority == Priority.QUERY:
        return 0
    if priority == Priority.INTERACTIVE:
        return 1
    return 2


def _queue_for(s, priority):
    if priority == Priority.QUERY:
        return s.q_query
    if priority == Priority.INTERACTIVE:
        return s.q_interactive
    return s.q_batch


def _enqueue_pipeline(s, p):
    pid = id(p)
    if pid not in s._seq:
        s._seq[pid] = s._enqueue_seq
        s._enqueue_seq += 1
    _queue_for(s, p.priority).append(p)


def _all_queues(s):
    # Return queues in strict priority order.
    return [s.q_query, s.q_interactive, s.q_batch]


def _has_high_priority_waiting(s):
    return bool(s.q_query) or bool(s.q_interactive)


def _is_done_or_terminal(p):
    st = p.runtime_status()
    if st.is_pipeline_successful():
        return True
    # If there are FAILED ops, we still may want to retry; do NOT drop.
    # Terminal failure handling is policy-specific; here we keep retrying.
    return False


def _get_next_ready_op(p):
    st = p.runtime_status()
    # Prefer "assignable states" whose parents are complete.
    ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
    if ops:
        return ops[0]
    return None


def _op_key(pipeline, op):
    return (pipeline.pipeline_id, id(op))


def _mem_est_gb(op):
    # Estimator may attach op.estimate.mem_peak_gb (float or None).
    try:
        est = op.estimate.mem_peak_gb
    except Exception:
        return None
    if est is None:
        return None
    try:
        estf = float(est)
    except Exception:
        return None
    if estf <= 0:
        return None
    return estf


def _compute_ram_request_gb(s, pool, pipeline, op):
    """
    RAM sizing:
    - Start near estimator (if present), with tiny safety margin.
    - On repeated failures, increase via exponential backoff.
    - Clamp to pool capacity.
    """
    key = _op_key(pipeline, op)
    fails = int(s.op_fail_count.get(key, 0))

    est = _mem_est_gb(op)

    # Baseline: small default if no estimate; keep it non-zero.
    base = 0.5
    if est is not None:
        # Keep it aggressive; rely on retry if underestimated.
        base = max(base, est * 1.05)

    # Failure backoff: 1.0, 1.6, 2.56, 4.10, ...
    mult = 1.0
    for _ in range(fails):
        mult *= 1.6

    req = base * mult

    # Avoid requesting more than the pool max; also don't exceed avail here.
    # (Caller will check avail and may choose to skip if not enough.)
    req = min(req, float(pool.max_ram_pool))
    return max(0.1, req)


def _compute_cpu_request(s, pool, pipeline_priority, avail_cpu):
    """
    CPU sizing geared for latency:
    - QUERY/INTERACTIVE: cap CPU to a modest slice to improve concurrency.
    - BATCH: take more when available to increase throughput.
    """
    max_cpu = float(pool.max_cpu_pool)

    # Minimum useful CPU request
    min_cpu = 0.25
    if max_cpu >= 1:
        min_cpu = 1.0

    if pipeline_priority == Priority.QUERY:
        # Small-ish slice; prioritize concurrency
        target = max(min_cpu, 0.25 * max_cpu)
    elif pipeline_priority == Priority.INTERACTIVE:
        target = max(min_cpu, 0.35 * max_cpu)
    else:
        # Batch can scale up
        target = max(min_cpu, 0.75 * max_cpu)

    return max(min_cpu, min(float(avail_cpu), target))


def _reserve_headroom(pool):
    """
    Soft reservations to prevent batch from consuming the entire pool.
    Kept intentionally small to be a "safe" first-step improvement.
    """
    reserve_cpu = 0.0
    reserve_ram = 0.0

    # Keep a small percentage headroom for high-priority bursts.
    reserve_cpu = 0.15 * float(pool.max_cpu_pool)
    reserve_ram = 0.15 * float(pool.max_ram_pool)

    return reserve_cpu, reserve_ram


@register_scheduler(key="scheduler_est_017")
def scheduler_est_017_scheduler(s, results, pipelines):
    """
    Scheduler tick:
    - Ingest new pipelines into priority FIFO queues.
    - Process results to update failure counters (OOM -> more RAM next retry).
    - For each pool, greedily assign ready ops from highest priority down,
      subject to soft reservations (batch can't use reserved headroom).
    """
    # Enqueue new arrivals
    for p in pipelines:
        _enqueue_pipeline(s, p)

    # Update failure-based RAM backoff
    for r in results:
        # If result failed, bump fail count for the involved ops.
        # We assume r.ops is iterable of op objects.
        if hasattr(r, "failed") and r.failed():
            for op in getattr(r, "ops", []) or []:
                # We need the pipeline_id for the key; ExecutionResult doesn't
                # expose it, so we can only update when we can infer it.
                # If op has pipeline_id attribute use it; else skip.
                pipeline_id = getattr(op, "pipeline_id", None)
                if pipeline_id is None:
                    continue
                key = (pipeline_id, id(op))
                s.op_fail_count[key] = int(s.op_fail_count.get(key, 0)) + 1

    # Early exit if nothing changed that affects decisions
    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    # Clean up queues: drop completed pipelines (keep failed for retry)
    for q in _all_queues(s):
        kept = []
        for p in q:
            if not _is_done_or_terminal(p):
                kept.append(p)
        q[:] = kept

    # Determine if we should protect headroom aggressively
    high_waiting = _has_high_priority_waiting(s)

    # Pool-by-pool greedy assignment
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)
        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        reserve_cpu, reserve_ram = _reserve_headroom(pool)

        # Greedily pack ops until we can't fit more
        made_progress = True
        while made_progress:
            made_progress = False

            # Candidate selection: scan queues by priority, preserving FIFO.
            chosen_pipeline = None
            chosen_op = None
            chosen_queue = None

            for q in _all_queues(s):
                # Iterate in FIFO order; find first pipeline with a ready op.
                for p in q:
                    op = _get_next_ready_op(p)
                    if op is None:
                        continue
                    chosen_pipeline = p
                    chosen_op = op
                    chosen_queue = q
                    break
                if chosen_op is not None:
                    break

            if chosen_op is None:
                break

            # Compute requested resources
            req_ram = _compute_ram_request_gb(s, pool, chosen_pipeline, chosen_op)
            req_cpu = _compute_cpu_request(s, pool, chosen_pipeline.priority, avail_cpu)

            # Enforce soft reservation for batch when high-priority is waiting.
            # Also always keep some RAM headroom from batch to reduce blocking.
            if chosen_pipeline.priority == Priority.BATCH_PIPELINE:
                cpu_limit = avail_cpu
                ram_limit = avail_ram

                # Always keep some headroom from batch.
                cpu_limit = max(0.0, cpu_limit - reserve_cpu)
                ram_limit = max(0.0, ram_limit - reserve_ram)

                # If high priority is actively waiting, be stricter.
                if high_waiting:
                    cpu_limit = max(0.0, cpu_limit - reserve_cpu)
                    ram_limit = max(0.0, ram_limit - reserve_ram)

                if req_cpu > cpu_limit or req_ram > ram_limit:
                    # Can't schedule batch without consuming headroom.
                    # Try next lower priority doesn't exist; so stop on this pool
                    # for this tick to avoid spinning.
                    break

            # Hard fit check
            if req_cpu <= 0 or req_ram <= 0 or req_cpu > avail_cpu or req_ram > avail_ram:
                break

            # Assign one op at a time (keeps policy robust to executor semantics)
            assignments.append(
                Assignment(
                    ops=[chosen_op],
                    cpu=req_cpu,
                    ram=req_ram,
                    priority=chosen_pipeline.priority,
                    pool_id=pool_id,
                    pipeline_id=chosen_pipeline.pipeline_id,
                )
            )

            # Track last sizing for this op
            ok = _op_key(chosen_pipeline, chosen_op)
            s.op_last_ram[ok] = float(req_ram)

            # Update local available resources for further packing
            avail_cpu -= float(req_cpu)
            avail_ram -= float(req_ram)
            made_progress = True

    return suspensions, assignments
