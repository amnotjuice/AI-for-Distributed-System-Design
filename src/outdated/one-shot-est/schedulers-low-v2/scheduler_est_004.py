# policy_key: scheduler_est_004
# reasoning_effort: low
# model: gpt-5.2-2025-12-11
# llm_cost: 0.046918
# generation_seconds: 37.38
# generated_at: 2026-04-01T13:38:22.671992
@register_scheduler_init(key="scheduler_est_004")
def scheduler_est_004_init(s):
    """Priority-aware, latency-first scheduler (small, incremental improvements over naive FIFO).

    Improvements vs. naive:
    - Maintain per-priority queues (QUERY > INTERACTIVE > BATCH) to reduce tail latency for high-priority work.
    - Avoid "monopolistic" single assignment: cap CPU per assignment (more concurrency, better latency under load).
    - Use (noisy) memory peak estimates conservatively and add retry-on-failure with RAM backoff to reduce repeated OOMs.
    - Soft reservation: when high-priority work is waiting, avoid consuming the last headroom with batch.
    - Pool preference: if multiple pools exist, prefer pool 0 for high-priority work (simple multi-pool isolation).
    """
    # Waiting queues by priority
    s.q_query = []
    s.q_interactive = []
    s.q_batch = []

    # Per-operator (by object identity) attempt count and last RAM choice
    s.op_attempts = {}          # op_key -> int
    s.op_last_ram_gb = {}       # op_key -> float

    # Per-pipeline arrival order for fairness within same priority
    s.arrival_seq = 0
    s.pipeline_seq = {}         # pipeline_id -> int

    # Tuning knobs (kept simple on purpose)
    s.cpu_cap_query = 4.0
    s.cpu_cap_interactive = 4.0
    s.cpu_cap_batch = 8.0

    # Default RAM "guess" when no estimate is present (GB)
    s.default_ram_query = 2.0
    s.default_ram_interactive = 4.0
    s.default_ram_batch = 8.0

    # Safety factor applied to estimate (estimates are hints; be conservative)
    s.mem_est_safety = 1.25

    # Backoff factor for RAM on retry after failure
    s.retry_ram_mult = 1.7

    # Minimum headroom to keep free for high priority when they are queued (GB, vCPU)
    s.reserve_ram_for_hp = 2.0
    s.reserve_cpu_for_hp = 1.0


def _prio_rank(p):
    # Higher number = higher priority
    if p == Priority.QUERY:
        return 3
    if p == Priority.INTERACTIVE:
        return 2
    return 1


def _enqueue_pipeline(s, p):
    if p.pipeline_id not in s.pipeline_seq:
        s.arrival_seq += 1
        s.pipeline_seq[p.pipeline_id] = s.arrival_seq

    if p.priority == Priority.QUERY:
        s.q_query.append(p)
    elif p.priority == Priority.INTERACTIVE:
        s.q_interactive.append(p)
    else:
        s.q_batch.append(p)


def _iter_priority_queues(s):
    # Priority order: QUERY -> INTERACTIVE -> BATCH
    return [s.q_query, s.q_interactive, s.q_batch]


def _cleanup_and_requeue(s, pipeline):
    """Return True if pipeline should remain queued; False if should be dropped."""
    status = pipeline.runtime_status()
    if status.is_pipeline_successful():
        return False
    # If there are FAILED ops, we still allow retries (we'll bump RAM); do not drop.
    return True


def _pick_next_pipeline_from_queue(q):
    # FIFO within a priority class
    if not q:
        return None
    return q.pop(0)


def _pool_order_for_priority(num_pools, priority):
    # If we have multiple pools, prefer pool 0 for high priority for basic isolation.
    if num_pools <= 1:
        return [0]
    if priority in (Priority.QUERY, Priority.INTERACTIVE):
        return [0] + [i for i in range(1, num_pools)]
    else:
        # Prefer non-0 pools for batch if possible
        return [i for i in range(1, num_pools)] + [0]


def _cpu_cap_for_priority(s, priority):
    if priority == Priority.QUERY:
        return s.cpu_cap_query
    if priority == Priority.INTERACTIVE:
        return s.cpu_cap_interactive
    return s.cpu_cap_batch


def _default_ram_for_priority(s, priority):
    if priority == Priority.QUERY:
        return s.default_ram_query
    if priority == Priority.INTERACTIVE:
        return s.default_ram_interactive
    return s.default_ram_batch


def _op_key(op):
    # Use object identity as a stable key within a simulation run.
    return id(op)


def _ram_request_for_op(s, op, priority, pool_avail_ram):
    """Compute a conservative RAM request for an operator."""
    key = _op_key(op)
    attempts = s.op_attempts.get(key, 0)
    last_ram = s.op_last_ram_gb.get(key, None)

    # Base guess: estimator hint if present, else a small priority-based default.
    est = None
    try:
        # Estimator may attach op.estimate.mem_peak_gb
        if hasattr(op, "estimate") and op.estimate is not None:
            est = getattr(op.estimate, "mem_peak_gb", None)
    except Exception:
        est = None

    base = _default_ram_for_priority(s, priority)
    if est is not None:
        # Treat estimate as a noisy lower bound and add safety factor
        try:
            base = max(base, float(est) * s.mem_est_safety)
        except Exception:
            pass

    # Retry logic: if we already tried and/or have a previous chosen RAM, scale up.
    ram = base
    if last_ram is not None:
        ram = max(ram, float(last_ram))
    if attempts > 0:
        ram = max(ram, base * (s.retry_ram_mult ** attempts))

    # Never request more than pool available (admission will check again).
    # Keep as float.
    return min(float(pool_avail_ram), float(ram))


def _should_reserve_for_hp(s, any_hp_waiting, priority):
    # If there is any waiting QUERY/INTERACTIVE, keep some headroom by not scheduling batch into the last bit.
    return any_hp_waiting and priority == Priority.BATCH_PIPELINE


@register_scheduler(key="scheduler_est_004")
def scheduler_est_004_scheduler(s, results, pipelines):
    """
    Priority-aware scheduler with conservative memory sizing + retry backoff.

    Behavior:
    - Enqueue arriving pipelines into per-priority FIFO queues.
    - Process results: on failures, increment per-op attempt counter so next scheduling uses more RAM.
    - For each pool, schedule at most one operator per tick (keeps changes small, reduces monopolization).
    - For each assignment, cap CPU to reduce latency for concurrent high-priority work.
    - Soft-reserve headroom: if high-priority is waiting, avoid packing batch into the last CPU/RAM.
    """
    # Enqueue new pipelines
    for p in pipelines:
        _enqueue_pipeline(s, p)

    # Update failure-driven RAM backoff state
    for r in results:
        if r is None:
            continue
        if getattr(r, "failed", None) and r.failed():
            # Increment attempts for any ops that were part of this result
            try:
                for op in getattr(r, "ops", []) or []:
                    key = _op_key(op)
                    s.op_attempts[key] = s.op_attempts.get(key, 0) + 1
                    # Remember last requested RAM if available from result
                    try:
                        if getattr(r, "ram", None) is not None:
                            s.op_last_ram_gb[key] = float(r.ram)
                    except Exception:
                        pass
            except Exception:
                pass
        else:
            # On success, clear attempts for those ops (optional, keeps state bounded)
            try:
                for op in getattr(r, "ops", []) or []:
                    key = _op_key(op)
                    if key in s.op_attempts:
                        del s.op_attempts[key]
                    if key in s.op_last_ram_gb:
                        del s.op_last_ram_gb[key]
            except Exception:
                pass

    # Early exit if nothing changed
    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    # Determine if high-priority work is waiting (for soft reservation)
    any_hp_waiting = (len(s.q_query) + len(s.q_interactive)) > 0

    # For each pool, try to schedule one operator to keep behavior close to naive-but-better.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)

        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        made_assignment = False

        # Try priorities in order; prefer certain pools per priority via ordering of pools,
        # but since we're already iterating pools, we'll simply skip if this pool isn't preferred.
        for q in _iter_priority_queues(s):
            if made_assignment:
                break

            # Peek candidate without losing it until we know we can schedule.
            pipeline = q[0] if q else None
            if pipeline is None:
                continue

            # Drop completed pipelines; keep unfinished ones
            if not _cleanup_and_requeue(s, pipeline):
                q.pop(0)
                continue

            # If this pipeline's priority "prefers" different pool, and there exists an alternative pool,
            # do a lightweight deferral by skipping (keeps logic simple/deterministic).
            preferred_pools = _pool_order_for_priority(s.executor.num_pools, pipeline.priority)
            if preferred_pools and preferred_pools[0] != pool_id:
                # If this pool isn't preferred and there are other pools, skip for now.
                continue

            status = pipeline.runtime_status()

            # Choose one ready op (parents complete) to reduce latency and avoid long monopolization.
            op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
            if not op_list:
                # Pipeline not currently schedulable; rotate within its priority queue for fairness.
                q.pop(0)
                q.append(pipeline)
                continue

            op = op_list[0]

            # Soft reservation: if high priority is waiting, keep minimal headroom by not scheduling batch
            # into the last bits of CPU/RAM.
            if _should_reserve_for_hp(s, any_hp_waiting, pipeline.priority):
                if avail_cpu <= s.reserve_cpu_for_hp or avail_ram <= s.reserve_ram_for_hp:
                    # Not enough headroom left; do not schedule batch here.
                    continue

            # Decide CPU/RAM request for this op
            cpu_cap = _cpu_cap_for_priority(s, pipeline.priority)
            cpu_req = min(avail_cpu, float(cpu_cap))
            if cpu_req <= 0:
                continue

            ram_req = _ram_request_for_op(s, op, pipeline.priority, avail_ram)
            if ram_req <= 0:
                continue

            # Admission check: must fit current availability
            if cpu_req > avail_cpu or ram_req > avail_ram:
                continue

            # Commit: pop pipeline from queue and assign
            q.pop(0)

            # Record last chosen RAM for future retries even if failure doesn't report it cleanly.
            s.op_last_ram_gb[_op_key(op)] = float(ram_req)

            assignments.append(
                Assignment(
                    ops=[op],
                    cpu=cpu_req,
                    ram=ram_req,
                    priority=pipeline.priority,
                    pool_id=pool_id,
                    pipeline_id=pipeline.pipeline_id,
                )
            )
            made_assignment = True

            # Requeue pipeline if it still has work left (most pipelines will)
            if _cleanup_and_requeue(s, pipeline):
                _enqueue_pipeline(s, pipeline)

    return suspensions, assignments
