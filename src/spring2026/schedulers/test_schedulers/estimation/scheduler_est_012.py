# policy_key: scheduler_est_012
# reasoning_effort: low
# exp: estimation
# model: gpt-5.2-2025-12-11
# llm_cost: 0.056322
# generation_seconds: 45.71
# generated_at: 2026-04-10T07:56:14.375024
@register_scheduler_init(key="scheduler_est_012")
def scheduler_est_012_init(s):
    """Priority- and memory-aware scheduler with OOM-driven retries.

    Core ideas:
      - Separate queues per priority (QUERY > INTERACTIVE > BATCH).
      - Memory-aware placement using op.estimate.mem_peak_gb (hint, may be None).
      - Avoid OOM-induced pipeline failures by (a) skipping infeasible pools, and
        (b) retrying failed operators with increased RAM (bounded retries).
      - Keep batch progressing with light aging to reduce starvation.
      - If multiple pools exist, keep pool 0 preferentially for QUERY/INTERACTIVE unless idle.
    """
    s.q_query = []
    s.q_interactive = []
    s.q_batch = []

    # Per-pipeline retry / sizing state
    s.pipeline_retries = {}         # pipeline_id -> int
    s.pipeline_ram_mult = {}        # pipeline_id -> float
    s.pipeline_last_enq_tick = {}   # pipeline_id -> int

    # Track last seen pipeline priority (for state cleanup)
    s.pipeline_priority = {}        # pipeline_id -> Priority

    # Tick counter (discrete scheduler invocations)
    s.tick = 0

    # Tuning knobs (conservative)
    s.max_retries = 3
    s.ram_mult_growth = 1.6         # multiplicative bump on failures
    s.ram_safety_gb = 0.5           # additive safety over estimate
    s.default_ram_frac = 0.30       # if no estimate, request this fraction of pool max
    s.min_ram_gb = 0.5              # never request below this
    s.batch_aging_ticks = 25        # after this many ticks in batch queue, allow earlier scheduling


def _get_prio_weight(priority):
    # Higher weight => more important (used only for tie-breaking)
    if priority == Priority.QUERY:
        return 3
    if priority == Priority.INTERACTIVE:
        return 2
    return 1


def _enqueue_pipeline(s, p):
    pid = p.pipeline_id
    s.pipeline_priority[pid] = p.priority
    if pid not in s.pipeline_retries:
        s.pipeline_retries[pid] = 0
    if pid not in s.pipeline_ram_mult:
        s.pipeline_ram_mult[pid] = 1.0
    s.pipeline_last_enq_tick[pid] = s.tick

    if p.priority == Priority.QUERY:
        s.q_query.append(p)
    elif p.priority == Priority.INTERACTIVE:
        s.q_interactive.append(p)
    else:
        s.q_batch.append(p)


def _cleanup_pipeline_state(s, pid):
    s.pipeline_retries.pop(pid, None)
    s.pipeline_ram_mult.pop(pid, None)
    s.pipeline_last_enq_tick.pop(pid, None)
    s.pipeline_priority.pop(pid, None)


def _pipeline_done_or_irrecoverable(s, pipeline):
    status = pipeline.runtime_status()
    if status.is_pipeline_successful():
        return True, True  # drop, successful

    # If there are failures, we may retry (bounded). If exceeded, drop.
    has_failures = status.state_counts[OperatorState.FAILED] > 0
    if not has_failures:
        return False, False

    pid = pipeline.pipeline_id
    retries = s.pipeline_retries.get(pid, 0)
    if retries >= s.max_retries:
        return True, False  # drop, irrecoverable
    return False, False


def _pick_next_pipeline(s):
    # Strict priority for latency-sensitive work; add mild batch aging to avoid starvation.
    if s.q_query:
        return s.q_query.pop(0)
    if s.q_interactive:
        return s.q_interactive.pop(0)

    # Batch queue: allow "aged" items to stay near front; otherwise FIFO.
    if not s.q_batch:
        return None

    # Find first aged batch pipeline; else FIFO.
    for i, p in enumerate(s.q_batch):
        enq_tick = s.pipeline_last_enq_tick.get(p.pipeline_id, s.tick)
        if (s.tick - enq_tick) >= s.batch_aging_ticks:
            return s.q_batch.pop(i)
    return s.q_batch.pop(0)


def _requeue_pipeline_front(s, pipeline):
    # Requeue near the front of its priority queue to reduce repeated long tail.
    pid = pipeline.pipeline_id
    s.pipeline_last_enq_tick[pid] = s.tick

    if pipeline.priority == Priority.QUERY:
        s.q_query.insert(0, pipeline)
    elif pipeline.priority == Priority.INTERACTIVE:
        s.q_interactive.insert(0, pipeline)
    else:
        s.q_batch.insert(0, pipeline)


def _requeue_pipeline_back(s, pipeline):
    _enqueue_pipeline(s, pipeline)


def _get_assignable_op(pipeline):
    status = pipeline.runtime_status()
    ops = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
    if not ops:
        return None
    return ops[0]


def _estimate_mem_gb(op):
    est = getattr(op, "estimate", None)
    if est is None:
        return None
    return getattr(est, "mem_peak_gb", None)


def _desired_ram_gb(s, pipeline, pool, op):
    # Memory request uses estimator hint if present; otherwise conservative fraction of pool.
    est_mem = _estimate_mem_gb(op)
    mult = s.pipeline_ram_mult.get(pipeline.pipeline_id, 1.0)

    if est_mem is None:
        ram = max(s.min_ram_gb, pool.max_ram_pool * s.default_ram_frac)
    else:
        ram = max(s.min_ram_gb, (est_mem * mult) + s.ram_safety_gb)

    # Never request more than pool maximum (if it can't ever fit, scheduler should skip).
    if ram > pool.max_ram_pool:
        ram = pool.max_ram_pool
    return ram


def _desired_cpu(s, pipeline, pool, op, avail_cpu):
    # Latency-sensitive: take more CPU if available, but avoid monopolizing the pool.
    # Batch: smaller share to preserve responsiveness.
    if pipeline.priority == Priority.QUERY:
        cap = max(1.0, pool.max_cpu_pool * 0.75)
    elif pipeline.priority == Priority.INTERACTIVE:
        cap = max(1.0, pool.max_cpu_pool * 0.60)
    else:
        cap = max(1.0, pool.max_cpu_pool * 0.40)

    cpu = min(avail_cpu, cap)
    if cpu < 1.0:
        cpu = avail_cpu  # may be fractional in some simulators; better than refusing work
    return cpu


def _pool_is_reserved_for_high_prio(s, pool_id):
    # Prefer keeping pool 0 for QUERY/INTERACTIVE if multiple pools exist.
    return (s.executor.num_pools > 1) and (pool_id == 0)


def _choose_pool_for_op(s, pipeline, op):
    # Choose a feasible pool that minimizes leftover RAM after assignment (best-fit),
    # while respecting pool 0 preference for latency-sensitive workloads.
    candidates = []

    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool
        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # If pool 0 is "reserved", don't schedule batch there unless no other feasible pool exists.
        if pipeline.priority == Priority.BATCH_PIPELINE and _pool_is_reserved_for_high_prio(s, pool_id):
            continue

        ram_req = _desired_ram_gb(s, pipeline, pool, op)
        est_mem = _estimate_mem_gb(op)

        # Use estimator as a hint to skip likely-infeasible placements.
        # If no estimate, only check against our requested RAM.
        if est_mem is not None and est_mem > pool.max_ram_pool:
            continue  # will never fit this pool

        if ram_req > avail_ram:
            continue

        # Score: prefer tighter RAM fit; tie-break with more available CPU.
        leftover_ram = avail_ram - ram_req
        candidates.append((leftover_ram, -avail_cpu, pool_id, ram_req))

    # If batch was blocked from pool 0 and nothing else works, allow pool 0 as fallback.
    if not candidates and pipeline.priority == Priority.BATCH_PIPELINE and _pool_is_reserved_for_high_prio(s, 0):
        pool = s.executor.pools[0]
        if pool.avail_cpu_pool > 0 and pool.avail_ram_pool > 0:
            ram_req = _desired_ram_gb(s, pipeline, pool, op)
            est_mem = _estimate_mem_gb(op)
            if (est_mem is None or est_mem <= pool.max_ram_pool) and (ram_req <= pool.avail_ram_pool):
                leftover_ram = pool.avail_ram_pool - ram_req
                candidates.append((leftover_ram, -pool.avail_cpu_pool, 0, ram_req))

    if not candidates:
        return None  # no feasible pool right now

    candidates.sort()
    _, _, pool_id, ram_req = candidates[0]
    return pool_id, ram_req


@register_scheduler(key="scheduler_est_012")
def scheduler_est_012(s, results, pipelines):
    """
    Scheduler step:
      - Enqueue arriving pipelines by priority.
      - Process results:
          * On failures: bump per-pipeline RAM multiplier and increment retries.
      - Attempt to assign one ready operator at a time, prioritizing QUERY/INTERACTIVE.
      - Memory-aware placement across pools using estimated peak memory hints.
    """
    s.tick += 1

    # Enqueue new arrivals
    for p in pipelines:
        _enqueue_pipeline(s, p)

    # Update retry state based on execution results
    # (We conservatively assume any failure might be memory-related; bump RAM multiplier.)
    for r in results:
        if r is None:
            continue
        if hasattr(r, "failed") and r.failed():
            for op in getattr(r, "ops", []) or []:
                pid = getattr(op, "pipeline_id", None)
                # We may not have pipeline_id on op; rely on r.pipeline_id if present.
                if pid is None:
                    pid = getattr(r, "pipeline_id", None)
                if pid is None:
                    continue
                s.pipeline_retries[pid] = s.pipeline_retries.get(pid, 0) + 1
                s.pipeline_ram_mult[pid] = s.pipeline_ram_mult.get(pid, 1.0) * s.ram_mult_growth

    # Early exit if nothing changed and no backlog
    if not pipelines and not results and not (s.q_query or s.q_interactive or s.q_batch):
        return [], []

    suspensions = []
    assignments = []

    # Try to keep pools busy, but avoid over-aggressive packing:
    # assign at most one operator per pool per tick.
    pools_considered = list(range(s.executor.num_pools))

    # For reserved pool 0: try to schedule high-priority first on it.
    if s.executor.num_pools > 1:
        pools_considered = [0] + [i for i in range(1, s.executor.num_pools)]

    used_pools = set()

    # Attempt to schedule up to N assignments per tick (N = number of pools).
    # We pop pipelines, attempt an assignment; if blocked (no feasible pool), we requeue.
    max_attempts = max(1, (len(s.q_query) + len(s.q_interactive) + len(s.q_batch)) * 2)
    attempts = 0

    while attempts < max_attempts and len(used_pools) < s.executor.num_pools:
        attempts += 1
        pipeline = _pick_next_pipeline(s)
        if pipeline is None:
            break

        pid = pipeline.pipeline_id
        drop, successful = _pipeline_done_or_irrecoverable(s, pipeline)
        if drop:
            _cleanup_pipeline_state(s, pid)
            continue

        op = _get_assignable_op(pipeline)
        if op is None:
            # Not ready yet; keep it in the queue.
            _requeue_pipeline_back(s, pipeline)
            continue

        choice = _choose_pool_for_op(s, pipeline, op)
        if choice is None:
            # Can't place now; keep it queued.
            _requeue_pipeline_back(s, pipeline)
            continue

        pool_id, ram_req = choice
        if pool_id in used_pools:
            # Only one assignment per pool per tick; requeue and try another pipeline.
            _requeue_pipeline_back(s, pipeline)
            continue

        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool
        if avail_cpu <= 0 or avail_ram <= 0 or ram_req > avail_ram:
            _requeue_pipeline_back(s, pipeline)
            continue

        cpu_req = _desired_cpu(s, pipeline, pool, op, avail_cpu)
        if cpu_req <= 0:
            _requeue_pipeline_back(s, pipeline)
            continue

        assignments.append(
            Assignment(
                ops=[op],
                cpu=cpu_req,
                ram=ram_req,
                priority=pipeline.priority,
                pool_id=pool_id,
                pipeline_id=pid,
            )
        )
        used_pools.add(pool_id)

        # Requeue pipeline to allow subsequent operators to be scheduled in later ticks.
        _requeue_pipeline_front(s, pipeline)

    return suspensions, assignments
