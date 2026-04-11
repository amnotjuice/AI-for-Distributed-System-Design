# policy_key: scheduler_medium_037
# reasoning_effort: medium
# model: gpt-5.2-2025-12-11
# llm_cost: 0.066821
# generation_seconds: 69.53
# generated_at: 2026-03-14T03:06:45.451387
@register_scheduler_init(key="scheduler_medium_037")
def scheduler_medium_037_init(s):
    """
    Priority-aware, multi-assignment scheduler with small, incremental improvements over FIFO:

    1) Maintain per-priority queues (QUERY/INTERACTIVE ahead of BATCH).
    2) Pack multiple runnable operators per pool per tick using fractional resource slices
       (instead of giving the entire pool to a single op).
    3) Add a simple anti-starvation rule: after a streak of high-priority assignments,
       try to schedule a batch op if any is runnable.
    4) Add conservative "OOM adaptation" without needing per-container mapping:
       if a pipeline shows new FAILED ops, increase its RAM scale for future retries
       (FAILED is considered assignable in the simulator).
    5) If there are multiple pools, treat pool 0 as "latency pool" by reserving headroom
       and avoiding batch there when high-priority work exists.
    """
    from collections import deque

    # Priority queues (round-robin within each priority class)
    s.q_query = deque()
    s.q_interactive = deque()
    s.q_batch = deque()

    # Per-pipeline adaptive state (keyed by pipeline_id)
    # Note: we infer failures from pipeline.runtime_status() rather than results, because
    # ExecutionResult does not reliably carry pipeline_id in the provided interface.
    s.pipeline_meta = {}  # pipeline_id -> {"ram_scale": float, "retries": int, "last_failed": int}

    # Retry / scaling knobs
    s.max_retries = 3
    s.ram_scale_init = 1.0
    s.ram_scale_mult = 1.75
    s.ram_scale_cap = 8.0

    # Anti-starvation knobs
    s.high_pr_streak = 0
    s.high_pr_streak_limit = 8  # after N high-pr assignments, try to schedule 1 batch op

    # Minimum allocation (avoid zero-sized assignments)
    s.min_cpu = 1
    s.min_ram = 1


def _q_for_priority(s, prio):
    if prio == Priority.QUERY:
        return s.q_query
    if prio == Priority.INTERACTIVE:
        return s.q_interactive
    return s.q_batch


def _ensure_pipeline_meta(s, pipeline_id):
    meta = s.pipeline_meta.get(pipeline_id)
    if meta is None:
        meta = {"ram_scale": float(s.ram_scale_init), "retries": 0, "last_failed": 0}
        s.pipeline_meta[pipeline_id] = meta
    return meta


def _update_meta_from_status(s, pipeline, status):
    """
    If we observe new FAILED ops since the last time we examined this pipeline, treat that as
    an OOM-like signal and increase RAM scale for future retries. Cap retries to avoid infinite loops.
    """
    meta = _ensure_pipeline_meta(s, pipeline.pipeline_id)
    failed_now = int(status.state_counts.get(OperatorState.FAILED, 0))
    if failed_now > meta["last_failed"]:
        meta["retries"] += 1
        meta["ram_scale"] = min(s.ram_scale_cap, meta["ram_scale"] * s.ram_scale_mult)
        meta["last_failed"] = failed_now
    else:
        # Keep last_failed in sync as failures clear (if the simulator transitions FAILED->COMPLETED on retry)
        meta["last_failed"] = failed_now
    return meta


def _compute_slice(priority, avail_cpu, avail_ram, pool, ram_scale):
    """
    Allocate a fractional "slice" of remaining resources.
    High priority gets larger slices to reduce latency; batch gets smaller slices to allow concurrency.
    RAM is scaled up for pipelines that previously failed (likely OOM).
    """
    # Fractions of remaining resources
    if priority == Priority.QUERY:
        cpu_frac, ram_frac = 0.60, 0.60
    elif priority == Priority.INTERACTIVE:
        cpu_frac, ram_frac = 0.50, 0.55
    else:
        cpu_frac, ram_frac = 0.25, 0.30

    # Compute slice from remaining resources (not from pool max) to pack multiple ops per tick
    cpu = int(max(1, min(avail_cpu, max(1, int(avail_cpu * cpu_frac)))))
    ram = int(max(1, min(avail_ram, max(1, int(avail_ram * ram_frac)))))

    # Apply RAM scale (bounded by availability)
    ram = int(min(avail_ram, max(1, int(ram * float(ram_scale)))))

    # Enforce minimums
    cpu = max(cpu, 1)
    ram = max(ram, 1)
    return cpu, ram


def _pop_runnable_op_from_queue(s, q, require_parents_complete=True):
    """
    Round-robin scan: find the next pipeline in q that has a runnable op.
    Requeue pipelines that are not runnable yet.
    Returns (pipeline, [op]) or (None, None).
    """
    n = len(q)
    for _ in range(n):
        pipeline = q.popleft()
        status = pipeline.runtime_status()

        # Drop terminal pipelines
        if status.is_pipeline_successful():
            continue

        # Update retry meta; if too many retries, drop pipeline (avoid infinite churn)
        meta = _update_meta_from_status(s, pipeline, status)
        if meta["retries"] > s.max_retries:
            continue

        # Find one runnable op (PENDING or FAILED, with parents complete)
        op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=require_parents_complete)[:1]
        if not op_list:
            # Not runnable yet; keep it in rotation
            q.append(pipeline)
            continue

        # Runnable: requeue pipeline for fairness, return one op
        q.append(pipeline)
        return pipeline, op_list

    return None, None


def _choose_priority_order(s, pool_id, has_latency_pool):
    """
    Decide which priorities to attempt first for this pool, with a simple anti-starvation rule.
    """
    # Base strict priority
    high_first = [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH]

    # Anti-starvation: after a streak of high-pr assignments, try batch once (if exists).
    if s.high_pr_streak >= s.high_pr_streak_limit and len(s.q_batch) > 0:
        return [Priority.BATCH, Priority.QUERY, Priority.INTERACTIVE]

    # If we have a dedicated latency pool (pool 0), bias it even more towards high-pr work.
    if has_latency_pool and pool_id == 0:
        return high_first

    return high_first


@register_scheduler(key="scheduler_medium_037")
def scheduler_medium_037(s, results, pipelines):
    """
    Main scheduling step:
    - Enqueue newly arrived pipelines by priority.
    - For each pool, repeatedly place runnable ops using priority-aware selection and sliced resources.
    - Keep pool 0 (if present) as a latency pool by reserving headroom and avoiding batch there
      when high-priority work exists.
    """
    # Enqueue arrivals
    for p in pipelines:
        _ensure_pipeline_meta(s, p.pipeline_id)
        _q_for_priority(s, p.priority).append(p)

    # Early exit if nothing changed
    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    has_latency_pool = s.executor.num_pools > 1

    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = int(pool.avail_cpu_pool)
        avail_ram = int(pool.avail_ram_pool)

        if avail_cpu < s.min_cpu or avail_ram < s.min_ram:
            continue

        # Latency-pool headroom rule (soft reservation):
        # If high-pr work exists, avoid consuming the last chunk of resources in pool 0.
        high_waiting = (len(s.q_query) > 0) or (len(s.q_interactive) > 0)
        reserve_cpu = 0
        reserve_ram = 0
        if has_latency_pool and pool_id == 0 and high_waiting:
            reserve_cpu = max(1, int(0.20 * int(pool.max_cpu_pool)))
            reserve_ram = max(1, int(0.20 * int(pool.max_ram_pool)))

        # Place multiple ops per pool per tick
        placed_any = True
        while placed_any:
            placed_any = False

            if avail_cpu < s.min_cpu or avail_ram < s.min_ram:
                break

            prio_order = _choose_priority_order(s, pool_id, has_latency_pool)

            chosen_pipeline = None
            chosen_ops = None
            chosen_prio = None

            # Find the highest-priority runnable op
            for pr in prio_order:
                # On latency pool with high-pr waiting, do not run batch at all.
                if has_latency_pool and pool_id == 0 and high_waiting and pr == Priority.BATCH:
                    continue

                q = _q_for_priority(s, pr)
                if not q:
                    continue

                pipeline, op_list = _pop_runnable_op_from_queue(s, q, require_parents_complete=True)
                if pipeline is None:
                    continue

                chosen_pipeline = pipeline
                chosen_ops = op_list
                chosen_prio = pr
                break

            if chosen_pipeline is None:
                break

            # Compute allocation (with per-pipeline RAM scaling)
            meta = _ensure_pipeline_meta(s, chosen_pipeline.pipeline_id)
            cpu, ram = _compute_slice(chosen_prio, avail_cpu, avail_ram, pool, meta["ram_scale"])

            # Enforce latency pool reservation by shrinking or skipping (only relevant for pool 0)
            if has_latency_pool and pool_id == 0 and high_waiting:
                # Ensure we don't go below reserve after this placement.
                if (avail_cpu - cpu) < reserve_cpu or (avail_ram - ram) < reserve_ram:
                    # Try shrinking once for batch/interactive; for query keep it aggressive.
                    if chosen_prio != Priority.QUERY:
                        cpu = max(s.min_cpu, min(cpu, max(0, avail_cpu - reserve_cpu)))
                        ram = max(s.min_ram, min(ram, max(0, avail_ram - reserve_ram)))

                    # If still can't respect reserve, stop placing on this pool this tick.
                    if cpu < s.min_cpu or ram < s.min_ram or (avail_cpu - cpu) < reserve_cpu or (avail_ram - ram) < reserve_ram:
                        break

            # Create assignment
            assignments.append(
                Assignment(
                    ops=chosen_ops,
                    cpu=int(cpu),
                    ram=int(ram),
                    priority=chosen_pipeline.priority,
                    pool_id=pool_id,
                    pipeline_id=chosen_pipeline.pipeline_id,
                )
            )

            # Update anti-starvation streak
            if chosen_prio in (Priority.QUERY, Priority.INTERACTIVE):
                s.high_pr_streak += 1
            else:
                s.high_pr_streak = 0

            # Consume available resources for further packing
            avail_cpu -= int(cpu)
            avail_ram -= int(ram)
            placed_any = True

    return suspensions, assignments
