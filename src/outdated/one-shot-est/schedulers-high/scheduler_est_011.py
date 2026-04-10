# policy_key: scheduler_est_011
# reasoning_effort: high
# model: gpt-5.2-2025-12-11
# llm_cost: 0.148784
# generation_seconds: 96.85
# generated_at: 2026-03-31T18:42:03.356831
@register_scheduler_init(key="scheduler_est_011")
def scheduler_est_011_init(s):
    """
    Priority-aware, headroom-preserving scheduler with simple right-sizing.

    Improvements over naive FIFO:
      - Separate per-priority queues; always schedule higher priority first.
      - Avoid "give the whole pool to one op": cap CPU per op to improve concurrency/latency.
      - Use op.estimate.mem_peak_gb to right-size RAM and reduce OOM risk.
      - On OOM failures (from ExecutionResult.error), increase RAM multiplier for that op and retry.
      - Basic head-of-line blocking avoidance: within each priority queue, scan for an op that fits.
      - Keep headroom for high-priority work by not filling pools with batch when HP is waiting.
    """
    from collections import deque

    s.queues = {
        Priority.QUERY: deque(),
        Priority.INTERACTIVE: deque(),
        Priority.BATCH_PIPELINE: deque(),
    }
    s.seen_pipeline_ids = set()

    # Per-op adaptive RAM multiplier (boost on OOM)
    s.op_ram_factor = {}  # op_key -> float

    # Conservative defaults; tuned for "small improvements" first.
    s.default_ram_factor = 1.20
    s.max_ram_factor = 6.00
    s.oom_backoff = 1.50

    s.min_ram_gb = 0.25  # never request < 256MB
    s.ram_overhead_gb = 0.10  # small overhead beyond estimate


def _op_key(op):
    """Best-effort stable key for an operator object."""
    k = getattr(op, "op_id", None)
    if k is not None:
        return ("op_id", k)
    k = getattr(op, "operator_id", None)
    if k is not None:
        return ("operator_id", k)
    return ("py_id", id(op))


def _get_est_mem_gb(op):
    est = getattr(op, "estimate", None)
    mem = getattr(est, "mem_peak_gb", None)
    if mem is None:
        return 1.0
    try:
        return float(mem)
    except Exception:
        return 1.0


def _cpu_cap_for_priority(priority, pool):
    # Small, safe CPU caps that improve latency by enabling concurrency.
    max_cpu = pool.max_cpu_pool
    if priority == Priority.QUERY:
        return min(4.0, max_cpu)
    if priority == Priority.INTERACTIVE:
        return min(2.0, max_cpu)
    return 1.0  # batch


def _ram_request_gb(s, op, pool):
    est_mem = _get_est_mem_gb(op)
    factor = s.op_ram_factor.get(_op_key(op), s.default_ram_factor)
    ram = est_mem * factor + s.ram_overhead_gb
    ram = max(ram, s.min_ram_gb)
    ram = min(ram, pool.max_ram_pool)
    return ram


def _dequeue_runnable_that_fits(s, prio, pool, avail_cpu, avail_ram, scheduled_pipeline_ids):
    """
    Scan within one priority queue to find a pipeline with a ready op that fits the current pool headroom.
    Returns (pipeline, [op], cpu, ram) or None.
    """
    q = s.queues[prio]
    n = len(q)
    for _ in range(n):
        p = q.popleft()

        # Avoid scheduling multiple ops from the same pipeline within one tick (simple fairness)
        if p.pipeline_id in scheduled_pipeline_ids:
            q.append(p)
            continue

        status = p.runtime_status()
        if status.is_pipeline_successful():
            continue

        op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
        if not op_list:
            q.append(p)
            continue

        op = op_list[0]
        ram = _ram_request_gb(s, op, pool)
        if ram > avail_ram:
            # Doesn't fit; rotate to avoid head-of-line blocking on a single big op.
            q.append(p)
            continue

        cpu_cap = _cpu_cap_for_priority(prio, pool)
        if avail_cpu <= 0:
            q.append(p)
            continue
        cpu = min(cpu_cap, avail_cpu)
        if cpu <= 0:
            q.append(p)
            continue

        # Put pipeline back so it can make further progress in later ticks.
        q.append(p)
        return p, [op], cpu, ram

    return None


@register_scheduler(key="scheduler_est_011")
def scheduler_est_011_scheduler(s, results: List["ExecutionResult"], pipelines: List["Pipeline"]) -> Tuple[List["Suspend"], List["Assignment"]]:
    # Enqueue new pipelines into per-priority queues.
    for p in pipelines:
        if p.pipeline_id in s.seen_pipeline_ids:
            continue
        s.seen_pipeline_ids.add(p.pipeline_id)
        s.queues[p.priority].append(p)

    # Update per-op RAM factors from observed OOMs.
    for r in results:
        if not r.failed():
            continue
        err = (r.error or "")
        if "oom" in err.lower() or "out of memory" in err.lower():
            for op in (r.ops or []):
                k = _op_key(op)
                cur = s.op_ram_factor.get(k, s.default_ram_factor)
                nxt = min(s.max_ram_factor, max(cur, cur * s.oom_backoff))
                s.op_ram_factor[k] = nxt

    # Early exit if nothing changed that should affect our choices.
    if not pipelines and not results:
        return [], []

    suspensions: List["Suspend"] = []
    assignments: List["Assignment"] = []

    # Determine whether to keep headroom for high-priority arrivals.
    hp_waiting = (len(s.queues[Priority.QUERY]) + len(s.queues[Priority.INTERACTIVE])) > 0

    # Pool ordering: if multiple pools exist, try to keep pool 0 "nicer" for high-priority work.
    pool_ids_hp = list(range(s.executor.num_pools))
    pool_ids_batch = list(range(s.executor.num_pools))
    if s.executor.num_pools > 1:
        pool_ids_batch = list(range(1, s.executor.num_pools)) + [0]

    scheduled_pipeline_ids = set()

    # Phase 1: schedule high priority (QUERY then INTERACTIVE) greedily across pools.
    for pool_id in pool_ids_hp:
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool
        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # Limit per-tick assignments per pool to avoid pathological loops.
        for _ in range(32):
            if avail_cpu <= 0 or avail_ram <= 0:
                break

            picked = None
            for prio in (Priority.QUERY, Priority.INTERACTIVE):
                picked = _dequeue_runnable_that_fits(
                    s, prio, pool, avail_cpu, avail_ram, scheduled_pipeline_ids
                )
                if picked is not None:
                    break

            if picked is None:
                break

            p, op_list, cpu, ram = picked
            assignments.append(
                Assignment(
                    ops=op_list,
                    cpu=cpu,
                    ram=ram,
                    priority=p.priority,
                    pool_id=pool_id,
                    pipeline_id=p.pipeline_id,
                )
            )
            scheduled_pipeline_ids.add(p.pipeline_id)
            avail_cpu -= cpu
            avail_ram -= ram

    # Phase 2: schedule batch, but keep a little headroom if HP is waiting.
    for pool_id in pool_ids_batch:
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool
        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # Soft headroom reservation when high-priority is queued.
        reserve_cpu = 0.0
        reserve_ram = 0.0
        if hp_waiting:
            reserve_cpu = 0.20 * pool.max_cpu_pool
            reserve_ram = 0.20 * pool.max_ram_pool

        for _ in range(64):
            if avail_cpu <= reserve_cpu or avail_ram <= reserve_ram:
                break

            picked = _dequeue_runnable_that_fits(
                s,
                Priority.BATCH_PIPELINE,
                pool,
                avail_cpu - reserve_cpu,
                avail_ram - reserve_ram,
                scheduled_pipeline_ids,
            )
            if picked is None:
                break

            p, op_list, cpu, ram = picked
            assignments.append(
                Assignment(
                    ops=op_list,
                    cpu=cpu,
                    ram=ram,
                    priority=p.priority,
                    pool_id=pool_id,
                    pipeline_id=p.pipeline_id,
                )
            )
            scheduled_pipeline_ids.add(p.pipeline_id)
            avail_cpu -= cpu
            avail_ram -= ram

    return suspensions, assignments
