# policy_key: scheduler_high_031
# reasoning_effort: high
# exp: reasoning
# model: gpt-5.2-2025-12-11
# llm_cost: 0.169873
# generation_seconds: 182.33
# generated_at: 2026-04-10T02:41:22.712940
@register_scheduler_init(key="scheduler_high_031")
def scheduler_high_031_init(s):
    """
    Priority-first, failure-averse scheduler.

    Core ideas:
      1) Strictly prioritize QUERY then INTERACTIVE to minimize weighted latency.
      2) Keep BATCH from blocking future high-priority arrivals via per-pool reservations.
      3) Aggressively mitigate OOM-driven failures by remembering per-operator RAM and
         exponentially increasing it after failures (bounded), because failures/incompletion
         are heavily penalized.
      4) Avoid duplicate queue entries and limit to one op per pipeline per tick for fairness.
    """
    from collections import deque

    s.ticks = 0

    # One FIFO queue per priority class
    s.queues = {
        Priority.QUERY: deque(),
        Priority.INTERACTIVE: deque(),
        Priority.BATCH_PIPELINE: deque(),
    }
    # Pipeline ids currently enqueued (prevents duplicates)
    s.enqueued = set()

    # Operator -> pipeline mapping (since ExecutionResult may not carry pipeline_id)
    s.op_to_pid = {}  # op_ident -> pipeline_id

    # Per-operator RAM estimate and retry counts
    s.op_ram_est = {}   # (pipeline_id, op_ident) -> ram
    s.op_retries = {}   # (pipeline_id, op_ident) -> int
    s.blacklisted_pipelines = set()  # pipelines we stop scheduling (after too many failures)

    # Tuning knobs (conservative defaults; goal: fewer failures + good query latency)
    s.max_retries_per_op = 6

    # Batch reservations (protect future high-priority arrivals from being blocked by long batch ops)
    s.batch_reserve_cpu_frac_hi_pool = 0.25
    s.batch_reserve_ram_frac_hi_pool = 0.25
    s.batch_reserve_cpu_frac_other_pool = 0.10
    s.batch_reserve_ram_frac_other_pool = 0.10

    # Default RAM fractions by priority (starting point; bump on failures)
    s.default_ram_frac = {
        Priority.QUERY: 0.20,
        Priority.INTERACTIVE: 0.15,
        Priority.BATCH_PIPELINE: 0.10,
    }

    # Per-op CPU caps by priority (sublinear scaling => cap to avoid waste & allow concurrency)
    s.cpu_cap_abs = {
        Priority.QUERY: 8.0,
        Priority.INTERACTIVE: 4.0,
        Priority.BATCH_PIPELINE: 2.0,
    }

    # Limits to keep scheduler work bounded
    s.max_assignments_per_pool_per_tick = 8
    s.max_queue_scan_per_pick = 64


def _op_ident(op):
    # Robust operator identifier for dict keys across callbacks.
    # Prefer object identity; if op is already a primitive, use it directly.
    try:
        return id(op)
    except Exception:
        return op


def _priority_order_high_then_low():
    return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]


def _priority_order_high():
    return [Priority.QUERY, Priority.INTERACTIVE]


def _is_high(prio):
    return prio in (Priority.QUERY, Priority.INTERACTIVE)


def _get_op_min_ram_hint(op):
    # Best-effort introspection: different simulators/traces may name this differently.
    for attr in ("min_ram", "ram_min", "ram_required", "required_ram", "peak_ram", "ram"):
        v = getattr(op, attr, None)
        if isinstance(v, (int, float)) and v > 0:
            return float(v)
    return None


def _cpu_request(s, pool, prio, avail_cpu):
    # Priority-based CPU caps; never request more than available.
    max_cpu = float(getattr(pool, "max_cpu_pool", avail_cpu))
    cap = min(float(s.cpu_cap_abs.get(prio, 2.0)), max_cpu)
    # Mildly scale with pool size (avoid too tiny caps on large pools)
    if prio == Priority.QUERY:
        cap = min(cap, max(1.0, 0.50 * max_cpu))
    elif prio == Priority.INTERACTIVE:
        cap = min(cap, max(1.0, 0.33 * max_cpu))
    else:
        cap = min(cap, max(1.0, 0.25 * max_cpu))
    return max(0.0, min(float(avail_cpu), float(cap)))


def _ram_request(s, pool, prio, pipeline_id, op):
    opid = _op_ident(op)
    key = (pipeline_id, opid)

    max_ram = float(getattr(pool, "max_ram_pool", pool.avail_ram_pool))
    est = s.op_ram_est.get(key, None)

    if est is None:
        # Default based on pool size and optional per-op min-ram hint.
        frac = float(s.default_ram_frac.get(prio, 0.10))
        est = max(1.0, frac * max_ram)

        hint = _get_op_min_ram_hint(op)
        if hint is not None:
            # Small safety factor over "min" to avoid borderline OOMs.
            est = max(est, 1.20 * float(hint))

    # Clamp to pool maximum RAM.
    est = min(float(est), max_ram)
    return est


def _enqueue_pipeline(s, p):
    pid = p.pipeline_id
    if pid in s.enqueued:
        return
    if pid in s.blacklisted_pipelines:
        return
    s.queues[p.priority].append(p)
    s.enqueued.add(pid)


def _requeue_pipeline(s, p):
    # Requeue if still relevant.
    pid = p.pipeline_id
    if pid in s.blacklisted_pipelines:
        return
    status = p.runtime_status()
    if status.is_pipeline_successful():
        return
    if pid in s.enqueued:
        return
    s.queues[p.priority].append(p)
    s.enqueued.add(pid)


def _pick_for_pool(s, pool, pool_id, prio_list, avail_cpu, avail_ram, reserve_cpu, reserve_ram, scheduled_this_tick):
    """
    Scan queues (bounded) for a runnable op that fits.
    Returns (pipeline, op_list, cpu_req, ram_req) or (None, None, None, None).
    """
    scans = 0
    for prio in prio_list:
        q = s.queues[prio]
        n = len(q)
        for _ in range(n):
            if scans >= s.max_queue_scan_per_pick:
                return None, None, None, None
            scans += 1

            p = q.popleft()
            pid = p.pipeline_id
            s.enqueued.discard(pid)

            if pid in s.blacklisted_pipelines:
                continue

            status = p.runtime_status()
            if status.is_pipeline_successful():
                continue

            # Limit: at most one op per pipeline per tick (reduces bursty domination).
            if pid in scheduled_this_tick:
                _requeue_pipeline(s, p)
                continue

            op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
            if not op_list:
                _requeue_pipeline(s, p)
                continue

            op = op_list[0]

            cpu_req = _cpu_request(s, pool, prio, avail_cpu)
            if cpu_req <= 0 or cpu_req > avail_cpu:
                _requeue_pipeline(s, p)
                continue

            ram_req = _ram_request(s, pool, prio, pid, op)
            if ram_req <= 0:
                _requeue_pipeline(s, p)
                continue

            # Must fit current availability
            if ram_req > avail_ram:
                _requeue_pipeline(s, p)
                continue

            # If scheduling low priority, enforce reserves for future high priority work.
            if not _is_high(prio):
                if (avail_cpu - cpu_req) < reserve_cpu or (avail_ram - ram_req) < reserve_ram:
                    _requeue_pipeline(s, p)
                    continue

            return p, op_list, cpu_req, ram_req

    return None, None, None, None


@register_scheduler(key="scheduler_high_031")
def scheduler_high_031(s, results: 'List[ExecutionResult]', pipelines: 'List[Pipeline]') -> 'Tuple[List[Suspend], List[Assignment]]':
    suspensions = []
    assignments = []

    s.ticks += 1

    # 1) Incorporate new arrivals
    for p in pipelines:
        _enqueue_pipeline(s, p)

    # 2) Learn from execution results (especially failures => bump RAM to avoid repeats)
    for r in results:
        if not getattr(r, "ops", None):
            continue

        pool_id = getattr(r, "pool_id", None)
        pool = None
        if pool_id is not None and 0 <= int(pool_id) < s.executor.num_pools:
            pool = s.executor.pools[int(pool_id)]

        for op in r.ops:
            opid = _op_ident(op)
            pid = s.op_to_pid.get(opid, None)
            if pid is None:
                continue

            key = (pid, opid)

            if r.failed():
                # Exponential RAM backoff (failure-averse; completion is crucial for the score).
                prev = s.op_ram_est.get(key, float(getattr(r, "ram", 0.0)) or 0.0)
                prev = max(prev, float(getattr(r, "ram", 0.0)) or 0.0, 1.0)

                max_ram = None
                if pool is not None:
                    max_ram = float(getattr(pool, "max_ram_pool", None) or 0.0)
                if not max_ram:
                    # Fallback: allow growth but keep it sane.
                    max_ram = max(prev * 8.0, prev)

                new_est = min(max_ram, max(prev * 2.0, prev + 1.0))
                s.op_ram_est[key] = new_est

                tries = int(s.op_retries.get(key, 0)) + 1
                s.op_retries[key] = tries

                # If an operator keeps failing even as we approach pool max RAM, stop burning resources.
                if tries > int(s.max_retries_per_op):
                    s.blacklisted_pipelines.add(pid)
            else:
                # On success: keep estimate (conservative) to avoid oscillation-induced OOMs.
                if key not in s.op_ram_est and float(getattr(r, "ram", 0.0)) > 0:
                    s.op_ram_est[key] = float(r.ram)

    # Early exit if no new info and no new pipelines (keeps simulator fast).
    if not pipelines and not results:
        return suspensions, assignments

    # 3) Scheduling
    scheduled_this_tick = set()

    # Pool policy: if multiple pools exist, prefer pool 0 for high priority (soft isolation).
    pool_order = list(range(s.executor.num_pools))
    if s.executor.num_pools > 1:
        pool_order = [0] + [i for i in range(1, s.executor.num_pools)]

    # Pass A: schedule high priority first on all pools (minimize weighted latency).
    for pool_id in pool_order:
        pool = s.executor.pools[pool_id]
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)
        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # Do not reserve against high-priority placements.
        reserve_cpu = 0.0
        reserve_ram = 0.0

        per_pool_assigned = 0
        while per_pool_assigned < int(s.max_assignments_per_pool_per_tick):
            p, op_list, cpu_req, ram_req = _pick_for_pool(
                s=s,
                pool=pool,
                pool_id=pool_id,
                prio_list=_priority_order_high(),
                avail_cpu=avail_cpu,
                avail_ram=avail_ram,
                reserve_cpu=reserve_cpu,
                reserve_ram=reserve_ram,
                scheduled_this_tick=scheduled_this_tick,
            )
            if p is None:
                break

            # Record op->pipeline mapping for future result attribution
            for op in op_list:
                s.op_to_pid[_op_ident(op)] = p.pipeline_id

            assignments.append(
                Assignment(
                    ops=op_list,
                    cpu=cpu_req,
                    ram=ram_req,
                    priority=p.priority,
                    pool_id=pool_id,
                    pipeline_id=p.pipeline_id,
                )
            )

            scheduled_this_tick.add(p.pipeline_id)
            avail_cpu -= float(cpu_req)
            avail_ram -= float(ram_req)
            per_pool_assigned += 1

            # Keep pipeline alive in the queue for future ops
            _requeue_pipeline(s, p)

            if avail_cpu <= 0 or avail_ram <= 0:
                break

    # Pass B: schedule batch with reservations to protect future high-priority arrivals.
    for pool_id in pool_order:
        pool = s.executor.pools[pool_id]
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)
        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        max_cpu = float(getattr(pool, "max_cpu_pool", avail_cpu))
        max_ram = float(getattr(pool, "max_ram_pool", avail_ram))

        # Stronger reserves on pool 0 (assumed "hi" pool), weaker elsewhere.
        if s.executor.num_pools > 1 and pool_id == 0:
            reserve_cpu = float(s.batch_reserve_cpu_frac_hi_pool) * max_cpu
            reserve_ram = float(s.batch_reserve_ram_frac_hi_pool) * max_ram
        else:
            reserve_cpu = float(s.batch_reserve_cpu_frac_other_pool) * max_cpu
            reserve_ram = float(s.batch_reserve_ram_frac_other_pool) * max_ram

        per_pool_assigned = 0
        while per_pool_assigned < int(s.max_assignments_per_pool_per_tick):
            p, op_list, cpu_req, ram_req = _pick_for_pool(
                s=s,
                pool=pool,
                pool_id=pool_id,
                prio_list=[Priority.BATCH_PIPELINE],
                avail_cpu=avail_cpu,
                avail_ram=avail_ram,
                reserve_cpu=reserve_cpu,
                reserve_ram=reserve_ram,
                scheduled_this_tick=scheduled_this_tick,
            )
            if p is None:
                break

            for op in op_list:
                s.op_to_pid[_op_ident(op)] = p.pipeline_id

            assignments.append(
                Assignment(
                    ops=op_list,
                    cpu=cpu_req,
                    ram=ram_req,
                    priority=p.priority,
                    pool_id=pool_id,
                    pipeline_id=p.pipeline_id,
                )
            )

            scheduled_this_tick.add(p.pipeline_id)
            avail_cpu -= float(cpu_req)
            avail_ram -= float(ram_req)
            per_pool_assigned += 1

            _requeue_pipeline(s, p)

            if avail_cpu <= 0 or avail_ram <= 0:
                break

    return suspensions, assignments
