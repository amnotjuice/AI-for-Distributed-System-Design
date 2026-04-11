# policy_key: scheduler_medium_001
# reasoning_effort: medium
# exp: reasoning
# model: gpt-5.2-2025-12-11
# llm_cost: 0.000000
# generation_seconds: 119.25
# generated_at: 2026-04-09T22:29:06.860403
@register_scheduler_init(key="scheduler_medium_001")
def scheduler_medium_001_init(s):
    """Priority-aware, failure-averse scheduler.

    Main ideas:
    - Always prioritize QUERY then INTERACTIVE then BATCH, but avoid starvation via simple aging.
    - Use conservative (generous) RAM sizing to reduce OOM risk (OOMs are very costly via 720s penalty).
    - Learn per-operator-type RAM needs from OOM failures and increase future allocations.
    - Use soft reservations: don't let BATCH consume the last headroom that high-priority work may need.
    - Assign a single operator per container for responsiveness and safer retries.
    """
    s.tick = 0

    # Per-priority pipeline queues (store Pipeline objects).
    s.queues = {}

    # Arrival tick for aging/fairness.
    s.arrival_tick = {}

    # Track repeated failures per pipeline (to abandon hopeless pipelines and protect high-priority latency).
    s.pipeline_fail_count = {}
    s.pipeline_abandoned = set()

    # RAM learning keyed by operator "type" signature.
    s.op_ram_est = {}          # op_type -> est_ram
    s.op_fail_count = {}       # op_type -> total_failures
    s.op_last_fail_oom = {}    # op_type -> bool

    # Per-tick duplicate prevention: set of (pipeline_id, op_uid) already assigned this scheduling call.
    s._assigned_this_tick = set()


@register_scheduler(key="scheduler_medium_001")
def scheduler_medium_001_scheduler(s, results, pipelines):
    """See init docstring for policy overview."""
    # Helper functions are defined inside to avoid imports at module scope.

    def _safe_getattr(obj, names, default=None):
        for n in names:
            if hasattr(obj, n):
                v = getattr(obj, n)
                if v is not None:
                    return v
        return default

    def _is_oom(err):
        if err is None:
            return False
        msg = str(err).lower()
        return ("oom" in msg) or ("out of memory" in msg) or ("killed process" in msg and "memory" in msg)

    def _op_uid(op):
        # Unique-ish within a pipeline for "already assigned this tick" checks.
        return _safe_getattr(op, ["operator_id", "op_id", "id", "uid", "name"], default=str(op))

    def _op_type(op):
        # Stable-ish signature for RAM learning; best effort based on available fields.
        return _safe_getattr(op, ["operator_type", "kind", "op_name", "name", "type"], default=str(op))

    def _op_min_ram(op):
        v = _safe_getattr(
            op,
            ["min_ram", "ram_min", "min_memory", "memory_min", "min_mem", "min_ram_gb", "ram_requirement"],
            default=None,
        )
        if v is None:
            return 0.0
        try:
            return float(v)
        except Exception:
            return 0.0

    def _priority_weight(priority):
        # Used only for aging heuristics; objective weights: query 10, interactive 5, batch 1.
        if priority == Priority.QUERY:
            return 10
        if priority == Priority.INTERACTIVE:
            return 5
        return 1

    def _cpu_target(pool, priority):
        max_cpu = pool.max_cpu_pool
        if priority == Priority.QUERY:
            return max(1, int(0.50 * max_cpu))
        if priority == Priority.INTERACTIVE:
            return max(1, int(0.40 * max_cpu))
        return max(1, int(0.25 * max_cpu))

    def _ram_target(pool, priority):
        max_ram = pool.max_ram_pool
        # Be intentionally generous on RAM to reduce OOMs (extra RAM only reduces concurrency).
        if priority == Priority.QUERY:
            return 0.35 * max_ram
        if priority == Priority.INTERACTIVE:
            return 0.28 * max_ram
        return 0.20 * max_ram

    def _global_max_ram():
        m = 0
        for i in range(s.executor.num_pools):
            m = max(m, s.executor.pools[i].max_ram_pool)
        return m

    def _pipeline_cleanup_and_queue(p):
        # Returns True if pipeline should stay queued.
        if p is None:
            return False
        pid = p.pipeline_id
        if pid in s.pipeline_abandoned:
            return False
        st = p.runtime_status()
        if st.is_pipeline_successful():
            return False

        # If it has lots of non-OOM failures, abandon to protect higher-priority latency.
        # (Abandoning doesn't worsen objective vs failing/incomplete; it avoids wasting capacity.)
        fails = s.pipeline_fail_count.get(pid, 0)
        if fails >= 6:
            s.pipeline_abandoned.add(pid)
            return False
        return True

    def _pipeline_info_cached(p, cache):
        pid = p.pipeline_id
        if pid in cache:
            return cache[pid]
        st = p.runtime_status()
        # Remaining work proxy (favor shorter pipelines to complete faster: SRPT-like).
        pending = 0
        failed = 0
        try:
            pending = st.state_counts.get(OperatorState.PENDING, 0)
            failed = st.state_counts.get(OperatorState.FAILED, 0)
        except Exception:
            pending, failed = 0, 0
        remaining = pending + failed
        age = s.tick - s.arrival_tick.get(pid, s.tick)
        cache[pid] = (st, remaining, age)
        return cache[pid]

    def _pick_ready_op_for_pipeline(p, st, scheduled_set):
        # Choose the first ready op that we haven't already assigned this tick.
        ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        if not ops:
            return None
        pid = p.pipeline_id
        for op in ops:
            uid = _op_uid(op)
            if (pid, uid) in scheduled_set:
                continue
            return op
        return None

    def _pick_pipeline(priority, cache, scheduled_set):
        # Select best candidate pipeline among queued ones for this priority:
        # - Prefer smaller remaining work (SRPT-like)
        # - Tie-break by higher age (older first, prevents starvation)
        # - Skip pipelines without a ready op
        q = s.queues.get(priority, [])
        best = None
        best_key = None
        best_op = None

        for p in q:
            if not _pipeline_cleanup_and_queue(p):
                continue
            pid = p.pipeline_id
            st, remaining, age = _pipeline_info_cached(p, cache)
            if st.is_pipeline_successful():
                continue

            op = _pick_ready_op_for_pipeline(p, st, scheduled_set)
            if op is None:
                continue

            # If we learned this op needs more RAM than any pool has, abandon early.
            op_type = _op_type(op)
            est_ram = s.op_ram_est.get(op_type, 0.0)
            if est_ram > _global_max_ram() and est_ram > 0:
                s.pipeline_abandoned.add(pid)
                continue

            # Priority-weighted aging: batch ages faster (to avoid starvation), but queries still win on class order.
            # This affects tie-breaking within a priority class only.
            w = _priority_weight(priority)
            weighted_age = age * max(1, int(10 / w))  # batch gets bigger multiplier

            key = (remaining, -weighted_age, str(pid))
            if best is None or key < best_key:
                best = p
                best_key = key
                best_op = op

        return best, best_op

    # ---- Step bookkeeping ----
    s.tick += 1
    s._assigned_this_tick = set()

    # Ensure queues exist for all priorities.
    for pr in [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]:
        if pr not in s.queues:
            s.queues[pr] = []

    # Process execution results: update failure counts and RAM learning.
    # NOTE: we intentionally do not do preemption (API for running containers isn't provided reliably).
    for r in results or []:
        if hasattr(r, "failed") and r.failed():
            # Associate failures with operator type; increase RAM estimate on OOM.
            ops = getattr(r, "ops", None) or []
            pid = _safe_getattr(r, ["pipeline_id"], default=None)  # may not exist in simulator
            if pid is not None:
                s.pipeline_fail_count[pid] = s.pipeline_fail_count.get(pid, 0) + 1

            oom = _is_oom(getattr(r, "error", None))
            for op in ops:
                op_type = _op_type(op)
                s.op_fail_count[op_type] = s.op_fail_count.get(op_type, 0) + 1
                s.op_last_fail_oom[op_type] = bool(oom)

                if oom:
                    # Aggressively bump RAM estimate to avoid repeated OOMs.
                    # Use the RAM that was allocated to the failing container as a floor.
                    try:
                        allocated_ram = float(getattr(r, "ram", 0.0) or 0.0)
                    except Exception:
                        allocated_ram = 0.0
                    prev = float(s.op_ram_est.get(op_type, 0.0) or 0.0)
                    bumped = max(prev, allocated_ram * 1.6, allocated_ram + 1.0)
                    s.op_ram_est[op_type] = bumped

    # Enqueue new pipelines (never drop arrivals).
    for p in pipelines or []:
        pid = p.pipeline_id
        if pid not in s.arrival_tick:
            s.arrival_tick[pid] = s.tick
        if pid not in s.pipeline_fail_count:
            s.pipeline_fail_count[pid] = 0
        if pid in s.pipeline_abandoned:
            continue
        s.queues[p.priority].append(p)

    # Remove completed/abandoned from queues to keep scans small.
    for pr in list(s.queues.keys()):
        new_q = []
        for p in s.queues[pr]:
            if _pipeline_cleanup_and_queue(p):
                new_q.append(p)
        s.queues[pr] = new_q

    # If no changes and no new arrivals, do nothing.
    if not results and not pipelines:
        return [], []

    suspensions = []
    assignments = []

    # Cache pipeline status computations per tick.
    cache = {}

    # Pool ordering: try to place high-priority work on pools with the most headroom.
    pool_ids = list(range(s.executor.num_pools))
    pool_ids.sort(
        key=lambda i: (
            s.executor.pools[i].avail_cpu_pool,
            s.executor.pools[i].avail_ram_pool,
            s.executor.pools[i].max_cpu_pool,
            s.executor.pools[i].max_ram_pool,
        ),
        reverse=True,
    )

    # Soft reservations per pool to protect future QUERY/INTERACTIVE arrivals.
    def _reserve_cpu(pool):
        return max(1, int(0.15 * pool.max_cpu_pool))

    def _reserve_ram(pool):
        return 0.15 * pool.max_ram_pool

    # Main scheduling loop: pack multiple single-op assignments per pool.
    for pool_id in pool_ids:
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool
        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # Keep attempting to place work while resources remain.
        while True:
            made = False

            # Strict class order for objective: QUERY, then INTERACTIVE, then BATCH.
            for pr in [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]:
                if avail_cpu <= 0 or avail_ram <= 0:
                    break

                p, op = _pick_pipeline(pr, cache, s._assigned_this_tick)
                if p is None or op is None:
                    continue

                pid = p.pipeline_id
                uid = _op_uid(op)
                op_type = _op_type(op)

                # Determine RAM request: max of (priority target, learned OOM estimate, op min RAM).
                learned = float(s.op_ram_est.get(op_type, 0.0) or 0.0)
                min_ram = float(_op_min_ram(op) or 0.0)
                base_ram = float(_ram_target(pool, pr) or 0.0)
                ram_req = max(base_ram, learned, min_ram, 0.0)

                # Determine CPU request: cap by available CPU.
                cpu_req = int(_cpu_target(pool, pr))
                cpu_req = max(1, min(int(avail_cpu), int(cpu_req)))

                # If batch, respect reservations so batch doesn't consume the last headroom.
                if pr == Priority.BATCH_PIPELINE:
                    if (avail_cpu - cpu_req) < _reserve_cpu(pool) or (avail_ram - ram_req) < _reserve_ram(pool):
                        continue

                if cpu_req <= 0 or ram_req <= 0:
                    continue
                if cpu_req > avail_cpu or ram_req > avail_ram:
                    continue

                # Avoid issuing duplicate assignments for the same op in the same tick.
                if (pid, uid) in s._assigned_this_tick:
                    continue

                assignment = Assignment(
                    ops=[op],
                    cpu=cpu_req,
                    ram=ram_req,
                    priority=p.priority,
                    pool_id=pool_id,
                    pipeline_id=pid,
                )
                assignments.append(assignment)

                s._assigned_this_tick.add((pid, uid))
                avail_cpu -= cpu_req
                avail_ram -= ram_req
                made = True
                break  # re-evaluate from highest priority again

            if not made:
                break

    return suspensions, assignments
