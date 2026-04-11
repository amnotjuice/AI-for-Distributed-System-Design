# policy_key: scheduler_high_049
# reasoning_effort: high
# exp: reasoning
# model: gpt-5.2-2025-12-11
# llm_cost: 0.112333
# generation_seconds: 126.96
# generated_at: 2026-04-10T07:43:39.835574
@register_scheduler_init(key="scheduler_high_049")
def scheduler_high_049_init(s):
    """Priority + OOM-aware conservative sizing scheduler.

    Goals:
      - Minimize weighted latency (query >> interactive >> batch) by prioritizing high-priority work.
      - Avoid 720s penalties by (a) not dropping pipelines, (b) reducing OOM risk via conservative RAM sizing,
        and (c) retrying OOM failures with escalating RAM.
      - Prevent starvation (especially batch) with deficit-based fairness across priorities.
      - Keep implementation simple: no preemption required (works with only pool headroom signals).
    """
    # Per-priority FIFO queues (pipelines rotate through these queues).
    s.queues = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }

    # Active pipelines by id (used for lazy cleanup and to avoid duplicate inserts).
    s.active = {}

    # Deficit round-robin state to prevent starvation.
    s.quantum = {
        Priority.QUERY: 10,
        Priority.INTERACTIVE: 5,
        Priority.BATCH_PIPELINE: 1,
    }
    s.deficit = {
        Priority.QUERY: 0,
        Priority.INTERACTIVE: 0,
        Priority.BATCH_PIPELINE: 0,
    }

    # Resource estimates per operator (keyed by (pipeline_id, op_identifier)).
    # RAM estimates are monotonic non-decreasing (we prefer completion + no OOM over aggressive packing).
    s.op_ram_est = {}        # op_key -> ram
    s.op_failures = {}       # op_key -> count
    s.op_to_pipeline = {}    # id(op_obj) -> pipeline_id (used to recover pipeline_id from ExecutionResult)

    # Non-OOM failures are treated as "likely fatal" after a small number of retries to avoid thrashing.
    s.max_retries_oom = 6
    s.max_retries_other = 1
    s.pipeline_fatal = set()  # pipeline_id that should no longer be scheduled

    # Limit within-tick concurrency per pipeline by priority (reduces head-of-line blocking + improves fairness).
    s.max_parallel = {
        Priority.QUERY: 4,
        Priority.INTERACTIVE: 2,
        Priority.BATCH_PIPELINE: 1,
    }


@register_scheduler(key="scheduler_high_049")
def scheduler_high_049_scheduler(s, results, pipelines):
    """
    Scheduler step:
      1) Enqueue new pipelines into per-priority queues.
      2) Update RAM estimates from failures (especially OOM) and mark pipelines fatal for non-OOM errors.
      3) Deficit round-robin across priorities; fill each pool with runnable operators.
      4) Prefer keeping batch off pool0 when multiple pools exist (protects query/interactive latency).
    """
    # ---- helpers (defined inside to avoid imports at module scope) ----
    def _is_oom_error(err):
        if err is None:
            return False
        e = str(err).lower()
        return ("oom" in e) or ("out of memory" in e) or ("memory" in e and "alloc" in e)

    def _op_identifier(op):
        # Try common fields first; fallback to object identity.
        for attr in ("op_id", "operator_id", "id", "name", "uid"):
            v = getattr(op, attr, None)
            if v is not None:
                return v
        return id(op)

    def _op_key(pipeline_id, op):
        return (pipeline_id, _op_identifier(op))

    def _largest_pool_max_ram():
        m = 0
        for pool in s.executor.pools:
            mr = getattr(pool, "max_ram_pool", 0) or 0
            if mr > m:
                m = mr
        return m

    def _largest_pool_max_cpu():
        m = 0
        for pool in s.executor.pools:
            mc = getattr(pool, "max_cpu_pool", 0) or 0
            if mc > m:
                m = mc
        return m

    def _priority_order_for_pool(pool_id):
        # Keep batch away from pool0 when possible (tail-latency protection for high priority).
        if s.executor.num_pools > 1 and pool_id == 0:
            return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]
        # Other pools: still prioritize QUERY/INTERACTIVE, but allow batch to make steady progress.
        return [Priority.INTERACTIVE, Priority.QUERY, Priority.BATCH_PIPELINE]

    def _default_ram_fraction(priority):
        # Conservative initial RAM to reduce OOM churn (OOM -> failure + re-run).
        if priority == Priority.QUERY:
            return 0.40
        if priority == Priority.INTERACTIVE:
            return 0.30
        return 0.20  # batch

    def _default_cpu_fraction(priority):
        # Favor faster completion for high priority without always monopolizing the whole pool.
        if priority == Priority.QUERY:
            return 0.60
        if priority == Priority.INTERACTIVE:
            return 0.50
        return 0.40  # batch

    def _desired_resources(priority, pool, avail_cpu, avail_ram, pipeline_id, op):
        # Compute desired CPU and RAM for an operator on this pool.
        # Policy: RAM is "safety first" (monotonic estimate), CPU is "latency first" for high priority.
        max_cpu = getattr(pool, "max_cpu_pool", 0) or 0
        max_ram = getattr(pool, "max_ram_pool", 0) or 0

        if avail_cpu <= 0 or avail_ram <= 0 or max_cpu <= 0 or max_ram <= 0:
            return None, None

        key = _op_key(pipeline_id, op)

        # RAM: use learned estimate if present; otherwise conservative fraction of pool max.
        est_ram = s.op_ram_est.get(key, None)
        if est_ram is None:
            est_ram = max_ram * _default_ram_fraction(priority)

        # If this op previously failed due to OOM, require more headroom.
        fail_cnt = s.op_failures.get(key, 0)
        if fail_cnt > 0:
            # Mild inflation to avoid repeated near-miss OOMs.
            est_ram = max(est_ram, est_ram * (1.0 + 0.15 * min(fail_cnt, 4)))

        # Bound estimate by global max RAM across pools (some pools may be smaller).
        global_ram_cap = _largest_pool_max_ram()
        if global_ram_cap > 0:
            est_ram = min(est_ram, global_ram_cap)

        # We refuse to schedule if we cannot meet the desired RAM, to avoid OOM churn.
        desired_ram = est_ram
        if desired_ram <= 0:
            desired_ram = min(avail_ram, max_ram * 0.20)
        if desired_ram > avail_ram:
            return None, None

        # CPU: choose a target fraction of pool max; never below 1 if available.
        target_cpu = max(1.0, max_cpu * _default_cpu_fraction(priority))
        desired_cpu = min(avail_cpu, target_cpu)
        if desired_cpu < 1.0 and avail_cpu >= 1.0:
            desired_cpu = 1.0

        if desired_cpu <= 0:
            return None, None

        return desired_cpu, desired_ram

    def _pipeline_inflight_count(status):
        # Count assigned + running (best-effort, depends on status structure).
        sc = getattr(status, "state_counts", {}) or {}
        return (sc.get(OperatorState.RUNNING, 0) or 0) + (sc.get(OperatorState.ASSIGNED, 0) or 0)

    def _try_pop_runnable(pqueue, priority, pool_id, avail_cpu, avail_ram, scheduled_in_tick):
        # Rotate through the queue at most once to find a runnable pipeline/op that fits.
        if not pqueue:
            return None

        n = len(pqueue)
        for _ in range(n):
            p = pqueue.pop(0)
            pid = getattr(p, "pipeline_id", None)

            # Drop from scheduling set if it is no longer active.
            if pid is None or pid not in s.active:
                continue
            if pid in s.pipeline_fatal:
                continue

            status = p.runtime_status()
            if status.is_pipeline_successful():
                # Cleanup.
                s.active.pop(pid, None)
                continue

            # Concurrency cap per pipeline per priority.
            inflight = _pipeline_inflight_count(status)
            inflight += scheduled_in_tick.get(pid, 0)
            if inflight >= s.max_parallel.get(priority, 1):
                # Keep it in rotation.
                pqueue.append(p)
                continue

            # Find next runnable operator (parents complete).
            op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
            if not op_list:
                pqueue.append(p)
                continue

            op = op_list[0]

            # Compute desired resources; if it doesn't fit, keep rotating.
            pool = s.executor.pools[pool_id]
            cpu_req, ram_req = _desired_resources(priority, pool, avail_cpu, avail_ram, pid, op)
            if cpu_req is None or ram_req is None:
                pqueue.append(p)
                continue

            # Record op->pipeline mapping for later result attribution.
            s.op_to_pipeline[id(op)] = pid

            # Put pipeline back at end (round-robin across pipelines).
            pqueue.append(p)

            return Assignment(
                ops=[op],
                cpu=cpu_req,
                ram=ram_req,
                priority=priority,
                pool_id=pool_id,
                pipeline_id=pid,
            )

        return None

    # ---- ingest new pipelines ----
    for p in pipelines:
        pid = getattr(p, "pipeline_id", None)
        if pid is None:
            continue
        if pid in s.active:
            continue
        s.active[pid] = p
        s.queues[p.priority].append(p)

    # ---- process results (update RAM estimates + fatal pipeline marking) ----
    if results:
        global_ram_cap = _largest_pool_max_ram()
        for r in results:
            if r is None:
                continue
            ops = getattr(r, "ops", None) or []
            failed = r.failed()

            for op in ops:
                pid = getattr(op, "pipeline_id", None)
                if pid is None:
                    pid = s.op_to_pipeline.get(id(op), None)

                if pid is None:
                    # Can't attribute; still can track by object id but it won't help much across steps.
                    continue

                key = _op_key(pid, op)

                if failed:
                    s.op_failures[key] = s.op_failures.get(key, 0) + 1

                    if _is_oom_error(getattr(r, "error", None)):
                        # Exponential-ish growth: double observed RAM, also ensure growth vs previous estimate.
                        prev = s.op_ram_est.get(key, 0) or 0
                        obs = getattr(r, "ram", None) or 0
                        # If obs is missing/0, still ensure some growth to avoid retrying with same size.
                        candidate = max(prev * 1.5, obs * 2.0, prev + max(1.0, 0.05 * global_ram_cap) if global_ram_cap else prev + 1.0)
                        if global_ram_cap:
                            candidate = min(candidate, global_ram_cap)
                        s.op_ram_est[key] = max(prev, candidate)
                    else:
                        # Likely deterministic failure; avoid burning cycles retrying.
                        if s.op_failures[key] >= s.max_retries_other:
                            s.pipeline_fatal.add(pid)
                else:
                    # Success: keep monotonic RAM estimates (do not shrink) to avoid regressions.
                    obs = getattr(r, "ram", None) or 0
                    if obs > 0 and key not in s.op_ram_est:
                        s.op_ram_est[key] = obs

    # ---- early exit if nothing changed that could affect decisions ----
    if not pipelines and not results:
        return [], []

    # ---- update deficit (fairness across priorities) ----
    for pr, q in s.quantum.items():
        s.deficit[pr] += q

    suspensions = []
    assignments = []
    scheduled_in_tick = {}  # pipeline_id -> num assignments made in this scheduler call

    # If multiple pools exist, strongly protect pool0 from batch when high-priority work is queued.
    high_backlog = (len(s.queues[Priority.QUERY]) + len(s.queues[Priority.INTERACTIVE])) > 0

    # ---- schedule per pool ----
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = getattr(pool, "avail_cpu_pool", 0) or 0
        avail_ram = getattr(pool, "avail_ram_pool", 0) or 0

        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        prio_order = _priority_order_for_pool(pool_id)

        # For pool0 with multiple pools, avoid batch unless no high-priority backlog.
        if s.executor.num_pools > 1 and pool_id == 0 and high_backlog:
            prio_order = [Priority.QUERY, Priority.INTERACTIVE]  # omit batch

        # Greedily fill the pool with assignments while resources remain.
        # To avoid infinite loops, cap the number of successful assignments per pool per tick.
        max_assignments_this_pool = 8
        made = 0

        while avail_cpu > 0 and avail_ram > 0 and made < max_assignments_this_pool:
            chosen = None

            # Choose next priority by deficit first; if no deficit, still try high priority (for latency).
            for pr in prio_order:
                if s.deficit.get(pr, 0) <= 0:
                    continue
                if not s.queues[pr]:
                    continue
                chosen = pr
                break

            if chosen is None:
                # No priority has deficit or queues empty; try in strict priority order (latency-first).
                for pr in prio_order:
                    if s.queues[pr]:
                        chosen = pr
                        break

            if chosen is None:
                break

            a = _try_pop_runnable(
                s.queues[chosen],
                chosen,
                pool_id,
                avail_cpu,
                avail_ram,
                scheduled_in_tick,
            )

            if a is None:
                # Can't place anything of the chosen priority right now; to prevent stalling,
                # try other priorities in order for this pool.
                placed = False
                for pr in prio_order:
                    if pr == chosen or not s.queues[pr]:
                        continue
                    a = _try_pop_runnable(
                        s.queues[pr],
                        pr,
                        pool_id,
                        avail_cpu,
                        avail_ram,
                        scheduled_in_tick,
                    )
                    if a is not None:
                        chosen = pr
                        placed = True
                        break
                if not placed:
                    break

            # Commit assignment.
            assignments.append(a)
            made += 1
            s.deficit[chosen] = max(0, s.deficit.get(chosen, 0) - 1)

            # Track within-tick pipeline parallelism and update remaining resources pessimistically.
            pid = a.pipeline_id
            scheduled_in_tick[pid] = scheduled_in_tick.get(pid, 0) + 1
            avail_cpu -= a.cpu
            avail_ram -= a.ram

    # Opportunistic cleanup of completed pipelines (keeps memory stable).
    # Limit work by only scanning a bounded number each tick.
    scan_cap = 64
    scanned = 0
    for pid, p in list(s.active.items()):
        if scanned >= scan_cap:
            break
        scanned += 1
        st = p.runtime_status()
        if st.is_pipeline_successful():
            s.active.pop(pid, None)
        elif pid in s.pipeline_fatal:
            # If it has failed ops and we decided it is fatal, stop scheduling it.
            sc = getattr(st, "state_counts", {}) or {}
            if (sc.get(OperatorState.FAILED, 0) or 0) > 0:
                s.active.pop(pid, None)

    return suspensions, assignments
