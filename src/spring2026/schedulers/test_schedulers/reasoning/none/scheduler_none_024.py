# policy_key: scheduler_none_024
# reasoning_effort: none
# model: gpt-5.2-2025-12-11
# llm_cost: 0.064775
# generation_seconds: 56.42
# generated_at: 2026-04-09T22:03:47.745180
@register_scheduler_init(key="scheduler_none_024")
def scheduler_none_024_init(s):
    """Priority-protecting, OOM-avoiding, aging-aware scheduler.

    Goals:
      - Minimize weighted latency (query/interact dominate) by prioritizing high-priority ready ops.
      - Avoid failures (720s penalty) by learning per-operator RAM from OOMs and retrying safely.
      - Avoid starvation by aging waiting pipelines, allowing batch to make progress under load.
      - Reduce head-of-line blocking by selecting "ready" operators (parents complete) and packing
        with small, safe slices rather than allocating entire pools to a single op.

    Key ideas:
      1) Maintain per-operator RAM estimate with exponential backoff on OOM.
      2) Per-pool reservations for query/interactive headroom; batch uses leftover.
      3) Greedy assignment loop per pool: schedule multiple ops while resources remain.
      4) Aging boosts lower-priority pipelines that have waited long enough.
    """
    s.waiting = []  # pipelines known to the scheduler (arrived but not necessarily complete)
    s.known = {}  # pipeline_id -> Pipeline (latest object reference)
    s.arrival_ts = {}  # pipeline_id -> first time seen (scheduler ticks)
    s.tick = 0

    # Memory learning: op_key -> estimated RAM needed (sufficient to avoid OOM)
    s.op_ram_est = {}

    # Track recent OOM events to accelerate ramp-up: (pipeline_id, op_key) -> count
    s.op_oom_count = {}

    # Soft reservations per pool for high priority (fractions of pool capacity).
    # We keep these modest to avoid starving batch; unused reservation is still usable
    # if there are no high-priority candidates ready.
    s.reserve_query_frac = 0.25
    s.reserve_interactive_frac = 0.15

    # CPU slicing: give at most this fraction of pool CPU per op to allow concurrency.
    # Also cap by a small constant so we don't oversubscribe a single op.
    s.max_cpu_frac_per_op = 0.50
    s.max_cpu_per_op_cap = 16.0

    # RAM slack multiplier: allocate a bit above estimate to reduce borderline OOM risk.
    s.ram_slack = 1.10

    # If a pipeline has waited this many ticks, apply aging to avoid starvation.
    s.aging_start_ticks = 25
    s.aging_gain_per_tick = 0.02  # small, steady boost

    # Preemption: keep conservative to avoid wasted work; only preempt when query arrives
    # and there is essentially no headroom.
    s.enable_preemption = True
    s.preempt_min_query_headroom_frac = 0.10  # if below, consider preemption
    s.preempt_only_batch = True  # only preempt batch containers


@register_scheduler(key="scheduler_none_024")
def scheduler_none_024(s, results, pipelines):
    """
    Scheduler step:
      - Incorporate new pipelines.
      - Update RAM estimates on OOM failures.
      - Optionally preempt batch when queries arrive and pools are tight.
      - Assign ready operators greedily per pool, honoring priority and aging, and avoiding OOM.
    """
    # Local helpers defined inside to avoid imports at module scope.
    def _priority_weight(pri):
        if pri == Priority.QUERY:
            return 10.0
        if pri == Priority.INTERACTIVE:
            return 5.0
        return 1.0

    def _base_rank(pri):
        # Higher is better.
        if pri == Priority.QUERY:
            return 300.0
        if pri == Priority.INTERACTIVE:
            return 200.0
        return 100.0

    def _safe_float(x, default=0.0):
        try:
            return float(x)
        except Exception:
            return default

    def _now():
        return s.tick

    def _pipeline_age(p):
        t0 = s.arrival_ts.get(p.pipeline_id, _now())
        return max(0, _now() - t0)

    def _aging_bonus(p):
        age = _pipeline_age(p)
        if age <= s.aging_start_ticks:
            return 0.0
        return (age - s.aging_start_ticks) * s.aging_gain_per_tick * _priority_weight(p.priority)

    def _op_key(pipeline, op):
        # Best-effort stable key: include pipeline id + op "name/id" where possible.
        # Fallback to repr(op) for uniqueness; simulator objects are deterministic.
        name = getattr(op, "op_id", None) or getattr(op, "operator_id", None) or getattr(op, "name", None)
        if name is None:
            name = repr(op)
        return f"{pipeline.pipeline_id}:{name}"

    def _pick_ready_ops(p):
        status = p.runtime_status()
        if status.is_pipeline_successful():
            return []
        # Try assignable states; require parents complete.
        ops = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        if not ops:
            return []
        # Prefer a small number of ops per pipeline per tick to reduce per-pipeline fanout.
        return ops[:2]

    def _estimate_ram_for_op(p, op, pool_avail_ram):
        key = _op_key(p, op)
        est = s.op_ram_est.get(key, None)

        # If we've never seen it, start with a conservative small slice of pool RAM.
        # Using too small risks OOM; too large blocks concurrency. We'll learn via OOM feedback.
        if est is None:
            est = max(0.5, pool_avail_ram * 0.20)

        # Apply OOM backoff if repeated failures.
        oom_n = s.op_oom_count.get((p.pipeline_id, key), 0)
        if oom_n > 0:
            # Exponential backoff, capped by pool capacity.
            est = est * (1.6 ** oom_n)

        # Add slack to reduce borderline OOM.
        est = est * s.ram_slack
        # Clamp to available ram and a minimum positive allocation.
        est = max(0.1, min(est, pool_avail_ram))
        return est

    def _choose_cpu_for_op(p, pool_avail_cpu, pool_max_cpu):
        # For latency, queries benefit from more CPU, but scaling is sublinear; avoid monopolizing.
        if pool_avail_cpu <= 0:
            return 0.0

        pri = p.priority
        if pri == Priority.QUERY:
            target = min(pool_avail_cpu, min(s.max_cpu_per_op_cap, max(2.0, pool_max_cpu * 0.35)))
        elif pri == Priority.INTERACTIVE:
            target = min(pool_avail_cpu, min(s.max_cpu_per_op_cap, max(1.0, pool_max_cpu * 0.25)))
        else:
            target = min(pool_avail_cpu, min(4.0, max(1.0, pool_max_cpu * 0.15)))

        # Also cap by fraction per op to allow concurrency.
        target = min(target, pool_avail_cpu * s.max_cpu_frac_per_op)
        return max(0.1, target)

    def _pool_reserved(pool, pri):
        # Compute reserved CPU/RAM for higher-priority work that we try to preserve.
        max_cpu = _safe_float(pool.max_cpu_pool, 0.0)
        max_ram = _safe_float(pool.max_ram_pool, 0.0)
        if pri == Priority.BATCH_PIPELINE:
            # For batch, reserve for query+interactive.
            res_cpu = max_cpu * (s.reserve_query_frac + s.reserve_interactive_frac)
            res_ram = max_ram * (s.reserve_query_frac + s.reserve_interactive_frac)
            return res_cpu, res_ram
        if pri == Priority.INTERACTIVE:
            # For interactive, reserve for query.
            res_cpu = max_cpu * s.reserve_query_frac
            res_ram = max_ram * s.reserve_query_frac
            return res_cpu, res_ram
        # Query: no reservation needed.
        return 0.0, 0.0

    def _candidate_score(p):
        # Higher score -> earlier scheduling.
        return _base_rank(p.priority) + _aging_bonus(p)

    def _is_done_or_failed_terminal(p):
        status = p.runtime_status()
        if status.is_pipeline_successful():
            return True
        # If there are failed ops, do NOT drop the pipeline; allow retry because failures are penalized.
        # However, if there is some non-OOM fatal error, we may still see failed() results. We'll keep retrying
        # with backoff RAM for safety.
        return False

    def _update_from_results(exec_results):
        # Update RAM estimates on OOM and success signals.
        for r in exec_results:
            # Some simulators store error strings; treat any failed() with "OOM" substring as OOM.
            is_fail = False
            try:
                is_fail = r.failed()
            except Exception:
                is_fail = bool(getattr(r, "error", None))

            if not r.ops:
                continue

            # Derive pipeline and op keys for all ops in the assignment.
            # We update all ops in the bundle similarly; in most settings ops list length is small.
            pid = getattr(r, "pipeline_id", None)
            # pipeline_id isn't documented on ExecutionResult; fall back to using op repr for keying.
            for op in r.ops:
                # Try to locate pipeline object: if pid not present, we won't be able to namespace.
                if pid is None:
                    # Best-effort: use "unknown" pipeline; still learn global-ish by op repr.
                    fake_pipeline_id = "unknown"
                else:
                    fake_pipeline_id = pid

                name = getattr(op, "op_id", None) or getattr(op, "operator_id", None) or getattr(op, "name", None)
                if name is None:
                    name = repr(op)
                key = f"{fake_pipeline_id}:{name}"

                if is_fail:
                    err = str(getattr(r, "error", "") or "")
                    if "OOM" in err.upper() or "OUT OF MEMORY" in err.upper():
                        # Record OOM; increase estimate beyond attempted RAM.
                        prev = s.op_ram_est.get(key, _safe_float(getattr(r, "ram", 0.0), 0.0))
                        attempted = _safe_float(getattr(r, "ram", 0.0), prev)
                        new_est = max(prev, attempted) * 1.5
                        s.op_ram_est[key] = new_est
                        s.op_oom_count[(fake_pipeline_id, key)] = s.op_oom_count.get((fake_pipeline_id, key), 0) + 1
                    else:
                        # Non-OOM failure: still cautiously bump RAM a bit (might be hidden memory peak),
                        # but don't explode the estimate.
                        prev = s.op_ram_est.get(key, _safe_float(getattr(r, "ram", 0.0), 0.0))
                        attempted = _safe_float(getattr(r, "ram", 0.0), prev)
                        s.op_ram_est[key] = max(prev, attempted) * 1.10
                else:
                    # Success: decay OOM counter and keep a stable estimate at/above observed RAM.
                    used = _safe_float(getattr(r, "ram", 0.0), 0.0)
                    prev = s.op_ram_est.get(key, used)
                    s.op_ram_est[key] = max(prev * 0.95, used)  # gentle decay
                    if (fake_pipeline_id, key) in s.op_oom_count:
                        s.op_oom_count[(fake_pipeline_id, key)] = max(0, s.op_oom_count[(fake_pipeline_id, key)] - 1)

    def _collect_running_containers():
        # Best-effort: different simulators expose running containers differently.
        # We return a list of dict-like objects with at least: container_id, pool_id, priority.
        containers = []
        for pool_id in range(s.executor.num_pools):
            pool = s.executor.pools[pool_id]
            # Try common attributes.
            for attr in ("containers", "running", "active_containers"):
                if hasattr(pool, attr):
                    try:
                        for c in getattr(pool, attr):
                            containers.append(c)
                    except Exception:
                        pass
        return containers

    # Tick advancement / state updates
    s.tick += 1

    # Add/refresh pipelines
    for p in pipelines:
        s.known[p.pipeline_id] = p
        if p.pipeline_id not in s.arrival_ts:
            s.arrival_ts[p.pipeline_id] = _now()
        s.waiting.append(p)

    # Update learning from execution results
    if results:
        _update_from_results(results)

    # Prune waiting list of completed pipelines (keep incomplete, including failed ops, to retry)
    new_waiting = []
    for p in s.waiting:
        if p is None:
            continue
        # Refresh reference if we have a newer object for same pipeline_id
        p2 = s.known.get(p.pipeline_id, p)
        if _is_done_or_failed_terminal(p2):
            continue
        new_waiting.append(p2)
    s.waiting = new_waiting

    # Preemption logic (conservative):
    # If new high-priority pipelines arrived and pools are tight, suspend some batch containers
    # to create headroom. We do NOT attempt perfect accounting; just a small nudge.
    suspensions = []
    if s.enable_preemption and pipelines:
        new_has_query = any(p.priority == Priority.QUERY for p in pipelines)
        if new_has_query:
            for pool_id in range(s.executor.num_pools):
                pool = s.executor.pools[pool_id]
                avail_cpu = _safe_float(pool.avail_cpu_pool, 0.0)
                avail_ram = _safe_float(pool.avail_ram_pool, 0.0)
                max_cpu = _safe_float(pool.max_cpu_pool, 0.0)
                max_ram = _safe_float(pool.max_ram_pool, 0.0)

                # If headroom is very low, consider preempting 1 batch container in this pool.
                if max_cpu <= 0 or max_ram <= 0:
                    continue
                if (avail_cpu / max_cpu) > s.preempt_min_query_headroom_frac and (avail_ram / max_ram) > s.preempt_min_query_headroom_frac:
                    continue

                # Try to find a batch container to suspend from recent results first (more reliable IDs).
                batch_candidates = []
                for r in results:
                    if getattr(r, "pool_id", None) != pool_id:
                        continue
                    if getattr(r, "container_id", None) is None:
                        continue
                    if getattr(r, "priority", None) == Priority.BATCH_PIPELINE:
                        batch_candidates.append(r.container_id)

                # If none found, attempt via pool container listings.
                if not batch_candidates:
                    for c in _collect_running_containers():
                        try:
                            c_pool = getattr(c, "pool_id", None)
                            c_pri = getattr(c, "priority", None)
                            c_id = getattr(c, "container_id", None) or getattr(c, "id", None)
                            if c_pool == pool_id and c_id is not None and c_pri == Priority.BATCH_PIPELINE:
                                batch_candidates.append(c_id)
                        except Exception:
                            continue

                if batch_candidates:
                    # Suspend just one to reduce churn.
                    suspensions.append(Suspend(batch_candidates[0], pool_id))

    # If no new pipelines and no results and no suspensions, early exit.
    if not pipelines and not results and not suspensions:
        return [], []

    # Build candidate list of pipelines sorted by score (high first).
    # Keep stability by adding pipeline_id as tie-breaker.
    candidates = []
    for p in s.waiting:
        candidates.append((-_candidate_score(p), str(p.pipeline_id), p))
    candidates.sort()

    assignments = []

    # Greedy packing per pool
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = _safe_float(pool.avail_cpu_pool, 0.0)
        avail_ram = _safe_float(pool.avail_ram_pool, 0.0)
        max_cpu = _safe_float(pool.max_cpu_pool, 0.0)

        if avail_cpu <= 0.0 or avail_ram <= 0.0:
            continue

        # We'll do multiple passes: prefer query, then interactive, then batch (with aging).
        # But allow opportunistic use of reserved capacity if no higher-priority work is ready.
        made_progress = True
        pass_count = 0
        while made_progress and avail_cpu > 0.0 and avail_ram > 0.0 and pass_count < 50:
            made_progress = False
            pass_count += 1

            best = None
            best_ops = None
            best_cpu = 0.0
            best_ram = 0.0

            # Determine if higher-priority work exists that is ready (for reservation logic).
            ready_has_query = False
            ready_has_interactive = False
            for _, _, p in candidates:
                ops = _pick_ready_ops(p)
                if not ops:
                    continue
                if p.priority == Priority.QUERY:
                    ready_has_query = True
                    break
                if p.priority == Priority.INTERACTIVE:
                    ready_has_interactive = True

            for _, _, p in candidates:
                ops = _pick_ready_ops(p)
                if not ops:
                    continue

                # Reservation: if scheduling lower priority, keep some headroom if higher priority exists ready.
                res_cpu, res_ram = _pool_reserved(pool, p.priority)
                eff_avail_cpu = avail_cpu
                eff_avail_ram = avail_ram

                if p.priority == Priority.BATCH_PIPELINE:
                    if ready_has_query or ready_has_interactive:
                        eff_avail_cpu = max(0.0, avail_cpu - res_cpu)
                        eff_avail_ram = max(0.0, avail_ram - res_ram)
                elif p.priority == Priority.INTERACTIVE:
                    if ready_has_query:
                        eff_avail_cpu = max(0.0, avail_cpu - res_cpu)
                        eff_avail_ram = max(0.0, avail_ram - res_ram)

                if eff_avail_cpu <= 0.0 or eff_avail_ram <= 0.0:
                    continue

                # Choose CPU slice and RAM estimate for the first op only (bundle small).
                cpu = _choose_cpu_for_op(p, eff_avail_cpu, max_cpu)
                if cpu <= 0.0:
                    continue

                # RAM: choose based on the "largest" estimated among the selected ops to avoid OOM.
                # Keep op bundle small; allocate to cover the worst.
                ram_needed = 0.0
                for op in ops[:1]:
                    ram_needed = max(ram_needed, _estimate_ram_for_op(p, op, eff_avail_ram))
                ram = max(0.1, min(ram_needed, eff_avail_ram))

                # Ensure both cpu/ram fit.
                if ram <= 0.0 or cpu <= 0.0 or ram > eff_avail_ram + 1e-9 or cpu > eff_avail_cpu + 1e-9:
                    continue

                # Score: prioritize by pipeline score; mild preference for smaller RAM to pack and reduce blocking.
                score = _candidate_score(p) - 0.05 * ram
                if best is None or score > best:
                    best = score
                    best_ops = ops[:1]
                    best_cpu = cpu
                    best_ram = ram
                    best_p = p

            if best_ops:
                assignments.append(
                    Assignment(
                        ops=best_ops,
                        cpu=best_cpu,
                        ram=best_ram,
                        priority=best_p.priority,
                        pool_id=pool_id,
                        pipeline_id=best_p.pipeline_id,
                    )
                )
                avail_cpu -= best_cpu
                avail_ram -= best_ram
                made_progress = True

    return suspensions, assignments
