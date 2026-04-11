# policy_key: scheduler_est_006
# reasoning_effort: low
# exp: estimation
# model: gpt-5.2-2025-12-11
# llm_cost: 0.047446
# generation_seconds: 60.90
# generated_at: 2026-04-10T07:51:02.037494
@register_scheduler_init(key="scheduler_est_006")
def scheduler_est_006_init(s):
    """Memory-aware, priority-protecting scheduler.

    Main ideas:
      - Maintain separate FIFO queues per priority (query > interactive > batch).
      - Use op.estimate.mem_peak_gb as a hint to pick a feasible pool and right-size RAM.
      - On failures (esp. OOM-like), retry the same operator with larger RAM (bounded, capped retries).
      - Protect high-priority latency by throttling batch when pools are tight (soft reservation).
      - Avoid starvation via simple aging: periodically allow batch dispatch even under pressure.
    """
    # Per-priority waiting queues of pipelines (FIFO within each class)
    s.q_query = []
    s.q_interactive = []
    s.q_batch = []

    # Track pipeline metadata and retry hints
    s._known_pipelines = set()          # pipeline_id seen
    s._op_ram_hint = {}                # id(op) -> last requested RAM (GB)
    s._op_cpu_hint = {}                # id(op) -> last requested CPU
    s._op_retry_count = {}             # id(op) -> retries so far

    # Simple fairness/anti-starvation knobs
    s._tick = 0
    s._hi_served_streak = 0            # count consecutive high-priority dispatches
    s._max_hi_streak = 6               # after this many hi dispatches, try batch if feasible

    # Batch throttling (soft reservations for high priority)
    s._reserve_ram_frac_for_hi = 0.20  # keep ~20% RAM headroom when scheduling batch
    s._reserve_cpu_frac_for_hi = 0.20  # keep ~20% CPU headroom when scheduling batch

    # Sizing knobs
    s._ram_safety_mult = 1.25          # inflate estimate to reduce OOM risk
    s._ram_min_default = {             # conservative minimums when estimate is missing
        Priority.QUERY: 2.0,
        Priority.INTERACTIVE: 2.0,
        Priority.BATCH_PIPELINE: 3.0,
    }
    s._cpu_default = {
        Priority.QUERY: 1.0,
        Priority.INTERACTIVE: 1.0,
        Priority.BATCH_PIPELINE: 2.0,
    }
    s._cpu_cap_frac_of_pool = {
        Priority.QUERY: 0.50,          # avoid taking the whole pool for one op
        Priority.INTERACTIVE: 0.60,
        Priority.BATCH_PIPELINE: 0.80,
    }

    # Retry policy
    s._max_retries_per_op = 5
    s._retry_ram_mult = 1.6            # bump RAM on each failure
    s._retry_cpu_mult = 1.2            # slight CPU bump on repeated failures


@register_scheduler(key="scheduler_est_006")
def scheduler_est_006_scheduler(s, results, pipelines):
    def _enqueue_pipeline(p):
        # Avoid duplicate enqueues in the same tick if generator repeats references
        if p.pipeline_id not in s._known_pipelines:
            s._known_pipelines.add(p.pipeline_id)
        if p.priority == Priority.QUERY:
            s.q_query.append(p)
        elif p.priority == Priority.INTERACTIVE:
            s.q_interactive.append(p)
        else:
            s.q_batch.append(p)

    def _pipeline_done_or_failed(p):
        st = p.runtime_status()
        if st.is_pipeline_successful():
            return True
        # If any ops are FAILED, we still keep pipeline around because we can retry.
        # But if the runtime treats FAILED as terminal, it will show up as "has_failures"
        # and we should stop re-enqueuing to avoid infinite churn. We interpret FAILED as retryable
        # only via ASSIGNABLE_STATES (which includes FAILED in this environment).
        return False

    def _get_next_assignable_op(p):
        st = p.runtime_status()
        ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        if not ops:
            return None
        return ops[0]

    def _op_est_mem(op):
        est = None
        try:
            if getattr(op, "estimate", None) is not None:
                est = getattr(op.estimate, "mem_peak_gb", None)
        except Exception:
            est = None
        if est is None:
            return None
        # Guard against weird estimator outputs
        try:
            est = float(est)
        except Exception:
            return None
        if est < 0:
            return None
        return est

    def _choose_pool_for_op(op, priority):
        # Choose the "best-fit feasible" pool by estimated memory, falling back to most RAM available.
        est_mem = _op_est_mem(op)

        feasible = []
        for pool_id in range(s.executor.num_pools):
            pool = s.executor.pools[pool_id]
            if pool.avail_cpu_pool <= 0 or pool.avail_ram_pool <= 0:
                continue

            # If we have an estimate, require it to fit in currently available RAM (with safety).
            # If no estimate, allow all pools.
            if est_mem is not None:
                need = est_mem * s._ram_safety_mult
                # If it cannot fit now, skip this pool to reduce OOM/queue churn.
                if need > pool.avail_ram_pool:
                    continue
            feasible.append(pool_id)

        if feasible:
            # Best-fit: pick pool with smallest avail RAM that still fits (reduces fragmentation).
            if est_mem is not None:
                need = est_mem * s._ram_safety_mult
                best = None
                best_slack = None
                for pid in feasible:
                    pool = s.executor.pools[pid]
                    slack = pool.avail_ram_pool - need
                    if slack < 0:
                        continue
                    if best is None or slack < best_slack:
                        best = pid
                        best_slack = slack
                if best is not None:
                    return best
            # Fallback: most available RAM (helps with unknown/noisy estimates)
            return max(feasible, key=lambda pid: s.executor.pools[pid].avail_ram_pool)

        # If no feasible pool under estimate, allow a fallback placement attempt only if estimate is missing.
        if est_mem is None:
            best = None
            best_score = None
            for pool_id in range(s.executor.num_pools):
                pool = s.executor.pools[pool_id]
                if pool.avail_cpu_pool <= 0 or pool.avail_ram_pool <= 0:
                    continue
                score = (pool.avail_ram_pool, pool.avail_cpu_pool)
                if best is None or score > best_score:
                    best = pool_id
                    best_score = score
            return best

        return None

    def _size_resources(op, priority, pool_id):
        pool = s.executor.pools[pool_id]
        op_key = id(op)

        # RAM sizing: prioritize safety (OOMs are heavily penalized by 720s failures).
        est_mem = _op_est_mem(op)
        base_min = s._ram_min_default.get(priority, 2.0)

        ram_hint = s._op_ram_hint.get(op_key, None)
        if ram_hint is not None and ram_hint > 0:
            target_ram = ram_hint
        else:
            if est_mem is not None:
                target_ram = max(base_min, est_mem * s._ram_safety_mult)
            else:
                # No estimate: choose a conservative default but avoid hogging.
                target_ram = max(base_min, min(8.0, pool.avail_ram_pool))

        # Cap to pool availability
        target_ram = min(target_ram, pool.avail_ram_pool)

        # CPU sizing: give enough to reduce tail latency for queries but don't monopolize.
        cpu_hint = s._op_cpu_hint.get(op_key, None)
        if cpu_hint is not None and cpu_hint > 0:
            target_cpu = cpu_hint
        else:
            target_cpu = s._cpu_default.get(priority, 1.0)

        # Cap per priority and to current availability
        cap_frac = s._cpu_cap_frac_of_pool.get(priority, 0.6)
        cpu_cap = max(1.0, pool.max_cpu_pool * cap_frac)
        target_cpu = min(target_cpu, cpu_cap, pool.avail_cpu_pool)

        # Ensure non-zero allocation (executor expects >0)
        if target_cpu <= 0:
            return None, None
        if target_ram <= 0:
            return None, None

        return float(target_cpu), float(target_ram)

    def _batch_allowed_in_pool(pool):
        # Soft reservation: if pool is tight, avoid starting batch work that might delay queries/interactive.
        reserve_ram = pool.max_ram_pool * s._reserve_ram_frac_for_hi
        reserve_cpu = pool.max_cpu_pool * s._reserve_cpu_frac_for_hi
        return pool.avail_ram_pool > reserve_ram and pool.avail_cpu_pool > reserve_cpu

    def _pop_next_pipeline_for_pool(pool_id):
        # Priority order with simple anti-starvation:
        # - Prefer query, then interactive.
        # - After a streak of high-priority dispatches, attempt one batch dispatch if feasible.
        pool = s.executor.pools[pool_id]

        # If we have many batch waiting and we've served many hi-priority ops consecutively, try batch.
        if s._hi_served_streak >= s._max_hi_streak and s.q_batch and _batch_allowed_in_pool(pool):
            s._hi_served_streak = 0
            return s.q_batch.pop(0)

        if s.q_query:
            s._hi_served_streak += 1
            return s.q_query.pop(0)
        if s.q_interactive:
            s._hi_served_streak += 1
            return s.q_interactive.pop(0)

        # For batch, throttle if pool is tight
        if s.q_batch and _batch_allowed_in_pool(pool):
            s._hi_served_streak = 0
            return s.q_batch.pop(0)

        return None

    # --- ingest new pipelines ---
    for p in pipelines:
        _enqueue_pipeline(p)

    # If nothing changed, exit quickly
    if not pipelines and not results:
        s._tick += 1
        return [], []

    # --- handle results: adapt hints on failures ---
    for r in results:
        try:
            if not r.failed():
                continue
        except Exception:
            # If the result interface is unexpected, do not adapt.
            continue

        # Increase RAM/CPU for the failed ops and allow retry (ASSIGNABLE_STATES includes FAILED).
        for op in getattr(r, "ops", []) or []:
            op_key = id(op)
            prev_ram = s._op_ram_hint.get(op_key, getattr(r, "ram", None))
            prev_cpu = s._op_cpu_hint.get(op_key, getattr(r, "cpu", None))

            # Initialize from estimate if we somehow had none
            if prev_ram is None or prev_ram <= 0:
                est_mem = _op_est_mem(op)
                if est_mem is not None:
                    prev_ram = est_mem * s._ram_safety_mult
                else:
                    prev_ram = 4.0

            if prev_cpu is None or prev_cpu <= 0:
                prev_cpu = 1.0

            # Retry cap
            cnt = s._op_retry_count.get(op_key, 0) + 1
            s._op_retry_count[op_key] = cnt
            if cnt > s._max_retries_per_op:
                # Stop growing hints; let the simulator mark it failed/incomplete.
                continue

            # Apply multiplicative backoff
            new_ram = float(prev_ram) * s._retry_ram_mult
            new_cpu = float(prev_cpu) * s._retry_cpu_mult

            # Cap RAM/CPU by the pool that it ran on (if provided), else global max across pools.
            pool_id = getattr(r, "pool_id", None)
            if pool_id is not None and 0 <= pool_id < s.executor.num_pools:
                pool = s.executor.pools[pool_id]
                new_ram = min(new_ram, pool.max_ram_pool)
                new_cpu = min(new_cpu, pool.max_cpu_pool)
            else:
                max_ram = max(s.executor.pools[i].max_ram_pool for i in range(s.executor.num_pools))
                max_cpu = max(s.executor.pools[i].max_cpu_pool for i in range(s.executor.num_pools))
                new_ram = min(new_ram, max_ram)
                new_cpu = min(new_cpu, max_cpu)

            s._op_ram_hint[op_key] = new_ram
            s._op_cpu_hint[op_key] = new_cpu

    suspensions = []
    assignments = []

    # --- scheduling loop: try to fill each pool with feasible work ---
    # We do multiple passes to better utilize pools when some candidates don't fit.
    # Bound the total work per tick to avoid pathological loops.
    max_attempts = 200
    attempts = 0

    # Put back pipelines that are not finished but have no assignable ops yet (waiting on parents/running).
    # We'll requeue them to preserve fairness.
    deferred = []

    while attempts < max_attempts:
        attempts += 1
        made_progress = False

        for pool_id in range(s.executor.num_pools):
            pool = s.executor.pools[pool_id]
            if pool.avail_cpu_pool <= 0 or pool.avail_ram_pool <= 0:
                continue

            # Pull next candidate for this pool by priority/fairness.
            p = _pop_next_pipeline_for_pool(pool_id)
            if p is None:
                continue

            # Drop completed pipelines; keep others in circulation.
            if _pipeline_done_or_failed(p):
                continue

            op = _get_next_assignable_op(p)
            if op is None:
                # Not assignable now (parents incomplete or already running); defer.
                deferred.append(p)
                continue

            # Prefer a pool based on memory estimate; if this pool isn't ideal, we may redirect.
            chosen_pool = _choose_pool_for_op(op, p.priority)
            if chosen_pool is None:
                # Can't fit anywhere (per estimate). Defer and hope resources free up / estimate was too high.
                deferred.append(p)
                continue

            # If chosen pool differs, push pipeline back and let the other pool pick it up in this tick.
            if chosen_pool != pool_id:
                # Put it back at the front of its priority queue to reduce latency.
                if p.priority == Priority.QUERY:
                    s.q_query.insert(0, p)
                elif p.priority == Priority.INTERACTIVE:
                    s.q_interactive.insert(0, p)
                else:
                    s.q_batch.insert(0, p)
                continue

            # Batch throttling on tight pools (double-check here even if selected)
            if p.priority == Priority.BATCH_PIPELINE and not _batch_allowed_in_pool(pool):
                deferred.append(p)
                continue

            cpu, ram = _size_resources(op, p.priority, pool_id)
            if cpu is None or ram is None:
                deferred.append(p)
                continue

            # If estimate exists and our chosen RAM is still materially below estimate, skip to avoid likely OOM.
            # (This happens when pool has low avail_ram_pool.)
            est_mem = _op_est_mem(op)
            if est_mem is not None and ram < est_mem * 0.95:
                deferred.append(p)
                continue

            assignments.append(
                Assignment(
                    ops=[op],
                    cpu=cpu,
                    ram=ram,
                    priority=p.priority,
                    pool_id=pool_id,
                    pipeline_id=p.pipeline_id,
                )
            )
            made_progress = True

            # Requeue pipeline so remaining ops can be scheduled in later ticks.
            # Keep FIFO order but ensure it doesn't starve behind large backlogs.
            if p.priority == Priority.QUERY:
                s.q_query.append(p)
            elif p.priority == Priority.INTERACTIVE:
                s.q_interactive.append(p)
            else:
                s.q_batch.append(p)

        if not made_progress:
            break

    # Requeue deferred pipelines at the back to preserve fairness, but keep class separation.
    for p in deferred:
        if p.priority == Priority.QUERY:
            s.q_query.append(p)
        elif p.priority == Priority.INTERACTIVE:
            s.q_interactive.append(p)
        else:
            s.q_batch.append(p)

    s._tick += 1
    return suspensions, assignments
