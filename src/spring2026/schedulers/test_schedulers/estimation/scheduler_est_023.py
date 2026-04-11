# policy_key: scheduler_est_023
# reasoning_effort: low
# exp: estimation
# model: gpt-5.2-2025-12-11
# llm_cost: 0.044982
# generation_seconds: 57.50
# generated_at: 2026-04-10T10:03:06.091196
@register_scheduler_init(key="scheduler_est_023")
def scheduler_est_023_init(s):
    """
    Memory-aware, priority-first best-fit scheduler with conservative OOM backoff.

    Main ideas:
    - Strictly prefer higher priority pipelines (QUERY > INTERACTIVE > BATCH) to protect weighted latency.
    - Within a pipeline, pick the smallest estimated-memory assignable operator first to reduce OOM risk.
    - Place using best-fit (smallest sufficient available RAM pool) to improve packing and reduce fragmentation.
    - On failures, especially suspected OOM, increase a per-pipeline RAM boost factor for retries.
    - Avoid "dropping" pipelines: keep them queued and retry with higher RAM rather than discarding.
    """
    # pipeline_id -> Pipeline
    s._pipelines = {}

    # pipeline_id -> first-seen tick (for simple aging)
    s._enqueued_tick = {}

    # pipeline_id -> multiplicative RAM safety/boost factor for future assignments
    s._ram_boost = {}

    # pipeline_id -> count of observed failures (for escalating boost)
    s._fail_count = {}

    # pipeline_id -> (last_failed_op_obj_id -> last_assigned_ram_gb)
    s._last_ram = {}

    # deterministic ticks for aging
    s._tick = 0


@register_scheduler(key="scheduler_est_023")
def scheduler_est_023(s, results, pipelines):
    """
    Scheduler step:
    - Ingest new pipelines.
    - Update RAM boost state from execution results (backoff on failures).
    - For each pool, select the "best" next pipeline/op that fits available resources.
    - Make at most one assignment per pool per tick (simple, stable baseline improvement).
    """
    s._tick += 1

    suspensions = []
    assignments = []

    # -----------------------------
    # Helpers (kept local to avoid imports)
    # -----------------------------
    def _prio_rank(prio):
        # Lower is better
        if prio == Priority.QUERY:
            return 0
        if prio == Priority.INTERACTIVE:
            return 1
        return 2  # Priority.BATCH_PIPELINE (and any others)

    def _get_assignable_op(pipeline):
        """
        Return a single best assignable op for this pipeline, or None if none ready.
        Chooses the smallest estimated memory op among ready ops to reduce OOM risk.
        """
        status = pipeline.runtime_status()
        op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        if not op_list:
            return None

        def _est_mem(op):
            est = None
            try:
                if op.estimate is not None:
                    est = op.estimate.mem_peak_gb
            except Exception:
                est = None
            # If estimator missing, treat as "unknown/large" to be conservative in ordering.
            if est is None:
                return float("inf")
            # Guard against nonsense
            try:
                return max(0.0, float(est))
            except Exception:
                return float("inf")

        # Prefer smallest estimated memory; tie-break by object id for determinism
        op_list.sort(key=lambda o: (_est_mem(o), id(o)))
        return op_list[0]

    def _pipeline_done_or_hard_failed(pipeline):
        """
        True if pipeline is completed successfully, or in a state that should not be retried.
        We avoid "dropping" on failures, but if the pipeline status declares failure
        (and there are no retryable ops), we will naturally stop scheduling it.
        """
        status = pipeline.runtime_status()
        if status.is_pipeline_successful():
            return True
        # Do not automatically discard failed pipelines; they are penalized heavily if incomplete.
        # We'll keep them unless they have no assignable ops left.
        return False

    def _age_bonus(pipeline_id):
        # Aging to avoid infinite starvation: batch slowly bubbles up over time.
        enq = s._enqueued_tick.get(pipeline_id, s._tick)
        waited = max(0, s._tick - enq)
        return waited

    def _estimate_required_ram_gb(pipeline, op, pool):
        """
        Decide RAM to request for this op:
        - Start from estimate (if present) * safety factor.
        - Apply per-pipeline boost based on previous failures (likely OOM).
        - Clamp to pool limits.
        """
        boost = s._ram_boost.get(pipeline.pipeline_id, 1.25)  # mild safety by default
        est_mem = None
        try:
            if op.estimate is not None:
                est_mem = op.estimate.mem_peak_gb
        except Exception:
            est_mem = None

        if est_mem is not None:
            try:
                est_mem = max(0.0, float(est_mem))
            except Exception:
                est_mem = None

        # If unknown, be conservative but not absurd: take a sizable fraction of the pool.
        if est_mem is None:
            base = max(1.0, 0.50 * float(pool.max_ram_pool))
        else:
            base = max(0.5, est_mem)  # avoid tiny allocations that increase OOM risk

        # If this op previously failed and we know last attempted RAM, ensure we step up.
        last_ram_by_op = s._last_ram.get(pipeline.pipeline_id, {})
        last_attempt = last_ram_by_op.get(id(op))
        if last_attempt is not None:
            # Step-up to reduce repeat failures
            base = max(base, 1.5 * float(last_attempt))

        req = base * boost

        # Clamp to pool maximum; if pool maximum is tiny, keep at least 0.5GB.
        req = max(0.5, min(float(pool.max_ram_pool), float(req)))
        return req

    def _choose_pool_for_req(req_ram, req_cpu):
        """
        Choose a pool that can fit req_ram and req_cpu based on current availability.
        Best-fit on available RAM (smallest available RAM that still fits), to improve packing.
        Returns pool_id or None.
        """
        candidates = []
        for pool_id in range(s.executor.num_pools):
            pool = s.executor.pools[pool_id]
            if pool.avail_cpu_pool >= req_cpu and pool.avail_ram_pool >= req_ram:
                candidates.append((float(pool.avail_ram_pool), float(pool.avail_cpu_pool), pool_id))
        if not candidates:
            return None
        candidates.sort(key=lambda x: (x[0], -x[1], x[2]))
        return candidates[0][2]

    # -----------------------------
    # Ingest new pipelines
    # -----------------------------
    for p in pipelines:
        s._pipelines[p.pipeline_id] = p
        if p.pipeline_id not in s._enqueued_tick:
            s._enqueued_tick[p.pipeline_id] = s._tick
        if p.pipeline_id not in s._ram_boost:
            s._ram_boost[p.pipeline_id] = 1.25
        if p.pipeline_id not in s._fail_count:
            s._fail_count[p.pipeline_id] = 0
        if p.pipeline_id not in s._last_ram:
            s._last_ram[p.pipeline_id] = {}

    # -----------------------------
    # Update state from results (OOM backoff)
    # -----------------------------
    for r in results:
        if getattr(r, "failed", None) is not None and r.failed():
            # Escalate RAM boost for this pipeline to reduce repeated failures.
            # We treat any failure as possibly memory-related (safe bias to improve completion rate).
            pid = getattr(r, "pipeline_id", None)
            # pipeline_id might not exist on result; infer from ops if possible (best-effort)
            if pid is None:
                # If ops belong to a single pipeline, the assignment carried pipeline_id, but result may not.
                # We can't reliably infer; skip pipeline-wide boost in that case.
                pid = None

            # Still store last attempted RAM per op object to force step-up on retry.
            try:
                ram_used = float(r.ram)
            except Exception:
                ram_used = None

            # Capture op ids if present
            ops = getattr(r, "ops", None) or []
            if pid is not None and pid in s._last_ram and ram_used is not None:
                for op in ops:
                    s._last_ram[pid][id(op)] = ram_used

            if pid is not None and pid in s._ram_boost:
                s._fail_count[pid] = s._fail_count.get(pid, 0) + 1
                # Exponential-ish backoff with cap to avoid requesting absurd RAM
                cur = float(s._ram_boost[pid])
                # Increase more aggressively after multiple failures
                mult = 1.5 if s._fail_count[pid] <= 2 else 1.8
                s._ram_boost[pid] = min(8.0, cur * mult)

    # -----------------------------
    # Early exit if nothing to do
    # -----------------------------
    if not pipelines and not results:
        return suspensions, assignments

    # -----------------------------
    # Remove completed pipelines from active map (keep state dicts; cheap and helps retries if reappear)
    # -----------------------------
    completed = []
    for pid, p in s._pipelines.items():
        if _pipeline_done_or_hard_failed(p):
            status = p.runtime_status()
            if status.is_pipeline_successful():
                completed.append(pid)
    for pid in completed:
        s._pipelines.pop(pid, None)

    # -----------------------------
    # Build a list of runnable candidates (pipeline, op)
    # -----------------------------
    candidates = []
    for pid, p in s._pipelines.items():
        status = p.runtime_status()
        if status.is_pipeline_successful():
            continue

        op = _get_assignable_op(p)
        if op is None:
            continue

        # Primary ordering: priority; then aging (to avoid total starvation); then est_mem
        pr = _prio_rank(p.priority)
        age = _age_bonus(pid)

        # For age: only help lower priorities (batch) meaningfully; queries should not be delayed by aging.
        # Convert to an "effective priority" by subtracting some age for batch/interactive.
        if pr == 2:  # batch
            pr_eff = pr - min(0.8, age / 200.0)  # slow promotion
        elif pr == 1:  # interactive
            pr_eff = pr - min(0.4, age / 300.0)
        else:
            pr_eff = pr

        # Estimated mem for tie-break (unknown -> inf)
        try:
            est_mem = op.estimate.mem_peak_gb if (op.estimate is not None) else None
        except Exception:
            est_mem = None
        if est_mem is None:
            est_mem_sort = float("inf")
        else:
            try:
                est_mem_sort = max(0.0, float(est_mem))
            except Exception:
                est_mem_sort = float("inf")

        candidates.append((pr_eff, -age, est_mem_sort, pid, p, op))

    # Sort once; we'll scan per pool picking the first that fits.
    candidates.sort(key=lambda x: (x[0], x[1], x[2], x[3]))

    # -----------------------------
    # For each pool, pick one best-fitting candidate and assign
    # -----------------------------
    # To avoid over-committing the same pipeline multiple times in one tick,
    # track pipelines already assigned this tick.
    assigned_pipelines = set()

    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)
        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # CPU request: keep it simple and modest to reduce interference & allow concurrency.
        # Use up to half the pool CPU for query/interactive, less for batch.
        chosen = None
        chosen_req_ram = None
        chosen_req_cpu = None

        for _, __, ___, pid, p, op in candidates:
            if pid in assigned_pipelines:
                continue

            # Decide CPU request based on priority and availability
            if p.priority == Priority.QUERY:
                req_cpu = max(1.0, min(avail_cpu, max(1.0, 0.75 * float(pool.max_cpu_pool))))
            elif p.priority == Priority.INTERACTIVE:
                req_cpu = max(1.0, min(avail_cpu, max(1.0, 0.50 * float(pool.max_cpu_pool))))
            else:
                req_cpu = max(1.0, min(avail_cpu, max(1.0, 0.33 * float(pool.max_cpu_pool))))

            # If we can't even give 1 CPU, skip
            if req_cpu > avail_cpu:
                continue

            req_ram = _estimate_required_ram_gb(p, op, pool)

            # If it fits this pool, select it
            if req_ram <= avail_ram:
                chosen = (pid, p, op)
                chosen_req_ram = req_ram
                chosen_req_cpu = req_cpu
                break

        if chosen is None:
            # If nothing fits by our requested RAM, try a "last resort" for top priorities:
            # allocate all available RAM to a single ready query/interactive op to maximize completion rate.
            # This reduces the 720s penalty risk from incompletes.
            for _, __, ___, pid, p, op in candidates:
                if pid in assigned_pipelines:
                    continue
                if p.priority not in (Priority.QUERY, Priority.INTERACTIVE):
                    continue
                if avail_cpu < 1.0 or avail_ram < 0.5:
                    continue
                chosen = (pid, p, op)
                chosen_req_ram = avail_ram
                chosen_req_cpu = max(1.0, min(avail_cpu, float(pool.max_cpu_pool)))
                break

        if chosen is None:
            continue

        pid, p, op = chosen

        # Best-fit refinement across pools:
        # If this op might fit in a different pool with tighter available RAM, place there instead.
        # (Only if it still fits there right now.)
        best_pool = _choose_pool_for_req(chosen_req_ram, chosen_req_cpu)
        if best_pool is not None:
            pool_id_use = best_pool
            pool_use = s.executor.pools[pool_id_use]
            # Re-check; if something changed (unlikely within tick), fall back
            if float(pool_use.avail_ram_pool) >= chosen_req_ram and float(pool_use.avail_cpu_pool) >= chosen_req_cpu:
                pool_id = pool_id_use

        assignment = Assignment(
            ops=[op],
            cpu=chosen_req_cpu,
            ram=chosen_req_ram,
            priority=p.priority,
            pool_id=pool_id,
            pipeline_id=p.pipeline_id,
        )
        assignments.append(assignment)
        assigned_pipelines.add(pid)

    return suspensions, assignments
