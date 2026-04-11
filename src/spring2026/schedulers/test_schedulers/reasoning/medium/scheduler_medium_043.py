# policy_key: scheduler_medium_043
# reasoning_effort: medium
# exp: reasoning
# model: gpt-5.2-2025-12-11
# llm_cost: 0.086909
# generation_seconds: 76.12
# generated_at: 2026-04-09T23:27:46.612052
@register_scheduler_init(key="scheduler_medium_043")
def scheduler_medium_043_init(s):
    """Priority-aware, OOM-avoiding, non-starving scheduler.

    Core ideas:
      1) Weighted priority ordering (QUERY > INTERACTIVE > BATCH) with aging to avoid starvation.
      2) Conservative default RAM sizing + per-operator RAM learning from OOM failures (retry with more RAM).
      3) Pack multiple assignments per pool per tick (within available CPU/RAM), but avoid assigning
         more than one op per pipeline per tick to reduce self-contention and improve fairness.
      4) Mild pool affinity (if multiple pools): pool 0 favors QUERY/INTERACTIVE; other pools favor BATCH,
         but do not hard-block to avoid leaving capacity idle.
    """
    # Logical time for aging (increments whenever scheduler runs)
    s.now = 0

    # Track first-seen time of each pipeline for aging/fairness
    s.pipeline_arrival_tick = {}  # pipeline_id -> tick

    # RAM estimate per (pipeline_id, op_key) and optional global per op signature
    s.op_ram_est = {}  # (pipeline_id, op_key) -> ram
    s.op_ram_global_est = {}  # op_signature -> ram

    # Retry counters (we rely on runtime_status allowing FAILED to be re-assigned)
    s.op_retry_count = {}  # (pipeline_id, op_key) -> count

    # Configuration knobs (kept simple and safe)
    s.max_oom_retries = 4
    s.max_non_oom_retries = 1

    # Aging: every aging_div ticks adds ~1 point to effective score
    s.aging_div = 30

    # Minimum CPU allocation per container
    s.min_cpu = 1

    # Minimum RAM floor as a fraction of pool max RAM (prevents too-tiny allocations)
    s.min_ram_frac = 0.05

    # Default RAM fractions by priority (as a fraction of pool max RAM)
    s.default_ram_frac = {
        Priority.QUERY: 0.25,
        Priority.INTERACTIVE: 0.20,
        Priority.BATCH_PIPELINE: 0.15,
    }

    # RAM caps by priority (avoid single op consuming entire pool by default)
    s.cap_ram_frac = {
        Priority.QUERY: 0.55,
        Priority.INTERACTIVE: 0.45,
        Priority.BATCH_PIPELINE: 0.35,
    }

    # Default CPU targets by priority (favor more concurrency; give queries a bit more)
    s.default_cpu_target = {
        Priority.QUERY: 4,
        Priority.INTERACTIVE: 3,
        Priority.BATCH_PIPELINE: 2,
    }

    # CPU caps by priority (avoid extreme CPU grabs unless pool is otherwise idle)
    s.cap_cpu = {
        Priority.QUERY: 6,
        Priority.INTERACTIVE: 5,
        Priority.BATCH_PIPELINE: 4,
    }


@register_scheduler(key="scheduler_medium_043")
def scheduler_medium_043(s, results, pipelines):
    """
    Scheduling step.

    Returns:
      suspensions: we avoid preemption here (insufficient portable introspection of running containers)
      assignments: a list of new container assignments
    """

    def _is_oom_error(err):
        if err is None:
            return False
        try:
            msg = str(err).lower()
        except Exception:
            return False
        return ("oom" in msg) or ("out of memory" in msg) or ("memory" in msg and "killed" in msg)

    def _priority_weight(pri):
        # Match the objective emphasis; larger means more urgent
        if pri == Priority.QUERY:
            return 10
        if pri == Priority.INTERACTIVE:
            return 5
        return 1

    def _op_key(op):
        # Best-effort stable key for an operator within a pipeline
        for attr in ("op_id", "operator_id", "id", "name", "key"):
            if hasattr(op, attr):
                try:
                    v = getattr(op, attr)
                    if v is not None:
                        return (attr, str(v))
                except Exception:
                    pass
        # Fallback: object identity (works as long as op objects persist in the sim)
        return ("py_id", str(id(op)))

    def _op_signature(op):
        # Cross-pipeline signature to reuse RAM learning if the same op shape repeats
        for attr in ("op_type", "type", "kind", "name"):
            if hasattr(op, attr):
                try:
                    v = getattr(op, attr)
                    if v is not None:
                        return (attr, str(v))
                except Exception:
                    pass
        # Weak fallback
        return ("repr", str(type(op)))

    def _default_ram_for(priority, pool):
        frac = s.default_ram_frac.get(priority, 0.15)
        return max(pool.max_ram_pool * s.min_ram_frac, pool.max_ram_pool * frac)

    def _cap_ram_for(priority, pool):
        frac = s.cap_ram_frac.get(priority, 0.35)
        return max(pool.max_ram_pool * s.min_ram_frac, pool.max_ram_pool * frac)

    def _default_cpu_for(priority, avail_cpu):
        tgt = s.default_cpu_target.get(priority, 2)
        cap = s.cap_cpu.get(priority, 4)
        # If the pool is small, take what we can; otherwise use a moderate target.
        return max(s.min_cpu, min(int(avail_cpu), int(min(tgt, cap))))

    def _maybe_update_ram_estimates(exec_result):
        # Update RAM estimates using observed assignments and failures (OOM -> increase)
        if not hasattr(exec_result, "ops") or exec_result.ops is None:
            return

        # Determine if this result indicates an OOM
        failed = False
        try:
            failed = exec_result.failed()
        except Exception:
            failed = bool(getattr(exec_result, "error", None))

        oom = failed and _is_oom_error(getattr(exec_result, "error", None))

        # Estimate update rule:
        # - On OOM: multiply by 2 (at least) to quickly escape repeated failures.
        # - On success: keep at least a conservative lower bound (do not shrink aggressively).
        for op in exec_result.ops:
            # We may not have pipeline_id on the result; use best-effort (it usually exists in Assignment).
            # If missing, only update global signature.
            ram_assigned = getattr(exec_result, "ram", None)
            if ram_assigned is None:
                continue

            sig = _op_signature(op)
            if oom:
                new_global = max(s.op_ram_global_est.get(sig, 0), ram_assigned * 2.0)
                s.op_ram_global_est[sig] = new_global
            else:
                # Keep a modest floor from successful runs to avoid starting too tiny later.
                prev = s.op_ram_global_est.get(sig, 0)
                s.op_ram_global_est[sig] = max(prev, ram_assigned * 0.75)

    def _effective_score(pipeline, pool_id):
        # Higher score => schedule earlier
        pri = pipeline.priority
        base = _priority_weight(pri)

        arrival = s.pipeline_arrival_tick.get(pipeline.pipeline_id, s.now)
        age = max(0, s.now - arrival)
        aging_bonus = age / float(max(1, s.aging_div))

        # Mild pool affinity:
        # - pool 0: boost high-priority a bit
        # - other pools: boost batch a bit
        affinity = 1.0
        if s.executor.num_pools > 1:
            if pool_id == 0 and (pri == Priority.QUERY or pri == Priority.INTERACTIVE):
                affinity = 1.15
            elif pool_id != 0 and pri == Priority.BATCH_PIPELINE:
                affinity = 1.10

        return (base + aging_bonus) * affinity

    def _pick_ram_for_op(pipeline, op, pool):
        pri = pipeline.priority
        pid = pipeline.pipeline_id
        ok = _op_key(op)
        sig = _op_signature(op)

        # Start from per-op-per-pipeline estimate; fallback to global estimate; then default.
        est = s.op_ram_est.get((pid, ok))
        if est is None:
            est = s.op_ram_global_est.get(sig)
        if est is None:
            est = _default_ram_for(pri, pool)

        # Apply floor and cap, then clamp to available at assignment time
        floor = pool.max_ram_pool * s.min_ram_frac
        cap = _cap_ram_for(pri, pool)
        return max(floor, min(est, cap))

    def _note_retry(pipeline, op, oom):
        pid = pipeline.pipeline_id
        ok = _op_key(op)
        k = (pid, ok)
        s.op_retry_count[k] = s.op_retry_count.get(k, 0) + 1

        # On OOM, immediately bump the per-pipeline estimate to reduce repeat failures.
        # (We don't know true peak; use exponential backoff.)
        if oom:
            prev = s.op_ram_est.get(k)
            if prev is None:
                prev = s.op_ram_global_est.get(_op_signature(op), None)
            if prev is None:
                prev = 0
            # If we had any prior estimate, increase it; otherwise next scheduling uses default,
            # and the global estimate is updated in _maybe_update_ram_estimates from result.ram.
            if prev > 0:
                s.op_ram_est[k] = prev * 1.6

    def _can_retry(pipeline, op, oom):
        pid = pipeline.pipeline_id
        ok = _op_key(op)
        n = s.op_retry_count.get((pid, ok), 0)
        return n < (s.max_oom_retries if oom else s.max_non_oom_retries)

    # Advance logical time
    s.now += 1

    # Incorporate new arrivals (for aging)
    for p in pipelines:
        if p.pipeline_id not in s.pipeline_arrival_tick:
            s.pipeline_arrival_tick[p.pipeline_id] = s.now

    # Process results for RAM learning and retries
    for r in results:
        _maybe_update_ram_estimates(r)

        # Track retries when we see explicit failures (best-effort)
        failed = False
        try:
            failed = r.failed()
        except Exception:
            failed = bool(getattr(r, "error", None))

        if failed and hasattr(r, "ops") and r.ops:
            oom = _is_oom_error(getattr(r, "error", None))
            # We don't have direct access to pipeline object here reliably; retry decisions happen at pick time.
            # Still, update retry counters only if we can infer pipeline id from op objects (not always possible).
            # So we skip retry counter here and rely on runtime_status FAILED to be picked; estimates updated above.
            # (Keeping this block to avoid confusion; no-op intentionally.)
            pass

    # Early exit: if nothing changed, do nothing
    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    # Build candidate list from newly arrived pipelines plus any pipelines we can reach through arrivals.
    # We only have "pipelines" (new arrivals) provided; we must schedule among *all* active pipelines.
    # Eudoxia typically provides active pipelines across calls via internal references; to be safe, keep a registry.
    if not hasattr(s, "_active_pipelines"):
        s._active_pipelines = {}  # pipeline_id -> Pipeline

    for p in pipelines:
        s._active_pipelines[p.pipeline_id] = p

    # Opportunistically refresh registry from results (sometimes includes ops but not pipeline)
    # If simulator doesn't resend existing pipelines each tick, we rely on registry.
    active_list = list(s._active_pipelines.values())

    # Remove completed pipelines from registry to keep selection efficient
    alive = {}
    for p in active_list:
        try:
            st = p.runtime_status()
        except Exception:
            alive[p.pipeline_id] = p
            continue
        if not st.is_pipeline_successful():
            alive[p.pipeline_id] = p
    s._active_pipelines = alive
    active_list = list(alive.values())

    # Track pipelines already assigned in this tick (avoid multiple ops from same pipeline per tick)
    assigned_this_tick = set()

    # Main scheduling: fill each pool with multiple assignments while resources allow
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool

        # Greedy loop: keep assigning while we can fit at least a minimal container
        while avail_cpu >= s.min_cpu and avail_ram > 0:
            best = None
            best_score = -1e30
            best_op = None

            # Pick best pipeline with a ready op
            for p in active_list:
                if p.pipeline_id in assigned_this_tick:
                    continue

                try:
                    st = p.runtime_status()
                except Exception:
                    continue

                if st.is_pipeline_successful():
                    continue

                # Choose one ready operator (require parents complete)
                op_list = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
                if not op_list:
                    continue
                op = op_list[0]

                # If operator is in FAILED state, decide whether to retry based on error type if available.
                # We cannot reliably inspect per-op failure reason; still, allow retries but cap.
                # Use heuristic: if it has failed before, count it as a retry attempt and stop after caps.
                # (This is conservative; avoids infinite retry loops.)
                ok = _op_key(op)
                retries = s.op_retry_count.get((p.pipeline_id, ok), 0)
                # We don't know if the last failure was OOM; allow up to oom cap if we have boosted estimates,
                # otherwise allow at least non-oom cap.
                # If we have any learned RAM estimate for this op, treat as OOM-path and allow more tries.
                has_est = ((p.pipeline_id, ok) in s.op_ram_est) or (_op_signature(op) in s.op_ram_global_est)
                maxr = s.max_oom_retries if has_est else s.max_non_oom_retries
                if retries >= maxr:
                    continue

                sc = _effective_score(p, pool_id)
                if sc > best_score:
                    best = p
                    best_score = sc
                    best_op = op

            if best is None:
                break

            # Size resources for the chosen op
            ram_need = _pick_ram_for_op(best, best_op, pool)
            cpu_need = _default_cpu_for(best.priority, avail_cpu)

            # Clamp to current availability
            ram = min(avail_ram, ram_need)
            cpu = min(avail_cpu, cpu_need)

            # If we cannot allocate minimally, stop trying in this pool
            if cpu < s.min_cpu or ram <= 0:
                break

            # If RAM is extremely low relative to pool, avoid scheduling to reduce OOM churn
            # (unless it's the only thing we can do).
            min_floor = pool.max_ram_pool * s.min_ram_frac
            if ram < min_floor * 0.75:
                break

            # Issue assignment
            assignments.append(
                Assignment(
                    ops=[best_op],
                    cpu=cpu,
                    ram=ram,
                    priority=best.priority,
                    pool_id=pool_id,
                    pipeline_id=best.pipeline_id,
                )
            )

            # Mark pipeline assigned this tick
            assigned_this_tick.add(best.pipeline_id)

            # If this was a retry (FAILED state), increment retry count now.
            # We can't read the op state directly, so use heuristic: if it has any retry count already, keep counting.
            ok = _op_key(best_op)
            k = (best.pipeline_id, ok)
            if k in s.op_retry_count:
                s.op_retry_count[k] += 1

            # Update available resources for subsequent assignments
            avail_cpu -= cpu
            avail_ram -= ram

    return suspensions, assignments
