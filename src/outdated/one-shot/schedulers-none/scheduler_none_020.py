# policy_key: scheduler_none_020
# reasoning_effort: none
# model: gpt-5.2-2025-12-11
# llm_cost: 0.032997
# generation_seconds: 52.63
# generated_at: 2026-03-12T21:34:28.394754
@register_scheduler_init(key="scheduler_none_020")
def scheduler_none_020_init(s):
    """Priority-aware FIFO with conservative admission and OOM backoff.

    Incremental improvements over naive FIFO:
      1) Maintain separate queues per priority and always schedule higher priority first.
      2) Only schedule "ready" operators (parents complete) and keep pipelines in queues until done.
      3) Use per-pipeline resource hints that increase on OOM to avoid repeated failures.
      4) Basic preemption: if high-priority work cannot start due to lack of headroom, suspend
         some lower-priority running containers in that pool to free resources.
    """
    # Queues are FIFO within each priority
    s.wait_q = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }

    # Resource hints per pipeline; start small and increase on OOM
    # Stored as dict[pipeline_id] = {"cpu": float, "ram": float}
    s.hints = {}

    # Track last-seen pipelines so we can continue scheduling them after initial arrival
    s.known = {}

    # Index of round-robin pool selection per priority to spread load gently
    s.rr_idx = {Priority.QUERY: 0, Priority.INTERACTIVE: 0, Priority.BATCH_PIPELINE: 0}

    # Track currently running containers per pool & priority using results stream
    # dict[pool_id] = dict[priority] = {container_id: {"cpu":..., "ram":...}}
    s.running = {}

    # Tuning knobs (kept intentionally simple)
    s.max_preempt_per_tick = 2
    s.max_assignments_per_pool_per_tick = 1  # incremental: avoid overpacking; reduce tail interference
    s.min_cpu_granularity = 1.0


@register_scheduler(key="scheduler_none_020")
def scheduler_none_020(s, results, pipelines):
    """Scheduler step: update state, optionally preempt, then assign ready ops by priority."""
    # --- helpers (no imports required) ---
    def _ensure_pool_state(pool_id):
        if pool_id not in s.running:
            s.running[pool_id] = {
                Priority.QUERY: {},
                Priority.INTERACTIVE: {},
                Priority.BATCH_PIPELINE: {},
            }

    def _p_order():
        # Highest first: QUERY > INTERACTIVE > BATCH
        return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]

    def _lower_priorities(pri):
        order = _p_order()
        if pri not in order:
            return []
        idx = order.index(pri)
        return order[idx + 1 :]

    def _hint_get(pipeline, pool):
        # Initialize hint if absent: use a conservative slice of pool capacity.
        pid = pipeline.pipeline_id
        if pid not in s.hints:
            base_cpu = max(s.min_cpu_granularity, pool.max_cpu_pool * 0.25)
            base_ram = max(1.0, pool.max_ram_pool * 0.25)
            s.hints[pid] = {"cpu": base_cpu, "ram": base_ram}
        return s.hints[pid]["cpu"], s.hints[pid]["ram"]

    def _hint_bump_on_oom(pipeline_id, pool):
        # Backoff: multiply RAM by 2 up to pool max; gently increase CPU too.
        hint = s.hints.get(pipeline_id)
        if not hint:
            return
        hint["ram"] = min(pool.max_ram_pool, max(1.0, hint["ram"] * 2.0))
        hint["cpu"] = min(pool.max_cpu_pool, max(s.min_cpu_granularity, hint["cpu"] * 1.25))

    def _hint_soft_decrease_on_success(pipeline_id, pool):
        # Optional: slightly decrease hints after success to avoid permanently inflated allocations.
        # Keep very gentle to avoid oscillation.
        hint = s.hints.get(pipeline_id)
        if not hint:
            return
        hint["ram"] = max(1.0, min(pool.max_ram_pool, hint["ram"] * 0.98))
        hint["cpu"] = max(s.min_cpu_granularity, min(pool.max_cpu_pool, hint["cpu"] * 0.99))

    def _enqueue_pipeline(pipeline):
        # Only enqueue once per "arrival"; we keep it in known and re-enqueue while incomplete.
        pid = pipeline.pipeline_id
        s.known[pid] = pipeline
        s.wait_q[pipeline.priority].append(pid)

    def _pipeline_done_or_failed(pipeline):
        st = pipeline.runtime_status()
        if st.is_pipeline_successful():
            return True
        # Treat any FAILED as terminal unless we want retries; we rely on ASSIGNABLE_STATES
        # to include FAILED but only for operator-level retries, which can be unsafe.
        # Here: if there are failed ops, we still allow retry if they are assignable and parents complete.
        return False

    def _next_ready_op(pipeline):
        st = pipeline.runtime_status()
        # Only schedule ops whose parents are complete.
        ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        if not ops:
            return []
        # Keep it incremental: schedule a single operator at a time to reduce interference.
        return ops[:1]

    def _choose_pool_for(pipeline, cpu_need, ram_need):
        # Prefer pool with enough headroom; among them pick most headroom in RAM (OOM avoidance),
        # tie-breaker by CPU headroom.
        best = None
        best_score = None
        for pool_id in range(s.executor.num_pools):
            pool = s.executor.pools[pool_id]
            if pool.avail_cpu_pool >= cpu_need and pool.avail_ram_pool >= ram_need:
                score = (pool.avail_ram_pool - ram_need, pool.avail_cpu_pool - cpu_need)
                if best is None or score > best_score:
                    best = pool_id
                    best_score = score
        return best

    def _preempt_to_fit(pool_id, cpu_need, ram_need, high_pri):
        # Preempt lower-priority running containers in this pool until the request fits
        # or we hit the preemption cap.
        _ensure_pool_state(pool_id)
        pool = s.executor.pools[pool_id]
        need_cpu = max(0.0, cpu_need - pool.avail_cpu_pool)
        need_ram = max(0.0, ram_need - pool.avail_ram_pool)
        if need_cpu <= 0 and need_ram <= 0:
            return []

        susp = []
        preempted = 0

        # Consider lower priorities in order: batch first, then interactive (when QUERY needs help).
        for lp in reversed(_lower_priorities(high_pri)):
            # Greedy: suspend largest RAM consumers first to address OOM risk.
            running_items = list(s.running[pool_id][lp].items())
            running_items.sort(key=lambda kv: (kv[1].get("ram", 0.0), kv[1].get("cpu", 0.0)), reverse=True)
            for cid, res in running_items:
                if preempted >= s.max_preempt_per_tick:
                    return susp
                susp.append(Suspend(cid, pool_id))
                preempted += 1

                # Optimistically account freed resources (simulator will update on next tick).
                freed_cpu = float(res.get("cpu", 0.0) or 0.0)
                freed_ram = float(res.get("ram", 0.0) or 0.0)
                need_cpu = max(0.0, need_cpu - freed_cpu)
                need_ram = max(0.0, need_ram - freed_ram)

                # Remove from our local running map immediately to avoid double-suspending
                s.running[pool_id][lp].pop(cid, None)

                if need_cpu <= 0 and need_ram <= 0:
                    return susp
        return susp

    # --- ingest new pipelines ---
    for p in pipelines:
        _enqueue_pipeline(p)

    # --- early exit if nothing changes ---
    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    # --- update running bookkeeping and hints from results ---
    for r in results:
        _ensure_pool_state(r.pool_id)

        # If we see an execution result, remove it from running if present (it finished/failed)
        # and adjust hints based on OOM.
        if getattr(r, "container_id", None) is not None:
            s.running[r.pool_id][r.priority].pop(r.container_id, None)

        # If failed due to OOM, bump hint for that pipeline
        if r.failed():
            # Be defensive: error string matching
            err = str(getattr(r, "error", "") or "").lower()
            if "oom" in err or "out of memory" in err or "memory" in err:
                pool = s.executor.pools[r.pool_id]
                _hint_bump_on_oom(r.pipeline_id, pool)
        else:
            # Softly decrease to reduce over-allocation after successes
            pool = s.executor.pools[r.pool_id]
            _hint_soft_decrease_on_success(r.pipeline_id, pool)

    # Also refresh running map from results that indicate currently running containers (if any).
    # In many sims, results are only for completed/failed. We still handle the case where results
    # include running-state updates by adding anything with cpu/ram and container_id.
    for r in results:
        if getattr(r, "container_id", None) is None:
            continue
        # If it's reported with resources and no failure, treat it as running until completion report removes it.
        if not r.failed() and getattr(r, "cpu", None) is not None and getattr(r, "ram", None) is not None:
            _ensure_pool_state(r.pool_id)
            s.running[r.pool_id][r.priority][r.container_id] = {"cpu": r.cpu, "ram": r.ram}

    # --- Build a list of candidate pipeline IDs per priority (stable FIFO, but skip completed) ---
    # Keep queues from growing unbounded with duplicates: we allow duplicates but clean as we pop.
    def _pop_next_pipeline_id(priority):
        q = s.wait_q[priority]
        while q:
            pid = q.pop(0)
            p = s.known.get(pid)
            if p is None:
                continue
            if _pipeline_done_or_failed(p):
                continue
            return pid
        return None

    # --- scheduling loop: by priority, try to place one op at a time ---
    # We perform limited preemption only if a high-priority ready op exists and no pool can fit it.
    for pri in _p_order():
        # Attempt to schedule multiple pipelines of this priority in this tick,
        # but limited naturally by pool limits.
        # We'll do a bounded number of attempts to avoid infinite loops with non-ready pipelines.
        attempts = 0
        max_attempts = 50

        while attempts < max_attempts:
            attempts += 1
            pid = _pop_next_pipeline_id(pri)
            if pid is None:
                break

            pipeline = s.known[pid]
            st = pipeline.runtime_status()

            # Only schedule if there is a ready operator
            op_list = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
            if not op_list:
                # Not ready yet; re-enqueue to revisit later
                s.wait_q[pri].append(pid)
                continue

            # Compute requested resources from hint; choose a pool that fits
            # (use max pool characteristics for initial hint, but pool-specific clamp below).
            # Try to choose the best-fitting pool first.
            best_pool = None
            best_assignment = None

            # Evaluate each pool for feasibility and create best assignment
            for pool_id in range(s.executor.num_pools):
                pool = s.executor.pools[pool_id]

                cpu_hint, ram_hint = _hint_get(pipeline, pool)
                cpu_req = min(pool.max_cpu_pool, max(s.min_cpu_granularity, float(cpu_hint)))
                ram_req = min(pool.max_ram_pool, max(1.0, float(ram_hint)))

                # Per-pool assignment limit to reduce interference
                # (If we've already scheduled for this pool, skip further placements here.)
                # Count assignments already planned for this pool.
                planned_for_pool = 0
                for a in assignments:
                    if a.pool_id == pool_id:
                        planned_for_pool += 1
                if planned_for_pool >= s.max_assignments_per_pool_per_tick:
                    continue

                if pool.avail_cpu_pool >= cpu_req and pool.avail_ram_pool >= ram_req:
                    score = (pool.avail_ram_pool - ram_req, pool.avail_cpu_pool - cpu_req)
                    if best_pool is None or score > best_assignment["score"]:
                        best_pool = pool_id
                        best_assignment = {
                            "cpu": cpu_req,
                            "ram": ram_req,
                            "score": score,
                        }

            if best_pool is not None:
                a = Assignment(
                    ops=op_list,
                    cpu=best_assignment["cpu"],
                    ram=best_assignment["ram"],
                    priority=pipeline.priority,
                    pool_id=best_pool,
                    pipeline_id=pipeline.pipeline_id,
                )
                assignments.append(a)

                # Optimistically reduce available capacity to avoid over-assigning within tick.
                pool = s.executor.pools[best_pool]
                pool.avail_cpu_pool -= best_assignment["cpu"]
                pool.avail_ram_pool -= best_assignment["ram"]

                # Keep pipeline in queue for subsequent ops
                s.wait_q[pri].append(pid)
                continue

            # If we couldn't place it: for QUERY/INTERACTIVE, try preempting lower priority in best pool.
            # Choose pool where preemption would help most (largest running lower-priority footprint).
            if pri in (Priority.QUERY, Priority.INTERACTIVE):
                # Pick a target pool: where lower priority is running and closest to fitting.
                target_pool = None
                best_deficit = None
                target_req = None

                for pool_id in range(s.executor.num_pools):
                    pool = s.executor.pools[pool_id]
                    cpu_hint, ram_hint = _hint_get(pipeline, pool)
                    cpu_req = min(pool.max_cpu_pool, max(s.min_cpu_granularity, float(cpu_hint)))
                    ram_req = min(pool.max_ram_pool, max(1.0, float(ram_hint)))

                    # If the request itself exceeds pool max, skip.
                    if cpu_req > pool.max_cpu_pool or ram_req > pool.max_ram_pool:
                        continue

                    _ensure_pool_state(pool_id)
                    # Only consider if there exists some lower-priority running work to preempt.
                    lower = _lower_priorities(pri)
                    has_lower_running = any(len(s.running[pool_id][lp]) > 0 for lp in lower)
                    if not has_lower_running:
                        continue

                    deficit = (max(0.0, cpu_req - pool.avail_cpu_pool) + max(0.0, ram_req - pool.avail_ram_pool))
                    if target_pool is None or deficit < best_deficit:
                        target_pool = pool_id
                        best_deficit = deficit
                        target_req = (cpu_req, ram_req)

                if target_pool is not None and target_req is not None:
                    cpu_req, ram_req = target_req
                    new_susp = _preempt_to_fit(target_pool, cpu_req, ram_req, pri)
                    if new_susp:
                        suspensions.extend(new_susp)
                        # Re-enqueue and try again later in this tick
                        s.wait_q[pri].insert(0, pid)
                        continue

            # Otherwise cannot place now; re-enqueue and move on
            s.wait_q[pri].append(pid)
            break

    return suspensions, assignments
