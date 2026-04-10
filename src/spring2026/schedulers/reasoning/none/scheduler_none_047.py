# policy_key: scheduler_none_047
# reasoning_effort: none
# model: gpt-5.2-2025-12-11
# llm_cost: 0.058867
# generation_seconds: 54.94
# generated_at: 2026-04-09T22:22:12.751561
@register_scheduler_init(key="scheduler_none_047")
def scheduler_none_047_init(s):
    """Priority-aware, OOM-adaptive, low-churn scheduler.

    Goals:
    - Minimize weighted latency: protect QUERY/INTERACTIVE responsiveness.
    - Avoid failures (720s penalty) by learning per-op RAM needs from OOMs and retrying.
    - Avoid starvation via priority aging (effective priority increases with wait time).
    - Avoid preemption churn: only preempt when high-priority work is blocked and needs headroom.
    """
    # Waiting pipelines (admission queue)
    s.waiting_queue = []

    # Per-(pipeline_id, op_id)-> learned minimum safe RAM (from OOMs). Used for future retries.
    s.op_ram_floor = {}

    # Per-(pipeline_id)->arrival time for aging
    s.pipeline_arrival_time = {}

    # Deterministic clock in ticks (we don't assume simulator provides wall time here)
    s.ticks = 0

    # Track last-seen running containers for optional preemption decisions
    # container_id -> dict(priority=..., pool_id=..., cpu=..., ram=..., pipeline_id=...)
    s.running = {}

    # Tuning knobs (conservative defaults; improve over naive FIFO without being too aggressive)
    s.max_assignments_per_pool_per_tick = 2  # limit fan-out to reduce fragmentation / churn
    s.max_ops_per_assignment = 1            # keep atomic assignments small to reduce tail impact
    s.oom_backoff_factor = 1.5              # increase RAM on OOM
    s.oom_backoff_add = 0.5                 # add RAM (GB-equivalent units) on OOM if small
    s.min_cpu_share_query = 0.6             # fraction of pool CPU to give QUERY when running
    s.min_cpu_share_interactive = 0.5       # fraction of pool CPU to give INTERACTIVE when running
    s.min_cpu_share_batch = 0.25            # fraction of pool CPU to give BATCH when running
    s.min_ram_share_query = 0.6             # fraction of pool RAM to give QUERY when running
    s.min_ram_share_interactive = 0.5       # fraction of pool RAM to give INTERACTIVE when running
    s.min_ram_share_batch = 0.25            # fraction of pool RAM to give BATCH when running

    # Soft reservation for high priority: do not let batch consume beyond this when there is waiting high-priority work
    s.reserve_cpu_for_high_prio = 0.2  # keep at least 20% CPU free if queries/interactive waiting
    s.reserve_ram_for_high_prio = 0.2  # keep at least 20% RAM free if queries/interactive waiting

    # Aging rates: increase effective priority with waiting time to avoid starvation
    s.aging_per_tick = {
        Priority.QUERY: 0.0,         # already top
        Priority.INTERACTIVE: 0.02,  # mild
        Priority.BATCH_PIPELINE: 0.05,  # stronger so batch eventually runs
    }


@register_scheduler(key="scheduler_none_047")
def scheduler_none_047(s, results, pipelines):
    """
    Scheduler step:
    1) Ingest new pipelines; record arrival tick for aging.
    2) Learn from results: on OOM/failure, raise per-op RAM floor and allow retry.
    3) Decide preemptions only if high-priority work is blocked and pool lacks headroom.
    4) Assign ready ops with priority+aging ordering; size CPU/RAM conservatively to avoid OOM while protecting latency.
    """
    # Local helpers kept inside to avoid imports / external dependencies
    def _priority_rank(pri):
        # Lower is better
        if pri == Priority.QUERY:
            return 0
        if pri == Priority.INTERACTIVE:
            return 1
        return 2  # Priority.BATCH_PIPELINE and others treated as lowest

    def _base_weight(pri):
        if pri == Priority.QUERY:
            return 10.0
        if pri == Priority.INTERACTIVE:
            return 5.0
        return 1.0

    def _iter_ops(pipeline):
        # Best-effort unique op id extraction; simulator ops usually have stable identifiers.
        # Fall back to python id if needed (still stable within a run).
        try:
            return list(getattr(pipeline, "values"))
        except Exception:
            return []

    def _op_key(pipeline, op):
        # Try to use an explicit op id/name; otherwise use object id.
        op_id = None
        for attr in ("op_id", "operator_id", "id", "name"):
            if hasattr(op, attr):
                try:
                    op_id = getattr(op, attr)
                    break
                except Exception:
                    pass
        if op_id is None:
            op_id = id(op)
        return (pipeline.pipeline_id, op_id)

    def _effective_score(pipeline):
        # Lower is scheduled earlier. Uses base priority rank minus aging factor (so waiting reduces score).
        pri = pipeline.priority
        arrival = s.pipeline_arrival_time.get(pipeline.pipeline_id, s.ticks)
        waited = max(0, s.ticks - arrival)
        aging = s.aging_per_tick.get(pri, 0.03) * waited
        return (_priority_rank(pri) - aging, -_base_weight(pri), waited)

    def _has_waiting_high_priority():
        for p in s.waiting_queue:
            if p.priority in (Priority.QUERY, Priority.INTERACTIVE):
                status = p.runtime_status()
                if status.is_pipeline_successful():
                    continue
                # if there exists any ready op, consider it waiting high-priority
                ready = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
                if ready:
                    return True
        return False

    def _choose_pool_for_priority(priority):
        # Prefer pool with most headroom (cpu+ram) for high priority; for batch prefer most remaining after reservations.
        best_pool = None
        best_score = None
        need_high = priority in (Priority.QUERY, Priority.INTERACTIVE)
        waiting_high = _has_waiting_high_priority()

        for pool_id in range(s.executor.num_pools):
            pool = s.executor.pools[pool_id]
            avail_cpu = pool.avail_cpu_pool
            avail_ram = pool.avail_ram_pool
            if avail_cpu <= 0 or avail_ram <= 0:
                continue

            # Effective headroom: if batch and high-priority waiting, penalize using reserved slack
            if (not need_high) and waiting_high:
                cpu_slack = max(0.0, avail_cpu - s.reserve_cpu_for_high_prio * pool.max_cpu_pool)
                ram_slack = max(0.0, avail_ram - s.reserve_ram_for_high_prio * pool.max_ram_pool)
                score = (cpu_slack / (pool.max_cpu_pool + 1e-9)) + (ram_slack / (pool.max_ram_pool + 1e-9))
            else:
                score = (avail_cpu / (pool.max_cpu_pool + 1e-9)) + (avail_ram / (pool.max_ram_pool + 1e-9))

            # For queries, prefer more headroom; for batch, prefer also more headroom but after reservations.
            if best_score is None or score > best_score:
                best_score = score
                best_pool = pool_id
        return best_pool

    def _size_resources(pool, priority, desired_ram_floor):
        # Conservative, latency-protective sizing. We bias CPU upward for high priority but avoid consuming entire pool.
        max_cpu = pool.max_cpu_pool
        max_ram = pool.max_ram_pool
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool

        if priority == Priority.QUERY:
            cpu_target = max(1.0, s.min_cpu_share_query * max_cpu)
            ram_target = max(desired_ram_floor, s.min_ram_share_query * max_ram)
        elif priority == Priority.INTERACTIVE:
            cpu_target = max(1.0, s.min_cpu_share_interactive * max_cpu)
            ram_target = max(desired_ram_floor, s.min_ram_share_interactive * max_ram)
        else:
            cpu_target = max(1.0, s.min_cpu_share_batch * max_cpu)
            ram_target = max(desired_ram_floor, s.min_ram_share_batch * max_ram)

        # Clamp to available and pool maxima
        cpu = min(avail_cpu, max(1.0, min(cpu_target, max_cpu)))
        ram = min(avail_ram, max(desired_ram_floor, min(ram_target, max_ram)))

        # If we can't meet desired RAM floor, return None to indicate can't safely schedule now.
        if ram + 1e-9 < desired_ram_floor:
            return None, None
        if cpu <= 0 or ram <= 0:
            return None, None
        return cpu, ram

    def _learn_from_failure(exec_result):
        # Increase RAM floor for ops in this container on OOM-like errors.
        if not exec_result.failed():
            return
        err = getattr(exec_result, "error", None)
        if err is None:
            return
        err_s = str(err).lower()
        oom_like = ("oom" in err_s) or ("out of memory" in err_s) or ("memory" in err_s)
        if not oom_like:
            return

        # Increase learned floor for each op in the failed container.
        # If we don't have exact op objects, we still set a pipeline-level bump by mapping via op keys if possible.
        failed_ops = getattr(exec_result, "ops", []) or []
        # If ops list is empty, nothing we can key on reliably.
        for op in failed_ops:
            # We don't know pipeline directly from op; so we use a weak key based on op identity alone.
            # However, the Assignment contains pipeline_id, but ExecutionResult doesn't in the template.
            # Best effort: skip if we can't associate.
            pass

    # Step clock
    s.ticks += 1

    # Ingest new pipelines
    for p in pipelines:
        s.waiting_queue.append(p)
        if p.pipeline_id not in s.pipeline_arrival_time:
            s.pipeline_arrival_time[p.pipeline_id] = s.ticks

    # Early exit if nothing changed
    if not pipelines and not results:
        return [], []

    # Update running container cache and learn from failures
    # Note: We don't get explicit "started" events; but results include container_id and priority/pool resources.
    for r in results:
        # If a container produced a result, it's no longer running; remove from cache
        cid = getattr(r, "container_id", None)
        if cid in s.running:
            del s.running[cid]

        # Learn from OOM-like failures: bump RAM floors for involved ops if we can map them
        if r.failed():
            err = getattr(r, "error", None)
            err_s = str(err).lower() if err is not None else ""
            oom_like = ("oom" in err_s) or ("out of memory" in err_s) or ("memory" in err_s)
            if oom_like:
                # Increase floor based on the RAM that was used when it OOM'd.
                used_ram = float(getattr(r, "ram", 0) or 0)
                bump = max(used_ram * s.oom_backoff_factor, used_ram + s.oom_backoff_add, used_ram + 1.0)
                # Try to update op-level floors; if ops are objects, we can create keys only if we find their pipeline later.
                # We'll also store a "global by op object id" fallback.
                for op in (getattr(r, "ops", []) or []):
                    key = ("_op_object_id", id(op))
                    prev = s.op_ram_floor.get(key, 0.0)
                    s.op_ram_floor[key] = max(prev, bump)

    # Clean up completed/failed pipelines from queue (but DO NOT drop incomplete ones)
    cleaned = []
    for p in s.waiting_queue:
        status = p.runtime_status()
        if status.is_pipeline_successful():
            continue
        # If pipeline has any FAILED ops, we keep it: FAILED is in ASSIGNABLE_STATES, so it can be retried.
        cleaned.append(p)
    s.waiting_queue = cleaned

    suspensions = []
    assignments = []

    # Optional preemption: only if there is ready QUERY waiting and no pool can fit it.
    # We keep this conservative to avoid wasted work.
    waiting_ready_query = None
    for p in sorted(s.waiting_queue, key=_effective_score):
        if p.priority != Priority.QUERY:
            continue
        st = p.runtime_status()
        if st.is_pipeline_successful():
            continue
        ready_ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        if ready_ops:
            waiting_ready_query = (p, ready_ops[0])
            break

    def _desired_ram_floor_for_op(pipeline, op, pool):
        # We don't have explicit operator RAM minima in the template, so we:
        # - start from a small fraction of pool RAM to reduce OOM risk
        # - incorporate learned floors from prior OOMs
        base = max(1.0, 0.1 * pool.max_ram_pool)
        k1 = _op_key(pipeline, op)
        learned = s.op_ram_floor.get(k1, 0.0)
        learned_fallback = s.op_ram_floor.get(("_op_object_id", id(op)), 0.0)
        return max(base, learned, learned_fallback)

    # If query is blocked, attempt to preempt a batch container in the best pool to free headroom.
    if waiting_ready_query is not None:
        p_q, op_q = waiting_ready_query
        # Find a pool that would be chosen for query
        pool_id = _choose_pool_for_priority(Priority.QUERY)
        if pool_id is not None:
            pool = s.executor.pools[pool_id]
            desired_floor = _desired_ram_floor_for_op(p_q, op_q, pool)
            cpu_needed, ram_needed = _size_resources(pool, Priority.QUERY, desired_floor)
            if cpu_needed is None:
                # Try to preempt one lowest-priority running container in this pool, if any known.
                # We only preempt BATCH; avoid interrupting INTERACTIVE/QUERY.
                candidates = []
                for cid, meta in list(s.running.items()):
                    if meta.get("pool_id") != pool_id:
                        continue
                    if meta.get("priority") != Priority.BATCH_PIPELINE:
                        continue
                    candidates.append((meta.get("cpu", 0.0) + meta.get("ram", 0.0), cid, meta))
                candidates.sort(reverse=True)
                if candidates:
                    _, cid, meta = candidates[0]
                    suspensions.append(Suspend(cid, pool_id))
                    # Remove it from running cache immediately to reduce repeated preemption decisions
                    del s.running[cid]

    # Scheduling loop: for each pool, place a small number of ops, prioritizing QUERY/INTERACTIVE and aged batch
    # We pick pipelines globally but place them on the pool selected for their priority.
    # This avoids the naive "first pool only" behavior and reduces queueing for high-priority.
    # To reduce head-of-line blocking, we skip pipelines with no ready ops.
    made_progress = True
    per_pool_count = {i: 0 for i in range(s.executor.num_pools)}

    # We'll iterate a bounded number of times to avoid O(n^2) blowups.
    for _ in range(max(1, len(s.waiting_queue) * 2)):
        if len(assignments) >= s.executor.num_pools * s.max_assignments_per_pool_per_tick:
            break

        # Find next best pipeline with a ready op
        candidate = None
        candidate_op = None
        candidate_pool = None
        candidate_score = None

        waiting_high = _has_waiting_high_priority()

        for p in s.waiting_queue:
            st = p.runtime_status()
            if st.is_pipeline_successful():
                continue

            # Do not schedule batch if we are preserving reservation and pool is tight; handled in pool scoring.
            ready_ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
            if not ready_ops:
                continue
            op = ready_ops[0]

            pool_id = _choose_pool_for_priority(p.priority)
            if pool_id is None:
                continue
            if per_pool_count.get(pool_id, 0) >= s.max_assignments_per_pool_per_tick:
                continue

            pool = s.executor.pools[pool_id]

            # If batch and high-priority waiting, enforce soft reservation by requiring slack above reservation
            if p.priority == Priority.BATCH_PIPELINE and waiting_high:
                if pool.avail_cpu_pool < s.reserve_cpu_for_high_prio * pool.max_cpu_pool:
                    continue
                if pool.avail_ram_pool < s.reserve_ram_for_high_prio * pool.max_ram_pool:
                    continue

            score = _effective_score(p)
            if candidate is None or score < candidate_score:
                candidate = p
                candidate_op = op
                candidate_pool = pool_id
                candidate_score = score

        if candidate is None:
            break

        pool = s.executor.pools[candidate_pool]

        # Determine desired RAM floor and size resources
        desired_floor = _desired_ram_floor_for_op(candidate, candidate_op, pool)
        cpu, ram = _size_resources(pool, candidate.priority, desired_floor)
        if cpu is None:
            # Can't safely place now; mildly increase its learned floor to avoid repeated attempts if tight
            # (but don't blow it up too much)
            k = _op_key(candidate, candidate_op)
            prev = s.op_ram_floor.get(k, 0.0)
            s.op_ram_floor[k] = max(prev, desired_floor)
            # Try other candidates
            continue

        # Create assignment (one op at a time to keep tail latency low and reduce OOM impact)
        op_list = [candidate_op][: s.max_ops_per_assignment]
        assignment = Assignment(
            ops=op_list,
            cpu=cpu,
            ram=ram,
            priority=candidate.priority,
            pool_id=candidate_pool,
            pipeline_id=candidate.pipeline_id,
        )
        assignments.append(assignment)
        per_pool_count[candidate_pool] = per_pool_count.get(candidate_pool, 0) + 1

        # Optimistically track container as running; container_id isn't known yet, but results will clear it.
        # We can't store by container_id; still, we can store a placeholder to estimate contention? Skip.

        # Remove candidate from queue temporarily to avoid re-selecting it in this tick; re-add at end.
        s.waiting_queue.remove(candidate)
        s.waiting_queue.append(candidate)

    return suspensions, assignments
