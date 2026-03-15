# policy_key: scheduler_none_044
# reasoning_effort: none
# model: gpt-5.2-2025-12-11
# llm_cost: 0.049800
# generation_seconds: 47.49
# generated_at: 2026-03-14T02:13:13.287862
@register_scheduler_init(key="scheduler_none_044")
def scheduler_none_044_init(s):
    """Priority-aware FIFO with conservative preemption and simple OOM-driven RAM retries.

    Small, incremental improvements over naive FIFO:
      1) Maintain separate queues per priority; always try higher priority first.
      2) Keep per-operator RAM "hints" that increase on OOM, so retries don't repeat failures.
      3) Light-touch preemption: if a high-priority op can't fit anywhere, suspend one
         lowest-priority container in the most suitable pool to make room.
      4) Avoid oversizing CPU for tiny high-priority ops by using a small default CPU cap,
         while still allowing batch to soak up the remaining capacity.
    """
    # Priority order: protect query/interactive tail latency
    s.priority_order = [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]

    # Per-priority FIFO queues of pipelines
    s.waiting_by_prio = {p: [] for p in s.priority_order}

    # Remember recent pool choice per pipeline (best-effort affinity)
    s.last_pool_for_pipeline = {}

    # Per-operator RAM hints: (pipeline_id, op_id) -> ram
    s.op_ram_hint = {}

    # Simple knobs
    s.min_cpu_slice = 1.0                # don't schedule with <1 vCPU when possible
    s.hp_cpu_cap_fraction = 0.5          # high-priority ops won't take more than 50% of a pool by default
    s.batch_cpu_floor_fraction = 0.25    # try to leave at least 25% pool CPU for batch when possible (soft)
    s.oom_backoff_multiplier = 2.0       # increase RAM hint on OOM
    s.max_ram_fraction = 1.0             # allow up to 100% of pool RAM for single op (must fit)
    s.preempt_enable = True
    s.preempt_only_if_needed = True      # only preempt if nothing can be scheduled otherwise


@register_scheduler(key="scheduler_none_044")
def scheduler_none_044(s, results, pipelines):
    """
    Priority-aware scheduling loop:
      - Enqueue arrivals by priority.
      - Update RAM hints from OOM failures.
      - Attempt to assign one ready operator per pool per tick, preferring high priority.
      - If high priority cannot be scheduled due to lack of headroom, preempt a single
        lowest-priority running container to free space, then schedule.
    """
    # Local helpers (no imports needed)
    def _queue_for(prio):
        if prio in s.waiting_by_prio:
            return s.waiting_by_prio[prio]
        # Safety: unknown priorities go to the lowest class
        return s.waiting_by_prio[s.priority_order[-1]]

    def _op_key(pipeline, op):
        # Robust operator identity: try common fields; fallback to object's id()
        op_id = getattr(op, "op_id", None)
        if op_id is None:
            op_id = getattr(op, "operator_id", None)
        if op_id is None:
            op_id = getattr(op, "id", None)
        if op_id is None:
            op_id = id(op)
        return (pipeline.pipeline_id, op_id)

    def _get_ready_ops(pipeline):
        status = pipeline.runtime_status()
        # Only schedule ops whose parents are complete
        return status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)

    def _pipeline_done_or_failed(pipeline):
        status = pipeline.runtime_status()
        if status.is_pipeline_successful():
            return True
        # If there are failures, we still may want to retry; only drop if it's "hard failed".
        # The starter example drops any failure; we improve by allowing retries via hints.
        # Here we keep pipeline unless it is successful. We rely on simulator to expose FAILED ops in ASSIGNABLE_STATES.
        return False

    def _hp(prio):
        return prio in (Priority.QUERY, Priority.INTERACTIVE)

    def _choose_cpu(pool, prio):
        avail = pool.avail_cpu_pool
        if avail <= 0:
            return 0.0
        if _hp(prio):
            cap = max(s.min_cpu_slice, pool.max_cpu_pool * s.hp_cpu_cap_fraction)
            return min(avail, cap)
        # Batch can use what's available (but we may soft-reserve some CPU)
        return avail

    def _choose_ram(pool, pipeline, op, prio):
        avail = pool.avail_ram_pool
        if avail <= 0:
            return 0.0
        hint = s.op_ram_hint.get(_op_key(pipeline, op), None)
        if hint is None:
            # Start conservative: give HP a smaller footprint to reduce interference.
            # (We don't know minimums; OOM will increase hint.)
            base = pool.max_ram_pool * (0.25 if _hp(prio) else 0.50)
            hint = max(1.0, min(base, pool.max_ram_pool * s.max_ram_fraction))
        return min(avail, hint)

    def _pool_preference_order(pipeline):
        # Best effort: try last pool first to keep locality/stability, then others.
        if s.executor.num_pools <= 1:
            return [0]
        last = s.last_pool_for_pipeline.get(pipeline.pipeline_id, None)
        order = list(range(s.executor.num_pools))
        if last is not None and last in order:
            order.remove(last)
            order.insert(0, last)
        return order

    def _try_schedule_one_in_pool(pool_id, pipeline):
        pool = s.executor.pools[pool_id]
        if pool.avail_cpu_pool <= 0 or pool.avail_ram_pool <= 0:
            return None

        ready_ops = _get_ready_ops(pipeline)
        if not ready_ops:
            return None

        # Schedule a single op per assignment (keep it simple and responsive)
        op = ready_ops[0]
        cpu = _choose_cpu(pool, pipeline.priority)
        ram = _choose_ram(pool, pipeline, op, pipeline.priority)

        if cpu <= 0 or ram <= 0:
            return None

        # Must fit within current availability
        if cpu > pool.avail_cpu_pool or ram > pool.avail_ram_pool:
            return None

        s.last_pool_for_pipeline[pipeline.pipeline_id] = pool_id
        return Assignment(
            ops=[op],
            cpu=cpu,
            ram=ram,
            priority=pipeline.priority,
            pool_id=pool_id,
            pipeline_id=pipeline.pipeline_id,
        )

    def _has_any_schedulable(pipeline):
        # Check if pipeline has a ready op and could fit somewhere using current hints.
        ready_ops = _get_ready_ops(pipeline)
        if not ready_ops:
            return False
        op = ready_ops[0]
        for pool_id in _pool_preference_order(pipeline):
            pool = s.executor.pools[pool_id]
            if pool.avail_cpu_pool <= 0 or pool.avail_ram_pool <= 0:
                continue
            cpu = _choose_cpu(pool, pipeline.priority)
            ram = _choose_ram(pool, pipeline, op, pipeline.priority)
            if cpu <= pool.avail_cpu_pool and ram <= pool.avail_ram_pool and cpu > 0 and ram > 0:
                return True
        return False

    def _select_preemption_candidate(target_pool_id):
        # Choose one running/assigned container to suspend in the target pool.
        # Prefer suspending lowest priority first to protect HP latency.
        # We rely on results to tell us which containers exist (observed so far).
        lowest = None
        lowest_rank = -1  # higher means worse priority
        for r in results:
            cid = getattr(r, "container_id", None)
            if cid is None:
                continue
            if r.pool_id != target_pool_id:
                continue
            # If it's failed/completed, don't preempt
            if hasattr(r, "failed") and r.failed():
                continue
            pr = getattr(r, "priority", None)
            if pr is None:
                continue
            try:
                rank = s.priority_order.index(pr)
            except Exception:
                rank = len(s.priority_order) - 1
            # We want to preempt the lowest priority => maximum rank
            if rank > lowest_rank:
                lowest_rank = rank
                lowest = r
            elif rank == lowest_rank and lowest is not None:
                # Tie-breaker: preempt the largest RAM consumer to free space quickly
                if getattr(r, "ram", 0) > getattr(lowest, "ram", 0):
                    lowest = r
        if lowest is None:
            return None
        return Suspend(lowest.container_id, lowest.pool_id)

    # Enqueue new pipelines by priority
    for p in pipelines:
        _queue_for(p.priority).append(p)

    # Update hints from execution results
    for r in results:
        if getattr(r, "error", None) is None:
            continue
        err = str(r.error).lower()
        if "oom" in err or "out of memory" in err:
            # Increase RAM hint for the specific failed operator(s) in that pipeline.
            # If we can't identify op ids reliably, we at least bump for all ops in r.ops.
            for op in getattr(r, "ops", []) or []:
                # We don't have pipeline object here; key by pipeline_id in result? If absent, skip.
                pid = getattr(r, "pipeline_id", None)
                if pid is None:
                    continue
                op_id = getattr(op, "op_id", None)
                if op_id is None:
                    op_id = getattr(op, "operator_id", None)
                if op_id is None:
                    op_id = getattr(op, "id", None)
                if op_id is None:
                    op_id = id(op)
                k = (pid, op_id)
                prev = s.op_ram_hint.get(k, None)
                prev = prev if prev is not None else max(1.0, float(getattr(r, "ram", 1.0) or 1.0))
                bumped = max(prev * s.oom_backoff_multiplier, prev + 1.0)
                # Cap at the pool max RAM observed for that run (if available), else leave uncapped
                pool = s.executor.pools[r.pool_id] if r.pool_id is not None else None
                if pool is not None:
                    bumped = min(bumped, pool.max_ram_pool * s.max_ram_fraction)
                s.op_ram_hint[k] = bumped

    # Early exit if nothing changed
    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    # Clean queues: drop successful pipelines; keep others
    for prio in list(s.waiting_by_prio.keys()):
        q = s.waiting_by_prio[prio]
        new_q = []
        for p in q:
            if _pipeline_done_or_failed(p):
                continue
            new_q.append(p)
        s.waiting_by_prio[prio] = new_q

    # Attempt scheduling: one assignment per pool per tick
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        if pool.avail_cpu_pool <= 0 or pool.avail_ram_pool <= 0:
            continue

        scheduled = False

        # Try higher priority queues first
        for prio in s.priority_order:
            q = s.waiting_by_prio[prio]
            if not q:
                continue

            # Find the first pipeline in this priority that has a ready op and can fit in this pool
            # (preserve FIFO order within the priority class)
            chosen_idx = None
            for i, p in enumerate(q):
                if _pipeline_done_or_failed(p):
                    continue
                # Try to schedule in preferred pools order; since we're in a specific pool loop,
                # only attempt if this pool is acceptable (either preferred or any if needed)
                # We'll attempt directly; if it fails, continue scanning.
                a = _try_schedule_one_in_pool(pool_id, p)
                if a is not None:
                    chosen_idx = i
                    assignments.append(a)
                    scheduled = True
                    break
            if scheduled:
                # Move chosen pipeline to back (round-robin within priority to reduce head-of-line blocking)
                if chosen_idx is not None:
                    p = q.pop(chosen_idx)
                    q.append(p)
                break

        if scheduled:
            continue

        # If we couldn't schedule anything and preemption is enabled, try to help high priority only.
        if not s.preempt_enable:
            continue

        # Identify highest-priority pipeline that is blocked globally (can't fit anywhere currently)
        blocked_hp = None
        for prio in s.priority_order:
            if not _hp(prio):
                continue
            for p in s.waiting_by_prio[prio]:
                if _pipeline_done_or_failed(p):
                    continue
                if _get_ready_ops(p) and not _has_any_schedulable(p):
                    blocked_hp = p
                    break
            if blocked_hp is not None:
                break

        if blocked_hp is None:
            continue

        # Preempt one low-priority container in this pool to create space
        cand = _select_preemption_candidate(pool_id)
        if cand is None:
            continue

        suspensions.append(cand)

        # After preemption, we do not immediately assume resources are free in this same tick;
        # keep behavior conservative and deterministic. Next tick will schedule.
        # (If simulator frees instantly, that's okay; but we keep logic simple.)

    return suspensions, assignments
