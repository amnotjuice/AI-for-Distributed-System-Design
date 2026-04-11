# policy_key: scheduler_none_027
# reasoning_effort: none
# model: gpt-5.2-2025-12-11
# llm_cost: 0.049753
# generation_seconds: 48.88
# generated_at: 2026-04-09T22:06:12.067307
@register_scheduler_init(key="scheduler_none_027")
def scheduler_none_027_init(s):
    """Priority-aware, OOM-adaptive, low-churn scheduler.

    Goals for the weighted latency objective:
      - Prefer QUERY/INTERACTIVE admission to protect tail latency.
      - Avoid failures: on OOM, increase RAM estimate and retry (bounded).
      - Reduce head-of-line blocking: schedule at most one ready op per pipeline per tick.
      - Avoid starvation: aging for lower priority over time.
      - Avoid churn: preempt only when a high-priority op cannot be placed anywhere.

    Design choices kept intentionally simple and robust for Eudoxia:
      - Per-(pipeline, op) RAM estimate tracked and increased after OOM.
      - Conservative CPU sizing: give enough to run, but do not greedily take whole pool.
      - Pool selection: prefer pools with more headroom for high priority.
      - Optional targeted preemption: suspend a single low-priority running container to fit one high-priority assignment.
    """
    # Waiting pipelines (keep arrival order, but we score by priority + aging)
    s.waiting_queue = []

    # RAM estimation state:
    # s.op_ram_est[(pipeline_id, op_id)] = estimated RAM to allocate next time
    s.op_ram_est = {}

    # OOM retry counters per op
    s.op_oom_retries = {}

    # For simple aging: record when pipeline first seen by scheduler (sim time if available, else tick counter)
    s.pipeline_first_seen = {}

    # Local tick counter if simulator doesn't expose time
    s._tick = 0

    # Tunables (kept conservative)
    s.MAX_OOM_RETRIES_PER_OP = 3
    s.RAM_GROWTH_FACTOR = 1.6
    s.RAM_GROWTH_ADD = 0.5  # additive bump too, in case small estimates
    s.RAM_HEADROOM_FACTOR = 1.10  # slight headroom above estimate to reduce repeated OOMs
    s.MIN_RAM_FLOOR = 0.5  # never allocate below this

    # CPU sizing
    s.CPU_FRACTION_QUERY = 0.60
    s.CPU_FRACTION_INTERACTIVE = 0.50
    s.CPU_FRACTION_BATCH = 0.35
    s.CPU_MIN = 0.5

    # Preemption controls
    s.ENABLE_PREEMPTION = True
    s.PREEMPT_ONLY_IF_CANNOT_FIT_ANYWHERE = True

    # Aging strength (batch gets more aging boost to avoid starvation)
    s.AGING_RATE_BATCH = 0.015
    s.AGING_RATE_INTERACTIVE = 0.008
    s.AGING_RATE_QUERY = 0.004

    # Internal: best-effort mapping from known priority values
    s._prio_weight = {
        Priority.QUERY: 3.0,
        Priority.INTERACTIVE: 2.0,
        Priority.BATCH_PIPELINE: 1.0,
    }


@register_scheduler(key="scheduler_none_027")
def scheduler_none_027(s, results, pipelines):
    """
    Priority + aging queue, OOM-adaptive RAM sizing, conservative CPU allocation, and limited preemption.

    Returns:
        suspensions: list of Suspend(container_id, pool_id)
        assignments: list of Assignment(ops, cpu, ram, priority, pool_id, pipeline_id)
    """
    s._tick += 1

    def _now():
        # Prefer a simulator-provided time if present; otherwise use ticks.
        t = getattr(s.executor, "now", None)
        if callable(t):
            try:
                return float(t())
            except Exception:
                pass
        t2 = getattr(s.executor, "time", None)
        if isinstance(t2, (int, float)):
            return float(t2)
        return float(s._tick)

    def _pipeline_age(p):
        first = s.pipeline_first_seen.get(p.pipeline_id, None)
        if first is None:
            first = _now()
            s.pipeline_first_seen[p.pipeline_id] = first
        return max(0.0, _now() - first)

    def _priority_score(priority, age):
        base = s._prio_weight.get(priority, 1.0)
        if priority == Priority.BATCH_PIPELINE:
            return base + age * s.AGING_RATE_BATCH
        if priority == Priority.INTERACTIVE:
            return base + age * s.AGING_RATE_INTERACTIVE
        return base + age * s.AGING_RATE_QUERY

    def _get_op_id(op):
        # Best effort stable id for dictionary keys.
        for attr in ("op_id", "operator_id", "id", "name"):
            if hasattr(op, attr):
                try:
                    v = getattr(op, attr)
                    if v is not None:
                        return str(v)
                except Exception:
                    pass
        return str(id(op))

    def _cpu_target(priority, avail_cpu, max_cpu):
        # Conservative, to allow concurrency and reduce queueing for high-priority.
        frac = s.CPU_FRACTION_BATCH
        if priority == Priority.QUERY:
            frac = s.CPU_FRACTION_QUERY
        elif priority == Priority.INTERACTIVE:
            frac = s.CPU_FRACTION_INTERACTIVE
        cpu = max(s.CPU_MIN, avail_cpu * frac)
        # Don't exceed pool max (or avail) as seen by this scheduling tick
        cpu = min(cpu, avail_cpu, max_cpu)
        return cpu

    def _ram_target(pipeline, op, pool_avail_ram, pool_max_ram):
        # Use learned estimate if any, else a small initial guess based on pool size.
        op_id = _get_op_id(op)
        key = (pipeline.pipeline_id, op_id)

        est = s.op_ram_est.get(key, None)
        if est is None:
            # Start small but not tiny to avoid immediate OOM loops.
            # Use a small fraction of pool max, capped, plus a floor.
            try:
                guess = max(s.MIN_RAM_FLOOR, min(2.0, 0.10 * float(pool_max_ram)))
            except Exception:
                guess = 1.0
            est = guess
            s.op_ram_est[key] = est

        ram = max(s.MIN_RAM_FLOOR, est * s.RAM_HEADROOM_FACTOR)
        # Clip to pool availability and capacity
        ram = min(ram, pool_avail_ram, pool_max_ram)
        return ram

    def _update_from_results():
        # Handle OOMs by increasing RAM estimate for that op.
        for r in results:
            if not getattr(r, "failed", lambda: False)():
                continue
            # Detect OOM-like failures
            err = getattr(r, "error", None)
            err_s = str(err).lower() if err is not None else ""
            is_oom = ("oom" in err_s) or ("out of memory" in err_s) or ("memory" in err_s and "alloc" in err_s)
            if not is_oom:
                continue

            # Update each failed op in the container's op list
            for op in getattr(r, "ops", []) or []:
                # We don't have pipeline_id on result in the spec; but Assignment used pipeline_id.
                # Best effort: try to find pipeline_id on result, else we can't key precisely.
                pid = getattr(r, "pipeline_id", None)
                if pid is None:
                    # Fall back: bump a generic per-op id estimate if possible (less effective).
                    pid = "unknown"
                op_id = _get_op_id(op)
                key = (pid, op_id)

                retries = s.op_oom_retries.get(key, 0) + 1
                s.op_oom_retries[key] = retries

                # Increase estimate based on what we attempted.
                prev_est = s.op_ram_est.get(key, None)
                attempted = getattr(r, "ram", None)
                base = None
                if attempted is not None:
                    try:
                        base = float(attempted)
                    except Exception:
                        base = None
                if base is None and prev_est is not None:
                    base = float(prev_est)
                if base is None:
                    base = 1.0

                # Multiply + add to escape small-start traps.
                new_est = max(base * s.RAM_GROWTH_FACTOR + s.RAM_GROWTH_ADD, base + s.RAM_GROWTH_ADD)
                s.op_ram_est[key] = new_est

    def _pipeline_is_done_or_failed(p):
        status = p.runtime_status()
        if status.is_pipeline_successful():
            return True
        # If any operators failed and we already exceeded retries, treat as failed and stop scheduling it.
        failed_ops = status.get_ops([OperatorState.FAILED], require_parents_complete=False)
        if not failed_ops:
            return False
        # If any failed op exceeded retries, drop the pipeline from queue (it will count as failed in metrics).
        for op in failed_ops:
            op_id = _get_op_id(op)
            key = (p.pipeline_id, op_id)
            if s.op_oom_retries.get(key, 0) > s.MAX_OOM_RETRIES_PER_OP:
                return True
        return False

    def _pick_ready_op(p):
        status = p.runtime_status()
        # Only schedule ops whose parents are complete.
        ready = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        if not ready:
            return []
        # Schedule only one op per pipeline at a time to reduce contention and head-of-line blocking.
        return ready[:1]

    def _sort_waiting(pipes):
        # Sort by (priority+aging) descending; tie-breaker by arrival order preserved by stable sort.
        decorated = []
        for idx, p in enumerate(pipes):
            age = _pipeline_age(p)
            score = _priority_score(p.priority, age)
            decorated.append((score, -idx, p))  # stable-ish with -idx to preserve earlier
        decorated.sort(key=lambda x: x[0], reverse=True)
        return [p for _, _, p in decorated]

    def _choose_pool_for(priority):
        # Prefer pools with more headroom for high priority; for batch, prefer fullest (to pack).
        pool_ids = list(range(s.executor.num_pools))
        pools = s.executor.pools

        def headroom(i):
            p = pools[i]
            # Normalize by max to compare across pools.
            cpu_h = (p.avail_cpu_pool / p.max_cpu_pool) if p.max_cpu_pool else 0.0
            ram_h = (p.avail_ram_pool / p.max_ram_pool) if p.max_ram_pool else 0.0
            # Bias slightly toward RAM since OOM failures are costly.
            return 0.45 * cpu_h + 0.55 * ram_h

        if priority in (Priority.QUERY, Priority.INTERACTIVE):
            pool_ids.sort(key=headroom, reverse=True)  # most headroom first
        else:
            pool_ids.sort(key=headroom)  # pack batch into tighter pools
        return pool_ids

    def _running_containers(results_list):
        # Best effort: infer running containers from results; Eudoxia may not expose full cluster state.
        # We'll only use this for preemption candidates when results contain container_id/pool_id/cpu/ram.
        seen = []
        for r in results_list:
            cid = getattr(r, "container_id", None)
            pid = getattr(r, "pool_id", None)
            pr = getattr(r, "priority", None)
            cpu = getattr(r, "cpu", None)
            ram = getattr(r, "ram", None)
            if cid is None or pid is None:
                continue
            seen.append((cid, pid, pr, cpu, ram))
        return seen

    def _can_fit(pool, cpu_need, ram_need):
        return (pool.avail_cpu_pool >= cpu_need) and (pool.avail_ram_pool >= ram_need)

    _update_from_results()

    # Enqueue new pipelines
    for p in pipelines:
        if p.pipeline_id not in s.pipeline_first_seen:
            s.pipeline_first_seen[p.pipeline_id] = _now()
        s.waiting_queue.append(p)

    if not pipelines and not results:
        return [], []

    # Filter out completed/terminal failed pipelines from waiting queue
    new_q = []
    for p in s.waiting_queue:
        if _pipeline_is_done_or_failed(p):
            continue
        new_q.append(p)
    s.waiting_queue = new_q

    suspensions = []
    assignments = []

    # Attempt to place high priority work first, but ensure eventual batch progress via aging.
    ordered = _sort_waiting(s.waiting_queue)

    # We'll try to schedule multiple assignments per tick across pools.
    # Track pipelines we touched to avoid scheduling multiple ops from same pipeline in one tick.
    scheduled_pipelines = set()

    # We will repeatedly sweep pools to place best-fitting ready ops.
    # Limit iterations to avoid infinite loops in edge cases.
    max_passes = max(1, s.executor.num_pools * 2)
    passes = 0

    while passes < max_passes:
        passes += 1
        made_progress = False

        # Consider pools in an order that benefits high priority: for each candidate, choose preferred pool list.
        # We'll just iterate pools sequentially here and do pool selection per assignment, which is simpler and stable.
        for candidate in ordered:
            if candidate.pipeline_id in scheduled_pipelines:
                continue

            op_list = _pick_ready_op(candidate)
            if not op_list:
                continue

            # Find a pool for it
            pool_order = _choose_pool_for(candidate.priority)
            placed = False

            for pool_id in pool_order:
                pool = s.executor.pools[pool_id]
                if pool.avail_cpu_pool <= 0 or pool.avail_ram_pool <= 0:
                    continue

                cpu_need = _cpu_target(candidate.priority, pool.avail_cpu_pool, pool.max_cpu_pool)
                ram_need = _ram_target(candidate, op_list[0], pool.avail_ram_pool, pool.max_ram_pool)

                # Ensure minimal viability
                if cpu_need <= 0 or ram_need <= 0:
                    continue

                if _can_fit(pool, cpu_need, ram_need):
                    assignments.append(
                        Assignment(
                            ops=op_list,
                            cpu=cpu_need,
                            ram=ram_need,
                            priority=candidate.priority,
                            pool_id=pool_id,
                            pipeline_id=candidate.pipeline_id,
                        )
                    )
                    scheduled_pipelines.add(candidate.pipeline_id)
                    made_progress = True
                    placed = True
                    break

            if placed:
                continue

            # If cannot place and it's high priority, consider a single targeted preemption.
            if s.ENABLE_PREEMPTION and candidate.priority in (Priority.QUERY, Priority.INTERACTIVE):
                # If we can fit it somewhere without preemption, don't preempt (extra guard)
                if s.PREEMPT_ONLY_IF_CANNOT_FIT_ANYWHERE:
                    can_fit_anywhere = False
                    for pool_id in range(s.executor.num_pools):
                        pool = s.executor.pools[pool_id]
                        if pool.avail_cpu_pool <= 0 or pool.avail_ram_pool <= 0:
                            continue
                        cpu_need = _cpu_target(candidate.priority, pool.avail_cpu_pool, pool.max_cpu_pool)
                        ram_need = _ram_target(candidate, op_list[0], pool.avail_ram_pool, pool.max_ram_pool)
                        if _can_fit(pool, cpu_need, ram_need):
                            can_fit_anywhere = True
                            break
                    if can_fit_anywhere:
                        continue

                # Choose a low-priority container to suspend from the best pool (highest headroom target).
                pool_order = _choose_pool_for(candidate.priority)
                running = _running_containers(results)
                # Prefer suspending BATCH first, then INTERACTIVE (never suspend QUERY to run QUERY).
                def victim_rank(v):
                    _, vid_pool, vprio, vcpu, vram = v
                    prio_rank = 0
                    if vprio == Priority.BATCH_PIPELINE:
                        prio_rank = 2
                    elif vprio == Priority.INTERACTIVE:
                        prio_rank = 1
                    else:
                        prio_rank = 0
                    # Prefer larger RAM reclaim to avoid multiple preemptions.
                    try:
                        reclaimed = float(vram) if vram is not None else 0.0
                    except Exception:
                        reclaimed = 0.0
                    # Prefer victims in preferred pools
                    pool_pref = 0
                    if vid_pool in pool_order[:1]:
                        pool_pref = 2
                    elif vid_pool in pool_order[:2]:
                        pool_pref = 1
                    return (pool_pref, prio_rank, reclaimed)

                # Find best victim that makes room in some pool after suspension (approximate by freeing its allocation)
                victims = [v for v in running if v[2] in (Priority.BATCH_PIPELINE, Priority.INTERACTIVE)]
                victims.sort(key=victim_rank, reverse=True)

                for victim in victims[:5]:
                    cid, vpool_id, vprio, vcpu, vram = victim
                    pool = s.executor.pools[vpool_id]
                    try:
                        freed_cpu = float(vcpu) if vcpu is not None else 0.0
                    except Exception:
                        freed_cpu = 0.0
                    try:
                        freed_ram = float(vram) if vram is not None else 0.0
                    except Exception:
                        freed_ram = 0.0

                    # Estimate if suspending would make room for the candidate in that pool
                    cpu_need = _cpu_target(candidate.priority, pool.avail_cpu_pool + freed_cpu, pool.max_cpu_pool)
                    ram_need = _ram_target(candidate, op_list[0], pool.avail_ram_pool + freed_ram, pool.max_ram_pool)

                    if (pool.avail_cpu_pool + freed_cpu >= cpu_need) and (pool.avail_ram_pool + freed_ram >= ram_need):
                        suspensions.append(Suspend(container_id=cid, pool_id=vpool_id))
                        # After suspension, attempt assignment in same pool.
                        assignments.append(
                            Assignment(
                                ops=op_list,
                                cpu=min(cpu_need, pool.max_cpu_pool),
                                ram=min(ram_need, pool.max_ram_pool),
                                priority=candidate.priority,
                                pool_id=vpool_id,
                                pipeline_id=candidate.pipeline_id,
                            )
                        )
                        scheduled_pipelines.add(candidate.pipeline_id)
                        made_progress = True
                        break

        if not made_progress:
            break

    # Rebuild queue: keep unscheduled pipelines, but also keep scheduled ones for later ops (they may still have remaining DAG)
    # We keep them all; the runtime_status will gate readiness.
    s.waiting_queue = ordered

    return suspensions, assignments
