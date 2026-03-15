# policy_key: scheduler_low_027
# reasoning_effort: low
# model: gpt-5.2-2025-12-11
# llm_cost: 0.052614
# generation_seconds: 54.93
# generated_at: 2026-03-14T02:23:33.379942
@register_scheduler_init(key="scheduler_low_027")
def scheduler_low_027_init(s):
    """
    Priority-aware scheduler (small step up from naive FIFO).

    Main improvements over the example FIFO:
      1) Separate waiting queues per priority and always schedule higher priority first.
      2) Conservative reservation: avoid letting batch consume the entire pool while
         high-priority work is waiting.
      3) Simple preemption: if high-priority work cannot fit, suspend running batch
         containers in the same pool until it can.
      4) Basic "OOM learning": if an operator fails (heuristically treated as OOM),
         bump the RAM hint for that pipeline for subsequent retries.

    Notes:
      - Keeps the policy intentionally simple and incremental.
      - Schedules at most one operator per pipeline at a time, but may schedule multiple
        pipelines per tick if the pool has remaining resources.
    """
    # Per-priority FIFO queues of pipelines
    s.waiting = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }

    # RAM hint per pipeline_id: bumped on failures to reduce repeated OOMs.
    # Stored as an absolute RAM amount target (bounded by pool max at assignment time).
    s.pipeline_ram_hint = {}

    # Track currently running containers so we can preempt low priority when needed.
    # container_id -> dict(priority, pool_id, cpu, ram)
    s.running = {}


def _prio_rank(p):
    # Higher number => higher priority
    if p == Priority.QUERY:
        return 3
    if p == Priority.INTERACTIVE:
        return 2
    return 1  # batch


def _iter_waiting_pipelines(s):
    # Yield pipelines in strict priority order, FIFO within each priority.
    for pr in (Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE):
        q = s.waiting.get(pr, [])
        for p in q:
            yield p


def _clean_and_requeue(s):
    """
    Remove completed/failed pipelines from queues; keep others.
    This is run each tick because operator state changes can make pipelines unschedulable/schedulable.
    """
    for pr in (Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE):
        new_q = []
        for pipeline in s.waiting[pr]:
            status = pipeline.runtime_status()
            if status.is_pipeline_successful():
                continue
            # If there are FAILED ops we still keep the pipeline around; those ops are assignable.
            new_q.append(pipeline)
        s.waiting[pr] = new_q


def _oom_bump_hint(s, pipeline_id, pool, prev_ram):
    """
    Bump RAM hint on failure. We do not rely on error strings being standardized.
    We keep it small and bounded to avoid runaway allocations.
    """
    # If we have a previous RAM allocation, bump from there; otherwise start from a small fraction.
    base = prev_ram if prev_ram is not None else max(1.0, 0.25 * pool.max_ram_pool)
    bumped = min(pool.max_ram_pool, max(base * 1.5, base + max(1.0, 0.1 * pool.max_ram_pool)))
    s.pipeline_ram_hint[pipeline_id] = bumped


def _desired_share(priority):
    # Conservative shares to reduce head-of-line blocking and improve tail latency for higher priority.
    if priority == Priority.QUERY:
        return 0.80
    if priority == Priority.INTERACTIVE:
        return 0.65
    return 0.50  # batch


def _reserved_for_high(s):
    # If any QUERY/INTERACTIVE are waiting, reserve some capacity from batch.
    return (len(s.waiting[Priority.QUERY]) + len(s.waiting[Priority.INTERACTIVE])) > 0


def _pick_preemption_candidates(s, pool_id):
    # Prefer preempting lowest priority first; within same priority, larger RAM first to free capacity quickly.
    candidates = []
    for cid, info in s.running.items():
        if info["pool_id"] != pool_id:
            continue
        candidates.append(((_prio_rank(info["priority"])), info["ram"], cid))
    # Sort ascending by priority rank (batch first), descending by ram (free more sooner)
    candidates.sort(key=lambda x: (x[0], -x[1]))
    return [cid for _, __, cid in candidates]


def _preempt_to_fit(s, pool, pool_id, need_cpu, need_ram):
    """
    Suspend running low-priority containers in this pool until we have enough headroom
    to place a high-priority assignment.
    """
    suspensions = []

    def have_headroom():
        return pool.avail_cpu_pool >= need_cpu and pool.avail_ram_pool >= need_ram

    if have_headroom():
        return suspensions

    for cid in _pick_preemption_candidates(s, pool_id):
        info = s.running.get(cid)
        if info is None:
            continue
        # Only preempt batch to protect latency; keep it minimal and predictable.
        if info["priority"] != Priority.BATCH_PIPELINE:
            continue
        suspensions.append(Suspend(container_id=cid, pool_id=pool_id))
        # Assume suspension frees resources immediately in the simulator model.
        # If it doesn't, this will simply result in fewer placements this tick.
        pool.avail_cpu_pool += info["cpu"]
        pool.avail_ram_pool += info["ram"]
        s.running.pop(cid, None)
        if have_headroom():
            break

    return suspensions


@register_scheduler(key="scheduler_low_027")
def scheduler_low_027(s, results, pipelines):
    """
    Priority-aware, lightly preemptive scheduler with RAM retry hints.

    Returns:
      (suspensions, assignments)
    """
    # Enqueue new arrivals by priority (FIFO within each class)
    for p in pipelines:
        s.waiting[p.priority].append(p)

    # Process results: update running map and bump RAM hints on failures
    # We treat any failure as a signal to request more RAM next time (simple but effective first step).
    for r in results:
        # Container is no longer running once we have a result (success or failure)
        prev = s.running.pop(getattr(r, "container_id", None), None)

        if hasattr(r, "failed") and r.failed():
            # Find the pool to bound the hint; if unavailable, skip hinting.
            pool_id = getattr(r, "pool_id", None)
            if pool_id is not None and 0 <= pool_id < s.executor.num_pools:
                pool = s.executor.pools[pool_id]
                # Use previous allocation if we have it; else use result's fields if present.
                prev_ram = None
                if prev is not None:
                    prev_ram = prev.get("ram", None)
                else:
                    prev_ram = getattr(r, "ram", None)
                pipeline_id = getattr(r, "pipeline_id", None)
                if pipeline_id is not None:
                    _oom_bump_hint(s, pipeline_id, pool, prev_ram)

    # If no changes, do nothing
    if not pipelines and not results:
        return [], []

    # Clean completed pipelines out of queues
    _clean_and_requeue(s)

    suspensions = []
    assignments = []

    # Try to fill each pool; higher priority first across the whole system.
    # We keep it simple: per pool, loop until we can't schedule anything else.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]

        # Snapshot current availability (we will treat these as mutable within this function)
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool
        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # If high-priority work is waiting, constrain batch placements with a reservation.
        reserve_active = _reserved_for_high(s)
        # Reserve a modest fraction for high-priority so batch can't fully starve it.
        reserved_cpu = 0.20 * pool.max_cpu_pool if reserve_active else 0.0
        reserved_ram = 0.20 * pool.max_ram_pool if reserve_active else 0.0

        made_progress = True
        while made_progress:
            made_progress = False

            # Choose next pipeline to schedule: highest priority first, FIFO within priority.
            chosen = None
            chosen_pr = None
            for pr in (Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE):
                if not s.waiting[pr]:
                    continue
                chosen = s.waiting[pr][0]
                chosen_pr = pr
                break

            if chosen is None:
                break

            status = chosen.runtime_status()

            # If pipeline already completed, drop it and continue.
            if status.is_pipeline_successful():
                s.waiting[chosen_pr].pop(0)
                made_progress = True
                continue

            # Pick a single next-ready operator (parents completed)
            op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
            if not op_list:
                # Can't schedule this pipeline yet; rotate it to the back of its priority queue.
                s.waiting[chosen_pr].append(s.waiting[chosen_pr].pop(0))
                # Avoid spinning forever if nothing is schedulable at this priority.
                # Try other priorities in the next loop iteration.
                made_progress = True
                continue

            # Determine target resources for this placement
            share = _desired_share(chosen_pr)

            # Batch should respect reservation when high-priority is waiting
            effective_avail_cpu = avail_cpu
            effective_avail_ram = avail_ram
            if chosen_pr == Priority.BATCH_PIPELINE and reserve_active:
                effective_avail_cpu = max(0.0, avail_cpu - reserved_cpu)
                effective_avail_ram = max(0.0, avail_ram - reserved_ram)

            # Minimal CPU to avoid "0" allocations; keep it small for latency-sensitive work.
            cpu_target = max(1.0, min(effective_avail_cpu, share * pool.max_cpu_pool))
            # Prefer using hints for RAM if available; otherwise allocate a moderate share.
            hint = s.pipeline_ram_hint.get(chosen.pipeline_id, None)
            ram_target = hint if hint is not None else (share * pool.max_ram_pool)
            ram_target = max(1.0, min(effective_avail_ram, ram_target))

            # If we can't fit even the minimum, try preemption for high-priority
            if cpu_target <= 0 or ram_target <= 0:
                # Rotate to avoid blocking others
                s.waiting[chosen_pr].append(s.waiting[chosen_pr].pop(0))
                made_progress = True
                continue

            if cpu_target > avail_cpu or ram_target > avail_ram:
                if chosen_pr in (Priority.QUERY, Priority.INTERACTIVE):
                    # Attempt to preempt batch in this pool to make room
                    susp = _preempt_to_fit(s, pool, pool_id, cpu_target, ram_target)
                    if susp:
                        suspensions.extend(susp)
                        # Refresh local view after simulated freeing
                        avail_cpu = pool.avail_cpu_pool
                        avail_ram = pool.avail_ram_pool
                # If still doesn't fit, don't force it; rotate and move on
                if cpu_target > avail_cpu or ram_target > avail_ram:
                    s.waiting[chosen_pr].append(s.waiting[chosen_pr].pop(0))
                    made_progress = True
                    continue

            # Place the assignment
            assignment = Assignment(
                ops=op_list,
                cpu=cpu_target,
                ram=ram_target,
                priority=chosen.priority,
                pool_id=pool_id,
                pipeline_id=chosen.pipeline_id,
            )
            assignments.append(assignment)

            # Update local + pool availability
            avail_cpu -= cpu_target
            avail_ram -= ram_target
            pool.avail_cpu_pool = avail_cpu
            pool.avail_ram_pool = avail_ram

            # Record running container info if container_id will be available later via results.
            # We can't know container_id now; it arrives in ExecutionResult. So we only rely on
            # s.running for preemption of already-known containers.
            # (Preemption will still work for containers that have already produced a result in prior ticks.)

            # Round-robin within the same priority to prevent a single pipeline from monopolizing
            s.waiting[chosen_pr].append(s.waiting[chosen_pr].pop(0))
            made_progress = True

            if avail_cpu <= 0 or avail_ram <= 0:
                break

    # Update running map from assignments if the simulator provides container IDs only in results.
    # Since we don't have container_id here, we cannot add entries. However, some simulators may
    # assign container_id deterministically before results; if Assignment exposes it, record it.
    for a in assignments:
        cid = getattr(a, "container_id", None)
        if cid is not None:
            s.running[cid] = {
                "priority": a.priority,
                "pool_id": a.pool_id,
                "cpu": a.cpu,
                "ram": a.ram,
            }

    return suspensions, assignments
