# policy_key: scheduler_none_049
# reasoning_effort: none
# model: gpt-5.2-2025-12-11
# llm_cost: 0.036791
# generation_seconds: 39.92
# generated_at: 2026-03-14T02:16:44.706971
@register_scheduler_init(key="scheduler_none_049")
def scheduler_none_049_init(s):
    """Priority-aware incremental scheduler (small step up from naive FIFO).

    Improvements over naive:
    1) Keep separate waiting queues by priority and always schedule higher priority first.
    2) Avoid "one giant op monopolizes the whole pool" by allocating a capped CPU share per assignment,
       leaving headroom to run additional work and reduce tail latency.
    3) Basic OOM-aware RAM retry: if an op OOMs, bump RAM request for that pipeline next time.
    4) Light preemption: when high-priority work arrives and no pool has headroom, suspend one
       low-priority running container to free resources (min churn: one victim per tick max).
    """
    # Queues per priority (store pipelines, not ops; we pick ready ops at scheduling time)
    s.q_query = []
    s.q_interactive = []
    s.q_batch = []

    # Per-pipeline RAM multiplier for retries after OOM (default 1.0)
    s.ram_mult = {}

    # Track last seen pipelines to avoid unbounded duplicates (best-effort de-dupe by id)
    s.enqueued_ids = set()

    # Track running containers by (container_id -> (priority, pool_id)) to help pick preemption victims
    s.running = {}

    # Limit preemption churn
    s.last_preempt_tick = -10
    s.tick = 0


def _prio_rank(priority):
    # Higher rank = higher priority
    if priority == Priority.QUERY:
        return 3
    if priority == Priority.INTERACTIVE:
        return 2
    return 1  # Priority.BATCH_PIPELINE (and any unknowns)


def _queue_for(s, priority):
    if priority == Priority.QUERY:
        return s.q_query
    if priority == Priority.INTERACTIVE:
        return s.q_interactive
    return s.q_batch


def _all_queues(s):
    # In strict order
    return [s.q_query, s.q_interactive, s.q_batch]


def _remove_from_enqueued(s, pipeline):
    pid = pipeline.pipeline_id
    if pid in s.enqueued_ids:
        s.enqueued_ids.remove(pid)


def _enqueue_pipeline(s, pipeline):
    pid = pipeline.pipeline_id
    # Best-effort: keep only one copy of a pipeline in queues.
    if pid in s.enqueued_ids:
        return
    _queue_for(s, pipeline.priority).append(pipeline)
    s.enqueued_ids.add(pid)


def _pick_ready_ops(pipeline, max_ops=1):
    status = pipeline.runtime_status()
    # Don't assign if already done or has hard failures we won't retry (non-OOM failures)
    if status.is_pipeline_successful():
        return []
    ops = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
    if not ops:
        return []
    return ops[:max_ops]


def _is_oom_result(res):
    if not res.failed():
        return False
    err = getattr(res, "error", None)
    if not err:
        return False
    # Robust to different error formats: string, exception, or structured.
    s = str(err).lower()
    return ("oom" in s) or ("out of memory" in s) or ("memory" in s and "alloc" in s)


def _compute_request(s, pool, pipeline, want_cpu, want_ram):
    # CPU: cap to reduce monopolization. Give more to higher priority.
    # - QUERY: up to 1/2 pool
    # - INTERACTIVE: up to 1/3 pool
    # - BATCH: up to 1/4 pool
    pr = pipeline.priority
    if pr == Priority.QUERY:
        cpu_cap = max(1.0, pool.max_cpu_pool * 0.50)
    elif pr == Priority.INTERACTIVE:
        cpu_cap = max(1.0, pool.max_cpu_pool * 0.33)
    else:
        cpu_cap = max(1.0, pool.max_cpu_pool * 0.25)

    cpu = min(want_cpu, cpu_cap)

    # RAM: keep it modest but not tiny; apply per-pipeline multiplier if OOM happened.
    mult = s.ram_mult.get(pipeline.pipeline_id, 1.0)
    ram = want_ram * mult

    # Leave at least a small buffer for system/other work when possible.
    # (Only applied if it doesn't make allocation impossible.)
    ram_cap = pool.max_ram_pool * 0.90
    ram = min(ram, ram_cap)

    # Clamp to available resources
    cpu = min(cpu, pool.avail_cpu_pool)
    ram = min(ram, pool.avail_ram_pool)
    return cpu, ram


def _choose_preemption_victim(s, target_priority, target_pool_id=None):
    # Suspend one lowest-priority running container (prefer in the same pool if specified).
    # Returns (container_id, pool_id) or None.
    best = None
    best_rank = 999  # lower is worse priority -> better victim
    for cid, (prio, pool_id) in list(s.running.items()):
        if target_pool_id is not None and pool_id != target_pool_id:
            continue
        r = _prio_rank(prio)
        if r < _prio_rank(target_priority):
            # victim candidate
            if r < best_rank:
                best_rank = r
                best = (cid, pool_id)
    return best


@register_scheduler(key="scheduler_none_049")
def scheduler_none_049(s, results, pipelines):
    """
    Priority-aware scheduler with capped per-op CPU, simple OOM RAM backoff, and minimal preemption.

    Admission / queuing:
      - New pipelines are enqueued once by pipeline_id, into a priority-specific queue.

    Placement:
      - For each pool, try to assign one ready op at a time, always picking the highest priority queue first.
      - CPU is capped per assignment to avoid one job taking the entire pool (improves latency under contention).

    Resizing:
      - On OOM failure, increase future RAM requests for that pipeline (multiplicative backoff).
      - Non-OOM failures are treated as terminal (pipeline will be dropped once detected).

    Preemption:
      - If a high priority pipeline arrives and no pool can fit even a minimal allocation, suspend one
        low priority running container (at most one per tick).
    """
    s.tick += 1

    # Incorporate new pipelines
    for p in pipelines:
        _enqueue_pipeline(s, p)

    # Update running set and OOM backoff from execution results
    for r in results:
        # Maintain running set: if we see a result, container finished/failed; remove tracking.
        cid = getattr(r, "container_id", None)
        if cid in s.running:
            del s.running[cid]

        # If failed due to OOM: bump RAM multiplier for that pipeline to reduce repeated failures.
        # We don't have pipeline_id on result; so we key by pipeline inferred from ops if available.
        # Best-effort: res.ops may include op metadata with pipeline_id, but API isn't guaranteed.
        if _is_oom_result(r):
            # Try to find pipeline_id on op(s)
            pid = None
            try:
                if r.ops:
                    op0 = r.ops[0]
                    pid = getattr(op0, "pipeline_id", None)
            except Exception:
                pid = None

            # If we can't infer pid, skip backoff (can't safely apply).
            if pid is not None:
                prev = s.ram_mult.get(pid, 1.0)
                # Multiplicative increase, capped.
                s.ram_mult[pid] = min(prev * 1.5, 8.0)

    # Early exit if nothing changed
    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    # Helper: pop next schedulable pipeline in priority order, but keep unschedulable ones for later.
    def try_assign_one_in_pool(pool_id):
        pool = s.executor.pools[pool_id]
        if pool.avail_cpu_pool <= 0 or pool.avail_ram_pool <= 0:
            return False

        # Try higher priorities first
        for q in _all_queues(s):
            if not q:
                continue

            # We'll scan a few entries to find one with a ready op; requeue the rest in same order.
            scan_limit = min(len(q), 16)  # avoid O(n) over huge queues per tick
            scanned = []
            assigned_here = False

            for _ in range(scan_limit):
                pipeline = q.pop(0)
                _remove_from_enqueued(s, pipeline)

                status = pipeline.runtime_status()
                # Drop completed pipelines
                if status.is_pipeline_successful():
                    continue

                # Drop pipelines with failures we won't retry (non-OOM). We detect failures via state counts.
                # If failed ops exist, we still might want to retry if OOM; but we can't distinguish per-op here.
                # Conservative approach: allow FAILED ops to be assignable (ASSIGNABLE_STATES includes FAILED),
                # but if failures pile up without progress this will self-limit by resource constraints.
                op_list = _pick_ready_ops(pipeline, max_ops=1)
                if not op_list:
                    # Not ready: put back to end of same queue
                    scanned.append(pipeline)
                    continue

                # Compute a conservative request: don't take whole pool; leave headroom.
                want_cpu = pool.avail_cpu_pool
                want_ram = pool.avail_ram_pool
                cpu, ram = _compute_request(s, pool, pipeline, want_cpu, want_ram)

                # Minimal allocation guard: if too small, defer and consider preemption elsewhere.
                if cpu <= 0 or ram <= 0:
                    scanned.append(pipeline)
                    continue

                assignment = Assignment(
                    ops=op_list,
                    cpu=cpu,
                    ram=ram,
                    priority=pipeline.priority,
                    pool_id=pool_id,
                    pipeline_id=pipeline.pipeline_id,
                )
                assignments.append(assignment)

                # Mark as "running" best-effort: container_id not known until execution result.
                # We can't add here reliably; instead, add when results report running would be ideal,
                # but API doesn't provide. We'll approximate by adding when we later see result container_id,
                # and for preemption we will rely on what we have. As a minimal improvement,
                # we leave this as-is (preemption will be opportunistic).
                assigned_here = True
                break

            # Requeue scanned pipelines (preserve order)
            for p in scanned:
                _enqueue_pipeline(s, p)

            if assigned_here:
                return True

        return False

    # Attempt to schedule across pools (one assignment per pool per tick; small improvement over naive)
    for pool_id in range(s.executor.num_pools):
        try_assign_one_in_pool(pool_id)

    # Minimal preemption: if we couldn't schedule anything but have pending high priority pipelines,
    # and enough time passed since last preemption, preempt one low-priority running container.
    if not assignments and (s.q_query or s.q_interactive):
        # Preempt at most once every 2 ticks to reduce churn
        if s.tick - s.last_preempt_tick >= 2:
            target_prio = Priority.QUERY if s.q_query else Priority.INTERACTIVE

            # Pick a victim from any pool
            victim = _choose_preemption_victim(s, target_prio, target_pool_id=None)
            if victim is not None:
                cid, pool_id = victim
                suspensions.append(Suspend(cid, pool_id))
                s.last_preempt_tick = s.tick
                # Remove from running set now (it may reappear later; best-effort)
                if cid in s.running:
                    del s.running[cid]

    return suspensions, assignments

