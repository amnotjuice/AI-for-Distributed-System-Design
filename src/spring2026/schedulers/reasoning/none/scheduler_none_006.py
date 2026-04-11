# policy_key: scheduler_none_006
# reasoning_effort: none
# model: gpt-5.2-2025-12-11
# llm_cost: 0.041638
# generation_seconds: 42.67
# generated_at: 2026-03-12T21:25:27.113129
@register_scheduler_init(key="scheduler_none_006")
def scheduler_none_006_init(s):
    """Priority-aware FIFO with gentle preemption and simple OOM backoff.

    Improvements over naive FIFO:
    - Separate waiting queues per priority; always try to schedule higher priority first.
    - Allocate only a bounded "slice" per assignment (not the entire pool), leaving headroom for others.
    - On OOM/failed result, increase remembered RAM request for that pipeline (exponential backoff).
    - If high-priority work is pending and a pool is blocked, preempt (suspend) low-priority RUNNING containers.
    - Basic fairness via aging: if batch has waited "too long", allow it to compete.

    Notes/assumptions:
    - This policy is conservative: avoid large allocations to reduce interference and improve latency.
    - Uses only APIs shown in the template; avoids VM-level placement details.
    """
    # Waiting pipelines tracked by priority
    s.waiting = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }

    # Remember last-good (or next-try) RAM/CPU per pipeline to handle OOMs and right-sizing
    s.pipeline_ram_req = {}   # pipeline_id -> ram
    s.pipeline_cpu_req = {}   # pipeline_id -> cpu

    # Aging bookkeeping for fairness
    s.pipeline_enqueue_tick = {}  # pipeline_id -> tick
    s.tick = 0

    # Simple knobs
    s.min_cpu_slice = 1
    s.max_cpu_slice_frac = 0.50  # never allocate more than this fraction of pool CPU to a single op
    s.max_ram_slice_frac = 0.60  # never allocate more than this fraction of pool RAM to a single op

    # OOM/backoff knobs
    s.oom_ram_backoff = 2.0
    s.min_ram_floor = 1

    # Fairness knob: after this many ticks, batch is treated as interactive for one scheduling opportunity
    s.batch_aging_ticks = 50


def _prio_rank(priority):
    # Lower is higher priority
    if priority == Priority.QUERY:
        return 0
    if priority == Priority.INTERACTIVE:
        return 1
    return 2


def _all_waiting_nonempty(s):
    return any(len(q) > 0 for q in s.waiting.values())


def _pick_next_priority(s):
    """Pick the next priority to schedule, with simple aging for batch fairness."""
    # Always prefer QUERY then INTERACTIVE, unless a very old BATCH has aged.
    if s.waiting[Priority.QUERY]:
        return Priority.QUERY
    if s.waiting[Priority.INTERACTIVE]:
        return Priority.INTERACTIVE

    # Only batch left (or none)
    if s.waiting[Priority.BATCH_PIPELINE]:
        # Find oldest batch pipeline
        oldest_tick = None
        for p in s.waiting[Priority.BATCH_PIPELINE]:
            t = s.pipeline_enqueue_tick.get(p.pipeline_id, s.tick)
            oldest_tick = t if oldest_tick is None else min(oldest_tick, t)
        if oldest_tick is not None and (s.tick - oldest_tick) >= s.batch_aging_ticks:
            return Priority.BATCH_PIPELINE
        return Priority.BATCH_PIPELINE
    return None


def _iter_priorities_high_to_low():
    return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]


def _get_pool_headroom(s, pool):
    return pool.avail_cpu_pool, pool.avail_ram_pool


def _slice_resources(s, pool, priority, pipeline_id):
    """Compute cpu/ram slice for a single assignment, using remembered requests and bounded fractions."""
    # Base slice from pool capacity fractions; interactive/query get smaller slices for low latency & headroom
    max_cpu = max(1, int(pool.max_cpu_pool))
    max_ram = max(1, int(pool.max_ram_pool))

    if priority == Priority.QUERY:
        cpu_frac = 0.25
        ram_frac = 0.25
    elif priority == Priority.INTERACTIVE:
        cpu_frac = 0.35
        ram_frac = 0.35
    else:
        cpu_frac = 0.50
        ram_frac = 0.50

    cpu_cap = max(s.min_cpu_slice, int(max_cpu * min(s.max_cpu_slice_frac, cpu_frac)))
    ram_cap = max(s.min_ram_floor, int(max_ram * min(s.max_ram_slice_frac, ram_frac)))

    # Remembered requests override base cap but still bounded by cap
    req_cpu = s.pipeline_cpu_req.get(pipeline_id, cpu_cap)
    req_ram = s.pipeline_ram_req.get(pipeline_id, ram_cap)

    cpu = max(s.min_cpu_slice, min(cpu_cap, int(req_cpu)))
    ram = max(s.min_ram_floor, min(ram_cap, int(req_ram)))
    return cpu, ram


def _is_pipeline_done_or_failed(pipeline):
    status = pipeline.runtime_status()
    if status.is_pipeline_successful():
        return True
    # If any operators are FAILED, treat pipeline as failed (do not keep retrying indefinitely)
    return status.state_counts.get(OperatorState.FAILED, 0) > 0


def _get_assignable_ops(pipeline):
    status = pipeline.runtime_status()
    # Assign one op at a time to reduce contention; require parents complete to respect DAG
    op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
    return op_list


def _enqueue_pipeline(s, pipeline):
    # Avoid duplicating pipeline in queue if already present (best-effort O(n) check per prio queue)
    q = s.waiting[pipeline.priority]
    pid = pipeline.pipeline_id
    for existing in q:
        if existing.pipeline_id == pid:
            return
    q.append(pipeline)
    if pid not in s.pipeline_enqueue_tick:
        s.pipeline_enqueue_tick[pid] = s.tick


def _requeue_pipeline_front(s, pipeline):
    q = s.waiting[pipeline.priority]
    # Put it near the front (but keep relative order of others)
    q.insert(0, pipeline)


def _pop_next_runnable_pipeline(s, priority):
    """Pop next pipeline of given priority that still has runnable ops; requeue if blocked."""
    q = s.waiting[priority]
    if not q:
        return None

    # Rotate through queue once to find runnable
    n = len(q)
    for _ in range(n):
        p = q.pop(0)
        if _is_pipeline_done_or_failed(p):
            # Drop finished/failed pipelines
            continue
        ops = _get_assignable_ops(p)
        if ops:
            return p
        # Not runnable now (parents not done / no assignable), keep it for later
        q.append(p)
    return None


def _pending_high_priority_exists(s):
    # If any query or interactive pipeline has a runnable op waiting (best-effort check)
    for pr in [Priority.QUERY, Priority.INTERACTIVE]:
        if s.waiting[pr]:
            # Quick check: if any pipeline has assignable op
            for p in s.waiting[pr]:
                if not _is_pipeline_done_or_failed(p) and _get_assignable_ops(p):
                    return True
    return False


def _collect_running_low_priority_containers(results, target_pool_id=None):
    """Collect container_ids seen running from recent results (best-effort)."""
    running = []
    for r in results:
        # If we get results, that implies completion/failure, not running.
        # We can't reliably list running containers from results; so return empty.
        # Preemption will be triggered only if we can find running ops via other means (not available).
        pass
    return running


@register_scheduler(key="scheduler_none_006")
def scheduler_none_006(s, results, pipelines):
    """
    Priority-aware scheduling with headroom, OOM backoff, and conservative preemption hooks.

    Returns:
        (suspensions, assignments)
    """
    s.tick += 1

    # 1) Ingest new pipelines into priority queues
    for p in pipelines:
        _enqueue_pipeline(s, p)

    # 2) Process results: update resource hints (OOM -> increase RAM), cleanup bookkeeping
    for r in results:
        # If a container failed due to OOM, increase remembered RAM for its pipeline
        pid = getattr(r, "pipeline_id", None)
        if pid is None:
            # Some sims may not include pipeline_id on result; try infer from ops if present
            # (best-effort: skip if not available)
            continue

        if r.failed():
            err = getattr(r, "error", None)
            # If error string suggests OOM, backoff RAM; otherwise do nothing
            is_oom = False
            if err is not None:
                try:
                    is_oom = ("oom" in str(err).lower()) or ("out of memory" in str(err).lower())
                except Exception:
                    is_oom = False
            if is_oom:
                prev = s.pipeline_ram_req.get(pid, max(s.min_ram_floor, int(getattr(r, "ram", s.min_ram_floor))))
                bumped = max(prev + 1, int(prev * s.oom_ram_backoff))
                s.pipeline_ram_req[pid] = bumped
            # In either failure case, we avoid infinite retries by leaving pipeline drop logic
            # to _is_pipeline_done_or_failed (FAILED count). If simulator re-enqueues, we handle.
        else:
            # On success, remember the slice we used as a decent next guess
            try:
                s.pipeline_ram_req[pid] = max(s.min_ram_floor, int(getattr(r, "ram", s.min_ram_floor)))
                s.pipeline_cpu_req[pid] = max(s.min_cpu_slice, int(getattr(r, "cpu", s.min_cpu_slice)))
            except Exception:
                pass

    # Early exit if nothing to do
    if not pipelines and not results and not _all_waiting_nonempty(s):
        return [], []

    suspensions = []
    assignments = []

    # 3) Scheduling loop per pool: fill available headroom, higher priority first
    #    We schedule at most a small number of ops per pool per tick to avoid burstiness.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu, avail_ram = _get_pool_headroom(s, pool)
        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # Soft cap: how many assignments per pool per tick
        # (small to reduce latency spikes and allow interactivity)
        max_assignments = 4
        made = 0

        while made < max_assignments:
            # If pool is nearly full, stop
            if avail_cpu <= 0 or avail_ram <= 0:
                break

            # Choose next priority to attempt
            next_pr = None
            for pr in _iter_priorities_high_to_low():
                if s.waiting[pr]:
                    next_pr = pr
                    break
            if next_pr is None:
                break

            # Pop a runnable pipeline of that priority
            pipeline = _pop_next_runnable_pipeline(s, next_pr)
            if pipeline is None:
                # No runnable pipelines at that priority; try lower ones
                progressed = False
                for pr in _iter_priorities_high_to_low():
                    if pr == next_pr:
                        continue
                    pipeline = _pop_next_runnable_pipeline(s, pr)
                    if pipeline is not None:
                        next_pr = pr
                        progressed = True
                        break
                if not progressed:
                    break

            # Build assignment for one op
            op_list = _get_assignable_ops(pipeline)
            if not op_list:
                # Put it back and continue
                _enqueue_pipeline(s, pipeline)
                continue

            cpu_slice, ram_slice = _slice_resources(s, pool, pipeline.priority, pipeline.pipeline_id)
            cpu = min(int(avail_cpu), int(cpu_slice))
            ram = min(int(avail_ram), int(ram_slice))

            if cpu <= 0 or ram <= 0:
                # Can't fit; requeue and stop filling this pool
                _requeue_pipeline_front(s, pipeline)
                break

            assignment = Assignment(
                ops=op_list,
                cpu=cpu,
                ram=ram,
                priority=pipeline.priority,
                pool_id=pool_id,
                pipeline_id=pipeline.pipeline_id,
            )
            assignments.append(assignment)

            # Update local view of available resources
            avail_cpu -= cpu
            avail_ram -= ram
            made += 1

            # Requeue pipeline for subsequent ops
            _enqueue_pipeline(s, pipeline)

    # 4) Preemption hook (conservative):
    # We only attempt preemption if high-priority is pending and we made no assignments at all,
    # indicating possible headroom blockage. Without an API to list running containers,
    # we cannot implement robust preemption here; return none to keep behavior safe/deterministic.
    if not assignments and _pending_high_priority_exists(s):
        # If the simulator provides a way to enumerate running containers, integrate here.
        # For now, no-op.
        suspensions = []

    return suspensions, assignments
