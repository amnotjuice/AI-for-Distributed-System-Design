# policy_key: scheduler_low_039
# reasoning_effort: low
# model: gpt-5.2-2025-12-11
# llm_cost: 0.035867
# generation_seconds: 36.39
# generated_at: 2026-03-14T02:32:27.278460
@register_scheduler_init(key="scheduler_low_039")
def scheduler_low_039_init(s):
    """
    Priority-aware, latency-oriented scheduler (incremental improvement over naive FIFO).

    Key ideas (kept intentionally simple/robust):
      1) Maintain separate FIFO queues per priority and always consider higher priority first.
      2) Avoid "give everything to one op" by slicing CPU/RAM per assignment so multiple ops can run concurrently.
      3) Adaptive RAM retry: if an op fails (likely OOM), increase the pipeline's requested RAM for subsequent attempts.
      4) Gentle anti-starvation: occasionally allow a batch pipeline to run even under sustained interactive load.
    """
    # Per-priority waiting queues (FIFO within each class)
    s.waiting = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }

    # Adaptive sizing state per pipeline
    s.pipeline_ram_req = {}  # pipeline_id -> ram to request
    s.pipeline_cpu_req = {}  # pipeline_id -> cpu to request

    # Batch aging / anti-starvation
    s.tick = 0
    s.batch_boost_every = 8  # every N scheduling ticks, allow one batch pick earlier


def _prio_rank(priority):
    # Lower number = higher priority
    if priority == Priority.QUERY:
        return 0
    if priority == Priority.INTERACTIVE:
        return 1
    return 2


def _iter_priority_order_with_aging(s):
    # Normally: QUERY -> INTERACTIVE -> BATCH
    # Occasionally: QUERY -> BATCH -> INTERACTIVE (to ensure some batch progress)
    s.tick += 1
    if s.tick % s.batch_boost_every == 0:
        return [Priority.QUERY, Priority.BATCH_PIPELINE, Priority.INTERACTIVE]
    return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]


def _default_slices_for_priority(pool, priority):
    """
    Return (cpu_slice, ram_slice) baseline for this priority.
    These are conservative slices to improve latency via concurrency while still
    favoring high priority with larger slices.
    """
    # CPU slices: ensure >=1 vCPU when possible.
    if priority == Priority.QUERY:
        cpu_frac = 0.60
        ram_frac = 0.60
    elif priority == Priority.INTERACTIVE:
        cpu_frac = 0.45
        ram_frac = 0.50
    else:
        cpu_frac = 0.25
        ram_frac = 0.30

    cpu_slice = max(1.0, pool.max_cpu_pool * cpu_frac) if pool.max_cpu_pool > 0 else 0.0
    ram_slice = pool.max_ram_pool * ram_frac if pool.max_ram_pool > 0 else 0.0
    return cpu_slice, ram_slice


def _clamp(v, lo, hi):
    if v < lo:
        return lo
    if v > hi:
        return hi
    return v


def _record_failure_and_bump(s, res, pool):
    """
    If a container failed, bump the pipeline's requested RAM/CPU for next retry.
    We treat any failure as a signal to increase RAM first; CPU is bumped mildly.
    """
    # Find pipeline id via ops if present; fall back to None-safe.
    pipeline_id = None
    try:
        if res.ops and len(res.ops) > 0:
            pipeline_id = res.ops[0].pipeline_id
    except Exception:
        pipeline_id = None

    if pipeline_id is None:
        return

    # Increase RAM request aggressively (OOM-safe), CPU slightly.
    prev_ram = s.pipeline_ram_req.get(pipeline_id, res.ram if res.ram else 0.0)
    prev_cpu = s.pipeline_cpu_req.get(pipeline_id, res.cpu if res.cpu else 0.0)

    # If we have no signal, start from a reasonable baseline.
    if prev_ram <= 0:
        prev_ram = max(0.1 * pool.max_ram_pool, 1.0) if pool.max_ram_pool > 0 else 1.0
    if prev_cpu <= 0:
        prev_cpu = 1.0

    bumped_ram = prev_ram * 1.8
    bumped_cpu = prev_cpu * 1.2

    # Clamp to pool maxima (can't exceed a pool anyway)
    s.pipeline_ram_req[pipeline_id] = _clamp(bumped_ram, 1.0, pool.max_ram_pool)
    s.pipeline_cpu_req[pipeline_id] = _clamp(bumped_cpu, 1.0, pool.max_cpu_pool)


def _enqueue_pipeline(s, p):
    # Only enqueue once; if already present, keep original position (FIFO).
    q = s.waiting[p.priority]
    for existing in q:
        if existing.pipeline_id == p.pipeline_id:
            return
    q.append(p)


def _pop_next_runnable_pipeline(s, priority, require_parents_complete=True):
    """
    Pop (and return) the next pipeline of this priority that has an assignable op ready.
    Pipelines without ready ops are rotated to the back to avoid head-of-line blocking.
    """
    q = s.waiting[priority]
    if not q:
        return None, None

    n = len(q)
    for _ in range(n):
        p = q.pop(0)
        status = p.runtime_status()

        # Drop completed pipelines
        if status.is_pipeline_successful():
            continue

        # Prefer ready ops; FAILED is included in ASSIGNABLE_STATES (retry is allowed)
        op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=require_parents_complete)[:1]
        if op_list:
            return p, op_list

        # Not runnable yet: rotate to back
        q.append(p)

    return None, None


@register_scheduler(key="scheduler_low_039")
def scheduler_low_039(s, results, pipelines):
    """
    Scheduler step:
      - ingest new pipelines into per-priority queues
      - update adaptive sizing based on failures
      - assign as many ready ops as possible per pool using priority order and resource slices
    """
    # Ingest new pipelines
    for p in pipelines:
        _enqueue_pipeline(s, p)

    # If nothing changes, do nothing
    if not pipelines and not results:
        return [], []

    suspensions = []  # No preemption in this "small improvement" iteration (keeps it safe)
    assignments = []

    # Update sizing hints from failures (best-effort; pool chosen from result)
    for res in results:
        try:
            if res.failed():
                pool = s.executor.pools[res.pool_id]
                _record_failure_and_bump(s, res, pool)
        except Exception:
            # If the simulator/result format differs, don't break scheduling.
            pass

    # Assign per pool: fill available resources with multiple small assignments, prioritizing latency-sensitive work.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool

        # If pool is essentially full, skip
        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # Attempt to pack multiple assignments into this pool
        made_progress = True
        while made_progress:
            made_progress = False

            # Stop if we can't place even a minimal container
            if avail_cpu < 1.0 or avail_ram < 1.0:
                break

            # Pick next runnable pipeline in priority order (with mild aging)
            chosen_p = None
            chosen_ops = None
            for prio in _iter_priority_order_with_aging(s):
                p, op_list = _pop_next_runnable_pipeline(s, prio, require_parents_complete=True)
                if p is not None and op_list is not None:
                    chosen_p, chosen_ops = p, op_list
                    break

            if chosen_p is None:
                break

            pid = chosen_p.pipeline_id
            prio = chosen_p.priority

            # Determine resource request:
            # - start from priority-based slices
            # - override with per-pipeline bumped requirements if present
            base_cpu, base_ram = _default_slices_for_priority(pool, prio)
            req_cpu = s.pipeline_cpu_req.get(pid, base_cpu)
            req_ram = s.pipeline_ram_req.get(pid, base_ram)

            # Clamp to what's available now and pool capacity
            req_cpu = _clamp(req_cpu, 1.0, min(pool.max_cpu_pool, avail_cpu))
            req_ram = _clamp(req_ram, 1.0, min(pool.max_ram_pool, avail_ram))

            # If we still can't fit, try a smaller "minimum viable" allocation for this tick
            # (keeps the system moving; failures will bump sizing next time)
            if req_cpu > avail_cpu or req_ram > avail_ram:
                req_cpu = _clamp(1.0, 1.0, avail_cpu)
                req_ram = _clamp(1.0, 1.0, avail_ram)

            # Create assignment
            assignment = Assignment(
                ops=chosen_ops,
                cpu=req_cpu,
                ram=req_ram,
                priority=prio,
                pool_id=pool_id,
                pipeline_id=pid,
            )
            assignments.append(assignment)

            # Update available resources and re-enqueue pipeline for remaining ops
            avail_cpu -= req_cpu
            avail_ram -= req_ram

            _enqueue_pipeline(s, chosen_p)
            made_progress = True

    return suspensions, assignments

