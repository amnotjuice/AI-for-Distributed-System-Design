# policy_key: scheduler_low_047
# reasoning_effort: low
# model: gpt-5.2-2025-12-11
# llm_cost: 0.038709
# generation_seconds: 44.27
# generated_at: 2026-03-14T02:38:38.316504
@register_scheduler_init(key="scheduler_low_047")
def scheduler_low_047_init(s):
    """Priority-aware FIFO with simple per-op retry/oom backoff.

    Improvements over naive FIFO:
      1) Maintain separate queues per priority and always prefer higher priority for admission.
      2) Avoid head-of-line blocking by rotating through queues until we find a pipeline with a ready op.
      3) Right-size allocations by priority using pool capacity "shares" (interactive gets more).
      4) On failure (esp. OOM), retry the same op with increased RAM up to a small cap.
    """
    from collections import deque

    # Per-priority pipeline queues (FIFO within each class).
    s.q_query = deque()
    s.q_interactive = deque()
    s.q_batch = deque()

    # Retry counters per (pipeline_id, op_key).
    s.op_retries = {}

    # Resource backoff factors per (pipeline_id, op_key).
    # Start with 1.0; on suspected OOM -> multiply by 2.0 (capped).
    s.op_ram_factor = {}

    # Keep small to avoid infinite retry loops in simulation.
    s.max_retries_per_op = 3

    # Cap RAM backoff to avoid requesting absurd sizes.
    s.max_ram_factor = 8.0

    # Cache pipelines by id so we can re-enqueue reliably.
    s.pipeline_by_id = {}


def _prio_rank(priority):
    # Lower rank = higher priority
    if priority == Priority.QUERY:
        return 0
    if priority == Priority.INTERACTIVE:
        return 1
    return 2  # Priority.BATCH_PIPELINE and everything else


def _get_queue(s, priority):
    if priority == Priority.QUERY:
        return s.q_query
    if priority == Priority.INTERACTIVE:
        return s.q_interactive
    return s.q_batch


def _suspected_oom(err):
    if not err:
        return False
    e = str(err).lower()
    return ("oom" in e) or ("out of memory" in e) or ("memoryerror" in e) or ("cuda out of memory" in e)


def _op_key(op):
    # Try to use stable identifiers if present; fall back to object id.
    for attr in ("op_id", "operator_id", "id", "name"):
        if hasattr(op, attr):
            v = getattr(op, attr)
            if v is not None:
                return (attr, str(v))
    return ("py_id", str(id(op)))


def _pop_next_runnable_pipeline(s, max_scans=64):
    """Pick the next pipeline (by priority) that has a runnable op ready.

    We rotate queues to avoid permanent head-of-line blocking if the front
    pipeline is waiting on dependencies.
    """
    queues = [s.q_query, s.q_interactive, s.q_batch]
    scanned = 0

    for q in queues:
        # Scan at most len(q) to preserve FIFO-ish ordering.
        for _ in range(min(len(q), max_scans - scanned)):
            scanned += 1
            p = q.popleft()
            if p is None:
                continue
            status = p.runtime_status()
            if status.is_pipeline_successful():
                continue

            # Only schedule if at least one op is runnable (parents complete).
            op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
            if op_list:
                return p

            # Not runnable yet; rotate to back.
            q.append(p)

            if scanned >= max_scans:
                break
        if scanned >= max_scans:
            break

    return None


def _choose_allocation(pool, priority, avail_cpu, avail_ram, ram_factor):
    """Simple priority-based right-sizing using pool capacity shares."""
    # Shares are fractions of total pool capacity, biased toward low latency.
    if priority == Priority.QUERY:
        cpu_share, ram_share = 0.60, 0.60
    elif priority == Priority.INTERACTIVE:
        cpu_share, ram_share = 0.50, 0.50
    else:
        cpu_share, ram_share = 0.25, 0.25

    # Base request from pool max, then clamp to current availability.
    req_cpu = pool.max_cpu_pool * cpu_share
    req_ram = (pool.max_ram_pool * ram_share) * float(ram_factor)

    # Ensure some minimums so we don't assign near-zero slices.
    # (Types vary across simulators; keep numeric operations generic.)
    req_cpu = max(req_cpu, 1)
    req_ram = max(req_ram, 1)

    # Clamp to available.
    cpu = min(avail_cpu, req_cpu)
    ram = min(avail_ram, req_ram)

    return cpu, ram


@register_scheduler(key="scheduler_low_047")
def scheduler_low_047_scheduler(s, results, pipelines):
    """
    Priority-aware scheduler with per-op OOM backoff and bounded retries.

    Behavior:
      - Enqueue new pipelines into a queue based on priority.
      - Process execution results:
          * On suspected OOM failure -> increase RAM factor for that op and retry (up to limit).
          * On other failures -> retry a limited number of times without backoff (still bounded).
      - For each pool, greedily assign one runnable op at a time, favoring higher priority queues.
    """
    # Track new pipelines.
    for p in pipelines:
        s.pipeline_by_id[p.pipeline_id] = p
        _get_queue(s, p.priority).append(p)

    # Early exit if nothing changed.
    if not pipelines and not results:
        return [], []

    # Update retry/backoff state based on results.
    for r in results:
        if not r or not getattr(r, "ops", None):
            continue

        # We only handle failures with retry/backoff; successes need no action.
        if not r.failed():
            continue

        # Apply backoff per op in this container result.
        for op in r.ops:
            key = (getattr(r, "pipeline_id", None), _op_key(op))
            # If pipeline_id isn't available on result, try to infer from op carrying pipeline_id (rare).
            if key[0] is None:
                # Fallback: leave pipeline id unknown; still track by op object to avoid infinite retries.
                key = ("unknown_pipeline", _op_key(op))

            s.op_retries[key] = s.op_retries.get(key, 0) + 1

            if _suspected_oom(getattr(r, "error", None)):
                prev = s.op_ram_factor.get(key, 1.0)
                s.op_ram_factor[key] = min(prev * 2.0, s.max_ram_factor)

    suspensions = []
    assignments = []

    # Greedily fill each pool with runnable work, prioritizing low-latency classes.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool

        # If nothing to allocate, skip.
        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # Place multiple ops per tick if the pool has headroom.
        # Use a conservative cap to avoid long scheduling loops.
        max_assignments_this_pool = 16
        made = 0

        while made < max_assignments_this_pool and avail_cpu > 0 and avail_ram > 0:
            p = _pop_next_runnable_pipeline(s)
            if p is None:
                break

            status = p.runtime_status()
            if status.is_pipeline_successful():
                continue

            # Pick exactly one runnable op (keeps behavior close to baseline but better latency via priority).
            op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
            if not op_list:
                # Put it back; it might become runnable later.
                _get_queue(s, p.priority).append(p)
                continue

            op = op_list[0]
            key = (p.pipeline_id, _op_key(op))
            retries = s.op_retries.get(key, 0)

            # If retries exceeded, stop trying this pipeline (drop it by not re-enqueueing).
            if retries > s.max_retries_per_op:
                continue

            ram_factor = s.op_ram_factor.get(key, 1.0)

            cpu, ram = _choose_allocation(pool, p.priority, avail_cpu, avail_ram, ram_factor)

            # If we can't give meaningful resources, stop for this pool.
            if cpu <= 0 or ram <= 0:
                # Re-enqueue pipeline to try later.
                _get_queue(s, p.priority).appendleft(p)
                break

            assignments.append(
                Assignment(
                    ops=op_list,
                    cpu=cpu,
                    ram=ram,
                    priority=p.priority,
                    pool_id=pool_id,
                    pipeline_id=p.pipeline_id,
                )
            )

            # Optimistically decrement availability for subsequent placements in this tick.
            avail_cpu -= cpu
            avail_ram -= ram
            made += 1

            # Re-enqueue pipeline so its subsequent ops can be scheduled later.
            _get_queue(s, p.priority).append(p)

    return suspensions, assignments
