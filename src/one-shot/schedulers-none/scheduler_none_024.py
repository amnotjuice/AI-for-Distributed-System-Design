# policy_key: scheduler_none_024
# reasoning_effort: none
# model: gpt-5.2-2025-12-11
# llm_cost: 0.042212
# generation_seconds: 43.51
# generated_at: 2026-03-14T01:58:51.368799
@register_scheduler_init(key="scheduler_none_024")
def scheduler_none_024_init(s):
    """Priority-aware incremental scheduler (small improvements over naive FIFO).

    Improvements vs naive:
      1) Maintain separate queues per priority and always consider higher priority first.
      2) OOM-aware RAM bumping: if an op fails, retry the pipeline later with increased RAM request.
      3) Gentle CPU capping for interactive/query to reduce head-of-line blocking and improve tail latency.
      4) Simple preemption: if high-priority work is waiting and a pool has no headroom, suspend a
         running low-priority container to free resources (best-effort; avoids complex churn control).

    Notes:
      - Keeps changes minimal and robust: one assignment per pool per tick, like the baseline.
      - Uses conservative heuristics because simulator interface does not expose per-op resource models here.
    """
    # Per-priority waiting queues (pipelines can appear multiple times; we dedupe on enqueue)
    s.waiting_q = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }
    s._enqueued = set()  # pipeline_id currently present in some queue

    # Track last-seen resource requests per pipeline, used for retries/adaptation
    s.pipeline_cpu_req = {}  # pipeline_id -> cpu
    s.pipeline_ram_req = {}  # pipeline_id -> ram

    # Track OOM-driven RAM bumps per pipeline
    s.oom_bumps = {}  # pipeline_id -> multiplier (>=1.0)

    # Basic knobs (keep conservative to avoid overfitting)
    s.cpu_cap_hi_pri_frac = 0.60   # cap hi-pri CPU to reduce monopolization
    s.cpu_floor_hi_pri = 1.0       # ensure at least 1 vCPU for hi-pri
    s.ram_bump_factor = 1.5        # OOM retry multiplier
    s.ram_bump_max = 8.0           # avoid runaway
    s.preempt_enabled = True
    s.preempt_max_per_tick = 1     # limit churn


def _priority_rank(priority):
    # Smaller is higher priority
    if priority == Priority.QUERY:
        return 0
    if priority == Priority.INTERACTIVE:
        return 1
    return 2  # Priority.BATCH_PIPELINE


def _is_oom_error(err):
    if not err:
        return False
    e = str(err).lower()
    return ("oom" in e) or ("out of memory" in e) or ("memory" in e and "exceed" in e)


def _clamp(x, lo, hi):
    return max(lo, min(hi, x))


def _enqueue_pipeline(s, p):
    # Avoid duplicating the same pipeline in queues.
    if p.pipeline_id in s._enqueued:
        return
    s.waiting_q[p.priority].append(p)
    s._enqueued.add(p.pipeline_id)


def _dequeue_next_runnable(s):
    # Return next pipeline to consider, in strict priority order.
    for pri in (Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE):
        q = s.waiting_q[pri]
        while q:
            p = q.pop(0)
            s._enqueued.discard(p.pipeline_id)
            return p
    return None


def _requeue_pipeline(s, p):
    # Reinsert at tail (preserves FIFO within priority class)
    _enqueue_pipeline(s, p)


def _compute_request_for_pipeline(s, p, pool):
    # Start from previous request if exists; otherwise use small/cheap defaults.
    pid = p.pipeline_id

    # Defaults: give batch more CPU when available; cap interactive/query.
    if pid in s.pipeline_cpu_req:
        base_cpu = s.pipeline_cpu_req[pid]
    else:
        if p.priority in (Priority.QUERY, Priority.INTERACTIVE):
            base_cpu = max(s.cpu_floor_hi_pri, pool.max_cpu_pool * 0.25)
        else:
            base_cpu = pool.max_cpu_pool * 0.75

    if pid in s.pipeline_ram_req:
        base_ram = s.pipeline_ram_req[pid]
    else:
        # Default to a modest slice of pool RAM; batch slightly larger.
        base_ram = pool.max_ram_pool * (0.30 if p.priority in (Priority.QUERY, Priority.INTERACTIVE) else 0.50)

    # Apply OOM multiplier if any
    mult = s.oom_bumps.get(pid, 1.0)
    req_ram = base_ram * mult

    # Apply CPU cap for high priority to improve tail latency under contention
    if p.priority in (Priority.QUERY, Priority.INTERACTIVE):
        cap = max(s.cpu_floor_hi_pri, pool.max_cpu_pool * s.cpu_cap_hi_pri_frac)
        req_cpu = min(base_cpu, cap)
    else:
        req_cpu = base_cpu

    # Clamp to pool maxima
    req_cpu = _clamp(req_cpu, 0.1, pool.max_cpu_pool)
    req_ram = _clamp(req_ram, 0.1, pool.max_ram_pool)

    # Also clamp to currently available to avoid over-asking; keep minimal floors
    req_cpu = _clamp(req_cpu, 0.1, pool.avail_cpu_pool)
    req_ram = _clamp(req_ram, 0.1, pool.avail_ram_pool)

    return req_cpu, req_ram


def _maybe_preempt_for_high_priority(s, results, pool_id, need_cpu, need_ram):
    """Best-effort preemption: if high-pri is waiting and pool lacks headroom, suspend one low-pri running container."""
    if not s.preempt_enabled:
        return []

    pool = s.executor.pools[pool_id]
    if pool.avail_cpu_pool >= need_cpu and pool.avail_ram_pool >= need_ram:
        return []

    # Identify currently running containers from results (limited visibility).
    # Prefer suspending low-priority (batch) containers first.
    candidates = []
    for r in results:
        try:
            if r.pool_id != pool_id:
                continue
            if not getattr(r, "container_id", None):
                continue
            # If result has ops, it indicates a container existed; we don't know if still running,
            # but suspending a completed container is a no-op in most sims; keep conservative.
            pr = getattr(r, "priority", None)
            if pr is None:
                continue
            candidates.append(r)
        except Exception:
            continue

    # Sort: lowest priority first (batch), then by larger allocations (free more)
    candidates.sort(
        key=lambda r: (
            -_priority_rank(getattr(r, "priority", Priority.BATCH_PIPELINE)),
            -(getattr(r, "cpu", 0.0) or 0.0),
            -(getattr(r, "ram", 0.0) or 0.0),
        )
    )

    suspensions = []
    # Suspend at most one container per call to avoid churn
    for r in candidates:
        if getattr(r, "priority", None) in (Priority.QUERY, Priority.INTERACTIVE):
            continue
        suspensions.append(Suspend(r.container_id, pool_id))
        break
    return suspensions


@register_scheduler(key="scheduler_none_024")
def scheduler_none_024(s, results, pipelines):
    """
    Priority-aware scheduler with simple OOM-aware retry and light preemption.

    Behavior:
      - Enqueue new pipelines into per-priority FIFO queues.
      - Process execution results to detect OOM and increase RAM request for retries.
      - For each pool, attempt to schedule exactly one ready operator from the highest-priority
        runnable pipeline. If no headroom and high-priority is waiting, preempt one batch container.
    """
    # Ingest new pipelines
    for p in pipelines:
        _enqueue_pipeline(s, p)

    # Update adaptation from results (OOM bumps; remember last allocations)
    for r in results:
        # Persist last used sizes by pipeline if available
        try:
            pid = getattr(r, "pipeline_id", None)
        except Exception:
            pid = None

        # Not all sims attach pipeline_id to results; we can still use r.ops to find pipeline indirectly
        # but interface doesn't expose that mapping here. Keep best-effort: only adapt when pipeline_id exists.
        if pid is not None:
            if getattr(r, "cpu", None) is not None:
                s.pipeline_cpu_req[pid] = float(r.cpu)
            if getattr(r, "ram", None) is not None:
                s.pipeline_ram_req[pid] = float(r.ram)

            if hasattr(r, "failed") and r.failed():
                if _is_oom_error(getattr(r, "error", None)):
                    prev = s.oom_bumps.get(pid, 1.0)
                    s.oom_bumps[pid] = min(s.ram_bump_max, max(1.0, prev * s.ram_bump_factor))

    # Early exit if nothing to do
    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    # Helper: detect if any high-priority work is waiting (for preemption trigger)
    def hi_pri_waiting():
        return bool(s.waiting_q[Priority.QUERY] or s.waiting_q[Priority.INTERACTIVE])

    # Iterate pools and try to place one op per pool per tick (keeps behavior close to baseline)
    requeue = []
    preempts_left = s.preempt_max_per_tick

    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]

        # If nothing available at all, skip (preemption may still help if hi-pri waiting)
        # We'll attempt preemption only if high-priority is waiting.
        if (pool.avail_cpu_pool <= 0 or pool.avail_ram_pool <= 0) and not hi_pri_waiting():
            continue

        scheduled = False
        tries = 0
        max_tries = 64  # avoid infinite loops if many blocked pipelines

        while not scheduled and tries < max_tries:
            tries += 1
            p = _dequeue_next_runnable(s)
            if p is None:
                break

            status = p.runtime_status()

            # Drop successful pipelines
            if status.is_pipeline_successful():
                continue

            # If pipeline has any failed ops, we still allow retry (ASSIGNABLE_STATES includes FAILED)
            # but we do not implement complex backoff here.

            # Find next runnable op (require parents complete)
            op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
            if not op_list:
                # Not ready yet; requeue and consider another
                requeue.append(p)
                continue

            # Compute requested resources for this pipeline on this pool
            # If pool has no headroom and high-priority waits, preempt one batch container.
            req_cpu, req_ram = _compute_request_for_pipeline(s, p, pool)

            if (pool.avail_cpu_pool < req_cpu or pool.avail_ram_pool < req_ram) and hi_pri_waiting() and preempts_left > 0:
                # Attempt to free resources (best effort)
                needed_cpu = max(0.0, req_cpu - pool.avail_cpu_pool)
                needed_ram = max(0.0, req_ram - pool.avail_ram_pool)
                new_susp = _maybe_preempt_for_high_priority(s, results, pool_id, needed_cpu, needed_ram)
                if new_susp:
                    suspensions.extend(new_susp)
                    preempts_left -= 1
                    # Requeue pipeline and try next decision next tick after resources change
                    requeue.append(p)
                    break

            # If still no room, requeue and move on (keeps system stable)
            if pool.avail_cpu_pool < req_cpu or pool.avail_ram_pool < req_ram:
                requeue.append(p)
                continue

            # Create assignment
            assignments.append(
                Assignment(
                    ops=op_list,
                    cpu=req_cpu,
                    ram=req_ram,
                    priority=p.priority,
                    pool_id=pool_id,
                    pipeline_id=p.pipeline_id,
                )
            )

            # Remember chosen sizes for subsequent ops in same pipeline (sticky sizing)
            s.pipeline_cpu_req[p.pipeline_id] = float(req_cpu)
            s.pipeline_ram_req[p.pipeline_id] = float(req_ram)

            scheduled = True

        # Done with this pool

    # Requeue pipelines we popped but didn't run
    for p in requeue:
        # If it completed in the meantime, avoid requeueing
        try:
            if p.runtime_status().is_pipeline_successful():
                continue
        except Exception:
            pass
        _requeue_pipeline(s, p)

    return suspensions, assignments
