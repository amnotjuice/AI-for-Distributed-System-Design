# policy_key: scheduler_est_019
# reasoning_effort: high
# model: gpt-5.2-2025-12-11
# llm_cost: 0.114176
# generation_seconds: 69.07
# generated_at: 2026-03-31T18:57:25.504261
@register_scheduler_init(key="scheduler_est_019")
def scheduler_est_019_init(s):
    """
    Priority-aware FIFO with basic memory estimation and OOM-aware retry.

    Incremental improvements over naive FIFO:
      1) Maintain separate per-priority queues and always schedule higher priority first.
      2) Use op.estimate.mem_peak_gb (when available) with a small safety factor to size RAM.
      3) If an op fails with OOM, retry it with increased RAM (bounded retries); otherwise mark pipeline failed.
      4) Avoid allocating an entire pool to a single op by capping per-op CPU/RAM to enable concurrency.
    """
    from collections import deque

    # Per-priority FIFO queues of pipelines
    s.q_by_prio = {
        Priority.QUERY: deque(),
        Priority.INTERACTIVE: deque(),
        Priority.BATCH_PIPELINE: deque(),
    }

    # Remember permanent failures (non-OOM or too many OOM retries)
    s.perm_failed_pipelines = set()

    # Track per-(pipeline, op) OOM retry count and next-ram override
    s.oom_retries = {}            # (pipeline_id, op_key) -> int
    s.op_ram_override_gb = {}     # (pipeline_id, op_key) -> float

    # Mild tuning knobs
    s.max_oom_retries = 3
    s.base_min_ram_gb = 0.25      # don't request tiny containers
    s.tick = 0


def _priority_order():
    # Highest to lowest
    return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]


def _is_oom_error(err) -> bool:
    if err is None:
        return False
    msg = str(err).lower()
    return ("oom" in msg) or ("out of memory" in msg) or ("outofmemory" in msg) or ("memoryerror" in msg)


def _get_assignable_states():
    # Some environments provide ASSIGNABLE_STATES; fall back safely if not.
    try:
        return ASSIGNABLE_STATES
    except Exception:
        return [OperatorState.PENDING, OperatorState.FAILED]


def _op_key(op):
    # Best-effort stable key for an operator within a pipeline.
    for attr in ("op_id", "operator_id", "id", "name"):
        if hasattr(op, attr):
            v = getattr(op, attr)
            if v is not None:
                return v
    # Last resort: string form
    return str(op)


def _mem_estimate_gb(op, default_gb=1.0):
    # Read op.estimate.mem_peak_gb if present.
    try:
        est = getattr(op, "estimate", None)
        if est is not None:
            v = getattr(est, "mem_peak_gb", None)
            if v is not None:
                return float(v)
    except Exception:
        pass
    return float(default_gb)


def _safety_factor_for_priority(priority):
    # Slightly larger safety for interactive to reduce disruptive OOM retries.
    if priority == Priority.QUERY:
        return 1.15
    if priority == Priority.INTERACTIVE:
        return 1.25
    return 1.10


def _cpu_cap_for_priority(priority, pool_max_cpu):
    # Keep per-op CPU bounded to allow multiple concurrent ops per pool.
    if priority == Priority.QUERY:
        return max(1.0, min(float(pool_max_cpu), 4.0))
    if priority == Priority.INTERACTIVE:
        return max(1.0, min(float(pool_max_cpu), 3.0))
    return max(1.0, min(float(pool_max_cpu), 2.0))


def _ram_cap_for_priority(priority, pool_max_ram):
    # Avoid a single op taking the whole pool unless necessary.
    # Higher priority can take more, but still capped.
    if priority == Priority.QUERY:
        return max(0.25, 0.75 * float(pool_max_ram))
    if priority == Priority.INTERACTIVE:
        return max(0.25, 0.70 * float(pool_max_ram))
    return max(0.25, 0.60 * float(pool_max_ram))


def _enqueue_pipeline(s, p):
    if p.pipeline_id in s.perm_failed_pipelines:
        return
    status = p.runtime_status()
    if status.is_pipeline_successful():
        return
    # Put into its priority queue (FIFO).
    if p.priority not in s.q_by_prio:
        s.q_by_prio[p.priority] = __import__("collections").deque()
    s.q_by_prio[p.priority].append(p)


def _pick_next_ready_op(s, prio, max_scan=32):
    """
    Pop/rotate within a priority queue until we find a pipeline with a ready op.
    Returns (pipeline, op) or (None, None).
    """
    q = s.q_by_prio.get(prio)
    if not q:
        return None, None

    assignable_states = _get_assignable_states()
    scanned = 0

    while q and scanned < max_scan:
        p = q.popleft()
        scanned += 1

        if p.pipeline_id in s.perm_failed_pipelines:
            continue

        status = p.runtime_status()
        if status.is_pipeline_successful():
            continue

        # Get one ready op (parents complete)
        op_list = status.get_ops(assignable_states, require_parents_complete=True)[:1]
        if not op_list:
            # Not currently schedulable; rotate to back
            q.append(p)
            continue

        # Found schedulable work; rotate pipeline to back for fairness after we schedule its op
        q.append(p)
        return p, op_list[0]

    return None, None


@register_scheduler(key="scheduler_est_019")
def scheduler_est_019_scheduler(s, results: list, pipelines: list):
    """
    Main scheduling loop.

    Inputs:
      - results: execution outcomes from previous tick
      - pipelines: new arrivals

    Outputs:
      - suspensions: (unused in this incremental policy; no preemption yet)
      - assignments: list of new container launches
    """
    s.tick += 1

    # Incorporate new pipelines
    for p in pipelines:
        _enqueue_pipeline(s, p)

    # Learn from results: handle OOM retries and permanent failures
    for r in results:
        # If pipeline already marked permanently failed, ignore.
        try:
            # r.ops is a list of ops involved in this container execution
            ops = getattr(r, "ops", []) or []
            pipeline_id = getattr(r, "pipeline_id", None)  # may not exist
        except Exception:
            ops = []
            pipeline_id = None

        if not hasattr(r, "failed") or not callable(r.failed):
            continue

        if not r.failed():
            # On success, we could potentially reduce overrides; keep it simple for now.
            continue

        err = getattr(r, "error", None)
        is_oom = _is_oom_error(err)

        # Determine pipeline_id best-effort: some sims may not include it in results.
        # If absent, we can only adjust using container-local info; skip permanent marking.
        if pipeline_id is None:
            # Still can do coarse handling via ops, but cannot bound per pipeline reliably.
            if is_oom:
                for op in ops:
                    ok = _op_key(op)
                    # Use a synthetic pipeline_id bucket to avoid key explosion
                    key = ("_unknown_pipeline_", ok)
                    prev = s.oom_retries.get(key, 0)
                    s.oom_retries[key] = prev + 1
                    # Increase based on what we attempted
                    tried_ram = float(getattr(r, "ram", 0.0) or 0.0)
                    next_ram = max(tried_ram * 1.5, tried_ram + 1.0, s.base_min_ram_gb)
                    s.op_ram_override_gb[key] = max(s.op_ram_override_gb.get(key, 0.0), next_ram)
            continue

        if is_oom:
            for op in ops:
                ok = _op_key(op)
                key = (pipeline_id, ok)
                prev = s.oom_retries.get(key, 0)
                s.oom_retries[key] = prev + 1

                tried_ram = float(getattr(r, "ram", 0.0) or 0.0)
                next_ram = max(tried_ram * 1.5, tried_ram + 1.0, s.base_min_ram_gb)
                s.op_ram_override_gb[key] = max(s.op_ram_override_gb.get(key, 0.0), next_ram)

                # Too many OOMs => treat as permanent failure for this pipeline (simple, conservative)
                if s.oom_retries[key] > s.max_oom_retries:
                    s.perm_failed_pipelines.add(pipeline_id)
        else:
            # Non-OOM failure: mark pipeline permanently failed (avoid infinite retries)
            s.perm_failed_pipelines.add(pipeline_id)

    # Early exit if nothing to do
    any_waiting = any(len(q) > 0 for q in s.q_by_prio.values())
    if not any_waiting:
        return [], []

    suspensions = []
    assignments = []

    # Mild standing headroom to reduce "pool fully committed to batch" incidents.
    # (No preemption yet, so this is a small latency-protection lever.)
    def reserve_cpu(pool):
        return max(0.0, min(1.0, 0.10 * float(pool.max_cpu_pool)))

    def reserve_ram(pool):
        return max(0.0, min(1.0, 0.10 * float(pool.max_ram_pool)))

    # Schedule independently per pool
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)

        if avail_cpu <= 0.0 or avail_ram <= 0.0:
            continue

        # If there is any high-priority queued, keep a small reserve from being consumed by batch.
        high_prio_waiting = (len(s.q_by_prio.get(Priority.QUERY, [])) > 0) or (len(s.q_by_prio.get(Priority.INTERACTIVE, [])) > 0)
        cpu_res = reserve_cpu(pool) if high_prio_waiting else 0.0
        ram_res = reserve_ram(pool) if high_prio_waiting else 0.0

        # Greedily pack multiple ops into the pool in priority order
        progress = True
        safety_iters = 0
        while progress and safety_iters < 128:
            safety_iters += 1
            progress = False

            # Stop if we can't fit even a minimal container
            if avail_cpu < 1.0 or avail_ram < s.base_min_ram_gb:
                break

            # Find next candidate op across priorities
            chosen = None  # (priority, pipeline, op)
            for prio in _priority_order():
                p, op = _pick_next_ready_op(s, prio)
                if p is None:
                    continue
                chosen = (prio, p, op)
                break

            if chosen is None:
                break

            prio, p, op = chosen

            # If we're about to schedule batch while high-priority exists, enforce headroom.
            if prio == Priority.BATCH_PIPELINE and high_prio_waiting:
                if avail_cpu <= cpu_res or avail_ram <= ram_res:
                    # Can't schedule batch without eating protected headroom
                    break

            # Compute RAM request from estimate + safety factor, then apply OOM override if any.
            est_gb = _mem_estimate_gb(op, default_gb=1.0)
            ram_req = max(s.base_min_ram_gb, est_gb * _safety_factor_for_priority(prio))

            key = (p.pipeline_id, _op_key(op))
            if key in s.op_ram_override_gb:
                ram_req = max(ram_req, float(s.op_ram_override_gb[key]))

            # Cap per-op RAM to avoid monopolization (but don't exceed what's available anyway).
            ram_req = min(ram_req, _ram_cap_for_priority(prio, pool.max_ram_pool))

            # If it doesn't fit, try to place smaller work of same priority by scanning a bit more next tick.
            # (Keeping it simple: just stop packing this pool this tick.)
            if ram_req > avail_ram:
                break

            # CPU request: prioritize latency but cap for concurrency
            cpu_cap = _cpu_cap_for_priority(prio, pool.max_cpu_pool)
            cpu_req = min(avail_cpu, cpu_cap)

            # For batch under high-priority pressure, also avoid consuming reserved CPU
            if prio == Priority.BATCH_PIPELINE and high_prio_waiting:
                cpu_req = min(cpu_req, max(0.0, avail_cpu - cpu_res))

            if cpu_req < 1.0:
                break

            # Also keep RAM reserve for batch under high-priority pressure
            if prio == Priority.BATCH_PIPELINE and high_prio_waiting:
                if avail_ram - ram_req < ram_res:
                    break

            assignments.append(
                Assignment(
                    ops=[op],
                    cpu=cpu_req,
                    ram=ram_req,
                    priority=p.priority,
                    pool_id=pool_id,
                    pipeline_id=p.pipeline_id,
                )
            )

            # Locally decrement remaining capacity so we don't over-assign in this tick.
            avail_cpu -= cpu_req
            avail_ram -= ram_req
            progress = True

    return suspensions, assignments
