# policy_key: scheduler_low_020
# reasoning_effort: low
# model: gpt-5.2-2025-12-11
# llm_cost: 0.047277
# generation_seconds: 55.70
# generated_at: 2026-03-12T21:49:48.963500
@register_scheduler_init(key="scheduler_low_020")
def scheduler_low_020_init(s):
    """Priority-aware, small-step improvement over naive FIFO.

    Improvements (kept intentionally simple / incremental):
      1) Maintain separate FIFO queues per priority (QUERY > INTERACTIVE > BATCH).
      2) Avoid "give the next op the entire pool" by capping per-op CPU/RAM to improve concurrency.
      3) Add light reservation: keep some headroom so low-priority work doesn't steal all resources.
      4) Add minimal OOM learning: if an op OOMs, retry it later with more RAM (exponential backoff).
      5) Add minimal preemption: if high-priority arrives and there's no headroom, suspend some
         known low-priority running containers (best-effort; simulator will reflect freed resources).

    Notes:
      - This policy does not assume perfect knowledge of operator resource curves.
      - It is designed to be "obviously better than naive" while still being robust and readable.
    """
    from collections import deque

    # Per-priority pipeline queues (FIFO within each priority)
    s.q_query = deque()
    s.q_interactive = deque()
    s.q_batch = deque()

    # Per-op RAM multiplier for retries after OOM. Keyed by a stable-ish op key (see _op_key()).
    s.op_ram_mult = {}

    # Track running containers we have recently seen, so we can preempt if needed.
    # container_id -> dict(priority=..., pool_id=..., cpu=..., ram=...)
    s.running = {}

    # Small knobs (conservative defaults)
    s.reserve_frac = 0.20          # keep ~20% headroom for higher priority arrivals
    s.per_op_cpu_cap_frac = 0.50   # an op gets at most 50% of pool CPU (improves concurrency)
    s.per_op_ram_cap_frac = 0.60   # an op gets at most 60% of pool RAM
    s.min_cpu = 1.0                # don't allocate < 1 vCPU if possible (avoid extreme underprovision)
    s.min_ram = 1.0                # don't allocate < 1 GiB if possible (avoid tiny containers)
    s.max_preempt_per_tick = 2     # avoid excessive churn


def _prio_rank(priority):
    # Higher is more important
    if priority == Priority.QUERY:
        return 3
    if priority == Priority.INTERACTIVE:
        return 2
    return 1  # Priority.BATCH_PIPELINE or others


def _enqueue_pipeline(s, p):
    # Push pipeline into its priority queue
    if p.priority == Priority.QUERY:
        s.q_query.append(p)
    elif p.priority == Priority.INTERACTIVE:
        s.q_interactive.append(p)
    else:
        s.q_batch.append(p)


def _op_key(pipeline, op):
    # Best-effort stable key across retries within a simulation run.
    # Prefer common fields if present; otherwise fallback to object id.
    pid = getattr(pipeline, "pipeline_id", None)
    oid = getattr(op, "op_id", None)
    if pid is not None and oid is not None:
        return (pid, oid)
    name = getattr(op, "name", None)
    if pid is not None and name is not None:
        return (pid, name)
    return (pid, id(op))


def _is_oom_error(err):
    if err is None:
        return False
    msg = str(err).lower()
    return ("oom" in msg) or ("out of memory" in msg) or ("memory" in msg and "exceed" in msg)


def _pick_next_pipeline_round(s):
    # Simple strict priority: query, then interactive, then batch
    if s.q_query:
        return s.q_query.popleft()
    if s.q_interactive:
        return s.q_interactive.popleft()
    if s.q_batch:
        return s.q_batch.popleft()
    return None


def _requeue_pipeline_front(s, p):
    # If we popped it but couldn't schedule, put it back at the front (preserve FIFO)
    if p.priority == Priority.QUERY:
        s.q_query.appendleft(p)
    elif p.priority == Priority.INTERACTIVE:
        s.q_interactive.appendleft(p)
    else:
        s.q_batch.appendleft(p)


def _compute_alloc(s, pool, pipeline, op, avail_cpu, avail_ram):
    # Cap per-op allocation to avoid the naive behavior of giving everything to one op.
    # Then apply an OOM-learned RAM multiplier for this op.
    max_cpu = max(0.0, float(pool.max_cpu_pool))
    max_ram = max(0.0, float(pool.max_ram_pool))

    cpu_cap = max(s.min_cpu, max_cpu * s.per_op_cpu_cap_frac) if max_cpu > 0 else avail_cpu
    ram_cap = max(s.min_ram, max_ram * s.per_op_ram_cap_frac) if max_ram > 0 else avail_ram

    cpu = min(avail_cpu, cpu_cap)
    ram = min(avail_ram, ram_cap)

    # Apply RAM multiplier after OOM (best-effort; still capped by avail_ram)
    k = _op_key(pipeline, op)
    mult = s.op_ram_mult.get(k, 1.0)
    ram = min(avail_ram, max(ram, s.min_ram) * mult)

    # Keep some sanity bounds
    cpu = max(0.0, cpu)
    ram = max(0.0, ram)
    return cpu, ram


def _reserve_for_higher_prio(s, pool, current_prio):
    # Reserve headroom for priorities strictly higher than current_prio.
    # If we're scheduling QUERY, reserve nothing. If INTERACTIVE, reserve for QUERY. If BATCH, reserve for both.
    max_cpu = float(pool.max_cpu_pool)
    max_ram = float(pool.max_ram_pool)

    if current_prio == Priority.QUERY:
        return 0.0, 0.0

    # Base reserve is a fraction of pool capacity.
    r_cpu = max(0.0, max_cpu * s.reserve_frac)
    r_ram = max(0.0, max_ram * s.reserve_frac)

    # For batch, reserve a bit more to protect both query+interactive tails.
    if current_prio == Priority.BATCH_PIPELINE:
        r_cpu = max(r_cpu, max_cpu * (s.reserve_frac + 0.10))
        r_ram = max(r_ram, max_ram * (s.reserve_frac + 0.10))

    return r_cpu, r_ram


def _maybe_preempt_for_priority(s, pool_id, need_cpu, need_ram, target_priority):
    # Best-effort: if we need resources for a high-priority op, suspend some low-priority containers.
    # We choose lowest priority first within the same pool.
    suspensions = []
    # Collect candidates in pool with lower priority
    candidates = []
    for cid, info in s.running.items():
        if info["pool_id"] != pool_id:
            continue
        if _prio_rank(info["priority"]) >= _prio_rank(target_priority):
            continue
        candidates.append(( _prio_rank(info["priority"]), cid, info))

    # Lowest rank first => preempt batch before interactive before query (query won't be preempted)
    candidates.sort(key=lambda x: x[0])

    freed_cpu = 0.0
    freed_ram = 0.0
    for _, cid, info in candidates:
        if len(suspensions) >= s.max_preempt_per_tick:
            break
        suspensions.append(Suspend(container_id=cid, pool_id=pool_id))
        freed_cpu += float(info.get("cpu", 0.0) or 0.0)
        freed_ram += float(info.get("ram", 0.0) or 0.0)

        # Remove from running map optimistically; simulator will confirm via results later.
        s.running.pop(cid, None)

        if freed_cpu >= need_cpu and freed_ram >= need_ram:
            break

    return suspensions


@register_scheduler(key="scheduler_low_020")
def scheduler_low_020(s, results, pipelines):
    """
    Priority-aware scheduler with basic right-sizing, reservations, OOM learning, and light preemption.
    """
    # Ingest new pipelines into queues
    for p in pipelines:
        _enqueue_pipeline(s, p)

    # Update running-container tracking + OOM learning from execution results
    for r in results:
        # Keep running-map fresh with latest observed allocations (useful for preemption decisions)
        if getattr(r, "container_id", None) is not None and getattr(r, "pool_id", None) is not None:
            if r.failed():
                # On failure, clear it from running and (if OOM) increase RAM multiplier for the involved ops
                s.running.pop(r.container_id, None)
                if _is_oom_error(getattr(r, "error", None)):
                    for op in getattr(r, "ops", []) or []:
                        # We don't have the pipeline object here; fall back to op-only key.
                        # This is weaker than pipeline+op, but still helps within a run.
                        k = getattr(op, "op_id", None)
                        k = ("op", k) if k is not None else ("op", id(op))
                        prev = float(s.op_ram_mult.get(k, 1.0))
                        s.op_ram_mult[k] = min(prev * 2.0, 16.0)
            else:
                # Not failed: treat as running/active signal (even if it's a completion event, it won't hurt much)
                s.running[r.container_id] = {
                    "priority": getattr(r, "priority", None),
                    "pool_id": r.pool_id,
                    "cpu": float(getattr(r, "cpu", 0.0) or 0.0),
                    "ram": float(getattr(r, "ram", 0.0) or 0.0),
                }

    # Early exit if nothing to do
    if not pipelines and not results and not (s.q_query or s.q_interactive or s.q_batch):
        return [], []

    suspensions = []
    assignments = []

    # Schedule independently per pool, filling with highest priority work first
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]

        # We will update these as we place work
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)

        if avail_cpu <= 0.0 or avail_ram <= 0.0:
            continue

        # Fill the pool with multiple ops until resources are tight
        guard = 0
        while avail_cpu > 0.0 and avail_ram > 0.0 and guard < 200:
            guard += 1

            p = _pick_next_pipeline_round(s)
            if p is None:
                break

            status = p.runtime_status()
            # Drop completed pipelines
            if status.is_pipeline_successful():
                continue

            # Don't keep retrying pipelines that have hard failures; keep behavior similar to baseline
            if status.state_counts[OperatorState.FAILED] > 0:
                continue

            # Only schedule ops whose parents are complete; schedule at most 1 op per pipeline per tick
            op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
            if not op_list:
                # Nothing to schedule now; requeue pipeline for later progress
                _requeue_pipeline_front(s, p)
                break

            op = op_list[0]

            # Apply reservation so batch work doesn't steal all headroom and spike interactive latency
            r_cpu, r_ram = _reserve_for_higher_prio(s, pool, p.priority)
            usable_cpu = max(0.0, avail_cpu - r_cpu)
            usable_ram = max(0.0, avail_ram - r_ram)

            # If we have too little usable resources for this priority, try next pool / next tick
            if usable_cpu <= 0.0 or usable_ram <= 0.0:
                # If this is high priority, try preemption to create headroom
                if _prio_rank(p.priority) >= _prio_rank(Priority.INTERACTIVE):
                    # Try to make at least min allocation possible
                    need_cpu = max(0.0, min(s.min_cpu, r_cpu) - max(0.0, avail_cpu - r_cpu))
                    need_ram = max(0.0, min(s.min_ram, r_ram) - max(0.0, avail_ram - r_ram))
                    suspensions.extend(_maybe_preempt_for_priority(s, pool_id, need_cpu, need_ram, p.priority))
                _requeue_pipeline_front(s, p)
                break

            cpu, ram = _compute_alloc(s, pool, p, op, usable_cpu, usable_ram)

            # If we cannot allocate a sensible minimum, stop trying to pack this pool this tick
            if cpu < 0.5 or ram < 0.5:
                _requeue_pipeline_front(s, p)
                break

            assignments.append(
                Assignment(
                    ops=[op],
                    cpu=cpu,
                    ram=ram,
                    priority=p.priority,
                    pool_id=pool_id,
                    pipeline_id=p.pipeline_id,
                )
            )

            # Optimistically decrement available resources (the simulator will enforce truth)
            avail_cpu -= cpu
            avail_ram -= ram

            # Requeue pipeline for further operators later
            _enqueue_pipeline(s, p)

    return suspensions, assignments
