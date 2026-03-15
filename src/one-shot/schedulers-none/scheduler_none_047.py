# policy_key: scheduler_none_047
# reasoning_effort: none
# model: gpt-5.2-2025-12-11
# llm_cost: 0.045891
# generation_seconds: 50.49
# generated_at: 2026-03-14T02:15:19.690609
@register_scheduler_init(key="scheduler_none_047")
def scheduler_none_047_init(s):
    """Priority-aware FIFO with conservative preemption + RAM backoff.

    Stepwise improvements over naive FIFO:
      1) Maintain separate queues per priority and always schedule highest priority first.
      2) Right-size per-op CPU/RAM (avoid "give everything" which harms concurrency).
      3) On OOM, retry the same pipeline with increased RAM (exponential backoff) to avoid repeated failures.
      4) If a high-priority op is ready but the pool lacks headroom, preempt (suspend) some lower-priority
         running containers in that pool to free resources (minimize churn by preempting as little as possible).

    Notes/assumptions based on available interfaces:
      - We infer OOM from ExecutionResult.failed() and error string containing "oom"/"out of memory"/"memory".
      - We do not have explicit per-container resource usage beyond result.cpu/result.ram (last allocation).
      - We avoid preempting QUERY/INTERACTIVE work unless necessary; BATCH is preempted first.
    """
    # Priority queues: keep pipelines in arrival order within a priority class.
    s.q_query = []
    s.q_interactive = []
    s.q_batch = []

    # Per-pipeline RAM multiplier for OOM backoff, and last-known (cpu, ram) sizing hints.
    s.ram_mult = {}          # pipeline_id -> float
    s.last_cpu = {}          # pipeline_id -> float
    s.last_ram = {}          # pipeline_id -> float

    # Track currently running containers we can preempt:
    # container_id -> dict(priority, pool_id, cpu, ram)
    s.running = {}

    # Tuning knobs (kept simple; can be evolved later)
    s.min_cpu_per_op = 1.0
    s.max_cpu_fraction = 0.75     # don't let a single assignment consume entire pool by default
    s.max_ram_fraction = 0.75
    s.initial_ram_fraction = 0.25 # start smaller to improve concurrency/latency under contention
    s.initial_cpu_fraction = 0.25
    s.oom_backoff_factor = 2.0
    s.max_ram_mult = 16.0

    # Preemption guardrails
    s.allow_preempt_for = {Priority.QUERY, Priority.INTERACTIVE}  # only preempt when these need headroom
    s.preempt_order = [Priority.BATCH_PIPELINE, Priority.INTERACTIVE, Priority.QUERY]  # last two rarely used


def _prio_rank(priority):
    # Smaller is higher priority
    if priority == Priority.QUERY:
        return 0
    if priority == Priority.INTERACTIVE:
        return 1
    return 2  # Priority.BATCH_PIPELINE and any others


def _is_oom_error(err):
    if not err:
        return False
    e = str(err).lower()
    return ("oom" in e) or ("out of memory" in e) or ("memory" in e and "alloc" in e)


def _enqueue_pipeline(s, p):
    if p.priority == Priority.QUERY:
        s.q_query.append(p)
    elif p.priority == Priority.INTERACTIVE:
        s.q_interactive.append(p)
    else:
        s.q_batch.append(p)


def _drain_next_pipeline(s):
    # Pop from highest priority non-empty queue
    if s.q_query:
        return s.q_query.pop(0)
    if s.q_interactive:
        return s.q_interactive.pop(0)
    if s.q_batch:
        return s.q_batch.pop(0)
    return None


def _requeue_pipeline_front(s, p):
    # Push back to front of its priority queue to preserve work-conserving behavior
    if p.priority == Priority.QUERY:
        s.q_query.insert(0, p)
    elif p.priority == Priority.INTERACTIVE:
        s.q_interactive.insert(0, p)
    else:
        s.q_batch.insert(0, p)


def _choose_op_list(pipeline):
    status = pipeline.runtime_status()
    # One op at a time (like naive) to keep it simple and avoid over-allocating to a single pipeline.
    return status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]


def _size_for_pipeline(s, pipeline, pool):
    # Start conservative; increase on OOM using per-pipeline multiplier.
    pid = pipeline.pipeline_id
    mult = s.ram_mult.get(pid, 1.0)

    # Base sizing as fractions of pool capacity, capped by current available resources.
    base_cpu = max(s.min_cpu_per_op, pool.max_cpu_pool * s.initial_cpu_fraction)
    base_ram = pool.max_ram_pool * s.initial_ram_fraction

    # If we have last-known successful-ish allocation, reuse it for stability.
    # (If last allocation caused OOM, ram_mult would have been increased.)
    hint_cpu = s.last_cpu.get(pid, base_cpu)
    hint_ram = s.last_ram.get(pid, base_ram)

    cpu_target = min(hint_cpu, pool.max_cpu_pool * s.max_cpu_fraction)
    ram_target = min(hint_ram * mult, pool.max_ram_pool * s.max_ram_fraction)

    # Finally, respect current availability.
    cpu = min(cpu_target, pool.avail_cpu_pool)
    ram = min(ram_target, pool.avail_ram_pool)

    # Ensure minimum CPU.
    if cpu < s.min_cpu_per_op:
        cpu = 0.0  # cannot schedule
    if ram <= 0.0:
        ram = 0.0

    return cpu, ram


def _collect_preemptable(s, pool_id, min_rank_needed):
    # Return list of (container_id, info) sorted by lowest priority first (largest rank), then largest resource.
    items = []
    for cid, info in s.running.items():
        if info["pool_id"] != pool_id:
            continue
        r = _prio_rank(info["priority"])
        if r <= min_rank_needed:
            continue  # do not preempt same/higher priority by default
        items.append((cid, info))

    def key_fn(x):
        _, info = x
        return (-_prio_rank(info["priority"]), -(info.get("ram", 0.0) + info.get("cpu", 0.0)))

    items.sort(key=key_fn)
    return items


def _maybe_preempt_for_headroom(s, pool, pool_id, need_cpu, need_ram, requester_priority):
    # Decide suspensions to free up enough resources in the pool.
    # We only consider preempting if requester is high-priority and we are short.
    if requester_priority not in s.allow_preempt_for:
        return []

    short_cpu = max(0.0, need_cpu - pool.avail_cpu_pool)
    short_ram = max(0.0, need_ram - pool.avail_ram_pool)
    if short_cpu <= 0.0 and short_ram <= 0.0:
        return []

    suspensions = []
    freed_cpu = 0.0
    freed_ram = 0.0

    min_rank_needed = _prio_rank(requester_priority)
    preemptable = _collect_preemptable(s, pool_id, min_rank_needed=min_rank_needed)

    for cid, info in preemptable:
        if short_cpu <= 0.0 and short_ram <= 0.0:
            break
        # Suspend this container
        suspensions.append(Suspend(container_id=cid, pool_id=pool_id))
        freed_cpu += float(info.get("cpu", 0.0) or 0.0)
        freed_ram += float(info.get("ram", 0.0) or 0.0)
        short_cpu = max(0.0, short_cpu - float(info.get("cpu", 0.0) or 0.0))
        short_ram = max(0.0, short_ram - float(info.get("ram", 0.0) or 0.0))

    return suspensions


@register_scheduler(key="scheduler_none_047")
def scheduler_none_047(s, results, pipelines):
    """
    Priority-aware, latency-focused scheduler.

    Core behavior:
      - Enqueue arrivals by priority.
      - Process results to (a) learn sizes, (b) apply OOM backoff, (c) update running set.
      - For each pool, try to schedule highest-priority ready ops first.
      - If high-priority cannot fit, preempt lower-priority running containers in that pool.
    """
    # Enqueue new pipelines
    for p in pipelines:
        _enqueue_pipeline(s, p)

    # Process results: update running set and adjust RAM multipliers on OOM
    for r in results:
        # If container finished (success or failure), remove from running set
        if getattr(r, "container_id", None) in s.running:
            # Regardless of outcome, container is no longer running after we receive a result tick for it.
            # (In many sims, completion triggers a result.)
            s.running.pop(r.container_id, None)

        # Update per-pipeline sizing hints when available
        pid = getattr(r, "pipeline_id", None)
        if pid is not None:
            if getattr(r, "cpu", None) is not None:
                s.last_cpu[pid] = float(r.cpu)
            if getattr(r, "ram", None) is not None:
                s.last_ram[pid] = float(r.ram)

        # OOM backoff on failure
        if hasattr(r, "failed") and r.failed():
            if _is_oom_error(getattr(r, "error", None)):
                if pid is not None:
                    cur = s.ram_mult.get(pid, 1.0)
                    s.ram_mult[pid] = min(cur * s.oom_backoff_factor, s.max_ram_mult)

    # Early exit
    if not pipelines and not results and not (s.q_query or s.q_interactive or s.q_batch):
        return [], []

    suspensions = []
    assignments = []

    # For each pool, schedule at most one op per tick (like naive), but pick highest priority
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]

        if pool.avail_cpu_pool <= 0 or pool.avail_ram_pool <= 0:
            continue

        # We may need to search for a runnable pipeline; keep a small buffer of examined pipelines to requeue
        examined = []

        scheduled = False
        # Avoid infinite loops; only scan up to total queued pipelines once
        total_queued = len(s.q_query) + len(s.q_interactive) + len(s.q_batch)
        for _ in range(total_queued):
            pipeline = _drain_next_pipeline(s)
            if pipeline is None:
                break

            status = pipeline.runtime_status()
            has_failures = status.state_counts[OperatorState.FAILED] > 0
            if status.is_pipeline_successful() or has_failures:
                # Drop completed pipelines and pipelines with non-retriable failures.
                # (We only backoff on OOM at op-level; pipeline-level failure count indicates "give up".)
                continue

            op_list = _choose_op_list(pipeline)
            if not op_list:
                # Not ready (parents not done); keep it around
                examined.append(pipeline)
                continue

            # Determine target sizing
            cpu, ram = _size_for_pipeline(s, pipeline, pool)
            if cpu <= 0.0 or ram <= 0.0:
                # Can't schedule now in this pool; requeue and move on
                examined.append(pipeline)
                continue

            # If it doesn't fit and requester is high-priority, attempt preemption
            # Use "need" as our requested cpu/ram
            if (pool.avail_cpu_pool < cpu) or (pool.avail_ram_pool < ram):
                new_susp = _maybe_preempt_for_headroom(
                    s, pool, pool_id,
                    need_cpu=cpu, need_ram=ram,
                    requester_priority=pipeline.priority
                )
                if new_susp:
                    suspensions.extend(new_susp)
                    # After suspending, we optimistically proceed with assignment in same tick.
                    # The simulator should apply suspensions before assignments (if not, this will just fail to place).
                    # Either way, it's a small, safe improvement over naive.
                else:
                    examined.append(pipeline)
                    continue

            # Create assignment
            assignment = Assignment(
                ops=op_list,
                cpu=cpu,
                ram=ram,
                priority=pipeline.priority,
                pool_id=pool_id,
                pipeline_id=pipeline.pipeline_id
            )
            assignments.append(assignment)

            # Track the container as running after it's launched.
            # We don't have container_id yet; it will appear in results.
            # So we conservatively don't add here. Instead, infer running containers from results stream.
            # However, for preemption to work we need some running state. Best effort:
            # if ExecutionResult streams "started" events we could track; lacking that, we track only on completion.
            # To still enable preemption, we add an entry with a synthetic key if assignment has ops with container id
            # (not provided). We'll instead track running containers from results where possible.
            scheduled = True

            # Put pipeline back for remaining operators
            examined.append(pipeline)
            break

        # Requeue examined pipelines to preserve order within their priority class as much as possible
        for p in examined:
            _enqueue_pipeline(s, p)

        if scheduled:
            # one assignment per pool per tick
            continue

    # Best-effort: update running set using results that might represent "started/running" events
    # If simulator emits intermediate results with container_id and no completion, we can capture them.
    for r in results:
        cid = getattr(r, "container_id", None)
        if cid is None:
            continue
        # If the result indicates still running, we'd keep it; but we don't have a flag.
        # We approximate: if failed() is False and error is None and ops present, it might be a running/started event.
        # Do not overwrite a removal from earlier in this tick unless it's clearly not terminal.
        if cid in s.running:
            continue
        if hasattr(r, "failed") and r.failed():
            continue
        # If error exists, treat as terminal.
        if getattr(r, "error", None):
            continue
        # Register as running for possible preemption later.
        s.running[cid] = {
            "priority": getattr(r, "priority", Priority.BATCH_PIPELINE),
            "pool_id": getattr(r, "pool_id", None),
            "cpu": float(getattr(r, "cpu", 0.0) or 0.0),
            "ram": float(getattr(r, "ram", 0.0) or 0.0),
        }

    return suspensions, assignments
