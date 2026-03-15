# policy_key: scheduler_low_029
# reasoning_effort: low
# model: gpt-5.2-2025-12-11
# llm_cost: 0.039311
# generation_seconds: 40.95
# generated_at: 2026-03-14T02:25:10.969011
@register_scheduler_init(key="scheduler_low_029")
def scheduler_low_029_init(s):
    """Priority-aware FIFO with simple resource reservations and OOM backoff.

    Incremental improvements over naive FIFO:
      1) Maintain separate waiting queues per priority (QUERY > INTERACTIVE > BATCH).
      2) Avoid letting BATCH consume the entire pool by reserving a slice of CPU/RAM
         for higher-priority work (helps tail latency under contention).
      3) On OOM-like failures, increase the next RAM request for that pipeline (simple backoff).

    Notes:
      - Keeps scheduling decisions simple: at most one ready operator per pool per tick.
      - No preemption (not enough runtime visibility here); relies on reservations instead.
    """
    s.waiting_queues = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }
    # Per-pipeline resource hints learned from prior executions (e.g., OOM backoff).
    # pipeline_id -> {"ram": float, "cpu": float}
    s.pipeline_hints = {}
    # Track arrival order for FIFO within priority.
    s.arrival_counter = 0
    s.pipeline_arrival = {}  # pipeline_id -> int


def _is_oom_error(err):
    if not err:
        return False
    e = str(err).upper()
    return ("OOM" in e) or ("OUT OF MEMORY" in e) or ("MEMORY" in e and "KILL" in e)


def _prio_rank(priority):
    # Smaller is higher priority.
    if priority == Priority.QUERY:
        return 0
    if priority == Priority.INTERACTIVE:
        return 1
    return 2


def _default_targets_for(priority, pool):
    # Targets are intentionally conservative and scale with pool size.
    max_cpu = pool.max_cpu_pool
    max_ram = pool.max_ram_pool

    if priority == Priority.QUERY:
        return 0.50 * max_cpu, 0.50 * max_ram
    if priority == Priority.INTERACTIVE:
        return 0.40 * max_cpu, 0.40 * max_ram
    # Batch gets less by default to reduce interference with latency-critical work.
    return 0.25 * max_cpu, 0.25 * max_ram


def _clamp_positive(x, lo, hi):
    if x is None:
        return None
    if x < lo:
        return lo
    if x > hi:
        return hi
    return x


def _pick_next_pipeline(s, pool_id):
    # Dedicated behavior if multiple pools:
    # - Prefer using pool 0 for QUERY/INTERACTIVE when possible.
    # - Other pools can do mixed but still prioritize higher classes.
    prios = [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]
    if s.executor.num_pools > 1 and pool_id == 0:
        # Strong preference for latency-sensitive work on pool 0.
        prios = [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]
    elif s.executor.num_pools > 1 and pool_id != 0:
        # Background pools still can run high-priority if they spill over.
        prios = [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]

    for pr in prios:
        q = s.waiting_queues.get(pr, [])
        # FIFO within the priority class.
        while q:
            p = q.pop(0)
            status = p.runtime_status()
            # Drop terminal pipelines.
            if status.is_pipeline_successful():
                continue
            # If pipeline has any failures, we will keep it only if we believe it was OOM and can retry.
            # Otherwise drop it to avoid infinite loops. We can't reliably detect all cases, so we only
            # keep retrying if we have a hint that we increased RAM due to OOM recently.
            has_failures = status.state_counts[OperatorState.FAILED] > 0
            if has_failures and p.pipeline_id not in s.pipeline_hints:
                continue
            return p
    return None


def _requeue_pipeline(s, p):
    s.waiting_queues[p.priority].append(p)


@register_scheduler(key="scheduler_low_029")
def scheduler_low_029(s, results, pipelines):
    """
    Scheduler step:
      - Enqueue new pipelines into per-priority FIFO queues.
      - Update per-pipeline RAM hints on OOM-like failures.
      - For each pool, schedule at most one ready op from the highest-priority eligible pipeline.
      - Apply CPU/RAM reservations to prevent batch from monopolizing resources.
    """
    # Enqueue new arrivals.
    for p in pipelines:
        s.arrival_counter += 1
        if p.pipeline_id not in s.pipeline_arrival:
            s.pipeline_arrival[p.pipeline_id] = s.arrival_counter
        s.waiting_queues[p.priority].append(p)

    # Learn from execution results (only simple OOM backoff here).
    for r in results:
        if not r.failed():
            continue
        if _is_oom_error(getattr(r, "error", None)):
            # Increase RAM hint for the owning pipeline.
            # We only have the container allocation; treat it as a lower bound that needs a bump.
            pid = getattr(r, "pipeline_id", None)
            # Some simulators may not attach pipeline_id to ExecutionResult; fall back to no-op.
            if pid is None:
                continue
            prev = s.pipeline_hints.get(pid, {})
            prev_ram = prev.get("ram", r.ram)
            bumped = max(prev_ram, r.ram) * 1.5
            s.pipeline_hints[pid] = {
                "ram": bumped,
                "cpu": prev.get("cpu", r.cpu),
            }

    # Early exit.
    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool
        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # Reserve some capacity for latency-sensitive work (only relevant in shared pools).
        # This is a "soft reservation": we simply restrict batch allocations when contention exists.
        reserve_cpu = 0.30 * pool.max_cpu_pool
        reserve_ram = 0.30 * pool.max_ram_pool

        # Pick a candidate pipeline (highest priority with runnable work).
        pipeline = _pick_next_pipeline(s, pool_id)
        if pipeline is None:
            continue

        status = pipeline.runtime_status()
        op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
        if not op_list:
            # Not runnable now; put it back and move on.
            _requeue_pipeline(s, pipeline)
            continue

        # Determine how much we are willing to allocate to this pipeline on this pool.
        pr = pipeline.priority
        default_cpu, default_ram = _default_targets_for(pr, pool)

        hint = s.pipeline_hints.get(pipeline.pipeline_id, {})
        target_cpu = hint.get("cpu", default_cpu)
        target_ram = hint.get("ram", default_ram)

        # For BATCH, do not eat into reserved headroom when the pool is under pressure.
        if pr == Priority.BATCH_PIPELINE:
            usable_cpu = avail_cpu
            usable_ram = avail_ram
            # Only enforce reservation if there is any higher-priority demand waiting.
            has_hp_waiting = bool(s.waiting_queues[Priority.QUERY] or s.waiting_queues[Priority.INTERACTIVE])
            if has_hp_waiting:
                usable_cpu = max(0.0, avail_cpu - reserve_cpu)
                usable_ram = max(0.0, avail_ram - reserve_ram)
            if usable_cpu <= 0 or usable_ram <= 0:
                # Can't run batch without violating headroom; requeue and skip this pool.
                _requeue_pipeline(s, pipeline)
                continue
            alloc_cpu = min(usable_cpu, target_cpu)
            alloc_ram = min(usable_ram, target_ram)
        else:
            # High priority can use full available.
            alloc_cpu = min(avail_cpu, target_cpu)
            alloc_ram = min(avail_ram, target_ram)

        # Ensure some minimal allocation (avoid zero allocations).
        alloc_cpu = _clamp_positive(alloc_cpu, 1.0, avail_cpu)
        alloc_ram = _clamp_positive(alloc_ram, 1.0, avail_ram)

        if alloc_cpu is None or alloc_ram is None or alloc_cpu <= 0 or alloc_ram <= 0:
            _requeue_pipeline(s, pipeline)
            continue

        assignments.append(
            Assignment(
                ops=op_list,
                cpu=alloc_cpu,
                ram=alloc_ram,
                priority=pr,
                pool_id=pool_id,
                pipeline_id=pipeline.pipeline_id,
            )
        )

        # Requeue pipeline so future ops can be scheduled in later ticks.
        _requeue_pipeline(s, pipeline)

    return suspensions, assignments
