# policy_key: scheduler_none_021
# reasoning_effort: none
# model: gpt-5.2-2025-12-11
# llm_cost: 0.037452
# generation_seconds: 31.27
# generated_at: 2026-03-14T01:57:05.141296
@register_scheduler_init(key="scheduler_none_021")
def scheduler_none_021_init(s):
    """Priority-aware, low-risk improvement over naive FIFO.

    Goals / small-step improvements:
      1) Prioritize interactive/query pipelines to reduce tail latency.
      2) Avoid over-allocating an entire pool to a single tiny op by using a conservative
         initial sizing (CPU/RAM caps per assignment), enabling more concurrency.
      3) Add simple OOM-aware RAM backoff: if an op failed (OOM), retry with more RAM next time.
      4) Keep batching fair: simple aging to prevent indefinite starvation.

    Notes:
      - This policy avoids preemption to keep churn low; it focuses on admission + sizing.
      - It schedules at most one operator per pipeline per tick to preserve DAG parallelism
        constraints and avoid flooding.
    """
    # Waiting pipelines tracked as dict for easy de-dupe
    s.waiting: dict = {}  # pipeline_id -> Pipeline

    # Per (pipeline_id, op_id) resource hints learned from failures
    s.op_hints: dict = {}  # (pipeline_id, op_id) -> {"ram": float, "cpu": float}

    # Aging bookkeeping
    s.enqueue_time: dict = {}  # pipeline_id -> first seen tick
    s.tick: int = 0

    # Conservative per-assignment caps (fractions of pool capacity)
    s.cpu_cap_frac = 0.5  # never allocate more than 50% of pool CPU to one assignment
    s.ram_cap_frac = 0.6  # never allocate more than 60% of pool RAM to one assignment

    # Minimum slices to avoid too-tiny allocations
    s.min_cpu = 1.0
    s.min_ram = 0.5

    # OOM backoff factors
    s.oom_ram_backoff = 1.6
    s.oom_cpu_backoff = 1.2  # modest CPU bump on failure (could be non-OOM too)


def _priority_rank(priority):
    # Lower is better
    if priority == Priority.QUERY:
        return 0
    if priority == Priority.INTERACTIVE:
        return 1
    return 2  # Priority.BATCH_PIPELINE and others


def _pipeline_sort_key(s, pipeline):
    # Priority first, then aging (older first), then stable pipeline_id
    pr = _priority_rank(pipeline.priority)
    age = s.tick - s.enqueue_time.get(pipeline.pipeline_id, s.tick)
    # Use negative age to sort older earlier (larger age first)
    return (pr, -age, pipeline.pipeline_id)


def _pick_pool_for_priority(s, priority):
    """Choose a pool for the given priority.
    Simple heuristic: pick the pool with most headroom (min of cpu% and ram%) to reduce queueing.
    """
    best = None
    best_score = None
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        # Compute relative headroom; avoid div by zero
        cpu_frac = (pool.avail_cpu_pool / pool.max_cpu_pool) if pool.max_cpu_pool else 0.0
        ram_frac = (pool.avail_ram_pool / pool.max_ram_pool) if pool.max_ram_pool else 0.0
        score = min(cpu_frac, ram_frac)
        if best is None or score > best_score:
            best = pool_id
            best_score = score
    return best if best is not None else 0


def _op_id(op):
    # Best-effort stable identifier for an operator object in simulator
    return getattr(op, "op_id", None) or getattr(op, "operator_id", None) or getattr(op, "name", None) or id(op)


def _clamp(x, lo, hi):
    if x < lo:
        return lo
    if x > hi:
        return hi
    return x


def _desired_resources(s, pool, pipeline_id, op, priority):
    """Compute CPU/RAM to allocate for this op in this pool, using hints and conservative caps."""
    opkey = (pipeline_id, _op_id(op))
    hint = s.op_hints.get(opkey, None)

    # Baseline: small slices to allow concurrency, scaled by priority
    # Queries get a bit more CPU; batch slightly less.
    if priority == Priority.QUERY:
        base_cpu_frac = 0.35
        base_ram_frac = 0.35
    elif priority == Priority.INTERACTIVE:
        base_cpu_frac = 0.30
        base_ram_frac = 0.33
    else:
        base_cpu_frac = 0.25
        base_ram_frac = 0.30

    cpu = pool.max_cpu_pool * base_cpu_frac
    ram = pool.max_ram_pool * base_ram_frac

    # Apply learned hints (typically from OOMs)
    if hint:
        cpu = max(cpu, hint.get("cpu", cpu))
        ram = max(ram, hint.get("ram", ram))

    # Cap per-assignment to avoid monopolizing pool
    cpu_cap = pool.max_cpu_pool * s.cpu_cap_frac
    ram_cap = pool.max_ram_pool * s.ram_cap_frac

    cpu = _clamp(cpu, s.min_cpu, cpu_cap)
    ram = _clamp(ram, s.min_ram, ram_cap)

    # Also cannot exceed currently available
    cpu = min(cpu, pool.avail_cpu_pool)
    ram = min(ram, pool.avail_ram_pool)
    return cpu, ram


@register_scheduler(key="scheduler_none_021")
def scheduler_none_021(s, results, pipelines):
    """
    Priority-aware scheduler with conservative sizing + OOM backoff + aging fairness.

    Core loop:
      - Ingest new pipelines into a de-duped waiting set.
      - Update resource hints from results:
          * on failure, increase RAM (and a bit CPU) for the specific operator and requeue.
      - For each pool, repeatedly pick the best next pipeline (by priority then age),
        assign exactly one ready operator with conservative resources, and continue
        until the pool is saturated or no ready work exists.
    """
    s.tick += 1

    # Ingest newly arrived pipelines
    for p in pipelines:
        pid = p.pipeline_id
        s.waiting[pid] = p
        if pid not in s.enqueue_time:
            s.enqueue_time[pid] = s.tick

    # Learn from results (especially OOM)
    # We avoid guessing error types; any failure triggers a cautious bump.
    for r in results:
        if not r.failed():
            continue
        if not getattr(r, "ops", None):
            continue
        # Some results may include multiple ops; apply hint to each
        for op in r.ops:
            pid = getattr(op, "pipeline_id", None)
            # If op doesn't carry pipeline_id, we can't key reliably; fall back to r.container_id tuple
            pipeline_id = pid if pid is not None else None
            # We'll still store a hint keyed by (unknown pipeline, op_id) which may not be reused.
            opkey = (pipeline_id, _op_id(op))
            prev = s.op_hints.get(opkey, {})
            prev_ram = prev.get("ram", r.ram if getattr(r, "ram", None) is not None else 0.0)
            prev_cpu = prev.get("cpu", r.cpu if getattr(r, "cpu", None) is not None else 0.0)

            new_ram = prev_ram * s.oom_ram_backoff if prev_ram else 1.0
            new_cpu = prev_cpu * s.oom_cpu_backoff if prev_cpu else s.min_cpu

            # Keep hints bounded by pool maxima at scheduling time; store raw here
            s.op_hints[opkey] = {"ram": new_ram, "cpu": new_cpu}

    # Early exit if nothing to do
    if not pipelines and not results and not s.waiting:
        return [], []

    suspensions = []  # no preemption in this incremental policy
    assignments = []

    # Clean out completed/failed pipelines from waiting set
    to_delete = []
    for pid, p in s.waiting.items():
        st = p.runtime_status()
        has_failures = st.state_counts.get(OperatorState.FAILED, 0) > 0
        if st.is_pipeline_successful() or has_failures:
            to_delete.append(pid)
    for pid in to_delete:
        s.waiting.pop(pid, None)
        s.enqueue_time.pop(pid, None)

    # If no pools, nothing we can do
    if s.executor.num_pools <= 0:
        return suspensions, assignments

    # Build a sorted list of candidate pipelines (we'll re-sort occasionally as ages change)
    # We'll pop from the front as we schedule; unselected stay in waiting.
    # For multi-pool, we choose best pool per assignment, not fixed per pipeline.
    while True:
        # Find any pool with headroom
        any_headroom = False
        for pool_id in range(s.executor.num_pools):
            pool = s.executor.pools[pool_id]
            if pool.avail_cpu_pool > 0 and pool.avail_ram_pool > 0:
                any_headroom = True
                break
        if not any_headroom:
            break

        # Select next schedulable pipeline by priority/aging that has a ready operator
        candidates = list(s.waiting.values())
        if not candidates:
            break
        candidates.sort(key=lambda p: _pipeline_sort_key(s, p))

        chosen = None
        chosen_ops = None
        chosen_pool_id = None
        chosen_cpu = None
        chosen_ram = None

        for p in candidates:
            st = p.runtime_status()
            # Only schedule operators whose parents are complete
            op_list = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
            if not op_list:
                continue

            # Choose a pool with most headroom (simple heuristic)
            pool_id = _pick_pool_for_priority(s, p.priority)
            pool = s.executor.pools[pool_id]
            if pool.avail_cpu_pool <= 0 or pool.avail_ram_pool <= 0:
                continue

            cpu, ram = _desired_resources(s, pool, p.pipeline_id, op_list[0], p.priority)
            if cpu <= 0 or ram <= 0:
                continue

            chosen = p
            chosen_ops = op_list
            chosen_pool_id = pool_id
            chosen_cpu = cpu
            chosen_ram = ram
            break

        if chosen is None:
            # No pipeline has ready ops; stop.
            break

        assignments.append(
            Assignment(
                ops=chosen_ops,
                cpu=chosen_cpu,
                ram=chosen_ram,
                priority=chosen.priority,
                pool_id=chosen_pool_id,
                pipeline_id=chosen.pipeline_id,
            )
        )

        # Soft fairness: once we schedule something from this pipeline, reset its enqueue time
        # so others can get a chance (prevents single pipeline dominating if always ready).
        s.enqueue_time[chosen.pipeline_id] = s.tick

        # Continue loop to try to fill remaining capacity across pools

    return suspensions, assignments
