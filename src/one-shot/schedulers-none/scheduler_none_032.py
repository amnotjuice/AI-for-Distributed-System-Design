# policy_key: scheduler_none_032
# reasoning_effort: none
# model: gpt-5.2-2025-12-11
# llm_cost: 0.037099
# generation_seconds: 34.73
# generated_at: 2026-03-14T02:04:52.375205
@register_scheduler_init(key="scheduler_none_032")
def scheduler_none_032_init(s):
    """Priority-aware, low-churn scheduler.

    Incremental improvements over naive FIFO:
      1) Maintain separate queues per priority and always pick highest priority first.
      2) Use conservative, per-op resource sizing (avoid "give everything to one op").
      3) Simple OOM-aware RAM backoff: if an op OOMs, retry it with larger RAM next time.
      4) Soft fairness: occasional batch picks to avoid indefinite starvation.

    Notes/assumptions:
      - We avoid preemption initially to prevent churn and wasted work; this is a "small improvement first".
      - We schedule at most one operator per pool per tick (like the example) to keep behavior stable.
    """
    from collections import deque

    # Separate waiting queues per priority
    s.q_query = deque()
    s.q_interactive = deque()
    s.q_batch = deque()

    # Track per-(pipeline, op) observed failures and resource hints
    # Key is (pipeline_id, op_id) where op_id is taken from op.op_id if present else id(op)
    s.op_hints = {}  # (pid, op_key) -> {"ram": float, "cpu": float, "ooms": int}

    # Soft fairness knobs
    s.batch_credit = 0
    s.batch_credit_max = 3  # after N high-pri picks, allow one batch pick
    s.batch_pick_period = 4  # try to pick batch every K picks if available

    # Minimal/default sizing caps (fractions of pool)
    s.default_cpu_frac = 0.5
    s.default_ram_frac = 0.5

    # Bounds for backoff on OOM
    s.oom_ram_growth = 1.6
    s.max_ram_frac = 0.9
    s.max_cpu_frac = 0.9
    s.min_cpu = 1.0  # don't request less than 1 vCPU if available
    s.min_ram = 0.5  # don't request less than 0.5 GiB (unit-agnostic, consistent with sim)


def _prio_rank(prio):
    # Higher number => higher priority
    if prio == Priority.QUERY:
        return 3
    if prio == Priority.INTERACTIVE:
        return 2
    return 1  # Priority.BATCH_PIPELINE or others


def _get_queue(s, prio):
    if prio == Priority.QUERY:
        return s.q_query
    if prio == Priority.INTERACTIVE:
        return s.q_interactive
    return s.q_batch


def _op_key(op):
    # Stable key if op_id exists, else fallback to object identity
    return getattr(op, "op_id", None) or getattr(op, "operator_id", None) or id(op)


def _is_oom_error(err):
    if not err:
        return False
    msg = str(err).lower()
    return ("oom" in msg) or ("out of memory" in msg) or ("memory" in msg and "killed" in msg)


def _choose_next_pipeline(s):
    # Soft fairness: mostly serve high priority, but ensure occasional batch progress.
    # We treat QUERY > INTERACTIVE > BATCH.
    has_q = len(s.q_query) > 0
    has_i = len(s.q_interactive) > 0
    has_b = len(s.q_batch) > 0

    if not (has_q or has_i or has_b):
        return None

    # If only batch exists, pick it
    if not (has_q or has_i):
        return s.q_batch.popleft()

    # Occasionally pick batch if it exists and we've recently served high-priority a lot
    s.batch_credit += 1
    if has_b and (s.batch_credit >= s.batch_credit_max or (s.batch_credit % s.batch_pick_period == 0)):
        s.batch_credit = 0
        return s.q_batch.popleft()

    # Otherwise strict priority among high-priority queues
    if has_q:
        return s.q_query.popleft()
    if has_i:
        return s.q_interactive.popleft()
    return s.q_batch.popleft()


def _base_request_for_pool(s, pool):
    # Conservative per-op sizing rather than "take all available"
    # Start with fractions of *available*, capped by fractions of *max* to avoid huge spikes.
    avail_cpu = pool.avail_cpu_pool
    avail_ram = pool.avail_ram_pool

    max_cpu = pool.max_cpu_pool
    max_ram = pool.max_ram_pool

    cpu_cap = max_cpu * s.max_cpu_frac
    ram_cap = max_ram * s.max_ram_frac

    cpu = min(avail_cpu, max(s.min_cpu, min(cpu_cap, avail_cpu * s.default_cpu_frac)))
    ram = min(avail_ram, max(s.min_ram, min(ram_cap, avail_ram * s.default_ram_frac)))

    # Ensure non-negative
    cpu = max(0.0, cpu)
    ram = max(0.0, ram)
    return cpu, ram


def _apply_hints(s, pipeline_id, ops, cpu, ram, pool):
    # If we have hints for a single op, adjust allocation upward if needed.
    # For multiple ops, keep conservative baseline (avoid over-allocating).
    if not ops:
        return cpu, ram
    if len(ops) != 1:
        return cpu, ram

    op = ops[0]
    key = (pipeline_id, _op_key(op))
    hint = s.op_hints.get(key)
    if not hint:
        return cpu, ram

    # Respect pool caps
    max_cpu = pool.max_cpu_pool * s.max_cpu_frac
    max_ram = pool.max_ram_pool * s.max_ram_frac

    # Raise to hinted levels but not beyond available
    cpu2 = min(pool.avail_cpu_pool, min(max_cpu, max(cpu, float(hint.get("cpu", cpu)))))
    ram2 = min(pool.avail_ram_pool, min(max_ram, max(ram, float(hint.get("ram", ram)))))
    return cpu2, ram2


@register_scheduler(key="scheduler_none_032")
def scheduler_none_032(s, results, pipelines):
    """
    Priority-aware scheduler with OOM backoff and soft fairness.

    Behavior:
      - Enqueue new pipelines into per-priority queues.
      - Process results to learn per-op hints (OOM => increase RAM next retry).
      - For each pool, schedule at most one ready operator from the highest-priority available pipeline.
      - Avoid preemption for now (small, safe improvement).
    """
    # Enqueue arriving pipelines
    for p in pipelines:
        _get_queue(s, p.priority).append(p)

    # Early exit if nothing changed
    if not pipelines and not results:
        return [], []

    # Learn from results: update per-op hints for OOMs, and remember last used cpu/ram for successes
    for r in results:
        if not getattr(r, "ops", None):
            continue
        # Only learn per-op if exactly one op was run; otherwise ambiguous
        if len(r.ops) != 1:
            continue
        op = r.ops[0]
        pid = getattr(r, "pipeline_id", None)
        # If pipeline_id isn't available on result, we can't key hints reliably
        if pid is None:
            continue

        key = (pid, _op_key(op))
        hint = s.op_hints.get(key, {"ram": float(getattr(r, "ram", 0.0) or 0.0),
                                    "cpu": float(getattr(r, "cpu", 0.0) or 0.0),
                                    "ooms": 0})

        if r.failed() and _is_oom_error(getattr(r, "error", None)):
            hint["ooms"] = int(hint.get("ooms", 0)) + 1
            # Increase RAM request for next attempt, keep CPU same (RAM-first for OOM)
            prev_ram = float(getattr(r, "ram", 0.0) or hint.get("ram", 0.0) or 0.0)
            if prev_ram <= 0:
                prev_ram = s.min_ram
            hint["ram"] = prev_ram * (s.oom_ram_growth ** 1)  # one-step growth per observed OOM
            # Keep CPU at least what we had (avoid regressing)
            prev_cpu = float(getattr(r, "cpu", 0.0) or hint.get("cpu", 0.0) or 0.0)
            hint["cpu"] = max(hint.get("cpu", 0.0), prev_cpu)
        else:
            # On non-OOM completion/failure, keep last allocation as a reasonable baseline hint
            hint["ram"] = max(float(hint.get("ram", 0.0)), float(getattr(r, "ram", 0.0) or 0.0))
            hint["cpu"] = max(float(hint.get("cpu", 0.0)), float(getattr(r, "cpu", 0.0) or 0.0))

        s.op_hints[key] = hint

    suspensions = []
    assignments = []

    # We'll pop pipelines to consider, but requeue those that can't run yet / already done.
    # Bound per tick processing to avoid O(N^2) scanning under load.
    max_pulls = 200
    pulled = 0

    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool
        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # Choose a pipeline by priority/fairness that has an assignable op
        chosen_pipeline = None
        chosen_ops = None

        # We'll temporarily hold pipelines we looked at but didn't schedule
        deferred = []

        while pulled < max_pulls:
            pulled += 1
            pipeline = _choose_next_pipeline(s)
            if pipeline is None:
                break

            status = pipeline.runtime_status()
            has_failures = status.state_counts[OperatorState.FAILED] > 0

            if status.is_pipeline_successful() or has_failures:
                # Drop completed/failed pipelines (no retries for non-OOM failures in this incremental policy)
                continue

            # Find first ready operator whose parents are complete
            op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
            if not op_list:
                # Not runnable yet; keep it in its priority queue
                deferred.append(pipeline)
                continue

            chosen_pipeline = pipeline
            chosen_ops = op_list
            break

        # Requeue deferred pipelines back into their respective queues
        for p in deferred:
            _get_queue(s, p.priority).append(p)

        if chosen_pipeline is None or not chosen_ops:
            continue

        # Compute conservative resource request, then apply hints (e.g., post-OOM RAM bump)
        cpu_req, ram_req = _base_request_for_pool(s, pool)
        cpu_req, ram_req = _apply_hints(s, chosen_pipeline.pipeline_id, chosen_ops, cpu_req, ram_req, pool)

        # If we still can't fit minimum positive requests, skip (requeue pipeline)
        if cpu_req <= 0 or ram_req <= 0:
            _get_queue(s, chosen_pipeline.priority).appendleft(chosen_pipeline)
            continue

        # Issue assignment
        assignments.append(
            Assignment(
                ops=chosen_ops,
                cpu=cpu_req,
                ram=ram_req,
                priority=chosen_pipeline.priority,
                pool_id=pool_id,
                pipeline_id=chosen_pipeline.pipeline_id,
            )
        )

        # Requeue the pipeline for its next ops in future ticks
        _get_queue(s, chosen_pipeline.priority).append(chosen_pipeline)

    return suspensions, assignments
