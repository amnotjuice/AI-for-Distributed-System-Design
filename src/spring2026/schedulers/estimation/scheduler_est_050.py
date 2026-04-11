# policy_key: scheduler_est_050
# reasoning_effort: low
# model: gpt-5.2-2025-12-11
# llm_cost: 0.038002
# generation_seconds: 44.07
# generated_at: 2026-04-01T00:18:04.524673
@register_scheduler_init(key="scheduler_est_050")
def scheduler_est_050_init(s):
    """Priority-aware, estimate-aware scheduler (incremental improvement over naive FIFO).

    Main ideas:
    - Maintain separate waiting queues per priority; always prefer higher priority.
    - Avoid "give everything to one op": cap CPU per op to allow concurrency (better latency).
    - Use op.estimate.mem_peak_gb as a RAM floor to reduce OOMs; on failure, backoff RAM.
    - Keep fairness: periodically allow lower priorities to run (light-weight weighted RR).
    """
    # Per-priority FIFO pipeline queues
    s.waiting_by_prio = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }

    # Active pipelines by id (lets us deduplicate arrivals and manage lifecycle)
    s.active = {}

    # Memory backoff per (pipeline_id, op_key)
    s.mem_backoff_factor = {}  # (pipeline_id, op_key) -> factor (>=1.0)
    s.op_fail_counts = {}      # (pipeline_id, op_key) -> int

    # Small, safe caps to enable parallelism and reduce tail latency under contention
    s.cpu_cap_query = 4.0
    s.cpu_cap_interactive = 4.0
    s.cpu_cap_batch = 2.0

    # Weighted scheduling budget across priorities per tick (simple and deterministic)
    # Higher = more chances to place ops in this tick when resources allow.
    s.tick_weights = {
        Priority.QUERY: 6,
        Priority.INTERACTIVE: 4,
        Priority.BATCH_PIPELINE: 1,
    }
    s._tick_budget = dict(s.tick_weights)

    # Guardrails for retries
    s.max_op_retries = 3
    s.max_backoff_factor = 8.0


def _op_key(op):
    # Try common stable identifiers; fallback to repr.
    for attr in ("operator_id", "op_id", "id", "name"):
        v = getattr(op, attr, None)
        if v is not None:
            return (attr, v)
    return ("repr", repr(op))


def _prio_order():
    return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]


def _cpu_cap_for_prio(s, prio):
    if prio == Priority.QUERY:
        return float(s.cpu_cap_query)
    if prio == Priority.INTERACTIVE:
        return float(s.cpu_cap_interactive)
    return float(s.cpu_cap_batch)


def _enqueue_pipeline(s, p):
    # Deduplicate by pipeline_id; append to its priority queue if new.
    if p.pipeline_id in s.active:
        return
    s.active[p.pipeline_id] = p
    s.waiting_by_prio[p.priority].append(p)


def _drop_pipeline(s, pipeline_id):
    # Remove from active; it will naturally drain from queues (we skip if not active).
    if pipeline_id in s.active:
        del s.active[pipeline_id]


def _refill_tick_budget(s):
    # Reset per-tick selection budgets.
    s._tick_budget = dict(s.tick_weights)


def _choose_next_pipeline(s):
    # Choose next priority with remaining budget and a non-empty queue.
    for prio in _prio_order():
        if s._tick_budget.get(prio, 0) <= 0:
            continue
        q = s.waiting_by_prio.get(prio, [])
        # Clean leading stale entries (completed/removed) lazily.
        while q and q[0].pipeline_id not in s.active:
            q.pop(0)
        if q:
            s._tick_budget[prio] -= 1
            return q.pop(0)
    return None


def _estimate_ram_gb(op):
    est = getattr(op, "estimate", None)
    if est is None:
        return None
    v = getattr(est, "mem_peak_gb", None)
    try:
        if v is None:
            return None
        v = float(v)
        if v <= 0:
            return None
        return v
    except Exception:
        return None


def _pick_resources(s, pool, pipeline, op):
    # RAM: use estimate as a floor, and apply backoff after failures. Extra RAM is "free",
    # but we still avoid allocating the entire pool to a single op to preserve concurrency.
    prio = pipeline.priority
    cap_cpu = _cpu_cap_for_prio(s, prio)

    # CPU: small cap to allow multiple ops concurrently; never exceed available.
    cpu = min(float(pool.avail_cpu_pool), float(cap_cpu))
    if cpu <= 0:
        return None, None

    # RAM floor from estimate/backoff
    base_est = _estimate_ram_gb(op)
    opk = (pipeline.pipeline_id, _op_key(op))
    backoff = float(s.mem_backoff_factor.get(opk, 1.0))
    if base_est is None:
        # Conservative default when we lack estimates; still modest to preserve headroom.
        ram_floor = 4.0
    else:
        # Small safety margin; backoff increases on failures.
        ram_floor = base_est * 1.15 * backoff

    # Concurrency-friendly RAM cap per op: do not take more than half the pool unless needed.
    # (If the pool is small, half still allows at least 2 concurrent ops when possible.)
    per_op_soft_cap = max(1.0, float(pool.max_ram_pool) * 0.5)

    # Final RAM allocation (must be <= available)
    ram = min(float(pool.avail_ram_pool), max(0.0, min(ram_floor, per_op_soft_cap)))

    # If we couldn't meet estimated floor due to tight pool memory, still try a minimal allocation
    # only if it leaves a chance of running; otherwise, return None to avoid immediate OOM churn.
    if ram <= 0:
        return None, None

    return cpu, ram


@register_scheduler(key="scheduler_est_050")
def scheduler_est_050_scheduler(s, results, pipelines):
    """
    Priority-aware, estimate-aware scheduling with basic retry/backoff.

    - Enqueue new pipelines by priority.
    - Process results: if an op failed, increase RAM backoff for that op (up to a cap).
    - For each pool, schedule multiple ready ops in a loop while resources remain:
        * choose pipelines by weighted priority budget
        * pick first ready op (parents complete) and allocate modest CPU + estimated RAM floor
        * requeue pipeline for future ops (unless completed)
    """
    # Add new arrivals
    for p in pipelines:
        _enqueue_pipeline(s, p)

    # Process results to update backoff/retry state
    for r in results:
        try:
            if not r.failed():
                continue
        except Exception:
            continue

        # Backoff per op in this result (results.ops is expected to be list-like)
        ops = getattr(r, "ops", None) or []
        for op in ops:
            # Map op to pipeline_id if possible; ExecutionResult doesn't include pipeline_id, so
            # use op key and apply backoff for all pipelines that might match by identity/repr.
            # Best-effort: if op is from a specific active pipeline, it will share identity in-sim.
            for pid, p in list(s.active.items()):
                opk = (pid, _op_key(op))
                # Track retries and backoff
                s.op_fail_counts[opk] = int(s.op_fail_counts.get(opk, 0)) + 1
                if s.op_fail_counts[opk] <= s.max_op_retries:
                    prev = float(s.mem_backoff_factor.get(opk, 1.0))
                    s.mem_backoff_factor[opk] = min(float(s.max_backoff_factor), prev * 2.0)

    # If nothing changed, do nothing
    if not pipelines and not results:
        return [], []

    suspensions = []  # no preemption in this incremental version
    assignments = []

    # Clean up completed pipelines (and keep queues lazy-cleaned)
    for pid, p in list(s.active.items()):
        st = p.runtime_status()
        if st.is_pipeline_successful():
            _drop_pipeline(s, pid)
        else:
            # If pipeline has any FAILED ops and exceeded retry limit for a ready op, drop it.
            # This prevents infinite loops in pathological cases.
            failed_ops = st.get_ops([OperatorState.FAILED], require_parents_complete=True)
            drop = False
            for op in failed_ops:
                opk = (pid, _op_key(op))
                if int(s.op_fail_counts.get(opk, 0)) >= int(s.max_op_retries):
                    drop = True
                    break
            if drop:
                _drop_pipeline(s, pid)

    # Schedule per pool
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]

        # Refill weighted selection budget per pool to improve latency responsiveness
        _refill_tick_budget(s)

        # Keep placing work while we have meaningful headroom
        while pool.avail_cpu_pool > 0 and pool.avail_ram_pool > 0:
            pipeline = _choose_next_pipeline(s)
            if pipeline is None:
                break

            # Pipeline could have been dropped since it was queued
            if pipeline.pipeline_id not in s.active:
                continue

            status = pipeline.runtime_status()
            if status.is_pipeline_successful():
                _drop_pipeline(s, pipeline.pipeline_id)
                continue

            # Pick at most one ready op from this pipeline (keeps good latency and fairness)
            op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
            if not op_list:
                # Not ready; requeue to check later
                s.waiting_by_prio[pipeline.priority].append(pipeline)
                continue

            op = op_list[0]

            # If this op has exceeded retry budget, drop pipeline to avoid churn
            opk = (pipeline.pipeline_id, _op_key(op))
            if int(s.op_fail_counts.get(opk, 0)) >= int(s.max_op_retries):
                _drop_pipeline(s, pipeline.pipeline_id)
                continue

            cpu, ram = _pick_resources(s, pool, pipeline, op)
            if cpu is None or ram is None:
                # Not enough resources in this pool; requeue and stop placing into this pool
                s.waiting_by_prio[pipeline.priority].append(pipeline)
                break

            assignments.append(
                Assignment(
                    ops=[op],
                    cpu=cpu,
                    ram=ram,
                    priority=pipeline.priority,
                    pool_id=pool_id,
                    pipeline_id=pipeline.pipeline_id,
                )
            )

            # Requeue pipeline for next opportunities (it might have more ready ops later)
            s.waiting_by_prio[pipeline.priority].append(pipeline)

    return suspensions, assignments
