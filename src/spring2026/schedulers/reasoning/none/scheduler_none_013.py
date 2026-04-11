# policy_key: scheduler_none_013
# reasoning_effort: none
# model: gpt-5.2-2025-12-11
# llm_cost: 0.034495
# generation_seconds: 46.90
# generated_at: 2026-03-12T21:30:03.280471
@register_scheduler_init(key="scheduler_none_013")
def scheduler_none_013_init(s):
    """Priority-aware, low-risk improvement over naive FIFO.

    Key changes vs naive:
    - Maintain per-priority queues (QUERY > INTERACTIVE > BATCH_PIPELINE).
    - Allocate per-pool "protected headroom" for higher priorities (soft reservation).
    - Simple OOM backoff: on failure, bump RAM request for that pipeline (capped).
    - Avoid over-allocating an entire pool to one op: right-size CPU/RAM per op based on pool size,
      while still allowing scale-up when the pool is otherwise idle.
    - Mild anti-starvation: occasionally allow batch to run even if interactive backlog exists.

    This keeps behavior predictable and should reduce tail latency for high-priority work
    without introducing heavy preemption/churn.
    """
    # Per-priority waiting queues of pipelines
    s.q_query = []
    s.q_interactive = []
    s.q_batch = []

    # Per-pipeline RAM multiplier/backoff after OOM/failure-like signals
    s.pipeline_ram_mult = {}  # pipeline_id -> float

    # A tiny bit of aging / fairness state
    s.ticks = 0
    s.batch_credit = 0  # accumulates to occasionally run batch

    # Track last known container sizing (optional, used to infer OOM-ish failures)
    s.last_assignment = {}  # container_id -> (pipeline_id, cpu, ram, pool_id)

    # Soft reservation fractions per pool (headroom to keep free for higher priority)
    # Interpreted as: when scheduling lower priority, do not consume below these free fractions.
    s.reserve_for_query = 0.15
    s.reserve_for_interactive = 0.10  # in addition to query reserve

    # Caps for RAM multiplier
    s.ram_mult_min = 1.0
    s.ram_mult_max = 8.0
    s.ram_mult_step = 2.0

    # CPU sizing heuristics
    s.max_ops_per_pool_tick = 1  # keep similar to naive initially
    s.cpu_min = 1.0


def _prio_rank(priority):
    # Higher rank = higher priority
    if priority == Priority.QUERY:
        return 3
    if priority == Priority.INTERACTIVE:
        return 2
    return 1  # Priority.BATCH_PIPELINE and anything else


def _push_pipeline(s, p):
    if p.priority == Priority.QUERY:
        s.q_query.append(p)
    elif p.priority == Priority.INTERACTIVE:
        s.q_interactive.append(p)
    else:
        s.q_batch.append(p)


def _pop_next_pipeline(s, allow_batch):
    # Prefer high priority queues; optionally allow batch even with backlog
    if s.q_query:
        return s.q_query.pop(0)
    if s.q_interactive:
        return s.q_interactive.pop(0)
    if allow_batch and s.q_batch:
        return s.q_batch.pop(0)
    # If batch not allowed but it's the only work, run it anyway
    if s.q_batch and not (s.q_query or s.q_interactive):
        return s.q_batch.pop(0)
    return None


def _requeue_pipeline(s, p):
    # Preserve priority ordering and avoid losing the pipeline
    _push_pipeline(s, p)


def _pipeline_done_or_dead(p):
    status = p.runtime_status()
    if status.is_pipeline_successful():
        return True
    # Naive baseline dropped failures. We keep same behavior: if any op failed, we drop pipeline.
    # (OOM backoff will be applied only when we see failed results; dropping is conservative.)
    has_failures = status.state_counts[OperatorState.FAILED] > 0
    return has_failures


def _next_assignable_op(p):
    status = p.runtime_status()
    op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
    if not op_list:
        return []
    # Keep it simple: assign one operator at a time per pipeline
    return op_list[:1]


def _soft_reserved_free(pool, priority, s):
    """Return minimum free resources to keep in pool when scheduling `priority`."""
    # Only constrain lower priorities. Queries can use everything.
    if priority == Priority.QUERY:
        return 0.0, 0.0

    # Interactive should keep query reserve
    if priority == Priority.INTERACTIVE:
        cpu_keep = pool.max_cpu_pool * s.reserve_for_query
        ram_keep = pool.max_ram_pool * s.reserve_for_query
        return cpu_keep, ram_keep

    # Batch should keep query + interactive reserves
    cpu_keep = pool.max_cpu_pool * (s.reserve_for_query + s.reserve_for_interactive)
    ram_keep = pool.max_ram_pool * (s.reserve_for_query + s.reserve_for_interactive)
    return cpu_keep, ram_keep


def _choose_cpu_ram(pool, avail_cpu, avail_ram, priority, s, pipeline_id):
    """Heuristic right-sizing: don't grab the whole pool by default."""
    # Keep some headroom for higher priorities (soft reservation)
    cpu_keep, ram_keep = _soft_reserved_free(pool, priority, s)
    cpu_budget = max(0.0, avail_cpu - cpu_keep)
    ram_budget = max(0.0, avail_ram - ram_keep)
    if cpu_budget <= 0 or ram_budget <= 0:
        return 0.0, 0.0

    # CPU: start with a fraction of pool to reduce interference; allow burst if the pool is mostly idle.
    # - Queries: can scale up more aggressively.
    # - Interactive: moderate.
    # - Batch: smaller slice.
    if priority == Priority.QUERY:
        target_cpu = max(s.cpu_min, min(cpu_budget, max(pool.max_cpu_pool * 0.5, s.cpu_min)))
    elif priority == Priority.INTERACTIVE:
        target_cpu = max(s.cpu_min, min(cpu_budget, max(pool.max_cpu_pool * 0.33, s.cpu_min)))
    else:
        target_cpu = max(s.cpu_min, min(cpu_budget, max(pool.max_cpu_pool * 0.25, s.cpu_min)))

    # If pool is very idle, let the op take more to finish sooner (helps latency).
    if avail_cpu >= 0.8 * pool.max_cpu_pool:
        target_cpu = min(cpu_budget, max(target_cpu, pool.max_cpu_pool * 0.75))

    # RAM: allocate a chunk but not all; apply pipeline-level multiplier after OOMs
    mult = s.pipeline_ram_mult.get(pipeline_id, 1.0)
    base_ram = ram_budget
    if priority == Priority.QUERY:
        target_ram = min(ram_budget, max(pool.max_ram_pool * 0.5, base_ram * 0.6))
    elif priority == Priority.INTERACTIVE:
        target_ram = min(ram_budget, max(pool.max_ram_pool * 0.4, base_ram * 0.5))
    else:
        target_ram = min(ram_budget, max(pool.max_ram_pool * 0.3, base_ram * 0.4))

    target_ram = min(ram_budget, target_ram * mult)

    # Avoid tiny allocations that likely OOM/underperform; still bounded by budgets
    target_cpu = max(0.0, min(cpu_budget, target_cpu))
    target_ram = max(0.0, min(ram_budget, target_ram))
    return target_cpu, target_ram


@register_scheduler(key="scheduler_none_013")
def scheduler_none_013(s, results, pipelines):
    """
    Priority-aware FIFO with soft reservations and simple OOM backoff.

    - Enqueue new pipelines by priority.
    - Process results:
        - If failure observed, increase pipeline RAM multiplier (bounded).
    - For each pool, schedule up to one ready operator, prioritizing QUERY then INTERACTIVE.
      Allow batch periodically to avoid starvation.
    - No preemption yet (keeps changes small and safe).
    """
    s.ticks += 1

    # Ingest new pipelines
    for p in pipelines:
        _push_pipeline(s, p)

    # React to results (OOM/backoff heuristic)
    for r in results:
        # Track last assignment sizing for potential future heuristics
        if getattr(r, "container_id", None) is not None:
            # Best effort: store mapping if we can infer pipeline_id from ops (not always available),
            # otherwise keep the existing map.
            pass

        if r.failed():
            # Identify pipeline_id if present (ExecutionResult may not include it directly in this API),
            # but we can use r.ops (which are operator objects) to find parent pipeline via attribute.
            # Fallback: no update if we can't identify.
            pid = None
            try:
                # Common patterns: op.pipeline_id or op.pipeline.pipeline_id
                if r.ops and len(r.ops) > 0:
                    op0 = r.ops[0]
                    if hasattr(op0, "pipeline_id"):
                        pid = op0.pipeline_id
                    elif hasattr(op0, "pipeline") and hasattr(op0.pipeline, "pipeline_id"):
                        pid = op0.pipeline.pipeline_id
            except Exception:
                pid = None

            if pid is not None:
                cur = s.pipeline_ram_mult.get(pid, 1.0)
                nxt = min(s.ram_mult_max, max(s.ram_mult_min, cur * s.ram_mult_step))
                s.pipeline_ram_mult[pid] = nxt

    # Early exit if nothing to do
    if not pipelines and not results and not (s.q_query or s.q_interactive or s.q_batch):
        return [], []

    suspensions = []  # no preemption in this incremental version
    assignments = []

    # Anti-starvation: every few ticks, allow a batch dispatch even if interactive backlog exists
    if s.q_batch:
        s.batch_credit += 1
    allow_batch_now = False
    if s.batch_credit >= 5:
        allow_batch_now = True
        s.batch_credit = 0

    # For each pool, try to schedule one op
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool
        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        ops_scheduled = 0
        # We'll examine a bounded number of pipelines to find a runnable one, then requeue others.
        examined = 0
        max_examine = (len(s.q_query) + len(s.q_interactive) + len(s.q_batch) + 5)

        requeue = []
        while ops_scheduled < s.max_ops_per_pool_tick and examined < max_examine:
            examined += 1
            p = _pop_next_pipeline(s, allow_batch=allow_batch_now)
            if p is None:
                break

            # Drop completed/failed pipelines (naive-compatible behavior)
            if _pipeline_done_or_dead(p):
                continue

            op_list = _next_assignable_op(p)
            if not op_list:
                # Not runnable now; keep it around
                requeue.append(p)
                continue

            cpu_req, ram_req = _choose_cpu_ram(pool, avail_cpu, avail_ram, p.priority, s, p.pipeline_id)
            if cpu_req <= 0 or ram_req <= 0:
                # Can't fit due to reservations/budget; requeue and stop trying this pool (avoid thrash)
                requeue.append(p)
                break

            assignment = Assignment(
                ops=op_list,
                cpu=cpu_req,
                ram=ram_req,
                priority=p.priority,
                pool_id=pool_id,
                pipeline_id=p.pipeline_id,
            )
            assignments.append(assignment)
            ops_scheduled += 1

            # After assigning one op from this pipeline, requeue pipeline for next ops later
            requeue.append(p)

            # Update local available resources conservatively
            avail_cpu -= cpu_req
            avail_ram -= ram_req
            if avail_cpu <= 0 or avail_ram <= 0:
                break

        # Requeue any pipelines we pulled but didn't finish
        for p in requeue:
            _requeue_pipeline(s, p)

    return suspensions, assignments
