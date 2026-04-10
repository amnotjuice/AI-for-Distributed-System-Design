# source: scheduler_low_011.py
# model: gpt-5.2-2025-12-11
# effort: low
# feedback: minimal
# iteration: r13
@register_scheduler_init(key="scheduler_low_011_r13")
def scheduler_low_011_r13_init(s):
    """Priority-first scheduler with small-but-real latency optimizations.

    Iteration goals vs. previous naive-ish policy:
    1) Reduce high-priority queueing by enabling *multiple* high-priority assignments per pool per tick
       (instead of at most one), using smaller per-op "bundles" to increase concurrency.
    2) Fix an obvious flaw: learn OOM retry hints reliably by maintaining an op->pipeline mapping,
       since ExecutionResult does not guarantee a pipeline_id field.
    3) Keep changes low-risk: no preemption, simple pool preference, bounded OOM retries.

    Design summary:
    - Separate FIFO queues per priority (QUERY > INTERACTIVE > BATCH_PIPELINE).
    - Schedule high-priority work first across pools (prefer pool 0 as "interactive" pool).
    - Use per-op resource bundles based on backlog to trade off concurrency vs. runtime.
    - On OOM-like failures, retry the same op with increased RAM (doubling) up to global max, bounded retries.
    - Non-OOM failures mark the pipeline terminal (drop from scheduling).
    """
    s.waiting_queues = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }
    # Track membership to avoid duplicate enqueues and reduce wasted scanning
    s.queue_members = {
        Priority.QUERY: set(),
        Priority.INTERACTIVE: set(),
        Priority.BATCH_PIPELINE: set(),
    }

    # Learned per-operator hints (mainly RAM) keyed by (pipeline_id, id(op))
    s.op_hints = {}       # k -> {"ram": float, "cpu": float}
    s.op_attempts = {}    # k -> int

    # Map operator identity back to pipeline for reliable hint updates from ExecutionResult
    s.op_to_pipeline = {}  # id(op) -> pipeline_id

    # Pipelines that experienced a non-retryable failure (or exceeded retry budget)
    s.terminal_pipelines = set()

    # Config knobs
    s.max_retries_per_op = 3
    s.interactive_pool_id = 0

    # Tick counter (useful for mild anti-starvation if extended later)
    s.tick = 0


def _prio_order_high_to_low():
    return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]


def _is_oom_error(err):
    if err is None:
        return False
    msg = str(err).lower()
    return ("oom" in msg) or ("out of memory" in msg) or ("memoryerror" in msg)


def _op_key(pipeline_id, op):
    return (pipeline_id, id(op))


def _global_max_ram(s):
    m = 0.0
    for i in range(s.executor.num_pools):
        try:
            m = max(m, float(s.executor.pools[i].max_ram_pool))
        except Exception:
            pass
    return max(1.0, m)


def _enqueue_pipeline(s, p):
    # Defensive: unknown priority -> treat as batch
    pr = p.priority if p.priority in s.waiting_queues else Priority.BATCH_PIPELINE
    pid = p.pipeline_id
    if pid in s.queue_members[pr]:
        return
    s.waiting_queues[pr].append(p)
    s.queue_members[pr].add(pid)


def _dequeue_pipeline(s, pr):
    """Pop from FIFO; caller must re-enqueue explicitly if needed."""
    q = s.waiting_queues[pr]
    while q:
        p = q.pop(0)
        s.queue_members[pr].discard(p.pipeline_id)
        return p
    return None


def _pipeline_is_done_or_terminal(s, p):
    if p.pipeline_id in s.terminal_pipelines:
        return True
    st = p.runtime_status()
    if st.is_pipeline_successful():
        return True
    return False


def _next_ready_op(p):
    st = p.runtime_status()
    ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
    if not ops:
        return None
    # Take the first ready op (FIFO-ish within the DAG frontier)
    return ops[0]


def _high_backlog(s):
    return len(s.waiting_queues[Priority.QUERY]) + len(s.waiting_queues[Priority.INTERACTIVE])


def _pool_order_for_high(s):
    if s.executor.num_pools <= 1:
        return list(range(s.executor.num_pools))
    ip = s.interactive_pool_id
    return [ip] + [i for i in range(s.executor.num_pools) if i != ip]


def _pool_order_for_batch(s, high_backlog_now):
    if s.executor.num_pools <= 1:
        return list(range(s.executor.num_pools))
    ip = s.interactive_pool_id
    # If high-priority backlog exists, keep batch off the interactive pool to protect tail latency
    if high_backlog_now > 0:
        return [i for i in range(s.executor.num_pools) if i != ip]
    return list(range(s.executor.num_pools))


def _bundle_request(pool, priority, high_backlog_now):
    """Choose a per-op CPU/RAM bundle to balance concurrency vs runtime.

    - High priority: smaller bundles when backlog is high => run more in parallel to reduce queueing.
    - Low priority: larger bundles to finish throughput work quickly (but typically in non-interactive pools).
    """
    avail_cpu = float(pool.avail_cpu_pool)
    avail_ram = float(pool.avail_ram_pool)
    max_cpu = float(pool.max_cpu_pool)
    max_ram = float(pool.max_ram_pool)

    if priority in (Priority.QUERY, Priority.INTERACTIVE):
        if high_backlog_now >= 6:
            cpu = min(2.0, max_cpu * 0.25)
            ram = max_ram * 0.25
        elif high_backlog_now >= 3:
            cpu = min(3.0, max_cpu * 0.33)
            ram = max_ram * 0.33
        else:
            # Low backlog: give more to reduce runtime and completion latency
            cpu = min(4.0, max_cpu * 0.5)
            ram = max_ram * 0.5

        cpu = max(1.0, cpu)
        ram = max(1.0, ram)
    else:
        # Batch: bigger container (scale-up bias), but bounded by pool size
        cpu = max(1.0, max_cpu)
        ram = max(1.0, max_ram)

    # Cap by currently available
    cpu = min(cpu, avail_cpu)
    ram = min(ram, avail_ram)
    return cpu, ram


def _apply_hints(s, pool, pipeline_id, op, cpu, ram):
    """Apply learned RAM/CPU hints (primarily for OOM retries), capped by pool limits and availability."""
    k = _op_key(pipeline_id, op)
    hint = s.op_hints.get(k)
    if hint:
        try:
            ram = max(ram, float(hint.get("ram", ram)))
        except Exception:
            pass
        try:
            cpu = max(cpu, float(hint.get("cpu", cpu)))
        except Exception:
            pass

    # Final caps
    cpu = min(max(1.0, cpu), float(pool.avail_cpu_pool), float(pool.max_cpu_pool))
    ram = min(max(1.0, ram), float(pool.avail_ram_pool), float(pool.max_ram_pool))
    return cpu, ram


def _record_assignment_mapping(s, pipeline_id, ops):
    # ExecutionResult will return op objects; map them to pipeline_id for hint updates.
    for op in (ops or []):
        s.op_to_pipeline[id(op)] = pipeline_id


def _update_hints_from_results(s, results):
    """Learn from OOM failures and mark terminal pipelines on non-retryable failures."""
    gmax_ram = _global_max_ram(s)

    for r in results:
        # Determine failure
        failed = False
        try:
            failed = bool(r.failed())
        except Exception:
            failed = getattr(r, "error", None) is not None

        ops = getattr(r, "ops", None) or []
        if not ops:
            continue

        # If failed due to OOM, bump RAM hint for retry; else mark pipeline terminal.
        is_oom = failed and _is_oom_error(getattr(r, "error", None))

        for op in ops:
            pid = s.op_to_pipeline.pop(id(op), None)
            if pid is None:
                # If we can't map, we can't safely update per-pipeline hints; skip.
                continue

            k = _op_key(pid, op)

            if not failed:
                # On success, we can clear retry attempt counters (hints are harmless but not needed).
                s.op_attempts.pop(k, None)
                continue

            if is_oom:
                prev_attempts = int(s.op_attempts.get(k, 0)) + 1
                s.op_attempts[k] = prev_attempts

                # Baseline is the RAM used for the failed attempt if present, else existing hint, else 1
                baseline_ram = 1.0
                try:
                    baseline_ram = float(getattr(r, "ram", 1.0) or 1.0)
                except Exception:
                    pass
                if k in s.op_hints:
                    try:
                        baseline_ram = max(baseline_ram, float(s.op_hints[k].get("ram", baseline_ram)))
                    except Exception:
                        pass

                new_ram = min(gmax_ram, max(1.0, baseline_ram * 2.0))

                # Keep CPU hint at least what we used (don't decrease)
                baseline_cpu = 1.0
                try:
                    baseline_cpu = float(getattr(r, "cpu", 1.0) or 1.0)
                except Exception:
                    pass
                if k in s.op_hints:
                    try:
                        baseline_cpu = max(baseline_cpu, float(s.op_hints[k].get("cpu", baseline_cpu)))
                    except Exception:
                        pass

                s.op_hints[k] = {"ram": new_ram, "cpu": max(1.0, baseline_cpu)}

                # If we exceeded retry budget, stop retrying this pipeline to avoid infinite churn
                if prev_attempts > int(s.max_retries_per_op):
                    s.terminal_pipelines.add(pid)
            else:
                # Non-OOM failure: treat as terminal for this pipeline (low-risk policy)
                s.terminal_pipelines.add(pid)


def _schedule_from_priority_queue(s, pool_id, priority, max_assignments, high_backlog_now):
    """Try to schedule up to max_assignments ops of a single priority on a pool."""
    pool = s.executor.pools[pool_id]
    assignments = []

    # Scan each queued pipeline at most once to avoid O(n^2) blowups under heavy load.
    qlen = len(s.waiting_queues[priority])
    deferred = []

    for _ in range(qlen):
        if len(assignments) >= max_assignments:
            break
        if float(pool.avail_cpu_pool) <= 0.0 or float(pool.avail_ram_pool) <= 0.0:
            break

        p = _dequeue_pipeline(s, priority)
        if p is None:
            break

        if _pipeline_is_done_or_terminal(s, p):
            # Drop it (do not re-enqueue)
            continue

        op = _next_ready_op(p)
        if op is None:
            # Not ready; rotate to avoid head-of-line blocking
            deferred.append(p)
            continue

        # Bundle request based on backlog and priority, then apply hints
        cpu, ram = _bundle_request(pool, priority, high_backlog_now)
        cpu, ram = _apply_hints(s, pool, p.pipeline_id, op, cpu, ram)

        # If it still doesn't fit, defer (don't spin)
        if cpu > float(pool.avail_cpu_pool) or ram > float(pool.avail_ram_pool):
            deferred.append(p)
            continue

        ops = [op]
        assignments.append(
            Assignment(
                ops=ops,
                cpu=cpu,
                ram=ram,
                priority=priority,
                pool_id=pool_id,
                pipeline_id=p.pipeline_id,
            )
        )
        _record_assignment_mapping(s, p.pipeline_id, ops)

        # Re-enqueue pipeline so future ops can run later
        deferred.append(p)

        # Locally decrement the pool availability to pack multiple assignments in one tick deterministically.
        # (The executor will also account for this; this local update prevents overscheduling within this call.)
        try:
            pool.avail_cpu_pool = float(pool.avail_cpu_pool) - float(cpu)
            pool.avail_ram_pool = float(pool.avail_ram_pool) - float(ram)
        except Exception:
            # If pool availability is read-only in this environment, we rely on conservative max_assignments
            # and available checks to limit overscheduling.
            pass

    # Put deferred pipelines back at the end (rotation)
    for p in deferred:
        _enqueue_pipeline(s, p)

    return assignments


@register_scheduler(key="scheduler_low_011_r13")
def scheduler_low_011_r13(s, results, pipelines):
    """
    Priority-aware, OOM-retrying, concurrency-boosting scheduler.

    Returns:
        (suspensions, assignments)
    """
    s.tick = int(getattr(s, "tick", 0)) + 1

    # Enqueue new arrivals
    for p in pipelines:
        _enqueue_pipeline(s, p)

    # Learn from results (OOM retries, terminal failures)
    if results:
        _update_hints_from_results(s, results)

    # Early exit if no new info
    if not pipelines and not results:
        return [], []

    suspensions = []  # no preemption in this iteration
    assignments = []

    high_backlog_now = _high_backlog(s)

    # 1) Schedule high priority first, preferring interactive pool ordering.
    for pool_id in _pool_order_for_high(s):
        pool = s.executor.pools[pool_id]
        if float(pool.avail_cpu_pool) <= 0.0 or float(pool.avail_ram_pool) <= 0.0:
            continue

        # Allow more concurrency for high priority; cap to avoid overscheduling when pool accounting is read-only.
        # Query first, then interactive.
        assignments += _schedule_from_priority_queue(
            s, pool_id, Priority.QUERY, max_assignments=4, high_backlog_now=high_backlog_now
        )
        assignments += _schedule_from_priority_queue(
            s, pool_id, Priority.INTERACTIVE, max_assignments=4, high_backlog_now=high_backlog_now
        )

    # 2) Schedule batch on remaining capacity (avoid interactive pool if high backlog exists).
    # Keep this conservative: at most 1 batch assignment per pool per tick to reduce interference.
    for pool_id in _pool_order_for_batch(s, high_backlog_now):
        pool = s.executor.pools[pool_id]
        if float(pool.avail_cpu_pool) <= 0.0 or float(pool.avail_ram_pool) <= 0.0:
            continue

        assignments += _schedule_from_priority_queue(
            s, pool_id, Priority.BATCH_PIPELINE, max_assignments=1, high_backlog_now=high_backlog_now
        )

    return suspensions, assignments