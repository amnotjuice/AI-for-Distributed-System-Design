# source: scheduler_low_011.py
# model: gpt-5.2-2025-12-11
# effort: low
# feedback: minimal
# iteration: r20
@register_scheduler_init(key="scheduler_low_011_r20")
def scheduler_low_011_r20_init(s):
    """Iteration 2: Priority-aware FIFO with (1) correct retry semantics and (2) better packing.

    Changes vs previous iteration:
    - Do NOT drop pipelines just because they have FAILED operators; FAILED is assignable and should be retried.
      We only stop retrying when the failure is non-OOM or we've exceeded a retry budget.
    - Schedule multiple operators per pool per tick (up to available resources), prioritizing QUERY/INTERACTIVE first.
    - Keep a small headroom reservation on the "interactive" pool for high-priority arrivals, so batch doesn't crowd it out.
    - Allow spillover of high-priority work to other pools when the interactive pool can't fit it.
    - Prefer retrying FAILED ops (that are retryable) before scheduling new PENDING ops within a pipeline.
    """
    # FIFO queues per priority
    s.waiting_queues = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }

    # Learned per-op resource hints (mostly RAM on OOM)
    # key: (pipeline_id, op_identity) -> {"ram": float, "cpu": float}
    s.op_hints = {}

    # Attempts per op key for retry budgeting
    s.op_attempts = {}

    # Track whether a FAILED op is retryable (OOM) or non-retryable (other errors)
    s.retryable_failed_ops = set()     # set[(pipeline_id, id(op))]
    s.nonretryable_failed_ops = set()  # set[(pipeline_id, id(op))]

    # Retry policy
    s.max_retries_per_op = 3

    # Pool preference: pool 0 is treated as interactive if multiple pools exist
    s.interactive_pool_id = 0

    # Per-priority target sizing (fractions of pool MAX), plus vCPU caps to increase concurrency
    # (CPU sublinear scaling means "all CPUs to one op" is often not optimal for latency.)
    s.size_fracs = {
        Priority.QUERY: {"cpu": 0.50, "ram": 0.50, "cpu_cap": 4.0},
        Priority.INTERACTIVE: {"cpu": 0.50, "ram": 0.50, "cpu_cap": 4.0},
        Priority.BATCH_PIPELINE: {"cpu": 0.25, "ram": 0.85, "cpu_cap": 2.0},
    }

    # Reservation on the interactive pool to prevent batch from consuming everything when HP might arrive.
    # Applied only when scheduling batch onto the interactive pool AND there is any HP backlog.
    s.interactive_reserve_frac = {"cpu": 0.25, "ram": 0.25}

    # Bound how many pipelines we scan in a queue while searching for a placeable op (keeps tick work stable)
    s.queue_scan_limit = 64


def _priority_order():
    return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]


def _is_oom_error(err):
    if err is None:
        return False
    msg = str(err).lower()
    return ("oom" in msg) or ("out of memory" in msg) or ("memoryerror" in msg)


def _op_key(pipeline_id, op):
    return (pipeline_id, id(op))


def _safe_state_count(status, state):
    try:
        return int(status.state_counts.get(state, 0))
    except Exception:
        return 0


def _pipeline_done(status):
    try:
        return bool(status.is_pipeline_successful())
    except Exception:
        return False


def _get_retryable_failed_op(s, pipeline, status):
    # Prefer retrying FAILED operators first to unblock the pipeline.
    try:
        failed_ops = status.get_ops([OperatorState.FAILED], require_parents_complete=True) or []
    except Exception:
        failed_ops = []

    if not failed_ops:
        return None

    for op in failed_ops:
        k = _op_key(pipeline.pipeline_id, op)
        # If we recorded it as non-retryable, do not retry.
        if k in s.nonretryable_failed_ops:
            continue
        # If retryable (OOM) and within budget, retry.
        attempts = int(s.op_attempts.get(k, 0))
        if (k in s.retryable_failed_ops) and attempts <= s.max_retries_per_op:
            return op
        # If we haven't seen the failure reason, be conservative: don't spin retries blindly.
        # (We could allow 1 retry, but that risks wasting time on non-OOM failures.)
    return None


def _get_next_pending_op(status):
    try:
        pending_ops = status.get_ops([OperatorState.PENDING], require_parents_complete=True) or []
    except Exception:
        pending_ops = []
    if not pending_ops:
        return None
    return pending_ops[0]


def _pool_order_for_priority(s, priority):
    # Prefer interactive pool first for high-priority work if it exists.
    if s.executor.num_pools <= 1:
        return list(range(s.executor.num_pools))

    if priority in (Priority.QUERY, Priority.INTERACTIVE):
        return [s.interactive_pool_id] + [i for i in range(s.executor.num_pools) if i != s.interactive_pool_id]

    # Batch: avoid interactive pool unless needed.
    return [i for i in range(s.executor.num_pools) if i != s.interactive_pool_id] + [s.interactive_pool_id]


def _compute_request(s, pool, priority, pipeline_id, op, avail_cpu, avail_ram, reserve_cpu=0.0, reserve_ram=0.0):
    # Default target based on fractions of pool max, but capped to promote concurrency.
    cfg = s.size_fracs.get(priority, {"cpu": 1.0, "ram": 1.0, "cpu_cap": 9999.0})
    target_cpu = max(1.0, pool.max_cpu_pool * float(cfg["cpu"]))
    target_ram = max(1.0, pool.max_ram_pool * float(cfg["ram"]))
    target_cpu = min(float(cfg.get("cpu_cap", target_cpu)), target_cpu)

    # Apply hints (OOM retries)
    hint = s.op_hints.get(_op_key(pipeline_id, op))
    if hint:
        try:
            target_ram = max(target_ram, float(hint.get("ram", target_ram)))
        except Exception:
            pass
        try:
            target_cpu = max(1.0, min(target_cpu, float(hint.get("cpu", target_cpu))))
        except Exception:
            pass

    # Cap to what we can actually allocate after reservations
    alloc_cpu = min(target_cpu, max(0.0, avail_cpu - reserve_cpu), pool.max_cpu_pool)
    alloc_ram = min(target_ram, max(0.0, avail_ram - reserve_ram), pool.max_ram_pool)

    if alloc_cpu < 1.0 or alloc_ram < 1.0:
        return None, None

    return alloc_cpu, alloc_ram


@register_scheduler(key="scheduler_low_011_r20")
def scheduler_low_011_r20(s, results, pipelines):
    """
    Priority-first, multi-assignment scheduler with OOM-aware retry + interactive headroom.

    High-level tick flow:
    1) Enqueue new pipelines into per-priority FIFO.
    2) Update retry hints from failures:
       - OOM: mark retryable + double RAM hint + increment attempts.
       - non-OOM: mark nonretryable.
    3) Schedule in two phases:
       A) Schedule QUERY then INTERACTIVE across pools (interactive pool preferred, but spillover allowed).
       B) Schedule BATCH with a reservation on interactive pool if any HP backlog exists.
    """
    # Enqueue new arrivals
    for p in pipelines:
        pr = p.priority if p.priority in s.waiting_queues else Priority.BATCH_PIPELINE
        s.waiting_queues[pr].append(p)

    # Learn from results (mostly for OOM retry behavior)
    for r in results:
        failed = False
        try:
            failed = bool(r.failed())
        except Exception:
            failed = getattr(r, "error", None) is not None

        if not failed:
            continue

        pipeline_id = getattr(r, "pipeline_id", None)
        ops = getattr(r, "ops", None) or []

        # If pipeline_id isn't present, we cannot reliably key; skip learning rather than corrupting hints.
        if pipeline_id is None:
            continue

        is_oom = _is_oom_error(getattr(r, "error", None))

        for op in ops:
            k = _op_key(pipeline_id, op)

            if is_oom:
                # Mark retryable and bump RAM hint exponentially.
                s.retryable_failed_ops.add(k)
                # Increment attempts (budget enforced when choosing FAILED ops)
                s.op_attempts[k] = int(s.op_attempts.get(k, 0)) + 1

                prev_hint = s.op_hints.get(k, {})
                prev_ram = float(prev_hint.get("ram", 0.0) or 0.0)
                obs_ram = float(getattr(r, "ram", 0.0) or 0.0)
                baseline = prev_ram if prev_ram > 0 else (obs_ram if obs_ram > 0 else 1.0)
                new_ram = max(1.0, baseline * 2.0)

                # Keep CPU hint as the last observed/requested (doesn't need to grow on OOM)
                obs_cpu = float(getattr(r, "cpu", 1.0) or 1.0)
                prev_cpu = float(prev_hint.get("cpu", obs_cpu) or obs_cpu)
                s.op_hints[k] = {"ram": new_ram, "cpu": max(1.0, prev_cpu)}
            else:
                # Non-OOM failures: do not retry (prevents endless churn).
                s.nonretryable_failed_ops.add(k)

    # Early exit when no new info arrived (keeps sim fast)
    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    # Local available resources per pool (so we can pack multiple assignments per pool in one tick)
    local_avail = {}
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        local_avail[pool_id] = [float(pool.avail_cpu_pool), float(pool.avail_ram_pool)]

    # Track per-tick assignments to avoid assigning the same op twice before runtime state updates next tick
    assigned_ops_this_tick = set()  # set[(pipeline_id, id(op))]

    # HP backlog heuristic for interactive pool reservation (cheap signal)
    hp_backlog = (len(s.waiting_queues[Priority.QUERY]) + len(s.waiting_queues[Priority.INTERACTIVE])) > 0

    def schedule_one_from_queue(priority, pool_id, reserve_cpu=0.0, reserve_ram=0.0):
        """Try to find ONE placeable op from the priority queue and schedule it on pool_id."""
        q = s.waiting_queues[priority]
        if not q:
            return None

        pool = s.executor.pools[pool_id]
        avail_cpu, avail_ram = local_avail[pool_id]

        scan = min(len(q), int(s.queue_scan_limit))
        for _ in range(scan):
            pipeline = q.pop(0)
            status = pipeline.runtime_status()

            # Drop completed pipelines
            if _pipeline_done(status):
                continue

            # If pipeline has FAILED operators, retry only if we have a known retryable FAILED op.
            # Otherwise, if there exists any known nonretryable failed op for this pipeline, drop it.
            # (We don't have direct access to all failed op identities cheaply; we rely on our per-op marks.)
            op = _get_retryable_failed_op(s, pipeline, status)
            if op is None:
                # If there are FAILED ops but none retryable, we should likely stop.
                if _safe_state_count(status, OperatorState.FAILED) > 0:
                    # Requeue once to avoid prematurely dropping unknown failures; but prevent infinite spinning by
                    # deprioritizing it behind others (append to end). If it never becomes schedulable, it will
                    # naturally stop being picked often.
                    q.append(pipeline)
                    continue

                # Otherwise schedule next pending op.
                op = _get_next_pending_op(status)
                if op is None:
                    # Not ready (parents incomplete); keep it in the queue.
                    q.append(pipeline)
                    continue

            opk = _op_key(pipeline.pipeline_id, op)
            if opk in assigned_ops_this_tick:
                q.append(pipeline)
                continue

            # If we've exceeded retry budget, don't keep hammering the same failed op.
            if opk in s.retryable_failed_ops and int(s.op_attempts.get(opk, 0)) > s.max_retries_per_op:
                # Consider it effectively nonretryable now.
                s.nonretryable_failed_ops.add(opk)
                q.append(pipeline)
                continue

            cpu, ram = _compute_request(
                s=s,
                pool=pool,
                priority=priority,
                pipeline_id=pipeline.pipeline_id,
                op=op,
                avail_cpu=avail_cpu,
                avail_ram=avail_ram,
                reserve_cpu=reserve_cpu,
                reserve_ram=reserve_ram,
            )
            if cpu is None or ram is None:
                # Doesn't fit here right now; keep pipeline in queue.
                q.append(pipeline)
                continue

            # Commit locally and emit assignment
            local_avail[pool_id][0] = avail_cpu - cpu
            local_avail[pool_id][1] = avail_ram - ram
            assigned_ops_this_tick.add(opk)

            q.append(pipeline)  # keep pipeline alive for next ops
            return Assignment(
                ops=[op],
                cpu=cpu,
                ram=ram,
                priority=priority,
                pool_id=pool_id,
                pipeline_id=pipeline.pipeline_id,
            )

        return None

    # Phase A: schedule high priority first, trying interactive pool first but allowing spillover.
    for pr in (Priority.QUERY, Priority.INTERACTIVE):
        for pool_id in _pool_order_for_priority(s, pr):
            # Keep packing until we can't place more for this priority on this pool
            while True:
                cpu_left, ram_left = local_avail[pool_id]
                if cpu_left < 1.0 or ram_left < 1.0:
                    break
                a = schedule_one_from_queue(pr, pool_id, reserve_cpu=0.0, reserve_ram=0.0)
                if a is None:
                    break
                assignments.append(a)

    # Phase B: schedule batch, reserving headroom on interactive pool when HP backlog exists.
    for pool_id in _pool_order_for_priority(s, Priority.BATCH_PIPELINE):
        pool = s.executor.pools[pool_id]
        reserve_cpu = 0.0
        reserve_ram = 0.0
        if (s.executor.num_pools > 1) and (pool_id == s.interactive_pool_id) and hp_backlog:
            reserve_cpu = max(1.0, pool.max_cpu_pool * float(s.interactive_reserve_frac["cpu"]))
            reserve_ram = max(1.0, pool.max_ram_pool * float(s.interactive_reserve_frac["ram"]))

        while True:
            cpu_left, ram_left = local_avail[pool_id]
            # Must leave reserved headroom if applicable
            if (cpu_left - reserve_cpu) < 1.0 or (ram_left - reserve_ram) < 1.0:
                break
            a = schedule_one_from_queue(Priority.BATCH_PIPELINE, pool_id, reserve_cpu=reserve_cpu, reserve_ram=reserve_ram)
            if a is None:
                break
            assignments.append(a)

    return suspensions, assignments