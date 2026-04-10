# policy_key: scheduler_est_009
# reasoning_effort: low
# model: gpt-5.2-2025-12-11
# llm_cost: 0.043110
# generation_seconds: 53.61
# generated_at: 2026-04-01T13:42:07.872386
@register_scheduler_init(key="scheduler_est_009")
def scheduler_est_009_init(s):
    """Priority-aware, conservative-reserve scheduler (incremental improvement over naive FIFO).

    Main ideas (kept deliberately simple and robust):
    - Maintain separate FIFO queues per priority to protect interactive/query latency.
    - Keep a small CPU/RAM reserve in each pool when high-priority work is waiting, so batch can't fully consume pools.
    - On OOM-like failures, retry the failed operator with increased RAM using a per-operator backoff hint.
    - Light starvation protection via aging: if a batch pipeline waits "too long", it is temporarily treated as higher priority.
    """
    # Per-priority FIFO queues of pipelines
    s.wait_q = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }

    # Bookkeeping / heuristics
    s.tick = 0
    s.pipeline_enqueued_tick = {}  # pipeline_id -> tick last enqueued (for aging)
    s.op_ram_hint_gb = {}          # id(op) -> next suggested RAM to avoid repeated OOMs
    s.op_fail_counts = {}          # id(op) -> number of failures seen
    s.op_nonretriable = set()      # id(op) set if we deem failure non-retriable

    # Tuning knobs (conservative defaults)
    s.min_ram_gb = 1.0
    s.min_cpu = 1.0

    # Defaults when no estimate is available (chosen as small fractions of pool at assignment time)
    s.default_hi_ram_frac = 0.50   # for QUERY/INTERACTIVE when no estimate/hint
    s.default_lo_ram_frac = 0.25   # for BATCH when no estimate/hint

    # Reserve fractions used when high-priority backlog exists
    s.reserve_cpu_frac_when_hi_waiting = 0.25
    s.reserve_ram_frac_when_hi_waiting = 0.25

    # Aging: after this many ticks waiting, batch is treated as interactive for ordering
    s.batch_aging_threshold_ticks = 50

    # Retry limits
    s.max_retries_retriable = 3
    s.max_retries_nonretriable = 1


def _is_retriable_error(err) -> bool:
    if err is None:
        return False
    msg = str(err).lower()
    return ("oom" in msg) or ("out of memory" in msg) or ("memoryerror" in msg)


def _get_op_mem_estimate_gb(op):
    # Per context: an estimator may attach op.estimate.mem_peak_gb (float or None)
    est_obj = getattr(op, "estimate", None)
    if est_obj is None:
        return None
    val = getattr(est_obj, "mem_peak_gb", None)
    try:
        if val is None:
            return None
        v = float(val)
        if v <= 0:
            return None
        return v
    except Exception:
        return None


def _effective_priority(s, pipeline) -> int:
    # Lower number => higher scheduling priority
    p = pipeline.priority
    # Aging: batch pipelines that have waited long get temporarily boosted above batch
    if p == Priority.BATCH_PIPELINE:
        t0 = s.pipeline_enqueued_tick.get(pipeline.pipeline_id, s.tick)
        if (s.tick - t0) >= s.batch_aging_threshold_ticks:
            # Boost to INTERACTIVE "lane" for ordering
            return 1
        return 2
    if p == Priority.INTERACTIVE:
        return 1
    return 0  # QUERY


def _priority_order_key(s, pipeline):
    # (effective_priority, enqueue_tick) for FIFO within class
    eff = _effective_priority(s, pipeline)
    t0 = s.pipeline_enqueued_tick.get(pipeline.pipeline_id, s.tick)
    return (eff, t0)


def _enqueue_pipeline(s, p):
    # Track enqueue time for aging/FIFO; only set if not present to preserve original order
    if p.pipeline_id not in s.pipeline_enqueued_tick:
        s.pipeline_enqueued_tick[p.pipeline_id] = s.tick
    # Place into its native priority queue
    if p.priority not in s.wait_q:
        s.wait_q[p.priority] = []
    s.wait_q[p.priority].append(p)


def _has_high_priority_backlog(s) -> bool:
    # "High priority" = QUERY or INTERACTIVE (native queues only; aging handled at selection time)
    return (len(s.wait_q.get(Priority.QUERY, [])) > 0) or (len(s.wait_q.get(Priority.INTERACTIVE, [])) > 0)


def _select_next_pipeline(s):
    # Choose next pipeline globally by (effective priority, FIFO enqueue time).
    candidates = []
    for pr, q in s.wait_q.items():
        if q:
            candidates.append(q[0])

    if not candidates:
        return None

    # Pick best candidate; then pop it from its native queue
    best = min(candidates, key=lambda p: _priority_order_key(s, p))
    s.wait_q[best.priority].pop(0)
    return best


def _compute_assignment_resources(s, pool, pipeline, op, avail_cpu, avail_ram, hi_backlog: bool):
    # CPU: give more to higher priority; limit batch to avoid hogging and to preserve headroom.
    if pipeline.priority in (Priority.QUERY, Priority.INTERACTIVE):
        cpu = max(s.min_cpu, min(avail_cpu, pool.max_cpu_pool))
    else:
        # Batch: take at most half of the currently usable CPU (but at least 1)
        cpu = max(s.min_cpu, min(avail_cpu, max(s.min_cpu, avail_cpu * 0.5)))

    # RAM: use max(estimate, OOM-backoff-hint, defaults). Be conservative for high priority.
    est_gb = _get_op_mem_estimate_gb(op)
    hint_gb = s.op_ram_hint_gb.get(id(op), None)

    # Base RAM if no signals: choose fraction of pool max (not avail) and then cap by avail at the end.
    if pipeline.priority in (Priority.QUERY, Priority.INTERACTIVE):
        default_gb = max(s.min_ram_gb, pool.max_ram_pool * s.default_hi_ram_frac)
    else:
        default_gb = max(s.min_ram_gb, pool.max_ram_pool * s.default_lo_ram_frac)

    # Use the largest signal as a safety lower-bound.
    ram_floor = default_gb
    if est_gb is not None:
        # Pad estimate slightly to reduce OOM odds.
        ram_floor = max(ram_floor, est_gb * 1.20)
    if hint_gb is not None:
        ram_floor = max(ram_floor, hint_gb)

    # If high-priority backlog exists, we prefer to keep some headroom by not consuming all RAM with batch.
    # (For high-priority work itself, we still allow using what's available.)
    if hi_backlog and pipeline.priority == Priority.BATCH_PIPELINE:
        # We'll be called with avail_ram already reduced by reserve; keep it within that.
        ram = min(avail_ram, ram_floor)
    else:
        ram = min(avail_ram, ram_floor)

    # Enforce minimums and feasible bounds
    cpu = max(s.min_cpu, min(cpu, avail_cpu))
    ram = max(s.min_ram_gb, min(ram, avail_ram))
    return cpu, ram


@register_scheduler(key="scheduler_est_009")
def scheduler_est_009(s, results, pipelines):
    """
    Priority-aware scheduler with conservative headroom + OOM RAM backoff.

    Returns:
        (suspensions, assignments)
    """
    s.tick += 1

    # Ingest new pipelines
    for p in pipelines:
        _enqueue_pipeline(s, p)

    # Process results: learn from failures (especially OOM) by increasing RAM hint for retry.
    for r in results:
        if not getattr(r, "failed", lambda: False)():
            continue

        retriable = _is_retriable_error(getattr(r, "error", None))
        for op in getattr(r, "ops", []) or []:
            oid = id(op)
            s.op_fail_counts[oid] = s.op_fail_counts.get(oid, 0) + 1

            if not retriable:
                # Mark as non-retriable after a small number of failures
                if s.op_fail_counts[oid] >= s.max_retries_nonretriable:
                    s.op_nonretriable.add(oid)
                continue

            # Retriable: bump RAM hint for next time.
            # Use current assigned RAM as baseline, then double; cap happens at assignment.
            prev_hint = s.op_ram_hint_gb.get(oid, None)
            base = getattr(r, "ram", None)
            try:
                base = float(base) if base is not None else None
            except Exception:
                base = None

            # If result.ram is missing, fall back to prior hint or a minimum.
            if base is None:
                base = prev_hint if prev_hint is not None else s.min_ram_gb

            next_hint = max(s.min_ram_gb, base * 2.0)
            if prev_hint is not None:
                next_hint = max(next_hint, prev_hint * 1.5)
            s.op_ram_hint_gb[oid] = next_hint

    # Early exit if nothing changed
    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    # Try to place work on each pool; allow multiple assignments per pool while resources remain.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]

        # Snapshot pool availability at this tick; we update local variables as we assign.
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool
        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        hi_backlog = _has_high_priority_backlog(s)

        # If high-priority is waiting, reserve some headroom so batch cannot fully consume the pool.
        if hi_backlog:
            reserve_cpu = pool.max_cpu_pool * s.reserve_cpu_frac_when_hi_waiting
            reserve_ram = pool.max_ram_pool * s.reserve_ram_frac_when_hi_waiting
        else:
            reserve_cpu = 0.0
            reserve_ram = 0.0

        # Schedule until we run out of usable resources or no runnable ops exist.
        # We keep a bounded number of iterations to avoid pathological loops.
        max_iters = 64
        iters = 0
        while iters < max_iters:
            iters += 1

            # Determine usable resources for choosing the next assignment.
            usable_cpu = max(0.0, avail_cpu - reserve_cpu)
            usable_ram = max(0.0, avail_ram - reserve_ram)

            # If no high-priority backlog exists, let batch use everything.
            if not hi_backlog:
                usable_cpu = avail_cpu
                usable_ram = avail_ram

            if usable_cpu < s.min_cpu or usable_ram < s.min_ram_gb:
                break

            pipeline = _select_next_pipeline(s)
            if pipeline is None:
                break

            status = pipeline.runtime_status()

            # Drop completed pipelines
            if status.is_pipeline_successful():
                continue

            # Find next runnable op (parents complete)
            op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
            if not op_list:
                # Not runnable now (waiting on parents or already running); requeue and move on.
                _enqueue_pipeline(s, pipeline)
                continue

            op = op_list[0]

            # If we marked this op as non-retriable, we avoid infinite thrash by not retrying it forever.
            oid = id(op)
            if oid in s.op_nonretriable:
                # Requeue pipeline (maybe other branches can run); but if this is the only frontier op,
                # it will keep coming back—bounded by the simulator's own failure handling.
                _enqueue_pipeline(s, pipeline)
                continue

            # If retriable and too many retries, stop retrying.
            fails = s.op_fail_counts.get(oid, 0)
            if fails > s.max_retries_retriable and _is_retriable_error(getattr(op, "error", None)):
                s.op_nonretriable.add(oid)
                _enqueue_pipeline(s, pipeline)
                continue

            # Compute resources (using usable_* which respects reserves when hi backlog exists).
            cpu, ram = _compute_assignment_resources(
                s=s,
                pool=pool,
                pipeline=pipeline,
                op=op,
                avail_cpu=usable_cpu,
                avail_ram=usable_ram,
                hi_backlog=hi_backlog,
            )

            # If we still can't meet minimums, requeue and stop scheduling in this pool.
            if cpu < s.min_cpu or ram < s.min_ram_gb:
                _enqueue_pipeline(s, pipeline)
                break

            assignments.append(
                Assignment(
                    ops=op_list,
                    cpu=cpu,
                    ram=ram,
                    priority=pipeline.priority,
                    pool_id=pool_id,
                    pipeline_id=pipeline.pipeline_id,
                )
            )

            # Update remaining availability
            avail_cpu -= cpu
            avail_ram -= ram

            # Requeue pipeline to allow downstream ops to be scheduled later
            _enqueue_pipeline(s, pipeline)

            if avail_cpu <= 0 or avail_ram <= 0:
                break

    return suspensions, assignments
