# source: scheduler_low_011.py
# model: gpt-5.2-2025-12-11
# effort: low
# feedback: rich
# iteration: r3
@register_scheduler_init(key="scheduler_low_011_r3")
def scheduler_low_011_r3_init(s):
    """Priority-aware, throughput-friendly scheduler focused on reducing queueing latency.

    Changes vs naive FIFO / prior iteration:
    - Per-priority FIFO queues with de-dup (each pipeline appears at most once in a queue).
    - Weighted round-robin (WRR) across priorities to prevent starvation (esp. INTERACTIVE),
      while still heavily favoring QUERY/INTERACTIVE for latency.
    - Fill each pool with multiple assignments per tick (not just one), using local accounting
      of remaining CPU/RAM to improve utilization and reduce waiting time.
    - Soft pool partitioning when multiple pools exist:
        * pool 0 is high-priority preferred
        * other pools are batch-preferred, but allow high-priority spillover under pressure/aging
    - OOM-aware RAM backoff retry (best-effort, only when pipeline_id is available in results).
    """
    s.tick = 0

    # FIFO queues by priority
    s.waiting_queues = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }
    # De-dup sets so we don't enqueue the same pipeline multiple times
    s.in_queue = {
        Priority.QUERY: set(),
        Priority.INTERACTIVE: set(),
        Priority.BATCH_PIPELINE: set(),
    }

    # Track when a pipeline first arrived (for aging / spillover decisions)
    s.first_seen_tick = {}  # pipeline_id -> tick

    # Pipelines we will never schedule again (too many retries / fatal error)
    s.dead_pipelines = set()

    # Operator resource hints & retry bookkeeping (for OOM backoff)
    s.op_hints = {}        # (pipeline_id, op_id) -> {"ram": float, "cpu": float}
    s.op_attempts = {}     # (pipeline_id, op_id) -> int
    s.op_fatal = set()     # (pipeline_id, op_id) marked non-retryable

    # Retry policy
    s.max_retries_per_op = 4
    s.oom_backoff_mult = 1.6

    # WRR sequence: bias toward QUERY while ensuring INTERACTIVE makes progress.
    # (Batch gets a small share unless aged / idle.)
    s.wrr_seq = (
        [Priority.QUERY] * 6 +
        [Priority.INTERACTIVE] * 4 +
        [Priority.BATCH_PIPELINE] * 1
    )
    s.wrr_idx = 0

    # Scheduling limits / knobs (bounded scanning to keep scheduler fast)
    s.max_scan_per_dequeue = 24
    s.max_assignments_per_pool = 64

    # Aging / spillover knobs (in ticks)
    s.batch_aging_threshold = 250      # allow batch into HP pool if it waits too long
    s.hp_spillover_age = 60            # allow HP onto batch pools if it waits too long
    s.hp_spillover_backlog = 40        # allow HP spillover when HP backlog is large

    # Minimum allocations (avoid zero)
    s.min_cpu = 1.0
    s.min_ram = 1.0

    # If multiple pools exist, pool 0 is treated as HP-preferred
    s.hp_pool_id = 0


def _is_oom_error(err):
    if err is None:
        return False
    msg = str(err).lower()
    return ("oom" in msg) or ("out of memory" in msg) or ("memoryerror" in msg)


def _op_key(pipeline_id, op):
    return (pipeline_id, id(op))


def _enqueue_pipeline(s, pipeline, front=False):
    """Enqueue pipeline into its priority queue if not already present."""
    if pipeline is None:
        return
    pid = pipeline.pipeline_id
    pr = pipeline.priority
    if pr not in s.waiting_queues:
        pr = Priority.BATCH_PIPELINE

    if pid in s.dead_pipelines:
        return

    if pid in s.in_queue[pr]:
        return

    if front:
        s.waiting_queues[pr].insert(0, pipeline)
    else:
        s.waiting_queues[pr].append(pipeline)
    s.in_queue[pr].add(pid)


def _oldest_age(s, pr):
    """Age of the oldest pipeline currently enqueued in priority pr."""
    q = s.waiting_queues.get(pr, [])
    if not q:
        return 0
    p = q[0]
    first = s.first_seen_tick.get(p.pipeline_id, s.tick)
    return max(0, s.tick - first)


def _wrr_pick_priority(s, allowed_priorities):
    """Pick next priority to try using a global WRR pointer (returns None if none allowed)."""
    if not allowed_priorities:
        return None
    n = len(s.wrr_seq)
    for _ in range(n):
        pr = s.wrr_seq[s.wrr_idx]
        s.wrr_idx = (s.wrr_idx + 1) % n
        if pr in allowed_priorities:
            return pr
    return None


def _dequeue_ready_pipeline(s, pr, scheduled_this_tick):
    """Pop up to max_scan items from pr queue; return (pipeline, op) when an assignable op is ready.

    - Skips completed pipelines.
    - Skips pipelines already scheduled in this scheduler call.
    - Keeps pipelines without ready ops by rotating them to the back (preserves FIFO-ish fairness).
    """
    q = s.waiting_queues[pr]
    scan = min(len(q), s.max_scan_per_dequeue)

    for _ in range(scan):
        p = q.pop(0)
        pid = p.pipeline_id
        s.in_queue[pr].discard(pid)

        if pid in s.dead_pipelines:
            continue
        if pid in scheduled_this_tick:
            # Avoid duplicate scheduling of the same pipeline in one tick (assignments apply after return)
            _enqueue_pipeline(s, p, front=False)
            continue

        st = p.runtime_status()
        if st.is_pipeline_successful():
            continue

        # If pipeline has FAILED ops, they are still in ASSIGNABLE_STATES; we may retry (esp. for OOM).
        ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        if not ops:
            _enqueue_pipeline(s, p, front=False)
            continue

        # Prefer the first ready op (FIFO within pipeline)
        op = ops[0]

        # If we've marked this op as fatal, stop scheduling this pipeline (can't complete).
        ok = _op_key(pid, op)
        if ok in s.op_fatal:
            s.dead_pipelines.add(pid)
            continue

        return p, op

    return None, None


def _compute_request(s, pool, pipeline_priority, hp_backlog, pipeline_id, op):
    """Compute CPU/RAM request with dynamic sizing under load + OOM hints."""
    # Dynamic target concurrency for high-priority work:
    # under heavier backlog, allocate smaller slices to reduce queueing latency.
    if hp_backlog > 200:
        hp_target_conc = 8
    elif hp_backlog > 100:
        hp_target_conc = 6
    elif hp_backlog > 50:
        hp_target_conc = 4
    else:
        hp_target_conc = 2

    if pipeline_priority in (Priority.QUERY, Priority.INTERACTIVE):
        target = hp_target_conc
        cpu = pool.max_cpu_pool / float(target)
        ram = pool.max_ram_pool / float(max(2, target))
        # Slightly favor INTERACTIVE CPU to help its tail latency when it does run
        if pipeline_priority == Priority.INTERACTIVE:
            cpu *= 1.10
    else:
        # Batch: keep it from consuming the entire pool during HP pressure.
        # If HP backlog exists, use smaller slices; otherwise allow bigger chunks.
        target = 3 if hp_backlog > 0 else 2
        cpu = pool.max_cpu_pool / float(target)
        ram = pool.max_ram_pool / float(target)

    # Floors (avoid zero/tiny requests)
    cpu = max(s.min_cpu, cpu)
    ram = max(s.min_ram, ram)

    # Apply learned hints (primarily RAM for OOM avoidance)
    k = _op_key(pipeline_id, op)
    hint = s.op_hints.get(k)
    if hint:
        cpu = max(cpu, float(hint.get("cpu", cpu)))
        ram = max(ram, float(hint.get("ram", ram)))

    # Cap to pool max (executor will also cap by availability per tick via our local accounting)
    cpu = min(cpu, pool.max_cpu_pool)
    ram = min(ram, pool.max_ram_pool)

    return cpu, ram


@register_scheduler(key="scheduler_low_011_r3")
def scheduler_low_011_r3(s, results, pipelines):
    """
    Priority-aware WRR scheduler that fills pools and avoids starvation.

    Key latency improvements:
    - Multiple assignments per pool per tick to reduce waiting time.
    - WRR between QUERY and INTERACTIVE (plus small BATCH share / aging) to prevent starvation.
    - Soft pool partitioning and spillover for multi-pool executors.
    - Best-effort OOM RAM backoff.
    """
    s.tick += 1

    # Ingest new pipelines
    for p in pipelines:
        pid = p.pipeline_id
        if pid not in s.first_seen_tick:
            s.first_seen_tick[pid] = s.tick
        _enqueue_pipeline(s, p, front=False)

    # Update OOM/fatal bookkeeping from results
    for r in results:
        try:
            failed = r.failed()
        except Exception:
            failed = getattr(r, "error", None) is not None

        if not failed:
            continue

        pid = getattr(r, "pipeline_id", None)
        ops = getattr(r, "ops", []) or []
        err = getattr(r, "error", None)

        # If we can't associate failures to a pipeline/op, we can't learn; continue.
        if pid is None or not ops:
            continue

        for op in ops:
            ok = _op_key(pid, op)

            if _is_oom_error(err):
                # Exponential-ish RAM backoff; keep CPU hint as the last seen allocation (or 1.0).
                prev_hint = s.op_hints.get(ok, {})
                prev_ram = float(prev_hint.get("ram", getattr(r, "ram", 0.0) or 0.0))
                prev_cpu = float(prev_hint.get("cpu", getattr(r, "cpu", 0.0) or 0.0))

                baseline_ram = prev_ram if prev_ram > 0 else float(getattr(r, "ram", 1.0) or 1.0)
                new_ram = max(s.min_ram, baseline_ram * float(s.oom_backoff_mult))

                baseline_cpu = prev_cpu if prev_cpu > 0 else float(getattr(r, "cpu", 1.0) or 1.0)
                new_cpu = max(s.min_cpu, baseline_cpu)

                s.op_hints[ok] = {"ram": new_ram, "cpu": new_cpu}
                s.op_attempts[ok] = int(s.op_attempts.get(ok, 0)) + 1

                if s.op_attempts[ok] > s.max_retries_per_op:
                    # Too many OOM retries; stop scheduling this pipeline (can't complete reliably).
                    s.dead_pipelines.add(pid)
            else:
                # Treat non-OOM failures as fatal (don't spin forever)
                s.op_fatal.add(ok)
                s.dead_pipelines.add(pid)

    # Early exit if nothing to do
    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    # Pre-compute backlog & aging signals used for pool-level policy
    q_query = len(s.waiting_queues[Priority.QUERY])
    q_inter = len(s.waiting_queues[Priority.INTERACTIVE])
    q_batch = len(s.waiting_queues[Priority.BATCH_PIPELINE])

    hp_backlog = q_query + q_inter
    oldest_hp = max(_oldest_age(s, Priority.QUERY), _oldest_age(s, Priority.INTERACTIVE))
    oldest_batch = _oldest_age(s, Priority.BATCH_PIPELINE)

    scheduled_this_tick = set()
    to_reenqueue = []  # pipelines scheduled in this tick, re-enqueued once at end

    num_pools = s.executor.num_pools

    for pool_id in range(num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)

        if avail_cpu < s.min_cpu or avail_ram < s.min_ram:
            continue

        # Decide which priorities are allowed/preferred in this pool
        if num_pools == 1:
            allowed = {Priority.QUERY, Priority.INTERACTIVE}
            # Let batch in only if HP is empty or batch is starving
            if hp_backlog == 0 or oldest_batch >= s.batch_aging_threshold:
                allowed.add(Priority.BATCH_PIPELINE)
        else:
            if pool_id == s.hp_pool_id:
                # HP-preferred pool
                allowed = {Priority.QUERY, Priority.INTERACTIVE}
                if hp_backlog == 0 or oldest_batch >= s.batch_aging_threshold:
                    allowed.add(Priority.BATCH_PIPELINE)
            else:
                # Batch-preferred pools with HP spillover under pressure/aging
                allowed = {Priority.BATCH_PIPELINE}

                hp_pressure = (hp_backlog >= s.hp_spillover_backlog) or (oldest_hp >= s.hp_spillover_age)
                if hp_pressure or q_batch == 0:
                    allowed.update({Priority.QUERY, Priority.INTERACTIVE})

        # Fill the pool with multiple assignments
        made = 0
        # To avoid infinite loops when allowed priorities have only blocked pipelines, bound attempts
        attempts = 0
        max_attempts = s.max_assignments_per_pool * 4

        while (made < s.max_assignments_per_pool and
               avail_cpu >= s.min_cpu and
               avail_ram >= s.min_ram and
               attempts < max_attempts):
            attempts += 1

            # If queues are empty for all allowed priorities, stop early
            any_nonempty = False
            for pr in allowed:
                if s.waiting_queues[pr]:
                    any_nonempty = True
                    break
            if not any_nonempty:
                break

            pr_try = _wrr_pick_priority(s, allowed)
            if pr_try is None:
                break

            p, op = _dequeue_ready_pipeline(s, pr_try, scheduled_this_tick)
            if p is None:
                # Nothing ready found in this priority after scanning; try again (WRR will move on)
                continue

            pid = p.pipeline_id
            if pid in s.dead_pipelines:
                continue

            req_cpu, req_ram = _compute_request(
                s=s,
                pool=pool,
                pipeline_priority=p.priority,
                hp_backlog=hp_backlog,
                pipeline_id=pid,
                op=op,
            )

            # Fit to what remains in this pool for this tick (prefer to keep RAM >= hint; but shrink if needed)
            if req_cpu > avail_cpu:
                req_cpu = max(s.min_cpu, avail_cpu)
            if req_ram > avail_ram:
                req_ram = max(s.min_ram, avail_ram)

            # If still can't fit, put it back and stop trying to pack more (pool is effectively full)
            if req_cpu > avail_cpu or req_ram > avail_ram:
                _enqueue_pipeline(s, p, front=True)
                break

            assignments.append(
                Assignment(
                    ops=[op],
                    cpu=req_cpu,
                    ram=req_ram,
                    priority=p.priority,
                    pool_id=pool_id,
                    pipeline_id=pid,
                )
            )

            scheduled_this_tick.add(pid)
            to_reenqueue.append(p)

            avail_cpu -= float(req_cpu)
            avail_ram -= float(req_ram)
            made += 1

    # Re-enqueue pipelines we scheduled (once) so their next operators can be scheduled in future ticks
    for p in to_reenqueue:
        # Don't re-enqueue pipelines that became dead due to excessive retries/fatal errors during this tick
        if p.pipeline_id in s.dead_pipelines:
            continue
        _enqueue_pipeline(s, p, front=False)

    return suspensions, assignments