# source: scheduler_low_011.py
# model: gpt-5.2-2025-12-11
# effort: low
# feedback: rich
# iteration: r4
@register_scheduler_init(key="scheduler_low_011_r4")
def scheduler_low_011_r4_init(s):
    """Priority-aware, work-conserving scheduler focused on lowering latency without starving other classes.

    Small, obvious fixes over the prior naive/FIFO-style approach:
    - Priority queues + weighted round-robin (WRR) budgets so QUERY stays fast but INTERACTIVE/BATCH make progress.
    - Work-conserving multi-assign per pool per tick (pack until pool capacity is used).
    - Remove hard pool-affinity that can leave pools idle; treat "interactive pool" as a soft preference only.
    - OOM-aware retry for FAILED ops (only retriable OOM failures), using per-op RAM backoff hints.

    Key implementation details:
    - De-duplicate pipelines in queues with per-priority membership sets.
    - Avoid duplicate assignment of the same pipeline within a tick (since status won't reflect ASSIGNED yet).
    - Pick candidates with a small lookahead window and a simple "finish-sooner" score (more completed ops, older age).
    """
    s.tick = 0

    # FIFO-ish queues per priority, with membership sets to avoid duplicates.
    s.waiting_queues = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }
    s.queue_sets = {
        Priority.QUERY: set(),
        Priority.INTERACTIVE: set(),
        Priority.BATCH_PIPELINE: set(),
    }

    # Track pipeline references and arrival ticks for lightweight aging.
    s.pipeline_index = {}      # pipeline_id -> Pipeline
    s.arrival_tick = {}        # pipeline_id -> tick when first seen

    # OOM retry state keyed by operator object identity (id(op)).
    s.op_hints_ram = {}        # op_id -> suggested RAM
    s.op_hints_cpu = {}        # op_id -> suggested CPU (kept minimal; mostly RAM matters for OOM)
    s.op_attempts = {}         # op_id -> retry attempts (OOM only)
    s.op_last_req = {}         # op_id -> (cpu, ram, pool_id) last requested
    s.op_to_pipeline = {}      # op_id -> pipeline_id (learned at assignment time)
    s.retriable_ops = set()    # op_ids that failed with OOM and are eligible for retry

    # Conservative retry policy.
    s.max_retries_per_op = 3

    # Scheduler knobs.
    s.max_scan_per_pick = 16  # lookahead to find ready work without scanning entire queue

    # Weighted RR quanta (higher means more picks per cycle when all have backlog).
    s.wrr_quanta = {
        Priority.QUERY: 6,
        Priority.INTERACTIVE: 3,
        Priority.BATCH_PIPELINE: 1,
    }

    # Soft preference: if multiple pools, pool 0 is "interactive-ish", but we do NOT block placement elsewhere.
    s.interactive_pool_id = 0


def _oom_like(err):
    if err is None:
        return False
    msg = str(err).lower()
    return ("oom" in msg) or ("out of memory" in msg) or ("memoryerror" in msg)


def _queue_add(s, pipeline):
    pr = pipeline.priority
    if pr not in s.waiting_queues:
        pr = Priority.BATCH_PIPELINE
    pid = pipeline.pipeline_id
    if pid in s.queue_sets[pr]:
        return
    s.waiting_queues[pr].append(pipeline)
    s.queue_sets[pr].add(pid)


def _queue_pop_index(s, pr, idx):
    p = s.waiting_queues[pr].pop(idx)
    pid = p.pipeline_id
    s.queue_sets[pr].discard(pid)
    return p


def _pipeline_age(s, pipeline_id):
    at = s.arrival_tick.get(pipeline_id, s.tick)
    return max(0, s.tick - at)


def _pick_ready_op(s, pipeline, allow_failed_retries=True):
    """Pick a single ready operator from a pipeline.

    Strategy:
    - Prefer PENDING ready ops.
    - If none, and allow_failed_retries=True, consider FAILED ready ops only if we know it's an OOM-retriable op.
    """
    status = pipeline.runtime_status()

    # First: normal PENDING work.
    pending = status.get_ops([OperatorState.PENDING], require_parents_complete=True)
    if pending:
        return pending[0]

    if not allow_failed_retries:
        return None

    # Second: FAILED work, but only if it's explicitly retriable (OOM backoff).
    failed = status.get_ops([OperatorState.FAILED], require_parents_complete=True)
    for op in failed or []:
        if id(op) in s.retriable_ops and s.op_attempts.get(id(op), 0) <= s.max_retries_per_op:
            return op
    return None


def _pipeline_is_terminal_failure(s, pipeline):
    """Drop pipelines that are effectively unrecoverable to avoid clogging the queues.

    If there are FAILED ops but none are OOM-retriable (or retry budget exceeded), treat as terminal failure.
    """
    status = pipeline.runtime_status()
    failed_cnt = status.state_counts.get(OperatorState.FAILED, 0)
    if failed_cnt <= 0:
        return False

    # If there exists a ready FAILED op we can retry, it's not terminal.
    op = _pick_ready_op(s, pipeline, allow_failed_retries=True)
    if op is not None and id(op) in s.retriable_ops and s.op_attempts.get(id(op), 0) <= s.max_retries_per_op:
        return False

    # Otherwise, consider it terminal (we don't have a safe recovery path).
    return True


def _score_pipeline(s, pipeline):
    """Higher score = schedule sooner (simple heuristic to reduce end-to-end latency)."""
    status = pipeline.runtime_status()
    completed = status.state_counts.get(OperatorState.COMPLETED, 0)
    age = _pipeline_age(s, pipeline.pipeline_id)

    # Bias towards finishing pipelines that are already partway done; age provides basic fairness.
    return completed * 10 + age


def _compute_request(s, pool, pr, op, backlog_len, num_pools, avail_cpu, avail_ram):
    """Compute CPU/RAM request for an op, with simple load-adaptive CPU sizing and OOM-hint RAM sizing."""
    # Base sizing: prefer scale-up when backlog is small (finish quickly), scale-out when backlog is large (reduce queueing).
    if pr == Priority.QUERY:
        cpu_frac = 1.0 if backlog_len <= max(1, num_pools) else (0.65 if backlog_len <= 4 * max(1, num_pools) else 0.45)
        ram_frac = 0.15
    elif pr == Priority.INTERACTIVE:
        cpu_frac = 0.85 if backlog_len <= max(1, num_pools) else (0.60 if backlog_len <= 4 * max(1, num_pools) else 0.45)
        ram_frac = 0.18
    else:
        cpu_frac = 0.50 if backlog_len <= 4 * max(1, num_pools) else 0.35
        ram_frac = 0.22

    # Desired requests based on pool max (conservative RAM to improve packing; OOM backoff corrects when wrong).
    desired_cpu = max(1.0, float(pool.max_cpu_pool) * cpu_frac)
    desired_ram = max(1.0, float(pool.max_ram_pool) * ram_frac)

    # Apply OOM hints if present for this operator.
    op_id = id(op)
    if op_id in s.op_hints_ram:
        desired_ram = max(desired_ram, float(s.op_hints_ram[op_id]))
    if op_id in s.op_hints_cpu:
        desired_cpu = max(desired_cpu, float(s.op_hints_cpu[op_id]))

    # Cap by currently available in this pool (local view for packing loop).
    cpu = min(desired_cpu, float(avail_cpu))
    ram = min(desired_ram, float(avail_ram))

    # Ensure strictly positive.
    cpu = max(1.0, cpu)
    ram = max(1.0, ram)

    return cpu, ram


def _pick_candidate_from_queue(s, pr, assigned_pipeline_ids):
    """Pick a pipeline+op from the first K entries, based on readiness and a small scoring heuristic."""
    q = s.waiting_queues[pr]
    if not q:
        return None, None

    best_idx = None
    best_score = None
    best_op = None

    scan = min(len(q), s.max_scan_per_pick)
    for i in range(scan):
        p = q[i]
        pid = p.pipeline_id
        if pid in assigned_pipeline_ids:
            continue

        status = p.runtime_status()
        if status.is_pipeline_successful():
            continue
        if _pipeline_is_terminal_failure(s, p):
            continue

        op = _pick_ready_op(s, p, allow_failed_retries=True)
        if op is None:
            continue

        sc = _score_pipeline(s, p)
        if best_idx is None or sc > best_score:
            best_idx = i
            best_score = sc
            best_op = op

    if best_idx is None:
        return None, None

    p = _queue_pop_index(s, pr, best_idx)
    return p, best_op


@register_scheduler(key="scheduler_low_011_r4")
def scheduler_low_011_r4(s, results, pipelines):
    """
    Main scheduling loop:
    - Ingest new pipelines into per-priority de-duped queues.
    - Update OOM retry hints from results.
    - For each pool, pack as many assignments as possible this tick using WRR across priorities.
    """
    s.tick += 1

    # Ingest new pipelines.
    for p in pipelines:
        s.pipeline_index[p.pipeline_id] = p
        if p.pipeline_id not in s.arrival_tick:
            s.arrival_tick[p.pipeline_id] = s.tick
        _queue_add(s, p)

    # Update retry hints from results (OOM backoff only).
    for r in results:
        ops = getattr(r, "ops", None) or []
        failed = False
        try:
            failed = bool(r.failed())
        except Exception:
            failed = getattr(r, "error", None) is not None

        if not failed:
            # On success, nothing to do; keep any existing hints (they're only RAM-floor hints).
            continue

        is_oom = _oom_like(getattr(r, "error", None))
        if not is_oom:
            # Non-OOM failures: prevent futile retries.
            for op in ops:
                s.retriable_ops.discard(id(op))
            continue

        # OOM failure: increase RAM hint for each op (usually only one).
        pool_id = getattr(r, "pool_id", None)
        pool = None
        if isinstance(pool_id, int) and 0 <= pool_id < s.executor.num_pools:
            pool = s.executor.pools[pool_id]

        for op in ops:
            op_id = id(op)
            last = s.op_last_req.get(op_id)
            last_ram = float(getattr(r, "ram", 0.0) or 0.0)
            last_cpu = float(getattr(r, "cpu", 1.0) or 1.0)
            if last is not None:
                last_cpu = float(last[0])
                last_ram = float(last[1])

            # Exponential RAM backoff; clamp to pool max if known.
            new_ram = max(1.0, last_ram * 2.0 if last_ram > 0 else 2.0)
            if pool is not None:
                new_ram = min(new_ram, float(pool.max_ram_pool))

            # Record hints and attempts.
            s.op_hints_ram[op_id] = max(float(s.op_hints_ram.get(op_id, 0.0) or 0.0), new_ram)
            s.op_hints_cpu[op_id] = max(float(s.op_hints_cpu.get(op_id, 0.0) or 0.0), max(1.0, last_cpu))
            s.op_attempts[op_id] = int(s.op_attempts.get(op_id, 0)) + 1

            if s.op_attempts[op_id] <= s.max_retries_per_op:
                s.retriable_ops.add(op_id)
            else:
                # Retry budget exceeded; stop retrying this op.
                s.retriable_ops.discard(op_id)

    # Early exit.
    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    num_pools = s.executor.num_pools

    # Schedule per pool; pack multiple assignments per pool per tick.
    for pool_id in range(num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)
        if avail_cpu < 1.0 or avail_ram < 1.0:
            continue

        # Avoid assigning the same pipeline multiple times in one tick (status won't update until executor runs).
        assigned_pipeline_ids = set()

        # Weighted RR budgets reset per pool per tick; work-conserving when some classes are empty.
        budgets = {pr: int(s.wrr_quanta.get(pr, 1)) for pr in s.waiting_queues.keys()}

        # Loop until resources exhausted or no work fits.
        while avail_cpu >= 1.0 and avail_ram >= 1.0:
            chosen_pr = None
            chosen_p = None
            chosen_op = None

            # 1) Try to pick respecting WRR budgets.
            for pr in (Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE):
                if budgets.get(pr, 0) <= 0:
                    continue
                p, op = _pick_candidate_from_queue(s, pr, assigned_pipeline_ids)
                if p is None:
                    continue
                chosen_pr, chosen_p, chosen_op = pr, p, op
                budgets[pr] -= 1
                break

            # 2) Work-conserving fallback: ignore budgets.
            if chosen_p is None:
                for pr in (Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE):
                    p, op = _pick_candidate_from_queue(s, pr, assigned_pipeline_ids)
                    if p is None:
                        continue
                    chosen_pr, chosen_p, chosen_op = pr, p, op
                    break

            if chosen_p is None:
                break

            # Compute request.
            backlog_len = len(s.waiting_queues.get(chosen_pr, []))
            cpu, ram = _compute_request(
                s=s,
                pool=pool,
                pr=chosen_pr,
                op=chosen_op,
                backlog_len=backlog_len,
                num_pools=num_pools,
                avail_cpu=avail_cpu,
                avail_ram=avail_ram,
            )

            # If we have an OOM RAM hint that exceeds current avail, don't schedule it now; requeue and try others.
            op_id = id(chosen_op)
            hint_ram = float(s.op_hints_ram.get(op_id, 0.0) or 0.0)
            if hint_ram > 0.0 and hint_ram > avail_ram:
                _queue_add(s, chosen_p)
                continue

            # Final fit: shrink CPU to fit; shrink RAM only if it's not a hinted minimum.
            if cpu > avail_cpu:
                cpu = max(1.0, avail_cpu)
            if ram > avail_ram:
                if hint_ram > 0.0:
                    # Can't safely shrink below hint; requeue.
                    _queue_add(s, chosen_p)
                    continue
                ram = max(1.0, avail_ram)

            # If still doesn't fit, requeue and stop trying to pack further in this pool (likely fragmented).
            if cpu > avail_cpu or ram > avail_ram:
                _queue_add(s, chosen_p)
                break

            # Assign.
            assignments.append(
                Assignment(
                    ops=[chosen_op],
                    cpu=cpu,
                    ram=ram,
                    priority=chosen_pr,
                    pool_id=pool_id,
                    pipeline_id=chosen_p.pipeline_id,
                )
            )

            # Record mapping for future OOM hinting.
            s.op_to_pipeline[op_id] = chosen_p.pipeline_id
            s.op_last_req[op_id] = (cpu, ram, pool_id)

            # Block pipeline for the remainder of this tick; requeue so it can progress in future ticks.
            assigned_pipeline_ids.add(chosen_p.pipeline_id)
            _queue_add(s, chosen_p)

            # Update local remaining capacity.
            avail_cpu -= cpu
            avail_ram -= ram

    return suspensions, assignments