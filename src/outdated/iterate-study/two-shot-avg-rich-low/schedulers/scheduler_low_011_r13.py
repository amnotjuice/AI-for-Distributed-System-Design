# source: scheduler_low_011.py
# model: gpt-5.2-2025-12-11
# effort: low
# feedback: rich
# iteration: r13
@register_scheduler_init(key="scheduler_low_011_r13")
def scheduler_low_011_r13_init(s):
    """Priority-aware, higher-throughput FIFO with weighted fairness.

    Key fixes vs the prior iteration:
    - Avoid starving INTERACTIVE by using weighted round-robin across priorities (instead of strict priority).
    - Improve latency by reducing queueing: schedule multiple operators per pool per tick (fill available resources).
    - Keep a light pool preference: pool 0 prefers QUERY/INTERACTIVE when multiple pools exist, but spillover is allowed.
    - Add simple OOM-aware RAM hinting per operator object (id(op)) to reduce repeated OOM retries.

    Design goals:
    - Keep the code robust and incremental (no preemption, no complex runtime introspection).
    - Protect tail latency by keeping per-op slices reasonable while increasing concurrency under high backlog.
    """
    # Per-priority FIFO pipeline queues
    s.waiting_queues = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }

    # De-dup pipeline presence in queues: pipeline_id -> True if currently enqueued
    s.in_queue = {}

    # Tick counter for simple aging/spillover heuristics
    s.tick = 0
    s.first_seen_tick = {}  # pipeline_id -> tick

    # OOM hinting per operator identity within this simulation process
    # key: id(op) -> {"ram": float, "cpu": float}
    s.op_hints = {}
    s.op_attempts = {}  # id(op) -> int
    s.max_retries_per_op = 4

    # Scheduling limits to prevent pathological scanning
    s.max_assignments_per_pool_per_tick = 8
    s.max_scan_per_assignment = 32

    # Pool preference knobs
    s.interactive_pool_id = 0
    s.spillover_age_ticks = 5  # allow QUERY/INTERACTIVE to use non-interactive pools after waiting this long

    # Weighted fairness (higher => more slots)
    # (We apply different weights depending on whether this is the interactive pool or not.)
    s.weights_interactive_pool = {
        Priority.INTERACTIVE: 4,
        Priority.QUERY: 3,
        Priority.BATCH_PIPELINE: 1,
    }
    s.weights_other_pools = {
        Priority.BATCH_PIPELINE: 4,
        Priority.INTERACTIVE: 1,
        Priority.QUERY: 1,
    }

    # Deficit counters per pool for weighted round-robin (Deficit Round Robin style)
    s.deficit = {}  # pool_id -> {priority -> deficit}
    for pool_id in range(getattr(s.executor, "num_pools", 1)):
        s.deficit[pool_id] = {
            Priority.QUERY: 0,
            Priority.INTERACTIVE: 0,
            Priority.BATCH_PIPELINE: 0,
        }


def _norm_priority(pr):
    if pr in (Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE):
        return pr
    return Priority.BATCH_PIPELINE


def _is_oom_error(err):
    if err is None:
        return False
    msg = str(err).lower()
    return ("oom" in msg) or ("out of memory" in msg) or ("memoryerror" in msg) or ("killed process" in msg)


def _has_high_priority_backlog(s):
    return (len(s.waiting_queues[Priority.INTERACTIVE]) > 0) or (len(s.waiting_queues[Priority.QUERY]) > 0)


def _pipeline_is_done(pipeline):
    try:
        return pipeline.runtime_status().is_pipeline_successful()
    except Exception:
        return False


def _pipeline_has_failed_ops(pipeline):
    # Do not automatically drop; we use this to stop retrying after too many attempts.
    try:
        status = pipeline.runtime_status()
        return status.state_counts.get(OperatorState.FAILED, 0) > 0
    except Exception:
        try:
            status = pipeline.runtime_status()
            return status.state_counts[OperatorState.FAILED] > 0
        except Exception:
            return False


def _next_ready_op(pipeline):
    status = pipeline.runtime_status()
    ops = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
    if not ops:
        return None
    return ops[0]


def _queue_len(s, pr):
    return len(s.waiting_queues.get(pr, []))


def _base_slice_fracs(priority, backlog_len, is_interactive_pool):
    # Small, conservative step: use bigger slices when backlog is low (faster single-op runtime),
    # smaller slices when backlog is high (more concurrency, lower queueing latency).
    if priority == Priority.BATCH_PIPELINE:
        # Batch: generally OK to be fatter; it benefits from scale-up.
        return 1.0, 1.0

    # QUERY / INTERACTIVE
    if backlog_len >= 100:
        cpu_frac = 0.25
        ram_frac = 0.25
    elif backlog_len >= 30:
        cpu_frac = 0.33
        ram_frac = 0.33
    else:
        cpu_frac = 0.5
        ram_frac = 0.5

    # On the interactive pool, slightly favor latency with a bit more CPU when lightly loaded
    if is_interactive_pool and backlog_len < 30:
        cpu_frac = min(0.66, max(cpu_frac, 0.5))

    return cpu_frac, ram_frac


def _compute_request(s, pool, priority, op, is_interactive_pool):
    # Determine requested CPU/RAM as a slice of pool MAX (not AVAIL), then clamp to AVAIL.
    backlog_len = _queue_len(s, priority)
    cpu_frac, ram_frac = _base_slice_fracs(priority, backlog_len, is_interactive_pool)

    cpu_req = max(1.0, pool.max_cpu_pool * cpu_frac)
    ram_req = max(1.0, pool.max_ram_pool * ram_frac)

    # Apply OOM-driven hints (RAM-first); key by id(op) since pipeline_id is not guaranteed in ExecutionResult.
    k = id(op)
    hint = s.op_hints.get(k)
    if hint:
        try:
            cpu_req = max(cpu_req, float(hint.get("cpu", cpu_req)))
            ram_req = max(ram_req, float(hint.get("ram", ram_req)))
        except Exception:
            pass

    # Cap to pool MAX first, then we'll clamp to AVAIL at placement time.
    cpu_req = min(cpu_req, pool.max_cpu_pool)
    ram_req = min(ram_req, pool.max_ram_pool)

    return cpu_req, ram_req


def _try_dequeue_pipeline(s, pr):
    # FIFO dequeue while skipping completed pipelines.
    q = s.waiting_queues[pr]
    while q:
        p = q.pop(0)
        s.in_queue.pop(p.pipeline_id, None)
        if _pipeline_is_done(p):
            continue
        return p
    return None


def _requeue_pipeline(s, pr, pipeline):
    # Prevent duplicates
    if s.in_queue.get(pipeline.pipeline_id, False):
        return
    s.waiting_queues[pr].append(pipeline)
    s.in_queue[pipeline.pipeline_id] = True


def _eligible_priorities_for_pool(s, pool_id):
    # With multiple pools:
    # - pool 0: prefers QUERY/INTERACTIVE; only schedule BATCH when no high-priority backlog.
    # - other pools: prefer BATCH; allow spillover for QUERY/INTERACTIVE if they've waited long enough.
    if s.executor.num_pools <= 1:
        return [Priority.INTERACTIVE, Priority.QUERY, Priority.BATCH_PIPELINE]

    if pool_id == s.interactive_pool_id:
        if _has_high_priority_backlog(s):
            return [Priority.INTERACTIVE, Priority.QUERY]
        return [Priority.INTERACTIVE, Priority.QUERY, Priority.BATCH_PIPELINE]

    # Non-interactive pools
    eligible = [Priority.BATCH_PIPELINE]

    # Spillover: if oldest QUERY/INTERACTIVE has waited enough, allow it here too
    for pr in (Priority.INTERACTIVE, Priority.QUERY):
        q = s.waiting_queues[pr]
        if not q:
            continue
        # Peek at head for age (approximation)
        head = q[0]
        first_seen = s.first_seen_tick.get(head.pipeline_id, s.tick)
        if (s.tick - first_seen) >= s.spillover_age_ticks:
            eligible.append(pr)

    return eligible


def _weights_for_pool(s, pool_id):
    if s.executor.num_pools <= 1:
        # Single pool: balanced weights, interactive slightly favored
        return {Priority.INTERACTIVE: 4, Priority.QUERY: 3, Priority.BATCH_PIPELINE: 2}
    if pool_id == s.interactive_pool_id:
        return s.weights_interactive_pool
    return s.weights_other_pools


def _pick_priority_drr(s, pool_id, eligible):
    # Pick a priority among eligible using deficits; return None if nothing runnable.
    # We cap the number of selection attempts to avoid infinite loops.
    for _ in range(8):
        # Choose eligible priority with highest deficit that also has any queued pipelines
        best_pr = None
        best_def = -1
        for pr in eligible:
            if not s.waiting_queues[pr]:
                continue
            d = s.deficit[pool_id].get(pr, 0)
            if d > best_def:
                best_def = d
                best_pr = pr

        if best_pr is None:
            return None

        if best_def > 0:
            return best_pr

        # If no one has deficit, caller should have topped up. Return best_pr anyway to make progress.
        return best_pr

    return None


@register_scheduler(key="scheduler_low_011_r13")
def scheduler_low_011_r13(s, results, pipelines):
    """
    Weighted priority scheduler (multi-assignment per pool per tick) + simple OOM RAM hinting.

    Main intended impact on latency:
    - Reduce queueing delay by filling pools each tick with multiple assignments.
    - Prevent interactive starvation by weighted fairness and spillover.
    """
    s.tick += 1

    # Enqueue new pipelines (de-duped)
    for p in pipelines:
        pr = _norm_priority(p.priority)
        if p.pipeline_id not in s.first_seen_tick:
            s.first_seen_tick[p.pipeline_id] = s.tick
        if not s.in_queue.get(p.pipeline_id, False) and not _pipeline_is_done(p):
            s.waiting_queues[pr].append(p)
            s.in_queue[p.pipeline_id] = True

    # Update OOM hints from results (RAM-first backoff)
    for r in results:
        failed = False
        try:
            failed = r.failed()
        except Exception:
            failed = getattr(r, "error", None) is not None

        if not failed:
            continue

        if not _is_oom_error(getattr(r, "error", None)):
            continue

        ops = getattr(r, "ops", None) or []
        for op in ops:
            k = id(op)
            prev = s.op_hints.get(k, {})
            prev_ram = float(prev.get("ram", 0.0) or 0.0)
            prev_cpu = float(prev.get("cpu", 0.0) or 0.0)

            observed_ram = float(getattr(r, "ram", 0.0) or 0.0)
            observed_cpu = float(getattr(r, "cpu", 0.0) or 0.0)

            base_ram = max(prev_ram, observed_ram, 1.0)
            new_ram = base_ram * 2.0

            # Keep CPU hint if present, but don't aggressively change it on OOM.
            new_cpu = max(prev_cpu, observed_cpu, 1.0)

            s.op_hints[k] = {"ram": new_ram, "cpu": new_cpu}
            s.op_attempts[k] = int(s.op_attempts.get(k, 0)) + 1

    # Early exit
    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    # Top up deficits once per tick, per pool
    for pool_id in range(s.executor.num_pools):
        weights = _weights_for_pool(s, pool_id)
        for pr, w in weights.items():
            s.deficit[pool_id][pr] = int(s.deficit[pool_id].get(pr, 0)) + int(w)

    # Schedule: fill each pool with multiple assignments
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)
        if avail_cpu < 1.0 or avail_ram < 1.0:
            continue

        eligible = _eligible_priorities_for_pool(s, pool_id)
        is_interactive_pool = (s.executor.num_pools > 1 and pool_id == s.interactive_pool_id) or (s.executor.num_pools <= 1)

        made = 0
        # Continue assigning while we have room
        while made < s.max_assignments_per_pool_per_tick and avail_cpu >= 1.0 and avail_ram >= 1.0:
            pr = _pick_priority_drr(s, pool_id, eligible)
            if pr is None:
                break

            # Consume one unit of deficit per assignment attempt that actually schedules work
            # (We only decrement on success; this avoids "charging" empty queues.)
            scheduled = False

            # Bounded scan to find a runnable pipeline/op in this priority queue
            scans = 0
            pipeline = _try_dequeue_pipeline(s, pr)
            while pipeline is not None and scans < s.max_scan_per_assignment:
                scans += 1

                # If pipeline has failed ops and we have exceeded retry budget for any op, stop trying (drop it).
                if _pipeline_has_failed_ops(pipeline):
                    # Heuristic: if we have any op in this pipeline with too many attempts, drop pipeline from queues.
                    # We can't easily enumerate ops here without deeper introspection, so only drop if *all* attempts are high
                    # is not possible; instead, be conservative: keep it unless we detect repeated OOM attempts on the next op.
                    pass

                op = _next_ready_op(pipeline)
                if op is None:
                    # Not ready; rotate to back of queue
                    _requeue_pipeline(s, pr, pipeline)
                    pipeline = _try_dequeue_pipeline(s, pr)
                    continue

                # If this op has exceeded retry budget due to repeated OOM, stop scheduling it (drop pipeline).
                k = id(op)
                if int(s.op_attempts.get(k, 0)) > int(s.max_retries_per_op):
                    # Do not requeue; effectively abandons the pipeline to avoid infinite retries.
                    pipeline = _try_dequeue_pipeline(s, pr)
                    continue

                cpu_req, ram_req = _compute_request(s, pool, pr, op, is_interactive_pool)

                # Clamp request to what is currently available (we allow smaller CPU; RAM below hint risks OOM).
                cpu_use = min(cpu_req, avail_cpu)
                ram_use = min(ram_req, avail_ram)

                # Must be at least 1 unit to be meaningful in this simulator's resource model.
                if cpu_use < 1.0 or ram_use < 1.0:
                    # Can't place right now; requeue and stop trying in this pool (likely fragmented).
                    _requeue_pipeline(s, pr, pipeline)
                    pipeline = None
                    break

                # Schedule exactly one op (safe default)
                assignments.append(
                    Assignment(
                        ops=[op],
                        cpu=cpu_use,
                        ram=ram_use,
                        priority=pr,
                        pool_id=pool_id,
                        pipeline_id=pipeline.pipeline_id,
                    )
                )

                avail_cpu -= cpu_use
                avail_ram -= ram_use

                _requeue_pipeline(s, pr, pipeline)
                s.deficit[pool_id][pr] = int(s.deficit[pool_id].get(pr, 0)) - 1

                made += 1
                scheduled = True
                break

            if not scheduled:
                # Could not schedule from this priority right now.
                # If there's still room, try another priority; otherwise break.
                # To avoid spinning, if scans were high and nothing scheduled, stop filling this pool this tick.
                if scans >= s.max_scan_per_assignment:
                    break
                # Otherwise continue to next iteration and let DRR pick another class.
                continue

    return suspensions, assignments