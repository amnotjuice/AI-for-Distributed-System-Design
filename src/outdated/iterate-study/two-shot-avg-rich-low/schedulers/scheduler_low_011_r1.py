# source: scheduler_low_011.py
# model: gpt-5.2-2025-12-11
# effort: low
# feedback: rich
# iteration: r1
@register_scheduler_init(key="scheduler_low_011_r1")
def scheduler_low_011_r1_init(s):
    """Iteration r1: latency-focused, priority-aware packing (still low complexity).

    Key changes vs the previous attempt:
    - Pack MULTIPLE assignments per pool per tick (the prior "1 op per pool per tick" left capacity idle).
    - Use a simple Deficit Round Robin (DRR) across priorities so INTERACTIVE doesn't starve behind QUERY.
    - Use smaller, fixed-ish CPU quanta for high-priority work to increase parallelism and reduce queueing latency.
    - Keep batch from consuming the last slice of a pool when high-priority backlog exists (soft headroom reservation).
    - Track op->pipeline mapping to learn from OOM failures and increase RAM on retry.
    """
    # FIFO queues per priority (pipelines stay in the queue until completed or deemed terminally failed)
    s.waiting_queues = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }

    # Learned per-operator hints keyed by operator object id(op)
    # {"ram": float, "cpu": float}
    s.op_hints = {}
    s.op_attempts = {}  # id(op) -> attempts count

    # Map operator object id(op) back to its pipeline_id (so ExecutionResult can be attributed)
    s.op_to_pipeline = {}

    # Track non-retryable failures (anything that's not OOM)
    s.nonretryable_failed_ops = set()

    # Retry configuration for OOM handling
    s.max_retries_per_op = 3

    # DRR (Deficit Round Robin) scheduling across priorities
    s.priorities = [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]
    s.drr_quantum = {
        Priority.QUERY: 5.0,
        Priority.INTERACTIVE: 2.0,
        Priority.BATCH_PIPELINE: 1.0,
    }
    s.drr_deficit = {}  # pool_id -> {priority -> deficit}
    s.drr_cursor = {}  # pool_id -> cursor index into s.priorities

    # Per-priority resource shaping:
    # Smaller quanta for high-priority => more parallelism, lower queueing.
    s.cpu_quantum = {
        Priority.QUERY: 2.0,
        Priority.INTERACTIVE: 2.0,
        Priority.BATCH_PIPELINE: 4.0,
    }
    # Baseline RAM request as a fraction of pool max RAM (then capped by available and hints).
    # Keep this moderate; if an op OOMs, we learn and retry with higher RAM.
    s.ram_frac = {
        Priority.QUERY: 0.30,
        Priority.INTERACTIVE: 0.35,
        Priority.BATCH_PIPELINE: 0.60,
    }

    # To avoid a single pipeline dominating within a tick, schedule at most one op per pipeline per tick.
    s.max_ops_per_pipeline_per_tick = 1

    # Pack multiple assignments per pool per tick, but cap for simulator overhead control
    s.max_assignments_per_pool_per_tick = 32

    # Soft reservation when high-priority backlog exists: don't let BATCH consume the last slice.
    s.hp_reserve_frac = {"cpu": 0.20, "ram": 0.20}


def _is_oom_error(err):
    if err is None:
        return False
    msg = str(err).lower()
    return ("oom" in msg) or ("out of memory" in msg) or ("memoryerror" in msg)


def _queue_len(s, pr):
    q = s.waiting_queues.get(pr)
    return len(q) if q is not None else 0


def _has_hp_backlog(s):
    return (_queue_len(s, Priority.QUERY) > 0) or (_queue_len(s, Priority.INTERACTIVE) > 0)


def _init_pool_drr_if_needed(s, pool_id):
    if pool_id not in s.drr_deficit:
        s.drr_deficit[pool_id] = {pr: 0.0 for pr in s.priorities}
    if pool_id not in s.drr_cursor:
        s.drr_cursor[pool_id] = 0


def _pipeline_terminally_failed(s, status):
    # If pipeline has FAILED ops, only treat as terminally failed if any failed op is non-retryable
    # or exceeded OOM retry budget.
    failed_count = status.state_counts.get(OperatorState.FAILED, 0) if hasattr(status, "state_counts") else 0
    if failed_count <= 0:
        return False

    failed_ops = status.get_ops([OperatorState.FAILED], require_parents_complete=False) or []
    for op in failed_ops:
        oid = id(op)
        if oid in s.nonretryable_failed_ops:
            return True
        if s.op_attempts.get(oid, 0) > s.max_retries_per_op:
            return True
    return False


def _dequeue_ready_pipeline_and_op(s, pr, scheduled_counts, scan_limit=32):
    """Find a pipeline in priority queue with a ready-to-assign op; rotate queue for fairness.

    Returns: (pipeline, op) or (None, None)
    """
    q = s.waiting_queues.get(pr, [])
    if not q:
        return None, None

    n = min(len(q), scan_limit)
    for _ in range(n):
        p = q.pop(0)
        status = p.runtime_status()

        # Drop completed pipelines
        if status.is_pipeline_successful():
            continue

        # Drop pipelines that we consider terminally failed (to avoid infinite cycling)
        if _pipeline_terminally_failed(s, status):
            continue

        # Enforce per-tick pipeline cap (avoid one pipeline taking multiple slots within the same tick)
        if scheduled_counts.get(p.pipeline_id, 0) >= s.max_ops_per_pipeline_per_tick:
            q.append(p)
            continue

        ops = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True) or []
        if not ops:
            # Not ready yet; keep it in the queue
            q.append(p)
            continue

        op = ops[0]
        # Keep pipeline in the queue (at the end) so later ops can be scheduled in future ticks
        q.append(p)
        return p, op

    return None, None


def _request_resources(s, pool, pr, op):
    """Compute a conservative-but-parallelism-friendly (cpu, ram) request, applying hints."""
    # Baseline CPU quantum (small for high-priority to increase concurrency)
    cpu = float(s.cpu_quantum.get(pr, 2.0))
    cpu = max(1.0, min(cpu, float(pool.avail_cpu_pool), float(pool.max_cpu_pool)))

    # Baseline RAM as fraction of pool max
    ram = float(pool.max_ram_pool) * float(s.ram_frac.get(pr, 0.50))
    ram = max(1.0, min(ram, float(pool.avail_ram_pool), float(pool.max_ram_pool)))

    # Apply learned hints from past OOMs / retries (if any)
    hint = s.op_hints.get(id(op))
    if hint:
        hint_cpu = float(hint.get("cpu", cpu))
        hint_ram = float(hint.get("ram", ram))
        cpu = max(cpu, hint_cpu)
        ram = max(ram, hint_ram)
        cpu = max(1.0, min(cpu, float(pool.avail_cpu_pool), float(pool.max_cpu_pool)))
        ram = max(1.0, min(ram, float(pool.avail_ram_pool), float(pool.max_ram_pool)))

    return cpu, ram


@register_scheduler(key="scheduler_low_011_r1")
def scheduler_low_011_r1(s, results, pipelines):
    """
    Priority-aware packing scheduler with DRR fairness and OOM-aware RAM backoff.

    Goals:
    - Reduce latency by reducing queueing: pack multiple small high-priority ops concurrently.
    - Prevent starvation: DRR ensures INTERACTIVE gets scheduled even when QUERY load is heavy.
    - Maintain robustness: on OOM-like failures, retry with increased RAM (bounded retries).
    """
    # Enqueue new pipelines
    for p in pipelines:
        pr = p.priority if p.priority in s.waiting_queues else Priority.BATCH_PIPELINE
        s.waiting_queues[pr].append(p)

    # Learn from execution results (especially OOM) using op identity
    for r in results:
        failed = False
        try:
            failed = r.failed()
        except Exception:
            failed = getattr(r, "error", None) is not None

        if not failed:
            continue

        ops = getattr(r, "ops", None) or []
        if _is_oom_error(getattr(r, "error", None)):
            # Retry with increased RAM (exponential backoff), keyed by operator identity
            for op in ops:
                oid = id(op)
                s.op_attempts[oid] = int(s.op_attempts.get(oid, 0)) + 1

                # Use observed RAM if present, else fall back to previous hint, else 1.0
                observed_ram = float(getattr(r, "ram", 0.0) or 0.0)
                prev_hint_ram = float(s.op_hints.get(oid, {}).get("ram", 0.0) or 0.0)
                base_ram = max(1.0, observed_ram, prev_hint_ram)

                new_ram = base_ram * 2.0  # exponential backoff
                new_cpu = float(getattr(r, "cpu", 1.0) or 1.0)
                new_cpu = max(1.0, new_cpu)

                s.op_hints[oid] = {"ram": new_ram, "cpu": new_cpu}
        else:
            # Mark these operators as non-retryable to avoid infinite loops
            for op in ops:
                s.nonretryable_failed_ops.add(id(op))

    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    # Track how many ops we've scheduled per pipeline in THIS scheduler invocation (tick)
    scheduled_counts = {}

    # Try to keep pools busy; pack multiple assignments per pool per tick
    hp_backlog = _has_hp_backlog(s)

    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        if pool.avail_cpu_pool <= 0 or pool.avail_ram_pool <= 0:
            continue

        _init_pool_drr_if_needed(s, pool_id)

        # Local accounting so we can pack multiple assignments without over-allocating
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)

        # Soft reserve (only matters when scheduling BATCH and HP backlog exists)
        reserve_cpu = float(pool.max_cpu_pool) * float(s.hp_reserve_frac["cpu"]) if hp_backlog else 0.0
        reserve_ram = float(pool.max_ram_pool) * float(s.hp_reserve_frac["ram"]) if hp_backlog else 0.0

        per_pool_assigned = 0
        tries_without_progress = 0

        while (
            per_pool_assigned < s.max_assignments_per_pool_per_tick
            and avail_cpu >= 1.0
            and avail_ram >= 1.0
            and tries_without_progress < len(s.priorities) * 4
        ):
            made_progress = False

            # DRR selection loop: advance cursor, add quantum, attempt to schedule from that class
            for _ in range(len(s.priorities)):
                cursor = int(s.drr_cursor[pool_id])
                pr = s.priorities[cursor]
                s.drr_cursor[pool_id] = (cursor + 1) % len(s.priorities)

                # Add quantum for this priority
                s.drr_deficit[pool_id][pr] = float(s.drr_deficit[pool_id].get(pr, 0.0)) + float(s.drr_quantum.get(pr, 1.0))

                # Need at least 1.0 "token" to schedule an op from this class
                if s.drr_deficit[pool_id][pr] < 1.0:
                    continue

                # Find a ready pipeline/op for this priority
                p, op = _dequeue_ready_pipeline_and_op(s, pr, scheduled_counts)
                if p is None or op is None:
                    continue

                # Compute request and ensure it fits current remaining resources
                cpu_req, ram_req = _request_resources(s, pool, pr, op)
                cpu_req = min(cpu_req, avail_cpu)
                ram_req = min(ram_req, avail_ram)
                cpu_req = max(1.0, cpu_req)
                ram_req = max(1.0, ram_req)

                # If this is BATCH and we have HP backlog, avoid consuming the last reserved slice
                if hp_backlog and pr == Priority.BATCH_PIPELINE:
                    if (avail_cpu - cpu_req) < reserve_cpu or (avail_ram - ram_req) < reserve_ram:
                        # Can't place more batch without eating reserved headroom; skip batch for now
                        continue

                if cpu_req > avail_cpu or ram_req > avail_ram:
                    continue

                # Record mapping so we can attribute future failures to the pipeline/op
                s.op_to_pipeline[id(op)] = p.pipeline_id

                # Make the assignment
                assignments.append(
                    Assignment(
                        ops=[op],
                        cpu=cpu_req,
                        ram=ram_req,
                        priority=pr,
                        pool_id=pool_id,
                        pipeline_id=p.pipeline_id,
                    )
                )

                # Update local remaining resources
                avail_cpu -= cpu_req
                avail_ram -= ram_req

                # Consume 1 token for this scheduled op
                s.drr_deficit[pool_id][pr] = float(s.drr_deficit[pool_id][pr]) - 1.0

                # Per-tick pipeline cap
                scheduled_counts[p.pipeline_id] = int(scheduled_counts.get(p.pipeline_id, 0)) + 1

                per_pool_assigned += 1
                made_progress = True
                break  # go back to while-loop to re-evaluate with updated resources

            if made_progress:
                tries_without_progress = 0
            else:
                tries_without_progress += 1

    return suspensions, assignments