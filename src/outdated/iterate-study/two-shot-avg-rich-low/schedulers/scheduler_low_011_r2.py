# source: scheduler_low_011.py
# model: gpt-5.2-2025-12-11
# effort: low
# feedback: rich
# iteration: r2
@register_scheduler_init(key="scheduler_low_011_r2")
def scheduler_low_011_r2_init(s):
    """Priority-aware queueing + pool-filling packing (r2).

    Small, incremental improvements over the prior iteration:
    - Fill each pool with multiple assignments per tick (fixes underutilization from 1-op-per-pool behavior).
    - Weighted round-robin across priorities to prevent starvation (QUERY and INTERACTIVE both get steady service).
    - Simple "batch backfill with aging": batch runs when no high-priority backlog, or when batch has waited too long.
    - Keep OOM-aware RAM backoff hints (best-effort) and bound retries to avoid infinite loops.

    Design intent:
    - Reduce queueing delay (main driver of median latency) by increasing effective concurrency,
      while still biasing toward high-priority latency.
    """
    from collections import deque

    s._deque = deque
    s.ticks = 0

    # Per-priority FIFO queues.
    s.queues = {
        Priority.QUERY: deque(),
        Priority.INTERACTIVE: deque(),
        Priority.BATCH_PIPELINE: deque(),
    }

    # Pipeline metadata for aging/backfill decisions.
    s.pipeline_first_seen_tick = {}  # pipeline_id -> tick

    # Operator hints keyed by operator object id (works even if results lack pipeline_id).
    # op_id -> {"ram": float, "cpu": float}
    s.op_hints = {}
    s.op_attempts = {}          # op_id -> int failure attempts
    s.op_retryable = set()      # op_id deemed retryable (OOM-like)
    s.op_nonretryable = set()   # op_id deemed non-retryable

    # Retry bounds (keep small; we only want to recover from RAM underestimation).
    s.max_retries_per_op = 3

    # Packing controls.
    s.max_assignments_per_pool_per_tick = 8
    s.min_cpu = 1.0
    s.min_ram = 1.0

    # CPU/RAM slicing targets: allocation ~= pool.max / slots (then bumped by hints if needed).
    # More slots => smaller per-op allocations => higher concurrency.
    s.cpu_slots = {
        Priority.QUERY: 3,
        Priority.INTERACTIVE: 3,
        Priority.BATCH_PIPELINE: 6,
    }
    # RAM is more failure-sensitive; keep allocations larger than CPU slicing.
    s.ram_slots = {
        Priority.QUERY: 2,
        Priority.INTERACTIVE: 2,
        Priority.BATCH_PIPELINE: 4,
    }

    # Weighted service order within each pool fill loop.
    # (Ensures INTERACTIVE is not starved by continuous QUERY arrivals.)
    s.weight_cycle = [
        Priority.QUERY,
        Priority.INTERACTIVE,
        Priority.QUERY,
        Priority.INTERACTIVE,
        Priority.BATCH_PIPELINE,
    ]

    # Batch backfill / starvation prevention: allow some batch if it's been waiting too long.
    s.batch_starvation_ticks = 40

    # When high-priority backlog exists, keep some headroom by limiting batch to "leftovers".
    s.batch_headroom_frac_cpu = 0.25
    s.batch_headroom_frac_ram = 0.25


def _oom_like(err):
    if err is None:
        return False
    msg = str(err).lower()
    return ("oom" in msg) or ("out of memory" in msg) or ("memoryerror" in msg)


def _normalized_priority(p):
    pr = getattr(p, "priority", None)
    if pr in (Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE):
        return pr
    return Priority.BATCH_PIPELINE


def _first_ready_op(pipeline):
    status = pipeline.runtime_status()
    ops = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
    if not ops:
        return None
    return ops[0]


def _failed_ops_best_effort(status):
    # Best-effort; simulator implementations may vary.
    try:
        return status.get_ops([OperatorState.FAILED], require_parents_complete=False) or []
    except Exception:
        return []


def _pipeline_drop_or_keep(s, pipeline):
    status = pipeline.runtime_status()
    if status.is_pipeline_successful():
        return False  # drop

    # If there are failed ops that we believe are non-retryable (or exceeded retries), drop the pipeline.
    # Otherwise keep it (FAILED is in ASSIGNABLE_STATES so it can be retried).
    failed_ops = _failed_ops_best_effort(status)
    for op in failed_ops:
        op_id = id(op)
        if op_id in s.op_nonretryable:
            return False
        if s.op_attempts.get(op_id, 0) > s.max_retries_per_op:
            return False
    return True  # keep


def _oldest_tick_in_queue(s, pr):
    q = s.queues[pr]
    if not q:
        return None
    # Use first element as "oldest-ish" (FIFO); good enough for backfill decisions.
    pid = q[0].pipeline_id
    return s.pipeline_first_seen_tick.get(pid, s.ticks)


def _request_size(s, pool, pr, op, avail_cpu, avail_ram, high_backlog_present):
    # Base share: pool.max / slots
    cpu_slots = max(1, int(s.cpu_slots.get(pr, 3)))
    ram_slots = max(1, int(s.ram_slots.get(pr, 2)))

    cpu = max(s.min_cpu, float(pool.max_cpu_pool) / float(cpu_slots))
    ram = max(s.min_ram, float(pool.max_ram_pool) / float(ram_slots))

    # Apply learned hints (primarily RAM after OOM).
    hint = s.op_hints.get(id(op))
    if hint:
        try:
            cpu = max(cpu, float(hint.get("cpu", cpu)))
        except Exception:
            pass
        try:
            ram = max(ram, float(hint.get("ram", ram)))
        except Exception:
            pass

    # Cap by pool maxima (defensive).
    cpu = min(cpu, float(pool.max_cpu_pool))
    ram = min(ram, float(pool.max_ram_pool))

    # Cap by currently available resources.
    cpu = min(cpu, float(avail_cpu))
    ram = min(ram, float(avail_ram))

    # If we're under high-priority pressure, don't let batch consume the last headroom.
    if pr == Priority.BATCH_PIPELINE and high_backlog_present:
        head_cpu = float(pool.max_cpu_pool) * float(s.batch_headroom_frac_cpu)
        head_ram = float(pool.max_ram_pool) * float(s.batch_headroom_frac_ram)
        if (float(avail_cpu) - cpu) < head_cpu or (float(avail_ram) - ram) < head_ram:
            return None, None

    # Ensure non-zero allocations.
    if cpu < s.min_cpu or ram < s.min_ram:
        return None, None

    return cpu, ram


@register_scheduler(key="scheduler_low_011_r2")
def scheduler_low_011_r2(s, results, pipelines):
    """Scheduler step: update hints from results, enqueue arrivals, then fill each pool with packed assignments."""
    s.ticks += 1

    # Enqueue new pipelines (FIFO per priority).
    for p in pipelines:
        pr = _normalized_priority(p)
        s.queues[pr].append(p)
        if p.pipeline_id not in s.pipeline_first_seen_tick:
            s.pipeline_first_seen_tick[p.pipeline_id] = s.ticks

    # Update retry hints from execution results (best-effort, works even if pipeline_id is unavailable).
    for r in results:
        try:
            failed = r.failed()
        except Exception:
            failed = getattr(r, "error", None) is not None

        if not failed:
            continue

        ops = getattr(r, "ops", None) or []
        oom = _oom_like(getattr(r, "error", None))

        # Pool max caps (if pool_id is known).
        pool = None
        pool_id = getattr(r, "pool_id", None)
        if pool_id is not None and 0 <= int(pool_id) < s.executor.num_pools:
            pool = s.executor.pools[int(pool_id)]

        for op in ops:
            op_id = id(op)
            s.op_attempts[op_id] = int(s.op_attempts.get(op_id, 0)) + 1

            if oom and s.op_attempts[op_id] <= s.max_retries_per_op:
                s.op_retryable.add(op_id)

                prev = s.op_hints.get(op_id, {})
                # Start from the last known allocation if present; otherwise from observed r.ram.
                observed_ram = float(getattr(r, "ram", 0.0) or 0.0)
                base_ram = float(prev.get("ram", observed_ram if observed_ram > 0 else s.min_ram))
                new_ram = max(s.min_ram, base_ram * 2.0)

                # Keep CPU hint stable (we don't try to tune CPU aggressively in r2).
                observed_cpu = float(getattr(r, "cpu", 0.0) or 0.0)
                base_cpu = float(prev.get("cpu", observed_cpu if observed_cpu > 0 else s.min_cpu))
                new_cpu = max(s.min_cpu, base_cpu)

                if pool is not None:
                    new_ram = min(new_ram, float(pool.max_ram_pool))
                    new_cpu = min(new_cpu, float(pool.max_cpu_pool))

                s.op_hints[op_id] = {"ram": new_ram, "cpu": new_cpu}
            else:
                # Non-OOM failures are treated as non-retryable to avoid infinite loops.
                s.op_nonretryable.add(op_id)

    suspensions = []
    assignments = []

    # Helper values for batch backfill decisions.
    high_backlog = len(s.queues[Priority.QUERY]) + len(s.queues[Priority.INTERACTIVE])
    high_backlog_present = high_backlog > 0

    batch_oldest = _oldest_tick_in_queue(s, Priority.BATCH_PIPELINE)
    batch_wait = 0 if batch_oldest is None else (s.ticks - batch_oldest)

    # Allow batch when there's no high backlog, or when batch has starved long enough.
    allow_batch = (not high_backlog_present) or (batch_wait >= int(s.batch_starvation_ticks))

    # Fill each pool (packing multiple ops per pool per tick).
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)

        if avail_cpu < s.min_cpu or avail_ram < s.min_ram:
            continue

        cycle = s.weight_cycle
        # If batch is not allowed right now, we'll just skip it in selection.
        max_to_assign = int(s.max_assignments_per_pool_per_tick)
        assigned_here = 0
        cycle_idx = 0

        while assigned_here < max_to_assign and avail_cpu >= s.min_cpu and avail_ram >= s.min_ram:
            made_progress = False

            # Try a bounded number of priority picks per "slot" to avoid infinite loops when queues are blocked.
            for _ in range(len(cycle)):
                pr = cycle[cycle_idx % len(cycle)]
                cycle_idx += 1

                if pr == Priority.BATCH_PIPELINE and not allow_batch:
                    continue

                q = s.queues[pr]
                if not q:
                    continue

                # Scan the queue at most once to find a runnable + fitting pipeline.
                scan_n = len(q)
                for _scan in range(scan_n):
                    p = q.popleft()

                    # Drop completed / irrecoverably failed pipelines; keep others.
                    if not _pipeline_drop_or_keep(s, p):
                        continue

                    op = _first_ready_op(p)
                    if op is None:
                        # Not ready yet; keep FIFO order by appending to the back.
                        q.append(p)
                        continue

                    cpu_req, ram_req = _request_size(
                        s, pool, pr, op, avail_cpu, avail_ram, high_backlog_present
                    )
                    if cpu_req is None or ram_req is None:
                        # Doesn't fit (or would violate headroom); push back and keep scanning.
                        q.append(p)
                        continue

                    # Assign exactly one op from this pipeline.
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

                    avail_cpu -= float(cpu_req)
                    avail_ram -= float(ram_req)
                    assigned_here += 1

                    # Put pipeline back so downstream ops can be scheduled later.
                    q.append(p)

                    made_progress = True
                    break  # stop scanning this priority queue

                if made_progress:
                    break  # move to next packing slot (re-evaluate priorities)

            if not made_progress:
                break  # nothing fit / runnable in any queue for this pool

    return suspensions, assignments