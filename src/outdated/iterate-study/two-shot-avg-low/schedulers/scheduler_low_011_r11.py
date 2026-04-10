# source: scheduler_low_011.py
# model: gpt-5.2-2025-12-11
# effort: low
# feedback: minimal
# iteration: r11
@register_scheduler_init(key="scheduler_low_011_r11")
def scheduler_low_011_r11_init(s):
    """Iteration 2: priority-first, OOM-retry-capable FIFO with better packing and pool protection.

    Small, latency-oriented improvements over the previous version:
    - Fix an obvious flaw: do NOT drop pipelines just because they have FAILED ops; allow OOM-retry.
    - Track (pipeline_id, op) mapping on assignment so ExecutionResult can update hints even if it lacks pipeline_id.
    - Schedule multiple ops per pool per tick (bounded), instead of at most one, to reduce queueing latency.
    - Protect the "interactive" pool from admitting new batch work while high-priority backlog exists,
      and keep a small headroom reserve when allowing batch on the interactive pool.
    - Use smaller default RAM probes to increase concurrency, with exponential RAM backoff on OOM.
    """
    # Per-priority FIFO queues of Pipeline objects
    s.waiting_queues = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }

    # Learned per-operator hints keyed by (pipeline_id, op_id)
    s.op_hints = {}  # (pipeline_id, op_id) -> {"ram": float, "cpu": float}
    s.op_attempts = {}  # (pipeline_id, op_id) -> int
    s.retryable_ops = set()  # set[(pipeline_id, op_id)] (OOM-triggered retries)

    # If we see a non-OOM failure for a pipeline, stop scheduling it.
    s.nonretryable_failed_pipelines = set()  # set[pipeline_id]

    # Mapping to recover pipeline_id from results even if ExecutionResult doesn't carry it.
    s.opid_to_pipeline_id = {}  # op_id -> pipeline_id

    # Config knobs (kept modest and safe)
    s.max_retries_per_op = 3

    # "Interactive pool" preference (if multiple pools exist)
    s.interactive_pool_id = 0

    # Per-tick cap on how many new containers to start per pool (reduces queueing without over-fragmenting)
    s.max_new_assignments_per_pool = 4

    # Default RAM probe fractions (smaller than before to improve concurrency; OOM backoff corrects)
    s.default_ram_frac = {
        Priority.QUERY: 0.40,
        Priority.INTERACTIVE: 0.40,
        Priority.BATCH_PIPELINE: 0.25,
    }

    # Default CPU targets: dynamic based on backlog; these are upper bounds
    s.max_cpu_frac = {
        Priority.QUERY: 0.75,
        Priority.INTERACTIVE: 0.75,
        Priority.BATCH_PIPELINE: 1.00,  # batch can use leftovers in non-interactive pools
    }

    # Keep headroom on the interactive pool when admitting batch (helps sudden interactive arrivals)
    s.interactive_batch_reserve_cpu_frac = 0.20
    s.interactive_batch_reserve_ram_frac = 0.20


def _prio_norm(s, p):
    return p if p in s.waiting_queues else Priority.BATCH_PIPELINE


def _priority_order():
    return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]


def _is_oom_error(err):
    if err is None:
        return False
    msg = str(err).lower()
    return ("oom" in msg) or ("out of memory" in msg) or ("memoryerror" in msg)


def _op_uid(pipeline_id, op):
    return (pipeline_id, id(op))


def _queue_len(s, pr):
    q = s.waiting_queues.get(pr)
    return len(q) if q is not None else 0


def _has_high_prio_backlog(s):
    return (_queue_len(s, Priority.QUERY) + _queue_len(s, Priority.INTERACTIVE)) > 0


def _pool_is_interactive(s, pool_id):
    return (s.executor.num_pools > 1) and (pool_id == s.interactive_pool_id)


def _cpu_target(s, pool, priority, high_backlog):
    # Start from a max fraction of pool capacity, then reduce per-op target when backlog is high
    max_cpu = max(1.0, float(pool.max_cpu_pool))
    avail_cpu = float(pool.avail_cpu_pool)

    base = max(1.0, max_cpu * float(s.max_cpu_frac.get(priority, 1.0)))

    # Backlog-sensitive downshift for high priority: prefer running multiple ops concurrently vs one big op
    if priority in (Priority.QUERY, Priority.INTERACTIVE):
        if high_backlog >= 4:
            base = min(base, max(1.0, max_cpu * 0.25))
        elif high_backlog >= 2:
            base = min(base, max(1.0, max_cpu * 0.50))
    else:
        # Batch: if any high-priority backlog exists, keep batch containers smaller by default
        if high_backlog > 0:
            base = min(base, max(1.0, max_cpu * 0.25))
        else:
            # If no high-priority waiting, batch can expand more
            base = min(base, max(1.0, max_cpu * 0.75))

    return min(base, avail_cpu)


def _ram_target(s, pool, priority):
    max_ram = max(1.0, float(pool.max_ram_pool))
    avail_ram = float(pool.avail_ram_pool)
    base = max(1.0, max_ram * float(s.default_ram_frac.get(priority, 0.30)))
    return min(base, avail_ram)


def _apply_hints(s, pool, pipeline_id, op, priority, high_backlog):
    cpu = _cpu_target(s, pool, priority, high_backlog)
    ram = _ram_target(s, pool, priority)

    uid = _op_uid(pipeline_id, op)
    hint = s.op_hints.get(uid)
    if hint:
        # Always respect the larger of default and hint to avoid repeating known-bad sizes
        cpu = max(cpu, float(hint.get("cpu", cpu)))
        ram = max(ram, float(hint.get("ram", ram)))

    # Cap to pool availability and max
    cpu = min(max(1.0, cpu), float(pool.avail_cpu_pool), float(pool.max_cpu_pool))
    ram = min(max(1.0, ram), float(pool.avail_ram_pool), float(pool.max_ram_pool))
    return cpu, ram


def _pipeline_done_or_drop(s, pipeline):
    status = pipeline.runtime_status()
    if status.is_pipeline_successful():
        return True  # done

    # If marked non-retryable, drop
    if pipeline.pipeline_id in s.nonretryable_failed_pipelines:
        return True

    # If there are FAILED ops, only keep pipeline if all failed ops are retryable (OOM) and within retry budget.
    failed_ops = status.get_ops([OperatorState.FAILED], require_parents_complete=False)
    if failed_ops:
        for op in failed_ops:
            uid = _op_uid(pipeline.pipeline_id, op)
            if uid not in s.retryable_ops:
                s.nonretryable_failed_pipelines.add(pipeline.pipeline_id)
                return True
            if int(s.op_attempts.get(uid, 0)) > int(s.max_retries_per_op):
                s.nonretryable_failed_pipelines.add(pipeline.pipeline_id)
                return True

    return False  # keep


def _next_assignable_op(pipeline):
    status = pipeline.runtime_status()
    ops = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
    return ops[0] if ops else None


def _can_admit_batch_on_interactive_pool(s, pool, req_cpu, req_ram, high_prio_backlog):
    # If any high-priority is waiting, do not start NEW batch on the interactive pool.
    if high_prio_backlog > 0:
        return False

    # Otherwise keep some headroom for sudden interactive arrivals.
    reserve_cpu = float(pool.max_cpu_pool) * float(s.interactive_batch_reserve_cpu_frac)
    reserve_ram = float(pool.max_ram_pool) * float(s.interactive_batch_reserve_ram_frac)

    return (float(pool.avail_cpu_pool) - float(req_cpu) >= reserve_cpu) and (
        float(pool.avail_ram_pool) - float(req_ram) >= reserve_ram
    )


def _rotate_pick_pipeline(s, pr, pipelines_scheduled_this_tick):
    """Round-robin within a priority queue while keeping FIFO-ish behavior.

    Pops from the front; if not usable now, appends to back.
    Returns a pipeline or None.
    """
    q = s.waiting_queues[pr]
    n = len(q)
    for _ in range(n):
        p = q.pop(0)

        # Avoid scheduling multiple ops from the same pipeline in one tick (reduces head-of-line blocking)
        if p.pipeline_id in pipelines_scheduled_this_tick:
            q.append(p)
            continue

        if _pipeline_done_or_drop(s, p):
            continue

        return p

    return None


@register_scheduler(key="scheduler_low_011_r11")
def scheduler_low_011_r11(s, results: List["ExecutionResult"], pipelines: List["Pipeline"]) -> Tuple[List["Suspend"], List["Assignment"]]:
    # Enqueue new pipelines
    for p in pipelines:
        pr = _prio_norm(s, p.priority)
        s.waiting_queues[pr].append(p)

    # Update hints and retryability based on results
    for r in results:
        # Determine failure status robustly
        is_failed = False
        try:
            is_failed = bool(r.failed())
        except Exception:
            is_failed = getattr(r, "error", None) is not None

        if not is_failed:
            continue

        ops = getattr(r, "ops", None) or []
        err = getattr(r, "error", None)
        oom = _is_oom_error(err)

        # Try to get pipeline_id from result; otherwise recover from op->pipeline mapping
        res_pipeline_id = getattr(r, "pipeline_id", None)

        for op in ops:
            op_id = id(op)
            pipeline_id = res_pipeline_id if res_pipeline_id is not None else s.opid_to_pipeline_id.get(op_id)
            if pipeline_id is None:
                # Without pipeline_id we cannot safely manage retries; treat as non-retryable best-effort.
                continue

            uid = (pipeline_id, op_id)

            if not oom:
                s.nonretryable_failed_pipelines.add(pipeline_id)
                continue

            # OOM: mark retryable and increase RAM hint exponentially (capped later by pool max)
            s.retryable_ops.add(uid)
            s.op_attempts[uid] = int(s.op_attempts.get(uid, 0)) + 1

            prev_hint = s.op_hints.get(uid, {})
            prev_ram = float(prev_hint.get("ram", 0.0))
            prev_cpu = float(prev_hint.get("cpu", 1.0))

            # Use observed allocation as baseline if present; otherwise use existing hint; else 1.0
            observed_ram = float(getattr(r, "ram", 0.0) or 0.0)
            baseline_ram = max(prev_ram, observed_ram, 1.0)
            new_ram = baseline_ram * 2.0

            observed_cpu = float(getattr(r, "cpu", 0.0) or 0.0)
            baseline_cpu = max(prev_cpu, observed_cpu, 1.0)

            s.op_hints[uid] = {"ram": new_ram, "cpu": baseline_cpu}

            # If exceeded retry budget, stop retrying the whole pipeline
            if int(s.op_attempts[uid]) > int(s.max_retries_per_op):
                s.nonretryable_failed_pipelines.add(pipeline_id)

    # If nothing changed, exit
    if not pipelines and not results:
        return [], []

    suspensions: List["Suspend"] = []
    assignments: List["Assignment"] = []

    pipelines_scheduled_this_tick = set()

    # For each pool, start up to N containers, priority-first.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        if float(pool.avail_cpu_pool) <= 0.0 or float(pool.avail_ram_pool) <= 0.0:
            continue

        high_backlog = _queue_len(s, Priority.QUERY) + _queue_len(s, Priority.INTERACTIVE)
        started = 0

        while started < int(s.max_new_assignments_per_pool):
            if float(pool.avail_cpu_pool) <= 0.0 or float(pool.avail_ram_pool) <= 0.0:
                break

            picked = None
            picked_pr = None

            # Interactive pool rule: prefer high-priority; restrict batch admissions as configured.
            for pr in _priority_order():
                # If interactive pool and batch, only admit if allowed (headroom reserve, no high backlog)
                if _pool_is_interactive(s, pool_id) and pr == Priority.BATCH_PIPELINE:
                    # We'll check admission after we compute the request; still, if high backlog exists, skip quickly.
                    if high_backlog > 0:
                        continue

                p = _rotate_pick_pipeline(s, pr, pipelines_scheduled_this_tick)
                if p is None:
                    continue

                op = _next_assignable_op(p)
                if op is None:
                    # Not ready; put back and move on
                    s.waiting_queues[pr].append(p)
                    continue

                # Compute request with hints
                cpu, ram = _apply_hints(s, pool, p.pipeline_id, op, pr, high_backlog)

                # If interactive pool and batch, enforce reserve/headroom
                if _pool_is_interactive(s, pool_id) and pr == Priority.BATCH_PIPELINE:
                    if not _can_admit_batch_on_interactive_pool(s, pool, cpu, ram, high_backlog):
                        # Defer batch elsewhere/later
                        s.waiting_queues[pr].append(p)
                        continue

                # Ensure fits current availability
                if float(cpu) > float(pool.avail_cpu_pool) or float(ram) > float(pool.avail_ram_pool):
                    # Not enough resources now; rotate to back and try other pipelines
                    s.waiting_queues[pr].append(p)
                    continue

                picked = (p, op, cpu, ram)
                picked_pr = pr
                break

            if picked is None:
                break

            p, op, cpu, ram = picked

            # Record mapping to recover pipeline_id on results
            s.opid_to_pipeline_id[id(op)] = p.pipeline_id

            assignments.append(
                Assignment(
                    ops=[op],
                    cpu=cpu,
                    ram=ram,
                    priority=picked_pr,
                    pool_id=pool_id,
                    pipeline_id=p.pipeline_id,
                )
            )

            pipelines_scheduled_this_tick.add(p.pipeline_id)
            started += 1

            # Re-enqueue pipeline for its subsequent ops
            s.waiting_queues[picked_pr].append(p)

    return suspensions, assignments