# policy_key: scheduler_est_011
# reasoning_effort: low
# model: gpt-5.2-2025-12-11
# llm_cost: 0.048908
# generation_seconds: 28.46
# generated_at: 2026-03-31T23:52:17.484963
@register_scheduler_init(key="scheduler_est_011")
def scheduler_est_011_init(s):
    """Priority-aware FIFO with simple sizing.
    Improvements over naive FIFO:
      1) Separate waiting queues per priority (QUERY > INTERACTIVE > BATCH_PIPELINE)
      2) Allocate RAM using op.estimate.mem_peak_gb as a floor, with retry-based RAM bumps on failures
      3) Avoid giving all available CPU to a single op; use priority-based CPU caps for better latency/throughput balance
      4) Schedule multiple ops per pool per tick while resources remain
    """
    # Per-priority FIFO queues of pipelines
    s.waiting_queues = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }

    # Track per-operator RAM bumps after failures (best-effort keying)
    # key: (pipeline_id, op_key) -> bumped_ram_gb
    s.op_ram_bump_gb = {}

    # Conservative defaults to reduce OOMs even when estimate is missing
    s.default_min_ram_gb = {
        Priority.QUERY: 2.0,
        Priority.INTERACTIVE: 2.0,
        Priority.BATCH_PIPELINE: 1.0,
    }

    # CPU caps per op by priority (fraction of pool max CPU)
    # Rationale: cap background ops to keep headroom; let high-priority ops scale up.
    s.cpu_cap_frac = {
        Priority.QUERY: 1.0,
        Priority.INTERACTIVE: 0.75,
        Priority.BATCH_PIPELINE: 0.50,
    }

    # Minimum CPU per op to avoid starving (if available)
    s.min_cpu_per_op = {
        Priority.QUERY: 1.0,
        Priority.INTERACTIVE: 1.0,
        Priority.BATCH_PIPELINE: 1.0,
    }


def _priority_order():
    return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]


def _op_key(op):
    # Best-effort stable-ish operator key across retries
    if hasattr(op, "op_id"):
        return op.op_id
    if hasattr(op, "operator_id"):
        return op.operator_id
    if hasattr(op, "name"):
        return op.name
    return id(op)


def _get_assignable_op(pipeline):
    status = pipeline.runtime_status()
    if status.is_pipeline_successful():
        return None
    # Only assign ops whose parents are complete; prefer a single op to keep placement simple
    op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
    return op_list[0] if op_list else None


def _pipeline_is_done_or_failed(pipeline):
    status = pipeline.runtime_status()
    if status.is_pipeline_successful():
        return True
    # If any failures exist, we still allow retries (Eudoxia often marks retryable failures as FAILED)
    # We do NOT drop pipelines solely due to failures here; we rely on RAM bumps to improve success rate.
    return False


def _update_ram_bumps_from_results(s, results):
    # If an op fails, bump its RAM for next time (simple exponential backoff).
    for r in results:
        if not r.failed():
            continue
        # Determine per-op bump based on what was attempted
        attempted_ram = float(getattr(r, "ram", 0.0) or 0.0)
        if attempted_ram <= 0:
            continue
        # We assume any failure could be memory-related in absence of structured error types.
        # Bump by 2x, with a small additive cushion.
        bumped = max(attempted_ram * 2.0, attempted_ram + 1.0)

        # Apply bump to all ops in the result (usually one)
        for op in (getattr(r, "ops", None) or []):
            opk = _op_key(op)
            key = (getattr(r, "pipeline_id", None), opk)
            prev = s.op_ram_bump_gb.get(key, 0.0)
            s.op_ram_bump_gb[key] = max(prev, bumped)


def _desired_ram_gb(s, pipeline, op, pool_max_ram):
    # Use estimator as a floor when available; extra RAM is "free" per simulator model.
    est = None
    if hasattr(op, "estimate") and op.estimate is not None and hasattr(op.estimate, "mem_peak_gb"):
        est = op.estimate.mem_peak_gb
    est = float(est) if (est is not None) else None

    base = s.default_min_ram_gb.get(pipeline.priority, 1.0)
    ram_floor = max(base, est if est is not None else 0.0)

    # Apply per-op bump if present
    bump_key = (pipeline.pipeline_id, _op_key(op))
    bumped = s.op_ram_bump_gb.get(bump_key, 0.0)
    ram = max(ram_floor, bumped)

    # Cap to pool max
    return min(ram, float(pool_max_ram))


def _desired_cpu(s, pipeline, pool_max_cpu, avail_cpu):
    prio = pipeline.priority
    cap_frac = s.cpu_cap_frac.get(prio, 0.5)
    cap = max(1.0, float(pool_max_cpu) * float(cap_frac))

    # Ensure minimum CPU if available
    min_cpu = float(s.min_cpu_per_op.get(prio, 1.0))

    cpu = min(float(avail_cpu), cap)
    if cpu < min_cpu:
        # Not enough CPU headroom to start this op right now
        return 0.0
    return cpu


@register_scheduler(key="scheduler_est_011")
def scheduler_est_011(s, results, pipelines):
    """
    Priority-aware scheduling:
      - Enqueue arriving pipelines into per-priority FIFO queues.
      - React to failures by bumping RAM for the failing operator.
      - For each pool, greedily schedule ready ops from highest priority to lowest,
        using estimator-based RAM floors and priority-based CPU caps.
    """
    # Enqueue new pipelines by priority
    for p in pipelines:
        if p.priority not in s.waiting_queues:
            # Fallback: treat unknown priorities as lowest
            s.waiting_queues[Priority.BATCH_PIPELINE].append(p)
        else:
            s.waiting_queues[p.priority].append(p)

    # Incorporate feedback from previous tick
    if results:
        _update_ram_bumps_from_results(s, results)

    # Early exit if nothing to do
    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    # Helper: pop next runnable pipeline in given queue, preserving non-runnable pipelines
    def pop_next_runnable(queue):
        kept = []
        chosen = None
        while queue:
            pl = queue.pop(0)
            if _pipeline_is_done_or_failed(pl):
                continue
            op = _get_assignable_op(pl)
            if op is None:
                kept.append(pl)
                continue
            chosen = (pl, op)
            break
        # Put back the rest
        if kept:
            queue[:0] = kept
        return chosen

    # Schedule within each pool
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)

        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # Greedily fill the pool with ready ops, highest priority first
        made_progress = True
        while made_progress:
            made_progress = False

            for prio in _priority_order():
                if avail_cpu <= 0 or avail_ram <= 0:
                    break

                q = s.waiting_queues.get(prio, [])
                if not q:
                    continue

                picked = pop_next_runnable(q)
                if picked is None:
                    continue

                pipeline, op = picked

                # Decide sizes
                cpu = _desired_cpu(s, pipeline, pool.max_cpu_pool, avail_cpu)
                if cpu <= 0:
                    # Not enough CPU to start; put back and try lower priorities (or stop if none can run)
                    q.insert(0, pipeline)
                    continue

                ram_need = _desired_ram_gb(s, pipeline, op, pool.max_ram_pool)
                if ram_need <= 0:
                    ram_need = 1.0

                # If we can't fit RAM now, put it back and try other work
                if ram_need > avail_ram:
                    q.insert(0, pipeline)
                    continue

                # Assign this operator
                assignment = Assignment(
                    ops=[op],
                    cpu=cpu,
                    ram=ram_need,
                    priority=pipeline.priority,
                    pool_id=pool_id,
                    pipeline_id=pipeline.pipeline_id,
                )
                assignments.append(assignment)

                # Update remaining resources
                avail_cpu -= cpu
                avail_ram -= ram_need

                # Requeue the pipeline so subsequent ops can be scheduled later
                q.append(pipeline)

                made_progress = True
                break  # restart priority scan with updated avail resources

    return suspensions, assignments
