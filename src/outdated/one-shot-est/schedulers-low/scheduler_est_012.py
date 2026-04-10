# policy_key: scheduler_est_012
# reasoning_effort: low
# model: gpt-5.2-2025-12-11
# llm_cost: 0.033326
# generation_seconds: 35.00
# generated_at: 2026-03-31T23:52:52.485475
@register_scheduler_init(key="scheduler_est_012")
def scheduler_est_012_init(s):
    """Priority-aware, estimate-guided FIFO with small, safe improvements over naive FIFO.

    Improvements (kept intentionally simple / low-risk):
      1) Priority queues: schedule QUERY/INTERACTIVE ahead of BATCH to reduce tail latency.
      2) Right-size per-op resources:
         - RAM: allocate at least op.estimate.mem_peak_gb (as a floor) to reduce OOMs.
         - CPU: cap per-op CPU to avoid one op monopolizing a pool (improves latency under contention).
      3) OOM retry bias: if an op fails (likely OOM), increase future RAM allocations for that pipeline.
      4) Simple fairness: occasionally allow a BATCH op even if high-priority work is queued.

    Notes:
      - No preemption yet (needs access to running containers list); kept incremental and robust.
      - Extra RAM is "free" in the simulator, but still bounded by pool availability.
    """
    # Separate waiting queues by priority (FIFO within each priority).
    s.waiting_q = []
    s.waiting_i = []
    s.waiting_b = []

    # Track per-pipeline RAM boost after failures (e.g., OOM). Multiplies estimated RAM floor.
    s.pipeline_ram_boost = {}  # pipeline_id -> float

    # Simple fairness knobs: after N high-priority assignments, force one batch if available.
    s.hp_assignments_since_batch = 0
    s.batch_every = 6  # allow 1 batch op per 6 high-priority ops when batch is waiting

    # Per-op CPU sizing knobs
    s.max_cpu_per_op = 4.0  # prevent a single op from consuming an entire pool
    s.min_cpu_per_op = 1.0  # don't schedule fractional CPUs to keep behavior stable

    # RAM sizing knobs
    s.default_mem_floor_gb = 1.0  # if no estimate, start with small safe floor
    s.max_mem_boost = 8.0         # cap runaway boost


def _priority_rank(priority):
    # Smaller rank = higher priority (scheduled earlier).
    if priority == Priority.QUERY:
        return 0
    if priority == Priority.INTERACTIVE:
        return 1
    return 2  # Priority.BATCH_PIPELINE and anything else


def _enqueue_pipeline(s, p):
    if p.priority == Priority.QUERY:
        s.waiting_q.append(p)
    elif p.priority == Priority.INTERACTIVE:
        s.waiting_i.append(p)
    else:
        s.waiting_b.append(p)


def _pop_next_pipeline(s):
    """Pick next pipeline to consider, with simple batch fairness."""
    batch_waiting = len(s.waiting_b) > 0
    hp_waiting = (len(s.waiting_q) + len(s.waiting_i)) > 0

    # If batch is waiting and we've scheduled enough high-priority ops, give batch a turn.
    if batch_waiting and (not hp_waiting or s.hp_assignments_since_batch >= s.batch_every):
        s.hp_assignments_since_batch = 0
        return s.waiting_b.pop(0)

    # Otherwise, strict priority ordering.
    if s.waiting_q:
        s.hp_assignments_since_batch += 1
        return s.waiting_q.pop(0)
    if s.waiting_i:
        s.hp_assignments_since_batch += 1
        return s.waiting_i.pop(0)
    if s.waiting_b:
        s.hp_assignments_since_batch = 0
        return s.waiting_b.pop(0)

    return None


def _requeue_pipeline(s, p):
    # Keep FIFO by re-enqueueing at tail of its priority queue.
    _enqueue_pipeline(s, p)


def _pipeline_done_or_failed(p):
    status = p.runtime_status()
    # If pipeline completed successfully, drop.
    if status.is_pipeline_successful():
        return True
    # If any failures exist, we still keep the pipeline (to allow retries / other ops),
    # but we should avoid infinite loops by letting ASSIGNABLE_STATES include FAILED
    # if the simulator allows retry. We'll not drop here.
    return False


def _maybe_update_boost_from_results(s, results):
    """Increase RAM boost for pipelines that experienced failures.

    We conservatively treat any failure as needing more RAM next time.
    """
    for r in results:
        if r is None:
            continue
        if hasattr(r, "failed") and r.failed():
            # Increase boost for this pipeline to reduce repeated OOMs.
            # If error string hints at OOM, boost more aggressively.
            boost = s.pipeline_ram_boost.get(r.ops[0].pipeline_id if hasattr(r.ops[0], "pipeline_id") else None, None)

            # If we can't find pipeline_id from op, fall back to searching is too expensive; skip.
            # Most Eudoxia operator objects are expected to carry pipeline_id in practice.
            if boost is None:
                # Try using result metadata if present (not guaranteed by spec).
                pid = getattr(r, "pipeline_id", None)
                if pid is None:
                    continue
                boost = s.pipeline_ram_boost.get(pid, 1.0)
            else:
                pid = r.ops[0].pipeline_id

            err = str(getattr(r, "error", "") or "")
            if "oom" in err.lower() or "out of memory" in err.lower():
                new_boost = boost * 2.0
            else:
                new_boost = boost * 1.5

            if new_boost > s.max_mem_boost:
                new_boost = s.max_mem_boost
            s.pipeline_ram_boost[pid] = new_boost


def _op_mem_floor_gb(s, op, pipeline_id):
    est = None
    if hasattr(op, "estimate") and op.estimate is not None:
        est = getattr(op.estimate, "mem_peak_gb", None)
    floor = est if (est is not None and est > 0) else s.default_mem_floor_gb
    boost = s.pipeline_ram_boost.get(pipeline_id, 1.0)
    return max(s.default_mem_floor_gb, floor * boost)


def _select_assignable_op(p):
    status = p.runtime_status()
    # Only schedule ops whose parents are complete.
    ops = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
    if not ops:
        return None
    # Keep it simple: schedule one op at a time per pipeline to avoid wide fan-out.
    return ops[0]


@register_scheduler(key="scheduler_est_012")
def scheduler_est_012_scheduler(s, results, pipelines):
    """
    Priority-aware scheduler that:
      - Enqueues new pipelines by priority.
      - Uses estimate-based RAM floors + failure-driven boost to reduce OOMs.
      - Caps CPU per op to improve interactive latency under contention.
      - Fills pools with multiple assignments when possible (instead of 1 op per pool per tick).
    """
    # Integrate new pipelines.
    for p in pipelines:
        _enqueue_pipeline(s, p)

    # React to last tick's results (OOM/failure -> boost RAM for that pipeline).
    if results:
        _maybe_update_boost_from_results(s, results)

    # Early exit if nothing to do.
    if not pipelines and not results and not (s.waiting_q or s.waiting_i or s.waiting_b):
        return [], []

    suspensions = []
    assignments = []

    # Attempt to fill each pool with as many ops as we can, while honoring priority.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)

        # If no resources, skip pool.
        if avail_cpu < s.min_cpu_per_op or avail_ram <= 0:
            continue

        # We'll loop while we can still place work.
        # To avoid infinite loops (e.g., RAM too small for top jobs), track attempts.
        max_attempts = (len(s.waiting_q) + len(s.waiting_i) + len(s.waiting_b)) + 8
        attempts = 0

        while avail_cpu >= s.min_cpu_per_op and avail_ram > 0 and attempts < max_attempts:
            attempts += 1
            p = _pop_next_pipeline(s)
            if p is None:
                break

            # Drop completed pipelines.
            if _pipeline_done_or_failed(p):
                continue

            op = _select_assignable_op(p)
            if op is None:
                # Not ready yet; requeue and continue.
                _requeue_pipeline(s, p)
                continue

            # Determine RAM floor from estimator, with boost on failures.
            mem_floor = _op_mem_floor_gb(s, op, p.pipeline_id)
            if avail_ram < mem_floor:
                # Can't place now in this pool; requeue for later/potentially other pool.
                _requeue_pipeline(s, p)
                continue

            # CPU sizing: cap per-op CPU to reduce monopolization; at least min_cpu_per_op.
            cpu = min(avail_cpu, s.max_cpu_per_op)
            if cpu < s.min_cpu_per_op:
                _requeue_pipeline(s, p)
                continue

            # RAM sizing: allocate the floor; optionally allocate a bit more if abundant.
            # Keep simple: give up to 25% extra headroom when available.
            ram = mem_floor
            extra = min(avail_ram - ram, 0.25 * mem_floor)
            if extra > 0:
                ram += extra

            assignment = Assignment(
                ops=[op],
                cpu=cpu,
                ram=ram,
                priority=p.priority,
                pool_id=pool_id,
                pipeline_id=p.pipeline_id,
            )
            assignments.append(assignment)

            # Update remaining resources and requeue pipeline for subsequent ops.
            avail_cpu -= cpu
            avail_ram -= ram
            _requeue_pipeline(s, p)

    return suspensions, assignments
