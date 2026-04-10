# policy_key: scheduler_est_040
# reasoning_effort: high
# model: gpt-5.2-2025-12-11
# llm_cost: 0.134714
# generation_seconds: 101.99
# generated_at: 2026-03-31T19:37:56.036897
@register_scheduler_init(key="scheduler_est_040")
def scheduler_est_040_init(s):
    """
    Priority-aware, estimate-sized, non-preemptive scheduler.

    Small improvements over naive FIFO:
      1) Priority queues: prefer QUERY > INTERACTIVE > BATCH.
      2) Right-size RAM to op.estimate.mem_peak_gb (with safety factor) instead of grabbing whole pool.
      3) Cap CPU per op to avoid a single op monopolizing a pool (enables concurrency).
      4) On OOM failures, increase per-op RAM safety factor and retry (instead of dropping the pipeline).
      5) Soft reservations: when high-priority work is waiting, avoid starting batch ops that would consume
         the last headroom on shared pools.

    Notes:
      - No preemption (insufficient public executor introspection in the provided interface).
      - Uses FAILED in ASSIGNABLE_STATES to allow retrying failed ops.
    """
    s.known_pipelines = {}  # pipeline_id -> Pipeline
    s.dead_pipelines = set()  # pipeline_ids that should never be retried (non-OOM failures or retry exceeded)

    # Per-priority FIFO lists of pipeline_ids (store ids to avoid holding duplicates)
    s.q_query = []
    s.q_interactive = []
    s.q_batch = []

    # Membership tracking to prevent duplicates in queues
    s.in_queue = set()  # pipeline_id

    # Per-op adaptive memory sizing based on past failures
    s.op_ram_factor = {}  # op_key -> float (multiplier on estimate)
    s.op_retries = {}  # op_key -> int
    s.op_to_pipeline = {}  # op_key -> pipeline_id

    # Config knobs (kept conservative to stay "small improvements" over naive)
    s.base_ram_factor = 1.25          # initial safety margin on top of estimate
    s.max_ram_factor = 6.0            # cap runaway growth
    s.min_ram_gb = 0.5                # never allocate less than this
    s.ram_additive_gb = 0.25          # small fixed overhead for runtime + estimation error
    s.max_retries_per_op = 4          # after this, mark pipeline as dead (avoids infinite thrash)

    # CPU capping: prevent single op from taking the entire pool (reduces latency under contention)
    s.cpu_caps = {
        Priority.QUERY: 8.0,
        Priority.INTERACTIVE: 4.0,
        Priority.BATCH_PIPELINE: 2.0,
    }
    s.min_cpu = 1.0

    # Soft reservation for high priority on shared pools (single-pool case; multi-pool handled by placement)
    s.reserve_frac_cpu = 0.25
    s.reserve_frac_ram = 0.25


def _op_key(op):
    # Prefer a stable identifier if present; fall back to object identity.
    for attr in ("op_id", "operator_id", "id", "name"):
        if hasattr(op, attr):
            v = getattr(op, attr)
            if v is not None:
                return f"{attr}:{v}"
    return f"pyid:{id(op)}"


def _is_oom_error(err):
    if err is None:
        return False
    try:
        msg = str(err).lower()
    except Exception:
        return False
    return ("oom" in msg) or ("out of memory" in msg) or ("out-of-memory" in msg) or ("memoryerror" in msg)


def _get_est_mem_gb(op):
    # Estimator contract: op.estimate.mem_peak_gb (may be missing)
    est = getattr(op, "estimate", None)
    if est is None:
        return None
    v = getattr(est, "mem_peak_gb", None)
    try:
        if v is None:
            return None
        v = float(v)
        if v <= 0:
            return None
        return v
    except Exception:
        return None


def _queue_for_priority(s, priority):
    if priority == Priority.QUERY:
        return s.q_query
    if priority == Priority.INTERACTIVE:
        return s.q_interactive
    return s.q_batch


def _enqueue_pipeline(s, p):
    pid = p.pipeline_id
    s.known_pipelines[pid] = p
    if pid in s.dead_pipelines:
        return
    if pid in s.in_queue:
        return
    _queue_for_priority(s, p.priority).append(pid)
    s.in_queue.add(pid)


def _remove_pipeline_everywhere(s, pipeline_id):
    s.in_queue.discard(pipeline_id)
    # Lazy removal from queues: filter occasionally is expensive; we remove opportunistically while scanning.
    # Still, we can drop the reference from known_pipelines to avoid stale growth.
    if pipeline_id in s.known_pipelines:
        del s.known_pipelines[pipeline_id]


def _high_prio_waiting(s):
    # Approximate: if any high-priority pipeline is enqueued (even if not immediately ready)
    return bool(s.q_query) or bool(s.q_interactive)


def _priority_order_for_pool(s, pool_id):
    # If multiple pools exist, prefer using pool 0 for high-priority work.
    # Other pools primarily serve batch, but will accept overflow high-priority if present.
    if s.executor.num_pools > 1 and pool_id == 0:
        return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]
    return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]


def _pool_accepts_batch_now(s, pool, pool_id):
    # If multi-pool: try to keep pool 0 mostly for high-priority; batch can use it only if no high-priority waiting.
    if s.executor.num_pools > 1 and pool_id == 0 and _high_prio_waiting(s):
        return False

    # If single-pool: when high-priority is waiting, keep some headroom (soft reservation).
    if s.executor.num_pools == 1 and _high_prio_waiting(s):
        reserve_cpu = s.reserve_frac_cpu * float(pool.max_cpu_pool)
        reserve_ram = s.reserve_frac_ram * float(pool.max_ram_pool)
        if float(pool.avail_cpu_pool) <= reserve_cpu or float(pool.avail_ram_pool) <= reserve_ram:
            return False
    return True


def _desired_cpu(s, pool, priority, avail_cpu):
    cap = float(s.cpu_caps.get(priority, 2.0))
    # Also cap by a fraction of pool max so tiny pools don't get over-allocated; big pools don't get monopolized.
    frac_cap = 0.5 * float(pool.max_cpu_pool) if priority == Priority.QUERY else 0.33 * float(pool.max_cpu_pool)
    cap = min(cap, max(s.min_cpu, frac_cap))
    return min(float(avail_cpu), max(s.min_cpu, cap))


def _desired_ram(s, pool, op, avail_ram):
    est = _get_est_mem_gb(op)
    opk = _op_key(op)
    factor = float(s.op_ram_factor.get(opk, s.base_ram_factor))
    if est is None:
        # Conservative fallback: allocate a moderate fraction of the pool, but avoid taking everything.
        # This still improves over naive "take all".
        req = max(s.min_ram_gb, min(0.5 * float(pool.max_ram_pool), float(avail_ram)))
    else:
        req = max(s.min_ram_gb, factor * float(est) + s.ram_additive_gb)

    # Never request more than the pool can ever provide.
    req = min(req, float(pool.max_ram_pool))
    return req


def _try_find_schedulable(s, pool, pool_id, priority, avail_cpu, avail_ram):
    """
    Scan the queue for a pipeline that has a parent-ready assignable op that fits in avail_cpu/avail_ram.
    Rotate pipelines to avoid head-of-line blocking.
    """
    q = _queue_for_priority(s, priority)
    if not q:
        return None

    # Batch admission gating to preserve headroom for high-priority work.
    if priority == Priority.BATCH_PIPELINE and not _pool_accepts_batch_now(s, pool, pool_id):
        return None

    qlen = len(q)
    for _ in range(qlen):
        pid = q.pop(0)

        # Skip duplicates/stale entries quickly
        if pid in s.dead_pipelines:
            s.in_queue.discard(pid)
            continue

        pipeline = s.known_pipelines.get(pid)
        if pipeline is None:
            s.in_queue.discard(pid)
            continue

        status = pipeline.runtime_status()
        if status.is_pipeline_successful():
            s.in_queue.discard(pid)
            _remove_pipeline_everywhere(s, pid)
            continue

        # Find one op that is assignable and whose parents are complete.
        op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
        if not op_list:
            # Not runnable yet; keep it in rotation
            q.append(pid)
            continue

        op = op_list[0]
        ram_need = _desired_ram(s, pool, op, avail_ram)
        cpu_need = _desired_cpu(s, pool, priority, avail_cpu)

        if cpu_need <= 0 or ram_need <= 0:
            q.append(pid)
            continue

        # Must fit current available resources
        if float(ram_need) <= float(avail_ram) and float(cpu_need) <= float(avail_cpu):
            # Keep pipeline in queue for subsequent ops (round-robin)
            q.append(pid)
            return pipeline, op, float(cpu_need), float(ram_need)

        # Doesn't fit right now; rotate
        q.append(pid)

    return None


@register_scheduler(key="scheduler_est_040")
def scheduler_est_040_scheduler(s, results, pipelines):
    """
    Main scheduling step.

    - Ingest new pipelines into per-priority FIFOs.
    - Process execution results to adapt RAM sizing:
        * OOM -> increase per-op RAM factor and retry
        * non-OOM failure -> mark pipeline dead (stop retrying)
        * success -> optionally decay factor a bit
    - For each pool, schedule as many runnable ops as possible, prioritizing high-priority queues.
    """
    # --- ingest arrivals ---
    for p in pipelines:
        _enqueue_pipeline(s, p)

    # --- process results (adapt sizing + kill on non-OOM) ---
    for r in results:
        # Results can contain multiple ops; update each.
        for op in getattr(r, "ops", []) or []:
            opk = _op_key(op)

            # Track which pipeline this op belonged to (best-effort)
            pid = s.op_to_pipeline.get(opk, None)

            if r.failed():
                s.op_retries[opk] = int(s.op_retries.get(opk, 0)) + 1

                if _is_oom_error(getattr(r, "error", None)):
                    # Increase RAM factor aggressively to avoid repeated OOM churn
                    cur = float(s.op_ram_factor.get(opk, s.base_ram_factor))
                    nxt = min(s.max_ram_factor, max(cur * 1.5, cur + 0.5))
                    s.op_ram_factor[opk] = nxt

                    # If we've retried too many times, stop the whole pipeline (prevents infinite loops)
                    if pid is not None and s.op_retries[opk] >= s.max_retries_per_op:
                        s.dead_pipelines.add(pid)
                else:
                    # Non-OOM failures are treated as non-retriable for the pipeline
                    if pid is not None:
                        s.dead_pipelines.add(pid)
            else:
                # Success: slight decay toward base factor (keeps learned factors from staying overly conservative forever)
                cur = float(s.op_ram_factor.get(opk, s.base_ram_factor))
                decayed = max(s.base_ram_factor, cur * 0.95)
                s.op_ram_factor[opk] = decayed
                s.op_retries[opk] = 0

    # If nothing changed and no arrivals, do nothing
    if not pipelines and not results:
        return [], []

    suspensions = []  # no preemption in this version
    assignments = []

    # --- scheduling / placement ---
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)

        # If no headroom, skip
        if avail_cpu < s.min_cpu or avail_ram <= 0:
            continue

        prio_order = _priority_order_for_pool(s, pool_id)

        # Fill the pool with as many ops as possible (1 op per assignment/container to match example expectations)
        made_progress = True
        while made_progress:
            made_progress = False

            for prio in prio_order:
                # For multi-pool, try to keep pool0 for high priority; batch handled by _pool_accepts_batch_now
                pick = _try_find_schedulable(s, pool, pool_id, prio, avail_cpu, avail_ram)
                if pick is None:
                    continue

                pipeline, op, cpu_need, ram_need = pick
                pid = pipeline.pipeline_id

                # If the pipeline is now dead, don't schedule it
                if pid in s.dead_pipelines:
                    continue

                # Final guardrails
                if cpu_need < s.min_cpu or ram_need <= 0:
                    continue
                if cpu_need > avail_cpu or ram_need > avail_ram:
                    continue

                # Record op->pipeline mapping for interpreting future results
                opk = _op_key(op)
                s.op_to_pipeline[opk] = pid

                assignment = Assignment(
                    ops=[op],
                    cpu=cpu_need,
                    ram=ram_need,
                    priority=pipeline.priority,
                    pool_id=pool_id,
                    pipeline_id=pid,
                )
                assignments.append(assignment)

                # Update local avail for packing multiple ops per pool
                avail_cpu -= cpu_need
                avail_ram -= ram_need

                made_progress = True
                break  # restart from highest priority each time

            if avail_cpu < s.min_cpu or avail_ram <= 0:
                break

    # Opportunistic cleanup: drop dead pipelines from known map (queues are lazily cleaned during scans)
    if s.dead_pipelines:
        for pid in list(s.dead_pipelines):
            # If it's still around, remove from known_pipelines to prevent growth.
            if pid in s.known_pipelines:
                del s.known_pipelines[pid]
            s.in_queue.discard(pid)

    return suspensions, assignments
