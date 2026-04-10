# policy_key: scheduler_est_031
# reasoning_effort: low
# exp: estimation
# model: gpt-5.2-2025-12-11
# llm_cost: 0.050554
# generation_seconds: 53.18
# generated_at: 2026-04-10T10:10:25.735517
@register_scheduler_init(key="scheduler_est_031")
def scheduler_est_031_init(s):
    """
    Memory-aware, priority-first scheduler with simple OOM backoff.

    Core ideas:
      - Maintain per-priority FIFO queues of pipelines; always prefer QUERY > INTERACTIVE > BATCH.
      - Within a priority, prefer assigning the smallest (estimated) memory operator that fits a pool
        to reduce OOM risk and improve packing.
      - Use estimator hint (op.estimate.mem_peak_gb) with a safety factor; on failures, increase
        per-operator RAM boost (exponential backoff) to improve completion rate.
      - Avoid starvation via lightweight aging: if batch has waited long enough, allow it to run
        when it can fit.
    """
    s.queues = {}  # priority -> List[Pipeline]
    s.tick = 0

    # Per-operator RAM boost multiplier (keyed by operator identity).
    s.op_ram_boost = {}  # op_key -> float

    # Aging / anti-starvation
    s.pipeline_age = {}  # pipeline_id -> int ticks in system

    # Tuning knobs
    s.mem_safety = 1.25          # estimator is noisy; request a bit more than estimate
    s.boost_on_fail = 2.0        # exponential RAM backoff on failure
    s.boost_max = 8.0            # cap to avoid runaway reservations
    s.boost_decay = 0.9          # gently decay boost after successes
    s.batch_age_threshold = 12   # allow batch to run if it has waited this many ticks

    # CPU caps as a fraction of pool capacity (avoid giving entire pool to one op by default).
    s.cpu_frac = {
        Priority.QUERY: 0.75,
        Priority.INTERACTIVE: 0.60,
        Priority.BATCH_PIPELINE: 0.50,
    }

    # Minimum CPU to request when any is available.
    s.min_cpu = 1.0


def _pkey(priority):
    # Lower is better (more urgent).
    if priority == Priority.QUERY:
        return 0
    if priority == Priority.INTERACTIVE:
        return 1
    return 2


def _get_op_key(op):
    # Try a stable identifier if present; otherwise fall back to object identity.
    return getattr(op, "op_id", None) or getattr(op, "operator_id", None) or id(op)


def _get_est_mem(op):
    est = getattr(op, "estimate", None)
    if est is None:
        return None
    return getattr(est, "mem_peak_gb", None)


def _desired_ram_for_op(s, op, pool, avail_ram):
    """
    Compute RAM request for op on this pool, using estimator + safety + backoff boost.
    Returns (ram_request, est_mem_used_for_decision).
    """
    est_mem = _get_est_mem(op)  # may be None
    boost = s.op_ram_boost.get(_get_op_key(op), 1.0)

    # If we have an estimate, request estimate * safety * boost, bounded by pool limits.
    if est_mem is not None:
        req = est_mem * s.mem_safety * boost
        # Request at least a small positive amount if pool has headroom.
        req = max(0.0, req)
        # Don't request more than what's available right now.
        req = min(req, avail_ram)
        return req, est_mem

    # No estimate: be conservative but try to make progress.
    # Request a moderate fraction of available RAM (boost helps after failures).
    req = avail_ram * min(0.70 * boost, 1.0)
    req = max(0.0, req)
    req = min(req, avail_ram)
    return req, None


def _desired_cpu_for_priority(s, priority, pool, avail_cpu):
    cap = pool.max_cpu_pool * s.cpu_frac.get(priority, 0.5)
    cpu = min(avail_cpu, cap)
    if cpu <= 0:
        return 0
    return max(s.min_cpu, cpu)


def _enqueue_pipeline(s, p):
    q = s.queues.setdefault(p.priority, [])
    q.append(p)
    s.pipeline_age.setdefault(p.pipeline_id, 0)


def _cleanup_and_requeue(s, pipeline, requeue_map):
    status = pipeline.runtime_status()
    has_failures = status.state_counts[OperatorState.FAILED] > 0
    if status.is_pipeline_successful() or has_failures:
        # If pipeline is done or has failures (we don't drop it immediately here),
        # we still allow FAILED ops to be rescheduled by keeping it around.
        # The simulator's ASSIGNABLE_STATES includes FAILED; so keep unless terminal.
        # However, if a pipeline is "failed" terminally in this sim, status will reflect that.
        pass

    # If pipeline isn't completed, keep it queued for future ticks.
    if not status.is_pipeline_successful():
        requeue_map.setdefault(pipeline.priority, []).append(pipeline)
        s.pipeline_age[pipeline.pipeline_id] = s.pipeline_age.get(pipeline.pipeline_id, 0) + 1


def _pick_best_candidate_op(s, pool, avail_cpu, avail_ram, considered_pipelines):
    """
    Choose (pipeline, op, cpu_req, ram_req) for this pool, or None if nothing fits.

    Strategy:
      - Prefer higher priority.
      - Within priority, prefer older pipelines slightly (aging),
        but still prefer small estimated memory ops to reduce OOM/fragmentation.
      - Use estimator to pre-filter obvious non-fits when possible.
    """
    best = None
    best_sort_key = None

    for pipeline in considered_pipelines:
        status = pipeline.runtime_status()
        if status.is_pipeline_successful():
            continue

        # Only schedule ops whose parents are complete.
        ops = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        if not ops:
            continue

        # Consider just the "best" op in this pipeline for now: smallest estimated memory.
        def op_mem_key(op):
            em = _get_est_mem(op)
            return (em is None, em if em is not None else 0.0)

        op = min(ops, key=op_mem_key)
        cpu_req = _desired_cpu_for_priority(s, pipeline.priority, pool, avail_cpu)
        if cpu_req <= 0:
            continue

        ram_req, est_mem = _desired_ram_for_op(s, op, pool, avail_ram)
        if ram_req <= 0:
            continue

        # Estimator-based pre-filter (soft):
        # If estimate is present and clearly exceeds *available* RAM, skip to avoid likely OOM.
        # But if the pipeline has waited long (aging), allow a "hail mary" attempt.
        age = s.pipeline_age.get(pipeline.pipeline_id, 0)
        if est_mem is not None and (est_mem * s.mem_safety) > avail_ram:
            if not (pipeline.priority == Priority.BATCH_PIPELINE and age >= s.batch_age_threshold):
                continue

        # Sort key: (priority, starvation/age, estimated_mem, queue_age)
        # Lower is better. We subtract age to slightly favor older pipelines within same priority.
        p_rank = _pkey(pipeline.priority)
        est_for_sort = est_mem if est_mem is not None else (avail_ram * 0.9)  # unknown -> treat as larger
        sort_key = (p_rank, -min(age, 1000), est_for_sort)

        if best is None or sort_key < best_sort_key:
            best = (pipeline, op, cpu_req, ram_req)
            best_sort_key = sort_key

    return best


@register_scheduler(key="scheduler_est_031")
def scheduler_est_031_scheduler(s, results, pipelines):
    """
    Priority-first, memory-aware scheduling:
      - Enqueue arriving pipelines into per-priority queues.
      - Update RAM backoff multipliers based on failures/successes in `results`.
      - For each pool, repeatedly assign one operator at a time while resources remain,
        choosing the best-fitting candidate across queues.
      - Avoid OOMs by using estimator + safety factor; recover from OOM-like failures by
        increasing RAM request next time for the same operator.
    """
    s.tick += 1

    # Incorporate newly arrived pipelines.
    for p in pipelines:
        _enqueue_pipeline(s, p)

    # Update backoff multipliers from execution feedback.
    for r in results:
        if not getattr(r, "ops", None):
            continue

        # Update per-op boost on success/failure.
        if r.failed():
            for op in r.ops:
                k = _get_op_key(op)
                cur = s.op_ram_boost.get(k, 1.0)
                s.op_ram_boost[k] = min(s.boost_max, cur * s.boost_on_fail)
        else:
            for op in r.ops:
                k = _get_op_key(op)
                if k in s.op_ram_boost:
                    s.op_ram_boost[k] = max(1.0, s.op_ram_boost[k] * s.boost_decay)

    # Early exit if nothing to do.
    if not pipelines and not results and not any(s.queues.values()):
        return [], []

    suspensions = []
    assignments = []

    # Requeue map built fresh each tick to preserve order while dropping completed pipelines.
    requeue_map = {Priority.QUERY: [], Priority.INTERACTIVE: [], Priority.BATCH_PIPELINE: []}

    # Build a flat list of pipelines per priority (keep existing order).
    # We also refresh ages for pipelines that remain queued.
    all_pipelines_ordered = []
    for pr in [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]:
        q = s.queues.get(pr, [])
        for pl in q:
            all_pipelines_ordered.append(pl)

    # Schedule per pool: keep assigning until pool can't fit more.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]

        # We'll allow multiple assignments per pool in one tick, one op per assignment.
        # Stop when CPU or RAM is exhausted.
        while True:
            avail_cpu = pool.avail_cpu_pool
            avail_ram = pool.avail_ram_pool
            if avail_cpu <= 0 or avail_ram <= 0:
                break

            # Consider all queued pipelines; selection logic will enforce priority.
            pick = _pick_best_candidate_op(s, pool, avail_cpu, avail_ram, all_pipelines_ordered)
            if pick is None:
                break

            pipeline, op, cpu_req, ram_req = pick

            # If our computed request is tiny due to low avail_ram, skip to avoid nonsense.
            if cpu_req <= 0 or ram_req <= 0:
                break

            # Create assignment for this single operator.
            assignments.append(
                Assignment(
                    ops=[op],
                    cpu=cpu_req,
                    ram=ram_req,
                    priority=pipeline.priority,
                    pool_id=pool_id,
                    pipeline_id=pipeline.pipeline_id,
                )
            )

            # Optimistically update local view of available resources to avoid over-assigning
            # within this tick. The simulator will enforce true limits.
            # (We can't mutate pool.avail_* directly; so we adjust by rebuilding all_pipelines_ordered only.)
            # To prevent selecting the same op repeatedly within this tick, remove the pipeline
            # from consideration list for this pool iteration; it will be reconsidered next loop tick.
            all_pipelines_ordered = [pl for pl in all_pipelines_ordered if pl.pipeline_id != pipeline.pipeline_id]

            # If nothing remains, stop.
            if not all_pipelines_ordered:
                break

    # Requeue all pipelines that are not completed (and still potentially runnable).
    # Preserve original queue ordering by scanning existing queues.
    for pr in [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]:
        for pipeline in s.queues.get(pr, []):
            _cleanup_and_requeue(s, pipeline, requeue_map)

    s.queues[Priority.QUERY] = requeue_map[Priority.QUERY]
    s.queues[Priority.INTERACTIVE] = requeue_map[Priority.INTERACTIVE]
    s.queues[Priority.BATCH_PIPELINE] = requeue_map[Priority.BATCH_PIPELINE]

    return suspensions, assignments
