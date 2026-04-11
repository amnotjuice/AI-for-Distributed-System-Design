# policy_key: scheduler_est_049
# reasoning_effort: low
# model: gpt-5.2-2025-12-11
# llm_cost: 0.038240
# generation_seconds: 34.13
# generated_at: 2026-04-01T00:17:20.456805
@register_scheduler_init(key="scheduler_est_049")
def scheduler_est_049_init(s):
    """
    Priority-aware, estimate-guided scheduler with small, safe improvements over naive FIFO:
      - Separate waiting queues by priority; always try to serve higher priorities first.
      - Use op.estimate.mem_peak_gb as a RAM floor; add a small safety margin.
      - On failure (heuristically OOM), increase the per-op RAM floor and retry (bounded).
      - Mild anti-starvation: periodically allow a batch dispatch even under sustained high-priority load.
    """
    s.waiting_by_prio = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }
    # Per-operator learned RAM floors (GB). Keyed by (pipeline_id, op_key).
    s.op_ram_floor_gb = {}
    # Retry counters per operator.
    s.op_retry_count = {}
    # Simple dispatch accounting for anti-starvation.
    s.dispatch_counter = 0
    s.batch_every_n = 6  # after N high-priority dispatches, allow one batch if waiting

    # Heuristic knobs
    s.mem_safety_margin = 1.15  # allocate 15% above estimate/learned floor
    s.mem_min_gb = 0.25         # never allocate less than this if we can
    s.max_retries = 3           # per operator; beyond this we stop retrying that op


def _op_key(op):
    # Best-effort stable key for an operator within a pipeline.
    for attr in ("op_id", "operator_id", "id", "name"):
        if hasattr(op, attr):
            try:
                v = getattr(op, attr)
                # Some "id" fields can be methods
                if callable(v):
                    v = v()
                if v is not None:
                    return v
            except Exception:
                pass
    return id(op)


def _is_likely_oom(error_obj):
    # Conservative heuristic: only classify as OOM if the error string suggests it.
    if error_obj is None:
        return False
    try:
        msg = str(error_obj).lower()
    except Exception:
        return False
    return ("oom" in msg) or ("out of memory" in msg) or ("cuda out of memory" in msg) or ("memoryerror" in msg)


def _prio_rank(priority):
    # Lower is higher priority.
    if priority == Priority.QUERY:
        return 0
    if priority == Priority.INTERACTIVE:
        return 1
    return 2


def _pick_next_priority(s):
    """
    Choose which priority queue to pop from next, with mild anti-starvation:
      - Prefer QUERY/INTERACTIVE.
      - If batch is waiting and we've dispatched many high-priority ops recently, allow a batch dispatch.
    """
    q_wait = len(s.waiting_by_prio[Priority.QUERY]) > 0
    i_wait = len(s.waiting_by_prio[Priority.INTERACTIVE]) > 0
    b_wait = len(s.waiting_by_prio[Priority.BATCH_PIPELINE]) > 0

    if not (q_wait or i_wait or b_wait):
        return None

    # Anti-starvation gate for batch
    if b_wait and (s.dispatch_counter % max(1, s.batch_every_n) == (s.batch_every_n - 1)):
        return Priority.BATCH_PIPELINE

    if q_wait:
        return Priority.QUERY
    if i_wait:
        return Priority.INTERACTIVE
    return Priority.BATCH_PIPELINE


def _compute_ram_gb(s, pipeline, op, avail_ram):
    # Base floor from estimate if present
    est = None
    try:
        est = getattr(getattr(op, "estimate", None), "mem_peak_gb", None)
    except Exception:
        est = None

    opk = (pipeline.pipeline_id, _op_key(op))
    learned_floor = s.op_ram_floor_gb.get(opk, None)

    floor = None
    if est is not None:
        floor = float(est)
    if learned_floor is not None:
        floor = float(learned_floor) if floor is None else max(float(floor), float(learned_floor))

    if floor is None:
        # If no estimate, be conservative but not huge: take a modest slice of pool RAM if possible.
        # This is still capped by avail_ram and pool constraints upstream.
        floor = max(s.mem_min_gb, min(2.0, float(avail_ram)))

    # Apply safety margin; extra RAM is "free" in the model, but still constrained by avail_ram.
    want = max(s.mem_min_gb, floor * s.mem_safety_margin)
    return min(float(avail_ram), float(want))


def _compute_cpu(pipeline_priority, pool, avail_cpu):
    # Simple CPU sizing based on priority:
    # - High priority tries to grab a strong slice to reduce latency.
    # - Batch uses smaller slices to improve sharing.
    max_cpu = float(pool.max_cpu_pool)
    avail = float(avail_cpu)

    if pipeline_priority == Priority.QUERY:
        target = max(1.0, 0.75 * max_cpu)
    elif pipeline_priority == Priority.INTERACTIVE:
        target = max(1.0, 0.50 * max_cpu)
    else:
        target = max(1.0, 0.33 * max_cpu)

    return min(avail, target)


@register_scheduler(key="scheduler_est_049")
def scheduler_est_049_scheduler(s, results, pipelines):
    """
    Priority-aware scheduler:
      - Enqueue new pipelines by priority.
      - Process results to learn from failures (especially OOM) and update per-op RAM floors.
      - For each pool, try to dispatch at most one ready operator per tick, choosing highest priority first.
      - Retry failed ops with increased RAM floor (bounded retries). Avoid dropping entire pipelines.
    """
    # Enqueue arrivals
    for p in pipelines:
        s.waiting_by_prio[p.priority].append(p)

    # Early exit if nothing new and no results to process
    if not pipelines and not results:
        return [], []

    # Learn from execution results (especially failures) to adjust RAM floors.
    for r in results:
        if not getattr(r, "failed", lambda: False)():
            continue

        # If this looks like an OOM, raise RAM floor for involved ops and allow retry.
        if _is_likely_oom(getattr(r, "error", None)):
            for op in getattr(r, "ops", []) or []:
                # We don't have pipeline_id directly from result in the contract; best-effort:
                # try to read it from the operator itself, otherwise skip learning.
                pipeline_id = getattr(op, "pipeline_id", None)
                if pipeline_id is None:
                    continue
                opk = (pipeline_id, _op_key(op))

                # Count retries
                s.op_retry_count[opk] = s.op_retry_count.get(opk, 0) + 1

                # If we already have a RAM amount that failed, bump above it.
                prev_floor = s.op_ram_floor_gb.get(opk, None)
                failed_ram = getattr(r, "ram", None)
                bump_base = float(failed_ram) if failed_ram is not None else (float(prev_floor) if prev_floor is not None else 0.0)

                # Bump aggressively enough to reduce repeated OOM churn.
                new_floor = max(float(prev_floor) if prev_floor is not None else 0.0, bump_base * 1.35, bump_base + 0.5)
                s.op_ram_floor_gb[opk] = new_floor

    suspensions = []
    assignments = []

    # For each pool, try to schedule one operator (simple, safe incremental improvement).
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool
        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # We may need to scan pipelines to find one with a ready operator.
        # Build a small list of candidates by priority to avoid starving and avoid expensive full scans.
        # We'll attempt a bounded number of pops and requeues per pool.
        max_attempts = 16
        attempts = 0

        while attempts < max_attempts:
            attempts += 1
            prio = _pick_next_priority(s)
            if prio is None:
                break

            if not s.waiting_by_prio[prio]:
                continue

            pipeline = s.waiting_by_prio[prio].pop(0)

            status = pipeline.runtime_status()
            if status.is_pipeline_successful():
                # Drop completed pipelines
                continue

            # Only schedule ops whose parents are complete
            op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
            if not op_list:
                # Not ready; requeue at end
                s.waiting_by_prio[prio].append(pipeline)
                continue

            op = op_list[0]

            # If op already exceeded retries, do not keep thrashing it; leave it unscheduled.
            opk = (pipeline.pipeline_id, _op_key(op))
            if s.op_retry_count.get(opk, 0) > s.max_retries:
                # Requeue pipeline (other ops might still be runnable depending on model),
                # but we avoid repeatedly hitting the same failing op every tick by pushing back.
                s.waiting_by_prio[prio].append(pipeline)
                continue

            # Size resources
            ram = _compute_ram_gb(s, pipeline, op, avail_ram)
            cpu = _compute_cpu(pipeline.priority, pool, avail_cpu)

            # If we can't meet even the minimal RAM, requeue and try another pipeline.
            if ram <= 0 or cpu <= 0:
                s.waiting_by_prio[prio].append(pipeline)
                continue

            assignment = Assignment(
                ops=[op],
                cpu=cpu,
                ram=ram,
                priority=pipeline.priority,
                pool_id=pool_id,
                pipeline_id=pipeline.pipeline_id,
            )
            assignments.append(assignment)

            # Requeue pipeline for future operators (or retries)
            s.waiting_by_prio[prio].append(pipeline)

            s.dispatch_counter += 1
            break

    return suspensions, assignments
