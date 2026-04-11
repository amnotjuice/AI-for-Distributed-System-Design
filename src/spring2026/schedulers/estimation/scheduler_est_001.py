# policy_key: scheduler_est_001
# reasoning_effort: low
# model: gpt-5.2-2025-12-11
# llm_cost: 0.000000
# generation_seconds: 40.35
# generated_at: 2026-03-31T23:45:54.332426
@register_scheduler_init(key="scheduler_est_001")
def scheduler_est_001_init(s):
    """
    Priority-aware FIFO with conservative right-sizing.

    Improvements over naive FIFO:
    - Maintain separate waiting queues per priority and always schedule higher priority first.
    - Right-size per-assignment CPU/RAM instead of greedily taking the entire pool.
    - Use op.estimate.mem_peak_gb (when available) as a RAM floor to reduce OOM risk.
    - Basic retry-on-failure with exponential RAM backoff (bounded) to learn unknown memory.
    - Simple anti-starvation: occasionally let batch run even under sustained interactive load.

    Notes:
    - No preemption yet (kept intentionally simple and robust as a first step).
    """
    s.waiting_queues = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }
    # Per-pipeline adaptive memory boost for retries after failures (OOM or otherwise).
    # Key: pipeline_id -> {"mem_boost": float, "retries": int}
    s.pipeline_meta = {}
    s.max_retries_per_pipeline = 3
    s.max_mem_boost = 8.0

    # Anti-starvation: after N high-priority dispatches, allow one batch dispatch if waiting.
    s.high_pri_budget = 8
    s.high_pri_used = 0

    # Track pipelines we decided to drop (exceeded retries or irrecoverable).
    s.dropped_pipeline_ids = set()


def _get_or_init_meta(s, pipeline_id):
    meta = s.pipeline_meta.get(pipeline_id)
    if meta is None:
        meta = {"mem_boost": 1.0, "retries": 0}
        s.pipeline_meta[pipeline_id] = meta
    return meta


def _enqueue_pipeline(s, pipeline):
    if pipeline.pipeline_id in s.dropped_pipeline_ids:
        return
    q = s.waiting_queues.get(pipeline.priority)
    if q is None:
        # Fallback: unknown priority treated as batch-like
        q = s.waiting_queues[Priority.BATCH_PIPELINE]
    q.append(pipeline)


def _pipeline_done_or_drop(s, pipeline):
    if pipeline.pipeline_id in s.dropped_pipeline_ids:
        return True
    status = pipeline.runtime_status()
    if status.is_pipeline_successful():
        return True
    return False


def _select_next_pipeline(s):
    """
    Choose the next pipeline to attempt scheduling.
    - Prefer QUERY > INTERACTIVE > BATCH, but allow occasional BATCH to avoid starvation.
    """
    q_query = s.waiting_queues[Priority.QUERY]
    q_inter = s.waiting_queues[Priority.INTERACTIVE]
    q_batch = s.waiting_queues[Priority.BATCH_PIPELINE]

    # If we've spent budget on high-priority, try to let one batch through.
    if s.high_pri_used >= s.high_pri_budget and q_batch:
        s.high_pri_used = 0
        return q_batch.pop(0)

    if q_query:
        s.high_pri_used += 1
        return q_query.pop(0)
    if q_inter:
        s.high_pri_used += 1
        return q_inter.pop(0)
    if q_batch:
        # Batch doesn't count toward high-priority budget
        return q_batch.pop(0)

    return None


def _op_mem_floor_gb(op):
    est = getattr(getattr(op, "estimate", None), "mem_peak_gb", None)
    if est is None:
        return None
    try:
        est = float(est)
    except Exception:
        return None
    if est <= 0:
        return None
    return est


def _default_mem_floor_by_priority(priority):
    # Conservative small defaults when no estimate is available.
    if priority == Priority.QUERY:
        return 2.0
    if priority == Priority.INTERACTIVE:
        return 3.0
    return 1.5  # batch


def _target_cpu_by_priority(priority, pool_max_cpu):
    # Keep simple: give higher priority more CPU to reduce latency,
    # but avoid monopolizing the entire pool to enable concurrency.
    pool_max_cpu = max(1.0, float(pool_max_cpu))
    if priority == Priority.QUERY:
        return min(4.0, max(1.0, 0.50 * pool_max_cpu))
    if priority == Priority.INTERACTIVE:
        return min(4.0, max(1.0, 0.45 * pool_max_cpu))
    return min(2.0, max(1.0, 0.25 * pool_max_cpu))


def _target_ram_gb(priority, pool_max_ram, mem_floor_gb):
    # Extra RAM beyond peak is "free" for performance; we allocate some buffer above floor.
    pool_max_ram = max(0.0, float(pool_max_ram))
    floor = mem_floor_gb if mem_floor_gb is not None else _default_mem_floor_by_priority(priority)

    # Small buffer factor, slightly larger for interactive/query to reduce retry likelihood.
    if priority in (Priority.QUERY, Priority.INTERACTIVE):
        buf_factor = 1.25
        min_cap = 4.0
    else:
        buf_factor = 1.15
        min_cap = 2.0

    target = max(min_cap, floor * buf_factor)
    return min(target, pool_max_ram)


@register_scheduler(key="scheduler_est_001")
def scheduler_est_001_scheduler(s, results, pipelines):
    """
    Priority-aware scheduler:
    - Enqueue new pipelines by priority.
    - Process recent results to adapt per-pipeline memory boost on failures.
    - For each pool, assign as many ready ops as resources allow, using right-sized CPU/RAM.
    """
    # Enqueue new arrivals
    for p in pipelines:
        _get_or_init_meta(s, p.pipeline_id)
        _enqueue_pipeline(s, p)

    # React to results: if anything failed, increase mem_boost for that pipeline and allow retry
    for r in results:
        if r is None:
            continue
        if getattr(r, "failed", None) and r.failed():
            # We can only reliably identify pipeline_id via r.ops[0].pipeline_id? Not guaranteed.
            # So we fall back to matching by r.priority only if needed; but prefer ops->pipeline.
            pipeline_id = None
            try:
                if r.ops and hasattr(r.ops[0], "pipeline_id"):
                    pipeline_id = r.ops[0].pipeline_id
            except Exception:
                pipeline_id = None

            # If we can't identify pipeline, we can't adapt precisely; skip adaptation.
            if pipeline_id is None:
                continue

            meta = _get_or_init_meta(s, pipeline_id)
            meta["retries"] += 1
            # Exponential backoff for RAM on retry; cap it.
            meta["mem_boost"] = min(s.max_mem_boost, meta["mem_boost"] * 2.0)

            # If exceeded retries, drop it to avoid infinite churn.
            if meta["retries"] > s.max_retries_per_pipeline:
                s.dropped_pipeline_ids.add(pipeline_id)

    # Early exit if nothing changed
    if not pipelines and not results:
        return [], []

    suspensions = []  # intentionally empty: no preemption in this iteration
    assignments = []

    # Attempt to schedule across pools
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]

        # Refresh available resources each loop (the simulator will apply assignments after return,
        # but within this tick we must decrement locally to avoid over-allocating).
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)

        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # Greedily pack multiple ops per pool using right-sized containers.
        # Stop when we can't find any assignable op that fits.
        made_progress = True
        while made_progress and avail_cpu > 0 and avail_ram > 0:
            made_progress = False

            pipeline = _select_next_pipeline(s)
            if pipeline is None:
                break

            # Drop completed pipelines, and do not reschedule dropped ones
            if _pipeline_done_or_drop(s, pipeline):
                continue

            if pipeline.pipeline_id in s.dropped_pipeline_ids:
                continue

            status = pipeline.runtime_status()

            # If pipeline already has failures, we still retry FAILED ops (ASSIGNABLE_STATES includes FAILED).
            # If some other non-retriable condition exists, we simply requeue and move on.
            op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
            if not op_list:
                # Not ready yet; requeue and try another pipeline
                _enqueue_pipeline(s, pipeline)
                continue

            op = op_list[0]
            meta = _get_or_init_meta(s, pipeline.pipeline_id)

            # Compute RAM floor using estimator (if provided) plus per-pipeline boost.
            mem_floor = _op_mem_floor_gb(op)
            if mem_floor is None:
                mem_floor = _default_mem_floor_by_priority(pipeline.priority)

            mem_floor *= float(meta["mem_boost"])

            # Size container for latency (CPU) and OOM avoidance (RAM)
            cpu_target = _target_cpu_by_priority(pipeline.priority, pool.max_cpu_pool)
            ram_target = _target_ram_gb(pipeline.priority, pool.max_ram_pool, mem_floor)

            # Clip to available resources
            cpu = min(avail_cpu, cpu_target)
            ram = min(avail_ram, ram_target)

            # If we can't fit even the floor, requeue and stop trying in this pool (likely full).
            if cpu < 1e-9 or ram < mem_floor:
                _enqueue_pipeline(s, pipeline)
                break

            assignments.append(
                Assignment(
                    ops=op_list,
                    cpu=cpu,
                    ram=ram,
                    priority=pipeline.priority,
                    pool_id=pool_id,
                    pipeline_id=pipeline.pipeline_id,
                )
            )

            # Decrement local availability and requeue pipeline for subsequent ops
            avail_cpu -= cpu
            avail_ram -= ram
            _enqueue_pipeline(s, pipeline)
            made_progress = True

    return suspensions, assignments
