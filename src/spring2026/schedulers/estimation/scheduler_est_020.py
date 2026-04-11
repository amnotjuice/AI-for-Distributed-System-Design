# policy_key: scheduler_est_020
# reasoning_effort: low
# model: gpt-5.2-2025-12-11
# llm_cost: 0.040186
# generation_seconds: 34.20
# generated_at: 2026-03-31T23:58:20.396014
@register_scheduler_init(key="scheduler_est_020")
def scheduler_est_020_init(s):
    """
    Priority-aware, estimation-aware scheduler (small, safe improvements over naive FIFO):

    Improvements over the example FIFO:
    1) Priority queues: always try QUERY -> INTERACTIVE -> BATCH_PIPELINE first.
    2) Better packing: schedule multiple ops per pool per tick (instead of at most one),
       using a per-op CPU cap so we don't give the entire pool to a single op.
    3) Memory floor: allocate RAM using op.estimate.mem_peak_gb when available.
    4) Simple OOM backoff: on failures, increase the remembered RAM floor for that op
       and retry (without immediately dropping the whole pipeline).

    Notes:
    - No preemption yet (kept intentionally simple to avoid churn/bugs as a first step).
    - Extra RAM is "free" in the model; CPU drives speed, so we cap CPU for concurrency/latency.
    """
    from collections import deque

    # Per-priority waiting queues of pipelines (pipelines can appear in multiple ticks; we rotate).
    s.q_by_pri = {
        Priority.QUERY: deque(),
        Priority.INTERACTIVE: deque(),
        Priority.BATCH_PIPELINE: deque(),
    }

    # Remembered RAM floor per operator identity (id(op) -> floor_gb).
    s.op_ram_floor_gb = {}

    # Remembered failure counts per operator identity; used to ramp RAM more aggressively.
    s.op_fail_count = {}

    # Round-robin pointer per priority (implicit via deque rotation).
    # CPU caps (fractions of pool max) to avoid a single op monopolizing the pool.
    s.cpu_cap_frac = {
        Priority.QUERY: 0.5,
        Priority.INTERACTIVE: 0.75,
        Priority.BATCH_PIPELINE: 1.0,
    }

    # Minimum CPU to allocate to an op when possible.
    s.min_cpu_per_op = 1.0

    # Safety: don't allocate less than this much RAM if the pool has it.
    s.min_ram_per_op_gb = 0.5

    # If an op fails, multiply last RAM by this factor (bounded by pool available RAM).
    s.fail_ram_multiplier = 2.0


def _est_ram_floor_gb(s, op):
    """Compute a RAM floor for op: max(estimate, historical floor, global minimum)."""
    est = None
    try:
        est = getattr(getattr(op, "estimate", None), "mem_peak_gb", None)
    except Exception:
        est = None

    hist = s.op_ram_floor_gb.get(id(op), None)
    floor = s.min_ram_per_op_gb
    if est is not None:
        try:
            floor = max(floor, float(est))
        except Exception:
            pass
    if hist is not None:
        floor = max(floor, float(hist))
    return floor


def _bump_ram_on_failure(s, op, last_ram_gb, pool_max_ram_gb):
    """Increase remembered RAM floor for an op after a failure (assume likely OOM)."""
    op_key = id(op)
    s.op_fail_count[op_key] = s.op_fail_count.get(op_key, 0) + 1

    # Ramp more if repeated failures.
    mult = s.fail_ram_multiplier * (1.25 ** max(0, s.op_fail_count[op_key] - 1))
    new_floor = max(_est_ram_floor_gb(s, op), float(last_ram_gb) * mult)

    # Cap to pool max; if still fails at max, it will keep retrying but won't exceed pool.
    new_floor = min(new_floor, float(pool_max_ram_gb))
    s.op_ram_floor_gb[op_key] = new_floor


def _enqueue_pipeline(s, pipeline):
    """Enqueue pipeline into its priority queue."""
    q = s.q_by_pri.get(pipeline.priority)
    if q is None:
        # Fallback: treat unknown priority as batch.
        q = s.q_by_pri[Priority.BATCH_PIPELINE]
    q.append(pipeline)


def _pop_next_runnable_pipeline(s, require_parents_complete=True):
    """
    Pick next pipeline (highest priority first) that has at least one assignable op.
    We rotate within each priority to get simple fairness.
    """
    for pri in (Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE):
        q = s.q_by_pri[pri]
        if not q:
            continue

        # Try up to len(q) pipelines to find one with runnable ops.
        for _ in range(len(q)):
            p = q.popleft()
            st = p.runtime_status()

            # Drop completed pipelines.
            if st.is_pipeline_successful():
                continue

            # NOTE: Unlike the naive example, we do NOT drop pipelines just because they
            # have failures; we rely on RAM backoff and retry.

            ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=require_parents_complete)
            if ops:
                return p, ops[0]
            # Not runnable now; put back for later (parents not complete, etc.).
            q.append(p)

    return None, None


@register_scheduler(key="scheduler_est_020")
def scheduler_est_020(s, results, pipelines):
    """
    Priority-aware + estimation-aware scheduling.

    High-level:
    - Ingest new pipelines into per-priority queues.
    - Process results to learn from failures (bump remembered RAM floors).
    - For each pool, schedule as many ops as fit:
        * pick highest-priority runnable op (round-robin within priority),
        * allocate RAM >= estimated floor,
        * allocate CPU with a per-op cap to improve concurrency/latency,
        * enqueue pipeline back for its next operators.
    """
    # Ingest new pipelines
    for p in pipelines:
        _enqueue_pipeline(s, p)

    # Early exit if no state changes that affect decisions
    if not pipelines and not results:
        return [], []

    # Learn from failures (simple: assume failure is likely OOM and increase RAM floor)
    for r in results:
        try:
            if r.failed():
                # r.ops is a list; we scheduled one op at a time, but be defensive.
                for op in getattr(r, "ops", []) or []:
                    pool = s.executor.pools[r.pool_id]
                    _bump_ram_on_failure(s, op, last_ram_gb=r.ram, pool_max_ram_gb=pool.max_ram_pool)
        except Exception:
            # Never let learning logic break scheduling.
            pass

    suspensions = []
    assignments = []

    # Schedule per pool
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)
        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # Keep scheduling while we have enough resources for a minimally-sized op.
        made_progress = True
        while made_progress:
            made_progress = False

            # Find next runnable op by priority.
            pipeline, op = _pop_next_runnable_pipeline(s, require_parents_complete=True)
            if pipeline is None or op is None:
                break

            # Compute RAM allocation: at least the estimated/historical floor.
            ram_floor = _est_ram_floor_gb(s, op)

            # If we can't satisfy RAM floor in this pool, requeue and stop trying this pool.
            if avail_ram < ram_floor:
                _enqueue_pipeline(s, pipeline)
                break

            # CPU allocation: cap to avoid starving other ops; ensure at least min CPU if possible.
            cap_frac = s.cpu_cap_frac.get(pipeline.priority, 1.0)
            cpu_cap = max(s.min_cpu_per_op, float(pool.max_cpu_pool) * float(cap_frac))

            cpu = min(avail_cpu, cpu_cap)
            if cpu < s.min_cpu_per_op and avail_cpu >= s.min_cpu_per_op:
                cpu = s.min_cpu_per_op

            # If we truly cannot allocate any CPU, put back and stop.
            if cpu <= 0:
                _enqueue_pipeline(s, pipeline)
                break

            # Allocate RAM: give the floor, but if there's abundant RAM, give a bit more to reduce OOM risk.
            # Extra RAM doesn't speed up, but can reduce retries when estimate is low.
            # Keep it modest so we can pack multiple ops if needed.
            headroom = max(0.0, avail_ram - ram_floor)
            ram = ram_floor + min(headroom, max(0.0, 0.25 * ram_floor))  # up to +25%

            # Build assignment for a single op
            assignment = Assignment(
                ops=[op],
                cpu=cpu,
                ram=ram,
                priority=pipeline.priority,
                pool_id=pool_id,
                pipeline_id=pipeline.pipeline_id,
            )
            assignments.append(assignment)

            # Update available resources in this pool for further packing this tick.
            avail_cpu -= float(cpu)
            avail_ram -= float(ram)
            made_progress = True

            # Requeue pipeline so its subsequent ops can be considered in later iterations/ticks.
            _enqueue_pipeline(s, pipeline)

            # Stop if remaining resources are too low to schedule another meaningful op.
            if avail_cpu < s.min_cpu_per_op or avail_ram < s.min_ram_per_op_gb:
                break

    return suspensions, assignments
