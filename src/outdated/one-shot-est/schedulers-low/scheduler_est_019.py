# policy_key: scheduler_est_019
# reasoning_effort: low
# model: gpt-5.2-2025-12-11
# llm_cost: 0.053808
# generation_seconds: 40.04
# generated_at: 2026-03-31T23:57:46.194576
@register_scheduler_init(key="scheduler_est_019")
def scheduler_est_019_init(s):
    """Priority-aware, estimate-informed scheduler (incremental upgrade over naive FIFO).

    Improvements (kept intentionally small/simple to stay robust):
    1) Separate queues per priority; always consider higher priority first to improve latency.
    2) Right-size CPU (cap per op) so a single assignment doesn't monopolize the whole pool.
    3) RAM sizing uses op.estimate.mem_peak_gb as a floor + safety factor; on failures, increase RAM multiplier.
    4) Basic "reservation": when scheduling batch, keep a small headroom to reduce interference with interactive/query.
    """
    s.tick = 0

    # Per-priority FIFO queues of pipelines
    s.queues = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }

    # Track when a pipeline was enqueued (for light aging / debugging)
    s.pipeline_enqueued_tick = {}  # pipeline_id -> tick

    # Track per-op failure counts and RAM backoff multipliers to reduce repeated OOMs
    s.op_fail_count = {}  # op_key -> int
    s.op_ram_mult = {}    # op_key -> float


def _op_key(op):
    # Use Python object identity as a stable in-sim key (no assumptions about op having an ID field).
    return id(op)


def _get_estimated_mem_gb(op):
    # estimator may be missing; handle gracefully
    est = None
    try:
        if getattr(op, "estimate", None) is not None:
            est = getattr(op.estimate, "mem_peak_gb", None)
    except Exception:
        est = None
    return est


def _pipeline_has_terminal_failure(status):
    # If an op fails repeatedly, we may eventually want to drop the pipeline.
    # Here we only drop if the status reports failures AND no retry is possible; since FAILED is assignable,
    # we don't treat a single failure as terminal.
    return False


def _pop_next_schedulable_pipeline(queue, require_parents_complete=True):
    """Pop left-most pipeline that has at least one assignable op; requeue others in original order."""
    requeue = []
    chosen = None
    chosen_status = None
    chosen_ops = None

    while queue:
        p = queue.pop(0)
        status = p.runtime_status()

        if status.is_pipeline_successful():
            continue

        if _pipeline_has_terminal_failure(status):
            continue

        ops = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=require_parents_complete)
        if ops:
            chosen = p
            chosen_status = status
            chosen_ops = ops
            break

        requeue.append(p)

    # Preserve order for pipelines we examined but couldn't schedule yet
    if requeue:
        queue[:0] = requeue

    return chosen, chosen_status, chosen_ops


@register_scheduler(key="scheduler_est_019")
def scheduler_est_019_scheduler(s, results, pipelines):
    """
    Priority-aware, estimate-informed scheduler.

    Core logic:
    - Enqueue pipelines by priority (QUERY > INTERACTIVE > BATCH).
    - Process results: on failures, increase RAM multiplier for involved ops to reduce repeat OOMs.
    - For each pool, greedily place as many single-op assignments as resources allow:
        * Higher priorities first.
        * CPU per op capped to improve concurrency and tail latency.
        * RAM uses estimate as a floor with safety factor; failures backoff RAM.
        * When scheduling BATCH, keep small CPU/RAM headroom for interactive/query arrivals.
    """
    s.tick += 1

    # Enqueue new pipelines by priority
    for p in pipelines:
        pr = p.priority
        if pr not in s.queues:
            # Default unknown priorities to batch-like behavior
            pr = Priority.BATCH_PIPELINE
        s.queues[pr].append(p)
        s.pipeline_enqueued_tick[p.pipeline_id] = s.tick

    # Update failure-driven RAM backoff based on results
    # Note: we don't assume we can distinguish OOM vs other failures reliably; we conservatively backoff RAM
    # but cap it to avoid runaway.
    if results:
        for r in results:
            if r is None:
                continue
            if getattr(r, "failed", None) is not None and r.failed():
                for op in getattr(r, "ops", []) or []:
                    k = _op_key(op)
                    s.op_fail_count[k] = s.op_fail_count.get(k, 0) + 1
                    prev = s.op_ram_mult.get(k, 1.0)
                    # Exponential backoff with a cap; tuned to be conservative but not extreme.
                    s.op_ram_mult[k] = min(8.0, max(prev, 1.0) * 2.0)

    # Early exit if nothing changed
    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    # Config knobs (small, obvious improvements; keep simple)
    priority_order = [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]

    # Per-op CPU caps to prevent a single op from grabbing the whole pool
    cpu_cap = {
        Priority.QUERY: 4.0,
        Priority.INTERACTIVE: 4.0,
        Priority.BATCH_PIPELINE: 2.0,
    }

    # For batch, keep headroom to reduce tail latency for higher priority work
    batch_reserve_cpu_frac = 0.20
    batch_reserve_ram_frac = 0.15

    # RAM sizing defaults
    default_mem_floor_gb = 1.0
    mem_safety_factor = 1.25

    # Greedily schedule per pool
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)

        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # Try to fill the pool with multiple small assignments
        # Stop when we can't place anything else without violating reservations/caps
        made_progress = True
        while made_progress:
            made_progress = False

            # Choose priority to schedule next, highest first
            chosen_pipeline = None
            chosen_priority = None
            chosen_op = None

            for pr in priority_order:
                q = s.queues.get(pr, [])
                if not q:
                    continue

                # For BATCH, apply reservation by reducing effective availability
                eff_cpu = avail_cpu
                eff_ram = avail_ram
                if pr == Priority.BATCH_PIPELINE:
                    eff_cpu = max(0.0, avail_cpu - batch_reserve_cpu_frac * float(pool.max_cpu_pool))
                    eff_ram = max(0.0, avail_ram - batch_reserve_ram_frac * float(pool.max_ram_pool))

                if eff_cpu <= 0 or eff_ram <= 0:
                    continue

                p, status, ops = _pop_next_schedulable_pipeline(q, require_parents_complete=True)
                if p is None:
                    continue

                # Choose a single op to keep latency predictable and allow concurrency
                op = ops[0]

                # Determine RAM need (estimate-informed)
                est_mem = _get_estimated_mem_gb(op)
                mem_floor = default_mem_floor_gb if est_mem is None else max(default_mem_floor_gb, float(est_mem))
                mult = s.op_ram_mult.get(_op_key(op), 1.0)
                ram_need = mem_floor * mem_safety_factor * mult

                # Determine CPU need (capped)
                cpu_need = min(eff_cpu, max(1.0, cpu_cap.get(pr, 2.0)))

                # If we can't meet RAM need with effective availability, put pipeline back and try lower pr
                if ram_need > eff_ram or cpu_need > eff_cpu:
                    # Requeue the pipeline at the front to preserve FIFO within priority
                    # (we didn't schedule it, but it remains eligible when resources change).
                    q.insert(0, p)
                    continue

                # Allocate resources; note: extra RAM beyond need is "free" perf-wise, but we avoid hoarding it.
                ram_alloc = min(ram_need, eff_ram)
                cpu_alloc = cpu_need

                assignment = Assignment(
                    ops=[op],
                    cpu=cpu_alloc,
                    ram=ram_alloc,
                    priority=p.priority,
                    pool_id=pool_id,
                    pipeline_id=p.pipeline_id,
                )
                assignments.append(assignment)

                # Update local accounting (so we can pack multiple assignments in one scheduling tick)
                avail_cpu -= cpu_alloc
                avail_ram -= ram_alloc

                # Requeue the pipeline so its next ops can be scheduled later
                q.append(p)

                chosen_pipeline = p
                chosen_priority = pr
                chosen_op = op
                made_progress = True
                break

            # If no priority could schedule, stop trying to pack more on this pool
            if not chosen_pipeline:
                break

            # If pool is exhausted, stop
            if avail_cpu <= 0 or avail_ram <= 0:
                break

    return suspensions, assignments
