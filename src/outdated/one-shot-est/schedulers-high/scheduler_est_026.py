# policy_key: scheduler_est_026
# reasoning_effort: high
# model: gpt-5.2-2025-12-11
# llm_cost: 0.116864
# generation_seconds: 80.00
# generated_at: 2026-03-31T19:09:33.616645
@register_scheduler_init(key="scheduler_est_026")
def scheduler_est_026_init(s):
    """Priority-aware, estimate-sized, multi-assignment scheduler.

    Improvements over naive FIFO:
      1) Priority-aware selection: QUERY > INTERACTIVE > BATCH_PIPELINE
      2) Right-size RAM using op.estimate.mem_peak_gb (+ safety margin), instead of grabbing entire pool
      3) Pack multiple operators per pool per tick (greedy), improving concurrency and latency under load
      4) OOM-aware retries: on OOM, increase RAM multiplier for the specific operator and retry (bounded)
      5) Soft headroom reservation in pool 0 for high-priority work to reduce queueing delay
    """
    s.active_pipelines = {}  # pipeline_id -> Pipeline
    s.enqueue_tick = {}      # pipeline_id -> tick enqueued (for mild fairness/aging)
    s.tick = 0

    # Per-operator adaptive RAM sizing (keyed by (pipeline_id, op_key))
    s.op_ram_multiplier = {}  # starts at 1.0, increases on OOM
    s.op_retries = {}         # number of retries for OOM failures

    # If a pipeline experiences non-OOM failure, stop retrying it (avoid infinite loops).
    s.permanent_failed_pipelines = set()

    # Tuning knobs (kept conservative for "small improvements first")
    s.ram_safety_factor = 1.20
    s.min_ram_gb = 0.25
    s.max_oom_retries = 4

    # Per-priority target CPU share of a pool (cap per op), to avoid one op monopolizing the pool.
    s.cpu_share = {
        Priority.QUERY: 0.75,
        Priority.INTERACTIVE: 0.50,
        Priority.BATCH_PIPELINE: 0.25,
    }
    s.min_cpu = 1.0

    # Soft reservation in pool 0 for high-priority workloads (to reduce tail latency without preemption).
    s.pool0_reserve_cpu_frac = 0.25
    s.pool0_reserve_ram_frac = 0.25


def _priority_rank(priority):
    # Lower is higher priority
    if priority == Priority.QUERY:
        return 0
    if priority == Priority.INTERACTIVE:
        return 1
    return 2  # Priority.BATCH_PIPELINE and any others treated as lowest


def _oom_like(err):
    if not err:
        return False
    e = str(err).lower()
    return ("oom" in e) or ("out of memory" in e) or ("out-of-memory" in e) or ("memory" in e and "exceeded" in e)


def _op_key(pipeline_id, op):
    # Best-effort stable key; fall back to Python identity if no obvious id exists.
    for attr in ("op_id", "operator_id", "id", "name"):
        if hasattr(op, attr):
            try:
                v = getattr(op, attr)
                if v is not None:
                    return (pipeline_id, str(v))
            except Exception:
                pass
    return (pipeline_id, str(id(op)))


def _estimate_ram_gb(s, pipeline, op, pool):
    # Use estimator if present; apply safety factor and any learned multiplier.
    est = None
    try:
        est = op.estimate.mem_peak_gb
    except Exception:
        est = None

    if est is None:
        # Conservative fallback if estimate missing: small but non-trivial, still bounded by pool.
        est = max(s.min_ram_gb, 1.0)

    try:
        est = float(est)
    except Exception:
        est = max(s.min_ram_gb, 1.0)

    mult = s.op_ram_multiplier.get(_op_key(pipeline.pipeline_id, op), 1.0)
    ram = max(s.min_ram_gb, est * s.ram_safety_factor * mult)

    # Never exceed pool max RAM; executor will enforce too, but we avoid impossible requests.
    try:
        ram = min(ram, float(pool.max_ram_pool))
    except Exception:
        pass
    return ram


def _target_cpu(s, pipeline, pool, avail_cpu):
    # Priority-based CPU cap per operator to improve concurrency and latency.
    share = s.cpu_share.get(pipeline.priority, 0.25)
    try:
        cap = float(pool.max_cpu_pool) * float(share)
    except Exception:
        cap = float(avail_cpu)

    cpu = min(float(avail_cpu), max(s.min_cpu, cap))
    # Keep CPU within [min_cpu, avail_cpu]
    cpu = max(0.0, min(float(avail_cpu), float(cpu)))
    return cpu


def _has_ready_high_prio_work(active_pipelines):
    # Check if any QUERY/INTERACTIVE pipeline has a ready op; used for soft headroom in pool 0.
    for p in active_pipelines.values():
        if p.priority not in (Priority.QUERY, Priority.INTERACTIVE):
            continue
        status = p.runtime_status()
        if status.is_pipeline_successful():
            continue
        if status.state_counts.get(OperatorState.FAILED, 0) > 0:
            # Failed ops are still ASSIGNABLE (retryable); treat as ready if parents done.
            pass
        ready = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        if ready:
            return True
    return False


@register_scheduler(key="scheduler_est_026")
def scheduler_est_026(s, results, pipelines):
    """
    Scheduler step:
      - Ingest new pipelines into active set
      - Learn from results (increase RAM on OOM, stop on non-OOM failures)
      - For each pool, greedily pack multiple ready ops, picking highest-priority first,
        while keeping soft headroom in pool 0 when high-priority work is waiting.
    """
    s.tick += 1

    # Add new pipelines to active set (avoid duplicates).
    for p in pipelines:
        if p.pipeline_id not in s.active_pipelines:
            s.active_pipelines[p.pipeline_id] = p
            s.enqueue_tick[p.pipeline_id] = s.tick

    # Learn from execution results (OOM -> increase RAM multiplier; non-OOM -> mark pipeline failed).
    for r in results:
        if not r.failed():
            continue

        # If result has ops list, update per-op multipliers.
        ops = getattr(r, "ops", None) or []
        prio = getattr(r, "priority", None)
        _ = prio  # not currently used, but kept for potential tuning later

        if _oom_like(getattr(r, "error", None)):
            for op in ops:
                # We need pipeline_id to key the op; results don't always carry it.
                # Best-effort: op might have pipeline_id attribute; otherwise, we can't learn.
                pipeline_id = None
                if hasattr(op, "pipeline_id"):
                    try:
                        pipeline_id = op.pipeline_id
                    except Exception:
                        pipeline_id = None
                if pipeline_id is None:
                    # Can't attribute OOM to a specific pipeline/op reliably; skip learning.
                    continue

                key = _op_key(pipeline_id, op)
                s.op_retries[key] = s.op_retries.get(key, 0) + 1
                # Exponential-ish backoff but modest to avoid huge over-allocation.
                cur = s.op_ram_multiplier.get(key, 1.0)
                s.op_ram_multiplier[key] = min(cur * 1.5, 16.0)
        else:
            # Non-OOM failure: do not keep retrying; mark pipeline(s) as permanently failed if possible.
            # Best-effort: infer pipeline_id from ops.
            for op in ops:
                if hasattr(op, "pipeline_id"):
                    try:
                        s.permanent_failed_pipelines.add(op.pipeline_id)
                    except Exception:
                        pass

    suspensions = []
    assignments = []

    # Early exit if nothing to do.
    if not s.active_pipelines and not pipelines and not results:
        return suspensions, assignments

    # Clean up completed / permanently failed pipelines.
    to_delete = []
    for pid, p in s.active_pipelines.items():
        status = p.runtime_status()
        if status.is_pipeline_successful() or pid in s.permanent_failed_pipelines:
            to_delete.append(pid)
    for pid in to_delete:
        s.active_pipelines.pop(pid, None)
        s.enqueue_tick.pop(pid, None)

    # Precompute whether high-priority work is waiting (for pool 0 soft reservation).
    high_prio_waiting = _has_ready_high_prio_work(s.active_pipelines)

    # Helper: choose next pipeline to schedule given a pool and remaining resources.
    def pick_next_pipeline(scheduled_this_pool):
        # Priority first, then FIFO-ish by enqueue tick; skip pipelines already scheduled in this pool this tick.
        candidates = []
        for p in s.active_pipelines.values():
            if p.pipeline_id in scheduled_this_pool:
                continue
            status = p.runtime_status()
            if status.is_pipeline_successful():
                continue
            if p.pipeline_id in s.permanent_failed_pipelines:
                continue
            # Only consider pipelines that have at least one ready op.
            ready = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
            if not ready:
                continue
            candidates.append(p)

        if not candidates:
            return None

        candidates.sort(key=lambda p: (_priority_rank(p.priority), s.enqueue_tick.get(p.pipeline_id, 0), p.pipeline_id))
        return candidates[0]

    # Schedule per pool (greedy packing).
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)

        if avail_cpu <= 0.0 or avail_ram <= 0.0:
            continue

        # Soft headroom: only in pool 0, only if high-priority work is waiting.
        reserve_cpu = 0.0
        reserve_ram = 0.0
        if pool_id == 0 and high_prio_waiting:
            reserve_cpu = float(pool.max_cpu_pool) * float(s.pool0_reserve_cpu_frac)
            reserve_ram = float(pool.max_ram_pool) * float(s.pool0_reserve_ram_frac)
            reserve_cpu = max(0.0, min(reserve_cpu, float(pool.max_cpu_pool)))
            reserve_ram = max(0.0, min(reserve_ram, float(pool.max_ram_pool)))

        scheduled_this_pool = set()

        # Greedily place as many ops as we can without violating soft headroom for low-priority work.
        while avail_cpu >= s.min_cpu and avail_ram >= s.min_ram_gb:
            pipeline = pick_next_pipeline(scheduled_this_pool)
            if pipeline is None:
                break

            status = pipeline.runtime_status()
            op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
            if not op_list:
                # No ready ops (race with status changes); try next iteration.
                scheduled_this_pool.add(pipeline.pipeline_id)
                continue

            op = op_list[0]

            # Enforce bounded retries on OOM for this op.
            key = _op_key(pipeline.pipeline_id, op)
            if s.op_retries.get(key, 0) > s.max_oom_retries:
                s.permanent_failed_pipelines.add(pipeline.pipeline_id)
                # Remove from active on next cleanup pass; skip for now.
                scheduled_this_pool.add(pipeline.pipeline_id)
                continue

            ram_req = _estimate_ram_gb(s, pipeline, op, pool)
            cpu_req = _target_cpu(s, pipeline, pool, avail_cpu)

            # Fit to currently available resources.
            ram_req = min(ram_req, avail_ram)
            cpu_req = min(cpu_req, avail_cpu)

            # Respect soft reservation for low-priority assignments in pool 0.
            # High-priority ops can use reserved capacity; low-priority should leave it available.
            if pool_id == 0 and high_prio_waiting and pipeline.priority == Priority.BATCH_PIPELINE:
                # Reduce requested resources to leave headroom; if impossible, skip scheduling batch here.
                cpu_cap = max(0.0, avail_cpu - reserve_cpu)
                ram_cap = max(0.0, avail_ram - reserve_ram)
                cpu_req = min(cpu_req, cpu_cap)
                ram_req = min(ram_req, ram_cap)
                if cpu_req < s.min_cpu or ram_req < s.min_ram_gb:
                    # Can't fit a batch op without eating into reserved headroom.
                    scheduled_this_pool.add(pipeline.pipeline_id)
                    continue

            if cpu_req < s.min_cpu or ram_req < s.min_ram_gb:
                break

            assignments.append(
                Assignment(
                    ops=op_list,
                    cpu=cpu_req,
                    ram=ram_req,
                    priority=pipeline.priority,
                    pool_id=pool_id,
                    pipeline_id=pipeline.pipeline_id,
                )
            )

            # Update remaining capacity for additional placements in this pool this tick.
            avail_cpu -= float(cpu_req)
            avail_ram -= float(ram_req)
            scheduled_this_pool.add(pipeline.pipeline_id)

    return suspensions, assignments
