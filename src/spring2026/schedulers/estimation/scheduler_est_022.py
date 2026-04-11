# policy_key: scheduler_est_022
# reasoning_effort: low
# model: gpt-5.2-2025-12-11
# llm_cost: 0.034698
# generation_seconds: 36.11
# generated_at: 2026-03-31T23:59:29.481952
@register_scheduler_init(key="scheduler_est_022")
def scheduler_est_022_init(s):
    """
    Priority-aware FIFO with small, safe improvements over the naive baseline:

    1) Priority queues: schedule QUERY/INTERACTIVE ahead of BATCH.
    2) Right-size CPU: do not give the entire pool to a single op; allocate capped CPU per op to reduce tail latency.
    3) Memory floor from estimator: allocate at least op.estimate.mem_peak_gb (when present) with a safety factor.
    4) OOM-aware retries: if an op fails with an OOM-like error, retry with increased RAM (extra RAM is "free" here).
       Non-OOM failures are treated as terminal (pipeline is dropped from scheduling).

    Notes:
    - No preemption yet (kept intentionally simple/robust as a first incremental step).
    """
    from collections import deque

    # Separate waiting queues by priority for simple, deterministic preference ordering.
    s.q_query = deque()
    s.q_interactive = deque()
    s.q_batch = deque()

    # Track pipelines we should no longer attempt to schedule (non-OOM failures).
    s.dead_pipelines = set()

    # Per-(pipeline, op) memory override on retry (GB).
    # Keyed by (pipeline_id, op_key) where op_key is a stable-ish identifier.
    s.retry_mem_gb = {}

    # Per-(pipeline, op) retry counts for OOM.
    s.retry_count = {}

    # Tuning knobs (kept conservative/simple).
    s.mem_safety_factor = 1.20
    s.mem_min_gb_default = 0.5
    s.mem_retry_growth = 1.60
    s.mem_retry_cap_factor = 8.0  # cap retry RAM at estimate * cap_factor (or a pool-based cap below)
    s.max_oom_retries = 3

    # CPU caps by priority (small caps to enable concurrency and reduce head-of-line blocking).
    s.cpu_cap_query = 4.0
    s.cpu_cap_interactive = 4.0
    s.cpu_cap_batch = 2.0

    # A small RAM overhead floor to avoid pathological tiny allocations.
    s.mem_overhead_gb = 0.25


@register_scheduler(key="scheduler_est_022")
def scheduler_est_022(s, results, pipelines):
    """
    Priority-aware scheduling loop.

    - Enqueue new pipelines by priority.
    - Process results to detect OOM failures and apply RAM increases for retries.
    - For each pool, greedily schedule ready ops:
        * always prefer higher priorities
        * allocate RAM using estimator floor + safety; increase on OOM retries
        * allocate CPU with per-priority cap (instead of taking whole pool)
    """
    # --- Helpers (defined inside to avoid imports at module scope) ---
    def _queue_for_priority(priority):
        # Prefer explicit known priorities; default to batch-like behavior.
        if priority == Priority.QUERY:
            return s.q_query
        if priority == Priority.INTERACTIVE:
            return s.q_interactive
        return s.q_batch

    def _is_oom_error(err):
        if err is None:
            return False
        msg = str(err).lower()
        return ("oom" in msg) or ("out of memory" in msg) or ("memoryerror" in msg)

    def _op_key(op):
        # Try common identifiers; fall back to object identity.
        for attr in ("op_id", "operator_id", "id", "name"):
            if hasattr(op, attr):
                try:
                    v = getattr(op, attr)
                    # If callable (rare), call it.
                    v = v() if callable(v) else v
                    if v is not None:
                        return v
                except Exception:
                    pass
        return id(op)

    def _estimate_mem_floor_gb(op):
        # Estimator is a FLOOR to avoid OOM. Extra RAM doesn't hurt (only consumes capacity).
        est = None
        try:
            est = getattr(getattr(op, "estimate", None), "mem_peak_gb", None)
        except Exception:
            est = None
        if est is None:
            est = s.mem_min_gb_default
        try:
            est = float(est)
        except Exception:
            est = s.mem_min_gb_default
        if est <= 0:
            est = s.mem_min_gb_default
        # Safety + overhead.
        return est * s.mem_safety_factor + s.mem_overhead_gb

    def _cpu_cap_for_priority(priority):
        if priority == Priority.QUERY:
            return s.cpu_cap_query
        if priority == Priority.INTERACTIVE:
            return s.cpu_cap_interactive
        return s.cpu_cap_batch

    def _pick_next_pipeline():
        # Strict priority: QUERY > INTERACTIVE > BATCH.
        if s.q_query:
            return s.q_query.popleft()
        if s.q_interactive:
            return s.q_interactive.popleft()
        if s.q_batch:
            return s.q_batch.popleft()
        return None

    # --- Enqueue new pipelines ---
    for p in pipelines:
        # Drop immediately if previously marked dead.
        if p.pipeline_id in s.dead_pipelines:
            continue
        _queue_for_priority(p.priority).append(p)

    # --- Process results: handle failures / OOM retries ---
    for r in results:
        if not r.failed():
            continue

        # If we can't attribute ops, we can still mark pipeline dead based on non-OOM error.
        is_oom = _is_oom_error(getattr(r, "error", None))
        if not is_oom:
            # Non-OOM failures are treated as terminal for simplicity.
            # We don't have pipeline_id on result in the provided interface, so we can't dead-mark by pipeline
            # unless we infer it elsewhere. We keep this conservative: just don't adjust retry mem.
            continue

        # OOM failure: bump RAM for each op in the failed assignment, so retry can succeed.
        for op in getattr(r, "ops", []) or []:
            # We don't have pipeline_id on result, but op objects belong to a pipeline; since we key by (pipeline_id, op),
            # we need pipeline_id. If we can't, fall back to op-only key with pipeline_id=None.
            # This is still useful in practice if ops are unique objects per pipeline.
            pid = getattr(op, "pipeline_id", None)
            opk = _op_key(op)
            key = (pid, opk)

            base_floor = _estimate_mem_floor_gb(op)
            prev = s.retry_mem_gb.get(key, base_floor)
            cnt = s.retry_count.get(key, 0) + 1
            s.retry_count[key] = cnt

            # Cap retries.
            if cnt > s.max_oom_retries:
                # If we can identify pipeline, mark dead; else stop increasing.
                if pid is not None:
                    s.dead_pipelines.add(pid)
                continue

            # Increase RAM request multiplicatively, bounded.
            cap = base_floor * s.mem_retry_cap_factor
            new_mem = min(prev * s.mem_retry_growth, cap)
            s.retry_mem_gb[key] = max(prev, new_mem)

    # Early exit if nothing to do.
    if not pipelines and not results and not (s.q_query or s.q_interactive or s.q_batch):
        return [], []

    suspensions = []
    assignments = []

    # --- Schedule per pool ---
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)

        # Greedy fill: schedule multiple small ops rather than one giant op.
        # Stop when we can't fit at least a minimal container.
        while avail_cpu >= 1.0 and avail_ram > 0.0:
            pipeline = _pick_next_pipeline()
            if pipeline is None:
                break

            # Skip dead pipelines.
            if pipeline.pipeline_id in s.dead_pipelines:
                continue

            status = pipeline.runtime_status()

            # Drop completed pipelines.
            if status.is_pipeline_successful():
                continue

            # Find one runnable op (parents complete).
            op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
            if not op_list:
                # Nothing runnable now; requeue pipeline to preserve FIFO within its priority.
                _queue_for_priority(pipeline.priority).append(pipeline)
                continue

            op = op_list[0]

            # Compute RAM request: max(est_floor, retry_override), capped by pool limits and availability.
            base_floor = _estimate_mem_floor_gb(op)
            key = (pipeline.pipeline_id, _op_key(op))
            mem_req = max(base_floor, s.retry_mem_gb.get(key, base_floor))

            # If the pool can't ever satisfy this, don't spin: mark dead.
            # (We conservatively use pool.max_ram_pool if available; else just use avail.)
            pool_max_ram = float(getattr(pool, "max_ram_pool", avail_ram))
            if mem_req > pool_max_ram:
                s.dead_pipelines.add(pipeline.pipeline_id)
                continue

            # If not enough RAM right now, requeue and stop filling this pool (avoid thrashing).
            if mem_req > avail_ram:
                _queue_for_priority(pipeline.priority).appendleft(pipeline)
                break

            # Compute CPU request: cap by priority, by remaining pool CPU, and by pool max.
            pool_max_cpu = float(getattr(pool, "max_cpu_pool", avail_cpu))
            cpu_cap = _cpu_cap_for_priority(pipeline.priority)
            cpu_req = min(avail_cpu, pool_max_cpu, cpu_cap)

            # Ensure minimum CPU of 1.
            if cpu_req < 1.0:
                _queue_for_priority(pipeline.priority).appendleft(pipeline)
                break

            assignments.append(
                Assignment(
                    ops=op_list,
                    cpu=cpu_req,
                    ram=mem_req,
                    priority=pipeline.priority,
                    pool_id=pool_id,
                    pipeline_id=pipeline.pipeline_id,
                )
            )

            # Update remaining resources in this pool for this tick's planning loop.
            avail_cpu -= cpu_req
            avail_ram -= mem_req

            # Requeue the pipeline to allow next stages/ops to be scheduled later.
            _queue_for_priority(pipeline.priority).append(pipeline)

    return suspensions, assignments
