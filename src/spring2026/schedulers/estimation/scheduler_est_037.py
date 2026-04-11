# policy_key: scheduler_est_037
# reasoning_effort: low
# model: gpt-5.2-2025-12-11
# llm_cost: 0.044372
# generation_seconds: 38.30
# generated_at: 2026-04-01T00:09:29.793322
@register_scheduler_init(key="scheduler_est_037")
def scheduler_est_037_init(s):
    """
    Priority-aware FIFO with simple right-sizing and OOM-aware RAM bumps.

    Improvements over naive FIFO:
      - Separate waiting queues per priority (QUERY > INTERACTIVE > BATCH_PIPELINE).
      - Avoid "give entire pool to one op" by capping per-op CPU share, enabling more parallelism.
      - Use op.estimate.mem_peak_gb as a RAM floor with a safety multiplier; bump RAM on OOM retries.
      - Don't drop pipelines just because an op failed; instead retry with increased RAM (bounded retries).
      - Simple weighted service between priority classes to reduce starvation (deficit/credits per tick).

    Notes:
      - This policy does not implement preemption because the minimal interface shown does not expose
        a reliable list of currently-running containers to suspend.
    """
    # Queues contain Pipeline objects (we re-check runtime state each tick).
    s.q_query = []
    s.q_interactive = []
    s.q_batch = []

    # Track per-(pipeline, op) retry counts and RAM multipliers for OOM mitigation.
    s.retry_counts = {}   # (pipeline_id, op_key) -> int
    s.ram_mult = {}       # (pipeline_id, op_key) -> float

    # Simple weighted fairness (credits replenish each tick; spending schedules ops).
    s.credits = {
        "query": 0,
        "interactive": 0,
        "batch": 0,
    }
    s.credit_weights = {
        "query": 6,
        "interactive": 3,
        "batch": 1,
    }

    # Tuning knobs (kept conservative to ensure "working code" first).
    s.max_retries_per_op = 3
    s.base_ram_safety = 1.20
    s.oom_ram_bump = 1.60  # multiplier applied per OOM retry (compounded via s.ram_mult)

    # CPU caps as fraction of pool capacity per op, by priority.
    s.cpu_cap_frac = {
        Priority.QUERY: 0.50,
        Priority.INTERACTIVE: 0.50,
        Priority.BATCH_PIPELINE: 0.80,
    }

    # Minimum allocs to avoid pathological tiny slices.
    s.min_cpu = 1.0
    s.min_ram_gb = 0.25


@register_scheduler(key="scheduler_est_037")
def scheduler_est_037(s, results, pipelines):
    """
    Scheduler step:
      1) Ingest new pipelines into per-priority queues.
      2) Observe failures; if OOM-like, increase RAM multiplier for that (pipeline, op) and allow retry.
      3) Refill credits and schedule runnable ops across pools using credit-weighted priority selection.
      4) Right-size: RAM = max(estimate floor * safety * retry_multiplier, min_ram); CPU capped by priority.
    """
    def _pclass(priority):
        if priority == Priority.QUERY:
            return "query"
        if priority == Priority.INTERACTIVE:
            return "interactive"
        return "batch"

    def _get_queue(priority):
        if priority == Priority.QUERY:
            return s.q_query
        if priority == Priority.INTERACTIVE:
            return s.q_interactive
        return s.q_batch

    def _op_key(op):
        # Best-effort stable identifier across retries.
        for attr in ("op_id", "operator_id", "id", "name", "key"):
            if hasattr(op, attr):
                v = getattr(op, attr)
                try:
                    if v is not None:
                        return str(v)
                except Exception:
                    pass
        return str(op)

    def _is_oom_error(err):
        if err is None:
            return False
        try:
            msg = str(err).lower()
        except Exception:
            return False
        return ("oom" in msg) or ("out of memory" in msg) or ("memoryerror" in msg)

    # Enqueue arriving pipelines.
    for p in pipelines:
        _get_queue(p.priority).append(p)

    # Incorporate results: bump RAM on OOM failures (bounded retries).
    for r in results:
        if not getattr(r, "failed", lambda: False)():
            continue
        if not _is_oom_error(getattr(r, "error", None)):
            continue
        # r.ops is expected to be a list; bump for each op in that assignment.
        for op in getattr(r, "ops", []) or []:
            pid = getattr(op, "pipeline_id", None)
            # Prefer the pipeline_id from result context if op doesn't have it.
            # We do not have direct pipeline_id on ExecutionResult in the provided surface;
            # so we key RAM bumps by op identity plus best-effort pid fallback.
            # If pid missing, we key by "unknown" to still help retries within the same tick loops.
            if pid is None:
                pid = "unknown"
            ok = _op_key(op)
            key = (pid, ok)
            s.retry_counts[key] = s.retry_counts.get(key, 0) + 1
            # Compound multiplier to ramp quickly but safely.
            prev = s.ram_mult.get(key, 1.0)
            s.ram_mult[key] = prev * s.oom_ram_bump

    # Early exit if nothing changed.
    if not pipelines and not results:
        return [], []

    # Refill credits each tick to enable weighted sharing and prevent starvation.
    for k, w in s.credit_weights.items():
        s.credits[k] = s.credits.get(k, 0) + w

    suspensions = []
    assignments = []

    # Helper: pull next runnable op from a specific queue, while cleaning up completed pipelines.
    def _pop_next_runnable_from_queue(q):
        # We'll rotate through q (FIFO) until we find an assignable op whose parents are complete.
        # Pipelines that are completed are dropped; pipelines with failures are not dropped automatically.
        n = len(q)
        for _ in range(n):
            p = q.pop(0)
            status = p.runtime_status()
            if status.is_pipeline_successful():
                continue  # drop completed
            # Find one runnable op respecting DAG dependencies.
            op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
            if not op_list:
                # Not runnable now, keep it in queue.
                q.append(p)
                continue
            # Runnable: requeue pipeline for subsequent ops later.
            q.append(p)
            return p, op_list
        return None, None

    # Helper: choose the next priority class to schedule based on credits and availability.
    def _choose_next_class():
        # Prefer higher priorities; within those, require positive credits.
        for cls in ("query", "interactive", "batch"):
            if s.credits.get(cls, 0) <= 0:
                continue
            q = s.q_query if cls == "query" else s.q_interactive if cls == "interactive" else s.q_batch
            if q:
                return cls
        # If no class has credits but work exists, allow batch to proceed (credits will replenish next tick).
        if s.q_query:
            return "query"
        if s.q_interactive:
            return "interactive"
        if s.q_batch:
            return "batch"
        return None

    # Scheduling loop across pools: pack as many ops as possible per pool in this tick.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)

        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # Keep assigning while we have capacity and runnable work.
        # Conservative stop conditions to avoid infinite loops if queues contain only blocked DAG nodes.
        no_progress_iters = 0
        max_iters = 64

        while avail_cpu > 0 and avail_ram > 0 and no_progress_iters < max_iters:
            cls = _choose_next_class()
            if cls is None:
                break

            q = s.q_query if cls == "query" else s.q_interactive if cls == "interactive" else s.q_batch
            pipeline, op_list = _pop_next_runnable_from_queue(q)
            if pipeline is None or not op_list:
                no_progress_iters += 1
                continue

            # Spend one credit per scheduled op (simple, robust).
            s.credits[cls] = max(0, s.credits.get(cls, 0) - 1)

            op = op_list[0]

            # RAM sizing:
            # - Use estimate as a floor; extra RAM is "free" but limited by pool capacity.
            # - Apply safety + OOM multiplier if present.
            est = getattr(getattr(op, "estimate", None), "mem_peak_gb", None)
            try:
                est_gb = float(est) if est is not None else 0.0
            except Exception:
                est_gb = 0.0

            # Key RAM bumps by (pipeline_id, op_key) when possible.
            pid = getattr(pipeline, "pipeline_id", "unknown")
            ok = _op_key(op)
            mkey = (pid, ok)
            mult = s.ram_mult.get(mkey, 1.0)

            # If we've retried too much, stop retrying by not scheduling further (leave for other work).
            # This avoids infinite failure loops and keeps latency for others stable.
            if s.retry_counts.get(mkey, 0) > s.max_retries_per_op:
                no_progress_iters += 1
                continue

            ram_req = max(s.min_ram_gb, est_gb * s.base_ram_safety * mult)
            if ram_req > avail_ram:
                # Not enough RAM right now; rotate and try other work.
                no_progress_iters += 1
                continue

            # CPU sizing:
            cap_frac = s.cpu_cap_frac.get(pipeline.priority, 0.8)
            cpu_cap = max(s.min_cpu, float(pool.max_cpu_pool) * float(cap_frac))
            cpu_req = min(avail_cpu, cpu_cap)
            if cpu_req < s.min_cpu:
                no_progress_iters += 1
                continue

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

            # Update local availability to enable multiple assignments per pool per tick.
            avail_cpu -= cpu_req
            avail_ram -= ram_req
            no_progress_iters = 0

    return suspensions, assignments
