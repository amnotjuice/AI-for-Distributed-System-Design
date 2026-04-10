# policy_key: scheduler_est_002
# reasoning_effort: low
# model: gpt-5.2-2025-12-11
# llm_cost: 0.052040
# generation_seconds: 38.29
# generated_at: 2026-04-03T01:05:39.869257
@register_scheduler_init(key="scheduler_est_002")
def scheduler_est_002_init(s):
    """Priority-aware FIFO with simple RAM-estimate retry.

    Small, incremental improvements over naive FIFO:
      1) Maintain separate queues per priority and always schedule highest priority first.
      2) Keep a small per-pool reservation so BATCH doesn't fully consume capacity, preserving
         headroom for QUERY/INTERACTIVE arrivals (improves tail latency under load).
      3) Use per-operator RAM estimates (if present) and retry on failures by increasing RAM.
         Failures are treated as likely OOM and are retried with exponential RAM bumps.
    """
    s.q_query = []
    s.q_interactive = []
    s.q_batch = []

    # Track per-(pipeline, op) failures and RAM multipliers for retries.
    s.op_fail_count = {}          # (pipeline_id, op_key) -> int
    s.op_ram_mult = {}            # (pipeline_id, op_key) -> float
    s.pipeline_fail_total = {}    # pipeline_id -> int

    # Config knobs: keep them simple and safe.
    s.max_op_retries = 3
    s.default_ram_gb = 1.0
    s.base_ram_slack = 1.10       # allocate close to estimate; retry if wrong
    s.retry_ram_growth = 1.60     # multiplicative RAM bump on each failure
    s.batch_cpu_fraction = 0.50   # cap batch per-op CPU to encourage concurrency
    s.interactive_cpu_fraction = 0.75

    # Per-pool reservation (fraction of pool) kept free from batch when possible.
    s.reserve_cpu_frac = 0.25
    s.reserve_ram_frac = 0.25


@register_scheduler(key="scheduler_est_002")
def scheduler_est_002(s, results, pipelines):
    """
    Scheduler step:
      - Ingest new pipelines into per-priority FIFO queues.
      - Process results to learn failures and update RAM multipliers.
      - For each pool, schedule runnable operators from highest priority queues first.
      - Avoid letting BATCH consume reserved headroom when possible.
    """
    def _enqueue(p):
        if p.priority == Priority.QUERY:
            s.q_query.append(p)
        elif p.priority == Priority.INTERACTIVE:
            s.q_interactive.append(p)
        else:
            s.q_batch.append(p)

    def _op_key(op):
        # Try common identifiers; fall back to stable repr.
        for attr in ("op_id", "operator_id", "id", "name"):
            if hasattr(op, attr):
                try:
                    v = getattr(op, attr)
                    if v is not None:
                        return str(v)
                except Exception:
                    pass
        return repr(op)

    def _get_est_mem_gb(op):
        # Estimator may attach op.estimate.mem_peak_gb (float or None).
        try:
            est = getattr(getattr(op, "estimate", None), "mem_peak_gb", None)
            if est is None:
                return None
            est = float(est)
            if est > 0:
                return est
        except Exception:
            return None
        return None

    def _get_min_mem_gb(op):
        # If an operator exposes a minimum RAM requirement, respect it; otherwise use default.
        for attr in ("mem_min_gb", "min_mem_gb", "min_ram_gb", "mem_gb", "ram_gb"):
            if hasattr(op, attr):
                try:
                    v = float(getattr(op, attr))
                    if v > 0:
                        return v
                except Exception:
                    pass
        return s.default_ram_gb

    def _compute_ram_request(pipeline_id, op, pool_avail_ram):
        opk = _op_key(op)
        mult = s.op_ram_mult.get((pipeline_id, opk), 1.0)

        est = _get_est_mem_gb(op)
        base = est if est is not None else _get_min_mem_gb(op)

        # Allocate close to estimate; rely on retries on OOM.
        req = base * s.base_ram_slack * mult

        # Clamp to what's available (scheduler must remain feasible).
        if req <= 0:
            req = s.default_ram_gb
        if pool_avail_ram > 0:
            req = min(req, pool_avail_ram)
        return req

    def _compute_cpu_request(priority, pool, pool_avail_cpu):
        if pool_avail_cpu <= 0:
            return 0.0
        max_cpu = float(pool.max_cpu_pool)

        if priority == Priority.QUERY:
            # Give queries as much as possible to reduce latency.
            target = max_cpu
        elif priority == Priority.INTERACTIVE:
            target = max(1.0, s.interactive_cpu_fraction * max_cpu)
        else:
            target = max(1.0, s.batch_cpu_fraction * max_cpu)

        return min(float(pool_avail_cpu), target)

    def _has_runnable_op(p):
        st = p.runtime_status()
        if st.is_pipeline_successful():
            return False
        ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        return bool(ops)

    def _next_runnable_from_queue(q):
        # FIFO scan with rotation to avoid head-of-line blocking on pipelines with no runnable ops.
        n = len(q)
        for _ in range(n):
            p = q.pop(0)
            st = p.runtime_status()
            if st.is_pipeline_successful():
                continue
            # Drop pipelines that exceeded retry budget.
            if s.pipeline_fail_total.get(p.pipeline_id, 0) > s.max_op_retries * 10:
                continue
            if _has_runnable_op(p):
                return p
            q.append(p)
        return None

    # Ingest new pipelines.
    for p in pipelines:
        _enqueue(p)

    # Process results: update failure counters + RAM multipliers for retries.
    for r in results:
        if not getattr(r, "ops", None):
            continue
        pipeline_id = None
        # ExecutionResult doesn't guarantee pipeline_id; infer from op if present.
        # If unavailable, we still update at (None, op_key) scope, but try hard to find id.
        for op in r.ops:
            if pipeline_id is None:
                for attr in ("pipeline_id", "dag_id", "job_id"):
                    if hasattr(op, attr):
                        try:
                            v = getattr(op, attr)
                            if v is not None:
                                pipeline_id = v
                                break
                        except Exception:
                            pass

        # If we couldn't infer, keep None; our scheduling uses pipeline.pipeline_id anyway.
        if r.failed():
            for op in r.ops:
                opk = _op_key(op)
                key = (pipeline_id, opk)
                s.op_fail_count[key] = s.op_fail_count.get(key, 0) + 1
                # Bump RAM multiplier so next retry requests more.
                prev = s.op_ram_mult.get(key, 1.0)
                s.op_ram_mult[key] = prev * s.retry_ram_growth

    # Early exit if nothing changed.
    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    # Scheduling loop per pool: try to place multiple ops while capacity exists.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)
        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # Reservation to protect high priority latency: batch shouldn't consume below this.
        reserve_cpu = float(pool.max_cpu_pool) * float(s.reserve_cpu_frac)
        reserve_ram = float(pool.max_ram_pool) * float(s.reserve_ram_frac)

        # Keep assigning while resources remain.
        while avail_cpu > 0 and avail_ram > 0:
            # Priority order: QUERY -> INTERACTIVE -> BATCH
            chosen_queue = None
            chosen_pipeline = _next_runnable_from_queue(s.q_query)
            if chosen_pipeline is not None:
                chosen_queue = s.q_query
            else:
                chosen_pipeline = _next_runnable_from_queue(s.q_interactive)
                if chosen_pipeline is not None:
                    chosen_queue = s.q_interactive
                else:
                    chosen_pipeline = _next_runnable_from_queue(s.q_batch)
                    chosen_queue = s.q_batch if chosen_pipeline is not None else None

            if chosen_pipeline is None:
                break

            st = chosen_pipeline.runtime_status()
            op_list = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
            if not op_list:
                # Nothing runnable; put back and stop trying for now.
                chosen_queue.append(chosen_pipeline)
                break

            op = op_list[0]

            # Enforce per-op retry budget (per pipeline/op) using the actual pipeline id.
            pid = chosen_pipeline.pipeline_id
            opk = _op_key(op)
            fail_key = (pid, opk)
            # Merge any "unknown pipeline" learning if present.
            unknown_key = (None, opk)
            if unknown_key in s.op_ram_mult and fail_key not in s.op_ram_mult:
                s.op_ram_mult[fail_key] = s.op_ram_mult[unknown_key]
            if unknown_key in s.op_fail_count and fail_key not in s.op_fail_count:
                s.op_fail_count[fail_key] = s.op_fail_count[unknown_key]

            fails = s.op_fail_count.get(fail_key, 0)
            if fails > s.max_op_retries:
                # Give up on this operator; effectively drop pipeline by not requeueing it.
                s.pipeline_fail_total[pid] = s.pipeline_fail_total.get(pid, 0) + 1
                continue

            # If this is batch, don't dip into reservation unless no high priority backlog.
            if chosen_pipeline.priority == Priority.BATCH_PIPELINE:
                high_backlog = bool(s.q_query) or bool(s.q_interactive)
                if high_backlog:
                    # If we'd go under reservation, stop scheduling batch in this pool.
                    if avail_cpu <= reserve_cpu or avail_ram <= reserve_ram:
                        chosen_queue.insert(0, chosen_pipeline)
                        break

            req_cpu = _compute_cpu_request(chosen_pipeline.priority, pool, avail_cpu)
            if req_cpu <= 0:
                chosen_queue.insert(0, chosen_pipeline)
                break

            req_ram = _compute_ram_request(pid, op, avail_ram)
            if req_ram <= 0:
                chosen_queue.insert(0, chosen_pipeline)
                break

            # Feasibility check.
            if req_cpu > avail_cpu or req_ram > avail_ram:
                chosen_queue.insert(0, chosen_pipeline)
                break

            assignments.append(
                Assignment(
                    ops=op_list,
                    cpu=req_cpu,
                    ram=req_ram,
                    priority=chosen_pipeline.priority,
                    pool_id=pool_id,
                    pipeline_id=chosen_pipeline.pipeline_id,
                )
            )

            # Reduce local accounting and requeue pipeline for subsequent ops.
            avail_cpu -= req_cpu
            avail_ram -= req_ram
            chosen_queue.append(chosen_pipeline)

    return suspensions, assignments
