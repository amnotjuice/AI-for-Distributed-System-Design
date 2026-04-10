# policy_key: scheduler_est_015
# reasoning_effort: low
# model: gpt-5.2-2025-12-11
# llm_cost: 0.036623
# generation_seconds: 34.09
# generated_at: 2026-04-03T01:14:17.934478
@register_scheduler_init(key="scheduler_est_015")
def scheduler_est_015_init(s):
    """Priority-aware FIFO with simple resource right-sizing and OOM-driven RAM backoff.

    Improvements over naive FIFO:
    - Separate waiting queues per priority (QUERY > INTERACTIVE > BATCH_PIPELINE).
    - Pack multiple ready operators per tick into available pool headroom (instead of at most one).
    - Allocate smaller, priority-dependent CPU/RAM slices to reduce head-of-line blocking.
    - Use op.estimate.mem_peak_gb (if present) with light headroom; if OOM occurs, retry with higher RAM.
    """
    # Per-priority pipeline queues (FIFO within each priority).
    s.waiting_q = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }

    # Remember observed/adjusted RAM targets for (pipeline_id, op_key)
    # so retries quickly converge to sufficient RAM after OOM.
    s.op_ram_target_gb = {}

    # Small constants (GB / CPU units are executor-defined).
    s.min_ram_gb = 0.25
    s.ram_headroom_frac = 0.15  # light buffer on top of estimate
    s.oom_backoff_mult = 1.6
    s.oom_backoff_add_gb = 0.5


@register_scheduler(key="scheduler_est_015")
def scheduler_est_015_scheduler(s, results, pipelines):
    """
    Scheduler step: ingest arrivals + results, then assign ready ops by priority.

    Strategy:
    1) Enqueue new pipelines by priority.
    2) Process results: if an op failed with likely OOM, increase its RAM target for retry.
    3) For each pool, repeatedly take the highest-priority pipeline with ready ops and
       assign one ready op at a time, using right-sized CPU/RAM allocations to leave
       headroom for additional (especially high priority) work.
    """
    # ---- helpers (kept inside for single-file policy portability) ----
    def _prio_rank(prio):
        if prio == Priority.QUERY:
            return 0
        if prio == Priority.INTERACTIVE:
            return 1
        return 2

    def _iter_prios():
        return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]

    def _op_key(op):
        # Try stable identifiers if present; fall back to Python object id.
        for attr in ("op_id", "operator_id", "id", "name"):
            v = getattr(op, attr, None)
            if v is not None:
                return str(v)
        return str(id(op))

    def _looks_like_oom(err):
        if err is None:
            return False
        msg = str(err).lower()
        return ("oom" in msg) or ("out of memory" in msg) or ("memoryerror" in msg)

    def _get_est_mem_gb(op):
        # Estimator hint may be attached as op.estimate.mem_peak_gb.
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

    def _ram_target_for(pipeline_id, op, pool_avail_ram, pool_max_ram):
        # 1) Use learned target if any.
        k = (pipeline_id, _op_key(op))
        learned = s.op_ram_target_gb.get(k, None)
        if learned is not None:
            return max(s.min_ram_gb, min(learned, pool_avail_ram))

        # 2) Else use estimator with light headroom.
        est = _get_est_mem_gb(op)
        if est is not None:
            target = est * (1.0 + s.ram_headroom_frac)
            return max(s.min_ram_gb, min(target, pool_avail_ram))

        # 3) Else choose a conservative small slice to reduce blocking (but not tiny).
        # Use up to ~20% of pool RAM for QUERY/INTERACTIVE, less for BATCH.
        # (Actual priority-specific scaling applied later; this is a base.)
        base = max(s.min_ram_gb, min(pool_max_ram * 0.20, pool_avail_ram))
        return base

    def _cpu_target_for(priority, pool_avail_cpu, pool_max_cpu):
        # Keep QUERY snappy with more CPU; keep BATCH from monopolizing.
        # Also avoid using all remaining CPU to preserve concurrency.
        if pool_avail_cpu <= 0:
            return 0

        # Hard minimum: at least 1 if any CPU is available.
        min_cpu = 1 if pool_avail_cpu >= 1 else pool_avail_cpu

        if priority == Priority.QUERY:
            cap = max(min_cpu, pool_max_cpu * 0.50)
        elif priority == Priority.INTERACTIVE:
            cap = max(min_cpu, pool_max_cpu * 0.35)
        else:
            cap = max(min_cpu, pool_max_cpu * 0.20)

        return max(min_cpu, min(pool_avail_cpu, cap))

    def _ram_adjust_for_priority(priority, base_ram, pool_avail_ram, pool_max_ram):
        # Slightly more RAM for higher priority to reduce OOM retries.
        if priority == Priority.QUERY:
            mult = 1.15
        elif priority == Priority.INTERACTIVE:
            mult = 1.05
        else:
            mult = 1.0

        # Don't allocate enormous RAM slices by default; keep headroom for others.
        # Allow up to 50% pool RAM for QUERY, 40% for INTERACTIVE, 30% for BATCH.
        if priority == Priority.QUERY:
            cap = pool_max_ram * 0.50
        elif priority == Priority.INTERACTIVE:
            cap = pool_max_ram * 0.40
        else:
            cap = pool_max_ram * 0.30

        target = base_ram * mult
        return max(s.min_ram_gb, min(target, pool_avail_ram, cap))

    def _dequeue_next_pipeline_with_ready_op(queues, pool_id):
        # Pick earliest-enqueued pipeline among highest priorities that has at least
        # one assignable op whose parents are complete.
        for pr in _iter_prios():
            q = queues[pr]
            # Scan FIFO; keep pipelines that are not ready for assignment.
            for idx in range(len(q)):
                p = q[idx]
                st = p.runtime_status()
                if st.is_pipeline_successful():
                    continue
                # If pipeline has any FAILED ops, we still may retry them (ASSIGNABLE_STATES includes FAILED).
                ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
                if ops:
                    # Remove from queue, return pipeline and a single ready op.
                    q.pop(idx)
                    return p, ops[0]
        return None, None

    def _requeue_pipeline(queues, p):
        queues[p.priority].append(p)

    # ---- ingest new pipelines ----
    for p in pipelines:
        s.waiting_q[p.priority].append(p)

    # If nothing new and nothing finished/failed, we can early exit.
    if not pipelines and not results:
        return [], []

    # ---- process results: learn RAM targets on OOM ----
    for r in results:
        if not getattr(r, "failed", lambda: False)():
            continue
        if not _looks_like_oom(getattr(r, "error", None)):
            continue

        # For each op in the failed result, bump RAM target for retry.
        # ExecutionResult.ops is expected to exist (per spec).
        for op in getattr(r, "ops", []) or []:
            # We don't always have pipeline_id from result, but scheduler always assigns with it.
            # Try to read it; if absent, we can only key by op object id (still helps within run).
            pipeline_id = getattr(r, "pipeline_id", None)
            if pipeline_id is None:
                pipeline_id = getattr(op, "pipeline_id", None)
            if pipeline_id is None:
                pipeline_id = "unknown"

            k = (pipeline_id, _op_key(op))
            prev = s.op_ram_target_gb.get(k, None)
            # If we know what was allocated, use that as baseline.
            allocated = getattr(r, "ram", None)
            try:
                allocated = float(allocated) if allocated is not None else None
            except Exception:
                allocated = None

            baseline = prev if prev is not None else (allocated if allocated is not None else s.min_ram_gb)
            bumped = max(baseline + s.oom_backoff_add_gb, baseline * s.oom_backoff_mult)
            s.op_ram_target_gb[k] = bumped

    suspensions = []
    assignments = []

    # ---- scheduling: per pool packing by priority ----
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool
        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # Pack until we run out of headroom or no eligible work.
        # Stop if we can't allocate at least minimal resources.
        while avail_cpu > 0 and avail_ram > s.min_ram_gb:
            p, op = _dequeue_next_pipeline_with_ready_op(s.waiting_q, pool_id)
            if p is None or op is None:
                break

            # Re-check status in case it changed.
            st = p.runtime_status()
            if st.is_pipeline_successful():
                continue

            # Compute right-sized resources.
            base_ram = _ram_target_for(p.pipeline_id, op, avail_ram, pool.max_ram_pool)
            ram = _ram_adjust_for_priority(p.priority, base_ram, avail_ram, pool.max_ram_pool)
            cpu = _cpu_target_for(p.priority, avail_cpu, pool.max_cpu_pool)

            # If we can't give at least some CPU+RAM, put back and stop.
            if cpu <= 0 or ram <= 0:
                _requeue_pipeline(s.waiting_q, p)
                break

            # If the chosen ram is below minimum threshold (due to low avail), stop.
            if ram < s.min_ram_gb:
                _requeue_pipeline(s.waiting_q, p)
                break

            assignment = Assignment(
                ops=[op],
                cpu=cpu,
                ram=ram,
                priority=p.priority,
                pool_id=pool_id,
                pipeline_id=p.pipeline_id,
            )
            assignments.append(assignment)

            # Update local available resources for packing more work.
            avail_cpu -= cpu
            avail_ram -= ram

            # Requeue pipeline for subsequent operators.
            _requeue_pipeline(s.waiting_q, p)

    return suspensions, assignments
