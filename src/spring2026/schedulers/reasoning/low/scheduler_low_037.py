# policy_key: scheduler_low_037
# reasoning_effort: low
# model: gpt-5.2-2025-12-11
# llm_cost: 0.043593
# generation_seconds: 42.99
# generated_at: 2026-04-09T21:32:10.672097
@register_scheduler_init(key="scheduler_low_037")
def scheduler_low_037_init(s):
    """
    Priority-first, OOM-adaptive, non-preemptive scheduler.

    Core ideas:
      - Strictly protect QUERY/INTERACTIVE latency by always scheduling them first.
      - Avoid pipeline failures (720s penalty) by retrying failed operators with RAM backoff
        when failures look like OOM/memory errors.
      - Use simple per-pool "headroom reservation" to prevent BATCH from consuming all
        resources and delaying high-priority arrivals.
      - Keep fairness within each priority via round-robin deques.

    Notes/assumptions:
      - We do not rely on inspecting operator RAM minima (not exposed in the template).
        Instead, we right-size using pool capacity and learn via OOM backoff.
      - We avoid preemption because the template does not expose a reliable way to list
        running containers to suspend.
    """
    from collections import deque

    # Priority queues (round-robin within each priority class)
    s.q_query = deque()
    s.q_interactive = deque()
    s.q_batch = deque()

    # Track known pipelines to avoid duplicate enqueues on repeated arrivals
    s.known_pipelines = {}  # pipeline_id -> Pipeline

    # Retry accounting per (pipeline_id, op_key)
    s.op_retries = {}  # (pipeline_id, op_key) -> int

    # Pipeline-level RAM boost factor (increases on OOM-like failures)
    s.pipeline_ram_boost = {}  # pipeline_id -> float

    # Caps/parameters (conservative defaults; small improvements over FIFO)
    s.max_retries_per_op = 3
    s.max_pipeline_boost = 4.0
    s.oom_boost_mult = 1.6

    # Per-pool headroom reserved for high priority (QUERY+INTERACTIVE).
    # BATCH only uses resources beyond these reserves (when possible).
    s.reserve_cpu_frac = 0.20
    s.reserve_ram_frac = 0.20

    # Sizing targets by priority (fractions of pool max, bounded by available)
    s.cpu_target_frac = {
        Priority.QUERY: 0.75,
        Priority.INTERACTIVE: 0.55,
        Priority.BATCH_PIPELINE: 0.50,
    }
    s.ram_target_frac = {
        Priority.QUERY: 0.80,
        Priority.INTERACTIVE: 0.65,
        Priority.BATCH_PIPELINE: 0.55,
    }

    # Minimum allocations to reduce accidental starvation / too-tiny containers
    s.min_cpu = 1.0
    s.min_ram = 0.5  # in "pool RAM units" (same units as avail_ram_pool)


@register_scheduler(key="scheduler_low_037")
def scheduler_low_037(s, results, pipelines):
    """
    Scheduling step:
      1) Ingest new pipelines into per-priority queues.
      2) Process results: detect OOM-like failures, increase RAM boost, count retries.
      3) For each pool, schedule at most one operator (to keep logic simple & stable):
           - Always prefer QUERY, then INTERACTIVE, then BATCH.
           - BATCH is gated by headroom reservation so it doesn't block future high-prio.
           - Allocate CPU/RAM based on priority targets and adaptive RAM boost.
      4) Requeue pipelines that still have remaining work.
    """
    from collections import deque

    def _get_queue(priority):
        if priority == Priority.QUERY:
            return s.q_query
        if priority == Priority.INTERACTIVE:
            return s.q_interactive
        return s.q_batch

    def _is_oom_like(err):
        if not err:
            return False
        msg = str(err).lower()
        # Keep broad to capture simulator error strings.
        return ("oom" in msg) or ("out of memory" in msg) or ("memory" in msg) or ("killed" in msg)

    def _op_key(op):
        # Try several likely stable identifiers; fall back to repr().
        for attr in ("op_id", "operator_id", "id", "name", "key"):
            if hasattr(op, attr):
                v = getattr(op, attr)
                try:
                    return str(v)
                except Exception:
                    pass
        try:
            return str(op)
        except Exception:
            return repr(op)

    def _enqueue_pipeline(p):
        q = _get_queue(p.priority)
        q.append(p)

    def _pipeline_done_or_hard_failed(p):
        st = p.runtime_status()
        if st.is_pipeline_successful():
            return True
        # We do not declare "hard failed" solely based on FAILED states because we may retry.
        return False

    def _next_assignable_op(p):
        st = p.runtime_status()
        # Only schedule ops whose parents are complete.
        ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        if not ops:
            return []
        return ops[:1]

    # -------------------------
    # 1) Ingest new pipelines
    # -------------------------
    for p in pipelines:
        pid = p.pipeline_id
        # If the same pipeline object arrives multiple times, don't duplicate queue entries.
        if pid not in s.known_pipelines:
            s.known_pipelines[pid] = p
            s.pipeline_ram_boost[pid] = 1.0
            _enqueue_pipeline(p)

    # Fast exit if nothing to do
    if not pipelines and not results:
        return [], []

    # -------------------------
    # 2) Process results
    # -------------------------
    for r in results:
        if not r.failed():
            continue

        pid = getattr(r, "pipeline_id", None)
        # Some simulators may not include pipeline_id on results; infer from op retries is optional.
        # If we cannot identify pipeline_id, we still avoid crashing.
        ops = getattr(r, "ops", None) or []
        if pid is None:
            # Best-effort: do nothing beyond returning to queue via runtime_status in future ticks.
            continue

        # Retry accounting per op
        for op in ops:
            k = (pid, _op_key(op))
            s.op_retries[k] = s.op_retries.get(k, 0) + 1

        # If it looks like OOM, increase RAM boost for this pipeline (bounded).
        if _is_oom_like(getattr(r, "error", None)):
            cur = s.pipeline_ram_boost.get(pid, 1.0)
            s.pipeline_ram_boost[pid] = min(s.max_pipeline_boost, cur * s.oom_boost_mult)

        # Ensure the pipeline is queued again if it still has work.
        p = s.known_pipelines.get(pid)
        if p is not None and not _pipeline_done_or_hard_failed(p):
            _enqueue_pipeline(p)

    # -------------------------
    # 3) Make assignments (one op per pool per tick)
    # -------------------------
    suspensions = []
    assignments = []

    def _try_schedule_from_queue(pool_id, pool, q, allow_even_if_low_headroom):
        """
        Attempt to schedule one operator from queue q onto given pool.
        Returns True if an assignment was made (consumes one per pool).
        """
        if not q:
            return False

        # Snapshot available resources at start of attempt
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool
        if avail_cpu <= 0 or avail_ram <= 0:
            return False

        # Round-robin scan up to queue length to find a runnable pipeline
        n = len(q)
        for _ in range(n):
            p = q.popleft()
            pid = p.pipeline_id

            # Drop completed pipelines from queues
            if _pipeline_done_or_hard_failed(p):
                continue

            # If pipeline has any FAILED ops, only retry up to max_retries_per_op.
            # We approximate by checking assignable ops (which includes FAILED) and seeing retry count.
            op_list = _next_assignable_op(p)
            if not op_list:
                # Not runnable yet; keep it in rotation.
                q.append(p)
                continue

            # Enforce retry cap for this op
            ok_to_run = True
            for op in op_list:
                rk = (pid, _op_key(op))
                if s.op_retries.get(rk, 0) > s.max_retries_per_op:
                    ok_to_run = False
                    break
            if not ok_to_run:
                # Don't keep thrashing this pipeline forever; still keep it queued so other ops
                # might become runnable (but in practice it will likely remain blocked).
                q.append(p)
                continue

            prio = p.priority

            # Headroom gating for BATCH: if we don't have enough free resources beyond reserves,
            # skip BATCH for now to protect future QUERY/INTERACTIVE arrivals.
            if prio == Priority.BATCH_PIPELINE and not allow_even_if_low_headroom:
                reserve_cpu = pool.max_cpu_pool * s.reserve_cpu_frac
                reserve_ram = pool.max_ram_pool * s.reserve_ram_frac
                if avail_cpu <= reserve_cpu or avail_ram <= reserve_ram:
                    q.append(p)
                    continue

            # Compute target sizes
            cpu_tgt = max(s.min_cpu, pool.max_cpu_pool * s.cpu_target_frac.get(prio, 0.5))
            ram_tgt = max(s.min_ram, pool.max_ram_pool * s.ram_target_frac.get(prio, 0.55))

            # Apply adaptive RAM boost to reduce OOM re-failures
            boost = s.pipeline_ram_boost.get(pid, 1.0)
            ram_tgt = ram_tgt * boost

            # Bound by available resources
            cpu = min(avail_cpu, cpu_tgt)
            ram = min(avail_ram, ram_tgt)

            # If we can't allocate minimums, requeue and skip
            if cpu < s.min_cpu or ram < s.min_ram:
                q.append(p)
                continue

            assignments.append(
                Assignment(
                    ops=op_list,
                    cpu=cpu,
                    ram=ram,
                    priority=prio,
                    pool_id=pool_id,
                    pipeline_id=pid,
                )
            )

            # Requeue pipeline for future steps (it likely has more ops)
            q.append(p)
            return True

        return False

    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]

        # Prefer high priorities regardless of headroom.
        did = _try_schedule_from_queue(pool_id, pool, s.q_query, allow_even_if_low_headroom=True)
        if did:
            continue
        did = _try_schedule_from_queue(pool_id, pool, s.q_interactive, allow_even_if_low_headroom=True)
        if did:
            continue

        # BATCH only if headroom permits.
        _try_schedule_from_queue(pool_id, pool, s.q_batch, allow_even_if_low_headroom=False)

    return suspensions, assignments
