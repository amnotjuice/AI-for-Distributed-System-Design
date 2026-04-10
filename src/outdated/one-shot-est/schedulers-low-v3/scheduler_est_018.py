# policy_key: scheduler_est_018
# reasoning_effort: low
# model: gpt-5.2-2025-12-11
# llm_cost: 0.042391
# generation_seconds: 28.77
# generated_at: 2026-04-03T01:16:01.175745
@register_scheduler_init(key="scheduler_est_018")
def scheduler_est_018_init(s):
    """Priority-aware, conservative-first improvement over naive FIFO.

    Main ideas:
      - Maintain separate FIFO queues per priority (QUERY > INTERACTIVE > BATCH_PIPELINE).
      - Prefer scheduling higher-priority ready operators first to reduce tail latency.
      - Simple CPU/RAM sizing by priority:
          * High priority gets more CPU share (faster completion).
          * Batch gets a smaller CPU share to avoid crowding out interactive work.
      - If an operator fails (likely OOM), remember and increase its RAM on retry.
    """
    from collections import deque

    # Per-priority pipeline queues (FIFO within each class)
    s.q_query = deque()
    s.q_interactive = deque()
    s.q_batch = deque()

    # Track per-operator retry RAM targets (keyed by a stable-ish operator key)
    s.op_ram_target_gb = {}  # op_key -> float
    s.op_oom_retries = {}    # op_key -> int


@register_scheduler(key="scheduler_est_018")
def scheduler_est_018_scheduler(s, results, pipelines):
    """
    Priority-aware FIFO scheduling with simple resource sizing and OOM-aware retries.

    Returns:
        (suspensions, assignments)
    """
    from collections import deque

    def _prio_rank(prio):
        # Lower is better
        if prio == Priority.QUERY:
            return 0
        if prio == Priority.INTERACTIVE:
            return 1
        return 2  # Priority.BATCH_PIPELINE and anything else

    def _enqueue_pipeline(p):
        if p.priority == Priority.QUERY:
            s.q_query.append(p)
        elif p.priority == Priority.INTERACTIVE:
            s.q_interactive.append(p)
        else:
            s.q_batch.append(p)

    def _pick_next_queue():
        # Strict priority among classes; FIFO within each class.
        if s.q_query:
            return s.q_query
        if s.q_interactive:
            return s.q_interactive
        return s.q_batch

    def _op_key(pipeline_id, op):
        # Try to find a stable identifier; fall back to object id.
        for attr in ("operator_id", "op_id", "id", "name"):
            if hasattr(op, attr):
                return (pipeline_id, getattr(op, attr))
        return (pipeline_id, id(op))

    def _desired_ram_gb(op, pipeline_id, pool_avail_ram):
        # Prefer estimator if present; otherwise be conservative but not huge.
        op_key = _op_key(pipeline_id, op)

        # If we previously learned a target (e.g., after OOM), use it.
        if op_key in s.op_ram_target_gb:
            return min(float(s.op_ram_target_gb[op_key]), float(pool_avail_ram))

        est = None
        if hasattr(op, "estimate") and op.estimate is not None and hasattr(op.estimate, "mem_peak_gb"):
            est = op.estimate.mem_peak_gb

        if est is None:
            # Conservative default: don't grab the entire pool; leave room for others.
            # (Also avoids spurious over-allocation that can block higher priority.)
            return min(float(pool_avail_ram), max(1.0, float(pool_avail_ram) * 0.50))

        # Use the estimate fairly aggressively with a small safety factor.
        # If it's an underestimate, OOM will trigger a retry with more RAM.
        return min(float(pool_avail_ram), max(1.0, float(est) * 1.10))

    def _desired_cpu(prio, pool_avail_cpu):
        # Give more CPU to higher-priority tasks for latency.
        # Keep within available CPU.
        if pool_avail_cpu <= 0:
            return 0

        if prio == Priority.QUERY:
            share = 1.00
        elif prio == Priority.INTERACTIVE:
            share = 0.75
        else:
            share = 0.50

        cpu = float(pool_avail_cpu) * share
        # Ensure we allocate something if any is available.
        return max(0.0, cpu)

    # Ingest new pipelines
    for p in pipelines:
        _enqueue_pipeline(p)

    # Learn from execution results (especially OOM-like failures)
    for r in results:
        if not r.failed():
            continue
        # Any failure: retry is allowed by ASSIGNABLE_STATES including FAILED.
        # Specifically handle OOM-like errors by increasing RAM target.
        is_oom = False
        if getattr(r, "error", None):
            err_str = str(r.error).lower()
            if "oom" in err_str or "out of memory" in err_str or "out-of-memory" in err_str:
                is_oom = True

        # Update per-op RAM target on failure.
        # We don't have explicit pipeline_id in result; best-effort map using op object.
        # If pipeline_id isn't available, fall back to op-only key.
        for op in getattr(r, "ops", []) or []:
            pid = getattr(op, "pipeline_id", None)
            if pid is None:
                # If the op doesn't carry pipeline_id, use a looser key.
                op_key = ("unknown", _op_key("unknown", op)[1])
            else:
                op_key = _op_key(pid, op)

            prev = s.op_ram_target_gb.get(op_key, None)
            # If we know what we allocated (r.ram), step up multiplicatively; else start from 2GB.
            base = float(getattr(r, "ram", 0.0) or (prev if prev is not None else 2.0))
            retries = int(s.op_oom_retries.get(op_key, 0))

            if is_oom:
                # Exponential-ish backoff, but not too explosive.
                factor = 1.6 if retries < 2 else 1.3
                new_target = base * factor
                s.op_ram_target_gb[op_key] = new_target
                s.op_oom_retries[op_key] = retries + 1
            else:
                # For non-OOM failures, don't blindly inflate RAM.
                # Still keep a minimal target so we don't shrink unexpectedly.
                if prev is None and base > 0:
                    s.op_ram_target_gb[op_key] = base

    # If nothing changed, exit quickly
    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    # For each pool, try to schedule one ready op at a time, prioritizing high-priority queues.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)

        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # We will attempt multiple placements per pool, but avoid long loops.
        # Each assignment consumes some CPU/RAM, so we update available headroom.
        max_iters = 64
        iters = 0

        while avail_cpu > 0 and avail_ram > 0 and iters < max_iters:
            iters += 1

            # Choose next pipeline from highest-priority non-empty queue.
            q = _pick_next_queue()
            if not q:
                break

            pipeline = q.popleft()
            status = pipeline.runtime_status()

            # Drop completed pipelines
            if status.is_pipeline_successful():
                continue

            # Find one ready-to-run operator (parents completed).
            op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
            if not op_list:
                # Not runnable yet; keep it in its class queue for later.
                _enqueue_pipeline(pipeline)
                continue

            op = op_list[0]
            need_ram = _desired_ram_gb(op, pipeline.pipeline_id, avail_ram)
            need_cpu = _desired_cpu(pipeline.priority, avail_cpu)

            # If we can't fit, requeue and stop trying in this pool (avoid spinning).
            if need_ram > avail_ram or need_cpu <= 0:
                _enqueue_pipeline(pipeline)
                break

            # Make the assignment
            assignments.append(
                Assignment(
                    ops=op_list,
                    cpu=need_cpu,
                    ram=need_ram,
                    priority=pipeline.priority,
                    pool_id=pool_id,
                    pipeline_id=pipeline.pipeline_id,
                )
            )

            # Update pool headroom for additional placements
            avail_cpu -= float(need_cpu)
            avail_ram -= float(need_ram)

            # Requeue pipeline to continue later (more ops may become runnable later)
            _enqueue_pipeline(pipeline)

    return suspensions, assignments
