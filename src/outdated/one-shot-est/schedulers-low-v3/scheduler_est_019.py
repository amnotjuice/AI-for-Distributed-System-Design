# policy_key: scheduler_est_019
# reasoning_effort: low
# model: gpt-5.2-2025-12-11
# llm_cost: 0.032633
# generation_seconds: 36.30
# generated_at: 2026-04-03T01:16:37.473296
@register_scheduler_init(key="scheduler_est_019")
def scheduler_est_019_init(s):
    """Priority-aware FIFO with simple OOM-aware RAM retries.

    Improvements over naive FIFO:
      - Maintain separate queues per priority and schedule higher priority first.
      - Avoid dropping pipelines on OOM; instead, retry failed ops with increased RAM.
      - Size RAM based on (optional) mem_peak estimate, with multiplicative backoff on OOM.
      - Assign CPU shares biased toward higher priorities (still simple and safe).
      - Keep fairness within each priority via round-robin requeue.
    """
    # Queues per priority (round-robin FIFO within each).
    s.q_query = []
    s.q_interactive = []
    s.q_batch = []

    # Per-operator RAM retry multiplier (increases on OOM).
    # Keyed by (pipeline_id, op_key_str).
    s.op_ram_mult = {}

    # Pipelines that should be allowed to retry even if they have FAILED ops (OOM).
    s.retryable_pipelines = set()


@register_scheduler(key="scheduler_est_019")
def scheduler_est_019_scheduler(s, results, pipelines):
    """
    Scheduler step:
      1) Ingest new pipelines into per-priority queues.
      2) Process execution results; on OOM, mark pipeline retryable and increase RAM multiplier.
      3) For each pool, repeatedly pick next ready op from highest-priority queue and assign
         a container with CPU/RAM sized by priority and memory estimate.
    """
    def _prio_rank(prio):
        # Lower is higher priority
        if prio == Priority.QUERY:
            return 0
        if prio == Priority.INTERACTIVE:
            return 1
        return 2  # Priority.BATCH_PIPELINE and others

    def _queue_for_priority(prio):
        if prio == Priority.QUERY:
            return s.q_query
        if prio == Priority.INTERACTIVE:
            return s.q_interactive
        return s.q_batch

    def _op_key(pipeline_id, op):
        # Try stable identifiers when available; fall back to repr(op).
        op_id = getattr(op, "op_id", None)
        if op_id is None:
            op_id = getattr(op, "operator_id", None)
        if op_id is None:
            op_id = repr(op)
        return (pipeline_id, str(op_id))

    def _is_oom_error(err):
        if err is None:
            return False
        msg = str(err).lower()
        return ("oom" in msg) or ("out of memory" in msg) or ("out-of-memory" in msg)

    def _get_mem_est_gb(op):
        # Estimator interface: op.estimate.mem_peak_gb may exist (float or None).
        est_obj = getattr(op, "estimate", None)
        if est_obj is None:
            return None
        return getattr(est_obj, "mem_peak_gb", None)

    def _cpu_target(pool, prio, avail_cpu):
        # Simple CPU sizing: bias higher priorities to get more CPU (latency),
        # but keep at least 1 vCPU and never exceed available.
        max_cpu = getattr(pool, "max_cpu_pool", avail_cpu)
        if prio == Priority.QUERY:
            frac = 0.60
        elif prio == Priority.INTERACTIVE:
            frac = 0.45
        else:
            frac = 0.30

        tgt = max(1.0, float(max_cpu) * frac)
        return max(0.0, min(float(avail_cpu), tgt))

    def _ram_target(pool, prio, op, pipeline_id, avail_ram):
        # Use estimate if present; otherwise choose a small-but-not-tiny default.
        # Apply an OOM-driven multiplicative backoff per op.
        max_ram = getattr(pool, "max_ram_pool", avail_ram)

        est = _get_mem_est_gb(op)
        if est is None:
            # Default: a modest chunk of the pool, tuned smaller for batch to pack more.
            if prio == Priority.QUERY:
                base = max(1.0, float(max_ram) * 0.25)
            elif prio == Priority.INTERACTIVE:
                base = max(1.0, float(max_ram) * 0.20)
            else:
                base = max(1.0, float(max_ram) * 0.15)
        else:
            # Aggressive allocation near estimate; tiny safety margin to reduce OOM frequency.
            base = max(0.5, float(est) * 1.05)

        mult = s.op_ram_mult.get(_op_key(pipeline_id, op), 1.0)
        tgt = base * mult

        # Clamp to what we can actually allocate.
        tgt = min(float(avail_ram), tgt)

        # If we can't allocate at least something meaningful, return 0 to indicate "no fit".
        if tgt < 0.25:
            return 0.0
        return tgt

    # Enqueue new pipelines.
    for p in pipelines:
        _queue_for_priority(p.priority).append(p)

    # Process results; on OOM, increase per-op RAM multiplier and allow retry.
    if results:
        for r in results:
            if getattr(r, "failed", None) and r.failed() and _is_oom_error(getattr(r, "error", None)):
                # Mark the pipeline as retryable (we'll keep it in queues).
                # Increase RAM multiplier for each failed op in this result.
                for op in getattr(r, "ops", []) or []:
                    k = _op_key(getattr(r, "pipeline_id", None), op) if hasattr(r, "pipeline_id") else None
                    # If result doesn't include pipeline_id, fall back to using (None, op_id).
                    if k is None:
                        k = (None, _op_key(None, op)[1])
                    old = s.op_ram_mult.get(k, 1.0)
                    # Exponential backoff, capped to avoid runaway. (Aggressive is okay in simulator.)
                    s.op_ram_mult[k] = min(old * 1.6, 32.0)

                # Best-effort: record retryable by scanning queues later; also keep a soft flag.
                # (Some simulator versions may not expose pipeline_id in results.)
                s.retryable_pipelines.add(getattr(r, "pipeline_id", None))

    # Early exit if no changes.
    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    # Helper: pop next pipeline from highest priority queue that still has work.
    def _next_pipeline_from_queue(q):
        # Rotate until we find a pipeline that is not done; return None if all are done.
        # Limit rotations to queue length to avoid infinite loops.
        for _ in range(len(q)):
            p = q.pop(0)
            status = p.runtime_status()
            if status.is_pipeline_successful():
                continue
            return p
        return None

    # Try scheduling on each pool.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)

        if avail_cpu <= 0.0 or avail_ram <= 0.0:
            continue

        # Keep assigning while we have capacity.
        while avail_cpu > 0.0 and avail_ram > 0.0:
            # Pick next pipeline by priority.
            pipeline = None
            for q in (s.q_query, s.q_interactive, s.q_batch):
                pipeline = _next_pipeline_from_queue(q)
                if pipeline is not None:
                    break

            if pipeline is None:
                break  # nothing runnable

            status = pipeline.runtime_status()

            # If there are failures, only keep it if it is retryable (OOM) OR has assignable FAILED ops.
            # Otherwise, drop it to avoid infinite retries on non-OOM errors.
            if status.state_counts[OperatorState.FAILED] > 0:
                # If we didn't see a specific OOM result, we still allow retrying FAILED ops
                # (OOM is the common case). This is still safer than immediate drop.
                pass

            # Get one ready op (parents complete).
            op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
            if not op_list:
                # Not ready now (waiting on parents or already running). Requeue and move on.
                _queue_for_priority(pipeline.priority).append(pipeline)
                # Avoid tight loop if only blocked pipelines exist.
                break

            op = op_list[0]
            cpu = _cpu_target(pool, pipeline.priority, avail_cpu)
            ram = _ram_target(pool, pipeline.priority, op, pipeline.pipeline_id, avail_ram)

            # If we can't fit, stop trying this pool (packing is simple).
            if cpu <= 0.0 or ram <= 0.0:
                # Requeue and stop on this pool to avoid thrash.
                _queue_for_priority(pipeline.priority).append(pipeline)
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

            avail_cpu -= float(cpu)
            avail_ram -= float(ram)

            # Requeue pipeline for round-robin fairness within its priority.
            _queue_for_priority(pipeline.priority).append(pipeline)

    return suspensions, assignments
