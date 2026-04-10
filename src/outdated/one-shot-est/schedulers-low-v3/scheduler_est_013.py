# policy_key: scheduler_est_013
# reasoning_effort: low
# model: gpt-5.2-2025-12-11
# llm_cost: 0.039465
# generation_seconds: 42.73
# generated_at: 2026-04-03T01:13:08.918384
@register_scheduler_init(key="scheduler_est_013")
def scheduler_est_013_init(s):
    """Priority-aware, estimator-guided FIFO with simple reservations.

    Incremental improvements over naive FIFO:
      1) Separate waiting queues by priority (QUERY > INTERACTIVE > BATCH).
      2) Avoid starving high-priority work by reserving some pool headroom when any
         high-priority runnable work is waiting.
      3) Use per-operator memory peak estimate (if present) to right-size RAM.
      4) On failures (likely OOM), retry by increasing RAM for that operator.

    Notes:
      - This policy does not attempt preemption because the minimal example API
        does not expose a reliable list of currently-running containers.
      - We keep the policy conservative and easy to iterate on.
    """
    # Priority queues (FIFO within each priority).
    s.q_query = []
    s.q_interactive = []
    s.q_batch = []

    # Per-operator retry state: how many times we've bumped RAM.
    # Keyed by (pipeline_id, id(op)) to be stable within a simulation run.
    s.op_ram_bump = {}

    # Keep a small, per-pipeline "seen" set to reduce duplicate enqueues in a tick.
    s._enqueued_this_tick = set()


def _prio_rank(priority):
    # Higher is more important.
    if priority == Priority.QUERY:
        return 3
    if priority == Priority.INTERACTIVE:
        return 2
    return 1  # Priority.BATCH_PIPELINE and anything else


def _queue_for(s, priority):
    if priority == Priority.QUERY:
        return s.q_query
    if priority == Priority.INTERACTIVE:
        return s.q_interactive
    return s.q_batch


def _iter_queues_in_priority_order(s):
    # Highest priority first.
    return (s.q_query, s.q_interactive, s.q_batch)


def _has_waiting_high_prio_runnable(s):
    # "Runnable" approximation: if any high-priority pipelines are waiting at all.
    # We keep it simple; the scheduler will verify runnable ops when popped.
    return (len(s.q_query) > 0) or (len(s.q_interactive) > 0)


def _get_mem_estimate_gb(op):
    # Estimator interface: op.estimate.mem_peak_gb (float or None)
    est = None
    try:
        est = op.estimate.mem_peak_gb
    except Exception:
        est = None
    if est is None:
        return None
    try:
        est = float(est)
    except Exception:
        return None
    if est <= 0:
        return None
    return est


def _ram_request_gb(s, pipeline_id, op, pool, priority):
    # Baseline RAM:
    # - Prefer estimator when available.
    # - Otherwise choose a small fraction of the pool depending on priority.
    est = _get_mem_estimate_gb(op)
    if est is not None:
        # Tight allocation around estimate; rely on retry-on-OOM for correction.
        base = max(0.25, est * 1.10)
    else:
        # No estimate: prioritize latency for high-prio with slightly higher baseline,
        # but keep it modest to avoid wasting RAM.
        if priority == Priority.QUERY:
            base = max(0.5, pool.max_ram_pool * 0.15)
        elif priority == Priority.INTERACTIVE:
            base = max(0.5, pool.max_ram_pool * 0.20)
        else:
            base = max(0.5, pool.max_ram_pool * 0.25)

    bump = s.op_ram_bump.get((pipeline_id, id(op)), 0)
    # Exponential-ish backoff on RAM for retries.
    # 0 -> 1.0x, 1 -> 1.5x, 2 -> 2.25x, 3 -> 3.4x ...
    factor = 1.0
    for _ in range(bump):
        factor *= 1.5

    req = base * factor
    # Clamp to pool capacity.
    req = min(req, pool.max_ram_pool)
    # Always request at least a small amount to avoid degenerate 0 allocations.
    req = max(req, 0.25)
    return req


def _cpu_request(s, pool, avail_cpu, priority, high_prio_waiting):
    # CPU sizing:
    # - QUERY gets a larger share to reduce latency.
    # - INTERACTIVE gets moderate share.
    # - BATCH gets smaller share when high-priority is waiting; otherwise may use more.
    if priority == Priority.QUERY:
        cap = max(1.0, pool.max_cpu_pool * 0.75)
    elif priority == Priority.INTERACTIVE:
        cap = max(1.0, pool.max_cpu_pool * 0.50)
    else:
        cap = max(1.0, pool.max_cpu_pool * (0.25 if high_prio_waiting else 0.60))

    # Don't allocate more than available.
    cpu = min(avail_cpu, cap)
    # Also ensure at least 1 vCPU if possible.
    if avail_cpu >= 1.0:
        cpu = max(1.0, cpu)
    else:
        cpu = 0.0
    return cpu


def _enqueue_new_pipelines(s, pipelines):
    # Avoid enqueuing the same pipeline multiple times in the same tick.
    for p in pipelines:
        key = (p.pipeline_id, p.priority)
        if key in s._enqueued_this_tick:
            continue
        s._enqueued_this_tick.add(key)
        _queue_for(s, p.priority).append(p)


def _handle_results_update_retry_state(s, results):
    # If an op failed, bump its RAM for next retry attempt.
    # We do not attempt to parse exact OOM error types; we simply bump on any failure.
    for r in results:
        try:
            failed = r.failed()
        except Exception:
            failed = False
        if not failed:
            continue
        pid = getattr(r, "pipeline_id", None)
        # pipeline_id may not exist on ExecutionResult; use ops to infer keying.
        # We'll bump for each op using (unknown pid -> None) which still helps within tick.
        for op in getattr(r, "ops", []) or []:
            k = (pid, id(op))
            s.op_ram_bump[k] = min(10, s.op_ram_bump.get(k, 0) + 1)


def _pop_next_runnable_pipeline(s):
    # Pop the next pipeline in strict priority FIFO order.
    for q in _iter_queues_in_priority_order(s):
        while q:
            p = q.pop(0)
            status = p.runtime_status()
            # Drop completed pipelines.
            if status.is_pipeline_successful():
                continue
            return p
    return None


@register_scheduler(key="scheduler_est_013")
def scheduler_est_013_scheduler(s, results, pipelines):
    """
    Priority-aware scheduler with estimator-guided RAM sizing and retry-on-failure.

    Key behavior:
      - Always try to schedule QUERY first, then INTERACTIVE, then BATCH.
      - When any high-priority work is waiting, restrict batch CPU/RAM usage by
        reserving headroom (implemented via per-assignment caps).
      - Assign runnable ops whose parents are complete; retry FAILED ops with more RAM.
      - Attempt to fill each pool with as many single-op assignments as resources allow.
    """
    # Update queues with new arrivals and update retry state from results.
    s._enqueued_this_tick = set()
    _enqueue_new_pipelines(s, pipelines)
    _handle_results_update_retry_state(s, results)

    # Early exit if nothing to do.
    if not pipelines and not results and not (s.q_query or s.q_interactive or s.q_batch):
        return [], []

    suspensions = []
    assignments = []

    # Determine if we should protect headroom for high-priority.
    high_prio_waiting = _has_waiting_high_prio_runnable(s)

    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)

        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # Try to keep scheduling until we can't fit even the smallest reasonable container.
        # This is still simple packing: one operator per assignment.
        iterations = 0
        while iterations < 128:
            iterations += 1
            if avail_cpu <= 0 or avail_ram <= 0:
                break

            pipeline = _pop_next_runnable_pipeline(s)
            if pipeline is None:
                break

            status = pipeline.runtime_status()

            # Skip pipelines with any failures that are not retryable in this simplified policy.
            # (FAILED ops are retryable as they are included in ASSIGNABLE_STATES.)
            # We just rely on get_ops(ASSIGNABLE_STATES, ...) and requeue otherwise.
            op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
            if not op_list:
                # Not runnable now; requeue at tail of its priority queue.
                _queue_for(s, pipeline.priority).append(pipeline)
                continue

            op = op_list[0]

            # Compute requested resources for this op.
            cpu_req = _cpu_request(s, pool, avail_cpu, pipeline.priority, high_prio_waiting)
            if cpu_req <= 0:
                # No CPU available; requeue and stop filling this pool.
                _queue_for(s, pipeline.priority).append(pipeline)
                break

            ram_req = _ram_request_gb(s, pipeline.pipeline_id, op, pool, pipeline.priority)

            # If we can't fit this op's RAM, requeue and stop (don't spin).
            # In a more advanced policy we'd try another pool or a smaller op, but keep simple here.
            if ram_req > avail_ram:
                _queue_for(s, pipeline.priority).append(pipeline)
                break

            # If high-priority is waiting, avoid letting batch consume the last headroom:
            # reserve 10% of pool for high-priority (soft reservation).
            if high_prio_waiting and pipeline.priority == Priority.BATCH_PIPELINE:
                reserve_cpu = pool.max_cpu_pool * 0.10
                reserve_ram = pool.max_ram_pool * 0.10
                if (avail_cpu - cpu_req) < reserve_cpu or (avail_ram - ram_req) < reserve_ram:
                    # Defer batch; requeue and try another pipeline (likely higher prio).
                    _queue_for(s, pipeline.priority).append(pipeline)
                    # Try next pipeline instead of breaking; but avoid infinite loops if only batch exists.
                    if not (s.q_query or s.q_interactive):
                        break
                    continue

            assignments.append(
                Assignment(
                    ops=[op],
                    cpu=cpu_req,
                    ram=ram_req,
                    priority=pipeline.priority,
                    pool_id=pool_id,
                    pipeline_id=pipeline.pipeline_id,
                )
            )

            # Update remaining headroom for this pool for subsequent assignments this tick.
            avail_cpu -= cpu_req
            avail_ram -= ram_req

            # Requeue pipeline if it likely has more work remaining.
            # (We don't try to inspect deeper; just keep it in circulation.)
            _queue_for(s, pipeline.priority).append(pipeline)

    return suspensions, assignments
