# policy_key: scheduler_est_021
# reasoning_effort: low
# model: gpt-5.2-2025-12-11
# llm_cost: 0.038593
# generation_seconds: 32.97
# generated_at: 2026-03-31T23:58:53.368573
@register_scheduler_init(key="scheduler_est_021")
def scheduler_est_021_init(s):
    """Priority-aware, estimate-aware scheduler (incremental improvement over naive FIFO).

    Main ideas (kept intentionally simple and robust):
      1) Maintain separate FIFO queues per priority to reduce head-of-line blocking for latency-critical work.
      2) Size RAM using op.estimate.mem_peak_gb (as a floor) to reduce OOMs; on failures, increase a per-op RAM floor.
      3) Bias CPU toward higher priorities (QUERY/INTERACTIVE) while still allowing batch progress when no high-priority work waits.
      4) Avoid dropping pipelines; requeue pipelines whose next runnable op isn't ready yet.

    Notes:
      - No preemption in this iteration (keeps behavior stable/understandable).
      - Assigns potentially multiple ops per pool per tick, but throttles BATCH when higher priorities are waiting.
    """
    from collections import deque

    # Separate queues to improve latency for high-priority workloads.
    s.q_query = deque()
    s.q_interactive = deque()
    s.q_batch = deque()

    # Per-(pipeline, op) RAM floor to mitigate repeated OOMs.
    # Keyed conservatively using repr(op) since operator IDs are not guaranteed in the interface contract.
    s.op_ram_floor_gb = {}  # (pipeline_id, op_key) -> ram_gb_floor

    # Optional per-(pipeline, op) CPU hint (not heavily used yet; reserved for future refinement).
    s.op_cpu_hint = {}  # (pipeline_id, op_key) -> cpu_hint


@register_scheduler(key="scheduler_est_021")
def scheduler_est_021_scheduler(s, results, pipelines):
    """
    Priority-first FIFO with RAM estimation + simple failure backoff.

    Returns:
        (suspensions, assignments)
    """
    # Local helpers kept inside function to avoid relying on external imports/module state.
    def _enqueue_pipeline(p):
        if p.priority == Priority.QUERY:
            s.q_query.append(p)
        elif p.priority == Priority.INTERACTIVE:
            s.q_interactive.append(p)
        else:
            s.q_batch.append(p)

    def _pick_queue():
        # Strict priority order to protect tail latency.
        if s.q_query:
            return s.q_query
        if s.q_interactive:
            return s.q_interactive
        return s.q_batch

    def _high_pri_waiting():
        return bool(s.q_query) or bool(s.q_interactive)

    def _op_key(op):
        # We don't assume stable IDs exist; repr(op) is usually stable enough in a deterministic simulator.
        return repr(op)

    def _est_mem_gb(op):
        # Use estimate as a RAM floor, if present; else be conservative.
        est = None
        try:
            est = getattr(getattr(op, "estimate", None), "mem_peak_gb", None)
        except Exception:
            est = None
        if est is None:
            return 1.0
        try:
            return float(est) if float(est) > 0 else 1.0
        except Exception:
            return 1.0

    def _cpu_target_for_priority(pool, priority):
        # Simple CPU bias for latency: give more CPU to high priority, less to background.
        max_cpu = getattr(pool, "max_cpu_pool", None)
        if max_cpu is None:
            # Fall back to "use what's available" if max is unknown.
            return None

        if priority == Priority.QUERY:
            frac = 0.75
        elif priority == Priority.INTERACTIVE:
            frac = 0.60
        else:
            frac = 0.30

        tgt = max(1.0, float(max_cpu) * frac)
        return tgt

    # --- Ingest new pipelines into queues ---
    for p in pipelines:
        _enqueue_pipeline(p)

    # Early exit if nothing to react to.
    if not pipelines and not results:
        return [], []

    # --- Update state from execution results (simple failure backoff) ---
    for r in results:
        if not getattr(r, "failed", lambda: False)():
            continue

        # Any failure is treated as a signal to increase RAM floor next try.
        # (In practice we'd inspect r.error for OOM vs other errors; kept simple here.)
        pid = getattr(r, "pipeline_id", None)
        if pid is None:
            # pipeline_id isn't guaranteed on result; use priority + container_id as fallback scoping.
            pid = ("unknown", r.priority)

        # r.ops is a list of operators that ran in the container.
        for op in getattr(r, "ops", []) or []:
            k = (pid, _op_key(op))
            prev = s.op_ram_floor_gb.get(k, 0.0)
            # Exponential backoff on the RAM we just tried.
            tried = float(getattr(r, "ram", 0.0) or 0.0)
            bumped = max(prev, tried * 2.0, 1.0)
            s.op_ram_floor_gb[k] = bumped

    suspensions = []
    assignments = []

    # --- Scheduling loop over pools ---
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = float(getattr(pool, "avail_cpu_pool", 0.0) or 0.0)
        avail_ram = float(getattr(pool, "avail_ram_pool", 0.0) or 0.0)

        if avail_cpu <= 0.0 or avail_ram <= 0.0:
            continue

        # Try to place multiple ops per pool if resources allow.
        # However, throttle batch if any high-priority work exists to reduce interference/latency.
        placed_something = True
        guard_iters = 0

        while placed_something and avail_cpu > 0.0 and avail_ram > 0.0 and guard_iters < 64:
            guard_iters += 1
            placed_something = False

            # Pick the best available queue, but optionally skip batch when high-priority is waiting.
            q = _pick_queue()
            if q is s.q_batch and _high_pri_waiting():
                break

            if not q:
                break

            # Pull a pipeline; if it's not runnable yet, requeue it (avoids HOL blocking).
            pipeline = q.popleft()
            status = pipeline.runtime_status()

            # Drop completed pipelines; drop pipelines with failures (no retries at pipeline level in this iteration).
            has_failures = status.state_counts[OperatorState.FAILED] > 0
            if status.is_pipeline_successful() or has_failures:
                continue

            # Find the next runnable operator(s) whose parents are complete.
            op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
            if not op_list:
                # Not runnable now; requeue at tail of same priority queue.
                _enqueue_pipeline(pipeline)
                continue

            op = op_list[0]

            # Compute RAM request as: max(estimate_floor, failure_backoff_floor), capped by pool availability.
            pid = pipeline.pipeline_id
            k = (pid, _op_key(op))
            est_floor = _est_mem_gb(op)
            backoff_floor = float(s.op_ram_floor_gb.get(k, 0.0) or 0.0)
            ram_req = max(est_floor, backoff_floor, 1.0)

            # If we can't satisfy the floor RAM now, requeue and stop trying in this pool (avoid thrash).
            if ram_req > avail_ram:
                _enqueue_pipeline(pipeline)
                break

            # Compute CPU request: bias by priority but don't exceed what is available.
            cpu_tgt = _cpu_target_for_priority(pool, pipeline.priority)
            if cpu_tgt is None:
                cpu_req = avail_cpu
            else:
                cpu_req = min(avail_cpu, float(cpu_tgt))

            # Ensure at least 1 CPU if any is available.
            if cpu_req <= 0.0:
                _enqueue_pipeline(pipeline)
                break

            assignment = Assignment(
                ops=op_list,
                cpu=cpu_req,
                ram=ram_req,
                priority=pipeline.priority,
                pool_id=pool_id,
                pipeline_id=pipeline.pipeline_id,
            )
            assignments.append(assignment)

            # Update local available resources (best-effort; simulator will enforce actual accounting).
            avail_cpu -= cpu_req
            avail_ram -= ram_req
            placed_something = True

            # Requeue the pipeline to allow its next ops to be scheduled in future ticks.
            _enqueue_pipeline(pipeline)

    return suspensions, assignments
