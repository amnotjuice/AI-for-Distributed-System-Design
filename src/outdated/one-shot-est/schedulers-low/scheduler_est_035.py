# policy_key: scheduler_est_035
# reasoning_effort: low
# model: gpt-5.2-2025-12-11
# llm_cost: 0.039976
# generation_seconds: 38.89
# generated_at: 2026-04-01T00:08:08.544827
@register_scheduler_init(key="scheduler_est_035")
def scheduler_est_035_init(s):
    """
    Priority-aware FIFO with conservative resource sizing + OOM-aware RAM floors.

    Improvements over naive FIFO:
    - Separate queues by priority (QUERY/INTERACTIVE ahead of BATCH).
    - Size RAM using op.estimate.mem_peak_gb as a floor; add small safety margin.
    - Avoid allocating an entire pool to a single op; cap per-op CPU to enable concurrency.
    - Simple OOM backoff: if an op fails with OOM-like error, increase its future RAM floor.
    - Light fairness: after serving several high-priority ops, allow a batch op if ready.
    """
    from collections import deque

    # Priority-separated waiting queues
    s.q_query = deque()
    s.q_interactive = deque()
    s.q_batch = deque()

    # Keep enqueue order stable and prevent unbounded growth from completed pipelines
    s.all_seen_pipeline_ids = set()

    # OOM backoff: key -> multiplier to apply to future RAM floors
    # Keying by (pipeline_id, op_signature) because op objects may not be hashable/stable.
    s.oom_ram_multiplier = {}

    # Simple tick counter for lightweight fairness and optional future aging
    s.tick = 0

    # Fairness knob: serve up to this many high-priority ops before forcing a batch attempt
    s.high_before_batch = 6
    s.high_served_since_batch = 0


@register_scheduler(key="scheduler_est_035")
def scheduler_est_035_scheduler(s, results, pipelines):
    """
    Priority-aware, estimate-aware scheduling loop.

    For each pool, repeatedly:
    - Pick next ready op from highest available priority queue (with mild batch fairness).
    - Allocate RAM as max(estimated_peak, small_min) * oom_multiplier, with safety margin.
    - Allocate CPU capped to a fraction of pool capacity to allow multiple concurrent ops.
    """
    # Local imports (allowed) and helpers
    from collections import deque

    def _is_oom_error(err):
        if not err:
            return False
        e = str(err).lower()
        return ("oom" in e) or ("out of memory" in e) or ("memory" in e and "alloc" in e)

    def _prio_rank(p):
        # Lower is higher priority
        if p == Priority.QUERY:
            return 0
        if p == Priority.INTERACTIVE:
            return 1
        return 2

    def _get_queue_for_priority(priority):
        if priority == Priority.QUERY:
            return s.q_query
        if priority == Priority.INTERACTIVE:
            return s.q_interactive
        return s.q_batch

    def _pipeline_done_or_failed(pipeline):
        status = pipeline.runtime_status()
        if status.is_pipeline_successful():
            return True
        # If any op failed, we still may want to retry (ASSIGNABLE includes FAILED),
        # but we drop pipelines that are "terminally failed" only if user chooses;
        # here we keep them to allow retry with larger RAM.
        return False

    def _next_ready_ops_from_pipeline(pipeline, k=1):
        status = pipeline.runtime_status()
        # Only schedule ops whose parents are complete, and that are in ASSIGNABLE states.
        return status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:k]

    def _op_signature(op):
        # Best-effort stable signature without relying on specific fields.
        # repr(op) can be large; str(op) might be friendlier; keep both small-ish.
        sig = None
        try:
            sig = getattr(op, "op_id", None)
        except Exception:
            sig = None
        if sig is None:
            try:
                sig = getattr(op, "operator_id", None)
            except Exception:
                sig = None
        if sig is None:
            try:
                sig = getattr(op, "name", None)
            except Exception:
                sig = None
        if sig is None:
            try:
                sig = str(op)
            except Exception:
                sig = repr(op)
        return sig

    def _ram_floor_gb_for_op(pipeline_id, op):
        # Base minimum to avoid absurdly tiny allocations if estimator is missing.
        base_min = 0.5  # GB
        est = None
        try:
            est = getattr(getattr(op, "estimate", None), "mem_peak_gb", None)
        except Exception:
            est = None
        if est is None:
            est = base_min
        # Apply OOM multiplier if we have seen OOM failures for this op before.
        key = (pipeline_id, _op_signature(op))
        mult = s.oom_ram_multiplier.get(key, 1.0)
        # Safety margin to reduce near-threshold OOMs (extra RAM is "free" per spec).
        safety = 1.15
        return max(base_min, float(est)) * float(mult) * safety

    def _choose_cpu_for_op(pool, avail_cpu):
        # Cap per-op CPU to allow multiple concurrent ops and reduce head-of-line blocking.
        # Use at least 1 vCPU if available.
        # If pool is small, let the op take more.
        max_per_op = max(1.0, 0.5 * float(pool.max_cpu_pool))
        return max(1.0, min(float(avail_cpu), max_per_op))

    def _choose_pool_order_for_priority(priority):
        # Prefer pool 0 for highest priority if multiple pools exist; otherwise sort by headroom.
        n = s.executor.num_pools
        if n <= 1:
            return [0]
        pool_ids = list(range(n))

        if priority in (Priority.QUERY, Priority.INTERACTIVE):
            # First try pool 0 (often treated as "interactive"), then others by available CPU.
            rest = [i for i in pool_ids if i != 0]
            rest.sort(key=lambda i: (s.executor.pools[i].avail_cpu_pool, s.executor.pools[i].avail_ram_pool), reverse=True)
            return [0] + rest
        else:
            # For batch, pick the pool with most headroom first to avoid interfering with interactive.
            pool_ids.sort(key=lambda i: (s.executor.pools[i].avail_cpu_pool, s.executor.pools[i].avail_ram_pool), reverse=True)
            return pool_ids

    def _pop_next_pipeline_with_ready_op(q):
        # Rotate through queue until we find a pipeline with a ready op, keeping FIFO order.
        # Returns (pipeline, op_list) or (None, []).
        for _ in range(len(q)):
            pipeline = q.popleft()
            # Drop completed pipelines to keep queues clean
            if _pipeline_done_or_failed(pipeline):
                continue
            op_list = _next_ready_ops_from_pipeline(pipeline, k=1)
            # Keep pipeline in queue regardless; if no ready op, push to back.
            q.append(pipeline)
            if op_list:
                return pipeline, op_list
        return None, []

    def _select_next_pipeline_and_ops():
        # Mild fairness: after several high-priority ops, attempt batch once if possible.
        # Otherwise strict priority: QUERY -> INTERACTIVE -> BATCH.
        if s.high_served_since_batch >= s.high_before_batch:
            p, ops = _pop_next_pipeline_with_ready_op(s.q_batch)
            if p is not None and ops:
                s.high_served_since_batch = 0
                return p, ops

        p, ops = _pop_next_pipeline_with_ready_op(s.q_query)
        if p is not None and ops:
            s.high_served_since_batch += 1
            return p, ops

        p, ops = _pop_next_pipeline_with_ready_op(s.q_interactive)
        if p is not None and ops:
            s.high_served_since_batch += 1
            return p, ops

        p, ops = _pop_next_pipeline_with_ready_op(s.q_batch)
        if p is not None and ops:
            s.high_served_since_batch = 0
            return p, ops

        return None, []

    # ---- Update state from new pipelines and results ----
    s.tick += 1

    # Enqueue new pipelines by priority, preserving arrival order within each class.
    for p in pipelines:
        # Avoid accidental double-enqueue if simulator sends same pipeline object again.
        # (If that assumption is wrong, this still keeps at-most-once enqueue.)
        if p.pipeline_id in s.all_seen_pipeline_ids:
            continue
        s.all_seen_pipeline_ids.add(p.pipeline_id)
        _get_queue_for_priority(p.priority).append(p)

    # Update OOM backoff from execution results.
    # If an op failed with OOM, increase multiplier (capped) for that op.
    for r in results:
        if not getattr(r, "failed", lambda: False)():
            continue
        if not _is_oom_error(getattr(r, "error", None)):
            continue
        try:
            pipeline_id = getattr(r, "pipeline_id", None)
        except Exception:
            pipeline_id = None
        # If pipeline_id isn't present on result, we can't key per-pipeline; fallback to per-op repr.
        # However, examples show Assignment includes pipeline_id; many simulators propagate it.
        if pipeline_id is None:
            pipeline_id = "__unknown_pipeline__"

        ops = getattr(r, "ops", []) or []
        for op in ops:
            key = (pipeline_id, _op_signature(op))
            old = s.oom_ram_multiplier.get(key, 1.0)
            # Increase aggressively but cap to avoid runaway.
            s.oom_ram_multiplier[key] = min(4.0, old * 1.5)

    # Early exit if nothing changed
    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    # ---- Scheduling loop per pool ----
    # We iterate pools in an order that generally protects interactive work.
    # We'll compute a global "desired priority" next op, then place it in a suitable pool.
    # This keeps the policy small/simple while still being priority-aware.
    # To avoid infinite loops, bound the number of assignment attempts.
    max_attempts = 64
    attempts = 0

    while attempts < max_attempts:
        attempts += 1

        pipeline, op_list = _select_next_pipeline_and_ops()
        if pipeline is None or not op_list:
            break

        op = op_list[0]
        ram_need = _ram_floor_gb_for_op(pipeline.pipeline_id, op)

        placed = False
        for pool_id in _choose_pool_order_for_priority(pipeline.priority):
            pool = s.executor.pools[pool_id]
            avail_cpu = float(pool.avail_cpu_pool)
            avail_ram = float(pool.avail_ram_pool)

            # Must fit at least minimum resources.
            if avail_cpu < 1.0 or avail_ram < ram_need:
                continue

            cpu = _choose_cpu_for_op(pool, avail_cpu)
            ram = min(avail_ram, max(ram_need, 0.0))

            # Create assignment (single op at a time is safest/lowest risk to start).
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
            placed = True
            break

        if not placed:
            # Couldn't place due to RAM/CPU constraints; rotate pipeline to avoid blocking.
            # We keep it in its priority queue (already appended during pop/rotate).
            # If it's high-priority and can't fit, the simulator will keep it waiting.
            # A future enhancement would add preemption here.
            continue

    return suspensions, assignments
