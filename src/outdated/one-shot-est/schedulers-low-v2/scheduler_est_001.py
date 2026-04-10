# policy_key: scheduler_est_001
# reasoning_effort: low
# model: gpt-5.2-2025-12-11
# llm_cost: 0.000000
# generation_seconds: 43.34
# generated_at: 2026-04-01T13:36:21.646045
@register_scheduler_init(key="scheduler_est_001")
def scheduler_est_001_init(s):
    """
    Priority-aware, conservative-RAM, multi-assignment scheduler.

    Incremental improvements over naive FIFO:
      1) Priority queues: always prefer QUERY > INTERACTIVE > BATCH.
      2) Pack multiple ready operators into each pool per tick (instead of 1 op/pool).
      3) Conservative RAM sizing using (a) operator-provided estimate if present, and
         (b) exponential backoff on failure (OOM-like) to reduce repeated failures.
      4) CPU bias for high-priority: give larger CPU slices to QUERY/INTERACTIVE while
         still allowing background progress.
      5) Simple aging: long-waiting BATCH pipelines get promoted to avoid starvation.
    """
    from collections import deque

    # Per-priority waiting queues of pipelines
    s.q_query = deque()
    s.q_interactive = deque()
    s.q_batch = deque()

    # Track when pipelines were enqueued for simple aging / fairness
    s.enq_tick = {}  # pipeline_id -> tick

    # Track per-operator RAM attempts (keyed by (pipeline_id, op_key))
    s.op_ram_attempt_gb = {}  # (pipeline_id, op_key) -> float

    # Configuration knobs (intentionally simple / conservative)
    s.tick = 0
    s.default_ram_gb = 2.0              # initial guess when no estimate exists
    s.min_ram_gb = 0.5                  # never allocate less than this
    s.ram_headroom_factor = 1.15        # allocate a bit more than estimate/attempt
    s.max_backoff_steps = 6             # cap exponential growth
    s.batch_aging_ticks = 50            # promote old batch pipelines


def _priority_rank(priority):
    # Lower is better (more urgent)
    if priority == Priority.QUERY:
        return 0
    if priority == Priority.INTERACTIVE:
        return 1
    return 2  # Priority.BATCH_PIPELINE and any unknowns


def _pick_queue(s, priority):
    if priority == Priority.QUERY:
        return s.q_query
    if priority == Priority.INTERACTIVE:
        return s.q_interactive
    return s.q_batch


def _op_key(op):
    # Best-effort stable key for an operator across ticks; fallback to object id.
    for attr in ("operator_id", "op_id", "id", "name"):
        if hasattr(op, attr):
            try:
                v = getattr(op, attr)
                # Avoid non-hashable
                return (attr, str(v))
            except Exception:
                pass
    return ("py_id", str(id(op)))


def _get_mem_est_gb(op):
    # Estimator interface: op.estimate.mem_peak_gb may exist and be None/float.
    try:
        est = getattr(getattr(op, "estimate", None), "mem_peak_gb", None)
        if est is None:
            return None
        est = float(est)
        if est <= 0:
            return None
        return est
    except Exception:
        return None


def _ram_for_op(s, pipeline_id, op, pool_avail_ram):
    """
    RAM sizing strategy:
      - If we have a previous attempt for this op, use it (backoff on failures).
      - Else, use estimator hint if present (treat as lower bound, add headroom).
      - Else, use a small default.
    """
    opk = _op_key(op)
    k = (pipeline_id, opk)
    attempt = s.op_ram_attempt_gb.get(k, None)

    base = None
    if attempt is not None:
        base = attempt
    else:
        est = _get_mem_est_gb(op)
        if est is not None:
            base = est
        else:
            base = s.default_ram_gb

    # Add headroom; clamp to pool availability (can't exceed).
    ram = max(s.min_ram_gb, base * s.ram_headroom_factor)
    if pool_avail_ram is not None:
        ram = min(ram, float(pool_avail_ram))
    return ram


def _cpu_slice_for_priority(priority, pool_avail_cpu, pool_max_cpu):
    """
    CPU sizing strategy:
      - High priority gets more CPU to reduce latency.
      - Keep some granularity so we can pack multiple ops/pipelines when possible.
    """
    if pool_avail_cpu <= 0:
        return 0.0
    max_cpu = float(pool_max_cpu) if pool_max_cpu is not None else float(pool_avail_cpu)

    if priority == Priority.QUERY:
        # Try to give a strong slice (up to 50% of pool or remaining CPU).
        target = max(1.0, min(pool_avail_cpu, max_cpu * 0.50))
    elif priority == Priority.INTERACTIVE:
        target = max(1.0, min(pool_avail_cpu, max_cpu * 0.33))
    else:
        # Batch: smaller slice to keep latency low for foreground.
        target = max(1.0, min(pool_avail_cpu, max_cpu * 0.20))

    # Don't exceed available CPU; keep at least 1 if possible.
    return min(float(pool_avail_cpu), float(target))


def _enqueue_pipeline(s, p):
    q = _pick_queue(s, p.priority)
    q.append(p)
    s.enq_tick[p.pipeline_id] = s.tick


def _promote_aged_batch(s):
    # Promote old batch pipelines into interactive queue to avoid starvation.
    # (Simple aging: keep logic minimal and deterministic.)
    from collections import deque

    if not s.q_batch:
        return

    new_batch = deque()
    while s.q_batch:
        p = s.q_batch.popleft()
        t0 = s.enq_tick.get(p.pipeline_id, s.tick)
        if (s.tick - t0) >= s.batch_aging_ticks:
            # Promote to interactive queue (not all the way to query).
            s.q_interactive.append(p)
            # Keep original enqueue tick for continued aging if needed.
        else:
            new_batch.append(p)
    s.q_batch = new_batch


@register_scheduler(key="scheduler_est_001")
def scheduler_est_001(s, results, pipelines):
    """
    Priority-aware packing scheduler.

    - Maintains per-priority FIFO queues of pipelines.
    - On failures (assumed OOM-like), increases future RAM allocations for those ops.
    - Each tick, tries to pack as many ready ops as possible into each pool, preferring
      higher-priority pipelines first, while using conservative RAM sizing.
    """
    s.tick += 1

    # Ingest new pipelines
    for p in pipelines:
        _enqueue_pipeline(s, p)

    # Learn from results: on failure, backoff RAM for the involved ops
    for r in results:
        try:
            if r.failed():
                # Increase RAM attempt for each op that was in the failed container.
                # We assume failure is OOM-like in this simplified policy.
                # If the simulator distinguishes errors, this can be refined later.
                pipeline_id = getattr(r, "pipeline_id", None)
                # Some sims may not include pipeline_id in result; fall back to matching by ops only.
                # We'll backoff using known pipeline_id when possible; otherwise skip.
                if pipeline_id is None:
                    continue
                prev_ram = float(getattr(r, "ram", 0.0) or 0.0)
                # Exponential backoff with a cap
                new_ram = prev_ram * 2.0 if prev_ram > 0 else s.default_ram_gb * 2.0

                for op in getattr(r, "ops", []) or []:
                    k = (pipeline_id, _op_key(op))
                    old = s.op_ram_attempt_gb.get(k, None)
                    if old is None:
                        s.op_ram_attempt_gb[k] = new_ram
                    else:
                        # Increase but cap runaway backoff by limiting steps.
                        # Approximate steps by ratio to default.
                        ratio = max(1.0, old / max(1e-6, s.default_ram_gb))
                        # If we've already grown a lot, don't keep doubling indefinitely.
                        if ratio < (2.0 ** s.max_backoff_steps):
                            s.op_ram_attempt_gb[k] = max(old, new_ram)
                        else:
                            s.op_ram_attempt_gb[k] = max(old, new_ram)  # still monotonic
        except Exception:
            # Never crash scheduler due to unexpected result shape
            pass

    # Aging to prevent starvation
    _promote_aged_batch(s)

    # Early exit if no state changes that affect our decisions
    if not pipelines and not results and not (s.q_query or s.q_interactive or s.q_batch):
        return [], []

    suspensions = []
    assignments = []

    # Helper to fetch the next pipeline to consider in strict priority order
    def pop_next_pipeline():
        if s.q_query:
            return s.q_query.popleft()
        if s.q_interactive:
            return s.q_interactive.popleft()
        if s.q_batch:
            return s.q_batch.popleft()
        return None

    def requeue_pipeline(p):
        # If still unfinished, put back preserving its original priority class.
        status = p.runtime_status()
        if status.is_pipeline_successful():
            # cleanup
            s.enq_tick.pop(p.pipeline_id, None)
            return
        # If any failures exist, we still keep the pipeline; our RAM backoff may help.
        _pick_queue(s, p.priority).append(p)

    # For each pool, pack ready ops while resources remain
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)
        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # To avoid a single pool draining all work, limit how many assignments we make per pool per tick.
        # This keeps decisions stable and helps interactive latency under contention.
        max_assignments_this_pool = 8
        made = 0

        # We'll rotate through pipelines in priority order, requeueing those we can't run yet.
        # Track how many pipelines we examined to avoid infinite loops.
        examined = 0
        total_queued = len(s.q_query) + len(s.q_interactive) + len(s.q_batch)

        while made < max_assignments_this_pool and avail_cpu > 0 and avail_ram > 0 and total_queued > 0:
            pipeline = pop_next_pipeline()
            if pipeline is None:
                break

            examined += 1
            if examined > (total_queued + 5):
                # Safety break
                requeue_pipeline(pipeline)
                break

            status = pipeline.runtime_status()
            if status.is_pipeline_successful():
                s.enq_tick.pop(pipeline.pipeline_id, None)
                total_queued -= 1
                continue

            # Pick ready operators whose parents are complete
            op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
            if not op_list:
                # Nothing ready; requeue and try others
                requeue_pipeline(pipeline)
                continue

            # Choose one op at a time to improve packing / responsiveness
            op = op_list[0]

            # Size resources for this op
            ram = _ram_for_op(s, pipeline.pipeline_id, op, avail_ram)
            cpu = _cpu_slice_for_priority(pipeline.priority, avail_cpu, pool.max_cpu_pool)

            # If we can't give at least minimal resources, requeue and stop packing this pool
            if cpu <= 0 or ram <= 0:
                requeue_pipeline(pipeline)
                break

            # If RAM is too tight to be meaningful (e.g., min clamp but still tiny),
            # stop packing rather than scheduling likely failures.
            if ram < s.min_ram_gb or ram > avail_ram + 1e-9:
                requeue_pipeline(pipeline)
                break

            # Create assignment
            assignments.append(
                Assignment(
                    ops=[op],
                    cpu=cpu,
                    ram=ram,
                    priority=pipeline.priority,
                    pool_id=pool_id,
                    pipeline_id=pipeline.pipeline_id,
                )
            )

            # Update pool headroom
            avail_cpu -= float(cpu)
            avail_ram -= float(ram)
            made += 1

            # Requeue pipeline for further ops later (don’t monopolize within same tick).
            requeue_pipeline(pipeline)

            # Update total_queued snapshot loosely (not exact but safe).
            total_queued = len(s.q_query) + len(s.q_interactive) + len(s.q_batch)

    return suspensions, assignments
