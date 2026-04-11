# policy_key: scheduler_low_045
# reasoning_effort: low
# model: gpt-5.2-2025-12-11
# llm_cost: 0.057579
# generation_seconds: 50.30
# generated_at: 2026-04-09T21:38:50.784050
@register_scheduler_init(key="scheduler_low_045")
def scheduler_low_045_init(s):
    """Priority-aware, failure-averse scheduler.

    Core ideas:
    - Maintain separate FIFO queues per priority (QUERY > INTERACTIVE > BATCH).
    - Conservative RAM sizing to reduce OOMs (OOMs are very costly due to 720s penalty).
    - On failures, assume likely under-provisioned RAM and retry the same operator with increased RAM (exponential backoff).
    - Avoid starvation via simple aging: if a pipeline waits "too long" in a lower-priority queue, allow it to be considered earlier.
    - Prefer pool placement by priority when multiple pools exist; otherwise share.
    """
    s.queues = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }

    # Per-operator RAM hint (bytes/GB units are whatever the simulator uses; we treat as abstract "ram units")
    # Keyed by (pipeline_id, op_key)
    s.ram_hint = {}

    # Track retries per (pipeline_id, op_key)
    s.retry_count = {}

    # Approximate "time" via scheduler ticks for simple aging (no wall clock exposed in the interface).
    s.tick = 0
    s.enqueue_tick = {}  # pipeline_id -> tick when last enqueued (for aging)

    # Optional: remember last seen priority for a pipeline_id (useful if pipeline objects are re-created)
    s.pipeline_priority = {}

    # Aging knobs (ticks)
    s.aging_interactive_over_batch = 30
    s.aging_query_over_interactive = 20

    # Retry knobs
    s.max_retries_per_op = 6
    s.ram_backoff_factor = 2.0
    s.ram_backoff_add = 1.0  # additive bump to avoid stalling when units are small

    # Baseline RAM fractions by priority (of pool max RAM)
    s.base_ram_frac = {
        Priority.QUERY: 0.55,
        Priority.INTERACTIVE: 0.40,
        Priority.BATCH_PIPELINE: 0.25,
    }

    # Baseline CPU targets by priority (caps per assignment); scheduler will also use opportunistic scale-up
    s.base_cpu_target = {
        Priority.QUERY: 4,
        Priority.INTERACTIVE: 2,
        Priority.BATCH_PIPELINE: 1,
    }


def _op_key_for_scheduler(op):
    """Best-effort stable-ish key for an operator without relying on imports."""
    for attr in ("op_id", "operator_id", "node_id", "id"):
        if hasattr(op, attr):
            try:
                return str(getattr(op, attr))
            except Exception:
                pass
    # Fallback: repr is often stable within a run
    try:
        return repr(op)
    except Exception:
        return str(id(op))


def _priority_order():
    return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]


def _preferred_pools_for_priority(num_pools, priority):
    """Return a list of pool_ids in preference order for a given priority."""
    if num_pools <= 1:
        return [0]
    if num_pools == 2:
        # pool0: QUERY+INTERACTIVE preferred, pool1: BATCH preferred
        if priority in (Priority.QUERY, Priority.INTERACTIVE):
            return [0, 1]
        return [1, 0]
    # 3+ pools: pool0 query, pool1 interactive, others batch; fall back to any
    if priority == Priority.QUERY:
        return [0] + [i for i in range(1, num_pools)]
    if priority == Priority.INTERACTIVE:
        pref = [1] if num_pools > 1 else [0]
        return pref + [i for i in range(num_pools) if i not in pref]
    # batch
    batch_prefs = [i for i in range(2, num_pools)] + [0, 1]
    # Ensure valid and unique
    seen = set()
    out = []
    for i in batch_prefs:
        if 0 <= i < num_pools and i not in seen:
            out.append(i)
            seen.add(i)
    if not out:
        out = list(range(num_pools))
    return out


def _compute_ram_request(s, pool, priority, pipeline_id, op):
    """Compute RAM request using per-op hint with conservative base sizing."""
    op_key = _op_key_for_scheduler(op)
    hint = s.ram_hint.get((pipeline_id, op_key), None)

    base = pool.max_ram_pool * s.base_ram_frac.get(priority, 0.30)
    # If we have a hint, trust it over base but never go below a small floor.
    ram = hint if hint is not None else base

    # Safety floor/ceil within pool bounds
    if ram < 1:
        ram = 1
    if ram > pool.max_ram_pool:
        ram = pool.max_ram_pool
    return ram


def _compute_cpu_request(s, pool, priority, avail_cpu, has_high_prio_backlog):
    """Compute CPU request with modest caps but opportunistic scaling for high priority."""
    base = s.base_cpu_target.get(priority, 1)
    # If there's no high-priority backlog, allow query to scale up a bit more to reduce latency.
    if priority == Priority.QUERY and not has_high_prio_backlog:
        base = max(base, min(int(pool.max_cpu_pool), 8))
    # Don't exceed what's available and ensure at least 1
    cpu = base
    if cpu < 1:
        cpu = 1
    if cpu > avail_cpu:
        cpu = avail_cpu
    # Some simulators treat fractional CPU; keep as-is if base is int but avail may be float
    if cpu <= 0:
        cpu = 0
    return cpu


def _pipeline_done_or_hopeless(pipeline):
    st = pipeline.runtime_status()
    return st.is_pipeline_successful()


def _ready_ops_for_pipeline(pipeline):
    st = pipeline.runtime_status()
    # Prefer ops whose parents are complete to respect DAG dependencies
    ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
    return ops or []


def _enqueue_pipeline(s, pipeline):
    s.pipeline_priority[pipeline.pipeline_id] = pipeline.priority
    s.queues[pipeline.priority].append(pipeline)
    s.enqueue_tick[pipeline.pipeline_id] = s.tick


def _dequeue_next_pipeline_with_ready_op(s):
    """Select next pipeline using priority + simple aging. Returns (pipeline, ready_ops) or (None, [])."""
    # Aging rules: promote BATCH to compete with INTERACTIVE after wait, and INTERACTIVE to compete with QUERY after wait.
    # We implement by occasionally scanning lower queues first if their head waited long enough.
    def waited_long_enough(p, threshold):
        t0 = s.enqueue_tick.get(p.pipeline_id, s.tick)
        return (s.tick - t0) >= threshold

    # Candidate queues in consideration order, with aging adjustments
    q_query = s.queues[Priority.QUERY]
    q_inter = s.queues[Priority.INTERACTIVE]
    q_batch = s.queues[Priority.BATCH_PIPELINE]

    # If oldest interactive waited long, consider it before query
    if q_inter and waited_long_enough(q_inter[0], s.aging_query_over_interactive):
        queues_to_try = [Priority.INTERACTIVE, Priority.QUERY, Priority.BATCH_PIPELINE]
    # If oldest batch waited long, consider it before interactive (but still after query unless query is empty)
    elif q_batch and waited_long_enough(q_batch[0], s.aging_interactive_over_batch):
        queues_to_try = [Priority.QUERY, Priority.BATCH_PIPELINE, Priority.INTERACTIVE]
    else:
        queues_to_try = _priority_order()

    # Try to find a pipeline that actually has a ready operator; rotate otherwise.
    for pr in queues_to_try:
        q = s.queues[pr]
        for _ in range(len(q)):
            p = q.pop(0)
            if _pipeline_done_or_hopeless(p):
                # Drop from queue; pipeline finished
                continue
            ops = _ready_ops_for_pipeline(p)
            if ops:
                # Put it back at the front only after we schedule; caller will decide to requeue
                return p, ops
            # Not runnable yet (waiting on parents); requeue to the back
            q.append(p)
    return None, []


@register_scheduler(key="scheduler_low_045")
def scheduler_low_045(s, results: List[ExecutionResult], pipelines: List[Pipeline]) -> Tuple[List[Suspend], List[Assignment]]:
    """
    Priority-aware FIFO with RAM backoff retries and light aging.

    Returns:
      - suspensions: kept empty (no reliable API to identify running low-priority containers to preempt safely)
      - assignments: schedules ready operators across pools, preferring pool placement by priority
    """
    s.tick += 1

    # Incorporate new pipelines
    for p in pipelines:
        _enqueue_pipeline(s, p)

    # Update RAM hints / retry counters based on failures
    for r in (results or []):
        if not r.failed():
            continue

        # Increase RAM hint for each failed op; assume OOM/underprovisioning is common and costly.
        for op in (r.ops or []):
            op_key = _op_key_for_scheduler(op)
            k = (r.pipeline_id if hasattr(r, "pipeline_id") else None, op_key)

            # If pipeline_id isn't present on result, fall back to a less precise key using op_key only.
            if k[0] is None:
                k = ("_unknown_pipeline_", op_key)

            prev_hint = s.ram_hint.get(k, None)
            prev_ram = r.ram if getattr(r, "ram", None) is not None else prev_hint
            if prev_ram is None:
                # Fallback: if we don't know, start from a moderately conservative default
                prev_ram = 1

            # Exponential backoff with small additive increase
            new_hint = (prev_ram * s.ram_backoff_factor) + s.ram_backoff_add
            s.ram_hint[k] = new_hint

            # Track retries
            s.retry_count[k] = s.retry_count.get(k, 0) + 1

    suspensions: List[Suspend] = []
    assignments: List[Assignment] = []

    # Early exit if no decision-relevant changes
    if not pipelines and not results:
        return suspensions, assignments

    # Schedule per pool with pool-aware placement.
    # We do a small bounded number of scheduling attempts per pool to avoid infinite loops with unrunnable pipelines.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool
        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # We'll fill the pool with multiple small-to-moderate assignments.
        attempts = 0
        max_attempts = 32

        while attempts < max_attempts:
            attempts += 1
            if avail_cpu <= 0 or avail_ram <= 0:
                break

            # Check if there is any high-priority backlog
            has_query_backlog = len(s.queues[Priority.QUERY]) > 0
            has_inter_backlog = len(s.queues[Priority.INTERACTIVE]) > 0
            has_high_prio_backlog = has_query_backlog or has_inter_backlog

            # Choose a pipeline that has a ready op
            pipeline, ready_ops = _dequeue_next_pipeline_with_ready_op(s)
            if pipeline is None:
                break

            priority = pipeline.priority
            # Enforce pool preference by priority: if this pool is not preferred and a preferred pool has room,
            # requeue and try another pipeline to reduce interference.
            preferred = _preferred_pools_for_priority(s.executor.num_pools, priority)
            if preferred and pool_id != preferred[0]:
                # If this pool is not top-choice, lightly bias away by requeueing and trying to pick something else.
                # (We don't have visibility into other pools' instantaneous headroom, so keep it simple.)
                s.queues[priority].append(pipeline)
                continue

            # Choose one operator at a time per pipeline to reduce OOM blast radius and improve fairness.
            op = ready_ops[0]
            op_key = _op_key_for_scheduler(op)

            # Retry cap: if we've tried too many times for this op, stop scheduling it to avoid churn.
            # (This may still incur 720s, but prevents runaway resource waste.)
            rk = (pipeline.pipeline_id, op_key)
            if s.retry_count.get(rk, 0) >= s.max_retries_per_op:
                # Requeue pipeline to avoid blocking other work; this op likely remains FAILED and pipeline will be penalized.
                s.queues[priority].append(pipeline)
                continue

            # Compute RAM request (cap to available)
            ram_req = _compute_ram_request(s, pool, priority, pipeline.pipeline_id, op)
            if ram_req > avail_ram:
                # Not enough RAM now; requeue and try other work.
                s.queues[priority].append(pipeline)
                continue

            # Compute CPU request (cap to available)
            cpu_req = _compute_cpu_request(s, pool, priority, avail_cpu, has_high_prio_backlog)
            if cpu_req <= 0:
                s.queues[priority].append(pipeline)
                break

            # If CPU is available but tiny, still allow 1 CPU to make progress.
            if cpu_req < 1 and avail_cpu >= 1:
                cpu_req = 1

            if cpu_req > avail_cpu:
                cpu_req = avail_cpu

            # Place assignment
            assignment = Assignment(
                ops=[op],
                cpu=cpu_req,
                ram=ram_req,
                priority=priority,
                pool_id=pool_id,
                pipeline_id=pipeline.pipeline_id,
            )
            assignments.append(assignment)

            # Update local available resources (best-effort; simulator will be source of truth next tick)
            avail_cpu -= cpu_req
            avail_ram -= ram_req

            # Requeue pipeline for its next operator(s)
            s.queues[priority].append(pipeline)

    return suspensions, assignments
