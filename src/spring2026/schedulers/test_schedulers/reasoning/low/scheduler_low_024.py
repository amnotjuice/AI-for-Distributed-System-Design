# policy_key: scheduler_low_024
# reasoning_effort: low
# model: gpt-5.2-2025-12-11
# llm_cost: 0.044391
# generation_seconds: 43.81
# generated_at: 2026-04-09T21:21:24.144299
@register_scheduler_init(key="scheduler_low_024")
def scheduler_low_024_init(s):
    """
    Priority-aware, failure-averse scheduler with conservative sizing + OOM backoff.

    Goals for the weighted-latency objective:
    - Keep QUERY/INTERACTIVE latency low via strict priority ordering and larger default shares.
    - Avoid pipeline failures/incompletions (720s penalty) by retrying failed ops with increased RAM.
    - Avoid starving BATCH by injecting occasional batch picks when queues are non-empty.

    Key ideas:
    - Three FIFO queues (query, interactive, batch) with a light-weight fairness "streak" breaker.
    - Assign at most one operator per pool per tick (simple, stable baseline improvement).
    - Start with moderate RAM/CPU fractions per priority; on failure, increase RAM hint for that op.
    - Do not drop pipelines that have failed ops; FAILED ops are considered assignable and retried.
    """
    from collections import deque

    s.q_query = deque()
    s.q_interactive = deque()
    s.q_batch = deque()

    # Per (pipeline_id, op_key): hinted RAM to request next time (increases on failures).
    s.ram_hint = {}

    # Per (pipeline_id, op_key): number of retries attempted.
    s.retry_count = {}

    # Simple fairness: after too many consecutive high-priority picks while batch waits,
    # force a batch pick to ensure progress and reduce incompletions.
    s.hp_streak = 0

    # Tuning knobs (kept modest to avoid thrash and preserve utilization).
    s.max_retries_per_op = 6
    s.oom_backoff_mult = 1.6  # increase requested RAM after failure


def _prio_bucket(s, priority):
    # Map priority -> queue reference; assumes constants exist as described in prompt.
    if priority == Priority.QUERY:
        return s.q_query
    if priority == Priority.INTERACTIVE:
        return s.q_interactive
    return s.q_batch


def _op_key(op):
    # Stable-ish identifier for an operator; fall back to repr.
    for attr in ("op_id", "operator_id", "name", "id"):
        if hasattr(op, attr):
            try:
                v = getattr(op, attr)
                if v is not None:
                    return str(v)
            except Exception:
                pass
    return repr(op)


def _default_cpu_frac(priority):
    # Conservative, priority-biased CPU shares.
    if priority == Priority.QUERY:
        return 0.75
    if priority == Priority.INTERACTIVE:
        return 0.55
    return 0.30


def _default_ram_frac(priority):
    # RAM shares favor completion for high priority; still leaves headroom for others.
    if priority == Priority.QUERY:
        return 0.55
    if priority == Priority.INTERACTIVE:
        return 0.40
    return 0.30


def _clamp(x, lo, hi):
    return max(lo, min(hi, x))


def _pipeline_done_or_giveup(s, pipeline):
    status = pipeline.runtime_status()
    if status.is_pipeline_successful():
        return True

    # If any FAILED op exceeded retry budget, stop scheduling this pipeline (it will remain incomplete/failed).
    # This prevents infinite retry loops that can starve other work and still end at 720s penalty.
    failed_ops = status.get_ops([OperatorState.FAILED], require_parents_complete=False) or []
    for op in failed_ops:
        k = (pipeline.pipeline_id, _op_key(op))
        if s.retry_count.get(k, 0) >= s.max_retries_per_op:
            return True
    return False


def _pop_next_pipeline(s):
    # If batch is waiting and we've taken too many high-priority picks in a row, force one batch pick.
    if s.q_batch and s.hp_streak >= 6 and (s.q_query or s.q_interactive):
        s.hp_streak = 0
        return s.q_batch.popleft()

    if s.q_query:
        s.hp_streak += 1
        return s.q_query.popleft()
    if s.q_interactive:
        s.hp_streak += 1
        return s.q_interactive.popleft()

    # Reset streak when we pick batch (or nothing).
    if s.q_batch:
        s.hp_streak = 0
        return s.q_batch.popleft()

    s.hp_streak = 0
    return None


def _requeue_pipeline(s, pipeline):
    _prio_bucket(s, pipeline.priority).append(pipeline)


@register_scheduler(key="scheduler_low_024")
def scheduler_low_024(s, results, pipelines):
    """
    Scheduler step:
    - Enqueue newly arrived pipelines into priority FIFOs.
    - Process results to update RAM hints on failures (OOM/backpressure-agnostic but helpful).
    - For each pool, attempt to assign exactly one ready operator from the best pipeline available.
    - Retry FAILED operators (ASSIGNABLE_STATES include FAILED) with larger RAM hints, up to a cap.
    """
    # Enqueue new arrivals.
    for p in pipelines:
        _prio_bucket(s, p.priority).append(p)

    # Early exit if nothing changed.
    if not pipelines and not results:
        return [], []

    # Update hints from execution results.
    for r in results:
        # If failure, bump RAM hint for each op in the result (typically single op).
        if getattr(r, "failed", None) and r.failed():
            # Best-effort extraction of pipeline_id: in this simulator's results, it may or may not exist.
            # We'll guard and only apply hints when available.
            pipeline_id = getattr(r, "pipeline_id", None)

            # Track error string just for heuristics; treat all failures as "maybe needs more RAM".
            # This is intentionally conservative: allocating more RAM is "free" performance-wise and reduces OOM risk.
            err = getattr(r, "error", "") or ""
            is_likely_oom = ("oom" in str(err).lower()) or ("out of memory" in str(err).lower())

            # Only apply per-op hints if we can associate to a pipeline id.
            if pipeline_id is not None:
                for op in getattr(r, "ops", []) or []:
                    k = (pipeline_id, _op_key(op))
                    s.retry_count[k] = s.retry_count.get(k, 0) + 1

                    # Increase hinted RAM based on what we tried last time (r.ram).
                    tried_ram = getattr(r, "ram", None)
                    if tried_ram is None:
                        continue

                    mult = s.oom_backoff_mult if is_likely_oom else 1.35
                    new_hint = tried_ram * mult
                    old_hint = s.ram_hint.get(k, 0)
                    if new_hint > old_hint:
                        s.ram_hint[k] = new_hint

    suspensions = []
    assignments = []

    # Assign up to one operator per pool per tick (stable baseline; reduces interference).
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool
        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        chosen_pipeline = None
        deferred = []

        # Find a pipeline with a runnable operator for this pool.
        while True:
            p = _pop_next_pipeline(s)
            if p is None:
                break

            if _pipeline_done_or_giveup(s, p):
                # Drop from scheduling loop; simulator will account for completion/failure.
                continue

            status = p.runtime_status()
            # Prefer ops whose parents are complete; allow FAILED ops to be retried.
            op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True) or []
            if not op_list:
                # No runnable ops right now; hold it aside and reconsider later.
                deferred.append(p)
                continue

            chosen_pipeline = p
            chosen_op = op_list[0]
            break

        # Requeue deferred pipelines (preserving relative order).
        for p in deferred:
            _requeue_pipeline(s, p)

        if chosen_pipeline is None:
            continue

        # Compute requested resources.
        prio = chosen_pipeline.priority
        cpu_frac = _default_cpu_frac(prio)
        ram_frac = _default_ram_frac(prio)

        # Baseline sizes from pool maxima; clamp to available.
        base_cpu = max(1, int(pool.max_cpu_pool * cpu_frac))
        req_cpu = _clamp(base_cpu, 1, int(avail_cpu))

        base_ram = max(1.0, float(pool.max_ram_pool) * ram_frac)
        req_ram = min(float(avail_ram), base_ram)

        # Apply per-op RAM hint if present (helps prevent repeated OOMs and 720s penalties).
        opk = (chosen_pipeline.pipeline_id, _op_key(chosen_op))
        hinted = s.ram_hint.get(opk, 0)
        if hinted and hinted > req_ram:
            req_ram = min(float(avail_ram), float(hinted))

        # If we still can't fit minimum meaningful resources, just requeue.
        if req_cpu <= 0 or req_ram <= 0:
            _requeue_pipeline(s, chosen_pipeline)
            continue

        # If this op has exceeded retry budget, don't keep hammering; requeue pipeline but it will be ignored later.
        if s.retry_count.get(opk, 0) >= s.max_retries_per_op:
            _requeue_pipeline(s, chosen_pipeline)
            continue

        assignments.append(
            Assignment(
                ops=[chosen_op],
                cpu=req_cpu,
                ram=req_ram,
                priority=prio,
                pool_id=pool_id,
                pipeline_id=chosen_pipeline.pipeline_id,
            )
        )

        # Requeue the pipeline for subsequent ops (pipeline-level time sharing).
        _requeue_pipeline(s, chosen_pipeline)

    return suspensions, assignments
