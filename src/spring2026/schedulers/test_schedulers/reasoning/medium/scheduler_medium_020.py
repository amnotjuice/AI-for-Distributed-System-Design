# policy_key: scheduler_medium_020
# reasoning_effort: medium
# exp: reasoning
# model: gpt-5.2-2025-12-11
# llm_cost: 0.085257
# generation_seconds: 72.67
# generated_at: 2026-04-09T22:55:54.653331
@register_scheduler_init(key="scheduler_medium_020")
def scheduler_medium_020_init(s):
    """Priority-aware, RAM-first, failure-adaptive scheduler.

    Design goals for the weighted-latency objective:
    - Protect query/interactive latency (dominant weights) via strict priority ordering.
    - Avoid OOM-driven failures by allocating generous RAM (RAM has no speed penalty in the simulator).
    - Adapt to observed failures by increasing per-operator RAM "hints" with capped retries.
    - Avoid starving batch by forcing occasional batch progress when it has been skipped repeatedly.
    - Use light pool specialization when multiple pools exist: pool 0 prefers query/interactive.
    """
    # Per-priority FIFO queues of pipelines
    s.q_query = []
    s.q_interactive = []
    s.q_batch = []

    # Failure-adaptive RAM multipliers and retry counters per operator identity
    # Keyed by (pipeline_id, op_key)
    s.op_ram_mult = {}     # default 1.0, grows on OOM-like failures
    s.op_retry_count = {}  # counts failures we reacted to

    # Starvation control: if batch is waiting but repeatedly not scheduled, force a batch slot
    s.batch_skip_streak = 0

    # Soft reservations when batch is scheduled (to leave headroom for bursty query/interactive)
    s.reserve_cpu_frac = 0.20
    s.reserve_ram_frac = 0.20

    # Base sizing fractions by priority (RAM-first; CPU moderate for concurrency)
    s.cpu_frac = {
        Priority.QUERY: 0.60,
        Priority.INTERACTIVE: 0.45,
        Priority.BATCH_PIPELINE: 0.30,
    }
    s.ram_frac = {
        Priority.QUERY: 0.60,
        Priority.INTERACTIVE: 0.50,
        Priority.BATCH_PIPELINE: 0.45,
    }

    # Retry policy: keep trying OOM-like failures (likely fixable); stop quickly on non-OOM
    s.max_retries_oom = 6
    s.max_retries_nonoom = 1

    # Batch anti-starvation threshold: after N consecutive "no batch scheduled" ticks with batch waiting,
    # force at least one batch assignment if feasible.
    s.batch_force_after = 6


def _priority_queue_for(s, prio):
    if prio == Priority.QUERY:
        return s.q_query
    if prio == Priority.INTERACTIVE:
        return s.q_interactive
    return s.q_batch


def _is_oom_error(err) -> bool:
    if err is None:
        return False
    msg = str(err).lower()
    return ("oom" in msg) or ("out of memory" in msg) or ("memory" in msg and "alloc" in msg)


def _op_key(op):
    # Prefer stable identifiers if present; otherwise fall back to object id.
    for attr in ("op_id", "operator_id", "id", "name"):
        if hasattr(op, attr):
            try:
                v = getattr(op, attr)
                # Avoid unhashable types
                return (attr, str(v))
            except Exception:
                pass
    return ("pyid", str(id(op)))


def _pipeline_done_or_hopeless(s, p) -> bool:
    st = p.runtime_status()
    if st.is_pipeline_successful():
        return True

    # If there are FAILED ops, check whether any are still retryable; if none are retryable, consider hopeless.
    if st.state_counts[OperatorState.FAILED] <= 0:
        return False

    failed_ops = st.get_ops([OperatorState.FAILED], require_parents_complete=False)
    if not failed_ops:
        return True

    for op in failed_ops:
        k = (p.pipeline_id, _op_key(op))
        # If we haven't exceeded retries, still retryable.
        if s.op_retry_count.get(k, 0) <= max(s.max_retries_oom, s.max_retries_nonoom):
            return False
    return True


def _dequeue_next_ready_op(s, q, require_parents_complete=True):
    """Pop pipelines in FIFO order until we find one with an assignable op ready.
    Returns (pipeline, op) or (None, None) if none found.
    """
    if not q:
        return None, None

    # Bounded scan to avoid infinite loops when many pipelines are blocked on dependencies
    n = len(q)
    for _ in range(n):
        p = q.pop(0)
        if _pipeline_done_or_hopeless(s, p):
            # Drop completed/hopeless pipelines from the queue permanently.
            continue

        st = p.runtime_status()
        ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=require_parents_complete)
        if ops:
            # Put pipeline back to preserve progress for subsequent ops (FIFO-ish)
            q.append(p)
            return p, ops[0]

        # No ready ops right now; keep it around.
        q.append(p)

    return None, None


def _has_any_ready(s, q) -> bool:
    if not q:
        return False
    n = len(q)
    for _ in range(n):
        p = q[_]
        if _pipeline_done_or_hopeless(s, p):
            continue
        st = p.runtime_status()
        ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        if ops:
            return True
    return False


def _choose_cpu_ram(s, pool, prio, pipeline_id, op, remaining_cpu, remaining_ram, keep_reserve=False):
    # CPU: allocate a fraction of pool max, but never exceed remaining.
    cpu_target = pool.max_cpu_pool * s.cpu_frac.get(prio, 0.4)
    cpu = min(remaining_cpu, max(1.0, cpu_target))

    # RAM: allocate a fraction of pool max (RAM-first to avoid OOM), then apply learned multiplier.
    base_ram = pool.max_ram_pool * s.ram_frac.get(prio, 0.5)
    mult = s.op_ram_mult.get((pipeline_id, _op_key(op)), 1.0)
    ram_target = base_ram * mult
    ram = min(remaining_ram, max(1.0, ram_target))

    # If we're scheduling batch and want to preserve headroom for arrivals, enforce a soft reservation.
    if keep_reserve:
        reserve_cpu = pool.max_cpu_pool * s.reserve_cpu_frac
        reserve_ram = pool.max_ram_pool * s.reserve_ram_frac
        cpu = min(cpu, max(1.0, remaining_cpu - reserve_cpu))
        ram = min(ram, max(1.0, remaining_ram - reserve_ram))

    # Final clamp (never allocate more than remaining).
    cpu = min(cpu, remaining_cpu)
    ram = min(ram, remaining_ram)
    if cpu <= 0 or ram <= 0:
        return 0, 0
    return cpu, ram


@register_scheduler(key="scheduler_medium_020")
def scheduler_medium_020(s, results, pipelines):
    """
    Policy:
    1) Enqueue arriving pipelines by priority.
    2) Update per-operator RAM multipliers on failures (especially OOM-like).
    3) For each pool, repeatedly assign 1 ready op at a time until resources are scarce:
       - If multiple pools: pool 0 prefers query/interactive; other pools prefer batch but may run high-priority if available.
       - Otherwise: strict priority selection with a small batch anti-starvation mechanism.
    4) No preemption (simulator interfaces for active containers are not assumed available).
    """
    # Enqueue new pipelines
    for p in pipelines:
        _priority_queue_for(s, p.priority).append(p)

    # Process results to adapt RAM sizing
    for r in results:
        if not r.failed():
            continue

        # Increase RAM multiplier for OOM-like failures; otherwise mark as non-retryable quickly.
        is_oom = _is_oom_error(r.error)
        for op in (r.ops or []):
            k = (r.pipeline_id, _op_key(op)) if hasattr(r, "pipeline_id") else (None, _op_key(op))
            # If we couldn't get pipeline_id from result, fall back to op-only key namespace.
            if k[0] is None:
                k = ("unknown_pipeline", k[1])

            s.op_retry_count[k] = s.op_retry_count.get(k, 0) + 1

            if is_oom:
                # Exponential-ish backoff to reach safe RAM quickly without too many retries.
                cur = s.op_ram_mult.get(k, 1.0)
                nxt = cur * 1.6
                # Cap multiplier to avoid absurd allocations; using pool.max will still clamp.
                s.op_ram_mult[k] = min(nxt, 8.0)
            else:
                # For non-OOM failures, don't keep retrying endlessly; multiplier doesn't help.
                s.op_ram_mult[k] = s.op_ram_mult.get(k, 1.0)

    # Fast exit if nothing changed
    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    # Determine whether batch is waiting (for anti-starvation)
    batch_waiting = _has_any_ready(s, s.q_batch)

    # Multi-pool specialization hint
    multi_pool = getattr(s.executor, "num_pools", 1) > 1

    # Track whether we scheduled any batch this tick
    scheduled_batch_this_tick = False

    # Helper: choose next (pipeline, op, priority) based on pool role and starvation controls
    def pick_next_for_pool(pool_id):
        nonlocal scheduled_batch_this_tick

        query_ready = _has_any_ready(s, s.q_query)
        инт_ready = _has_any_ready(s, s.q_interactive)  # internal variable name; not user-visible

        # If batch has been skipped too long and batch is waiting, force a batch pick if possible.
        force_batch = batch_waiting and (s.batch_skip_streak >= s.batch_force_after)

        # Pool roles:
        # - If multi-pool: pool 0 prefers (query, interactive); others prefer batch.
        # - If single pool: global preference query > interactive > batch with forced batch occasionally.
        if multi_pool:
            if pool_id == 0:
                # interactive/query pool
                if query_ready:
                    p, op = _dequeue_next_ready_op(s, s.q_query, require_parents_complete=True)
                    if p and op:
                        return p, op, Priority.QUERY
                if инт_ready:
                    p, op = _dequeue_next_ready_op(s, s.q_interactive, require_parents_complete=True)
                    if p and op:
                        return p, op, Priority.INTERACTIVE
                # If no high-priority work, allow batch to use pool 0
                if batch_waiting:
                    p, op = _dequeue_next_ready_op(s, s.q_batch, require_parents_complete=True)
                    if p and op:
                        scheduled_batch_this_tick = True
                        return p, op, Priority.BATCH_PIPELINE
                return None, None, None
            else:
                # batch pool(s): primarily batch, but can run high-priority if batch not ready.
                if force_batch and batch_waiting:
                    p, op = _dequeue_next_ready_op(s, s.q_batch, require_parents_complete=True)
                    if p and op:
                        scheduled_batch_this_tick = True
                        return p, op, Priority.BATCH_PIPELINE

                if batch_waiting:
                    p, op = _dequeue_next_ready_op(s, s.q_batch, require_parents_complete=True)
                    if p and op:
                        scheduled_batch_this_tick = True
                        return p, op, Priority.BATCH_PIPELINE

                # Backfill high-priority if batch not ready
                if query_ready:
                    p, op = _dequeue_next_ready_op(s, s.q_query, require_parents_complete=True)
                    if p and op:
                        return p, op, Priority.QUERY
                if инт_ready:
                    p, op = _dequeue_next_ready_op(s, s.q_interactive, require_parents_complete=True)
                    if p and op:
                        return p, op, Priority.INTERACTIVE
                return None, None, None
        else:
            # Single pool: strict priority with batch forcing.
            if not force_batch:
                if query_ready:
                    p, op = _dequeue_next_ready_op(s, s.q_query, require_parents_complete=True)
                    if p and op:
                        return p, op, Priority.QUERY
                if инт_ready:
                    p, op = _dequeue_next_ready_op(s, s.q_interactive, require_parents_complete=True)
                    if p and op:
                        return p, op, Priority.INTERACTIVE
            if batch_waiting:
                p, op = _dequeue_next_ready_op(s, s.q_batch, require_parents_complete=True)
                if p and op:
                    scheduled_batch_this_tick = True
                    return p, op, Priority.BATCH_PIPELINE

            # If forced batch but none was schedulable, fall back to high-priority again.
            if query_ready:
                p, op = _dequeue_next_ready_op(s, s.q_query, require_parents_complete=True)
                if p and op:
                    return p, op, Priority.QUERY
            if инт_ready:
                p, op = _dequeue_next_ready_op(s, s.q_interactive, require_parents_complete=True)
                if p and op:
                    return p, op, Priority.INTERACTIVE
            return None, None, None

    # Main assignment loop per pool
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        remaining_cpu = pool.avail_cpu_pool
        remaining_ram = pool.avail_ram_pool
        if remaining_cpu <= 0 or remaining_ram <= 0:
            continue

        # Make multiple small-to-medium assignments per pool to improve latency under concurrency.
        # Stop when resources are too low to be useful.
        for _ in range(64):
            if remaining_cpu <= 0 or remaining_ram <= 0:
                break

            p, op, prio = pick_next_for_pool(pool_id)
            if p is None or op is None:
                break

            # Decide if we should keep headroom when placing batch (helps sudden query arrivals)
            keep_reserve = (prio == Priority.BATCH_PIPELINE) and (_has_any_ready(s, s.q_query) or _has_any_ready(s, s.q_interactive))

            cpu, ram = _choose_cpu_ram(
                s, pool, prio, p.pipeline_id, op,
                remaining_cpu=remaining_cpu,
                remaining_ram=remaining_ram,
                keep_reserve=keep_reserve
            )
            if cpu <= 0 or ram <= 0:
                # Can't fit anything sensible right now.
                break

            assignments.append(
                Assignment(
                    ops=[op],
                    cpu=cpu,
                    ram=ram,
                    priority=prio,
                    pool_id=pool_id,
                    pipeline_id=p.pipeline_id,
                )
            )

            remaining_cpu -= cpu
            remaining_ram -= ram

    # Update starvation tracking
    if batch_waiting and not scheduled_batch_this_tick:
        s.batch_skip_streak += 1
    else:
        s.batch_skip_streak = 0

    return suspensions, assignments
