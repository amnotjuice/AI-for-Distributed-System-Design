# policy_key: scheduler_high_037
# reasoning_effort: high
# model: gpt-5.2-2025-12-11
# llm_cost: 0.121732
# generation_seconds: 141.82
# generated_at: 2026-03-14T04:07:07.198016
@register_scheduler_init(key="scheduler_high_037")
def scheduler_high_037_init(s):
    """Priority-aware, resource-capped scheduler to reduce tail latency for high-priority work.

    Incremental improvements over naive FIFO:
      1) Maintain separate queues per priority; always prefer QUERY/INTERACTIVE over BATCH.
      2) Avoid allocating the entire pool to a single operator by capping per-op CPU/RAM,
         enabling concurrency and faster response for interactive workloads.
      3) Basic OOM-driven RAM hinting per operator: retry failed (OOM) operators with more RAM.
      4) Light fairness: after a streak of high-priority placements, allow a batch placement if possible.
    """
    from collections import deque

    # Per-priority FIFO queues of Pipeline objects
    s.q_query = deque()
    s.q_interactive = deque()
    s.q_batch = deque()

    # Track active pipelines to avoid re-enqueueing duplicates
    s.active = {}  # pipeline_id -> Pipeline

    # Per-operator hints and retry bookkeeping (keyed by a stable-ish operator uid)
    s.op_ram_hint = {}          # op_uid -> ram to request next time
    s.op_fail_count = {}        # op_uid -> number of failures observed
    s.op_last_fail_oom = {}     # op_uid -> bool, last failure looked like OOM

    # Fairness knobs
    s.hi_streak = 0
    s.hi_streak_limit = 8  # after this many high-priority placements, try to place a batch op if waiting

    # Retry limits
    s.max_oom_retries = 4
    s.max_nonoom_retries = 1


def _prio_rank(p):
    # Higher is more important
    if p == Priority.QUERY:
        return 3
    if p == Priority.INTERACTIVE:
        return 2
    return 1  # Priority.BATCH_PIPELINE (and anything else) treated as lowest


def _queue_for_priority(s, prio):
    if prio == Priority.QUERY:
        return s.q_query
    if prio == Priority.INTERACTIVE:
        return s.q_interactive
    return s.q_batch


def _is_oom(error) -> bool:
    if not error:
        return False
    msg = str(error).lower()
    return ("oom" in msg) or ("out of memory" in msg) or ("memoryerror" in msg)


def _op_uid(op) -> str:
    # Try to use a stable operator identifier if available; fall back to object identity.
    for attr in ("op_id", "operator_id", "id", "name", "key"):
        v = getattr(op, attr, None)
        if v is not None:
            return f"{attr}:{v}"
    return f"pyid:{id(op)}"


def _clamp(x, lo, hi):
    if x < lo:
        return lo
    if x > hi:
        return hi
    return x


def _desired_fractions(priority):
    # Conservative fractions: give more to higher priority, but never the whole pool by default.
    if priority == Priority.QUERY:
        return 0.60, 0.60  # cpu_frac, ram_frac
    if priority == Priority.INTERACTIVE:
        return 0.50, 0.50
    return 0.35, 0.35


def _compute_request(s, pool, priority, op):
    """Compute CPU/RAM request, using priority-based caps and per-op OOM hints."""
    cpu_frac, ram_frac = _desired_fractions(priority)

    # Start from capped shares of the pool to avoid monopolizing a pool with one op.
    base_cpu = pool.max_cpu_pool * cpu_frac
    base_ram = pool.max_ram_pool * ram_frac

    # Apply per-op RAM hint if we have one (usually increased after OOM).
    uid = _op_uid(op)
    hinted_ram = s.op_ram_hint.get(uid, None)
    if hinted_ram is not None:
        base_ram = max(base_ram, hinted_ram)

    # Ensure minimums (avoid zeros). We keep this minimal and generic since units vary by simulator.
    min_cpu = 1.0 if pool.max_cpu_pool >= 1.0 else max(0.1, pool.max_cpu_pool)
    min_ram = 1.0 if pool.max_ram_pool >= 1.0 else max(0.1, pool.max_ram_pool)

    # Clamp to pool limits.
    req_cpu = _clamp(base_cpu, min_cpu, pool.max_cpu_pool)
    req_ram = _clamp(base_ram, min_ram, pool.max_ram_pool)
    return req_cpu, req_ram


def _retry_allowed(s, op) -> bool:
    uid = _op_uid(op)
    fails = s.op_fail_count.get(uid, 0)
    last_oom = s.op_last_fail_oom.get(uid, False)
    if last_oom:
        return fails <= s.max_oom_retries
    return fails <= s.max_nonoom_retries


def _next_ready_op(s, status):
    """Prefer PENDING ops; fall back to retryable FAILED ops."""
    pending = status.get_ops([OperatorState.PENDING], require_parents_complete=True)
    if pending:
        return pending[0]

    failed = status.get_ops([OperatorState.FAILED], require_parents_complete=True)
    for op in failed:
        if _retry_allowed(s, op):
            return op
    return None


def _queues_have_high(s) -> bool:
    # Note: deques may contain completed pipelines; we'll lazily skip them on pop.
    return bool(s.q_query) or bool(s.q_interactive)


def _pick_pipeline_from_queue(s, q, scheduled_pipeline_ids):
    """Pop/rotate until a runnable pipeline is found or we've inspected the full queue."""
    n = len(q)
    for _ in range(n):
        p = q.popleft()

        # Pipeline may have been removed from active (completed/abandoned)
        if p.pipeline_id not in s.active:
            continue

        if p.pipeline_id in scheduled_pipeline_ids:
            q.append(p)
            continue

        status = p.runtime_status()
        if status.is_pipeline_successful():
            # Lazily retire completed pipelines.
            s.active.pop(p.pipeline_id, None)
            continue

        op = _next_ready_op(s, status)
        if op is None:
            # Not runnable yet; rotate to back.
            q.append(p)
            continue

        # Found a runnable pipeline+op; keep pipeline in queue for future ops (fairness via rotation).
        q.append(p)
        return p, op

    return None, None


def _pick_next(s, pool_id, scheduled_pipeline_ids):
    """Pick the next (pipeline, op) to run based on priority and a small fairness mechanism."""
    # Determine priority order.
    # If any high-priority work exists, always try to schedule it first on all pools.
    # Otherwise, schedule batch first.
    high_present = _queues_have_high(s)

    # Fairness: after a high streak, allow one batch placement if any batch exists.
    allow_batch_break = (s.hi_streak >= s.hi_streak_limit) and bool(s.q_batch)

    if high_present and not allow_batch_break:
        orders = (s.q_query, s.q_interactive, s.q_batch)
    else:
        # Either no high work, or we want to give batch a turn.
        orders = (s.q_batch, s.q_query, s.q_interactive)

    for q in orders:
        p, op = _pick_pipeline_from_queue(s, q, scheduled_pipeline_ids)
        if p is not None:
            return p, op

    return None, None


@register_scheduler(key="scheduler_high_037")
def scheduler_high_037(s, results: List[ExecutionResult], pipelines: List[Pipeline]) -> Tuple[List[Suspend], List[Assignment]]:
    """See init docstring for high-level policy behavior."""
    # Enqueue newly arrived pipelines into the appropriate priority queue.
    for p in pipelines:
        if p.pipeline_id in s.active:
            continue
        s.active[p.pipeline_id] = p
        _queue_for_priority(s, p.priority).append(p)

    # Update RAM hints and failure counters based on execution results.
    # We only assume results include: .ops, .ram, .cpu, .error, .pool_id, .failed()
    for r in results:
        if not getattr(r, "ops", None):
            continue

        failed = False
        try:
            failed = r.failed()
        except Exception:
            failed = bool(getattr(r, "error", None))

        is_oom = _is_oom(getattr(r, "error", None))
        pool = None
        try:
            pool = s.executor.pools[r.pool_id]
        except Exception:
            pool = None

        for op in r.ops:
            uid = _op_uid(op)

            if failed:
                s.op_fail_count[uid] = s.op_fail_count.get(uid, 0) + 1
                s.op_last_fail_oom[uid] = is_oom

                # If OOM: increase RAM hint aggressively for next retry.
                if is_oom:
                    prev = s.op_ram_hint.get(uid, None)
                    # Prefer doubling the last allocation; ensure at least a small bump.
                    try:
                        bumped = float(r.ram) * 2.0
                    except Exception:
                        bumped = None

                    # Cap at pool max if known.
                    if pool is not None and bumped is not None:
                        bumped = min(bumped, pool.max_ram_pool)

                    if prev is None and bumped is not None:
                        s.op_ram_hint[uid] = bumped
                    elif prev is not None and bumped is not None:
                        s.op_ram_hint[uid] = max(prev * 2.0, bumped)
                    elif prev is not None:
                        s.op_ram_hint[uid] = prev * 2.0
            else:
                # On success, record that this RAM level worked (do not decrease aggressively).
                try:
                    worked = float(r.ram)
                    if pool is not None:
                        worked = min(worked, pool.max_ram_pool)
                    prev = s.op_ram_hint.get(uid, None)
                    if prev is None:
                        s.op_ram_hint[uid] = worked
                    else:
                        s.op_ram_hint[uid] = max(prev, worked)
                except Exception:
                    pass

                # Clear failure info on success (optional but helps avoid over-penalizing future retries).
                if uid in s.op_fail_count:
                    s.op_fail_count.pop(uid, None)
                    s.op_last_fail_oom.pop(uid, None)

    # Early exit if nothing changed that should affect our decisions.
    if not pipelines and not results and not s.active:
        return [], []

    suspensions: List[Suspend] = []
    assignments: List[Assignment] = []

    # Attempt to schedule as many operators as possible across pools.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        rem_cpu = float(pool.avail_cpu_pool)
        rem_ram = float(pool.avail_ram_pool)

        if rem_cpu <= 0 or rem_ram <= 0:
            continue

        scheduled_this_pool = set()

        # Greedily place operators until we run out of either CPU or RAM.
        # To avoid pathological long loops, cap placements per pool per tick.
        placements_cap = 64
        while placements_cap > 0 and rem_cpu > 0 and rem_ram > 0:
            placements_cap -= 1

            p, op = _pick_next(s, pool_id, scheduled_this_pool)
            if p is None:
                break

            # If the pipeline completed between pick and now, skip.
            status = p.runtime_status()
            if status.is_pipeline_successful():
                s.active.pop(p.pipeline_id, None)
                continue

            # Compute resource request and fit to remaining pool headroom.
            req_cpu, req_ram = _compute_request(s, pool, p.priority, op)

            # Fit to remaining. For latency, we still try to run even if we can't meet full request,
            # but we never allocate <= 0.
            cpu = min(req_cpu, rem_cpu)
            ram = min(req_ram, rem_ram)

            if cpu <= 0 or ram <= 0:
                break

            # Avoid producing extremely tiny fragments.
            min_cpu = 1.0 if pool.max_cpu_pool >= 1.0 else max(0.1, pool.max_cpu_pool)
            min_ram = 1.0 if pool.max_ram_pool >= 1.0 else max(0.1, pool.max_ram_pool)
            if cpu < min_cpu or ram < min_ram:
                break

            assignments.append(
                Assignment(
                    ops=[op],
                    cpu=cpu,
                    ram=ram,
                    priority=p.priority,
                    pool_id=pool_id,
                    pipeline_id=p.pipeline_id,
                )
            )

            rem_cpu -= cpu
            rem_ram -= ram
            scheduled_this_pool.add(p.pipeline_id)

            # Update high-streak fairness counter.
            if p.priority in (Priority.QUERY, Priority.INTERACTIVE):
                s.hi_streak += 1
            else:
                s.hi_streak = 0

    return suspensions, assignments
