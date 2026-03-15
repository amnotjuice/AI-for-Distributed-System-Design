# policy_key: scheduler_high_023
# reasoning_effort: high
# model: gpt-5.2-2025-12-11
# llm_cost: 0.147254
# generation_seconds: 151.87
# generated_at: 2026-03-14T03:31:32.542905
@register_scheduler_init(key="scheduler_high_023")
def scheduler_high_023_init(s):
    """
    Priority-aware, headroom-preserving scheduler.

    Incremental improvements over naive FIFO:
      1) Separate queues per priority and always serve higher priority first.
      2) Pack multiple operators per pool per tick (instead of 1 op/pool) with per-op CPU/RAM caps
         to improve concurrency and reduce queueing latency for interactive/query work.
      3) Preserve CPU/RAM headroom in non-dedicated pools so batch cannot fully consume capacity,
         improving tail latency for higher-priority arrivals.
      4) Simple OOM-aware RAM backoff: on OOM failure, increase future RAM requests for that operator.
    """
    from collections import deque

    # Priority queues (round-robin within each priority).
    s.q_query = deque()
    s.q_interactive = deque()
    s.q_batch = deque()

    # Track which pipeline_ids are currently enqueued (avoid duplicates).
    s.inqueue = set()

    # Operator-level hints/backoff.
    s.op_ram_hint = {}        # op_uid -> last successful RAM (or last attempted)
    s.op_ram_mult = {}        # op_uid -> multiplicative backoff after OOMs
    s.op_fail_count = {}      # op_uid -> number of failures observed
    s.blacklisted_ops = set() # op_uid -> do not retry (too many failures / non-OOM)

    # Pipelines we consider terminal (e.g., contain a blacklisted failed op).
    s.dead_pipelines = set()

    # Logical epoch counter (ticks) for any future policy extensions.
    s.epoch = 0


def _is_oom_error(err) -> bool:
    if not err:
        return False
    msg = str(err).lower()
    return ("oom" in msg) or ("out of memory" in msg) or ("out-of-memory" in msg) or ("cuda oom" in msg)


def _op_uid(op):
    # Best-effort stable operator identifier (different sims may expose different fields).
    for attr in ("op_id", "operator_id", "id", "name"):
        if hasattr(op, attr):
            try:
                v = getattr(op, attr)
                if v is not None:
                    return (attr, v)
            except Exception:
                pass
    # Fallback: object identity (stable for the lifetime of the pipeline object in the sim).
    return ("py_id", id(op))


def _get_assignable_states():
    # Be robust if ASSIGNABLE_STATES is not defined by the environment.
    try:
        return ASSIGNABLE_STATES
    except Exception:
        return [OperatorState.PENDING, OperatorState.FAILED]


def _enqueue_pipeline(s, p):
    if p.pipeline_id in s.dead_pipelines:
        return
    if p.pipeline_id in s.inqueue:
        return
    s.inqueue.add(p.pipeline_id)
    if p.priority == Priority.QUERY:
        s.q_query.append(p)
    elif p.priority == Priority.INTERACTIVE:
        s.q_interactive.append(p)
    else:
        s.q_batch.append(p)


def _remove_pipeline_everywhere(s, pipeline_id: str):
    # Remove only from inqueue/dead sets here; actual deque cleanup is lazy during popping.
    if pipeline_id in s.inqueue:
        s.inqueue.remove(pipeline_id)


def _pipeline_is_terminal(s, pipeline) -> bool:
    # Completed pipelines are terminal.
    status = pipeline.runtime_status()
    if status.is_pipeline_successful():
        return True

    # If any FAILED op is blacklisted, treat pipeline as terminal (won't make progress).
    if status.state_counts.get(OperatorState.FAILED, 0) > 0:
        try:
            failed_ops = status.get_ops([OperatorState.FAILED], require_parents_complete=False)
        except Exception:
            failed_ops = []
        for op in failed_ops or []:
            if _op_uid(op) in s.blacklisted_ops:
                return True

    return False


def _pop_runnable_pipeline_and_op(s, q, assigned_pipelines, assigned_ops):
    """
    Round-robin scan of a deque to find a pipeline with an assignable op (parents complete).
    Performs lazy cleanup of completed/terminal pipelines.
    """
    n = len(q)
    if n == 0:
        return None, None

    assignable_states = _get_assignable_states()

    for _ in range(n):
        p = q.popleft()

        # Drop pipelines we consider dead/terminal.
        if p.pipeline_id in s.dead_pipelines:
            _remove_pipeline_everywhere(s, p.pipeline_id)
            continue

        if _pipeline_is_terminal(s, p):
            s.dead_pipelines.add(p.pipeline_id)
            _remove_pipeline_everywhere(s, p.pipeline_id)
            continue

        # Only allow one op per pipeline per scheduling tick to prevent duplicate assignment
        # before the simulator applies state transitions.
        if p.pipeline_id in assigned_pipelines:
            q.append(p)
            continue

        status = p.runtime_status()
        try:
            ops = status.get_ops(assignable_states, require_parents_complete=True)
        except Exception:
            ops = []

        if not ops:
            q.append(p)
            continue

        op = ops[0]
        uid = _op_uid(op)

        # Skip ops that we've decided not to retry.
        if uid in s.blacklisted_ops:
            # Pipeline cannot progress if this is the only runnable op; mark dead and drop.
            s.dead_pipelines.add(p.pipeline_id)
            _remove_pipeline_everywhere(s, p.pipeline_id)
            continue

        # Avoid duplicate assignment of the same op within this scheduling call.
        if uid in assigned_ops:
            q.append(p)
            continue

        assigned_ops.add(uid)
        return p, op

    return None, None


def _size_cpu(pool, prio, cpu_cap):
    # Per-op CPU caps improve concurrency and reduce queueing latency for high priority.
    # (We still "burst" within the cap.)
    if prio == Priority.QUERY:
        frac = 0.50
        abs_cap = 8.0
    elif prio == Priority.INTERACTIVE:
        frac = 0.60
        abs_cap = 8.0
    else:
        frac = 0.25
        abs_cap = 4.0

    target = pool.max_cpu_pool * frac
    target = min(target, abs_cap)
    target = max(1.0, target)

    # Fit to what's allowed in this pool right now.
    return min(cpu_cap, target)


def _size_ram(s, pool, prio, op, ram_cap):
    uid = _op_uid(op)

    # Start with conservative defaults; rely on OOM backoff to correct upward.
    if prio == Priority.QUERY:
        base_frac = 0.55
    elif prio == Priority.INTERACTIVE:
        base_frac = 0.60
    else:
        base_frac = 0.30

    base = pool.max_ram_pool * base_frac

    # Prefer a known-good RAM size if we have one.
    hint = s.op_ram_hint.get(uid)
    if hint is not None:
        base = max(base * 0.50, hint)  # don't drop too aggressively below hint

    mult = s.op_ram_mult.get(uid, 1.0)
    req = base * mult

    # Clamp to available / maximum.
    req = min(req, pool.max_ram_pool)
    req = min(req, ram_cap)

    # Ensure positive request.
    if req <= 0:
        return 0.0
    return req


@register_scheduler(key="scheduler_high_023")
def scheduler_high_023(s, results: List[ExecutionResult], pipelines: List[Pipeline]) -> Tuple[List[Suspend], List[Assignment]]:
    """
    Priority-aware scheduler with reserved headroom and OOM RAM backoff.

    Pool strategy:
      - If multiple pools exist, pool 0 acts as an "interactive-preferred" pool:
        when any QUERY/INTERACTIVE work is queued, pool 0 avoids scheduling BATCH.
      - Other pools preserve headroom (20%) to reduce latency spikes for high-priority arrivals.

    No preemption yet (kept simple and robust).
    """
    s.epoch += 1

    # Incorporate new arrivals.
    for p in pipelines or []:
        _enqueue_pipeline(s, p)

    # Update hints/backoff from finished containers.
    for r in results or []:
        ops = getattr(r, "ops", None) or []
        failed = False
        try:
            failed = r.failed()
        except Exception:
            failed = bool(getattr(r, "error", None))

        oom = _is_oom_error(getattr(r, "error", None)) if failed else False

        for op in ops:
            uid = _op_uid(op)

            if failed:
                prev = s.op_fail_count.get(uid, 0) + 1
                s.op_fail_count[uid] = prev

                # OOM => increase RAM multiplier (retry-friendly).
                if oom:
                    cur = s.op_ram_mult.get(uid, 1.0)
                    s.op_ram_mult[uid] = min(cur * 2.0, 16.0)

                    # Remember last attempted RAM as a weak hint.
                    try:
                        attempted_ram = getattr(r, "ram", None)
                        if attempted_ram is not None:
                            s.op_ram_hint[uid] = max(s.op_ram_hint.get(uid, 0.0), float(attempted_ram))
                    except Exception:
                        pass
                else:
                    # Non-OOM failures usually won't improve with retries; blacklist after 1-2 tries.
                    if prev >= 2:
                        s.blacklisted_ops.add(uid)
            else:
                # Success: record RAM hint and reduce multiplier toward 1.
                try:
                    used_ram = getattr(r, "ram", None)
                    if used_ram is not None:
                        s.op_ram_hint[uid] = max(s.op_ram_hint.get(uid, 0.0), float(used_ram))
                except Exception:
                    pass

                cur = s.op_ram_mult.get(uid, 1.0)
                if cur > 1.0:
                    s.op_ram_mult[uid] = max(1.0, cur / 2.0)

                if uid in s.op_fail_count:
                    s.op_fail_count[uid] = 0

    # Fast exit if nothing to do.
    if not (pipelines or results) and (len(s.q_query) + len(s.q_interactive) + len(s.q_batch) == 0):
        return [], []

    suspensions: List[Suspend] = []
    assignments: List[Assignment] = []

    # Avoid double-assigning within one scheduler call.
    assigned_pipelines = set()
    assigned_ops = set()

    # Whether we have (possibly) pending high-priority work.
    # (We use a light heuristic; full validation is done when popping from queues.)
    has_hi_pending = (len(s.q_query) > 0) or (len(s.q_interactive) > 0)

    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool

        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # Preserve headroom in non-dedicated pools so batch can't starve interactive/query.
        reserved_cpu = 0.0
        reserved_ram = 0.0
        if pool_id != 0:
            reserved_cpu = pool.max_cpu_pool * 0.20
            reserved_ram = pool.max_ram_pool * 0.20

        # If pool 0 exists and any HI work is pending, avoid scheduling batch on pool 0.
        allow_batch = True
        if pool_id == 0 and has_hi_pending:
            allow_batch = False

        # Keep packing until we can't place more.
        while avail_cpu > 0 and avail_ram > 0:
            made_assignment = False

            # Always consider priorities in descending order.
            for prio, q in (
                (Priority.QUERY, s.q_query),
                (Priority.INTERACTIVE, s.q_interactive),
                (Priority.BATCH_PIPELINE, s.q_batch),
            ):
                if prio == Priority.BATCH_PIPELINE and not allow_batch:
                    continue

                # Apply headroom constraint only to batch.
                cpu_cap = avail_cpu
                ram_cap = avail_ram
                if prio == Priority.BATCH_PIPELINE:
                    cpu_cap = max(0.0, avail_cpu - reserved_cpu)
                    ram_cap = max(0.0, avail_ram - reserved_ram)

                if cpu_cap < 1.0 or ram_cap <= 0.0:
                    continue

                pipeline, op = _pop_runnable_pipeline_and_op(s, q, assigned_pipelines, assigned_ops)
                if pipeline is None:
                    continue

                # Size resources with caps and OOM backoff.
                cpu_req = _size_cpu(pool, prio, cpu_cap)
                ram_req = _size_ram(s, pool, prio, op, ram_cap)

                # If we can't fit even minimal resources, put the pipeline back and try others.
                if cpu_req < 1.0 or ram_req <= 0.0:
                    q.append(pipeline)
                    continue

                # Commit assignment.
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

                avail_cpu -= cpu_req
                avail_ram -= ram_req
                assigned_pipelines.add(pipeline.pipeline_id)

                # Requeue pipeline for its next op (lazy cleanup will drop it when completed).
                q.append(pipeline)

                made_assignment = True
                break  # Re-evaluate from highest priority again.

            if not made_assignment:
                break

    return suspensions, assignments
