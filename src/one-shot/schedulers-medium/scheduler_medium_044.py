# policy_key: scheduler_medium_044
# reasoning_effort: medium
# model: gpt-5.2-2025-12-11
# llm_cost: 0.069946
# generation_seconds: 82.75
# generated_at: 2026-03-14T03:18:14.822575
@register_scheduler_init(key="scheduler_medium_044")
def scheduler_medium_044_init(s):
    """Priority-aware, multi-assignment scheduler with simple OOM-driven RAM backoff.

    Incremental improvements over naive FIFO:
      1) Maintain separate queues by priority and always schedule higher priority first.
      2) Avoid giving an entire pool to a single operator; slice resources to enable concurrency
         (improves tail latency under mixed workloads).
      3) If an operator fails with an OOM-like error, increase its future RAM request (per-op key).
      4) Basic fairness within a priority via round-robin, with enqueue-time tracking.
    """
    # Queues per priority
    s.waiting = {
        Priority.INTERACTIVE: [],
        Priority.QUERY: [],
        Priority.BATCH_PIPELINE: [],
    }

    # Bookkeeping for fairness / mild anti-starvation
    s._tick = 0
    s._seen_pipeline_ids = set()
    s._enqueued_at = {}  # pipeline_id -> tick when first seen

    # Per-operator adaptive sizing (best-effort; no access to true minima here)
    s._op_ram_mult = {}      # op_key -> multiplier (>=1.0)
    s._op_fail_count = {}    # op_key -> recent failure count (for non-OOM)
    s._op_nonretriable = set()


def _op_key(op):
    """Best-effort stable key for an operator across retries."""
    # Try common identifiers; fall back to repr.
    for attr in ("op_id", "operator_id", "id", "name"):
        if hasattr(op, attr):
            try:
                v = getattr(op, attr)
                if v is not None:
                    return f"{attr}:{v}"
            except Exception:
                pass
    try:
        return repr(op)
    except Exception:
        return f"op@{id(op)}"


def _is_oom_error(err):
    if err is None:
        return False
    try:
        msg = str(err).lower()
    except Exception:
        return False
    # Keep this broad; simulator/runtime may vary error text.
    return ("oom" in msg) or ("out of memory" in msg) or ("memory" in msg and "alloc" in msg)


def _priority_order():
    # Highest to lowest
    return [Priority.INTERACTIVE, Priority.QUERY, Priority.BATCH_PIPELINE]


def _base_fractions(priority):
    # Fractions of pool max to request per container.
    # Interactive gets more to reduce latency; batch gets less to enable concurrency.
    if priority == Priority.INTERACTIVE:
        return 0.60, 0.60  # cpu_frac, ram_frac
    if priority == Priority.QUERY:
        return 0.45, 0.45
    return 0.30, 0.35  # batch: slightly more RAM than CPU to reduce OOM risk


def _clamp(x, lo, hi):
    return lo if x < lo else hi if x > hi else x


def _pick_runnable_pipeline(s, priority):
    """Round-robin pick a pipeline that has an assignable op with parents complete."""
    q = s.waiting[priority]
    if not q:
        return None, None

    # Iterate at most len(q) times to avoid infinite looping when nothing is runnable yet.
    n = len(q)
    for _ in range(n):
        p = q.pop(0)
        status = p.runtime_status()

        # Drop completed pipelines.
        if status.is_pipeline_successful():
            continue

        # If pipeline contains a known non-retriable failed op, drop it to avoid thrash.
        failed_ops = status.get_ops([OperatorState.FAILED], require_parents_complete=False)
        for op in failed_ops:
            if _op_key(op) in s._op_nonretriable:
                # Don't requeue; treat as terminally failed for scheduling purposes.
                p = None
                break
        if p is None:
            continue

        # Find next runnable operator (parents complete).
        op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
        # Requeue pipeline for future consideration (round-robin fairness).
        q.append(p)

        if op_list:
            return p, op_list

    return None, None


@register_scheduler(key="scheduler_medium_044")
def scheduler_medium_044(s, results: List["ExecutionResult"], pipelines: List["Pipeline"]) -> Tuple[List["Suspend"], List["Assignment"]]:
    """
    Scheduler step:
      - Ingest new pipelines into per-priority queues.
      - Update per-op RAM multipliers from failures (OOM => increase RAM).
      - For each pool (most headroom first), schedule multiple containers while resources remain:
          * Always prefer higher priority runnable work.
          * Allocate a slice of pool resources rather than the entire pool to one op.
    """
    s._tick += 1

    # Ingest new pipelines.
    for p in pipelines:
        pr = p.priority
        if pr not in s.waiting:
            # Unknown priority; treat as lowest.
            pr = Priority.BATCH_PIPELINE
        s.waiting[pr].append(p)
        if p.pipeline_id not in s._seen_pipeline_ids:
            s._seen_pipeline_ids.add(p.pipeline_id)
            s._enqueued_at[p.pipeline_id] = s._tick

    # Update sizing hints from execution results.
    for r in results:
        # Result.ops is a list; in this simulator we usually schedule one op per container.
        ops = getattr(r, "ops", None) or []
        for op in ops:
            k = _op_key(op)

            if r.failed():
                if _is_oom_error(getattr(r, "error", None)):
                    # Increase RAM multiplier aggressively on OOM.
                    cur = s._op_ram_mult.get(k, 1.0)
                    s._op_ram_mult[k] = _clamp(cur * 2.0, 1.0, 16.0)
                    # Reset generic failure count since we handled it.
                    s._op_fail_count[k] = 0
                else:
                    # Non-OOM failures: allow a few retries, then mark as non-retriable to avoid churn.
                    s._op_fail_count[k] = s._op_fail_count.get(k, 0) + 1
                    if s._op_fail_count[k] >= 3:
                        s._op_nonretriable.add(k)
            else:
                # On success, slowly decay RAM multiplier back toward 1.0.
                if k in s._op_ram_mult:
                    s._op_ram_mult[k] = max(1.0, s._op_ram_mult[k] * 0.85)
                    if s._op_ram_mult[k] <= 1.05:
                        s._op_ram_mult.pop(k, None)
                # Decay failure count as well.
                if k in s._op_fail_count:
                    s._op_fail_count[k] = max(0, s._op_fail_count[k] - 1)

    # Early exit if nothing changed that could affect decisions.
    if not pipelines and not results:
        any_waiting = any(bool(q) for q in s.waiting.values())
        if not any_waiting:
            return [], []

    suspensions: List["Suspend"] = []
    assignments: List["Assignment"] = []

    # Schedule pools with most headroom first (helps reduce fragmentation).
    pool_order = list(range(s.executor.num_pools))
    pool_order.sort(
        key=lambda pid: (s.executor.pools[pid].avail_cpu_pool + s.executor.pools[pid].avail_ram_pool),
        reverse=True,
    )

    for pool_id in pool_order:
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool

        # Require at least 1 CPU and 1 RAM unit to start a container (conservative).
        # If your simulator uses fractional units, this naturally becomes a bit more selective.
        while avail_cpu >= 1 and avail_ram >= 1:
            chosen_pipeline = None
            chosen_ops = None
            chosen_priority = None

            # Strict priority: INTERACTIVE > QUERY > BATCH
            for pr in _priority_order():
                p, op_list = _pick_runnable_pipeline(s, pr)
                if p is not None and op_list:
                    chosen_pipeline = p
                    chosen_ops = op_list
                    chosen_priority = pr
                    break

            if chosen_pipeline is None:
                break  # nothing runnable for this pool right now

            # Base request as a fraction of pool max (not avail), then clamp to avail.
            cpu_frac, ram_frac = _base_fractions(chosen_priority)
            base_cpu = max(1, int(pool.max_cpu_pool * cpu_frac))
            base_ram = max(1, int(pool.max_ram_pool * ram_frac))

            # Apply per-op OOM-driven RAM multiplier.
            opk = _op_key(chosen_ops[0])
            ram_mult = s._op_ram_mult.get(opk, 1.0)
            req_ram = int(base_ram * ram_mult)

            # Clamp requests to available + pool max.
            req_cpu = int(_clamp(base_cpu, 1, int(pool.max_cpu_pool)))
            req_ram = int(_clamp(req_ram, 1, int(pool.max_ram_pool)))

            # If we can't fit the request, try to shrink (CPU first, then RAM) to still make progress.
            if req_cpu > avail_cpu:
                req_cpu = int(avail_cpu)
            if req_ram > avail_ram:
                req_ram = int(avail_ram)

            # Ensure still viable after shrinking.
            if req_cpu < 1 or req_ram < 1:
                break

            assignments.append(
                Assignment(
                    ops=chosen_ops,
                    cpu=req_cpu,
                    ram=req_ram,
                    priority=chosen_pipeline.priority,
                    pool_id=pool_id,
                    pipeline_id=chosen_pipeline.pipeline_id,
                )
            )

            # Account locally so we can schedule multiple containers in one tick for this pool.
            avail_cpu -= req_cpu
            avail_ram -= req_ram

    return suspensions, assignments
