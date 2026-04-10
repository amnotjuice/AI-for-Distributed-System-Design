# policy_key: scheduler_low_010
# reasoning_effort: low
# model: gpt-5.2-2025-12-11
# llm_cost: 0.042489
# generation_seconds: 62.61
# generated_at: 2026-03-12T21:42:00.541208
@register_scheduler_init(key="scheduler_low_010")
def scheduler_low_010_init(s):
    """
    Priority-aware FIFO with simple RAM backoff on OOM.

    Improvements over naive FIFO:
      - Maintains separate waiting queues by priority (QUERY > INTERACTIVE > BATCH).
      - Uses conservative per-op RAM hints and doubles them on OOM failures (retry),
        while dropping non-OOM failures to avoid infinite loops.
      - Allocates more CPU to higher priority work and keeps some headroom for
        high-priority arrivals by limiting batch allocations when high-priority
        work is waiting (soft reservation; no preemption).
      - Schedules multiple ops per pool per tick when resources allow.
    """
    s.ticks = 0

    # Waiting sets: pipeline_id -> pipeline
    s.waiting = {}

    # FIFO order (pipeline_id) per priority class
    s.q_query = []
    s.q_interactive = []
    s.q_batch = []

    # Enqueue tick for aging/cleanup
    s.enqueue_tick = {}

    # Failure bookkeeping:
    # (pipeline_id, op_key) -> ram_hint
    s.ram_hint = {}

    # (pipeline_id, op_key) -> count of OOMs (for diagnostics / more aggressive backoff)
    s.oom_count = {}

    # pipeline_id -> terminal failure count (non-OOM); once >0, drop it
    s.hard_fail = {}


@register_scheduler(key="scheduler_low_010")
def scheduler_low_010_scheduler(s, results, pipelines):
    """
    Priority-aware scheduler with OOM-sensitive retries.

    Key behaviors:
      - New pipelines are enqueued into per-priority FIFO queues.
      - Results update per-op RAM hints:
          * OOM failure => increase RAM hint, keep pipeline for retry
          * other failure => mark pipeline as hard-failed and drop
      - Per pool, schedule as many ready ops as possible:
          * Prefer QUERY then INTERACTIVE then BATCH
          * Allocate CPU shares by priority
          * Soft-reserve capacity by limiting batch when high-priority work is waiting
    """
    s.ticks += 1

    suspensions = []
    assignments = []

    def _prio_rank(prio):
        # Smaller is higher priority
        if prio == Priority.QUERY:
            return 0
        if prio == Priority.INTERACTIVE:
            return 1
        return 2  # Priority.BATCH_PIPELINE and any others

    def _queue_for_priority(prio):
        if prio == Priority.QUERY:
            return s.q_query
        if prio == Priority.INTERACTIVE:
            return s.q_interactive
        return s.q_batch

    def _op_key(op):
        # Stable-enough key across ticks: prefer explicit identifiers if present.
        for attr in ("op_id", "operator_id", "id", "name"):
            if hasattr(op, attr):
                try:
                    v = getattr(op, attr)
                    return (attr, v() if callable(v) else v)
                except Exception:
                    pass
        # Fallback: object identity (works within one simulation run if op objects persist)
        return ("py_id", id(op))

    def _is_oom_error(err):
        if err is None:
            return False
        try:
            msg = str(err).lower()
        except Exception:
            return False
        return ("oom" in msg) or ("out of memory" in msg) or ("memory" in msg and "alloc" in msg)

    def _base_ram_fraction(prio):
        # Conservative starting points; OOM backoff will correct upward.
        if prio == Priority.QUERY:
            return 0.25
        if prio == Priority.INTERACTIVE:
            return 0.22
        return 0.15

    def _cpu_share(prio, pool_max_cpu):
        # More CPU for latency-sensitive work; keep batch smaller to reduce interference.
        if pool_max_cpu <= 0:
            return 1
        if prio == Priority.QUERY:
            return max(1, int(round(pool_max_cpu * 0.75)))
        if prio == Priority.INTERACTIVE:
            return max(1, int(round(pool_max_cpu * 0.60)))
        return max(1, int(round(pool_max_cpu * 0.35)))

    def _get_assignable_op(pipeline):
        status = pipeline.runtime_status()
        if status.is_pipeline_successful():
            return None
        # Only schedule ops whose parents are complete.
        op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
        return op_list[0] if op_list else None

    def _enqueue_pipeline(p):
        pid = p.pipeline_id
        if pid in s.waiting:
            # Already enqueued; keep earliest position.
            s.waiting[pid] = p
            return
        s.waiting[pid] = p
        s.enqueue_tick[pid] = s.ticks
        _queue_for_priority(p.priority).append(pid)

    def _drop_pipeline(pid):
        # Remove from waiting dict; we do lazy cleanup for queue lists.
        if pid in s.waiting:
            del s.waiting[pid]
        s.hard_fail.pop(pid, None)
        s.enqueue_tick.pop(pid, None)

    # Enqueue new pipelines
    for p in pipelines:
        _enqueue_pipeline(p)

    # Process execution results: update RAM hints and hard-fail marking
    for r in results:
        # Results may have multiple ops; in our assignments we submit one at a time.
        if not getattr(r, "ops", None):
            continue
        op0 = r.ops[0]
        pid = None

        # Prefer explicit pipeline_id on result if present, else try to infer from op
        if hasattr(r, "pipeline_id"):
            pid = r.pipeline_id
        # If we can't infer pid, we still can update by matching op keys for all pipelines
        opk = _op_key(op0)

        if hasattr(r, "failed") and callable(r.failed) and r.failed():
            # Decide whether this is an OOM retry or a hard failure
            if _is_oom_error(getattr(r, "error", None)):
                # Increase RAM hint for this op; cap at pool max RAM if known
                # Use last attempted RAM as the baseline if provided; else bump existing hint.
                attempted_ram = getattr(r, "ram", None)
                bump_base = attempted_ram if (attempted_ram is not None and attempted_ram > 0) else None

                # If pid is unknown, we cannot store per-pipeline hints reliably.
                # In that case, do nothing (conservative).
                if pid is not None:
                    key = (pid, opk)
                    prev = s.ram_hint.get(key, None)
                    if bump_base is None:
                        bump_base = prev if (prev is not None and prev > 0) else 0
                    # Double with a small additive bump to escape tiny allocations
                    new_hint = max(bump_base * 2, bump_base + 1) if bump_base > 0 else 2
                    s.ram_hint[key] = new_hint
                    s.oom_count[key] = s.oom_count.get(key, 0) + 1
            else:
                # Hard fail: do not retry (prevents infinite retry loops on logic errors)
                if pid is not None:
                    s.hard_fail[pid] = s.hard_fail.get(pid, 0) + 1

    # Early exit if nothing changed
    if not pipelines and not results:
        return suspensions, assignments

    # Lazy cleanup: drop completed and hard-failed pipelines from waiting
    for pid, p in list(s.waiting.items()):
        status = p.runtime_status()
        if status.is_pipeline_successful():
            _drop_pipeline(pid)
            continue
        if s.hard_fail.get(pid, 0) > 0:
            _drop_pipeline(pid)
            continue
        # If the pipeline has failures, we only keep it if it's retryable (OOM).
        # This relies on OOM failing ops being in FAILED state and becoming assignable again.
        # If failures accumulate but are non-OOM, the result handler will mark hard-fail.

    # Helper to test whether there is any high-priority runnable work system-wide
    def _has_high_pri_runnable():
        for pid, p in s.waiting.items():
            if p.priority in (Priority.QUERY, Priority.INTERACTIVE):
                if _get_assignable_op(p) is not None:
                    return True
        return False

    high_pri_waiting = _has_high_pri_runnable()

    # Per-pool scheduling loop
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool
        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # Soft reservation: when high-priority is waiting, limit batch usage so that
        # some headroom remains for upcoming high-priority placements in this tick.
        reserve_cpu = 0
        reserve_ram = 0
        if high_pri_waiting:
            reserve_cpu = max(1, int(round(pool.max_cpu_pool * 0.25)))
            reserve_ram = max(1, int(round(pool.max_ram_pool * 0.20)))

        # Candidate queues in strict priority order
        q_order = [s.q_query, s.q_interactive, s.q_batch]

        # Avoid infinite loops if many pipelines are not runnable or don't fit
        # We'll attempt limited number of pops per queue per pool per tick, and reappend.
        max_attempts = 64
        attempts = 0

        while attempts < max_attempts and avail_cpu > 0 and avail_ram > 0:
            attempts += 1
            chosen_pid = None
            chosen_pipeline = None
            chosen_op = None

            # Find next runnable pipeline in priority order
            for q in q_order:
                if not q:
                    continue

                # Scan a small window to skip stale/dropped/unrunnable entries
                scan = min(len(q), 8)
                picked_index = None
                for i in range(scan):
                    pid = q[i]
                    p = s.waiting.get(pid)
                    if p is None:
                        continue
                    op = _get_assignable_op(p)
                    if op is None:
                        continue
                    # If batch and high-priority waiting, ensure we don't consume reserved headroom
                    if p.priority == Priority.BATCH_PIPELINE and high_pri_waiting:
                        if avail_cpu <= reserve_cpu or avail_ram <= reserve_ram:
                            continue
                    picked_index = i
                    chosen_pid = pid
                    chosen_pipeline = p
                    chosen_op = op
                    break

                if chosen_pid is not None:
                    # Move chosen pid to the back (round-robin within same priority)
                    q.pop(picked_index)
                    q.append(chosen_pid)
                    break
                else:
                    # Rotate queue to avoid repeatedly scanning same stale head entries
                    head = q.pop(0)
                    q.append(head)

            if chosen_pid is None:
                break  # nothing runnable

            # Compute resource request using per-op hint (RAM) and priority-based CPU share
            opk = _op_key(chosen_op)
            hint_key = (chosen_pid, opk)

            # Base RAM target by priority + pool size, then apply any learned hint
            base_ram = max(1, int(round(pool.max_ram_pool * _base_ram_fraction(chosen_pipeline.priority))))
            learned_ram = s.ram_hint.get(hint_key)
            req_ram = learned_ram if (learned_ram is not None and learned_ram > 0) else base_ram
            req_ram = min(req_ram, pool.max_ram_pool)

            # CPU target: priority-based share, but never exceed currently available
            req_cpu = _cpu_share(chosen_pipeline.priority, pool.max_cpu_pool)
            req_cpu = min(req_cpu, avail_cpu)

            # If RAM doesn't fit, try a smaller "best-effort" downscale only when we have no learned hint.
            if req_ram > avail_ram:
                if learned_ram is None:
                    # Shrink to available, but keep at least 1.
                    req_ram = max(1, int(avail_ram))
                else:
                    # Learned hint says it needs more; don't thrash by scheduling it underprovisioned.
                    continue

            # If CPU is 0 after min, we can't schedule
            if req_cpu <= 0 or req_ram <= 0:
                break

            # Create assignment (one op at a time)
            assignments.append(
                Assignment(
                    ops=[chosen_op],
                    cpu=req_cpu,
                    ram=req_ram,
                    priority=chosen_pipeline.priority,
                    pool_id=pool_id,
                    pipeline_id=chosen_pipeline.pipeline_id,
                )
            )

            avail_cpu -= req_cpu
            avail_ram -= req_ram

    return suspensions, assignments
