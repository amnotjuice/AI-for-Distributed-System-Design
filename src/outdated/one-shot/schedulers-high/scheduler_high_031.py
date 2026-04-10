# policy_key: scheduler_high_031
# reasoning_effort: high
# model: gpt-5.2-2025-12-11
# llm_cost: 0.093995
# generation_seconds: 132.07
# generated_at: 2026-03-14T03:53:13.406211
@register_scheduler_init(key="scheduler_high_031")
def scheduler_high_031_init(s):
    """Priority-aware, right-sized, OOM-adaptive scheduler.

    Improvements over naive FIFO:
      1) Separate queues per priority and always schedule higher priority first.
      2) Avoid "give the whole pool to one op" by capping per-op CPU/RAM shares
         to improve concurrency and tail latency for interactive work.
      3) Reserve a small headroom slice when high-priority work is waiting, so
         background work cannot fill the pool and block quick admissions.
      4) On OOM failures, retry the same operator with a larger RAM hint (simple
         multiplicative backoff). Non-OOM failures are treated as terminal.
    """
    from collections import deque, defaultdict

    # Per-priority queues (FIFO within each class).
    s.q_query = deque()
    s.q_interactive = deque()
    s.q_batch = deque()

    # Operator-level failure handling.
    s.op_ram_hint = {}  # op_key -> next_ram_to_try
    s.op_oom_retries = defaultdict(int)  # op_key -> count
    s.oom_ops = set()  # ops that have failed due to OOM at least once
    s.hard_fail_ops = set()  # ops that failed for non-OOM reasons or exceeded retry budget

    # Simple logical clock (ticks) for potential future improvements.
    s.tick = 0

    # Tuning knobs (kept intentionally simple).
    s.reserve_frac = 0.20          # reserve this fraction for high-priority when any is waiting
    s.max_oom_retries = 4          # after this, treat as hard failure (prevents infinite loops)
    s.ram_backoff_mult = 2.0       # on OOM, multiply RAM allocation by this
    s.ram_backoff_add = 1          # ...and add this (helps small RAM values grow)


@register_scheduler(key="scheduler_high_031")
def scheduler_high_031(s, results, pipelines):
    """Scheduler step: process results, enqueue arrivals, then place ready ops.

    Returns:
      suspensions: currently unused (no preemption in this first incremental policy)
      assignments: list of new container assignments
    """
    import math

    def _ceil(x):
        return int(math.ceil(x))

    def _is_oom(err):
        if err is None:
            return False
        try:
            msg = str(err).lower()
        except Exception:
            return False
        return ("oom" in msg) or ("out of memory" in msg) or ("out-of-memory" in msg) or ("killed" in msg)

    def _op_key(op):
        """Best-effort stable key for an operator across retries/results."""
        # Prefer explicit identifiers if present; fall back to object identity.
        for attr in ("operator_id", "op_id", "id", "name"):
            if hasattr(op, attr):
                try:
                    v = getattr(op, attr)
                    # Ensure hashable / stable-ish.
                    return (attr, str(v))
                except Exception:
                    pass
        return ("py_id", str(id(op)))

    def _cpu_cap(priority, max_cpu):
        # Give more CPU to higher priority to reduce latency, but cap to preserve concurrency.
        if priority == Priority.QUERY:
            frac = 0.60
        elif priority == Priority.INTERACTIVE:
            frac = 0.45
        else:
            frac = 0.30
        return max(1, _ceil(max_cpu * frac))

    def _ram_cap(priority, max_ram):
        if priority == Priority.QUERY:
            frac = 0.60
        elif priority == Priority.INTERACTIVE:
            frac = 0.45
        else:
            frac = 0.30
        return max(1, _ceil(max_ram * frac))

    def _enqueue_pipeline(p):
        if p.priority == Priority.QUERY:
            s.q_query.append(p)
        elif p.priority == Priority.INTERACTIVE:
            s.q_interactive.append(p)
        else:
            s.q_batch.append(p)

    def _any_high_prio_waiting():
        return (len(s.q_query) > 0) or (len(s.q_interactive) > 0)

    s.tick += 1

    # 1) Process execution results to learn from failures (especially OOM).
    for r in results:
        if not r.failed():
            continue

        ops = r.ops or []
        oom = _is_oom(r.error)
        pool = None
        try:
            pool = s.executor.pools[r.pool_id]
        except Exception:
            pool = None

        if oom:
            for op in ops:
                k = _op_key(op)
                s.oom_ops.add(k)
                s.op_oom_retries[k] += 1

                # Compute next RAM hint from the last attempted RAM.
                try:
                    last_ram = float(r.ram)
                except Exception:
                    last_ram = 0.0

                bumped = (last_ram * float(s.ram_backoff_mult)) + float(s.ram_backoff_add)
                bumped_int = max(1, int(bumped))

                # Clamp to pool maximum if known.
                if pool is not None:
                    try:
                        bumped_int = min(bumped_int, int(pool.max_ram_pool))
                    except Exception:
                        pass

                # Monotonic increase.
                prev = s.op_ram_hint.get(k, 0)
                if bumped_int > prev:
                    s.op_ram_hint[k] = bumped_int

                # Prevent infinite OOM loops.
                if s.op_oom_retries[k] >= int(s.max_oom_retries):
                    s.hard_fail_ops.add(k)
        else:
            # Treat non-OOM failures as terminal for that operator.
            for op in ops:
                s.hard_fail_ops.add(_op_key(op))

    # 2) Admit newly arrived pipelines (priority queues).
    for p in pipelines:
        _enqueue_pipeline(p)

    # Early exit if nothing changed and no new work.
    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    # 3) Placement loop: for each pool, schedule as many ops as resources allow.
    priority_queues = [s.q_query, s.q_interactive, s.q_batch]
    scheduled_pipeline_ids = set()

    high_waiting = _any_high_prio_waiting()

    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]

        # Current free resources in this pool.
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool
        if avail_cpu is None or avail_ram is None:
            continue
        if avail_cpu < 1 or avail_ram < 1:
            continue

        # Reserve headroom only when high-priority is waiting, so batch doesn't "fill the pool".
        reserve_cpu = _ceil(float(pool.max_cpu_pool) * float(s.reserve_frac)) if high_waiting else 0
        reserve_ram = _ceil(float(pool.max_ram_pool) * float(s.reserve_frac)) if high_waiting else 0

        local_cpu = float(avail_cpu)
        local_ram = float(avail_ram)

        made_progress = True
        while made_progress:
            made_progress = False

            # Stop if we can't fit even a minimal container.
            if local_cpu < 1 or local_ram < 1:
                break

            # Iterate priorities in order: QUERY -> INTERACTIVE -> BATCH
            for q in priority_queues:
                # For batch, enforce reservation if high-priority is waiting.
                eff_cpu = local_cpu
                eff_ram = local_ram
                if (q is s.q_batch) and high_waiting:
                    eff_cpu = max(0.0, local_cpu - float(reserve_cpu))
                    eff_ram = max(0.0, local_ram - float(reserve_ram))

                if eff_cpu < 1 or eff_ram < 1:
                    continue

                # Search this queue (round-robin) for a schedulable pipeline.
                n = len(q)
                if n == 0:
                    continue

                for _ in range(n):
                    pipeline = q.popleft()
                    pid = pipeline.pipeline_id

                    # Avoid scheduling multiple ops from the same pipeline in one tick (reduces burstiness).
                    if pid in scheduled_pipeline_ids:
                        q.append(pipeline)
                        continue

                    status = pipeline.runtime_status()

                    # Drop completed pipelines.
                    if status.is_pipeline_successful():
                        continue

                    # If there are FAILED ops, only keep the pipeline if all such failures are OOM-retriable.
                    failed_ops = status.get_ops([OperatorState.FAILED], require_parents_complete=False)
                    if failed_ops:
                        hard = False
                        for op in failed_ops:
                            if _op_key(op) in s.hard_fail_ops:
                                hard = True
                                break
                        if hard:
                            # Drop terminally failed pipelines (don't requeue).
                            continue

                    # Get one next-ready op (parents complete).
                    op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
                    if not op_list:
                        # Nothing ready right now; keep pipeline in its queue.
                        q.append(pipeline)
                        continue

                    op = op_list[0]
                    k = _op_key(op)

                    # If this op is known-terminal, drop the pipeline.
                    if k in s.hard_fail_ops:
                        continue

                    # Determine per-op resource request (capped slice, not "all remaining").
                    cap_cpu = _cpu_cap(pipeline.priority, pool.max_cpu_pool)
                    cap_ram = _ram_cap(pipeline.priority, pool.max_ram_pool)

                    req_cpu = min(float(cap_cpu), float(eff_cpu))
                    req_ram = min(float(cap_ram), float(eff_ram))

                    # Apply OOM-based RAM hint if present.
                    if k in s.op_ram_hint:
                        req_ram = max(req_ram, float(s.op_ram_hint[k]))
                        req_ram = min(req_ram, float(eff_ram))

                    # Convert to ints (sim expects numeric amounts; ints are safest).
                    cpu_i = int(req_cpu)
                    ram_i = int(req_ram)

                    # Ensure valid non-zero allocation and within true local availability.
                    cpu_i = min(cpu_i, int(local_cpu))
                    ram_i = min(ram_i, int(local_ram))
                    if cpu_i < 1 or ram_i < 1:
                        # Can't fit; keep pipeline queued and try other pipelines/priorities.
                        q.append(pipeline)
                        continue

                    assignments.append(
                        Assignment(
                            ops=op_list,
                            cpu=cpu_i,
                            ram=ram_i,
                            priority=pipeline.priority,
                            pool_id=pool_id,
                            pipeline_id=pid,
                        )
                    )

                    # Account resources locally so we can schedule multiple ops per pool per tick.
                    local_cpu -= float(cpu_i)
                    local_ram -= float(ram_i)
                    scheduled_pipeline_ids.add(pid)

                    # Requeue the pipeline for future steps (it will advance as ops complete).
                    q.append(pipeline)

                    made_progress = True
                    break  # move to next priority level after placing one op

                # If we placed something, restart priority scan to favor higher priority again.
                if made_progress:
                    break

    return suspensions, assignments
