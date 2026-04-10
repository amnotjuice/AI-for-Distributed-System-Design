# source: scheduler_low_011.py
# model: gpt-5.2-2025-12-11
# effort: low
# feedback: minimal
# iteration: r15
@register_scheduler_init(key="scheduler_low_011_r15")
def scheduler_low_011_r15_init(s):
    """Priority-aware FIFO++ scheduler focused on reducing queueing latency.

    Incremental improvements over prior version:
    - Per-priority FIFO queues with de-dup (avoid repeated pipeline entries).
    - Head-of-line blocking mitigation: small lookahead scan to find a ready op that fits.
    - Fill pools more than "one op per pool per tick" (bounded) to reduce wait time under load.
    - Capacity reservation (no preemption): when high-priority backlog exists, throttle new BATCH
      admissions to keep headroom for QUERY/INTERACTIVE, improving tail latency.
    - OOM-aware retries: exponential RAM backoff per-operator (by op object id), bounded retries.
    """
    from collections import deque

    # FIFO queues per priority
    s.q = {
        Priority.QUERY: deque(),
        Priority.INTERACTIVE: deque(),
        Priority.BATCH_PIPELINE: deque(),
    }
    # De-dup sets per priority (pipeline_id)
    s.in_q = {
        Priority.QUERY: set(),
        Priority.INTERACTIVE: set(),
        Priority.BATCH_PIPELINE: set(),
    }

    # OOM learning keyed by operator object identity (id(op))
    s.op_hints = {}        # op_id -> {"ram": float, "cpu": float}
    s.op_attempts = {}     # op_id -> int
    s.oom_retryable = set()  # op_id that most recently failed due to OOM and may be retried
    s.nonretryable_failed = set()  # op_id failed due to non-OOM (do not retry)

    # Small, safe knobs
    s.max_retries_per_op = 3
    s.scan_limit = 8                 # lookahead depth per priority queue (mitigate HOL blocking)
    s.max_assignments_per_pool = 6   # bound per-tick work to keep scheduling overhead low

    # Pool preference
    s.interactive_pool_id = 0

    # Sizing fractions of POOL MAX (requests are then capped by current available)
    # (We bias high-priority to larger allocations to reduce runtime, but still allow concurrency.)
    s.frac_interactive_pool = {
        Priority.QUERY: {"cpu": 0.75, "ram": 0.75},
        Priority.INTERACTIVE: {"cpu": 0.75, "ram": 0.75},
        Priority.BATCH_PIPELINE: {"cpu": 0.50, "ram": 0.50},  # if batch allowed on interactive pool
    }
    s.frac_other_pools = {
        Priority.QUERY: {"cpu": 0.60, "ram": 0.60},          # allow spillover if interactive pool is tight
        Priority.INTERACTIVE: {"cpu": 0.60, "ram": 0.60},
        Priority.BATCH_PIPELINE: {"cpu": 1.00, "ram": 1.00},
    }

    # Headroom reservation when high-priority backlog exists (no preemption available here)
    # Only applied to *new BATCH* admissions.
    s.reserve_frac_interactive_pool = 0.25
    s.reserve_frac_other_pools = 0.10


def _prio_order():
    return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]


def _is_oom_error(err):
    if err is None:
        return False
    msg = str(err).lower()
    return ("oom" in msg) or ("out of memory" in msg) or ("memoryerror" in msg)


def _enqueue_pipeline(s, p):
    pr = p.priority if p.priority in s.q else Priority.BATCH_PIPELINE
    pid = p.pipeline_id
    if pid not in s.in_q[pr]:
        s.q[pr].append(p)
        s.in_q[pr].add(pid)


def _requeue_pipeline(s, p):
    # Requeue to tail within its priority (FIFO). De-dup protected.
    _enqueue_pipeline(s, p)


def _pop_left(s, pr):
    # Pop left and clear de-dup marker
    p = s.q[pr].popleft()
    s.in_q[pr].discard(p.pipeline_id)
    return p


def _pipeline_drop_or_keep(s, p):
    """Return True if pipeline should be kept in queues, False if it should be dropped."""
    st = p.runtime_status()
    if st.is_pipeline_successful():
        return False

    # If there are FAILED ops, only keep if *all* failed ops are OOM-retryable and within retry budget.
    try:
        failed_ops = st.get_ops([OperatorState.FAILED], require_parents_complete=False)
    except Exception:
        failed_ops = []

    if failed_ops:
        for op in failed_ops:
            oid = id(op)
            if oid in s.nonretryable_failed:
                return False
            if oid not in s.oom_retryable:
                return False
            if int(s.op_attempts.get(oid, 0)) > int(s.max_retries_per_op):
                return False
        return True

    return True


def _next_assignable_op(p):
    st = p.runtime_status()
    ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
    if not ops:
        return None
    return ops[0]


def _pool_is_interactive(s, pool_id):
    return (s.executor.num_pools > 1) and (pool_id == getattr(s, "interactive_pool_id", 0))


def _request_size(s, pool, pool_id, priority, op):
    # Choose fractions based on pool role
    if _pool_is_interactive(s, pool_id):
        fr = s.frac_interactive_pool.get(priority, {"cpu": 1.0, "ram": 1.0})
    else:
        fr = s.frac_other_pools.get(priority, {"cpu": 1.0, "ram": 1.0})

    # Default request based on pool MAX (not current avail), then capped
    cpu = max(1.0, float(pool.max_cpu_pool) * float(fr["cpu"]))
    ram = max(1.0, float(pool.max_ram_pool) * float(fr["ram"]))

    # Apply learned OOM hints
    oid = id(op)
    hint = s.op_hints.get(oid)
    if hint:
        cpu = max(cpu, float(hint.get("cpu", cpu)))
        ram = max(ram, float(hint.get("ram", ram)))

    # Cap to what is currently available
    cpu = min(cpu, float(pool.avail_cpu_pool), float(pool.max_cpu_pool))
    ram = min(ram, float(pool.avail_ram_pool), float(pool.max_ram_pool))

    # Ensure still positive
    cpu = max(1.0, cpu)
    ram = max(1.0, ram)
    return cpu, ram


def _batch_budget(s, pool, pool_id, hp_backlog):
    """Return (cpu_budget, ram_budget) allowed for NEW batch assignments in this pool."""
    avail_cpu = float(pool.avail_cpu_pool)
    avail_ram = float(pool.avail_ram_pool)
    if not hp_backlog:
        return avail_cpu, avail_ram

    if _pool_is_interactive(s, pool_id):
        rfrac = float(getattr(s, "reserve_frac_interactive_pool", 0.25))
    else:
        rfrac = float(getattr(s, "reserve_frac_other_pools", 0.10))

    reserve_cpu = float(pool.max_cpu_pool) * rfrac
    reserve_ram = float(pool.max_ram_pool) * rfrac
    return max(0.0, avail_cpu - reserve_cpu), max(0.0, avail_ram - reserve_ram)


@register_scheduler(key="scheduler_low_011_r15")
def scheduler_low_011_r15(s, results, pipelines):
    """
    Priority-first, lookahead, headroom-reserving scheduler.

    High-level flow per tick:
    1) Enqueue new pipelines (priority FIFO with de-dup).
    2) Process results to learn OOM RAM requirements and mark retryable failures.
    3) For each pool, greedily assign multiple operators (bounded), always choosing highest priority first,
       using lookahead to avoid head-of-line blocking, and reserving headroom from new batch admissions
       when high-priority backlog exists.
    """
    # 1) Enqueue new arrivals
    for p in pipelines:
        _enqueue_pipeline(s, p)

    # 2) Process results to update retry/hints
    for r in results:
        try:
            is_failed = bool(r.failed())
        except Exception:
            is_failed = getattr(r, "error", None) is not None

        ops = getattr(r, "ops", None) or []

        if not is_failed:
            # Clear retry markers on success (best-effort)
            for op in ops:
                oid = id(op)
                s.oom_retryable.discard(oid)
                s.nonretryable_failed.discard(oid)
            continue

        err = getattr(r, "error", None)
        if _is_oom_error(err):
            # Exponential RAM backoff based on last observed allocation if available
            obs_ram = float(getattr(r, "ram", 0.0) or 0.0)
            obs_cpu = float(getattr(r, "cpu", 0.0) or 0.0)
            for op in ops:
                oid = id(op)
                prev = s.op_hints.get(oid, {})
                prev_ram = float(prev.get("ram", 0.0) or 0.0)
                prev_cpu = float(prev.get("cpu", 0.0) or 0.0)

                baseline_ram = max(1.0, obs_ram, prev_ram)
                new_ram = max(1.0, baseline_ram * 2.0)

                baseline_cpu = max(1.0, obs_cpu, prev_cpu)
                s.op_hints[oid] = {"ram": new_ram, "cpu": baseline_cpu}

                s.op_attempts[oid] = int(s.op_attempts.get(oid, 0)) + 1
                s.oom_retryable.add(oid)
                s.nonretryable_failed.discard(oid)
        else:
            # Mark as non-retryable; pipeline will be dropped on observation
            for op in ops:
                oid = id(op)
                s.nonretryable_failed.add(oid)
                s.oom_retryable.discard(oid)

    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    # High-priority backlog indicator (used for headroom reservation)
    hp_backlog = (len(s.q[Priority.QUERY]) + len(s.q[Priority.INTERACTIVE])) > 0

    # Greedy fill: prioritize interactive pool first so high-priority work gets first shot at headroom
    pool_order = list(range(s.executor.num_pools))
    if s.executor.num_pools > 1 and getattr(s, "interactive_pool_id", 0) in pool_order:
        ip = getattr(s, "interactive_pool_id", 0)
        pool_order = [ip] + [i for i in pool_order if i != ip]

    for pool_id in pool_order:
        pool = s.executor.pools[pool_id]
        if float(pool.avail_cpu_pool) <= 0.0 or float(pool.avail_ram_pool) <= 0.0:
            continue

        made = 0
        # Try to make multiple assignments until resources are too small or we've hit the per-pool cap.
        while made < int(getattr(s, "max_assignments_per_pool", 6)):
            if float(pool.avail_cpu_pool) < 1.0 or float(pool.avail_ram_pool) < 1.0:
                break

            chosen = None  # (pipeline, pr, op, cpu, ram)
            # Lookahead scan per priority to avoid HOL blocking
            for pr in _prio_order():
                qlen = len(s.q[pr])
                if qlen == 0:
                    continue

                scan = min(int(getattr(s, "scan_limit", 8)), qlen)
                found = None
                rotated = 0

                # Rotate through up to `scan` pipelines to find an op that is ready and fits
                while rotated < scan and len(s.q[pr]) > 0:
                    p = _pop_left(s, pr)

                    # Drop completed / non-retryable failed pipelines early
                    if not _pipeline_drop_or_keep(s, p):
                        rotated += 1
                        continue

                    op = _next_assignable_op(p)
                    if op is None:
                        # Not ready now; keep in FIFO by requeueing to tail
                        _requeue_pipeline(s, p)
                        rotated += 1
                        continue

                    # Enforce retry budget for FAILED ops (ASSIGNABLE includes FAILED)
                    if id(op) in s.oom_retryable and int(s.op_attempts.get(id(op), 0)) > int(s.max_retries_per_op):
                        # Over budget: treat as non-retryable
                        s.nonretryable_failed.add(id(op))
                        rotated += 1
                        continue

                    cpu, ram = _request_size(s, pool, pool_id, pr, op)

                    # Apply headroom reservation for NEW batch work when high-priority backlog exists
                    if pr == Priority.BATCH_PIPELINE:
                        cpu_budget, ram_budget = _batch_budget(s, pool, pool_id, hp_backlog)
                        if cpu > cpu_budget or ram > ram_budget:
                            # Too big for the batch budget; push back to tail and keep scanning
                            _requeue_pipeline(s, p)
                            rotated += 1
                            continue

                    # Must fit current availability
                    if cpu <= float(pool.avail_cpu_pool) and ram <= float(pool.avail_ram_pool):
                        found = (p, pr, op, cpu, ram)
                        break

                    # Doesn't fit now; requeue to tail and continue scanning
                    _requeue_pipeline(s, p)
                    rotated += 1

                if found is not None:
                    chosen = found
                    break

            if chosen is None:
                break

            p, pr, op, cpu, ram = chosen

            # Create assignment (one operator per container)
            assignments.append(
                Assignment(
                    ops=[op],
                    cpu=cpu,
                    ram=ram,
                    priority=pr,
                    pool_id=pool_id,
                    pipeline_id=p.pipeline_id,
                )
            )

            # Requeue pipeline so its next op(s) can be considered in future ticks
            _requeue_pipeline(s, p)

            made += 1

    return suspensions, assignments