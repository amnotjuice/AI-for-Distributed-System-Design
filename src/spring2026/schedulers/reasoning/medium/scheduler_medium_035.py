# policy_key: scheduler_medium_035
# reasoning_effort: medium
# model: gpt-5.2-2025-12-11
# llm_cost: 0.100284
# generation_seconds: 116.66
# generated_at: 2026-03-14T03:04:31.608520
@register_scheduler_init(key="scheduler_medium_035")
def scheduler_medium_035_init(s):
    """Priority-aware, retry-on-OOM scheduler.

    Incremental improvements over naive FIFO:
      1) Use separate queues per priority and always prefer higher priority work.
      2) Do NOT drop pipelines on failure if the failure looks like OOM; instead, retry
         the failed operator with increased RAM next time.
      3) Avoid letting batch consume the entire pool when higher-priority work is waiting,
         by reserving a small headroom fraction (soft reservation).
      4) Place each assignment on the pool with the most headroom that can satisfy the
         RAM requirement, to reduce queueing and OOM risk.
    """
    # Waiting queues per priority (round-robin within each).
    from collections import deque

    s.queues = {
        Priority.QUERY: deque(),
        Priority.INTERACTIVE: deque(),
        Priority.BATCH_PIPELINE: deque(),
    }

    # Hints keyed by operator object identity (stable within a simulation run).
    # - ram_hint is treated as a minimum requirement (to avoid repeating OOM).
    # - cpu_hint is a preference; we may scale it down if the pool is tight.
    s.op_ram_hint = {}  # id(op) -> ram
    s.op_cpu_hint = {}  # id(op) -> cpu

    # Track whether a FAILED op is retryable (currently only OOM is treated as retryable).
    s.retryable_failed_ops = {}  # id(op) -> bool

    # Soft reservation used only to limit BATCH when interactive/query are waiting.
    s.reserve_frac_cpu = 0.20
    s.reserve_frac_ram = 0.20

    # Keep the scheduler from doing unbounded scanning in a single tick.
    s.max_pipeline_scans_per_priority = 128


def _sched035_is_oom_error(err):
    """Best-effort OOM classification based on error string."""
    if err is None:
        return False
    try:
        msg = str(err).lower()
    except Exception:
        return False
    # Common variants across runtimes.
    return ("oom" in msg) or ("out of memory" in msg) or ("memoryerror" in msg)


def _sched035_priority_order():
    # Highest to lowest priority.
    return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]


def _sched035_default_cpu_ram_for_priority(pool, priority):
    """Conservative per-op defaults that trade off latency vs. concurrency."""
    max_cpu = pool.max_cpu_pool
    max_ram = pool.max_ram_pool

    # CPU defaults: give more to high-priority to reduce latency.
    if priority == Priority.QUERY:
        cpu = min(8.0, max(2.0, 0.50 * max_cpu))
        ram = max(1.0, 0.30 * max_ram)
        min_cpu = 2.0
    elif priority == Priority.INTERACTIVE:
        cpu = min(6.0, max(2.0, 0.33 * max_cpu))
        ram = max(1.0, 0.25 * max_ram)
        min_cpu = 2.0
    else:  # Priority.BATCH_PIPELINE (or unknown -> treat as batch)
        cpu = min(4.0, max(1.0, 0.25 * max_cpu))
        ram = max(1.0, 0.20 * max_ram)
        min_cpu = 1.0

    # Cap by pool maxima (scheduler will cap by current availability later).
    cpu = min(cpu, max_cpu)
    ram = min(ram, max_ram)
    return cpu, ram, min_cpu


def _sched035_pick_pool_and_resources(
    s,
    pools,
    local_avail_cpu,
    local_avail_ram,
    priority,
    op,
    high_prio_waiting,
):
    """Choose the best pool that can fit the op's RAM hint; return (pool_id, cpu, ram) or (None, None, None)."""
    op_key = id(op)

    best = None  # (score, pool_id, cpu, ram)
    for pool_id, pool in enumerate(pools):
        avail_cpu = local_avail_cpu[pool_id]
        avail_ram = local_avail_ram[pool_id]
        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # Soft reservation: when high-priority work is waiting, keep headroom by
        # limiting what batch is allowed to consume.
        if priority == Priority.BATCH_PIPELINE and high_prio_waiting:
            avail_cpu = max(0.0, avail_cpu - s.reserve_frac_cpu * pool.max_cpu_pool)
            avail_ram = max(0.0, avail_ram - s.reserve_frac_ram * pool.max_ram_pool)

        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        default_cpu, default_ram, min_cpu = _sched035_default_cpu_ram_for_priority(pool, priority)

        # RAM: treat hint as a minimum (avoid repeated OOM).
        ram_req = s.op_ram_hint.get(op_key, default_ram)
        ram_req = min(ram_req, pool.max_ram_pool)

        # CPU: hint is a preference; we can reduce if necessary.
        cpu_pref = s.op_cpu_hint.get(op_key, default_cpu)
        cpu_pref = min(cpu_pref, pool.max_cpu_pool)

        if avail_ram < ram_req:
            continue

        # Allocate as much CPU as we can up to cpu_pref, but not below min_cpu if possible.
        cpu_alloc = min(cpu_pref, avail_cpu)
        if cpu_alloc < 1.0:
            continue
        if cpu_alloc < min_cpu and avail_cpu >= min_cpu:
            cpu_alloc = min_cpu

        ram_alloc = ram_req  # we intentionally do not shrink below ram_req

        # Score: prefer placing on pools with more remaining headroom after placing this op.
        rem_cpu = avail_cpu - cpu_alloc
        rem_ram = avail_ram - ram_alloc
        score = rem_cpu + (rem_ram / (pool.max_ram_pool + 1e-9))  # normalize RAM term
        if best is None or score > best[0]:
            best = (score, pool_id, cpu_alloc, ram_alloc)

    if best is None:
        return None, None, None
    _, pool_id, cpu_alloc, ram_alloc = best
    return pool_id, cpu_alloc, ram_alloc


@register_scheduler(key="scheduler_medium_035")
def scheduler_medium_035(s, results, pipelines):
    """
    Priority-aware round-robin scheduler with OOM-aware retries and soft batch headroom reservation.

    Key behaviors:
      - Always try QUERY first, then INTERACTIVE, then BATCH.
      - Retry FAILED operators only if they previously failed with an OOM-like error.
      - On OOM: increase RAM hint for the operator (exponential backoff).
      - When any high-priority pipelines are waiting, limit batch from consuming the last ~20% of each pool.
      - Choose the pool with the best post-placement headroom to reduce queueing and fragmentation.
    """
    # Enqueue newly arrived pipelines into the right priority queue.
    for p in pipelines:
        pr = p.priority
        if pr not in s.queues:
            pr = Priority.BATCH_PIPELINE
        s.queues[pr].append(p)

    # Incorporate execution feedback (OOM -> increase RAM hints; mark failed op as retryable).
    for r in results:
        # If the executor returns the operator(s) for this container, use them to update hints.
        try:
            ops = r.ops or []
        except Exception:
            ops = []

        is_oom = False
        try:
            is_oom = r.failed() and _sched035_is_oom_error(r.error)
        except Exception:
            # If r.failed() isn't reliable, fall back to error string only.
            is_oom = _sched035_is_oom_error(getattr(r, "error", None))

        if is_oom:
            # Exponential RAM backoff: double the last attempted RAM.
            # Cap by the pool's max RAM (if known).
            pool_max_ram = None
            try:
                pool_max_ram = s.executor.pools[r.pool_id].max_ram_pool
            except Exception:
                pool_max_ram = None

            last_ram = getattr(r, "ram", None)
            for op in ops:
                op_key = id(op)
                s.retryable_failed_ops[op_key] = True
                prev = s.op_ram_hint.get(op_key, None)
                if last_ram is not None:
                    new_hint = float(last_ram) * 2.0
                elif prev is not None:
                    new_hint = float(prev) * 2.0
                else:
                    # If we have no signal, start with a modest bump.
                    new_hint = 2.0
                if pool_max_ram is not None:
                    new_hint = min(new_hint, float(pool_max_ram))
                s.op_ram_hint[op_key] = max(float(prev) if prev is not None else 0.0, float(new_hint))
        else:
            # Successful completion: (optional) remember CPU used as a gentle preference.
            # We do not reduce RAM hints here to avoid oscillations.
            try:
                if not r.failed():
                    last_cpu = getattr(r, "cpu", None)
                    if last_cpu is not None:
                        for op in ops:
                            op_key = id(op)
                            # Keep the larger of existing hint and observed CPU to avoid undersizing.
                            prev_cpu = s.op_cpu_hint.get(op_key, 0.0)
                            s.op_cpu_hint[op_key] = max(float(prev_cpu), float(last_cpu))
            except Exception:
                pass

    # If nothing changed, early exit.
    if not pipelines and not results:
        return [], []

    suspensions = []  # No preemption in this incremental policy.
    assignments = []

    pools = s.executor.pools
    num_pools = s.executor.num_pools

    local_avail_cpu = [float(pools[i].avail_cpu_pool) for i in range(num_pools)]
    local_avail_ram = [float(pools[i].avail_ram_pool) for i in range(num_pools)]

    # If there is any queued high-priority work, apply soft reservation for batch.
    # (We conservatively use queue non-emptiness rather than "runnable now".)
    high_prio_waiting = (len(s.queues[Priority.QUERY]) > 0) or (len(s.queues[Priority.INTERACTIVE]) > 0)

    # Main loop: keep making assignments while any can be made.
    made_progress = True
    while made_progress:
        made_progress = False

        # Stop if all pools are fully utilized.
        if all(c <= 0.0 for c in local_avail_cpu) or all(r <= 0.0 for r in local_avail_ram):
            break

        for priority in _sched035_priority_order():
            q = s.queues[priority]
            if not q:
                continue

            scans = 0
            # Try to find a runnable pipeline in this priority class (bounded scans).
            while q and scans < min(len(q), s.max_pipeline_scans_per_priority):
                scans += 1
                pipeline = q.popleft()

                status = pipeline.runtime_status()
                if status.is_pipeline_successful():
                    # Drop completed pipelines.
                    continue

                # If there are failed ops, only continue if all failures are retryable (OOM-like).
                failed_ops = status.get_ops([OperatorState.FAILED], require_parents_complete=False)
                if failed_ops:
                    non_retryable = False
                    for op in failed_ops:
                        if not s.retryable_failed_ops.get(id(op), False):
                            non_retryable = True
                            break
                    if non_retryable:
                        # Drop pipelines with non-retryable failures (same behavior as naive, but safer).
                        continue

                # Choose exactly one op at a time to avoid head-of-line blocking and improve fairness.
                pending_ops = status.get_ops([OperatorState.PENDING], require_parents_complete=True)
                retry_ops = status.get_ops([OperatorState.FAILED], require_parents_complete=True)
                retry_ops = [op for op in retry_ops if s.retryable_failed_ops.get(id(op), False)]

                op_list = (pending_ops[:1] if pending_ops else retry_ops[:1])
                if not op_list:
                    # Not runnable yet; put it back for later.
                    q.append(pipeline)
                    continue

                op = op_list[0]

                pool_id, cpu_alloc, ram_alloc = _sched035_pick_pool_and_resources(
                    s=s,
                    pools=pools,
                    local_avail_cpu=local_avail_cpu,
                    local_avail_ram=local_avail_ram,
                    priority=priority,
                    op=op,
                    high_prio_waiting=high_prio_waiting,
                )

                if pool_id is None:
                    # Can't fit right now on any pool; put it back and try others.
                    q.append(pipeline)
                    continue

                # Commit assignment and update local pool headroom.
                assignments.append(
                    Assignment(
                        ops=[op],
                        cpu=cpu_alloc,
                        ram=ram_alloc,
                        priority=pipeline.priority,
                        pool_id=pool_id,
                        pipeline_id=pipeline.pipeline_id,
                    )
                )
                local_avail_cpu[pool_id] -= float(cpu_alloc)
                local_avail_ram[pool_id] -= float(ram_alloc)

                # Requeue pipeline for future ops.
                q.append(pipeline)

                made_progress = True
                # Move on to next priority to avoid starving lower priorities entirely.
                break

    return suspensions, assignments
