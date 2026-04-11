# policy_key: scheduler_none_005
# reasoning_effort: none
# model: gpt-5.2-2025-12-11
# llm_cost: 0.044295
# generation_seconds: 43.23
# generated_at: 2026-03-12T21:24:44.436778
@register_scheduler_init(key="scheduler_none_005")
def scheduler_none_005_init(s):
    """Priority-aware, low-risk improvements over naive FIFO.

    Goals / small-step improvements:
      1) Keep separate queues per priority and always try to schedule higher priority first.
      2) Avoid "one giant assignment" that monopolizes an entire pool by using per-priority CPU/RAM caps.
      3) Add simple OOM-aware RAM backoff: if an op fails with OOM, retry with more RAM next time.
      4) Light preemption: if high-priority work cannot be scheduled due to insufficient headroom, suspend
         some lower-priority running containers to free resources (limited churn).

    This stays conservative: schedules at most one operator per pipeline per tick, per pool, and avoids
    sophisticated runtime prediction.
    """
    # Per-priority waiting queues (pipelines). We'll re-check pipeline status each tick.
    s.waiting = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }

    # Track per-(pipeline_id, op_id) RAM multipliers for OOM retries.
    # Value is an integer multiplier applied to the base RAM slice.
    s.op_ram_mult = {}

    # Remember last seen OOM failures to update multipliers idempotently.
    s._seen_oom_events = set()

    # Soft caps per priority to prevent one task from eating the entire pool.
    # These are fractions of currently available pool resources (not max).
    s.prio_cpu_frac = {
        Priority.QUERY: 0.75,
        Priority.INTERACTIVE: 0.60,
        Priority.BATCH_PIPELINE: 0.50,
    }
    s.prio_ram_frac = {
        Priority.QUERY: 0.75,
        Priority.INTERACTIVE: 0.60,
        Priority.BATCH_PIPELINE: 0.50,
    }

    # Minimum slices so tiny pools can still run something.
    s.min_cpu = 1.0
    s.min_ram = 1.0

    # Preemption controls
    s.max_preempt_per_tick = 2  # limit churn


def _prio_order():
    # Highest first (QUERY/INTERACTIVE) then BATCH
    return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]


def _is_oom_error(err):
    if err is None:
        return False
    try:
        msg = str(err).lower()
    except Exception:
        return False
    return ("oom" in msg) or ("out of memory" in msg) or ("memory" in msg and "alloc" in msg)


def _op_key(pipeline_id, op):
    # Best-effort stable key for operator identity within pipeline
    # Many simulators have op.op_id; otherwise fallback to repr.
    op_id = getattr(op, "op_id", None)
    if op_id is None:
        op_id = getattr(op, "operator_id", None)
    if op_id is None:
        op_id = repr(op)
    return (pipeline_id, op_id)


def _pipeline_is_dead_or_done(p):
    st = p.runtime_status()
    if st.is_pipeline_successful():
        return True
    # If any op is FAILED, we treat pipeline as dead (no retries) except we may retry OOM by rescheduling.
    # However, the starter policy drops failures entirely. We'll keep that behavior unless it's an OOM
    # we observed and are retrying via re-assigning FAILED ops.
    return False


def _pop_next_runnable_pipeline(s, prio):
    # Round-robin within a priority: pop from front, requeue later if not runnable.
    if not s.waiting[prio]:
        return None
    return s.waiting[prio].pop(0)


def _requeue_pipeline(s, p):
    s.waiting[p.priority].append(p)


def _pick_one_assignable_op(p):
    st = p.runtime_status()
    # Allow retrying FAILED ops as "assignable"; the simulator's ASSIGNABLE_STATES includes FAILED.
    op_list = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
    return op_list


def _desired_resources(s, pool, pipeline, op, avail_cpu, avail_ram):
    # Base slice: cap by priority fraction of available headroom; also enforce minima.
    prio = pipeline.priority
    cpu_cap = max(s.min_cpu, avail_cpu * s.prio_cpu_frac.get(prio, 0.5))
    ram_cap = max(s.min_ram, avail_ram * s.prio_ram_frac.get(prio, 0.5))

    # Apply OOM multiplier to RAM, but never exceed available.
    mult = s.op_ram_mult.get(_op_key(pipeline.pipeline_id, op), 1)
    ram = min(avail_ram, ram_cap * mult)

    # CPU: don't scale CPU with mult; keep conservative to reduce interference.
    cpu = min(avail_cpu, cpu_cap)

    # Ensure we don't request more than pool max (defensive).
    cpu = min(cpu, getattr(pool, "max_cpu_pool", cpu))
    ram = min(ram, getattr(pool, "max_ram_pool", ram))

    # If after min constraints we still can't allocate meaningful resources, return zeros.
    if cpu <= 0 or ram <= 0:
        return 0.0, 0.0
    return cpu, ram


def _collect_running_containers(results):
    # Build a list of best-effort running containers from results stream.
    # We don't have a direct executor API for live containers in the prompt, so
    # we approximate by remembering last seen successful-start events.
    # If the simulator doesn't emit such, preemption will be a no-op.
    running = []
    for r in results or []:
        # Heuristic: if result isn't failed and has container_id, treat as running/completed event.
        # We can't distinguish completion; still, suspending a completed container should be ignored by executor.
        cid = getattr(r, "container_id", None)
        if cid is None:
            continue
        running.append(r)
    return running


def _try_preempt_for(s, needed_cpu, needed_ram, target_pool_id, results):
    # Suspend a small number of lower-priority containers in the same pool to free headroom.
    suspensions = []
    if needed_cpu <= 0 and needed_ram <= 0:
        return suspensions

    # Prefer suspending batch first, then interactive; never preempt QUERY.
    candidates = []
    for r in _collect_running_containers(results):
        if getattr(r, "pool_id", None) != target_pool_id:
            continue
        pr = getattr(r, "priority", None)
        if pr == Priority.QUERY:
            continue
        # Sort key: lower priority first (BATCH highest preemptability)
        score = 0
        if pr == Priority.BATCH_PIPELINE:
            score = 0
        elif pr == Priority.INTERACTIVE:
            score = 1
        else:
            score = 2
        candidates.append((score, r))

    candidates.sort(key=lambda x: x[0])

    freed_cpu = 0.0
    freed_ram = 0.0
    for _, r in candidates:
        if len(suspensions) >= s.max_preempt_per_tick:
            break
        cid = getattr(r, "container_id", None)
        pool_id = getattr(r, "pool_id", None)
        if cid is None or pool_id is None:
            continue
        suspensions.append(Suspend(cid, pool_id))
        freed_cpu += float(getattr(r, "cpu", 0.0) or 0.0)
        freed_ram += float(getattr(r, "ram", 0.0) or 0.0)
        if freed_cpu >= needed_cpu and freed_ram >= needed_ram:
            break

    return suspensions


@register_scheduler(key="scheduler_none_005")
def scheduler_none_005(s, results, pipelines):
    """
    Scheduler step.

    Main loop:
      - Ingest new pipelines into per-priority queues.
      - Update OOM RAM multipliers based on failures.
      - For each pool, try to schedule one runnable operator at a time, prioritizing QUERY > INTERACTIVE > BATCH.
      - If can't fit any high-priority op due to headroom, attempt limited preemption of lower priority.
    """
    # Enqueue new arrivals
    for p in pipelines or []:
        s.waiting[p.priority].append(p)

    # Early exit if nothing changed
    if not (pipelines or results):
        return [], []

    # Update OOM multipliers based on execution results
    for r in results or []:
        if not hasattr(r, "failed") or not r.failed():
            continue
        if not _is_oom_error(getattr(r, "error", None)):
            continue

        # Create a stable-ish key for the op in this result; fall back to repr of ops list.
        ops = getattr(r, "ops", None)
        if ops:
            op = ops[0]
            key = _op_key(getattr(r, "pipeline_id", None), op)  # pipeline_id may not exist on result
        else:
            key = ("unknown_pipeline", repr(ops))

        # Ensure idempotency across repeated result processing
        event_id = (getattr(r, "container_id", None), key)
        if event_id in s._seen_oom_events:
            continue
        s._seen_oom_events.add(event_id)

        # Increase RAM multiplier (capped)
        cur = s.op_ram_mult.get(key, 1)
        s.op_ram_mult[key] = min(cur * 2, 8)

    suspensions = []
    assignments = []

    # We'll do a conservative scheduling pass: for each pool, schedule up to one op per priority level
    # (so at most 3 assignments per pool per tick), and at most one op per pipeline.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)

        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # Try priorities in order
        for prio in _prio_order():
            if avail_cpu <= 0 or avail_ram <= 0:
                break

            # Scan a bounded number of pipelines in this priority to find a runnable one.
            # This avoids O(N) worst-case per tick when many are blocked on deps.
            scan_budget = min(len(s.waiting[prio]), 32)
            picked = None
            picked_ops = None

            for _ in range(scan_budget):
                p = _pop_next_runnable_pipeline(s, prio)
                if p is None:
                    break

                st = p.runtime_status()
                # Drop completed pipelines
                if st.is_pipeline_successful():
                    continue

                ops = _pick_one_assignable_op(p)
                if not ops:
                    # Not runnable now; requeue
                    _requeue_pipeline(s, p)
                    continue

                picked = p
                picked_ops = ops
                break

            if picked is None:
                continue

            op0 = picked_ops[0]
            cpu_req, ram_req = _desired_resources(s, pool, picked, op0, avail_cpu, avail_ram)

            if cpu_req <= avail_cpu and ram_req <= avail_ram and cpu_req > 0 and ram_req > 0:
                assignments.append(
                    Assignment(
                        ops=picked_ops,
                        cpu=cpu_req,
                        ram=ram_req,
                        priority=picked.priority,
                        pool_id=pool_id,
                        pipeline_id=picked.pipeline_id,
                    )
                )
                avail_cpu -= cpu_req
                avail_ram -= ram_req
                # Requeue pipeline to allow subsequent ops later
                _requeue_pipeline(s, picked)
                continue

            # If this is high priority, attempt limited preemption of lower priority work in this pool.
            if prio in (Priority.QUERY, Priority.INTERACTIVE):
                need_cpu = max(0.0, cpu_req - avail_cpu)
                need_ram = max(0.0, ram_req - avail_ram)
                new_susp = _try_preempt_for(s, need_cpu, need_ram, pool_id, results)
                if new_susp:
                    suspensions.extend(new_susp)
                    # Optimistically assume resources free next tick; don't over-assign now.
                    # Requeue and move on.
                    _requeue_pipeline(s, picked)
                    continue

            # Couldn't schedule; requeue
            _requeue_pipeline(s, picked)

    return suspensions, assignments
