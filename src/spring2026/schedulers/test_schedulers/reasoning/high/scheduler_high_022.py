# policy_key: scheduler_high_022
# reasoning_effort: high
# exp: reasoning
# model: gpt-5.2-2025-12-11
# llm_cost: 0.219097
# generation_seconds: 201.02
# generated_at: 2026-04-10T01:23:18.199213
@register_scheduler_init(key="scheduler_high_022")
def scheduler_high_022_init(s):
    """Priority-first, OOM-avoidant scheduler with soft reservations and simple aging.

    Goals:
    - Minimize weighted latency by always servicing QUERY > INTERACTIVE > BATCH.
    - Reduce 720s penalties by retrying failed (likely OOM) ops with increased RAM and by avoiding
      scheduling ops with known RAM needs unless they fit.
    - Prevent batch from consuming all resources when high-priority work is queued via soft
      per-pool headroom reservations; add aging to avoid indefinite batch starvation.
    """
    s.step_count = 0

    # Per-priority FIFO queues of pipelines (round-robin by re-appending after one placement).
    s.queues = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }

    # Arrival/aging bookkeeping (keyed by pipeline_id).
    s.pipeline_enqueued_step = {}

    # Per-operator RAM estimate keyed by operator object identity (id(op)).
    # This is increased on failure (OOM-like) and retained on success to avoid repeat OOMs.
    s.op_ram_est = {}

    # Per-operator retry count (id(op) -> int).
    s.op_retries = {}

    # Max retries per operator before we stop escalating further (we still may schedule with max RAM).
    s.max_op_retries = 8

    # Batch aging: after enough scheduler invocations, batch may break reservations to ensure progress.
    s.batch_aging_steps = 40


def _is_oom_error(err) -> bool:
    if err is None:
        return False
    try:
        msg = str(err).lower()
    except Exception:
        return False
    return ("oom" in msg) or ("out of memory" in msg) or ("memory" in msg and "alloc" in msg)


def _op_key(op) -> int:
    # Stable within the simulation run for the same operator object.
    return id(op)


def _get_op_min_ram_hint(op):
    # Best-effort introspection; simulator/operator may or may not expose these.
    for attr in (
        "min_ram",
        "ram_min",
        "minimum_ram",
        "min_memory",
        "memory_min",
        "peak_ram",
        "req_ram",
        "required_ram",
        "ram",
        "memory",
    ):
        try:
            v = getattr(op, attr, None)
        except Exception:
            v = None
        if isinstance(v, (int, float)) and v > 0:
            return float(v)
    return None


def _queue_has_ready_ops(queue, limit=6) -> bool:
    # Peek a few pipelines to decide whether to reserve headroom.
    n = min(len(queue), limit)
    for i in range(n):
        p = queue[i]
        st = p.runtime_status()
        if st.is_pipeline_successful():
            continue
        ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        if ops:
            return True
    return False


def _priority_targets(priority, pool, num_waiting_same_prio: int):
    # Base fractions tuned to reduce tail latency for high-priority work while keeping some parallelism.
    if priority == Priority.QUERY:
        cpu_frac, ram_frac = 0.60, 0.45
        cpu_cap = 16.0
    elif priority == Priority.INTERACTIVE:
        cpu_frac, ram_frac = 0.45, 0.35
        cpu_cap = 12.0
    else:  # BATCH_PIPELINE
        cpu_frac, ram_frac = 0.25, 0.25
        cpu_cap = 8.0

    # If many pipelines of the same priority are waiting, reduce per-op sizing a bit to increase concurrency.
    if num_waiting_same_prio >= 2:
        cpu_frac *= 0.80
        ram_frac *= 0.85
    if num_waiting_same_prio >= 4:
        cpu_frac *= 0.85
        ram_frac *= 0.90

    cpu_target = cpu_frac * float(pool.max_cpu_pool)
    cpu_target = max(1.0, min(cpu_target, cpu_cap, float(pool.max_cpu_pool)))

    ram_target = ram_frac * float(pool.max_ram_pool)
    ram_target = max(1e-9, min(ram_target, float(pool.max_ram_pool)))

    return cpu_target, ram_target


def _size_op(s, op, priority, pool, avail_cpu, avail_ram, num_waiting_same_prio: int):
    # Start from priority-based targets.
    cpu_target, ram_target = _priority_targets(priority, pool, num_waiting_same_prio)

    # Incorporate hints/estimates to reduce OOM risk.
    k = _op_key(op)
    est_ram = s.op_ram_est.get(k)
    min_hint = _get_op_min_ram_hint(op)

    if min_hint is not None:
        ram_target = max(ram_target, float(min_hint))
    if est_ram is not None:
        ram_target = max(ram_target, float(est_ram))

    # Clamp to availability: we never intentionally schedule below known estimate/hint.
    # If it doesn't fit, caller should skip for now (avoid repeated OOM and wasted work).
    if ram_target > float(avail_ram) or ram_target > float(pool.max_ram_pool):
        return None, None

    # CPU is flexible; we can run with less than target, but keep at least 1 if possible.
    if float(avail_cpu) < 1.0:
        return None, None
    cpu_alloc = min(float(avail_cpu), float(cpu_target))
    if cpu_alloc < 1.0:
        cpu_alloc = 1.0
        if cpu_alloc > float(avail_cpu):
            return None, None

    ram_alloc = float(ram_target)
    if ram_alloc <= 0.0 or ram_alloc > float(avail_ram):
        return None, None

    return cpu_alloc, ram_alloc


def _update_ram_estimates_from_results(s, results):
    # Update RAM estimates and retry counters from completed/failed executions.
    for r in results:
        try:
            ops = list(r.ops) if r.ops is not None else []
        except Exception:
            ops = []
        pool = s.executor.pools[r.pool_id] if hasattr(r, "pool_id") else None

        for op in ops:
            k = _op_key(op)
            prev = s.op_ram_est.get(k)

            if r.failed():
                # Escalate RAM aggressively on OOM-like failures; moderately otherwise.
                retries = s.op_retries.get(k, 0) + 1
                s.op_retries[k] = retries

                # Base on what we tried + any hints.
                tried = float(getattr(r, "ram", 0.0) or 0.0)
                min_hint = _get_op_min_ram_hint(op)
                base = tried if tried > 0 else (prev if prev is not None else 0.0)
                if min_hint is not None:
                    base = max(base, float(min_hint))

                if _is_oom_error(getattr(r, "error", None)):
                    new_est = max(base * 2.0, tried * 2.0, (prev * 1.5 if prev is not None else 0.0))
                else:
                    new_est = max(base * 1.25, tried * 1.25, (prev if prev is not None else 0.0))

                if pool is not None:
                    new_est = min(new_est, float(pool.max_ram_pool))
                # Keep estimates sensible.
                if new_est > 0:
                    s.op_ram_est[k] = new_est
            else:
                # Success: keep the maximum we have seen to avoid later OOMs on the same op.
                tried = float(getattr(r, "ram", 0.0) or 0.0)
                if tried > 0:
                    s.op_ram_est[k] = max(prev or 0.0, tried)
                # Reset retries on success.
                s.op_retries[k] = 0


def _clean_completed_from_queue(queue):
    # Drop completed pipelines from the queue.
    out = []
    for p in queue:
        st = p.runtime_status()
        if st.is_pipeline_successful():
            continue
        out.append(p)
    return out


def _reserve_for_high_priority(s, pool):
    # Soft reservation per pool when ready high-priority ops exist.
    query_ready = _queue_has_ready_ops(s.queues[Priority.QUERY])
    interactive_ready = _queue_has_ready_ops(s.queues[Priority.INTERACTIVE])

    if query_ready:
        # Reserve enough for (approximately) one query op placement.
        reserve_cpu = max(1.0, 0.60 * float(pool.max_cpu_pool))
        reserve_ram = 0.45 * float(pool.max_ram_pool)
        return reserve_cpu, reserve_ram
    if interactive_ready:
        reserve_cpu = max(1.0, 0.45 * float(pool.max_cpu_pool))
        reserve_ram = 0.35 * float(pool.max_ram_pool)
        return reserve_cpu, reserve_ram
    return 0.0, 0.0


def _try_schedule_from_priority(
    s,
    prio,
    pool_id,
    avail_cpu,
    avail_ram,
    reserve_cpu,
    reserve_ram,
):
    """Attempt to schedule a single ready operator from the given priority queue onto this pool.

    Returns: (assignment or None, updated_avail_cpu, updated_avail_ram)
    """
    q = s.queues[prio]
    if not q:
        return None, avail_cpu, avail_ram

    # Iterate at most once over the queue to avoid infinite loops.
    qlen = len(q)
    num_waiting_same_prio = qlen

    for _ in range(qlen):
        p = q.pop(0)
        st = p.runtime_status()
        if st.is_pipeline_successful():
            continue

        # Get one ready op (parents complete).
        op_list = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
        if not op_list:
            # Pipeline is currently blocked (parents running/not complete); rotate it.
            q.append(p)
            continue

        op = op_list[0]
        pool = s.executor.pools[pool_id]

        # Batch aging: allow breaking reservations if the pipeline has waited long enough.
        allow_break_reservation = False
        if prio == Priority.BATCH_PIPELINE:
            enq_step = s.pipeline_enqueued_step.get(p.pipeline_id, s.step_count)
            if (s.step_count - enq_step) >= s.batch_aging_steps:
                allow_break_reservation = True

        cpu_alloc, ram_alloc = _size_op(
            s,
            op,
            prio,
            pool,
            avail_cpu,
            avail_ram,
            num_waiting_same_prio=num_waiting_same_prio,
        )

        # If it doesn't fit, rotate and try others (maybe smaller ops/pipelines exist).
        if cpu_alloc is None or ram_alloc is None:
            q.append(p)
            continue

        # Enforce soft reservations only for batch (to protect query/interactive tail latency).
        if prio == Priority.BATCH_PIPELINE and not allow_break_reservation:
            if (float(avail_cpu) - float(cpu_alloc)) < float(reserve_cpu) or (float(avail_ram) - float(ram_alloc)) < float(reserve_ram):
                q.append(p)
                continue

        # Retry escalation guard: if many failures, schedule only if we can give it near-max RAM.
        k = _op_key(op)
        retries = s.op_retries.get(k, 0)
        if retries >= s.max_op_retries:
            # If we have lots of retries, avoid wasting time unless we can allocate the pool max RAM.
            if float(ram_alloc) < 0.90 * float(pool.max_ram_pool):
                # Rotate; maybe other work can proceed.
                q.append(p)
                continue

        assignment = Assignment(
            ops=[op],
            cpu=float(cpu_alloc),
            ram=float(ram_alloc),
            priority=p.priority,
            pool_id=pool_id,
            pipeline_id=p.pipeline_id,
        )

        # Round-robin fairness within the same priority: re-append pipeline after scheduling one op.
        q.append(p)

        return assignment, float(avail_cpu) - float(cpu_alloc), float(avail_ram) - float(ram_alloc)

    return None, avail_cpu, avail_ram


@register_scheduler(key="scheduler_high_022")
def scheduler_high_022(s, results: List["ExecutionResult"], pipelines: List["Pipeline"]) -> Tuple[List["Suspend"], List["Assignment"]]:
    suspensions = []
    assignments = []

    s.step_count += 1

    # 1) Enqueue new pipelines by priority (FIFO).
    for p in pipelines:
        s.queues[p.priority].append(p)
        # Record "arrival" for aging (only used for batch but harmless for others).
        s.pipeline_enqueued_step[p.pipeline_id] = s.step_count

    # 2) Update resource estimates from execution feedback (especially OOM retries).
    if results:
        _update_ram_estimates_from_results(s, results)

    # 3) Remove completed pipelines from queues to reduce scan overhead.
    s.queues[Priority.QUERY] = _clean_completed_from_queue(s.queues[Priority.QUERY])
    s.queues[Priority.INTERACTIVE] = _clean_completed_from_queue(s.queues[Priority.INTERACTIVE])
    s.queues[Priority.BATCH_PIPELINE] = _clean_completed_from_queue(s.queues[Priority.BATCH_PIPELINE])

    # 4) Schedule across pools. Priority order dominates the objective weights.
    priority_order = [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]

    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)

        # Compute per-pool soft reservation to keep headroom for ready high-priority work.
        reserve_cpu, reserve_ram = _reserve_for_high_priority(s, pool)

        # Keep placing while we have resources; stop when no progress.
        made_progress = True
        while made_progress:
            made_progress = False

            # Stop if we can't place anything meaningful.
            if avail_cpu < 1.0 or avail_ram <= 0.0:
                break

            for prio in priority_order:
                a, new_cpu, new_ram = _try_schedule_from_priority(
                    s,
                    prio,
                    pool_id,
                    avail_cpu,
                    avail_ram,
                    reserve_cpu,
                    reserve_ram,
                )
                if a is not None:
                    assignments.append(a)
                    avail_cpu, avail_ram = new_cpu, new_ram
                    made_progress = True
                    break  # re-evaluate from top priority with updated availability

    return suspensions, assignments
