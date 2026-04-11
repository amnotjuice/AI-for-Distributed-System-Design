# policy_key: scheduler_medium_034
# reasoning_effort: medium
# exp: reasoning
# model: gpt-5.2-2025-12-11
# llm_cost: 0.087721
# generation_seconds: 71.98
# generated_at: 2026-04-09T23:15:24.909721
@register_scheduler_init(key="scheduler_medium_034")
def scheduler_medium_034_init(s):
    """
    Priority-aware, failure-averse scheduler.

    Core ideas:
    - Strictly prefer QUERY, then INTERACTIVE, then BATCH for admissions to minimize weighted latency.
    - Use conservative (but not maximal) default RAM to reduce OOM-driven failures (which incur huge penalty).
    - On OOM failures, apply exponential RAM backoff per-operator (learned hint) and retry.
    - Avoid head-of-line blocking: if a pipeline's next runnable op doesn't fit in a pool, rotate it to the back.
    - Basic fairness: ensure BATCH can make progress when headroom exists (without preempting queries).
    """
    # Per-priority FIFO queues
    s.queues = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }

    # Per-operator resource hints (keyed by (pipeline_id, op_id))
    s.op_ram_hint = {}  # bytes/units as used by simulator
    s.op_cpu_hint = {}

    # Failure tracking (keyed by (pipeline_id, op_id))
    s.op_fail_oom = {}
    s.op_fail_other = {}

    # Pipelines we stop scheduling (to avoid infinite thrash when repeatedly failing)
    s.abandoned_pipelines = set()

    # Soft round-robin token to occasionally allow batch when possible
    s._tick = 0


@register_scheduler(key="scheduler_medium_034")
def scheduler_medium_034_scheduler(s, results, pipelines):
    """
    Scheduler step:
    - Ingest new pipelines into per-priority queues.
    - Update per-op hints from execution results (OOM => increase RAM hint).
    - Create assignments in two phases:
        1) Schedule QUERY + INTERACTIVE onto pools with most headroom (protect tail latency).
        2) Schedule BATCH onto remaining capacity with a packing bias.
    """
    s._tick += 1

    def _prio_rank(p):
        if p == Priority.QUERY:
            return 0
        if p == Priority.INTERACTIVE:
            return 1
        return 2

    def _pool_headroom_score(pool):
        # Prefer pools with more combined headroom for latency-sensitive work.
        return (pool.avail_ram_pool / max(pool.max_ram_pool, 1e-9)) + (pool.avail_cpu_pool / max(pool.max_cpu_pool, 1e-9))

    def _is_oom_error(err):
        if err is None:
            return False
        msg = str(err).lower()
        return ("oom" in msg) or ("out of memory" in msg) or ("memory" in msg and "alloc" in msg)

    def _base_ram_for_priority(pool, prio):
        # Conservative defaults to reduce OOMs (failures are extremely expensive in the objective).
        if prio == Priority.QUERY:
            frac = 0.30
        elif prio == Priority.INTERACTIVE:
            frac = 0.22
        else:
            frac = 0.14
        # Minimum floor to avoid tiny allocations that are likely to OOM.
        floor = 0.06 * pool.max_ram_pool
        return max(floor, frac * pool.max_ram_pool)

    def _base_cpu_for_priority(pool, prio):
        # Give latency-sensitive work more CPU, but cap to avoid over-allocating a single op.
        if prio == Priority.QUERY:
            cap_frac = 0.60
        elif prio == Priority.INTERACTIVE:
            cap_frac = 0.45
        else:
            cap_frac = 0.35
        cap = max(1.0, cap_frac * pool.max_cpu_pool)
        # Also ensure some minimal CPU so we don't create pathological slowdowns.
        return min(cap, max(1.0, 0.20 * pool.max_cpu_pool))

    def _op_key(pipeline_id, op):
        # Use id(op) as a stable identifier within the simulation for that operator object.
        return (pipeline_id, id(op))

    def _should_abandon_op(pipeline_id, op):
        k = _op_key(pipeline_id, op)
        oom = s.op_fail_oom.get(k, 0)
        other = s.op_fail_other.get(k, 0)
        # Be more patient with OOM (we can often fix with RAM backoff), less with other errors.
        return (other >= 2) or (oom >= 6)

    def _pipeline_done_or_abandoned(p):
        if p.pipeline_id in s.abandoned_pipelines:
            return True
        st = p.runtime_status()
        return st.is_pipeline_successful()

    def _enqueue_pipeline(p):
        # Avoid enqueueing pipelines we already decided to abandon.
        if p.pipeline_id in s.abandoned_pipelines:
            return
        s.queues[p.priority].append(p)

    # Ingest new pipelines
    for p in pipelines:
        _enqueue_pipeline(p)

    # Early exit if nothing changed
    if not pipelines and not results:
        return [], []

    # Update hints based on results
    for r in results:
        # If no ops, nothing to learn.
        if not getattr(r, "ops", None):
            continue

        pool = s.executor.pools[r.pool_id]
        oom = r.failed() and _is_oom_error(r.error)

        # Try to infer pipeline_id for hinting: we can't rely on it being present on ExecutionResult,
        # so we use the op object's attributes if available, else leave hints unchanged.
        # (Scheduling will still work with conservative defaults.)
        for op in r.ops:
            pid = getattr(op, "pipeline_id", None)
            if pid is None:
                pid = getattr(op, "pipeline", None)
                pid = getattr(pid, "pipeline_id", None) if pid is not None else None

            # Without pipeline_id we cannot key hints safely across different pipelines; skip.
            if pid is None:
                continue

            k = _op_key(pid, op)

            if r.failed():
                if oom:
                    s.op_fail_oom[k] = s.op_fail_oom.get(k, 0) + 1
                    # Exponential RAM backoff, capped by pool max.
                    prev = s.op_ram_hint.get(k, None)
                    # If we know what we allocated, double it; otherwise start from a conservative base.
                    allocated = getattr(r, "ram", None)
                    if allocated is None or allocated <= 0:
                        allocated = prev if prev is not None else _base_ram_for_priority(pool, r.priority)
                    new_hint = min(pool.max_ram_pool, max(_base_ram_for_priority(pool, r.priority), 2.0 * allocated))
                    s.op_ram_hint[k] = new_hint
                else:
                    s.op_fail_other[k] = s.op_fail_other.get(k, 0) + 1
            else:
                # On success, keep RAM hint as-is (don't risk oscillations that cause OOM).
                # Optionally nudge CPU hint slightly downward over time to improve packing.
                allocated_cpu = getattr(r, "cpu", None)
                if allocated_cpu is not None and allocated_cpu > 0:
                    prev_cpu = s.op_cpu_hint.get(k, None)
                    # Gentle decay; keep at least 1.
                    if prev_cpu is None:
                        s.op_cpu_hint[k] = max(1.0, float(allocated_cpu))
                    else:
                        s.op_cpu_hint[k] = max(1.0, 0.90 * float(prev_cpu))

    suspensions = []  # no preemption in this version (keeps churn/waste low)
    assignments = []

    # Pool ordering:
    pool_ids_by_headroom_desc = sorted(
        range(s.executor.num_pools),
        key=lambda i: _pool_headroom_score(s.executor.pools[i]),
        reverse=True,
    )
    pool_ids_by_headroom_asc = list(reversed(pool_ids_by_headroom_desc))

    def _try_schedule_one_from_queue_into_pool(prio, pool_id, max_rotations):
        """
        Try to schedule one runnable op from the given priority queue into the specified pool.
        Rotate unschedulable pipelines to the back to avoid head-of-line blocking.
        """
        q = s.queues[prio]
        if not q:
            return False

        pool = s.executor.pools[pool_id]
        if pool.avail_cpu_pool <= 0 or pool.avail_ram_pool <= 0:
            return False

        rotations = 0
        while rotations < max_rotations and q:
            p = q.pop(0)

            # Drop completed/abandoned pipelines from the queue
            if _pipeline_done_or_abandoned(p):
                rotations += 1
                continue

            st = p.runtime_status()
            op_list = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
            if not op_list:
                # Not runnable yet; keep it in queue for later.
                q.append(p)
                rotations += 1
                continue

            op = op_list[0]
            if _should_abandon_op(p.pipeline_id, op):
                s.abandoned_pipelines.add(p.pipeline_id)
                rotations += 1
                continue

            # Decide resource request using hints + conservative defaults.
            k = _op_key(p.pipeline_id, op)
            base_ram = _base_ram_for_priority(pool, p.priority)
            hint_ram = s.op_ram_hint.get(k, base_ram)
            # Clamp hint into pool size.
            req_ram = min(pool.max_ram_pool, max(base_ram, hint_ram))

            base_cpu = _base_cpu_for_priority(pool, p.priority)
            hint_cpu = s.op_cpu_hint.get(k, base_cpu)
            # Cap CPU per op for better concurrency; allow query/interactive to go higher.
            if p.priority == Priority.QUERY:
                cpu_cap = max(1.0, 0.70 * pool.max_cpu_pool)
            elif p.priority == Priority.INTERACTIVE:
                cpu_cap = max(1.0, 0.55 * pool.max_cpu_pool)
            else:
                cpu_cap = max(1.0, 0.40 * pool.max_cpu_pool)
            req_cpu = min(cpu_cap, max(1.0, float(hint_cpu)))

            # Fit to available resources (CPU can be reduced, RAM should not go below base_ram).
            if pool.avail_ram_pool < base_ram:
                # Can't safely schedule here; rotate.
                q.append(p)
                rotations += 1
                continue

            req_ram = min(req_ram, pool.avail_ram_pool)
            req_cpu = min(req_cpu, pool.avail_cpu_pool)
            if req_cpu < 1.0:
                # Can't schedule without at least 1 vCPU; rotate.
                q.append(p)
                rotations += 1
                continue

            assignments.append(
                Assignment(
                    ops=op_list,
                    cpu=req_cpu,
                    ram=req_ram,
                    priority=p.priority,
                    pool_id=pool_id,
                    pipeline_id=p.pipeline_id,
                )
            )

            # Keep pipeline in queue for subsequent ops (it will have fewer runnable ops until parents complete).
            q.append(p)
            return True

        return False

    # Phase 1: prioritize QUERY and INTERACTIVE on high-headroom pools
    for pool_id in pool_ids_by_headroom_desc:
        # Try to schedule a small number of latency-sensitive ops per pool per tick.
        # This reduces per-tick overhead and prevents batch from consuming everything first.
        for _ in range(3):
            progressed = False
            progressed |= _try_schedule_one_from_queue_into_pool(Priority.QUERY, pool_id, max_rotations=len(s.queues[Priority.QUERY]) + 1)
            progressed |= _try_schedule_one_from_queue_into_pool(Priority.INTERACTIVE, pool_id, max_rotations=len(s.queues[Priority.INTERACTIVE]) + 1)
            if not progressed:
                break

    # Fairness nudge: if it's been a few ticks and batch is waiting, allow at least one batch op if possible.
    allow_one_batch = (s._tick % 3 == 0)

    # Phase 2: schedule batch using packing bias (low-headroom pools first)
    for pool_id in pool_ids_by_headroom_asc:
        if not s.queues[Priority.BATCH_PIPELINE]:
            break

        # Only schedule batch if we either have a fairness tick or there is no high-priority backlog.
        high_backlog = bool(s.queues[Priority.QUERY]) or bool(s.queues[Priority.INTERACTIVE])
        if high_backlog and not allow_one_batch:
            continue

        # Limit batch per pool per tick to avoid swamping future interactive arrivals.
        for _ in range(2 if allow_one_batch else 1):
            ok = _try_schedule_one_from_queue_into_pool(
                Priority.BATCH_PIPELINE,
                pool_id,
                max_rotations=len(s.queues[Priority.BATCH_PIPELINE]) + 1,
            )
            if not ok:
                break

    return suspensions, assignments
