# policy_key: scheduler_est_039
# reasoning_effort: low
# model: gpt-5.2-2025-12-11
# llm_cost: 0.036574
# generation_seconds: 40.69
# generated_at: 2026-04-01T00:10:46.526107
@register_scheduler_init(key="scheduler_est_039")
def scheduler_est_039_init(s):
    """
    Priority-aware, OOM-avoiding FIFO with small, incremental improvements over naive FIFO.

    Key ideas:
      - Maintain separate queues per priority to reduce head-of-line blocking for interactive work.
      - Round-robin within each priority so a single pipeline doesn't monopolize scheduling.
      - Use op.estimate.mem_peak_gb as a RAM floor; on failures, increase RAM for retries.
      - Avoid allocating an entire pool to one operator by capping per-op CPU (improves latency under contention).
    """
    # Per-priority pipeline queues (round-robin within each).
    s.waiting_queues = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }

    # Track pipelines we've enqueued to avoid duplicates (pipelines can be re-added via results).
    s._enqueued_pipeline_ids = set()

    # Per-(pipeline, op) attempt tracking for RAM bump on failures.
    # key -> {"attempts": int, "last_ram": float}
    s._op_attempts = {}

    # Tunables (kept simple & conservative to start).
    s._max_assignments_per_pool_per_tick = 4

    # Per-op CPU caps by priority to avoid one op grabbing the full pool.
    s._cpu_cap_by_priority = {
        Priority.QUERY: 8.0,
        Priority.INTERACTIVE: 6.0,
        Priority.BATCH_PIPELINE: 4.0,
    }

    # Minimum RAM floor if estimate is missing (GB).
    s._default_mem_floor_gb = 0.5

    # Retry RAM multiplier after a failure.
    s._retry_ram_multiplier = 1.5


@register_scheduler(key="scheduler_est_039")
def scheduler_est_039_scheduler(s, results, pipelines):
    """
    Scheduler step:
      1) Ingest new pipelines into priority queues.
      2) Learn from failures to increase RAM next time (simple backoff).
      3) For each pool, place ready operators by priority order (QUERY > INTERACTIVE > BATCH),
         round-robin within each priority, with per-op CPU caps and RAM floors.
    """
    def _priority_order():
        # Protect tail latency by prioritizing interactive/query work first.
        return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]

    def _op_key(pipeline, op):
        # Best-effort stable identifier across scheduling ticks.
        # Many simulators provide op_id/operator_id; otherwise fallback to object id.
        oid = getattr(op, "op_id", None)
        if oid is None:
            oid = getattr(op, "operator_id", None)
        if oid is None:
            oid = id(op)
        return (pipeline.pipeline_id, oid)

    def _is_pipeline_done_or_hard_failed(pipeline):
        status = pipeline.runtime_status()
        if status.is_pipeline_successful():
            return True
        # If any operator failed, we still allow retries (ASSIGNABLE_STATES includes FAILED).
        # We only drop the pipeline if the simulator considers it successful; otherwise keep it.
        return False

    def _enqueue_pipeline(pipeline):
        if pipeline.pipeline_id in s._enqueued_pipeline_ids:
            return
        s._enqueued_pipeline_ids.add(pipeline.pipeline_id)
        q = s.waiting_queues.get(pipeline.priority, None)
        if q is None:
            # Unknown priority -> treat as lowest.
            s.waiting_queues[Priority.BATCH_PIPELINE].append(pipeline)
        else:
            q.append(pipeline)

    def _dequeue_next_runnable_pipeline(priority):
        """
        Round-robin: pop from left until we find one that has a runnable op;
        if none found, restore order and return None.
        """
        q = s.waiting_queues[priority]
        if not q:
            return None, None, None

        # We'll iterate at most len(q) items to avoid infinite loops.
        n = len(q)
        for _ in range(n):
            pipeline = q.pop(0)

            # If complete, drop it and clear from enqueued set.
            if _is_pipeline_done_or_hard_failed(pipeline):
                s._enqueued_pipeline_ids.discard(pipeline.pipeline_id)
                continue

            status = pipeline.runtime_status()
            # Only schedule ops whose parents are complete.
            op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
            if not op_list:
                # Not runnable yet; put it back and try another pipeline.
                q.append(pipeline)
                continue

            op = op_list[0]
            # Found a runnable op; caller will either schedule or requeue pipeline.
            return pipeline, status, op

        return None, None, None

    def _ram_floor_for_op(pipeline, op):
        est = None
        try:
            est = getattr(getattr(op, "estimate", None), "mem_peak_gb", None)
        except Exception:
            est = None

        base = s._default_mem_floor_gb
        if isinstance(est, (int, float)) and est is not None and est > 0:
            base = max(base, float(est))

        k = _op_key(pipeline, op)
        prev = s._op_attempts.get(k)
        if prev and prev.get("attempts", 0) > 0:
            # Increase floor based on last attempted RAM.
            last_ram = prev.get("last_ram", 0.0) or 0.0
            if last_ram > 0:
                base = max(base, float(last_ram) * s._retry_ram_multiplier)

        return base

    def _cpu_for_op(priority, pool, avail_cpu):
        # Cap CPU per op to reduce queueing delay for other small/interactive tasks.
        cap = float(s._cpu_cap_by_priority.get(priority, 4.0))

        # Also avoid taking more than half of the pool in one assignment when possible.
        half_pool = float(getattr(pool, "max_cpu_pool", cap)) / 2.0
        cap = min(cap, max(1.0, half_pool))

        # Always allocate at least 1 CPU if possible.
        return max(1.0, min(float(avail_cpu), cap))

    def _record_result_learning(res):
        # Learn from failures: bump RAM next time for the same operator.
        # We treat any failure as potentially memory-related; the estimate is just a floor anyway.
        if not res.failed():
            return

        for op in getattr(res, "ops", []) or []:
            # res doesn't carry pipeline_id directly in the provided interface, but Assignment did.
            # Some simulators attach pipeline_id to op; best-effort.
            pid = getattr(op, "pipeline_id", None)
            if pid is None:
                # If missing, we can't reliably attribute; skip.
                continue

            k = (pid, getattr(op, "op_id", getattr(op, "operator_id", id(op))))
            prev = s._op_attempts.get(k, {"attempts": 0, "last_ram": 0.0})
            prev["attempts"] = int(prev.get("attempts", 0)) + 1
            prev["last_ram"] = float(getattr(res, "ram", prev.get("last_ram", 0.0)) or 0.0)
            s._op_attempts[k] = prev

    # 1) Ingest new pipelines
    for p in pipelines:
        _enqueue_pipeline(p)

    # 2) Learn from results (RAM backoff on failures)
    for r in results:
        _record_result_learning(r)

    # If nothing to do, exit quickly
    if not pipelines and not results:
        # There still might be queued work, but without resource changes it won't schedule differently.
        # (Kept consistent with the starter template behavior.)
        return [], []

    suspensions = []
    assignments = []

    # 3) Schedule per pool
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)

        if avail_cpu < 1.0 or avail_ram <= 0.0:
            continue

        made = 0
        while made < s._max_assignments_per_pool_per_tick and avail_cpu >= 1.0 and avail_ram > 0.0:
            scheduled_any = False

            for prio in _priority_order():
                pipeline, status, op = _dequeue_next_runnable_pipeline(prio)
                if pipeline is None:
                    continue

                # Compute RAM floor and allocate (RAM above floor is "free" for performance).
                ram_floor = _ram_floor_for_op(pipeline, op)

                # If we can't meet the floor in this pool right now, put pipeline back and try others.
                if avail_ram < ram_floor:
                    s.waiting_queues[prio].append(pipeline)
                    continue

                # Allocate modestly above the floor when possible (helps absorb underestimates).
                # Don't overthink: extra RAM doesn't speed up, but reduces OOM risk when estimates are low.
                ram_alloc = min(avail_ram, max(ram_floor, ram_floor * 1.2))

                cpu_alloc = _cpu_for_op(prio, pool, avail_cpu)

                assignment = Assignment(
                    ops=[op],
                    cpu=cpu_alloc,
                    ram=ram_alloc,
                    priority=pipeline.priority,
                    pool_id=pool_id,
                    pipeline_id=pipeline.pipeline_id,
                )
                assignments.append(assignment)

                # Update attempt tracking with what we tried (useful even if it fails).
                k = _op_key(pipeline, op)
                prev = s._op_attempts.get(k, {"attempts": 0, "last_ram": 0.0})
                prev["last_ram"] = float(ram_alloc)
                s._op_attempts[k] = prev

                # Round-robin: put pipeline back for its next runnable op later.
                s.waiting_queues[prio].append(pipeline)

                avail_cpu -= cpu_alloc
                avail_ram -= ram_alloc
                made += 1
                scheduled_any = True
                break  # re-check from highest priority again

            if not scheduled_any:
                break

    return suspensions, assignments
