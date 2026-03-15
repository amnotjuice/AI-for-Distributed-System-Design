# policy_key: scheduler_none_001
# reasoning_effort: none
# model: gpt-5.2-2025-12-11
# llm_cost: 0.046104
# generation_seconds: 43.73
# generated_at: 2026-03-12T21:21:48.014796
@register_scheduler_init(key="scheduler_none_001")
def scheduler_none_001_init(s):
    """Priority-aware, low-risk improvement over naive FIFO.

    Main ideas (kept intentionally simple and robust):
    1) Maintain separate FIFO queues per priority (QUERY > INTERACTIVE > BATCH).
    2) Reserve a small fraction of each pool for high-priority work (soft reservation).
    3) Conservative per-op sizing:
       - Start with a small CPU/RAM "probe" to reduce queueing delay and avoid hogging.
       - On OOM, increase RAM with exponential backoff (per pipeline), and requeue.
    4) Gentle preemption:
       - If high-priority work is waiting and pool has no headroom, suspend ONE lower-priority
         container in that pool to create space (minimize churn).
    """
    # FIFO queues per priority
    s.q_query = []
    s.q_interactive = []
    s.q_batch = []

    # Track per-pipeline resource "hints" that we adapt based on results
    # pipeline_id -> dict(cpu=..., ram=...)
    s.hints = {}

    # Track running containers per pool for potential preemption
    # pool_id -> list of dict(container_id=..., priority=..., cpu=..., ram=...)
    s.running_by_pool = {}

    # Configuration knobs (small, safe changes vs. naive)
    s.base_cpu_frac = 0.25  # probe CPU as fraction of pool max
    s.base_ram_frac = 0.25  # probe RAM as fraction of pool max

    # Soft reservations for headroom (fraction of pool) to protect latency
    s.reserve_cpu_query = 0.15
    s.reserve_ram_query = 0.15
    s.reserve_cpu_interactive = 0.10
    s.reserve_ram_interactive = 0.10

    # Backoff factors after OOM
    s.oom_ram_backoff = 2.0
    s.max_backoff_ram_frac = 0.90  # don't ask for more than this fraction of pool

    # Preemption control
    s.enable_preemption = True


def _prio_rank(priority):
    # Higher number = higher priority for comparisons/sorting
    if priority == Priority.QUERY:
        return 3
    if priority == Priority.INTERACTIVE:
        return 2
    return 1  # Priority.BATCH_PIPELINE and any others


def _enqueue_pipeline(s, p):
    if p.priority == Priority.QUERY:
        s.q_query.append(p)
    elif p.priority == Priority.INTERACTIVE:
        s.q_interactive.append(p)
    else:
        s.q_batch.append(p)


def _all_queues_empty(s):
    return (not s.q_query) and (not s.q_interactive) and (not s.q_batch)


def _iter_queues_in_priority_order(s):
    # Return queues in strict priority order
    return [s.q_query, s.q_interactive, s.q_batch]


def _pipeline_is_droppable(p):
    status = p.runtime_status()
    has_failures = status.state_counts[OperatorState.FAILED] > 0
    if status.is_pipeline_successful() or has_failures:
        return True
    return False


def _next_assignable_op(p):
    status = p.runtime_status()
    op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
    return op_list


def _get_hint(s, pipeline_id, pool):
    # Initialize with conservative probe sizes based on pool max.
    # Use minimum of pool max and current hint.
    h = s.hints.get(pipeline_id)
    if h is None:
        cpu = max(1.0, pool.max_cpu_pool * s.base_cpu_frac)
        ram = max(1.0, pool.max_ram_pool * s.base_ram_frac)
        h = {"cpu": cpu, "ram": ram}
        s.hints[pipeline_id] = h
    # Clip to pool max (and keep some headroom)
    h["cpu"] = max(1.0, min(h["cpu"], pool.max_cpu_pool))
    h["ram"] = max(1.0, min(h["ram"], pool.max_ram_pool))
    return h


def _pool_soft_available(s, pool, prio):
    # Soft-availability subtracts reserved headroom for higher priority classes.
    # For QUERY: no reservation needed.
    # For INTERACTIVE: leave room for QUERY.
    # For BATCH: leave room for QUERY + INTERACTIVE.
    avail_cpu = pool.avail_cpu_pool
    avail_ram = pool.avail_ram_pool

    if prio == Priority.QUERY:
        return avail_cpu, avail_ram

    reserve_cpu = 0.0
    reserve_ram = 0.0

    # Always reserve for QUERY when scheduling anything below QUERY.
    reserve_cpu += pool.max_cpu_pool * s.reserve_cpu_query
    reserve_ram += pool.max_ram_pool * s.reserve_ram_query

    if prio == Priority.BATCH_PIPELINE:
        # Additionally reserve for INTERACTIVE when scheduling batch.
        reserve_cpu += pool.max_cpu_pool * s.reserve_cpu_interactive
        reserve_ram += pool.max_ram_pool * s.reserve_ram_interactive

    return max(0.0, avail_cpu - reserve_cpu), max(0.0, avail_ram - reserve_ram)


def _record_running(s, res):
    if res.container_id is None:
        return
    pool_id = res.pool_id
    if pool_id not in s.running_by_pool:
        s.running_by_pool[pool_id] = []
    # Remove any stale entry for same container_id then add
    lst = s.running_by_pool[pool_id]
    lst = [x for x in lst if x.get("container_id") != res.container_id]
    lst.append(
        {
            "container_id": res.container_id,
            "priority": res.priority,
            "cpu": getattr(res, "cpu", None),
            "ram": getattr(res, "ram", None),
        }
    )
    s.running_by_pool[pool_id] = lst


def _remove_running(s, res):
    if res.container_id is None:
        return
    pool_id = res.pool_id
    if pool_id not in s.running_by_pool:
        return
    s.running_by_pool[pool_id] = [x for x in s.running_by_pool[pool_id] if x.get("container_id") != res.container_id]


def _maybe_update_hints_from_result(s, res):
    # If OOM: increase RAM hint for that pipeline.
    # We don't have explicit pipeline_id in result, so we infer via res.ops[0].pipeline_id if present.
    # If not present, we cannot update.
    pid = None
    try:
        if res.ops and hasattr(res.ops[0], "pipeline_id"):
            pid = res.ops[0].pipeline_id
    except Exception:
        pid = None

    if pid is None:
        return

    # Ensure hint exists
    # Pool max used for clipping on next scheduling; here just increase
    if pid not in s.hints:
        s.hints[pid] = {"cpu": res.cpu if res.cpu else 1.0, "ram": res.ram if res.ram else 1.0}

    if res.failed():
        # Heuristic: treat any error as potential memory issue if it looks like OOM.
        err = str(res.error).lower() if res.error is not None else ""
        is_oom = ("oom" in err) or ("out of memory" in err) or ("memory" in err and "alloc" in err)
        if is_oom:
            s.hints[pid]["ram"] = max(s.hints[pid].get("ram", 1.0) * s.oom_ram_backoff, (res.ram or 1.0) * s.oom_ram_backoff)


def _pick_preemption_victim(s, pool_id, min_prio_rank):
    # Pick a single lowest-priority container in pool with priority rank < min_prio_rank.
    # Conservative: only one victim to reduce churn.
    lst = s.running_by_pool.get(pool_id, [])
    candidates = [x for x in lst if _prio_rank(x.get("priority")) < min_prio_rank]
    if not candidates:
        return None
    # Prefer preempting the lowest-priority; tie-breaker: largest RAM+CPU to free space quickly.
    candidates.sort(key=lambda x: (_prio_rank(x.get("priority")), -(x.get("ram") or 0.0) - (x.get("cpu") or 0.0)))
    return candidates[0]


@register_scheduler(key="scheduler_none_001")
def scheduler_none_001(s, results, pipelines):
    """
    Scheduler loop:
    - Ingest new pipelines into priority queues.
    - Process results: update running set; on failure/OOM update hints; requeue pipeline if still alive.
    - For each pool:
        * If high-priority waiting and insufficient headroom, preempt one lower-priority container.
        * Assign at most one operator per pool per tick (simple, predictable).
    """
    # Ingest arrivals
    for p in pipelines:
        _enqueue_pipeline(s, p)

    # Early exit if nothing changed and no work waiting
    if not pipelines and not results and _all_queues_empty(s):
        return [], []

    suspensions = []
    assignments = []

    # Process results: update bookkeeping and requeue pipelines to continue
    # Note: pipelines themselves are provided only on arrival; we keep them in queues for future steps.
    for res in results:
        # Update running map: remove completed/failed containers, keep others
        if res.failed():
            _remove_running(s, res)
        else:
            # On success, container likely finished that op; remove from running.
            _remove_running(s, res)

        _maybe_update_hints_from_result(s, res)

    # Clean droppable pipelines from the front as we pop; keep FIFO otherwise.
    # Scheduling: one op per pool per tick, strict priority order.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]

        # Determine if any high-priority is waiting
        query_waiting = len(s.q_query) > 0
        interactive_waiting = len(s.q_interactive) > 0

        # If no resources at all, consider preemption only if high-priority is waiting
        if s.enable_preemption and (query_waiting or interactive_waiting):
            # If QUERY waiting, try to make room by preempting non-QUERY
            if query_waiting and (pool.avail_cpu_pool <= 0 or pool.avail_ram_pool <= 0):
                victim = _pick_preemption_victim(s, pool_id, min_prio_rank=_prio_rank(Priority.QUERY))
                if victim is not None:
                    suspensions.append(Suspend(victim["container_id"], pool_id))
                    # Optimistically remove victim from running set to avoid repeated suspends
                    s.running_by_pool[pool_id] = [x for x in s.running_by_pool.get(pool_id, []) if x.get("container_id") != victim["container_id"]]
            # If INTERACTIVE waiting, try to make room by preempting BATCH
            elif interactive_waiting and (pool.avail_cpu_pool <= 0 or pool.avail_ram_pool <= 0):
                victim = _pick_preemption_victim(s, pool_id, min_prio_rank=_prio_rank(Priority.INTERACTIVE))
                if victim is not None:
                    suspensions.append(Suspend(victim["container_id"], pool_id))
                    s.running_by_pool[pool_id] = [x for x in s.running_by_pool.get(pool_id, []) if x.get("container_id") != victim["container_id"]]

        # Assign one op per pool, in priority order
        assigned_this_pool = False
        for q in _iter_queues_in_priority_order(s):
            if assigned_this_pool:
                break
            if not q:
                continue

            # Peek-pop until we find a pipeline with an assignable op (or drop it)
            requeue = []
            selected_pipeline = None
            selected_op_list = None

            while q:
                p = q.pop(0)
                if _pipeline_is_droppable(p):
                    continue
                op_list = _next_assignable_op(p)
                if not op_list:
                    # Not ready; keep it for later
                    requeue.append(p)
                    continue
                selected_pipeline = p
                selected_op_list = op_list
                break

            # Put back the pipelines we popped but didn't choose
            # Keep FIFO by appending to tail
            q.extend(requeue)

            if selected_pipeline is None:
                continue

            # Compute soft available and hints
            soft_cpu, soft_ram = _pool_soft_available(s, pool, selected_pipeline.priority)
            if soft_cpu <= 0 or soft_ram <= 0:
                # Can't schedule this priority here now; put it back and try next pool
                q.append(selected_pipeline)
                continue

            hint = _get_hint(s, selected_pipeline.pipeline_id, pool)

            # Choose allocation: don't exceed soft availability; also avoid taking the entire pool
            cpu = min(hint["cpu"], soft_cpu)
            ram = min(hint["ram"], soft_ram)

            # Ensure at least 1 unit
            cpu = max(1.0, cpu)
            ram = max(1.0, ram)

            # Cap RAM backoff fraction
            ram = min(ram, pool.max_ram_pool * s.max_backoff_ram_frac)

            # If still exceeds hard availability, give up for now (requeue)
            if cpu > pool.avail_cpu_pool or ram > pool.avail_ram_pool:
                q.append(selected_pipeline)
                continue

            assignment = Assignment(
                ops=selected_op_list,
                cpu=cpu,
                ram=ram,
                priority=selected_pipeline.priority,
                pool_id=pool_id,
                pipeline_id=selected_pipeline.pipeline_id,
            )
            assignments.append(assignment)
            assigned_this_pool = True

            # Requeue pipeline for next operators
            q.append(selected_pipeline)

    return suspensions, assignments
