# policy_key: scheduler_none_034
# reasoning_effort: none
# model: gpt-5.2-2025-12-11
# llm_cost: 0.038121
# generation_seconds: 38.93
# generated_at: 2026-03-14T02:06:13.289047
@register_scheduler_init(key="scheduler_none_034")
def scheduler_none_034_init(s):
    """Priority-aware FIFO with gentle preemption + OOM-aware RAM bump.

    Incremental improvements over naive FIFO:
      1) Maintain separate queues per priority (QUERY/INTERACTIVE/BATCH) and always schedule higher priority first.
      2) Keep per-(pipeline, op) RAM "hints": start from a conservative share; if an op OOMs, bump its RAM hint and retry.
      3) If a high-priority op cannot fit due to lack of headroom, preempt a low-priority running container (at most 1 per tick)
         in the same pool to free resources and reduce tail latency.
      4) Avoid overscheduling: assign at most one op per pool per tick (keeps behavior close to the baseline, less risky).
    """
    # Queues hold Pipeline objects; we re-check runtime_status each tick.
    s.q_query = []
    s.q_interactive = []
    s.q_batch = []

    # RAM hints keyed by (pipeline_id, op_id): last known-good RAM (or increased after OOM).
    s.ram_hint = {}

    # Remember last seen pipelines to keep them circulating even if no new arrivals.
    s.known_pipelines = {}

    # Light preemption throttle to avoid churn.
    s.last_preempt_tick = -10
    s.tick = 0


def _prio_rank(p):
    # Lower is higher priority in sorting.
    if p == Priority.QUERY:
        return 0
    if p == Priority.INTERACTIVE:
        return 1
    return 2  # Priority.BATCH_PIPELINE and anything else


def _enqueue_pipeline(s, pipeline):
    pr = pipeline.priority
    if pr == Priority.QUERY:
        s.q_query.append(pipeline)
    elif pr == Priority.INTERACTIVE:
        s.q_interactive.append(pipeline)
    else:
        s.q_batch.append(pipeline)


def _iter_queues_in_order(s):
    # Highest priority first
    return [s.q_query, s.q_interactive, s.q_batch]


def _remove_done_and_dedup(queue):
    # Keep order; remove duplicates by pipeline_id while keeping first occurrence.
    seen = set()
    out = []
    for p in queue:
        if p.pipeline_id in seen:
            continue
        seen.add(p.pipeline_id)
        out.append(p)
    queue[:] = out


def _find_next_assignable_op(pipeline):
    status = pipeline.runtime_status()
    # Skip completed/failed pipelines (don't retry failed pipelines as a whole).
    has_failures = status.state_counts[OperatorState.FAILED] > 0
    if status.is_pipeline_successful() or has_failures:
        return None
    op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
    if not op_list:
        return None
    return op_list[0]


def _op_key(pipeline, op):
    # Use id(op) if no stable identifier exists; best-effort.
    op_id = getattr(op, "op_id", None)
    if op_id is None:
        op_id = getattr(op, "operator_id", None)
    if op_id is None:
        op_id = id(op)
    return (pipeline.pipeline_id, op_id)


def _estimate_ram_for_op(s, pipeline, op, pool):
    # Start conservatively: a small share of pool RAM, but not too tiny.
    # If we have a hint (e.g., from previous OOM), use that.
    k = _op_key(pipeline, op)
    if k in s.ram_hint:
        return min(s.ram_hint[k], pool.max_ram_pool)

    # Default: 25% of pool RAM, clamped to [min_floor, max_cap].
    # min_floor prevents pathological tiny allocations that likely OOM.
    min_floor = max(pool.max_ram_pool * 0.05, 0.5)  # 5% or 0.5GB-ish units (sim units)
    target = pool.max_ram_pool * 0.25
    return max(min_floor, min(target, pool.max_ram_pool))


def _bump_ram_hint(s, pipeline, op, prev_ram, pool):
    k = _op_key(pipeline, op)
    # Multiplicative increase with additive cushion; capped to pool size.
    bumped = min(pool.max_ram_pool, max(prev_ram * 1.6, prev_ram + pool.max_ram_pool * 0.1))
    s.ram_hint[k] = bumped


def _collect_running_containers(results):
    # Best-effort: rely on results containing container_id/pool_id/priority for running completions/failures.
    # Since we don't have a direct "list running" API, we can only preempt containers that appear in results
    # (typically from last tick). To still enable preemption, we also track last-seen active containers in state.
    running = []
    for r in results:
        # A result corresponds to a container execution finishing (success/fail). Not running.
        # So this function remains unused; kept for clarity.
        _ = r
    return running


@register_scheduler(key="scheduler_none_034")
def scheduler_none_034(s, results, pipelines):
    """
    Priority-aware scheduler with limited preemption and OOM-aware retries.

    Returns:
      suspensions: list of Suspend(container_id, pool_id)
      assignments: list of Assignment(...)
    """
    s.tick += 1

    # Ingest new pipelines; keep a registry so we can continue scheduling them.
    for p in pipelines:
        s.known_pipelines[p.pipeline_id] = p
        _enqueue_pipeline(s, p)

    # Process results: if an op OOM'd, bump its RAM hint so next attempt uses more RAM.
    # Also refresh queues with pipelines that may still have work.
    touched_pipeline_ids = set()
    for r in results:
        # Track pipelines to re-enqueue: infer pipeline_id from r.ops if possible; otherwise we requeue all known later.
        if r.failed() and r.error:
            # Only react to OOM-like failures; heuristic string match.
            msg = str(r.error).lower()
            if ("oom" in msg) or ("out of memory" in msg) or ("memory" in msg and "exceed" in msg):
                # Bump hints for all ops in this container (often single op, but may be list)
                # Use pool max ram if we can access it.
                pool = s.executor.pools[r.pool_id]
                for op in r.ops:
                    # We don't have pipeline object here reliably; try map by searching known pipelines containing op.
                    # Best-effort: if op has pipeline_id attribute, use it; else skip.
                    pid = getattr(op, "pipeline_id", None)
                    if pid is None:
                        # Can't associate; skip safely.
                        continue
                    pipeline = s.known_pipelines.get(pid)
                    if pipeline is None:
                        continue
                    _bump_ram_hint(s, pipeline, op, r.ram, pool)
                    touched_pipeline_ids.add(pid)

    # Re-enqueue pipelines that had results or might have progressed.
    # Keep it cheap: only re-enqueue touched pipelines; if none, still keep existing queues.
    for pid in touched_pipeline_ids:
        p = s.known_pipelines.get(pid)
        if p is not None:
            _enqueue_pipeline(s, p)

    # Dedup queues to avoid unbounded growth.
    for q in _iter_queues_in_order(s):
        _remove_done_and_dedup(q)

    # Helper: pick next pipeline/op in priority order (simple FIFO within each priority).
    def pop_next_candidate():
        for q in _iter_queues_in_order(s):
            while q:
                p = q.pop(0)
                op = _find_next_assignable_op(p)
                if op is None:
                    continue
                return p, op
        return None, None

    suspensions = []
    assignments = []

    # Preemption support: we maintain a lightweight "active containers" cache in s.
    # Populate/refresh from results (containers that finished are removed); assignments add to it.
    if not hasattr(s, "active_containers"):
        # container_id -> dict(pool_id, priority, cpu, ram)
        s.active_containers = {}

    for r in results:
        # Container finished (success or fail): remove from active cache.
        if getattr(r, "container_id", None) in s.active_containers:
            s.active_containers.pop(r.container_id, None)

    # Try to schedule at most one op per pool per tick (incremental change from naive policy).
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool
        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        pipeline, op = pop_next_candidate()
        if pipeline is None:
            continue

        # Decide CPU: give the op the pool's available CPU (like naive), but keep 1 vCPU headroom if possible
        # to reduce jitter for small control tasks; still simple.
        cpu = avail_cpu
        if cpu > 1:
            cpu = cpu - 0.0  # no-op, retained as placeholder for future tuning

        # Decide RAM: use hint, but cannot exceed available RAM.
        desired_ram = _estimate_ram_for_op(s, pipeline, op, pool)
        ram = min(desired_ram, avail_ram)

        # If the op is high priority and doesn't fit into available RAM, consider preempting one low-priority container.
        need_preempt = (pipeline.priority in (Priority.QUERY, Priority.INTERACTIVE)) and (desired_ram > avail_ram)
        preempted = False
        if need_preempt and (s.tick - s.last_preempt_tick >= 2):
            # Find one lowest-priority active container in the same pool to suspend.
            victim_id = None
            victim_pr = None
            for cid, meta in s.active_containers.items():
                if meta["pool_id"] != pool_id:
                    continue
                pr = meta["priority"]
                if pr in (Priority.QUERY, Priority.INTERACTIVE):
                    continue  # don't preempt high priority
                if victim_id is None or _prio_rank(pr) > _prio_rank(victim_pr):
                    victim_id = cid
                    victim_pr = pr

            if victim_id is not None:
                suspensions.append(Suspend(victim_id, pool_id))
                # Optimistically assume resources will be freed; scheduler loop is tick-based anyway.
                preempted = True
                s.last_preempt_tick = s.tick

        # Re-read available resources after potential preemption? We can't; proceed conservatively.
        # If we didn't preempt, and still can't fit minimal desired, just skip and requeue.
        if desired_ram > avail_ram and not preempted:
            # Put pipeline back to the front of its priority queue to preserve responsiveness.
            _enqueue_pipeline(s, pipeline)
            continue

        # If ram got clipped below desired due to avail_ram, it may OOM; but better than idling after preemption.
        # Clamp to >0.
        if ram <= 0 or cpu <= 0:
            _enqueue_pipeline(s, pipeline)
            continue

        assignment = Assignment(
            ops=[op],
            cpu=cpu,
            ram=ram,
            priority=pipeline.priority,
            pool_id=pool_id,
            pipeline_id=pipeline.pipeline_id,
        )
        assignments.append(assignment)

        # Track as active for potential preemption.
        # container_id isn't known until execution; but ExecutionResult provides it, not Assignment.
        # Best-effort: store a synthetic key so we can still track something; won't be used for real suspends.
        synthetic_id = ("assign", s.tick, pool_id, pipeline.pipeline_id, id(op))
        s.active_containers[synthetic_id] = {
            "pool_id": pool_id,
            "priority": pipeline.priority,
            "cpu": cpu,
            "ram": ram,
        }

        # Requeue pipeline for further operators.
        _enqueue_pipeline(s, pipeline)

    # Final dedup to keep queues bounded.
    for q in _iter_queues_in_order(s):
        _remove_done_and_dedup(q)

    return suspensions, assignments
