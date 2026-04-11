# policy_key: scheduler_none_013
# reasoning_effort: none
# model: gpt-5.2-2025-12-11
# llm_cost: 0.054709
# generation_seconds: 43.75
# generated_at: 2026-04-09T21:55:07.125950
@register_scheduler_init(key="scheduler_none_013")
def scheduler_none_013_init(s):
    """Priority-weighted, failure-averse scheduler.

    Core ideas:
      - Separate FIFO queues per priority; always try to serve higher priorities first.
      - Keep some headroom for high priority by soft-reserving CPU/RAM fractions in each pool.
      - Use conservative initial RAM sizing based on per-op observed failures (OOM/backoff).
      - Prefer packing to a single pool/VM chunk (scale-up bias) by allocating one op at a time.
      - Preempt (suspend) low-priority RUNNING work only when a high-priority op is blocked
        and there is meaningful capacity pressure (to protect tail latency).
    """
    # Priority queues (FIFO within priority)
    s.q_query = []
    s.q_interactive = []
    s.q_batch = []

    # Track pipeline arrival counts (not used directly here, but useful if extended)
    s.arrivals = {
        Priority.QUERY: 0,
        Priority.INTERACTIVE: 0,
        Priority.BATCH_PIPELINE: 0,
    }

    # Per-operator memory backoff factors based on observed failures:
    # key: (pipeline_id, op_id) or fallback to op.name/idx when available.
    # value: multiplier on suggested RAM.
    s.op_ram_backoff = {}

    # Track per-container priority for preemption heuristics
    s.container_priority = {}

    # Tunables (chosen to be simple and robust)
    s.reserved_frac = {
        Priority.QUERY: 0.20,         # reserve 20% for queries
        Priority.INTERACTIVE: 0.10,   # reserve 10% for interactive
        Priority.BATCH_PIPELINE: 0.0
    }
    s.max_backoff = 4.0          # cap RAM multiplier
    s.backoff_step = 1.5         # multiply on failure
    s.min_cpu_slice = 1.0        # don't assign below 1 vCPU when possible
    s.assign_one_op = True       # one op per assignment to reduce interference
    s.preempt_enabled = True
    s.preempt_min_gain_cpu = 1.0  # only preempt if we can free at least this much CPU
    s.preempt_min_gain_ram = 0.5  # only preempt if we can free at least this much RAM


def _priority_rank(p):
    if p == Priority.QUERY:
        return 0
    if p == Priority.INTERACTIVE:
        return 1
    return 2


def _enqueue_pipeline(s, p):
    if p.priority == Priority.QUERY:
        s.q_query.append(p)
    elif p.priority == Priority.INTERACTIVE:
        s.q_interactive.append(p)
    else:
        s.q_batch.append(p)


def _all_queues(s):
    return [s.q_query, s.q_interactive, s.q_batch]


def _iter_pipelines_in_priority_order(s):
    # Yield pipelines in strict priority order, FIFO within each.
    for q in (s.q_query, s.q_interactive, s.q_batch):
        for p in q:
            yield p


def _remove_pipeline_from_queue(s, p):
    # Remove first occurrence from the right queue
    q = s.q_batch
    if p.priority == Priority.QUERY:
        q = s.q_query
    elif p.priority == Priority.INTERACTIVE:
        q = s.q_interactive
    for i, x in enumerate(q):
        if x is p:
            q.pop(i)
            return


def _op_key(pipeline, op):
    # Best-effort stable key.
    # Many simulators give ops with .op_id or .operator_id; fall back to object id.
    op_id = getattr(op, "op_id", None)
    if op_id is None:
        op_id = getattr(op, "operator_id", None)
    if op_id is None:
        op_id = getattr(op, "name", None)
    if op_id is None:
        op_id = id(op)
    return (pipeline.pipeline_id, op_id)


def _estimate_ram_for_op(s, pipeline, op, pool_avail_ram):
    # Try to read the op's minimum RAM requirement if exposed by the simulator.
    # Fall back to a conservative fraction of available RAM to reduce OOM risk.
    base = None
    for attr in ("min_ram", "min_ram_gb", "ram_min", "required_ram", "peak_ram", "true_ram"):
        if hasattr(op, attr):
            try:
                base = float(getattr(op, attr))
                break
            except Exception:
                pass

    if base is None:
        # Conservative default: take up to 1/4 of available, but not more than pool.
        base = max(0.5, pool_avail_ram * 0.25) if pool_avail_ram > 0 else 0.5

    mult = s.op_ram_backoff.get(_op_key(pipeline, op), 1.0)
    ram = base * mult

    # Never exceed available; if extremely constrained, allow tiny allocations (may fail, but avoids deadlock).
    ram = min(ram, pool_avail_ram) if pool_avail_ram > 0 else ram
    return max(0.1, ram)


def _estimate_cpu_for_op(s, pool_avail_cpu, pool_max_cpu, priority):
    # Give more CPU to higher priority but keep it simple and avoid starving others.
    if pool_avail_cpu <= 0:
        return 0.0
    if priority == Priority.QUERY:
        target = min(pool_avail_cpu, max(s.min_cpu_slice, 0.75 * pool_max_cpu))
    elif priority == Priority.INTERACTIVE:
        target = min(pool_avail_cpu, max(s.min_cpu_slice, 0.50 * pool_max_cpu))
    else:
        target = min(pool_avail_cpu, max(s.min_cpu_slice, 0.25 * pool_max_cpu))
    return max(s.min_cpu_slice if pool_avail_cpu >= s.min_cpu_slice else pool_avail_cpu, target)


def _pool_headroom_after_reserve(s, pool, prio):
    # Compute effective available resources after soft reservations for higher priorities.
    # This is a "soft" mechanism: if only batch exists, it can still use all resources.
    avail_cpu = pool.avail_cpu_pool
    avail_ram = pool.avail_ram_pool

    # For a given request priority, reserve capacity for *higher* priorities only.
    reserve_cpu = 0.0
    reserve_ram = 0.0
    if prio == Priority.BATCH_PIPELINE:
        reserve_cpu += pool.max_cpu_pool * s.reserved_frac[Priority.QUERY]
        reserve_cpu += pool.max_cpu_pool * s.reserved_frac[Priority.INTERACTIVE]
        reserve_ram += pool.max_ram_pool * s.reserved_frac[Priority.QUERY]
        reserve_ram += pool.max_ram_pool * s.reserved_frac[Priority.INTERACTIVE]
    elif prio == Priority.INTERACTIVE:
        reserve_cpu += pool.max_cpu_pool * s.reserved_frac[Priority.QUERY]
        reserve_ram += pool.max_ram_pool * s.reserved_frac[Priority.QUERY]

    eff_cpu = max(0.0, avail_cpu - reserve_cpu)
    eff_ram = max(0.0, avail_ram - reserve_ram)
    return eff_cpu, eff_ram


def _collect_running_containers(results):
    # Best effort: some simulators include RUNNING results; otherwise, no preemption.
    running = []
    for r in results:
        # If result has a state field, use it; otherwise, can't infer running here.
        state = getattr(r, "state", None)
        if state is not None and state == OperatorState.RUNNING:
            running.append(r)
    return running


def _maybe_update_backoff_from_results(s, results):
    for r in results:
        if not hasattr(r, "ops") or not r.ops:
            continue
        pipeline_id = getattr(r, "pipeline_id", None)
        # We can't always access pipeline_id from result; we still backoff using op object id only.
        for op in r.ops:
            # Failure signal: r.failed() or r.error not None
            failed = False
            try:
                failed = r.failed()
            except Exception:
                failed = bool(getattr(r, "error", None))
            if not failed:
                continue

            # Heuristic: treat any failure as potential OOM and backoff RAM.
            # (This reduces repeated failures, heavily penalized by the objective.)
            if pipeline_id is None:
                key = (None, getattr(op, "op_id", None) or getattr(op, "operator_id", None) or getattr(op, "name", None) or id(op))
            else:
                key = (pipeline_id, getattr(op, "op_id", None) or getattr(op, "operator_id", None) or getattr(op, "name", None) or id(op))

            cur = s.op_ram_backoff.get(key, 1.0)
            s.op_ram_backoff[key] = min(s.max_backoff, cur * s.backoff_step)


def _choose_next_assignable_op(s, pipeline):
    status = pipeline.runtime_status()
    if status.is_pipeline_successful():
        return None

    # Don't retry pipelines with failures stuck (but allow retry of FAILED ops if simulator supports it).
    # We prefer retry to dropping because failure penalty is huge.
    op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
    if not op_list:
        return None
    return op_list[:1]  # one op at a time


def _pipeline_is_done_or_hopeless(pipeline):
    status = pipeline.runtime_status()
    if status.is_pipeline_successful():
        return True
    # If the simulator marks failures that are terminal, runtime_status might keep FAILED ops.
    # We still keep it for retry (failure-averse), so don't drop here.
    return False


@register_scheduler(key="scheduler_none_013")
def scheduler_none_013(s, results, pipelines):
    """
    Scheduler step:
      1) Ingest new pipelines into per-priority FIFO queues.
      2) Update RAM backoff from recent failures (aim to avoid repeated 720s penalties).
      3) For each pool, attempt to schedule highest-priority ready work subject to soft reservations.
      4) If high-priority work is blocked by lack of capacity, optionally preempt batch work.
    """
    # Enqueue new arrivals
    for p in pipelines:
        s.arrivals[p.priority] = s.arrivals.get(p.priority, 0) + 1
        _enqueue_pipeline(s, p)

    # No changes => no action
    if not pipelines and not results:
        return [], []

    # Update backoff based on observed failures
    _maybe_update_backoff_from_results(s, results)

    suspensions = []
    assignments = []

    # Clean completed pipelines from queues to prevent buildup
    for q in _all_queues(s):
        i = 0
        while i < len(q):
            if _pipeline_is_done_or_hopeless(q[i]):
                q.pop(i)
            else:
                i += 1

    # Try to schedule work on each pool
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]

        # Attempt to fill pool with as many assignments as possible (one op at a time).
        # We re-check availability after each assignment.
        made_progress = True
        while made_progress:
            made_progress = False

            # Find best (highest-priority) pipeline with an assignable op that fits.
            chosen = None
            chosen_op_list = None
            chosen_cpu = 0.0
            chosen_ram = 0.0

            # Consider pipelines in priority order
            for pipeline in _iter_pipelines_in_priority_order(s):
                op_list = _choose_next_assignable_op(s, pipeline)
                if not op_list:
                    continue

                # Effective headroom after reserving for higher priorities
                eff_cpu, eff_ram = _pool_headroom_after_reserve(s, pool, pipeline.priority)
                # If this priority has no effective headroom, still allow if higher queues are empty.
                if eff_cpu <= 0 or eff_ram <= 0:
                    # Only let lower priorities borrow reserved capacity when no higher-priority backlog exists
                    higher_backlog = False
                    if pipeline.priority == Priority.BATCH_PIPELINE and (len(s.q_query) > 0 or len(s.q_interactive) > 0):
                        higher_backlog = True
                    if pipeline.priority == Priority.INTERACTIVE and len(s.q_query) > 0:
                        higher_backlog = True
                    if higher_backlog:
                        continue
                    eff_cpu = pool.avail_cpu_pool
                    eff_ram = pool.avail_ram_pool

                if eff_cpu <= 0 or eff_ram <= 0:
                    continue

                # Determine requested resources
                op = op_list[0]
                req_ram = _estimate_ram_for_op(s, pipeline, op, eff_ram)
                req_cpu = _estimate_cpu_for_op(s, eff_cpu, pool.max_cpu_pool, pipeline.priority)

                # Fit check
                if req_cpu <= 0 or req_ram <= 0:
                    continue
                if req_cpu > eff_cpu + 1e-9 or req_ram > eff_ram + 1e-9:
                    continue

                chosen = pipeline
                chosen_op_list = op_list
                chosen_cpu = req_cpu
                chosen_ram = req_ram
                break  # strict priority order: first feasible wins

            if chosen is None:
                # Consider preemption if queries/interactive are waiting but can't fit due to low avail.
                if not s.preempt_enabled:
                    break

                # If there is pending query/interactive work, and pool has low availability,
                # attempt to suspend some batch/interactive work to free room.
                need_prio = None
                if len(s.q_query) > 0:
                    need_prio = Priority.QUERY
                elif len(s.q_interactive) > 0:
                    need_prio = Priority.INTERACTIVE

                if need_prio is None:
                    break

                # Compute a target free capacity fraction for the needed priority
                target_cpu = pool.max_cpu_pool * (0.30 if need_prio == Priority.QUERY else 0.20)
                target_ram = pool.max_ram_pool * (0.30 if need_prio == Priority.QUERY else 0.20)

                if pool.avail_cpu_pool >= target_cpu and pool.avail_ram_pool >= target_ram:
                    break

                # Best-effort preemption list: suspend lowest-priority containers we know about.
                # Note: If the simulator doesn't expose running containers via results, this is a no-op.
                running = _collect_running_containers(results)
                # Sort by priority descending (batch first), then by resource size descending to free faster.
                running.sort(
                    key=lambda r: (
                        _priority_rank(getattr(r, "priority", Priority.BATCH_PIPELINE)),
                        -(getattr(r, "cpu", 0.0) or 0.0),
                        -(getattr(r, "ram", 0.0) or 0.0),
                    ),
                    reverse=True,
                )

                freed_cpu = 0.0
                freed_ram = 0.0
                for r in running:
                    r_prio = getattr(r, "priority", Priority.BATCH_PIPELINE)
                    if _priority_rank(r_prio) <= _priority_rank(need_prio):
                        # Don't preempt same-or-higher priority
                        continue
                    cid = getattr(r, "container_id", None)
                    r_pool = getattr(r, "pool_id", None)
                    if cid is None or r_pool is None or r_pool != pool_id:
                        continue
                    suspensions.append(Suspend(cid, pool_id))
                    freed_cpu += float(getattr(r, "cpu", 0.0) or 0.0)
                    freed_ram += float(getattr(r, "ram", 0.0) or 0.0)
                    if freed_cpu >= s.preempt_min_gain_cpu or freed_ram >= s.preempt_min_gain_ram:
                        break

                # If we couldn't preempt anything, stop trying for this pool this tick.
                if freed_cpu <= 0.0 and freed_ram <= 0.0:
                    break

                # After suspending, loop again; pool availability will reflect on next tick in simulator.
                break

            # Emit assignment
            assignments.append(
                Assignment(
                    ops=chosen_op_list,
                    cpu=chosen_cpu,
                    ram=chosen_ram,
                    priority=chosen.priority,
                    pool_id=pool_id,
                    pipeline_id=chosen.pipeline_id,
                )
            )
            made_progress = True

            # Rotate chosen pipeline to end of its priority queue for fairness within priority
            _remove_pipeline_from_queue(s, chosen)
            _enqueue_pipeline(s, chosen)

            # If we can only assign one op per pool per tick, break
            if s.assign_one_op:
                break

    return suspensions, assignments
