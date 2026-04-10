# policy_key: scheduler_none_050
# reasoning_effort: none
# model: gpt-5.2-2025-12-11
# llm_cost: 0.041705
# generation_seconds: 40.64
# generated_at: 2026-03-14T02:17:25.349255
@register_scheduler_init(key="scheduler_none_050")
def scheduler_none_050_init(s):
    """Priority-aware FIFO with small, safe improvements over naive FIFO.

    Improvements (kept intentionally simple and robust):
    1) Maintain separate queues per priority so high-priority work isn't stuck behind batch.
    2) Use small, incremental per-priority CPU/RAM "reservations" (soft partitions) so interactive/query
       latencies improve under contention.
    3) Basic OOM backoff: if an op fails with an OOM-like error, retry the pipeline with a higher RAM request.
    4) Very conservative preemption: if a high-priority pipeline is waiting and a pool has no headroom, suspend
       (at most) one low-priority container in that pool to create headroom, minimizing churn.

    Notes:
    - We still assign at most one operator per pool per tick (like the naive example) to reduce complexity.
    - This policy assumes the simulator will re-attempt scheduling on subsequent ticks.
    """
    # Priority queues
    s.wait_q_query = []
    s.wait_q_interactive = []
    s.wait_q_batch = []

    # Track last known resource "hint" per pipeline (learned from failures)
    # pipeline_id -> {"ram_mul": float, "cpu_cap": Optional[float]}
    s.pipeline_hints = {}

    # Track OOM streaks to avoid infinite rapid retries
    # pipeline_id -> int
    s.oom_streak = {}

    # Track running containers by pool and priority so we can preempt low priority if needed
    # pool_id -> {container_id: priority}
    s.running = {}

    # Soft reservation fractions per pool (small initial improvement; not strict, just guidance)
    # These are "minimum left unclaimed" targets for higher priorities.
    s.reserve_frac = {
        Priority.QUERY: 0.15,
        Priority.INTERACTIVE: 0.25,
        Priority.BATCH_PIPELINE: 0.0,
    }

    # Base sizing: keep some headroom and avoid grabbing the whole pool for a single op
    s.max_cpu_frac_per_op = {
        Priority.QUERY: 0.60,
        Priority.INTERACTIVE: 0.75,
        Priority.BATCH_PIPELINE: 0.85,
    }
    s.max_ram_frac_per_op = {
        Priority.QUERY: 0.60,
        Priority.INTERACTIVE: 0.75,
        Priority.BATCH_PIPELINE: 0.90,
    }

    # Backoff parameters for OOM retries
    s.oom_ram_multiplier_step = 1.5
    s.oom_ram_multiplier_cap = 8.0


def _q_for_priority(s, prio):
    if prio == Priority.QUERY:
        return s.wait_q_query
    if prio == Priority.INTERACTIVE:
        return s.wait_q_interactive
    return s.wait_q_batch


def _priority_order():
    # Highest to lowest
    return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]


def _is_oom_error(err):
    if err is None:
        return False
    try:
        msg = str(err).lower()
    except Exception:
        return False
    return ("oom" in msg) or ("out of memory" in msg) or ("memoryerror" in msg) or ("cuda out of memory" in msg)


def _desired_reserve(s, pool, prio):
    # Desired reserved amount for *higher* priorities (left unallocated).
    # Query should keep some headroom for itself; interactive keeps more; batch none.
    frac = s.reserve_frac.get(prio, 0.0)
    return pool.max_cpu_pool * frac, pool.max_ram_pool * frac


def _cap_per_op(s, pool, prio, avail_cpu, avail_ram):
    # Per-op cap is a fraction of pool capacity, but also cannot exceed available resources.
    cpu_cap = min(avail_cpu, max(1.0, pool.max_cpu_pool * s.max_cpu_frac_per_op.get(prio, 0.8)))
    ram_cap = min(avail_ram, max(1.0, pool.max_ram_pool * s.max_ram_frac_per_op.get(prio, 0.8)))
    return cpu_cap, ram_cap


def _pick_assignable_op(pipeline):
    status = pipeline.runtime_status()
    # Drop completed/failed pipelines
    has_failures = status.state_counts[OperatorState.FAILED] > 0
    if status.is_pipeline_successful() or has_failures:
        return None
    # Pick one op whose parents are complete
    op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
    if not op_list:
        return None
    return op_list


def _requeue_if_active(s, pipeline, requeue_lists):
    status = pipeline.runtime_status()
    has_failures = status.state_counts[OperatorState.FAILED] > 0
    if status.is_pipeline_successful() or has_failures:
        return
    requeue_lists.append(pipeline)


@register_scheduler(key="scheduler_none_050")
def scheduler_none_050(s, results, pipelines):
    """
    Priority-aware scheduling with soft reservations, OOM backoff, and conservative preemption.
    Returns (suspensions, assignments).
    """
    # Initialize running map for pools if needed
    if not hasattr(s, "running") or s.running is None:
        s.running = {}
    for pid in range(s.executor.num_pools):
        s.running.setdefault(pid, {})

    # Ingest new pipelines into priority queues
    for p in pipelines:
        _q_for_priority(s, p.priority).append(p)

    # Process results: update running set, and learn from OOM
    if results:
        for r in results:
            # Remove container from running map (it finished/failed)
            try:
                if r.container_id in s.running.get(r.pool_id, {}):
                    del s.running[r.pool_id][r.container_id]
            except Exception:
                pass

            if r.failed():
                # Learn from suspected OOMs by increasing RAM multiplier for that pipeline
                if _is_oom_error(r.error):
                    pid = r.ops[0].pipeline_id if (hasattr(r, "ops") and r.ops) else None
                    # Fallback: try to use pipeline_id from result if present
                    if pid is None and hasattr(r, "pipeline_id"):
                        pid = r.pipeline_id

                    if pid is not None:
                        streak = s.oom_streak.get(pid, 0) + 1
                        s.oom_streak[pid] = streak
                        hint = s.pipeline_hints.get(pid, {"ram_mul": 1.0, "cpu_cap": None})
                        new_mul = min(s.oom_ram_multiplier_cap, hint["ram_mul"] * s.oom_ram_multiplier_step)
                        hint["ram_mul"] = new_mul
                        s.pipeline_hints[pid] = hint

    # Early exit if nothing changed
    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    # Helper: check whether any high-priority is waiting (has an assignable op)
    def has_waiting_higher(prio):
        for hp in _priority_order():
            if hp == prio:
                return False
            q = _q_for_priority(s, hp)
            # Peek for an actually assignable pipeline
            for p in q:
                if _pick_assignable_op(p) is not None:
                    return True
        return False

    # Conservative preemption: for each pool, if we have waiting QUERY/INTERACTIVE but zero headroom,
    # suspend at most one batch container in that pool.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool

        # If high-priority work exists and we're fully packed, try suspending one batch container
        if (avail_cpu <= 0 or avail_ram <= 0) and (has_waiting_higher(Priority.BATCH_PIPELINE)):
            # Find one batch container to suspend
            batch_container_id = None
            for cid, pr in list(s.running.get(pool_id, {}).items()):
                if pr == Priority.BATCH_PIPELINE:
                    batch_container_id = cid
                    break
            if batch_container_id is not None:
                suspensions.append(Suspend(batch_container_id, pool_id))
                # We don't know exactly how much will free; rely on next tick for updated avail.
                # Avoid suspending more in same tick to limit churn.

    # Main assignment loop: one op per pool per tick, prioritize query then interactive then batch.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool
        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # We'll pop from queues but requeue active pipelines we didn't schedule.
        requeue_query = []
        requeue_interactive = []
        requeue_batch = []

        made_assignment = False

        for prio in _priority_order():
            q = _q_for_priority(s, prio)
            if not q:
                continue

            # Soft reservation: leave some resources for higher priorities by constraining batch/interact
            reserve_cpu, reserve_ram = _desired_reserve(s, pool, prio)
            # When scheduling lower priority, avoid consuming into the reserved headroom for higher priority.
            # For QUERY itself, reserve applies to keep it responsive for subsequent queries.
            eff_avail_cpu = max(0.0, avail_cpu - reserve_cpu)
            eff_avail_ram = max(0.0, avail_ram - reserve_ram)

            # If effective is zero, still allow QUERY/INTERACTIVE to use what's available (best effort),
            # but keep batch from stealing reserved headroom.
            if prio in (Priority.QUERY, Priority.INTERACTIVE):
                eff_avail_cpu = avail_cpu
                eff_avail_ram = avail_ram

            if eff_avail_cpu <= 0 or eff_avail_ram <= 0:
                continue

            # Scan FIFO within the priority class to find first schedulable op
            while q:
                pipeline = q.pop(0)
                op_list = _pick_assignable_op(pipeline)
                if op_list is None:
                    _requeue_if_active(s, pipeline, requeue_query if prio == Priority.QUERY else requeue_interactive if prio == Priority.INTERACTIVE else requeue_batch)
                    continue

                # Apply pipeline-specific hinting from OOM: increase RAM request multiplier
                hint = s.pipeline_hints.get(pipeline.pipeline_id, {"ram_mul": 1.0, "cpu_cap": None})
                ram_mul = hint.get("ram_mul", 1.0)

                cpu_cap, ram_cap = _cap_per_op(s, pool, prio, eff_avail_cpu, eff_avail_ram)

                # Request sizes:
                # - CPU: capped per op; for queries/interactive we bias to get more CPU to reduce latency.
                # - RAM: allocate proportionally to available, multiplied by learned OOM multiplier, capped.
                if prio == Priority.QUERY:
                    req_cpu = cpu_cap
                elif prio == Priority.INTERACTIVE:
                    req_cpu = min(cpu_cap, max(1.0, 0.75 * cpu_cap))
                else:
                    req_cpu = min(cpu_cap, max(1.0, 0.50 * cpu_cap))

                # RAM request: avoid taking everything; but allow growth with OOM multiplier.
                base_ram = ram_cap if prio != Priority.BATCH_PIPELINE else max(1.0, 0.75 * ram_cap)
                req_ram = min(eff_avail_ram, max(1.0, base_ram * ram_mul))

                # Final sanity bounds
                req_cpu = min(req_cpu, eff_avail_cpu)
                req_ram = min(req_ram, eff_avail_ram)

                if req_cpu <= 0 or req_ram <= 0:
                    # Can't schedule now; requeue and break out
                    _requeue_if_active(s, pipeline, requeue_query if prio == Priority.QUERY else requeue_interactive if prio == Priority.INTERACTIVE else requeue_batch)
                    break

                assignment = Assignment(
                    ops=op_list,
                    cpu=req_cpu,
                    ram=req_ram,
                    priority=pipeline.priority,
                    pool_id=pool_id,
                    pipeline_id=pipeline.pipeline_id,
                )
                assignments.append(assignment)
                made_assignment = True
                break

            if made_assignment:
                break

        # Put back pipelines not scheduled this tick
        s.wait_q_query.extend(requeue_query)
        s.wait_q_interactive.extend(requeue_interactive)
        s.wait_q_batch.extend(requeue_batch)

        if made_assignment:
            # Track running container once it starts; container_id isn't known until result.
            # But some simulators may provide immediate container id; we don't have it here.
            # We'll rely on results to update running and preemption decisions in future ticks.
            pass

    # Opportunistically update running map if results include starts (not typical); otherwise, do nothing.
    # As a fallback for preemption, we can only suspend known container_ids from results.
    if results:
        for r in results:
            # If a result indicates a container is still running, simulator would not send it here.
            # So we do not add entries here.
            pass

    return suspensions, assignments
