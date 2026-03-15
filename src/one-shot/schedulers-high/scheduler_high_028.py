# policy_key: scheduler_high_028
# reasoning_effort: high
# model: gpt-5.2-2025-12-11
# llm_cost: 0.172745
# generation_seconds: 161.39
# generated_at: 2026-03-14T03:45:58.728069
@register_scheduler_init(key="scheduler_high_028")
def scheduler_high_028_init(s):
    """
    Priority-aware, latency-oriented scheduler (small, safe improvements over naive FIFO).

    Key ideas (kept intentionally simple and robust):
    - Maintain separate FIFO queues per priority (QUERY > INTERACTIVE > BATCH).
    - Avoid "assign everything to one op": right-size per-op CPU/RAM using fractions + caps,
      enabling more concurrent progress and better tail latency under contention.
    - Protect latency by reserving headroom: when high-priority work is waiting, cap how much
      batch can consume (per pool) and limit batch placements per tick.
    - Basic OOM-aware retries: if an operator fails with an OOM-like error, increase its RAM hint
      for the next attempt; mark non-OOM failures as permanent to avoid infinite retries.
    """
    from collections import deque

    s.queues = {
        Priority.QUERY: deque(),
        Priority.INTERACTIVE: deque(),
        Priority.BATCH_PIPELINE: deque(),
    }

    # Operator-level hints keyed by operator object identity (stable within simulation).
    s.op_ram_hint = {}          # op_key -> suggested RAM for next attempt
    s.op_oom_retries = {}       # op_key -> retry count
    s.op_permanent_fail = {}    # op_key -> bool

    # Tunables (kept conservative).
    s.max_oom_retries = 3
    s.oom_ram_backoff = 1.8

    # Headroom protection: when high priority is waiting, batch is capped to leave slack.
    s.reserve_frac = 0.20  # reserve 20% of each pool for high priority (enforced by capping batch only)
    s.max_batch_assignments_per_tick_when_high = 2

    # Per-op sizing: allocate a fraction of *currently available* resources with per-priority caps.
    s.target_frac = {
        Priority.QUERY: 0.55,
        Priority.INTERACTIVE: 0.45,
        Priority.BATCH_PIPELINE: 0.30,
    }
    s.max_frac_of_pool = {
        Priority.QUERY: 0.80,
        Priority.INTERACTIVE: 0.60,
        Priority.BATCH_PIPELINE: 0.45,
    }

    # Floors to avoid pathological 0 allocations.
    s.cpu_floor = 1
    s.ram_floor = 1.0

    # Bound the amount of work we try to place per tick to keep the scheduler cheap/deterministic.
    s.max_total_assignments_per_tick = 64
    s.tick = 0


def _is_oom_error(err) -> bool:
    if err is None:
        return False
    msg = str(err).lower()
    # Cover common OOM patterns across runtimes.
    return ("oom" in msg) or ("out of memory" in msg) or ("killed process" in msg and "memory" in msg)


def _op_key(op) -> int:
    # Use object identity; in the simulator the operator object is typically stable across ticks.
    return id(op)


def _has_high_priority_waiting(s) -> bool:
    # Quick check; we keep it intentionally simple to avoid scanning large queues every tick.
    return bool(s.queues[Priority.QUERY]) or bool(s.queues[Priority.INTERACTIVE])


def _drop_pipeline_if_done_or_permafailed(s, pipeline) -> bool:
    """
    Returns True if the pipeline should be dropped (completed or permanently failed).
    """
    status = pipeline.runtime_status()
    if status.is_pipeline_successful():
        return True

    # If any FAILED operator is marked permanent, consider the pipeline failed and drop it.
    failed_ops = status.get_ops([OperatorState.FAILED], require_parents_complete=False)
    for op in failed_ops:
        k = _op_key(op)
        if s.op_permanent_fail.get(k, False):
            return True
        if s.op_oom_retries.get(k, 0) > s.max_oom_retries:
            s.op_permanent_fail[k] = True
            return True

    return False


def _dequeue_next_ready_op(s, prio):
    """
    Pop/rotate within a priority queue to find one runnable op (parents complete).
    Returns (pipeline, [op]) with pipeline removed from queue, or (None, None).
    """
    q = s.queues[prio]
    if not q:
        return None, None

    # Rotate at most once through the queue.
    for _ in range(len(q)):
        pipeline = q.popleft()

        if _drop_pipeline_if_done_or_permafailed(s, pipeline):
            continue

        status = pipeline.runtime_status()
        op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
        if not op_list:
            # Not runnable yet; keep FIFO order by appending to the end.
            q.append(pipeline)
            continue

        op = op_list[0]
        if s.op_permanent_fail.get(_op_key(op), False):
            # Can't make progress; drop pipeline.
            continue

        return pipeline, op_list

    return None, None


def _requeue_pipeline(s, pipeline):
    s.queues[pipeline.priority].append(pipeline)


def _plan_allocation_for_pool(s, pool, pool_id, prio, op, cpu_left, ram_left, high_waiting, batch_cpu_cap, batch_ram_cap):
    """
    Decide (cpu, ram) for placing this op on this pool given remaining capacity snapshots.
    Returns (cpu, ram) or None if it doesn't fit / violates caps.
    """
    # Enforce batch caps only when high priority is waiting.
    if prio == Priority.BATCH_PIPELINE and high_waiting:
        cpu_cap = min(cpu_left, batch_cpu_cap[pool_id])
        ram_cap = min(ram_left, batch_ram_cap[pool_id])
    else:
        cpu_cap = cpu_left
        ram_cap = ram_left

    if cpu_cap < s.cpu_floor or ram_cap < s.ram_floor:
        return None

    # Base sizing from available capacity (fractional), with per-priority cap as fraction of pool max.
    target = s.target_frac.get(prio, 0.30)
    max_frac = s.max_frac_of_pool.get(prio, 0.50)

    cpu_req = int(cpu_cap * target)
    cpu_req = max(s.cpu_floor, cpu_req)
    cpu_req = min(cpu_req, int(pool.max_cpu_pool * max_frac), int(cpu_cap))

    if cpu_req < s.cpu_floor:
        return None

    ram_req = float(ram_cap) * target
    ram_req = max(s.ram_floor, ram_req)
    ram_req = min(ram_req, float(pool.max_ram_pool) * max_frac, float(ram_cap))

    # Apply OOM-derived RAM hint if present.
    hint = s.op_ram_hint.get(_op_key(op))
    if hint is not None:
        # If hint exceeds cap, this pool can't satisfy it now.
        if hint > float(ram_cap):
            return None
        ram_req = max(ram_req, float(hint))

    # Final fit check
    if cpu_req > cpu_cap or ram_req > ram_cap:
        return None

    return cpu_req, ram_req


@register_scheduler(key="scheduler_high_028")
def scheduler_high_028(s, results: List["ExecutionResult"], pipelines: List["Pipeline"]) -> Tuple[List["Suspend"], List["Assignment"]]:
    """
    Priority-aware scheduler with headroom protection + basic OOM-adaptive RAM hints.
    """
    s.tick += 1

    suspensions: List[Suspend] = []
    assignments: List[Assignment] = []

    # Enqueue arrivals by priority.
    for p in pipelines:
        s.queues[p.priority].append(p)

    # Process results to learn from failures (especially OOM).
    for r in results:
        if not r.failed():
            continue

        is_oom = _is_oom_error(r.error)
        for op in (r.ops or []):
            k = _op_key(op)
            if is_oom:
                s.op_oom_retries[k] = s.op_oom_retries.get(k, 0) + 1

                # Increase RAM hint; start from the RAM used in the failed attempt.
                try:
                    attempted_ram = float(r.ram)
                except Exception:
                    attempted_ram = None

                if attempted_ram is not None:
                    prev = float(s.op_ram_hint.get(k, 0.0))
                    bumped = max(prev, attempted_ram * s.oom_ram_backoff)
                    s.op_ram_hint[k] = bumped

                if s.op_oom_retries[k] > s.max_oom_retries:
                    s.op_permanent_fail[k] = True
            else:
                # Non-OOM failures are treated as permanent for now (simple, avoids infinite retries).
                s.op_permanent_fail[k] = True

    # Early exit if nothing to do.
    if not pipelines and not results and not any(s.queues.values()):
        return [], []

    # Snapshot remaining capacity per pool; we'll mutate these locals as we "virtually place" assignments.
    num_pools = s.executor.num_pools
    cpu_left = [0] * num_pools
    ram_left = [0.0] * num_pools
    batch_cpu_cap = [0] * num_pools
    batch_ram_cap = [0.0] * num_pools

    high_waiting = _has_high_priority_waiting(s)

    for pool_id in range(num_pools):
        pool = s.executor.pools[pool_id]
        cpu_left[pool_id] = int(pool.avail_cpu_pool)
        ram_left[pool_id] = float(pool.avail_ram_pool)

        if high_waiting:
            # Leave headroom by capping batch consumption only.
            reserve_cpu = float(pool.max_cpu_pool) * float(s.reserve_frac)
            reserve_ram = float(pool.max_ram_pool) * float(s.reserve_frac)
            batch_cpu_cap[pool_id] = max(0, int(cpu_left[pool_id] - reserve_cpu))
            batch_ram_cap[pool_id] = max(0.0, float(ram_left[pool_id] - reserve_ram))
        else:
            batch_cpu_cap[pool_id] = int(cpu_left[pool_id])
            batch_ram_cap[pool_id] = float(ram_left[pool_id])

    # Place work greedily but with priority ordering and pool-choice heuristics.
    batch_assigned_this_tick = 0
    no_progress_spins = 0

    for _ in range(s.max_total_assignments_per_tick):
        high_waiting = _has_high_priority_waiting(s)

        # Choose the next runnable op by strict priority.
        chosen_pipeline = None
        chosen_ops = None
        chosen_prio = None

        for pr in (Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE):
            if pr == Priority.BATCH_PIPELINE and high_waiting and batch_assigned_this_tick >= s.max_batch_assignments_per_tick_when_high:
                continue
            pipeline, ops = _dequeue_next_ready_op(s, pr)
            if pipeline is not None:
                chosen_pipeline, chosen_ops, chosen_prio = pipeline, ops, pr
                break

        if chosen_pipeline is None:
            break  # no runnable work found

        op = chosen_ops[0]

        # Find the best pool for this op.
        best_pool_id = None
        best_cpu = None
        best_ram = None
        best_score = None

        for pool_id in range(num_pools):
            pool = s.executor.pools[pool_id]
            plan = _plan_allocation_for_pool(
                s=s,
                pool=pool,
                pool_id=pool_id,
                prio=chosen_prio,
                op=op,
                cpu_left=cpu_left[pool_id],
                ram_left=ram_left[pool_id],
                high_waiting=high_waiting,
                batch_cpu_cap=batch_cpu_cap,
                batch_ram_cap=batch_ram_cap,
            )
            if plan is None:
                continue

            cpu_req, ram_req = plan

            # Scoring:
            # - For QUERY/INTERACTIVE: prefer pools with more headroom after placement (less likely to block),
            #   and also allow larger allocations (lower runtime).
            # - For BATCH: best-fit packing (minimize leftover) to keep big pools available for latency-sensitive work.
            if chosen_prio in (Priority.QUERY, Priority.INTERACTIVE):
                score = (cpu_left[pool_id] - cpu_req) * 1.0 + (ram_left[pool_id] - ram_req) * 0.01
            else:
                score = (ram_left[pool_id] - ram_req) * 1.0 + (cpu_left[pool_id] - cpu_req) * 0.1

            if best_score is None or score < best_score:
                best_score = score
                best_pool_id = pool_id
                best_cpu = cpu_req
                best_ram = ram_req

        if best_pool_id is None:
            # Couldn't place anywhere right now; requeue and avoid busy-looping forever.
            _requeue_pipeline(s, chosen_pipeline)
            no_progress_spins += 1
            if no_progress_spins > (len(s.queues[Priority.QUERY]) + len(s.queues[Priority.INTERACTIVE]) + len(s.queues[Priority.BATCH_PIPELINE]) + 1):
                break
            continue

        no_progress_spins = 0

        assignments.append(
            Assignment(
                ops=chosen_ops,
                cpu=best_cpu,
                ram=best_ram,
                priority=chosen_pipeline.priority,
                pool_id=best_pool_id,
                pipeline_id=chosen_pipeline.pipeline_id,
            )
        )

        # Update local capacity snapshots.
        cpu_left[best_pool_id] -= int(best_cpu)
        ram_left[best_pool_id] -= float(best_ram)

        if chosen_prio == Priority.BATCH_PIPELINE and high_waiting:
            batch_cpu_cap[best_pool_id] = max(0, int(batch_cpu_cap[best_pool_id] - int(best_cpu)))
            batch_ram_cap[best_pool_id] = max(0.0, float(batch_ram_cap[best_pool_id] - float(best_ram)))
            batch_assigned_this_tick += 1

        # Keep pipeline in circulation until it finishes.
        _requeue_pipeline(s, chosen_pipeline)

    return suspensions, assignments
