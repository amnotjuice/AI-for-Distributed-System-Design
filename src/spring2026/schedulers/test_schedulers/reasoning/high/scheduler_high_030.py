# policy_key: scheduler_high_030
# reasoning_effort: high
# exp: reasoning
# model: gpt-5.2-2025-12-11
# llm_cost: 0.139185
# generation_seconds: 193.84
# generated_at: 2026-04-10T02:38:20.379189
@register_scheduler_init(key="scheduler_high_030")
def scheduler_high_030_init(s):
    """Priority-aware, failure-averse scheduler.

    Main ideas:
      - Separate queues per priority; always try to run QUERY then INTERACTIVE first.
      - Avoid OOM-induced pipeline failures by:
          * starting with moderately conservative RAM allocations (fraction of pool RAM),
          * learning per-operator RAM needs from failures (double RAM on OOM and retry),
          * refusing to (re)run an operator unless we can meet its learned RAM target.
      - Prevent batch starvation with a small "batch token" mechanism and mild aging.
      - No preemption (keeps churn low and avoids relying on non-portable executor APIs).
    """
    from collections import deque

    s.now = 0

    # Per-priority FIFO queues of Pipeline objects
    s.queues = {
        Priority.QUERY: deque(),
        Priority.INTERACTIVE: deque(),
        Priority.BATCH_PIPELINE: deque(),
    }

    # Track when pipelines arrived (for aging); keyed by pipeline_id
    s.pipeline_arrival = {}

    # Per-operator learned profile, keyed by id(op)
    # Fields:
    #   desired_ram: target RAM to allocate next time
    #   desired_cpu: target CPU to allocate next time
    #   failures: count of failures observed
    #   oom_failures: count of likely OOM failures observed
    #   last_ram/last_cpu: last attempted resources
    s.op_profile = {}

    # Batch progress controls
    s.batch_tokens = 0
    s.max_batch_tokens = 3
    s.batch_token_refill = 1

    # Retry policy
    s.max_retries = 8

    # Aging: after enough scheduler ticks, allow batch to compete as interactive sometimes
    s.batch_age_boost_ticks = 80

    # When scheduling batch on shared pools, keep a small headroom to reduce tail latency for future high-pri arrivals
    s.batch_headroom_frac_cpu = 0.10
    s.batch_headroom_frac_ram = 0.10


def _is_oom_error(err) -> bool:
    if not err:
        return False
    e = str(err).lower()
    return ("oom" in e) or ("out of memory" in e) or ("memory" in e and "alloc" in e)


def _base_cpu_for_priority(priority, pool_max_cpu: float) -> float:
    # Keep CPU modest to allow concurrency; CPU scaling is sublinear in the simulator.
    if priority == Priority.QUERY:
        return min(4.0, float(pool_max_cpu))
    if priority == Priority.INTERACTIVE:
        return min(3.0, float(pool_max_cpu))
    return min(2.0, float(pool_max_cpu))


def _base_ram_for_priority(priority, pool_max_ram: float) -> float:
    # Start moderately conservative to reduce OOM probability (OOM penalties dominate the objective).
    if priority == Priority.QUERY:
        frac = 0.25
    elif priority == Priority.INTERACTIVE:
        frac = 0.22
    else:
        frac = 0.18
    return max(0.10 * float(pool_max_ram), frac * float(pool_max_ram))


def _get_op_profile(s, op):
    oid = id(op)
    prof = s.op_profile.get(oid)
    if prof is None:
        prof = {
            "desired_ram": None,
            "desired_cpu": None,
            "failures": 0,
            "oom_failures": 0,
            "last_ram": None,
            "last_cpu": None,
        }
        s.op_profile[oid] = prof
    return prof


def _requested_resources(s, op, priority, pool_max_cpu: float, pool_max_ram: float):
    base_cpu = _base_cpu_for_priority(priority, pool_max_cpu)
    base_ram = _base_ram_for_priority(priority, pool_max_ram)

    prof = _get_op_profile(s, op)

    desired_cpu = prof["desired_cpu"] if prof["desired_cpu"] is not None else base_cpu
    desired_ram = prof["desired_ram"] if prof["desired_ram"] is not None else base_ram

    # Keep within pool limits
    desired_cpu = max(1.0, min(float(pool_max_cpu), float(desired_cpu)))
    desired_ram = max(0.10 * float(pool_max_ram), min(float(pool_max_ram), float(desired_ram)))

    return desired_cpu, desired_ram


def _pipeline_should_give_up(s, pipeline) -> bool:
    # Only give up if we have repeatedly failed the same operator many times.
    # (We prefer retries because an incomplete pipeline costs 720s in the objective anyway.)
    status = pipeline.runtime_status()
    failed_ops = status.get_ops([OperatorState.FAILED], require_parents_complete=False)
    for op in failed_ops:
        prof = _get_op_profile(s, op)
        if prof["failures"] >= s.max_retries:
            return True
    return False


def _pick_runnable_op_from_queue(
    s,
    queue,
    pool_id: int,
    pool_avail_cpu: float,
    pool_avail_ram: float,
    pool_max_cpu: float,
    pool_max_ram: float,
    scheduled_ops: set,
    scheduled_pipelines: set,
):
    """Rotate through a queue to find a pipeline with an assignable op that fits."""
    n = len(queue)
    for _ in range(n):
        pipeline = queue.popleft()

        # Skip pipelines we've already scheduled this tick (avoid duplicate scheduling decisions in same call)
        if pipeline.pipeline_id in scheduled_pipelines:
            queue.append(pipeline)
            continue

        status = pipeline.runtime_status()

        # Drop completed pipelines
        if status.is_pipeline_successful():
            s.pipeline_arrival.pop(pipeline.pipeline_id, None)
            continue

        # If a pipeline is hopelessly failing, stop spending cycles scanning it
        if _pipeline_should_give_up(s, pipeline):
            s.pipeline_arrival.pop(pipeline.pipeline_id, None)
            continue

        # Find a runnable operator (parents done)
        op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        if not op_list:
            # Not runnable yet; keep it in the queue
            queue.append(pipeline)
            continue

        # Choose first op that we haven't scheduled already this tick and that "fits"
        for op in op_list:
            oid = id(op)
            if oid in scheduled_ops:
                continue

            desired_cpu, desired_ram = _requested_resources(s, op, pipeline.priority, pool_max_cpu, pool_max_ram)

            # Enforce "RAM target must be met" to avoid repeated OOMs.
            if pool_avail_ram < desired_ram or pool_avail_cpu < 1.0:
                continue

            # Found a fit: requeue pipeline (it may have more work later) and return selection
            queue.append(pipeline)
            return pipeline, op, desired_cpu, desired_ram

        # No op fit; keep pipeline in queue
        queue.append(pipeline)

    return None


@register_scheduler(key="scheduler_high_030")
def scheduler_high_030(s, results: List[ExecutionResult], pipelines: List[Pipeline]) -> Tuple[List[Suspend], List[Assignment]]:
    # Advance logical scheduler time and refill batch tokens
    s.now += 1
    s.batch_tokens = min(s.max_batch_tokens, s.batch_tokens + s.batch_token_refill)

    # Enqueue newly arrived pipelines
    for p in pipelines:
        # Avoid double-enqueue (can happen in some generators)
        if p.pipeline_id not in s.pipeline_arrival:
            s.pipeline_arrival[p.pipeline_id] = s.now
            s.queues[p.priority].append(p)

    # Learn from execution results (especially failures/OOMs)
    for r in results:
        # Update every op in the result (usually one, but API supports list)
        for op in r.ops:
            prof = _get_op_profile(s, op)
            prof["last_ram"] = r.ram
            prof["last_cpu"] = r.cpu

            if r.failed():
                prof["failures"] += 1
                if _is_oom_error(r.error):
                    prof["oom_failures"] += 1
                    # Double RAM on likely OOM to drive completion rate up.
                    if r.ram is not None:
                        nxt = float(r.ram) * 2.0
                        prof["desired_ram"] = max(prof["desired_ram"] or 0.0, nxt)
                    # Keep CPU modest; OOM isn't fixed by CPU.
                    if r.cpu is not None:
                        prof["desired_cpu"] = max(prof["desired_cpu"] or 0.0, float(r.cpu))
                else:
                    # Unknown failure: cautiously increase CPU a bit; don't overreact with RAM.
                    if r.cpu is not None:
                        prof["desired_cpu"] = max(prof["desired_cpu"] or 0.0, float(r.cpu) + 1.0)
                    if r.ram is not None:
                        prof["desired_ram"] = max(prof["desired_ram"] or 0.0, float(r.ram))
            else:
                # Success: keep learned settings (do not downsize aggressively; avoids regressions into OOM)
                if r.ram is not None:
                    prof["desired_ram"] = max(prof["desired_ram"] or 0.0, float(r.ram))
                if r.cpu is not None:
                    prof["desired_cpu"] = max(prof["desired_cpu"] or 0.0, float(r.cpu))

    # Early exit if nothing changed that could affect scheduling
    if not pipelines and not results:
        return [], []

    suspensions: List[Suspend] = []
    assignments: List[Assignment] = []

    # Snapshot pool capacities (we'll decrement locally as we create assignments)
    pool_avail = {}
    pool_max = {}
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        pool_avail[pool_id] = [float(pool.avail_cpu_pool), float(pool.avail_ram_pool)]
        pool_max[pool_id] = [float(pool.max_cpu_pool), float(pool.max_ram_pool)]

    scheduled_ops = set()
    scheduled_pipelines = set()

    # Helper: schedule a single op of a given priority class onto a given pool, if possible
    def try_schedule_from_queue(priority, pool_id, enforce_batch_headroom: bool) -> bool:
        nonlocal assignments, scheduled_ops, scheduled_pipelines

        avail_cpu, avail_ram = pool_avail[pool_id]
        max_cpu, max_ram = pool_max[pool_id]
        if avail_cpu < 1.0 or avail_ram <= 0:
            return False

        # For batch, optionally keep headroom to reduce high-priority tail when new work arrives later.
        if enforce_batch_headroom and priority == Priority.BATCH_PIPELINE:
            keep_cpu = s.batch_headroom_frac_cpu * max_cpu
            keep_ram = s.batch_headroom_frac_ram * max_ram
            if avail_cpu <= keep_cpu or avail_ram <= keep_ram:
                return False

        picked = _pick_runnable_op_from_queue(
            s,
            s.queues[priority],
            pool_id=pool_id,
            pool_avail_cpu=avail_cpu,
            pool_avail_ram=avail_ram,
            pool_max_cpu=max_cpu,
            pool_max_ram=max_ram,
            scheduled_ops=scheduled_ops,
            scheduled_pipelines=scheduled_pipelines,
        )
        if not picked:
            return False

        pipeline, op, desired_cpu, desired_ram = picked

        # Finalize sizing: allocate the desired RAM (must meet target), and modest CPU.
        cpu = min(desired_cpu, pool_avail[pool_id][0])
        ram = desired_ram  # enforced fit already

        # Build assignment (single-op container)
        assignments.append(
            Assignment(
                ops=[op],
                cpu=cpu,
                ram=ram,
                priority=pipeline.priority,
                pool_id=pool_id,
                pipeline_id=pipeline.pipeline_id,
            )
        )

        # Update local pool availability
        pool_avail[pool_id][0] -= cpu
        pool_avail[pool_id][1] -= ram

        scheduled_ops.add(id(op))
        scheduled_pipelines.add(pipeline.pipeline_id)
        return True

    # Determine pool roles: if multiple pools exist, bias pool 0 toward high priority
    num_pools = s.executor.num_pools
    dedicated_hi_pool = (num_pools >= 2)

    # Mild aging: if old batch exists, allow it to "compete" as interactive occasionally
    def should_age_boost_batch(p: Pipeline) -> bool:
        at = s.pipeline_arrival.get(p.pipeline_id, s.now)
        return (s.now - at) >= s.batch_age_boost_ticks

    # Phase A: schedule high priority first (QUERY then INTERACTIVE), prefer pool 0 if dedicated
    hi_pool_order = list(range(num_pools))
    if dedicated_hi_pool and 0 in hi_pool_order:
        hi_pool_order.remove(0)
        hi_pool_order = [0] + hi_pool_order

    made_progress = True
    while made_progress:
        made_progress = False
        for pool_id in hi_pool_order:
            # If pool 0 is dedicated, do not schedule batch there unless no high-priority backlog
            while True:
                progressed = False
                progressed = progressed or try_schedule_from_queue(Priority.QUERY, pool_id, enforce_batch_headroom=False)
                progressed = progressed or try_schedule_from_queue(Priority.INTERACTIVE, pool_id, enforce_batch_headroom=False)

                # Optional: if interactive/query are empty on pool 0 and it's dedicated, let batch use it.
                if dedicated_hi_pool and pool_id == 0:
                    # Only if no runnable high-priority work exists (approx: queues empty)
                    if len(s.queues[Priority.QUERY]) == 0 and len(s.queues[Priority.INTERACTIVE]) == 0:
                        progressed = progressed or try_schedule_from_queue(Priority.BATCH_PIPELINE, pool_id, enforce_batch_headroom=True)

                if not progressed:
                    break
                made_progress = True

    # Phase B: ensure batch makes progress (but controlled) to avoid starvation/incompletion penalties.
    # Rule:
    #   - If there is high-priority backlog, allow limited batch via tokens and aging boost.
    #   - Otherwise, fill remaining capacity with batch.
    hi_backlog = (len(s.queues[Priority.QUERY]) > 0) or (len(s.queues[Priority.INTERACTIVE]) > 0)

    # First, apply aging boost: old batch pipelines can be scheduled as if interactive, but only when tokens exist.
    if hi_backlog and s.batch_tokens > 0 and len(s.queues[Priority.BATCH_PIPELINE]) > 0:
        # Scan a small number of batch entries by rotating; schedule at most one aged batch per pool in this pass.
        for pool_id in range(num_pools):
            if s.batch_tokens <= 0:
                break
            # Try to find an "aged" batch pipeline at the head by rotating a bit
            q = s.queues[Priority.BATCH_PIPELINE]
            n = len(q)
            for _ in range(min(n, 12)):
                bp = q[0]
                if should_age_boost_batch(bp):
                    # Temporarily attempt scheduling from batch queue (still counts as batch op; token-gated)
                    if try_schedule_from_queue(Priority.BATCH_PIPELINE, pool_id, enforce_batch_headroom=True):
                        s.batch_tokens -= 1
                    break
                q.rotate(-1)

    # Then schedule remaining batch:
    for pool_id in range(num_pools):
        while True:
            # If high-priority backlog exists, only schedule batch if we have tokens
            if hi_backlog:
                if s.batch_tokens <= 0:
                    break
                if not try_schedule_from_queue(Priority.BATCH_PIPELINE, pool_id, enforce_batch_headroom=True):
                    break
                s.batch_tokens -= 1
            else:
                # No high-priority backlog: fill freely with batch (still keep a bit of headroom)
                if not try_schedule_from_queue(Priority.BATCH_PIPELINE, pool_id, enforce_batch_headroom=True):
                    break

    return suspensions, assignments
