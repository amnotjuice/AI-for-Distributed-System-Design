# policy_key: scheduler_high_020
# reasoning_effort: high
# exp: reasoning
# model: gpt-5.2-2025-12-11
# llm_cost: 0.147627
# generation_seconds: 164.03
# generated_at: 2026-04-10T01:15:30.348707
@register_scheduler_init(key="scheduler_high_020")
def scheduler_high_020_init(s):
    """Priority-protecting, OOM-aware, low-churn scheduler.

    Core ideas:
      - Strict priority order for new work: QUERY > INTERACTIVE > BATCH.
      - Leave headroom by throttling *new* BATCH admissions when higher-priority work exists.
      - Avoid cascading OOMs via per-operator RAM floors that ramp up on OOM failures.
      - Reduce contention by limiting to at most one in-flight op per pipeline (no intra-pipeline parallelism).
      - Fairness within each priority via round-robin (requeue pipelines each tick unless completed).
    """
    # Per-priority round-robin queues of pipelines.
    s.queues = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }

    # Track seen pipelines to avoid double-enqueue on repeated arrivals (if that happens).
    s._seen_pipeline_ids = set()

    # Per-operator (by object id) RAM floor hints; increased on OOM.
    s._op_ram_floor = {}  # op_key -> float

    # Per-operator failure counters and "dead" marker to avoid infinite futile retries.
    s._op_failures = {}   # op_key -> int
    s._op_dead = set()    # op_key

    # Soft caps for retries (kept conservative to avoid wasting cluster time).
    s._max_oom_retries = 6
    s._max_nonoom_retries = 1


def _is_oom_error(err):
    if not err:
        return False
    e = str(err).lower()
    return ("oom" in e) or ("out of memory" in e) or ("killed" in e and "memory" in e) or ("memoryerror" in e)


def _base_frac(priority):
    # Conservative initial RAM sizing to reduce first-try OOMs (failures are very costly),
    # but still allow multiple concurrent ops in a pool.
    if priority == Priority.QUERY:
        return 0.35, 0.60  # (cpu_frac, ram_frac)
    if priority == Priority.INTERACTIVE:
        return 0.25, 0.45
    return 0.15, 0.30  # batch


def _reserve_frac(highest_waiting_priority):
    # Reserve headroom by limiting *new* batch work when higher priority is waiting.
    # (No preemption here, so reservations only help if we keep some capacity free.)
    if highest_waiting_priority == Priority.QUERY:
        return 0.25, 0.25  # (cpu_reserve_frac, ram_reserve_frac)
    if highest_waiting_priority == Priority.INTERACTIVE:
        return 0.15, 0.15
    return 0.0, 0.0


def _priority_rank(p):
    if p == Priority.QUERY:
        return 3
    if p == Priority.INTERACTIVE:
        return 2
    return 1


def _pick_pool_for_op(priority, op_key, s, pool_avail_cpu, pool_avail_ram, allow_use_reserved):
    # Choose a pool and concrete (cpu, ram) for an op under current virtual availability.
    # allow_use_reserved=False means apply reservation (used for batch when high-priority waiting).
    cpu_frac, ram_frac = _base_frac(priority)

    candidates = []
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool_avail_cpu[pool_id]
        avail_ram = pool_avail_ram[pool_id]
        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # Desired resources relative to the pool size, but never below 1 unit.
        desired_cpu = max(1.0, cpu_frac * float(pool.max_cpu_pool))
        desired_ram = max(1.0, ram_frac * float(pool.max_ram_pool))

        # Apply learned per-op RAM floor (absolute units).
        if op_key in s._op_ram_floor:
            desired_ram = max(desired_ram, float(s._op_ram_floor[op_key]))

        # If we are not allowed to consume reserved headroom, clamp effective availability.
        if not allow_use_reserved:
            # Find what we're reserving given current waiting high priority.
            # The caller already decided "highest waiting"; we store it temporarily on s for simplicity.
            r_cpu, r_ram = _reserve_frac(getattr(s, "_highest_waiting_prio", None))
            eff_cpu = max(0.0, avail_cpu - r_cpu * float(pool.max_cpu_pool))
            eff_ram = max(0.0, avail_ram - r_ram * float(pool.max_ram_pool))
        else:
            eff_cpu, eff_ram = avail_cpu, avail_ram

        # Must fit RAM floor; CPU can be smaller than desired (slower but feasible).
        if eff_ram < desired_ram or eff_cpu <= 0:
            continue

        cpu = min(eff_cpu, desired_cpu)
        ram = desired_ram  # don't undershoot the floor; that's how we avoid repeat OOM.

        if cpu <= 0 or ram <= 0 or eff_cpu < cpu or eff_ram < ram:
            continue

        # Scoring:
        #   - For query/interactive: prefer more CPU headroom for lower latency.
        #   - For batch: prefer best-fit RAM to pack without blocking high prio.
        if priority in (Priority.QUERY, Priority.INTERACTIVE):
            score = (eff_cpu / max(1.0, float(pool.max_cpu_pool))) * 10.0 + (eff_ram / max(1.0, float(pool.max_ram_pool)))
        else:
            leftover_ram = eff_ram - ram
            score = -leftover_ram  # pack RAM tightly
        candidates.append((score, pool_id, cpu, ram))

    if not candidates:
        return None
    candidates.sort(reverse=True, key=lambda x: x[0])
    _, pool_id, cpu, ram = candidates[0]
    return pool_id, cpu, ram


@register_scheduler(key="scheduler_high_020")
def scheduler_high_020(s, results: List[ExecutionResult], pipelines: List[Pipeline]) -> Tuple[List[Suspend], List[Assignment]]:
    """
    Scheduler step:
      - Enqueue new pipelines by priority.
      - Learn from failures (especially OOM): bump per-op RAM floors.
      - Assign ready ops, respecting strict priority and batch throttling when high-priority waits.
    """
    # Add new pipelines to round-robin queues.
    for p in pipelines:
        if p.pipeline_id in s._seen_pipeline_ids:
            continue
        s._seen_pipeline_ids.add(p.pipeline_id)
        s.queues[p.priority].append(p)

    # Fast exit if no new information.
    if not pipelines and not results:
        return [], []

    # Update RAM floors from failures.
    for r in results:
        if not r.failed():
            continue

        oom = _is_oom_error(getattr(r, "error", None))
        for op in getattr(r, "ops", []) or []:
            op_key = id(op)
            s._op_failures[op_key] = s._op_failures.get(op_key, 0) + 1

            if oom:
                # Ramp RAM floor aggressively to converge quickly and avoid repeated OOM churn.
                prev = float(s._op_ram_floor.get(op_key, 0.0))
                # If r.ram is what we requested, use it as a baseline; else fall back to prev.
                baseline = float(getattr(r, "ram", 0.0) or prev or 1.0)
                new_floor = max(prev, baseline * 1.7 + 1.0)
                s._op_ram_floor[op_key] = new_floor

                if s._op_failures[op_key] > s._max_oom_retries:
                    s._op_dead.add(op_key)
            else:
                # Non-OOM errors are less likely to be fixed by resizing; avoid wasting resources.
                if s._op_failures[op_key] > s._max_nonoom_retries:
                    s._op_dead.add(op_key)

    suspensions = []  # No preemption in this version (keeps churn low and behavior predictable).
    assignments = []

    # Snapshot virtual pool availability (we decrement as we assign).
    pool_avail_cpu = []
    pool_avail_ram = []
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        pool_avail_cpu.append(float(pool.avail_cpu_pool))
        pool_avail_ram.append(float(pool.avail_ram_pool))

    # Determine highest waiting priority (approximate) to decide batch throttling.
    # We set on s for helper access; this is only used for reserving headroom for batch.
    highest_waiting = None
    if s.queues[Priority.QUERY]:
        highest_waiting = Priority.QUERY
    elif s.queues[Priority.INTERACTIVE]:
        highest_waiting = Priority.INTERACTIVE
    elif s.queues[Priority.BATCH_PIPELINE]:
        highest_waiting = Priority.BATCH_PIPELINE
    s._highest_waiting_prio = highest_waiting

    # Per-tick: avoid multiple concurrent ops from the same pipeline.
    scheduled_pipelines = set()

    # Try to schedule in strict priority order.
    for prio in (Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE):
        q = s.queues[prio]
        if not q:
            continue

        # Batch should not consume reserved headroom when higher priority is waiting.
        allow_use_reserved = True
        if prio == Priority.BATCH_PIPELINE and highest_waiting in (Priority.QUERY, Priority.INTERACTIVE):
            allow_use_reserved = False

        # Round-robin: only iterate through current queue length once.
        n = len(q)
        for _ in range(n):
            p = q.pop(0)

            # Drop completed pipelines from queues.
            status = p.runtime_status()
            if status.is_pipeline_successful():
                continue

            # Don't start a new op if pipeline already has work in-flight (limits memory pressure / failures).
            in_flight = status.state_counts.get(OperatorState.RUNNING, 0) + status.state_counts.get(OperatorState.ASSIGNED, 0)
            if in_flight > 0 or p.pipeline_id in scheduled_pipelines:
                q.append(p)
                continue

            # Find one ready-to-assign operator whose parents are complete.
            op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
            if not op_list:
                q.append(p)
                continue

            op = op_list[0]
            op_key = id(op)
            if op_key in s._op_dead:
                # Keep pipeline in queue so we don't "drop" it; but we won't burn resources on a hopeless op.
                q.append(p)
                continue

            picked = _pick_pool_for_op(prio, op_key, s, pool_avail_cpu, pool_avail_ram, allow_use_reserved=allow_use_reserved)
            if picked is None:
                # Can't place now; try later.
                q.append(p)
                continue

            pool_id, cpu, ram = picked

            # Create assignment.
            assignment = Assignment(
                ops=op_list,
                cpu=cpu,
                ram=ram,
                priority=p.priority,
                pool_id=pool_id,
                pipeline_id=p.pipeline_id
            )
            assignments.append(assignment)

            # Update virtual availability.
            pool_avail_cpu[pool_id] -= float(cpu)
            pool_avail_ram[pool_id] -= float(ram)

            scheduled_pipelines.add(p.pipeline_id)

            # Requeue pipeline for subsequent ops after this one completes.
            q.append(p)

    return suspensions, assignments
