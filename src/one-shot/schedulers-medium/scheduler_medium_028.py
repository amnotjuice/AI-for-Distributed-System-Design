# policy_key: scheduler_medium_028
# reasoning_effort: medium
# model: gpt-5.2-2025-12-11
# llm_cost: 0.103655
# generation_seconds: 116.64
# generated_at: 2026-03-14T02:53:28.438826
@register_scheduler_init(key="scheduler_medium_028")
def scheduler_medium_028_init(s):
    """
    Priority-aware, small-step improvement over naive FIFO:

    1) Maintain separate FIFO queues per priority and always schedule higher priority first.
    2) Prevent batch from consuming the entire pool when higher-priority work exists by reserving
       headroom (soft reservation via a cap on batch consumption).
    3) Basic OOM-aware RAM backoff: if a container fails with an OOM-like error, increase the RAM
       multiplier for that pipeline on the next attempt; decay the multiplier after successes.
    4) Safety: schedule at most one operator per pipeline per tick to avoid duplicate assignment
       when runtime state hasn't reflected new assignments yet.
    """
    # Per-priority waiting queues (FIFO)
    s.waiting_queues = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }

    # Per-pipeline adaptive RAM multiplier (only increases on OOM-like failures)
    s.pipeline_ram_mult = {}  # pipeline_id -> float

    # Per-pipeline non-OOM failure counts (eventually give up to avoid infinite retries)
    s.pipeline_fail_count = {}  # pipeline_id -> int
    s.dead_pipelines = set()  # pipeline_id

    # Tuning knobs (keep simple and conservative)
    s.reserved_frac_for_high_pri = 0.30  # when high-priority exists, keep this fraction from batch
    s.max_assignments_per_pool_per_tick = 6

    # Baseline per-op "quanta" to reduce head-of-line blocking for high priority
    # (Batch may take larger chunks, especially when no high-priority exists.)
    s.base_quanta = {
        Priority.QUERY: {"cpu": 2, "ram": 4, "max_cpu": 4, "max_ram": 8},
        Priority.INTERACTIVE: {"cpu": 4, "ram": 8, "max_cpu": 8, "max_ram": 16},
        Priority.BATCH_PIPELINE: {"cpu": 8, "ram": 16, "max_cpu": 16, "max_ram": 32},
    }

    # OOM backoff / decay
    s.max_ram_mult = 16.0
    s.ram_mult_decay = 0.90

    # Failure handling
    s.max_non_oom_failures = 2

    # Simple tick counter (useful for future aging; currently unused for decisions)
    s.tick = 0


@register_scheduler(key="scheduler_medium_028")
def scheduler_medium_028(s, results: List["ExecutionResult"], pipelines: List["Pipeline"]) -> Tuple[List["Suspend"], List["Assignment"]]:
    """See init docstring for policy overview."""
    s.tick += 1

    def _is_oom_error(err) -> bool:
        if err is None:
            return False
        msg = str(err).lower()
        return ("oom" in msg) or ("out of memory" in msg) or ("memoryerror" in msg)

    def _infer_pipeline_id_from_result(r):
        # Best-effort: try to infer pipeline_id from the operator objects.
        # Fall back to None if the simulator doesn't attach it.
        try:
            if getattr(r, "ops", None):
                op0 = r.ops[0]
                pid = getattr(op0, "pipeline_id", None)
                if pid is not None:
                    return pid
        except Exception:
            pass
        return None

    def _cleanup_pipeline_state(pipeline_id: str):
        s.pipeline_ram_mult.pop(pipeline_id, None)
        s.pipeline_fail_count.pop(pipeline_id, None)
        s.dead_pipelines.discard(pipeline_id)

    # Enqueue new pipelines by priority
    for p in pipelines:
        s.waiting_queues[p.priority].append(p)

    # Process results to update OOM backoff and failure counters
    for r in results:
        pid = _infer_pipeline_id_from_result(r)

        if r.failed():
            if _is_oom_error(getattr(r, "error", None)):
                # Increase RAM for the next attempt of this pipeline (best effort).
                if pid is not None:
                    cur = s.pipeline_ram_mult.get(pid, 1.0)
                    s.pipeline_ram_mult[pid] = min(cur * 2.0, s.max_ram_mult)
            else:
                if pid is not None:
                    s.pipeline_fail_count[pid] = s.pipeline_fail_count.get(pid, 0) + 1
                    if s.pipeline_fail_count[pid] >= s.max_non_oom_failures:
                        s.dead_pipelines.add(pid)
        else:
            # On success, slowly decay any RAM multiplier back toward 1.0.
            if pid is not None and pid in s.pipeline_ram_mult:
                s.pipeline_ram_mult[pid] = max(1.0, s.pipeline_ram_mult[pid] * s.ram_mult_decay)
                if s.pipeline_ram_mult[pid] <= 1.01:
                    s.pipeline_ram_mult.pop(pid, None)

    # Early exit if nothing new to decide on
    if not pipelines and not results:
        return [], []

    suspensions: List["Suspend"] = []
    assignments: List["Assignment"] = []

    # Determine if there is any high-priority work waiting (soft reservation trigger).
    # Note: queues may contain blocked/completed pipelines; we handle those while popping.
    high_waiting_hint = (len(s.waiting_queues[Priority.QUERY]) > 0) or (len(s.waiting_queues[Priority.INTERACTIVE]) > 0)

    # Avoid scheduling more than one op per pipeline per tick (prevents duplicate assignment)
    scheduled_pipelines_this_tick = set()

    # Priority order: schedule query first, then interactive, then batch
    priority_order = [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]

    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool

        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # Track consumption inside this scheduler step (pool availability doesn't update until applied).
        used_cpu = 0.0
        used_ram = 0.0
        per_pool_assignments = 0

        # Compute a soft cap on how much batch is allowed to consume when high-priority exists.
        # This is a "small step" over naive: prevents batch from always taking all resources.
        batch_cpu_cap = avail_cpu
        batch_ram_cap = avail_ram
        if high_waiting_hint:
            batch_cpu_cap = max(0.0, avail_cpu - pool.max_cpu_pool * s.reserved_frac_for_high_pri)
            batch_ram_cap = max(0.0, avail_ram - pool.max_ram_pool * s.reserved_frac_for_high_pri)

        for pri in priority_order:
            if per_pool_assignments >= s.max_assignments_per_pool_per_tick:
                break

            q = s.waiting_queues[pri]
            if not q:
                continue

            # Attempt at most one full pass through the current queue to avoid infinite cycling
            # on pipelines that are blocked waiting for parents.
            attempts = 0
            max_attempts = len(q)

            while attempts < max_attempts and per_pool_assignments < s.max_assignments_per_pool_per_tick:
                attempts += 1

                # Compute remaining resources for this priority
                pool_rem_cpu = avail_cpu - used_cpu
                pool_rem_ram = avail_ram - used_ram
                if pool_rem_cpu <= 0 or pool_rem_ram <= 0:
                    break

                if pri == Priority.BATCH_PIPELINE and high_waiting_hint:
                    # Batch is additionally limited by the soft cap.
                    rem_cpu = min(pool_rem_cpu, max(0.0, batch_cpu_cap - used_cpu))
                    rem_ram = min(pool_rem_ram, max(0.0, batch_ram_cap - used_ram))
                else:
                    rem_cpu = pool_rem_cpu
                    rem_ram = pool_rem_ram

                if rem_cpu <= 0 or rem_ram <= 0:
                    break

                pipeline = q.pop(0)
                pid = pipeline.pipeline_id

                # Drop pipelines we decided to give up on
                if pid in s.dead_pipelines:
                    continue

                status = pipeline.runtime_status()

                # Remove completed pipelines from scheduling and clean state
                if status.is_pipeline_successful():
                    _cleanup_pipeline_state(pid)
                    continue

                # Avoid multiple assignments for the same pipeline in one tick
                if pid in scheduled_pipelines_this_tick:
                    q.append(pipeline)
                    continue

                # Choose one operator whose parents are complete
                op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
                if not op_list:
                    # Pipeline is currently blocked; rotate it to the back
                    q.append(pipeline)
                    continue

                # Decide CPU/RAM sizing
                quanta = s.base_quanta.get(pri, {"cpu": 1, "ram": 1, "max_cpu": 1, "max_ram": 1})

                # Batch can be greedier when there is no high-priority waiting (throughput mode)
                if pri == Priority.BATCH_PIPELINE and not high_waiting_hint:
                    target_cpu = min(rem_cpu, float(pool_rem_cpu))
                    target_ram = min(rem_ram, float(pool_rem_ram))
                else:
                    target_cpu = min(rem_cpu, float(quanta["cpu"]))
                    target_ram = min(rem_ram, float(quanta["ram"]))

                # Apply per-pipeline RAM backoff multiplier (OOM retry behavior)
                ram_mult = s.pipeline_ram_mult.get(pid, 1.0)
                target_ram = min(rem_ram, target_ram * ram_mult)

                # Cap per-op allocations so one high-priority op doesn't monopolize the pool
                # (only meaningful when not in throughput mode).
                max_cpu = float(quanta.get("max_cpu", quanta["cpu"]))
                max_ram = float(quanta.get("max_ram", quanta["ram"]))
                if not (pri == Priority.BATCH_PIPELINE and not high_waiting_hint):
                    target_cpu = min(target_cpu, max_cpu)
                    target_ram = min(target_ram, max_ram)

                # Final sanity
                if target_cpu <= 0 or target_ram <= 0:
                    # Not enough resources; put back and stop trying in this pool
                    q.insert(0, pipeline)
                    break

                # Ensure we never exceed pool max (extra safety)
                target_cpu = min(target_cpu, float(pool.max_cpu_pool))
                target_ram = min(target_ram, float(pool.max_ram_pool))

                assignments.append(
                    Assignment(
                        ops=op_list,
                        cpu=target_cpu,
                        ram=target_ram,
                        priority=pipeline.priority,
                        pool_id=pool_id,
                        pipeline_id=pid,
                    )
                )

                used_cpu += target_cpu
                used_ram += target_ram
                per_pool_assignments += 1
                scheduled_pipelines_this_tick.add(pid)

                # Round-robin: requeue pipeline for future ops
                q.append(pipeline)

    return suspensions, assignments
