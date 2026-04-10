# policy_key: scheduler_est_050
# reasoning_effort: high
# model: gpt-5.2-2025-12-11
# llm_cost: 0.150086
# generation_seconds: 94.84
# generated_at: 2026-03-31T19:56:43.086958
@register_scheduler_init(key="scheduler_est_050")
def scheduler_est_050_init(s):
    """Priority-aware, estimate-driven scheduler (incremental improvement over naive FIFO).

    Core ideas:
      1) Priority queues: always try QUERY > INTERACTIVE > BATCH.
      2) Memory sizing from op.estimate.mem_peak_gb with a small safety buffer.
      3) OOM-aware retries: if a container fails with an OOM-like error, retry the pipeline's failed op
         with a larger per-pipeline memory scale (bounded #retries).
      4) Latency protection without preemption: cap batch resource grabs and keep a small reserve
         of CPU/RAM in each pool so high-priority arrivals can start sooner.

    Notes:
      - Keeps code conservative to avoid relying on undocumented executor internals (no preemption).
      - Uses per-pipeline tracking because ExecutionResult does not expose pipeline_id directly.
    """
    # Waiting queues per priority
    s.q_query = []
    s.q_interactive = []
    s.q_batch = []

    # Pipeline registry (pipeline_id -> Pipeline object) so we can requeue on failures
    s.pipeline_by_id = {}

    # Track which pipeline a given op belongs to (id(op) -> pipeline_id)
    s.op_to_pipeline = {}

    # Per-pipeline memory scaling factor and OOM retry counts
    s.pipeline_mem_scale = {}   # pipeline_id -> float
    s.pipeline_oom_retries = {} # pipeline_id -> int

    # Pipelines that encountered non-retryable failures (drop from queues)
    s.dropped_pipelines = set()

    # Simple anti-starvation knob: after scheduling many high-priority ops, allow a batch op.
    s.hp_streak = 0
    s.batch_every = 8  # allow at least one batch placement after this many high-priority placements

    # Retry policy
    s.max_oom_retries = 3
    s.oom_scale_mult = 1.6
    s.oom_scale_cap = 8.0


@register_scheduler(key="scheduler_est_050")
def scheduler_est_050_scheduler(s, results, pipelines):
    """
    Scheduler tick:
      - Ingest new pipelines into priority queues.
      - Learn from completed/failed results (OOM -> increase memory scale & retry).
      - For each pool, place as many ready ops as possible using priority order, with
        batch reservations to protect latency for high-priority work.
    """
    def _enqueue(p, front=False):
        if p is None:
            return
        s.pipeline_by_id[p.pipeline_id] = p
        if p.pipeline_id in s.dropped_pipelines:
            return

        if p.priority == Priority.QUERY:
            (s.q_query.insert(0, p) if front else s.q_query.append(p))
        elif p.priority == Priority.INTERACTIVE:
            (s.q_interactive.insert(0, p) if front else s.q_interactive.append(p))
        else:
            (s.q_batch.insert(0, p) if front else s.q_batch.append(p))

    def _is_oom_error(err):
        if err is None:
            return False
        msg = str(err).lower()
        # Keep heuristic broad; simulator/error strings may vary.
        return ("oom" in msg) or ("out of memory" in msg) or ("memoryerror" in msg) or ("cuda out of memory" in msg)

    def _get_ready_op(pipeline):
        status = pipeline.runtime_status()
        if status.is_pipeline_successful():
            return None

        # If pipeline is marked dropped, never retry
        if pipeline.pipeline_id in s.dropped_pipelines:
            return None

        # Choose a single next assignable op whose parents are complete
        op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
        if not op_list:
            return None
        return op_list[0]

    def _mem_est_gb(op, pool):
        # Estimate-driven RAM sizing; fall back to a conservative fraction of pool RAM.
        est_obj = getattr(op, "estimate", None)
        if est_obj is not None and hasattr(est_obj, "mem_peak_gb"):
            try:
                v = float(est_obj.mem_peak_gb)
                # Guard against pathological values
                if v > 0:
                    return v
            except Exception:
                pass
        # Unknown: assume moderately sized op to reduce OOM risk.
        return max(0.5, 0.25 * float(pool.max_ram_pool))

    def _cpu_target(priority, pool):
        max_cpu = float(pool.max_cpu_pool)
        if priority == Priority.QUERY:
            frac, cap = 0.70, 8.0
        elif priority == Priority.INTERACTIVE:
            frac, cap = 0.55, 6.0
        else:
            frac, cap = 0.35, 4.0
        return max(1.0, min(cap, frac * max_cpu))

    def _ram_target(priority, pid, op, pool):
        base = _mem_est_gb(op, pool)
        scale = float(s.pipeline_mem_scale.get(pid, 1.0))
        # Safety buffer: small additive + multiplicative
        mult = 1.20 if priority in (Priority.QUERY, Priority.INTERACTIVE) else 1.15
        add = 0.25
        target = base * scale * mult + add
        # Bound by pool RAM
        return max(0.5, min(float(pool.max_ram_pool), target))

    def _min_ram_required(pid, op, pool):
        # Minimum safe RAM to even attempt: estimate * scale (with tiny buffer).
        base = _mem_est_gb(op, pool)
        scale = float(s.pipeline_mem_scale.get(pid, 1.0))
        return max(0.5, min(float(pool.max_ram_pool), base * scale * 1.02))

    # Enqueue new pipelines
    for p in pipelines:
        _enqueue(p, front=False)

    # If no state changes, we can exit early
    if not pipelines and not results:
        return [], []

    # Learn from execution results (especially OOM)
    for r in results:
        if not r.failed():
            continue

        # Map failure back to pipeline_id via the op object(s) we assigned
        pid = None
        try:
            if getattr(r, "ops", None):
                for op in r.ops:
                    pid = s.op_to_pipeline.get(id(op))
                    if pid is not None:
                        break
        except Exception:
            pid = None

        if pid is None:
            # Can't attribute; safest is to do nothing special.
            continue

        if pid in s.dropped_pipelines:
            continue

        if _is_oom_error(getattr(r, "error", None)):
            # Increase memory scale and retry (bounded)
            cur_retries = int(s.pipeline_oom_retries.get(pid, 0)) + 1
            s.pipeline_oom_retries[pid] = cur_retries

            if cur_retries > int(s.max_oom_retries):
                s.dropped_pipelines.add(pid)
                continue

            cur_scale = float(s.pipeline_mem_scale.get(pid, 1.0))
            new_scale = min(float(s.oom_scale_cap), cur_scale * float(s.oom_scale_mult))
            s.pipeline_mem_scale[pid] = new_scale

            # Requeue pipeline at front to reduce latency for interactive retries
            _enqueue(s.pipeline_by_id.get(pid), front=True)
        else:
            # Non-OOM failure: treat as non-retryable (avoids infinite retries)
            s.dropped_pipelines.add(pid)

    suspensions = []
    assignments = []

    # Avoid scheduling multiple ops from the same pipeline in one tick (reduces burstiness)
    scheduled_pipelines = set()

    # Choose pool iteration order: schedule on pools with more headroom first
    pool_order = list(range(s.executor.num_pools))
    def _pool_headroom_key(pool_id):
        pool = s.executor.pools[pool_id]
        # Weight CPU a bit more (often the bottleneck for latency)
        return (float(pool.avail_cpu_pool) * 1000.0) + float(pool.avail_ram_pool)
    pool_order.sort(key=_pool_headroom_key, reverse=True)

    # Place as many ops as possible across pools
    for pool_id in pool_order:
        pool = s.executor.pools[pool_id]
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)

        if avail_cpu < 1.0 or avail_ram <= 0.0:
            continue

        # Per-pool latency reserve: keep some headroom by limiting how much batch can consume.
        reserve_cpu = max(1.0, 0.15 * float(pool.max_cpu_pool))
        reserve_ram = max(0.5, 0.15 * float(pool.max_ram_pool))

        # Helper to attempt scheduling from a given queue list
        def _try_from_queue(q, priority, allow_use_reserve):
            nonlocal avail_cpu, avail_ram

            if avail_cpu < 1.0 or avail_ram <= 0.0:
                return False

            # Scan the queue once to find something runnable that fits.
            n = len(q)
            if n <= 0:
                return False

            for _ in range(n):
                pipeline = q.pop(0)

                if pipeline is None:
                    continue

                pid = pipeline.pipeline_id
                if pid in s.dropped_pipelines:
                    continue

                if pid in scheduled_pipelines:
                    # Already scheduled an op for this pipeline this tick; defer.
                    q.append(pipeline)
                    continue

                status = pipeline.runtime_status()
                if status.is_pipeline_successful():
                    continue

                op = _get_ready_op(pipeline)
                if op is None:
                    # Not ready now; keep it in queue
                    q.append(pipeline)
                    continue

                # Determine resource caps for this priority
                cpu_tgt = _cpu_target(priority, pool)
                ram_tgt = _ram_target(priority, pid, op, pool)
                min_ram = _min_ram_required(pid, op, pool)

                # Enforce batch reserve unless explicitly allowed
                if priority == Priority.BATCH_PIPELINE and not allow_use_reserve:
                    eff_cpu = max(0.0, avail_cpu - reserve_cpu)
                    eff_ram = max(0.0, avail_ram - reserve_ram)
                else:
                    eff_cpu = avail_cpu
                    eff_ram = avail_ram

                if eff_cpu < 1.0 or eff_ram < min_ram:
                    # Can't fit here; put back and keep scanning for smaller tasks
                    q.append(pipeline)
                    continue

                cpu = min(eff_cpu, cpu_tgt)
                cpu = max(1.0, cpu)

                ram = min(eff_ram, ram_tgt)
                # Ensure we honor minimum safe RAM
                if ram < min_ram:
                    # Try giving it exactly min_ram if possible
                    if eff_ram >= min_ram:
                        ram = min_ram
                    else:
                        q.append(pipeline)
                        continue

                # Create the assignment (single op)
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

                # Track mapping to attribute future failures back to pipeline
                s.op_to_pipeline[id(op)] = pid

                # Update headroom for further packing in this pool
                avail_cpu -= cpu
                avail_ram -= ram

                # Mark pipeline as scheduled this tick and keep it for future ops
                scheduled_pipelines.add(pid)
                q.append(pipeline)

                # Update anti-starvation streak
                if priority in (Priority.QUERY, Priority.INTERACTIVE):
                    s.hp_streak += 1
                else:
                    s.hp_streak = 0

                return True

            return False

        # Fill this pool with multiple placements (bounded by resources and queue content)
        # We attempt to schedule one op at a time, re-evaluating after each placement.
        while avail_cpu >= 1.0 and avail_ram > 0.0:
            placed = False

            # Occasionally force a batch placement attempt (if any), to avoid starvation.
            force_batch = (s.hp_streak >= s.batch_every)

            if force_batch:
                placed = _try_from_queue(s.q_batch, Priority.BATCH_PIPELINE, allow_use_reserve=False)
                if placed:
                    # We scheduled a batch op; reset streak so we don't over-favor batch.
                    s.hp_streak = 0
                    continue

            # Normal priority order: QUERY -> INTERACTIVE -> BATCH
            placed = _try_from_queue(s.q_query, Priority.QUERY, allow_use_reserve=True)
            if placed:
                continue

            placed = _try_from_queue(s.q_interactive, Priority.INTERACTIVE, allow_use_reserve=True)
            if placed:
                continue

            # For batch, honor reserve to protect latency; if nothing else is runnable,
            # allow batch to use full headroom (prevents under-utilization when system is idle).
            placed = _try_from_queue(s.q_batch, Priority.BATCH_PIPELINE, allow_use_reserve=False)
            if placed:
                continue

            # If we couldn't place anything with reserves, try letting batch use reserves too.
            placed = _try_from_queue(s.q_batch, Priority.BATCH_PIPELINE, allow_use_reserve=True)
            if placed:
                continue

            # No schedulable work fits in this pool right now.
            break

    return suspensions, assignments
