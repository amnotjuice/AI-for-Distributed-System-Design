# policy_key: scheduler_est_003
# reasoning_effort: high
# model: gpt-5.2-2025-12-11
# llm_cost: 0.097124
# generation_seconds: 113.63
# generated_at: 2026-03-31T18:26:17.530517
@register_scheduler_init(key="scheduler_est_003")
def scheduler_est_003_init(s):
    """Priority-aware, estimate-guided packing scheduler.

    Improvements over naive FIFO (kept intentionally incremental and robust):
      1) Priority queues: always try QUERY, then INTERACTIVE, then BATCH.
      2) Right-size RAM using op.estimate.mem_peak_gb (+ headroom) instead of
         handing the entire pool RAM/CPU to one op.
      3) Pack multiple ops per pool per tick when resources allow.
      4) Simple OOM learning loop: on OOM failures, increase per-op RAM headroom
         and a floor based on last attempted RAM; on success, slowly decay.
      5) Basic SLO protection: when high-priority work is waiting, cap how much
         of a pool batch work may consume (soft reservation).
    """
    # Per-priority FIFO queues of pipelines
    s.queues = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }

    # Track which pipeline_ids are currently enqueued (avoid duplicates)
    s.enqueued_pipeline_ids = set()

    # Per-operator learning state (keyed by object identity; stable in-sim)
    s.op_mem_mult = {}       # id(op) -> multiplier applied to estimate
    s.op_mem_floor_gb = {}   # id(op) -> absolute floor GB (e.g., from OOM)
    s.op_attempts = {}       # id(op) -> consecutive attempts (reset on success)
    s.op_last_error = {}     # id(op) -> last seen error string (from results)

    # Tunables (kept simple)
    s.base_mem_headroom = 1.30
    s.min_ram_gb = 0.50

    s.oom_bump_mult = 1.50
    s.oom_bump_floor_factor = 1.25
    s.max_mem_mult = 8.0
    s.max_attempts = 6

    # CPU sizing by priority (avoid naive "take all CPU")
    s.cpu_target = {
        Priority.QUERY: 4.0,
        Priority.INTERACTIVE: 2.0,
        Priority.BATCH_PIPELINE: 1.0,
    }

    # When high-priority is waiting, batch can only consume up to this fraction
    s.batch_soft_cap_cpu_frac = 0.65
    s.batch_soft_cap_ram_frac = 0.65


def _is_oom_error(err) -> bool:
    if not err:
        return False
    e = str(err).lower()
    return ("oom" in e) or ("out of memory" in e) or ("cuda out of memory" in e) or ("memoryerror" in e)


def _get_op_key(op):
    # Operators are objects in the simulator; using id(op) is stable across ticks.
    return id(op)


def _op_est_mem_gb(op, default_gb=1.0):
    # Estimator field is expected by the simulator context; keep fallback robust.
    try:
        est = op.estimate.mem_peak_gb
        if est is None:
            return float(default_gb)
        return float(est)
    except Exception:
        return float(default_gb)


def _enqueue_pipeline(s, p):
    if p.pipeline_id in s.enqueued_pipeline_ids:
        return
    s.enqueued_pipeline_ids.add(p.pipeline_id)
    s.queues[p.priority].append(p)


def _drop_pipeline(s, p):
    # Remove from "enqueued" tracking; queue removal is handled by pop/rotation.
    s.enqueued_pipeline_ids.discard(p.pipeline_id)


def _pipeline_is_retryable(s, pipeline_status):
    # If there are FAILED ops, only retry when the last recorded error is OOM-like.
    failed_ops = pipeline_status.get_ops([OperatorState.FAILED], require_parents_complete=False)
    if not failed_ops:
        return True

    for op in failed_ops:
        k = _get_op_key(op)
        err = s.op_last_error.get(k)
        # If we don't know the error yet, be conservative: allow one retry.
        if err is None:
            continue
        if not _is_oom_error(err):
            return False
        # Also cap repeated attempts to avoid infinite thrash.
        if s.op_attempts.get(k, 0) >= s.max_attempts:
            return False
    return True


def _size_resources_for_op(s, op, pool, avail_cpu, avail_ram, priority):
    # CPU: priority-based fixed target (bounded by available).
    cpu_target = float(s.cpu_target.get(priority, 1.0))
    cpu = min(float(avail_cpu), cpu_target)
    if cpu < 1e-9:
        return None

    # RAM: estimated peak * multiplier, with floor learned from OOMs.
    k = _get_op_key(op)
    mult = float(s.op_mem_mult.get(k, s.base_mem_headroom))
    mult = max(s.base_mem_headroom, min(mult, s.max_mem_mult))

    est_gb = _op_est_mem_gb(op, default_gb=max(s.min_ram_gb, 1.0))
    floor_gb = float(s.op_mem_floor_gb.get(k, 0.0))

    ram = max(s.min_ram_gb, est_gb * mult, floor_gb)

    # Can't allocate more than pool max/available; clamp to max, but only accept if fits available now.
    ram = min(float(pool.max_ram_pool), ram)
    if ram > float(avail_ram) + 1e-9:
        return None

    return cpu, ram


def _try_pick_fitting_from_queue(s, q, pool, avail_cpu, avail_ram, priority, allow_cpu, allow_ram):
    """Rotate through the queue once to find the first pipeline with a ready op that fits."""
    if not q:
        return None

    scan_n = len(q)
    for _ in range(scan_n):
        pipeline = q.pop(0)  # rotate
        status = pipeline.runtime_status()

        # Drop completed pipelines
        if status.is_pipeline_successful():
            _drop_pipeline(s, pipeline)
            continue

        # Drop pipelines with non-OOM failures (or too many attempts)
        if not _pipeline_is_retryable(s, status):
            _drop_pipeline(s, pipeline)
            continue

        # Only schedule ops whose parents are complete
        op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
        if not op_list:
            # Nothing ready; keep pipeline in queue for later.
            q.append(pipeline)
            continue

        op = op_list[0]

        # Apply batch soft caps (when high-priority is waiting)
        eff_avail_cpu = min(float(avail_cpu), float(allow_cpu))
        eff_avail_ram = min(float(avail_ram), float(allow_ram))
        sized = _size_resources_for_op(s, op, pool, eff_avail_cpu, eff_avail_ram, priority)
        if sized is None:
            # Doesn't fit; keep pipeline in queue and try next.
            q.append(pipeline)
            continue

        cpu, ram = sized

        # Requeue pipeline (round-robin across pipelines)
        q.append(pipeline)
        return pipeline, op, cpu, ram

    return None


@register_scheduler(key="scheduler_est_003")
def scheduler_est_003(s, results, pipelines):
    """
    Main scheduling loop.

    Returns:
        (suspensions, assignments)
    """
    # Enqueue arriving pipelines
    for p in pipelines:
        _enqueue_pipeline(s, p)

    # Learn from execution results (primarily OOM adaptation)
    for r in results:
        for op in getattr(r, "ops", []) or []:
            k = _get_op_key(op)
            s.op_last_error[k] = getattr(r, "error", None)

            if r.failed():
                s.op_attempts[k] = s.op_attempts.get(k, 0) + 1

                if _is_oom_error(getattr(r, "error", None)):
                    # Increase multiplier and set a RAM floor based on what we just tried.
                    prev_mult = float(s.op_mem_mult.get(k, s.base_mem_headroom))
                    s.op_mem_mult[k] = min(prev_mult * s.oom_bump_mult, s.max_mem_mult)

                    tried_ram = float(getattr(r, "ram", 0.0) or 0.0)
                    if tried_ram > 0:
                        prev_floor = float(s.op_mem_floor_gb.get(k, 0.0))
                        s.op_mem_floor_gb[k] = max(prev_floor, tried_ram * s.oom_bump_floor_factor)
            else:
                # Success: reset attempts and slowly decay multiplier toward base headroom.
                s.op_attempts[k] = 0
                if k in s.op_mem_mult:
                    s.op_mem_mult[k] = max(s.base_mem_headroom, float(s.op_mem_mult[k]) * 0.95)

    # Early exit if nothing to do
    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    # High-priority waiting?
    high_waiting = bool(s.queues[Priority.QUERY] or s.queues[Priority.INTERACTIVE])

    # Order pools by available resources to help place large/high-priority work first.
    pool_ids = list(range(s.executor.num_pools))
    pool_ids.sort(
        key=lambda i: (
            float(s.executor.pools[i].avail_cpu_pool),
            float(s.executor.pools[i].avail_ram_pool),
        ),
        reverse=True,
    )

    for pool_id in pool_ids:
        pool = s.executor.pools[pool_id]
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)

        if avail_cpu <= 1e-9 or avail_ram <= 1e-9:
            continue

        # Soft caps for batch when high-priority is waiting (reserve headroom).
        if high_waiting:
            batch_allow_cpu = max(0.0, min(avail_cpu, float(pool.max_cpu_pool) * s.batch_soft_cap_cpu_frac))
            batch_allow_ram = max(0.0, min(avail_ram, float(pool.max_ram_pool) * s.batch_soft_cap_ram_frac))
        else:
            batch_allow_cpu = avail_cpu
            batch_allow_ram = avail_ram

        # Keep packing until we can't fit anything else.
        # Also guard against infinite loops with a conservative iteration cap.
        pack_iter_cap = 64
        iters = 0

        while iters < pack_iter_cap:
            iters += 1

            if avail_cpu <= 1e-9 or avail_ram <= 1e-9:
                break

            picked = None

            # 1) QUERY first (no soft caps)
            picked = _try_pick_fitting_from_queue(
                s,
                s.queues[Priority.QUERY],
                pool,
                avail_cpu,
                avail_ram,
                Priority.QUERY,
                allow_cpu=avail_cpu,
                allow_ram=avail_ram,
            )

            # 2) INTERACTIVE next (no soft caps)
            if picked is None:
                picked = _try_pick_fitting_from_queue(
                    s,
                    s.queues[Priority.INTERACTIVE],
                    pool,
                    avail_cpu,
                    avail_ram,
                    Priority.INTERACTIVE,
                    allow_cpu=avail_cpu,
                    allow_ram=avail_ram,
                )

            # 3) BATCH last (apply soft caps if high-priority waiting)
            if picked is None:
                picked = _try_pick_fitting_from_queue(
                    s,
                    s.queues[Priority.BATCH_PIPELINE],
                    pool,
                    avail_cpu,
                    avail_ram,
                    Priority.BATCH_PIPELINE,
                    allow_cpu=batch_allow_cpu,
                    allow_ram=batch_allow_ram,
                )

            if picked is None:
                break

            pipeline, op, cpu, ram = picked

            # Create assignment for a single op; keep it atomic and simple.
            assignment = Assignment(
                ops=[op],
                cpu=cpu,
                ram=ram,
                priority=pipeline.priority,
                pool_id=pool_id,
                pipeline_id=pipeline.pipeline_id,
            )
            assignments.append(assignment)

            # Update local available resources for additional packing.
            avail_cpu -= float(cpu)
            avail_ram -= float(ram)

            # If high-priority was waiting, batch caps should track remaining headroom
            if high_waiting:
                batch_allow_cpu = min(batch_allow_cpu, avail_cpu)
                batch_allow_ram = min(batch_allow_ram, avail_ram)

    return suspensions, assignments
