# policy_key: scheduler_est_048
# reasoning_effort: high
# model: gpt-5.2-2025-12-11
# llm_cost: 0.146600
# generation_seconds: 97.79
# generated_at: 2026-03-31T19:52:43.114408
@register_scheduler_init(key="scheduler_est_048")
def scheduler_est_048_init(s):
    """Priority-aware, estimate-guided scheduler with conservative headroom.

    Small, obvious improvements over naive FIFO:
      1) Separate per-priority queues (QUERY > INTERACTIVE > BATCH) to reduce tail latency.
      2) Size RAM using op.estimate.mem_peak_gb with a small safety multiplier to reduce OOMs.
      3) On OOM-like failures, retry with increased RAM (bounded retries) instead of dropping the pipeline.
      4) Keep CPU caps per op (bigger for high-priority) so a single batch op doesn't monopolize a pool.
      5) When high-priority backlog exists, keep a small CPU/RAM reserve to admit new high-priority work quickly.

    Notes:
      - This policy intentionally avoids preemption because the minimal public API here does not
        expose a reliable view of currently running containers to choose safe victims.
      - It schedules multiple operators per pool per tick when resources allow.
    """
    # Per-priority FIFO queues of pipeline_ids (round-robin within each priority by rotation).
    s.queues = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }
    # pipeline_id -> Pipeline (so we can dereference ids in queues)
    s.pipeline_map = {}

    # Operators are retried on OOM-like failures with increased memory.
    # Keyed by python object identity to avoid depending on simulator-specific operator ids.
    s.op_mem_mult = {}        # op_key -> multiplier
    s.op_retry_count = {}     # op_key -> retries so far

    # Pipelines marked dead are skipped and eventually fall out of the queues.
    s.dead_pipelines = set()

    # Tuning knobs (keep conservative / simple initially).
    s.default_mem_mult = 1.20
    s.oom_mem_backoff = 1.50
    s.max_mem_mult = 6.00
    s.max_retries_per_op = 3

    # Resource floors (avoid scheduling tiny allocations that unrealistically "fit").
    s.min_cpu = 1.0
    s.min_ram_gb = 0.25

    # When any high-priority backlog exists, keep this fraction of each pool free
    # before scheduling BATCH, to reduce admission latency for new interactive/query work.
    s.hp_reserve_frac_cpu = 0.15
    s.hp_reserve_frac_ram = 0.15


def _op_key(op):
    """Stable key for per-operator retry/memory state.

    Prefer explicit ids when present; fall back to python object identity.
    """
    for attr in ("op_id", "operator_id", "task_id", "node_id", "id"):
        if hasattr(op, attr):
            val = getattr(op, attr)
            # Avoid methods or non-primitive ids.
            if isinstance(val, (int, str)):
                return (attr, val)
    return ("pyid", id(op))


def _is_oom_error(err):
    if err is None:
        return False
    msg = str(err).lower()
    # Keep matching broad; simulator implementations differ.
    return ("oom" in msg) or ("out of memory" in msg) or ("memory" in msg and "exceed" in msg)


def _all_operator_states():
    return [
        OperatorState.PENDING,
        OperatorState.ASSIGNED,
        OperatorState.RUNNING,
        OperatorState.SUSPENDING,
        OperatorState.COMPLETED,
        OperatorState.FAILED,
    ]


def _pipeline_has_any_retryable_work(pipeline):
    """True if there exists any assignable op whose parents are complete."""
    status = pipeline.runtime_status()
    if status.is_pipeline_successful():
        return False
    ops = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
    return bool(ops)


def _estimate_op_ram_gb(s, op):
    """RAM sizing using estimator + safety multiplier + retry backoff."""
    est = None
    if hasattr(op, "estimate") and op.estimate is not None and hasattr(op.estimate, "mem_peak_gb"):
        est = op.estimate.mem_peak_gb

    # If estimator missing, choose a conservative small default rather than consuming the whole pool.
    base = float(est) if isinstance(est, (int, float)) and est > 0 else 1.0

    mult = s.op_mem_mult.get(_op_key(op), s.default_mem_mult)
    return max(s.min_ram_gb, base * mult)


def _cpu_cap_for_priority(pool, priority):
    """Per-op CPU caps: higher for interactive/query to reduce latency."""
    max_cpu = float(pool.max_cpu_pool)

    if priority == Priority.QUERY:
        return max(2.0, max_cpu * 0.75)
    if priority == Priority.INTERACTIVE:
        return max(2.0, max_cpu * 0.60)
    # Batch: encourage concurrency and keep headroom
    return max(1.0, max_cpu * 0.40)


def _pick_fittable_op_from_queue(s, pool, queue, eff_avail_cpu, eff_avail_ram):
    """Round-robin scan for the first pipeline with an op that fits current resources.

    Returns: (pipeline_id, pipeline, op) or None
    """
    n = len(queue)
    if n == 0:
        return None

    for _ in range(n):
        pipeline_id = queue.pop(0)
        pipeline = s.pipeline_map.get(pipeline_id)

        # Pipeline may have been garbage-collected/removed; skip.
        if pipeline is None:
            continue

        # Skip dead pipelines.
        if pipeline_id in s.dead_pipelines:
            continue

        status = pipeline.runtime_status()

        # Drop completed pipelines from queues.
        if status.is_pipeline_successful():
            continue

        # Find one assignable op (parents completed).
        op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
        if not op_list:
            # Not currently schedulable; keep it in rotation.
            queue.append(pipeline_id)
            continue

        op = op_list[0]

        # Check fit.
        need_ram = _estimate_op_ram_gb(s, op)
        need_cpu = s.min_cpu

        # If it can't meet min resources, don't schedule it now.
        if need_cpu <= eff_avail_cpu and need_ram <= eff_avail_ram:
            # Keep pipeline in rotation for future ops.
            queue.append(pipeline_id)
            return (pipeline_id, pipeline, op)

        # Not fittable right now; keep in queue for later.
        queue.append(pipeline_id)

    return None


@register_scheduler(key="scheduler_est_048")
def scheduler_est_048(s, results, pipelines):
    """
    Priority-aware scheduling with estimate-guided RAM sizing and OOM retries.

    Scheduling flow:
      - Enqueue arriving pipelines by priority
      - Process results to adjust per-op RAM multipliers on OOM failures
      - For each pool, schedule as many ops as fit, always prioritizing QUERY > INTERACTIVE > BATCH,
        while keeping a small reserve for high-priority work when there is backlog.
    """
    # Enqueue newly arrived pipelines (avoid duplicate insertion).
    for p in pipelines:
        pid = p.pipeline_id
        if pid in s.dead_pipelines:
            continue
        if pid not in s.pipeline_map:
            s.pipeline_map[pid] = p
            # Unknown priorities fall back to batch-like behavior.
            pr = p.priority if p.priority in s.queues else Priority.BATCH_PIPELINE
            s.queues[pr].append(pid)

    # Process execution results from last tick.
    # - Retry OOM-like failures with increased RAM.
    # - Non-OOM failures are retried a limited number of times; after that, mark pipeline dead.
    if results:
        # Build a cheap cache of pipeline -> operator membership only if we need to mark pipelines dead.
        # (We avoid doing this on every tick.)
        for r in results:
            if not r.failed():
                continue

            is_oom = _is_oom_error(getattr(r, "error", None))
            ops = getattr(r, "ops", []) or []

            for op in ops:
                k = _op_key(op)
                s.op_retry_count[k] = s.op_retry_count.get(k, 0) + 1

                if is_oom:
                    old = s.op_mem_mult.get(k, s.default_mem_mult)
                    s.op_mem_mult[k] = min(s.max_mem_mult, max(old, s.default_mem_mult) * s.oom_mem_backoff)

                # If we've retried too many times, mark the owning pipeline as dead.
                if s.op_retry_count[k] >= s.max_retries_per_op:
                    # Best-effort: find the pipeline containing this op.
                    # This is O(#pipelines) but should only happen on repeated failures.
                    for pid, p in list(s.pipeline_map.items()):
                        if pid in s.dead_pipelines:
                            continue
                        st = p.runtime_status()
                        if st.is_pipeline_successful():
                            continue
                        try:
                            # Search all states for identity match.
                            all_ops = st.get_ops(_all_operator_states(), require_parents_complete=False)
                            if op in all_ops:
                                s.dead_pipelines.add(pid)
                                break
                        except Exception:
                            # If simulator rejects this query shape, fall back to not killing the pipeline.
                            pass

    # No preemption in this version (API surface is insufficient to pick victims safely).
    suspensions = []
    assignments = []

    # Helper: is there any high-priority backlog right now?
    def has_hp_backlog():
        return bool(s.queues.get(Priority.QUERY)) or bool(s.queues.get(Priority.INTERACTIVE))

    # Schedule per pool, filling resources opportunistically.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)

        if avail_cpu < s.min_cpu or avail_ram < s.min_ram_gb:
            continue

        # Try to place multiple ops on this pool in one tick.
        # Hard cap to avoid pathological loops if queues contain many unfittable pipelines.
        max_placements = 64
        placements = 0

        while placements < max_placements:
            placements += 1

            # Recompute backlog and reserve for HP admission before scheduling batch.
            hp_backlog = has_hp_backlog()
            reserve_cpu = float(pool.max_cpu_pool) * s.hp_reserve_frac_cpu if hp_backlog else 0.0
            reserve_ram = float(pool.max_ram_pool) * s.hp_reserve_frac_ram if hp_backlog else 0.0

            made_progress = False

            # Priority order: QUERY > INTERACTIVE > BATCH
            for pr in (Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE):
                queue = s.queues.get(pr, [])
                if not queue:
                    continue

                # For batch when HP backlog exists: keep headroom.
                eff_avail_cpu = avail_cpu
                eff_avail_ram = avail_ram
                if pr == Priority.BATCH_PIPELINE and hp_backlog:
                    eff_avail_cpu = max(0.0, avail_cpu - reserve_cpu)
                    eff_avail_ram = max(0.0, avail_ram - reserve_ram)

                if eff_avail_cpu < s.min_cpu or eff_avail_ram < s.min_ram_gb:
                    continue

                picked = _pick_fittable_op_from_queue(s, pool, queue, eff_avail_cpu, eff_avail_ram)
                if not picked:
                    continue

                pipeline_id, pipeline, op = picked

                # Size resources.
                ram_need = _estimate_op_ram_gb(s, op)
                cpu_cap = _cpu_cap_for_priority(pool, pr)

                cpu = min(eff_avail_cpu, cpu_cap)
                cpu = max(0.0, cpu)
                if cpu < s.min_cpu:
                    continue

                ram = min(eff_avail_ram, ram_need)
                ram = max(0.0, ram)
                if ram < s.min_ram_gb:
                    continue

                # Create assignment for a single op.
                assignments.append(
                    Assignment(
                        ops=[op],
                        cpu=cpu,
                        ram=ram,
                        priority=pr,
                        pool_id=pool_id,
                        pipeline_id=pipeline_id,
                    )
                )

                avail_cpu -= cpu
                avail_ram -= ram
                made_progress = True
                break

            if not made_progress:
                break

            if avail_cpu < s.min_cpu or avail_ram < s.min_ram_gb:
                break

    return suspensions, assignments
