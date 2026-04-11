# policy_key: scheduler_high_040
# reasoning_effort: high
# model: gpt-5.2-2025-12-11
# llm_cost: 0.130059
# generation_seconds: 162.89
# generated_at: 2026-03-14T04:15:04.900822
@register_scheduler_init(key="scheduler_high_040")
def scheduler_high_040_init(s):
    """
    Priority-aware, latency-first scheduler (small, safe improvements over naive FIFO).

    Key ideas:
    - Maintain separate FIFO queues per priority (QUERY > INTERACTIVE > BATCH_PIPELINE).
    - Prevent obvious latency pathologies by (soft) reserving capacity for runnable high-priority work:
        * when high-priority runnable ops exist, do not let batch consume the whole pool.
    - Avoid allocating "all available resources" to a single op; use per-priority caps to improve
      concurrency and reduce head-of-line blocking for interactive/query work.
    - Retry failed ops a small number of times, ramping RAM on suspected OOM to reduce repeated failures.

    Notes:
    - This policy intentionally avoids preemption because we cannot reliably enumerate running containers
      from the provided interface. Instead, it uses reservation + admission control.
    """
    s.ticks = 0

    # Per-priority waiting queues (FIFO within each priority).
    s.queues = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }

    # Pipeline bookkeeping.
    s.pipeline_arrival_tick = {}  # pipeline_id -> tick when first seen
    s.dead_pipelines = set()      # pipeline_ids we stop scheduling (too many failures)

    # Operator-level history for simple, robust retry sizing.
    # op_key is a tuple derived from (pipeline_id, operator identity).
    s.op_failures = {}    # op_key -> failure count
    s.op_ram_hint = {}    # op_key -> suggested RAM for next run
    s.op_cpu_hint = {}    # op_key -> suggested CPU for next run

    # Map op object identity to pipeline_id so we can attribute ExecutionResult events.
    s.op_to_pipeline = {}  # id(op_obj) -> pipeline_id

    # Tunables (start conservative; keep behavior stable).
    s.max_retries_per_op = 3
    s.batch_starvation_ticks = 120  # allow some batch progress even under constant high-priority load

    # Soft reservation when runnable high-priority work exists.
    s.reserve_frac_cpu = 0.35
    s.reserve_frac_ram = 0.35

    # Per-op caps as fraction of pool max; reduces the naive "one op consumes everything" flaw.
    # Query gets more to reduce tail latency, but still leaves headroom for concurrent high-priority ops.
    s.cap_frac_cpu = {
        Priority.QUERY: 0.75,
        Priority.INTERACTIVE: 0.55,
        Priority.BATCH_PIPELINE: 0.30,
    }
    s.cap_frac_ram = {
        Priority.QUERY: 0.75,
        Priority.INTERACTIVE: 0.60,
        Priority.BATCH_PIPELINE: 0.35,
    }

    # Bound the number of containers we start per pool per tick to avoid thrash.
    s.max_assignments_per_pool_per_tick = 4


def _op_key(pipeline_id, op):
    """Create a stable-ish key for an operator within a pipeline."""
    for attr in ("op_id", "operator_id", "name", "id"):
        if hasattr(op, attr):
            v = getattr(op, attr)
            if isinstance(v, (int, str)):
                return (pipeline_id, attr, v)
    # Fallback to python object identity (works if DAG nodes are stable objects in the simulator).
    return (pipeline_id, "pyid", id(op))


def _is_oom_error(err):
    """Heuristic OOM detection from error payloads (string-ish)."""
    if err is None:
        return False
    try:
        s = str(err).lower()
    except Exception:
        return False
    return ("oom" in s) or ("out of memory" in s) or ("memoryerror" in s) or ("cuda out of memory" in s)


def _has_runnable_op(queue, dead_pipelines, scheduled_pipelines, scheduled_ops):
    """
    Non-mutating check: is there at least one runnable operator in this queue?
    Avoids reserving capacity when high-priority pipelines are blocked on dependencies.
    """
    for p in queue:
        if p.pipeline_id in dead_pipelines:
            continue
        if p.pipeline_id in scheduled_pipelines:
            continue
        st = p.runtime_status()
        if st.is_pipeline_successful():
            continue
        ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        for op in ops:
            if _op_key(p.pipeline_id, op) in scheduled_ops:
                continue
            return True
    return False


def _oldest_wait_ticks(queue, now_tick, pipeline_arrival_tick):
    oldest = None
    for p in queue:
        t0 = pipeline_arrival_tick.get(p.pipeline_id, now_tick)
        w = now_tick - t0
        if oldest is None or w > oldest:
            oldest = w
    return oldest or 0


def _pop_next_runnable(queue, dead_pipelines, scheduled_pipelines, scheduled_ops, max_retries_per_op, op_failures):
    """
    Pop/rotate FIFO until we find a runnable op (parents complete, assignable state),
    while keeping queue order fair. Returns (pipeline, op) or (None, None).
    """
    n = len(queue)
    for _ in range(n):
        p = queue.pop(0)

        # Drop pipelines we should no longer schedule.
        if p.pipeline_id in dead_pipelines:
            continue

        st = p.runtime_status()
        if st.is_pipeline_successful():
            continue

        # Avoid scheduling multiple ops from the same pipeline in the same tick
        # (keeps behavior stable and avoids accidental double-scheduling of the same op).
        if p.pipeline_id in scheduled_pipelines:
            queue.append(p)
            continue

        ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        chosen = None
        for op in ops:
            ok = _op_key(p.pipeline_id, op)
            if ok in scheduled_ops:
                continue
            # If an op has repeatedly failed, stop attempting it (and thus the pipeline).
            if op_failures.get(ok, 0) > max_retries_per_op:
                dead_pipelines.add(p.pipeline_id)
                chosen = None
                break
            chosen = op
            break

        # Keep pipeline in the queue (round-robin fairness) unless it became dead.
        if p.pipeline_id not in dead_pipelines:
            queue.append(p)

        if chosen is not None and p.pipeline_id not in dead_pipelines:
            return p, chosen

    return None, None


def _compute_allocation(priority, pool, avail_cpu, avail_ram, reserved_cpu, reserved_ram,
                        op_key, op_cpu_hint, op_ram_hint, enforce_reservation):
    """
    Compute CPU/RAM to assign for a single-op container.
    - Use per-priority cap based on pool max (avoid allocating entire pool to one op).
    - Apply hints (from prior attempts) but do not exceed caps unless reservation is not enforced.
    """
    # Budget available for this allocation.
    if enforce_reservation:
        cpu_budget = max(0.0, avail_cpu - reserved_cpu)
        ram_budget = max(0.0, avail_ram - reserved_ram)
    else:
        cpu_budget = avail_cpu
        ram_budget = avail_ram

    # If we cannot allocate any meaningful resources, return zeros.
    if cpu_budget <= 0 or ram_budget <= 0:
        return 0.0, 0.0

    # Cap per container by priority.
    cap_cpu = min(cpu_budget, pool.max_cpu_pool * float(s.cap_frac_cpu.get(priority, 0.5)))
    cap_ram = min(ram_budget, pool.max_ram_pool * float(s.cap_frac_ram.get(priority, 0.5)))

    # Start from caps (latency-friendly for high priority; batch is already limited by small caps).
    cpu = cap_cpu
    ram = cap_ram

    # Apply sizing hints from previous attempts (usually increases on OOM).
    if op_key in op_cpu_hint:
        cpu = max(cpu, float(op_cpu_hint[op_key]))
    if op_key in op_ram_hint:
        ram = max(ram, float(op_ram_hint[op_key]))

    # Enforce cap (keeps concurrency predictable), but don't exceed total availability.
    cpu = min(cpu, cpu_budget)
    ram = min(ram, ram_budget)

    # Avoid zero allocations; if pool is fractional, require >0.
    if cpu <= 0 or ram <= 0:
        return 0.0, 0.0

    return cpu, ram


@register_scheduler(key="scheduler_high_040")
def scheduler_high_040_scheduler(s, results, pipelines):
    """
    See init docstring for the policy overview.
    """
    s.ticks += 1

    # --- 1) Incorporate execution feedback (simple retry sizing) ---
    for r in (results or []):
        if not getattr(r, "ops", None):
            continue

        pool = s.executor.pools[r.pool_id]
        for op in r.ops:
            pid = s.op_to_pipeline.get(id(op))
            if pid is None:
                # If we can't attribute this op to a pipeline, skip hints.
                continue

            ok = _op_key(pid, op)

            # Track failures and ramp RAM on suspected OOM.
            if r.failed():
                s.op_failures[ok] = s.op_failures.get(ok, 0) + 1

                # On OOM, ramp RAM aggressively; otherwise do a small bump.
                if _is_oom_error(r.error):
                    next_ram = min(pool.max_ram_pool, float(r.ram) * 2.0 if r.ram else pool.max_ram_pool * 0.5)
                    s.op_ram_hint[ok] = max(float(s.op_ram_hint.get(ok, 0.0)), next_ram)
                else:
                    # Non-OOM failure: mild bump to reduce repeated under-provisioning.
                    next_ram = min(pool.max_ram_pool, float(r.ram) * 1.25 if r.ram else pool.max_ram_pool * 0.25)
                    s.op_ram_hint[ok] = max(float(s.op_ram_hint.get(ok, 0.0)), next_ram)

                # Small CPU bump (bounded) to hedge against CPU starvation.
                if r.cpu:
                    next_cpu = min(pool.max_cpu_pool, float(r.cpu) * 1.25)
                    s.op_cpu_hint[ok] = max(float(s.op_cpu_hint.get(ok, 0.0)), next_cpu)
            else:
                # On success, keep hints as at least what worked (useful for future runs).
                if r.ram:
                    s.op_ram_hint[ok] = max(float(s.op_ram_hint.get(ok, 0.0)), float(r.ram))
                if r.cpu:
                    s.op_cpu_hint[ok] = max(float(s.op_cpu_hint.get(ok, 0.0)), float(r.cpu))

    # --- 2) Enqueue new pipelines by priority ---
    for p in (pipelines or []):
        if p.pipeline_id not in s.pipeline_arrival_tick:
            s.pipeline_arrival_tick[p.pipeline_id] = s.ticks
        # Default unknown priorities to batch-like behavior.
        pr = getattr(p, "priority", Priority.BATCH_PIPELINE)
        if pr not in s.queues:
            pr = Priority.BATCH_PIPELINE
        s.queues[pr].append(p)

    # Early exit if nothing to do.
    if not (pipelines or results):
        # Still might have backlog; don't early exit if queues have work.
        if not any(s.queues[pr] for pr in s.queues):
            return [], []

    suspensions = []
    assignments = []

    # Track what we schedule within this tick to avoid duplicates.
    scheduled_pipelines = set()
    scheduled_ops = set()

    # Determine if there is runnable high-priority work (non-mutating checks).
    runnable_query = _has_runnable_op(
        s.queues[Priority.QUERY], s.dead_pipelines, scheduled_pipelines, scheduled_ops
    )
    runnable_interactive = _has_runnable_op(
        s.queues[Priority.INTERACTIVE], s.dead_pipelines, scheduled_pipelines, scheduled_ops
    )
    runnable_high = runnable_query or runnable_interactive

    # Batch fairness: if batch has been waiting too long, allow some progress even under contention.
    oldest_batch_wait = _oldest_wait_ticks(s.queues[Priority.BATCH_PIPELINE], s.ticks, s.pipeline_arrival_tick)
    batch_starved = oldest_batch_wait >= s.batch_starvation_ticks

    # --- 3) Place work onto pools ---
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool

        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # Soft reservation only matters when high-priority runnable ops exist.
        reserved_cpu = pool.max_cpu_pool * float(s.reserve_frac_cpu) if runnable_high else 0.0
        reserved_ram = pool.max_ram_pool * float(s.reserve_frac_ram) if runnable_high else 0.0

        num_started = 0
        batch_started_in_this_pool = False

        while num_started < s.max_assignments_per_pool_per_tick:
            if avail_cpu <= 0 or avail_ram <= 0:
                break

            # Prefer high priority always.
            chosen_pipeline = None
            chosen_op = None
            chosen_priority = None

            # 1) QUERY
            chosen_pipeline, chosen_op = _pop_next_runnable(
                s.queues[Priority.QUERY],
                s.dead_pipelines,
                scheduled_pipelines,
                scheduled_ops,
                s.max_retries_per_op,
                s.op_failures,
            )
            if chosen_pipeline is not None:
                chosen_priority = Priority.QUERY
            else:
                # 2) INTERACTIVE
                chosen_pipeline, chosen_op = _pop_next_runnable(
                    s.queues[Priority.INTERACTIVE],
                    s.dead_pipelines,
                    scheduled_pipelines,
                    scheduled_ops,
                    s.max_retries_per_op,
                    s.op_failures,
                )
                if chosen_pipeline is not None:
                    chosen_priority = Priority.INTERACTIVE
                else:
                    # 3) BATCH (guarded by reservation + starvation logic)
                    allow_batch = (not runnable_high) or batch_starved
                    if runnable_high and batch_started_in_this_pool:
                        allow_batch = False

                    if allow_batch:
                        chosen_pipeline, chosen_op = _pop_next_runnable(
                            s.queues[Priority.BATCH_PIPELINE],
                            s.dead_pipelines,
                            scheduled_pipelines,
                            scheduled_ops,
                            s.max_retries_per_op,
                            s.op_failures,
                        )
                        if chosen_pipeline is not None:
                            chosen_priority = Priority.BATCH_PIPELINE

            if chosen_pipeline is None or chosen_op is None or chosen_priority is None:
                break

            # Compute allocation. For batch under high contention, enforce reservation.
            enforce_reservation = (chosen_priority == Priority.BATCH_PIPELINE) and runnable_high

            pid = chosen_pipeline.pipeline_id
            ok = _op_key(pid, chosen_op)

            cpu, ram = _compute_allocation(
                chosen_priority,
                pool,
                avail_cpu,
                avail_ram,
                reserved_cpu,
                reserved_ram,
                ok,
                s.op_cpu_hint,
                s.op_ram_hint,
                enforce_reservation,
            )

            # If batch can't fit beyond reserved headroom, stop placing batch in this pool for now.
            if cpu <= 0 or ram <= 0:
                # Put back scheduling markers (none were set yet), then stop.
                break

            # Record mapping for later ExecutionResult attribution.
            s.op_to_pipeline[id(chosen_op)] = pid

            # Create assignment (one op per container, to keep behavior predictable).
            assignments.append(
                Assignment(
                    ops=[chosen_op],
                    cpu=cpu,
                    ram=ram,
                    priority=chosen_pipeline.priority,
                    pool_id=pool_id,
                    pipeline_id=pid,
                )
            )

            # Update local accounting to enable multiple placements in same pool.
            avail_cpu -= cpu
            avail_ram -= ram
            num_started += 1

            # Mark scheduled items for this tick.
            scheduled_pipelines.add(pid)
            scheduled_ops.add(ok)

            if chosen_priority == Priority.BATCH_PIPELINE:
                batch_started_in_this_pool = True

    return suspensions, assignments
