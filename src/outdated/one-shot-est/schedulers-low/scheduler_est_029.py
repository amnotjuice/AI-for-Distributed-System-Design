# policy_key: scheduler_est_029
# reasoning_effort: low
# model: gpt-5.2-2025-12-11
# llm_cost: 0.048096
# generation_seconds: 38.00
# generated_at: 2026-04-01T00:04:15.491536
@register_scheduler_init(key="scheduler_est_029")
def scheduler_est_029_init(s):
    """
    Priority-aware FIFO with small, safe improvements over the naive baseline:

    1) Separate waiting queues per priority; always attempt higher priority first.
    2) Right-size CPU instead of greedily taking the entire pool (reduces head-of-line blocking).
    3) Use op.estimate.mem_peak_gb as a RAM floor and keep an adaptive per-op RAM floor that
       increases after failures (OOM-like), to reduce repeated failures.
    4) Basic aging: old batch work slowly gains effective priority to reduce starvation.

    Notes:
    - This policy intentionally avoids complex preemption logic since the minimal interface
      provided here does not expose a reliable list of currently running containers to suspend.
    """
    # Waiting items are stored as (enqueue_tick, pipeline)
    s.wait_q = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }

    # Adaptive RAM floors keyed by (pipeline_id, op_key). We update this on failures.
    # op_key is derived from attributes if present, else id(op).
    s.op_ram_floor_gb = {}

    # Global tick for simple aging
    s.tick = 0

    # Tuning knobs (conservative defaults)
    s.min_cpu_per_op = 1.0
    s.min_ram_per_op_gb = 0.25

    # Target CPU slices by priority (fraction of pool max CPU)
    s.cpu_frac = {
        Priority.QUERY: 0.75,
        Priority.INTERACTIVE: 0.60,
        Priority.BATCH_PIPELINE: 0.35,
    }

    # How aggressively to pad estimates (extra RAM is "free" vs OOM risk)
    s.ram_pad_mult = {
        Priority.QUERY: 1.40,
        Priority.INTERACTIVE: 1.35,
        Priority.BATCH_PIPELINE: 1.25,
    }

    # Aging: every N ticks waiting, batch gets a boost to compete with interactive/query
    s.aging_batch_to_interactive_ticks = 200
    s.aging_batch_to_query_ticks = 400


@register_scheduler(key="scheduler_est_029")
def scheduler_est_029(s, results, pipelines):
    """
    Scheduler step:
    - Enqueue arriving pipelines into per-priority queues.
    - Learn from failures (treat as memory floor increase).
    - For each pool, greedily assign at most one ready operator at a time,
      choosing the highest effective priority job that fits, with CPU/RAM sized
      to reduce contention and tail latency.
    """
    def _op_key(pipeline_id, op):
        # Prefer stable identifiers if they exist; fall back to object identity.
        op_id = getattr(op, "op_id", None)
        if op_id is None:
            op_id = getattr(op, "operator_id", None)
        if op_id is None:
            op_id = getattr(op, "id", None)
        if op_id is None:
            op_id = id(op)
        return (pipeline_id, op_id)

    def _clamp(x, lo, hi):
        return max(lo, min(hi, x))

    def _effective_priority(enqueue_tick, pipeline):
        # Base priority is pipeline.priority, but apply aging so batch eventually progresses.
        base = pipeline.priority
        waited = max(0, s.tick - enqueue_tick)

        if base == Priority.BATCH_PIPELINE:
            # After enough waiting, allow batch to compete as INTERACTIVE, then QUERY.
            if waited >= s.aging_batch_to_query_ticks:
                return Priority.QUERY
            if waited >= s.aging_batch_to_interactive_ticks:
                return Priority.INTERACTIVE
        return base

    def _priority_rank(pri):
        # Higher rank means higher scheduling precedence.
        if pri == Priority.QUERY:
            return 3
        if pri == Priority.INTERACTIVE:
            return 2
        return 1  # BATCH_PIPELINE

    def _ram_floor_for_op(pipeline, op):
        # Use estimator when available; otherwise be conservative but not huge.
        est = None
        if hasattr(op, "estimate") and op.estimate is not None:
            est = getattr(op.estimate, "mem_peak_gb", None)

        floor = s.min_ram_per_op_gb
        if est is not None:
            # Treat estimate as a floor (avoid OOM), but pad later based on priority.
            floor = max(floor, float(est))

        learned = s.op_ram_floor_gb.get(_op_key(pipeline.pipeline_id, op))
        if learned is not None:
            floor = max(floor, learned)

        return floor

    def _size_for(pool, pipeline, op):
        # CPU: allocate a slice of the pool, not the whole thing (reduces HOL blocking).
        max_cpu = float(pool.max_cpu_pool)
        avail_cpu = float(pool.avail_cpu_pool)

        target_frac = s.cpu_frac.get(pipeline.priority, 0.4)
        target_cpu = max(s.min_cpu_per_op, max_cpu * target_frac)
        cpu = min(avail_cpu, target_cpu)

        # RAM: allocate at least the floor; pad by priority. Cap by availability.
        avail_ram = float(pool.avail_ram_pool)
        floor = _ram_floor_for_op(pipeline, op)
        pad = s.ram_pad_mult.get(pipeline.priority, 1.25)
        ram = floor * pad

        # If we can afford more RAM (and it doesn't reduce ability to schedule others),
        # it's safe to round up modestly; keep within availability.
        ram = _clamp(ram, s.min_ram_per_op_gb, avail_ram)

        # Must fit; return None to indicate infeasible right now.
        if cpu <= 0 or ram <= 0:
            return None
        if cpu > avail_cpu + 1e-9 or ram > avail_ram + 1e-9:
            return None

        return cpu, ram

    # Advance tick (for aging)
    s.tick += 1

    # Enqueue new pipelines
    for p in pipelines:
        pri = p.priority
        if pri not in s.wait_q:
            # Unknown priority: treat as batch
            pri = Priority.BATCH_PIPELINE
        s.wait_q[pri].append((s.tick, p))

    # Learn from results: on failure, increase RAM floor for the involved ops.
    # We do not blindly retry here; the pipeline will naturally present FAILED ops in ASSIGNABLE_STATES.
    for r in results:
        if getattr(r, "failed", None) and r.failed():
            # Assume the failure could be memory-related; double the last attempted RAM.
            # (This is a safe heuristic; extra RAM is "free" relative to OOM.)
            last_ram = getattr(r, "ram", None)
            if last_ram is None:
                continue
            bump = float(last_ram) * 2.0
            for op in getattr(r, "ops", []) or []:
                key = _op_key(getattr(r, "pipeline_id", None), op) if hasattr(r, "pipeline_id") else None
                # If pipeline_id isn't in result, fall back to best-effort by searching queues later.
                if key is not None:
                    prev = s.op_ram_floor_gb.get(key, 0.0)
                    s.op_ram_floor_gb[key] = max(prev, bump)

    # Early exit if nothing changed (helps simulator speed)
    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    # Build a single "view" over all queued pipelines with effective priorities.
    # We'll pick candidates per pool, but keep stable FIFO within a given effective priority.
    def _iter_candidates():
        # Collect all queued items into a list of (eff_rank, base_rank, enqueue_tick, queue_pri, idx, pipeline)
        cands = []
        for qpri, items in s.wait_q.items():
            for idx, (enq, pl) in enumerate(items):
                status = pl.runtime_status()
                has_failures = status.state_counts[OperatorState.FAILED] > 0
                if status.is_pipeline_successful():
                    continue
                if has_failures:
                    # We still allow retries of FAILED ops (ASSIGNABLE_STATES includes FAILED).
                    pass
                eff_pri = _effective_priority(enq, pl)
                cands.append((_priority_rank(eff_pri), _priority_rank(pl.priority), enq, qpri, idx, pl))
        # Sort by: effective priority desc, then base priority desc, then enqueue time asc (FIFO)
        cands.sort(key=lambda x: (-x[0], -x[1], x[2]))
        return cands

    # Try to schedule at most one operator per pool per tick (simple, stable improvement over baseline).
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        if pool.avail_cpu_pool <= 0 or pool.avail_ram_pool <= 0:
            continue

        scheduled = False
        # Recompute candidates per pool to reflect updated avail; still cheap at simulator scale.
        for _, _, _, qpri, idx, pipeline in _iter_candidates():
            if scheduled:
                break

            status = pipeline.runtime_status()
            if status.is_pipeline_successful():
                continue

            # Pick exactly one ready-to-run operator (parents complete)
            op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
            if not op_list:
                continue

            op = op_list[0]
            sized = _size_for(pool, pipeline, op)
            if sized is None:
                continue
            cpu, ram = sized

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
            scheduled = True

            # Remove pipeline from its original queue entry (by index).
            # Since we only schedule one op, keep the pipeline queued for future ops.
            # We remove and re-append with original enqueue time to preserve FIFO/aging.
            try:
                enq, pl = s.wait_q[qpri].pop(idx)
                s.wait_q[qpri].append((enq, pl))
            except Exception:
                # If indices shifted due to concurrent modifications, do nothing; safe fallback.
                pass

    # Garbage-collect completed pipelines from queues to prevent unbounded growth.
    for pri in list(s.wait_q.keys()):
        new_items = []
        for enq, pl in s.wait_q[pri]:
            st = pl.runtime_status()
            if st.is_pipeline_successful():
                continue
            new_items.append((enq, pl))
        s.wait_q[pri] = new_items

    return suspensions, assignments
