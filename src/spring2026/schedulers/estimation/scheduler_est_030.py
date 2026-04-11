# policy_key: scheduler_est_030
# reasoning_effort: low
# model: gpt-5.2-2025-12-11
# llm_cost: 0.040732
# generation_seconds: 35.57
# generated_at: 2026-04-01T00:04:51.060537
@register_scheduler_init(key="scheduler_est_030")
def scheduler_est_030_init(s):
    """
    Priority-aware, estimate-guided scheduler (small improvement over naive FIFO).

    Main changes vs naive:
      - Maintain separate queues by priority (QUERY/INTERACTIVE before BATCH).
      - Avoid allocating the *entire* pool to a single op; pack multiple ops per tick.
      - Use op.estimate.mem_peak_gb as a RAM floor (plus a safety multiplier) to reduce OOMs.
      - On OOM-like failures, bump the per-op RAM multiplier and retry (bounded retries).
      - Basic fairness: ensure batch gets some scheduling opportunities when resources allow.
    """
    s.q_query = []
    s.q_interactive = []
    s.q_batch = []

    # Per-operator adaptive memory multiplier (keyed by Python object id(op))
    s.mem_mult = {}  # id(op) -> float

    # Per-operator failure counters to avoid infinite retries
    s.fail_counts = {}  # id(op) -> int

    # Lightweight fairness knobs
    s.batch_every_n = 5  # after scheduling this many high-pri ops, try to schedule 1 batch op
    s._hi_scheduled_since_batch = 0


@register_scheduler(key="scheduler_est_030")
def scheduler_est_030_scheduler(s, results, pipelines):
    """
    Scheduler step: enqueue new pipelines, adapt memory multipliers based on failures,
    then assign ready operators across pools, respecting priority and packing multiple
    ops per pool subject to CPU/RAM availability.
    """
    # ---------- helpers (kept inside to match template constraints) ----------
    def _enqueue_pipeline(p):
        if p.priority == Priority.QUERY:
            s.q_query.append(p)
        elif p.priority == Priority.INTERACTIVE:
            s.q_interactive.append(p)
        else:
            s.q_batch.append(p)

    def _is_oom_like(err):
        if err is None:
            return False
        msg = str(err).lower()
        return ("oom" in msg) or ("out of memory" in msg) or ("memory" in msg and "exceed" in msg)

    def _next_assignable_op(pipeline):
        status = pipeline.runtime_status()
        # Only consider ops whose parents are complete, and are in assignable states
        op_list = status.get_ops([OperatorState.PENDING, OperatorState.FAILED], require_parents_complete=True)
        if not op_list:
            return None
        # Prefer an op that hasn't exceeded retry budget
        for op in op_list:
            if s.fail_counts.get(id(op), 0) <= 3:
                return op
        return None

    def _pipeline_done_or_dead(pipeline):
        status = pipeline.runtime_status()
        if status.is_pipeline_successful():
            return True
        # If all remaining assignable ops are beyond retry budget, treat as dead to avoid livelock
        op_list = status.get_ops([OperatorState.PENDING, OperatorState.FAILED], require_parents_complete=True)
        if not op_list:
            return False
        for op in op_list:
            if s.fail_counts.get(id(op), 0) <= 3:
                return False
        return True

    def _estimate_ram_floor_gb(op):
        # Use estimator as a RAM floor; extra RAM is "free" per simulator model.
        est = None
        if hasattr(op, "estimate") and op.estimate is not None and hasattr(op.estimate, "mem_peak_gb"):
            est = op.estimate.mem_peak_gb
        if est is None:
            est = 1.0  # conservative default
        mult = s.mem_mult.get(id(op), 1.25)  # start slightly above estimate
        return max(0.25, float(est) * float(mult))

    def _target_cpu_for(priority, pool):
        # Keep per-op CPU modest so we can pack and reduce head-of-line blocking.
        # Still bias interactive/query slightly higher.
        max_cpu = getattr(pool, "max_cpu_pool", 1) or 1
        base = max(1.0, max_cpu / 4.0)
        if priority in (Priority.QUERY, Priority.INTERACTIVE):
            base = max(base, max_cpu / 3.0)
        # Never request more than 8 vCPU per op in this simple policy.
        return min(8.0, base)

    def _pop_next_pipeline_for_scheduling():
        # Strict priority, with a small fairness rule to occasionally pick batch.
        # We count how many high-priority ops we scheduled since last batch.
        if s._hi_scheduled_since_batch >= s.batch_every_n and s.q_batch:
            s._hi_scheduled_since_batch = 0
            return s.q_batch.pop(0)

        if s.q_query:
            s._hi_scheduled_since_batch += 1
            return s.q_query.pop(0)
        if s.q_interactive:
            s._hi_scheduled_since_batch += 1
            return s.q_interactive.pop(0)
        if s.q_batch:
            s._hi_scheduled_since_batch = 0
            return s.q_batch.pop(0)
        return None

    # ---------- ingest new pipelines ----------
    for p in pipelines:
        _enqueue_pipeline(p)

    # If nothing changed, early exit (matches example behavior)
    if not pipelines and not results:
        return [], []

    # ---------- adapt memory multipliers based on recent failures ----------
    for r in results:
        if r is None:
            continue
        if hasattr(r, "failed") and r.failed():
            # Increase RAM multiplier on OOM-like failures; also track retry count
            if getattr(r, "ops", None):
                for op in r.ops:
                    oid = id(op)
                    s.fail_counts[oid] = s.fail_counts.get(oid, 0) + 1
                    if _is_oom_like(getattr(r, "error", None)):
                        # Exponential backoff; cap to avoid absurd allocations
                        s.mem_mult[oid] = min(16.0, s.mem_mult.get(oid, 1.25) * 2.0)
                    else:
                        # Non-OOM failures: small bump only, still allow a couple retries
                        s.mem_mult[oid] = min(4.0, s.mem_mult.get(oid, 1.25) * 1.25)

    # ---------- scheduling / placement ----------
    suspensions = []
    assignments = []

    # We pack multiple ops per pool while resources remain.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)

        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # Try to fill the pool with multiple assignments, but avoid long loops.
        # The cap prevents pathological behavior when queues contain many blocked pipelines.
        attempts = 0
        max_attempts = 64

        # Local buffer for pipelines we popped but didn't schedule (put back)
        put_back = []

        while attempts < max_attempts and avail_cpu > 0 and avail_ram > 0:
            attempts += 1
            pipeline = _pop_next_pipeline_for_scheduling()
            if pipeline is None:
                break

            if _pipeline_done_or_dead(pipeline):
                # Drop completed or unschedulable pipelines
                continue

            op = _next_assignable_op(pipeline)
            if op is None:
                # No ready ops yet; put back to try later
                put_back.append(pipeline)
                continue

            ram_floor = _estimate_ram_floor_gb(op)

            # If we can't meet the RAM floor in this pool right now, defer
            if ram_floor > avail_ram:
                put_back.append(pipeline)
                continue

            # CPU sizing: modest, priority-biased, and bounded by availability
            cpu_req = min(avail_cpu, _target_cpu_for(pipeline.priority, pool))
            # Ensure at least 1 vCPU if any CPU remains (avoid 0-sized assignments)
            if cpu_req < 1.0 and avail_cpu >= 1.0:
                cpu_req = 1.0
            if cpu_req <= 0:
                put_back.append(pipeline)
                break

            # RAM sizing: give at least the floor; if plenty of RAM, add a small cushion.
            # (Extra RAM is "free", but we still want to pack multiple ops.)
            ram_req = ram_floor
            # Add cushion for interactive/query when possible
            if pipeline.priority in (Priority.QUERY, Priority.INTERACTIVE):
                ram_req = min(avail_ram, max(ram_req, ram_floor * 1.2))
            # For batch, be closer to floor to improve packing
            else:
                ram_req = min(avail_ram, max(ram_req, ram_floor * 1.05))

            # Final check: must fit
            if cpu_req > avail_cpu or ram_req > avail_ram:
                put_back.append(pipeline)
                continue

            assignment = Assignment(
                ops=[op],
                cpu=cpu_req,
                ram=ram_req,
                priority=pipeline.priority,
                pool_id=pool_id,
                pipeline_id=pipeline.pipeline_id,
            )
            assignments.append(assignment)

            # Update remaining resources (approximate packing model)
            avail_cpu -= cpu_req
            avail_ram -= ram_req

            # Keep pipeline around for its next ops
            put_back.append(pipeline)

        # Re-enqueue deferred pipelines to their respective priority queues (preserve order)
        for p in put_back:
            _enqueue_pipeline(p)

    return suspensions, assignments
