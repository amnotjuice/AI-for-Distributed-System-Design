# policy_key: scheduler_medium_040
# reasoning_effort: medium
# model: gpt-5.2-2025-12-11
# llm_cost: 0.089277
# generation_seconds: 146.33
# generated_at: 2026-03-14T03:12:46.511735
@register_scheduler_init(key="scheduler_medium_040")
def scheduler_medium_040_init(s):
    """Priority-aware, reservation-based scheduler with simple OOM-adaptive retries.

    Incremental improvements over naive FIFO:
      1) Maintain separate waiting queues per priority (QUERY > INTERACTIVE > BATCH).
      2) Avoid "give one op the whole pool" by using per-priority CPU/RAM shares.
      3) Keep headroom reserved for higher-priority work (soft admission control).
      4) Retry failed operators (esp. OOM-like) with exponentially increased RAM.
      5) Add a small starvation-escape hatch for batch to ensure eventual progress.

    Notes:
      - No hard preemption (needs visibility into running containers / container ids).
      - Schedules at most one operator per pipeline per scheduler tick to avoid duplicate
        assignments of the same op across pools before the executor updates state.
    """
    # Waiting queues by priority
    s.waiting = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }

    # Per-operator adaptive state (keyed by operator identity)
    s.op_ram_mult = {}       # op_key -> multiplier applied to base RAM share
    s.op_retry_count = {}    # op_key -> number of failures observed

    # Retry / backoff knobs
    s.max_retries = 3
    s.ram_mult_cap = 8.0

    # Resource shaping knobs (fractions of a pool's max capacity)
    s.cpu_share = {
        Priority.QUERY: 0.75,
        Priority.INTERACTIVE: 0.50,
        Priority.BATCH_PIPELINE: 0.25,
    }
    s.ram_share = {
        Priority.QUERY: 0.75,
        Priority.INTERACTIVE: 0.50,
        Priority.BATCH_PIPELINE: 0.25,
    }

    # Soft reservations: keep some headroom when higher priority is waiting
    s.reserve_for_query = 0.50        # reserve this fraction of pool for QUERY
    s.reserve_for_interactive = 0.25  # reserve this fraction for INTERACTIVE (when QUERY absent)

    # Simple batch starvation escape hatch
    s.starve_limit = 8
    s.batch_starve_ticks = 0

    # Pipelines we stop scheduling (e.g., operator exceeded retry limit)
    s.dropped_pipeline_ids = set()

    # Logical time
    s.tick = 0


@register_scheduler(key="scheduler_medium_040")
def scheduler_medium_040_scheduler(s, results, pipelines):
    """Scheduling step for the priority-aware, reservation-based policy."""
    def _op_key(op):
        # Best-effort stable identifier across callbacks without relying on imports.
        for attr in ("op_id", "operator_id", "task_id", "name"):
            if hasattr(op, attr):
                try:
                    return (attr, getattr(op, attr))
                except Exception:
                    pass
        return ("py_id", id(op))

    def _is_high_pri(pri):
        return pri in (Priority.QUERY, Priority.INTERACTIVE)

    def _queue_has_ready(priority, limit=32):
        q = s.waiting.get(priority, [])
        n = min(len(q), limit)
        for i in range(n):
            p = q[i]
            if p.pipeline_id in s.dropped_pipeline_ids:
                continue
            st = p.runtime_status()
            if st.is_pipeline_successful():
                continue
            op_list = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
            if op_list:
                op = op_list[0]
                if s.op_retry_count.get(_op_key(op), 0) <= s.max_retries:
                    return True
        return False

    def _priority_order():
        # QUERY always first.
        # If batch has been starved for a while, let it compete before INTERACTIVE.
        if s.batch_starve_ticks >= s.starve_limit:
            return [Priority.QUERY, Priority.BATCH_PIPELINE, Priority.INTERACTIVE]
        return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]

    def _allowed_capacity(pool, pri, avail_cpu, avail_ram, query_waiting, interactive_waiting):
        # Soft reservation: limit how much lower-priority work can consume when higher-priority is waiting.
        if pri == Priority.BATCH_PIPELINE:
            if query_waiting:
                reserve_cpu = pool.max_cpu_pool * s.reserve_for_query
                reserve_ram = pool.max_ram_pool * s.reserve_for_query
                return max(0.0, avail_cpu - reserve_cpu), max(0.0, avail_ram - reserve_ram)
            if interactive_waiting:
                reserve_cpu = pool.max_cpu_pool * s.reserve_for_interactive
                reserve_ram = pool.max_ram_pool * s.reserve_for_interactive
                return max(0.0, avail_cpu - reserve_cpu), max(0.0, avail_ram - reserve_ram)

        if pri == Priority.INTERACTIVE and query_waiting:
            reserve_cpu = pool.max_cpu_pool * s.reserve_for_query
            reserve_ram = pool.max_ram_pool * s.reserve_for_query
            return max(0.0, avail_cpu - reserve_cpu), max(0.0, avail_ram - reserve_ram)

        return avail_cpu, avail_ram

    def _request_size(pool, pri, op):
        # Base shares (avoid "one op eats the whole pool" which hurts tail latency).
        base_cpu = max(1.0, pool.max_cpu_pool * s.cpu_share.get(pri, 0.25))
        base_ram = max(1.0, pool.max_ram_pool * s.ram_share.get(pri, 0.25))

        # OOM-adaptive bump: exponential backoff on RAM after failures.
        mult = s.op_ram_mult.get(_op_key(op), 1.0)
        mult = min(max(1.0, mult), s.ram_mult_cap)
        ram = min(pool.max_ram_pool, base_ram * mult)
        cpu = min(pool.max_cpu_pool, base_cpu)

        return cpu, ram

    def _pop_next_ready_from_queue(priority, scheduled_pipeline_ids):
        # Round-robin within a priority queue: pop-front, examine, then either schedule or append-back.
        q = s.waiting.get(priority, [])
        if not q:
            return None, None

        attempts = len(q)
        while attempts > 0:
            attempts -= 1
            p = q.pop(0)

            if p.pipeline_id in scheduled_pipeline_ids:
                q.append(p)
                continue

            if p.pipeline_id in s.dropped_pipeline_ids:
                continue

            st = p.runtime_status()
            if st.is_pipeline_successful():
                continue

            op_list = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
            if not op_list:
                q.append(p)
                continue

            op = op_list[0]
            if s.op_retry_count.get(_op_key(op), 0) > s.max_retries:
                # Give up on this pipeline (keeps queues from clogging indefinitely).
                s.dropped_pipeline_ids.add(p.pipeline_id)
                continue

            # Keep pipeline in circulation for later ops, but ensure only one op per tick.
            q.append(p)
            return p, op_list

        return None, None

    # --- Incorporate new pipelines ---
    for p in pipelines:
        # Default unknown priorities to batch-like behavior
        pri = p.priority if p.priority in s.waiting else Priority.BATCH_PIPELINE
        s.waiting[pri].append(p)

    # --- Process results: update retry counters and RAM multipliers ---
    for r in results:
        if r.failed():
            # Treat failures as a signal to increase RAM for the involved ops.
            # (We don't parse error strings to keep this robust across simulators.)
            for op in getattr(r, "ops", []) or []:
                k = _op_key(op)
                s.op_retry_count[k] = s.op_retry_count.get(k, 0) + 1
                # Increase RAM multiplier exponentially; cap to avoid requesting absurd RAM.
                prev = s.op_ram_mult.get(k, 1.0)
                s.op_ram_mult[k] = min(s.ram_mult_cap, max(prev * 2.0, 2.0))
        else:
            # On success, keep learned RAM multiplier as-is (conservative; avoids oscillation).
            pass

    # Early exit if nothing to do
    if not pipelines and not results:
        return [], []

    s.tick += 1
    suspensions = []
    assignments = []

    # Starvation accounting: if batch has runnable ops but higher priority also runnable, tick up.
    query_ready_global = _queue_has_ready(Priority.QUERY)
    interactive_ready_global = _queue_has_ready(Priority.INTERACTIVE)
    batch_ready_global = _queue_has_ready(Priority.BATCH_PIPELINE)

    if batch_ready_global and (query_ready_global or interactive_ready_global):
        s.batch_starve_ticks += 1
    elif not batch_ready_global:
        s.batch_starve_ticks = 0

    # Prevent duplicate scheduling of the same pipeline in a single scheduler tick
    scheduled_pipeline_ids = set()

    # --- Main loop: fill pools with runnable ops, honoring priority and reservations ---
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool

        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # Re-check waiting status each pool (queues are shared across pools).
        query_waiting = _queue_has_ready(Priority.QUERY)
        interactive_waiting = _queue_has_ready(Priority.INTERACTIVE)

        made_progress = True
        while made_progress:
            made_progress = False

            # Try priorities in order. We attempt to schedule as many ops as possible per pool.
            for pri in _priority_order():
                if avail_cpu <= 0 or avail_ram <= 0:
                    break

                p, op_list = _pop_next_ready_from_queue(pri, scheduled_pipeline_ids)
                if not p or not op_list:
                    continue

                op = op_list[0]
                req_cpu, req_ram = _request_size(pool, pri, op)

                # Clamp by current pool availability (but still respect reservations for lower prios).
                allowed_cpu, allowed_ram = _allowed_capacity(
                    pool, pri, avail_cpu, avail_ram, query_waiting, interactive_waiting
                )

                # Also clamp the requested size to what is currently available.
                req_cpu = min(req_cpu, avail_cpu)
                req_ram = min(req_ram, avail_ram)

                # Check if it fits within allowed capacity (post-reservation).
                if req_cpu <= 0 or req_ram <= 0 or req_cpu > allowed_cpu or req_ram > allowed_ram:
                    # Can't place here now; leave pipeline in queue and try another.
                    continue

                assignments.append(
                    Assignment(
                        ops=op_list,
                        cpu=req_cpu,
                        ram=req_ram,
                        priority=p.priority,
                        pool_id=pool_id,
                        pipeline_id=p.pipeline_id,
                    )
                )

                scheduled_pipeline_ids.add(p.pipeline_id)
                avail_cpu -= req_cpu
                avail_ram -= req_ram
                made_progress = True

                # If we successfully scheduled batch, reset starvation counter.
                if pri == Priority.BATCH_PIPELINE:
                    s.batch_starve_ticks = 0

                # Move on to attempt another placement in this pool.
                break

    return suspensions, assignments
