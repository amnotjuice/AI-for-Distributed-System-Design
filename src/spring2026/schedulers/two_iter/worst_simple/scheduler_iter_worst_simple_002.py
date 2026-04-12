# policy_key: scheduler_iter_worst_simple_002
# context: simple
# model: gpt-5.2-2025-12-11
# llm_cost: 0.060924
# generation_seconds: 46.10
# generated_at: 2026-04-12T00:35:49.143074
@register_scheduler_init(key="scheduler_iter_worst_simple_002")
def scheduler_iter_worst_simple_002_init(s):
    """
    Iteration 2: Priority-first packing with explicit headroom reservations.

    Goals (reduce weighted latency):
      - Always schedule QUERY first, then INTERACTIVE, then BATCH.
      - If multiple pools: keep pool 0 primarily for latency-sensitive work.
      - Even in a single pool: reserve CPU/RAM headroom so high-priority arrivals can start quickly.
      - Pack multiple small containers per pool per tick (not just one), but keep BATCH capped.
      - Learn RAM needs on OOM-like failures and retry FAILED ops with increased RAM.

    Notes:
      - We do NOT drop a pipeline just because it contains FAILED ops; FAILED is assignable and may
        represent an OOM retry path in the simulator.
      - We keep the logic intentionally simple and deterministic.
    """
    from collections import deque

    # Per-priority queues (base priority)
    s.q_query = deque()
    s.q_interactive = deque()
    s.q_batch = deque()

    # pipeline_id -> enqueue tick (for optional aging; kept but conservative)
    s.enq_tick = {}
    s.tick = 0

    # (pipeline_id, op_key) -> resource hints
    s.op_ram_hint = {}
    s.op_cpu_hint = {}

    # Aging threshold (kept high; weighted latency cares more about high-priority)
    s.batch_promotion_ticks = 10_000

    # Minimum slices
    s.min_cpu_slice = 1
    s.min_ram_slice = 1

    # Reservation fractions (leave headroom for high-priority arrivals)
    # Applied per pool when deciding whether to schedule lower-priority work.
    s.reserve_frac_cpu_single_pool = 0.35
    s.reserve_frac_ram_single_pool = 0.35

    # When multiple pools exist, we reserve more headroom on pool 0 (latency pool).
    s.reserve_frac_cpu_pool0 = 0.45
    s.reserve_frac_ram_pool0 = 0.45

    # Cap batch per-container to avoid one batch operator monopolizing a pool
    s.batch_cap_frac_cpu = 0.35
    s.batch_cap_frac_ram = 0.50

    # Default target CPU allocations by priority (bounded by availability/caps)
    s.query_target_cpu = 8
    s.interactive_target_cpu = 4
    s.batch_target_cpu = 2


@register_scheduler(key="scheduler_iter_worst_simple_002")
def scheduler_iter_worst_simple_002_scheduler(s, results, pipelines):
    """
    Priority-first packing with headroom reservations and OOM backoff.

    Each tick:
      1) Enqueue new pipelines into per-priority queues.
      2) Update resource hints from failures (OOM => bump RAM aggressively).
      3) For each pool, repeatedly schedule ready operators while available resources remain
         above a priority-dependent reservation threshold.
    """
    s.tick += 1

    # ---------------- Helpers ----------------
    def _queue_for_priority(prio):
        if prio == Priority.QUERY:
            return s.q_query
        if prio == Priority.INTERACTIVE:
            return s.q_interactive
        return s.q_batch

    def _enqueue_pipeline(p):
        pid = p.pipeline_id
        if pid not in s.enq_tick:
            s.enq_tick[pid] = s.tick
            _queue_for_priority(p.priority).append(p)

    def _drop_pipeline(p):
        pid = p.pipeline_id
        if pid in s.enq_tick:
            del s.enq_tick[pid]
        # Actual deque cleanup is lazy.

    def _effective_priority(p):
        # Conservative aging: only promote very old batch (mostly irrelevant for weighted latency).
        if p.priority != Priority.BATCH_PIPELINE:
            return p.priority
        waited = s.tick - s.enq_tick.get(p.pipeline_id, s.tick)
        if waited >= s.batch_promotion_ticks:
            return Priority.INTERACTIVE
        return p.priority

    def _op_key(op):
        # Try stable-ish identifiers; fallback to Python identity.
        if hasattr(op, "op_id"):
            return ("op_id", getattr(op, "op_id"))
        if hasattr(op, "operator_id"):
            return ("operator_id", getattr(op, "operator_id"))
        if hasattr(op, "name"):
            return ("name", getattr(op, "name"))
        return ("py_id", id(op))

    def _get_one_ready_op(p):
        st = p.runtime_status()
        if st.is_pipeline_successful():
            return None
        # FAILED is in ASSIGNABLE_STATES (retry path); require parents complete.
        ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        if not ops:
            return None
        return ops[0]

    def _pool_reserve(pool_id, pool):
        # Reserve more on pool 0 when multiple pools exist.
        if s.executor.num_pools > 1 and pool_id == 0:
            return (
                int(pool.max_cpu_pool * s.reserve_frac_cpu_pool0),
                int(pool.max_ram_pool * s.reserve_frac_ram_pool0),
            )
        # Single pool or non-latency pool
        return (
            int(pool.max_cpu_pool * s.reserve_frac_cpu_single_pool),
            int(pool.max_ram_pool * s.reserve_frac_ram_single_pool),
        )

    def _prefer_pool_for_priority(prio, pool_id):
        # If multiple pools, keep pool 0 for QUERY/INTERACTIVE and keep BATCH off pool 0.
        if s.executor.num_pools <= 1:
            return True
        if prio in (Priority.QUERY, Priority.INTERACTIVE):
            return pool_id == 0 or True  # allowed anywhere, but we will naturally fill pool 0 first
        # BATCH: strongly discourage pool 0
        return pool_id != 0

    def _target_cpu_for_priority(prio):
        if prio == Priority.QUERY:
            return s.query_target_cpu
        if prio == Priority.INTERACTIVE:
            return s.interactive_target_cpu
        return s.batch_target_cpu

    def _compute_request(p, op, pool_id):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool
        if avail_cpu <= 0 or avail_ram <= 0:
            return 0, 0

        eff_prio = _effective_priority(p)
        target_cpu = _target_cpu_for_priority(eff_prio)

        # Start with small base RAM; rely on OOM learning to grow.
        base_ram = max(s.min_ram_slice, min(4, pool.max_ram_pool))
        base_cpu = max(s.min_cpu_slice, min(target_cpu, pool.max_cpu_pool))

        key = (p.pipeline_id, _op_key(op))
        hinted_ram = s.op_ram_hint.get(key, base_ram)
        hinted_cpu = s.op_cpu_hint.get(key, base_cpu)

        req_cpu = max(base_cpu, hinted_cpu)
        req_ram = max(base_ram, hinted_ram)

        # Batch caps to avoid monopolizing pools.
        if eff_prio == Priority.BATCH_PIPELINE:
            cpu_cap = max(s.min_cpu_slice, int(pool.max_cpu_pool * s.batch_cap_frac_cpu))
            ram_cap = max(s.min_ram_slice, int(pool.max_ram_pool * s.batch_cap_frac_ram))
            req_cpu = min(req_cpu, cpu_cap)
            req_ram = min(req_ram, ram_cap)

        # Never exceed currently available resources.
        req_cpu = max(s.min_cpu_slice, min(req_cpu, avail_cpu))
        req_ram = max(s.min_ram_slice, min(req_ram, avail_ram))
        return req_cpu, req_ram

    def _scan_pop_runnable(q, pool_id):
        """
        Rotate through a queue once to find a pipeline with a ready op.
        Returns (pipeline, op) or (None, None). Preserves queue order otherwise.
        """
        n = len(q)
        for _ in range(n):
            p = q.popleft()
            st = p.runtime_status()
            if st.is_pipeline_successful():
                _drop_pipeline(p)
                continue

            eff_prio = _effective_priority(p)
            if not _prefer_pool_for_priority(eff_prio, pool_id):
                # Keep batch off pool 0 when multiple pools exist.
                q.append(p)
                continue

            op = _get_one_ready_op(p)
            if op is None:
                q.append(p)
                continue

            # Found runnable
            return p, op

        return None, None

    # ---------------- Ingest new pipelines ----------------
    for p in pipelines:
        _enqueue_pipeline(p)

    # Early exit if nothing changed
    if not pipelines and not results:
        return [], []

    # ---------------- Learn from results (OOM backoff) ----------------
    for r in results:
        if not (hasattr(r, "failed") and r.failed()):
            continue

        pid = getattr(r, "pipeline_id", None)
        ops = getattr(r, "ops", None) or []
        if pid is None or not ops:
            continue

        err = str(getattr(r, "error", "") or "").lower()
        oom_like = ("oom" in err) or ("out of memory" in err) or ("memory" in err)

        prev_ram = int(getattr(r, "ram", 1) or 1)
        prev_cpu = int(getattr(r, "cpu", 1) or 1)
        prev_ram = max(s.min_ram_slice, prev_ram)
        prev_cpu = max(s.min_cpu_slice, prev_cpu)

        for op in ops:
            key = (pid, _op_key(op))
            cur_ram = s.op_ram_hint.get(key, prev_ram)
            cur_cpu = s.op_cpu_hint.get(key, prev_cpu)

            if oom_like:
                # Aggressively increase RAM; CPU usually doesn't fix OOM.
                s.op_ram_hint[key] = max(cur_ram, prev_ram * 2)
                s.op_cpu_hint[key] = cur_cpu
            else:
                # Generic failure: small bump to both (bounded later by availability/caps).
                s.op_ram_hint[key] = max(cur_ram, int(prev_ram * 1.25) + 1)
                s.op_cpu_hint[key] = max(cur_cpu, int(prev_cpu * 1.15) + 1)

    # ---------------- Dispatch ----------------
    suspensions = []
    assignments = []

    # Pool iteration order: if multiple pools, fill pool 0 first (latency pool).
    pool_order = list(range(s.executor.num_pools))
    if s.executor.num_pools > 1:
        pool_order = [0] + [i for i in range(1, s.executor.num_pools)]

    for pool_id in pool_order:
        pool = s.executor.pools[pool_id]
        if pool.avail_cpu_pool <= 0 or pool.avail_ram_pool <= 0:
            continue

        reserve_cpu, reserve_ram = _pool_reserve(pool_id, pool)

        # Pack multiple assignments per pool per tick.
        # Stop when remaining headroom falls below minimal runnable resources for high-priority.
        while True:
            avail_cpu = pool.avail_cpu_pool
            avail_ram = pool.avail_ram_pool

            if avail_cpu <= 0 or avail_ram <= 0:
                break

            # Decide which priorities are allowed to consume beyond the reservation.
            # QUERY may use all remaining resources; INTERACTIVE may use most; BATCH must leave headroom.
            allow_interactive = True
            allow_batch = True

            # If we are near (or under) reservation, stop scheduling lower-priority work.
            if avail_cpu <= reserve_cpu or avail_ram <= reserve_ram:
                allow_batch = False
                allow_interactive = False

            # Slightly more permissive for INTERACTIVE: allow it a bit closer to reserve than batch.
            if allow_interactive is False:
                # keep as-is
                pass
            else:
                # If only slightly above reserve, keep space for QUERY arrivals by blocking batch first.
                if avail_cpu <= int(reserve_cpu * 1.15) or avail_ram <= int(reserve_ram * 1.15):
                    allow_batch = False

            chosen_p = None
            chosen_op = None

            # Always try QUERY first
            chosen_p, chosen_op = _scan_pop_runnable(s.q_query, pool_id)

            # Then INTERACTIVE
            if chosen_p is None and allow_interactive:
                chosen_p, chosen_op = _scan_pop_runnable(s.q_interactive, pool_id)

            # Then BATCH
            if chosen_p is None and allow_batch:
                chosen_p, chosen_op = _scan_pop_runnable(s.q_batch, pool_id)

            if chosen_p is None:
                break

            req_cpu, req_ram = _compute_request(chosen_p, chosen_op, pool_id)

            # Enforce reservation for lower priorities (QUERY can violate reservation).
            eff_prio = _effective_priority(chosen_p)
            if eff_prio != Priority.QUERY:
                if (pool.avail_cpu_pool - req_cpu) < reserve_cpu or (pool.avail_ram_pool - req_ram) < reserve_ram:
                    # Not enough headroom; put it back and stop trying lower-priority in this pool.
                    _queue_for_priority(chosen_p.priority).append(chosen_p)
                    break

            if req_cpu <= 0 or req_ram <= 0:
                _queue_for_priority(chosen_p.priority).append(chosen_p)
                break

            assignments.append(
                Assignment(
                    ops=[chosen_op],
                    cpu=req_cpu,
                    ram=req_ram,
                    priority=chosen_p.priority,  # keep original priority label
                    pool_id=pool_id,
                    pipeline_id=chosen_p.pipeline_id,
                )
            )

            # Requeue pipeline for subsequent ops
            _queue_for_priority(chosen_p.priority).append(chosen_p)

    return suspensions, assignments
