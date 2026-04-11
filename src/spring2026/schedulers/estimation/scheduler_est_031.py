# policy_key: scheduler_est_031
# reasoning_effort: low
# model: gpt-5.2-2025-12-11
# llm_cost: 0.038786
# generation_seconds: 39.42
# generated_at: 2026-04-01T00:05:30.483725
@register_scheduler_init(key="scheduler_est_031")
def scheduler_est_031_init(s):
    """Priority-aware, estimate-aware scheduler with conservative right-sizing.

    Incremental improvements over naive FIFO:
      1) Separate waiting queues by priority (QUERY > INTERACTIVE > BATCH).
      2) Schedule *multiple* ready operators per tick per pool (not just one),
         using small CPU quanta to reduce head-of-line blocking.
      3) Use op.estimate.mem_peak_gb (when present) as a RAM *floor* to avoid OOMs;
         on failures, retry a limited number of times with increased RAM multiplier.
      4) Capacity reservation: when high-priority work is waiting, avoid consuming
         the last slice of pool CPU/RAM with low-priority assignments.

    Notes:
      - This policy intentionally avoids preemption to keep it simple and stable.
      - Extra RAM beyond the floor is "free" for performance; we allocate enough
        to reduce OOM risk while still allowing concurrency.
    """
    # Waiting queues per priority class
    s.waiting_query = []
    s.waiting_interactive = []
    s.waiting_batch = []

    # Per-operator retry/ram-bump tracking (keyed by (pipeline_id, id(op)))
    s.op_attempts = {}          # (pid, op_key) -> int
    s.op_ram_multiplier = {}    # (pid, op_key) -> float

    # Limits and tuning knobs
    s.max_attempts_per_op = 3

    # CPU sizing: keep interactive latency low by avoiding "one op grabs all CPU".
    s.cpu_quantum_query = 2.0
    s.cpu_quantum_interactive = 2.0
    s.cpu_quantum_batch = 1.0
    s.cpu_max_per_op_fraction = 0.5  # never allocate more than this fraction of pool CPU to one op

    # RAM sizing
    s.ram_min_default_gb = 1.0
    s.ram_overcommit_factor = 1.25   # allocate a bit above estimate to reduce OOM risk
    s.ram_multiplier_on_fail = 1.6   # ramp up quickly on failures

    # Reservation when high-priority waiting (avoid starvation of QUERY/INTERACTIVE)
    s.reserve_cpu_if_high_waiting = 2.0
    s.reserve_ram_if_high_waiting = 2.0


@register_scheduler(key="scheduler_est_031")
def scheduler_est_031(s, results, pipelines):
    """Scheduler step: enqueue arrivals, process results, assign ready ops by priority."""
    # ---- Helper functions (kept inside for single-file policy submission) ----
    def _enqueue_pipeline(p):
        if p.priority == Priority.QUERY:
            s.waiting_query.append(p)
        elif p.priority == Priority.INTERACTIVE:
            s.waiting_interactive.append(p)
        else:
            s.waiting_batch.append(p)

    def _is_done_or_give_up(p):
        st = p.runtime_status()
        if st.is_pipeline_successful():
            return True

        # If any operator is FAILED, we *may* retry; if too many attempts, give up.
        failed_ops = st.get_ops([OperatorState.FAILED], require_parents_complete=False)
        for op in failed_ops:
            op_key = (p.pipeline_id, id(op))
            attempts = s.op_attempts.get(op_key, 0)
            if attempts >= s.max_attempts_per_op:
                return True
        return False

    def _op_mem_floor_gb(op):
        # Use estimate.mem_peak_gb if present; otherwise default.
        est = None
        if hasattr(op, "estimate") and op.estimate is not None and hasattr(op.estimate, "mem_peak_gb"):
            est = op.estimate.mem_peak_gb
        if est is None:
            return s.ram_min_default_gb
        try:
            est_val = float(est)
        except Exception:
            est_val = s.ram_min_default_gb
        if est_val <= 0:
            est_val = s.ram_min_default_gb
        return est_val

    def _op_ram_request_gb(pipeline_id, op, pool_ram_cap):
        # Apply multiplier on retries; allocate a modest safety margin above estimate.
        op_key = (pipeline_id, id(op))
        mult = s.op_ram_multiplier.get(op_key, 1.0)
        base = _op_mem_floor_gb(op) * s.ram_overcommit_factor
        req = base * mult
        # Clamp to pool capacity; if we can't fit even the estimate, we still cap and let it fail.
        if req > pool_ram_cap:
            req = pool_ram_cap
        if req < s.ram_min_default_gb:
            req = s.ram_min_default_gb
        return req

    def _cpu_quantum_for_priority(pri):
        if pri == Priority.QUERY:
            return s.cpu_quantum_query
        if pri == Priority.INTERACTIVE:
            return s.cpu_quantum_interactive
        return s.cpu_quantum_batch

    def _has_high_priority_waiting():
        # "High priority" here means QUERY or INTERACTIVE has at least one not-done pipeline in queue
        for q in (s.waiting_query, s.waiting_interactive):
            for p in q:
                if not _is_done_or_give_up(p):
                    return True
        return False

    def _pop_next_runnable_from_queue(q):
        """Pop the next pipeline in q that has a runnable op; rotate others to preserve order."""
        requeue = []
        chosen = None
        chosen_op = None

        while q:
            p = q.pop(0)
            if _is_done_or_give_up(p):
                continue

            st = p.runtime_status()
            # Find one runnable operator whose parents are complete (if DAG constrained)
            ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
            if ops:
                chosen = p
                chosen_op = ops[0]
                break
            requeue.append(p)

        # Restore non-chosen pipelines
        if requeue:
            q[:0] = requeue
        return chosen, chosen_op

    # ---- Ingest arrivals ----
    for p in pipelines:
        _enqueue_pipeline(p)

    # ---- Process results: update retry counters and RAM multipliers on failures ----
    # We don't assume error types; any failure triggers RAM bump for the involved operators.
    for r in results:
        if r is None:
            continue
        if hasattr(r, "failed") and callable(r.failed) and r.failed():
            # Bump attempts and RAM multipliers for the ops that were in this container execution.
            # r.ops is expected to be a list of ops executed in the container.
            for op in getattr(r, "ops", []) or []:
                op_key = (getattr(r, "pipeline_id", None), id(op))  # pipeline_id may not exist on ExecutionResult
                # If pipeline_id isn't available on result, fall back to tracking by op id only within this tick
                # but still apply multiplier in a best-effort way.
                if op_key[0] is None:
                    # Use a global-ish key that still lets the op ramp up.
                    op_key = ("unknown", id(op))

                s.op_attempts[op_key] = s.op_attempts.get(op_key, 0) + 1
                s.op_ram_multiplier[op_key] = s.op_ram_multiplier.get(op_key, 1.0) * s.ram_multiplier_on_fail

    # ---- Early exit ----
    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    # ---- Main scheduling loop: per pool, fill with as many ops as we can, by priority ----
    high_waiting = _has_high_priority_waiting()

    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)

        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # Determine reservation amounts (only applied when scheduling low-priority work)
        reserve_cpu = s.reserve_cpu_if_high_waiting if high_waiting else 0.0
        reserve_ram = s.reserve_ram_if_high_waiting if high_waiting else 0.0

        pool_cpu_cap_for_single_op = float(pool.max_cpu_pool) * float(s.cpu_max_per_op_fraction)

        # Try to schedule multiple ops into this pool this tick
        while avail_cpu > 0 and avail_ram > 0:
            # Priority order
            candidate = None
            candidate_op = None
            candidate_queue = None

            # First try QUERY, then INTERACTIVE, then BATCH
            for q in (s.waiting_query, s.waiting_interactive, s.waiting_batch):
                p, op = _pop_next_runnable_from_queue(q)
                if p is not None and op is not None:
                    candidate, candidate_op, candidate_queue = p, op, q
                    break

            if candidate is None:
                break

            pri = candidate.priority

            # Compute CPU request (quantum-based), clamped to available and per-op max fraction of pool
            cpu_q = _cpu_quantum_for_priority(pri)
            cpu_req = min(cpu_q, avail_cpu, pool_cpu_cap_for_single_op)
            if cpu_req <= 0:
                # Can't schedule anything meaningful; put pipeline back and stop for this pool.
                _enqueue_pipeline(candidate)
                break

            # Compute RAM request based on estimate floor and retry multiplier
            ram_req = _op_ram_request_gb(candidate.pipeline_id, candidate_op, float(pool.max_ram_pool))
            ram_req = min(ram_req, avail_ram)

            # If scheduling low-priority while high-priority waits, keep reservation headroom.
            if pri == Priority.BATCH_PIPELINE and high_waiting:
                if (avail_cpu - cpu_req) < reserve_cpu or (avail_ram - ram_req) < reserve_ram:
                    # Don't consume reserved headroom; requeue and stop scheduling batch in this pool.
                    s.waiting_batch.insert(0, candidate)
                    break

            # If we can't fit RAM at all, requeue and stop (pool likely full); avoid busy-loop.
            if ram_req <= 0:
                _enqueue_pipeline(candidate)
                break

            # Assign exactly one operator at a time (atom), allowing concurrency across pipelines.
            assignments.append(
                Assignment(
                    ops=[candidate_op],
                    cpu=cpu_req,
                    ram=ram_req,
                    priority=pri,
                    pool_id=pool_id,
                    pipeline_id=candidate.pipeline_id,
                )
            )

            avail_cpu -= cpu_req
            avail_ram -= ram_req

            # Requeue the pipeline to allow subsequent ops to be scheduled later (DAG constraints apply)
            _enqueue_pipeline(candidate)

    return suspensions, assignments
