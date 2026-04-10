# policy_key: scheduler_est_004
# reasoning_effort: low
# model: gpt-5.2-2025-12-11
# llm_cost: 0.053563
# generation_seconds: 36.55
# generated_at: 2026-04-03T01:07:04.263021
@register_scheduler_init(key="scheduler_est_004")
def scheduler_est_004_init(s):
    """Priority-aware FIFO with small, safe improvements over naive FIFO.

    Main ideas (kept intentionally simple to stay robust):
      1) Maintain per-priority queues and always try to schedule higher priority first.
      2) When high-priority work is waiting, avoid consuming the entire pool with batch
         by leaving a small CPU/RAM reserve.
      3) Use per-operator peak-memory estimates (if provided) to right-size RAM;
         on OOM-like failures, increase RAM for retries via a per-operator multiplier.

    Notes:
      - No explicit preemption here (requires access to running containers beyond the
        minimal interfaces shown). Reserve-based isolation improves tail latency without churn.
      - Schedules at most one operator per pool per tick (like the example) to keep
        the policy incremental and predictable.
    """
    from collections import deque

    # Priority queues
    s.q_query = deque()
    s.q_interactive = deque()
    s.q_batch = deque()

    # Per-operator RAM retry multipliers (grow on OOM-ish failures)
    # Keyed by a stable-ish identifier (operator_id if available, else python id()).
    s.op_ram_mult = {}

    # Conservative initial and growth factors
    s.ram_mult_init = 1.05
    s.ram_mult_growth = 1.6
    s.ram_mult_cap = 16.0

    # Keep a small reserve for high priority when high priority is waiting
    s.reserve_cpu_frac = 0.25
    s.reserve_ram_frac = 0.25

    # Minimal CPU slices for fairness/concurrency
    s.min_cpu_query = 1.0
    s.min_cpu_interactive = 1.0
    s.min_cpu_batch = 1.0


@register_scheduler(key="scheduler_est_004")
def scheduler_est_004_scheduler(s, results, pipelines):
    from collections import deque

    def _q_for_priority(pri):
        if pri == Priority.QUERY:
            return s.q_query
        if pri == Priority.INTERACTIVE:
            return s.q_interactive
        return s.q_batch

    def _has_high_waiting():
        # Any waiting QUERY or INTERACTIVE pipeline (not necessarily runnable right now)
        return (len(s.q_query) > 0) or (len(s.q_interactive) > 0)

    def _op_key(op):
        # Prefer a stable operator id if present; else fall back to python id.
        return getattr(op, "operator_id", None) or getattr(op, "op_id", None) or id(op)

    def _is_oom_error(err):
        if err is None:
            return False
        msg = str(err).lower()
        return ("oom" in msg) or ("out of memory" in msg) or ("killed" in msg and "memory" in msg)

    def _update_from_results(exec_results):
        # If an operator fails with OOM-ish signal, bump its RAM multiplier for retries.
        for r in exec_results:
            try:
                failed = r.failed()
            except Exception:
                failed = False
            if not failed:
                continue

            if not _is_oom_error(getattr(r, "error", None)):
                continue

            # Increase multiplier for all ops reported by this result (usually one)
            for op in getattr(r, "ops", []) or []:
                k = _op_key(op)
                cur = s.op_ram_mult.get(k, s.ram_mult_init)
                nxt = min(s.ram_mult_cap, max(cur, s.ram_mult_init) * s.ram_mult_growth)
                s.op_ram_mult[k] = nxt

    def _pick_next_runnable_op(pipeline):
        status = pipeline.runtime_status()
        if status.is_pipeline_successful():
            return None
        # Only schedule ops whose parents are complete; allow retries of FAILED.
        op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        if not op_list:
            return None
        return op_list[0]

    def _compute_ram_for_op(op, avail_ram, pool_max_ram):
        # Use estimate if available; otherwise allocate a modest chunk of available RAM.
        # Always apply retry multiplier if present.
        est = getattr(op, "estimate", None)
        est_peak = None
        if est is not None:
            est_peak = getattr(est, "mem_peak_gb", None)

        k = _op_key(op)
        mult = s.op_ram_mult.get(k, s.ram_mult_init)

        # Base ask:
        if isinstance(est_peak, (int, float)) and est_peak is not None and est_peak > 0:
            base = float(est_peak) * mult
        else:
            # Fallback: allocate enough to make progress but avoid monopolizing the pool.
            # (This mirrors the naive "use everything" but tempers it for latency.)
            base = max(1.0, min(avail_ram, pool_max_ram * 0.25))

        # Clamp to what we can allocate now.
        return max(0.0, min(float(avail_ram), float(base)))

    def _compute_cpu_for_priority(priority, avail_cpu):
        # Keep it simple: high priority can use more, but never less than 1.
        # Batch is capped to avoid blocking high priority (reserve handles most of this).
        if avail_cpu <= 0:
            return 0.0
        if priority == Priority.QUERY:
            return max(s.min_cpu_query, float(avail_cpu))
        if priority == Priority.INTERACTIVE:
            return max(s.min_cpu_interactive, float(avail_cpu))
        # Batch: don't grab everything by default.
        return max(s.min_cpu_batch, float(avail_cpu) * 0.75)

    # Enqueue new pipelines by priority
    for p in pipelines:
        _q_for_priority(p.priority).append(p)

    # Early exit if nothing changed
    if (not pipelines) and (not results):
        return [], []

    # Update retry multipliers based on failures
    _update_from_results(results)

    suspensions = []
    assignments = []

    # Helper to pop next pipeline from queues with round-robin-like reinsertion.
    # Tries QUERY -> INTERACTIVE -> BATCH.
    def _dequeue_candidate():
        for q in (s.q_query, s.q_interactive, s.q_batch):
            while q:
                p = q.popleft()
                st = p.runtime_status()
                # Drop completed pipelines
                if st.is_pipeline_successful():
                    continue
                return p
        return None

    # Requeue a pipeline at the tail of its priority queue
    def _requeue(p):
        _q_for_priority(p.priority).append(p)

    # Schedule at most one operator per pool per tick (incremental improvement over naive)
    high_waiting = _has_high_waiting()

    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)

        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # If high priority is waiting, keep a reserve by limiting what batch can consume.
        # We implement this by computing "effective available" for batch selection.
        reserve_cpu = float(pool.max_cpu_pool) * float(s.reserve_cpu_frac) if high_waiting else 0.0
        reserve_ram = float(pool.max_ram_pool) * float(s.reserve_ram_frac) if high_waiting else 0.0

        # Peek whether we have any runnable high-priority op; if not, don't reserve.
        # (Avoid wasting capacity due to over-reservation.)
        runnable_high_exists = False
        for q in (s.q_query, s.q_interactive):
            for p in list(q)[:8]:  # small bounded lookahead
                if _pick_next_runnable_op(p) is not None:
                    runnable_high_exists = True
                    break
            if runnable_high_exists:
                break
        if not runnable_high_exists:
            reserve_cpu = 0.0
            reserve_ram = 0.0

        # Try a few candidates to find a runnable op, without infinite looping.
        tries = 0
        candidate = _dequeue_candidate()
        while candidate is not None and tries < 64:
            tries += 1

            op = _pick_next_runnable_op(candidate)
            if op is None:
                # Not runnable right now; requeue and continue searching.
                _requeue(candidate)
                candidate = _dequeue_candidate()
                continue

            # If candidate is batch and high priority might need reserve, cap available.
            eff_avail_cpu = avail_cpu
            eff_avail_ram = avail_ram
            if (candidate.priority == Priority.BATCH_PIPELINE) and (reserve_cpu > 0.0 or reserve_ram > 0.0):
                eff_avail_cpu = max(0.0, avail_cpu - reserve_cpu)
                eff_avail_ram = max(0.0, avail_ram - reserve_ram)

            if eff_avail_cpu <= 0.0 or eff_avail_ram <= 0.0:
                # Can't fit this batch work without violating reserve; keep it queued.
                _requeue(candidate)
                candidate = None
                break

            cpu = _compute_cpu_for_priority(candidate.priority, eff_avail_cpu)
            ram = _compute_ram_for_op(op, eff_avail_ram, float(pool.max_ram_pool))

            # If we couldn't allocate meaningful RAM/CPU, skip
            if cpu <= 0.0 or ram <= 0.0:
                _requeue(candidate)
                candidate = _dequeue_candidate()
                continue

            assignment = Assignment(
                ops=[op],
                cpu=cpu,
                ram=ram,
                priority=candidate.priority,
                pool_id=pool_id,
                pipeline_id=candidate.pipeline_id,
            )
            assignments.append(assignment)

            # Requeue pipeline for subsequent ops after this one is assigned.
            _requeue(candidate)
            break

    return suspensions, assignments
