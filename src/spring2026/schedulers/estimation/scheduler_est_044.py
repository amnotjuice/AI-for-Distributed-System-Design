# policy_key: scheduler_est_044
# reasoning_effort: low
# exp: estimation
# model: gpt-5.2-2025-12-11
# llm_cost: 0.053816
# generation_seconds: 48.72
# generated_at: 2026-04-10T10:21:14.840885
@register_scheduler_init(key="scheduler_est_044")
def scheduler_est_044_init(s):
    """
    Memory-aware, priority-protecting scheduler.

    Core ideas:
    - Strictly prefer QUERY then INTERACTIVE then BATCH, but allow progress for BATCH via aging.
    - Use op.estimate.mem_peak_gb as a noisy hint to (a) avoid obviously-infeasible placements and
      (b) right-size RAM with a small safety factor to reduce OOM retries.
    - On observed failures (likely OOM), retry the failed operator with increased RAM request.
    - Pack multiple small ops per tick/pool when possible to improve throughput without starving
      high-priority work.

    Notes:
    - This policy intentionally avoids preemption to reduce churn/wasted work; instead it focuses on
      memory-fit placement and conservative sizing to reduce failures (which are heavily penalized).
    """
    from collections import deque

    # Per-priority waiting queues (pipelines)
    s.q_query = deque()
    s.q_interactive = deque()
    s.q_batch = deque()

    # Pipelines we've seen (for de-dup / retention)
    s.known_pipeline_ids = set()

    # Simple per-pipeline aging to prevent indefinite starvation
    s.pipeline_age = {}  # pipeline_id -> int ticks in system

    # Per-operator RAM override after failures: key by python object id(op)
    s.op_ram_override_gb = {}  # op_key -> float

    # Round-robin pointer for batch fairness among all priorities (aging-based selection)
    s.tick = 0

    # Heuristics knobs
    s.safety_factor_query = 1.25
    s.safety_factor_interactive = 1.20
    s.safety_factor_batch = 1.10

    # Minimum RAM request if we have no estimate; keep conservative to avoid OOM cascades but not too large
    s.default_ram_frac_of_pool = 0.40  # of pool.max_ram_pool if no estimate

    # Upper bound on safety-scaled requests to avoid over-reserving on noisy overestimates
    s.max_overreserve_multiplier = 1.80

    # Backoff multiplier when we observe a failure for an operator (e.g., OOM)
    s.failure_backoff_multiplier = 1.50

    # Cap number of ops we try to start per pool per scheduler tick (avoid thrash)
    s.max_assignments_per_pool_per_tick = 4


@register_scheduler(key="scheduler_est_044")
def scheduler_est_044_scheduler(s, results, pipelines):
    """
    Scheduler step.

    Returns:
      suspensions: [] (this policy does not preempt)
      assignments: list of Assignment objects
    """
    # ---- helpers ----
    def _is_done_or_failed(p):
        st = p.runtime_status()
        if st.is_pipeline_successful():
            return True
        # If pipeline has failures, we *still* keep it for retry, because dropping is heavily penalized.
        return False

    def _enqueue_pipeline(p):
        if p.pipeline_id in s.known_pipeline_ids:
            return
        s.known_pipeline_ids.add(p.pipeline_id)
        s.pipeline_age[p.pipeline_id] = 0
        if p.priority == Priority.QUERY:
            s.q_query.append(p)
        elif p.priority == Priority.INTERACTIVE:
            s.q_interactive.append(p)
        else:
            s.q_batch.append(p)

    def _op_key(op):
        # Use object identity as stable key within the simulation run.
        return id(op)

    def _result_indicates_mem_failure(res):
        # Heuristic: treat any failure as memory-related unless error suggests otherwise.
        # (We bias toward increasing RAM because failures are expensive in the objective.)
        if not res.failed():
            return False
        err = getattr(res, "error", None)
        if err is None:
            return True
        es = str(err).lower()
        if "oom" in es or "out of memory" in es or "memory" in es:
            return True
        # If unknown error, still attempt a RAM backoff once.
        return True

    def _get_assignable_op(p):
        st = p.runtime_status()
        # Prefer retrying FAILED ops first if parents are complete; otherwise pick first PENDING op.
        # ASSIGNABLE_STATES includes PENDING and FAILED.
        ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        if not ops:
            return None
        # Small bias: prefer FAILED first (to avoid pipeline stuck / penalty)
        failed_ops = [op for op in ops if getattr(op, "state", None) == OperatorState.FAILED]
        return failed_ops[0] if failed_ops else ops[0]

    def _estimate_mem_gb(op):
        # RAM override from failures takes precedence
        k = _op_key(op)
        if k in s.op_ram_override_gb:
            return float(s.op_ram_override_gb[k])

        est = getattr(op, "estimate", None)
        if est is None:
            return None
        v = getattr(est, "mem_peak_gb", None)
        if v is None:
            return None
        try:
            return float(v)
        except Exception:
            return None

    def _safety_factor_for_priority(prio):
        if prio == Priority.QUERY:
            return s.safety_factor_query
        if prio == Priority.INTERACTIVE:
            return s.safety_factor_interactive
        return s.safety_factor_batch

    def _ram_request_for_op(op, prio, pool):
        """
        Determine a RAM request that tries to avoid OOM while not over-reserving too much.
        """
        est = _estimate_mem_gb(op)
        if est is None:
            # No estimate: pick a conservative fraction of the pool, but not exceeding available.
            base = max(0.5, pool.max_ram_pool * s.default_ram_frac_of_pool)
            return min(base, pool.max_ram_pool)

        # Apply safety factor, but cap to avoid huge reservation on noisy overestimates.
        sf = _safety_factor_for_priority(prio)
        req = est * sf
        req = min(req, est * s.max_overreserve_multiplier)
        # At least a small floor to avoid allocating near-zero due to bad estimates.
        req = max(req, 0.5)
        # Can't request more than pool max.
        req = min(req, pool.max_ram_pool)
        return req

    def _cpu_request_for_priority(prio, pool, avail_cpu):
        # Bias CPU to protect QUERY/INTERACTIVE latency while allowing some concurrency.
        # Keep at least 1 vCPU when possible.
        if avail_cpu <= 0:
            return 0
        if prio == Priority.QUERY:
            target = max(1.0, pool.max_cpu_pool * 0.75)
        elif prio == Priority.INTERACTIVE:
            target = max(1.0, pool.max_cpu_pool * 0.50)
        else:
            target = max(1.0, pool.max_cpu_pool * 0.25)
        return min(avail_cpu, target)

    def _pool_can_fit(pool, cpu_req, ram_req):
        return cpu_req <= pool.avail_cpu_pool and ram_req <= pool.avail_ram_pool and cpu_req > 0 and ram_req > 0

    def _choose_next_pipeline_for_pool(pool):
        """
        Priority-first selection with aging:
        - Always attempt QUERY then INTERACTIVE then BATCH.
        - However, if an older lower-priority pipeline is aging a lot, let it jump ahead occasionally.
        """
        # Aging thresholds: allow older pipelines to bubble up to avoid starvation.
        # (Higher thresholds for lower priorities so we still protect query latency.)
        age_promote_interactive = 30
        age_promote_batch = 60

        # Find the max-aged head candidates
        head_q = s.q_query[0] if s.q_query else None
        head_i = s.q_interactive[0] if s.q_interactive else None
        head_b = s.q_batch[0] if s.q_batch else None

        # Promotion checks (only if queries are absent or blocked later by fit constraints;
        # here we simply provide candidate ordering).
        candidates = []
        if head_q is not None:
            candidates.append(head_q)
        if head_i is not None:
            candidates.append(head_i)
        if head_b is not None:
            candidates.append(head_b)

        # If batch is very old, consider it earlier (but still after query if present).
        if head_b is not None and s.pipeline_age.get(head_b.pipeline_id, 0) >= age_promote_batch:
            # Put batch before interactive if interactive isn't also old enough
            if head_i is None or s.pipeline_age.get(head_i.pipeline_id, 0) < age_promote_interactive:
                return head_b

        # If interactive is old, consider it earlier (but still after query).
        if head_i is not None and s.pipeline_age.get(head_i.pipeline_id, 0) >= age_promote_interactive:
            return head_i

        # Default strict priority
        if head_q is not None:
            return head_q
        if head_i is not None:
            return head_i
        return head_b

    def _rotate_queue_after_pop(p):
        # No-op placeholder; kept for clarity if we later add more complex queue behavior.
        return

    def _pop_pipeline_from_its_queue(p):
        if p.priority == Priority.QUERY:
            return s.q_query.popleft()
        if p.priority == Priority.INTERACTIVE:
            return s.q_interactive.popleft()
        return s.q_batch.popleft()

    def _requeue_pipeline(p):
        # Requeue at tail to avoid head-of-line blocking when it can't be scheduled now.
        if p.priority == Priority.QUERY:
            s.q_query.append(p)
        elif p.priority == Priority.INTERACTIVE:
            s.q_interactive.append(p)
        else:
            s.q_batch.append(p)

    # ---- ingest new pipelines ----
    for p in pipelines:
        _enqueue_pipeline(p)

    # Early exit if nothing changed and no queued work
    if not pipelines and not results and not (s.q_query or s.q_interactive or s.q_batch):
        return [], []

    # ---- update ages ----
    s.tick += 1
    for pid in list(s.pipeline_age.keys()):
        s.pipeline_age[pid] += 1

    # ---- process results (failure-driven RAM backoff) ----
    for res in results:
        if _result_indicates_mem_failure(res):
            # Increase RAM override for ops in this result; future retries will request more RAM.
            # We don't know exact cause, but increasing RAM is the safest way to reduce repeated failures.
            for op in getattr(res, "ops", []) or []:
                k = _op_key(op)
                prev = s.op_ram_override_gb.get(k, None)
                # Use either previous override or observed allocation as baseline
                baseline = None
                try:
                    baseline = float(prev) if prev is not None else float(getattr(res, "ram", 0.0))
                except Exception:
                    baseline = float(prev) if prev is not None else 0.0

                if baseline <= 0:
                    # Fall back to estimate if allocation not available
                    est = _estimate_mem_gb(op)
                    baseline = est if est is not None and est > 0 else 1.0

                new_override = baseline * s.failure_backoff_multiplier
                # Clamp to the max RAM of the pool where it failed (if available), else leave as is.
                try:
                    pool = s.executor.pools[res.pool_id]
                    new_override = min(new_override, pool.max_ram_pool)
                except Exception:
                    pass
                s.op_ram_override_gb[k] = new_override

    # ---- scheduling loop ----
    suspensions = []
    assignments = []

    # We will try to fill each pool with multiple ops, preferring memory-fit and priority.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]

        # Local cap to avoid over-scheduling in one tick
        assigned_here = 0

        while assigned_here < s.max_assignments_per_pool_per_tick:
            if pool.avail_cpu_pool <= 0 or pool.avail_ram_pool <= 0:
                break
            if not (s.q_query or s.q_interactive or s.q_batch):
                break

            # Pick a candidate pipeline (priority first, with limited aging promotion)
            candidate = _choose_next_pipeline_for_pool(pool)
            if candidate is None:
                break

            # Temporarily pop; if can't schedule, we requeue at tail to reduce head-of-line blocking.
            p = _pop_pipeline_from_its_queue(candidate)
            if _is_done_or_failed(p):
                # If done, drop it from active bookkeeping
                s.pipeline_age.pop(p.pipeline_id, None)
                continue

            op = _get_assignable_op(p)
            if op is None:
                # No ready op (parents not complete or pipeline waiting); requeue
                _requeue_pipeline(p)
                continue

            # Compute requests
            ram_req = _ram_request_for_op(op, p.priority, pool)
            cpu_req = _cpu_request_for_priority(p.priority, pool, pool.avail_cpu_pool)

            # If request doesn't fit current pool, try to reduce CPU (but not RAM) to improve fit
            if ram_req <= pool.avail_ram_pool and cpu_req > pool.avail_cpu_pool:
                cpu_req = pool.avail_cpu_pool

            # If still doesn't fit, requeue and try others.
            if not _pool_can_fit(pool, cpu_req, ram_req):
                _requeue_pipeline(p)
                # Avoid infinite loop if *nothing* fits in this pool right now:
                # attempt a bounded number of rotations across queues by breaking after one miss.
                break

            # Assign a single op at a time (keeps work granular and helps tail latency)
            assignment = Assignment(
                ops=[op],
                cpu=cpu_req,
                ram=ram_req,
                priority=p.priority,
                pool_id=pool_id,
                pipeline_id=p.pipeline_id,
            )
            assignments.append(assignment)
            assigned_here += 1

            # Requeue pipeline if it still has work; this also enables some fairness.
            _requeue_pipeline(p)

    return suspensions, assignments
