# policy_key: scheduler_est_006
# reasoning_effort: low
# model: gpt-5.2-2025-12-11
# llm_cost: 0.051806
# generation_seconds: 37.73
# generated_at: 2026-03-31T23:49:02.677520
@register_scheduler_init(key="scheduler_est_006")
def scheduler_est_006_init(s):
    """
    Priority-aware FIFO with conservative right-sizing.

    Improvements over naive FIFO:
      - Separate queues per priority; always try higher-priority work first.
      - Right-size CPU (cap for interactive) and RAM (use op.estimate.mem_peak_gb as a floor).
      - Basic per-operator retry RAM backoff on failures (assume likely OOM/underprovision).
      - Simple pool preference: if multiple pools, steer interactive/query to pool 0, batch to others.
      - Mild fairness via batch "aging" (periodically allow one batch assignment even under pressure).
    """
    s.q_query = []
    s.q_interactive = []
    s.q_batch = []

    # Track per-operator memory multipliers for retries (keyed by python object id of op)
    s.op_mem_mult = {}

    # A simple tick counter to implement mild fairness/aging
    s.tick = 0

    # Tunables (kept intentionally simple)
    s.cpu_cap_query = 4.0
    s.cpu_cap_interactive = 4.0
    s.cpu_cap_batch = None  # None => can take more
    s.min_cpu = 1.0

    # When estimate missing, use a small default RAM request to avoid over-grabbing
    s.default_ram_gb = 2.0
    s.ram_safety_factor = 1.15  # modest headroom above estimate
    s.retry_backoff = 1.5       # multiplier applied on each failure for that op
    s.max_mem_mult = 8.0

    # Every N ticks, allow a batch assignment even if high-priority is waiting (anti-starvation)
    s.batch_fairness_period = 8


@register_scheduler(key="scheduler_est_006")
def scheduler_est_006_scheduler(s, results, pipelines):
    """
    Scheduler step:
      1) Enqueue new pipelines by priority.
      2) Update per-op retry RAM multipliers on failures.
      3) For each pool, greedily assign ready ops with:
         - priority selection (QUERY > INTERACTIVE > BATCH) with mild batch fairness
         - pool preference (if multiple pools)
         - CPU caps by priority and RAM floor using estimates and retry multiplier
    """
    s.tick += 1

    # --- Helpers (kept local, no imports needed) ---
    def _enqueue_pipeline(p):
        if p.priority == Priority.QUERY:
            s.q_query.append(p)
        elif p.priority == Priority.INTERACTIVE:
            s.q_interactive.append(p)
        else:
            s.q_batch.append(p)

    def _pipeline_done_or_failed(p):
        st = p.runtime_status()
        # If any failures exist, we still keep the pipeline since FAILED ops are retryable (ASSIGNABLE)
        # But if the pipeline is "successful", drop it.
        return st.is_pipeline_successful()

    def _next_ready_op(p):
        st = p.runtime_status()
        # Only schedule ops whose parents are complete to respect DAG dependencies
        ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        if not ops:
            return None
        return ops[0]

    def _op_ram_request_gb(op, avail_ram):
        # Use estimate as a floor; extra RAM beyond peak is "free" aside from capacity usage.
        est = None
        try:
            est = op.estimate.mem_peak_gb
        except Exception:
            est = None

        base = s.default_ram_gb if (est is None) else max(float(est) * s.ram_safety_factor, s.default_ram_gb)
        mult = s.op_mem_mult.get(id(op), 1.0)
        req = base * mult

        # Never request more than what's available in the pool right now
        # (If req > avail_ram, we just can't schedule it in this pool this tick.)
        return req

    def _cpu_request(priority, avail_cpu):
        # Keep interactive work from consuming entire pools; batch can be greedy.
        if priority == Priority.QUERY:
            cap = s.cpu_cap_query
        elif priority == Priority.INTERACTIVE:
            cap = s.cpu_cap_interactive
        else:
            cap = s.cpu_cap_batch

        if cap is None:
            return max(s.min_cpu, avail_cpu)  # take all available (greedy) for batch
        return max(s.min_cpu, min(float(cap), avail_cpu))

    def _has_waiting_high_priority():
        # Only counts pipelines that are not already done
        for q in (s.q_query, s.q_interactive):
            for p in q:
                if not _pipeline_done_or_failed(p):
                    return True
        return False

    def _pool_preference_for_priority(priority):
        # If multiple pools exist, steer high-priority to pool 0, batch to pool 1+.
        if s.executor.num_pools <= 1:
            return None  # no preference
        if priority in (Priority.QUERY, Priority.INTERACTIVE):
            return 0
        return 1  # "preferred starting pool" for batch; may still run elsewhere

    def _pick_from_queue(q):
        """
        Pop/rotate until we find a pipeline that:
          - is not completed
          - has a ready op
        Returns (pipeline, op) or (None, None).
        """
        if not q:
            return None, None

        n = len(q)
        for _ in range(n):
            p = q.pop(0)
            if _pipeline_done_or_failed(p):
                # Drop completed pipelines
                continue
            op = _next_ready_op(p)
            if op is None:
                # Not ready yet; rotate to back
                q.append(p)
                continue
            # Found schedulable op; keep pipeline in queue for future ops
            q.append(p)
            return p, op
        return None, None

    def _try_pick_candidate(allow_batch):
        """
        Returns (pipeline, op) in global priority order.
        With allow_batch=False, will not pick from batch queue.
        """
        p, op = _pick_from_queue(s.q_query)
        if op is not None:
            return p, op
        p, op = _pick_from_queue(s.q_interactive)
        if op is not None:
            return p, op
        if allow_batch:
            p, op = _pick_from_queue(s.q_batch)
            if op is not None:
                return p, op
        return None, None

    # --- Enqueue new pipelines ---
    for p in pipelines:
        _enqueue_pipeline(p)

    # Early exit if nothing changed
    if not pipelines and not results:
        return [], []

    # --- Update retry RAM multipliers based on failures ---
    for r in results:
        if r is None:
            continue
        if hasattr(r, "failed") and r.failed():
            # We treat failures as likely underprovisioned RAM in this incremental policy.
            # Increase only for the specific ops that failed.
            for op in getattr(r, "ops", []) or []:
                cur = s.op_mem_mult.get(id(op), 1.0)
                nxt = min(cur * s.retry_backoff, s.max_mem_mult)
                s.op_mem_mult[id(op)] = nxt
        else:
            # On success, very gently decay multiplier back toward 1.0 for the ops that ran.
            for op in getattr(r, "ops", []) or []:
                cur = s.op_mem_mult.get(id(op), 1.0)
                if cur > 1.0:
                    s.op_mem_mult[id(op)] = max(1.0, cur * 0.9)

    suspensions = []
    assignments = []

    # Mild anti-starvation: periodically allow one batch assignment even if high-priority is waiting
    high_waiting = _has_waiting_high_priority()
    allow_batch_despite_high = (s.tick % s.batch_fairness_period == 0)

    # --- Assign ops pool by pool, greedily packing by available CPU/RAM ---
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)

        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # Try to fill the pool with multiple assignments if possible.
        # Note: In this simulator interface, an Assignment can include multiple ops;
        # we keep it to 1 op per assignment to keep sizing precise.
        while avail_cpu > 0 and avail_ram > 0:
            # Determine whether to consider batch right now
            allow_batch = (not high_waiting) or allow_batch_despite_high

            p, op = _try_pick_candidate(allow_batch=allow_batch)
            if op is None:
                break

            preferred_pool = _pool_preference_for_priority(p.priority)
            if preferred_pool is not None:
                # If this pool is not preferred and preferred pool exists,
                # leave capacity here for work that prefers it.
                if p.priority in (Priority.QUERY, Priority.INTERACTIVE) and pool_id != preferred_pool:
                    # Don't run high-priority outside preferred pool unless no other option:
                    # fallback only when pool 0 has no capacity (we don't know it here), so skip for now.
                    # This keeps the policy simple and avoids interference.
                    break
                if p.priority == Priority.BATCH_PIPELINE and pool_id == 0 and s.executor.num_pools > 1:
                    # Keep pool 0 more available for interactive if possible
                    if high_waiting and not allow_batch_despite_high:
                        break

            ram_req = _op_ram_request_gb(op, avail_ram=avail_ram)
            if ram_req > avail_ram:
                # Can't fit this op in this pool right now; try another candidate.
                # To avoid tight loops, we'll stop filling this pool this tick.
                break

            cpu_req = _cpu_request(p.priority, avail_cpu=avail_cpu)
            if cpu_req > avail_cpu:
                # If we can't satisfy even minimum cpu, stop.
                break

            assignments.append(
                Assignment(
                    ops=[op],
                    cpu=cpu_req,
                    ram=ram_req,
                    priority=p.priority,
                    pool_id=pool_id,
                    pipeline_id=p.pipeline_id,
                )
            )

            avail_cpu -= cpu_req
            avail_ram -= ram_req

            # After scheduling one batch op under fairness exception, revert to protecting high priority
            if p.priority == Priority.BATCH_PIPELINE and high_waiting and allow_batch_despite_high:
                allow_batch_despite_high = False

    return suspensions, assignments
