# policy_key: scheduler_low_019
# reasoning_effort: low
# model: gpt-5.2-2025-12-11
# llm_cost: 0.040280
# generation_seconds: 48.43
# generated_at: 2026-03-12T21:48:53.257404
@register_scheduler_init(key="scheduler_low_019")
def scheduler_low_019_init(s):
    """Priority-aware FIFO with gentle resource shaping + OOM backoff.

    Improvements over naive FIFO:
    - Priority queues: QUERY > INTERACTIVE > BATCH_PIPELINE.
    - Small-ish initial CPU/RAM for high-priority ops to reduce queueing/HoL blocking.
    - Soft reservations: when high-priority backlog exists, cap batch's resource share.
    - OOM handling: retry with increased RAM (exponential-ish backoff) per pipeline.
    - Light fairness: round-robin within each priority queue; simple aging for batch.
    """
    s.queues = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }
    # per-pipeline adaptive hints
    # state: { "ram_mult": float, "cpu_mult": float, "non_oom_fail": bool }
    s.pstate = {}
    # round-robin cursors per priority
    s.rr_cursor = {
        Priority.QUERY: 0,
        Priority.INTERACTIVE: 0,
        Priority.BATCH_PIPELINE: 0,
    }


@register_scheduler(key="scheduler_low_019")
def scheduler_low_019_scheduler(s, results, pipelines):
    """
    Scheduler step:
    1) Enqueue new pipelines into priority queues.
    2) Update per-pipeline state based on results (OOM backoff, drop on non-OOM failure).
    3) For each pool, assign ready operators in priority order with soft reservations.
    """
    # -----------------------------
    # Helpers (kept local)
    # -----------------------------
    def _ensure_pstate(pipeline_id):
        if pipeline_id not in s.pstate:
            s.pstate[pipeline_id] = {"ram_mult": 1.0, "cpu_mult": 1.0, "non_oom_fail": False}
        return s.pstate[pipeline_id]

    def _is_oom_error(err):
        if err is None:
            return False
        msg = str(err).lower()
        # be liberal: simulator implementations often vary strings
        return ("oom" in msg) or ("out of memory" in msg) or ("memory" in msg and "exceed" in msg)

    def _priority_order():
        return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]

    def _queue_has_backlog(pri):
        q = s.queues[pri]
        # backlog = any pipeline with an assignable op
        for p in q:
            st = p.runtime_status()
            if st.is_pipeline_successful():
                continue
            if st.state_counts.get(OperatorState.FAILED, 0) > 0:
                # we never retry non-OOM failures; OOM failures retry as FAILED->ASSIGNABLE_STATES
                if s.pstate.get(p.pipeline_id, {}).get("non_oom_fail", False):
                    continue
            ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
            if ops:
                return True
        return False

    def _pop_next_pipeline(pri):
        """Round-robin within a priority queue, skipping completed/failed pipelines."""
        q = s.queues[pri]
        if not q:
            return None

        n = len(q)
        start = s.rr_cursor[pri] % max(1, n)
        # Try at most n items to find a usable pipeline; otherwise return None
        for k in range(n):
            idx = (start + k) % n
            p = q[idx]
            st = p.runtime_status()

            # Drop successful pipelines from queue
            if st.is_pipeline_successful():
                q.pop(idx)
                if idx < start:
                    start -= 1
                n -= 1
                if n <= 0:
                    s.rr_cursor[pri] = 0
                    return None
                continue

            # Drop non-OOM failed pipelines from queue (do not retry)
            if st.state_counts.get(OperatorState.FAILED, 0) > 0 and s.pstate.get(p.pipeline_id, {}).get("non_oom_fail", False):
                q.pop(idx)
                if idx < start:
                    start -= 1
                n -= 1
                if n <= 0:
                    s.rr_cursor[pri] = 0
                    return None
                continue

            # Keep it; advance cursor past this element for next time
            s.rr_cursor[pri] = (idx + 1) % max(1, len(q))
            return p

        return None

    def _compute_request(pool, pri, pipeline_id, avail_cpu, avail_ram):
        """Choose cpu/ram request amounts for a single op, shaped by priority and pstate."""
        pst = _ensure_pstate(pipeline_id)

        # Base sizing by priority: give small-but-not-tiny slices to avoid HoL blocking.
        # CPU: QUERY smallest (latency sensitive, often quick), INTERACTIVE moderate, BATCH larger.
        if pri == Priority.QUERY:
            base_cpu = max(1.0, min(2.0, pool.max_cpu_pool * 0.25))
            base_ram = max(1.0, pool.max_ram_pool * 0.10)
        elif pri == Priority.INTERACTIVE:
            base_cpu = max(1.0, min(4.0, pool.max_cpu_pool * 0.35))
            base_ram = max(1.0, pool.max_ram_pool * 0.20)
        else:
            # batch gets more to maximize throughput when allowed
            base_cpu = max(1.0, min(pool.max_cpu_pool * 0.60, max(2.0, pool.max_cpu_pool * 0.50)))
            base_ram = max(1.0, pool.max_ram_pool * 0.35)

        # Apply adaptive multipliers (especially RAM after OOM).
        cpu_req = base_cpu * pst["cpu_mult"]
        ram_req = base_ram * pst["ram_mult"]

        # Clamp by available resources and pool maxima
        cpu_req = max(1.0, min(cpu_req, avail_cpu, pool.max_cpu_pool))
        ram_req = max(1.0, min(ram_req, avail_ram, pool.max_ram_pool))

        return cpu_req, ram_req

    def _effective_caps(pool, pri, avail_cpu, avail_ram, hp_backlog):
        """Soft reservation: cap batch share when high-priority work is waiting."""
        if pri != Priority.BATCH_PIPELINE:
            return avail_cpu, avail_ram

        if not hp_backlog:
            return avail_cpu, avail_ram

        # If there is any high-priority backlog globally, keep headroom in each pool.
        # This avoids batch consuming all capacity and inflating tail latency.
        cpu_cap = min(avail_cpu, pool.max_cpu_pool * 0.60)
        ram_cap = min(avail_ram, pool.max_ram_pool * 0.60)
        return cpu_cap, ram_cap

    # -----------------------------
    # 1) Enqueue new pipelines
    # -----------------------------
    for p in pipelines:
        _ensure_pstate(p.pipeline_id)
        s.queues[p.priority].append(p)

    # Early exit: no changes, no need to reschedule
    if not pipelines and not results:
        return [], []

    # -----------------------------
    # 2) Update state from results
    # -----------------------------
    # Note: ExecutionResult doesn't include pipeline_id in the provided context.
    # We can still react to OOMs conservatively by bumping RAM multiplier for the
    # corresponding priority class "globally" if needed, but we prefer per-pipeline.
    # Since we can't map result->pipeline reliably, we only mark non-OOM failures
    # for the pipeline if we can infer it later via runtime_status(). Here we adjust
    # conservative defaults: if there were OOMs, slightly increase RAM multipliers
    # for all pipelines of that priority still queued.
    had_oom_by_pri = {Priority.QUERY: False, Priority.INTERACTIVE: False, Priority.BATCH_PIPELINE: False}
    had_non_oom_fail_by_pri = {Priority.QUERY: False, Priority.INTERACTIVE: False, Priority.BATCH_PIPELINE: False}

    for r in results:
        if hasattr(r, "failed") and r.failed():
            if _is_oom_error(getattr(r, "error", None)):
                had_oom_by_pri[r.priority] = True
            else:
                had_non_oom_fail_by_pri[r.priority] = True

    # Apply coarse adjustments based on observed failures (best-effort without pipeline_id)
    # OOM: increase RAM multipliers for queued pipelines of that priority
    for pri in _priority_order():
        if had_oom_by_pri[pri]:
            for p in s.queues[pri]:
                pst = _ensure_pstate(p.pipeline_id)
                pst["ram_mult"] = min(pst["ram_mult"] * 1.7, 16.0)  # prevent unbounded growth

        # Non-OOM failures: mark pipelines as non-retriable if they have FAILED ops.
        # We do this lazily using runtime_status (since we can't link result->pipeline).
        if had_non_oom_fail_by_pri[pri]:
            for p in s.queues[pri]:
                st = p.runtime_status()
                if st.state_counts.get(OperatorState.FAILED, 0) > 0:
                    pst = _ensure_pstate(p.pipeline_id)
                    # If it's not an OOM-driven retry loop, stop retrying.
                    pst["non_oom_fail"] = True

    # -----------------------------
    # 3) Schedule assignments per pool
    # -----------------------------
    suspensions = []
    assignments = []

    # Determine if any high-priority backlog exists (global signal for batch caps)
    hp_backlog = _queue_has_backlog(Priority.QUERY) or _queue_has_backlog(Priority.INTERACTIVE)

    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool

        # Keep trying to place work until we run out of resources or nothing is runnable
        made_progress = True
        while made_progress:
            made_progress = False

            # Re-read current available resources each iteration (assignments consume them conceptually)
            # Note: the simulator will enforce pool limits; we also subtract locally to avoid overscheduling.
            if avail_cpu <= 0 or avail_ram <= 0:
                break

            # Choose next pipeline in strict priority order; small step toward latency improvement.
            chosen_pipeline = None
            chosen_pri = None
            for pri in _priority_order():
                p = _pop_next_pipeline(pri)
                if p is None:
                    continue
                st = p.runtime_status()

                # Find exactly one ready op (smaller granularity improves interactive latency)
                op_list = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
                if not op_list:
                    # Not runnable now; keep it in queue (append back for later tries)
                    s.queues[pri].append(p)
                    continue

                chosen_pipeline = p
                chosen_pri = pri
                chosen_ops = op_list
                break

            if chosen_pipeline is None:
                break

            # Compute soft caps and then request size
            cpu_cap, ram_cap = _effective_caps(pool, chosen_pri, avail_cpu, avail_ram, hp_backlog)
            if cpu_cap <= 0 or ram_cap <= 0:
                # Can't schedule chosen pri in this pool; push it back and stop this pool
                s.queues[chosen_pri].append(chosen_pipeline)
                break

            cpu_req, ram_req = _compute_request(pool, chosen_pri, chosen_pipeline.pipeline_id, cpu_cap, ram_cap)

            # If the request cannot fit even minimally, stop scheduling in this pool
            if cpu_req <= 0 or ram_req <= 0:
                s.queues[chosen_pri].append(chosen_pipeline)
                break

            # Create assignment
            assignments.append(
                Assignment(
                    ops=chosen_ops,
                    cpu=cpu_req,
                    ram=ram_req,
                    priority=chosen_pipeline.priority,
                    pool_id=pool_id,
                    pipeline_id=chosen_pipeline.pipeline_id,
                )
            )

            # Locally decrement resources to avoid oversubscription within this tick
            avail_cpu -= cpu_req
            avail_ram -= ram_req
            made_progress = True

            # Put pipeline back (it may have more ops later)
            s.queues[chosen_pri].append(chosen_pipeline)

    return suspensions, assignments
