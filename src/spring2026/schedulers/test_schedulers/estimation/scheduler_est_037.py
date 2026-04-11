# policy_key: scheduler_est_037
# reasoning_effort: low
# exp: estimation
# model: gpt-5.2-2025-12-11
# llm_cost: 0.048216
# generation_seconds: 53.15
# generated_at: 2026-04-10T10:15:26.333202
@register_scheduler_init(key="scheduler_est_037")
def scheduler_est_037_init(s):
    """Priority- and memory-aware best-fit scheduler with cautious RAM sizing.

    Goals:
      - Protect QUERY/INTERACTIVE latency via strict priority ordering.
      - Reduce pipeline failures (720s penalty) by using op.estimate.mem_peak_gb to avoid likely OOM placements.
      - Improve throughput without starving BATCH via simple aging.

    Core ideas:
      1) Maintain per-priority FIFO queues of pipelines (with arrival tick).
      2) For each pool, pick one runnable operator using:
           - strict priority, then aging boost, then smaller estimated memory first.
      3) Placement uses a best-fit heuristic on RAM (place where it "just fits"),
         with a safety multiplier that increases after failures.
      4) Batch uses only a fraction of available resources to keep headroom for bursts of high priority work.
    """
    s.ticks = 0
    s.q_query = []
    s.q_interactive = []
    s.q_batch = []

    # Per-operator adaptive safety factor to reduce repeat OOM/failures.
    # Keyed by Python object identity (id(op)) which is stable for the op lifetime in sim.
    s.op_mem_safety = {}

    # Track arrivals to enable simple aging (avoid starvation).
    s.pipe_arrival_tick = {}

    # Small constants (GB) to avoid pathological tiny allocations.
    s.min_ram_gb = 0.25
    s.min_cpu = 1.0


def _prio_rank(priority):
    # Lower is higher priority
    if priority == Priority.QUERY:
        return 0
    if priority == Priority.INTERACTIVE:
        return 1
    return 2


def _get_queue_for_priority(s, priority):
    if priority == Priority.QUERY:
        return s.q_query
    if priority == Priority.INTERACTIVE:
        return s.q_interactive
    return s.q_batch


def _pipeline_done_or_failed(pipeline):
    st = pipeline.runtime_status()
    if st.is_pipeline_successful():
        return True
    # If any operator failed, treat pipeline as failed (don't retry endlessly).
    # (If the system supports retries via FAILED->ASSIGNABLE, those retries should be explicit;
    # here we bias toward not burning time on repeated failures.)
    if st.state_counts.get(OperatorState.FAILED, 0) > 0:
        return True
    return False


def _next_assignable_ops(pipeline, limit=8):
    st = pipeline.runtime_status()
    # Require parents complete to preserve correctness.
    ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
    if not ops:
        return []
    return ops[:limit]


def _op_est_mem_gb(op):
    est = None
    if hasattr(op, "estimate") and op.estimate is not None:
        est = getattr(op.estimate, "mem_peak_gb", None)
    # Ensure non-negative if estimator misbehaves
    if est is not None and est < 0:
        est = 0.0
    return est


def _mem_request_gb(s, op, pool, priority):
    # Base memory request from estimator (hint), otherwise conservative fraction of pool.
    est = _op_est_mem_gb(op)
    safety = s.op_mem_safety.get(id(op), 1.25)  # default headroom
    if priority == Priority.QUERY:
        base_safety = 1.35
    elif priority == Priority.INTERACTIVE:
        base_safety = 1.30
    else:
        base_safety = 1.20

    # Combine safety factors; cap to avoid requesting absurdly large RAM.
    mult = min(3.5, safety * base_safety)

    if est is None:
        # No estimate: request a moderate amount to avoid OOM, but still allow concurrency.
        # Use 35% of pool max as a conservative default.
        req = 0.35 * pool.max_ram_pool
    else:
        req = est * mult

    # Clamp to sane bounds.
    req = max(s.min_ram_gb, min(req, pool.max_ram_pool))
    return req


def _cpu_request(s, pool, priority, avail_cpu):
    # Favor latency for high priority but keep some ability to run multiple tasks across pools.
    if priority == Priority.QUERY:
        target = max(s.min_cpu, 0.75 * pool.max_cpu_pool)
    elif priority == Priority.INTERACTIVE:
        target = max(s.min_cpu, 0.60 * pool.max_cpu_pool)
    else:
        target = max(s.min_cpu, 0.35 * pool.max_cpu_pool)

    # Never exceed what's available.
    return max(s.min_cpu, min(float(avail_cpu), float(target)))


def _batch_headroom(avail_cpu, avail_ram):
    # Leave headroom for high-priority bursts.
    return 0.65 * avail_cpu, 0.65 * avail_ram


def _score_pipeline_for_pick(s, pipeline):
    # Lower is better (picked earlier).
    # Strict priority first, then aging (older gets a small boost), then FIFO by arrival tick.
    pr = _prio_rank(pipeline.priority)
    arrival = s.pipe_arrival_tick.get(pipeline.pipeline_id, s.ticks)
    age = max(0, s.ticks - arrival)

    # Aging: after ~120 ticks, batch can start competing a bit more.
    # Keep it gentle to preserve query/interactive SLOs.
    aging_bonus = 0.0
    if pipeline.priority == Priority.BATCH_PIPELINE:
        aging_bonus = -min(0.8, age / 150.0)  # negative improves score slightly as it ages
    elif pipeline.priority == Priority.INTERACTIVE:
        aging_bonus = -min(0.4, age / 250.0)
    else:
        aging_bonus = -min(0.2, age / 400.0)

    return (pr, aging_bonus, arrival)


def _clean_queue(s, q):
    # Remove pipelines that are completed/failed.
    kept = []
    for p in q:
        if not _pipeline_done_or_failed(p):
            kept.append(p)
    q[:] = kept


def _all_queues_nonempty(s):
    return bool(s.q_query or s.q_interactive or s.q_batch)


def _iter_candidate_pipelines(s):
    # Merge candidates from all queues in priority+aging order.
    # Limit per queue to keep per-tick overhead low.
    candidates = []
    for q in (s.q_query, s.q_interactive, s.q_batch):
        for p in q[:64]:
            if _pipeline_done_or_failed(p):
                continue
            candidates.append(p)
    candidates.sort(key=lambda p: _score_pipeline_for_pick(s, p))
    return candidates


def _pick_best_op_for_pool(s, pool, candidates, avail_cpu, avail_ram):
    # Return (pipeline, op, cpu, ram) or None.
    best = None

    for p in candidates:
        ops = _next_assignable_ops(p, limit=8)
        if not ops:
            continue

        # Prefer smaller estimated mem ops first within a pipeline to reduce blocking/OOM cascades.
        def op_sort_key(op):
            est = _op_est_mem_gb(op)
            # None treated as large to avoid speculative placement when estimator is missing.
            return (1 if est is None else 0, float("inf") if est is None else est)

        ops.sort(key=op_sort_key)

        for op in ops:
            # Apply batch headroom.
            eff_cpu, eff_ram = avail_cpu, avail_ram
            if p.priority == Priority.BATCH_PIPELINE:
                eff_cpu, eff_ram = _batch_headroom(avail_cpu, avail_ram)

            if eff_cpu <= 0 or eff_ram <= 0:
                continue

            req_ram = _mem_request_gb(s, op, pool, p.priority)

            # Memory-aware feasibility check:
            # - If estimate exists and already exceeds pool max, skip this pool (cannot ever fit).
            est = _op_est_mem_gb(op)
            if est is not None and est > pool.max_ram_pool:
                continue

            # If request doesn't fit available RAM, skip for this pool.
            if req_ram > eff_ram:
                continue

            # CPU request
            req_cpu = _cpu_request(s, pool, p.priority, eff_cpu)
            if req_cpu > eff_cpu:
                continue

            # Best-fit on remaining RAM to reduce fragmentation; bias to high-priority a bit.
            rem_ram = eff_ram - req_ram
            pr = _prio_rank(p.priority)

            # Ranking tuple: prioritize higher pr, then tighter RAM fit, then larger CPU (for latency)
            cand = (pr, rem_ram, -req_cpu)
            if best is None or cand < best[0]:
                best = (cand, p, op, req_cpu, req_ram)

        # For QUERY, don't scan too far if we already found a good fit.
        if best is not None and best[1].priority == Priority.QUERY:
            # If we found a query fit with small rem_ram, accept quickly.
            if best[0][1] <= 0.15 * avail_ram:
                break

    if best is None:
        return None
    _, p, op, cpu, ram = best
    return p, op, cpu, ram


@register_scheduler(key="scheduler_est_037")
def scheduler_est_037_scheduler(s, results, pipelines):
    """
    Per-tick scheduling:
      - Ingest new pipelines into per-priority queues.
      - Update per-operator safety factors on failures to reduce repeat failures.
      - For each pool with resources, pick one best operator to run (memory-aware best-fit).
    """
    # Advance logical time when we have any new information.
    if pipelines or results:
        s.ticks += 1

    # Ingest arrivals.
    for p in pipelines:
        q = _get_queue_for_priority(s, p.priority)
        q.append(p)
        s.pipe_arrival_tick[p.pipeline_id] = s.ticks

    # React to execution results: increase memory safety after failures.
    # We do not attempt container suspension/preemption here (keeps churn low).
    for r in results:
        if r is None:
            continue
        if hasattr(r, "failed") and r.failed():
            # Treat any failure as a reason to be more conservative next time (OOM or not).
            for op in getattr(r, "ops", []) or []:
                k = id(op)
                prev = s.op_mem_safety.get(k, 1.25)
                # Escalate quickly to prevent repeated failures; cap to avoid total starvation.
                s.op_mem_safety[k] = min(4.0, prev * 1.6)

    # Clean completed/failed from queues.
    _clean_queue(s, s.q_query)
    _clean_queue(s, s.q_interactive)
    _clean_queue(s, s.q_batch)

    if not pipelines and not results and not _all_queues_nonempty(s):
        return [], []

    suspensions = []
    assignments = []

    # Build a candidate list once; pools will choose from it.
    candidates = _iter_candidate_pipelines(s)

    # For each pool, schedule at most one assignment per tick (simple + avoids oversubscription).
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)

        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        pick = _pick_best_op_for_pool(s, pool, candidates, avail_cpu, avail_ram)
        if pick is None:
            continue

        p, op, cpu, ram = pick

        # Final guard: don't request more than what's available.
        cpu = min(cpu, avail_cpu)
        ram = min(ram, avail_ram)

        assignment = Assignment(
            ops=[op],
            cpu=cpu,
            ram=ram,
            priority=p.priority,
            pool_id=pool_id,
            pipeline_id=p.pipeline_id,
        )
        assignments.append(assignment)

        # Optimistically update local view for subsequent pools in this tick.
        # (Eudoxia will enforce actual limits; this just reduces over-assign.)
        for i, cand_p in enumerate(candidates):
            if cand_p.pipeline_id == p.pipeline_id:
                # Keep pipeline in candidates; other pools may schedule other independent ops later ticks.
                break

    return suspensions, assignments
