# policy_key: scheduler_est_016
# reasoning_effort: low
# exp: estimation
# model: gpt-5.2-2025-12-11
# llm_cost: 0.071167
# generation_seconds: 44.12
# generated_at: 2026-04-10T09:04:25.073259
@register_scheduler_init(key="scheduler_est_016")
def scheduler_est_016_init(s):
    """Memory-aware, priority-weighted scheduler with failure-aware RAM backoff.

    Goals:
    - Minimize weighted latency by strongly favoring QUERY and INTERACTIVE pipelines.
    - Reduce OOM-driven failures (720s penalty) by using op.estimate.mem_peak_gb as a hint.
    - Maintain progress for BATCH via mild aging (prevents indefinite starvation).
    - Keep implementation simple: no preemption; one-op-at-a-time per pool; conservative RAM sizing.
    """
    # Per-priority FIFO queues of pipeline ids (actual Pipeline objects stored separately)
    s.q_query = []
    s.q_interactive = []
    s.q_batch = []

    # pipeline_id -> Pipeline
    s.pipelines_by_id = {}

    # pipeline_id -> enqueue tick (for aging/fairness)
    s.enqueued_at = {}

    # pipeline_id -> RAM multiplier (increases after failures; helps avoid repeated OOMs)
    s.ram_mult = {}

    # pipeline_id -> recent failure count
    s.fail_count = {}

    # Monotonic tick counter for aging
    s.tick = 0


@register_scheduler(key="scheduler_est_016")
def scheduler_est_016_scheduler(s, results, pipelines):
    """
    Scheduling strategy:
    1) Enqueue arriving pipelines into priority queues.
    2) Process results to detect failures; on failure, increase that pipeline's RAM multiplier.
    3) For each pool, pick the "best" next (pipeline, op) that fits memory headroom.
       - Best is based on (priority weight + aging bonus) and smaller estimated memory.
    4) Allocate RAM based on estimate * safety_factor * per-pipeline backoff multiplier,
       but never exceed pool availability; allocate CPU based on priority.
    """
    def _prio_weight(p):
        # Higher = more important for the objective.
        if p == Priority.QUERY:
            return 10
        if p == Priority.INTERACTIVE:
            return 5
        return 1

    def _queue_for_priority(p):
        if p == Priority.QUERY:
            return s.q_query
        if p == Priority.INTERACTIVE:
            return s.q_interactive
        return s.q_batch

    def _safe_get_est_mem_gb(op):
        est = getattr(op, "estimate", None)
        if est is None:
            return None
        v = getattr(est, "mem_peak_gb", None)
        if v is None:
            return None
        try:
            v = float(v)
        except Exception:
            return None
        if v != v or v < 0:  # NaN or negative
            return None
        return v

    def _is_pipeline_done_or_failed(pipeline):
        status = pipeline.runtime_status()
        if status.is_pipeline_successful():
            return True
        # If any operator is FAILED, consider the pipeline failed (baseline behavior).
        if status.state_counts[OperatorState.FAILED] > 0:
            return True
        return False

    def _get_assignable_op(pipeline):
        status = pipeline.runtime_status()
        op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        if not op_list:
            return None
        # Keep it simple: pick the first assignable op in topological order.
        return op_list[0]

    def _effective_priority_score(pipeline):
        # Aging for fairness: only meaningfully boosts BATCH after it waits.
        # This prevents complete starvation while still prioritizing query/interactive.
        base = _prio_weight(pipeline.priority)
        waited = max(0, s.tick - s.enqueued_at.get(pipeline.pipeline_id, s.tick))
        if pipeline.priority == Priority.BATCH_PIPELINE:
            # Slow aging; caps out to avoid overtaking interactive too easily.
            bonus = min(3.0, waited / 50.0)  # +0..3 over time
        else:
            bonus = min(1.0, waited / 200.0)  # very mild nudging within same priority
        return base + bonus

    def _cpu_allocation(pool, pipeline_priority, avail_cpu):
        # Favor higher priority with more CPU to reduce latency; keep at least 1 vCPU when possible.
        if avail_cpu <= 0:
            return 0
        if pipeline_priority == Priority.QUERY:
            frac = 0.75
        elif pipeline_priority == Priority.INTERACTIVE:
            frac = 0.55
        else:
            frac = 0.30
        cpu = int(max(1, round(avail_cpu * frac)))
        cpu = min(cpu, int(avail_cpu))
        cpu = max(1, cpu) if avail_cpu >= 1 else 0
        return cpu

    def _ram_request(pool, pipeline_id, op, avail_ram):
        # RAM sizing uses estimator hint; a safety factor; and pipeline backoff after failures.
        # We avoid allocating "all available" by default to keep concurrency possible.
        if avail_ram <= 0:
            return 0

        # If estimate missing, choose a conservative default relative to pool size.
        est_mem = _safe_get_est_mem_gb(op)
        if est_mem is None:
            # Conservative default that still allows multiple tasks in a pool.
            base = max(1.0, min(pool.max_ram_pool * 0.25, avail_ram))
        else:
            # Safety factor to reduce OOM risk under noise; slightly higher for queries.
            if s.pipelines_by_id[pipeline_id].priority == Priority.QUERY:
                safety = 1.45
            elif s.pipelines_by_id[pipeline_id].priority == Priority.INTERACTIVE:
                safety = 1.35
            else:
                safety = 1.25
            base = est_mem * safety

            # Do not request below 1GB when estimate is tiny; avoids overly tight allocations.
            base = max(1.0, base)

        mult = s.ram_mult.get(pipeline_id, 1.0)
        req = base * mult

        # Cap to pool and available RAM.
        req = min(req, pool.max_ram_pool)
        req = min(req, avail_ram)

        # If we ended up with <1GB due to tight pool, still allow minimal if possible.
        if req < 1.0 and avail_ram >= 1.0:
            req = 1.0
        return req

    def _fits_some_pool(op):
        # If estimate exists and exceeds every pool's max RAM (with headroom), don't schedule now.
        est_mem = _safe_get_est_mem_gb(op)
        if est_mem is None:
            return True
        for i in range(s.executor.num_pools):
            pool = s.executor.pools[i]
            # Require a small headroom margin to account for noise.
            if est_mem <= pool.max_ram_pool * 0.97:
                return True
        return False

    # ---- Tick bookkeeping and input ingestion ----
    s.tick += 1

    for p in pipelines:
        s.pipelines_by_id[p.pipeline_id] = p
        if p.pipeline_id not in s.enqueued_at:
            s.enqueued_at[p.pipeline_id] = s.tick
        if p.pipeline_id not in s.ram_mult:
            s.ram_mult[p.pipeline_id] = 1.0
        if p.pipeline_id not in s.fail_count:
            s.fail_count[p.pipeline_id] = 0
        _queue_for_priority(p.priority).append(p.pipeline_id)

    # ---- Process results (failures -> RAM backoff) ----
    for r in results:
        # If an operator failed, increase RAM multiplier for its pipeline to reduce repeats.
        # We treat any failure as "needs more RAM" because the penalty for repeated failures is huge.
        if hasattr(r, "failed") and r.failed():
            pid = getattr(r, "pipeline_id", None)
            # Some simulators may not provide pipeline_id in result; fall back to no-op.
            if pid is not None and pid in s.ram_mult:
                s.fail_count[pid] = s.fail_count.get(pid, 0) + 1
                # Exponential-ish backoff, capped; large enough to meaningfully reduce repeat OOMs.
                # cap at 4x to avoid monopolizing a pool forever.
                s.ram_mult[pid] = min(4.0, s.ram_mult.get(pid, 1.0) * 1.6)

    # Early exit if nothing to do
    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    # ---- Helper to pick best candidate for a given pool ----
    def _pick_best_for_pool(pool):
        # Collect a small number of candidates from each queue (head-biased, but not strictly FIFO).
        # This small lookahead improves packing and completion without heavy complexity.
        candidate_ids = []

        def _take_some(q, k):
            # Return up to k pipeline ids in queue order.
            out = []
            n = min(k, len(q))
            for j in range(n):
                out.append(q[j])
            return out

        candidate_ids.extend(_take_some(s.q_query, 6))
        candidate_ids.extend(_take_some(s.q_interactive, 5))
        candidate_ids.extend(_take_some(s.q_batch, 4))

        best = None  # (score, est_mem, pipeline_id, op)
        for pid in candidate_ids:
            pipeline = s.pipelines_by_id.get(pid)
            if pipeline is None:
                continue
            if _is_pipeline_done_or_failed(pipeline):
                continue
            op = _get_assignable_op(pipeline)
            if op is None:
                continue

            # If the estimator says this op is too big for all pools, don't churn on it.
            if not _fits_some_pool(op):
                continue

            # Pool feasibility check using estimate (hint) and current availability.
            est_mem = _safe_get_est_mem_gb(op)
            if est_mem is not None:
                # Require it to fit with a bit of headroom to reduce immediate OOM.
                # Also incorporate current pipeline backoff multiplier.
                mult = s.ram_mult.get(pid, 1.0)
                if est_mem * 1.10 * mult > pool.avail_ram_pool:
                    continue

            # Compute selection key: higher effective priority first, then smaller memory.
            eff = _effective_priority_score(pipeline)

            # Small bias toward memory-light ops when priority is similar (better throughput).
            mem_key = est_mem if est_mem is not None else (pool.max_ram_pool * 0.30)

            key = (eff, -mem_key)  # maximize eff, and (implicitly) minimize mem by -mem_key
            if best is None or key > best[0]:
                best = (key, mem_key, pid, op)

        if best is None:
            return None
        return best[2], best[3]

    # ---- Main scheduling loop: one assignment per pool per tick ----
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool
        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        picked = _pick_best_for_pool(pool)
        if picked is None:
            continue

        pid, op = picked
        pipeline = s.pipelines_by_id[pid]

        cpu = _cpu_allocation(pool, pipeline.priority, avail_cpu)
        ram = _ram_request(pool, pid, op, avail_ram)

        if cpu <= 0 or ram <= 0:
            continue

        # Make the assignment for this single operator.
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

        # Remove pid from whichever queue it is in to avoid re-considering immediately.
        # Re-enqueue it at the end to preserve pipeline progress without strict head-of-line blocking.
        q = _queue_for_priority(pipeline.priority)
        try:
            q.remove(pid)
            q.append(pid)
        except Exception:
            pass

    # ---- Queue cleanup: remove completed/failed pipelines to avoid growth ----
    def _clean_queue(q):
        out = []
        for pid in q:
            pipeline = s.pipelines_by_id.get(pid)
            if pipeline is None:
                continue
            if _is_pipeline_done_or_failed(pipeline):
                continue
            out.append(pid)
        return out

    s.q_query = _clean_queue(s.q_query)
    s.q_interactive = _clean_queue(s.q_interactive)
    s.q_batch = _clean_queue(s.q_batch)

    return suspensions, assignments
