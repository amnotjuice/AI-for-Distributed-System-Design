# policy_key: scheduler_none_007
# reasoning_effort: none
# model: gpt-5.2-2025-12-11
# llm_cost: 0.045413
# generation_seconds: 48.90
# generated_at: 2026-04-09T21:50:19.032948
@register_scheduler_init(key="scheduler_none_007")
def scheduler_none_007_init(s):
    """Priority-aware, OOM-adaptive, low-churn scheduler.

    Core ideas:
    - Strictly protect QUERY/INTERACTIVE latency using pool-aware placement + limited preemption.
    - Avoid failures (720s penalty) via per-operator/pipeline RAM learning: on OOM, increase RAM request and retry.
    - Keep throughput reasonable: pack opportunistically, but keep headroom in each pool for high-priority bursts.
    - Prevent starvation: mild aging for BATCH pipelines.
    """
    # FIFO-ish queues per priority (we'll select using priority + aging)
    s.wait_q_query = []
    s.wait_q_interactive = []
    s.wait_q_batch = []

    # Remember pipelines we've seen (avoid double-enqueue)
    s.known_pipeline_ids = set()

    # Adaptive memory estimate per (pipeline_id, op_id) if available; fallback to pipeline-level
    # We don't know operator identifiers for sure; we use stable object identity if needed.
    s.op_ram_hint = {}        # key -> ram
    s.pipeline_ram_hint = {}  # pipeline_id -> ram

    # Track observed "good" cpu per priority class (start conservative)
    s.cpu_hint_by_pri = {
        Priority.QUERY: 2.0,
        Priority.INTERACTIVE: 2.0,
        Priority.BATCH_PIPELINE: 1.0,
    }

    # Headroom fractions reserved per pool (soft): we try not to dip below these for high priorities
    s.reserve_frac_by_pri = {
        Priority.QUERY: 0.20,
        Priority.INTERACTIVE: 0.10,
        Priority.BATCH_PIPELINE: 0.00,
    }

    # Preemption limits to reduce churn
    s.max_preempt_per_tick = 2

    # A tiny aging mechanism to prevent batch starvation
    s.batch_age = {}  # pipeline_id -> ticks waited
    s.ticks = 0

    # Track running containers by pool to enable preemption without needing full executor introspection
    # container_id -> dict(priority=..., pool_id=..., cpu=..., ram=..., pipeline_id=...)
    s.running = {}


@register_scheduler(key="scheduler_none_007")
def scheduler_none_007_scheduler(s, results, pipelines):
    """
    Returns:
      suspensions: list of Suspend(container_id, pool_id) to preempt low-priority work when needed.
      assignments: list of Assignment(...) to launch ready operators.

    Policy steps per tick:
    1) Ingest new pipelines into per-priority queues.
    2) Process execution results:
       - On success: update CPU hints slightly.
       - On failure/OOM: increase RAM hint for affected op/pipeline and requeue pipeline.
       - Maintain running-container bookkeeping.
    3) For each pool, attempt to schedule ready ops:
       - Prefer QUERY > INTERACTIVE > BATCH (with batch aging).
       - Use RAM hint to avoid OOM; allocate only minimal CPU initially, scale with available CPU.
       - If high-priority can't fit, preempt a limited number of batch containers in that pool.
    """
    s.ticks += 1

    # ----------------------------
    # Helper functions (no imports)
    # ----------------------------
    def _pri_weight(pri):
        if pri == Priority.QUERY:
            return 3
        if pri == Priority.INTERACTIVE:
            return 2
        return 1

    def _queue_for_priority(pri):
        if pri == Priority.QUERY:
            return s.wait_q_query
        if pri == Priority.INTERACTIVE:
            return s.wait_q_interactive
        return s.wait_q_batch

    def _enqueue_pipeline(p):
        # Avoid duplicates; keep it simple and safe
        if p.pipeline_id in s.known_pipeline_ids:
            return
        s.known_pipeline_ids.add(p.pipeline_id)
        _queue_for_priority(p.priority).append(p)
        if p.priority == Priority.BATCH_PIPELINE:
            s.batch_age[p.pipeline_id] = 0

    def _is_done_or_failed(p):
        status = p.runtime_status()
        if status.is_pipeline_successful():
            return True
        # If pipeline has any FAILED ops, we will *retry* (not drop) by re-queuing.
        # But if it has failures and no retryable path, we still keep it to minimize 720 penalties.
        return False

    def _get_ready_ops(p, max_ops=1):
        status = p.runtime_status()
        # Require parents complete to respect DAG
        ops = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        if not ops:
            return []
        return ops[:max_ops]

    def _op_key(pipeline_id, op):
        # Prefer stable identifier if present, else object id (deterministic enough within sim run)
        op_id = getattr(op, "op_id", None)
        if op_id is None:
            op_id = getattr(op, "operator_id", None)
        if op_id is None:
            op_id = id(op)
        return (pipeline_id, op_id)

    def _hint_ram_for(p, op, pool):
        # Use most-specific hint; otherwise, be conservative: request a small fraction of pool RAM
        key = _op_key(p.pipeline_id, op)
        if key in s.op_ram_hint:
            return s.op_ram_hint[key]
        if p.pipeline_id in s.pipeline_ram_hint:
            return s.pipeline_ram_hint[p.pipeline_id]
        # Default: modest request to reduce contention but not too low
        # (OOM is costly, but total pool RAM might be large; we start with 20% capped to avail)
        base = max(1.0, 0.20 * pool.max_ram_pool)
        return base

    def _bump_ram_hint(p, op, used_ram, pool):
        # Increase RAM hint after OOM/failure; exponential backoff up to pool max
        key = _op_key(p.pipeline_id, op)
        prev = s.op_ram_hint.get(key, None)
        if prev is None:
            prev = max(1.0, used_ram)
        new_hint = min(pool.max_ram_pool, max(prev * 1.5, used_ram * 2.0, prev + 1.0))
        s.op_ram_hint[key] = new_hint
        # Also bump pipeline-level hint slightly to help subsequent ops
        prev_p = s.pipeline_ram_hint.get(p.pipeline_id, prev)
        s.pipeline_ram_hint[p.pipeline_id] = min(pool.max_ram_pool, max(prev_p, new_hint * 0.75))

    def _clamp(x, lo, hi):
        if x < lo:
            return lo
        if x > hi:
            return hi
        return x

    def _cpu_request(pri, pool):
        # Prefer small CPU for concurrency; let queries take a bit more if possible.
        hint = s.cpu_hint_by_pri.get(pri, 1.0)
        # Cap per-assignment to avoid single-op hogging
        cap = 0.50 * pool.max_cpu_pool if pri != Priority.BATCH_PIPELINE else 0.35 * pool.max_cpu_pool
        return _clamp(hint, 1.0, max(1.0, cap))

    def _should_reserve(pool, pri):
        # Compute soft reserved resources for higher priorities to keep headroom
        frac = s.reserve_frac_by_pri.get(pri, 0.0)
        return pool.max_cpu_pool * frac, pool.max_ram_pool * frac

    def _select_next_pipeline():
        # Choose by priority first; within batch, apply aging.
        # We don't reorder query/interactive; keep FIFO there.
        if s.wait_q_query:
            return s.wait_q_query.pop(0)
        if s.wait_q_interactive:
            return s.wait_q_interactive.pop(0)
        if not s.wait_q_batch:
            return None
        # Pick the oldest batch to avoid starvation (linear scan)
        best_i = 0
        best_age = -1
        for i, p in enumerate(s.wait_q_batch):
            age = s.batch_age.get(p.pipeline_id, 0)
            if age > best_age:
                best_age = age
                best_i = i
        return s.wait_q_batch.pop(best_i)

    def _requeue_pipeline(p):
        # Only requeue if not complete; keep known_pipeline_ids as is
        status = p.runtime_status()
        if status.is_pipeline_successful():
            return
        _queue_for_priority(p.priority).append(p)

    def _age_batches():
        for p in s.wait_q_batch:
            s.batch_age[p.pipeline_id] = s.batch_age.get(p.pipeline_id, 0) + 1

    def _find_preemption_candidate(pool_id):
        # Preempt the lowest priority running container in this pool (batch first, then interactive if needed)
        # Prefer larger RAM consumers (free more headroom).
        best = None
        best_score = None
        for cid, info in s.running.items():
            if info.get("pool_id") != pool_id:
                continue
            pri = info.get("priority")
            if pri == Priority.QUERY:
                continue
            # Score: lower priority first, then largest RAM
            pri_rank = 0 if pri == Priority.BATCH_PIPELINE else 1
            ram = info.get("ram", 0.0)
            score = (pri_rank, -ram)
            if best is None or score < best_score:
                best = (cid, info)
                best_score = score
        return best  # (container_id, info) or None

    # ----------------------------
    # Ingest new pipelines
    # ----------------------------
    for p in pipelines:
        _enqueue_pipeline(p)

    # ----------------------------
    # Process results and update hints / bookkeeping
    # ----------------------------
    if results:
        for r in results:
            # Clear from running bookkeeping if present (container finished or failed)
            if getattr(r, "container_id", None) in s.running:
                s.running.pop(r.container_id, None)

            # Update CPU hint on success: gently move toward used cpu (or keep stable if missing)
            if not r.failed():
                # If op succeeded with certain CPU, keep that as a reasonable hint (EWMA-like)
                pri = r.priority
                used_cpu = getattr(r, "cpu", None)
                if used_cpu is not None and used_cpu > 0:
                    prev = s.cpu_hint_by_pri.get(pri, 1.0)
                    s.cpu_hint_by_pri[pri] = 0.8 * prev + 0.2 * float(used_cpu)
                continue

            # Failure: treat as likely OOM; bump RAM hints for affected ops
            # We don't know error codes; conservative: any failure triggers RAM bump.
            # We'll requeue pipelines naturally because they remain in known set and will be selected again.
            pool = s.executor.pools[r.pool_id]
            used_ram = float(getattr(r, "ram", 1.0))
            # r.ops is list of operators; bump each if possible
            for op in getattr(r, "ops", []) or []:
                pid = getattr(op, "pipeline_id", None)
                # If operator doesn't carry pipeline_id, we can't map perfectly; skip op-level and only do pool-level fallback
                # Most sims keep operators associated to a pipeline via assignment; but we avoid assuming.
                if pid is not None:
                    dummy_pipeline_id = pid
                    # Create a pseudo key using available pipeline_id
                    key = _op_key(dummy_pipeline_id, op)
                    prev = s.op_ram_hint.get(key, used_ram)
                    s.op_ram_hint[key] = min(pool.max_ram_pool, max(prev * 1.5, used_ram * 2.0, prev + 1.0))

    _age_batches()

    # early exit if nothing changed
    if not pipelines and not results and not (s.wait_q_query or s.wait_q_interactive or s.wait_q_batch):
        return [], []

    suspensions = []
    assignments = []
    preempts_used = 0

    # ----------------------------
    # Main scheduling loop per pool
    # ----------------------------
    # We iterate pools and try to place one op at a time to keep fairness across pools.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]

        # Keep scheduling while we have headroom and work
        # Limit per pool per tick to reduce churn and over-commit behaviors
        per_pool_launches = 0
        while per_pool_launches < 2:
            avail_cpu = pool.avail_cpu_pool
            avail_ram = pool.avail_ram_pool
            if avail_cpu <= 0 or avail_ram <= 0:
                break

            # Peek next pipeline candidate (without losing it if not schedulable)
            pipeline = _select_next_pipeline()
            if pipeline is None:
                break

            # Drop if already complete
            if _is_done_or_failed(pipeline) and pipeline.runtime_status().is_pipeline_successful():
                continue

            ready_ops = _get_ready_ops(pipeline, max_ops=1)
            if not ready_ops:
                # Nothing ready now; requeue and move on
                _requeue_pipeline(pipeline)
                continue

            op = ready_ops[0]

            # Soft reservations: for batch, don't consume last reserved headroom intended for higher priorities
            # We'll enforce as: when scheduling batch, keep some CPU/RAM unallocated.
            if pipeline.priority == Priority.BATCH_PIPELINE:
                # Reserve for query+interactive
                reserve_cpu = pool.max_cpu_pool * (s.reserve_frac_by_pri[Priority.QUERY] + s.reserve_frac_by_pri[Priority.INTERACTIVE])
                reserve_ram = pool.max_ram_pool * (s.reserve_frac_by_pri[Priority.QUERY] + s.reserve_frac_by_pri[Priority.INTERACTIVE])
                if avail_cpu <= reserve_cpu or avail_ram <= reserve_ram:
                    # Not enough slack; requeue batch and stop trying batch in this pool this tick
                    _requeue_pipeline(pipeline)
                    break

            # Determine resource request
            req_cpu = min(avail_cpu, _cpu_request(pipeline.priority, pool))
            req_ram = min(avail_ram, _hint_ram_for(pipeline, op, pool))

            # Ensure minimal viability
            req_cpu = max(1.0, req_cpu)
            req_ram = max(1.0, req_ram)

            # If it doesn't fit, try preemption only for high priority
            if req_cpu > avail_cpu or req_ram > avail_ram:
                need_preempt = pipeline.priority in (Priority.QUERY, Priority.INTERACTIVE)
                if need_preempt and preempts_used < s.max_preempt_per_tick:
                    cand = _find_preemption_candidate(pool_id)
                    if cand is not None:
                        cid, info = cand
                        suspensions.append(Suspend(container_id=cid, pool_id=pool_id))
                        preempts_used += 1
                        # Assume resources will be freed next tick; requeue current pipeline and stop in this pool
                        _requeue_pipeline(pipeline)
                        break
                # Otherwise can't place; requeue and stop attempting further placements in this pool this tick
                _requeue_pipeline(pipeline)
                break

            # Create assignment
            assignment = Assignment(
                ops=[op],
                cpu=req_cpu,
                ram=req_ram,
                priority=pipeline.priority,
                pool_id=pool_id,
                pipeline_id=pipeline.pipeline_id,
            )
            assignments.append(assignment)

            # Bookkeeping: we can't know container_id yet, so we update when results arrive.
            per_pool_launches += 1

            # Requeue pipeline to continue later (DAG may have more ops)
            _requeue_pipeline(pipeline)

    return suspensions, assignments
