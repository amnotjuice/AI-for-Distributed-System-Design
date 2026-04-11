# policy_key: scheduler_est_003
# reasoning_effort: low
# exp: estimation
# model: gpt-5.2-2025-12-11
# llm_cost: 0.050442
# generation_seconds: 41.74
# generated_at: 2026-04-10T07:48:15.188379
@register_scheduler_init(key="scheduler_est_003")
def scheduler_est_003_init(s):
    """Memory-aware, priority-biased FIFO with OOM-avoidance and bounded retries.

    Core ideas:
      - Keep separate FIFO queues per priority; always try QUERY > INTERACTIVE > BATCH.
      - For each pool, pick the first pipeline (in that priority order) that has an assignable op
        whose *estimated* peak memory can plausibly fit in that pool.
      - Size RAM using the estimator with a safety margin; if an op previously failed, backoff RAM.
      - Do not drop failed pipelines immediately; retry failed ops a small number of times to avoid
        the heavy 720s penalty for failure/incompleteness.
      - Keep CPU sizing simple and priority-biased (more CPU for higher priority).
    """
    from collections import deque

    s.q_query = deque()
    s.q_interactive = deque()
    s.q_batch = deque()

    # Track per-operator retry count (keyed by id(op) to avoid depending on unknown identifiers).
    s.op_retries = {}  # Dict[int, int]

    # Track pipelines we have seen (optional bookkeeping; not required, but useful for sanity).
    s.seen_pipelines = set()

    # Tunables
    s.max_retries_per_op = 3

    # Memory safety margins (multiplicative), by priority.
    s.mem_safety = {
        Priority.QUERY: 1.35,
        Priority.INTERACTIVE: 1.30,
        Priority.BATCH_PIPELINE: 1.20,
    }

    # Additional RAM backoff per retry (multiplicative).
    s.mem_retry_backoff = 1.35

    # Minimum RAM floor (GB) to avoid absurd under-allocation when estimate is missing/small.
    s.min_ram_gb = {
        Priority.QUERY: 2.0,
        Priority.INTERACTIVE: 1.5,
        Priority.BATCH_PIPELINE: 1.0,
    }

    # CPU shares (fraction of pool max), by priority; bounded by available CPU at assignment time.
    s.cpu_share = {
        Priority.QUERY: 1.00,
        Priority.INTERACTIVE: 0.75,
        Priority.BATCH_PIPELINE: 0.50,
    }


@register_scheduler(key="scheduler_est_003")
def scheduler_est_003_scheduler(s, results, pipelines):
    """
    Returns:
      suspensions: we don't actively preempt in this simple policy (empty list).
      assignments: at most one assignment per pool per tick, chosen to protect latency and avoid OOM.
    """
    from collections import deque

    def _enqueue_pipeline(p):
        if p.pipeline_id not in s.seen_pipelines:
            s.seen_pipelines.add(p.pipeline_id)
        if p.priority == Priority.QUERY:
            s.q_query.append(p)
        elif p.priority == Priority.INTERACTIVE:
            s.q_interactive.append(p)
        else:
            s.q_batch.append(p)

    def _deques_in_priority_order():
        # Fixed order matching objective weights: QUERY (10x) > INTERACTIVE (5x) > BATCH (1x).
        return (s.q_query, s.q_interactive, s.q_batch)

    def _pick_first_assignable_op(pipeline):
        status = pipeline.runtime_status()
        # Prefer ops that are ready to run (parents complete).
        ops = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        if not ops:
            return None
        return ops[0]

    def _est_mem_gb(op):
        est = getattr(op, "estimate", None)
        if est is None:
            return None
        return getattr(est, "mem_peak_gb", None)

    def _retry_count(op):
        return s.op_retries.get(id(op), 0)

    def _should_retry_op(op):
        return _retry_count(op) < s.max_retries_per_op

    def _bump_retry(op):
        k = id(op)
        s.op_retries[k] = s.op_retries.get(k, 0) + 1

    def _ram_request_gb(pipeline_priority, op, pool):
        """
        Compute a RAM request based on estimator + safety margin + retry backoff.
        Clamp to [min_floor, pool.max_ram_pool], then later clamp to pool.avail_ram_pool.
        """
        est = _est_mem_gb(op)
        retries = _retry_count(op)

        floor = s.min_ram_gb.get(pipeline_priority, 1.0)
        safety = s.mem_safety.get(pipeline_priority, 1.25)
        backoff = (s.mem_retry_backoff ** retries) if retries > 0 else 1.0

        if est is None:
            # No estimate: be conservative for high-priority; for batch, avoid grabbing everything.
            # Use a fraction of pool capacity but never below the floor.
            frac = 0.70 if pipeline_priority in (Priority.QUERY, Priority.INTERACTIVE) else 0.50
            base = max(floor, pool.max_ram_pool * frac)
        else:
            # Estimator-driven request with modest additive cushion to reduce near-threshold OOMs.
            base = max(floor, est * safety + 0.25)

        req = base * backoff

        # Clamp to the pool's maximum (can't request more than a container can get in this pool).
        if req > pool.max_ram_pool:
            req = pool.max_ram_pool
        return req

    def _cpu_request(pipeline_priority, pool):
        share = s.cpu_share.get(pipeline_priority, 0.5)
        req = max(1.0, pool.max_cpu_pool * share)
        if req > pool.max_cpu_pool:
            req = pool.max_cpu_pool
        return req

    def _op_fits_pool(pipeline_priority, op, pool):
        """
        Decide if op is plausibly feasible in this pool.
        We use estimate to avoid clear mismatches; if estimate is missing, allow it.
        """
        est = _est_mem_gb(op)
        if est is None:
            return True
        # Use a slightly relaxed feasibility check (est * 1.05) to avoid over-filtering.
        return (est * 1.05) <= pool.max_ram_pool

    def _pipeline_done_or_impossible(pipeline):
        status = pipeline.runtime_status()
        if status.is_pipeline_successful():
            return True

        # If there are FAILED ops, only consider the pipeline impossible if any failed op exceeded retries.
        failed_ops = status.get_ops([OperatorState.FAILED], require_parents_complete=False)
        for op in failed_ops:
            if not _should_retry_op(op):
                return True
        return False

    # Ingest arrivals
    for p in pipelines:
        _enqueue_pipeline(p)

    # Early exit if nothing new to do
    if not pipelines and not results:
        return [], []

    # Update retry counts from results:
    # If an execution failed, increment retry count for involved ops (so future RAM requests back off).
    for r in results:
        if getattr(r, "failed", None) and r.failed():
            for op in getattr(r, "ops", []) or []:
                _bump_retry(op)

    suspensions = []
    assignments = []

    # Scheduling loop: at most one assignment per pool per tick (keeps behavior stable and latency-friendly).
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool
        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        chosen_pipeline = None
        chosen_op = None
        chosen_cpu = None
        chosen_ram = None
        chosen_priority = None

        # We will scan each priority queue in order.
        # For each queue, do a bounded scan over its current length to preserve FIFO-ish behavior.
        for q in _deques_in_priority_order():
            if not q:
                continue
            qlen = len(q)

            # Rotate through pipelines to find a feasible/ready one, keeping relative order mostly intact.
            for _ in range(qlen):
                pipeline = q.popleft()

                # Drop completed pipelines or pipelines with exhausted retries.
                if _pipeline_done_or_impossible(pipeline):
                    continue

                status = pipeline.runtime_status()

                # If there are FAILED ops, prefer retrying them (they are in ASSIGNABLE_STATES).
                op = _pick_first_assignable_op(pipeline)
                if op is None:
                    # Not ready; keep it in queue.
                    q.append(pipeline)
                    continue

                # If op failed too many times, treat pipeline as impossible (stop retrying).
                if op in status.get_ops([OperatorState.FAILED], require_parents_complete=False) and not _should_retry_op(op):
                    continue

                # Filter pools that cannot possibly fit estimated memory.
                if not _op_fits_pool(pipeline.priority, op, pool):
                    q.append(pipeline)
                    continue

                # Compute resource request and ensure we can satisfy it now.
                ram_req = _ram_request_gb(pipeline.priority, op, pool)
                cpu_req = _cpu_request(pipeline.priority, pool)

                # Clamp to currently available resources.
                ram_req = min(ram_req, avail_ram)
                cpu_req = min(cpu_req, avail_cpu)

                # Enforce minimum CPU=1.
                if cpu_req < 1.0:
                    q.append(pipeline)
                    continue

                # If we have an estimate and even the clamped RAM is far below estimate, skip to avoid likely OOM.
                est = _est_mem_gb(op)
                if est is not None and ram_req < min(est * 0.95, est - 0.1):
                    q.append(pipeline)
                    continue

                chosen_pipeline = pipeline
                chosen_op = op
                chosen_cpu = cpu_req
                chosen_ram = ram_req
                chosen_priority = pipeline.priority

                # Put the pipeline back at the front (it may have more ops later; keep it warm).
                q.appendleft(pipeline)
                break

            if chosen_pipeline is not None:
                break

        if chosen_pipeline is None:
            continue

        assignment = Assignment(
            ops=[chosen_op],
            cpu=chosen_cpu,
            ram=chosen_ram,
            priority=chosen_priority,
            pool_id=pool_id,
            pipeline_id=chosen_pipeline.pipeline_id,
        )
        assignments.append(assignment)

    return suspensions, assignments

