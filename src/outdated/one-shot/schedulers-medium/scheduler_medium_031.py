# policy_key: scheduler_medium_031
# reasoning_effort: medium
# model: gpt-5.2-2025-12-11
# llm_cost: 0.090621
# generation_seconds: 104.86
# generated_at: 2026-03-14T02:58:20.072078
@register_scheduler_init(key="scheduler_medium_031")
def scheduler_medium_031_init(s):
    """
    Priority-aware, resource-capped scheduler with basic OOM retry.

    Improvements over naive FIFO:
    - Priority ordering: QUERY > INTERACTIVE > BATCH_PIPELINE.
    - Avoid "give whole pool to one op": cap per-op CPU/RAM to allow concurrency.
    - Reserve headroom for high priority to protect tail latency.
    - If an op fails with an OOM-like error, retry it with increased RAM (bounded),
      otherwise treat failures as non-retryable and stop scheduling that pipeline.
    """
    # Per-priority pipeline queues (pipelines stay queued until success or non-retryable failure).
    s.queues = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }

    # Track per-operator retryability / sizing hints using operator object identity.
    # Keyed by id(op) because ExecutionResult exposes ops but may not expose pipeline_id.
    s.retryable_oom_ops = set()        # {id(op)} marked retryable due to OOM
    s.non_retryable_ops = set()        # {id(op)} marked non-retryable due to non-OOM failure
    s.op_retry_count = {}              # id(op) -> int
    s.op_ram_hint = {}                 # id(op) -> float (next RAM to try)

    # If any op in a pipeline is known non-retryable, we drop the pipeline to prevent churn.
    s.dead_pipelines = set()           # {pipeline_id}

    # Parameters (kept intentionally simple / conservative).
    s.max_oom_retries = 2

    # Per-op sizing caps as fractions of pool size by priority (prevents one op monopolizing a pool).
    s.cpu_frac = {
        Priority.QUERY: 0.60,
        Priority.INTERACTIVE: 0.50,
        Priority.BATCH_PIPELINE: 0.30,
    }
    s.ram_frac = {
        Priority.QUERY: 0.60,
        Priority.INTERACTIVE: 0.50,
        Priority.BATCH_PIPELINE: 0.35,
    }

    # Keep headroom for urgent work (QUERY/INTERACTIVE) when placing batch work.
    s.reserve_cpu_frac_for_urgent = 0.25
    s.reserve_ram_frac_for_urgent = 0.25


def _is_oom_error(err) -> bool:
    if not err:
        return False
    e = str(err).lower()
    # Keep matching broad to be robust to differing simulator error strings.
    return ("oom" in e) or ("out of memory" in e) or ("out-of-memory" in e) or ("memoryerror" in e)


def _clamp(x, lo, hi):
    if x < lo:
        return lo
    if x > hi:
        return hi
    return x


def _target_allocation(pool, prio, cpu_frac_map, ram_frac_map, cpu_min=1.0, ram_min=1.0):
    # Compute per-op caps from pool maxima; then clamp to current availability.
    cpu_cap = max(cpu_min, float(pool.max_cpu_pool) * float(cpu_frac_map.get(prio, 0.3)))
    ram_cap = max(ram_min, float(pool.max_ram_pool) * float(ram_frac_map.get(prio, 0.35)))

    cpu = _clamp(cpu_cap, cpu_min, float(pool.avail_cpu_pool))
    ram = _clamp(ram_cap, ram_min, float(pool.avail_ram_pool))
    return cpu, ram


def _pipeline_has_retryable_failed_ops(status, retryable_oom_ops, op_retry_count, max_oom_retries):
    failed_ops = status.get_ops([OperatorState.FAILED], require_parents_complete=True)
    for op in failed_ops:
        oid = id(op)
        if oid in retryable_oom_ops and op_retry_count.get(oid, 0) <= max_oom_retries:
            return True
    return False


def _choose_next_op(status, retryable_oom_ops, non_retryable_ops, op_retry_count, max_oom_retries):
    # Prefer retryable FAILED ops first so OOM recovery happens quickly.
    failed_ops = status.get_ops([OperatorState.FAILED], require_parents_complete=True)
    for op in failed_ops:
        oid = id(op)
        if oid in non_retryable_ops:
            continue
        if oid in retryable_oom_ops and op_retry_count.get(oid, 0) <= max_oom_retries:
            return op

    # Otherwise schedule new work (PENDING).
    pending_ops = status.get_ops([OperatorState.PENDING], require_parents_complete=True)
    if pending_ops:
        # Skip ops we already learned are non-retryable (should be rare for PENDING).
        for op in pending_ops:
            if id(op) not in non_retryable_ops:
                return op
    return None


@register_scheduler(key="scheduler_medium_031")
def scheduler_medium_031(s, results: List["ExecutionResult"], pipelines: List["Pipeline"]) -> Tuple[List["Suspend"], List["Assignment"]]:
    """
    Priority-aware, resource-capped scheduling with basic OOM retry.

    Key behavior:
    - Enqueue new pipelines into per-priority queues.
    - Use results to classify failed ops as retryable (OOM) or non-retryable.
    - Schedule as many ops as fit per pool, but:
        * always schedule QUERY then INTERACTIVE then BATCH,
        * cap per-op resources to avoid monopolizing pools,
        * reserve pool headroom for urgent work before scheduling BATCH.
    """
    # Enqueue new arrivals.
    for p in pipelines:
        s.queues[p.priority].append(p)

    # Update hints / retryability from last tick results.
    if results:
        for r in results:
            if not r.failed():
                continue

            oom = _is_oom_error(getattr(r, "error", None))
            # Increase RAM for OOM failures; mark others non-retryable.
            for op in getattr(r, "ops", []) or []:
                oid = id(op)
                if oom:
                    s.retryable_oom_ops.add(oid)
                    s.op_retry_count[oid] = s.op_retry_count.get(oid, 0) + 1

                    # Exponential backoff on RAM using the RAM that just failed.
                    prev_ram = getattr(r, "ram", None)
                    try:
                        prev_ram = float(prev_ram) if prev_ram is not None else None
                    except Exception:
                        prev_ram = None

                    if prev_ram is not None and prev_ram > 0:
                        # Double with a small additive bump to escape tiny allocations.
                        s.op_ram_hint[oid] = max(s.op_ram_hint.get(oid, 0.0), prev_ram * 2.0 + 0.5)
                else:
                    s.non_retryable_ops.add(oid)

    # Early exit if nothing changed.
    if not pipelines and not results:
        return [], []

    suspensions: List["Suspend"] = []
    assignments: List["Assignment"] = []

    # Local guard: avoid scheduling multiple ops from the same pipeline in one tick.
    scheduled_pipeline_ids = set()

    # Determine if we should reserve headroom for urgent workloads.
    urgent_waiting = (len(s.queues[Priority.QUERY]) > 0) or (len(s.queues[Priority.INTERACTIVE]) > 0)

    # Priority order: protect latency.
    prio_order = [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]

    # We'll rebuild queues as we go (drop completed/dead; keep others).
    new_queues = {Priority.QUERY: [], Priority.INTERACTIVE: [], Priority.BATCH_PIPELINE: []}

    # First, filter out completed pipelines and pipelines already known dead.
    for prio in prio_order:
        for p in s.queues[prio]:
            if p.pipeline_id in s.dead_pipelines:
                continue
            st = p.runtime_status()
            if st.is_pipeline_successful():
                continue
            new_queues[prio].append(p)
    s.queues = new_queues

    # Helper: pop/append round-robin from a priority queue.
    def pop_rr(prio):
        q = s.queues[prio]
        if not q:
            return None
        p = q.pop(0)
        return p

    def push_rr(p):
        s.queues[p.priority].append(p)

    # Schedule per pool, pulling from global priority queues.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]

        # We'll repeatedly place ops until we can't make progress.
        made_progress = True
        while made_progress:
            made_progress = False

            # Stop if effectively no resources.
            if float(pool.avail_cpu_pool) < 1.0 or float(pool.avail_ram_pool) <= 0.0:
                break

            # Try priorities in order.
            for prio in prio_order:
                # Apply reservation only when scheduling batch, and only if urgent is waiting.
                if prio == Priority.BATCH_PIPELINE and urgent_waiting:
                    reserve_cpu = float(pool.max_cpu_pool) * float(s.reserve_cpu_frac_for_urgent)
                    reserve_ram = float(pool.max_ram_pool) * float(s.reserve_ram_frac_for_urgent)
                    if float(pool.avail_cpu_pool) <= reserve_cpu or float(pool.avail_ram_pool) <= reserve_ram:
                        continue

                p = pop_rr(prio)
                if p is None:
                    continue

                # If we already scheduled something from this pipeline this tick, requeue and keep searching.
                if p.pipeline_id in scheduled_pipeline_ids:
                    push_rr(p)
                    continue

                st = p.runtime_status()

                # If the pipeline has failures, only allow continuation if we have a retryable FAILED op ready.
                has_failures = st.state_counts.get(OperatorState.FAILED, 0) > 0
                if has_failures:
                    if not _pipeline_has_retryable_failed_ops(st, s.retryable_oom_ops, s.op_retry_count, s.max_oom_retries):
                        # Treat as dead to avoid endless churn.
                        s.dead_pipelines.add(p.pipeline_id)
                        continue

                op = _choose_next_op(st, s.retryable_oom_ops, s.non_retryable_ops, s.op_retry_count, s.max_oom_retries)
                if op is None:
                    # Nothing runnable right now; keep pipeline queued.
                    push_rr(p)
                    continue

                # Choose per-op CPU/RAM (cap + availability). For OOM-retry ops, use RAM hint.
                cpu_req, ram_req = _target_allocation(pool, prio, s.cpu_frac, s.ram_frac)

                oid = id(op)
                if oid in s.op_ram_hint:
                    # Bound hint by availability; also bound by pool max to avoid runaway.
                    hinted = float(s.op_ram_hint[oid])
                    hinted = min(hinted, float(pool.max_ram_pool))
                    ram_req = _clamp(hinted, 1.0, float(pool.avail_ram_pool))

                # If reservation applies for batch, ensure we don't consume reserved headroom.
                if prio == Priority.BATCH_PIPELINE and urgent_waiting:
                    reserve_cpu = float(pool.max_cpu_pool) * float(s.reserve_cpu_frac_for_urgent)
                    reserve_ram = float(pool.max_ram_pool) * float(s.reserve_ram_frac_for_urgent)
                    cpu_req = min(cpu_req, max(1.0, float(pool.avail_cpu_pool) - reserve_cpu))
                    ram_req = min(ram_req, max(1.0, float(pool.avail_ram_pool) - reserve_ram))

                # If after reservation we can't fit, requeue pipeline and try another.
                if cpu_req < 1.0 or ram_req <= 0.0:
                    push_rr(p)
                    continue

                # Final clamp to not exceed current available.
                cpu_req = min(cpu_req, float(pool.avail_cpu_pool))
                ram_req = min(ram_req, float(pool.avail_ram_pool))

                # Create assignment for a single op to avoid head-of-line blocking.
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

                # Mark scheduled and requeue pipeline for future ops.
                scheduled_pipeline_ids.add(p.pipeline_id)
                push_rr(p)

                made_progress = True
                break  # restart priority loop with updated pool availability

    return suspensions, assignments
