# policy_key: scheduler_none_019
# reasoning_effort: none
# model: gpt-5.2-2025-12-11
# llm_cost: 0.031387
# generation_seconds: 33.28
# generated_at: 2026-03-12T21:33:35.760999
@register_scheduler_init(key="scheduler_none_019")
def scheduler_none_019_init(s):
    """Priority-aware FIFO with minimal, safe improvements over the naive example.

    Improvements (kept intentionally small / robust):
    1) Maintain separate queues per priority (QUERY > INTERACTIVE > BATCH_PIPELINE).
    2) Use results feedback:
       - On OOM/failure: increase a per-pipeline RAM hint (exponential backoff) and requeue.
       - On success: decay RAM hint slowly toward a small baseline to avoid over-allocation.
    3) Limit per-assignment CPU to a per-priority cap to reduce head-of-line blocking and
       improve latency under contention (keeps some CPU headroom for new high-priority work).
    4) Prefer placing higher-priority work into pools with more available headroom.
    """
    # Priority-ordered queues
    s.q_query = []
    s.q_interactive = []
    s.q_batch = []

    # Resource hints keyed by pipeline_id
    s.ram_hint = {}  # bytes/units consistent with executor pools
    s.cpu_hint = {}  # vCPUs

    # Tunables (conservative defaults)
    s.base_ram_frac = 0.10  # start at 10% of pool RAM if no hint
    s.min_ram_frac = 0.05   # never go below 5% of pool RAM
    s.max_ram_frac = 0.90   # never request more than 90% of pool RAM in one go

    # Per-priority CPU caps (fraction of pool max cpu)
    s.cpu_cap_frac = {
        Priority.QUERY: 0.5,
        Priority.INTERACTIVE: 0.75,
        Priority.BATCH_PIPELINE: 1.0,
    }

    # Per-priority minimal CPU request to avoid assigning 0
    s.cpu_min = {
        Priority.QUERY: 1,
        Priority.INTERACTIVE: 1,
        Priority.BATCH_PIPELINE: 1,
    }

    # Backoff/decay factors for RAM hints
    s.ram_backoff = 2.0   # on OOM/failure, multiply RAM hint by this
    s.ram_decay = 0.85    # on success, multiply RAM hint by this (slowly reduce)


def _priority_rank(pri):
    # Higher is more important
    if pri == Priority.QUERY:
        return 3
    if pri == Priority.INTERACTIVE:
        return 2
    return 1


def _enqueue_pipeline(s, p):
    if p.priority == Priority.QUERY:
        s.q_query.append(p)
    elif p.priority == Priority.INTERACTIVE:
        s.q_interactive.append(p)
    else:
        s.q_batch.append(p)


def _pop_next_pipeline(s):
    # Strict priority ordering with FIFO within each class
    if s.q_query:
        return s.q_query.pop(0)
    if s.q_interactive:
        return s.q_interactive.pop(0)
    if s.q_batch:
        return s.q_batch.pop(0)
    return None


def _requeue_pipeline_front(s, p):
    # Put back at the front of its priority queue (helps reduce latency jitter)
    if p.priority == Priority.QUERY:
        s.q_query.insert(0, p)
    elif p.priority == Priority.INTERACTIVE:
        s.q_interactive.insert(0, p)
    else:
        s.q_batch.insert(0, p)


def _requeue_pipeline_back(s, p):
    _enqueue_pipeline(s, p)


def _is_active_or_done(p):
    st = p.runtime_status()
    if st.is_pipeline_successful():
        return True
    # If any failures exist, we still may retry (unlike naive) because failures can be OOM sizing.
    # We will let hints adjust and keep going; if a pipeline is truly unrecoverable, the sim may
    # keep failing, but this is still a "small improvement" baseline.
    return False


def _get_assignable_op(p):
    st = p.runtime_status()
    # Require parents complete to preserve DAG semantics
    op_list = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
    if not op_list:
        return None
    # Only schedule one operator at a time per assignment to reduce latency variance
    return op_list[:1]


def _pool_order_for_priority(s, pri):
    # Prefer pools with most *available* headroom (simple heuristic)
    pool_ids = list(range(s.executor.num_pools))
    pool_ids.sort(
        key=lambda i: (
            s.executor.pools[i].avail_cpu_pool,
            s.executor.pools[i].avail_ram_pool,
            s.executor.pools[i].max_cpu_pool,
            s.executor.pools[i].max_ram_pool,
        ),
        reverse=True,
    )
    return pool_ids


def _ram_request_for_pipeline(s, pipeline_id, pool, pri):
    # Use stored hint if present; otherwise start from fraction of pool RAM.
    hint = s.ram_hint.get(pipeline_id, None)
    if hint is None:
        hint = max(int(pool.max_ram_pool * s.base_ram_frac), 1)

    # Clamp to sensible bounds relative to pool size
    min_ram = max(int(pool.max_ram_pool * s.min_ram_frac), 1)
    max_ram = max(int(pool.max_ram_pool * s.max_ram_frac), 1)
    hint = max(min(hint, max_ram), min_ram)
    return hint


def _cpu_request_for_pipeline(s, pipeline_id, pool, pri):
    # Use hint if present; else pick cap by priority.
    cap_frac = s.cpu_cap_frac.get(pri, 1.0)
    cap = max(int(pool.max_cpu_pool * cap_frac), s.cpu_min.get(pri, 1))
    hint = s.cpu_hint.get(pipeline_id, None)
    if hint is None:
        hint = cap
    # Clamp
    hint = max(min(int(hint), cap), s.cpu_min.get(pri, 1))
    return hint


@register_scheduler(key="scheduler_none_019")
def scheduler_none_019(s, results, pipelines):
    """
    Priority-aware scheduler with conservative resource hinting.

    Admission/selection:
      - Separate FIFO queues per priority; always try QUERY then INTERACTIVE then BATCH.
    Placement:
      - Try pools in descending available headroom order.
    Sizing:
      - CPU capped per priority to improve latency (avoid one big batch grabbing all CPU).
      - RAM uses per-pipeline hint; on failure/oom, backoff RAM and retry later.
    """
    # Enqueue newly arrived pipelines
    for p in pipelines:
        _enqueue_pipeline(s, p)

    # Update hints based on results feedback
    for r in results:
        # Some simulators may return results with multiple ops, but we use pipeline_id-based hints.
        # We infer pipeline_id by scanning ops' pipeline_id if present; otherwise skip.
        # To stay compatible with the example API, we rely on r.ops and assume each op has pipeline_id.
        pipeline_id = None
        try:
            if r.ops and hasattr(r.ops[0], "pipeline_id"):
                pipeline_id = r.ops[0].pipeline_id
        except Exception:
            pipeline_id = None

        if pipeline_id is None:
            continue

        # If failed, assume likely OOM/sizing issue and increase RAM hint.
        if r.failed():
            prev = s.ram_hint.get(pipeline_id, None)
            if prev is None:
                # Start from what was used if available, else a conservative guess.
                prev = r.ram if getattr(r, "ram", None) is not None else 1
            # Exponential backoff, clamped later per pool on request
            s.ram_hint[pipeline_id] = max(int(prev * s.ram_backoff), int(prev) + 1)

            # If CPU was very low, bump it slightly for next time (small nudge)
            prev_cpu = s.cpu_hint.get(pipeline_id, None)
            if prev_cpu is None:
                prev_cpu = r.cpu if getattr(r, "cpu", None) is not None else 1
            s.cpu_hint[pipeline_id] = max(int(prev_cpu), 1)
        else:
            # On success, decay RAM hint toward smaller sizes to improve packing over time.
            if pipeline_id in s.ram_hint:
                decayed = max(int(s.ram_hint[pipeline_id] * s.ram_decay), 1)
                s.ram_hint[pipeline_id] = decayed

    # If nothing new and no results, do nothing
    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    # For now: no preemption (small, safe improvement step)
    # Future improvements could consider suspending batch when QUERY arrives.

    # Attempt to schedule at most one op per pool per tick (like the example), but priority-aware
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        if pool.avail_cpu_pool <= 0 or pool.avail_ram_pool <= 0:
            continue

        # We try to pick the best next pipeline for this pool by scanning priorities.
        # To keep FIFO semantics, we pop then requeue if it can't run here.
        candidate = _pop_next_pipeline(s)
        if candidate is None:
            continue

        # Skip pipelines that are already successful
        if _is_active_or_done(candidate):
            continue

        op_list = _get_assignable_op(candidate)
        if not op_list:
            # Not ready yet, requeue and move on
            _requeue_pipeline_back(s, candidate)
            continue

        pri = candidate.priority
        # Compute requested resources (clamped by pool availability)
        req_cpu = _cpu_request_for_pipeline(s, candidate.pipeline_id, pool, pri)
        req_ram = _ram_request_for_pipeline(s, candidate.pipeline_id, pool, pri)

        cpu = min(req_cpu, pool.avail_cpu_pool)
        ram = min(req_ram, pool.avail_ram_pool)

        # Enforce minimums: if we can't even allocate 1 CPU and some RAM, requeue
        if cpu <= 0 or ram <= 0:
            _requeue_pipeline_front(s, candidate)
            continue

        assignment = Assignment(
            ops=op_list,
            cpu=cpu,
            ram=ram,
            priority=pri,
            pool_id=pool_id,
            pipeline_id=candidate.pipeline_id,
        )
        assignments.append(assignment)

        # Put the pipeline back so future ops can be scheduled once this one completes
        _requeue_pipeline_back(s, candidate)

    return suspensions, assignments
