# policy_key: scheduler_none_023
# reasoning_effort: none
# model: gpt-5.2-2025-12-11
# llm_cost: 0.034680
# generation_seconds: 28.49
# generated_at: 2026-03-14T01:58:07.862503
@register_scheduler_init(key="scheduler_none_023")
def scheduler_none_023_init(s):
    """Priority-aware, low-churn improvement over naive FIFO.

    Incremental improvements vs. the example FIFO:
      1) Maintain separate queues per priority and always consider higher priority first.
      2) Admit *ready* operators (parents complete) to reduce head-of-line blocking.
      3) Conservative right-sizing: avoid assigning the entire pool to a single op; instead,
         allocate a per-op slice so multiple ops can run concurrently and reduce latency.
      4) Simple OOM backoff: if an op fails, retry it with more RAM next time (bounded),
         without immediately penalizing unrelated work.

    Notes:
      - No preemption yet (keeps churn low). This is intentionally a "small improvement first".
      - Policy is pool-agnostic; it fills each pool with a few assignments per tick.
    """
    # Per-priority pipeline queues (store pipeline_ids for stable ordering + dedupe)
    s.q_query = []
    s.q_interactive = []
    s.q_batch = []

    # pipeline_id -> Pipeline (latest reference)
    s.pipeline_by_id = {}

    # Simple per-(pipeline, op) RAM multiplier updated on OOM. We key by (pipeline_id, op_id)
    # and store a multiplier (>=1.0). Since op identity fields are unknown, we fallback to id(op).
    s.ram_mult = {}

    # Bound for retries
    s.max_ram_mult = 8.0

    # To avoid infinite buildup of duplicates when the same pipeline arrives again
    s.enqueued = set()

    # Tunables for packing
    s.max_assignments_per_pool_per_tick = 4   # keep scheduling responsive
    s.min_cpu_per_assignment = 1.0            # avoid tiny CPU slices
    s.min_ram_per_assignment = 0.25           # "fractional GB" in sim units, if supported


def _prio_rank(priority):
    # Lower is higher priority in sorting
    if priority == Priority.QUERY:
        return 0
    if priority == Priority.INTERACTIVE:
        return 1
    return 2  # Priority.BATCH_PIPELINE and everything else


def _get_queue_for_priority(s, priority):
    if priority == Priority.QUERY:
        return s.q_query
    if priority == Priority.INTERACTIVE:
        return s.q_interactive
    return s.q_batch


def _enqueue_pipeline(s, p):
    s.pipeline_by_id[p.pipeline_id] = p
    if p.pipeline_id in s.enqueued:
        return
    _get_queue_for_priority(s, p.priority).append(p.pipeline_id)
    s.enqueued.add(p.pipeline_id)


def _dequeue_next_runnable_pipeline_id(s):
    # Always pull from highest priority queue first
    for q in (s.q_query, s.q_interactive, s.q_batch):
        while q:
            pid = q.pop(0)
            s.enqueued.discard(pid)
            p = s.pipeline_by_id.get(pid)
            if p is None:
                continue
            st = p.runtime_status()
            # Drop completed pipelines
            if st.is_pipeline_successful():
                continue
            # If pipeline has any failed ops, we still allow retrying those ops (ASSIGNABLE_STATES)
            return pid
    return None


def _requeue_pipeline_id(s, pid):
    p = s.pipeline_by_id.get(pid)
    if p is None:
        return
    if p.pipeline_id in s.enqueued:
        return
    _get_queue_for_priority(s, p.priority).append(pid)
    s.enqueued.add(pid)


def _op_key(pipeline_id, op):
    # We don't know if Operator has a stable id attribute; use hasattr checks and fall back.
    if hasattr(op, "op_id"):
        return (pipeline_id, ("op_id", op.op_id))
    if hasattr(op, "operator_id"):
        return (pipeline_id, ("operator_id", op.operator_id))
    if hasattr(op, "id"):
        return (pipeline_id, ("id", op.id))
    return (pipeline_id, ("pyid", id(op)))


def _estimate_ram_for_op(s, pool, pipeline_id, op, base_ram):
    # Apply OOM-informed multiplier; keep within pool max.
    mult = s.ram_mult.get(_op_key(pipeline_id, op), 1.0)
    ram = base_ram * mult
    if ram < s.min_ram_per_assignment:
        ram = s.min_ram_per_assignment
    if ram > pool.max_ram_pool:
        ram = pool.max_ram_pool
    return ram


def _slice_resources_for_priority(s, pool, priority, avail_cpu, avail_ram):
    # Reserve a "bigger bite" for higher priority to reduce latency.
    # This is intentionally simple and stable.
    if priority == Priority.QUERY:
        cpu_frac, ram_frac = 0.60, 0.60
    elif priority == Priority.INTERACTIVE:
        cpu_frac, ram_frac = 0.50, 0.50
    else:
        cpu_frac, ram_frac = 0.35, 0.35

    cpu = max(s.min_cpu_per_assignment, avail_cpu * cpu_frac)
    ram = max(s.min_ram_per_assignment, avail_ram * ram_frac)

    # Do not exceed availability
    cpu = min(cpu, avail_cpu)
    ram = min(ram, avail_ram)
    return cpu, ram


@register_scheduler(key="scheduler_none_023")
def scheduler_none_023(s, results, pipelines):
    """
    Priority-aware, ready-op-first scheduler with conservative per-op resource slicing.

    Algorithm per tick:
      - Ingest new pipelines into per-priority queues (deduped).
      - Process results: if an op failed with OOM-like error, increase RAM multiplier for that op.
      - For each pool, greedily assign up to K ready operators (parents complete), prioritizing
        QUERY > INTERACTIVE > BATCH, using per-op resource slices rather than the whole pool.
      - Requeue pipelines that still have work.
    """
    # Ingest pipelines
    for p in pipelines:
        _enqueue_pipeline(s, p)

    # Learn from failures (simple OOM backoff)
    for r in results:
        if not r.failed():
            continue
        # Heuristic: treat any failure as potential memory pressure if error mentions OOM
        err = ""
        if hasattr(r, "error") and r.error is not None:
            err = str(r.error).lower()
        oom_like = ("oom" in err) or ("out of memory" in err) or ("memory" in err and "exceed" in err)

        # We need to map back to (pipeline_id, op). We only have ops in the result.
        # We'll bump multiplier for each op in the result if oom-like, else no change.
        if oom_like:
            for op in getattr(r, "ops", []) or []:
                # pipeline_id isn't in ExecutionResult in the provided list; so we can't key by it reliably.
                # Fallback: use the op object id alone by searching all known pipelines and matching by identity.
                # This is best-effort; if not found, we still key with a synthetic pipeline_id of -1.
                found_pid = None
                for pid, p in s.pipeline_by_id.items():
                    # If op object exists in pipeline values, match by identity
                    try:
                        if hasattr(p, "values") and op in p.values:
                            found_pid = pid
                            break
                    except Exception:
                        pass
                if found_pid is None:
                    found_pid = -1
                k = _op_key(found_pid, op)
                cur = s.ram_mult.get(k, 1.0)
                s.ram_mult[k] = min(s.max_ram_mult, cur * 2.0)

    # Early exit
    if not pipelines and not results and not (s.q_query or s.q_interactive or s.q_batch):
        return [], []

    suspensions = []
    assignments = []

    # Schedule across pools
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool
        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        made = 0
        # Try to place a handful of ops per pool to improve latency under contention
        while made < s.max_assignments_per_pool_per_tick and avail_cpu > 0 and avail_ram > 0:
            pid = _dequeue_next_runnable_pipeline_id(s)
            if pid is None:
                break

            p = s.pipeline_by_id.get(pid)
            if p is None:
                continue

            st = p.runtime_status()
            if st.is_pipeline_successful():
                continue

            # Prefer ready ops to avoid blocking on upstream tasks
            op_list = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
            if not op_list:
                # Nothing ready; requeue and move on (don't block the pool)
                _requeue_pipeline_id(s, pid)
                continue

            # One op per assignment (small step improvement; keeps fragmentation low and latency good)
            op = op_list[0]

            # Choose resource slice based on priority and current headroom
            cpu, ram = _slice_resources_for_priority(s, pool, p.priority, avail_cpu, avail_ram)

            # Apply OOM-based RAM backoff (best effort) while respecting availability
            ram = _estimate_ram_for_op(s, pool, p.pipeline_id, op, ram)
            ram = min(ram, avail_ram)

            # If we can't give even minimal resources, stop for this pool
            if cpu <= 0 or ram <= 0:
                _requeue_pipeline_id(s, pid)
                break

            assignments.append(
                Assignment(
                    ops=[op],
                    cpu=cpu,
                    ram=ram,
                    priority=p.priority,
                    pool_id=pool_id,
                    pipeline_id=p.pipeline_id,
                )
            )

            made += 1
            avail_cpu -= cpu
            avail_ram -= ram

            # Requeue pipeline if it still has work (likely yes)
            st2 = p.runtime_status()
            if not st2.is_pipeline_successful():
                _requeue_pipeline_id(s, pid)

    return suspensions, assignments

