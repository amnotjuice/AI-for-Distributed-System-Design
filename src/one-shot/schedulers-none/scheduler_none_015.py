# policy_key: scheduler_none_015
# reasoning_effort: none
# model: gpt-5.2-2025-12-11
# llm_cost: 0.029805
# generation_seconds: 36.98
# generated_at: 2026-03-12T21:31:13.955561
@register_scheduler_init(key="scheduler_none_015")
def scheduler_none_015_init(s):
    """Priority-aware, low-churn improvements over naive FIFO.

    Design goals (small, obvious improvements first):
    1) Priority ordering: serve higher priority pipelines first (QUERY > INTERACTIVE > BATCH).
    2) Avoid head-of-line blocking: keep per-priority queues and rotate within a priority for fairness.
    3) Basic OOM backoff: if an op OOMs at (cpu, ram), retry with larger RAM next time (per-op memory hint).
    4) Gentle CPU sizing: don't hand an entire pool to one op; cap CPU share per assignment to reduce interference
       and improve tail latency for interactive/query mixes.

    Notes:
    - We keep the policy conservative: no preemption to avoid churn.
    - We assign at most one operator per pool per tick (like the baseline), but choose better.
    """

    # Per-priority waiting queues (pipelines).
    s.waiting_q = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }

    # Round-robin cursor per priority to prevent one pipeline dominating within same priority.
    s.rr_idx = {
        Priority.QUERY: 0,
        Priority.INTERACTIVE: 0,
        Priority.BATCH_PIPELINE: 0,
    }

    # Per-operator resource hints keyed by (pipeline_id, op_id/op_name fallback).
    # Each value: {"ram": float, "cpu": float}
    s.op_hints = {}

    # Track recently seen pipelines to avoid unbounded duplicates in queues.
    s.enqueued = set()

    # Tuning knobs.
    s.max_cpu_share_per_pool = 0.75   # cap cpu given to a single assignment
    s.min_cpu_per_assignment = 1.0    # keep progress
    s.min_ram_per_assignment = 0.5    # avoid trivially tiny allocations if pool huge
    s.oom_ram_multiplier = 1.5        # increase RAM on OOM
    s.oom_ram_additive = 0.5          # and add a small fixed bump
    s.max_ram_fraction_of_pool = 0.95 # avoid allocating 100% and leaving no headroom
    s.max_scan_per_priority = 8       # bound queue scanning per pool


def _priority_order():
    # Highest first
    return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]


def _op_key(pipeline, op):
    # Best-effort stable key without relying on unknown fields.
    # Prefer op_id if present, else name, else repr(op).
    op_id = getattr(op, "op_id", None)
    if op_id is None:
        op_id = getattr(op, "operator_id", None)
    if op_id is None:
        op_id = getattr(op, "name", None)
    if op_id is None:
        op_id = repr(op)
    return (pipeline.pipeline_id, op_id)


def _is_oom_error(err):
    if err is None:
        return False
    # Heuristic: common markers
    s = str(err).lower()
    return ("oom" in s) or ("out of memory" in s) or ("memory" in s and "alloc" in s)


def _enqueue_pipeline(s, p):
    # Avoid duplicates by pipeline_id; we will re-enqueue after scheduling decision as needed.
    if p.pipeline_id in s.enqueued:
        return
    s.enqueued.add(p.pipeline_id)
    s.waiting_q[p.priority].append(p)


def _dequeue_rr(s, prio):
    q = s.waiting_q[prio]
    if not q:
        return None
    idx = s.rr_idx[prio] % len(q)
    p = q.pop(idx)
    # Keep rr cursor stable for next time (points to next element)
    s.rr_idx[prio] = idx
    # Mark as not enqueued while it's being considered; it can be re-enqueued later.
    s.enqueued.discard(p.pipeline_id)
    return p


def _requeue_pipeline(s, p):
    _enqueue_pipeline(s, p)


def _choose_assignable_op(pipeline):
    status = pipeline.runtime_status()
    # Only schedule ops whose parents are complete to respect DAG dependencies.
    op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
    if not op_list:
        return None
    # Keep it simple: pick first available ready op (could be improved later).
    return op_list[0]


def _size_resources(s, pool, pipeline, op):
    # Determine target RAM/CPU using hints; otherwise, conservative fractions.
    hint = s.op_hints.get(_op_key(pipeline, op), None)

    avail_cpu = pool.avail_cpu_pool
    avail_ram = pool.avail_ram_pool

    # CPU: allocate a capped share; ensure at least min and at most available.
    cpu_cap = max(s.min_cpu_per_assignment, pool.max_cpu_pool * 0.25)
    cpu_target = min(avail_cpu, pool.max_cpu_pool * s.max_cpu_share_per_pool, cpu_cap)
    if hint and hint.get("cpu") is not None:
        cpu_target = min(avail_cpu, hint["cpu"], pool.max_cpu_pool * s.max_cpu_share_per_pool)
        cpu_target = max(s.min_cpu_per_assignment, cpu_target)

    # RAM: allocate with headroom; for high priority give a bit more.
    base_fraction = 0.40
    if pipeline.priority == Priority.QUERY:
        base_fraction = 0.55
    elif pipeline.priority == Priority.INTERACTIVE:
        base_fraction = 0.45
    ram_target = min(avail_ram, pool.max_ram_pool * base_fraction, pool.max_ram_pool * s.max_ram_fraction_of_pool)
    ram_target = max(s.min_ram_per_assignment, ram_target)
    if hint and hint.get("ram") is not None:
        ram_target = min(avail_ram, hint["ram"], pool.max_ram_pool * s.max_ram_fraction_of_pool)
        ram_target = max(s.min_ram_per_assignment, ram_target)

    # If we can't allocate minimally, signal no fit.
    if cpu_target <= 0 or ram_target <= 0:
        return None, None
    if cpu_target > avail_cpu or ram_target > avail_ram:
        return None, None
    return cpu_target, ram_target


@register_scheduler(key="scheduler_none_015")
def scheduler_none_015(s, results, pipelines):
    """
    Priority-aware scheduling with basic OOM-adaptive RAM hints and conservative resource sizing.

    Steps:
    - Ingest new pipelines into per-priority queues.
    - Update per-op hints based on results (OOM -> RAM increase, success -> keep).
    - For each pool: pick the best next pipeline by priority, find its next ready op, size resources, assign.
    - Requeue pipelines that still have runnable work but weren't assigned this tick.

    Returns:
        (suspensions, assignments)
    """
    # Ingest arrivals
    for p in pipelines:
        _enqueue_pipeline(s, p)

    # Update hints from results (focus on OOM)
    for r in results:
        # r.ops is a list of operators (per example Assignment uses ops list)
        if not getattr(r, "ops", None):
            continue
        # We can only hint if we can identify pipeline/op; pipeline_id is on assignment not result,
        # but result contains ops; we best-effort update by matching a single op hint using container sizing.
        # Since pipeline_id isn't available, we only update if op carries pipeline_id; else skip.
        # This keeps code safe across different simulator schemas.
        for op in r.ops:
            pipeline_id = getattr(op, "pipeline_id", None)
            if pipeline_id is None:
                # Can't key reliably; skip
                continue
            fake_pipeline = type("P", (), {"pipeline_id": pipeline_id})  # tiny shim
            key = _op_key(fake_pipeline, op)

            prev = s.op_hints.get(key, {"ram": None, "cpu": None})
            if hasattr(r, "failed") and r.failed() and _is_oom_error(getattr(r, "error", None)):
                # Increase RAM hint; keep CPU the same.
                prev_ram = prev["ram"] if prev["ram"] is not None else getattr(r, "ram", None)
                if prev_ram is None:
                    continue
                new_ram = prev_ram * s.oom_ram_multiplier + s.oom_ram_additive
                s.op_hints[key] = {"ram": new_ram, "cpu": prev.get("cpu", getattr(r, "cpu", None))}
            else:
                # On success, remember last known good sizing lightly (helps stability).
                last_ram = getattr(r, "ram", None)
                last_cpu = getattr(r, "cpu", None)
                if last_ram is not None or last_cpu is not None:
                    s.op_hints[key] = {
                        "ram": last_ram if last_ram is not None else prev.get("ram", None),
                        "cpu": last_cpu if last_cpu is not None else prev.get("cpu", None),
                    }

    # Early exit
    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    # We'll requeue pipelines we pop but cannot/should not schedule now.
    to_requeue = []

    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        if pool.avail_cpu_pool <= 0 or pool.avail_ram_pool <= 0:
            continue

        assigned_this_pool = False

        # Try priorities in order. Within each priority, scan a bounded number for a runnable op.
        for prio in _priority_order():
            if assigned_this_pool:
                break

            scanned = 0
            temp_popped = []
            while scanned < s.max_scan_per_priority:
                p = _dequeue_rr(s, prio)
                if p is None:
                    break
                temp_popped.append(p)
                scanned += 1

                status = p.runtime_status()
                has_failures = status.state_counts[OperatorState.FAILED] > 0
                if status.is_pipeline_successful() or has_failures:
                    # Drop completed/failed pipelines (naive behavior keeps it simple: no retries except op-level FAILED->ASSIGNABLE)
                    continue

                op = _choose_assignable_op(p)
                if op is None:
                    # Not runnable now; keep in system
                    to_requeue.append(p)
                    continue

                cpu, ram = _size_resources(s, pool, p, op)
                if cpu is None or ram is None:
                    # Doesn't fit in this pool right now; keep it waiting (maybe other pool has headroom)
                    to_requeue.append(p)
                    continue

                assignment = Assignment(
                    ops=[op],
                    cpu=cpu,
                    ram=ram,
                    priority=p.priority,
                    pool_id=pool_id,
                    pipeline_id=p.pipeline_id,
                )
                assignments.append(assignment)
                assigned_this_pool = True

                # Requeue the pipeline for its next ops in future ticks.
                to_requeue.append(p)
                break

            # Any popped but not handled yet should be requeued (unless dropped)
            for p in temp_popped:
                # If pipeline already placed into to_requeue, fine; if not, it was dropped (completed/failed)
                pass

    # Requeue pipelines we decided to keep
    for p in to_requeue:
        _requeue_pipeline(s, p)

    return suspensions, assignments
