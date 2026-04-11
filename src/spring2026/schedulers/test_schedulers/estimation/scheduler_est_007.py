# policy_key: scheduler_est_007
# reasoning_effort: low
# exp: estimation
# model: gpt-5.2-2025-12-11
# llm_cost: 0.058982
# generation_seconds: 53.45
# generated_at: 2026-04-10T07:51:55.488108
@register_scheduler_init(key="scheduler_est_007")
def scheduler_est_007_init(s):
    """Memory-aware, priority-first, retry-on-failure scheduler.

    Goals:
    - Minimize weighted latency (query >> interactive >> batch) by prioritizing high-priority work.
    - Reduce OOM/failures by using op.estimate.mem_peak_gb (noisy) to avoid infeasible placements.
    - Improve completion rate via bounded retries with exponential RAM backoff (treat failures as likely OOM).
    - Avoid starvation via simple aging (pipelines waiting longer get a small boost within their class).
    """
    # Priority queues (FIFO within each class; small aging handled by a side counter)
    s.queues = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }

    # Per-pipeline bookkeeping
    s.wait_ticks = {}            # pipeline_id -> ticks waited in queue (increments when queued)
    s.pipeline_failures = {}     # pipeline_id -> total failure count (any failure)
    s.pipeline_seen = set()      # pipeline_ids already admitted at least once

    # Per-operator memory backoff (keyed by id(op) to avoid relying on operator fields)
    s.op_ram_multiplier = {}     # id(op) -> multiplier applied to estimate
    s.op_last_ram_req = {}       # id(op) -> last requested RAM (GB)

    # Tunables
    s.max_retries_per_pipeline = 4
    s.base_overprov = 1.25       # headroom on top of estimate
    s.min_ram_gb = 1.0           # never request less than this
    s.unknown_est_ram_frac = 0.25  # if estimate missing, request this fraction of pool RAM (capped)
    s.max_scan_per_pool = 25     # bound CPU time in scheduler
    s.batch_cpu_cap_frac = 0.60  # leave headroom for higher priority
    s.interactive_cpu_cap_frac = 0.80


def _priority_order_with_aging(s):
    """Return a list of priorities in strict order. Aging is handled within queues (selection)."""
    return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]


def _get_assignable_op(pipeline):
    status = pipeline.runtime_status()
    if status.is_pipeline_successful():
        return None
    # Avoid retrying pipelines with terminal failures in the DAG (policy: keep retrying until cap)
    op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
    if not op_list:
        return None
    return op_list[0]


def _est_mem_gb(op):
    est = getattr(op, "estimate", None)
    if est is None:
        return None
    return getattr(est, "mem_peak_gb", None)


def _compute_ram_request_gb(s, op, pool, pipeline_id):
    """Compute RAM request using estimator + backoff, and cap to pool limits."""
    pool_max = float(pool.max_ram_pool)
    pool_avail = float(pool.avail_ram_pool)

    est_mem = _est_mem_gb(op)
    if est_mem is None:
        # Conservative but not extreme: a small slice of the pool, to reduce OOM risk.
        # We cap it so we don't over-allocate wildly when pool is huge.
        ram = max(s.min_ram_gb, min(pool_max, max(2.0, pool_max * s.unknown_est_ram_frac)))
    else:
        ram = max(s.min_ram_gb, float(est_mem) * s.base_overprov)

    mult = s.op_ram_multiplier.get(id(op), 1.0)
    ram *= float(mult)

    # If we previously requested more for this op, don't regress (helps after near-OOM behavior).
    last = s.op_last_ram_req.get(id(op))
    if last is not None:
        ram = max(ram, float(last))

    # Cap by pool max; for feasibility we also consider current availability.
    ram = min(ram, pool_max)
    # If pool has less available than we want, we may still schedule smaller if it remains plausible:
    # but since we can't know true min, we should not shrink below estimate-driven value too much.
    # Here: if requested > avail, treat as non-fit for this pool (caller handles).
    return ram, est_mem


def _compute_cpu_request(s, pool, priority):
    """Allocate CPU with a cap for lower priority to preserve responsiveness."""
    avail = float(pool.avail_cpu_pool)
    if avail <= 0:
        return 0.0

    max_cpu = float(pool.max_cpu_pool)
    # Default to grabbing as much as available to reduce latency, but cap for lower priorities.
    if priority == Priority.QUERY:
        cap = max_cpu  # allow full pool for queries
    elif priority == Priority.INTERACTIVE:
        cap = max_cpu * float(s.interactive_cpu_cap_frac)
    else:
        cap = max_cpu * float(s.batch_cpu_cap_frac)

    cpu = min(avail, max(1.0, cap))
    return cpu


def _queue_push(s, pipeline):
    pr = pipeline.priority
    if pr not in s.queues:
        # Fallback: treat unknown as batch
        pr = Priority.BATCH_PIPELINE
    s.queues[pr].append(pipeline)
    pid = pipeline.pipeline_id
    s.wait_ticks[pid] = s.wait_ticks.get(pid, 0)


def _queue_remove_all_instances(s, pipeline_id):
    """Best-effort cleanup if a pipeline completes; queues are small, so linear scan is OK."""
    for pr in list(s.queues.keys()):
        q = s.queues[pr]
        if not q:
            continue
        s.queues[pr] = [p for p in q if p.pipeline_id != pipeline_id]


def _pick_candidate_for_pool(s, pool):
    """Pick a pipeline+op that fits this pool, prioritizing higher priorities and aged pipelines."""
    # To avoid starvation within a class, we implement a light "aged FIFO":
    # scan a bounded prefix; pick the one with highest wait_ticks (tie -> earliest in queue).
    for pr in _priority_order_with_aging(s):
        q = s.queues.get(pr, [])
        if not q:
            continue

        best_idx = None
        best_wait = -1

        scan = min(len(q), s.max_scan_per_pool)
        for i in range(scan):
            p = q[i]
            pid = p.pipeline_id

            # Skip already completed pipelines (stale queue entries)
            st = p.runtime_status()
            if st.is_pipeline_successful():
                continue

            # Respect retry cap (avoid infinite churn); still keep it queued if below cap.
            fails = s.pipeline_failures.get(pid, 0)
            if fails > s.max_retries_per_pipeline:
                continue

            op = _get_assignable_op(p)
            if op is None:
                continue

            ram_req, est_mem = _compute_ram_request_gb(s, op, pool, pid)

            # Feasibility check using estimate/hint:
            # - If estimate exists and exceeds pool max -> never schedule here.
            # - If requested RAM exceeds current availability -> doesn't fit right now.
            if est_mem is not None and float(est_mem) > float(pool.max_ram_pool):
                continue
            if ram_req > float(pool.avail_ram_pool):
                continue

            w = s.wait_ticks.get(pid, 0)
            if w > best_wait:
                best_wait = w
                best_idx = i

        if best_idx is not None:
            p = q.pop(best_idx)
            op = _get_assignable_op(p)
            return p, op, pr

    return None, None, None


@register_scheduler(key="scheduler_est_007")
def scheduler_est_007_scheduler(s, results: list, pipelines: list):
    """
    Scheduler loop:
    - Admit arriving pipelines into per-priority queues.
    - Process results: on failure, requeue with increased RAM for the failing ops (bounded retries).
    - For each pool, pick the best fitting (priority + aged) operator that fits memory constraints.
    - Assign one operator per pool per tick to reduce interference and protect tail latency.
    """
    # Admit new pipelines
    for p in pipelines:
        pid = p.pipeline_id
        s.pipeline_seen.add(pid)
        s.pipeline_failures.setdefault(pid, 0)
        s.wait_ticks.setdefault(pid, 0)
        _queue_push(s, p)

    # Process execution results
    for r in results:
        # Increment failure and apply RAM backoff for the involved ops
        if r.failed():
            pid = getattr(r, "pipeline_id", None)
            # pipeline_id is not guaranteed on ExecutionResult per the prompt; we infer from ops if needed.
            # But Assignment includes pipeline_id; results may not. We'll still backoff per-op.
            if pid is not None:
                s.pipeline_failures[pid] = s.pipeline_failures.get(pid, 0) + 1

            # Increase RAM multiplier for the failing operators, assuming likely OOM/under-provisioning.
            for op in getattr(r, "ops", []) or []:
                k = id(op)
                cur = float(s.op_ram_multiplier.get(k, 1.0))
                # Exponential backoff with a modest factor to converge quickly but avoid huge jumps.
                new_mult = min(cur * 1.5, 8.0)
                s.op_ram_multiplier[k] = new_mult
                # Also remember last requested RAM (if present on result), so we don't shrink.
                if getattr(r, "ram", None) is not None:
                    try:
                        s.op_last_ram_req[k] = max(float(s.op_last_ram_req.get(k, 0.0)), float(r.ram))
                    except Exception:
                        pass

        # If a pipeline completed, clean up stale queue entries (best effort).
        # We don't have direct pipeline completion events; rely on re-checking during selection.

    # Aging: increment wait ticks for queued pipelines (simple, ensures eventual progress).
    for pr, q in s.queues.items():
        for p in q:
            pid = p.pipeline_id
            s.wait_ticks[pid] = s.wait_ticks.get(pid, 0) + 1

    # Early exit if nothing changed
    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    # For each pool, schedule at most one op per tick to protect high-priority tail latency.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        if float(pool.avail_cpu_pool) <= 0 or float(pool.avail_ram_pool) <= 0:
            continue

        p, op, pr = _pick_candidate_for_pool(s, pool)
        if p is None or op is None:
            continue

        pid = p.pipeline_id

        # Drop completed pipelines (stale pick) and continue
        st = p.runtime_status()
        if st.is_pipeline_successful():
            _queue_remove_all_instances(s, pid)
            s.wait_ticks.pop(pid, None)
            continue

        # Enforce retry cap: still keep it queued, but don't schedule endlessly if it's failing.
        fails = s.pipeline_failures.get(pid, 0)
        if fails > s.max_retries_per_pipeline:
            # Keep it in queue so it is counted by simulator as incomplete/failed by time limit;
            # but don't consume resources on it further.
            continue

        cpu_req = _compute_cpu_request(s, pool, p.priority)
        ram_req, est_mem = _compute_ram_request_gb(s, op, pool, pid)

        # Final fit check
        if cpu_req <= 0 or ram_req > float(pool.avail_ram_pool) or ram_req <= 0:
            # Put it back to the end of its queue (avoid head-of-line blocking).
            _queue_push(s, p)
            continue

        # Record last RAM request for this operator (avoid shrinking across attempts).
        s.op_last_ram_req[id(op)] = float(ram_req)

        assignment = Assignment(
            ops=[op],
            cpu=cpu_req,
            ram=ram_req,
            priority=p.priority,
            pool_id=pool_id,
            pipeline_id=pid,
        )
        assignments.append(assignment)

        # Reset wait tick for scheduled pipeline to avoid monopolizing via aging.
        s.wait_ticks[pid] = 0

        # Requeue the pipeline so its next operator can be scheduled in future ticks.
        _queue_push(s, p)

    return suspensions, assignments
