# policy_key: scheduler_est_045
# reasoning_effort: low
# model: gpt-5.2-2025-12-11
# llm_cost: 0.036462
# generation_seconds: 36.76
# generated_at: 2026-04-01T00:14:36.173766
@register_scheduler_init(key="scheduler_est_045")
def scheduler_est_045_init(s):
    """
    Priority-aware FIFO with small, safe improvements over naive FIFO:

    1) Separate waiting queues per priority (QUERY > INTERACTIVE > BATCH).
    2) Memory sizing uses op.estimate.mem_peak_gb as a RAM floor + safety margin.
    3) Basic OOM retry: on likely-OOM failure, increase per-op RAM multiplier and requeue.
    4) Soft reservation: when high-priority work is waiting, avoid consuming the last slice
       of pool resources with BATCH to reduce latency spikes for interactive arrivals.

    Notes:
    - Intentionally avoids preemption because simulator APIs for enumerating running containers
      may not be available uniformly.
    - Schedules at most one operator per pool per tick (low risk; easy to iterate from here).
    """
    # Waiting pipelines grouped by priority; FIFO within each class.
    s.wait_q = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }

    # Per-operator RAM multiplier increased on OOM-like failures.
    # Keyed by id(op) to avoid relying on a specific operator-id attribute.
    s.op_ram_mult = {}

    # Config knobs (kept conservative).
    s.base_ram_safety = 1.20          # RAM = floor * safety
    s.min_ram_gb = 0.25               # never allocate below this if possible
    s.oom_backoff_mult = 1.50         # multiply RAM multiplier per OOM retry
    s.oom_backoff_cap = 16.0          # cap multiplier to avoid runaway
    s.batch_reserve_frac = 0.20       # reserve this fraction for high-priority if waiting

    # CPU targeting per priority (fraction of pool max; bounded by avail at decision time).
    s.cpu_frac = {
        Priority.QUERY: 0.75,
        Priority.INTERACTIVE: 0.50,
        Priority.BATCH_PIPELINE: 0.25,
    }


def _is_likely_oom(err) -> bool:
    if not err:
        return False
    msg = str(err).lower()
    return ("oom" in msg) or ("out of memory" in msg) or ("memoryerror" in msg)


def _ram_floor_gb_for_op(s, op) -> float:
    # Use estimate.mem_peak_gb as a RAM floor if available; otherwise a tiny baseline.
    est = None
    try:
        est = getattr(getattr(op, "estimate", None), "mem_peak_gb", None)
    except Exception:
        est = None

    floor = float(est) if est is not None else s.min_ram_gb
    floor = max(floor, s.min_ram_gb)

    mult = s.op_ram_mult.get(id(op), 1.0)
    floor = floor * mult

    # Safety margin; extra RAM is "free" (capacity aside) and avoids OOM churn.
    return max(s.min_ram_gb, floor * s.base_ram_safety)


def _get_next_ready_op(pipeline):
    # Return next assignable op whose parents are complete, or None.
    status = pipeline.runtime_status()
    op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
    if not op_list:
        return None
    return op_list[0]


def _pipeline_done_or_hard_failed(pipeline) -> bool:
    status = pipeline.runtime_status()
    if status.is_pipeline_successful():
        return True
    # If any op is FAILED, naive template drops the pipeline. We keep it only when we
    # believe it's an OOM (handled by requeue). Otherwise treat as hard-failed.
    failed_ops = status.get_ops([OperatorState.FAILED], require_parents_complete=False)
    return len(failed_ops) > 0


def _any_high_pri_waiting_with_ready_ops(s) -> bool:
    for pr in (Priority.QUERY, Priority.INTERACTIVE):
        for p in s.wait_q.get(pr, []):
            if _get_next_ready_op(p) is not None and not p.runtime_status().is_pipeline_successful():
                return True
    return False


def _pop_next_pipeline_with_ready_op(q):
    # FIFO scan: pop the earliest pipeline that has a ready op.
    for i, p in enumerate(q):
        if p.runtime_status().is_pipeline_successful():
            continue
        if _get_next_ready_op(p) is not None:
            return q.pop(i)
    return None


@register_scheduler(key="scheduler_est_045")
def scheduler_est_045_scheduler(s, results: list, pipelines: list):
    """
    See init docstring for policy overview.
    """
    # Ingest new pipelines into the appropriate priority queue.
    for p in pipelines:
        s.wait_q[p.priority].append(p)

    # Process results: on OOM-like failures, increase per-op RAM multiplier and requeue pipeline.
    # On non-OOM failures, drop (treat as hard failure to avoid infinite retries).
    if results:
        for r in results:
            if r.failed():
                if _is_likely_oom(r.error):
                    # Increase RAM multiplier for the ops that failed.
                    for op in getattr(r, "ops", []) or []:
                        key = id(op)
                        cur = s.op_ram_mult.get(key, 1.0)
                        nxt = min(s.oom_backoff_cap, cur * s.oom_backoff_mult)
                        s.op_ram_mult[key] = nxt
                else:
                    # Non-OOM failure: do not retry.
                    pass

    # If nothing changed, early exit.
    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    # For each pool, attempt to schedule exactly one op (low-risk improvement over naive).
    # Priority order: QUERY > INTERACTIVE > BATCH.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)
        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        high_pri_waiting = _any_high_pri_waiting_with_ready_ops(s)

        # Try to pick a pipeline in priority order.
        chosen_pipeline = None
        chosen_pr = None
        for pr in (Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE):
            q = s.wait_q[pr]
            p = _pop_next_pipeline_with_ready_op(q)
            if p is not None:
                chosen_pipeline = p
                chosen_pr = pr
                break

        if chosen_pipeline is None:
            continue

        # Drop completed pipelines (shouldn't happen due to ready-op filter, but safe).
        if chosen_pipeline.runtime_status().is_pipeline_successful():
            continue

        op = _get_next_ready_op(chosen_pipeline)
        if op is None:
            # No ready op after all; requeue to preserve FIFO and move on.
            s.wait_q[chosen_pr].append(chosen_pipeline)
            continue

        # Soft reservation: if high-priority work is waiting, don't let BATCH consume
        # the last portion of pool resources.
        eff_avail_cpu = avail_cpu
        eff_avail_ram = avail_ram
        if chosen_pr == Priority.BATCH_PIPELINE and high_pri_waiting:
            eff_avail_cpu = max(0.0, avail_cpu - pool.max_cpu_pool * s.batch_reserve_frac)
            eff_avail_ram = max(0.0, avail_ram - pool.max_ram_pool * s.batch_reserve_frac)

        if eff_avail_cpu <= 0 or eff_avail_ram <= 0:
            # Can't schedule batch without violating reservation; requeue batch and skip.
            s.wait_q[chosen_pr].append(chosen_pipeline)
            continue

        # RAM sizing: allocate at least estimated peak (floor) with safety margin.
        ram_need = _ram_floor_gb_for_op(s, op)
        if ram_need > eff_avail_ram:
            # Not enough RAM right now; requeue and move on.
            s.wait_q[chosen_pr].append(chosen_pipeline)
            continue

        # CPU sizing: give more CPU to higher priority (sublinear scaling, but helps latency).
        target_cpu = pool.max_cpu_pool * float(s.cpu_frac.get(chosen_pr, 0.25))
        cpu_alloc = max(1.0, min(eff_avail_cpu, target_cpu))

        assignment = Assignment(
            ops=[op],
            cpu=cpu_alloc,
            ram=min(eff_avail_ram, max(ram_need, s.min_ram_gb)),
            priority=chosen_pipeline.priority,
            pool_id=pool_id,
            pipeline_id=chosen_pipeline.pipeline_id,
        )
        assignments.append(assignment)

        # Requeue the pipeline so its next ops can be scheduled on later ticks.
        s.wait_q[chosen_pr].append(chosen_pipeline)

    return suspensions, assignments
