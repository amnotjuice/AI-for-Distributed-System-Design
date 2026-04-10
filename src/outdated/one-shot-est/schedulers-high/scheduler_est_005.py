# policy_key: scheduler_est_005
# reasoning_effort: high
# model: gpt-5.2-2025-12-11
# llm_cost: 0.102752
# generation_seconds: 117.97
# generated_at: 2026-03-31T18:30:06.124777
@register_scheduler_init(key="scheduler_est_005")
def scheduler_est_005_init(s):
    """Priority-aware FIFO with estimator-based RAM sizing + OOM-aware retries.

    Improvements over the naive FIFO baseline:
      1) Priority queues: always try QUERY, then INTERACTIVE, then BATCH.
      2) Estimator-based RAM sizing: allocate RAM near op.estimate.mem_peak_gb with a safety factor.
      3) OOM retries: if an op fails with an OOM-like error, retry it with progressively more RAM and a small backoff.
      4) Headroom for latency: when higher-priority work is waiting, keep a small per-pool CPU/RAM reserve by limiting BATCH.
    """
    from collections import deque

    # Per-priority waiting queues (pipelines are rotated FIFO-style).
    s.queues = {
        Priority.QUERY: deque(),
        Priority.INTERACTIVE: deque(),
        Priority.BATCH_PIPELINE: deque(),
    }

    # Per-op retry state keyed by id(op) (op objects are stable within a pipeline DAG).
    # Value: {"retries": int, "mem_mult": float, "next_retry_tick": int, "terminal": bool}
    s.op_retry = {}

    # Pipelines we consider terminally failed and should no longer schedule.
    s.dropped_pipeline_ids = set()

    # Discrete scheduler tick for retry backoff.
    s.tick = 0

    # Tuning knobs (kept intentionally simple).
    s.mem_safety = 1.20          # base safety factor on top of estimated peak
    s.mem_bump = 1.60            # multiplicative bump on OOM retry
    s.max_retries = 4            # per op
    s.base_backoff = 1           # ticks; grows exponentially with retries

    # Per-priority CPU "generosity" to reduce latency for higher priority.
    s.cpu_frac = {
        Priority.QUERY: 0.90,
        Priority.INTERACTIVE: 0.75,
        Priority.BATCH_PIPELINE: 0.60,
    }

    # When high-priority work is waiting, BATCH may only use (avail - reserve).
    s.reserve_frac_cpu = 0.25
    s.reserve_frac_ram = 0.25

    # Minimum container sizes to avoid pathological tiny allocations.
    s.min_cpu = 1.0
    s.min_ram_gb = 0.25


def _is_oom_error(err):
    if not err:
        return False
    try:
        e = str(err).lower()
    except Exception:
        return False
    return ("oom" in e) or ("outofmemory" in e) or ("out of memory" in e) or ("memory" in e and "killed" in e)


def _op_est_mem_gb(op):
    est = getattr(op, "estimate", None)
    if est is None:
        return None
    v = getattr(est, "mem_peak_gb", None)
    try:
        if v is None:
            return None
        v = float(v)
        if v <= 0:
            return None
        return v
    except Exception:
        return None


def _retry_state(s, op):
    k = id(op)
    st = s.op_retry.get(k)
    if st is None:
        st = {"retries": 0, "mem_mult": 1.0, "next_retry_tick": 0, "terminal": False}
        s.op_retry[k] = st
    return st


def _can_retry_now(s, op):
    st = s.op_retry.get(id(op))
    if not st:
        return True
    if st.get("terminal", False):
        return False
    if st.get("retries", 0) > s.max_retries:
        return False
    return s.tick >= st.get("next_retry_tick", 0)


def _ram_request_gb(s, pool, op, usable_ram_gb):
    # Start from estimator if present; otherwise pick a conservative default based on what's usable.
    est_gb = _op_est_mem_gb(op)
    if est_gb is None:
        # Conservative fallback: ask for up to half of usable RAM (but at least min_ram).
        base = max(s.min_ram_gb, min(usable_ram_gb * 0.5, pool.max_ram_pool))
    else:
        base = est_gb

    st = s.op_retry.get(id(op))
    mult = st["mem_mult"] if st else 1.0

    req = base * s.mem_safety * mult
    # Clamp to pool maximum; admission control will reject if still doesn't fit usable.
    req = max(s.min_ram_gb, min(req, pool.max_ram_pool))
    return req


def _cpu_request(s, pool, priority, usable_cpu):
    frac = s.cpu_frac.get(priority, 0.60)
    target = pool.max_cpu_pool * frac
    req = min(usable_cpu, target)
    req = max(s.min_cpu, req)
    return req


def _pipeline_terminal(s, pipeline):
    # Drop if user already marked it terminal, or if it has FAILED ops that we will never retry.
    if pipeline.pipeline_id in s.dropped_pipeline_ids:
        return True

    status = pipeline.runtime_status()
    if status.is_pipeline_successful():
        return True

    # If there are FAILED ops but none are retryable anymore, consider the pipeline terminal.
    try:
        failed_ops = status.get_ops([OperatorState.FAILED], require_parents_complete=False) or []
    except Exception:
        failed_ops = []

    if not failed_ops:
        return False

    for op in failed_ops:
        st = s.op_retry.get(id(op))
        if st is None:
            # Unknown failure type; allow at least one retry.
            return False
        if st.get("terminal", False):
            continue
        if st.get("retries", 0) <= s.max_retries:
            return False

    # All failed ops are terminal/unretryable.
    s.dropped_pipeline_ids.add(pipeline.pipeline_id)
    return True


def _find_ready_op_that_fits(s, queue, local_avail_cpu, local_avail_ram, high_pri_waiting):
    """Scan the queue (rotating FIFO) to find the first ready op that fits somewhere."""
    n = len(queue)
    if n == 0:
        return None, None, None, None, None  # (pipeline, op, pool_id, cpu, ram)

    # We'll rotate through the current queue once to avoid starvation within the same priority.
    for _ in range(n):
        pipeline = queue.popleft()

        # Skip completed/terminal pipelines (do not requeue).
        if _pipeline_terminal(s, pipeline):
            continue

        status = pipeline.runtime_status()
        ops = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True) or []

        # Filter out FAILED ops that are in backoff or terminal.
        ready_ops = []
        for op in ops:
            if _can_retry_now(s, op):
                ready_ops.append(op)

        if not ready_ops:
            queue.append(pipeline)
            continue

        op = ready_ops[0]

        # Choose a feasible pool placement (best-fit by RAM slack).
        best = None  # (ram_slack, pool_id, cpu, ram)
        for pool_id in range(s.executor.num_pools):
            pool = s.executor.pools[pool_id]
            avail_cpu = float(local_avail_cpu[pool_id])
            avail_ram = float(local_avail_ram[pool_id])
            if avail_cpu <= 0 or avail_ram <= 0:
                continue

            # Keep headroom only for BATCH when high-priority is waiting.
            if pipeline.priority == Priority.BATCH_PIPELINE and high_pri_waiting:
                reserve_cpu = pool.max_cpu_pool * s.reserve_frac_cpu
                reserve_ram = pool.max_ram_pool * s.reserve_frac_ram
                usable_cpu = max(0.0, avail_cpu - reserve_cpu)
                usable_ram = max(0.0, avail_ram - reserve_ram)
            else:
                usable_cpu = avail_cpu
                usable_ram = avail_ram

            if usable_cpu < s.min_cpu or usable_ram < s.min_ram_gb:
                continue

            req_ram = _ram_request_gb(s, pool, op, usable_ram)
            req_cpu = _cpu_request(s, pool, pipeline.priority, usable_cpu)

            if req_ram <= usable_ram and req_cpu <= usable_cpu:
                slack = usable_ram - req_ram
                cand = (slack, pool_id, req_cpu, req_ram)
                if best is None or cand[0] < best[0]:
                    best = cand

        # Requeue pipeline regardless; we may have more ops later.
        queue.append(pipeline)

        if best is None:
            # This ready op doesn't fit anywhere right now; keep scanning.
            continue

        _, pool_id, cpu, ram = best
        return pipeline, op, pool_id, cpu, ram

    return None, None, None, None, None


@register_scheduler(key="scheduler_est_005")
def scheduler_est_005_scheduler(s, results: List[ExecutionResult], pipelines: List[Pipeline]) -> Tuple[List[Suspend], List[Assignment]]:
    """Priority-aware scheduler with estimator-based RAM sizing and OOM-aware retries/backoff."""
    s.tick += 1

    # Ingest new pipelines into priority queues.
    for p in pipelines:
        q = s.queues.get(p.priority)
        if q is None:
            # Unknown priority: treat as lowest.
            s.queues[Priority.BATCH_PIPELINE].append(p)
        else:
            q.append(p)

    # Update retry state from finished containers.
    for r in results:
        if not getattr(r, "ops", None):
            continue

        if r.failed():
            oom = _is_oom_error(getattr(r, "error", None))
            for op in r.ops:
                st = _retry_state(s, op)
                st["retries"] += 1

                if oom:
                    st["mem_mult"] *= s.mem_bump
                    # Exponential backoff in ticks: 1, 2, 4, 8...
                    backoff = s.base_backoff * (2 ** max(0, st["retries"] - 1))
                    st["next_retry_tick"] = s.tick + backoff
                else:
                    # Non-OOM failures are treated as terminal (avoid infinite retry loops).
                    st["terminal"] = True
        else:
            # On success, clear retry state (good signal that current sizing works).
            for op in r.ops:
                if id(op) in s.op_retry:
                    del s.op_retry[id(op)]

    # Early exit if nothing changed.
    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    # Snapshot pool availability locally so we can propose multiple assignments without oversubscription.
    local_avail_cpu = {}
    local_avail_ram = {}
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        local_avail_cpu[pool_id] = float(pool.avail_cpu_pool)
        local_avail_ram[pool_id] = float(pool.avail_ram_pool)

    # Consider "high priority waiting" if the high-priority queues are non-empty.
    # (We keep it simple; a more advanced version would check "ready ops exist".)
    high_pri_waiting = (len(s.queues[Priority.QUERY]) > 0) or (len(s.queues[Priority.INTERACTIVE]) > 0)

    # Greedily place as many ops as possible, respecting priority order.
    # Hard cap to prevent pathological infinite loops.
    max_assignments = 256
    for _ in range(max_assignments):
        made = False

        for pri in (Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE):
            pipeline, op, pool_id, cpu, ram = _find_ready_op_that_fits(
                s,
                s.queues[pri],
                local_avail_cpu,
                local_avail_ram,
                high_pri_waiting,
            )
            if pipeline is None:
                continue

            # Commit the assignment.
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

            local_avail_cpu[pool_id] = max(0.0, local_avail_cpu[pool_id] - float(cpu))
            local_avail_ram[pool_id] = max(0.0, local_avail_ram[pool_id] - float(ram))
            made = True
            break  # restart from highest priority after making progress

        if not made:
            break

    return suspensions, assignments
