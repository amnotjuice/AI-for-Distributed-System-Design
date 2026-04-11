# policy_key: scheduler_high_016
# reasoning_effort: high
# model: gpt-5.2-2025-12-11
# llm_cost: 0.132677
# generation_seconds: 191.08
# generated_at: 2026-03-12T23:00:50.081168
@register_scheduler_init(key="scheduler_high_016")
def scheduler_high_016_init(s):
    """Priority-aware, concurrency-friendly scheduler.

    Improvements over naive FIFO:
    - Separate per-priority round-robin queues (QUERY/INTERACTIVE first).
    - Allow multiple concurrent assignments per pool (up to a small cap) instead of 1.
    - Soft headroom reservation for high priority to reduce queueing latency.
    - OOM-aware retries: on OOM failures, exponentially increase RAM request for the same op.
    - Drop/ignore pipelines with repeated non-OOM failures (best-effort, based on result error string).
    """
    # Per-priority round-robin queues of pipeline_ids
    s.queues = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }

    # Known pipelines and lightweight indices
    s.pipelines_by_id = {}          # pipeline_id -> Pipeline
    s.op_to_pipeline_id = {}        # id(op) -> pipeline_id

    # Failure handling
    s.terminal_pipelines = set()    # pipeline_ids that we consider unschedulable (non-OOM or too many OOMs)

    # OOM backoff state (keyed by (pipeline_id, id(op)))
    s.oom_next_ram = {}             # (pid, op_obj_id) -> next_ram_request
    s.oom_retries = {}              # (pid, op_obj_id) -> retry_count
    s.max_oom_retries = 5

    # Concurrency and sizing knobs (kept simple and conservative)
    s.max_assignments_per_pool = 6

    # Minimum quanta (fractions of pool capacity) to avoid absurdly tiny allocations
    s.min_cpu_quantum_frac = 0.05
    s.min_ram_quantum_frac = 0.05
    s.min_abs_cpu = 0.1
    s.min_abs_ram = 0.1

    # Soft reservation for high-priority headroom (fraction of pool max)
    s.high_reserve_cpu_frac = 0.25
    s.high_reserve_ram_frac = 0.25

    # For minor determinism/debug
    s._tick = 0


def _iter_pipeline_ops(pipeline):
    """Best-effort iteration over pipeline operators to build op->pipeline index."""
    vals = getattr(pipeline, "values", None)
    if vals is None:
        return []
    # Handle dict-like or list-like
    if hasattr(vals, "values") and callable(getattr(vals, "values")):
        try:
            return list(vals.values())
        except Exception:
            return []
    try:
        return list(vals)
    except Exception:
        return []


def _is_oom_error(err):
    if err is None:
        return False
    msg = str(err).lower()
    return ("oom" in msg) or ("out of memory" in msg) or ("out-of-memory" in msg) or ("memoryerror" in msg)


def _min_quantum(pool, min_frac, min_abs, max_value):
    q = max(max_value * float(min_frac), float(min_abs))
    # Never exceed the pool max (can happen if max_value is tiny and min_abs is larger)
    return min(q, max_value) if max_value > 0 else 0.0


def _pipeline_active(s, pipeline):
    """True iff pipeline should still be considered for scheduling."""
    if pipeline is None:
        return False
    pid = pipeline.pipeline_id
    if pid in s.terminal_pipelines:
        return False
    try:
        status = pipeline.runtime_status()
        if status.is_pipeline_successful():
            return False
    except Exception:
        # If status fails, conservatively keep it active (better than dropping)
        return True
    return True


def _cleanup_queue_in_place(s, prio):
    """Remove completed/terminal pipelines from the queue to reduce scan overhead."""
    q = s.queues.get(prio, [])
    if not q:
        return
    new_q = []
    seen = set()
    for pid in q:
        if pid in seen:
            continue
        seen.add(pid)
        p = s.pipelines_by_id.get(pid)
        if p is None:
            continue
        if not _pipeline_active(s, p):
            continue
        new_q.append(pid)
    s.queues[prio] = new_q


def _has_any_high_backlog(s):
    # Backlog = any active pipeline in QUERY or INTERACTIVE queues.
    for prio in (Priority.QUERY, Priority.INTERACTIVE):
        for pid in s.queues.get(prio, []):
            p = s.pipelines_by_id.get(pid)
            if p is not None and _pipeline_active(s, p):
                return True
    return False


def _pop_ready_from_prio_queue(s, prio):
    """Round-robin scan to find a pipeline with a ready (assignable) op.

    Returns:
        (pipeline_id, pipeline, op_list) where op_list is a single-op list.
        The returned pipeline_id is removed from the front; caller must re-append it
        (or drop it) to preserve round-robin fairness.
    """
    q = s.queues.get(prio, [])
    if not q:
        return None, None, None

    n = len(q)
    for _ in range(n):
        pid = q.pop(0)
        p = s.pipelines_by_id.get(pid)
        if p is None or not _pipeline_active(s, p):
            # Drop it (do not reappend)
            continue

        try:
            status = p.runtime_status()
            op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
        except Exception:
            op_list = []

        if not op_list:
            # Not ready now; rotate to the back
            q.append(pid)
            continue

        # Found something schedulable; do not reappend here (caller will)
        return pid, p, op_list

    return None, None, None


def _target_sizing(pool, prio, high_backlog):
    """Heuristic sizing: larger slices for high-priority; smaller for batch when high backlog exists."""
    max_cpu = float(pool.max_cpu_pool)
    max_ram = float(pool.max_ram_pool)

    # If high backlog, slightly reduce per-op high slice to allow more concurrency.
    if prio in (Priority.QUERY, Priority.INTERACTIVE):
        cpu_frac = 0.50 if not high_backlog else 0.33
        ram_frac = 0.50 if not high_backlog else 0.33
    else:
        # Batch: if high backlog, keep it smaller to preserve headroom; else let it run bigger.
        cpu_frac = 0.20 if high_backlog else 0.45
        ram_frac = 0.20 if high_backlog else 0.45

    cpu = max(max_cpu * cpu_frac, 0.0)
    ram = max(max_ram * ram_frac, 0.0)
    return cpu, ram


@register_scheduler(key="scheduler_high_016")
def scheduler_high_016(s, results, pipelines):
    """
    Priority-first, multi-assignment scheduling with OOM backoff.

    Key behavior:
    - Always try QUERY then INTERACTIVE then BATCH.
    - Multiple assignments per pool per tick while resources allow.
    - When any high-priority backlog exists, preserve a soft reserve so batch doesn't
      consume the last headroom; this reduces high-priority queueing latency.
    - On OOM failure, double RAM request for the failed op (up to pool max) and retry.
    """
    s._tick += 1

    # Ingest new pipelines
    for p in pipelines:
        pid = p.pipeline_id
        if pid not in s.pipelines_by_id:
            s.pipelines_by_id[pid] = p
            # Index ops -> pipeline for failure attribution
            for op in _iter_pipeline_ops(p):
                try:
                    s.op_to_pipeline_id[id(op)] = pid
                except Exception:
                    pass
            # Enqueue once
            prio = p.priority
            if prio not in s.queues:
                s.queues[prio] = []
            s.queues[prio].append(pid)
        else:
            # Refresh pointer (defensive)
            s.pipelines_by_id[pid] = p

    # Process execution results (update OOM backoff / terminal marking)
    for r in results:
        # Determine affected pipeline(s) from ops
        ops = getattr(r, "ops", None) or []
        if not ops:
            continue

        is_failed = False
        try:
            is_failed = r.failed()
        except Exception:
            # If we can't tell, assume not failed
            is_failed = False

        if not is_failed:
            # Success: clear any OOM backoff for these ops
            for op in ops:
                pid = s.op_to_pipeline_id.get(id(op))
                if pid is None:
                    continue
                key = (pid, id(op))
                if key in s.oom_next_ram:
                    del s.oom_next_ram[key]
                if key in s.oom_retries:
                    del s.oom_retries[key]
            continue

        # Failed
        err = getattr(r, "error", None)
        if _is_oom_error(err):
            # OOM: back off RAM and retry (bounded)
            for op in ops:
                pid = s.op_to_pipeline_id.get(id(op))
                if pid is None:
                    continue
                key = (pid, id(op))
                s.oom_retries[key] = s.oom_retries.get(key, 0) + 1
                if s.oom_retries[key] > s.max_oom_retries:
                    # Too many OOMs; stop retrying this pipeline to avoid churn
                    s.terminal_pipelines.add(pid)
                    continue

                prev = float(getattr(r, "ram", 0.0) or 0.0)
                # If prev is missing/0, start from a conservative baseline later via sizing; keep 0 here.
                next_req = prev * 2.0 if prev > 0 else 0.0
                s.oom_next_ram[key] = max(s.oom_next_ram.get(key, 0.0), next_req)
        else:
            # Non-OOM failure: mark pipeline terminal (best-effort)
            for op in ops:
                pid = s.op_to_pipeline_id.get(id(op))
                if pid is not None:
                    s.terminal_pipelines.add(pid)

    # Early exit if nothing changed (keeps behavior aligned with baseline template)
    if not pipelines and not results:
        return [], []

    # Cleanup queues to avoid repeated scans over completed/terminal pipelines
    _cleanup_queue_in_place(s, Priority.QUERY)
    _cleanup_queue_in_place(s, Priority.INTERACTIVE)
    _cleanup_queue_in_place(s, Priority.BATCH_PIPELINE)

    suspensions = []
    assignments = []

    high_backlog = _has_any_high_backlog(s)

    # Schedule per pool
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)

        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        max_cpu = float(pool.max_cpu_pool)
        max_ram = float(pool.max_ram_pool)

        min_cpu_q = _min_quantum(pool, s.min_cpu_quantum_frac, s.min_abs_cpu, max_cpu)
        min_ram_q = _min_quantum(pool, s.min_ram_quantum_frac, s.min_abs_ram, max_ram)

        # Soft reserve for high-priority: only relevant when high backlog exists.
        reserve_cpu = (max_cpu * float(s.high_reserve_cpu_frac)) if high_backlog else 0.0
        reserve_ram = (max_ram * float(s.high_reserve_ram_frac)) if high_backlog else 0.0

        made = 0
        while made < int(s.max_assignments_per_pool):
            if avail_cpu < min_cpu_q or avail_ram < min_ram_q:
                break

            scheduled = False

            for prio in (Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE):
                # If high backlog exists, do not let batch eat into reserved headroom.
                if prio == Priority.BATCH_PIPELINE and high_backlog:
                    if avail_cpu <= reserve_cpu or avail_ram <= reserve_ram:
                        continue

                pid, pipeline, op_list = _pop_ready_from_prio_queue(s, prio)
                if pid is None:
                    continue

                # Default target sizing by priority + backlog
                target_cpu, target_ram = _target_sizing(pool, prio, high_backlog)

                # Apply OOM-derived RAM requirement (per-op)
                op_obj = op_list[0]
                oom_key = (pid, id(op_obj))
                oom_req_ram = float(s.oom_next_ram.get(oom_key, 0.0) or 0.0)

                # Respect soft reserve for batch by shrinking request if needed.
                # (For high-priority, we allow consuming into reserve.)
                eff_avail_cpu = avail_cpu
                eff_avail_ram = avail_ram
                if prio == Priority.BATCH_PIPELINE and high_backlog:
                    eff_avail_cpu = max(0.0, avail_cpu - reserve_cpu)
                    eff_avail_ram = max(0.0, avail_ram - reserve_ram)

                # Compute final request (bounded by effective availability)
                req_cpu = min(eff_avail_cpu, target_cpu if target_cpu > 0 else eff_avail_cpu)
                req_ram = min(eff_avail_ram, max(target_ram if target_ram > 0 else eff_avail_ram, oom_req_ram))

                # Enforce minimum quanta
                req_cpu = max(req_cpu, min_cpu_q)
                req_ram = max(req_ram, min_ram_q)

                # If after min-quantum we exceed effective availability, we can't place now.
                if req_cpu > eff_avail_cpu or req_ram > eff_avail_ram or req_cpu <= 0 or req_ram <= 0:
                    # Reappend pipeline and try next candidate
                    s.queues[prio].append(pid)
                    continue

                assignment = Assignment(
                    ops=op_list,
                    cpu=req_cpu,
                    ram=req_ram,
                    priority=pipeline.priority,
                    pool_id=pool_id,
                    pipeline_id=pipeline.pipeline_id,
                )
                assignments.append(assignment)

                # Round-robin: reappend pipeline for future ops
                s.queues[prio].append(pid)

                # Update pool-local availability
                avail_cpu -= req_cpu
                avail_ram -= req_ram
                made += 1
                scheduled = True
                break  # move to next assignment in this pool (re-check priority ordering)

            if not scheduled:
                break

    return suspensions, assignments
