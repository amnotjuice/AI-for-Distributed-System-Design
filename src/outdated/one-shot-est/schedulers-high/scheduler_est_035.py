# policy_key: scheduler_est_035
# reasoning_effort: high
# model: gpt-5.2-2025-12-11
# llm_cost: 0.126034
# generation_seconds: 115.32
# generated_at: 2026-03-31T19:28:04.789349
@register_scheduler_init(key="scheduler_est_035")
def scheduler_est_035_init(s):
    """
    Priority-aware, estimate-driven packing scheduler.

    Improvements over naive FIFO:
      - Maintain separate queues per priority and schedule high-priority first.
      - Right-size RAM using op.estimate.mem_peak_gb (plus overhead), instead of giving an op the whole pool.
      - Pack multiple ops per pool per tick when resources allow (improves latency/throughput under contention).
      - Retry FAILED ops (likely OOM) by increasing a per-operator memory multiplier, up to a max retry limit.
      - Simple fairness: after N high-priority placements on the interactive pool, try to place one batch op.
    """
    # Per-priority queues of pipelines (pipelines stay queued until completion)
    s.queues = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }

    # Per-operator adaptive knobs (keyed by a best-effort stable operator key)
    s.mem_factor = {}          # op_key -> multiplier applied to estimate.mem_peak_gb
    s.attempts = {}            # op_key -> number of failed attempts observed
    s.blacklisted = set()      # op_key that exceeded max retries; we stop scheduling these ops

    # Tuning parameters (kept conservative / "small improvements first")
    s.mem_overhead = 1.20      # slack over the estimator to reduce OOM risk
    s.min_ram_gb = 0.50        # avoid pathological tiny allocations
    s.cpu_cap_hp = 4.0         # max vCPU per high-priority op (QUERY/INTERACTIVE)
    s.cpu_cap_batch = 4.0      # max vCPU per batch op
    s.max_retries = 3          # after this, mark op as unschedulable to avoid infinite loops

    # Fairness on pool 0 (assumed "interactive-ish" if multiple pools exist)
    s.batch_every_n_hp = 6
    s.hp_since_batch = 0


def _op_key(op):
    # Best-effort stable key: prefer explicit IDs/names if present.
    k = getattr(op, "op_id", None)
    if k is not None:
        return ("op_id", k)
    k = getattr(op, "operator_id", None)
    if k is not None:
        return ("operator_id", k)
    k = getattr(op, "name", None)
    if k is not None:
        return ("name", k)
    # Fallback: repr/op identity (may be process-stable within a simulation run)
    return ("repr", repr(op))


def _mem_est_gb(op):
    est = getattr(op, "estimate", None)
    mem = getattr(est, "mem_peak_gb", None) if est is not None else None
    try:
        if mem is not None and float(mem) > 0:
            return float(mem)
    except Exception:
        pass
    return None


def _is_likely_oom_error(err):
    if not err:
        return False
    try:
        e = str(err).lower()
    except Exception:
        return False
    return ("oom" in e) or ("out of memory" in e) or ("memory" in e and "exceed" in e)


def _pipeline_has_inflight(status):
    # Avoid scheduling multiple ops from the same pipeline concurrently (reduces interference, helps tail latency).
    inflight = status.get_ops(
        [OperatorState.ASSIGNED, OperatorState.RUNNING, OperatorState.SUSPENDING],
        require_parents_complete=False,
    )
    return bool(inflight)


def _pipeline_retryable(s, status):
    # If there are FAILED ops, we only keep the pipeline if at least one failed op is still retryable.
    failed_ops = status.get_ops([OperatorState.FAILED], require_parents_complete=False)
    if not failed_ops:
        return True
    for op in failed_ops:
        k = _op_key(op)
        if k in s.blacklisted:
            continue
        if s.attempts.get(k, 0) <= s.max_retries:
            return True
    return False


def _ram_request_gb(s, op, pool):
    # Use estimator if present; otherwise pick a conservative default fraction of pool RAM.
    est = _mem_est_gb(op)
    if est is None:
        est = max(1.0, min(4.0, float(pool.max_ram_pool) * 0.25))

    k = _op_key(op)
    factor = float(s.mem_factor.get(k, 1.0))

    # Clamp to pool max, keep a floor.
    req = max(s.min_ram_gb, est * s.mem_overhead * factor)
    req = min(float(pool.max_ram_pool), req)
    return req


def _cpu_request(s, priority, avail_cpu):
    cap = s.cpu_cap_hp if priority in (Priority.QUERY, Priority.INTERACTIVE) else s.cpu_cap_batch
    # Ensure we request at least some CPU if any is available.
    return max(0.0, min(float(avail_cpu), float(cap)))


def _priority_order_for_pool(s, pool_id):
    # Pool 0 is treated as "interactive-leaning" when multiple pools exist.
    if pool_id == 0 and s.executor.num_pools > 1:
        # Simple fairness: occasionally give batch a chance on pool 0.
        if s.hp_since_batch >= s.batch_every_n_hp and s.queues[Priority.BATCH_PIPELINE]:
            return [Priority.BATCH_PIPELINE, Priority.QUERY, Priority.INTERACTIVE]
        return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]

    # Other pools: batch-leaning but allow high priority to spill if idle.
    return [Priority.BATCH_PIPELINE, Priority.INTERACTIVE, Priority.QUERY]


def _pop_fit_candidate(s, pool_id, avail_ram, scheduled_pipeline_ids):
    """
    Scan queues in priority order, rotate within each queue (round-robin),
    and return (pipeline, op) that can fit in avail_ram. Returns (None, None) if none fit.
    """
    prio_order = _priority_order_for_pool(s, pool_id)
    for pr in prio_order:
        q = s.queues[pr]
        if not q:
            continue

        # Scan at most one full rotation for this priority level.
        n = len(q)
        for _ in range(n):
            p = q.pop(0)
            status = p.runtime_status()

            # Drop completed pipelines.
            if status.is_pipeline_successful():
                continue

            # Drop pipelines that are no longer retryable (avoid permanent clogging).
            if not _pipeline_retryable(s, status):
                continue

            # Avoid issuing multiple ops from the same pipeline in one scheduler tick.
            if p.pipeline_id in scheduled_pipeline_ids:
                q.append(p)
                continue

            # Reduce interference by keeping each pipeline single-flight.
            if _pipeline_has_inflight(status):
                q.append(p)
                continue

            # Find next ready op.
            op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
            if not op_list:
                q.append(p)
                continue

            op = op_list[0]
            k = _op_key(op)
            if k in s.blacklisted:
                # This op exceeded retries; keep pipeline queued in case other branches can proceed (rare),
                # but we won't schedule this particular op again.
                q.append(p)
                continue

            # Check RAM fit.
            pool = s.executor.pools[pool_id]
            ram_req = _ram_request_gb(s, op, pool)
            if ram_req <= float(avail_ram) + 1e-9:
                # Candidate found; keep pipeline in queue (append) so it stays tracked.
                q.append(p)
                return p, op
            else:
                # Doesn't fit right now; rotate to back and keep searching.
                q.append(p)

    return None, None


@register_scheduler(key="scheduler_est_035")
def scheduler_est_035(s, results: List["ExecutionResult"], pipelines: List["Pipeline"]) -> Tuple[List["Suspend"], List["Assignment"]]:
    """
    Deterministic, priority-aware packing with adaptive RAM retries on failures.

    Returns:
      - suspensions: empty (no preemption yet; keeping changes small and safe)
      - assignments: list of placements sized by estimates and limited cpu caps
    """
    # Enqueue new pipelines by priority.
    for p in pipelines:
        s.queues[p.priority].append(p)

    # Early exit when nothing changed (keeps overhead low).
    if not pipelines and not results:
        return [], []

    # Learn from results (especially failures).
    for r in results:
        if not r.ops:
            continue
        if r.failed():
            oom_like = _is_likely_oom_error(r.error)
            bump = 1.70 if oom_like else 1.30
            for op in r.ops:
                k = _op_key(op)
                s.attempts[k] = s.attempts.get(k, 0) + 1
                s.mem_factor[k] = float(s.mem_factor.get(k, 1.0)) * float(bump)
                if s.attempts[k] > s.max_retries:
                    s.blacklisted.add(k)
        else:
            # On success, keep current factor; optionally could decay later.
            pass

    suspensions: List["Suspend"] = []
    assignments: List["Assignment"] = []

    # Prevent scheduling the same pipeline multiple times in a single scheduler call.
    scheduled_pipeline_ids = set()

    # Try to pack multiple assignments per pool, bounded by available resources.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)

        # Small guard against tiny fragments.
        if avail_cpu <= 0.0 or avail_ram <= 0.0:
            continue

        # Cap the amount of scanning work per pool to keep the scheduler fast/deterministic.
        total_queued = sum(len(q) for q in s.queues.values())
        scan_budget = max(8, 2 * total_queued)

        scans = 0
        while avail_cpu >= 1e-6 and avail_ram >= s.min_ram_gb - 1e-9 and scans < scan_budget:
            scans += 1

            p, op = _pop_fit_candidate(s, pool_id, avail_ram, scheduled_pipeline_ids)
            if p is None or op is None:
                break

            # Size resources.
            ram_req = _ram_request_gb(s, op, pool)
            cpu_req = _cpu_request(s, p.priority, avail_cpu)

            # If we can't give at least a minimal CPU slice, stop packing this pool.
            if cpu_req <= 1e-6:
                break

            # Allocate exactly what we requested (bounded by availability).
            ram_alloc = min(avail_ram, float(ram_req))
            cpu_alloc = min(avail_cpu, float(cpu_req))

            # Construct assignment (one op at a time).
            assignment = Assignment(
                ops=[op],
                cpu=cpu_alloc,
                ram=ram_alloc,
                priority=p.priority,
                pool_id=pool_id,
                pipeline_id=p.pipeline_id,
            )
            assignments.append(assignment)

            # Update accounting for this pool and fairness counters.
            avail_cpu -= cpu_alloc
            avail_ram -= ram_alloc
            scheduled_pipeline_ids.add(p.pipeline_id)

            if pool_id == 0 and s.executor.num_pools > 1:
                if p.priority in (Priority.QUERY, Priority.INTERACTIVE):
                    s.hp_since_batch += 1
                elif p.priority == Priority.BATCH_PIPELINE:
                    s.hp_since_batch = 0

    return suspensions, assignments
