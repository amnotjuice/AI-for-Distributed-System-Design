# policy_key: scheduler_est_041
# reasoning_effort: high
# model: gpt-5.2-2025-12-11
# llm_cost: 0.093554
# generation_seconds: 140.69
# generated_at: 2026-03-31T19:40:16.731132
@register_scheduler_init(key="scheduler_est_041")
def scheduler_est_041_init(s):
    """Priority-aware, estimate-guided scheduler (small, incremental improvements over naive FIFO).

    Improvements vs. naive FIFO:
      1) Priority ordering (QUERY > INTERACTIVE > BATCH) to reduce tail latency for high-priority work.
      2) Right-size RAM using op.estimate.mem_peak_gb (+ slack) to reduce OOMs and avoid over-allocating.
      3) Avoid "give the whole pool to one op" by slicing resources and scheduling multiple ops per tick/pool.
      4) Simple OOM-driven retry: on failure, increase the op's RAM multiplier and retry (bounded).
      5) Soft headroom reservation: when high-priority work is waiting, limit batch from consuming the last
         chunk of resources in a pool to preserve responsiveness.
    """
    # Per-priority FIFO queues of pipelines (we round-robin within each priority for fairness)
    s.waiting_by_prio = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }
    s.enqueued_pipeline_ids = set()

    # Round-robin cursor per priority queue
    s.rr_idx = {
        Priority.QUERY: 0,
        Priority.INTERACTIVE: 0,
        Priority.BATCH_PIPELINE: 0,
    }

    # Failure/oom adaptation state keyed by operator object identity
    s.op_ram_mult = {}     # oid -> multiplier for estimated peak memory
    s.op_attempts = {}     # oid -> number of failed attempts observed
    s.op_hard_fail = set() # oid -> do not retry (e.g., failed with near-max RAM)

    # Remember which pipeline an op belongs to (set when we first schedule it)
    s.op_to_pipeline_id = {}  # oid -> pipeline_id

    # Tuning knobs (kept conservative to stay "small improvement" and robust)
    s.mem_slack = 1.15             # slack over estimate for initial placement
    s.oom_backoff = 1.6            # RAM multiplier increase on failure
    s.max_ram_mult = 16.0          # cap multiplier to avoid runaway
    s.max_retries_per_op = 3       # bound retries to avoid infinite loops
    s.min_cpu = 0.5                # allow fractional CPU if simulator supports it
    s.min_ram_gb = 0.25            # avoid tiny allocations

    # Batch headroom reservation if high-priority work is waiting
    s.reserve_cpu_frac = 0.15
    s.reserve_ram_frac = 0.15

    # Per-priority CPU targets as a fraction of pool max; caps per-op CPU to avoid monopolization
    s.cpu_target_frac = {
        Priority.QUERY: 0.60,
        Priority.INTERACTIVE: 0.50,
        Priority.BATCH_PIPELINE: 0.35,
    }


def _op_est_mem_gb(op):
    est = getattr(op, "estimate", None)
    if est is None:
        return None
    return getattr(est, "mem_peak_gb", None)


def _cleanup_queue(s, prio):
    """Remove completed / terminally failed / explicitly dropped pipelines from a priority queue."""
    q = s.waiting_by_prio[prio]
    if not q:
        s.rr_idx[prio] = 0
        return

    new_q = []
    for p in q:
        pid = p.pipeline_id
        status = p.runtime_status()

        # Drop completed pipelines
        if status.is_pipeline_successful():
            s.enqueued_pipeline_ids.discard(pid)
            continue

        # Drop pipelines that have failures but no retryable ops left at all
        has_failures = status.state_counts[OperatorState.FAILED] > 0
        if has_failures:
            # If nothing is in an assignable state anywhere in the DAG, treat as terminal failure
            any_assignable_anywhere = bool(status.get_ops(ASSIGNABLE_STATES, require_parents_complete=False))
            if not any_assignable_anywhere:
                s.enqueued_pipeline_ids.discard(pid)
                continue

        new_q.append(p)

    s.waiting_by_prio[prio] = new_q
    if not new_q:
        s.rr_idx[prio] = 0
    else:
        s.rr_idx[prio] = s.rr_idx[prio] % len(new_q)


def _any_ready_ops(s, prio):
    """Conservatively check if there exists at least one ready (parent-complete) op for this priority."""
    q = s.waiting_by_prio[prio]
    for p in q:
        status = p.runtime_status()
        if status.is_pipeline_successful():
            continue
        ops = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        if ops:
            return True
    return False


def _pick_ready_op_for_pool(s, prio, pool):
    """Round-robin: pick the next pipeline with a ready op that can fit in this pool (by estimated RAM)."""
    q = s.waiting_by_prio[prio]
    n = len(q)
    if n == 0:
        return None, None

    start = s.rr_idx.get(prio, 0) % n

    for step in range(n):
        idx = (start + step) % n
        p = q[idx]
        status = p.runtime_status()
        if status.is_pipeline_successful():
            continue

        ops = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        if not ops:
            continue

        op = ops[0]
        oid = id(op)

        # Do not retry ops marked as hard-fail
        if oid in s.op_hard_fail:
            continue

        # Skip if retries exceeded
        if s.op_attempts.get(oid, 0) >= s.max_retries_per_op:
            continue

        # Skip if estimated memory can never fit this pool
        est = _op_est_mem_gb(op)
        if est is not None and est > pool.max_ram_pool:
            continue

        # Advance RR cursor past this pipeline
        s.rr_idx[prio] = (idx + 1) % n
        return p, op

    return None, None


def _compute_ram_request_gb(s, op, pool):
    """Compute RAM request from estimate * slack * learned multiplier, clamped to pool max."""
    est = _op_est_mem_gb(op)
    if est is None:
        # Fallback: small-but-nontrivial default if estimate missing
        est = max(s.min_ram_gb, 0.10 * pool.max_ram_pool)

    mult = s.op_ram_mult.get(id(op), 1.0)
    ram = est * s.mem_slack * mult

    # Clamp
    ram = max(s.min_ram_gb, ram)
    ram = min(ram, pool.max_ram_pool)
    return ram


def _compute_cpu_request(s, prio, pool, rem_cpu):
    """Compute per-op CPU request as a capped fraction of pool max, bounded by remaining CPU."""
    target = s.cpu_target_frac.get(prio, 0.35) * pool.max_cpu_pool
    cpu = min(rem_cpu, max(s.min_cpu, target))
    return cpu


@register_scheduler(key="scheduler_est_041")
def scheduler_est_041(s, results: List["ExecutionResult"], pipelines: List["Pipeline"]):
    """
    Priority-aware, estimate-guided scheduler.

    Returns:
        (suspensions, assignments)
    """
    suspensions = []
    assignments = []

    # Enqueue new pipelines by priority (avoid duplicates)
    for p in pipelines:
        pid = p.pipeline_id
        if pid in s.enqueued_pipeline_ids:
            continue
        s.enqueued_pipeline_ids.add(pid)
        s.waiting_by_prio[p.priority].append(p)

    # Process execution results: learn from failures by increasing RAM multiplier for that op
    for r in results:
        if not r.failed():
            continue

        pool = s.executor.pools[r.pool_id]

        for op in r.ops:
            oid = id(op)
            s.op_attempts[oid] = s.op_attempts.get(oid, 0) + 1

            # Increase RAM multiplier (assume memory-related failure; conservative but effective)
            old = s.op_ram_mult.get(oid, 1.0)
            s.op_ram_mult[oid] = min(s.max_ram_mult, old * s.oom_backoff)

            # If we already tried with near-max RAM for this pool, stop retrying this op
            try:
                if r.ram >= 0.90 * pool.max_ram_pool:
                    s.op_hard_fail.add(oid)
            except Exception:
                # If ram missing / non-numeric, ignore
                pass

    # Cleanup queues (remove completed / terminal pipelines)
    _cleanup_queue(s, Priority.QUERY)
    _cleanup_queue(s, Priority.INTERACTIVE)
    _cleanup_queue(s, Priority.BATCH_PIPELINE)

    # If nothing changed and no queued work, exit
    if not results and not pipelines:
        any_waiting = (
            len(s.waiting_by_prio[Priority.QUERY]) +
            len(s.waiting_by_prio[Priority.INTERACTIVE]) +
            len(s.waiting_by_prio[Priority.BATCH_PIPELINE])
        ) > 0
        if not any_waiting:
            return [], []

    # Global signal: is there *any* high-priority ready op waiting?
    high_prio_waiting = _any_ready_ops(s, Priority.QUERY) or _any_ready_ops(s, Priority.INTERACTIVE)

    # Schedule per pool, filling resources with multiple assignments (high priority first)
    prio_order = [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]

    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        rem_cpu = pool.avail_cpu_pool
        rem_ram = pool.avail_ram_pool

        if rem_cpu <= 0 or rem_ram <= 0:
            continue

        reserve_cpu = (s.reserve_cpu_frac * pool.max_cpu_pool) if high_prio_waiting else 0.0
        reserve_ram = (s.reserve_ram_frac * pool.max_ram_pool) if high_prio_waiting else 0.0

        made_progress = True
        while made_progress:
            made_progress = False

            for prio in prio_order:
                # Soft headroom: if high priority is waiting, prevent batch from consuming last chunk
                if prio == Priority.BATCH_PIPELINE and high_prio_waiting:
                    allowed_cpu = max(0.0, rem_cpu - reserve_cpu)
                    allowed_ram = max(0.0, rem_ram - reserve_ram)
                else:
                    allowed_cpu = rem_cpu
                    allowed_ram = rem_ram

                if allowed_cpu < s.min_cpu or allowed_ram < s.min_ram_gb:
                    continue

                p, op = _pick_ready_op_for_pool(s, prio, pool)
                if op is None:
                    continue

                oid = id(op)

                # Record op->pipeline mapping (used for bookkeeping/debug; safe if repeated)
                s.op_to_pipeline_id[oid] = p.pipeline_id

                # If retries exceeded, skip scheduling this op
                if s.op_attempts.get(oid, 0) >= s.max_retries_per_op:
                    continue

                ram_req = _compute_ram_request_gb(s, op, pool)
                if ram_req > allowed_ram:
                    # Not enough RAM headroom right now; try other priorities/pipelines
                    continue

                cpu_req = _compute_cpu_request(s, prio, pool, allowed_cpu)
                if cpu_req < s.min_cpu:
                    continue

                # Create assignment for a single ready op
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

                # Update remaining resources for this pool in this tick to avoid oversubscription
                rem_cpu -= cpu_req
                rem_ram -= ram_req

                made_progress = True

                # If the pool is basically full, stop
                if rem_cpu < s.min_cpu or rem_ram < s.min_ram_gb:
                    break

            if rem_cpu < s.min_cpu or rem_ram < s.min_ram_gb:
                break

    return suspensions, assignments
