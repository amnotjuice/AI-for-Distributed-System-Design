# source: scheduler_low_011.py
# model: gpt-5.2-2025-12-11
# effort: low
# feedback: minimal
# iteration: r8
@register_scheduler_init(key="scheduler_low_011_r8")
def scheduler_low_011_r8_init(s):
    """Latency-first priority scheduler (incremental step from naive FIFO).

    Key changes vs naive:
    - Per-priority FIFO queues (QUERY > INTERACTIVE > BATCH).
    - Reserve headroom for high-priority work so batch doesn't fill the pool and cause queueing delay.
    - Smaller default CPU allocations for high-priority ops to increase concurrency and reduce wait time.
    - Allow multiple assignments per pool per tick (bounded), while preventing duplicate scheduling of the same pipeline/op.
    - OOM-aware RAM backoff retries; non-OOM failures are marked non-retryable and the pipeline is dropped.
    """
    # Per-priority pipeline queues (FIFO)
    s.waiting_queues = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }

    # Learned per-operator RAM hints (keyed by (pipeline_id, op_id) when possible, else (None, op_id))
    s.ram_hints = {}          # key -> ram
    s.retry_counts = {}       # key -> int

    # Track which operator failures are retryable (OOM) vs non-retryable
    s.retryable_ops = set()   # keys
    s.nonretryable_ops = set()  # keys
    s.bad_pipelines = set()   # pipeline_id that encountered non-OOM failure or exceeded retry budget

    # Retry policy
    s.max_retries_per_op = 3
    s.ram_backoff = 2.0

    # Pool preference: prefer placing high-priority on pool 0 (if it exists)
    s.interactive_pool_id = 0

    # Headroom reservation for high priority (fraction of pool max to keep free when HP backlog exists)
    s.reserve_frac_interactive_pool = 0.25
    s.reserve_frac_other_pools = 0.10

    # Default sizing targets (latency-first for high priority, throughput for batch)
    s.hp_cpu_target_frac = 0.25  # fraction of pool max cpu, capped
    s.hp_cpu_cap = 4.0
    s.hp_ram_target_frac = 0.25  # fraction of pool max ram, but never below 1.0

    # Bound assignments per pool per tick (keep small; we rely on multiple ticks for steady-state)
    s.max_assignments_per_pool = 4


def _prio_order():
    return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]


def _is_oom_error(err):
    if err is None:
        return False
    msg = str(err).lower()
    return ("oom" in msg) or ("out of memory" in msg) or ("memoryerror" in msg)


def _op_id(op):
    # Object identity is the safest cross-schema "identifier" available.
    return id(op)


def _make_key(pipeline_id, op):
    # Prefer pipeline-scoped key to avoid cross-pipeline contamination when possible.
    return (pipeline_id, _op_id(op))


def _make_fallback_key(op):
    return (None, _op_id(op))


def _min_positive(x, floor=1.0):
    try:
        if x is None:
            return floor
        x = float(x)
    except Exception:
        return floor
    return max(floor, x)


def _queue_has_ready_hp(s, limit=8):
    """Best-effort detection of whether HP queues have READY/FAILED(assignable) ops.
    Used to enable/disable batch headroom reservation to reduce latency.
    """
    for pr in (Priority.QUERY, Priority.INTERACTIVE):
        q = s.waiting_queues.get(pr, [])
        # Peek at first few pipelines (don't mutate)
        for i in range(min(limit, len(q))):
            p = q[i]
            if getattr(p, "pipeline_id", None) in s.bad_pipelines:
                continue
            st = p.runtime_status()
            ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
            if not ops:
                continue
            # Check whether at least one op is actually schedule-worthy
            pid = p.pipeline_id
            for op in ops[:4]:
                k = _make_key(pid, op)
                fk = _make_fallback_key(op)
                if k in s.nonretryable_ops or fk in s.nonretryable_ops:
                    continue
                # If this op is FAILED and we haven't marked it retryable, skip it (avoid retry loops)
                if k in s.retryable_ops or fk in s.retryable_ops:
                    return True
                # Pending ops are also valid
                return True
    return False


def _reserve_amounts(s, pool, pool_id, hp_backlog_ready):
    """Compute how much CPU/RAM to reserve (keep free) to protect HP latency."""
    if not hp_backlog_ready:
        return 0.0, 0.0
    frac = s.reserve_frac_interactive_pool if pool_id == s.interactive_pool_id else s.reserve_frac_other_pools
    reserve_cpu = float(pool.max_cpu_pool) * frac
    reserve_ram = float(pool.max_ram_pool) * frac
    # Keep reservations meaningful but not exceeding pool maxima
    reserve_cpu = max(0.0, min(reserve_cpu, float(pool.max_cpu_pool)))
    reserve_ram = max(0.0, min(reserve_ram, float(pool.max_ram_pool)))
    return reserve_cpu, reserve_ram


def _hp_request(s, pool, pid, op):
    """Small high-priority requests to reduce queueing delay; apply RAM hint if present."""
    # CPU: fraction of pool max, capped, and not exceeding available
    cpu_target = float(pool.max_cpu_pool) * float(s.hp_cpu_target_frac)
    cpu_target = min(float(s.hp_cpu_cap), cpu_target)
    cpu = _min_positive(cpu_target, 1.0)

    # RAM: fraction of pool max, but honor hints (OOM backoff)
    ram_target = float(pool.max_ram_pool) * float(s.hp_ram_target_frac)
    ram = _min_positive(ram_target, 1.0)

    k = _make_key(pid, op)
    fk = _make_fallback_key(op)
    hinted = s.ram_hints.get(k, None)
    if hinted is None:
        hinted = s.ram_hints.get(fk, None)
    if hinted is not None:
        ram = max(ram, _min_positive(hinted, 1.0))

    # Cap by pool maxima
    cpu = min(cpu, float(pool.max_cpu_pool))
    ram = min(ram, float(pool.max_ram_pool))
    return cpu, ram


def _batch_request(avail_cpu, avail_ram, reserve_cpu, reserve_ram):
    """Batch soaks up remaining resources but leaves headroom if needed."""
    cpu = avail_cpu - reserve_cpu
    ram = avail_ram - reserve_ram
    # Must be positive to schedule anything
    if cpu <= 0.0 or ram <= 0.0:
        return None, None
    cpu = max(1.0, cpu)
    ram = max(1.0, ram)
    return cpu, ram


def _pick_schedulable_op(s, pipeline, assigned_pipeline_ids, assigned_op_ids):
    """Pick the first assignable operator that is not known non-retryable and not already assigned this tick."""
    st = pipeline.runtime_status()
    ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
    if not ops:
        return None

    pid = pipeline.pipeline_id
    if pid in assigned_pipeline_ids:
        return None

    for op in ops:
        oid = _op_id(op)
        if oid in assigned_op_ids:
            continue

        k = _make_key(pid, op)
        fk = _make_fallback_key(op)

        # Skip known non-retryable failed ops
        if k in s.nonretryable_ops or fk in s.nonretryable_ops:
            continue

        # If this op previously FAILED and isn't marked retryable, we should avoid re-running it forever.
        # We can't directly inspect op.state portably, so we approximate:
        # - If it's in retryable_ops: OK to retry (OOM).
        # - Else: also OK if it has no recorded failure markers (pending).
        # This is conservative and relies on s.bad_pipelines to block non-OOM failures.
        return op

    return None


@register_scheduler(key="scheduler_low_011_r8")
def scheduler_low_011_r8(s, results, pipelines):
    """
    Priority-first, latency-protecting scheduler with headroom reservation and OOM-aware retries.

    Scheduling loop:
    - Ingest new pipelines into per-priority queues.
    - Process results:
        * OOM failures => mark op retryable, increase RAM hint (exponential backoff), cap retries.
        * Non-OOM failures => mark pipeline bad, never retry.
    - For each pool, schedule up to K ops:
        * Prefer high-priority queues; for batch, require headroom if HP backlog exists.
        * Use smaller HP allocations to reduce queue time; batch uses remaining.
        * Avoid assigning multiple ops from the same pipeline or the same op in a single tick.
    """
    # Enqueue arrivals
    for p in pipelines:
        pr = p.priority if p.priority in s.waiting_queues else Priority.BATCH_PIPELINE
        s.waiting_queues[pr].append(p)

    # Update retry state based on results
    for r in results:
        try:
            failed = bool(r.failed())
        except Exception:
            failed = getattr(r, "error", None) is not None

        if not failed:
            continue

        err = getattr(r, "error", None)
        is_oom = _is_oom_error(err)
        ops = getattr(r, "ops", None) or []
        pid = getattr(r, "pipeline_id", None)  # may exist in this simulator; handle None robustly

        if not ops:
            # If we can't attribute to an op, safest is to avoid retrying the pipeline if we can identify it.
            if not is_oom and pid is not None:
                s.bad_pipelines.add(pid)
            continue

        for op in ops:
            k = (pid, _op_id(op)) if pid is not None else _make_fallback_key(op)
            fk = _make_fallback_key(op)

            if is_oom:
                # Increase RAM hint based on last attempted RAM if available, otherwise start from current hint or 1.0
                baseline = getattr(r, "ram", None)
                baseline = _min_positive(baseline, 1.0) if baseline is not None else None
                prev = s.ram_hints.get(k, s.ram_hints.get(fk, None))
                prev = _min_positive(prev, 1.0) if prev is not None else None

                start = prev if prev is not None else (baseline if baseline is not None else 1.0)
                new_ram = max(1.0, float(start) * float(s.ram_backoff))

                s.ram_hints[k] = new_ram
                s.ram_hints[fk] = max(_min_positive(s.ram_hints.get(fk, 1.0), 1.0), new_ram)

                s.retryable_ops.add(k)
                s.retryable_ops.add(fk)

                s.retry_counts[k] = int(s.retry_counts.get(k, 0)) + 1
                if s.retry_counts[k] > int(s.max_retries_per_op):
                    # Give up: mark pipeline bad if we can identify it
                    if pid is not None:
                        s.bad_pipelines.add(pid)
            else:
                # Non-OOM failure: never retry
                s.nonretryable_ops.add(k)
                s.nonretryable_ops.add(fk)
                if pid is not None:
                    s.bad_pipelines.add(pid)

    # Early exit if nothing changed
    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    # Per-tick duplicate protection
    assigned_pipeline_ids = set()
    assigned_op_ids = set()

    # Determine whether to protect headroom from batch
    hp_backlog_ready = _queue_has_ready_hp(s, limit=8)

    # Order pools to prefer interactive pool first for fastest HP service
    pool_order = list(range(s.executor.num_pools))
    if s.executor.num_pools > 1 and s.interactive_pool_id in pool_order:
        pool_order.remove(s.interactive_pool_id)
        pool_order = [s.interactive_pool_id] + pool_order

    # For each pool, schedule up to max_assignments_per_pool ops, updating local available resources.
    for pool_id in pool_order:
        pool = s.executor.pools[pool_id]
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)
        if avail_cpu <= 0.0 or avail_ram <= 0.0:
            continue

        reserve_cpu, reserve_ram = _reserve_amounts(s, pool, pool_id, hp_backlog_ready)

        made = 0
        # Keep a small local list of popped pipelines to requeue (preserves FIFO-ish behavior)
        requeue = {Priority.QUERY: [], Priority.INTERACTIVE: [], Priority.BATCH_PIPELINE: []}

        while made < int(s.max_assignments_per_pool) and avail_cpu > 0.0 and avail_ram > 0.0:
            chosen = None
            chosen_pr = None
            chosen_op = None
            req_cpu = None
            req_ram = None

            # Try priorities in order; for batch, require headroom reservation if HP backlog exists.
            for pr in _prio_order():
                q = s.waiting_queues.get(pr, [])
                while q:
                    p = q.pop(0)

                    # Drop completed pipelines
                    st = p.runtime_status()
                    if st.is_pipeline_successful():
                        continue

                    # Drop pipelines known-bad
                    if getattr(p, "pipeline_id", None) in s.bad_pipelines:
                        continue

                    op = _pick_schedulable_op(s, p, assigned_pipeline_ids, assigned_op_ids)
                    if op is None:
                        # Not ready or not schedulable now; requeue for later ticks
                        requeue[pr].append(p)
                        continue

                    # Compute request
                    if pr in (Priority.QUERY, Priority.INTERACTIVE):
                        cpu, ram = _hp_request(s, pool, p.pipeline_id, op)
                        # Cap to local availability
                        cpu = min(cpu, avail_cpu)
                        ram = min(ram, avail_ram)
                        if cpu < 1.0 or ram < 1.0:
                            requeue[pr].append(p)
                            continue
                        chosen = p
                        chosen_pr = pr
                        chosen_op = op
                        req_cpu = cpu
                        req_ram = ram
                        break

                    # Batch
                    # If HP backlog exists, only take what's beyond reservation
                    cpu, ram = _batch_request(avail_cpu, avail_ram, reserve_cpu, reserve_ram)
                    if cpu is None or ram is None:
                        # Can't schedule batch without violating headroom; push pipeline back and stop trying batch
                        requeue[pr].append(p)
                        break

                    # Cap to local availability (should already be)
                    cpu = min(cpu, avail_cpu)
                    ram = min(ram, avail_ram)
                    if cpu < 1.0 or ram < 1.0:
                        requeue[pr].append(p)
                        continue

                    chosen = p
                    chosen_pr = pr
                    chosen_op = op
                    req_cpu = cpu
                    req_ram = ram
                    break

                if chosen is not None:
                    break

            # Requeue any pipelines we temporarily popped but didn't schedule
            for pr in _prio_order():
                if requeue[pr]:
                    # Put them back at the front to preserve FIFO-ish semantics within priority
                    s.waiting_queues[pr] = requeue[pr] + s.waiting_queues[pr]
                    requeue[pr] = []

            if chosen is None:
                break

            # Final safety checks
            if req_cpu is None or req_ram is None:
                break
            if req_cpu > avail_cpu or req_ram > avail_ram:
                # Shouldn't happen due to our local caps; just stop on this pool.
                break

            # Create assignment
            assignments.append(
                Assignment(
                    ops=[chosen_op],
                    cpu=req_cpu,
                    ram=req_ram,
                    priority=chosen_pr,
                    pool_id=pool_id,
                    pipeline_id=chosen.pipeline_id,
                )
            )

            # Update per-tick duplicate protection
            assigned_pipeline_ids.add(chosen.pipeline_id)
            assigned_op_ids.add(_op_id(chosen_op))

            # Update local availability
            avail_cpu -= float(req_cpu)
            avail_ram -= float(req_ram)
            made += 1

            # Re-enqueue pipeline so it can make progress in future ticks
            s.waiting_queues[chosen_pr].append(chosen)

    return suspensions, assignments