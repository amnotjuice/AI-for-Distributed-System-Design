# policy_key: scheduler_est_036
# reasoning_effort: low
# model: gpt-5.2-2025-12-11
# llm_cost: 0.041390
# generation_seconds: 42.94
# generated_at: 2026-04-01T00:08:51.488915
@register_scheduler_init(key="scheduler_est_036")
def scheduler_est_036_init(s):
    """Priority-aware, estimate-aware incremental improvement over naive FIFO.

    Main changes vs naive:
    - Maintain per-priority queues; schedule higher priority first (QUERY > INTERACTIVE > BATCH).
    - Right-size CPU and RAM per operator instead of giving the whole pool to one op.
    - Use op.estimate.mem_peak_gb (plus a small safety margin) as a RAM floor to reduce OOMs.
    - On OOM-like failures, learn a higher RAM floor for that operator and retry a limited number of times.
    - Avoid head-of-line blocking by scanning within each priority queue for a runnable pipeline.
    """
    # Per-priority pipeline queues (FIFO within each priority)
    s.queues = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }

    # Learned per-operator minimum RAM floors after OOMs.
    # key: (pipeline_id, op_key) -> learned_floor_gb
    s.learned_ram_floor_gb = {}

    # Retry budget for OOM-like failures per operator.
    # key: (pipeline_id, op_key) -> retries_used
    s.oom_retries_used = {}

    # Config knobs (kept simple and conservative)
    s.max_oom_retries_per_op = 2
    s.mem_safety_factor = 1.30  # allocate a bit above estimate/learned floor when possible

    # CPU target fractions by priority (relative to pool max), bounded to at least 1 vCPU
    s.cpu_frac_by_prio = {
        Priority.QUERY: 0.50,
        Priority.INTERACTIVE: 0.25,
        Priority.BATCH_PIPELINE: 0.125,
    }

    # Minimum RAM to attempt when we have no estimate (GB)
    s.default_min_ram_gb = 1.0


@register_scheduler(key="scheduler_est_036")
def scheduler_est_036_scheduler(s, results, pipelines):
    """
    Priority-aware, estimate-aware scheduler.

    Strategy:
    - Enqueue arrivals into per-priority FIFO queues.
    - Process results: if an op failed with OOM-like error, bump learned RAM floor and retry (limited).
    - For each pool: repeatedly pick the next runnable pipeline by priority, assign exactly one runnable op,
      using CPU/RAM sizing that aims to reduce latency for high priority and increase concurrency overall.
    """
    def _op_key(op):
        # Prefer stable user-facing keys if present; otherwise fall back to object id.
        for attr in ("op_id", "operator_id", "id", "name"):
            if hasattr(op, attr):
                try:
                    v = getattr(op, attr)
                    # Avoid non-hashable structures
                    return v if isinstance(v, (int, float, str, tuple)) else str(v)
                except Exception:
                    pass
        return id(op)

    def _is_oom_error(err):
        if err is None:
            return False
        try:
            msg = str(err).lower()
        except Exception:
            return False
        return ("oom" in msg) or ("out of memory" in msg) or ("memory" in msg and "exceed" in msg)

    def _prio_order():
        return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]

    def _get_runnable_op(pipeline):
        status = pipeline.runtime_status()
        # Drop if successful
        if status.is_pipeline_successful():
            return None
        # Find first runnable op whose parents are complete
        ops = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        if not ops:
            return None
        return ops[0]

    def _desired_cpu(pool, priority, avail_cpu):
        # Target a fraction of the pool, but don't exceed availability.
        frac = s.cpu_frac_by_prio.get(priority, 0.125)
        target = max(1, int(round(pool.max_cpu_pool * frac)))
        if target > avail_cpu:
            target = avail_cpu
        # Must be at least 1 to make progress.
        return max(0, int(target))

    def _desired_ram_gb(pool, pipeline_id, op, avail_ram):
        # Compute a RAM floor from (estimate, learned floor, default).
        est = None
        try:
            est = getattr(getattr(op, "estimate", None), "mem_peak_gb", None)
        except Exception:
            est = None

        key = (pipeline_id, _op_key(op))
        learned = s.learned_ram_floor_gb.get(key, 0.0)

        floor_gb = max(s.default_min_ram_gb, float(learned or 0.0), float(est or 0.0))

        # Request a bit above the floor when possible; RAM beyond minimum is "free" for performance,
        # but we still avoid consuming the whole pool to allow concurrency.
        request_gb = floor_gb * s.mem_safety_factor

        # Soft cap: don't take more than 60% of pool RAM for a single op unless needed by floor.
        soft_cap = 0.60 * float(pool.max_ram_pool)
        request_gb = min(request_gb, soft_cap)
        request_gb = max(request_gb, floor_gb)

        # Bound by availability.
        if request_gb > avail_ram:
            # If we can't meet the floor, we must not schedule it (likely to OOM).
            if floor_gb > avail_ram:
                return None
            # Otherwise schedule with what's available (still >= floor).
            request_gb = avail_ram

        return float(request_gb)

    # Enqueue new pipelines by priority
    for p in pipelines:
        if p.priority not in s.queues:
            # Be robust to unknown priorities; treat as batch-like.
            s.queues[Priority.BATCH_PIPELINE].append(p)
        else:
            s.queues[p.priority].append(p)

    # Process execution results: learn from OOMs and decide whether to retry.
    # NOTE: We do not directly manipulate operator states; Eudoxia will reflect FAILED in runtime_status().
    # We keep pipelines in queues so they can be retried by re-scheduling their FAILED ops.
    for r in (results or []):
        if not getattr(r, "failed", lambda: False)():
            continue
        if not _is_oom_error(getattr(r, "error", None)):
            continue

        # Increase learned RAM floor for each failed op and record a retry.
        for op in getattr(r, "ops", []) or []:
            key = (getattr(r, "pipeline_id", None), _op_key(op))
            # If we don't have pipeline_id in results, fall back to searching is expensive; skip learning.
            if key[0] is None:
                continue
            used = s.oom_retries_used.get(key, 0)
            if used >= s.max_oom_retries_per_op:
                continue

            prev_floor = float(s.learned_ram_floor_gb.get(key, 0.0))
            # Bump based on what we attempted plus headroom (or at least +25%).
            attempted = float(getattr(r, "ram", 0.0) or 0.0)
            bumped = max(prev_floor, max(attempted * 1.25, prev_floor * 1.25, prev_floor + 1.0))
            s.learned_ram_floor_gb[key] = bumped
            s.oom_retries_used[key] = used + 1

    suspensions = []
    assignments = []

    # Helper to pick next runnable pipeline from queues without head-of-line blocking.
    def _pop_next_runnable_pipeline(priority):
        q = s.queues.get(priority, [])
        if not q:
            return None
        # Scan a limited number of elements to find one with runnable work.
        # Keep order mostly FIFO while avoiding total blocking.
        scan_n = min(len(q), 8)
        for i in range(scan_n):
            p = q[i]
            op = _get_runnable_op(p)
            if op is not None:
                # Remove from its position and return.
                del q[i]
                return p
        return None

    # Schedule per pool: pack multiple ops with right-sized resource requests.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = int(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)

        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # Keep scheduling until we cannot fit any more work in this pool.
        made_progress = True
        while made_progress:
            made_progress = False

            # Try priorities in order each iteration.
            chosen_pipeline = None
            chosen_priority = None
            chosen_op = None

            for prio in _prio_order():
                p = _pop_next_runnable_pipeline(prio)
                if p is None:
                    continue
                op = _get_runnable_op(p)
                if op is None:
                    # Put it back (unlikely due to check), and continue.
                    s.queues[prio].append(p)
                    continue

                # Compute resource request; if it doesn't fit, requeue and try next.
                cpu_req = _desired_cpu(pool, prio, avail_cpu)
                if cpu_req <= 0:
                    s.queues[prio].append(p)
                    continue

                ram_req = _desired_ram_gb(pool, p.pipeline_id, op, avail_ram)
                if ram_req is None or ram_req <= 0:
                    s.queues[prio].append(p)
                    continue

                # Fits current pool budget.
                chosen_pipeline = p
                chosen_priority = prio
                chosen_op = op
                chosen_cpu = cpu_req
                chosen_ram = ram_req
                break

            if chosen_pipeline is None:
                # Nothing runnable that fits in remaining resources.
                break

            # Create assignment for exactly one operator (keeps semantics close to baseline).
            assignment = Assignment(
                ops=[chosen_op],
                cpu=chosen_cpu,
                ram=chosen_ram,
                priority=chosen_priority,
                pool_id=pool_id,
                pipeline_id=chosen_pipeline.pipeline_id,
            )
            assignments.append(assignment)

            # Update remaining pool resources for additional packing.
            avail_cpu -= int(chosen_cpu)
            avail_ram -= float(chosen_ram)
            made_progress = True

            # Requeue the pipeline to allow subsequent ops to be scheduled later.
            # (If it becomes blocked on dependencies, it will be skipped by runnable scan.)
            s.queues[chosen_priority].append(chosen_pipeline)

            if avail_cpu <= 0 or avail_ram <= 0:
                break

    return suspensions, assignments
