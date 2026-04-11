# policy_key: scheduler_est_017
# reasoning_effort: low
# model: gpt-5.2-2025-12-11
# llm_cost: 0.047956
# generation_seconds: 41.16
# generated_at: 2026-03-31T23:56:14.240088
@register_scheduler_init(key="scheduler_est_017")
def scheduler_est_017_init(s):
    """Priority-aware FIFO with simple sizing and conservative headroom.

    Small, obvious improvements over naive FIFO:
      - Maintain separate waiting queues per priority and always prefer higher priority.
      - Size RAM using the operator's memory estimate (as a floor) and learn from failures
        via simple multiplicative backoff for the next retry.
      - Keep modest per-pool headroom for high-priority work to reduce latency under load.

    Notes:
      - This version avoids preemption to keep churn low and because container/run-state
        inspection APIs are not assumed.
      - Schedules one operator per assignment (simple and robust); aims to increase
        concurrency by not always allocating the entire pool to one task.
    """
    s.waiting_by_prio = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }
    # Learned per-operator memory floors (GB) keyed by a stable-ish string.
    s.op_mem_floor_gb = {}
    # Per-priority default CPU targets (vCPU) for interactive latency vs batch throughput.
    s.cpu_target_by_prio = {
        Priority.QUERY: 4.0,
        Priority.INTERACTIVE: 3.0,
        Priority.BATCH_PIPELINE: 2.0,
    }
    # Per-priority maximum share of a pool we let a single operator consume.
    s.cpu_share_cap_by_prio = {
        Priority.QUERY: 0.75,
        Priority.INTERACTIVE: 0.65,
        Priority.BATCH_PIPELINE: 0.50,
    }
    # Minimum CPU per assignment to avoid pathological tiny slices.
    s.min_cpu_per_assignment = 1.0


@register_scheduler(key="scheduler_est_017")
def scheduler_est_017_scheduler(s, results, pipelines):
    """
    Scheduler step:
      1) Enqueue new pipelines into priority-specific FIFO queues.
      2) Update learned memory floors on failures (especially OOM-like).
      3) For each pool, admit ready operators in strict priority order with headroom:
         - If any QUERY/INTERACTIVE work is runnable, reserve some CPU/RAM from batch.
         - Use op.estimate.mem_peak_gb (and learned backoff) as a RAM floor.
         - Allocate moderate CPU per op (not always the whole pool) to improve concurrency.
    """
    def _op_key(op):
        # Best-effort stable key: prefer explicit identifiers if present.
        for attr in ("op_id", "operator_id", "id", "name"):
            if hasattr(op, attr):
                try:
                    v = getattr(op, attr)
                    if v is not None:
                        return f"{attr}:{v}"
                except Exception:
                    pass
        # Fallback: repr is stable enough within a simulation run.
        return f"repr:{repr(op)}"

    def _is_oom_error(err):
        if err is None:
            return False
        try:
            msg = str(err).lower()
        except Exception:
            return False
        return ("oom" in msg) or ("out of memory" in msg) or ("memory" in msg and "killed" in msg)

    def _pipeline_done_or_failed(p):
        status = p.runtime_status()
        if status.is_pipeline_successful():
            return True
        # If any op has failed, consider the pipeline failed (naive baseline behavior).
        # (We still allow failed ops to be ASSIGNABLE via ASSIGNABLE_STATES; but if the
        # pipeline has any failed ops and no retry semantics are desired, drop it.)
        # Here we keep it conservative: only drop if there are failures AND no assignable ops remain.
        try:
            has_failures = status.state_counts[OperatorState.FAILED] > 0
        except Exception:
            has_failures = False
        if not has_failures:
            return False
        # If failures exist but there are still assignable ops (including FAILED), keep it to retry.
        ops_retry = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        return len(ops_retry) == 0

    def _get_next_ready_op(p):
        status = p.runtime_status()
        op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        if not op_list:
            return None
        return op_list[0]

    def _has_runnable_priority(prio):
        q = s.waiting_by_prio.get(prio, [])
        for p in q:
            if _pipeline_done_or_failed(p):
                continue
            op = _get_next_ready_op(p)
            if op is not None:
                return True
        return False

    def _pop_next_pipeline_with_ready_op(prio):
        q = s.waiting_by_prio[prio]
        requeue = []
        chosen = None
        chosen_op = None

        while q:
            p = q.pop(0)
            if _pipeline_done_or_failed(p):
                continue
            op = _get_next_ready_op(p)
            if op is None:
                requeue.append(p)
                continue
            chosen = p
            chosen_op = op
            break

        # Put back the rest preserving relative order.
        if requeue:
            q[0:0] = requeue
        return chosen, chosen_op

    def _estimate_mem_floor_gb(op):
        # Use estimator as a RAM floor; extra RAM is "free" (only affects feasibility).
        est = None
        try:
            est = getattr(getattr(op, "estimate", None), "mem_peak_gb", None)
        except Exception:
            est = None

        floor = 0.0
        if isinstance(est, (int, float)) and est and est > 0:
            floor = float(est)

        key = _op_key(op)
        learned = s.op_mem_floor_gb.get(key, 0.0)
        if learned > floor:
            floor = learned

        # Always keep a small safety margin if we have any estimate.
        if floor > 0:
            floor = floor * 1.10 + 0.25  # +256MB-ish cushion
        return max(0.0, floor)

    # 1) Enqueue new pipelines.
    for p in pipelines:
        pr = getattr(p, "priority", Priority.BATCH_PIPELINE)
        if pr not in s.waiting_by_prio:
            # Unknown priority buckets fall back to batch behavior.
            pr = Priority.BATCH_PIPELINE
        s.waiting_by_prio[pr].append(p)

    # Early exit if nothing changed.
    if not pipelines and not results:
        return [], []

    # 2) Learn from results: bump memory floor on failures, especially OOM-like.
    for r in results:
        try:
            if not r.failed():
                continue
        except Exception:
            continue

        # If we can identify ops, update learned floor for each.
        ops = []
        try:
            ops = list(getattr(r, "ops", []) or [])
        except Exception:
            ops = []

        for op in ops:
            key = _op_key(op)
            prev = s.op_mem_floor_gb.get(key, 0.0)

            # If it looks like OOM, back off aggressively; otherwise, mild bump.
            oomish = _is_oom_error(getattr(r, "error", None))
            if oomish:
                # Try to use observed allocated RAM as a baseline, then grow.
                try:
                    allocated = float(getattr(r, "ram", 0.0) or 0.0)
                except Exception:
                    allocated = 0.0
                base = max(prev, allocated, _estimate_mem_floor_gb(op))
                new_floor = max(base * 1.5 + 0.5, base + 1.0)
            else:
                new_floor = max(prev * 1.10 + 0.25, prev + 0.25)

            s.op_mem_floor_gb[key] = new_floor

    suspensions = []
    assignments = []

    # Determine whether we should reserve headroom for high-priority work.
    hi_waiting = _has_runnable_priority(Priority.QUERY) or _has_runnable_priority(Priority.INTERACTIVE)

    # 3) Schedule per pool.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = float(getattr(pool, "avail_cpu_pool", 0.0) or 0.0)
        avail_ram = float(getattr(pool, "avail_ram_pool", 0.0) or 0.0)
        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # Reserve some capacity for high priority when present, to protect latency.
        # This reduces the chance that batch consumes everything just before a query arrives.
        reserve_cpu = 0.0
        reserve_ram = 0.0
        if hi_waiting:
            reserve_cpu = 0.15 * float(getattr(pool, "max_cpu_pool", avail_cpu) or avail_cpu)
            reserve_ram = 0.15 * float(getattr(pool, "max_ram_pool", avail_ram) or avail_ram)

        # Helper to compute usable capacity for a given priority.
        def _usable_capacity(prio, cur_avail_cpu, cur_avail_ram):
            if prio == Priority.BATCH_PIPELINE and hi_waiting:
                return max(0.0, cur_avail_cpu - reserve_cpu), max(0.0, cur_avail_ram - reserve_ram)
            return cur_avail_cpu, cur_avail_ram

        # Keep placing while we have resources.
        while True:
            made_progress = False

            for prio in (Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE):
                use_cpu, use_ram = _usable_capacity(prio, avail_cpu, avail_ram)
                if use_cpu < s.min_cpu_per_assignment or use_ram <= 0:
                    continue

                p, op = _pop_next_pipeline_with_ready_op(prio)
                if p is None or op is None:
                    continue

                mem_floor = _estimate_mem_floor_gb(op)
                # If we have no estimate, still allocate a small default to avoid pathological 0.
                if mem_floor <= 0:
                    mem_floor = 1.0

                if mem_floor > use_ram:
                    # Not enough RAM in this pool right now; requeue and try another prio/pool.
                    s.waiting_by_prio[prio].append(p)
                    continue

                # CPU sizing: cap share per prio, target a modest number for concurrency.
                share_cap = float(s.cpu_share_cap_by_prio.get(prio, 0.5))
                cpu_cap = max(s.min_cpu_per_assignment, share_cap * float(getattr(pool, "max_cpu_pool", use_cpu) or use_cpu))
                cpu_target = float(s.cpu_target_by_prio.get(prio, 2.0))

                cpu_alloc = min(use_cpu, cpu_cap, cpu_target)
                cpu_alloc = max(s.min_cpu_per_assignment, cpu_alloc)

                # RAM sizing: allocate at least the floor; beyond that, keep some slack but don't hog.
                # Since extra RAM is "free" for performance, we mainly avoid blocking others.
                # Give queries a bit more slack.
                slack_mult = 1.25 if prio in (Priority.QUERY, Priority.INTERACTIVE) else 1.10
                ram_alloc = min(use_ram, max(mem_floor, mem_floor * slack_mult))
                # Ensure we don't allocate below floor due to min/max rounding.
                if ram_alloc < mem_floor:
                    ram_alloc = mem_floor

                assignment = Assignment(
                    ops=[op],
                    cpu=cpu_alloc,
                    ram=ram_alloc,
                    priority=p.priority,
                    pool_id=pool_id,
                    pipeline_id=p.pipeline_id,
                )
                assignments.append(assignment)

                # Requeue pipeline for remaining ops.
                s.waiting_by_prio[prio].append(p)

                avail_cpu -= cpu_alloc
                avail_ram -= ram_alloc
                made_progress = True
                break  # restart from highest priority

            if not made_progress:
                break

    return suspensions, assignments
