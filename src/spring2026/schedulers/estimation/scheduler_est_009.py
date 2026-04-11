# policy_key: scheduler_est_009
# reasoning_effort: low
# model: gpt-5.2-2025-12-11
# llm_cost: 0.042020
# generation_seconds: 44.03
# generated_at: 2026-03-31T23:51:02.842121
@register_scheduler_init(key="scheduler_est_009")
def scheduler_est_009_init(s):
    """
    Priority-aware, estimate-aware FIFO scheduler (small, safe improvements over naive FIFO).

    Improvements vs naive:
    - Maintain separate queues per priority and always schedule higher priorities first.
    - Use op.estimate.mem_peak_gb as a RAM floor (plus a small safety margin) to reduce OOMs.
    - On failures (esp. OOM-like), increase a remembered per-op RAM floor for retries.
    - Avoid letting batch consume all capacity by keeping a small reservation for high-priority work
      (implemented as: if any high-priority runnable ops exist, don't schedule batch).
    - Mild CPU capping for batch so one batch op doesn't monopolize a pool.
    """
    s.waiting_by_prio = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }
    # Remember a per-operator RAM floor that we increase after failures.
    # Keyed by id(op) since we don't have a guaranteed stable op identifier in the interface.
    s.op_mem_floor_gb = {}

    # Round-robin cursor per priority to avoid repeatedly picking the same pipeline.
    s.rr_cursor = {
        Priority.QUERY: 0,
        Priority.INTERACTIVE: 0,
        Priority.BATCH_PIPELINE: 0,
    }

    # A small per-priority minimum RAM floor (GB) to reduce tiny allocations.
    s.prio_min_ram_gb = {
        Priority.QUERY: 1.0,
        Priority.INTERACTIVE: 1.0,
        Priority.BATCH_PIPELINE: 1.0,
    }


@register_scheduler(key="scheduler_est_009")
def scheduler_est_009_scheduler(s, results: list, pipelines: list):
    """
    Scheduler step:
    - Ingest new pipelines into per-priority queues.
    - Update RAM floors based on failures from the last tick.
    - For each pool, schedule as many runnable operators as fit, picking higher priorities first.
    """
    suspensions = []
    assignments = []

    # Enqueue newly arrived pipelines by priority.
    for p in pipelines:
        if p.priority not in s.waiting_by_prio:
            # Defensive fallback: treat unknown as batch.
            s.waiting_by_prio[Priority.BATCH_PIPELINE].append(p)
        else:
            s.waiting_by_prio[p.priority].append(p)

    # Update per-operator RAM floors based on failures.
    for r in results:
        if not getattr(r, "failed", lambda: False)():
            continue
        # If an operator failed, increase its remembered RAM floor so a retry is less likely to OOM again.
        # We don't have a typed error, so treat any failure as a hint, with extra weight for OOM-like text.
        err = str(getattr(r, "error", "") or "").lower()
        oom_like = ("oom" in err) or ("out of memory" in err) or ("memory" in err)

        # r.ops is expected to be a list of operators.
        for op in getattr(r, "ops", []) or []:
            k = id(op)
            prev = float(s.op_mem_floor_gb.get(k, 0.0) or 0.0)
            tried = float(getattr(r, "ram", 0.0) or 0.0)

            # Backoff: on oom-like errors, be more aggressive.
            if oom_like:
                bumped = max(prev, tried) * 2.0 if max(prev, tried) > 0 else 2.0
            else:
                bumped = max(prev, tried) * 1.5 if max(prev, tried) > 0 else 1.5

            # Cap will be applied later by pool availability; keep a reasonable minimum bump.
            s.op_mem_floor_gb[k] = max(prev, bumped, 1.0)

    # Early exit if nothing to do.
    if not pipelines and not results:
        return suspensions, assignments

    def _pipeline_done_or_failed(p):
        st = p.runtime_status()
        if st.is_pipeline_successful():
            return True
        # If anything is FAILED, we consider the pipeline failed and stop retrying (matches baseline behavior).
        # Note: some runtimes may use FAILED for retryable steps; we keep it simple/safe.
        has_failures = st.state_counts[OperatorState.FAILED] > 0
        return has_failures

    def _next_runnable_op(p):
        st = p.runtime_status()
        # Only assign ops whose parents are complete; prefer first available (FIFO within pipeline).
        op_list = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        if not op_list:
            return None
        return op_list[0]

    def _has_any_runnable(prio):
        for p in s.waiting_by_prio.get(prio, []):
            if _pipeline_done_or_failed(p):
                continue
            if _next_runnable_op(p) is not None:
                return True
        return False

    def _pick_next_pipeline_rr(prio):
        q = s.waiting_by_prio.get(prio, [])
        if not q:
            return None, None

        n = len(q)
        start = s.rr_cursor.get(prio, 0) % max(n, 1)
        # Scan all pipelines once to find a runnable one.
        for i in range(n):
            idx = (start + i) % n
            p = q[idx]
            if _pipeline_done_or_failed(p):
                continue
            op = _next_runnable_op(p)
            if op is not None:
                s.rr_cursor[prio] = (idx + 1) % n
                return p, op
        return None, None

    def _ram_floor_for_op(op, prio):
        est = getattr(getattr(op, "estimate", None), "mem_peak_gb", None)
        est = float(est) if est is not None else 0.0
        learned = float(s.op_mem_floor_gb.get(id(op), 0.0) or 0.0)
        base = float(s.prio_min_ram_gb.get(prio, 1.0) or 1.0)
        # Use estimate as a floor, plus a small margin to reduce borderline OOM.
        floor = max(base, learned, est * 1.10 if est > 0 else 0.0)
        return max(floor, 1.0)

    def _cpu_cap_for_prio(pool, prio):
        # Mild caps to avoid batch monopolizing a pool; high-priority can burst more.
        max_cpu = float(getattr(pool, "max_cpu_pool", 0.0) or 0.0)
        if max_cpu <= 0:
            return None  # no cap information; caller will use avail
        if prio == Priority.QUERY:
            return max(1.0, max_cpu * 1.00)
        if prio == Priority.INTERACTIVE:
            return max(1.0, max_cpu * 0.85)
        return max(1.0, max_cpu * 0.50)  # batch

    # Clean queues by removing completed/failed pipelines (best-effort, keep order).
    for prio, q in list(s.waiting_by_prio.items()):
        s.waiting_by_prio[prio] = [p for p in q if not _pipeline_done_or_failed(p)]

    # Main scheduling: per pool, schedule multiple ops while resources remain.
    prio_order = [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]

    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)

        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # If any high-priority runnable work exists, avoid scheduling batch into this pool
        # (simple reservation policy that improves tail latency without needing preemption).
        high_prio_runnable = _has_any_runnable(Priority.QUERY) or _has_any_runnable(Priority.INTERACTIVE)

        made_progress = True
        while made_progress:
            made_progress = False

            for prio in prio_order:
                if prio == Priority.BATCH_PIPELINE and high_prio_runnable:
                    continue

                if avail_cpu <= 0 or avail_ram <= 0:
                    break

                p, op = _pick_next_pipeline_rr(prio)
                if p is None or op is None:
                    continue

                # Compute RAM allocation: floor + small "free" padding if available.
                ram_floor = _ram_floor_for_op(op, prio)
                if ram_floor > avail_ram:
                    # Can't fit this op right now; try other priorities/pipelines.
                    continue

                # Give a bit extra RAM when possible (extra RAM is "free" for performance, but improves OOM safety).
                # Keep it conservative to allow parallelism.
                target_ram = min(avail_ram, max(ram_floor, min(ram_floor * 1.25, ram_floor + 2.0)))

                # Compute CPU allocation.
                cpu_cap = _cpu_cap_for_prio(pool, prio)
                if cpu_cap is None:
                    target_cpu = avail_cpu
                else:
                    target_cpu = min(avail_cpu, cpu_cap)

                # Always allocate at least 1 vCPU if any is available (avoid 0-CPU assignments).
                if target_cpu < 1.0:
                    if avail_cpu >= 1.0:
                        target_cpu = 1.0
                    else:
                        continue

                # Create the assignment for a single op (keep behavior close to baseline, but allow multiple per tick).
                assignments.append(
                    Assignment(
                        ops=[op],
                        cpu=target_cpu,
                        ram=target_ram,
                        priority=p.priority,
                        pool_id=pool_id,
                        pipeline_id=p.pipeline_id,
                    )
                )

                # Update local availability for packing multiple ops per pool per tick.
                avail_cpu -= target_cpu
                avail_ram -= target_ram
                made_progress = True

                # Recompute whether we should keep batch blocked (if high-priority queues have runnable ops).
                high_prio_runnable = _has_any_runnable(Priority.QUERY) or _has_any_runnable(Priority.INTERACTIVE)

                if avail_cpu <= 0 or avail_ram <= 0:
                    break

    return suspensions, assignments
