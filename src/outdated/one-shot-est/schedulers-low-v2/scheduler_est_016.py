# policy_key: scheduler_est_016
# reasoning_effort: low
# model: gpt-5.2-2025-12-11
# llm_cost: 0.044454
# generation_seconds: 45.44
# generated_at: 2026-04-01T13:47:12.132433
@register_scheduler_init(key="scheduler_est_016")
def scheduler_est_016_init(s):
    """Priority-aware FIFO with conservative memory sizing and basic fairness.

    Incremental improvements over naive FIFO:
      1) Maintain separate queues per priority and always try to schedule higher priority first.
      2) Avoid starving high-priority work by holding back batch when any high-priority is waiting.
      3) Use operator-provided memory estimates (if present) with a safety factor.
      4) On failures, pessimistically raise a per-operator RAM floor to reduce repeated OOM churn.
      5) Allocate CPU by priority share to reduce "one op grabs everything" behavior while still
         favoring interactive/query latency.
    """
    s.queues = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }
    # Per-operator RAM floor (GB). Keyed by (pipeline_id, id(op)).
    s.op_ram_floor_gb = {}
    # Per-operator failure count to increase RAM more aggressively on repeated failures.
    s.op_fail_count = {}
    # Simple per-priority round-robin cursor (implemented by rotating lists).
    s._tick = 0


@register_scheduler(key="scheduler_est_016")
def scheduler_est_016(s, results: list, pipelines: list):
    """
    Scheduler step.

    Strategy:
      - Enqueue arriving pipelines into per-priority queues.
      - Learn from failures: raise RAM floor for the failing ops.
      - For each pool, repeatedly assign ready ops:
          * Priority order: QUERY > INTERACTIVE > BATCH_PIPELINE
          * If any QUERY/INTERACTIVE is waiting, throttle BATCH admissions (reserve headroom).
          * Size RAM from (estimate * safety) bounded by pool availability, never below learned floor.
          * Size CPU using priority-based pool shares.
    """
    s._tick += 1

    # ---- helpers (kept inside to avoid imports / external dependencies) ----
    def _prio_list():
        return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]

    def _has_waiting_high_prio():
        return (len(s.queues[Priority.QUERY]) > 0) or (len(s.queues[Priority.INTERACTIVE]) > 0)

    def _is_pipeline_done_or_failed(p):
        st = p.runtime_status()
        # If any op is FAILED, treat as failed pipeline (naive baseline behavior).
        has_failures = st.state_counts[OperatorState.FAILED] > 0
        return st.is_pipeline_successful() or has_failures

    def _ready_ops(p):
        st = p.runtime_status()
        return st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)

    def _op_key(pipeline_id, op):
        return (pipeline_id, id(op))

    def _get_op_est_mem_gb(op):
        # Estimator may attach op.estimate.mem_peak_gb (float or None).
        est = None
        try:
            est = op.estimate.mem_peak_gb
        except Exception:
            est = None
        # Defensive: ignore non-positive or non-numeric.
        try:
            if est is None:
                return None
            est = float(est)
            if est <= 0:
                return None
            return est
        except Exception:
            return None

    def _priority_cpu_share(priority):
        # Shares bias toward lower latency for high priority while allowing concurrency.
        if priority == Priority.QUERY:
            return 0.75
        if priority == Priority.INTERACTIVE:
            return 0.60
        return 0.35  # batch

    def _reserve_share_for_high_prio():
        # When high-priority exists, keep some headroom by throttling batch admissions.
        # (We cannot preempt safely without a running-container inventory API.)
        return 0.20

    def _pick_next_pipeline(priority):
        # Rotate queue until we find a pipeline that is not done/failed and has a ready op.
        q = s.queues[priority]
        for _ in range(len(q)):
            p = q.pop(0)
            if _is_pipeline_done_or_failed(p):
                continue
            if _ready_ops(p):
                # Put it back at tail to get RR fairness among pipelines of same priority.
                q.append(p)
                return p
            # Not ready yet (waiting on parents); keep it in queue to check later.
            q.append(p)
        return None

    def _choose_op_from_pipeline(p):
        ops = _ready_ops(p)
        if not ops:
            return None
        # Small improvement: pick one op at a time to reduce contention and improve tail latency.
        return ops[0]

    def _size_ram_gb(pool, pipeline_id, op, avail_ram_gb):
        # Base from estimate with safety factor; fallback to a small conservative slice.
        est = _get_op_est_mem_gb(op)
        safety = 1.20
        if est is None:
            # Without estimate, avoid grabbing the whole pool; use a conservative chunk.
            # Still, never exceed availability.
            base = min(avail_ram_gb, max(1.0, 0.35 * pool.max_ram_pool))
        else:
            base = min(avail_ram_gb, est * safety)

        k = _op_key(pipeline_id, op)
        floor = s.op_ram_floor_gb.get(k, 0.0)
        # Ensure we respect learned floor.
        ram = max(base, floor, 1.0)

        # Hard cap to availability.
        if ram > avail_ram_gb:
            ram = avail_ram_gb

        # If we still cannot allocate at least 1 GB (or any positive amount), signal failure to place.
        if ram <= 0:
            return 0.0
        return ram

    def _size_cpu(pool, priority, avail_cpu):
        # Allocate CPU using shares, but ensure progress with at least 1 vCPU if possible.
        share = _priority_cpu_share(priority)
        target = share * pool.max_cpu_pool
        cpu = min(avail_cpu, target)

        # Avoid pathological tiny allocations if CPU is fractional.
        if cpu < 1 and avail_cpu >= 1:
            cpu = 1
        # If still < 1, allow fractional if simulator supports it; otherwise it won't place.
        return cpu

    # ---- ingest new pipelines ----
    for p in pipelines:
        # Default to batch if unknown; but Priority enum is expected.
        pr = getattr(p, "priority", Priority.BATCH_PIPELINE)
        if pr not in s.queues:
            pr = Priority.BATCH_PIPELINE
        s.queues[pr].append(p)

    # ---- learn from results (failure-driven RAM floor increases) ----
    # We treat any failure as potential OOM and increase RAM floor for the failed ops.
    # This is a conservative heuristic that typically reduces repeated failures in the sim.
    for r in results:
        try:
            if not r.failed():
                continue
        except Exception:
            continue

        # If the result includes ops, increase RAM floor for each.
        ops = getattr(r, "ops", None) or []
        pipeline_id = getattr(r, "pipeline_id", None)

        # Best-effort: if pipeline_id is unavailable in result, we cannot safely key floors,
        # so we skip learning (keeps policy robust).
        if pipeline_id is None:
            continue

        # Increase factor grows with repeated failures.
        for op in ops:
            k = _op_key(pipeline_id, op)
            s.op_fail_count[k] = s.op_fail_count.get(k, 0) + 1

            prev_floor = s.op_ram_floor_gb.get(k, 0.0)
            last_ram = float(getattr(r, "ram", 0.0) or 0.0)

            # Multiplicative backoff; more aggressive after repeated failures.
            # If last_ram missing, just bump the floor slightly.
            if last_ram > 0:
                mult = 1.6 if s.op_fail_count[k] <= 2 else 2.0
                new_floor = max(prev_floor, last_ram * mult)
            else:
                new_floor = max(prev_floor, prev_floor * 1.3, 2.0)

            s.op_ram_floor_gb[k] = new_floor

    # Early exit: no events to react to.
    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    # ---- schedule per pool ----
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]

        # Snapshot availability (we decrement locally as we assign multiple ops).
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool

        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # If high priority is waiting, throttle batch by reserving some capacity.
        reserve_cpu = 0.0
        reserve_ram = 0.0
        if _has_waiting_high_prio():
            rs = _reserve_share_for_high_prio()
            reserve_cpu = rs * pool.max_cpu_pool
            reserve_ram = rs * pool.max_ram_pool

        # Assign multiple ops per pool per tick to improve utilization,
        # while still respecting priority ordering.
        made_progress = True
        while made_progress:
            made_progress = False

            for pr in _prio_list():
                # If high priority exists, only allow batch to use "excess" over reserved headroom.
                if pr == Priority.BATCH_PIPELINE and _has_waiting_high_prio():
                    if avail_cpu <= reserve_cpu or avail_ram <= reserve_ram:
                        continue

                if avail_cpu <= 0 or avail_ram <= 0:
                    continue

                p = _pick_next_pipeline(pr)
                if p is None:
                    continue

                op = _choose_op_from_pipeline(p)
                if op is None:
                    continue

                # Size resources.
                cpu = _size_cpu(pool, pr, avail_cpu)
                if cpu <= 0:
                    continue

                # For batch under high-priority pressure, cap what batch can take to preserve headroom.
                if pr == Priority.BATCH_PIPELINE and _has_waiting_high_prio():
                    cpu_cap = max(1.0, avail_cpu - reserve_cpu)
                    if cpu > cpu_cap:
                        cpu = cpu_cap

                ram = _size_ram_gb(pool, p.pipeline_id, op, avail_ram)
                if ram <= 0:
                    continue

                if pr == Priority.BATCH_PIPELINE and _has_waiting_high_prio():
                    ram_cap = max(1.0, avail_ram - reserve_ram)
                    if ram > ram_cap:
                        ram = ram_cap

                # If we can't allocate at least minimal sensible resources, skip.
                if cpu <= 0 or ram <= 0:
                    continue

                assignments.append(
                    Assignment(
                        ops=[op],
                        cpu=cpu,
                        ram=ram,
                        priority=pr,
                        pool_id=pool_id,
                        pipeline_id=p.pipeline_id,
                    )
                )

                # Decrement local availability.
                avail_cpu -= cpu
                avail_ram -= ram
                made_progress = True

                # Continue scheduling within this pool.

    return suspensions, assignments
