# policy_key: scheduler_medium_038
# reasoning_effort: medium
# exp: reasoning
# model: gpt-5.2-2025-12-11
# llm_cost: 0.099747
# generation_seconds: 83.45
# generated_at: 2026-04-09T23:21:26.100489
@register_scheduler_init(key="scheduler_medium_038")
def scheduler_medium_038_init(s):
    """
    Priority-aware, retry-friendly scheduler with lightweight fairness.

    Core ideas:
      - Strictly prefer QUERY then INTERACTIVE, but enforce occasional BATCH service
        to avoid starvation (token/budget approach).
      - Be conservative on initial sizing, but react quickly to failures:
          * On OOM-like failures, double RAM hint for the operator and retry.
          * On other failures, gently increase RAM/CPU hints and retry a few times.
      - Avoid "dropping on first failure" (which would incur a large objective penalty).
      - Keep per-operator resource hints (RAM/CPU) to reduce repeat OOMs and improve completion rate.
    """
    from collections import deque

    s.q_query = deque()
    s.q_interactive = deque()
    s.q_batch = deque()

    # Per-operator hints and retry counts.
    s.op_ram_hint = {}   # op_key -> ram
    s.op_cpu_hint = {}   # op_key -> cpu
    s.op_retries = {}    # op_key -> retries

    # Fairness budget: after N high-priority assignments, try to run one batch if possible.
    s.hp_budget = 0
    s.hp_budget_cap = 8

    # Retry/backoff controls.
    s.max_retries_per_op = 8
    s.heavy_retry_threshold = 3  # after this, require closer-to-hint fits to avoid thrash


@register_scheduler(key="scheduler_medium_038")
def scheduler_medium_038(s, results, pipelines):
    """
    Scheduler step:
      1) Ingest new pipelines into priority queues.
      2) Update per-operator hints from execution results (OOM-aware).
      3) For each pool, greedily place ready operators while capacity allows:
           - Prefer QUERY/INTERACTIVE, periodically admit BATCH.
           - Allocate CPU/RAM using per-op hints with priority-based floors.
           - Retry failed operators with increased hints to avoid repeated failures.
    """
    # -----------------------------
    # Helpers (local, no top-level imports)
    # -----------------------------
    def _prio_rank(prio):
        if prio == Priority.QUERY:
            return 3
        if prio == Priority.INTERACTIVE:
            return 2
        return 1

    def _queue_for_prio(prio):
        if prio == Priority.QUERY:
            return s.q_query
        if prio == Priority.INTERACTIVE:
            return s.q_interactive
        return s.q_batch

    def _op_key(pipeline, op):
        # Try common id attributes; fall back to repr for stability in dict keys.
        oid = None
        for attr in ("op_id", "operator_id", "node_id", "task_id", "name"):
            if hasattr(op, attr):
                oid = getattr(op, attr)
                if oid is not None:
                    break
        if oid is None:
            oid = repr(op)
        return (pipeline.pipeline_id, oid)

    def _is_oom_error(err):
        if not err:
            return False
        try:
            msg = str(err).lower()
        except Exception:
            return False
        return ("oom" in msg) or ("out of memory" in msg) or ("cuda out of memory" in msg) or ("memoryerror" in msg)

    def _prio_floors(pool, prio):
        # Floors prevent absurdly tiny allocations that almost surely OOM.
        # Keep them modest to allow concurrency; queries get a higher floor.
        max_ram = pool.max_ram_pool
        max_cpu = pool.max_cpu_pool

        if prio == Priority.QUERY:
            ram_floor = max(1.0, 0.20 * max_ram)
            cpu_floor = max(1.0, min(8.0, 0.50 * max_cpu))
            ram_target = 0.45 * max_ram
            cpu_target = min(12.0, 0.70 * max_cpu)
        elif prio == Priority.INTERACTIVE:
            ram_floor = max(1.0, 0.15 * max_ram)
            cpu_floor = max(1.0, min(6.0, 0.35 * max_cpu))
            ram_target = 0.35 * max_ram
            cpu_target = min(10.0, 0.55 * max_cpu)
        else:
            ram_floor = max(1.0, 0.10 * max_ram)
            cpu_floor = max(1.0, min(4.0, 0.25 * max_cpu))
            ram_target = 0.25 * max_ram
            cpu_target = min(8.0, 0.40 * max_cpu)

        return ram_floor, cpu_floor, ram_target, cpu_target

    def _desired_resources(pool, pipeline, op, avail_cpu, avail_ram):
        prio = pipeline.priority
        ram_floor, cpu_floor, ram_target, cpu_target = _prio_floors(pool, prio)

        key = _op_key(pipeline, op)
        ram_hint = s.op_ram_hint.get(key, ram_target)
        cpu_hint = s.op_cpu_hint.get(key, cpu_target)

        # Clamp hints into reasonable bounds.
        ram_hint = max(ram_floor, min(float(pool.max_ram_pool), float(ram_hint)))
        cpu_hint = max(cpu_floor, min(float(pool.max_cpu_pool), float(cpu_hint)))

        retries = s.op_retries.get(key, 0)
        # If we have many retries, insist on (near) hinted RAM to avoid repeated failures.
        must_fit_hint = retries >= s.heavy_retry_threshold

        # Choose allocation:
        # - CPU: for high priority, allocate more to reduce latency; for batch, keep moderate.
        # - RAM: prefer enough to avoid OOM; if scarce and low retries, allow smaller allocation.
        if must_fit_hint:
            cpu = min(avail_cpu, cpu_hint)
            ram = min(avail_ram, ram_hint)
        else:
            # Permit smaller-than-hint if constrained, but never below floor.
            cpu = min(avail_cpu, max(cpu_floor, min(cpu_hint, avail_cpu)))
            ram = min(avail_ram, max(ram_floor, min(ram_hint, avail_ram)))

        # If we can't meet the floor, not runnable here.
        if cpu < cpu_floor or ram < ram_floor:
            return None

        # If we are in must_fit_hint mode and can't get close to the hint, wait.
        if must_fit_hint:
            if ram < 0.90 * ram_hint:
                return None

        # Avoid allocating essentially all pool RAM to a low-priority op when others might be waiting.
        if prio == Priority.BATCH_PIPELINE:
            ram = min(ram, max(ram_floor, 0.60 * pool.max_ram_pool))
            cpu = min(cpu, max(1.0, 0.60 * pool.max_cpu_pool))

        return float(cpu), float(ram)

    def _clean_and_get_ready_op(pipeline):
        status = pipeline.runtime_status()
        if status.is_pipeline_successful():
            return None

        # We keep retrying FAILED ops (ASSIGNABLE_STATES includes FAILED).
        ops = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        if not ops:
            return None
        return ops[0]

    def _pick_pipeline_for_pool(pool, avail_cpu, avail_ram):
        """
        Pick next pipeline to schedule on this pool.
        Strict preference: QUERY > INTERACTIVE > BATCH, but after hp_budget_cap
        high-priority assignments, attempt one batch if feasible.
        """
        # If we have spent enough budget on high priority, try batch first (if any).
        order = []
        if s.hp_budget >= s.hp_budget_cap and len(s.q_batch) > 0:
            order = [s.q_batch, s.q_query, s.q_interactive]
        else:
            order = [s.q_query, s.q_interactive, s.q_batch]

        # Try each queue in order; rotate within the chosen queue for fairness.
        for q in order:
            if not q:
                continue

            n = len(q)
            for _ in range(n):
                p = q.popleft()

                # Drop completed pipelines from queues.
                if p.runtime_status().is_pipeline_successful():
                    continue

                op = _clean_and_get_ready_op(p)
                if op is None:
                    # Not ready right now; requeue to preserve order.
                    q.append(p)
                    continue

                # Check if runnable on this pool with current availability.
                res = _desired_resources(pool, p, op, avail_cpu, avail_ram)
                if res is None:
                    q.append(p)
                    continue

                cpu, ram = res
                # Put pipeline back for subsequent ops; we schedule one op at a time.
                q.append(p)
                return p, op, cpu, ram

        return None, None, None, None

    # -----------------------------
    # Ingest new pipelines
    # -----------------------------
    for p in pipelines:
        _queue_for_prio(p.priority).append(p)

    # Early return only if truly nothing to do.
    if not pipelines and not results:
        return [], []

    # -----------------------------
    # Update hints from results
    # -----------------------------
    for r in results:
        # We only adjust hints based on completed attempts; especially on failures.
        if not getattr(r, "ops", None):
            continue

        # Results are per-container; could include multiple ops, but we treat each op similarly.
        for op in r.ops:
            # We cannot directly map back to pipeline object from result, but we can still use
            # a best-effort key: use container-scoped op repr. If pipeline id isn't accessible
            # here, we only update using (None, op_id) to still gain learning.
            # Prefer pipeline_id when present in result (some simulators include it).
            pid = getattr(r, "pipeline_id", None)
            oid = None
            for attr in ("op_id", "operator_id", "node_id", "task_id", "name"):
                if hasattr(op, attr):
                    oid = getattr(op, attr)
                    if oid is not None:
                        break
            if oid is None:
                oid = repr(op)
            key = (pid, oid)

            prev_ram = float(s.op_ram_hint.get(key, float(getattr(r, "ram", 0.0) or 0.0) or 0.0))
            prev_cpu = float(s.op_cpu_hint.get(key, float(getattr(r, "cpu", 0.0) or 0.0) or 0.0))

            # If no prior hints, seed from what we tried.
            if prev_ram <= 0:
                prev_ram = float(getattr(r, "ram", 1.0) or 1.0)
            if prev_cpu <= 0:
                prev_cpu = float(getattr(r, "cpu", 1.0) or 1.0)

            if r.failed():
                s.op_retries[key] = s.op_retries.get(key, 0) + 1
                if _is_oom_error(getattr(r, "error", None)):
                    # Aggressively increase RAM to stop repeated OOMs.
                    new_ram = max(prev_ram * 2.0, float(getattr(r, "ram", prev_ram)) * 2.0)
                    new_cpu = max(prev_cpu, float(getattr(r, "cpu", prev_cpu)))
                else:
                    # Non-OOM failures: small increases; avoid runaway allocations.
                    new_ram = max(prev_ram * 1.25, float(getattr(r, "ram", prev_ram)))
                    new_cpu = max(prev_cpu * 1.10, float(getattr(r, "cpu", prev_cpu)))
                s.op_ram_hint[key] = new_ram
                s.op_cpu_hint[key] = new_cpu
            else:
                # Success: lightly converge hints toward what worked (avoid oscillations).
                tried_ram = float(getattr(r, "ram", prev_ram))
                tried_cpu = float(getattr(r, "cpu", prev_cpu))
                s.op_ram_hint[key] = max(1.0, 0.90 * prev_ram + 0.10 * tried_ram)
                s.op_cpu_hint[key] = max(1.0, 0.90 * prev_cpu + 0.10 * tried_cpu)

    # -----------------------------
    # Create assignments (no preemption in this policy)
    # -----------------------------
    suspensions = []
    assignments = []

    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)

        # If the pool is essentially full, skip.
        if avail_cpu <= 0.0 or avail_ram <= 0.0:
            continue

        # Greedily fill the pool with runnable ops, but stop if we can't place anything.
        # Also cap number of launches per pool per tick to reduce thrash.
        launches = 0
        launch_cap = 8

        while launches < launch_cap and avail_cpu > 0.0 and avail_ram > 0.0:
            p, op, cpu, ram = _pick_pipeline_for_pool(pool, avail_cpu, avail_ram)
            if p is None:
                break

            # Respect per-op retry cap: after too many retries, still try to run occasionally,
            # but avoid consuming scarce high-priority capacity if it's likely a hard failure.
            key_live = _op_key(p, op)
            retries = s.op_retries.get(key_live, 0)
            if retries > s.max_retries_per_op:
                # Treat as "background" work: only run if plenty of headroom.
                if avail_ram < 0.50 * pool.max_ram_pool or avail_cpu < 0.25 * pool.max_cpu_pool:
                    # Rotate budget to allow others; skip this attempt.
                    # (Pipeline stays queued because _pick_pipeline_for_pool re-queued it.)
                    break

            assignment = Assignment(
                ops=[op],
                cpu=cpu,
                ram=ram,
                priority=p.priority,
                pool_id=pool_id,
                pipeline_id=p.pipeline_id,
            )
            assignments.append(assignment)

            # Update budget and available resources.
            if p.priority in (Priority.QUERY, Priority.INTERACTIVE):
                s.hp_budget += 1
            else:
                s.hp_budget = max(0, s.hp_budget - 2)

            avail_cpu -= float(cpu)
            avail_ram -= float(ram)
            launches += 1

    return suspensions, assignments
