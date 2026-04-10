# policy_key: scheduler_est_012
# reasoning_effort: low
# model: gpt-5.2-2025-12-11
# llm_cost: 0.037818
# generation_seconds: 48.48
# generated_at: 2026-04-01T13:44:24.300102
@register_scheduler_init(key="scheduler_est_012")
def scheduler_est_012_init(s):
    """Priority-aware FIFO with conservative memory sizing and OOM-driven warm-start retries.

    Improvements over naive FIFO:
      1) Maintain separate per-priority queues; always try to schedule higher priority first.
      2) Reserve headroom for higher priorities by capping batch allocations when higher-priority
         work is waiting (avoids batch soaking the whole pool and hurting tail latency).
      3) Use per-operator memory estimates (if present) and learn from OOM failures by increasing
         future RAM requests for that operator (simple, robust warm-start).
    """
    # Per-priority pipeline queues (FIFO within each priority class)
    s.q_query = []
    s.q_interactive = []
    s.q_batch = []

    # Learned RAM hint per operator (keyed by object id(op)); used to avoid repeated OOM
    s.op_ram_hint_gb = {}

    # Simple retry budget for repeated OOMs per operator (keyed by object id(op))
    s.op_oom_retries = {}

    # Tunables (kept intentionally simple / "small improvements first")
    s.max_oom_retries_per_op = 2
    s.oom_ram_bump_factor = 1.6  # bump RAM on OOM by 60%

    # When higher-priority work is waiting, limit batch to leave headroom in the pool
    s.reserve_cpu_frac_for_hp = 0.25
    s.reserve_ram_frac_for_hp = 0.25

    # Default RAM fractions when no estimate/hint exists (capped by availability at assignment time)
    s.default_ram_frac = {
        Priority.QUERY: 0.12,
        Priority.INTERACTIVE: 0.10,
        Priority.BATCH_PIPELINE: 0.06,
    }

    # Default CPU fractions (relative to pool max) for initial placements
    s.default_cpu_frac = {
        Priority.QUERY: 0.75,
        Priority.INTERACTIVE: 0.60,
        Priority.BATCH_PIPELINE: 0.50,
    }


@register_scheduler(key="scheduler_est_012")
def scheduler_est_012(s, results: list, pipelines: list):
    """
    Scheduler step.

    Strategy:
      - Ingest new pipelines into per-priority FIFO queues.
      - Learn from OOM failures to increase operator RAM hints.
      - For each pool, schedule at most one ready operator, choosing highest-priority runnable op.
      - If higher-priority work exists, keep headroom by capping batch allocations.
    """
    # --- Helpers (kept local to avoid imports/global dependencies) ---
    def _is_oom_error(err):
        if err is None:
            return False
        try:
            msg = str(err).lower()
        except Exception:
            return False
        return ("oom" in msg) or ("out of memory" in msg) or ("killed" in msg and "memory" in msg)

    def _enqueue_pipeline(p):
        # Avoid enqueuing obviously completed pipelines
        st = p.runtime_status()
        if st.is_pipeline_successful():
            return
        if p.priority == Priority.QUERY:
            s.q_query.append(p)
        elif p.priority == Priority.INTERACTIVE:
            s.q_interactive.append(p)
        else:
            s.q_batch.append(p)

    def _get_estimated_mem_gb(op):
        est_obj = getattr(op, "estimate", None)
        if est_obj is None:
            return None
        v = getattr(est_obj, "mem_peak_gb", None)
        try:
            if v is None:
                return None
            v = float(v)
            if v <= 0:
                return None
            return v
        except Exception:
            return None

    def _next_runnable_op(p):
        st = p.runtime_status()
        # If any operator has FAILED, we still allow retry (ASSIGNABLE_STATES includes FAILED),
        # but we require parents completed to avoid pointless retries.
        ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        if not ops:
            return []
        # Keep it simple: schedule one operator per assignment.
        return ops[:1]

    def _pick_from_queue(q):
        # FIFO scan: pop front, if not runnable now, put back.
        # Returns (pipeline, op_list) or (None, None).
        n = len(q)
        for _ in range(n):
            p = q.pop(0)
            st = p.runtime_status()
            if st.is_pipeline_successful():
                continue
            op_list = _next_runnable_op(p)
            q.append(p)  # round-robin through pipelines of same priority
            if op_list:
                return p, op_list
        return None, None

    def _has_hp_waiting():
        # "Higher priority waiting" means any query or interactive pipeline exists and has any
        # assignable operator (or might soon; being conservative is fine for headroom).
        return (len(s.q_query) > 0) or (len(s.q_interactive) > 0)

    def _ram_request_gb(op, pool_max_ram_gb, priority):
        # Use learned hint first, then estimator, then priority-based default fraction of pool.
        op_key = id(op)
        hint = s.op_ram_hint_gb.get(op_key, None)
        est = _get_estimated_mem_gb(op)

        base = None
        if hint is not None:
            base = hint
        elif est is not None:
            # Treat estimate as a lower bound; add small safety margin
            base = est * 1.15
        else:
            frac = s.default_ram_frac.get(priority, 0.08)
            base = max(0.5, pool_max_ram_gb * frac)  # never below 0.5GB if units are GB

        # Don't request more than the pool max (availability handled outside)
        if base > pool_max_ram_gb:
            base = pool_max_ram_gb
        return base

    def _cpu_request(op, pool_max_cpu, priority):
        # CPU scaling is sublinear; don't always grab all CPU. Use a priority-based fraction.
        frac = s.default_cpu_frac.get(priority, 0.5)
        req = pool_max_cpu * frac
        if req < 1:
            req = 1
        if req > pool_max_cpu:
            req = pool_max_cpu
        return req

    # --- Ingest new pipelines ---
    for p in pipelines:
        _enqueue_pipeline(p)

    # --- Learn from results (OOM => bump RAM hint for the operator) ---
    for r in results:
        if not hasattr(r, "failed") or not r.failed():
            continue
        if not _is_oom_error(getattr(r, "error", None)):
            continue

        bumped_any = False
        for op in getattr(r, "ops", []) or []:
            op_key = id(op)
            prev = s.op_ram_hint_gb.get(op_key, None)
            # If we know what we tried, bump from that; else, be conservative with a small baseline.
            tried = getattr(r, "ram", None)
            try:
                tried = float(tried) if tried is not None else None
            except Exception:
                tried = None

            if tried is not None and tried > 0:
                new_hint = tried * s.oom_ram_bump_factor
            else:
                # If we don't know tried RAM, just set a modest bump baseline.
                new_hint = 2.0

            if prev is not None:
                new_hint = max(prev, new_hint)

            # Respect retry budget: if we've bumped too many times, keep the hint but stop growing.
            retries = s.op_oom_retries.get(op_key, 0)
            if retries < s.max_oom_retries_per_op:
                s.op_ram_hint_gb[op_key] = new_hint
                s.op_oom_retries[op_key] = retries + 1
                bumped_any = True
            else:
                # Still store at least the max we've seen to avoid repeated tiny allocations.
                s.op_ram_hint_gb[op_key] = max(prev or 0, new_hint)

        # If we bumped, the pipeline/op will naturally be eligible again because FAILED is assignable.

    # Early exit if nothing to do
    if not pipelines and not results and not (s.q_query or s.q_interactive or s.q_batch):
        return [], []

    suspensions = []
    assignments = []

    hp_waiting = _has_hp_waiting()

    # --- Schedule: one assignment per pool per tick, prioritize query > interactive > batch ---
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool
        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # Determine how much capacity to leave for high priority, if any exists.
        reserve_cpu = 0
        reserve_ram = 0
        if hp_waiting:
            reserve_cpu = pool.max_cpu_pool * s.reserve_cpu_frac_for_hp
            reserve_ram = pool.max_ram_pool * s.reserve_ram_frac_for_hp

        # Pick pipeline/op: strict priority order
        chosen = None
        for q in (s.q_query, s.q_interactive, s.q_batch):
            p, op_list = _pick_from_queue(q)
            if p is None:
                continue
            chosen = (p, op_list)
            break

        if chosen is None:
            continue

        pipeline, op_list = chosen
        prio = pipeline.priority

        # Requested resources
        cpu_req = _cpu_request(op_list[0], pool.max_cpu_pool, prio)
        ram_req = _ram_request_gb(op_list[0], pool.max_ram_pool, prio)

        # Headroom: if higher-priority exists and we're about to schedule batch, cap by reserves
        if hp_waiting and prio == Priority.BATCH_PIPELINE:
            cpu_cap = max(0, avail_cpu - reserve_cpu)
            ram_cap = max(0, avail_ram - reserve_ram)
        else:
            cpu_cap = avail_cpu
            ram_cap = avail_ram

        # Final allocated resources (cannot exceed caps/availability)
        cpu = min(avail_cpu, cpu_cap, cpu_req)
        ram = min(avail_ram, ram_cap, ram_req)

        # If we can't fit with caps (or the pool is too tight), try to schedule a higher priority
        # item instead of batch; this prevents batch from blocking a pool with tiny leftovers.
        if cpu <= 0 or ram <= 0:
            if prio == Priority.BATCH_PIPELINE:
                # Try again with higher priority for this pool
                retry_chosen = None
                for q in (s.q_query, s.q_interactive):
                    p2, op_list2 = _pick_from_queue(q)
                    if p2 is None:
                        continue
                    retry_chosen = (p2, op_list2)
                    break
                if retry_chosen is None:
                    continue
                pipeline, op_list = retry_chosen
                prio = pipeline.priority
                cpu_req = _cpu_request(op_list[0], pool.max_cpu_pool, prio)
                ram_req = _ram_request_gb(op_list[0], pool.max_ram_pool, prio)
                cpu = min(avail_cpu, cpu_req)
                ram = min(avail_ram, ram_req)

            if cpu <= 0 or ram <= 0:
                continue

        assignments.append(
            Assignment(
                ops=op_list,
                cpu=cpu,
                ram=ram,
                priority=prio,
                pool_id=pool_id,
                pipeline_id=pipeline.pipeline_id,
            )
        )

    return suspensions, assignments
