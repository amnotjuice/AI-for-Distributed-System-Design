# policy_key: scheduler_iter_median_simple_010
# context: simple
# model: gpt-5.2-2025-12-11
# llm_cost: 0.049477
# generation_seconds: 52.01
# generated_at: 2026-04-12T01:13:51.429470
@register_scheduler_init(key="scheduler_iter_median_simple_010")
def scheduler_iter_median_simple_010_init(s):
    """Iteration 2: Priority-first + shortest-remaining-work selection + better pool usage.

    Key changes vs prior attempt (aimed at lowering latency):
    - Strictly prioritize QUERY/INTERACTIVE and use SRPT-like selection within each priority
      (pipelines with fewer remaining ops first) to reduce median latency.
    - Smarter multi-pool placement: allocate high-priority work to the most free pools first.
    - Stronger batch throttling when high-priority backlog exists (keep headroom).
    - Per-pipeline OOM backoff when we can infer pipeline_id from results (fallback to coarse pri-level).
    - Gentle RAM decay on success to avoid permanently over-allocating after transient OOMs.
    """
    s.tick = 0

    # Priority queues (store Pipeline objects)
    s.queues = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }

    # Per-pipeline state
    # pstate[pipeline_id] = {
    #   "ram_mult": float,           # >= 1.0
    #   "non_oom_fail": bool,        # if True and there are FAILED ops, we drop pipeline
    #   "enq_tick": int,             # for simple aging (anti-starvation)
    # }
    s.pstate = {}

    # Coarse (priority-level) RAM multiplier fallback when we can't attribute failures
    s.pri_ram_mult = {
        Priority.QUERY: 1.0,
        Priority.INTERACTIVE: 1.0,
        Priority.BATCH_PIPELINE: 1.0,
    }


@register_scheduler(key="scheduler_iter_median_simple_010")
def scheduler_iter_median_simple_010_scheduler(s, results, pipelines):
    """
    Scheduling logic:
    1) Update tick; enqueue new pipelines.
    2) Learn from results:
       - On OOM: increase RAM multiplier (per pipeline if possible; else per priority).
       - On non-OOM failure: mark pipeline as non-retriable (best-effort).
       - On success: gently decay RAM multiplier toward 1.0 (avoid bloat).
    3) For each pool (most-free-first), schedule as many QUERY/INTERACTIVE ops as fit.
       Keep headroom when any high-priority backlog exists.
    4) Only then schedule BATCH, but capped under high-priority backlog.
    """
    s.tick += 1

    # -----------------------------
    # Helpers (no imports)
    # -----------------------------
    def _ensure_pstate(p):
        pid = p.pipeline_id
        if pid not in s.pstate:
            s.pstate[pid] = {
                "ram_mult": 1.0,
                "non_oom_fail": False,
                "enq_tick": s.tick,
            }
        return s.pstate[pid]

    def _is_oom_error(err):
        if err is None:
            return False
        msg = str(err).lower()
        return ("oom" in msg) or ("out of memory" in msg) or ("killed" in msg and "memory" in msg)

    def _infer_pipeline_id_from_result(r):
        # Try common fields first
        pid = getattr(r, "pipeline_id", None)
        if pid is not None:
            return pid
        # Try to infer from ops
        ops = getattr(r, "ops", None) or []
        if ops:
            op0 = ops[0]
            for attr in ("pipeline_id", "pipeline", "pipelineId", "pipelineID"):
                pid = getattr(op0, attr, None)
                if pid is not None:
                    return pid
            # Some implementations might store parent pipeline on op
            pid = getattr(op0, "parent_pipeline_id", None)
            if pid is not None:
                return pid
        return None

    def _pipeline_remaining_ops(p):
        st = p.runtime_status()
        # Compute remaining ops as a cheap SRPT proxy
        # Prefer status counters if available; fallback to len(p.values).
        try:
            completed = st.state_counts.get(OperatorState.COMPLETED, 0)
            failed = st.state_counts.get(OperatorState.FAILED, 0)
            assigned = st.state_counts.get(OperatorState.ASSIGNED, 0)
            running = st.state_counts.get(OperatorState.RUNNING, 0)
            pending = st.state_counts.get(OperatorState.PENDING, 0)
            # Remaining = everything not completed (include failed/assigned/running/pending)
            remaining = pending + running + assigned + failed
            # If counters are sparse, use total ops length as a fallback hint
            if remaining <= 0:
                total = len(getattr(p, "values", []) or [])
                remaining = max(0, total - completed)
            return remaining
        except Exception:
            total = len(getattr(p, "values", []) or [])
            # Can't see completion reliably; return total as coarse proxy
            return max(1, total)

    def _pipeline_age(p):
        pst = s.pstate.get(p.pipeline_id)
        if not pst:
            return 0
        return max(0, s.tick - pst.get("enq_tick", s.tick))

    def _get_ready_ops_one(p):
        st = p.runtime_status()
        # Drop completed pipelines
        if st.is_pipeline_successful():
            return None
        # If we marked as non-retriable and it has failures, drop it
        if s.pstate.get(p.pipeline_id, {}).get("non_oom_fail", False):
            if st.state_counts.get(OperatorState.FAILED, 0) > 0:
                return None
        ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
        if not ops:
            return []
        return ops

    def _has_hp_backlog():
        # Any runnable QUERY or INTERACTIVE op waiting?
        for pri in (Priority.QUERY, Priority.INTERACTIVE):
            for p in s.queues[pri]:
                ops = _get_ready_ops_one(p)
                if ops:
                    return True
        return False

    def _select_best_pipeline(pri):
        # SRPT-like: fewest remaining ops, tie-break by age (older first)
        best = None
        best_key = None
        q = s.queues[pri]
        for p in q:
            ops = _get_ready_ops_one(p)
            if ops is None:
                # completed or non-retriable failure -> skip (will be cleaned later)
                continue
            if ops == []:
                continue
            rem = _pipeline_remaining_ops(p)
            age = _pipeline_age(p)
            # Lower rem first; older gets slight preference to avoid unfairness.
            key = (rem, -age)
            if best is None or key < best_key:
                best = p
                best_key = key
        return best

    def _cleanup_queue(pri):
        # Remove pipelines that are completed or permanently failed
        newq = []
        for p in s.queues[pri]:
            st = p.runtime_status()
            if st.is_pipeline_successful():
                continue
            if s.pstate.get(p.pipeline_id, {}).get("non_oom_fail", False):
                if st.state_counts.get(OperatorState.FAILED, 0) > 0:
                    continue
            newq.append(p)
        s.queues[pri] = newq

    def _compute_request(pool, pri, p):
        # Size to reduce high-priority latency: give more CPU than before, but not the whole pool.
        pst = s.pstate.get(p.pipeline_id, None)
        ram_mult = 1.0
        if pst is not None:
            ram_mult = pst.get("ram_mult", 1.0)
        else:
            ram_mult = 1.0

        # Fallback: also apply coarse pri-level multiplier
        ram_mult = max(ram_mult, s.pri_ram_mult.get(pri, 1.0))

        if pri == Priority.QUERY:
            cpu_frac = 0.70
            ram_frac = 0.25
        elif pri == Priority.INTERACTIVE:
            cpu_frac = 0.80
            ram_frac = 0.35
        else:
            cpu_frac = 0.85
            ram_frac = 0.55

        cpu_req = max(1.0, min(pool.avail_cpu_pool, pool.max_cpu_pool * cpu_frac))
        ram_req = max(1.0, min(pool.avail_ram_pool, pool.max_ram_pool * ram_frac * ram_mult))

        # Clamp again to pool availability as observed in the loop (local avail may be smaller)
        return cpu_req, ram_req

    # -----------------------------
    # 1) Enqueue new pipelines
    # -----------------------------
    for p in pipelines:
        _ensure_pstate(p)
        s.queues[p.priority].append(p)

    # Early exit if nothing changed
    if not pipelines and not results:
        return [], []

    # -----------------------------
    # 2) Learn from results (OOM backoff + RAM decay)
    # -----------------------------
    # Track whether we saw OOMs per priority for coarse fallback
    saw_oom_pri = {
        Priority.QUERY: False,
        Priority.INTERACTIVE: False,
        Priority.BATCH_PIPELINE: False,
    }

    for r in results:
        if not (hasattr(r, "failed") and r.failed()):
            # Success: gently decay RAM multipliers if we can map to a pipeline
            pid = _infer_pipeline_id_from_result(r)
            if pid is not None and pid in s.pstate:
                pst = s.pstate[pid]
                pst["ram_mult"] = max(1.0, pst["ram_mult"] * 0.92)
            continue

        # Failed
        err = getattr(r, "error", None)
        if _is_oom_error(err):
            saw_oom_pri[r.priority] = True
            pid = _infer_pipeline_id_from_result(r)
            if pid is not None:
                # Ensure pstate exists even if it wasn't enqueued (defensive)
                if pid not in s.pstate:
                    s.pstate[pid] = {"ram_mult": 1.0, "non_oom_fail": False, "enq_tick": s.tick}
                s.pstate[pid]["ram_mult"] = min(32.0, max(1.0, s.pstate[pid]["ram_mult"] * 2.0))
        else:
            # Non-OOM failure: best-effort mark pipeline non-retriable
            pid = _infer_pipeline_id_from_result(r)
            if pid is not None:
                if pid not in s.pstate:
                    s.pstate[pid] = {"ram_mult": 1.0, "non_oom_fail": False, "enq_tick": s.tick}
                s.pstate[pid]["non_oom_fail"] = True

    # Coarse fallback for OOMs we can't attribute
    for pri, saw in saw_oom_pri.items():
        if saw:
            s.pri_ram_mult[pri] = min(32.0, max(1.0, s.pri_ram_mult[pri] * 1.6))

    # Clean queues of completed/perma-failed pipelines
    _cleanup_queue(Priority.QUERY)
    _cleanup_queue(Priority.INTERACTIVE)
    _cleanup_queue(Priority.BATCH_PIPELINE)

    # -----------------------------
    # 3) Build pool order (most free first) to cut queueing latency
    # -----------------------------
    pool_order = []
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        # Simple headroom score
        score = float(pool.avail_cpu_pool) + (float(pool.avail_ram_pool) / max(1.0, float(pool.max_ram_pool))) * float(pool.max_cpu_pool)
        pool_order.append((score, pool_id))
    pool_order.sort(reverse=True)

    hp_backlog = _has_hp_backlog()

    # When high-priority backlog exists, keep some headroom for future arrivals (tail latency protection).
    # This is a stronger version than before: batch must not reduce pool below these floors.
    def _hp_headroom_floors(pool):
        # Keep a floor proportional to pool size (works across different pool shapes).
        cpu_floor = max(0.0, pool.max_cpu_pool * 0.20)
        ram_floor = max(0.0, pool.max_ram_pool * 0.20)
        return cpu_floor, ram_floor

    suspensions = []
    assignments = []

    # -----------------------------
    # 4) Schedule high priority first (QUERY then INTERACTIVE) with SRPT selection
    # -----------------------------
    for _, pool_id in pool_order:
        pool = s.executor.pools[pool_id]

        # We'll schedule multiple ops per pool per tick, but always one op per pipeline at a time
        # to reduce per-pipeline latency variance and avoid over-committing to one pipeline.
        local_avail_cpu = pool.avail_cpu_pool
        local_avail_ram = pool.avail_ram_pool

        made_progress = True
        while made_progress and local_avail_cpu > 0 and local_avail_ram > 0:
            made_progress = False

            # Strict priority between QUERY and INTERACTIVE
            chosen_pri = None
            chosen_pipeline = None
            for pri in (Priority.QUERY, Priority.INTERACTIVE):
                p = _select_best_pipeline(pri)
                if p is not None:
                    chosen_pri = pri
                    chosen_pipeline = p
                    break

            if chosen_pipeline is None:
                break

            ops = _get_ready_ops_one(chosen_pipeline)
            if not ops:
                # Not runnable anymore; try again
                made_progress = True
                continue

            # Size request (then clamp to local availability)
            cpu_req, ram_req = _compute_request(pool, chosen_pri, chosen_pipeline)
            cpu_req = max(1.0, min(cpu_req, local_avail_cpu))
            ram_req = max(1.0, min(ram_req, local_avail_ram))

            if cpu_req <= 0 or ram_req <= 0:
                break

            assignments.append(
                Assignment(
                    ops=ops,
                    cpu=cpu_req,
                    ram=ram_req,
                    priority=chosen_pipeline.priority,
                    pool_id=pool_id,
                    pipeline_id=chosen_pipeline.pipeline_id,
                )
            )

            local_avail_cpu -= cpu_req
            local_avail_ram -= ram_req
            made_progress = True

    # Recompute backlog after scheduling HP (still may remain if pools were tight)
    hp_backlog = _has_hp_backlog()

    # -----------------------------
    # 5) Schedule batch, but throttle aggressively under HP backlog
    # -----------------------------
    for _, pool_id in pool_order:
        pool = s.executor.pools[pool_id]
        local_avail_cpu = pool.avail_cpu_pool
        local_avail_ram = pool.avail_ram_pool

        # If HP backlog exists, only allow batch to use resources above floors.
        if hp_backlog:
            cpu_floor, ram_floor = _hp_headroom_floors(pool)
            local_avail_cpu = max(0.0, local_avail_cpu - cpu_floor)
            local_avail_ram = max(0.0, local_avail_ram - ram_floor)

        if local_avail_cpu <= 0 or local_avail_ram <= 0:
            continue

        # Pick an older batch pipeline first to avoid starvation, but still prefer smaller remaining work.
        best_batch = None
        best_key = None
        for p in s.queues[Priority.BATCH_PIPELINE]:
            ops = _get_ready_ops_one(p)
            if ops is None or ops == []:
                continue
            rem = _pipeline_remaining_ops(p)
            age = _pipeline_age(p)
            # Older first, then shorter remaining
            key = (-age, rem)
            if best_batch is None or key < best_key:
                best_batch = p
                best_key = key

        if best_batch is None:
            continue

        ops = _get_ready_ops_one(best_batch)
        if not ops:
            continue

        # Batch sizing:
        # - If no HP backlog, allow bigger grabs for throughput.
        # - If HP backlog, keep batch smaller to reduce interference.
        if hp_backlog:
            cpu_req = max(1.0, min(local_avail_cpu, pool.max_cpu_pool * 0.35))
            ram_mult = max(s.pstate.get(best_batch.pipeline_id, {}).get("ram_mult", 1.0), s.pri_ram_mult[Priority.BATCH_PIPELINE])
            ram_req = max(1.0, min(local_avail_ram, pool.max_ram_pool * 0.30 * ram_mult))
        else:
            cpu_req, ram_req = _compute_request(pool, Priority.BATCH_PIPELINE, best_batch)
            cpu_req = max(1.0, min(cpu_req, local_avail_cpu))
            ram_req = max(1.0, min(ram_req, local_avail_ram))

        if cpu_req <= 0 or ram_req <= 0:
            continue

        assignments.append(
            Assignment(
                ops=ops,
                cpu=cpu_req,
                ram=ram_req,
                priority=best_batch.priority,
                pool_id=pool_id,
                pipeline_id=best_batch.pipeline_id,
            )
        )

    return suspensions, assignments
