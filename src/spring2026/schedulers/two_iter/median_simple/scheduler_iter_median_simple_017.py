# policy_key: scheduler_iter_median_simple_017
# context: simple
# model: gpt-5.2-2025-12-11
# llm_cost: 0.036989
# generation_seconds: 35.94
# generated_at: 2026-04-12T01:40:54.586564
@register_scheduler_init(key="scheduler_iter_median_simple_017")
def scheduler_iter_median_simple_017_init(s):
    """Priority-first, latency-oriented scheduler (incremental improvements over naive FIFO).

    Key ideas:
    - Strict priority order for dispatch: QUERY > INTERACTIVE > BATCH.
    - Keep batch from consuming the whole pool when any high-priority backlog exists (soft reservation).
    - Use small CPU slices for high-priority ops to increase concurrency and reduce queueing delay.
    - Track OOMs and bump RAM for the affected pipeline (best-effort mapping from result->pipeline).
    - Avoid duplicate enqueues; keep round-robin fairness within each priority.
    """
    from collections import deque

    s.q_query = deque()
    s.q_interactive = deque()
    s.q_batch = deque()

    # Track pipelines we've enqueued so we don't create duplicates when pipelines are re-sent.
    s.enqueued = set()

    # Per-pipeline resource hints.
    # ram_mult: bumped on OOM; cpu_mult: kept simple (can extend later).
    # drop: if we infer a non-OOM failure, stop retrying the pipeline.
    s.pstate = {}  # pipeline_id -> dict(ram_mult, cpu_mult, drop)

    # Optional: a tiny "anti-starvation" counter for batch under sustained HP load.
    s.hp_pressure_ticks = 0


@register_scheduler(key="scheduler_iter_median_simple_017")
def scheduler_iter_median_simple_017_scheduler(s, results, pipelines):
    """
    Step logic:
    1) Enqueue new pipelines into per-priority deques (no duplicates).
    2) Update per-pipeline RAM multiplier on OOM (best-effort using result.ops to find pipeline_id).
    3) For each pool: schedule as many ready ops as fit, in strict priority order.
       - While HP backlog exists, cap batch share and limit batch assignments to reduce latency impact.
    """
    from collections import deque

    # -----------------------------
    # Helpers
    # -----------------------------
    def _ensure_pstate(pid):
        if pid not in s.pstate:
            s.pstate[pid] = {"ram_mult": 1.0, "cpu_mult": 1.0, "drop": False}
        return s.pstate[pid]

    def _is_oom_error(err):
        if err is None:
            return False
        msg = str(err).lower()
        return ("oom" in msg) or ("out of memory" in msg) or ("killed" in msg and "memory" in msg)

    def _get_pipeline_id_from_result(r):
        # Best-effort: sometimes the operator objects include pipeline_id.
        # r.ops is a list of ops; we try common attribute names.
        try:
            ops = getattr(r, "ops", None) or []
            if ops:
                op0 = ops[0]
                for attr in ("pipeline_id", "pipeline", "pipelineId", "dag_id"):
                    if hasattr(op0, attr):
                        val = getattr(op0, attr)
                        # If it's an object with pipeline_id, deref it.
                        if hasattr(val, "pipeline_id"):
                            return getattr(val, "pipeline_id")
                        return val
        except Exception:
            return None
        return None

    def _enqueue_pipeline(p):
        pid = p.pipeline_id
        if pid in s.enqueued:
            return
        s.enqueued.add(pid)
        _ensure_pstate(pid)
        if p.priority == Priority.QUERY:
            s.q_query.append(p)
        elif p.priority == Priority.INTERACTIVE:
            s.q_interactive.append(p)
        else:
            s.q_batch.append(p)

    def _cleanup_and_has_runnable(q):
        # Remove completed/dropped pipelines that might still be lingering.
        # Also detect whether any pipeline has a runnable op.
        n = len(q)
        runnable = False
        for _ in range(n):
            p = q.popleft()
            st = p.runtime_status()
            pid = p.pipeline_id
            pst = _ensure_pstate(pid)

            if st.is_pipeline_successful():
                # Fully done; allow future re-enqueue with same pipeline_id if simulator reuses ids (rare).
                s.enqueued.discard(pid)
                continue

            # If pipeline has failures and we marked it as dropped, don't keep retrying.
            if pst["drop"] and st.state_counts.get(OperatorState.FAILED, 0) > 0:
                s.enqueued.discard(pid)
                continue

            # Keep it; note if it has any runnable ops.
            ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
            if ops:
                runnable = True
            q.append(p)
        return runnable

    def _pick_next_runnable(q, max_scan=None):
        # Round-robin: rotate queue until we find a pipeline with a runnable op.
        if not q:
            return None, None
        scan = len(q) if max_scan is None else min(len(q), max_scan)
        for _ in range(scan):
            p = q.popleft()
            st = p.runtime_status()
            pid = p.pipeline_id
            pst = _ensure_pstate(pid)

            if st.is_pipeline_successful():
                s.enqueued.discard(pid)
                continue
            if pst["drop"] and st.state_counts.get(OperatorState.FAILED, 0) > 0:
                s.enqueued.discard(pid)
                continue

            ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
            if ops:
                # Put back for fairness after taking one op.
                q.append(p)
                return p, ops

            q.append(p)
        return None, None

    def _size_for_priority(pool, pri, pid, avail_cpu, avail_ram, hp_backlog):
        pst = _ensure_pstate(pid)

        # Latency-oriented sizing: keep high-priority slices small to improve parallelism.
        # Batch can use more, but is constrained under HP backlog.
        if pri == Priority.QUERY:
            cpu = min(2.0, max(1.0, avail_cpu))
            ram = min(avail_ram, max(1.0, pool.max_ram_pool * 0.18))
        elif pri == Priority.INTERACTIVE:
            cpu = min(3.0, max(1.0, avail_cpu))
            ram = min(avail_ram, max(1.0, pool.max_ram_pool * 0.25))
        else:
            # Batch:
            if hp_backlog:
                # Preserve headroom for HP; keep batch small.
                cpu = min(max(1.0, avail_cpu), max(1.0, pool.max_cpu_pool * 0.20))
                ram = min(max(1.0, avail_ram), max(1.0, pool.max_ram_pool * 0.25))
            else:
                # When no HP pressure, let batch scale up.
                cpu = min(avail_cpu, max(2.0, pool.max_cpu_pool * 0.60))
                ram = min(avail_ram, max(1.0, pool.max_ram_pool * 0.40))

        # Apply RAM backoff on OOM; keep CPU multiplier conservative for now.
        cpu *= pst["cpu_mult"]
        ram *= pst["ram_mult"]

        # Clamp to what we can actually allocate right now.
        cpu = max(1.0, min(cpu, avail_cpu, pool.max_cpu_pool))
        ram = max(1.0, min(ram, avail_ram, pool.max_ram_pool))
        return cpu, ram

    # -----------------------------
    # 1) Enqueue new pipelines
    # -----------------------------
    for p in pipelines:
        _enqueue_pipeline(p)

    # Early exit if no changes that could affect decisions
    if not pipelines and not results:
        return [], []

    # -----------------------------
    # 2) Update state from results (OOM backoff + drop non-OOM failures)
    # -----------------------------
    for r in results:
        if not (hasattr(r, "failed") and r.failed()):
            continue

        pid = _get_pipeline_id_from_result(r)
        if pid is None:
            # Can't attribute precisely; skip per-pipeline update.
            continue

        pst = _ensure_pstate(pid)
        if _is_oom_error(getattr(r, "error", None)):
            # Exponential-ish RAM bump; cap to avoid runaway allocations.
            pst["ram_mult"] = min(pst["ram_mult"] * 1.8, 32.0)
        else:
            # Non-OOM failures are usually deterministic; stop retrying.
            pst["drop"] = True

    # -----------------------------
    # 3) Determine backlog pressure (after cleanup)
    # -----------------------------
    has_query = _cleanup_and_has_runnable(s.q_query)
    has_interactive = _cleanup_and_has_runnable(s.q_interactive)
    hp_backlog = has_query or has_interactive

    if hp_backlog:
        s.hp_pressure_ticks += 1
    else:
        s.hp_pressure_ticks = 0

    # -----------------------------
    # 4) Schedule per pool
    # -----------------------------
    suspensions = []
    assignments = []

    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool

        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # Under HP backlog, allow at most one batch assignment per pool per tick to limit interference.
        batch_assigned_this_pool = 0
        # Keep scheduling until we can't fit even one more small HP op.
        while avail_cpu > 0 and avail_ram > 0:
            chosen_p = None
            chosen_ops = None
            chosen_pri = None

            # Strict priority: QUERY -> INTERACTIVE -> BATCH
            if has_query:
                chosen_p, chosen_ops = _pick_next_runnable(s.q_query)
                chosen_pri = Priority.QUERY
            if chosen_p is None and has_interactive:
                chosen_p, chosen_ops = _pick_next_runnable(s.q_interactive)
                chosen_pri = Priority.INTERACTIVE
            if chosen_p is None:
                # Batch gating under HP pressure
                if hp_backlog and batch_assigned_this_pool >= 1:
                    break
                chosen_p, chosen_ops = _pick_next_runnable(s.q_batch)
                chosen_pri = Priority.BATCH_PIPELINE

            if chosen_p is None or not chosen_ops:
                break

            # If HP exists but we ended up with batch and we want additional protection,
            # require some minimal reserved headroom (avoid starving HP arrivals in next tick).
            if hp_backlog and chosen_pri == Priority.BATCH_PIPELINE:
                # Keep at least 1 vCPU and 10% RAM in reserve when possible.
                if avail_cpu <= 1.0 or avail_ram <= (pool.max_ram_pool * 0.10):
                    break

            cpu_req, ram_req = _size_for_priority(pool, chosen_pri, chosen_p.pipeline_id, avail_cpu, avail_ram, hp_backlog)

            # If we can't allocate even minimal resources, stop on this pool.
            if cpu_req <= 0 or ram_req <= 0 or cpu_req > avail_cpu or ram_req > avail_ram:
                break

            assignments.append(
                Assignment(
                    ops=chosen_ops,
                    cpu=cpu_req,
                    ram=ram_req,
                    priority=chosen_p.priority,
                    pool_id=pool_id,
                    pipeline_id=chosen_p.pipeline_id,
                )
            )

            avail_cpu -= cpu_req
            avail_ram -= ram_req

            if chosen_pri == Priority.BATCH_PIPELINE:
                batch_assigned_this_pool += 1

            # Update "has_query/has_interactive" cheaply: under heavy scheduling, recompute occasionally.
            # This avoids stale flags without repeated full scans.
            if hp_backlog and (len(assignments) % 8 == 0):
                has_query = _cleanup_and_has_runnable(s.q_query)
                has_interactive = _cleanup_and_has_runnable(s.q_interactive)
                hp_backlog = has_query or has_interactive

    return suspensions, assignments
