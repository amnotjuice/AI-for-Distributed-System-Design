# policy_key: scheduler_est_006
# reasoning_effort: low
# model: gpt-5.2-2025-12-11
# llm_cost: 0.043973
# generation_seconds: 37.06
# generated_at: 2026-04-03T01:08:20.154411
@register_scheduler_init(key="scheduler_est_006")
def scheduler_est_006_init(s):
    """
    Priority-aware, estimate-guided FIFO scheduler (small, safe improvements over naive FIFO).

    Key ideas:
      - Maintain separate waiting queues per priority (QUERY/INTERACTIVE/BATCH) and schedule in that order.
      - Do not give a single operator the entire pool (cap per-op CPU by priority) to reduce head-of-line blocking.
      - Use per-operator memory peak estimates when available; on OOM failures, retry with an increased RAM request.
      - Simple fairness: after a small burst of high-priority ops, force-schedule one batch op if available.
    """
    # Pipelines we are tracking (by id), so we can keep re-attempting ready ops until completion.
    s.pipelines_by_id = {}

    # Per-priority waiting queues (store pipeline_ids).
    s.wait_q = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }

    # Retry state keyed by (pipeline_id, op_key): multiplicative RAM bump on OOM.
    s.oom_ram_mult = {}  # (pid, op_key) -> float

    # Fairness knobs
    s.hp_burst_limit = 6   # allow this many high-priority assignments before forcing one batch (if any)
    s.hp_burst_used = 0

    # CPU caps per assignment (avoid giving all CPUs to a single op and blocking others)
    s.cpu_cap = {
        Priority.QUERY: 4.0,
        Priority.INTERACTIVE: 8.0,
        Priority.BATCH_PIPELINE: 16.0,
    }

    # Memory safety factor when using estimates (be aggressive; rely on retry for underestimates)
    s.mem_safety = 1.05


@register_scheduler(key="scheduler_est_006")
def scheduler_est_006_scheduler(s, results, pipelines):
    """
    Priority-aware scheduler step.

    Behavior:
      - Add arriving pipelines to tracking + corresponding priority queue.
      - Process results: if an op failed (likely OOM), increase its RAM multiplier for retry.
      - For each pool, greedily assign ready ops while resources remain.
      - Choose next pipeline by priority queues, with a small fairness guard to avoid indefinite batch starvation.
    """
    def _op_key(op):
        # Best-effort stable identifier for retry bumps across ticks.
        for attr in ("op_id", "operator_id", "id", "name", "key"):
            if hasattr(op, attr):
                try:
                    v = getattr(op, attr)
                    if v is not None:
                        return str(v)
                except Exception:
                    pass
        return repr(op)

    def _is_pipeline_done_or_failed(p):
        st = p.runtime_status()
        if st.is_pipeline_successful():
            return True
        # If any operator is FAILED, we still allow retry (FAILED is in ASSIGNABLE_STATES),
        # but some failures might be non-retriable. We don't have a strong signal, so we retry.
        return False

    def _enqueue_pipeline(p):
        pid = p.pipeline_id
        if pid not in s.pipelines_by_id:
            s.pipelines_by_id[pid] = p
        # Avoid duplicate entries in queues: enqueue only if not already present anywhere.
        # (O(n) check is fine for simulator scale; keeps logic simple and safe.)
        in_any = False
        for q in s.wait_q.values():
            if pid in q:
                in_any = True
                break
        if not in_any:
            s.wait_q[p.priority].append(pid)

    def _pick_next_pipeline_id():
        # Fairness: after a burst of high-priority assignments, force one batch if available.
        if s.hp_burst_used >= s.hp_burst_limit and s.wait_q[Priority.BATCH_PIPELINE]:
            s.hp_burst_used = 0
            return s.wait_q[Priority.BATCH_PIPELINE].pop(0)

        # Otherwise strict priority order: QUERY -> INTERACTIVE -> BATCH
        for pr in (Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE):
            q = s.wait_q.get(pr, [])
            while q:
                pid = q.pop(0)
                if pid in s.pipelines_by_id:
                    return pid
        return None

    def _estimate_ram_gb(op, pool, pid):
        # Determine requested RAM for this op.
        # - If an estimate exists, request close to it (aggressive), with small safety.
        # - If no estimate, request a conservative fraction of pool RAM (keeps concurrency).
        # - Apply OOM backoff multiplier if we've seen failures for this (pipeline, op).
        opk = _op_key(op)
        mult = s.oom_ram_mult.get((pid, opk), 1.0)

        est = None
        try:
            est = getattr(getattr(op, "estimate", None), "mem_peak_gb", None)
        except Exception:
            est = None

        if est is not None:
            base = float(est) * float(s.mem_safety)
        else:
            # Unknown memory: pick a moderate default to avoid monopolizing the pool.
            # High-priority gets a bit more to reduce OOM retries; batch gets less to improve packing.
            base_frac = 0.35
            if hasattr(s.pipelines_by_id.get(pid, None), "priority"):
                pr = s.pipelines_by_id[pid].priority
                if pr == Priority.QUERY:
                    base_frac = 0.45
                elif pr == Priority.INTERACTIVE:
                    base_frac = 0.40
                else:
                    base_frac = 0.30
            base = max(1.0, float(pool.max_ram_pool) * base_frac)

        req = base * mult
        # Clamp to pool capacity (admission control is done by avail checks).
        req = min(req, float(pool.max_ram_pool))
        # Never request <=0
        return max(0.1, req)

    def _pick_cpu(pr, pool_avail_cpu):
        cap = float(s.cpu_cap.get(pr, 8.0))
        # Keep at least 1 vCPU if possible; avoid assigning 0.
        if pool_avail_cpu <= 0:
            return 0.0
        return max(1.0, min(float(pool_avail_cpu), cap))

    # Ingest new pipelines
    for p in pipelines:
        _enqueue_pipeline(p)

    # Early exit if nothing to do
    if not pipelines and not results:
        return [], []

    # Process results: if failures, bump RAM multiplier for the specific op(s)
    for r in results:
        try:
            if r.failed():
                # Treat any failure as potentially OOM-like; increase RAM on retry.
                # If it's not OOM, extra RAM won't hurt; simulator cost is only resource contention.
                pid = getattr(r, "pipeline_id", None)
                # pipeline_id may not be present on result; fall back to None-safe behavior.
                # If missing, we cannot key precisely; we skip to avoid global inflation.
                if pid is None:
                    continue
                for op in getattr(r, "ops", []) or []:
                    opk = _op_key(op)
                    cur = float(s.oom_ram_mult.get((pid, opk), 1.0))
                    # Exponential backoff but capped to avoid runaway; most OOMs converge quickly.
                    nxt = min(cur * 1.6, 8.0)
                    s.oom_ram_mult[(pid, opk)] = nxt
        except Exception:
            # Be resilient to missing fields in simulator objects.
            pass

    suspensions = []
    assignments = []

    # Attempt to schedule across pools
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)

        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # Greedily fill the pool with ready ops
        # Keep a local list of pipeline_ids we touched and need to requeue if still not done.
        touched = []

        # Safety to prevent infinite loops if queues keep recycling non-ready pipelines
        tries = 0
        max_tries = 50

        while avail_cpu > 0 and avail_ram > 0 and tries < max_tries:
            tries += 1
            pid = _pick_next_pipeline_id()
            if pid is None:
                break

            p = s.pipelines_by_id.get(pid, None)
            if p is None:
                continue

            if _is_pipeline_done_or_failed(p):
                # Pipeline completed; remove from tracking.
                s.pipelines_by_id.pop(pid, None)
                continue

            st = p.runtime_status()
            # Pick a single ready op (simple step; avoids allocating too many resources at once).
            op_list = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
            if not op_list:
                # Not ready yet; requeue and move on.
                touched.append(pid)
                continue

            op = op_list[0]
            req_ram = _estimate_ram_gb(op, pool, pid)
            req_cpu = _pick_cpu(p.priority, avail_cpu)

            # If we can't fit this op right now, requeue it and stop scheduling on this pool.
            if req_cpu <= 0 or req_ram <= 0 or req_cpu > avail_cpu or req_ram > avail_ram:
                touched.append(pid)
                # No fit: break to avoid spinning; another pool may be able to schedule it.
                break

            assignments.append(
                Assignment(
                    ops=[op],
                    cpu=req_cpu,
                    ram=req_ram,
                    priority=p.priority,
                    pool_id=pool_id,
                    pipeline_id=p.pipeline_id,
                )
            )

            # Consume resources
            avail_cpu -= req_cpu
            avail_ram -= req_ram

            # Update burst counter for fairness (only counts for high priority)
            if p.priority in (Priority.QUERY, Priority.INTERACTIVE):
                s.hp_burst_used += 1

            # Pipeline likely still has more ops; requeue for further consideration.
            touched.append(pid)

        # Requeue touched pipelines so they can be considered in subsequent pools/ticks
        # (but avoid duplicates by using _enqueue_pipeline).
        for pid in touched:
            p = s.pipelines_by_id.get(pid, None)
            if p is not None and not _is_pipeline_done_or_failed(p):
                _enqueue_pipeline(p)

    return suspensions, assignments
