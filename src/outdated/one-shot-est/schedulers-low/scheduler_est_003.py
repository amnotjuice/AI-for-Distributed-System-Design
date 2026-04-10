# policy_key: scheduler_est_003
# reasoning_effort: low
# model: gpt-5.2-2025-12-11
# llm_cost: 0.035818
# generation_seconds: 33.98
# generated_at: 2026-03-31T23:47:00.768092
@register_scheduler_init(key="scheduler_est_003")
def scheduler_est_003_init(s):
    """
    Priority-aware, estimator-backed scheduler (incremental improvement over naive FIFO).

    Main ideas:
      1) Maintain a persistent queue of active pipelines; always prefer higher-priority work.
      2) Pack multiple operators per pool per tick (instead of "one op per pool"), using
         small per-op CPU targets to reduce head-of-line blocking and improve latency.
      3) Use op.estimate.mem_peak_gb as a RAM floor; on OOM (failed result), increase
         a per-operator RAM boost and re-run later.
      4) Add mild aging to prevent indefinite starvation of lower priorities.
    """
    s.waiting_pipelines = []          # stable list of pipelines that are not done/failed
    s.pipeline_enqueued_at = {}       # pipeline_id -> tick when first seen
    s.op_ram_boost = {}              # (pipeline_id, op_key) -> multiplicative boost for RAM floor
    s.tick = 0                       # logical time for aging


@register_scheduler(key="scheduler_est_003")
def scheduler_est_003(s, results, pipelines):
    """
    Scheduler step:
      - ingest new pipelines
      - process recent results to learn from OOM failures (increase RAM boost)
      - for each pool, repeatedly select a "best next" runnable operator from the highest
        effective priority pipeline (with aging) and assign it with conservative CPU slices
        and estimator-based RAM floors.
    """
    # ---- helpers (kept inside to avoid imports) ----
    def _base_prio_score(prio):
        # Higher score = more urgent.
        # Explicit mapping to avoid assuming enum ordering.
        try:
            if prio == Priority.QUERY:
                return 300
            if prio == Priority.INTERACTIVE:
                return 200
            if prio == Priority.BATCH_PIPELINE:
                return 100
        except Exception:
            pass
        return 0

    def _pipeline_done_or_failed(p):
        st = p.runtime_status()
        # If any operator failed, consider pipeline failed (keep simple; no retries at pipeline level).
        has_failures = getattr(st, "state_counts", {}).get(OperatorState.FAILED, 0) > 0
        return st.is_pipeline_successful() or has_failures

    def _get_ready_ops(p, max_n=1):
        st = p.runtime_status()
        ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        if not ops:
            return []
        return ops[:max_n]

    def _op_key(op):
        # Prefer stable identifiers if present; otherwise fall back to object id.
        for attr in ("op_id", "operator_id", "id", "name"):
            if hasattr(op, attr):
                try:
                    v = getattr(op, attr)
                    # If it's callable (e.g., property-like), avoid calling.
                    if not callable(v):
                        return (attr, str(v))
                except Exception:
                    pass
        return ("pyid", str(id(op)))

    def _estimate_mem_floor_gb(op):
        # Use estimate as a floor; if missing, pick a small conservative default.
        est = None
        try:
            est = getattr(getattr(op, "estimate", None), "mem_peak_gb", None)
        except Exception:
            est = None
        if est is None:
            return 1.0
        try:
            # Ensure it's a usable float and non-trivial.
            estf = float(est)
            return 1.0 if estf <= 0 else estf
        except Exception:
            return 1.0

    def _cpu_target_for_priority(prio, avail_cpu):
        # Small CPU slices reduce head-of-line blocking (better latency under contention).
        # Keep simple and safe: cap per-op CPU.
        if avail_cpu <= 0:
            return 0
        if prio == Priority.QUERY:
            target = 2.0
        elif prio == Priority.INTERACTIVE:
            target = 1.5
        else:
            target = 1.0
        # Ensure at least 1 vCPU if any CPU is available.
        if avail_cpu < 1.0:
            return avail_cpu
        return min(avail_cpu, max(1.0, target))

    def _ram_request_for_op(pipeline, op, avail_ram):
        # Request at least the estimator floor; boost on prior OOM.
        floor = _estimate_mem_floor_gb(op)
        boost = s.op_ram_boost.get((pipeline.pipeline_id, _op_key(op)), 1.0)
        req = floor * boost

        # If the floor is extremely small, add a tiny cushion to reduce near-floor OOM.
        req = max(req, 1.0)

        # Never request more than what's available (otherwise can't schedule here).
        return min(req, avail_ram)

    def _effective_pipeline_score(p):
        base = _base_prio_score(p.priority)
        enq = s.pipeline_enqueued_at.get(p.pipeline_id, s.tick)
        age = max(0, s.tick - enq)
        # Mild aging: +0.5 score per tick waited.
        return base + 0.5 * age

    def _select_next_candidate(pipelines_list):
        # Choose the highest effective score pipeline with at least one ready op.
        best_p = None
        best_score = None
        best_op = None
        for p in pipelines_list:
            if _pipeline_done_or_failed(p):
                continue
            ops = _get_ready_ops(p, max_n=1)
            if not ops:
                continue
            score = _effective_pipeline_score(p)
            if best_score is None or score > best_score:
                best_score = score
                best_p = p
                best_op = ops[0]
        return best_p, best_op

    # ---- tick update ----
    if pipelines or results:
        s.tick += 1

    # ---- ingest new pipelines ----
    for p in pipelines:
        if p.pipeline_id not in s.pipeline_enqueued_at:
            s.pipeline_enqueued_at[p.pipeline_id] = s.tick
        s.waiting_pipelines.append(p)

    # ---- learn from results (RAM boosts on OOM-like failures) ----
    # We keep the learning rule simple and robust to unknown error formats.
    for r in results:
        if not getattr(r, "failed", lambda: False)():
            continue
        err = getattr(r, "error", None)
        err_s = ""
        try:
            err_s = "" if err is None else str(err).lower()
        except Exception:
            err_s = ""
        # Treat anything that looks like OOM / memory as a memory undersizing signal.
        is_oom = ("oom" in err_s) or ("out of memory" in err_s) or ("memory" in err_s) or ("malloc" in err_s)
        if not is_oom:
            continue

        # Increase RAM boost for the failed op(s) so the next attempt requests more.
        # If we can't map to specific pipeline/op reliably, skip.
        pid = getattr(r, "pipeline_id", None)
        if pid is None:
            # Some simulators don't attach pipeline_id to results; try to infer from ops list usage only.
            continue
        rops = getattr(r, "ops", None) or []
        for op in rops:
            k = (pid, _op_key(op))
            prev = s.op_ram_boost.get(k, 1.0)
            # Multiplicative backoff: 1.5x each time, capped to avoid runaway.
            s.op_ram_boost[k] = min(prev * 1.5, 16.0)

    # ---- cleanup waiting list (drop completed/failed pipelines) ----
    if pipelines or results:
        new_wait = []
        for p in s.waiting_pipelines:
            if _pipeline_done_or_failed(p):
                continue
            new_wait.append(p)
        s.waiting_pipelines = new_wait

    # If nothing to do, exit fast.
    if not s.waiting_pipelines and not pipelines and not results:
        return [], []

    suspensions = []  # no preemption in this incremental version
    assignments = []

    # ---- scheduling per pool (packing multiple ops per tick) ----
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool

        # Keep assigning while we have meaningful resources left.
        # Also cap the number of assignments per pool per tick to avoid long loops.
        per_pool_cap = 32
        made = 0

        while made < per_pool_cap and avail_cpu > 0 and avail_ram > 0:
            p, op = _select_next_candidate(s.waiting_pipelines)
            if p is None or op is None:
                break

            cpu_req = _cpu_target_for_priority(p.priority, avail_cpu)
            if cpu_req <= 0:
                break

            ram_req = _ram_request_for_op(p, op, avail_ram)

            # If we can't meet even the estimator floor due to pool pressure, stop packing this pool.
            # This avoids assigning under-provisioned containers that would predictably OOM.
            mem_floor = _estimate_mem_floor_gb(op) * s.op_ram_boost.get((p.pipeline_id, _op_key(op)), 1.0)
            if avail_ram < min(mem_floor, 1.0 if mem_floor <= 0 else mem_floor):
                break

            # If rounding/limits produced a tiny ram_req, ensure minimum 1GB when possible.
            if ram_req < 1.0 and avail_ram >= 1.0:
                ram_req = 1.0

            # Final feasibility check.
            if cpu_req <= 0 or ram_req <= 0 or cpu_req > avail_cpu or ram_req > avail_ram:
                break

            assignments.append(
                Assignment(
                    ops=[op],
                    cpu=cpu_req,
                    ram=ram_req,
                    priority=p.priority,
                    pool_id=pool_id,
                    pipeline_id=p.pipeline_id,
                )
            )

            # Update local pool budget for packing.
            avail_cpu -= cpu_req
            avail_ram -= ram_req
            made += 1

    return suspensions, assignments
