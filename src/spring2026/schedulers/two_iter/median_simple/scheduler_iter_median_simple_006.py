# policy_key: scheduler_iter_median_simple_006
# context: simple
# model: gpt-5.2-2025-12-11
# llm_cost: 0.056183
# generation_seconds: 32.50
# generated_at: 2026-04-12T01:11:10.391252
@register_scheduler_init(key="scheduler_iter_median_simple_006")
def scheduler_iter_median_simple_006_init(s):
    """Priority-first, latency-biased FIFO with better runnable selection and per-pipeline OOM backoff.

    Changes vs naive / earlier attempt (aimed at lower median latency):
    - Strict priority service: QUERY > INTERACTIVE > BATCH.
    - Batch gating: if ANY high-priority backlog exists, do not schedule new batch work
      unless there is "excess" capacity (prevents batch from inflating HP queueing).
    - Latency-biased sizing: give QUERY/INTERACTIVE more CPU (finish sooner) while keeping
      RAM conservative + OOM-driven ramp-up per pipeline.
    - Better runnable selection: search within each priority queue for a runnable pipeline
      (avoid repeatedly picking pipelines blocked on parents).
    - Per-pipeline failure handling: on OOM, increase RAM multiplier; on non-OOM failures,
      mark pipeline as non-retriable and stop scheduling it.
    """
    # FIFO queues per priority
    s.q_query = []
    s.q_interactive = []
    s.q_batch = []

    # Per-pipeline adaptive state
    # ram_mult: multiplicative bump after OOM; non_oom_fail: stop retrying when FAILED exists.
    s.pstate = {}  # pipeline_id -> {"ram_mult": float, "non_oom_fail": bool}

    # Round-robin cursor per queue (so scans are fair)
    s.cursor = {
        Priority.QUERY: 0,
        Priority.INTERACTIVE: 0,
        Priority.BATCH_PIPELINE: 0,
    }


@register_scheduler(key="scheduler_iter_median_simple_006")
def scheduler_iter_median_simple_006_scheduler(s, results, pipelines):
    """
    Returns:
        suspensions: (unused; no preemption primitives exposed in the provided interface)
        assignments: schedule ready operators with strict priority and batch gating
    """
    # -----------------------------
    # Helpers
    # -----------------------------
    def _ensure_pstate(pipeline_id):
        if pipeline_id not in s.pstate:
            s.pstate[pipeline_id] = {"ram_mult": 1.0, "non_oom_fail": False}
        return s.pstate[pipeline_id]

    def _is_oom_error(err):
        if err is None:
            return False
        msg = str(err).lower()
        return ("oom" in msg) or ("out of memory" in msg) or ("memoryerror" in msg)

    def _queues_by_priority():
        return {
            Priority.QUERY: s.q_query,
            Priority.INTERACTIVE: s.q_interactive,
            Priority.BATCH_PIPELINE: s.q_batch,
        }

    def _priority_order():
        return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]

    def _infer_pipeline_id_from_result(r):
        # Best-effort: some simulator versions attach pipeline_id to op objects or directly.
        pid = getattr(r, "pipeline_id", None)
        if pid is not None:
            return pid
        ops = getattr(r, "ops", None)
        if ops:
            op0 = ops[0]
            pid = getattr(op0, "pipeline_id", None)
            if pid is not None:
                return pid
        return None

    def _pipeline_should_drop(p):
        st = p.runtime_status()
        if st.is_pipeline_successful():
            return True
        pst = s.pstate.get(p.pipeline_id, None)
        if pst and pst.get("non_oom_fail", False) and st.state_counts.get(OperatorState.FAILED, 0) > 0:
            return True
        return False

    def _has_hp_backlog():
        # backlog means: some QUERY/INTERACTIVE pipeline has an assignable op now
        for pri in (Priority.QUERY, Priority.INTERACTIVE):
            q = _queues_by_priority()[pri]
            for p in q:
                if _pipeline_should_drop(p):
                    continue
                st = p.runtime_status()
                ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
                if ops:
                    return True
        return False

    def _find_runnable_pipeline(pri):
        """Scan within a priority queue (RR) to find a pipeline with a ready op."""
        q = _queues_by_priority()[pri]
        if not q:
            return None, None

        # Clean a bounded number of entries (avoid O(n^2) under churn)
        n = len(q)
        if n == 0:
            return None, None

        start = s.cursor[pri] % n
        for k in range(n):
            idx = (start + k) % n
            p = q[idx]

            if _pipeline_should_drop(p):
                # remove and keep scanning
                q.pop(idx)
                if not q:
                    s.cursor[pri] = 0
                    return None, None
                # adjust start if needed due to removal
                if idx < start:
                    start -= 1
                n = len(q)
                if n == 0:
                    s.cursor[pri] = 0
                    return None, None
                continue

            st = p.runtime_status()
            op_list = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
            if not op_list:
                continue

            # Advance cursor beyond this element for next scan
            s.cursor[pri] = (idx + 1) % max(1, len(q))
            return p, op_list

        return None, None

    def _cpu_target(pool, pri, avail_cpu):
        # Latency-biased: give HP more CPU so they complete sooner.
        # Still keep a floor of 1 vCPU.
        if pri == Priority.QUERY:
            # Prefer to finish quickly; often short critical-path ops.
            target = max(1.0, min(avail_cpu, max(2.0, pool.max_cpu_pool * 0.85)))
        elif pri == Priority.INTERACTIVE:
            target = max(1.0, min(avail_cpu, max(2.0, pool.max_cpu_pool * 0.75)))
        else:
            # Batch: smaller slices to reduce interference and preserve headroom.
            target = max(1.0, min(avail_cpu, max(1.0, pool.max_cpu_pool * 0.35)))
        return target

    def _ram_target(pool, pri, pipeline_id, avail_ram):
        pst = _ensure_pstate(pipeline_id)

        # Conservative baselines; OOM backoff grows these.
        if pri == Priority.QUERY:
            base = max(1.0, pool.max_ram_pool * 0.12)
        elif pri == Priority.INTERACTIVE:
            base = max(1.0, pool.max_ram_pool * 0.22)
        else:
            base = max(1.0, pool.max_ram_pool * 0.30)

        req = base * pst["ram_mult"]
        return max(1.0, min(req, avail_ram, pool.max_ram_pool))

    # -----------------------------
    # Enqueue new pipelines
    # -----------------------------
    for p in pipelines:
        _ensure_pstate(p.pipeline_id)
        if p.priority == Priority.QUERY:
            s.q_query.append(p)
        elif p.priority == Priority.INTERACTIVE:
            s.q_interactive.append(p)
        else:
            s.q_batch.append(p)

    if not pipelines and not results:
        return [], []

    # -----------------------------
    # Update pstate based on results (OOM backoff, non-OOM failure suppression)
    # -----------------------------
    for r in results:
        if hasattr(r, "failed") and r.failed():
            pid = _infer_pipeline_id_from_result(r)
            if pid is None:
                continue
            pst = _ensure_pstate(pid)
            if _is_oom_error(getattr(r, "error", None)):
                # Exponential-ish backoff; cap to avoid runaway.
                pst["ram_mult"] = min(pst["ram_mult"] * 1.8, 32.0)
            else:
                pst["non_oom_fail"] = True

    # -----------------------------
    # Build assignments
    # -----------------------------
    suspensions = []
    assignments = []

    hp_backlog = _has_hp_backlog()

    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool

        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # Pack multiple ops per pool per tick, but keep HP first.
        while avail_cpu > 0 and avail_ram > 0:
            chosen_p = None
            chosen_ops = None
            chosen_pri = None

            for pri in _priority_order():
                # Batch gating: if HP backlog exists, avoid starting batch unless there's ample slack.
                if pri == Priority.BATCH_PIPELINE and hp_backlog:
                    # Only allow batch if we have clear excess headroom.
                    # This is intentionally strict to cut median latency.
                    if avail_cpu < pool.max_cpu_pool * 0.50 or avail_ram < pool.max_ram_pool * 0.50:
                        continue

                p, op_list = _find_runnable_pipeline(pri)
                if p is None:
                    continue
                chosen_p, chosen_ops, chosen_pri = p, op_list, pri
                break

            if chosen_p is None:
                break

            cpu_req = _cpu_target(pool, chosen_pri, avail_cpu)
            ram_req = _ram_target(pool, chosen_pri, chosen_p.pipeline_id, avail_ram)

            # If we cannot allocate at least minimal resources, stop scheduling in this pool.
            if cpu_req <= 0 or ram_req <= 0:
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

            # Local accounting to avoid overscheduling within this tick.
            avail_cpu -= cpu_req
            avail_ram -= ram_req

            # If we just scheduled HP work, keep hp_backlog True for stronger batch gating.
            if chosen_pri in (Priority.QUERY, Priority.INTERACTIVE):
                hp_backlog = True

    return suspensions, assignments
