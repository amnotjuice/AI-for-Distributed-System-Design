# policy_key: scheduler_iter_median_simple_002
# context: simple
# model: gpt-5.2-2025-12-11
# llm_cost: 0.042099
# generation_seconds: 41.39
# generated_at: 2026-04-12T01:08:13.440475
@register_scheduler_init(key="scheduler_iter_median_simple_002")
def scheduler_iter_median_simple_002_init(s):
    """Priority-first latency scheduler (incremental improvement over naive FIFO).

    Design goals (small, obvious improvements):
    - Strict priority across classes: QUERY > INTERACTIVE > BATCH.
    - Within high priority, pick the pipeline that is most likely to finish sooner:
      * prefer pipelines with fewer remaining operators (SRPT-lite heuristic).
    - Resource shaping for latency:
      * give higher CPU shares to high-priority ops to reduce service time,
        but still avoid letting batch consume the whole pool when HP backlog exists.
    - Basic OOM adaptation:
      * if an op OOMs, bump RAM multiplier for that pipeline and retry.

    Notes:
    - We intentionally avoid complex preemption because simulator surface for enumerating
      running containers is not guaranteed here. We instead reserve/cap batch usage
      when HP backlog exists to reduce interference and tail latency.
    """
    s.queues = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }
    # per pipeline adaptive multipliers
    s.pstate = {}  # pipeline_id -> {"ram_mult": float}
    # to provide mild fairness within each priority (aging counter)
    s.age = {}  # pipeline_id -> int


@register_scheduler(key="scheduler_iter_median_simple_002")
def scheduler_iter_median_simple_002_scheduler(s, results, pipelines):
    """
    Step algorithm:
    1) Enqueue new pipelines by priority.
    2) Update per-pipeline RAM multiplier on OOM failures.
    3) For each pool, repeatedly pick the best next runnable op using:
         - priority order
         - SRPT-lite within priority (fewest remaining ops, then oldest)
       and assign with latency-oriented CPU sizing + batch caps during HP backlog.
    """
    # -----------------------------
    # Helpers
    # -----------------------------
    def _ensure_pipeline_state(p):
        if p.pipeline_id not in s.pstate:
            s.pstate[p.pipeline_id] = {"ram_mult": 1.0}
        if p.pipeline_id not in s.age:
            s.age[p.pipeline_id] = 0

    def _is_oom_error(err):
        if err is None:
            return False
        msg = str(err).lower()
        return ("oom" in msg) or ("out of memory" in msg) or ("killed" in msg and "memory" in msg)

    def _priority_order():
        return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]

    def _pipeline_remaining_ops(p):
        # Use whatever status provides; fallback to DAG size heuristics.
        st = p.runtime_status()
        try:
            completed = st.state_counts.get(OperatorState.COMPLETED, 0)
            failed = st.state_counts.get(OperatorState.FAILED, 0)
            pending = st.state_counts.get(OperatorState.PENDING, 0)
            assigned = st.state_counts.get(OperatorState.ASSIGNED, 0)
            running = st.state_counts.get(OperatorState.RUNNING, 0)
            # Remaining = everything not completed; FAILED are considered remaining (retryable for OOM path)
            remaining = pending + failed + assigned + running
            # If status accounting is odd, clamp at least 1 when not successful.
            if st.is_pipeline_successful():
                return 0
            return max(1, remaining)
        except Exception:
            # Fallback: approximate by total ops - completed
            try:
                total = len(getattr(p, "values", []) or [])
            except Exception:
                total = 1
            if st.is_pipeline_successful():
                return 0
            return max(1, total)

    def _has_runnable_op(p):
        st = p.runtime_status()
        if st.is_pipeline_successful():
            return False
        # treat non-OOM failures as terminal only if the simulator marks them FAILED;
        # we still allow FAILED to be assignable (ASSIGNABLE_STATES) for retries.
        ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        return bool(ops)

    def _pick_best_pipeline(pri):
        """Pick best runnable pipeline within priority using SRPT-lite + aging."""
        q = s.queues[pri]
        best_idx = None
        best_key = None
        # scan queue; keep it simple and robust
        for i, p in enumerate(q):
            st = p.runtime_status()
            if st.is_pipeline_successful():
                continue
            if not _has_runnable_op(p):
                continue

            rem = _pipeline_remaining_ops(p)
            age = s.age.get(p.pipeline_id, 0)
            # Primary: fewer remaining ops (finish faster), Secondary: older first (fairness)
            key = (rem, -age)
            if best_key is None or key < best_key:
                best_key = key
                best_idx = i

        if best_idx is None:
            return None
        return q.pop(best_idx)

    def _hp_backlog_exists():
        # any runnable op in QUERY or INTERACTIVE queues
        for pri in (Priority.QUERY, Priority.INTERACTIVE):
            for p in s.queues[pri]:
                if _has_runnable_op(p):
                    return True
        return False

    def _compute_request(pool, pri, pipeline_id, avail_cpu, avail_ram, hp_backlog):
        """Latency-oriented sizing with batch caps when HP backlog exists."""
        ram_mult = s.pstate.get(pipeline_id, {}).get("ram_mult", 1.0)

        # If HP backlog exists, keep explicit headroom from batch.
        # (We apply as a cap on what batch can take in this pool this tick.)
        if pri == Priority.BATCH_PIPELINE and hp_backlog:
            avail_cpu = min(avail_cpu, pool.max_cpu_pool * 0.45)
            avail_ram = min(avail_ram, pool.max_ram_pool * 0.50)

        # CPU sizing: give more to high priority so they finish sooner.
        if pri == Priority.QUERY:
            base_cpu = max(1.0, pool.max_cpu_pool * 0.70)
            base_ram = max(1.0, pool.max_ram_pool * 0.18)
        elif pri == Priority.INTERACTIVE:
            base_cpu = max(1.0, pool.max_cpu_pool * 0.75)
            base_ram = max(1.0, pool.max_ram_pool * 0.25)
        else:
            # batch: throughput-oriented but capped above if HP backlog exists
            base_cpu = max(1.0, pool.max_cpu_pool * 0.80)
            base_ram = max(1.0, pool.max_ram_pool * 0.40)

        cpu_req = min(base_cpu, avail_cpu, pool.max_cpu_pool)
        ram_req = min(base_ram * ram_mult, avail_ram, pool.max_ram_pool)

        # Ensure strictly positive
        cpu_req = max(0.0, cpu_req)
        ram_req = max(0.0, ram_req)
        return cpu_req, ram_req

    def _infer_pipeline_id_from_result(r):
        # Best-effort: many sims attach pipeline_id to operator objects.
        # r.ops may be a list of operators.
        try:
            if getattr(r, "ops", None):
                op0 = r.ops[0]
                pid = getattr(op0, "pipeline_id", None)
                if pid is not None:
                    return pid
        except Exception:
            pass
        return None

    # -----------------------------
    # 1) Enqueue new pipelines
    # -----------------------------
    for p in pipelines:
        _ensure_pipeline_state(p)
        s.queues[p.priority].append(p)

    # Early exit if nothing to do
    if not pipelines and not results:
        return [], []

    # -----------------------------
    # 2) Update state from results (OOM backoff)
    # -----------------------------
    for r in results:
        if hasattr(r, "failed") and r.failed() and _is_oom_error(getattr(r, "error", None)):
            pid = _infer_pipeline_id_from_result(r)
            if pid is not None:
                # Exponential-ish bump but bounded; we want fast convergence for latency.
                pst = s.pstate.get(pid)
                if pst is None:
                    s.pstate[pid] = {"ram_mult": 1.0}
                    pst = s.pstate[pid]
                pst["ram_mult"] = min(pst["ram_mult"] * 2.0, 32.0)
            else:
                # If we can't map to a pipeline, do a small bump on all queued pipelines
                # of the same priority to reduce repeated OOM churn.
                for p in s.queues.get(r.priority, []):
                    _ensure_pipeline_state(p)
                    s.pstate[p.pipeline_id]["ram_mult"] = min(s.pstate[p.pipeline_id]["ram_mult"] * 1.25, 32.0)

    # Aging: increment for queued pipelines to avoid starvation within each priority
    for pri in _priority_order():
        for p in s.queues[pri]:
            s.age[p.pipeline_id] = s.age.get(p.pipeline_id, 0) + 1

    # -----------------------------
    # 3) Assign work
    # -----------------------------
    suspensions = []
    assignments = []

    hp_backlog = _hp_backlog_exists()

    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool

        # Keep placing small "atoms" (one op at a time) to reduce HoL blocking.
        while avail_cpu > 0 and avail_ram > 0:
            chosen_pipeline = None
            chosen_pri = None

            # Strict priority selection
            for pri in _priority_order():
                p = _pick_best_pipeline(pri)
                if p is None:
                    continue
                # Validate runnable op still exists (state can change across scheduling loop)
                st = p.runtime_status()
                op_list = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
                if not op_list:
                    # Put it back and continue searching
                    s.queues[pri].append(p)
                    continue
                chosen_pipeline = p
                chosen_pri = pri
                chosen_ops = op_list
                break

            if chosen_pipeline is None:
                break

            cpu_req, ram_req = _compute_request(
                pool=pool,
                pri=chosen_pri,
                pipeline_id=chosen_pipeline.pipeline_id,
                avail_cpu=avail_cpu,
                avail_ram=avail_ram,
                hp_backlog=hp_backlog,
            )

            # If we can't fit even minimal resources, put back and stop scheduling this pool.
            if cpu_req < 1.0 or ram_req < 1.0:
                s.queues[chosen_pri].append(chosen_pipeline)
                break

            # Create assignment
            assignments.append(
                Assignment(
                    ops=chosen_ops,
                    cpu=cpu_req,
                    ram=ram_req,
                    priority=chosen_pipeline.priority,
                    pool_id=pool_id,
                    pipeline_id=chosen_pipeline.pipeline_id,
                )
            )

            # Local accounting to avoid oversubscription in this tick
            avail_cpu -= cpu_req
            avail_ram -= ram_req

            # Reset age since we just served it; re-enqueue for future ops
            s.age[chosen_pipeline.pipeline_id] = 0
            s.queues[chosen_pri].append(chosen_pipeline)

            # HP backlog might have been reduced; recompute occasionally (cheap enough here)
            hp_backlog = _hp_backlog_exists()

    return suspensions, assignments
