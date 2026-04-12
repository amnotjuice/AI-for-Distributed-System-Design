# policy_key: scheduler_iter_median_simple_014
# context: simple
# model: gpt-5.2-2025-12-11
# llm_cost: 0.053047
# generation_seconds: 43.80
# generated_at: 2026-04-12T01:39:11.364484
@register_scheduler_init(key="scheduler_iter_median_simple_014")
def scheduler_iter_median_simple_014_init(s):
    """Priority-first, latency-oriented scheduler with per-pipeline OOM learning + pool steering.

    Incremental improvements over naive FIFO / prior attempt:
    - Strict priority ordering: QUERY > INTERACTIVE > BATCH_PIPELINE.
    - Per-pipeline OOM backoff using op->pipeline mapping from ExecutionResult.ops.
    - "Min remaining ops" selection within each priority to reduce head-of-line blocking.
    - Pool steering: keep pool 0 preferentially for high-priority when possible; push batch to other pools.
    - Resource shaping:
        * High-priority gets small defaults to start quickly, but can burst when headroom exists.
        * Batch is capped when any high-priority backlog exists (keeps latency down).
    - Conservative failure handling: only treat OOM as retriable signal; other failures stop retries.
    """
    # Per-priority pipeline queues
    s.queues = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }

    # Per-pipeline adaptive state:
    # - ram_mult: increase on OOM to avoid repeat failures
    # - cpu_hint: recent successful cpu allocation (used as a hint, not a hard rule)
    # - ram_hint: recent successful ram allocation
    # - non_oom_fail: if pipeline has FAILED ops and we saw non-OOM failures, stop retrying
    s.pstate = {}

    # Map op identity -> pipeline_id to attribute results back to pipelines
    s.op_to_pipeline = {}

    # Round-robin cursors per priority for tie-breaking when remaining-ops scores match
    s.rr = {Priority.QUERY: 0, Priority.INTERACTIVE: 0, Priority.BATCH_PIPELINE: 0}


@register_scheduler(key="scheduler_iter_median_simple_014")
def scheduler_iter_median_simple_014_scheduler(s, results, pipelines):
    """Latency-oriented, priority-aware scheduler.

    Core logic:
    1) Enqueue new pipelines.
    2) Attribute results back to pipelines (via op id) and update hints:
       - On OOM: increase ram_mult for that pipeline.
       - On success: record cpu/ram hints (lightly).
       - On non-OOM failure: mark pipeline non-retriable if it has FAILED ops.
    3) For each pool, repeatedly assign *one* ready op at a time, preferring:
       - high-priority work first
       - within a priority, pipelines with fewer remaining incomplete ops
       - steer batch away from pool 0 if high-priority backlog exists
       - cap batch allocations when high-priority backlog exists
    """
    suspensions = []
    assignments = []

    # -----------------------------
    # Helpers
    # -----------------------------
    def _ensure_pstate(pipeline_id):
        if pipeline_id not in s.pstate:
            s.pstate[pipeline_id] = {
                "ram_mult": 1.0,
                "cpu_hint": None,
                "ram_hint": None,
                "non_oom_fail": False,
            }
        return s.pstate[pipeline_id]

    def _is_oom_error(err):
        if err is None:
            return False
        msg = str(err).lower()
        return ("oom" in msg) or ("out of memory" in msg)

    def _priority_order():
        return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]

    def _pipeline_remaining_ops(p):
        st = p.runtime_status()
        # Approximate remaining work by counting non-completed operators in the DAG
        # (works even without runtime estimates).
        try:
            total = len(p.values)
        except Exception:
            total = 0
        completed = st.state_counts.get(OperatorState.COMPLETED, 0)
        failed = st.state_counts.get(OperatorState.FAILED, 0)
        # Failed are "not done" unless we decide to stop retrying; still count them as remaining.
        remaining = max(0, total - completed)
        # Slightly penalize pipelines with failures to avoid thrashing them ahead of clean ones.
        remaining += 2 * failed
        return remaining

    def _has_runnable_op(p):
        st = p.runtime_status()
        if st.is_pipeline_successful():
            return False
        if st.state_counts.get(OperatorState.FAILED, 0) > 0 and s.pstate.get(p.pipeline_id, {}).get("non_oom_fail", False):
            return False
        ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        return bool(ops)

    def _global_hp_backlog():
        # Any runnable QUERY/INTERACTIVE anywhere means we should protect headroom vs batch.
        for pri in (Priority.QUERY, Priority.INTERACTIVE):
            for p in s.queues[pri]:
                if _has_runnable_op(p):
                    return True
        return False

    def _pick_pipeline(pri):
        """Pick best pipeline within priority: min remaining ops; RR tie-break."""
        q = s.queues[pri]
        if not q:
            return None

        # Clean out completed / non-retriable failed pipelines opportunistically
        cleaned = []
        for p in q:
            st = p.runtime_status()
            if st.is_pipeline_successful():
                continue
            if st.state_counts.get(OperatorState.FAILED, 0) > 0 and s.pstate.get(p.pipeline_id, {}).get("non_oom_fail", False):
                continue
            cleaned.append(p)
        s.queues[pri] = cleaned
        q = cleaned
        if not q:
            s.rr[pri] = 0
            return None

        # Score by remaining ops; only consider pipelines with a runnable op right now.
        candidates = []
        for idx, p in enumerate(q):
            if not _has_runnable_op(p):
                continue
            candidates.append(( _pipeline_remaining_ops(p), idx, p ))
        if not candidates:
            return None

        candidates.sort(key=lambda x: (x[0], x[1]))
        # RR tie-break among same remaining-ops score
        best_score = candidates[0][0]
        tied = [c for c in candidates if c[0] == best_score]
        cursor = s.rr[pri] % len(tied)
        chosen = tied[cursor][2]
        s.rr[pri] = (s.rr[pri] + 1) % max(1, len(tied))
        return chosen

    def _pool_preference(pool_id, pri, hp_backlog):
        # Reserve pool 0 preferentially for high-priority if backlog exists.
        if s.executor.num_pools <= 1:
            return True
        if pri == Priority.BATCH_PIPELINE and hp_backlog and pool_id == 0:
            return False
        return True

    def _compute_request(pool, pri, pipeline_id, avail_cpu, avail_ram, hp_backlog):
        pst = _ensure_pstate(pipeline_id)

        # Defaults biased for latency: start small for high-pri so more can start quickly.
        if pri == Priority.QUERY:
            base_cpu = 1.0
            base_ram = pool.max_ram_pool * 0.08
            # Allow a small burst when pool is mostly idle
            if avail_cpu >= pool.max_cpu_pool * 0.60:
                base_cpu = min(2.0, avail_cpu)
        elif pri == Priority.INTERACTIVE:
            base_cpu = 2.0 if pool.max_cpu_pool >= 2 else 1.0
            base_ram = pool.max_ram_pool * 0.14
            if avail_cpu >= pool.max_cpu_pool * 0.70:
                base_cpu = min(4.0, avail_cpu)
        else:
            # Batch: throughput-oriented when allowed, but kept in check under hp backlog.
            base_cpu = min(max(2.0, pool.max_cpu_pool * 0.50), avail_cpu)
            base_ram = pool.max_ram_pool * 0.28

        # Use hints from recent successes to avoid chronic under/over-sizing.
        # For latency, we don't slavishly follow hints—just nudge.
        if pst["cpu_hint"] is not None:
            if pri in (Priority.QUERY, Priority.INTERACTIVE):
                base_cpu = max(base_cpu, min(pst["cpu_hint"], base_cpu * 1.5))
            else:
                base_cpu = max(base_cpu, min(pst["cpu_hint"], pool.max_cpu_pool))
        if pst["ram_hint"] is not None:
            base_ram = max(base_ram, min(pst["ram_hint"], pool.max_ram_pool))

        # OOM backoff is applied primarily to RAM.
        ram_req = base_ram * pst["ram_mult"]
        cpu_req = base_cpu

        # Under hp backlog, cap batch allocations to preserve headroom.
        if pri == Priority.BATCH_PIPELINE and hp_backlog:
            cpu_req = min(cpu_req, pool.max_cpu_pool * 0.45)
            ram_req = min(ram_req, pool.max_ram_pool * 0.45)

        # Clamp to available + ensure minimums
        cpu_req = max(1.0, min(cpu_req, avail_cpu, pool.max_cpu_pool))
        ram_req = max(1.0, min(ram_req, avail_ram, pool.max_ram_pool))
        return cpu_req, ram_req

    # -----------------------------
    # 1) Enqueue new pipelines
    # -----------------------------
    for p in pipelines:
        _ensure_pstate(p.pipeline_id)
        s.queues[p.priority].append(p)

    if not pipelines and not results:
        return [], []

    # -----------------------------
    # 2) Process results -> update per-pipeline state
    # -----------------------------
    for r in results:
        # Attribute to pipeline via op identity (best effort)
        pipeline_id = None
        try:
            if getattr(r, "ops", None):
                pipeline_id = s.op_to_pipeline.get(id(r.ops[0]))
        except Exception:
            pipeline_id = None

        if pipeline_id is None:
            # Can't attribute; ignore (keeps scheduler robust).
            continue

        pst = _ensure_pstate(pipeline_id)

        if hasattr(r, "failed") and r.failed():
            if _is_oom_error(getattr(r, "error", None)):
                # Increase RAM multiplier aggressively to stop repeated OOMs.
                pst["ram_mult"] = min(pst["ram_mult"] * 1.9, 32.0)
            else:
                # Mark non-OOM failures as non-retriable if pipeline currently has FAILED ops.
                # We don't have the pipeline object here; we will enforce drop in queue cleanup.
                pst["non_oom_fail"] = True
        else:
            # Success: record hints (light smoothing)
            try:
                if getattr(r, "cpu", None) is not None:
                    pst["cpu_hint"] = r.cpu if pst["cpu_hint"] is None else (0.7 * pst["cpu_hint"] + 0.3 * r.cpu)
                if getattr(r, "ram", None) is not None:
                    pst["ram_hint"] = r.ram if pst["ram_hint"] is None else (0.7 * pst["ram_hint"] + 0.3 * r.ram)
            except Exception:
                pass
            # If we're succeeding, very slowly relax ram_mult back toward 1.0 to reduce waste.
            pst["ram_mult"] = max(1.0, pst["ram_mult"] * 0.98)

    # -----------------------------
    # 3) Scheduling loop
    # -----------------------------
    hp_backlog = _global_hp_backlog()

    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]

        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool
        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # Try to pack multiple ops per pool per tick, but always pick at most one op per assignment
        # to reduce head-of-line blocking and keep interactive latency down.
        while avail_cpu > 0 and avail_ram > 0:
            chosen_pipeline = None
            chosen_pri = None

            # Priority-first: attempt in order; also respect pool steering.
            for pri in _priority_order():
                if not _pool_preference(pool_id, pri, hp_backlog):
                    continue
                p = _pick_pipeline(pri)
                if p is None:
                    continue
                chosen_pipeline = p
                chosen_pri = pri
                break

            if chosen_pipeline is None:
                # If we refused batch on pool 0 due to steering, allow it only if no hp backlog runnable.
                if pool_id == 0 and s.executor.num_pools > 1 and not hp_backlog:
                    p = _pick_pipeline(Priority.BATCH_PIPELINE)
                    if p is not None:
                        chosen_pipeline = p
                        chosen_pri = Priority.BATCH_PIPELINE
                if chosen_pipeline is None:
                    break

            st = chosen_pipeline.runtime_status()
            # Assign exactly one ready op
            op_list = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
            if not op_list:
                # Nothing runnable at this instant; try next selection (avoid tight infinite loops by breaking)
                break

            cpu_req, ram_req = _compute_request(
                pool=pool,
                pri=chosen_pri,
                pipeline_id=chosen_pipeline.pipeline_id,
                avail_cpu=avail_cpu,
                avail_ram=avail_ram,
                hp_backlog=hp_backlog,
            )

            if cpu_req <= 0 or ram_req <= 0:
                break

            # Record mapping so we can attribute results to pipeline later
            try:
                s.op_to_pipeline[id(op_list[0])] = chosen_pipeline.pipeline_id
            except Exception:
                pass

            assignments.append(
                Assignment(
                    ops=op_list,
                    cpu=cpu_req,
                    ram=ram_req,
                    priority=chosen_pipeline.priority,
                    pool_id=pool_id,
                    pipeline_id=chosen_pipeline.pipeline_id,
                )
            )

            # Local accounting to avoid overscheduling this pool in this tick
            avail_cpu -= cpu_req
            avail_ram -= ram_req

            # Update hp_backlog after scheduling a high-priority op (it may reduce backlog slightly);
            # we keep it simple and only recompute if we scheduled something non-batch.
            if chosen_pri in (Priority.QUERY, Priority.INTERACTIVE):
                hp_backlog = _global_hp_backlog()

            # Small guard: if remaining headroom is tiny, stop trying to place more.
            if avail_cpu < 1.0 or avail_ram < 1.0:
                break

    return suspensions, assignments
