# policy_key: scheduler_iter_median_simple_003
# context: simple
# model: gpt-5.2-2025-12-11
# llm_cost: 0.048469
# generation_seconds: 50.30
# generated_at: 2026-04-12T01:09:03.746519
@register_scheduler_init(key="scheduler_iter_median_simple_003")
def scheduler_iter_median_simple_003_init(s):
    """Priority-first, latency-oriented scheduler with better placement and precise OOM backoff.

    Incremental improvements over naive FIFO / earlier draft:
    - Strict priority ordering: QUERY > INTERACTIVE > BATCH (never let batch steal capacity while HP runnable).
    - Global (cross-pool) placement: pick the best pool for each chosen op (most headroom), not just first pool.
    - SRPT-ish tie-break within a priority: prefer pipelines with fewer remaining (unfinished) ops to reduce median latency.
    - Precise OOM retry sizing: track RAM multiplier per-operator using ExecutionResult.ops -> pipeline mapping.
    - Conservative CPU shaping for HP: enough CPU to finish quickly, but avoid monopolizing pools.
    """
    # Pipelines waiting to be scheduled (we keep them all here; selection is computed each tick)
    s.waiting = []  # List[Pipeline]

    # Map operator identity -> pipeline id (to attribute failures accurately)
    s.op_to_pid = {}  # Dict[int, str]

    # Per-operator sizing multipliers (persist across retries)
    # key: op_id (int), value: {"ram_mult": float, "cpu_mult": float}
    s.op_hint = {}

    # Per-pipeline bookkeeping
    # key: pipeline_id, value: {"drop": bool}
    s.p_hint = {}

    # Round-robin cursor per priority to avoid starvation within same priority
    s.rr = {Priority.QUERY: 0, Priority.INTERACTIVE: 0, Priority.BATCH_PIPELINE: 0}


@register_scheduler(key="scheduler_iter_median_simple_003")
def scheduler_iter_median_simple_003_scheduler(s, results, pipelines):
    """
    Each tick:
      1) Ingest pipelines; build op->pipeline mapping.
      2) Update per-operator RAM multiplier on OOM (precise attribution via result.ops).
      3) Build runnable candidates by priority.
      4) While resources exist, greedily schedule highest priority runnable op to best-fitting pool.
         - Never schedule BATCH if any QUERY/INTERACTIVE runnable anywhere (protect latency).
         - Within same priority, prefer pipelines with fewer remaining ops (SRPT-ish proxy).
    """
    # -----------------------------
    # Helpers
    # -----------------------------
    def _op_id(op):
        # Use Python object identity as stable key within the simulator process
        return id(op)

    def _is_oom_error(err):
        if err is None:
            return False
        msg = str(err).lower()
        return ("oom" in msg) or ("out of memory" in msg) or ("memory" in msg and ("exceed" in msg or "alloc" in msg))

    def _ensure_pipeline_hint(p):
        if p.pipeline_id not in s.p_hint:
            s.p_hint[p.pipeline_id] = {"drop": False}
        return s.p_hint[p.pipeline_id]

    def _ensure_op_hint(opid):
        if opid not in s.op_hint:
            s.op_hint[opid] = {"ram_mult": 1.0, "cpu_mult": 1.0}
        return s.op_hint[opid]

    def _priority_order():
        return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]

    def _pipeline_remaining_ops(p):
        # SRPT-ish proxy: count unfinished ops (including pending/assigned/running/failed)
        st = p.runtime_status()
        # If runtime_status exposes counts, use them; otherwise approximate
        # We avoid counting COMPLETED.
        cnt = 0
        for state in [OperatorState.PENDING, OperatorState.ASSIGNED, OperatorState.RUNNING, OperatorState.SUSPENDING, OperatorState.FAILED]:
            cnt += st.state_counts.get(state, 0)
        return cnt

    def _get_runnable_op(p):
        # One operator at a time: smaller scheduling atom reduces HoL blocking for interactive work
        st = p.runtime_status()
        if st.is_pipeline_successful():
            return None
        if _ensure_pipeline_hint(p).get("drop", False):
            return None
        # Only schedule ops with parents complete (DAG correctness)
        ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        if not ops:
            return None
        return ops[0]

    def _base_request(pool, pri):
        # Latency-oriented shaping:
        # - QUERY: small-but-fast (avoid huge allocations), but not starved (give moderate CPU)
        # - INTERACTIVE: moderate
        # - BATCH: larger only when no high-priority work runnable
        if pri == Priority.QUERY:
            base_cpu = min(max(2.0, pool.max_cpu_pool * 0.35), max(2.0, pool.max_cpu_pool))
            base_ram = max(1.0, pool.max_ram_pool * 0.12)
        elif pri == Priority.INTERACTIVE:
            base_cpu = min(max(2.0, pool.max_cpu_pool * 0.40), max(2.0, pool.max_cpu_pool))
            base_ram = max(1.0, pool.max_ram_pool * 0.20)
        else:
            base_cpu = min(max(2.0, pool.max_cpu_pool * 0.55), pool.max_cpu_pool)
            base_ram = max(1.0, pool.max_ram_pool * 0.35)
        return base_cpu, base_ram

    def _fit_request(pool, avail_cpu, avail_ram, pri, opid, hp_runnable_exists):
        # Hard policy: if any high-priority runnable exists, don't schedule batch at all
        if pri == Priority.BATCH_PIPELINE and hp_runnable_exists:
            return None

        base_cpu, base_ram = _base_request(pool, pri)
        oh = _ensure_op_hint(opid)

        cpu_req = base_cpu * oh["cpu_mult"]
        ram_req = base_ram * oh["ram_mult"]

        # Clamp to available/pool max; enforce minimums
        cpu_req = max(1.0, min(cpu_req, avail_cpu, pool.max_cpu_pool))
        ram_req = max(1.0, min(ram_req, avail_ram, pool.max_ram_pool))

        # If it can't even fit minimums, reject
        if cpu_req <= 0 or ram_req <= 0:
            return None
        if cpu_req > avail_cpu or ram_req > avail_ram:
            return None
        return cpu_req, ram_req

    def _pool_score(pool, avail_cpu, avail_ram, cpu_req, ram_req, pri):
        # Prefer pools that will still have headroom after placement.
        # Slightly bias QUERY/INTERACTIVE toward larger headroom to reduce contention.
        post_cpu = max(0.0, avail_cpu - cpu_req)
        post_ram = max(0.0, avail_ram - ram_req)

        headroom = (post_cpu / max(1.0, pool.max_cpu_pool)) + (post_ram / max(1.0, pool.max_ram_pool))
        waste = ((avail_cpu - cpu_req) / max(1.0, pool.max_cpu_pool)) + ((avail_ram - ram_req) / max(1.0, pool.max_ram_pool))

        # For HP: prioritize headroom more; for batch: prioritize not wasting scarce RAM
        if pri in (Priority.QUERY, Priority.INTERACTIVE):
            return 2.0 * headroom + 0.5 * waste
        return 1.0 * headroom + 0.25 * waste

    def _refresh_pool_avail():
        # Local view to avoid overscheduling within a tick
        return [(s.executor.pools[i].avail_cpu_pool, s.executor.pools[i].avail_ram_pool) for i in range(s.executor.num_pools)]

    # -----------------------------
    # 1) Ingest new pipelines + build op->pipeline mapping
    # -----------------------------
    for p in pipelines:
        _ensure_pipeline_hint(p)
        s.waiting.append(p)

        # Best-effort op discovery: Pipeline.values is documented as DAG of operators.
        # We assume it's iterable over operators; if not, we simply won't map those ops.
        try:
            for op in p.values:
                s.op_to_pid[_op_id(op)] = p.pipeline_id
        except Exception:
            pass

    # Early exit if nothing changed
    if not pipelines and not results:
        return [], []

    # -----------------------------
    # 2) Update hints from execution results (precise OOM attribution)
    # -----------------------------
    for r in results:
        if hasattr(r, "failed") and r.failed():
            # Attribute to first op in result (common case: single-op assignment)
            opid = None
            try:
                if getattr(r, "ops", None):
                    opid = _op_id(r.ops[0])
            except Exception:
                opid = None

            # If we can attribute, adjust per-op; otherwise ignore (fallback behavior)
            if opid is not None:
                oh = _ensure_op_hint(opid)
                if _is_oom_error(getattr(r, "error", None)):
                    # Exponential-ish backoff on RAM to converge quickly
                    oh["ram_mult"] = min(oh["ram_mult"] * 1.9, 32.0)
                else:
                    # Non-OOM failures: mark pipeline as drop to avoid retry loops (latency protection)
                    pid = s.op_to_pid.get(opid, None)
                    if pid is not None and pid in s.p_hint:
                        s.p_hint[pid]["drop"] = True

    # -----------------------------
    # 3) Build runnable candidates grouped by priority
    # -----------------------------
    # Clean up finished/dropped pipelines in place (keep order stable-ish)
    new_waiting = []
    for p in s.waiting:
        st = p.runtime_status()
        if st.is_pipeline_successful():
            continue
        if _ensure_pipeline_hint(p).get("drop", False):
            continue
        new_waiting.append(p)
    s.waiting = new_waiting

    # Gather runnable (pipeline, op) pairs
    runnable = {Priority.QUERY: [], Priority.INTERACTIVE: [], Priority.BATCH_PIPELINE: []}
    for p in s.waiting:
        op = _get_runnable_op(p)
        if op is None:
            continue
        runnable[p.priority].append((p, op))

    hp_runnable_exists = (len(runnable[Priority.QUERY]) > 0) or (len(runnable[Priority.INTERACTIVE]) > 0)

    # Within each priority: SRPT-ish (fewer remaining ops first), then round-robin-ish via cursor
    for pri in _priority_order():
        if not runnable[pri]:
            continue
        # Stable sort by remaining ops (cheap proxy for "shorter job first")
        runnable[pri].sort(key=lambda po: _pipeline_remaining_ops(po[0]))

        # Rotate by rr cursor to avoid repeatedly picking the same pipeline when ties exist
        c = s.rr.get(pri, 0) % max(1, len(runnable[pri]))
        if c:
            runnable[pri] = runnable[pri][c:] + runnable[pri][:c]

    # -----------------------------
    # 4) Greedy global placement: pick best pool for best runnable op
    # -----------------------------
    suspensions = []  # no preemption in this iteration (keep policy small + safe)
    assignments = []

    pool_avail = _refresh_pool_avail()

    made_any = True
    while made_any:
        made_any = False

        # Stop if all pools have no resources
        if all((cpu <= 0 or ram <= 0) for cpu, ram in pool_avail):
            break

        # Choose next op by strict priority
        chosen = None  # (pri, idx_in_list, pipeline, op)
        for pri in _priority_order():
            if pri == Priority.BATCH_PIPELINE and hp_runnable_exists:
                continue
            if runnable[pri]:
                chosen = (pri, 0, runnable[pri][0][0], runnable[pri][0][1])
                break
        if chosen is None:
            break

        pri, ridx, p, op = chosen
        opid = _op_id(op)

        # Find best pool for this op
        best = None  # (score, pool_id, cpu_req, ram_req)
        for pool_id in range(s.executor.num_pools):
            pool = s.executor.pools[pool_id]
            avail_cpu, avail_ram = pool_avail[pool_id]
            if avail_cpu <= 0 or avail_ram <= 0:
                continue

            req = _fit_request(pool, avail_cpu, avail_ram, pri, opid, hp_runnable_exists)
            if req is None:
                continue
            cpu_req, ram_req = req

            score = _pool_score(pool, avail_cpu, avail_ram, cpu_req, ram_req, pri)
            if best is None or score > best[0]:
                best = (score, pool_id, cpu_req, ram_req)

        if best is None:
            # Cannot place this chosen op anywhere; remove it for this tick and try others
            runnable[pri].pop(0)
            continue

        _, pool_id, cpu_req, ram_req = best

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

        # Update local pool availability
        avail_cpu, avail_ram = pool_avail[pool_id]
        pool_avail[pool_id] = (avail_cpu - cpu_req, avail_ram - ram_req)

        # Remove this runnable item (we scheduled it)
        runnable[pri].pop(0)

        # Advance rr cursor for this priority for next tick (fairness within pri)
        # Cursor advances by 1 over the runnable list length; we keep it simple.
        s.rr[pri] = (s.rr.get(pri, 0) + 1) % 10_000_000

        made_any = True

    return suspensions, assignments
