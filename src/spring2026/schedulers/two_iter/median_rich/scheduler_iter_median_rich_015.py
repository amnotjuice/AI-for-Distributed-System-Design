# policy_key: scheduler_iter_median_rich_015
# context: rich
# model: gpt-5.2-2025-12-11
# llm_cost: 0.053974
# generation_seconds: 54.35
# generated_at: 2026-04-12T01:55:26.536556
@register_scheduler_init(key="scheduler_iter_median_rich_015")
def scheduler_iter_median_rich_015_init(s):
    """Priority-aware scheduler with per-operator RAM learning to reduce OOM + latency.

    Incremental improvements over naive FIFO / simple priority FIFO:
    - Strict priority order: QUERY > INTERACTIVE > BATCH, but with a small batch allowance to avoid total starvation.
    - Per-operator RAM estimate table learned from OOM failures (and mildly from timeouts), so retries are less frequent.
    - Conservative initial RAM for high-priority ops to avoid OOM storms; batch starts smaller but adapts quickly on OOM.
    - Pool selection: place high-priority work into the pool with the most headroom to reduce queueing.
    - Keeps scheduling atom at 1 ready op per pipeline to reduce head-of-line blocking inside a pipeline.
    """
    # Queues per priority (round-robin within each)
    s.queues = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }
    s.rr = {
        Priority.QUERY: 0,
        Priority.INTERACTIVE: 0,
        Priority.BATCH_PIPELINE: 0,
    }

    # Per-pipeline bookkeeping (minimal)
    s.pipeline_drop = {}  # pipeline_id -> bool (drop if non-OOM failure persists)

    # Learned per-operator resource hints (keyed by a stable-ish operator identity string)
    # Values are "target allocations" (not minima). We only learn on failure signals we can see.
    s.op_ram_target = {}  # op_key -> float (GiB-ish units consistent with pool.ram)
    s.op_cpu_target = {}  # op_key -> float (vCPU units consistent with pool.cpu)

    # Soft fairness knobs
    s.batch_quota_per_tick = 2  # allow at least N batch assignments per scheduler call (if resources exist)
    s._batch_assigned_this_tick = 0


@register_scheduler(key="scheduler_iter_median_rich_015")
def scheduler_iter_median_rich_015_scheduler(s, results, pipelines):
    """
    Step algorithm:
    1) Enqueue new pipelines.
    2) Update learned RAM/CPU targets from execution results (OOM => bump RAM; timeout => bump CPU a bit).
    3) Schedule ops:
       - Prefer QUERY/INTERACTIVE first, across pools with most headroom.
       - Batch is scheduled after, but allowed a small quota each tick to keep progress.
       - Allocate RAM using learned targets (or sensible defaults) and clamp to pool availability.
    """
    # -----------------------
    # Helpers (no imports)
    # -----------------------
    def _is_oom_error(err):
        if err is None:
            return False
        msg = str(err).lower()
        return ("oom" in msg) or ("out of memory" in msg)

    def _is_timeout_error(err):
        if err is None:
            return False
        msg = str(err).lower()
        return ("timeout" in msg) or ("timed out" in msg) or ("deadline" in msg)

    def _op_key(op):
        # Try common stable identifiers first; fall back to repr (works in simulator contexts).
        for attr in ("op_id", "operator_id", "id", "name"):
            if hasattr(op, attr):
                try:
                    v = getattr(op, attr)
                    if v is not None:
                        return f"{attr}:{v}"
                except Exception:
                    pass
        try:
            return repr(op)
        except Exception:
            return str(type(op))

    def _enqueue(p):
        s.queues[p.priority].append(p)
        if p.pipeline_id not in s.pipeline_drop:
            s.pipeline_drop[p.pipeline_id] = False

    def _prio_list():
        return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]

    def _pool_headroom(pool):
        # Simple scalar ranking for choosing "best" pool for latency-sensitive work.
        return (pool.avail_cpu_pool / max(1e-9, pool.max_cpu_pool)) + (pool.avail_ram_pool / max(1e-9, pool.max_ram_pool))

    def _pop_rr(pri):
        q = s.queues[pri]
        if not q:
            return None

        n = len(q)
        start = s.rr[pri] % n
        for k in range(n):
            idx = (start + k) % n
            p = q[idx]
            st = p.runtime_status()

            # Drop completed pipelines
            if st.is_pipeline_successful():
                q.pop(idx)
                if not q:
                    s.rr[pri] = 0
                else:
                    s.rr[pri] = idx % len(q)
                return None  # force caller to retry selection (queue mutated)

            # Drop pipelines marked as "non-retriable failures" (best-effort)
            if s.pipeline_drop.get(p.pipeline_id, False):
                if st.state_counts.get(OperatorState.FAILED, 0) > 0:
                    q.pop(idx)
                    if not q:
                        s.rr[pri] = 0
                    else:
                        s.rr[pri] = idx % len(q)
                    return None
                # If it's not currently failed, keep it (could be partially done)

            # Rotate: take it out and re-append (RR fairness)
            q.pop(idx)
            s.rr[pri] = idx % max(1, len(q)) if q else 0
            return p

        return None

    def _default_targets(pri, pool):
        # Defaults biased to reduce OOMs for latency-sensitive work.
        if pri == Priority.QUERY:
            return max(1.0, pool.max_cpu_pool * 0.25), max(1.0, pool.max_ram_pool * 0.18)
        if pri == Priority.INTERACTIVE:
            return max(1.0, pool.max_cpu_pool * 0.35), max(1.0, pool.max_ram_pool * 0.26)
        # Batch: smaller to allow concurrency, but will ramp quickly on OOM via learning.
        return max(1.0, pool.max_cpu_pool * 0.45), max(1.0, pool.max_ram_pool * 0.18)

    def _choose_alloc(pri, pool, op):
        opk = _op_key(op)
        cpu_def, ram_def = _default_targets(pri, pool)
        cpu_t = s.op_cpu_target.get(opk, cpu_def)
        ram_t = s.op_ram_target.get(opk, ram_def)

        # Clamp to available and pool maxima; keep a small safety margin for other work.
        cpu = min(max(1.0, cpu_t), pool.avail_cpu_pool, pool.max_cpu_pool)
        ram = min(max(1.0, ram_t), pool.avail_ram_pool, pool.max_ram_pool)

        # If we're allocating almost all memory, leave a tiny sliver to prevent dead-ends
        # where nothing else can be scheduled (helps interactive tail).
        if ram > pool.avail_ram_pool * 0.98 and pool.avail_ram_pool > 1.0:
            ram = max(1.0, pool.avail_ram_pool * 0.98)

        return cpu, ram

    def _assign_one(pool_id, p, pri):
        pool = s.executor.pools[pool_id]
        if pool.avail_cpu_pool <= 0 or pool.avail_ram_pool <= 0:
            return None

        st = p.runtime_status()
        # If pipeline has a FAILED op, we still allow retry (FAILED is in ASSIGNABLE_STATES),
        # unless we've decided it is non-OOM failure (pipeline_drop=True).
        if st.is_pipeline_successful():
            return None
        if s.pipeline_drop.get(p.pipeline_id, False) and st.state_counts.get(OperatorState.FAILED, 0) > 0:
            return None

        ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
        if not ops:
            return None

        cpu, ram = _choose_alloc(pri, pool, ops[0])
        if cpu <= 0 or ram <= 0:
            return None

        return Assignment(
            ops=ops,
            cpu=cpu,
            ram=ram,
            priority=p.priority,
            pool_id=pool_id,
            pipeline_id=p.pipeline_id,
        )

    # -----------------------
    # 1) Enqueue new arrivals
    # -----------------------
    for p in pipelines:
        _enqueue(p)

    if not pipelines and not results:
        return [], []

    # Reset per-tick batch quota usage
    s._batch_assigned_this_tick = 0

    # -----------------------
    # 2) Learn from results
    # -----------------------
    for r in results:
        if not (hasattr(r, "failed") and r.failed()):
            continue

        err = getattr(r, "error", None)
        is_oom = _is_oom_error(err)
        is_to = _is_timeout_error(err)
        ops = getattr(r, "ops", None) or []

        # If we don't have ops, nothing to learn per-operator.
        for op in ops:
            opk = _op_key(op)

            # OOM: increase RAM target aggressively for that operator.
            if is_oom:
                prev = s.op_ram_target.get(opk, max(1.0, getattr(r, "ram", 1.0)))
                # Use last allocation as a floor; then bump.
                base = max(prev, max(1.0, float(getattr(r, "ram", 1.0))))
                s.op_ram_target[opk] = min(base * 2.0, 1e12)

            # Timeout: mildly increase CPU target for that operator (avoid runaway).
            if is_to:
                prevc = s.op_cpu_target.get(opk, max(1.0, getattr(r, "cpu", 1.0)))
                basec = max(prevc, max(1.0, float(getattr(r, "cpu", 1.0))))
                s.op_cpu_target[opk] = min(basec * 1.25, 1e6)

        # If it's a failed op but not OOM/timeout, we conservatively avoid infinite retry loops
        # by marking pipelines with FAILED state as droppable (best-effort; we can't map result->pipeline directly).
        if (not is_oom) and (not is_to):
            # Mark-by-priority: if a pipeline is currently in FAILED state, drop it on next scheduling pass.
            for pri in _prio_list():
                for p in s.queues[pri]:
                    st = p.runtime_status()
                    if st.state_counts.get(OperatorState.FAILED, 0) > 0:
                        s.pipeline_drop[p.pipeline_id] = True

    # -----------------------
    # 3) Scheduling
    # -----------------------
    suspensions = []
    assignments = []

    # Create a pool ordering for latency-sensitive work (recomputed each tick)
    pool_ids = list(range(s.executor.num_pools))
    pool_ids_sorted = sorted(pool_ids, key=lambda i: _pool_headroom(s.executor.pools[i]), reverse=True)

    # Phase A: schedule QUERY and INTERACTIVE first, always picking best-headroom pool for each attempt.
    for pri in (Priority.QUERY, Priority.INTERACTIVE):
        # Try multiple passes; stop when we fail to make progress across all pools.
        made_progress = True
        while made_progress:
            made_progress = False

            p = _pop_rr(pri)
            if p is None:
                break  # queue empty or mutated; next outer iteration will retry

            # Try pools in best-first order for this latency-sensitive pipeline.
            assigned = None
            for pool_id in pool_ids_sorted:
                a = _assign_one(pool_id, p, pri)
                if a is not None:
                    assigned = a
                    break

            if assigned is not None:
                assignments.append(assigned)
                made_progress = True

            # Requeue pipeline for RR fairness (whether assigned or not); its next op may become ready later.
            s.queues[pri].append(p)

    # Phase B: allow a small batch quota each tick even if interactive exists (prevents starvation),
    # then continue scheduling batch normally.
    batch_passes = 0
    while True:
        if not s.queues[Priority.BATCH_PIPELINE]:
            break

        # If we've satisfied the quota and there is any higher-priority backlog, we still schedule batch,
        # but only if pools have headroom (the assignment logic naturally enforces this).
        p = _pop_rr(Priority.BATCH_PIPELINE)
        if p is None:
            # queue mutated or empty; stop if empty, else continue
            if not s.queues[Priority.BATCH_PIPELINE]:
                break
            continue

        assigned = None
        # For batch, we try the most available pool first as well; improves throughput and reduces timeouts.
        for pool_id in pool_ids_sorted:
            a = _assign_one(pool_id, p, Priority.BATCH_PIPELINE)
            if a is not None:
                assigned = a
                break

        if assigned is not None:
            assignments.append(assigned)
            s._batch_assigned_this_tick += 1

        s.queues[Priority.BATCH_PIPELINE].append(p)

        batch_passes += 1
        # Bound work per tick to avoid quadratic behavior with huge queues.
        # Also enforce a minimal "quota attempt": if we can't even place batch a few times, stop early.
        if batch_passes >= 256:
            break

        # If we have not assigned anything in a while, stop (prevents infinite loops when pools are full).
        if assigned is None and batch_passes >= max(16, s.batch_quota_per_tick * 4):
            break

    return suspensions, assignments
