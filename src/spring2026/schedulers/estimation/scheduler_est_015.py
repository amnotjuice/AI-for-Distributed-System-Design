# policy_key: scheduler_est_015
# reasoning_effort: low
# model: gpt-5.2-2025-12-11
# llm_cost: 0.043588
# generation_seconds: 42.08
# generated_at: 2026-03-31T23:54:50.169640
@register_scheduler_init(key="scheduler_est_015")
def scheduler_est_015_init(s):
    """
    Priority-aware, estimate-guided scheduler.

    Small, obvious improvements over naive FIFO:
      1) Use priority ordering (QUERY > INTERACTIVE > BATCH) for runnable ops.
      2) Right-size resource requests instead of handing the entire pool to one op.
      3) Use op.estimate.mem_peak_gb as a RAM floor (with a safety factor) to reduce OOMs.
      4) On failures, conservatively increase the remembered RAM floor for that op and retry.

    Notes:
      - No preemption yet (kept simple/robust).
      - Uses mild aging so batch work isn't starved under sustained interactive load.
    """
    # Queues hold pipelines; we schedule at most a few ops per pool per tick to improve packing.
    s.q_query = []
    s.q_interactive = []
    s.q_batch = []

    # Track when a pipeline was last enqueued (for aging).
    s.enqueue_tick = {}  # pipeline_id -> tick

    # Remember per-operator RAM floors learned from failures.
    # Keyed by (pipeline_id, op_key) where op_key is a best-effort stable id.
    s.op_ram_floor_gb = {}

    # Basic config knobs (kept intentionally simple).
    s.tick = 0
    s.max_assignments_per_pool = 4

    # RAM sizing
    s.ram_safety = 1.25          # apply on top of estimate/floor
    s.ram_growth_on_fail = 1.60  # multiply observed RAM allocation on OOM-like failures
    s.min_ram_gb = 0.25          # avoid allocating tiny RAM slices

    # Aging: extra score per tick waited (kept small; priority dominates)
    s.aging_per_tick = 0.002

    # Priority weights (dominant term)
    s.priority_weight = {
        Priority.QUERY: 3.0,
        Priority.INTERACTIVE: 2.0,
        Priority.BATCH_PIPELINE: 1.0,
    }


@register_scheduler(key="scheduler_est_015")
def scheduler_est_015_scheduler(s, results: list, pipelines: list):
    """
    Scheduler step:
      - Incorporate new pipelines into per-priority queues.
      - Learn from failures: increase per-op RAM floor to reduce repeated OOMs.
      - For each pool, repeatedly pick the best runnable op (priority + aging),
        then assign right-sized CPU/RAM to allow better concurrency/packing.
    """
    s.tick += 1

    def _op_key(op):
        # Best-effort stable operator id across simulator objects.
        # Falls back to id(op) if no stable field exists.
        return getattr(op, "operator_id", getattr(op, "op_id", id(op)))

    def _is_active_pipeline(p):
        st = p.runtime_status()
        if st.is_pipeline_successful():
            return False
        # Keep failed pipelines in the system (we may be retrying with larger RAM).
        # If the simulator marks some failures as terminal, they'd keep failing and eventually stall;
        # still better than dropping silently.
        return True

    def _enqueue_pipeline(p):
        pid = p.pipeline_id
        if pid not in s.enqueue_tick:
            s.enqueue_tick[pid] = s.tick
        if p.priority == Priority.QUERY:
            s.q_query.append(p)
        elif p.priority == Priority.INTERACTIVE:
            s.q_interactive.append(p)
        else:
            s.q_batch.append(p)

    def _requeue_pipeline(p):
        # Update enqueue tick so aging reflects time since last scheduling attempt.
        s.enqueue_tick[p.pipeline_id] = s.tick
        _enqueue_pipeline(p)

    def _learn_from_results():
        # If an op failed, grow RAM floor for that operator (esp. for OOM-like errors).
        for r in results:
            if not getattr(r, "failed", lambda: False)():
                continue
            pid = getattr(r, "pipeline_id", None)
            # Some simulators may not attach pipeline_id to result; fall back to None-safe keying.
            # We still try to key by op identity as best as possible.
            ops = getattr(r, "ops", []) or []
            err = str(getattr(r, "error", "") or "").lower()

            # Heuristic: treat any failure as a signal to increase RAM, but be more aggressive for OOM.
            is_oom_like = ("oom" in err) or ("out of memory" in err) or ("memory" in err and "alloc" in err)

            base_mult = s.ram_growth_on_fail if is_oom_like else 1.25
            observed_ram = float(getattr(r, "ram", 0.0) or 0.0)
            if observed_ram <= 0:
                # If we don't know what was allocated, bump minimally via estimate later.
                observed_ram = s.min_ram_gb

            for op in ops:
                opk = _op_key(op)
                key = (pid, opk)
                prev = float(s.op_ram_floor_gb.get(key, 0.0) or 0.0)
                new_floor = max(prev, observed_ram * base_mult)
                s.op_ram_floor_gb[key] = new_floor

    def _pipeline_first_runnable_op(p):
        st = p.runtime_status()
        # Only schedule ops whose parents are complete.
        ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        if not ops:
            return None
        return ops[0]

    def _score_pipeline(p):
        # Priority dominates; aging prevents indefinite starvation.
        base = float(s.priority_weight.get(p.priority, 1.0))
        waited = max(0, s.tick - int(s.enqueue_tick.get(p.pipeline_id, s.tick)))
        return base + s.aging_per_tick * waited

    def _pick_candidate_pipeline():
        # Scan heads of each queue (small/simple). Keep it robust by filtering inactive pipelines.
        # We don't strictly need FIFO within priority; we use score (priority + aging).
        best = None
        best_score = -1.0

        # Consider a limited number from each queue to keep overhead low.
        # If queues are small, this is effectively full scan.
        for q in (s.q_query, s.q_interactive, s.q_batch):
            # Clean inactive pipelines encountered at head, but don't aggressively prune whole queue.
            for p in q[: min(len(q), 16)]:
                if not _is_active_pipeline(p):
                    continue
                op = _pipeline_first_runnable_op(p)
                if op is None:
                    continue
                sc = _score_pipeline(p)
                if sc > best_score:
                    best = p
                    best_score = sc
        return best

    def _remove_one_from_queue(p):
        # Remove a single instance of pipeline p from its corresponding queue.
        q = s.q_query if p.priority == Priority.QUERY else (s.q_interactive if p.priority == Priority.INTERACTIVE else s.q_batch)
        for i in range(len(q)):
            if q[i].pipeline_id == p.pipeline_id:
                q.pop(i)
                return

    def _cpu_target(pool, priority, avail_cpu):
        # Right-size CPU to allow concurrency. Queries get more.
        max_cpu = float(getattr(pool, "max_cpu_pool", avail_cpu) or avail_cpu)
        if priority == Priority.QUERY:
            share = 0.50
            min_cpu = 2.0
        elif priority == Priority.INTERACTIVE:
            share = 0.35
            min_cpu = 1.0
        else:
            share = 0.25
            min_cpu = 1.0

        target = max(min_cpu, max_cpu * share)
        # Clamp to available and ensure at least 1 vCPU if possible.
        if avail_cpu < 1.0:
            return 0.0
        return max(1.0, min(float(avail_cpu), float(int(target)) if target >= 1.0 else 1.0))

    def _ram_target_gb(pool, pipeline, op, avail_ram):
        # Determine RAM floor from estimate and learned failures.
        est = getattr(getattr(op, "estimate", None), "mem_peak_gb", None)
        try:
            est = float(est) if est is not None else None
        except Exception:
            est = None

        pid = pipeline.pipeline_id
        opk = _op_key(op)
        learned_floor = float(s.op_ram_floor_gb.get((pid, opk), 0.0) or 0.0)

        floor_gb = max(s.min_ram_gb, learned_floor, est or 0.0)
        need = floor_gb * s.ram_safety

        # Don't take the whole pool; just allocate what we think we need (bounded by avail).
        if avail_ram <= 0:
            return 0.0
        return max(s.min_ram_gb, min(float(avail_ram), float(need)))

    # Ingest new pipelines
    for p in pipelines:
        _enqueue_pipeline(p)

    # Learn from results (failure-driven RAM floor adjustment)
    if results:
        _learn_from_results()

    # Early exit if nothing to do
    if not pipelines and not results and not (s.q_query or s.q_interactive or s.q_batch):
        return [], []

    suspensions = []
    assignments = []

    # Per-pool packing loop: assign a few ops per tick per pool to reduce queueing latency.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]

        assigns_this_pool = 0
        while assigns_this_pool < s.max_assignments_per_pool:
            avail_cpu = float(pool.avail_cpu_pool)
            avail_ram = float(pool.avail_ram_pool)
            if avail_cpu < 1.0 or avail_ram < s.min_ram_gb:
                break

            p = _pick_candidate_pipeline()
            if p is None:
                break

            # Remove it from queue now; if we can't schedule, we will requeue it.
            _remove_one_from_queue(p)

            if not _is_active_pipeline(p):
                continue

            op = _pipeline_first_runnable_op(p)
            if op is None:
                # Not runnable yet; requeue so it can be reconsidered after dependencies complete.
                _requeue_pipeline(p)
                continue

            cpu = _cpu_target(pool, p.priority, avail_cpu)
            ram = _ram_target_gb(pool, p, op, avail_ram)

            if cpu < 1.0 or ram < s.min_ram_gb:
                # Can't fit right now; requeue and move on.
                _requeue_pipeline(p)
                break

            assignment = Assignment(
                ops=[op],
                cpu=cpu,
                ram=ram,
                priority=p.priority,
                pool_id=pool_id,
                pipeline_id=p.pipeline_id,
            )
            assignments.append(assignment)
            assigns_this_pool += 1

            # After giving one op a slot, requeue pipeline so its next runnable op can contend fairly.
            _requeue_pipeline(p)

    return suspensions, assignments
