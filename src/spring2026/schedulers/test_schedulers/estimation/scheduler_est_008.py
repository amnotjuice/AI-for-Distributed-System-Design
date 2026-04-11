# policy_key: scheduler_est_008
# reasoning_effort: low
# exp: estimation
# model: gpt-5.2-2025-12-11
# llm_cost: 0.048524
# generation_seconds: 50.79
# generated_at: 2026-04-10T07:52:46.280019
@register_scheduler_init(key="scheduler_est_008")
def scheduler_est_008_init(s):
    """Priority- and memory-aware scheduler with conservative OOM avoidance.

    Core ideas:
      - Maintain per-priority FIFO queues with mild aging for fairness.
      - Use op.estimate.mem_peak_gb (when present) to choose a feasible pool and right-size RAM.
      - On failures (likely OOM), increase a per-pipeline RAM multiplier to reduce repeated failures.
      - Avoid preemption to minimize churn; instead, prioritize admissions by class and fit.
    """
    # Queues store pipeline_ids; actual pipeline objects are tracked in a dict for stable references.
    s._p_by_id = {}  # pipeline_id -> Pipeline
    s._prio_queues = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }
    s._arrived_tick = {}  # pipeline_id -> tick when first seen
    s._tick = 0

    # Per-pipeline memory multiplier that increases after failures; used to inflate estimated RAM.
    s._ram_mult = {}  # pipeline_id -> float

    # Light de-dup: pipeline_id currently enqueued?
    s._in_queue = set()

    # Tunables (kept simple; adjust in future iterations based on sim results)
    s._min_ram_gb_floor = 0.25  # never ask for less than this (if units are GB)
    s._default_ram_frac_no_est = 0.50  # if no estimate, request this fraction of chosen pool RAM
    s._max_ram_mult = 8.0
    s._ram_mult_on_fail = 1.7
    s._ram_mult_decay_on_success = 0.98  # slowly decay multipliers over time as pipelines succeed
    s._aging_boost_batch = 0.002  # per tick; helps prevent starvation
    s._aging_boost_interactive = 0.0008
    s._aging_boost_query = 0.0  # queries already dominate

    # CPU sizing caps by priority to avoid letting one op monopolize a pool
    s._cpu_cap = {
        Priority.QUERY: 6.0,
        Priority.INTERACTIVE: 4.0,
        Priority.BATCH_PIPELINE: 2.0,
    }
    s._cpu_min = {
        Priority.QUERY: 1.0,
        Priority.INTERACTIVE: 1.0,
        Priority.BATCH_PIPELINE: 1.0,
    }


@register_scheduler(key="scheduler_est_008")
def scheduler_est_008_scheduler(s, results, pipelines):
    """
    Scheduling step:
      1) Ingest new pipelines into per-priority queues.
      2) Update memory multipliers based on execution failures.
      3) For each pool, pick the best next runnable operator across priorities using:
           - priority order (query > interactive > batch) with mild aging,
           - feasibility filtering by estimated memory,
           - best-fit placement (min RAM slack) to reduce fragmentation.
      4) Issue one assignment per pool per tick (simple, stable baseline improvement).
    """
    s._tick += 1

    def _safe_get_est_mem_gb(op):
        est = getattr(op, "estimate", None)
        if est is None:
            return None
        v = getattr(est, "mem_peak_gb", None)
        try:
            if v is None:
                return None
            v = float(v)
            if v < 0:
                return None
            return v
        except Exception:
            return None

    def _pipeline_done_or_failed(p):
        st = p.runtime_status()
        # Treat any failure as terminal for this scheduler (avoid infinite retries; simulator penalizes failures anyway).
        # We will still keep it out of the queue once failed.
        has_failures = st.state_counts[OperatorState.FAILED] > 0
        return st.is_pipeline_successful() or has_failures

    def _enqueue_pipeline(p):
        pid = p.pipeline_id
        s._p_by_id[pid] = p
        if pid not in s._arrived_tick:
            s._arrived_tick[pid] = s._tick
        if pid not in s._ram_mult:
            s._ram_mult[pid] = 1.0
        if pid in s._in_queue:
            return
        s._prio_queues[p.priority].append(pid)
        s._in_queue.add(pid)

    def _dequeue_pipeline_id(pid, prio):
        # Remove from front when popped; ensure _in_queue consistency
        s._in_queue.discard(pid)

    def _score_pipeline_for_pick(p):
        # Higher score picked first.
        # Priority dominates; aging gives batch a gentle nudge so it makes progress.
        pid = p.pipeline_id
        age = max(0, s._tick - s._arrived_tick.get(pid, s._tick))
        if p.priority == Priority.QUERY:
            return 1000.0 + age * s._aging_boost_query
        if p.priority == Priority.INTERACTIVE:
            return 500.0 + age * s._aging_boost_interactive
        return 0.0 + age * s._aging_boost_batch

    def _get_next_assignable_op(p):
        st = p.runtime_status()
        # Only operators whose parents are complete.
        ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        if not ops:
            return None
        return ops[0]  # FIFO within a pipeline

    def _choose_pool_for_op(op, priority):
        # Return (pool_id, ram_req, cpu_req) or None if cannot place.
        best = None  # (slack_ram, pool_id, ram_req, cpu_req)
        est_mem = _safe_get_est_mem_gb(op)

        for pool_id in range(s.executor.num_pools):
            pool = s.executor.pools[pool_id]
            avail_cpu = float(pool.avail_cpu_pool)
            avail_ram = float(pool.avail_ram_pool)
            if avail_cpu <= 0 or avail_ram <= 0:
                continue

            # CPU request: cap by priority, but not more than available.
            cpu_cap = float(s._cpu_cap.get(priority, 2.0))
            cpu_min = float(s._cpu_min.get(priority, 1.0))
            cpu_req = min(avail_cpu, cpu_cap)
            if cpu_req < cpu_min:
                continue

            # RAM request: use estimate if present, otherwise conservative fraction of pool RAM.
            if est_mem is not None:
                ram_req = est_mem
            else:
                # No estimate: conservative but not maximal; prevents starving others.
                ram_req = max(s._min_ram_gb_floor, float(pool.max_ram_pool) * float(s._default_ram_frac_no_est))

            ram_req = max(s._min_ram_gb_floor, ram_req)

            # Apply per-pipeline multiplier at call site; here we only check feasibility.
            # But we still allow placement if it fits in avail_ram (final check after multiplier).
            if ram_req > float(pool.max_ram_pool):
                # If even the base estimate doesn't fit max pool, skip this pool.
                continue

            # Prefer best-fit among feasible pools (min slack).
            # We'll re-check with multiplier in the caller.
            slack = avail_ram - ram_req
            if slack < 0:
                continue

            # Secondary preference: more available CPU (faster) while keeping fit tight.
            # Encode by slightly reducing slack for higher CPU.
            adjusted = slack - 0.01 * avail_cpu
            cand = (adjusted, pool_id, ram_req, cpu_req)
            if best is None or cand[0] < best[0]:
                best = cand

        if best is None:
            return None
        _, pool_id, ram_req, cpu_req = best
        return pool_id, ram_req, cpu_req

    # Ingest new pipelines
    for p in pipelines:
        _enqueue_pipeline(p)

    # Update multipliers based on results (inflate on failure; gently decay on success)
    for r in results:
        try:
            # Decay multiplier for the pipeline (success signal) when ops complete successfully.
            # We don't have explicit "success" flags per op beyond failed(), so treat non-failed results as success.
            pid = getattr(r, "pipeline_id", None)
        except Exception:
            pid = None

        if pid is None:
            # If pipeline_id isn't present on results in this simulator build, skip decay.
            pid = None

        if r.failed():
            # Inflate based on the pipeline(s) impacted. Result contains .ops, but not necessarily pipeline_id.
            # We conservatively inflate for all pipelines currently queued at the same priority class.
            # If pipeline_id exists, only inflate that one.
            if pid is not None and pid in s._ram_mult:
                s._ram_mult[pid] = min(s._max_ram_mult, s._ram_mult.get(pid, 1.0) * s._ram_mult_on_fail)
            else:
                # Fallback: inflate multiplier for pipelines that share the result priority (coarse signal).
                q = s._prio_queues.get(r.priority, [])
                for pid2 in q[: min(len(q), 3)]:  # only a few to avoid runaway inflation
                    s._ram_mult[pid2] = min(s._max_ram_mult, s._ram_mult.get(pid2, 1.0) * 1.15)
        else:
            # Gentle global decay (bounded)
            for pid2 in list(s._ram_mult.keys())[:200]:
                s._ram_mult[pid2] = max(1.0, s._ram_mult.get(pid2, 1.0) * s._ram_mult_decay_on_success)

    # Early exit: no new pipelines and no results => no state change we can exploit
    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    # Build candidate list per priority each tick (bounded scan to keep runtime stable)
    def _gather_candidates(max_scan_per_prio=64):
        cands = []  # list of (score, pid, pipeline, op)
        for prio, q in s._prio_queues.items():
            scanned = 0
            # Scan the queue without permanently reordering it; we'll pop when assigning.
            for pid in q:
                if scanned >= max_scan_per_prio:
                    break
                p = s._p_by_id.get(pid)
                if p is None:
                    scanned += 1
                    continue
                if _pipeline_done_or_failed(p):
                    scanned += 1
                    continue
                op = _get_next_assignable_op(p)
                if op is None:
                    scanned += 1
                    continue
                cands.append((_score_pipeline_for_pick(p), pid, p, op))
                scanned += 1
        # Highest score first
        cands.sort(key=lambda x: x[0], reverse=True)
        return cands

    # Helper to remove a pipeline_id from its queue (linear, but queues are typically small; bounded by usage)
    def _remove_from_queue(pid, prio):
        q = s._prio_queues[prio]
        for i in range(len(q)):
            if q[i] == pid:
                q.pop(i)
                break
        _dequeue_pipeline_id(pid, prio)

    # Try to issue at most one assignment per pool per tick (stable baseline that improves FIFO)
    used_pools = set()
    candidates = _gather_candidates()

    # If nothing is runnable, keep queues but drop finished/failed ones to avoid buildup.
    if not candidates:
        for prio, q in s._prio_queues.items():
            new_q = []
            for pid in q:
                p = s._p_by_id.get(pid)
                if p is None:
                    s._in_queue.discard(pid)
                    continue
                if _pipeline_done_or_failed(p):
                    s._in_queue.discard(pid)
                    continue
                new_q.append(pid)
            s._prio_queues[prio] = new_q
        return suspensions, assignments

    # Greedy placement: iterate candidates in priority/aging order; place if any pool can fit.
    for score, pid, p, op in candidates:
        # Skip if already scheduled this tick (may happen if duplicates in candidates due to stale state)
        if pid not in s._in_queue:
            continue
        if _pipeline_done_or_failed(p):
            _remove_from_queue(pid, p.priority)
            continue

        pick = _choose_pool_for_op(op, p.priority)
        if pick is None:
            # Can't place anywhere now; keep queued.
            continue

        pool_id, base_ram_req, cpu_req = pick
        if pool_id in used_pools:
            continue

        pool = s.executor.pools[pool_id]
        avail_ram = float(pool.avail_ram_pool)
        avail_cpu = float(pool.avail_cpu_pool)

        mult = float(s._ram_mult.get(pid, 1.0))
        ram_req = min(float(pool.max_ram_pool), max(s._min_ram_gb_floor, base_ram_req * mult))

        # Final feasibility check with multiplier applied
        if ram_req > avail_ram or cpu_req > avail_cpu:
            continue

        # Make the assignment (single op to reduce risk of mixing memory spikes)
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
        used_pools.add(pool_id)

        # Keep pipeline in queue for subsequent operators, but move it to the back (round-robin within priority)
        _remove_from_queue(pid, p.priority)
        s._prio_queues[p.priority].append(pid)
        s._in_queue.add(pid)

        # If we've filled all pools, stop
        if len(used_pools) >= s.executor.num_pools:
            break

    # Cleanup: drop completed/failed pipelines from queues
    for prio, q in s._prio_queues.items():
        new_q = []
        for pid in q:
            p = s._p_by_id.get(pid)
            if p is None:
                s._in_queue.discard(pid)
                continue
            if _pipeline_done_or_failed(p):
                s._in_queue.discard(pid)
                continue
            new_q.append(pid)
        s._prio_queues[prio] = new_q

    return suspensions, assignments
