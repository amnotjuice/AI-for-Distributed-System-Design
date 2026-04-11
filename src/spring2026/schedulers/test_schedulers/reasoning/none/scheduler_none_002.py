# policy_key: scheduler_none_002
# reasoning_effort: none
# model: gpt-5.2-2025-12-11
# llm_cost: 0.055860
# generation_seconds: 43.31
# generated_at: 2026-04-09T21:46:26.824459
@register_scheduler_init(key="scheduler_none_002")
def scheduler_none_002_init(s):
    """
    Priority-aware, failure-averse scheduler.

    Main ideas:
      - Keep separate per-priority queues; always try to schedule QUERY then INTERACTIVE then BATCH.
      - Use conservative "base" CPU/RAM sizing per operator, with exponential backoff on failures (e.g., OOM).
      - Avoid wasting work: preempt only when a high-priority pipeline is blocked and we need headroom.
      - Ensure progress for batch via light aging (periodically let one batch op through if queues persist).
    """
    # Waiting pipelines per priority
    s.q_query = []
    s.q_interactive = []
    s.q_batch = []

    # Per-(pipeline_id, op_id) resource hints learned from failures/successes
    # key -> {"ram": float, "cpu": float, "fails": int}
    s.op_hints = {}

    # Track which pipeline IDs we've seen for arrival accounting (optional; used only for internal heuristics)
    s.seen_pipelines = set()

    # Simple anti-starvation knob: after N high-priority assignments in a row, allow one batch if available
    s.hp_streak = 0
    s.hp_streak_limit = 8

    # Preemption guardrails
    s.max_preemptions_per_tick = 2

    # Clamp factors
    s.ram_backoff = 2.0
    s.cpu_backoff = 1.5
    s.max_hint_factor = 8.0  # avoid runaway allocations


@register_scheduler(key="scheduler_none_002")
def scheduler_none_002_scheduler(s, results, pipelines):
    """
    Scheduler step.

    Admission:
      - enqueue arriving pipelines into per-priority queues.

    Resizing:
      - on failure, increase RAM first (OOM-likely), then CPU modestly to reduce latency.
      - on success, keep hints (do not aggressively shrink to avoid oscillation).

    Placement:
      - prefer pools with the most available RAM for safety, then CPU for speed.

    Preemption:
      - if a QUERY/INTERACTIVE op is ready but cannot fit anywhere, suspend a low-priority running container
        (best-effort, limited per tick) to create headroom.
    """
    # Local helpers defined inside to avoid imports / external dependencies
    def _prio_rank(prio):
        if prio == Priority.QUERY:
            return 0
        if prio == Priority.INTERACTIVE:
            return 1
        return 2  # batch

    def _enqueue_pipeline(p):
        if p.priority == Priority.QUERY:
            s.q_query.append(p)
        elif p.priority == Priority.INTERACTIVE:
            s.q_interactive.append(p)
        else:
            s.q_batch.append(p)

    def _pipeline_done_or_dead(p):
        st = p.runtime_status()
        if st.is_pipeline_successful():
            return True
        # If any operator FAILED, we will retry it (FAILED is in ASSIGNABLE_STATES).
        # We do not drop pipelines here; objective heavily penalizes incompletes.
        return False

    def _get_ready_ops(p, limit=1):
        st = p.runtime_status()
        if st.is_pipeline_successful():
            return []
        # Prefer assignable ops whose parents are complete (critical path progress)
        ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        if not ops:
            return []
        return ops[:limit]

    def _op_key(pipeline_id, op):
        # Operator object shape is simulator-defined; use the safest stable identifier available.
        # Fall back to python id() to keep the map functional.
        op_id = getattr(op, "op_id", None)
        if op_id is None:
            op_id = getattr(op, "operator_id", None)
        if op_id is None:
            op_id = getattr(op, "name", None)
        if op_id is None:
            op_id = id(op)
        return (pipeline_id, op_id)

    def _clamp(v, lo, hi):
        if v < lo:
            return lo
        if v > hi:
            return hi
        return v

    def _pool_headroom_score(pool):
        # Higher is better; prioritize RAM headroom to reduce OOM risk
        # then CPU headroom to reduce latency.
        return (pool.avail_ram_pool, pool.avail_cpu_pool)

    def _base_alloc_for_pool(pool, prio):
        # Base sizing: keep query/interactive snappy by giving a decent fraction of a pool,
        # but avoid monopolizing to keep concurrency and completion rate high.
        # Always keep >=1 CPU and some RAM if available.
        if prio == Priority.QUERY:
            cpu_frac = 0.75
            ram_frac = 0.75
        elif prio == Priority.INTERACTIVE:
            cpu_frac = 0.60
            ram_frac = 0.65
        else:
            cpu_frac = 0.40
            ram_frac = 0.50

        cpu = max(1.0, pool.max_cpu_pool * cpu_frac)
        ram = max(1.0, pool.max_ram_pool * ram_frac)

        # Never allocate more than what's currently available in pool
        cpu = min(cpu, pool.avail_cpu_pool)
        ram = min(ram, pool.avail_ram_pool)
        return cpu, ram

    def _hinted_alloc(pool, pipeline, op):
        # Start from base alloc; apply learned hints from failures.
        base_cpu, base_ram = _base_alloc_for_pool(pool, pipeline.priority)
        k = _op_key(pipeline.pipeline_id, op)
        hint = s.op_hints.get(k)

        if not hint:
            return base_cpu, base_ram

        # Use max(base, hint), but clamp to available. Hints store absolute requested sizes.
        cpu = max(base_cpu, hint.get("cpu", 0.0))
        ram = max(base_ram, hint.get("ram", 0.0))
        cpu = min(cpu, pool.avail_cpu_pool)
        ram = min(ram, pool.avail_ram_pool)
        return cpu, ram

    def _update_hints_from_result(res):
        # Learn only from failed results to avoid oscillation; keep success hints stable.
        if not res.ops:
            return

        # Infer pipeline_id: ExecutionResult may not contain it; ops belong to a pipeline but not always linked.
        # We try to fetch pipeline_id from the op if present; otherwise skip hinting.
        # This keeps code robust across simulator versions.
        for op in res.ops:
            pipeline_id = getattr(op, "pipeline_id", None)
            if pipeline_id is None:
                pipeline_id = getattr(res, "pipeline_id", None)
            if pipeline_id is None:
                # Cannot map reliably
                continue

            k = _op_key(pipeline_id, op)
            cur = s.op_hints.get(k, {"ram": 0.0, "cpu": 0.0, "fails": 0})
            if res.failed():
                cur["fails"] = cur.get("fails", 0) + 1

                # Backoff strategy:
                # - If error indicates OOM, grow RAM aggressively; otherwise moderate RAM growth.
                # - Always grow CPU modestly for high-priority to reduce latency, but keep bounded.
                err = getattr(res, "error", None)
                err_s = str(err).lower() if err is not None else ""
                oom_like = ("oom" in err_s) or ("out of memory" in err_s) or ("memory" in err_s)

                # Use last attempted as the baseline for backoff
                last_ram = float(getattr(res, "ram", 0.0) or 0.0)
                last_cpu = float(getattr(res, "cpu", 0.0) or 0.0)
                if last_ram <= 0:
                    last_ram = cur.get("ram", 0.0)
                if last_cpu <= 0:
                    last_cpu = cur.get("cpu", 0.0)

                ram_mult = s.ram_backoff if oom_like else 1.4
                cpu_mult = s.cpu_backoff

                # Increase and clamp by max_hint_factor times pool max isn't accessible here;
                # clamp relative to last values.
                new_ram = last_ram * ram_mult if last_ram > 0 else cur.get("ram", 0.0) * ram_mult
                new_cpu = last_cpu * cpu_mult if last_cpu > 0 else cur.get("cpu", 0.0) * cpu_mult

                # Ensure monotonic non-decrease
                cur["ram"] = max(cur.get("ram", 0.0), new_ram)
                cur["cpu"] = max(cur.get("cpu", 0.0), new_cpu)

                # Hard clamp to avoid runaway; interpret as "no more than factor of 8 over the last attempt"
                if last_ram > 0:
                    cur["ram"] = min(cur["ram"], last_ram * s.max_hint_factor)
                if last_cpu > 0:
                    cur["cpu"] = min(cur["cpu"], last_cpu * s.max_hint_factor)

            s.op_hints[k] = cur

    def _iter_queues_in_service_order():
        # Mostly strict priority, with light batch aging via hp_streak.
        # If we've served many high-priority in a row, allow one batch if present.
        if (s.hp_streak >= s.hp_streak_limit) and s.q_batch:
            # Provide an opening for batch, but keep query first if present (never delay query).
            if s.q_query:
                return [s.q_query, s.q_interactive, s.q_batch]
            # If no query, allow batch ahead of interactive occasionally
            return [s.q_batch, s.q_interactive]

        return [s.q_query, s.q_interactive, s.q_batch]

    def _pick_pool_for_request(cpu_need, ram_need):
        # Choose pool where it fits, preferring more headroom.
        best = None
        best_score = None
        for pid in range(s.executor.num_pools):
            pool = s.executor.pools[pid]
            if pool.avail_cpu_pool >= cpu_need and pool.avail_ram_pool >= ram_need:
                score = _pool_headroom_score(pool)
                if best is None or score > best_score:
                    best = pid
                    best_score = score
        return best

    def _choose_preemption_candidate(target_prio):
        # Try to suspend a lower-priority running container to free resources.
        # We can't see full container runtime cost; just pick "lowest priority available" best-effort.
        # If executor doesn't expose running containers list, return None gracefully.
        containers = getattr(s.executor, "containers", None)
        if containers is None:
            containers = getattr(s.executor, "running_containers", None)
        if containers is None:
            return None

        cand = None
        cand_rank = None
        for c in containers:
            c_prio = getattr(c, "priority", None)
            c_pool = getattr(c, "pool_id", None)
            c_id = getattr(c, "container_id", None)
            c_state = getattr(c, "state", None)
            # Only consider running/assigned containers; skip unknown states
            if c_id is None or c_pool is None or c_prio is None:
                continue
            # If state exists, only preempt if running/assigned
            if c_state is not None:
                st_s = str(c_state).lower()
                if ("running" not in st_s) and ("assigned" not in st_s):
                    continue

            if _prio_rank(c_prio) <= _prio_rank(target_prio):
                continue  # don't preempt same/higher priority

            r = _prio_rank(c_prio)
            if cand is None or r > cand_rank:
                # prefer preempting lowest priority (largest rank)
                cand = (c_id, c_pool)
                cand_rank = r

        return cand

    # Enqueue new pipelines
    for p in pipelines:
        s.seen_pipelines.add(p.pipeline_id)
        _enqueue_pipeline(p)

    # Process results to update hints (especially on failures)
    for res in results:
        _update_hints_from_result(res)

    # Early exit if nothing changed
    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    # First attempt: schedule as many ready ops as possible, pool by pool.
    # We do a global loop to place work onto the best-fitting pool (not strict per-pool FIFO).
    made_progress = True
    while made_progress:
        made_progress = False

        # Clean out completed pipelines from queue fronts (keep order stable)
        for q in (s.q_query, s.q_interactive, s.q_batch):
            i = 0
            while i < len(q):
                if _pipeline_done_or_dead(q[i]):
                    q.pop(i)
                else:
                    i += 1

        # Find next schedulable op among queues in service order
        chosen = None  # (pipeline, op)
        for q in _iter_queues_in_service_order():
            # Scan a few pipelines to find a ready op (avoid head-of-line blocking)
            scan_limit = min(8, len(q))
            found_idx = None
            found_op = None
            for idx in range(scan_limit):
                p = q[idx]
                ops = _get_ready_ops(p, limit=1)
                if ops:
                    found_idx = idx
                    found_op = ops[0]
                    break
            if found_idx is not None:
                chosen = (q, found_idx, q[found_idx], found_op)
                break

        if chosen is None:
            break  # nothing ready anywhere

        qref, idx, pipeline, op = chosen

        # Determine a minimal request by looking across pools: compute hinted alloc per pool and try to fit.
        # We try to fit with the smallest reasonable alloc that still respects hints by using the "best pool" path:
        # pick pool that would give the largest hinted alloc but still fit; however we can't allocate > avail.
        # So: pick a pool by headroom first, then allocate within it.
        # Try pools in descending headroom.
        pool_order = list(range(s.executor.num_pools))
        pool_order.sort(key=lambda pid: _pool_headroom_score(s.executor.pools[pid]), reverse=True)

        placed = False
        for pid in pool_order:
            pool = s.executor.pools[pid]
            if pool.avail_cpu_pool <= 0 or pool.avail_ram_pool <= 0:
                continue

            cpu_req, ram_req = _hinted_alloc(pool, pipeline, op)
            # Ensure we request something meaningful
            cpu_req = max(1.0, cpu_req) if pool.avail_cpu_pool >= 1.0 else pool.avail_cpu_pool
            ram_req = max(1.0, ram_req) if pool.avail_ram_pool >= 1.0 else pool.avail_ram_pool

            if cpu_req <= 0 or ram_req <= 0:
                continue
            if pool.avail_cpu_pool < cpu_req or pool.avail_ram_pool < ram_req:
                continue

            assignments.append(
                Assignment(
                    ops=[op],
                    cpu=cpu_req,
                    ram=ram_req,
                    priority=pipeline.priority,
                    pool_id=pid,
                    pipeline_id=pipeline.pipeline_id,
                )
            )
            placed = True
            made_progress = True

            # Update streak accounting
            if pipeline.priority in (Priority.QUERY, Priority.INTERACTIVE):
                s.hp_streak += 1
            else:
                s.hp_streak = 0

            # Move the pipeline to the back of its queue to improve fairness within same priority
            qref.pop(idx)
            qref.append(pipeline)
            break

        if not placed:
            # If we couldn't place a QUERY/INTERACTIVE op anywhere, consider preemption of batch.
            if pipeline.priority in (Priority.QUERY, Priority.INTERACTIVE):
                if len(suspensions) < s.max_preemptions_per_tick:
                    cand = _choose_preemption_candidate(pipeline.priority)
                    if cand is not None:
                        c_id, c_pool = cand
                        suspensions.append(Suspend(c_id, c_pool))
                        made_progress = True  # after suspension, we may be able to place next loop
                        # Do not rotate pipeline yet; keep it at front for fast retry
                        continue
            # Otherwise stop trying to place this tick; avoid busy-looping
            break

    return suspensions, assignments
