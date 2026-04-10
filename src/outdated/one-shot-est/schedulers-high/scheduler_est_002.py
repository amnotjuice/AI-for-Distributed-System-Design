# policy_key: scheduler_est_002
# reasoning_effort: high
# model: gpt-5.2-2025-12-11
# llm_cost: 0.155017
# generation_seconds: 105.33
# generated_at: 2026-03-31T18:24:23.898200
@register_scheduler_init(key="scheduler_est_002")
def scheduler_est_002_init(s):
    """Priority-aware, estimate-driven scheduler (small step up from naive FIFO).

    Improvements over naive:
      1) Separate queues per priority (QUERY > INTERACTIVE > BATCH).
      2) Avoid "give all resources to one op" by right-sizing CPU/RAM per op.
      3) Use op.estimate.mem_peak_gb (plus a safety factor) to size RAM.
      4) Basic OOM-aware retry: on OOM-like failures, increase RAM multiplier and retry
         (FAILED is assignable in Eudoxia via ASSIGNABLE_STATES).
      5) Simple headroom reservation: when high-priority demand exists, avoid starting
         new low-priority work that would consume the last remaining headroom.
    """
    from collections import deque

    # Per-priority pipeline queues (round-robin within each class).
    s.q_query = deque()
    s.q_interactive = deque()
    s.q_batch = deque()

    # Track which pipeline_ids we've already enqueued (avoid duplicates on arrival).
    s.known_pipeline_ids = set()

    # Per-operator (by object identity) failure tracking and RAM bumping.
    s.op_fail_count = {}   # key=id(op) -> int
    s.op_mem_bump = {}     # key=id(op) -> float multiplier

    # Policy knobs (kept conservative; can be tuned with simulation feedback).
    s.max_retries = 3
    s.mem_safety = 1.20          # multiply estimate to reduce OOM risk
    s.mem_bump_mult = 1.60       # multiplier applied after each OOM-like failure
    s.mem_bump_cap = 8.0

    # Minimal RAM floor per priority (GB): helps when estimates are missing/too small.
    s.min_ram_floor_gb = {
        Priority.QUERY: 1.0,
        Priority.INTERACTIVE: 1.0,
        Priority.BATCH_PIPELINE: 1.0,
    }

    # Target CPU fraction per op by priority (of pool.max_cpu_pool).
    # (Naive used "all available CPU" which can hurt concurrency and tail latency.)
    s.cpu_frac = {
        Priority.QUERY: 0.50,
        Priority.INTERACTIVE: 0.33,
        Priority.BATCH_PIPELINE: 0.25,
    }

    # Headroom reservations (fractions of pool.max_*), applied only when scheduling
    # lower priorities while higher-priority demand exists.
    s.reserve_for_query = 0.50
    s.reserve_for_interactive = 0.25


@register_scheduler(key="scheduler_est_002")
def scheduler_est_002(s, results, pipelines):
    """Priority-aware, estimate-driven scheduling step."""
    # ----------------------------
    # Helper functions (pure local)
    # ----------------------------
    def _prio_queue(priority):
        if priority == Priority.QUERY:
            return s.q_query
        if priority == Priority.INTERACTIVE:
            return s.q_interactive
        return s.q_batch

    def _is_oom_error(err) -> bool:
        if err is None:
            return False
        msg = str(err).lower()
        # Keep this heuristic broad; Eudoxia errors can vary.
        return ("oom" in msg) or ("out of memory" in msg) or ("memory" in msg and "alloc" in msg)

    def _pipeline_should_drop(p) -> bool:
        # Drop if fully successful.
        st = p.runtime_status()
        if st.is_pipeline_successful():
            return True

        # Drop if any FAILED op exceeded retry cap (including non-OOM failures).
        failed_ops = st.get_ops([OperatorState.FAILED], require_parents_complete=False)
        for op in failed_ops:
            if s.op_fail_count.get(id(op), 0) > s.max_retries:
                return True
        return False

    def _pipeline_ready_op(p):
        # Returns first ready op or None.
        st = p.runtime_status()
        ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        if not ops:
            return None
        return ops[0]

    def _has_ready_in_queue(q) -> bool:
        # Scan without permanently reordering (rotate through once).
        n = len(q)
        for _ in range(n):
            p = q.popleft()
            q.append(p)
            if _pipeline_should_drop(p):
                continue
            if _pipeline_ready_op(p) is not None:
                return True
        return False

    def _op_mem_est_gb(op) -> float:
        est = None
        try:
            est_obj = getattr(op, "estimate", None)
            est = getattr(est_obj, "mem_peak_gb", None)
        except Exception:
            est = None
        if est is None:
            return 1.0
        try:
            est_val = float(est)
            return est_val if est_val > 0 else 1.0
        except Exception:
            return 1.0

    def _compute_request(pool, priority, op):
        # CPU request: a capped fraction of pool max, at least 1.
        max_cpu = float(pool.max_cpu_pool)
        target = max_cpu * float(s.cpu_frac.get(priority, 0.25))
        cpu_req = int(max(1, round(target)))

        # RAM request: estimate * safety * bump, plus a floor.
        mem_est = _op_mem_est_gb(op)
        bump = float(s.op_mem_bump.get(id(op), 1.0))
        floor = float(s.min_ram_floor_gb.get(priority, 1.0))
        ram_req = max(mem_est, floor) * float(s.mem_safety) * bump
        return cpu_req, ram_req

    def _pick_fit_from_queue(q, pool, eff_avail_cpu, eff_avail_ram):
        """Round-robin scan for a pipeline with a ready op that fits."""
        n = len(q)
        for _ in range(n):
            p = q.popleft()

            # Lazy cleanup: drop completed/terminally-failed pipelines.
            if _pipeline_should_drop(p):
                s.known_pipeline_ids.discard(p.pipeline_id)
                continue

            op = _pipeline_ready_op(p)
            if op is None:
                q.append(p)
                continue

            cpu_req, ram_req = _compute_request(pool, p.priority, op)

            # Must fit in the effective available headroom.
            if eff_avail_cpu >= cpu_req and eff_avail_ram >= ram_req:
                # Keep pipeline in rotation (append to end), but allow this op now.
                q.append(p)
                return p, op, cpu_req, ram_req

            # Doesn't fit; keep RR order.
            q.append(p)
        return None

    # ----------------------------
    # Ingest new pipelines (enqueue once)
    # ----------------------------
    for p in pipelines:
        if p.pipeline_id in s.known_pipeline_ids:
            continue
        s.known_pipeline_ids.add(p.pipeline_id)
        _prio_queue(p.priority).append(p)

    # ----------------------------
    # Learn from results (OOM -> bump memory and retry)
    # ----------------------------
    for r in results:
        ops = getattr(r, "ops", None) or []
        for op in ops:
            k = id(op)
            if r.failed():
                # Non-OOM failures are treated as non-retriable (cap exceeded).
                if _is_oom_error(getattr(r, "error", None)):
                    s.op_fail_count[k] = s.op_fail_count.get(k, 0) + 1
                    prev = float(s.op_mem_bump.get(k, 1.0))
                    s.op_mem_bump[k] = min(s.mem_bump_cap, prev * float(s.mem_bump_mult))
                else:
                    s.op_fail_count[k] = s.max_retries + 1
            else:
                # Success: clear learned bumps for this op.
                s.op_fail_count.pop(k, None)
                s.op_mem_bump.pop(k, None)

    # Early exit if nothing new happened that could change decisions.
    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    # Compute current high-priority demand to guide headroom reservations.
    has_query_demand = _has_ready_in_queue(s.q_query)
    has_interactive_demand = _has_ready_in_queue(s.q_interactive)

    # ----------------------------
    # Schedule per pool (pack multiple ops per pool if resources allow)
    # ----------------------------
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)
        if avail_cpu < 1 or avail_ram <= 0:
            continue

        # Keep packing until we can no longer place anything that fits.
        while avail_cpu >= 1 and avail_ram > 0:
            placed = False

            # 1) QUERY: no reservation applied (highest priority)
            pick = _pick_fit_from_queue(s.q_query, pool, avail_cpu, avail_ram)
            if pick is not None:
                p, op, cpu_req, ram_req = pick
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
                avail_cpu -= cpu_req
                avail_ram -= ram_req
                placed = True
                continue

            # 2) INTERACTIVE: reserve headroom if query demand exists
            if has_query_demand:
                res_cpu = float(pool.max_cpu_pool) * float(s.reserve_for_query)
                res_ram = float(pool.max_ram_pool) * float(s.reserve_for_query)
                eff_cpu = max(0.0, avail_cpu - res_cpu)
                eff_ram = max(0.0, avail_ram - res_ram)
            else:
                eff_cpu, eff_ram = avail_cpu, avail_ram

            pick = _pick_fit_from_queue(s.q_interactive, pool, eff_cpu, eff_ram)
            if pick is not None:
                p, op, cpu_req, ram_req = pick
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
                avail_cpu -= cpu_req
                avail_ram -= ram_req
                placed = True
                continue

            # 3) BATCH: reserve headroom if any higher-priority demand exists
            if has_query_demand:
                reserve_frac = float(s.reserve_for_query)
            elif has_interactive_demand:
                reserve_frac = float(s.reserve_for_interactive)
            else:
                reserve_frac = 0.0

            if reserve_frac > 0.0:
                res_cpu = float(pool.max_cpu_pool) * reserve_frac
                res_ram = float(pool.max_ram_pool) * reserve_frac
                eff_cpu = max(0.0, avail_cpu - res_cpu)
                eff_ram = max(0.0, avail_ram - res_ram)
            else:
                eff_cpu, eff_ram = avail_cpu, avail_ram

            pick = _pick_fit_from_queue(s.q_batch, pool, eff_cpu, eff_ram)
            if pick is not None:
                p, op, cpu_req, ram_req = pick
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
                avail_cpu -= cpu_req
                avail_ram -= ram_req
                placed = True
                continue

            # Nothing fits (or we are intentionally reserving headroom) => stop packing this pool.
            if not placed:
                break

    return suspensions, assignments
