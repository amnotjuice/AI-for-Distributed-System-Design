# policy_key: scheduler_est_007
# reasoning_effort: high
# model: gpt-5.2-2025-12-11
# llm_cost: 0.085994
# generation_seconds: 77.19
# generated_at: 2026-03-31T18:33:01.624844
@register_scheduler_init(key="scheduler_est_007")
def scheduler_est_007_init(s):
    """Priority-aware, right-sized, work-conserving scheduler.

    Incremental improvements over naive FIFO:
      1) Priority queues: schedule QUERY/INTERACTIVE before BATCH.
      2) Right-size per-op CPU/RAM (use op.estimate.mem_peak_gb + safety margin),
         instead of handing an op the entire pool.
      3) Pack multiple ops per tick (across pipelines) while resources last.
      4) OOM-style retry: if an op fails, increase its RAM multiplier and retry
         up to a small max_retries; then drop the pipeline to avoid infinite churn.
      5) Reserve some headroom in each pool for high-priority work by limiting
         how much BATCH is allowed to consume.
    """
    from collections import deque

    # Per-priority pipeline queues (round-robin within each priority).
    s.q_query = deque()
    s.q_interactive = deque()
    s.q_batch = deque()

    # Failure-adaptive sizing keyed by operator object identity.
    s.op_attempts = {}     # op_key -> attempts
    s.op_mem_mult = {}     # op_key -> multiplier

    # Policy knobs (kept conservative; grow complexity only after this is stable).
    s.max_retries = 3

    # Headroom reserved for higher priority (limits batch consumption).
    s.reserve_cpu_frac_for_hp = 0.25
    s.reserve_ram_frac_for_hp = 0.20

    # Sizing defaults.
    s.default_mem_gb = 2.0
    s.mem_safety_hp = 1.35   # QUERY/INTERACTIVE
    s.mem_safety_batch = 1.20

    # CPU caps to avoid hogging a whole pool for a single op.
    s.cpu_cap_hp = 4.0
    s.cpu_cap_batch = 2.0

    # Small floor to avoid pathological tiny allocations.
    s.min_ram_gb = 0.25
    s.min_cpu = 0.5


@register_scheduler(key="scheduler_est_007")
def scheduler_est_007(s, results, pipelines):
    """
    See init() docstring for policy summary.
    """
    # ---- Helpers (defined inside to keep the submission self-contained) ----
    def _prio_class(priority):
        # Highest to lowest
        if priority == Priority.QUERY:
            return 0
        if priority == Priority.INTERACTIVE:
            return 1
        return 2  # Priority.BATCH_PIPELINE and anything else

    def _enqueue_pipeline(p):
        pc = _prio_class(p.priority)
        if pc == 0:
            s.q_query.append(p)
        elif pc == 1:
            s.q_interactive.append(p)
        else:
            s.q_batch.append(p)

    def _iter_queues_in_order():
        # QUERY -> INTERACTIVE -> BATCH
        return (s.q_query, s.q_interactive, s.q_batch)

    def _op_key(op):
        # Use object identity; stable during the simulation process.
        return id(op)

    def _base_mem_est_gb(op):
        est = getattr(getattr(op, "estimate", None), "mem_peak_gb", None)
        if est is None:
            return s.default_mem_gb
        try:
            # Some sims may provide numpy scalars; cast robustly.
            return float(est)
        except Exception:
            return s.default_mem_gb

    def _mem_multiplier(op):
        return float(s.op_mem_mult.get(_op_key(op), 1.0))

    def _estimate_ram_gb(op, priority, pool_avail_ram):
        base = _base_mem_est_gb(op)
        safety = s.mem_safety_hp if _prio_class(priority) < 2 else s.mem_safety_batch
        mult = _mem_multiplier(op)
        req = base * safety * mult

        # Clamp into sensible bounds.
        if req < s.min_ram_gb:
            req = s.min_ram_gb
        if req > pool_avail_ram:
            req = pool_avail_ram
        return req

    def _estimate_cpu(op, priority, pool_avail_cpu):
        # For interactive-ish work, bias toward a few vCPUs for latency.
        cap = s.cpu_cap_hp if _prio_class(priority) < 2 else s.cpu_cap_batch
        req = min(pool_avail_cpu, cap)
        if req < s.min_cpu:
            req = min(pool_avail_cpu, s.min_cpu)  # allow tiny pools to still run
        return req

    def _batch_avail_limit(pool, avail_cpu, avail_ram):
        """
        Limit how much BATCH can consume to preserve headroom for high-priority work.
        Effective available = current available - reserved_fraction * pool_capacity.
        """
        cpu_limit = max(0.0, avail_cpu - pool.max_cpu_pool * float(s.reserve_cpu_frac_for_hp))
        ram_limit = max(0.0, avail_ram - pool.max_ram_pool * float(s.reserve_ram_frac_for_hp))
        return cpu_limit, ram_limit

    def _pick_next_runnable(queue):
        """
        Round-robin: rotate through pipelines until a runnable op is found.
        Returns (pipeline, op) or (None, None). Completed pipelines are dropped.
        Pipelines with repeatedly failing ops are dropped after max_retries.
        """
        if not queue:
            return None, None

        n = len(queue)
        for _ in range(n):
            p = queue.popleft()
            st = p.runtime_status()

            # Drop completed pipelines.
            if st.is_pipeline_successful():
                continue

            # If some failed ops exceeded retry budget, drop the whole pipeline
            # (prevents infinite churn due to deterministic non-memory errors).
            failed_ops = st.get_ops([OperatorState.FAILED], require_parents_complete=False)
            too_many = False
            for op in failed_ops:
                if s.op_attempts.get(_op_key(op), 0) >= s.max_retries:
                    too_many = True
                    break
            if too_many:
                continue

            # Find exactly one runnable op (parents complete) to keep assignments small.
            op_list = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
            if not op_list:
                # Not runnable yet; keep pipeline in queue for later.
                queue.append(p)
                continue

            # Runnable: put pipeline back (round-robin fairness) and return op.
            queue.append(p)
            return p, op_list[0]

        return None, None

    # ---- Ingest new pipelines ----
    for p in pipelines:
        _enqueue_pipeline(p)

    # If nothing changed, avoid recomputation (executor frees resources via results).
    if not pipelines and not results:
        return [], []

    # ---- Update failure-adaptive sizing from recent results ----
    # We do not attempt preemption here (keeps policy safe/simple).
    for r in results:
        if hasattr(r, "failed") and r.failed():
            # Increase RAM multiplier more aggressively if error message looks like OOM.
            err = str(getattr(r, "error", "") or "").lower()
            oomish = ("oom" in err) or ("out of memory" in err) or ("memory" in err)

            for op in getattr(r, "ops", []) or []:
                k = _op_key(op)
                s.op_attempts[k] = int(s.op_attempts.get(k, 0)) + 1

                prev = float(s.op_mem_mult.get(k, 1.0))
                bump = 1.60 if oomish else 1.25
                # Cap growth to avoid asking for absurd RAM indefinitely.
                s.op_mem_mult[k] = min(prev * bump, 8.0)

    suspensions = []
    assignments = []

    # Snapshot pool availability locally so we can pack multiple assignments.
    avail_cpu = []
    avail_ram = []
    pools = []
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        pools.append(pool)
        avail_cpu.append(float(pool.avail_cpu_pool))
        avail_ram.append(float(pool.avail_ram_pool))

    # ---- Core scheduling loop: pack work while any pool has resources ----
    # We iterate by priority, but choose the best-fitting pool for each picked op.
    max_total_assignments = 256  # safety bound
    made_progress = True

    while made_progress and len(assignments) < max_total_assignments:
        made_progress = False

        for queue in _iter_queues_in_order():
            # Try to schedule multiple ops per priority level per tick.
            # Bounded by queue length to avoid infinite loops when nothing fits.
            tries = len(queue)
            while tries > 0 and len(assignments) < max_total_assignments:
                tries -= 1

                p, op = _pick_next_runnable(queue)
                if p is None or op is None:
                    break

                pc = _prio_class(p.priority)

                # Choose a pool that can fit this op (best-fit-ish).
                chosen_pool_id = None
                chosen_cpu = None
                chosen_ram = None
                best_score = None

                for pool_id, pool in enumerate(pools):
                    cpu_here = avail_cpu[pool_id]
                    ram_here = avail_ram[pool_id]
                    if cpu_here <= 0.0 or ram_here <= 0.0:
                        continue

                    # Apply batch headroom limits.
                    if pc == 2:
                        cpu_here, ram_here = _batch_avail_limit(pool, cpu_here, ram_here)
                        if cpu_here <= 0.0 or ram_here <= 0.0:
                            continue

                    req_ram = _estimate_ram_gb(op, p.priority, ram_here)
                    req_cpu = _estimate_cpu(op, p.priority, cpu_here)

                    if req_cpu <= 0.0 or req_ram <= 0.0:
                        continue
                    if req_cpu > cpu_here + 1e-9 or req_ram > ram_here + 1e-9:
                        continue

                    # Score: prefer leaving more slack (reduce fragmentation / future OOM risk),
                    # but also prefer using resources (avoid tiny placements).
                    slack_cpu = cpu_here - req_cpu
                    slack_ram = ram_here - req_ram
                    score = (slack_ram * 10.0) + slack_cpu  # RAM is usually tighter for analytics

                    if best_score is None or score < best_score:
                        best_score = score
                        chosen_pool_id = pool_id
                        chosen_cpu = req_cpu
                        chosen_ram = req_ram

                if chosen_pool_id is None:
                    # Couldn't place this op now; rotate and let others try.
                    # (Pipeline already re-appended by _pick_next_runnable).
                    continue

                assignments.append(
                    Assignment(
                        ops=[op],
                        cpu=chosen_cpu,
                        ram=chosen_ram,
                        priority=p.priority,
                        pool_id=chosen_pool_id,
                        pipeline_id=p.pipeline_id,
                    )
                )

                # Decrement local availability so we can keep packing.
                avail_cpu[chosen_pool_id] -= float(chosen_cpu)
                avail_ram[chosen_pool_id] -= float(chosen_ram)
                made_progress = True

    return suspensions, assignments
