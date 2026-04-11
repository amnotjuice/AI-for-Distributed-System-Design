# policy_key: scheduler_high_004
# reasoning_effort: high
# exp: reasoning
# model: gpt-5.2-2025-12-11
# llm_cost: 0.144015
# generation_seconds: 187.07
# generated_at: 2026-04-10T00:23:54.300773
@register_scheduler_init(key="scheduler_high_004")
def scheduler_high_004_init(s):
    """Priority-aware, failure-averse scheduler with gentle batching.

    Core ideas:
      - Strict priority ordering for QUERY > INTERACTIVE > BATCH, with mild aging for batch to avoid starvation.
      - Conservative CPU sizing (sublinear scaling) + adaptive RAM sizing (OOM backoff) to maximize completion rate.
      - Keep a small headroom reserve when scheduling batch work to reduce queueing spikes for high priority arrivals.
      - Retry FAILED operators (instead of dropping pipelines) with increased RAM hints to avoid 720s penalties.
    """
    s.tick = 0

    # Track all pipelines we have ever seen and keep stable references.
    s.pipelines_by_id = {}          # pipeline_id -> Pipeline
    s.pipeline_priority = {}        # pipeline_id -> Priority
    s.pipeline_enqueue_tick = {}    # pipeline_id -> first seen tick

    # Priority queues store pipeline_id entries (round-robin).
    s.q_query = []
    s.q_interactive = []
    s.q_batch = []

    # Adaptive per-operator RAM hinting to reduce repeated OOM failures.
    # Keys use operator objects directly (stable within a simulation run).
    s.op_ram_hint = {}              # op -> float (ram)
    s.op_fail_count = {}            # op -> int

    # Mild starvation protection for batch.
    s.batch_promote_age_ticks = 200     # if oldest batch waits this long, treat it like interactive
    s.batch_min_progress_period = 20    # every N ticks, allow at least one batch placement per pool (if possible)
    s.last_batch_progress_tick = {}     # pool_id -> last tick when batch was scheduled

    # Keep a per-pool "HP preference" split: if >=2 pools, prefer pool 0 for HP (still allow HP anywhere).
    s.hp_pool_ids = None


@register_scheduler(key="scheduler_high_004")
def scheduler_high_004(s, results: List["ExecutionResult"],
                       pipelines: List["Pipeline"]) -> Tuple[List["Suspend"], List["Assignment"]]:
    # ---- helpers (defined inside to avoid imports / external dependencies) ----
    def _queue_for_priority(pri):
        if pri == Priority.QUERY:
            return s.q_query
        if pri == Priority.INTERACTIVE:
            return s.q_interactive
        return s.q_batch

    def _is_pipeline_done(p):
        st = p.runtime_status()
        return st.is_pipeline_successful()

    def _pipeline_inflight_ops(p):
        st = p.runtime_status()
        return (
            st.state_counts.get(OperatorState.ASSIGNED, 0) +
            st.state_counts.get(OperatorState.RUNNING, 0) +
            st.state_counts.get(OperatorState.SUSPENDING, 0)
        )

    def _inflight_limit_for_priority(pri):
        # Keep HP pipelines less fanned-out to reduce memory blowups and contention.
        if pri == Priority.BATCH_PIPELINE:
            return 2
        return 1

    def _base_cpu_target(pool, pri):
        # Sublinear CPU scaling: avoid "take all CPUs" unless necessary.
        # Targets are fractions of the pool; actual will be min(target, avail_cpu).
        mc = float(pool.max_cpu_pool)
        if pri == Priority.QUERY:
            return max(1.0, mc * 0.50)
        if pri == Priority.INTERACTIVE:
            return max(1.0, mc * 0.35)
        return max(1.0, mc * 0.25)

    def _base_ram_target(pool, pri):
        # RAM beyond minimum doesn't speed up, but under-alloc causes OOM.
        # Start moderate (not huge) to permit concurrency; rely on OOM backoff to converge.
        mr = float(pool.max_ram_pool)
        if pri == Priority.QUERY:
            return max(1.0, mr * 0.35)
        if pri == Priority.INTERACTIVE:
            return max(1.0, mr * 0.25)
        return max(1.0, mr * 0.20)

    def _headroom_reserve(pool, hp_backlog_present):
        # Without preemption, keeping a small reserve reduces HP queueing spikes.
        # If no HP backlog, allow batch to use more capacity (reserve shrinks).
        if not hp_backlog_present:
            return 0.0, 0.0
        # Modest reserve; don't starve batch by holding too much idle.
        reserve_cpu = max(1.0, float(pool.max_cpu_pool) * 0.10)
        reserve_ram = max(1.0, float(pool.max_ram_pool) * 0.10)
        return reserve_cpu, reserve_ram

    def _op_ram_hint(op):
        hint = s.op_ram_hint.get(op, 0.0)
        if hint is None:
            return 0.0
        try:
            return float(hint)
        except Exception:
            return 0.0

    def _compute_request(pool, pri, op, avail_cpu, avail_ram, hp_backlog_present, is_batch_pick):
        # RAM: at least hint, at least base target; never exceed pool max.
        base_ram = _base_ram_target(pool, pri)
        hint_ram = _op_ram_hint(op)
        req_ram = min(float(pool.max_ram_pool), max(base_ram, hint_ram))

        # CPU: base target; can be reduced to fit available; never exceed pool max.
        base_cpu = _base_cpu_target(pool, pri)
        req_cpu = min(float(pool.max_cpu_pool), base_cpu)

        # Batch headroom reservation (only applied when picking batch and HP backlog exists).
        if is_batch_pick:
            reserve_cpu, reserve_ram = _headroom_reserve(pool, hp_backlog_present)
            # If we don't have room above reserve, don't place more batch.
            if (avail_cpu - reserve_cpu) < 1.0 or (avail_ram - reserve_ram) < max(1.0, min(req_ram, base_ram)):
                return None

            # Cap batch request so it doesn't gobble the entire pool and block later HP arrivals.
            req_cpu = min(req_cpu, max(1.0, float(pool.max_cpu_pool) * 0.25))
            req_ram = min(req_ram, max(1.0, float(pool.max_ram_pool) * 0.25))

        # Fit to current available.
        cpu = min(float(avail_cpu), req_cpu)
        ram = min(float(avail_ram), req_ram)

        # Enforce minimum CPU.
        if cpu < 1.0:
            return None

        # Enforce that RAM is at least our current hint (to avoid repeated OOM).
        if ram + 1e-9 < hint_ram:
            return None

        # Avoid trivially small RAM allocations.
        if ram < 1.0:
            return None

        return cpu, ram

    def _oldest_age_ticks(queue_list):
        oldest = None
        for pid in queue_list:
            t0 = s.pipeline_enqueue_tick.get(pid, None)
            if t0 is None:
                continue
            age = s.tick - t0
            if oldest is None or age > oldest:
                oldest = age
        return oldest if oldest is not None else 0

    def _pick_next_pipeline_id(queue_list):
        # Round-robin: pop front; if still active, append back after consideration.
        # Returns pipeline_id or None.
        while queue_list:
            pid = queue_list.pop(0)
            p = s.pipelines_by_id.get(pid, None)
            if p is None:
                continue
            if _is_pipeline_done(p):
                # Drop completed pipelines aggressively to keep queues small.
                s.pipelines_by_id.pop(pid, None)
                s.pipeline_priority.pop(pid, None)
                s.pipeline_enqueue_tick.pop(pid, None)
                continue
            # Keep it in rotation.
            queue_list.append(pid)
            return pid
        return None

    def _get_runnable_op(p):
        st = p.runtime_status()
        ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        if not ops:
            return None
        return ops[0]

    def _hp_backlog_exists():
        # "Backlog" means there exists at least one runnable QUERY or INTERACTIVE op not already saturated by inflight limit.
        for q, pri in ((s.q_query, Priority.QUERY), (s.q_interactive, Priority.INTERACTIVE)):
            for pid in q:
                p = s.pipelines_by_id.get(pid, None)
                if p is None:
                    continue
                if _is_pipeline_done(p):
                    continue
                if _pipeline_inflight_ops(p) >= _inflight_limit_for_priority(pri):
                    continue
                op = _get_runnable_op(p)
                if op is not None:
                    return True
        return False

    # ---- tick bookkeeping ----
    s.tick += 1

    # Initialize hp pool preferences once executor is available.
    if s.hp_pool_ids is None:
        try:
            n = int(s.executor.num_pools)
        except Exception:
            n = 1
        if n >= 2:
            s.hp_pool_ids = {0}
        else:
            s.hp_pool_ids = set(range(max(1, n)))

    # ---- ingest new pipelines ----
    for p in pipelines:
        pid = p.pipeline_id
        if pid not in s.pipelines_by_id:
            s.pipelines_by_id[pid] = p
            s.pipeline_priority[pid] = p.priority
            s.pipeline_enqueue_tick[pid] = s.tick
            _queue_for_priority(p.priority).append(pid)
        else:
            # Update reference just in case the simulator provides a newer object wrapper.
            s.pipelines_by_id[pid] = p
            s.pipeline_priority[pid] = p.priority

    # ---- process results: update RAM hints on failures to reduce repeat OOM ----
    for r in results:
        if getattr(r, "ops", None) is None:
            continue

        # Determine pool max RAM for capping hints where possible.
        pool_max_ram = None
        try:
            pool_max_ram = float(s.executor.pools[r.pool_id].max_ram_pool)
        except Exception:
            pool_max_ram = None

        if r.failed():
            for op in r.ops:
                prev = _op_ram_hint(op)
                alloc = 0.0
                try:
                    alloc = float(getattr(r, "ram", 0.0) or 0.0)
                except Exception:
                    alloc = 0.0

                # Exponential backoff: if we failed, double the larger of (previous hint, last allocation).
                bumped = max(prev, alloc, 1.0) * 2.0

                # Cap to pool max if we can (still may run on another pool later, but avoids absurd growth).
                if pool_max_ram is not None:
                    bumped = min(bumped, pool_max_ram)

                s.op_ram_hint[op] = max(prev, bumped)
                s.op_fail_count[op] = int(s.op_fail_count.get(op, 0)) + 1
        else:
            # On success, record a weak lower-bound (do not inflate hints).
            for op in r.ops:
                alloc = 0.0
                try:
                    alloc = float(getattr(r, "ram", 0.0) or 0.0)
                except Exception:
                    alloc = 0.0
                if alloc > 0:
                    cur = _op_ram_hint(op)
                    # Keep the smaller of current hint and observed alloc if current hint is larger,
                    # but never reduce below a small floor to avoid oscillations.
                    if cur > 0:
                        s.op_ram_hint[op] = max(1.0, min(cur, alloc))
                    else:
                        s.op_ram_hint[op] = max(1.0, alloc)

    # Early exit if nothing changed.
    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    # Global backlog signals for headroom policy and batch aging.
    hp_backlog_present = _hp_backlog_exists()
    oldest_batch_age = _oldest_age_ticks(s.q_batch)
    batch_is_urgent = (oldest_batch_age >= s.batch_promote_age_ticks)

    # ---- scheduling loop across pools ----
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)
        if avail_cpu < 1.0 or avail_ram < 1.0:
            continue

        max_assignments_this_pool = 4
        made = 0

        # Decide local preference: if we have a "preferred HP pool", try harder to place HP here.
        prefer_hp_here = (pool_id in s.hp_pool_ids)

        while made < max_assignments_this_pool:
            if avail_cpu < 1.0 or avail_ram < 1.0:
                break

            # Choose which priority to pull from:
            #   - Always try QUERY first
            #   - Then INTERACTIVE
            #   - Then BATCH, unless oldest batch is urgent (then consider it as INTERACTIVE),
            #     and allow occasional batch progress even under HP backlog.
            pick_order = []

            pick_order.append(Priority.QUERY)
            pick_order.append(Priority.INTERACTIVE)

            allow_batch_now = True
            if hp_backlog_present and not batch_is_urgent:
                # Ensure at least some periodic batch progress to avoid end-of-sim incomplete penalties,
                # but primarily protect HP latency.
                last = int(s.last_batch_progress_tick.get(pool_id, -10**9))
                allow_batch_now = (s.tick - last) >= int(s.batch_min_progress_period)

            # Prefer HP more strongly on preferred HP pools: only schedule batch there when no HP backlog.
            if prefer_hp_here and hp_backlog_present and not batch_is_urgent:
                allow_batch_now = False

            if batch_is_urgent:
                # Promote batch to be considered before interactive (but after queries).
                pick_order = [Priority.QUERY, Priority.BATCH_PIPELINE, Priority.INTERACTIVE]
            elif allow_batch_now:
                pick_order.append(Priority.BATCH_PIPELINE)

            chosen_pid = None
            chosen_pri = None
            chosen_op = None
            chosen_is_batch = False

            # Find a runnable op in priority order.
            for pri in pick_order:
                q = _queue_for_priority(pri)
                if not q:
                    continue

                # Scan limited number of candidates to avoid pathological loops.
                scanned = 0
                scan_limit = min(len(q), 25)
                while scanned < scan_limit and q:
                    pid = _pick_next_pipeline_id(q)
                    scanned += 1
                    if pid is None:
                        break

                    p = s.pipelines_by_id.get(pid, None)
                    if p is None:
                        continue
                    if _is_pipeline_done(p):
                        continue

                    # Enforce per-pipeline inflight cap.
                    inflight = _pipeline_inflight_ops(p)
                    if inflight >= _inflight_limit_for_priority(pri):
                        continue

                    op = _get_runnable_op(p)
                    if op is None:
                        continue

                    # Check fit with current availability; compute request.
                    req = _compute_request(
                        pool=pool,
                        pri=pri if pri != Priority.BATCH_PIPELINE else Priority.BATCH_PIPELINE,
                        op=op,
                        avail_cpu=avail_cpu,
                        avail_ram=avail_ram,
                        hp_backlog_present=hp_backlog_present,
                        is_batch_pick=(pri == Priority.BATCH_PIPELINE),
                    )
                    if req is None:
                        continue

                    chosen_pid = pid
                    chosen_pri = pri
                    chosen_op = op
                    chosen_is_batch = (pri == Priority.BATCH_PIPELINE)
                    chosen_cpu, chosen_ram = req
                    break

                if chosen_pid is not None:
                    break

            if chosen_pid is None:
                break

            # Place the assignment.
            assignments.append(
                Assignment(
                    ops=[chosen_op],
                    cpu=chosen_cpu,
                    ram=chosen_ram,
                    priority=s.pipeline_priority.get(chosen_pid, chosen_pri),
                    pool_id=pool_id,
                    pipeline_id=chosen_pid,
                )
            )

            avail_cpu -= float(chosen_cpu)
            avail_ram -= float(chosen_ram)
            made += 1

            if chosen_is_batch:
                s.last_batch_progress_tick[pool_id] = s.tick

    return suspensions, assignments
