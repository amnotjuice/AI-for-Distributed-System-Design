# policy_key: scheduler_none_005
# reasoning_effort: none
# model: gpt-5.2-2025-12-11
# llm_cost: 0.048157
# generation_seconds: 56.51
# generated_at: 2026-04-09T21:48:50.547531
@register_scheduler_init(key="scheduler_none_005")
def scheduler_none_005_init(s):
    """Priority-aware, OOM-adaptive, low-churn scheduler.

    Goals:
    - Minimize weighted latency (query > interactive > batch) while avoiding failures (720s penalty).
    - Keep high-priority tail latency low via (a) pool headroom reservation and (b) targeted preemption.
    - Improve completion rate by learning per-op RAM requirement from OOM failures and retrying with more RAM.
    - Avoid starvation via simple aging boosts for long-waiting pipelines.
    """
    # Queues per priority (store pipelines; we re-check their runnable ops each tick)
    s.q_query = []
    s.q_interactive = []
    s.q_batch = []

    # Book-keeping for pipeline arrivals (for aging) and for simple de-dup in queues
    s.now = 0
    s.enqueue_ts = {}  # pipeline_id -> logical time when last enqueued
    s.in_queue = set()  # pipeline_id currently present in any queue

    # Learned RAM minima per (pipeline_id, op_id) from OOM failures; used for retries
    # Keying op by id(op) is safe within a single simulation run where objects are stable.
    s.learned_op_ram = {}  # (pipeline_id, op_key) -> ram
    s.learned_pipe_ram_floor = {}  # pipeline_id -> ram floor (max of learned op ram)

    # Track running containers by pool and "estimated priority" (from last results/assignments)
    s.container_meta = {}  # (pool_id, container_id) -> dict(priority=..., cpu=..., ram=..., pipeline_id=...)

    # Reservation fractions per pool for higher priorities.
    # We reserve some CPU/RAM headroom to absorb bursts of queries/interactive without queueing behind batch.
    s.reserve = {
        "query_cpu_frac": 0.25,
        "query_ram_frac": 0.25,
        "interactive_cpu_frac": 0.15,
        "interactive_ram_frac": 0.15,
    }

    # Preemption tuning
    s.max_preempt_per_tick = 2
    s.preempt_cooldown = {}  # (pool_id, container_id) -> until_time
    s.preempt_cooldown_ticks = 3

    # Conservative default sizing: avoid OOMs; keep CPU moderate to reduce interference.
    # RAM is set to a safe floor and grows on OOM.
    s.default_cpu_frac = {
        Priority.QUERY: 0.7,
        Priority.INTERACTIVE: 0.6,
        Priority.BATCH_PIPELINE: 0.5,
    }
    s.min_cpu = 1.0  # ensure non-zero CPU assignment if pool has tiny headroom

    # RAM growth factor on OOM
    s.oom_growth = 1.5
    s.oom_additive = 0.5  # add a small absolute bump in addition to multiplicative growth

    # Small number of ops per assignment (reduces blast radius and improves responsiveness)
    s.ops_per_assignment = {
        Priority.QUERY: 2,
        Priority.INTERACTIVE: 2,
        Priority.BATCH_PIPELINE: 1,
    }


@register_scheduler(key="scheduler_none_005")
def scheduler_none_005_scheduler(s, results, pipelines):
    """
    Decision loop each tick:
    1) Ingest new pipelines into per-priority queues.
    2) Update learned RAM floors from OOM failures; requeue pipelines for retry.
    3) If high-priority runnable work exists but reserved headroom is insufficient, preempt low-priority containers.
    4) Place runnable ops using priority + aging, with conservative sizing and OOM-adaptive RAM.
    """
    # --- helpers (defined inside to avoid imports at top-level) ---
    def _prio_weight(prio):
        if prio == Priority.QUERY:
            return 3
        if prio == Priority.INTERACTIVE:
            return 2
        return 1

    def _get_queue(prio):
        if prio == Priority.QUERY:
            return s.q_query
        if prio == Priority.INTERACTIVE:
            return s.q_interactive
        return s.q_batch

    def _pipeline_is_done_or_failed(p):
        st = p.runtime_status()
        if st.is_pipeline_successful():
            return True
        # If any operator failed, we will still retry (OOM) but treat non-OOM failures as terminal
        # We infer OOM by ExecutionResult.failed() plus error text; if we never see such a result,
        # pipeline failures could be non-retryable. We do not drop here; we drop only if pipeline
        # has failures and no runnable ops ever appear (handled naturally by not scheduling).
        return False

    def _has_runnable_ops(p):
        st = p.runtime_status()
        op_list = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        return bool(op_list)

    def _pick_runnable_ops(p, n):
        st = p.runtime_status()
        ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        return ops[:n] if ops else []

    def _op_key(op):
        # Stable enough within a simulation run.
        return id(op)

    def _learn_from_result(r):
        # Update container meta and learned RAM on OOM-like failures
        # We keep meta for potential preemption selection.
        if getattr(r, "container_id", None) is not None:
            s.container_meta[(r.pool_id, r.container_id)] = {
                "priority": r.priority,
                "cpu": r.cpu,
                "ram": r.ram,
                "pipeline_id": getattr(r, "pipeline_id", None),
            }

        if not r.failed():
            return

        err = str(getattr(r, "error", "")).lower()
        is_oom = ("oom" in err) or ("out of memory" in err) or ("killed" in err and "memory" in err)

        if not is_oom:
            # Non-OOM failures are not handled here; we avoid aggressive retries to prevent thrash.
            return

        # If we can, learn per-op required RAM for future retries.
        pid = getattr(r, "pipeline_id", None)
        if pid is None:
            # If pipeline_id isn't available in results, we can still learn a generic bump via container meta,
            # but won't be able to target specific pipelines; skip to avoid mislearning.
            return

        for op in getattr(r, "ops", []) or []:
            k = (pid, _op_key(op))
            prev = s.learned_op_ram.get(k, 0.0)
            # Increase RAM floor beyond what was used when it OOMed.
            bumped = max(r.ram * s.oom_growth + s.oom_additive, r.ram + s.oom_additive)
            s.learned_op_ram[k] = max(prev, bumped)
            s.learned_pipe_ram_floor[pid] = max(s.learned_pipe_ram_floor.get(pid, 0.0), s.learned_op_ram[k])

    def _enqueue_pipeline(p, ts=None):
        pid = p.pipeline_id
        if pid in s.in_queue:
            return
        q = _get_queue(p.priority)
        q.append(p)
        s.in_queue.add(pid)
        s.enqueue_ts[pid] = s.now if ts is None else ts

    def _dequeue_pipeline(q, idx):
        p = q.pop(idx)
        s.in_queue.discard(p.pipeline_id)
        return p

    def _queue_has_runnable(prio):
        q = _get_queue(prio)
        for p in q:
            if _pipeline_is_done_or_failed(p):
                continue
            if _has_runnable_ops(p):
                return True
        return False

    def _compute_reserved(pool, target_prio):
        # Reserve headroom for higher priorities. For query, reserve query share.
        # For interactive, reserve query+interactive share.
        if target_prio == Priority.QUERY:
            rcpu = pool.max_cpu_pool * s.reserve["query_cpu_frac"]
            rram = pool.max_ram_pool * s.reserve["query_ram_frac"]
            return rcpu, rram
        if target_prio == Priority.INTERACTIVE:
            rcpu = pool.max_cpu_pool * (s.reserve["query_cpu_frac"] + s.reserve["interactive_cpu_frac"])
            rram = pool.max_ram_pool * (s.reserve["query_ram_frac"] + s.reserve["interactive_ram_frac"])
            return rcpu, rram
        # For batch, no reservation.
        return 0.0, 0.0

    def _available_for_prio(pool, prio):
        # Compute allocatable resources after respecting reservation for higher priorities.
        # Example: when scheduling batch, keep query+interactive reserved.
        if prio == Priority.BATCH_PIPELINE:
            rcpu = pool.max_cpu_pool * (s.reserve["query_cpu_frac"] + s.reserve["interactive_cpu_frac"])
            rram = pool.max_ram_pool * (s.reserve["query_ram_frac"] + s.reserve["interactive_ram_frac"])
        elif prio == Priority.INTERACTIVE:
            rcpu = pool.max_cpu_pool * s.reserve["query_cpu_frac"]
            rram = pool.max_ram_pool * s.reserve["query_ram_frac"]
        else:
            rcpu, rram = 0.0, 0.0

        return max(0.0, pool.avail_cpu_pool - rcpu), max(0.0, pool.avail_ram_pool - rram)

    def _choose_cpu(pool, prio, avail_cpu):
        # Conservative CPU sizing: fraction of pool max, but cannot exceed avail_cpu.
        want = pool.max_cpu_pool * s.default_cpu_frac.get(prio, 0.5)
        cpu = min(avail_cpu, max(s.min_cpu, want))
        # If pool has less than min_cpu remaining, don't schedule.
        if avail_cpu < s.min_cpu:
            return 0.0
        return cpu

    def _choose_ram(pool, p, ops, avail_ram):
        # RAM sizing: use learned floors when available; otherwise be conservative
        # by allocating a fraction of pool RAM, capped by availability.
        pid = p.pipeline_id
        learned_floor = s.learned_pipe_ram_floor.get(pid, 0.0)
        # Also consider per-op learned requirements.
        for op in ops:
            learned_floor = max(learned_floor, s.learned_op_ram.get((pid, _op_key(op)), 0.0))

        # Base request: for high priorities, allocate more to avoid OOM; for batch, moderate.
        if p.priority == Priority.QUERY:
            base = pool.max_ram_pool * 0.50
        elif p.priority == Priority.INTERACTIVE:
            base = pool.max_ram_pool * 0.40
        else:
            base = pool.max_ram_pool * 0.30

        want = max(base, learned_floor)
        ram = min(avail_ram, want)
        # If we cannot meet learned_floor at all, better to not schedule than to likely OOM again.
        if learned_floor > 0.0 and avail_ram < learned_floor:
            return 0.0
        # If ram is tiny, don't schedule.
        if avail_ram <= 0:
            return 0.0
        return ram

    def _age_boost(p):
        # Simple aging: every 20 ticks waiting increases its effective score.
        pid = p.pipeline_id
        waited = max(0, s.now - s.enqueue_ts.get(pid, s.now))
        return waited // 20

    def _select_next_pipeline():
        # Choose pipeline across queues using priority + aging, but never starve.
        # We pick the best among the first few candidates of each queue to keep O(n) bounded.
        best = None
        best_q = None
        best_idx = None
        best_score = None

        for prio in (Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE):
            q = _get_queue(prio)
            # scan limited window
            window = min(len(q), 8)
            for i in range(window):
                p = q[i]
                if _pipeline_is_done_or_failed(p):
                    continue
                if not _has_runnable_ops(p):
                    continue
                score = (_prio_weight(prio) * 10) + _age_boost(p)
                # Tie-breaker: earlier enqueue
                if best is None or score > best_score:
                    best, best_q, best_idx, best_score = p, q, i, score
        if best is None:
            return None, None
        return best_q, best_idx

    def _need_preemption_for_high_prio(pool_id, pool, target_prio):
        # Determine if there is runnable high-priority work and insufficient headroom vs reservation.
        if target_prio == Priority.QUERY and not _queue_has_runnable(Priority.QUERY):
            return False
        if target_prio == Priority.INTERACTIVE and not (_queue_has_runnable(Priority.QUERY) or _queue_has_runnable(Priority.INTERACTIVE)):
            return False

        # If we are here, there is runnable high-priority work (query for query; query/interactive for interactive).
        # Require minimum allocatable CPU/RAM to start at least something.
        if target_prio == Priority.QUERY:
            min_cpu_need = max(s.min_cpu, pool.max_cpu_pool * 0.2)
            min_ram_need = pool.max_ram_pool * 0.2
        else:
            min_cpu_need = max(s.min_cpu, pool.max_cpu_pool * 0.15)
            min_ram_need = pool.max_ram_pool * 0.15

        # If current available is below these, try preempting.
        return (pool.avail_cpu_pool < min_cpu_need) or (pool.avail_ram_pool < min_ram_need)

    def _pick_preemption_candidates(pool_id, need_cpu, need_ram):
        # Pick lowest priority running containers first; avoid repeated churn via cooldown.
        candidates = []
        for (pid, cid), meta in s.container_meta.items():
            if pid != pool_id:
                continue
            if (pid, cid) in s.preempt_cooldown and s.preempt_cooldown[(pid, cid)] > s.now:
                continue
            prio = meta.get("priority", Priority.BATCH_PIPELINE)
            # Only preempt batch first, then interactive if absolutely necessary for queries.
            candidates.append((prio, meta.get("cpu", 0.0), meta.get("ram", 0.0), cid))
        # sort by priority ascending (batch lowest), then by biggest resource to free quickly
        def _prio_rank(prio):
            if prio == Priority.BATCH_PIPELINE:
                return 0
            if prio == Priority.INTERACTIVE:
                return 1
            return 2

        candidates.sort(key=lambda x: (_prio_rank(x[0]), -(x[1] + 0.01 * x[2])))
        picked = []
        freed_cpu = 0.0
        freed_ram = 0.0
        for prio, cpu, ram, cid in candidates:
            picked.append((prio, cid, cpu, ram))
            freed_cpu += cpu
            freed_ram += ram
            if freed_cpu >= need_cpu and freed_ram >= need_ram:
                break
        return picked

    # --- tick bookkeeping ---
    s.now += 1

    # Ingest new pipelines
    for p in pipelines:
        _enqueue_pipeline(p)

    # Process results: learn from failures and update meta; requeue if it fell out of queues.
    for r in results:
        _learn_from_result(r)

        # Ensure pipeline is queued to continue if it still has work (some policies may have removed it).
        pid = getattr(r, "pipeline_id", None)
        if pid is not None:
            # Locate pipeline object from existing queues or from arrivals; if not found, skip.
            # We can't reliably reconstruct pipeline object from id; thus only keep queue-based pipelines.
            pass

    # Clean queues: drop completed pipelines; keep in_queue consistent
    for q in (s.q_query, s.q_interactive, s.q_batch):
        i = 0
        while i < len(q):
            p = q[i]
            if p.runtime_status().is_pipeline_successful():
                s.in_queue.discard(p.pipeline_id)
                q.pop(i)
                continue
            i += 1

    # Early exit if no changes
    if not pipelines and not results:
        # Still may want to schedule if resources freed asynchronously; but simulator typically calls with results.
        # We'll proceed to scheduling anyway to be safe.
        pass

    suspensions = []
    assignments = []

    # --- Preemption stage: only if needed to protect high-priority latency ---
    # We attempt query protection first, then interactive protection.
    preempt_budget = s.max_preempt_per_tick
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]

        # If queries are runnable and headroom is too low, preempt batch first.
        if preempt_budget > 0 and _need_preemption_for_high_prio(pool_id, pool, Priority.QUERY):
            # Compute desired headroom: enough to start a typical query assignment
            need_cpu = max(0.0, max(s.min_cpu, pool.max_cpu_pool * 0.25) - pool.avail_cpu_pool)
            need_ram = max(0.0, pool.max_ram_pool * 0.25 - pool.avail_ram_pool)
            picked = _pick_preemption_candidates(pool_id, need_cpu, need_ram)
            for prio, cid, cpu, ram in picked:
                if preempt_budget <= 0:
                    break
                # For query protection: allow preempting batch and interactive; avoid preempting queries.
                if prio == Priority.QUERY:
                    continue
                suspensions.append(Suspend(container_id=cid, pool_id=pool_id))
                s.preempt_cooldown[(pool_id, cid)] = s.now + s.preempt_cooldown_ticks
                preempt_budget -= 1

        # If interactive runnable (or queries) and headroom too low, preempt batch only.
        if preempt_budget > 0 and _need_preemption_for_high_prio(pool_id, pool, Priority.INTERACTIVE):
            need_cpu = max(0.0, max(s.min_cpu, pool.max_cpu_pool * 0.15) - pool.avail_cpu_pool)
            need_ram = max(0.0, pool.max_ram_pool * 0.15 - pool.avail_ram_pool)
            picked = _pick_preemption_candidates(pool_id, need_cpu, need_ram)
            for prio, cid, cpu, ram in picked:
                if preempt_budget <= 0:
                    break
                if prio != Priority.BATCH_PIPELINE:
                    continue
                suspensions.append(Suspend(container_id=cid, pool_id=pool_id))
                s.preempt_cooldown[(pool_id, cid)] = s.now + s.preempt_cooldown_ticks
                preempt_budget -= 1

    # --- Placement stage: fill pools while respecting reservation ---
    # We schedule in priority order but let aging influence selection within/between queues via _select_next_pipeline.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]

        # Try to place multiple assignments per pool if headroom allows.
        # Keep a small cap to avoid one tick over-allocations and to preserve responsiveness.
        local_assignments = 0
        local_cap = 4

        while local_assignments < local_cap:
            sel = _select_next_pipeline()
            if sel[0] is None:
                break

            q, idx = sel
            p = _dequeue_pipeline(q, idx)

            # Determine allocatable resources for its priority after reservations
            alloc_cpu, alloc_ram = _available_for_prio(pool, p.priority)
            if alloc_cpu < s.min_cpu or alloc_ram <= 0:
                # Not enough capacity in this pool right now; requeue and stop filling this pool.
                _enqueue_pipeline(p, ts=s.enqueue_ts.get(p.pipeline_id, s.now))
                break

            ops = _pick_runnable_ops(p, s.ops_per_assignment.get(p.priority, 1))
            if not ops:
                # Nothing runnable now; requeue and continue
                _enqueue_pipeline(p, ts=s.enqueue_ts.get(p.pipeline_id, s.now))
                continue

            cpu = _choose_cpu(pool, p.priority, alloc_cpu)
            if cpu <= 0:
                _enqueue_pipeline(p, ts=s.enqueue_ts.get(p.pipeline_id, s.now))
                break

            ram = _choose_ram(pool, p, ops, alloc_ram)
            if ram <= 0:
                _enqueue_pipeline(p, ts=s.enqueue_ts.get(p.pipeline_id, s.now))
                break

            assignments.append(
                Assignment(
                    ops=ops,
                    cpu=cpu,
                    ram=ram,
                    priority=p.priority,
                    pool_id=pool_id,
                    pipeline_id=p.pipeline_id,
                )
            )

            # Track meta for potential preemption choices later.
            # We don't have container_id until results, but keep a placeholder record keyed later.
            # Still, record pipeline-level learned floor on enqueue.
            if p.pipeline_id in s.learned_pipe_ram_floor:
                pass

            # Requeue pipeline if it likely has more work (don’t starve other pipelines: append to end)
            _enqueue_pipeline(p, ts=s.enqueue_ts.get(p.pipeline_id, s.now))

            local_assignments += 1

    return suspensions, assignments
