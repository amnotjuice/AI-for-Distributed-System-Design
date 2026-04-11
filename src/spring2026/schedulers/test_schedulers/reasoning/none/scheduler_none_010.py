# policy_key: scheduler_none_010
# reasoning_effort: none
# model: gpt-5.2-2025-12-11
# llm_cost: 0.051265
# generation_seconds: 41.67
# generated_at: 2026-04-09T21:52:43.845509
@register_scheduler_init(key="scheduler_none_010")
def scheduler_none_010_init(s):
    """Priority-aware, OOM-adaptive, low-churn scheduling.

    Goals for the weighted latency objective:
      - Favor query + interactive (lower queueing delay).
      - Strongly avoid failures (720s penalty) by learning per-op RAM needs from OOM signals.
      - Keep batch making progress via mild aging and a small reserved share.
      - Limit preemption churn: only preempt for high-priority arrivals when necessary.

    Key ideas:
      - Maintain per-(pipeline_id, op_id) RAM estimates; on OOM, bump and retry (no dropping).
      - Use largest-available-pool placement to reduce contention and preemption needs.
      - Allocate CPU in a "reasonable slice" to allow concurrency, but let high priority grab more.
      - Soft fairness via per-pipeline aging to avoid indefinite starvation.
    """
    # Queues of pipelines waiting for scheduling consideration
    s.waiting = []

    # Per operator RAM estimate learned from OOM (keyed by (pipeline_id, op_id))
    # Values are in "RAM units" consistent with simulator pool.ram units.
    s.op_ram_est = {}

    # Track pipeline arrival tick for aging
    s.pipeline_first_seen_tick = {}

    # Track current tick (discrete scheduler calls)
    s.tick = 0

    # Optional: conservative starting guess for RAM if we can't infer
    s.default_ram_frac = 0.25  # of pool max RAM, capped by availability

    # On OOM, multiply RAM estimate by this factor
    s.oom_backoff = 1.6

    # Upper bound for per-op RAM request as a fraction of pool max RAM (avoid requesting "everything")
    s.max_ram_frac = 0.95

    # CPU slicing: attempt to run multiple ops concurrently; high priority gets larger slices
    s.cpu_slice_query = 0.75
    s.cpu_slice_interactive = 0.60
    s.cpu_slice_batch = 0.40
    s.min_cpu = 1.0  # don't schedule <1 vCPU (keeps runtime stable in typical models)

    # Reserve a small share for batch to avoid starvation when queries keep arriving
    s.batch_reserve_cpu_frac = 0.10
    s.batch_reserve_ram_frac = 0.10

    # Preemption: only preempt for query/interactive when we can't place otherwise
    s.enable_preemption = True

    # Track running containers we started (to enable selective preemption)
    # container_id -> dict(priority=..., pool_id=...)
    s.running = {}

    # Track which pipelines are completed/failed to prune from queue
    s.terminal_pipelines = set()


@register_scheduler(key="scheduler_none_010")
def scheduler_none_010(s, results, pipelines):
    """
    Scheduler step:
      1) Ingest new pipelines; record first seen time for aging.
      2) Process results:
          - On OOM failure: increase op RAM estimate and keep pipeline in queue for retry.
          - On other failure: keep pipeline (do not drop) but try with higher RAM next time.
          - On success: mark terminal pipeline when all ops done.
      3) Decide suspensions (preempt low priority if needed for high priority work).
      4) Create assignments:
          - Order candidates by (priority weight, aging).
          - Place on the pool with most headroom for the op.
          - Size RAM using learned estimate; size CPU using priority-based slice.
    """
    s.tick += 1

    # -----------------------------
    # Helper functions (local)
    # -----------------------------
    def _priority_weight(pri):
        # Higher means more important in our local ordering heuristic
        if pri == Priority.QUERY:
            return 1000
        if pri == Priority.INTERACTIVE:
            return 500
        return 100  # batch

    def _is_high_priority(pri):
        return pri in (Priority.QUERY, Priority.INTERACTIVE)

    def _op_key(pipeline, op):
        # Use pipeline_id + op_id if present; fall back to object id for determinism within run.
        op_id = getattr(op, "op_id", None)
        if op_id is None:
            op_id = getattr(op, "operator_id", None)
        if op_id is None:
            op_id = id(op)
        return (pipeline.pipeline_id, op_id)

    def _estimate_ram(pool, pipeline, op):
        # Use learned estimate; else conservative default fraction of pool RAM.
        k = _op_key(pipeline, op)
        est = s.op_ram_est.get(k, None)
        if est is None:
            est = max(1.0, pool.max_ram_pool * s.default_ram_frac)
        # Cap to pool max; and never exceed availability in assignment (we'll min with avail later)
        est = min(est, pool.max_ram_pool * s.max_ram_frac)
        return est

    def _cpu_slice(pool, pri):
        frac = s.cpu_slice_batch
        if pri == Priority.QUERY:
            frac = s.cpu_slice_query
        elif pri == Priority.INTERACTIVE:
            frac = s.cpu_slice_interactive
        cpu = max(s.min_cpu, pool.max_cpu_pool * frac)
        # Do not exceed pool max; later min with availability
        return min(cpu, pool.max_cpu_pool)

    def _prune_and_requeue(p):
        # Keep only non-terminal pipelines in waiting list
        if p.pipeline_id in s.terminal_pipelines:
            return
        status = p.runtime_status()
        if status.is_pipeline_successful():
            s.terminal_pipelines.add(p.pipeline_id)
            return
        s.waiting.append(p)

    def _pick_assignable_op(p):
        status = p.runtime_status()
        # If pipeline already terminal, skip
        if status.is_pipeline_successful():
            s.terminal_pipelines.add(p.pipeline_id)
            return None
        # Prefer ops whose parents are complete; take one op at a time to reduce contention/latency variance
        op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
        if not op_list:
            return None
        return op_list[0]

    def _pool_order_for_placement():
        # Prefer pools with most available resources to reduce interference and preemption likelihood
        pools = []
        for pid in range(s.executor.num_pools):
            pool = s.executor.pools[pid]
            pools.append((pool.avail_cpu_pool + 1e-9, pool.avail_ram_pool + 1e-9, pid))
        # Sort descending by both cpu and ram availability
        pools.sort(reverse=True)
        return [pid for _, __, pid in pools]

    def _aging_bonus(p):
        # Mild aging so batch doesn't starve, but not enough to harm query/interactive
        first = s.pipeline_first_seen_tick.get(p.pipeline_id, s.tick)
        age = max(0, s.tick - first)
        # Convert to small additive score; cap to avoid dominating priority
        return min(200, age)

    # -----------------------------
    # Ingest new pipelines
    # -----------------------------
    for p in pipelines:
        if p.pipeline_id not in s.pipeline_first_seen_tick:
            s.pipeline_first_seen_tick[p.pipeline_id] = s.tick
        # Avoid duplicates: we may keep old references; just append and dedup later via pipeline_id
        s.waiting.append(p)

    # -----------------------------
    # Process results: learn RAM needs and update running map
    # -----------------------------
    # Note: we don't know exact error types; treat any failure as potential under-provision.
    for r in results:
        # Remove from running tracking (container finished/failed)
        if getattr(r, "container_id", None) is not None and r.container_id in s.running:
            del s.running[r.container_id]

        if r.failed():
            # Heuristic: if error message indicates OOM, increase more aggressively.
            err = (r.error or "").lower() if hasattr(r, "error") else ""
            is_oom = ("oom" in err) or ("out of memory" in err) or ("memory" in err and "kill" in err)

            # Learn RAM estimate for each op in this container's op list.
            # We don't know true minimum; increase from previous estimate or from last assigned ram.
            for op in (r.ops or []):
                # We don't have pipeline object here; best-effort use pipeline_id + op_id from op.
                op_id = getattr(op, "op_id", None)
                if op_id is None:
                    op_id = getattr(op, "operator_id", None)
                if op_id is None:
                    op_id = id(op)
                k = (r.pipeline_id, op_id) if hasattr(r, "pipeline_id") else (None, op_id)

                prev = s.op_ram_est.get(k, None)
                base = r.ram if getattr(r, "ram", None) is not None else prev
                if base is None:
                    # Fallback: bump to a moderate value; pool not known -> use a generic guess
                    base = 1.0

                factor = s.oom_backoff * (1.25 if is_oom else 1.0)
                new_est = max(base * factor, (prev or 0) * factor)
                s.op_ram_est[k] = new_est

        # No special action needed on success here; pruning handled when we rescan pipelines.

    # Early exit if nothing changed
    if not pipelines and not results and not s.waiting:
        return [], []

    # -----------------------------
    # Dedup waiting pipelines by pipeline_id while preserving recent ordering
    # -----------------------------
    dedup = {}
    # Reverse to keep latest instance, then reverse back
    for p in reversed(s.waiting):
        if p.pipeline_id in s.terminal_pipelines:
            continue
        if p.pipeline_id not in dedup:
            dedup[p.pipeline_id] = p
    s.waiting = list(reversed(list(dedup.values())))

    # Prune completed pipelines
    kept = []
    for p in s.waiting:
        if p.pipeline_id in s.terminal_pipelines:
            continue
        st = p.runtime_status()
        if st.is_pipeline_successful():
            s.terminal_pipelines.add(p.pipeline_id)
            continue
        kept.append(p)
    s.waiting = kept

    # -----------------------------
    # Build a prioritized list of candidate pipelines (by priority weight + mild aging)
    # -----------------------------
    candidates = []
    for p in s.waiting:
        pri = p.priority
        score = _priority_weight(pri) + _aging_bonus(p)
        candidates.append((score, pri, p))
    candidates.sort(reverse=True, key=lambda x: x[0])

    suspensions = []
    assignments = []

    # -----------------------------
    # Optional preemption: if we have high-priority work but no headroom, preempt batch first.
    # We can only preempt containers we have tracked in s.running.
    # Since we don't have direct access to current running containers from executor API here,
    # we rely on s.running (containers we started). This keeps churn low and deterministic.
    # -----------------------------
    def _need_preempt_for(pri):
        if not s.enable_preemption or not _is_high_priority(pri):
            return False
        # If ANY pool has meaningful free cpu+ram, don't preempt.
        for pid in range(s.executor.num_pools):
            pool = s.executor.pools[pid]
            if pool.avail_cpu_pool >= s.min_cpu and pool.avail_ram_pool > 0:
                return False
        return True

    if candidates and _need_preempt_for(candidates[0][1]):
        # Preempt one batch container (or lowest priority) to create headroom.
        # Prefer preempting batch, then interactive (never preempt query unless nothing else).
        # We don't know exact freed resources; in sim, suspension should free allocation.
        def _preempt_rank(pri):
            if pri == Priority.BATCH_PIPELINE:
                return 0
            if pri == Priority.INTERACTIVE:
                return 1
            return 2  # query last

        running_list = []
        for cid, meta in s.running.items():
            running_list.append((_preempt_rank(meta["priority"]), meta["priority"], cid, meta["pool_id"]))
        running_list.sort(key=lambda x: x[0])

        # Suspend at most one container per tick to reduce churn
        if running_list:
            _, _, cid, pid = running_list[0]
            suspensions.append(Suspend(cid, pid))
            # Remove it from running immediately to avoid double-suspends
            del s.running[cid]

    # -----------------------------
    # Placement + assignment loop
    # We try to schedule up to one op per pool per tick (keeps latency stable & avoids thrash).
    # -----------------------------
    pool_order = _pool_order_for_placement()
    used_pool = set()

    for _, pri, p in candidates:
        # Skip if we've already assigned something for this pipeline this tick
        # (avoids over-parallelizing within one pipeline and hurting end-to-end latency)
        if any(a.pipeline_id == p.pipeline_id for a in assignments):
            continue

        op = _pick_assignable_op(p)
        if op is None:
            continue

        # Find a pool with enough resources
        chosen = None
        for pid in pool_order:
            if pid in used_pool:
                continue
            pool = s.executor.pools[pid]

            # Keep a small reserve for batch so it can progress even under query load.
            reserve_cpu = 0.0
            reserve_ram = 0.0
            if pri != Priority.BATCH_PIPELINE:
                reserve_cpu = pool.max_cpu_pool * s.batch_reserve_cpu_frac
                reserve_ram = pool.max_ram_pool * s.batch_reserve_ram_frac

            avail_cpu = max(0.0, pool.avail_cpu_pool - reserve_cpu)
            avail_ram = max(0.0, pool.avail_ram_pool - reserve_ram)

            if avail_cpu < s.min_cpu or avail_ram <= 0:
                continue

            req_cpu = min(_cpu_slice(pool, pri), avail_cpu)
            # RAM: learned estimate, but don't request more than available
            req_ram_est = _estimate_ram(pool, p, op)
            req_ram = min(req_ram_est, avail_ram)

            # If we are forced to shrink below our estimate, only do it for batch;
            # for query/interactive, better to wait than to risk failure penalty.
            if req_ram < 0.90 * req_ram_est and _is_high_priority(pri):
                continue

            # Still need at least some RAM to run
            if req_ram <= 0:
                continue

            chosen = (pid, req_cpu, req_ram)
            break

        if chosen is None:
            continue

        pid, cpu, ram = chosen
        pool = s.executor.pools[pid]
        used_pool.add(pid)

        assignment = Assignment(
            ops=[op],
            cpu=cpu,
            ram=ram,
            priority=pri,
            pool_id=pid,
            pipeline_id=p.pipeline_id,
        )
        assignments.append(assignment)

        # Note: We won't know container_id until results come back; s.running is updated there.
        # Some simulators may not provide container_id immediately, so preemption might be limited.
        # Still, keeping the structure allows future extension if container_id is available at submit time.

    # -----------------------------
    # Rebuild waiting queue: keep pipelines that are not terminal
    # -----------------------------
    new_waiting = []
    for p in s.waiting:
        if p.pipeline_id in s.terminal_pipelines:
            continue
        status = p.runtime_status()
        if status.is_pipeline_successful():
            s.terminal_pipelines.add(p.pipeline_id)
            continue
        new_waiting.append(p)
    s.waiting = new_waiting

    return suspensions, assignments
