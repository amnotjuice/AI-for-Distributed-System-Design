# policy_key: scheduler_none_033
# reasoning_effort: none
# model: gpt-5.2-2025-12-11
# llm_cost: 0.041395
# generation_seconds: 41.25
# generated_at: 2026-04-09T22:10:42.017925
@register_scheduler_init(key="scheduler_none_033")
def scheduler_none_033_init(s):
    """Priority-aware, failure-averse scheduler.

    Main ideas:
    - Strictly prefer QUERY then INTERACTIVE then BATCH, but add aging so batch makes progress.
    - Run at most one operator per pipeline at a time (reduces head-of-line blocking & contention).
    - Conservative RAM sizing with multiplicative backoff on OOM, to minimize repeat failures (720s penalty).
    - Mild CPU right-sizing: give more CPU to high priority when available; otherwise small CPU slices to keep concurrency.
    - Lightweight preemption: if high-priority work is waiting and we can't place it anywhere, suspend one low-priority
      running container to free headroom (minimize query/interactive latency).
    """
    s.waiting = []  # FIFO arrival order
    s.arrival_tick = {}  # pipeline_id -> tick first seen
    s.tick = 0

    # Per-(pipeline, op) RAM multiplier to avoid repeat OOMs
    s.op_ram_mult = {}  # (pipeline_id, op_id) -> multiplier >= 1.0

    # Track last observed successful resources for a pipeline (helps stable sizing)
    s.pipe_last_good = {}  # pipeline_id -> {"ram": float, "cpu": float, "pool_id": int}

    # Track running containers we started (best-effort; simulator may also start others via other policies)
    s.running = {}  # container_id -> {"pool_id": int, "priority": Priority, "pipeline_id": str/int}

    # A gentle aging factor for fairness (prevents indefinite starvation)
    s.aging_per_tick = 0.002  # small so priority dominates

    # Hard caps / floors for sizing
    s.min_cpu = 0.25
    s.max_cpu_frac_high = 1.0   # allow taking whole pool for query if empty
    s.max_cpu_frac_low = 0.5    # don't let batch monopolize an entire pool
    s.min_ram_frac = 0.05       # don't allocate too tiny RAM; still obey pool availability

    # OOM backoff knobs
    s.oom_mult = 1.6            # multiplicative increase on OOM
    s.oom_mult_cap = 8.0        # cap multiplier to avoid runaway
    s.success_decay = 0.98      # slight decay on success (keeps learning but avoids permanent bloat)


@register_scheduler(key="scheduler_none_033")
def scheduler_none_033(s, results, pipelines):
    """
    Scheduler step:
    - Ingest new pipelines.
    - Update learning signals from results (OOM backoff, success decay, running bookkeeping).
    - Decide suspensions if needed to admit high priority.
    - Assign ready ops across pools using priority+aging score and conservative sizing.
    """
    s.tick += 1

    # --- Helpers (defined inside to avoid imports / external dependencies) ---
    def _prio_weight(pr):
        # Higher is more important
        if pr == Priority.QUERY:
            return 100.0
        if pr == Priority.INTERACTIVE:
            return 50.0
        return 10.0

    def _is_high(pr):
        return pr in (Priority.QUERY, Priority.INTERACTIVE)

    def _op_id(op):
        # Best-effort stable key for operator; fallback to repr
        return getattr(op, "op_id", None) or getattr(op, "operator_id", None) or getattr(op, "id", None) or repr(op)

    def _pipeline_state(p):
        st = p.runtime_status()
        # drop completed or failed pipelines from consideration
        has_failures = st.state_counts.get(OperatorState.FAILED, 0) > 0
        if st.is_pipeline_successful() or has_failures:
            return st, True
        return st, False

    def _pick_ready_op(p):
        st = p.runtime_status()
        # require parents complete; pick exactly one op to avoid intra-pipeline parallelism (reduces interference)
        ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        if not ops:
            return None
        return ops[0]

    def _pipeline_has_running_op(p):
        st = p.runtime_status()
        # If any op is assigned/running/suspending, treat pipeline as currently active and avoid starting another
        active = 0
        active += st.state_counts.get(OperatorState.ASSIGNED, 0)
        active += st.state_counts.get(OperatorState.RUNNING, 0)
        active += st.state_counts.get(OperatorState.SUSPENDING, 0)
        return active > 0

    def _score_pipeline(p):
        # Higher score scheduled earlier: priority dominates; aging prevents starvation
        base = _prio_weight(p.priority)
        first_seen = s.arrival_tick.get(p.pipeline_id, s.tick)
        age = max(0, s.tick - first_seen)
        return base * (1.0 + s.aging_per_tick * age)

    def _choose_pool_for(p, cpu_need, ram_need):
        # Prefer a pool with enough headroom; among those choose max remaining cpu (helps latency for high-pri)
        best = None
        best_metric = None
        for pool_id in range(s.executor.num_pools):
            pool = s.executor.pools[pool_id]
            if pool.avail_cpu_pool >= cpu_need and pool.avail_ram_pool >= ram_need:
                metric = (pool.avail_cpu_pool, pool.avail_ram_pool)
                if best is None or metric > best_metric:
                    best = pool_id
                    best_metric = metric
        return best

    def _size_for(pool_id, p, op):
        pool = s.executor.pools[pool_id]
        avail_cpu = max(0.0, pool.avail_cpu_pool)
        avail_ram = max(0.0, pool.avail_ram_pool)

        # CPU sizing: query gets more when available; batch limited
        if p.priority == Priority.QUERY:
            cpu_target = max(s.min_cpu, min(avail_cpu, pool.max_cpu_pool * s.max_cpu_frac_high))
        elif p.priority == Priority.INTERACTIVE:
            cpu_target = max(s.min_cpu, min(avail_cpu, max(s.min_cpu, pool.max_cpu_pool * 0.5)))
        else:
            cpu_target = max(s.min_cpu, min(avail_cpu, max(s.min_cpu, pool.max_cpu_pool * s.max_cpu_frac_low)))

        # RAM sizing: allocate a reasonable chunk to reduce OOM risk.
        # We don't know true min; use conservative fraction plus learned multiplier.
        op_key = (p.pipeline_id, _op_id(op))
        mult = s.op_ram_mult.get(op_key, 1.0)

        # Start from last good if present; else from a modest fraction of pool
        last_good = s.pipe_last_good.get(p.pipeline_id)
        if last_good is not None:
            base_ram = min(avail_ram, max(pool.max_ram_pool * s.min_ram_frac, float(last_good.get("ram", 0.0))))
        else:
            # For high priority, be more conservative to avoid failures; for batch, smaller
            frac = 0.25 if _is_high(p.priority) else 0.15
            base_ram = min(avail_ram, max(pool.max_ram_pool * s.min_ram_frac, pool.max_ram_pool * frac))

        ram_target = min(avail_ram, max(pool.max_ram_pool * s.min_ram_frac, base_ram * mult))
        return cpu_target, ram_target

    def _need_preempt_for_high():
        # If any high-priority pipeline has a ready op but can't fit anywhere (even minimal),
        # attempt to preempt a low-priority running container.
        candidates = []
        for p in s.waiting:
            if not _is_high(p.priority):
                continue
            st, done = _pipeline_state(p)
            if done:
                continue
            if _pipeline_has_running_op(p):
                continue
            op = _pick_ready_op(p)
            if op is None:
                continue

            # Check feasibility across pools with minimal sizing
            for pool_id in range(s.executor.num_pools):
                pool = s.executor.pools[pool_id]
                min_cpu = min(max(s.min_cpu, pool.avail_cpu_pool), pool.avail_cpu_pool)
                min_ram = min(max(pool.max_ram_pool * s.min_ram_frac, pool.avail_ram_pool), pool.avail_ram_pool)
                # If pool has essentially no resources, skip
                if min_cpu <= 0 or min_ram <= 0:
                    continue
            # We'll instead decide by attempting placement with real sizing; if no pool can fit, preempt.
            can_place = False
            for pool_id in range(s.executor.num_pools):
                pool = s.executor.pools[pool_id]
                if pool.avail_cpu_pool <= 0 or pool.avail_ram_pool <= 0:
                    continue
                cpu_t, ram_t = _size_for(pool_id, p, op)
                if pool.avail_cpu_pool >= cpu_t and pool.avail_ram_pool >= ram_t:
                    can_place = True
                    break
            if not can_place:
                candidates.append(p)
        return candidates

    def _pick_preempt_victim():
        # Preempt one running BATCH container if possible; otherwise INTERACTIVE (never QUERY).
        # Choose any known running container; since we don't have progress, keep it simple.
        victim = None
        victim_rank = None
        for cid, meta in s.running.items():
            pr = meta.get("priority")
            if pr == Priority.QUERY:
                continue
            # Prefer preempting batch over interactive
            rank = 0 if pr == Priority.BATCH_PIPELINE else 1
            if victim is None or rank < victim_rank:
                victim = (cid, meta.get("pool_id"))
                victim_rank = rank
        return victim

    # --- Ingest new pipelines ---
    for p in pipelines:
        s.waiting.append(p)
        if p.pipeline_id not in s.arrival_tick:
            s.arrival_tick[p.pipeline_id] = s.tick

    # --- Process results: update learning & bookkeeping ---
    for r in results:
        # Container finished (success or failure). Remove from running map.
        if getattr(r, "container_id", None) in s.running:
            s.running.pop(r.container_id, None)

        # Update RAM multiplier on OOM-like failures.
        # We only have r.error string; use substring heuristics.
        if r.failed():
            err = (r.error or "")
            is_oom = ("oom" in err.lower()) or ("out of memory" in err.lower()) or ("memory" in err.lower() and "alloc" in err.lower())
            if is_oom:
                for op in (r.ops or []):
                    op_key = (getattr(r, "pipeline_id", None) or getattr(op, "pipeline_id", None), _op_id(op))
                    # If pipeline_id not available on result/op, fall back to None-key; still helps a bit.
                    mult = s.op_ram_mult.get(op_key, 1.0)
                    s.op_ram_mult[op_key] = min(s.oom_mult_cap, mult * s.oom_mult)
        else:
            # On success, record "last good" sizing for the pipeline and slightly decay any multiplier.
            pid = getattr(r, "pipeline_id", None)
            if pid is not None:
                s.pipe_last_good[pid] = {"ram": float(r.ram), "cpu": float(r.cpu), "pool_id": int(r.pool_id)}
            for op in (r.ops or []):
                op_key = (pid, _op_id(op))
                if op_key in s.op_ram_mult:
                    s.op_ram_mult[op_key] = max(1.0, s.op_ram_mult[op_key] * s.success_decay)

    # --- Remove completed/failed pipelines from waiting (avoid queue bloat) ---
    new_waiting = []
    for p in s.waiting:
        _, done = _pipeline_state(p)
        if not done:
            new_waiting.append(p)
    s.waiting = new_waiting

    # Early exit if nothing to do
    if not s.waiting and not results and not pipelines:
        return [], []

    suspensions = []
    assignments = []

    # --- Preemption: only when necessary for high priority admission ---
    high_blocked = _need_preempt_for_high()
    if high_blocked:
        victim = _pick_preempt_victim()
        if victim is not None:
            cid, pool_id = victim
            suspensions.append(Suspend(container_id=cid, pool_id=pool_id))
            # Best-effort: remove immediately so we don't preempt repeatedly
            s.running.pop(cid, None)

    # --- Build candidate pipelines list sorted by priority+aging score ---
    # Also enforce: at most one in-flight op per pipeline.
    candidates = []
    for p in s.waiting:
        if _pipeline_has_running_op(p):
            continue
        op = _pick_ready_op(p)
        if op is None:
            continue
        candidates.append((p, op, _score_pipeline(p)))
    candidates.sort(key=lambda x: x[2], reverse=True)

    # --- Assign across pools ---
    # Greedy fill: for each pool, attempt to place best-scoring candidate that fits; repeat.
    # This tends to prioritize query/interactive latency while still making progress for others via aging.
    made_progress = True
    while made_progress:
        made_progress = False
        if not candidates:
            break

        for pool_id in range(s.executor.num_pools):
            pool = s.executor.pools[pool_id]
            if pool.avail_cpu_pool <= 0 or pool.avail_ram_pool <= 0:
                continue

            # Find best candidate that fits in this pool
            chosen_idx = None
            chosen_cpu = None
            chosen_ram = None
            for idx, (p, op, _sc) in enumerate(candidates):
                cpu_t, ram_t = _size_for(pool_id, p, op)
                if pool.avail_cpu_pool >= cpu_t and pool.avail_ram_pool >= ram_t:
                    chosen_idx = idx
                    chosen_cpu, chosen_ram = cpu_t, ram_t
                    break

            if chosen_idx is None:
                continue

            p, op, _sc = candidates.pop(chosen_idx)

            assignment = Assignment(
                ops=[op],
                cpu=chosen_cpu,
                ram=chosen_ram,
                priority=p.priority,
                pool_id=pool_id,
                pipeline_id=p.pipeline_id,
            )
            assignments.append(assignment)
            made_progress = True

            # Note: executor pool availability will be updated by simulator after applying assignments.
            # We still track running container ids only when results come back; but we can record intent
            # if container_id were known (it's not at assignment time). We'll instead track on results.

            # Avoid launching more than one op per pipeline in the same tick; already ensured by candidates list.

    return suspensions, assignments
