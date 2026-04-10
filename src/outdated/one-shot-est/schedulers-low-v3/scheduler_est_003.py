# policy_key: scheduler_est_003
# reasoning_effort: low
# model: gpt-5.2-2025-12-11
# llm_cost: 0.044631
# generation_seconds: 47.84
# generated_at: 2026-04-03T01:06:27.713699
@register_scheduler_init(key="scheduler_est_003")
def scheduler_est_003_init(s):
    """Priority-aware FIFO with conservative, incremental improvements over naive FIFO.

    Main ideas:
    - Maintain per-priority waiting queues (QUERY > INTERACTIVE > BATCH_PIPELINE).
    - Schedule only one ready operator per decision per pool, but always prefer higher priority.
    - Basic preemption: if a high-priority op can't fit, suspend lowest-priority running containers
      in that pool until it fits (or we run out of preemptable work).
    - Use optional per-operator RAM estimate (op.estimate.mem_peak_gb) with a small safety factor.
      On OOM-like failures, increase the RAM request for that operator on retry (RAM backoff).
    - Simple CPU sizing by priority (higher priority gets more CPU; batch is capped).
    """
    from collections import deque

    # Waiting pipelines by priority. Pipelines can appear multiple times; we dedupe per tick.
    s.q_query = deque()
    s.q_interactive = deque()
    s.q_batch = deque()

    # Track which pipelines are currently enqueued to reduce duplicates.
    s.enqueued = set()  # pipeline_id

    # Track RAM backoff per operator identity (best-effort).
    # Keyed by (pipeline_id, id(op)) => ram_gb
    s.op_ram_override_gb = {}

    # Track running containers so we can preempt deterministically.
    # container_id => dict(pool_id, priority, cpu, ram, pipeline_id)
    s.running = {}

    # Soft tuning knobs
    s.ram_safety = 1.10
    s.ram_backoff_mult = 1.60
    s.ram_backoff_add_gb = 0.5

    # CPU fractions by priority
    # (These are fractions of pool max CPU, then clipped by current availability)
    s.cpu_frac_query = 1.00
    s.cpu_frac_interactive = 0.75
    s.cpu_frac_batch = 0.50

    # Batch should not take the last sliver if higher-priority is waiting
    s.keep_cpu_headroom_if_hp_waiting = 0.10  # fraction of pool max cpu
    s.keep_ram_headroom_if_hp_waiting = 0.10  # fraction of pool max ram


@register_scheduler(key="scheduler_est_003")
def scheduler_est_003_scheduler(s, results, pipelines):
    """
    Priority-aware scheduler with basic preemption and RAM-estimate-driven sizing.

    Returns:
      (suspensions, assignments)
    """
    from collections import defaultdict

    def _prio_rank(p):
        # Lower is higher priority
        if p == Priority.QUERY:
            return 0
        if p == Priority.INTERACTIVE:
            return 1
        return 2  # Priority.BATCH_PIPELINE (and any others treated as batch)

    def _enqueue_pipeline(p):
        if p.pipeline_id in s.enqueued:
            return
        s.enqueued.add(p.pipeline_id)
        if p.priority == Priority.QUERY:
            s.q_query.append(p)
        elif p.priority == Priority.INTERACTIVE:
            s.q_interactive.append(p)
        else:
            s.q_batch.append(p)

    def _dequeue_from_queue(q):
        # Pop left, but skip completed pipelines; allow failed ops to retry.
        while q:
            p = q.popleft()
            s.enqueued.discard(p.pipeline_id)

            st = p.runtime_status()
            if st.is_pipeline_successful():
                continue
            return p
        return None

    def _requeue_pipeline(p):
        # Re-enqueue if still not finished (avoid duplicates via s.enqueued)
        st = p.runtime_status()
        if st.is_pipeline_successful():
            return
        _enqueue_pipeline(p)

    def _has_high_priority_waiting():
        return bool(s.q_query) or bool(s.q_interactive)

    def _iter_priority_queues():
        # Always try higher priority first
        yield s.q_query
        yield s.q_interactive
        yield s.q_batch

    def _pick_next_ready_op(p):
        st = p.runtime_status()
        # Use ASSIGNABLE_STATES and require parents complete: only schedule ready ops.
        op_list = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        if not op_list:
            return None
        # Keep it simple: schedule one operator at a time (reduces interference and tail latency).
        return op_list[0]

    def _is_oom_error(err):
        if err is None:
            return False
        msg = str(err).lower()
        return ("oom" in msg) or ("out of memory" in msg) or ("memory" in msg and "exceed" in msg)

    def _ram_request_gb(pipeline_id, op, pool_max_ram_gb):
        # Start with override if we've seen OOMs for this operator before.
        key = (pipeline_id, id(op))
        if key in s.op_ram_override_gb:
            return min(pool_max_ram_gb, max(0.1, float(s.op_ram_override_gb[key])))

        # Else use estimate if present.
        est = getattr(op, "estimate", None)
        est_mem = None
        if est is not None:
            est_mem = getattr(est, "mem_peak_gb", None)

        if est_mem is not None:
            try:
                req = float(est_mem) * float(s.ram_safety)
                # Keep a small minimum to avoid degenerate zeros
                return min(pool_max_ram_gb, max(0.1, req))
            except Exception:
                pass

        # Fallback: pick a modest default that doesn't monopolize the pool.
        # (We rely on OOM backoff to grow it as needed.)
        return max(0.5, min(pool_max_ram_gb, 0.25 * pool_max_ram_gb))

    def _cpu_request(p_priority, pool_max_cpu, avail_cpu):
        if p_priority == Priority.QUERY:
            target = s.cpu_frac_query * pool_max_cpu
        elif p_priority == Priority.INTERACTIVE:
            target = s.cpu_frac_interactive * pool_max_cpu
        else:
            target = s.cpu_frac_batch * pool_max_cpu
        # Clip to availability, and ensure positive.
        return max(0.0, min(float(avail_cpu), float(target)))

    def _maybe_reserve_headroom_for_hp(pool, avail_cpu, avail_ram):
        # If high priority is waiting, don't let batch consume the last headroom.
        if not _has_high_priority_waiting():
            return avail_cpu, avail_ram

        min_cpu_left = s.keep_cpu_headroom_if_hp_waiting * float(pool.max_cpu_pool)
        min_ram_left = s.keep_ram_headroom_if_hp_waiting * float(pool.max_ram_pool)

        # We do not forcefully reclaim; we only reduce what we are willing to allocate now.
        return max(0.0, float(avail_cpu) - min_cpu_left), max(0.0, float(avail_ram) - min_ram_left)

    def _preempt_to_fit(pool_id, need_cpu, need_ram):
        # Suspend lowest priority first within the same pool, until we (likely) have room.
        # We estimate freed resources from what we assigned earlier.
        susp = []
        pool = s.executor.pools[pool_id]

        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)

        if need_cpu <= avail_cpu and need_ram <= avail_ram:
            return susp

        # Gather running containers in this pool sorted by (priority rank desc) => batch first
        candidates = []
        for cid, info in list(s.running.items()):
            if info.get("pool_id") != pool_id:
                continue
            candidates.append((info.get("priority"), cid, info))
        candidates.sort(key=lambda x: _prio_rank(x[0]), reverse=True)

        for prio, cid, info in candidates:
            # Never preempt QUERY; try to keep interactive stable too if possible.
            if prio == Priority.QUERY:
                continue
            if prio == Priority.INTERACTIVE and _has_high_priority_waiting():
                # If a query is waiting, interactive is preemptable; otherwise avoid.
                if not s.q_query:
                    continue

            susp.append(Suspend(container_id=cid, pool_id=pool_id))
            # Estimate resource release
            avail_cpu += float(info.get("cpu", 0.0))
            avail_ram += float(info.get("ram", 0.0))

            # Remove from running map so we don't try to preempt it twice
            s.running.pop(cid, None)

            if need_cpu <= avail_cpu and need_ram <= avail_ram:
                break

        return susp

    # --- Ingest new pipelines ---
    for p in pipelines:
        _enqueue_pipeline(p)

    # --- Process results: update running map and handle OOM backoff ---
    # Also requeue pipelines that still have work.
    pipeline_by_id = {}
    for q in (s.q_query, s.q_interactive, s.q_batch):
        for p in q:
            pipeline_by_id[p.pipeline_id] = p

    for r in results:
        # A container finished (success or failure): stop tracking it as running.
        if getattr(r, "container_id", None) is not None:
            s.running.pop(r.container_id, None)

        # If failed due to OOM, bump RAM override for those ops so retry requests more RAM.
        if r.failed() and _is_oom_error(getattr(r, "error", None)):
            pid = getattr(r, "pipeline_id", None)
            if pid is None:
                # If pipeline_id isn't on result, try to infer from tracked running info (best-effort).
                # Not always possible; skip if unknown.
                pid = None

            # Apply backoff per op if we can identify it.
            ops = getattr(r, "ops", None) or []
            for op in ops:
                if pid is None:
                    # Without pipeline_id, we can still key by just id(op), but that may collide.
                    key = ("unknown", id(op))
                else:
                    key = (pid, id(op))

                prev = s.op_ram_override_gb.get(key)
                base = float(prev) if prev is not None else float(getattr(r, "ram", 0.0) or 0.0)
                if base <= 0.0:
                    base = 1.0
                bumped = base * float(s.ram_backoff_mult) + float(s.ram_backoff_add_gb)
                s.op_ram_override_gb[key] = bumped

        # Requeue the pipeline if we can find it in this tick's known set
        # (Pipelines are also requeued when we pop them for consideration.)
        rid_pid = getattr(r, "pipeline_id", None)
        if rid_pid is not None and rid_pid in pipeline_by_id:
            _requeue_pipeline(pipeline_by_id[rid_pid])

    # Early exit if nothing changed
    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    # --- Scheduling loop: attempt one assignment per pool per tick ---
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)
        if avail_cpu <= 0.0 or avail_ram <= 0.0:
            continue

        # Try to find best (highest priority) runnable op by probing queues.
        picked = None  # (pipeline, op)
        picked_queue = None
        for q in _iter_priority_queues():
            p = _dequeue_from_queue(q)
            if p is None:
                continue

            op = _pick_next_ready_op(p)
            if op is None:
                # No ready op now; requeue and continue.
                _requeue_pipeline(p)
                continue

            picked = (p, op)
            picked_queue = q
            break

        if picked is None:
            continue

        p, op = picked

        # Determine request sizes
        req_ram = _ram_request_gb(p.pipeline_id, op, float(pool.max_ram_pool))

        # CPU sizing: for batch, reserve headroom if higher priority is waiting
        eff_avail_cpu, eff_avail_ram = avail_cpu, avail_ram
        if p.priority == Priority.BATCH_PIPELINE:
            eff_avail_cpu, eff_avail_ram = _maybe_reserve_headroom_for_hp(pool, eff_avail_cpu, eff_avail_ram)

        req_cpu = _cpu_request(p.priority, float(pool.max_cpu_pool), eff_avail_cpu)
        if req_cpu <= 0.0:
            # Can't run anything useful; requeue and move on
            _requeue_pipeline(p)
            continue

        # If doesn't fit, try preempting only for high priority work
        if req_ram > eff_avail_ram or req_cpu > eff_avail_cpu:
            if p.priority in (Priority.QUERY, Priority.INTERACTIVE):
                suspensions.extend(_preempt_to_fit(pool_id, req_cpu, req_ram))
                # Re-read pool availability is not possible without simulator updates; instead
                # rely on our preemption resource estimate by proceeding.
                # If still can't fit, we will requeue.
                # Note: we don't have updated avail_* here, but preemption estimate is accounted in function.
            else:
                # Don't preempt for batch
                _requeue_pipeline(p)
                continue

        # After preemption attempt, check against actual current pool availability (best effort).
        # If preemptions happen, simulator should free resources; but within this tick,
        # avail values might still be stale, so we accept assignment if it fits the stale numbers
        # OR we preempted something (optimistic).
        fits_now = (req_cpu <= float(pool.avail_cpu_pool)) and (req_ram <= float(pool.avail_ram_pool))
        optimistic_after_preempt = len(suspensions) > 0 and p.priority in (Priority.QUERY, Priority.INTERACTIVE)
        if not fits_now and not optimistic_after_preempt:
            _requeue_pipeline(p)
            continue

        # Create assignment for single op
        assignment = Assignment(
            ops=[op],
            cpu=req_cpu,
            ram=req_ram,
            priority=p.priority,
            pool_id=pool_id,
            pipeline_id=p.pipeline_id,
        )
        assignments.append(assignment)

        # Requeue pipeline for its subsequent ops (if any)
        _requeue_pipeline(p)

        # Track running container after assignment if container_id becomes available in results.
        # We don't have container_id yet, so store a pending record keyed by (pool_id, pipeline_id, id(op)).
        # This is best-effort; we primarily use r.container_id-based tracking from results.
        # (If the simulator provides container_id immediately somewhere else, this would be updated there.)
        # No-op here.

    return suspensions, assignments
