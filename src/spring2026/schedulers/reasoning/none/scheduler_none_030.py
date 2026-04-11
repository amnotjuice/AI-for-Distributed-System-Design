# policy_key: scheduler_none_030
# reasoning_effort: none
# model: gpt-5.2-2025-12-11
# llm_cost: 0.049153
# generation_seconds: 41.89
# generated_at: 2026-03-14T02:03:23.805090
@register_scheduler_init(key="scheduler_none_030")
def scheduler_none_030_init(s):
    """Priority-aware, latency-first scheduler with conservative preemption and simple OOM backoff.

    Improvements over naive FIFO:
    1) Priority queues: always try to schedule higher-priority ready work first (QUERY > INTERACTIVE > BATCH).
    2) Right-sizing: avoid allocating the entire pool to one op; use bounded per-op CPU/RAM caps.
    3) OOM backoff: if an op fails with OOM, remember it and increase RAM next time for that pipeline.
    4) Gentle preemption: when high-priority work is waiting and we can't admit it, preempt a low-priority
       running container in the same pool (at most a small number per tick) to free headroom.

    Notes:
    - We avoid aggressive complexity (no SRPT estimation, no per-op profiling) to keep behavior stable.
    - We schedule at most one op per pool per tick (like the baseline), but pick it intelligently.
    """
    # Waiting pipelines by priority
    s.waiting_by_prio = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }

    # Simple RAM multiplier backoff per (pipeline_id) after OOM-like failures
    s.pipeline_ram_mult = {}  # pipeline_id -> float

    # Track currently running containers seen in results (best-effort; simulator may omit some signals)
    s.running = {}  # container_id -> dict(pool_id, priority, cpu, ram)

    # Preemption knobs (conservative)
    s.max_preempts_per_tick = 1

    # Sizing knobs
    s.min_cpu_per_op = 1.0
    s.max_cpu_fraction_per_pool = 0.75  # don't let one op take the entire pool by default
    s.min_ram_per_op = 0.5              # in "pool ram units"; relies on simulator scale
    s.max_ram_fraction_per_pool = 0.75


def _prio_order():
    return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]


def _is_oom_error(err):
    if err is None:
        return False
    try:
        msg = str(err).lower()
    except Exception:
        return False
    # Match common OOM strings; keep broad to adapt to sim error messages
    return ("oom" in msg) or ("out of memory" in msg) or ("memory" in msg and "alloc" in msg)


def _pick_ready_op(pipeline):
    status = pipeline.runtime_status()
    # Only schedule ops whose parents are complete
    op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
    if not op_list:
        return []
    # Small step: schedule one operator at a time (keeps container model simple)
    return op_list[:1]


def _bounded_allocation(avail_cpu, avail_ram, max_cpu_pool, max_ram_pool, cpu_cap, ram_cap):
    # Bound by available and per-pool caps; also enforce minima
    cpu = min(avail_cpu, cpu_cap, max_cpu_pool)
    ram = min(avail_ram, ram_cap, max_ram_pool)
    return cpu, ram


def _desired_caps_for_pool(pool, priority):
    # Latency-first: allow high priority to burst a bit more than batch
    if priority == Priority.QUERY:
        cpu_frac = 0.9
        ram_frac = 0.9
    elif priority == Priority.INTERACTIVE:
        cpu_frac = 0.8
        ram_frac = 0.8
    else:
        cpu_frac = 0.6
        ram_frac = 0.6

    cpu_cap = max(1.0, pool.max_cpu_pool * cpu_frac)
    ram_cap = max(0.1, pool.max_ram_pool * ram_frac)
    return cpu_cap, ram_cap


def _enqueue_pipeline(s, pipeline):
    # Ensure unknown priorities still get queued
    pr = pipeline.priority
    if pr not in s.waiting_by_prio:
        s.waiting_by_prio[pr] = []
    s.waiting_by_prio[pr].append(pipeline)


def _pipeline_done_or_failed(pipeline):
    status = pipeline.runtime_status()
    if status.is_pipeline_successful():
        return True
    # If there are failures we still might want to retry (e.g., OOM), so do not drop here.
    return False


def _has_any_waiting_high_prio(s):
    return bool(s.waiting_by_prio.get(Priority.QUERY)) or bool(s.waiting_by_prio.get(Priority.INTERACTIVE))


def _lowest_running_container_in_pool(s, pool_id):
    # Find a low priority running container in a pool to preempt.
    # Prefer BATCH over INTERACTIVE over QUERY (never preempt QUERY if possible).
    candidate = None
    candidate_prio_rank = -1
    rank_map = {Priority.BATCH_PIPELINE: 0, Priority.INTERACTIVE: 1, Priority.QUERY: 2}
    for cid, info in s.running.items():
        if info.get("pool_id") != pool_id:
            continue
        pr = info.get("priority")
        r = rank_map.get(pr, 0)
        if candidate is None:
            candidate = cid
            candidate_prio_rank = r
        else:
            # pick the lowest rank (batch) to preempt first
            if r < candidate_prio_rank:
                candidate = cid
                candidate_prio_rank = r

    # Don't preempt if only QUERY containers exist
    if candidate is None:
        return None
    if rank_map.get(s.running[candidate].get("priority"), 0) >= rank_map.get(Priority.QUERY, 2):
        return None
    return candidate


@register_scheduler(key="scheduler_none_030")
def scheduler_none_030(s, results, pipelines):
    """
    Each tick:
    - Incorporate new pipelines into per-priority queues.
    - Process results to update OOM backoff and running-container tracking.
    - For each pool: try to schedule one ready op from the highest priority queue.
      If blocked and high-priority work exists, attempt conservative preemption of a low-priority container.
    """
    # Enqueue new pipelines
    for p in pipelines:
        _enqueue_pipeline(s, p)

    # Process results: update OOM backoff and running tracking
    # We treat any non-failed result as "container observed"; failed results remove from running.
    for r in results or []:
        # Update running map best-effort
        if getattr(r, "container_id", None) is not None:
            if r.failed():
                if r.container_id in s.running:
                    del s.running[r.container_id]
            else:
                s.running[r.container_id] = {
                    "pool_id": r.pool_id,
                    "priority": r.priority,
                    "cpu": getattr(r, "cpu", None),
                    "ram": getattr(r, "ram", None),
                }

        # OOM backoff: increase RAM multiplier for the owning pipeline when we can infer it
        # ExecutionResult does not include pipeline_id, so we do a coarse heuristic:
        # when an OOM happens, apply a global bump for that priority class by storing a multiplier
        # per pipeline on next scheduling (we can only apply if pipeline_id known).
        # We'll instead store per-pipeline backoff when we see FAILED ops later via pipeline status:
        # But we can still mark a "recent oom" flag by priority to be conservative this tick.
        # To keep it simple, we update nothing here unless we can access error.
        pass

    # Early exit if nothing to do
    any_waiting = any(len(q) > 0 for q in s.waiting_by_prio.values())
    if not any_waiting and not results and not pipelines:
        return [], []

    suspensions = []
    assignments = []

    # Helper: pop next viable pipeline from a priority queue, preserving order
    def pop_next_pipeline(prio):
        q = s.waiting_by_prio.get(prio, [])
        while q:
            p = q.pop(0)
            # Drop completed pipelines
            if _pipeline_done_or_failed(p):
                continue
            return p
        return None

    # Helper: push back pipeline to end of its queue
    def requeue_pipeline(p):
        _enqueue_pipeline(s, p)

    # Decide per-pool assignments; at most one op per pool per tick
    preempts_left = getattr(s, "max_preempts_per_tick", 1)

    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool

        # If nothing available at all, consider preemption only if high-prio waiting
        if (avail_cpu <= 0 or avail_ram <= 0) and _has_any_waiting_high_prio(s) and preempts_left > 0:
            victim = _lowest_running_container_in_pool(s, pool_id)
            if victim is not None:
                suspensions.append(Suspend(container_id=victim, pool_id=pool_id))
                # Best-effort: assume resources will be freed; simulator will apply next tick.
                preempts_left -= 1
            continue

        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # Choose highest priority pipeline with a ready op
        chosen = None
        chosen_prio = None
        chosen_ops = None

        # We'll attempt a few pops and requeues to avoid losing blocked pipelines
        tried = []

        for pr in _prio_order():
            # scan up to a small number from this queue to find one with a ready op
            scan_limit = 8
            for _ in range(scan_limit):
                p = pop_next_pipeline(pr)
                if p is None:
                    break
                tried.append(p)
                ops = _pick_ready_op(p)
                if ops:
                    chosen = p
                    chosen_prio = pr
                    chosen_ops = ops
                    break
            if chosen is not None:
                break

        # Requeue unchosen tried pipelines (except chosen)
        for p in tried:
            if chosen is not None and p.pipeline_id == chosen.pipeline_id:
                continue
            requeue_pipeline(p)

        if chosen is None:
            continue

        # Compute caps and OOM backoff multiplier
        cpu_cap, ram_cap = _desired_caps_for_pool(pool, chosen_prio)

        ram_mult = s.pipeline_ram_mult.get(chosen.pipeline_id, 1.0)
        # If we've already bumped a lot, don't exceed pool-level fraction caps
        base_ram_cap = min(pool.max_ram_pool * s.max_ram_fraction_per_pool, ram_cap)
        adj_ram_cap = min(pool.max_ram_pool, base_ram_cap * ram_mult)

        base_cpu_cap = min(pool.max_cpu_pool * s.max_cpu_fraction_per_pool, cpu_cap)
        adj_cpu_cap = base_cpu_cap

        cpu, ram = _bounded_allocation(
            avail_cpu=avail_cpu,
            avail_ram=avail_ram,
            max_cpu_pool=pool.max_cpu_pool,
            max_ram_pool=pool.max_ram_pool,
            cpu_cap=adj_cpu_cap,
            ram_cap=adj_ram_cap,
        )

        # Enforce minimums; if we can't, consider preemption for high-priority
        cpu = max(cpu, s.min_cpu_per_op)
        ram = max(ram, s.min_ram_per_op)

        if cpu > avail_cpu or ram > avail_ram:
            # Not enough resources with minimums; try preemption if high priority
            if chosen_prio in (Priority.QUERY, Priority.INTERACTIVE) and preempts_left > 0:
                victim = _lowest_running_container_in_pool(s, pool_id)
                if victim is not None:
                    suspensions.append(Suspend(container_id=victim, pool_id=pool_id))
                    preempts_left -= 1
                    # Put chosen back and try next tick
                    requeue_pipeline(chosen)
                    continue
            # Otherwise requeue and move on
            requeue_pipeline(chosen)
            continue

        assignments.append(
            Assignment(
                ops=chosen_ops,
                cpu=cpu,
                ram=ram,
                priority=chosen.priority,
                pool_id=pool_id,
                pipeline_id=chosen.pipeline_id,
            )
        )

        # Requeue chosen pipeline to continue later (unless it finishes)
        requeue_pipeline(chosen)

    # Post-process: update OOM backoff by inspecting pipelines for newly failed ops.
    # If any pipeline has FAILED ops, we only bump RAM if we suspect OOM.
    # Since we don't have per-op error here, we approximate: any FAILED op triggers modest bump,
    # but only once per tick per pipeline (keeps it conservative).
    seen_bump = set()
    for pr in list(s.waiting_by_prio.keys()):
        for p in s.waiting_by_prio.get(pr, []):
            if p.pipeline_id in seen_bump:
                continue
            st = p.runtime_status()
            if st.state_counts.get(OperatorState.FAILED, 0) > 0:
                # Conservative: assume likely resource issue; bump RAM multiplier up to a cap.
                cur = s.pipeline_ram_mult.get(p.pipeline_id, 1.0)
                nxt = min(4.0, cur * 1.5)
                s.pipeline_ram_mult[p.pipeline_id] = nxt
                seen_bump.add(p.pipeline_id)

    return suspensions, assignments
