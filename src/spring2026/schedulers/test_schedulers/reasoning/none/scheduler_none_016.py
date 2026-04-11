# policy_key: scheduler_none_016
# reasoning_effort: none
# model: gpt-5.2-2025-12-11
# llm_cost: 0.055227
# generation_seconds: 36.86
# generated_at: 2026-04-09T21:57:13.398209
@register_scheduler_init(key="scheduler_none_016")
def scheduler_none_016_init(s):
    """Priority-aware, failure-averse scheduler with gentle preemption and OOM backoff.

    Main ideas:
    - Keep per-pipeline FIFO queues per priority (query > interactive > batch) to protect tail latency.
    - Use conservative CPU/RAM sizing by default; on OOM, increase RAM for that pipeline with exponential backoff.
    - Preempt (suspend) at most one low-priority container per tick when high-priority work is waiting and
      there isn't enough headroom in any pool to place it.
    - Avoid starvation via aging: if a pipeline waits long enough, it is temporarily boosted.
    """
    # Waiting queues by priority (pipeline_id -> pipeline); preserve FIFO using insertion order lists.
    s.q_query = []
    s.q_interactive = []
    s.q_batch = []

    # Map pipeline_id -> pipeline for de-dup and updates
    s.waiting = {}

    # Per-pipeline RAM backoff multiplier (>=1.0) and last seen op failure type
    s.ram_mult = {}          # pipeline_id -> float
    s.last_oom_ts = {}       # pipeline_id -> float (logical ticks)
    s.pipeline_arrival_ts = {}  # pipeline_id -> float (logical ticks)
    s.tick = 0

    # Track running containers to enable targeted preemption decisions.
    # container_id -> dict(priority, pool_id)
    s.running = {}

    # Parameters (tuned to be conservative and avoid failures)
    s.base_ram_frac = 0.35   # fraction of pool RAM used for an assignment by default
    s.base_cpu_frac = 0.50   # fraction of pool CPU used for an assignment by default
    s.max_ram_frac = 0.95
    s.max_cpu_frac = 1.00

    # Aging: after this many ticks waiting, boost one level (batch->interactive->query)
    s.aging_ticks = 40

    # Preemption limits
    s.max_preemptions_per_tick = 1

    # OOM backoff
    s.oom_backoff_factor = 1.8
    s.oom_backoff_cap = 8.0


def _p_rank(priority):
    # Higher is more important
    if priority == Priority.QUERY:
        return 3
    if priority == Priority.INTERACTIVE:
        return 2
    return 1


def _enqueue_pipeline(s, p):
    pid = p.pipeline_id
    if pid not in s.pipeline_arrival_ts:
        s.pipeline_arrival_ts[pid] = s.tick
    s.waiting[pid] = p
    # Keep original arrival order within each priority queue by appending if not present.
    q = s.q_batch
    if p.priority == Priority.QUERY:
        q = s.q_query
    elif p.priority == Priority.INTERACTIVE:
        q = s.q_interactive
    if pid not in q:
        q.append(pid)


def _drop_if_terminal(pipeline):
    st = pipeline.runtime_status()
    if st.is_pipeline_successful():
        return True
    # If any failed op exists, we do NOT drop (720s penalty is brutal); we allow retry with larger RAM.
    return False


def _get_assignable_ops(pipeline):
    st = pipeline.runtime_status()
    # Prefer parents-complete ops to make progress; take only one op to reduce interference and failures.
    ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
    if ops:
        return ops
    # If nothing is ready with parents complete, don't force execution.
    return []


def _boosted_priority(s, pipeline):
    """Apply aging boost to reduce starvation risk without sacrificing high-priority latency too much."""
    pid = pipeline.pipeline_id
    waited = s.tick - s.pipeline_arrival_ts.get(pid, s.tick)
    pr = pipeline.priority
    if waited < s.aging_ticks:
        return pr
    # One-level boost
    if pr == Priority.BATCH_PIPELINE:
        return Priority.INTERACTIVE
    if pr == Priority.INTERACTIVE:
        return Priority.QUERY
    return pr


def _choose_pool_for_priority(s, desired_priority):
    """Pick the pool with most available RAM among those with some CPU, to reduce OOM risk."""
    best_pool_id = None
    best_score = -1
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        if pool.avail_cpu_pool <= 0 or pool.avail_ram_pool <= 0:
            continue
        # Prefer RAM headroom; tie-break on CPU headroom.
        score = pool.avail_ram_pool * 1000.0 + pool.avail_cpu_pool
        if score > best_score:
            best_score = score
            best_pool_id = pool_id
    return best_pool_id


def _sizing_for_pool(s, pool_id, pipeline_id):
    """Compute conservative cpu/ram request for one operator in a pool, with per-pipeline RAM backoff."""
    pool = s.executor.pools[pool_id]
    mult = s.ram_mult.get(pipeline_id, 1.0)

    # Conservative default: allocate a fraction of pool resources, leaving headroom for high-priority arrivals.
    cpu = max(1.0, min(pool.avail_cpu_pool, pool.max_cpu_pool * s.base_cpu_frac, pool.avail_cpu_pool * 0.9))
    ram_target = pool.max_ram_pool * s.base_ram_frac * mult

    # Clamp to what's available and a maximum fraction.
    ram_cap = min(pool.avail_ram_pool, pool.max_ram_pool * s.max_ram_frac)
    ram = max(1.0, min(ram_target, ram_cap))

    # If backoff wants more than we can give now, at least grab whatever we can (or skip if tiny).
    # Skipping tiny RAM reduces repeated OOM loops.
    if mult > 1.0 and ram < min(2.0, ram_cap * 0.25):
        return None, None
    return cpu, ram


def _need_preemption_for(s, desired_priority):
    """Return True if there is waiting high-priority work and no pool has enough headroom to start it."""
    # If any pool has meaningful headroom, avoid preemption.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        if pool.avail_cpu_pool >= 1.0 and pool.avail_ram_pool >= 1.0:
            return False
    return True


def _pick_preemption_candidate(s, min_priority_to_protect):
    """Pick one running container with lower priority to suspend."""
    # Prefer suspending batch first, then interactive, and avoid suspending queries.
    candidate = None
    candidate_rank = 10
    candidate_pool = None

    for cid, meta in s.running.items():
        pr = meta.get("priority", Priority.BATCH_PIPELINE)
        pool_id = meta.get("pool_id", 0)
        if _p_rank(pr) >= _p_rank(min_priority_to_protect):
            continue
        # Lower priority rank is better candidate to suspend.
        r = _p_rank(pr)
        if r < candidate_rank:
            candidate_rank = r
            candidate = cid
            candidate_pool = pool_id

    if candidate is None:
        return None
    return Suspend(candidate, candidate_pool)


@register_scheduler(key="scheduler_none_016")
def scheduler_none_016(s, results, pipelines):
    """
    Scheduler step:
    - Integrate new arrivals into per-priority FIFO queues.
    - Process execution results: on OOM, increase RAM multiplier and requeue pipeline; track running set.
    - Preempt at most one low-priority container if high-priority work is waiting and no headroom exists.
    - Make up to one assignment per pool per tick, picking highest effective (aged) priority pipeline first.
    """
    s.tick += 1

    # Ingest new pipelines
    for p in pipelines:
        _enqueue_pipeline(s, p)

    suspensions = []
    assignments = []

    # Process results: update running set; apply OOM backoff and requeue
    for r in results:
        # If we saw the container finish/fail, it is no longer running.
        if getattr(r, "container_id", None) is not None and r.container_id in s.running:
            # Completion/failure both remove from running
            del s.running[r.container_id]

        if r.failed():
            # Treat explicit OOM as a strong signal to increase RAM for the owning pipeline.
            err = (r.error or "").lower()
            is_oom = ("oom" in err) or ("out of memory" in err) or ("memory" in err and "killed" in err)

            pid = getattr(r, "pipeline_id", None)
            # pipeline_id may not be present; fall back to any op's pipeline_id if available.
            if pid is None:
                # Best-effort: infer from op metadata if present.
                try:
                    if r.ops and hasattr(r.ops[0], "pipeline_id"):
                        pid = r.ops[0].pipeline_id
                except Exception:
                    pid = None

            if pid is not None:
                # Backoff on OOM; on other failures, still backoff mildly to reduce repeat failures.
                if is_oom:
                    old = s.ram_mult.get(pid, 1.0)
                    new = min(s.oom_backoff_cap, max(1.0, old * s.oom_backoff_factor))
                    s.ram_mult[pid] = new
                    s.last_oom_ts[pid] = s.tick
                else:
                    # Mild increase to improve completion odds.
                    old = s.ram_mult.get(pid, 1.0)
                    s.ram_mult[pid] = min(s.oom_backoff_cap, max(1.0, old * 1.2))

                # Ensure pipeline stays queued for retry (do not drop; failures are heavily penalized).
                if pid in s.waiting:
                    p = s.waiting[pid]
                    _enqueue_pipeline(s, p)

    # Early exit if nothing changed
    if not pipelines and not results and not s.waiting:
        return [], []

    # Determine if high-priority work is waiting (consider aging boosts)
    def has_waiting_at_or_above(pr):
        for pid, p in s.waiting.items():
            if _drop_if_terminal(p):
                continue
            if _p_rank(_boosted_priority(s, p)) >= _p_rank(pr):
                ops = _get_assignable_ops(p)
                if ops:
                    return True
        return False

    # Gentle preemption: only if query/interactive is waiting and there is no headroom right now.
    preemptions_left = s.max_preemptions_per_tick
    if preemptions_left > 0 and (has_waiting_at_or_above(Priority.QUERY) or has_waiting_at_or_above(Priority.INTERACTIVE)):
        if _need_preemption_for(s, Priority.INTERACTIVE):
            # Protect at least interactive; preempt batch if possible.
            cand = _pick_preemption_candidate(s, Priority.INTERACTIVE)
            if cand is not None:
                suspensions.append(cand)
                preemptions_left -= 1

    # Build a unified ordered list of candidate pipeline_ids:
    # highest effective priority first; FIFO within each queue; include aging boosts by re-sorting.
    candidate_pids = []
    seen = set()

    # Gather from queues in their natural order
    for q in (s.q_query, s.q_interactive, s.q_batch):
        for pid in q:
            if pid in seen:
                continue
            if pid in s.waiting:
                candidate_pids.append(pid)
                seen.add(pid)

    # Sort by boosted priority rank (desc), then by arrival time (asc) for fairness
    candidate_pids.sort(
        key=lambda pid: (
            -_p_rank(_boosted_priority(s, s.waiting[pid])),
            s.pipeline_arrival_ts.get(pid, 0),
        )
    )

    # Try to schedule at most one op per pool
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        if pool.avail_cpu_pool <= 0 or pool.avail_ram_pool <= 0:
            continue

        # Find best pipeline for this pool (first fit in priority order)
        chosen_pid = None
        chosen_ops = None
        chosen_priority = None
        chosen_cpu = None
        chosen_ram = None

        for pid in candidate_pids:
            p = s.waiting.get(pid)
            if p is None:
                continue
            if _drop_if_terminal(p):
                continue

            ops = _get_assignable_ops(p)
            if not ops:
                continue

            cpu, ram = _sizing_for_pool(s, pool_id, pid)
            if cpu is None or ram is None:
                continue

            # Quick feasibility: require at least requested resources available now.
            if cpu > pool.avail_cpu_pool + 1e-9 or ram > pool.avail_ram_pool + 1e-9:
                continue

            chosen_pid = pid
            chosen_ops = ops
            chosen_priority = _boosted_priority(s, p)
            chosen_cpu = cpu
            chosen_ram = ram
            break

        if chosen_pid is None:
            continue

        assignment = Assignment(
            ops=chosen_ops,
            cpu=chosen_cpu,
            ram=chosen_ram,
            priority=chosen_priority,
            pool_id=pool_id,
            pipeline_id=chosen_pid,
        )
        assignments.append(assignment)

        # Note: we cannot know container_id until execution result; still remove from queue candidates
        # to avoid multiple assignments for same pipeline within this tick.
        candidate_pids = [pid for pid in candidate_pids if pid != chosen_pid]

    # Cleanup: remove completed pipelines from waiting map and queues opportunistically
    to_remove = []
    for pid, p in list(s.waiting.items()):
        st = p.runtime_status()
        if st.is_pipeline_successful():
            to_remove.append(pid)
    for pid in to_remove:
        s.waiting.pop(pid, None)
        for q in (s.q_query, s.q_interactive, s.q_batch):
            if pid in q:
                q.remove(pid)

    # Track running containers from results is handled above; but we can also record newly assigned as "pending running"
    # once their container_id appears in results. Nothing to do here.

    return suspensions, assignments
