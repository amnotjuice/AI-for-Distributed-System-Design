# policy_key: scheduler_none_042
# reasoning_effort: none
# model: gpt-5.2-2025-12-11
# llm_cost: 0.047569
# generation_seconds: 44.51
# generated_at: 2026-04-09T22:17:46.615811
@register_scheduler_init(key="scheduler_none_042")
def scheduler_none_042_init(s):
    """Priority + SLO-protecting scheduler with OOM-aware RAM retries and bounded preemption.

    Main ideas:
    - Keep separate queues per priority (QUERY > INTERACTIVE > BATCH) and schedule ready ops.
    - Reserve a slice of each pool for high priority (soft reservation): don't let BATCH consume
      the last headroom needed to start QUERY/INTERACTIVE quickly.
    - OOM/backoff: if an op fails (likely OOM), retry the pipeline with increased RAM request.
      This reduces the 720s penalty by improving completion rate.
    - Gentle preemption: if high-priority work is waiting and no pool can fit it, suspend some
      running BATCH/INTERACTIVE containers to free resources, with cooldown to reduce churn.
    - CPU sizing: allocate moderate CPU per assignment to avoid starving concurrency and reduce
      tail latency for many small interactive/query ops.
    """
    # Queues keyed by priority
    s.q_query = []
    s.q_interactive = []
    s.q_batch = []

    # Track per-pipeline RAM "multiplier" to reduce repeat OOMs
    s.pipeline_ram_mult = {}  # pipeline_id -> float

    # Track recently suspended containers to avoid thrash
    s.preempt_cooldown_until = {}  # (pool_id, container_id) -> sim_time

    # Simple time source (incremented every scheduler tick)
    s._tick = 0

    # Policy knobs (conservative, incremental over FIFO)
    s.max_ops_per_assignment = 1  # keep atomic and responsive
    s.base_ram_fraction = 0.6     # start by giving a fraction of available RAM, not all
    s.base_cpu_fraction = 0.5     # start by giving a fraction of available CPU, not all
    s.min_cpu = 1.0               # never allocate less than 1 vCPU when assigning
    s.min_ram = 0.25              # never allocate less than 0.25 GB (or unit) to reduce tiny allocations
    s.oom_ram_bump = 1.6          # multiply RAM request after OOM-ish failure
    s.max_ram_mult = 6.0          # cap multiplier to avoid requesting absurd RAM
    s.preempt_cooldown_ticks = 6  # don't re-preempt the same container too frequently

    # Soft reservations: keep this fraction of each pool free for high priority.
    # This improves query/interactive latency and reduces their 720s penalties.
    s.reserve_query = 0.15
    s.reserve_interactive = 0.10

    # Max preemptions per tick to limit churn
    s.max_preemptions_per_tick = 2

    # If a pipeline is stuck too long (not progressing) we still keep it; no drops.
    # (No explicit timeout since simulator penalizes incomplete anyway; we focus on completion.)


@register_scheduler(key="scheduler_none_042")
def scheduler_none_042(s, results, pipelines):
    """
    Returns (suspensions, assignments).

    Approach:
    1) Ingest new pipelines into per-priority queues.
    2) Process results to update RAM multipliers on failures.
    3) Try to schedule ready operators with soft reservations:
       - Prefer QUERY then INTERACTIVE then BATCH
       - For each pool, allocate moderate CPU/RAM and place at most one op per assignment.
    4) If high-priority ops are waiting but cannot be placed, preempt a limited number of
       low-priority running containers to free headroom.
    """
    s._tick += 1

    def _prio_rank(prio):
        # Lower is higher priority
        if prio == Priority.QUERY:
            return 0
        if prio == Priority.INTERACTIVE:
            return 1
        return 2  # Priority.BATCH_PIPELINE or others

    def _enqueue(p):
        if p.priority == Priority.QUERY:
            s.q_query.append(p)
        elif p.priority == Priority.INTERACTIVE:
            s.q_interactive.append(p)
        else:
            s.q_batch.append(p)

    def _iter_queues_in_order():
        # Yield (queue_name, queue_list) in priority order
        return (("query", s.q_query), ("interactive", s.q_interactive), ("batch", s.q_batch))

    def _pipeline_done_or_dead(p):
        st = p.runtime_status()
        if st.is_pipeline_successful():
            return True
        # We never "drop" failed pipelines wholesale; but if status says pipeline has failures,
        # we keep it to allow retries from FAILED (ASSIGNABLE_STATES) per simulator model.
        return False

    def _get_ready_ops(p):
        st = p.runtime_status()
        # Prefer ops whose parents are complete to preserve correctness and avoid waste.
        ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        if not ops:
            return []
        return ops[: s.max_ops_per_assignment]

    def _soft_reserved(pool, prio):
        # Amount to keep free. Queries reserve more; interactive some; batch none.
        if prio == Priority.QUERY:
            return (pool.max_cpu_pool * s.reserve_query, pool.max_ram_pool * s.reserve_query)
        if prio == Priority.INTERACTIVE:
            return (pool.max_cpu_pool * s.reserve_interactive, pool.max_ram_pool * s.reserve_interactive)
        return (0.0, 0.0)

    def _alloc_for(pool, prio, pipeline_id):
        # Allocate moderate CPU/RAM to reduce head-of-line blocking and improve p95 for queries.
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool

        # Apply soft reservation for *lower* priorities (batch shouldn't eat reserved headroom).
        # For query/interactive we ignore reservations (they are the ones being protected).
        if prio == Priority.BATCH_PIPELINE:
            res_cpu_q, res_ram_q = _soft_reserved(pool, Priority.QUERY)
            res_cpu_i, res_ram_i = _soft_reserved(pool, Priority.INTERACTIVE)
            reserved_cpu = max(res_cpu_q, res_cpu_i)
            reserved_ram = max(res_ram_q, res_ram_i)
            avail_cpu = max(0.0, avail_cpu - reserved_cpu)
            avail_ram = max(0.0, avail_ram - reserved_ram)
        elif prio == Priority.INTERACTIVE:
            # Protect queries: interactive shouldn't consume query-reserved headroom
            res_cpu_q, res_ram_q = _soft_reserved(pool, Priority.QUERY)
            avail_cpu = max(0.0, avail_cpu - res_cpu_q)
            avail_ram = max(0.0, avail_ram - res_ram_q)
        # QUERY: no reservation subtraction

        if avail_cpu <= 0 or avail_ram <= 0:
            return (0.0, 0.0)

        # Base sizing (sublinear scaling + desire for concurrency)
        cpu = max(s.min_cpu, avail_cpu * s.base_cpu_fraction)
        ram = max(s.min_ram, avail_ram * s.base_ram_fraction)

        # Apply OOM backoff multiplier per pipeline
        mult = s.pipeline_ram_mult.get(pipeline_id, 1.0)
        ram = min(avail_ram, ram * mult)

        # If CPU is huge, cap it to avoid over-allocating one op and hurting latency for others
        # (heuristic; works better than "take all" FIFO in mixed workloads).
        cpu_cap = max(s.min_cpu, pool.max_cpu_pool * 0.75)
        cpu = min(cpu, cpu_cap)

        return (cpu, ram)

    def _pool_order_for_priority():
        # Place high priority where there is most headroom (prefer faster admission).
        # For batch, prefer most fragmented pool? We'll just use most headroom too.
        pool_ids = list(range(s.executor.num_pools))
        pool_ids.sort(
            key=lambda i: (
                -(s.executor.pools[i].avail_cpu_pool + 0.001),
                -(s.executor.pools[i].avail_ram_pool + 0.001),
            )
        )
        return pool_ids

    # Ingest new pipelines
    for p in pipelines:
        _enqueue(p)

    # Process results: bump RAM multiplier on failures; keep pipelines in queues for retry.
    # We avoid dropping because failures/incomplete are heavily penalized (720s).
    for r in results:
        if r is None:
            continue
        if getattr(r, "failed", None) and r.failed():
            # Most common failure is OOM in this domain; bump RAM multiplier for that pipeline.
            # Use pipeline_id when available in result; if absent, infer from ops' pipeline_id if present.
            pid = getattr(r, "pipeline_id", None)
            if pid is None:
                # Try infer from the first op if it carries pipeline_id
                try:
                    if r.ops:
                        pid = getattr(r.ops[0], "pipeline_id", None)
                except Exception:
                    pid = None

            if pid is not None:
                cur = s.pipeline_ram_mult.get(pid, 1.0)
                nxt = min(s.max_ram_mult, max(cur, 1.0) * s.oom_ram_bump)
                s.pipeline_ram_mult[pid] = nxt

    # Early exit if nothing to do
    if not pipelines and not results and not (s.q_query or s.q_interactive or s.q_batch):
        return [], []

    suspensions = []
    assignments = []

    # Helper: attempt to schedule one ready op from a given queue on a specific pool
    def _try_schedule_from_queue(q, pool_id):
        pool = s.executor.pools[pool_id]
        if pool.avail_cpu_pool <= 0 or pool.avail_ram_pool <= 0:
            return False

        # Iterate through queue with rotation: avoid starvation within same priority
        n = len(q)
        if n == 0:
            return False

        scheduled_any = False
        requeue = []

        for _ in range(n):
            p = q.pop(0)
            if _pipeline_done_or_dead(p):
                continue

            ops = _get_ready_ops(p)
            if not ops:
                requeue.append(p)
                continue

            cpu, ram = _alloc_for(pool, p.priority, p.pipeline_id)
            if cpu <= 0 or ram <= 0:
                # Can't fit now
                requeue.append(p)
                continue

            # Final fit check against actual pool availability (not reserved-adjusted)
            if cpu > pool.avail_cpu_pool or ram > pool.avail_ram_pool:
                requeue.append(p)
                continue

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
            # Put pipeline back to allow subsequent operators later
            requeue.append(p)
            scheduled_any = True
            break

        q.extend(requeue)
        return scheduled_any

    # First pass: schedule without preemption, prioritize high priority
    pool_ids = _pool_order_for_priority()
    for pool_id in pool_ids:
        # Attempt at most a couple of assignments per pool per tick to avoid monopolizing the tick
        per_pool_budget = 2
        while per_pool_budget > 0:
            made = False
            # High priority first
            made = _try_schedule_from_queue(s.q_query, pool_id) or made
            if not made:
                made = _try_schedule_from_queue(s.q_interactive, pool_id) or made
            if not made:
                made = _try_schedule_from_queue(s.q_batch, pool_id) or made

            if not made:
                break
            per_pool_budget -= 1

    # Preemption logic: if query/interactive are waiting with ready ops but nothing scheduled for them,
    # try to free resources by suspending low-priority running containers.
    def _has_waiting_ready(prio_queue):
        for p in prio_queue:
            if _pipeline_done_or_dead(p):
                continue
            if _get_ready_ops(p):
                return True
        return False

    need_query = _has_waiting_ready(s.q_query)
    need_interactive = _has_waiting_ready(s.q_interactive)

    # If we already scheduled some high priority, avoid preemption this tick.
    scheduled_high = any(a.priority in (Priority.QUERY, Priority.INTERACTIVE) for a in assignments)

    if (need_query or need_interactive) and not scheduled_high:
        preemptions_left = s.max_preemptions_per_tick

        # Choose preemption victims: prefer BATCH, then INTERACTIVE (never preempt QUERY).
        # We need visibility into running containers; assume executor exposes containers per pool.
        # We'll defensively probe common attributes.
        for pool_id in pool_ids:
            if preemptions_left <= 0:
                break

            pool = s.executor.pools[pool_id]

            # If pool already has headroom, skip preemption.
            if pool.avail_cpu_pool > 0 and pool.avail_ram_pool > 0:
                continue

            containers = []
            # Try common container listings (best-effort; if absent, skip preemption)
            for attr in ("containers", "running_containers", "active_containers"):
                if hasattr(pool, attr):
                    try:
                        containers = list(getattr(pool, attr))
                        break
                    except Exception:
                        containers = []
            if not containers:
                continue

            # Normalize container objects: some may be dict-like, some objects.
            def _cget(c, name, default=None):
                if hasattr(c, name):
                    return getattr(c, name)
                if isinstance(c, dict):
                    return c.get(name, default)
                return default

            # Build victim candidates
            victims = []
            for c in containers:
                cid = _cget(c, "container_id", None)
                if cid is None:
                    cid = _cget(c, "id", None)
                if cid is None:
                    continue

                cprio = _cget(c, "priority", None)
                # Only preempt lower than the needed class
                if cprio == Priority.QUERY:
                    continue

                cooldown_key = (pool_id, cid)
                if s.preempt_cooldown_until.get(cooldown_key, -1) > s._tick:
                    continue

                # Prefer preempting batch first, then interactive
                rank = _prio_rank(cprio) if cprio is not None else 2
                victims.append((rank, cid))

            victims.sort(reverse=True)  # batch (rank 2) first
            for _, cid in victims:
                if preemptions_left <= 0:
                    break
                suspensions.append(Suspend(cid, pool_id))
                s.preempt_cooldown_until[(pool_id, cid)] = s._tick + s.preempt_cooldown_ticks
                preemptions_left -= 1

    return suspensions, assignments
