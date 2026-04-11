# policy_key: scheduler_none_026
# reasoning_effort: none
# model: gpt-5.2-2025-12-11
# llm_cost: 0.049797
# generation_seconds: 45.81
# generated_at: 2026-03-14T02:00:24.649569
@register_scheduler_init(key="scheduler_none_026")
def scheduler_none_026_init(s):
    """Priority-aware, small-step improvement over naive FIFO.

    Key ideas (kept intentionally simple and incremental):
    1) Maintain separate FIFO queues per priority (QUERY, INTERACTIVE, BATCH).
    2) Prefer scheduling higher priority work first, but prevent starvation via
       lightweight aging (batch jobs gain "effective priority" as they wait).
    3) Adaptive right-sizing on OOM: retry the same operator with increased RAM
       (and slightly reduced CPU if needed) using per-(pipeline, op) learned hints.
    4) Conservative placement: try to place an assignment into the pool with the
       most headroom that can satisfy the request, to reduce fragmentation.
    5) Minimal preemption: only preempt when a high-priority pipeline is waiting
       and cannot be admitted anywhere due to insufficient free resources. Evict
       one lowest-priority container to create headroom.

    Notes:
    - This policy assumes the simulator provides s.executor.running_containers
      as an iterable of running container objects with fields: container_id,
      pool_id, priority, cpu, ram. If not present, preemption becomes a no-op.
    - CPU/RAM sizing is heuristic because the scheduler doesn't know true curves.
    """

    # Per-priority waiting queues (FIFO within each)
    s.q_query = []
    s.q_interactive = []
    s.q_batch = []

    # Track arrival "ticks" for aging (pipeline_id -> tick)
    s.tick = 0
    s.enqueue_tick = {}

    # OOM-driven sizing hints keyed by (pipeline_id, op_id)
    # Values: dict(cpu=..., ram=...)
    s.op_hints = {}

    # Basic sizing knobs (conservative defaults)
    s.min_cpu = 1.0
    s.max_cpu_fraction = 1.0  # can cap per-op cpu to avoid hogging
    s.ram_growth_factor = 1.6
    s.ram_growth_add = 0.0  # can add fixed overhead if desired
    s.cpu_backoff_factor_on_oom = 0.85

    # Aging knobs
    s.aging_interval = 25  # ticks waited to gain +1 "effective priority level"
    s.max_aging_boost = 2  # batch can at most climb two levels via aging

    # Attempt to keep a small safety headroom for bursty interactive work.
    # This is a "soft" reserve by biasing CPU/RAM allocations.
    s.soft_reserve_fraction = 0.10

    # Track last scheduling decisions to reduce churn (optional)
    s.last_nonempty_tick = 0


@register_scheduler(key="scheduler_none_026")
def scheduler_none_026(s, results, pipelines):
    """
    Scheduler step:
    - ingest new pipelines into per-priority queues
    - process results to learn from OOM and requeue pipelines
    - schedule ready operators with priority/aging, with light preemption
    """
    # Local helpers kept inside for portability (no global imports)
    def _prio_rank(p):
        # Higher number => higher priority
        if p == Priority.QUERY:
            return 3
        if p == Priority.INTERACTIVE:
            return 2
        return 1  # Priority.BATCH_PIPELINE (and any unknown treated as lowest)

    def _queue_for_priority(prio):
        if prio == Priority.QUERY:
            return s.q_query
        if prio == Priority.INTERACTIVE:
            return s.q_interactive
        return s.q_batch

    def _pipeline_enqueue(pipeline):
        q = _queue_for_priority(pipeline.priority)
        q.append(pipeline)
        if pipeline.pipeline_id not in s.enqueue_tick:
            s.enqueue_tick[pipeline.pipeline_id] = s.tick

    def _pipeline_dequeue_best():
        # Apply lightweight aging: compute "effective rank" based on waiting time.
        # We still preserve FIFO within each base queue by only choosing from heads.
        candidates = []
        for q in (s.q_query, s.q_interactive, s.q_batch):
            if q:
                candidates.append(q[0])
        if not candidates:
            return None

        best = None
        best_score = -1e18
        for p in candidates:
            base = _prio_rank(p.priority)
            waited = max(0, s.tick - s.enqueue_tick.get(p.pipeline_id, s.tick))
            boost = min(s.max_aging_boost, waited // max(1, s.aging_interval))
            score = (base + boost) * 1_000_000 - s.enqueue_tick.get(p.pipeline_id, s.tick)
            if score > best_score:
                best_score = score
                best = p

        # Remove from its actual queue head
        q = _queue_for_priority(best.priority)
        if q and q[0].pipeline_id == best.pipeline_id:
            q.pop(0)
        else:
            # Fallback: remove by search (should be rare)
            for i, p in enumerate(q):
                if p.pipeline_id == best.pipeline_id:
                    q.pop(i)
                    break
        return best

    def _op_id(op):
        # Try common id/name fields; fallback to Python object id
        for attr in ("op_id", "operator_id", "id", "name"):
            if hasattr(op, attr):
                return getattr(op, attr)
        return id(op)

    def _headroom_bias(pool, prio):
        # For high priority work, bias to keep some reserve for future bursts
        # by reducing what we try to allocate, not by hard admission control.
        # For batch, use full headroom.
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool
        if prio in (Priority.QUERY, Priority.INTERACTIVE):
            cpu = max(0.0, avail_cpu - pool.max_cpu_pool * s.soft_reserve_fraction)
            ram = max(0.0, avail_ram - pool.max_ram_pool * s.soft_reserve_fraction)
            # If reserve makes it impossible but resources exist, allow using it.
            if cpu <= 0 and avail_cpu > 0:
                cpu = avail_cpu
            if ram <= 0 and avail_ram > 0:
                ram = avail_ram
            return cpu, ram
        return avail_cpu, avail_ram

    def _choose_pool_for_request(prio, req_cpu, req_ram):
        # Choose pool with most leftover headroom after admitting request.
        best_pool_id = None
        best_leftover = -1e18
        for pool_id in range(s.executor.num_pools):
            pool = s.executor.pools[pool_id]
            avail_cpu, avail_ram = _headroom_bias(pool, prio)
            if avail_cpu + 1e-9 < req_cpu or avail_ram + 1e-9 < req_ram:
                continue
            leftover = (avail_cpu - req_cpu) + (avail_ram - req_ram) / 4.0
            if leftover > best_leftover:
                best_leftover = leftover
                best_pool_id = pool_id
        return best_pool_id

    def _default_request_for_op(pool, prio):
        # Simple right-sizing: for high priority, give a decent chunk but don't hog.
        # For batch, more conservative to improve overall throughput fairness.
        avail_cpu, avail_ram = _headroom_bias(pool, prio)

        if prio == Priority.QUERY:
            cpu = min(avail_cpu, max(s.min_cpu, pool.max_cpu_pool * 0.5))
            ram = min(avail_ram, pool.max_ram_pool * 0.5)
        elif prio == Priority.INTERACTIVE:
            cpu = min(avail_cpu, max(s.min_cpu, pool.max_cpu_pool * 0.35))
            ram = min(avail_ram, pool.max_ram_pool * 0.35)
        else:
            cpu = min(avail_cpu, max(s.min_cpu, pool.max_cpu_pool * 0.20))
            ram = min(avail_ram, pool.max_ram_pool * 0.20)

        # Cap CPU to avoid giving everything to a single op when other ops are waiting
        cpu = min(cpu, pool.max_cpu_pool * s.max_cpu_fraction)
        cpu = max(s.min_cpu, cpu)
        ram = max(0.0, ram)
        return cpu, ram

    def _apply_hint(pipeline_id, op, cpu, ram):
        key = (pipeline_id, _op_id(op))
        hint = s.op_hints.get(key)
        if not hint:
            return cpu, ram
        # Honor hint but also keep within the current pool availability decisions later
        return max(s.min_cpu, hint.get("cpu", cpu)), max(0.0, hint.get("ram", ram))

    def _learn_from_result(res):
        # On OOM, increase RAM hint and slightly reduce CPU to make room.
        if not res.failed():
            return
        err = getattr(res, "error", None)
        if not err:
            return
        # Heuristic: treat any error containing 'oom' as memory failure
        if "oom" not in str(err).lower():
            return

        for op in getattr(res, "ops", []) or []:
            key = (getattr(res, "pipeline_id", None), _op_id(op))
            # If pipeline_id not present on result, try to infer via op fields (best-effort)
            pid = key[0]
            if pid is None and hasattr(op, "pipeline_id"):
                pid = getattr(op, "pipeline_id")
            if pid is None:
                # Can't persist hint without pipeline id; skip.
                continue
            key = (pid, _op_id(op))

            prev = s.op_hints.get(key, {"cpu": getattr(res, "cpu", s.min_cpu), "ram": getattr(res, "ram", 0.0)})
            new_ram = max(prev.get("ram", 0.0), getattr(res, "ram", 0.0))
            new_ram = new_ram * s.ram_growth_factor + s.ram_growth_add

            prev_cpu = prev.get("cpu", getattr(res, "cpu", s.min_cpu))
            new_cpu = max(s.min_cpu, prev_cpu * s.cpu_backoff_factor_on_oom)

            s.op_hints[key] = {"cpu": new_cpu, "ram": new_ram}

    def _get_running_containers():
        # Best-effort discovery; if not available, return empty list.
        rc = getattr(s.executor, "running_containers", None)
        if rc is None:
            return []
        try:
            return list(rc)
        except Exception:
            return []

    def _pick_preemption_victim():
        # Choose lowest-priority running container, preferring largest resource hog.
        running = _get_running_containers()
        if not running:
            return None

        def victim_key(c):
            pr = getattr(c, "priority", Priority.BATCH_PIPELINE)
            rank = _prio_rank(pr)
            cpu = float(getattr(c, "cpu", 0.0) or 0.0)
            ram = float(getattr(c, "ram", 0.0) or 0.0)
            # Lower rank first; among same, evict bigger hogs
            return (rank, -(cpu + ram / 4.0))

        running.sort(key=victim_key)
        # Only preempt if there's at least one batch running; avoid preempting interactive/query unless necessary.
        for c in running:
            if getattr(c, "priority", Priority.BATCH_PIPELINE) == Priority.BATCH_PIPELINE:
                return c
        # Next, consider interactive (but not query) as last resort
        for c in running:
            if getattr(c, "priority", Priority.BATCH_PIPELINE) == Priority.INTERACTIVE:
                return c
        return running[0] if running else None

    # --- Start of tick logic ---
    s.tick += 1

    # Ingest new pipelines
    for p in pipelines:
        _pipeline_enqueue(p)

    # Learn from results; requeue pipelines that still have work
    # Also remove completed/failed from queues lazily when popped.
    for r in results:
        _learn_from_result(r)

    # Early exit
    if not pipelines and not results and not (s.q_query or s.q_interactive or s.q_batch):
        return [], []

    suspensions = []
    assignments = []

    # Attempt to schedule at most one assignment per pool per tick (like the baseline),
    # but using global priority selection and pool choice.
    # This reduces interference and keeps behavior stable while improving latency.
    pools_free = []
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        if pool.avail_cpu_pool > 0 and pool.avail_ram_pool > 0:
            pools_free.append(pool_id)

    # We will create up to len(pools_free) assignments.
    for _ in range(len(pools_free)):
        pipeline = _pipeline_dequeue_best()
        if pipeline is None:
            break

        status = pipeline.runtime_status()
        has_failures = status.state_counts[OperatorState.FAILED] > 0

        # Drop completed or definitively failed pipelines (baseline behavior: no retries except OOM sizing).
        if status.is_pipeline_successful() or has_failures:
            # cleanup bookkeeping
            s.enqueue_tick.pop(pipeline.pipeline_id, None)
            continue

        # Choose next ready ops (1 op at a time, like baseline).
        op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
        if not op_list:
            # Not ready; put back to its queue tail to avoid blocking others.
            _pipeline_enqueue(pipeline)
            continue

        op = op_list[0]

        # Find a pool and size request.
        # Start by proposing a request using the pool with best current headroom.
        # We'll iterate pools to find a feasible placement.
        chosen_pool_id = None
        chosen_cpu = None
        chosen_ram = None

        # Build per-pool proposals and pick best fit.
        for pool_id in range(s.executor.num_pools):
            pool = s.executor.pools[pool_id]
            if pool.avail_cpu_pool <= 0 or pool.avail_ram_pool <= 0:
                continue
            cpu, ram = _default_request_for_op(pool, pipeline.priority)
            cpu, ram = _apply_hint(pipeline.pipeline_id, op, cpu, ram)

            # Clamp to what's available in this pool (admission check uses biased headroom).
            avail_cpu, avail_ram = _headroom_bias(pool, pipeline.priority)
            if cpu <= avail_cpu + 1e-9 and ram <= avail_ram + 1e-9 and ram > 0:
                # Score by leftover headroom to reduce fragmentation
                leftover = (avail_cpu - cpu) + (avail_ram - ram) / 4.0
                if chosen_pool_id is None or leftover > ((s.executor.pools[chosen_pool_id].avail_cpu_pool - chosen_cpu)
                                                        + (s.executor.pools[chosen_pool_id].avail_ram_pool - chosen_ram) / 4.0):
                    chosen_pool_id = pool_id
                    chosen_cpu = cpu
                    chosen_ram = ram

        # If couldn't place, consider minimal preemption for high-priority
        if chosen_pool_id is None and pipeline.priority in (Priority.QUERY, Priority.INTERACTIVE):
            victim = _pick_preemption_victim()
            if victim is not None:
                suspensions.append(Suspend(getattr(victim, "container_id"), getattr(victim, "pool_id")))
                # Re-enqueue pipeline to try next tick after resources freed
                _pipeline_enqueue(pipeline)
                continue

        if chosen_pool_id is None:
            # Can't schedule now; put it back and move on
            _pipeline_enqueue(pipeline)
            continue

        assignments.append(
            Assignment(
                ops=op_list,
                cpu=chosen_cpu,
                ram=chosen_ram,
                priority=pipeline.priority,
                pool_id=chosen_pool_id,
                pipeline_id=pipeline.pipeline_id,
            )
        )

        # Re-enqueue pipeline since it likely has more ops later
        _pipeline_enqueue(pipeline)

    return suspensions, assignments
