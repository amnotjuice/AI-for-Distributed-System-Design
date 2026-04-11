# policy_key: scheduler_none_045
# reasoning_effort: none
# model: gpt-5.2-2025-12-11
# llm_cost: 0.044391
# generation_seconds: 62.46
# generated_at: 2026-04-09T22:20:21.525505
@register_scheduler_init(key="scheduler_none_045")
def scheduler_none_045_init(s):
    """
    Priority-aware, failure-averse scheduler.

    Main ideas:
    - Keep per-priority queues (QUERY > INTERACTIVE > BATCH).
    - Use conservative initial RAM sizing (a per-operator learned estimate with a safety factor)
      and retry-on-OOM by bumping RAM and requeueing.
    - Use headroom reservations to protect high-priority latency.
    - Light preemption: when a high-priority op cannot be admitted due to lack of headroom,
      suspend some lower-priority containers in that pool until enough resources are freed.
    - Avoid wasting cycles: right-size CPU for high priority (enough to be fast) but do not
      over-allocate CPU to a single op when RAM is the dominant OOM risk.
    """
    # Priority queues of pipelines
    s.q_query = []
    s.q_interactive = []
    s.q_batch = []

    # Simple learned resource hints keyed by (pipeline_id, op_id-like key)
    # Since we don't have a stable operator id API, we use repr(op) as a fallback key.
    s.op_hints = {}  # key -> {"ram": float, "cpu": float, "ooms": int}

    # Track active containers we started: container_id -> metadata
    s.active = {}  # container_id -> {"pipeline_id": ..., "priority": ..., "pool_id": ..., "ops_key": ..., "cpu": ..., "ram": ...}

    # Per-pipeline attempt tracking to avoid infinite retries on repeated failures
    s.pipe_failures = {}  # pipeline_id -> count

    # Tuning knobs
    s.max_pipe_failures = 6  # after too many failures, stop spending resources; pipeline will be penalized anyway
    s.base_ram_safety = 1.25  # initial RAM cushion above last-known / last-assigned
    s.oom_bump = 1.6          # RAM multiplier on OOM retry
    s.min_cpu_per_op = 1.0
    s.max_cpu_per_op_fraction = 0.75  # do not allocate more than this fraction of pool CPU to a single op
    s.query_cpu_boost_fraction = 0.60  # for QUERY, try to use this fraction of pool CPU (bounded)
    s.interactive_cpu_boost_fraction = 0.45

    # Headroom reservations (fractions of pool resources) to preserve latency for high priorities.
    # These act as "do not consume beyond" limits for lower priorities.
    s.reserve_query_cpu = 0.20
    s.reserve_query_ram = 0.20
    s.reserve_interactive_cpu = 0.10
    s.reserve_interactive_ram = 0.10

    # Limit suspensions per tick to reduce churn
    s.max_suspends_per_tick = 4

    # Small aging to avoid batch starvation: after some time, allow one batch per pool per tick if feasible.
    s.tick = 0
    s.batch_quota_every = 3  # every N ticks, attempt at least one batch if resources allow


@register_scheduler(key="scheduler_none_045")
def scheduler_none_045_scheduler(s, results, pipelines):
    """
    Deterministic step scheduler.

    Returns:
      suspensions: containers to suspend (preempt lower priority to admit higher priority)
      assignments: new operator assignments (one-op-at-a-time per assignment)
    """
    s.tick += 1

    # ---------------------------
    # Helpers (kept inside to avoid imports / external deps)
    # ---------------------------
    def _prio_rank(prio):
        # Higher value => higher priority
        if prio == Priority.QUERY:
            return 3
        if prio == Priority.INTERACTIVE:
            return 2
        return 1  # BATCH_PIPELINE or others

    def _enqueue_pipeline(p):
        if p.priority == Priority.QUERY:
            s.q_query.append(p)
        elif p.priority == Priority.INTERACTIVE:
            s.q_interactive.append(p)
        else:
            s.q_batch.append(p)

    def _all_queues_empty():
        return not s.q_query and not s.q_interactive and not s.q_batch

    def _pipeline_done_or_dead(p):
        st = p.runtime_status()
        if st.is_pipeline_successful():
            return True
        # If too many pipeline-level failures, stop retrying to prevent cascading penalties to others.
        if s.pipe_failures.get(p.pipeline_id, 0) >= s.max_pipe_failures:
            return True
        return False

    def _get_assignable_op(p):
        st = p.runtime_status()
        # Prefer ops whose parents are complete (normal DAG execution).
        ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
        if ops:
            return ops[0]
        return None

    def _op_key(pipeline_id, op):
        # Use pipeline_id + repr(op) to reduce collisions when same repr appears across pipelines.
        return (pipeline_id, repr(op))

    def _estimate_ram_cpu(pool, p, op, prio):
        """
        RAM-first sizing:
        - Start from last known hint (or a fraction of pool RAM) with safety factor.
        - CPU: give a reasonable boost for query/interactive; cap to avoid hogging.
        """
        key = _op_key(p.pipeline_id, op)
        hint = s.op_hints.get(key, None)

        # RAM estimation:
        if hint and hint.get("ram", 0) > 0:
            ram = hint["ram"] * s.base_ram_safety
        else:
            # Conservative default: avoid immediate OOM by not going tiny, but also avoid reserving most of the pool.
            # Use larger defaults for higher priority to reduce OOM-related penalty (720s) and retries.
            if prio == Priority.QUERY:
                ram = 0.40 * pool.max_ram_pool
            elif prio == Priority.INTERACTIVE:
                ram = 0.30 * pool.max_ram_pool
            else:
                ram = 0.20 * pool.max_ram_pool

        # Clamp RAM to pool bounds
        if ram < 0.05 * pool.max_ram_pool:
            ram = 0.05 * pool.max_ram_pool
        if ram > pool.max_ram_pool:
            ram = pool.max_ram_pool

        # CPU estimation:
        # Prefer not to allocate entire pool; keep headroom for concurrent high-priority and reduce preemption.
        if hint and hint.get("cpu", 0) > 0:
            cpu = hint["cpu"]
        else:
            if prio == Priority.QUERY:
                cpu = max(s.min_cpu_per_op, s.query_cpu_boost_fraction * pool.max_cpu_pool)
            elif prio == Priority.INTERACTIVE:
                cpu = max(s.min_cpu_per_op, s.interactive_cpu_boost_fraction * pool.max_cpu_pool)
            else:
                cpu = max(s.min_cpu_per_op, 0.25 * pool.max_cpu_pool)

        # Cap CPU per op
        cpu_cap = max(s.min_cpu_per_op, s.max_cpu_per_op_fraction * pool.max_cpu_pool)
        if cpu > cpu_cap:
            cpu = cpu_cap
        if cpu > pool.max_cpu_pool:
            cpu = pool.max_cpu_pool
        return cpu, ram

    def _reserved_for_higher(prio):
        # Returns reserved fractions to keep available for higher priorities.
        # Lower priority work cannot consume below these headroom thresholds.
        if prio == Priority.BATCH_PIPELINE:
            # Must leave room for query+interactive
            return (s.reserve_query_cpu + s.reserve_interactive_cpu,
                    s.reserve_query_ram + s.reserve_interactive_ram)
        if prio == Priority.INTERACTIVE:
            # Must leave room for query
            return (s.reserve_query_cpu, s.reserve_query_ram)
        return (0.0, 0.0)  # QUERY has no reservation above it

    def _can_fit(pool, cpu, ram, prio):
        # Enforce headroom for higher priorities.
        reserve_cpu_frac, reserve_ram_frac = _reserved_for_higher(prio)
        min_avail_cpu = reserve_cpu_frac * pool.max_cpu_pool
        min_avail_ram = reserve_ram_frac * pool.max_ram_pool
        return (pool.avail_cpu_pool - cpu) >= min_avail_cpu and (pool.avail_ram_pool - ram) >= min_avail_ram

    def _pick_next_pipeline(prio_class):
        # Pop from the appropriate queue, skipping completed/dead ones.
        q = s.q_query if prio_class == Priority.QUERY else (s.q_interactive if prio_class == Priority.INTERACTIVE else s.q_batch)
        while q:
            p = q.pop(0)
            if _pipeline_done_or_dead(p):
                continue
            # Only queue pipelines that have work ready; otherwise requeue to back.
            op = _get_assignable_op(p)
            if op is None:
                q.append(p)
                # Avoid infinite loops in a single tick by breaking; we'll revisit next tick.
                break
            return p, op
        return None, None

    def _requeue_pipeline_front(p):
        # Put back near front to reduce latency after an OOM bump / preemption.
        if p.priority == Priority.QUERY:
            s.q_query.insert(0, p)
        elif p.priority == Priority.INTERACTIVE:
            s.q_interactive.insert(0, p)
        else:
            s.q_batch.insert(0, p)

    # ---------------------------
    # Ingest new arrivals
    # ---------------------------
    for p in pipelines:
        _enqueue_pipeline(p)

    # Early exit if nothing changed
    if not pipelines and not results:
        return [], []

    # ---------------------------
    # Process results: update hints; handle OOM by bumping RAM and retrying
    # ---------------------------
    for r in results:
        # Clean up active container tracking
        meta = s.active.pop(getattr(r, "container_id", None), None)

        if r.failed():
            # Pipeline-level failure count: we don't have direct pipeline_id in result, use meta if present.
            if meta and "pipeline_id" in meta:
                s.pipe_failures[meta["pipeline_id"]] = s.pipe_failures.get(meta["pipeline_id"], 0) + 1

            # Detect OOM-ish errors; if unknown, still treat as failure (but don't endlessly retry).
            err = (r.error or "").lower() if hasattr(r, "error") else ""
            is_oom = ("oom" in err) or ("out of memory" in err) or ("memory" in err and "exceed" in err)

            # Learn and bump RAM on OOM; also slightly downshift CPU if repeatedly OOMing to reduce wasted parallelism.
            if meta and meta.get("ops_key") is not None:
                key = meta["ops_key"]
                hint = s.op_hints.get(key, {"ram": meta.get("ram", 0), "cpu": meta.get("cpu", 0), "ooms": 0})

                # Record last attempted sizes as baseline
                last_ram = float(meta.get("ram", 0) or 0)
                last_cpu = float(meta.get("cpu", 0) or 0)
                if last_ram > 0:
                    hint["ram"] = max(hint.get("ram", 0) or 0, last_ram)
                if last_cpu > 0:
                    hint["cpu"] = last_cpu

                if is_oom:
                    hint["ooms"] = int(hint.get("ooms", 0) or 0) + 1
                    # Bump expected RAM for next attempt
                    if hint["ram"] > 0:
                        hint["ram"] = hint["ram"] * s.oom_bump
                    # If repeated OOMs, reduce CPU a bit to reduce contention while we fix RAM sizing.
                    if hint["ooms"] >= 2 and hint.get("cpu", 0) > s.min_cpu_per_op:
                        hint["cpu"] = max(s.min_cpu_per_op, 0.8 * hint["cpu"])
                s.op_hints[key] = hint
        else:
            # On success, trust the RAM/CPU we used as a good hint for next time (slightly reduce RAM to avoid over-allocation).
            if meta and meta.get("ops_key") is not None:
                key = meta["ops_key"]
                hint = s.op_hints.get(key, {"ram": 0.0, "cpu": 0.0, "ooms": 0})
                used_ram = float(getattr(r, "ram", meta.get("ram", 0)) or 0)
                used_cpu = float(getattr(r, "cpu", meta.get("cpu", 0)) or 0)
                if used_ram > 0:
                    # Keep a modest cushion but allow downward drift over time
                    hint["ram"] = 0.9 * max(hint.get("ram", 0) or 0, used_ram)
                if used_cpu > 0:
                    hint["cpu"] = used_cpu
                hint["ooms"] = 0
                s.op_hints[key] = hint

    # ---------------------------
    # Build preemption candidates (from active map) grouped by pool, lowest priority first
    # ---------------------------
    active_by_pool = {}
    for cid, meta in s.active.items():
        pool_id = meta.get("pool_id", None)
        if pool_id is None:
            continue
        active_by_pool.setdefault(pool_id, []).append((cid, meta))
    for pool_id in active_by_pool:
        # sort by increasing priority, then by larger resource usage first (free headroom faster)
        active_by_pool[pool_id].sort(key=lambda x: (_prio_rank(x[1].get("priority")), -(x[1].get("ram", 0) or 0), -(x[1].get("cpu", 0) or 0)))

    # ---------------------------
    # Main scheduling loop: iterate pools and attempt to admit work
    # ---------------------------
    suspensions = []
    assignments = []
    suspends_left = s.max_suspends_per_tick

    # Decide whether to try a batch admission this tick (anti-starvation)
    allow_batch_this_tick = (s.tick % s.batch_quota_every == 0)

    # Attempt multiple rounds so that after preemptions we can assign in the same tick
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]

        # Try to schedule up to a small number of ops per pool per tick (avoid overreacting)
        per_pool_budget = 2
        scheduled = 0

        while scheduled < per_pool_budget:
            # Pick next priority class to serve
            p, op = None, None

            # Strict priority, but allow one batch occasionally to prevent indefinite starvation.
            p, op = _pick_next_pipeline(Priority.QUERY)
            if p is None:
                p, op = _pick_next_pipeline(Priority.INTERACTIVE)
            if p is None and allow_batch_this_tick:
                p, op = _pick_next_pipeline(Priority.BATCH_PIPELINE)
            if p is None:
                # If batch not allowed this tick, still try query/interactive already attempted; break.
                break

            prio = p.priority
            cpu_req, ram_req = _estimate_ram_cpu(pool, p, op, prio)

            # If request doesn't fit, try to preempt lower-priority containers in this pool.
            if not _can_fit(pool, cpu_req, ram_req, prio):
                if suspends_left <= 0:
                    # Can't preempt now; requeue and move on.
                    _requeue_pipeline_front(p)
                    break

                candidates = active_by_pool.get(pool_id, [])
                did_preempt = False

                # Preempt only strictly lower priority containers
                for (cid, meta) in list(candidates):
                    if suspends_left <= 0:
                        break
                    if _prio_rank(meta.get("priority")) >= _prio_rank(prio):
                        continue
                    suspensions.append(Suspend(cid, pool_id))
                    suspends_left -= 1
                    did_preempt = True

                    # Optimistically update available resources (simulator will apply suspensions; this helps avoid over-preempting)
                    pool.avail_cpu_pool += float(meta.get("cpu", 0) or 0)
                    pool.avail_ram_pool += float(meta.get("ram", 0) or 0)

                    # Remove from candidates/active_by_pool list so we don't preempt it twice
                    try:
                        candidates.remove((cid, meta))
                    except Exception:
                        pass

                    # Stop once it fits
                    if _can_fit(pool, cpu_req, ram_req, prio):
                        break

                if not did_preempt or not _can_fit(pool, cpu_req, ram_req, prio):
                    # Still can't fit; requeue and stop for this pool this tick.
                    _requeue_pipeline_front(p)
                    break

            # Admit: allocate but also cap by current available
            cpu = min(cpu_req, pool.avail_cpu_pool)
            ram = min(ram_req, pool.avail_ram_pool)

            # Ensure minimums (avoid zero allocations if nearly exhausted)
            if cpu <= 0 or ram <= 0:
                _requeue_pipeline_front(p)
                break

            assignment = Assignment(
                ops=[op],
                cpu=cpu,
                ram=ram,
                priority=prio,
                pool_id=pool_id,
                pipeline_id=p.pipeline_id
            )
            assignments.append(assignment)

            # Track active; we don't know container_id yet (comes in results),
            # but ExecutionResult will provide container_id; we set tracking there.
            # However, to support preemption candidate building within this tick, we need a placeholder.
            # We'll instead rely on existing active containers only; newly assigned won't be preempted until next tick.

            # Decrement available resources (optimistic local bookkeeping)
            pool.avail_cpu_pool -= cpu
            pool.avail_ram_pool -= ram

            # Requeue pipeline so its next op can be scheduled later
            # (or if this op blocks, _get_assignable_op will return None next time)
            _enqueue_pipeline(p)

            scheduled += 1

    # ---------------------------
    # Post-process: attach active metadata for newly assigned ops when results arrive.
    # We can't do it here because container_id is unknown. So we update in a best-effort way:
    # if the simulator reports container_id in results on the next tick, we will record it then.
    # To enable that, we store a small "pending assignment" list in scheduler state, matching by (pool_id, pipeline_id, priority, op repr).
    # ---------------------------
    if not hasattr(s, "pending"):
        s.pending = []
    # Keep pending small
    if len(s.pending) > 2000:
        s.pending = s.pending[-1000:]

    for a in assignments:
        # a.ops is a list with single op
        op0 = a.ops[0] if a.ops else None
        if op0 is None:
            continue
        s.pending.append({
            "pool_id": a.pool_id,
            "pipeline_id": a.pipeline_id,
            "priority": a.priority,
            "ops_key": _op_key(a.pipeline_id, op0),
            "cpu": float(a.cpu),
            "ram": float(a.ram),
        })

    # When results come back next tick, try to bind container_id -> last pending in same pool+priority (best effort).
    # We do this binding now as well for current results (if any) in case the simulator returns container_id immediately.
    if results and hasattr(s, "pending") and s.pending:
        # Build index by (pool_id, priority) for quick match
        by_pp = {}
        for idx, pend in enumerate(s.pending):
            by_pp.setdefault((pend["pool_id"], pend["priority"]), []).append((idx, pend))
        # For each result with container_id, bind if not already bound
        for r in results:
            cid = getattr(r, "container_id", None)
            if cid is None:
                continue
            if cid in s.active:
                continue
            pool_id = getattr(r, "pool_id", None)
            prio = getattr(r, "priority", None)
            if pool_id is None or prio is None:
                continue
            lst = by_pp.get((pool_id, prio), [])
            if not lst:
                continue
            # Pop the earliest pending in that pool/priority
            idx, pend = lst.pop(0)
            by_pp[(pool_id, prio)] = lst
            s.active[cid] = {
                "pipeline_id": pend["pipeline_id"],
                "priority": pend["priority"],
                "pool_id": pend["pool_id"],
                "ops_key": pend["ops_key"],
                "cpu": pend["cpu"],
                "ram": pend["ram"],
            }
            # Mark pending entry removed
            s.pending[idx] = None
        # Compact pending list
        s.pending = [p for p in s.pending if p is not None]

    return suspensions, assignments
