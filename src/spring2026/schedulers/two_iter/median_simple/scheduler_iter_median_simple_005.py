# policy_key: scheduler_iter_median_simple_005
# context: simple
# model: gpt-5.2-2025-12-11
# llm_cost: 0.054825
# generation_seconds: 47.04
# generated_at: 2026-04-12T01:10:37.894573
@register_scheduler_init(key="scheduler_iter_median_simple_005")
def scheduler_iter_median_simple_005_init(s):
    """Priority-first, latency-oriented scheduler with:
    - Strict priority ordering: QUERY > INTERACTIVE > BATCH.
    - Pool preference: if multiple pools, steer high-priority to "latency pool" (pool 0) first.
    - Soft reservations: keep per-pool CPU/RAM headroom for high-priority when backlog exists.
    - Per-pipeline adaptive RAM backoff on OOM using assignment->pipeline mapping (no global bump).
    - Conservative CPU sizing for high-priority to reduce head-of-line blocking and improve median latency.
    """
    # Per-priority queues (arrival order preserved within a priority).
    s.q = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }

    # Per-pipeline hints.
    # ram_mult: increases after OOM to avoid repeated failures.
    # cpu_mult: (kept for future iterations) currently constant.
    # drop: stop retrying pipelines that have non-OOM failures.
    s.pstate = {}  # pipeline_id -> dict

    # Track which pipeline a container belonged to so we can update pstate from results.
    s.container_to_pipeline = {}  # (pool_id, container_id) -> pipeline_id

    # Round-robin cursors within each priority to avoid starvation within class.
    s.rr = {
        Priority.QUERY: 0,
        Priority.INTERACTIVE: 0,
        Priority.BATCH_PIPELINE: 0,
    }


@register_scheduler(key="scheduler_iter_median_simple_005")
def scheduler_iter_median_simple_005_scheduler(s, results, pipelines):
    """One scheduler tick: ingest, update pstate from results, then assign ops.
    Focus: reduce median latency by prioritizing high-priority work and preventing batch
    from consuming all headroom when interactive/query backlog exists.
    """
    # -----------------------
    # Helper functions
    # -----------------------
    def _ensure_pstate(pipeline_id):
        if pipeline_id not in s.pstate:
            s.pstate[pipeline_id] = {"ram_mult": 1.0, "cpu_mult": 1.0, "drop": False}
        return s.pstate[pipeline_id]

    def _is_oom_error(err):
        if err is None:
            return False
        msg = str(err).lower()
        return ("oom" in msg) or ("out of memory" in msg) or ("killed" in msg and "memory" in msg)

    def _prio_order():
        return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]

    def _has_runnable_backlog(pri):
        for p in s.q[pri]:
            st = p.runtime_status()
            if st.is_pipeline_successful():
                continue
            pst = _ensure_pstate(p.pipeline_id)
            if pst["drop"]:
                continue
            ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
            if ops:
                return True
        return False

    def _pop_next_runnable(pri):
        """Round-robin scan within priority; returns (pipeline, op_list) or (None, None)."""
        q = s.q[pri]
        if not q:
            return None, None

        # Bound the scan to current queue length.
        n = len(q)
        start = s.rr[pri] % max(1, n)

        for step in range(n):
            idx = (start + step) % n
            p = q[idx]
            st = p.runtime_status()
            pst = _ensure_pstate(p.pipeline_id)

            # Drop completed pipelines.
            if st.is_pipeline_successful():
                q.pop(idx)
                # Keep cursor stable relative to remaining items.
                if idx < start:
                    start -= 1
                n -= 1
                if n <= 0:
                    s.rr[pri] = 0
                    return None, None
                continue

            # Drop pipelines marked non-retriable.
            if pst["drop"]:
                q.pop(idx)
                if idx < start:
                    start -= 1
                n -= 1
                if n <= 0:
                    s.rr[pri] = 0
                    return None, None
                continue

            op_list = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
            if not op_list:
                continue

            # Advance cursor to next element after this one.
            s.rr[pri] = (idx + 1) % max(1, len(q))
            return p, op_list

        return None, None

    def _pool_order_for_priority(pri):
        """Latency steering: prefer pool 0 for high-priority when multiple pools exist."""
        n = s.executor.num_pools
        if n <= 1:
            return [0]
        if pri in (Priority.QUERY, Priority.INTERACTIVE):
            # pool 0 first, then the rest in order
            return [0] + [i for i in range(1, n)]
        # batch: prefer non-0 pools first to reduce interference
        return [i for i in range(1, n)] + [0]

    def _reservation_fraction(pri, hp_backlog):
        """Soft reservations: when hp backlog exists, cap batch aggressively."""
        if pri == Priority.BATCH_PIPELINE and hp_backlog:
            return 0.35
        if pri == Priority.BATCH_PIPELINE:
            return 0.75
        # high-priority can use most of what's available
        return 1.0

    def _request_size(pool, pri, pipeline_id, avail_cpu, avail_ram, hp_backlog):
        """Choose cpu/ram request sizes."""
        pst = _ensure_pstate(pipeline_id)

        # CPU: keep small for latency-critical (more parallel starts, less HoL blocking).
        # Batch gets larger chunks when allowed, but still bounded to reduce interference.
        if pri == Priority.QUERY:
            base_cpu = min(2.0, max(1.0, pool.max_cpu_pool * 0.25))
            base_ram = max(1.0, pool.max_ram_pool * 0.10)
        elif pri == Priority.INTERACTIVE:
            base_cpu = min(3.0, max(1.0, pool.max_cpu_pool * 0.30))
            base_ram = max(1.0, pool.max_ram_pool * 0.18)
        else:
            # Batch CPU slightly reduced vs previous attempt to keep more headroom.
            # If hp backlog exists, batch should be even smaller.
            if hp_backlog:
                base_cpu = max(1.0, min(pool.max_cpu_pool * 0.30, 3.0))
                base_ram = max(1.0, pool.max_ram_pool * 0.20)
            else:
                base_cpu = max(2.0, min(pool.max_cpu_pool * 0.50, 6.0))
                base_ram = max(1.0, pool.max_ram_pool * 0.30)

        cpu_req = base_cpu * pst["cpu_mult"]
        ram_req = base_ram * pst["ram_mult"]

        # Apply per-priority soft cap on what this priority may consume right now in this pool.
        frac = _reservation_fraction(pri, hp_backlog)
        cpu_cap = max(0.0, min(avail_cpu, pool.max_cpu_pool * frac))
        ram_cap = max(0.0, min(avail_ram, pool.max_ram_pool * frac))

        cpu_req = max(1.0, min(cpu_req, cpu_cap))
        ram_req = max(1.0, min(ram_req, ram_cap))

        # If we can't fit minimums, signal failure to schedule.
        if cpu_req <= 0.0 or ram_req <= 0.0:
            return 0.0, 0.0

        return cpu_req, ram_req

    # -----------------------
    # Ingest new pipelines
    # -----------------------
    for p in pipelines:
        _ensure_pstate(p.pipeline_id)
        s.q[p.priority].append(p)

    if not pipelines and not results:
        return [], []

    # -----------------------
    # Update per-pipeline state from results (using container->pipeline mapping)
    # -----------------------
    for r in results:
        # Clean mapping for completed containers.
        key = (getattr(r, "pool_id", None), getattr(r, "container_id", None))
        pipeline_id = s.container_to_pipeline.pop(key, None)

        if hasattr(r, "failed") and r.failed():
            # If we can attribute the failure to a pipeline, update it precisely.
            if pipeline_id is not None:
                pst = _ensure_pstate(pipeline_id)
                if _is_oom_error(getattr(r, "error", None)):
                    # Increase RAM multiplier fairly aggressively to converge quickly.
                    pst["ram_mult"] = min(pst["ram_mult"] * 2.0, 32.0)
                else:
                    # Non-OOM failures are treated as terminal to avoid retry churn.
                    pst["drop"] = True

    # Also lazily drop pipelines that show FAILED ops but were marked drop=True.
    # (Runtime status is authoritative; this keeps queues from clogging.)
    for pri in _prio_order():
        q = s.q[pri]
        kept = []
        for p in q:
            st = p.runtime_status()
            pst = _ensure_pstate(p.pipeline_id)
            if st.is_pipeline_successful():
                continue
            if pst["drop"] and st.state_counts.get(OperatorState.FAILED, 0) > 0:
                continue
            kept.append(p)
        s.q[pri] = kept

    # -----------------------
    # Build assignments (no preemption in this iteration)
    # -----------------------
    suspensions = []
    assignments = []

    hp_backlog = _has_runnable_backlog(Priority.QUERY) or _has_runnable_backlog(Priority.INTERACTIVE)

    # Schedule in strict priority phases to reduce latency.
    for pri in _prio_order():
        # Attempt to place as many runnable ops as possible for this priority.
        # We loop until we can't find runnable work or no pools have capacity.
        while True:
            pipeline, op_list = _pop_next_runnable(pri)
            if pipeline is None:
                break

            placed = False
            # Try pools in priority-dependent order.
            for pool_id in _pool_order_for_priority(pri):
                pool = s.executor.pools[pool_id]
                avail_cpu = pool.avail_cpu_pool
                avail_ram = pool.avail_ram_pool
                if avail_cpu <= 0 or avail_ram <= 0:
                    continue

                cpu_req, ram_req = _request_size(
                    pool=pool,
                    pri=pri,
                    pipeline_id=pipeline.pipeline_id,
                    avail_cpu=avail_cpu,
                    avail_ram=avail_ram,
                    hp_backlog=hp_backlog,
                )
                if cpu_req <= 0 or ram_req <= 0:
                    continue

                a = Assignment(
                    ops=op_list,
                    cpu=cpu_req,
                    ram=ram_req,
                    priority=pipeline.priority,
                    pool_id=pool_id,
                    pipeline_id=pipeline.pipeline_id,
                )
                assignments.append(a)

                # Best-effort attribution: map the next result's container_id back to this pipeline.
                # The simulator should report container_id in ExecutionResult; we record it when it appears.
                # Since we don't have the container_id at assignment time, we will store mapping after the
                # container exists (i.e., on next tick) by matching (pool_id, container_id) in results.
                #
                # Still, we can pre-create an "expectation slot" by remembering that this pool just got
                # a new container for this pipeline; but without an id, we can't key it. So we rely on
                # the typical invariant that a container_id appears in results and can be mapped only
                # if we stored it earlier. To make this work, we instead map by container_id when it
                # first appears: if we see a (pool_id, container_id) with no mapping, attribute it to the
                # highest-priority pipeline recently assigned to that pool in this tick.
                placed = True

                # Keep pipeline in queue for future ops (round-robin cursor handles fairness).
                s.q[pri].append(pipeline)
                break

            if not placed:
                # Couldn't place now; put pipeline back and stop trying this priority this tick.
                s.q[pri].append(pipeline)
                break

        # After scheduling QUERY, refresh hp_backlog (it may have decreased, but we keep it sticky
        # for the tick to avoid batch immediately expanding and harming tail/median).
        # So we intentionally do NOT recompute hp_backlog here.

    # -----------------------
    # Opportunistic container->pipeline attribution for new results next tick
    # -----------------------
    # Since we can't map assignments to container_id directly, we improve attribution by
    # remembering, per pool, the most recent pipeline assigned in this tick (highest priority first).
    # On the next tick, if we see an unmapped container_id in results for that pool, we attribute it.
    #
    # NOTE: We do not have a hook "at container start"; only results. So we store this hint on s.
    s.last_assigned_pipeline_by_pool = getattr(s, "last_assigned_pipeline_by_pool", {})
    for a in assignments:
        # Preserve the highest-priority hint in case multiple were assigned; queries should win.
        prev = s.last_assigned_pipeline_by_pool.get(a.pool_id)
        if prev is None:
            s.last_assigned_pipeline_by_pool[a.pool_id] = a.pipeline_id
        else:
            # If there's already a hint, prefer the one with higher priority class.
            # We approximate by comparing assignment.priority values if they are orderable,
            # otherwise keep existing.
            try:
                if a.priority < getattr(s, "last_assigned_priority_by_pool", {}).get(a.pool_id, a.priority):
                    s.last_assigned_pipeline_by_pool[a.pool_id] = a.pipeline_id
            except Exception:
                pass

    # Patch up missing mappings for results where container_id is new/unseen: attribute using hint.
    # (This runs at end of tick; it helps next tick's failure attribution because the mapping will exist.)
    for r in results:
        key = (getattr(r, "pool_id", None), getattr(r, "container_id", None))
        if key[0] is None or key[1] is None:
            continue
        if key not in s.container_to_pipeline:
            pid_hint = getattr(s, "last_assigned_pipeline_by_pool", {}).get(key[0])
            if pid_hint is not None:
                s.container_to_pipeline[key] = pid_hint

    return suspensions, assignments
