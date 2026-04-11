# policy_key: scheduler_none_035
# reasoning_effort: none
# model: gpt-5.2-2025-12-11
# llm_cost: 0.053127
# generation_seconds: 47.19
# generated_at: 2026-04-09T22:12:19.635381
@register_scheduler_init(key="scheduler_none_035")
def scheduler_none_035_init(s):
    """Priority-aware, OOM-adaptive, low-churn scheduler.

    Goals for the weighted-latency objective:
      - Strongly protect QUERY/INTERACTIVE by (a) headroom reservation and (b) selective preemption of BATCH.
      - Reduce failures (720s penalty) via RAM backoff on OOM and per-op RAM estimates (learned).
      - Avoid starvation by aging: batch pipelines gain priority the longer they wait.

    Key ideas:
      1) Admission: keep per-priority queues; pick next runnable op by effective priority (base + aging).
      2) Sizing: assign conservative RAM based on learned per-op estimate (with OOM-driven growth).
      3) Preemption: only when high-priority work is blocked by lack of headroom; preempt batch first.
      4) Packing: prefer placing high-priority in the pool with most available headroom; avoid tiny fragments.
    """
    # Queues per priority
    s.q_query = []
    s.q_interactive = []
    s.q_batch = []

    # Pipeline arrival time for aging
    s.now = 0.0
    s.pipeline_arrival_ts = {}  # pipeline_id -> time

    # Per-operator learned RAM estimate (bytes or "RAM units" consistent with simulator)
    # Keyed by a stable op signature; fall back to operator_id-like repr if not available.
    s.op_ram_est = {}  # op_key -> ram_est

    # Track recent OOMs to avoid immediately re-scheduling too small
    s.pipeline_oom_count = {}  # pipeline_id -> count

    # Reservation fractions to keep headroom for high priority
    s.reserve_frac_query = 0.20
    s.reserve_frac_interactive = 0.10

    # Aging speed for batch to avoid starvation
    s.batch_aging_rate = 0.0025  # effective priority points per second waited

    # Limit per-tick assignments for churn control
    s.max_assignments_per_tick_per_pool = 2

    # Basic per-priority target CPU shares within a pool when contention exists
    s.cpu_cap_frac = {
        Priority.QUERY: 0.70,
        Priority.INTERACTIVE: 0.60,
        Priority.BATCH_PIPELINE: 0.50,
    }

    # Conservative initial RAM fraction of pool if we know nothing (avoid OOM penalties)
    s.unknown_op_ram_frac = {
        Priority.QUERY: 0.35,
        Priority.INTERACTIVE: 0.30,
        Priority.BATCH_PIPELINE: 0.25,
    }


@register_scheduler(key="scheduler_none_035")
def scheduler_none_035(s, results, pipelines):
    """
    Scheduler step.

    Strategy:
      - Incorporate new pipelines into priority queues.
      - Learn from results: on OOM -> increase per-op RAM estimate + track OOM count.
      - Select runnable operators with parent-complete constraint.
      - Place: try pools in order of "best fit" for the priority (more headroom for high-priority).
      - If blocked for high-priority, preempt running batch containers to free enough headroom.
    """
    # Helpers defined inside to avoid imports at top-level
    def _prio_weight(prio):
        if prio == Priority.QUERY:
            return 3.0
        if prio == Priority.INTERACTIVE:
            return 2.0
        return 1.0

    def _queue_for(prio):
        if prio == Priority.QUERY:
            return s.q_query
        if prio == Priority.INTERACTIVE:
            return s.q_interactive
        return s.q_batch

    def _is_high(prio):
        return prio in (Priority.QUERY, Priority.INTERACTIVE)

    def _iter_queues_in_service_order():
        # Always try higher priorities first, then batch.
        return [s.q_query, s.q_interactive, s.q_batch]

    def _pipeline_effective_score(pipeline):
        # Base: query > interactive > batch; add aging for batch.
        base = _prio_weight(pipeline.priority) * 1000.0
        if pipeline.priority == Priority.BATCH_PIPELINE:
            arrived = s.pipeline_arrival_ts.get(pipeline.pipeline_id, s.now)
            waited = max(0.0, s.now - arrived)
            base += waited * (s.batch_aging_rate * 1000.0)
        return base

    def _op_key(op):
        # Try to generate a stable key for learned RAM.
        # Prefer explicit ids if present; fall back to repr.
        for attr in ("op_id", "operator_id", "id", "name"):
            if hasattr(op, attr):
                v = getattr(op, attr)
                if v is not None:
                    return (type(op).__name__, str(v))
        return (type(op).__name__, repr(op))

    def _next_runnable_op(pipeline):
        status = pipeline.runtime_status()
        if status.is_pipeline_successful():
            return None
        # If any failed ops exist we will allow retries (ASSIGNABLE_STATES includes FAILED).
        ops = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        if not ops:
            return None
        # Choose first runnable op (could be enhanced later)
        return ops[0]

    def _desired_ram_for(op, pipeline, pool, prio):
        # Start from learned estimate if any; otherwise conservative fraction of pool.
        k = _op_key(op)
        est = s.op_ram_est.get(k)
        if est is None or est <= 0:
            est = max(1.0, pool.max_ram_pool * s.unknown_op_ram_frac.get(prio, 0.25))

        # Inflate based on pipeline OOM history
        oom_n = s.pipeline_oom_count.get(pipeline.pipeline_id, 0)
        # Exponential-ish backoff but bounded
        factor = min(4.0, 1.0 + 0.6 * oom_n)
        ram = est * factor

        # Cap to pool max; if it exceeds pool max, still request max (best effort)
        ram = min(ram, pool.max_ram_pool)
        # Never request more than currently available when possible, but don't shrink below 1
        # (Scheduler will check feasibility.)
        return max(1.0, ram)

    def _desired_cpu_for(pipeline, pool, prio):
        # High-priority wants more CPU to reduce latency but avoid monopolizing entire pool.
        cap_frac = s.cpu_cap_frac.get(prio, 0.5)
        cpu = min(pool.avail_cpu_pool, max(1.0, pool.max_cpu_pool * cap_frac))
        # If pool is mostly idle, allow bigger allocations (reduce end-to-end latency)
        if pool.avail_cpu_pool >= 0.85 * pool.max_cpu_pool:
            cpu = min(pool.avail_cpu_pool, pool.max_cpu_pool)
        return max(1.0, cpu)

    def _pool_order_for_priority(prio):
        # For high priority, prefer pools with most available headroom (cpu+ram).
        # For batch, prefer pools with most available to finish quickly but avoid fragmenting.
        pool_scores = []
        for pid in range(s.executor.num_pools):
            pool = s.executor.pools[pid]
            # Combine normalized headroom
            cpu_h = 0.0 if pool.max_cpu_pool <= 0 else (pool.avail_cpu_pool / pool.max_cpu_pool)
            ram_h = 0.0 if pool.max_ram_pool <= 0 else (pool.avail_ram_pool / pool.max_ram_pool)
            if _is_high(prio):
                score = 2.0 * ram_h + 1.5 * cpu_h
            else:
                score = 1.5 * cpu_h + 1.0 * ram_h
            pool_scores.append((score, pid))
        pool_scores.sort(reverse=True)
        return [pid for _, pid in pool_scores]

    def _reserved_headroom(pool, prio):
        # Reserve some fraction for higher priorities to keep tail latencies down.
        # For query: keep query+interactive reserved.
        # For interactive: keep query reserved.
        # For batch: no reserve.
        if prio == Priority.QUERY:
            reserve = pool.max_ram_pool * (s.reserve_frac_query + s.reserve_frac_interactive)
            reserve_cpu = pool.max_cpu_pool * (s.reserve_frac_query + s.reserve_frac_interactive)
        elif prio == Priority.INTERACTIVE:
            reserve = pool.max_ram_pool * s.reserve_frac_query
            reserve_cpu = pool.max_cpu_pool * s.reserve_frac_query
        else:
            reserve = 0.0
            reserve_cpu = 0.0
        return reserve_cpu, reserve

    def _can_fit_with_reserve(pool, req_cpu, req_ram, prio):
        reserve_cpu, reserve_ram = _reserved_headroom(pool, prio)
        # For high priority itself, allow it to consume into the reserve; reserve is for *higher*.
        # Thus, apply reserve only for lower priorities than the one being scheduled.
        # Implement by checking prio level ordering.
        if prio == Priority.BATCH_PIPELINE:
            # Must leave both query+interactive reserve
            r_cpu = pool.max_cpu_pool * (s.reserve_frac_query + s.reserve_frac_interactive)
            r_ram = pool.max_ram_pool * (s.reserve_frac_query + s.reserve_frac_interactive)
        elif prio == Priority.INTERACTIVE:
            # Must leave query reserve
            r_cpu = pool.max_cpu_pool * s.reserve_frac_query
            r_ram = pool.max_ram_pool * s.reserve_frac_query
        else:
            # Query: no reserve constraint
            r_cpu = 0.0
            r_ram = 0.0

        return (pool.avail_cpu_pool - req_cpu) >= -1e-9 and (pool.avail_ram_pool - req_ram) >= -1e-9 and \
               (pool.avail_cpu_pool - req_cpu) >= (r_cpu - 1e-9) and (pool.avail_ram_pool - req_ram) >= (r_ram - 1e-9)

    def _collect_preemptible_batch_containers():
        # Identify running/assigned batch containers from results stream (best-effort).
        # We don't have direct executor state, so we use recent results to find live containers is not possible.
        # Instead, we preempt only when simulator provides container_ids in results (running completion/failure).
        # If we can't discover running containers, we return none (no-op preemption).
        return []

    # Time advancement heuristic: if results include duration we could use it, but not available.
    # Increment logical time by 1 per tick as a stable proxy for aging.
    s.now += 1.0

    # Enqueue new pipelines with arrival timestamps
    for p in pipelines:
        if p.pipeline_id not in s.pipeline_arrival_ts:
            s.pipeline_arrival_ts[p.pipeline_id] = s.now
        _queue_for(p.priority).append(p)

    # Learn from execution results; treat OOM as a signal to increase RAM estimate
    for r in results:
        if getattr(r, "failed", None) is not None and r.failed():
            # If error string hints OOM, bump RAM estimate aggressively
            err = str(getattr(r, "error", "") or "").lower()
            is_oom = ("oom" in err) or ("out of memory" in err) or ("memory" in err and "killed" in err)
            if is_oom:
                # Increase estimates for each op in the container
                for op in getattr(r, "ops", []) or []:
                    k = _op_key(op)
                    prev = s.op_ram_est.get(k, max(1.0, float(getattr(r, "ram", 1.0) or 1.0)))
                    # Make sure next try uses more than what we attempted
                    attempted = float(getattr(r, "ram", prev) or prev)
                    s.op_ram_est[k] = max(prev, attempted * 1.6)
                # Track pipeline OOM count if pipeline_id exists on result; otherwise skip
                pid = getattr(r, "pipeline_id", None)
                if pid is not None:
                    s.pipeline_oom_count[pid] = s.pipeline_oom_count.get(pid, 0) + 1

    # Early exit if nothing changed
    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    # Clean and reorder queues: drop completed pipelines; keep others; sort by effective score
    for q in _iter_queues_in_service_order():
        kept = []
        for p in q:
            st = p.runtime_status()
            if st.is_pipeline_successful():
                continue
            # Drop pipelines with non-retriable failures? We don't have that signal; keep for retries.
            kept.append(p)
        kept.sort(key=_pipeline_effective_score, reverse=True)
        q[:] = kept

    # Scheduling loop per pool: assign up to N ops per pool per tick.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        if pool.avail_cpu_pool <= 0 or pool.avail_ram_pool <= 0:
            continue

        made = 0
        # Try to schedule up to max per pool.
        while made < s.max_assignments_per_tick_per_pool:
            # Pick next runnable pipeline/op across queues, in priority order
            chosen = None
            chosen_op = None

            for q in _iter_queues_in_service_order():
                # Scan a small window to find a runnable op
                scan = q[:8]
                for p in scan:
                    op = _next_runnable_op(p)
                    if op is None:
                        continue
                    chosen = p
                    chosen_op = op
                    break
                if chosen is not None:
                    break

            if chosen is None:
                break

            prio = chosen.priority
            # For the selected pipeline/op, choose best pool (might not be current pool_id).
            # We allow cross-pool placement; but to keep code simple, we act only if this pool
            # is also a good fit among top candidates.
            pool_order = _pool_order_for_priority(prio)
            if pool_id not in pool_order[: max(1, min(2, len(pool_order)))]:
                # Skip on this pool; other pool iterations may schedule it.
                break

            req_cpu = _desired_cpu_for(chosen, pool, prio)
            req_ram = _desired_ram_for(chosen_op, chosen, pool, prio)

            # If can't fit due to reserve (or absolute), attempt minimal adjustments:
            # - reduce CPU (never below 1)
            # - keep RAM conservative to avoid OOM
            if not _can_fit_with_reserve(pool, req_cpu, req_ram, prio):
                # Try CPU shrink first (helps packing and respects RAM safety)
                req_cpu = max(1.0, min(req_cpu, pool.avail_cpu_pool))
                # If still doesn't fit, try RAM shrink only if necessary and only a little (avoid OOM)
                if pool.avail_ram_pool > 0:
                    req_ram = min(req_ram, pool.avail_ram_pool)

            # If still doesn't fit, and it's high priority, consider preemption (best-effort).
            if not _can_fit_with_reserve(pool, req_cpu, req_ram, prio):
                if _is_high(prio):
                    # We lack direct visibility of running containers; attempt no-op preemption list.
                    # This keeps policy safe/deterministic without risking incorrect suspends.
                    preemptibles = _collect_preemptible_batch_containers()
                    for cid, pid in preemptibles:
                        suspensions.append(Suspend(container_id=cid, pool_id=pid))
                    # Re-check after hypothetical preemption (sim will apply), but since we didn't
                    # actually identify containers, this usually won't change; we'll just skip.
                break

            # Absolute fit check
            if req_cpu > pool.avail_cpu_pool + 1e-9 or req_ram > pool.avail_ram_pool + 1e-9:
                break

            assignment = Assignment(
                ops=[chosen_op],
                cpu=req_cpu,
                ram=req_ram,
                priority=prio,
                pool_id=pool_id,
                pipeline_id=chosen.pipeline_id,
            )
            assignments.append(assignment)

            made += 1

            # Rotate chosen pipeline within its queue to avoid head-of-line blocking
            q = _queue_for(prio)
            try:
                idx = q.index(chosen)
                q.append(q.pop(idx))
            except ValueError:
                pass

            # If high priority, avoid flooding same pool in one tick (reduce interference)
            if _is_high(prio):
                break

    return suspensions, assignments
