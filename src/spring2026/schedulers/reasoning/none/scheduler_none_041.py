# policy_key: scheduler_none_041
# reasoning_effort: none
# model: gpt-5.2-2025-12-11
# llm_cost: 0.052346
# generation_seconds: 45.01
# generated_at: 2026-04-09T22:17:02.102405
@register_scheduler_init(key="scheduler_none_041")
def scheduler_none_041_init(s):
    """Priority-aware, OOM-adaptive, low-churn scheduler.

    Goals for the weighted-latency objective:
    - Strongly protect QUERY/INTERACTIVE (dominant weights) with headroom admission + selective preemption.
    - Avoid failures (720s penalty) via OOM-triggered RAM backoff (increase on retry) and conservative initial RAM.
    - Avoid starvation via light aging and always allowing some batch progress when safe.

    Key ideas:
    - Maintain per-pipeline RAM multiplier that increases on OOM failures; decreases slowly on successes.
    - Assign at most one op per pipeline per tick, favoring high priority and "oldest waiting".
    - Reserve a fraction of each pool for high priority; allow batch to use it only when queue is empty.
    - Preempt only when high priority cannot be placed and there exists low-priority running work in that pool.
    """
    s.waiting = []  # list of Pipeline
    s.arrival_seq = 0
    s.pipeline_meta = {}  # pid -> dict(meta)
    s.last_tick_had_change = True

    # RAM sizing control
    s.base_ram_frac = 0.55     # initial RAM fraction of pool for an op (cap with pool avail)
    s.min_ram_frac = 0.20
    s.max_ram_frac = 0.90
    s.oom_backoff = 1.6        # multiply RAM fraction on OOM
    s.success_decay = 0.95     # reduce RAM fraction slowly after successes

    # CPU sizing control
    s.base_cpu_frac = 0.75     # allocate substantial CPU to reduce latency; cap to avail
    s.min_cpu_frac = 0.25

    # Reservation / protection
    s.reserve_query_frac = 0.25        # reserve this much pool capacity for QUERY
    s.reserve_interactive_frac = 0.15  # reserve this much pool capacity for INTERACTIVE
    s.max_preempt_per_tick = 2         # low churn

    # Aging to prevent starvation (adds small score per second in queue)
    s.aging_per_sec = 0.0005

    # Track last seen sim time if available (optional)
    s._last_time = None


@register_scheduler(key="scheduler_none_041")
def scheduler_none_041(s, results, pipelines):
    """
    See init docstring for policy overview.
    """
    # ---------- helpers (kept inside to avoid imports at top-level) ----------
    def _prio_weight(prio):
        # Larger means more important.
        if prio == Priority.QUERY:
            return 3
        if prio == Priority.INTERACTIVE:
            return 2
        return 1

    def _ensure_meta(p):
        pid = p.pipeline_id
        if pid not in s.pipeline_meta:
            s.pipeline_meta[pid] = {
                "seq": s.arrival_seq,
                "ram_frac": s.base_ram_frac,
                "fail_oom": 0,
                "last_queue_time": 0.0,
            }
            s.arrival_seq += 1
        return s.pipeline_meta[pid]

    def _infer_now():
        # If simulator exposes time on scheduler or executor, use it; otherwise use monotonic ticks.
        # We avoid imports; rely on attributes if present.
        if hasattr(s, "now"):
            try:
                return float(s.now)
            except Exception:
                pass
        if hasattr(s.executor, "now"):
            try:
                return float(s.executor.now)
            except Exception:
                pass
        if hasattr(s.executor, "time"):
            try:
                return float(s.executor.time)
            except Exception:
                pass
        # fallback: tick counter
        if not hasattr(s, "_tick"):
            s._tick = 0
        s._tick += 1
        return float(s._tick)

    def _is_oom_failure(res):
        # Best-effort detection; error could be string/enum.
        if not res.failed():
            return False
        err = getattr(res, "error", None)
        if err is None:
            return False
        try:
            es = str(err).lower()
        except Exception:
            return False
        return ("oom" in es) or ("out of memory" in es) or ("memory" in es and "exceed" in es)

    def _pipeline_done_or_failed(p):
        st = p.runtime_status()
        if st.is_pipeline_successful():
            return True
        # If any operator failed, we consider pipeline failed and stop retrying only if it wasn't OOM.
        # But we don't know operator-level reasons; handle via results-based OOM backoff.
        # Here: don't drop the pipeline just because it has FAILED ops; allow retries (ASSIGNABLE includes FAILED).
        return False

    def _assignable_ops(p):
        st = p.runtime_status()
        if st.is_pipeline_successful():
            return []
        # Take only one op to reduce contention and avoid oversubscription.
        op_list = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
        return op_list

    def _running_low_priority_containers(pool_id, min_priority_to_keep):
        # Attempt to read running containers from executor/pool. If not available, return [].
        pool = s.executor.pools[pool_id]
        candidates = []
        # Common attribute names guess; simulator may expose different structures.
        container_lists = []
        for attr in ("containers", "running_containers", "active_containers"):
            if hasattr(pool, attr):
                try:
                    container_lists.append(getattr(pool, attr))
                except Exception:
                    pass
        # If none found, try executor-level
        if not container_lists:
            for attr in ("containers", "running_containers", "active_containers"):
                if hasattr(s.executor, attr):
                    try:
                        container_lists.append(getattr(s.executor, attr))
                    except Exception:
                        pass

        for cl in container_lists:
            try:
                it = cl.values() if hasattr(cl, "values") else cl
            except Exception:
                continue
            for c in it:
                try:
                    c_pool_id = getattr(c, "pool_id", pool_id)
                    if c_pool_id != pool_id:
                        continue
                    c_prio = getattr(c, "priority", None)
                    if c_prio is None:
                        continue
                    # Preempt only if strictly lower than required
                    if _prio_weight(c_prio) < _prio_weight(min_priority_to_keep):
                        cid = getattr(c, "container_id", None)
                        if cid is None:
                            cid = getattr(c, "id", None)
                        if cid is not None:
                            candidates.append(cid)
                except Exception:
                    continue
        return candidates

    def _compute_reserve(prio, pool):
        # Return (reserve_cpu, reserve_ram) that must remain for higher priorities.
        # Only enforce reserve when there exists higher-priority work waiting.
        reserve_cpu = 0.0
        reserve_ram = 0.0
        if prio == Priority.BATCH_PIPELINE:
            reserve_cpu = pool.max_cpu_pool * (s.reserve_query_frac + s.reserve_interactive_frac)
            reserve_ram = pool.max_ram_pool * (s.reserve_query_frac + s.reserve_interactive_frac)
        elif prio == Priority.INTERACTIVE:
            reserve_cpu = pool.max_cpu_pool * s.reserve_query_frac
            reserve_ram = pool.max_ram_pool * s.reserve_query_frac
        else:
            reserve_cpu = 0.0
            reserve_ram = 0.0
        return reserve_cpu, reserve_ram

    def _waiting_has_higher(prio):
        # Check if there is waiting work with higher priority than prio.
        for p in s.waiting:
            if _pipeline_done_or_failed(p):
                continue
            if _prio_weight(p.priority) > _prio_weight(prio):
                if _assignable_ops(p):
                    return True
        return False

    # ---------- ingest arrivals ----------
    for p in pipelines:
        _ensure_meta(p)
        s.waiting.append(p)

    # early exit if no changes
    if not pipelines and not results and not s.last_tick_had_change:
        return [], []

    now = _infer_now()

    # ---------- process results: adapt RAM on OOM / decay on success ----------
    # Also, if a pipeline failed non-OOM repeatedly, we still keep it; but avoid infinite churn by modest scaling.
    for r in results:
        # Find pipeline meta by pipeline_id if present on result; otherwise by searching op ownership (not available).
        pid = getattr(r, "pipeline_id", None)
        if pid is None:
            # best-effort: if op has pipeline id attribute
            try:
                if r.ops and hasattr(r.ops[0], "pipeline_id"):
                    pid = r.ops[0].pipeline_id
            except Exception:
                pid = None
        if pid is None or pid not in s.pipeline_meta:
            continue
        meta = s.pipeline_meta[pid]
        if r.failed():
            if _is_oom_failure(r):
                meta["fail_oom"] += 1
                meta["ram_frac"] = min(s.max_ram_frac, meta["ram_frac"] * s.oom_backoff)
            else:
                # Non-OOM failure: slightly increase RAM as a hedge (might still be memory-related),
                # but less aggressively to avoid hogging.
                meta["ram_frac"] = min(s.max_ram_frac, meta["ram_frac"] * 1.15)
        else:
            # success: decay RAM fraction slowly to improve packing
            meta["ram_frac"] = max(s.min_ram_frac, meta["ram_frac"] * s.success_decay)

    # ---------- clean & score waiting pipelines ----------
    # Keep only incomplete pipelines; preserve ordering but we'll sort for dispatch.
    cleaned = []
    for p in s.waiting:
        if _pipeline_done_or_failed(p):
            continue
        cleaned.append(p)
    s.waiting = cleaned

    # Aging: track last_queue_time; approximate waiting time by "now - last_queue_time" increments.
    for p in s.waiting:
        meta = _ensure_meta(p)
        if meta["last_queue_time"] == 0.0:
            meta["last_queue_time"] = now

    def _pipeline_rank_key(p):
        meta = _ensure_meta(p)
        age = max(0.0, now - meta["last_queue_time"])
        # Higher is better: priority weight dominates; aging helps prevent starvation.
        return (_prio_weight(p.priority) * 1000.0) + (age * s.aging_per_sec * 1000.0) - (meta["seq"] * 1e-6)

    # Sort by descending rank
    s.waiting.sort(key=_pipeline_rank_key, reverse=True)

    suspensions = []
    assignments = []

    # ---------- attempt to schedule across pools ----------
    # Strategy: iterate pools, try to place the best-ranked assignable op that fits (respecting reserves).
    # If cannot place a high priority op due to lack of capacity, attempt targeted preemption of low priority in that pool.
    preempts_used = 0
    made_assignment = False

    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)
        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # We'll attempt multiple placements per pool while there is capacity.
        # To keep churn low, cap placements per pool per tick.
        placements_this_pool = 0
        max_place_per_pool = 2

        while placements_this_pool < max_place_per_pool:
            # Find first waiting pipeline with an assignable op that can fit given reserves.
            chosen = None
            chosen_ops = None
            chosen_cpu = None
            chosen_ram = None

            for p in s.waiting:
                ops = _assignable_ops(p)
                if not ops:
                    continue

                meta = _ensure_meta(p)

                # Compute reservable headroom: enforce only if higher priority work exists
                reserve_cpu, reserve_ram = _compute_reserve(p.priority, pool)
                if not _waiting_has_higher(p.priority):
                    reserve_cpu = 0.0
                    reserve_ram = 0.0

                effective_cpu = max(0.0, avail_cpu - reserve_cpu)
                effective_ram = max(0.0, avail_ram - reserve_ram)
                if effective_cpu <= 0.0 or effective_ram <= 0.0:
                    continue

                # Size the container: favor enough CPU for latency, but don't monopolize entire pool.
                cpu_req = max(pool.max_cpu_pool * s.min_cpu_frac, min(effective_cpu, pool.max_cpu_pool * s.base_cpu_frac))
                # RAM: per-pipeline adaptive fraction; cap by effective RAM.
                ram_req = min(effective_ram, max(pool.max_ram_pool * s.min_ram_frac, pool.max_ram_pool * meta["ram_frac"]))

                # Basic fit check
                if cpu_req <= effective_cpu + 1e-9 and ram_req <= effective_ram + 1e-9 and cpu_req > 0 and ram_req > 0:
                    chosen = p
                    chosen_ops = ops
                    chosen_cpu = cpu_req
                    chosen_ram = ram_req
                    break

            if chosen is None:
                # If we have high priority waiting and can't fit due to reservation/availability,
                # attempt preemption of low priority running work to free headroom.
                if preempts_used < s.max_preempt_per_tick:
                    # Find top-ranked high-priority pipeline that has an assignable op.
                    hp = None
                    for p in s.waiting:
                        if _prio_weight(p.priority) >= _prio_weight(Priority.INTERACTIVE):
                            if _assignable_ops(p):
                                hp = p
                                break
                    if hp is None:
                        break  # no urgent work
                    # Preempt low-priority containers in this pool (if we can detect them).
                    cand_ids = _running_low_priority_containers(pool_id, min_priority_to_keep=hp.priority)
                    if cand_ids:
                        # Suspend at most one per attempt for low churn
                        suspensions.append(Suspend(cand_ids[0], pool_id))
                        preempts_used += 1
                        # After preemption, resources won't be available until next tick; break to avoid overscheduling.
                        break
                break

            # Emit assignment
            assignments.append(
                Assignment(
                    ops=chosen_ops,
                    cpu=chosen_cpu,
                    ram=chosen_ram,
                    priority=chosen.priority,
                    pool_id=pool_id,
                    pipeline_id=chosen.pipeline_id,
                )
            )
            made_assignment = True
            placements_this_pool += 1

            # Update local availability estimates
            avail_cpu -= chosen_cpu
            avail_ram -= chosen_ram

            # Update queue time marker so "age" doesn't explode while it is getting service attempts
            s.pipeline_meta[chosen.pipeline_id]["last_queue_time"] = now

            # Keep pipeline in waiting; it may have more ops later, but we only assign one per tick.

            # Stop if pool nearly exhausted
            if avail_cpu <= 0.0 or avail_ram <= 0.0:
                break

    s.last_tick_had_change = bool(pipelines or results or made_assignment or suspensions)
    return suspensions, assignments
