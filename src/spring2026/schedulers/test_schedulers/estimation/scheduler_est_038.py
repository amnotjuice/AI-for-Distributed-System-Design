# policy_key: scheduler_est_038
# reasoning_effort: low
# exp: estimation
# model: gpt-5.2-2025-12-11
# llm_cost: 0.048552
# generation_seconds: 52.28
# generated_at: 2026-04-10T10:16:18.619099
@register_scheduler_init(key="scheduler_est_038")
def scheduler_est_038_init(s):
    """
    Memory-aware, priority-first, failure-adaptive scheduler.

    Core ideas:
      - Protect QUERY and INTERACTIVE latency by always selecting their ready operators first.
      - Use op.estimate.mem_peak_gb as a (noisy) hint to (a) avoid obviously infeasible placements and
        (b) right-size RAM with a safety factor to reduce OOM-induced failures (which are heavily penalized).
      - Keep pipelines alive even after failures (FAILED is assignable) and adapt RAM upward on failures.
      - Pack multiple smaller operators per pool when possible; prefer "fits-now" operators to keep completion rate high.
    """
    s._est038_seq = 0  # monotonically increasing arrival order
    s._est038_pipelines = []  # active pipelines (including failed ones; we retry)
    s._est038_meta = {}  # pipeline_id -> dict(seq=int, wait=int, ram_mult=float)
    s._est038_op_retries = {}  # (pipeline_id, op_object_id) -> int

    # Tunables (small improvements over naive FIFO)
    s._est038_min_ram_gb = 0.5  # never allocate less than this
    s._est038_max_ram_mult = 6.0  # cap on adaptive multiplier
    s._est038_fail_ram_bump = 1.6  # multiplier increase on failure

    # Priority handling
    s._est038_prio_rank = {
        Priority.QUERY: 0,
        Priority.INTERACTIVE: 1,
        Priority.BATCH_PIPELINE: 2,
    }
    s._est038_prio_cpu_quantum = {
        Priority.QUERY: 4.0,
        Priority.INTERACTIVE: 2.0,
        Priority.BATCH_PIPELINE: 1.0,
    }
    # Safety factor on estimator (higher for high-priority to reduce OOM / retries)
    s._est038_prio_mem_safety = {
        Priority.QUERY: 1.35,
        Priority.INTERACTIVE: 1.25,
        Priority.BATCH_PIPELINE: 1.15,
    }
    # If estimate is missing, reserve a conservative fraction of pool RAM (higher for high priority)
    s._est038_prio_fallback_pool_frac = {
        Priority.QUERY: 0.55,
        Priority.INTERACTIVE: 0.45,
        Priority.BATCH_PIPELINE: 0.35,
    }


@register_scheduler(key="scheduler_est_038")
def scheduler_est_038_scheduler(s, results, pipelines):
    def _get_meta(p):
        m = s._est038_meta.get(p.pipeline_id)
        if m is None:
            m = {"seq": s._est038_seq, "wait": 0, "ram_mult": 1.0}
            s._est038_seq += 1
            s._est038_meta[p.pipeline_id] = m
        return m

    def _is_done(p):
        st = p.runtime_status()
        return st.is_pipeline_successful()

    def _estimate_mem_gb(op):
        est = None
        try:
            if getattr(op, "estimate", None) is not None:
                est = op.estimate.mem_peak_gb
        except Exception:
            est = None
        if est is None:
            return None
        try:
            est = float(est)
        except Exception:
            return None
        if est < 0:
            return 0.0
        return est

    def _desired_ram_gb(p, op, pool):
        pr = p.priority
        meta = _get_meta(p)

        est = _estimate_mem_gb(op)
        if est is None:
            # No estimate yet: allocate a conservative portion of pool RAM to reduce surprise OOMs.
            ram = pool.max_ram_pool * s._est038_prio_fallback_pool_frac.get(pr, 0.40)
        else:
            safety = s._est038_prio_mem_safety.get(pr, 1.2)
            ram = est * safety

        # Adaptive bump for pipelines that have seen failures (assume memory-related unless proven otherwise).
        ram *= meta["ram_mult"]

        # Clamp to pool limits and a small minimum.
        if ram < s._est038_min_ram_gb:
            ram = s._est038_min_ram_gb
        if ram > pool.max_ram_pool:
            ram = pool.max_ram_pool
        return ram

    def _desired_cpu(p, pool_avail_cpu):
        pr = p.priority
        q = s._est038_prio_cpu_quantum.get(pr, 1.0)
        # Keep at least 1 CPU if available; avoid grabbing entire pool to allow concurrency.
        cpu = min(pool_avail_cpu, q)
        if cpu < 1.0 and pool_avail_cpu >= 1.0:
            cpu = 1.0
        return cpu

    def _fits(pool, cpu_need, ram_need):
        return (pool.avail_cpu_pool >= cpu_need) and (pool.avail_ram_pool >= ram_need)

    def _update_from_results():
        # Adapt RAM upward when ops fail (commonly OOM); retrying is preferable to dropping.
        for r in results:
            if not getattr(r, "failed", None):
                continue
            if not r.failed():
                continue

            # Attempt to bump pipeline ram multiplier; also count op retries.
            pid = getattr(r, "pipeline_id", None)
            # Some sims may not include pipeline_id on results; fall back to using r.ops' pipeline association if present.
            # If unavailable, still bump nothing.
            if pid is None:
                continue

            # Ensure meta exists
            m = s._est038_meta.get(pid)
            if m is None:
                m = {"seq": s._est038_seq, "wait": 0, "ram_mult": 1.0}
                s._est038_seq += 1
                s._est038_meta[pid] = m

            # Count retries per op object id
            for op in getattr(r, "ops", []) or []:
                key = (pid, id(op))
                s._est038_op_retries[key] = s._est038_op_retries.get(key, 0) + 1

            # Increase multiplier (capped)
            m["ram_mult"] = min(s._est038_max_ram_mult, m["ram_mult"] * s._est038_fail_ram_bump)

    def _refresh_active_pipelines(new_pipelines):
        # Add new arrivals; keep existing active ones.
        for p in new_pipelines:
            _get_meta(p)
            s._est038_pipelines.append(p)

        # Drop completed pipelines from active list to reduce scanning cost.
        still_active = []
        for p in s._est038_pipelines:
            if _is_done(p):
                continue
            still_active.append(p)
        s._est038_pipelines = still_active

        # Aging to prevent starvation: increase wait counter for non-completed pipelines.
        for p in s._est038_pipelines:
            m = _get_meta(p)
            m["wait"] += 1

    def _ready_ops_for_pipeline(p):
        st = p.runtime_status()
        # Only schedule ops whose parents are complete; this prevents wasted placements.
        ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True) or []
        return ops

    def _candidate_sort_key(p, op):
        # Priority first (QUERY then INTERACTIVE then BATCH), then smaller mem first, then older arrival,
        # with mild aging so long-waiting pipelines get chances.
        m = _get_meta(p)
        pr_rank = s._est038_prio_rank.get(p.priority, 99)

        est = _estimate_mem_gb(op)
        # Unknown estimates are treated as larger to avoid clogging when resources are tight.
        est_key = est if est is not None else 1e18

        # Aging: every 50 ticks of waiting reduces effective rank by 1 (cannot surpass QUERY).
        age_boost = m["wait"] // 50
        eff_rank = max(0, pr_rank - age_boost)

        return (eff_rank, est_key, m["seq"])

    def _pool_order():
        # Prefer pools with more free RAM to reduce OOM/retry risk, while still filling smaller pools.
        idxs = list(range(s.executor.num_pools))
        idxs.sort(key=lambda i: (s.executor.pools[i].avail_ram_pool, s.executor.pools[i].avail_cpu_pool), reverse=True)
        return idxs

    def _pick_best_fit_for_pool(pool_id):
        pool = s.executor.pools[pool_id]
        if pool.avail_cpu_pool <= 0 or pool.avail_ram_pool <= 0:
            return None

        # Build a small candidate list (ready ops) each time; keep it simple and deterministic.
        cands = []
        for p in s._est038_pipelines:
            if _is_done(p):
                continue
            ops = _ready_ops_for_pipeline(p)
            if not ops:
                continue
            # Only consider the first few assignable ops per pipeline to reduce scanning overhead.
            for op in ops[:2]:
                cands.append((p, op))

        if not cands:
            return None

        cands.sort(key=lambda t: _candidate_sort_key(t[0], t[1]))

        # Choose the first that "fits now" on this pool with our sizing.
        for (p, op) in cands:
            cpu_need = _desired_cpu(p, pool.avail_cpu_pool)
            if cpu_need <= 0:
                continue

            ram_need = _desired_ram_gb(p, op, pool)

            # If we have an estimate and it clearly exceeds pool max, still allow an attempt at max (clamped)
            # but only if there's enough RAM right now.
            if not _fits(pool, cpu_need, ram_need):
                continue

            return (p, op, cpu_need, ram_need)

        return None

    # Main control
    if not pipelines and not results:
        return [], []

    _update_from_results()
    _refresh_active_pipelines(pipelines)

    suspensions = []
    assignments = []

    # Try to make multiple assignments per pool while resources remain.
    for pool_id in _pool_order():
        pool = s.executor.pools[pool_id]

        # Greedily pack: schedule as long as we can place something.
        # Avoid infinite loops by limiting the number of placements per pool per tick.
        max_places = 32
        places = 0
        while places < max_places and pool.avail_cpu_pool > 0 and pool.avail_ram_pool > 0:
            picked = _pick_best_fit_for_pool(pool_id)
            if picked is None:
                break

            p, op, cpu_need, ram_need = picked

            # Create assignment for a single op at a time (keeps failure recovery granular).
            assignment = Assignment(
                ops=[op],
                cpu=cpu_need,
                ram=ram_need,
                priority=p.priority,
                pool_id=pool_id,
                pipeline_id=p.pipeline_id,
            )
            assignments.append(assignment)

            # Optimistically account for consumed resources to enable multi-placement in one tick.
            # (The simulator will enforce actual availability; this just prevents over-issuing.)
            try:
                pool.avail_cpu_pool -= cpu_need
                pool.avail_ram_pool -= ram_need
            except Exception:
                # If pool fields are read-only in this environment, we still proceed; simulator will gate actual execution.
                pass

            places += 1

    return suspensions, assignments
