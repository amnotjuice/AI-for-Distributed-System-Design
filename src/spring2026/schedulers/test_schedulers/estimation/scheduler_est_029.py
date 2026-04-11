# policy_key: scheduler_est_029
# reasoning_effort: low
# exp: estimation
# model: gpt-5.2-2025-12-11
# llm_cost: 0.050330
# generation_seconds: 61.63
# generated_at: 2026-04-10T10:08:41.755512
@register_scheduler_init(key="scheduler_est_029")
def scheduler_est_029_init(s):
    """
    Memory-aware, priority-weighted greedy scheduler.

    Goals:
      - Reduce OOM failures using op.estimate.mem_peak_gb as a noisy hint.
      - Protect QUERY/INTERACTIVE latency via strict priority ordering + slightly more resources.
      - Avoid starvation via simple aging boost for BATCH.
      - Retry failed operators with increasing RAM "boost" (assumes many failures are memory-related).

    Design choices (kept intentionally simple vs. complex preemption):
      - No preemption (suspensions always empty) to avoid churn/wasted work.
      - Greedy packing per pool: schedule smallest-estimated-memory ops first within a priority band.
      - Conservative RAM sizing for high priority; adaptive RAM increase on failures.
    """
    # Logical time (ticks) for aging.
    s._tick = 0

    # Pipelines we know about (by id), and first-seen tick for aging.
    s._known_pipelines = {}          # pipeline_id -> Pipeline
    s._first_seen_tick = {}          # pipeline_id -> int

    # Per-operator retry/boost state.
    # Keyed by (pipeline_id, op_key) where op_key is a stable identifier when available.
    s._op_fail_count = {}            # (pid, opk) -> int
    s._op_ram_boost = {}             # (pid, opk) -> float (multiplier >= 1.0)

    # Pipelines to stop trying (too many repeated failures).
    s._abandoned_pipelines = set()   # set[pipeline_id]


@register_scheduler(key="scheduler_est_029")
def scheduler_est_029(s, results, pipelines):
    """
    Scheduler step.

    Strategy:
      1) Ingest new pipelines and update failure-based RAM boosts from recent results.
      2) Build a candidate list of assignable ops (parents complete) across all active pipelines.
      3) Sort candidates by:
           - Effective priority (QUERY > INTERACTIVE > BATCH with mild aging for BATCH)
           - Estimated memory ascending (pack small ops first to reduce fragmentation/OOM)
      4) For each pool, greedily assign as many ops as fit (RAM & CPU).
    """
    s._tick += 1

    # -----------------------------
    # Helpers (defined inside function to avoid imports at top-level)
    # -----------------------------
    def _priority_rank(pri):
        # Lower is better (sorted ascending).
        if pri == Priority.QUERY:
            return 0
        if pri == Priority.INTERACTIVE:
            return 1
        return 2  # Priority.BATCH_PIPELINE and anything else

    def _op_identifier(op):
        # Try common attribute names; otherwise fall back to Python object identity.
        for attr in ("operator_id", "op_id", "id", "name"):
            if hasattr(op, attr):
                try:
                    v = getattr(op, attr)
                    # Ensure hashable and stable-ish.
                    if isinstance(v, (int, str, tuple)):
                        return v
                except Exception:
                    pass
        return id(op)

    def _get_est_mem_gb(op):
        try:
            est = getattr(op, "estimate", None)
            if est is None:
                return None
            v = getattr(est, "mem_peak_gb", None)
            if v is None:
                return None
            # Guard against bad values.
            if v < 0:
                return None
            return float(v)
        except Exception:
            return None

    def _clamp(x, lo, hi):
        if x < lo:
            return lo
        if x > hi:
            return hi
        return x

    def _age_boost(pipeline_id):
        # Mild aging only for batch to avoid starvation.
        p = s._known_pipelines.get(pipeline_id)
        if p is None or p.priority != Priority.BATCH_PIPELINE:
            return 0.0
        first = s._first_seen_tick.get(pipeline_id, s._tick)
        waited = max(0, s._tick - first)
        # Every ~20 ticks of waiting gives a small boost (capped).
        return min(0.75, waited / 80.0)

    def _effective_rank(pipeline_id, pri):
        # Combine base priority with aging (aging can move BATCH slightly up).
        base = _priority_rank(pri)
        if pri == Priority.BATCH_PIPELINE:
            # Move batch closer to interactive as it ages, but never above interactive.
            # base=2; subtract up to 0.75 -> at best 1.25 (still below interactive rank=1).
            return base - _age_boost(pipeline_id)
        return float(base)

    def _cpu_target(pool, pri):
        # Reserve more CPU for high priority, but still allow packing multiple ops.
        if pri == Priority.QUERY:
            frac = 0.75
        elif pri == Priority.INTERACTIVE:
            frac = 0.60
        else:
            frac = 0.50
        # At least 1 vCPU when possible.
        tgt = int(max(1, round(pool.max_cpu_pool * frac)))
        return tgt

    def _ram_target(pool, pri, est_mem, boost):
        # Base safety factor; higher for high priority to reduce failure risk.
        if pri == Priority.QUERY:
            safety = 1.35
            floor_frac = 0.40
        elif pri == Priority.INTERACTIVE:
            safety = 1.30
            floor_frac = 0.35
        else:
            safety = 1.25
            floor_frac = 0.30

        # If no estimate, allocate a conservative chunk of the pool.
        if est_mem is None:
            base = pool.max_ram_pool * (0.65 if pri in (Priority.QUERY, Priority.INTERACTIVE) else 0.55)
        else:
            base = est_mem * safety

        # Apply retry-based boost.
        base *= boost

        # Enforce a minimum floor to avoid tiny allocations that often OOM in practice.
        base = max(base, pool.max_ram_pool * floor_frac)

        # Cap at pool maximum.
        return _clamp(base, 0.0, pool.max_ram_pool)

    def _likely_fits_pool(pool, est_mem):
        # If estimate is wildly above max RAM, usually infeasible; but noisy estimates can be high.
        if est_mem is None:
            return True
        # Allow slight overshoot to account for estimator overestimation (noisy).
        return est_mem <= pool.max_ram_pool * 1.10

    # -----------------------------
    # 1) Ingest new pipelines
    # -----------------------------
    for p in pipelines:
        s._known_pipelines[p.pipeline_id] = p
        if p.pipeline_id not in s._first_seen_tick:
            s._first_seen_tick[p.pipeline_id] = s._tick

    # -----------------------------
    # 2) Update failure-based boost from results
    # -----------------------------
    # Note: We do not assume a specific error string, but if it looks like OOM we boost more.
    for r in results:
        try:
            if not r.failed():
                continue
        except Exception:
            continue

        # For each failed op, increase RAM boost for retry.
        for op in getattr(r, "ops", []) or []:
            pid = getattr(r, "pipeline_id", None)
            # If pipeline_id isn't on result, try to infer from op (rare); else skip boost.
            if pid is None:
                # Best effort: results API in prompt doesn't guarantee pipeline_id; skip if absent.
                continue

            opk = _op_identifier(op)
            key = (pid, opk)

            s._op_fail_count[key] = s._op_fail_count.get(key, 0) + 1

            # Detect OOM-ish signal (best-effort).
            err = getattr(r, "error", "")
            err_s = str(err).lower()
            oomish = ("oom" in err_s) or ("out of memory" in err_s) or ("memory" in err_s)

            prev_boost = s._op_ram_boost.get(key, 1.0)
            # Boost more aggressively if it smells like OOM; otherwise moderate.
            mult = 1.60 if oomish else 1.30
            new_boost = min(4.0, prev_boost * mult)
            s._op_ram_boost[key] = new_boost

            # If an operator keeps failing, abandon the pipeline to avoid endless churn.
            # (This is a tradeoff; but repeated failures often imply infeasible memory.)
            if s._op_fail_count[key] >= 4:
                s._abandoned_pipelines.add(pid)

    # -----------------------------
    # 3) Build candidate ops across pipelines
    # -----------------------------
    candidates = []
    for pid, p in list(s._known_pipelines.items()):
        if pid in s._abandoned_pipelines:
            continue

        status = None
        try:
            status = p.runtime_status()
        except Exception:
            continue

        # Drop successful pipelines from consideration.
        try:
            if status.is_pipeline_successful():
                continue
        except Exception:
            pass

        # Only schedule ops whose parents are complete.
        try:
            ops = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True) or []
        except Exception:
            ops = []

        # Add all ready ops; greedy selection later.
        for op in ops:
            opk = _op_identifier(op)
            est_mem = _get_est_mem_gb(op)
            boost = s._op_ram_boost.get((pid, opk), 1.0)

            # Sort key parts:
            #  - effective rank (priority + aging)
            #  - estimated memory (None treated as larger to avoid overpacking unknowns)
            mem_sort = est_mem if est_mem is not None else 1e18

            candidates.append({
                "pipeline": p,
                "pipeline_id": pid,
                "priority": p.priority,
                "op": op,
                "opk": opk,
                "est_mem": est_mem,
                "mem_sort": mem_sort,
                "eff_rank": _effective_rank(pid, p.priority),
                "boost": boost,
            })

    if not candidates and not pipelines and not results:
        return [], []

    # Priority first, then memory ascending.
    candidates.sort(key=lambda c: (c["eff_rank"], c["mem_sort"]))

    # -----------------------------
    # 4) Greedy assignment per pool
    # -----------------------------
    suspensions = []
    assignments = []

    # Track which (pid, opk) we assigned this tick to avoid duplicates.
    assigned_set = set()

    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]

        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool

        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        made_progress = True
        while made_progress:
            made_progress = False

            # Find first candidate that plausibly fits (estimate-wise) and fits available resources.
            chosen_idx = None
            chosen_cpu = None
            chosen_ram = None

            for i, c in enumerate(candidates):
                pid = c["pipeline_id"]
                opk = c["opk"]
                if (pid, opk) in assigned_set:
                    continue

                # Skip abandoned pipelines.
                if pid in s._abandoned_pipelines:
                    continue

                est_mem = c["est_mem"]
                if not _likely_fits_pool(pool, est_mem):
                    continue

                # Compute requested resources.
                cpu_req = _cpu_target(pool, c["priority"])
                cpu_req = min(cpu_req, avail_cpu)
                if cpu_req <= 0:
                    continue

                ram_req = _ram_target(pool, c["priority"], est_mem, c["boost"])
                ram_req = min(ram_req, avail_ram)

                # If RAM target collapses too small due to avail_ram, avoid risky placements for high priority.
                if c["priority"] in (Priority.QUERY, Priority.INTERACTIVE):
                    # If we cannot give at least 70% of the computed target, skip to reduce failure risk.
                    full_target = _ram_target(pool, c["priority"], est_mem, c["boost"])
                    if full_target > 0 and ram_req < 0.70 * full_target:
                        continue

                # Must fit both.
                if ram_req <= 0 or ram_req > avail_ram:
                    continue
                if cpu_req > avail_cpu:
                    continue

                chosen_idx = i
                chosen_cpu = cpu_req
                chosen_ram = ram_req
                break

            if chosen_idx is None:
                break

            c = candidates[chosen_idx]
            p = c["pipeline"]
            op = c["op"]

            assignments.append(Assignment(
                ops=[op],
                cpu=chosen_cpu,
                ram=chosen_ram,
                priority=p.priority,
                pool_id=pool_id,
                pipeline_id=p.pipeline_id
            ))

            assigned_set.add((c["pipeline_id"], c["opk"]))
            avail_cpu -= chosen_cpu
            avail_ram -= chosen_ram
            made_progress = True

            if avail_cpu <= 0 or avail_ram <= 0:
                break

    return suspensions, assignments
