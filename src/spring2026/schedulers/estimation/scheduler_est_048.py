# policy_key: scheduler_est_048
# reasoning_effort: low
# exp: estimation
# model: gpt-5.2-2025-12-11
# llm_cost: 0.057638
# generation_seconds: 45.21
# generated_at: 2026-04-10T10:24:27.098080
@register_scheduler_init(key="scheduler_est_048")
def scheduler_est_048_init(s):
    """Priority + memory-aware scheduler with OOM-adaptive RAM sizing.

    Goals:
      - Minimize weighted latency (query >> interactive >> batch) by prioritizing high-priority work.
      - Reduce costly failures (720s penalty) by using op.estimate.mem_peak_gb to avoid likely OOM placements.
      - Maintain progress for batch via mild aging to avoid indefinite starvation.

    Key ideas:
      1) Maintain per-pipeline enqueue order + aging.
      2) Select assignable ops from highest "effective priority" pipelines first.
      3) Place ops into pools where estimated memory best fits (min leftover RAM), skipping infeasible placements.
      4) On failures, increase a per-operator RAM multiplier (especially for likely-OOM) and retry.
    """
    from collections import deque

    s._tick = 0
    s._arrival_seq = 0

    # Pipelines waiting for scheduling decisions (may be requeued across ticks)
    s._waiting = deque()

    # Per-pipeline metadata
    s._pipeline_meta = {}  # pipeline_id -> dict(arrival_seq, enq_tick)

    # Per-operator retry / sizing state (keyed by (pipeline_id, id(op)))
    # Stores: {"mult": float, "fails": int}
    s._op_state = {}

    # Tuning knobs (conservative defaults; can be adjusted experimentally)
    s._base_headroom = 1.20          # default multiplicative headroom on estimator
    s._oom_like_keywords = ("oom", "out of memory", "out-of-memory", "cuda oom", "killed")  # best-effort
    s._max_ram_mult = 8.0            # cap RAM multiplier escalation
    s._max_fail_retries = 4          # avoid infinite retry loops for truly-broken ops


@register_scheduler(key="scheduler_est_048")
def scheduler_est_048_scheduler(s, results, pipelines):
    from collections import defaultdict

    s._tick += 1

    # --- ingest new pipelines ---
    for p in pipelines:
        pid = p.pipeline_id
        if pid not in s._pipeline_meta:
            s._pipeline_meta[pid] = {"arrival_seq": s._arrival_seq, "enq_tick": s._tick}
            s._arrival_seq += 1
        else:
            # If a pipeline re-arrives (unlikely), refresh enqueue tick
            s._pipeline_meta[pid]["enq_tick"] = s._tick
        s._waiting.append(p)

    # --- process results: adapt RAM on failures ---
    # We don't have direct "OOM" signals in the API beyond error strings; treat any failure as a retry signal,
    # but escalate RAM more aggressively if the error looks OOM-like.
    for r in results:
        if not getattr(r, "failed", lambda: False)():
            continue
        err = (getattr(r, "error", None) or "")
        err_l = err.lower() if isinstance(err, str) else ""
        oom_like = any(k in err_l for k in s._oom_like_keywords)

        for op in getattr(r, "ops", []) or []:
            key = (getattr(r, "pipeline_id", None), id(op))
            # If pipeline_id isn't available on result, fall back to a weaker key
            if key[0] is None:
                key = ("_unknown_pipeline_", id(op))

            st = s._op_state.get(key)
            if st is None:
                st = {"mult": 1.0, "fails": 0}
            st["fails"] += 1

            # Escalation: double on OOM-like, else mild bump (some failures are transient/modelled)
            if oom_like:
                st["mult"] = min(s._max_ram_mult, st["mult"] * 2.0)
            else:
                st["mult"] = min(s._max_ram_mult, st["mult"] * 1.25)

            s._op_state[key] = st

    # Early exit if nothing to do
    if not pipelines and not results and not s._waiting:
        return [], []

    # --- helpers ---
    def _prio_weight(prio):
        # Higher is more important
        if prio == Priority.QUERY:
            return 10
        if prio == Priority.INTERACTIVE:
            return 5
        return 1  # Priority.BATCH_PIPELINE (and any unknowns)

    def _aging_bonus_ticks(pid):
        meta = s._pipeline_meta.get(pid)
        if not meta:
            return 0
        waited = max(0, s._tick - meta.get("enq_tick", s._tick))
        # Mild linear aging: batch will eventually climb, without eclipsing queries quickly.
        # 1 bonus per 20 ticks waited.
        return waited // 20

    def _effective_rank(p):
        pid = p.pipeline_id
        base = _prio_weight(p.priority)
        # Apply aging more to lower priorities to prevent starvation
        age = _aging_bonus_ticks(pid)
        if p.priority == Priority.BATCH_PIPELINE:
            eff = base + age
        elif p.priority == Priority.INTERACTIVE:
            eff = base + (age // 2)
        else:
            eff = base  # queries shouldn't be "aged" vs. each other; keep stable
        # Lower arrival_seq means earlier
        arr = s._pipeline_meta.get(pid, {}).get("arrival_seq", 0)
        # Sort key: higher eff first, then earlier arrival
        return (-eff, arr)

    def _op_key(pipeline_id, op):
        return (pipeline_id, id(op))

    def _op_retry_state(pipeline_id, op):
        key = _op_key(pipeline_id, op)
        st = s._op_state.get(key)
        if st is None:
            st = {"mult": 1.0, "fails": 0}
            s._op_state[key] = st
        return st

    def _est_mem(op):
        est = getattr(op, "estimate", None)
        mem = getattr(est, "mem_peak_gb", None) if est is not None else None
        if mem is None:
            return None
        try:
            # Clamp negative/NaN-like values defensively
            mem_f = float(mem)
            if mem_f < 0:
                return None
            return mem_f
        except Exception:
            return None

    def _choose_cpu(prio, avail_cpu, pool):
        # Give queries/interact more CPU to reduce latency; keep some room for concurrency.
        # Clamp to pool bounds.
        max_cpu = getattr(pool, "max_cpu_pool", avail_cpu)
        # Basic caps (can be tuned):
        if prio == Priority.QUERY:
            target = min(avail_cpu, max_cpu, max(1.0, 0.75 * max_cpu))
        elif prio == Priority.INTERACTIVE:
            target = min(avail_cpu, max_cpu, max(1.0, 0.60 * max_cpu))
        else:
            target = min(avail_cpu, max_cpu, max(1.0, 0.40 * max_cpu))
        # Also avoid consuming *all* CPU when plenty of ops are pending; leave 1 core if possible
        if avail_cpu > 1.0 and target >= avail_cpu:
            target = max(1.0, avail_cpu - 1.0)
        return max(1.0, float(target))

    def _choose_ram(pipeline_id, op, avail_ram, pool):
        # Use estimator if present, add headroom, then apply retry multiplier.
        # Ensure we never ask for more than pool max or current avail.
        max_ram = getattr(pool, "max_ram_pool", avail_ram)

        est = _est_mem(op)
        st = _op_retry_state(pipeline_id, op)

        # If too many failures, stop retrying to avoid endless queue churn.
        if st["fails"] >= s._max_fail_retries:
            return None

        # Baseline RAM request:
        if est is None:
            # Without estimate, be conservative but not huge:
            # allocate a moderate slice of pool (helps avoid OOM but keeps concurrency).
            base = max(1.0, min(max_ram, 0.33 * max_ram))
        else:
            base = est * s._base_headroom

        req = base * max(1.0, float(st["mult"]))

        # Clamp to feasible range
        req = min(float(req), float(max_ram), float(avail_ram))
        # Avoid tiny allocations
        req = max(1.0, float(req))
        return req

    def _pool_fit_score(op, pipeline_id, pool):
        # Lower is better. Return None if infeasible.
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool
        if avail_cpu <= 0 or avail_ram <= 0:
            return None

        est = _est_mem(op)
        st = _op_retry_state(pipeline_id, op)
        if st["fails"] >= s._max_fail_retries:
            return None

        # Determine RAM we'd request in this pool.
        req_ram = _choose_ram(pipeline_id, op, avail_ram, pool)
        if req_ram is None or req_ram > avail_ram:
            return None

        # If we have an estimate and even the *estimated* need (with minimal headroom) exceeds pool max,
        # treat as infeasible (reduces OOM cascades).
        if est is not None:
            max_ram = getattr(pool, "max_ram_pool", avail_ram)
            if est * 1.05 > max_ram:
                return None

        # Prefer tight RAM fit (reduces fragmentation and keeps headroom for big ops).
        leftover_ram = avail_ram - req_ram

        # Add a small penalty if estimator is missing (riskier).
        risk = 1.0 if est is None else 0.0

        # Add a small penalty for high retry multiplier (likely memory-heavy/unstable).
        retry_pen = max(0.0, st["mult"] - 1.0)

        return leftover_ram + 2.0 * risk + 0.5 * retry_pen

    # --- scheduling loop ---
    suspensions = []  # No preemption in this version (safer; avoids wasted work without full running-state visibility)
    assignments = []

    # Rebuild a working list of live pipelines (drop completed, keep retryable)
    # Also refresh their enqueue tick as they remain in queue.
    live = []
    while s._waiting:
        p = s._waiting.popleft()
        status = p.runtime_status()

        # Drop successful pipelines
        if status.is_pipeline_successful():
            continue

        # If pipeline has failures, we still try to retry ops (FAILED is in ASSIGNABLE_STATES)
        # but if it has no assignable ops at all, keep it queued for later ticks.
        pid = p.pipeline_id
        if pid in s._pipeline_meta:
            s._pipeline_meta[pid]["enq_tick"] = s._tick

        live.append(p)

    # Order pipelines by effective rank (priority + aging)
    live.sort(key=_effective_rank)

    # For each pool, try to pack multiple ops, but prioritize fairness across pipelines:
    # We'll iterate in rounds selecting at most one op per pipeline per round.
    # This avoids a single pipeline monopolizing a pool when others are waiting.
    per_pool_cpu = [s.executor.pools[i].avail_cpu_pool for i in range(s.executor.num_pools)]
    per_pool_ram = [s.executor.pools[i].avail_ram_pool for i in range(s.executor.num_pools)]

    made_progress = True
    scheduled_any = False

    # We'll do up to a bounded number of rounds to avoid excessive runtime.
    max_rounds = 8
    rounds = 0

    while made_progress and rounds < max_rounds:
        rounds += 1
        made_progress = False

        for p in live:
            pid = p.pipeline_id
            status = p.runtime_status()

            # Choose the next assignable op (single-op assignments work best with unknown scaling curves)
            op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
            if not op_list:
                continue
            op = op_list[0]

            # Find the best pool for this op given current availability
            best_pool = None
            best_score = None
            for pool_id in range(s.executor.num_pools):
                pool = s.executor.pools[pool_id]
                # Temporarily override pool availability with our tracked per-pool values
                # by using local vars rather than pool.* in scoring where needed.
                # We'll emulate by constructing a minimal proxy approach:
                class _TmpPool:
                    pass
                tmp = _TmpPool()
                tmp.avail_cpu_pool = per_pool_cpu[pool_id]
                tmp.avail_ram_pool = per_pool_ram[pool_id]
                tmp.max_cpu_pool = getattr(pool, "max_cpu_pool", per_pool_cpu[pool_id])
                tmp.max_ram_pool = getattr(pool, "max_ram_pool", per_pool_ram[pool_id])

                score = _pool_fit_score(op, pid, tmp)
                if score is None:
                    continue
                if best_score is None or score < best_score:
                    best_score = score
                    best_pool = pool_id

            if best_pool is None:
                # Cannot place now; keep pipeline in queue for later
                continue

            pool = s.executor.pools[best_pool]
            avail_cpu = per_pool_cpu[best_pool]
            avail_ram = per_pool_ram[best_pool]

            req_ram = _choose_ram(pid, op, avail_ram, pool)
            if req_ram is None or req_ram > avail_ram:
                continue

            req_cpu = _choose_cpu(p.priority, avail_cpu, pool)
            if req_cpu > avail_cpu:
                continue

            # Make assignment
            assignments.append(
                Assignment(
                    ops=op_list,
                    cpu=req_cpu,
                    ram=req_ram,
                    priority=p.priority,
                    pool_id=best_pool,
                    pipeline_id=pid,
                )
            )

            # Update per-pool availability
            per_pool_cpu[best_pool] -= req_cpu
            per_pool_ram[best_pool] -= req_ram

            made_progress = True
            scheduled_any = True

        # Stop early if all pools are effectively exhausted
        if all(c <= 0.0 or r <= 0.0 for c, r in zip(per_pool_cpu, per_pool_ram)):
            break

    # Requeue pipelines that are still not complete/failed-terminal
    # (We avoid dropping pipelines; failures are heavily penalized in the objective.)
    for p in live:
        status = p.runtime_status()
        if status.is_pipeline_successful():
            continue
        s._waiting.append(p)

    return suspensions, assignments
