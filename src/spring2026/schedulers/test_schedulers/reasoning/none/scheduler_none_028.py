# policy_key: scheduler_none_028
# reasoning_effort: none
# model: gpt-5.2-2025-12-11
# llm_cost: 0.057803
# generation_seconds: 55.36
# generated_at: 2026-04-09T22:07:07.424611
@register_scheduler_init(key="scheduler_none_028")
def scheduler_none_028_init(s):
    """Priority-protecting, OOM-avoidant, low-churn scheduler.

    Goals for the weighted-latency objective:
    - Keep QUERY/INTERACTIVE latency low via strict priority ordering + light reserved headroom.
    - Maximize completion rate by learning per-op RAM needs from OOMs and retrying with higher RAM.
    - Reduce tail latency by allowing multiple small high-priority ops to run concurrently (fair-share CPU),
      while ensuring each op gets enough RAM to avoid failures.
    - Avoid starvation by aging lower priorities over time (soft boost), without hurting query latency much.

    Key mechanisms:
    - Central waiting queue of pipelines; pick assignable ops (parents complete).
    - Per-(pipeline, op) RAM estimate with exponential backoff on OOM; small safety margin.
    - Per-priority pool selection with optional "interactive-preferred" pool 0 bias.
    - Gentle preemption: only preempt BATCH (and INTERACTIVE only for QUERY) when high-priority cannot start
      due to insufficient headroom, and only if we can actually fit the incoming op after preemption.
    """
    s.waiting_queue = []  # FIFO arrivals, selection is priority-aware
    s.known_ram = {}  # (pipeline_id, op_id) -> estimated required RAM
    s.arrival_ts = {}  # pipeline_id -> first-seen sim time (if available)
    s.last_seen_priority = {}  # pipeline_id -> priority
    s.preempt_grace = 10.0  # seconds to wait before considering preemption for a waiting high-priority pipe
    s.headroom_frac = {
        Priority.QUERY: 0.15,
        Priority.INTERACTIVE: 0.10,
        Priority.BATCH_PIPELINE: 0.00,
    }
    s.ram_safety = 1.10  # allocate a bit above estimate to avoid borderline OOM
    s.max_ops_per_tick_per_pool = 4  # avoid over-issuing; keep churn low
    s.min_cpu_per_op = 1.0  # don't starve an op with near-zero CPU
    s.max_cpu_per_op = None  # if None, cap is pool size / concurrent ops
    s.aging_half_life = 120.0  # seconds for an effective +1 boost unit

    # Try to track running containers by pool for preemption decisions.
    s.running = {}  # (pool_id) -> list of dicts {container_id, priority, cpu, ram}


@register_scheduler(key="scheduler_none_028")
def scheduler_none_028_scheduler(s, results, pipelines):
    """
    Step function:
    - Incorporate new pipelines.
    - Update RAM estimates from failures (OOM-like errors) and update running-container bookkeeping.
    - Select ops to run per pool, prioritizing QUERY > INTERACTIVE > BATCH with aging.
    - Allocate CPU via fair-share among issued ops in a pool for this tick, but ensure min per op.
    - If a high-priority op cannot be placed due to headroom, preempt lowest-priority running work
      (batch first; for queries, interactive can also be preempted as last resort).
    """
    # Local helpers (no imports required).
    def _now():
        # Best-effort access to simulator time if available.
        return getattr(getattr(s, "executor", None), "now", None) or getattr(s, "now", None) or 0.0

    def _prio_weight(p):
        if p == Priority.QUERY:
            return 3
        if p == Priority.INTERACTIVE:
            return 2
        return 1

    def _prio_rank(p):
        # Higher is better.
        if p == Priority.QUERY:
            return 3
        if p == Priority.INTERACTIVE:
            return 2
        return 1

    def _effective_rank(pipeline, now_ts):
        # Aging: slowly increases effective rank of long-waiting lower-priority pipelines to avoid starvation.
        # We keep it small vs true priority rank to avoid harming queries.
        base = _prio_rank(pipeline.priority) * 10.0
        pid = pipeline.pipeline_id
        arr = s.arrival_ts.get(pid, now_ts)
        waited = max(0.0, now_ts - arr)
        # Convert waited time into a small additive boost; capped to avoid overtaking true priority.
        boost = min(4.0, waited / max(1.0, s.aging_half_life))
        # Batch can at most gain a bit; interactive gains less; query gains none (already top).
        if pipeline.priority == Priority.BATCH_PIPELINE:
            return base + boost
        if pipeline.priority == Priority.INTERACTIVE:
            return base + min(2.0, boost * 0.5)
        return base  # QUERY

    def _pipeline_done_or_failed(p):
        st = p.runtime_status()
        if st.is_pipeline_successful():
            return True
        # If any FAILED ops exist we may still retry; but if pipeline is considered failed terminally
        # by simulator, we don't have explicit flag. We'll keep it in queue unless fully successful.
        return False

    def _assignable_ops(p):
        st = p.runtime_status()
        # Only schedule one op per pipeline per tick to avoid over-parallelizing a single DAG and to reduce churn.
        ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        if not ops:
            return []
        return ops[:1]

    def _op_id(op):
        # Robust identifier for operator objects.
        return getattr(op, "op_id", None) or getattr(op, "operator_id", None) or getattr(op, "id", None) or str(op)

    def _base_ram_guess_for_op(op, pool_ram_max):
        # If operator carries a min_ram field use it; else use a conservative small guess.
        mr = getattr(op, "min_ram", None)
        if mr is None:
            mr = getattr(op, "ram_min", None)
        if mr is None:
            mr = getattr(op, "min_memory", None)
        if mr is None:
            # Conservative baseline: 5% of pool RAM max (bounded).
            return max(0.5, min(pool_ram_max * 0.05, 8.0))
        # Ensure positive.
        try:
            return max(0.1, float(mr))
        except Exception:
            return max(0.5, min(pool_ram_max * 0.05, 8.0))

    def _ram_estimate(pipeline, op, pool_ram_max):
        pid = pipeline.pipeline_id
        oid = _op_id(op)
        key = (pid, oid)
        est = s.known_ram.get(key)
        if est is None:
            est = _base_ram_guess_for_op(op, pool_ram_max)
        # Apply safety margin.
        return min(pool_ram_max, max(0.1, est * s.ram_safety))

    def _record_oom_and_bump_ram(res):
        # On failure, bump RAM estimate for those ops; treat any failure as "maybe OOM" if error hints so.
        if not res.failed():
            return
        err = (res.error or "")
        is_oom = False
        # Heuristic keywords.
        low = str(err).lower()
        if ("oom" in low) or ("out of memory" in low) or ("memory" in low and "killed" in low):
            is_oom = True
        # If no message, still bump slightly to improve completion probability.
        bump_factor = 2.0 if is_oom else 1.3

        # res.ops is a list of op objects in this container assignment.
        for op in (res.ops or []):
            # We don't always have pipeline_id in result, but container belonged to some pipeline_id when assigned.
            # ExecutionResult does include container_id/pool_id; we use pipeline_id only if present.
            # Fall back to storing a generic op-based key if needed.
            pid = getattr(res, "pipeline_id", None)
            oid = _op_id(op)
            if pid is None:
                # Without pid, cannot do per-pipeline; store per-op-id global key using None.
                key = (None, oid)
            else:
                key = (pid, oid)
            prev = s.known_ram.get(key, None)
            # Use observed allocated RAM as a floor for the next estimate (since it failed with that, we must go higher).
            floor = None
            try:
                floor = float(res.ram) * bump_factor
            except Exception:
                floor = None
            if prev is None and floor is not None:
                s.known_ram[key] = max(0.1, floor)
            elif prev is not None and floor is not None:
                s.known_ram[key] = max(prev * bump_factor, floor, 0.1)
            elif prev is not None:
                s.known_ram[key] = max(prev * bump_factor, 0.1)
            else:
                s.known_ram[key] = 2.0  # conservative fallback

    def _update_running_bookkeeping_from_results():
        # Remove completed/failed containers; results correspond to containers that ended this tick.
        for res in results:
            pool_id = res.pool_id
            if pool_id not in s.running:
                continue
            cid = res.container_id
            new_list = [c for c in s.running[pool_id] if c.get("container_id") != cid]
            s.running[pool_id] = new_list

    def _add_running_from_assignments(assignments):
        # Track container_id isn't known yet at assignment time; we can't add real container id.
        # So we only track running based on results. Keep s.running as "best effort" and use live executor state
        # if exposed. If executor doesn't expose, preemption will be conservative (rare).
        return

    def _try_get_live_running_containers(pool_id):
        # Best-effort: if executor/pool exposes running containers list, use it for preemption.
        pool = s.executor.pools[pool_id]
        candidates = []
        live = getattr(pool, "containers", None) or getattr(pool, "running_containers", None) or None
        if live is None:
            return candidates
        try:
            for c in live:
                # Attempt to normalize fields.
                cid = getattr(c, "container_id", None) or getattr(c, "id", None) or c.get("container_id")
                pr = getattr(c, "priority", None) or c.get("priority")
                cpu = getattr(c, "cpu", None) or c.get("cpu")
                ram = getattr(c, "ram", None) or c.get("ram")
                candidates.append(
                    {"container_id": cid, "priority": pr, "cpu": float(cpu), "ram": float(ram), "pool_id": pool_id}
                )
        except Exception:
            return []
        return candidates

    def _reserved_headroom(pool, priority):
        frac = s.headroom_frac.get(priority, 0.0)
        return pool.max_cpu_pool * frac, pool.max_ram_pool * frac

    def _pick_pool_for(pipeline, ram_need, cpu_need):
        # Pool selection: prefer pool 0 for QUERY/INTERACTIVE when possible (reduces interference),
        # otherwise choose pool with most headroom for the need.
        best = None
        best_score = None
        for pool_id in range(s.executor.num_pools):
            pool = s.executor.pools[pool_id]
            res_cpu, res_ram = _reserved_headroom(pool, pipeline.priority)
            avail_cpu = max(0.0, pool.avail_cpu_pool - res_cpu)
            avail_ram = max(0.0, pool.avail_ram_pool - res_ram)

            # Must fit RAM at least; CPU can be small (we'll allow min).
            if avail_ram + 1e-9 < ram_need:
                continue
            if avail_cpu + 1e-9 < min(cpu_need, max(avail_cpu, 0.0)):
                # If requested cpu_need is > avail_cpu, we can still run with smaller CPU,
                # so don't reject based on cpu here.
                pass

            # Score: prioritize dedicated placement for high priority, then maximize slack.
            slack_cpu = avail_cpu - cpu_need
            slack_ram = avail_ram - ram_need
            score = slack_ram * 2.0 + slack_cpu * 1.0

            # Bias pool 0 for high priority.
            if pool_id == 0 and pipeline.priority in (Priority.QUERY, Priority.INTERACTIVE):
                score += 1000.0

            if best is None or score > best_score:
                best = pool_id
                best_score = score
        return best

    def _maybe_preempt_for(pool_id, incoming_priority, ram_need, cpu_need):
        # Preempt only when necessary and only low priority first.
        pool = s.executor.pools[pool_id]
        res_cpu, res_ram = _reserved_headroom(pool, incoming_priority)
        eff_avail_cpu = max(0.0, pool.avail_cpu_pool - res_cpu)
        eff_avail_ram = max(0.0, pool.avail_ram_pool - res_ram)

        if eff_avail_ram >= ram_need and eff_avail_cpu >= min(cpu_need, eff_avail_cpu):
            return []

        # Determine what can be preempted.
        # For QUERY: can preempt BATCH, then INTERACTIVE (last resort). For INTERACTIVE: preempt BATCH only.
        if incoming_priority == Priority.QUERY:
            preempt_set = {Priority.BATCH_PIPELINE, Priority.INTERACTIVE}
        elif incoming_priority == Priority.INTERACTIVE:
            preempt_set = {Priority.BATCH_PIPELINE}
        else:
            return []

        live = _try_get_live_running_containers(pool_id)
        if not live:
            # Fall back to our best-effort bookkeeping.
            live = list(s.running.get(pool_id, []))

        # Filter preemptable.
        victims = [c for c in live if c.get("container_id") is not None and c.get("priority") in preempt_set]
        # Sort by lowest priority first, then largest RAM (free RAM quickly), then largest CPU.
        def _victim_key(c):
            pr = c.get("priority")
            pr_rank = _prio_rank(pr) if pr is not None else 0
            return (pr_rank, -float(c.get("ram", 0.0)), -float(c.get("cpu", 0.0)))

        victims.sort(key=_victim_key)

        susp = []
        freed_ram = 0.0
        freed_cpu = 0.0
        for v in victims:
            susp.append(Suspend(v["container_id"], pool_id))
            freed_ram += float(v.get("ram", 0.0))
            freed_cpu += float(v.get("cpu", 0.0))
            if (eff_avail_ram + freed_ram) >= ram_need and (eff_avail_cpu + freed_cpu) >= min(cpu_need, eff_avail_cpu + freed_cpu):
                break

        # Only preempt if it actually helps fit the incoming work.
        if (eff_avail_ram + freed_ram) < ram_need:
            return []
        return susp

    now_ts = _now()

    # Add new pipelines.
    for p in pipelines:
        s.waiting_queue.append(p)
        pid = p.pipeline_id
        if pid not in s.arrival_ts:
            s.arrival_ts[pid] = now_ts
        s.last_seen_priority[pid] = p.priority

    # Update from results.
    for res in results:
        _record_oom_and_bump_ram(res)
    _update_running_bookkeeping_from_results()

    # Early exit if nothing changed.
    if not pipelines and not results and not s.waiting_queue:
        return [], []

    # Build list of runnable candidates (pipeline, op) with effective ranks.
    # Keep pipelines in queue; we will rotate them (stable).
    runnable = []
    kept = []
    for p in s.waiting_queue:
        if _pipeline_done_or_failed(p):
            continue
        ops = _assignable_ops(p)
        if ops:
            runnable.append((p, ops[0], _effective_rank(p, now_ts)))
        kept.append(p)
    s.waiting_queue = kept

    # Sort runnable by effective rank (desc), then by arrival time (asc) to protect latency.
    def _rkey(item):
        p, op, er = item
        return (-er, s.arrival_ts.get(p.pipeline_id, now_ts))

    runnable.sort(key=_rkey)

    suspensions = []
    assignments = []

    # Attempt to schedule across pools. We'll do multiple passes so high priority can claim capacity first.
    # Pass order: QUERY -> INTERACTIVE -> BATCH
    prio_order = [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]

    # For each pool, issue up to max ops per tick, preferring high priority.
    issued_per_pool = {pid: 0 for pid in range(s.executor.num_pools)}

    # We'll keep track of which (pipeline_id, op_id) we've scheduled this tick to avoid duplicates.
    scheduled_keys = set()

    for pr in prio_order:
        # Iterate runnable list in rank order; attempt to place those with matching priority.
        for (p, op, er) in runnable:
            if p.priority != pr:
                continue
            pid = p.pipeline_id
            oid = _op_id(op)
            key = (pid, oid)
            if key in scheduled_keys:
                continue

            # Determine a conservative CPU request:
            # - High priority: request a fair slice but not the whole pool.
            # - Batch: smaller CPU to increase concurrency without hurting high priority latency.
            cpu_need = 1.0
            if pr == Priority.QUERY:
                cpu_need = 2.0
            elif pr == Priority.INTERACTIVE:
                cpu_need = 1.5
            else:
                cpu_need = 1.0

            # Pick pool based on RAM fit and preference.
            # Need pool max RAM for estimate.
            best_pool = None
            best_ram_need = None
            for pool_id in range(s.executor.num_pools):
                pool = s.executor.pools[pool_id]
                ram_need = _ram_estimate(p, op, pool.max_ram_pool)
                chosen = _pick_pool_for(p, ram_need, cpu_need)
                # _pick_pool_for searches across pools; only do once using representative pool max.
                # But ram_need depends on pool max, so we compute for pool 0 first, then re-evaluate if chosen differs.
                best_pool = chosen
                break

            if best_pool is None:
                continue

            pool = s.executor.pools[best_pool]
            ram_need = _ram_estimate(p, op, pool.max_ram_pool)
            best_ram_need = ram_need

            # Respect per-pool issue limit.
            if issued_per_pool[best_pool] >= s.max_ops_per_tick_per_pool:
                continue

            # Compute effective available after reserved headroom.
            res_cpu, res_ram = _reserved_headroom(pool, pr)
            avail_cpu = max(0.0, pool.avail_cpu_pool - res_cpu)
            avail_ram = max(0.0, pool.avail_ram_pool - res_ram)
            if avail_ram + 1e-9 < ram_need:
                # Try preemption if this is high priority and pipeline has waited a bit.
                waited = max(0.0, now_ts - s.arrival_ts.get(pid, now_ts))
                if pr in (Priority.QUERY, Priority.INTERACTIVE) and waited >= s.preempt_grace:
                    susp = _maybe_preempt_for(best_pool, pr, ram_need, cpu_need)
                    if susp:
                        suspensions.extend(susp)
                        # After preemption, assume capacity will be available next tick; don't assign immediately.
                continue

            # Decide CPU allocation: don't grab entire pool; use a moderate slice to allow concurrency.
            # We'll allocate min(request, avail_cpu) but cap to a fraction of pool for high priority.
            if pr == Priority.QUERY:
                cpu_cap = max(s.min_cpu_per_op, pool.max_cpu_pool * 0.5)
            elif pr == Priority.INTERACTIVE:
                cpu_cap = max(s.min_cpu_per_op, pool.max_cpu_pool * 0.35)
            else:
                cpu_cap = max(s.min_cpu_per_op, pool.max_cpu_pool * 0.25)

            cpu_alloc = min(max(s.min_cpu_per_op, cpu_need), avail_cpu, cpu_cap)
            if cpu_alloc <= 0.0:
                continue

            assignment = Assignment(
                ops=[op],
                cpu=cpu_alloc,
                ram=ram_need,
                priority=pr,
                pool_id=best_pool,
                pipeline_id=pid,
            )
            assignments.append(assignment)
            scheduled_keys.add(key)
            issued_per_pool[best_pool] += 1

    # Keep unscheduled pipelines in queue (already kept). Re-add those not done; stable order preserved.
    # (We don't remove scheduled pipelines; runtime_status will move ops to ASSIGNED so they won't reappear.)

    _add_running_from_assignments(assignments)
    return suspensions, assignments
