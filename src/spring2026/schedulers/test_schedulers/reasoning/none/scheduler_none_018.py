# policy_key: scheduler_none_018
# reasoning_effort: none
# model: gpt-5.2-2025-12-11
# llm_cost: 0.049123
# generation_seconds: 41.92
# generated_at: 2026-04-09T21:58:40.106129
@register_scheduler_init(key="scheduler_none_018")
def scheduler_none_018_init(s):
    """Priority-aware, failure-avoiding scheduler with conservative RAM sizing + limited preemption.

    High-level idea:
      - Keep per-pipeline RAM "floor" estimates and increase them on OOM failures (binary-ish backoff).
      - Reserve headroom for high-priority (QUERY/INTERACTIVE) by optionally preempting BATCH work.
      - Prefer running at least one ready op per pipeline (reduce tail latency) while avoiding starvation via aging.
      - Use multi-pool placement by choosing the pool that can fit the op with least fragmentation.
    """
    # Global waiting set (pipelines that have arrived and are not terminal)
    s.waiting_queue = []

    # Per-pipeline resource learning
    # pid -> {"ram_floor": float, "cpu_cap": float, "failures": int, "last_seen_t": int}
    s.pipe_state = {}

    # Tick counter for simple aging / periodic decisions
    s.t = 0

    # Track arrivals by pid so we don't double-add
    s.seen_pipelines = set()

    # Preemption knobs (keep conservative to avoid churn / wasted work)
    s.enable_preemption = True
    s.preempt_only_if_needed = True
    s.max_preemptions_per_tick = 2

    # RAM growth on OOM: new_floor = max(prev_floor * factor, prev_floor + additive)
    s.oom_ram_factor = 1.6
    s.oom_ram_add = 0.25

    # RAM safety margin even without failures (reduce OOM risk)
    s.ram_safety = 0.10  # 10%

    # CPU sizing: keep moderate parallelism (too much CPU for one op can starve others)
    s.cpu_share_query = 0.75
    s.cpu_share_interactive = 0.60
    s.cpu_share_batch = 0.40
    s.cpu_min = 0.5

    # Limit how many ops from the same pipeline we assign at once (favor responsiveness / fairness)
    s.max_ops_per_pipeline_per_tick = 1

    # Headroom reservation per pool for high priority
    # Keep a small reserve to reduce probability a query waits behind big batch assignments.
    s.reserve_cpu_query = 0.5
    s.reserve_ram_query = 0.5
    s.reserve_cpu_interactive = 0.25
    s.reserve_ram_interactive = 0.25


def _priority_weight(priority):
    # Higher value => higher importance
    if priority == Priority.QUERY:
        return 3
    if priority == Priority.INTERACTIVE:
        return 2
    return 1  # BATCH_PIPELINE


def _cpu_share_for_priority(s, priority):
    if priority == Priority.QUERY:
        return s.cpu_share_query
    if priority == Priority.INTERACTIVE:
        return s.cpu_share_interactive
    return s.cpu_share_batch


def _reserve_for_priority(s, priority):
    # Reserve is used only when admitting lower priority work (leave headroom).
    if priority == Priority.QUERY:
        return s.reserve_cpu_query, s.reserve_ram_query
    if priority == Priority.INTERACTIVE:
        return s.reserve_cpu_interactive, s.reserve_ram_interactive
    return 0.0, 0.0


def _is_terminal(pipeline):
    st = pipeline.runtime_status()
    if st.is_pipeline_successful():
        return True
    # If any operators have FAILED we consider pipeline terminal in this simulator baseline
    # (the starter template avoids retries of failures).
    return st.state_counts[OperatorState.FAILED] > 0


def _get_ready_ops(pipeline, limit, allow_failed_retry=False):
    st = pipeline.runtime_status()
    states = ASSIGNABLE_STATES if allow_failed_retry else [OperatorState.PENDING]
    return st.get_ops(states, require_parents_complete=True)[:limit]


def _find_pool_fit(pools, cpu_need, ram_need, prefer_more_headroom=False):
    # Choose the pool that can fit (cpu_need, ram_need).
    # Heuristic: minimize leftover (waste) while still fitting; optionally prefer more headroom.
    best = None
    best_score = None
    for i, pool in enumerate(pools):
        if pool.avail_cpu_pool + 1e-9 < cpu_need or pool.avail_ram_pool + 1e-9 < ram_need:
            continue
        cpu_left = pool.avail_cpu_pool - cpu_need
        ram_left = pool.avail_ram_pool - ram_need
        # Penalize leaving tiny unusable fragments; also avoid exhausting pool completely.
        score = (cpu_left ** 2) + (ram_left ** 2)
        if prefer_more_headroom:
            # Invert preference: pick pool with more remaining after placing.
            score = -((cpu_left) + (ram_left))
        if best is None or score < best_score:
            best = i
            best_score = score
    return best


def _collect_running_containers(results):
    # Best-effort view of currently running containers based on recent results.
    # Some simulators may emit results for completions/failures only; this is conservative.
    running = []
    for r in results:
        # We only know about containers that emitted an event this tick.
        # Use them for preemption decisions if needed.
        if getattr(r, "container_id", None) is not None and getattr(r, "pool_id", None) is not None:
            running.append(r)
    return running


@register_scheduler(key="scheduler_none_018")
def scheduler_none_018_scheduler(s, results, pipelines):
    """
    Scheduler loop:
      1) Ingest arrivals; maintain per-pipeline learned RAM floors.
      2) Update RAM floors on OOM failures (based on ExecutionResult.error).
      3) Build candidate ready ops across all pipelines, sort by (priority, aging, pipeline progress).
      4) Place ops into pools with packing heuristic; keep small high-priority headroom.
      5) If a QUERY/INTERACTIVE op cannot be placed and preemption enabled, suspend a BATCH container.
    """
    s.t += 1

    # Ingest new pipelines
    for p in pipelines:
        if p.pipeline_id in s.seen_pipelines:
            continue
        s.seen_pipelines.add(p.pipeline_id)
        s.waiting_queue.append(p)
        if p.pipeline_id not in s.pipe_state:
            s.pipe_state[p.pipeline_id] = {
                "ram_floor": 0.0,
                "cpu_cap": 0.0,
                "failures": 0,
                "last_seen_t": s.t,
            }
        else:
            s.pipe_state[p.pipeline_id]["last_seen_t"] = s.t

    # Early exit when nothing to do
    if not pipelines and not results:
        return [], []

    # Update learned RAM floors on failures (try to detect OOM-like errors)
    # We do not automatically retry FAILED operators here (to match baseline), but the larger
    # ram_floor can help subsequent operators in same pipeline (or future if simulator retries).
    for r in results:
        if not hasattr(r, "failed") or not r.failed():
            continue
        pid = getattr(r, "pipeline_id", None)
        # pipeline_id might not be present on ExecutionResult; fall back to r.ops' pipeline linkage if absent
        # (if unavailable, we skip learning).
        if pid is None:
            continue
        ps = s.pipe_state.get(pid)
        if ps is None:
            continue
        ps["failures"] += 1

        err = str(getattr(r, "error", "") or "").lower()
        if "oom" in err or "out of memory" in err or "memory" in err:
            prev = float(ps.get("ram_floor", 0.0) or 0.0)
            # If we know what we tried, use it as baseline; else bump minimally.
            tried = float(getattr(r, "ram", 0.0) or 0.0)
            base = max(prev, tried, 0.1)
            ps["ram_floor"] = max(prev, base * s.oom_ram_factor, base + s.oom_ram_add)

    # Prune terminal pipelines from waiting queue to avoid scheduling them again
    active = []
    for p in s.waiting_queue:
        if _is_terminal(p):
            continue
        active.append(p)
    s.waiting_queue = active

    suspensions = []
    assignments = []

    # Build candidates: one ready op per pipeline (configurable), compute score with aging
    candidates = []
    for p in s.waiting_queue:
        ps = s.pipe_state.get(p.pipeline_id, {})
        age = s.t - int(ps.get("last_seen_t", s.t))
        # Update last_seen to support "aging since last scheduled/considered"
        ps["last_seen_t"] = s.t
        s.pipe_state[p.pipeline_id] = ps

        ops = _get_ready_ops(p, limit=s.max_ops_per_pipeline_per_tick, allow_failed_retry=False)
        if not ops:
            continue

        # Candidate score: prioritize higher priority; within same priority, older pipelines get boost.
        pri_w = _priority_weight(p.priority)
        # Aging boost is mild to avoid harming p95 for high priority; capped.
        aging_boost = min(5, age) * 0.05
        score = pri_w + aging_boost
        candidates.append((score, pri_w, p, ops))

    # Sort descending by score, then by priority weight
    candidates.sort(key=lambda x: (x[0], x[1]), reverse=True)

    pools = [s.executor.pools[i] for i in range(s.executor.num_pools)]

    def try_place(pipeline, op_list):
        # Decide resource sizes
        pool_max_cpu = [pool.max_cpu_pool for pool in pools]
        pool_max_ram = [pool.max_ram_pool for pool in pools]

        ps = s.pipe_state.get(pipeline.pipeline_id, {"ram_floor": 0.0, "cpu_cap": 0.0})
        # RAM: learned floor + safety margin, but no more than pool max.
        ram_floor = float(ps.get("ram_floor", 0.0) or 0.0)
        # If we have no floor yet, start small but non-trivial to reduce OOM chance.
        baseline_ram = max(ram_floor, 0.5)
        ram_need_unclamped = baseline_ram * (1.0 + s.ram_safety)

        # CPU: allocate a share of the target pool available CPU; choose pool later, so compute min cpu
        cpu_min = s.cpu_min

        # Placement:
        # We try to fit while keeping headroom for higher priority tasks:
        # - When scheduling BATCH, keep reserve for QUERY+INTERACTIVE by reducing effective available.
        # - When scheduling INTERACTIVE, keep reserve for QUERY.
        # - When scheduling QUERY, no reserve.
        best_pool = None
        best_cpu = None
        best_ram = None
        best_frag = None

        for i, pool in enumerate(pools):
            avail_cpu = pool.avail_cpu_pool
            avail_ram = pool.avail_ram_pool

            # Compute reserve to leave for more important work
            # For BATCH, reserve for QUERY + INTERACTIVE (use interactive reserve as proxy plus query reserve)
            if pipeline.priority == Priority.BATCH_PIPELINE:
                res_cpu_q, res_ram_q = _reserve_for_priority(s, Priority.QUERY)
                res_cpu_i, res_ram_i = _reserve_for_priority(s, Priority.INTERACTIVE)
                eff_cpu = max(0.0, avail_cpu - (res_cpu_q + res_cpu_i))
                eff_ram = max(0.0, avail_ram - (res_ram_q + res_ram_i))
            elif pipeline.priority == Priority.INTERACTIVE:
                res_cpu_q, res_ram_q = _reserve_for_priority(s, Priority.QUERY)
                eff_cpu = max(0.0, avail_cpu - res_cpu_q)
                eff_ram = max(0.0, avail_ram - res_ram_q)
            else:
                eff_cpu = avail_cpu
                eff_ram = avail_ram

            if eff_cpu <= 0 or eff_ram <= 0:
                continue

            # CPU allocation as fraction of effective available, bounded by pool max (and eff_cpu).
            share = _cpu_share_for_priority(s, pipeline.priority)
            cpu_need = max(cpu_min, eff_cpu * share)
            cpu_need = min(cpu_need, eff_cpu, pool_max_cpu[i])

            # RAM need bounded by pool max and eff_ram
            ram_need = min(ram_need_unclamped, eff_ram, pool_max_ram[i])

            # Ensure still feasible (might have been clamped below baseline; if too small, skip)
            if cpu_need + 1e-9 < cpu_min or ram_need + 1e-9 < min(0.1, ram_need_unclamped):
                continue
            if cpu_need > eff_cpu + 1e-9 or ram_need > eff_ram + 1e-9:
                continue

            # Prefer pool that leaves least squared leftover (packing) but doesn't exhaust completely
            cpu_left = eff_cpu - cpu_need
            ram_left = eff_ram - ram_need
            frag = (cpu_left ** 2) + (ram_left ** 2)

            # Slightly bias QUERY/INTERACTIVE to pools with more headroom to reduce interference.
            if pipeline.priority in (Priority.QUERY, Priority.INTERACTIVE):
                frag -= 0.1 * (cpu_left + ram_left)

            if best_pool is None or frag < best_frag:
                best_pool = i
                best_cpu = cpu_need
                best_ram = ram_need
                best_frag = frag

        if best_pool is None:
            return None

        return Assignment(
            ops=op_list,
            cpu=best_cpu,
            ram=best_ram,
            priority=pipeline.priority,
            pool_id=best_pool,
            pipeline_id=pipeline.pipeline_id,
        )

    # Attempt to place all candidates in priority order
    for _, _, p, ops in candidates:
        a = try_place(p, ops)
        if a is not None:
            assignments.append(a)
        else:
            # If we couldn't place a high-priority op, attempt limited preemption of batch.
            if s.enable_preemption and p.priority in (Priority.QUERY, Priority.INTERACTIVE):
                if len(suspensions) >= s.max_preemptions_per_tick:
                    continue

                # Find a batch container to preempt (best-effort using results events).
                # If the simulator provides richer APIs, this remains conservative and may no-op.
                running_events = _collect_running_containers(results)
                victim = None
                for r in running_events:
                    if getattr(r, "priority", None) == Priority.BATCH_PIPELINE:
                        cid = getattr(r, "container_id", None)
                        pid = getattr(r, "pool_id", None)
                        if cid is None or pid is None:
                            continue
                        victim = (cid, pid)
                        break

                if victim is not None:
                    cid, pool_id = victim
                    suspensions.append(Suspend(container_id=cid, pool_id=pool_id))

    return suspensions, assignments
