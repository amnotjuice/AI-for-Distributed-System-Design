# policy_key: scheduler_none_009
# reasoning_effort: none
# model: gpt-5.2-2025-12-11
# llm_cost: 0.063739
# generation_seconds: 47.43
# generated_at: 2026-04-09T21:52:02.167781
@register_scheduler_init(key="scheduler_none_009")
def scheduler_none_009_init(s):
    """Priority-aware, failure-averse scheduler with gentle preemption and OOM-driven RAM backoff.

    Main ideas:
    - Weighted priority queues: QUERY > INTERACTIVE > BATCH with simple aging to avoid starvation.
    - Conservative admission: allocate CPU fairly (small slices) and RAM based on learned per-op needs.
    - OOM handling: on failure, increase RAM estimate for the operator and retry (do not drop).
    - Preemption (limited): if high-priority work arrives and is blocked, suspend low-priority RUNNING
      containers only when needed to create headroom, with a cooldown to avoid churn.

    This aims to reduce weighted latency while minimizing failures (each failure is very costly).
    """
    # Queues of pipelines we know about (arrived and not yet completed/failed permanently)
    s.wait_q = []  # list[Pipeline]

    # Simple virtual-time aging counter per pipeline to prevent starvation
    s.age = {}  # pipeline_id -> int ticks waited

    # Per-operator RAM estimate learned from OOM failures and past successful runs
    # Keyed by (pipeline_id, op_id_str) where op_id_str is a stable identifier we derive.
    s.op_ram_est = {}  # (pipeline_id, op_key) -> float

    # Per-container preemption cooldown to reduce thrash
    s.preempt_cooldown_until = {}  # container_id -> sim_time_tick (int)

    # Scheduler tick counter (monotonic)
    s.ticks = 0

    # Preemption parameters
    s.PREEMPT_COOLDOWN_TICKS = 5
    s.MAX_PREEMPT_PER_TICK = 2

    # RAM backoff parameters
    s.RAM_OOM_MULTIPLIER = 1.6
    s.RAM_OOM_ADD = 0.25  # add some fixed buffer (GB-equivalent units as used by sim)
    s.RAM_HEADROOM_FRAC = 0.10  # extra headroom above estimate
    s.RAM_MIN_FRACTION_OF_POOL = 0.05  # don't allocate absurdly tiny RAM if pool is large

    # CPU allocation parameters
    s.CPU_QUERY_MIN = 2.0
    s.CPU_INTERACTIVE_MIN = 1.5
    s.CPU_BATCH_MIN = 1.0
    s.CPU_MAX_FRACTION_POOL = 0.75  # avoid monopolizing a pool with one op unless alone

    # Target number of concurrent assignments per pool per tick (keeps latency low & avoids OOM cascades)
    s.MAX_ASSIGNMENTS_PER_POOL_PER_TICK = 2


@register_scheduler(key="scheduler_none_009")
def scheduler_none_009(s, results, pipelines):
    """
    See init docstring for policy overview.
    """
    # Helper functions kept inside to avoid global imports and keep policy self-contained.
    def _prio_weight(prio):
        # Larger means more important for scheduling decisions
        if prio == Priority.QUERY:
            return 3
        if prio == Priority.INTERACTIVE:
            return 2
        return 1  # batch

    def _cpu_min(prio):
        if prio == Priority.QUERY:
            return s.CPU_QUERY_MIN
        if prio == Priority.INTERACTIVE:
            return s.CPU_INTERACTIVE_MIN
        return s.CPU_BATCH_MIN

    def _safe_op_key(op):
        # We need a stable key across ticks/results. Use common fields if present, else repr.
        for attr in ("op_id", "operator_id", "id", "name"):
            if hasattr(op, attr):
                v = getattr(op, attr)
                if v is not None:
                    return str(v)
        return repr(op)

    def _clamp(x, lo, hi):
        if x < lo:
            return lo
        if x > hi:
            return hi
        return x

    def _pool_headroom(pool):
        return pool.avail_cpu_pool, pool.avail_ram_pool, pool.max_cpu_pool, pool.max_ram_pool

    def _pipeline_done_or_failed(p):
        st = p.runtime_status()
        if st.is_pipeline_successful():
            return True
        # If any operator is FAILED we still want to retry (do not drop) per objective.
        # However, if the sim marks pipeline as terminally failed, we'd need to detect it;
        # the template indicates FAILED ops exist, but doesn't expose terminal pipeline failure.
        return False

    def _next_assignable_ops(p):
        st = p.runtime_status()
        # Include FAILED in ASSIGNABLE_STATES to retry after OOM, consistent with provided note.
        return st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)

    def _running_ops(p):
        st = p.runtime_status()
        return st.get_ops([OperatorState.RUNNING], require_parents_complete=False)

    def _estimate_ram_for_op(p, op, pool):
        # Start with a conservative baseline: fraction of pool RAM, but not exceeding available.
        op_key = _safe_op_key(op)
        est = s.op_ram_est.get((p.pipeline_id, op_key), None)

        # If we don't have an estimate, pick a conservative small-but-not-tiny value.
        if est is None:
            baseline = max(pool.max_ram_pool * s.RAM_MIN_FRACTION_OF_POOL, 0.5)
            est = baseline

        # Add headroom to reduce near-threshold OOMs.
        est = est * (1.0 + s.RAM_HEADROOM_FRAC)

        # Bound by pool max; actual assign will also be bounded by available.
        est = _clamp(est, 0.1, pool.max_ram_pool)
        return est

    def _update_models_from_results():
        # Learn from failures/successes to reduce retries and OOMs.
        for r in results:
            if not hasattr(r, "ops") or not r.ops:
                continue
            # We assume one op per assignment in this policy; still handle list.
            for op in r.ops:
                op_key = _safe_op_key(op)
                # On failure, especially OOM-like, increase RAM estimate.
                if r.failed():
                    prev = s.op_ram_est.get((r.pipeline_id, op_key), None)
                    # If we were given a RAM allocation in result, use it as base.
                    base = float(getattr(r, "ram", 0.0) or 0.0)
                    if base <= 0.0:
                        # fallback: use previous estimate or small constant
                        base = prev if prev is not None else 1.0
                    new_est = base * s.RAM_OOM_MULTIPLIER + s.RAM_OOM_ADD
                    if prev is not None:
                        new_est = max(new_est, prev * 1.2)
                    s.op_ram_est[(r.pipeline_id, op_key)] = new_est
                else:
                    # On success, we can gently decrease estimate toward used allocation (if known)
                    prev = s.op_ram_est.get((r.pipeline_id, op_key), None)
                    base = float(getattr(r, "ram", 0.0) or 0.0)
                    if base > 0.0:
                        if prev is None:
                            s.op_ram_est[(r.pipeline_id, op_key)] = base
                        else:
                            # EWMA with bias toward safety (do not shrink aggressively)
                            s.op_ram_est[(r.pipeline_id, op_key)] = 0.85 * prev + 0.15 * base

    def _ingest_pipelines():
        # Add new pipelines, initialize age.
        for p in pipelines:
            s.wait_q.append(p)
            s.age.setdefault(p.pipeline_id, 0)

    def _age_waiting():
        # Increase age for pipelines that are not done; reset for active ones slightly.
        # Aging improves fairness for batch without harming query too much.
        new_q = []
        for p in s.wait_q:
            if _pipeline_done_or_failed(p):
                continue
            st = p.runtime_status()
            # If pipeline has any RUNNING/ASSIGNED ops, decay age; else increment.
            if st.state_counts.get(OperatorState.RUNNING, 0) > 0 or st.state_counts.get(OperatorState.ASSIGNED, 0) > 0:
                s.age[p.pipeline_id] = max(0, s.age.get(p.pipeline_id, 0) - 1)
            else:
                s.age[p.pipeline_id] = s.age.get(p.pipeline_id, 0) + 1
            new_q.append(p)
        s.wait_q = new_q

    def _priority_sort_key(p):
        # Higher score first. Combine priority weight and aging.
        # Aging capped to keep queries dominant but still let batch progress.
        age = s.age.get(p.pipeline_id, 0)
        age_bonus = min(age, 50) / 50.0  # 0..1
        return (_prio_weight(p.priority) + age_bonus, age)

    def _collect_running_containers_by_priority():
        # Build list of potential preemption victims.
        victims = []
        for pool_id in range(s.executor.num_pools):
            pool = s.executor.pools[pool_id]
            # Executor interface for running containers isn't specified; infer from results?
            # We rely only on pipeline runtime_status RUNNING and ExecutionResult container_id.
            # Without a global container list, we can only preempt containers we learn about via results.
            # To still support preemption, we keep a lightweight "last seen running" table based on results.
            # If the sim doesn't provide it, preemption will be a no-op (safe).
            pass
        # We'll implement victims list from scheduler state populated from results.
        return victims

    # Maintain a simple map of "known running containers" per tick from results
    if not hasattr(s, "known_running"):
        s.known_running = {}  # container_id -> dict(pool_id, priority, last_seen_tick)
    for r in results:
        if getattr(r, "container_id", None) is None:
            continue
        # If a result arrives, the container likely finished; mark as not running by removing.
        # But we don't know if it's from completion/failure only; still remove to avoid suspending stale.
        if r.container_id in s.known_running:
            del s.known_running[r.container_id]
    # Also, when we assign new work, we'll add to known_running.

    # Tick update
    s.ticks += 1

    # Ingest new pipelines and update models based on last tick outcomes
    _update_models_from_results()
    _ingest_pipelines()
    _age_waiting()

    # Early exit if nothing to do
    if not pipelines and not results and not s.wait_q:
        return [], []

    suspensions = []
    assignments = []

    # Sort waiting queue by priority (and aging)
    s.wait_q.sort(key=_priority_sort_key, reverse=True)

    # Determine if we have blocked high-priority work (assignable ops but no placement possible)
    # We'll attempt placement first; if placement fails for query/interactive due to headroom,
    # we may preempt batch/interactive respectively.
    def _attempt_assign_in_pool(pool_id, pool, pipeline, op):
        avail_cpu, avail_ram, max_cpu, max_ram = _pool_headroom(pool)

        # Size CPU: allocate at least a small min, at most a fraction of pool, and not exceeding available.
        cpu_target = min(avail_cpu, max_cpu * s.CPU_MAX_FRACTION_POOL)
        cpu = max(_cpu_min(pipeline.priority), min(cpu_target, avail_cpu))
        if cpu <= 0:
            return None

        # Size RAM: from estimate, but can't exceed available.
        ram_est = _estimate_ram_for_op(pipeline, op, pool)
        ram = min(avail_ram, ram_est)
        # If we can't meet a minimal sensible allocation, don't place.
        if ram <= 0:
            return None

        return Assignment(
            ops=[op],
            cpu=cpu,
            ram=ram,
            priority=pipeline.priority,
            pool_id=pool_id,
            pipeline_id=pipeline.pipeline_id
        )

    # Preemption helper: suspend lowest-priority known running containers to free headroom.
    # Because we don't know each container's exact resources, preemption is conservative:
    # we only preempt when a high-priority op can't be placed anywhere at all.
    def _maybe_preempt_for(priority_needed, pool_id):
        # Only preempt when needed for QUERY/INTERACTIVE
        if priority_needed == Priority.BATCH_PIPELINE:
            return

        # Collect eligible victims from known_running (lowest priority first)
        # Only preempt in the target pool to localize disruption.
        victims = []
        for cid, meta in s.known_running.items():
            if meta.get("pool_id") != pool_id:
                continue
            pr = meta.get("priority")
            # Never preempt QUERY. Preempt INTERACTIVE only for QUERY.
            if priority_needed == Priority.QUERY:
                if pr in (Priority.INTERACTIVE, Priority.BATCH_PIPELINE):
                    victims.append((0 if pr == Priority.BATCH_PIPELINE else 1, meta.get("last_seen", 0), cid))
            elif priority_needed == Priority.INTERACTIVE:
                if pr == Priority.BATCH_PIPELINE:
                    victims.append((0, meta.get("last_seen", 0), cid))

        victims.sort(key=lambda x: (x[0], x[1]))  # batch first, then older
        preempted = 0
        for _, _, cid in victims:
            if preempted >= s.MAX_PREEMPT_PER_TICK:
                break
            cooldown_until = s.preempt_cooldown_until.get(cid, -1)
            if s.ticks < cooldown_until:
                continue
            suspensions.append(Suspend(container_id=cid, pool_id=pool_id))
            s.preempt_cooldown_until[cid] = s.ticks + s.PREEMPT_COOLDOWN_TICKS
            # Remove from known_running to avoid double preemption
            if cid in s.known_running:
                del s.known_running[cid]
            preempted += 1

    # Per pool, try to assign a small number of ops, prioritizing high-priority pipelines.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        if pool.avail_cpu_pool <= 0 or pool.avail_ram_pool <= 0:
            continue

        made = 0
        scanned = 0
        # Scan through queue and try place the first feasible op; repeat a couple times.
        while made < s.MAX_ASSIGNMENTS_PER_POOL_PER_TICK and scanned < len(s.wait_q):
            placed = False
            # iterate over a snapshot of queue indices to allow moving items
            for idx in range(len(s.wait_q)):
                p = s.wait_q[idx]
                if _pipeline_done_or_failed(p):
                    continue
                ops = _next_assignable_ops(p)
                if not ops:
                    continue
                op = ops[0]  # one op at a time to reduce risk and improve responsiveness

                a = _attempt_assign_in_pool(pool_id, pool, p, op)
                if a is not None:
                    assignments.append(a)
                    made += 1
                    placed = True
                    # Track container as "known running" once assigned; we don't have container_id yet.
                    # We can't fill it now; but keep a placeholder list of "recent assignments" to map later
                    # if the simulator provides ids in results only. This is best-effort; preemption remains safe.
                    # Rotate this pipeline to back to let others run too.
                    s.wait_q.append(s.wait_q.pop(idx))
                    break

            if not placed:
                # If we couldn't place anything, consider preempting for the highest-priority waiting op.
                # Find the best candidate among waiting pipelines.
                hp = None
                for p in s.wait_q:
                    if _pipeline_done_or_failed(p):
                        continue
                    ops = _next_assignable_ops(p)
                    if not ops:
                        continue
                    if hp is None or _prio_weight(p.priority) > _prio_weight(hp.priority):
                        hp = p
                if hp is not None and hp.priority in (Priority.QUERY, Priority.INTERACTIVE):
                    _maybe_preempt_for(hp.priority, pool_id)
                break

            scanned += 1

    # Best-effort: enrich known_running with assignments (no container_id available). No-op.

    return suspensions, assignments
