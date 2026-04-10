# policy_key: scheduler_none_011
# reasoning_effort: none
# model: gpt-5.2-2025-12-11
# llm_cost: 0.049263
# generation_seconds: 53.16
# generated_at: 2026-04-09T21:53:37.008467
@register_scheduler_init(key="scheduler_none_011")
def scheduler_none_011_init(s):
    """Priority-aware, OOM-adaptive, low-churn scheduler.

    Goals for the weighted-latency objective:
    - Prefer completing QUERY/INTERACTIVE quickly (dominant weights) by reserving headroom.
    - Avoid failures (720s penalty) via per-operator RAM learning and bounded retries.
    - Avoid starvation of BATCH via simple aging.
    - Keep it simple: 1-op-per-pipeline concurrency, conservative preemption only when needed.
    """
    # Waiting pipelines (we select among them each tick).
    s.waiting_queue = []

    # Learned RAM requirements by operator identity (best-effort key).
    # Stores: op_key -> {"ram": float, "failures": int}
    s.op_ram_hint = {}

    # Pipeline arrival time (sim tick) for aging.
    s.pipeline_arrival_ts = {}

    # Per-pipeline retry counter for OOM-like failures.
    s.pipeline_retries = {}

    # Config knobs (kept conservative).
    s.max_retries_per_pipeline = 3
    s.ram_backoff = 1.6  # multiply RAM on OOM
    s.ram_safety = 1.15  # small headroom over learned value
    s.min_query_interactive_headroom_cpu_frac = 0.20  # keep some CPU free when possible
    s.min_query_interactive_headroom_ram_frac = 0.15  # keep some RAM free when possible
    s.batch_aging_boost_per_sec = 0.002  # small boost to prevent starvation

    # If simulator doesn't provide a clock, we maintain our own step counter.
    s._tick = 0


@register_scheduler(key="scheduler_none_011")
def scheduler_none_011(s, results, pipelines):
    """
    Policy outline:
    1) Ingest arrivals into a waiting queue; track arrival time for aging.
    2) Process results:
       - On failure: if looks like OOM, increase learned RAM hint and requeue pipeline (bounded retries).
       - On success: slightly decay RAM hint toward used RAM (optional, conservative).
    3) Build candidate assignable ops from waiting pipelines (parents complete).
    4) Place ops into pools with a soft-reservation for QUERY/INTERACTIVE:
       - For batch, do not consume the last headroom needed for potential QUERY/INTERACTIVE unless no such work exists.
    5) Limited preemption:
       - If QUERY/INTERACTIVE is waiting and cannot be scheduled anywhere due to lack of free resources,
         suspend one low-priority container (batch preferred) from the most constrained pool to create headroom.
       - Keep churn low: at most 1 suspension per tick.
    """
    s._tick += 1

    # -------- Helpers (local to avoid imports) --------
    def _pipeline_done_or_failed(p):
        st = p.runtime_status()
        if st.is_pipeline_successful():
            return True
        # Treat any FAILED operator as "pipeline has failures" but we may still retry on OOM-ish errors.
        return False

    def _status(p):
        return p.runtime_status()

    def _has_any_failed_ops(p):
        st = _status(p)
        return st.state_counts.get(OperatorState.FAILED, 0) > 0

    def _get_assignable_ops(p, limit=1):
        st = _status(p)
        return st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:limit]

    def _op_key(pipeline, op):
        # Best-effort stable identity across retries/ticks.
        # Use operator id/name if present; fall back to repr.
        op_id = getattr(op, "op_id", None)
        if op_id is None:
            op_id = getattr(op, "operator_id", None)
        if op_id is None:
            op_id = getattr(op, "name", None)
        if op_id is None:
            op_id = repr(op)
        return (pipeline.pipeline_id, op_id)

    def _priority_weight(pri):
        if pri == Priority.QUERY:
            return 10
        if pri == Priority.INTERACTIVE:
            return 5
        return 1

    def _base_priority_rank(pri):
        # Larger is better (more urgent).
        if pri == Priority.QUERY:
            return 3
        if pri == Priority.INTERACTIVE:
            return 2
        return 1

    def _pool_headroom_targets(pool):
        # Soft reservation: keep a fraction of total resources available for QUERY/INTERACTIVE.
        cpu_target = pool.max_cpu_pool * s.min_query_interactive_headroom_cpu_frac
        ram_target = pool.max_ram_pool * s.min_query_interactive_headroom_ram_frac
        return cpu_target, ram_target

    def _has_waiting_qi_work():
        for pp in s.waiting_queue:
            st = pp.runtime_status()
            if st.is_pipeline_successful():
                continue
            if pp.priority in (Priority.QUERY, Priority.INTERACTIVE):
                if _get_assignable_ops(pp, limit=1):
                    return True
        return False

    def _classify_oom(error):
        if not error:
            return False
        msg = str(error).lower()
        # Heuristic: common OOM markers.
        return ("oom" in msg) or ("out of memory" in msg) or ("killed" in msg) or ("memory" in msg and "alloc" in msg)

    def _clamp(x, lo, hi):
        if x < lo:
            return lo
        if x > hi:
            return hi
        return x

    def _recommended_ram(pool, p, op, last_ram=None):
        # Start with previous attempt ram, else learned hint, else conservative fraction of pool.
        key = _op_key(p, op)
        hint = s.op_ram_hint.get(key, None)
        if last_ram is not None and last_ram > 0:
            base = last_ram
        elif hint and hint.get("ram", 0) > 0:
            base = hint["ram"] * s.ram_safety
        else:
            # Default: don't grab whole pool; enough to avoid obvious OOMs but allow sharing.
            # If pool is small, this scales down.
            base = max(0.10 * pool.max_ram_pool, min(0.50 * pool.max_ram_pool, pool.max_ram_pool))
        return _clamp(base, 0.01, pool.max_ram_pool)

    def _recommended_cpu(pool, p):
        # Keep CPU moderate to allow more parallelism; queries get more CPU for latency.
        if p.priority == Priority.QUERY:
            return max(1.0, min(pool.avail_cpu_pool, 0.75 * pool.max_cpu_pool))
        if p.priority == Priority.INTERACTIVE:
            return max(1.0, min(pool.avail_cpu_pool, 0.50 * pool.max_cpu_pool))
        # Batch: be conservative to improve completion probability under contention.
        return max(1.0, min(pool.avail_cpu_pool, 0.33 * pool.max_cpu_pool))

    def _effective_urgency(p):
        # Combine base priority with aging for batch to avoid starvation.
        base = _base_priority_rank(p.priority) * 100.0
        arrived = s.pipeline_arrival_ts.get(p.pipeline_id, s._tick)
        age = max(0, s._tick - arrived)
        if p.priority == Priority.BATCH_PIPELINE:
            base += age * (100.0 * s.batch_aging_boost_per_sec)
        else:
            # Slight aging even for Q/I to break ties.
            base += age * 0.01
        return base

    # -------- Ingest new pipelines --------
    for p in pipelines:
        s.waiting_queue.append(p)
        if p.pipeline_id not in s.pipeline_arrival_ts:
            s.pipeline_arrival_ts[p.pipeline_id] = s._tick
        if p.pipeline_id not in s.pipeline_retries:
            s.pipeline_retries[p.pipeline_id] = 0

    suspensions = []
    assignments = []

    # -------- Process results: update hints, decide retries --------
    # Track last resources per op in case of failure.
    last_attempt_by_opkey = {}
    for r in results:
        # Save last attempt resources for potential backoff.
        for op in (r.ops or []):
            # Note: op object in results should be same type as in pipeline status.
            # We cannot always reconstruct pipeline id from op; store by repr as fallback.
            # Prefer (pipeline_id, op_id) if possible: r may not carry pipeline id.
            op_id = getattr(op, "op_id", None) or getattr(op, "operator_id", None) or getattr(op, "name", None) or repr(op)
            last_attempt_by_opkey[op_id] = (r.ram, r.cpu)

        if r.failed():
            # We only take action on likely OOM: increase RAM hint and allow retry (bounded).
            if _classify_oom(r.error):
                for op in (r.ops or []):
                    op_id = getattr(op, "op_id", None) or getattr(op, "operator_id", None) or getattr(op, "name", None) or repr(op)
                    prev_ram = r.ram if (r.ram is not None and r.ram > 0) else None
                    # We don't know pipeline_id here; update a generic key if pipeline-specific key can't be made.
                    # We'll also update pipeline-specific hint when we see op again via waiting queue.
                    generic_key = ("*", op_id)
                    prev_hint = s.op_ram_hint.get(generic_key, {"ram": 0.0, "failures": 0})
                    new_ram = (prev_ram * s.ram_backoff) if prev_ram else max(prev_hint["ram"] * s.ram_backoff, prev_hint["ram"] + 0.1)
                    s.op_ram_hint[generic_key] = {"ram": new_ram, "failures": prev_hint["failures"] + 1}
            # Otherwise: don't force retry logic here; pipeline/runtime will expose FAILED ops, and our main loop will decide.

        else:
            # On success, very conservatively "learn" that this RAM worked.
            # Again, without pipeline_id we use generic key.
            for op in (r.ops or []):
                op_id = getattr(op, "op_id", None) or getattr(op, "operator_id", None) or getattr(op, "name", None) or repr(op)
                generic_key = ("*", op_id)
                prev_hint = s.op_ram_hint.get(generic_key, {"ram": 0.0, "failures": 0})
                if r.ram is not None and r.ram > 0:
                    # Exponential moving average toward observed successful RAM.
                    if prev_hint["ram"] <= 0:
                        learned = r.ram
                    else:
                        learned = 0.8 * prev_hint["ram"] + 0.2 * r.ram
                    s.op_ram_hint[generic_key] = {"ram": learned, "failures": prev_hint.get("failures", 0)}

    # Early exit if nothing changed.
    if not pipelines and not results and not s.waiting_queue:
        return [], []

    # -------- Clean / keep waiting queue (drop only completed; avoid dropping failed) --------
    new_waiting = []
    for p in s.waiting_queue:
        st = p.runtime_status()
        if st.is_pipeline_successful():
            continue
        # Keep it even if it has failed ops: we may retry them with more RAM (bounded).
        new_waiting.append(p)
    s.waiting_queue = new_waiting

    # -------- Build candidate list: one assignable op per pipeline --------
    candidates = []
    for p in s.waiting_queue:
        st = p.runtime_status()
        if st.is_pipeline_successful():
            continue
        ops = _get_assignable_ops(p, limit=1)
        if not ops:
            continue
        op = ops[0]

        # If the pipeline has failures and we've exceeded retries, deprioritize heavily
        # (but still keep it around in case resources become ample).
        retries = s.pipeline_retries.get(p.pipeline_id, 0)
        failed_ops = _has_any_failed_ops(p)
        penalty = 0.0
        if failed_ops and retries >= s.max_retries_per_pipeline:
            penalty = 1e6  # effectively makes it last resort

        candidates.append((p, op, _effective_urgency(p) - penalty))

    # Sort by urgency descending (QUERY > INTERACTIVE > BATCH, with aging).
    candidates.sort(key=lambda x: x[2], reverse=True)

    # -------- Optional preemption (low churn): create headroom for Q/I if blocked --------
    # Only if there is Q/I work waiting and there are no assignments possible for it.
    qi_waiting = _has_waiting_qi_work()

    def _can_place_any_qi_without_preempt():
        if not qi_waiting:
            return True
        for pool_id in range(s.executor.num_pools):
            pool = s.executor.pools[pool_id]
            avail_cpu = pool.avail_cpu_pool
            avail_ram = pool.avail_ram_pool
            if avail_cpu <= 0 or avail_ram <= 0:
                continue
            # Find the highest-urgency Q/I candidate and see if it fits.
            for (p, op, _) in candidates:
                if p.priority not in (Priority.QUERY, Priority.INTERACTIVE):
                    continue
                cpu_need = min(avail_cpu, _recommended_cpu(pool, p))
                ram_need = _recommended_ram(pool, p, op, last_ram=None)
                if cpu_need <= avail_cpu and ram_need <= avail_ram:
                    return True
        return False

    if qi_waiting and not _can_place_any_qi_without_preempt():
        # Try suspending one low-priority running container to free up resources.
        # Executor interface for running containers isn't specified, so we only preempt if results carry container ids
        # and if the simulator provides a list via s.executor.pools[pid].containers (best-effort).
        preempted = False
        for pool_id in range(s.executor.num_pools):
            if preempted:
                break
            pool = s.executor.pools[pool_id]

            # Best-effort: look for pool.containers iterable with .priority and .container_id and .state.
            containers = getattr(pool, "containers", None)
            if not containers:
                continue

            # Pick a batch container if available; else interactive; never preempt query.
            victim = None
            victim_rank = None
            for c in containers:
                c_pri = getattr(c, "priority", None)
                c_state = getattr(c, "state", None)
                # Only suspend running/assigned containers (best-effort).
                if c_state not in (OperatorState.RUNNING, OperatorState.ASSIGNED):
                    continue
                if c_pri == Priority.QUERY:
                    continue
                rank = 0
                if c_pri == Priority.BATCH_PIPELINE:
                    rank = 3
                elif c_pri == Priority.INTERACTIVE:
                    rank = 2
                else:
                    rank = 1
                if victim is None or rank > victim_rank:
                    victim = c
                    victim_rank = rank

            if victim is not None:
                cid = getattr(victim, "container_id", None)
                if cid is not None:
                    suspensions.append(Suspend(container_id=cid, pool_id=pool_id))
                    preempted = True

    # -------- Placement & admission: fill pools in rounds --------
    # Approach: iterate pools, assign at most a few ops per pool per tick (to avoid greedy capture).
    # Respect soft reservations when there is Q/I demand.
    qi_demand = qi_waiting

    # Keep track to avoid scheduling multiple ops from same pipeline in one tick.
    scheduled_pipelines = set()

    # Multi-round: attempt to place candidates in priority order across pools.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool
        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        cpu_headroom, ram_headroom = _pool_headroom_targets(pool)

        # Assign multiple ops if resources allow, but stop when we can't fit anything else.
        made_progress = True
        while made_progress:
            made_progress = False

            for idx in range(len(candidates)):
                p, op, _urg = candidates[idx]
                if p.pipeline_id in scheduled_pipelines:
                    continue

                # Retry logic: if pipeline has failed ops, increment retry counter when we schedule a failed op.
                # We can't always know op state here; runtime_status can.
                st = p.runtime_status()
                op_state = getattr(st, "op_state", None)
                # If there's no API, rely on pipeline_has_failed_ops.
                has_failed = _has_any_failed_ops(p)

                # Soft reservation: if candidate is batch and there is Q/I demand,
                # do not consume below headroom thresholds.
                if qi_demand and p.priority == Priority.BATCH_PIPELINE:
                    # Only enforce reservation when headroom would be violated.
                    if (avail_cpu - 1.0) < cpu_headroom or (avail_ram - 0.01) < ram_headroom:
                        continue

                cpu_req = _recommended_cpu(pool, p)
                cpu_req = min(cpu_req, avail_cpu)

                # Use pipeline-specific hint if possible; fall back to generic.
                key = _op_key(p, op)
                generic_key = ("*", getattr(op, "op_id", None) or getattr(op, "operator_id", None) or getattr(op, "name", None) or repr(op))
                # Prefer pipeline-specific hint; else generic hint.
                last_ram = None
                if key in s.op_ram_hint and s.op_ram_hint[key].get("ram", 0) > 0:
                    last_ram = s.op_ram_hint[key]["ram"]
                elif generic_key in s.op_ram_hint and s.op_ram_hint[generic_key].get("ram", 0) > 0:
                    last_ram = s.op_ram_hint[generic_key]["ram"]

                ram_req = _recommended_ram(pool, p, op, last_ram=last_ram)

                # If it doesn't fit, try a smaller CPU first (since RAM is the main failure driver).
                if ram_req <= avail_ram and cpu_req <= avail_cpu:
                    pass
                else:
                    # Try reducing CPU down to 1 before giving up (doesn't affect OOM risk).
                    cpu_req2 = min(avail_cpu, 1.0)
                    if ram_req <= avail_ram and cpu_req2 <= avail_cpu:
                        cpu_req = cpu_req2
                    else:
                        continue

                # If pipeline has failures, cap retries; but still allow if we haven't exceeded.
                if has_failed:
                    if s.pipeline_retries.get(p.pipeline_id, 0) >= s.max_retries_per_pipeline:
                        continue
                    # Increase retry count once we attempt to run after failure.
                    s.pipeline_retries[p.pipeline_id] = s.pipeline_retries.get(p.pipeline_id, 0) + 1

                    # Also update pipeline-specific RAM hint upward a bit to reduce repeat failures.
                    prev = s.op_ram_hint.get(key, {"ram": 0.0, "failures": 0})
                    base = ram_req
                    if prev["ram"] > 0:
                        base = max(base, prev["ram"] * s.ram_backoff)
                    s.op_ram_hint[key] = {"ram": base, "failures": prev.get("failures", 0) + 1}

                # Make assignment.
                assignments.append(
                    Assignment(
                        ops=[op],
                        cpu=cpu_req,
                        ram=ram_req,
                        priority=p.priority,
                        pool_id=pool_id,
                        pipeline_id=p.pipeline_id,
                    )
                )

                # Update local available resources.
                avail_cpu -= cpu_req
                avail_ram -= ram_req
                scheduled_pipelines.add(p.pipeline_id)

                made_progress = True
                break  # restart scanning candidates with updated avail resources

    # Keep only pipelines still not finished in waiting queue; scheduled pipelines remain (they may have more ops later).
    # No need to remove scheduled ones: status will move ops to ASSIGNED/RUNNING, so next tick they won't be assignable.
    return suspensions, assignments
