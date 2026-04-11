# policy_key: scheduler_none_040
# reasoning_effort: none
# model: gpt-5.2-2025-12-11
# llm_cost: 0.065909
# generation_seconds: 48.85
# generated_at: 2026-04-09T22:16:17.085222
@register_scheduler_init(key="scheduler_none_040")
def scheduler_none_040_init(s):
    """
    Priority-first, OOM-avoiding, fairness-preserving scheduler.

    Core ideas:
    - Maintain per-priority queues (QUERY > INTERACTIVE > BATCH) with mild aging to prevent starvation.
    - Assign at most one ready operator per tick per pool (reduces head-of-line blocking and overcommit).
    - Conservative RAM sizing with per-(pipeline, op) learned retry RAM after OOM; ramp up quickly to avoid repeated failures.
    - CPU sizing: give high priority jobs a larger share, but cap to leave room for concurrency.
    - Lightweight preemption: if a high-priority pipeline is waiting and no pool has headroom, suspend one low-priority running
      container in the most constrained pool to free resources (minimize churn by limiting preemptions per tick).
    """
    # Waiting queues by priority
    s.q_query = []
    s.q_interactive = []
    s.q_batch = []

    # Simple aging to avoid starvation: store enqueue time per pipeline_id
    s.enqueue_ts = {}  # pipeline_id -> time first seen

    # Learned per-op RAM to avoid repeated OOMs: (pipeline_id, op_id) -> ram
    s.op_ram_hint = {}

    # Track OOM streaks to ramp RAM faster: (pipeline_id, op_id) -> count
    s.oom_streak = {}

    # Best-effort time source (ticks). If simulator exposes time, use it; otherwise monotonic tick count.
    s._tick = 0

    # Limit preemption churn
    s.max_preemptions_per_tick = 1

    # How aggressively to scale RAM after OOM
    s.ram_backoff_mult = 1.6
    s.ram_backoff_add_frac = 0.10  # +10% of pool ram as extra cushion (capped)

    # Minimum slice to avoid too tiny allocations that increase runtime / timeouts
    s.min_cpu_slice = 1.0
    s.min_ram_slice_frac = 0.05  # at least 5% of pool RAM for any assignment (capped by avail)

    # If any pool has at least this much free fraction, avoid preemption
    s.preempt_headroom_frac = 0.02


def _priority_rank(p):
    if p == Priority.QUERY:
        return 3
    if p == Priority.INTERACTIVE:
        return 2
    return 1


def _select_queue(s, pipeline):
    if pipeline.priority == Priority.QUERY:
        return s.q_query
    if pipeline.priority == Priority.INTERACTIVE:
        return s.q_interactive
    return s.q_batch


def _pipeline_ready_ops(pipeline):
    status = pipeline.runtime_status()
    if status.is_pipeline_successful():
        return []
    # Do not retry pipelines that have failed operators beyond recoverable state:
    # The framework marks FAILED; ASSIGNABLE_STATES includes FAILED, so we can retry operator-level failures.
    # But if there are many failures, repeated retries likely waste time; still prefer completion over drop.
    return status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)


def _op_id(op):
    # Try common identifiers; fall back to object id string
    for attr in ("op_id", "operator_id", "id", "name"):
        if hasattr(op, attr):
            try:
                v = getattr(op, attr)
                if v is not None:
                    return str(v)
            except Exception:
                pass
    return str(id(op))


def _ram_min_of_op(op):
    # Try common fields for RAM minimum; fallback to None
    for attr in ("ram_min", "min_ram", "min_memory", "memory_min", "ram", "memory"):
        if hasattr(op, attr):
            try:
                v = getattr(op, attr)
                if isinstance(v, (int, float)) and v > 0:
                    return float(v)
            except Exception:
                pass
    return None


def _cpu_hint_of_op(op):
    # Optional: if operator exposes preferred CPU; fallback None
    for attr in ("cpu", "cpu_req", "cpu_request", "cpu_hint", "vcpus"):
        if hasattr(op, attr):
            try:
                v = getattr(op, attr)
                if isinstance(v, (int, float)) and v > 0:
                    return float(v)
            except Exception:
                pass
    return None


def _score_with_aging(s, pipeline):
    # Higher is better. Base on priority and waiting time.
    base = _priority_rank(pipeline.priority) * 1000.0
    pid = pipeline.pipeline_id
    enq = s.enqueue_ts.get(pid, s._tick)
    wait = max(0, s._tick - enq)
    # Aging: batch can overtake if waiting long enough; keep mild to protect latency-dominant classes.
    if pipeline.priority == Priority.BATCH_PIPELINE:
        age_mult = 1.0 + min(0.50, wait / 600.0)  # up to +50% after 10 minutes
    elif pipeline.priority == Priority.INTERACTIVE:
        age_mult = 1.0 + min(0.25, wait / 600.0)
    else:  # query
        age_mult = 1.0 + min(0.10, wait / 600.0)
    return base * age_mult


def _choose_candidate_pipeline(s, candidates):
    # Choose by priority + aging; break ties by pipeline_id for determinism
    best = None
    best_key = None
    for p in candidates:
        key = (_score_with_aging(s, p), -int(p.pipeline_id) if isinstance(p.pipeline_id, int) else 0)
        if best is None or key > best_key:
            best = p
            best_key = key
    return best


def _size_resources_for_op(s, pipeline, op, pool, avail_cpu, avail_ram):
    # RAM: start from op min if available; else conservative slice.
    pid = pipeline.pipeline_id
    oid = _op_id(op)
    key = (pid, oid)

    pool_ram = float(pool.max_ram_pool)
    pool_cpu = float(pool.max_cpu_pool)

    hinted = s.op_ram_hint.get(key, None)
    ram_min = _ram_min_of_op(op)

    # Start point
    if hinted is not None:
        ram_target = float(hinted)
    elif ram_min is not None:
        # Add small safety margin to avoid borderline OOM.
        ram_target = float(ram_min) * 1.10
    else:
        # Unknown: allocate a modest fraction of pool RAM, biased by priority.
        if pipeline.priority == Priority.QUERY:
            ram_target = 0.25 * pool_ram
        elif pipeline.priority == Priority.INTERACTIVE:
            ram_target = 0.20 * pool_ram
        else:
            ram_target = 0.15 * pool_ram

    # Clamp by available and a minimum slice (avoid tiny allocations).
    min_slice = s.min_ram_slice_frac * pool_ram
    ram_target = max(ram_target, min_slice)
    ram_target = min(ram_target, float(avail_ram))

    # CPU: prioritize QUERY/INTERACTIVE but cap to allow concurrency.
    cpu_hint = _cpu_hint_of_op(op)
    if cpu_hint is not None:
        cpu_target = float(cpu_hint)
    else:
        if pipeline.priority == Priority.QUERY:
            cpu_target = 0.60 * pool_cpu
        elif pipeline.priority == Priority.INTERACTIVE:
            cpu_target = 0.45 * pool_cpu
        else:
            cpu_target = 0.30 * pool_cpu

    cpu_target = max(cpu_target, s.min_cpu_slice)
    cpu_target = min(cpu_target, float(avail_cpu))

    # Ensure not zeroed out
    if cpu_target <= 0 or ram_target <= 0:
        return 0.0, 0.0
    return cpu_target, ram_target


def _collect_running_containers(results):
    # Best-effort: results entries likely include running/completed; we only can preempt by container_id if visible.
    # We'll gather the most recently seen container per pool by lowest priority.
    running = []
    for r in results or []:
        try:
            if getattr(r, "container_id", None) is None:
                continue
            # If result indicates a terminal event, it might not be running anymore; but preemption request will be ignored safely.
            running.append(r)
        except Exception:
            continue
    return running


@register_scheduler(key="scheduler_none_040")
def scheduler_none_040_scheduler(s, results, pipelines):
    """
    Step function:
    - Incorporate new pipelines into priority queues.
    - Update RAM hints on OOM failures (fast backoff).
    - Optionally preempt one low-priority container if a high-priority op is blocked by headroom.
    - Place up to one operator per pool per tick, picking best pipeline by (priority + aging) among ready ones.
    """
    s._tick += 1

    # Enqueue new pipelines
    for p in pipelines:
        q = _select_queue(s, p)
        q.append(p)
        if p.pipeline_id not in s.enqueue_ts:
            s.enqueue_ts[p.pipeline_id] = s._tick

    # Update hints based on failures (focus on OOM-like errors)
    for r in results or []:
        try:
            if not r.failed():
                continue
            err = getattr(r, "error", None)
            # Heuristic: treat any error mentioning OOM/memory as memory failure
            is_oom = False
            if err is not None:
                try:
                    e = str(err).lower()
                    if "oom" in e or "out of memory" in e or "memory" in e:
                        is_oom = True
                except Exception:
                    pass

            # Map ops -> update per op hint
            for op in getattr(r, "ops", []) or []:
                pid = getattr(r, "pipeline_id", None)
                # pipeline_id might not exist on result; fallback to None-safe
                if pid is None:
                    # If not present, we can't key by pipeline; skip
                    continue
                oid = _op_id(op)
                key = (pid, oid)
                if is_oom:
                    prev = s.op_ram_hint.get(key, float(getattr(r, "ram", 0.0)) or 0.0)
                    if prev <= 0:
                        prev = float(getattr(r, "ram", 0.0)) or 0.0
                    streak = s.oom_streak.get(key, 0) + 1
                    s.oom_streak[key] = streak
                    # Multiply plus small additive cushion proportional to pool size (unknown here), so just add 10% of prev.
                    new_hint = max(prev * s.ram_backoff_mult, prev + 0.10 * max(prev, 1.0))
                    # Cap to something reasonable; actual clamp happens per pool.
                    s.op_ram_hint[key] = new_hint
                else:
                    # Non-OOM failures: don't shrink/expand aggressively; keep current hints.
                    pass
        except Exception:
            continue

    # Helper to purge completed pipelines from queues (but keep incomplete/failed ops eligible)
    def purge_queue(q):
        kept = []
        for p in q:
            st = p.runtime_status()
            if st.is_pipeline_successful():
                continue
            kept.append(p)
        return kept

    s.q_query = purge_queue(s.q_query)
    s.q_interactive = purge_queue(s.q_interactive)
    s.q_batch = purge_queue(s.q_batch)

    # Build quick list of all queued pipelines
    all_q = s.q_query + s.q_interactive + s.q_batch
    if not all_q and not (results or pipelines):
        return [], []

    suspensions = []
    assignments = []

    # Determine whether any high-priority work is blocked (no headroom in any pool)
    def any_pool_has_headroom():
        for pool_id in range(s.executor.num_pools):
            pool = s.executor.pools[pool_id]
            if pool.avail_cpu_pool > 0 and pool.avail_ram_pool > 0:
                # treat tiny slivers as no headroom to avoid futile assignments
                if (pool.avail_cpu_pool / max(1.0, pool.max_cpu_pool) >= s.preempt_headroom_frac and
                        pool.avail_ram_pool / max(1.0, pool.max_ram_pool) >= s.preempt_headroom_frac):
                    return True
        return False

    def has_waiting_high_priority_ready():
        for q in (s.q_query, s.q_interactive):
            for p in q:
                if _pipeline_ready_ops(p):
                    return True
        return False

    # Preempt at most one low-priority container if necessary
    preemptions_left = s.max_preemptions_per_tick
    if preemptions_left > 0 and has_waiting_high_priority_ready() and not any_pool_has_headroom():
        # Find a low priority container to suspend from results (best-effort)
        running = _collect_running_containers(results)
        victim = None
        victim_key = None
        for r in running:
            try:
                pr = getattr(r, "priority", None)
                # Prefer suspending BATCH over INTERACTIVE over QUERY (never suspend QUERY unless only option)
                rank = _priority_rank(pr) if pr is not None else 1
                # Lower rank -> better victim
                pool_id = getattr(r, "pool_id", 0)
                # Prefer victim in the most constrained pool (lowest available resources)
                pool = s.executor.pools[pool_id]
                constrained = (pool.avail_cpu_pool / max(1.0, pool.max_cpu_pool)) + (pool.avail_ram_pool / max(1.0, pool.max_ram_pool))
                key = (rank, constrained)
                if victim is None or key < victim_key:
                    victim = r
                    victim_key = key
            except Exception:
                continue
        if victim is not None:
            try:
                suspensions.append(Suspend(victim.container_id, victim.pool_id))
                preemptions_left -= 1
            except Exception:
                pass

    # Scheduling: one op per pool per tick, chosen by priority+aging among ready candidates
    # Iterate pools and assign best candidate that fits available resources.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)

        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # Consider candidates in descending priority order, but allow aging within each.
        # We'll assemble a candidate set from the first K pipelines of each queue to keep runtime small.
        K = 25
        candidates = []
        for q in (s.q_query, s.q_interactive, s.q_batch):
            # Keep only pipelines with at least one ready op
            cnt = 0
            for p in q:
                if cnt >= K:
                    break
                ops = _pipeline_ready_ops(p)
                if ops:
                    candidates.append(p)
                    cnt += 1

        if not candidates:
            continue

        # Choose best pipeline then choose its first ready op (deterministic)
        chosen = _choose_candidate_pipeline(s, candidates)
        if chosen is None:
            continue

        ops_ready = _pipeline_ready_ops(chosen)
        if not ops_ready:
            continue
        op = ops_ready[0]

        cpu_target, ram_target = _size_resources_for_op(s, chosen, op, pool, avail_cpu, avail_ram)
        if cpu_target <= 0 or ram_target <= 0:
            continue

        assignments.append(
            Assignment(
                ops=[op],
                cpu=cpu_target,
                ram=ram_target,
                priority=chosen.priority,
                pool_id=pool_id,
                pipeline_id=chosen.pipeline_id
            )
        )

    return suspensions, assignments

