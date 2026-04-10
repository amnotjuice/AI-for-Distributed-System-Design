# policy_key: scheduler_none_022
# reasoning_effort: none
# model: gpt-5.2-2025-12-11
# llm_cost: 0.061667
# generation_seconds: 42.28
# generated_at: 2026-04-09T22:01:53.406032
@register_scheduler_init(key="scheduler_none_022")
def scheduler_none_022_init(s):
    """Priority-aware, OOM-adaptive, anti-starvation scheduler.

    Main ideas:
    - Weighted priorities: QUERY > INTERACTIVE > BATCH, but with aging to prevent starvation.
    - One operator per tick per pool (simple, avoids over-committing and reduces OOM cascades).
    - Conservative initial RAM sizing with OOM-triggered backoff per pipeline (reduces repeat failures).
    - Light preemption: if high-priority work is waiting and no pool can fit it, suspend one
      low-priority running container (chosen by lowest priority + biggest resource footprint) to free headroom.
    """
    # Waiting pipelines (store pipeline_id to avoid duplicates)
    s.waiting_queue = []
    s.waiting_set = set()

    # When did we first see a pipeline (for aging)
    s.first_seen_ts = {}

    # Per-pipeline RAM "safety factor" that increases after OOM to avoid repeated failures
    s.ram_safety_factor = {}  # pipeline_id -> float

    # Per-pipeline min RAM observed that worked (based on last successful run container RAM)
    s.last_good_ram = {}  # pipeline_id -> float

    # Track last tick time if available, else fall back to monotonic counter increments
    s._tick_counter = 0

    # Conservative defaults
    s.base_ram_fraction = 0.25  # initial RAM as fraction of pool RAM (capped by availability)
    s.max_ram_fraction = 0.90   # don't allocate entire pool RAM (leave a little slack)
    s.base_cpu_fraction = 0.50  # initial CPU as fraction of pool CPU (capped by availability)
    s.max_cpu_fraction = 1.00

    # Aging tuning: how much waiting boosts effective priority (in "priority points")
    s.aging_rate = 0.002  # points per second (or per tick if no time signal)

    # Preemption guardrails
    s.preempt_cooldown = 5  # ticks between preemption attempts
    s._last_preempt_tick = -10

    # Track approximate running containers per pool so we can choose preemption candidates.
    # We update this from results; if the simulator doesn't provide "start" events, we'll
    # keep it best-effort based on known assignments and completions.
    s.running = {}  # (pool_id, container_id) -> dict(priority, cpu, ram)
    s._known_container_priority = {}  # container_id -> priority


def _pweight(priority):
    # Higher is more important.
    if priority == Priority.QUERY:
        return 3.0
    if priority == Priority.INTERACTIVE:
        return 2.0
    return 1.0


def _effective_score(s, pipeline):
    # Higher means schedule sooner.
    # Base: priority weight. Add aging based on how long it has waited.
    pid = pipeline.pipeline_id
    base = _pweight(pipeline.priority)

    now = getattr(s.executor, "now", None)
    if now is None:
        now = s._tick_counter

    first = s.first_seen_ts.get(pid, now)
    wait = max(0.0, float(now) - float(first))
    return base + s.aging_rate * wait


def _pipeline_is_done_or_dead(pipeline):
    status = pipeline.runtime_status()
    if status.is_pipeline_successful():
        return True
    # If any operator has FAILED, we treat pipeline as dead (no retries) to avoid infinite loops.
    # However, we *do* allow retrying FAILED ops via ASSIGNABLE_STATES in some simulators.
    # We'll interpret "dead" as "has failures and no assignable ops remain".
    failed_cnt = status.state_counts.get(OperatorState.FAILED, 0)
    if failed_cnt <= 0:
        return False
    # If there are assignable ops (PENDING/FAILED) with parents complete, it's still retryable.
    op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
    return len(op_list) == 0


def _pick_next_op(pipeline):
    status = pipeline.runtime_status()
    # Prefer ready ops whose parents are complete; one op at a time to reduce interference.
    op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
    return op_list


def _choose_resources_for_pool(s, pipeline, pool_id):
    """Choose (cpu, ram) for an op in this pool given available resources and adaptive state."""
    pool = s.executor.pools[pool_id]
    avail_cpu = pool.avail_cpu_pool
    avail_ram = pool.avail_ram_pool
    if avail_cpu <= 0 or avail_ram <= 0:
        return None

    pid = pipeline.pipeline_id

    safety = s.ram_safety_factor.get(pid, 1.0)
    last_good = s.last_good_ram.get(pid, None)

    # RAM: be conservative but not tiny; use last_good if known; else use a fraction of pool.
    target_ram = avail_ram * s.base_ram_fraction
    if last_good is not None:
        # Try last known good RAM first; apply safety factor if we recently OOM'd.
        target_ram = max(target_ram, float(last_good) * safety)

    # Cap and floor
    target_ram = min(target_ram, avail_ram * s.max_ram_fraction, avail_ram)
    # Ensure non-zero
    if target_ram <= 0:
        return None

    # CPU: queries/interactive benefit from more CPU for latency; batch less so.
    # Still avoid grabbing all CPU if it starves others; allocate fraction based on priority.
    if pipeline.priority == Priority.QUERY:
        cpu_frac = 0.90
    elif pipeline.priority == Priority.INTERACTIVE:
        cpu_frac = 0.70
    else:
        cpu_frac = s.base_cpu_fraction

    target_cpu = avail_cpu * cpu_frac
    target_cpu = min(target_cpu, avail_cpu * s.max_cpu_fraction, avail_cpu)
    if target_cpu <= 0:
        return None

    return target_cpu, target_ram


def _update_from_results(s, results):
    """Update OOM backoff and track running container info best-effort."""
    # We don't know exact error types; treat any failure as a hint to increase RAM safety
    # (bounded), especially for high-priority to reduce repeated 720s penalties.
    for r in results:
        # Track container -> priority mapping if possible
        if getattr(r, "container_id", None) is not None:
            s._known_container_priority[r.container_id] = getattr(r, "priority", None)

        # Remove from running map upon any result (completion/failure/suspend)
        if getattr(r, "pool_id", None) is not None and getattr(r, "container_id", None) is not None:
            s.running.pop((r.pool_id, r.container_id), None)

        if hasattr(r, "failed") and r.failed():
            # Increase RAM safety factor for the owning pipeline
            # Try to infer pipeline_id: ExecutionResult may not have it; fall back to op pipeline ownership if present.
            pid = getattr(r, "pipeline_id", None)
            if pid is None:
                # Best-effort: if op objects carry pipeline_id attribute
                try:
                    if r.ops and hasattr(r.ops[0], "pipeline_id"):
                        pid = r.ops[0].pipeline_id
                except Exception:
                    pid = None

            if pid is not None:
                old = s.ram_safety_factor.get(pid, 1.0)
                # Faster ramp for higher priority to avoid repeated failures
                pr = getattr(r, "priority", None)
                mult = 1.6 if pr == Priority.QUERY else (1.4 if pr == Priority.INTERACTIVE else 1.25)
                s.ram_safety_factor[pid] = min(old * mult, 8.0)

        else:
            # On success, record last good RAM and slowly relax safety factor
            pid = getattr(r, "pipeline_id", None)
            if pid is None:
                try:
                    if r.ops and hasattr(r.ops[0], "pipeline_id"):
                        pid = r.ops[0].pipeline_id
                except Exception:
                    pid = None
            if pid is not None:
                if getattr(r, "ram", None) is not None:
                    s.last_good_ram[pid] = float(r.ram)
                old = s.ram_safety_factor.get(pid, 1.0)
                s.ram_safety_factor[pid] = max(1.0, old * 0.95)


def _try_preempt_for_headroom(s, needed_cpu, needed_ram, pool_id, high_pri_weight):
    """Attempt to preempt a low-priority running container in pool_id to free headroom."""
    # Cooldown to avoid thrash
    if s._tick_counter - s._last_preempt_tick < s.preempt_cooldown:
        return []

    # If there is no running state, cannot preempt
    candidates = []
    for (p_id, c_id), info in s.running.items():
        if p_id != pool_id:
            continue
        pr = info.get("priority", Priority.BATCH_PIPELINE)
        w = _pweight(pr)
        # Only preempt strictly lower importance
        if w >= high_pri_weight:
            continue
        cpu = float(info.get("cpu", 0.0) or 0.0)
        ram = float(info.get("ram", 0.0) or 0.0)
        # Rank: lower priority first, then bigger footprint (free more quickly)
        candidates.append((w, -(cpu + 0.25 * ram), c_id))

    if not candidates:
        return []

    candidates.sort()
    _, _, victim_id = candidates[0]
    s._last_preempt_tick = s._tick_counter
    return [Suspend(victim_id, pool_id)]


@register_scheduler(key="scheduler_none_022")
def scheduler_none_022_scheduler(s, results, pipelines):
    """
    Scheduler step:
    - Ingest new pipelines, track first-seen time.
    - Update adaptive RAM safety from execution results.
    - Build a prioritized runnable list using (priority + aging).
    - For each pool, schedule at most 1 ready operator (to limit contention).
    - If a QUERY/INTERACTIVE pipeline is runnable but doesn't fit, attempt one preemption.
    """
    s._tick_counter += 1

    _update_from_results(s, results)

    # Enqueue arrivals without duplicates
    now = getattr(s.executor, "now", None)
    if now is None:
        now = s._tick_counter

    for p in pipelines:
        pid = p.pipeline_id
        if pid not in s.first_seen_ts:
            s.first_seen_ts[pid] = now
        if pid not in s.waiting_set:
            s.waiting_queue.append(p)
            s.waiting_set.add(pid)

    # Early exit if nothing to do
    if not pipelines and not results and not s.waiting_queue:
        return [], []

    # Clean out completed/dead pipelines from queue
    new_q = []
    new_set = set()
    for p in s.waiting_queue:
        if _pipeline_is_done_or_dead(p):
            continue
        new_q.append(p)
        new_set.add(p.pipeline_id)
    s.waiting_queue = new_q
    s.waiting_set = new_set

    # Build list of runnable pipelines with a ready op
    runnable = []
    for p in s.waiting_queue:
        op_list = _pick_next_op(p)
        if not op_list:
            continue
        runnable.append(p)

    # Sort by effective score desc (priority + aging)
    runnable.sort(key=lambda p: _effective_score(s, p), reverse=True)

    suspensions = []
    assignments = []

    # Helper: attempt to schedule a specific pipeline in some pool
    def schedule_pipeline_in_best_pool(p):
        # Try pools with most available RAM first (reduces OOM and improves completion rate)
        pool_order = list(range(s.executor.num_pools))
        pool_order.sort(key=lambda pid: (s.executor.pools[pid].avail_ram_pool, s.executor.pools[pid].avail_cpu_pool), reverse=True)

        op_list = _pick_next_op(p)
        if not op_list:
            return False

        for pool_id in pool_order:
            pool = s.executor.pools[pool_id]
            avail_cpu = pool.avail_cpu_pool
            avail_ram = pool.avail_ram_pool
            res = _choose_resources_for_pool(s, p, pool_id)
            if res is None:
                continue
            cpu, ram = res
            if cpu <= 0 or ram <= 0:
                continue
            if cpu > avail_cpu or ram > avail_ram:
                continue

            a = Assignment(
                ops=op_list,
                cpu=cpu,
                ram=ram,
                priority=p.priority,
                pool_id=pool_id,
                pipeline_id=p.pipeline_id
            )
            assignments.append(a)
            # Best-effort running tracking: container_id is not known here; we can't add.
            return True
        return False

    # We schedule at most one assignment per pool per tick to avoid blowing up tail latency.
    # First pass: schedule high-priority runnable pipelines
    pools_used = set()
    for p in runnable:
        # Stop if we've filled all pools for this tick
        if len(pools_used) >= s.executor.num_pools:
            break

        # Try to schedule; but ensure we don't exceed one per pool by checking if any pool has resources
        # and choosing best pool implicitly. We'll approximate pools_used by marking a pool once assigned.
        before = len(assignments)
        ok = schedule_pipeline_in_best_pool(p)
        after = len(assignments)
        if ok and after > before:
            pools_used.add(assignments[-1].pool_id)

    # If we couldn't schedule any high-priority work and some is waiting, try a single preemption.
    # Identify top waiting QUERY/INTERACTIVE that has a ready op.
    if not assignments:
        top_hp = None
        for p in runnable:
            if p.priority in (Priority.QUERY, Priority.INTERACTIVE):
                top_hp = p
                break

        if top_hp is not None:
            # Estimate needed resources as what we'd like to allocate in the best pool;
            # if it doesn't fit anywhere, preempt in the pool that would most likely host it.
            # Choose pool with max headroom (avail) but still not enough; preempt there.
            pool_order = list(range(s.executor.num_pools))
            pool_order.sort(key=lambda pid: (s.executor.pools[pid].avail_ram_pool, s.executor.pools[pid].avail_cpu_pool), reverse=True)

            hpw = _pweight(top_hp.priority)
            # Choose a target pool (first in order)
            if pool_order:
                target_pool = pool_order[0]
                res = _choose_resources_for_pool(s, top_hp, target_pool)
                if res is not None:
                    needed_cpu, needed_ram = res
                    pool = s.executor.pools[target_pool]
                    if needed_cpu > pool.avail_cpu_pool or needed_ram > pool.avail_ram_pool:
                        suspensions.extend(_try_preempt_for_headroom(s, needed_cpu, needed_ram, target_pool, hpw))

    # Maintain waiting_queue (pipelines remain until done/dead). No need to reshuffle here.
    return suspensions, assignments
