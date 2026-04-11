# policy_key: scheduler_none_025
# reasoning_effort: none
# model: gpt-5.2-2025-12-11
# llm_cost: 0.042212
# generation_seconds: 47.47
# generated_at: 2026-03-14T01:59:38.842394
@register_scheduler_init(key="scheduler_none_025")
def scheduler_none_025_init(s):
    """Priority-aware, low-churn scheduler (incremental improvement over naive FIFO).

    Improvements vs naive:
      1) Maintain separate queues per priority and always try to schedule higher priority first.
      2) Avoid wasting the whole pool on a single tiny op by using simple per-op CPU/RAM caps.
      3) On OOM (or failure), increase the pipeline's "next RAM" hint and retry later (basic inference).
      4) Add lightweight aging so batch work eventually makes progress under sustained interactive load.
      5) Optionally preempt low-priority running containers if a high-priority op cannot fit (guarded to avoid churn).

    Notes:
      - This policy intentionally stays simple and safe: it never suspends equal/higher priority, and preemption
        only happens when we cannot place any high-priority ready op otherwise.
      - We assume failure reason is not perfectly modeled; we treat any failed() with error containing "OOM"
        (case-insensitive) as memory pressure and bump RAM hint.
    """
    # Queues of pipelines waiting to schedule (we keep pipelines and select ready ops each tick).
    s.q_query = []
    s.q_interactive = []
    s.q_batch = []

    # Per-pipeline resource hints learned from failures: {pipeline_id: {"ram": float, "cpu": float}}
    s.hints = {}

    # Basic aging: incremented while pipeline is enqueued but not scheduled.
    s.age = {}  # pipeline_id -> int

    # Preemption guardrails
    s.preempt_budget_per_tick = 2  # limit churn
    s.preempt_cooldown_ticks = 5   # don't repeatedly preempt the same container
    s._container_last_preempt_tick = {}  # (pool_id, container_id) -> last_tick
    s._tick = 0

    # Default initial sizing fractions of a pool (kept small to reduce interference)
    s.init_cpu_frac = {
        Priority.QUERY: 0.50,
        Priority.INTERACTIVE: 0.50,
        Priority.BATCH_PIPELINE: 0.25,
    }
    s.init_ram_frac = {
        Priority.QUERY: 0.50,
        Priority.INTERACTIVE: 0.50,
        Priority.BATCH_PIPELINE: 0.33,
    }

    # Caps so we don't allocate entire pool by default (keeps room for more ops / reduces tail latency)
    s.max_cpu_frac = {
        Priority.QUERY: 0.85,
        Priority.INTERACTIVE: 0.85,
        Priority.BATCH_PIPELINE: 0.70,
    }
    s.max_ram_frac = {
        Priority.QUERY: 0.85,
        Priority.INTERACTIVE: 0.85,
        Priority.BATCH_PIPELINE: 0.70,
    }

    # Failure backoff multipliers (RAM-first; CPU left mostly unchanged)
    s.oom_ram_mult = 1.5
    s.fail_ram_mult = 1.2

    # Aging thresholds: if batch has been waiting long, let it compete with interactive
    s.batch_age_promote = 20  # ticks


def _get_priority_queue(s, prio):
    if prio == Priority.QUERY:
        return s.q_query
    if prio == Priority.INTERACTIVE:
        return s.q_interactive
    return s.q_batch


def _is_oom_error(err):
    if err is None:
        return False
    try:
        return "oom" in str(err).lower() or "out of memory" in str(err).lower()
    except Exception:
        return False


def _pipeline_done_or_failed(p):
    st = p.runtime_status()
    if st.is_pipeline_successful():
        return True
    # If any operators are failed we still allow retry; we only drop if there are failures AND nothing assignable.
    return False


def _get_ready_ops(p):
    st = p.runtime_status()
    # We only schedule ops whose parents are complete.
    return st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)


def _ensure_hint(s, pipeline, pool):
    pid = pipeline.pipeline_id
    if pid not in s.hints:
        # Start with a conservative fraction of pool; this is an improvement over "take all avail".
        cpu = max(1.0, pool.max_cpu_pool * s.init_cpu_frac.get(pipeline.priority, 0.25))
        ram = max(1.0, pool.max_ram_pool * s.init_ram_frac.get(pipeline.priority, 0.33))
        s.hints[pid] = {"cpu": float(cpu), "ram": float(ram)}
    return s.hints[pid]


def _bounded_allocation(s, pipeline, pool, avail_cpu, avail_ram):
    """Return (cpu, ram) to try, bounded by pool max fractions and current availability."""
    hint = _ensure_hint(s, pipeline, pool)

    max_cpu = max(1.0, pool.max_cpu_pool * s.max_cpu_frac.get(pipeline.priority, 0.7))
    max_ram = max(1.0, pool.max_ram_pool * s.max_ram_frac.get(pipeline.priority, 0.7))

    cpu = min(float(hint["cpu"]), float(max_cpu), float(avail_cpu))
    ram = min(float(hint["ram"]), float(max_ram), float(avail_ram))

    # Ensure non-zero allocations if any resources exist.
    if avail_cpu > 0:
        cpu = max(1.0, cpu)
    if avail_ram > 0:
        ram = max(1.0, ram)
    return cpu, ram


def _bump_hints_on_failure(s, res):
    """Update per-pipeline hints based on failure signals."""
    # We rely on res.ops to find pipeline_id? Not provided; but scheduler assigns pipeline_id to Assignment.
    # ExecutionResult does not expose pipeline_id per provided API. So we approximate by using priority-wide bumps
    # only when we can map; otherwise we do nothing. Many simulators include op metadata, but we stay defensive.
    # If ops carry pipeline_id attribute, we use it.
    pipeline_id = None
    try:
        if res.ops and hasattr(res.ops[0], "pipeline_id"):
            pipeline_id = res.ops[0].pipeline_id
    except Exception:
        pipeline_id = None

    if pipeline_id is None:
        return

    hint = s.hints.get(pipeline_id)
    if hint is None:
        return

    if res.failed():
        if _is_oom_error(res.error):
            hint["ram"] = max(1.0, float(res.ram)) * s.oom_ram_mult if getattr(res, "ram", None) else float(hint["ram"]) * s.oom_ram_mult
        else:
            hint["ram"] = float(hint["ram"]) * s.fail_ram_mult
        # CPU hint: keep stable; minor nudge down if we likely over-allocated and still failed non-OOM.
        if not _is_oom_error(res.error) and getattr(res, "cpu", None):
            hint["cpu"] = max(1.0, float(hint["cpu"]) * 0.95)


def _collect_running_containers(results):
    """Best-effort view of running containers by pool and priority.

    The simulator may not send RUNNING events each tick; we infer from latest results list only.
    This is used only for conservative preemption decisions, so partial info is acceptable.
    """
    running = {}  # pool_id -> list of (priority, container_id)
    for r in results:
        # If result isn't failed and has a container_id, we consider it as evidence container exists.
        try:
            if getattr(r, "container_id", None) is None:
                continue
            pool_id = r.pool_id
            prio = r.priority
            running.setdefault(pool_id, [])
            running[pool_id].append((prio, r.container_id))
        except Exception:
            continue
    return running


def _pick_preemptions(s, pool_id, running, needed_cpu, needed_ram, avail_cpu, avail_ram):
    """Pick low-priority containers to preempt to free enough headroom.

    Returns list of Suspend. We do NOT know container sizes, so we use a crude strategy:
    suspend up to N low-priority containers when we are short on either CPU or RAM.
    This is deliberately conservative: only used when high priority cannot be placed otherwise.
    """
    short_cpu = needed_cpu - avail_cpu
    short_ram = needed_ram - avail_ram
    if short_cpu <= 0 and short_ram <= 0:
        return []

    candidates = running.get(pool_id, [])
    # Sort low priority first (batch, then interactive). Never preempt QUERY.
    def score(item):
        prio, cid = item
        if prio == Priority.BATCH_PIPELINE:
            return 0
        if prio == Priority.INTERACTIVE:
            return 1
        return 10  # QUERY protected

    candidates = sorted(candidates, key=score)
    susp = []
    budget = s.preempt_budget_per_tick
    for prio, cid in candidates:
        if budget <= 0:
            break
        if prio == Priority.QUERY:
            continue

        last = s._container_last_preempt_tick.get((pool_id, cid), -10**9)
        if (s._tick - last) < s.preempt_cooldown_ticks:
            continue

        susp.append(Suspend(container_id=cid, pool_id=pool_id))
        s._container_last_preempt_tick[(pool_id, cid)] = s._tick
        budget -= 1

        # We don't know exact freed resources; assume each preemption helps enough after a couple.
        if len(susp) >= 2:
            break
    return susp


@register_scheduler(key="scheduler_none_025")
def scheduler_none_025(s, results, pipelines):
    """
    Priority-aware queueing with RAM-hint retries and guarded preemption.

    Decision order per tick:
      1) Incorporate new pipelines into per-priority queues.
      2) Update resource hints based on any observed failures (OOM => RAM up).
      3) Age waiting pipelines.
      4) For each pool, greedily schedule ready ops:
           - serve QUERY first, then INTERACTIVE, then BATCH (with aging promotion).
           - allocate bounded CPU/RAM (hinted, capped, and within availability).
           - if a high-priority ready op can't fit and pool has low-priority containers, preempt a few.
    """
    s._tick += 1

    # Learn from results
    for r in results:
        try:
            _bump_hints_on_failure(s, r)
        except Exception:
            pass

    # Enqueue new pipelines by priority
    for p in pipelines:
        _get_priority_queue(s, p.priority).append(p)
        s.age[p.pipeline_id] = s.age.get(p.pipeline_id, 0)

    # If nothing changed, avoid work.
    if not pipelines and not results:
        return [], []

    # Age everything in queues (simple fairness)
    for q in (s.q_query, s.q_interactive, s.q_batch):
        for p in q:
            s.age[p.pipeline_id] = s.age.get(p.pipeline_id, 0) + 1

    suspensions = []
    assignments = []

    # Best-effort view of containers in pools to inform preemption (conservative).
    running = _collect_running_containers(results)

    # Helper to pop next schedulable pipeline from a queue, with rotation.
    def pop_next_schedulable(q):
        # Rotate through queue to find a pipeline with a ready op.
        n = len(q)
        for _ in range(n):
            p = q.pop(0)
            if _pipeline_done_or_failed(p):
                # drop successful pipelines
                continue

            ready = _get_ready_ops(p)
            if ready:
                return p, ready
            # Not ready yet (waiting on parents); keep it.
            q.append(p)
        return None, None

    # Batch promotion: if some batch has aged enough, let it be treated as interactive for selection.
    def has_promoted_batch():
        for p in s.q_batch:
            if s.age.get(p.pipeline_id, 0) >= s.batch_age_promote:
                return True
        return False

    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool
        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # Try to place multiple ops per pool if resources remain.
        # Keep it bounded to avoid overscheduling in one tick.
        placements_this_pool = 0
        max_placements = 4

        while placements_this_pool < max_placements:
            avail_cpu = pool.avail_cpu_pool
            avail_ram = pool.avail_ram_pool
            if avail_cpu <= 0 or avail_ram <= 0:
                break

            # Choose which queue to draw from (priority order with batch aging promotion)
            queues_in_order = [s.q_query, s.q_interactive]
            if has_promoted_batch():
                # Promote aged batch to compete before interactive (but after query).
                queues_in_order = [s.q_query, s.q_batch, s.q_interactive]
            else:
                queues_in_order = [s.q_query, s.q_interactive, s.q_batch]

            chosen = None
            chosen_ops = None
            chosen_q = None
            for q in queues_in_order:
                p, ready_ops = pop_next_schedulable(q)
                if p is not None:
                    chosen, chosen_ops, chosen_q = p, ready_ops, q
                    break

            if chosen is None:
                break

            # Schedule only one op at a time from the chosen pipeline (incremental from naive).
            op_list = chosen_ops[:1]

            # Compute bounded allocation.
            cpu, ram = _bounded_allocation(s, chosen, pool, avail_cpu, avail_ram)

            # If it doesn't fit well (e.g., due to caps or availability), attempt guarded preemption
            # only for high priority (QUERY/INTERACTIVE).
            if (cpu > avail_cpu or ram > avail_ram) and chosen.priority in (Priority.QUERY, Priority.INTERACTIVE):
                # Request at least the bounded allocation, but if availability is lower, preempt.
                needed_cpu = min(cpu, pool.max_cpu_pool)
                needed_ram = min(ram, pool.max_ram_pool)
                new_susp = _pick_preemptions(s, pool_id, running, needed_cpu, needed_ram, avail_cpu, avail_ram)
                if new_susp:
                    suspensions.extend(new_susp)
                    # Requeue chosen pipeline and stop scheduling more in this pool this tick
                    # to let preemption take effect deterministically next tick.
                    chosen_q.append(chosen)
                    break

            # If still can't fit, downshift allocation to whatever is available.
            cpu = min(cpu, avail_cpu)
            ram = min(ram, avail_ram)
            if cpu <= 0 or ram <= 0:
                chosen_q.append(chosen)
                break

            assignment = Assignment(
                ops=op_list,
                cpu=cpu,
                ram=ram,
                priority=chosen.priority,
                pool_id=pool_id,
                pipeline_id=chosen.pipeline_id,
            )
            assignments.append(assignment)

            # Put pipeline back for future ops (if any remain).
            chosen_q.append(chosen)
            placements_this_pool += 1

    return suspensions, assignments
