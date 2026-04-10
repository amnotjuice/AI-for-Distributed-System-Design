# policy_key: scheduler_high_027
# reasoning_effort: high
# model: gpt-5.2-2025-12-11
# llm_cost: 0.168044
# generation_seconds: 184.01
# generated_at: 2026-03-14T03:43:17.338336
def _prio_rank(prio):
    """Lower is higher priority."""
    if prio == Priority.QUERY:
        return 0
    if prio == Priority.INTERACTIVE:
        return 1
    return 2  # Priority.BATCH_PIPELINE and anything else


def _is_high_priority(prio):
    return prio in (Priority.QUERY, Priority.INTERACTIVE)


def _op_key(op):
    """
    Build a stable-ish, hashable key for per-operator retry tracking.

    We avoid relying on a single attribute because different simulator versions
    may name operator ids differently.
    """
    pid = getattr(op, "pipeline_id", None)
    oid = (
        getattr(op, "op_id", None)
        or getattr(op, "operator_id", None)
        or getattr(op, "id", None)
        or getattr(op, "name", None)
    )
    # Always include Python object identity to disambiguate in worst case.
    return (str(pid), str(oid), int(id(op)))


def _is_oom_error(err):
    s = ("" if err is None else str(err)).lower()
    return ("oom" in s) or ("out of memory" in s) or ("memoryerror" in s)


def _pool_tunables(s, pool_id):
    """
    Small, incremental improvements over naive FIFO:
      - Make pool 0 more "latency-protecting" (bigger reserve for high priority).
      - Other pools are more throughput-oriented.
    """
    if pool_id == getattr(s, "interactive_pool_id", 0):
        return {
            "reserve_cpu_frac": s.reserve_cpu_frac_interactive,
            "reserve_ram_frac": s.reserve_ram_frac_interactive,
            "hi_cpu_frac": s.hi_cpu_frac_interactive,
            "hi_ram_frac": s.hi_ram_frac_interactive,
            "batch_cpu_frac": s.batch_cpu_frac_interactive,
            "batch_ram_frac": s.batch_ram_frac_interactive,
        }
    return {
        "reserve_cpu_frac": s.reserve_cpu_frac_other,
        "reserve_ram_frac": s.reserve_ram_frac_other,
        "hi_cpu_frac": s.hi_cpu_frac_other,
        "hi_ram_frac": s.hi_ram_frac_other,
        "batch_cpu_frac": s.batch_cpu_frac_other,
        "batch_ram_frac": s.batch_ram_frac_other,
    }


def _compute_request(s, pool, pool_id, prio, avail_cpu, avail_ram, attempt, solo_on_empty_pool):
    """
    Decide cpu/ram for *one* operator container.

    Key incremental fixes vs naive:
      - Don't greedily take all available resources (reduces head-of-line blocking).
      - If the pool looks empty and this is the only ready work of its class,
        allow "scale-up" by taking the whole pool for faster completion.
      - On OOM, retry with exponential RAM backoff.
    """
    t = _pool_tunables(s, pool_id)

    # Decide target fractions based on priority.
    if _is_high_priority(prio):
        cpu_frac = t["hi_cpu_frac"]
        ram_frac = t["hi_ram_frac"]
    else:
        cpu_frac = t["batch_cpu_frac"]
        ram_frac = t["batch_ram_frac"]

    # Base targets from pool capacity.
    target_cpu = max(s.min_cpu, pool.max_cpu_pool * cpu_frac)
    target_ram = max(s.min_ram, pool.max_ram_pool * ram_frac)

    # If we're alone and the pool is empty-ish, scale up for speed.
    if solo_on_empty_pool:
        target_cpu = max(target_cpu, pool.max_cpu_pool)
        target_ram = max(target_ram, pool.max_ram_pool)

    # Apply OOM backoff (RAM-first); cap at pool max.
    if attempt > 0:
        target_ram = min(pool.max_ram_pool, max(s.min_ram, target_ram * (2 ** attempt)))

    # Fit into current availability.
    cpu = min(avail_cpu, target_cpu)
    ram = min(avail_ram, target_ram)

    # Enforce minimum allocs (if we can't satisfy them, caller should not schedule).
    if cpu < s.min_cpu:
        cpu = 0
    if ram < s.min_ram:
        ram = 0

    return cpu, ram


@register_scheduler_init(key="scheduler_high_027")
def scheduler_high_027_init(s):
    """
    Priority-aware, non-greedy scheduler with simple latency protection.

    Incremental improvements over naive FIFO:
      1) Always pick QUERY/INTERACTIVE work ahead of BATCH (prevents obvious HOL blocking).
      2) Avoid giving a single op the entire pool by default (more concurrency; faster starts).
      3) Reserve a fraction of resources (especially in pool 0) so high-priority arrivals
         can start quickly even when batch is present (soft SLO protection).
      4) On OOM failures, retry the same operator with exponential RAM backoff (RAM-first).
    """
    s.waiting_queue = []
    s.seen_pipeline_ids = set()

    # Track OOM retry attempts per operator.
    s.op_attempts = {}
    s.dead_ops = set()
    s.dead_pipelines = set()

    # Fairness knob: after many consecutive high-priority starts, allow one batch start.
    # (This is a small step to avoid total starvation without complicating preemption.)
    s.high_streak = 0
    s.max_high_streak = 12

    # Retry policy.
    s.max_oom_retries = 4

    # Minimum allocs. (Units match the simulator's pool cpu/ram units.)
    s.min_cpu = 1
    s.min_ram = 1

    # Treat pool 0 as "interactive-ish" if multiple pools exist (soft partitioning).
    s.interactive_pool_id = 0

    # Resource reservation fractions (only enforced against *batch* placements).
    s.reserve_cpu_frac_interactive = 0.40
    s.reserve_ram_frac_interactive = 0.40
    s.reserve_cpu_frac_other = 0.20
    s.reserve_ram_frac_other = 0.20

    # Per-op sizing fractions.
    # High priority: don't take whole pool by default (lets multiple queries start).
    s.hi_cpu_frac_interactive = 0.55
    s.hi_ram_frac_interactive = 0.60
    s.hi_cpu_frac_other = 0.45
    s.hi_ram_frac_other = 0.55

    # Batch: on non-interactive pools, allow larger grabs for throughput;
    # on pool 0, keep batch smaller to reduce interference.
    s.batch_cpu_frac_interactive = 0.50
    s.batch_ram_frac_interactive = 0.45
    s.batch_cpu_frac_other = 0.85
    s.batch_ram_frac_other = 0.75

    # Safety limit: avoid launching too many containers in one scheduler tick.
    s.max_assignments_per_pool = 8


@register_scheduler(key="scheduler_high_027")
def scheduler_high_027(s, results: List["ExecutionResult"], pipelines: List["Pipeline"]) -> Tuple[List["Suspend"], List["Assignment"]]:
    """
    Scheduler step.

    Notes:
      - We intentionally do *not* implement true preemption here because the minimal
        public interface in the prompt doesn't expose a pool's running containers list.
        Instead we do "soft preemption": when high-priority is waiting, we stop feeding
        batch beyond a reserved headroom.
      - This policy is designed to be a first, working latency-focused improvement
        over naive FIFO, without overfitting to unknown simulator internals.
    """
    # --- Ingest new pipelines ---
    for p in pipelines:
        pid = p.pipeline_id
        if pid in s.seen_pipeline_ids:
            continue
        s.seen_pipeline_ids.add(pid)
        s.waiting_queue.append(p)

    # --- Learn from results (OOM backoff; drop non-OOM failures) ---
    for r in results:
        ops = getattr(r, "ops", None) or []
        for op in ops:
            k = _op_key(op)
            if r.failed():
                if _is_oom_error(getattr(r, "error", None)):
                    s.op_attempts[k] = min(s.max_oom_retries + 1, s.op_attempts.get(k, 0) + 1)
                else:
                    # Non-OOM failures are treated as fatal: don't churn.
                    pid = getattr(op, "pipeline_id", None)
                    if pid is not None:
                        s.dead_pipelines.add(pid)
                    else:
                        s.dead_ops.add(k)
            else:
                # Success clears retry state for that op.
                if k in s.op_attempts:
                    del s.op_attempts[k]

    # Early exit if nothing changed.
    if not pipelines and not results:
        return [], []

    # --- Filter waiting pipelines; cache statuses ---
    status_cache = {}
    new_waiting = []
    for p in s.waiting_queue:
        pid = p.pipeline_id
        if pid in s.dead_pipelines:
            continue
        st = p.runtime_status()
        if st.is_pipeline_successful():
            continue
        status_cache[pid] = st
        new_waiting.append(p)
    s.waiting_queue = new_waiting

    # --- Compute next ready operator per pipeline (parents complete) ---
    ready_op = {}  # pipeline_id -> op
    ready_counts = {Priority.QUERY: 0, Priority.INTERACTIVE: 0, Priority.BATCH_PIPELINE: 0}

    for p in s.waiting_queue:
        pid = p.pipeline_id
        st = status_cache[pid]

        ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        if not ops:
            continue

        selected = None
        any_candidate = False
        for op in ops:
            k = _op_key(op)
            if k in s.dead_ops:
                any_candidate = True
                continue
            attempt = s.op_attempts.get(k, 0)
            any_candidate = True
            if attempt > s.max_oom_retries:
                # We've tried too many times; stop wasting capacity.
                s.dead_pipelines.add(pid)
                selected = None
                break
            selected = op
            break

        if pid in s.dead_pipelines:
            continue

        if selected is None:
            # If there were assignable ops but all are "dead"/exhausted, the pipeline is stuck.
            if any_candidate:
                s.dead_pipelines.add(pid)
            continue

        ready_op[pid] = selected
        if p.priority in ready_counts:
            ready_counts[p.priority] += 1
        else:
            # Unknown priority class: treat as batch-like.
            ready_counts[Priority.BATCH_PIPELINE] += 1

    # Re-filter after marking new dead pipelines above.
    if s.dead_pipelines:
        s.waiting_queue = [p for p in s.waiting_queue if p.pipeline_id not in s.dead_pipelines]
        ready_op = {pid: op for pid, op in ready_op.items() if pid not in s.dead_pipelines}

    high_waiting = (ready_counts.get(Priority.QUERY, 0) + ready_counts.get(Priority.INTERACTIVE, 0)) > 0

    # --- Helper to pick next pipeline in FIFO-within-priority order ---
    scheduled_this_tick = set()

    def pick_next(prio_set):
        for p in s.waiting_queue:
            pid = p.pipeline_id
            if pid in scheduled_this_tick:
                continue
            if pid not in ready_op:
                continue
            if p.priority in prio_set:
                return p, ready_op[pid]
        return None, None

    # --- Main placement loop across pools ---
    suspensions: List["Suspend"] = []
    assignments: List["Assignment"] = []

    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]

        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool
        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # Calculate batch reserve headroom (soft SLO protection).
        t = _pool_tunables(s, pool_id)
        reserve_cpu = (pool.max_cpu_pool * t["reserve_cpu_frac"]) if high_waiting else 0
        reserve_ram = (pool.max_ram_pool * t["reserve_ram_frac"]) if high_waiting else 0

        launched = 0
        while launched < s.max_assignments_per_pool and avail_cpu > 0 and avail_ram > 0:
            # Fairness: after a streak of high-priority launches, allow one batch launch.
            allow_batch_now = (s.high_streak >= s.max_high_streak) and (ready_counts.get(Priority.BATCH_PIPELINE, 0) > 0)

            # Prefer high priority generally; optionally insert batch for fairness.
            candidate_order = []
            if allow_batch_now:
                candidate_order = [
                    {Priority.BATCH_PIPELINE},
                    {Priority.QUERY},
                    {Priority.INTERACTIVE},
                ]
            else:
                candidate_order = [
                    {Priority.QUERY},
                    {Priority.INTERACTIVE},
                    {Priority.BATCH_PIPELINE},
                ]

            chosen_p = None
            chosen_op = None
            chosen_is_batch = False

            for prios in candidate_order:
                p, op = pick_next(prios)
                if p is None:
                    continue

                is_batch = (p.priority == Priority.BATCH_PIPELINE) or (not _is_high_priority(p.priority))

                # Enforce reserve only against batch.
                eff_cpu = avail_cpu
                eff_ram = avail_ram
                if is_batch and high_waiting:
                    eff_cpu = max(0, avail_cpu - reserve_cpu)
                    eff_ram = max(0, avail_ram - reserve_ram)

                # If batch can't fit minimums under reserve, skip it for now.
                if eff_cpu < s.min_cpu or eff_ram < s.min_ram:
                    continue

                chosen_p, chosen_op, chosen_is_batch = p, op, is_batch
                # Use the effective availability computed above when building the request.
                chosen_eff_cpu, chosen_eff_ram = eff_cpu, eff_ram
                break

            if chosen_p is None:
                break  # nothing fits in this pool right now

            # Decide whether we should scale up (empty pool + solo ready of this class).
            empty_pool = (avail_cpu >= pool.max_cpu_pool) and (avail_ram >= pool.max_ram_pool)
            if _is_high_priority(chosen_p.priority):
                solo = (ready_counts.get(Priority.QUERY, 0) + ready_counts.get(Priority.INTERACTIVE, 0)) == 1
            else:
                solo = ready_counts.get(Priority.BATCH_PIPELINE, 0) == 1
            solo_on_empty_pool = bool(empty_pool and solo)

            k = _op_key(chosen_op)
            attempt = s.op_attempts.get(k, 0)

            cpu, ram = _compute_request(
                s=s,
                pool=pool,
                pool_id=pool_id,
                prio=chosen_p.priority,
                avail_cpu=chosen_eff_cpu,
                avail_ram=chosen_eff_ram,
                attempt=attempt,
                solo_on_empty_pool=solo_on_empty_pool,
            )

            if cpu <= 0 or ram <= 0:
                # Can't satisfy minimums (likely due to reserve); stop trying in this pool.
                break

            assignments.append(
                Assignment(
                    ops=[chosen_op],
                    cpu=cpu,
                    ram=ram,
                    priority=chosen_p.priority,
                    pool_id=pool_id,
                    pipeline_id=chosen_p.pipeline_id,
                )
            )

            launched += 1
            scheduled_this_tick.add(chosen_p.pipeline_id)

            # Update local availability for additional placements in this pool.
            avail_cpu -= cpu
            avail_ram -= ram

            # Update streak counters for fairness.
            if _is_high_priority(chosen_p.priority):
                s.high_streak += 1
            else:
                s.high_streak = 0

    return suspensions, assignments
