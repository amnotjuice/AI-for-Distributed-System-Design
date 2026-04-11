# policy_key: scheduler_none_002
# reasoning_effort: none
# model: gpt-5.2-2025-12-11
# llm_cost: 0.046104
# generation_seconds: 37.34
# generated_at: 2026-03-12T21:22:25.353188
@register_scheduler_init(key="scheduler_none_002")
def scheduler_none_002_init(s):
    """Priority-aware FIFO with minimal, safe improvements over naive FIFO.

    Improvements vs naive example:
    - Maintain separate FIFO queues per priority (QUERY, INTERACTIVE, BATCH_PIPELINE).
    - Reserve a small fraction of each pool for high priority (soft reservation):
      do not schedule BATCH if it would dip below reserved headroom.
    - Place high priority first; pack onto the pool with most available CPU (simple best-fit).
    - On OOM, retry the same pipeline later with increased RAM guess for that pipeline (bounded).
    """
    # Per-priority waiting queues (FIFO)
    s.wait_q = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }

    # Remember per-pipeline resource guesses (RAM grows on OOM; CPU is conservative)
    # pipeline_id -> {"ram": float, "cpu": float}
    s.pipeline_guess = {}

    # Track simple OOM counts to backoff/ramp resources
    s.oom_count = {}  # pipeline_id -> int

    # Soft reservations (fractions of pool capacity kept available)
    # Batch will not consume into these reservations when higher priority work exists/arrives.
    s.reserve_frac = {
        Priority.QUERY: (0.10, 0.10),         # (cpu_frac, ram_frac)
        Priority.INTERACTIVE: (0.20, 0.20),
    }

    # Clamp guesses to avoid extreme allocations
    s.max_ram_frac = 0.80  # per assignment at most 80% of pool RAM
    s.min_cpu = 1.0
    s.min_ram = 0.5


def _prio_order():
    # Highest first
    return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]


def _ensure_guess(s, pipeline, pool):
    pid = pipeline.pipeline_id
    if pid in s.pipeline_guess:
        return
    # Start small and safe: allocate a modest share; allow concurrency.
    # (CPU too large can starve others; RAM too small might OOM; we ramp on OOM.)
    init_cpu = max(s.min_cpu, min(pool.max_cpu_pool * 0.25, pool.max_cpu_pool))
    init_ram = max(s.min_ram, min(pool.max_ram_pool * 0.25, pool.max_ram_pool))
    s.pipeline_guess[pid] = {"cpu": init_cpu, "ram": init_ram}
    s.oom_count.setdefault(pid, 0)


def _bump_ram_on_oom(s, pipeline_id, pool):
    g = s.pipeline_guess.get(pipeline_id)
    if not g:
        return
    # Exponential-ish increase with cap
    s.oom_count[pipeline_id] = s.oom_count.get(pipeline_id, 0) + 1
    factor = 1.5 if s.oom_count[pipeline_id] <= 2 else 2.0
    new_ram = min(g["ram"] * factor, pool.max_ram_pool * s.max_ram_frac)
    g["ram"] = max(new_ram, s.min_ram)


def _select_pool_for_priority(s, prio):
    # Choose pool with most available CPU; simple, stable heuristic.
    best = None
    best_score = None
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        # Skip empty pools
        if pool.avail_cpu_pool <= 0 or pool.avail_ram_pool <= 0:
            continue
        score = (pool.avail_cpu_pool, pool.avail_ram_pool)
        if best is None or score > best_score:
            best = pool_id
            best_score = score
    return best


def _has_waiting_high_priority(s):
    return bool(s.wait_q[Priority.QUERY] or s.wait_q[Priority.INTERACTIVE])


def _pool_reserved(s, pool, prio):
    # Return reserved absolute amounts (cpu, ram) for given pool and prio
    if prio not in s.reserve_frac:
        return (0.0, 0.0)
    cpu_f, ram_f = s.reserve_frac[prio]
    return (pool.max_cpu_pool * cpu_f, pool.max_ram_pool * ram_f)


def _can_schedule_batch_without_breaking_reserves(s, pool, cpu_need, ram_need):
    # If there is high-priority waiting, protect reserved headroom in this pool.
    if not _has_waiting_high_priority(s):
        return True
    # Reserve for both QUERY and INTERACTIVE; take max reservation
    rq_cpu, rq_ram = _pool_reserved(s, pool, Priority.QUERY)
    ri_cpu, ri_ram = _pool_reserved(s, pool, Priority.INTERACTIVE)
    res_cpu = max(rq_cpu, ri_cpu)
    res_ram = max(rq_ram, ri_ram)

    # After scheduling batch, remaining should still be >= reserved
    return (pool.avail_cpu_pool - cpu_need) >= res_cpu and (pool.avail_ram_pool - ram_need) >= res_ram


@register_scheduler(key="scheduler_none_002")
def scheduler_none_002(s, results, pipelines):
    """
    Priority-aware FIFO scheduler with soft reservations and OOM-driven RAM growth.

    Core loop:
    1) Enqueue new pipelines into per-priority FIFOs.
    2) Process results: on OOM failure, increase RAM guess and requeue pipeline.
    3) For each pool, schedule at most one operator (like naive baseline), but:
       - pick highest priority available pipeline first,
       - apply soft reservation against batch if high priority is waiting.
    """
    # Enqueue new arrivals
    for p in pipelines:
        s.wait_q[p.priority].append(p)

    # Early exit if nothing changed
    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    # Handle results: ramp RAM on OOM and requeue the pipeline (unless terminal failure policy)
    for r in results:
        if not r.failed():
            continue
        # Best-effort: only treat OOM specially; other failures are not retried.
        err = (r.error or "").lower() if hasattr(r, "error") else ""
        if "oom" in err or "out of memory" in err or "memory" in err:
            pool = s.executor.pools[r.pool_id]
            _bump_ram_on_oom(s, r.ops[0].pipeline_id if hasattr(r.ops[0], "pipeline_id") else None, pool)

    # To requeue on OOM we need pipeline objects; we don't have a reverse map from result->pipeline.
    # Instead, we rely on runtime_status() showing FAILED state which is assignable; and we increase
    # guesses when we see OOM, but pipeline is already in queues or will be re-enqueued below.
    #
    # Ensure all non-terminal pipelines remain in queues (avoid dropping).
    # We do a light "refresh": keep only pipelines that still need work; re-enqueue others.
    refreshed = {Priority.QUERY: [], Priority.INTERACTIVE: [], Priority.BATCH_PIPELINE: []}
    for pr in _prio_order():
        for p in s.wait_q[pr]:
            st = p.runtime_status()
            has_failures = st.state_counts[OperatorState.FAILED] > 0
            # Drop completed or (non-OOM) failed pipelines; since we cannot disambiguate here,
            # we keep FAILED pipelines as retryable (aligns with ASSIGNABLE_STATES including FAILED).
            if st.is_pipeline_successful():
                continue
            if has_failures:
                # Keep for retry; resource bump is handled elsewhere / next scheduling.
                refreshed[pr].append(p)
            else:
                refreshed[pr].append(p)
    s.wait_q = refreshed

    # Schedule: at most one operator per pool (incremental improvement over baseline)
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        if pool.avail_cpu_pool <= 0 or pool.avail_ram_pool <= 0:
            continue

        # Find next runnable op from highest priority pipeline
        chosen_pipeline = None
        chosen_op_list = None
        chosen_prio = None

        for pr in _prio_order():
            q = s.wait_q[pr]
            if not q:
                continue

            # FIFO scan with small bounded lookahead to skip blocked pipelines
            # (e.g., parents not complete). Put skipped ones back preserving order.
            skipped = []
            found = None
            found_ops = None
            lookahead = min(8, len(q))
            for _ in range(lookahead):
                p = q.pop(0)
                st = p.runtime_status()
                if st.is_pipeline_successful():
                    continue
                ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
                if ops:
                    found = p
                    found_ops = ops
                    break
                skipped.append(p)

            # Restore skipped to front in original order
            s.wait_q[pr] = skipped + s.wait_q[pr]

            if found is not None:
                chosen_pipeline = found
                chosen_op_list = found_ops
                chosen_prio = pr
                break

        if chosen_pipeline is None:
            continue

        # Create/adjust resource guesses with awareness of this pool capacity
        _ensure_guess(s, chosen_pipeline, pool)
        g = s.pipeline_guess[chosen_pipeline.pipeline_id]

        # Determine requested resources, limited by pool availability
        cpu_need = max(s.min_cpu, min(g["cpu"], pool.avail_cpu_pool))
        ram_cap = min(pool.max_ram_pool * s.max_ram_frac, pool.avail_ram_pool)
        ram_need = max(s.min_ram, min(g["ram"], ram_cap))

        # Soft reservation: prevent batch from consuming reserved headroom if high-priority queued
        if chosen_prio == Priority.BATCH_PIPELINE:
            if not _can_schedule_batch_without_breaking_reserves(s, pool, cpu_need, ram_need):
                # Put it back to the front and skip scheduling on this pool this tick
                s.wait_q[chosen_prio].insert(0, chosen_pipeline)
                continue

        # If we cannot meet minimums, defer
        if cpu_need <= 0 or ram_need <= 0:
            s.wait_q[chosen_prio].insert(0, chosen_pipeline)
            continue

        assignments.append(
            Assignment(
                ops=chosen_op_list,
                cpu=cpu_need,
                ram=ram_need,
                priority=chosen_pipeline.priority,
                pool_id=pool_id,
                pipeline_id=chosen_pipeline.pipeline_id,
            )
        )

        # Requeue pipeline for future operators
        s.wait_q[chosen_prio].append(chosen_pipeline)

    return suspensions, assignments
