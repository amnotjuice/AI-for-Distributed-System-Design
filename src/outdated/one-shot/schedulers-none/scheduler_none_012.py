# policy_key: scheduler_none_012
# reasoning_effort: none
# model: gpt-5.2-2025-12-11
# llm_cost: 0.035951
# generation_seconds: 36.88
# generated_at: 2026-03-12T21:29:16.380665
@register_scheduler_init(key="scheduler_none_012")
def scheduler_none_012_init(s):
    """Priority-aware FIFO with small, safe improvements over naive.

    Goals (incremental vs naive):
      1) Order admission by priority (QUERY > INTERACTIVE > BATCH), while keeping FIFO within each class.
      2) Avoid "give everything to one op" by using per-priority CPU/RAM caps (leave headroom for future arrivals).
      3) Basic OOM backoff: on failure, increase RAM guess for that pipeline and retry later.
      4) Gentle preemption: if a high-priority op can't fit, suspend one low-priority running container to make room.
         (Keeps churn low: at most one suspension per tick.)
    """
    # Priority ordering: smaller index => higher priority in our internal queues
    s._prio_order = [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]
    s._prio_rank = {p: i for i, p in enumerate(s._prio_order)}

    # Per-priority queue (FIFO)
    s.waiting_queues = {p: [] for p in s._prio_order}

    # Track per-pipeline RAM guess (start small-ish; increase on OOM)
    s.pipeline_ram_guess = {}  # pipeline_id -> ram

    # Track per-pipeline CPU share target (fraction of pool max); cap prevents monopolization
    # Keep these conservative: they should improve tail latency by reducing interference.
    s.cpu_frac_by_prio = {
        Priority.QUERY: 0.75,
        Priority.INTERACTIVE: 0.60,
        Priority.BATCH_PIPELINE: 0.40,
    }

    # Similarly cap RAM fraction per assignment, but allow query to take more.
    s.ram_frac_by_prio = {
        Priority.QUERY: 0.75,
        Priority.INTERACTIVE: 0.60,
        Priority.BATCH_PIPELINE: 0.50,
    }

    # Backoff factor for OOM-ish failures
    s.oom_backoff_factor = 1.6

    # Minimal allocation to avoid assigning "0" resources
    s.min_cpu = 0.1
    s.min_ram = 0.1


def _get_prio_rank(s, prio):
    return s._prio_rank.get(prio, len(s._prio_order))


def _enqueue_pipeline(s, pipeline):
    prio = pipeline.priority
    if prio not in s.waiting_queues:
        # Unknown priority class: treat as lowest
        s.waiting_queues.setdefault(prio, [])
        s._prio_rank.setdefault(prio, len(s._prio_order))
    s.waiting_queues[prio].append(pipeline)


def _pop_next_runnable_op(s, pipeline):
    status = pipeline.runtime_status()
    # Drop completed pipelines
    if status.is_pipeline_successful():
        return None

    # We do allow retry of FAILED ops (ASSIGNABLE_STATES likely includes FAILED) but we
    # apply RAM backoff when we see failure results.
    op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
    if not op_list:
        return None
    return op_list


def _compute_alloc(s, pool, pipeline, prio):
    # Allocate a capped share of pool, bounded by current availability.
    cpu_cap = max(s.min_cpu, pool.max_cpu_pool * s.cpu_frac_by_prio.get(prio, 0.4))
    ram_cap = max(s.min_ram, pool.max_ram_pool * s.ram_frac_by_prio.get(prio, 0.5))

    cpu = min(pool.avail_cpu_pool, cpu_cap)
    ram_avail_capped = min(pool.avail_ram_pool, ram_cap)

    # Apply per-pipeline RAM guess (if any) to avoid repeated OOM loops; never exceed capped availability.
    guess = s.pipeline_ram_guess.get(pipeline.pipeline_id, None)
    if guess is None:
        ram = ram_avail_capped
    else:
        ram = min(ram_avail_capped, max(s.min_ram, guess))

    # Ensure >0 allocations
    cpu = max(s.min_cpu, cpu)
    ram = max(s.min_ram, ram)
    return cpu, ram


def _collect_running_containers_by_pool_and_prio(results):
    # Best-effort view of currently running containers based on last-seen results.
    # We keep only the latest observation per container_id.
    latest = {}
    for r in results:
        if getattr(r, "container_id", None) is None:
            continue
        latest[r.container_id] = r

    running = {}  # (pool_id) -> list of results for containers we consider running
    for r in latest.values():
        # Treat entries without failure as potentially running/active. We don't have explicit RUNNING state here.
        # Also ignore completed containers if simulator reports them only via results (unknown); safe to include.
        running.setdefault(r.pool_id, []).append(r)
    return running


@register_scheduler(key="scheduler_none_012")
def scheduler_none_012(s, results, pipelines):
    """
    Priority-aware scheduler with caps, OOM backoff, and gentle preemption.

    Decision flow:
      - Ingest new pipelines into per-priority FIFO queues.
      - Update RAM guesses when failures are observed (increase on OOM-ish errors).
      - For each pool, try to schedule highest-priority runnable op.
      - If it doesn't fit, optionally suspend one low-priority container to make headroom.
    """
    # Enqueue new arrivals
    for p in pipelines:
        _enqueue_pipeline(s, p)

    # Update RAM guesses based on failures
    # Heuristic: if failed() and error text mentions OOM/memory, treat as RAM underprovision.
    for r in results:
        try:
            is_failed = r.failed()
        except Exception:
            is_failed = bool(getattr(r, "error", None))
        if not is_failed:
            continue

        # Identify pipeline and error cause
        pipeline_id = getattr(r, "pipeline_id", None)
        # Some sims may not expose pipeline_id on result; fallback by scanning ops (if they carry pipeline_id)
        if pipeline_id is None:
            pipeline_id = None
            if getattr(r, "ops", None):
                op0 = r.ops[0]
                pipeline_id = getattr(op0, "pipeline_id", None)

        if pipeline_id is None:
            continue

        err = str(getattr(r, "error", "")).lower()
        looks_like_oom = ("oom" in err) or ("out of memory" in err) or ("memory" in err) or ("killed" in err)

        if looks_like_oom:
            prev = s.pipeline_ram_guess.get(pipeline_id, None)
            # If we know what we allocated, scale that; else start with a small bump.
            base = getattr(r, "ram", None)
            if base is None:
                base = prev if prev is not None else s.min_ram
            new_guess = max(s.min_ram, float(base) * s.oom_backoff_factor)
            if prev is None or new_guess > prev:
                s.pipeline_ram_guess[pipeline_id] = new_guess

    # Early exit if nothing changed
    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    # Snapshot of "running" containers for preemption choices
    running_by_pool = _collect_running_containers_by_pool_and_prio(results)

    # Helper to pick a victim container in a pool: lowest priority first, then largest resource footprint.
    def pick_victim(pool_id):
        candidates = running_by_pool.get(pool_id, [])
        if not candidates:
            return None
        # Only consider victims that are not high priority
        # Prefer suspending BATCH first, then INTERACTIVE; never suspend QUERY for this simple policy.
        def victim_key(r):
            pr = getattr(r, "priority", Priority.BATCH_PIPELINE)
            rank = _get_prio_rank(s, pr)
            # Higher rank => lower priority => better victim; we invert by using rank directly.
            cpu = float(getattr(r, "cpu", 0.0) or 0.0)
            ram = float(getattr(r, "ram", 0.0) or 0.0)
            return (rank, cpu + ram)

        # Filter out QUERY victims
        filtered = [r for r in candidates if getattr(r, "priority", None) != Priority.QUERY]
        if not filtered:
            return None
        # Choose lowest priority (largest rank) and biggest footprint among them
        filtered.sort(key=victim_key, reverse=True)
        return filtered[0]

    # Try to schedule at most one op per pool per tick (small incremental improvement over naive)
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        if pool.avail_cpu_pool <= 0 or pool.avail_ram_pool <= 0:
            continue

        # Find next runnable pipeline/op in priority order, keeping FIFO within each queue.
        chosen_pipeline = None
        chosen_op_list = None

        # We'll rotate through queues, but preserve ordering by scanning and requeuing.
        for prio in sorted(s.waiting_queues.keys(), key=lambda p: _get_prio_rank(s, p)):
            if not s.waiting_queues[prio]:
                continue

            # Scan up to N from front to find one with a runnable op, requeue the rest in order.
            n = len(s.waiting_queues[prio])
            requeue = []
            found = None
            found_ops = None
            for _ in range(n):
                pl = s.waiting_queues[prio].pop(0)
                ops = _pop_next_runnable_op(s, pl)
                if ops is not None and found is None:
                    found = pl
                    found_ops = ops
                requeue.append(pl)
            # Put them back in the same FIFO order
            s.waiting_queues[prio].extend(requeue)

            if found is not None:
                chosen_pipeline = found
                chosen_op_list = found_ops
                break

        if chosen_pipeline is None:
            continue

        prio = chosen_pipeline.priority
        cpu, ram = _compute_alloc(s, pool, chosen_pipeline, prio)

        # If it fits, assign immediately
        if cpu <= pool.avail_cpu_pool and ram <= pool.avail_ram_pool:
            assignments.append(
                Assignment(
                    ops=chosen_op_list,
                    cpu=cpu,
                    ram=ram,
                    priority=prio,
                    pool_id=pool_id,
                    pipeline_id=chosen_pipeline.pipeline_id,
                )
            )
            continue

        # If it doesn't fit and it's high priority, try gentle preemption (one victim)
        if prio in (Priority.QUERY, Priority.INTERACTIVE):
            victim = pick_victim(pool_id)
            if victim is not None and len(suspensions) == 0:
                # Suspend one victim to free headroom; schedule will happen next tick with updated avail.
                suspensions.append(Suspend(victim.container_id, victim.pool_id))

    return suspensions, assignments
