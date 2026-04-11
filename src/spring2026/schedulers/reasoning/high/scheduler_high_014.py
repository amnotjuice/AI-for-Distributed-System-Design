# policy_key: scheduler_high_014
# reasoning_effort: high
# model: gpt-5.2-2025-12-11
# llm_cost: 0.083691
# generation_seconds: 157.35
# generated_at: 2026-03-12T22:55:15.046960
@register_scheduler_init(key="scheduler_high_014")
def scheduler_high_014_init(s):
    """Priority-aware, latency-biased scheduler (small improvements over naive FIFO).

    Improvements vs. the naive example:
      - Separate per-priority queues (QUERY > INTERACTIVE > BATCH) to reduce head-of-line blocking.
      - Avoid "give the whole pool to one op" by capping per-assignment CPU/RAM by priority,
        enabling concurrency among interactive tasks.
      - OOM-aware retry: on likely OOM failures, retry the same op with increased RAM (bounded),
        instead of dropping the whole pipeline.
      - Light reservation: when scheduling BATCH in a shared pool, keep some headroom for higher priority.
      - Optional "dedicated" pool behavior when multiple pools exist: pool 0 prefers QUERY/INTERACTIVE.
    """
    from collections import deque

    # Per-priority pipeline queues (store Pipeline objects)
    s.queues = {
        Priority.QUERY: deque(),
        Priority.INTERACTIVE: deque(),
        Priority.BATCH_PIPELINE: deque(),
    }

    # Track which pipeline_ids are currently enqueued (prevents duplicates).
    s.enqueued_ids = set()

    # Pipelines we should not schedule again (non-OOM failure or too many OOMs).
    s.dropped_pipeline_ids = set()

    # Per-(pipeline_id, op_key) RAM hint for retries (mainly used after OOM).
    s.op_ram_hint = {}

    # Per-(pipeline_id, op_key) OOM retry counters.
    s.op_oom_retries = {}

    # Tuning knobs (kept simple; can be iterated on after we see sim results).
    s.max_assignments_per_pool = 6
    s.max_oom_retries = 3
    s.oom_backoff = 2.0  # multiply RAM by this factor on OOM
    s.ram_safety_mult = 1.15  # small safety margin on top of hints

    # Per-priority caps (fractions of pool capacity per assignment).
    # These caps increase concurrency and reduce HO blocking for interactive work.
    s.cpu_cap_frac = {
        Priority.QUERY: 0.60,
        Priority.INTERACTIVE: 0.45,
        Priority.BATCH_PIPELINE: 0.35,
    }
    s.ram_cap_frac = {
        Priority.QUERY: 0.50,
        Priority.INTERACTIVE: 0.40,
        Priority.BATCH_PIPELINE: 0.35,
    }

    # Headroom reservation when placing lower priority work in shared pools
    # (keeps resources available for bursts of QUERY/INTERACTIVE).
    s.reserve_hp_cpu_frac = 0.20
    s.reserve_hp_ram_frac = 0.20

    # If multiple pools exist, treat pool 0 as "interactive-preferred".
    s.use_dedicated_interactive_pool0 = True

    # If the dedicated pool is idle of high-priority work, allow batch to use it to avoid idling.
    s.allow_batch_on_pool0_if_no_hp_waiting = True

    # Minimum quanta (avoid 0 allocations in small pools / fractional scenarios).
    s.min_cpu = 0.5
    s.min_ram = 0.5


def _scheduler_high_014_is_oom_error(err) -> bool:
    try:
        msg = str(err).lower()
    except Exception:
        return False
    return ("oom" in msg) or ("out of memory" in msg) or ("memory" in msg and "alloc" in msg)


def _scheduler_high_014_op_key(op):
    # Best-effort stable key for an operator instance.
    if hasattr(op, "op_id"):
        return getattr(op, "op_id")
    if hasattr(op, "operator_id"):
        return getattr(op, "operator_id")
    if hasattr(op, "id"):
        return getattr(op, "id")
    return id(op)


def _scheduler_high_014_result_pipeline_id(r):
    # ExecutionResult may or may not expose pipeline_id directly; infer from ops if needed.
    if hasattr(r, "pipeline_id"):
        return getattr(r, "pipeline_id")
    ops = getattr(r, "ops", None) or []
    if ops:
        op0 = ops[0]
        if hasattr(op0, "pipeline_id"):
            return getattr(op0, "pipeline_id")
        if hasattr(op0, "pipeline") and hasattr(getattr(op0, "pipeline"), "pipeline_id"):
            return getattr(getattr(op0, "pipeline"), "pipeline_id")
    return None


def _scheduler_high_014_enqueue(s, pipeline):
    pid = pipeline.pipeline_id
    if pid in s.dropped_pipeline_ids:
        return
    if pid in s.enqueued_ids:
        return
    s.queues[pipeline.priority].append(pipeline)
    s.enqueued_ids.add(pid)


def _scheduler_high_014_pop_ready_pipeline(s, prio, assigned_this_tick):
    """Round-robin within a priority queue until we find a pipeline with a ready assignable op.

    Returns:
        (pipeline, op) or (None, None)
    """
    q = s.queues[prio]
    if not q:
        return None, None

    scanned = 0
    qlen = len(q)

    while scanned < qlen and q:
        pipeline = q.popleft()
        s.enqueued_ids.discard(pipeline.pipeline_id)
        scanned += 1

        pid = pipeline.pipeline_id
        if pid in s.dropped_pipeline_ids:
            continue
        if pid in assigned_this_tick:
            # Defer this pipeline to keep fairness within a tick.
            _scheduler_high_014_enqueue(s, pipeline)
            continue

        status = pipeline.runtime_status()
        if status.is_pipeline_successful():
            continue

        # Only schedule ops whose parents are complete.
        op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        if not op_list:
            # Blocked; keep it in queue.
            _scheduler_high_014_enqueue(s, pipeline)
            continue

        return pipeline, op_list[0]

    return None, None


def _scheduler_high_014_compute_request(s, pool, pipeline_id, op, prio, eff_cpu, eff_ram):
    """Compute cpu/ram request for an op, using priority caps + OOM-derived RAM hints."""
    opk = _scheduler_high_014_op_key(op)
    hint = s.op_ram_hint.get((pipeline_id, opk), None)

    # Priority-based caps to avoid single-op monopolization.
    cpu_cap = max(s.min_cpu, float(pool.max_cpu_pool) * float(s.cpu_cap_frac.get(prio, 0.35)))
    ram_cap = max(s.min_ram, float(pool.max_ram_pool) * float(s.ram_cap_frac.get(prio, 0.35)))

    # For RAM: if we have a hint (e.g., from an OOM retry), use it (with a safety margin),
    # otherwise use the priority-based cap (bounded by the pool cap above).
    if hint is None:
        desired_ram = ram_cap
    else:
        desired_ram = max(ram_cap, float(hint) * float(s.ram_safety_mult))

    # Always respect currently effective available resources.
    cpu = min(float(eff_cpu), float(cpu_cap))
    ram = min(float(eff_ram), float(desired_ram))

    # Avoid near-zero allocations.
    if cpu < s.min_cpu or ram < s.min_ram:
        return 0.0, 0.0

    return cpu, ram


@register_scheduler(key="scheduler_high_014")
def scheduler_high_014(s, results, pipelines):
    """
    Priority-aware, OOM-adaptive, concurrency-friendly scheduler.

    Key behavior:
      - Enqueue new pipelines by priority.
      - Update RAM hints on OOM failures and retry (up to a limit).
      - Schedule across pools, prioritizing QUERY then INTERACTIVE then BATCH.
      - In shared pools, reserve headroom when scheduling BATCH to reduce latency spikes for high priority.
      - If multiple pools exist, pool 0 is treated as interactive-preferred (but can be used for BATCH if no HP waiting).
    """
    suspensions = []
    assignments = []

    # 1) Admit new pipelines.
    for p in pipelines:
        _scheduler_high_014_enqueue(s, p)

    # 2) Learn from results (OOM backoff; drop on non-OOM failures).
    for r in results:
        pid = _scheduler_high_014_result_pipeline_id(r)
        if pid is None:
            continue

        if r.failed():
            if _scheduler_high_014_is_oom_error(getattr(r, "error", None)):
                # Increase RAM hint for each op in the failed container.
                pool = s.executor.pools[r.pool_id]
                max_ram = float(pool.max_ram_pool)
                used_ram = float(getattr(r, "ram", 0.0) or 0.0)
                used_ram = max(s.min_ram, used_ram)

                for op in getattr(r, "ops", []) or []:
                    opk = _scheduler_high_014_op_key(op)
                    key = (pid, opk)

                    # Retry budget per op.
                    s.op_oom_retries[key] = s.op_oom_retries.get(key, 0) + 1
                    if s.op_oom_retries[key] > s.max_oom_retries:
                        s.dropped_pipeline_ids.add(pid)
                        continue

                    prev = float(s.op_ram_hint.get(key, used_ram))
                    bumped = max(prev, used_ram) * float(s.oom_backoff)
                    s.op_ram_hint[key] = min(max_ram, bumped)
            else:
                # Non-OOM failures are treated as non-retriable here (avoid infinite loops).
                s.dropped_pipeline_ids.add(pid)

    # Quick exit if nothing to do.
    if not any(s.queues[p] for p in s.queues):
        return suspensions, assignments

    # 3) Scheduling pass.
    hp_waiting = bool(s.queues[Priority.QUERY] or s.queues[Priority.INTERACTIVE])
    assigned_this_tick = set()

    num_pools = s.executor.num_pools
    dedicated_pool0 = (num_pools > 1) and bool(getattr(s, "use_dedicated_interactive_pool0", True))

    # Simple pool iteration; can be improved later by sorting pools by headroom.
    for pool_id in range(num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)

        if avail_cpu < s.min_cpu or avail_ram < s.min_ram:
            continue

        # Decide which priorities are allowed in this pool.
        if dedicated_pool0 and pool_id == 0:
            allowed_prios = [Priority.QUERY, Priority.INTERACTIVE]
            if getattr(s, "allow_batch_on_pool0_if_no_hp_waiting", True) and not hp_waiting:
                allowed_prios.append(Priority.BATCH_PIPELINE)
        else:
            allowed_prios = [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]

        made = 0
        while made < s.max_assignments_per_pool:
            # Choose next (pipeline, op) by strict priority order among allowed prios.
            chosen = None

            for prio in allowed_prios:
                # Apply lightweight headroom reservation only in shared pools.
                eff_cpu, eff_ram = avail_cpu, avail_ram
                if not (dedicated_pool0 and pool_id == 0):
                    if prio == Priority.BATCH_PIPELINE:
                        eff_cpu = max(0.0, avail_cpu - float(pool.max_cpu_pool) * float(s.reserve_hp_cpu_frac))
                        eff_ram = max(0.0, avail_ram - float(pool.max_ram_pool) * float(s.reserve_hp_ram_frac))

                if eff_cpu < s.min_cpu or eff_ram < s.min_ram:
                    continue

                pipeline, op = _scheduler_high_014_pop_ready_pipeline(s, prio, assigned_this_tick)
                if pipeline is None:
                    continue

                chosen = (pipeline, op, prio, eff_cpu, eff_ram)
                break

            if chosen is None:
                break

            pipeline, op, prio, eff_cpu, eff_ram = chosen
            pid = pipeline.pipeline_id

            # Compute request size; if it doesn't fit, re-enqueue and try next.
            cpu_req, ram_req = _scheduler_high_014_compute_request(
                s, pool, pid, op, prio, eff_cpu, eff_ram
            )
            if cpu_req <= 0.0 or ram_req <= 0.0:
                _scheduler_high_014_enqueue(s, pipeline)
                continue

            if cpu_req > eff_cpu or ram_req > eff_ram:
                # Can't fit under current (possibly reserved) budget; requeue and keep searching.
                _scheduler_high_014_enqueue(s, pipeline)
                continue

            assignments.append(
                Assignment(
                    ops=[op],
                    cpu=cpu_req,
                    ram=ram_req,
                    priority=prio,
                    pool_id=pool_id,
                    pipeline_id=pid,
                )
            )

            assigned_this_tick.add(pid)
            made += 1
            avail_cpu -= cpu_req
            avail_ram -= ram_req

            # Keep pipeline in the system for future ops (if still active).
            status = pipeline.runtime_status()
            if (pid not in s.dropped_pipeline_ids) and (not status.is_pipeline_successful()):
                _scheduler_high_014_enqueue(s, pipeline)

            if avail_cpu < s.min_cpu or avail_ram < s.min_ram:
                break

    return suspensions, assignments
