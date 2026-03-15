# policy_key: scheduler_medium_045
# reasoning_effort: medium
# model: gpt-5.2-2025-12-11
# llm_cost: 0.077825
# generation_seconds: 78.22
# generated_at: 2026-03-14T03:19:33.045690
@register_scheduler_init(key="scheduler_medium_045")
def scheduler_medium_045_init(s):
    """Priority-aware, reserve-based FIFO with simple OOM-driven RAM tuning.

    Improvements over naive FIFO:
      - Separate queues by priority (QUERY > INTERACTIVE > BATCH).
      - Soft resource reservations: keep headroom for high-priority work while it is pending.
      - Basic aging for batch: very old batch pipelines can jump ahead of normal batch.
      - On OOM-like failures, retry the same operator with increased RAM (exponential backoff).

    Notes:
      - We avoid preemption because the public template does not expose a reliable way to
        enumerate currently-running containers to suspend.
      - We schedule up to a small number of assignments per pool per tick to reduce churn
        and keep latency-friendly behavior.
    """
    s.tick = 0

    # Per-priority waiting queues (store Pipeline objects)
    s.queues = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }

    # Bookkeeping for fairness/aging
    s.first_seen_tick = {}  # pipeline_id -> tick first observed

    # Per-operator OOM adaptation:
    # factor multiplies guessed operator minimum RAM on retry after OOM
    s.op_ram_factor = {}    # (pipeline_id, op_key) -> float
    s.op_oom_retries = {}   # (pipeline_id, op_key) -> int

    # Tunables (kept modest to stay "small improvement" and robust)
    s.reserve_frac_cpu = 0.20
    s.reserve_frac_ram = 0.20
    s.batch_aging_threshold_ticks = 25
    s.batch_aging_scan_limit = 8

    # Caps to avoid one task grabbing an entire pool by default
    s.query_cpu_cap_frac = 0.50
    s.interactive_cpu_cap_frac = 0.75
    s.batch_cpu_cap_frac = 0.50

    s.query_ram_cap_frac = 0.80
    s.interactive_ram_cap_frac = 0.80
    s.batch_ram_cap_frac = 0.60

    # Scheduling granularity per tick
    s.max_assignments_per_pool_per_tick = 2

    # OOM backoff
    s.oom_backoff_multiplier = 2.0
    s.oom_backoff_max_factor = 16.0


@register_scheduler(key="scheduler_medium_045")
def scheduler_medium_045(s, results, pipelines):
    """Priority-aware scheduler with headroom reservation and OOM-based RAM backoff."""
    s.tick += 1

    def _is_oom_error(err) -> bool:
        if err is None:
            return False
        msg = str(err).lower()
        return ("oom" in msg) or ("out of memory" in msg) or ("cuda oom" in msg) or ("memoryerror" in msg)

    def _op_key(op):
        # Try stable identifiers first; fall back to object identity.
        return getattr(op, "op_id", getattr(op, "operator_id", getattr(op, "id", id(op))))

    def _op_min_ram_guess(op, pool):
        # Attempt to find a declared minimum RAM on the operator; otherwise guess conservatively.
        for attr in ("min_ram", "ram_min", "min_memory", "memory_min", "min_mem", "mem_min", "ram"):
            val = getattr(op, attr, None)
            if isinstance(val, (int, float)) and val > 0:
                return float(val)
        # Fallback guess: 10% of pool RAM, but at least 1 unit.
        return max(1.0, float(pool.max_ram_pool) * 0.10)

    def _priority_rank(pri):
        # Lower is higher priority
        if pri == Priority.QUERY:
            return 0
        if pri == Priority.INTERACTIVE:
            return 1
        return 2

    def _pool_score(pool):
        # Favor pools with more normalized headroom
        cpu_part = 0.0 if pool.max_cpu_pool <= 0 else (pool.avail_cpu_pool / pool.max_cpu_pool)
        ram_part = 0.0 if pool.max_ram_pool <= 0 else (pool.avail_ram_pool / pool.max_ram_pool)
        return cpu_part + ram_part

    def _has_high_priority_pending():
        return bool(s.queues[Priority.QUERY]) or bool(s.queues[Priority.INTERACTIVE])

    def _cleanup_and_get_next_op(pipeline):
        status = pipeline.runtime_status()
        if status.is_pipeline_successful():
            return None, True  # done
        # If failures exist, we still allow retries (ASSIGNABLE_STATES includes FAILED)
        op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
        if not op_list:
            return None, False
        return op_list, False

    def _compute_caps(pool, pri):
        if pri == Priority.QUERY:
            cpu_cap = max(1.0, float(pool.max_cpu_pool) * s.query_cpu_cap_frac)
            ram_cap = max(1.0, float(pool.max_ram_pool) * s.query_ram_cap_frac)
        elif pri == Priority.INTERACTIVE:
            cpu_cap = max(1.0, float(pool.max_cpu_pool) * s.interactive_cpu_cap_frac)
            ram_cap = max(1.0, float(pool.max_ram_pool) * s.interactive_ram_cap_frac)
        else:
            cpu_cap = max(1.0, float(pool.max_cpu_pool) * s.batch_cpu_cap_frac)
            ram_cap = max(1.0, float(pool.max_ram_pool) * s.batch_ram_cap_frac)
        return cpu_cap, ram_cap

    def _effective_available_for_priority(pool, avail_cpu, avail_ram, pri):
        # While high-priority work exists, keep a reserve by restricting batch usage.
        if pri == Priority.BATCH_PIPELINE and _has_high_priority_pending():
            reserve_cpu = float(pool.max_cpu_pool) * s.reserve_frac_cpu
            reserve_ram = float(pool.max_ram_pool) * s.reserve_frac_ram
            return max(0.0, avail_cpu - reserve_cpu), max(0.0, avail_ram - reserve_ram)
        return avail_cpu, avail_ram

    def _size_request(pool, pipeline, op, avail_cpu, avail_ram):
        pri = pipeline.priority
        cpu_cap, ram_cap = _compute_caps(pool, pri)

        # CPU: cap allocation to keep more responsive sharing (especially under contention).
        cpu_req = min(float(avail_cpu), float(cpu_cap))
        if cpu_req <= 0:
            return 0.0, 0.0

        # RAM: start from guessed min; apply OOM backoff factor if any.
        base_min_ram = _op_min_ram_guess(op, pool)
        key = (pipeline.pipeline_id, _op_key(op))
        factor = float(s.op_ram_factor.get(key, 1.0))
        ram_target = base_min_ram * factor * 1.10  # small slack
        ram_req = min(float(avail_ram), float(ram_cap), float(ram_target))

        # If we can't even fit base minimum (guessed), don't schedule now.
        if ram_req < min(base_min_ram, float(avail_ram)):
            return 0.0, 0.0
        if ram_req <= 0:
            return 0.0, 0.0

        return cpu_req, ram_req

    def _pick_pipeline_from_queue(queue):
        # Pop-left semantics with requeue handled by caller.
        if not queue:
            return None
        return queue.pop(0)

    def _find_aged_batch_pipeline():
        # Scan a limited prefix of the batch queue for an "aged" pipeline.
        q = s.queues[Priority.BATCH_PIPELINE]
        if not q:
            return None, None
        limit = min(len(q), int(s.batch_aging_scan_limit))
        for i in range(limit):
            p = q[i]
            first = s.first_seen_tick.get(p.pipeline_id, s.tick)
            if (s.tick - first) >= s.batch_aging_threshold_ticks:
                # Remove and return it
                return q.pop(i), i
        return None, None

    # Ingest new pipelines
    for p in pipelines:
        if p.pipeline_id not in s.first_seen_tick:
            s.first_seen_tick[p.pipeline_id] = s.tick
        s.queues[p.priority].append(p)

    # Update OOM backoff state from results
    for r in results:
        if r is None:
            continue
        if getattr(r, "failed", None) and r.failed() and _is_oom_error(getattr(r, "error", None)):
            for op in getattr(r, "ops", []) or []:
                key = (getattr(r, "pipeline_id", None), _op_key(op))
                # If pipeline_id isn't on result, fall back to op-only key scoped by container info
                if key[0] is None:
                    key = (("container", getattr(r, "container_id", None), getattr(r, "pool_id", None)), _op_key(op))
                prev = float(s.op_ram_factor.get(key, 1.0))
                new = min(prev * s.oom_backoff_multiplier, s.oom_backoff_max_factor)
                s.op_ram_factor[key] = new
                s.op_oom_retries[key] = int(s.op_oom_retries.get(key, 0)) + 1

    # Early exit if no changes that affect decisions
    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    # Consider pools in descending headroom order (helps latency: place high-priority where it fits best).
    pool_ids = list(range(s.executor.num_pools))
    pool_ids.sort(key=lambda pid: _pool_score(s.executor.pools[pid]), reverse=True)

    for pool_id in pool_ids:
        pool = s.executor.pools[pool_id]
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)
        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        assigned_here = 0
        while assigned_here < int(s.max_assignments_per_pool_per_tick):
            # Priority selection order with batch aging:
            # QUERY -> INTERACTIVE -> (aged BATCH) -> BATCH
            pipeline = None
            from_priority = None

            if s.queues[Priority.QUERY]:
                pipeline = _pick_pipeline_from_queue(s.queues[Priority.QUERY])
                from_priority = Priority.QUERY
            elif s.queues[Priority.INTERACTIVE]:
                pipeline = _pick_pipeline_from_queue(s.queues[Priority.INTERACTIVE])
                from_priority = Priority.INTERACTIVE
            else:
                aged, _ = _find_aged_batch_pipeline()
                if aged is not None:
                    pipeline = aged
                    from_priority = Priority.BATCH_PIPELINE
                elif s.queues[Priority.BATCH_PIPELINE]:
                    pipeline = _pick_pipeline_from_queue(s.queues[Priority.BATCH_PIPELINE])
                    from_priority = Priority.BATCH_PIPELINE

            if pipeline is None:
                break

            # Drop completed pipelines; otherwise find a runnable operator
            op_list, done = _cleanup_and_get_next_op(pipeline)
            if done:
                # Do not requeue
                continue

            if not op_list:
                # Not runnable yet; requeue at end of same priority
                s.queues[from_priority].append(pipeline)
                # Avoid spinning if nothing is runnable across queues; let next tick advance.
                break

            # Respect headroom reservation for batch when high-priority is pending
            eff_cpu, eff_ram = _effective_available_for_priority(pool, avail_cpu, avail_ram, pipeline.priority)
            if eff_cpu <= 0 or eff_ram <= 0:
                # Can't schedule this pipeline on this pool now; requeue it and stop on this pool
                s.queues[from_priority].append(pipeline)
                break

            op = op_list[0]
            cpu_req, ram_req = _size_request(pool, pipeline, op, eff_cpu, eff_ram)
            if cpu_req <= 0 or ram_req <= 0:
                # Can't fit now; requeue and stop on this pool to avoid tight loops
                s.queues[from_priority].append(pipeline)
                break

            # Create assignment (one op per container)
            assignments.append(
                Assignment(
                    ops=op_list,
                    cpu=cpu_req,
                    ram=ram_req,
                    priority=pipeline.priority,
                    pool_id=pool_id,
                    pipeline_id=pipeline.pipeline_id,
                )
            )

            # Requeue pipeline for subsequent operators
            s.queues[from_priority].append(pipeline)

            avail_cpu -= cpu_req
            avail_ram -= ram_req
            assigned_here += 1

            if avail_cpu <= 0 or avail_ram <= 0:
                break

    return suspensions, assignments
