# policy_key: scheduler_est_046
# reasoning_effort: high
# model: gpt-5.2-2025-12-11
# llm_cost: 0.112286
# generation_seconds: 125.95
# generated_at: 2026-03-31T19:48:36.226360
@register_scheduler_init(key="scheduler_est_046")
def scheduler_est_046_init(s):
    """Priority-aware, estimate-based sizing scheduler.

    Small, "obvious" improvements over naive FIFO:
      1) Maintain separate FIFO queues per priority (QUERY > INTERACTIVE > BATCH).
      2) Right-size RAM per op using op.estimate.mem_peak_gb with a safety factor.
      3) Cap per-op CPU to avoid a single op consuming an entire pool (improves latency under contention).
      4) Keep headroom for high-priority work by reserving a fraction of each pool when high-priority is waiting,
         and avoid running batch on pool 0 while high-priority is queued (if multiple pools exist).
      5) On failures, pessimistically increase the op's RAM multiplier and retry (instead of dropping the pipeline).
    """
    s.waiting_by_pri = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }

    # Per-op adaptive memory sizing based on prior failures/successes.
    s.op_mem_mult = {}        # op_key -> float
    s.op_fail_count = {}      # op_key -> int
    s.op_give_up = set()      # op_key -> permanently stop retrying after too many failures

    # Coarse "age" tracking to reduce indefinite starvation for batch (best-effort).
    s.pipeline_first_seen_tick = {}  # pipeline_id -> tick
    s.tick = 0


@register_scheduler(key="scheduler_est_046")
def scheduler_est_046(s, results, pipelines):
    """
    Scheduler step:
      - Ingest new pipelines into per-priority queues.
      - Update per-op memory multipliers based on execution results.
      - For each pool, repeatedly assign ready ops while resources remain:
          * Prefer high priority ops.
          * Enforce headroom reservation when high priority is waiting.
          * Size RAM from estimates (with adaptive multiplier) and cap CPU per op.
    """
    s.tick += 1

    def _norm_priority(p):
        if p in (Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE):
            return p
        return Priority.BATCH_PIPELINE

    def _op_key(op):
        # Try common stable identifiers first; fall back to Python object id.
        for attr in ("op_id", "operator_id", "uid", "id", "name"):
            if hasattr(op, attr):
                try:
                    val = getattr(op, attr)
                    return f"{attr}:{val}"
                except Exception:
                    pass
        return f"pyid:{id(op)}"

    def _est_mem_gb(op):
        # Default to 1GB if estimate absent/unusable.
        try:
            est = getattr(getattr(op, "estimate", None), "mem_peak_gb", None)
            if est is None:
                return 1.0
            est = float(est)
            return 1.0 if est <= 0 else est
        except Exception:
            return 1.0

    def _has_ready_op(pipeline):
        status = pipeline.runtime_status()
        if status.is_pipeline_successful():
            return False
        op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
        return bool(op_list)

    def _get_ready_op(pipeline):
        status = pipeline.runtime_status()
        if status.is_pipeline_successful():
            return None
        op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
        return op_list[0] if op_list else None

    def _cpu_cap_for_priority(priority, pool):
        # Cap per-op CPU to avoid one op monopolizing the pool.
        max_cpu = float(pool.max_cpu_pool)
        if priority in (Priority.QUERY, Priority.INTERACTIVE):
            return max(1.0, 0.50 * max_cpu)
        return max(1.0, 0.25 * max_cpu)

    def _min_ram_floor_gb(priority):
        # Small floor avoids absurdly tiny allocations.
        if priority in (Priority.QUERY, Priority.INTERACTIVE):
            return 0.5
        return 0.5

    def _reserve_fracs(pool_id, high_waiting):
        # Keep some capacity for high priority if it's queued.
        if not high_waiting:
            return 0.0, 0.0
        # If multiple pools exist, treat pool 0 as "latency pool".
        if s.executor.num_pools > 1 and pool_id == 0:
            return 0.40, 0.40
        return 0.20, 0.20

    def _batch_starvation_override(pipeline):
        # Best-effort: if a batch pipeline has existed for a long time, allow it to bypass reservation
        # (but only on non-zero pools to protect latency pool).
        try:
            first = s.pipeline_first_seen_tick.get(pipeline.pipeline_id, s.tick)
            age = s.tick - first
            return age >= 80
        except Exception:
            return False

    # Ingest new pipelines.
    for p in pipelines:
        pri = _norm_priority(p.priority)
        s.waiting_by_pri[pri].append(p)
        if p.pipeline_id not in s.pipeline_first_seen_tick:
            s.pipeline_first_seen_tick[p.pipeline_id] = s.tick

    # Update adaptive memory multipliers from results.
    for r in results:
        try:
            ops = getattr(r, "ops", []) or []
        except Exception:
            ops = []

        if getattr(r, "failed", lambda: False)():
            for op in ops:
                k = _op_key(op)
                s.op_fail_count[k] = s.op_fail_count.get(k, 0) + 1
                # Pessimistically bump memory multiplier on any failure (OOM or otherwise).
                cur = s.op_mem_mult.get(k, 1.25)
                s.op_mem_mult[k] = min(cur * 1.5, 16.0)

                # Give up after too many retries to avoid infinite loops.
                # (Keeping it modest; the simulator doesn't distinguish OOM vs. user-code failure.)
                if s.op_fail_count[k] >= 6:
                    s.op_give_up.add(k)
        else:
            # On success, gently decay multiplier towards 1.0.
            for op in ops:
                k = _op_key(op)
                if k in s.op_mem_mult:
                    s.op_mem_mult[k] = max(1.0, s.op_mem_mult[k] * 0.95)

    # Early exit if nothing changed.
    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []
    scheduled_pipelines_this_tick = set()

    # Determine if any high-priority work is actually READY (parents complete).
    high_waiting = False
    for pri in (Priority.QUERY, Priority.INTERACTIVE):
        for pl in s.waiting_by_pri.get(pri, []):
            if _has_ready_op(pl):
                high_waiting = True
                break
        if high_waiting:
            break

    # Main scheduling: per pool, pack multiple ops with resource-aware sizing.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)

        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # If multiple pools exist, avoid running batch on pool 0 while high priority is waiting.
        allow_batch_on_pool0 = not (s.executor.num_pools > 1 and pool_id == 0 and high_waiting)

        # Reservation for high-priority headroom.
        res_cpu_frac, res_ram_frac = _reserve_fracs(pool_id, high_waiting)
        reserv_cpu = res_cpu_frac * float(pool.max_cpu_pool)
        reserv_ram = res_ram_frac * float(pool.max_ram_pool)

        # We'll attempt multiple assignments per pool per tick.
        max_assignments = 16
        made_progress = True

        while made_progress and max_assignments > 0 and avail_cpu > 0 and avail_ram > 0:
            made_progress = False
            max_assignments -= 1

            # Compute "low-priority budget" if reserving for high-priority.
            low_budget_cpu = avail_cpu
            low_budget_ram = avail_ram
            if high_waiting:
                low_budget_cpu = max(0.0, avail_cpu - reserv_cpu)
                low_budget_ram = max(0.0, avail_ram - reserv_ram)

            # Choose a priority order per pool: protect latency when high-priority exists,
            # otherwise allow batch to use background pools more aggressively.
            if high_waiting:
                prio_order = [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]
            else:
                if s.executor.num_pools > 1 and pool_id != 0:
                    prio_order = [Priority.BATCH_PIPELINE, Priority.INTERACTIVE, Priority.QUERY]
                else:
                    prio_order = [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]

            chosen = None  # (pipeline, op, cpu_alloc, ram_alloc)

            for pri in prio_order:
                if pri == Priority.BATCH_PIPELINE and not allow_batch_on_pool0:
                    continue

                q = s.waiting_by_pri.get(pri, [])
                if not q:
                    continue

                # Scan the queue once, rotating to preserve FIFO-ish behavior within each priority.
                qlen = len(q)
                for _ in range(qlen):
                    pl = q.pop(0)

                    # Skip if we already scheduled this pipeline this tick (simple anti-hogging).
                    if pl.pipeline_id in scheduled_pipelines_this_tick:
                        q.append(pl)
                        continue

                    status = pl.runtime_status()
                    if status.is_pipeline_successful():
                        # Drop completed pipelines from the queue.
                        continue

                    op = _get_ready_op(pl)
                    if op is None:
                        # Not ready yet (parents incomplete / nothing assignable) => keep waiting.
                        q.append(pl)
                        continue

                    k = _op_key(op)
                    if k in s.op_give_up:
                        # Stop retrying this op; drop pipeline from queues to avoid endless cycling.
                        # (In a real system we'd mark the pipeline failed explicitly.)
                        continue

                    # Determine budgets depending on priority and starvation override.
                    if pri in (Priority.QUERY, Priority.INTERACTIVE):
                        budget_cpu = avail_cpu
                        budget_ram = avail_ram
                    else:
                        # Batch normally uses only the non-reserved budget while high-priority is queued.
                        # If it's been waiting "too long", allow it to bypass reservation on non-latency pools.
                        if (high_waiting and pool_id != 0 and _batch_starvation_override(pl)):
                            budget_cpu = avail_cpu
                            budget_ram = avail_ram
                        else:
                            budget_cpu = low_budget_cpu
                            budget_ram = low_budget_ram

                    if budget_cpu < 1.0 or budget_ram <= 0:
                        q.append(pl)
                        continue

                    # RAM sizing: estimate * safety * adaptive multiplier.
                    est = _est_mem_gb(op)
                    mult = s.op_mem_mult.get(k, 1.25)
                    safety = 1.10 if pri in (Priority.QUERY, Priority.INTERACTIVE) else 1.20
                    ram_need = max(_min_ram_floor_gb(pri), est * mult * safety)

                    if ram_need > budget_ram:
                        # Can't fit now; keep pipeline in queue.
                        q.append(pl)
                        continue

                    # CPU sizing: capped per op; high priority gets a higher cap.
                    cpu_cap = _cpu_cap_for_priority(pri, pool)
                    cpu_alloc = min(budget_cpu, cpu_cap)
                    cpu_alloc = max(1.0, cpu_alloc)

                    ram_alloc = ram_need  # allocate what we think we need (keeps headroom for concurrency)
                    chosen = (pl, op, cpu_alloc, ram_alloc)

                    # Requeue the pipeline after scheduling (it may have more ops later).
                    q.append(pl)
                    break

                if chosen is not None:
                    break

            if chosen is None:
                break

            pl, op, cpu_alloc, ram_alloc = chosen

            assignments.append(
                Assignment(
                    ops=[op],
                    cpu=cpu_alloc,
                    ram=ram_alloc,
                    priority=pl.priority,
                    pool_id=pool_id,
                    pipeline_id=pl.pipeline_id,
                )
            )
            scheduled_pipelines_this_tick.add(pl.pipeline_id)

            # Update local remaining resources for this pool for additional packing.
            avail_cpu = max(0.0, avail_cpu - cpu_alloc)
            avail_ram = max(0.0, avail_ram - ram_alloc)
            made_progress = True

    return suspensions, assignments
