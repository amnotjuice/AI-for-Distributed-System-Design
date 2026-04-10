# policy_key: scheduler_est_005
# reasoning_effort: low
# model: gpt-5.2-2025-12-11
# llm_cost: 0.038450
# generation_seconds: 48.94
# generated_at: 2026-03-31T23:48:24.940621
@register_scheduler_init(key="scheduler_est_005")
def scheduler_est_005_init(s):
    """
    Priority-aware, estimator-guided FIFO with small, safe improvements over naive FIFO:

    1) Separate per-priority FIFO queues (QUERY/INTERACTIVE ahead of BATCH).
    2) Avoid "give the whole pool to one op": right-size CPU in quanta to improve latency/parallelism.
    3) Use op.estimate.mem_peak_gb as a RAM floor (plus a small safety margin) to reduce OOMs.
    4) Simple OOM backoff: if an op fails, bump its remembered RAM floor for the next retry.
    5) Simple aging: old batch pipelines are gradually promoted to reduce starvation.

    No preemption (kept intentionally minimal/robust given uncertain executor introspection).
    """
    # Per-priority FIFO queues of pipelines
    s.q_query = []
    s.q_interactive = []
    s.q_batch = []

    # Track when we first saw a pipeline to implement simple aging
    s.pipeline_first_seen_ts = {}

    # Track per-(pipeline,op) memory floors learned from failures
    s.op_mem_floor_gb = {}

    # A simple logical clock for aging decisions
    s.ts = 0

    # Tuning knobs (conservative defaults)
    s.mem_safety_margin_gb = 0.5          # add-on to estimator to reduce near-miss OOMs
    s.mem_backoff_multiplier = 1.5        # multiply floor on failures
    s.mem_backoff_add_gb = 1.0            # additive bump on failures
    s.aging_threshold_ticks = 50          # after this, batch is eligible for promotion
    s.aging_promote_every_ticks = 25      # further promotions with time
    s.max_attempts_per_tick_per_pool = 32 # avoid long loops per pool


@register_scheduler(key="scheduler_est_005")
def scheduler_est_005_scheduler(s, results, pipelines):
    """
    Scheduler step:
      - Enqueue new pipelines into per-priority queues.
      - Process results to learn memory floors on failures.
      - For each pool, repeatedly pick the best next runnable op (by priority, then aging),
        size it with RAM floor + CPU quanta, and assign while pool headroom remains.
    """
    s.ts += 1

    def _priority_rank(prio):
        # Lower is "more important" in our internal ranking.
        if prio == Priority.QUERY:
            return 0
        if prio == Priority.INTERACTIVE:
            return 1
        return 2  # Priority.BATCH_PIPELINE and any others

    def _enqueue_pipeline(p):
        if p.pipeline_id not in s.pipeline_first_seen_ts:
            s.pipeline_first_seen_ts[p.pipeline_id] = s.ts
        if p.priority == Priority.QUERY:
            s.q_query.append(p)
        elif p.priority == Priority.INTERACTIVE:
            s.q_interactive.append(p)
        else:
            s.q_batch.append(p)

    def _pipeline_age_ticks(p):
        return s.ts - s.pipeline_first_seen_ts.get(p.pipeline_id, s.ts)

    def _maybe_promote_aged_batch():
        # Promote some batch pipelines to interactive if they've waited long enough.
        # This is a simple starvation-avoidance mechanism.
        if not s.q_batch:
            return
        promoted = []
        keep = []
        for p in s.q_batch:
            age = _pipeline_age_ticks(p)
            if age >= s.aging_threshold_ticks:
                # Further age increases chance of promotion deterministically.
                # (Promotion cadence based on time waited.)
                if ((age - s.aging_threshold_ticks) // max(1, s.aging_promote_every_ticks)) >= 1:
                    promoted.append(p)
                else:
                    keep.append(p)
            else:
                keep.append(p)
        s.q_batch = keep
        # Promoted pipelines keep FIFO order among themselves.
        s.q_interactive.extend(promoted)

    def _op_key(pipeline_id, op):
        # Try to use a stable operator identifier if available; otherwise fall back.
        oid = getattr(op, "operator_id", None)
        if oid is None:
            oid = getattr(op, "op_id", None)
        if oid is None:
            oid = getattr(op, "id", None)
        if oid is None:
            oid = id(op)
        return (pipeline_id, oid)

    def _get_mem_floor_gb(p, op):
        # Start with estimator if present; treat as a floor.
        est = getattr(getattr(op, "estimate", None), "mem_peak_gb", None)
        base = 0.0
        if isinstance(est, (int, float)) and est > 0:
            base = float(est) + float(s.mem_safety_margin_gb)

        # Learned floor (from prior failures) overrides upward.
        k = _op_key(p.pipeline_id, op)
        learned = s.op_mem_floor_gb.get(k, 0.0)
        return max(base, learned, 0.0)

    def _learn_from_results():
        # On failures, bump memory floor for the ops that were in that container.
        for r in results:
            if not r.failed():
                continue
            # Treat any failure as "maybe OOM" for this minimal policy; we only increase RAM floors.
            # If error strings are available, we could narrow this, but we keep it robust.
            for op in getattr(r, "ops", []) or []:
                k = _op_key(getattr(r, "pipeline_id", None) or getattr(op, "pipeline_id", None) or "unknown", op)
                # If we can't attribute pipeline_id reliably, fall back to op identity without pipeline.
                if k[0] == "unknown":
                    k = ("unknown", k[1])

                prev = s.op_mem_floor_gb.get(k, 0.0)
                # Bump from what we just tried, if known, otherwise from previous.
                tried = getattr(r, "ram", None)
                tried_val = float(tried) if isinstance(tried, (int, float)) else 0.0
                start = max(prev, tried_val)
                bumped = max(start * s.mem_backoff_multiplier, start + s.mem_backoff_add_gb, prev + s.mem_backoff_add_gb)
                s.op_mem_floor_gb[k] = bumped

    def _pipeline_done_or_failed(p):
        st = p.runtime_status()
        if st.is_pipeline_successful():
            return True
        # If any op is FAILED, we still allow retries (ASSIGNABLE includes FAILED per provided note).
        # We only drop truly successful pipelines here.
        return False

    def _get_next_runnable_op_from_queue(queue):
        # FIFO scan: return (pipeline, op) for the first pipeline that has an assignable op
        # with parents complete. Keep other pipelines in order.
        kept = []
        chosen = (None, None)
        while queue:
            p = queue.pop(0)
            if _pipeline_done_or_failed(p):
                continue
            st = p.runtime_status()
            op_list = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
            if op_list and chosen == (None, None):
                chosen = (p, op_list[0])
                kept.append(p)  # keep pipeline in queue for subsequent ops
                break
            kept.append(p)

        # Put remaining items back (preserving relative order)
        queue[:0] = kept
        return chosen

    def _queues_in_priority_order():
        # Apply aging promotions before scheduling decisions.
        _maybe_promote_aged_batch()

        # Return a list of (queue, rank) in strict priority order.
        return [(s.q_query, 0), (s.q_interactive, 1), (s.q_batch, 2)]

    def _choose_pool_for_priority(prio):
        # If multiple pools exist, bias:
        # - pool 0 for QUERY/INTERACTIVE
        # - non-zero pools for BATCH (to reduce interference)
        if s.executor.num_pools <= 1:
            return list(range(s.executor.num_pools))
        if prio in (Priority.QUERY, Priority.INTERACTIVE):
            return [0] + [i for i in range(1, s.executor.num_pools)]
        else:
            return [i for i in range(1, s.executor.num_pools)] + [0]

    def _cpu_quanta_for_priority(prio, pool):
        # Conservative quanta to improve latency without starving others.
        # If pool is large, allow a bit more for interactive/query.
        max_cpu = float(pool.max_cpu_pool)
        if prio == Priority.QUERY:
            return min(6.0, max_cpu) if max_cpu >= 6 else max_cpu
        if prio == Priority.INTERACTIVE:
            return min(4.0, max_cpu) if max_cpu >= 4 else max_cpu
        return min(2.0, max_cpu) if max_cpu >= 2 else max_cpu

    # Enqueue new arrivals
    for p in pipelines:
        _enqueue_pipeline(p)

    # Learn from last tick results
    _learn_from_results()

    # If nothing changed, no need to act
    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    # For each pool, attempt to schedule multiple ops with right-sized resources.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        attempts = 0

        while attempts < s.max_attempts_per_tick_per_pool:
            attempts += 1
            avail_cpu = float(pool.avail_cpu_pool)
            avail_ram = float(pool.avail_ram_pool)
            if avail_cpu <= 0.0 or avail_ram <= 0.0:
                break

            # Find next (pipeline, op) considering priority and pool affinity.
            chosen_p = None
            chosen_op = None

            # We do a priority-ordered search across queues.
            # Additionally, we bias by pool suitability: if this pool is not preferred for that
            # priority and other pools exist, we still allow scheduling, but only if high-priority
            # work is not waiting (simple, safe heuristic).
            queues = _queues_in_priority_order()
            for q, rank in queues:
                if not q:
                    continue
                # Simple pool affinity gating:
                # - If this priority prefers other pools and there are other pools, only schedule it
                #   here if no higher-priority work is waiting.
                # We'll just proceed without hard gating for robustness, but keep the search ordered.

                p, op = _get_next_runnable_op_from_queue(q)
                if p is not None and op is not None:
                    chosen_p, chosen_op = p, op
                    break

            if chosen_p is None:
                break

            # Size RAM: at least the estimator/learned floor; otherwise a small default,
            # but never exceed available RAM.
            mem_floor = _get_mem_floor_gb(chosen_p, chosen_op)
            # Default RAM target when no estimator exists; keep small to allow concurrency.
            if chosen_p.priority in (Priority.QUERY, Priority.INTERACTIVE):
                default_ram = 6.0
            else:
                default_ram = 4.0

            # If floor is 0 (unknown), use default; if floor is known, allocate at least that.
            target_ram = max(mem_floor, default_ram)
            ram = min(avail_ram, target_ram)

            # If even the minimum we believe we need doesn't fit, don't schedule in this pool now.
            # (We keep the pipeline queued for future ticks when resources free up.)
            if mem_floor > 0.0 and avail_ram < mem_floor:
                break

            # Size CPU in quanta, not "all available", to reduce head-of-line blocking.
            preferred_pools = _choose_pool_for_priority(chosen_p.priority)
            cpu_quanta = _cpu_quanta_for_priority(chosen_p.priority, pool)

            # If this pool is preferred for the priority, be a bit more generous.
            if preferred_pools and preferred_pools[0] == pool_id:
                cpu_target = cpu_quanta
            else:
                # Non-preferred pool: keep conservative.
                cpu_target = min(cpu_quanta, 2.0) if chosen_p.priority != Priority.QUERY else min(cpu_quanta, 3.0)

            # If there is plenty of CPU and little contention, allow scaling up modestly.
            total_waiting = len(s.q_query) + len(s.q_interactive) + len(s.q_batch)
            if total_waiting <= 1:
                cpu_target = min(float(pool.max_cpu_pool), max(cpu_target, min(avail_cpu, cpu_quanta * 2.0)))

            cpu = min(avail_cpu, max(1.0, cpu_target)) if avail_cpu >= 1.0 else avail_cpu
            if cpu <= 0.0 or ram <= 0.0:
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

            # Continue loop; pool availability will be updated by the simulator on next tick,
            # but we can still attempt multiple assignments in the same tick with current snapshot.
            # To avoid oversubscription in this tick, we conservatively decrement local vars
            # (won't be perfect, but helps reduce over-assign).
            pool.avail_cpu_pool = max(0.0, float(pool.avail_cpu_pool) - cpu)
            pool.avail_ram_pool = max(0.0, float(pool.avail_ram_pool) - ram)

    return suspensions, assignments
