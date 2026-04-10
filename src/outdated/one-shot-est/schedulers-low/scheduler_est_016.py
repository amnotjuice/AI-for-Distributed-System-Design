# policy_key: scheduler_est_016
# reasoning_effort: low
# model: gpt-5.2-2025-12-11
# llm_cost: 0.044484
# generation_seconds: 42.90
# generated_at: 2026-03-31T23:55:33.074137
@register_scheduler_init(key="scheduler_est_016")
def scheduler_est_016_init(s):
    """
    Priority-aware, right-sized scheduler with simple OOM backoff.

    Improvements over naive FIFO:
      - Separate waiting queues per priority (QUERY > INTERACTIVE > BATCH)
      - Right-size CPU per op (avoid giving one op the entire pool)
      - Allocate RAM using estimator as a floor; on OOM, exponentially back off RAM for that pipeline
      - Keep headroom for high-priority work by reserving a fraction of pool resources from batch
      - Simple aging: long-waiting batch pipelines get temporarily treated as interactive
    """
    # Step counter to support simple aging without needing wall-clock time.
    s._step = 0

    # Waiting queues store tuples: (enqueue_step, pipeline)
    s.waiting_query = []
    s.waiting_interactive = []
    s.waiting_batch = []

    # Per-pipeline retry/override state for OOM backoff.
    s.retry_count = {}          # pipeline_id -> int
    s.ram_override_gb = {}      # pipeline_id -> float (RAM to try next time, if larger than estimator)
    s.max_retries = 3

    # Tuning knobs (kept deliberately simple).
    s.default_mem_floor_gb = 1.0
    s.batch_aging_steps = 25

    # Per-op CPU cap: avoid assigning the whole pool to one op, improve latency under contention.
    # We compute an effective cap per pool at runtime, but keep a lower bound here.
    s.min_cpu_per_op = 1.0

    # Reserve fractions used only when there is waiting high-priority work.
    s.reserve_cpu_frac_for_hp = 0.25
    s.reserve_ram_frac_for_hp = 0.25


@register_scheduler(key="scheduler_est_016")
def scheduler_est_016_scheduler(s, results, pipelines):
    """
    Policy:
      1) Ingest new pipelines into per-priority queues.
      2) Process execution results:
         - On failure, increase RAM override for the pipeline (OOM backoff heuristic).
      3) For each pool, schedule ready operators:
         - Always prefer QUERY, then INTERACTIVE, then BATCH (with aging).
         - Use estimator.mem_peak_gb as RAM floor; apply per-pipeline RAM override if present.
         - Cap CPU per op to allow concurrency and reduce tail latency.
         - When high-priority work is waiting, prevent batch from consuming a reserved headroom slice.
    """
    s._step += 1

    # --- Helpers (kept local to avoid relying on external imports) ---
    def _enqueue(p):
        if p.priority == Priority.QUERY:
            s.waiting_query.append((s._step, p))
        elif p.priority == Priority.INTERACTIVE:
            s.waiting_interactive.append((s._step, p))
        else:
            s.waiting_batch.append((s._step, p))

    def _is_dropped(pipeline_id):
        return s.retry_count.get(pipeline_id, 0) > s.max_retries

    def _pop_next_pipeline():
        """
        Select next pipeline to attempt scheduling.
        Includes simple aging: old batch can be treated as interactive.
        Returns (enqueue_step, pipeline) or None.
        """
        # Highest priority first.
        if s.waiting_query:
            return s.waiting_query.pop(0)

        if s.waiting_interactive:
            return s.waiting_interactive.pop(0)

        # Aging for batch: if it waited long enough, treat it as interactive.
        if s.waiting_batch:
            enqueue_step, p = s.waiting_batch[0]
            if (s._step - enqueue_step) >= s.batch_aging_steps:
                return s.waiting_batch.pop(0)

        # Otherwise, normal batch.
        if s.waiting_batch:
            return s.waiting_batch.pop(0)

        return None

    def _requeue(enqueue_step, p):
        # Keep original enqueue_step to preserve waiting time/aging.
        if p.priority == Priority.QUERY:
            s.waiting_query.append((enqueue_step, p))
        elif p.priority == Priority.INTERACTIVE:
            s.waiting_interactive.append((enqueue_step, p))
        else:
            s.waiting_batch.append((enqueue_step, p))

    def _has_waiting_high_priority():
        return bool(s.waiting_query or s.waiting_interactive)

    def _cpu_cap_for_pool(pool):
        # Allow some concurrency: cap at about half the pool, but at least 1 vCPU.
        cap = pool.max_cpu_pool / 2.0
        if cap < s.min_cpu_per_op:
            cap = s.min_cpu_per_op
        return cap

    def _ram_floor_for_op(op):
        est = None
        try:
            est = op.estimate.mem_peak_gb
        except Exception:
            est = None
        if est is None:
            return s.default_mem_floor_gb
        # Be defensive: avoid zero/negative estimates.
        return est if est > 0 else s.default_mem_floor_gb

    # --- Ingest arrivals ---
    for p in pipelines:
        _enqueue(p)

    # Early exit if nothing changed
    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    # --- Process results: OOM backoff / retry accounting ---
    for r in results:
        if not r.failed():
            continue

        # Increase retry count (per pipeline) and backoff RAM for next attempt.
        # We do not attempt fine-grained per-operator tuning here (keep it simple and robust).
        pid = getattr(r, "pipeline_id", None)
        # ExecutionResult in this simulator template doesn't guarantee pipeline_id,
        # so infer it from ops if needed; if unavailable, skip tracking.
        if pid is None:
            # Best-effort: ops likely belong to one pipeline; but we can't safely infer pipeline_id.
            continue

        s.retry_count[pid] = s.retry_count.get(pid, 0) + 1

        # Heuristic: if this looks like an OOM, increase RAM more aggressively.
        err = (r.error or "").lower() if hasattr(r, "error") else ""
        oom_like = ("oom" in err) or ("out of memory" in err) or ("memory" in err)

        bump = 2.0 if oom_like else 1.5
        next_ram = max(r.ram * bump, s.ram_override_gb.get(pid, 0.0))

        # Cap will be applied at scheduling time per target pool.
        s.ram_override_gb[pid] = next_ram

    # --- Scheduling loop over pools ---
    # We will attempt to schedule multiple operators per pool, respecting headroom reservations.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]

        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool

        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # If high-priority work exists anywhere, reserve headroom from batch in this pool.
        reserve_cpu = 0.0
        reserve_ram = 0.0
        if _has_waiting_high_priority():
            reserve_cpu = pool.max_cpu_pool * s.reserve_cpu_frac_for_hp
            reserve_ram = pool.max_ram_pool * s.reserve_ram_frac_for_hp

        cpu_cap = _cpu_cap_for_pool(pool)

        # Keep a temporary list to requeue pipelines we looked at but couldn't schedule this tick.
        looked_at = []

        # Attempt to fill this pool with multiple assignments.
        while True:
            # Stop if no useful resources remain.
            if avail_cpu < s.min_cpu_per_op or avail_ram <= 0:
                break

            item = _pop_next_pipeline()
            if item is None:
                break

            enqueue_step, pipeline = item

            # Drop pipelines that exceeded retry limit.
            if _is_dropped(pipeline.pipeline_id):
                continue

            status = pipeline.runtime_status()
            if status.is_pipeline_successful():
                continue

            # Get one ready op (keep it simple: schedule at most one op per pipeline at a time).
            op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
            if not op_list:
                # Not ready yet; requeue and try another pipeline.
                looked_at.append((enqueue_step, pipeline))
                continue

            op = op_list[0]

            # Compute RAM target: estimator floor + per-pipeline override (from failures).
            base_floor = _ram_floor_for_op(op)
            override = s.ram_override_gb.get(pipeline.pipeline_id, 0.0)
            target_ram = max(base_floor, override)

            # Cap target RAM by pool max and current availability.
            # (extra RAM beyond need is fine, but cannot exceed what the pool can provide)
            if target_ram > pool.max_ram_pool:
                target_ram = pool.max_ram_pool

            # Determine if this is a batch pipeline and apply headroom reservation.
            is_batch = (pipeline.priority == Priority.BATCH_PIPELINE)

            effective_avail_cpu = avail_cpu
            effective_avail_ram = avail_ram
            if is_batch and _has_waiting_high_priority():
                effective_avail_cpu = max(0.0, avail_cpu - reserve_cpu)
                effective_avail_ram = max(0.0, avail_ram - reserve_ram)

            # Must be able to at least meet RAM floor and minimal CPU.
            if effective_avail_cpu < s.min_cpu_per_op or effective_avail_ram < target_ram:
                # Can't fit right now; requeue and stop trying to pack this pool further
                # to avoid spinning on unschedulable work.
                looked_at.append((enqueue_step, pipeline))
                break

            # Right-size CPU to enable concurrency (helps latency under contention).
            cpu_to_give = min(effective_avail_cpu, cpu_cap)
            if cpu_to_give < s.min_cpu_per_op:
                cpu_to_give = s.min_cpu_per_op

            # If after rounding we can't fit, stop.
            if cpu_to_give > effective_avail_cpu or target_ram > effective_avail_ram:
                looked_at.append((enqueue_step, pipeline))
                break

            assignments.append(
                Assignment(
                    ops=[op],
                    cpu=cpu_to_give,
                    ram=target_ram,
                    priority=pipeline.priority,
                    pool_id=pool_id,
                    pipeline_id=pipeline.pipeline_id,
                )
            )

            avail_cpu -= cpu_to_give
            avail_ram -= target_ram

            # Put the pipeline back so it can schedule its next operators later.
            looked_at.append((enqueue_step, pipeline))

        # Requeue pipelines we examined but did not complete.
        for enqueue_step, p in looked_at:
            # It's possible it completed while we were iterating; check again.
            st = p.runtime_status()
            if st.is_pipeline_successful():
                continue
            if _is_dropped(p.pipeline_id):
                continue
            _requeue(enqueue_step, p)

    return suspensions, assignments
