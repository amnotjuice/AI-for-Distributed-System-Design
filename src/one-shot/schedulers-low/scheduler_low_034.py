# policy_key: scheduler_low_034
# reasoning_effort: low
# model: gpt-5.2-2025-12-11
# llm_cost: 0.043525
# generation_seconds: 49.63
# generated_at: 2026-03-14T02:28:47.626430
@register_scheduler_init(key="scheduler_low_034")
def scheduler_low_034_init(s):
    """
    Priority-aware scheduler with small, safe improvements over naive FIFO.

    Improvements (kept intentionally incremental):
      1) Separate waiting queues per priority (QUERY > INTERACTIVE > BATCH).
      2) Reserve a fraction of each pool's capacity for high-priority work by
         limiting how much BATCH can consume when contention exists.
      3) Simple failure-aware right-sizing:
         - If an op fails with an OOM-like error, increase its RAM hint (exponential backoff).
         - Otherwise, retry a small number of times with the same sizing.
      4) Fill each pool with multiple single-op assignments per tick (instead of at most one),
         using per-op CPU/RAM caps to reduce head-of-line blocking.
    """
    # Per-priority FIFO queues of pipelines
    s.waiting = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }

    # Per-op resource hints (learned from failures / previous attempts)
    # key: (pipeline_id, op_key) -> dict(cpu=..., ram=..., oom_retries=..., other_retries=...)
    s.op_hints = {}

    # Reservation fractions (protect latency for higher priority by constraining BATCH)
    s.reserve_cpu_frac = 0.35
    s.reserve_ram_frac = 0.35

    # Per-op caps to avoid assigning "everything" to a single op and causing long tails
    s.max_cpu_per_op_frac = 0.60  # up to 60% of a pool's max vCPU for one op
    s.max_ram_per_op_frac = 0.75  # up to 75% of a pool's max RAM for one op

    # Minimum allocations (avoid pathological tiny containers)
    s.min_cpu = 1e-6
    s.min_ram = 1e-6

    # Retry limits
    s.max_oom_retries = 4
    s.max_other_retries = 2

    # Weighted service to avoid total starvation of lower priority while still latency-first
    # Per pool, we cycle through this pattern when picking next pipeline to schedule.
    s.service_pattern = [
        Priority.QUERY, Priority.QUERY, Priority.QUERY, Priority.QUERY,
        Priority.INTERACTIVE, Priority.INTERACTIVE,
        Priority.BATCH_PIPELINE,
    ]
    s.service_cursor = 0


@register_scheduler(key="scheduler_low_034")
def scheduler_low_034(s, results, pipelines):
    """
    Main scheduling step.

    Strategy:
      - Enqueue new pipelines into per-priority FIFO.
      - Update per-op hints from execution results (especially OOM => raise RAM).
      - For each pool:
          * compute available CPU/RAM
          * repeatedly schedule one runnable op at a time until resources are tight
          * choose pipelines in a latency-first order with a small WRR pattern
          * for BATCH, limit "effective available" resources by keeping a reserve
            of the pool for future high-priority arrivals.
    """
    # --- helpers (kept nested to avoid imports / global dependencies) ---
    def _is_oom_error(err):
        if err is None:
            return False
        msg = str(err).lower()
        return ("oom" in msg) or ("out of memory" in msg) or ("cuda out of memory" in msg)

    def _op_key(pipeline_id, op):
        # Prefer stable explicit ids when present
        oid = getattr(op, "op_id", None)
        if oid is None:
            oid = getattr(op, "operator_id", None)
        if oid is None:
            oid = id(op)
        return (pipeline_id, oid)

    def _get_hint(pipeline_id, op):
        k = _op_key(pipeline_id, op)
        h = s.op_hints.get(k)
        if h is None:
            h = {"cpu": None, "ram": None, "oom_retries": 0, "other_retries": 0}
            s.op_hints[k] = h
        return h

    def _clamp(x, lo, hi):
        if x < lo:
            return lo
        if x > hi:
            return hi
        return x

    def _pool_effective_avail(pool, priority):
        """
        For BATCH, keep some reserve for future QUERY/INTERACTIVE work.
        For higher priorities, use full availability.
        """
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool
        if priority == Priority.BATCH_PIPELINE:
            reserve_cpu = s.reserve_cpu_frac * pool.max_cpu_pool
            reserve_ram = s.reserve_ram_frac * pool.max_ram_pool
            avail_cpu = max(0.0, avail_cpu - reserve_cpu)
            avail_ram = max(0.0, avail_ram - reserve_ram)
        return avail_cpu, avail_ram

    def _pick_next_priority_with_work():
        """
        Latency-first WRR:
          - Advance cursor until we find a priority with queued pipelines.
          - If none, return None.
        """
        total = len(s.service_pattern)
        for i in range(total):
            pr = s.service_pattern[(s.service_cursor + i) % total]
            if s.waiting[pr]:
                s.service_cursor = (s.service_cursor + i + 1) % total
                return pr
        return None

    def _choose_pool_for_priority(priority):
        """
        Simple placement: choose pool with most effective RAM headroom for that priority,
        tie-break on CPU headroom. (RAM tends to be the constraining resource for OOM risk.)
        """
        best = None
        best_tuple = None
        for pool_id in range(s.executor.num_pools):
            pool = s.executor.pools[pool_id]
            eff_cpu, eff_ram = _pool_effective_avail(pool, priority)
            score = (eff_ram, eff_cpu)
            if best is None or score > best_tuple:
                best = pool_id
                best_tuple = score
        return best

    # --- enqueue new pipelines ---
    for p in pipelines:
        # Unknown priority values: default to BATCH_PIPELINE-like behavior
        pr = getattr(p, "priority", Priority.BATCH_PIPELINE)
        if pr not in s.waiting:
            pr = Priority.BATCH_PIPELINE
        s.waiting[pr].append(p)

    # Early exit if no new info; still could schedule if queue non-empty, so don't exit
    # solely on pipelines/results.

    # --- learn from results (failure-aware sizing) ---
    for r in results:
        if not getattr(r, "failed", lambda: False)():
            continue

        # If this result corresponds to multiple ops, update each
        ops = getattr(r, "ops", []) or []
        # We might not always know pipeline_id here; use a best-effort extraction.
        # If absent, we can't key hints correctly; fall back to op identity only.
        # (Most implementations include pipeline association in op objects.)
        for op in ops:
            # Try to discover pipeline_id from op or result
            pipeline_id = getattr(op, "pipeline_id", None)
            if pipeline_id is None:
                pipeline_id = getattr(r, "pipeline_id", None)
            if pipeline_id is None:
                # As a fallback, use a shared placeholder so we at least adapt within the run
                pipeline_id = -1

            h = _get_hint(pipeline_id, op)
            if _is_oom_error(getattr(r, "error", None)):
                # Exponential backoff on RAM hint (bounded by pool max later)
                h["oom_retries"] += 1
                # Base on last attempted RAM if known, else on reported r.ram if present
                base_ram = h["ram"]
                if base_ram is None:
                    base_ram = getattr(r, "ram", None)
                if base_ram is None:
                    base_ram = s.min_ram
                # Double RAM each OOM up to retry cap
                h["ram"] = float(base_ram) * 2.0
            else:
                h["other_retries"] += 1
                # Keep hints unchanged; we only limit retries.

    suspensions = []
    assignments = []

    # --- scheduling loop: fill pools with single-op assignments ---
    # We schedule per-pool, but pick pipelines based on global queues.
    # This keeps behavior simple and tends to reduce tail latency for high priorities.
    for _ in range(s.executor.num_pools * 8):
        # Choose a priority that has work
        pr = _pick_next_priority_with_work()
        if pr is None:
            break

        # Choose a pool for this priority
        pool_id = _choose_pool_for_priority(pr)
        if pool_id is None:
            break
        pool = s.executor.pools[pool_id]

        # Compute effective availability (BATCH constrained by reserve)
        eff_cpu, eff_ram = _pool_effective_avail(pool, pr)
        if eff_cpu <= 0.0 or eff_ram <= 0.0:
            # Can't place this priority right now; try another priority next iteration
            continue

        # Pop next pipeline in this priority queue
        pipeline = s.waiting[pr].pop(0)

        status = pipeline.runtime_status()
        if status.is_pipeline_successful():
            continue

        # If pipeline has failures, we still allow retrying, but only for limited attempts per op.
        # We'll select only ops whose parents are complete.
        op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
        if not op_list:
            # Nothing runnable now (parents not ready or in-flight); push back to tail
            s.waiting[pr].append(pipeline)
            continue

        op = op_list[0]
        hint = _get_hint(pipeline.pipeline_id, op)

        # Enforce retry caps: if exceeded, treat as terminal and drop pipeline from scheduling.
        if hint["oom_retries"] > s.max_oom_retries or hint["other_retries"] > s.max_other_retries:
            # Give up on this op/pipeline; do not reschedule it (prevents infinite churn)
            continue

        # Determine target CPU/RAM with caps; default to a moderate chunk rather than "all"
        max_cpu_for_op = max(s.min_cpu, s.max_cpu_per_op_frac * pool.max_cpu_pool)
        max_ram_for_op = max(s.min_ram, s.max_ram_per_op_frac * pool.max_ram_pool)

        # If we have learned hints, respect them; otherwise choose a balanced default share
        target_cpu = hint["cpu"]
        if target_cpu is None:
            # Default: 25% of pool max, but no more than currently available
            target_cpu = 0.25 * pool.max_cpu_pool
        target_ram = hint["ram"]
        if target_ram is None:
            # Default: 25% of pool max
            target_ram = 0.25 * pool.max_ram_pool

        # Clamp by per-op caps and current effective availability
        cpu = _clamp(float(target_cpu), s.min_cpu, min(max_cpu_for_op, eff_cpu))
        ram = _clamp(float(target_ram), s.min_ram, min(max_ram_for_op, eff_ram))

        # If still no room, push pipeline back and move on
        if cpu <= 0.0 or ram <= 0.0:
            s.waiting[pr].append(pipeline)
            continue

        # Create assignment
        assignments.append(
            Assignment(
                ops=[op],
                cpu=cpu,
                ram=ram,
                priority=pipeline.priority,
                pool_id=pool_id,
                pipeline_id=pipeline.pipeline_id,
            )
        )

        # Push pipeline back to tail so others can proceed (reduces HoL blocking)
        s.waiting[pr].append(pipeline)

        # Note: We rely on the simulator updating pool.avail_* after applying assignments.
        # The outer loop will naturally stop when effective availability becomes small.

    return suspensions, assignments
