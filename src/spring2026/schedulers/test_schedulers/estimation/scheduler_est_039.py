# policy_key: scheduler_est_039
# reasoning_effort: low
# exp: estimation
# model: gpt-5.2-2025-12-11
# llm_cost: 0.050652
# generation_seconds: 47.86
# generated_at: 2026-04-10T10:17:06.482452
@register_scheduler_init(key="scheduler_est_039")
def scheduler_est_039_init(s):
    """Memory-aware, priority-protecting scheduler.

    Main ideas:
      - Priority-first service with small batch share to avoid starvation.
      - Memory-aware placement using op.estimate.mem_peak_gb (noisy hint) to reduce OOMs.
      - Conservative RAM sizing with per-pipeline backoff on observed failures (OOM or any failure).
      - Avoid over-allocating CPU to a single op to enable concurrency and protect tail latency.
    """
    from collections import deque

    # Separate queues by priority class
    s.q_query = deque()
    s.q_interactive = deque()
    s.q_batch = deque()

    # Per-pipeline RAM backoff multiplier (increases after failures)
    s.ram_boost = {}  # pipeline_id -> float

    # Per-pipeline cooldown after repeated failures (tick-based)
    s.cooldown_until = {}  # pipeline_id -> int (logical tick)

    # Logical tick counter (advances on each scheduler call where something changes)
    s.tick = 0

    # Service pattern per pool per tick: protects query/interactive, still gives batch chances
    # This is a simple "quota" sequence used to pick next pipeline to attempt.
    s.service_pattern = ["query", "query", "interactive", "query", "interactive", "batch"]
    s.service_idx = 0

    # Tunables (kept modest to remain robust across pool sizes)
    s.cpu_cap_per_op_frac = 0.50  # at most 50% of pool CPU to one assignment
    s.cpu_min_per_op = 1.0        # never allocate less than 1 vCPU if possible
    s.ram_headroom_frac = 0.10    # keep 10% headroom to reduce fragmentation/OOM cascades
    s.ram_min_per_op_gb = 0.5     # minimum RAM request even if estimator is tiny/None
    s.ram_safety_mult = 1.15      # estimator safety factor (noisy hint)
    s.ram_boost_init = 1.00
    s.ram_boost_max = 6.00
    s.ram_boost_on_fail = 1.60    # increase RAM request multiplier after failure
    s.cooldown_base = 2           # ticks to cooldown after a failure
    s.cooldown_max = 20           # max ticks cooldown


@register_scheduler(key="scheduler_est_039")
def scheduler_est_039_scheduler(s, results, pipelines):
    """
    Scheduler step.

    Policy:
      1) Enqueue arriving pipelines by priority.
      2) Process results: on failure, increase per-pipeline RAM boost and add a short cooldown.
      3) For each pool, greedily assign multiple ops while resources remain:
           - choose next pipeline by (priority pattern + fairness),
           - select an assignable op (parents complete),
           - place only if estimated memory likely fits,
           - request RAM ~= est * safety * boost (bounded), CPU capped to allow concurrency.
    """
    # --- helpers (kept inside to avoid imports at module scope) ---
    def _now_tick():
        return s.tick

    def _priority_bucket(p):
        pr = getattr(p, "priority", None)
        if pr == Priority.QUERY:
            return "query"
        if pr == Priority.INTERACTIVE:
            return "interactive"
        return "batch"

    def _get_queue(bucket):
        if bucket == "query":
            return s.q_query
        if bucket == "interactive":
            return s.q_interactive
        return s.q_batch

    def _ensure_pipeline_state(pipeline_id):
        if pipeline_id not in s.ram_boost:
            s.ram_boost[pipeline_id] = s.ram_boost_init
        if pipeline_id not in s.cooldown_until:
            s.cooldown_until[pipeline_id] = -1

    def _pipeline_done_or_dead(p):
        st = p.runtime_status()
        if st.is_pipeline_successful():
            return True
        # If any operator failed, we still allow retries (FAILED is assignable),
        # but if the runtime marks pipeline as irrecoverably failed, drop it.
        # Lacking a direct flag, we only drop if there are no assignable ops and
        # no pending/running ops either (stuck).
        return False

    def _pipeline_has_assignable_op(p):
        st = p.runtime_status()
        ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        return bool(ops)

    def _pick_assignable_op(p):
        st = p.runtime_status()
        ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        if not ops:
            return None
        # Prefer lower estimated memory ops first within a pipeline to improve fit.
        def mem_key(op):
            est = None
            if getattr(op, "estimate", None) is not None:
                est = getattr(op.estimate, "mem_peak_gb", None)
            # Unknown estimates are treated as larger to reduce risk
            if est is None:
                return 1e18
            return float(est)
        ops_sorted = sorted(ops, key=mem_key)
        return ops_sorted[0]

    def _op_est_mem(op):
        est = None
        if getattr(op, "estimate", None) is not None:
            est = getattr(op.estimate, "mem_peak_gb", None)
        return None if est is None else float(est)

    def _estimate_ram_request(op, pool, pipeline_id):
        """
        Compute RAM request in GB for the assignment (bounded by pool capacity).
        Uses estimator as hint + per-pipeline backoff multiplier.
        """
        boost = s.ram_boost.get(pipeline_id, s.ram_boost_init)
        est_mem = _op_est_mem(op)

        # Keep some pool headroom to reduce fragmentation and OOM cascades
        pool_effective_max = max(0.0, float(pool.max_ram_pool) * (1.0 - s.ram_headroom_frac))
        pool_effective_avail = max(0.0, float(pool.avail_ram_pool) * (1.0 - s.ram_headroom_frac))

        if est_mem is None:
            # Conservative default: request a moderate chunk to reduce OOM risk,
            # but don't block others.
            base = max(s.ram_min_per_op_gb, min(pool_effective_max, pool_effective_avail))
            # If plenty of memory, request a bit more; if tight, keep small.
            # This avoids "all-or-nothing" behavior in small pools.
            ram_req = max(s.ram_min_per_op_gb, min(pool_effective_max, base))
            # Apply modest boost (still helps when we observed failures)
            ram_req = min(pool_effective_max, max(s.ram_min_per_op_gb, ram_req * min(boost, 2.0)))
            return ram_req

        # Estimator-guided request with safety factor and backoff
        ram_req = est_mem * s.ram_safety_mult * boost

        # Bound request: at least a floor, at most effective max
        ram_req = max(s.ram_min_per_op_gb, min(pool_effective_max, ram_req))

        # Also don't request more than we can actually allocate now (effective available),
        # otherwise it will never be placed in this pool this tick.
        ram_req = min(ram_req, pool_effective_avail)
        return ram_req

    def _estimate_cpu_request(pool):
        """
        Allocate capped CPU to allow concurrency.
        """
        avail_cpu = float(pool.avail_cpu_pool)
        if avail_cpu <= 0:
            return 0.0
        cap = float(pool.max_cpu_pool) * float(s.cpu_cap_per_op_frac)
        cpu_req = min(avail_cpu, cap)
        if cpu_req < s.cpu_min_per_op and avail_cpu >= s.cpu_min_per_op:
            cpu_req = s.cpu_min_per_op
        return cpu_req

    def _fits_some_pool(op):
        """Quick feasibility check to avoid repeatedly trying impossible ops."""
        est_mem = _op_est_mem(op)
        if est_mem is None:
            return True  # unknown; let it try
        need = est_mem * s.ram_safety_mult
        for i in range(s.executor.num_pools):
            pool = s.executor.pools[i]
            if need <= float(pool.max_ram_pool):
                return True
        return False

    def _pick_next_pipeline_bucket():
        # Cycle through a fixed pattern to protect query/interactive,
        # while ensuring batch gets periodic opportunities.
        b = s.service_pattern[s.service_idx % len(s.service_pattern)]
        s.service_idx = (s.service_idx + 1) % len(s.service_pattern)
        return b

    def _pop_next_pipeline(preferred_bucket):
        """
        Pop next eligible pipeline:
          - Try preferred bucket first.
          - Fallback to other buckets in order query->interactive->batch to protect objective.
          - Skip completed or cooling down pipelines (requeue them).
        """
        buckets_to_try = [preferred_bucket]
        for b in ["query", "interactive", "batch"]:
            if b not in buckets_to_try:
                buckets_to_try.append(b)

        now = _now_tick()

        for b in buckets_to_try:
            q = _get_queue(b)
            # Scan through queue up to its current length to find an eligible item.
            # Requeue skipped items to preserve FIFO-ish ordering within each bucket.
            n = len(q)
            for _ in range(n):
                p = q.popleft()
                pid = p.pipeline_id
                _ensure_pipeline_state(pid)

                if _pipeline_done_or_dead(p):
                    continue

                # Cooldown to avoid thrashing on repeated failures
                if s.cooldown_until.get(pid, -1) > now:
                    q.append(p)
                    continue

                # If currently no assignable op (blocked by deps), requeue and continue
                if not _pipeline_has_assignable_op(p):
                    q.append(p)
                    continue

                return p
        return None

    # --- ingest new pipelines ---
    for p in pipelines:
        pid = p.pipeline_id
        _ensure_pipeline_state(pid)
        bucket = _priority_bucket(p)
        _get_queue(bucket).append(p)

    # --- advance tick if anything changed (pipelines arrival or results) ---
    if pipelines or results:
        s.tick += 1

    # --- process results: on failures, increase RAM boost and set cooldown ---
    for r in results:
        # Results may include successes and failures; we only react strongly to failures.
        if hasattr(r, "failed") and r.failed():
            pid = getattr(r, "pipeline_id", None)
            # pipeline_id may not exist on result in some implementations; fallback:
            # infer from ops (if any) not feasible here, so guard.
            if pid is None:
                continue
            _ensure_pipeline_state(pid)
            # Increase RAM boost to reduce repeat OOMs; cap to prevent monopolization.
            s.ram_boost[pid] = min(s.ram_boost_max, s.ram_boost.get(pid, s.ram_boost_init) * s.ram_boost_on_fail)
            # Add cooldown that grows with boost (simple proxy for repeated failures)
            growth = int(min(s.cooldown_max, s.cooldown_base * s.ram_boost[pid]))
            s.cooldown_until[pid] = max(s.cooldown_until.get(pid, -1), _now_tick() + growth)

    # early exit if no state changes that affect our decisions
    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    # --- main placement loop: fill each pool greedily with multiple assignments ---
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]

        # Try to place as many ops as we can in this pool this tick.
        # Stop when CPU or RAM is too tight to place even minimal work.
        while True:
            avail_cpu = float(pool.avail_cpu_pool)
            avail_ram = float(pool.avail_ram_pool)
            if avail_cpu <= 0 or avail_ram <= s.ram_min_per_op_gb:
                break

            preferred_bucket = _pick_next_pipeline_bucket()
            p = _pop_next_pipeline(preferred_bucket)
            if p is None:
                break  # no eligible pipelines available

            pid = p.pipeline_id
            op = _pick_assignable_op(p)
            if op is None:
                # Nothing assignable right now; requeue in its bucket and try again
                _get_queue(_priority_bucket(p)).append(p)
                continue

            # Avoid repeatedly attempting ops that almost certainly don't fit anywhere.
            if not _fits_some_pool(op):
                # Put it back but with cooldown to reduce churn; likely to fail anyway.
                s.cooldown_until[pid] = max(s.cooldown_until.get(pid, -1), _now_tick() + s.cooldown_max)
                _get_queue(_priority_bucket(p)).append(p)
                continue

            # Memory-aware placement: if it doesn't fit the current pool's available RAM (with headroom),
            # requeue and let other pools / future ticks handle it.
            ram_req = _estimate_ram_request(op, pool, pid)
            if ram_req <= 0 or ram_req > float(pool.avail_ram_pool) * (1.0 - s.ram_headroom_frac):
                _get_queue(_priority_bucket(p)).append(p)
                continue

            cpu_req = _estimate_cpu_request(pool)
            if cpu_req <= 0:
                _get_queue(_priority_bucket(p)).append(p)
                break  # pool is CPU constrained; move to next pool

            # Construct assignment for a single op to keep scheduling granular and responsive.
            assignment = Assignment(
                ops=[op],
                cpu=cpu_req,
                ram=ram_req,
                priority=p.priority,
                pool_id=pool_id,
                pipeline_id=pid,
            )
            assignments.append(assignment)

            # Requeue the pipeline to allow other pipelines to interleave (prevents starvation)
            _get_queue(_priority_bucket(p)).append(p)

            # If pool becomes tight, stop trying to pack more.
            if float(pool.avail_cpu_pool) <= 0 or float(pool.avail_ram_pool) <= s.ram_min_per_op_gb:
                break

    return suspensions, assignments
