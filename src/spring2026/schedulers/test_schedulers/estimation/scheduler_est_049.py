# policy_key: scheduler_est_049
# reasoning_effort: low
# exp: estimation
# model: gpt-5.2-2025-12-11
# llm_cost: 0.053312
# generation_seconds: 40.95
# generated_at: 2026-04-10T10:25:08.051751
@register_scheduler_init(key="scheduler_est_049")
def scheduler_est_049_init(s):
    """Memory-aware, priority-weighted greedy scheduler with conservative OOM backoff.

    Core ideas:
      - Protect QUERY and INTERACTIVE latency via strict priority ordering (with light aging).
      - Reduce OOM failures using op.estimate.mem_peak_gb as a noisy hint + safety margin.
      - On observed OOM failures, increase per-operator RAM request multiplier and retry.
      - Avoid wasting resources on repeated non-OOM failures by capping retries per operator.
      - Greedily pack operators that fit (prefer smaller estimated memory to preserve headroom).
    """
    s.waiting_pipelines = []  # pipelines in arrival order; we will re-sort each tick
    s.tick = 0

    # Per operator retry state keyed by (pipeline_id, id(op))
    s.op_ram_mult = {}        # multiplicative RAM backoff after OOM
    s.op_fail_count = {}      # total failure count (any reason)
    s.op_nonoom_fail = {}     # count of non-OOM failures

    # Per pipeline metadata
    s.pipeline_first_seen_tick = {}  # pipeline_id -> tick
    s.pipeline_blacklisted = set()   # pipelines we stop scheduling (repeated non-OOM failures)

    # Tuning knobs
    s.mem_safety = 1.25       # safety factor on top of estimator (noisy)
    s.mem_floor_gb = 0.5      # minimum RAM request if estimator missing/small
    s.mem_cap_fraction = 0.98 # never request more than this fraction of pool max RAM
    s.max_nonoom_fail_per_op = 1
    s.max_total_fail_per_op = 4

    # Aging to prevent starvation; small because we heavily prioritize queries/interactives
    s.aging_per_tick = 0.0005


@register_scheduler(key="scheduler_est_049")
def scheduler_est_049_scheduler(s, results, pipelines):
    def _prio_rank(p):
        # Higher is more important
        if p == Priority.QUERY:
            return 3
        if p == Priority.INTERACTIVE:
            return 2
        return 1  # Priority.BATCH_PIPELINE and others

    def _prio_cpu_fraction(p):
        # More CPU for higher priority to reduce latency.
        if p == Priority.QUERY:
            return 0.85
        if p == Priority.INTERACTIVE:
            return 0.70
        return 0.50

    def _is_oom_result(r):
        # Best-effort OOM detection: rely on error string if present.
        try:
            if r is None or not r.failed():
                return False
        except Exception:
            return False
        err = getattr(r, "error", None)
        if err is None:
            return False
        try:
            e = str(err).lower()
        except Exception:
            return False
        return ("oom" in e) or ("out of memory" in e) or ("memory" in e and "killed" in e)

    def _op_key(pipeline_id, op):
        return (pipeline_id, id(op))

    def _get_est_mem_gb(op):
        est = getattr(op, "estimate", None)
        if est is None:
            return None
        v = getattr(est, "mem_peak_gb", None)
        try:
            if v is None:
                return None
            v = float(v)
            if v < 0:
                return None
            return v
        except Exception:
            return None

    def _ram_request_gb(pipeline_id, op, pool):
        # Base on estimator with safety, then apply OOM backoff multiplier.
        est_mem = _get_est_mem_gb(op)
        base = s.mem_floor_gb if est_mem is None else max(s.mem_floor_gb, est_mem * s.mem_safety)

        mult = s.op_ram_mult.get(_op_key(pipeline_id, op), 1.0)
        req = base * mult

        # Clamp to pool's maximum RAM (keep a tiny headroom to reduce fragmentation risk).
        cap = max(0.0, float(pool.max_ram_pool) * s.mem_cap_fraction)
        if cap > 0:
            req = min(req, cap)
        # Also never request <= 0
        return max(s.mem_floor_gb, req)

    def _fits_any_pool(pipeline_id, op):
        # Skip operators that clearly cannot fit in any pool by estimated memory.
        # Use a lenient check: if estimate exists and exceeds every pool cap, delay (or effectively unschedulable).
        est_mem = _get_est_mem_gb(op)
        if est_mem is None:
            return True
        for i in range(s.executor.num_pools):
            pool = s.executor.pools[i]
            cap = float(pool.max_ram_pool) * s.mem_cap_fraction
            if est_mem <= cap:
                return True
        return False

    def _pipeline_sort_key(p):
        # Strict priority dominates; then modest aging; then FIFO by first-seen.
        first = s.pipeline_first_seen_tick.get(p.pipeline_id, s.tick)
        age = max(0, s.tick - first)
        # Negative for descending sort
        return (-_prio_rank(p.priority), -(age * s.aging_per_tick), first)

    s.tick += 1

    # Ingest new pipelines
    for p in pipelines:
        s.waiting_pipelines.append(p)
        if p.pipeline_id not in s.pipeline_first_seen_tick:
            s.pipeline_first_seen_tick[p.pipeline_id] = s.tick

    # Update retry state from results
    for r in results:
        if r is None:
            continue
        try:
            if not r.failed():
                continue
        except Exception:
            continue

        pipeline_id = getattr(r, "pipeline_id", None)
        # If pipeline_id is not present on results, fall back to op association only.
        ops = getattr(r, "ops", []) or []

        for op in ops:
            # Best-effort: if pipeline_id missing, use a sentinel key per-op
            pid = pipeline_id if pipeline_id is not None else -1
            k = _op_key(pid, op)

            s.op_fail_count[k] = s.op_fail_count.get(k, 0) + 1

            if _is_oom_result(r):
                # Increase RAM multiplier (bounded) and allow retry.
                cur = s.op_ram_mult.get(k, 1.0)
                # Escalate quickly early, then taper.
                nxt = cur * (1.6 if cur < 2.0 else 1.3)
                s.op_ram_mult[k] = min(nxt, 8.0)
            else:
                s.op_nonoom_fail[k] = s.op_nonoom_fail.get(k, 0) + 1

    # Early exit
    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    # Remove completed/failed pipelines; keep the rest
    retained = []
    for p in s.waiting_pipelines:
        if p.pipeline_id in s.pipeline_blacklisted:
            continue
        status = p.runtime_status()
        if status.is_pipeline_successful():
            continue
        # If any operator is FAILED, the pipeline might still be recoverable (OOM), so keep it.
        retained.append(p)
    s.waiting_pipelines = retained

    # Build a list of assignable candidates from pipelines (one "next op" per pipeline)
    # Then greedily place into pools preferring best-fit by memory and priority.
    candidate_list = []
    sorted_pipes = sorted(s.waiting_pipelines, key=_pipeline_sort_key)
    for p in sorted_pipes:
        if p.pipeline_id in s.pipeline_blacklisted:
            continue
        status = p.runtime_status()

        # Select the next assignable op whose parents are complete (single-op step).
        op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
        if not op_list:
            continue
        op = op_list[0]

        # Enforce failure caps: avoid burning cycles on repeated non-OOM failures.
        k = _op_key(p.pipeline_id, op)
        if s.op_fail_count.get(k, 0) >= s.max_total_fail_per_op:
            # Give up on this pipeline (likely deterministic failure).
            s.pipeline_blacklisted.add(p.pipeline_id)
            continue
        if s.op_nonoom_fail.get(k, 0) > s.max_nonoom_fail_per_op:
            s.pipeline_blacklisted.add(p.pipeline_id)
            continue

        # If estimator says it can't fit anywhere, skip for now (prevents pointless OOM churn).
        if not _fits_any_pool(p.pipeline_id, op):
            continue

        est_mem = _get_est_mem_gb(op)
        # Prefer smaller memory footprint within same priority to improve packing.
        mem_sort = float("inf") if est_mem is None else float(est_mem)
        candidate_list.append((p, op, mem_sort))

    # If nothing is runnable, do nothing.
    if not candidate_list:
        return suspensions, assignments

    # Greedy placement per pool:
    # - Higher priority first globally; within same priority, smaller estimated memory first.
    # - For each pool, keep assigning while resources allow.
    candidate_list.sort(key=lambda x: (-_prio_rank(x[0].priority), x[2]))

    # Track which pipelines have been scheduled this tick to reduce head-of-line blocking.
    scheduled_pipeline_ids = set()

    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        # Snapshot available resources; we will update locally as we assign.
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)
        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        made_progress = True
        while made_progress:
            made_progress = False

            # Choose the best candidate that fits this pool now.
            best_idx = None
            best_score = None

            for idx, (p, op, mem_sort) in enumerate(candidate_list):
                if p.pipeline_id in scheduled_pipeline_ids and _prio_rank(p.priority) < 3:
                    # Allow multiple concurrent QUERY ops, but be conservative for others.
                    continue

                ram_req = _ram_request_gb(p.pipeline_id, op, pool)
                if ram_req > avail_ram:
                    continue

                # Compute CPU request:
                # - Try to give a meaningful slice based on priority, but don't exceed availability.
                cpu_target = max(1.0, float(pool.max_cpu_pool) * _prio_cpu_fraction(p.priority))
                cpu_req = min(avail_cpu, cpu_target)
                if cpu_req <= 0:
                    continue

                # Score: priority dominates; then prefer tighter RAM fit (to reduce fragmentation).
                # Lower score is better.
                slack = avail_ram - ram_req
                score = (-_prio_rank(p.priority), slack, mem_sort)
                if best_score is None or score < best_score:
                    best_score = score
                    best_idx = idx

            if best_idx is None:
                break

            p, op, _ = candidate_list.pop(best_idx)
            ram_req = _ram_request_gb(p.pipeline_id, op, pool)
            cpu_target = max(1.0, float(pool.max_cpu_pool) * _prio_cpu_fraction(p.priority))
            cpu_req = min(avail_cpu, cpu_target)

            # Final fit check
            if ram_req <= avail_ram and cpu_req > 0:
                assignments.append(
                    Assignment(
                        ops=[op],
                        cpu=cpu_req,
                        ram=ram_req,
                        priority=p.priority,
                        pool_id=pool_id,
                        pipeline_id=p.pipeline_id,
                    )
                )
                avail_cpu -= cpu_req
                avail_ram -= ram_req
                scheduled_pipeline_ids.add(p.pipeline_id)
                made_progress = True

            # Stop if this pool is exhausted
            if avail_cpu <= 0 or avail_ram <= 0:
                break

    return suspensions, assignments
