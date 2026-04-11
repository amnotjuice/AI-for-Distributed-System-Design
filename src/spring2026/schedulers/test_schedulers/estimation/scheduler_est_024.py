# policy_key: scheduler_est_024
# reasoning_effort: low
# exp: estimation
# model: gpt-5.2-2025-12-11
# llm_cost: 0.055552
# generation_seconds: 45.56
# generated_at: 2026-04-10T10:03:51.647746
@register_scheduler_init(key="scheduler_est_024")
def scheduler_est_024_init(s):
    """Memory-aware, priority-weighted scheduler.

    Core ideas:
      - Maintain per-priority FIFO queues (QUERY > INTERACTIVE > BATCH) with light aging to avoid starvation.
      - Use op.estimate.mem_peak_gb (when available) to (a) pre-filter infeasible pools and (b) size RAM requests.
      - On failures, pessimistically increase a per-operator RAM multiplier and retry (reduces repeated OOMs).
      - Pack multiple small operators per pool per tick (favor low estimated memory) to reduce head-of-line blocking.
      - Avoid preemption (keeps churn low, reduces wasted work); focus on high completion rate + low latency.
    """
    s.q_query = []
    s.q_interactive = []
    s.q_batch = []

    # pipeline_id -> first-seen tick (for aging)
    s.pipeline_first_seen_tick = {}

    # operator identity (id(op)) -> RAM multiplier (>= 1.0), boosted after failures
    s.op_ram_mult = {}

    # operator identity (id(op)) -> recent failure tick (optional; can be used for backoff)
    s.op_last_fail_tick = {}

    # simple logical tick counter (increments each scheduler call)
    s._tick = 0


@register_scheduler(key="scheduler_est_024")
def scheduler_est_024(s, results, pipelines):
    """
    Scheduler step:
      1) Ingest new pipelines into per-priority queues.
      2) Process results: on failure, increase RAM multiplier for failed ops.
      3) For each pool, repeatedly select next best (priority+aging) pipeline with an assignable op that fits,
         preferring smaller estimated memory ops to improve packing and reduce OOM risk.
      4) Create Assignments sized by estimates (or conservative defaults when unknown).
    """
    s._tick += 1

    def _prio_weight(priority):
        if priority == Priority.QUERY:
            return 3
        if priority == Priority.INTERACTIVE:
            return 2
        return 1

    def _queue_for(priority):
        if priority == Priority.QUERY:
            return s.q_query
        if priority == Priority.INTERACTIVE:
            return s.q_interactive
        return s.q_batch

    def _is_done_or_dead(p):
        st = p.runtime_status()
        if st.is_pipeline_successful():
            return True
        # Drop pipelines that already have failed operators (baseline behavior); however, to avoid heavy penalties
        # from abandonment, we *do not* drop on failures here. We keep retrying by re-queuing.
        return False

    def _get_assignable_op(p):
        st = p.runtime_status()
        op_list = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        if not op_list:
            return None
        # Only schedule one operator at a time per pipeline to reduce blast radius of bad sizing.
        return op_list[0]

    def _est_mem_gb(op):
        try:
            est = op.estimate.mem_peak_gb
            return est
        except Exception:
            return None

    def _ram_mult(op):
        return s.op_ram_mult.get(id(op), 1.0)

    def _bump_ram_mult(op):
        # Aggressive enough to escape repeated OOMs, but capped to avoid runaway.
        k = id(op)
        cur = s.op_ram_mult.get(k, 1.0)
        new = cur * 1.8
        if new > 16.0:
            new = 16.0
        s.op_ram_mult[k] = new
        s.op_last_fail_tick[k] = s._tick

    def _default_ram_request(pool, priority):
        # Conservative sizing when estimator is missing: prioritize completion for high-priority.
        # Values are fractions of pool max; clamped to [1GB, max_ram].
        if priority == Priority.QUERY:
            frac = 0.33
        elif priority == Priority.INTERACTIVE:
            frac = 0.25
        else:
            frac = 0.15
        req = pool.max_ram_pool * frac
        if req < 1.0:
            req = 1.0
        if req > pool.max_ram_pool:
            req = pool.max_ram_pool
        return req

    def _ram_request(pool, op, priority):
        est = _est_mem_gb(op)
        mult = _ram_mult(op)

        if est is None:
            return _default_ram_request(pool, priority)

        # Safety factor to absorb estimator noise; higher for high priority to avoid 720s penalties.
        if priority == Priority.QUERY:
            safety = 1.35
        elif priority == Priority.INTERACTIVE:
            safety = 1.25
        else:
            safety = 1.15

        req = est * mult * safety

        # Clamp to [1GB, pool.max_ram_pool]
        if req < 1.0:
            req = 1.0
        if req > pool.max_ram_pool:
            req = pool.max_ram_pool
        return req

    def _cpu_request(pool, priority, avail_cpu, avail_ram):
        # Avoid giving the whole pool to a single op; keep headroom for parallelism.
        if priority == Priority.QUERY:
            cap = max(1.0, pool.max_cpu_pool * 0.60)
        elif priority == Priority.INTERACTIVE:
            cap = max(1.0, pool.max_cpu_pool * 0.45)
        else:
            cap = max(1.0, pool.max_cpu_pool * 0.30)

        cpu = min(avail_cpu, cap)

        # Ensure at least 1 vCPU if any CPU is available.
        if cpu < 1.0 and avail_cpu >= 1.0:
            cpu = 1.0
        return cpu

    def _age_boost(pipeline):
        # Light aging: if a pipeline has waited "too long", it gains effective weight
        # to reduce chance of indefinite incompletion (which is heavily penalized).
        first = s.pipeline_first_seen_tick.get(pipeline.pipeline_id, s._tick)
        waited = s._tick - first
        if waited >= 120:
            return 2
        if waited >= 60:
            return 1
        return 0

    # 1) Ingest new pipelines
    for p in pipelines:
        if p.pipeline_id not in s.pipeline_first_seen_tick:
            s.pipeline_first_seen_tick[p.pipeline_id] = s._tick
        _queue_for(p.priority).append(p)

    # 2) Process results: bump RAM multiplier on failures (treat all failures as potential OOM to be safe)
    for r in results:
        if hasattr(r, "failed") and r.failed():
            for op in getattr(r, "ops", []) or []:
                _bump_ram_mult(op)

    # Early exit
    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    # Build a unified list for scanning and re-queueing each round.
    all_queues = [s.q_query, s.q_interactive, s.q_batch]

    # 3) For each pool, pack as many assignments as feasible.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool
        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # Limit per-pool assignments per tick to avoid over-scheduling and to reduce bad packing.
        max_assignments_this_pool = 6
        made = 0

        while made < max_assignments_this_pool and avail_cpu > 0 and avail_ram > 0:
            # Candidate selection:
            # - Prefer higher priority, but allow aging to elevate long-waiting work.
            # - Among same effective priority, prefer smaller estimated memory to improve packing.
            best = None  # (score_tuple, queue_idx, pipeline_idx, pipeline, op, ram_req, cpu_req)
            for q_idx, q in enumerate(all_queues):
                # Scan a bounded prefix to keep runtime predictable.
                scan_n = 12
                n = len(q) if len(q) < scan_n else scan_n
                for i in range(n):
                    p = q[i]
                    if _is_done_or_dead(p):
                        continue

                    op = _get_assignable_op(p)
                    if op is None:
                        continue

                    # Pool feasibility using estimate (hint): if estimate exceeds pool max, skip this pool.
                    est = _est_mem_gb(op)
                    if est is not None and est > pool.max_ram_pool:
                        continue

                    ram_req = _ram_request(pool, op, p.priority)
                    if ram_req > avail_ram:
                        continue

                    cpu_req = _cpu_request(pool, p.priority, avail_cpu, avail_ram)
                    if cpu_req <= 0:
                        continue

                    # Build a sortable key:
                    # Higher effective priority first; then smaller RAM; then FIFO (older first).
                    eff = _prio_weight(p.priority) + _age_boost(p)
                    first = s.pipeline_first_seen_tick.get(p.pipeline_id, s._tick)
                    waited = s._tick - first
                    # Use est if present else ram_req as a proxy for footprint.
                    footprint = est if est is not None else ram_req
                    key = (-eff, footprint, -waited, i)

                    cand = (key, q_idx, i, p, op, ram_req, cpu_req)
                    if best is None or cand[0] < best[0]:
                        best = cand

            if best is None:
                break

            _, q_idx, i, p, op, ram_req, cpu_req = best

            # Create assignment
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

            # Update available resources optimistically (simulation will enforce actual).
            avail_cpu -= cpu_req
            avail_ram -= ram_req
            made += 1

            # Rotate the chosen pipeline to the back of its queue to avoid head-of-line blocking.
            q = all_queues[q_idx]
            # Remove by index i (bounded scan ensures O(1..12) shifts).
            chosen = q.pop(i)
            q.append(chosen)

    # 4) Cleanup: remove completed pipelines from queue heads opportunistically (bounded).
    for q in all_queues:
        # Bounded cleanup to keep step time stable.
        cleanup_n = 16
        j = 0
        while j < len(q) and j < cleanup_n:
            p = q[j]
            if p.runtime_status().is_pipeline_successful():
                q.pop(j)
                continue
            j += 1

    return suspensions, assignments
