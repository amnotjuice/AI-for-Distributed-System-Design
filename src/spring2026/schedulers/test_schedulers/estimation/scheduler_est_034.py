# policy_key: scheduler_est_034
# reasoning_effort: low
# exp: estimation
# model: gpt-5.2-2025-12-11
# llm_cost: 0.044646
# generation_seconds: 55.12
# generated_at: 2026-04-10T10:12:59.743110
@register_scheduler_init(key="scheduler_est_034")
def scheduler_est_034_init(s):
    """
    Memory-aware, priority-weighted scheduler (query > interactive > batch) that aims to:
      - reduce OOM-driven failures by using op.estimate.mem_peak_gb as a noisy hint
      - keep high-priority latency low via preferential service + mild aging for fairness
      - avoid dropping failed pipelines (retry with higher RAM after failures, capped)
      - improve utilization by packing multiple small ops per pool when possible

    Key ideas:
      - Maintain per-priority FIFO queues with aging.
      - Build a pool-local candidate set and choose the "best-fit" (smallest estimated RAM that fits).
      - On failures, increase a per-pipeline RAM boost factor and re-attempt (bounded retries).
    """
    s.tick = 0

    # Priority queues (store pipeline ids to avoid duplicates)
    s.q_query = []
    s.q_interactive = []
    s.q_batch = []

    # Pipeline registry and metadata
    s.pipelines_by_id = {}  # pipeline_id -> Pipeline
    s.meta = {}  # pipeline_id -> dict with arrival_tick, last_enqueued_tick, fail_count, ram_boost

    # Track membership to prevent duplicate enqueues
    s.in_queue = set()

    # Tuning knobs (conservative; incremental improvement over FIFO)
    s.max_scan_per_queue = 32
    s.max_retries = 3  # after this, stop retrying to avoid infinite churn

    # RAM sizing heuristics
    s.mem_safety = 1.25          # multiplicative safety margin on estimator
    s.mem_floor_gb = 0.5         # minimum RAM request when estimator is None/small
    s.mem_headroom_frac = 0.05   # keep small headroom to reduce fragmentation/OOM risk

    # CPU sizing heuristics (favor concurrency; don't give everything to one op)
    s.cpu_caps = {
        Priority.QUERY: 6.0,
        Priority.INTERACTIVE: 4.0,
        Priority.BATCH_PIPELINE: 2.0,
    }
    s.cpu_floor = 1.0


@register_scheduler(key="scheduler_est_034")
def scheduler_est_034_scheduler(s, results, pipelines):
    """
    Returns suspensions (none in this policy) and assignments.

    Scheduling loop:
      1) Ingest new pipelines into per-priority queues.
      2) Process results: update failure metadata; keep failed pipelines eligible for retry.
      3) For each pool, repeatedly pick the best-fit ready operator among the highest effective
         priority class that has any operator fitting current pool resources.
    """
    s.tick += 1

    suspensions = []
    assignments = []

    # ----------------------------
    # Helpers (local, no imports)
    # ----------------------------
    def _prio_queue(priority):
        if priority == Priority.QUERY:
            return s.q_query
        if priority == Priority.INTERACTIVE:
            return s.q_interactive
        return s.q_batch

    def _prio_weight(priority):
        # Aligns with objective weights; used for queue order preference.
        if priority == Priority.QUERY:
            return 10
        if priority == Priority.INTERACTIVE:
            return 5
        return 1

    def _enqueue_pipeline(p):
        pid = p.pipeline_id
        s.pipelines_by_id[pid] = p
        if pid not in s.meta:
            s.meta[pid] = {
                "arrival_tick": s.tick,
                "last_enqueued_tick": s.tick,
                "fail_count": 0,
                "ram_boost": 1.0,  # increased on failures
            }
        else:
            s.meta[pid]["last_enqueued_tick"] = s.tick

        if pid in s.in_queue:
            return
        _prio_queue(p.priority).append(pid)
        s.in_queue.add(pid)

    def _drop_from_all_queues(pid):
        # Remove pid from all queues if present (O(n) but bounded by queue sizes).
        if pid in s.in_queue:
            s.in_queue.remove(pid)
        for q in (s.q_query, s.q_interactive, s.q_batch):
            if not q:
                continue
            try:
                q.remove(pid)
            except ValueError:
                pass

    def _pipeline_done_or_terminal(p):
        st = p.runtime_status()
        if st.is_pipeline_successful():
            return True
        # Terminal failure is handled by retry cap logic; keep schedulable until cap reached.
        return False

    def _first_ready_op(p):
        st = p.runtime_status()
        # Only operators whose parents are complete
        ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        if not ops:
            return None
        # Only schedule one operator per pipeline at a time (conservative; reduces interference)
        return ops[0]

    def _op_est_mem_gb(op):
        est = None
        try:
            if op.estimate is not None:
                est = op.estimate.mem_peak_gb
        except Exception:
            est = None
        if est is None:
            return None
        # Guard against non-positive or pathological values
        if est <= 0:
            return None
        return float(est)

    def _ram_request_gb(p, op, pool):
        pid = p.pipeline_id
        est = _op_est_mem_gb(op)
        boost = s.meta.get(pid, {}).get("ram_boost", 1.0)

        # Base request from estimator or floor
        if est is None:
            base = s.mem_floor_gb
        else:
            base = est * s.mem_safety

        req = base * boost

        # Never request more than pool max; if we do, it will never fit this pool.
        if req > pool.max_ram_pool:
            req = pool.max_ram_pool

        # Keep tiny headroom so we don't pack to the absolute edge.
        headroom = max(pool.max_ram_pool * s.mem_headroom_frac, 0.0)
        # For placement we compare against available - headroom; request itself shouldn't include headroom.
        return max(req, s.mem_floor_gb), headroom

    def _cpu_request(p, pool):
        cap = s.cpu_caps.get(p.priority, 2.0)
        # Give more CPU if pool is idle, but keep cap to encourage concurrency.
        cpu = min(pool.avail_cpu_pool, cap)
        return max(min(cpu, pool.max_cpu_pool), s.cpu_floor)

    def _effective_class_order():
        # Prefer higher priority, but if higher-priority queue is empty/unready, fall through.
        return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]

    def _score_pid_for_fairness(pid):
        # Lower is better (we choose minimum): higher priority weight and longer waiting should win.
        p = s.pipelines_by_id.get(pid)
        if p is None:
            return float("inf")
        meta = s.meta.get(pid, {})
        waited = s.tick - meta.get("last_enqueued_tick", s.tick)
        # Convert to a score where more waiting reduces score (wins),
        # and higher priority class reduces score more strongly.
        w = _prio_weight(p.priority)
        return -(waited * w)

    # ----------------------------
    # Ingest new pipelines
    # ----------------------------
    for p in pipelines:
        _enqueue_pipeline(p)

    # ----------------------------
    # Process results (update retry logic)
    # ----------------------------
    # If we see failures, assume they might be OOM-like and bump RAM boost.
    # We do NOT drop pipelines on first failure; we retry up to s.max_retries.
    for r in results:
        for op in getattr(r, "ops", []) or []:
            pid = getattr(op, "pipeline_id", None)
            if pid is None:
                continue
            if pid not in s.meta:
                # Pipeline might have arrived earlier but not tracked (defensive)
                p = s.pipelines_by_id.get(pid)
                if p is not None:
                    s.meta[pid] = {
                        "arrival_tick": s.tick,
                        "last_enqueued_tick": s.tick,
                        "fail_count": 0,
                        "ram_boost": 1.0,
                    }

            if hasattr(r, "failed") and r.failed():
                m = s.meta.get(pid)
                if m is None:
                    continue
                m["fail_count"] += 1
                # Mild ramp; avoid explosive growth.
                # After a few failures, this quickly reaches ~2x-3x.
                m["ram_boost"] = min(m["ram_boost"] * 1.6, 4.0)

                # Ensure it's enqueued for retry if not already present.
                p = s.pipelines_by_id.get(pid)
                if p is not None and pid not in s.in_queue:
                    _enqueue_pipeline(p)

    # ----------------------------
    # Garbage collect completed pipelines from queues/metadata
    # ----------------------------
    # We scan queues and remove completed; bounded by queue lengths.
    for q in (s.q_query, s.q_interactive, s.q_batch):
        if not q:
            continue
        keep = []
        for pid in q:
            p = s.pipelines_by_id.get(pid)
            if p is None:
                continue
            if _pipeline_done_or_terminal(p):
                # completed; remove tracking to keep state small
                s.in_queue.discard(pid)
                s.meta.pop(pid, None)
                s.pipelines_by_id.pop(pid, None)
                continue
            keep.append(pid)
        q[:] = keep
        # Rebuild in_queue membership for this queue portion (avoid drift)
        # (We keep s.in_queue as a quick "any queue" membership.)
        for pid in keep:
            s.in_queue.add(pid)

    # Early exit if nothing changed and no pipelines
    if not pipelines and not results and not (s.q_query or s.q_interactive or s.q_batch):
        return suspensions, assignments

    # ----------------------------
    # Main assignment loop (per pool)
    # ----------------------------
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]

        # Work with local mutable resource snapshots
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool

        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # Keep scheduling into this pool while we can place at least one op.
        # (Bounded by resource exhaustion and queue sizes.)
        while avail_cpu > 0 and avail_ram > 0:
            chosen = None  # tuple(pipeline, op, cpu_req, ram_req)
            chosen_pid = None

            # Search by priority class, but within each class pick best-fit by estimated RAM
            # to reduce fragmentation and OOM risk.
            for pr in _effective_class_order():
                q = _prio_queue(pr)
                if not q:
                    continue

                # Consider up to N from this queue: stable + bounded cost.
                scan = q[: s.max_scan_per_queue]

                # Optional fairness: among scanned pids, sort by (fairness score, FIFO position)
                # Negative score means "waited longer" (wins).
                scan_sorted = sorted(
                    enumerate(scan),
                    key=lambda t: (_score_pid_for_fairness(t[1]), t[0]),
                )

                best_fit = None  # (est_mem, idx_in_queue, pid, p, op, cpu_req, ram_req)
                for idx_in_scan, pid in scan_sorted:
                    p = s.pipelines_by_id.get(pid)
                    if p is None:
                        continue

                    # Retry cap: after too many failures, don't churn forever.
                    m = s.meta.get(pid, {})
                    if m.get("fail_count", 0) > s.max_retries:
                        # Let it sit out (still counts against objective, but avoids wasting cluster)
                        # Remove from queue to reduce overhead.
                        _drop_from_all_queues(pid)
                        s.meta.pop(pid, None)
                        s.pipelines_by_id.pop(pid, None)
                        continue

                    op = _first_ready_op(p)
                    if op is None:
                        # Not ready yet; keep in queue
                        continue

                    ram_req, headroom = _ram_request_gb(p, op, pool)
                    # Must fit in current pool availability (with headroom).
                    if ram_req > max(avail_ram - headroom, 0.0):
                        continue

                    cpu_req = _cpu_request(p, pool)
                    if cpu_req > avail_cpu:
                        # If CPU is tight, allow a smaller CPU down to floor to avoid stalling.
                        cpu_req = max(min(avail_cpu, cpu_req), s.cpu_floor)
                        if cpu_req > avail_cpu:
                            continue

                    # Best-fit by estimated RAM (smaller first); estimator None treated as medium.
                    est = _op_est_mem_gb(op)
                    est_for_sort = est if est is not None else (ram_req * 1.5)

                    cand = (est_for_sort, idx_in_scan, pid, p, op, cpu_req, ram_req)
                    if best_fit is None or cand < best_fit:
                        best_fit = cand

                if best_fit is not None:
                    _, _, pid, p, op, cpu_req, ram_req = best_fit
                    chosen = (p, op, cpu_req, ram_req)
                    chosen_pid = pid
                    break  # don't look at lower-priority classes if we found a fit here

            if chosen is None:
                break  # no operator fits in this pool right now

            p, op, cpu_req, ram_req = chosen

            # Create assignment (single op)
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

            # Update local resource snapshots (so we can pack multiple ops)
            avail_cpu -= cpu_req
            avail_ram -= ram_req

            # FIFO behavior: move chosen pipeline to back of its priority queue
            # to avoid one pipeline monopolizing scheduling decisions.
            if chosen_pid is not None:
                q = _prio_queue(p.priority)
                try:
                    q.remove(chosen_pid)
                except ValueError:
                    pass
                q.append(chosen_pid)
                s.in_queue.add(chosen_pid)

    return suspensions, assignments
