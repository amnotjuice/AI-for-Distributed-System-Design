# policy_key: scheduler_est_019
# reasoning_effort: low
# exp: estimation
# model: gpt-5.2-2025-12-11
# llm_cost: 0.000000
# generation_seconds: 51.36
# generated_at: 2026-04-10T09:59:29.723140
@register_scheduler_init(key="scheduler_est_019")
def scheduler_est_019_init(s):
    """Memory-aware, priority-first packing scheduler.

    Core ideas:
      - Keep separate FIFO queues per priority (QUERY > INTERACTIVE > BATCH).
      - Within each pool tick, pack multiple ready operators while resources last.
      - Use op.estimate.mem_peak_gb as a noisy hint to (a) avoid infeasible placements and
        (b) prefer smaller-memory ops to reduce OOM cascades and fragmentation.
      - On OOM-like failures, retry the same pipeline with increased RAM headroom (per-op backoff).
      - Add light aging so BATCH eventually makes progress without harming tail latency too much.
    """
    s.q_query = []
    s.q_interactive = []
    s.q_batch = []

    # Per-(pipeline_id, op) multiplicative RAM backoff for retries after OOM-like failures.
    s.mem_backoff = {}  # key: (pipeline_id, op) -> float

    # Per-pipeline OOM retry count to avoid infinite churn (still try hard to complete).
    s.oom_retries = {}  # pipeline_id -> int

    # Pipeline arrival tick for aging.
    s.arrival_tick = {}  # pipeline_id -> int

    # Logical clock.
    s.tick = 0


@register_scheduler(key="scheduler_est_019")
def scheduler_est_019_scheduler(s, results, pipelines):
    """
    Returns:
      suspensions: none (this policy avoids preemption to reduce churn / wasted work)
      assignments: multiple per pool per tick (packing while resources permit)
    """
    # -------- helpers (kept inside for single-file policy portability) --------
    def _now():
        return s.tick

    def _enqueue_pipeline(p):
        pid = p.pipeline_id
        if pid not in s.arrival_tick:
            s.arrival_tick[pid] = _now()

        if p.priority == Priority.QUERY:
            s.q_query.append(p)
        elif p.priority == Priority.INTERACTIVE:
            s.q_interactive.append(p)
        else:
            s.q_batch.append(p)

    def _pipeline_done_or_irrecoverable(p):
        st = p.runtime_status()
        if st.is_pipeline_successful():
            return True
        # Do not drop on mere operator failure; we want high completion rate.
        return False

    def _has_assignable_op(p):
        st = p.runtime_status()
        ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        return ops[0] if ops else None

    def _is_oom_error(err):
        if err is None:
            return False
        msg = str(err).lower()
        return ("oom" in msg) or ("out of memory" in msg) or ("cuda out of memory" in msg) or ("memoryerror" in msg)

    def _get_est_mem_gb(op):
        # Estimator may be missing; treat as unknown.
        try:
            est = op.estimate.mem_peak_gb
        except Exception:
            est = None
        if est is None:
            return None
        try:
            estf = float(est)
            if estf < 0:
                return None
            return estf
        except Exception:
            return None

    def _ram_request_gb(pipeline_id, op, pool, priority):
        # Base estimate
        est_mem = _get_est_mem_gb(op)

        # Priority-dependent headroom: protect high-priority from OOM retries (which cost latency)
        if priority == Priority.QUERY:
            headroom = 1.35
            unknown_base = max(1.0, 0.30 * float(pool.max_ram_pool))
        elif priority == Priority.INTERACTIVE:
            headroom = 1.30
            unknown_base = max(1.0, 0.25 * float(pool.max_ram_pool))
        else:
            headroom = 1.20
            unknown_base = max(1.0, 0.20 * float(pool.max_ram_pool))

        base = (est_mem * headroom) if (est_mem is not None) else unknown_base

        # Backoff on prior OOMs for this exact op within this pipeline.
        backoff = s.mem_backoff.get((pipeline_id, op), 1.0)

        # Clamp: never request > pool max, but also keep a small floor.
        req = max(1.0, min(float(pool.max_ram_pool), base * backoff))
        return req, est_mem

    def _cpu_request(p, pool, avail_cpu):
        # Give higher priority a larger CPU slice to reduce latency; keep batch smaller for packing.
        max_cpu = int(pool.max_cpu_pool)
        if max_cpu <= 0:
            return 0

        if p.priority == Priority.QUERY:
            target = max(1, int(round(0.60 * max_cpu)))
        elif p.priority == Priority.INTERACTIVE:
            target = max(1, int(round(0.45 * max_cpu)))
        else:
            target = max(1, int(round(0.30 * max_cpu)))

        return int(min(int(avail_cpu), target))

    def _aging_boost(p):
        # Aging score: BATCH can bubble up if it has waited a long time.
        waited = _now() - s.arrival_tick.get(p.pipeline_id, _now())
        # Slow aging to avoid hurting QUERY/INTERACTIVE tails.
        return waited

    def _select_candidate_for_pool(pool, avail_cpu, avail_ram):
        """
        Choose a (pipeline, op, req_cpu, req_ram) that fits current pool resources.

        Selection policy:
          1) Prefer QUERY, then INTERACTIVE, then BATCH.
          2) Within the same priority class, prefer smaller estimated memory (packing).
          3) Add light aging for BATCH so it isn't starved forever.
        """
        # Scan limited window to avoid O(n) across huge queues.
        SCAN_N = 20

        # Build priority-ordered queue list with slight aging injection for batch.
        queue_specs = [
            (Priority.QUERY, s.q_query),
            (Priority.INTERACTIVE, s.q_interactive),
            (Priority.BATCH, s.q_batch),
        ]

        best = None
        best_meta = None  # (prio_rank, est_mem_or_inf, -aging, index_in_queue)
        for prio_rank, (prio, q) in enumerate(queue_specs):
            if not q:
                continue

            scan_lim = min(SCAN_N, len(q))
            for idx in range(scan_lim):
                p = q[idx]
                if _pipeline_done_or_irrecoverable(p):
                    continue

                op = _has_assignable_op(p)
                if op is None:
                    continue

                req_cpu = _cpu_request(p, pool, avail_cpu)
                if req_cpu <= 0 or req_cpu > avail_cpu:
                    continue

                req_ram, est_mem = _ram_request_gb(p.pipeline_id, op, pool, p.priority)

                # Quick infeasibility check: if estimate exists and exceeds pool max, skip this pool.
                if est_mem is not None and est_mem > float(pool.max_ram_pool):
                    continue

                if req_ram > avail_ram:
                    continue

                # Rank: priority first, then smaller est mem (unknown treated as larger),
                # then (for batch only) older first.
                est_key = est_mem if est_mem is not None else float("inf")
                age = _aging_boost(p) if prio == Priority.BATCH else 0
                meta = (prio_rank, est_key, -age, idx)

                if best is None or meta < best_meta:
                    best = (p, op, req_cpu, req_ram)
                    best_meta = meta

        return best

    def _remove_from_queue(q, index):
        # Remove q[index] without importing deque.
        p = q[index]
        del q[index]
        return p

    def _requeue_back(p):
        # Keep arrival_tick unchanged for aging.
        if p.priority == Priority.QUERY:
            s.q_query.append(p)
        elif p.priority == Priority.INTERACTIVE:
            s.q_interactive.append(p)
        else:
            s.q_batch.append(p)

    def _compact_queues():
        # Remove pipelines that have completed to avoid repeated scanning.
        def _compact(q):
            newq = []
            for p in q:
                if not _pipeline_done_or_irrecoverable(p):
                    newq.append(p)
            return newq

        s.q_query = _compact(s.q_query)
        s.q_interactive = _compact(s.q_interactive)
        s.q_batch = _compact(s.q_batch)

    # -------- tick bookkeeping --------
    s.tick += 1

    # Add newly arrived pipelines.
    for p in pipelines:
        _enqueue_pipeline(p)

    # Process execution results: update OOM backoff on failures, especially for high-priority.
    for r in results:
        if not r.failed():
            continue
        # We only apply aggressive backoff for OOM-like errors; other failures may be logic/IO etc.
        if not _is_oom_error(r.error):
            continue

        # Increase retry counters per pipeline.
        # r.ops is list of ops for the container; typically one in our assignments.
        pid = getattr(r, "pipeline_id", None)  # may not exist; use r.ops' pipeline association if absent
        # Fall back: if pipeline_id not available on result, we can only backoff per-op object.
        # We'll still increment a generic retry counter keyed by (container_id, pool_id) if needed.
        if pid is None:
            pid = ("unknown", r.container_id, r.pool_id)

        s.oom_retries[pid] = int(s.oom_retries.get(pid, 0)) + 1

        # Per-op memory backoff; cap to avoid requesting absurd RAM and blocking forever.
        for op in getattr(r, "ops", []) or []:
            key = (pid, op)
            cur = float(s.mem_backoff.get(key, 1.0))
            # Larger bump for QUERY/INTERACTIVE to reduce repeated 720s penalties via failure.
            if r.priority == Priority.QUERY:
                mul = 1.90
            elif r.priority == Priority.INTERACTIVE:
                mul = 1.75
            else:
                mul = 1.55
            s.mem_backoff[key] = min(6.0, cur * mul)

    # Quick cleanup if nothing changed.
    if not pipelines and not results:
        return [], []

    # Remove completed pipelines from queues to reduce scan overhead.
    _compact_queues()

    suspensions = []
    assignments = []

    # Pack multiple assignments per pool per tick using memory-aware candidate selection.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = int(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)

        # If pool has no resources, skip.
        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # Pack until resources are effectively exhausted or no fitting work exists.
        while avail_cpu > 0 and avail_ram > 0:
            cand = _select_candidate_for_pool(pool, avail_cpu, avail_ram)
            if cand is None:
                break

            p, op, req_cpu, req_ram = cand

            # Remove selected pipeline from its queue (from within first SCAN_N window we searched).
            # We requeue it after assignment so subsequent ops can be scheduled later fairly.
            if p.priority == Priority.QUERY:
                q = s.q_query
            elif p.priority == Priority.INTERACTIVE:
                q = s.q_interactive
            else:
                q = s.q_batch

            # Find and remove first occurrence (queues are small-ish; scanned window already limited).
            removed = False
            for i in range(min(len(q), 50)):
                if q[i].pipeline_id == p.pipeline_id:
                    _remove_from_queue(q, i)
                    removed = True
                    break
            if not removed:
                # If not found (should be rare), continue without assignment to avoid inconsistency.
                break

            # Issue assignment for a single operator (atomic scheduling unit).
            assignments.append(
                Assignment(
                    ops=[op],
                    cpu=int(req_cpu),
                    ram=float(req_ram),
                    priority=p.priority,
                    pool_id=pool_id,
                    pipeline_id=p.pipeline_id,
                )
            )

            # Decrement local availability for packing.
            avail_cpu -= int(req_cpu)
            avail_ram -= float(req_ram)

            # Requeue pipeline to the back (FIFO within priority) if it's not done.
            if not _pipeline_done_or_irrecoverable(p):
                _requeue_back(p)

    return suspensions, assignments
