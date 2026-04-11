# policy_key: scheduler_est_009
# reasoning_effort: low
# exp: estimation
# model: gpt-5.2-2025-12-11
# llm_cost: 0.050582
# generation_seconds: 53.04
# generated_at: 2026-04-10T07:53:39.322631
@register_scheduler_init(key="scheduler_est_009")
def scheduler_est_009_init(s):
    """
    Memory-aware, priority-biased packed scheduler.

    Goals:
    - Protect QUERY and INTERACTIVE latency via strict priority ordering and soft reservation for batch.
    - Reduce OOM failures by sizing RAM using op.estimate.mem_peak_gb (noisy hint) + adaptive bump on failures.
    - Improve throughput vs FIFO by packing multiple operators per pool per tick when resources allow.
    """
    from collections import deque

    # Separate FIFO queues by priority (keeps ordering stable within each class).
    s.q_query = deque()
    s.q_interactive = deque()
    s.q_batch = deque()

    # Adaptive per-operator RAM hint keyed by (pipeline_id, op_object_id).
    # Updated on failures (especially OOM-like).
    s.op_ram_hint_gb = {}

    # Track failure counts to avoid pathological tiny increments.
    s.op_fail_count = {}

    # Tuning knobs (kept conservative).
    s.mem_safety_mult = 1.25          # inflate estimator a bit
    s.mem_safety_add_gb = 0.5         # small additive guard
    s.oom_backoff_mult = 2.0          # bump RAM on suspected OOM
    s.max_retries_soft = 6            # after this, just request pool max RAM for the op

    # Soft reservations to prevent batch from consuming the whole pool when higher priority may arrive.
    # These are *only* applied when selecting batch work.
    s.batch_reserve_cpu_frac = 0.35
    s.batch_reserve_ram_frac = 0.35

    # Limit search per pool per tick to keep runtime small/deterministic.
    s.scan_window_per_prio = 8


@register_scheduler(key="scheduler_est_009")
def scheduler_est_009_scheduler(s, results, pipelines):
    """
    Scheduling loop:
    - Enqueue new pipelines by priority.
    - Update RAM hints on failures (OOM-detection heuristic).
    - For each pool, repeatedly pick the best "ready" op (parents complete) from the highest priority queue
      that can fit in the pool (RAM+CPU), preferring smaller estimated memory to pack better.
    - Batch work is admitted only if pool headroom remains beyond a soft reserve.
    """
    from collections import deque

    def _prio_queue(priority):
        if priority == Priority.QUERY:
            return s.q_query
        if priority == Priority.INTERACTIVE:
            return s.q_interactive
        return s.q_batch

    def _is_oom_like(err) -> bool:
        if err is None:
            return False
        try:
            msg = str(err).lower()
        except Exception:
            return False
        return ("oom" in msg) or ("out of memory" in msg) or ("cuda out of memory" in msg) or ("memoryerror" in msg)

    def _op_key(pipeline_id, op):
        return (pipeline_id, id(op))

    def _est_mem_gb(op):
        est = getattr(op, "estimate", None)
        if est is None:
            return None
        v = getattr(est, "mem_peak_gb", None)
        if v is None:
            return None
        try:
            v = float(v)
        except Exception:
            return None
        if v < 0:
            return None
        return v

    def _base_ram_request(pool, op):
        # Use estimator when available; otherwise choose a conservative-but-not-huge default.
        est = _est_mem_gb(op)
        if est is None:
            # Default: 20% of pool RAM capped to [2, 8] GB to avoid massive overallocation.
            guess = 0.20 * float(pool.max_ram_pool)
            return max(2.0, min(8.0, guess))
        return max(1.0, est * s.mem_safety_mult + s.mem_safety_add_gb)

    def _ram_request_gb(pool, pipeline_id, op):
        k = _op_key(pipeline_id, op)
        base = _base_ram_request(pool, op)

        # If we have a hint (from prior failures), take the max.
        hinted = s.op_ram_hint_gb.get(k, 0.0)
        req = max(base, float(hinted))

        # After many failures, stop incremental guessing and just request pool max to maximize completion chance.
        fails = s.op_fail_count.get(k, 0)
        if fails >= s.max_retries_soft:
            req = max(req, float(pool.max_ram_pool))

        # Clamp to pool max.
        return min(req, float(pool.max_ram_pool))

    def _cpu_request(pool, priority, avail_cpu):
        # Small fixed allocations encourage packing while still favoring high priority.
        # (Bauplan steps are often scale-up biased, but aggressive CPU grabs can starve others.)
        if priority == Priority.QUERY:
            target = min(4.0, float(pool.max_cpu_pool))
        elif priority == Priority.INTERACTIVE:
            target = min(3.0, float(pool.max_cpu_pool))
        else:
            target = min(2.0, float(pool.max_cpu_pool))
        return max(1.0, min(float(avail_cpu), target))

    def _pipeline_done_or_terminal(p):
        st = p.runtime_status()
        return st.is_pipeline_successful()

    def _get_first_ready_op(p):
        st = p.runtime_status()
        ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        if not ops:
            return None
        return ops[0]

    def _pick_candidate_from_queue(q: deque, pool, pool_id, allow_batch: bool):
        """
        Scan a small window of the queue to find a ready op that fits in this pool.
        Preference: smallest estimated RAM among those that fit (to reduce fragmentation and OOM risk).
        Returns: (pipeline, op, ram_req, cpu_req, idx_in_queue) or None.
        """
        best = None
        best_idx = None
        best_mem = None

        window = min(len(q), s.scan_window_per_prio)
        for i in range(window):
            p = q[i]
            if _pipeline_done_or_terminal(p):
                continue

            op = _get_first_ready_op(p)
            if op is None:
                continue

            # Compute requests.
            ram_req = _ram_request_gb(pool, p.pipeline_id, op)
            if ram_req > float(pool.avail_ram_pool):
                continue

            cpu_req = _cpu_request(pool, p.priority, pool.avail_cpu_pool)
            if cpu_req > float(pool.avail_cpu_pool):
                continue

            # For batch, enforce soft reserve (only evaluated here).
            if (p.priority == Priority.BATCH_PIPELINE) and (not allow_batch):
                continue

            # Compare by estimated RAM (fallback to ram_req) then FIFO index.
            est = _est_mem_gb(op)
            mem_sort = float(est) if est is not None else float(ram_req)

            if (best is None) or (mem_sort < best_mem) or (mem_sort == best_mem and i < best_idx):
                best = (p, op, ram_req, cpu_req, i)
                best_idx = i
                best_mem = mem_sort

        return best

    # Enqueue new pipelines.
    for p in pipelines:
        _prio_queue(p.priority).append(p)

    # Update hints based on execution results.
    # If a container failed with OOM-like error, bump the RAM hint for its ops.
    # Otherwise, keep hints as-is (do not shrink to avoid thrash).
    for r in results:
        if not r.failed():
            continue

        oom_like = _is_oom_like(getattr(r, "error", None))
        for op in getattr(r, "ops", []) or []:
            k = _op_key(getattr(r, "pipeline_id", None) or getattr(op, "pipeline_id", None) or -1, op)

            # If pipeline_id isn't on result/op, fall back to op object id only (best-effort).
            # This keeps the scheduler robust to differing simulator structs.
            if k[0] == -1:
                k = ("unknown", id(op))

            s.op_fail_count[k] = s.op_fail_count.get(k, 0) + 1

            if oom_like:
                # Use observed RAM request as a floor and bump it aggressively.
                prev = float(s.op_ram_hint_gb.get(k, 0.0))
                used = float(getattr(r, "ram", 0.0) or 0.0)
                bumped = max(prev, used, 1.0) * s.oom_backoff_mult
                s.op_ram_hint_gb[k] = bumped

    # Early exit if nothing to do.
    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    # Helper: clean completed pipelines from the front opportunistically.
    def _prune_front(q: deque):
        while q and _pipeline_done_or_terminal(q[0]):
            q.popleft()

    _prune_front(s.q_query)
    _prune_front(s.q_interactive)
    _prune_front(s.q_batch)

    # For each pool, pack as many operators as we can this tick.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]

        # Continue packing until no more feasible candidate fits.
        while True:
            avail_cpu = float(pool.avail_cpu_pool)
            avail_ram = float(pool.avail_ram_pool)
            if avail_cpu <= 0.0 or avail_ram <= 0.0:
                break

            # Decide whether batch is allowed based on soft reserves.
            allow_batch = True
            if (avail_cpu <= float(pool.max_cpu_pool) * s.batch_reserve_cpu_frac) or (
                avail_ram <= float(pool.max_ram_pool) * s.batch_reserve_ram_frac
            ):
                allow_batch = False

            # Try in strict priority order; within each, pick a fit candidate biased to small mem.
            picked = None

            # QUERY
            _prune_front(s.q_query)
            if s.q_query:
                cand = _pick_candidate_from_queue(s.q_query, pool, pool_id, allow_batch=True)
                if cand is not None:
                    picked = cand

            # INTERACTIVE
            if picked is None:
                _prune_front(s.q_interactive)
                if s.q_interactive:
                    cand = _pick_candidate_from_queue(s.q_interactive, pool, pool_id, allow_batch=True)
                    if cand is not None:
                        picked = cand

            # BATCH
            if picked is None:
                _prune_front(s.q_batch)
                if s.q_batch:
                    cand = _pick_candidate_from_queue(s.q_batch, pool, pool_id, allow_batch=allow_batch)
                    if cand is not None:
                        picked = cand

            if picked is None:
                break

            pipeline, op, ram_req, cpu_req, idx = picked

            # Move selected pipeline to back (round-robin within its class) to avoid intra-class starvation.
            q = _prio_queue(pipeline.priority)
            try:
                # Remove by index in deque (rotate trick for O(k) with small k).
                q.rotate(-idx)
                sel = q.popleft()
                q.rotate(idx)
                q.append(sel)
            except Exception:
                # If anything goes wrong, just append it to end (best-effort).
                try:
                    q.remove(pipeline)
                    q.append(pipeline)
                except Exception:
                    pass

            # Create assignment for exactly one ready op to preserve DAG correctness.
            assignment = Assignment(
                ops=[op],
                cpu=float(cpu_req),
                ram=float(ram_req),
                priority=pipeline.priority,
                pool_id=pool_id,
                pipeline_id=pipeline.pipeline_id,
            )
            assignments.append(assignment)

            # Loop again to try packing more; pool.avail_* will reflect assignment next tick in simulator,
            # but we can still be conservative here by breaking if we're close to exhaustion.
            # (We do not manually decrement since executor updates are simulator-owned.)
            if len(assignments) > 256:
                # Safety cap to avoid runaway in a single tick.
                break

    return suspensions, assignments
