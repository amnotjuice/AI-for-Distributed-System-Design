# policy_key: scheduler_low_036
# reasoning_effort: low
# model: gpt-5.2-2025-12-11
# llm_cost: 0.037925
# generation_seconds: 41.97
# generated_at: 2026-03-14T02:30:15.895186
@register_scheduler_init(key="scheduler_low_036")
def scheduler_low_036_init(s):
    """
    Priority-aware FIFO with conservative headroom and simple OOM-driven RAM backoff.

    Improvements over naive FIFO:
      - Separate queues by priority; always schedule QUERY/INTERACTIVE before BATCH.
      - Preserve per-pool headroom so high-priority work is less likely to queue behind batch.
      - If an operator fails with an OOM-like error, retry it later with increased RAM request.
    """
    # Per-priority waiting queues (pipelines). Pipelines remain until success.
    s.waiting = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }

    # Track RAM inflation factor for operators that previously OOM'd.
    # Keyed by (pipeline_id, op_key) where op_key is a stable string-ish identity.
    s.ram_inflation = {}

    # Simple round-robin cursor per priority to avoid always picking the same pipeline.
    s.rr_cursor = {
        Priority.QUERY: 0,
        Priority.INTERACTIVE: 0,
        Priority.BATCH_PIPELINE: 0,
    }

    # Headroom to keep in each pool (fraction of max) to reduce tail latency for bursts.
    s.headroom_frac = {
        Priority.QUERY: 0.00,         # allow queries to use the whole pool if needed
        Priority.INTERACTIVE: 0.05,   # keep a little slack for query spikes
        Priority.BATCH_PIPELINE: 0.30 # keep substantial slack so batch doesn't fill the pool
    }

    # CPU sizing (fraction of currently-available CPU to allocate per assignment).
    # This is intentionally simple and conservative for batch.
    s.cpu_frac = {
        Priority.QUERY: 1.00,
        Priority.INTERACTIVE: 0.75,
        Priority.BATCH_PIPELINE: 0.50
    }

    # RAM sizing (fraction of currently-available RAM to allocate per assignment),
    # before applying any OOM-driven inflation.
    s.ram_frac = {
        Priority.QUERY: 0.80,
        Priority.INTERACTIVE: 0.70,
        Priority.BATCH_PIPELINE: 0.60
    }

    # Limits to avoid oversubscribing the pool with many small assignments.
    s.max_assignments_per_pool_per_tick = 2


@register_scheduler(key="scheduler_low_036")
def scheduler_low_036(s, results, pipelines):
    """
    Scheduler step:
      - Ingest new pipelines into priority queues.
      - Update OOM backoff signals from recent failures.
      - For each pool, schedule up to N assignments, choosing highest-priority ready ops first.
    """
    # ---- helpers (kept inside to avoid top-level imports) ----
    def _prio_order():
        return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]

    def _op_key(op):
        # Best-effort stable identifier without relying on unknown fields.
        # Use repr(op) which typically includes an id/name; falls back to object id.
        try:
            return repr(op)
        except Exception:
            return str(id(op))

    def _is_oom_error(err):
        if err is None:
            return False
        try:
            msg = str(err).lower()
        except Exception:
            return False
        # Common substrings across runtimes.
        return ("oom" in msg) or ("out of memory" in msg) or ("memoryerror" in msg) or ("killed" in msg and "memory" in msg)

    def _enqueue_pipeline(p):
        pr = p.priority
        if pr not in s.waiting:
            # Unknown priority -> treat as batch-like.
            pr = Priority.BATCH_PIPELINE
        s.waiting[pr].append(p)

    def _pipeline_done_or_permafailed(p):
        st = p.runtime_status()
        # We only consider "done" when successful. Failed ops may be retried (esp. OOM).
        return st.is_pipeline_successful()

    def _get_next_ready_op_from_queue(priority):
        """
        Round-robin scan pipelines of a given priority to find one ready op.
        Returns (pipeline, [op]) or (None, None)
        """
        q = s.waiting[priority]
        if not q:
            return None, None

        n = len(q)
        start = s.rr_cursor.get(priority, 0) % n
        idx = start
        for _ in range(n):
            p = q[idx]
            st = p.runtime_status()
            if st.is_pipeline_successful():
                # Remove completed pipeline from the queue.
                q.pop(idx)
                n -= 1
                if n <= 0:
                    s.rr_cursor[priority] = 0
                    return None, None
                if idx >= n:
                    idx = 0
                continue

            ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
            if ops:
                # Advance RR cursor to next pipeline for fairness.
                s.rr_cursor[priority] = (idx + 1) % len(q)
                return p, [ops[0]]

            idx = (idx + 1) % n

        # No ready ops found for this priority.
        return None, None

    # ---- ingest new pipelines ----
    for p in pipelines:
        _enqueue_pipeline(p)

    # Early exit if nothing changes.
    if not pipelines and not results:
        return [], []

    # ---- process results: update OOM-driven RAM inflation signals ----
    # Note: ExecutionResult doesn't include pipeline_id in the given API. We therefore
    # use a conservative approach: inflate by operator identity only when we can
    # reasonably associate it later (via pipeline_id + repr(op) when scheduling).
    #
    # Practically, this works because failed ops will reappear in ASSIGNABLE_STATES,
    # and their repr(op) tends to be stable within a pipeline.
    for r in results:
        if not getattr(r, "failed", lambda: False)():
            continue
        if not _is_oom_error(getattr(r, "error", None)):
            continue

        # Inflate for each op in this result; if ops is missing/empty, do nothing.
        ops = getattr(r, "ops", None) or []
        for op in ops:
            # We cannot reliably map to pipeline_id from result, so we store a global key
            # with pipeline_id placeholder None; then during scheduling we check both.
            key_global = (None, _op_key(op))
            prev = s.ram_inflation.get(key_global, 1.0)
            # Multiplicative backoff; cap to avoid requesting entire pool forever.
            s.ram_inflation[key_global] = min(prev * 1.5, 8.0)

    suspensions = []  # No preemption yet (kept simple and safe).
    assignments = []

    # ---- scheduling loop over pools ----
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]

        made = 0
        while made < s.max_assignments_per_pool_per_tick:
            avail_cpu = pool.avail_cpu_pool
            avail_ram = pool.avail_ram_pool
            if avail_cpu <= 0 or avail_ram <= 0:
                break

            # Choose highest-priority ready op.
            chosen_p = None
            chosen_ops = None
            chosen_pr = None
            for pr in _prio_order():
                p, ops = _get_next_ready_op_from_queue(pr)
                if p is not None and ops is not None:
                    chosen_p, chosen_ops, chosen_pr = p, ops, pr
                    break

            if chosen_p is None:
                break  # nothing runnable anywhere

            # Apply per-priority headroom (keep a fraction of max resources unused).
            headroom_cpu = pool.max_cpu_pool * s.headroom_frac.get(chosen_pr, 0.0)
            headroom_ram = pool.max_ram_pool * s.headroom_frac.get(chosen_pr, 0.0)

            usable_cpu = max(0.0, avail_cpu - headroom_cpu)
            usable_ram = max(0.0, avail_ram - headroom_ram)

            if usable_cpu <= 0 or usable_ram <= 0:
                # Not enough headroom to schedule this priority in this pool right now.
                # Put the pipeline back (front) and stop trying in this pool.
                s.waiting[chosen_pr].insert(0, chosen_p)
                break

            # Size the assignment based on priority fractions, but never exceed usable.
            cpu_req = max(1.0, usable_cpu * s.cpu_frac.get(chosen_pr, 0.5))
            cpu_req = min(cpu_req, usable_cpu)

            base_ram_req = usable_ram * s.ram_frac.get(chosen_pr, 0.6)

            # Apply OOM inflation if we have a signal for this operator.
            opk = _op_key(chosen_ops[0])
            infl = 1.0
            # Prefer pipeline-specific inflation if present; otherwise use global.
            infl = max(infl, s.ram_inflation.get((chosen_p.pipeline_id, opk), 1.0))
            infl = max(infl, s.ram_inflation.get((None, opk), 1.0))

            ram_req = base_ram_req * infl
            # Clamp: must fit in usable, and must be positive.
            ram_req = max(1.0, min(ram_req, usable_ram))

            # If we had to clamp due to lack of RAM, don't keep hammering; requeue and stop.
            # (This avoids thrashing when batch is holding most RAM and an interactive op needs more.)
            if ram_req >= usable_ram and usable_ram < avail_ram * 0.25:
                s.waiting[chosen_pr].insert(0, chosen_p)
                break

            assignments.append(
                Assignment(
                    ops=chosen_ops,
                    cpu=cpu_req,
                    ram=ram_req,
                    priority=chosen_pr,
                    pool_id=pool_id,
                    pipeline_id=chosen_p.pipeline_id,
                )
            )
            made += 1

    return suspensions, assignments
