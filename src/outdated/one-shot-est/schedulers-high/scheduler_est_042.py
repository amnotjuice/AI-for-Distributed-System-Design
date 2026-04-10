# policy_key: scheduler_est_042
# reasoning_effort: high
# model: gpt-5.2-2025-12-11
# llm_cost: 0.129478
# generation_seconds: 70.53
# generated_at: 2026-03-31T19:41:27.264957
@register_scheduler_init(key="scheduler_est_042")
def scheduler_est_042_init(s):
    """Priority-aware, estimate-based scheduler with simple OOM backoff.

    Small, practical improvements over naive FIFO:
      - Maintain separate waiting queues by priority (QUERY > INTERACTIVE > BATCH).
      - Right-size RAM using op.estimate.mem_peak_gb (+ safety factor), instead of giving an op the whole pool.
      - Greedy packing: within a pool, pick the first READY op that *fits* available RAM to reduce head-of-line blocking.
      - On OOM failures, retry the same op with increased RAM multiplier (bounded).
      - (Optional) If multiple pools exist: prefer pool 0 for high-priority work, keep batch off it when possible.
    """
    # Queues per priority (round-robin within each queue to avoid starvation)
    s.waiting_by_prio = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }

    # Remember OOM-driven RAM inflation per (pipeline_id, op_key)
    s.oom_ram_mult = {}  # (pipeline_id, op_key) -> float

    # Pipelines that experienced non-OOM failures; stop retrying to avoid infinite loops
    s.dead_pipelines = set()  # set[pipeline_id]

    # Round-robin cursor per priority queue
    s.rr_cursor = {
        Priority.QUERY: 0,
        Priority.INTERACTIVE: 0,
        Priority.BATCH_PIPELINE: 0,
    }

    # Tuning knobs
    s.ram_safety = 1.20          # multiplicative safety on top of estimate
    s.ram_additive_gb = 0.25     # additive RAM buffer (GB)
    s.min_ram_gb = 0.25          # never allocate below this (GB)
    s.max_oom_mult = 8.0         # upper bound on OOM multiplier
    s.oom_backoff = 1.6          # multiplier growth on each OOM

    # CPU sizing knobs (keep small to allow concurrency; favor latency for high priority)
    s.cpu_query = 4.0
    s.cpu_interactive = 3.0
    s.cpu_batch = 2.0
    s.cpu_min = 1.0


@register_scheduler(key="scheduler_est_042")
def scheduler_est_042(s, results: list, pipelines: list):
    """Make scheduling decisions for this tick."""
    def _prio_rank(prio):
        if prio == Priority.QUERY:
            return 0
        if prio == Priority.INTERACTIVE:
            return 1
        return 2  # batch

    def _is_oom_error(err):
        if err is None:
            return False
        msg = str(err).upper()
        return ("OOM" in msg) or ("OUT OF MEMORY" in msg) or ("CUDA_ERROR_OUT_OF_MEMORY" in msg)

    def _op_key(op):
        # Try stable identifiers if available; fall back to object id.
        k = getattr(op, "op_id", None)
        if k is None:
            k = getattr(op, "operator_id", None)
        if k is None:
            k = getattr(op, "id", None)
        if k is None:
            k = id(op)
        return k

    def _desired_cpu_for_priority(prio, avail_cpu):
        if avail_cpu <= 0:
            return 0.0
        if prio == Priority.QUERY:
            target = s.cpu_query
        elif prio == Priority.INTERACTIVE:
            target = s.cpu_interactive
        else:
            target = s.cpu_batch
        # Avoid tiny fragments; but also do not exceed availability.
        return max(s.cpu_min, min(float(avail_cpu), float(target)))

    def _estimated_ram_gb(pipeline_id, op, pool_max_ram):
        est = getattr(getattr(op, "estimate", None), "mem_peak_gb", None)
        if est is None:
            est = 0.0
        try:
            est = float(est)
        except Exception:
            est = 0.0

        mult = s.oom_ram_mult.get((pipeline_id, _op_key(op)), 1.0)
        ram = (est * mult * s.ram_safety) + s.ram_additive_gb
        ram = max(s.min_ram_gb, ram)
        # Never ask for more than the pool can ever provide.
        return min(float(pool_max_ram), ram)

    def _cleanup_queue(prio):
        q = s.waiting_by_prio[prio]
        if not q:
            s.rr_cursor[prio] = 0
            return
        kept = []
        for p in q:
            if p.pipeline_id in s.dead_pipelines:
                continue
            st = p.runtime_status()
            if st.is_pipeline_successful():
                continue
            kept.append(p)
        s.waiting_by_prio[prio] = kept
        if not kept:
            s.rr_cursor[prio] = 0
        else:
            s.rr_cursor[prio] %= len(kept)

    def _rr_iter_indices(n, start):
        # Yield indices in round-robin order starting at "start".
        if n <= 0:
            return
        for off in range(n):
            yield (start + off) % n

    def _find_fittable_candidate(pool, prio_order, pool_id):
        """Find (priority, queue_index, pipeline, op, ram_need) that fits current pool RAM."""
        avail_ram = float(pool.avail_ram_pool)
        if avail_ram <= 0:
            return None

        # Scan queues by priority and within each queue in RR order; pick the first fittable op.
        for prio in prio_order:
            q = s.waiting_by_prio[prio]
            if not q:
                continue

            start = s.rr_cursor[prio] % len(q)
            for idx in _rr_iter_indices(len(q), start):
                p = q[idx]
                if p.pipeline_id in s.dead_pipelines:
                    continue

                st = p.runtime_status()
                if st.is_pipeline_successful():
                    continue

                # Only schedule ops whose parents are complete.
                op_list = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
                if not op_list:
                    continue
                op = op_list[0]

                ram_need = _estimated_ram_gb(p.pipeline_id, op, pool.max_ram_pool)
                if ram_need <= avail_ram and ram_need > 0:
                    return (prio, idx, p, op, ram_need)

        return None

    # Ingest new pipelines
    for p in pipelines:
        # If new pipeline arrives, enqueue by priority
        prio = p.priority
        if prio not in s.waiting_by_prio:
            # Unknown priority -> treat as batch
            prio = Priority.BATCH_PIPELINE
        s.waiting_by_prio[prio].append(p)

    # Process results: learn from failures (OOM -> increase RAM for that op; non-OOM -> stop pipeline)
    for r in results:
        if not getattr(r, "failed", lambda: False)():
            continue

        # Mark pipeline as dead on non-OOM failures to avoid infinite retries.
        is_oom = _is_oom_error(getattr(r, "error", None))
        if not is_oom:
            # If pipeline_id isn't present, we can't target; best effort: ignore.
            pipeline_id = getattr(r, "pipeline_id", None)
            if pipeline_id is not None:
                s.dead_pipelines.add(pipeline_id)
            continue

        # OOM: increase RAM multiplier for each op that ran in that container.
        pipeline_id = getattr(r, "pipeline_id", None)
        if pipeline_id is None:
            # Best-effort fallback: cannot associate retry multiplier reliably.
            continue

        for op in getattr(r, "ops", []) or []:
            k = (pipeline_id, _op_key(op))
            cur = float(s.oom_ram_mult.get(k, 1.0))
            nxt = min(s.max_oom_mult, max(cur, 1.0) * s.oom_backoff)
            s.oom_ram_mult[k] = nxt

    # Clean up finished/dead pipelines from queues
    for pr in (Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE):
        _cleanup_queue(pr)

    # Early exit if nothing to do
    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    # Pool preference:
    # - If multiple pools: pool 0 is "latency pool" (QUERY/INTERACTIVE first).
    # - Other pools prefer batch, but will run high-priority if batch is empty.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)
        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        if s.executor.num_pools >= 2:
            if pool_id == 0:
                prio_order = [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]
            else:
                prio_order = [Priority.BATCH_PIPELINE, Priority.INTERACTIVE, Priority.QUERY]
        else:
            prio_order = [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]

        # Greedily pack multiple ops into the pool this tick.
        # We stop when we can't find any op that fits remaining RAM (or CPU).
        made_progress = True
        while made_progress:
            made_progress = False

            avail_cpu = float(pool.avail_cpu_pool)
            avail_ram = float(pool.avail_ram_pool)
            if avail_cpu < s.cpu_min or avail_ram < s.min_ram_gb:
                break

            cand = _find_fittable_candidate(pool, prio_order, pool_id)
            if cand is None:
                break

            prio, q_idx, pipeline, op, ram_need = cand

            # If the op needs more RAM than pool can ever offer, kill the pipeline (unschedulable).
            if ram_need >= float(pool.max_ram_pool) and ram_need > float(pool.avail_ram_pool):
                s.dead_pipelines.add(pipeline.pipeline_id)
                _cleanup_queue(prio)
                made_progress = True
                continue

            cpu_alloc = _desired_cpu_for_priority(prio, avail_cpu)
            ram_alloc = min(float(pool.avail_ram_pool), float(ram_need))

            if cpu_alloc <= 0 or ram_alloc <= 0:
                break

            # Create assignment (one op per assignment; allows better interleaving across pipelines)
            assignments.append(
                Assignment(
                    ops=[op],
                    cpu=cpu_alloc,
                    ram=ram_alloc,
                    priority=pipeline.priority,
                    pool_id=pool_id,
                    pipeline_id=pipeline.pipeline_id,
                )
            )

            # Advance RR cursor for that priority and move pipeline to the back (simple RR fairness)
            q = s.waiting_by_prio[prio]
            if 0 <= q_idx < len(q):
                # Rotate: pop and append to end so other pipelines get a chance.
                p = q.pop(q_idx)
                q.append(p)
                # Cursor stays pointing at next element (same index) after rotation.
                if q:
                    s.rr_cursor[prio] %= len(q)

            made_progress = True

    return suspensions, assignments
