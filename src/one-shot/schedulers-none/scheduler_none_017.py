# policy_key: scheduler_none_017
# reasoning_effort: none
# model: gpt-5.2-2025-12-11
# llm_cost: 0.034733
# generation_seconds: 38.90
# generated_at: 2026-03-12T21:32:29.097213
@register_scheduler_init(key="scheduler_none_017")
def scheduler_none_017_init(s):
    """Priority-aware FIFO with minimal, safe improvements over naive baseline.

    Goals / incremental improvements:
      1) Respect priority: always consider higher-priority pipelines first.
      2) Avoid head-of-line blocking: keep per-priority queues and do round-robin across pools.
      3) Basic OOM backoff: if an op OOMs, retry with more RAM (bounded) for that (pipeline, op) key.
      4) Gentle preemption: when high-priority work arrives and no pool has room, suspend one
         low-priority container (if any) to free some capacity. Keep churn low by limiting
         preemptions per tick and preferring suspending newest-seen low-priority containers.
    """
    # Waiting queues separated by priority
    s.wait_q = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }

    # Tracks last requested (cpu, ram) for a (pipeline_id, op_id) so we can retry with higher RAM on OOM.
    # Key uses repr(op) to avoid depending on specific operator attributes existing.
    s.op_req = {}

    # For light-touch preemption: map running container_id -> metadata for choosing what to preempt.
    # Updated from results/events when we see a container id.
    s.running = {}

    # Limit preemption churn
    s.max_preempt_per_tick = 1

    # Sizing knobs
    s.min_cpu_frac = {
        Priority.QUERY: 0.5,         # give interactive-ish more CPU share
        Priority.INTERACTIVE: 0.5,
        Priority.BATCH_PIPELINE: 0.25,
    }
    s.min_ram_frac = {
        Priority.QUERY: 0.5,
        Priority.INTERACTIVE: 0.5,
        Priority.BATCH_PIPELINE: 0.25,
    }

    # OOM backoff knobs
    s.oom_ram_growth = 1.5   # multiply RAM on each OOM
    s.oom_ram_add_frac = 0.10  # or add 10% of pool RAM (whichever is larger after multiply)
    s.max_ram_frac_of_pool = 0.95  # never request >95% of pool RAM for a single op


@register_scheduler(key="scheduler_none_017")
def scheduler_none_017(s, results, pipelines):
    """
    Priority-aware scheduling with bounded OOM retries and minimal preemption.

    Strategy:
      - Enqueue new pipelines into per-priority queues.
      - Process results:
          * On OOM: increase RAM request for that op (pipeline_id, op_key) for next retry.
          * Track running containers (best-effort) for preemption decisions.
      - For each pool:
          * Try to assign ready ops by scanning priorities in order: QUERY > INTERACTIVE > BATCH.
          * Request a conservative share of pool resources based on priority and availability.
      - If we cannot assign any high-priority work due to lack of resources, preempt at most one
        low-priority running container to make room (best-effort).
    """
    # Helper: priority order from highest to lowest
    prio_order = [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]

    def _enqueue_pipeline(p):
        s.wait_q[p.priority].append(p)

    def _pipeline_done_or_failed(p):
        st = p.runtime_status()
        # If any failures exist, drop the pipeline (baseline behavior: don't retry permanent failures)
        has_failures = st.state_counts.get(OperatorState.FAILED, 0) > 0
        return st.is_pipeline_successful() or has_failures

    def _next_assignable_op(p):
        st = p.runtime_status()
        ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        if not ops:
            return None
        # Keep it simple: one op at a time per pipeline to reduce interference
        return ops[0]

    def _op_key(pipeline_id, op):
        return (pipeline_id, repr(op))

    def _is_oom_result(r):
        # Be defensive: the simulator may encode errors differently
        if not r.failed():
            return False
        if r.error is None:
            return False
        try:
            msg = str(r.error).lower()
        except Exception:
            return False
        return ("oom" in msg) or ("out of memory" in msg) or ("memory" in msg and "alloc" in msg)

    def _bump_ram(pool, pipeline_id, op):
        k = _op_key(pipeline_id, op)
        prev = s.op_req.get(k)
        prev_ram = prev[1] if prev else max(1.0, 0.25 * pool.max_ram_pool)
        mul = prev_ram * s.oom_ram_growth
        add = prev_ram + s.oom_ram_add_frac * pool.max_ram_pool
        new_ram = max(mul, add)
        new_ram = min(new_ram, s.max_ram_frac_of_pool * pool.max_ram_pool)
        # Keep previous CPU if known; else a reasonable default
        prev_cpu = prev[0] if prev else max(1.0, 0.25 * pool.max_cpu_pool)
        s.op_req[k] = (prev_cpu, new_ram)

    def _suggest_request(pool, pipeline_id, op, priority):
        # If we have a remembered request (e.g., after OOM), start from it
        k = _op_key(pipeline_id, op)
        remembered = s.op_req.get(k)

        # Base request as fraction of pool (priority dependent), but never exceed availability.
        base_cpu = max(1.0, s.min_cpu_frac.get(priority, 0.25) * pool.max_cpu_pool)
        base_ram = max(1.0, s.min_ram_frac.get(priority, 0.25) * pool.max_ram_pool)

        if remembered:
            cpu_req = max(base_cpu, float(remembered[0]))
            ram_req = max(base_ram, float(remembered[1]))
        else:
            cpu_req = float(base_cpu)
            ram_req = float(base_ram)

        # Cap at per-op max and current availability
        cpu_req = min(cpu_req, float(pool.avail_cpu_pool))
        ram_req = min(ram_req, float(pool.avail_ram_pool))
        return cpu_req, ram_req

    def _try_assign_from_queue(pool_id):
        pool = s.executor.pools[pool_id]
        if pool.avail_cpu_pool <= 0 or pool.avail_ram_pool <= 0:
            return None

        # Iterate priorities in order; within each, FIFO pipelines
        for pr in prio_order:
            q = s.wait_q[pr]
            if not q:
                continue

            # Rotate through queue once; requeue non-ready pipelines to avoid HOL blocking
            n = len(q)
            for _ in range(n):
                p = q.pop(0)
                if _pipeline_done_or_failed(p):
                    continue

                op = _next_assignable_op(p)
                if op is None:
                    # Not ready yet; keep it in queue
                    q.append(p)
                    continue

                cpu_req, ram_req = _suggest_request(pool, p.pipeline_id, op, p.priority)
                # If the suggested request cannot fit, requeue and keep searching
                if cpu_req <= 0 or ram_req <= 0:
                    q.append(p)
                    continue

                # If we only have tiny slivers, avoid assigning and causing thrash
                if cpu_req < 1.0 or ram_req < 1.0:
                    q.append(p)
                    continue

                assignment = Assignment(
                    ops=[op],
                    cpu=cpu_req,
                    ram=ram_req,
                    priority=p.priority,
                    pool_id=pool_id,
                    pipeline_id=p.pipeline_id,
                )

                # Remember request for potential retries even without failure signal
                s.op_req[_op_key(p.pipeline_id, op)] = (cpu_req, ram_req)

                # Requeue pipeline so subsequent ops can be scheduled later
                q.append(p)
                return assignment

        return None

    def _choose_preemption_candidate():
        # Prefer suspending the lowest priority container; within that, arbitrary (best-effort FIFO-ish)
        # We only have what we've observed; if empty, cannot preempt.
        if not s.running:
            return None
        # Sort by (priority rank low->high), then by a monotonic "seen" index
        pr_rank = {Priority.BATCH_PIPELINE: 0, Priority.INTERACTIVE: 1, Priority.QUERY: 2}
        candidates = []
        for cid, meta in s.running.items():
            candidates.append((pr_rank.get(meta.get("priority"), 0), meta.get("seen", 0), cid, meta))
        candidates.sort(key=lambda x: (x[0], x[1]))
        # Only preempt batch (and maybe interactive if absolutely needed). Do not preempt QUERY.
        for rank, _, cid, meta in candidates:
            if meta.get("priority") == Priority.QUERY:
                continue
            return cid, meta
        return None

    # Enqueue new pipelines
    for p in pipelines:
        _enqueue_pipeline(p)

    # Early exit if no new pipelines and no results that could change state
    if not pipelines and not results:
        return [], []

    # Process results: handle OOM backoff and update running containers bookkeeping
    # Note: This is best-effort; we don't assume we get explicit start/finish events beyond results.
    for r in results:
        # Track container metadata for possible preemption decisions
        if getattr(r, "container_id", None) is not None:
            if r.failed() or (r.error is None):
                # We may be seeing completion/failure results; remove from running set
                if r.container_id in s.running:
                    s.running.pop(r.container_id, None)

        # On OOM: bump RAM for each op in the result (typically one)
        if _is_oom_result(r):
            pool = s.executor.pools[r.pool_id]
            for op in getattr(r, "ops", []) or []:
                # We don't have pipeline_id on result; best-effort: try to match via op_req keys.
                # If only one key matches repr(op), bump all matching pipeline entries.
                op_repr = repr(op)
                for (pid, ok), _v in list(s.op_req.items()):
                    if ok == op_repr:
                        _bump_ram(pool, pid, op)

    # Ensure all pools get a chance to schedule at most one assignment per tick (simple, stable)
    assignments = []
    suspensions = []

    # First pass: attempt to schedule without preemption
    for pool_id in range(s.executor.num_pools):
        a = _try_assign_from_queue(pool_id)
        if a is not None:
            assignments.append(a)

    # If we still have high-priority work waiting and couldn't schedule anything, try minimal preemption
    # Determine if there is pending high-priority ready work
    def _has_ready_high_prio():
        for pr in [Priority.QUERY, Priority.INTERACTIVE]:
            for p in s.wait_q[pr]:
                if _pipeline_done_or_failed(p):
                    continue
                if _next_assignable_op(p) is not None:
                    return True
        return False

    if _has_ready_high_prio() and not assignments:
        # Preempt at most one container
        for _ in range(s.max_preempt_per_tick):
            cand = _choose_preemption_candidate()
            if cand is None:
                break
            cid, meta = cand
            suspensions.append(Suspend(container_id=cid, pool_id=meta.get("pool_id")))
            # Remove immediately to avoid repeated suspends
            s.running.pop(cid, None)

        # After preemption, attempt scheduling again (one per pool)
        for pool_id in range(s.executor.num_pools):
            if len(assignments) >= s.executor.num_pools:
                break
            a = _try_assign_from_queue(pool_id)
            if a is not None:
                assignments.append(a)

    return suspensions, assignments
