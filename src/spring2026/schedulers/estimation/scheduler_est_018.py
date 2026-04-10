# policy_key: scheduler_est_018
# reasoning_effort: low
# exp: estimation
# model: gpt-5.2-2025-12-11
# llm_cost: 0.052808
# generation_seconds: 76.86
# generated_at: 2026-04-10T09:06:45.501067
@register_scheduler_init(key="scheduler_est_018")
def scheduler_est_018_init(s):
    """Memory-aware, priority-first scheduler with OOM backoff.

    Goals:
      - Minimize weighted latency (query > interactive > batch) while avoiding failures (720s penalty).
      - Use mem_peak estimate (noisy hint) to reduce OOM by choosing feasible pools and RAM sizing.
      - Prevent batch starvation with light aging / occasional batch admission.

    Key ideas:
      - Maintain per-priority FIFO queues; schedule in weighted round-robin with batch "aging" boost.
      - For each assignment, pick an assignable op whose parents are complete; prefer lower estimated mem.
      - Pool selection via best-fit on available RAM given estimated peak; skip infeasible placements.
      - On failure, treat as likely OOM and increase RAM multiplier for that (pipeline, op) on retry.
    """
    from collections import deque

    s.q_query = deque()
    s.q_interactive = deque()
    s.q_batch = deque()

    # Per-(pipeline_id, op_id) RAM multipliers to back off after failures.
    s.ram_mult = {}

    # Bookkeeping for light anti-starvation.
    s.enqueue_tick = {}  # pipeline_id -> first seen tick
    s.tick = 0
    s.assign_count = 0


@register_scheduler(key="scheduler_est_018")
def scheduler_est_018(s, results, pipelines):
    from collections import deque

    # --- Helpers (kept inside for self-containment) ---
    def _prio_weight(priority):
        if priority == Priority.QUERY:
            return 10
        if priority == Priority.INTERACTIVE:
            return 5
        return 1

    def _queue_for(priority):
        if priority == Priority.QUERY:
            return s.q_query
        if priority == Priority.INTERACTIVE:
            return s.q_interactive
        return s.q_batch

    def _iter_all_queues():
        # Order matters: higher weight first.
        return (s.q_query, s.q_interactive, s.q_batch)

    def _is_done_or_failed(p):
        st = p.runtime_status()
        if st.is_pipeline_successful():
            return True
        # If any operator failed, this pipeline is effectively doomed in the baseline template.
        # But in this simulator, FAILED is often retryable (ASSIGNABLE_STATES includes FAILED).
        # We therefore DO NOT drop on failures here; we allow retry by re-assigning FAILED ops.
        return False

    def _has_progressable_ops(p):
        st = p.runtime_status()
        ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        return bool(ops)

    def _pick_op(p):
        """Pick one assignable op; prefer smaller estimated memory to reduce fragmentation/OOM."""
        st = p.runtime_status()
        ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        if not ops:
            return None

        def est_mem(op):
            e = getattr(op, "estimate", None)
            m = None
            if e is not None:
                m = getattr(e, "mem_peak_gb", None)
            # None is treated as "unknown / medium".
            return float("inf") if m is None else float(m)

        # Prefer known-small memory; if all unknown, just take the first.
        ops_sorted = sorted(ops, key=lambda op: est_mem(op))
        return ops_sorted[0]

    def _op_key(pipeline_id, op):
        # Best-effort stable key without relying on a specific attribute name.
        for attr in ("op_id", "operator_id", "id", "name"):
            if hasattr(op, attr):
                return (pipeline_id, getattr(op, attr))
        return (pipeline_id, id(op))

    def _estimate_mem(op):
        e = getattr(op, "estimate", None)
        if e is None:
            return None
        m = getattr(e, "mem_peak_gb", None)
        if m is None:
            return None
        try:
            return float(m)
        except Exception:
            return None

    def _ram_multiplier(pipeline_id, op):
        return s.ram_mult.get(_op_key(pipeline_id, op), 1.0)

    def _set_ram_multiplier(pipeline_id, op, mult):
        s.ram_mult[_op_key(pipeline_id, op)] = mult

    def _update_from_results():
        """On failures, increase RAM multiplier for retried ops (OOM backoff heuristic)."""
        for r in results or []:
            if hasattr(r, "failed") and r.failed():
                # Conservatively treat any failure as potentially memory-related; increase RAM.
                # This reduces repeat failures (heavy penalty) at the cost of lower utilization.
                try:
                    p_id = r.pipeline_id  # may not exist
                except Exception:
                    p_id = None

                # We can only reliably map to ops present in r.ops.
                for op in getattr(r, "ops", []) or []:
                    if p_id is None:
                        # Fall back: if pipeline_id isn't on the result, we cannot key it well.
                        # Use a global key using None; still helpful to reduce repeated failures.
                        pid = -1
                    else:
                        pid = p_id
                    prev = _ram_multiplier(pid, op)
                    # Multiplicative backoff; cap to avoid runaway.
                    new = min(prev * 1.5, 8.0)
                    _set_ram_multiplier(pid, op, new)

    def _choose_pipeline_for_pool(avail_ram, pool_max_ram):
        """Select next pipeline to schedule, with priority bias and batch anti-starvation.

        Returns a pipeline or None. The selected pipeline is removed from its queue.
        Pipelines not ready are rotated to the back.
        """
        # Hard bias: serve Q > I > B, but occasionally allow batch if it's been waiting.
        # We implement: every 10 assignments, try to serve a batch if any is ready.
        if s.assign_count % 10 == 9 and s.q_batch:
            # Look for a progressable batch within a small scan window.
            for _ in range(min(len(s.q_batch), 8)):
                p = s.q_batch.popleft()
                if _is_done_or_failed(p):
                    continue
                if _has_progressable_ops(p):
                    return p
                s.q_batch.append(p)

        # Otherwise, serve high priority first.
        for q in _iter_all_queues():
            # Small scan window to avoid O(n) per tick.
            for _ in range(min(len(q), 8)):
                p = q.popleft()
                if _is_done_or_failed(p):
                    continue
                if _has_progressable_ops(p):
                    return p
                q.append(p)

        # If nothing is immediately progressable, allow "waiting for parents" pipelines
        # to remain queued (no selection).
        return None

    def _select_pool_for_op(op, priority):
        """Pick pool with best-fit RAM feasibility for this op given its estimated memory."""
        est = _estimate_mem(op)

        # If unknown, we avoid overly fragmenting small pools by preferring the pool
        # with the most available RAM (reduces risk of OOM).
        if est is None:
            best_pool = None
            best_avail = -1.0
            for pool_id in range(s.executor.num_pools):
                pool = s.executor.pools[pool_id]
                if pool.avail_cpu_pool <= 0 or pool.avail_ram_pool <= 0:
                    continue
                if pool.avail_ram_pool > best_avail:
                    best_avail = pool.avail_ram_pool
                    best_pool = pool_id
            return best_pool

        # With an estimate: require it to (roughly) fit. Use best-fit on available RAM.
        # Add a small safety factor; noisy estimates can be underestimates.
        safety = 1.20 if priority in (Priority.QUERY, Priority.INTERACTIVE) else 1.10
        need = est * safety

        best_pool = None
        best_slack = None
        for pool_id in range(s.executor.num_pools):
            pool = s.executor.pools[pool_id]
            if pool.avail_cpu_pool <= 0 or pool.avail_ram_pool <= 0:
                continue
            if need <= pool.avail_ram_pool:
                slack = pool.avail_ram_pool - need
                if best_slack is None or slack < best_slack:
                    best_slack = slack
                    best_pool = pool_id

        # If nothing fits, signal unschedulable now (avoid known-OOM placement).
        return best_pool

    def _compute_resources(pool, op, priority, pipeline_id):
        """Compute cpu/ram request bounded by pool availability."""
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool

        # CPU: prioritize latency-sensitive work; keep batch from grabbing entire pool.
        if priority == Priority.QUERY:
            cpu = max(1, avail_cpu)  # take all available in pool for fast completion
        elif priority == Priority.INTERACTIVE:
            cpu = max(1, int(avail_cpu * 0.75)) if avail_cpu > 1 else 1
        else:
            cpu = max(1, int(avail_cpu * 0.50)) if avail_cpu > 1 else 1
        cpu = min(cpu, avail_cpu)

        # RAM: use estimate * safety * backoff multiplier; otherwise allocate a conservative chunk.
        est = _estimate_mem(op)
        mult = _ram_multiplier(pipeline_id, op)

        if est is None:
            # Unknown: request a moderate chunk; too small increases failure risk (720s penalty).
            base = max(1.0, pool.max_ram_pool * (0.40 if priority != Priority.BATCH_PIPELINE else 0.30))
            ram_req = base * mult
        else:
            safety = 1.25 if priority in (Priority.QUERY, Priority.INTERACTIVE) else 1.15
            ram_req = est * safety * mult

        # Bound to pool availability; if we can't meet the request, we'll let placement logic decide.
        ram = min(avail_ram, max(0.0, ram_req))

        # Avoid trivially small RAM allocations (likely to fail); if too small, let it wait.
        if est is not None and ram < est * 0.90:
            return None, None

        # Ensure at least some RAM if we schedule.
        if ram <= 0:
            return None, None

        return cpu, ram

    # --- Main scheduler logic ---
    s.tick += 1

    # Enqueue new pipelines by priority.
    for p in pipelines or []:
        _queue_for(p.priority).append(p)
        if p.pipeline_id not in s.enqueue_tick:
            s.enqueue_tick[p.pipeline_id] = s.tick

    # Update retry multipliers from execution results.
    if results:
        _update_from_results()

    # Early exit when nothing changes.
    if not (pipelines or results):
        return [], []

    suspensions = []
    assignments = []

    # We build assignments with explicit pool selection per op.
    # Strategy: up to one assignment per pool per tick, but can schedule multiple pools.
    for _ in range(s.executor.num_pools):
        # Choose a candidate pipeline first (priority/aging), then place its next op to a suitable pool.
        # This "pipeline-first" approach reduces head-of-line blocking for high-priority queues.
        # It also avoids scheduling ops that are likely to OOM by skipping pools that don't fit.
        probe_pipeline = _choose_pipeline_for_pool(avail_ram=None, pool_max_ram=None)
        if probe_pipeline is None:
            continue

        op = _pick_op(probe_pipeline)
        if op is None:
            # Not actually schedulable now; rotate back.
            _queue_for(probe_pipeline.priority).append(probe_pipeline)
            continue

        pool_id = _select_pool_for_op(op, probe_pipeline.priority)
        if pool_id is None:
            # No feasible pool given estimate; keep it queued to avoid known-bad placement.
            _queue_for(probe_pipeline.priority).append(probe_pipeline)
            continue

        pool = s.executor.pools[pool_id]
        if pool.avail_cpu_pool <= 0 or pool.avail_ram_pool <= 0:
            _queue_for(probe_pipeline.priority).append(probe_pipeline)
            continue

        cpu, ram = _compute_resources(pool, op, probe_pipeline.priority, probe_pipeline.pipeline_id)
        if cpu is None or ram is None:
            # Can't meet minimum inferred requirement; requeue and try others.
            _queue_for(probe_pipeline.priority).append(probe_pipeline)
            continue

        assignment = Assignment(
            ops=[op],
            cpu=cpu,
            ram=ram,
            priority=probe_pipeline.priority,
            pool_id=pool_id,
            pipeline_id=probe_pipeline.pipeline_id,
        )
        assignments.append(assignment)
        s.assign_count += 1

        # Requeue the pipeline if it likely has more work later.
        _queue_for(probe_pipeline.priority).append(probe_pipeline)

    return suspensions, assignments
