# policy_key: scheduler_est_019
# reasoning_effort: low
# model: gpt-5.2-2025-12-11
# llm_cost: 0.063214
# generation_seconds: 45.31
# generated_at: 2026-04-01T13:49:45.217053
@register_scheduler_init(key="scheduler_est_019")
def scheduler_est_019_init(s):
    """Priority-aware, small-step improvement over naive FIFO.

    Changes vs naive:
      - Maintain separate queues by priority (QUERY/INTERACTIVE ahead of BATCH).
      - Avoid allocating *all* pool CPU/RAM to a single op; instead pack multiple ops per pool
        with per-op caps, improving latency under contention.
      - Use (optional) op.estimate.mem_peak_gb conservatively for RAM sizing and retry bigger on failure.
      - Lightweight headroom reservation for high-priority work (no preemption yet).
    """
    # Pipeline queues by priority (store pipeline objects)
    s.q_query = []
    s.q_interactive = []
    s.q_batch = []

    # Track per-op RAM hints that we increase on failure (best-effort; attribute availability may vary)
    # key: (pipeline_id, op_key) -> ram_gb
    s.op_ram_hint_gb = {}

    # Round-robin pointer to avoid always draining the same queue first within a priority class
    s.rr_ptr = {
        "query": 0,
        "interactive": 0,
        "batch": 0,
    }


@register_scheduler(key="scheduler_est_019")
def scheduler_est_019_scheduler(s, results, pipelines):
    """
    Priority-aware packing scheduler.

    Policy outline:
      1) Enqueue new pipelines into per-priority queues.
      2) Process results to learn from failures (increase RAM hint for the failed op).
      3) For each pool:
         - Reserve some CPU/RAM for high priority if high-priority work is waiting.
         - Schedule ready QUERY/INTERACTIVE ops first with modest CPU caps.
         - Then schedule BATCH ops with remaining resources.
      4) No preemption in this iteration; focus on obvious FIFO flaws first.
    """
    # -------------------------
    # Helpers (kept local)
    # -------------------------
    def _is_done_or_failed(p):
        st = p.runtime_status()
        if st.is_pipeline_successful():
            return True
        # If any operator is FAILED, we still may want to retry it (ASSIGNABLE_STATES includes FAILED)
        # So do not drop pipeline solely due to failures.
        return False

    def _enqueue_pipeline(p):
        if p.priority == Priority.QUERY:
            s.q_query.append(p)
        elif p.priority == Priority.INTERACTIVE:
            s.q_interactive.append(p)
        else:
            s.q_batch.append(p)

    def _op_key(op):
        # Try common stable identifiers, fallback to Python object id
        for attr in ("op_id", "operator_id", "id", "name"):
            v = getattr(op, attr, None)
            if v is not None:
                return v
        return id(op)

    def _learn_from_results():
        # Increase RAM hint for any failed op based on what we tried.
        # If the simulator provides error strings, treat any failure as "needs more RAM" (conservative).
        for r in results or []:
            try:
                failed = r.failed()
            except Exception:
                failed = bool(getattr(r, "error", None))

            if not failed:
                continue

            # r.ops might be a list
            rops = getattr(r, "ops", None) or []
            if not isinstance(rops, list):
                rops = [rops]

            for op in rops:
                # Best-effort: we don't have pipeline_id in ExecutionResult, so we key by container_id + op
                # but prefer to use just op_key (still useful across retries within a run).
                ok = _op_key(op)
                # Use the RAM that was allocated for the failed container as baseline, then grow
                prev = s.op_ram_hint_gb.get((None, ok), None)
                base = getattr(r, "ram", None)
                if base is None:
                    base = prev if prev is not None else 1.0
                # Exponential backoff with a floor increment
                new_hint = max(float(base) * 2.0, float(base) + 1.0)
                s.op_ram_hint_gb[(None, ok)] = new_hint

    def _ready_op_from_pipeline(p):
        st = p.runtime_status()
        op_list = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        if not op_list:
            return None
        return op_list[0]

    def _rotate_and_collect_ready(q, rr_key, limit):
        # Round-robin through queue to find up to 'limit' pipelines that currently have a ready op.
        # Pipelines without a ready op are kept in the queue.
        n = len(q)
        if n == 0 or limit <= 0:
            return [], q

        start = s.rr_ptr.get(rr_key, 0) % max(n, 1)
        out = []
        kept = []

        # Iterate the queue starting at 'start'
        for i in range(n):
            p = q[(start + i) % n]
            if _is_done_or_failed(p):
                # Drop completed pipelines
                continue
            op = _ready_op_from_pipeline(p)
            if op is None or len(out) >= limit:
                kept.append(p)
            else:
                out.append((p, op))
                kept.append(p)

        # Advance rr pointer (approximate)
        s.rr_ptr[rr_key] = (start + 1) % max(len(kept), 1) if kept else 0
        return out, kept

    def _mem_request_gb(p, op, pool_avail_ram):
        # Base RAM: prefer op.estimate.mem_peak_gb, else a small default.
        est = getattr(getattr(op, "estimate", None), "mem_peak_gb", None)
        ok = _op_key(op)

        # Learned hint from failures (global best-effort, may help even without pipeline_id in results)
        learned = s.op_ram_hint_gb.get((p.pipeline_id, ok), None)
        if learned is None:
            learned = s.op_ram_hint_gb.get((None, ok), None)

        if learned is not None:
            base = float(learned)
        elif est is not None:
            # Conservative: add modest slack
            base = float(est) * 1.25
        else:
            # Safe-ish default for unknown ops
            base = 2.0

        # Clamp to what we can allocate now (never exceed pool avail)
        # Also ensure at least a tiny amount if pool has any RAM.
        req = min(float(pool_avail_ram), max(0.1, base))
        return req

    def _cpu_request(p, pool_avail_cpu, class_name):
        # Keep per-op CPU caps to improve latency for concurrent interactive work.
        # These are intentionally conservative (small-step improvement).
        if class_name in ("query", "interactive"):
            cap = 4.0
            floor = 1.0
        else:
            cap = 8.0
            floor = 1.0

        return min(float(pool_avail_cpu), max(floor, min(cap, float(pool_avail_cpu))))

    # -------------------------
    # Enqueue new pipelines
    # -------------------------
    for p in pipelines or []:
        _enqueue_pipeline(p)

    # Early exit if nothing changes
    if not (pipelines or results):
        return [], []

    suspensions = []  # no preemption in this iteration
    assignments = []

    # Learn from failures
    _learn_from_results()

    # Determine whether we should reserve headroom for high-priority work
    def _has_high_priority_waiting():
        # Consider "waiting" as having any pipeline that is not done; ready-ness checked per pool fill.
        for q in (s.q_query, s.q_interactive):
            for p in q:
                if not _is_done_or_failed(p):
                    return True
        return False

    high_waiting = _has_high_priority_waiting()

    # -------------------------
    # Scheduling per pool
    # -------------------------
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)
        if avail_cpu <= 0.0 or avail_ram <= 0.0:
            continue

        # Reserve a fraction of resources for high priority if any exists (headroom protection).
        # This is a soft reserve; we only enforce it when scheduling batch.
        reserve_cpu = 0.0
        reserve_ram = 0.0
        if high_waiting:
            reserve_cpu = 0.25 * float(pool.max_cpu_pool)
            reserve_ram = 0.25 * float(pool.max_ram_pool)

        # Fill helper: attempt to place up to N ops from a given priority class.
        def _fill_from_queue(class_name, q, cpu_reserve_for_others, ram_reserve_for_others, max_ops):
            nonlocal avail_cpu, avail_ram, assignments

            # Collect candidates in RR order; we will only actually assign while resources allow
            candidates, kept = _rotate_and_collect_ready(q, class_name, limit=max_ops)
            # Update the queue to the kept list (RR helper already filtered done pipelines)
            if class_name == "query":
                s.q_query[:] = kept
            elif class_name == "interactive":
                s.q_interactive[:] = kept
            else:
                s.q_batch[:] = kept

            # Try to assign candidates one by one
            for (p, op) in candidates:
                if avail_cpu <= 0.0 or avail_ram <= 0.0:
                    break

                # Enforce soft reserve: don't let this class consume below reserves for "others"
                if avail_cpu <= cpu_reserve_for_others or avail_ram <= ram_reserve_for_others:
                    break

                # Decide resources for this op
                cpu_req = _cpu_request(p, avail_cpu - cpu_reserve_for_others, class_name)
                ram_req = _mem_request_gb(p, op, avail_ram - ram_reserve_for_others)

                # If we can't fit anything meaningful, stop
                if cpu_req <= 0.0 or ram_req <= 0.0:
                    break

                # Create assignment for a single op (atomic scheduling unit)
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

                # Reduce remaining pool capacity
                avail_cpu -= cpu_req
                avail_ram -= ram_req

        # Prioritize QUERY then INTERACTIVE; no reserve against each other
        # Limit number of ops per tick per pool to avoid over-scheduling in one decision step
        _fill_from_queue("query", s.q_query, cpu_reserve_for_others=0.0, ram_reserve_for_others=0.0, max_ops=8)
        _fill_from_queue("interactive", s.q_interactive, cpu_reserve_for_others=0.0, ram_reserve_for_others=0.0, max_ops=8)

        # Only schedule batch if we keep some headroom (when high priority exists)
        batch_cpu_reserve = max(0.0, reserve_cpu) if high_waiting else 0.0
        batch_ram_reserve = max(0.0, reserve_ram) if high_waiting else 0.0
        _fill_from_queue("batch", s.q_batch, cpu_reserve_for_others=batch_cpu_reserve, ram_reserve_for_others=batch_ram_reserve, max_ops=8)

    return suspensions, assignments
