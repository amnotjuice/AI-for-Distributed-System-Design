# policy_key: scheduler_est_007
# reasoning_effort: low
# model: gpt-5.2-2025-12-11
# llm_cost: 0.040067
# generation_seconds: 42.13
# generated_at: 2026-04-03T01:09:02.288602
@register_scheduler_init(key="scheduler_est_007")
def scheduler_est_007_init(s):
    """
    Priority-aware, retry-on-OOM scheduler with small, safe improvements over FIFO.

    Key ideas (kept intentionally simple):
    - Maintain separate FIFO queues per priority to reduce head-of-line blocking for high-priority work.
    - Schedule ready operators (parents complete) from highest priority first.
    - Use operator memory peak estimates if present; on OOM failure, retry with multiplicative RAM backoff.
    - Mild per-priority CPU caps to preserve headroom for interactive/query latency.
    """
    from collections import deque

    # Per-priority pipeline queues (FIFO within a priority class)
    s.q_query = deque()
    s.q_interactive = deque()
    s.q_batch = deque()

    # RAM backoff state for retries keyed by (pipeline_id, op_key)
    # value: multiplicative factor applied to estimated RAM (or to a default guess)
    s.op_ram_backoff = {}

    # For mild fairness across pools/ticks (not strict), rotate which pool we start filling from
    s.pool_rr = 0


@register_scheduler(key="scheduler_est_007")
def scheduler_est_007_scheduler(s, results, pipelines):
    """
    Scheduler step:
    - Ingest new pipelines into per-priority queues.
    - Process execution results: on OOM-like failures, increase RAM backoff for that operator.
    - For each pool, greedily assign ready ops from highest priority queues first, respecting
      available CPU/RAM and per-priority CPU caps.
    """
    from collections import deque

    # ---- helpers ----
    def _prio_rank(prio):
        # Higher rank == higher priority
        if prio == Priority.QUERY:
            return 3
        if prio == Priority.INTERACTIVE:
            return 2
        return 1  # Priority.BATCH_PIPELINE and any unknown fall here

    def _get_queue(prio):
        if prio == Priority.QUERY:
            return s.q_query
        if prio == Priority.INTERACTIVE:
            return s.q_interactive
        return s.q_batch

    def _op_key(op):
        # Prefer stable identifiers if they exist; otherwise fallback to object's id.
        for attr in ("op_id", "operator_id", "id", "name"):
            if hasattr(op, attr):
                v = getattr(op, attr)
                # avoid callable or empty
                if v is not None and not callable(v):
                    return (attr, str(v))
        return ("pyid", str(id(op)))

    def _looks_like_oom(err):
        if err is None:
            return False
        s_err = str(err).lower()
        # Keep permissive; simulator may label differently.
        return ("oom" in s_err) or ("out of memory" in s_err) or ("out-of-memory" in s_err) or ("memory" in s_err and "exceed" in s_err)

    def _estimate_mem_gb(op):
        # Estimator interface: op.estimate.mem_peak_gb may be float or None
        est = None
        if hasattr(op, "estimate") and op.estimate is not None and hasattr(op.estimate, "mem_peak_gb"):
            est = op.estimate.mem_peak_gb
        try:
            if est is None:
                return None
            est_f = float(est)
            if est_f > 0:
                return est_f
        except Exception:
            return None
        return None

    def _cpu_cap(pool, prio):
        # Mild caps so batch doesn't crowd out interactive/query latency.
        # Query: allow full pool if available.
        # Interactive: up to 75% of pool.
        # Batch: up to 50% of pool.
        max_cpu = float(pool.max_cpu_pool)
        if prio == Priority.QUERY:
            return max_cpu
        if prio == Priority.INTERACTIVE:
            return max(1.0, 0.75 * max_cpu)
        return max(1.0, 0.50 * max_cpu)

    def _default_mem_guess(pool, prio):
        # Small default guess to enable concurrency; relies on OOM-retry to correct.
        # Query/Interactive get a bit more to avoid repeated OOMs.
        max_ram = float(pool.max_ram_pool)
        if prio == Priority.QUERY:
            return max(1.0, 0.20 * max_ram)
        if prio == Priority.INTERACTIVE:
            return max(1.0, 0.15 * max_ram)
        return max(1.0, 0.10 * max_ram)

    def _choose_ram(pool, op, pipeline_id, prio, avail_ram):
        # Use estimate if present; allocate close (10% headroom). On failures, apply backoff.
        est = _estimate_mem_gb(op)
        base = est if est is not None else _default_mem_guess(pool, prio)
        backoff = s.op_ram_backoff.get((pipeline_id, _op_key(op)), 1.0)

        # Close-to-estimate + modest headroom (aggressive by design; retry on OOM)
        ram_req = float(base) * float(backoff) * 1.10

        # Ensure at least a small positive allocation
        ram_req = max(0.25, ram_req)

        # Must fit in pool available RAM to assign now
        if ram_req > float(avail_ram):
            return None
        return ram_req

    def _choose_cpu(pool, prio, avail_cpu):
        cap = _cpu_cap(pool, prio)
        # Give as much as possible up to cap (helps latency), but at least 1 if any CPU exists.
        cpu = min(float(avail_cpu), float(cap))
        if cpu <= 0:
            return None
        return max(1.0, cpu) if float(avail_cpu) >= 1.0 else cpu

    # ---- ingest new pipelines ----
    for p in pipelines:
        _get_queue(p.priority).append(p)

    # ---- learn from results (OOM backoff) ----
    for r in results:
        try:
            if r is not None and r.failed() and _looks_like_oom(r.error):
                # For each op in this failed container, bump RAM for retry.
                # Keep a cap to avoid runaway allocations.
                for op in getattr(r, "ops", []) or []:
                    key = (getattr(r, "pipeline_id", None), _op_key(op))
                    # pipeline_id may not be on result; fallback to None still works within a run if unique enough
                    prev = float(s.op_ram_backoff.get(key, 1.0))
                    s.op_ram_backoff[key] = min(prev * 2.0, 32.0)
        except Exception:
            # Be robust to interface differences; don't fail the scheduler
            pass

    # Early exit if no actionable updates
    if not pipelines and not results and not (s.q_query or s.q_interactive or s.q_batch):
        return [], []

    suspensions = []
    assignments = []

    # ---- scheduling loop across pools ----
    num_pools = s.executor.num_pools
    if num_pools <= 0:
        return suspensions, assignments

    # Rotate start pool for slight fairness across pools
    start = int(s.pool_rr) % int(num_pools)
    pool_order = list(range(start, num_pools)) + list(range(0, start))
    s.pool_rr = (start + 1) % num_pools

    # Utility: pop-next runnable pipeline from a given queue, but keep non-runnable ones for later
    def _pop_next_runnable(q):
        # bounded scan to preserve FIFO-ish behavior without infinite loops
        n = len(q)
        for _ in range(n):
            p = q.popleft()
            st = p.runtime_status()
            # Drop completed pipelines
            if st.is_pipeline_successful():
                continue
            # Keep pipeline in play; caller will decide if it's runnable now
            return p
        return None

    # Try to assign multiple ops per pool tick, highest priority first
    for pool_id in pool_order:
        pool = s.executor.pools[pool_id]
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)
        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        made_progress = True
        # Greedy packing: keep assigning while we can fit something useful
        while made_progress and avail_cpu > 0 and avail_ram > 0:
            made_progress = False

            # Priority order: QUERY -> INTERACTIVE -> BATCH
            for q in (s.q_query, s.q_interactive, s.q_batch):
                if not q:
                    continue

                # Peek runnable candidate (rotate within same priority)
                p = _pop_next_runnable(q)
                if p is None:
                    continue

                st = p.runtime_status()
                if st.is_pipeline_successful():
                    # already done
                    continue

                # Only schedule ops whose parents are complete
                op_list = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
                if not op_list:
                    # Not ready yet; put it back to end of its priority queue
                    q.append(p)
                    continue

                op = op_list[0]
                prio = p.priority

                cpu = _choose_cpu(pool, prio, avail_cpu)
                if cpu is None or cpu <= 0:
                    # No CPU; put back and stop trying this pool
                    q.appendleft(p)
                    break

                ram = _choose_ram(pool, op, p.pipeline_id, prio, avail_ram)
                if ram is None or ram <= 0:
                    # Not enough RAM right now; put back and try next priority/pipeline
                    q.append(p)
                    continue

                # Assign the op
                assignments.append(
                    Assignment(
                        ops=op_list,
                        cpu=cpu,
                        ram=ram,
                        priority=prio,
                        pool_id=pool_id,
                        pipeline_id=p.pipeline_id,
                    )
                )

                # Update available resources pessimistically (assume immediate reservation)
                avail_cpu -= float(cpu)
                avail_ram -= float(ram)

                # Put pipeline back for further stages/ops in future ticks
                q.append(p)

                made_progress = True
                break  # after an assignment, re-check highest priority from the top

    return suspensions, assignments
