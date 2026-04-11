# policy_key: scheduler_medium_042
# reasoning_effort: medium
# exp: reasoning
# model: gpt-5.2-2025-12-11
# llm_cost: 0.064873
# generation_seconds: 84.09
# generated_at: 2026-04-09T23:26:30.495552
@register_scheduler_init(key="scheduler_medium_042")
def scheduler_medium_042_init(s):
    """
    Priority-aware, completion-biased scheduler.

    Main ideas:
    - Strictly prioritize QUERY and INTERACTIVE to reduce the weighted objective.
    - Keep BATCH making progress via weighted round-robin and anti-starvation counters.
    - Avoid OOM-induced failures by (a) starting with conservative RAM fractions and
      (b) learning per-operator RAM hints from failures (OOM -> exponential RAM bump).
    - Use pools preferentially: pool 0 is "latency pool" for QUERY/INTERACTIVE (if >1 pool),
      remaining pools absorb BATCH. Batch may spill into pool 0 only when no HP demand exists.
    """
    from collections import deque

    s.q_query = deque()
    s.q_interactive = deque()
    s.q_batch = deque()

    # Learned resource hints keyed by (pipeline_id, op_key)
    s.op_ram_hint = {}      # bytes/GB units as used by the simulator
    s.op_retry_count = {}   # retries per operator instance

    # Anti-starvation: ensure batch eventually runs even under HP churn
    s.non_batch_since_last_batch = 0

    # Conservative base allocations per priority (fractions of pool max)
    s.base_ram_frac = {
        Priority.QUERY: 0.55,
        Priority.INTERACTIVE: 0.45,
        Priority.BATCH_PIPELINE: 0.30,
    }
    s.base_cpu_frac = {
        Priority.QUERY: 0.60,
        Priority.INTERACTIVE: 0.50,
        Priority.BATCH_PIPELINE: 0.35,
    }

    # Soft admission thresholds: require this fraction of target RAM to start an op
    # (HP is stricter to avoid costly failures; batch is a bit more permissive)
    s.ram_admit_frac = {
        Priority.QUERY: 1.00,
        Priority.INTERACTIVE: 0.95,
        Priority.BATCH_PIPELINE: 0.80,
    }

    # Retry policy
    s.max_retries_per_op = 6


@register_scheduler(key="scheduler_medium_042")
def scheduler_medium_042(s, results, pipelines):
    """
    Scheduler step:
    - Enqueue new pipelines by priority.
    - Process results to learn from failures (especially OOM) and update RAM hints.
    - Create assignments across pools with a priority-first, fairness-aware policy:
        * Prefer QUERY then INTERACTIVE, then BATCH.
        * If multiple pools: pool 0 serves HP; batch goes to other pools unless no HP waiting.
    """
    from collections import deque

    def _prio_queue(prio):
        if prio == Priority.QUERY:
            return s.q_query
        if prio == Priority.INTERACTIVE:
            return s.q_interactive
        return s.q_batch

    def _op_key(pipeline_id, op):
        # Try common identifiers; fall back to object id (stable during simulation).
        for attr in ("op_id", "operator_id", "id", "name"):
            if hasattr(op, attr):
                v = getattr(op, attr)
                if isinstance(v, (int, str)):
                    return (pipeline_id, v)
        return (pipeline_id, id(op))

    def _is_oom_error(err):
        if err is None:
            return False
        try:
            msg = str(err).lower()
        except Exception:
            return False
        return ("oom" in msg) or ("out of memory" in msg) or ("killed" in msg) or ("cuda" in msg and "memory" in msg)

    def _pipeline_done_or_terminal(p):
        st = p.runtime_status()
        if st.is_pipeline_successful():
            return True
        # Do not treat FAILED as terminal: we intentionally retry to avoid 720s penalty.
        return False

    def _pop_next_ready_pipeline(q, require_parents_complete=True):
        """
        Round-robin over a deque to find a pipeline with at least one ready operator.
        Returns (pipeline, op_list) or (None, None).
        """
        if not q:
            return None, None

        n = len(q)
        for _ in range(n):
            p = q.popleft()
            if _pipeline_done_or_terminal(p):
                continue

            st = p.runtime_status()
            op_list = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=require_parents_complete)[:1]
            if op_list:
                return p, op_list

            # Not ready now; rotate to the back
            q.append(p)

        return None, None

    def _target_resources(pool, prio, op_list, pipeline_id):
        """
        Compute CPU/RAM targets using:
        - base fractions of pool max
        - learned per-op RAM hint (from previous failures)
        """
        base_ram = pool.max_ram_pool * s.base_ram_frac.get(prio, 0.35)
        base_cpu = pool.max_cpu_pool * s.base_cpu_frac.get(prio, 0.40)

        # Learned RAM hint for the (pipeline, op) instance
        hint_ram = None
        if op_list:
            k = _op_key(pipeline_id, op_list[0])
            hint_ram = s.op_ram_hint.get(k)

        target_ram = base_ram if hint_ram is None else max(base_ram, hint_ram)
        target_ram = min(target_ram, pool.max_ram_pool)

        # Keep CPU at least a small floor when possible; use fractions otherwise.
        # (CPU may be float in the simulator; don't force int.)
        cpu_floor = min(1.0, pool.max_cpu_pool)
        target_cpu = max(cpu_floor, base_cpu)
        target_cpu = min(target_cpu, pool.max_cpu_pool)

        return target_cpu, target_ram

    # Enqueue new pipelines
    for p in pipelines:
        _prio_queue(p.priority).append(p)

    # Learn from results (especially failures) to reduce repeat failures and penalties
    for r in results:
        if not getattr(r, "failed", lambda: False)():
            continue

        pool_id = getattr(r, "pool_id", 0)
        pool_id = 0 if pool_id is None else pool_id
        pool = s.executor.pools[pool_id]

        ops = getattr(r, "ops", None) or []
        for op in ops:
            # Best-effort pipeline id extraction
            pipeline_id = getattr(op, "pipeline_id", None)
            if pipeline_id is None:
                # If unknown, we still keep a global key (less ideal but prevents crashes)
                pipeline_id = "unknown"

            k = _op_key(pipeline_id, op)
            prev_retries = s.op_retry_count.get(k, 0)
            if prev_retries >= s.max_retries_per_op:
                continue
            s.op_retry_count[k] = prev_retries + 1

            # If OOM-ish failure, aggressively bump RAM hint; otherwise mild bump.
            allocated_ram = getattr(r, "ram", None)
            allocated_ram = allocated_ram if allocated_ram is not None else (pool.max_ram_pool * 0.25)
            bump = 2.0 if _is_oom_error(getattr(r, "error", None)) else 1.25

            prev_hint = s.op_ram_hint.get(k, pool.max_ram_pool * 0.20)
            new_hint = max(prev_hint, allocated_ram * bump)
            new_hint = min(new_hint, pool.max_ram_pool)
            s.op_ram_hint[k] = new_hint

    # Early exit if nothing changed
    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    def _hp_waiting():
        # Approximate "HP demand exists" by non-empty deques; completed pipelines are culled during pop.
        return (len(s.q_query) > 0) or (len(s.q_interactive) > 0)

    def _choose_next_priority():
        """
        Weighted selection with anti-starvation:
        - Primarily serve QUERY, then INTERACTIVE.
        - Ensure BATCH is not indefinitely starved by injecting it periodically.
        """
        if len(s.q_batch) > 0 and s.non_batch_since_last_batch >= 24:
            return Priority.BATCH_PIPELINE
        if len(s.q_query) > 0:
            return Priority.QUERY
        if len(s.q_interactive) > 0:
            return Priority.INTERACTIVE
        if len(s.q_batch) > 0:
            return Priority.BATCH_PIPELINE
        return None

    def _attempt_fill_pool(pool_id, allow_batch):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool

        # Conservative loop bounds to avoid long spins when queues contain non-ready pipelines.
        # Each iteration schedules at most one operator.
        max_attempts = 32
        attempts = 0

        while avail_cpu > 0 and avail_ram > 0 and attempts < max_attempts:
            attempts += 1

            # Decide which priority to try next (HP first; batch gated by allow_batch)
            prio = _choose_next_priority()
            if prio is None:
                break
            if prio == Priority.BATCH_PIPELINE and not allow_batch:
                # If batch not allowed in this pool, try to pick HP only.
                if len(s.q_query) > 0:
                    prio = Priority.QUERY
                elif len(s.q_interactive) > 0:
                    prio = Priority.INTERACTIVE
                else:
                    break

            q = _prio_queue(prio)
            p, op_list = _pop_next_ready_pipeline(q, require_parents_complete=True)
            if p is None:
                # No ready op found for that priority; try others next loop.
                # To avoid spinning, temporarily relax: if HP empty, allow batch if permitted.
                if prio != Priority.BATCH_PIPELINE and allow_batch and len(s.q_batch) > 0:
                    p, op_list = _pop_next_ready_pipeline(s.q_batch, require_parents_complete=True)
                    prio = Priority.BATCH_PIPELINE if p is not None else prio
                if p is None:
                    break

            target_cpu, target_ram = _target_resources(pool, prio, op_list, p.pipeline_id)

            # Admission control to avoid undersized runs that would likely fail (720s penalty is brutal).
            admit_frac = s.ram_admit_frac.get(prio, 0.90)
            if avail_ram < (target_ram * admit_frac) or avail_cpu <= 0:
                # Put pipeline back and stop trying to fill this pool (avoid thrashing).
                q.appendleft(p)
                break

            cpu = min(avail_cpu, target_cpu)
            ram = min(avail_ram, target_ram)

            # Create assignment
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

            # Update local availability
            avail_cpu -= cpu
            avail_ram -= ram

            # Requeue pipeline for later stages
            q.append(p)

            # Starvation accounting
            if prio == Priority.BATCH_PIPELINE:
                s.non_batch_since_last_batch = 0
            else:
                s.non_batch_since_last_batch += 1

    num_pools = s.executor.num_pools

    if num_pools <= 1:
        # Single pool: strict HP-first, batch still gets periodic access via anti-starvation.
        _attempt_fill_pool(0, allow_batch=True)
        return suspensions, assignments

    # Multi-pool behavior:
    # - Pool 0 is latency-focused: do not run batch there if any HP demand exists.
    # - Pools 1..N-1 run everything, but tend to absorb batch.
    hp_waiting_now = _hp_waiting()

    # First, fill pool 0 with HP work (and batch only if no HP waiting)
    _attempt_fill_pool(0, allow_batch=(not hp_waiting_now))

    # Then, fill remaining pools; allow batch always there
    for pool_id in range(1, num_pools):
        _attempt_fill_pool(pool_id, allow_batch=True)

    return suspensions, assignments
