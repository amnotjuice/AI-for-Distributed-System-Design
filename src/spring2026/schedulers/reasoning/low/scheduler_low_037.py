# policy_key: scheduler_low_037
# reasoning_effort: low
# model: gpt-5.2-2025-12-11
# llm_cost: 0.034103
# generation_seconds: 35.97
# generated_at: 2026-03-14T02:30:51.866401
@register_scheduler_init(key="scheduler_low_037")
def scheduler_low_037_init(s):
    """Priority-aware, latency-oriented scheduler (incremental improvement over naive FIFO).

    Key changes vs. naive:
      1) Separate queues per priority (QUERY > INTERACTIVE > BATCH) to reduce tail latency.
      2) Avoid monopolizing a pool with a single op by capping per-container CPU/RAM.
      3) Simple OOM-aware retry: if an op fails with an OOM-like error, requeue and increase RAM.
      4) Light fairness: round-robin within each priority by rotating pipelines in the queue.

    Notes/assumptions:
      - We only use interfaces shown in the provided template.
      - Preemption is intentionally NOT implemented yet (needs reliable visibility into running containers).
      - CPU/RAM sizing is heuristic and conservative; Eudoxia will let you iterate based on metrics.
    """
    s.q_query = []
    s.q_interactive = []
    s.q_batch = []

    # Track pipelines that had non-OOM failures so we stop retrying them.
    s.dead_pipelines = set()

    # Adaptive per-operator RAM multiplier for OOM retries: op_key -> multiplier (1,2,4,...)
    s.op_ram_mult = {}

    # For stable-ish operator keys across ticks.
    def _op_key(op, pipeline_id):
        # Prefer explicit ids if present, else fall back to Python object id.
        oid = getattr(op, "op_id", None)
        if oid is None:
            oid = getattr(op, "operator_id", None)
        if oid is None:
            oid = id(op)
        return (pipeline_id, oid)

    s._op_key = _op_key


@register_scheduler(key="scheduler_low_037")
def scheduler_low_037_scheduler(s, results, pipelines):
    """
    Priority-first scheduling with capped per-container allocations and OOM-aware RAM backoff.
    """
    # --- helpers (kept local; no imports required) ---
    def _is_oom_error(err) -> bool:
        if err is None:
            return False
        msg = str(err).lower()
        # Keep broad; simulator/error strings may vary.
        return ("oom" in msg) or ("out of memory" in msg) or ("memory" in msg and "exceed" in msg)

    def _enqueue_pipeline(p):
        if p.pipeline_id in s.dead_pipelines:
            return
        if p.priority == Priority.QUERY:
            s.q_query.append(p)
        elif p.priority == Priority.INTERACTIVE:
            s.q_interactive.append(p)
        else:
            s.q_batch.append(p)

    def _queues_in_priority_order():
        return [s.q_query, s.q_interactive, s.q_batch]

    def _pop_next_runnable_pipeline():
        # Rotate through priority queues, returning the next pipeline that has an assignable op.
        # We pop from front; if not runnable now, we move it to the back (round-robin).
        for q in _queues_in_priority_order():
            if not q:
                continue
            # Bounded scan to avoid infinite loop.
            n = len(q)
            for _ in range(n):
                p = q.pop(0)
                if p.pipeline_id in s.dead_pipelines:
                    continue
                st = p.runtime_status()
                if st.is_pipeline_successful():
                    continue
                # If any FAILED op exists we still allow retry (ASSIGNABLE_STATES typically includes FAILED).
                op_list = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
                if op_list:
                    return p, op_list
                # Not runnable yet; rotate to back.
                q.append(p)
        return None, None

    def _size_for(priority, pool, op, pipeline_id, avail_cpu, avail_ram):
        # Conservative caps to prevent "single-op takes whole pool" which hurts latency.
        # These are intentionally simple; refine after seeing Eudoxia results.
        max_cpu = pool.max_cpu_pool
        max_ram = pool.max_ram_pool

        # Priority-specific caps (vCPU and RAM fractions).
        if priority == Priority.QUERY:
            cpu_cap = min(2.0, max_cpu * 0.25)
            ram_frac = 0.15
        elif priority == Priority.INTERACTIVE:
            cpu_cap = min(4.0, max_cpu * 0.35)
            ram_frac = 0.25
        else:  # BATCH_PIPELINE
            cpu_cap = min(8.0, max_cpu * 0.60)
            ram_frac = 0.40

        # Ensure we don't request more than currently available.
        cpu = min(avail_cpu, cpu_cap)
        if cpu <= 0:
            cpu = 0

        base_ram = max_ram * ram_frac
        # Apply OOM backoff multiplier per operator.
        op_key = s._op_key(op, pipeline_id)
        mult = s.op_ram_mult.get(op_key, 1.0)
        ram = base_ram * mult

        # Clamp to available and pool max.
        ram = min(ram, avail_ram, max_ram)
        if ram <= 0:
            ram = 0

        return cpu, ram

    # --- ingest new pipelines ---
    for p in pipelines:
        _enqueue_pipeline(p)

    # Early exit if nothing new happened that could change scheduling decisions.
    if not pipelines and not results:
        return [], []

    # --- process results (failures and adaptive retries) ---
    for r in results:
        if not r.failed():
            continue

        # If it's an OOM-like failure, increase RAM multiplier for the op and allow retry.
        if _is_oom_error(r.error):
            # r.ops can be a list; update all ops in the batch result.
            for op in (r.ops or []):
                op_key = s._op_key(op, getattr(r, "pipeline_id", None))
                # If pipeline_id isn't present on result, fall back to op object identity only.
                # This fallback still helps within the same simulation run.
                if op_key[0] is None:
                    op_key = (None, op_key[1])

                cur = s.op_ram_mult.get(op_key, 1.0)
                # Exponential-ish backoff, capped.
                nxt = cur * 2.0
                if nxt > 16.0:
                    nxt = 16.0
                s.op_ram_mult[op_key] = nxt
        else:
            # Non-OOM failure: do not keep retrying this pipeline forever.
            # We don't have a direct pipeline_id on ExecutionResult per the given interface;
            # so we only mark dead if it exists. Otherwise we let pipeline status drop it naturally.
            pid = getattr(r, "pipeline_id", None)
            if pid is not None:
                s.dead_pipelines.add(pid)

    suspensions = []
    assignments = []

    # --- main scheduling loop: fill each pool with prioritized work ---
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool

        # Keep scheduling while there's meaningful headroom.
        # (Using small epsilons to avoid tiny allocations.)
        while avail_cpu > 0.01 and avail_ram > 0.01:
            p, op_list = _pop_next_runnable_pipeline()
            if not p:
                break

            op = op_list[0]
            cpu, ram = _size_for(p.priority, pool, op, p.pipeline_id, avail_cpu, avail_ram)

            # If we cannot allocate a sensible amount, put pipeline back and stop trying this pool now.
            # (Avoid spinning; other pools may have different headroom.)
            if cpu <= 0.01 or ram <= 0.01:
                _enqueue_pipeline(p)
                break

            assignments.append(
                Assignment(
                    ops=op_list,
                    cpu=cpu,
                    ram=ram,
                    priority=p.priority,
                    pool_id=pool_id,
                    pipeline_id=p.pipeline_id,
                )
            )

            # Decrement local headroom and rotate pipeline to back for fairness.
            avail_cpu -= cpu
            avail_ram -= ram
            _enqueue_pipeline(p)

    return suspensions, assignments
