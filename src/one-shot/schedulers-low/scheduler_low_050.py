# policy_key: scheduler_low_050
# reasoning_effort: low
# model: gpt-5.2-2025-12-11
# llm_cost: 0.041397
# generation_seconds: 44.24
# generated_at: 2026-03-14T02:40:47.453670
@register_scheduler_init(key="scheduler_low_050")
def scheduler_low_050_init(s):
    """
    Priority-aware, low-complexity improvement over naive FIFO.

    Key ideas (kept intentionally simple / robust):
    - Maintain separate FIFO queues per priority (QUERY > INTERACTIVE > BATCH_PIPELINE).
    - Allocate smaller, per-op CPU/RAM "quanta" to run more concurrent work, improving latency
      under contention (instead of giving a single op the whole pool).
    - On failures (assumed often OOM / under-provision), retry the same op later with
      increased RAM (exponential backoff), capped by pool max RAM.
    - Avoid preemption (no reliable running-container inventory exposed in the template);
      focus on safer, incremental gains first.
    """
    # Separate queues per priority for obvious latency wins
    s.queues = {}  # priority -> List[Pipeline]

    # Per-(pipeline, op) resource hints that adapt based on observed failures
    s.op_ram_hint = {}  # (pipeline_id, op_key) -> ram
    s.op_cpu_hint = {}  # (pipeline_id, op_key) -> cpu

    # Simple round-robin pointer per priority to avoid head-of-line blocking in a queue
    s.rr_index = {}  # priority -> int

    # Policy knobs (kept small and conservative)
    s.max_ops_per_pool_per_tick = 8  # limit churn / too many tiny assignments
    s.min_cpu_quantum = 1.0
    s.min_ram_quantum = 0.5  # in whatever RAM units the simulator uses

    # Default quanta by priority (fractions of pool max)
    s.cpu_frac = {}
    s.ram_frac = {}

    # Failure backoff factors
    s.ram_backoff = 2.0
    s.cpu_backoff = 1.5

    # Bounds on backoff to keep it sane
    s.max_cpu_frac = 1.0
    s.max_ram_frac = 1.0


@register_scheduler(key="scheduler_low_050")
def scheduler_low_050(s, results, pipelines):
    """
    Scheduler step:
    - Ingest new pipelines into per-priority queues.
    - Update per-op hints from execution results (increase RAM/CPU on failures).
    - For each pool, repeatedly assign ready ops in priority order using per-op quanta
      until resources are insufficient or we hit a per-tick limit.
    """
    def _priority_order():
        # Highest to lowest priority
        return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]

    def _ensure_priority_state(prio):
        if prio not in s.queues:
            s.queues[prio] = []
        if prio not in s.rr_index:
            s.rr_index[prio] = 0

    def _get_op_key(op):
        # Try common identifiers; fall back to object identity (stable if op objects persist).
        for attr in ("op_id", "operator_id", "id", "name"):
            if hasattr(op, attr):
                v = getattr(op, attr)
                # Avoid callable or complex objects; stringify to be safe as dict key
                try:
                    return str(v)
                except Exception:
                    pass
        return str(id(op))

    def _base_quanta_for_pool(pool, prio):
        # Set default fractions lazily once we see pool sizes
        # QUERY: more aggressive; INTERACTIVE: medium; BATCH: smaller
        if not s.cpu_frac:
            s.cpu_frac = {
                Priority.QUERY: 0.50,
                Priority.INTERACTIVE: 0.35,
                Priority.BATCH_PIPELINE: 0.25,
            }
        if not s.ram_frac:
            s.ram_frac = {
                Priority.QUERY: 0.40,
                Priority.INTERACTIVE: 0.30,
                Priority.BATCH_PIPELINE: 0.20,
            }

        cpu = max(s.min_cpu_quantum, pool.max_cpu_pool * s.cpu_frac.get(prio, 0.25))
        ram = max(s.min_ram_quantum, pool.max_ram_pool * s.ram_frac.get(prio, 0.20))
        # Never exceed pool maxima
        cpu = min(cpu, pool.max_cpu_pool)
        ram = min(ram, pool.max_ram_pool)
        return cpu, ram

    def _hinted_quanta(pool, prio, pipeline_id, op):
        op_key = _get_op_key(op)
        base_cpu, base_ram = _base_quanta_for_pool(pool, prio)

        cpu = s.op_cpu_hint.get((pipeline_id, op_key), base_cpu)
        ram = s.op_ram_hint.get((pipeline_id, op_key), base_ram)

        # Clamp to pool max
        cpu = min(max(s.min_cpu_quantum, cpu), pool.max_cpu_pool * s.max_cpu_frac)
        ram = min(max(s.min_ram_quantum, ram), pool.max_ram_pool * s.max_ram_frac)
        return cpu, ram

    def _queue_nonterminal(p):
        st = p.runtime_status()
        # Drop completed pipelines
        if st.is_pipeline_successful():
            return False
        # If it has failures, we still keep it: our retry logic can recover from OOM-like failures.
        return True

    # Ingest new pipelines
    for p in pipelines:
        _ensure_priority_state(p.priority)
        s.queues[p.priority].append(p)

    # If nothing changed, no action
    if not pipelines and not results:
        return [], []

    # Update hints based on results: if failed, back off RAM (and modest CPU)
    # We avoid trying to interpret exact error strings; treat failure as "needs more".
    for r in results:
        if not hasattr(r, "ops") or not r.ops:
            continue
        if not hasattr(r, "failed") or not callable(r.failed):
            continue
        if not r.failed():
            continue

        # Ramp hints for each op in the assignment
        for op in r.ops:
            # We don't have pipeline_id on result; so we can't reliably store per pipeline.
            # However, Assignment includes pipeline_id, but result may not. Fall back to per-op-only key.
            # If pipeline_id exists on result, use it; else use wildcard.
            pipeline_id = getattr(r, "pipeline_id", "*")
            op_key = _get_op_key(op)

            # Use executed allocations if present, else base them later.
            prev_ram = getattr(r, "ram", None)
            prev_cpu = getattr(r, "cpu", None)

            # Backoff RAM aggressively; CPU moderately.
            if prev_ram is not None:
                new_ram = prev_ram * s.ram_backoff
            else:
                # If we don't know, nudge existing hint upward
                new_ram = s.op_ram_hint.get((pipeline_id, op_key), s.min_ram_quantum) * s.ram_backoff

            if prev_cpu is not None:
                new_cpu = prev_cpu * s.cpu_backoff
            else:
                new_cpu = s.op_cpu_hint.get((pipeline_id, op_key), s.min_cpu_quantum) * s.cpu_backoff

            # Store (clamping happens at assignment time when pool max is known)
            s.op_ram_hint[(pipeline_id, op_key)] = new_ram
            s.op_cpu_hint[(pipeline_id, op_key)] = new_cpu

    suspensions = []  # no preemption in this incremental version
    assignments = []

    # For each pool, schedule multiple small ops, honoring priority order
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool
        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        ops_assigned = 0

        # Keep attempting assignments while we have resources
        while ops_assigned < s.max_ops_per_pool_per_tick:
            made_assignment = False

            # Visit priorities in order, and within each, round-robin pipelines
            for prio in _priority_order():
                _ensure_priority_state(prio)
                q = s.queues[prio]
                if not q:
                    continue

                # Round-robin scan to find a pipeline with a ready op
                start = s.rr_index[prio] % max(1, len(q))
                idx = start
                scanned = 0

                chosen_pipeline = None
                chosen_op_list = None

                while scanned < len(q):
                    p = q[idx]
                    if not _queue_nonterminal(p):
                        # Drop completed pipelines
                        q.pop(idx)
                        if idx < start and start > 0:
                            start -= 1
                        if not q:
                            break
                        idx = idx % len(q)
                        continue

                    st = p.runtime_status()
                    # Only schedule ops whose parents are complete
                    op_list = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
                    if op_list:
                        chosen_pipeline = p
                        chosen_op_list = op_list
                        break

                    idx = (idx + 1) % len(q)
                    scanned += 1

                if chosen_pipeline is None or not chosen_op_list:
                    continue

                # Compute quanta for this op and check fit
                op = chosen_op_list[0]
                cpu_q, ram_q = _hinted_quanta(pool, prio, chosen_pipeline.pipeline_id, op)

                if cpu_q > avail_cpu or ram_q > avail_ram:
                    # Can't fit even a single quantum now; try next priority/pipeline
                    continue

                assignments.append(
                    Assignment(
                        ops=chosen_op_list,
                        cpu=cpu_q,
                        ram=ram_q,
                        priority=chosen_pipeline.priority,
                        pool_id=pool_id,
                        pipeline_id=chosen_pipeline.pipeline_id,
                    )
                )

                # Update available resources pessimistically for this tick's packing
                avail_cpu -= cpu_q
                avail_ram -= ram_q
                ops_assigned += 1
                made_assignment = True

                # Advance RR pointer to the next pipeline after the chosen one
                if q:
                    # Find current index of chosen_pipeline; advance by one
                    try:
                        cur = q.index(chosen_pipeline)
                        s.rr_index[prio] = (cur + 1) % len(q)
                    except Exception:
                        s.rr_index[prio] = (s.rr_index[prio] + 1) % max(1, len(q))

                break  # after a successful assignment, restart from highest priority

            if not made_assignment:
                break

    # Lightweight cleanup: remove completed pipelines from queues to reduce scanning overhead
    for prio in list(s.queues.keys()):
        q = s.queues[prio]
        if not q:
            continue
        new_q = []
        for p in q:
            st = p.runtime_status()
            if not st.is_pipeline_successful():
                new_q.append(p)
        s.queues[prio] = new_q
        if new_q:
            s.rr_index[prio] = s.rr_index[prio] % len(new_q)
        else:
            s.rr_index[prio] = 0

    return suspensions, assignments
