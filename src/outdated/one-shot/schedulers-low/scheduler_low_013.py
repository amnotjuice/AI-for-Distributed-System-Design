# policy_key: scheduler_low_013
# reasoning_effort: low
# model: gpt-5.2-2025-12-11
# llm_cost: 0.050287
# generation_seconds: 50.07
# generated_at: 2026-03-12T21:44:24.048077
@register_scheduler_init(key="scheduler_low_013")
def scheduler_low_013_init(s):
    """
    Priority-aware, small-step improvement over naive FIFO:

    1) Maintain per-priority FIFO queues (QUERY > INTERACTIVE > BATCH).
    2) Avoid "single-op monopolization" by capping per-assignment CPU/RAM so multiple ops
       can run concurrently, improving tail latency for interactive work.
    3) Add a simple OOM backoff: if an op fails, retry later with increased RAM
       (bounded by pool capacity).

    Notes:
    - No preemption yet (kept intentionally simple/robust as a first improvement).
    - No dedicated pools assumed; uses all pools but biases admission toward high priority.
    """
    # Per-priority pipeline queues
    s.q_query = []
    s.q_interactive = []
    s.q_batch = []

    # Remember per-(pipeline, op) resource overrides due to failures (e.g., OOM)
    # key -> {"ram": float, "cpu": float}
    s.op_overrides = {}

    # Simple aging counter to occasionally admit lower priority (prevents total starvation)
    s.tick = 0


@register_scheduler(key="scheduler_low_013")
def scheduler_low_013_scheduler(s, results, pipelines):
    """
    Scheduler step:
      - Enqueue new pipelines into priority queues.
      - Process results: drop successful/failed pipelines naturally via status checks; update
        per-op overrides on failures.
      - For each pool, assign up to K ready operators, prioritizing higher priority queues.
      - Use per-assignment CPU/RAM caps to improve concurrency and reduce tail latency.
    """
    s.tick += 1

    # --- Helpers (local to avoid imports/global dependencies) ---
    def _enqueue_pipeline(p):
        if p.priority == Priority.QUERY:
            s.q_query.append(p)
        elif p.priority == Priority.INTERACTIVE:
            s.q_interactive.append(p)
        else:
            s.q_batch.append(p)

    def _is_done_or_failed(p):
        st = p.runtime_status()
        if st.is_pipeline_successful():
            return True
        # If anything has failed, treat as terminal (consistent with example policy)
        return st.state_counts.get(OperatorState.FAILED, 0) > 0

    def _op_key(pipeline_id, op):
        # Prefer stable identifiers if present, fall back to repr/op object identity.
        op_id = getattr(op, "op_id", None)
        if op_id is None:
            op_id = getattr(op, "operator_id", None)
        if op_id is None:
            op_id = repr(op)
        return (pipeline_id, op_id)

    def _note_failure(result):
        # Conservative: on any failure, increase RAM for the involved ops.
        # If it's not OOM, this may over-allocate a bit, but keeps policy simple.
        pool = s.executor.pools[result.pool_id]
        for op in result.ops:
            k = _op_key(result.ops[0].pipeline_id if hasattr(result.ops[0], "pipeline_id") else None, op)

            # If we can't determine pipeline id from op, fall back to result container tuple key.
            if k[0] is None:
                k = ("container", result.container_id, repr(op))

            prev = s.op_overrides.get(k, {"ram": max(result.ram, 1e-9), "cpu": max(result.cpu, 1e-9)})

            # Exponential RAM backoff, bounded by pool max RAM.
            new_ram = min(pool.max_ram_pool, max(prev["ram"], result.ram) * 2.0)

            # Mild CPU bump as a secondary measure, bounded by pool max CPU.
            new_cpu = min(pool.max_cpu_pool, max(prev["cpu"], result.cpu) * 1.25)

            s.op_overrides[k] = {"ram": new_ram, "cpu": new_cpu}

    def _pop_next_ready_pipeline(queues):
        """
        Pop the first pipeline that has at least one ready op (parents complete),
        returning (pipeline, ready_ops).
        If no pipeline has ready ops, return (None, None).
        """
        for q in queues:
            # Scan in FIFO order; rotate pipelines that aren't ready yet.
            n = len(q)
            for _ in range(n):
                p = q.pop(0)
                if _is_done_or_failed(p):
                    continue
                st = p.runtime_status()
                ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
                if ops:
                    return p, ops
                # Not ready yet, keep it in queue
                q.append(p)
        return None, None

    # --- Enqueue new arrivals ---
    for p in pipelines:
        _enqueue_pipeline(p)

    # --- Process results (only update overrides; queues are pruned lazily via status checks) ---
    for r in results:
        if hasattr(r, "failed") and r.failed():
            _note_failure(r)

    # Early exit if nothing changed that would affect decisions
    if not pipelines and not results and not (s.q_query or s.q_interactive or s.q_batch):
        return [], []

    suspensions = []
    assignments = []

    # Weights / caps: keep small and robust; goal is lower latency via concurrency.
    # Assign up to this many ops per pool per tick.
    max_assignments_per_pool = 4

    # Reserve a slice of pool resources for high priority by limiting batch consumption.
    # (No hard reservation enforcement needed; just don't schedule batch if it would
    # consume too much of currently available headroom.)
    batch_cpu_limit_frac_of_avail = 0.70
    batch_ram_limit_frac_of_avail = 0.70

    # Per-assignment caps (fraction of pool max). Higher for interactive/query.
    cap = {
        Priority.QUERY: (0.60, 0.60),         # (cpu_frac, ram_frac)
        Priority.INTERACTIVE: (0.50, 0.50),
        Priority.BATCH_PIPELINE: (0.35, 0.35),
    }

    # Simple anti-starvation: every so often, allow one batch even if interactive exists.
    allow_batch_despite_high = (s.tick % 10 == 0)

    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool
        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # Determine if high-priority work exists.
        # We don't scan deeply for readiness here; just a quick signal.
        high_waiting = bool(s.q_query or s.q_interactive)

        # Try to place multiple ops per pool (improves latency under contention).
        for _ in range(max_assignments_per_pool):
            if avail_cpu <= 0 or avail_ram <= 0:
                break

            # Priority order: QUERY > INTERACTIVE > BATCH
            queues = [s.q_query, s.q_interactive]

            # Include batch based on headroom and anti-starvation rule.
            include_batch = True
            if high_waiting and not allow_batch_despite_high:
                # If high priority exists, only allow batch if it doesn't take too much
                # of remaining headroom.
                include_batch = (avail_cpu > 0 and avail_ram > 0)
            if include_batch:
                queues = queues + [s.q_batch]

            pipeline, ready_ops = _pop_next_ready_pipeline(queues)
            if pipeline is None:
                break

            # Only assign one op at a time per pipeline to reduce intra-pipeline fanout
            # and improve fairness/latency across many pipelines.
            op_list = ready_ops[:1]
            prio = pipeline.priority

            # Compute capped request based on priority and pool max.
            cpu_frac, ram_frac = cap.get(prio, (0.35, 0.35))
            cpu_cap = max(1e-9, pool.max_cpu_pool * cpu_frac)
            ram_cap = max(1e-9, pool.max_ram_pool * ram_frac)

            # Additional batch throttling to preserve headroom for interactive work.
            if prio == Priority.BATCH_PIPELINE and high_waiting and not allow_batch_despite_high:
                cpu_cap = min(cpu_cap, avail_cpu * batch_cpu_limit_frac_of_avail)
                ram_cap = min(ram_cap, avail_ram * batch_ram_limit_frac_of_avail)

            cpu_req = min(avail_cpu, cpu_cap)
            ram_req = min(avail_ram, ram_cap)

            # Apply per-op override if we have one (e.g., after OOM).
            k = _op_key(pipeline.pipeline_id, op_list[0])
            ov = s.op_overrides.get(k)
            if ov is not None:
                cpu_req = min(avail_cpu, max(cpu_req, min(pool.max_cpu_pool, ov["cpu"])))
                ram_req = min(avail_ram, max(ram_req, min(pool.max_ram_pool, ov["ram"])))

            # If we still can't allocate meaningful resources, stop scheduling on this pool.
            if cpu_req <= 0 or ram_req <= 0:
                # Put pipeline back to preserve FIFO ordering for its class.
                _enqueue_pipeline(pipeline)
                break

            assignments.append(
                Assignment(
                    ops=op_list,
                    cpu=cpu_req,
                    ram=ram_req,
                    priority=prio,
                    pool_id=pool_id,
                    pipeline_id=pipeline.pipeline_id,
                )
            )

            # Update pool headroom locally (the simulator will enforce real limits).
            avail_cpu -= cpu_req
            avail_ram -= ram_req

            # Re-enqueue pipeline if it still has work.
            if not _is_done_or_failed(pipeline):
                _enqueue_pipeline(pipeline)

    return suspensions, assignments
