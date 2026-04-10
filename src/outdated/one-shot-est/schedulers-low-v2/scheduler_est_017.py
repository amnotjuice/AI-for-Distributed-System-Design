# policy_key: scheduler_est_017
# reasoning_effort: low
# model: gpt-5.2-2025-12-11
# llm_cost: 0.045476
# generation_seconds: 46.29
# generated_at: 2026-04-01T13:47:58.419474
@register_scheduler_init(key="scheduler_est_017")
def scheduler_est_017_init(s):
    """Priority-aware, latency-focused scheduler with conservative RAM sizing and simple OOM backoff.

    Improvements over naive FIFO:
      - Maintain per-priority queues to avoid head-of-line blocking across mixed workloads.
      - Limit per-op CPU (caps by priority) to allow concurrency and reduce tail latency.
      - Use operator memory estimates conservatively + per-op backoff after failures to reduce repeated OOMs.
      - Schedule multiple ready ops per pool per tick (instead of at most one), improving responsiveness.
    """
    # Per-priority pipeline queues (stable FIFO within each priority class)
    s.queues = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }

    # Per-operator RAM backoff multiplier after failures (likely OOM); key is (pipeline_id, op_key)
    s.ram_backoff = {}

    # Round-robin cursor per priority to avoid repeatedly scanning from the front
    s.rr_cursor = {
        Priority.QUERY: 0,
        Priority.INTERACTIVE: 0,
        Priority.BATCH_PIPELINE: 0,
    }

    # Tuning knobs
    s.min_cpu_per_op = 1.0

    # CPU caps are chosen to favor higher priorities while still allowing concurrency.
    # Caps are interpreted as a fraction of the pool's max vCPU.
    s.cpu_cap_frac = {
        Priority.QUERY: 0.60,
        Priority.INTERACTIVE: 0.45,
        Priority.BATCH_PIPELINE: 0.30,
    }

    # How many ops to try to place per pool per tick (prevents over-scheduling churn)
    s.max_ops_per_pool_per_tick = 8

    # Conservative multiplier on memory estimate (treat estimate as a noisy lower bound)
    s.mem_est_safety = 1.25

    # Default RAM request when no estimate exists: fraction of pool RAM (kept modest to improve packing)
    s.default_ram_frac = 0.20

    # Upper bound on requested RAM (avoid grabbing the entire pool unnecessarily)
    s.max_ram_frac = 0.70

    # Failure backoff (multiply requested RAM for that op key)
    s.backoff_initial = 1.6
    s.backoff_max = 8.0


@register_scheduler(key="scheduler_est_017")
def scheduler_est_017_scheduler(s, results, pipelines):
    """
    Priority-aware multi-assignment scheduler.

    Algorithm sketch:
      1) Enqueue new pipelines by priority.
      2) Learn from failures: increase per-op RAM backoff for failed ops.
      3) For each pool, repeatedly pick the next runnable op from the highest non-empty priority queue,
         allocate conservative RAM and capped CPU, and emit assignments until the pool is tight.
    """
    # ---- Helpers (kept local; no imports needed) ----
    def _prio_order():
        # Highest to lowest.
        return (Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE)

    def _op_key(pipeline_id, op):
        # Prefer stable IDs if present; fallback to object identity.
        oid = getattr(op, "op_id", None)
        if oid is None:
            oid = getattr(op, "operator_id", None)
        if oid is None:
            oid = id(op)
        return (pipeline_id, oid)

    def _is_pipeline_done_or_failed(p):
        st = p.runtime_status()
        if st.is_pipeline_successful():
            return True
        # If any operator is FAILED, treat pipeline as failed (no retry at pipeline level here).
        return st.state_counts.get(OperatorState.FAILED, 0) > 0

    def _get_first_ready_op(p):
        st = p.runtime_status()
        ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        if not ops:
            return None
        return ops[0]

    def _desired_ram_gb(pool, pipeline_id, op):
        # Start from estimate if present; else use a modest default fraction of pool.
        est = None
        if hasattr(op, "estimate") and op.estimate is not None:
            est = getattr(op.estimate, "mem_peak_gb", None)

        if est is None:
            base = max(0.1, pool.max_ram_pool * float(s.default_ram_frac))
        else:
            # Treat estimate as a lower bound; apply safety factor.
            base = max(0.1, float(est) * float(s.mem_est_safety))

        # Apply per-op backoff after failures (often OOM).
        bk = s.ram_backoff.get(_op_key(pipeline_id, op), 1.0)
        base *= float(bk)

        # Clamp to a reasonable fraction of the pool (and at least minimal).
        base = max(0.1, min(base, pool.max_ram_pool * float(s.max_ram_frac)))
        return base

    def _desired_cpu(pool, priority):
        cap = pool.max_cpu_pool * float(s.cpu_cap_frac.get(priority, 0.30))
        return max(float(s.min_cpu_per_op), cap)

    def _try_pick_pipeline_with_ready_op(prio):
        q = s.queues[prio]
        if not q:
            return None, None

        # Round-robin scan to avoid always favoring the front pipeline when it's blocked.
        n = len(q)
        start = s.rr_cursor.get(prio, 0) % n
        for i in range(n):
            idx = (start + i) % n
            p = q[idx]

            # Drop completed/failed pipelines eagerly.
            if _is_pipeline_done_or_failed(p):
                # Remove and adjust cursor.
                q.pop(idx)
                if q:
                    s.rr_cursor[prio] = idx % len(q)
                else:
                    s.rr_cursor[prio] = 0
                return _try_pick_pipeline_with_ready_op(prio)

            op = _get_first_ready_op(p)
            if op is not None:
                # Advance cursor past this pipeline for next time.
                s.rr_cursor[prio] = (idx + 1) % len(q)
                return p, op

        return None, None

    # ---- Ingest new pipelines ----
    for p in pipelines:
        # Ensure unknown priorities still get enqueued (default to batch behavior).
        pr = getattr(p, "priority", Priority.BATCH_PIPELINE)
        if pr not in s.queues:
            pr = Priority.BATCH_PIPELINE
        s.queues[pr].append(p)

    # ---- Learn from execution results (simple RAM backoff on failures) ----
    for r in results:
        if hasattr(r, "failed") and r.failed():
            # Increase RAM for each op in the failed container (often a single op).
            for op in getattr(r, "ops", []) or []:
                k = _op_key(getattr(r, "pipeline_id", None), op) if hasattr(r, "pipeline_id") else None
                # If pipeline_id isn't available on the result, fall back to op identity only.
                if k is None or k[0] is None:
                    k = ("unknown", id(op))
                cur = float(s.ram_backoff.get(k, 1.0))
                nxt = cur * float(s.backoff_initial)
                s.ram_backoff[k] = min(nxt, float(s.backoff_max))

    # Early exit if no events (keeps simulator fast).
    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    # ---- Scheduling loop: fill each pool with multiple ops per tick ----
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)

        if avail_cpu < float(s.min_cpu_per_op) or avail_ram <= 0:
            continue

        ops_scheduled = 0
        # Keep trying to place work while we have meaningful headroom.
        while (
            ops_scheduled < int(s.max_ops_per_pool_per_tick)
            and avail_cpu >= float(s.min_cpu_per_op)
            and avail_ram > 0
        ):
            picked_pipeline = None
            picked_op = None
            picked_prio = None

            # Priority-first selection (latency focus).
            for prio in _prio_order():
                p, op = _try_pick_pipeline_with_ready_op(prio)
                if p is not None and op is not None:
                    picked_pipeline, picked_op, picked_prio = p, op, prio
                    break

            if picked_pipeline is None:
                break  # Nothing runnable right now.

            # Size resources conservatively.
            cpu_need = min(avail_cpu, _desired_cpu(pool, picked_prio))

            # Compute RAM need (conservative). If we can't satisfy, stop filling this pool.
            ram_need = _desired_ram_gb(pool, picked_pipeline.pipeline_id, picked_op)
            ram_need = min(avail_ram, ram_need)  # Don't request more than available now.

            # If remaining resources are too small for a meaningful attempt, stop.
            if cpu_need < float(s.min_cpu_per_op) or ram_need <= 0:
                break

            # If we had an estimate/backoff that exceeds remaining RAM, don't schedule this op here.
            # (Avoids immediate OOM risk and improves packing; next tick or another pool may fit.)
            if avail_ram < _desired_ram_gb(pool, picked_pipeline.pipeline_id, picked_op) * 0.95:
                # Try lower-priority work that might fit in smaller RAM (rare but can improve utilization).
                # However, never schedule lower priority if a higher priority is runnable.
                # Since we already picked the highest runnable, just stop for this pool.
                break

            assignment = Assignment(
                ops=[picked_op],
                cpu=cpu_need,
                ram=ram_need,
                priority=picked_prio,
                pool_id=pool_id,
                pipeline_id=picked_pipeline.pipeline_id,
            )
            assignments.append(assignment)

            # Update local accounting.
            avail_cpu -= float(cpu_need)
            avail_ram -= float(ram_need)
            ops_scheduled += 1

    return suspensions, assignments
