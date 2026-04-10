# policy_key: scheduler_est_014
# reasoning_effort: low
# model: gpt-5.2-2025-12-11
# llm_cost: 0.045261
# generation_seconds: 34.92
# generated_at: 2026-04-03T01:13:43.838295
@register_scheduler_init(key="scheduler_est_014")
def scheduler_est_014_init(s):
    """Priority-aware FIFO with simple right-sizing and OOM-driven RAM backoff.

    Incremental improvements over naive FIFO:
      1) Keep separate waiting queues by priority; always prefer higher priority.
      2) Avoid "give all resources to one op": cap per-op CPU/RAM by priority shares
         to reduce head-of-line blocking and improve tail latency for interactive work.
      3) If an op fails (e.g., OOM), retry later with increased RAM via a per-op
         multiplicative backoff factor.
    """
    from collections import deque

    # Per-priority waiting queues (pipelines).
    s.q_query = deque()
    s.q_interactive = deque()
    s.q_batch = deque()

    # Per-op RAM boost factor increased after failures (e.g., OOM).
    s.op_ram_boost = {}  # uid -> float multiplier

    # Round-robin cursor among queues (for minimal fairness within same priority decision).
    s.rr_cursor = 0


@register_scheduler(key="scheduler_est_014")
def scheduler_est_014_scheduler(s, results, pipelines):
    """
    Scheduler step:
      - Enqueue new pipelines into per-priority queues.
      - Observe failures and increase RAM boost for involved ops.
      - For each pool, greedily place ready ops preferring higher priorities.
    """
    from collections import deque

    def _get_op_uid(op):
        # Best-effort stable identifier across retries/simulation.
        for attr in ("operator_id", "op_id", "node_id", "id", "name"):
            if hasattr(op, attr):
                v = getattr(op, attr)
                if v is not None:
                    return f"{attr}:{v}"
        # Fallback to repr (may be noisy but acceptable for a backoff hint).
        return f"repr:{repr(op)}"

    def _queues_in_priority_order():
        # Higher priority first (QUERY > INTERACTIVE > BATCH_PIPELINE).
        return [s.q_query, s.q_interactive, s.q_batch]

    def _enqueue_pipeline(p):
        pr = p.priority
        if pr == Priority.QUERY:
            s.q_query.append(p)
        elif pr == Priority.INTERACTIVE:
            s.q_interactive.append(p)
        else:
            s.q_batch.append(p)

    def _pick_next_pipeline_with_ready_op(q, max_scan):
        """Pop/rotate pipelines until one with a ready op is found (or exhausted)."""
        scanned = 0
        while q and scanned < max_scan:
            p = q.popleft()
            status = p.runtime_status()

            # Drop completed pipelines.
            if status.is_pipeline_successful():
                scanned += 1
                continue

            # Identify next assignable op whose parents are complete.
            op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
            if op_list:
                return p, op_list[0]

            # Not ready yet (blocked on parents/running ops); rotate to the back.
            q.append(p)
            scanned += 1

        return None, None

    def _cpu_cap_for_priority(pool, priority):
        # Caps are expressed as fraction of pool max, to prevent one op from monopolizing.
        # QUERY gets the most to reduce latency; BATCH gets less to improve packing.
        if priority == Priority.QUERY:
            frac = 0.75
        elif priority == Priority.INTERACTIVE:
            frac = 0.60
        else:
            frac = 0.35
        cap = float(pool.max_cpu_pool) * frac
        # Keep at least a small slice if available.
        return max(0.0, cap)

    def _ram_cap_for_priority(pool, priority):
        # Let high priority consume more RAM if needed, but still avoid full monopolization.
        if priority == Priority.QUERY:
            frac = 0.80
        elif priority == Priority.INTERACTIVE:
            frac = 0.70
        else:
            frac = 0.50
        cap = float(pool.max_ram_pool) * frac
        return max(0.0, cap)

    def _estimate_mem_gb(op):
        est = getattr(op, "estimate", None)
        if est is None:
            return None
        v = getattr(est, "mem_peak_gb", None)
        try:
            if v is None:
                return None
            v = float(v)
            if v <= 0:
                return None
            return v
        except Exception:
            return None

    # Enqueue new pipelines.
    for p in pipelines:
        _enqueue_pipeline(p)

    # Update RAM backoff based on failures.
    for r in results:
        try:
            if r.failed():
                # Increase RAM multiplier for the involved op(s).
                # We assume r.ops is a list of operator objects.
                for op in (r.ops or []):
                    uid = _get_op_uid(op)
                    cur = float(s.op_ram_boost.get(uid, 1.0))
                    # Exponential backoff with a modest step to converge quickly after OOM.
                    s.op_ram_boost[uid] = min(8.0, cur * 1.5)
        except Exception:
            # If result shape differs, fail safe: ignore.
            pass

    # Early exit if nothing changed.
    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    # Schedule per pool.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)

        # Greedy packing: assign multiple ops as long as resources remain.
        # Stop if we can't allocate a meaningful slice.
        while avail_cpu > 0.01 and avail_ram > 0.01:
            # Choose queue by strict priority order; within each, rotate a bit to avoid stalling.
            picked_pipeline = None
            picked_op = None

            for q in _queues_in_priority_order():
                if not q:
                    continue
                # Scan at most the current queue length to find a ready pipeline.
                p, op = _pick_next_pipeline_with_ready_op(q, max_scan=len(q))
                if p is not None:
                    picked_pipeline, picked_op = p, op
                    break

            if picked_pipeline is None:
                break  # nothing ready anywhere

            pr = picked_pipeline.priority

            # Determine CPU allocation (cap by priority + remaining available).
            cpu_cap = _cpu_cap_for_priority(pool, pr)
            cpu = min(avail_cpu, cpu_cap if cpu_cap > 0 else avail_cpu)

            # Prefer at least a small CPU slice; if none, put pipeline back and stop.
            if cpu <= 0.01:
                _enqueue_pipeline(picked_pipeline)
                break

            # Determine RAM allocation:
            # - Use estimator if available, applying per-op boost after failures.
            # - Clamp by priority cap and remaining RAM.
            uid = _get_op_uid(picked_op)
            boost = float(s.op_ram_boost.get(uid, 1.0))
            est_mem = _estimate_mem_gb(picked_op)

            ram_cap = _ram_cap_for_priority(pool, pr)
            max_ram_for_this = min(avail_ram, ram_cap if ram_cap > 0 else avail_ram)

            if est_mem is None:
                # No estimate: be conservative but avoid taking the whole pool.
                ram = max_ram_for_this
            else:
                # Allocate close to estimate with a small headroom; rely on retry + backoff on OOM.
                ram = (est_mem * boost) + 0.2
                ram = min(ram, max_ram_for_this)
                # Ensure we allocate something sensible if estimate is tiny.
                ram = max(0.1, ram)

            # If we still can't fit, requeue and stop trying in this pool.
            if ram > avail_ram + 1e-9 or ram <= 0.01:
                _enqueue_pipeline(picked_pipeline)
                break

            # Create assignment for single ready op.
            assignments.append(
                Assignment(
                    ops=[picked_op],
                    cpu=cpu,
                    ram=ram,
                    priority=pr,
                    pool_id=pool_id,
                    pipeline_id=picked_pipeline.pipeline_id,
                )
            )

            # Update local remaining resources and requeue the pipeline for later ops.
            avail_cpu -= cpu
            avail_ram -= ram
            _enqueue_pipeline(picked_pipeline)

    return suspensions, assignments
