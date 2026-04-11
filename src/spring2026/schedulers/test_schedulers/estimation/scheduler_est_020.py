# policy_key: scheduler_est_020
# reasoning_effort: low
# exp: estimation
# model: gpt-5.2-2025-12-11
# llm_cost: 0.056327
# generation_seconds: 56.52
# generated_at: 2026-04-10T10:00:26.248823
@register_scheduler_init(key="scheduler_est_020")
def scheduler_est_020_init(s):
    """
    Memory-aware, priority-weighted round-robin scheduler.

    Goals aligned to weighted latency objective:
      - Strong preference for QUERY then INTERACTIVE then BATCH (protects score).
      - Reduce OOM-driven failures by using op.estimate.mem_peak_gb with a safety margin.
      - On failures, retry conservatively by inflating RAM hints for that operator (bounded retries).
      - Avoid starvation via round-robin within each priority queue.
    """
    # Separate FIFO queues per priority for predictable latency protection
    s.q_query = []
    s.q_interactive = []
    s.q_batch = []

    # Track per-operator RAM inflation factor after failures (esp. OOM)
    # key: (pipeline_id, op_key) -> factor
    s.op_ram_inflation = {}

    # Track failure counts to avoid infinite retry loops
    # key: (pipeline_id, op_key) -> count
    s.op_fail_counts = {}

    # Tunables
    s.max_retries_per_op = 3
    s.default_safety_margin = 1.30  # estimator is noisy; allocate a bit extra by default
    s.oom_safety_margin = 2.00      # after failure, be more conservative


@register_scheduler(key="scheduler_est_020")
def scheduler_est_020_scheduler(s, results, pipelines):
    """
    Schedule assignable operators with:
      1) strict priority across classes (QUERY > INTERACTIVE > BATCH),
      2) memory-fit filtering using noisy estimator hints + adaptive inflation on failures,
      3) small-op-first selection within a pool to pack RAM and keep headroom,
      4) round-robin fairness within each class.
    """
    # --- helpers (defined inside to avoid imports and keep policy self-contained) ---
    def _op_key(op):
        # Prefer stable IDs if present; fall back to Python object id.
        return getattr(op, "op_id", id(op))

    def _enqueue_pipeline(p):
        if p.priority == Priority.QUERY:
            s.q_query.append(p)
        elif p.priority == Priority.INTERACTIVE:
            s.q_interactive.append(p)
        else:
            s.q_batch.append(p)

    def _all_queues():
        return [s.q_query, s.q_interactive, s.q_batch]

    def _priority_queues_in_order():
        # Strict ordering to protect score
        return [s.q_query, s.q_interactive, s.q_batch]

    def _queue_scan_limit(q):
        # Bound per-tick scanning cost; still gives good choices under contention.
        # Slightly higher for high-priority so they find a fitting op sooner.
        if q is s.q_query:
            return 50
        if q is s.q_interactive:
            return 35
        return 25

    def _cpu_fraction_for_priority(priority):
        # Give more CPU to higher priority to reduce latency (dominates objective),
        # while still leaving room for concurrency.
        if priority == Priority.QUERY:
            return 0.60
        if priority == Priority.INTERACTIVE:
            return 0.45
        return 0.30

    def _min_ram_floor(pool):
        # Avoid absurdly small allocations that are likely to fail immediately.
        # Use 5% of pool RAM as a floor (still flexible across pool sizes).
        return max(0.1, 0.05 * pool.max_ram_pool)

    def _estimate_mem_gb(op):
        est = None
        if getattr(op, "estimate", None) is not None:
            est = getattr(op.estimate, "mem_peak_gb", None)
        # Guard against pathological values
        if est is None:
            return None
        try:
            est = float(est)
        except Exception:
            return None
        if est < 0:
            return None
        return est

    def _ram_needed_gb(pipeline_id, op, pool):
        est = _estimate_mem_gb(op)
        inflation = s.op_ram_inflation.get((pipeline_id, _op_key(op)), 1.0)
        safety = s.default_safety_margin
        # If we already inflated (likely after failure), add extra conservatism.
        if inflation > 1.0:
            safety = max(safety, s.oom_safety_margin)

        floor = _min_ram_floor(pool)

        if est is None:
            # No estimate: allocate a conservative chunk but not the entire pool.
            # This reduces risk of over-allocating to unknown ops and blocking others.
            return max(floor, 0.40 * pool.max_ram_pool)

        return max(floor, est * inflation * safety)

    def _cpu_needed(pool, priority, avail_cpu):
        # Allocate a fraction of the pool (cap by availability).
        frac = _cpu_fraction_for_priority(priority)
        target = max(1.0, pool.max_cpu_pool * frac)
        return min(avail_cpu, target)

    def _pipeline_done_or_hopeless(p):
        status = p.runtime_status()
        if status.is_pipeline_successful():
            return True

        # If there are failed ops, decide whether they are still retryable.
        failed_ops = status.get_ops([OperatorState.FAILED], require_parents_complete=False) or []
        for op in failed_ops:
            k = (p.pipeline_id, _op_key(op))
            if s.op_fail_counts.get(k, 0) <= s.max_retries_per_op:
                return False  # retryable failures exist
        # Either no retryable failed ops exist, or failures exceeded limits.
        return False

    def _should_drop_pipeline(p):
        # Drop only when all failed ops exceeded retry limits (prevents infinite loops).
        status = p.runtime_status()
        if status.is_pipeline_successful():
            return True
        failed_ops = status.get_ops([OperatorState.FAILED], require_parents_complete=False) or []
        if not failed_ops:
            return False
        for op in failed_ops:
            k = (p.pipeline_id, _op_key(op))
            if s.op_fail_counts.get(k, 0) <= s.max_retries_per_op:
                return False
        return True

    def _get_ready_op(p):
        status = p.runtime_status()
        # Assignable means PENDING or FAILED; require parents complete for correctness.
        ops = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True) or []
        if not ops:
            return None
        # Schedule one op at a time to reduce blast radius on bad estimates.
        return ops[0]

    def _pool_can_possibly_fit_op(op, pipeline_id):
        # Quick global feasibility check: if it can't fit in ANY pool's max RAM, don't keep retrying.
        est = _estimate_mem_gb(op)
        if est is None:
            return True  # unknown; could be small; allow
        inflation = s.op_ram_inflation.get((pipeline_id, _op_key(op)), 1.0)
        need = est * inflation * s.default_safety_margin
        max_pool_ram = 0.0
        for i in range(s.executor.num_pools):
            max_pool_ram = max(max_pool_ram, s.executor.pools[i].max_ram_pool)
        return need <= max_pool_ram

    def _pick_candidate_for_pool(pool):
        """
        Choose the best (pipeline, op) candidate for this pool:
          - highest priority queue first
          - among scanned pipelines, choose the smallest estimated RAM that fits current availability
        Returns: (queue_ref, index_in_queue, pipeline, op, ram_need)
        """
        best = None  # tuple: (priority_rank, ram_need, queue_ref, idx, p, op)
        prio_rank = 0
        for q in _priority_queues_in_order():
            scan = min(len(q), _queue_scan_limit(q))
            for idx in range(scan):
                p = q[idx]

                if _should_drop_pipeline(p):
                    continue

                op = _get_ready_op(p)
                if op is None:
                    continue

                # If estimate indicates it can't fit in any pool, skip for now (prevents churn).
                if not _pool_can_possibly_fit_op(op, p.pipeline_id):
                    continue

                ram_need = _ram_needed_gb(p.pipeline_id, op, pool)

                # Fit check against *current* availability in this pool
                if ram_need > pool.avail_ram_pool or pool.avail_cpu_pool <= 0:
                    continue

                # Prefer smaller RAM to pack effectively and preserve headroom for big ops.
                cand = (prio_rank, ram_need, q, idx, p, op)
                if best is None:
                    best = cand
                else:
                    # Strictly prefer higher priority; within same priority prefer smaller RAM.
                    if cand[0] < best[0] or (cand[0] == best[0] and cand[1] < best[1]):
                        best = cand
            prio_rank += 1
        return best

    # --- ingest new pipelines ---
    for p in pipelines:
        _enqueue_pipeline(p)

    # Early exit if nothing changed
    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    # --- incorporate execution feedback ---
    # Use failures to adapt RAM inflation, improving completion rate (reduces 720s penalties).
    for r in results:
        if not r.failed():
            continue

        # Identify failed ops (could be one or many; typically one)
        for op in (r.ops or []):
            k = (r.pipeline_id if hasattr(r, "pipeline_id") else None, _op_key(op))
            # If pipeline_id isn't in result, fall back to best-effort op-only key.
            if k[0] is None:
                k = ("unknown", k[1])

            s.op_fail_counts[k] = s.op_fail_counts.get(k, 0) + 1

            # Inflate RAM after failure. If error hints OOM, inflate more aggressively.
            err = getattr(r, "error", None)
            err_s = str(err).lower() if err is not None else ""
            is_oom = ("oom" in err_s) or ("out of memory" in err_s) or ("cuda" in err_s and "memory" in err_s)

            # Bump factor multiplicatively; cap to avoid runaway allocations.
            bump = 2.0 if is_oom else 1.5
            prev = s.op_ram_inflation.get(k, 1.0)
            s.op_ram_inflation[k] = min(8.0, prev * bump)

    # --- clean queues of completed / hopeless pipelines (without dropping retryable failures) ---
    for q in _all_queues():
        if not q:
            continue
        new_q = []
        for p in q:
            status = p.runtime_status()
            if status.is_pipeline_successful():
                continue
            # If all failed ops exceeded retry limits, stop scheduling this pipeline.
            if _should_drop_pipeline(p):
                continue
            new_q.append(p)
        q[:] = new_q

    # --- scheduling loop: fill each pool with as many single-op assignments as fit ---
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]

        # Greedy fill: keep assigning while there is room for at least one op.
        while pool.avail_cpu_pool > 0 and pool.avail_ram_pool > 0:
            picked = _pick_candidate_for_pool(pool)
            if picked is None:
                break

            prio_rank, ram_need, qref, idx, p, op = picked

            # Compute CPU and RAM assignment (bounded by availability)
            cpu = _cpu_needed(pool, p.priority, pool.avail_cpu_pool)
            if cpu <= 0:
                break

            # Cap RAM to availability (should already fit) but keep nonzero
            ram = min(pool.avail_ram_pool, max(0.1, ram_need))
            if ram <= 0 or ram > pool.avail_ram_pool:
                break

            assignments.append(
                Assignment(
                    ops=[op],
                    cpu=cpu,
                    ram=ram,
                    priority=p.priority,
                    pool_id=pool_id,
                    pipeline_id=p.pipeline_id,
                )
            )

            # Round-robin within the same queue: move scheduled pipeline to the end
            # to avoid monopolization among same-priority pipelines.
            qref.pop(idx)
            qref.append(p)

            # The simulator will apply the assignment and update pool availability later;
            # however, we also decrement locally to avoid over-committing within this tick.
            pool.avail_cpu_pool -= cpu
            pool.avail_ram_pool -= ram

            # Stop if remaining resources are too small to be useful.
            if pool.avail_cpu_pool <= 0 or pool.avail_ram_pool <= 0:
                break

    return suspensions, assignments
