# policy_key: scheduler_iter_median_rich_004
# context: rich
# model: gpt-5.2-2025-12-11
# llm_cost: 0.060050
# generation_seconds: 51.97
# generated_at: 2026-04-12T01:46:09.506878
@register_scheduler_init(key="scheduler_iter_median_rich_004")
def scheduler_iter_median_rich_004_init(s):
    """Priority-aware scheduler with OOM-driven per-operator RAM learning.

    Iteration goals vs prior version:
    - Reduce latency by cutting OOM churn (huge in stats) via per-operator RAM hints.
    - Avoid over-allocating RAM broadly (allocated ~94% vs consumed ~30%) by sizing
      close to learned minima rather than big pool fractions.
    - Keep strict priority ordering (QUERY > INTERACTIVE > BATCH) with light fairness
      within each class (round-robin).
    - Use multi-pool placement: prefer "best headroom" pool for high priority; steer
      batch away when high-priority backlog exists.

    Notes:
    - ExecutionResult does not expose pipeline_id in the provided interface; we learn
      RAM hints keyed by operator identity from ExecutionResult.ops.
    """
    s.queues = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }
    s.rr_cursor = {
        Priority.QUERY: 0,
        Priority.INTERACTIVE: 0,
        Priority.BATCH_PIPELINE: 0,
    }

    # Learned per-operator resource hints (best-effort):
    # op_key -> {"ram_min": float, "cpu_boost": float}
    s.op_hints = {}

    # Track recent OOM pressure by priority to apply gentle global nudges for unknown ops.
    s.oom_pressure = {
        Priority.QUERY: 0.0,
        Priority.INTERACTIVE: 0.0,
        Priority.BATCH_PIPELINE: 0.0,
    }


@register_scheduler(key="scheduler_iter_median_rich_004")
def scheduler_iter_median_rich_004_scheduler(s, results, pipelines):
    """
    Step:
    1) Enqueue new pipelines.
    2) Update per-operator RAM minima on OOM (double-last-ram heuristic).
    3) For each pool, repeatedly assign 1 ready op at a time, strict priority order.
       - High priority picks the pool with most headroom (reduces queueing delay).
       - Batch is softly capped when high-priority backlog exists.
       - RAM request = max(base, learned_min), clamped to avoid reserving huge fractions.
    """
    # ----------------------------
    # Helpers
    # ----------------------------
    def _priority_order():
        return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]

    def _is_oom_error(err):
        if err is None:
            return False
        msg = str(err).lower()
        return ("oom" in msg) or ("out of memory" in msg)

    def _op_key(op):
        # Prefer stable identifiers if present; fall back to repr.
        # (We avoid Python's id(op) because it isn't stable across serialization.)
        k = getattr(op, "op_id", None)
        if k is not None:
            return f"op_id:{k}"
        k = getattr(op, "name", None)
        if k is not None:
            return f"name:{k}"
        return f"repr:{repr(op)}"

    def _hint_for_op(op):
        k = _op_key(op)
        if k not in s.op_hints:
            s.op_hints[k] = {"ram_min": 0.0, "cpu_boost": 1.0}
        return s.op_hints[k]

    def _has_assignable_op(p):
        st = p.runtime_status()
        if st.is_pipeline_successful():
            return False
        ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        return bool(ops)

    def _queue_has_backlog(pri):
        for p in s.queues[pri]:
            if _has_assignable_op(p):
                return True
        return False

    def _pop_rr(pri):
        """Round-robin: pick next pipeline with an assignable op; drop completed ones."""
        q = s.queues[pri]
        if not q:
            return None

        n = len(q)
        start = s.rr_cursor[pri] % n
        for step in range(n):
            idx = (start + step) % n
            p = q[idx]
            st = p.runtime_status()
            if st.is_pipeline_successful():
                q.pop(idx)
                if not q:
                    s.rr_cursor[pri] = 0
                    return None
                # restart search because indices shifted
                n = len(q)
                start = s.rr_cursor[pri] % n
                continue

            ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
            if not ops:
                continue

            # Remove from queue for now; caller can append back.
            chosen = q.pop(idx)
            if q:
                s.rr_cursor[pri] = idx % len(q)
            else:
                s.rr_cursor[pri] = 0
            return chosen
        return None

    def _base_cpu(pri, pool):
        # Keep CPU modest to fit more concurrent short tasks; increase for batch slightly.
        if pri == Priority.QUERY:
            return min(2.0, max(1.0, pool.max_cpu_pool * 0.20))
        if pri == Priority.INTERACTIVE:
            return min(3.0, max(1.0, pool.max_cpu_pool * 0.30))
        return min(6.0, max(2.0, pool.max_cpu_pool * 0.45))

    def _base_ram(pri, pool):
        # More conservative than "fraction of pool": start smaller to avoid huge reserved RAM.
        # OOM learning should quickly correct underestimates for heavy ops.
        if pri == Priority.QUERY:
            return max(1.0, pool.max_ram_pool * 0.06)
        if pri == Priority.INTERACTIVE:
            return max(1.0, pool.max_ram_pool * 0.10)
        return max(1.0, pool.max_ram_pool * 0.14)

    def _ram_cap(pri, pool, hp_backlog):
        # Cap per-op RAM to avoid reserving an entire pool for a single op by default.
        # If a specific op learns a higher min, it can exceed this cap up to pool max.
        if pri == Priority.QUERY:
            return pool.max_ram_pool * 0.35
        if pri == Priority.INTERACTIVE:
            return pool.max_ram_pool * 0.55
        # Batch: be stricter when HP backlog exists to preserve headroom.
        return pool.max_ram_pool * (0.50 if hp_backlog else 0.70)

    def _choose_pool_for_priority(pri, hp_backlog):
        # Heuristic placement:
        # - QUERY/INTERACTIVE: choose pool with highest (avail_ram, avail_cpu) lexicographically.
        # - BATCH: if HP backlog exists and multiple pools, avoid the best headroom pool to reduce interference.
        candidates = list(range(s.executor.num_pools))
        if not candidates:
            return None

        # Score pools by headroom.
        scored = []
        for pid in candidates:
            pool = s.executor.pools[pid]
            scored.append((pool.avail_ram_pool, pool.avail_cpu_pool, pid))
        scored.sort(reverse=True)

        if pri in (Priority.QUERY, Priority.INTERACTIVE):
            return scored[0][2]

        # Batch
        if hp_backlog and len(scored) > 1:
            return scored[1][2]
        return scored[0][2]

    def _try_assign_one(pool_id, pri, hp_backlog):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool
        if avail_cpu <= 0 or avail_ram <= 0:
            return None

        p = _pop_rr(pri)
        if p is None:
            return None

        st = p.runtime_status()
        ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
        if not ops:
            # Not runnable right now; put back.
            s.queues[pri].append(p)
            return None

        op = ops[0]
        hint = _hint_for_op(op)

        cpu_req = _base_cpu(pri, pool) * float(hint.get("cpu_boost", 1.0))
        cpu_req = max(1.0, min(cpu_req, avail_cpu, pool.max_cpu_pool))

        # RAM request: base, plus gentle global nudge if this priority had OOMs recently.
        # Then apply learned per-op minimum (dominant).
        pressure = s.oom_pressure.get(pri, 0.0)
        base = _base_ram(pri, pool) * (1.0 + min(0.75, pressure))
        learned = float(hint.get("ram_min", 0.0))
        cap = _ram_cap(pri, pool, hp_backlog)

        ram_req = max(base, learned)
        # Cap "unknown" bloat, but never cap below learned minima.
        ram_req = min(max(learned, min(ram_req, cap)), pool.max_ram_pool)
        ram_req = max(1.0, min(ram_req, avail_ram))

        # If we can't fit learned minimum, don't thrash: requeue and fail this attempt.
        if learned > 0.0 and avail_ram < min(learned, pool.max_ram_pool):
            s.queues[pri].append(p)
            return None

        # If even minimal doesn't fit, requeue.
        if cpu_req <= 0 or ram_req <= 0:
            s.queues[pri].append(p)
            return None

        # Soft admission: when HP backlog exists, avoid consuming the last memory with batch.
        if pri == Priority.BATCH_PIPELINE and hp_backlog:
            # Keep at least 15% of pool RAM free for HP arrivals.
            if (pool.avail_ram_pool - ram_req) < (pool.max_ram_pool * 0.15):
                s.queues[pri].append(p)
                return None

        assignment = Assignment(
            ops=ops,
            cpu=cpu_req,
            ram=ram_req,
            priority=p.priority,
            pool_id=pool_id,
            pipeline_id=p.pipeline_id,
        )

        # Put pipeline back for future ops (round-robin fairness).
        s.queues[pri].append(p)
        return assignment

    # ----------------------------
    # 1) Enqueue new pipelines
    # ----------------------------
    for p in pipelines:
        s.queues[p.priority].append(p)

    if not pipelines and not results:
        return [], []

    # ----------------------------
    # 2) Learn from results (OOM-driven RAM minima)
    # ----------------------------
    for r in results:
        if hasattr(r, "failed") and r.failed():
            if _is_oom_error(getattr(r, "error", None)):
                # Increase OOM pressure for that priority (decays later).
                s.oom_pressure[r.priority] = min(3.0, s.oom_pressure.get(r.priority, 0.0) + 0.35)

                # Update RAM minima for each op in the failed container.
                # Heuristic: if it OOM'd at ram=r.ram, next try needs ~2x.
                next_min = min(
                    float(getattr(s.executor.pools[r.pool_id], "max_ram_pool", r.ram)),
                    max(float(r.ram) * 2.0, float(r.ram) + 1.0),
                )
                for op in getattr(r, "ops", []) or []:
                    hint = _hint_for_op(op)
                    hint["ram_min"] = max(float(hint.get("ram_min", 0.0)), float(next_min))
            else:
                # Slightly increase OOM pressure anyway to be conservative when failures happen.
                s.oom_pressure[r.priority] = min(3.0, s.oom_pressure.get(r.priority, 0.0) + 0.05)
        else:
            # Success: decay OOM pressure for that priority to avoid permanent inflation.
            s.oom_pressure[r.priority] = max(0.0, s.oom_pressure.get(r.priority, 0.0) * 0.92)

    # If no results for some priorities, still slowly decay.
    if results:
        seen = {r.priority for r in results}
        for pri in _priority_order():
            if pri not in seen:
                s.oom_pressure[pri] = max(0.0, s.oom_pressure.get(pri, 0.0) * 0.98)
    else:
        for pri in _priority_order():
            s.oom_pressure[pri] = max(0.0, s.oom_pressure.get(pri, 0.0) * 0.98)

    # ----------------------------
    # 3) Build assignments
    # ----------------------------
    suspensions = []
    assignments = []

    hp_backlog = _queue_has_backlog(Priority.QUERY) or _queue_has_backlog(Priority.INTERACTIVE)

    # We schedule in small increments (1 op per decision) to reduce head-of-line blocking.
    # Loop until no pool can make progress this tick.
    made_progress = True
    while made_progress:
        made_progress = False

        # Strict global priority: always attempt to place higher priority first.
        for pri in _priority_order():
            pool_id = _choose_pool_for_priority(pri, hp_backlog)
            if pool_id is None:
                continue

            a = _try_assign_one(pool_id, pri, hp_backlog)
            if a is None:
                continue

            assignments.append(a)
            made_progress = True

            # After placing one, re-check HP backlog signal (it can change as queues drain).
            hp_backlog = _queue_has_backlog(Priority.QUERY) or _queue_has_backlog(Priority.INTERACTIVE)

            # Continue trying to place more work at same priority before dropping to lower.
            # (This improves tail latency for HP classes.)
            break

    return suspensions, assignments
