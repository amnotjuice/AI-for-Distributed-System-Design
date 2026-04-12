# policy_key: scheduler_iter_median_rich_014
# context: rich
# model: gpt-5.2-2025-12-11
# llm_cost: 0.067442
# generation_seconds: 44.43
# generated_at: 2026-04-12T01:54:32.188787
@register_scheduler_init(key="scheduler_iter_median_rich_014")
def scheduler_iter_median_rich_014_init(s):
    """Iteration 2: Priority-first, per-operator RAM learning to cut OOM retries + latency.

    Changes vs prior attempt:
    - Keep strict priority queues (QUERY > INTERACTIVE > BATCH) but avoid global RAM multiplier jumps.
    - Learn RAM needs per *operator signature* from ExecutionResult:
        * On OOM: bump that operator's RAM hint aggressively (alloc * 1.8).
        * On success: tighten upper bound slightly (hint <= alloc * 1.2).
    - Start with smaller RAM slices by default to reduce wasted pool RAM (mean allocated was very high),
      but converge quickly for "big" ops via the OOM hinting.
    - Place high priority on the pool with most available headroom (simple multi-pool awareness).
    - Keep assignments to 1 op at a time for responsiveness; allow multiple assignments per pool per tick.
    """
    # Priority queues: round-robin within each
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

    # Per-operator RAM hint (in "RAM units" used by simulator); learned from results.
    # key -> {"ram_hint": float}
    s.op_ram = {}

    # Per-operator CPU hint multiplier (very light-touch; mostly avoid timeouts)
    # key -> {"cpu_mult": float}
    s.op_cpu = {}

    # Logical time to support mild aging if needed later
    s.tick = 0


@register_scheduler(key="scheduler_iter_median_rich_014")
def scheduler_iter_median_rich_014_scheduler(s, results, pipelines):
    """
    Step:
    1) Enqueue new pipelines by priority.
    2) Update per-op RAM/CPU hints from results (target OOM/timeouts without global overreaction).
    3) For each pool, schedule ready ops in priority order, using learned hints and small defaults.
       High-priority placement prefers pools with most headroom.
    """
    s.tick += 1

    # -----------------------------
    # Helpers
    # -----------------------------
    def _is_oom_error(err):
        if err is None:
            return False
        msg = str(err).lower()
        return ("oom" in msg) or ("out of memory" in msg)

    def _is_timeout_error(err):
        if err is None:
            return False
        msg = str(err).lower()
        return "timeout" in msg

    def _op_signature(op):
        # Try stable identifiers if present; fall back to repr (best-effort).
        for attr in ("op_id", "operator_id", "id", "name"):
            if hasattr(op, attr):
                try:
                    v = getattr(op, attr)
                    # guard against callables
                    if callable(v):
                        continue
                    return (attr, str(v))
                except Exception:
                    pass
        try:
            return ("repr", repr(op))
        except Exception:
            return ("pyid", str(id(op)))

    def _get_op_hint(sig):
        if sig not in s.op_ram:
            s.op_ram[sig] = {"ram_hint": None}
        if sig not in s.op_cpu:
            s.op_cpu[sig] = {"cpu_mult": 1.0}
        return s.op_ram[sig], s.op_cpu[sig]

    def _priority_order():
        return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]

    def _cleanup_queue(pri):
        """Remove completed or definitively failed pipelines from the queue."""
        q = s.queues[pri]
        kept = []
        for p in q:
            st = p.runtime_status()
            if st.is_pipeline_successful():
                continue
            # Keep FAILED pipelines: in this simulator FAILED is assignable for retry; we rely on RAM learning.
            kept.append(p)
        s.queues[pri] = kept
        s.rr_cursor[pri] = 0 if not kept else (s.rr_cursor[pri] % len(kept))

    def _pop_rr(pri):
        """Round-robin pop: returns a pipeline or None (does not drop it permanently)."""
        q = s.queues[pri]
        if not q:
            return None
        idx = s.rr_cursor[pri] % len(q)
        s.rr_cursor[pri] = (idx + 1) % len(q)
        return q.pop(idx)

    def _push_back(pri, p):
        s.queues[pri].append(p)

    def _pick_pool_for_priority(pri):
        """Choose a pool for this priority based on available headroom."""
        # High priority: maximize available RAM first (OOM avoidance) then CPU.
        # Batch: maximize CPU first (throughput) while still considering RAM.
        best = None
        best_score = None
        for pool_id in range(s.executor.num_pools):
            pool = s.executor.pools[pool_id]
            a_cpu = pool.avail_cpu_pool
            a_ram = pool.avail_ram_pool
            if a_cpu <= 0 or a_ram <= 0:
                continue
            if pri in (Priority.QUERY, Priority.INTERACTIVE):
                score = (a_ram, a_cpu)
            else:
                score = (a_cpu, a_ram)
            if best is None or score > best_score:
                best = pool_id
                best_score = score
        return best

    def _default_ram_fraction(pri):
        # Smaller defaults than prior attempt to reduce wasted allocations; RAM hints will correct upward.
        if pri == Priority.QUERY:
            return 0.05
        if pri == Priority.INTERACTIVE:
            return 0.08
        return 0.12  # batch

    def _default_cpu_fraction(pri):
        # Keep query/interactive snappy; batch larger when possible.
        if pri == Priority.QUERY:
            return 0.20
        if pri == Priority.INTERACTIVE:
            return 0.30
        return 0.55

    def _compute_request(pool, pri, op_list, avail_cpu, avail_ram):
        """Compute cpu/ram for a single-op assignment using per-op hints."""
        # CPU
        base_cpu = max(1.0, pool.max_cpu_pool * _default_cpu_fraction(pri))
        # RAM default
        base_ram = max(1.0, pool.max_ram_pool * _default_ram_fraction(pri))

        # Incorporate per-op hints (use max across ops in this assignment; we assign only 1 op)
        req_ram = base_ram
        cpu_mult = 1.0
        for op in op_list:
            sig = _op_signature(op)
            ram_state, cpu_state = _get_op_hint(sig)
            if ram_state["ram_hint"] is not None:
                # small safety margin
                req_ram = max(req_ram, ram_state["ram_hint"] * 1.10)
            cpu_mult = max(cpu_mult, cpu_state["cpu_mult"])

        req_cpu = base_cpu * cpu_mult

        # Clamp by availability and pool maxima
        req_cpu = max(1.0, min(req_cpu, avail_cpu, pool.max_cpu_pool))
        req_ram = max(1.0, min(req_ram, avail_ram, pool.max_ram_pool))

        return req_cpu, req_ram

    # -----------------------------
    # 1) Enqueue new pipelines
    # -----------------------------
    for p in pipelines:
        s.queues[p.priority].append(p)

    # If nothing changed, no need to make decisions
    if not pipelines and not results:
        return [], []

    # Opportunistic cleanup (keeps queues small)
    for pri in _priority_order():
        _cleanup_queue(pri)

    # -----------------------------
    # 2) Learn from results (per-operator hints)
    # -----------------------------
    for r in results:
        # Update hints based on each op in the result.
        try:
            ops = list(getattr(r, "ops", []) or [])
        except Exception:
            ops = []

        if hasattr(r, "failed") and r.failed():
            # OOM: bump RAM hint aggressively so next retry succeeds without global over-allocation.
            if _is_oom_error(getattr(r, "error", None)):
                for op in ops:
                    sig = _op_signature(op)
                    ram_state, _ = _get_op_hint(sig)
                    alloc = float(getattr(r, "ram", 0) or 0)
                    if alloc > 0:
                        new_hint = alloc * 1.8
                        if ram_state["ram_hint"] is None:
                            ram_state["ram_hint"] = new_hint
                        else:
                            ram_state["ram_hint"] = max(ram_state["ram_hint"], new_hint)
            # Timeout: modestly increase CPU multiplier for these ops.
            if _is_timeout_error(getattr(r, "error", None)):
                for op in ops:
                    sig = _op_signature(op)
                    _, cpu_state = _get_op_hint(sig)
                    cpu_state["cpu_mult"] = min(cpu_state["cpu_mult"] * 1.25, 4.0)
        else:
            # Success: cap the hint to avoid it drifting upward forever; keep as a conservative upper bound.
            alloc = float(getattr(r, "ram", 0) or 0)
            if alloc > 0:
                for op in ops:
                    sig = _op_signature(op)
                    ram_state, _ = _get_op_hint(sig)
                    # If we already have a hint, tighten it toward what worked, but don't go below a floor.
                    if ram_state["ram_hint"] is None:
                        ram_state["ram_hint"] = alloc * 1.15
                    else:
                        ram_state["ram_hint"] = min(ram_state["ram_hint"], alloc * 1.20)

    # -----------------------------
    # 3) Schedule (no suspensions in this iteration)
    # -----------------------------
    suspensions = []
    assignments = []

    # We'll schedule in multiple passes: always try QUERY+INTERACTIVE first with best pool choice,
    # then fill remaining capacity with batch. This reduces contention-induced tail latency.
    def _try_schedule_one(pri):
        pool_id = _pick_pool_for_priority(pri)
        if pool_id is None:
            return False

        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool
        if avail_cpu <= 0 or avail_ram <= 0:
            return False

        # Pull a pipeline RR; if not runnable, push back and try a few more.
        tried = 0
        qlen = len(s.queues[pri])
        if qlen == 0:
            return False

        while tried < min(qlen, 8):  # bounded scan to avoid O(n) each tick
            tried += 1
            p = _pop_rr(pri)
            if p is None:
                return False

            st = p.runtime_status()
            if st.is_pipeline_successful():
                # drop
                continue

            op_list = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
            if not op_list:
                _push_back(pri, p)
                continue

            cpu_req, ram_req = _compute_request(pool, pri, op_list, avail_cpu, avail_ram)

            # If even minimal doesn't fit, put back and stop for this priority.
            if cpu_req <= 0 or ram_req <= 0:
                _push_back(pri, p)
                return False

            assignments.append(
                Assignment(
                    ops=op_list,
                    cpu=cpu_req,
                    ram=ram_req,
                    priority=p.priority,
                    pool_id=pool_id,
                    pipeline_id=p.pipeline_id,
                )
            )

            # Push back pipeline for future ops
            _push_back(pri, p)
            return True

        return False

    # Keep scheduling until no progress. This allows multiple placements per tick across pools.
    made_progress = True
    while made_progress:
        made_progress = False

        # Drain high-priority first
        for pri in (Priority.QUERY, Priority.INTERACTIVE):
            # Schedule at most a few per loop to avoid starving batch entirely
            for _ in range(4 if pri == Priority.QUERY else 3):
                if _try_schedule_one(pri):
                    made_progress = True
                else:
                    break

        # Then allow batch to fill slack
        for _ in range(6):
            if _try_schedule_one(Priority.BATCH_PIPELINE):
                made_progress = True
            else:
                break

        # Stop if nothing got scheduled this round
        if not made_progress:
            break

    return suspensions, assignments
