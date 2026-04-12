# policy_key: scheduler_iter_median_rich_013
# context: rich
# model: gpt-5.2-2025-12-11
# llm_cost: 0.050404
# generation_seconds: 62.18
# generated_at: 2026-04-12T01:53:47.755155
@register_scheduler_init(key="scheduler_iter_median_rich_013")
def scheduler_iter_median_rich_013_init(s):
    """Priority-aware scheduler with (1) dedicated high-priority pool preference,
    (2) per-operator RAM/CPU hinting from past outcomes (OOM/timeout/success),
    (3) smaller default RAM to reduce memory hoarding + faster OOM backoff,
    (4) strict priority with round-robin within each class.

    Design goal: reduce latency by (a) preventing batch from crowding out QUERY/INTERACTIVE,
    and (b) reducing OOM churn while avoiding allocating excessive RAM that lowers concurrency.
    """
    # Per-priority FIFO queues (RR via cursor index).
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

    # Operator-level resource hints keyed by a best-effort operator fingerprint.
    # Hints are "requested allocation that tends to work", not necessarily true usage.
    s.op_ram_hint = {}   # key -> float (GB or simulator units)
    s.op_cpu_hint = {}   # key -> float

    # Track recent failures to bias scheduling away from repeated small allocations.
    s.op_oom_streak = {}      # key -> int
    s.op_timeout_streak = {}  # key -> int


@register_scheduler(key="scheduler_iter_median_rich_013")
def scheduler_iter_median_rich_013_scheduler(s, results, pipelines):
    """One tick of scheduling.

    Key behaviors:
    - Pool preference:
        * If multiple pools: pool 0 is "HP-favored" (QUERY/INTERACTIVE first).
        * Other pools are batch-favored but may run INTERACTIVE/QUERY if batch empty.
    - Per-op RAM hinting:
        * Default RAM request is modest to avoid memory hoarding.
        * On OOM: increase RAM hint aggressively for that op fingerprint.
        * On success: gently tighten/confirm hints.
    - Per-op CPU hinting:
        * On timeout: increase CPU hint (mildly).
        * Otherwise keep CPU small for latency-sensitive work to reduce contention.
    """
    # ----------------------------
    # Helpers
    # ----------------------------
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

    def _op_fingerprint(op):
        # Try common fields first, fall back to repr/str.
        for attr in ("op_id", "operator_id", "id", "name"):
            if hasattr(op, attr):
                try:
                    v = getattr(op, attr)
                    if v is not None:
                        return f"{attr}:{v}"
                except Exception:
                    pass
        # Fall back to stable-ish representation.
        try:
            return f"repr:{repr(op)}"
        except Exception:
            return f"str:{str(op)}"

    def _enqueue_pipeline(p):
        s.queues[p.priority].append(p)

    def _drop_done_or_hard_failed(q, idx):
        # Remove pipeline at idx from queue.
        q.pop(idx)

    def _next_pipeline_rr(pri):
        """Pick next pipeline (RR) that still has potential runnable work; drop completed ones."""
        q = s.queues[pri]
        if not q:
            return None

        n = len(q)
        start = s.rr_cursor[pri] % max(1, n)

        tried = 0
        while tried < n and q:
            idx = (start + tried) % len(q)
            p = q[idx]
            st = p.runtime_status()

            if st.is_pipeline_successful():
                _drop_done_or_hard_failed(q, idx)
                # Adjust start if needed (conservatively).
                if not q:
                    s.rr_cursor[pri] = 0
                    return None
                start = start % len(q)
                n = len(q)
                continue

            # Keep it, advance cursor.
            s.rr_cursor[pri] = (idx + 1) % max(1, len(q))
            return p

            tried += 1

        return None

    def _has_runnable_in_queue(pri):
        q = s.queues[pri]
        for p in q:
            st = p.runtime_status()
            if st.is_pipeline_successful():
                continue
            ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
            if ops:
                return True
        return False

    def _priority_order_for_pool(pool_id):
        # If we have multiple pools, reserve pool 0 to favor HP latency.
        if s.executor.num_pools > 1 and pool_id == 0:
            return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]
        # Other pools favor batch to protect pool 0, but can steal HP work if batch empty.
        return [Priority.BATCH_PIPELINE, Priority.INTERACTIVE, Priority.QUERY]

    def _choose_op_from_pipeline(p):
        st = p.runtime_status()
        # One op at a time to reduce head-of-line blocking and improve responsiveness.
        return st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]

    def _base_targets(pool, pri):
        # Conservative defaults to reduce RAM hoarding.
        # These are *starting points*; hints can override upward.
        if pri == Priority.QUERY:
            base_cpu = max(1.0, min(2.0, pool.max_cpu_pool * 0.20))
            base_ram = max(1.0, pool.max_ram_pool * 0.06)
        elif pri == Priority.INTERACTIVE:
            base_cpu = max(1.0, min(4.0, pool.max_cpu_pool * 0.30))
            base_ram = max(1.0, pool.max_ram_pool * 0.10)
        else:
            base_cpu = max(1.0, min(8.0, pool.max_cpu_pool * 0.50))
            base_ram = max(1.0, pool.max_ram_pool * 0.14)
        return base_cpu, base_ram

    def _request_for_op(pool, pri, op_key, avail_cpu, avail_ram, hp_backlog):
        base_cpu, base_ram = _base_targets(pool, pri)

        # Operator-specific hints (from past OOM/timeout).
        ram_hint = s.op_ram_hint.get(op_key, 0.0)
        cpu_hint = s.op_cpu_hint.get(op_key, 0.0)

        # OOM streak escalates RAM faster; timeout streak escalates CPU a bit.
        oom_k = s.op_oom_streak.get(op_key, 0)
        tmo_k = s.op_timeout_streak.get(op_key, 0)

        # RAM:
        # - Use max(base, hint) then apply streak multiplier.
        # - Keep batch from taking too much when HP backlog exists.
        ram_req = max(base_ram, ram_hint)
        if oom_k > 0:
            ram_req *= min(4.0, 1.0 + 0.75 * oom_k)  # grows quickly for repeated OOMs

        # Soft caps to keep headroom for HP work.
        if pri == Priority.BATCH_PIPELINE and hp_backlog:
            ram_req = min(ram_req, pool.max_ram_pool * 0.35)

        # CPU:
        cpu_req = max(base_cpu, cpu_hint)
        if tmo_k > 0:
            cpu_req *= min(2.0, 1.0 + 0.25 * tmo_k)

        # Don't let a single op monopolize the pool.
        # (Especially important for latency; spreading work reduces queueing delay.)
        per_op_cpu_cap = pool.max_cpu_pool * (0.50 if pri == Priority.BATCH_PIPELINE else 0.40)
        per_op_ram_cap = pool.max_ram_pool * (0.60 if pri == Priority.BATCH_PIPELINE else 0.50)

        cpu_req = min(cpu_req, per_op_cpu_cap)
        ram_req = min(ram_req, per_op_ram_cap)

        # Clamp to what's currently available.
        cpu_req = max(1.0, min(cpu_req, avail_cpu, pool.max_cpu_pool))
        ram_req = max(1.0, min(ram_req, avail_ram, pool.max_ram_pool))
        return cpu_req, ram_req

    # ----------------------------
    # Ingest new pipelines
    # ----------------------------
    for p in pipelines:
        _enqueue_pipeline(p)

    if not pipelines and not results:
        return [], []

    # ----------------------------
    # Learn from results (op-level hinting)
    # ----------------------------
    for r in results:
        ops = getattr(r, "ops", None) or []
        failed = False
        try:
            failed = r.failed()
        except Exception:
            failed = bool(getattr(r, "error", None))

        if not ops:
            continue

        # Use first op as representative if list is present; still update all for safety.
        for op in ops:
            k = _op_fingerprint(op)

            if failed and _is_oom_error(getattr(r, "error", None)):
                # OOM => we allocated too little RAM. Increase hint based on what we tried.
                prev = s.op_ram_hint.get(k, 0.0)
                tried = float(getattr(r, "ram", 0.0) or 0.0)
                # If tried is missing/zero, fall back to multiplying existing hint.
                if tried > 0:
                    # Jump more aggressively than before to reduce repeated retries.
                    s.op_ram_hint[k] = max(prev, tried * 2.0)
                else:
                    s.op_ram_hint[k] = max(1.0, prev * 2.0 if prev > 0 else 2.0)
                s.op_oom_streak[k] = min(10, s.op_oom_streak.get(k, 0) + 1)
                # OOM isn't a CPU issue; clear timeout streak.
                s.op_timeout_streak[k] = max(0, s.op_timeout_streak.get(k, 0) - 1)

            elif failed and _is_timeout_error(getattr(r, "error", None)):
                # Timeout => likely too little CPU or too much contention; bump CPU hint modestly.
                prev = s.op_cpu_hint.get(k, 0.0)
                tried = float(getattr(r, "cpu", 0.0) or 0.0)
                if tried > 0:
                    s.op_cpu_hint[k] = max(prev, tried * 1.5)
                else:
                    s.op_cpu_hint[k] = max(1.0, prev * 1.3 if prev > 0 else 2.0)
                s.op_timeout_streak[k] = min(10, s.op_timeout_streak.get(k, 0) + 1)
                # Timeout is not necessarily RAM; gently relax OOM streak.
                s.op_oom_streak[k] = max(0, s.op_oom_streak.get(k, 0) - 1)

            else:
                # Success: we can cautiously reduce streaks and tighten hints slightly
                # to avoid long-term memory hoarding after a few OOM-driven increases.
                tried_ram = float(getattr(r, "ram", 0.0) or 0.0)
                if tried_ram > 0:
                    prev = s.op_ram_hint.get(k, 0.0)
                    # Keep a floor at 90% of last successful allocation (avoid oscillations),
                    # but allow drifting down slowly.
                    if prev <= 0:
                        s.op_ram_hint[k] = tried_ram
                    else:
                        s.op_ram_hint[k] = max(tried_ram * 0.90, prev * 0.97)

                tried_cpu = float(getattr(r, "cpu", 0.0) or 0.0)
                if tried_cpu > 0:
                    prevc = s.op_cpu_hint.get(k, 0.0)
                    if prevc <= 0:
                        s.op_cpu_hint[k] = tried_cpu
                    else:
                        s.op_cpu_hint[k] = max(1.0, prevc * 0.98)

                s.op_oom_streak[k] = max(0, s.op_oom_streak.get(k, 0) - 2)
                s.op_timeout_streak[k] = max(0, s.op_timeout_streak.get(k, 0) - 2)

    # ----------------------------
    # Scheduling
    # ----------------------------
    suspensions = []
    assignments = []

    # If any HP work exists, we keep batch more constrained.
    hp_backlog = _has_runnable_in_queue(Priority.QUERY) or _has_runnable_in_queue(Priority.INTERACTIVE)

    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool

        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # If HP backlog exists, keep some headroom in every pool (especially RAM).
        # This helps reduce the chance that a new QUERY/INTERACTIVE arrival has to wait a whole tick.
        if hp_backlog:
            # Keep a small fixed headroom; don't overdo it or we lose throughput.
            ram_headroom = pool.max_ram_pool * (0.08 if (s.executor.num_pools > 1 and pool_id == 0) else 0.05)
            cpu_headroom = pool.max_cpu_pool * (0.05 if (s.executor.num_pools > 1 and pool_id == 0) else 0.03)
        else:
            ram_headroom = 0.0
            cpu_headroom = 0.0

        # Greedy fill with small ops; stop when no progress.
        made_progress = True
        while made_progress:
            made_progress = False

            # Respect headroom if possible.
            eff_avail_cpu = max(0.0, avail_cpu - cpu_headroom)
            eff_avail_ram = max(0.0, avail_ram - ram_headroom)
            if eff_avail_cpu <= 0 or eff_avail_ram <= 0:
                break

            chosen = None
            chosen_pri = None
            chosen_ops = None

            for pri in _priority_order_for_pool(pool_id):
                p = _next_pipeline_rr(pri)
                if p is None:
                    continue

                op_list = _choose_op_from_pipeline(p)
                if not op_list:
                    # Not runnable right now; keep it in queue.
                    s.queues[pri].append(p)
                    continue

                chosen = p
                chosen_pri = pri
                chosen_ops = op_list
                break

            if chosen is None:
                break

            # Determine per-op key and requested resources.
            op0 = chosen_ops[0]
            op_key = _op_fingerprint(op0)
            cpu_req, ram_req = _request_for_op(pool, chosen_pri, op_key, eff_avail_cpu, eff_avail_ram, hp_backlog)

            # If we can't fit even minimal request, stop on this pool.
            if cpu_req <= 0 or ram_req <= 0:
                s.queues[chosen_pri].append(chosen)
                break

            assignments.append(
                Assignment(
                    ops=chosen_ops,
                    cpu=cpu_req,
                    ram=ram_req,
                    priority=chosen.priority,
                    pool_id=pool_id,
                    pipeline_id=chosen.pipeline_id,
                )
            )

            # Update local remaining resources.
            avail_cpu -= cpu_req
            avail_ram -= ram_req

            # Put pipeline back for future ops.
            s.queues[chosen_pri].append(chosen)

            made_progress = True

    return suspensions, assignments
