# policy_key: scheduler_iter_best_rich_005
# context: rich
# model: gpt-5.2-2025-12-11
# llm_cost: 0.052263
# generation_seconds: 36.58
# generated_at: 2026-04-12T00:23:35.936240
@register_scheduler_init(key="scheduler_iter_best_rich_005")
def scheduler_iter_best_rich_005_init(s):
    """Priority-aware, work-conserving scheduler focused on lowering weighted latency.

    Improvements over prior iteration:
      - Reduce systematic RAM overallocation (which was starving concurrency and causing timeouts).
      - Learn per-operator RAM/CPU multipliers from outcomes:
          * OOM -> increase RAM multiplier aggressively
          * timeout -> increase CPU multiplier moderately
          * success -> decay multipliers back toward 1.0
      - Queue discipline: strict priority (QUERY > INTERACTIVE > BATCH) with round-robin within each class.

    Intent:
      - Keep the system highly concurrent (lower RAM fractions) while adapting quickly for memory-hungry ops.
      - Reduce timeout failures by increasing CPU for ops that repeatedly time out.
    """
    # Per-priority FIFO queues (round-robin via rotate-on-attempt).
    s.q_query = []
    s.q_interactive = []
    s.q_batch = []

    # Per-operator adaptive sizing profile keyed by operator identity.
    # profile := {"ram_mult": float, "cpu_mult": float, "last_seen": int}
    s.op_profile = {}

    # Tick counter for light bookkeeping.
    s._tick = 0

    def _op_stable_key(op):
        # Prefer explicit ids if present; otherwise use object identity.
        oid = getattr(op, "op_id", None)
        if oid is None:
            oid = getattr(op, "operator_id", None)
        if oid is None:
            oid = id(op)
        return oid

    s._op_key = _op_stable_key


@register_scheduler(key="scheduler_iter_best_rich_005")
def scheduler_iter_best_rich_005_scheduler(s, results, pipelines):
    """Scheduler step: process results (update profiles), enqueue new pipelines, then fill pools."""
    s._tick += 1

    # --- helpers ---
    def _enqueue_pipeline(p):
        if p.priority == Priority.QUERY:
            s.q_query.append(p)
        elif p.priority == Priority.INTERACTIVE:
            s.q_interactive.append(p)
        else:
            s.q_batch.append(p)

    def _queues_in_order():
        return (s.q_query, s.q_interactive, s.q_batch)

    def _is_oom(err) -> bool:
        if err is None:
            return False
        msg = str(err).lower()
        return ("oom" in msg) or ("out of memory" in msg)

    def _is_timeout(err) -> bool:
        if err is None:
            return False
        msg = str(err).lower()
        return ("timeout" in msg) or ("timed out" in msg) or ("time limit" in msg)

    def _get_profile(op_key):
        prof = s.op_profile.get(op_key)
        if prof is None:
            prof = {"ram_mult": 1.0, "cpu_mult": 1.0, "last_seen": s._tick}
            s.op_profile[op_key] = prof
        else:
            prof["last_seen"] = s._tick
        return prof

    def _update_profiles_from_results():
        # Update per-op multipliers based on completion outcomes.
        for r in results:
            ops = r.ops or []
            if not ops:
                continue

            if r.failed():
                is_oom = _is_oom(r.error)
                is_to = _is_timeout(r.error)

                for op in ops:
                    k = s._op_key(op)
                    prof = _get_profile(k)

                    # OOM: increase RAM quickly; CPU doesn't help if we're below min RAM.
                    if is_oom:
                        prof["ram_mult"] = min(32.0, prof["ram_mult"] * 2.0)
                        # Slightly reduce CPU mult back toward 1 to avoid wasting CPU on memory-bound retries.
                        prof["cpu_mult"] = max(1.0, prof["cpu_mult"] * 0.95)

                    # Timeout: likely underprovisioned CPU or too much queueing; try more CPU next time.
                    elif is_to:
                        prof["cpu_mult"] = min(8.0, prof["cpu_mult"] * 1.5)
                        # Keep RAM roughly stable; slight bump can help if paging-like behavior is modeled.
                        prof["ram_mult"] = min(32.0, prof["ram_mult"] * 1.1)

                    # Other failures: don't thrash sizing too much.
                    else:
                        prof["cpu_mult"] = min(8.0, prof["cpu_mult"] * 1.05)
                        prof["ram_mult"] = min(32.0, prof["ram_mult"] * 1.05)
            else:
                # Success: decay multipliers toward 1.0 to reclaim concurrency.
                for op in ops:
                    k = s._op_key(op)
                    prof = _get_profile(k)
                    prof["ram_mult"] = max(1.0, prof["ram_mult"] * 0.90)
                    prof["cpu_mult"] = max(1.0, prof["cpu_mult"] * 0.92)

    def _pop_next_runnable():
        # Strict priority with round-robin within each priority class.
        for q in _queues_in_order():
            if not q:
                continue
            n = len(q)
            for _ in range(n):
                p = q.pop(0)
                st = p.runtime_status()
                if st.is_pipeline_successful():
                    continue
                op_list = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
                if op_list:
                    return p, op_list
                # Not runnable yet (parents incomplete), rotate.
                q.append(p)
        return None, None

    def _base_fracs_and_caps(priority):
        # Lower RAM fractions than the previous policy to avoid starving concurrency.
        # CPU caps are moderate; we prefer parallelism over per-op scale-up unless timeouts force adaptation.
        if priority == Priority.QUERY:
            return 0.05, 0.30, 1.0, 4.0   # ram_frac, cpu_cap_frac, cpu_min, cpu_cap_abs
        if priority == Priority.INTERACTIVE:
            return 0.07, 0.40, 1.0, 6.0
        return 0.09, 0.60, 1.0, 10.0     # batch

    def _size_op(priority, pool, op, avail_cpu, avail_ram):
        ram_frac, cpu_cap_frac, cpu_min, cpu_cap_abs = _base_fracs_and_caps(priority)
        max_cpu = pool.max_cpu_pool
        max_ram = pool.max_ram_pool

        # Base request sizes (fractions of pool); then adapt via per-op multipliers.
        base_ram = max_ram * ram_frac
        base_cpu = max(cpu_min, min(max_cpu * 0.10, 2.0))  # default around 1-2 vCPU

        # Priority nudges: queries slightly more CPU by default.
        if priority == Priority.QUERY:
            base_cpu = max(base_cpu, 2.0)
        elif priority == Priority.BATCH_PIPELINE:
            base_cpu = max(base_cpu, 1.0)

        prof = _get_profile(s._op_key(op))
        ram = base_ram * prof["ram_mult"]
        cpu = base_cpu * prof["cpu_mult"]

        # Caps: prevent a single op from consuming the whole pool.
        cpu_cap = min(max_cpu * cpu_cap_frac, cpu_cap_abs, avail_cpu)
        cpu = min(max(cpu_min, cpu), cpu_cap)

        # RAM: cap by available and pool capacity.
        # Ensure we don't request trivially tiny RAM that causes inevitable OOM churn.
        ram_floor = max_ram * 0.02
        ram = max(ram_floor, ram)
        ram = min(ram, avail_ram, max_ram)

        # If resources are very tight, shrink CPU first (RAM below floor tends to create OOM churn).
        if ram > avail_ram:
            ram = avail_ram
        if cpu > avail_cpu:
            cpu = avail_cpu

        return cpu, ram

    # --- ingest new pipelines ---
    for p in pipelines:
        _enqueue_pipeline(p)

    if not pipelines and not results:
        return [], []

    # --- update adaptive sizing based on results ---
    _update_profiles_from_results()

    suspensions = []
    assignments = []

    # --- fill each pool work-conservingly with strict priority ---
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool

        # Schedule as many single-op assignments as fit; prioritize keeping RAM low to boost concurrency.
        while avail_cpu > 0.01 and avail_ram > 0.01:
            p, op_list = _pop_next_runnable()
            if p is None:
                break

            op = op_list[0]
            cpu, ram = _size_op(p.priority, pool, op, avail_cpu, avail_ram)

            # If we can't allocate meaningful resources, put it back and stop on this pool.
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

            avail_cpu -= cpu
            avail_ram -= ram

            # Round-robin fairness within a priority: re-enqueue pipeline.
            _enqueue_pipeline(p)

    return suspensions, assignments
