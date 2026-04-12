# policy_key: scheduler_iter_best_rich_017
# context: rich
# model: gpt-5.2-2025-12-11
# llm_cost: 0.058283
# generation_seconds: 39.95
# generated_at: 2026-04-12T00:31:55.269631
@register_scheduler_init(key="scheduler_iter_best_rich_017")
def scheduler_iter_best_rich_017_init(s):
    """Priority-aware scheduler focused on lowering weighted latency with minimal added complexity.

    Improvements over the previous iteration:
      - Smaller default RAM requests (RAM beyond the true minimum doesn't help; over-allocation hurts concurrency).
      - Adaptive backoff not only for OOM (RAM) but also for timeout (CPU).
      - Soft resource reservations: if high-priority work is queued, don't let BATCH consume the whole pool.
      - Slight operator batching for high-priority (assign up to 2 ready ops) to reduce per-op queuing overhead.

    Intentional omissions (for robustness with the exposed simulator API):
      - No preemption/suspension (we don't rely on visibility into running containers).
      - No per-VM placement (only per-pool headroom is exposed in the template).
    """
    # Per-priority queues (simple RR fairness within each class)
    s.q_query = []
    s.q_interactive = []
    s.q_batch = []

    # Adaptive multipliers per operator identity (best-effort stable key).
    # We key by (pipeline_id, op_id-ish). If op lacks ids, fall back to id(op).
    s.op_ram_mult = {}
    s.op_cpu_mult = {}

    # Remember last seen pipeline_id for op object ids (helps when ExecutionResult lacks pipeline_id).
    s.opid_to_pid = {}

    def _op_key(pipeline_id, op):
        oid = getattr(op, "op_id", None)
        if oid is None:
            oid = getattr(op, "operator_id", None)
        if oid is None:
            oid = getattr(op, "id", None)
        if oid is None:
            oid = id(op)
        return (pipeline_id, oid)

    s._op_key = _op_key


@register_scheduler(key="scheduler_iter_best_rich_017")
def scheduler_iter_best_rich_017_scheduler(s, results, pipelines):
    """Run one scheduling tick."""
    # --- helpers ---
    def _enqueue(p):
        if p.priority == Priority.QUERY:
            s.q_query.append(p)
        elif p.priority == Priority.INTERACTIVE:
            s.q_interactive.append(p)
        else:
            s.q_batch.append(p)

    def _oom(err) -> bool:
        if err is None:
            return False
        msg = str(err).lower()
        return ("oom" in msg) or ("out of memory" in msg)

    def _timeout(err) -> bool:
        if err is None:
            return False
        msg = str(err).lower()
        return ("timeout" in msg) or ("timed out" in msg) or ("deadline" in msg)

    def _backlog_flags():
        # "Queued" means present in queue; we don't attempt to compute exact runnable readiness here.
        return (len(s.q_query) > 0), (len(s.q_interactive) > 0)

    def _pick_next_runnable(priorities):
        # priorities: list of queues in preference order
        for q in priorities:
            if not q:
                continue
            n = len(q)
            for _ in range(n):
                p = q.pop(0)
                st = p.runtime_status()
                if st.is_pipeline_successful():
                    continue

                # Track mapping for later result processing (if results don't carry pipeline_id).
                # We'll update once we see runnable ops below.
                # For high priority, we may assign up to 2 ops; for others, 1.
                max_ops = 2 if p.priority in (Priority.QUERY, Priority.INTERACTIVE) else 1
                ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:max_ops]
                if ops:
                    for op in ops:
                        s.opid_to_pid[id(op)] = p.pipeline_id
                    return p, ops

                # Not runnable yet; rotate for RR fairness.
                q.append(p)
        return None, None

    def _base_caps(priority, pool):
        # CPU: reduce timeouts by giving more CPU than the previous conservative caps,
        # while still preventing one container from eating an entire pool.
        if priority == Priority.QUERY:
            cpu_cap = min(6.0, pool.max_cpu_pool * 0.50)
            ram_frac = 0.05
        elif priority == Priority.INTERACTIVE:
            cpu_cap = min(8.0, pool.max_cpu_pool * 0.60)
            ram_frac = 0.07
        else:
            cpu_cap = min(10.0, pool.max_cpu_pool * 0.75)
            ram_frac = 0.10
        return cpu_cap, ram_frac

    def _reserve_for_high_pri(pool, has_query, has_interactive):
        # Soft reservations: when high-priority work is waiting, keep headroom so it can start quickly.
        # These are intentionally simple and pool-relative.
        if has_query:
            return (pool.max_cpu_pool * 0.35, pool.max_ram_pool * 0.20)
        if has_interactive:
            return (pool.max_cpu_pool * 0.20, pool.max_ram_pool * 0.10)
        return (0.0, 0.0)

    def _size(p, ops, pool, avail_cpu, avail_ram):
        cpu_cap, ram_frac = _base_caps(p.priority, pool)

        # If we batch 2 ops, split CPU/RAM budget between them to avoid over-requesting.
        # (Assignment still gets a single cpu/ram value; we approximate by scaling down when batching.)
        batch_factor = 1.0 if len(ops) <= 1 else 1.35  # >1 means slightly more resources when batching, but not 2x
        base_cpu = max(1.0, cpu_cap / batch_factor)
        base_ram = max(0.01, (pool.max_ram_pool * ram_frac) / batch_factor)

        # Operator-specific adaptation: use the worst-case multiplier among ops in this assignment.
        # (Keeps logic simple and avoids partial OOM in batched assignment.)
        ram_mult = 1.0
        cpu_mult = 1.0
        for op in ops:
            k = s._op_key(p.pipeline_id, op)
            ram_mult = max(ram_mult, s.op_ram_mult.get(k, 1.0))
            cpu_mult = max(cpu_mult, s.op_cpu_mult.get(k, 1.0))

        cpu = min(avail_cpu, cpu_cap, base_cpu * cpu_mult)
        ram = min(avail_ram, pool.max_ram_pool, base_ram * ram_mult)

        # Avoid tiny allocations that are likely to increase runtime / cause timeouts.
        if cpu < 1.0:
            cpu = 0.0
        if ram <= 0.01:
            ram = 0.0
        return cpu, ram

    # --- ingest new pipelines ---
    for p in pipelines:
        _enqueue(p)

    # --- process results: adaptive backoff on OOM/timeout, mild decay on success ---
    for r in results:
        # Determine pipeline id best-effort
        pid = getattr(r, "pipeline_id", None)
        if pid is None and r.ops:
            # use first op to find pipeline
            pid = s.opid_to_pid.get(id(r.ops[0]), None)

        if r.failed():
            if r.ops:
                for op in r.ops:
                    op_pid = pid
                    if op_pid is None:
                        op_pid = s.opid_to_pid.get(id(op), None)
                    k = s._op_key(op_pid, op)

                    if _oom(r.error):
                        cur = s.op_ram_mult.get(k, 1.0)
                        # Aggressive enough to converge quickly, but capped to avoid reserving the whole pool.
                        nxt = min(cur * 2.0, 32.0)
                        s.op_ram_mult[k] = nxt
                    elif _timeout(r.error):
                        cur = s.op_cpu_mult.get(k, 1.0)
                        nxt = min(cur * 1.6, 8.0)
                        s.op_cpu_mult[k] = nxt
            continue

        # On success, mildly decay multipliers (prevents permanent over-allocation after transient spikes).
        if r.ops:
            for op in r.ops:
                op_pid = pid
                if op_pid is None:
                    op_pid = s.opid_to_pid.get(id(op), None)
                k = s._op_key(op_pid, op)
                if k in s.op_ram_mult:
                    s.op_ram_mult[k] = max(1.0, s.op_ram_mult[k] * 0.95)
                if k in s.op_cpu_mult:
                    s.op_cpu_mult[k] = max(1.0, s.op_cpu_mult[k] * 0.97)

    # Early exit if no new information to act on
    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    # --- schedule: fill pools while respecting soft reservations for high priority ---
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool

        # Attempt to schedule multiple assignments per pool per tick
        # but stop if headroom is too low.
        while avail_cpu > 0.5 and avail_ram > 0.05:
            has_query, has_interactive = _backlog_flags()
            res_cpu, res_ram = _reserve_for_high_pri(pool, has_query, has_interactive)

            # Priority order: always try QUERY first, then INTERACTIVE, then BATCH.
            p, ops = _pick_next_runnable([s.q_query, s.q_interactive, s.q_batch])
            if not p:
                break

            cpu, ram = _size(p, ops, pool, avail_cpu, avail_ram)

            # If this is BATCH and we need to preserve headroom for queued high-priority work, defer it.
            if p.priority == Priority.BATCH_PIPELINE and (has_query or has_interactive):
                # Only admit batch if we'd still have reservation left after starting it.
                if (avail_cpu - cpu) < res_cpu or (avail_ram - ram) < res_ram:
                    # Put it back and try again; if only batch exists, reservation is 0 and it will pass.
                    _enqueue(p)
                    # If we can't schedule batch due to reservation, try to find high-priority runnable again.
                    # To avoid infinite looping when high-priority exists but isn't runnable (blocked on deps),
                    # we allow batch through if it's the only runnable option.
                    # We implement that by doing a second attempt without reservation if no high-priority runnable.
                    p2, ops2 = _pick_next_runnable([s.q_query, s.q_interactive])
                    if p2:
                        # Put p2 back at the front of its queue by re-enqueueing (RR)
                        # and continue loop to schedule it.
                        _enqueue(p2)
                        continue
                    # No runnable high priority right now; relax reservation and proceed with batch.
                    # (Still uses the computed cpu/ram for p/ops.)
                # else: OK to schedule batch

            if cpu <= 0.0 or ram <= 0.0:
                _enqueue(p)
                break

            assignments.append(
                Assignment(
                    ops=ops,
                    cpu=cpu,
                    ram=ram,
                    priority=p.priority,
                    pool_id=pool_id,
                    pipeline_id=p.pipeline_id,
                )
            )

            # Update local headroom and RR rotate pipeline
            avail_cpu -= cpu
            avail_ram -= ram
            _enqueue(p)

    return suspensions, assignments
