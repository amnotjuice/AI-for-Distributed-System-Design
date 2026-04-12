# policy_key: scheduler_iter_best_rich_001
# context: rich
# model: gpt-5.2-2025-12-11
# llm_cost: 0.000000
# generation_seconds: 46.25
# generated_at: 2026-04-12T00:21:00.592755
@register_scheduler_init(key="scheduler_iter_best_rich_001")
def scheduler_iter_best_rich_001_init(s):
    """Priority-aware scheduler tuned to reduce weighted latency by improving concurrency and adaptivity.

    Incremental improvements over the previous priority FIFO:
      - Reduce chronic RAM overallocation (was ~93% allocated, ~29% consumed) by starting smaller.
      - Per-operator adaptive RAM sizing via bounds learning:
          * On OOM at ram=r => lower bound lo=max(lo,r)
          * On success at ram=r => upper bound hi=min(hi,r)
          * Next ram chosen via (lo,hi) bisection when both known; otherwise cautious scaling.
      - Per-operator CPU boost on timeout-like failures (often indicates underprovisioned CPU under contention).
      - Soft reservations: keep headroom for high-priority work; only let batch consume beyond reserves
        when there is interactive/query backlog.
      - Optional pool specialization when multiple pools exist (pool 0 favors QUERY/INTERACTIVE).

    Notes:
      - No preemption yet (no reliable running-container visibility in the provided interface).
      - Uses only interfaces demonstrated in the template.
    """
    # Per-priority round-robin queues
    s.q_query = []
    s.q_interactive = []
    s.q_batch = []

    # Track hopeless pipelines if we can identify them; keep minimal to avoid dropping useful work.
    s.dead_pipelines = set()

    # Operator-level resource learning keyed by (pipeline_id, op_id-ish)
    # ram_bounds: key -> (lo, hi) where hi can be None (unknown)
    s.ram_bounds = {}
    # cpu_mult: key -> multiplier for cpu request (reacts to timeouts)
    s.cpu_mult = {}

    def _op_key(op, pipeline_id):
        oid = getattr(op, "op_id", None)
        if oid is None:
            oid = getattr(op, "operator_id", None)
        if oid is None:
            oid = getattr(op, "name", None)
        if oid is None:
            oid = id(op)
        return (pipeline_id, oid)

    s._op_key = _op_key


@register_scheduler(key="scheduler_iter_best_rich_001")
def scheduler_iter_best_rich_001_scheduler(s, results, pipelines):
    """Priority-first scheduling with adaptive RAM bounds + timeout-driven CPU boosts + soft reservations."""
    # ---------------- helpers ----------------
    def _enqueue_pipeline(p):
        if p.pipeline_id in s.dead_pipelines:
            return
        if p.priority == Priority.QUERY:
            s.q_query.append(p)
        elif p.priority == Priority.INTERACTIVE:
            s.q_interactive.append(p)
        else:
            s.q_batch.append(p)

    def _has_high_prio_backlog():
        return bool(s.q_query) or bool(s.q_interactive)

    def _is_oom_error(err) -> bool:
        if err is None:
            return False
        msg = str(err).lower()
        return ("oom" in msg) or ("out of memory" in msg)

    def _is_timeout_error(err) -> bool:
        if err is None:
            return False
        msg = str(err).lower()
        return ("timeout" in msg) or ("timed out" in msg) or ("time out" in msg)

    def _eligible_in_pool(priority, pool_id, num_pools):
        # Mild pool specialization: if we have multiple pools, keep pool 0 for QUERY/INTERACTIVE
        # to protect latency (without implementing preemption).
        if num_pools >= 2 and pool_id == 0:
            return priority in (Priority.QUERY, Priority.INTERACTIVE)
        return True

    def _next_runnable_op(pool_id):
        # Pick next runnable op in priority order with round-robin within each queue.
        # If pool 0 is specialized, skip batch there.
        queues = [s.q_query, s.q_interactive, s.q_batch]
        for q in queues:
            if not q:
                continue
            n = len(q)
            for _ in range(n):
                p = q.pop(0)
                if p.pipeline_id in s.dead_pipelines:
                    continue
                if not _eligible_in_pool(p.priority, pool_id, s.executor.num_pools):
                    # Put back and continue scanning
                    q.append(p)
                    continue

                st = p.runtime_status()
                if st.is_pipeline_successful():
                    continue

                op_list = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
                if op_list:
                    return p, op_list
                # Not runnable yet; rotate
                q.append(p)
        return None, None

    def _get_ram_bounds(op_key):
        lo, hi = s.ram_bounds.get(op_key, (0.0, None))
        if lo < 0:
            lo = 0.0
        if hi is not None and hi < lo:
            # Shouldn't happen, but keep consistent
            hi = lo
        return lo, hi

    def _update_bounds_on_oom(op_key, ram_used):
        lo, hi = _get_ram_bounds(op_key)
        if ram_used is None:
            return
        # OOM means true peak > allocated; set lower bound at least allocated.
        lo = max(lo, float(ram_used))
        # If we had a known upper bound that is now below lo, relax it.
        if hi is not None and hi < lo:
            hi = None
        s.ram_bounds[op_key] = (lo, hi)

    def _update_bounds_on_success(op_key, ram_used):
        if ram_used is None:
            return
        lo, hi = _get_ram_bounds(op_key)
        r = float(ram_used)
        # Success at r gives an upper bound.
        if hi is None:
            hi = r
        else:
            hi = min(hi, r)
        # If lo > hi due to noise, clamp.
        if hi is not None and lo > hi:
            lo = hi
        s.ram_bounds[op_key] = (lo, hi)

    def _bump_cpu_on_timeout(op_key):
        cur = s.cpu_mult.get(op_key, 1.0)
        nxt = cur * 1.35
        if nxt > 3.0:
            nxt = 3.0
        s.cpu_mult[op_key] = nxt

    def _size_request(priority, pool, op, pipeline_id, avail_cpu, avail_ram):
        # CPU: prioritize latency by giving more CPU to QUERY/INTERACTIVE,
        # but avoid taking the entire pool for a single op.
        max_cpu = float(pool.max_cpu_pool)
        max_ram = float(pool.max_ram_pool)

        if priority == Priority.QUERY:
            cpu_cap = min(6.0, max_cpu * 0.60)
            base_ram_frac = 0.05
            min_cpu = 1.0
        elif priority == Priority.INTERACTIVE:
            cpu_cap = min(8.0, max_cpu * 0.65)
            base_ram_frac = 0.07
            min_cpu = 1.0
        else:
            cpu_cap = min(10.0, max_cpu * 0.70)
            base_ram_frac = 0.09
            min_cpu = 0.5

        op_key = s._op_key(op, pipeline_id)
        cpu_mult = s.cpu_mult.get(op_key, 1.0)

        # CPU allocation
        cpu = min(float(avail_cpu), cpu_cap * cpu_mult)
        if cpu < min_cpu:
            cpu = 0.0

        # RAM allocation using learned bounds
        lo, hi = _get_ram_bounds(op_key)
        base = max_ram * base_ram_frac

        if hi is not None and lo > 0:
            # Bisection between bounds (slightly above midpoint to reduce repeated OOMs)
            target = (lo + hi) * 0.55
            ram = max(target, lo * 1.10)
            ram = min(ram, hi)
        elif hi is not None:
            # If we only know it can succeed at hi, try to reduce a bit for better concurrency.
            ram = max(base, hi * 0.75)
        elif lo > 0:
            # Only know it OOM'd at lo -> allocate some headroom above it.
            ram = max(base, lo * 1.65)
        else:
            # Cold start: keep small to avoid choking the pool (improves latency via parallelism).
            ram = base

        # Clamp to what's available
        ram = min(float(avail_ram), float(max_ram), float(ram))
        if ram <= 0.0:
            ram = 0.0

        return cpu, ram

    def _reserve_headroom(pool, allow_batch):
        # If there is high-priority backlog, reserve some cpu/ram so batch doesn't crowd it out.
        # Reservations are "soft" (we still schedule batch if no high-priority waiting).
        if not allow_batch:
            return 0.0, 0.0
        if not _has_high_prio_backlog():
            return 0.0, 0.0
        # Keep modest reserved headroom; too high reduces throughput and can increase timeouts.
        return (pool.max_cpu_pool * 0.20, pool.max_ram_pool * 0.18)

    # ---------------- ingest new pipelines ----------------
    for p in pipelines:
        _enqueue_pipeline(p)

    # Early exit if nothing changed
    if not pipelines and not results:
        return [], []

    # ---------------- process results (learn per-op) ----------------
    for r in results:
        # We can only reliably learn using r.ops and r.ram/r.cpu.
        ops = r.ops or []
        # pipeline_id is not part of the listed ExecutionResult interface; use None if absent.
        pid = getattr(r, "pipeline_id", None)

        if r.failed():
            if _is_oom_error(r.error):
                for op in ops:
                    op_key = s._op_key(op, pid)
                    _update_bounds_on_oom(op_key, r.ram)
            elif _is_timeout_error(r.error):
                for op in ops:
                    op_key = s._op_key(op, pid)
                    _bump_cpu_on_timeout(op_key)
            else:
                # Unknown failure: avoid infinite retries if pipeline id is available.
                if pid is not None:
                    s.dead_pipelines.add(pid)
        else:
            # Success: tighten upper bounds and optionally relax CPU multiplier slowly.
            for op in ops:
                op_key = s._op_key(op, pid)
                _update_bounds_on_success(op_key, r.ram)
                # Decay cpu multiplier toward 1.0 on successes to avoid runaway CPU hogging.
                cur = s.cpu_mult.get(op_key, 1.0)
                if cur > 1.0:
                    s.cpu_mult[op_key] = max(1.0, cur * 0.95)

    suspensions = []
    assignments = []

    # ---------------- schedule across pools ----------------
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)

        # Fill the pool while we have meaningful headroom.
        # Use reservations only to limit BATCH when high-priority backlog exists.
        while avail_cpu > 0.01 and avail_ram > 0.01:
            p, op_list = _next_runnable_op(pool_id)
            if not p:
                break

            # If the selected work is batch and we should reserve headroom, enforce it.
            reserve_cpu, reserve_ram = _reserve_headroom(pool, allow_batch=True)
            if p.priority == Priority.BATCH_PIPELINE and _has_high_prio_backlog():
                # Only allow batch to use resources above the reserved headroom.
                eff_avail_cpu = max(0.0, avail_cpu - reserve_cpu)
                eff_avail_ram = max(0.0, avail_ram - reserve_ram)
                if eff_avail_cpu <= 0.01 or eff_avail_ram <= 0.01:
                    # Put pipeline back and stop scheduling batch in this pool for now.
                    _enqueue_pipeline(p)
                    break
                cpu, ram = _size_request(p.priority, pool, op_list[0], p.pipeline_id, eff_avail_cpu, eff_avail_ram)
            else:
                cpu, ram = _size_request(p.priority, pool, op_list[0], p.pipeline_id, avail_cpu, avail_ram)

            # If we can't fit a sensible allocation, requeue and stop on this pool to avoid spinning.
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

            # Round-robin fairness: put pipeline back for later ops.
            _enqueue_pipeline(p)

    return suspensions, assignments
