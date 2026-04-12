# policy_key: scheduler_iter_best_rich_012
# context: rich
# model: gpt-5.2-2025-12-11
# llm_cost: 0.057737
# generation_seconds: 43.49
# generated_at: 2026-04-12T00:28:17.363273
@register_scheduler_init(key="scheduler_iter_best_rich_012")
def scheduler_iter_best_rich_012_init(s):
    """Priority-aware scheduler focused on reducing weighted latency with small, robust improvements.

    Improvements over naive FIFO and the prior iteration:
      - Pool specialization (when multiple pools exist): prefer QUERY/INTERACTIVE on earlier pools,
        keep BATCH mostly on later pools; allow spillover when dedicated pools are idle.
      - Right-size RAM to avoid waste: start small and learn per-operator RAM from successes;
        only grow on OOM via multiplicative backoff.
      - React to timeouts by increasing CPU for the specific operator (per-op learned CPU).
      - Keep simple round-robin fairness within each priority queue.

    Intentional omissions (kept simple/compatible with minimal interfaces):
      - No preemption/suspension (requires reliable visibility into running containers).
      - No DAG-wide critical-path analysis; still schedules one ready op at a time.
    """
    s.q_query = []
    s.q_interactive = []
    s.q_batch = []

    # Per-operator learned sizing (keys are stable tuples).
    s.last_good_ram = {}   # op_key -> ram
    s.last_good_cpu = {}   # op_key -> cpu

    # Backoff multipliers applied on top of last_good_* (or base defaults).
    s.oom_ram_mult = {}    # op_key -> multiplier
    s.to_cpu_mult = {}     # op_key -> multiplier

    # Track pipelines to avoid infinite retries on non-resource failures (best-effort).
    s.dead_pipelines = set()

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


@register_scheduler(key="scheduler_iter_best_rich_012")
def scheduler_iter_best_rich_012_scheduler(s, results, pipelines):
    # ---- helpers (no imports; keep simulator-friendly) ----
    def _is_oom(err) -> bool:
        if err is None:
            return False
        msg = str(err).lower()
        return ("oom" in msg) or ("out of memory" in msg)

    def _is_timeout(err) -> bool:
        if err is None:
            return False
        msg = str(err).lower()
        return ("timeout" in msg) or ("timed out" in msg) or ("time out" in msg)

    def _enqueue(p):
        if p.pipeline_id in s.dead_pipelines:
            return
        if p.priority == Priority.QUERY:
            s.q_query.append(p)
        elif p.priority == Priority.INTERACTIVE:
            s.q_interactive.append(p)
        else:
            s.q_batch.append(p)

    def _priority_queues():
        return [s.q_query, s.q_interactive, s.q_batch]

    def _queue_for_priority(pri):
        if pri == Priority.QUERY:
            return s.q_query
        if pri == Priority.INTERACTIVE:
            return s.q_interactive
        return s.q_batch

    def _pop_next_runnable(allowed_priorities):
        # Round-robin within each allowed priority, checked in priority order.
        for pri in allowed_priorities:
            q = _queue_for_priority(pri)
            if not q:
                continue
            n = len(q)
            for _ in range(n):
                p = q.pop(0)
                if p.pipeline_id in s.dead_pipelines:
                    continue
                st = p.runtime_status()
                if st.is_pipeline_successful():
                    continue
                op_list = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
                if op_list:
                    return p, op_list
                # Not runnable yet; rotate to back.
                q.append(p)
        return None, None

    def _pool_allowed_priorities(pool_id, num_pools):
        # Specialize pools when possible:
        #   pool 0: QUERY then INTERACTIVE (spill BATCH if idle)
        #   pool 1: INTERACTIVE then QUERY (spill BATCH if idle)
        #   pools 2+: BATCH then INTERACTIVE then QUERY (spill up)
        if num_pools <= 1:
            return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]
        if pool_id == 0:
            return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]
        if pool_id == 1:
            return [Priority.INTERACTIVE, Priority.QUERY, Priority.BATCH_PIPELINE]
        return [Priority.BATCH_PIPELINE, Priority.INTERACTIVE, Priority.QUERY]

    def _base_cpu(priority, pool):
        # Give higher priority slightly more CPU to reduce timeout risk & tail latency.
        maxc = pool.max_cpu_pool
        if priority == Priority.QUERY:
            return min(6.0, maxc * 0.50)
        if priority == Priority.INTERACTIVE:
            return min(8.0, maxc * 0.65)
        return min(10.0, maxc * 0.80)

    def _cpu_cap(priority, pool):
        maxc = pool.max_cpu_pool
        if priority == Priority.QUERY:
            return min(10.0, maxc * 0.70)
        if priority == Priority.INTERACTIVE:
            return min(14.0, maxc * 0.85)
        return min(maxc, maxc * 0.95)

    def _base_ram(priority, pool):
        # Start small to avoid reserving most of the cluster RAM (which raised allocated% massively).
        # Backoff on OOM + learn from successes.
        maxr = pool.max_ram_pool
        if priority == Priority.QUERY:
            return maxr * 0.06
        if priority == Priority.INTERACTIVE:
            return maxr * 0.09
        return maxr * 0.12

    def _ram_cap(priority, pool):
        # Keep an upper bound to avoid one op hoarding RAM in a pool.
        maxr = pool.max_ram_pool
        if priority == Priority.QUERY:
            return maxr * 0.40
        if priority == Priority.INTERACTIVE:
            return maxr * 0.55
        return maxr * 0.75

    def _size_op(priority, pool, op, pipeline_id, avail_cpu, avail_ram):
        opk = s._op_key(op, pipeline_id)

        # RAM sizing: learned last_good * oom_mult, otherwise base.
        base_r = _base_ram(priority, pool)
        good_r = s.last_good_ram.get(opk, base_r)
        oom_m = s.oom_ram_mult.get(opk, 1.0)
        want_r = good_r * oom_m
        want_r = min(want_r, _ram_cap(priority, pool), pool.max_ram_pool, avail_ram)

        # CPU sizing: learned last_good * to_mult, otherwise base.
        base_c = _base_cpu(priority, pool)
        good_c = s.last_good_cpu.get(opk, base_c)
        to_m = s.to_cpu_mult.get(opk, 1.0)
        want_c = good_c * to_m
        want_c = min(want_c, _cpu_cap(priority, pool), pool.max_cpu_pool, avail_cpu)

        # Guard against degenerate tiny allocations that just create churn.
        if want_c < 0.25 or want_r < 0.25:
            return 0.0, 0.0
        return want_c, want_r

    # ---- ingest new pipelines ----
    for p in pipelines:
        _enqueue(p)

    if not pipelines and not results:
        return [], []

    # ---- process results to adapt sizing ----
    for r in results:
        # Attempt to update per-op sizing based on outcomes.
        # Note: ExecutionResult interface doesn't guarantee pipeline_id; use None if missing.
        pid = getattr(r, "pipeline_id", None)
        ops = r.ops or []

        if r.failed():
            if _is_oom(r.error):
                for op in ops:
                    opk = s._op_key(op, pid)
                    # Increase RAM multiplier (capped) and remember that at least r.ram was insufficient.
                    cur_m = s.oom_ram_mult.get(opk, 1.0)
                    nxt_m = cur_m * 2.0
                    if nxt_m > 16.0:
                        nxt_m = 16.0
                    s.oom_ram_mult[opk] = nxt_m

                    # Push the "good" estimate upward toward what we tried (so next try isn't too small).
                    tried = getattr(r, "ram", None)
                    if tried is not None:
                        prev = s.last_good_ram.get(opk, 0.0)
                        if tried > prev:
                            s.last_good_ram[opk] = tried
            elif _is_timeout(r.error):
                for op in ops:
                    opk = s._op_key(op, pid)
                    # Increase CPU multiplier (capped).
                    cur_m = s.to_cpu_mult.get(opk, 1.0)
                    nxt_m = cur_m * 1.5
                    if nxt_m > 6.0:
                        nxt_m = 6.0
                    s.to_cpu_mult[opk] = nxt_m

                    # Also nudge last_good_cpu upward toward what we tried (if available).
                    tried = getattr(r, "cpu", None)
                    if tried is not None:
                        prev = s.last_good_cpu.get(opk, 0.0)
                        if tried > prev:
                            s.last_good_cpu[opk] = tried
            else:
                # Non-resource failure: best-effort mark pipeline as dead if id is available.
                if pid is not None:
                    s.dead_pipelines.add(pid)
        else:
            # Success: remember the resource point as "works"; reduce multipliers slowly.
            for op in ops:
                opk = s._op_key(op, pid)
                tried_r = getattr(r, "ram", None)
                tried_c = getattr(r, "cpu", None)
                if tried_r is not None:
                    s.last_good_ram[opk] = tried_r
                if tried_c is not None:
                    s.last_good_cpu[opk] = tried_c

                # Decay multipliers so we don't permanently over-allocate after a transient issue.
                if opk in s.oom_ram_mult:
                    s.oom_ram_mult[opk] = max(1.0, s.oom_ram_mult[opk] * 0.85)
                    if s.oom_ram_mult[opk] <= 1.01:
                        s.oom_ram_mult.pop(opk, None)
                if opk in s.to_cpu_mult:
                    s.to_cpu_mult[opk] = max(1.0, s.to_cpu_mult[opk] * 0.85)
                    if s.to_cpu_mult[opk] <= 1.01:
                        s.to_cpu_mult.pop(opk, None)

    suspensions = []
    assignments = []

    # ---- schedule: fill pools with prioritized work, respecting specialization ----
    num_pools = s.executor.num_pools
    for pool_id in range(num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool

        allowed = _pool_allowed_priorities(pool_id, num_pools)

        # Try to pack multiple ops per pool to increase throughput, but keep allocations right-sized.
        # Stop if we can't place anything meaningful.
        placed_any = True
        while placed_any and avail_cpu > 0.25 and avail_ram > 0.25:
            placed_any = False

            p, op_list = _pop_next_runnable(allowed)
            if not p:
                break

            op = op_list[0]
            cpu, ram = _size_op(p.priority, pool, op, p.pipeline_id, avail_cpu, avail_ram)

            if cpu <= 0.0 or ram <= 0.0:
                # Can't size it here; rotate pipeline back and stop trying this pool for now.
                _enqueue(p)
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
            placed_any = True

            # Re-enqueue pipeline for next eligible op later (RR fairness).
            _enqueue(p)

    return suspensions, assignments
