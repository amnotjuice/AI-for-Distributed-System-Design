# policy_key: scheduler_iter_best_rich_016
# context: rich
# model: gpt-5.2-2025-12-11
# llm_cost: 0.063449
# generation_seconds: 41.10
# generated_at: 2026-04-12T00:31:15.314568
@register_scheduler_init(key="scheduler_iter_best_rich_016")
def scheduler_iter_best_rich_016_init(s):
    """Incremental improvement over prior priority-aware FIFO to further reduce weighted latency.

    Main changes (kept intentionally simple and robust):
      1) Much lower *initial* RAM reservations + learned per-operator RAM hints from past successes.
         - Prior policy reserved large RAM fractions, leading to very high allocated RAM (92%),
           low consumed RAM (29%), and reduced concurrency -> more queueing/timeouts.
      2) Soft reservations: when high-priority work is waiting, limit how much batch can consume.
      3) Light pool specialization when multiple pools exist:
         - pool 0: prefers QUERY/INTERACTIVE
         - last pool: prefers BATCH
         - middle pools: mixed, but still priority-first
      4) Adaptive backoff:
         - OOM => increase RAM multiplier for that operator
         - timeout => modestly increase CPU multiplier for that operator (bounded)
         - success => record RAM/CPU hints for future sizing

    Preemption is still omitted (no reliable visibility into running containers is guaranteed
    by the provided interface).
    """
    # Per-priority waiting queues (round-robin within each).
    s.q_query = []
    s.q_interactive = []
    s.q_batch = []

    # Operator-level learned hints and multipliers.
    # Keyed by a stable-ish (pipeline_id, op_id) tuple when possible.
    s.op_ram_hint = {}    # op_key -> best-known successful RAM allocation
    s.op_cpu_hint = {}    # op_key -> best-known successful CPU allocation
    s.op_ram_mult = {}    # op_key -> multiplier applied to ram_hint/base
    s.op_cpu_mult = {}    # op_key -> multiplier applied to cpu_hint/base

    # Pipelines to stop retrying on non-resource-related failures (best effort).
    s.dead_pipelines = set()

    def _op_key(op, pipeline_id):
        oid = getattr(op, "op_id", None)
        if oid is None:
            oid = getattr(op, "operator_id", None)
        if oid is None:
            oid = id(op)
        return (pipeline_id, oid)

    s._op_key = _op_key


@register_scheduler(key="scheduler_iter_best_rich_016")
def scheduler_iter_best_rich_016_scheduler(s, results, pipelines):
    """Priority-first, RAM-efficient scheduling with learned sizing and soft reservations."""
    def _is_oom_error(err) -> bool:
        if err is None:
            return False
        msg = str(err).lower()
        return ("oom" in msg) or ("out of memory" in msg) or ("killed" in msg and "memory" in msg)

    def _is_timeout_error(err) -> bool:
        if err is None:
            return False
        msg = str(err).lower()
        return "timeout" in msg or "timed out" in msg or "time out" in msg

    def _enqueue_pipeline(p):
        if p.pipeline_id in s.dead_pipelines:
            return
        if p.priority == Priority.QUERY:
            s.q_query.append(p)
        elif p.priority == Priority.INTERACTIVE:
            s.q_interactive.append(p)
        else:
            s.q_batch.append(p)

    def _queue_for_priority(priority):
        if priority == Priority.QUERY:
            return s.q_query
        if priority == Priority.INTERACTIVE:
            return s.q_interactive
        return s.q_batch

    def _any_waiting(prio) -> bool:
        q = _queue_for_priority(prio)
        return len(q) > 0

    def _pool_priority_preferences(pool_id: int):
        # When we have multiple pools, lightly specialize to reduce interference:
        # - pool 0: prefer QUERY/INTERACTIVE
        # - last pool: prefer BATCH
        # - middle pools: mixed priority order
        n = s.executor.num_pools
        if n <= 1:
            return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]
        if pool_id == 0:
            return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]
        if pool_id == n - 1:
            return [Priority.BATCH_PIPELINE, Priority.INTERACTIVE, Priority.QUERY]
        return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]

    def _pop_next_runnable_pipeline(preferred_priorities):
        # Scan queues in preferred order; round-robin within each queue.
        for prio in preferred_priorities:
            q = _queue_for_priority(prio)
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
                # Not runnable yet, rotate.
                q.append(p)
        return None, None

    def _soft_reserve_headroom(priority, avail_cpu, avail_ram, pool):
        # If high-priority work is waiting anywhere, keep some headroom by restricting batch.
        # This reduces queueing/tail latency for interactive/query.
        if priority != Priority.BATCH_PIPELINE:
            return avail_cpu, avail_ram

        # If query waiting, reserve more headroom; if only interactive waiting, reserve some.
        query_wait = _any_waiting(Priority.QUERY)
        interactive_wait = _any_waiting(Priority.INTERACTIVE)

        if not query_wait and not interactive_wait:
            return avail_cpu, avail_ram

        # Reserve fractions of total pool capacity (not just current availability).
        # Use soft caps (don't go negative).
        if query_wait:
            reserve_cpu = 0.25 * pool.max_cpu_pool
            reserve_ram = 0.30 * pool.max_ram_pool
        else:
            reserve_cpu = 0.15 * pool.max_cpu_pool
            reserve_ram = 0.20 * pool.max_ram_pool

        eff_cpu = max(0.0, avail_cpu - reserve_cpu)
        eff_ram = max(0.0, avail_ram - reserve_ram)
        return eff_cpu, eff_ram

    def _size_for(priority, pool, op, pipeline_id, avail_cpu, avail_ram):
        # Use learned hints first; otherwise start with small fractions to avoid over-reserving RAM.
        op_key = s._op_key(op, pipeline_id)

        # Base CPU and RAM sizing targets by priority.
        # (Aim: more CPU for query/interactive; small initial RAM to improve concurrency.)
        if priority == Priority.QUERY:
            base_cpu = min(4.0, pool.max_cpu_pool * 0.40)
            base_ram = pool.max_ram_pool * 0.03
            cpu_cap = min(8.0, pool.max_cpu_pool * 0.70)
            ram_cap = pool.max_ram_pool * 0.25
        elif priority == Priority.INTERACTIVE:
            base_cpu = min(4.0, pool.max_cpu_pool * 0.35)
            base_ram = pool.max_ram_pool * 0.05
            cpu_cap = min(8.0, pool.max_cpu_pool * 0.65)
            ram_cap = pool.max_ram_pool * 0.35
        else:  # BATCH
            base_cpu = min(8.0, pool.max_cpu_pool * 0.55)
            base_ram = pool.max_ram_pool * 0.07
            cpu_cap = min(16.0, pool.max_cpu_pool * 0.85)
            ram_cap = pool.max_ram_pool * 0.60

        # Learned hints (from successful runs) often prevent OOM retries without wasting RAM.
        ram_hint = s.op_ram_hint.get(op_key, 0.0)
        cpu_hint = s.op_cpu_hint.get(op_key, 0.0)

        # Multipliers (OOM/timeout backoff), bounded.
        ram_mult = s.op_ram_mult.get(op_key, 1.0)
        cpu_mult = s.op_cpu_mult.get(op_key, 1.0)

        # Pick targets; never below a tiny floor to avoid zero allocations.
        target_ram = max(base_ram, ram_hint) * ram_mult
        target_cpu = max(base_cpu, cpu_hint) * cpu_mult

        # Clamp to caps and availability.
        cpu = min(avail_cpu, cpu_cap, target_cpu)
        ram = min(avail_ram, ram_cap, target_ram)

        # Simple minimum floors (avoid pathological tiny allocations).
        if cpu < 0.5:
            cpu = 0.0
        if ram < (pool.max_ram_pool * 0.005):
            ram = 0.0

        return cpu, ram

    # Enqueue new arrivals.
    for p in pipelines:
        _enqueue_pipeline(p)

    if not pipelines and not results:
        return [], []

    # Process results to learn hints and adjust backoff.
    for r in results:
        # Learn from successes: record that this RAM/CPU worked for these ops.
        if not r.failed():
            for op in (r.ops or []):
                # Best effort: ExecutionResult doesn't promise pipeline_id, so we fall back.
                pid = getattr(r, "pipeline_id", None)
                op_key = s._op_key(op, pid)

                # Record the minimum successful ram/cpu we observed (prefer smaller to reduce reservation),
                # but keep the maximum between observed and existing if existing is 0.
                prev_ram = s.op_ram_hint.get(op_key, 0.0)
                prev_cpu = s.op_cpu_hint.get(op_key, 0.0)
                if prev_ram <= 0.0:
                    s.op_ram_hint[op_key] = float(r.ram)
                else:
                    s.op_ram_hint[op_key] = min(prev_ram, float(r.ram))

                if prev_cpu <= 0.0:
                    s.op_cpu_hint[op_key] = float(r.cpu)
                else:
                    s.op_cpu_hint[op_key] = min(prev_cpu, float(r.cpu))

                # On success, gently decay multipliers back toward 1 to avoid permanent bloat.
                rm = s.op_ram_mult.get(op_key, 1.0)
                cm = s.op_cpu_mult.get(op_key, 1.0)
                if rm > 1.0:
                    s.op_ram_mult[op_key] = max(1.0, rm * 0.9)
                if cm > 1.0:
                    s.op_cpu_mult[op_key] = max(1.0, cm * 0.9)
            continue

        # Failures: adjust backoff.
        if _is_oom_error(r.error):
            for op in (r.ops or []):
                pid = getattr(r, "pipeline_id", None)
                op_key = s._op_key(op, pid)
                cur = s.op_ram_mult.get(op_key, 1.0)
                nxt = cur * 2.0
                if nxt > 32.0:
                    nxt = 32.0
                s.op_ram_mult[op_key] = nxt

                # Also raise RAM hint at least to what we attempted (so we don't keep starting too low).
                prev = s.op_ram_hint.get(op_key, 0.0)
                if prev <= 0.0:
                    s.op_ram_hint[op_key] = float(r.ram)
                else:
                    s.op_ram_hint[op_key] = max(prev, float(r.ram))
        elif _is_timeout_error(r.error):
            # Timeouts are often CPU starvation under contention; modestly boost CPU for next try.
            for op in (r.ops or []):
                pid = getattr(r, "pipeline_id", None)
                op_key = s._op_key(op, pid)
                cur = s.op_cpu_mult.get(op_key, 1.0)
                nxt = cur * 1.25
                if nxt > 4.0:
                    nxt = 4.0
                s.op_cpu_mult[op_key] = nxt

                # Raise CPU hint to at least what we attempted.
                prev = s.op_cpu_hint.get(op_key, 0.0)
                if prev <= 0.0:
                    s.op_cpu_hint[op_key] = float(r.cpu)
                else:
                    s.op_cpu_hint[op_key] = max(prev, float(r.cpu))
        else:
            # Unknown failure: best effort to stop infinite retries if we can identify the pipeline.
            pid = getattr(r, "pipeline_id", None)
            if pid is not None:
                s.dead_pipelines.add(pid)

    suspensions = []
    assignments = []

    # Fill pools with prioritized work, limiting batch when high-priority is waiting.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool

        preferred = _pool_priority_preferences(pool_id)

        # Keep assigning while headroom remains.
        while avail_cpu > 0.01 and avail_ram > 0.01:
            p, op_list = _pop_next_runnable_pipeline(preferred)
            if not p:
                break

            # Apply soft reservations before placing batch work.
            eff_cpu, eff_ram = _soft_reserve_headroom(p.priority, avail_cpu, avail_ram, pool)
            if p.priority == Priority.BATCH_PIPELINE and (eff_cpu <= 0.01 or eff_ram <= 0.01):
                # Put batch pipeline back and stop on this pool for now; allow other pools to work.
                _enqueue_pipeline(p)
                break

            op = op_list[0]
            cpu, ram = _size_for(p.priority, pool, op, p.pipeline_id, eff_cpu, eff_ram)

            if cpu <= 0.01 or ram <= 0.01:
                # Can't fit sensibly now; rotate pipeline back and stop trying this pool this tick.
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

            # Decrement actual local headroom (not eff_*) since we placed a container.
            avail_cpu -= cpu
            avail_ram -= ram

            # Round-robin fairness within priority: re-enqueue the pipeline.
            _enqueue_pipeline(p)

    return suspensions, assignments
