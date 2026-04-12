# policy_key: scheduler_iter_best_simple_014
# context: simple
# model: gpt-5.2-2025-12-11
# llm_cost: 0.039561
# generation_seconds: 32.87
# generated_at: 2026-04-11T22:46:50.258992
@register_scheduler_init(key="scheduler_iter_best_simple_014")
def scheduler_iter_best_simple_014_init(s):
    """Priority-aware scheduler focused on weighted latency.

    Incremental improvements over the prior version:
      - Hard reservations: keep a CPU/RAM reserve for QUERY+INTERACTIVE whenever they are waiting.
        This prevents BATCH from consuming the last headroom and inflating tail latency.
      - More aggressive sizing for high priority: give QUERY/INTERACTIVE more CPU (and enough RAM)
        to finish quickly; still cap to avoid single-op monopolization.
      - Pool selection: schedule high-priority work into the pool with the most headroom first.
      - Basic starvation avoidance: allow BATCH to bypass reservations after it has waited long enough.
      - OOM-aware retry: exponential RAM backoff per operator key.

    Deliberately not included yet:
      - Preemption (needs reliable visibility into currently running containers).
      - True SRPT (requires duration estimates not shown in the minimal interface).
    """
    s.q_query = []
    s.q_interactive = []
    s.q_batch = []

    s.dead_pipelines = set()

    # op_key -> RAM multiplier (1,2,4,...)
    s.op_ram_mult = {}

    # pipeline_id -> tick when first seen (for aging)
    s.arrival_tick = {}

    # logical tick counter
    s.tick = 0

    def _op_key(op, pipeline_id):
        oid = getattr(op, "op_id", None)
        if oid is None:
            oid = getattr(op, "operator_id", None)
        if oid is None:
            oid = id(op)
        return (pipeline_id, oid)

    s._op_key = _op_key


@register_scheduler(key="scheduler_iter_best_simple_014")
def scheduler_iter_best_simple_014_scheduler(s, results, pipelines):
    """Priority-first scheduling with high-priority reservations and OOM-aware RAM backoff."""
    s.tick += 1

    # ---- helpers ----
    def _is_oom_error(err) -> bool:
        if err is None:
            return False
        msg = str(err).lower()
        return ("oom" in msg) or ("out of memory" in msg) or ("killed" in msg and "memory" in msg)

    def _enqueue(p):
        if p.pipeline_id in s.dead_pipelines:
            return
        if p.pipeline_id not in s.arrival_tick:
            s.arrival_tick[p.pipeline_id] = s.tick
        if p.priority == Priority.QUERY:
            s.q_query.append(p)
        elif p.priority == Priority.INTERACTIVE:
            s.q_interactive.append(p)
        else:
            s.q_batch.append(p)

    def _has_high_waiting() -> bool:
        return bool(s.q_query) or bool(s.q_interactive)

    def _queue_for_priority(priority):
        if priority == Priority.QUERY:
            return s.q_query
        if priority == Priority.INTERACTIVE:
            return s.q_interactive
        return s.q_batch

    def _pop_next_runnable_from_queue(q):
        # Round-robin scan for a runnable op.
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
            q.append(p)
        return None, None

    def _pop_next_runnable(priority_order):
        for pr in priority_order:
            p, ops = _pop_next_runnable_from_queue(_queue_for_priority(pr))
            if p:
                return p, ops
        return None, None

    def _age(p) -> int:
        return s.tick - s.arrival_tick.get(p.pipeline_id, s.tick)

    def _pool_order_by_headroom():
        # Prefer the pool with the most CPU headroom; tie-breaker by RAM headroom.
        ids = list(range(s.executor.num_pools))
        def key_fn(i):
            pool = s.executor.pools[i]
            return (pool.avail_cpu_pool, pool.avail_ram_pool)
        ids.sort(key=key_fn, reverse=True)
        return ids

    def _size_for(priority, pool, op, pipeline_id, avail_cpu, avail_ram, high_backlog: bool):
        # More aggressive CPU for high priority to reduce latency.
        max_cpu = pool.max_cpu_pool
        max_ram = pool.max_ram_pool

        # CPU caps: allow higher for QUERY/INTERACTIVE than the previous iteration.
        if priority == Priority.QUERY:
            cpu_cap = min(8.0, max_cpu * (0.80 if high_backlog else 0.65))
            ram_target = max_ram * 0.20
        elif priority == Priority.INTERACTIVE:
            cpu_cap = min(10.0, max_cpu * (0.85 if high_backlog else 0.70))
            ram_target = max_ram * 0.28
        else:
            cpu_cap = min(12.0, max_cpu * 0.60)
            ram_target = max_ram * 0.40

        cpu = min(avail_cpu, cpu_cap)

        opk = s._op_key(op, pipeline_id)
        mult = s.op_ram_mult.get(opk, 1.0)

        ram = ram_target * mult
        ram = min(ram, avail_ram, max_ram)

        return cpu, ram

    # ---- ingest new pipelines ----
    for p in pipelines:
        _enqueue(p)

    if not pipelines and not results:
        return [], []

    # ---- process results for OOM backoff ----
    for r in results:
        if not r.failed():
            continue

        if _is_oom_error(r.error):
            # Increase RAM multiplier for failed ops (best-effort).
            pid = getattr(r, "pipeline_id", None)
            for op in (r.ops or []):
                opk = s._op_key(op, pid)
                # If pid isn't available, still back off by operator identity for this run.
                if opk[0] is None:
                    opk = (None, opk[1])
                cur = s.op_ram_mult.get(opk, 1.0)
                nxt = cur * 2.0
                if nxt > 32.0:
                    nxt = 32.0
                s.op_ram_mult[opk] = nxt
        else:
            # Non-OOM failures: if we can identify pipeline, stop retrying it.
            pid = getattr(r, "pipeline_id", None)
            if pid is not None:
                s.dead_pipelines.add(pid)

    suspensions = []
    assignments = []

    # ---- reservation policy ----
    # If high-priority work is waiting, keep some headroom in every pool.
    # This reduces queueing delay for weighted latency.
    RESERVE_CPU_FRAC = 0.35
    RESERVE_RAM_FRAC = 0.35

    # Starvation avoidance: allow old batch to ignore reservations.
    BATCH_MAX_WAIT_TICKS = 25

    # ---- schedule: prioritize high priority, best pool headroom first ----
    pool_order = _pool_order_by_headroom()
    high_waiting = _has_high_waiting()
    high_backlog = (len(s.q_query) + len(s.q_interactive)) >= 3

    for pool_id in pool_order:
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool
        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # Compute reservation thresholds only when high-priority work is waiting.
        reserve_cpu = pool.max_cpu_pool * RESERVE_CPU_FRAC if high_waiting else 0.0
        reserve_ram = pool.max_ram_pool * RESERVE_RAM_FRAC if high_waiting else 0.0

        # Keep assigning while we have meaningful headroom.
        while avail_cpu > 0.01 and avail_ram > 0.01:
            # When high-priority exists, strictly prioritize it.
            if high_waiting:
                priority_order = [Priority.QUERY, Priority.INTERACTIVE]
            else:
                priority_order = [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]

            p, op_list = _pop_next_runnable(priority_order)

            if not p:
                # If no runnable high-priority ops, allow batch (subject to reservations).
                if high_waiting:
                    # Try batch only if we have non-reserved slack or a starving batch exists.
                    # Find a runnable batch pipeline without permanently consuming reserved capacity.
                    # We do a bounded scan by reusing the queue's round-robin behavior.
                    starving_allowed = False
                    if s.q_batch:
                        # Peek for starvation: if any batch waited long enough, allow.
                        for bp in s.q_batch[: min(len(s.q_batch), 16)]:
                            if _age(bp) >= BATCH_MAX_WAIT_TICKS:
                                starving_allowed = True
                                break

                    slack_ok = (avail_cpu - reserve_cpu) > 0.25 and (avail_ram - reserve_ram) > 0.25
                    if not (slack_ok or starving_allowed):
                        break

                    p, op_list = _pop_next_runnable([Priority.BATCH_PIPELINE])
                    if not p:
                        break
                else:
                    break

            # Enforce reservations: do not schedule batch that dips into reserve when high is waiting,
            # unless the batch has been waiting too long.
            if high_waiting and p.priority == Priority.BATCH_PIPELINE:
                if _age(p) < BATCH_MAX_WAIT_TICKS:
                    # Put it back and stop for this pool; leave headroom for high priority.
                    _enqueue(p)
                    break

            op = op_list[0]
            cpu, ram = _size_for(p.priority, pool, op, p.pipeline_id, avail_cpu, avail_ram, high_backlog)

            if cpu <= 0.01 or ram <= 0.01:
                _enqueue(p)
                break

            # If high waiting and this is batch, ensure we don't consume reserved resources.
            if high_waiting and p.priority == Priority.BATCH_PIPELINE and _age(p) < BATCH_MAX_WAIT_TICKS:
                if (avail_cpu - cpu) < reserve_cpu or (avail_ram - ram) < reserve_ram:
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

            # Re-enqueue pipeline for round-robin fairness within its priority.
            _enqueue(p)

        # Update whether high-priority work is still waiting before moving to next pool.
        high_waiting = _has_high_waiting()
        high_backlog = (len(s.q_query) + len(s.q_interactive)) >= 3

    return suspensions, assignments
