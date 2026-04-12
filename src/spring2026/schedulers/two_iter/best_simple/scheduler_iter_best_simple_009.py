# policy_key: scheduler_iter_best_simple_009
# context: simple
# model: gpt-5.2-2025-12-11
# llm_cost: 0.041409
# generation_seconds: 33.65
# generated_at: 2026-04-11T22:43:49.391631
@register_scheduler_init(key="scheduler_iter_best_simple_009")
def scheduler_iter_best_simple_009_init(s):
    """Priority- & reservation-aware scheduler to reduce weighted latency.

    Incremental improvements over the prior priority-queue version:
      1) Strict priority ordering (QUERY > INTERACTIVE > BATCH) with round-robin within each class.
      2) Pool headroom reservations for high-priority work:
         - When QUERY/INTERACTIVE are waiting, we avoid consuming the last reserved CPU/RAM with BATCH.
      3) More aggressive sizing for QUERY/INTERACTIVE (to finish faster), more conservative for BATCH.
      4) OOM-aware RAM backoff keyed by (pipeline_id, operator_id-ish) so retries ramp quickly.

    Intentionally still simple:
      - No preemption (insufficient reliable visibility of running containers in the provided interface).
      - No runtime prediction; just priority + reservations + sizing heuristics.
    """
    s.q_query = []
    s.q_interactive = []
    s.q_batch = []

    # Track pipelines that encountered non-OOM failures (best-effort; pipeline itself may also encode failures).
    s.dead_pipelines = set()

    # Adaptive per-operator RAM multiplier for OOM retries: (pipeline_id, op_id) -> multiplier.
    s.op_ram_mult = {}

    # Stable operator key helper.
    def _op_key(pipeline_id, op):
        oid = getattr(op, "op_id", None)
        if oid is None:
            oid = getattr(op, "operator_id", None)
        if oid is None:
            oid = getattr(op, "name", None)
        if oid is None:
            oid = id(op)
        return (pipeline_id, oid)

    s._op_key = _op_key


@register_scheduler(key="scheduler_iter_best_simple_009")
def scheduler_iter_best_simple_009_scheduler(s, results, pipelines):
    """Priority-first with high-priority reservations and OOM RAM backoff."""
    # -------- helpers --------
    def _is_oom_error(err) -> bool:
        if err is None:
            return False
        msg = str(err).lower()
        return ("oom" in msg) or ("out of memory" in msg) or ("killed" in msg and "memory" in msg)

    def _enqueue(p):
        if p.pipeline_id in s.dead_pipelines:
            return
        if p.priority == Priority.QUERY:
            s.q_query.append(p)
        elif p.priority == Priority.INTERACTIVE:
            s.q_interactive.append(p)
        else:
            s.q_batch.append(p)

    def _hp_waiting() -> bool:
        return bool(s.q_query) or bool(s.q_interactive)

    def _pop_rr(q):
        """Round-robin: pop from front, caller may re-enqueue to back."""
        if not q:
            return None
        return q.pop(0)

    def _next_runnable_from_queue(q):
        """Return (pipeline, [op]) or (None, None). Rotates non-runnable pipelines to back."""
        n = len(q)
        for _ in range(n):
            p = _pop_rr(q)
            if p is None:
                break
            if p.pipeline_id in s.dead_pipelines:
                continue
            st = p.runtime_status()
            if st.is_pipeline_successful():
                continue
            op_list = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
            if op_list:
                return p, op_list
            # Not runnable yet; rotate.
            q.append(p)
        return None, None

    def _pick_next_pipeline():
        """Strict priority: QUERY > INTERACTIVE > BATCH."""
        p, ops = _next_runnable_from_queue(s.q_query)
        if p:
            return p, ops
        p, ops = _next_runnable_from_queue(s.q_interactive)
        if p:
            return p, ops
        p, ops = _next_runnable_from_queue(s.q_batch)
        if p:
            return p, ops
        return None, None

    def _size(priority, pool, pipeline_id, op, avail_cpu, avail_ram):
        # Base caps by priority (aim: lower latency for high-priority; avoid batch monopolization).
        max_cpu = pool.max_cpu_pool
        max_ram = pool.max_ram_pool

        if priority == Priority.QUERY:
            cpu_cap = min(max_cpu * 0.75, 12.0)
            ram_frac = 0.30
        elif priority == Priority.INTERACTIVE:
            cpu_cap = min(max_cpu * 0.60, 10.0)
            ram_frac = 0.25
        else:  # BATCH_PIPELINE
            cpu_cap = min(max_cpu * 0.40, 8.0)
            ram_frac = 0.20

        cpu = min(avail_cpu, cpu_cap)
        if cpu <= 0:
            return 0.0, 0.0

        # OOM backoff multiplier per operator.
        mult = s.op_ram_mult.get(s._op_key(pipeline_id, op), 1.0)
        # Keep RAM conservative for batch; let backoff handle true needs.
        ram = min(avail_ram, max_ram, (max_ram * ram_frac) * mult)
        if ram <= 0:
            return 0.0, 0.0

        return float(cpu), float(ram)

    # -------- ingest new pipelines --------
    for p in pipelines:
        _enqueue(p)

    # If nothing changed, do nothing.
    if not pipelines and not results:
        return [], []

    # -------- process results: OOM backoff and dead pipeline marking --------
    for r in results:
        if not r.failed():
            continue

        # Best-effort: bump RAM multipliers for all ops in this failed result.
        if _is_oom_error(r.error):
            for op in (r.ops or []):
                # ExecutionResult does not guarantee pipeline_id access; infer from op if present, else None.
                pid = getattr(r, "pipeline_id", None)
                if pid is None:
                    pid = getattr(op, "pipeline_id", None)
                key = s._op_key(pid, op)
                cur = s.op_ram_mult.get(key, 1.0)
                nxt = cur * 2.0
                if nxt > 32.0:
                    nxt = 32.0
                s.op_ram_mult[key] = nxt
        else:
            # Non-OOM failures: stop retrying if we can identify the pipeline.
            pid = getattr(r, "pipeline_id", None)
            if pid is not None:
                s.dead_pipelines.add(pid)

    suspensions = []
    assignments = []

    # -------- scheduling: per-pool fill with reservations for high priority --------
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]

        # Local view of headroom for this tick.
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)

        if avail_cpu <= 0.01 or avail_ram <= 0.01:
            continue

        # Reservations: keep some capacity available when HP is waiting.
        # (These are fractions of total pool capacity, not of currently available headroom.)
        reserve_cpu = float(pool.max_cpu_pool) * 0.35
        reserve_ram = float(pool.max_ram_pool) * 0.25

        # Fill the pool with as many ops as we can.
        # We always try to schedule HP first; batch is admission-controlled to protect reservations.
        while avail_cpu > 0.01 and avail_ram > 0.01:
            p, op_list = _pick_next_pipeline()
            if not p:
                break

            op = op_list[0]

            # If this is a batch op and high-priority is waiting, do not consume reserved headroom.
            if p.priority == Priority.BATCH_PIPELINE and _hp_waiting():
                # If we're already at/below reserved headroom, stop scheduling batch on this pool now.
                if avail_cpu <= reserve_cpu + 0.01 or avail_ram <= reserve_ram + 0.01:
                    # Put it back and break; other pools may still have room, and HP will be scheduled next tick.
                    _enqueue(p)
                    break

            cpu, ram = _size(p.priority, pool, p.pipeline_id, op, avail_cpu, avail_ram)

            # If we can't allocate meaningful resources, requeue and stop on this pool to avoid spinning.
            if cpu <= 0.01 or ram <= 0.01:
                _enqueue(p)
                break

            # Additional guard: if batch would dip into reservation, shrink it (rather than stopping entirely).
            if p.priority == Priority.BATCH_PIPELINE and _hp_waiting():
                target_cpu_floor = max(0.0, avail_cpu - reserve_cpu)
                target_ram_floor = max(0.0, avail_ram - reserve_ram)
                cpu = min(cpu, target_cpu_floor) if target_cpu_floor > 0 else 0.0
                ram = min(ram, target_ram_floor) if target_ram_floor > 0 else 0.0
                if cpu <= 0.01 or ram <= 0.01:
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

            # Round-robin fairness within each priority: re-enqueue pipeline after scheduling one op.
            _enqueue(p)

    return suspensions, assignments
