# policy_key: scheduler_iter_best_rich_020
# context: rich
# model: gpt-5.2-2025-12-11
# llm_cost: 0.060831
# generation_seconds: 42.67
# generated_at: 2026-04-12T00:34:00.949040
@register_scheduler_init(key="scheduler_iter_best_rich_020")
def scheduler_iter_best_rich_020_init(s):
    """Incremental latency-focused improvement over naive FIFO with priority + adaptive sizing.

    Main ideas:
      - Strict priority queues (QUERY > INTERACTIVE > BATCH) with round-robin within each class.
      - Two-phase per-pool fill: reserve a slice of each pool for BATCH (when batch backlog exists)
        to reduce starvation/timeouts while still protecting high-priority weighted latency.
      - Adaptive per-operator RAM and CPU recommendations learned from ExecutionResult:
          * OOM -> increase next RAM for that op
          * timeout -> increase next CPU for that op
          * success -> gently decrease over-provisioned RAM/CPU for that op (to improve concurrency)
      - Avoid pool monopolization with per-priority caps and "sensible minimum" allocations.

    Intentionally still no preemption (suspensions) because the provided interface does not expose
    running-container inventories safely; this version focuses on better packing + fewer failures.
    """
    # Per-priority pipeline queues
    s.q_query = []
    s.q_interactive = []
    s.q_batch = []

    # Drop pipelines that hit non-retriable errors repeatedly (best-effort; may be unused if no pid on results)
    s.dead_pipelines = set()

    # Operator-level learned recommendations: op_key -> value
    s.op_ram_rec = {}   # recommended RAM for next try
    s.op_cpu_rec = {}   # recommended CPU for next try

    # Map op object identity -> op_key (to connect results back to assignments)
    s.op_obj_to_key = {}

    def _get_op_id(op):
        oid = getattr(op, "op_id", None)
        if oid is None:
            oid = getattr(op, "operator_id", None)
        if oid is None:
            oid = id(op)
        return oid

    def _op_key(pipeline_id, op):
        return (pipeline_id, _get_op_id(op))

    s._get_op_id = _get_op_id
    s._op_key = _op_key


@register_scheduler(key="scheduler_iter_best_rich_020")
def scheduler_iter_best_rich_020_scheduler(s, results, pipelines):
    """Priority-aware, adaptive-sizing scheduler targeting lower weighted latency and fewer timeouts."""
    # ---------- helpers ----------
    def _is_oom_error(err) -> bool:
        if err is None:
            return False
        msg = str(err).lower()
        return ("oom" in msg) or ("out of memory" in msg)

    def _is_timeout_error(err) -> bool:
        if err is None:
            return False
        msg = str(err).lower()
        return ("timeout" in msg) or ("timed out" in msg) or ("deadline" in msg)

    def _enqueue(p):
        if p.pipeline_id in s.dead_pipelines:
            return
        if p.priority == Priority.QUERY:
            s.q_query.append(p)
        elif p.priority == Priority.INTERACTIVE:
            s.q_interactive.append(p)
        else:
            s.q_batch.append(p)

    def _have_batch_backlog() -> bool:
        # "Backlog" = any batch pipelines waiting (even if not runnable yet).
        return len(s.q_batch) > 0

    def _select_next_from_queue(q):
        """Round-robin scan of a single queue for the next runnable op (parents complete)."""
        if not q:
            return None, None
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
            # Not runnable yet; rotate
            q.append(p)
        return None, None

    def _select_next_runnable(allow_batch: bool):
        """Strict priority selection across queues, with optional batch exclusion."""
        p, ops = _select_next_from_queue(s.q_query)
        if p:
            return p, ops
        p, ops = _select_next_from_queue(s.q_interactive)
        if p:
            return p, ops
        if allow_batch:
            p, ops = _select_next_from_queue(s.q_batch)
            if p:
                return p, ops
        return None, None

    def _default_caps(priority, pool):
        # Per-priority caps to avoid single container monopolizing a pool.
        # Tuned to reduce timeouts (more CPU for interactive/batch than prior iteration),
        # while preventing RAM overallocation (keep defaults modest and rely on learning).
        max_cpu = pool.max_cpu_pool
        max_ram = pool.max_ram_pool

        if priority == Priority.QUERY:
            cpu_cap = min(4.0, max_cpu * 0.35)
            ram_default = max_ram * 0.08
            ram_cap = max_ram * 0.25
        elif priority == Priority.INTERACTIVE:
            cpu_cap = min(8.0, max_cpu * 0.55)
            ram_default = max_ram * 0.12
            ram_cap = max_ram * 0.35
        else:  # BATCH_PIPELINE
            cpu_cap = min(12.0, max_cpu * 0.75)
            ram_default = max_ram * 0.10
            ram_cap = max_ram * 0.40

        # Sensible minimums to avoid pathological tiny allocations that extend runtimes.
        cpu_min = 1.0
        ram_min = max_ram * 0.02

        return cpu_min, cpu_cap, ram_min, ram_default, ram_cap

    def _size_op(p, op, pool, avail_cpu, avail_ram):
        """Choose CPU/RAM using learned per-op recommendations with safe caps and headroom."""
        cpu_min, cpu_cap, ram_min, ram_default, ram_cap = _default_caps(p.priority, pool)
        key = s._op_key(p.pipeline_id, op)

        # Start from defaults; adjust with learned recommendations if present.
        cpu = s.op_cpu_rec.get(key, ram_default * 0.0)  # placeholder; overwritten below
        ram = s.op_ram_rec.get(key, ram_default)

        # CPU: if we have a learned recommendation, use it; otherwise use a small priority-based default.
        if key in s.op_cpu_rec:
            cpu = s.op_cpu_rec[key]
        else:
            # Default CPU is modest but not tiny; interactive/batch get more to reduce timeouts.
            if p.priority == Priority.QUERY:
                cpu = 2.0
            elif p.priority == Priority.INTERACTIVE:
                cpu = 3.0
            else:
                cpu = 3.0

        # Clamp to caps and availability.
        if cpu < cpu_min:
            cpu = cpu_min
        if cpu > cpu_cap:
            cpu = cpu_cap
        if cpu > avail_cpu:
            cpu = avail_cpu

        if ram < ram_min:
            ram = ram_min
        if ram > ram_cap:
            ram = ram_cap
        if ram > avail_ram:
            ram = avail_ram

        return cpu, ram, key

    def _learn_from_result(r):
        """Update per-op recommendations based on success/failure."""
        # Identify ops; map to keys using op object identity if possible.
        ops = r.ops or []
        for op in ops:
            k = s.op_obj_to_key.get(id(op), None)

            # If we cannot map back, we cannot reliably learn per-op; skip.
            if k is None:
                continue

            # On success: gently reduce to improve concurrency (since allocated>>consumed in stats).
            if not r.failed():
                # RAM: decay towards a slightly smaller value, but keep a floor.
                prev_ram = s.op_ram_rec.get(k, r.ram)
                # Keep at least 85% of last successful allocation; reduce slowly from previous.
                target_ram = max(r.ram * 0.85, prev_ram * 0.90)
                # Don't increase on success.
                if target_ram < prev_ram:
                    s.op_ram_rec[k] = target_ram

                # CPU: also gently decay; too much CPU can reduce concurrency.
                prev_cpu = s.op_cpu_rec.get(k, r.cpu)
                target_cpu = max(r.cpu * 0.85, prev_cpu * 0.92)
                if target_cpu < prev_cpu:
                    s.op_cpu_rec[k] = target_cpu
                continue

            # Failures: treat OOM and timeout differently.
            if _is_oom_error(r.error):
                prev_ram = s.op_ram_rec.get(k, r.ram)
                # Increase aggressively; OOM implies hard minimum.
                nxt = max(prev_ram, r.ram) * 2.0
                # Hard cap handled later in sizing.
                s.op_ram_rec[k] = nxt
            elif _is_timeout_error(r.error):
                prev_cpu = s.op_cpu_rec.get(k, r.cpu)
                # Increase CPU to reduce runtime / timeouts, but not explosively.
                nxt = max(prev_cpu, r.cpu) * 1.5
                s.op_cpu_rec[k] = nxt
            else:
                # Unknown/non-retriable: best effort mark pipeline dead if pid present.
                pid = getattr(r, "pipeline_id", None)
                if pid is not None:
                    s.dead_pipelines.add(pid)

    # ---------- ingest new pipelines ----------
    for p in pipelines:
        _enqueue(p)

    # Early exit if nothing changed.
    if not pipelines and not results:
        return [], []

    # ---------- learn from previous tick results ----------
    for r in results:
        _learn_from_result(r)

    suspensions = []
    assignments = []

    # ---------- scheduling ----------
    # Reserve some capacity for batch when batch backlog exists to reduce starvation/timeouts.
    # This is per-pool and only applies if batch queue non-empty.
    reserve_batch_cpu_frac = 0.20
    reserve_batch_ram_frac = 0.20

    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool
        if avail_cpu <= 0.01 or avail_ram <= 0.01:
            continue

        batch_backlog = _have_batch_backlog()
        cpu_reserve = pool.max_cpu_pool * reserve_batch_cpu_frac if batch_backlog else 0.0
        ram_reserve = pool.max_ram_pool * reserve_batch_ram_frac if batch_backlog else 0.0

        # Phase 1: prioritize QUERY/INTERACTIVE, but keep reserves for batch if needed.
        while avail_cpu > 0.01 and avail_ram > 0.01:
            # If we have batch backlog, stop high-priority filling once we hit the reserve.
            if batch_backlog and (avail_cpu <= cpu_reserve or avail_ram <= ram_reserve):
                break

            p, op_list = _select_next_runnable(allow_batch=False)
            if not p:
                break

            op = op_list[0]
            cpu, ram, key = _size_op(p, op, pool, avail_cpu, avail_ram)

            # If we can't allocate a meaningful amount, rotate and stop this pool for now.
            if cpu <= 0.01 or ram <= 0.01:
                _enqueue(p)
                break

            # Record mapping so results can be attributed back to this operator.
            s.op_obj_to_key[id(op)] = key

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
            _enqueue(p)  # fairness within priority class

        # Phase 2: fill remaining capacity with batch (or any work if no batch runnable).
        while avail_cpu > 0.01 and avail_ram > 0.01:
            p, op_list = _select_next_runnable(allow_batch=True)
            if not p:
                break

            op = op_list[0]
            cpu, ram, key = _size_op(p, op, pool, avail_cpu, avail_ram)

            if cpu <= 0.01 or ram <= 0.01:
                _enqueue(p)
                break

            s.op_obj_to_key[id(op)] = key

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
            _enqueue(p)

    return suspensions, assignments
