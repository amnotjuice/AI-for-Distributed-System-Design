# policy_key: scheduler_iter_best_rich_008
# context: rich
# model: gpt-5.2-2025-12-11
# llm_cost: 0.048595
# generation_seconds: 36.46
# generated_at: 2026-04-12T00:25:26.290730
@register_scheduler_init(key="scheduler_iter_best_rich_008")
def scheduler_iter_best_rich_008_init(s):
    """Priority-aware, latency-optimized scheduler (incremental, simulation-friendly).

    Improvements over prior priority FIFO:
      - Two-tier admission per pool: always try QUERY/INTERACTIVE first, then BATCH.
      - Soft reservations: keep CPU/RAM headroom for high-priority work; throttle batch when needed.
      - Adaptive per-operator sizing from feedback:
          * On OOM: increase RAM multiplier (exp backoff).
          * On timeout: increase CPU multiplier (gentle backoff).
          * On success: record observed RAM/CPU and use as a sizing hint (EMA).
      - Avoid chronic over-allocation: RAM baseline is smaller; we rely on OOM feedback to right-size.
    """
    from collections import deque

    s.q_query = deque()
    s.q_interactive = deque()
    s.q_batch = deque()

    # Pipelines that had non-retriable failures (best-effort; depends on result fields).
    s.dead_pipelines = set()

    # Adaptive multipliers per (pipeline_id, op_id-ish)
    s.op_ram_mult = {}
    s.op_cpu_mult = {}

    # Observed successful allocations (EMA) per op.
    s.op_ram_ema = {}
    s.op_cpu_ema = {}

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


@register_scheduler(key="scheduler_iter_best_rich_008")
def scheduler_iter_best_rich_008_scheduler(s, results, pipelines):
    from collections import deque

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

    def _has_high_pri_waiting() -> bool:
        return (len(s.q_query) > 0) or (len(s.q_interactive) > 0)

    def _pick_next_op(q: deque):
        # Round-robin within the queue: pop-left, if not runnable push-right.
        n = len(q)
        for _ in range(n):
            p = q.popleft()
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

    def _ema_update(prev, val, alpha=0.3):
        if prev is None:
            return float(val)
        return (1.0 - alpha) * float(prev) + alpha * float(val)

    # --- ingest new pipelines ---
    for p in pipelines:
        _enqueue(p)

    if not pipelines and not results:
        return [], []

    # --- incorporate feedback from execution results ---
    for r in results:
        # Update EMAs on success to reduce chronic over-allocation.
        if not r.failed():
            # Best-effort: if ops list is present, update for each op.
            for op in (r.ops or []):
                # If pipeline_id exists on result use it; otherwise the key will be weaker but still useful.
                pid = getattr(r, "pipeline_id", None)
                k = s._op_key(op, pid)
                s.op_ram_ema[k] = _ema_update(s.op_ram_ema.get(k), getattr(r, "ram", 0.0))
                s.op_cpu_ema[k] = _ema_update(s.op_cpu_ema.get(k), getattr(r, "cpu", 0.0))
            continue

        # Failure handling
        pid = getattr(r, "pipeline_id", None)
        err = getattr(r, "error", None)

        if _is_oom_error(err):
            for op in (r.ops or []):
                k = s._op_key(op, pid)
                cur = s.op_ram_mult.get(k, 1.0)
                nxt = cur * 2.0
                if nxt > 32.0:
                    nxt = 32.0
                s.op_ram_mult[k] = nxt
        elif _is_timeout_error(err):
            for op in (r.ops or []):
                k = s._op_key(op, pid)
                cur = s.op_cpu_mult.get(k, 1.0)
                nxt = cur * 1.5
                if nxt > 8.0:
                    nxt = 8.0
                s.op_cpu_mult[k] = nxt
        else:
            # Unknown/non-retriable: if we can identify the pipeline, stop trying it.
            if pid is not None:
                s.dead_pipelines.add(pid)

    suspensions = []
    assignments = []

    def _size(priority, pool, op, pipeline_id, avail_cpu, avail_ram, reserve_cpu, reserve_ram):
        # Priority caps: protect latency while preventing one container from monopolizing a pool.
        max_cpu = pool.max_cpu_pool
        max_ram = pool.max_ram_pool

        if priority == Priority.QUERY:
            cpu_cap = min(6.0, max_cpu * 0.45)
            ram_cap = max_ram * 0.50
            base_cpu = 2.0
            base_ram_frac = 0.06
        elif priority == Priority.INTERACTIVE:
            cpu_cap = min(8.0, max_cpu * 0.55)
            ram_cap = max_ram * 0.65
            base_cpu = 2.0
            base_ram_frac = 0.10
        else:  # batch
            cpu_cap = min(12.0, max_cpu * 0.70)
            ram_cap = max_ram * 0.85
            base_cpu = 3.0
            base_ram_frac = 0.16

        k = s._op_key(op, pipeline_id)
        cpu_mult = s.op_cpu_mult.get(k, 1.0)
        ram_mult = s.op_ram_mult.get(k, 1.0)

        # Use observed successful allocations as hints (slightly padded).
        hint_cpu = s.op_cpu_ema.get(k, None)
        hint_ram = s.op_ram_ema.get(k, None)

        cpu_target = base_cpu
        if hint_cpu is not None and hint_cpu > 0.01:
            cpu_target = max(cpu_target, float(hint_cpu) * 1.10)
        cpu_target *= cpu_mult
        cpu = min(cpu_target, cpu_cap)

        # For RAM, start small to avoid over-allocation; rely on OOM backoff to adjust.
        ram_target = max_ram * base_ram_frac
        if hint_ram is not None and hint_ram > 0.01:
            ram_target = max(ram_target, float(hint_ram) * 1.15)
        ram_target *= ram_mult
        ram = min(ram_target, ram_cap)

        # Enforce per-pool reserve (soft): never consume below reserve in this assignment.
        cpu_budget = max(0.0, avail_cpu - reserve_cpu)
        ram_budget = max(0.0, avail_ram - reserve_ram)
        cpu = min(cpu, cpu_budget)
        ram = min(ram, ram_budget)

        # Avoid tiny allocations that tend to be useless and increase scheduling churn.
        if cpu < 0.75:
            cpu = 0.0
        if ram <= 0.0:
            ram = 0.0

        return cpu, ram

    def _schedule_queue_into_pool(q: deque, pool_id: int, allow_priority, avail_cpu, avail_ram, reserve_cpu, reserve_ram):
        pool = s.executor.pools[pool_id]
        local_assignments = []
        spins = 0

        while avail_cpu > (reserve_cpu + 0.75) and avail_ram > (reserve_ram + 0.01) and len(q) > 0:
            p, op_list = _pick_next_op(q)
            if not p:
                break
            if p.priority != allow_priority:
                # Put it back; this helper is for a single queue/priority.
                _enqueue(p)
                spins += 1
                if spins > 8:
                    break
                continue

            op = op_list[0]
            cpu, ram = _size(p.priority, pool, op, p.pipeline_id, avail_cpu, avail_ram, reserve_cpu, reserve_ram)
            if cpu <= 0.0 or ram <= 0.0:
                # Not enough headroom under reservation; requeue and stop for this pool.
                _enqueue(p)
                break

            local_assignments.append(
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
            # Rotate pipeline for fairness and to allow other pipelines to progress.
            _enqueue(p)

        return local_assignments, avail_cpu, avail_ram

    # --- main scheduling: per-pool fill, always favor high priority; throttle batch via reservations ---
    # Soft reservation policy:
    #   - If any high priority is waiting globally, keep a larger reserve in each pool to absorb bursts.
    #   - Batch can still run, but must fit in remaining headroom above reserve.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool
        if avail_cpu <= 0.01 or avail_ram <= 0.01:
            continue

        if _has_high_pri_waiting():
            reserve_cpu = max(1.0, pool.max_cpu_pool * 0.20)
            reserve_ram = pool.max_ram_pool * 0.15
        else:
            reserve_cpu = 0.0
            reserve_ram = 0.0

        # 1) QUERY first
        a, avail_cpu, avail_ram = _schedule_queue_into_pool(
            s.q_query, pool_id, Priority.QUERY, avail_cpu, avail_ram, reserve_cpu=0.0, reserve_ram=0.0
        )
        assignments.extend(a)

        # 2) INTERACTIVE next
        a, avail_cpu, avail_ram = _schedule_queue_into_pool(
            s.q_interactive, pool_id, Priority.INTERACTIVE, avail_cpu, avail_ram, reserve_cpu=0.0, reserve_ram=0.0
        )
        assignments.extend(a)

        # 3) BATCH last, under reservation if high priority is waiting
        a, avail_cpu, avail_ram = _schedule_queue_into_pool(
            s.q_batch, pool_id, Priority.BATCH_PIPELINE, avail_cpu, avail_ram, reserve_cpu=reserve_cpu, reserve_ram=reserve_ram
        )
        assignments.extend(a)

    return suspensions, assignments
