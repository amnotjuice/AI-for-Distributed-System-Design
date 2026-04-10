# policy_key: scheduler_est_020
# reasoning_effort: high
# model: gpt-5.2-2025-12-11
# llm_cost: 0.072106
# generation_seconds: 75.67
# generated_at: 2026-03-31T18:58:41.178972
@register_scheduler_init(key="scheduler_est_020")
def scheduler_est_020_init(s):
    """Priority-aware, estimate-driven scheduler.

    Improvements over naive FIFO:
      1) Maintain separate queues per priority and always schedule higher priority first.
      2) Use op.estimate.mem_peak_gb to right-size RAM (with a safety factor).
      3) Keep a small reserved headroom per pool so BATCH work cannot consume all resources,
         reducing latency for newly arriving QUERY/INTERACTIVE work (without preemption).
      4) If an op fails with an OOM-like error, increase its RAM multiplier and retry.
    """
    # Separate FIFO queues per priority (simple, predictable behavior).
    s.q_query = []
    s.q_interactive = []
    s.q_batch = []

    # Per-operator RAM inflation factor to react to underestimation/OOMs.
    # Keyed by a stable-ish operator key derived from (pipeline_id, op_id/name/...).
    s.op_ram_mult = {}

    # Track pipelines that should not be scheduled again due to non-OOM failures.
    s.dead_pipeline_ids = set()

    # Clamp multipliers to avoid runaway RAM reservations.
    s.ram_mult_min = 1.15
    s.ram_mult_max = 8.0


@register_scheduler(key="scheduler_est_020")
def scheduler_est_020_scheduler(s, results, pipelines):
    """
    Scheduler tick:
      - Enqueue new pipelines by priority.
      - Process results to learn from OOMs (increase per-op RAM multiplier) and
        blacklist pipelines with non-OOM failures.
      - For each pool, greedily assign as many ops as fit:
          QUERY -> INTERACTIVE -> BATCH
        while protecting headroom by preventing BATCH from consuming reserved resources.
    """
    def _get_q(priority):
        if priority == Priority.QUERY:
            return s.q_query
        if priority == Priority.INTERACTIVE:
            return s.q_interactive
        return s.q_batch

    def _safe_float(x, default=0.0):
        try:
            if x is None:
                return default
            return float(x)
        except Exception:
            return default

    def _looks_like_oom(err) -> bool:
        if err is None:
            return False
        txt = str(err).lower()
        return ("oom" in txt) or ("out of memory" in txt) or ("cuda oom" in txt) or ("memoryerror" in txt)

    def _op_key(op):
        # Try to build a stable key that survives across scheduler ticks.
        pid = getattr(op, "pipeline_id", None)
        oid = (
            getattr(op, "op_id", None)
            or getattr(op, "operator_id", None)
            or getattr(op, "node_id", None)
            or getattr(op, "name", None)
            or getattr(op, "id", None)
        )
        if pid is not None and oid is not None:
            return f"{pid}:{oid}"
        if oid is not None:
            return str(oid)
        # Last resort; may include memory address but still helps within a run.
        return repr(op)

    def _mem_est_gb(op) -> float:
        est = getattr(op, "estimate", None)
        mem = getattr(est, "mem_peak_gb", None) if est is not None else None
        mem = _safe_float(mem, default=0.0)
        # Avoid zero-RAM assignments; keep a small baseline.
        return max(0.25, mem)

    def _cpu_quanta(priority, pool_max_cpu):
        # Small, safe increments: give more CPU to higher priority to reduce latency.
        pm = max(1.0, _safe_float(pool_max_cpu, default=1.0))
        if priority == Priority.QUERY:
            return max(1.0, min(4.0, 0.50 * pm))
        if priority == Priority.INTERACTIVE:
            return max(1.0, min(3.0, 0.40 * pm))
        # Batch: keep small so it doesn't crowd out new interactive arrivals.
        return max(1.0, min(2.0, 0.25 * pm))

    def _reserved_headroom(pool):
        # Reserve a small fraction so batch cannot fully exhaust a pool.
        # (No preemption here; this is the simplest latency protection mechanism.)
        res_cpu = max(1.0, 0.20 * _safe_float(pool.max_cpu_pool, default=0.0))
        res_ram = max(1.0, 0.20 * _safe_float(pool.max_ram_pool, default=0.0))
        return res_cpu, res_ram

    # Enqueue new pipelines by priority.
    for p in pipelines:
        _get_q(p.priority).append(p)

    # If nothing changed, do nothing.
    if not pipelines and not results:
        return [], []

    # Learn from results: OOM -> increase RAM multiplier; non-OOM failure -> mark pipeline dead.
    for r in results:
        if not hasattr(r, "failed") or not r.failed():
            continue

        is_oom = _looks_like_oom(getattr(r, "error", None))
        ops = getattr(r, "ops", None) or []
        for op in ops:
            k = _op_key(op)
            if is_oom:
                prev = _safe_float(s.op_ram_mult.get(k, s.ram_mult_min), default=s.ram_mult_min)
                # Gentle exponential backoff.
                s.op_ram_mult[k] = min(s.ram_mult_max, max(s.ram_mult_min, prev * 1.5))
            else:
                # Non-OOM failure: stop scheduling this pipeline to avoid endless retries.
                pid = getattr(op, "pipeline_id", None)
                if pid is not None:
                    s.dead_pipeline_ids.add(pid)

    suspensions = []  # No preemption in this incremental policy.
    assignments = []

    # For each pool, pack as many runnable ops as fit, prioritizing QUERY/INTERACTIVE.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = _safe_float(pool.avail_cpu_pool, default=0.0)
        avail_ram = _safe_float(pool.avail_ram_pool, default=0.0)
        if avail_cpu <= 0.0 or avail_ram <= 0.0:
            continue

        res_cpu, res_ram = _reserved_headroom(pool)

        # Greedy loop: always restart priority scan after a successful assignment.
        while True:
            made_assignment = False

            for prio in (Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH):
                q = _get_q(prio)
                if not q:
                    continue

                # For batch, enforce headroom reservation; for higher priorities, use all.
                cpu_cap = avail_cpu
                ram_cap = avail_ram
                if prio == Priority.BATCH:
                    cpu_cap = max(0.0, avail_cpu - res_cpu)
                    ram_cap = max(0.0, avail_ram - res_ram)

                if cpu_cap < 1.0 or ram_cap < 0.5:
                    continue

                # Scan each pipeline at most once for this priority in this pool iteration.
                n = len(q)
                scheduled_here = False

                for _ in range(n):
                    pipeline = q.pop(0)

                    # Drop pipelines marked dead.
                    if pipeline.pipeline_id in s.dead_pipeline_ids:
                        continue

                    status = pipeline.runtime_status()
                    if status.is_pipeline_successful():
                        continue

                    # Only schedule ops whose parents are complete; one op per pipeline per tick.
                    op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
                    if not op_list:
                        # Not runnable yet; keep pipeline in queue for later.
                        q.append(pipeline)
                        continue

                    op = op_list[0]

                    # RAM sizing from estimate + learned multiplier.
                    mem_est = _mem_est_gb(op)
                    mult = _safe_float(s.op_ram_mult.get(_op_key(op), s.ram_mult_min), default=s.ram_mult_min)
                    ram_need = mem_est * max(s.ram_mult_min, mult)

                    # Add a small fixed cushion to reduce borderline OOMs on slightly underestimated ops.
                    ram_need = ram_need + 0.25

                    # CPU sizing by priority (small improvements over "use all CPU").
                    cpu_need = _cpu_quanta(prio, getattr(pool, "max_cpu_pool", avail_cpu))

                    # Must fit within caps; do not "squeeze" RAM below need (would likely OOM).
                    if cpu_need > cpu_cap or ram_need > ram_cap:
                        q.append(pipeline)
                        continue

                    assignment = Assignment(
                        ops=op_list,
                        cpu=cpu_need,
                        ram=ram_need,
                        priority=pipeline.priority,
                        pool_id=pool_id,
                        pipeline_id=pipeline.pipeline_id,
                    )
                    assignments.append(assignment)

                    # Update available resources and requeue pipeline for subsequent operators.
                    avail_cpu -= cpu_need
                    avail_ram -= ram_need
                    q.append(pipeline)

                    made_assignment = True
                    scheduled_here = True
                    break

                if scheduled_here:
                    # Restart from highest priority with updated avail_*.
                    break

            if not made_assignment:
                break

    return suspensions, assignments
