# policy_key: scheduler_iter_best_rich_003
# context: rich
# model: gpt-5.2-2025-12-11
# llm_cost: 0.052165
# generation_seconds: 38.07
# generated_at: 2026-04-12T00:22:20.490357
@register_scheduler_init(key="scheduler_iter_best_rich_003")
def scheduler_iter_best_rich_003_init(s):
    """Priority-aware scheduler tuned to reduce weighted latency by increasing safe concurrency.

    Improvements over previous iteration:
      - Much smaller initial RAM requests (reduces wasted allocation; raises parallelism).
      - Per-operator adaptive RAM *and* CPU multipliers:
          * OOM -> increase RAM (and a touch of CPU)
          * timeout -> increase CPU (and a touch of RAM)
          * success -> decay multipliers toward 1.0
      - Pool-aware placement:
          * pool 0 prefers QUERY/INTERACTIVE to protect tail latency
          * other pools accept all priorities
      - Simple high-priority headroom reservation when batch is scheduled, to avoid starving QUERY/INTERACTIVE.

    Intentionally still avoids suspensions/preemption until we have reliable visibility into running containers.
    """
    s.q_query = []
    s.q_interactive = []
    s.q_batch = []

    # Op-level adaptation (keyed by pipeline/op id if possible, else by object id fallback)
    s.op_ram_mult = {}  # op_key -> float
    s.op_cpu_mult = {}  # op_key -> float

    # Track pipelines that had clear non-retryable failures (best-effort, depends on result having pipeline_id)
    s.dead_pipelines = set()

    def _op_key(op, pipeline_id):
        oid = getattr(op, "op_id", None)
        if oid is None:
            oid = getattr(op, "operator_id", None)
        if oid is None:
            oid = id(op)
        return (pipeline_id, oid)

    s._op_key = _op_key


@register_scheduler(key="scheduler_iter_best_rich_003")
def scheduler_iter_best_rich_003_scheduler(s, results, pipelines):
    """Main scheduling loop."""
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

    def _enqueue_pipeline(p):
        if p.pipeline_id in s.dead_pipelines:
            return
        if p.priority == Priority.QUERY:
            s.q_query.append(p)
        elif p.priority == Priority.INTERACTIVE:
            s.q_interactive.append(p)
        else:
            s.q_batch.append(p)

    def _has_waiting_high_prio() -> bool:
        return bool(s.q_query) or bool(s.q_interactive)

    def _pop_next_runnable_pipeline(allow_query=True, allow_interactive=True, allow_batch=True):
        # Priority order always honored; per-pool filters control which priorities are eligible.
        queues = []
        if allow_query:
            queues.append(s.q_query)
        if allow_interactive:
            queues.append(s.q_interactive)
        if allow_batch:
            queues.append(s.q_batch)

        for q in queues:
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
                # Not runnable yet; rotate.
                q.append(p)
        return None, None

    def _get_mults(op, pipeline_id):
        k = s._op_key(op, pipeline_id)
        ram_m = s.op_ram_mult.get(k, 1.0)
        cpu_m = s.op_cpu_mult.get(k, 1.0)
        if ram_m < 1.0:
            ram_m = 1.0
        if cpu_m < 1.0:
            cpu_m = 1.0
        return k, ram_m, cpu_m

    def _size_for(priority, pool, op, pipeline_id, avail_cpu, avail_ram):
        # Smaller initial RAM allocations increase concurrency; rely on OOM backoff to converge.
        max_cpu = pool.max_cpu_pool
        max_ram = pool.max_ram_pool

        # Base targets by priority (kept conservative; multipliers adapt).
        if priority == Priority.QUERY:
            base_cpu = min(2.0, max_cpu * 0.35)
            cpu_cap = min(4.0, max_cpu * 0.60)
            base_ram_frac = 0.05
            ram_cap_frac = 0.22
        elif priority == Priority.INTERACTIVE:
            base_cpu = min(3.0, max_cpu * 0.40)
            cpu_cap = min(6.0, max_cpu * 0.75)
            base_ram_frac = 0.07
            ram_cap_frac = 0.28
        else:  # BATCH_PIPELINE
            base_cpu = min(4.0, max_cpu * 0.50)
            cpu_cap = min(8.0, max_cpu * 0.90)
            base_ram_frac = 0.09
            ram_cap_frac = 0.40

        k, ram_m, cpu_m = _get_mults(op, pipeline_id)

        cpu = base_cpu * cpu_m
        if cpu > cpu_cap:
            cpu = cpu_cap
        if cpu > avail_cpu:
            cpu = avail_cpu

        ram = (max_ram * base_ram_frac) * ram_m
        ram_cap = max_ram * ram_cap_frac
        if ram > ram_cap:
            ram = ram_cap
        if ram > avail_ram:
            ram = avail_ram

        # Avoid degenerate tiny allocations.
        if cpu < 0.1 or ram < 0.1:
            return 0.0, 0.0

        return cpu, ram

    # Ingest new pipelines
    for p in pipelines:
        _enqueue_pipeline(p)

    if not pipelines and not results:
        return [], []

    # Update adaptation based on results (success => decay; failures => increase)
    for r in results:
        # Best-effort op keys (pipeline_id may not exist on result; fall back to None pipeline_id).
        pid = getattr(r, "pipeline_id", None)

        # Success path: decay multipliers to reduce over-allocation over time.
        if not r.failed():
            for op in (r.ops or []):
                k = s._op_key(op, pid)
                s.op_ram_mult[k] = max(1.0, s.op_ram_mult.get(k, 1.0) * 0.90)
                s.op_cpu_mult[k] = max(1.0, s.op_cpu_mult.get(k, 1.0) * 0.92)
            continue

        # Failure path
        err = r.error
        if _is_oom_error(err):
            for op in (r.ops or []):
                k = s._op_key(op, pid)
                cur_r = s.op_ram_mult.get(k, 1.0)
                cur_c = s.op_cpu_mult.get(k, 1.0)
                # Strong RAM backoff; small CPU nudge.
                nxt_r = cur_r * 1.7
                if nxt_r > 16.0:
                    nxt_r = 16.0
                nxt_c = cur_c * 1.10
                if nxt_c > 6.0:
                    nxt_c = 6.0
                s.op_ram_mult[k] = nxt_r
                s.op_cpu_mult[k] = max(1.0, nxt_c)
        elif _is_timeout_error(err):
            for op in (r.ops or []):
                k = s._op_key(op, pid)
                cur_r = s.op_ram_mult.get(k, 1.0)
                cur_c = s.op_cpu_mult.get(k, 1.0)
                # CPU backoff; small RAM nudge in case timeout is due to spill/thrash.
                nxt_c = cur_c * 1.55
                if nxt_c > 8.0:
                    nxt_c = 8.0
                nxt_r = cur_r * 1.10
                if nxt_r > 8.0:
                    nxt_r = 8.0
                s.op_cpu_mult[k] = max(1.0, nxt_c)
                s.op_ram_mult[k] = max(1.0, nxt_r)
        else:
            # Non-OOM/timeout failures: try not to loop forever if we can identify pipeline.
            if pid is not None:
                s.dead_pipelines.add(pid)

    suspensions = []
    assignments = []

    # Schedule per pool
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool

        # Pool 0: protect tail latency by preferring QUERY/INTERACTIVE
        # Other pools: accept all.
        strict_hi_prio = (pool_id == 0)

        while avail_cpu > 0.01 and avail_ram > 0.01:
            # Headroom reservation: if high-priority work is waiting, don't let batch consume the pool dry.
            # (Only applies when we are about to schedule batch in a non-strict pool.)
            reserve_cpu = 0.0
            reserve_ram = 0.0
            if _has_waiting_high_prio():
                reserve_cpu = pool.max_cpu_pool * 0.10
                reserve_ram = pool.max_ram_pool * 0.10

            # Choose eligible priorities for this pool and current headroom.
            if strict_hi_prio:
                p, op_list = _pop_next_runnable_pipeline(
                    allow_query=True, allow_interactive=True, allow_batch=False
                )
                # If no high-priority runnable work exists, let pool0 help with batch.
                if not p:
                    p, op_list = _pop_next_runnable_pipeline(
                        allow_query=False, allow_interactive=False, allow_batch=True
                    )
            else:
                p, op_list = _pop_next_runnable_pipeline(
                    allow_query=True, allow_interactive=True, allow_batch=True
                )

            if not p:
                break

            # If this is batch and high-priority is waiting, enforce reservation.
            eff_avail_cpu = avail_cpu
            eff_avail_ram = avail_ram
            if p.priority == Priority.BATCH_PIPELINE and _has_waiting_high_prio():
                eff_avail_cpu = max(0.0, avail_cpu - reserve_cpu)
                eff_avail_ram = max(0.0, avail_ram - reserve_ram)
                if eff_avail_cpu <= 0.01 or eff_avail_ram <= 0.01:
                    # Can't schedule batch without hurting high-priority; rotate batch and stop on this pool.
                    _enqueue_pipeline(p)
                    break

            op = op_list[0]
            cpu, ram = _size_for(p.priority, pool, op, p.pipeline_id, eff_avail_cpu, eff_avail_ram)

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

            # Round-robin fairness within priority: rotate pipeline back.
            _enqueue_pipeline(p)

    return suspensions, assignments
