# policy_key: scheduler_est_002
# reasoning_effort: low
# model: gpt-5.2-2025-12-11
# llm_cost: 0.048181
# generation_seconds: 39.40
# generated_at: 2026-04-01T13:37:01.050139
@register_scheduler_init(key="scheduler_est_002")
def scheduler_est_002_init(s):
    """
    Priority-aware improvement over naive FIFO:
      - Maintain per-priority queues (QUERY > INTERACTIVE > BATCH_PIPELINE).
      - Keep small "latency headroom" by reserving some CPU/RAM so batch doesn't consume everything.
      - Use conservative RAM sizing with estimator hints when available.
      - On OOM-like failures, retry the same op with increased RAM (exponential backoff).
      - Avoid infinite retry loops by capping OOM retries; drop pipelines with non-OOM failures.
    """
    # Per-priority waiting queues of pipelines
    s.waiting_by_prio = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }

    # Track last allocation and OOM retry counts per operator object (by id(op))
    s.op_last_ram = {}        # op_key -> last_ram
    s.op_last_cpu = {}        # op_key -> last_cpu
    s.op_oom_retries = {}     # op_key -> retries

    # Track pipelines that should not be retried due to non-OOM failures
    s.dead_pipelines = set()  # pipeline_id

    # Tuning knobs (small, incremental improvements)
    s.max_oom_retries = 3

    # Reserve some headroom when there is pending high-priority work
    s.reserve_cpu_frac = 0.15
    s.reserve_ram_frac = 0.10

    # CPU caps to avoid a single batch op monopolizing a pool (helps latency under contention)
    s.cpu_cap_query = 8.0
    s.cpu_cap_interactive = 6.0
    s.cpu_cap_batch = 4.0

    # RAM headroom over estimates / previous allocations
    s.ram_headroom_frac = 0.20
    s.min_ram_gb = 0.25  # avoid allocating tiny RAM that would trivially OOM


@register_scheduler(key="scheduler_est_002")
def scheduler_est_002_scheduler(s, results: List["ExecutionResult"],
                                pipelines: List["Pipeline"]) -> Tuple[List["Suspend"], List["Assignment"]]:
    """
    Scheduler step:
      1) Ingest new pipelines into per-priority queues.
      2) Process results; on OOM failures, increase RAM and allow retry; on other failures, mark pipeline dead.
      3) For each pool, assign ready ops, prioritizing high-priority work and preserving headroom if needed.
    """
    # -------------------------
    # Helpers (kept inside to match "no imports" and single-file policy)
    # -------------------------
    def _prio_order():
        return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]

    def _is_oom_error(err) -> bool:
        if err is None:
            return False
        msg = str(err).lower()
        return ("oom" in msg) or ("out of memory" in msg) or ("memoryerror" in msg)

    def _enqueue_pipeline(p):
        if p.pipeline_id in s.dead_pipelines:
            return
        if p.priority not in s.waiting_by_prio:
            # Unknown priority: treat as lowest.
            s.waiting_by_prio[Priority.BATCH_PIPELINE].append(p)
        else:
            s.waiting_by_prio[p.priority].append(p)

    def _queue_has_pending_high():
        # If there is any QUERY/INTERACTIVE pipeline with assignable ops, treat as pending high.
        for pr in (Priority.QUERY, Priority.INTERACTIVE):
            q = s.waiting_by_prio.get(pr, [])
            for p in q:
                if p.pipeline_id in s.dead_pipelines:
                    continue
                st = p.runtime_status()
                if st.is_pipeline_successful():
                    continue
                ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
                if ops:
                    return True
        return False

    def _cpu_cap_for_priority(pr):
        if pr == Priority.QUERY:
            return float(s.cpu_cap_query)
        if pr == Priority.INTERACTIVE:
            return float(s.cpu_cap_interactive)
        return float(s.cpu_cap_batch)

    def _desired_ram_for_op(op, pipeline_prio, avail_ram):
        # Base: last allocation, estimator hint, and a small minimum.
        op_key = id(op)
        last_ram = float(s.op_last_ram.get(op_key, 0.0))

        # Estimator hint: "noisy hint about the minimum safe RAM needed to avoid OOM"
        est = None
        try:
            est = getattr(getattr(op, "estimate", None), "mem_peak_gb", None)
        except Exception:
            est = None

        base = max(float(s.min_ram_gb), last_ram)
        if est is not None:
            try:
                est = float(est)
                # Treat estimate as a lower bound; add headroom.
                base = max(base, est * (1.0 + float(s.ram_headroom_frac)))
            except Exception:
                pass

        # If we've had OOM retries, bump more aggressively.
        retries = int(s.op_oom_retries.get(op_key, 0))
        if retries > 0:
            # Exponential growth starting from current base.
            base = max(base, last_ram * (2.0 ** retries), base)

        # Don't exceed what's available in pool right now.
        return min(base, float(avail_ram))

    def _pick_next_pipeline_and_op():
        # Strict priority: QUERY -> INTERACTIVE -> BATCH. (Small, obvious improvement vs FIFO)
        for pr in _prio_order():
            q = s.waiting_by_prio.get(pr, [])
            # Rotate through queue to avoid sticking on a blocked pipeline
            for _ in range(len(q)):
                p = q.pop(0)
                if p.pipeline_id in s.dead_pipelines:
                    continue
                st = p.runtime_status()
                if st.is_pipeline_successful():
                    continue
                # If pipeline has FAILED ops due to non-OOM, treat as dead (avoid churn)
                # (We only retry ops if we see OOM in results; otherwise dead_pipelines is set there.)
                ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
                # Requeue pipeline whether or not it had an assignable op, to check later.
                q.append(p)
                if ops:
                    return p, ops
        return None, None

    # -------------------------
    # Ingest new pipelines
    # -------------------------
    for p in pipelines:
        _enqueue_pipeline(p)

    # Early exit if nothing changed
    if not pipelines and not results:
        return [], []

    # -------------------------
    # Process results (learn from failures, update retry state)
    # -------------------------
    for r in results:
        # Track last used resources for ops, so we can warm-start retries / future runs.
        try:
            for op in (r.ops or []):
                op_key = id(op)
                try:
                    s.op_last_ram[op_key] = float(r.ram)
                except Exception:
                    pass
                try:
                    s.op_last_cpu[op_key] = float(r.cpu)
                except Exception:
                    pass

                if r.failed():
                    if _is_oom_error(r.error):
                        # Allow retry; increase retry count with cap.
                        cur = int(s.op_oom_retries.get(op_key, 0))
                        s.op_oom_retries[op_key] = cur + 1
                        if s.op_oom_retries[op_key] > int(s.max_oom_retries):
                            # Too many retries; mark the whole pipeline dead to avoid infinite loops.
                            s.dead_pipelines.add(getattr(r, "pipeline_id", None) or None)
                    else:
                        # Non-OOM failure: mark the pipeline dead if we can; otherwise do nothing.
                        # (If pipeline_id isn't available on result, the pipeline may continue, but
                        # we won't actively reschedule non-OOM FAILED ops.)
                        pid = getattr(r, "pipeline_id", None)
                        if pid is not None:
                            s.dead_pipelines.add(pid)
        except Exception:
            # If result parsing fails, don't crash the scheduler.
            pass

    suspensions = []
    assignments = []

    # -------------------------
    # Scheduling / assignments per pool
    # -------------------------
    pending_high = _queue_has_pending_high()

    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)
        if avail_cpu <= 0.0 or avail_ram <= 0.0:
            continue

        # Keep a little headroom when high-priority work exists (avoid batch fully consuming the pool)
        if pending_high:
            reserve_cpu = float(pool.max_cpu_pool) * float(s.reserve_cpu_frac)
            reserve_ram = float(pool.max_ram_pool) * float(s.reserve_ram_frac)
        else:
            reserve_cpu = 0.0
            reserve_ram = 0.0

        effective_cpu = max(0.0, avail_cpu - reserve_cpu)
        effective_ram = max(0.0, avail_ram - reserve_ram)

        # If we have no effective capacity, skip this pool this tick.
        if effective_cpu <= 0.0 or effective_ram <= 0.0:
            continue

        # Assign at most one op per pool per tick (small improvement; avoids overcommitting based on stale avail)
        p, op_list = _pick_next_pipeline_and_op()
        if p is None or not op_list:
            continue

        # Decide CPU: cap based on priority; use all effective CPU up to cap for interactive-ish latency
        cap = _cpu_cap_for_priority(p.priority)
        cpu = min(effective_cpu, cap)
        if cpu <= 0.0:
            continue

        # Decide RAM: conservative based on last allocation / estimator + headroom, within effective_ram
        op = op_list[0]
        ram = _desired_ram_for_op(op, p.priority, effective_ram)
        if ram <= 0.0:
            continue

        # If an op has exceeded OOM retry cap, drop the pipeline to avoid churn
        if int(s.op_oom_retries.get(id(op), 0)) > int(s.max_oom_retries):
            s.dead_pipelines.add(p.pipeline_id)
            continue

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

    return suspensions, assignments
