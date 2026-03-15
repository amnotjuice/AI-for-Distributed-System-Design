# policy_key: scheduler_medium_036
# reasoning_effort: medium
# model: gpt-5.2-2025-12-11
# llm_cost: 0.061375
# generation_seconds: 64.31
# generated_at: 2026-03-14T03:05:35.918822
@register_scheduler_init(key="scheduler_medium_036")
def scheduler_medium_036_init(s):
    """Priority-aware, OOM-adaptive scheduler (incremental improvement over naive FIFO).

    Goals:
      - Improve latency for high-priority (QUERY/INTERACTIVE) work by (1) strict priority ordering
        and (2) reserving some headroom so batch doesn't fully consume pools.
      - Reduce repeated OOM churn by remembering per-operator RAM hints and retrying with more RAM.
      - Keep code simple: no preemption (suspensions) and no complex runtime prediction.
    """
    # Per-priority FIFO queues of pipelines
    s.queues = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }

    # RAM sizing hints keyed by operator identity (best-effort, from prior OOMs)
    s.op_ram_hint = {}  # op_key -> ram

    # Track non-retryable failures at pipeline granularity (drop quickly)
    s.pipeline_blacklist = set()  # pipeline_id

    # Count OOM retries per (pipeline_id, op_key) to avoid infinite loops
    s.oom_retries = {}  # (pipeline_id, op_key) -> int

    # Round-robin cursor per priority to avoid "head of line" sticking
    s.rr_cursor = {
        Priority.QUERY: 0,
        Priority.INTERACTIVE: 0,
        Priority.BATCH_PIPELINE: 0,
    }


@register_scheduler(key="scheduler_medium_036")
def scheduler_medium_036_scheduler(s, results, pipelines):
    """
    Scheduling strategy:
      1) Enqueue new pipelines into per-priority queues.
      2) Observe results:
         - On OOM: increase RAM hint for the operator; allow retries (bounded).
         - On other failure: blacklist pipeline (do not retry).
      3) For each pool, repeatedly place one ready operator at a time:
         - Strict priority: QUERY > INTERACTIVE > BATCH
         - Reserve headroom for high priority before scheduling BATCH (simple admission control)
         - Size requests using a fraction of pool capacity, then apply RAM hints.

    Returns:
      - suspensions: [] (no preemption in this incremental version)
      - assignments: list of Assignment objects
    """
    def _is_oom_error(err):
        if err is None:
            return False
        try:
            msg = str(err).lower()
        except Exception:
            return False
        return ("oom" in msg) or ("out of memory" in msg) or ("out-of-memory" in msg)

    def _op_key(op):
        # Best-effort stable identity for an operator across retries.
        for attr in ("op_id", "operator_id", "id", "name"):
            if hasattr(op, attr):
                try:
                    v = getattr(op, attr)
                    if v is not None:
                        return (attr, str(v))
                except Exception:
                    pass
        # Fall back to repr; may be unstable but better than nothing
        try:
            return ("repr", repr(op))
        except Exception:
            return ("repr", str(type(op)))

    def _enqueue_pipeline(p):
        # Unknown priorities go to batch queue by default.
        pr = getattr(p, "priority", Priority.BATCH_PIPELINE)
        if pr not in s.queues:
            pr = Priority.BATCH_PIPELINE
        s.queues[pr].append(p)

    def _pop_next_ready_pipeline(priority):
        q = s.queues[priority]
        if not q:
            return None, None, None  # (pipeline, status, op_list)

        # Round-robin scan to avoid sticking on blocked DAGs.
        start = s.rr_cursor[priority] % max(1, len(q))
        n = len(q)
        for i in range(n):
            idx = (start + i) % n
            p = q[idx]

            # Drop blacklisted pipelines eagerly.
            if p.pipeline_id in s.pipeline_blacklist:
                q.pop(idx)
                s.rr_cursor[priority] = idx % max(1, len(q)) if q else 0
                return None, None, None

            status = p.runtime_status()
            if status.is_pipeline_successful():
                q.pop(idx)
                s.rr_cursor[priority] = idx % max(1, len(q)) if q else 0
                return None, None, None

            # Get one runnable op whose parents are complete.
            op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
            if op_list:
                # Move cursor past this pipeline for next time.
                s.rr_cursor[priority] = (idx + 1) % n
                # Remove and re-append at end (simple RR fairness).
                q.pop(idx)
                q.append(p)
                return p, status, op_list

        # Nothing ready.
        s.rr_cursor[priority] = (start + 1) % max(1, len(q))
        return None, None, None

    # 1) Enqueue new arrivals
    for p in pipelines:
        _enqueue_pipeline(p)

    # 2) Incorporate execution results (OOM hints + blacklist)
    for r in results:
        if r is None:
            continue
        if not r.failed():
            continue

        # Identify op (single-op assignments are expected, but handle list)
        ops = getattr(r, "ops", None) or []
        op = ops[0] if ops else None
        opk = _op_key(op) if op is not None else None

        if _is_oom_error(getattr(r, "error", None)):
            # Increase RAM hint; prefer doubling the actual attempted RAM, bounded by pool availability at runtime.
            tried_ram = getattr(r, "ram", None)
            if tried_ram is None:
                tried_ram = 0

            if opk is not None:
                prev = s.op_ram_hint.get(opk, 0)
                # Conservative: at least 1.5x, typically 2x.
                new_hint = max(prev, tried_ram * 2, tried_ram + max(1, int(tried_ram * 0.5)))
                s.op_ram_hint[opk] = new_hint

                key = (getattr(r, "pipeline_id", None), opk)
                # Some ExecutionResult may not have pipeline_id; use (None, opk) in that case.
                if key[0] is None:
                    key = (None, opk)
                s.oom_retries[key] = s.oom_retries.get(key, 0) + 1
        else:
            # Non-OOM failures are considered non-retryable in this simple policy.
            # If pipeline_id is not present on result, we cannot blacklist safely.
            pid = getattr(r, "pipeline_id", None)
            if pid is not None:
                s.pipeline_blacklist.add(pid)

    # Early exit optimization
    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    # Helper: do we currently have any ready high-priority work?
    def _has_ready_high_priority():
        for pr in (Priority.QUERY, Priority.INTERACTIVE):
            q = s.queues.get(pr, [])
            for p in q:
                if p.pipeline_id in s.pipeline_blacklist:
                    continue
                st = p.runtime_status()
                if st.is_pipeline_successful():
                    continue
                ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
                if ops:
                    return True
        return False

    high_pri_waiting = _has_ready_high_priority()

    # 3) Schedule per pool
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]

        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool
        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # Headroom reservation: keep some slack for high priority when it is waiting.
        # (Simple but effective latency improvement over naive "fill everything with batch".)
        reserve_cpu = 0
        reserve_ram = 0
        if high_pri_waiting:
            reserve_cpu = max(0, int(pool.max_cpu_pool * 0.20))
            reserve_ram = max(0, int(pool.max_ram_pool * 0.20))

        # To prevent infinite loops, cap per-pool placements per tick.
        max_placements = 32
        placements = 0

        while placements < max_placements and avail_cpu > 0 and avail_ram > 0:
            placements += 1

            chosen = None  # (pipeline, op_list, priority)
            for pr in (Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE):
                # Admission control for batch: only use resources beyond reserved headroom.
                eff_cpu = avail_cpu
                eff_ram = avail_ram
                if pr == Priority.BATCH_PIPELINE and high_pri_waiting:
                    eff_cpu = max(0, avail_cpu - reserve_cpu)
                    eff_ram = max(0, avail_ram - reserve_ram)

                if eff_cpu <= 0 or eff_ram <= 0:
                    continue

                p, _st, op_list = _pop_next_ready_pipeline(pr)
                if p is None or not op_list:
                    continue

                chosen = (p, op_list, pr, eff_cpu, eff_ram)
                break

            if chosen is None:
                break

            pipeline, op_list, pr, eff_cpu, eff_ram = chosen
            op = op_list[0]
            opk = _op_key(op)

            # Base sizing: fraction of pool capacity (favor scale-up, but allow concurrency).
            if pr in (Priority.QUERY, Priority.INTERACTIVE):
                cpu_share = 0.50
                ram_share = 0.50
                min_cpu = 1
            else:
                cpu_share = 0.25
                ram_share = 0.30
                min_cpu = 1

            req_cpu = max(min_cpu, int(pool.max_cpu_pool * cpu_share))
            req_ram = max(1, int(pool.max_ram_pool * ram_share))

            # Apply per-op RAM hint from OOM history.
            hint_ram = s.op_ram_hint.get(opk, 0)
            if hint_ram and hint_ram > req_ram:
                req_ram = hint_ram

            # Bound by effective available resources for this priority.
            req_cpu = min(req_cpu, eff_cpu)
            req_ram = min(req_ram, eff_ram)

            # If RAM hint doesn't fit, decide best-effort behavior:
            # - For high priority: try with whatever RAM is available (may still OOM, but reduces latency).
            # - For batch: skip to avoid churn.
            if hint_ram and hint_ram > eff_ram:
                if pr in (Priority.QUERY, Priority.INTERACTIVE):
                    req_ram = eff_ram
                else:
                    # Put pipeline back (already RR-appended) and stop trying batch in this pool right now.
                    break

            # OOM retry bound: if we've retried too many times for this op, blacklist the pipeline.
            # (Prevents infinite OOM loops from poisoning the queue.)
            retry_key = (pipeline.pipeline_id, opk)
            if s.oom_retries.get(retry_key, 0) >= 3:
                s.pipeline_blacklist.add(pipeline.pipeline_id)
                continue

            # If we can't allocate at least 1 CPU and some RAM, stop.
            if req_cpu <= 0 or req_ram <= 0:
                break

            assignments.append(
                Assignment(
                    ops=op_list,
                    cpu=req_cpu,
                    ram=req_ram,
                    priority=pr,
                    pool_id=pool_id,
                    pipeline_id=pipeline.pipeline_id,
                )
            )

            # Update remaining pool resources in this scheduling tick.
            avail_cpu -= req_cpu
            avail_ram -= req_ram

            # If we just scheduled high priority, re-evaluate waiting flag cheaply.
            if pr in (Priority.QUERY, Priority.INTERACTIVE):
                high_pri_waiting = _has_ready_high_priority()

    return suspensions, assignments
