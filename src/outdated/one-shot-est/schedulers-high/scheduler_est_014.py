# policy_key: scheduler_est_014
# reasoning_effort: high
# model: gpt-5.2-2025-12-11
# llm_cost: 0.104292
# generation_seconds: 83.62
# generated_at: 2026-03-31T18:46:39.600732
@register_scheduler_init(key="scheduler_est_014")
def scheduler_est_014_init(s):
    """Priority-aware FIFO with right-sized containers + simple OOM backoff.

    Improvements over the naive FIFO baseline:
      - Priority ordering: QUERY > INTERACTIVE > BATCH_PIPELINE
      - Right-size RAM using op.estimate.mem_peak_gb (+ safety margin)
      - On OOM: increase per-op RAM floor for subsequent retries (bounded), retry a few times
      - Avoid giving BATCH the entire pool when higher-priority work is waiting (leave headroom)
    """
    # Per-priority FIFO queues of pipelines
    s.queues = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }

    # Light metadata for queueing / retries
    s.now = 0
    s.enqueued_at = {}  # pipeline_id -> tick

    # Per-operator retry / sizing state (keyed by operator object identity)
    s.fail_count = {}          # op_key -> int
    s.oom_ram_floor_gb = {}    # op_key -> float (minimum RAM for next retry)
    s.max_retries_per_op = 3

    # Safety margins / caps
    s.mem_safety_margin = 1.20
    s.min_ram_gb = 0.25

    # Allow scheduling FAILED ops (for retries), plus PENDING
    try:
        s.assignable_states = ASSIGNABLE_STATES
    except NameError:
        s.assignable_states = [OperatorState.PENDING, OperatorState.FAILED]


@register_scheduler(key="scheduler_est_014")
def scheduler_est_014_scheduler(s, results, pipelines):
    """
    Scheduler step:
      1) Ingest new pipelines into per-priority queues.
      2) Process results to learn from failures (OOM => raise RAM floor for that op).
      3) For each pool, pick the highest-priority runnable op that fits and assign it.
    """
    # Early exit when nothing changed (match the baseline behavior)
    if not pipelines and not results:
        return [], []

    s.now += 1

    # --- helpers (kept inside for self-contained policy code) ---
    def _is_oom_error(err):
        if not err:
            return False
        e = str(err).lower()
        return ("oom" in e) or ("out of memory" in e) or ("memoryerror" in e)

    def _op_key(op):
        # Use object identity; operators are stable objects in the pipeline DAG in most simulators.
        return id(op)

    def _get_mem_est_gb(op):
        # Prefer op.estimate.mem_peak_gb, but be defensive about missing fields.
        try:
            est = getattr(op, "estimate", None)
            if est is not None and hasattr(est, "mem_peak_gb") and est.mem_peak_gb is not None:
                return float(est.mem_peak_gb)
        except Exception:
            pass
        return 1.0  # conservative fallback

    def _cpu_cap_for_priority(pool, prio, high_prio_backlog):
        # Keep this simple and conservative:
        # - High priority gets more CPU to reduce latency.
        # - Batch is capped more tightly, especially when high-priority backlog exists, to leave headroom.
        max_cpu = float(pool.max_cpu_pool)
        if prio == Priority.QUERY:
            frac = 0.75
        elif prio == Priority.INTERACTIVE:
            frac = 0.60
        else:
            frac = 0.50 if not high_prio_backlog else 0.35
        return max(1.0, max_cpu * frac)

    def _ram_cap_for_batch(pool, high_prio_backlog):
        # When high-priority work is waiting, avoid consuming the whole pool RAM with batch.
        if not high_prio_backlog:
            return float(pool.avail_ram_pool)
        return max(s.min_ram_gb, float(pool.max_ram_pool) * 0.70)

    def _size_request(pool, prio, op, high_prio_backlog):
        # Compute RAM need from estimate + safety margin + learned OOM floor.
        est_gb = _get_mem_est_gb(op)
        base_ram = max(s.min_ram_gb, est_gb * float(s.mem_safety_margin))
        learned_floor = float(s.oom_ram_floor_gb.get(_op_key(op), 0.0))
        ram_need = max(base_ram, learned_floor)

        # Clamp to pool constraints
        ram_need = min(ram_need, float(pool.max_ram_pool))

        # If this is batch and high-priority is waiting, leave headroom by applying a RAM cap.
        if prio == Priority.BATCH_PIPELINE and high_prio_backlog:
            ram_need = min(ram_need, _ram_cap_for_batch(pool, high_prio_backlog))

        # CPU: cap per-priority; never exceed available
        cpu_need = min(float(pool.avail_cpu_pool), _cpu_cap_for_priority(pool, prio, high_prio_backlog))
        cpu_need = max(1.0, cpu_need)

        return cpu_need, ram_need

    def _try_pick_from_queue(queue, pool, high_prio_backlog):
        # Scan FIFO to find the first runnable op that fits in this pool.
        # Pipelines that aren't runnable yet are requeued in FIFO order.
        n = len(queue)
        if n == 0:
            return None

        requeue = []
        chosen = None

        for _ in range(n):
            pipeline = queue.pop(0)
            status = pipeline.runtime_status()

            # Drop completed pipelines
            if status.is_pipeline_successful():
                continue

            # Pick one runnable op (parents complete)
            op_list = status.get_ops(s.assignable_states, require_parents_complete=True)[:1]
            if not op_list:
                requeue.append(pipeline)
                continue

            op = op_list[0]

            # If op has exceeded retry budget, drop this pipeline (avoid infinite loops)
            if int(s.fail_count.get(_op_key(op), 0)) > int(s.max_retries_per_op):
                continue

            # Check fit
            cpu_need, ram_need = _size_request(pool, pipeline.priority, op, high_prio_backlog)
            if cpu_need <= float(pool.avail_cpu_pool) and ram_need <= float(pool.avail_ram_pool):
                chosen = (pipeline, op_list, cpu_need, ram_need)
                # Put back everything else we pulled but didn't schedule
                break
            else:
                # Doesn't fit right now; keep it in FIFO
                requeue.append(pipeline)

        # Restore any unscheduled pipelines to preserve ordering
        queue[:0] = requeue
        return chosen

    # --- ingest new pipelines ---
    for p in pipelines:
        if p.pipeline_id not in s.enqueued_at:
            s.enqueued_at[p.pipeline_id] = s.now
        pr = p.priority
        if pr not in s.queues:
            # Unknown priority: treat as batch
            pr = Priority.BATCH_PIPELINE
        s.queues[pr].append(p)

    # --- process results (learn from failures) ---
    for r in results:
        if not r.failed():
            continue

        oom = _is_oom_error(getattr(r, "error", None))

        for op in getattr(r, "ops", []) or []:
            k = _op_key(op)
            s.fail_count[k] = int(s.fail_count.get(k, 0)) + 1

            if oom:
                # If it OOM'd with ram=r.ram, ensure next try uses more than that.
                prev_alloc = float(getattr(r, "ram", 0.0) or 0.0)
                # 30% step-up is a simple, stable backoff that converges quickly.
                new_floor = max(float(s.oom_ram_floor_gb.get(k, 0.0)), prev_alloc * 1.30, s.min_ram_gb)
                # Cap at something reasonable to avoid runaway
                # (still bounded by pool.max_ram_pool during sizing)
                s.oom_ram_floor_gb[k] = new_floor

    # Determine if any higher-priority backlog exists (used to cap batch sizing)
    high_prio_backlog = (len(s.queues[Priority.QUERY]) + len(s.queues[Priority.INTERACTIVE])) > 0

    suspensions = []
    assignments = []

    # For each pool, schedule at most one op; prioritize higher-priority queues first.
    priority_order = [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]

    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        if float(pool.avail_cpu_pool) <= 0.0 or float(pool.avail_ram_pool) <= 0.0:
            continue

        picked = None
        for pr in priority_order:
            picked = _try_pick_from_queue(s.queues[pr], pool, high_prio_backlog)
            if picked is not None:
                break

        if picked is None:
            continue

        pipeline, op_list, cpu_need, ram_need = picked
        assignments.append(
            Assignment(
                ops=op_list,
                cpu=cpu_need,
                ram=ram_need,
                priority=pipeline.priority,
                pool_id=pool_id,
                pipeline_id=pipeline.pipeline_id,
            )
        )

    return suspensions, assignments
