# policy_key: scheduler_est_013
# reasoning_effort: high
# model: gpt-5.2-2025-12-11
# llm_cost: 0.072484
# generation_seconds: 113.15
# generated_at: 2026-03-31T18:45:15.975462
@register_scheduler_init(key="scheduler_est_013")
def scheduler_est_013_init(s):
    """Priority-aware, estimate-driven scheduler (small, safe improvements over naive FIFO).

    Improvements vs. naive:
      1) Separate FIFO queues per priority and always prefer higher priority runnable work.
      2) Size RAM from op.estimate.mem_peak_gb with a safety margin (instead of "take all RAM").
      3) On failures, assume potential under-allocation and increase a per-op RAM multiplier (bounded).
      4) Leave headroom when scheduling low-priority (batch) work to protect latency for higher priority arrivals.
    """
    # FIFO queues per priority
    s.waiting_queues = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }

    # Track whether a pipeline_id is currently enqueued (avoid duplicates)
    s._in_queue = set()

    # Per-(pipeline,op) RAM multiplier adjusted on failures (simple backoff)
    s._op_ram_mult = {}     # key -> float
    s._op_fail_count = {}   # key -> int

    # Tunables (kept intentionally simple / conservative)
    s.scan_limit = 64  # how many pipelines to scan per queue when searching for runnable work

    s.ram_safety = 1.20      # safety factor over estimate.mem_peak_gb
    s.ram_padding_gb = 0.25  # small constant cushion
    s.min_ram_gb = 0.50      # don't schedule tiny containers
    s.max_mult = 6.00        # cap RAM multiplier growth

    s.min_cpu = 0.10         # if less than this, consider pool saturated

    # CPU caps by priority (relative to pool max); prevents batch from grabbing the whole pool
    s.cpu_cap_frac = {
        Priority.QUERY: 0.85,
        Priority.INTERACTIVE: 0.60,
        Priority.BATCH_PIPELINE: 0.35,
    }

    # When high-priority work exists, keep this fraction of each pool free from batch allocations
    # (a simple latency-protection headroom heuristic).
    s.batch_headroom_frac = 0.25
    s.interactive_headroom_frac = 0.10

    # A small tick counter for debugging / future extensions (not required for correctness)
    s._tick = 0


@register_scheduler(key="scheduler_est_013")
def scheduler_est_013(s, results: List[ExecutionResult], pipelines: List[Pipeline]) -> Tuple[List[Suspend], List[Assignment]]:
    """Priority-aware scheduler with estimate-based RAM sizing and simple OOM-backoff.

    Policy outline each tick:
      - Enqueue new pipelines into per-priority FIFO queues (deduped by pipeline_id).
      - Process results: if an op failed, increase its RAM multiplier; if it succeeded, mildly decay it.
      - For each pool: schedule as many runnable ops as fit, preferring QUERY > INTERACTIVE > BATCH.
        Batch is only allowed to consume resources if we keep headroom when there is higher-priority backlog.
    """
    s._tick += 1

    def _op_key(pipeline_id, op):
        # Best-effort stable identifier across ticks.
        op_id = getattr(op, "op_id", None)
        if op_id is None:
            op_id = getattr(op, "operator_id", None)
        if op_id is None:
            op_id = getattr(op, "name", None)
        if op_id is None:
            # Fallback: deterministic string representation (may be stable in this simulator)
            op_id = str(op)
        return (pipeline_id, op_id)

    def _get_est_mem_gb(op):
        est = None
        try:
            est_obj = getattr(op, "estimate", None)
            if est_obj is not None:
                est = getattr(est_obj, "mem_peak_gb", None)
        except Exception:
            est = None
        if est is None:
            return 1.0
        try:
            return max(0.1, float(est))
        except Exception:
            return 1.0

    def _compute_ram_gb(pool, pipeline_id, op):
        est_gb = _get_est_mem_gb(op)
        key = _op_key(pipeline_id, op)
        mult = s._op_ram_mult.get(key, 1.0)
        ram = est_gb * mult * s.ram_safety + s.ram_padding_gb
        ram = max(s.min_ram_gb, ram)
        ram = min(float(pool.max_ram_pool), float(ram))
        return ram

    def _compute_cpu(pool, priority, avail_cpu):
        cap = float(pool.max_cpu_pool) * float(s.cpu_cap_frac.get(priority, 0.50))
        cap = max(1.0, cap)  # aim to give at least 1 vCPU when possible
        return max(s.min_cpu, min(float(avail_cpu), float(cap)))

    def _enqueue_pipeline(p: Pipeline):
        pid = p.pipeline_id
        if pid in s._in_queue:
            return
        s._in_queue.add(pid)
        s.waiting_queues[p.priority].append(p)

    # Enqueue new pipelines
    for p in pipelines:
        _enqueue_pipeline(p)

    # Early exit: if nothing changed, no need to recompute decisions.
    # (If resources free up, we should see completion results.)
    if not pipelines and not results:
        return [], []

    # Update RAM multipliers based on recent execution outcomes
    for r in results:
        # Adjust all ops reported in this result.
        for op in getattr(r, "ops", []) or []:
            pid = getattr(op, "pipeline_id", None)
            # If pipeline_id isn't on op, we can't form a proper key; skip.
            if pid is None:
                continue
            key = _op_key(pid, op)
            if r.failed():
                # Conservative assumption: failures might be due to under-sized RAM.
                # Increase multiplier with bounded exponential-ish backoff.
                prev = s._op_ram_mult.get(key, 1.0)
                new_mult = min(s.max_mult, max(prev * 1.5, prev + 0.5))
                s._op_ram_mult[key] = new_mult
                s._op_fail_count[key] = s._op_fail_count.get(key, 0) + 1
            else:
                # Mildly decay multiplier on success (never below 1.0)
                prev = s._op_ram_mult.get(key, 1.0)
                s._op_ram_mult[key] = max(1.0, prev * 0.90)

    # Quick backlog flags to decide whether to protect headroom from batch/interactive
    has_query_backlog = len(s.waiting_queues[Priority.QUERY]) > 0
    has_interactive_backlog = len(s.waiting_queues[Priority.INTERACTIVE]) > 0
    has_high_backlog = has_query_backlog or has_interactive_backlog

    def _pop_next_runnable_from_queue(q, pool, avail_cpu, avail_ram, enforce_headroom, pool_id):
        """Scan up to s.scan_limit pipelines in FIFO order, rotating the queue, and return (pipeline, op, ram, cpu)."""
        scanned = 0
        n = len(q)
        if n == 0:
            return None

        while scanned < min(n, s.scan_limit):
            scanned += 1
            pipeline = q.pop(0)

            status = pipeline.runtime_status()
            # Drop completed pipelines
            if status.is_pipeline_successful():
                s._in_queue.discard(pipeline.pipeline_id)
                continue

            # Find next runnable operator (parents complete)
            op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
            if not op_list:
                # Nothing runnable yet; keep pipeline in queue
                q.append(pipeline)
                continue

            op = op_list[0]
            ram_need = _compute_ram_gb(pool, pipeline.pipeline_id, op)
            cpu_need = _compute_cpu(pool, pipeline.priority, avail_cpu)

            # Must fit in remaining capacity
            if cpu_need > float(avail_cpu) or ram_need > float(avail_ram):
                q.append(pipeline)
                continue

            # Optional headroom protection:
            # - If high-priority backlog exists, prevent batch from consuming too much.
            # - If query backlog exists, prevent interactive from consuming too much.
            if enforce_headroom:
                if pipeline.priority == Priority.BATCH_PIPELINE and has_high_backlog:
                    reserve_cpu = float(pool.max_cpu_pool) * float(s.batch_headroom_frac)
                    reserve_ram = float(pool.max_ram_pool) * float(s.batch_headroom_frac)
                    # If this allocation would cut into reserved headroom, skip for now.
                    if (float(avail_cpu) - cpu_need) < reserve_cpu or (float(avail_ram) - ram_need) < reserve_ram:
                        q.append(pipeline)
                        continue

                if pipeline.priority == Priority.INTERACTIVE and has_query_backlog:
                    reserve_cpu = float(pool.max_cpu_pool) * float(s.interactive_headroom_frac)
                    reserve_ram = float(pool.max_ram_pool) * float(s.interactive_headroom_frac)
                    if (float(avail_cpu) - cpu_need) < reserve_cpu or (float(avail_ram) - ram_need) < reserve_ram:
                        q.append(pipeline)
                        continue

            # Keep pipeline queued for future ops (or parallel branches) after this assignment
            q.append(pipeline)
            return pipeline, op, ram_need, cpu_need

        return None

    suspensions: List[Suspend] = []
    assignments: List[Assignment] = []

    # Attempt to schedule work on each pool independently.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)

        if avail_cpu <= s.min_cpu or avail_ram <= s.min_ram_gb:
            continue

        # Fill the pool while we can (local bookkeeping of remaining avail)
        while avail_cpu > s.min_cpu and avail_ram > s.min_ram_gb:
            # Strict pass (protect headroom from low priority)
            candidate = None
            for prio in (Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE):
                q = s.waiting_queues[prio]
                candidate = _pop_next_runnable_from_queue(
                    q=q,
                    pool=pool,
                    avail_cpu=avail_cpu,
                    avail_ram=avail_ram,
                    enforce_headroom=True,
                    pool_id=pool_id,
                )
                if candidate is not None:
                    break

            # Relaxed pass (avoid idling a pool if strict headroom rules block everything)
            if candidate is None:
                for prio in (Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE):
                    q = s.waiting_queues[prio]
                    candidate = _pop_next_runnable_from_queue(
                        q=q,
                        pool=pool,
                        avail_cpu=avail_cpu,
                        avail_ram=avail_ram,
                        enforce_headroom=False,
                        pool_id=pool_id,
                    )
                    if candidate is not None:
                        break

            if candidate is None:
                break

            pipeline, op, ram_need, cpu_need = candidate
            assignments.append(
                Assignment(
                    ops=[op],
                    cpu=cpu_need,
                    ram=ram_need,
                    priority=pipeline.priority,
                    pool_id=pool_id,
                    pipeline_id=pipeline.pipeline_id,
                )
            )

            # Update local remaining capacity for this pool (so we can place more in the same tick)
            avail_cpu -= float(cpu_need)
            avail_ram -= float(ram_need)

    return suspensions, assignments
