# policy_key: scheduler_est_035
# reasoning_effort: low
# exp: estimation
# model: gpt-5.2-2025-12-11
# llm_cost: 0.055006
# generation_seconds: 46.00
# generated_at: 2026-04-10T10:13:45.744001
@register_scheduler_init(key="scheduler_est_035")
def scheduler_est_035_init(s):
    """Memory-aware, priority-first scheduler with gentle fairness + OOM backoff.

    Core ideas:
      - Protect QUERY/INTERACTIVE latency via strict priority ordering with light aging for BATCH.
      - Use op.estimate.mem_peak_gb (when available) to avoid infeasible placements that likely OOM.
      - On failures (especially OOM-like), increase per-pipeline RAM multiplier and retry later instead of
        repeatedly failing at the same size.
      - Pack multiple single-operator assignments per pool per tick while resources allow.
    """
    s.tick = 0

    # Priority queues store pipeline_ids (pipelines objects are looked up in s.pipelines_by_id)
    s.queues = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }

    # pipeline_id -> Pipeline (most recent object reference)
    s.pipelines_by_id = {}

    # pipeline_id -> metadata
    #   arrival_tick: int
    #   ram_mult: float (>=1.0) multiplier applied to estimated memory when sizing RAM
    #   last_enqueued_tick: int (for avoiding excessive reshuffling)
    s.meta = {}


def _priority_rank(priority):
    # Lower is better (scheduled first)
    if priority == Priority.QUERY:
        return 0
    if priority == Priority.INTERACTIVE:
        return 1
    return 2


def _aging_bonus(tick_now, arrival_tick):
    # Small aging to prevent starvation without compromising high-priority latency too much.
    # Returns a value in [0, ~0.5] typically.
    waited = max(0, tick_now - arrival_tick)
    return min(0.5, waited / 2000.0)


def _iter_queue_pop_best(s):
    """Pop the best next pipeline_id to consider across all queues.

    Priority-first with mild aging:
      - Always consider QUERY before INTERACTIVE before BATCH,
        but allow an old BATCH to occasionally move ahead of a very new INTERACTIVE
        only if INTERACTIVE/QUERY are empty.
    """
    # Hard priority: if any QUERY exists, take its head; else INTERACTIVE; else BATCH
    for pr in (Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE):
        q = s.queues[pr]
        while q:
            pid = q.pop(0)
            p = s.pipelines_by_id.get(pid)
            if p is None:
                continue
            return pid
    return None


def _requeue_front(s, pipeline_id):
    p = s.pipelines_by_id.get(pipeline_id)
    if p is None:
        return
    s.queues[p.priority].insert(0, pipeline_id)


def _requeue_back(s, pipeline_id):
    p = s.pipelines_by_id.get(pipeline_id)
    if p is None:
        return
    s.queues[p.priority].append(pipeline_id)


def _estimate_op_mem_gb(op):
    est = getattr(op, "estimate", None)
    if est is None:
        return None
    return getattr(est, "mem_peak_gb", None)


def _is_oom_error(err):
    if err is None:
        return False
    try:
        msg = str(err).lower()
    except Exception:
        return False
    return ("oom" in msg) or ("out of memory" in msg) or ("out-of-memory" in msg) or ("cuda oom" in msg)


def _size_for_op(s, pipeline, op, pool):
    """Return (cpu, ram) to allocate for this op on this pool, or (None, None) if infeasible."""
    pid = pipeline.pipeline_id
    meta = s.meta.get(pid, {})
    ram_mult = float(meta.get("ram_mult", 1.0))

    avail_cpu = pool.avail_cpu_pool
    avail_ram = pool.avail_ram_pool
    if avail_cpu <= 0 or avail_ram <= 0:
        return None, None

    est_mem = _estimate_op_mem_gb(op)

    # Base RAM sizing:
    # - If we have an estimate: allocate est * multiplier with a small safety margin.
    # - If not: be conservative but not overly (to avoid blocking); use a fraction of pool max.
    if est_mem is not None:
        # Safety margin: 15% to reduce under-estimation risk, but keep it modest to avoid over-reserving.
        target_ram = est_mem * ram_mult * 1.15
        # Minimum floor: avoid tiny allocations that might spuriously fail.
        target_ram = max(target_ram, 0.5)
    else:
        # No estimate: use 30% of pool max (bounded), scaled by ram_mult if we've seen failures.
        target_ram = max(0.5, pool.max_ram_pool * 0.30 * ram_mult)

    # Clamp to pool limits and available.
    target_ram = min(target_ram, pool.max_ram_pool)
    if target_ram <= 0:
        return None, None

    # Feasibility filter using estimate: if it clearly doesn't fit, don't schedule here.
    if est_mem is not None and (est_mem * 0.95) > avail_ram:
        return None, None

    ram = min(target_ram, avail_ram)

    # CPU sizing: prioritize low latency for QUERY/INTERACTIVE; keep BATCH smaller to reduce interference.
    if pipeline.priority == Priority.QUERY:
        cpu_target = max(1.0, pool.max_cpu_pool * 0.60)
    elif pipeline.priority == Priority.INTERACTIVE:
        cpu_target = max(1.0, pool.max_cpu_pool * 0.45)
    else:
        cpu_target = max(1.0, pool.max_cpu_pool * 0.25)

    cpu = min(cpu_target, avail_cpu)

    # If we're forced to allocate too little RAM compared to an estimate, skip rather than likely OOM.
    if est_mem is not None:
        # If we can only give <80% of estimated need, better to wait for headroom.
        if ram < (est_mem * 0.80):
            return None, None

    if cpu <= 0 or ram <= 0:
        return None, None

    return cpu, ram


@register_scheduler(key="scheduler_est_035")
def scheduler_est_035_scheduler(s, results: List[ExecutionResult],
                               pipelines: List[Pipeline]) -> Tuple[List[Suspend], List[Assignment]]:
    """Main scheduling loop.

    - Incorporates new arrivals into per-priority FIFO queues.
    - Reacts to failures by increasing per-pipeline RAM multiplier (OOM backoff).
    - For each pool, greedily assigns ready operators (1 op per assignment) while resources remain.
    """
    s.tick += 1

    # Ingest new pipelines
    for p in pipelines:
        pid = p.pipeline_id
        s.pipelines_by_id[pid] = p
        if pid not in s.meta:
            s.meta[pid] = {"arrival_tick": s.tick, "ram_mult": 1.0, "last_enqueued_tick": s.tick}
            s.queues[p.priority].append(pid)
        else:
            # If we already know it, just refresh the reference; avoid double-enqueue.
            pass

    # Process results: adjust RAM multiplier on failures; clean up finished pipelines opportunistically.
    for r in results:
        if r is None:
            continue
        # If a container failed, try to identify its pipeline and apply backoff.
        if hasattr(r, "failed") and r.failed():
            # Try to locate pipeline_id from result if present; otherwise infer from ops if they carry it.
            pid = getattr(r, "pipeline_id", None)

            if pid is None:
                # Best-effort inference: if ops exist and they have pipeline_id
                try:
                    if getattr(r, "ops", None):
                        pid = getattr(r.ops[0], "pipeline_id", None)
                except Exception:
                    pid = None

            if pid is not None and pid in s.meta:
                is_oom = _is_oom_error(getattr(r, "error", None))
                # Increase RAM multiplier more aggressively on OOM-like failures.
                bump = 1.60 if is_oom else 1.25
                s.meta[pid]["ram_mult"] = min(8.0, max(1.0, float(s.meta[pid].get("ram_mult", 1.0)) * bump))

                # Re-enqueue for retry if it still exists; put to back to reduce thrash.
                if pid in s.pipelines_by_id:
                    _requeue_back(s, pid)

    # Early exit if nothing changed and no queued work
    if not pipelines and not results:
        any_waiting = any(len(q) > 0 for q in s.queues.values())
        if not any_waiting:
            return [], []

    suspensions: List[Suspend] = []
    assignments: List[Assignment] = []

    # Greedy assignment per pool
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]

        # While there are resources, try to place one operator at a time (better packing + less OOM cascade)
        placed_any = True
        loop_guard = 0
        while placed_any:
            placed_any = False
            loop_guard += 1
            if loop_guard > 200:
                break  # safety against infinite loops

            avail_cpu = pool.avail_cpu_pool
            avail_ram = pool.avail_ram_pool
            if avail_cpu <= 0 or avail_ram <= 0:
                break

            pid = _iter_queue_pop_best(s)
            if pid is None:
                break

            pipeline = s.pipelines_by_id.get(pid)
            if pipeline is None:
                continue

            status = pipeline.runtime_status()
            # Drop completed pipelines from state
            if status.is_pipeline_successful():
                s.meta.pop(pid, None)
                s.pipelines_by_id.pop(pid, None)
                continue

            # If pipeline has any failures, we still allow retries (unlike the baseline),
            # but Eudoxia may mark some failures terminal; we avoid spinning by requiring assignable ops.
            op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
            if not op_list:
                # Not ready; requeue to back and move on.
                _requeue_back(s, pid)
                continue

            # Pick the next op (simple FIFO within pipeline). Optionally, choose smallest-est-mem first
            # among currently assignable ops to improve packing.
            best_op = None
            best_key = None
            for op in op_list:
                est_mem = _estimate_op_mem_gb(op)
                key = (est_mem is None, est_mem if est_mem is not None else 0.0)
                if best_key is None or key < best_key:
                    best_key = key
                    best_op = op
            op = best_op if best_op is not None else op_list[0]

            cpu, ram = _size_for_op(s, pipeline, op, pool)
            if cpu is None or ram is None:
                # Can't fit in this pool right now. Requeue and stop trying in this pool
                # if this is a high priority pipeline to avoid repeated scanning.
                _requeue_back(s, pid)
                # If we couldn't place due to tight memory, try another pool (next iteration of outer loop),
                # so break the inner loop to avoid wasting time.
                break

            assignment = Assignment(
                ops=[op],
                cpu=cpu,
                ram=ram,
                priority=pipeline.priority,
                pool_id=pool_id,
                pipeline_id=pid,
            )
            assignments.append(assignment)
            placed_any = True

            # If pipeline still has more ops, requeue to allow some round-robin among pipelines.
            # Keep query pipelines nearer the front to reduce E2E latency.
            if pipeline.priority in (Priority.QUERY, Priority.INTERACTIVE):
                _requeue_front(s, pid)
            else:
                _requeue_back(s, pid)

    return suspensions, assignments
