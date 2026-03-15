# policy_key: scheduler_medium_012
# reasoning_effort: medium
# model: gpt-5.2-2025-12-11
# llm_cost: 0.099511
# generation_seconds: 114.17
# generated_at: 2026-03-12T22:10:10.894659
@register_scheduler_init(key="scheduler_medium_012")
def scheduler_medium_012_init(s):
    """
    Priority-aware, reservation-based scheduler (incremental improvement over naive FIFO).

    Core ideas:
    - Maintain separate round-robin queues per priority class (QUERY, INTERACTIVE, BATCH).
    - When high-priority work is waiting, reserve a fraction of each pool's CPU/RAM so batch
      can't monopolize the pool (reduces tail latency for interactive/query arrivals).
    - Retry OOM-failed operators by increasing per-pipeline RAM multiplier (simple inference).
    - Provide basic fairness: mostly serve high-priority, but allow batch to make progress.
    """
    from collections import deque

    s.tick = 0

    # Queues by priority (round-robin within each priority).
    s.q_query = deque()
    s.q_interactive = deque()
    s.q_batch = deque()

    # Index of known pipelines (for cleanup / bookkeeping).
    s.pipeline_index = {}

    # Per-pipeline sizing multipliers (start at 1.0; increase on OOM).
    s.pipeline_ram_mult = {}
    s.pipeline_cpu_mult = {}

    # Track operator failures to avoid infinite retries on non-OOM errors.
    s.op_fail_counts = {}   # (pipeline_id, op_key) -> count
    s.dead_pipelines = set()

    # Simple fairness knobs.
    s.high_to_batch_ratio = 5  # after ~5 high-priority assignments, try 1 batch if possible


def _prio_bucket(priority):
    # Higher urgency first.
    if priority == Priority.QUERY:
        return "query"
    if priority == Priority.INTERACTIVE:
        return "interactive"
    return "batch"


def _is_high_priority(priority):
    return priority in (Priority.QUERY, Priority.INTERACTIVE)


def _is_oom_error(err):
    if not err:
        return False
    e = str(err).lower()
    return ("oom" in e) or ("out of memory" in e) or ("killed" in e and "memory" in e)


def _extract_pipeline_id_from_result(res):
    # Best-effort: try ops[0].pipeline_id (common pattern), then res.pipeline_id if present.
    try:
        if getattr(res, "ops", None):
            op0 = res.ops[0]
            pid = getattr(op0, "pipeline_id", None)
            if pid is not None:
                return pid
    except Exception:
        pass
    return getattr(res, "pipeline_id", None)


def _op_key(op):
    # Best-effort stable key for failure tracking.
    for attr in ("op_id", "operator_id", "id", "name"):
        v = getattr(op, attr, None)
        if v is not None:
            return v
    return id(op)


def _queue_for_pipeline(s, pipeline):
    b = _prio_bucket(pipeline.priority)
    if b == "query":
        return s.q_query
    if b == "interactive":
        return s.q_interactive
    return s.q_batch


def _enqueue_pipeline_if_new(s, pipeline):
    pid = pipeline.pipeline_id
    if pid in s.pipeline_index:
        return
    s.pipeline_index[pid] = pipeline
    s.pipeline_ram_mult.setdefault(pid, 1.0)
    s.pipeline_cpu_mult.setdefault(pid, 1.0)
    _queue_for_pipeline(s, pipeline).append(pipeline)


def _cleanup_pipeline(s, pipeline):
    pid = pipeline.pipeline_id
    s.pipeline_index.pop(pid, None)
    s.pipeline_ram_mult.pop(pid, None)
    s.pipeline_cpu_mult.pop(pid, None)
    # Note: we don't remove from deques eagerly; we skip stale entries lazily when popped.


def _has_waiting_high(s):
    # Cheap conservative check: if any high-priority pipeline in index isn't done/dead.
    for pid, p in list(s.pipeline_index.items()):
        if pid in s.dead_pipelines:
            continue
        if not _is_high_priority(p.priority):
            continue
        st = p.runtime_status()
        if st.is_pipeline_successful():
            continue
        # If it has any assignable ops (or will soon), treat as waiting.
        return True
    return False


def _find_ready_pipeline_and_op(s, q, require_parents_complete=True):
    """
    Round-robin scan through a queue to find one pipeline with a ready op.
    Returns (pipeline, [op]) or (None, None).
    """
    n = len(q)
    for _ in range(n):
        p = q.popleft()
        pid = p.pipeline_id

        # Drop dead or completed pipelines lazily.
        if pid in s.dead_pipelines:
            _cleanup_pipeline(s, p)
            continue
        st = p.runtime_status()
        if st.is_pipeline_successful():
            _cleanup_pipeline(s, p)
            continue

        # Try to get a ready operator.
        ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=require_parents_complete)[:1]
        q.append(p)  # keep round-robin order

        if ops:
            return p, ops

    return None, None


def _compute_target_allocation(pool, priority, cpu_mult, ram_mult):
    """
    Compute a heuristic (cpu, ram) request based on pool size and priority.
    Returns integers >= 1.
    """
    # Base shares by priority: higher priority gets more resources to reduce latency.
    if priority == Priority.QUERY:
        cpu_share, ram_share = 0.55, 0.45
    elif priority == Priority.INTERACTIVE:
        cpu_share, ram_share = 0.45, 0.40
    else:  # BATCH_PIPELINE
        cpu_share, ram_share = 0.25, 0.30

    max_cpu = int(getattr(pool, "max_cpu_pool", 1) or 1)
    max_ram = int(getattr(pool, "max_ram_pool", 1) or 1)

    cpu = int(max(1, round(max_cpu * cpu_share * float(cpu_mult))))
    ram = int(max(1, round(max_ram * ram_share * float(ram_mult))))

    # Don't exceed pool maxima.
    cpu = min(cpu, max_cpu)
    ram = min(ram, max_ram)
    return cpu, ram


@register_scheduler(key="scheduler_medium_012")
def scheduler_medium_012(s, results, pipelines):
    """
    Priority + reservations + simple OOM-aware retries.

    Scheduling loop per pool:
    - Prefer QUERY then INTERACTIVE; occasionally schedule BATCH for fairness.
    - If any high-priority exists, keep a CPU/RAM reserve so batch doesn't consume all headroom.
    - Use per-pipeline RAM multiplier; on OOM, double multiplier (bounded), and retry.
    - On non-OOM failures, mark the pipeline dead to avoid infinite retries.
    """
    s.tick += 1

    # Enqueue new pipelines.
    for p in pipelines:
        _enqueue_pipeline_if_new(s, p)

    # Process results to adapt per-pipeline sizing (OOM backoff) and stop hopeless retries.
    for res in results:
        try:
            if not res.failed():
                # Slightly decay multipliers on success (avoid staying over-provisioned forever).
                pid = _extract_pipeline_id_from_result(res)
                if pid is not None and pid in s.pipeline_index:
                    s.pipeline_ram_mult[pid] = max(1.0, s.pipeline_ram_mult.get(pid, 1.0) * 0.98)
                    s.pipeline_cpu_mult[pid] = max(1.0, s.pipeline_cpu_mult.get(pid, 1.0) * 0.99)
                continue

            pid = _extract_pipeline_id_from_result(res)
            if pid is None or pid not in s.pipeline_index:
                continue

            # Identify the operator for failure counting (best-effort).
            op0 = None
            if getattr(res, "ops", None):
                op0 = res.ops[0]
            ok = _op_key(op0) if op0 is not None else ("unknown",)

            k = (pid, ok)
            s.op_fail_counts[k] = s.op_fail_counts.get(k, 0) + 1

            if _is_oom_error(getattr(res, "error", None)):
                # Increase RAM multiplier to avoid repeating OOM; keep bounded to reduce thrash.
                cur = float(s.pipeline_ram_mult.get(pid, 1.0))
                s.pipeline_ram_mult[pid] = min(8.0, max(cur * 2.0, cur + 0.5))

                # If we OOM'd even with a lot of RAM, we can also try modest CPU reduction to
                # reduce memory pressure in some runtimes (heuristic).
                ccur = float(s.pipeline_cpu_mult.get(pid, 1.0))
                s.pipeline_cpu_mult[pid] = max(1.0, ccur * 0.95)
            else:
                # Non-OOM failures are treated as terminal after 1-2 attempts.
                if s.op_fail_counts[k] >= 2:
                    s.dead_pipelines.add(pid)
        except Exception:
            # Never crash the scheduler on unexpected result shapes.
            continue

    # Early exit if nothing to do.
    if not pipelines and not results and not s.pipeline_index:
        return [], []

    suspensions = []
    assignments = []

    high_waiting = _has_waiting_high(s)

    # Per-pool scheduling.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]

        avail_cpu = int(getattr(pool, "avail_cpu_pool", 0) or 0)
        avail_ram = int(getattr(pool, "avail_ram_pool", 0) or 0)
        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        max_cpu = int(getattr(pool, "max_cpu_pool", 1) or 1)
        max_ram = int(getattr(pool, "max_ram_pool", 1) or 1)

        # Reserve some headroom for future/queued high-priority work.
        reserve_cpu = int(round(0.20 * max_cpu)) if high_waiting else 0
        reserve_ram = int(round(0.20 * max_ram)) if high_waiting else 0

        # Counters for simple fairness.
        high_assigned = 0
        batch_assigned = 0

        # Limit per-tick churn: don't start too many containers per pool per tick.
        per_pool_start_limit = max(2, int(round(0.50 * max_cpu)))
        started = 0

        while started < per_pool_start_limit and avail_cpu > 0 and avail_ram > 0:
            # Decide which priority bucket to draw from:
            # - Primarily high priority
            # - Periodically allow one batch if resources beyond reserved headroom
            want_batch = False
            if (high_assigned >= s.high_to_batch_ratio) and (batch_assigned == 0):
                # Only attempt batch if we can do it without eating into reserve.
                if (avail_cpu - reserve_cpu) > 0 and (avail_ram - reserve_ram) > 0:
                    want_batch = True

            if want_batch:
                p, ops = _find_ready_pipeline_and_op(s, s.q_batch, require_parents_complete=True)
                if p is None:
                    # Reset batch attempt; continue with high priority.
                    batch_assigned = 1  # avoid repeatedly trying batch this tick
                    continue
            else:
                # Try QUERY first, then INTERACTIVE, then fallback to BATCH if no high ready.
                p, ops = _find_ready_pipeline_and_op(s, s.q_query, require_parents_complete=True)
                if p is None:
                    p, ops = _find_ready_pipeline_and_op(s, s.q_interactive, require_parents_complete=True)
                if p is None:
                    # Only schedule batch if it doesn't consume reserved headroom when high is waiting.
                    if high_waiting and ((avail_cpu - reserve_cpu) <= 0 or (avail_ram - reserve_ram) <= 0):
                        break
                    p, ops = _find_ready_pipeline_and_op(s, s.q_batch, require_parents_complete=True)

            if p is None or not ops:
                break  # nothing ready anywhere

            pid = p.pipeline_id
            if pid in s.dead_pipelines:
                _cleanup_pipeline(s, p)
                continue

            # Compute target container size.
            cpu_mult = s.pipeline_cpu_mult.get(pid, 1.0)
            ram_mult = s.pipeline_ram_mult.get(pid, 1.0)
            target_cpu, target_ram = _compute_target_allocation(pool, p.priority, cpu_mult, ram_mult)

            # Apply reservation policy: batch must not consume reserved headroom when high exists.
            eff_avail_cpu = avail_cpu
            eff_avail_ram = avail_ram
            if high_waiting and (p.priority == Priority.BATCH_PIPELINE):
                eff_avail_cpu = max(0, avail_cpu - reserve_cpu)
                eff_avail_ram = max(0, avail_ram - reserve_ram)

            # Fit to available resources.
            cpu = min(target_cpu, eff_avail_cpu)
            ram = min(target_ram, eff_avail_ram)

            # If we can't fit even minimal resources, stop for this pool.
            if cpu <= 0 or ram <= 0:
                # For high-priority, we still can't fit: give up this tick (no preemption here).
                break

            assignments.append(
                Assignment(
                    ops=ops,
                    cpu=cpu,
                    ram=ram,
                    priority=p.priority,
                    pool_id=pool_id,
                    pipeline_id=pid,
                )
            )

            avail_cpu -= cpu
            avail_ram -= ram
            started += 1

            if _is_high_priority(p.priority):
                high_assigned += 1
            else:
                batch_assigned += 1
                high_assigned = 0  # allow returning to high priority sooner

    return suspensions, assignments
