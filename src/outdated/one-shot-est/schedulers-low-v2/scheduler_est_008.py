# policy_key: scheduler_est_008
# reasoning_effort: low
# model: gpt-5.2-2025-12-11
# llm_cost: 0.043656
# generation_seconds: 41.05
# generated_at: 2026-04-01T13:41:14.255188
@register_scheduler_init(key="scheduler_est_008")
def scheduler_est_008_init(s):
    """Priority-aware, conservative admission scheduler (small step up from naive FIFO).

    Main improvements vs naive:
      1) Maintain separate queues per priority and always try to schedule higher priority first.
      2) Add simple "headroom reservation" so BATCH doesn't consume the last resources and block
         interactive/query arrivals (latency protection without needing true preemption APIs).
      3) Use optional op.estimate.mem_peak_gb conservatively for RAM sizing, and on failure (e.g., OOM)
         retry with increased RAM (pipeline-level backoff).
      4) Avoid duplicate enqueues; keep pipelines alive in queues until completion.
    """
    # Per-priority FIFO queues of pipelines
    s.q_query = []
    s.q_interactive = []
    s.q_batch = []

    # Fast membership tracking to prevent duplicate enqueues
    s._queued = set()  # pipeline_id currently in any queue

    # Pipeline-level RAM inflation factor for retries after memory-related failures
    s._ram_inflate = {}  # pipeline_id -> float

    # Soft "aging" tick to occasionally give batch a chance when there is constant interactive load
    s._tick = 0


def _prio_rank(p):
    # Smaller is higher priority
    if p == Priority.QUERY:
        return 0
    if p == Priority.INTERACTIVE:
        return 1
    return 2


def _enqueue_pipeline(s, p):
    pid = p.pipeline_id
    if pid in s._queued:
        return
    if p.priority == Priority.QUERY:
        s.q_query.append(p)
    elif p.priority == Priority.INTERACTIVE:
        s.q_interactive.append(p)
    else:
        s.q_batch.append(p)
    s._queued.add(pid)
    s._ram_inflate.setdefault(pid, 1.0)


def _dequeue_pipeline(s, p):
    # Remove from queued set; actual list removal happens by pop(0) in the main loop.
    pid = p.pipeline_id
    if pid in s._queued:
        s._queued.remove(pid)


def _requeue_front(q, p):
    # Put pipeline back at the end (FIFO fairness within that priority class)
    q.append(p)


def _pick_next_pipeline(s):
    # Weighted priority: mostly query/interactive, but allow occasional batch to avoid starvation.
    # Every 10 ticks, allow batch to compete earlier if present.
    s._tick += 1
    allow_batch_early = (s._tick % 10 == 0)

    if s.q_query:
        return s.q_query, s.q_query.pop(0)
    if s.q_interactive:
        return s.q_interactive, s.q_interactive.pop(0)
    if allow_batch_early and s.q_batch:
        return s.q_batch, s.q_batch.pop(0)
    if s.q_batch:
        return s.q_batch, s.q_batch.pop(0)
    return None, None


def _mem_hint_gb(op):
    # Estimator attaches op.estimate.mem_peak_gb; handle missing gracefully.
    est = getattr(op, "estimate", None)
    if est is None:
        return None
    v = getattr(est, "mem_peak_gb", None)
    try:
        if v is None:
            return None
        v = float(v)
        if v <= 0:
            return None
        return v
    except Exception:
        return None


def _is_probable_oom(err):
    if err is None:
        return False
    try:
        msg = str(err).lower()
    except Exception:
        return False
    # Heuristic: simulator / runtime often includes "oom" or "out of memory"
    return ("oom" in msg) or ("out of memory" in msg) or ("memory" in msg and "killed" in msg)


def _reserve_headroom(pool, incoming_priority):
    # Reserve a fraction of pool capacity so that high-priority work can still be admitted quickly.
    # For low priority work, we keep more headroom.
    if incoming_priority in (Priority.QUERY, Priority.INTERACTIVE):
        # High priority: allow using almost everything.
        return 0.05, 0.05  # reserve 5% cpu/ram
    # Batch: reserve more so interactive/query aren't blocked by batch filling the pool.
    return 0.25, 0.25  # reserve 25% cpu/ram


def _choose_cpu(pool, incoming_priority, avail_cpu):
    # Latency bias: give more CPU to higher priorities, but keep at least 1 vCPU.
    max_cpu = pool.max_cpu_pool
    if incoming_priority == Priority.QUERY:
        target = max(1.0, 0.75 * max_cpu)
    elif incoming_priority == Priority.INTERACTIVE:
        target = max(1.0, 0.50 * max_cpu)
    else:
        target = max(1.0, 0.25 * max_cpu)

    # Don't exceed what's available.
    if avail_cpu < 1.0:
        return 0.0
    return min(avail_cpu, target)


def _choose_ram(pool, pipeline_id, incoming_priority, avail_ram, op):
    # Conservative RAM sizing:
    # - If estimator is present, pad it (30%) and apply pipeline-level inflation (after failures).
    # - Otherwise, allocate a modest default share (priority aware) and inflation.
    inflate = float(getattr(s, "_ram_inflate", {}).get(pipeline_id, 1.0))  # may be shadowed; see caller
    est = _mem_hint_gb(op)
    if est is not None:
        need = est * 1.30 * inflate
    else:
        # Default RAM targets; keeps latency decent while avoiding massive overallocation.
        max_ram = pool.max_ram_pool
        if incoming_priority == Priority.QUERY:
            need = 0.35 * max_ram * inflate
        elif incoming_priority == Priority.INTERACTIVE:
            need = 0.25 * max_ram * inflate
        else:
            need = 0.15 * max_ram * inflate

        # Ensure a small floor to reduce immediate OOM in common cases.
        need = max(1.0, need)

    if avail_ram < need:
        return 0.0
    return min(avail_ram, need)


@register_scheduler(key="scheduler_est_008")
def scheduler_est_008_scheduler(s, results, pipelines):
    """
    Priority-aware scheduler with headroom reservation and OOM-aware retries.

    Decisions:
      - Admission: enqueue all arriving pipelines into per-priority FIFO.
      - Placement: iterate pools and assign at most one ready op per pool per tick.
      - Sizing: CPU sized by priority; RAM uses estimator hint with padding; retry inflates RAM.
      - Preemption: not used (no reliable running-container listing in this interface); instead
        we protect latency via headroom reservation to reduce blocking.
    """
    # Enqueue arrivals
    for p in pipelines:
        _enqueue_pipeline(s, p)

    # Process results: on probable OOM, inflate RAM for that pipeline and requeue if needed.
    # Also remove completed pipelines from our bookkeeping.
    if results:
        for r in results:
            pid = getattr(r, "pipeline_id", None)
            # Some simulators may not attach pipeline_id to result; fall back to None-safe logic.
            # If we can't map it, we still proceed without pipeline-specific tuning.
            if pid is not None and r.failed():
                if _is_probable_oom(getattr(r, "error", None)):
                    # Exponential-ish backoff, capped.
                    cur = s._ram_inflate.get(pid, 1.0)
                    s._ram_inflate[pid] = min(8.0, cur * 1.6)

    # Early exit if nothing new to decide and no resources changed materially
    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    # Try to assign one operator per pool per tick to reduce contention and keep decisions simple.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool
        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # Attempt a few picks to find a runnable pipeline whose parents are complete
        # and that fits under headroom constraints.
        attempts = 0
        max_attempts = 32  # prevents long loops when many pipelines are blocked on dependencies
        while attempts < max_attempts:
            attempts += 1
            q, pipeline = _pick_next_pipeline(s)
            if pipeline is None:
                break

            status = pipeline.runtime_status()

            # Drop successful pipelines
            if status.is_pipeline_successful():
                _dequeue_pipeline(s, pipeline)
                continue

            # If any ops are FAILED, we allow retry (ASSIGNABLE_STATES includes FAILED),
            # relying on RAM inflation for OOM-like failures.
            op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
            if not op_list:
                # Not runnable now (parents not done / nothing assignable); put back.
                _requeue_front(q, pipeline)
                continue

            op = op_list[0]

            # Headroom reservation: for batch, don't consume the last chunk of the pool.
            reserve_cpu_frac, reserve_ram_frac = _reserve_headroom(pool, pipeline.priority)
            reserve_cpu = reserve_cpu_frac * pool.max_cpu_pool
            reserve_ram = reserve_ram_frac * pool.max_ram_pool

            effective_avail_cpu = max(0.0, avail_cpu - reserve_cpu)
            effective_avail_ram = max(0.0, avail_ram - reserve_ram)

            if effective_avail_cpu < 1.0 or effective_avail_ram <= 0:
                # Can't safely admit more work in this pool right now; put pipeline back.
                _requeue_front(q, pipeline)
                break

            # Choose CPU and RAM for this operator.
            cpu = _choose_cpu(pool, pipeline.priority, effective_avail_cpu)
            if cpu <= 0:
                _requeue_front(q, pipeline)
                break

            # Inline RAM sizing using s._ram_inflate (avoid helper shadowing 's').
            inflate = float(s._ram_inflate.get(pipeline.pipeline_id, 1.0))
            est = _mem_hint_gb(op)
            if est is not None:
                need_ram = est * 1.30 * inflate
            else:
                if pipeline.priority == Priority.QUERY:
                    need_ram = max(1.0, 0.35 * pool.max_ram_pool * inflate)
                elif pipeline.priority == Priority.INTERACTIVE:
                    need_ram = max(1.0, 0.25 * pool.max_ram_pool * inflate)
                else:
                    need_ram = max(1.0, 0.15 * pool.max_ram_pool * inflate)

            if effective_avail_ram < need_ram:
                # Not enough RAM with headroom; defer.
                _requeue_front(q, pipeline)
                continue

            ram = min(effective_avail_ram, need_ram)

            assignments.append(
                Assignment(
                    ops=op_list,
                    cpu=cpu,
                    ram=ram,
                    priority=pipeline.priority,
                    pool_id=pool_id,
                    pipeline_id=pipeline.pipeline_id,
                )
            )

            # Put pipeline back for future operators; keep it marked as queued.
            _requeue_front(q, pipeline)
            break

    return suspensions, assignments
