# policy_key: scheduler_est_021
# reasoning_effort: low
# exp: estimation
# model: gpt-5.2-2025-12-11
# llm_cost: 0.051772
# generation_seconds: 53.95
# generated_at: 2026-04-10T10:01:20.203042
@register_scheduler_init(key="scheduler_est_021")
def scheduler_est_021_init(s):
    """Memory-aware, priority-first scheduler with OOM-adaptive RAM sizing.

    Core ideas:
      - Maintain separate FIFO queues per priority; always prefer QUERY then INTERACTIVE then BATCH.
      - Within a pool, greedily pack ready operators that "fit" by estimated peak memory (hint).
      - Right-size RAM per operator using estimate + safety factor; increase RAM on repeated OOMs.
      - Allocate CPU proportionally by priority (more CPU to higher priority) while leaving room to pack.
      - Avoid head-of-line blocking by rotating queues when the head cannot run in the current pool.
    """
    s.waitq = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }
    # Per-operator adaptive RAM multiplier based on OOM history
    s.oom_count_by_op = {}  # key: id(op) -> int
    # Track pipelines we've already queued to avoid duplicates if they re-appear
    s.known_pipeline_ids = set()


def _prio_order():
    return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]


def _safety_factor(priority):
    # More conservative for high-priority to reduce OOM-induced 720s penalties
    if priority == Priority.QUERY:
        return 1.45
    if priority == Priority.INTERACTIVE:
        return 1.35
    return 1.20


def _cpu_cap_fraction(priority):
    # Bias CPU to protect query/interactive latency; still allow multiple concurrent ops in a pool.
    if priority == Priority.QUERY:
        return 0.75
    if priority == Priority.INTERACTIVE:
        return 0.60
    return 0.45


def _min_ram_gb(priority):
    # Conservative floor to avoid tiny allocations causing OOM from overheads
    if priority == Priority.QUERY:
        return 1.0
    if priority == Priority.INTERACTIVE:
        return 1.0
    return 0.75


def _get_ready_op(pipeline):
    status = pipeline.runtime_status()
    op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
    if not op_list:
        return None
    return op_list[0]


def _op_est_mem_gb(op):
    est = None
    try:
        if getattr(op, "estimate", None) is not None:
            est = getattr(op.estimate, "mem_peak_gb", None)
    except Exception:
        est = None
    if est is None:
        return None
    try:
        est = float(est)
    except Exception:
        return None
    if est < 0:
        return None
    return est


def _ram_request_gb(s, op, priority, pool, avail_ram):
    est = _op_est_mem_gb(op)

    # Base estimate fallback: take a modest slice of pool RAM so we can still pack,
    # but not so small that overheads dominate.
    if est is None:
        est = max(_min_ram_gb(priority), 0.18 * float(pool.max_ram_pool))

    # Apply safety and OOM backoff multiplier (exponential growth on repeated OOMs).
    oom_cnt = s.oom_count_by_op.get(id(op), 0)
    backoff = 2.0 ** min(oom_cnt, 3)  # cap to avoid runaway
    req = est * _safety_factor(priority) * backoff

    # Clamp request within feasible bounds.
    # Leave a small headroom margin to reduce near-capacity fragmentation/OOM risk.
    max_feasible = 0.95 * float(pool.max_ram_pool)
    req = max(req, _min_ram_gb(priority))
    req = min(req, max_feasible)

    # Never request more than currently available in the pool.
    req = min(req, float(avail_ram))
    return req


def _cpu_request(s, priority, pool, avail_cpu):
    # Allocate CPU with a priority-based cap; never exceed availability.
    cap = _cpu_cap_fraction(priority) * float(pool.max_cpu_pool)
    req = min(float(avail_cpu), max(1.0, cap))
    # If the pool only has fractional CPU left, still try to use it.
    req = min(req, float(avail_cpu))
    return req


def _pipeline_terminal_or_failed(pipeline):
    status = pipeline.runtime_status()
    if status.is_pipeline_successful():
        return True
    # Drop pipelines with any failures (baseline behavior); we still try to avoid OOMs proactively.
    try:
        has_failures = status.state_counts[OperatorState.FAILED] > 0
    except Exception:
        has_failures = False
    return bool(has_failures)


def _rotate_until_runnable(s, prio, pool, avail_ram, avail_cpu, max_rotations):
    """Find a pipeline in queue that has a ready op that can fit this pool right now.
    Rotates the queue to avoid head-of-line blocking. Returns (pipeline, op, ram_req, cpu_req) or (None,...).
    """
    q = s.waitq[prio]
    if not q:
        return None, None, None, None

    rotations = 0
    while rotations < max_rotations and q:
        p = q[0]

        if _pipeline_terminal_or_failed(p):
            q.pop(0)
            continue

        op = _get_ready_op(p)
        if op is None:
            # Not ready yet; rotate to allow other pipelines to proceed.
            q.append(q.pop(0))
            rotations += 1
            continue

        ram_req = _ram_request_gb(s, op, p.priority, pool, avail_ram)
        cpu_req = _cpu_request(s, p.priority, pool, avail_cpu)

        # If it can't fit now, rotate to avoid blocking.
        # If it can't ever fit in this pool (ram_req clamped to avail, so check estimated vs pool max),
        # rotating still helps other work; this pipeline may run in another pool.
        est = _op_est_mem_gb(op)
        if est is not None and est > 0.95 * float(pool.max_ram_pool):
            # Effectively unschedulable in this pool; rotate.
            q.append(q.pop(0))
            rotations += 1
            continue

        if ram_req <= 0 or cpu_req <= 0:
            q.append(q.pop(0))
            rotations += 1
            continue

        # Must fit current availability.
        if ram_req <= float(avail_ram) and cpu_req <= float(avail_cpu):
            return p, op, ram_req, cpu_req

        q.append(q.pop(0))
        rotations += 1

    return None, None, None, None


@register_scheduler(key="scheduler_est_021")
def scheduler_est_021_scheduler(s, results, pipelines):
    """
    Priority-first, memory-aware greedy packing across pools.

    Scheduling loop:
      1) Ingest new pipelines into per-priority FIFO queues.
      2) Update OOM backoff state from recent execution results.
      3) For each pool, greedily assign ready operators that fit available CPU/RAM, preferring:
         QUERY > INTERACTIVE > BATCH, and avoiding head-of-line blocking by rotating queues.
    """
    # Ingest new arrivals (avoid duplicating already-known pipelines)
    for p in pipelines:
        pid = getattr(p, "pipeline_id", None)
        if pid is not None and pid in s.known_pipeline_ids:
            continue
        if pid is not None:
            s.known_pipeline_ids.add(pid)
        s.waitq[p.priority].append(p)

    # Update OOM history from results to adapt RAM sizing.
    for r in results:
        try:
            if r.failed():
                err = (r.error or "")
                err_l = err.lower() if isinstance(err, str) else ""
                # Best-effort detection of OOM-like failures.
                is_oom = ("oom" in err_l) or ("out of memory" in err_l) or ("cuda oom" in err_l) or ("killed" in err_l)
                if is_oom:
                    for op in getattr(r, "ops", []) or []:
                        s.oom_count_by_op[id(op)] = s.oom_count_by_op.get(id(op), 0) + 1
        except Exception:
            # Never let bookkeeping break scheduling
            pass

    # Early exit if nothing changed
    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    # Greedy packing per pool
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)

        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # Keep assigning while resources remain; stop when no runnable work found.
        # Limit iterations to avoid infinite loops on rotation.
        progress = True
        iter_guard = 0
        while progress and avail_cpu > 0 and avail_ram > 0 and iter_guard < 64:
            iter_guard += 1
            progress = False

            chosen = None
            # Strict priority order to protect weighted objective.
            for prio in _prio_order():
                # Rotate at most len(queue) to find a runnable pipeline for this pool.
                max_rot = max(1, len(s.waitq[prio]))
                p, op, ram_req, cpu_req = _rotate_until_runnable(
                    s, prio, pool, avail_ram, avail_cpu, max_rotations=max_rot
                )
                if p is None:
                    continue

                chosen = (p, op, ram_req, cpu_req)
                break

            if chosen is None:
                break

            p, op, ram_req, cpu_req = chosen

            # Pop the chosen pipeline from head (it should be at head after rotation and selection).
            # If not at head due to unexpected mutations, remove defensively.
            q = s.waitq[p.priority]
            if q and q[0] is p:
                q.pop(0)
            else:
                try:
                    q.remove(p)
                except Exception:
                    pass

            assignments.append(
                Assignment(
                    ops=[op],
                    cpu=cpu_req,
                    ram=ram_req,
                    priority=p.priority,
                    pool_id=pool_id,
                    pipeline_id=p.pipeline_id,
                )
            )

            avail_cpu -= float(cpu_req)
            avail_ram -= float(ram_req)

            # Requeue pipeline for subsequent operators unless it has become terminal/failed.
            if not _pipeline_terminal_or_failed(p):
                s.waitq[p.priority].append(p)

            progress = True

    return suspensions, assignments
