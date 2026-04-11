# policy_key: scheduler_low_014
# reasoning_effort: low
# model: gpt-5.2-2025-12-11
# llm_cost: 0.059497
# generation_seconds: 51.35
# generated_at: 2026-04-09T21:13:05.622115
@register_scheduler_init(key="scheduler_low_014")
def scheduler_low_014_init(s):
    """
    Priority- and reliability-aware scheduler tuned for low weighted-latency score.

    Core ideas:
    - Strict priority ordering for QUERY > INTERACTIVE > BATCH, with limited spillover to avoid starvation.
    - Conservative (larger) initial RAM sizing to reduce OOM-driven failures (heavily penalized by objective).
    - Adaptive retry on OOM: increase per-pipeline RAM hint (and modestly CPU hint) and retry a few times.
    - Gentle per-op CPU/RAM caps by priority to avoid a single batch op hoarding an entire pool.
    - Optional pool preference: if multiple pools exist, prefer pool 0 for QUERY/INTERACTIVE, other pools for BATCH,
      but allow spillover when preferred queues are empty to keep utilization up.
    """
    # Queues per priority (FIFO within each)
    s.q_query = []
    s.q_interactive = []
    s.q_batch = []

    # Per-pipeline adaptive resource hints
    # Stored as "target allocation for next op" (clamped per-pool at assignment time)
    s.ram_hint = {}  # pipeline_id -> float
    s.cpu_hint = {}  # pipeline_id -> float

    # Track OOM retry attempts per pipeline (stop escalating after a few)
    s.oom_retries = {}  # pipeline_id -> int

    # Simple round-robin cursor per queue to prevent pathological re-scans
    s.rr_cursor = {"query": 0, "interactive": 0, "batch": 0}


def _is_oom_error(err) -> bool:
    """Heuristic OOM detector from error object/string."""
    if err is None:
        return False
    try:
        msg = str(err).lower()
    except Exception:
        return False
    return ("oom" in msg) or ("out of memory" in msg) or ("memory" in msg and "exceed" in msg)


def _priority_name(p) -> str:
    if p == Priority.QUERY:
        return "query"
    if p == Priority.INTERACTIVE:
        return "interactive"
    return "batch"


def _enqueue_pipeline(s, p):
    """Add pipeline to its priority queue and initialize hints if needed."""
    pid = p.pipeline_id
    pr = p.priority
    if pr == Priority.QUERY:
        s.q_query.append(p)
    elif pr == Priority.INTERACTIVE:
        s.q_interactive.append(p)
    else:
        s.q_batch.append(p)

    # Initialize conservative hints once per pipeline; actual clamping happens per pool at scheduling time.
    if pid not in s.ram_hint or pid not in s.cpu_hint:
        # Use pool maxima as scale (prefer pool 0 as reference if present)
        if s.executor.num_pools > 0:
            ref_pool = s.executor.pools[0]
            max_ram = float(getattr(ref_pool, "max_ram_pool", 0) or 0)
            max_cpu = float(getattr(ref_pool, "max_cpu_pool", 0) or 0)
        else:
            max_ram = 0.0
            max_cpu = 0.0

        # Conservative initial sizing to reduce OOM failures:
        # Queries and interactive get more RAM; batch slightly less but still substantial.
        if pr == Priority.QUERY:
            s.ram_hint[pid] = max_ram * 0.75 if max_ram > 0 else 1.0
            s.cpu_hint[pid] = max_cpu * 0.70 if max_cpu > 0 else 1.0
        elif pr == Priority.INTERACTIVE:
            s.ram_hint[pid] = max_ram * 0.65 if max_ram > 0 else 1.0
            s.cpu_hint[pid] = max_cpu * 0.60 if max_cpu > 0 else 1.0
        else:
            s.ram_hint[pid] = max_ram * 0.55 if max_ram > 0 else 1.0
            s.cpu_hint[pid] = max_cpu * 0.50 if max_cpu > 0 else 1.0

    if pid not in s.oom_retries:
        s.oom_retries[pid] = 0


def _pipeline_done_or_hard_failed(p) -> bool:
    """Return True if pipeline is successful or contains non-retriable failures."""
    st = p.runtime_status()
    if st.is_pipeline_successful():
        return True
    # If there are FAILED ops, we *may* retry them (OOM only), otherwise consider hard-failed.
    return False


def _select_next_pipeline_and_ops(s, queue, cursor_key, require_parents_complete=True):
    """
    From a FIFO-like queue, find the next pipeline with an assignable ready op.
    Uses a round-robin cursor to avoid repeatedly scanning from head.
    Returns (pipeline, op_list) or (None, []).
    """
    n = len(queue)
    if n == 0:
        return None, []

    start = s.rr_cursor.get(cursor_key, 0) % max(n, 1)
    # Full scan at most once
    for i in range(n):
        idx = (start + i) % n
        p = queue[idx]
        st = p.runtime_status()

        # Drop successful pipelines from queues (cleanup)
        if st.is_pipeline_successful():
            queue.pop(idx)
            # Adjust cursor conservatively
            s.rr_cursor[cursor_key] = idx % max(len(queue), 1) if len(queue) > 0 else 0
            return _select_next_pipeline_and_ops(s, queue, cursor_key, require_parents_complete=require_parents_complete)

        # If pipeline has failures, we still allow retry via ASSIGNABLE_STATES (FAILED should be included)
        ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=require_parents_complete)[:1]
        if ops:
            # Advance cursor to the next position after selected pipeline
            s.rr_cursor[cursor_key] = (idx + 1) % n
            return p, ops

    return None, []


def _clamp(v, lo, hi):
    if hi < lo:
        hi = lo
    if v < lo:
        return lo
    if v > hi:
        return hi
    return v


def _compute_allocation(s, pool, pipeline_priority, pid):
    """
    Compute CPU/RAM allocation for a single op, using per-pipeline hints and per-priority caps.
    Designed to reduce OOMs while keeping concurrency reasonable.
    """
    avail_cpu = float(pool.avail_cpu_pool)
    avail_ram = float(pool.avail_ram_pool)
    max_cpu = float(pool.max_cpu_pool)
    max_ram = float(pool.max_ram_pool)

    # Priority caps: avoid letting a single op consume the whole pool (esp. batch)
    if pipeline_priority == Priority.QUERY:
        cpu_cap = max_cpu * 0.85
        ram_cap = max_ram * 0.90
        cpu_floor = 1.0
        ram_floor = max(1.0, max_ram * 0.20) if max_ram > 0 else 1.0
    elif pipeline_priority == Priority.INTERACTIVE:
        cpu_cap = max_cpu * 0.75
        ram_cap = max_ram * 0.85
        cpu_floor = 1.0
        ram_floor = max(1.0, max_ram * 0.15) if max_ram > 0 else 1.0
    else:
        cpu_cap = max_cpu * 0.60
        ram_cap = max_ram * 0.75
        cpu_floor = 1.0
        ram_floor = max(1.0, max_ram * 0.10) if max_ram > 0 else 1.0

    # Use hint, but clamp to caps and availability
    target_cpu = float(s.cpu_hint.get(pid, cpu_floor))
    target_ram = float(s.ram_hint.get(pid, ram_floor))

    cpu = _clamp(target_cpu, cpu_floor, min(cpu_cap, avail_cpu))
    ram = _clamp(target_ram, ram_floor, min(ram_cap, avail_ram))

    # Final safety: if pool has tiny remaining resources, don't allocate 0.
    if avail_cpu <= 0 or avail_ram <= 0:
        return 0.0, 0.0
    cpu = _clamp(cpu, 1.0, avail_cpu)
    ram = _clamp(ram, 1.0, avail_ram)
    return cpu, ram


@register_scheduler(key="scheduler_low_014")
def scheduler_low_014(s, results: list, pipelines: list):
    """
    Scheduling step:
    1) Enqueue new pipelines into per-priority queues and initialize per-pipeline hints.
    2) Process results; on OOM failures, increase RAM hint (and modestly CPU hint) for that pipeline.
    3) For each pool, assign at most one ready operator:
       - If multiple pools: pool 0 prefers QUERY/INTERACTIVE; other pools prefer BATCH.
       - Spillover: if preferred queues empty, take from next priority to keep utilization high.
    4) No preemption (keeps churn low and avoids wasting work), rely on prioritization + conservative sizing.
    """
    # Enqueue new arrivals
    for p in pipelines:
        _enqueue_pipeline(s, p)

    # Process execution results to adapt hints and clean up queues indirectly via status checks later
    for r in results or []:
        # We only adjust on failures (OOM indicates insufficient RAM sizing)
        try:
            failed = r.failed()
        except Exception:
            failed = False

        if not failed:
            continue

        pid = getattr(r, "pipeline_id", None)
        # Some traces may not include pipeline_id in result; fall back to no-op in that case
        if pid is None:
            continue

        pr = getattr(r, "priority", None)
        if pr is None:
            pr = Priority.BATCH_PIPELINE

        is_oom = _is_oom_error(getattr(r, "error", None))
        if is_oom:
            # Escalate RAM strongly (OOM is catastrophic for objective due to 720s penalty)
            retries = int(s.oom_retries.get(pid, 0))
            if retries < 4:
                s.oom_retries[pid] = retries + 1

                # Use the attempted RAM as a baseline if present; otherwise grow existing hint.
                attempted_ram = float(getattr(r, "ram", 0) or 0)
                current_hint = float(s.ram_hint.get(pid, attempted_ram if attempted_ram > 0 else 1.0))

                # Growth factor depends on priority (queries get more aggressive rescue)
                grow = 1.8 if pr == Priority.QUERY else (1.6 if pr == Priority.INTERACTIVE else 1.5)
                new_ram = max(current_hint * grow, attempted_ram * 1.5 if attempted_ram > 0 else current_hint * grow)

                # Cap by max RAM of the pool where it ran (if available)
                pool_id = int(getattr(r, "pool_id", -1))
                if 0 <= pool_id < s.executor.num_pools:
                    max_ram = float(s.executor.pools[pool_id].max_ram_pool)
                    new_ram = min(new_ram, max_ram)

                s.ram_hint[pid] = new_ram

                # Slight CPU bump to reduce runtime after memory stabilizes
                attempted_cpu = float(getattr(r, "cpu", 0) or 0)
                current_cpu_hint = float(s.cpu_hint.get(pid, attempted_cpu if attempted_cpu > 0 else 1.0))
                cpu_grow = 1.15 if pr != Priority.BATCH_PIPELINE else 1.10
                new_cpu = max(current_cpu_hint * cpu_grow, attempted_cpu)
                if 0 <= pool_id < s.executor.num_pools:
                    max_cpu = float(s.executor.pools[pool_id].max_cpu_pool)
                    new_cpu = min(new_cpu, max_cpu)
                s.cpu_hint[pid] = new_cpu
        else:
            # Non-OOM failure: don't keep retrying blindly; mild CPU bump only.
            attempted_cpu = float(getattr(r, "cpu", 0) or 0)
            pid_cpu = float(s.cpu_hint.get(pid, attempted_cpu if attempted_cpu > 0 else 1.0))
            s.cpu_hint[pid] = pid_cpu * 1.05

    # Early exit when nothing changed (best-effort; queues may still have work)
    if (not pipelines) and (not results) and (len(s.q_query) + len(s.q_interactive) + len(s.q_batch) == 0):
        return [], []

    suspensions = []
    assignments = []

    # Helper to attempt assignment for a given pool with a priority order
    def try_assign_from_order(pool_id, order):
        pool = s.executor.pools[pool_id]
        if pool.avail_cpu_pool <= 0 or pool.avail_ram_pool <= 0:
            return False

        for pr_name in order:
            if pr_name == "query":
                q = s.q_query
                cursor_key = "query"
            elif pr_name == "interactive":
                q = s.q_interactive
                cursor_key = "interactive"
            else:
                q = s.q_batch
                cursor_key = "batch"

            p, ops = _select_next_pipeline_and_ops(s, q, cursor_key, require_parents_complete=True)
            if p is None or not ops:
                continue

            cpu, ram = _compute_allocation(s, pool, p.priority, p.pipeline_id)
            if cpu <= 0 or ram <= 0:
                continue

            assignments.append(
                Assignment(
                    ops=ops,
                    cpu=cpu,
                    ram=ram,
                    priority=p.priority,
                    pool_id=pool_id,
                    pipeline_id=p.pipeline_id,
                )
            )
            return True
        return False

    # Pool preference strategy:
    # - If multiple pools: pool 0 prefers QUERY/INTERACTIVE; others prefer BATCH.
    # - Spillover keeps throughput up when preferred queue empty.
    for pool_id in range(s.executor.num_pools):
        if s.executor.num_pools > 1:
            if pool_id == 0:
                # Protect top priorities: query first, then interactive, then allow batch spillover
                order = ["query", "interactive", "batch"]
            else:
                # Run batch primarily on non-reserved pools; allow interactive/query spillover if batch empty
                order = ["batch", "interactive", "query"]
        else:
            # Single pool: strict priority ordering
            order = ["query", "interactive", "batch"]

        # Assign at most one op per pool per tick to reduce head-of-line blocking and allow reprioritization
        try_assign_from_order(pool_id, order)

    return suspensions, assignments
