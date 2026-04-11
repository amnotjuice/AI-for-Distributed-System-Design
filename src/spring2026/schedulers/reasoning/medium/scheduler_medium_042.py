# policy_key: scheduler_medium_042
# reasoning_effort: medium
# model: gpt-5.2-2025-12-11
# llm_cost: 0.058561
# generation_seconds: 68.51
# generated_at: 2026-03-14T03:15:15.405618
@register_scheduler_init(key="scheduler_medium_042")
def scheduler_medium_042_init(s):
    """
    Priority-aware, right-sizing scheduler (incremental improvement over naive FIFO).

    Key ideas (kept intentionally simple / "small improvements first"):
      1) Maintain separate per-priority round-robin queues (QUERY/INTERACTIVE ahead of BATCH).
      2) Avoid monopolizing an entire pool with a single operator: allocate a reasonable CPU/RAM slice.
      3) Learn RAM needs from OOM failures (exponential backoff) and from successes (record observed RAM).
      4) Keep a small headroom reserve for high-priority work to reduce tail latency under contention.

    Notes:
      - No preemption (sim API does not expose running containers list in the provided template).
      - One operator per pipeline per scheduling tick to reduce bursty interference and improve latency.
    """
    from collections import deque

    # Per-priority pipeline queues (round-robin within each class)
    s.q_query = deque()
    s.q_interactive = deque()
    s.q_batch = deque()

    # Track known pipelines to avoid accidental duplication
    s.known_pipeline_ids = set()

    # Per-operator learned sizing:
    #   key -> {"ram": float, "cpu": float, "oom_retries": int}
    s.op_profile = {}

    # Small knobs (safe defaults)
    s.min_cpu_grant = 1.0
    s.max_cpu_grant = 8.0  # prevent extreme scale-up that starves others
    s.success_ram_safety = 1.10  # keep slight slack over last known good RAM
    s.initial_ram_frac = 0.20  # first-try RAM as a fraction of pool max
    s.initial_cpu_frac_hi = 0.50  # first-try CPU fraction for high priority
    s.initial_cpu_frac_lo = 0.25  # first-try CPU fraction for batch

    # Headroom reservation to protect latency when batch load is high
    s.reserve_cpu_frac_for_hi = 0.20
    s.reserve_ram_frac_for_hi = 0.20


def _sm042_is_oom(err) -> bool:
    if not err:
        return False
    try:
        msg = str(err).lower()
    except Exception:
        return False
    return ("oom" in msg) or ("out of memory" in msg) or ("memoryerror" in msg)


def _sm042_op_key(pipeline, op):
    """
    Build a stable-ish key for an operator inside a pipeline.
    We try common fields but fall back to repr(op).
    """
    for attr in ("op_id", "operator_id", "id", "name"):
        if hasattr(op, attr):
            try:
                return (pipeline.pipeline_id, str(getattr(op, attr)))
            except Exception:
                pass
    try:
        return (pipeline.pipeline_id, repr(op))
    except Exception:
        return (pipeline.pipeline_id, str(type(op)))


def _sm042_enqueue(s, p):
    # Enqueue pipeline into the appropriate priority queue
    if p.priority == Priority.QUERY:
        s.q_query.append(p)
    elif p.priority == Priority.INTERACTIVE:
        s.q_interactive.append(p)
    else:
        s.q_batch.append(p)


def _sm042_has_hi_waiting(s) -> bool:
    return (len(s.q_query) > 0) or (len(s.q_interactive) > 0)


def _sm042_pick_queue_order(s):
    # Strict priority between classes; round-robin within each queue
    return (s.q_query, s.q_interactive, s.q_batch)


def _sm042_pop_next_ready(s, q, assigned_pipeline_ids):
    """
    Round-robin scan: find the next pipeline in queue q with a parent-ready assignable op.
    Returns (pipeline, [op]) or (None, None). Pipelines not ready are rotated to the back.
    """
    if not q:
        return None, None

    # Scan at most len(q) to preserve fairness within the queue
    n = len(q)
    for _ in range(n):
        p = q.popleft()

        # Avoid scheduling multiple ops from the same pipeline in the same tick
        if p.pipeline_id in assigned_pipeline_ids:
            q.append(p)
            continue

        status = p.runtime_status()
        has_failures = status.state_counts[OperatorState.FAILED] > 0

        # Drop completed/failed pipelines instead of re-queueing
        if status.is_pipeline_successful() or has_failures:
            continue

        op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
        if not op_list:
            # Not ready (e.g., waiting on parents); rotate
            q.append(p)
            continue

        # Found ready work; re-queue pipeline for future ops
        q.append(p)
        return p, op_list

    return None, None


def _sm042_compute_grant(s, pool, pipeline_priority, op_key, avail_cpu, avail_ram):
    """
    Decide CPU/RAM for the assignment using:
      - learned profile if available
      - otherwise a conservative initial slice
      - priority affects CPU aggressiveness
    """
    prof = s.op_profile.get(op_key)

    # RAM decision
    if prof and prof.get("ram"):
        target_ram = float(prof["ram"])
    else:
        target_ram = float(pool.max_ram_pool) * float(s.initial_ram_frac)

    # CPU decision
    if prof and prof.get("cpu"):
        target_cpu = float(prof["cpu"])
    else:
        if pipeline_priority in (Priority.QUERY, Priority.INTERACTIVE):
            target_cpu = float(pool.max_cpu_pool) * float(s.initial_cpu_frac_hi)
        else:
            target_cpu = float(pool.max_cpu_pool) * float(s.initial_cpu_frac_lo)

    # Clamp and fit to available
    cpu = max(s.min_cpu_grant, min(float(avail_cpu), min(target_cpu, s.max_cpu_grant)))
    ram = max(0.0, min(float(avail_ram), target_ram))

    # If RAM computed as 0 due to avail, caller will reject
    return cpu, ram


@register_scheduler(key="scheduler_medium_042")
def scheduler_medium_042(s, results: List[ExecutionResult], pipelines: List[Pipeline]) -> Tuple[List[Suspend], List[Assignment]]:
    """
    Priority-aware right-sizing + OOM backoff scheduler.

    Scheduling loop per tick:
      1) Incorporate new pipelines into per-priority queues.
      2) Update per-operator profiles from execution results (OOM => RAM*2; success => record RAM).
      3) For each pool, allocate multiple assignments while resources remain:
           - Serve QUERY then INTERACTIVE then BATCH.
           - Keep headroom reserved for high priority by limiting batch to (avail - reserve).
           - Allocate modest slices; do not hand entire pool to a single op.
    """
    # --- Ingest new pipelines ---
    for p in pipelines:
        if p.pipeline_id in s.known_pipeline_ids:
            continue
        s.known_pipeline_ids.add(p.pipeline_id)
        _sm042_enqueue(s, p)

    # --- Learn from results (only RAM is adjusted aggressively; CPU remains conservative) ---
    if results:
        for r in results:
            # If ops list missing, skip safely
            ops = getattr(r, "ops", None) or []
            for op in ops:
                # We need pipeline_id for key stability; it's available on ExecutionResult
                # via pipeline_id? Not specified. Fall back to embedding container_id if needed.
                # Best effort: attempt r.pipeline_id, otherwise use a per-result surrogate.
                pid = getattr(r, "pipeline_id", None)
                if pid is None:
                    # Use container_id to prevent key collisions; this reduces learning reuse but is safe.
                    pid = f"container:{getattr(r, 'container_id', 'unknown')}"
                    class _TmpP:
                        pipeline_id = pid
                    tmp_p = _TmpP()
                    k = _sm042_op_key(tmp_p, op)
                else:
                    class _TmpP:
                        pipeline_id = pid
                    tmp_p = _TmpP()
                    k = _sm042_op_key(tmp_p, op)

                prof = s.op_profile.get(k, {"ram": None, "cpu": None, "oom_retries": 0})

                if r.failed() and _sm042_is_oom(getattr(r, "error", None)):
                    # Exponential backoff on RAM; cap at pool max if available
                    pool = s.executor.pools[r.pool_id]
                    prev = prof["ram"]
                    if prev is None:
                        # If we don't know what we tried, use reported r.ram as starting point
                        prev = float(getattr(r, "ram", 0.0)) or (float(pool.max_ram_pool) * float(s.initial_ram_frac))
                    new_ram = min(float(pool.max_ram_pool), float(prev) * 2.0)
                    prof["ram"] = new_ram
                    prof["oom_retries"] = int(prof.get("oom_retries", 0)) + 1
                elif not r.failed():
                    # Record last-known-good RAM with safety factor
                    tried_ram = float(getattr(r, "ram", 0.0)) or prof["ram"]
                    if tried_ram:
                        pool = s.executor.pools[r.pool_id]
                        good = min(float(pool.max_ram_pool), float(tried_ram) * float(s.success_ram_safety))
                        # Keep the max good RAM we have seen (avoid flapping too low)
                        prof["ram"] = max(float(prof["ram"] or 0.0), good)
                    # Record CPU similarly, but don't grow aggressively
                    tried_cpu = float(getattr(r, "cpu", 0.0)) or prof["cpu"]
                    if tried_cpu:
                        prof["cpu"] = max(float(prof["cpu"] or 0.0), min(tried_cpu, s.max_cpu_grant))

                s.op_profile[k] = prof

    # Early exit if nothing changed
    if not pipelines and not results:
        return [], []

    suspensions: List[Suspend] = []
    assignments: List[Assignment] = []

    hi_waiting = _sm042_has_hi_waiting(s)

    # --- Place work across pools ---
    assigned_pipeline_ids = set()

    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)

        if avail_cpu <= 0.0 or avail_ram <= 0.0:
            continue

        # Keep headroom if high-priority work is queued
        reserve_cpu = float(pool.max_cpu_pool) * float(s.reserve_cpu_frac_for_hi) if hi_waiting else 0.0
        reserve_ram = float(pool.max_ram_pool) * float(s.reserve_ram_frac_for_hi) if hi_waiting else 0.0

        # We'll continue assigning while we have resources
        # For batch assignments, we restrict to (avail - reserve) to preserve headroom
        while avail_cpu >= s.min_cpu_grant and avail_ram > 0.0:
            made_progress = False

            for q in _sm042_pick_queue_order(s):
                # Determine effective available resources for this priority class
                is_batch_queue = (q is s.q_batch)
                eff_avail_cpu = avail_cpu
                eff_avail_ram = avail_ram
                if is_batch_queue and hi_waiting:
                    eff_avail_cpu = max(0.0, avail_cpu - reserve_cpu)
                    eff_avail_ram = max(0.0, avail_ram - reserve_ram)

                # Not enough headroom to schedule batch safely right now
                if eff_avail_cpu < s.min_cpu_grant or eff_avail_ram <= 0.0:
                    continue

                p, op_list = _sm042_pop_next_ready(s, q, assigned_pipeline_ids)
                if not p or not op_list:
                    continue

                op = op_list[0]
                k = _sm042_op_key(p, op)
                cpu, ram = _sm042_compute_grant(s, pool, p.priority, k, eff_avail_cpu, eff_avail_ram)

                # If we can't even allocate meaningful RAM, skip for now
                if ram <= 0.0 or cpu < s.min_cpu_grant:
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

                assigned_pipeline_ids.add(p.pipeline_id)
                avail_cpu -= cpu
                avail_ram -= ram
                made_progress = True
                break  # re-evaluate from highest priority after each placement

            if not made_progress:
                break

    return suspensions, assignments
