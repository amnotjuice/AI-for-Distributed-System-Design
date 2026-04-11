# policy_key: scheduler_medium_030
# reasoning_effort: medium
# model: gpt-5.2-2025-12-11
# llm_cost: 0.106581
# generation_seconds: 65.22
# generated_at: 2026-03-14T02:56:35.211813
@register_scheduler_init(key="scheduler_medium_030")
def scheduler_medium_030_init(s):
    """
    Priority-aware FIFO with small, safe improvements over naive FIFO:

    1) Separate waiting queues by priority (QUERY > INTERACTIVE > BATCH).
    2) Simple aging to prevent starvation (batch can be boosted after waiting).
    3) Conservative resource sizing with OOM-aware RAM backoff per (pipeline, op).
    4) Pool choice:
       - high priority: pick pool with most headroom
       - batch: best-fit packing to reduce fragmentation
    5) Soft reservation: keep a fraction of each pool free from batch so
       high-priority arrivals see lower latency.

    Notes:
    - No preemption (insufficient API surface to enumerate running containers).
    - Failed ops are retried if failure looks like OOM, with increased RAM.
    """
    # Active pipelines (avoid duplicate inserts across ticks)
    s.active = {}  # pipeline_id -> Pipeline

    # Per-priority FIFO queues of pipeline_ids
    s.q_query = []
    s.q_interactive = []
    s.q_batch = []

    # Aging: ticks spent waiting (used for priority boost)
    s.wait_age = {}  # pipeline_id -> int

    # Retry tracking per op
    # op_key = (pipeline_id, op_identifier)
    s.op_retries = {}       # op_key -> int
    s.op_ram_factor = {}    # op_key -> float (multiplier applied to base RAM guess)

    # A small base guess; adjusted on OOM
    s.base_ram_guess = 1.0  # GB-like units (sim depends on executor units)
    s.base_cpu_query = 2.0
    s.base_cpu_interactive = 2.0
    s.base_cpu_batch = 1.0

    # Policy knobs (kept simple and conservative)
    s.max_retries_per_op = 5
    s.aging_threshold = 25  # ticks before boosting a waiting pipeline
    s.batch_reserve_frac_cpu = 0.20
    s.batch_reserve_frac_ram = 0.20

    # OOM backoff
    s.oom_ram_backoff = 1.6
    s.oom_ram_cap_frac_pool = 0.90  # don't request more than this fraction of pool RAM

    # Assignment granularity
    s.max_assignments_per_pool_per_tick = 4


def _priority_rank(priority):
    # Lower is higher priority
    if priority == Priority.QUERY:
        return 0
    if priority == Priority.INTERACTIVE:
        return 1
    return 2  # Priority.BATCH_PIPELINE (and any unknown)


def _queue_for_priority(s, priority):
    if priority == Priority.QUERY:
        return s.q_query
    if priority == Priority.INTERACTIVE:
        return s.q_interactive
    return s.q_batch


def _safe_remove_from_queue(q, pipeline_id):
    # Small helper to avoid ValueError spam
    try:
        q.remove(pipeline_id)
    except ValueError:
        pass


def _is_oom_error(err):
    if err is None:
        return False
    msg = str(err).lower()
    return ("oom" in msg) or ("out of memory" in msg) or ("memory" in msg and "alloc" in msg)


def _op_identifier(op):
    # Robust-ish identifier; simulator may expose op_id/name; fallback to repr(op)
    oid = getattr(op, "op_id", None)
    if oid is not None:
        return ("op_id", oid)
    name = getattr(op, "name", None)
    if name is not None:
        return ("name", name)
    # As a last resort, repr(op) should be stable within a single simulation process
    return ("repr", repr(op))


def _effective_priority_with_aging(s, pipeline):
    """
    Apply a single-step boost if a pipeline has been waiting "too long".
    QUERY stays QUERY; INTERACTIVE may be boosted to QUERY; BATCH may be boosted to INTERACTIVE.
    """
    pr = pipeline.priority
    age = s.wait_age.get(pipeline.pipeline_id, 0)
    if age < s.aging_threshold:
        return pr

    # Boost at most one level to keep behavior predictable
    if pr == Priority.BATCH_PIPELINE:
        return Priority.INTERACTIVE
    if pr == Priority.INTERACTIVE:
        return Priority.QUERY
    return pr


def _pick_pool_for_assignment(s, priority, cpu_req, ram_req):
    """
    Pool selection:
      - High priority: choose pool with maximum headroom (RAM-first, then CPU)
      - Batch: best-fit on RAM to reduce fragmentation
    """
    best_pool = None

    if priority in (Priority.QUERY, Priority.INTERACTIVE):
        best_score = None
        for pool_id in range(s.executor.num_pools):
            pool = s.executor.pools[pool_id]
            if pool.avail_cpu_pool < cpu_req or pool.avail_ram_pool < ram_req:
                continue
            # Headroom score: prefer high RAM and CPU availability
            score = (pool.avail_ram_pool, pool.avail_cpu_pool)
            if best_score is None or score > best_score:
                best_score = score
                best_pool = pool_id
        return best_pool

    # Batch best-fit on RAM leftover
    best_leftover = None
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        if pool.avail_cpu_pool < cpu_req or pool.avail_ram_pool < ram_req:
            continue
        leftover = pool.avail_ram_pool - ram_req
        if best_leftover is None or leftover < best_leftover:
            best_leftover = leftover
            best_pool = pool_id
    return best_pool


def _compute_request(s, pool, pipeline, op, eff_priority):
    """
    Compute a conservative cpu/ram request:
      - RAM: start at base guess * per-op multiplier; cap to fraction of pool
      - CPU: small base per priority; if plenty of slack and no apparent contention, can grow modestly
    """
    pid = pipeline.pipeline_id
    op_key = (pid, _op_identifier(op))
    ram_factor = s.op_ram_factor.get(op_key, 1.0)

    # RAM request
    ram_req = s.base_ram_guess * ram_factor
    ram_req = min(ram_req, max(0.1, pool.max_ram_pool * s.oom_ram_cap_frac_pool))

    # CPU request by priority
    if eff_priority == Priority.QUERY:
        base_cpu = s.base_cpu_query
        # If pool is large and idle, allow a bit more to reduce tail latency
        cpu_req = min(pool.avail_cpu_pool, max(base_cpu, 0.50 * pool.max_cpu_pool))
    elif eff_priority == Priority.INTERACTIVE:
        base_cpu = s.base_cpu_interactive
        cpu_req = min(pool.avail_cpu_pool, max(base_cpu, 0.33 * pool.max_cpu_pool))
    else:
        base_cpu = s.base_cpu_batch
        cpu_req = min(pool.avail_cpu_pool, max(base_cpu, 0.25 * pool.max_cpu_pool))

    # Ensure we never request 0
    cpu_req = max(0.1, float(cpu_req))
    ram_req = max(0.1, float(ram_req))
    return cpu_req, ram_req


@register_scheduler(key="scheduler_medium_030")
def scheduler_medium_030(s, results, pipelines):
    """
    Scheduler step:
      - Ingest new pipelines, enqueue by priority.
      - Process results to learn from OOMs (increase RAM multiplier on retry).
      - Build assignments by scanning queues in effective priority order (with aging),
        applying a small batch reservation in each pool.
    """
    suspensions = []
    assignments = []

    # 1) Ingest new pipelines
    for p in pipelines:
        pid = p.pipeline_id
        if pid in s.active:
            continue
        s.active[pid] = p
        s.wait_age[pid] = 0
        _queue_for_priority(s, p.priority).append(pid)

    # Early exit if no new information (still allow aging to advance only when we do work)
    if not pipelines and not results:
        return suspensions, assignments

    # 2) Process execution results (OOM-aware RAM backoff)
    for r in results:
        # If we cannot associate to a pipeline/op safely, skip learning
        # ExecutionResult exposes .ops (list); we use the first op as the "key"
        if not getattr(r, "ops", None):
            continue
        op0 = r.ops[0]
        pipeline_id = getattr(r, "pipeline_id", None)  # may not exist; fallback below

        # Best-effort pipeline association:
        # - If result has pipeline_id, use it
        # - Else we cannot reliably map; skip
        if pipeline_id is None:
            continue

        op_key = (pipeline_id, _op_identifier(op0))

        if r.failed():
            retries = s.op_retries.get(op_key, 0) + 1
            s.op_retries[op_key] = retries

            # Only apply RAM backoff on OOM-ish failures; otherwise keep retry count but no change
            if _is_oom_error(getattr(r, "error", None)):
                cur = s.op_ram_factor.get(op_key, 1.0)
                s.op_ram_factor[op_key] = min(cur * s.oom_ram_backoff, 64.0)  # avoid runaway

    # 3) Cleanup completed or terminally failed pipelines + update aging
    to_remove = []
    for pid, p in list(s.active.items()):
        status = p.runtime_status()
        if status.is_pipeline_successful():
            to_remove.append(pid)
            continue

        # If there are failed ops with too many retries, drop the pipeline
        failed_ops = status.get_ops([OperatorState.FAILED], require_parents_complete=False)
        terminal = False
        for op in failed_ops:
            op_key = (pid, _op_identifier(op))
            if s.op_retries.get(op_key, 0) >= s.max_retries_per_op:
                terminal = True
                break
        if terminal:
            to_remove.append(pid)
            continue

        # Still active: age it (only when we had some event this tick)
        s.wait_age[pid] = s.wait_age.get(pid, 0) + 1

    for pid in to_remove:
        p = s.active.get(pid)
        if p is not None:
            _safe_remove_from_queue(s.q_query, pid)
            _safe_remove_from_queue(s.q_interactive, pid)
            _safe_remove_from_queue(s.q_batch, pid)
        s.active.pop(pid, None)
        s.wait_age.pop(pid, None)

    # 4) Build a "selection order" list of pipeline_ids based on effective priority + FIFO within class
    #    (We do this per tick to incorporate aging boosts without reshuffling the underlying queues too much.)
    boosted_query = []
    boosted_interactive = []
    boosted_batch = []

    # Preserve FIFO order within each underlying queue
    for pid in s.q_query:
        p = s.active.get(pid)
        if p is None:
            continue
        boosted_query.append(pid)

    for pid in s.q_interactive:
        p = s.active.get(pid)
        if p is None:
            continue
        eff = _effective_priority_with_aging(s, p)
        if eff == Priority.QUERY:
            boosted_query.append(pid)
        else:
            boosted_interactive.append(pid)

    for pid in s.q_batch:
        p = s.active.get(pid)
        if p is None:
            continue
        eff = _effective_priority_with_aging(s, p)
        if eff == Priority.QUERY:
            boosted_query.append(pid)
        elif eff == Priority.INTERACTIVE:
            boosted_interactive.append(pid)
        else:
            boosted_batch.append(pid)

    selection_order = boosted_query + boosted_interactive + boosted_batch

    # 5) For each pool, assign up to N runnable ops, honoring batch reservation
    #    We do a simple scan over selection_order and pick the first runnable op we can fit.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)

        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # Soft reservation for batch: compute "effective" available for batch work
        reserve_cpu = float(pool.max_cpu_pool) * float(s.batch_reserve_frac_cpu)
        reserve_ram = float(pool.max_ram_pool) * float(s.batch_reserve_frac_ram)

        assignments_made = 0

        while assignments_made < s.max_assignments_per_pool_per_tick:
            made_one = False

            for pid in selection_order:
                pipeline = s.active.get(pid)
                if pipeline is None:
                    continue

                status = pipeline.runtime_status()
                if status.is_pipeline_successful():
                    continue

                # Runnable ops: allow PENDING and FAILED, but only when parents complete
                op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
                if not op_list:
                    continue
                op = op_list[0]

                eff_pr = _effective_priority_with_aging(s, pipeline)

                # Compute request based on THIS pool (so we can compare to availability)
                cpu_req, ram_req = _compute_request(s, pool, pipeline, op, eff_pr)

                # Apply batch reservation (QUERY/INTERACTIVE can use full availability)
                if eff_pr == Priority.BATCH_PIPELINE:
                    eff_avail_cpu = max(0.0, avail_cpu - reserve_cpu)
                    eff_avail_ram = max(0.0, avail_ram - reserve_ram)
                else:
                    eff_avail_cpu = avail_cpu
                    eff_avail_ram = avail_ram

                if cpu_req > eff_avail_cpu or ram_req > eff_avail_ram:
                    continue

                # Pool selection: ensure this is a good pool for the job (avoid placing high-prio on tiny headroom pool)
                # If this pool isn't the best choice, skip to let the best pool loop take it.
                chosen_pool = _pick_pool_for_assignment(s, eff_pr, cpu_req, ram_req)
                if chosen_pool is None:
                    continue
                if chosen_pool != pool_id:
                    continue

                # Make assignment
                assignments.append(
                    Assignment(
                        ops=[op],
                        cpu=cpu_req,
                        ram=ram_req,
                        priority=pipeline.priority,  # keep original priority on the assignment
                        pool_id=pool_id,
                        pipeline_id=pipeline.pipeline_id,
                    )
                )

                # Update local available resources for subsequent packing this tick
                avail_cpu -= cpu_req
                avail_ram -= ram_req
                assignments_made += 1
                made_one = True
                break  # restart scan from highest effective priority

            if not made_one:
                break

    return suspensions, assignments
