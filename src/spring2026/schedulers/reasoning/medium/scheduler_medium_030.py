# policy_key: scheduler_medium_030
# reasoning_effort: medium
# exp: reasoning
# model: gpt-5.2-2025-12-11
# llm_cost: 0.116939
# generation_seconds: 76.79
# generated_at: 2026-04-09T23:10:04.039598
@register_scheduler_init(key="scheduler_medium_030")
def scheduler_medium_030_init(s):
    """
    Priority + aging + OOM-aware retry scheduler.

    Goals:
      - Protect query/interactive latency with strict priority ordering + mild aging.
      - Avoid catastrophic failures by retrying OOM with exponential RAM backoff (bounded).
      - Avoid starvation (and 720s incomplete penalties) by periodically forcing batch progress.
      - Keep packing simple/robust: one ready operator per assignment (lower OOM coupling).
    """
    import collections

    s.tick = 0

    # Per-priority FIFO queues of pipeline_ids (we keep a map to actual Pipeline objects).
    s.q = {
        Priority.QUERY: collections.deque(),
        Priority.INTERACTIVE: collections.deque(),
        Priority.BATCH_PIPELINE: collections.deque(),
    }
    s.pipelines_by_id = {}

    # Arrival tick for mild aging (prevents indefinite batch starvation).
    s.arrival_tick = {}

    # Per-operator RAM estimate + attempt counts (keyed by a stable op key).
    s.op_ram_est = {}        # op_key -> ram
    s.op_attempts = {}       # op_key -> count
    s.op_to_pipeline = {}    # op_key -> pipeline_id

    # Pipeline-level gating: if we see non-OOM failures for any op, stop retrying that pipeline.
    s.pipeline_non_oom_failed = set()

    # Batch fairness: ensure we schedule at least one batch op every N ticks if any exist.
    s.force_batch_every = 6
    s.last_batch_scheduled_tick = -10


def _sm030_op_key(op):
    """Best-effort stable key for an operator across scheduler calls/results."""
    # Prefer explicit ids if they exist; otherwise fall back to object identity.
    for attr in ("op_id", "operator_id", "id", "name"):
        if hasattr(op, attr):
            v = getattr(op, attr)
            if isinstance(v, (int, str)):
                return (attr, v)
    return ("py_id", id(op))


def _sm030_is_oom(err) -> bool:
    """Detect OOM-ish failures from result.error (string or exception)."""
    if err is None:
        return False
    try:
        msg = str(err).lower()
    except Exception:
        return False
    return ("oom" in msg) or ("out of memory" in msg) or ("cuda out of memory" in msg) or ("memoryerror" in msg)


def _sm030_op_declared_ram_min(op):
    """Try to read an operator-declared RAM minimum if present; else None."""
    for attr in ("ram_min", "min_ram", "minimum_ram", "ram", "memory", "mem"):
        if hasattr(op, attr):
            v = getattr(op, attr)
            try:
                if v is not None and float(v) > 0:
                    return float(v)
            except Exception:
                continue
    return None


def _sm030_base_cpu_for_priority(priority, avail_cpu):
    """Small, conservative CPU sizing to reduce interference; boost for query/interactive."""
    if avail_cpu <= 0:
        return 0
    # Keep requests integer-ish if simulator expects that (safe even if it accepts floats).
    if priority == Priority.QUERY:
        target = 6
    elif priority == Priority.INTERACTIVE:
        target = 3
    else:
        target = 1
    cpu = min(avail_cpu, target)
    # Ensure at least 1 CPU if any is available.
    if cpu < 1 and avail_cpu >= 1:
        cpu = 1
    return cpu


def _sm030_ram_request(s, op, pool, priority):
    """
    RAM sizing:
      - Use declared minimum if available.
      - Otherwise use learned estimate, else a conservative fraction of pool RAM (priority-dependent).
      - Apply OOM backoff via op_ram_est updates.
    """
    op_key = _sm030_op_key(op)

    declared = _sm030_op_declared_ram_min(op)
    learned = s.op_ram_est.get(op_key, None)

    # Conservative default fraction (slightly higher for high priority to reduce OOM retries).
    if priority == Priority.QUERY:
        frac = 0.40
    elif priority == Priority.INTERACTIVE:
        frac = 0.30
    else:
        frac = 0.22

    default_guess = max(1.0, float(pool.max_ram_pool) * frac)

    if declared is not None:
        base = max(float(declared), 1.0)
        # Light headroom above declared minimum to reduce near-threshold OOMs.
        base = base * 1.15
    else:
        base = default_guess

    if learned is not None:
        base = max(base, float(learned))

    # Bound within pool capacity; actual admission checks will also require <= avail.
    base = min(base, float(pool.max_ram_pool))
    return base


def _sm030_priority_order(s):
    """
    Choose priority order. Normally: query > interactive > batch.
    But if batch has been delayed too long, temporarily elevate it.
    """
    batch_waiting = len(s.q[Priority.BATCH_PIPELINE]) > 0
    must_force_batch = batch_waiting and (s.tick - s.last_batch_scheduled_tick >= s.force_batch_every)
    if must_force_batch:
        return [Priority.QUERY, Priority.BATCH_PIPELINE, Priority.INTERACTIVE]
    return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]


def _sm030_pool_preference_score(pool_id, num_pools, priority):
    """
    Bias placement slightly:
      - If multiple pools, prefer pool 0 for query/interactive (lower interference assumptions).
      - Prefer non-0 pools for batch when available to keep pool 0 responsive.
    """
    if num_pools <= 1:
        return 0.0
    if priority in (Priority.QUERY, Priority.INTERACTIVE):
        return 1.0 if pool_id == 0 else 0.0
    # batch
    return 1.0 if pool_id != 0 else 0.0


def _sm030_pick_pool(s, priority, cpu_req, ram_req):
    """Pick the best pool that can fit the request now."""
    best_pool = None
    best_score = None
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        if pool.avail_cpu_pool < cpu_req or pool.avail_ram_pool < ram_req:
            continue
        # Score by headroom + mild bias by pool id.
        cpu_head = float(pool.avail_cpu_pool - cpu_req) / max(1.0, float(pool.max_cpu_pool))
        ram_head = float(pool.avail_ram_pool - ram_req) / max(1.0, float(pool.max_ram_pool))
        bias = _sm030_pool_preference_score(pool_id, s.executor.num_pools, priority)
        score = (0.55 * cpu_head) + (0.45 * ram_head) + (0.10 * bias)
        if best_score is None or score > best_score:
            best_score = score
            best_pool = pool_id
    return best_pool


def _sm030_pop_next_runnable(s, priority, pool):
    """
    Round-robin through queued pipelines of a priority class and find one runnable op that fits.
    Returns (pipeline, op, cpu_req, ram_req) or (None, None, None, None).
    """
    q = s.q[priority]
    if not q:
        return None, None, None, None

    # Search bounded by current queue size to avoid infinite loops.
    n = len(q)
    for _ in range(n):
        pid = q.popleft()
        pipeline = s.pipelines_by_id.get(pid, None)
        if pipeline is None:
            continue

        # Drop terminal pipelines.
        status = pipeline.runtime_status()
        if status.is_pipeline_successful():
            continue
        if pid in s.pipeline_non_oom_failed:
            continue

        # Find next runnable op (parents complete).
        op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
        if not op_list:
            # Not runnable now; keep it in rotation.
            q.append(pid)
            continue

        op = op_list[0]

        # Size request.
        cpu_req = _sm030_base_cpu_for_priority(priority, pool.avail_cpu_pool)
        if cpu_req <= 0:
            q.append(pid)
            continue

        ram_req = _sm030_ram_request(s, op, pool, priority)

        # If it doesn't fit in this pool right now, keep it queued (might fit later or in other pool).
        if ram_req > pool.avail_ram_pool or cpu_req > pool.avail_cpu_pool:
            q.append(pid)
            continue

        # Track op->pipeline mapping for interpreting results later.
        op_key = _sm030_op_key(op)
        s.op_to_pipeline[op_key] = pid

        # Put pipeline back to end for fairness (pipeline likely has more ops after this one).
        q.append(pid)
        return pipeline, op, cpu_req, ram_req

    return None, None, None, None


@register_scheduler(key="scheduler_medium_030")
def scheduler_medium_030(s, results: list, pipelines: list):
    """
    Scheduler step:
      - Incorporate new arrivals into per-priority queues.
      - Update RAM estimates based on OOM failures (exponential backoff) and small decay on success.
      - Create assignments with one op per container; bias pool selection for responsiveness.
      - Ensure batch makes progress periodically to avoid large incomplete penalties.
    """
    s.tick += 1

    suspensions = []
    assignments = []

    # 1) Enqueue new pipelines.
    for p in pipelines:
        s.pipelines_by_id[p.pipeline_id] = p
        if p.pipeline_id not in s.arrival_tick:
            s.arrival_tick[p.pipeline_id] = s.tick
        s.q[p.priority].append(p.pipeline_id)

    # 2) Process results: learn RAM needs and mark non-OOM failures.
    #    We only "retry" failures implicitly by allowing FAILED ops in ASSIGNABLE_STATES.
    if results:
        for r in results:
            # Update per-op estimates/attempts based on ops in this container.
            is_oom = _sm030_is_oom(getattr(r, "error", None))
            failed = False
            try:
                failed = r.failed()
            except Exception:
                failed = False

            ops = getattr(r, "ops", []) or []
            for op in ops:
                op_key = _sm030_op_key(op)
                s.op_attempts[op_key] = s.op_attempts.get(op_key, 0) + (1 if failed else 0)

                if failed and is_oom:
                    # Exponential backoff based on the RAM we just tried, capped by pool max.
                    pool = s.executor.pools[r.pool_id]
                    prev = s.op_ram_est.get(op_key, float(getattr(r, "ram", 1.0)))
                    tried = float(getattr(r, "ram", prev))
                    bumped = max(prev, tried) * 2.0
                    s.op_ram_est[op_key] = min(bumped, float(pool.max_ram_pool))
                elif not failed:
                    # Gentle decay to avoid runaway overallocation (keep safety margin).
                    if op_key in s.op_ram_est:
                        s.op_ram_est[op_key] = max(1.0, float(s.op_ram_est[op_key]) * 0.98)

                # If non-OOM failure, stop retrying that pipeline to prevent churn.
                if failed and (not is_oom):
                    pid = s.op_to_pipeline.get(op_key, None)
                    if pid is not None:
                        s.pipeline_non_oom_failed.add(pid)

                # Prevent infinite OOM retry loops: after too many attempts, stop retrying pipeline.
                if failed and is_oom and s.op_attempts.get(op_key, 0) >= 4:
                    pid = s.op_to_pipeline.get(op_key, None)
                    if pid is not None:
                        s.pipeline_non_oom_failed.add(pid)

    # 3) Scheduling: fill pools with highest-priority runnable work, with periodic batch forcing.
    #    We do not preempt (no stable access to running containers in the provided interface).
    prio_order = _sm030_priority_order(s)

    # To avoid head-of-line blocking, we attempt per pool to schedule multiple containers
    # until the pool can't fit our typical requests.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]

        # Iterate a bounded number of times to avoid long loops when nothing fits.
        # Upper bound: total queued pipelines across priorities.
        max_iters = len(s.q[Priority.QUERY]) + len(s.q[Priority.INTERACTIVE]) + len(s.q[Priority.BATCH_PIPELINE])
        if max_iters <= 0:
            continue
        max_iters = min(max_iters, 64)

        for _ in range(max_iters):
            if pool.avail_cpu_pool <= 0 or pool.avail_ram_pool <= 0:
                break

            scheduled_any = False

            # Try priorities in order, but also consider pool specialization:
            # - If pool 0 and we have query/interactive, keep it responsive.
            # - Allow batch on pool 0 if we are in "force batch" mode.
            for pr in prio_order:
                if not s.q[pr]:
                    continue

                # Batch placement bias: if multiple pools and this is pool 0, prefer to skip batch
                # unless we are in force mode or other queues are empty.
                if pr == Priority.BATCH_PIPELINE and s.executor.num_pools > 1 and pool_id == 0:
                    batch_force = (s.tick - s.last_batch_scheduled_tick >= s.force_batch_every)
                    if not batch_force and (len(s.q[Priority.QUERY]) > 0 or len(s.q[Priority.INTERACTIVE]) > 0):
                        continue

                pipeline, op, cpu_req, ram_req = _sm030_pop_next_runnable(s, pr, pool)
                if pipeline is None:
                    continue

                # As a safety valve, if this pool can't fit after sizing (race with local updates),
                # try to place into a different pool that fits now.
                if cpu_req > pool.avail_cpu_pool or ram_req > pool.avail_ram_pool:
                    alt_pool_id = _sm030_pick_pool(s, pr, cpu_req, ram_req)
                    if alt_pool_id is None:
                        continue
                    # Use alternative pool (do not mutate local `pool`, just emit assignment).
                    assignments.append(
                        Assignment(
                            ops=[op],
                            cpu=cpu_req,
                            ram=ram_req,
                            priority=pipeline.priority,
                            pool_id=alt_pool_id,
                            pipeline_id=pipeline.pipeline_id,
                        )
                    )
                else:
                    # Place in this pool.
                    assignments.append(
                        Assignment(
                            ops=[op],
                            cpu=cpu_req,
                            ram=ram_req,
                            priority=pipeline.priority,
                            pool_id=pool_id,
                            pipeline_id=pipeline.pipeline_id,
                        )
                    )

                if pr == Priority.BATCH_PIPELINE:
                    s.last_batch_scheduled_tick = s.tick

                # Update local view of pool headroom for packing within this scheduler tick.
                pool.avail_cpu_pool -= cpu_req
                pool.avail_ram_pool -= ram_req

                scheduled_any = True
                break  # after scheduling one container, re-evaluate priorities with updated headroom

            if not scheduled_any:
                break

    return suspensions, assignments
