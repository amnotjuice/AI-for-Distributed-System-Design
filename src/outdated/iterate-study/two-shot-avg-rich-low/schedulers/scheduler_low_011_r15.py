# source: scheduler_low_011.py
# model: gpt-5.2-2025-12-11
# effort: low
# feedback: rich
# iteration: r15
@register_scheduler_init(key="scheduler_low_011_r15")
def scheduler_low_011_r15_init(s):
    """Iteration r15: priority-aware, multi-assignment packing, with fairness + safe OOM retry.

    Changes vs the prior attempt (based on observed results: great query latency, but interactive/batch starvation
    and low overall completion counts):
      1) Fill each pool with multiple containers per tick (instead of 1) to reduce queueing delay.
      2) Weighted round-robin between QUERY and INTERACTIVE to avoid starving INTERACTIVE.
      3) Do NOT drop pipelines just because they have FAILED ops; FAILED is assignable in this simulator.
         We only drop when we see a non-retriable failure for an op.
      4) Smaller, capped default sizing for high-priority work to increase concurrency; OOM triggers RAM backoff.
      5) Reserve some headroom on the "interactive pool" to protect high-priority latency from batch.
    """
    # Per-priority FIFO queues of pipelines
    s.waiting_queues = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }

    # Learned hints per operator object id (results give us op objects; pipeline_id is not guaranteed on results)
    # op_id -> {"ram": float, "cpu": float}
    s.op_hints = {}

    # op_id -> int (how many times we saw an OOM for this op and decided to retry)
    s.op_oom_retries = {}

    # Terminal failures (non-OOM): if an op lands here, we stop retrying and drop its pipeline.
    s.op_terminal_fail = set()

    # Tuning knobs (kept simple / low-risk)
    s.max_oom_retries_per_op = 3

    # Weighted RR schedule among high-priority classes
    # (2x QUERY : 1x INTERACTIVE) to preserve query latency while making interactive progress.
    s.hp_rr = [Priority.QUERY, Priority.QUERY, Priority.INTERACTIVE]
    s.hp_rr_cursor = 0

    # Pool placement preferences
    s.interactive_pool_id = 0

    # Per-priority default resource sizing:
    # - Use modest slices (caps) to increase concurrency and reduce queueing.
    # - RAM > minimum doesn't help; we start smaller and only grow on OOM.
    s.req_cfg = {
        Priority.QUERY: {"cpu_frac": 0.25, "ram_frac": 0.25, "cpu_cap": 4.0},
        Priority.INTERACTIVE: {"cpu_frac": 0.35, "ram_frac": 0.25, "cpu_cap": 6.0},
        Priority.BATCH_PIPELINE: {"cpu_frac": 0.50, "ram_frac": 0.50, "cpu_cap": 8.0},
    }

    # Headroom reservation on interactive pool when high-priority backlog exists (to prevent batch interference)
    s.hp_reserve_frac = {"cpu": 0.25, "ram": 0.25}

    # Avoid pathological long loops per tick
    s.max_assignments_per_pool_per_tick = 16
    s.max_queue_scan_per_pick = 32


def _is_oom_error(err):
    if err is None:
        return False
    msg = str(err).lower()
    return ("oom" in msg) or ("out of memory" in msg) or ("memoryerror" in msg)


def _op_id(op):
    # In this simulator, operator objects are stable within a run; id(op) is unique enough.
    return id(op)


def _enqueue_pipeline(s, p):
    pr = p.priority if p.priority in s.waiting_queues else Priority.BATCH_PIPELINE
    s.waiting_queues[pr].append(p)


def _pipeline_has_terminal_failure(s, pipeline):
    status = pipeline.runtime_status()
    failed_ops = status.get_ops([OperatorState.FAILED], require_parents_complete=False) or []
    for op in failed_ops:
        if _op_id(op) in s.op_terminal_fail:
            return True
    return False


def _pick_hp_priority(s):
    # Pick next high-priority class in weighted RR order, skipping empties.
    for _ in range(len(s.hp_rr)):
        pr = s.hp_rr[s.hp_rr_cursor]
        s.hp_rr_cursor = (s.hp_rr_cursor + 1) % len(s.hp_rr)
        if s.waiting_queues[pr]:
            return pr
    return None


def _pick_next_pipeline_from_queue(s, pr, assigned_pipelines):
    """Pop a pipeline that is not completed, not terminal-failed, and not already assigned this tick."""
    q = s.waiting_queues[pr]
    scans = 0
    while q and scans < s.max_queue_scan_per_pick:
        scans += 1
        p = q.pop(0)
        if p.pipeline_id in assigned_pipelines:
            # Already scheduled an op for this pipeline in this tick; push back.
            q.append(p)
            continue
        status = p.runtime_status()
        if status.is_pipeline_successful():
            continue
        if _pipeline_has_terminal_failure(s, p):
            continue
        return p
    return None


def _get_next_assignable_op(pipeline):
    status = pipeline.runtime_status()
    ops = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True) or []
    if not ops:
        return None
    return ops[0]


def _compute_request(s, pool, priority, op):
    cfg = s.req_cfg.get(priority, s.req_cfg[Priority.BATCH_PIPELINE])

    # Start from small slices of pool max; cap CPU to avoid giant single-tenant allocations.
    target_cpu = pool.max_cpu_pool * float(cfg["cpu_frac"])
    target_cpu = min(float(cfg.get("cpu_cap", target_cpu)), target_cpu)
    target_cpu = max(1.0, target_cpu)

    target_ram = pool.max_ram_pool * float(cfg["ram_frac"])
    target_ram = max(1.0, target_ram)

    # Apply learned hints (only ever increase relative to our default targets).
    oid = _op_id(op)
    hint = s.op_hints.get(oid)
    if hint:
        if "cpu" in hint:
            target_cpu = max(target_cpu, float(hint["cpu"]))
        if "ram" in hint:
            target_ram = max(target_ram, float(hint["ram"]))

    # Fit within pool availability.
    cpu = min(target_cpu, pool.avail_cpu_pool)
    ram = min(target_ram, pool.avail_ram_pool)

    # Ensure positive.
    cpu = max(1.0, cpu)
    ram = max(1.0, ram)
    return cpu, ram


@register_scheduler(key="scheduler_low_011_r15")
def scheduler_low_011_r15(s, results, pipelines):
    """
    Scheduler step:
      - ingest new pipelines
      - learn from failures (OOM => RAM backoff; non-OOM => terminal)
      - pack multiple assignments per pool per tick with high-priority fairness and interactive headroom
    """
    # Ingest arrivals
    for p in pipelines:
        _enqueue_pipeline(s, p)

    # Learn from results (OOM retry vs terminal failure)
    for r in results:
        failed = False
        try:
            failed = r.failed()
        except Exception:
            failed = getattr(r, "error", None) is not None

        if not failed:
            continue

        ops = getattr(r, "ops", None) or []
        is_oom = _is_oom_error(getattr(r, "error", None))

        if is_oom:
            # Exponential RAM backoff; keep CPU the same as used (or 1.0)
            used_ram = float(getattr(r, "ram", 1.0) or 1.0)
            used_cpu = float(getattr(r, "cpu", 1.0) or 1.0)
            for op in ops:
                oid = _op_id(op)
                retries = int(s.op_oom_retries.get(oid, 0))
                if retries >= s.max_oom_retries_per_op:
                    # Too many OOMs -> treat as terminal to avoid infinite loops
                    s.op_terminal_fail.add(oid)
                    continue

                # Bump RAM hint; cap to a large value (actual pool cap applied at request time)
                prev = s.op_hints.get(oid, {})
                prev_ram = float(prev.get("ram", used_ram))
                new_ram = max(1.0, prev_ram * 2.0)
                s.op_hints[oid] = {"ram": new_ram, "cpu": max(1.0, float(prev.get("cpu", used_cpu)))}
                s.op_oom_retries[oid] = retries + 1
        else:
            # Non-OOM failure: mark op terminal; its pipeline will be dropped when encountered.
            for op in ops:
                s.op_terminal_fail.add(_op_id(op))

    # If nothing changed, exit
    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    # Schedule interactive pool first to reduce tail latency for high-priority work
    pool_order = list(range(s.executor.num_pools))
    if s.executor.num_pools > 1 and s.interactive_pool_id in pool_order:
        pool_order.remove(s.interactive_pool_id)
        pool_order = [s.interactive_pool_id] + pool_order

    # Track pipelines already given an op this tick (to avoid one pipeline dominating placements)
    assigned_pipelines = set()

    def hp_backlog_exists():
        return bool(s.waiting_queues[Priority.QUERY] or s.waiting_queues[Priority.INTERACTIVE])

    for pool_id in pool_order:
        pool = s.executor.pools[pool_id]

        # Local available resources accounting as we pack multiple assignments
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)
        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # On interactive pool, reserve headroom for high-priority work (when backlog exists)
        reserve_cpu = 0.0
        reserve_ram = 0.0
        if pool_id == s.interactive_pool_id and hp_backlog_exists():
            reserve_cpu = float(pool.max_cpu_pool) * float(s.hp_reserve_frac["cpu"])
            reserve_ram = float(pool.max_ram_pool) * float(s.hp_reserve_frac["ram"])

        packed = 0
        while packed < s.max_assignments_per_pool_per_tick:
            if avail_cpu <= 0 or avail_ram <= 0:
                break

            # Determine which priority class to try next.
            pr = _pick_hp_priority(s)
            if pr is None:
                pr = Priority.BATCH_PIPELINE

            # If on interactive pool with HP backlog, avoid scheduling batch that would eat into reserved headroom.
            if (
                pool_id == s.interactive_pool_id
                and hp_backlog_exists()
                and pr == Priority.BATCH_PIPELINE
                and (avail_cpu <= reserve_cpu or avail_ram <= reserve_ram)
            ):
                # Can't place batch without violating headroom; stop packing this pool for now.
                break

            p = _pick_next_pipeline_from_queue(s, pr, assigned_pipelines)
            if p is None:
                # If no pipeline in this class, try other classes in descending importance.
                if pr != Priority.BATCH_PIPELINE:
                    # Try the other HP class before falling back to batch
                    other = Priority.INTERACTIVE if pr == Priority.QUERY else Priority.QUERY
                    p = _pick_next_pipeline_from_queue(s, other, assigned_pipelines)
                    pr = other if p is not None else pr

                if p is None and pr != Priority.BATCH_PIPELINE:
                    p = _pick_next_pipeline_from_queue(s, Priority.BATCH_PIPELINE, assigned_pipelines)
                    pr = Priority.BATCH_PIPELINE if p is not None else pr

            if p is None:
                break

            op = _get_next_assignable_op(p)
            if op is None:
                # Not ready; requeue and keep searching.
                s.waiting_queues[pr].append(p)
                continue

            # Compute request against real pool object (hints rely on pool max), then check fit vs local avail.
            cpu_req, ram_req = _compute_request(s, pool, pr, op)

            # Fit within remaining local availability; also respect interactive headroom for batch.
            cpu_req = min(cpu_req, avail_cpu)
            ram_req = min(ram_req, avail_ram)

            if pr == Priority.BATCH_PIPELINE and pool_id == s.interactive_pool_id and hp_backlog_exists():
                # Ensure we don't dip below reserved headroom.
                if (avail_cpu - cpu_req) < reserve_cpu or (avail_ram - ram_req) < reserve_ram:
                    # Requeue and stop trying to pack batch on this pool.
                    s.waiting_queues[pr].append(p)
                    break

            # If we still can't allocate minimally, requeue and stop (pool too fragmented/empty).
            if cpu_req <= 0 or ram_req <= 0:
                s.waiting_queues[pr].append(p)
                break

            # Emit assignment (one op per container)
            assignments.append(
                Assignment(
                    ops=[op],
                    cpu=cpu_req,
                    ram=ram_req,
                    priority=pr,
                    pool_id=pool_id,
                    pipeline_id=p.pipeline_id,
                )
            )

            packed += 1
            assigned_pipelines.add(p.pipeline_id)

            # Requeue pipeline so future ready ops can be scheduled in later ticks.
            s.waiting_queues[pr].append(p)

            # Update local availability
            avail_cpu -= float(cpu_req)
            avail_ram -= float(ram_req)

    return suspensions, assignments