# policy_key: scheduler_est_011
# reasoning_effort: low
# model: gpt-5.2-2025-12-11
# llm_cost: 0.040641
# generation_seconds: 47.09
# generated_at: 2026-04-03T01:11:51.093492
@register_scheduler_init(key="scheduler_est_011")
def scheduler_est_011_init(s):
    """
    Priority-aware FIFO with simple resource reservation + OOM-adaptive RAM retries.

    Improvements over naive FIFO:
      - Separate waiting queues per priority; always schedule higher priority first.
      - Reserve a small CPU/RAM slice for high priority so batch cannot fully consume the pool.
      - Use op.estimate.mem_peak_gb when present; on OOM failures, retry with increased RAM for that op.
      - Schedule multiple ops per pool per tick (until resources are exhausted) to reduce idle gaps.
    """
    # Per-priority waiting queues (FIFO within each priority).
    s.waiting_queues = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }

    # Per-operator RAM override learned from OOMs: op_obj -> ram_gb to request next time.
    s.op_ram_override_gb = {}

    # Track pipelines we've already enqueued to avoid accidental duplicates if generator resends.
    # (We still allow the same pipeline_id to appear again if the simulator does that intentionally,
    # but this reduces repeated enqueues within a tick.)
    s._seen_pipeline_ids = set()


def _is_oom_error(err) -> bool:
    if err is None:
        return False
    try:
        msg = str(err).lower()
    except Exception:
        return False
    return ("oom" in msg) or ("out of memory" in msg) or ("cuda out of memory" in msg) or ("killed" in msg)


def _priority_order():
    return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]


def _desired_cpu_for_priority(pool, prio, avail_cpu: float) -> float:
    # Scale-up bias for interactive-ish work: give it a larger slice when possible.
    max_cpu = float(pool.max_cpu_pool)
    if prio == Priority.QUERY:
        target = max(1.0, 0.50 * max_cpu)
    elif prio == Priority.INTERACTIVE:
        target = max(1.0, 0.40 * max_cpu)
    else:
        target = max(1.0, 0.25 * max_cpu)
    return max(0.0, min(float(avail_cpu), float(target)))


def _desired_ram_for_op(pool, op, avail_ram: float, override_gb: float = None) -> float:
    # RAM beyond peak doesn't help; allocate close to estimate to improve packing.
    # If we learned an override from OOM, trust it (still clamp to pool/availability).
    max_ram = float(pool.max_ram_pool)

    if override_gb is not None:
        req = float(override_gb)
    else:
        est = None
        try:
            est = getattr(op, "estimate", None)
            est = getattr(est, "mem_peak_gb", None)
        except Exception:
            est = None

        if est is None:
            # Conservative default when we know nothing: take a modest slice of the pool.
            req = max(1.0, 0.25 * max_ram)
        else:
            # Be fairly aggressive: slight headroom only.
            try:
                req = float(est) * 1.10
            except Exception:
                req = max(1.0, 0.25 * max_ram)

    # Clamp to pool max and currently available in pool.
    req = min(req, max_ram)
    req = min(req, float(avail_ram))
    return max(0.0, float(req))


def _has_high_prio_waiting(s) -> bool:
    return bool(s.waiting_queues[Priority.QUERY]) or bool(s.waiting_queues[Priority.INTERACTIVE])


def _pop_next_ready_pipeline(s, prio):
    """
    FIFO scan: pop first pipeline that has a parents-complete assignable op.
    Pipelines without ready ops get rotated to back to avoid head-of-line blocking.
    Returns (pipeline, op_list) or (None, None).
    """
    q = s.waiting_queues[prio]
    if not q:
        return None, None

    # Scan at most len(q) entries to avoid infinite loops.
    n = len(q)
    for _ in range(n):
        pipeline = q.pop(0)
        status = pipeline.runtime_status()

        if status.is_pipeline_successful():
            # Drop completed pipeline.
            continue

        op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
        if op_list:
            return pipeline, op_list

        # Not ready yet; rotate.
        q.append(pipeline)

    return None, None


@register_scheduler(key="scheduler_est_011")
def scheduler_est_011(s, results: list, pipelines: list):
    """
    Scheduler step:
      1) Ingest new pipelines into per-priority queues.
      2) Learn from results: on OOM failures, increase RAM override for failed ops.
      3) For each pool, schedule as many ready ops as possible:
         - Always consider higher priorities first.
         - When batch is scheduled and high-priority is waiting, enforce a small reservation.
    """
    # --- (1) Enqueue new pipelines by priority ---
    for p in pipelines:
        # Best-effort de-dup per scheduler lifetime; if pipeline_id is absent, just enqueue.
        pid = getattr(p, "pipeline_id", None)
        if pid is not None:
            if pid in s._seen_pipeline_ids:
                continue
            s._seen_pipeline_ids.add(pid)
        s.waiting_queues[p.priority].append(p)

    # --- (2) Process execution results to learn OOM and adjust RAM for retry ---
    for r in results:
        if getattr(r, "failed", None) is not None and r.failed() and _is_oom_error(getattr(r, "error", None)):
            # Exponential backoff on RAM for each failed op.
            # If multiple ops are returned, apply to all (common in fused execution).
            prev_ram = float(getattr(r, "ram", 0.0) or 0.0)
            next_ram = max(prev_ram * 2.0, prev_ram + 1.0, 1.0)
            for op in getattr(r, "ops", []) or []:
                old = s.op_ram_override_gb.get(op, 0.0)
                s.op_ram_override_gb[op] = max(float(old), float(next_ram))

    # Early exit if nothing changed that could affect decisions.
    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    high_waiting = _has_high_prio_waiting(s)

    # --- (3) Schedule work per pool ---
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)

        if avail_cpu <= 0.0 or avail_ram <= 0.0:
            continue

        # Reservation to protect latency: keep some headroom if high priority is waiting.
        # Only enforced when trying to schedule batch work.
        reserve_cpu = 0.0
        reserve_ram = 0.0
        if high_waiting:
            reserve_cpu = 0.15 * float(pool.max_cpu_pool)
            reserve_ram = 0.15 * float(pool.max_ram_pool)

        # Attempt to fill the pool with as many assignments as possible.
        # To avoid long loops, cap number of assignments per pool per tick.
        max_assignments = 64
        made = 0

        while made < max_assignments and avail_cpu > 0.0 and avail_ram > 0.0:
            picked = None
            picked_ops = None

            # Pick highest priority pipeline with a ready op.
            for prio in _priority_order():
                pipeline, op_list = _pop_next_ready_pipeline(s, prio)
                if pipeline is not None:
                    picked, picked_ops = pipeline, op_list
                    break

            if picked is None:
                break  # nothing ready anywhere

            prio = picked.priority
            op = picked_ops[0]

            # Enforce reservation only for batch when high priority is waiting.
            eff_avail_cpu = avail_cpu
            eff_avail_ram = avail_ram
            if prio == Priority.BATCH_PIPELINE and high_waiting:
                eff_avail_cpu = max(0.0, avail_cpu - reserve_cpu)
                eff_avail_ram = max(0.0, avail_ram - reserve_ram)

            # If reservation leaves nothing, put the pipeline back and stop scheduling batch in this pool.
            if eff_avail_cpu <= 0.0 or eff_avail_ram <= 0.0:
                s.waiting_queues[prio].insert(0, picked)
                break

            cpu_req = _desired_cpu_for_priority(pool, prio, eff_avail_cpu)
            ram_override = s.op_ram_override_gb.get(op, None)
            ram_req = _desired_ram_for_op(pool, op, eff_avail_ram, override_gb=ram_override)

            # If we cannot allocate meaningful resources, stop.
            if cpu_req <= 0.0 or ram_req <= 0.0:
                s.waiting_queues[prio].insert(0, picked)
                break

            assignments.append(
                Assignment(
                    ops=picked_ops,
                    cpu=cpu_req,
                    ram=ram_req,
                    priority=prio,
                    pool_id=pool_id,
                    pipeline_id=picked.pipeline_id,
                )
            )
            made += 1

            # Update pool-local available resources.
            avail_cpu -= cpu_req
            avail_ram -= ram_req

            # Put the pipeline back so its next ready op can be scheduled in future ticks.
            s.waiting_queues[prio].append(picked)

    return suspensions, assignments
