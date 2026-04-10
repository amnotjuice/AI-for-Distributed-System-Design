# policy_key: scheduler_est_015
# reasoning_effort: low
# model: gpt-5.2-2025-12-11
# llm_cost: 0.052798
# generation_seconds: 40.26
# generated_at: 2026-04-01T13:46:26.687009
@register_scheduler_init(key="scheduler_est_015")
def scheduler_est_015_init(s):
    """Priority-aware, estimate-aware scheduler with conservative admission.

    Improvements over naive FIFO:
      - Separate waiting queues by priority; always consider higher priority first.
      - Pack multiple runnable operators per tick per pool (not just one), while keeping headroom.
      - Use operator memory estimate (op.estimate.mem_peak_gb) and OOM feedback to size RAM conservatively.
      - Reserve some CPU/RAM capacity for high-priority work by limiting how much batch can consume.
      - Simple fairness: small per-priority round-robin within each class by rotating pipelines.
    """
    # Waiting pipelines per priority (store Pipeline objects)
    s.wait_q = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }

    # Per-operator RAM floor derived from past OOMs (keyed by op object)
    s.op_oom_ram_floor_gb = {}

    # Per-pipeline rotation indices for fairness within priority
    s.rr_cursor = {
        Priority.QUERY: 0,
        Priority.INTERACTIVE: 0,
        Priority.BATCH_PIPELINE: 0,
    }

    # Config knobs (keep conservative / simple; evolve later)
    s.cfg = {
        # Keep headroom to reduce fragmentation/latency spikes
        "pool_headroom_cpu_frac": 0.05,
        "pool_headroom_ram_frac": 0.05,
        # How much of each pool batch is allowed to consume (leave room for arrivals)
        "batch_max_pool_cpu_frac": 0.70,
        "batch_max_pool_ram_frac": 0.70,
        # Per-op CPU caps by priority (avoid letting single batch op grab whole pool)
        "cpu_cap": {
            Priority.QUERY: 8.0,
            Priority.INTERACTIVE: 6.0,
            Priority.BATCH_PIPELINE: 4.0,
        },
        # Per-op minimum CPU allocation to prevent extreme slowdown (if available)
        "cpu_min": {
            Priority.QUERY: 1.0,
            Priority.INTERACTIVE: 1.0,
            Priority.BATCH_PIPELINE: 1.0,
        },
        # Extra RAM cushion above estimate/floor (GB)
        "ram_cushion_gb": {
            Priority.QUERY: 0.5,
            Priority.INTERACTIVE: 0.5,
            Priority.BATCH_PIPELINE: 0.25,
        },
        # When OOM happens, multiply last allocation to retry
        "oom_retry_mult": 2.0,
        # Clamp to avoid requesting absurdly small/negative RAM
        "ram_min_gb": 0.25,
    }


def _is_oom_error(err) -> bool:
    if err is None:
        return False
    try:
        msg = str(err).lower()
    except Exception:
        return False
    return ("oom" in msg) or ("out of memory" in msg) or ("memory" in msg and "killed" in msg)


def _get_op_mem_est_gb(op):
    # Estimator interface: op.estimate.mem_peak_gb (float or None)
    est = None
    try:
        estimate_obj = getattr(op, "estimate", None)
        if estimate_obj is not None:
            est = getattr(estimate_obj, "mem_peak_gb", None)
    except Exception:
        est = None
    if est is None:
        return None
    try:
        est_f = float(est)
        if est_f > 0:
            return est_f
    except Exception:
        return None
    return None


def _pipeline_is_done_or_failed(pipeline) -> bool:
    st = pipeline.runtime_status()
    if st.is_pipeline_successful():
        return True
    # If any operator is FAILED, we still keep the pipeline: FAILED is assignable for retries.
    return False


def _rotate_list_in_place(lst, k):
    if not lst:
        return
    k = k % len(lst)
    if k:
        lst[:] = lst[k:] + lst[:k]


def _pop_next_runnable_op_from_priority_queue(s, prio):
    """Return (pipeline, [op]) or (None, []) if none runnable.
    Rotates pipelines for simple RR fairness within the priority class.
    """
    q = s.wait_q[prio]
    if not q:
        return None, []

    # Rotate queue by cursor to ensure fairness across ticks
    cur = s.rr_cursor.get(prio, 0)
    if cur:
        _rotate_list_in_place(q, cur)
        s.rr_cursor[prio] = 0

    requeue = []
    chosen_pipeline = None
    chosen_ops = []

    # Scan pipelines once; pick first with runnable ops
    while q:
        p = q.pop(0)
        if _pipeline_is_done_or_failed(p):
            continue

        st = p.runtime_status()
        op_list = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
        if op_list:
            chosen_pipeline = p
            chosen_ops = op_list
            # Put it back at end to round-robin for next time
            requeue.append(p)
            break
        else:
            # Not runnable yet; keep it for later
            requeue.append(p)

    # Restore remaining pipelines plus scanned ones
    q.extend(requeue)
    return chosen_pipeline, chosen_ops


def _compute_ram_request_gb(s, op, priority, pool_avail_ram_gb):
    """Conservative RAM sizing:
      max(oom_floor, estimate) + cushion, clamped to pool availability.
    """
    cfg = s.cfg
    floor = s.op_oom_ram_floor_gb.get(op, None)
    est = _get_op_mem_est_gb(op)

    base = None
    if floor is not None and est is not None:
        base = max(float(floor), float(est))
    elif floor is not None:
        base = float(floor)
    elif est is not None:
        base = float(est)

    if base is None:
        # No knowledge: request a small-but-nontrivial slice of available RAM to reduce OOM risk,
        # but do not starve the pool.
        base = max(cfg["ram_min_gb"], min(2.0, 0.25 * float(pool_avail_ram_gb)))

    ram = base + cfg["ram_cushion_gb"].get(priority, 0.25)
    ram = max(cfg["ram_min_gb"], ram)
    ram = min(float(pool_avail_ram_gb), ram)
    return ram


def _compute_cpu_request(s, priority, pool_avail_cpu):
    cfg = s.cfg
    cpu_cap = float(cfg["cpu_cap"].get(priority, 4.0))
    cpu_min = float(cfg["cpu_min"].get(priority, 1.0))
    cpu = min(float(pool_avail_cpu), cpu_cap)
    if cpu < cpu_min and float(pool_avail_cpu) >= cpu_min:
        cpu = cpu_min
    return cpu


@register_scheduler(key="scheduler_est_015")
def scheduler_est_015(s, results, pipelines):
    """
    Priority-aware scheduler:
      1) Enqueue arriving pipelines into per-priority queues.
      2) Update RAM floors on OOM failures from results (retry will request more RAM).
      3) For each pool, schedule as many runnable ops as possible:
         - QUERY, then INTERACTIVE, then BATCH
         - Keep small headroom, and limit batch to a fraction of pool capacity.
    """
    # Enqueue new pipelines
    for p in pipelines:
        pr = getattr(p, "priority", Priority.BATCH_PIPELINE)
        if pr not in s.wait_q:
            # Unknown priority: treat as batch
            pr = Priority.BATCH_PIPELINE
        s.wait_q[pr].append(p)

    # Update from execution results (learn from OOM)
    for r in results:
        try:
            if r is not None and r.failed() and _is_oom_error(getattr(r, "error", None)):
                # Grow RAM floor for the involved ops
                last_ram = getattr(r, "ram", None)
                if last_ram is None:
                    continue
                try:
                    last_ram = float(last_ram)
                except Exception:
                    continue
                retry_ram = max(s.cfg["ram_min_gb"], last_ram * float(s.cfg["oom_retry_mult"]))
                for op in getattr(r, "ops", []) or []:
                    prev = s.op_oom_ram_floor_gb.get(op, None)
                    if prev is None:
                        s.op_oom_ram_floor_gb[op] = retry_ram
                    else:
                        s.op_oom_ram_floor_gb[op] = max(float(prev), retry_ram)
        except Exception:
            # Never let estimator logic crash scheduling
            pass

    # Early exit if nothing changed
    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    # Scheduling loop per pool
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]

        # Available resources with headroom reserved
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)
        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        head_cpu = float(pool.max_cpu_pool) * float(s.cfg["pool_headroom_cpu_frac"])
        head_ram = float(pool.max_ram_pool) * float(s.cfg["pool_headroom_ram_frac"])
        eff_cpu = max(0.0, avail_cpu - head_cpu)
        eff_ram = max(0.0, avail_ram - head_ram)
        if eff_cpu <= 0 or eff_ram <= 0:
            continue

        # Track how much batch we've admitted in this pool this tick (soft cap)
        batch_cpu_cap = float(pool.max_cpu_pool) * float(s.cfg["batch_max_pool_cpu_frac"])
        batch_ram_cap = float(pool.max_ram_pool) * float(s.cfg["batch_max_pool_ram_frac"])
        batch_cpu_used = 0.0
        batch_ram_used = 0.0

        # Greedily fill pool with runnable ops, prioritizing high priority.
        # Stop when remaining resources are too small to be useful.
        while eff_cpu > 0 and eff_ram > 0:
            picked = False

            # Try priorities in order
            for prio in (Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE):
                # Apply batch admission limit to keep capacity for latency-sensitive arrivals
                if prio == Priority.BATCH_PIPELINE:
                    if batch_cpu_used >= batch_cpu_cap or batch_ram_used >= batch_ram_cap:
                        continue

                pipeline, op_list = _pop_next_runnable_op_from_priority_queue(s, prio)
                if not op_list:
                    continue

                # Determine request sizes
                cpu_req = _compute_cpu_request(s, prio, eff_cpu)
                ram_req = _compute_ram_request_gb(s, op_list[0], prio, eff_ram)

                # If batch, also respect soft caps (shrink request if needed)
                if prio == Priority.BATCH_PIPELINE:
                    cpu_left = max(0.0, batch_cpu_cap - batch_cpu_used)
                    ram_left = max(0.0, batch_ram_cap - batch_ram_used)
                    cpu_req = min(cpu_req, cpu_left) if cpu_left > 0 else 0.0
                    ram_req = min(ram_req, ram_left) if ram_left > 0 else 0.0

                # If we can't allocate meaningful resources, skip this pool
                if cpu_req <= 0 or ram_req <= 0:
                    # Put pipeline back for later (already requeued in helper); just stop trying
                    continue

                # Final fit check
                if cpu_req > eff_cpu or ram_req > eff_ram:
                    continue

                assignments.append(
                    Assignment(
                        ops=op_list,
                        cpu=cpu_req,
                        ram=ram_req,
                        priority=prio,
                        pool_id=pool_id,
                        pipeline_id=pipeline.pipeline_id,
                    )
                )

                eff_cpu -= cpu_req
                eff_ram -= ram_req

                if prio == Priority.BATCH_PIPELINE:
                    batch_cpu_used += cpu_req
                    batch_ram_used += ram_req

                picked = True
                break  # go back to highest priority again

            if not picked:
                # No runnable ops fit this pool right now
                break

    return suspensions, assignments
