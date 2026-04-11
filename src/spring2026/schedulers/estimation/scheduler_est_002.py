# policy_key: scheduler_est_002
# reasoning_effort: low
# model: gpt-5.2-2025-12-11
# llm_cost: 0.044725
# generation_seconds: 32.44
# generated_at: 2026-03-31T23:46:26.772309
@register_scheduler_init(key="scheduler_est_002")
def scheduler_est_002_init(s):
    """Priority-aware FIFO with memory-floor sizing and simple OOM backoff.

    Small, incremental improvements over naive FIFO:
      1) Maintain separate FIFO queues per priority; always try higher priority first.
      2) Size RAM to a per-op "floor" using op.estimate.mem_peak_gb when available.
      3) If an op fails (likely OOM), increase its RAM floor (exponential backoff) and requeue.
      4) Avoid allocating all available RAM/CPU to a single op by using per-priority caps,
         improving concurrency and reducing tail latency for interactive work.
    """
    s.wait_q = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }
    # Per (pipeline_id, op_key) RAM floors in GB to avoid repeated OOMs.
    s.ram_floor_gb = {}
    # Track recent failures for backoff.
    s.fail_count = {}  # (pipeline_id, op_key) -> int


def _prio_order():
    # Highest urgency first (treat QUERY as most latency sensitive).
    return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]


def _op_key(pipeline_id, op):
    # Try to use a stable operator id if present; fall back to Python object id.
    oid = getattr(op, "op_id", None)
    if oid is None:
        oid = getattr(op, "operator_id", None)
    if oid is None:
        oid = id(op)
    return (pipeline_id, oid)


def _est_mem_gb(op):
    est = None
    est_obj = getattr(op, "estimate", None)
    if est_obj is not None:
        est = getattr(est_obj, "mem_peak_gb", None)
    if est is None:
        return None
    try:
        est = float(est)
    except Exception:
        return None
    if est <= 0:
        return None
    return est


def _choose_cpu_cap(priority, pool):
    # Cap CPU per assignment to preserve concurrency.
    # High priority gets more CPU per op to reduce latency; batch gets less to reduce interference.
    if priority == Priority.QUERY:
        frac = 0.75
    elif priority == Priority.INTERACTIVE:
        frac = 0.60
    else:
        frac = 0.40
    cap = pool.max_cpu_pool * frac
    # Never cap below 1 vCPU.
    return max(1.0, cap)


def _safety_factor(priority):
    # Slightly larger safety factor for latency-sensitive work.
    if priority == Priority.QUERY:
        return 1.35
    if priority == Priority.INTERACTIVE:
        return 1.25
    return 1.15


@register_scheduler(key="scheduler_est_002")
def scheduler_est_002_scheduler(s, results, pipelines):
    """
    Scheduler step:
      - Ingest new pipelines into priority FIFO queues.
      - Process results: on failures, raise per-op RAM floor and requeue via normal queueing.
      - For each pool, assign as many ready ops as resources allow, prioritizing QUERY/INTERACTIVE.
    """
    # Enqueue new pipelines.
    for p in pipelines:
        s.wait_q[p.priority].append(p)

    # Early exit if nothing to do.
    if not pipelines and not results:
        return [], []

    # Process results to learn from failures (OOM/backoff).
    for r in results:
        if not r.failed():
            continue

        # Best-effort: assume failures are often OOM; increase RAM floor for the op(s) that failed.
        for op in (r.ops or []):
            k = _op_key(r.pipeline_id if hasattr(r, "pipeline_id") else None, op)
            # If pipeline_id isn't on result, fall back to op key without it.
            if k[0] is None:
                k = (getattr(op, "pipeline_id", None), k[1])

            prev_floor = s.ram_floor_gb.get(k, 0.0)
            prev_fail = s.fail_count.get(k, 0)
            s.fail_count[k] = prev_fail + 1

            # Exponential backoff based on what we tried last time, plus a minimum bump.
            tried = getattr(r, "ram", None)
            try:
                tried = float(tried) if tried is not None else None
            except Exception:
                tried = None

            bump_base = tried if (tried is not None and tried > 0) else (prev_floor if prev_floor > 0 else 1.0)
            # Double RAM on each failure, but cap growth per step to keep behavior stable.
            new_floor = max(prev_floor, bump_base * 2.0, bump_base + 1.0)

            # If we have an estimate, ensure floor stays at least that estimate.
            est = _est_mem_gb(op)
            if est is not None:
                new_floor = max(new_floor, est)

            s.ram_floor_gb[k] = new_floor

    suspensions = []
    assignments = []

    # Helper: pop next schedulable pipeline in FIFO order for a given priority.
    def pop_next_pipeline_with_ready_op(priority):
        q = s.wait_q[priority]
        requeue = []
        chosen = None
        chosen_op = None

        while q:
            p = q.pop(0)
            st = p.runtime_status()

            # Drop completed pipelines.
            if st.is_pipeline_successful():
                continue

            # If pipeline has failed ops, we still allow retry (ASSIGNABLE_STATES includes FAILED).
            # But if there are no ready ops, just requeue.
            ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
            if ops:
                chosen = p
                chosen_op = ops[0]
                break
            requeue.append(p)

        # Put back those we couldn't schedule now.
        q[0:0] = requeue
        return chosen, chosen_op

    # Try to schedule across pools; within each pool, keep assigning while resources remain.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)

        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # Keep placing ops until we run out of either CPU or RAM, or nothing is runnable.
        while avail_cpu > 0 and avail_ram > 0:
            picked = None
            picked_op = None
            picked_prio = None

            # Priority first.
            for pr in _prio_order():
                p, op = pop_next_pipeline_with_ready_op(pr)
                if p is not None and op is not None:
                    picked, picked_op, picked_prio = p, op, pr
                    break

            if picked is None:
                break  # nothing runnable in any queue

            # Compute RAM target: max(estimate, learned floor) * safety; then fit in pool.
            k = _op_key(picked.pipeline_id, picked_op)
            floor = float(s.ram_floor_gb.get(k, 0.0))
            est = _est_mem_gb(picked_op)
            base = max(floor, est if est is not None else 1.0)
            ram_target = base * _safety_factor(picked_prio)

            # Avoid consuming the entire pool RAM on a single op unless necessary.
            # Leave some headroom to keep concurrency/latency better for other work.
            if picked_prio in (Priority.QUERY, Priority.INTERACTIVE):
                max_frac = 0.85
            else:
                max_frac = 0.70
            ram_cap = max(1.0, pool.max_ram_pool * max_frac)

            ram = min(avail_ram, ram_target, ram_cap)

            # If we can't satisfy the base requirement, requeue and stop trying this pool.
            # (Other pools might have more RAM headroom.)
            if ram + 1e-9 < min(avail_ram, base):
                # Requeue pipeline to its queue tail.
                s.wait_q[picked_prio].append(picked)
                break

            # Compute CPU target with per-priority cap.
            cpu_cap = _choose_cpu_cap(picked_prio, pool)
            cpu = min(avail_cpu, cpu_cap)
            cpu = max(1.0, cpu) if avail_cpu >= 1.0 else avail_cpu

            # If CPU is too low to schedule, requeue and stop.
            if cpu <= 0:
                s.wait_q[picked_prio].append(picked)
                break

            assignments.append(
                Assignment(
                    ops=[picked_op],
                    cpu=cpu,
                    ram=ram,
                    priority=picked_prio,
                    pool_id=pool_id,
                    pipeline_id=picked.pipeline_id,
                )
            )

            # Decrement local availability to allow multiple placements per tick.
            avail_cpu -= cpu
            avail_ram -= ram

            # Requeue the pipeline (it may have more ops later).
            s.wait_q[picked_prio].append(picked)

    return suspensions, assignments
