# policy_key: scheduler_est_014
# reasoning_effort: low
# model: gpt-5.2-2025-12-11
# llm_cost: 0.037932
# generation_seconds: 39.94
# generated_at: 2026-03-31T23:54:08.085008
@register_scheduler_init(key="scheduler_est_014")
def scheduler_est_014_init(s):
    """
    Priority-aware, estimate-driven scheduler (small incremental improvements over FIFO).

    Improvements vs naive FIFO:
    - Separate waiting queues by priority (QUERY/INTERACTIVE first, then BATCH).
    - Right-size CPU to improve concurrency (avoid giving entire pool to a single op).
    - Allocate RAM using op.estimate.mem_peak_gb as a floor with small headroom.
    - On suspected OOM failures, increase the remembered RAM floor for that op shape.
    - Soft reservation: when high-priority work is waiting, avoid consuming the whole pool with batch.
    """
    # Queues of pipelines waiting for scheduling consideration
    s.wait_q_high = []  # QUERY + INTERACTIVE
    s.wait_q_low = []   # BATCH_PIPELINE

    # Remembered per-op RAM floors (GB) based on estimates and observed failures
    s.op_mem_floor_gb = {}

    # Track how many times we've bumped memory for a given op key (avoid runaway)
    s.op_mem_bumps = {}

    # Tuning knobs (conservative defaults)
    s.defaults = {
        "default_mem_gb": 2.0,         # if no estimate is present
        "mem_headroom": 1.20,          # add headroom above estimate/floor
        "oom_bump_factor": 1.50,       # multiply floor on suspected OOM
        "oom_bump_cap": 3,             # max number of bumps per op key
        "batch_soft_reserve_cpu": 1.0, # if high-pri waiting, keep at least this many CPUs free
        "batch_soft_reserve_ram_gb": 2.0,  # if high-pri waiting, keep at least this much RAM free
        "high_cpu_cap": 4.0,           # per-op CPU cap for high priority
        "low_cpu_cap": 2.0,            # per-op CPU cap for batch
        "min_cpu": 1.0,
        "min_ram_gb": 0.5,
    }


def _priority_is_high(pri):
    return pri in (Priority.QUERY, Priority.INTERACTIVE)


def _op_key(op):
    # Best-effort stable key across retries; fall back to repr if needed.
    for attr in ("operator_id", "op_id", "id", "name"):
        if hasattr(op, attr):
            v = getattr(op, attr)
            if v is not None:
                return f"{attr}:{v}"
    return f"repr:{repr(op)}"


def _is_suspected_oom(err):
    if err is None:
        return False
    msg = str(err).lower()
    return ("oom" in msg) or ("out of memory" in msg) or ("cuda out of memory" in msg) or ("memoryerror" in msg)


def _get_estimated_mem_gb(op, default_mem_gb):
    est = None
    if hasattr(op, "estimate") and op.estimate is not None and hasattr(op.estimate, "mem_peak_gb"):
        est = op.estimate.mem_peak_gb
    try:
        if est is None:
            return float(default_mem_gb)
        return float(est)
    except Exception:
        return float(default_mem_gb)


def _compute_ram_request_gb(s, op, avail_ram_gb):
    # Use (remembered floor OR estimate OR default) as a floor, then add headroom.
    key = _op_key(op)
    base_floor = s.op_mem_floor_gb.get(key, None)
    est = _get_estimated_mem_gb(op, s.defaults["default_mem_gb"])
    floor = max(est, base_floor) if base_floor is not None else est

    req = max(s.defaults["min_ram_gb"], floor * s.defaults["mem_headroom"])

    # Never request more than available in this pool decision.
    if avail_ram_gb is not None:
        req = min(req, float(avail_ram_gb))

    return req


def _compute_cpu_request(s, pri, pool, avail_cpu, high_waiting):
    # Keep per-op CPU small to improve concurrency & latency under contention.
    cap = s.defaults["high_cpu_cap"] if _priority_is_high(pri) else s.defaults["low_cpu_cap"]
    cap = min(float(cap), float(getattr(pool, "max_cpu_pool", cap) or cap))

    # If high work is waiting, be more conservative with batch.
    if (not _priority_is_high(pri)) and high_waiting:
        cap = min(cap, 1.0)

    req = min(float(avail_cpu), cap)
    req = max(s.defaults["min_cpu"], req) if avail_cpu >= s.defaults["min_cpu"] else 0.0
    return req


def _enqueue_pipeline(s, p):
    if _priority_is_high(p.priority):
        s.wait_q_high.append(p)
    else:
        s.wait_q_low.append(p)


def _requeue_pipeline(s, p):
    # Preserve priority class; append to end for simple fairness within class.
    _enqueue_pipeline(s, p)


def _drain_invalid_pipelines(q):
    # Drop completed/terminally failed pipelines from a queue.
    kept = []
    for p in q:
        status = p.runtime_status()
        has_failures = status.state_counts[OperatorState.FAILED] > 0
        if status.is_pipeline_successful() or has_failures:
            continue
        kept.append(p)
    return kept


@register_scheduler(key="scheduler_est_014")
def scheduler_est_014_scheduler(s, results, pipelines):
    """
    Policy step:
    1) Update per-op memory floors based on failures (suspected OOM => bump).
    2) Enqueue new pipelines by priority.
    3) For each pool, schedule as many ready ops as possible:
       - Always prefer high-priority queues.
       - Apply soft reservation so batch doesn't consume the last resources when high is waiting.
       - Right-size CPU/RAM requests.
    """
    # ---- 1) Learn from results (OOM => bump remembered floor) ----
    for r in results:
        if not getattr(r, "failed", lambda: False)():
            continue
        if not _is_suspected_oom(getattr(r, "error", None)):
            continue

        # Bump based on what we attempted to allocate, if present; otherwise fall back.
        attempted_ram = getattr(r, "ram", None)
        bump_from = None
        try:
            if attempted_ram is not None:
                bump_from = float(attempted_ram)
        except Exception:
            bump_from = None

        for op in getattr(r, "ops", []) or []:
            key = _op_key(op)
            bumps = s.op_mem_bumps.get(key, 0)
            if bumps >= s.defaults["oom_bump_cap"]:
                continue

            est = _get_estimated_mem_gb(op, s.defaults["default_mem_gb"])
            current_floor = s.op_mem_floor_gb.get(key, est)
            base = bump_from if bump_from is not None else current_floor
            new_floor = max(current_floor, base * s.defaults["oom_bump_factor"])

            s.op_mem_floor_gb[key] = new_floor
            s.op_mem_bumps[key] = bumps + 1

    # ---- 2) Enqueue new pipelines ----
    for p in pipelines:
        _enqueue_pipeline(s, p)

    # Clean out completed/failed pipelines from queues
    s.wait_q_high = _drain_invalid_pipelines(s.wait_q_high)
    s.wait_q_low = _drain_invalid_pipelines(s.wait_q_low)

    # Early exit: nothing to do
    if not pipelines and not results and not s.wait_q_high and not s.wait_q_low:
        return [], []

    suspensions = []  # No preemption here (no reliable running-container introspection in provided API)
    assignments = []

    # Helper: pick next schedulable op from a queue (rotating fairness within queue)
    def pop_next_ready_op_from_queue(q):
        # Rotate through pipelines once; if none ready, leave queue order unchanged.
        n = len(q)
        if n == 0:
            return None, None

        for _ in range(n):
            p = q.pop(0)
            status = p.runtime_status()
            has_failures = status.state_counts[OperatorState.FAILED] > 0
            if status.is_pipeline_successful() or has_failures:
                # Drop terminal pipelines
                continue

            # Put back at end; we may schedule from it.
            q.append(p)

            ops = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
            if ops:
                return p, ops[0]

        return None, None

    # ---- 3) Schedule per pool ----
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)

        if avail_cpu <= 0.0 or avail_ram <= 0.0:
            continue

        # If any high-priority pipeline exists, treat it as "high waiting"
        high_waiting = len(s.wait_q_high) > 0

        # Keep scheduling while resources remain
        while avail_cpu >= s.defaults["min_cpu"] and avail_ram >= s.defaults["min_ram_gb"]:
            # Always prefer high priority
            p, op = pop_next_ready_op_from_queue(s.wait_q_high)
            picked_from = "high"
            if op is None:
                p, op = pop_next_ready_op_from_queue(s.wait_q_low)
                picked_from = "low"

            if op is None or p is None:
                break

            # Soft reservation: if high is waiting, don't let batch use the last bit
            if picked_from == "low" and high_waiting:
                if (avail_cpu <= s.defaults["batch_soft_reserve_cpu"]) or (avail_ram <= s.defaults["batch_soft_reserve_ram_gb"]):
                    # Can't safely schedule batch without risking head-of-line blocking for high priority
                    break

            # Compute requests
            cpu_req = _compute_cpu_request(s, p.priority, pool, avail_cpu, high_waiting)
            if cpu_req <= 0.0:
                break

            ram_req = _compute_ram_request_gb(s, op, avail_ram)
            if ram_req < s.defaults["min_ram_gb"]:
                break

            # If we can't fit the RAM floor, skip scheduling further in this pool.
            # (We don't try other pools here; next iteration/pool may have more headroom.)
            if ram_req > avail_ram or cpu_req > avail_cpu:
                break

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

            # Update remaining capacity for additional placements in this pool tick
            avail_cpu -= cpu_req
            avail_ram -= ram_req

    return suspensions, assignments
