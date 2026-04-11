# policy_key: scheduler_est_027
# reasoning_effort: low
# exp: estimation
# model: gpt-5.2-2025-12-11
# llm_cost: 0.050470
# generation_seconds: 69.01
# generated_at: 2026-04-10T10:06:49.218350
@register_scheduler_init(key="scheduler_est_027")
def scheduler_est_027_init(s):
    """
    Memory-aware, priority-first greedy packing scheduler.

    Main ideas:
    - Maintain an active set of arrived pipelines; never drop them intentionally.
    - Prefer QUERY then INTERACTIVE then BATCH, but add mild aging to avoid starvation.
    - Use op.estimate.mem_peak_gb as a noisy hint to:
        * choose a feasible pool (avoid obvious OOM placements),
        * prioritize smaller-memory ready ops when resources are tight.
    - On failures (especially OOM-like), increase a per-pipeline RAM multiplier and retry.
    - Greedily pack multiple assignments per pool per tick while resources remain.
    """
    from collections import deque

    # Active pipelines by id (arrived but not yet successful/terminal)
    s.active = {}  # pipeline_id -> Pipeline

    # Simple aging to prevent starvation (ticks since last successful assignment attempt)
    s.age = {}  # pipeline_id -> int

    # Per-pipeline RAM multiplier for retries (esp. after OOM)
    s.ram_mult = {}  # pipeline_id -> float

    # Track failures to escalate conservatism
    s.fail_count = {}  # pipeline_id -> int

    # Round-robin pointer per priority to spread attention
    s.rr = {
        "query": deque(),
        "interactive": deque(),
        "batch": deque(),
    }

    # Tunables (conservative defaults; estimator is noisy)
    s.headroom = 1.25          # multiply estimate by headroom before requesting RAM
    s.min_base_ram_frac = 0.10 # base request as fraction of pool max RAM when estimate missing
    s.max_ram_mult = 6.0       # cap for ram multiplier on repeated failures
    s.aging_boost = 0.015      # per-tick aging boost for lower priorities (small to keep p99 good)
    s.oom_boost = 1.8          # multiplier step on OOM-like failure
    s.fail_boost = 1.25        # multiplier step on other failure
    s.try_hard_after_fails = 3 # after this many fails, request very conservative (near-max) RAM/CPU


def _priority_key(priority):
    # Higher is better
    try:
        if priority == Priority.QUERY:
            return 3
        if priority == Priority.INTERACTIVE:
            return 2
        return 1
    except Exception:
        return 1


def _priority_name(priority):
    try:
        if priority == Priority.QUERY:
            return "query"
        if priority == Priority.INTERACTIVE:
            return "interactive"
        return "batch"
    except Exception:
        return "batch"


def _safe_est_mem_gb(op):
    est = getattr(op, "estimate", None)
    if est is None:
        return None
    val = getattr(est, "mem_peak_gb", None)
    if val is None:
        return None
    try:
        # Guard against pathological values
        if val != val:  # NaN
            return None
        if val < 0:
            return None
        return float(val)
    except Exception:
        return None


def _is_oom_error(err):
    if err is None:
        return False
    try:
        s = str(err).lower()
    except Exception:
        return False
    # Heuristic: match common OOM markers
    return ("oom" in s) or ("out of memory" in s) or ("cuda oom" in s) or ("memoryerror" in s)


def _pick_ready_op(pipeline):
    status = pipeline.runtime_status()
    has_failures = status.state_counts[OperatorState.FAILED] > 0
    if status.is_pipeline_successful():
        return None, True, False  # (op, done, failed_terminal)
    # We do not treat failures as terminal; we retry with larger RAM.
    # But if pipeline has failures, only FAILED ops (and/or PENDING) are in ASSIGNABLE_STATES.
    op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
    if not op_list:
        # Nothing assignable right now (waiting on parents running/completing)
        return None, False, has_failures
    return op_list[0], False, has_failures


def _compute_request(pool, pipeline, op, ram_mult, fail_count, headroom, min_base_ram_frac, try_hard_after_fails):
    # RAM request:
    # - Use estimate * headroom when available; otherwise base fraction of pool max.
    # - Apply per-pipeline ram_mult which grows on failures.
    # - If many failures, become very conservative (request near-max).
    est_mem = _safe_est_mem_gb(op)
    if fail_count >= try_hard_after_fails:
        # Try-hard mode: maximize chance of completion (avoid 720s penalty).
        # Leave a little headroom in pool to reduce fragmentation.
        ram_req = max(1e-6, pool.max_ram_pool * 0.95)
    else:
        base = pool.max_ram_pool * min_base_ram_frac
        if est_mem is None:
            target = base
        else:
            target = max(base, est_mem * headroom)
        ram_req = target * ram_mult

    # CPU request: prioritize latency for queries/interactive but still allow packing.
    pr = getattr(pipeline, "priority", None)
    if fail_count >= try_hard_after_fails:
        cpu_req = max(1.0, pool.max_cpu_pool * 0.90)
    else:
        if pr == Priority.QUERY:
            cpu_req = max(1.0, pool.max_cpu_pool * 0.75)
        elif pr == Priority.INTERACTIVE:
            cpu_req = max(1.0, pool.max_cpu_pool * 0.55)
        else:
            cpu_req = max(1.0, pool.max_cpu_pool * 0.30)

    # Clamp to pool maxima (avail checked outside)
    if ram_req > pool.max_ram_pool:
        ram_req = pool.max_ram_pool
    if cpu_req > pool.max_cpu_pool:
        cpu_req = pool.max_cpu_pool

    return float(cpu_req), float(ram_req), est_mem


def _best_pool_for_op(s, pipeline, op):
    # Choose the pool that can fit the operator with minimal risk:
    # - Prefer pools where (estimated) memory fits and leaves most remaining RAM.
    # - If estimate missing, treat as unknown and avoid the tiniest pool by default.
    best = None
    best_score = None

    prio = getattr(pipeline, "priority", None)
    pkey = _priority_key(prio)
    pid = getattr(pipeline, "pipeline_id", None)

    ram_mult = s.ram_mult.get(pid, 1.0)
    fail_count = s.fail_count.get(pid, 0)

    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        if pool.avail_cpu_pool <= 0 or pool.avail_ram_pool <= 0:
            continue

        cpu_req, ram_req, est_mem = _compute_request(
            pool=pool,
            pipeline=pipeline,
            op=op,
            ram_mult=ram_mult,
            fail_count=fail_count,
            headroom=s.headroom,
            min_base_ram_frac=s.min_base_ram_frac,
            try_hard_after_fails=s.try_hard_after_fails,
        )

        # Hard feasibility by request
        if cpu_req > pool.avail_cpu_pool or ram_req > pool.avail_ram_pool:
            continue

        # Extra estimator-based guard: if estimate says it likely won't fit even at max,
        # still allow "try-hard" mode after multiple failures to avoid indefinite delay.
        if est_mem is not None and fail_count < s.try_hard_after_fails:
            # If estimate exceeds max usable RAM, it's very likely to OOM; delay for now.
            if est_mem * s.headroom > pool.max_ram_pool * 0.98:
                continue

        # Score: prioritize (1) higher priority, (2) tighter memory ops when tight,
        # and (3) leaving RAM headroom (to reduce future OOM cascades).
        # Lower score is better.
        ram_left = pool.avail_ram_pool - ram_req
        cpu_left = pool.avail_cpu_pool - cpu_req

        # Memory pressure factor: if pool is tight on RAM, emphasize smaller estimates.
        if est_mem is None:
            est_term = pool.max_ram_pool * 0.50  # unknown -> treat as moderate/large
        else:
            est_term = est_mem

        # Convert priority to a negative bias (higher prio => better)
        prio_bias = -1000.0 * pkey

        score = (
            prio_bias
            + 2.5 * est_term
            - 1.0 * ram_left
            - 0.05 * cpu_left
        )

        if best is None or score < best_score:
            best = (pool_id, cpu_req, ram_req)
            best_score = score

    return best  # (pool_id, cpu_req, ram_req) or None


@register_scheduler(key="scheduler_est_027")
def scheduler_est_027(s, results, pipelines):
    """
    Tick procedure:
    1) Ingest new pipelines into active set and RR lists.
    2) Process results: on failure, bump ram_mult (OOM more aggressively).
    3) Greedy packing per pool:
       - Build candidate ready ops across active pipelines.
       - Select best candidates by priority + aging + (estimated) memory footprint.
       - Place each on the best feasible pool; repeat until pools are full or no candidates fit.
    """
    # Add newly arrived pipelines
    for p in pipelines:
        pid = p.pipeline_id
        if pid not in s.active:
            s.active[pid] = p
            s.age[pid] = 0
            s.ram_mult[pid] = 1.0
            s.fail_count[pid] = 0
            s.rr[_priority_name(p.priority)].append(pid)

    # Process results to update retry multipliers and clear completed pipelines
    # Note: We rely on pipeline.runtime_status() for completion; results mainly inform failure type.
    for r in results:
        try:
            pid = getattr(r, "pipeline_id", None)
        except Exception:
            pid = None

        # If pipeline_id isn't available in result, infer from op's pipeline linkage is not exposed;
        # we still can use priority-based heuristics but cannot map. Safest: skip mapping.
        if pid is None:
            continue

        if pid not in s.active:
            continue

        if r.failed():
            s.fail_count[pid] = s.fail_count.get(pid, 0) + 1
            if _is_oom_error(getattr(r, "error", None)):
                s.ram_mult[pid] = min(s.max_ram_mult, s.ram_mult.get(pid, 1.0) * s.oom_boost)
            else:
                s.ram_mult[pid] = min(s.max_ram_mult, s.ram_mult.get(pid, 1.0) * s.fail_boost)

    # Age all active pipelines (small fairness boost)
    for pid in list(s.active.keys()):
        s.age[pid] = s.age.get(pid, 0) + 1

    # Clean up pipelines that are already successful (avoid wasting cycles)
    # Keep failed pipelines (we retry) to avoid 720s penalty for incomplete.
    to_remove = []
    for pid, p in s.active.items():
        st = p.runtime_status()
        if st.is_pipeline_successful():
            to_remove.append(pid)
    for pid in to_remove:
        p = s.active.pop(pid, None)
        s.age.pop(pid, None)
        s.ram_mult.pop(pid, None)
        s.fail_count.pop(pid, None)
        # Remove from RR deques lazily (skip when encountered)

    # Early exit if nothing changed and nothing to do
    if not pipelines and not results and not s.active:
        return [], []

    suspensions = []
    assignments = []

    # Build list of candidate (pipeline, op) pairs that are ready
    candidates = []
    for pid, p in s.active.items():
        op, done, _has_fail = _pick_ready_op(p)
        if done or op is None:
            continue
        pr = p.priority
        pkey = _priority_key(pr)
        est_mem = _safe_est_mem_gb(op)

        # Aging boost: only meaningfully helps BATCH, slightly INTERACTIVE, almost none for QUERY
        # (keep query latency protected).
        age = s.age.get(pid, 0)
        if pr == Priority.BATCH_PIPELINE:
            boost = s.aging_boost * age
        elif pr == Priority.INTERACTIVE:
            boost = 0.5 * s.aging_boost * age
        else:
            boost = 0.1 * s.aging_boost * age

        # Candidate score: higher is better.
        # - Priority dominates.
        # - Prefer smaller estimated memory to pack better / reduce OOM.
        # - Add a mild aging boost.
        if est_mem is None:
            mem_pen = 1.0  # unknown; don't over-penalize
        else:
            mem_pen = 1.0 + 0.15 * est_mem

        score = (100.0 * pkey + boost) / mem_pen
        candidates.append((score, pid, p, op, est_mem))

    # If no ready candidates, nothing to schedule
    if not candidates:
        return suspensions, assignments

    # Sort candidates once; during packing we may rescore lightly by refiltering.
    candidates.sort(key=lambda x: x[0], reverse=True)

    # Greedily pack into pools by repeatedly selecting best candidate that fits somewhere.
    # To avoid O(N^2) explosion, we do bounded passes.
    max_passes = 3 * (len(candidates) + 1)

    used_pipelines_this_tick = set()  # avoid assigning many ops from same pipeline in one tick
    passes = 0

    while passes < max_passes:
        passes += 1
        made_assignment = False

        # Recompute total available resources quickly; if nothing left, stop
        total_avail_cpu = 0.0
        total_avail_ram = 0.0
        for pool_id in range(s.executor.num_pools):
            pool = s.executor.pools[pool_id]
            total_avail_cpu += max(0.0, pool.avail_cpu_pool)
            total_avail_ram += max(0.0, pool.avail_ram_pool)
        if total_avail_cpu <= 0.0 or total_avail_ram <= 0.0:
            break

        # Pick first candidate that can be placed (best-effort)
        picked_idx = None
        picked = None

        for i, (score, pid, p, op, _est_mem) in enumerate(candidates):
            # Spread: don't schedule too many from same pipeline in one tick unless it's high priority
            if pid in used_pipelines_this_tick and p.priority == Priority.BATCH_PIPELINE:
                continue

            choice = _best_pool_for_op(s, p, op)
            if choice is None:
                continue

            pool_id, cpu_req, ram_req = choice
            picked_idx = i
            picked = (pid, p, op, pool_id, cpu_req, ram_req)
            break

        if picked is None:
            break

        pid, p, op, pool_id, cpu_req, ram_req = picked
        pool = s.executor.pools[pool_id]

        # Final guard with current avail (may have changed slightly across loop iterations)
        if cpu_req > pool.avail_cpu_pool or ram_req > pool.avail_ram_pool:
            # Remove this candidate for this tick to avoid spinning
            if picked_idx is not None:
                candidates.pop(picked_idx)
            continue

        # Issue assignment: one op at a time (keeps failure isolation and improves completion rate)
        assignment = Assignment(
            ops=[op],
            cpu=cpu_req,
            ram=ram_req,
            priority=p.priority,
            pool_id=pool_id,
            pipeline_id=p.pipeline_id,
        )
        assignments.append(assignment)
        made_assignment = True

        used_pipelines_this_tick.add(pid)
        s.age[pid] = 0  # reset aging when we make progress

        # Remove picked candidate; next op readiness will be visible on later ticks
        if picked_idx is not None:
            candidates.pop(picked_idx)

        # Stop if nothing else to schedule
        if not candidates:
            break

        if not made_assignment:
            break

    return suspensions, assignments
