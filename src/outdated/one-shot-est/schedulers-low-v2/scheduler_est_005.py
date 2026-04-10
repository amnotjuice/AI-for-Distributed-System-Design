# policy_key: scheduler_est_005
# reasoning_effort: low
# model: gpt-5.2-2025-12-11
# llm_cost: 0.042886
# generation_seconds: 44.35
# generated_at: 2026-04-01T13:39:07.024459
@register_scheduler_init(key="scheduler_est_005")
def scheduler_est_005_init(s):
    """
    Priority-aware FIFO with conservative memory estimation and OOM backoff.

    Improvements over naive FIFO:
      1) Global priority ordering: QUERY > INTERACTIVE > BATCH_PIPELINE
      2) Avoid "give everything to the next op": allocate CPU/RAM with caps so multiple ops can run concurrently
      3) Use op.estimate.mem_peak_gb as a *lower-bound hint* (plus safety margin) to reduce OOMs
      4) If an op fails with OOM, retry it with increased RAM (exponential-ish backoff)
      5) Drop pipelines that hit non-OOM failures to avoid infinite retry loops
      6) At most one new op scheduled per pipeline per tick (prevents a single pipeline from dominating)
    """
    # All pipelines waiting for scheduling (we keep them until completed or terminally failed).
    s.waiting_pipelines = []

    # Per-operator memory requirement "learned" from OOMs or used as an override above the estimate.
    # Keyed by (pipeline_id, id(op)).
    s.op_ram_floor_gb = {}

    # Pipelines that we consider terminally failed (non-OOM failures).
    s.dead_pipelines = set()

    # Scheduler tick counter (useful if we later want to add aging).
    s.tick = 0


def _prio_rank(priority):
    # Smaller is higher priority.
    if priority == Priority.QUERY:
        return 0
    if priority == Priority.INTERACTIVE:
        return 1
    return 2  # Priority.BATCH_PIPELINE (and anything else)


def _is_oom_error(err):
    if err is None:
        return False
    e = str(err).lower()
    return ("oom" in e) or ("out of memory" in e) or ("cuda out of memory" in e) or ("memoryerror" in e)


def _clamp(x, lo, hi):
    if x < lo:
        return lo
    if x > hi:
        return hi
    return x


def _compute_ram_request_gb(s, pipeline_id, op, pool, priority, avail_ram):
    """
    Compute RAM to allocate:
      - Start from estimate.mem_peak_gb (if provided) as a lower bound
      - Apply safety margin
      - Respect any learned floor from previous OOMs
      - Cap to a priority-dependent fraction of pool RAM to encourage concurrency
      - Ensure it fits in currently available RAM
    """
    # Default baseline: a small, safe fraction of pool RAM (but not too tiny).
    # (If estimate is absent, this prevents allocating all RAM to one op.)
    if priority == Priority.QUERY:
        base = 0.35 * pool.max_ram_pool
        cap_frac = 0.60
        safety = 1.25
    elif priority == Priority.INTERACTIVE:
        base = 0.25 * pool.max_ram_pool
        cap_frac = 0.50
        safety = 1.20
    else:  # batch
        base = 0.15 * pool.max_ram_pool
        cap_frac = 0.40
        safety = 1.15

    # Use provided estimate as a hint (lower bound), but keep a safety margin.
    est = None
    if hasattr(op, "estimate") and op.estimate is not None and hasattr(op.estimate, "mem_peak_gb"):
        est = op.estimate.mem_peak_gb

    ram_req = base
    if est is not None:
        # Treat as a lower-bound hint; add safety factor and a small constant.
        ram_req = max(ram_req, float(est) * safety + 0.25)

    # Apply learned RAM floor from previous OOMs.
    key = (pipeline_id, id(op))
    if key in s.op_ram_floor_gb:
        ram_req = max(ram_req, s.op_ram_floor_gb[key])

    # Cap to encourage concurrency; still allow larger if it's the only way to run (when pool is empty)
    # but never exceed pool max.
    cap = cap_frac * pool.max_ram_pool
    ram_req = min(ram_req, cap, pool.max_ram_pool)

    # Must fit in available RAM right now.
    ram_req = min(ram_req, avail_ram)

    # Avoid zero/negative allocations.
    ram_req = max(ram_req, 0.01)
    return ram_req


def _compute_cpu_request(s, pool, priority, avail_cpu):
    """
    CPU allocation:
      - High priority gets more CPU (lower latency).
      - Keep caps so we can run multiple ops concurrently.
    """
    # Choose a cap as a fraction of pool CPU to avoid giving all CPU to one op.
    if priority == Priority.QUERY:
        cap_frac = 0.60
        min_cpu = 1.0
    elif priority == Priority.INTERACTIVE:
        cap_frac = 0.50
        min_cpu = 1.0
    else:
        cap_frac = 0.35
        min_cpu = 0.5

    cpu_cap = cap_frac * pool.max_cpu_pool
    cpu_req = min(avail_cpu, cpu_cap)
    cpu_req = max(cpu_req, min_cpu)

    # If we don't actually have min_cpu available, just take what's left (if any).
    cpu_req = min(cpu_req, avail_cpu)
    return cpu_req


@register_scheduler(key="scheduler_est_005")
def scheduler_est_005(s, results, pipelines):
    """
    Priority-aware scheduler with conservative RAM sizing and OOM backoff retries.
    """
    s.tick += 1

    # Incorporate new pipelines.
    for p in pipelines:
        if p.pipeline_id not in s.dead_pipelines:
            s.waiting_pipelines.append(p)

    # Process results to learn from OOM and to mark dead pipelines on non-OOM failures.
    for r in results:
        if not r.failed():
            continue

        # If we can attribute an OOM to the ops, increase their RAM floor for retries.
        if _is_oom_error(r.error):
            # Backoff: grow by 1.6x + 0.5GB (and ensure monotonic increase).
            for op in getattr(r, "ops", []) or []:
                key = (getattr(r, "pipeline_id", None), id(op))
                # pipeline_id may not exist on result; fall back to storing by op id only in that case.
                if key[0] is None:
                    key = (-1, id(op))

                prev = s.op_ram_floor_gb.get(key, 0.0)
                bumped = max(prev, float(getattr(r, "ram", 0.0)) * 1.6 + 0.5)
                s.op_ram_floor_gb[key] = bumped
        else:
            # Non-OOM failure: avoid infinite loops by marking the pipeline as dead.
            pid = getattr(r, "pipeline_id", None)
            if pid is not None:
                s.dead_pipelines.add(pid)

    # Early exit if nothing changed.
    if not pipelines and not results:
        return [], []

    # Filter out completed or dead pipelines; keep others.
    live = []
    for p in s.waiting_pipelines:
        if p.pipeline_id in s.dead_pipelines:
            continue
        st = p.runtime_status()
        if st.is_pipeline_successful():
            continue
        # If it has FAILED ops due to non-OOM errors, we may not detect it here; we handle via results above.
        live.append(p)
    s.waiting_pipelines = live

    suspensions = []
    assignments = []

    # Global order: prioritize higher priority pipelines first; stable by arrival order.
    # (We keep it simple: resort every tick; small overhead in simulation.)
    s.waiting_pipelines.sort(key=lambda p: _prio_rank(p.priority))

    # We'll schedule at most one op per pipeline per tick to reduce head-of-line blocking and improve fairness.
    scheduled_this_tick = set()

    # Build a list of candidate (pipeline, op) pairs in priority order.
    candidates = []
    for p in s.waiting_pipelines:
        if p.pipeline_id in scheduled_this_tick:
            continue
        if p.pipeline_id in s.dead_pipelines:
            continue
        st = p.runtime_status()
        # Only schedule ops whose parents are complete.
        ready_ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        if not ready_ops:
            continue
        # Only consider one ready op per pipeline (the first) to reduce per-pipeline burstiness.
        candidates.append((p, ready_ops[0]))

    # Try to place candidates across pools, prioritizing fitting high-priority work first.
    # We loop pools and greedily assign first-fit by priority order.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool
        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        i = 0
        while i < len(candidates) and avail_cpu > 0 and avail_ram > 0:
            p, op = candidates[i]

            # Skip if already scheduled (could happen if we later extend logic).
            if p.pipeline_id in scheduled_this_tick:
                candidates.pop(i)
                continue

            # Compute resource request with conservative caps.
            cpu_req = _compute_cpu_request(s, pool, p.priority, avail_cpu)
            if cpu_req <= 0:
                break

            ram_req = _compute_ram_request_gb(s, p.pipeline_id, op, pool, p.priority, avail_ram)
            if ram_req <= 0:
                i += 1
                continue

            # If it doesn't fit, try next candidate (maybe a smaller one fits).
            if cpu_req > avail_cpu or ram_req > avail_ram:
                i += 1
                continue

            # Make assignment.
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

            # Update available resources and mark pipeline as scheduled this tick.
            avail_cpu -= cpu_req
            avail_ram -= ram_req
            scheduled_this_tick.add(p.pipeline_id)

            # Remove this candidate; keep scanning from same index.
            candidates.pop(i)

    return suspensions, assignments
