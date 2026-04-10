# policy_key: scheduler_est_028
# reasoning_effort: low
# exp: estimation
# model: gpt-5.2-2025-12-11
# llm_cost: 0.050330
# generation_seconds: 50.90
# generated_at: 2026-04-10T10:07:40.123343
@register_scheduler_init(key="scheduler_est_028")
def scheduler_est_028_init(s):
    """
    Memory-aware, priority-first best-fit scheduler with simple retry-on-OOM.

    Core ideas:
      - Maintain per-priority FIFO queues of pipelines; always prefer QUERY then INTERACTIVE then BATCH.
      - Select runnable operators using estimated peak memory as a hint:
          * Prefer smaller estimated-memory ops first (within same effective priority) to reduce fragmentation.
          * Only place an op on pools where it is likely to fit (estimate <= available RAM).
      - Right-size RAM requests using a safety factor on the estimate (higher for high priority).
      - If an op fails with likely OOM, bump its safety factor for subsequent retries to reduce repeat failures.
      - Add mild aging for BATCH so it eventually makes progress (avoid 720s penalties from incompletion).
    """
    from collections import deque

    s.q_query = deque()
    s.q_interactive = deque()
    s.q_batch = deque()

    # Metadata for fairness/aging and placement decisions
    s.step_count = 0
    s.pipeline_meta = {}  # pipeline_id -> {"arrival_step": int}

    # Per-operator adaptive RAM safety multiplier bumps (keyed by (pipeline_id, op_obj_id))
    s.op_ram_mult = {}  # (pipeline_id, op_obj_id) -> float


def _priority_weight(pri):
    # Higher is more important for placement.
    if pri == Priority.QUERY:
        return 10
    if pri == Priority.INTERACTIVE:
        return 5
    return 1


def _base_safety_mult(pri):
    # Higher priority gets more headroom to avoid OOM -> better completion rate and latency.
    if pri == Priority.QUERY:
        return 1.70
    if pri == Priority.INTERACTIVE:
        return 1.55
    return 1.35


def _queue_for_priority(s, pri):
    if pri == Priority.QUERY:
        return s.q_query
    if pri == Priority.INTERACTIVE:
        return s.q_interactive
    return s.q_batch


def _is_likely_oom_error(err):
    if err is None:
        return False
    try:
        msg = str(err).lower()
    except Exception:
        return False
    return ("oom" in msg) or ("out of memory" in msg) or ("memory" in msg and "killed" in msg)


def _pipeline_terminal(pipeline):
    status = pipeline.runtime_status()
    if status.is_pipeline_successful():
        return True
    # Drop pipelines that have any FAILED ops (baseline template behavior); we rely on OOM-avoidance to
    # prevent most failures. If the simulator supports retries via FAILED->ASSIGNABLE, it will still
    # be picked up as assignable; this check keeps "hard failed" pipelines from clogging queues.
    has_failures = status.state_counts[OperatorState.FAILED] > 0
    return has_failures


def _get_assignable_op(pipeline):
    status = pipeline.runtime_status()
    op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
    if not op_list:
        return None
    return op_list[0]


def _est_mem_gb(op):
    est = None
    try:
        if getattr(op, "estimate", None) is not None:
            est = getattr(op.estimate, "mem_peak_gb", None)
    except Exception:
        est = None
    if est is None:
        return None
    try:
        if est < 0:
            return None
    except Exception:
        return None
    return float(est)


def _effective_priority_score(s, pipeline):
    # Priority-first, but with mild aging for batch to avoid starvation/incompletion penalties.
    base = _priority_weight(pipeline.priority)
    meta = s.pipeline_meta.get(pipeline.pipeline_id)
    if meta is None:
        return base
    age = max(0, s.step_count - meta.get("arrival_step", s.step_count))
    # Only age BATCH; QUERY/INTERACTIVE must remain protected.
    if pipeline.priority == Priority.BATCH_PIPELINE:
        # After ~50 steps, batch gets +1; after ~100 steps, +2.
        return base + min(2, age // 50)
    return base


def _compute_ram_request_gb(s, pipeline, op, pool, avail_ram):
    est = _est_mem_gb(op)
    base_mult = _base_safety_mult(pipeline.priority)

    # Adaptive bump on prior likely OOM for this operator instance.
    bump = s.op_ram_mult.get((pipeline.pipeline_id, id(op)), 1.0)
    mult = base_mult * bump

    # If no estimate, be conservative but not huge: request a modest chunk to avoid blocking.
    # (Better to run and learn/fail quickly than to hoard RAM and cause queueing.)
    if est is None:
        # Query/interactive get a bit more by default.
        default = 2.0 if pipeline.priority != Priority.BATCH_PIPELINE else 1.5
        req = default
    else:
        req = est * mult

    # Guardrails: always request at least a small amount; never exceed available RAM.
    req = max(0.25, req)
    req = min(req, float(avail_ram))
    # Also never exceed pool max (defensive).
    try:
        req = min(req, float(pool.max_ram_pool))
    except Exception:
        pass
    return req


def _fits_pool(op, pool, avail_ram):
    est = _est_mem_gb(op)
    if est is None:
        # Unknown estimate: allow placement if there is reasonable headroom.
        return avail_ram >= 0.5
    # Fit check: require estimated peak <= available RAM (no safety factor here; safety is in request size).
    return est <= float(avail_ram)


@register_scheduler(key="scheduler_est_028")
def scheduler_est_028(s, results, pipelines):
    """
    Scheduler step:
      1) Ingest new pipelines into per-priority queues and record arrival steps.
      2) Process results: on likely OOM failure, increase operator RAM multiplier for retry.
      3) Build a candidate set of runnable (pipeline, op) pairs, scored by effective priority and memory.
      4) For each pool, pick the best-fit candidate that can likely fit; assign a single op per pool per tick.
    """
    s.step_count += 1

    # Ingest new pipelines
    for p in pipelines:
        if p.pipeline_id not in s.pipeline_meta:
            s.pipeline_meta[p.pipeline_id] = {"arrival_step": s.step_count}
        _queue_for_priority(s, p.priority).append(p)

    # Process results for adaptive OOM handling
    # Note: we avoid aggressive preemption because we don't have robust visibility into running containers here.
    for r in results:
        try:
            if r.failed():
                # Only bump RAM multiplier for likely OOM errors.
                if _is_likely_oom_error(getattr(r, "error", None)):
                    for op in getattr(r, "ops", []) or []:
                        key = (getattr(r, "pipeline_id", None), id(op))
                        # If pipeline_id isn't carried on result, fall back to op object id alone by using None.
                        prev = s.op_ram_mult.get(key, 1.0)
                        # Increase multiplicatively; cap to avoid runaway hoarding.
                        s.op_ram_mult[key] = min(3.0, prev * 1.35)
        except Exception:
            # Never let bookkeeping break scheduling.
            pass

    # Early exit if no state changes
    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    # Rebuild queues excluding terminal pipelines (completed or hard-failed)
    def _filter_queue(q):
        from collections import deque
        nq = deque()
        while q:
            p = q.popleft()
            if _pipeline_terminal(p):
                continue
            nq.append(p)
        return nq

    s.q_query = _filter_queue(s.q_query)
    s.q_interactive = _filter_queue(s.q_interactive)
    s.q_batch = _filter_queue(s.q_batch)

    # Build candidates: at most one runnable op per pipeline (the next assignable)
    candidates = []
    seen_pipelines = set()

    def _collect_from_queue(q):
        for p in list(q):
            if p.pipeline_id in seen_pipelines:
                continue
            op = _get_assignable_op(p)
            if op is None:
                continue
            seen_pipelines.add(p.pipeline_id)
            pr_score = _effective_priority_score(s, p)
            est = _est_mem_gb(op)
            # Sort key: higher effective priority first, then smaller estimated mem, then older arrival.
            meta = s.pipeline_meta.get(p.pipeline_id, {})
            arrival_step = meta.get("arrival_step", s.step_count)
            candidates.append((p, op, pr_score, est, arrival_step))

    _collect_from_queue(s.q_query)
    _collect_from_queue(s.q_interactive)
    _collect_from_queue(s.q_batch)

    if not candidates:
        return suspensions, assignments

    # Candidate ordering: priority desc, est mem asc (None last), arrival asc (older first)
    def _cand_sort_key(item):
        _, _, pr_score, est, arrival_step = item
        est_key = est if est is not None else float("inf")
        return (-pr_score, est_key, arrival_step)

    candidates.sort(key=_cand_sort_key)

    # For each pool, pick the best candidate that fits and yields minimal RAM waste (best-fit).
    # We assign at most one op per pool per tick, matching the baseline's simplicity.
    used_candidate_idx = set()
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool
        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # Select candidate: among those that fit, prefer higher priority then best-fit (least leftover RAM).
        best = None
        best_idx = None

        for idx, (p, op, pr_score, est, arrival_step) in enumerate(candidates):
            if idx in used_candidate_idx:
                continue
            if not _fits_pool(op, pool, avail_ram):
                continue

            # Compute ram request with headroom; if it ends up too small/large, clamp.
            ram_req = _compute_ram_request_gb(s, p, op, pool, avail_ram)
            if ram_req <= 0:
                continue

            # Best-fit metric: minimize leftover RAM after allocation, with priority dominating.
            leftover = float(avail_ram) - float(ram_req)

            score_tuple = (pr_score, -leftover)  # maximize pr_score; then maximize (-leftover) => minimize leftover
            if best is None:
                best = (p, op, ram_req, score_tuple)
                best_idx = idx
            else:
                if score_tuple > best[3]:
                    best = (p, op, ram_req, score_tuple)
                    best_idx = idx

        if best is None:
            continue

        p, op, ram_req, _ = best
        used_candidate_idx.add(best_idx)

        # CPU sizing: give the pool's available CPU to the chosen op to reduce latency for high priority.
        # (Keeping it simple and effective in a function-first runtime.)
        cpu_req = float(avail_cpu)

        assignments.append(
            Assignment(
                ops=[op],
                cpu=cpu_req,
                ram=float(ram_req),
                priority=p.priority,
                pool_id=pool_id,
                pipeline_id=p.pipeline_id,
            )
        )

    # Keep queues intact; pipelines remain until terminal.
    return suspensions, assignments
