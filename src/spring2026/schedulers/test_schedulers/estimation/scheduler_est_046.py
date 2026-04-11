# policy_key: scheduler_est_046
# reasoning_effort: low
# exp: estimation
# model: gpt-5.2-2025-12-11
# llm_cost: 0.052108
# generation_seconds: 50.68
# generated_at: 2026-04-10T10:22:51.650487
@register_scheduler_init(key="scheduler_est_046")
def scheduler_est_046_init(s):
    """Memory-aware, priority-weighted scheduler with cautious sizing + OOM backoff.

    Goals (aligned to weighted latency objective):
      - Protect QUERY/INTERACTIVE latency via strict priority ordering + small-batch dispatch.
      - Reduce 720s failure penalties by avoiding obvious OOM placements using mem estimates.
      - Maintain completion rate by requeuing failed ops with increased RAM requests (bounded),
        and by adding mild aging to prevent indefinite batch starvation.

    Key ideas:
      - Maintain per-priority FIFO queues of pipelines.
      - On each tick, try to assign at most 1 ready operator per pool (limits blast radius).
      - For each op, choose a pool where estimated peak memory fits with headroom.
      - RAM request = clamp( estimate * safety * (2 ** oom_failures), min, pool_max, avail_ram ).
      - If an op doesn't fit any pool, do not assign it (avoid guaranteed failure).
    """
    s.q_query = []
    s.q_interactive = []
    s.q_batch = []

    # Bookkeeping for fairness/aging and OOM backoff
    s._tick = 0
    s._arrival_seq = 0
    s._pipe_meta = {}  # pipeline_id -> dict(enq_tick, last_seen_tick, seq, priority)
    s._op_oom_fails = {}  # (pipeline_id, op_key) -> int
    s._recent_failures = []  # list of (tick, pipeline_id) for light dampening


def _pqueue_for_priority(s, prio):
    if prio == Priority.QUERY:
        return s.q_query
    if prio == Priority.INTERACTIVE:
        return s.q_interactive
    return s.q_batch


def _priority_weight(prio):
    if prio == Priority.QUERY:
        return 10
    if prio == Priority.INTERACTIVE:
        return 5
    return 1


def _safety_factor(prio):
    # Slightly higher headroom for higher priority to avoid costly failures/retries.
    if prio == Priority.QUERY:
        return 1.60
    if prio == Priority.INTERACTIVE:
        return 1.45
    return 1.30


def _cpu_fraction(prio):
    # Give more CPU to latency-sensitive work, but keep some room for concurrency.
    if prio == Priority.QUERY:
        return 0.75
    if prio == Priority.INTERACTIVE:
        return 0.55
    return 0.35


def _get_op_key(op):
    # Best-effort stable key. Fall back to object id.
    for attr in ("op_id", "operator_id", "id", "name"):
        if hasattr(op, attr):
            try:
                v = getattr(op, attr)
                if v is not None:
                    return (attr, v)
            except Exception:
                pass
    return ("obj", id(op))


def _get_est_mem(op):
    try:
        est = getattr(op, "estimate", None)
        if est is None:
            return None
        v = getattr(est, "mem_peak_gb", None)
        if v is None:
            return None
        # Guard against weird negatives/NaNs.
        if not (v >= 0):
            return None
        return float(v)
    except Exception:
        return None


def _pipeline_ready_ops(pipeline):
    status = pipeline.runtime_status()
    if status.is_pipeline_successful():
        return []
    # If anything failed, we still allow retry of FAILED ops (ASSIGNABLE_STATES includes FAILED).
    return status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)


def _fits_any_pool(s, est_mem):
    if est_mem is None:
        return True
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        # Feasibility check against maximum capacity (not just current availability).
        if est_mem <= pool.max_ram_pool:
            return True
    return False


def _choose_pool_for_op(s, prio, est_mem, need_ram_min_gb):
    # Pool selection heuristic:
    #  1) Prefer pools where (estimated) memory fits within available RAM (with some slack).
    #  2) For high priority, prefer "most headroom" pool to reduce queueing contention.
    #  3) Otherwise, prefer tighter fit to reduce fragmentation.
    candidates = []
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        if pool.avail_cpu_pool <= 0 or pool.avail_ram_pool <= 0:
            continue

        # Must be able to allocate at least the requested minimum RAM now.
        if need_ram_min_gb > pool.avail_ram_pool:
            continue

        # If we have an estimate, avoid obviously-risky placements.
        if est_mem is not None:
            # Require estimate to fit in pool max, and prefer it to fit in avail.
            if est_mem > pool.max_ram_pool:
                continue
            # Soft constraint: if est exceeds avail, still allow only if we are already
            # requesting near avail (i.e., we can't do better elsewhere) handled by scoring.
        candidates.append((pool_id, pool))

    if not candidates:
        return None

    # Score candidates: lower score is better.
    scored = []
    for pool_id, pool in candidates:
        avail_ram = pool.avail_ram_pool
        avail_cpu = pool.avail_cpu_pool

        if est_mem is None:
            # Without estimate, prioritize more headroom for queries, otherwise pack.
            if prio in (Priority.QUERY, Priority.INTERACTIVE):
                score = (-avail_ram, -avail_cpu)
            else:
                score = (avail_ram, avail_cpu)
        else:
            # Penalize placements where est_mem exceeds avail_ram (higher OOM likelihood / delay).
            overflow = max(0.0, est_mem - avail_ram)
            # Prefer: for high prio, more headroom; for batch, tighter packing.
            if prio in (Priority.QUERY, Priority.INTERACTIVE):
                score = (overflow * 1000.0, -avail_ram, -avail_cpu)
            else:
                # Batch: pack (small slack) but still avoid overflow.
                slack = max(0.0, avail_ram - est_mem)
                score = (overflow * 1000.0, slack, avail_cpu)
        scored.append((score, pool_id))

    scored.sort(key=lambda x: x[0])
    return scored[0][1]


def _compute_ram_request(s, pipeline, op, prio, pool):
    est_mem = _get_est_mem(op)

    op_key = _get_op_key(op)
    oom_fails = s._op_oom_fails.get((pipeline.pipeline_id, op_key), 0)

    # Base request:
    # - if estimate exists, request estimate * safety
    # - else request a conservative small chunk, leaving room for others; rely on backoff on fail
    safety = _safety_factor(prio)
    if est_mem is not None:
        base = est_mem * safety
    else:
        # Conservative default; avoid grabbing whole pool on unknown operators.
        base = max(0.5, min(4.0, 0.25 * pool.max_ram_pool))

    # Exponential backoff after OOM-like failures.
    req = base * (2.0 ** oom_fails)

    # Bound requests to pool limits and current availability.
    req = min(req, pool.max_ram_pool)
    req = min(req, pool.avail_ram_pool)

    # Ensure some minimum (if pool has it), but don't exceed availability.
    req = max(0.1, req)
    return req, est_mem


def _compute_cpu_request(prio, pool):
    # CPU request based on priority fraction, bounded by availability.
    frac = _cpu_fraction(prio)
    target = max(1.0, pool.max_cpu_pool * frac)
    cpu = min(pool.avail_cpu_pool, target)
    cpu = max(1.0, cpu) if pool.avail_cpu_pool >= 1.0 else pool.avail_cpu_pool
    return cpu


def _pipeline_sort_key(s, pipeline):
    meta = s._pipe_meta.get(pipeline.pipeline_id)
    if meta is None:
        # Should not happen, but keep stable behavior.
        return (0, 0, 0)

    prio = meta["priority"]
    age = max(0, s._tick - meta["enq_tick"])

    # Strict priority first, then aging within/between queues.
    # Aging is mild to avoid harming high-priority latency.
    prio_rank = 0 if prio == Priority.QUERY else (1 if prio == Priority.INTERACTIVE else 2)

    # Age bonus only meaningfully helps batch after it waits.
    age_bonus = 0
    if prio == Priority.BATCH_PIPELINE:
        age_bonus = min(50, age // 5)
    elif prio == Priority.INTERACTIVE:
        age_bonus = min(20, age // 10)

    # Lower tuple sorts first; subtract bonus to bring older pipelines forward.
    return (prio_rank, -age_bonus, meta["seq"])


@register_scheduler(key="scheduler_est_046")
def scheduler_est_046(s, results, pipelines):
    """
    Scheduler step:
      - Ingest new pipelines into per-priority queues.
      - Process execution results to detect failures; requeue by leaving pipelines in queues and
        increasing RAM backoff for the failing operator on subsequent attempts.
      - For each pool, pick the best next ready operator across queues (priority+aging),
        ensuring estimated memory is feasible, and assign with cautious RAM/CPU.
    """
    s._tick += 1

    # Ingest new arrivals.
    for p in pipelines:
        q = _pqueue_for_priority(s, p.priority)
        q.append(p)
        s._arrival_seq += 1
        s._pipe_meta[p.pipeline_id] = {
            "enq_tick": s._tick,
            "last_seen_tick": s._tick,
            "seq": s._arrival_seq,
            "priority": p.priority,
        }

    # Update failure backoff hints from results (best-effort).
    for r in results:
        if getattr(r, "failed", None) is not None and r.failed():
            # Record recent failures for possible future tuning (not used aggressively).
            s._recent_failures.append((s._tick, getattr(r, "pipeline_id", None)))
            # If we can identify the failing operator, bump its OOM fail count.
            # We only have r.ops (list). If multiple, bump all (safe).
            try:
                for op in getattr(r, "ops", []) or []:
                    # Assume OOM-like error signals; if we can't detect, still back off slightly.
                    err = str(getattr(r, "error", "") or "")
                    is_oomish = ("oom" in err.lower()) or ("out of memory" in err.lower()) or ("mem" in err.lower())
                    bump = 1 if is_oomish else 0.5
                    op_key = _get_op_key(op)
                    pid = getattr(r, "pipeline_id", None)
                    # r may not have pipeline_id; if absent, we can't track precisely.
                    if pid is None:
                        continue
                    cur = s._op_oom_fails.get((pid, op_key), 0)
                    # Store as int steps; 0.5 bump rounds up probabilistically not supported -> ceil.
                    s._op_oom_fails[(pid, op_key)] = int(cur + bump + 0.999)
            except Exception:
                pass

    # Early exit.
    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    # Compact queues: remove completed/failed pipelines, keep active ones.
    def _filter_queue(q):
        newq = []
        for p in q:
            try:
                status = p.runtime_status()
                if status.is_pipeline_successful():
                    continue
                # If pipeline has failures, we still keep it; failures are penalized heavily, so
                # give it a chance to retry FAILED ops (ASSIGNABLE_STATES includes FAILED).
                newq.append(p)
                if p.pipeline_id in s._pipe_meta:
                    s._pipe_meta[p.pipeline_id]["last_seen_tick"] = s._tick
            except Exception:
                # If status call fails, keep it to avoid dropping.
                newq.append(p)
        return newq

    s.q_query = _filter_queue(s.q_query)
    s.q_interactive = _filter_queue(s.q_interactive)
    s.q_batch = _filter_queue(s.q_batch)

    # Helper: iterate pipelines in global order (priority then mild aging).
    def _iter_pipelines_ordered():
        all_p = s.q_query + s.q_interactive + s.q_batch
        all_p.sort(key=lambda p: _pipeline_sort_key(s, p))
        return all_p

    ordered_pipelines = _iter_pipelines_ordered()

    # One assignment per pool per tick (simple, safer than aggressive packing).
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        if pool.avail_cpu_pool <= 0 or pool.avail_ram_pool <= 0:
            continue

        best = None  # tuple(score, pipeline, op, prio, ram_req, cpu_req, est_mem)
        for p in ordered_pipelines:
            prio = p.priority
            ready_ops = _pipeline_ready_ops(p)
            if not ready_ops:
                continue

            # Prefer the first ready operator; keep it simple (DAG order).
            op = ready_ops[0]

            # If estimate says it can't ever fit, skip to avoid inevitable failure penalty.
            est_mem = _get_est_mem(op)
            if est_mem is not None and not _fits_any_pool(s, est_mem):
                continue

            # Compute RAM request (bounded by pool) and check feasibility now.
            ram_req, est_mem = _compute_ram_request(s, p, op, prio, pool)
            if ram_req <= 0 or ram_req > pool.avail_ram_pool:
                continue

            # If estimate exists and is materially larger than what we'd allocate now, treat as
            # risky and only schedule if there isn't a better option.
            risk = 0.0
            if est_mem is not None:
                if est_mem > pool.avail_ram_pool:
                    # Likely won't fit without waiting; deprioritize strongly.
                    risk += 1000.0
                elif ram_req < 0.90 * est_mem:
                    # Under-allocating vs estimate increases OOM risk; deprioritize moderately.
                    risk += 50.0

            # CPU request (bounded by pool).
            cpu_req = _compute_cpu_request(prio, pool)
            if cpu_req <= 0:
                continue

            # Score: prioritize high weight and low risk; also prefer smaller estimated memory to
            # improve packing and reduce cascading OOM.
            w = _priority_weight(prio)
            est_part = est_mem if est_mem is not None else ram_req
            # Lower is better: negative weight encourages high priority.
            score = (risk + 0.01 * est_part) - (100.0 * w)

            # Mild preference for older batch/interactive to prevent starvation.
            meta = s._pipe_meta.get(p.pipeline_id, {})
            age = max(0, s._tick - meta.get("enq_tick", s._tick))
            if prio == Priority.BATCH_PIPELINE:
                score -= min(30.0, age / 10.0)
            elif prio == Priority.INTERACTIVE:
                score -= min(10.0, age / 30.0)

            if best is None or score < best[0]:
                best = (score, p, op, prio, ram_req, cpu_req, est_mem)

        # If we found a suitable op for this pool, assign it; else leave pool idle this tick.
        if best is not None:
            _, p, op, prio, ram_req, cpu_req, _ = best

            # Final: if operator estimate suggests it doesn't fit well here, try to pick a better pool.
            # This prevents assigning to a suboptimal pool when a better one exists.
            est_mem = _get_est_mem(op)
            if est_mem is not None:
                alt_pool_id = _choose_pool_for_op(s, prio, est_mem, ram_req)
                if alt_pool_id is not None and alt_pool_id != pool_id:
                    # Only switch if alternative still has resources for our request.
                    alt_pool = s.executor.pools[alt_pool_id]
                    if alt_pool.avail_cpu_pool >= cpu_req and alt_pool.avail_ram_pool >= ram_req:
                        pool_id = alt_pool_id
                        pool = alt_pool
                        cpu_req = min(cpu_req, pool.avail_cpu_pool)
                        ram_req = min(ram_req, pool.avail_ram_pool)

            assignments.append(
                Assignment(
                    ops=[op],
                    cpu=cpu_req,
                    ram=ram_req,
                    priority=prio,
                    pool_id=pool_id,
                    pipeline_id=p.pipeline_id,
                )
            )

    return suspensions, assignments
