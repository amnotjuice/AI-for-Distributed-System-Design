# policy_key: scheduler_high_006
# reasoning_effort: high
# exp: reasoning
# model: gpt-5.2-2025-12-11
# llm_cost: 0.130771
# generation_seconds: 176.84
# generated_at: 2026-04-10T00:29:21.366544
@register_scheduler_init(key="scheduler_high_006")
def scheduler_high_006_init(s):
    """Priority-aware, OOM-robust, headroom-preserving scheduler.

    Core ideas:
      1) Prioritize QUERY > INTERACTIVE > BATCH via weighted round-robin (10/5/1) to reduce weighted latency.
      2) Avoid costly failures by tracking per-operator RAM needs and doubling RAM on OOM (fast convergence).
      3) Preserve headroom for high-priority work by preventing batch from consuming the last chunk of CPU/RAM
         whenever there is outstanding high-priority demand.
      4) Fill each pool with multiple assignments per tick (instead of 1) to improve utilization/throughput.
    """
    # Global tick for simple aging/housekeeping
    s._tick = 0

    # Per-priority FIFO queues of Pipeline objects
    s._queues = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }

    # Track active pipelines and arrivals (mainly for bookkeeping / future extensions)
    s._active = {}          # pipeline_id -> Pipeline
    s._arrival_tick = {}    # pipeline_id -> tick

    # Per-operator learned resource hints (keyed by id(op) to avoid assumptions about op schema)
    s._op_ram_est = {}      # op_id -> ram_estimate
    s._op_retries = {}      # op_id -> retry_count
    s._op_to_pipeline = {}  # op_id -> pipeline_id (learned when we first see the op as runnable)

    # Give-up set for operators that appear unschedulable (e.g., OOM even at near-max RAM repeatedly)
    # Note: We keep this conservative for high-priority ops to avoid dropping them.
    s._op_giveup = set()

    # Weighted RR order (QUERY dominates score, then INTERACTIVE, then BATCH)
    s._rr_seq = ([Priority.QUERY] * 10) + ([Priority.INTERACTIVE] * 5) + ([Priority.BATCH_PIPELINE] * 1)
    s._rr_idx = 0

    # Limit how many new containers we start per pool per tick to reduce churn
    s._max_assignments_per_pool = 4

    # Cache overall maxima to detect impossible RAM fits
    try:
        s._max_ram_overall = max(p.max_ram_pool for p in s.executor.pools)
        s._max_cpu_overall = max(p.max_cpu_pool for p in s.executor.pools)
    except Exception:
        # Safe fallback (should not happen in normal simulator wiring)
        s._max_ram_overall = 0
        s._max_cpu_overall = 0


def _sched_high_006_assignable_states():
    # Be robust if the simulator exposes ASSIGNABLE_STATES globally (as in the template),
    # otherwise derive it from OperatorState.
    states = globals().get("ASSIGNABLE_STATES")
    if states is not None:
        return states
    return [OperatorState.PENDING, OperatorState.FAILED]


def _sched_high_006_is_oom(error_obj) -> bool:
    if not error_obj:
        return False
    msg = str(error_obj).lower()
    return ("oom" in msg) or ("out of memory" in msg) or ("cuda out of memory" in msg) or ("cannot allocate memory" in msg)


def _sched_high_006_next_prio(s):
    pr = s._rr_seq[s._rr_idx]
    s._rr_idx = (s._rr_idx + 1) % len(s._rr_seq)
    return pr


def _sched_high_006_base_ram_frac(priority):
    # Slightly conservative to reduce OOM churn (OOMs inflate end-to-end latency and risk 720s penalty).
    if priority == Priority.QUERY:
        return 0.30
    if priority == Priority.INTERACTIVE:
        return 0.25
    return 0.20  # batch


def _sched_high_006_base_cpu_request(priority, pool):
    # CPU has sublinear scaling; cap per-op CPU to allow concurrency but still keep high priority fast.
    max_cpu = float(pool.max_cpu_pool)
    if priority == Priority.QUERY:
        # Prefer more CPU to reduce tail latency; still capped.
        return max(2.0, min(8.0, 0.60 * max_cpu))
    if priority == Priority.INTERACTIVE:
        return max(1.0, min(6.0, 0.45 * max_cpu))
    return max(1.0, min(4.0, 0.30 * max_cpu))


def _sched_high_006_request_ram(s, op_id, priority, pool, avail_ram):
    max_ram = float(pool.max_ram_pool)
    # Learned estimate wins; otherwise a conservative base fraction of pool RAM.
    est = s._op_ram_est.get(op_id)
    if est is None or est <= 0:
        est = _sched_high_006_base_ram_frac(priority) * max_ram
    # Never request >90% of pool RAM by default (keeps some slack for variability / other work).
    req = min(est, 0.90 * max_ram)
    # Must fit what is currently available.
    return min(float(avail_ram), float(req))


def _sched_high_006_request_cpu(priority, pool, avail_cpu):
    req = _sched_high_006_base_cpu_request(priority, pool)
    return min(float(avail_cpu), float(req))


def _sched_high_006_has_high_pressure(s) -> bool:
    # Cheap approximation: if there exist non-empty high-priority queues, assume pressure.
    # (We avoid scanning full DAG readiness to keep policy simple and fast.)
    return (len(s._queues[Priority.QUERY]) > 0) or (len(s._queues[Priority.INTERACTIVE]) > 0)


def _sched_high_006_find_fit(s, priority, pool, avail_cpu, avail_ram, high_pressure, reserve_cpu, reserve_ram):
    """Scan the priority queue once to find a runnable op that fits current resources.

    Returns: (pipeline, ops, cpu, ram) or None
    """
    q = s._queues[priority]
    if not q:
        return None

    assignable_states = _sched_high_006_assignable_states()
    n = len(q)

    for _ in range(n):
        pipeline = q.pop(0)
        pid = pipeline.pipeline_id

        status = pipeline.runtime_status()
        if status.is_pipeline_successful():
            # Drop completed pipelines from active tracking; do not requeue.
            s._active.pop(pid, None)
            s._arrival_tick.pop(pid, None)
            continue

        # Only schedule ops whose parents are complete to respect DAG dependencies.
        ops = status.get_ops(assignable_states, require_parents_complete=True)[:1]
        if not ops:
            # Not runnable yet; keep it in rotation.
            q.append(pipeline)
            continue

        op = ops[0]
        op_id = id(op)
        s._op_to_pipeline.setdefault(op_id, pid)

        # If we've concluded this operator is not feasible, skip it (prevents endless OOM thrash).
        if op_id in s._op_giveup:
            q.append(pipeline)
            continue

        cpu_req = _sched_high_006_request_cpu(priority, pool, avail_cpu)
        ram_req = _sched_high_006_request_ram(s, op_id, priority, pool, avail_ram)

        # Must be positive to make progress
        if cpu_req <= 0 or ram_req <= 0:
            q.append(pipeline)
            continue

        # Headroom protection: if high pressure exists, don't let batch consume the last chunk.
        if priority == Priority.BATCH_PIPELINE and high_pressure:
            if (float(avail_cpu) - float(cpu_req)) < float(reserve_cpu) or (float(avail_ram) - float(ram_req)) < float(reserve_ram):
                # Put it back and allow higher priorities to take resources.
                q.append(pipeline)
                return None

        # Check fit
        if float(cpu_req) <= float(avail_cpu) and float(ram_req) <= float(avail_ram):
            # Requeue pipeline to allow round-robin across pipelines of same priority.
            q.append(pipeline)
            return pipeline, ops, cpu_req, ram_req

        # Doesn't fit; keep it in queue and continue scanning for a smaller one.
        q.append(pipeline)

    return None


@register_scheduler(key="scheduler_high_006")
def scheduler_high_006_scheduler(s, results: List["ExecutionResult"], pipelines: List["Pipeline"]) -> Tuple[List["Suspend"], List["Assignment"]]:
    """Scheduling step.

    - Ingest new pipelines
    - Learn from execution results (especially OOM backoff)
    - Place runnable ops onto pools using weighted priority RR + batch headroom gating
    """
    s._tick += 1

    # Learn from results (focus: reduce failures by converging on sufficient RAM quickly).
    for r in results:
        if not getattr(r, "ops", None):
            continue

        pool_id = getattr(r, "pool_id", None)
        pool = s.executor.pools[pool_id] if pool_id is not None and 0 <= pool_id < s.executor.num_pools else None
        max_ram_pool = float(pool.max_ram_pool) if pool is not None else float(s._max_ram_overall or 0)

        for op in r.ops:
            op_id = id(op)

            # Record mapping if we saw the op only via results (best-effort).
            if op_id not in s._op_to_pipeline:
                # Can't infer pipeline_id from results reliably; leave unset.
                pass

            if r.failed():
                s._op_retries[op_id] = s._op_retries.get(op_id, 0) + 1

                if _sched_high_006_is_oom(getattr(r, "error", None)):
                    # Double RAM on OOM, capped by overall/pool max.
                    prev = s._op_ram_est.get(op_id, float(getattr(r, "ram", 0)) or 0.0)
                    allocated = float(getattr(r, "ram", 0) or 0.0)
                    base = max(prev, allocated, 1.0)
                    new_est = base * 2.0
                    cap = max_ram_pool if max_ram_pool > 0 else float(s._max_ram_overall or new_est)
                    s._op_ram_est[op_id] = min(new_est, cap)

                    # If we keep OOM-ing near max RAM, eventually stop retrying batch ops to avoid wasting time.
                    # For high-priority ops we keep retrying longer (since they dominate the score).
                    near_max = (cap > 0) and (s._op_ram_est.get(op_id, 0.0) >= 0.95 * cap)
                    if near_max:
                        max_retry = 8 if r.priority in (Priority.QUERY, Priority.INTERACTIVE) else 4
                        if s._op_retries.get(op_id, 0) >= max_retry:
                            s._op_giveup.add(op_id)
            else:
                # On success, we can seed RAM estimate (useful for retries / similar future attempts in the same run).
                ram_used = float(getattr(r, "ram", 0) or 0.0)
                if ram_used > 0:
                    prev = s._op_ram_est.get(op_id)
                    if prev is None or prev <= 0:
                        s._op_ram_est[op_id] = ram_used
                    else:
                        # Very light smoothing (avoid oscillations if allocations vary by pool).
                        s._op_ram_est[op_id] = 0.85 * float(prev) + 0.15 * ram_used

    # Ingest new pipelines
    for p in pipelines:
        pid = p.pipeline_id
        if pid in s._active:
            continue
        s._active[pid] = p
        s._arrival_tick[pid] = s._tick
        # Ensure priority key exists even if new enum value appears (fallback to batch queue).
        pr = getattr(p, "priority", Priority.BATCH_PIPELINE)
        if pr not in s._queues:
            pr = Priority.BATCH_PIPELINE
        s._queues[pr].append(p)

    # Early exit if nothing changed
    if not pipelines and not results:
        return [], []

    suspensions: List["Suspend"] = []
    assignments: List["Assignment"] = []

    # Global high-pressure signal to preserve some headroom against batch (no preemption in this policy).
    high_pressure = _sched_high_006_has_high_pressure(s)

    # Schedule across pools (pack multiple ops per pool per tick).
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)

        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # Reserve headroom for high priority arrivals/backlog (prevents batch from blocking query/interactive).
        reserve_cpu = max(1.0, 0.25 * float(pool.max_cpu_pool)) if high_pressure else 0.0
        reserve_ram = max(0.0, 0.25 * float(pool.max_ram_pool)) if high_pressure else 0.0

        started = 0
        # Try to start multiple containers to improve throughput/utilization.
        while started < s._max_assignments_per_pool and avail_cpu > 0 and avail_ram > 0:
            made_progress = False

            # Weighted RR across priorities (10/5/1), skip empties / non-fitting candidates.
            for _ in range(len(s._rr_seq)):
                prio = _sched_high_006_next_prio(s)
                fit = _sched_high_006_find_fit(
                    s=s,
                    priority=prio,
                    pool=pool,
                    avail_cpu=avail_cpu,
                    avail_ram=avail_ram,
                    high_pressure=high_pressure,
                    reserve_cpu=reserve_cpu,
                    reserve_ram=reserve_ram,
                )
                if fit is None:
                    continue

                pipeline, ops, cpu_req, ram_req = fit

                # Create the assignment
                assignments.append(
                    Assignment(
                        ops=ops,
                        cpu=float(cpu_req),
                        ram=float(ram_req),
                        priority=pipeline.priority,
                        pool_id=pool_id,
                        pipeline_id=pipeline.pipeline_id,
                    )
                )

                # Update local available resources and continue packing.
                avail_cpu -= float(cpu_req)
                avail_ram -= float(ram_req)
                started += 1
                made_progress = True
                break

            if not made_progress:
                break

    return suspensions, assignments
