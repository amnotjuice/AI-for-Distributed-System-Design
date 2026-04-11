# policy_key: scheduler_est_017
# reasoning_effort: low
# exp: estimation
# model: gpt-5.2-2025-12-11
# llm_cost: 0.045612
# generation_seconds: 63.56
# generated_at: 2026-04-10T09:05:28.638763
@register_scheduler_init(key="scheduler_est_017")
def scheduler_est_017_init(s):
    """Memory-aware, priority-weighted scheduler focused on lowering weighted latency and avoiding failures.

    Core ideas:
      - Strict priority ordering (QUERY > INTERACTIVE > BATCH) with mild aging for starvation avoidance.
      - Memory-aware admission/placement using op.estimate.mem_peak_gb (noisy hint) plus a learned per-op RAM hint.
      - Conservative RAM headroom to reduce OOMs; on failure, exponentially back off RAM up to pool max.
      - Simple round-robin within each priority class to reduce head-of-line blocking.
      - No preemption (keeps churn low); instead, careful packing and retry sizing.
    """
    s.q_query = []
    s.q_interactive = []
    s.q_batch = []

    # Per-operator learned RAM hint and retry counter (keyed by id(op) to avoid relying on unknown fields).
    s.op_ram_hint_gb = {}     # op_key -> float
    s.op_retry_count = {}     # op_key -> int

    # Pipeline aging to avoid indefinite starvation for low priority.
    s.pipe_age = {}           # pipeline_id -> int

    # Per-tick guard to avoid scheduling multiple ops from the same pipeline in a single tick.
    s._scheduled_pipelines_this_tick = set()


def _get_queue_for_priority(s, prio):
    if prio == Priority.QUERY:
        return s.q_query
    if prio == Priority.INTERACTIVE:
        return s.q_interactive
    return s.q_batch


def _iter_priority_queues(s):
    # Strict priority order to protect weighted objective.
    return [s.q_query, s.q_interactive, s.q_batch]


def _pipeline_is_done(pipeline):
    st = pipeline.runtime_status()
    return st.is_pipeline_successful()


def _get_first_assignable_op(pipeline):
    st = pipeline.runtime_status()
    ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
    if not ops:
        return None
    return ops[0]


def _op_key(op):
    # Use object identity as a stable key during a simulation run.
    return id(op)


def _estimate_mem_gb(op):
    est = None
    if getattr(op, "estimate", None) is not None:
        est = getattr(op.estimate, "mem_peak_gb", None)
    if est is None:
        return None
    try:
        estf = float(est)
        if estf < 0:
            return None
        return estf
    except Exception:
        return None


def _desired_ram_gb(s, op, pool, priority):
    # Combine estimator + learned hint, add headroom, then clamp to pool max.
    est = _estimate_mem_gb(op)
    hint = s.op_ram_hint_gb.get(_op_key(op), None)

    # Base memory guess: prefer learned hint (based on actual failures) over noisy estimate.
    base = None
    if hint is not None:
        base = hint
    elif est is not None:
        base = est
    else:
        base = None

    # Headroom tuning by priority: higher priority gets slightly more to reduce failure risk.
    if priority == Priority.QUERY:
        headroom = 1.30
        min_floor = 0.5
    elif priority == Priority.INTERACTIVE:
        headroom = 1.25
        min_floor = 0.5
    else:
        headroom = 1.20
        min_floor = 0.5

    if base is None:
        # No estimate: be conservative but avoid monopolizing the pool.
        # This reduces OOMs while keeping some concurrency.
        base = max(min_floor, 0.50 * float(pool.max_ram_pool))

    desired = max(min_floor, headroom * float(base))
    # Clamp to pool max.
    desired = min(desired, float(pool.max_ram_pool))
    return desired


def _desired_cpu(s, pool, priority, avail_cpu):
    # Simple CPU sizing:
    # - Give high priority more CPU to reduce latency.
    # - Keep some concurrency by capping per-op CPU.
    max_cpu = float(pool.max_cpu_pool)
    avail = float(avail_cpu)

    if priority == Priority.QUERY:
        cap = max(1.0, 0.80 * max_cpu)
        return max(1.0, min(avail, cap))
    if priority == Priority.INTERACTIVE:
        cap = max(1.0, 0.70 * max_cpu)
        return max(1.0, min(avail, cap))
    # Batch: smaller slices to avoid blocking high priority and to increase fairness.
    cap = max(1.0, 0.50 * max_cpu)
    return max(1.0, min(avail, cap))


def _fits_in_pool(desired_ram, pool_avail_ram):
    return float(desired_ram) <= float(pool_avail_ram)


def _pick_candidate_for_pool(s, pool_id):
    pool = s.executor.pools[pool_id]
    avail_cpu = pool.avail_cpu_pool
    avail_ram = pool.avail_ram_pool
    if avail_cpu <= 0 or avail_ram <= 0:
        return None

    # Determine if there is any higher-priority pressure; if so, throttle batch usage.
    high_pressure = (len(s.q_query) > 0) or (len(s.q_interactive) > 0)

    best = None
    best_score = None

    # Consider a bounded scan to keep runtime predictable.
    # We prefer:
    #   - higher priority
    #   - older pipelines (aging)
    #   - smaller memory footprint (fits more easily, reduces OOM risk)
    # while ensuring feasibility in this pool.
    for q in _iter_priority_queues(s):
        if not q:
            continue

        # If high-priority work exists, only consider batch when pool has plenty of headroom.
        if q is s.q_batch and high_pressure:
            # Leave headroom for interactive/query to reduce their tail latency.
            if float(avail_ram) < 0.50 * float(pool.max_ram_pool) or float(avail_cpu) < 0.50 * float(pool.max_cpu_pool):
                continue

        scan_n = min(len(q), 12)
        for i in range(scan_n):
            pipeline = q[i]
            if _pipeline_is_done(pipeline):
                continue
            if pipeline.pipeline_id in s._scheduled_pipelines_this_tick:
                continue

            op = _get_first_assignable_op(pipeline)
            if op is None:
                continue

            desired_ram = _desired_ram_gb(s, op, pool, pipeline.priority)
            # If estimator is present and clearly exceeds pool max, skip (won't ever fit here).
            est = _estimate_mem_gb(op)
            if est is not None and float(est) > float(pool.max_ram_pool) * 1.05:
                continue

            if not _fits_in_pool(desired_ram, avail_ram):
                continue

            # Score: lower is better.
            # Priority term dominates; then age (older => lower score), then memory size.
            if pipeline.priority == Priority.QUERY:
                pterm = 0.0
            elif pipeline.priority == Priority.INTERACTIVE:
                pterm = 10.0
            else:
                pterm = 25.0

            age = float(s.pipe_age.get(pipeline.pipeline_id, 0))
            mem_term = float(desired_ram) / max(1e-6, float(pool.max_ram_pool))

            score = pterm - 0.15 * age + 2.0 * mem_term

            if best is None or score < best_score:
                best = (pipeline, op, desired_ram)
                best_score = score

    return best


def _bump_ram_hint_on_failure(s, result):
    # Use result info to increase future allocations for those ops.
    try:
        ops = getattr(result, "ops", None) or []
        prev_ram = float(getattr(result, "ram", 0.0) or 0.0)
        pool_id = getattr(result, "pool_id", None)
        pool = s.executor.pools[int(pool_id)] if pool_id is not None else None
        pool_max = float(pool.max_ram_pool) if pool is not None else None
    except Exception:
        ops = []
        prev_ram = 0.0
        pool_max = None

    for op in ops:
        k = _op_key(op)
        s.op_retry_count[k] = int(s.op_retry_count.get(k, 0)) + 1

        # Exponential backoff, capped; add a small additive term to escape tiny allocations.
        bumped = max(prev_ram * 2.0, (s.op_ram_hint_gb.get(k, prev_ram) or prev_ram) * 1.6, prev_ram + 1.0, 1.0)
        if pool_max is not None:
            bumped = min(bumped, pool_max)
        s.op_ram_hint_gb[k] = bumped


@register_scheduler(key="scheduler_est_017")
def scheduler_est_017(s, results, pipelines):
    """
    Scheduler step:
      1) Enqueue new pipelines by priority; update aging.
      2) Incorporate failures to increase RAM hints (reduces repeated OOMs).
      3) For each pool, greedily assign feasible ops by priority with memory-aware sizing.
    """
    # Enqueue newly arrived pipelines.
    for p in pipelines:
        _get_queue_for_priority(s, p.priority).append(p)
        s.pipe_age[p.pipeline_id] = 0

    # Early exit if nothing changed.
    if not pipelines and not results:
        return [], []

    # Update per-op hints based on results (mostly to reduce future failures).
    for r in results:
        if getattr(r, "failed", None) is not None and r.failed():
            _bump_ram_hint_on_failure(s, r)

    # Age pipelines that are still pending (mild starvation avoidance).
    for q in _iter_priority_queues(s):
        for p in q:
            if not _pipeline_is_done(p):
                s.pipe_age[p.pipeline_id] = int(s.pipe_age.get(p.pipeline_id, 0)) + 1

    # Clean completed pipelines from queues (keep failures to allow retry via FAILED in ASSIGNABLE_STATES).
    for q in _iter_priority_queues(s):
        alive = []
        for p in q:
            if not _pipeline_is_done(p):
                alive.append(p)
        q[:] = alive

    s._scheduled_pipelines_this_tick = set()

    suspensions = []
    assignments = []

    # Greedy per-pool packing with feasibility checks using estimator/hints.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]

        # Continue assigning while there are resources and feasible candidates.
        while pool.avail_cpu_pool > 0 and pool.avail_ram_pool > 0:
            candidate = _pick_candidate_for_pool(s, pool_id)
            if candidate is None:
                break

            pipeline, op, desired_ram = candidate

            # Determine final cpu/ram within current availability.
            cpu = _desired_cpu(s, pool, pipeline.priority, pool.avail_cpu_pool)
            ram = min(float(desired_ram), float(pool.avail_ram_pool))
            if cpu <= 0 or ram <= 0:
                break

            # If an operator has failed repeatedly, force a larger minimum allocation (up to pool max)
            # to avoid spinning on too-small allocations.
            rk = _op_key(op)
            retries = int(s.op_retry_count.get(rk, 0))
            if retries >= 2:
                min_after_retries = min(float(pool.max_ram_pool), max(ram, 0.70 * float(pool.max_ram_pool)))
                ram = min(min_after_retries, float(pool.avail_ram_pool))

            # Final safety: if still doesn't fit, stop packing this pool this tick.
            if ram > float(pool.avail_ram_pool) or cpu > float(pool.avail_cpu_pool):
                break

            assignments.append(
                Assignment(
                    ops=[op],
                    cpu=cpu,
                    ram=ram,
                    priority=pipeline.priority,
                    pool_id=pool_id,
                    pipeline_id=pipeline.pipeline_id,
                )
            )
            s._scheduled_pipelines_this_tick.add(pipeline.pipeline_id)

            # Round-robin: move scheduled pipeline to the back of its queue.
            q = _get_queue_for_priority(s, pipeline.priority)
            try:
                idx = q.index(pipeline)
                q.append(q.pop(idx))
            except Exception:
                pass

            # Refresh pool reference (simulator updates avail resources after applying assignments;
            # but within this function we conservatively stop if near depletion).
            # We rely on the next tick to reflect exact availability.

            # Avoid over-scheduling beyond what's likely to fit after the simulator applies assignments.
            if float(pool.avail_cpu_pool) <= 1.0 or float(pool.avail_ram_pool) <= 0.5:
                break

    return suspensions, assignments
