# policy_key: scheduler_est_036
# reasoning_effort: low
# exp: estimation
# model: gpt-5.2-2025-12-11
# llm_cost: 0.046494
# generation_seconds: 47.43
# generated_at: 2026-04-10T10:14:33.181230
@register_scheduler_init(key="scheduler_est_036")
def scheduler_est_036_init(s):
    """
    Memory-aware, priority-first FIFO scheduler with OOM-avoidance retries.

    Core ideas:
      - Maintain separate FIFO queues per priority (QUERY > INTERACTIVE > BATCH).
      - Prefer scheduling high-priority pipelines first to minimize weighted latency.
      - Use op.estimate.mem_peak_gb as a hint to right-size RAM and avoid likely OOM placements.
      - On failures (likely OOM), exponentially increase RAM for that operator on retry.
      - Keep assignments small (one op per container) to reduce blast radius of bad estimates.
    """
    # FIFO queues by priority
    s.q_query = []
    s.q_interactive = []
    s.q_batch = []

    # Per-operator retry sizing state (keyed by Python object id to avoid relying on unknown fields)
    # op_state[op_key] = {"attempts": int, "ram_bump_gb": float}
    s.op_state = {}

    # Track pipelines we consider "known" so we can avoid re-enqueue duplication
    s.known_pipeline_ids = set()


def _p_weight(priority):
    # Larger weight => more urgent
    if priority == Priority.QUERY:
        return 3
    if priority == Priority.INTERACTIVE:
        return 2
    return 1


def _enqueue_pipeline(s, p):
    if p.pipeline_id in s.known_pipeline_ids:
        return
    s.known_pipeline_ids.add(p.pipeline_id)
    if p.priority == Priority.QUERY:
        s.q_query.append(p)
    elif p.priority == Priority.INTERACTIVE:
        s.q_interactive.append(p)
    else:
        s.q_batch.append(p)


def _all_queues(s):
    # Highest priority first
    return [s.q_query, s.q_interactive, s.q_batch]


def _pipeline_done_or_failed(p):
    st = p.runtime_status()
    # If any failures exist, we still allow retry at operator level (FAILED is assignable),
    # but if the pipeline is marked failed by runtime semantics, drop it.
    if st.is_pipeline_successful():
        return True
    # If runtime marks failures as terminal at pipeline-level, it will be reflected here.
    # We treat "has failures" as non-terminal because FAILED is in ASSIGNABLE_STATES.
    return False


def _get_one_assignable_op(p):
    st = p.runtime_status()
    if st.is_pipeline_successful():
        return None
    ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
    if not ops:
        return None
    return ops[0]


def _op_key(op):
    # Stable for the lifetime of the object in the sim
    return id(op)


def _estimate_ram_gb(op):
    est = None
    if hasattr(op, "estimate") and op.estimate is not None:
        est = getattr(op.estimate, "mem_peak_gb", None)
    # Guard against bogus values
    if est is None:
        return None
    try:
        est = float(est)
    except Exception:
        return None
    if est <= 0:
        return None
    return est


def _choose_ram_cpu(s, pool, op, priority):
    """
    Choose RAM/CPU for a single-op container.

    RAM strategy:
      - Start from estimator hint with headroom (1.25x) to counter noise.
      - Apply exponential bump on previous failures.
      - Clamp to pool availability/max to avoid infeasible placements.

    CPU strategy:
      - Give more CPU to high-priority ops when available.
      - Keep at least 1 CPU when possible.
    """
    opk = _op_key(op)
    st = s.op_state.get(opk, {"attempts": 0, "ram_bump_gb": 0.0})

    est_mem = _estimate_ram_gb(op)

    # Baselines and headroom
    if priority == Priority.QUERY:
        headroom = 1.30
        min_ram = 1.0
        cpu_frac = 0.75
    elif priority == Priority.INTERACTIVE:
        headroom = 1.25
        min_ram = 1.0
        cpu_frac = 0.60
    else:
        headroom = 1.20
        min_ram = 1.0
        cpu_frac = 0.50

    if est_mem is None:
        # Conservative default if we don't have a hint:
        # small enough to avoid blocking, but not tiny.
        base_ram = 2.0
    else:
        base_ram = max(min_ram, est_mem * headroom)

    # Add bump if we have failed before
    # attempts=0 => no bump; attempts=1 => +50%; attempts=2 => +125%; etc.
    attempts = int(st.get("attempts", 0))
    bump = float(st.get("ram_bump_gb", 0.0))
    if attempts > 0 and bump <= 0:
        # If we haven't recorded an explicit bump, bump multiplicatively.
        bump = base_ram * (0.5 * (2 ** (attempts - 1)))

    target_ram = base_ram + max(0.0, bump)

    # Clamp within pool bounds
    target_ram = min(target_ram, float(pool.max_ram_pool))
    target_ram = min(target_ram, float(pool.avail_ram_pool))

    # CPU choice
    avail_cpu = float(pool.avail_cpu_pool)
    if avail_cpu <= 0:
        return 0.0, 0.0

    target_cpu = max(1.0, avail_cpu * cpu_frac)
    target_cpu = min(target_cpu, float(pool.max_cpu_pool))
    target_cpu = min(target_cpu, avail_cpu)

    # Ensure we never allocate 0 RAM if pool has RAM (avoid pathological cases)
    if target_ram <= 0 and float(pool.avail_ram_pool) > 0:
        target_ram = min(1.0, float(pool.avail_ram_pool))

    return target_ram, target_cpu


def _pool_can_fit_op(pool, ram_need, cpu_need):
    if cpu_need <= 0 or ram_need <= 0:
        return False
    return (float(pool.avail_cpu_pool) >= cpu_need) and (float(pool.avail_ram_pool) >= ram_need)


def _best_pool_for_op(s, op, priority):
    """
    Choose a pool for the op using a memory-aware heuristic:
      - Prefer pools where (estimated) RAM fits.
      - Among feasible pools, choose the one with the most remaining RAM after placement
        (reduces immediate OOM risk from noise) and then most CPU.
    """
    best = None
    best_score = None

    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        if pool.avail_cpu_pool <= 0 or pool.avail_ram_pool <= 0:
            continue

        ram_need, cpu_need = _choose_ram_cpu(s, pool, op, priority)

        # If estimator exists and says "way too large", skip quickly (avoid doomed placement).
        est_mem = _estimate_ram_gb(op)
        if est_mem is not None and est_mem > float(pool.avail_ram_pool) * 1.05:
            continue

        if not _pool_can_fit_op(pool, ram_need, cpu_need):
            continue

        # Score: higher is better
        remaining_ram = float(pool.avail_ram_pool) - ram_need
        remaining_cpu = float(pool.avail_cpu_pool) - cpu_need
        score = (remaining_ram, remaining_cpu)

        if best is None or score > best_score:
            best = (pool_id, ram_need, cpu_need)
            best_score = score

    return best  # (pool_id, ram_need, cpu_need) or None


@register_scheduler(key="scheduler_est_036")
def scheduler_est_036(s, results: list, pipelines: list):
    """
    Scheduling step:
      1) Enqueue new pipelines into priority queues.
      2) Use results to update per-operator retry sizing on failures.
      3) For each pool, repeatedly pick the next highest-priority pipeline with an assignable op
         that fits in some pool (and preferably this one via global best-pool selection),
         then issue one-op assignments until the pool is saturated or no feasible work exists.

    No preemption is used (suspensions list empty) to avoid churn and wasted work.
    """
    # Enqueue newly arrived pipelines
    for p in pipelines:
        _enqueue_pipeline(s, p)

    # Update retry state based on failures
    for r in results:
        # If an op failed, increase its RAM bump to reduce repeated failures.
        if hasattr(r, "failed") and r.failed():
            # Increase for each op in the container, conservatively
            for op in getattr(r, "ops", []) or []:
                opk = _op_key(op)
                prev = s.op_state.get(opk, {"attempts": 0, "ram_bump_gb": 0.0})
                attempts = int(prev.get("attempts", 0)) + 1

                # If the container had RAM allocated, bump relative to that (strong signal).
                prev_ram = float(getattr(r, "ram", 0.0) or 0.0)
                # Add extra headroom each attempt; grows quickly to reach feasibility.
                add_bump = max(1.0, prev_ram * 0.75)

                s.op_state[opk] = {
                    "attempts": attempts,
                    "ram_bump_gb": float(prev.get("ram_bump_gb", 0.0)) + add_bump,
                }

    # If nothing changed, bail out quickly
    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    # Clean front of queues from completed pipelines (keep FIFO behavior)
    for q in _all_queues(s):
        i = 0
        while i < len(q):
            p = q[i]
            if _pipeline_done_or_failed(p):
                q.pop(i)
                continue
            i += 1

    # Try to fill pools; create at most one container assignment per loop iteration per pool
    # to avoid overcommitting based on noisy estimates.
    made_progress = True
    while made_progress:
        made_progress = False

        # For each pool, attempt one assignment
        for pool_id in range(s.executor.num_pools):
            pool = s.executor.pools[pool_id]
            if pool.avail_cpu_pool <= 0 or pool.avail_ram_pool <= 0:
                continue

            # Select next op using priority queues; allow skipping pipelines that are blocked
            # (no assignable ops) while keeping FIFO ordering among runnable ones.
            chosen = None  # (pipeline, op, best_pool_tuple)
            for q in _all_queues(s):
                # Scan a small window to avoid head-of-line blocking while preserving FIFO-ish ordering
                scan_n = min(8, len(q))
                for idx in range(scan_n):
                    p = q[idx]
                    if _pipeline_done_or_failed(p):
                        continue
                    op = _get_one_assignable_op(p)
                    if op is None:
                        continue

                    best = _best_pool_for_op(s, op, p.priority)
                    if best is None:
                        # Doesn't fit anywhere right now; skip
                        continue

                    # Prefer ops whose best pool is the current pool (local fill),
                    # otherwise we may schedule it when iterating its chosen pool.
                    best_pool_id, ram_need, cpu_need = best
                    if best_pool_id != pool_id:
                        continue

                    chosen = (p, op, best)
                    break
                if chosen is not None:
                    break

            # If no op prefers this pool, try any op that can fit THIS pool (second pass)
            if chosen is None:
                for q in _all_queues(s):
                    scan_n = min(8, len(q))
                    for idx in range(scan_n):
                        p = q[idx]
                        if _pipeline_done_or_failed(p):
                            continue
                        op = _get_one_assignable_op(p)
                        if op is None:
                            continue

                        ram_need, cpu_need = _choose_ram_cpu(s, pool, op, p.priority)

                        # Quick estimator gate
                        est_mem = _estimate_ram_gb(op)
                        if est_mem is not None and est_mem > float(pool.avail_ram_pool) * 1.10:
                            continue

                        if not _pool_can_fit_op(pool, ram_need, cpu_need):
                            continue

                        chosen = (p, op, (pool_id, ram_need, cpu_need))
                        break
                    if chosen is not None:
                        break

            if chosen is None:
                continue

            p, op, (best_pool_id, ram_need, cpu_need) = chosen

            # Issue a one-op assignment
            assignment = Assignment(
                ops=[op],
                cpu=cpu_need,
                ram=ram_need,
                priority=p.priority,
                pool_id=best_pool_id,
                pipeline_id=p.pipeline_id,
            )
            assignments.append(assignment)
            made_progress = True

            # Mild fairness: rotate the chosen pipeline to the back of its priority queue
            # so a single pipeline with many ready ops doesn't monopolize the queue.
            if p.priority == Priority.QUERY:
                q = s.q_query
            elif p.priority == Priority.INTERACTIVE:
                q = s.q_interactive
            else:
                q = s.q_batch

            # Remove and append to end if still active
            for i in range(len(q)):
                if q[i].pipeline_id == p.pipeline_id:
                    q.pop(i)
                    break
            if not _pipeline_done_or_failed(p):
                q.append(p)

    return suspensions, assignments
