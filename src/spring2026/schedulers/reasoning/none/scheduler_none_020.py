# policy_key: scheduler_none_020
# reasoning_effort: none
# model: gpt-5.2-2025-12-11
# llm_cost: 0.057358
# generation_seconds: 47.33
# generated_at: 2026-04-09T22:00:16.905451
@register_scheduler_init(key="scheduler_none_020")
def scheduler_none_020_init(s):
    """Priority-aware, OOM-adaptive scheduler with guarded preemption.

    Goals (aligned to objective):
    - Keep QUERY + INTERACTIVE latency low via reserved headroom and fast admission.
    - Avoid failures (720s penalty) via per-op RAM learning and cautious CPU sizing.
    - Maintain progress for BATCH via aging and limited preemption churn.

    Key ideas:
    1) Maintain per-pipeline queue; pick ready ops by (priority, age) rather than FIFO.
    2) Allocate conservative RAM using learned "next RAM" from prior failures; never below estimate.
    3) Keep CPU modest per op to pack more concurrent work (improves p95 under load),
       while allowing larger CPU for high priority when headroom exists.
    4) When high-priority work can't fit, preempt (suspend) a low-priority running container
       only if it likely creates enough headroom; otherwise avoid churn.
    """
    # Waiting pipelines (store ids and arrival tick for aging)
    s.waiting = []  # list of dict: {"p": Pipeline, "t": int}

    # Simple tick counter for aging decisions
    s.tick = 0

    # Learned per-operator resource hints keyed by (pipeline_id, op_name)
    # Values: {"ram": float, "fails": int}
    s.op_hints = {}

    # Track running containers from ExecutionResult stream.
    # container_id -> dict: {"pool_id": int, "priority": Priority, "cpu": float, "ram": float, "last_seen": int}
    s.running = {}

    # Policy knobs (conservative defaults)
    s.reserve_frac_query = 0.15  # keep at least this fraction of each pool free if possible
    s.reserve_frac_interactive = 0.10
    s.max_assign_ops_per_pool_per_tick = 2  # avoid flooding a single pool in one tick
    s.max_preemptions_per_tick = 1  # minimize churn

    # RAM backoff when OOM/failure suspected; multiplicative growth
    s.ram_backoff = 1.6
    s.ram_min_floor_frac = 0.10  # never allocate below 10% pool RAM to reduce tiny allocations
    s.ram_cap_frac = 0.95  # never allocate more than this fraction of pool RAM to one assignment

    # CPU sizing: default per-op fraction of pool CPU by priority
    s.cpu_frac_query = 0.50
    s.cpu_frac_interactive = 0.40
    s.cpu_frac_batch = 0.30
    s.cpu_cap_frac = 0.90

    # Batch fairness aging: after this many ticks waiting, treat as higher urgency
    s.batch_aging_ticks = 40


def _prio_weight(prio):
    # Higher is better
    if prio == Priority.QUERY:
        return 3
    if prio == Priority.INTERACTIVE:
        return 2
    return 1


def _reserve_frac(s, prio):
    if prio == Priority.QUERY:
        return s.reserve_frac_query
    if prio == Priority.INTERACTIVE:
        return s.reserve_frac_interactive
    return 0.0


def _cpu_frac(s, prio):
    if prio == Priority.QUERY:
        return s.cpu_frac_query
    if prio == Priority.INTERACTIVE:
        return s.cpu_frac_interactive
    return s.cpu_frac_batch


def _op_key(pipeline, op):
    # op may not have stable attributes in all implementations; try common ones.
    name = getattr(op, "op_id", None) or getattr(op, "name", None) or getattr(op, "operator_id", None) or str(op)
    return (pipeline.pipeline_id, name)


def _update_hints_from_results(s, results):
    # Update running containers map and learn RAM for failures.
    for r in results:
        # Keep a lightweight "last seen" state for preemption candidates
        if getattr(r, "container_id", None) is not None:
            s.running[r.container_id] = {
                "pool_id": r.pool_id,
                "priority": r.priority,
                "cpu": r.cpu,
                "ram": r.ram,
                "last_seen": s.tick,
            }

        # If failed, assume likely OOM if error contains hint; otherwise still backoff gently.
        if hasattr(r, "failed") and r.failed():
            is_oom = False
            err = getattr(r, "error", None)
            if err is not None:
                try:
                    msg = str(err).lower()
                    if "oom" in msg or "out of memory" in msg or "memory" in msg:
                        is_oom = True
                except Exception:
                    pass

            # Apply RAM backoff for each op in the failed result
            for op in getattr(r, "ops", []) or []:
                # We don't have pipeline object here; key by container+op string fallback.
                # Best effort: use op itself with unknown pipeline id (-1); many sims include op unique name.
                # To keep hints useful, also store by op string only in a side channel.
                op_name = getattr(op, "op_id", None) or getattr(op, "name", None) or getattr(op, "operator_id", None) or str(op)
                key_unknown = (-1, op_name)
                hint = s.op_hints.get(key_unknown, {"ram": r.ram, "fails": 0})
                hint["fails"] += 1
                # If OOM suspected, grow more aggressively; else small growth still helps reduce repeat fails.
                grow = s.ram_backoff if is_oom else 1.25
                hint["ram"] = max(hint["ram"], r.ram * grow)
                s.op_hints[key_unknown] = hint

        # If completed successfully, we can consider r.ram as sufficient. Record as lower bound.
        if hasattr(r, "failed") and not r.failed():
            for op in getattr(r, "ops", []) or []:
                op_name = getattr(op, "op_id", None) or getattr(op, "name", None) or getattr(op, "operator_id", None) or str(op)
                key_unknown = (-1, op_name)
                hint = s.op_hints.get(key_unknown, {"ram": r.ram, "fails": 0})
                hint["ram"] = max(hint["ram"], r.ram)  # sufficient RAM lower bound
                s.op_hints[key_unknown] = hint


def _pipeline_done_or_failed(pipeline):
    status = pipeline.runtime_status()
    has_failures = status.state_counts[OperatorState.FAILED] > 0
    return status.is_pipeline_successful() or has_failures


def _get_ready_ops(pipeline, max_ops=1):
    status = pipeline.runtime_status()
    # Pull only ready ops whose parents are complete; retry FAILED is allowed (ASSIGNABLE_STATES includes FAILED).
    return status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:max_ops]


def _pick_next_pipeline(s):
    # Choose pipeline using priority then aging; avoid starvation.
    # Returns index in s.waiting.
    best_i = None
    best_score = None

    for i, item in enumerate(s.waiting):
        p = item["p"]
        t0 = item["t"]
        if _pipeline_done_or_failed(p):
            continue

        # Skip if no ready ops currently
        ops = _get_ready_ops(p, max_ops=1)
        if not ops:
            continue

        age = max(0, s.tick - t0)
        pr = p.priority

        # Batch aging: after enough wait, boost its urgency to reduce 720s incomplete penalties.
        batch_boost = 0
        if pr == Priority.BATCH_PIPELINE and age >= s.batch_aging_ticks:
            batch_boost = 1  # acts like moving closer to interactive

        # Score: priority weight dominates; age adds tie-break and anti-starvation.
        score = (_prio_weight(pr) + batch_boost) * 1000 + age

        if best_score is None or score > best_score:
            best_score = score
            best_i = i

    return best_i


def _estimate_ram_for_op(s, pool, pipeline, op):
    # Use best-effort hint: pipeline-specific key, then unknown-pipeline key by op name.
    key = _op_key(pipeline, op)
    hint = s.op_hints.get(key)
    if hint is None:
        op_name = key[1]
        hint = s.op_hints.get((-1, op_name))

    # Start with a conservative fraction of pool RAM if no hint exists
    base = max(pool.max_ram_pool * s.ram_min_floor_frac, 0.0)
    if hint is not None and "ram" in hint and hint["ram"] is not None:
        base = max(base, float(hint["ram"]))

    # Never exceed cap fraction (leave room for others / avoid single-op monopolization)
    return min(base, pool.max_ram_pool * s.ram_cap_frac)


def _size_cpu_ram(s, pool, pipeline, op):
    # RAM: conservative to avoid OOM, but bounded to avoid monopolization
    ram = _estimate_ram_for_op(s, pool, pipeline, op)

    # CPU: allocate a fraction of pool, but no more than available; also keep some reserve for future arrivals.
    frac = _cpu_frac(s, pipeline.priority)
    cpu_target = pool.max_cpu_pool * frac
    cpu_target = min(cpu_target, pool.max_cpu_pool * s.cpu_cap_frac)

    # Enforce at least 1 vCPU if available (common simulation assumption); but keep float-safe.
    cpu = max(1.0, cpu_target) if pool.max_cpu_pool >= 1.0 else cpu_target

    return cpu, ram


def _choose_pool_for_assignment(s, pipeline, op):
    # Prefer pool with most available RAM (to avoid OOM retries) but also enough CPU.
    best = None
    best_score = None
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool
        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        cpu, ram = _size_cpu_ram(s, pool, pipeline, op)

        # Apply reservation: try to keep some fraction unallocated for high-priority workloads.
        reserve = _reserve_frac(s, pipeline.priority)
        # Soft constraint: if allocating would dip below reserve, penalize (but allow if otherwise impossible).
        after_ram = avail_ram - ram
        after_cpu = avail_cpu - cpu
        reserve_ram = pool.max_ram_pool * reserve
        reserve_cpu = pool.max_cpu_pool * reserve

        fits = (after_ram >= 0) and (after_cpu >= 0)
        if not fits:
            continue

        penalty = 0
        if after_ram < reserve_ram:
            penalty += (reserve_ram - after_ram) / max(1e-9, pool.max_ram_pool)
        if after_cpu < reserve_cpu:
            penalty += (reserve_cpu - after_cpu) / max(1e-9, pool.max_cpu_pool)

        # Prefer higher remaining headroom and lower penalty
        score = (after_ram / max(1e-9, pool.max_ram_pool)) + (after_cpu / max(1e-9, pool.max_cpu_pool)) - 2.0 * penalty
        if best_score is None or score > best_score:
            best_score = score
            best = (pool_id, cpu, ram)

    return best  # (pool_id, cpu, ram) or None


def _pick_preemption_candidate(s, target_pool_id):
    # Find a running low-priority container in the same pool to suspend.
    # Prefer BATCH, then INTERACTIVE (never preempt QUERY).
    candidate = None
    candidate_score = None
    for cid, info in s.running.items():
        if info["pool_id"] != target_pool_id:
            continue
        pr = info["priority"]
        if pr == Priority.QUERY:
            continue

        # Lower priority and larger resource footprint are better to preempt (frees more headroom).
        pr_rank = 2 if pr == Priority.BATCH_PIPELINE else 1  # batch best to preempt
        freed = float(info.get("ram", 0.0)) + 0.5 * float(info.get("cpu", 0.0))
        score = pr_rank * 1000 + freed

        if candidate_score is None or score > candidate_score:
            candidate_score = score
            candidate = (cid, info["pool_id"])

    return candidate  # (container_id, pool_id) or None


@register_scheduler(key="scheduler_none_020")
def scheduler_none_020(s, results, pipelines):
    """
    Step function: update hints, enqueue arrivals, then repeatedly:
    - pick best next pipeline (priority + aging),
    - pick one ready op,
    - place it on best pool with conservative sizing,
    - if no fit and op is high priority, attempt limited preemption.
    """
    s.tick += 1

    # Update learned hints and running state from results
    if results:
        _update_hints_from_results(s, results)

        # Opportunistically add pipeline-specific hints if we can map op->pipeline via waiting list
        # (best-effort: if op objects are shared, we can still store by (-1, op_name) already).
        # No additional mapping attempted to avoid reliance on simulator internals.

    # Enqueue new pipelines with arrival tick
    for p in pipelines:
        s.waiting.append({"p": p, "t": s.tick})

    # Early exit if nothing to do
    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    # Clean queue: drop completed/failed pipelines to avoid re-scanning
    new_waiting = []
    for item in s.waiting:
        if not _pipeline_done_or_failed(item["p"]):
            new_waiting.append(item)
    s.waiting = new_waiting

    # Attempt to schedule a few assignments per pool per tick
    preemptions_left = s.max_preemptions_per_tick
    assigns_left_per_pool = {pid: s.max_assign_ops_per_pool_per_tick for pid in range(s.executor.num_pools)}

    # Outer loop bounded to avoid infinite attempts
    max_attempts = max(1, len(s.waiting) * 2)
    attempts = 0
    while attempts < max_attempts:
        attempts += 1

        idx = _pick_next_pipeline(s)
        if idx is None:
            break

        pipeline = s.waiting[idx]["p"]
        ops = _get_ready_ops(pipeline, max_ops=1)
        if not ops:
            # Nothing ready; skip this tick for this pipeline
            continue
        op = ops[0]

        placement = _choose_pool_for_assignment(s, pipeline, op)
        if placement is None:
            # If high-priority, try preemption in the pool with max available RAM.
            if pipeline.priority in (Priority.QUERY, Priority.INTERACTIVE) and preemptions_left > 0:
                # Choose pool to preempt from: max available RAM (more likely to be close to fitting)
                best_pool = None
                best_avail = None
                for pool_id in range(s.executor.num_pools):
                    pool = s.executor.pools[pool_id]
                    avail = pool.avail_ram_pool
                    if best_avail is None or avail > best_avail:
                        best_avail = avail
                        best_pool = pool_id

                if best_pool is not None:
                    cand = _pick_preemption_candidate(s, best_pool)
                    if cand is not None:
                        cid, pool_id = cand
                        suspensions.append(Suspend(container_id=cid, pool_id=pool_id))
                        preemptions_left -= 1
                        # After preemption, stop scheduling this tick to let resources "return" in next step.
                        # This reduces churn and respects simulator's event timing.
                        break

            # Can't place; avoid dropping. Let it wait.
            # Ageing will eventually boost batches; queries/interactive will trigger preemption attempts.
            continue

        pool_id, cpu, ram = placement

        # Respect per-pool per-tick cap
        if assigns_left_per_pool.get(pool_id, 0) <= 0:
            # Try another pipeline or wait for next tick
            continue

        pool = s.executor.pools[pool_id]
        # Clamp to currently available
        cpu = min(cpu, pool.avail_cpu_pool)
        ram = min(ram, pool.avail_ram_pool)
        if cpu <= 0 or ram <= 0:
            continue

        # Create assignment (one op at a time to reduce blast radius of wrong sizing)
        assignment = Assignment(
            ops=[op],
            cpu=cpu,
            ram=ram,
            priority=pipeline.priority,
            pool_id=pool_id,
            pipeline_id=pipeline.pipeline_id,
        )
        assignments.append(assignment)
        assigns_left_per_pool[pool_id] = assigns_left_per_pool.get(pool_id, 0) - 1

        # Rotate pipeline to end to provide fairness among same-priority pipelines
        item = s.waiting.pop(idx)
        s.waiting.append(item)

        # Continue scheduling more if possible
        continue

    return suspensions, assignments
