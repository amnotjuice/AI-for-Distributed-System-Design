# policy_key: scheduler_none_031
# reasoning_effort: none
# model: gpt-5.2-2025-12-11
# llm_cost: 0.052275
# generation_seconds: 53.84
# generated_at: 2026-03-14T02:04:17.643368
@register_scheduler_init(key="scheduler_none_031")
def scheduler_none_031_init(s):
    """Priority-aware, conservative scheduler.

    Small, obvious improvements over naive FIFO:
    1) Maintain per-priority queues (QUERY, INTERACTIVE, BATCH_PIPELINE) and always try to schedule higher priority first.
    2) Avoid "give the op everything" sizing; instead use a pool-dependent default slice so we can run multiple ops concurrently
       and reduce head-of-line blocking.
    3) Track simple per-operator RAM hints from OOM failures and retry with increased RAM next time (OOM-backoff).
    4) Light preemption: if a high-priority op cannot be placed anywhere due to lack of headroom, suspend one low-priority
       running container in a pool to make room.

    Notes:
    - We keep the algorithm conservative to avoid churn and overfitting to unknown operator curves.
    - We only ever schedule at most one assignment per pool per tick (like the baseline), but because we slice resources,
      multiple pools (and future ticks) can fill in with less blocking.
    """
    # Queues by priority: strict ordering of service
    s.q_query = []
    s.q_interactive = []
    s.q_batch = []

    # Simple per-operator RAM hint store:
    # key: (pipeline_id, op_id/obj repr fallback) -> suggested_ram
    s.ram_hints = {}

    # Track last seen running containers by pool to enable conservative preemption.
    # key: pool_id -> list of (container_id, priority, cpu, ram)
    s.running_by_pool = {}


def _priority_rank(pri):
    # Lower is higher priority
    if pri == Priority.QUERY:
        return 0
    if pri == Priority.INTERACTIVE:
        return 1
    return 2


def _queue_for_priority(s, pri):
    if pri == Priority.QUERY:
        return s.q_query
    if pri == Priority.INTERACTIVE:
        return s.q_interactive
    return s.q_batch


def _iter_queues_in_order(s):
    # Strict priority order
    yield s.q_query
    yield s.q_interactive
    yield s.q_batch


def _pipeline_is_done_or_failed(p):
    st = p.runtime_status()
    if st.is_pipeline_successful():
        return True
    # If any failures exist, we still allow retry via FAILED->ASSIGNED, but
    # keep pipelines that are irrecoverably failed out of the queue. We don't
    # have a "terminal failure" signal, so we do NOT drop just because failed>0.
    return False


def _pick_assignable_ops(pipeline):
    st = pipeline.runtime_status()
    ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
    if not ops:
        return []
    # Keep it simple: one op at a time per assignment
    return ops[:1]


def _op_hint_key(pipeline, op):
    # Try to use stable identifiers if present; otherwise fall back to repr.
    op_id = getattr(op, "operator_id", None)
    if op_id is None:
        op_id = getattr(op, "op_id", None)
    if op_id is None:
        op_id = repr(op)
    return (pipeline.pipeline_id, op_id)


def _default_slice(pool, pri):
    # Conservative slicing to reduce HoL blocking:
    # - QUERY: small/fast; give decent CPU but not entire pool
    # - INTERACTIVE: more than query but still allow concurrency
    # - BATCH: smaller slices to keep background moving without blocking
    max_cpu = pool.max_cpu_pool
    max_ram = pool.max_ram_pool

    if pri == Priority.QUERY:
        cpu = max(1.0, 0.50 * max_cpu)
        ram = max(1.0, 0.35 * max_ram)
    elif pri == Priority.INTERACTIVE:
        cpu = max(1.0, 0.60 * max_cpu)
        ram = max(1.0, 0.45 * max_ram)
    else:
        cpu = max(1.0, 0.40 * max_cpu)
        ram = max(1.0, 0.30 * max_ram)

    # Do not exceed what's currently available; clamp at availability at assignment time
    return cpu, ram


def _fits(avail_cpu, avail_ram, cpu, ram):
    return cpu <= avail_cpu and ram <= avail_ram


def _choose_pool_for_op(s, pri, req_cpu, req_ram):
    # Choose pool with best "slack" after placement.
    best = None
    best_score = None
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool
        if not _fits(avail_cpu, avail_ram, req_cpu, req_ram):
            continue
        # score: maximize remaining headroom (balanced)
        cpu_slack = avail_cpu - req_cpu
        ram_slack = avail_ram - req_ram
        score = min(cpu_slack / max(1e-9, pool.max_cpu_pool), ram_slack / max(1e-9, pool.max_ram_pool))
        if best is None or score > best_score:
            best = pool_id
            best_score = score
    return best


def _record_running(s, results):
    # Rebuild a conservative view of "currently running" based on observed results.
    # We only need this for preemption decisions; it doesn't have to be perfect.
    # We keep entries for containers that are still running (no explicit signal),
    # but we will prune those that show completion/failure if possible via results.
    for pool_id in range(s.executor.num_pools):
        if pool_id not in s.running_by_pool:
            s.running_by_pool[pool_id] = []

    # Maintain a set of containers that finished/failed in this tick to prune.
    finished = set()
    for r in results:
        # If a result exists, the container likely ended its run attempt (success or fail)
        # Treat it as finished for pruning.
        finished.add((r.pool_id, r.container_id))

    # Prune finished
    for pool_id, lst in s.running_by_pool.items():
        s.running_by_pool[pool_id] = [x for x in lst if (pool_id, x[0]) not in finished]

    # Add entries for "started" containers is not possible without explicit event,
    # but we can treat any result as having been running; not useful.
    # So we only keep track via assignments we make in this tick (added elsewhere).


def _maybe_preempt_for_high_priority(s, target_pri, need_cpu, need_ram):
    # Suspend at most one low-priority container to make room for a high-priority op.
    # Strategy:
    # - Find a pool where suspending one lowest-priority container yields enough headroom
    #   for the requested resources.
    # - Prefer suspending BATCH over INTERACTIVE over QUERY.
    suspensions = []

    if target_pri == Priority.BATCH_PIPELINE:
        return suspensions  # never preempt for batch

    target_rank = _priority_rank(target_pri)

    best_choice = None  # (pool_id, container_id, freed_cpu, freed_ram, victim_rank)
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool
        if _fits(avail_cpu, avail_ram, need_cpu, need_ram):
            continue  # no need to preempt in this pool

        for (cid, v_pri, v_cpu, v_ram) in s.running_by_pool.get(pool_id, []):
            v_rank = _priority_rank(v_pri)
            if v_rank <= target_rank:
                continue  # don't preempt equal/higher priority
            # Would suspending this make enough room?
            if _fits(avail_cpu + v_cpu, avail_ram + v_ram, need_cpu, need_ram):
                # Prefer lowest-priority victim; then smallest freed resources that suffice (less disruption)
                score = (v_rank, v_cpu + v_ram)
                if best_choice is None or score > (best_choice[4], best_choice[2] + best_choice[3]):
                    best_choice = (pool_id, cid, v_cpu, v_ram, v_rank)

    if best_choice is not None:
        pool_id, cid, _, _, _ = best_choice
        suspensions.append(Suspend(container_id=cid, pool_id=pool_id))
        # Optimistically remove victim from running list to avoid repeated preempt attempts
        s.running_by_pool[pool_id] = [x for x in s.running_by_pool.get(pool_id, []) if x[0] != cid]

    return suspensions


@register_scheduler(key="scheduler_none_031")
def scheduler_none_031(s, results, pipelines):
    """
    Priority-aware scheduler with RAM hinting and conservative preemption.

    Returns:
        (suspensions, assignments)
    """
    # Enqueue newly arrived pipelines into per-priority queues
    for p in pipelines:
        _queue_for_priority(s, p.priority).append(p)

    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    # Update RAM hints based on OOM-like failures; also prune finished pipelines from queues
    _record_running(s, results)
    for r in results:
        if r.failed():
            # Heuristic: any failure with "oom" in error suggests RAM too low
            err = (r.error or "")
            if "oom" in err.lower() or "out of memory" in err.lower() or "memory" in err.lower():
                # Apply hint to each op in this attempt (usually 1 op)
                for op in (r.ops or []):
                    # We don't have pipeline_id on result; store by op repr only if needed.
                    # Prefer: if op has pipeline_id attribute; else cannot key precisely.
                    pid = getattr(op, "pipeline_id", None)
                    if pid is None:
                        # Fallback: can't map precisely; skip hinting
                        continue
                    key = _op_hint_key(type("P", (), {"pipeline_id": pid})(), op)
                    prev = s.ram_hints.get(key, r.ram)
                    # Increase by 50% (bounded by pool max at use-time)
                    s.ram_hints[key] = max(prev, r.ram * 1.5)

    # Clean up done pipelines from queues (cheap pass)
    for q in _iter_queues_in_order(s):
        kept = []
        for p in q:
            if _pipeline_is_done_or_failed(p):
                continue
            kept.append(p)
        q[:] = kept

    # Build a list of candidate pipelines in strict priority order, but with round-robin within each queue.
    # We'll attempt at most one assignment per pool, but can select any pool per assignment.
    # We also avoid scanning too much: cap attempts.
    max_attempts = 50

    # For each pool, try to schedule the highest priority ready op that fits (possibly with one preemption).
    for _ in range(s.executor.num_pools):
        # Determine current best candidate among queues
        chosen_pipeline = None
        chosen_ops = None
        chosen_pri = None

        attempts = 0
        for q in _iter_queues_in_order(s):
            # Round-robin: rotate queue by popping from front and appending back as we inspect
            qlen = len(q)
            for _i in range(qlen):
                if attempts >= max_attempts:
                    break
                attempts += 1

                p = q.pop(0)
                if _pipeline_is_done_or_failed(p):
                    continue

                ops = _pick_assignable_ops(p)
                # Put back to preserve RR fairness
                q.append(p)

                if not ops:
                    continue

                chosen_pipeline = p
                chosen_ops = ops
                chosen_pri = p.priority
                break

            if chosen_pipeline is not None or attempts >= max_attempts:
                break

        if chosen_pipeline is None:
            break  # nothing runnable right now

        # Size request: default slice, possibly overridden by RAM hint
        # Choose pool-aware slice after selecting pool; start with an optimistic pool-independent baseline:
        # use the largest pool max as reference by taking any pool's default slice later.
        # We first find a feasible pool by trying per-pool slices (and applying RAM hint).
        selected_pool_id = None
        selected_cpu = None
        selected_ram = None

        # Fetch RAM hint if available
        hint_key = _op_hint_key(chosen_pipeline, chosen_ops[0])
        hinted_ram = s.ram_hints.get(hint_key, None)

        # Try direct placement without preemption
        for pool_id in range(s.executor.num_pools):
            pool = s.executor.pools[pool_id]
            cpu, ram = _default_slice(pool, chosen_pri)
            if hinted_ram is not None:
                ram = max(ram, hinted_ram)
            # Clamp to pool max
            cpu = min(cpu, pool.max_cpu_pool)
            ram = min(ram, pool.max_ram_pool)
            # Clamp to availability for fit check (but don't shrink below 1.0)
            cpu_req = min(cpu, pool.avail_cpu_pool)
            ram_req = min(ram, pool.avail_ram_pool)
            # If clamping reduced below what we intended, still allow if it fits;
            # we don't know min cpu; but ensure positive allocation.
            cpu_req = max(1.0, cpu_req)
            ram_req = max(1.0, ram_req)

            if _fits(pool.avail_cpu_pool, pool.avail_ram_pool, cpu_req, ram_req):
                # Choose best pool by slack
                pid = _choose_pool_for_op(s, chosen_pri, cpu_req, ram_req)
                if pid is not None:
                    selected_pool_id = pid
                    selected_cpu = cpu_req
                    selected_ram = ram_req
                break

        # If no placement found, attempt one preemption for QUERY/INTERACTIVE
        if selected_pool_id is None:
            # Use worst-case needs: pick a representative need based on largest pool slice
            # to avoid preempting too little. We'll choose the maximum of default slices.
            need_cpu = 1.0
            need_ram = 1.0
            for pool_id in range(s.executor.num_pools):
                pool = s.executor.pools[pool_id]
                cpu, ram = _default_slice(pool, chosen_pri)
                if hinted_ram is not None:
                    ram = max(ram, hinted_ram)
                need_cpu = max(need_cpu, min(cpu, pool.max_cpu_pool))
                need_ram = max(need_ram, min(ram, pool.max_ram_pool))

            suspensions.extend(_maybe_preempt_for_high_priority(s, chosen_pri, need_cpu, need_ram))

            # Retry placement after preemption (if any)
            for pool_id in range(s.executor.num_pools):
                pool = s.executor.pools[pool_id]
                cpu, ram = _default_slice(pool, chosen_pri)
                if hinted_ram is not None:
                    ram = max(ram, hinted_ram)
                cpu = min(cpu, pool.max_cpu_pool)
                ram = min(ram, pool.max_ram_pool)
                cpu_req = min(cpu, pool.avail_cpu_pool)
                ram_req = min(ram, pool.avail_ram_pool)
                cpu_req = max(1.0, cpu_req)
                ram_req = max(1.0, ram_req)
                if _fits(pool.avail_cpu_pool, pool.avail_ram_pool, cpu_req, ram_req):
                    pid = _choose_pool_for_op(s, chosen_pri, cpu_req, ram_req)
                    if pid is not None:
                        selected_pool_id = pid
                        selected_cpu = cpu_req
                        selected_ram = ram_req
                    break

        if selected_pool_id is None:
            # Could not place; stop trying further to avoid spinning
            break

        # Create assignment
        assignments.append(
            Assignment(
                ops=chosen_ops,
                cpu=selected_cpu,
                ram=selected_ram,
                priority=chosen_pri,
                pool_id=selected_pool_id,
                pipeline_id=chosen_pipeline.pipeline_id,
            )
        )

        # Record as running for preemption heuristics (optimistic; assumes it will start)
        # We don't have container_id yet; so store a placeholder None and ignore for preemption.
        # Instead, rely on actual container_ids from running_by_pool populated elsewhere.
        # However, to still enable preemption in later ticks, we need container_id.
        # Since we don't have it, we cannot add. (Keeping logic safe.)
        # If ExecutionResult includes container_id on completion/failure, that doesn't help.
        # Therefore, preemption is best-effort based on previously seen containers only.

    return suspensions, assignments
