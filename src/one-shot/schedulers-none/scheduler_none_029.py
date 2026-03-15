# policy_key: scheduler_none_029
# reasoning_effort: none
# model: gpt-5.2-2025-12-11
# llm_cost: 0.049153
# generation_seconds: 54.01
# generated_at: 2026-03-14T02:02:41.915005
@register_scheduler_init(key="scheduler_none_029")
def scheduler_none_029_init(s):
    """Priority-aware FIFO with minimal, safe improvements over the naive baseline.

    Direction of improvement (kept intentionally small/safe):
    1) Separate waiting queues by priority and schedule higher priority first.
    2) Keep per-pool "soft reservations" for high-priority work to protect latency.
    3) Simple OOM backoff: if an op OOMs at some RAM, retry it with higher RAM next time.
    4) Very conservative preemption: only if a high-priority op is ready and there is
       not enough headroom, suspend a low-priority running container in that pool.

    Notes:
    - We avoid aggressive binpacking and multi-op per assignment to keep behavior predictable.
    - We do not assume detailed operator resource metadata; we learn RAM needs from OOMs.
    """

    # Priority order: lower index is more important
    s._prio_order = [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]

    # Separate FIFO queues per priority
    s._waiting = {p: [] for p in s._prio_order}

    # RAM retry hints keyed by (pipeline_id, op_id)
    # Value is the next RAM to try for that op.
    s._ram_hint = {}

    # Conservative multiplicative increase on OOM
    s._oom_ram_mult = 1.5
    s._oom_ram_add = 0.5  # in "RAM units" of the simulator (kept small; exact units abstract)

    # Soft reservations per pool to protect latency.
    # Keep some CPU/RAM available for QUERY/INTERACTIVE; allow BATCH to use it only if idle.
    # Fractions of pool max.
    s._reserve_frac = {
        Priority.QUERY: (0.15, 0.15),         # (cpu_frac, ram_frac)
        Priority.INTERACTIVE: (0.10, 0.10),
        Priority.BATCH_PIPELINE: (0.0, 0.0),
    }

    # Track last known running containers by pool to enable conservative preemption
    # pool_id -> list of dicts(container_id, priority, cpu, ram)
    s._running = {i: [] for i in range(s.executor.num_pools)}


def _prio_rank(s, prio):
    for i, p in enumerate(s._prio_order):
        if p == prio:
            return i
    return len(s._prio_order)


def _update_running_from_results(s, results):
    # Rebuild running lists incrementally from completion/failure signals.
    # We assume each ExecutionResult corresponds to a container that ended (success or fail).
    if not results:
        return

    # Remove any container_ids that appear in results from our running lists.
    ended = set()
    for r in results:
        if getattr(r, "container_id", None) is not None:
            ended.add((r.pool_id, r.container_id))

    for pool_id, lst in s._running.items():
        if not lst or not ended:
            continue
        s._running[pool_id] = [x for x in lst if (pool_id, x["container_id"]) not in ended]


def _learn_from_results(s, results):
    # Learn RAM hints on OOM-like failures.
    for r in results or []:
        if not r.failed():
            continue

        # If we can detect OOM from error string, increase RAM hint.
        err = str(getattr(r, "error", "") or "").lower()
        is_oom = ("oom" in err) or ("out of memory" in err) or ("memory" in err and "kill" in err)
        if not is_oom:
            continue

        # We only have ops in result; take first op as representative for hinting.
        ops = getattr(r, "ops", None) or []
        if not ops:
            continue
        op = ops[0]
        op_id = getattr(op, "op_id", None)
        if op_id is None:
            # Fallback: try common alternative field names
            op_id = getattr(op, "operator_id", None) or getattr(op, "id", None)
        if op_id is None:
            continue

        pipeline_id = getattr(op, "pipeline_id", None)
        if pipeline_id is None:
            # Fallback: some implementations may not attach pipeline_id to op;
            # in that case, we cannot key reliably.
            continue

        key = (pipeline_id, op_id)
        prev = s._ram_hint.get(key, None)
        base = float(getattr(r, "ram", 0) or 0)
        if base <= 0:
            # If we don't know previous RAM, don't set a hint.
            continue

        bumped = max(base + s._oom_ram_add, base * s._oom_ram_mult)
        if prev is None or bumped > prev:
            s._ram_hint[key] = bumped


def _pick_next_ready_op(pipeline):
    status = pipeline.runtime_status()
    # Only schedule assignable ops whose parents are complete
    op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
    if not op_list:
        return None
    return op_list[0]


def _pipeline_done_or_failed(pipeline):
    status = pipeline.runtime_status()
    if status.is_pipeline_successful():
        return True
    # If any failures exist, we drop the pipeline (same as baseline).
    # Note: this is conservative; a future iteration could retry specific ops.
    return status.state_counts[OperatorState.FAILED] > 0


def _get_op_id(op):
    op_id = getattr(op, "op_id", None)
    if op_id is None:
        op_id = getattr(op, "operator_id", None) or getattr(op, "id", None)
    return op_id


def _desired_allocation_for_op(s, pool, pipeline, op, prio):
    # Minimal approach: give the op a reasonable slice rather than "all available".
    # - For high priority, bias to more CPU to reduce latency.
    # - For batch, take a smaller slice to allow sharing and reduce head-of-line blocking.
    max_cpu = pool.max_cpu_pool
    max_ram = pool.max_ram_pool
    avail_cpu = pool.avail_cpu_pool
    avail_ram = pool.avail_ram_pool

    # Default RAM is a modest slice; OOM hints can bump it.
    # Keep within avail/max.
    if prio == Priority.QUERY:
        cpu_target = min(avail_cpu, max(1.0, 0.5 * max_cpu))
        ram_target = min(avail_ram, max(1.0, 0.5 * max_ram))
    elif prio == Priority.INTERACTIVE:
        cpu_target = min(avail_cpu, max(1.0, 0.4 * max_cpu))
        ram_target = min(avail_ram, max(1.0, 0.4 * max_ram))
    else:
        cpu_target = min(avail_cpu, max(1.0, 0.25 * max_cpu))
        ram_target = min(avail_ram, max(1.0, 0.25 * max_ram))

    # Apply learned RAM hint if present
    op_id = _get_op_id(op)
    if op_id is not None:
        key = (pipeline.pipeline_id, op_id)
        hint = s._ram_hint.get(key, None)
        if hint is not None:
            ram_target = min(avail_ram, max(ram_target, hint))

    # Never exceed pool max (just in case avail > max due to sim specifics)
    cpu_target = min(cpu_target, max_cpu)
    ram_target = min(ram_target, max_ram)

    return float(cpu_target), float(ram_target)


def _reservation_amounts(s, pool, prio):
    cpu_frac, ram_frac = s._reserve_frac.get(prio, (0.0, 0.0))
    return pool.max_cpu_pool * cpu_frac, pool.max_ram_pool * ram_frac


def _can_admit_under_reservation(s, pool, prio, cpu_need, ram_need):
    # For each priority, enforce that lower-priority work doesn't consume reserved headroom
    # intended for higher-priority work. Implemented as:
    # - QUERY can use everything.
    # - INTERACTIVE must leave QUERY's reservation untouched.
    # - BATCH must leave QUERY + INTERACTIVE reservations untouched.
    avail_cpu = pool.avail_cpu_pool
    avail_ram = pool.avail_ram_pool

    reserve_cpu = 0.0
    reserve_ram = 0.0
    if prio == Priority.INTERACTIVE:
        r_cpu, r_ram = _reservation_amounts(s, pool, Priority.QUERY)
        reserve_cpu += r_cpu
        reserve_ram += r_ram
    elif prio == Priority.BATCH_PIPELINE:
        for hp in (Priority.QUERY, Priority.INTERACTIVE):
            r_cpu, r_ram = _reservation_amounts(s, pool, hp)
            reserve_cpu += r_cpu
            reserve_ram += r_ram

    # Effective capacity for this prio is what remains after reservation.
    eff_cpu = max(0.0, avail_cpu - reserve_cpu)
    eff_ram = max(0.0, avail_ram - reserve_ram)
    return cpu_need <= eff_cpu and ram_need <= eff_ram


def _maybe_preempt_for_high_priority(s, pool_id, pool, needed_cpu, needed_ram):
    # Conservative preemption within the same pool:
    # If we don't have enough raw avail resources, suspend one low-priority container.
    # We pick the lowest priority container first (batch before interactive).
    avail_cpu = pool.avail_cpu_pool
    avail_ram = pool.avail_ram_pool
    if avail_cpu >= needed_cpu and avail_ram >= needed_ram:
        return []

    running = s._running.get(pool_id, [])
    if not running:
        return []

    # Sort by lowest priority first (largest rank)
    running_sorted = sorted(running, key=lambda x: _prio_rank(s, x["priority"]), reverse=True)

    # Only preempt BATCH first; if still insufficient, then INTERACTIVE (never QUERY).
    suspensions = []
    freed_cpu = 0.0
    freed_ram = 0.0
    for c in running_sorted:
        if c["priority"] == Priority.QUERY:
            continue
        # Suspend this container
        suspensions.append(Suspend(c["container_id"], pool_id))
        freed_cpu += float(c.get("cpu", 0) or 0)
        freed_ram += float(c.get("ram", 0) or 0)
        # Stop after suspending one container to minimize churn
        break

    # If suspending one doesn't help enough, return it anyway (maybe future ticks accumulate).
    return suspensions


@register_scheduler(key="scheduler_none_029")
def scheduler_none_029(s, results, pipelines):
    """
    Priority-aware scheduler with soft reservations + conservative preemption + OOM RAM backoff.

    Returns:
        (suspensions, assignments)
    """
    # Incorporate new pipelines into per-priority FIFO queues
    for p in pipelines or []:
        s._waiting[p.priority].append(p)

    # Update internal running bookkeeping and learn OOM hints
    _update_running_from_results(s, results or [])
    _learn_from_results(s, results or [])

    # Early exit if nothing changed
    if not (pipelines or results):
        return [], []

    suspensions = []
    assignments = []

    # Clean completed/failed pipelines from queues while preserving order
    for prio in s._prio_order:
        newq = []
        for p in s._waiting[prio]:
            if _pipeline_done_or_failed(p):
                continue
            newq.append(p)
        s._waiting[prio] = newq

    # For each pool, try to schedule at most one op per tick (like baseline) to keep stable.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]

        # If no resources, skip
        if pool.avail_cpu_pool <= 0 or pool.avail_ram_pool <= 0:
            continue

        made_assignment = False

        # Highest priority first across queues
        for prio in s._prio_order:
            q = s._waiting[prio]
            if not q:
                continue

            # Find first pipeline with a ready op (scan FIFO; keep FIFO-ish)
            pick_idx = None
            pick_pipeline = None
            pick_op = None
            for i, p in enumerate(q):
                if _pipeline_done_or_failed(p):
                    continue
                op = _pick_next_ready_op(p)
                if op is None:
                    continue
                pick_idx = i
                pick_pipeline = p
                pick_op = op
                break

            if pick_pipeline is None:
                continue

            # Decide allocation
            cpu_need, ram_need = _desired_allocation_for_op(s, pool, pick_pipeline, pick_op, prio)

            # If high priority, allow conservative preemption to create headroom
            if prio in (Priority.QUERY, Priority.INTERACTIVE):
                # Only attempt preemption if raw resources insufficient (not reservation-based)
                susp = _maybe_preempt_for_high_priority(s, pool_id, pool, cpu_need, ram_need)
                if susp:
                    # Enqueue suspensions and stop scheduling in this pool this tick;
                    # the freed resources will appear next tick deterministically.
                    suspensions.extend(susp)
                    made_assignment = True  # treat as "did work" to avoid batch stealing
                    break

            # Enforce reservation policy for lower priorities
            if not _can_admit_under_reservation(s, pool, prio, cpu_need, ram_need):
                # Can't admit this priority on this pool now; try next pool (or next prio)
                continue

            # Also require raw availability
            if cpu_need > pool.avail_cpu_pool or ram_need > pool.avail_ram_pool:
                continue

            # Create assignment for a single op
            assignment = Assignment(
                ops=[pick_op],
                cpu=cpu_need,
                ram=ram_need,
                priority=pick_pipeline.priority,
                pool_id=pool_id,
                pipeline_id=pick_pipeline.pipeline_id,
            )
            assignments.append(assignment)

            # Track it as running (best-effort; container_id unknown until results, so we
            # track a placeholder and won't preempt it until we get a container_id in results).
            # However, Suspend requires container_id, so we only preempt containers we know.
            # To still get some preemption benefit, we rely on known running containers only.
            # (If the simulator provides container_id at assignment time in the future, adapt.)
            made_assignment = True

            # Pop from queue and reappend to preserve round-robin within same priority
            q.pop(pick_idx)
            q.append(pick_pipeline)
            break

        if made_assignment:
            continue

    # Best-effort: augment running list with containers that started this tick if results contain them.
    # Some simulators may emit "started" events; if so, capture them here.
    for r in results or []:
        cid = getattr(r, "container_id", None)
        if cid is None:
            continue
        pool_id = r.pool_id
        # If it's already tracked, skip
        existing = s._running.get(pool_id, [])
        if any(x["container_id"] == cid for x in existing):
            continue
        # Only track if it appears to be a running-like event; lacking explicit state,
        # we conservatively track all container_ids seen in results with cpu/ram.
        s._running.setdefault(pool_id, []).append(
            {
                "container_id": cid,
                "priority": r.priority,
                "cpu": float(getattr(r, "cpu", 0) or 0),
                "ram": float(getattr(r, "ram", 0) or 0),
            }
        )

    return suspensions, assignments
