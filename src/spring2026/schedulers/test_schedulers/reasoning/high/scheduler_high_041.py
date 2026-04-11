# policy_key: scheduler_high_041
# reasoning_effort: high
# exp: reasoning
# model: gpt-5.2-2025-12-11
# llm_cost: 0.108203
# generation_seconds: 208.34
# generated_at: 2026-04-10T03:09:46.928584
@register_scheduler_init(key="scheduler_high_041")
def scheduler_high_041_init(s):
    """Priority-aware, OOM-resilient scheduler.

    Main ideas:
      - Weighted deficit round-robin across priority classes (query:interactive:batch = 10:5:1)
        to protect high-priority latency while still guaranteeing batch progress (avoid 720s penalties).
      - OOM-aware per-operator RAM estimation with exponential backoff on failure to maximize completion rate.
      - Soft resource reservation: when high-priority work is queued, batch work cannot consume the last
        slice of CPU/RAM in a pool (reduces queueing for queries/interactive).
      - Gentle sizing: queries get more CPU, batch gets smaller CPU slices to reduce interference.
    """
    s.queues = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }

    # Track which pipelines are currently enqueued/active so we don't duplicate them across ticks.
    s.active_pipeline_ids = set()

    # Deficit round-robin state (credits).
    s.quantum = {
        Priority.QUERY: 10,
        Priority.INTERACTIVE: 5,
        Priority.BATCH_PIPELINE: 1,
    }
    s.deficit = {k: 0 for k in s.quantum}

    # Per-operator resource estimates and attempt counts.
    # Keys are (pipeline_id, op_id-ish).
    s.op_ram_est = {}
    s.op_cpu_est = {}
    s.op_attempts = {}

    # Pipeline "age" bookkeeping (for mild anti-starvation).
    s.tick = 0
    s.pipeline_last_service_tick = {}  # pipeline_id -> tick of last successful assignment (or enqueue)

    # Tunables
    s.scan_limit = 32               # max pipelines to rotate per queue when searching for a runnable op
    s.max_assignments_per_pool = 64 # avoid pathological long loops in a single tick
    s.reserve_frac_query = 0.20     # reserve fraction (for lower-priority) when queries are waiting
    s.reserve_frac_interactive = 0.12  # reserve when only interactive is waiting
    s.batch_aging_ticks = 60        # after this, batch gets a small effective priority boost


def _safe_num(x, default=None):
    try:
        if x is None:
            return default
        return float(x)
    except Exception:
        return default


def _lower_str(x):
    try:
        return str(x).lower()
    except Exception:
        return ""


def _is_oom(result):
    # Best-effort detection; simulator may encode OOM in error strings.
    msg = _lower_str(getattr(result, "error", ""))
    return ("oom" in msg) or ("out of memory" in msg) or ("memory" in msg and "exceed" in msg)


def _get_attr_any(obj, names):
    for n in names:
        if hasattr(obj, n):
            v = getattr(obj, n)
            if v is not None:
                return v
    return None


def _infer_op_ram_min(op):
    # Best-effort: different generators may name this differently.
    direct = _get_attr_any(
        op,
        [
            "ram_min", "min_ram", "min_memory",
            "ram_req", "ram_requirement", "mem_req",
            "memory_min", "peak_ram", "peak_memory",
            "ram", "memory", "mem",
        ],
    )
    v = _safe_num(direct, default=None)
    if v is not None and v > 0:
        return v

    # Possibly nested under a 'resources' object
    res = getattr(op, "resources", None)
    if res is not None:
        nested = _get_attr_any(res, ["ram", "memory", "mem", "ram_min", "min_ram"])
        v2 = _safe_num(nested, default=None)
        if v2 is not None and v2 > 0:
            return v2

    return None


def _infer_op_cpu_hint(op):
    direct = _get_attr_any(op, ["cpu", "vcpu", "cpu_req", "cpu_requirement"])
    v = _safe_num(direct, default=None)
    if v is not None and v > 0:
        return v
    res = getattr(op, "resources", None)
    if res is not None:
        nested = _get_attr_any(res, ["cpu", "vcpu"])
        v2 = _safe_num(nested, default=None)
        if v2 is not None and v2 > 0:
            return v2
    return None


def _op_key(pipeline_id, op):
    # Use stable operator identifier if present; otherwise use object identity.
    oid = _get_attr_any(op, ["op_id", "operator_id", "id", "name"])
    if oid is None:
        oid = id(op)
    return (pipeline_id, oid)


def _reserve_for_lower_priority(s, pool, has_query_waiting, has_interactive_waiting):
    if has_query_waiting:
        frac = s.reserve_frac_query
    elif has_interactive_waiting:
        frac = s.reserve_frac_interactive
    else:
        frac = 0.0
    return (pool.max_cpu_pool * frac, pool.max_ram_pool * frac)


def _base_cpu_target(priority, pool):
    # Give queries more CPU to reduce tail latency; keep batch smaller to reduce interference.
    if priority == Priority.QUERY:
        return max(1.0, pool.max_cpu_pool * 0.75)
    if priority == Priority.INTERACTIVE:
        return max(1.0, pool.max_cpu_pool * 0.50)
    return max(1.0, pool.max_cpu_pool * 0.25)


def _base_ram_target(priority, pool):
    # Conservative defaults to reduce OOM risk (OOMs are costly for completion/score).
    if priority == Priority.QUERY:
        return max(1.0, pool.max_ram_pool * 0.18)
    if priority == Priority.INTERACTIVE:
        return max(1.0, pool.max_ram_pool * 0.14)
    return max(1.0, pool.max_ram_pool * 0.10)


def _ram_request(s, pipeline, op, pool):
    prio = pipeline.priority
    key = _op_key(pipeline.pipeline_id, op)
    est = _safe_num(s.op_ram_est.get(key), default=None)

    ram_min = _infer_op_ram_min(op)
    if est is None:
        # Initial estimate: max(inferred minimum * headroom, base default by priority).
        if ram_min is not None:
            est = max(_base_ram_target(prio, pool), ram_min * (1.25 if prio != Priority.BATCH_PIPELINE else 1.15))
        else:
            est = _base_ram_target(prio, pool)

    # If we've already tried and failed, avoid being too aggressive about shrinking.
    # Keep estimate within pool bounds.
    est = max(1.0, min(float(est), float(pool.max_ram_pool)))

    # Mild extra headroom for high priority to reduce retries.
    if prio in (Priority.QUERY, Priority.INTERACTIVE):
        est = min(pool.max_ram_pool, est * 1.05)

    return est


def _cpu_request(s, pipeline, op, pool, avail_cpu):
    prio = pipeline.priority
    key = _op_key(pipeline.pipeline_id, op)
    est = _safe_num(s.op_cpu_est.get(key), default=None)

    base = _base_cpu_target(prio, pool)
    hint = _infer_op_cpu_hint(op)
    if est is None:
        # If we have a hint, respect it but bias by priority.
        if hint is not None:
            est = max(1.0, min(pool.max_cpu_pool, hint))
            if prio == Priority.QUERY:
                est = min(pool.max_cpu_pool, max(est, base))
            elif prio == Priority.INTERACTIVE:
                est = min(pool.max_cpu_pool, max(est, base))
            else:
                est = min(pool.max_cpu_pool, min(est, base))
        else:
            est = base

    # Avoid taking the entire pool if there is room for concurrency; but never below 1.
    est = max(1.0, min(float(est), float(pool.max_cpu_pool)))

    # If resources are tight, allow CPU to shrink (slower but avoids blocking and OOM risk is RAM-driven).
    return max(1.0, min(est, float(avail_cpu)))


def _ensure_deficits(s):
    # Refill deficits if exhausted (classic DRR).
    if all(s.deficit[p] <= 0 for p in s.deficit):
        for p, q in s.quantum.items():
            # Cap burstiness to keep scheduling stable.
            s.deficit[p] = min(s.deficit[p] + q, 3 * q)


def _nonempty_priorities(s):
    return [p for p, q in s.queues.items() if q]


def _pick_priority(s):
    prios = _nonempty_priorities(s)
    if not prios:
        return None
    _ensure_deficits(s)

    # Mild aging boost for batch if it has been waiting long (reduces end-of-sim incompletes).
    # We implement this by temporarily bumping the effective deficit for batch when its head-of-line age is large.
    eff = {}
    for p in prios:
        d = s.deficit.get(p, 0)
        if p == Priority.BATCH_PIPELINE and s.queues[p]:
            pid = s.queues[p][0].pipeline_id
            last = s.pipeline_last_service_tick.get(pid, s.tick)
            age = s.tick - last
            if age >= s.batch_aging_ticks:
                d = d + 3  # small boost; doesn't overpower query weights but prevents starvation
        eff[p] = d

    # Pick the priority with the highest (effective) deficit; tie-break by quantum then by fixed order.
    order = {Priority.QUERY: 3, Priority.INTERACTIVE: 2, Priority.BATCH_PIPELINE: 1}
    return max(prios, key=lambda p: (eff[p], s.quantum.get(p, 0), order.get(p, 0)))


def _dequeue_runnable_that_fits(s, prio, pool, avail_cpu, avail_ram, reserve_cpu, reserve_ram, has_query_waiting):
    """Rotate within the priority queue to find a runnable op that fits this pool."""
    q = s.queues[prio]
    if not q:
        return None

    scans = min(len(q), s.scan_limit)
    for _ in range(scans):
        pipeline = q.pop(0)

        status = pipeline.runtime_status()
        if status.is_pipeline_successful():
            # Cleanup completed pipelines.
            s.active_pipeline_ids.discard(pipeline.pipeline_id)
            s.pipeline_last_service_tick.pop(pipeline.pipeline_id, None)
            continue

        op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
        if not op_list:
            # Not runnable yet; rotate to preserve fairness within the class.
            q.append(pipeline)
            continue

        op = op_list[0]
        need_ram = _ram_request(s, pipeline, op, pool)

        # If the pool cannot ever satisfy the estimate, clamp to max to try making progress.
        # (If the true requirement exceeds max, the workload is infeasible; but we still try.)
        need_ram = min(need_ram, float(pool.max_ram_pool))

        need_cpu = _cpu_request(s, pipeline, op, pool, avail_cpu)

        # Soft reservation: only applies when scheduling lower priority while queries are waiting.
        if has_query_waiting and prio != Priority.QUERY:
            if (avail_cpu - need_cpu) < reserve_cpu or (avail_ram - need_ram) < reserve_ram:
                q.append(pipeline)
                continue

        # For batch (and interactive), also apply reservation for "some high-priority work waiting".
        if prio == Priority.BATCH_PIPELINE:
            if (avail_cpu - need_cpu) < reserve_cpu or (avail_ram - need_ram) < reserve_ram:
                q.append(pipeline)
                continue

        if need_cpu <= avail_cpu and need_ram <= avail_ram:
            return pipeline, op_list, need_cpu, need_ram

        # Doesn't fit right now; rotate.
        q.append(pipeline)

    return None


@register_scheduler(key="scheduler_high_041")
def scheduler_high_041(s, results, pipelines):
    """
    Scheduler step:
      - Enqueue new pipelines by priority (no dropping).
      - Update per-op RAM estimates on failures (OOM -> exponential increase).
      - For each pool, greedily assign runnable operators using weighted DRR and soft reservations.
    """
    s.tick += 1

    # Enqueue arrivals (avoid duplication).
    for p in pipelines:
        if p.pipeline_id in s.active_pipeline_ids:
            continue
        s.active_pipeline_ids.add(p.pipeline_id)
        s.queues[p.priority].append(p)
        s.pipeline_last_service_tick.setdefault(p.pipeline_id, s.tick)

    # Process execution results to update estimates (especially on OOM).
    for r in results:
        ops = getattr(r, "ops", []) or []
        pool_id = getattr(r, "pool_id", None)
        pool = s.executor.pools[pool_id] if (pool_id is not None and 0 <= pool_id < s.executor.num_pools) else None

        for op in ops:
            # Use pipeline_id from result if present; otherwise best-effort from op or skip.
            pipeline_id = getattr(r, "pipeline_id", None)
            if pipeline_id is None:
                pipeline_id = getattr(op, "pipeline_id", None)
            if pipeline_id is None:
                # Can't reliably key; skip learning.
                continue

            key = _op_key(pipeline_id, op)
            prev_attempts = s.op_attempts.get(key, 0)

            if r.failed():
                s.op_attempts[key] = prev_attempts + 1

                last_ram = _safe_num(getattr(r, "ram", None), default=s.op_ram_est.get(key, 1.0))
                last_cpu = _safe_num(getattr(r, "cpu", None), default=s.op_cpu_est.get(key, 1.0))

                if _is_oom(r):
                    new_ram = max(1.0, float(last_ram) * 2.0)
                else:
                    # Non-OOM failure: still increase RAM a bit (sometimes errors correlate with memory pressure),
                    # and also allow CPU to rise mildly.
                    new_ram = max(1.0, float(last_ram) * 1.5)

                if pool is not None:
                    new_ram = min(float(pool.max_ram_pool), new_ram)

                # Never decrease estimate on failures.
                s.op_ram_est[key] = max(float(s.op_ram_est.get(key, 1.0)), float(new_ram))

                if not _is_oom(r):
                    if pool is not None:
                        new_cpu = min(float(pool.max_cpu_pool), max(1.0, float(last_cpu) * 1.25))
                    else:
                        new_cpu = max(1.0, float(last_cpu) * 1.25)
                    s.op_cpu_est[key] = max(float(s.op_cpu_est.get(key, 1.0)), float(new_cpu))
            else:
                # Success: record a working baseline (do not aggressively shrink; OOMs are expensive).
                ram_used = _safe_num(getattr(r, "ram", None), default=None)
                cpu_used = _safe_num(getattr(r, "cpu", None), default=None)
                if ram_used is not None:
                    if key not in s.op_ram_est:
                        s.op_ram_est[key] = float(ram_used)
                if cpu_used is not None:
                    if key not in s.op_cpu_est:
                        s.op_cpu_est[key] = float(cpu_used)

    # Early exit optimization: if nothing arrived/finished, avoid extra work.
    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    # Pool ordering: schedule on the most available pools first to reduce head-of-line blocking.
    pool_ids = list(range(s.executor.num_pools))
    pool_ids.sort(
        key=lambda i: (
            (s.executor.pools[i].avail_cpu_pool / max(1.0, s.executor.pools[i].max_cpu_pool))
            + (s.executor.pools[i].avail_ram_pool / max(1.0, s.executor.pools[i].max_ram_pool))
        ),
        reverse=True,
    )

    # Flags for reservations.
    has_query_waiting = len(s.queues[Priority.QUERY]) > 0
    has_interactive_waiting = len(s.queues[Priority.INTERACTIVE]) > 0

    for pool_id in pool_ids:
        pool = s.executor.pools[pool_id]
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)

        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        reserve_cpu, reserve_ram = _reserve_for_lower_priority(s, pool, has_query_waiting, has_interactive_waiting)

        made = 0
        while avail_cpu > 0 and avail_ram > 0 and made < s.max_assignments_per_pool:
            prio = _pick_priority(s)
            if prio is None:
                break

            # Try to find a runnable op in the chosen priority; if none fits, reduce its deficit and try others.
            candidate = _dequeue_runnable_that_fits(
                s=s,
                prio=prio,
                pool=pool,
                avail_cpu=avail_cpu,
                avail_ram=avail_ram,
                reserve_cpu=reserve_cpu,
                reserve_ram=reserve_ram,
                has_query_waiting=has_query_waiting,
            )

            if candidate is None:
                # If chosen priority can't place anything in this pool right now, burn its credit and try next.
                s.deficit[prio] = min(s.deficit.get(prio, 0), 0)
                # If nothing can be scheduled across any class, exit the pool loop.
                # Quick check: attempt at most once for each priority class.
                found_any = False
                for _ in range(2):
                    prio2 = _pick_priority(s)
                    if prio2 is None:
                        break
                    candidate2 = _dequeue_runnable_that_fits(
                        s=s,
                        prio=prio2,
                        pool=pool,
                        avail_cpu=avail_cpu,
                        avail_ram=avail_ram,
                        reserve_cpu=reserve_cpu,
                        reserve_ram=reserve_ram,
                        has_query_waiting=has_query_waiting,
                    )
                    if candidate2 is not None:
                        candidate = candidate2
                        prio = prio2
                        found_any = True
                        break
                    s.deficit[prio2] = min(s.deficit.get(prio2, 0), 0)

                if not found_any and candidate is None:
                    break

            pipeline, op_list, need_cpu, need_ram = candidate

            # Create assignment.
            assignments.append(
                Assignment(
                    ops=op_list,
                    cpu=float(need_cpu),
                    ram=float(need_ram),
                    priority=pipeline.priority,
                    pool_id=pool_id,
                    pipeline_id=pipeline.pipeline_id,
                )
            )

            # Update local availability (avoid overcommitting within this tick).
            avail_cpu -= float(need_cpu)
            avail_ram -= float(need_ram)

            # Consume deficit token.
            s.deficit[prio] = s.deficit.get(prio, 0) - 1

            # Mark service time for aging.
            s.pipeline_last_service_tick[pipeline.pipeline_id] = s.tick

            made += 1

        # Update waiting flags after scheduling on this pool (queues may have changed).
        has_query_waiting = len(s.queues[Priority.QUERY]) > 0
        has_interactive_waiting = len(s.queues[Priority.INTERACTIVE]) > 0

    return suspensions, assignments
