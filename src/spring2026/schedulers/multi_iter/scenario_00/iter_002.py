@register_scheduler_init(key="scheduler_low_001")
def scheduler_low_001_init(s):
    # Latest Pipeline object per id (Pipeline objects may be refreshed across ticks)
    s.pipelines_by_id = {}  # pipeline_id -> Pipeline

    # Per-priority round-robin queues (store pipeline_ids, not Pipeline objects)
    s.queues_by_prio = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }
    s.rr_idx_by_prio = {
        Priority.QUERY: 0,
        Priority.INTERACTIVE: 0,
        Priority.BATCH_PIPELINE: 0,
    }
    s.queued_ids = set()  # pipeline_ids currently enqueued (best-effort; lazily cleaned)

    # RAM adaptation:
    # - pipeline-level boost: coarse multiplicative hedge after OOMs
    # - op-level hints: remember "this op needs at least ~X RAM" from failures
    s.pipeline_ram_boost = {}  # pipeline_id -> float
    s.op_ram_hint = {}  # (pipeline_id, op_key) -> float
    s.max_ram_boost = 16.0

    # Non-OOM failure retry shaping (never drop; incomplete pipelines are heavily penalized)
    s.failed_seen_count = {}  # pipeline_id -> int

    # Scheduling knobs
    s.min_cpu = 1.0
    s.min_ram = 1.0

    # Per-tick guards
    s.max_assignments_per_pool_per_tick = 256
    s.max_total_assignments_per_tick = 2048

    # Limit multiple assignments from same pipeline in a single tick (status doesn't update mid-call)
    s.max_assignments_per_pid_per_tick = {
        Priority.QUERY: 2,
        Priority.INTERACTIVE: 1,
        Priority.BATCH_PIPELINE: 1,
    }

    # Headroom reservation to protect high-priority latency
    s.reserve_frac_for_query = 0.15
    s.reserve_frac_for_interactive = 0.10

    # Pool preference (if multiple pools): prefer pool 0 for high-priority; avoid it for batch unless needed
    s.batch_avoid_pool0_penalty = 10_000.0
    s.highprio_prefer_pool0_bonus = -5.0


def _is_oom_error(err) -> bool:
    if not err:
        return False
    msg = str(err).lower()
    return ("oom" in msg) or ("out of memory" in msg) or ("killed" in msg and "memory" in msg)


def _prio_order():
    return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]


def _drop_pipeline_id(s, pipeline_id):
    s.pipelines_by_id.pop(pipeline_id, None)
    s.queued_ids.discard(pipeline_id)
    s.pipeline_ram_boost.pop(pipeline_id, None)
    s.failed_seen_count.pop(pipeline_id, None)
    # Keep op_ram_hint entries; they are keyed by (pid, ...) so naturally become unreachable


def _maybe_enqueue(s, p):
    pid = p.pipeline_id
    s.pipelines_by_id[pid] = p  # always refresh reference

    # Don't enqueue already-successful pipelines
    try:
        if p.runtime_status().is_pipeline_successful():
            _drop_pipeline_id(s, pid)
            return
    except Exception:
        pass

    if pid in s.queued_ids:
        return

    s.queues_by_prio[p.priority].append(pid)
    s.queued_ids.add(pid)


def _op_key(op):
    # Try to build a stable-ish key; fall back to string (works within a tick; across ticks hints are best-effort)
    for attr in ("op_id", "operator_id", "node_id", "task_id", "name", "uid", "id"):
        try:
            v = getattr(op, attr, None)
            if v is not None:
                return str(v)
        except Exception:
            pass
    try:
        return str(op)
    except Exception:
        return str(id(op))


def _base_cpu_for(pool, prio) -> float:
    # Sublinear scaling with cluster size: grows ~sqrt(pool), capped to avoid over-scaling single ops.
    try:
        m = float(pool.max_cpu_pool)
    except Exception:
        m = 1.0
    base = m ** 0.5
    if base < 2.0:
        base = 2.0
    if base > 16.0:
        base = 16.0

    mult = 1.0
    if prio == Priority.QUERY:
        mult = 1.5
    elif prio == Priority.INTERACTIVE:
        mult = 1.0
    else:
        mult = 0.75

    cpu = base * mult
    if cpu < 1.0:
        cpu = 1.0
    if cpu > 32.0:
        cpu = 32.0
    return cpu


def _base_ram_for(pool, prio) -> float:
    # Sublinear scaling with cluster size, with a generous cap to avoid pathological fragmentation.
    try:
        m = float(pool.max_ram_pool)
    except Exception:
        m = 1.0
    base = (m ** 0.5) * 2.0  # e.g., 256GB -> ~32GB
    if base < 4.0:
        base = 4.0
    if base > 128.0:
        base = 128.0

    mult = 1.0
    if prio == Priority.QUERY:
        mult = 1.25
    elif prio == Priority.INTERACTIVE:
        mult = 1.0
    else:
        mult = 0.90

    ram = base * mult
    if ram < 1.0:
        ram = 1.0
    # Never request more than half a pool by default (boost/hints can exceed base, but still cap later)
    half_pool = m * 0.5
    if ram > half_pool:
        ram = half_pool
    return ram


def _reserve_for_higher_prio(s, pool, any_query_waiting: bool, any_interactive_waiting: bool):
    max_cpu = float(pool.max_cpu_pool)
    max_ram = float(pool.max_ram_pool)

    reserve_cpu = 0.0
    reserve_ram = 0.0

    if any_query_waiting:
        reserve_cpu += max_cpu * float(s.reserve_frac_for_query)
        reserve_ram += max_ram * float(s.reserve_frac_for_query)
    if any_interactive_waiting:
        reserve_cpu += max_cpu * float(s.reserve_frac_for_interactive)
        reserve_ram += max_ram * float(s.reserve_frac_for_interactive)

    return reserve_cpu, reserve_ram


def _compute_request(s, pool, pid, prio, op_list):
    cpu = _base_cpu_for(pool, prio)
    base_ram = _base_ram_for(pool, prio)

    boost = float(s.pipeline_ram_boost.get(pid, 1.0))
    if boost < 1.0:
        boost = 1.0
    if boost > s.max_ram_boost:
        boost = float(s.max_ram_boost)

    # Op-level hint: if we've OOM'd this op before, start at (or above) the hint.
    hint = 0.0
    if op_list:
        try:
            k = (pid, _op_key(op_list[0]))
            hint = float(s.op_ram_hint.get(k, 0.0))
        except Exception:
            hint = 0.0

    ram = base_ram
    if hint > ram:
        ram = hint

    # Apply pipeline-level boost multiplicatively (aggressive on OOM; improves completion rate)
    ram = ram * boost

    # Global caps
    max_ram_cap = float(pool.max_ram_pool) * 0.90
    if ram > max_ram_cap:
        ram = max_ram_cap

    if cpu < s.min_cpu:
        cpu = float(s.min_cpu)
    if ram < s.min_ram:
        ram = float(s.min_ram)

    return cpu, ram


def _next_pid_rr(s, prio, pipelines_scheduled_this_tick):
    q = s.queues_by_prio[prio]
    if not q:
        return None

    n = len(q)
    if n == 0:
        return None

    start = int(s.rr_idx_by_prio.get(prio, 0))
    if start < 0 or start >= n:
        start = 0

    for i in range(n):
        idx = (start + i) % n
        pid = q[idx]

        if pid in pipelines_scheduled_this_tick:
            continue

        p = s.pipelines_by_id.get(pid)
        if p is None:
            continue

        try:
            if p.runtime_status().is_pipeline_successful():
                # lazily cleaned elsewhere
                continue
        except Exception:
            pass

        s.rr_idx_by_prio[prio] = (idx + 1) % n
        return pid

    return None


def _clean_queues(s):
    for prio in _prio_order():
        q = s.queues_by_prio[prio]
        if not q:
            continue
        newq = []
        for pid in q:
            p = s.pipelines_by_id.get(pid)
            if p is None:
                s.queued_ids.discard(pid)
                continue
            try:
                if p.runtime_status().is_pipeline_successful():
                    _drop_pipeline_id(s, pid)
                    continue
            except Exception:
                pass
            newq.append(pid)
        s.queues_by_prio[prio] = newq
        if s.rr_idx_by_prio.get(prio, 0) >= len(newq):
            s.rr_idx_by_prio[prio] = 0


@register_scheduler(key="scheduler_low_001")
def scheduler_low_001_scheduler(s, results, pipelines):
    # Ingest pipelines
    for p in pipelines:
        _maybe_enqueue(s, p)

    # Adapt RAM boosts and op-level hints from observed results
    for r in results:
        try:
            if getattr(r, "failed", None) and r.failed():
                pid = getattr(r, "pipeline_id", None)
                if pid is None:
                    continue

                # Track non-OOM failures but never drop (incomplete pipelines are heavily penalized)
                s.failed_seen_count[pid] = s.failed_seen_count.get(pid, 0) + 1

                if _is_oom_error(getattr(r, "error", None)):
                    # Increase pipeline boost aggressively
                    cur = float(s.pipeline_ram_boost.get(pid, 1.0))
                    nxt = max(cur * 2.0, cur + 1.0)
                    if nxt > s.max_ram_boost:
                        nxt = float(s.max_ram_boost)
                    s.pipeline_ram_boost[pid] = nxt

                    # Record per-op hint: next time, start at >= 2x the last allocation for that op
                    try:
                        ops = getattr(r, "ops", None) or []
                        if ops:
                            ok = (pid, _op_key(ops[0]))
                            prev = float(s.op_ram_hint.get(ok, 0.0))
                            last_alloc = float(getattr(r, "ram", 0.0) or 0.0)
                            hinted = max(prev, last_alloc * 2.0, 2.0)
                            # Cap to a large value; true hard cap applied per-pool at request time
                            if hinted > 1e9:
                                hinted = 1e9
                            s.op_ram_hint[ok] = hinted
                    except Exception:
                        pass
            else:
                # On success: (optional) soften pipeline boost very slowly to reclaim utilization
                pid = getattr(r, "pipeline_id", None)
                if pid is not None:
                    cur = float(s.pipeline_ram_boost.get(pid, 1.0))
                    if cur > 1.0:
                        # Gentle decay to avoid oscillations
                        cur = max(1.0, cur * 0.98)
                        s.pipeline_ram_boost[pid] = cur
        except Exception:
            pass

    # Lazily clean queues of completed pipelines
    _clean_queues(s)

    suspensions = []
    assignments = []

    # Local pool availability (executor does not update within this call)
    local_avail_cpu = {}
    local_avail_ram = {}
    per_pool_made = {}
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        local_avail_cpu[pool_id] = float(pool.avail_cpu_pool)
        local_avail_ram[pool_id] = float(pool.avail_ram_pool)
        per_pool_made[pool_id] = 0

    # Simple notion of "waiting": if the queue is non-empty, assume there may be runnable work soon.
    any_query_waiting = len(s.queues_by_prio[Priority.QUERY]) > 0
    any_interactive_waiting = len(s.queues_by_prio[Priority.INTERACTIVE]) > 0

    pipelines_scheduled_this_tick = set()
    scheduled_count_by_pid = {}

    # Global greedy loop: schedule in priority stages with best-fit pool selection.
    total_made = 0
    while total_made < s.max_total_assignments_per_tick:
        made_one = False

        for prio in _prio_order():
            pid = _next_pid_rr(s, prio, pipelines_scheduled_this_tick)
            if pid is None:
                continue

            p = s.pipelines_by_id.get(pid)
            if p is None:
                continue

            try:
                status = p.runtime_status()
            except Exception:
                continue

            # Skip completed pipelines
            try:
                if status.is_pipeline_successful():
                    _drop_pipeline_id(s, pid)
                    continue
            except Exception:
                pass

            # Get ready-to-run ops (PENDING or FAILED) whose parents are complete
            max_ops = int(s.max_assignments_per_pid_per_tick.get(prio, 1))
            already = int(scheduled_count_by_pid.get(pid, 0))
            if already >= max_ops:
                continue

            op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
            if not op_list:
                continue
            op_list = op_list[:1]  # one operator per container for predictable resource behavior

            # Find best-fit pool
            best_pool = None
            best_cpu = None
            best_ram = None
            best_score = None

            for pool_id in range(s.executor.num_pools):
                if per_pool_made[pool_id] >= s.max_assignments_per_pool_per_tick:
                    continue

                pool = s.executor.pools[pool_id]
                avail_cpu = float(local_avail_cpu[pool_id])
                avail_ram = float(local_avail_ram[pool_id])

                if avail_cpu < s.min_cpu or avail_ram < s.min_ram:
                    continue

                cpu_req, ram_req = _compute_request(s, pool, pid, prio, op_list)

                # Fit checks with CPU-first flexibility (reduce CPU to fit; avoid reducing RAM below req)
                if ram_req > avail_ram:
                    continue

                cpu = cpu_req
                ram = ram_req

                if cpu > avail_cpu:
                    cpu = avail_cpu
                if cpu < s.min_cpu:
                    continue

                # Headroom reservation (protect higher priority from batch/interference)
                if prio == Priority.BATCH_PIPELINE:
                    reserve_cpu, reserve_ram = _reserve_for_higher_prio(
                        s,
                        pool,
                        any_query_waiting=any_query_waiting,
                        any_interactive_waiting=any_interactive_waiting,
                    )
                    if (avail_cpu - cpu) < reserve_cpu or (avail_ram - ram) < reserve_ram:
                        continue

                if prio == Priority.INTERACTIVE:
                    reserve_cpu, reserve_ram = _reserve_for_higher_prio(
                        s,
                        pool,
                        any_query_waiting=any_query_waiting,
                        any_interactive_waiting=False,
                    )
                    if (avail_cpu - cpu) < reserve_cpu or (avail_ram - ram) < reserve_ram:
                        continue

                # Pool preference: isolate high-priority in pool 0 when possible; keep batch off pool 0
                pref = 0.0
                if s.executor.num_pools > 1:
                    if prio == Priority.BATCH_PIPELINE and pool_id == 0:
                        pref += float(s.batch_avoid_pool0_penalty)
                    if prio in (Priority.QUERY, Priority.INTERACTIVE) and pool_id == 0:
                        pref += float(s.highprio_prefer_pool0_bonus)

                # Best-fit packing (favor tight RAM fit to increase RAM utilization)
                score = (avail_ram - ram) + 0.05 * (avail_cpu - cpu) + pref
                if best_score is None or score < best_score:
                    best_score = score
                    best_pool = pool_id
                    best_cpu = cpu
                    best_ram = ram

            if best_pool is None:
                # Couldn't place this pipeline at this priority; try next priority/pipeline
                continue

            assignments.append(
                Assignment(
                    ops=op_list,
                    cpu=float(best_cpu),
                    ram=float(best_ram),
                    priority=p.priority,
                    pool_id=int(best_pool),
                    pipeline_id=pid,
                )
            )

            # Update local availability
            local_avail_cpu[best_pool] = max(0.0, float(local_avail_cpu[best_pool]) - float(best_cpu))
            local_avail_ram[best_pool] = max(0.0, float(local_avail_ram[best_pool]) - float(best_ram))
            per_pool_made[best_pool] += 1

            pipelines_scheduled_this_tick.add(pid)
            scheduled_count_by_pid[pid] = scheduled_count_by_pid.get(pid, 0) + 1

            total_made += 1
            made_one = True
            break  # restart from highest priority

        if not made_one:
            break

    return suspensions, assignments
