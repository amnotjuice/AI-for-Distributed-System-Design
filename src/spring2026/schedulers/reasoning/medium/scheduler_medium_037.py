# policy_key: scheduler_medium_037
# reasoning_effort: medium
# exp: reasoning
# model: gpt-5.2-2025-12-11
# llm_cost: 0.090227
# generation_seconds: 97.46
# generated_at: 2026-04-09T23:20:02.650804
@register_scheduler_init(key="scheduler_medium_037")
def scheduler_medium_037_init(s):
    """
    Priority + fairness + OOM-avoidance policy.

    Goals:
      - Protect QUERY/INTERACTIVE latency via strict priority ordering + soft reservations.
      - Avoid the heavy 720s penalty by being conservative with RAM and retrying FAILED ops
        with exponentially increased RAM (OOM-aware backoff).
      - Avoid starvation via round-robin within each priority + aging-based promotion for
        long-waiting lower-priority pipelines.
      - Optional preemption (best-effort) if the simulator exposes running containers.
    """
    from collections import deque

    s.tick = 0

    # Per-priority RR queues (store pipeline_id; actual objects kept in s.pipelines_by_id)
    s.q_query = deque()
    s.q_interactive = deque()
    s.q_batch = deque()

    s.pipelines_by_id = {}   # pipeline_id -> Pipeline
    s.meta = {}              # pipeline_id -> dict(enqueue_tick, last_dequeue_tick, last_assign_tick, fail_count, cooldown_until)

    # Per-operator RAM estimate to reduce OOM failures: (pipeline_id, op_key) -> ram
    s.op_ram_est = {}
    s.op_fail_count = {}

    # If we cannot find pipeline_id in ExecutionResult, we cannot do per-op learning.
    # Most simulators propagate pipeline_id; handle best-effort if present.
    s.last_seen_pipeline_ids = set()

    # Aging: after this many ticks, allow lower-priority to compete more aggressively
    s.aging_ticks_interactive = 25
    s.aging_ticks_batch = 60

    # Retry backoff: after a failure, wait a couple ticks before retrying to avoid thrash
    s.failure_cooldown_ticks = 2

    # Hard limit to prevent infinite thrash if an operator is fundamentally broken;
    # still keep trying but escalate to "max RAM" sizing.
    s.fail_escalate_after = 3


def _prio_rank(priority):
    # Smaller is more important
    if priority == Priority.QUERY:
        return 0
    if priority == Priority.INTERACTIVE:
        return 1
    return 2


def _get_queue(s, priority):
    if priority == Priority.QUERY:
        return s.q_query
    if priority == Priority.INTERACTIVE:
        return s.q_interactive
    return s.q_batch


def _safe_getattr(obj, names, default=None):
    for n in names:
        if hasattr(obj, n):
            return getattr(obj, n)
    return default


def _op_key(op):
    # Try common identifiers; fall back to stable repr
    k = _safe_getattr(op, ["op_id", "operator_id", "id", "name", "uid"], None)
    if k is not None:
        return str(k)
    return repr(op)


def _op_min_ram(op):
    v = _safe_getattr(op, ["min_ram", "ram_min", "memory_min", "min_memory", "min_rss", "ram_requirement"], None)
    if v is None:
        return 0.0
    try:
        return float(v)
    except Exception:
        return 0.0


def _compute_target_resources(s, pool, pipeline, op):
    """
    Compute a conservative (cpu, ram) request:
      - RAM: max(estimate, min_ram*safety, priority_floor), capped to avoid monopolizing pool.
      - CPU: moderate shares by priority (sublinear speedups); avoid consuming whole pool.
    """
    import math

    pr = pipeline.priority
    pool_max_cpu = float(pool.max_cpu_pool)
    pool_max_ram = float(pool.max_ram_pool)

    # Priority-based sizing heuristics
    if pr == Priority.QUERY:
        cpu_share = 0.60
        ram_floor = 0.18 * pool_max_ram
        ram_cap = 0.70 * pool_max_ram
        min_cpu = 1.0
    elif pr == Priority.INTERACTIVE:
        cpu_share = 0.45
        ram_floor = 0.15 * pool_max_ram
        ram_cap = 0.65 * pool_max_ram
        min_cpu = 1.0
    else:
        cpu_share = 0.30
        ram_floor = 0.10 * pool_max_ram
        ram_cap = 0.60 * pool_max_ram
        min_cpu = 1.0

    # Operator-specific minimum, if exposed
    min_ram = _op_min_ram(op)
    safety = 1.50 if pr != Priority.BATCH_PIPELINE else 1.35
    base_from_min = min_ram * safety

    # Learned estimate
    est_key = (pipeline.pipeline_id, _op_key(op))
    learned = s.op_ram_est.get(est_key, 0.0)

    # If the op failed multiple times, escalate aggressively
    fail_count = s.op_fail_count.get(est_key, 0)
    if fail_count >= s.fail_escalate_after:
        ram_target = min(ram_cap, 0.95 * pool_max_ram)
    else:
        ram_target = max(base_from_min, learned, ram_floor)
        ram_target = min(ram_target, ram_cap)

    # CPU target: moderate; ensure at least 1, cap to avoid full monopolization
    cpu_target = max(min_cpu, math.floor(pool_max_cpu * cpu_share))
    cpu_target = max(1.0, float(cpu_target))
    cpu_target = min(cpu_target, max(1.0, pool_max_cpu - 0.5))  # keep a bit of headroom when possible

    return cpu_target, ram_target


def _pick_next_pipeline_rr(s, q, now_tick, require_ready_op=True):
    """
    Round-robin dequeue with filtering:
      - Skip pipelines in cooldown.
      - Optionally require that at least one op is ready (parents complete).
    Returns a Pipeline or None.
    """
    n = len(q)
    if n == 0:
        return None

    for _ in range(n):
        pid = q.popleft()
        p = s.pipelines_by_id.get(pid)
        if p is None:
            continue

        st = p.runtime_status()
        if st.is_pipeline_successful():
            continue

        meta = s.meta.get(pid, {})
        if meta.get("cooldown_until", -1) > now_tick:
            # Put back and skip
            q.append(pid)
            continue

        if require_ready_op:
            ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
            if not ops:
                # Not ready; keep in queue
                q.append(pid)
                continue

        # Eligible
        meta["last_dequeue_tick"] = now_tick
        s.meta[pid] = meta
        q.append(pid)  # keep RR by re-adding immediately; we schedule at most one op per selection
        return p

    return None


def _pipeline_age_ticks(s, pid, now_tick):
    meta = s.meta.get(pid, {})
    enq = meta.get("enqueue_tick", now_tick)
    return max(0, now_tick - enq)


def _aged_priority_order(s, now_tick):
    """
    Build a priority order list, but allow aging:
      - INTERACTIVE older than threshold can be treated as QUERY-adjacent only in tie-breaking.
      - BATCH older than threshold can compete with INTERACTIVE when no QUERY work is pending.
    """
    # Base order always protects queries first.
    order = [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]

    # If no queries are waiting/ready, we may reorder interactive vs batch based on aging.
    # We keep it simple to avoid starving interactive.
    return order


def _list_running_containers_best_effort(pool):
    """
    Try to extract running containers from the pool object, if the simulator exposes them.
    Returns list of objects with attributes: container_id, priority, cpu, ram, (optional) state.
    """
    # Common attribute names in simulators
    for attr in ["containers", "running_containers", "active_containers", "workers"]:
        if hasattr(pool, attr):
            try:
                items = list(getattr(pool, attr))
                if items:
                    return items
            except Exception:
                pass
    return []


def _maybe_preempt_for_priority(s, pool_id, min_cpu_needed, min_ram_needed, desired_priority):
    """
    Best-effort preemption:
      - Only preempt lower-priority containers.
      - Prefer preempting BATCH first, then INTERACTIVE (only if desired is QUERY).
    """
    pool = s.executor.pools[pool_id]
    suspensions = []

    avail_cpu = float(pool.avail_cpu_pool)
    avail_ram = float(pool.avail_ram_pool)
    if avail_cpu >= min_cpu_needed and avail_ram >= min_ram_needed:
        return suspensions

    containers = _list_running_containers_best_effort(pool)
    if not containers:
        return suspensions

    def cont_priority(c):
        pr = getattr(c, "priority", None)
        return pr

    def cont_id(c):
        return _safe_getattr(c, ["container_id", "id", "cid"], None)

    def cont_cpu(c):
        v = _safe_getattr(c, ["cpu", "vcpus", "cpu_req"], 0.0)
        try:
            return float(v)
        except Exception:
            return 0.0

    def cont_ram(c):
        v = _safe_getattr(c, ["ram", "mem", "memory", "ram_req"], 0.0)
        try:
            return float(v)
        except Exception:
            return 0.0

    # Select preemptable set
    preemptable = []
    for c in containers:
        pr = cont_priority(c)
        if pr is None:
            continue
        if _prio_rank(pr) <= _prio_rank(desired_priority):
            continue
        cid = cont_id(c)
        if cid is None:
            continue
        preemptable.append(c)

    # Sort: lowest priority first (largest rank), and then largest resource hogs first
    preemptable.sort(key=lambda c: (_prio_rank(cont_priority(c)), -(cont_cpu(c) + 0.01 * cont_ram(c))), reverse=True)

    for c in preemptable:
        if avail_cpu >= min_cpu_needed and avail_ram >= min_ram_needed:
            break
        cid = cont_id(c)
        if cid is None:
            continue
        suspensions.append(Suspend(container_id=cid, pool_id=pool_id))
        avail_cpu += cont_cpu(c)
        avail_ram += cont_ram(c)

    return suspensions


@register_scheduler(key="scheduler_medium_037")
def scheduler_medium_037(s, results, pipelines):
    """
    Scheduling loop:
      1) Enqueue new pipelines into per-priority RR queues.
      2) Learn from failures (especially OOM-like) by increasing per-op RAM estimate and adding cooldown.
      3) For each pool, schedule ready operators using strict priority + soft reservations:
           - If QUERY/INTERACTIVE are waiting, reserve headroom by restricting BATCH allocations.
      4) Best-effort preemption if a high-priority op can't be scheduled due to lack of headroom.
    """
    import math

    s.tick += 1
    now = s.tick

    # --- ingest new arrivals ---
    for p in pipelines:
        pid = p.pipeline_id
        s.pipelines_by_id[pid] = p
        if pid not in s.meta:
            s.meta[pid] = {
                "enqueue_tick": now,
                "last_dequeue_tick": -1,
                "last_assign_tick": -1,
                "fail_count": 0,
                "cooldown_until": -1,
            }
        _get_queue(s, p.priority).append(pid)
        s.last_seen_pipeline_ids.add(pid)

    # --- incorporate execution results (update RAM estimates, cooldowns) ---
    for r in results:
        # Try to locate pipeline_id for learning
        r_pid = getattr(r, "pipeline_id", None)
        if r_pid is None:
            # Some simulators may store pipeline_id in the op(s)
            # (best-effort; if not found, learning is skipped)
            ops = getattr(r, "ops", []) or []
            for op in ops:
                r_pid = getattr(op, "pipeline_id", None)
                if r_pid is not None:
                    break

        # Update per-op estimates on failure
        if hasattr(r, "failed") and r.failed():
            if r_pid is not None:
                meta = s.meta.get(r_pid)
                if meta is not None:
                    meta["fail_count"] = int(meta.get("fail_count", 0)) + 1
                    meta["cooldown_until"] = max(int(meta.get("cooldown_until", -1)), now + s.failure_cooldown_ticks)
                    s.meta[r_pid] = meta

            # Increase RAM estimate if possible (OOM-like or generic failure)
            ops = getattr(r, "ops", []) or []
            err = str(getattr(r, "error", "") or "").lower()
            oom_like = ("oom" in err) or ("out of memory" in err) or ("memory" in err) or ("killed" in err)

            # Use pool max RAM as a scale for increments
            pool_id = getattr(r, "pool_id", None)
            pool_max_ram = None
            if pool_id is not None and 0 <= int(pool_id) < s.executor.num_pools:
                pool_max_ram = float(s.executor.pools[int(pool_id)].max_ram_pool)

            for op in ops:
                if r_pid is None:
                    continue
                k = (r_pid, _op_key(op))
                prev = float(s.op_ram_est.get(k, 0.0))

                # If r.ram is present, it is the attempted RAM; start from that baseline.
                tried = getattr(r, "ram", None)
                try:
                    tried = float(tried) if tried is not None else 0.0
                except Exception:
                    tried = 0.0

                # Update failure count
                s.op_fail_count[k] = int(s.op_fail_count.get(k, 0)) + 1
                fc = s.op_fail_count[k]

                # Backoff: multiplicative increase, with a small additive bump scaled by pool size.
                bump = 0.0
                if pool_max_ram is not None:
                    bump = 0.04 * pool_max_ram

                base = max(prev, tried, _op_min_ram(op) * 1.5)
                if oom_like:
                    new_est = base * (1.75 if fc <= 2 else 1.50) + bump
                else:
                    # Unknown failure: still increase a bit, but less aggressively
                    new_est = base * 1.25 + 0.5 * bump

                # Cap to something reasonable (cannot exceed pool max, but we don't always know pool)
                if pool_max_ram is not None:
                    new_est = min(new_est, 0.98 * pool_max_ram)

                s.op_ram_est[k] = float(new_est)

    # Early exit if nothing changed and no results
    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    # Determine whether we have high-priority demand waiting (to enforce soft reservations)
    def queue_has_ready_work(priority):
        q = _get_queue(s, priority)
        n = len(q)
        for _ in range(min(n, 12)):  # small lookahead
            pid = q[0]
            q.rotate(-1)
            p = s.pipelines_by_id.get(pid)
            if p is None:
                continue
            st = p.runtime_status()
            if st.is_pipeline_successful():
                continue
            meta = s.meta.get(pid, {})
            if meta.get("cooldown_until", -1) > now:
                continue
            ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
            if ops:
                return True
        return False

    has_query_ready = queue_has_ready_work(Priority.QUERY)
    has_interactive_ready = queue_has_ready_work(Priority.INTERACTIVE)

    # Soft reservations: if high-priority work exists, restrict BATCH from consuming all resources.
    # (Values chosen to be a small improvement over FIFO without overcomplicating.)
    def batch_allowed(avail_cpu, avail_ram, pool):
        if not (has_query_ready or has_interactive_ready):
            return True
        # Keep headroom for at least one reasonable high-priority container
        reserve_cpu = max(1.0, math.floor(0.20 * float(pool.max_cpu_pool)))
        reserve_ram = 0.15 * float(pool.max_ram_pool)
        return (avail_cpu > reserve_cpu) and (avail_ram > reserve_ram)

    # Pool preference by priority (if multiple pools)
    def pool_order_for_priority(pr):
        n = s.executor.num_pools
        if n <= 1:
            return [0]
        if pr == Priority.QUERY:
            return list(range(n))  # try all, but earlier pools first
        if pr == Priority.INTERACTIVE:
            # favor first two pools
            return list(dict.fromkeys([0, 1] + list(range(n))))
        # batch: prefer later pools
        return list(reversed(range(n)))

    # --- scheduling per pool ---
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]

        # Assign multiple ops while capacity remains, but cap to avoid extreme packing
        max_assignments_this_pool = 6
        made = 0

        while made < max_assignments_this_pool:
            avail_cpu = float(pool.avail_cpu_pool)
            avail_ram = float(pool.avail_ram_pool)
            if avail_cpu <= 0.0 or avail_ram <= 0.0:
                break

            # Decide which priority to consider next
            pr_order = _aged_priority_order(s, now)

            chosen = None
            chosen_op = None
            chosen_cpu = None
            chosen_ram = None

            # Try to find an eligible pipeline/op for this pool
            for pr in pr_order:
                # Enforce soft reservation against batch when high-priority exists
                if pr == Priority.BATCH_PIPELINE and not batch_allowed(avail_cpu, avail_ram, pool):
                    continue

                # If multiple pools exist, bias placement by priority
                if pool_id not in pool_order_for_priority(pr):
                    continue

                q = _get_queue(s, pr)
                p = _pick_next_pipeline_rr(s, q, now, require_ready_op=True)
                if p is None:
                    continue

                # Aging-based relaxation:
                # If batch is very old and no query is ready, allow it even under mild pressure.
                if pr == Priority.BATCH_PIPELINE:
                    age = _pipeline_age_ticks(s, p.pipeline_id, now)
                    if has_query_ready:
                        # never let batch jump ahead of queries
                        continue
                    if age >= s.aging_ticks_batch:
                        pass  # allowed; already handled by queue selection
                    else:
                        # keep batch behind interactive when interactive exists
                        if has_interactive_ready:
                            continue

                # Pick first ready op
                st = p.runtime_status()
                op_list = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
                if not op_list:
                    continue
                op = op_list[0]

                # Compute target resources and check fit
                cpu_t, ram_t = _compute_target_resources(s, pool, p, op)
                cpu_req = min(cpu_t, avail_cpu)
                ram_req = min(ram_t, avail_ram)

                # Ensure we don't schedule with trivially tiny resources
                if cpu_req < 1.0 or ram_req <= 0.0:
                    continue

                # If RAM is below operator min (if known), bump up to avoid guaranteed OOM
                min_ram = _op_min_ram(op)
                if min_ram > 0.0 and ram_req < min_ram:
                    if avail_ram >= min_ram:
                        ram_req = min_ram
                    else:
                        continue

                chosen = p
                chosen_op = op
                chosen_cpu = float(cpu_req)
                chosen_ram = float(ram_req)
                break

            # If we couldn't find work for this pool, stop
            if chosen is None:
                break

            # If chosen is high priority but we still barely fit, attempt best-effort preemption
            # (Only if the simulator exposes running containers.)
            if chosen.priority in (Priority.QUERY, Priority.INTERACTIVE):
                # ensure minimal resources for our chosen sizing
                need_cpu = chosen_cpu
                need_ram = chosen_ram
                # If pool currently doesn't have enough due to concurrent changes, attempt preempt
                if float(pool.avail_cpu_pool) < need_cpu or float(pool.avail_ram_pool) < need_ram:
                    susp = _maybe_preempt_for_priority(s, pool_id, need_cpu, need_ram, chosen.priority)
                    if susp:
                        suspensions.extend(susp)
                        # After preemption, recompute
                        avail_cpu = float(pool.avail_cpu_pool)
                        avail_ram = float(pool.avail_ram_pool)
                        if avail_cpu < need_cpu or avail_ram < need_ram:
                            # Still can't; give up for this pool this tick
                            break

            assignment = Assignment(
                ops=[chosen_op],
                cpu=chosen_cpu,
                ram=chosen_ram,
                priority=chosen.priority,
                pool_id=pool_id,
                pipeline_id=chosen.pipeline_id,
            )
            assignments.append(assignment)

            meta = s.meta.get(chosen.pipeline_id, {})
            meta["last_assign_tick"] = now
            s.meta[chosen.pipeline_id] = meta

            made += 1

    return suspensions, assignments
