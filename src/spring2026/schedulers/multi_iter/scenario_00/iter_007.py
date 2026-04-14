from typing import List, Tuple


@register_scheduler_init(key="scheduler_low_001")
def scheduler_low_001_init(s):
    # Queues store pipeline_ids (not Pipeline objects) to avoid stale references.
    s.waiting_by_prio = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }

    # Latest Pipeline object per pipeline_id (updated on every call).
    s.pipeline_by_id = {}

    # Track which pipeline_ids are currently enqueued (exactly once).
    s.enqueued_pipeline_ids = set()

    # Per-pipeline multiplicative RAM boost on OOM failures.
    s.pipeline_ram_boost = {}

    # Per-operator RAM learning:
    # - op_ram_lb: learned lower-bound estimate (raised by OOM, capped by later successes)
    # - op_ram_est: working estimate (moves up on OOM, slowly down on success)
    # Keyed by (pipeline_id or None, op_key).
    s.op_ram_lb = {}
    s.op_ram_est = {}
    s.op_oom_count = {}

    # Aging (ticks since last successful scheduling decision for this pipeline).
    s.pipeline_age = {}

    # Logical tick counter.
    s.tick = 0

    # Limits
    s.max_assignments_per_pool_per_tick = 64
    s.max_total_assignments_per_tick = 2048
    s.scan_depth_per_prio = 160
    s.place_attempts_per_prio = 10
    s.backoff_age_penalty_on_place_fail = 4

    # Minimums
    s.min_cpu = 1.0
    s.min_ram = 1.0

    # Per-container safe caps (approx a "single VM" envelope).
    s.container_cpu_cap = {
        Priority.QUERY: 56.0,
        Priority.INTERACTIVE: 40.0,
        Priority.BATCH_PIPELINE: 28.0,
    }
    s.container_ram_cap = {
        Priority.QUERY: 224.0,
        Priority.INTERACTIVE: 192.0,
        Priority.BATCH_PIPELINE: 192.0,
    }

    # Default request sizes (slightly more RAM-first to minimize early OOM churn).
    s.default_cpu = {
        Priority.QUERY: 16.0,
        Priority.INTERACTIVE: 10.0,
        Priority.BATCH_PIPELINE: 6.0,
    }
    s.default_ram = {
        Priority.QUERY: 72.0,
        Priority.INTERACTIVE: 56.0,
        Priority.BATCH_PIPELINE: 40.0,
    }

    # Allow some intra-pipeline parallelism for higher priorities
    s.max_ops_per_pipeline_per_tick = {
        Priority.QUERY: 10,
        Priority.INTERACTIVE: 7,
        Priority.BATCH_PIPELINE: 3,
    }

    # OOM handling / learning
    s.max_ram_boost = 48.0
    s.oom_ram_multiplier = 2.05
    s.oom_additive_gb = 10.0

    # RAM safety multipliers
    s.ram_safety_lb = 1.22          # for OOM-derived lower-bounds
    s.ram_safety_est = 1.12         # for moving estimate
    s.ram_safety_unknown = 1.16     # for unknown operators

    # Success-based relaxation (prevents permanent over-allocation after an early OOM)
    s.success_est_decay = 0.985     # slowly reduce est over repeated successes
    s.success_lb_decay = 0.997      # tiny decay on lb (still bounded by later OOMs)
    s.lb_cap_to_success = 1.00      # cap lb to <= assigned_ram on success

    # Pipeline boost behavior
    s.pipeline_boost_on_oom = 1.60
    s.pipeline_boost_decay_on_success = 0.965

    # CPU scaling when there is headroom (reduce high-priority latency on big clusters)
    s.cpu_scale_if_room = {
        Priority.QUERY: 2.0,
        Priority.INTERACTIVE: 1.6,
        Priority.BATCH_PIPELINE: 1.25,
    }
    s.cpu_room_factor = 1.35  # need eff_cpu >= base_cpu*scale and eff_ram >= ram*room to scale up
    s.ram_room_factor = 1.10

    # Headroom protection (work-conserving; only applied to lower priority placements)
    s.reserve_abs_for_query = (10.0, 36.0)  # (cpu, ram)
    s.reserve_abs_for_interactive = (8.0, 28.0)

    # Anti-starvation
    s.batch_starve_age = 48
    s.batch_share_when_starving = 0.22

    # Per-tick CPU/RAM share guidance (work-conserving)
    s.share_floor = {
        Priority.QUERY: 0.0,
        Priority.INTERACTIVE: 0.0,
        Priority.BATCH_PIPELINE: 0.06,
    }
    s.share_base = {
        Priority.QUERY: 0.72,
        Priority.INTERACTIVE: 0.20,
        Priority.BATCH_PIPELINE: 0.08,
    }


def _is_oom_error(err) -> bool:
    if not err:
        return False
    msg = str(err).lower()
    return ("oom" in msg) or ("out of memory" in msg) or ("killed" in msg and "memory" in msg)


def _prio_order():
    return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]


def _enqueue_pipeline(s, p: Pipeline):
    pid = getattr(p, "pipeline_id", None)
    if pid is None:
        return
    s.pipeline_by_id[pid] = p
    if pid in s.enqueued_pipeline_ids:
        return
    s.enqueued_pipeline_ids.add(pid)
    s.waiting_by_prio[p.priority].append(pid)
    s.pipeline_age[pid] = 0


def _drop_pipeline_id(s, pid):
    s.enqueued_pipeline_ids.discard(pid)
    s.pipeline_by_id.pop(pid, None)
    s.pipeline_ram_boost.pop(pid, None)
    s.pipeline_age.pop(pid, None)

    # Remove pid-scoped op learnings to avoid unbounded growth.
    to_del = []
    for k in list(s.op_ram_lb.keys()):
        if isinstance(k, tuple) and len(k) == 2 and k[0] == pid:
            to_del.append(k)
    for k in to_del:
        s.op_ram_lb.pop(k, None)

    to_del = []
    for k in list(s.op_ram_est.keys()):
        if isinstance(k, tuple) and len(k) == 2 and k[0] == pid:
            to_del.append(k)
    for k in to_del:
        s.op_ram_est.pop(k, None)

    to_del = []
    for k in list(s.op_oom_count.keys()):
        if isinstance(k, tuple) and len(k) == 2 and k[0] == pid:
            to_del.append(k)
    for k in to_del:
        s.op_oom_count.pop(k, None)


def _op_key(op):
    for attr in ("op_id", "operator_id", "id", "name", "key"):
        v = getattr(op, attr, None)
        if v is not None:
            return str(v)
    try:
        return f"{type(op).__name__}:{str(op)}"
    except Exception:
        return f"{type(op).__name__}:{repr(op)}"


def _pick_best_op(ops):
    # Prefer retrying FAILED ops first (often OOM-related and increases completion rate)
    best_pending = None
    for op in ops:
        st = getattr(op, "state", None)
        if st == OperatorState.FAILED:
            return op
        if st == OperatorState.PENDING and best_pending is None:
            best_pending = op
    return best_pending if best_pending is not None else (ops[0] if ops else None)


def _pipeline_runnable(p: Pipeline) -> bool:
    st = p.runtime_status()
    if st.is_pipeline_successful():
        return False
    ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
    return bool(ops)


def _has_runnable(s, prio, scan_depth: int) -> bool:
    q = s.waiting_by_prio[prio]
    if not q:
        return False
    k = min(scan_depth, len(q))
    for i in range(k):
        pid = q[i]
        p = s.pipeline_by_id.get(pid)
        if p is None:
            continue
        if _pipeline_runnable(p):
            return True
    return False


def _oldest_age(s, prio, scan_depth: int) -> int:
    q = s.waiting_by_prio[prio]
    if not q:
        return 0
    k = min(scan_depth, len(q))
    m = 0
    for i in range(k):
        pid = q[i]
        a = s.pipeline_age.get(pid, 0)
        if a > m:
            m = a
    return m


def _select_candidate_from_prio(s, prio, scheduled_count_by_pid, scan_depth: int):
    """
    Rotate through up to scan_depth items; return best (pid, pipeline, op) by age among runnable.
    Moves selected pid to end, and drops completed/stale pids.
    """
    q = s.waiting_by_prio[prio]
    if not q:
        return None

    k = min(scan_depth, len(q))
    popped = []
    runnable = []  # (score, pid, p, op)

    for _ in range(k):
        pid = q.pop(0)
        p = s.pipeline_by_id.get(pid)
        if p is None:
            s.enqueued_pipeline_ids.discard(pid)
            s.pipeline_age.pop(pid, None)
            continue

        st = p.runtime_status()
        if st.is_pipeline_successful():
            _drop_pipeline_id(s, pid)
            continue

        popped.append(pid)

        max_ops = int(s.max_ops_per_pipeline_per_tick.get(prio, 1) or 1)
        if scheduled_count_by_pid.get(pid, 0) >= max_ops:
            continue

        ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        if not ops:
            continue

        op = _pick_best_op(ops)
        if op is None:
            continue

        age = s.pipeline_age.get(pid, 0)

        # Bias: prioritize higher priorities when ages are similar.
        if prio == Priority.QUERY:
            bias = 3
        elif prio == Priority.INTERACTIVE:
            bias = 1
        else:
            bias = 0

        score = age + bias
        runnable.append((score, pid, p, op))

    if not runnable:
        for pid in popped:
            q.append(pid)
        return None

    runnable.sort(key=lambda x: x[0], reverse=True)
    _, best_pid, best_p, best_op = runnable[0]

    for pid in popped:
        if pid != best_pid:
            q.append(pid)
    q.append(best_pid)

    return best_pid, best_p, best_op


def _effective_remaining(
    s,
    remaining_cpu,
    remaining_ram,
    pool,
    prio,
    query_backlog,
    interactive_backlog,
    query_underbudget: bool,
    interactive_underbudget: bool,
    batch_starving: bool,
):
    eff_cpu = float(remaining_cpu)
    eff_ram = float(remaining_ram)

    # Only constrain lower priority placements; keep scheduler work-conserving.
    # Also disable/relax reservation when batch is starving to prevent penalty via non-completion.
    if prio == Priority.BATCH_PIPELINE and not batch_starving:
        if query_backlog and query_underbudget:
            rc, rr = s.reserve_abs_for_query
            eff_cpu = max(0.0, eff_cpu - min(float(pool.max_cpu_pool) * 0.14, float(rc)))
            eff_ram = max(0.0, eff_ram - min(float(pool.max_ram_pool) * 0.14, float(rr)))
        if interactive_backlog and interactive_underbudget:
            rc, rr = s.reserve_abs_for_interactive
            eff_cpu = max(0.0, eff_cpu - min(float(pool.max_cpu_pool) * 0.12, float(rc)))
            eff_ram = max(0.0, eff_ram - min(float(pool.max_ram_pool) * 0.12, float(rr)))

    return eff_cpu, eff_ram


def _get_ram_stats(s, pid, ok):
    lb_pid = float(s.op_ram_lb.get((pid, ok), 0.0) or 0.0) if pid is not None else 0.0
    lb_g = float(s.op_ram_lb.get((None, ok), 0.0) or 0.0)
    lb = max(lb_pid, lb_g)

    est = s.op_ram_est.get((pid, ok)) if pid is not None else None
    if est is None:
        est = s.op_ram_est.get((None, ok))
    est = float(est) if est is not None else None

    return lb, est


def _compute_request(s, pool, prio, pid, op, eff_cpu, eff_ram):
    ok = _op_key(op)

    boost = float(s.pipeline_ram_boost.get(pid, 1.0) or 1.0) if pid is not None else 1.0
    boost = max(1.0, boost)

    # Defaults
    base_cpu = float(s.default_cpu.get(prio, 4.0) or 4.0)
    base_ram = float(s.default_ram.get(prio, 16.0) or 16.0) * boost

    # Learned stats
    lb, est = _get_ram_stats(s, pid, ok)

    # Apply a damped boost to learned values to avoid overreaction (since boost already scales defaults)
    damp = 1.0 + 0.45 * (boost - 1.0)
    lb_eff = float(lb) * damp
    est_eff = float(est) * damp if est is not None else None

    desired_ram = base_ram * float(s.ram_safety_unknown)

    if lb_eff > 0.0:
        desired_ram = max(desired_ram, lb_eff * float(s.ram_safety_lb))
    if est_eff is not None and est_eff > 0.0:
        desired_ram = max(desired_ram, est_eff * float(s.ram_safety_est))

    # Caps
    cpu_cap = float(s.container_cpu_cap.get(prio, 8.0) or 8.0)
    ram_cap = float(s.container_ram_cap.get(prio, 48.0) or 48.0)
    cpu_cap = min(cpu_cap, float(getattr(pool, "max_cpu_pool", cpu_cap) or cpu_cap))
    ram_cap = min(ram_cap, float(getattr(pool, "max_ram_pool", ram_cap) or ram_cap))

    desired_ram = max(float(s.min_ram), min(float(desired_ram), ram_cap))

    # HARD RAM constraint: do not schedule if we cannot meet desired RAM.
    if float(eff_ram) + 1e-9 < float(desired_ram):
        return 0.0, 0.0

    # CPU sizing: default, with optional scale-up if there is plenty of headroom.
    cpu = max(float(s.min_cpu), min(float(base_cpu), cpu_cap))
    scale = float(s.cpu_scale_if_room.get(prio, 1.0) or 1.0)
    if scale > 1.0:
        # Need enough room; also require a bit of RAM headroom to avoid fragmenting memory too aggressively.
        if (float(eff_cpu) >= float(base_cpu) * scale * float(s.cpu_room_factor)) and (
            float(eff_ram) >= float(desired_ram) * float(s.ram_room_factor)
        ):
            cpu = min(cpu_cap, float(base_cpu) * scale)

    # CPU is soft: shrink CPU down to fit (never below min_cpu).
    cpu = min(float(cpu), float(eff_cpu))
    if cpu < float(s.min_cpu):
        return 0.0, 0.0

    return float(cpu), float(desired_ram)


def _best_pool_for_request(
    s,
    prio,
    pid,
    op,
    remaining_cpu_by_pool,
    remaining_ram_by_pool,
    per_pool_made,
    query_backlog,
    interactive_backlog,
    query_underbudget: bool,
    interactive_underbudget: bool,
    batch_starving: bool,
):
    best = None  # (score, pool_id, cpu, ram)
    for pool_id in range(s.executor.num_pools):
        if int(per_pool_made.get(pool_id, 0) or 0) >= int(s.max_assignments_per_pool_per_tick):
            continue

        pool = s.executor.pools[pool_id]
        rem_cpu = float(remaining_cpu_by_pool[pool_id])
        rem_ram = float(remaining_ram_by_pool[pool_id])
        if rem_cpu < float(s.min_cpu) or rem_ram < float(s.min_ram):
            continue

        eff_cpu, eff_ram = _effective_remaining(
            s,
            rem_cpu,
            rem_ram,
            pool,
            prio,
            query_backlog,
            interactive_backlog,
            query_underbudget=query_underbudget,
            interactive_underbudget=interactive_underbudget,
            batch_starving=batch_starving,
        )
        if eff_cpu < float(s.min_cpu) or eff_ram < float(s.min_ram):
            continue

        cpu, ram = _compute_request(s, pool, prio, pid, op, eff_cpu, eff_ram)
        if cpu < float(s.min_cpu) or ram < float(s.min_ram):
            continue
        if cpu > eff_cpu + 1e-9 or ram > eff_ram + 1e-9:
            continue

        left_ram = eff_ram - ram
        left_cpu = eff_cpu - cpu

        # Scoring:
        # - QUERY: pick pool with max effective headroom (min interference/tail risk)
        # - INTERACTIVE: moderate packing but keep headroom
        # - BATCH: best-fit packing (RAM-first) for high utilization
        if prio == Priority.QUERY:
            score = -(0.70 * eff_cpu + 0.30 * eff_ram)
        elif prio == Priority.INTERACTIVE:
            score = 0.75 * left_ram + 0.12 * left_cpu
        else:
            score = 1.00 * left_ram + 0.05 * left_cpu

        if best is None or score < best[0]:
            best = (score, pool_id, cpu, ram)

    if best is None:
        return None
    _, pool_id, cpu, ram = best
    return pool_id, cpu, ram


def _compute_shares(s, runnable_by_prio, batch_starving: bool):
    shares = {
        Priority.QUERY: float(s.share_base[Priority.QUERY]),
        Priority.INTERACTIVE: float(s.share_base[Priority.INTERACTIVE]),
        Priority.BATCH_PIPELINE: float(s.share_base[Priority.BATCH_PIPELINE]),
    }

    # If only one high-priority class is runnable, allow it to dominate.
    if runnable_by_prio.get(Priority.QUERY, False) and not runnable_by_prio.get(Priority.INTERACTIVE, False):
        shares[Priority.QUERY] = max(shares[Priority.QUERY], 0.80)
        shares[Priority.BATCH_PIPELINE] = min(shares[Priority.BATCH_PIPELINE], 0.08)

    if runnable_by_prio.get(Priority.INTERACTIVE, False) and not runnable_by_prio.get(Priority.QUERY, False):
        shares[Priority.INTERACTIVE] = max(shares[Priority.INTERACTIVE], 0.60)
        shares[Priority.BATCH_PIPELINE] = max(shares[Priority.BATCH_PIPELINE], 0.14)

    # When batch is starving, increase batch share to ensure completion and avoid penalty.
    if batch_starving and runnable_by_prio.get(Priority.BATCH_PIPELINE, False):
        target = float(s.batch_share_when_starving)
        shares[Priority.BATCH_PIPELINE] = max(shares[Priority.BATCH_PIPELINE], target)

        extra = shares[Priority.BATCH_PIPELINE] - float(s.share_base[Priority.BATCH_PIPELINE])
        if extra > 0.0:
            take_i = min(extra, max(0.0, shares[Priority.INTERACTIVE] - 0.10))
            shares[Priority.INTERACTIVE] -= take_i
            extra -= take_i
            if extra > 0.0:
                take_q = min(extra, max(0.0, shares[Priority.QUERY] - 0.52))
                shares[Priority.QUERY] -= take_q

    for prio, floor in s.share_floor.items():
        if runnable_by_prio.get(prio, False):
            shares[prio] = max(shares.get(prio, 0.0), float(floor))

    for prio in list(shares.keys()):
        if not runnable_by_prio.get(prio, False):
            shares[prio] = 0.0

    total = sum(shares.values())
    if total <= 0.0:
        return {Priority.QUERY: 0.0, Priority.INTERACTIVE: 0.0, Priority.BATCH_PIPELINE: 0.0}

    for prio in list(shares.keys()):
        shares[prio] = shares[prio] / total
    return shares


@register_scheduler(key="scheduler_low_001")
def scheduler_low_001_scheduler(s, results: List[ExecutionResult], pipelines: List[Pipeline]):
    s.tick += 1

    # Ingest / refresh pipelines
    for p in pipelines:
        _enqueue_pipeline(s, p)

    # Age all enqueued pipelines (reset on successful scheduling of that pipeline below)
    for pid in list(s.enqueued_pipeline_ids):
        if pid in s.pipeline_age:
            s.pipeline_age[pid] += 1

    # Learn from results
    for r in results:
        pid = getattr(r, "pipeline_id", None)
        ops = getattr(r, "ops", None) or []
        assigned_ram = float(getattr(r, "ram", 0.0) or 0.0)

        is_failed = bool(getattr(r, "failed", None) and r.failed())
        is_oom = is_failed and _is_oom_error(getattr(r, "error", None))

        if is_oom:
            # Escalate pipeline-level boost quickly to reduce repeated failures.
            if pid is not None:
                cur = float(s.pipeline_ram_boost.get(pid, 1.0) or 1.0)
                s.pipeline_ram_boost[pid] = min(float(s.max_ram_boost), cur * float(s.pipeline_boost_on_oom))

            for op in ops:
                ok = _op_key(op)

                for scope_pid in (pid, None):
                    key = (scope_pid, ok)

                    cur_ooms = int(s.op_oom_count.get(key, 0) or 0) + 1
                    s.op_oom_count[key] = cur_ooms

                    cur_lb = float(s.op_ram_lb.get(key, 0.0) or 0.0)
                    cur_est = s.op_ram_est.get(key)
                    cur_est = float(cur_est) if cur_est is not None else 0.0

                    # Raise LB/EST aggressively to reach safe region quickly.
                    mult = float(s.oom_ram_multiplier) * (1.0 + 0.10 * min(7, cur_ooms))
                    new_lb = max(cur_lb, assigned_ram * mult, assigned_ram + float(s.oom_additive_gb))

                    s.op_ram_lb[key] = new_lb
                    s.op_ram_est[key] = max(cur_est, new_lb)
        else:
            # On successful completions, decay pipeline boost and relax per-op estimates.
            if pid is not None and pid in s.pipeline_ram_boost:
                cur = float(s.pipeline_ram_boost.get(pid, 1.0) or 1.0)
                cur = max(1.0, cur * float(s.pipeline_boost_decay_on_success))
                s.pipeline_ram_boost[pid] = cur

            for op in ops:
                ok = _op_key(op)
                for scope_pid in (pid, None):
                    key = (scope_pid, ok)

                    # Lightly decay OOM count
                    if key in s.op_oom_count:
                        s.op_oom_count[key] = max(0, int(s.op_oom_count.get(key, 0) or 0) - 1)

                    # Cap LB by observed success (prevents permanent over-allocation from earlier multiplier)
                    if key in s.op_ram_lb and assigned_ram > 0.0:
                        cur_lb = float(s.op_ram_lb.get(key, 0.0) or 0.0)
                        if cur_lb > 0.0:
                            capped = min(cur_lb, assigned_ram * float(s.lb_cap_to_success))
                            capped = max(float(s.min_ram), capped * float(s.success_lb_decay))
                            s.op_ram_lb[key] = capped

                    # Relax EST slowly downward (never below LB)
                    if assigned_ram > 0.0:
                        lb = float(s.op_ram_lb.get(key, 0.0) or 0.0)
                        cur_est = s.op_ram_est.get(key)
                        cur_est = float(cur_est) if cur_est is not None else assigned_ram
                        # Use the smaller of current est and what just worked; then decay a bit.
                        new_est = min(cur_est, assigned_ram) * float(s.success_est_decay)
                        new_est = max(lb, float(s.min_ram), new_est)
                        s.op_ram_est[key] = new_est

    suspensions: List[Suspend] = []
    assignments: List[Assignment] = []

    # Remaining resources by pool (pool.avail_* do NOT update within a single scheduler call)
    remaining_cpu_by_pool = {}
    remaining_ram_by_pool = {}
    per_pool_made = {}
    total_avail_cpu = 0.0
    total_avail_ram = 0.0
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        c = float(pool.avail_cpu_pool)
        r = float(pool.avail_ram_pool)
        remaining_cpu_by_pool[pool_id] = c
        remaining_ram_by_pool[pool_id] = r
        per_pool_made[pool_id] = 0
        total_avail_cpu += c
        total_avail_ram += r

    runnable_by_prio = {
        Priority.QUERY: _has_runnable(s, Priority.QUERY, scan_depth=40),
        Priority.INTERACTIVE: _has_runnable(s, Priority.INTERACTIVE, scan_depth=40),
        Priority.BATCH_PIPELINE: _has_runnable(s, Priority.BATCH_PIPELINE, scan_depth=40),
    }
    query_backlog = runnable_by_prio[Priority.QUERY]
    interactive_backlog = runnable_by_prio[Priority.INTERACTIVE]

    batch_oldest = _oldest_age(s, Priority.BATCH_PIPELINE, scan_depth=120)
    batch_starving = batch_oldest >= int(s.batch_starve_age) and runnable_by_prio[Priority.BATCH_PIPELINE]

    shares = _compute_shares(s, runnable_by_prio, batch_starving=batch_starving)

    cpu_budget = {prio: float(shares.get(prio, 0.0)) * float(total_avail_cpu) for prio in _prio_order()}
    ram_budget = {prio: float(shares.get(prio, 0.0)) * float(total_avail_ram) for prio in _prio_order()}
    cpu_used = {Priority.QUERY: 0.0, Priority.INTERACTIVE: 0.0, Priority.BATCH_PIPELINE: 0.0}
    ram_used = {Priority.QUERY: 0.0, Priority.INTERACTIVE: 0.0, Priority.BATCH_PIPELINE: 0.0}

    scheduled_count_by_pid = {}

    total_limit = min(
        int(s.max_total_assignments_per_tick),
        int(s.max_assignments_per_pool_per_tick) * max(1, int(s.executor.num_pools)),
    )

    made_total = 0
    runnable_cache = {Priority.QUERY: True, Priority.INTERACTIVE: True, Priority.BATCH_PIPELINE: True}
    runnable_cache_tick = -1

    while made_total < total_limit:
        # Refresh runnable cache occasionally.
        if runnable_cache_tick != s.tick or (made_total % 24 == 0):
            runnable_cache = {
                Priority.QUERY: _has_runnable(s, Priority.QUERY, scan_depth=28),
                Priority.INTERACTIVE: _has_runnable(s, Priority.INTERACTIVE, scan_depth=28),
                Priority.BATCH_PIPELINE: _has_runnable(s, Priority.BATCH_PIPELINE, scan_depth=28),
            }
            runnable_cache_tick = s.tick

        if not any(runnable_cache.values()):
            break

        # Determine "underbudget" for headroom reservation logic.
        query_underbudget = (cpu_used[Priority.QUERY] + ram_used[Priority.QUERY]) < (
            cpu_budget[Priority.QUERY] + ram_budget[Priority.QUERY]
        )
        interactive_underbudget = (cpu_used[Priority.INTERACTIVE] + ram_used[Priority.INTERACTIVE]) < (
            cpu_budget[Priority.INTERACTIVE] + ram_budget[Priority.INTERACTIVE]
        )

        # Order priorities by "budget deficit"; if none are under budget, use strict priority order.
        deficit_list = []
        for prio in _prio_order():
            if not runnable_cache.get(prio, False):
                continue
            cb = float(cpu_budget.get(prio, 0.0))
            rb = float(ram_budget.get(prio, 0.0))
            denom = max(1e-6, cb + rb)
            cdef = cb - float(cpu_used.get(prio, 0.0))
            rdef = rb - float(ram_used.get(prio, 0.0))
            deficit_score = (cdef + rdef) / denom
            deficit_list.append((deficit_score, prio))

        deficit_list.sort(key=lambda x: x[0], reverse=True)
        if deficit_list and deficit_list[0][0] > 0.0:
            prio_sequence = [p for _, p in deficit_list]
        else:
            prio_sequence = [p for p in _prio_order() if runnable_cache.get(p, False)]

        made_this_outer = False

        for prio in prio_sequence:
            tried_pids = set()
            placed = False

            for _ in range(int(s.place_attempts_per_prio)):
                cand = _select_candidate_from_prio(
                    s, prio, scheduled_count_by_pid, scan_depth=int(s.scan_depth_per_prio)
                )
                if cand is None:
                    break

                pid, p, op = cand
                if pid in tried_pids:
                    break
                tried_pids.add(pid)

                best = _best_pool_for_request(
                    s,
                    prio=prio,
                    pid=pid,
                    op=op,
                    remaining_cpu_by_pool=remaining_cpu_by_pool,
                    remaining_ram_by_pool=remaining_ram_by_pool,
                    per_pool_made=per_pool_made,
                    query_backlog=query_backlog,
                    interactive_backlog=interactive_backlog,
                    query_underbudget=query_underbudget,
                    interactive_underbudget=interactive_underbudget,
                    batch_starving=batch_starving,
                )
                if best is None:
                    # Back off this pipeline a bit this tick to avoid head-of-line blocking.
                    if pid is not None:
                        s.pipeline_age[pid] = max(0, int(s.pipeline_age.get(pid, 0) or 0) - int(s.backoff_age_penalty_on_place_fail))
                    continue

                pool_id, cpu, ram = best

                if cpu < float(s.min_cpu) or ram < float(s.min_ram):
                    if pid is not None:
                        s.pipeline_age[pid] = max(0, int(s.pipeline_age.get(pid, 0) or 0) - 1)
                    continue
                if cpu > float(remaining_cpu_by_pool[pool_id]) + 1e-9 or ram > float(remaining_ram_by_pool[pool_id]) + 1e-9:
                    if pid is not None:
                        s.pipeline_age[pid] = max(0, int(s.pipeline_age.get(pid, 0) or 0) - 1)
                    continue

                assignments.append(
                    Assignment(
                        ops=[op],
                        cpu=float(cpu),
                        ram=float(ram),
                        priority=p.priority,
                        pool_id=pool_id,
                        pipeline_id=pid,
                    )
                )

                remaining_cpu_by_pool[pool_id] -= float(cpu)
                remaining_ram_by_pool[pool_id] -= float(ram)
                per_pool_made[pool_id] = int(per_pool_made.get(pool_id, 0) or 0) + 1

                cpu_used[prio] += float(cpu)
                ram_used[prio] += float(ram)

                made_total += 1
                made_this_outer = True
                placed = True

                scheduled_count_by_pid[pid] = scheduled_count_by_pid.get(pid, 0) + 1
                s.pipeline_age[pid] = 0

                # Update backlog signals lightly.
                query_backlog = _has_runnable(s, Priority.QUERY, scan_depth=22)
                interactive_backlog = _has_runnable(s, Priority.INTERACTIVE, scan_depth=22)

                runnable_cache_tick = -1
                break

            if placed:
                break

        if not made_this_outer:
            break

    return suspensions, assignments
