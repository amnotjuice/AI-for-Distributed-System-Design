from typing import List, Tuple


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
    s.queued_ids = set()

    # Deterministic "time"
    s.tick = 0
    s.enqueued_tick = {}  # pipeline_id -> tick first enqueued
    s.last_scheduled_tick = {}  # pipeline_id -> tick last assigned an op

    # RAM adaptation per operator:
    # (lo, hi, est, oom_count)
    s.op_ram = {}  # (pipeline_id, op_key) -> (lo, hi, est, ooms)

    # Pipeline-level RAM hedge (bounded)
    s.pipeline_ram_boost = {}  # pipeline_id -> float
    s.max_ram_boost = 16.0

    # Failure tracking
    s.failed_seen_count = {}  # pipeline_id -> int

    # Scheduling knobs
    s.min_cpu = 1.0
    s.min_ram = 1.0

    # Per-tick guards
    s.max_assignments_per_pool_per_tick = 512
    s.max_total_assignments_per_tick = 4096

    # Avoid duplicate op assignment within a single scheduler tick (status doesn't update mid-call)
    s.max_assignments_per_pid_per_tick = {
        Priority.QUERY: 2,
        Priority.INTERACTIVE: 1,
        Priority.BATCH_PIPELINE: 1,
    }

    # Headroom reservation (soft; we also reserve "one query container" worth when query runnable)
    s.reserve_frac_for_query = 0.08
    s.reserve_frac_for_interactive = 0.04

    # Aging thresholds (in ticks) to prevent starvation (important due to incomplete penalties)
    s.age_interactive_to_query = 70
    s.age_batch_to_interactive = 50
    s.age_batch_to_query = 160

    # Panic mode: if waited too long, relax reservations and scale RAM faster to finish
    s.panic_wait_ticks = 220

    # Pool preference (if multiple pools)
    s.batch_avoid_pool0_penalty = 2000.0
    s.highprio_prefer_pool0_bonus = -2.0

    # Candidate scan limits
    s.scan_limit_per_pick = 48

    # Preemption (best-effort; only if we can find running container ids)
    s.max_preemptions_per_tick = 8
    s.preempt_cooldown_ticks = 12
    s.preempted_recent = {}  # container_id -> tick

    # Work-conserving fallback: if we couldn't schedule anything but runnable work exists,
    # do a second pass with reservations disabled.
    s.enable_work_conserving_fallback = True


def _is_oom_error(err) -> bool:
    if not err:
        return False
    msg = str(err).lower()
    return ("oom" in msg) or ("out of memory" in msg) or ("memory" in msg and "killed" in msg)


def _prio_order():
    return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]


def _drop_pipeline_id(s, pipeline_id):
    s.pipelines_by_id.pop(pipeline_id, None)
    s.queued_ids.discard(pipeline_id)
    s.pipeline_ram_boost.pop(pipeline_id, None)
    s.failed_seen_count.pop(pipeline_id, None)
    s.enqueued_tick.pop(pipeline_id, None)
    s.last_scheduled_tick.pop(pipeline_id, None)
    # Keep s.op_ram entries (keyed by pid) as harmless cache


def _maybe_enqueue(s, p):
    pid = p.pipeline_id
    s.pipelines_by_id[pid] = p

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
    if pid not in s.enqueued_tick:
        s.enqueued_tick[pid] = s.tick
    if pid not in s.last_scheduled_tick:
        s.last_scheduled_tick[pid] = s.tick


def _op_key(op):
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


def _pid_from_result(r):
    pid = getattr(r, "pipeline_id", None)
    if pid is not None:
        return pid
    try:
        ops = getattr(r, "ops", None) or []
        if ops:
            op = ops[0]
            for attr in ("pipeline_id", "pid", "job_id", "dag_id"):
                try:
                    v = getattr(op, attr, None)
                    if v is not None:
                        return v
                except Exception:
                    pass
    except Exception:
        pass
    return None


def _effective_priority(s, pid, base_prio):
    last = int(s.last_scheduled_tick.get(pid, s.tick))
    waited = int(s.tick - last)

    if waited >= int(s.panic_wait_ticks):
        return Priority.QUERY

    if base_prio == Priority.INTERACTIVE:
        if waited >= int(s.age_interactive_to_query):
            return Priority.QUERY
        return Priority.INTERACTIVE

    if base_prio == Priority.BATCH_PIPELINE:
        if waited >= int(s.age_batch_to_query):
            return Priority.QUERY
        if waited >= int(s.age_batch_to_interactive):
            return Priority.INTERACTIVE
        return Priority.BATCH_PIPELINE

    return Priority.QUERY


def _clamp(x, lo, hi):
    if x < lo:
        return lo
    if x > hi:
        return hi
    return x


def _base_cpu_for(pool, eff_prio, avail_cpu=None) -> float:
    try:
        max_cpu = float(pool.max_cpu_pool)
    except Exception:
        max_cpu = 1.0

    # Fractional sizing: better packing + throughput on small clusters; still scales with pool size.
    if eff_prio == Priority.QUERY:
        cpu = 0.14 * max_cpu
        cpu = _clamp(cpu, 2.0, 24.0)
    elif eff_prio == Priority.INTERACTIVE:
        cpu = 0.10 * max_cpu
        cpu = _clamp(cpu, 1.5, 16.0)
    else:
        cpu = 0.06 * max_cpu
        cpu = _clamp(cpu, 1.0, 8.0)

    # If pool is very idle, scale up query a bit to reduce tail.
    if eff_prio == Priority.QUERY and avail_cpu is not None:
        try:
            a = float(avail_cpu)
            if a >= cpu * 2.5:
                cpu = min(a, cpu * 1.8)
        except Exception:
            pass

    return float(_clamp(cpu, 1.0, 32.0))


def _base_ram_for(pool, eff_prio) -> float:
    try:
        max_ram = float(pool.max_ram_pool)
    except Exception:
        max_ram = 1.0

    # Start smaller to pack more; OOM backoff is fast for big ops.
    # (GB units in simulator)
    base = max(2.0, min(96.0, 0.05 * max_ram))

    if eff_prio == Priority.QUERY:
        mult = 1.25
    elif eff_prio == Priority.INTERACTIVE:
        mult = 1.10
    else:
        mult = 1.00

    ram = base * mult

    # Default cap: don't take more than 70% of a pool unless we have evidence/panic.
    cap = max_ram * 0.70
    if ram > cap:
        ram = cap

    return float(max(1.0, ram))


def _min_headroom_for_one_query(s, pool):
    # Reserve enough for at least one query container (soft reserve).
    try:
        max_cpu = float(pool.max_cpu_pool)
        max_ram = float(pool.max_ram_pool)
    except Exception:
        max_cpu, max_ram = 1.0, 1.0

    cpu = _clamp(0.14 * max_cpu, 2.0, 24.0)
    ram = max(4.0, min(0.08 * max_ram, 128.0))
    return float(cpu), float(ram)


def _min_headroom_for_one_interactive(s, pool):
    try:
        max_cpu = float(pool.max_cpu_pool)
        max_ram = float(pool.max_ram_pool)
    except Exception:
        max_cpu, max_ram = 1.0, 1.0

    cpu = _clamp(0.10 * max_cpu, 1.5, 16.0)
    ram = max(3.0, min(0.06 * max_ram, 96.0))
    return float(cpu), float(ram)


def _reserve_for_higher_prio(s, pool, any_query_runnable: bool, any_interactive_runnable: bool):
    try:
        max_cpu = float(pool.max_cpu_pool)
        max_ram = float(pool.max_ram_pool)
    except Exception:
        max_cpu, max_ram = 1.0, 1.0

    reserve_cpu = 0.0
    reserve_ram = 0.0

    if any_query_runnable:
        frac_cpu = max_cpu * float(s.reserve_frac_for_query)
        frac_ram = max_ram * float(s.reserve_frac_for_query)
        one_cpu, one_ram = _min_headroom_for_one_query(s, pool)
        reserve_cpu = max(reserve_cpu, frac_cpu, one_cpu)
        reserve_ram = max(reserve_ram, frac_ram, one_ram)

    if any_interactive_runnable:
        frac_cpu = max_cpu * float(s.reserve_frac_for_interactive)
        frac_ram = max_ram * float(s.reserve_frac_for_interactive)
        one_cpu, one_ram = _min_headroom_for_one_interactive(s, pool)
        reserve_cpu = max(reserve_cpu, frac_cpu, one_cpu)
        reserve_ram = max(reserve_ram, frac_ram, one_ram)

    # Never reserve too much; keep system work-conserving.
    reserve_cpu = min(reserve_cpu, max_cpu * 0.25)
    reserve_ram = min(reserve_ram, max_ram * 0.25)

    return float(reserve_cpu), float(reserve_ram)


def _compute_request(s, pool, pid, base_prio, eff_prio, op):
    try:
        max_ram = float(pool.max_ram_pool)
    except Exception:
        max_ram = 1.0

    # CPU: prioritize packing; scale up queries if idle
    cpu = _base_cpu_for(pool, eff_prio)

    # RAM: base + per-op estimate + pipeline boost + safety
    ram = _base_ram_for(pool, eff_prio)

    boost = float(s.pipeline_ram_boost.get(pid, 1.0))
    boost = _clamp(boost, 1.0, float(s.max_ram_boost))
    ram *= boost

    safety = 1.0
    if eff_prio == Priority.QUERY:
        safety = 1.10
    elif eff_prio == Priority.INTERACTIVE:
        safety = 1.07
    else:
        safety = 1.05

    ok = (pid, _op_key(op))
    lo, hi, est, ooms = s.op_ram.get(ok, (0.0, None, 0.0, 0))

    # If we've OOMed this op, be aggressive to converge quickly.
    if ooms and ooms > 0:
        safety *= min(1.40, 1.10 + 0.06 * float(ooms))

    if est and float(est) > 0.0:
        ram = max(ram, float(est))
    if lo and float(lo) > 0.0:
        ram = max(ram, float(lo) * 1.12)

    waited = int(s.tick - int(s.last_scheduled_tick.get(pid, s.tick)))
    if waited >= int(s.panic_wait_ticks) or (ooms and ooms >= 2):
        # Finish mode: reduce repeated OOM/retry loops for big operators.
        ram = max(ram, max_ram * 0.45)

    ram *= safety

    # Apply upper bound softly (to prevent oscillation); only if it doesn't cut below lo.
    if hi is not None:
        try:
            hif = float(hi)
            if hif > 0.0:
                soft_hi = hif * 0.99
                if lo and float(lo) > 0.0:
                    soft_hi = max(soft_hi, float(lo) * 1.02)
                ram = min(ram, soft_hi)
        except Exception:
            pass

    # Global caps (keep some slack)
    max_cap = max_ram * 0.92
    if ram > max_cap:
        ram = max_cap
    if ram < float(s.min_ram):
        ram = float(s.min_ram)

    return float(max(float(s.min_cpu), cpu)), float(ram)


def _clean_queues(s):
    for prio in _prio_order():
        q = s.queues_by_prio.get(prio, [])
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


def _any_runnable_in_queue(s, prio, scan_limit=24):
    q = s.queues_by_prio.get(prio, [])
    if not q:
        return False
    n = len(q)
    start = int(s.rr_idx_by_prio.get(prio, 0))
    if start < 0 or start >= n:
        start = 0
    scanned = 0
    for i in range(n):
        if scanned >= int(scan_limit):
            break
        pid = q[(start + i) % n]
        p = s.pipelines_by_id.get(pid)
        if p is None:
            scanned += 1
            continue
        try:
            st = p.runtime_status()
            if st.is_pipeline_successful():
                scanned += 1
                continue
            ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
            if ops:
                return True
        except Exception:
            pass
        scanned += 1
    return False


def _pick_runnable(s, stage_prio, scheduled_count_by_pid, assigned_op_keys_by_pid):
    # Candidate queues for this stage (includes aged promotions)
    if stage_prio == Priority.QUERY:
        candidate_queues = [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]
    elif stage_prio == Priority.INTERACTIVE:
        candidate_queues = [Priority.INTERACTIVE, Priority.BATCH_PIPELINE]
    else:
        candidate_queues = [Priority.BATCH_PIPELINE]

    best = None  # (score, pid, op, eff_prio, base_prio, qprio, next_rr)

    for qprio in candidate_queues:
        q = s.queues_by_prio.get(qprio, [])
        if not q:
            continue

        n = len(q)
        start = int(s.rr_idx_by_prio.get(qprio, 0))
        if start < 0 or start >= n:
            start = 0

        scanned = 0
        for i in range(n):
            if scanned >= int(s.scan_limit_per_pick):
                break
            idx = (start + i) % n
            pid = q[idx]
            p = s.pipelines_by_id.get(pid)
            if p is None:
                scanned += 1
                continue

            base_prio = p.priority
            eff_prio = _effective_priority(s, pid, base_prio)

            # Promotion must match stage
            if stage_prio == Priority.QUERY and eff_prio != Priority.QUERY:
                scanned += 1
                continue
            if stage_prio == Priority.INTERACTIVE and eff_prio not in (Priority.INTERACTIVE, Priority.QUERY):
                scanned += 1
                continue
            if stage_prio == Priority.BATCH_PIPELINE and eff_prio != Priority.BATCH_PIPELINE:
                scanned += 1
                continue

            # Per-pipeline cap (avoid duplicates in same tick)
            cap = int(s.max_assignments_per_pid_per_tick.get(base_prio, 1))
            already = int(scheduled_count_by_pid.get(pid, 0))
            if already >= cap:
                scanned += 1
                continue

            try:
                st = p.runtime_status()
            except Exception:
                scanned += 1
                continue

            try:
                if st.is_pipeline_successful():
                    scanned += 1
                    continue
            except Exception:
                pass

            try:
                ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True) or []
            except Exception:
                ops = []

            if not ops:
                scanned += 1
                continue

            used = assigned_op_keys_by_pid.get(pid, set())
            picked_op = None
            for op in ops[:8]:
                k = _op_key(op)
                if k not in used:
                    picked_op = op
                    break
            if picked_op is None:
                scanned += 1
                continue

            waited = int(s.tick - int(s.last_scheduled_tick.get(pid, s.tick)))

            # Base priority bias (keep high-priority fast while still allowing aging to win)
            if base_prio == Priority.QUERY:
                base_bias = 300
            elif base_prio == Priority.INTERACTIVE:
                base_bias = 200
            else:
                base_bias = 100

            # Failed pipelines get a slight bump to encourage completion and avoid penalty
            fail_bump = min(120, 10 * int(s.failed_seen_count.get(pid, 0)))

            # Score: waited dominates; base bias breaks ties; fail bump helps avoid incompletes.
            score = waited * 10 + base_bias + fail_bump

            next_rr = (idx + 1) % n
            cand = (score, pid, picked_op, eff_prio, base_prio, qprio, next_rr)
            if best is None or cand[0] > best[0]:
                best = cand

            scanned += 1

    if best is None:
        return None, None, None, None

    _, pid, op, eff_prio, base_prio, qprio, next_rr = best
    s.rr_idx_by_prio[qprio] = next_rr
    return pid, op, eff_prio, base_prio


def _extract_running_containers(pipelines):
    # Best-effort: depends on simulator object model.
    # Returns list of dicts: {container_id, pool_id, priority, cpu, ram}
    out = []
    for p in pipelines:
        try:
            st = p.runtime_status()
        except Exception:
            continue
        try:
            running_ops = st.get_ops([OperatorState.RUNNING], require_parents_complete=False) or []
        except Exception:
            running_ops = []
        for op in running_ops:
            try:
                cid = getattr(op, "container_id", None) or getattr(op, "cid", None)
                pool_id = getattr(op, "pool_id", None)
                cpu = getattr(op, "cpu", None)
                ram = getattr(op, "ram", None)
                if cid is None or pool_id is None:
                    continue
                out.append(
                    {
                        "container_id": cid,
                        "pool_id": int(pool_id),
                        "priority": getattr(p, "priority", Priority.BATCH_PIPELINE),
                        "cpu": float(cpu) if cpu is not None else None,
                        "ram": float(ram) if ram is not None else None,
                    }
                )
            except Exception:
                pass
    return out


def _can_place_any_query(s, local_avail_cpu, local_avail_ram, any_query_runnable):
    if not any_query_runnable:
        return True
    # Conservative check: can we fit at least a minimal query container anywhere?
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        need_cpu, need_ram = _min_headroom_for_one_query(s, pool)
        if float(local_avail_cpu.get(pool_id, 0.0)) >= need_cpu and float(local_avail_ram.get(pool_id, 0.0)) >= need_ram:
            return True
    return False


def _maybe_preempt_for_query(s, suspensions, pipelines, any_query_runnable, local_avail_cpu, local_avail_ram):
    if not any_query_runnable:
        return
    if _can_place_any_query(s, local_avail_cpu, local_avail_ram, any_query_runnable=True):
        return

    # Preempt best-effort: suspend batch (then interactive) running containers to free resources next tick.
    running = _extract_running_containers(pipelines)
    if not running:
        return

    # Sort by lowest priority first (batch), then interactive; keep queries last.
    def prio_rank(pr):
        if pr == Priority.BATCH_PIPELINE:
            return 0
        if pr == Priority.INTERACTIVE:
            return 1
        return 2

    running.sort(key=lambda x: (prio_rank(x.get("priority")), -(x.get("ram") or 0.0), -(x.get("cpu") or 0.0)))

    made = 0
    for r in running:
        if made >= int(s.max_preemptions_per_tick):
            break
        pr = r.get("priority")
        if pr == Priority.QUERY:
            continue

        cid = r.get("container_id")
        pool_id = r.get("pool_id")
        if cid is None or pool_id is None:
            continue

        last = s.preempted_recent.get(cid, -10**9)
        if int(s.tick - int(last)) < int(s.preempt_cooldown_ticks):
            continue

        suspensions.append(Suspend(container_id=cid, pool_id=int(pool_id)))
        s.preempted_recent[cid] = s.tick
        made += 1


def _attempt_scheduling_pass(
    s,
    any_query_runnable,
    any_interactive_runnable,
    local_avail_cpu,
    local_avail_ram,
    per_pool_made,
    scheduled_count_by_pid,
    assigned_op_keys_by_pid,
    disable_reservations=False,
):
    assignments = []
    total_made = 0

    # Weighted stage sequence: prioritize query/interactive, but keep some batch flow to avoid incompletes.
    prio_seq = (
        [Priority.QUERY] * 10
        + [Priority.INTERACTIVE] * 6
        + [Priority.BATCH_PIPELINE] * 4
    )

    seq_idx = 0
    while total_made < int(s.max_total_assignments_per_tick):
        made_one = False

        for _ in range(len(prio_seq)):
            stage = prio_seq[seq_idx]
            seq_idx = (seq_idx + 1) % len(prio_seq)

            pid, op, eff_prio, base_prio = _pick_runnable(
                s,
                stage_prio=stage,
                scheduled_count_by_pid=scheduled_count_by_pid,
                assigned_op_keys_by_pid=assigned_op_keys_by_pid,
            )
            if pid is None or op is None:
                continue

            p = s.pipelines_by_id.get(pid)
            if p is None:
                continue

            waited = int(s.tick - int(s.last_scheduled_tick.get(pid, s.tick)))
            panic = waited >= int(s.panic_wait_ticks)

            # Pool iteration order
            pool_ids = list(range(s.executor.num_pools))
            if s.executor.num_pools > 1:
                if eff_prio in (Priority.QUERY, Priority.INTERACTIVE):
                    pool_ids = [0] + [i for i in pool_ids if i != 0]
                else:
                    pool_ids = [i for i in pool_ids if i != 0] + [0]
                # Very old batch: allow pool 0 sooner
                if base_prio == Priority.BATCH_PIPELINE and eff_prio == Priority.QUERY:
                    pool_ids = [0] + [i for i in range(s.executor.num_pools) if i != 0]

            best_pool = None
            best_cpu = None
            best_ram = None
            best_score = None

            for pool_id in pool_ids:
                if per_pool_made.get(pool_id, 0) >= int(s.max_assignments_per_pool_per_tick):
                    continue

                pool = s.executor.pools[pool_id]
                avail_cpu = float(local_avail_cpu.get(pool_id, 0.0))
                avail_ram = float(local_avail_ram.get(pool_id, 0.0))

                if avail_cpu < float(s.min_cpu) or avail_ram < float(s.min_ram):
                    continue

                cpu_req, ram_req = _compute_request(s, pool, pid, base_prio, eff_prio, op)

                # Fit checks (RAM is hard constraint)
                if ram_req > avail_ram:
                    continue

                # CPU is capped by availability; avoid tiny fragments
                cpu = float(cpu_req)
                if cpu > avail_cpu:
                    cpu = avail_cpu
                if cpu < float(s.min_cpu):
                    continue

                ram = float(ram_req)

                # Reservations (soft): enforce for lower prios unless disabled or in panic
                if not disable_reservations and not panic:
                    if eff_prio == Priority.BATCH_PIPELINE:
                        reserve_cpu, reserve_ram = _reserve_for_higher_prio(
                            s,
                            pool,
                            any_query_runnable=any_query_runnable,
                            any_interactive_runnable=any_interactive_runnable,
                        )
                        # Relax reserves gradually with waiting
                        relax = 0.0
                        if waited >= int(s.age_batch_to_interactive):
                            relax = 0.35
                        if waited >= int(s.age_batch_to_query):
                            relax = 0.60
                        if (avail_cpu - cpu) < reserve_cpu * (1.0 - relax):
                            continue
                        if (avail_ram - ram) < reserve_ram * (1.0 - relax):
                            continue
                    elif eff_prio == Priority.INTERACTIVE:
                        reserve_cpu, reserve_ram = _reserve_for_higher_prio(
                            s,
                            pool,
                            any_query_runnable=any_query_runnable,
                            any_interactive_runnable=False,
                        )
                        if (avail_cpu - cpu) < reserve_cpu or (avail_ram - ram) < reserve_ram:
                            continue

                # Pool preference penalty/bonus
                pref = 0.0
                if s.executor.num_pools > 1:
                    if eff_prio == Priority.BATCH_PIPELINE and pool_id == 0:
                        pref += float(s.batch_avoid_pool0_penalty)
                    if eff_prio in (Priority.QUERY, Priority.INTERACTIVE) and pool_id == 0:
                        pref += float(s.highprio_prefer_pool0_bonus)

                # Packing score: prefer tighter RAM fit; light CPU term; plus preference.
                score = (avail_ram - ram) + 0.02 * (avail_cpu - cpu) + pref
                if best_score is None or score < best_score:
                    best_score = score
                    best_pool = pool_id
                    best_cpu = cpu
                    best_ram = ram

            if best_pool is None:
                continue

            assignments.append(
                Assignment(
                    ops=[op],
                    cpu=float(best_cpu),
                    ram=float(best_ram),
                    # IMPORTANT: use effective priority so aged pipelines are treated urgently by executor too.
                    priority=eff_prio,
                    pool_id=int(best_pool),
                    pipeline_id=pid,
                )
            )

            # Update local availability snapshot
            local_avail_cpu[best_pool] = max(0.0, float(local_avail_cpu.get(best_pool, 0.0)) - float(best_cpu))
            local_avail_ram[best_pool] = max(0.0, float(local_avail_ram.get(best_pool, 0.0)) - float(best_ram))
            per_pool_made[best_pool] = per_pool_made.get(best_pool, 0) + 1

            # Record per-pipeline / per-op for this tick to avoid duplicates
            scheduled_count_by_pid[pid] = scheduled_count_by_pid.get(pid, 0) + 1
            used = assigned_op_keys_by_pid.get(pid)
            if used is None:
                used = set()
                assigned_op_keys_by_pid[pid] = used
            used.add(_op_key(op))

            s.last_scheduled_tick[pid] = s.tick

            total_made += 1
            made_one = True
            break

        if not made_one:
            break

    return assignments


@register_scheduler(key="scheduler_low_001")
def scheduler_low_001_scheduler(s, results, pipelines):
    s.tick = int(getattr(s, "tick", 0)) + 1

    # Ingest pipelines
    for p in pipelines:
        _maybe_enqueue(s, p)

    # Adapt RAM estimates from observed results
    for r in results:
        try:
            is_failed = False
            try:
                if getattr(r, "failed", None) and r.failed():
                    is_failed = True
            except Exception:
                is_failed = False

            pid = _pid_from_result(r)
            if pid is None:
                continue

            ops = getattr(r, "ops", None) or []
            op0 = ops[0] if ops else None
            ok = (pid, _op_key(op0)) if op0 is not None else None

            last_alloc = float(getattr(r, "ram", 0.0) or 0.0)
            if last_alloc < 1.0:
                last_alloc = 1.0

            if is_failed:
                s.failed_seen_count[pid] = s.failed_seen_count.get(pid, 0) + 1

                if _is_oom_error(getattr(r, "error", None)):
                    # Pipeline boost: stronger backoff to reduce repeated OOM loops
                    cur = float(s.pipeline_ram_boost.get(pid, 1.0))
                    nxt = max(cur * 1.45 + 0.10, cur + 0.50)
                    s.pipeline_ram_boost[pid] = min(float(s.max_ram_boost), nxt)

                    # Per-op backoff: exponential-ish jump
                    if ok is not None:
                        lo, hi, est, ooms = s.op_ram.get(ok, (0.0, None, 0.0, 0))
                        ooms = int(ooms) + 1

                        # OOM implies required > last_alloc: raise lo above last_alloc
                        lo = max(float(lo), last_alloc * 1.25)

                        # Invalidate hi if it conflicts
                        if hi is not None:
                            try:
                                if float(hi) < lo:
                                    hi = None
                            except Exception:
                                hi = None

                        # Aggressive estimate increase to converge in a few retries
                        if est is None or float(est) <= 0.0:
                            est = max(lo * 1.15, last_alloc * 2.0)
                        else:
                            est = max(float(est) * 1.85, last_alloc * 2.0, lo * 1.10)

                        s.op_ram[ok] = (lo, hi, est, ooms)
                else:
                    # Non-OOM failures: keep small boost to reduce flakiness
                    cur = float(s.pipeline_ram_boost.get(pid, 1.0))
                    if cur < 2.0:
                        s.pipeline_ram_boost[pid] = min(2.0, cur * 1.08 + 0.02)
            else:
                # Success: decay pipeline boost and tighten per-op upper bound
                cur = float(s.pipeline_ram_boost.get(pid, 1.0))
                if cur > 1.0:
                    s.pipeline_ram_boost[pid] = max(1.0, cur * 0.992)

                if ok is not None:
                    lo, hi, est, ooms = s.op_ram.get(ok, (0.0, None, 0.0, 0))
                    # Success => required <= last_alloc => update upper bound
                    try:
                        hi = last_alloc if hi is None else min(float(hi), last_alloc)
                    except Exception:
                        hi = last_alloc

                    lo = float(lo)
                    if hi is not None:
                        try:
                            hif = float(hi)
                            if lo > hif:
                                lo = hif * 0.90
                        except Exception:
                            pass

                    # Decrease estimate, but keep above lo
                    if est is None or float(est) <= 0.0:
                        est = last_alloc
                    else:
                        est = float(est) * 0.90
                    if hi is not None:
                        try:
                            est = min(float(est), float(hi) * 0.98)
                        except Exception:
                            pass
                    if lo > 0.0:
                        est = max(float(est), lo * 1.05)

                    # Reduce oom count slowly on success
                    ooms = max(0, int(ooms) - 1)

                    s.op_ram[ok] = (lo, hi, est, ooms)
        except Exception:
            pass

    # Lazily clean queues of completed pipelines
    _clean_queues(s)

    suspensions = []
    assignments = []

    # Local pool availability snapshot (executor does not update within this call)
    local_avail_cpu = {}
    local_avail_ram = {}
    per_pool_made = {}
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        local_avail_cpu[pool_id] = float(pool.avail_cpu_pool)
        local_avail_ram[pool_id] = float(pool.avail_ram_pool)
        per_pool_made[pool_id] = 0

    # Determine runnable higher-priority work
    any_query_runnable = _any_runnable_in_queue(s, Priority.QUERY, scan_limit=32)
    any_interactive_runnable = _any_runnable_in_queue(s, Priority.INTERACTIVE, scan_limit=32)

    # Best-effort preemption if queries are blocked by long-running low-priority work
    _maybe_preempt_for_query(
        s,
        suspensions=suspensions,
        pipelines=pipelines,
        any_query_runnable=any_query_runnable,
        local_avail_cpu=local_avail_cpu,
        local_avail_ram=local_avail_ram,
    )

    scheduled_count_by_pid = {}
    assigned_op_keys_by_pid = {}

    # Primary pass (with reservations)
    assignments.extend(
        _attempt_scheduling_pass(
            s,
            any_query_runnable=any_query_runnable,
            any_interactive_runnable=any_interactive_runnable,
            local_avail_cpu=local_avail_cpu,
            local_avail_ram=local_avail_ram,
            per_pool_made=per_pool_made,
            scheduled_count_by_pid=scheduled_count_by_pid,
            assigned_op_keys_by_pid=assigned_op_keys_by_pid,
            disable_reservations=False,
        )
    )

    # Work-conserving fallback: if we scheduled nothing but there is runnable work, retry without reservations.
    if s.enable_work_conserving_fallback and not assignments:
        any_batch_runnable = _any_runnable_in_queue(s, Priority.BATCH_PIPELINE, scan_limit=32)
        if any_query_runnable or any_interactive_runnable or any_batch_runnable:
            assignments.extend(
                _attempt_scheduling_pass(
                    s,
                    any_query_runnable=any_query_runnable,
                    any_interactive_runnable=any_interactive_runnable,
                    local_avail_cpu=local_avail_cpu,
                    local_avail_ram=local_avail_ram,
                    per_pool_made=per_pool_made,
                    scheduled_count_by_pid=scheduled_count_by_pid,
                    assigned_op_keys_by_pid=assigned_op_keys_by_pid,
                    disable_reservations=True,
                )
            )

    return suspensions, assignments
