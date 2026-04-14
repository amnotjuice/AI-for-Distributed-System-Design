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
    s.last_scheduled_tick = {}  # pipeline_id -> tick last assigned an op (progress clock)

    # RAM adaptation per operator:
    # (lo, hi, est, oom_count, last_good)
    s.op_ram = {}  # (pipeline_id, op_key) -> tuple

    # Pipeline-level RAM hedge (bounded)
    s.pipeline_ram_boost = {}  # pipeline_id -> float
    s.max_ram_boost = 5.0

    # Failure tracking
    s.failed_seen_count = {}  # pipeline_id -> int

    # Scheduling knobs
    s.min_cpu = 1.0
    s.min_ram = 1.0

    # Per-tick guards
    s.max_assignments_per_pool_per_tick = 1024
    s.max_total_assignments_per_tick = 8192

    # Intra-pipeline parallelism caps
    s.max_assignments_per_pid_per_tick = {
        Priority.QUERY: 6,
        Priority.INTERACTIVE: 4,
        Priority.BATCH_PIPELINE: 3,
    }

    # Reservations: keep small and only enforced softly (for batch/interactive) to protect query tail.
    s.reserve_frac_for_query = 0.030
    s.reserve_frac_for_interactive = 0.015

    # Aging thresholds (in ticks)
    s.age_interactive_to_query = 50
    s.age_batch_to_interactive = 40
    s.age_batch_to_query = 120

    # Panic mode: if waited too long, prioritize completion and allocate conservatively (RAM-first).
    s.panic_wait_ticks = 160

    # Candidate scan limits
    s.scan_limit_per_pick = 220
    s.max_ops_considered_per_pipeline = 10

    # Preemption (best-effort)
    s.max_preemptions_per_tick = 8
    s.preempt_cooldown_ticks = 12
    s.preempted_recent = {}  # container_id -> tick

    # Always do a work-conserving fill pass to improve completion rate.
    s.enable_work_conserving_fill = True


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
    # Keep CPU per-op moderate to preserve concurrency and avoid long backlogs (completion matters).
    if eff_prio == Priority.QUERY:
        cpu = 4.0
        cpu = _clamp(cpu, 2.0, 10.0)
    elif eff_prio == Priority.INTERACTIVE:
        cpu = 3.0
        cpu = _clamp(cpu, 2.0, 8.0)
    else:
        cpu = 2.0
        cpu = _clamp(cpu, 1.0, 6.0)

    # Scale up if pool is very idle.
    if avail_cpu is not None:
        try:
            a = float(avail_cpu)
            if a >= cpu * 6.0:
                cpu = min(a, cpu * 2.0)
            elif a >= cpu * 3.0:
                cpu = min(a, cpu * 1.5)
        except Exception:
            pass

    return float(_clamp(cpu, 1.0, 24.0))


def _base_ram_for(pool, eff_prio) -> float:
    try:
        max_ram = float(pool.max_ram_pool)
    except Exception:
        max_ram = 1.0

    # Choose a small fraction of pool RAM, but large enough to avoid common OOMs.
    # (Do not scale linearly with max_ram; learned per-op estimates should dominate.)
    if eff_prio == Priority.QUERY:
        ram = max(6.0, min(24.0, 0.016 * max_ram))
    elif eff_prio == Priority.INTERACTIVE:
        ram = max(5.0, min(20.0, 0.014 * max_ram))
    else:
        ram = max(4.0, min(16.0, 0.012 * max_ram))

    # Cap: avoid grabbing a huge share by default.
    ram = min(ram, max_ram * 0.50)
    return float(max(1.0, ram))


def _min_headroom_for_one_query(s, pool):
    try:
        max_cpu = float(pool.max_cpu_pool)
        max_ram = float(pool.max_ram_pool)
    except Exception:
        max_cpu, max_ram = 1.0, 1.0
    cpu = _clamp(0.06 * max_cpu, 2.0, 10.0)
    ram = max(6.0, min(0.030 * max_ram, 64.0))
    return float(cpu), float(ram)


def _min_headroom_for_one_interactive(s, pool):
    try:
        max_cpu = float(pool.max_cpu_pool)
        max_ram = float(pool.max_ram_pool)
    except Exception:
        max_cpu, max_ram = 1.0, 1.0
    cpu = _clamp(0.045 * max_cpu, 2.0, 8.0)
    ram = max(5.0, min(0.025 * max_ram, 48.0))
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

    reserve_cpu = min(reserve_cpu, max_cpu * 0.15)
    reserve_ram = min(reserve_ram, max_ram * 0.15)
    return float(reserve_cpu), float(reserve_ram)


def _compute_request(s, pool, pid, base_prio, eff_prio, op, avail_cpu=None):
    # IMPORTANT: do NOT clamp request RAM to current availability; that causes endless OOM loops.
    try:
        max_ram = float(pool.max_ram_pool)
    except Exception:
        max_ram = 1.0

    cpu = _base_cpu_for(pool, eff_prio, avail_cpu=avail_cpu)
    ram = _base_ram_for(pool, eff_prio)

    boost = float(s.pipeline_ram_boost.get(pid, 1.0))
    boost = _clamp(boost, 1.0, float(s.max_ram_boost))
    ram *= boost

    ok = (pid, _op_key(op))
    lo, hi, est, ooms, last_good = s.op_ram.get(ok, (0.0, None, 0.0, 0, 0.0))

    # Small safety, larger when we have OOM history (converge quickly to "no OOM").
    if eff_prio == Priority.QUERY:
        safety = 1.06
    elif eff_prio == Priority.INTERACTIVE:
        safety = 1.05
    else:
        safety = 1.04

    if ooms and int(ooms) > 0:
        safety *= min(1.70, 1.10 + 0.18 * float(int(ooms)))

    if est and float(est) > 0.0:
        ram = max(ram, float(est))
    if lo and float(lo) > 0.0:
        ram = max(ram, float(lo) * 1.08)
    if last_good and float(last_good) > 0.0 and (not ooms or int(ooms) <= 0):
        ram = max(ram, float(last_good) * 0.99)

    waited = int(s.tick - int(s.last_scheduled_tick.get(pid, s.tick)))
    if waited >= int(s.panic_wait_ticks):
        # Completion mode: avoid further OOMs; prefer a bigger, safe RAM allocation.
        ram = max(ram, max_ram * 0.70)
        try:
            max_cpu = float(getattr(pool, "max_cpu_pool", 1.0))
        except Exception:
            max_cpu = 1.0
        cpu = max(cpu, _clamp(0.08 * max_cpu, 3.0, 16.0))
        safety = max(safety, 1.10)

    ram *= safety

    # Respect learned upper bound if present.
    if hi is not None:
        try:
            hif = float(hi)
            if hif > 0.0:
                ram = min(ram, hif * 0.995)
                if lo and float(lo) > 0.0:
                    ram = max(ram, float(lo) * 1.01)
        except Exception:
            pass

    # Final caps
    ram = min(ram, max_ram * 0.94)
    ram = max(ram, float(s.min_ram))
    cpu = max(float(s.min_cpu), float(cpu))
    return float(cpu), float(ram)


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


def _any_runnable_in_queue(s, prio, scan_limit=64):
    q = s.queues_by_prio.get(prio, [])
    if not q:
        return False
    n = len(q)
    if n <= 0:
        return False
    start = int(s.rr_idx_by_prio.get(prio, 0))
    if start < 0 or start >= n:
        start = 0
    scanned = 0
    limit = int(scan_limit)
    if limit <= 0:
        limit = 1
    limit = min(limit, n)
    for i in range(n):
        if scanned >= limit:
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


def _extract_running_containers(pipelines):
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

    running = _extract_running_containers(pipelines)
    if not running:
        return

    def prio_rank(pr):
        if pr == Priority.BATCH_PIPELINE:
            return 0
        if pr == Priority.INTERACTIVE:
            return 1
        return 2

    # Free big RAM first among low priority.
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


def _stage_allows(stage_prio, eff_prio):
    # stage_prio is the class we're trying to schedule next.
    if stage_prio == Priority.QUERY:
        return eff_prio == Priority.QUERY
    if stage_prio == Priority.INTERACTIVE:
        return eff_prio in (Priority.INTERACTIVE, Priority.QUERY)
    return eff_prio == Priority.BATCH_PIPELINE


def _urgency_score(s, pid, base_prio):
    last = int(s.last_scheduled_tick.get(pid, s.tick))
    waited = int(s.tick - last)
    enq = int(s.enqueued_tick.get(pid, s.tick))
    age = int(s.tick - enq)
    fails = int(s.failed_seen_count.get(pid, 0))

    if base_prio == Priority.QUERY:
        bias = 520
    elif base_prio == Priority.INTERACTIVE:
        bias = 360
    else:
        bias = 220

    # Completion pressure: waited dominates, age gives slow steady push, failures bump.
    return bias + waited * 13 + age * 2 + min(220, fails * 18)


def _best_feasible_placement(
    s,
    pid,
    base_prio,
    eff_prio,
    op,
    local_avail_cpu,
    local_avail_ram,
    any_query_runnable,
    any_interactive_runnable,
    disable_reservations: bool,
):
    # Pool preference: if multiple pools, prefer pool 0 for query/interactive, non-0 for batch.
    pool_ids = list(range(s.executor.num_pools))
    if s.executor.num_pools > 1:
        if eff_prio in (Priority.QUERY, Priority.INTERACTIVE):
            pool_ids = [0] + [i for i in pool_ids if i != 0]
        else:
            pool_ids = [i for i in pool_ids if i != 0] + [0]

    best = None  # (waste_tuple, pool_id, cpu, ram)
    for pool_id in pool_ids:
        pool = s.executor.pools[pool_id]
        avail_cpu = float(local_avail_cpu.get(pool_id, 0.0))
        avail_ram = float(local_avail_ram.get(pool_id, 0.0))
        if avail_cpu < float(s.min_cpu) or avail_ram < float(s.min_ram):
            continue

        cpu_req, ram_req = _compute_request(s, pool, pid, base_prio, eff_prio, op, avail_cpu=avail_cpu)

        # Fit checks (RAM is hard constraint; CPU we can clip but must have at least min_cpu).
        if ram_req > avail_ram:
            continue
        cpu = float(cpu_req)
        if cpu > avail_cpu:
            cpu = avail_cpu
        if cpu < float(s.min_cpu):
            continue
        ram = float(ram_req)

        # Soft reservations: only restrict lower priority, primarily on pool 0 to protect interactive feel.
        if (not disable_reservations) and pool_id == 0:
            waited = int(s.tick - int(s.last_scheduled_tick.get(pid, s.tick)))
            panic = waited >= int(s.panic_wait_ticks)

            if not panic:
                if eff_prio == Priority.BATCH_PIPELINE:
                    reserve_cpu, reserve_ram = _reserve_for_higher_prio(
                        s, pool, any_query_runnable=any_query_runnable, any_interactive_runnable=any_interactive_runnable
                    )
                    # Relax reserves as batch waits.
                    relax = 0.0
                    if waited >= int(s.age_batch_to_interactive):
                        relax = 0.45
                    if waited >= int(s.age_batch_to_query):
                        relax = 0.75
                    if (avail_cpu - cpu) < reserve_cpu * (1.0 - relax):
                        continue
                    if (avail_ram - ram) < reserve_ram * (1.0 - relax):
                        continue
                elif eff_prio == Priority.INTERACTIVE:
                    reserve_cpu, reserve_ram = _reserve_for_higher_prio(
                        s, pool, any_query_runnable=any_query_runnable, any_interactive_runnable=False
                    )
                    if (avail_cpu - cpu) < reserve_cpu or (avail_ram - ram) < reserve_ram:
                        continue

        # Packing objective: fill RAM tightly to maximize utilization, then CPU.
        # Also prefer keeping some small slack RAM to reduce fragmentation.
        waste_ram = avail_ram - ram
        waste_cpu = avail_cpu - cpu
        waste_tuple = (waste_ram, waste_cpu)

        if best is None or waste_tuple < best[0]:
            best = (waste_tuple, pool_id, cpu, ram)

    if best is None:
        return None
    _, pool_id, cpu, ram = best
    return int(pool_id), float(cpu), float(ram)


def _attempt_fill(
    s,
    any_query_runnable,
    any_interactive_runnable,
    local_avail_cpu,
    local_avail_ram,
    per_pool_made,
    scheduled_count_by_pid,
    assigned_op_keys_by_pid,
    disable_reservations=False,
    max_total=None,
):
    assignments = []
    total_made = 0
    if max_total is None:
        max_total = int(s.max_total_assignments_per_tick)

    # Stage mix: protect query/interactive, but keep batch flowing to reduce incomplete penalties.
    stage_seq = [Priority.QUERY] * 10 + [Priority.INTERACTIVE] * 6 + [Priority.BATCH_PIPELINE] * 6
    seq_idx = 0

    while total_made < int(max_total):
        made_one = False

        # One "round" over stages; if nothing is placeable, stop.
        for _ in range(len(stage_seq)):
            stage = stage_seq[seq_idx]
            seq_idx = (seq_idx + 1) % len(stage_seq)

            # Scan candidates and pick best FEASIBLE (consider current local availability).
            best_cand = None
            # best_cand = (urgency, -tightness, pid, op, eff_prio, base_prio, chosen_pool, cpu, ram, qprio, next_rr)
            for qprio in (
                [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]
                if stage == Priority.QUERY
                else ([Priority.INTERACTIVE, Priority.BATCH_PIPELINE] if stage == Priority.INTERACTIVE else [Priority.BATCH_PIPELINE])
            ):
                q = s.queues_by_prio.get(qprio, [])
                if not q:
                    continue
                n = len(q)
                if n <= 0:
                    continue
                start = int(s.rr_idx_by_prio.get(qprio, 0))
                if start < 0 or start >= n:
                    start = 0

                scan_limit = min(int(s.scan_limit_per_pick), n)
                scanned = 0

                for i in range(n):
                    if scanned >= scan_limit:
                        break
                    idx = (start + i) % n
                    pid = q[idx]
                    p = s.pipelines_by_id.get(pid)
                    if p is None:
                        scanned += 1
                        continue

                    base_prio = p.priority
                    eff_prio = _effective_priority(s, pid, base_prio)
                    if not _stage_allows(stage, eff_prio):
                        scanned += 1
                        continue

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
                    urgency = _urgency_score(s, pid, base_prio)

                    # Consider a handful of ops and select one that can be placed now.
                    best_for_pid = None  # (tightness, op, pool_id, cpu, ram)
                    for op in ops[: int(s.max_ops_considered_per_pipeline)]:
                        k = _op_key(op)
                        if k in used:
                            continue
                        placement = _best_feasible_placement(
                            s,
                            pid=pid,
                            base_prio=base_prio,
                            eff_prio=eff_prio,
                            op=op,
                            local_avail_cpu=local_avail_cpu,
                            local_avail_ram=local_avail_ram,
                            any_query_runnable=any_query_runnable,
                            any_interactive_runnable=any_interactive_runnable,
                            disable_reservations=disable_reservations,
                        )
                        if placement is None:
                            continue
                        pool_id, cpu, ram = placement
                        avail_ram = float(local_avail_ram.get(pool_id, 0.0))
                        # Tightness: prefer filling RAM closely (low waste)
                        tightness = -(avail_ram - float(ram))
                        if best_for_pid is None or tightness > best_for_pid[0]:
                            best_for_pid = (tightness, op, pool_id, cpu, ram)

                    if best_for_pid is None:
                        scanned += 1
                        continue

                    # Compose overall candidate score: urgency dominates, then tightness.
                    tightness, op, pool_id, cpu, ram = best_for_pid
                    next_rr = (idx + 1) % n
                    cand = (urgency, tightness, pid, op, eff_prio, base_prio, pool_id, cpu, ram, qprio, next_rr)
                    if best_cand is None or cand[0] > best_cand[0] or (cand[0] == best_cand[0] and cand[1] > best_cand[1]):
                        best_cand = cand

                    scanned += 1

            if best_cand is None:
                continue

            urgency, tightness, pid, op, eff_prio, base_prio, pool_id, cpu, ram, qprio, next_rr = best_cand

            if per_pool_made.get(pool_id, 0) >= int(s.max_assignments_per_pool_per_tick):
                continue

            # Final guard against races in our local snapshot
            if float(local_avail_ram.get(pool_id, 0.0)) < float(ram) or float(local_avail_cpu.get(pool_id, 0.0)) < float(cpu):
                continue

            assignments.append(
                Assignment(
                    ops=[op],
                    cpu=float(cpu),
                    ram=float(ram),
                    priority=eff_prio,
                    pool_id=int(pool_id),
                    pipeline_id=pid,
                )
            )

            local_avail_cpu[pool_id] = max(0.0, float(local_avail_cpu.get(pool_id, 0.0)) - float(cpu))
            local_avail_ram[pool_id] = max(0.0, float(local_avail_ram.get(pool_id, 0.0)) - float(ram))
            per_pool_made[pool_id] = per_pool_made.get(pool_id, 0) + 1

            scheduled_count_by_pid[pid] = scheduled_count_by_pid.get(pid, 0) + 1
            used = assigned_op_keys_by_pid.get(pid)
            if used is None:
                used = set()
                assigned_op_keys_by_pid[pid] = used
            used.add(_op_key(op))

            s.last_scheduled_tick[pid] = s.tick
            s.rr_idx_by_prio[qprio] = next_rr

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
                    # OOM => converge fast to "no OOM": increase operator estimate and mild pipeline boost.
                    cur = float(s.pipeline_ram_boost.get(pid, 1.0))
                    nxt = max(cur * 1.18 + 0.03, cur + 0.22)
                    s.pipeline_ram_boost[pid] = min(float(s.max_ram_boost), nxt)

                    if ok is not None:
                        lo, hi, est, ooms, last_good = s.op_ram.get(ok, (0.0, None, 0.0, 0, 0.0))
                        ooms = int(ooms) + 1

                        # Lower bound jumps above what just OOM'd.
                        lo = max(float(lo), last_alloc * 1.35)

                        # If upper bound conflicts, drop it.
                        if hi is not None:
                            try:
                                if float(hi) < lo:
                                    hi = None
                            except Exception:
                                hi = None

                        # Aggressive estimate growth (but not insane).
                        if est is None or float(est) <= 0.0:
                            est = max(lo * 1.06, last_alloc * 1.80)
                        else:
                            est = max(float(est) * 1.65, last_alloc * 1.80, lo * 1.04)

                        s.op_ram[ok] = (float(lo), hi, float(est), int(ooms), float(last_good or 0.0))
                else:
                    # Non-OOM failures: small boost (often correlated with memory/other constraints).
                    cur = float(s.pipeline_ram_boost.get(pid, 1.0))
                    if cur < 2.2:
                        s.pipeline_ram_boost[pid] = min(2.2, cur * 1.08 + 0.02)
            else:
                # Success: decay pipeline boost quickly to restore concurrency.
                cur = float(s.pipeline_ram_boost.get(pid, 1.0))
                if cur > 1.0:
                    s.pipeline_ram_boost[pid] = max(1.0, cur * 0.975)

                if ok is not None:
                    lo, hi, est, ooms, last_good = s.op_ram.get(ok, (0.0, None, 0.0, 0, 0.0))

                    # Success => required <= last_alloc => tighten upper bound.
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

                    # Estimate decays toward successful allocation.
                    if est is None or float(est) <= 0.0:
                        est = last_alloc
                    else:
                        est = float(est) * 0.82 + last_alloc * 0.18

                    if hi is not None:
                        try:
                            est = min(float(est), float(hi) * 0.995)
                        except Exception:
                            pass
                    if lo > 0.0:
                        est = max(float(est), lo * 1.02)

                    last_good = max(float(last_good or 0.0), last_alloc)
                    ooms = max(0, int(ooms) - 1)

                    s.op_ram[ok] = (float(lo), hi, float(est), int(ooms), float(last_good))
        except Exception:
            pass

    _clean_queues(s)

    suspensions = []
    assignments = []

    # Local pool availability snapshot
    local_avail_cpu = {}
    local_avail_ram = {}
    per_pool_made = {}
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        local_avail_cpu[pool_id] = float(pool.avail_cpu_pool)
        local_avail_ram[pool_id] = float(pool.avail_ram_pool)
        per_pool_made[pool_id] = 0

    any_query_runnable = _any_runnable_in_queue(s, Priority.QUERY, scan_limit=128)
    any_interactive_runnable = _any_runnable_in_queue(s, Priority.INTERACTIVE, scan_limit=128)

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

    # Primary pass with reservations
    assignments.extend(
        _attempt_fill(
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

    # Work-conserving fill: always try to use remaining capacity, ignoring reservations.
    if s.enable_work_conserving_fill:
        assignments.extend(
            _attempt_fill(
                s,
                any_query_runnable=any_query_runnable,
                any_interactive_runnable=any_interactive_runnable,
                local_avail_cpu=local_avail_cpu,
                local_avail_ram=local_avail_ram,
                per_pool_made=per_pool_made,
                scheduled_count_by_pid=scheduled_count_by_pid,
                assigned_op_keys_by_pid=assigned_op_keys_by_pid,
                disable_reservations=True,
                max_total=max(0, int(s.max_total_assignments_per_tick) - len(assignments)),
            )
        )

    return suspensions, assignments
