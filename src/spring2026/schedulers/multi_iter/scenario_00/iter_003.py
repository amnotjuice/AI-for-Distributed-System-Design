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
    s.queued_ids = set()  # pipeline_ids currently enqueued (best-effort; lazily cleaned)

    # Time / aging (tick is deterministic "scheduler step count")
    s.tick = 0
    s.enqueued_tick = {}  # pipeline_id -> tick first enqueued
    s.last_scheduled_tick = {}  # pipeline_id -> tick last assigned an op

    # RAM adaptation:
    # Keep per-op lower/upper bounds and a running estimate.
    # - On OOM: raise lower bound and increase estimate.
    # - On success: lower upper bound and decrease estimate (slowly).
    # bounds: (lo, hi, est), where hi may be +inf-ish (None).
    s.op_ram_bounds = {}  # (pipeline_id, op_key) -> (lo, hi, est)

    # Coarse pipeline-level hedge (small, to avoid getting stuck with too-small per-op guesses early)
    s.pipeline_ram_boost = {}  # pipeline_id -> float
    s.max_ram_boost = 8.0

    # Track failures (best-effort; never drop pipelines)
    s.failed_seen_count = {}  # pipeline_id -> int

    # Scheduling knobs
    s.min_cpu = 1.0
    s.min_ram = 1.0

    # Per-tick guards
    s.max_assignments_per_pool_per_tick = 512
    s.max_total_assignments_per_tick = 4096

    # Limit multiple assignments from same pipeline in a single tick (status doesn't update mid-call)
    s.max_assignments_per_pid_per_tick = {
        Priority.QUERY: 8,
        Priority.INTERACTIVE: 4,
        Priority.BATCH_PIPELINE: 16,
    }

    # Headroom reservation to protect high-priority latency (reduced to avoid starving batch on small clusters)
    s.reserve_frac_for_query = 0.10
    s.reserve_frac_for_interactive = 0.05

    # Aging thresholds (in ticks) to prevent indefinite starvation (important due to incomplete penalties)
    s.age_interactive_to_query = 80
    s.age_batch_to_interactive = 60
    s.age_batch_to_query = 200

    # Pool preference (if multiple pools): prefer pool 0 for high-priority; avoid it for batch unless needed
    s.batch_avoid_pool0_penalty = 10_000.0
    s.highprio_prefer_pool0_bonus = -5.0

    # Candidate scan limits per scheduling attempt (avoid O(n^2) behavior)
    s.scan_limit_per_pick = 32


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
    s.enqueued_tick.pop(pipeline_id, None)
    s.last_scheduled_tick.pop(pipeline_id, None)
    # Keep op_ram_bounds entries; keyed by (pid, ...) so naturally become unreachable


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


def _base_cpu_for(pool, prio) -> float:
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
        mult = 1.6
    elif prio == Priority.INTERACTIVE:
        mult = 1.15
    else:
        mult = 0.80

    cpu = base * mult
    if cpu < 1.0:
        cpu = 1.0
    if cpu > 32.0:
        cpu = 32.0
    return cpu


def _base_ram_for(pool, prio) -> float:
    try:
        m = float(pool.max_ram_pool)
    except Exception:
        m = 1.0
    base = (m ** 0.5) * 2.0
    if base < 4.0:
        base = 4.0
    if base > 128.0:
        base = 128.0

    mult = 1.0
    if prio == Priority.QUERY:
        mult = 1.25
    elif prio == Priority.INTERACTIVE:
        mult = 1.05
    else:
        mult = 0.95

    ram = base * mult
    if ram < 1.0:
        ram = 1.0

    # Never request more than half a pool by default (hints/boosts can exceed base, but still cap later)
    half_pool = m * 0.5
    if ram > half_pool:
        ram = half_pool
    return ram


def _reserve_for_higher_prio(s, pool, any_query_runnable: bool, any_interactive_runnable: bool):
    max_cpu = float(pool.max_cpu_pool)
    max_ram = float(pool.max_ram_pool)

    reserve_cpu = 0.0
    reserve_ram = 0.0

    if any_query_runnable:
        reserve_cpu += max_cpu * float(s.reserve_frac_for_query)
        reserve_ram += max_ram * float(s.reserve_frac_for_query)
    if any_interactive_runnable:
        reserve_cpu += max_cpu * float(s.reserve_frac_for_interactive)
        reserve_ram += max_ram * float(s.reserve_frac_for_interactive)

    return reserve_cpu, reserve_ram


def _effective_priority(s, pid, base_prio):
    last = int(s.last_scheduled_tick.get(pid, s.tick))
    waited = int(s.tick - last)
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


def _compute_request(s, pool, pid, base_prio, eff_prio, op_list):
    cpu = _base_cpu_for(pool, eff_prio)
    base_ram = _base_ram_for(pool, eff_prio)

    # Add small safety margin for high-priority to reduce OOM-induced retries
    safety = 1.0
    if eff_prio == Priority.QUERY:
        safety = 1.12
    elif eff_prio == Priority.INTERACTIVE:
        safety = 1.08
    else:
        safety = 1.05

    boost = float(s.pipeline_ram_boost.get(pid, 1.0))
    if boost < 1.0:
        boost = 1.0
    if boost > float(s.max_ram_boost):
        boost = float(s.max_ram_boost)

    ram = base_ram * boost

    # Per-op bounds/estimate
    if op_list:
        try:
            k = (pid, _op_key(op_list[0]))
            lo, hi, est = s.op_ram_bounds.get(k, (0.0, None, 0.0))
            if est and est > 0.0:
                ram = max(ram, float(est))
            if lo and lo > 0.0:
                ram = max(ram, float(lo) * 1.05)
            if hi is not None:
                try:
                    hif = float(hi)
                    if hif > 0.0:
                        # Keep a small cushion below hi to avoid oscillating above the upper bound.
                        ram = min(ram, hif * 0.98)
                except Exception:
                    pass
        except Exception:
            pass

    ram *= safety

    # Global caps
    max_ram_cap = float(pool.max_ram_pool) * 0.92
    if ram > max_ram_cap:
        ram = max_ram_cap

    if cpu < float(s.min_cpu):
        cpu = float(s.min_cpu)
    if ram < float(s.min_ram):
        ram = float(s.min_ram)

    return float(cpu), float(ram)


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


def _pick_runnable(s, prio, scheduled_count_by_pid, any_query_runnable, any_interactive_runnable):
    """
    Pick a runnable (pid, op_list, eff_prio) candidate.
    Includes aged promotion: when scheduling QUERY, also consider aged INTERACTIVE/BATCH promoted to QUERY; etc.
    """
    # Build candidate queue list(s) for this stage
    candidate_queues = []
    if prio == Priority.QUERY:
        candidate_queues = [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]
    elif prio == Priority.INTERACTIVE:
        candidate_queues = [Priority.INTERACTIVE, Priority.BATCH_PIPELINE]
    else:
        candidate_queues = [Priority.BATCH_PIPELINE]

    best = None  # (rank, pid, op_list, eff_prio, base_prio, qprio, idx_advance)
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

            # Only allow promotion into this stage if eff_prio matches stage
            if prio == Priority.QUERY and eff_prio != Priority.QUERY:
                scanned += 1
                continue
            if prio == Priority.INTERACTIVE and eff_prio not in (Priority.INTERACTIVE, Priority.QUERY):
                scanned += 1
                continue
            if prio == Priority.BATCH_PIPELINE and eff_prio != Priority.BATCH_PIPELINE:
                scanned += 1
                continue

            # Per-pipeline per-tick cap
            cap = int(s.max_assignments_per_pid_per_tick.get(base_prio, 1))
            already = int(scheduled_count_by_pid.get(pid, 0))
            if already >= cap:
                scanned += 1
                continue

            try:
                status = p.runtime_status()
            except Exception:
                scanned += 1
                continue

            try:
                if status.is_pipeline_successful():
                    scanned += 1
                    continue
            except Exception:
                pass

            op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
            if not op_list:
                scanned += 1
                continue

            op_list = op_list[:1]

            # Prefer older (more waited) pipelines, and those in higher base priority when tied.
            waited = int(s.tick - int(s.last_scheduled_tick.get(pid, s.tick)))
            base_rank = 0
            if base_prio == Priority.QUERY:
                base_rank = 3
            elif base_prio == Priority.INTERACTIVE:
                base_rank = 2
            else:
                base_rank = 1

            # rank: bigger waited first, bigger base_rank first
            rank = (waited * 10 + base_rank)

            # Advance rr pointer for that queue upon selection (fairness)
            idx_advance = (idx + 1) % n

            cand = (rank, pid, op_list, eff_prio, base_prio, qprio, idx_advance)
            if best is None or cand[0] > best[0]:
                best = cand

            scanned += 1

    if best is None:
        return None, None, None, None

    _, pid, op_list, eff_prio, base_prio, qprio, idx_advance = best
    s.rr_idx_by_prio[qprio] = idx_advance
    return pid, op_list, eff_prio, base_prio


def _any_runnable_in_queue(s, prio, scan_limit=16):
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

            if is_failed:
                s.failed_seen_count[pid] = s.failed_seen_count.get(pid, 0) + 1

                if _is_oom_error(getattr(r, "error", None)):
                    # Slightly increase pipeline boost (bounded) to reduce repeated OOM early on
                    cur = float(s.pipeline_ram_boost.get(pid, 1.0))
                    nxt = max(cur * 1.30, cur + 0.15)
                    if nxt > float(s.max_ram_boost):
                        nxt = float(s.max_ram_boost)
                    s.pipeline_ram_boost[pid] = nxt

                    # Update per-op lower bound and estimate
                    ops = getattr(r, "ops", None) or []
                    if ops:
                        ok = (pid, _op_key(ops[0]))
                        lo, hi, est = s.op_ram_bounds.get(ok, (0.0, None, 0.0))
                        last_alloc = float(getattr(r, "ram", 0.0) or 0.0)
                        if last_alloc < 1.0:
                            last_alloc = 1.0
                        lo = max(float(lo), last_alloc)
                        # If we had an upper bound that is now below lo, invalidate it.
                        if hi is not None:
                            try:
                                hif = float(hi)
                                if hif < lo:
                                    hi = None
                            except Exception:
                                hi = None
                        # Increase estimate aggressively but not explosively
                        if est is None or float(est) <= 0.0:
                            est = lo * 1.35
                        else:
                            est = max(float(est) * 1.60, lo * 1.30)
                        s.op_ram_bounds[ok] = (lo, hi, est)
                else:
                    # Non-OOM failures: keep retries flowing; small boost to reduce flakiness
                    cur = float(s.pipeline_ram_boost.get(pid, 1.0))
                    if cur < 2.0:
                        s.pipeline_ram_boost[pid] = min(2.0, cur * 1.05 + 0.02)
            else:
                # Success: decay pipeline boost slowly and tighten per-op upper bound
                cur = float(s.pipeline_ram_boost.get(pid, 1.0))
                if cur > 1.0:
                    s.pipeline_ram_boost[pid] = max(1.0, cur * 0.995)

                ops = getattr(r, "ops", None) or []
                if ops:
                    ok = (pid, _op_key(ops[0]))
                    lo, hi, est = s.op_ram_bounds.get(ok, (0.0, None, 0.0))
                    last_alloc = float(getattr(r, "ram", 0.0) or 0.0)
                    if last_alloc < 1.0:
                        last_alloc = 1.0
                    # Success implies required <= last_alloc => update upper bound
                    try:
                        if hi is None:
                            hi = last_alloc
                        else:
                            hi = min(float(hi), last_alloc)
                    except Exception:
                        hi = last_alloc
                    # Keep lo <= hi
                    lo = float(lo)
                    if hi is not None:
                        try:
                            hif = float(hi)
                            if lo > hif:
                                lo = hif * 0.9
                        except Exception:
                            pass
                    # Adjust estimate down slowly but keep above lo
                    if est is None or float(est) <= 0.0:
                        est = last_alloc
                    else:
                        est = float(est) * 0.92
                    if hi is not None:
                        try:
                            est = min(float(est), float(hi) * 0.95)
                        except Exception:
                            pass
                    if lo > 0.0:
                        est = max(float(est), lo * 1.05)
                    s.op_ram_bounds[ok] = (lo, hi, est)
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

    # Determine runnable higher-priority work (not just "queue non-empty")
    any_query_runnable = _any_runnable_in_queue(s, Priority.QUERY, scan_limit=24)
    any_interactive_runnable = _any_runnable_in_queue(s, Priority.INTERACTIVE, scan_limit=24)

    scheduled_count_by_pid = {}

    # Weighted round-robin across priority stages (prevents batch starvation)
    # More QUERY slots, but always some batch slots.
    prio_seq = (
        [Priority.QUERY] * 7
        + [Priority.INTERACTIVE] * 4
        + [Priority.BATCH_PIPELINE] * 3
    )

    total_made = 0
    seq_idx = 0
    while total_made < int(s.max_total_assignments_per_tick):
        made_one = False

        # Try up to len(prio_seq) stages to find something runnable/placeable
        for _ in range(len(prio_seq)):
            prio = prio_seq[seq_idx]
            seq_idx = (seq_idx + 1) % len(prio_seq)

            pid, op_list, eff_prio, base_prio = _pick_runnable(
                s,
                prio,
                scheduled_count_by_pid,
                any_query_runnable=any_query_runnable,
                any_interactive_runnable=any_interactive_runnable,
            )
            if pid is None:
                continue

            p = s.pipelines_by_id.get(pid)
            if p is None:
                continue

            # Find best-fit pool
            best_pool = None
            best_cpu = None
            best_ram = None
            best_score = None

            # Pool iteration order based on effective priority
            pool_ids = list(range(s.executor.num_pools))
            if s.executor.num_pools > 1:
                if eff_prio in (Priority.QUERY, Priority.INTERACTIVE):
                    # Prefer pool 0 for high-priority
                    pool_ids = [0] + [i for i in pool_ids if i != 0]
                else:
                    # Prefer non-zero pools for batch
                    pool_ids = [i for i in pool_ids if i != 0] + [0]

            # If this pipeline is heavily aged, allow it to use pool0 earlier
            waited = int(s.tick - int(s.last_scheduled_tick.get(pid, s.tick)))
            if s.executor.num_pools > 1 and base_prio == Priority.BATCH_PIPELINE and waited >= int(s.age_batch_to_query):
                pool_ids = [0] + [i for i in range(s.executor.num_pools) if i != 0]

            for pool_id in pool_ids:
                if per_pool_made[pool_id] >= int(s.max_assignments_per_pool_per_tick):
                    continue

                pool = s.executor.pools[pool_id]
                avail_cpu = float(local_avail_cpu[pool_id])
                avail_ram = float(local_avail_ram[pool_id])

                if avail_cpu < float(s.min_cpu) or avail_ram < float(s.min_ram):
                    continue

                cpu_req, ram_req = _compute_request(s, pool, pid, base_prio, eff_prio, op_list)

                # Reservations apply based on effective priority (unless promoted to higher class)
                if eff_prio == Priority.BATCH_PIPELINE:
                    reserve_cpu, reserve_ram = _reserve_for_higher_prio(
                        s,
                        pool,
                        any_query_runnable=any_query_runnable,
                        any_interactive_runnable=any_interactive_runnable,
                    )
                    # Allow very old batch to slightly violate reserve to guarantee completion
                    relax = 0.0
                    if waited >= int(s.age_batch_to_interactive):
                        relax = 0.25
                    if (avail_cpu - float(s.min_cpu)) < (reserve_cpu * (1.0 - relax)):
                        continue
                    if (avail_ram - float(s.min_ram)) < (reserve_ram * (1.0 - relax)):
                        continue
                elif eff_prio == Priority.INTERACTIVE:
                    reserve_cpu, reserve_ram = _reserve_for_higher_prio(
                        s,
                        pool,
                        any_query_runnable=any_query_runnable,
                        any_interactive_runnable=False,
                    )
                    if (avail_cpu - float(s.min_cpu)) < reserve_cpu or (avail_ram - float(s.min_ram)) < reserve_ram:
                        continue

                # Fit checks
                if ram_req > avail_ram:
                    continue

                cpu = float(cpu_req)
                ram = float(ram_req)

                if cpu > avail_cpu:
                    cpu = avail_cpu
                if cpu < float(s.min_cpu):
                    continue

                # Pool preference penalty/bonus
                pref = 0.0
                if s.executor.num_pools > 1:
                    if eff_prio == Priority.BATCH_PIPELINE and pool_id == 0:
                        pref += float(s.batch_avoid_pool0_penalty)
                    if eff_prio in (Priority.QUERY, Priority.INTERACTIVE) and pool_id == 0:
                        pref += float(s.highprio_prefer_pool0_bonus)

                # Best-fit packing: prefer tighter RAM fit; small CPU term
                score = (avail_ram - ram) + 0.03 * (avail_cpu - cpu) + pref
                if best_score is None or score < best_score:
                    best_score = score
                    best_pool = pool_id
                    best_cpu = cpu
                    best_ram = ram

            if best_pool is None:
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

            # Update per-pipeline accounting (allow multiple assignments per pid per tick, up to cap)
            scheduled_count_by_pid[pid] = scheduled_count_by_pid.get(pid, 0) + 1
            s.last_scheduled_tick[pid] = s.tick

            total_made += 1
            made_one = True
            break

        if not made_one:
            break

    return suspensions, assignments
