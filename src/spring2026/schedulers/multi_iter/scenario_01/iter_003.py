from typing import List, Tuple

_INFLIGHT_STATES = {OperatorState.ASSIGNED, OperatorState.RUNNING, OperatorState.SUSPENDING}
_NOT_DONE_STATES = {
    OperatorState.PENDING,
    OperatorState.ASSIGNED,
    OperatorState.RUNNING,
    OperatorState.SUSPENDING,
    OperatorState.FAILED,
}


@register_scheduler_init(key="scheduler_low_001")
def scheduler_low_001_init(s):
    s.waiting_by_prio = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }
    s.known_pipeline_ids = set()
    s.pipeline_meta = {}  # pid -> {"arrival": int, "last_sched": int}

    # Resource learning:
    # For each op we keep a lower bound (failed with <= lo) and an upper bound (succeeded with <= hi).
    # Keyed by (pipeline_id, op_object) for deterministic in-sim identity.
    s.op_ram_lo = {}
    s.op_ram_hi = {}

    # Pipeline-level RAM multiplier: only used when we have little info; decays on success
    s.pipeline_ram_mult = {}
    s.max_pipeline_ram_mult = 8.0

    # Fairness / anti-starvation
    s.prio_weights = {
        Priority.QUERY: 10.0,
        Priority.INTERACTIVE: 5.0,
        Priority.BATCH_PIPELINE: 1.0,
    }
    s.prio_deficit = {
        Priority.QUERY: 0.0,
        Priority.INTERACTIVE: 0.0,
        Priority.BATCH_PIPELINE: 0.0,
    }
    s.ticks_since_batch = 0
    s.tick = 0

    # Scheduling knobs
    s.max_assignments_per_pool_per_tick = 64
    s.max_scan_per_prio = 192

    s.min_cpu = 1.0
    s.min_ram = 1.0

    # Concurrency limits per pipeline (avoid one pipeline consuming the whole cluster)
    s.max_inflight_per_pipeline = {
        Priority.QUERY: 2,
        Priority.INTERACTIVE: 3,
        Priority.BATCH_PIPELINE: 4,
    }

    # CPU sizing (favor parallelism under contention)
    s.cpu_frac = {
        Priority.QUERY: 0.22,
        Priority.INTERACTIVE: 0.14,
        Priority.BATCH_PIPELINE: 0.08,
    }
    s.cpu_cap = {
        Priority.QUERY: 24.0,
        Priority.INTERACTIVE: 16.0,
        Priority.BATCH_PIPELINE: 8.0,
    }

    # RAM sizing: smaller baseline to increase utilization; avoid "never schedulable" by high cap
    s.ram_base_frac = {
        Priority.QUERY: 0.035,
        Priority.INTERACTIVE: 0.028,
        Priority.BATCH_PIPELINE: 0.020,
    }
    s.ram_cap_frac = 0.995  # allow very large ops to run (if they fit at all)

    # Learning update factors
    s.fail_mult_oom = 1.60
    s.fail_mult_generic = 1.25
    s.pipeline_mult_step_oom = 1.25
    s.pipeline_mult_step_generic = 1.10
    s.pipeline_mult_decay = 0.90

    # Query protection (only enforced when queries are runnable)
    s.reserve_frac_when_queries = 0.15


def _prio_order_strict():
    return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]


def _enqueue_pipeline(s, p: Pipeline):
    pid = getattr(p, "pipeline_id", None)
    if pid is None:
        return
    if pid in s.known_pipeline_ids:
        return
    s.known_pipeline_ids.add(pid)
    s.waiting_by_prio[p.priority].append(p)
    s.pipeline_meta[pid] = {"arrival": s.tick, "last_sched": -1}


def _drop_pipeline(s, p: Pipeline):
    pid = getattr(p, "pipeline_id", None)
    if pid is None:
        return
    s.known_pipeline_ids.discard(pid)
    s.pipeline_meta.pop(pid, None)
    s.pipeline_ram_mult.pop(pid, None)


def _is_oom_error(err) -> bool:
    if err is None:
        return False
    try:
        msg = str(err).lower()
    except Exception:
        return False
    return ("oom" in msg) or ("out of memory" in msg) or ("killed" in msg and "memory" in msg)


def _note_failure(s, r: ExecutionResult):
    pid = getattr(r, "pipeline_id", None)
    oom = _is_oom_error(getattr(r, "error", None))
    if pid is not None:
        cur = float(s.pipeline_ram_mult.get(pid, 1.0))
        step = s.pipeline_mult_step_oom if oom else s.pipeline_mult_step_generic
        nxt = cur * float(step)
        if nxt < cur + 0.10:
            nxt = cur + 0.10
        s.pipeline_ram_mult[pid] = min(float(s.max_pipeline_ram_mult), nxt)

    mult = s.fail_mult_oom if oom else s.fail_mult_generic
    ram_lim = float(getattr(r, "ram", 0.0) or 0.0)
    if ram_lim < s.min_ram:
        ram_lim = s.min_ram

    for op in getattr(r, "ops", []) or []:
        key = (pid, op)
        prev_lo = float(s.op_ram_lo.get(key, 0.0) or 0.0)
        new_lo = max(prev_lo, ram_lim)
        s.op_ram_lo[key] = new_lo

        # If we already had a "hi" that is <= lo, bump hi upward (avoid inconsistent bounds)
        if key in s.op_ram_hi:
            hi = float(s.op_ram_hi.get(key, 0.0) or 0.0)
            if hi > 0.0 and hi <= new_lo:
                s.op_ram_hi[key] = new_lo * float(mult)


def _note_success(s, r: ExecutionResult):
    pid = getattr(r, "pipeline_id", None)
    if pid is not None:
        cur = float(s.pipeline_ram_mult.get(pid, 1.0))
        if cur > 1.0:
            s.pipeline_ram_mult[pid] = max(1.0, cur * float(s.pipeline_mult_decay))

    ram_lim = float(getattr(r, "ram", 0.0) or 0.0)
    if ram_lim < s.min_ram:
        ram_lim = s.min_ram

    for op in getattr(r, "ops", []) or []:
        key = (pid, op)
        prev_hi = s.op_ram_hi.get(key, None)
        if prev_hi is None or float(prev_hi) <= 0.0:
            s.op_ram_hi[key] = ram_lim
        else:
            s.op_ram_hi[key] = min(float(prev_hi), ram_lim)

        # Ensure lo <= hi
        if key in s.op_ram_lo:
            lo = float(s.op_ram_lo.get(key, 0.0) or 0.0)
            if lo > 0.0 and lo > float(s.op_ram_hi[key]):
                s.op_ram_lo[key] = float(s.op_ram_hi[key]) * 0.95


def _pipeline_runnable_ops(p: Pipeline):
    st = p.runtime_status()
    if st.is_pipeline_successful():
        return None
    ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
    if not ops:
        return []
    return ops


def _pipeline_inflight(p: Pipeline) -> int:
    st = p.runtime_status()
    ops = st.get_ops(_INFLIGHT_STATES, require_parents_complete=False)
    return int(len(ops) if ops else 0)


def _pipeline_remaining_ops(p: Pipeline) -> int:
    st = p.runtime_status()
    ops = st.get_ops(_NOT_DONE_STATES, require_parents_complete=False)
    return int(len(ops) if ops else 0)


def _estimate_cpu(s, prio, pool, remaining_cpu: float, backlog: int):
    base = float(pool.max_cpu_pool) * float(s.cpu_frac[prio])
    cap = min(float(s.cpu_cap[prio]), float(pool.max_cpu_pool))
    cpu = min(base, cap, float(remaining_cpu))
    if cpu < s.min_cpu:
        return 0.0

    # Under heavy backlog, favor more parallelism
    if backlog >= (s.executor.num_pools * 14):
        cpu = min(cpu, max(s.min_cpu, float(remaining_cpu) / 5.0))
    elif backlog >= (s.executor.num_pools * 8):
        cpu = min(cpu, max(s.min_cpu, float(remaining_cpu) / 4.0))
    elif backlog >= (s.executor.num_pools * 4):
        cpu = min(cpu, max(s.min_cpu, float(remaining_cpu) / 3.0))

    if cpu < s.min_cpu:
        return 0.0
    return float(cpu)


def _op_target_ram_unclamped(s, prio, pool_max_ram: float, pipeline_id, op):
    mult = float(s.pipeline_ram_mult.get(pipeline_id, 1.0))
    base = float(pool_max_ram) * float(s.ram_base_frac[prio]) * mult

    key = (pipeline_id, op)
    lo = float(s.op_ram_lo.get(key, 0.0) or 0.0)
    hi = s.op_ram_hi.get(key, None)
    hi = float(hi) if hi is not None else 0.0

    # If we have a known sufficient bound, prefer it (avoid OOM churn).
    if hi > 0.0:
        target = max(base, hi)
        # If we also have a failing bound, keep some margin above it.
        if lo > 0.0:
            target = max(target, lo * 1.08)
        return float(target)

    # Only a failing bound: step up from it (fast convergence, but not extreme).
    if lo > 0.0:
        target = max(base, lo * float(s.fail_mult_oom))
        return float(target)

    return float(max(base, s.min_ram))


def _estimate_ram(s, prio, pool, remaining_ram: float, pipeline_id, op, backlog: int):
    cap = float(pool.max_ram_pool) * float(s.ram_cap_frac)
    target = _op_target_ram_unclamped(s, prio, float(pool.max_ram_pool), pipeline_id, op)

    # Under heavy RAM pressure, allow batch to pack more tightly (it can retry),
    # but never under the known successful bound (handled in _op_target_ram_unclamped).
    if prio == Priority.BATCH_PIPELINE:
        if float(remaining_ram) < (0.18 * float(pool.max_ram_pool)) and backlog >= (s.executor.num_pools * 8):
            target = min(target, max(s.min_ram, float(remaining_ram) / 2.0))

    ram = min(float(target), cap, float(remaining_ram))
    if ram < s.min_ram:
        return 0.0
    return float(ram)


def _choose_pool(s, prio, pipeline_id, op, remaining_cpu, remaining_ram, backlog: int, reserve_cpu, reserve_ram):
    best_pool = None
    best_cpu = 0.0
    best_ram = 0.0
    best_score = None

    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        rcpu = float(remaining_cpu[pool_id])
        rram = float(remaining_ram[pool_id])
        if rcpu < s.min_cpu or rram < s.min_ram:
            continue

        cpu = _estimate_cpu(s, prio, pool, rcpu, backlog)
        if cpu < s.min_cpu:
            continue
        ram = _estimate_ram(s, prio, pool, rram, pipeline_id, op, backlog)
        if ram < s.min_ram:
            continue

        # Enforce query reserve for lower priorities when there are runnable queries.
        if prio != Priority.QUERY:
            if (rcpu - cpu) < float(reserve_cpu[pool_id]) or (rram - ram) < float(reserve_ram[pool_id]):
                continue

        # Score:
        # - QUERY/INTERACTIVE: prefer headroom to reduce interference and queueing
        # - BATCH: best-fit packing (RAM first)
        if prio in (Priority.QUERY, Priority.INTERACTIVE):
            # Encourage leaving headroom; penalize consuming the last RAM/CPU
            headroom_cpu = (rcpu - cpu) / max(1.0, float(pool.max_cpu_pool))
            headroom_ram = (rram - ram) / max(1.0, float(pool.max_ram_pool))
            score = -(headroom_cpu * 0.25 + headroom_ram * 0.75)
        else:
            # Best-fit: smaller leftover is better
            score = (rram - ram) * 1.0 + (rcpu - cpu) * 0.10

        if best_score is None or score < best_score:
            best_score = score
            best_pool = pool_id
            best_cpu = cpu
            best_ram = ram

    return best_pool, best_cpu, best_ram


def _pick_best_candidate(s, prio, remaining_cpu, remaining_ram, backlog: int, reserve_cpu, reserve_ram):
    q = s.waiting_by_prio[prio]
    if not q:
        return None

    scan = min(len(q), int(s.max_scan_per_prio))
    best = None  # (score, idx, p, op, pool_id, cpu, ram)

    for idx in range(scan):
        p = q[idx]
        pid = getattr(p, "pipeline_id", None)
        if pid is None:
            continue

        ops = _pipeline_runnable_ops(p)
        if ops is None:
            _drop_pipeline(s, p)
            continue
        if not ops:
            continue

        inflight = _pipeline_inflight(p)
        if inflight >= int(s.max_inflight_per_pipeline[prio]):
            continue

        # Pipeline priority inside class: SRPT-ish for QUERY/INTERACTIVE
        remaining_ops = _pipeline_remaining_ops(p)
        meta = s.pipeline_meta.get(pid, {"arrival": s.tick, "last_sched": -1})
        age = max(0, int(s.tick) - int(meta.get("arrival", s.tick)))
        last_sched = int(meta.get("last_sched", -1))
        since_last = max(0, int(s.tick) - last_sched) if last_sched >= 0 else age + 1

        # Choose an op among runnable ops:
        # - QUERY/INTERACTIVE: prefer "heavier" op (higher target RAM) to unblock critical work early.
        # - BATCH: prefer "lighter" op to pack fragments.
        best_op = None
        best_op_key = None
        best_op_val = None

        for op in ops:
            val = _op_target_ram_unclamped(s, prio, float(s.executor.pools[0].max_ram_pool), pid, op)
            if best_op is None:
                best_op, best_op_val = op, float(val)
                continue
            if prio == Priority.BATCH_PIPELINE:
                if float(val) < float(best_op_val):
                    best_op, best_op_val = op, float(val)
            else:
                if float(val) > float(best_op_val):
                    best_op, best_op_val = op, float(val)

        if best_op is None:
            continue

        pool_id, cpu_req, ram_req = _choose_pool(
            s, prio, pid, best_op, remaining_cpu, remaining_ram, backlog, reserve_cpu, reserve_ram
        )
        if pool_id is None or cpu_req < s.min_cpu or ram_req < s.min_ram:
            continue

        if prio in (Priority.QUERY, Priority.INTERACTIVE):
            # Strong SRPT component; also add aging to avoid pathological starvation within class.
            score = (
                float(remaining_ops) * 10.0
                - float(age) * 0.15
                - float(since_last) * 0.25
                + float(inflight) * 5.0
            )
        else:
            # Batch: favor aging + fairness, but keep packing by preferring ops that fit well.
            # Smaller ram_req is slightly preferred.
            score = (
                -float(age) * 0.50
                - float(since_last) * 0.30
                + float(inflight) * 2.0
                + float(ram_req) * 0.002
            )

        if best is None or float(score) < float(best[0]):
            best = (float(score), idx, p, best_op, pool_id, float(cpu_req), float(ram_req))

    return best


@register_scheduler(key="scheduler_low_001")
def scheduler_low_001_scheduler(s, results: List[ExecutionResult], pipelines: List[Pipeline]):
    s.tick += 1

    # Ingest new pipelines (best-effort dedupe)
    for p in pipelines or []:
        _enqueue_pipeline(s, p)

    # Learn from results
    for r in results or []:
        if getattr(r, "failed", None) is not None and r.failed():
            _note_failure(s, r)
        else:
            _note_success(s, r)

    suspensions = []
    assignments = []

    # Local remaining resources (executor view doesn't update within this call)
    remaining_cpu = {}
    remaining_ram = {}
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        remaining_cpu[pool_id] = float(pool.avail_cpu_pool)
        remaining_ram[pool_id] = float(pool.avail_ram_pool)

    backlog = (
        len(s.waiting_by_prio[Priority.QUERY])
        + len(s.waiting_by_prio[Priority.INTERACTIVE])
        + len(s.waiting_by_prio[Priority.BATCH_PIPELINE])
    )

    # Add fairness credits
    for prio in _prio_order_strict():
        s.prio_deficit[prio] += float(s.prio_weights[prio])

    # Determine whether there are runnable queries (enables reserve)
    query_runnable = False
    q_q = s.waiting_by_prio[Priority.QUERY]
    scan_q = min(len(q_q), int(s.max_scan_per_prio))
    for i in range(scan_q):
        p = q_q[i]
        ops = _pipeline_runnable_ops(p)
        if ops is None:
            _drop_pipeline(s, p)
            continue
        if ops:
            query_runnable = True
            break

    reserve_cpu = {}
    reserve_ram = {}
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        if query_runnable:
            reserve_cpu[pool_id] = float(pool.max_cpu_pool) * float(s.reserve_frac_when_queries)
            reserve_ram[pool_id] = float(pool.max_ram_pool) * float(s.reserve_frac_when_queries)
        else:
            reserve_cpu[pool_id] = 0.0
            reserve_ram[pool_id] = 0.0

    max_total = int(max(1, s.executor.num_pools) * int(s.max_assignments_per_pool_per_tick))
    made = 0
    consecutive_failures = 0

    while made < max_total:
        # Stop if no usable resources remain anywhere.
        any_room = False
        for pool_id in range(s.executor.num_pools):
            if remaining_cpu[pool_id] >= s.min_cpu and remaining_ram[pool_id] >= s.min_ram:
                any_room = True
                break
        if not any_room:
            break

        # Priority selection:
        # - If queries are runnable, always try them first.
        # - Otherwise schedule between INTERACTIVE and BATCH using deficits + starvation guard.
        prio_try = []
        if query_runnable:
            prio_try = [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]
        else:
            force_batch = (s.ticks_since_batch >= 6) and (len(s.waiting_by_prio[Priority.BATCH_PIPELINE]) > 0)
            if force_batch:
                prio_try = [Priority.BATCH_PIPELINE, Priority.INTERACTIVE]
            else:
                if float(s.prio_deficit[Priority.BATCH_PIPELINE]) > float(s.prio_deficit[Priority.INTERACTIVE]):
                    prio_try = [Priority.BATCH_PIPELINE, Priority.INTERACTIVE]
                else:
                    prio_try = [Priority.INTERACTIVE, Priority.BATCH_PIPELINE]

        picked = None
        for prio in prio_try:
            cand = _pick_best_candidate(s, prio, remaining_cpu, remaining_ram, backlog, reserve_cpu, reserve_ram)
            if cand is not None:
                picked = (prio,) + cand  # (prio, score, idx, p, op, pool, cpu, ram)
                break

        if picked is None:
            break

        prio, score, idx, p, op, pool_id, cpu_req, ram_req = picked

        pid = getattr(p, "pipeline_id", None)
        if pid is None:
            _drop_pipeline(s, p)
            consecutive_failures += 1
            if consecutive_failures >= 16:
                break
            continue

        # Commit assignment
        assignments.append(
            Assignment(
                ops=[op],
                cpu=float(cpu_req),
                ram=float(ram_req),
                priority=prio,
                pool_id=int(pool_id),
                pipeline_id=pid,
            )
        )
        remaining_cpu[pool_id] -= float(cpu_req)
        remaining_ram[pool_id] -= float(ram_req)

        meta = s.pipeline_meta.get(pid, None)
        if meta is not None:
            meta["last_sched"] = int(s.tick)

        # Fairness accounting
        s.prio_deficit[prio] = max(0.0, float(s.prio_deficit[prio]) - 1.0)
        if prio == Priority.BATCH_PIPELINE:
            s.ticks_since_batch = 0
        else:
            s.ticks_since_batch += 1

        # Rotate queue: remove at idx, reinsert at end (RR), but keep QUERY more sticky
        q = s.waiting_by_prio[prio]
        if 0 <= int(idx) < len(q) and q[int(idx)] is p:
            q.pop(int(idx))
        else:
            # Fallback: attempt to remove by identity (bounded)
            removed = False
            lim = min(len(q), int(s.max_scan_per_prio))
            for j in range(lim):
                if q[j] is p:
                    q.pop(j)
                    removed = True
                    break

        if prio == Priority.QUERY:
            # Keep queries near the front to reduce end-to-end latency, but not strictly head-of-line.
            q.insert(0, p)
        else:
            q.append(p)

        made += 1
        consecutive_failures = 0

    return suspensions, assignments
