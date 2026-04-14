from typing import List, Tuple

@register_scheduler_init(key="scheduler_low_001")
def scheduler_low_001_init(s):
    s.waiting_by_prio = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }
    s.known_pipeline_ids = set()

    # Resource learning
    # Keyed by (pipeline_id, op_object) for deterministic in-sim identity.
    s.op_ram_est = {}
    s.pipeline_ram_mult = {}  # multiplicative backoff on repeated failures

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
    s.max_assignments_per_pool_per_tick = 32
    s.max_scan_per_prio = 128

    s.min_cpu = 1.0
    s.min_ram = 1.0

    # CPU sizing (scale-up bias for high priority, but avoid over-scaling under congestion)
    s.cpu_frac = {
        Priority.QUERY: 0.30,
        Priority.INTERACTIVE: 0.18,
        Priority.BATCH_PIPELINE: 0.10,
    }
    s.cpu_cap = {
        Priority.QUERY: 48.0,
        Priority.INTERACTIVE: 24.0,
        Priority.BATCH_PIPELINE: 12.0,
    }

    # RAM sizing: start moderately conservative; learn upward on OOM/failures
    s.ram_base_frac = 0.08
    s.ram_cap_frac = 0.85
    s.max_pipeline_ram_mult = 16.0

    # Update factors
    s.fail_ram_mult = 2.2         # multiply RAM estimate on failure
    s.pipeline_mult_step = 1.6    # multiply pipeline multiplier on failure
    s.pipeline_mult_decay = 0.92  # decay on success (slow, to keep near-zero OOM)


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


def _drop_pipeline(s, p: Pipeline):
    pid = getattr(p, "pipeline_id", None)
    if pid is None:
        return
    if pid in s.known_pipeline_ids:
        s.known_pipeline_ids.remove(pid)
    s.pipeline_ram_mult.pop(pid, None)
    # Leave op_ram_est entries; they are keyed by (pid, op) so will be unreachable once pid is gone.


def _note_failure(s, r: ExecutionResult):
    pid = getattr(r, "pipeline_id", None)
    # Some simulators may not include pipeline_id on results; fall back to None-safe handling.
    if pid is not None:
        cur = s.pipeline_ram_mult.get(pid, 1.0)
        nxt = cur * s.pipeline_mult_step
        if nxt < cur + 0.25:
            nxt = cur + 0.25
        s.pipeline_ram_mult[pid] = min(s.max_pipeline_ram_mult, nxt)

    # Increase per-op RAM estimate based on the failed container's RAM limit.
    # (Failure may not always be OOM, but this avoids repeated retries and incompletes.)
    for op in getattr(r, "ops", []) or []:
        key = (pid, op)
        prev = s.op_ram_est.get(key, 0.0)
        est = float(getattr(r, "ram", 0.0) or 0.0) * s.fail_ram_mult
        if est < s.min_ram:
            est = s.min_ram
        if est > prev:
            s.op_ram_est[key] = est


def _note_success(s, r: ExecutionResult):
    pid = getattr(r, "pipeline_id", None)
    if pid is None:
        return
    cur = s.pipeline_ram_mult.get(pid, 1.0)
    if cur > 1.0:
        s.pipeline_ram_mult[pid] = max(1.0, cur * s.pipeline_mult_decay)


def _pipeline_runnable_ops(p: Pipeline):
    st = p.runtime_status()
    if st.is_pipeline_successful():
        return None
    ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
    if not ops:
        return []
    return ops


def _pick_runnable_pipeline_from_queue(s, prio, prefer_front: bool):
    q = s.waiting_by_prio[prio]
    if not q:
        return None, None

    scan = min(len(q), s.max_scan_per_prio)
    picked = None
    picked_ops = None

    for _ in range(scan):
        p = q.pop(0)
        ops = _pipeline_runnable_ops(p)

        if ops is None:
            _drop_pipeline(s, p)
            continue

        if ops:
            picked = p
            picked_ops = ops
            break

        # Not runnable yet; keep it moving (avoid hot-looping on blocked pipelines)
        q.append(p)

    # Restore if nothing found: keep queue as-is (already rotated)
    if picked is None:
        return None, None

    # For sticky behavior, we can put it back to the front (or end) after scheduling.
    if prefer_front:
        q.insert(0, picked)
    else:
        q.append(picked)
    return picked, picked_ops


def _estimate_cpu(s, prio, pool, remaining_cpu: float, backlog: int):
    base = float(pool.max_cpu_pool) * float(s.cpu_frac[prio])
    cap = min(float(s.cpu_cap[prio]), float(pool.max_cpu_pool))
    cpu = min(base, cap, remaining_cpu)
    if cpu < s.min_cpu:
        return 0.0

    # Under congestion, avoid giving too much CPU to one op; favor parallelism.
    if backlog >= (s.executor.num_pools * 12):
        cpu = min(cpu, max(s.min_cpu, remaining_cpu / 4.0))
    elif backlog >= (s.executor.num_pools * 6):
        cpu = min(cpu, max(s.min_cpu, remaining_cpu / 3.0))

    if cpu < s.min_cpu:
        return 0.0
    return cpu


def _estimate_ram(s, prio, pool, remaining_ram: float, pipeline_id, op, backlog: int):
    mult = s.pipeline_ram_mult.get(pipeline_id, 1.0)
    base = float(pool.max_ram_pool) * float(s.ram_base_frac) * float(mult)
    cap = float(pool.max_ram_pool) * float(s.ram_cap_frac)

    key = (pipeline_id, op)
    learned = float(s.op_ram_est.get(key, 0.0) or 0.0)
    ram = max(base, learned, s.min_ram)
    ram = min(ram, cap, remaining_ram)
    if ram < s.min_ram:
        return 0.0

    # For batch under heavy load, allow tighter packing (it can retry if wrong).
    if prio == Priority.BATCH_PIPELINE and backlog >= (s.executor.num_pools * 10):
        ram = min(ram, max(s.min_ram, remaining_ram / 2.0))

    if ram < s.min_ram:
        return 0.0
    return ram


def _choose_pool(s, prio, pipeline_id, op, remaining_cpu, remaining_ram, backlog: int):
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

        # Scoring:
        # - For QUERY/INTERACTIVE: favor pools with more headroom (reduce queueing, speed up)
        # - For BATCH: favor best-fit packing to increase RAM utilization
        if prio in (Priority.QUERY, Priority.INTERACTIVE):
            score = (rcpu - cpu) * 0.2 + (rram - ram) * 0.8  # smaller leftover is better
            # But also strongly prefer "bigger" pools for scale-up bias when ties
            score -= (float(pool.max_cpu_pool) * 0.01 + float(pool.max_ram_pool) * 0.0001)
        else:
            score = (rram - ram) * 1.0 + (rcpu - cpu) * 0.15  # pack RAM first

        if best_score is None or score < best_score:
            best_score = score
            best_pool = pool_id
            best_cpu = cpu
            best_ram = ram

    return best_pool, best_cpu, best_ram


@register_scheduler(key="scheduler_low_001")
def scheduler_low_001_scheduler(s, results: List[ExecutionResult], pipelines: List[Pipeline]):
    s.tick += 1

    # Ingest new pipelines (best-effort dedupe)
    for p in pipelines:
        _enqueue_pipeline(s, p)

    # Learn from results (RAM failures) and slightly relax multipliers on success
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
    total_avail_cpu = 0.0
    total_avail_ram = 0.0
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        remaining_cpu[pool_id] = float(pool.avail_cpu_pool)
        remaining_ram[pool_id] = float(pool.avail_ram_pool)
        total_avail_cpu += float(pool.avail_cpu_pool)
        total_avail_ram += float(pool.avail_ram_pool)

    backlog = (
        len(s.waiting_by_prio[Priority.QUERY])
        + len(s.waiting_by_prio[Priority.INTERACTIVE])
        + len(s.waiting_by_prio[Priority.BATCH_PIPELINE])
    )

    # Weighted deficits for INTERACTIVE vs BATCH fairness (QUERY remains strict-first)
    for prio in _prio_order_strict():
        s.prio_deficit[prio] += float(s.prio_weights[prio])

    max_total = int(max(1, s.executor.num_pools) * int(s.max_assignments_per_pool_per_tick))
    made_any = True
    made = 0

    while made < max_total and made_any:
        made_any = False

        # Stop quickly if no usable resources remain anywhere.
        any_room = False
        for pool_id in range(s.executor.num_pools):
            if remaining_cpu[pool_id] >= s.min_cpu and remaining_ram[pool_id] >= s.min_ram:
                any_room = True
                break
        if not any_room:
            break

        # Strict QUERY-first selection when runnable
        p_q, ops_q = _pick_runnable_pipeline_from_queue(s, Priority.QUERY, prefer_front=True)
        if p_q is not None and ops_q:
            prio = Priority.QUERY
            p = p_q
            ops = ops_q
        else:
            # Between INTERACTIVE and BATCH, use deficits + starvation guard
            # If batch is starving for too long, force a batch attempt.
            force_batch = s.ticks_since_batch >= 8 and len(s.waiting_by_prio[Priority.BATCH_PIPELINE]) > 0

            prio_candidates = [Priority.INTERACTIVE, Priority.BATCH_PIPELINE]
            if force_batch:
                prio_candidates = [Priority.BATCH_PIPELINE, Priority.INTERACTIVE]
            else:
                if s.prio_deficit[Priority.BATCH_PIPELINE] > s.prio_deficit[Priority.INTERACTIVE]:
                    prio_candidates = [Priority.BATCH_PIPELINE, Priority.INTERACTIVE]

            p = None
            ops = None
            prio = None
            for cand in prio_candidates:
                prefer_front = cand == Priority.INTERACTIVE
                pp, oo = _pick_runnable_pipeline_from_queue(s, cand, prefer_front=prefer_front)
                if pp is not None and oo:
                    prio = cand
                    p = pp
                    ops = oo
                    break

            if p is None:
                # Nothing runnable across all classes
                break

        # Choose an op to run (single-op assignments for safety)
        op = ops[0]
        pid = getattr(p, "pipeline_id", None)
        if pid is None:
            # Can't schedule without a pipeline id; drop it.
            _drop_pipeline(s, p)
            continue

        pool_id, cpu_req, ram_req = _choose_pool(
            s, prio, pid, op, remaining_cpu, remaining_ram, backlog
        )
        if pool_id is None or cpu_req < s.min_cpu or ram_req < s.min_ram:
            # Couldn't place now; avoid hot-looping.
            # For high priority, keep at front; for batch, rotate to end.
            q = s.waiting_by_prio[prio]
            if prio == Priority.BATCH_PIPELINE:
                if q and q[0] is p:
                    q.pop(0)
                    q.append(p)
            else:
                # keep it near front for latency
                pass
            # If placement fails repeatedly due to RAM fragmentation, allow other work.
            s.prio_deficit[prio] = max(0.0, s.prio_deficit[prio] - 0.5)
            break

        assignments.append(
            Assignment(
                ops=[op],
                cpu=cpu_req,
                ram=ram_req,
                priority=prio,
                pool_id=pool_id,
                pipeline_id=pid,
            )
        )

        remaining_cpu[pool_id] -= float(cpu_req)
        remaining_ram[pool_id] -= float(ram_req)

        # Account fairness / starvation
        s.prio_deficit[prio] = max(0.0, s.prio_deficit[prio] - 1.0)
        if prio == Priority.BATCH_PIPELINE:
            s.ticks_since_batch = 0
        else:
            s.ticks_since_batch += 1

        # Queue positioning: sticky for QUERY/INTERACTIVE to reduce end-to-end latency
        q = s.waiting_by_prio[prio]
        if q and q[0] is p:
            q.pop(0)
        if prio in (Priority.QUERY, Priority.INTERACTIVE):
            q.insert(0, p)
        else:
            q.append(p)

        made += 1
        made_any = True

    return suspensions, assignments
