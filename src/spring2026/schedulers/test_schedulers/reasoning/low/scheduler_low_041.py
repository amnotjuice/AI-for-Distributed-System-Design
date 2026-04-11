# policy_key: scheduler_low_041
# reasoning_effort: low
# model: gpt-5.2-2025-12-11
# llm_cost: 0.059287
# generation_seconds: 50.05
# generated_at: 2026-04-09T21:35:38.906659
@register_scheduler_init(key="scheduler_low_041")
def scheduler_low_041_init(s):
    """
    Priority- and failure-aware, low-risk policy focused on minimizing weighted end-to-end latency.

    Core ideas:
    - Separate FIFO queues per priority (QUERY > INTERACTIVE > BATCH) to protect high-weight latency.
    - Soft headroom reservation for high-priority work when scheduling batch (avoid blocking future queries).
    - Conservative, adaptive RAM retry on failures (assume OOM-like) to reduce failed/incomplete pipelines (720s penalty).
    - Prefer dedicated "high-priority pool" when multiple pools exist; otherwise share with reservation.
    - Keep policy simple: assign at most one ready operator per pool per tick (like baseline), but smarter selection/sizing.
    """
    s.q_query = []
    s.q_interactive = []
    s.q_batch = []

    # Per-(pipeline,op) resource hints updated from observed failures/successes.
    # Keys: (pipeline_id, op_key) where op_key is best-effort stable identifier.
    s.op_ram_hint = {}   # bytes/units as used by the simulator
    s.op_cpu_hint = {}   # vCPUs

    # Retry counters to avoid infinite tight loops; we still keep retrying with capped growth.
    s.op_fail_count = {}

    # Aging to avoid starvation: track arrival tick order.
    s._tick = 0
    s._arrival_seq = 0
    s._pipeline_arrival_seq = {}  # pipeline_id -> seq

    # Cache of pipelines by id so we can find queues efficiently.
    s._pipelines = {}


def _priority_rank(p):
    # Lower is higher priority
    if p == Priority.QUERY:
        return 0
    if p == Priority.INTERACTIVE:
        return 1
    return 2


def _enqueue_pipeline(s, pipeline):
    pid = pipeline.pipeline_id
    if pid not in s._pipeline_arrival_seq:
        s._pipeline_arrival_seq[pid] = s._arrival_seq
        s._arrival_seq += 1
    s._pipelines[pid] = pipeline

    if pipeline.priority == Priority.QUERY:
        s.q_query.append(pid)
    elif pipeline.priority == Priority.INTERACTIVE:
        s.q_interactive.append(pid)
    else:
        s.q_batch.append(pid)


def _op_key(pipeline_id, op):
    # Best-effort stable key; prefer common attribute names if present.
    for attr in ("op_id", "operator_id", "id", "name"):
        if hasattr(op, attr):
            try:
                v = getattr(op, attr)
                if v is not None:
                    return (pipeline_id, str(v))
            except Exception:
                pass
    return (pipeline_id, str(id(op)))


def _hint_get(s, dct, key, default):
    v = dct.get(key, None)
    return default if v is None else v


def _update_hints_from_results(s, results):
    # Update RAM/CPU hints based on completed/failed executions.
    for r in results:
        try:
            ops = getattr(r, "ops", None) or []
        except Exception:
            ops = []
        pid = getattr(r, "pipeline_id", None)

        # Some simulators may not attach pipeline_id to results; fall back to None and skip hinting.
        if pid is None:
            continue

        for op in ops:
            k = _op_key(pid, op)

            # Track last used cpu/ram as a baseline.
            if getattr(r, "ram", None) is not None:
                s.op_ram_hint[k] = max(s.op_ram_hint.get(k, 0) or 0, r.ram)
            if getattr(r, "cpu", None) is not None:
                s.op_cpu_hint[k] = max(s.op_cpu_hint.get(k, 0) or 0, r.cpu)

            # On failure, increase RAM hint aggressively (most costly failures are OOM-like).
            if hasattr(r, "failed") and r.failed():
                s.op_fail_count[k] = s.op_fail_count.get(k, 0) + 1

                prev = s.op_ram_hint.get(k, getattr(r, "ram", None) or 0) or 0
                # Exponential backoff with cap; keep within pool max at assignment time.
                # Minimum bump to avoid stuck retries when prev is 0/unknown.
                bumped = int(max(prev * 2, prev + max(1, int(prev * 0.25)), prev + 1))
                s.op_ram_hint[k] = max(prev, bumped)
            else:
                # On success, slowly reduce failure count to allow recovery.
                if k in s.op_fail_count and s.op_fail_count[k] > 0:
                    s.op_fail_count[k] -= 1


def _is_done_or_dead(pipeline):
    st = pipeline.runtime_status()
    # If any FAILED exists, we still try to recover by re-scheduling FAILED (ASSIGNABLE_STATES includes FAILED).
    # Only treat as terminal if simulator marks pipeline successful.
    return st.is_pipeline_successful()


def _pick_next_pipeline_id(s):
    """
    Pick next pipeline id to attempt scheduling from, with:
    - strict priority order
    - mild aging within each priority (FIFO by arrival sequence)
    """
    # Remove stale pipeline ids that have completed.
    def _clean(q):
        out = []
        for pid in q:
            p = s._pipelines.get(pid, None)
            if p is None:
                continue
            if _is_done_or_dead(p):
                continue
            out.append(pid)
        return out

    s.q_query = _clean(s.q_query)
    s.q_interactive = _clean(s.q_interactive)
    s.q_batch = _clean(s.q_batch)

    for q in (s.q_query, s.q_interactive, s.q_batch):
        if q:
            return q.pop(0)
    return None


def _has_pending_high_priority(s):
    # Any not-yet-successful query/interactive remaining in queues?
    for q in (s.q_query, s.q_interactive):
        for pid in q:
            p = s._pipelines.get(pid, None)
            if p is not None and not _is_done_or_dead(p):
                return True
    return False


def _ready_ops_for_pipeline(pipeline):
    st = pipeline.runtime_status()
    # One-op-at-a-time to keep policy stable; require parents complete for correctness.
    ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
    return ops or []


def _choose_pool_for_priority(num_pools, priority):
    # If we have multiple pools, prefer pool 0 as a "high-priority lane".
    # Batch prefers non-zero pools to reduce interference.
    if num_pools <= 1:
        return None  # caller will iterate
    if priority in (Priority.QUERY, Priority.INTERACTIVE):
        return 0
    # Batch: prefer the last pool (often least contended in simple configs)
    return num_pools - 1


def _compute_reservation(pool, has_high_pri_waiting):
    # Soft reservation to reduce chance that a big batch op blocks upcoming query/interactive.
    if not has_high_pri_waiting:
        return 0, 0
    # Reserve a fraction; keep modest to avoid starving batch.
    res_cpu = max(1, int(pool.max_cpu_pool * 0.30)) if pool.max_cpu_pool > 1 else 0
    res_ram = int(pool.max_ram_pool * 0.30) if pool.max_ram_pool > 1 else 0
    return res_cpu, res_ram


def _size_for_op(s, pipeline, op, pool, avail_cpu, avail_ram, allow_reservation, has_high_pri_waiting):
    pid = pipeline.pipeline_id
    k = _op_key(pid, op)

    # Headroom reservation applies mainly when placing batch on a shared pool.
    res_cpu, res_ram = (0, 0)
    if allow_reservation and pipeline.priority == Priority.BATCH_PIPELINE:
        res_cpu, res_ram = _compute_reservation(pool, has_high_pri_waiting)

    eff_cpu = max(0, avail_cpu - res_cpu)
    eff_ram = max(0, avail_ram - res_ram)

    if eff_cpu <= 0 or eff_ram <= 0:
        return 0, 0

    # RAM: use hint if any; otherwise take a reasonable initial slice.
    # If prior failures exist, bias toward larger RAM to avoid repeated 720s penalties.
    fail_cnt = s.op_fail_count.get(k, 0)
    hinted_ram = s.op_ram_hint.get(k, 0) or 0

    if hinted_ram > 0:
        ram = min(eff_ram, hinted_ram)
        # If we have failure history, proactively allocate more (within pool limits).
        if fail_cnt >= 1:
            ram = min(eff_ram, max(ram, int(min(pool.max_ram_pool, ram * 1.5))))
    else:
        # Initial guess: high priority gets more RAM to avoid OOM retries.
        if pipeline.priority in (Priority.QUERY, Priority.INTERACTIVE):
            ram = min(eff_ram, int(pool.max_ram_pool * 0.60) if pool.max_ram_pool > 1 else eff_ram)
        else:
            ram = min(eff_ram, int(pool.max_ram_pool * 0.40) if pool.max_ram_pool > 1 else eff_ram)

    ram = max(1, int(ram))

    # CPU: give high priority as much as possible for latency; throttle batch slightly to reduce blocking.
    hinted_cpu = s.op_cpu_hint.get(k, 0) or 0
    if pipeline.priority in (Priority.QUERY, Priority.INTERACTIVE):
        cpu = eff_cpu
        # If we have a useful hint smaller than all available, cap to hint to reduce waste.
        if hinted_cpu > 0:
            cpu = min(cpu, hinted_cpu)
    else:
        # Batch: cap to ~60% of pool to keep some headroom even without explicit reservation.
        cap = max(1, int(pool.max_cpu_pool * 0.60)) if pool.max_cpu_pool > 1 else eff_cpu
        cpu = min(eff_cpu, cap)
        if hinted_cpu > 0:
            cpu = min(cpu, hinted_cpu)

    cpu = max(1, int(cpu))
    cpu = min(cpu, eff_cpu)

    # Final clamp
    ram = min(ram, eff_ram)
    return cpu, ram


@register_scheduler(key="scheduler_low_041")
def scheduler_low_041(s, results: list, pipelines: list):
    """
    Scheduler step:
    - ingest new pipelines into per-priority queues
    - update resource hints from execution results (retry with higher RAM on failure)
    - for each pool, select a pipeline/op and assign one operator if resources permit
    """
    s._tick += 1

    for p in pipelines:
        _enqueue_pipeline(s, p)

    if not pipelines and not results:
        return [], []

    _update_hints_from_results(s, results)

    suspensions = []
    assignments = []

    has_high_pri_waiting = _has_pending_high_priority(s)

    # If multi-pool, try to bias placement by priority; otherwise iterate all pools.
    # We still allow any pool to be used as a fallback if the preferred pool is full.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool
        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # Decide which priority should primarily target this pool.
        preferred_for_pool = None
        if s.executor.num_pools > 1:
            if pool_id == 0:
                preferred_for_pool = (Priority.QUERY, Priority.INTERACTIVE)
            else:
                preferred_for_pool = (Priority.BATCH_PIPELINE, Priority.INTERACTIVE, Priority.QUERY)

        # Attempt a few picks to find something ready that fits.
        # Keep small to avoid O(n) scans.
        tries = 6
        picked = False

        while tries > 0 and not picked:
            tries -= 1

            pid = _pick_next_pipeline_id(s)
            if pid is None:
                break

            pipeline = s._pipelines.get(pid, None)
            if pipeline is None:
                continue

            if preferred_for_pool is not None and pipeline.priority not in preferred_for_pool:
                # Put back to end of its queue (avoid dropping) and continue.
                # This preserves FIFO within that priority roughly while still biasing placement.
                if pipeline.priority == Priority.QUERY:
                    s.q_query.append(pid)
                elif pipeline.priority == Priority.INTERACTIVE:
                    s.q_interactive.append(pid)
                else:
                    s.q_batch.append(pid)
                continue

            ops = _ready_ops_for_pipeline(pipeline)
            if not ops:
                # Nothing schedulable now; requeue and try next.
                if pipeline.priority == Priority.QUERY:
                    s.q_query.append(pid)
                elif pipeline.priority == Priority.INTERACTIVE:
                    s.q_interactive.append(pid)
                else:
                    s.q_batch.append(pid)
                continue

            # One op at a time.
            op_list = ops[:1]

            # If multi-pool: if pipeline is high-priority but we're not on pool 0, allow it only if pool 0 is saturated.
            if s.executor.num_pools > 1 and pipeline.priority in (Priority.QUERY, Priority.INTERACTIVE):
                preferred_pool = _choose_pool_for_priority(s.executor.num_pools, pipeline.priority)
                if preferred_pool == 0 and pool_id != 0:
                    # Only spill over if pool 0 currently has no headroom at all.
                    p0 = s.executor.pools[0]
                    if p0.avail_cpu_pool > 0 and p0.avail_ram_pool > 0:
                        # Requeue and wait for pool 0.
                        if pipeline.priority == Priority.QUERY:
                            s.q_query.append(pid)
                        else:
                            s.q_interactive.append(pid)
                        continue

            # Reservation is relevant primarily in single-pool or when batch uses pool 0.
            allow_reservation = True if s.executor.num_pools <= 1 or pool_id == 0 else False
            cpu, ram = _size_for_op(
                s, pipeline, op_list[0], pool, avail_cpu, avail_ram,
                allow_reservation=allow_reservation,
                has_high_pri_waiting=has_high_pri_waiting
            )

            if cpu <= 0 or ram <= 0:
                # Can't fit now; requeue.
                if pipeline.priority == Priority.QUERY:
                    s.q_query.append(pid)
                elif pipeline.priority == Priority.INTERACTIVE:
                    s.q_interactive.append(pid)
                else:
                    s.q_batch.append(pid)
                continue

            assignment = Assignment(
                ops=op_list,
                cpu=cpu,
                ram=ram,
                priority=pipeline.priority,
                pool_id=pool_id,
                pipeline_id=pipeline.pipeline_id
            )
            assignments.append(assignment)
            picked = True

        # Keep at most one assignment per pool per tick for stability (like baseline).
        # If picked, move to next pool.
        continue

    return suspensions, assignments
