# source: scheduler_low_011.py
# model: gpt-5.2-2025-12-11
# effort: low
# feedback: rich
# iteration: r9
@register_scheduler_init(key="scheduler_low_011_r9")
def scheduler_low_011_r9_init(s):
    """Priority-aware, low-churn scheduler focused on lowering latency while avoiding starvation.

    Incremental improvements over naive FIFO (and the prior iteration):
    - Separate per-priority FIFO queues (QUERY, INTERACTIVE, BATCH).
    - Weighted round-robin between QUERY and INTERACTIVE so INTERACTIVE makes progress (prevents starvation).
    - Pack multiple operators per pool per tick (instead of at most one), while ensuring:
        * At most one assignment per pipeline per scheduler tick (avoids duplicate-assigning due to stale runtime_status).
    - Conservative "chunk" CPU sizing to increase concurrency and reduce queueing delay.
    - Lower initial RAM requests (optimistic), with OOM-aware exponential backoff hints if failures occur.
    - Keep headroom in non-primary pools when high-priority backlog exists (reduces tail latency on bursts).
    """
    from collections import deque

    s.queues = {
        Priority.QUERY: deque(),
        Priority.INTERACTIVE: deque(),
        Priority.BATCH_PIPELINE: deque(),
    }

    # Weighted RR pattern for high-priority classes (QUERY favored, but INTERACTIVE guaranteed progress)
    s.hp_rr = [Priority.QUERY, Priority.QUERY, Priority.INTERACTIVE, Priority.QUERY, Priority.INTERACTIVE]
    s.hp_rr_cursor = 0

    # Per-operator resource hints (keyed by op object identity)
    s.op_hints = {}       # op_id -> {"ram": float, "cpu": float}
    s.op_attempts = {}    # op_id -> int
    s.op_blacklist = set()  # op_ids that failed with non-OOM errors (avoid infinite retry loops)

    s.max_retries_per_op = 3

    # Packing / sizing knobs
    s.max_assignments_per_pool = 32
    s.scan_limit_per_pick = 64

    # CPU chunking improves concurrency (reduces queueing -> lower latency)
    s.base_cpu = {
        Priority.QUERY: 2.0,
        Priority.INTERACTIVE: 2.0,
        Priority.BATCH_PIPELINE: 1.0,
    }

    # Optimistic initial RAM to increase concurrency; rely on OOM backoff when needed
    s.base_ram_frac = {
        Priority.QUERY: 0.20,
        Priority.INTERACTIVE: 0.25,
        Priority.BATCH_PIPELINE: 0.15,
    }

    # Prefer pool 0 for high priority, but allow spillover when pool 0 is busy
    s.primary_hp_pool = 0

    # Reserve headroom on non-primary pools when HP backlog exists (prevents HP burst latency spikes)
    s.reserve_cpu_frac = 0.25
    s.reserve_ram_frac = 0.25


def _is_oom_error(err):
    if err is None:
        return False
    msg = str(err).lower()
    return ("oom" in msg) or ("out of memory" in msg) or ("memoryerror" in msg)


def _clamp(x, lo, hi):
    if x < lo:
        return lo
    if x > hi:
        return hi
    return x


def _op_id(op):
    return id(op)


def _clean_queue_prefix(q, limit):
    """Drop completed pipelines from the front portion of a queue to keep backlog estimates sane."""
    n = min(len(q), limit)
    for _ in range(n):
        p = q.popleft()
        st = p.runtime_status()
        if st.is_pipeline_successful():
            continue
        q.append(p)


def _pick_ready_from_queue(s, pr, assigned_pipeline_ids, assigned_op_ids):
    """Rotate through the queue to find a ready (assignable) op, preserving FIFO-ish fairness."""
    q = s.queues[pr]
    if not q:
        return None, None

    scan = min(len(q), s.scan_limit_per_pick)
    for _ in range(scan):
        p = q.popleft()

        st = p.runtime_status()
        if st.is_pipeline_successful():
            # Drop completed pipelines entirely
            continue

        if p.pipeline_id in assigned_pipeline_ids:
            # Only one assignment per pipeline per scheduler tick to avoid duplicate assigns
            q.append(p)
            continue

        ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        if not ops:
            q.append(p)
            continue

        chosen = None
        for op in ops:
            oid = _op_id(op)
            if oid in assigned_op_ids:
                continue
            if oid in s.op_blacklist:
                continue
            chosen = op
            break

        if chosen is None:
            # Assignable ops exist but all are blacklisted/already-chosen in this tick
            q.append(p)
            continue

        # Requeue pipeline at tail for fairness across pipelines
        q.append(p)
        return p, chosen

    return None, None


def _pick_hp(s, assigned_pipeline_ids, assigned_op_ids):
    """Weighted RR across QUERY and INTERACTIVE, with fallback if one class has no ready work."""
    rr = s.hp_rr
    for _ in range(len(rr)):
        pr = rr[s.hp_rr_cursor % len(rr)]
        s.hp_rr_cursor += 1
        p, op = _pick_ready_from_queue(s, pr, assigned_pipeline_ids, assigned_op_ids)
        if p is not None:
            return pr, p, op

    # Fallback: try both explicitly
    for pr in (Priority.QUERY, Priority.INTERACTIVE):
        p, op = _pick_ready_from_queue(s, pr, assigned_pipeline_ids, assigned_op_ids)
        if p is not None:
            return pr, p, op

    return None, None, None


def _size_request(s, pool, pr, op, hp_backlog):
    """Choose CPU/RAM for an assignment; aim for concurrency (low queueing) + low tail latency."""
    # Start from base CPU chunk; adapt slightly based on HP backlog
    cpu = float(s.base_cpu.get(pr, 1.0))
    if pr in (Priority.QUERY, Priority.INTERACTIVE):
        # If HP backlog is small, give a bit more CPU to reduce single-job latency.
        if hp_backlog <= 2:
            cpu = max(cpu, 4.0)
        elif hp_backlog >= 16:
            cpu = min(cpu, 2.0)

    # Apply learned hint if any (use max to avoid regressing below known-safe allocations)
    hint = s.op_hints.get(_op_id(op))
    if hint:
        cpu = max(cpu, float(hint.get("cpu", cpu)))

    cpu = _clamp(cpu, 1.0, pool.max_cpu_pool)

    # RAM: optimistic fraction of pool max, with hint backoff support
    ram = float(pool.max_ram_pool) * float(s.base_ram_frac.get(pr, 0.20))
    ram = max(1.0, ram)
    if hint:
        ram = max(ram, float(hint.get("ram", ram)))

    ram = _clamp(ram, 1.0, pool.max_ram_pool)
    return cpu, ram


@register_scheduler(key="scheduler_low_011_r9")
def scheduler_low_011_r9(s, results, pipelines):
    """
    Scheduler loop:
    - Enqueue new pipelines into per-priority queues.
    - Update OOM hints / blacklist operators from execution results.
    - Clean queue prefixes (drop completed) to prevent backlog distortion.
    - For each pool, pack multiple assignments, prioritizing HP work:
        * Pool 0 focuses on HP.
        * Other pools run BATCH but keep headroom when HP backlog exists, and spill HP when pool 0 is busy.
    """
    # Enqueue new arrivals
    for p in pipelines:
        pr = p.priority if p.priority in s.queues else Priority.BATCH_PIPELINE
        s.queues[pr].append(p)

    # Learn from results (OOM backoff; blacklist non-OOM failures)
    for r in results:
        failed = False
        try:
            failed = r.failed()
        except Exception:
            failed = getattr(r, "error", None) is not None

        if not failed:
            continue

        ops = getattr(r, "ops", None) or []
        if _is_oom_error(getattr(r, "error", None)):
            for op in ops:
                oid = _op_id(op)
                s.op_attempts[oid] = int(s.op_attempts.get(oid, 0)) + 1
                if s.op_attempts[oid] > s.max_retries_per_op:
                    # Too many retries; avoid infinite churn on hopeless ops
                    s.op_blacklist.add(oid)
                    continue

                prev = s.op_hints.get(oid, {})
                prev_ram = float(prev.get("ram", 0.0)) or float(getattr(r, "ram", 1.0) or 1.0)
                prev_cpu = float(prev.get("cpu", 0.0)) or float(getattr(r, "cpu", 1.0) or 1.0)

                # Exponential RAM backoff; keep CPU at least what we had
                s.op_hints[oid] = {"ram": max(1.0, prev_ram * 2.0), "cpu": max(1.0, prev_cpu)}
        else:
            for op in ops:
                s.op_blacklist.add(_op_id(op))

    # Lightweight cleanup to keep queues from filling with completed pipelines
    for pr in (Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE):
        _clean_queue_prefix(s.queues[pr], limit=128)

    suspensions = []
    assignments = []

    # Backlog signal (approximate, but good enough for sizing/headroom decisions)
    hp_backlog = len(s.queues[Priority.QUERY]) + len(s.queues[Priority.INTERACTIVE])

    # Pool 0 busy? If so, allow HP spillover into other pools.
    pool0_busy = False
    if s.executor.num_pools > 0:
        p0 = s.executor.pools[min(s.primary_hp_pool, s.executor.num_pools - 1)]
        pool0_busy = (
            (p0.avail_cpu_pool < 0.25 * p0.max_cpu_pool) or
            (p0.avail_ram_pool < 0.25 * p0.max_ram_pool)
        )

    assigned_pipeline_ids = set()
    assigned_op_ids = set()

    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)

        if avail_cpu < 1.0 or avail_ram < 1.0:
            continue

        # Decide role / headroom on this pool
        is_primary_hp_pool = (pool_id == s.primary_hp_pool)

        # Reserve headroom on non-primary pools when HP backlog exists,
        # unless we are actively spilling HP onto this pool.
        spill_hp_here = (not is_primary_hp_pool) and (hp_backlog > 0) and pool0_busy
        if (not is_primary_hp_pool) and (hp_backlog > 0) and (not spill_hp_here):
            reserve_cpu = max(1.0, pool.max_cpu_pool * float(s.reserve_cpu_frac))
            reserve_ram = max(1.0, pool.max_ram_pool * float(s.reserve_ram_frac))
        else:
            reserve_cpu = 0.0
            reserve_ram = 0.0

        planned = 0
        while planned < s.max_assignments_per_pool:
            if avail_cpu < 1.0 or avail_ram < 1.0:
                break

            # If we are on pool 0 (or spilling due to busy pool 0), prioritize HP work.
            # Otherwise, run batch but stop when reaching reserved headroom.
            want_hp = False
            if hp_backlog > 0:
                if is_primary_hp_pool:
                    want_hp = True
                elif spill_hp_here:
                    want_hp = True

            if not want_hp:
                # Respect headroom on batch-focused pools
                if (avail_cpu <= reserve_cpu) or (avail_ram <= reserve_ram):
                    break

            pr = None
            p = None
            op = None

            if want_hp:
                pr, p, op = _pick_hp(s, assigned_pipeline_ids, assigned_op_ids)
                if p is None:
                    # No ready HP work right now; allow batch (especially on primary pool)
                    pr = Priority.BATCH_PIPELINE
                    p, op = _pick_ready_from_queue(s, pr, assigned_pipeline_ids, assigned_op_ids)
            else:
                pr = Priority.BATCH_PIPELINE
                p, op = _pick_ready_from_queue(s, pr, assigned_pipeline_ids, assigned_op_ids)
                if p is None and hp_backlog > 0 and spill_hp_here:
                    pr, p, op = _pick_hp(s, assigned_pipeline_ids, assigned_op_ids)

            if p is None:
                break

            # Size request and fit to available resources (shrink CPU first, then RAM if needed)
            cpu_req, ram_req = _size_request(s, pool, pr, op, hp_backlog)

            # Cap to currently available
            cpu_req = min(cpu_req, avail_cpu)
            ram_req = min(ram_req, avail_ram)

            # Ensure minimums
            cpu_req = max(1.0, cpu_req)
            ram_req = max(1.0, ram_req)

            # If it still doesn't fit due to headroom thresholding, stop
            if cpu_req > avail_cpu or ram_req > avail_ram:
                break

            assignments.append(
                Assignment(
                    ops=[op],
                    cpu=cpu_req,
                    ram=ram_req,
                    priority=pr,
                    pool_id=pool_id,
                    pipeline_id=p.pipeline_id,
                )
            )

            assigned_pipeline_ids.add(p.pipeline_id)
            assigned_op_ids.add(_op_id(op))

            avail_cpu -= cpu_req
            avail_ram -= ram_req
            planned += 1

    return suspensions, assignments