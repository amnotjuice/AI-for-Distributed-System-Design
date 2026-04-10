# source: scheduler_low_011.py
# model: gpt-5.2-2025-12-11
# effort: low
# feedback: rich
# iteration: r17
@register_scheduler_init(key="scheduler_low_011_r17")
def scheduler_low_011_r17_init(s):
    """Priority-aware multi-assign scheduler (iteration 2): reduce queueing latency without starving classes.

    Key changes vs the previous policy:
    - Fix obvious underutilization: assign MULTIPLE operators per pool per tick (bounded), instead of 1.
    - Prevent starvation: use a simple weighted round-robin "cycle" per pool so INTERACTIVE/BATCH make progress.
    - Reduce wasteful oversizing: smaller default RAM fractions (RAM > minimum doesn't help), improving concurrency.
    - Keep headroom in the high-priority pool to protect QUERY/INTERACTIVE latency (no preemption needed).
    - Keep OOM-aware RAM backoff hints, keyed by operator identity (id(op)) so retries actually work.
    """
    # Per-priority FIFO queues implemented as list + head index (avoid pop(0) shifting costs without imports)
    s._q = {
        Priority.QUERY: {"items": [], "head": 0},
        Priority.INTERACTIVE: {"items": [], "head": 0},
        Priority.BATCH_PIPELINE: {"items": [], "head": 0},
    }
    # De-dup: pipeline_id currently enqueued per priority
    s._enqueued = {
        Priority.QUERY: set(),
        Priority.INTERACTIVE: set(),
        Priority.BATCH_PIPELINE: set(),
    }
    # pipeline_id -> Pipeline (latest object reference)
    s._pipelines = {}

    # OOM/backoff hints keyed by operator identity
    # key: id(op) -> {"ram": float, "cpu": float}
    s._op_hints = {}
    # key: id(op) -> int retries attempted
    s._op_attempts = {}
    s._max_retries_per_op = 3

    # Multi-assign bounds per pool per tick (kept modest to avoid churn)
    s._max_assignments_per_pool = 6
    s._max_scan_per_pool = 80  # bound queue scanning work per pool per tick

    # Inflight caps per pipeline (enables some parallelism for high-priority DAGs)
    s._max_inflight = {
        Priority.QUERY: 2,
        Priority.INTERACTIVE: 2,
        Priority.BATCH_PIPELINE: 1,
    }

    # Default sizing (fractions of pool max). Keep RAM smaller to improve packing.
    # CPU scaling is sublinear, so moderate shares often reduce overall queueing latency.
    s._size_fracs = {
        Priority.QUERY: {"cpu": 0.75, "ram": 0.30},
        Priority.INTERACTIVE: {"cpu": 0.60, "ram": 0.30},
        Priority.BATCH_PIPELINE: {"cpu": 0.40, "ram": 0.20},
    }

    # Pool role: pool 0 is treated as "hi-priority" pool when multiple pools exist
    s._hipri_pool_id = 0

    # Per-pool scheduling cycles (weighted RR). Initialized lazily in scheduler if pool count not known yet.
    s._pool_cycles = []
    s._pool_cycle_ptr = []


def _norm_prio(pr):
    if pr in (Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE):
        return pr
    return Priority.BATCH_PIPELINE


def _q_len(q):
    return max(0, len(q["items"]) - q["head"])


def _q_compact_if_needed(q):
    # Compact occasionally to avoid unbounded growth.
    head = q["head"]
    if head > 256 and head > (len(q["items"]) // 2):
        q["items"] = q["items"][head:]
        q["head"] = 0


def _q_pop(q):
    if q["head"] >= len(q["items"]):
        return None
    item = q["items"][q["head"]]
    q["head"] += 1
    _q_compact_if_needed(q)
    return item


def _q_push(q, item):
    q["items"].append(item)


def _enqueue_pipeline(s, p):
    pr = _norm_prio(getattr(p, "priority", Priority.BATCH_PIPELINE))
    pid = getattr(p, "pipeline_id", None)
    if pid is None:
        return
    s._pipelines[pid] = p
    if pid in s._enqueued[pr]:
        return
    _q_push(s._q[pr], pid)
    s._enqueued[pr].add(pid)


def _requeue_pipeline(s, p):
    # Requeue only if not already enqueued (caller should have removed on pop)
    _enqueue_pipeline(s, p)


def _drop_pipeline(s, p):
    # Best-effort removal from maps; if it still exists in queue, it will be skipped when popped.
    pid = getattr(p, "pipeline_id", None)
    if pid is None:
        return
    s._pipelines.pop(pid, None)
    for pr in (Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE):
        s._enqueued[pr].discard(pid)


def _pop_next_pipeline(s, pr):
    # Pops until a pipeline_id maps to an existing pipeline (skip stale ids).
    q = s._q[pr]
    for _ in range(32):  # small bound; stale ids are rare
        pid = _q_pop(q)
        if pid is None:
            return None
        s._enqueued[pr].discard(pid)  # popped => not enqueued anymore
        p = s._pipelines.get(pid)
        if p is None:
            continue
        return p
    return None


def _is_oom_error(err):
    if err is None:
        return False
    msg = str(err).lower()
    return ("oom" in msg) or ("out of memory" in msg) or ("memoryerror" in msg)


def _ensure_pool_cycles(s):
    # Initialize per-pool cycles/pointers if needed or if pool count changed.
    n = getattr(s.executor, "num_pools", 1)
    if len(s._pool_cycles) == n and len(s._pool_cycle_ptr) == n:
        return

    s._pool_cycles = []
    s._pool_cycle_ptr = [0 for _ in range(n)]

    for pool_id in range(n):
        if n > 1 and pool_id == s._hipri_pool_id:
            # Hi-priority pool: favor QUERY/INTERACTIVE but still give BATCH a small slice.
            s._pool_cycles.append(
                [Priority.QUERY, Priority.QUERY, Priority.INTERACTIVE, Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]
            )
        elif n > 1:
            # Background pools: favor BATCH, but allow INTERACTIVE and a tiny QUERY slice for overflow.
            s._pool_cycles.append(
                [Priority.BATCH_PIPELINE, Priority.BATCH_PIPELINE, Priority.INTERACTIVE, Priority.BATCH_PIPELINE, Priority.INTERACTIVE, Priority.QUERY]
            )
        else:
            # Single pool: balanced with priority preference.
            s._pool_cycles.append(
                [Priority.QUERY, Priority.INTERACTIVE, Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]
            )


def _pick_next_priority_for_pool(s, pool_id):
    cycle = s._pool_cycles[pool_id]
    if not cycle:
        return None

    # Try a full cycle to find a non-empty queue for that priority.
    for _ in range(len(cycle)):
        idx = s._pool_cycle_ptr[pool_id] % len(cycle)
        pr = cycle[idx]
        s._pool_cycle_ptr[pool_id] = (idx + 1) % len(cycle)
        if _q_len(s._q[pr]) > 0:
            return pr

    # If all queues empty, no work.
    return None


def _pipeline_inflight(status):
    # Count "inflight" ops to bound per-pipeline parallelism.
    return (
        int(status.state_counts.get(OperatorState.ASSIGNED, 0))
        + int(status.state_counts.get(OperatorState.RUNNING, 0))
        + int(status.state_counts.get(OperatorState.SUSPENDING, 0))
    )


def _has_unhandled_failure(status):
    # Any FAILED ops means pipeline has a failure; we only "handle" it if we can retry FAILED ops
    # (i.e., if there exists a FAILED op we have an OOM hint for and within retry budget).
    return int(status.state_counts.get(OperatorState.FAILED, 0)) > 0


def _retryable_failed_op(s, status):
    # Find a failed op that we can retry (OOM-hinted and within retry limit).
    # Note: ASSIGNABLE_STATES includes FAILED, but we still gate retries.
    ops = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
    for op in ops:
        # We don't know the state of each op directly; but if pipeline has failures,
        # this list can include FAILED ops. We allow retry only if we have a hint.
        k = id(op)
        if k in s._op_hints and int(s._op_attempts.get(k, 0)) <= s._max_retries_per_op:
            return op
    return None


def _next_assignable_op(s, status):
    # Prefer retrying a known OOM-failed op if present; otherwise take the first pending-ready op.
    if _has_unhandled_failure(status):
        op = _retryable_failed_op(s, status)
        return op
    ops = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
    if not ops:
        return None
    return ops[0]


def _reserved_headroom(s, pool, pool_id):
    # Keep headroom in the hi-priority pool when high-priority queues are non-empty.
    # This reduces p99 latency without preemption.
    if s.executor.num_pools <= 1:
        return 0.0, 0.0

    if pool_id != s._hipri_pool_id:
        # Minimal reservation in background pools.
        return 0.0, 0.0

    q_wait = _q_len(s._q[Priority.QUERY]) > 0
    i_wait = _q_len(s._q[Priority.INTERACTIVE]) > 0
    if not (q_wait or i_wait):
        return 0.0, 0.0

    # Reserve a portion of max resources so QUERY/INTERACTIVE can start quickly.
    reserve_cpu = max(1.0, float(pool.max_cpu_pool) * 0.25)
    reserve_ram = max(1.0, float(pool.max_ram_pool) * 0.25)
    return reserve_cpu, reserve_ram


def _compute_request(s, pool, pr, op, avail_cpu, avail_ram):
    # Base on pool max to keep sizing stable, then cap to current availability.
    fr = s._size_fracs.get(pr, {"cpu": 0.5, "ram": 0.25})

    # Small dynamic boost for QUERY when backlog is tiny: reduce per-op runtime.
    if pr == Priority.QUERY and _q_len(s._q[Priority.QUERY]) <= 2:
        cpu_frac = min(1.0, float(fr["cpu"]) + 0.20)
    else:
        cpu_frac = float(fr["cpu"])

    ram_frac = float(fr["ram"])

    cpu = max(1.0, float(pool.max_cpu_pool) * cpu_frac)
    ram = max(1.0, float(pool.max_ram_pool) * ram_frac)

    # Apply OOM/backoff hints if present
    k = id(op)
    hint = s._op_hints.get(k)
    if hint:
        ram = max(ram, float(hint.get("ram", ram)))
        cpu = max(cpu, float(hint.get("cpu", cpu)))

    # Cap to availability and pool max
    cpu = min(cpu, float(pool.max_cpu_pool), float(avail_cpu))
    ram = min(ram, float(pool.max_ram_pool), float(avail_ram))

    # Ensure still positive and feasible
    cpu = max(1.0, cpu)
    ram = max(1.0, ram)
    return cpu, ram


@register_scheduler(key="scheduler_low_011_r17")
def scheduler_low_011_r17(s, results, pipelines):
    """
    Priority-aware, fairness-preserving, multi-assignment scheduler.

    Goals:
    - Reduce latency by reducing queueing (fill pools with multiple smaller containers).
    - Preserve high-priority latency with headroom in the hi-priority pool.
    - Avoid starvation via simple weighted round-robin per pool.
    - Handle OOM by retrying failed ops with increased RAM.
    """
    _ensure_pool_cycles(s)

    # Enqueue new arrivals
    for p in pipelines:
        _enqueue_pipeline(s, p)

    # Learn from failures (only act on OOM-like errors)
    for r in results:
        failed = False
        try:
            failed = bool(r.failed())
        except Exception:
            failed = getattr(r, "error", None) is not None

        if not failed:
            continue

        if not _is_oom_error(getattr(r, "error", None)):
            continue

        pool_id = getattr(r, "pool_id", None)
        pool_max_ram = None
        if pool_id is not None and 0 <= int(pool_id) < s.executor.num_pools:
            pool_max_ram = float(s.executor.pools[int(pool_id)].max_ram_pool)

        ops = getattr(r, "ops", []) or []
        for op in ops:
            k = id(op)
            prev = s._op_hints.get(k, {})
            prev_ram = float(prev.get("ram", 0.0))
            prev_cpu = float(prev.get("cpu", 1.0))

            baseline_ram = float(getattr(r, "ram", 1.0) or 1.0)
            # RAM-first exponential backoff, capped by pool max if known
            new_ram = max(prev_ram, baseline_ram * 2.0, 1.0)
            if pool_max_ram is not None:
                new_ram = min(new_ram, pool_max_ram)

            s._op_hints[k] = {"ram": new_ram, "cpu": max(1.0, float(getattr(r, "cpu", prev_cpu) or prev_cpu))}
            s._op_attempts[k] = int(s._op_attempts.get(k, 0)) + 1

    # If no arrivals/results, we can still have work; but keep the early exit small/cheap.
    if not pipelines and not results:
        # Only exit if all queues empty
        if (
            _q_len(s._q[Priority.QUERY]) == 0
            and _q_len(s._q[Priority.INTERACTIVE]) == 0
            and _q_len(s._q[Priority.BATCH_PIPELINE]) == 0
        ):
            return [], []

    suspensions = []
    assignments = []

    # Schedule per pool
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)
        if avail_cpu < 1.0 or avail_ram < 1.0:
            continue

        reserve_cpu, reserve_ram = _reserved_headroom(s, pool, pool_id)

        made = 0
        scans = 0

        while made < s._max_assignments_per_pool and avail_cpu >= 1.0 and avail_ram >= 1.0 and scans < s._max_scan_per_pool:
            pr = _pick_next_priority_for_pool(s, pool_id)
            if pr is None:
                break

            p = _pop_next_pipeline(s, pr)
            scans += 1
            if p is None:
                continue

            status = p.runtime_status()

            # Drop completed pipelines
            if status.is_pipeline_successful():
                _drop_pipeline(s, p)
                continue

            # If pipeline has failures and none are retryable (OOM-hinted), drop it to avoid spinning forever.
            if _has_unhandled_failure(status) and _retryable_failed_op(s, status) is None:
                _drop_pipeline(s, p)
                continue

            # Per-pipeline inflight cap
            inflight_cap = int(s._max_inflight.get(_norm_prio(p.priority), 1))
            if _pipeline_inflight(status) >= inflight_cap:
                _requeue_pipeline(s, p)
                continue

            op = _next_assignable_op(s, status)
            if op is None:
                _requeue_pipeline(s, p)
                continue

            # Enforce headroom in the hi-priority pool by restricting BATCH when high-priority is waiting.
            if (
                s.executor.num_pools > 1
                and pool_id == s._hipri_pool_id
                and pr == Priority.BATCH_PIPELINE
                and (_q_len(s._q[Priority.QUERY]) > 0 or _q_len(s._q[Priority.INTERACTIVE]) > 0)
            ):
                eff_cpu = max(0.0, avail_cpu - reserve_cpu)
                eff_ram = max(0.0, avail_ram - reserve_ram)
                # If we can't fit a minimal batch container beyond the reserve, defer batch.
                if eff_cpu < 1.0 or eff_ram < 1.0:
                    _requeue_pipeline(s, p)
                    continue
                cpu, ram = _compute_request(s, pool, pr, op, eff_cpu, eff_ram)
            else:
                cpu, ram = _compute_request(s, pool, pr, op, avail_cpu, avail_ram)

            if cpu > avail_cpu or ram > avail_ram or cpu < 1.0 or ram < 1.0:
                # Can't fit now; requeue and try other work.
                _requeue_pipeline(s, p)
                continue

            assignments.append(
                Assignment(
                    ops=[op],
                    cpu=cpu,
                    ram=ram,
                    priority=_norm_prio(p.priority),
                    pool_id=pool_id,
                    pipeline_id=p.pipeline_id,
                )
            )

            avail_cpu -= cpu
            avail_ram -= ram
            made += 1

            # Keep pipeline in the system for subsequent operators
            _requeue_pipeline(s, p)

    return suspensions, assignments