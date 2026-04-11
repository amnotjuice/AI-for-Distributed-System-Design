# policy_key: scheduler_medium_003
# reasoning_effort: medium
# exp: reasoning
# model: gpt-5.2-2025-12-11
# llm_cost: 0.067197
# generation_seconds: 74.90
# generated_at: 2026-04-09T22:31:20.562829
@register_scheduler_init(key="scheduler_medium_003")
def scheduler_medium_003_init(s):
    """Priority-first, failure-averse scheduler.

    Main ideas:
      - Strict priority ordering (QUERY > INTERACTIVE > BATCH) to protect the score-dominant classes.
      - Reservation-based admission: keep a per-pool CPU/RAM safety margin for high-priority arrivals.
      - OOM-aware retries: on OOM, bump per-operator RAM hint and retry (instead of dropping), capped by a retry limit.
      - Gentle fairness: if the system has no high-priority backlog, allow batch to consume reservations; also age batches.
    """
    from collections import deque

    s.tick = 0

    # Per-priority FIFO queues of Pipeline objects
    s.queues = {
        Priority.QUERY: deque(),
        Priority.INTERACTIVE: deque(),
        Priority.BATCH_PIPELINE: deque(),
    }

    # Metadata keyed by pipeline_id
    s.pipeline_meta = {}  # pipeline_id -> {"arrival_tick": int, "last_enqueued_tick": int}

    # Per-operator hints to reduce OOMs; key includes pipeline_id when available
    s.op_ram_hint = {}  # (pipeline_id, op_key) -> ram
    s.op_oom_count = {}  # (pipeline_id, op_key) -> int
    s.op_fail_count = {}  # (pipeline_id, op_key) -> int (any failure)

    # Retry policy: prioritize completion (avoid 720s penalty), but cap churn
    s.max_oom_retries = 4
    s.max_other_retries = 1

    # Reservations protect QUERY/INTERACTIVE latency (dominates objective)
    s.reserve_cpu_frac = 0.35
    s.reserve_ram_frac = 0.35

    # Default resource sizing (conservative on RAM to avoid OOM; moderate CPU for latency)
    s.cpu_frac = {
        Priority.QUERY: 0.55,
        Priority.INTERACTIVE: 0.45,
        Priority.BATCH_PIPELINE: 0.35,
    }
    s.ram_frac = {
        Priority.QUERY: 0.65,
        Priority.INTERACTIVE: 0.60,
        Priority.BATCH_PIPELINE: 0.55,
    }

    # Minimum CPU slice we try to allocate
    s.min_cpu = 1.0

    # Batch aging: after waiting long enough and if high-priority backlog is low, let batches through
    s.batch_aging_threshold_ticks = 60


@register_scheduler(key="scheduler_medium_003")
def scheduler_medium_003(s, results: List["ExecutionResult"], pipelines: List["Pipeline"]) -> Tuple[List["Suspend"], List["Assignment"]]:
    from collections import deque

    def _op_key(op):
        # Stable identifier if available; else fall back to Python object id
        return (
            getattr(op, "op_id", None)
            or getattr(op, "operator_id", None)
            or getattr(op, "id", None)
            or id(op)
        )

    def _result_pipeline_id(res):
        return getattr(res, "pipeline_id", None)

    def _is_oom_error(err) -> bool:
        if err is None:
            return False
        msg = str(err).lower()
        return ("oom" in msg) or ("out of memory" in msg) or ("memoryerror" in msg)

    def _min_ram_from_op(op):
        # Best-effort extraction; if absent, return None and rely on pool fractions / hints.
        for attr in ("min_ram", "min_ram_gb", "min_ram_mb", "ram_min", "peak_ram", "peak_ram_gb", "peak_ram_mb"):
            if hasattr(op, attr):
                v = getattr(op, attr)
                try:
                    if v is None:
                        continue
                    return float(v)
                except Exception:
                    continue
        return None

    def _eligible_ops(p):
        st = p.runtime_status()
        return st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)

    def _pipeline_done_or_terminal(p):
        st = p.runtime_status()
        if st.is_pipeline_successful():
            return True
        # We do NOT treat FAILED as terminal (we retry), so only success is terminal here.
        return False

    def _enqueue_new_pipeline(p):
        pid = p.pipeline_id
        if pid not in s.pipeline_meta:
            s.pipeline_meta[pid] = {"arrival_tick": s.tick, "last_enqueued_tick": s.tick}
        else:
            s.pipeline_meta[pid]["last_enqueued_tick"] = s.tick
        s.queues[p.priority].append(p)

    def _has_high_backlog():
        # Query backlog first, then interactive. Backlog means "has at least one eligible op somewhere in queue".
        for pr in (Priority.QUERY, Priority.INTERACTIVE):
            q = s.queues[pr]
            # Peek a bounded number to keep overhead low
            n = min(len(q), 12)
            for i in range(n):
                p = q[i]
                if not _pipeline_done_or_terminal(p) and _eligible_ops(p):
                    return True
        return False

    def _oldest_batch_age():
        q = s.queues[Priority.BATCH_PIPELINE]
        if not q:
            return 0
        # Estimate using the front element's arrival tick
        p = q[0]
        meta = s.pipeline_meta.get(p.pipeline_id, None)
        if not meta:
            return 0
        return s.tick - meta["arrival_tick"]

    def _pick_next_pipeline(allow_batch: bool):
        # Strict priority selection with rotation to avoid head-of-line blocking.
        # Returns a pipeline or None.
        for pr in (Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE):
            if pr == Priority.BATCH_PIPELINE and not allow_batch:
                continue
            q = s.queues[pr]
            if not q:
                continue
            # Rotate through a bounded number of items to find an eligible pipeline
            limit = min(len(q), 24)
            picked = None
            for _ in range(limit):
                p = q.popleft()
                if _pipeline_done_or_terminal(p):
                    # Drop completed pipelines from queues
                    continue
                ops = _eligible_ops(p)
                if ops:
                    picked = p
                    break
                # Not ready yet; requeue to avoid blocking
                q.append(p)
            if picked is not None:
                return picked
        return None

    def _fits_with_reservation(pr, pool, cpu_need, ram_need, reserve_cpu, reserve_ram, high_backlog):
        # If there is high-priority backlog, enforce reservations for batch.
        if pr == Priority.BATCH_PIPELINE and high_backlog:
            # Must leave reserved headroom after this allocation
            return (pool.avail_cpu_pool - cpu_need) >= reserve_cpu and (pool.avail_ram_pool - ram_need) >= reserve_ram
        return True

    def _size_resources(p, op, pool):
        pr = p.priority
        # CPU sizing: moderate share for faster tail latency on queries; avoid taking the entire pool by default.
        target_cpu = max(s.min_cpu, s.cpu_frac.get(pr, 0.4) * pool.max_cpu_pool)
        # Keep some room for concurrent work unless pool is mostly free.
        cpu = min(pool.avail_cpu_pool, max(s.min_cpu, min(target_cpu, 0.80 * pool.max_cpu_pool)))

        # RAM sizing: conservative to avoid OOMs (which are highly penalized via 720s failures).
        base_ram = s.ram_frac.get(pr, 0.55) * pool.max_ram_pool

        # Best-effort operator minimum/peak hint if present
        op_min = _min_ram_from_op(op)
        if op_min is not None:
            # Treat op_min as a floor; stay conservative above it.
            base_ram = max(base_ram, float(op_min))

        pid = p.pipeline_id
        ok = _op_key(op)
        hint = s.op_ram_hint.get((pid, ok), None)
        if hint is not None:
            base_ram = max(base_ram, float(hint))

        # Clamp to what's available now and what's possible in the pool.
        ram = min(pool.avail_ram_pool, max(1.0, min(base_ram, pool.max_ram_pool)))

        # If we ended up with a tiny sliver of RAM due to fragmentation, prefer waiting rather than risking OOM.
        if ram < 0.10 * pool.max_ram_pool and pool.avail_ram_pool < 0.20 * pool.max_ram_pool:
            return None, None

        return cpu, ram

    # Advance time
    s.tick += 1

    # Incorporate new arrivals
    for p in pipelines:
        _enqueue_new_pipeline(p)

    # Update hints based on execution results (especially OOMs)
    for r in results:
        if not r.failed():
            continue
        pid = _result_pipeline_id(r)
        oom = _is_oom_error(getattr(r, "error", None))
        # If we can't identify pipeline_id, still update using pid=None; op objects should still be unique-ish.
        for op in getattr(r, "ops", []) or []:
            ok = _op_key(op)
            key = (pid, ok)
            s.op_fail_count[key] = s.op_fail_count.get(key, 0) + 1
            if oom:
                s.op_oom_count[key] = s.op_oom_count.get(key, 0) + 1
                # Aggressively bump RAM to avoid repeated OOMs; cap at pool max where possible later.
                prev = s.op_ram_hint.get(key, float(getattr(r, "ram", 0) or 0))
                if prev <= 0:
                    prev = float(getattr(r, "ram", 1.0) or 1.0)
                bumped = max(prev * 1.60, prev + 1.0)
                # Record bump; clamping to pool max happens at sizing time.
                s.op_ram_hint[key] = bumped

    # Early exit if nothing changed
    if not pipelines and not results:
        return [], []

    suspensions: List["Suspend"] = []
    assignments: List["Assignment"] = []

    # Determine whether to relax reservations for batch (no high backlog) and batch aging.
    high_backlog = _has_high_backlog()
    batch_age = _oldest_batch_age()
    allow_batch = (not high_backlog) or (batch_age >= s.batch_aging_threshold_ticks and not s.queues[Priority.QUERY])

    # Schedule per pool, attempting to fill while respecting reservations when needed.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]

        # Compute per-pool reservations (only enforced when high_backlog is True)
        reserve_cpu = s.reserve_cpu_frac * pool.max_cpu_pool
        reserve_ram = s.reserve_ram_frac * pool.max_ram_pool

        # Try to keep making assignments until we can't place more.
        # Use a bounded loop to avoid infinite rotation when queues are blocked.
        guard = 0
        while guard < 64:
            guard += 1

            if pool.avail_cpu_pool <= 0 or pool.avail_ram_pool <= 0:
                break

            p = _pick_next_pipeline(allow_batch=allow_batch)
            if p is None:
                break

            st = p.runtime_status()
            if st.is_pipeline_successful():
                continue

            ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
            if not ops:
                # Put back (not ready); avoid losing it
                s.queues[p.priority].append(p)
                continue

            op = ops[0]

            # Retry limiting: if an op has failed too many times, stop thrashing.
            pid = p.pipeline_id
            ok = _op_key(op)

            # Look up failure counts both with known pid and with pid=None (in case results lacked pipeline_id).
            fail_any = s.op_fail_count.get((pid, ok), 0) + s.op_fail_count.get((None, ok), 0)
            oom_cnt = s.op_oom_count.get((pid, ok), 0) + s.op_oom_count.get((None, ok), 0)

            if oom_cnt > s.max_oom_retries or (fail_any - oom_cnt) > s.max_other_retries:
                # Skip this pipeline for now; requeue to avoid deadlocking scheduler.
                # (Dropping isn't modeled here, but thrash would hurt latency for high-priority work.)
                s.queues[p.priority].append(p)
                continue

            cpu, ram = _size_resources(p, op, pool)
            if cpu is None or ram is None:
                # Can't safely size right now; requeue and stop trying to pack this pool this tick.
                s.queues[p.priority].append(p)
                break

            # Enforce reservations for batch when high backlog exists.
            if not _fits_with_reservation(p.priority, pool, cpu, ram, reserve_cpu, reserve_ram, high_backlog):
                # Requeue batch and stop filling this pool; keep headroom for high-priority.
                s.queues[p.priority].append(p)
                break

            # If this op has a strong RAM hint that exceeds current free RAM, wait rather than under-alloc and OOM.
            hint = s.op_ram_hint.get((pid, ok), None)
            if hint is None:
                hint = s.op_ram_hint.get((None, ok), None)
            if hint is not None and hint > pool.avail_ram_pool:
                s.queues[p.priority].append(p)
                break

            assignments.append(
                Assignment(
                    ops=[op],
                    cpu=cpu,
                    ram=ram,
                    priority=p.priority,
                    pool_id=pool_id,
                    pipeline_id=p.pipeline_id,
                )
            )

            # Optimistically decrement pool availability locally (scheduler may be called again before executor updates).
            pool.avail_cpu_pool -= cpu
            pool.avail_ram_pool -= ram

            # Requeue pipeline for additional ops later (keeps FIFO-ish behavior within priority).
            s.queues[p.priority].append(p)

    return suspensions, assignments
