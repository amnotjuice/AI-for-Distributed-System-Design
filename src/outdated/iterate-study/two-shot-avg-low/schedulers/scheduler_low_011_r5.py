# source: scheduler_low_011.py
# model: gpt-5.2-2025-12-11
# effort: low
# feedback: minimal
# iteration: r5
@register_scheduler_init(key="scheduler_low_011_r5")
def scheduler_low_011_r5_init(s):
    """Priority-first, work-conserving scheduler with small but meaningful latency optimizations.

    Incremental improvements over the prior version:
    1) True priority queues (QUERY > INTERACTIVE > BATCH), but now fill pools with multiple small assignments per tick
       to increase concurrency and reduce mean completion time.
    2) Work-conserving reservations: keep headroom on the "interactive" pool for high-priority work *only when* there
       is ready high-priority work (or likely-to-be-ready soon). This reduces tail latency without needless idling.
    3) OOM-aware retry: do not drop pipelines solely because they contain FAILED ops; retry FAILED ops when we observed
       an OOM-like error for that op, with exponential RAM backoff and a retry budget.
    4) Better pool preference: schedule high-priority on the interactive pool first, then spill over to other pools.
    """
    # Use deque for efficient FIFO behavior
    from collections import deque

    s.queues = {
        Priority.QUERY: deque(),
        Priority.INTERACTIVE: deque(),
        Priority.BATCH_PIPELINE: deque(),
    }

    # Learned per-operator resource hints keyed by op object identity (id(op))
    # value: {"ram": float, "cpu": float}
    s.op_hints = {}

    # Retry accounting and retryability after OOM-like failures
    s.op_attempts = {}          # id(op) -> int
    s.op_oom_retryable = set()  # set(id(op)) of ops that are allowed to be retried (observed OOM)

    # Conservative knobs
    s.max_retries_per_op = 3

    # Target sizing fractions (of pool max). Batch is intentionally smaller for concurrency.
    s.target_fracs = {
        Priority.QUERY: {"cpu": 0.75, "ram": 0.70},
        Priority.INTERACTIVE: {"cpu": 0.75, "ram": 0.70},
        Priority.BATCH_PIPELINE: {"cpu": 0.25, "ram": 0.40},
    }

    # Simple per-tick limits to avoid creating too many containers at once
    s.max_assignments_per_pool_per_tick = 8

    # Pool preference: if multiple pools, treat pool 0 as interactive by default
    s.interactive_pool_id = 0

    # Headroom reservations (fraction of pool max) to protect latency on interactive pool
    # These are *work-conserving* and only apply when high-priority work is ready/likely-ready.
    s.reserve_fracs_pool0 = {"cpu": 0.35, "ram": 0.35}
    s.reserve_fracs_other = {"cpu": 0.15, "ram": 0.15}

    # How many queued high-priority pipelines to probe for readiness when deciding whether to reserve
    s.ready_probe = 6


def _prio_order():
    return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]


def _is_oom_error(err):
    if err is None:
        return False
    msg = str(err).lower()
    return ("oom" in msg) or ("out of memory" in msg) or ("memoryerror" in msg) or ("std::bad_alloc" in msg)


def _pool_order_for_priority(s, priority):
    n = s.executor.num_pools
    if n <= 1:
        return [0] if n == 1 else []
    ip = s.interactive_pool_id
    others = [i for i in range(n) if i != ip]
    if priority in (Priority.QUERY, Priority.INTERACTIVE):
        return [ip] + others
    return others + [ip]


def _pipeline_is_terminal_or_unretryable(s, pipeline):
    """Return True if pipeline should be dropped (successful, or contains non-retryable failures)."""
    status = pipeline.runtime_status()
    if status.is_pipeline_successful():
        return True

    # If there are FAILED ops, only keep the pipeline if every failed op is marked OOM-retryable
    # and still within retry budget.
    failed_ops = status.get_ops([OperatorState.FAILED], require_parents_complete=False)
    if failed_ops:
        for op in failed_ops:
            oid = id(op)
            if oid not in s.op_oom_retryable:
                return True
            if int(s.op_attempts.get(oid, 0)) > int(s.max_retries_per_op):
                return True
    return False


def _pop_next_ready(s, priority, scheduled_pipeline_ids):
    """Pop the next pipeline with a ready op (parents complete), respecting FIFO and per-tick anti-hogging."""
    q = s.queues[priority]
    if not q:
        return None, None

    # Bounded rotation: try each current element at most once
    for _ in range(len(q)):
        p = q.popleft()

        if _pipeline_is_terminal_or_unretryable(s, p):
            continue

        # Avoid scheduling multiple ops from the same pipeline in the same tick
        if p.pipeline_id in scheduled_pipeline_ids:
            q.append(p)
            continue

        status = p.runtime_status()
        ops = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        if not ops:
            q.append(p)
            continue

        # Found a schedulable op
        return p, ops[0]

    return None, None


def _has_ready_high_priority(s):
    """Heuristic: probe a few items in high-priority queues to decide if reserving headroom is necessary."""
    # If there are no HP pipelines at all, no need to reserve.
    if not s.queues[Priority.QUERY] and not s.queues[Priority.INTERACTIVE]:
        return False

    # Probe without permanently reordering: rotate and restore.
    for pr in (Priority.QUERY, Priority.INTERACTIVE):
        q = s.queues[pr]
        if not q:
            continue
        k = min(len(q), int(s.ready_probe))
        for _ in range(k):
            p = q.popleft()
            try:
                if _pipeline_is_terminal_or_unretryable(s, p):
                    continue
                st = p.runtime_status()
                ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
                if ops:
                    return True
            finally:
                # Restore FIFO order as best-effort (append back)
                q.append(p)

    # If nothing looked immediately ready, do not reserve aggressively (work-conserving choice).
    return False


def _request_resources(s, pool, priority, op, cpu_left, ram_left):
    """Compute a CPU/RAM request for one operator, applying hints and caps."""
    fr = s.target_fracs.get(priority, {"cpu": 0.5, "ram": 0.5})
    # Base targets based on pool max (not current avail), then cap to remaining headroom.
    base_cpu = max(1.0, float(pool.max_cpu_pool) * float(fr["cpu"]))
    base_ram = max(1.0, float(pool.max_ram_pool) * float(fr["ram"]))

    oid = id(op)
    hint = s.op_hints.get(oid)
    if hint:
        # Hints are treated as a floor to avoid repeating OOMs.
        base_cpu = max(base_cpu, float(hint.get("cpu", base_cpu)))
        base_ram = max(base_ram, float(hint.get("ram", base_ram)))

    cpu = min(base_cpu, float(cpu_left), float(pool.max_cpu_pool))
    ram = min(base_ram, float(ram_left), float(pool.max_ram_pool))

    # Must be positive to schedule
    cpu = max(0.0, cpu)
    ram = max(0.0, ram)
    return cpu, ram


@register_scheduler(key="scheduler_low_011_r5")
def scheduler_low_011_r5(s, results, pipelines):
    """
    Priority-first scheduler with:
    - Multi-assignment per pool per tick (controlled) for better concurrency,
    - Work-conserving headroom reservations for high-priority latency protection,
    - OOM-aware retry and RAM backoff keyed by operator identity.
    """
    # Enqueue new arrivals
    for p in pipelines:
        pr = p.priority if p.priority in s.queues else Priority.BATCH_PIPELINE
        s.queues[pr].append(p)

    # Learn from results (especially OOM) to enable retry + RAM backoff
    for r in results:
        ops = getattr(r, "ops", None) or []
        failed = False
        try:
            failed = bool(r.failed())
        except Exception:
            failed = getattr(r, "error", None) is not None

        if not failed:
            # On success, clear OOM-retryable marker for the op(s)
            for op in ops:
                s.op_oom_retryable.discard(id(op))
            continue

        # Failure case
        if _is_oom_error(getattr(r, "error", None)):
            observed_ram = float(getattr(r, "ram", 0.0) or 0.0)
            observed_cpu = float(getattr(r, "cpu", 0.0) or 0.0)

            for op in ops:
                oid = id(op)
                # Increment attempts and mark retryable
                s.op_attempts[oid] = int(s.op_attempts.get(oid, 0)) + 1
                s.op_oom_retryable.add(oid)

                # RAM backoff: double last observed or hinted RAM (whichever is higher)
                prev = s.op_hints.get(oid, {})
                prev_ram = float(prev.get("ram", 0.0) or 0.0)
                baseline_ram = max(1.0, prev_ram, observed_ram if observed_ram > 0 else 1.0)
                new_ram = baseline_ram * 2.0

                prev_cpu = float(prev.get("cpu", 0.0) or 0.0)
                new_cpu = max(1.0, prev_cpu, observed_cpu if observed_cpu > 0 else 1.0)

                s.op_hints[oid] = {"ram": new_ram, "cpu": new_cpu}
        else:
            # Non-OOM failures are treated as non-retryable; prevent endless cycling.
            for op in ops:
                oid = id(op)
                s.op_oom_retryable.discard(oid)
                s.op_attempts[oid] = int(s.max_retries_per_op) + 1

    # Early exit
    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    # Snapshot pool capacities and track remaining headroom locally to avoid over-assigning in one tick.
    pool_left = {}
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        pool_left[pool_id] = {
            "cpu": float(pool.avail_cpu_pool),
            "ram": float(pool.avail_ram_pool),
        }

    # Per-tick anti-hogging: schedule at most one op per pipeline in this tick
    scheduled_pipeline_ids = set()

    # Decide whether to reserve headroom for high priority (work-conserving)
    reserve_for_hp = _has_ready_high_priority(s)

    # Phase 1: schedule high priority first (QUERY then INTERACTIVE), prefer interactive pool first
    for pr in (Priority.QUERY, Priority.INTERACTIVE):
        for pool_id in _pool_order_for_priority(s, pr):
            pool = s.executor.pools[pool_id]
            for _ in range(int(s.max_assignments_per_pool_per_tick)):
                cpu_left = pool_left[pool_id]["cpu"]
                ram_left = pool_left[pool_id]["ram"]
                if cpu_left < 1.0 or ram_left < 1.0:
                    break

                p, op = _pop_next_ready(s, pr, scheduled_pipeline_ids)
                if p is None:
                    break

                cpu_req, ram_req = _request_resources(s, pool, pr, op, cpu_left, ram_left)

                # If we can't fit minimal allocations, put pipeline back and stop trying this pool.
                if cpu_req < 1.0 or ram_req < 1.0:
                    s.queues[pr].append(p)
                    break

                # Commit assignment
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
                pool_left[pool_id]["cpu"] -= cpu_req
                pool_left[pool_id]["ram"] -= ram_req
                scheduled_pipeline_ids.add(p.pipeline_id)

                # Re-enqueue pipeline for subsequent operators
                s.queues[pr].append(p)

    # Phase 2: schedule batch, but preserve headroom if high-priority is ready
    pr = Priority.BATCH_PIPELINE
    for pool_id in _pool_order_for_priority(s, pr):
        pool = s.executor.pools[pool_id]

        # Compute reservation for this pool (only enforced when HP is ready)
        if reserve_for_hp:
            if s.executor.num_pools <= 1:
                rf = s.reserve_fracs_pool0
            else:
                rf = s.reserve_fracs_pool0 if pool_id == s.interactive_pool_id else s.reserve_fracs_other
            reserve_cpu = float(pool.max_cpu_pool) * float(rf["cpu"])
            reserve_ram = float(pool.max_ram_pool) * float(rf["ram"])
        else:
            reserve_cpu = 0.0
            reserve_ram = 0.0

        for _ in range(int(s.max_assignments_per_pool_per_tick)):
            cpu_left = pool_left[pool_id]["cpu"]
            ram_left = pool_left[pool_id]["ram"]

            # Enforce reservation (work-conserving): batch can only use headroom above reserved.
            eff_cpu = cpu_left - reserve_cpu
            eff_ram = ram_left - reserve_ram
            if eff_cpu < 1.0 or eff_ram < 1.0:
                break

            p, op = _pop_next_ready(s, pr, scheduled_pipeline_ids)
            if p is None:
                break

            cpu_req, ram_req = _request_resources(s, pool, pr, op, eff_cpu, eff_ram)

            # If hinted RAM exceeds effective headroom, do not schedule (avoid predictable OOM/churn).
            if cpu_req < 1.0 or ram_req < 1.0:
                s.queues[pr].append(p)
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
            pool_left[pool_id]["cpu"] -= cpu_req
            pool_left[pool_id]["ram"] -= ram_req
            scheduled_pipeline_ids.add(p.pipeline_id)

            # Re-enqueue pipeline for later ops
            s.queues[pr].append(p)

    return suspensions, assignments