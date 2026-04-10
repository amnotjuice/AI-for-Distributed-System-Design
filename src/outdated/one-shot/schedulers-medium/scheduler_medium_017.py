# policy_key: scheduler_medium_017
# reasoning_effort: medium
# model: gpt-5.2-2025-12-11
# llm_cost: 0.062218
# generation_seconds: 94.92
# generated_at: 2026-03-12T22:17:12.260273
@register_scheduler_init(key="scheduler_medium_017")
def scheduler_medium_017_init(s):
    """Priority-aware, latency-oriented scheduler (incremental improvement over naive FIFO).

    Key ideas (kept intentionally simple / "small improvements first"):
      1) Maintain per-priority queues and always prefer higher priorities (QUERY/INTERACTIVE).
      2) Avoid "one giant assignment monopolizes the pool" by right-sizing CPU/RAM per op
         (instead of consuming all available resources).
      3) Add a small reserved headroom per pool for high-priority work to reduce tail latency.
      4) Retry OOM failures with exponential RAM backoff (but do not retry non-OOM failures).
      5) Add a tiny anti-starvation mechanism: after serving N high-priority ops, allow 1 batch op.
    """
    from collections import deque

    # Pipeline tracking
    s.pipelines_by_id = {}         # pipeline_id -> Pipeline
    s.enqueued_pipeline_ids = set()  # pipeline_id currently present in exactly one priority queue

    # Per-priority queues (round-robin within each priority)
    s.q_query = deque()
    s.q_interactive = deque()
    s.q_batch = deque()

    # Per-operator resource guesses (learned via outcomes)
    # Keyed by (pipeline_id, op_key)
    s.op_ram_guess = {}  # float
    s.op_cpu_guess = {}  # float

    # Pipelines that encountered a non-OOM failure (do not retry)
    s.dead_pipelines = set()

    # Simple anti-starvation knobs
    s.hp_budget = 6   # serve this many high-priority ops before allowing one batch op (if pending)
    s.hp_served = 0


def _prio_rank(priority):
    # Smaller is higher priority.
    if priority == Priority.QUERY:
        return 0
    if priority == Priority.INTERACTIVE:
        return 1
    return 2  # Priority.BATCH_PIPELINE


def _queue_for_priority(s, priority):
    if priority == Priority.QUERY:
        return s.q_query
    if priority == Priority.INTERACTIVE:
        return s.q_interactive
    return s.q_batch


def _is_oom_error(err):
    if not err:
        return False
    msg = str(err).lower()
    return ("oom" in msg) or ("out of memory" in msg) or ("outofmemory" in msg) or ("memoryerror" in msg)


def _op_key(op):
    # Try to use a stable identifier if present; fallback to repr.
    k = getattr(op, "op_id", None)
    if k is None:
        k = getattr(op, "operator_id", None)
    if k is None:
        k = repr(op)
    return k


def _default_cpu_for(priority, pool):
    # Favor latency for high priorities; keep batch conservative to improve overall responsiveness.
    max_cpu = float(pool.max_cpu_pool)
    if priority == Priority.QUERY:
        return max(1.0, min(4.0, max_cpu * 0.50))
    if priority == Priority.INTERACTIVE:
        return max(1.0, min(3.0, max_cpu * 0.35))
    return max(1.0, min(2.0, max_cpu * 0.20))


def _default_ram_for(priority, pool):
    # Start with moderate RAM to reduce OOM churn; retry on OOM will increase as needed.
    max_ram = float(pool.max_ram_pool)
    if priority == Priority.QUERY:
        return max(1.0, max_ram * 0.30)
    if priority == Priority.INTERACTIVE:
        return max(1.0, max_ram * 0.25)
    return max(1.0, max_ram * 0.20)


def _reserved_headroom(pool):
    # Keep a small portion of each pool available for high-priority arrivals.
    # (Used only when there is pending high-priority work.)
    return (float(pool.max_cpu_pool) * 0.20, float(pool.max_ram_pool) * 0.20)


def _has_ready_op(pipeline, planned_pipelines, planned_ops):
    """Return a single ready op (or None). Avoid scheduling >1 op per pipeline per tick."""
    if pipeline.pipeline_id in planned_pipelines:
        return None

    status = pipeline.runtime_status()
    if status.is_pipeline_successful():
        return None

    # If previously marked dead due to non-OOM failure, skip.
    # (We can't reliably map failures to specific ops via status alone, so track per pipeline.)
    # Note: This "dead" mark is set from ExecutionResult processing.
    # The pipeline may still show FAILED ops; we won't retry those here.
    # Caller handles dead pipeline removal.
    # (We still allow OOM-failed ops to be retried; they won't be marked dead.)
    op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
    if not op_list:
        return None

    # Choose the first op that hasn't already been planned this tick.
    for op in op_list:
        ok = (pipeline.pipeline_id, _op_key(op))
        if ok in planned_ops:
            continue
        return op
    return None


def _pop_next_pipeline_with_ready_op(queue, planned_pipelines, planned_ops, dead_pipelines):
    """Round-robin through a queue to find one pipeline with a ready op."""
    n = len(queue)
    for _ in range(n):
        p = queue.popleft()

        # Drop pipelines that are known dead.
        if p.pipeline_id in dead_pipelines:
            continue

        status = p.runtime_status()
        if status.is_pipeline_successful():
            continue

        op = _has_ready_op(p, planned_pipelines, planned_ops)
        # Always requeue unless completed/dead; we want pipelines to persist across ticks.
        queue.append(p)
        if op is not None:
            return p, op
    return None, None


def _any_high_priority_pending(s):
    # Lightweight check: scan queues for *any* ready op (bounded by queue length each).
    planned_pipelines = set()
    planned_ops = set()

    for q in (s.q_query, s.q_interactive):
        n = len(q)
        for _ in range(n):
            p = q.popleft()
            # Keep queue order stable (rotate)
            q.append(p)

            if p.pipeline_id in s.dead_pipelines:
                continue
            st = p.runtime_status()
            if st.is_pipeline_successful():
                continue
            if _has_ready_op(p, planned_pipelines, planned_ops) is not None:
                return True
    return False


def _any_batch_pending(s):
    planned_pipelines = set()
    planned_ops = set()
    q = s.q_batch
    n = len(q)
    for _ in range(n):
        p = q.popleft()
        q.append(p)
        if p.pipeline_id in s.dead_pipelines:
            continue
        st = p.runtime_status()
        if st.is_pipeline_successful():
            continue
        if _has_ready_op(p, planned_pipelines, planned_ops) is not None:
            return True
    return False


def _choose_next_priority(s, any_high_pending, any_batch_pending):
    # Serve high priorities first for latency, but allow a batch op occasionally to avoid starvation.
    if any_high_pending:
        if (s.hp_served >= s.hp_budget) and any_batch_pending:
            return Priority.BATCH_PIPELINE
        # Prefer QUERY over INTERACTIVE when both exist.
        # If chosen queue has no ready work, caller will fall back.
        return Priority.QUERY
    return Priority.BATCH_PIPELINE


@register_scheduler(key="scheduler_medium_017")
def scheduler_medium_017(s, results: List[ExecutionResult], pipelines: List[Pipeline]) -> Tuple[List[Suspend], List[Assignment]]:
    """
    Priority-aware scheduling with modest per-op sizing + OOM backoff + headroom reservation.

    Notes / limitations (intentional for robustness):
      - No preemption (needs reliable access to running container IDs; not assumed here).
      - Schedules at most one operator per pipeline per tick to avoid duplicate assignment races.
      - Uses conservative learned guesses keyed by (pipeline_id, op_id-ish).
    """
    # 1) Ingest new pipelines
    for p in pipelines:
        s.pipelines_by_id[p.pipeline_id] = p
        if p.pipeline_id not in s.enqueued_pipeline_ids:
            _queue_for_priority(s, p.priority).append(p)
            s.enqueued_pipeline_ids.add(p.pipeline_id)

    # 2) Learn from previous tick results (OOM backoff, non-OOM failure quarantine)
    for r in results:
        # Some results may refer to pipelines we didn't record (defensive)
        pool = s.executor.pools[r.pool_id]
        prio = r.priority

        # Update guesses per op in the result
        for op in (r.ops or []):
            k = (getattr(r, "pipeline_id", None), _op_key(op))
            # If ExecutionResult doesn't carry pipeline_id, we cannot key it well; fallback to None.
            # This still improves retries within the same tick/pool for similar ops less reliably.
            if k[0] is None:
                k = ("_unknown_pipeline_", _op_key(op))

            if r.failed():
                if _is_oom_error(r.error):
                    # Exponential RAM backoff, capped by pool max.
                    prev = float(s.op_ram_guess.get(k, max(1.0, float(r.ram) if r.ram else _default_ram_for(prio, pool))))
                    base = float(r.ram) if r.ram else prev
                    bumped = max(prev, base) * 2.0
                    cap = float(pool.max_ram_pool)
                    s.op_ram_guess[k] = min(bumped, cap)

                    # If we've already effectively hit the pool cap and still OOM, stop retrying.
                    if (float(r.ram) if r.ram else prev) >= 0.95 * cap:
                        # We can't reliably map back to a pipeline_id here unless present.
                        pid = getattr(r, "pipeline_id", None)
                        if pid is not None:
                            s.dead_pipelines.add(pid)
                else:
                    # Non-OOM failures: don't churn; mark pipeline dead if we can identify it.
                    pid = getattr(r, "pipeline_id", None)
                    if pid is not None:
                        s.dead_pipelines.add(pid)
            else:
                # Success: keep RAM guess (optionally tighten slightly to improve packing).
                # (Stay conservative: don't drop too aggressively; just cap at observed.)
                if r.ram:
                    observed = float(r.ram)
                    prev = float(s.op_ram_guess.get(k, observed))
                    s.op_ram_guess[k] = max(1.0, min(prev, observed * 1.10))

                if r.cpu:
                    observed_cpu = float(r.cpu)
                    prev_cpu = float(s.op_cpu_guess.get(k, observed_cpu))
                    # Keep within a reasonable band; avoid ramping up based on one run.
                    s.op_cpu_guess[k] = max(1.0, min(prev_cpu, observed_cpu * 1.10))

    # Early exit if nothing to do
    if not pipelines and not results:
        return [], []

    suspensions: List[Suspend] = []
    assignments: List[Assignment] = []

    any_high_pending = _any_high_priority_pending(s)
    any_batch_pending = _any_batch_pending(s)

    planned_pipelines = set()
    planned_ops = set()

    # 3) Per-pool scheduling loop
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)
        if avail_cpu <= 0.0 or avail_ram <= 0.0:
            continue

        # Effective batch headroom: keep some resources free if high-priority work exists.
        reserve_cpu, reserve_ram = _reserved_headroom(pool) if any_high_pending else (0.0, 0.0)

        # Try to place multiple small-to-moderate ops per pool per tick, respecting headroom.
        # Bound the inner loop to avoid long scans.
        for _ in range(64):
            if avail_cpu <= 0.0 or avail_ram <= 0.0:
                break

            # Determine effective available resources for the next choice
            choice = _choose_next_priority(s, any_high_pending, any_batch_pending)

            # If choosing batch while high-priority pending, enforce reserved headroom
            eff_cpu = avail_cpu
            eff_ram = avail_ram
            if any_high_pending and choice == Priority.BATCH_PIPELINE:
                eff_cpu = max(0.0, avail_cpu - reserve_cpu)
                eff_ram = max(0.0, avail_ram - reserve_ram)
                if eff_cpu <= 0.0 or eff_ram <= 0.0:
                    break

            # Choose from queues in priority order, with fallback if the chosen queue has no ready op.
            # Primary order: chosen -> next priority -> batch
            prio_order = []
            if choice == Priority.QUERY:
                prio_order = [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]
            elif choice == Priority.INTERACTIVE:
                prio_order = [Priority.INTERACTIVE, Priority.QUERY, Priority.BATCH_PIPELINE]
            else:
                prio_order = [Priority.BATCH_PIPELINE, Priority.INTERACTIVE, Priority.QUERY]

            picked_pipeline = None
            picked_op = None
            picked_priority = None

            for pr in prio_order:
                q = _queue_for_priority(s, pr)
                p, op = _pop_next_pipeline_with_ready_op(q, planned_pipelines, planned_ops, s.dead_pipelines)
                if p is not None and op is not None:
                    picked_pipeline, picked_op, picked_priority = p, op, pr
                    break

            if picked_pipeline is None:
                break

            # 4) Size the assignment using learned guesses (or defaults), then clamp to available
            ok = (picked_pipeline.pipeline_id, _op_key(picked_op))
            cpu_guess = float(s.op_cpu_guess.get(ok, _default_cpu_for(picked_pipeline.priority, pool)))
            ram_guess = float(s.op_ram_guess.get(ok, _default_ram_for(picked_pipeline.priority, pool)))

            # If we are placing batch with reserved headroom, respect effective budget
            cpu_to_use = min(cpu_guess, eff_cpu)
            ram_to_use = min(ram_guess, eff_ram)

            # If we cannot fit this op now, stop trying on this pool this tick.
            # (Avoid busy looping; it'll fit in a later tick or another pool.)
            if cpu_to_use <= 0.0 or ram_to_use <= 0.0:
                break

            # Record planning to avoid duplicate assignment within this tick
            planned_pipelines.add(picked_pipeline.pipeline_id)
            planned_ops.add(ok)

            assignments.append(
                Assignment(
                    ops=[picked_op],
                    cpu=cpu_to_use,
                    ram=ram_to_use,
                    priority=picked_pipeline.priority,
                    pool_id=pool_id,
                    pipeline_id=picked_pipeline.pipeline_id,
                )
            )

            # Update local pool availability
            avail_cpu -= cpu_to_use
            avail_ram -= ram_to_use

            # Update anti-starvation counter
            if picked_pipeline.priority in (Priority.QUERY, Priority.INTERACTIVE):
                s.hp_served += 1
            else:
                s.hp_served = 0

            # Refresh pending flags occasionally (cheap enough at low frequency)
            if (len(assignments) % 8) == 0:
                any_high_pending = _any_high_priority_pending(s)
                any_batch_pending = _any_batch_pending(s)

    return suspensions, assignments
