# policy_key: scheduler_medium_017
# reasoning_effort: medium
# exp: reasoning
# model: gpt-5.2-2025-12-11
# llm_cost: 0.097941
# generation_seconds: 83.83
# generated_at: 2026-04-09T22:52:20.345263
@register_scheduler_init(key="scheduler_medium_017")
def scheduler_medium_017_init(s):
    """
    Priority-aware, OOM-resilient scheduler.

    Core ideas:
    - Separate queues per priority (query > interactive > batch) with round-robin within each class.
    - Conservative headroom reservation: when high-priority work is waiting, batch is limited to a fraction
      of each pool's available CPU/RAM to reduce tail-latency for queries/interactive.
    - Failure-driven RAM backoff: if an operator fails with an OOM-like error, retry it with higher RAM
      (multiplicative backoff, capped). This targets high completion rate to avoid the 720s penalty.
    - Fill each pool greedily in a single tick using local resource accounting (decrementing avail_cpu/ram).
    """
    from collections import deque

    s.tick = 0

    # Priority queues hold (pipeline, arrival_tick)
    s.q_query = deque()
    s.q_interactive = deque()
    s.q_batch = deque()

    # Per-operator retry state (keyed by (pipeline_id, op_key))
    s.op_ram_mult = {}       # multiplier applied to a base RAM target
    s.op_fail_count = {}     # total failures observed
    s.op_oom_count = {}      # OOM-like failures observed

    # Small amount of batch "credit" to avoid total starvation under steady high-priority load.
    s.batch_credit = 0


def _priority_bucket(priority):
    if priority == Priority.QUERY:
        return "query"
    if priority == Priority.INTERACTIVE:
        return "interactive"
    return "batch"


def _is_oom_error(err) -> bool:
    if not err:
        return False
    e = str(err).lower()
    return ("oom" in e) or ("out of memory" in e) or ("memory" in e and ("killed" in e or "kill" in e))


def _op_key(op):
    # Best-effort stable key: prefer explicit ids, fallback to name/str, then object id.
    k = getattr(op, "op_id", None)
    if k is not None:
        return ("op_id", k)
    k = getattr(op, "operator_id", None)
    if k is not None:
        return ("operator_id", k)
    k = getattr(op, "name", None)
    if k is not None:
        return ("name", k)
    try:
        s = str(op)
        if s:
            return ("str", s)
    except Exception:
        pass
    return ("id", id(op))


def _pipeline_done(pipeline) -> bool:
    try:
        return pipeline.runtime_status().is_pipeline_successful()
    except Exception:
        return False


def _ready_ops(pipeline, limit=1):
    status = pipeline.runtime_status()
    ops = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
    if not ops:
        return []
    return ops[:limit]


def _queue_len(q):
    try:
        return len(q)
    except Exception:
        return 0


def _queue_has_ready(q) -> bool:
    # Rotate through the queue once to check if any pipeline is immediately schedulable.
    n = _queue_len(q)
    for _ in range(n):
        pipeline, t = q[0]
        if _pipeline_done(pipeline):
            q.popleft()
            n -= 1
            if n <= 0:
                break
            continue
        if _ready_ops(pipeline, limit=1):
            return True
        q.rotate(-1)
    return False


def _pop_next_ready(q):
    # Pop the next pipeline that has at least one ready op; drop completed pipelines.
    n = _queue_len(q)
    for _ in range(n):
        pipeline, t = q.popleft()
        if _pipeline_done(pipeline):
            continue
        if _ready_ops(pipeline, limit=1):
            return pipeline, t
        q.append((pipeline, t))
    return None, None


def _targets_for_priority(priority, pool):
    # Targets are intentionally "chunky" to reduce latency for high priority and reduce OOMs.
    # (RAM beyond minimum doesn't speed up; but under-allocating risks OOM which is heavily penalized.)
    max_cpu = pool.max_cpu_pool
    max_ram = pool.max_ram_pool

    if priority == Priority.QUERY:
        cpu_t = max(1.0, 0.80 * max_cpu)
        ram_t = max(0.05 * max_ram, 0.55 * max_ram)
    elif priority == Priority.INTERACTIVE:
        cpu_t = max(1.0, 0.60 * max_cpu)
        ram_t = max(0.05 * max_ram, 0.45 * max_ram)
    else:
        cpu_t = max(1.0, 0.45 * max_cpu)
        ram_t = max(0.05 * max_ram, 0.30 * max_ram)

    return cpu_t, ram_t


def _apply_batch_reservation(avail_cpu, avail_ram, pool, high_prio_waiting):
    # When high-priority is waiting, keep headroom in each pool so queries/interactive can start quickly.
    if not high_prio_waiting:
        return avail_cpu, avail_ram
    reserve_cpu = 0.25 * pool.max_cpu_pool
    reserve_ram = 0.25 * pool.max_ram_pool
    return max(0.0, avail_cpu - reserve_cpu), max(0.0, avail_ram - reserve_ram)


@register_scheduler(key="scheduler_medium_017")
def scheduler_medium_017(s, results, pipelines):
    """
    Scheduler step:
    - Ingest new pipelines into per-priority queues.
    - Update per-operator RAM backoff based on failures (especially OOM).
    - For each pool (largest free resources first), greedily assign ready operators:
        * Prefer query, then interactive, then batch.
        * Limit batch resource usage when high-priority work is waiting (reservation).
        * Apply per-operator RAM multiplier after OOM to avoid repeated failures.
    """
    s.tick += 1

    # Enqueue new arrivals
    for p in pipelines:
        b = _priority_bucket(p.priority)
        if b == "query":
            s.q_query.append((p, s.tick))
        elif b == "interactive":
            s.q_interactive.append((p, s.tick))
        else:
            s.q_batch.append((p, s.tick))

    # Update backoff state from execution results
    for r in results:
        if not r.failed():
            # Optional: on success, gently decay RAM multiplier for involved ops.
            for op in getattr(r, "ops", []) or []:
                k = (getattr(r, "pipeline_id", None), _op_key(op))
                if k in s.op_ram_mult:
                    s.op_ram_mult[k] = max(1.0, s.op_ram_mult[k] * 0.95)
            continue

        err = getattr(r, "error", None)
        is_oom = _is_oom_error(err)

        for op in getattr(r, "ops", []) or []:
            # Prefer pipeline_id from result if present; otherwise fall back to None (still helps within run).
            pid = getattr(r, "pipeline_id", None)
            k = (pid, _op_key(op))

            s.op_fail_count[k] = s.op_fail_count.get(k, 0) + 1
            if is_oom:
                s.op_oom_count[k] = s.op_oom_count.get(k, 0) + 1
                cur = s.op_ram_mult.get(k, 1.0)
                # Multiplicative backoff tuned to converge quickly (avoid repeated 720s penalties), capped.
                s.op_ram_mult[k] = min(8.0, cur * 1.7)
            else:
                # Non-OOM failures: small bump to reduce chance of resource-related transient failures.
                cur = s.op_ram_mult.get(k, 1.0)
                s.op_ram_mult[k] = min(4.0, cur * 1.15)

    # If nothing changed, bail quickly
    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    # Determine whether high-priority work is waiting and schedulable (for batch reservation)
    high_prio_waiting = _queue_has_ready(s.q_query) or _queue_has_ready(s.q_interactive)

    # Batch credit: accrue when high-priority is waiting; spend to occasionally allow a batch start anyway.
    # This mitigates indefinite starvation without harming query/interactive much.
    if high_prio_waiting:
        s.batch_credit = min(3, s.batch_credit + 1)
    else:
        s.batch_credit = max(0, s.batch_credit - 1)

    # Schedule pools in order of most available resources to reduce fragmentation
    pool_ids = list(range(s.executor.num_pools))
    pool_ids.sort(
        key=lambda i: (s.executor.pools[i].avail_cpu_pool, s.executor.pools[i].avail_ram_pool),
        reverse=True,
    )

    for pool_id in pool_ids:
        pool = s.executor.pools[pool_id]
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)

        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # Greedily fill the pool using local accounting
        while avail_cpu > 0 and avail_ram > 0:
            # Choose which queue to draw from
            chosen = None

            # Always prefer query / interactive when ready
            if _queue_has_ready(s.q_query):
                chosen = "query"
            elif _queue_has_ready(s.q_interactive):
                chosen = "interactive"
            else:
                chosen = "batch"

            # If high-priority is waiting, still allow occasional batch starts (using credit),
            # but only after we couldn't find any ready high-priority ops for this iteration.
            if high_prio_waiting and chosen == "batch" and s.batch_credit > 0:
                # Allow one batch operator to be scheduled even during high-priority pressure.
                pass

            if chosen == "query":
                q = s.q_query
                pr = Priority.QUERY
            elif chosen == "interactive":
                q = s.q_interactive
                pr = Priority.INTERACTIVE
            else:
                q = s.q_batch
                pr = Priority.BATCH_PIPELINE

            pipeline, _t = _pop_next_ready(q)
            if pipeline is None:
                # No ready work for this chosen class; if we picked batch but high priority exists,
                # try again for high priority, else break.
                if chosen == "batch" and high_prio_waiting:
                    # Retry: pick high priority if any are ready now.
                    if _queue_has_ready(s.q_query):
                        q = s.q_query
                        pr = Priority.QUERY
                    elif _queue_has_ready(s.q_interactive):
                        q = s.q_interactive
                        pr = Priority.INTERACTIVE
                    pipeline, _t = _pop_next_ready(q)
                if pipeline is None:
                    break

            # Determine effective availability (batch reservation when high priority is waiting)
            eff_cpu, eff_ram = avail_cpu, avail_ram
            if pr == Priority.BATCH_PIPELINE:
                eff_cpu, eff_ram = _apply_batch_reservation(avail_cpu, avail_ram, pool, high_prio_waiting)

            # Pick one ready operator
            ops = _ready_ops(pipeline, limit=1)
            if not ops:
                # Not schedulable right now; requeue and move on.
                q.append((pipeline, _t if _t is not None else s.tick))
                continue

            op = ops[0]

            # Base targets by priority
            cpu_t, ram_t = _targets_for_priority(pr, pool)

            # Apply per-operator backoff based on OOM history
            pid = getattr(pipeline, "pipeline_id", None)
            mult = s.op_ram_mult.get((pid, _op_key(op)), 1.0)
            ram_t = ram_t * mult

            # Fit to effective availability; keep minimums so we don't schedule obviously-doomed tasks.
            cpu_req = min(eff_cpu, cpu_t)
            ram_req = min(eff_ram, ram_t)

            # Ensure some minimal allocation
            if cpu_req < 1.0:
                # Can't run any container meaningfully
                q.append((pipeline, _t if _t is not None else s.tick))
                break

            min_ram = max(0.02 * pool.max_ram_pool, 1e-6)
            if ram_req < min_ram:
                q.append((pipeline, _t if _t is not None else s.tick))
                break

            # Commit assignment
            assignment = Assignment(
                ops=[op],
                cpu=cpu_req,
                ram=ram_req,
                priority=pipeline.priority,
                pool_id=pool_id,
                pipeline_id=pipeline.pipeline_id,
            )
            assignments.append(assignment)

            # Local accounting: assume assignment consumes requested resources
            avail_cpu -= cpu_req
            avail_ram -= ram_req

            # Requeue the pipeline for subsequent operators
            q.append((pipeline, _t if _t is not None else s.tick))

            # Spend batch credit when we successfully scheduled batch while high-priority is waiting
            if high_prio_waiting and pr == Priority.BATCH_PIPELINE and s.batch_credit > 0:
                s.batch_credit -= 1

    return suspensions, assignments
