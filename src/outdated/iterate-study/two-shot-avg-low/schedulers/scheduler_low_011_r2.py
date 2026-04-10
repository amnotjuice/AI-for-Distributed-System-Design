# source: scheduler_low_011.py
# model: gpt-5.2-2025-12-11
# effort: low
# feedback: minimal
# iteration: r2
@register_scheduler_init(key="scheduler_low_011_r2")
def scheduler_low_011_r2_init(s):
    """Priority-aware, queueing-latency-optimized scheduler (incremental step up from naive FIFO).

    Key changes vs naive / prior attempt:
    - Strict priority queues (QUERY > INTERACTIVE > BATCH) with per-priority round-robin.
    - Fill a pool with multiple assignments per tick (instead of at most one), to reduce queueing latency.
    - Smaller default sizing for high-priority ops (to increase concurrency); larger sizing for batch.
    - OOM-aware retry: if an op fails with OOM, increase its RAM hint and allow retry; non-OOM failures are not retried.
    - Keep a small reservation on the "interactive" pool when high-priority backlog exists, to reduce tail latency.
    - Limited scan (small lookahead) to pick a "smaller" (lower RAM-hint) ready op first within a priority,
      which tends to reduce mean latency under contention.
    """
    from collections import deque

    s.queues = {
        Priority.QUERY: deque(),
        Priority.INTERACTIVE: deque(),
        Priority.BATCH_PIPELINE: deque(),
    }
    # Avoid unbounded duplicates: track pipeline_ids currently enqueued per priority.
    s.queue_members = {
        Priority.QUERY: set(),
        Priority.INTERACTIVE: set(),
        Priority.BATCH_PIPELINE: set(),
    }

    # Per-operator hints keyed by operator object identity (id(op)).
    # We cannot reliably key by pipeline_id from results in this simulator API, so id(op) is the robust join key.
    s.op_hints = {}         # op_id -> {"ram": float, "cpu": float}
    s.op_attempts = {}      # op_id -> int
    s.oom_retriable_ops = set()     # op_ids that may be retried (OOM-style)
    s.nonretriable_failed_ops = set()  # op_ids that should not be retried (non-OOM)

    # Tunables (kept simple)
    s.max_retries_per_op = 4
    s.interactive_pool_id = 0

    # Reserve a small fraction of the interactive pool when high-priority backlog exists.
    s.interactive_reserve_frac_cpu = 0.20
    s.interactive_reserve_frac_ram = 0.20

    # Scan a few pipelines within a priority to find a schedulable/ready one (prevents head-of-line blocking).
    s.scan_limit = 8

    # Spillover: on non-interactive pools, allow only a few high-priority placements per tick
    # (keeps batch throughput while still reducing high-priority queueing).
    s.max_highpri_spill_per_pool_per_tick = 2


def _is_oom_error(err):
    if err is None:
        return False
    msg = str(err).lower()
    return ("oom" in msg) or ("out of memory" in msg) or ("memoryerror" in msg)


def _enqueue_pipeline(s, p):
    pr = p.priority
    if pr not in s.queues:
        pr = Priority.BATCH_PIPELINE
    pid = p.pipeline_id
    if pid in s.queue_members[pr]:
        return
    s.queues[pr].append(p)
    s.queue_members[pr].add(pid)


def _drop_pipeline_if_present(s, pr, pid):
    # We only track membership for de-dup; dropping is naturally handled by not re-enqueueing.
    if pr in s.queue_members and pid in s.queue_members[pr]:
        s.queue_members[pr].discard(pid)


def _pipeline_has_nonretriable_failure(s, pipeline):
    status = pipeline.runtime_status()
    # If there are FAILED ops that are known non-retriable, consider pipeline terminal.
    try:
        failed_ops = status.get_ops([OperatorState.FAILED], require_parents_complete=False)
    except Exception:
        failed_ops = []
    for op in failed_ops or []:
        if id(op) in s.nonretriable_failed_ops:
            return True
    return False


def _get_ready_assignable_op(pipeline):
    status = pipeline.runtime_status()
    ops = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
    if not ops:
        return None
    return ops[0]


def _highpri_backlog(s):
    return len(s.queues[Priority.QUERY]) + len(s.queues[Priority.INTERACTIVE])


def _base_request(s, pool, priority, highpri_backlog):
    # Smaller high-priority slices => more concurrency (lower queueing latency).
    # Batch prefers larger slices (throughput).
    max_cpu = float(pool.max_cpu_pool)
    max_ram = float(pool.max_ram_pool)

    if priority in (Priority.QUERY, Priority.INTERACTIVE):
        # If backlog is large, reduce per-op CPU so more can start immediately.
        cpu_cap = 4.0 if highpri_backlog <= 4 else 2.0
        cpu = min(cpu_cap, max(1.0, 0.25 * max_cpu))
        ram = max(1.0, 0.25 * max_ram)
        # Avoid giving half the machine by default to high-pri (hurts concurrency).
        ram = min(ram, 0.50 * max_ram)
        return cpu, ram

    # Batch
    cpu = max(1.0, 0.80 * max_cpu)
    ram = max(1.0, 0.80 * max_ram)
    return cpu, ram


def _apply_hints_and_cap(s, pool, eff_cpu, eff_ram, op, priority, highpri_backlog):
    base_cpu, base_ram = _base_request(s, pool, priority, highpri_backlog)

    oid = id(op)
    hint = s.op_hints.get(oid, None)

    cpu = base_cpu
    ram = base_ram

    if hint:
        # RAM hints are critical for OOM avoidance; CPU hints are best-effort.
        try:
            ram = max(ram, float(hint.get("ram", ram)))
        except Exception:
            pass
        try:
            cpu = max(cpu, float(hint.get("cpu", cpu)))
        except Exception:
            pass

    # Keep high-priority CPU bounded to preserve concurrency even if hints are large.
    if priority in (Priority.QUERY, Priority.INTERACTIVE):
        cpu = min(cpu, 8.0, float(pool.max_cpu_pool))

    # Cap to what's effectively available in this pool (after reservation) and pool max.
    cpu = min(cpu, float(pool.max_cpu_pool), float(eff_cpu))
    ram = min(ram, float(pool.max_ram_pool), float(eff_ram))

    # Ensure positive allocations (if we cannot, caller should treat as not fit).
    cpu = max(0.0, cpu)
    ram = max(0.0, ram)
    return cpu, ram


def _select_from_priority_queue(s, pr, pool_id, pool, eff_cpu, eff_ram, highpri_backlog, prefer_interactive_pool):
    """Rotate through up to scan_limit pipelines in this priority queue and pick a schedulable one.

    Selection rule (within scanned window):
    - must have a ready assignable op
    - must fit (cpu/ram <= eff_cpu/eff_ram)
    - choose the one with smallest implied RAM request (good for reducing mean latency)
    """
    q = s.queues[pr]
    if not q:
        return None

    best = None  # (score_ram, pipeline, op, cpu, ram)
    scanned = 0
    rotated = []

    while scanned < s.scan_limit and q:
        p = q.popleft()
        s.queue_members[pr].discard(p.pipeline_id)
        scanned += 1

        status = p.runtime_status()
        if status.is_pipeline_successful():
            # Drop completed
            continue
        if _pipeline_has_nonretriable_failure(s, p):
            # Drop terminal failures to avoid spinning
            continue

        # Pool preference: keep high-priority on interactive pool when possible,
        # but allow spillover when we are explicitly considering this pool.
        if prefer_interactive_pool and pr in (Priority.QUERY, Priority.INTERACTIVE) and pool_id != s.interactive_pool_id:
            # Rotate but don't select from non-interactive if we prefer the interactive pool.
            rotated.append(p)
            continue

        op = _get_ready_assignable_op(p)
        if op is None:
            rotated.append(p)
            continue

        cpu, ram = _apply_hints_and_cap(s, pool, eff_cpu, eff_ram, op, pr, highpri_backlog)
        if cpu <= 0.0 or ram <= 0.0:
            rotated.append(p)
            continue
        if cpu > eff_cpu or ram > eff_ram:
            rotated.append(p)
            continue

        # Score: prefer smaller RAM (often correlates with faster + better packing)
        score_ram = ram
        candidate = (score_ram, p, op, cpu, ram)

        if best is None or candidate[0] < best[0]:
            best = candidate

        # We still rotate it for fairness; if selected, we'll remove by not re-enqueueing it here.
        rotated.append(p)

    # Put rotated pipelines back preserving order.
    for p in rotated:
        _enqueue_pipeline(s, p)

    if best is None:
        return None

    # Remove the chosen pipeline once (it is currently enqueued due to rotation).
    # We remove by scanning and rebuilding at small cost only within this priority deque.
    # This keeps the "selected" pipeline out until we re-enqueue after assignment.
    chosen_p = best[1]
    pid = chosen_p.pipeline_id
    newq = []
    while s.queues[pr]:
        p = s.queues[pr].popleft()
        s.queue_members[pr].discard(p.pipeline_id)
        if p.pipeline_id == pid:
            # drop one instance
            pid = None
            continue
        newq.append(p)
    for p in newq:
        _enqueue_pipeline(s, p)

    return best[1], best[2], best[3], best[4]


@register_scheduler(key="scheduler_low_011_r2")
def scheduler_low_011_r2(s, results, pipelines):
    """
    Priority-first, multi-assignment-per-tick scheduler with OOM-aware RAM hinting.

    Returns:
      suspensions: none (no preemption in this iteration)
      assignments: list of Assignment objects for ready ops
    """
    # Enqueue new arrivals
    for p in pipelines:
        _enqueue_pipeline(s, p)

    # Learn from results
    for r in results:
        try:
            failed = r.failed()
        except Exception:
            failed = getattr(r, "error", None) is not None
        if not failed:
            continue

        ops = getattr(r, "ops", None) or []
        is_oom = _is_oom_error(getattr(r, "error", None))

        if is_oom:
            for op in ops:
                oid = id(op)
                # Retry budget
                s.op_attempts[oid] = int(s.op_attempts.get(oid, 0)) + 1

                # If over budget, stop retrying to prevent churn.
                if s.op_attempts[oid] > s.max_retries_per_op:
                    s.nonretriable_failed_ops.add(oid)
                    s.oom_retriable_ops.discard(oid)
                    continue

                s.oom_retriable_ops.add(oid)
                s.nonretriable_failed_ops.discard(oid)

                # Exponential RAM backoff based on last attempted allocation.
                prev = s.op_hints.get(oid, {})
                prev_ram = float(prev.get("ram", 0.0) or 0.0)
                last_ram = float(getattr(r, "ram", 0.0) or 0.0)
                baseline = max(prev_ram, last_ram, 1.0)
                new_ram = baseline * 2.0

                prev_cpu = float(prev.get("cpu", 0.0) or 0.0)
                last_cpu = float(getattr(r, "cpu", 0.0) or 0.0)
                keep_cpu = max(prev_cpu, last_cpu, 1.0)

                s.op_hints[oid] = {"ram": new_ram, "cpu": keep_cpu}
        else:
            for op in ops:
                oid = id(op)
                s.nonretriable_failed_ops.add(oid)
                s.oom_retriable_ops.discard(oid)

    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    highpri_backlog = _highpri_backlog(s)

    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)
        if avail_cpu <= 0.0 or avail_ram <= 0.0:
            continue

        # Effective available after reservation (only on interactive pool, only when high-pri backlog exists).
        eff_cpu = avail_cpu
        eff_ram = avail_ram
        if pool_id == s.interactive_pool_id and highpri_backlog > 0:
            eff_cpu = max(0.0, eff_cpu - float(pool.max_cpu_pool) * float(s.interactive_reserve_frac_cpu))
            eff_ram = max(0.0, eff_ram - float(pool.max_ram_pool) * float(s.interactive_reserve_frac_ram))

        # Spill control on non-interactive pools (keeps batch moving).
        highpri_spill_budget = s.max_highpri_spill_per_pool_per_tick if pool_id != s.interactive_pool_id else 10**9

        # Fill the pool with as many assignments as we can this tick.
        while eff_cpu > 0.0 and eff_ram > 0.0:
            # If interactive pool is present, prefer keeping QUERY/INTERACTIVE there when backlog is small,
            # but allow spillover under pressure (large backlog) or when this is the only pool.
            prefer_interactive_pool = (
                s.executor.num_pools > 1
                and highpri_backlog <= 2
                and pool_id != s.interactive_pool_id
            )

            # Priority order; on non-interactive pools, once spill budget is exhausted, focus on batch.
            if pool_id != s.interactive_pool_id and highpri_spill_budget <= 0:
                pr_order = [Priority.BATCH_PIPELINE, Priority.QUERY, Priority.INTERACTIVE]
            else:
                pr_order = [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]

            picked = None  # (pipeline, op, cpu, ram, pr)
            for pr in pr_order:
                # On the interactive pool with high-pri backlog, avoid scheduling batch if any high-pri is waiting.
                if pr == Priority.BATCH_PIPELINE and pool_id == s.interactive_pool_id and highpri_backlog > 0:
                    # Only schedule batch if there is no schedulable high-priority work (checked by attempts below).
                    pass

                sel = _select_from_priority_queue(
                    s=s,
                    pr=pr,
                    pool_id=pool_id,
                    pool=pool,
                    eff_cpu=eff_cpu,
                    eff_ram=eff_ram,
                    highpri_backlog=highpri_backlog,
                    prefer_interactive_pool=prefer_interactive_pool,
                )
                if sel is None:
                    continue

                p, op, cpu, ram = sel

                # Interactive-pool batch guard: if batch selected but high-pri backlog exists, double-check
                # that there truly isn't high-pri schedulable right now (cheap: attempt once).
                if pr == Priority.BATCH_PIPELINE and pool_id == s.interactive_pool_id and highpri_backlog > 0:
                    hi_try = None
                    for hi_pr in (Priority.QUERY, Priority.INTERACTIVE):
                        hi_try = _select_from_priority_queue(
                            s=s,
                            pr=hi_pr,
                            pool_id=pool_id,
                            pool=pool,
                            eff_cpu=eff_cpu,
                            eff_ram=eff_ram,
                            highpri_backlog=highpri_backlog,
                            prefer_interactive_pool=False,
                        )
                        if hi_try is not None:
                            # Put batch back by re-enqueueing it; schedule the high-priority instead.
                            _enqueue_pipeline(s, p)
                            p, op, cpu, ram = hi_try
                            pr = hi_pr
                            break

                picked = (p, op, cpu, ram, pr)
                break

            if picked is None:
                break

            p, op, cpu, ram, pr = picked

            # One last fit check
            if cpu <= 0.0 or ram <= 0.0 or cpu > eff_cpu or ram > eff_ram:
                # Put it back; can't schedule now.
                _enqueue_pipeline(s, p)
                break

            assignments.append(
                Assignment(
                    ops=[op],
                    cpu=cpu,
                    ram=ram,
                    priority=pr,
                    pool_id=pool_id,
                    pipeline_id=p.pipeline_id,
                )
            )

            eff_cpu -= cpu
            eff_ram -= ram

            if pool_id != s.interactive_pool_id and pr in (Priority.QUERY, Priority.INTERACTIVE):
                highpri_spill_budget -= 1

            # Re-enqueue pipeline for subsequent ops (unless it finished or became terminal).
            status = p.runtime_status()
            if not status.is_pipeline_successful() and not _pipeline_has_nonretriable_failure(s, p):
                _enqueue_pipeline(s, p)

    return suspensions, assignments