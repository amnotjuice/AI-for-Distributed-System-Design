# source: scheduler_low_011.py
# model: gpt-5.2-2025-12-11
# effort: low
# feedback: minimal
# iteration: r12
@register_scheduler_init(key="scheduler_low_011_r12")
def scheduler_low_011_r12_init(s):
    """Priority-aware FIFO with OOM-aware retries, soft pool preference, and headroom protection.

    Incremental improvements over the prior iteration:
    - Correctly retry FAILED ops when the last failure looked like OOM (ASSIGNABLE_STATES includes FAILED).
    - Avoid queue blow-up by keeping at most one queue entry per pipeline_id per priority.
    - Soft pool preference: keep high-priority on an "interactive" pool when possible, but spill over when needed.
    - Headroom protection: when high-priority work is waiting, avoid letting batch consume the last pool capacity.
    - Simple aging to avoid indefinite batch starvation under constant high-priority arrivals.
    """
    from collections import deque

    s.tick = 0

    # One FIFO queue per priority, plus a set to ensure a pipeline_id appears at most once per queue.
    s.waiting_queues = {
        Priority.QUERY: deque(),
        Priority.INTERACTIVE: deque(),
        Priority.BATCH_PIPELINE: deque(),
    }
    s.in_queue = {
        Priority.QUERY: set(),
        Priority.INTERACTIVE: set(),
        Priority.BATCH_PIPELINE: set(),
    }

    # Operator-level hints keyed by operator object id (stable within a simulation run).
    # ram_hint: last-known safe RAM after OOMs; cpu_hint: optional, conservative.
    s.op_hints = {}          # op_id -> {"ram": float, "cpu": float}
    s.op_attempts = {}       # op_id -> int (only counts retry-worthy failures like OOM)
    s.op_retryable = {}      # op_id -> bool (True if we believe retry may succeed)

    # Pipeline bookkeeping for simple aging/backoff decisions.
    s.pipeline_first_seen = {}   # pipeline_id -> tick
    s.pipeline_last_scheduled = {}  # pipeline_id -> tick

    # Knobs (kept simple and safe)
    s.max_retries_per_op = 3
    s.interactive_pool_id = 0

    # Default sizing fractions of pool MAX. (Favor latency for high-priority, full utilization for batch.)
    s.size_fracs = {
        Priority.QUERY: {"cpu": 0.90, "ram": 0.60},
        Priority.INTERACTIVE: {"cpu": 0.80, "ram": 0.60},
        Priority.BATCH_PIPELINE: {"cpu": 1.00, "ram": 1.00},
    }

    # When high-priority is waiting, protect a small headroom slice to reduce admission latency.
    s.headroom_frac = {"cpu": 0.15, "ram": 0.15}

    # Aging: after this many ticks waiting, allow batch to ignore headroom protection to ensure progress.
    s.batch_aging_ticks = 50


def _prio_order():
    return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]


def _is_oom_error(err):
    if err is None:
        return False
    msg = str(err).lower()
    return ("oom" in msg) or ("out of memory" in msg) or ("memoryerror" in msg) or ("bad_alloc" in msg)


def _enqueue_pipeline(s, pipeline):
    pr = pipeline.priority if pipeline.priority in s.waiting_queues else Priority.BATCH_PIPELINE
    pid = pipeline.pipeline_id
    if pid not in s.pipeline_first_seen:
        s.pipeline_first_seen[pid] = s.tick

    if pid not in s.in_queue[pr]:
        s.waiting_queues[pr].append(pipeline)
        s.in_queue[pr].add(pid)


def _dequeue_left(s, pr):
    q = s.waiting_queues[pr]
    if not q:
        return None
    p = q.popleft()
    # Keep set in sync (defensive: allow duplicates to be cleaned up)
    s.in_queue[pr].discard(p.pipeline_id)
    return p


def _requeue_right(s, pipeline):
    pr = pipeline.priority if pipeline.priority in s.waiting_queues else Priority.BATCH_PIPELINE
    pid = pipeline.pipeline_id
    if pid not in s.in_queue[pr]:
        s.waiting_queues[pr].append(pipeline)
        s.in_queue[pr].add(pid)


def _pipeline_has_unretryable_failure(s, pipeline):
    """Drop pipelines only if they have FAILED ops that we deem non-retryable."""
    st = pipeline.runtime_status()
    failed_ops = st.get_ops([OperatorState.FAILED], require_parents_complete=False) or []
    if not failed_ops:
        return False

    # If any failed op is not marked retryable, treat pipeline as unrecoverably failed.
    for op in failed_ops:
        op_id = id(op)
        # If we never saw a failure result for it, we conservatively treat it as non-retryable.
        if not s.op_retryable.get(op_id, False):
            return True
        # If we've already exhausted retries, treat as non-retryable going forward.
        if s.op_attempts.get(op_id, 0) > s.max_retries_per_op:
            return True

    return False


def _next_assignable_op(pipeline):
    st = pipeline.runtime_status()
    ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True) or []
    return ops[0] if ops else None


def _high_priority_waiting(s):
    return bool(s.waiting_queues[Priority.QUERY]) or bool(s.waiting_queues[Priority.INTERACTIVE])


def _batch_aged_out(s, pipeline):
    if pipeline.priority != Priority.BATCH_PIPELINE:
        return False
    first = s.pipeline_first_seen.get(pipeline.pipeline_id, s.tick)
    return (s.tick - first) >= s.batch_aging_ticks


def _preferred_pool_for_priority(s, pr):
    # Single-pool: no preference.
    if s.executor.num_pools <= 1:
        return None
    if pr in (Priority.QUERY, Priority.INTERACTIVE):
        return s.interactive_pool_id
    return None


def _default_request(s, pool, pr):
    fr = s.size_fracs.get(pr, {"cpu": 1.0, "ram": 1.0})
    cpu = max(1.0, pool.max_cpu_pool * float(fr["cpu"]))
    ram = max(1.0, pool.max_ram_pool * float(fr["ram"]))

    # Cap to pool availability.
    cpu = min(cpu, pool.avail_cpu_pool, pool.max_cpu_pool)
    ram = min(ram, pool.avail_ram_pool, pool.max_ram_pool)

    # Ensure still positive.
    cpu = max(1.0, cpu)
    ram = max(1.0, ram)
    return cpu, ram


def _apply_hints(s, pool, pr, op, cpu, ram):
    hint = s.op_hints.get(id(op))
    if hint:
        # Use max() so we don't regress into repeated OOM by shrinking.
        ram = max(ram, float(hint.get("ram", ram)))
        cpu = max(cpu, float(hint.get("cpu", cpu)))

    # Final caps.
    cpu = min(cpu, pool.avail_cpu_pool, pool.max_cpu_pool)
    ram = min(ram, pool.avail_ram_pool, pool.max_ram_pool)
    cpu = max(1.0, cpu)
    ram = max(1.0, ram)
    return cpu, ram


def _apply_headroom_protection(s, pool, pr, pipeline, cpu, ram):
    """If high-priority is waiting, keep a small reserve so new high-priority work can start quickly."""
    if pr != Priority.BATCH_PIPELINE:
        return cpu, ram

    # If batch has been waiting long enough, allow it to consume headroom to ensure progress.
    if _batch_aged_out(s, pipeline):
        return cpu, ram

    if not _high_priority_waiting(s):
        return cpu, ram

    reserve_cpu = pool.max_cpu_pool * float(s.headroom_frac["cpu"])
    reserve_ram = pool.max_ram_pool * float(s.headroom_frac["ram"])

    # Leave headroom if possible.
    max_cpu_for_batch = pool.avail_cpu_pool - reserve_cpu
    max_ram_for_batch = pool.avail_ram_pool - reserve_ram

    # If we can't leave any headroom, don't schedule batch in this pool right now.
    if max_cpu_for_batch < 1.0 or max_ram_for_batch < 1.0:
        return None, None

    cpu = min(cpu, max_cpu_for_batch)
    ram = min(ram, max_ram_for_batch)
    cpu = max(1.0, cpu)
    ram = max(1.0, ram)
    return cpu, ram


@register_scheduler(key="scheduler_low_011_r12")
def scheduler_low_011_r12(s, results, pipelines):
    """
    Scheduler step:
    - Enqueue new pipelines.
    - Learn from failures (especially OOM) and mark failed ops as retryable/non-retryable.
    - For each pool, pick at most one operator to run:
        * Always try higher priorities first.
        * Keep high-priority on interactive pool when feasible; spill over if necessary.
        * When high-priority is waiting, protect headroom against batch (with aging escape hatch).
    """
    s.tick += 1

    # Enqueue arrivals
    for p in pipelines:
        _enqueue_pipeline(s, p)

    # Learn from results
    for r in results:
        # If a container finished successfully, we don't need to learn much for latency-first policy.
        failed = False
        try:
            failed = r.failed()
        except Exception:
            failed = getattr(r, "error", None) is not None

        if not failed:
            continue

        err = getattr(r, "error", None)
        ops = getattr(r, "ops", None) or []
        is_oom = _is_oom_error(err)

        for op in ops:
            op_id = id(op)
            if is_oom:
                # Mark retryable and inflate RAM hint (exponential backoff).
                s.op_retryable[op_id] = True
                s.op_attempts[op_id] = int(s.op_attempts.get(op_id, 0)) + 1

                prev = s.op_hints.get(op_id, {})
                prev_ram = float(prev.get("ram", getattr(r, "ram", 1.0) or 1.0))
                prev_cpu = float(prev.get("cpu", getattr(r, "cpu", 1.0) or 1.0))

                # Double RAM from last known allocation/hint; CPU remains conservative.
                new_ram = max(1.0, prev_ram * 2.0)
                s.op_hints[op_id] = {"ram": new_ram, "cpu": max(1.0, prev_cpu)}
            else:
                # Non-OOM failures are considered non-retryable for this iteration.
                s.op_retryable[op_id] = False

    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    # Pool-local scheduling: one assignment per pool per tick (low-risk, keeps interference bounded).
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        if pool.avail_cpu_pool <= 0 or pool.avail_ram_pool <= 0:
            continue

        # If multiple pools exist, try to keep the interactive pool free of batch when high-priority exists.
        if s.executor.num_pools > 1 and pool_id == s.interactive_pool_id and _high_priority_waiting(s):
            batch_allowed_in_interactive_pool = False
        else:
            batch_allowed_in_interactive_pool = True

        chosen = None
        chosen_op = None
        chosen_pr = None
        chosen_cpu = None
        chosen_ram = None

        for pr in _prio_order():
            if pr == Priority.BATCH_PIPELINE and not batch_allowed_in_interactive_pool and pool_id == s.interactive_pool_id:
                continue

            q = s.waiting_queues[pr]
            n = len(q)
            if n == 0:
                continue

            # Soft preference: if this is high-priority and we're not on interactive pool,
            # skip scheduling it here if the interactive pool still has capacity.
            pref_pool = _preferred_pool_for_priority(s, pr)
            if pref_pool is not None and pool_id != pref_pool:
                ip = s.executor.pools[pref_pool]
                # If interactive pool has at least some meaningful headroom, keep high-priority there.
                if ip.avail_cpu_pool >= max(1.0, ip.max_cpu_pool * 0.25) and ip.avail_ram_pool >= max(1.0, ip.max_ram_pool * 0.25):
                    # We will still spill over if interactive pool is tight; otherwise prefer to wait.
                    pass
                else:
                    pref_pool = None  # spillover allowed

            # Rotate through the queue to find a runnable pipeline without breaking FIFO too hard.
            for _ in range(n):
                p = _dequeue_left(s, pr)
                if p is None:
                    break

                st = p.runtime_status()
                # Drop completed pipelines.
                if st.is_pipeline_successful():
                    continue

                # Drop pipelines with non-retryable failures.
                if _pipeline_has_unretryable_failure(s, p):
                    continue

                # If we are in a non-preferred pool and still trying to keep it on preferred pool, requeue and move on.
                if pref_pool is not None and pool_id != pref_pool:
                    _requeue_right(s, p)
                    continue

                op = _next_assignable_op(p)
                if op is None:
                    # Not ready; keep it for later.
                    _requeue_right(s, p)
                    continue

                cpu, ram = _default_request(s, pool, pr)
                cpu, ram = _apply_hints(s, pool, pr, op, cpu, ram)

                cpu, ram = _apply_headroom_protection(s, pool, pr, p, cpu, ram)
                if cpu is None or ram is None:
                    # Can't schedule batch here due to headroom; requeue and keep scanning.
                    _requeue_right(s, p)
                    continue

                if cpu <= pool.avail_cpu_pool and ram <= pool.avail_ram_pool:
                    chosen = p
                    chosen_op = op
                    chosen_pr = pr
                    chosen_cpu = cpu
                    chosen_ram = ram
                    break

                # Doesn't fit; requeue and continue.
                _requeue_right(s, p)

            if chosen is not None:
                break

        if chosen is None:
            continue

        assignments.append(
            Assignment(
                ops=[chosen_op],
                cpu=chosen_cpu,
                ram=chosen_ram,
                priority=chosen_pr,
                pool_id=pool_id,
                pipeline_id=chosen.pipeline_id,
            )
        )
        s.pipeline_last_scheduled[chosen.pipeline_id] = s.tick

        # Keep pipeline in circulation for next operator(s).
        _requeue_right(s, chosen)

    return suspensions, assignments