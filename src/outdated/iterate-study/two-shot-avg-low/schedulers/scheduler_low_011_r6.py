# source: scheduler_low_011.py
# model: gpt-5.2-2025-12-11
# effort: low
# feedback: minimal
# iteration: r6
@register_scheduler_init(key="scheduler_low_011_r6")
def scheduler_low_011_r6_init(s):
    """Priority-first, multi-assignment scheduler with light isolation + OOM-aware retries.

    Incremental fixes over the previous iteration:
    - Fill each pool with multiple assignments per scheduler tick (avoid leaving idle resources).
    - Remove the unsafe "early exit" when there is already queued work.
    - Stronger priority isolation: avoid scheduling batch on the interactive pool unless no HP backlog.
    - OOM retry actually works: do NOT drop pipelines with FAILED ops if those failures are OOM-retriable.
    - Learn per-operator RAM hints from OOM failures (exponential backoff), and reuse them on retries.

    Design goals:
    - Reduce queueing delay for QUERY/INTERACTIVE (latency).
    - Avoid interference from BATCH on the interactive pool when HP work is waiting.
    - Keep policy simple and robust (no preemption assumptions about executor internals).
    """
    from collections import deque

    # Per-priority FIFO queues of pipelines (round-robin within a priority).
    s.waiting_queues = {
        Priority.QUERY: deque(),
        Priority.INTERACTIVE: deque(),
        Priority.BATCH_PIPELINE: deque(),
    }

    # De-dup tracking per priority to prevent a pipeline from being queued multiple times.
    s.in_queue = {
        Priority.QUERY: set(),
        Priority.INTERACTIVE: set(),
        Priority.BATCH_PIPELINE: set(),
    }

    # Track pipeline priority for lookups (e.g., when results lack pipeline_id).
    s.pipeline_priority = {}

    # Map operator identity -> pipeline_id (best-effort, set when we attempt to schedule an op).
    s.op_to_pipeline = {}

    # Learned hints, keyed by (pipeline_id, op_id): {"ram": float, "cpu": float}
    s.op_hints = {}

    # Attempt counters for OOM retries, keyed by (pipeline_id, op_id)
    s.op_attempts = {}

    # Ops that are currently considered OOM-retriable failures, keyed by (pipeline_id, op_id)
    s.retriable_failed_ops = set()

    # Config knobs
    s.max_retries_per_op = 3

    # When multiple pools exist, treat pool 0 as "interactive-preferred"
    s.interactive_pool_id = 0


def _prio_order():
    return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]


def _is_oom_error(err):
    if err is None:
        return False
    msg = str(err).lower()
    return ("oom" in msg) or ("out of memory" in msg) or ("memoryerror" in msg)


def _op_id(op):
    return id(op)


def _hint_key(pipeline_id, op):
    return (pipeline_id, _op_id(op))


def _queue_len(s, pr):
    return len(s.waiting_queues[pr])


def _any_hp_backlog(s):
    return (_queue_len(s, Priority.QUERY) + _queue_len(s, Priority.INTERACTIVE)) > 0


def _enqueue_pipeline(s, p, front=False):
    pr = p.priority if p.priority in s.waiting_queues else Priority.BATCH_PIPELINE
    s.pipeline_priority[p.pipeline_id] = pr
    if p.pipeline_id in s.in_queue[pr]:
        return
    if front:
        s.waiting_queues[pr].appendleft(p)
    else:
        s.waiting_queues[pr].append(p)
    s.in_queue[pr].add(p.pipeline_id)


def _dequeue_pipeline(s, pr):
    """Pop left; updates de-dup set. Returns None if empty."""
    if not s.waiting_queues[pr]:
        return None
    p = s.waiting_queues[pr].popleft()
    s.in_queue[pr].discard(p.pipeline_id)
    return p


def _requeue_pipeline(s, p, front=False):
    """Reinsert pipeline (round-robin)."""
    _enqueue_pipeline(s, p, front=front)


def _get_failed_ops(status):
    # Best-effort: Eudoxia get_ops should accept an iterable of states.
    try:
        return status.get_ops([OperatorState.FAILED], require_parents_complete=False) or []
    except Exception:
        return []


def _pipeline_has_nonretriable_failures(s, p):
    status = p.runtime_status()
    failed_ops = _get_failed_ops(status)
    if not failed_ops:
        return False

    # If ANY failed op is not marked retriable, or exceeded retries, pipeline is considered failed.
    for op in failed_ops:
        k = _hint_key(p.pipeline_id, op)
        if k not in s.retriable_failed_ops:
            return True
        if int(s.op_attempts.get(k, 0)) > int(s.max_retries_per_op):
            return True
    return False


def _next_ready_op(p):
    status = p.runtime_status()
    ops = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
    if not ops:
        return None
    return ops[0]


def _pool_iteration_order(s):
    # Prefer to place HP work quickly on the interactive pool by scheduling it first.
    if s.executor.num_pools <= 1:
        return list(range(s.executor.num_pools))
    ip = s.interactive_pool_id
    return [ip] + [i for i in range(s.executor.num_pools) if i != ip]


def _compute_request(s, pool, pr, pipeline_id, op, cpu_left, ram_left, hp_backlog_count):
    """Compute CPU/RAM request for this op in this pool under current backlog."""
    # Base targets tuned for latency isolation and concurrency:
    # - HP: allocate bigger slices, but share if backlog > 1
    # - Batch: allocate smaller slices for better packing and to avoid hogging shared pools
    if pr in (Priority.QUERY, Priority.INTERACTIVE):
        cpu_frac = 0.85 if hp_backlog_count <= 1 else 0.50
        ram_frac = 0.70 if hp_backlog_count <= 1 else 0.55
        cpu = max(1.0, pool.max_cpu_pool * cpu_frac)
        ram = max(1.0, pool.max_ram_pool * ram_frac)

        # If multiple HP ops are waiting, try to leave room for another HP op in this pool.
        if hp_backlog_count > 1:
            cpu = min(cpu, max(1.0, cpu_left / 2.0))
            ram = min(ram, max(1.0, ram_left / 2.0))
    else:
        # Batch: smaller chunks; allow growth when no HP backlog and plenty of headroom.
        if hp_backlog_count == 0 and cpu_left >= (0.75 * pool.max_cpu_pool):
            cpu_frac = 0.50
        else:
            cpu_frac = 0.25
        ram_frac = 0.25
        cpu = max(1.0, pool.max_cpu_pool * cpu_frac)
        ram = max(1.0, pool.max_ram_pool * ram_frac)

    # Apply learned hints (especially for OOM retries)
    hk = _hint_key(pipeline_id, op)
    hint = s.op_hints.get(hk)
    if hint:
        try:
            cpu = max(cpu, float(hint.get("cpu", cpu)))
        except Exception:
            pass
        try:
            ram = max(ram, float(hint.get("ram", ram)))
        except Exception:
            pass

    # Cap to pool max and current remaining headroom
    cpu = min(cpu, pool.max_cpu_pool, cpu_left)
    ram = min(ram, pool.max_ram_pool, ram_left)

    # Ensure positive minima
    cpu = max(1.0, cpu)
    ram = max(1.0, ram)
    return cpu, ram


@register_scheduler(key="scheduler_low_011_r6")
def scheduler_low_011_r6(s, results, pipelines):
    """
    Priority-first scheduler that fills pools each tick and retries OOM failures with more RAM.

    Key behaviors:
    - Enqueue new pipelines into per-priority queues (de-duplicated).
    - Update OOM hints from results and mark failed ops as retriable up to a retry budget.
    - For each pool (interactive pool first), repeatedly assign ready ops while resources remain:
        * Always try QUERY, then INTERACTIVE, then BATCH.
        * Avoid BATCH on the interactive pool if any HP backlog exists.
        * Keep small headroom on non-interactive pools for HP spillover when HP backlog exists.
    """
    # 1) Enqueue new arrivals
    for p in pipelines:
        _enqueue_pipeline(s, p, front=False)

    # 2) Process results to learn OOM hints and manage retriable failures
    for r in results:
        # Clear retriable marker on success-like events (best-effort)
        try:
            failed = r.failed()
        except Exception:
            failed = getattr(r, "error", None) is not None

        ops = getattr(r, "ops", []) or []

        if not failed:
            # On success, remove retriable markers (but keep hints).
            for op in ops:
                pid = getattr(r, "pipeline_id", None)
                if pid is None:
                    pid = s.op_to_pipeline.get(_op_id(op))
                if pid is None:
                    continue
                s.retriable_failed_ops.discard(_hint_key(pid, op))
            continue

        # Failure path
        err = getattr(r, "error", None)
        if not _is_oom_error(err):
            # Non-OOM failure: do not mark retriable; pipeline will be dropped by failure check.
            continue

        for op in ops:
            pid = getattr(r, "pipeline_id", None)
            if pid is None:
                pid = s.op_to_pipeline.get(_op_id(op))
            if pid is None:
                continue

            hk = _hint_key(pid, op)
            prev_attempts = int(s.op_attempts.get(hk, 0))
            s.op_attempts[hk] = prev_attempts + 1

            # If exceeded retry budget, stop treating it as retriable.
            if s.op_attempts[hk] > int(s.max_retries_per_op):
                s.retriable_failed_ops.discard(hk)
                continue

            # Learn RAM hint (double the last known/allocated)
            prev_hint = s.op_hints.get(hk, {})
            prev_ram = float(prev_hint.get("ram", 0.0) or 0.0)
            alloc_ram = float(getattr(r, "ram", 0.0) or 0.0)

            baseline_ram = prev_ram if prev_ram > 0 else (alloc_ram if alloc_ram > 0 else 1.0)
            new_ram = max(1.0, baseline_ram * 2.0)

            # Keep CPU hint as the last allocation if present; otherwise leave unset.
            alloc_cpu = float(getattr(r, "cpu", 0.0) or 0.0)
            prev_cpu = float(prev_hint.get("cpu", 0.0) or 0.0)
            cpu_hint = prev_cpu if prev_cpu > 0 else (alloc_cpu if alloc_cpu > 0 else 1.0)

            s.op_hints[hk] = {"ram": new_ram, "cpu": max(1.0, cpu_hint)}
            s.retriable_failed_ops.add(hk)

            # Push the pipeline to the front to reduce latency of the retry (HP primarily benefits)
            # (If it's already queued, enqueue is de-duped; this is best-effort.)
            pr = s.pipeline_priority.get(pid, getattr(r, "priority", Priority.BATCH_PIPELINE))
            # We don't have the pipeline object here reliably, so we cannot requeue directly.
            # The pipeline will reappear in queues via normal rotation; the key win is the hint + retriable marker.

    # 3) If no waiting work at all, do nothing
    any_waiting = False
    for pr in _prio_order():
        if s.waiting_queues[pr]:
            any_waiting = True
            break
    if not any_waiting:
        return [], []

    suspensions = []
    assignments = []

    # Helper counts for HP backlog (proxy for contention)
    hp_backlog_count = _queue_len(s, Priority.QUERY) + _queue_len(s, Priority.INTERACTIVE)

    # 4) Main placement loop: fill pools (interactive pool first)
    for pool_id in _pool_iteration_order(s):
        pool = s.executor.pools[pool_id]

        cpu_left = float(pool.avail_cpu_pool)
        ram_left = float(pool.avail_ram_pool)
        if cpu_left <= 0 or ram_left <= 0:
            continue

        # Batch isolation rules:
        # - On interactive pool, avoid batch whenever any HP backlog exists.
        allow_batch_here = True
        if s.executor.num_pools > 1 and pool_id == s.interactive_pool_id and _any_hp_backlog(s):
            allow_batch_here = False

        # On non-interactive pools, if HP backlog exists, keep a little reserve to allow HP spillover.
        reserve_cpu = 0.0
        reserve_ram = 0.0
        if s.executor.num_pools > 1 and pool_id != s.interactive_pool_id and _any_hp_backlog(s):
            reserve_cpu = max(1.0, 0.10 * float(pool.max_cpu_pool))
            reserve_ram = max(1.0, 0.10 * float(pool.max_ram_pool))

        # Try to assign multiple ops until we can't make progress.
        # Use a bounded scan per priority to avoid infinite loops when many pipelines are blocked.
        made_progress = True
        while made_progress:
            made_progress = False

            for pr in _prio_order():
                if pr == Priority.BATCH_PIPELINE and not allow_batch_here:
                    continue
                if not s.waiting_queues[pr]:
                    continue

                # Available headroom for this priority (batch respects reserves when HP backlog exists)
                pr_cpu_left = cpu_left
                pr_ram_left = ram_left
                if pr == Priority.BATCH_PIPELINE:
                    pr_cpu_left = cpu_left - reserve_cpu
                    pr_ram_left = ram_left - reserve_ram
                    if pr_cpu_left < 1.0 or pr_ram_left < 1.0:
                        continue

                qlen = len(s.waiting_queues[pr])
                if qlen == 0:
                    continue

                # Scan at most qlen pipelines to find a ready op that fits.
                scanned = 0
                while scanned < qlen and s.waiting_queues[pr]:
                    p = _dequeue_pipeline(s, pr)
                    scanned += 1
                    if p is None:
                        break

                    status = p.runtime_status()

                    # Drop completed pipelines
                    if status.is_pipeline_successful():
                        continue

                    # Drop pipelines with non-retriable failures
                    if _pipeline_has_nonretriable_failures(s, p):
                        continue

                    op = _next_ready_op(p)
                    if op is None:
                        # Not ready; keep it in the queue to be revisited later.
                        _requeue_pipeline(s, p, front=False)
                        continue

                    # Record mapping so result processing can recover pipeline_id when needed.
                    s.op_to_pipeline[_op_id(op)] = p.pipeline_id
                    s.pipeline_priority[p.pipeline_id] = pr

                    req_cpu, req_ram = _compute_request(
                        s=s,
                        pool=pool,
                        pr=pr,
                        pipeline_id=p.pipeline_id,
                        op=op,
                        cpu_left=pr_cpu_left,
                        ram_left=pr_ram_left,
                        hp_backlog_count=hp_backlog_count,
                    )

                    if req_cpu <= pr_cpu_left and req_ram <= pr_ram_left:
                        assignments.append(
                            Assignment(
                                ops=[op],
                                cpu=req_cpu,
                                ram=req_ram,
                                priority=pr,
                                pool_id=pool_id,
                                pipeline_id=p.pipeline_id,
                            )
                        )

                        # Update local remaining headroom to allow multiple placements in one tick.
                        cpu_left -= req_cpu
                        ram_left -= req_ram

                        # Requeue the pipeline (round-robin) so its next ready op can be scheduled later.
                        _requeue_pipeline(s, p, front=False)

                        made_progress = True
                        break  # re-start from highest priority with updated resources
                    else:
                        # Doesn't fit right now; keep it and try another pipeline.
                        _requeue_pipeline(s, p, front=False)

                if made_progress:
                    break  # restart priority loop

            # Stop if we can't allocate even minimal resources anymore
            if cpu_left < 1.0 or ram_left < 1.0:
                break

    return suspensions, assignments