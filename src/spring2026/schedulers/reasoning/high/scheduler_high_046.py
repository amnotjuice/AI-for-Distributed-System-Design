# policy_key: scheduler_high_046
# reasoning_effort: high
# exp: reasoning
# model: gpt-5.2-2025-12-11
# llm_cost: 0.252277
# generation_seconds: 194.25
# generated_at: 2026-04-10T07:36:34.196023
@register_scheduler_init(key="scheduler_high_046")
def scheduler_high_046_init(s):
    """
    Priority-aware, failure-adaptive weighted round-robin scheduler.

    Goals for the objective:
      - Strongly protect QUERY and INTERACTIVE latency via (a) priority bias and
        (b) modest "scale-up" sizing for those priorities.
      - Avoid incomplete/failed pipelines (720s penalty) by:
          * treating FAILED ops as retriable (no dropping on first failure),
          * rapidly increasing RAM after failures (OOM-style backoff),
          * prioritizing retries (front of queue) to finish pipelines.
      - Ensure batch still makes progress using weighted round-robin (10:5:1)
        and small per-op resource caps to prevent batch from monopolizing a pool.
    """
    from collections import deque

    s.active = {}  # pipeline_id -> Pipeline
    s.queues = {
        Priority.QUERY: deque(),
        Priority.INTERACTIVE: deque(),
        Priority.BATCH_PIPELINE: deque(),
    }
    s.in_queue = {
        Priority.QUERY: set(),
        Priority.INTERACTIVE: set(),
        Priority.BATCH_PIPELINE: set(),
    }

    # Weighted round-robin order (roughly aligned with score weights).
    s.rr_order = (
        [Priority.QUERY] * 10
        + [Priority.INTERACTIVE] * 5
        + [Priority.BATCH_PIPELINE] * 1
    )
    s.rr_cursor = 0

    # Failure-adaptive hints (absolute units in "pool ram/cpu" units).
    s.ram_hint = {}     # pipeline_id -> suggested RAM for next attempt
    s.cpu_hint = {}     # pipeline_id -> suggested CPU (rarely used; mostly RAM-driven)
    s.fail_count = {}   # pipeline_id -> number of observed failed execution results

    # Map operator-object identity -> pipeline_id so we can attribute results.
    s.op_to_pipeline = {}

    # Bounded scanning to avoid O(n) per tick when queues are large.
    s.scan_limit = 32

    # Minimal CPU to launch a container; keep simple and conservative.
    s.min_cpu = 1.0


def _iter_pipeline_ops(pipeline):
    """Best-effort iterator over operators inside pipeline.values."""
    vals = getattr(pipeline, "values", None)
    if vals is None:
        return []
    if isinstance(vals, dict):
        return list(vals.values())
    if isinstance(vals, (list, tuple, set)):
        return list(vals)
    # Fallback: try iterating
    try:
        return list(vals)
    except Exception:
        return []


def _enqueue(s, pid, prio, front=False):
    """Enqueue pipeline id once per priority queue."""
    if pid in s.in_queue[prio]:
        return
    if front:
        s.queues[prio].appendleft(pid)
    else:
        s.queues[prio].append(pid)
    s.in_queue[prio].add(pid)


def _dequeue(s, prio):
    """Pop from queue and clear membership tracking."""
    if not s.queues[prio]:
        return None
    pid = s.queues[prio].popleft()
    s.in_queue[prio].discard(pid)
    return pid


def _cleanup_pipeline(s, pid):
    """Remove pipeline bookkeeping (safe if partially missing)."""
    p = s.active.pop(pid, None)
    s.ram_hint.pop(pid, None)
    s.cpu_hint.pop(pid, None)
    s.fail_count.pop(pid, None)

    # Best-effort cleanup of op->pipeline mapping to prevent unbounded growth.
    if p is not None:
        for op in _iter_pipeline_ops(p):
            s.op_to_pipeline.pop(id(op), None)


def _estimate_remaining_ops(pipeline, status):
    """Approximate remaining ops; used as a mild SRPT tie-breaker."""
    # Total ops
    try:
        total = len(getattr(pipeline, "values", []))
    except Exception:
        total = None

    if total is None or total <= 0:
        # Fallback: sum of known state counts
        try:
            total = sum(getattr(status, "state_counts", {}).values())
        except Exception:
            total = 0

    # Completed ops
    try:
        completed = status.state_counts.get(OperatorState.COMPLETED, 0)
    except Exception:
        completed = 0

    rem = total - completed
    return rem if rem > 0 else 0


def _priority_params(priority, pool):
    """
    Per-priority sizing knobs:
      - CPU caps bias toward finishing queries/interactive quickly without starving batch.
      - RAM baseline fractions start modest (to allow concurrency), but failures ramp quickly.
    """
    max_cpu = float(pool.max_cpu_pool)
    max_ram = float(pool.max_ram_pool)

    if priority == Priority.QUERY:
        cpu_cap = min(max_cpu, max(4.0, 0.60 * max_cpu), 16.0)
        ram_base = 0.35 * max_ram
        ram_min = 0.08 * max_ram
    elif priority == Priority.INTERACTIVE:
        cpu_cap = min(max_cpu, max(2.0, 0.40 * max_cpu), 8.0)
        ram_base = 0.25 * max_ram
        ram_min = 0.06 * max_ram
    else:  # Priority.BATCH_PIPELINE
        cpu_cap = min(max_cpu, max(1.0, 0.25 * max_cpu), 4.0)
        ram_base = 0.15 * max_ram
        ram_min = 0.04 * max_ram

    return cpu_cap, ram_base, ram_min


def _compute_allocation(s, pid, priority, pool, avail_cpu, avail_ram):
    """Compute (cpu, ram) allocation for a single operator attempt."""
    cpu_cap, ram_base, ram_min = _priority_params(priority, pool)

    # Ensure we don't launch "tiny" containers that are likely to just fail.
    if avail_cpu < s.min_cpu or avail_ram < ram_min:
        return 0.0, 0.0

    # RAM target: max(baseline, learned hint). After multiple failures, bias higher.
    hint_ram = float(s.ram_hint.get(pid, 0.0))
    failures = int(s.fail_count.get(pid, 0))

    target_ram = max(ram_base, hint_ram)

    # If we've failed repeatedly, try to "get it done" to avoid 720s incomplete penalty.
    if failures >= 2:
        target_ram = max(target_ram, 0.70 * float(pool.max_ram_pool))
    elif failures == 1:
        target_ram = max(target_ram, 0.50 * float(pool.max_ram_pool))

    ram = min(float(avail_ram), float(pool.max_ram_pool), target_ram)

    # CPU target: cap by priority; slight boost after failures to reduce retry wall time.
    hint_cpu = float(s.cpu_hint.get(pid, 0.0))
    target_cpu = cpu_cap
    if hint_cpu > 0:
        target_cpu = min(target_cpu, hint_cpu)
    if failures >= 1:
        target_cpu = min(float(pool.max_cpu_pool), max(target_cpu, 1.10 * cpu_cap))

    cpu = min(float(avail_cpu), float(pool.max_cpu_pool), target_cpu)

    if cpu < s.min_cpu or ram <= 0:
        return 0.0, 0.0
    return cpu, ram


def _next_rr_priority(s):
    pr = s.rr_order[s.rr_cursor]
    s.rr_cursor = (s.rr_cursor + 1) % len(s.rr_order)
    return pr


def _scan_pick_candidate(s, prio, pool, avail_cpu, avail_ram, scheduled_this_tick):
    """
    Scan up to s.scan_limit pipelines of a given priority and pick the best ready candidate.
    "Best" is smallest remaining ops (mild SRPT) among those that can be allocated.
    """
    q = s.queues[prio]
    if not q:
        return None

    scanned = []
    best = None  # (remaining_ops, pid, op, cpu, ram)

    limit = min(len(q), int(s.scan_limit))
    for _ in range(limit):
        pid = _dequeue(s, prio)
        if pid is None:
            break
        scanned.append(pid)

        if pid in scheduled_this_tick:
            continue

        pipeline = s.active.get(pid)
        if pipeline is None:
            continue

        status = pipeline.runtime_status()
        if status.is_pipeline_successful():
            _cleanup_pipeline(s, pid)
            continue

        ops = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        if not ops:
            continue

        cpu, ram = _compute_allocation(s, pid, pipeline.priority, pool, avail_cpu, avail_ram)
        if cpu <= 0 or ram <= 0:
            continue

        remaining = _estimate_remaining_ops(pipeline, status)
        cand = (remaining, pid, ops[0], cpu, ram)
        if best is None or cand[0] < best[0]:
            best = cand

    # Requeue scanned (excluding cleaned-up pipelines).
    for pid in scanned:
        pipeline = s.active.get(pid)
        if pipeline is None:
            continue
        _enqueue(s, pid, prio, front=False)

    return best


def _pick_next_assignment(s, pool, avail_cpu, avail_ram, scheduled_this_tick):
    """Pick next assignment using weighted RR across priorities, then scan within that priority."""
    attempts = len(s.rr_order)
    for _ in range(attempts):
        prio = _next_rr_priority(s)
        cand = _scan_pick_candidate(s, prio, pool, avail_cpu, avail_ram, scheduled_this_tick)
        if cand is not None:
            _, pid, op, cpu, ram = cand
            return pid, op, cpu, ram
    return None


@register_scheduler(key="scheduler_high_046")
def scheduler_high_046_scheduler(s, results: List[ExecutionResult], pipelines: List[Pipeline]):
    """
    Main scheduling loop.

    Key behaviors:
      - Admit all new pipelines; enqueue by priority.
      - On failure result: increase RAM hint aggressively and push pipeline to the front of its queue.
      - Fill each pool using weighted RR (10:5:1) + small SRPT-like tie-breaker.
      - No preemption (avoids churn); instead rely on priority bias + resource caps to protect latency.
    """
    # Track new arrivals.
    for p in pipelines:
        s.active[p.pipeline_id] = p

        # Build op->pipeline mapping for attributing ExecutionResults.
        for op in _iter_pipeline_ops(p):
            s.op_to_pipeline[id(op)] = p.pipeline_id

        _enqueue(s, p.pipeline_id, p.priority, front=False)

    # Early exit if nothing changed that could free capacity or add work.
    if not pipelines and not results:
        return [], []

    # Process results: update failure hints and prioritize retries.
    for r in results:
        pid = None
        try:
            if getattr(r, "ops", None):
                pid = s.op_to_pipeline.get(id(r.ops[0]))
        except Exception:
            pid = None

        # Fallback: try common fields if mapping failed.
        if pid is None:
            try:
                if getattr(r, "ops", None) and hasattr(r.ops[0], "pipeline_id"):
                    pid = r.ops[0].pipeline_id
            except Exception:
                pid = None

        if pid is None:
            continue

        pipeline = s.active.get(pid)
        if pipeline is None:
            continue

        if r.failed():
            # Treat as likely OOM/resource failure: increase RAM hint rapidly.
            prev = float(s.ram_hint.get(pid, 0.0))
            try:
                used_ram = float(getattr(r, "ram", 0.0))
            except Exception:
                used_ram = 0.0

            # Exponential backoff based on what we attempted last time.
            new_hint = max(prev, used_ram * 2.0 if used_ram > 0 else prev * 2.0, 1.0)
            s.ram_hint[pid] = new_hint

            s.fail_count[pid] = int(s.fail_count.get(pid, 0)) + 1

            # Retry soon to avoid leaving the pipeline incomplete.
            _enqueue(s, pid, pipeline.priority, front=True)

    suspensions = []  # No preemption in this iteration.
    assignments = []
    scheduled_this_tick = set()

    # Fill each pool to available capacity.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)

        # Greedily assign multiple ops per pool until resources are fragmented or queues empty.
        while avail_cpu >= s.min_cpu and avail_ram > 0:
            pick = _pick_next_assignment(s, pool, avail_cpu, avail_ram, scheduled_this_tick)
            if pick is None:
                break

            pid, op, cpu, ram = pick
            pipeline = s.active.get(pid)
            if pipeline is None:
                continue

            # Final guard: if pipeline already completed since we scanned, skip.
            status = pipeline.runtime_status()
            if status.is_pipeline_successful():
                _cleanup_pipeline(s, pid)
                continue

            # Assign a single operator (simple + reduces multi-op memory spikes).
            assignments.append(
                Assignment(
                    ops=[op],
                    cpu=cpu,
                    ram=ram,
                    priority=pipeline.priority,
                    pool_id=pool_id,
                    pipeline_id=pid,
                )
            )

            scheduled_this_tick.add(pid)
            avail_cpu -= float(cpu)
            avail_ram -= float(ram)

    return suspensions, assignments
