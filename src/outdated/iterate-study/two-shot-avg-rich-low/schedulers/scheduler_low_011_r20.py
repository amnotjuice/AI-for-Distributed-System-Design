# source: scheduler_low_011.py
# model: gpt-5.2-2025-12-11
# effort: low
# feedback: rich
# iteration: r20
@register_scheduler_init(key="scheduler_low_011_r20")
def scheduler_low_011_r20_init(s):
    """Priority-aware, latency-focused scheduler (v2) with fairness + higher parallelism.

    Key changes vs prior iteration:
    - Schedules MULTIPLE containers per pool per tick (greedy packing) to cut queueing latency.
    - Uses weighted service across priorities to avoid starvation (query still favored).
    - Soft pool partitioning + headroom reservation to protect query latency while allowing progress for others.
    - OOM-aware RAM backoff retries (only when we actually observe OOM-like failures).
    - Keeps pipelines enqueued uniquely (by pipeline_id) to avoid duplicate queue blowups.
    """
    from collections import deque

    # Per-priority FIFO of pipeline_ids (unique membership enforced by s.in_queue_prio)
    s.queues = {
        Priority.QUERY: deque(),
        Priority.INTERACTIVE: deque(),
        Priority.BATCH_PIPELINE: deque(),
    }
    s.pipeline_by_id = {}         # pipeline_id -> Pipeline
    s.in_queue_prio = {}          # pipeline_id -> Priority (if currently enqueued)

    # Lightweight "time" for aging/progress heuristics
    s.tick = 0
    s.enqueue_tick = {}           # pipeline_id -> tick enqueued
    s.last_progress_tick = {}     # pipeline_id -> tick of last successful op completion

    # Failure handling / hints
    s.op_hints = {}               # (pipeline_id, op_id) -> {"ram": float, "cpu": float}
    s.op_attempts = {}            # (pipeline_id, op_id) -> int
    s.oom_retriable = set()       # (pipeline_id, op_id) that we observed failing with OOM-like error
    s.non_oom_failed = set()      # (pipeline_id, op_id) that we observed failing with non-OOM error
    s.max_retries_per_op = 3

    # Map op object identity back to pipeline_id (filled on assignment; consumed on results)
    s.op_to_pipeline = {}         # id(op) -> pipeline_id

    # Weighted service: query is favored, but interactive and batch get guaranteed turns
    s.service_cycle = [
        Priority.QUERY, Priority.QUERY, Priority.QUERY, Priority.QUERY,
        Priority.INTERACTIVE, Priority.QUERY,
        Priority.INTERACTIVE, Priority.BATCH_PIPELINE,
    ]
    s.pool_cycle_idx = {}         # pool_id -> index into service_cycle

    # Soft pool partition: prefer pool 0 for query/interactive if multiple pools exist
    s.interactive_pool_id = 0

    # Per-priority "slotting" (smaller containers => more concurrency => lower queueing latency)
    # Request ~= pool.max / slots (then apply hints and caps).
    s.slots_cpu = {
        Priority.QUERY: 8,
        Priority.INTERACTIVE: 6,
        Priority.BATCH_PIPELINE: 2,
    }
    s.slots_ram = {
        Priority.QUERY: 10,
        Priority.INTERACTIVE: 8,
        Priority.BATCH_PIPELINE: 2,
    }
    # Absolute caps to avoid single tasks consuming whole pools (keeps latency stable under load)
    s.cpu_cap = {
        Priority.QUERY: 6.0,
        Priority.INTERACTIVE: 8.0,
        Priority.BATCH_PIPELINE: 32.0,  # effectively "no cap" for typical pool sizes
    }

    # Protect high-priority admission on interactive pool by reserving headroom
    s.reserve_frac_interactive_pool = {
        "cpu": 0.20,
        "ram": 0.20,
    }

    # Safety bounds for per-tick scheduling loops
    s.max_assignments_per_pool_per_tick = 64
    s.max_scan_per_pick = 48


def _norm_priority(pr):
    if pr in (Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE):
        return pr
    return Priority.BATCH_PIPELINE


def _op_key(pipeline_id, op):
    return (pipeline_id, id(op))


def _is_oom_error(err):
    if err is None:
        return False
    msg = str(err).lower()
    return ("oom" in msg) or ("out of memory" in msg) or ("memoryerror" in msg)


def _ensure_pool_state(s, pool_id):
    if pool_id not in s.pool_cycle_idx:
        s.pool_cycle_idx[pool_id] = 0


def _dequeue_next_pipeline_id(s, pr):
    q = s.queues[pr]
    if not q:
        return None
    pid = q.popleft()
    # Remove membership; caller must re-enqueue if still active
    if s.in_queue_prio.get(pid) == pr:
        del s.in_queue_prio[pid]
    return pid


def _enqueue_pipeline_id(s, pid, pr, front=False):
    pr = _norm_priority(pr)
    if pid in s.in_queue_prio:
        return
    if front:
        s.queues[pr].appendleft(pid)
    else:
        s.queues[pr].append(pid)
    s.in_queue_prio[pid] = pr


def _cleanup_pipeline(s, pid):
    # Remove from all tracking; caller should ensure it's not still enqueued
    s.pipeline_by_id.pop(pid, None)
    s.enqueue_tick.pop(pid, None)
    s.last_progress_tick.pop(pid, None)


def _pipeline_is_terminal_failed(s, pipeline):
    """Only treat as terminal if we *observed* a non-OOM failure, or OOM retries exhausted."""
    status = pipeline.runtime_status()
    # If we can't introspect failed ops, be conservative and don't drop.
    try:
        failed_ops = status.get_ops([OperatorState.FAILED], require_parents_complete=False) or []
    except Exception:
        failed_ops = []

    if not failed_ops:
        return False

    pid = pipeline.pipeline_id
    for op in failed_ops:
        k = _op_key(pid, op)
        if k in s.non_oom_failed:
            return True
        if k in s.oom_retriable and s.op_attempts.get(k, 0) > s.max_retries_per_op:
            return True
    return False


def _pick_next_op(s, pipeline):
    """Prefer retriable FAILED ops first (OOM backoff), else PENDING ops."""
    status = pipeline.runtime_status()
    pid = pipeline.pipeline_id

    # Retry failed ops first if they were OOM and still within retry budget
    try:
        failed_ops = status.get_ops([OperatorState.FAILED], require_parents_complete=True) or []
    except Exception:
        failed_ops = []
    for op in failed_ops:
        k = _op_key(pid, op)
        if k in s.oom_retriable and s.op_attempts.get(k, 0) <= s.max_retries_per_op:
            return op

    # Otherwise schedule pending ops
    try:
        pending_ops = status.get_ops([OperatorState.PENDING], require_parents_complete=True) or []
    except Exception:
        pending_ops = []
    if pending_ops:
        return pending_ops[0]

    # Fallback: anything assignable
    ops = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True) or []
    return ops[0] if ops else None


def _compute_request(s, pool, pr, pid, op):
    """Compute conservative request sizes for better concurrency; apply OOM hints if present."""
    pr = _norm_priority(pr)

    # Slot-based baseline (keeps many small tasks flowing to reduce queueing)
    slots_cpu = float(max(1, s.slots_cpu.get(pr, 4)))
    slots_ram = float(max(1, s.slots_ram.get(pr, 4)))

    cpu = max(1.0, float(pool.max_cpu_pool) / slots_cpu)
    ram = max(1.0, float(pool.max_ram_pool) / slots_ram)

    # Cap CPU for high-priority to avoid a single query consuming whole pool (improves tail under load)
    cpu = min(cpu, float(s.cpu_cap.get(pr, cpu)))

    # Apply learned hints (mostly for OOM retries)
    k = (pid, id(op))
    hint = s.op_hints.get(k)
    if hint:
        try:
            cpu = max(cpu, float(hint.get("cpu", cpu)))
        except Exception:
            pass
        try:
            ram = max(ram, float(hint.get("ram", ram)))
        except Exception:
            pass

    # If this is a known OOM-retriable op, add a small safety margin to reduce repeated OOM churn
    if k in s.oom_retriable:
        ram = ram * 1.10

    # Cap by pool maxima (availability handled by caller's fit check)
    cpu = min(cpu, float(pool.max_cpu_pool))
    ram = min(ram, float(pool.max_ram_pool))

    # Ensure positive
    cpu = max(1.0, cpu)
    ram = max(1.0, ram)
    return cpu, ram


def _would_violate_headroom(s, pool, pool_id, pr, avail_cpu_after, avail_ram_after):
    """Reserve headroom on the interactive pool to keep query/interactive latency low."""
    if s.executor.num_pools <= 1:
        return False
    if pool_id != s.interactive_pool_id:
        return False
    if pr != Priority.BATCH_PIPELINE:
        return False

    reserve_cpu = float(pool.max_cpu_pool) * float(s.reserve_frac_interactive_pool.get("cpu", 0.0))
    reserve_ram = float(pool.max_ram_pool) * float(s.reserve_frac_interactive_pool.get("ram", 0.0))
    return (avail_cpu_after < reserve_cpu) or (avail_ram_after < reserve_ram)


def _priority_order_for_pool(s, pool_id, pool):
    """Soft partition: on non-interactive pools, try batch first to ensure progress, else fall back."""
    if s.executor.num_pools <= 1:
        return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]
    if pool_id == s.interactive_pool_id:
        return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]
    # If batch exists, it should primarily use non-interactive pools
    return [Priority.BATCH_PIPELINE, Priority.INTERACTIVE, Priority.QUERY]


def _pick_priority_by_cycle(s, pool_id, fallback_order):
    """Weighted service: pick next priority from a cycle, but respect pool's preferred ordering."""
    _ensure_pool_state(s, pool_id)
    idx = s.pool_cycle_idx[pool_id]
    s.pool_cycle_idx[pool_id] = (idx + 1) % len(s.service_cycle)
    cyc_pr = s.service_cycle[idx]

    # If the cycled priority is in fallback_order (it is), try it first; otherwise use fallback order
    if cyc_pr in fallback_order:
        return [cyc_pr] + [p for p in fallback_order if p != cyc_pr]
    return list(fallback_order)


@register_scheduler(key="scheduler_low_011_r20")
def scheduler_low_011_r20(s, results, pipelines):
    """
    Priority-aware, greedy multi-assignment scheduler with:
    - higher parallelism (multiple assignments per pool per tick),
    - starvation avoidance (weighted service cycle),
    - soft pool partitioning with interactive headroom,
    - OOM-aware RAM backoff retries.
    """
    s.tick += 1

    # Register new pipelines (unique enqueue)
    for p in pipelines:
        pid = p.pipeline_id
        s.pipeline_by_id[pid] = p
        if pid not in s.enqueue_tick:
            s.enqueue_tick[pid] = s.tick
            s.last_progress_tick[pid] = s.tick
        pr = _norm_priority(p.priority)
        _enqueue_pipeline_id(s, pid, pr, front=False)

    # Process results: learn OOM hints and mark progress
    for r in results:
        ops = getattr(r, "ops", None) or []
        is_failed = False
        try:
            is_failed = bool(r.failed())
        except Exception:
            is_failed = getattr(r, "error", None) is not None

        oom = _is_oom_error(getattr(r, "error", None)) if is_failed else False

        for op in ops:
            op_id = id(op)
            pid = s.op_to_pipeline.pop(op_id, None)  # consume mapping to avoid unbounded growth
            if pid is None:
                continue

            # Update pipeline progress time on success
            if not is_failed:
                s.last_progress_tick[pid] = s.tick
                continue

            k = (pid, op_id)

            if oom:
                s.oom_retriable.add(k)
                s.op_attempts[k] = int(s.op_attempts.get(k, 0)) + 1

                # Exponential RAM backoff from observed allocation (best-effort); keep CPU at least prior allocation
                prev = s.op_hints.get(k, {})
                prev_ram = float(prev.get("ram", getattr(r, "ram", 1.0) or 1.0))
                prev_cpu = float(prev.get("cpu", getattr(r, "cpu", 1.0) or 1.0))
                base_ram = float(getattr(r, "ram", prev_ram) or prev_ram or 1.0)
                new_ram = max(prev_ram, base_ram) * 2.0
                new_cpu = max(1.0, prev_cpu)

                s.op_hints[k] = {"ram": max(1.0, new_ram), "cpu": new_cpu}

                # Re-enqueue the pipeline to the front of its priority queue to retry sooner
                p = s.pipeline_by_id.get(pid)
                if p is not None:
                    pr = _norm_priority(p.priority)
                    _enqueue_pipeline_id(s, pid, pr, front=True)
            else:
                # Mark as terminally failed (we won't intentionally keep retrying these)
                s.non_oom_failed.add(k)

    # Early exit if no decision-impacting changes
    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    # Greedy packing per pool
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)
        if avail_cpu <= 0.0 or avail_ram <= 0.0:
            continue

        base_order = _priority_order_for_pool(s, pool_id, pool)
        pr_try_order = _pick_priority_by_cycle(s, pool_id, base_order)

        made = 0
        no_progress_iters = 0

        # Keep placing work while resources remain
        while made < s.max_assignments_per_pool_per_tick and avail_cpu >= 1.0 and avail_ram >= 1.0:
            scheduled_any = False

            # Try priorities in (cycle-adjusted) order
            for pr in pr_try_order:
                q = s.queues[pr]
                if not q:
                    continue

                # Scan a bounded number of pipelines to find a runnable op that fits
                scans = 0
                while scans < s.max_scan_per_pick and q:
                    scans += 1
                    pid = _dequeue_next_pipeline_id(s, pr)
                    if pid is None:
                        break

                    p = s.pipeline_by_id.get(pid)
                    if p is None:
                        # Pipeline disappeared; skip
                        continue

                    status = p.runtime_status()

                    # Drop completed pipelines
                    if status.is_pipeline_successful():
                        _cleanup_pipeline(s, pid)
                        continue

                    # Drop terminal failures (only when we observed non-OOM fail or OOM retries exhausted)
                    if _pipeline_is_terminal_failed(s, p):
                        _cleanup_pipeline(s, pid)
                        continue

                    # Aging guardrail: if something hasn't made progress in a while, don't let it starve forever.
                    # (Promote by choosing "pr_eff" for sizing only; queue priority remains the same.)
                    waited = int(s.tick - int(s.last_progress_tick.get(pid, s.enqueue_tick.get(pid, s.tick))))
                    pr_eff = pr
                    if pr == Priority.BATCH_PIPELINE and waited >= 80:
                        pr_eff = Priority.INTERACTIVE
                    if pr in (Priority.BATCH_PIPELINE, Priority.INTERACTIVE) and waited >= 160:
                        pr_eff = Priority.QUERY

                    op = _pick_next_op(s, p)
                    if op is None:
                        # Not ready yet; rotate to back
                        _enqueue_pipeline_id(s, pid, pr, front=False)
                        continue

                    cpu_req, ram_req = _compute_request(s, pool, pr_eff, pid, op)

                    # Fit check against local availability (accounts for assignments in this tick)
                    if cpu_req > avail_cpu or ram_req > avail_ram:
                        _enqueue_pipeline_id(s, pid, pr, front=False)
                        continue

                    # Headroom protection (avoid batch taking the interactive pool)
                    if _would_violate_headroom(
                        s, pool, pool_id, pr,
                        avail_cpu_after=(avail_cpu - cpu_req),
                        avail_ram_after=(avail_ram - ram_req),
                    ):
                        _enqueue_pipeline_id(s, pid, pr, front=False)
                        continue

                    # Soft placement: on non-interactive pools, avoid pulling queries if there's batch/interactive waiting,
                    # but only if we can still make progress with other priorities.
                    if (
                        s.executor.num_pools > 1
                        and pool_id != s.interactive_pool_id
                        and pr == Priority.QUERY
                        and (len(s.queues[Priority.BATCH_PIPELINE]) > 0 or len(s.queues[Priority.INTERACTIVE]) > 0)
                    ):
                        # Rotate query to back and try other work for this pool
                        _enqueue_pipeline_id(s, pid, pr, front=False)
                        break

                    # Schedule it
                    assignments.append(
                        Assignment(
                            ops=[op],
                            cpu=cpu_req,
                            ram=ram_req,
                            priority=pr,
                            pool_id=pool_id,
                            pipeline_id=pid,
                        )
                    )

                    # Track op -> pipeline to interpret future results
                    s.op_to_pipeline[id(op)] = pid

                    # Re-enqueue pipeline for subsequent operators
                    _enqueue_pipeline_id(s, pid, pr, front=False)

                    avail_cpu -= cpu_req
                    avail_ram -= ram_req
                    made += 1
                    scheduled_any = True
                    break  # move to next placement attempt (recompute priority order via cycle next loop)

                if scheduled_any:
                    break

            if scheduled_any:
                no_progress_iters = 0
                # Advance the per-pool cycle for the next placement attempt to improve fairness within the same tick
                pr_try_order = _pick_priority_by_cycle(s, pool_id, base_order)
                continue

            # Couldn't place anything this iteration
            no_progress_iters += 1
            if no_progress_iters >= 2:
                # Likely fragmentation/fit issues; stop trying for this pool this tick
                break

    return suspensions, assignments