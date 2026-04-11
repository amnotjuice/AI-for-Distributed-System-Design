# policy_key: scheduler_low_021
# reasoning_effort: low
# model: gpt-5.2-2025-12-11
# llm_cost: 0.052194
# generation_seconds: 50.48
# generated_at: 2026-03-14T02:18:19.639232
@register_scheduler_init(key="scheduler_low_021")
def scheduler_low_021_init(s):
    """Priority-aware FIFO with simple right-sizing and starvation avoidance.

    Improvements over naive FIFO:
    - Maintain separate waiting queues per priority and always prefer higher priority work.
    - Avoid head-of-line blocking by scanning queues for a pipeline that has a runnable operator.
    - Allocate only a *slice* of pool resources per op (by priority), instead of giving all resources
      to the first runnable op. This improves latency for concurrent interactive/query workloads.
    - On operator failure that looks like OOM, increase a per-(pipeline, op) RAM multiplier and retry.

    Intentional simplicity:
    - No active preemption (no reliable API for enumerating running containers in the template).
    - Conservative multi-assign loop per pool to fill remaining capacity with small slices.
    """
    s.waiting_by_prio = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }
    # Track all pipelines we've seen so we can re-enqueue them when needed.
    s.known_pipelines = {}  # pipeline_id -> Pipeline

    # Simple RAM backoff on suspected OOM: (pipeline_id, op_key) -> multiplier
    s.ram_mult = {}

    # Round-robin cursor per priority to avoid starving within a class.
    s.rr_cursor = {
        Priority.QUERY: 0,
        Priority.INTERACTIVE: 0,
        Priority.BATCH_PIPELINE: 0,
    }

    # Priority slices of a pool's total capacity (soft targets; can borrow if idle).
    s.prio_cpu_share = {
        Priority.QUERY: 0.60,
        Priority.INTERACTIVE: 0.30,
        Priority.BATCH_PIPELINE: 0.10,
    }
    s.prio_ram_share = {
        Priority.QUERY: 0.60,
        Priority.INTERACTIVE: 0.30,
        Priority.BATCH_PIPELINE: 0.10,
    }

    # Starvation avoidance: after this many scheduler ticks waiting, allow a batch op to borrow.
    s.tick = 0
    s.enqueue_tick = {}  # pipeline_id -> tick first enqueued (approx)
    s.batch_aging_ticks = 25


def _op_key(op):
    # Best-effort stable key for an operator across retries.
    for attr in ("op_id", "operator_id", "id", "name"):
        if hasattr(op, attr):
            return getattr(op, attr)
    return str(op)


def _looks_like_oom(err):
    if err is None:
        return False
    try:
        msg = str(err).lower()
    except Exception:
        return False
    return ("oom" in msg) or ("out of memory" in msg) or ("memory" in msg and "killed" in msg)


def _highest_waiting_prio(s):
    for pr in (Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE):
        if s.waiting_by_prio[pr]:
            return pr
    return None


def _has_waiting_higher_than(s, prio):
    order = [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]
    try:
        idx = order.index(prio)
    except ValueError:
        return False
    for higher in order[:idx]:
        if s.waiting_by_prio[higher]:
            return True
    return False


def _rotate_and_find_runnable(s, prio):
    """Return (pipeline, op_list) for the next runnable pipeline in this priority queue, else (None, None).

    We scan the queue once (with a rotating cursor) to avoid head-of-line blocking.
    """
    q = s.waiting_by_prio[prio]
    n = len(q)
    if n == 0:
        return None, None

    # Cursor-based scan to give fairness within same priority.
    start = s.rr_cursor[prio] % n
    for k in range(n):
        i = (start + k) % n
        p = q[i]
        status = p.runtime_status()

        # Drop completed pipelines.
        if status.is_pipeline_successful():
            q.pop(i)
            # Cursor stays valid by pointing at same index.
            if i < start:
                start -= 1
            n -= 1
            if n == 0:
                s.rr_cursor[prio] = 0
                return None, None
            continue

        # Find a runnable operator (parents complete).
        op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        if op_list:
            # Update cursor to start *after* this pipeline next time.
            s.rr_cursor[prio] = (i + 1) % len(q)
            return p, op_list

    return None, None


def _maybe_reenqueue(s, pipeline):
    """Ensure pipeline is present in its priority queue exactly once."""
    prio = pipeline.priority
    q = s.waiting_by_prio[prio]
    pid = pipeline.pipeline_id

    if pid not in s.known_pipelines:
        s.known_pipelines[pid] = pipeline
    if pid not in s.enqueue_tick:
        s.enqueue_tick[pid] = s.tick

    # Avoid duplicates: only add if not already present.
    for p in q:
        if p.pipeline_id == pid:
            return
    q.append(pipeline)


@register_scheduler(key="scheduler_low_021")
def scheduler_low_021(s, results: List[ExecutionResult], pipelines: List[Pipeline]) -> Tuple[List[Suspend], List[Assignment]]:
    """
    Policy: priority-aware, slice-based multi-assignment per pool, with simple OOM RAM backoff.

    Per tick:
    - Incorporate new pipelines into priority queues.
    - Process results: on suspected OOM, increase RAM multiplier for that operator.
    - For each pool, repeatedly:
        - pick highest priority queue with runnable ops (with batch aging),
        - allocate a target slice of cpu/ram (by priority),
        - apply RAM multiplier (if any) for that op,
        - assign one op at a time until pool capacity is effectively exhausted.
    """
    s.tick += 1

    # 1) Add new pipelines.
    for p in pipelines:
        _maybe_reenqueue(s, p)

    # 2) Process results: if an op failed (especially OOM), bump RAM multiplier and re-enqueue pipeline.
    for r in results:
        try:
            failed = r.failed()
        except Exception:
            failed = False

        # Re-enqueue the owning pipeline to keep it moving after a result arrives.
        pid = getattr(r, "pipeline_id", None)
        if pid is None:
            # Fall back: find by scanning known pipelines (best-effort; might be absent).
            pass
        else:
            if pid in s.known_pipelines:
                _maybe_reenqueue(s, s.known_pipelines[pid])

        if failed and _looks_like_oom(getattr(r, "error", None)):
            # Increase RAM multiplier for each failed op in this result.
            for op in getattr(r, "ops", []) or []:
                opk = _op_key(op)
                # Best-effort pipeline id: prefer result.pipeline_id, else op may carry it.
                pp = pid
                if pp is None and hasattr(op, "pipeline_id"):
                    pp = getattr(op, "pipeline_id")
                if pp is None:
                    continue
                key = (pp, opk)
                cur = s.ram_mult.get(key, 1.0)
                # Exponential-ish backoff, capped.
                nxt = min(8.0, max(cur * 1.5, cur + 0.5))
                s.ram_mult[key] = nxt

    # Early exit if nothing to do.
    if not pipelines and not results:
        # We still might have queued work, so don't early-exit unless all queues empty.
        if not any(s.waiting_by_prio[p] for p in s.waiting_by_prio):
            return [], []

    suspensions: List[Suspend] = []
    assignments: List[Assignment] = []

    # 3) For each pool, place work with priority and slices.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool
        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # We'll keep assigning until remaining resources are too small to be useful.
        # (We don't know min CPU/RAM required, so we use simple thresholds.)
        min_cpu_chunk = max(1, int(0.05 * pool.max_cpu_pool))
        min_ram_chunk = max(1, int(0.05 * pool.max_ram_pool))

        # Local loop budget prevents pathologically long scans.
        loop_budget = 32

        while loop_budget > 0 and avail_cpu >= 1 and avail_ram >= 1:
            loop_budget -= 1

            # Batch aging: if batch has waited "long enough" and there is spare, let it borrow.
            oldest_batch_wait = None
            if s.waiting_by_prio[Priority.BATCH_PIPELINE]:
                oldest_batch_wait = min(
                    (s.tick - s.enqueue_tick.get(p.pipeline_id, s.tick))
                    for p in s.waiting_by_prio[Priority.BATCH_PIPELINE]
                )

            # Choose which priority to try first.
            prio_order = [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]
            if oldest_batch_wait is not None and oldest_batch_wait >= s.batch_aging_ticks:
                # If batch is aging, allow it to be considered earlier *if* there is no QUERY waiting.
                if not s.waiting_by_prio[Priority.QUERY]:
                    prio_order = [Priority.INTERACTIVE, Priority.BATCH_PIPELINE, Priority.QUERY]

            chosen = None
            chosen_ops = None
            for pr in prio_order:
                p, op_list = _rotate_and_find_runnable(s, pr)
                if p is not None and op_list:
                    chosen = p
                    chosen_ops = op_list
                    break

            if chosen is None or not chosen_ops:
                break

            # Decide slice sizes.
            pr = chosen.priority
            cpu_target = max(1, int(pool.max_cpu_pool * s.prio_cpu_share.get(pr, 0.25)))
            ram_target = max(1, int(pool.max_ram_pool * s.prio_ram_share.get(pr, 0.25)))

            # Borrowing: if no higher-priority work is waiting, allow using more of what's free.
            if not _has_waiting_higher_than(s, pr):
                cpu_target = max(cpu_target, int(0.75 * avail_cpu))
                ram_target = max(ram_target, int(0.75 * avail_ram))

            # Don't try to place tiny slivers unless nothing else fits.
            if avail_cpu < min_cpu_chunk and avail_ram < min_ram_chunk:
                cpu_target = avail_cpu
                ram_target = avail_ram

            # Apply per-op RAM multiplier (best-effort with op key).
            op0 = chosen_ops[0]
            key = (chosen.pipeline_id, _op_key(op0))
            mult = s.ram_mult.get(key, 1.0)

            req_cpu = min(avail_cpu, max(1, cpu_target))
            req_ram = min(avail_ram, max(1, int(ram_target * mult)))

            # Final guard: if we can't allocate at least 1 unit each, stop.
            if req_cpu <= 0 or req_ram <= 0:
                break

            # Assign only one operator at a time (keeps simulator predictable, reduces OOM blast radius).
            assignment = Assignment(
                ops=[op0],
                cpu=req_cpu,
                ram=req_ram,
                priority=chosen.priority,
                pool_id=pool_id,
                pipeline_id=chosen.pipeline_id,
            )
            assignments.append(assignment)

            # Update local availability estimates.
            avail_cpu -= req_cpu
            avail_ram -= req_ram

            # Keep pipeline enqueued for future ops; do not remove it here.
            # Reset enqueue tick when it gets scheduled to reflect progress and reduce "aging" bias.
            s.enqueue_tick[chosen.pipeline_id] = s.tick

    return suspensions, assignments
