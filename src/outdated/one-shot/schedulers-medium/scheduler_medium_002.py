# policy_key: scheduler_medium_002
# reasoning_effort: medium
# model: gpt-5.2-2025-12-11
# llm_cost: 0.102972
# generation_seconds: 96.86
# generated_at: 2026-03-12T21:53:30.456224
@register_scheduler_init(key="scheduler_medium_002")
def scheduler_medium_002_init(s):
    """
    Priority-aware, retry-on-OOM, small-slice packing scheduler.

    Improvements over naive FIFO:
    - Separate per-priority queues (QUERY > INTERACTIVE > BATCH_PIPELINE).
    - Avoid "give the whole pool to one op": allocate capped CPU/RAM slices to improve concurrency/latency.
    - Detect OOM failures and retry the same operator with exponential RAM backoff (bounded).
    - Weighted round-robin across priorities to reduce starvation while protecting high-priority latency.
    - At most one assignment per pipeline per scheduler tick to avoid duplicate assignment races.
    """
    from collections import deque

    s.tick = 0

    # Per-priority pipeline queues (store pipeline_id only).
    s.queues = {
        Priority.QUERY: deque(),
        Priority.INTERACTIVE: deque(),
        Priority.BATCH_PIPELINE: deque(),
    }

    # Pipeline registry / dedupe.
    s.pipeline_by_id = {}
    s.enqueued = set()  # pipeline_ids currently in any queue
    s.pipeline_enqueue_tick = {}  # pipeline_id -> tick

    # Failure handling / retry tracking keyed by operator identity.
    s.op_ram_mult = {}  # op_key -> multiplier applied to base RAM slice
    s.op_retry = {}  # op_key -> retry count
    s.op_last_error = {}  # op_key -> last error string (best-effort)

    # Map operator identity back to its pipeline_id (since results may not include pipeline_id).
    s.op_to_pipeline = {}

    # If an operator fails for non-OOM reasons, mark pipeline as permanently failed (drop it).
    s.pipeline_failed_permanent = set()

    # Retry policy.
    s.max_oom_retries = 5
    s.max_ram_multiplier = 16.0

    # Weighted RR budget per cycle: protect high priority while ensuring some batch progress.
    s.rr_budget_init = {
        Priority.QUERY: 6,
        Priority.INTERACTIVE: 3,
        Priority.BATCH_PIPELINE: 1,
    }
    s.rr_budget = dict(s.rr_budget_init)


def _op_key_for(op):
    """Best-effort stable operator key without assuming specific operator fields."""
    # Prefer an explicit id if present; otherwise fall back to object identity.
    for attr in ("op_id", "operator_id", "id", "uid", "uuid"):
        try:
            v = getattr(op, attr)
            if v is not None:
                return ("attr", attr, v)
        except Exception:
            pass
    return ("obj", id(op))


def _is_oom_error(err):
    """Heuristic: detect OOM-like failures from an error string."""
    if err is None:
        return False
    try:
        s = str(err).lower()
    except Exception:
        return False
    return ("oom" in s) or ("out of memory" in s) or ("memoryerror" in s) or ("cuda out of memory" in s)


def _reset_rr_budget_if_needed(s):
    if all(v <= 0 for v in s.rr_budget.values()):
        s.rr_budget = dict(s.rr_budget_init)


def _pick_priority(s):
    """
    Pick next priority to schedule using weighted RR, but always in strict priority order
    when budgets allow.
    """
    _reset_rr_budget_if_needed(s)

    # Strict preference order; budgets control how long each class can dominate per cycle.
    order = (Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE)
    for pr in order:
        if s.rr_budget.get(pr, 0) > 0 and s.queues[pr]:
            s.rr_budget[pr] -= 1
            return pr

    # If budgets block progress but queues are non-empty, reset once and retry.
    s.rr_budget = dict(s.rr_budget_init)
    for pr in order:
        if s.rr_budget.get(pr, 0) > 0 and s.queues[pr]:
            s.rr_budget[pr] -= 1
            return pr

    return None


def _cpu_slice_for(pool, priority, avail_cpu):
    """
    Allocate a capped CPU slice to improve concurrency and reduce HOL blocking.
    """
    # Conservative caps for latency-sensitive classes.
    if priority in (Priority.QUERY, Priority.INTERACTIVE):
        cap = 4
        # If pool is small, scale down; if large, keep cap.
        target = max(1, min(cap, int(max(1, pool.max_cpu_pool * 0.25))))
    else:
        cap = 8
        target = max(1, min(cap, int(max(1, pool.max_cpu_pool * 0.50))))

    return min(avail_cpu, target)


def _ram_slice_for(pool, priority, avail_ram):
    """
    Allocate a base RAM slice as a fraction of pool size; op-specific multiplier applied later.
    """
    # Smaller slices for high priority to allow more concurrency; batch can use larger slices.
    if priority == Priority.QUERY:
        frac = 0.12
    elif priority == Priority.INTERACTIVE:
        frac = 0.18
    else:
        frac = 0.30

    base = pool.max_ram_pool * frac

    # Ensure a minimal useful amount if units are small; avoid allocating 0.
    # (We keep it numeric without assuming units; 1 is a reasonable floor in most sims.)
    base = max(1, base)

    return min(avail_ram, base)


@register_scheduler(key="scheduler_medium_002")
def scheduler_medium_002(s, results, pipelines):
    """
    Scheduler step:
    - Enqueue new pipelines by priority.
    - Process execution results; on OOM, increase RAM multiplier and allow retry; on other failure, drop pipeline.
    - For each pool (most headroom first), place small-slice assignments from priority queues using weighted RR.
    """
    s.tick += 1

    suspensions = []
    assignments = []

    # --- Ingest new pipelines (admission into queues) ---
    for p in pipelines:
        s.pipeline_by_id[p.pipeline_id] = p
        if p.pipeline_id not in s.enqueued:
            s.queues[p.priority].append(p.pipeline_id)
            s.enqueued.add(p.pipeline_id)
            s.pipeline_enqueue_tick[p.pipeline_id] = s.tick

    # --- Process results: update OOM backoff / permanent failure marking ---
    for r in results:
        # Track per-op outcomes.
        is_fail = False
        try:
            is_fail = r.failed()
        except Exception:
            # If simulator doesn't provide failed(), treat error presence as failure.
            is_fail = getattr(r, "error", None) is not None

        if not is_fail:
            continue

        err = getattr(r, "error", None)
        oom = _is_oom_error(err)

        # Results can include multiple ops (we still treat each independently for sizing).
        for op in getattr(r, "ops", []) or []:
            ok = _op_key_for(op)
            s.op_last_error[ok] = str(err) if err is not None else None

            # Try to recover pipeline_id from our mapping.
            pid = s.op_to_pipeline.get(ok, None)

            if oom:
                s.op_retry[ok] = s.op_retry.get(ok, 0) + 1
                # Exponential backoff on RAM; bounded.
                cur = s.op_ram_mult.get(ok, 1.0)
                nxt = min(s.max_ram_multiplier, cur * 2.0)
                s.op_ram_mult[ok] = nxt

                # If it keeps OOMing even at high multipliers, stop retrying and drop pipeline.
                if s.op_retry[ok] > s.max_oom_retries and pid is not None:
                    s.pipeline_failed_permanent.add(pid)
            else:
                # Non-OOM failures: do not retry (prevents infinite loops on bad code).
                if pid is not None:
                    s.pipeline_failed_permanent.add(pid)

    # Fast path: if nothing changed and no queued work, do nothing.
    if not pipelines and not results and all(len(q) == 0 for q in s.queues.values()):
        return suspensions, assignments

    # --- Scheduling loop ---
    # Sort pools by available headroom to place latency-sensitive tasks on the best pool first.
    pool_ids = list(range(s.executor.num_pools))
    pool_ids.sort(
        key=lambda i: (s.executor.pools[i].avail_cpu_pool, s.executor.pools[i].avail_ram_pool),
        reverse=True,
    )

    scheduled_pipelines_this_tick = set()

    for pool_id in pool_ids:
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool

        # Keep packing small slices while the pool has headroom.
        # Stop if we can't schedule anything meaningful.
        no_progress_iters = 0
        while avail_cpu >= 1 and avail_ram > 0:
            pr = _pick_priority(s)
            if pr is None:
                break

            # Nothing to run at this priority.
            if not s.queues[pr]:
                no_progress_iters += 1
                if no_progress_iters >= 3:
                    break
                continue

            pid = s.queues[pr].popleft()

            # If pipeline is unknown (shouldn't happen), drop it.
            pipeline = s.pipeline_by_id.get(pid, None)
            if pipeline is None:
                s.enqueued.discard(pid)
                continue

            # If pipeline is permanently failed, drop it.
            if pid in s.pipeline_failed_permanent:
                s.enqueued.discard(pid)
                continue

            # If already scheduled a task for this pipeline this tick, requeue and skip
            # (prevents duplicate assignment of the same op within one tick).
            if pid in scheduled_pipelines_this_tick:
                s.queues[pr].append(pid)
                no_progress_iters += 1
                if no_progress_iters >= 6:
                    break
                continue

            status = pipeline.runtime_status()

            # Drop finished pipelines.
            if status.is_pipeline_successful():
                s.enqueued.discard(pid)
                continue

            # If there are failed ops but this wasn't an OOM-retriable situation, drop.
            try:
                failed_count = status.state_counts.get(OperatorState.FAILED, 0)
            except Exception:
                failed_count = 0
            if failed_count > 0 and pid in s.pipeline_failed_permanent:
                s.enqueued.discard(pid)
                continue

            # Find next runnable op (parents completed).
            op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
            if not op_list:
                # Not ready yet; requeue and keep trying others.
                s.queues[pr].append(pid)
                no_progress_iters += 1
                if no_progress_iters >= 6:
                    break
                continue

            op = op_list[0]
            ok = _op_key_for(op)
            s.op_to_pipeline[ok] = pid

            # Size the container.
            cpu = _cpu_slice_for(pool, pr, avail_cpu)
            base_ram = _ram_slice_for(pool, pr, avail_ram)
            mult = s.op_ram_mult.get(ok, 1.0)
            ram = min(avail_ram, max(1, base_ram * mult))

            # If we can't allocate meaningful resources, stop packing this pool.
            if cpu < 1 or ram <= 0:
                s.queues[pr].appendleft(pid)
                break

            assignments.append(
                Assignment(
                    ops=[op],
                    cpu=cpu,
                    ram=ram,
                    priority=pipeline.priority,
                    pool_id=pool_id,
                    pipeline_id=pipeline.pipeline_id,
                )
            )

            scheduled_pipelines_this_tick.add(pid)
            no_progress_iters = 0

            # Update local headroom for this pool (to allow multiple placements per tick).
            avail_cpu -= cpu
            avail_ram -= ram

            # Requeue pipeline for further steps.
            s.queues[pr].append(pid)

    return suspensions, assignments
