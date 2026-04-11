# policy_key: scheduler_high_036
# reasoning_effort: high
# exp: reasoning
# model: gpt-5.2-2025-12-11
# llm_cost: 0.212811
# generation_seconds: 131.99
# generated_at: 2026-04-10T02:55:09.430150
@register_scheduler_init(key="scheduler_high_036")
def scheduler_high_036_init(s):
    """
    Priority-first + fit-aware + OOM-adaptive scheduler.

    Goals:
      - Minimize weighted latency: prioritize QUERY then INTERACTIVE then BATCH.
      - Avoid OOM-driven failures by learning per-operator RAM needs and retrying with higher RAM.
      - Avoid starvation (incomplete pipelines are heavily penalized) via simple aging:
        after a configurable streak of high-priority placements, force a batch placement when possible.
      - Improve mean latency by preferring pipelines with fewer remaining ops (SRPT-ish) within a priority.
      - Use pool "soft partitioning" when multiple pools exist: pool0 prefers high-priority; other pools prefer batch.
    """
    s.clock = 0

    # Per-priority FIFO lists of Pipeline objects (we scan for best-fit each tick).
    s.queues = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }
    s.enqueued_pipeline_ids = set()

    # Map operator identity -> owning pipeline_id (lets us attribute failures to pipelines).
    s.op_to_pipeline_id = {}

    # Learned per-operator RAM estimate (in whatever units the simulator uses).
    s.op_ram_est = {}          # op_key -> est_ram
    s.op_oom_retries = {}      # op_key -> count

    # Pipelines we consider terminal (non-OOM errors, or repeated OOM at max RAM).
    s.terminal_pipelines = set()

    # Starvation avoidance.
    s.high_prio_streak = 0
    s.high_prio_streak_limit = 8

    # Per-tick caps to avoid one pipeline taking over a pool in a single scheduling step.
    s.max_parallel_ops_per_pipeline_per_tick = 2
    s.max_assignments_per_pool_per_tick = 6


def _op_key(op):
    # Prefer stable IDs if present; otherwise fall back to object identity.
    for attr in ("op_id", "operator_id", "id", "uid", "name"):
        if hasattr(op, attr):
            v = getattr(op, attr)
            # Avoid unhashable values.
            if isinstance(v, (int, str)):
                return (attr, v)
    return ("pyid", id(op))


def _safe_len(x, default=0):
    try:
        return len(x)
    except Exception:
        return default


def _priority_order_for_pool(s, pool_id):
    # Soft partitioning:
    # - If multiple pools exist, pool0 prefers high-priority; other pools prefer batch.
    if s.executor.num_pools >= 2 and pool_id == 0:
        return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]
    if s.executor.num_pools >= 2 and pool_id != 0:
        return [Priority.BATCH_PIPELINE, Priority.INTERACTIVE, Priority.QUERY]
    # Single pool: strict priority (with aging handled separately).
    return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]


def _is_oom_error(err):
    if err is None:
        return False
    msg = str(err).lower()
    # Keep broad to match simulator error strings.
    return ("oom" in msg) or ("out of memory" in msg) or ("cuda out of memory" in msg) or ("memoryerror" in msg)


def _op_min_ram_guess(op):
    # Try common attribute names used in simulators.
    for attr in ("min_ram", "ram_min", "min_memory", "memory_min", "peak_ram", "ram"):
        if hasattr(op, attr):
            try:
                v = float(getattr(op, attr))
                if v > 0:
                    return v
            except Exception:
                pass
    return None


def _estimate_op_ram(s, op, pool):
    k = _op_key(op)
    est = s.op_ram_est.get(k)
    if est is not None and est > 0:
        return est

    min_ram = _op_min_ram_guess(op)
    if min_ram is not None:
        # Small safety margin to reduce first-try OOMs without wasting too much memory.
        return min(min_ram * 1.15, pool.max_ram_pool)

    # Fallback: small slice of the pool, but not too tiny.
    # (Choosing too small causes OOM thrash; too large kills concurrency.)
    try:
        return min(max(1.0, 0.10 * float(pool.max_ram_pool)), float(pool.max_ram_pool))
    except Exception:
        return 1.0


def _target_cpu(priority, pool, avail_cpu):
    # Give more CPU to high-priority to reduce latency, but keep enough concurrency.
    try:
        max_cpu = float(pool.max_cpu_pool)
    except Exception:
        max_cpu = float(avail_cpu) if avail_cpu > 0 else 1.0

    if priority == Priority.QUERY:
        frac = 0.50
        cap = 8.0
    elif priority == Priority.INTERACTIVE:
        frac = 0.35
        cap = 6.0
    else:
        frac = 0.25
        cap = 4.0

    target = max(1.0, min(cap, max_cpu * frac))
    # If pool is mostly idle, allow a bit more for high-priority.
    if priority in (Priority.QUERY, Priority.INTERACTIVE) and avail_cpu >= 2 * target:
        target = min(cap, target * 1.25)

    return max(1.0, min(float(avail_cpu), target))


def _pipeline_remaining_ops(pipeline):
    status = pipeline.runtime_status()
    completed = 0
    try:
        completed = int(status.state_counts.get(OperatorState.COMPLETED, 0))
    except Exception:
        try:
            completed = int(status.state_counts[OperatorState.COMPLETED])
        except Exception:
            completed = 0

    total = _safe_len(getattr(pipeline, "values", []), default=completed + 1)
    remaining = max(1, total - completed)
    return remaining


def _get_ready_ops(pipeline):
    status = pipeline.runtime_status()
    try:
        return status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
    except Exception:
        # If require_parents_complete isn't supported, fall back.
        return status.get_ops(ASSIGNABLE_STATES)


def _best_fit_ready_op(s, pipeline, pool, avail_ram):
    # Choose among ready ops the one that fits and uses least RAM estimate.
    ready = _get_ready_ops(pipeline)
    if not ready:
        return None

    best = None
    best_est = None
    for op in ready:
        est = _estimate_op_ram(s, op, pool)
        # Can't possibly run on this pool if it exceeds the pool max.
        if est > pool.max_ram_pool:
            continue
        if est <= avail_ram:
            if best is None or est < best_est:
                best = op
                best_est = est

    # If none fit by estimate, don't pick one (avoid dead-alloc attempts).
    return best


def _pick_pipeline_and_op(s, priority, pool, avail_ram, assigned_this_tick):
    # SRPT-ish: choose pipeline with smallest remaining ops among those with a fit-ready op.
    best_pipeline = None
    best_op = None
    best_remaining = None

    q = s.queues.get(priority, [])
    for pipeline in q:
        if pipeline.pipeline_id in s.terminal_pipelines:
            continue

        status = pipeline.runtime_status()
        if status.is_pipeline_successful():
            continue

        # Per-tick cap on parallelism per pipeline.
        if assigned_this_tick.get(pipeline.pipeline_id, 0) >= s.max_parallel_ops_per_pipeline_per_tick:
            continue

        op = _best_fit_ready_op(s, pipeline, pool, avail_ram)
        if op is None:
            continue

        rem = _pipeline_remaining_ops(pipeline)
        if best_pipeline is None or rem < best_remaining:
            best_pipeline = pipeline
            best_op = op
            best_remaining = rem

    return best_pipeline, best_op


def _rotate_queue_to_end(queue, pipeline):
    # Keep queues roughly fair without expensive re-sorts.
    try:
        i = queue.index(pipeline)
        queue.append(queue.pop(i))
    except Exception:
        pass


@register_scheduler(key="scheduler_high_036")
def scheduler_high_036(s, results: List["ExecutionResult"], pipelines: List["Pipeline"]) -> Tuple[List["Suspend"], List["Assignment"]]:
    s.clock += 1

    # Ingest new pipelines and build operator->pipeline mapping.
    for p in pipelines:
        if p.pipeline_id in s.enqueued_pipeline_ids:
            continue
        s.enqueued_pipeline_ids.add(p.pipeline_id)
        s.queues[p.priority].append(p)

        # Map ops to pipeline for attributing failures.
        try:
            for op in getattr(p, "values", []):
                s.op_to_pipeline_id[_op_key(op)] = p.pipeline_id
        except Exception:
            pass

    # Update learned RAM on failures; mark terminal on non-OOM or impossible OOM.
    if results:
        # Precompute max RAM per pool for "impossible to satisfy" checks.
        try:
            global_max_ram = max(pool.max_ram_pool for pool in s.executor.pools)
        except Exception:
            global_max_ram = None

        for r in results:
            if not r.failed():
                continue

            is_oom = _is_oom_error(getattr(r, "error", None))
            # Attribute to pipeline via first op (or any op) in the result.
            pipeline_id = None
            for op in getattr(r, "ops", []) or []:
                pipeline_id = s.op_to_pipeline_id.get(_op_key(op))
                if pipeline_id is not None:
                    break

            # If we can't attribute, we can still learn op RAM estimates.
            for op in getattr(r, "ops", []) or []:
                k = _op_key(op)
                if is_oom:
                    prev = s.op_ram_est.get(k, 0.0) or 0.0
                    # If r.ram is available, use it as the last attempted size; otherwise double previous.
                    attempted = None
                    try:
                        attempted = float(getattr(r, "ram", None))
                    except Exception:
                        attempted = None

                    if attempted is not None and attempted > 0:
                        new_est = max(prev, attempted * 2.0)
                    else:
                        new_est = max(1.0, prev * 2.0 if prev > 0 else 2.0)

                    s.op_ram_est[k] = new_est
                    s.op_oom_retries[k] = s.op_oom_retries.get(k, 0) + 1

                    # If estimate exceeds any pool's max RAM, it will never succeed.
                    if global_max_ram is not None and new_est > global_max_ram and pipeline_id is not None:
                        s.terminal_pipelines.add(pipeline_id)
                else:
                    # Non-OOM failures are unlikely to be fixed by resizing; treat as terminal to avoid churn.
                    if pipeline_id is not None:
                        s.terminal_pipelines.add(pipeline_id)

    # Queue cleanup: drop successful pipelines; keep others to avoid starvation penalties.
    for prio, q in s.queues.items():
        if not q:
            continue
        new_q = []
        for p in q:
            if p.pipeline_id in s.terminal_pipelines:
                continue
            st = p.runtime_status()
            if st.is_pipeline_successful():
                continue
            new_q.append(p)
        s.queues[prio] = new_q

    # Aging: if we've been scheduling only high-priority for a while, try to schedule a batch next.
    batch_waiting = len(s.queues.get(Priority.BATCH_PIPELINE, [])) > 0
    force_batch = batch_waiting and (s.high_prio_streak >= s.high_prio_streak_limit)

    suspensions: List["Suspend"] = []
    assignments: List["Assignment"] = []

    assigned_this_tick = {}  # pipeline_id -> number of ops assigned this tick

    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)

        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        prio_order = _priority_order_for_pool(s, pool_id)

        # If forcing batch, try batch first on non-dedicated pools; on pool0 only if no high-prio is runnable.
        if force_batch:
            if not (s.executor.num_pools >= 2 and pool_id == 0):
                prio_order = [Priority.BATCH_PIPELINE] + [p for p in prio_order if p != Priority.BATCH_PIPELINE]

        num_assigned = 0
        while num_assigned < s.max_assignments_per_pool_per_tick and avail_cpu > 0 and avail_ram > 0:
            chosen_pipeline = None
            chosen_op = None
            chosen_prio = None

            # Find the best (pipeline, op) among allowed priorities for this pool.
            for prio in prio_order:
                # Hard rule: on dedicated pool0, do not run batch if any high-priority exists (reduces query/interactive latency).
                if s.executor.num_pools >= 2 and pool_id == 0 and prio == Priority.BATCH_PIPELINE:
                    if s.queues.get(Priority.QUERY) or s.queues.get(Priority.INTERACTIVE):
                        continue

                p, op = _pick_pipeline_and_op(s, prio, pool, avail_ram, assigned_this_tick)
                if p is not None and op is not None:
                    chosen_pipeline, chosen_op, chosen_prio = p, op, prio
                    break

            if chosen_pipeline is None:
                break

            # Size container.
            est_ram = _estimate_op_ram(s, chosen_op, pool)
            # Conservative extra margin for first attempt; after OOMs, use estimate directly (already inflated).
            k = _op_key(chosen_op)
            oom_retries = s.op_oom_retries.get(k, 0)

            ram = est_ram * (1.10 if oom_retries == 0 else 1.00)
            ram = max(1.0, min(float(pool.max_ram_pool), float(avail_ram), float(ram)))

            # If even our minimum allocation can't fit, stop trying on this pool for now.
            if ram <= 0 or ram > avail_ram:
                break

            cpu = _target_cpu(chosen_prio, pool, avail_cpu)
            cpu = max(1.0, min(float(avail_cpu), float(cpu)))

            # If CPU can't fit, stop.
            if cpu <= 0 or cpu > avail_cpu:
                break

            # Assign exactly one op per container for clearer RAM learning and less correlated failure.
            assignment = Assignment(
                ops=[chosen_op],
                cpu=cpu,
                ram=ram,
                priority=chosen_pipeline.priority,
                pool_id=pool_id,
                pipeline_id=chosen_pipeline.pipeline_id,
            )
            assignments.append(assignment)

            # Update local accounting.
            avail_cpu -= cpu
            avail_ram -= ram
            num_assigned += 1
            assigned_this_tick[chosen_pipeline.pipeline_id] = assigned_this_tick.get(chosen_pipeline.pipeline_id, 0) + 1

            # Update streak for aging.
            if chosen_prio in (Priority.QUERY, Priority.INTERACTIVE):
                s.high_prio_streak += 1
            else:
                s.high_prio_streak = 0
                force_batch = False  # satisfied the batch nudge

            # Rotate within its priority queue for fairness.
            _rotate_queue_to_end(s.queues[chosen_prio], chosen_pipeline)

    return suspensions, assignments
