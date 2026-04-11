# policy_key: scheduler_high_003
# reasoning_effort: high
# exp: reasoning
# model: gpt-5.2-2025-12-11
# llm_cost: 0.145345
# generation_seconds: 165.49
# generated_at: 2026-04-10T00:20:47.228114
@register_scheduler_init(key="scheduler_high_003")
def scheduler_high_003_init(s):
    """Priority-first + OOM-aware retry scheduler.

    Main ideas:
      - Strictly protect QUERY and INTERACTIVE latency (dominant in objective).
      - Avoid pipeline failures by learning per-operator RAM hints from OOMs and retrying with more RAM.
      - Prevent batch starvation via a simple starvation counter (rarely lets batch run even under load).
      - If multiple pools exist: pool 0 is treated as "high-priority preferred"; other pools are mixed with
        soft reservation for high-priority headroom (non-preemptive).
      - Safety: at most one operator per pipeline per scheduling tick to avoid duplicate assignment when
        runtime statuses don't reflect same-tick assignments yet.
    """
    s.queues = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }

    # Per-operator learned sizing (keyed by id(op) because op objects persist in the sim).
    s.op_ram_hint = {}       # op_id -> ram
    s.op_attempts = {}       # op_id -> attempts (all failures)
    s.op_oom_attempts = {}   # op_id -> OOM-specific attempts

    # Map op_id -> pipeline_id so results can attribute failures to pipelines.
    s.op_to_pipeline = {}

    # Pipelines we stop retrying (e.g., repeated non-OOM failures or impossible RAM requirement).
    s.pipeline_fatal = set()

    # Prefer retried pipelines (e.g., after OOM) to reduce end-to-end latency impact.
    s.boost_pipelines = set()

    # Batch starvation prevention.
    s.batch_starvation = 0
    s.batch_starvation_threshold = 30  # ticks without scheduling any batch while batch runnable exists

    # Mild safeguards for endless OOM retries.
    s.max_oom_retries_per_op = 4
    s.max_total_failures_per_op = 6


def _priority_order():
    # Strict priority: QUERY > INTERACTIVE > BATCH (batch is admitted via starvation escape hatch).
    return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]


def _is_oom_error(err):
    if not err:
        return False
    e = str(err).lower()
    return ("oom" in e) or ("out of memory" in e) or ("cuda out of memory" in e)


def _max_ram_all_pools(executor):
    m = 0
    for i in range(executor.num_pools):
        m = max(m, executor.pools[i].max_ram_pool)
    return m


def _base_cpu_for_priority(pool, prio, avail_cpu):
    # Allocate enough CPU to help latency for high-priority, but cap to avoid monopolizing.
    # (Sublinear scaling means "all CPU" is often wasteful; better to keep concurrency.)
    max_cpu = pool.max_cpu_pool

    if prio == Priority.QUERY:
        frac = 0.60
        cap = 8.0
        floor = 1.0
    elif prio == Priority.INTERACTIVE:
        frac = 0.45
        cap = 6.0
        floor = 1.0
    else:  # batch
        frac = 0.25
        cap = 4.0
        floor = 1.0

    target = min(cap, max_cpu * frac)
    cpu = max(floor, min(avail_cpu, target))
    return cpu


def _base_ram_for_priority(pool, prio):
    # Start moderately small to keep concurrency; learn upward from OOMs.
    max_ram = pool.max_ram_pool
    if prio == Priority.QUERY:
        frac = 0.10
        floor_frac = 0.03
    elif prio == Priority.INTERACTIVE:
        frac = 0.08
        floor_frac = 0.03
    else:
        frac = 0.06
        floor_frac = 0.02

    base = max(max_ram * floor_frac, max_ram * frac)
    return base


def _effective_ram(pool, prio, avail_ram, hint):
    base = _base_ram_for_priority(pool, prio)
    ram = max(base, hint or 0)
    ram = min(ram, avail_ram)
    # Ensure we don't allocate zero RAM if simulator uses floats.
    if ram <= 0:
        ram = min(avail_ram, pool.max_ram_pool)
    return ram


def _get_first_runnable_op(pipeline):
    status = pipeline.runtime_status()
    if status.is_pipeline_successful():
        return None
    op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
    if not op_list:
        return None
    return op_list[0]


def _queue_has_runnable(queue, scan_limit=6):
    # Cheap check to avoid full scans for reservation decisions.
    n = min(len(queue), scan_limit)
    for i in range(n):
        p = queue[i]
        op = _get_first_runnable_op(p)
        if op is not None:
            return True
    return False


def _pop_next_runnable_from_queue(s, queue, assigned_op_ids, assigned_pipeline_ids):
    # Two-pass selection:
    #   1) boosted pipelines first (typically just OOM-retried) to reduce latency penalty from retries
    #   2) normal FIFO among remaining
    if not queue:
        return None, None

    def scan(prefer_boosted):
        L = len(queue)
        for _ in range(L):
            p = queue.pop(0)

            # Drop completed pipelines eagerly.
            status = p.runtime_status()
            if status.is_pipeline_successful():
                continue

            # Don't keep re-attempting pipelines deemed unschedulable/fatal.
            if p.pipeline_id in s.pipeline_fatal:
                continue

            boosted = (p.pipeline_id in s.boost_pipelines)
            if prefer_boosted and not boosted:
                queue.append(p)
                continue
            if (not prefer_boosted) and boosted:
                # Keep boosted pipelines near the front; they'll be considered in pass 1.
                queue.append(p)
                continue

            op = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
            if not op:
                queue.append(p)
                continue

            op0 = op[0]
            op_id = id(op0)

            # Prevent duplicates within the same scheduling tick.
            if op_id in assigned_op_ids or p.pipeline_id in assigned_pipeline_ids:
                queue.append(p)
                continue

            # Requeue pipeline for future ops; return chosen op.
            queue.append(p)
            return p, op0

        return None, None

    p, op = scan(prefer_boosted=True)
    if p is not None:
        return p, op
    return scan(prefer_boosted=False)


@register_scheduler(key="scheduler_high_003")
def scheduler_high_003(s, results: List["ExecutionResult"], pipelines: List["Pipeline"]) -> Tuple[List["Suspend"], List["Assignment"]]:
    # Enqueue newly arrived pipelines by priority.
    for p in pipelines:
        # If a pipeline arrives already "fatal" (shouldn't happen), ignore it.
        if getattr(p, "pipeline_id", None) in s.pipeline_fatal:
            continue
        s.queues[p.priority].append(p)

    # Update learned hints from execution results.
    if results:
        max_ram_any = _max_ram_all_pools(s.executor)

        for r in results:
            # r.ops is a list of operator objects that ran in that container.
            for op in (r.ops or []):
                op_id = id(op)

                # Always record mapping to attribute failures.
                # (op_to_pipeline is set at assignment time; if missing, we can't blame a pipeline reliably.)
                pipeline_id = s.op_to_pipeline.get(op_id, None)

                if r.failed():
                    s.op_attempts[op_id] = s.op_attempts.get(op_id, 0) + 1

                    err_is_oom = _is_oom_error(getattr(r, "error", None))
                    if err_is_oom:
                        s.op_oom_attempts[op_id] = s.op_oom_attempts.get(op_id, 0) + 1

                        # Increase RAM hint aggressively to reduce repeated OOM latency.
                        prev_hint = s.op_ram_hint.get(op_id, 0)
                        allocated = getattr(r, "ram", 0) or 0
                        # Use the larger of prior hint / allocated as base; grow multiplicatively.
                        base = max(prev_hint, allocated, max_ram_any * 0.02)
                        new_hint = min(max_ram_any, base * 1.8)
                        s.op_ram_hint[op_id] = new_hint

                        if pipeline_id is not None:
                            s.boost_pipelines.add(pipeline_id)

                        # If we already tried near max RAM multiple times, mark as unschedulable.
                        if (s.op_oom_attempts.get(op_id, 0) >= s.max_oom_retries_per_op) and (new_hint >= 0.95 * max_ram_any):
                            if pipeline_id is not None:
                                s.pipeline_fatal.add(pipeline_id)

                    else:
                        # Non-OOM failures are often deterministic; avoid infinite retries that harm score.
                        # We allow a couple failures, but then stop spending resources.
                        if pipeline_id is not None and s.op_attempts.get(op_id, 0) >= 2:
                            s.pipeline_fatal.add(pipeline_id)

                    # Global safeguard: too many total failures => stop.
                    if pipeline_id is not None and s.op_attempts.get(op_id, 0) >= s.max_total_failures_per_op:
                        s.pipeline_fatal.add(pipeline_id)
                else:
                    # On success, keep RAM hint (don't downsize) to avoid oscillation OOMs.
                    # If pipeline was boosted, clear boost gradually (once we made progress).
                    if pipeline_id is not None and pipeline_id in s.boost_pipelines:
                        s.boost_pipelines.discard(pipeline_id)

    # If nothing changed, do nothing.
    if not pipelines and not results:
        return [], []

    suspensions: List["Suspend"] = []
    assignments: List["Assignment"] = []

    assigned_op_ids = set()
    assigned_pipeline_ids = set()

    # Determine if batch has any runnable ops (for starvation tracking).
    batch_runnable_exists = _queue_has_runnable(s.queues[Priority.BATCH_PIPELINE])

    # Check if there is high-priority runnable work overall.
    high_runnable_exists = _queue_has_runnable(s.queues[Priority.QUERY]) or _queue_has_runnable(s.queues[Priority.INTERACTIVE])

    # Scheduling across pools.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool
        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # Pool roles:
        #   - If multiple pools: pool 0 prefers high priority; other pools are mixed with a soft reserve.
        #   - If single pool: behave as mixed with reserve (non-preemptive protection).
        high_preferred_pool = (s.executor.num_pools > 1 and pool_id == 0)

        # Soft reservation to keep headroom for QUERY/INTERACTIVE in mixed pools.
        # (Non-preemptive, so we must not let batch consume all free capacity.)
        if high_runnable_exists and not high_preferred_pool:
            reserve_cpu = 0.25 * pool.max_cpu_pool
            reserve_ram = 0.25 * pool.max_ram_pool
        else:
            reserve_cpu = 0.0
            reserve_ram = 0.0

        # Allow batch under load only if:
        #  - no high runnable exists, OR
        #  - batch has been starved for long enough AND we still keep reserved headroom.
        allow_batch_escape = (
            batch_runnable_exists
            and high_runnable_exists
            and (s.batch_starvation >= s.batch_starvation_threshold)
            and (avail_cpu > reserve_cpu)
            and (avail_ram > reserve_ram)
        )

        # Loop: try to place more work while resources remain.
        # Safety: at most one op per pipeline per tick (enforced via assigned_pipeline_ids).
        while avail_cpu > 0 and avail_ram > 0:
            chosen_prio = None
            chosen_pipeline = None
            chosen_op = None

            # High-priority pool: never run batch if any high-priority runnable exists.
            if high_preferred_pool:
                for pr in [Priority.QUERY, Priority.INTERACTIVE]:
                    p, op = _pop_next_runnable_from_queue(s, s.queues[pr], assigned_op_ids, assigned_pipeline_ids)
                    if p is not None:
                        chosen_prio, chosen_pipeline, chosen_op = pr, p, op
                        break

                # Only if no high runnable work exists, optionally run batch.
                if chosen_pipeline is None and not high_runnable_exists:
                    p, op = _pop_next_runnable_from_queue(s, s.queues[Priority.BATCH_PIPELINE], assigned_op_ids, assigned_pipeline_ids)
                    if p is not None:
                        chosen_prio, chosen_pipeline, chosen_op = Priority.BATCH_PIPELINE, p, op

            else:
                # Mixed pools: strict priority, with a starvation escape hatch for batch.
                for pr in _priority_order():
                    if pr == Priority.BATCH_PIPELINE and high_runnable_exists and not allow_batch_escape:
                        # While high runnable exists, batch can only consume resources beyond reservation.
                        # If we're too close to reserve, stop scheduling batch.
                        if avail_cpu <= reserve_cpu or avail_ram <= reserve_ram:
                            continue
                    p, op = _pop_next_runnable_from_queue(s, s.queues[pr], assigned_op_ids, assigned_pipeline_ids)
                    if p is not None:
                        chosen_prio, chosen_pipeline, chosen_op = pr, p, op
                        break

            if chosen_pipeline is None:
                break

            # Compute sizing.
            op_id = id(chosen_op)
            s.op_to_pipeline[op_id] = chosen_pipeline.pipeline_id

            cpu = _base_cpu_for_priority(pool, chosen_prio, avail_cpu)

            hint = s.op_ram_hint.get(op_id, 0)
            ram = _effective_ram(pool, chosen_prio, avail_ram, hint)

            # If this is batch and high runnable exists in mixed pool, enforce reservation by shrinking
            # the usable window (non-preemptive headroom protection).
            if (chosen_prio == Priority.BATCH_PIPELINE) and high_runnable_exists and (not high_preferred_pool) and (not allow_batch_escape):
                cpu = min(cpu, max(0.0, avail_cpu - reserve_cpu))
                ram = min(ram, max(0.0, avail_ram - reserve_ram))
                if cpu <= 0 or ram <= 0:
                    # Can't safely schedule batch without violating reserve.
                    break

            # Final sanity clamps.
            cpu = min(cpu, avail_cpu)
            ram = min(ram, avail_ram)
            if cpu <= 0 or ram <= 0:
                break

            # Create assignment.
            assignments.append(
                Assignment(
                    ops=[chosen_op],
                    cpu=cpu,
                    ram=ram,
                    priority=chosen_pipeline.priority,
                    pool_id=pool_id,
                    pipeline_id=chosen_pipeline.pipeline_id,
                )
            )

            assigned_op_ids.add(op_id)
            assigned_pipeline_ids.add(chosen_pipeline.pipeline_id)

            # Consume pool resources.
            avail_cpu -= cpu
            avail_ram -= ram

    # Update batch starvation tracking.
    any_batch_scheduled = any(a.priority == Priority.BATCH_PIPELINE for a in assignments)
    if any_batch_scheduled:
        s.batch_starvation = 0
    else:
        if batch_runnable_exists and high_runnable_exists:
            s.batch_starvation += 1
        else:
            # If no pressure, don't accumulate starvation.
            s.batch_starvation = 0

    return suspensions, assignments
