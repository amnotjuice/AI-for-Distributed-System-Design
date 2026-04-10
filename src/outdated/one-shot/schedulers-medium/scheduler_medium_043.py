# policy_key: scheduler_medium_043
# reasoning_effort: medium
# model: gpt-5.2-2025-12-11
# llm_cost: 0.093673
# generation_seconds: 96.66
# generated_at: 2026-03-14T03:16:52.070767
@register_scheduler_init(key="scheduler_medium_043")
def scheduler_medium_043_init(s):
    """
    Priority-aware, slice-based scheduler.

    Improvements over naive FIFO:
      1) Separate queues per priority and always prefer higher-priority work.
      2) Avoid allocating an entire pool to a single operator; instead allocate
         bounded "slices" to reduce head-of-line blocking and improve latency.
      3) Keep small headroom when high-priority work exists to reduce burst latency.
      4) Simple failure-aware RAM backoff (heuristic) using pipeline status deltas,
         with a cap and a max-retry guard to avoid infinite retry loops.
    """
    # Per-priority round-robin queues of Pipeline objects
    s.queues = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }

    # Track which pipelines are currently enqueued (avoid duplicates)
    s.enqueued_pipeline_ids = set()

    # Heuristic RAM multiplier per pipeline_id (increases after failures)
    s.ram_mult = {}

    # Track last observed FAILED-op count per pipeline_id (to detect new failures)
    s.last_failed_count = {}

    # Count of detected failures per pipeline_id (to stop infinite retries)
    s.fail_count = {}

    # Pipelines we will no longer schedule (non-productive failures)
    s.dead_pipeline_ids = set()

    # Tuning knobs (kept simple and conservative)
    s.max_failures_per_pipeline = 3
    s.ram_mult_increase = 1.5
    s.ram_mult_cap = 8.0

    # Resource sizing by priority (fractions of pool max)
    s.cpu_frac = {
        Priority.QUERY: 0.50,
        Priority.INTERACTIVE: 0.50,
        Priority.BATCH_PIPELINE: 0.25,
    }
    s.ram_frac = {
        Priority.QUERY: 0.50,
        Priority.INTERACTIVE: 0.50,
        Priority.BATCH_PIPELINE: 0.30,
    }

    # Caps to avoid one assignment monopolizing a pool even if fraction suggests larger
    s.cpu_cap_frac_of_pool = 0.75
    s.ram_cap_frac_of_pool = 0.85

    # Minimum slice to avoid zero allocations
    s.min_cpu = 1
    s.min_ram_frac_of_pool = 0.05  # 5% of pool RAM

    # Keep some headroom when high-priority demand exists
    s.reserve_frac_cpu_for_hp = 0.15
    s.reserve_frac_ram_for_hp = 0.15


def _priority_order():
    # Smaller index = higher priority
    return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]


def _pipeline_done_or_dead(s, pipeline):
    if pipeline.pipeline_id in s.dead_pipeline_ids:
        return True
    status = pipeline.runtime_status()
    if status.is_pipeline_successful():
        return True
    return False


def _cleanup_pipeline_if_terminal(s, pipeline):
    """Remove from bookkeeping if completed or marked dead."""
    pid = pipeline.pipeline_id
    if pid in s.dead_pipeline_ids:
        s.enqueued_pipeline_ids.discard(pid)
        return True
    status = pipeline.runtime_status()
    if status.is_pipeline_successful():
        s.enqueued_pipeline_ids.discard(pid)
        # Keep small maps from growing without bound
        s.ram_mult.pop(pid, None)
        s.last_failed_count.pop(pid, None)
        s.fail_count.pop(pid, None)
        return True
    return False


def _detect_and_handle_new_failures(s, pipeline):
    """
    Detect newly failed ops by comparing FAILED-op count to last seen.

    Since we may not reliably get pipeline_id from ExecutionResult in this interface,
    we use pipeline status deltas as a simple feedback signal.

    Policy:
      - On any new failure, increase RAM multiplier (heuristic) and increment fail count.
      - After too many failures, mark pipeline dead to avoid infinite retries.
    """
    pid = pipeline.pipeline_id
    status = pipeline.runtime_status()
    failed_now = status.state_counts.get(OperatorState.FAILED, 0)
    failed_prev = s.last_failed_count.get(pid, 0)

    if failed_now > failed_prev:
        # New failure observed
        s.fail_count[pid] = s.fail_count.get(pid, 0) + (failed_now - failed_prev)

        # Heuristic: treat repeated failures as potential under-provisioning and backoff RAM.
        cur_mult = s.ram_mult.get(pid, 1.0)
        new_mult = cur_mult * s.ram_mult_increase
        if new_mult > s.ram_mult_cap:
            new_mult = s.ram_mult_cap
        s.ram_mult[pid] = new_mult

        # Too many failures => stop scheduling to prevent churn
        if s.fail_count[pid] >= s.max_failures_per_pipeline:
            s.dead_pipeline_ids.add(pid)

    s.last_failed_count[pid] = failed_now


def _has_ready_op(pipeline):
    status = pipeline.runtime_status()
    ops = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
    if not ops:
        return None
    return ops[0]


def _any_high_priority_ready(s):
    """True if there exists any ready op in QUERY or INTERACTIVE queues."""
    for pr in (Priority.QUERY, Priority.INTERACTIVE):
        q = s.queues.get(pr, [])
        for p in q:
            if p.pipeline_id in s.dead_pipeline_ids:
                continue
            if p.runtime_status().is_pipeline_successful():
                continue
            if _has_ready_op(p) is not None:
                return True
    return False


def _pick_next_ready_pipeline_rr(s, pr):
    """
    Round-robin scan within a single priority queue to find a pipeline with a ready op.

    Returns:
        (pipeline, op) or (None, None)
    """
    q = s.queues.get(pr, [])
    n = len(q)
    for _ in range(n):
        p = q.pop(0)

        # Terminal cleanup (don't requeue)
        if _cleanup_pipeline_if_terminal(s, p):
            continue

        # Failure feedback update
        _detect_and_handle_new_failures(s, p)
        if p.pipeline_id in s.dead_pipeline_ids:
            continue

        op = _has_ready_op(p)

        # Always requeue for RR fairness (unless terminal/dead above)
        q.append(p)

        if op is not None:
            return p, op

    return None, None


def _size_allocation(s, pool, priority, pipeline_id, avail_cpu, avail_ram):
    """
    Decide CPU/RAM for one operator.

    Goals:
      - Small bounded slices to improve latency and avoid pool monopolization.
      - Priority-aware: higher priority gets larger slices (scale-up bias kept but bounded).
      - Failure-aware: increase RAM multiplier after observed failures.
    """
    max_cpu = pool.max_cpu_pool
    max_ram = pool.max_ram_pool

    # Compute baseline slice from pool max
    cpu_target = int(max_cpu * s.cpu_frac.get(priority, 0.25))
    if cpu_target < s.min_cpu:
        cpu_target = s.min_cpu
    cpu_cap = int(max_cpu * s.cpu_cap_frac_of_pool)
    if cpu_cap < s.min_cpu:
        cpu_cap = s.min_cpu
    if cpu_target > cpu_cap:
        cpu_target = cpu_cap

    # RAM sizing (float-friendly)
    base_ram = max_ram * s.ram_frac.get(priority, 0.30)
    min_ram = max_ram * s.min_ram_frac_of_pool
    if base_ram < min_ram:
        base_ram = min_ram

    mult = s.ram_mult.get(pipeline_id, 1.0)
    ram_target = base_ram * mult

    # Apply per-assignment caps
    ram_cap = max_ram * s.ram_cap_frac_of_pool
    if ram_target > ram_cap:
        ram_target = ram_cap
    if ram_target < min_ram:
        ram_target = min_ram

    # Fit to available resources (as the final step)
    if cpu_target > avail_cpu:
        cpu_target = int(avail_cpu)
    if cpu_target < s.min_cpu and avail_cpu >= s.min_cpu:
        cpu_target = s.min_cpu

    if ram_target > avail_ram:
        ram_target = avail_ram

    return cpu_target, ram_target


@register_scheduler(key="scheduler_medium_043")
def scheduler_medium_043_scheduler(s, results, pipelines):
    """
    Priority-aware scheduling with bounded slices and small high-priority headroom.

    Key behavior:
      - Enqueue new pipelines by priority (QUERY > INTERACTIVE > BATCH).
      - Within each priority, do round-robin across pipelines.
      - Within each pool tick, schedule multiple assignments while resources remain.
      - If high-priority work exists, reserve headroom so batch cannot fully consume pool.
      - Heuristic RAM backoff when new failures are observed for a pipeline.
    """
    # Enqueue new pipelines (avoid duplicates)
    for p in pipelines:
        pid = p.pipeline_id
        if pid in s.dead_pipeline_ids:
            continue
        if pid in s.enqueued_pipeline_ids:
            continue
        if p.runtime_status().is_pipeline_successful():
            continue
        s.enqueued_pipeline_ids.add(pid)
        if p.priority not in s.queues:
            # Unknown priority => treat as batch-like
            s.queues[Priority.BATCH_PIPELINE].append(p)
        else:
            s.queues[p.priority].append(p)

    # We do not implement preemption in this version (interface may not expose
    # enough info to reliably identify suspend candidates across pools).
    suspensions = []
    assignments = []

    # Fast-path: if no waiting work and no new inputs and no results, do nothing
    if not pipelines and not results:
        any_waiting = False
        for pr in s.queues:
            if s.queues[pr]:
                any_waiting = True
                break
        if not any_waiting:
            return suspensions, assignments

    # Determine if we should keep headroom for high-priority
    hp_demand = _any_high_priority_ready(s)

    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool
        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # Reserve some capacity if high-priority demand exists
        reserve_cpu = 0
        reserve_ram = 0.0
        if hp_demand:
            reserve_cpu = int(pool.max_cpu_pool * s.reserve_frac_cpu_for_hp)
            reserve_ram = pool.max_ram_pool * s.reserve_frac_ram_for_hp
            if reserve_cpu < 0:
                reserve_cpu = 0
            if reserve_ram < 0:
                reserve_ram = 0.0

        # Keep assigning while we have resources (respecting headroom)
        # Note: Use local avail_* to avoid over-assigning within the tick.
        made_progress = True
        while made_progress:
            made_progress = False

            # If we have high-priority demand, don't let batch consume into reserved headroom.
            effective_avail_cpu = avail_cpu - reserve_cpu
            effective_avail_ram = avail_ram - reserve_ram
            if effective_avail_cpu < 0:
                effective_avail_cpu = 0
            if effective_avail_ram < 0:
                effective_avail_ram = 0

            # Try to pick next ready op in strict priority order.
            picked = None
            for pr in _priority_order():
                # If HP demand exists and we're at/under headroom, only schedule HP.
                if hp_demand and pr == Priority.BATCH_PIPELINE:
                    if effective_avail_cpu <= 0 or effective_avail_ram <= 0:
                        continue

                p, op = _pick_next_ready_pipeline_rr(s, pr)
                if p is None or op is None:
                    continue
                picked = (pr, p, op)
                break

            if picked is None:
                break

            pr, pipeline, op = picked

            # Recompute demand after potential cleanup/failure updates
            if pipeline.pipeline_id in s.dead_pipeline_ids:
                continue
            if pipeline.runtime_status().is_pipeline_successful():
                _cleanup_pipeline_if_terminal(s, pipeline)
                continue

            # Choose allocation for this assignment
            cpu_req, ram_req = _size_allocation(
                s,
                pool=pool,
                priority=pr,
                pipeline_id=pipeline.pipeline_id,
                avail_cpu=effective_avail_cpu if hp_demand and pr == Priority.BATCH_PIPELINE else avail_cpu,
                avail_ram=effective_avail_ram if hp_demand and pr == Priority.BATCH_PIPELINE else avail_ram,
            )

            # If we cannot allocate meaningful resources, stop scheduling in this pool.
            if cpu_req is None or ram_req is None:
                break
            if cpu_req <= 0 or ram_req <= 0:
                break

            # Final safety clamp to pool-local available
            if cpu_req > avail_cpu:
                cpu_req = int(avail_cpu)
            if ram_req > avail_ram:
                ram_req = avail_ram
            if cpu_req <= 0 or ram_req <= 0:
                break

            assignments.append(
                Assignment(
                    ops=[op],
                    cpu=cpu_req,
                    ram=ram_req,
                    priority=pipeline.priority,
                    pool_id=pool_id,
                    pipeline_id=pipeline.pipeline_id,
                )
            )

            avail_cpu -= cpu_req
            avail_ram -= ram_req
            made_progress = True

            # After giving resources to a high-priority op, we may want to keep reserving headroom;
            # after scheduling one op, check if high priority still exists.
            hp_demand = _any_high_priority_ready(s)

            if avail_cpu <= 0 or avail_ram <= 0:
                break

    return suspensions, assignments
