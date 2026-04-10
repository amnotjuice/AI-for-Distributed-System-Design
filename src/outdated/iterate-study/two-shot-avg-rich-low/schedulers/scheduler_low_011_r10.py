# source: scheduler_low_011.py
# model: gpt-5.2-2025-12-11
# effort: low
# feedback: rich
# iteration: r10
@register_scheduler_init(key="scheduler_low_011_r10")
def scheduler_low_011_r10_init(s):
    """Priority-aware, throughput-friendly scheduler to reduce end-to-end latency.

    Iteration goals (building on naive FIFO, fixing obvious flaws seen in prior attempt):
    - Avoid starving INTERACTIVE/BATCH: use weighted round-robin across priority queues (not strict priority).
    - Avoid underutilization: pack multiple assignments per tick until pools are full (not 1 op/pool/tick).
    - Placement: prefer an "interactive pool" for high-priority when possible, but NEVER block INTERACTIVE from other pools.
    - Light isolation: keep a small headroom reserve on interactive pool when high-priority backlog exists.
    - Failure handling: OOM -> retry with higher RAM (per-op hints by op object id); non-OOM -> mark op fatal (no retries).

    Notes:
    - No preemption/suspension (sim interface doesn't expose running set reliably in the prompt).
    - Uses only safe, local state and avoids depending on unspecified fields.
    """
    # FIFO queues per priority
    s.waiting_queues = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }

    # Round-robin weights: ensures INTERACTIVE and BATCH get scheduled even under heavy QUERY load.
    s.weight_cycle = (
        [Priority.QUERY] * 4
        + [Priority.INTERACTIVE] * 3
        + [Priority.BATCH_PIPELINE] * 1
    )
    s.rr_idx = 0

    # "Interactive" pool preference (if multiple pools exist)
    s.interactive_pool_id = 0

    # Per-op (by object id) hints learned from failures
    s.op_ram_hint = {}   # op_id -> ram
    s.op_cpu_hint = {}   # op_id -> cpu (rarely used; kept for extensibility)
    s.op_attempts = {}   # op_id -> retry count
    s.fatal_ops = set()  # op_ids that failed with non-OOM or exceeded retry limit

    s.max_oom_retries = 3

    # Conservative sizing targets (fractions of pool max). Smaller for BATCH to increase concurrency.
    # These are "targets"; we will downsize to current pool availability when needed.
    s.size_fracs = {
        Priority.QUERY: {"cpu": 0.50, "ram": 0.40},
        Priority.INTERACTIVE: {"cpu": 0.60, "ram": 0.55},
        Priority.BATCH_PIPELINE: {"cpu": 0.25, "ram": 0.25},
    }

    # Minimum allocation to avoid degenerate 0-sized containers
    s.min_cpu = 1.0
    s.min_ram = 1.0

    # Per-pipeline cap on how many ops we schedule in a single tick (prevents a single pipeline dominating)
    s.max_ops_per_pipeline_per_tick = {
        Priority.QUERY: 4,
        Priority.INTERACTIVE: 2,
        Priority.BATCH_PIPELINE: 1,
    }

    # Headroom reservation on interactive pool when there is high-priority backlog,
    # to protect QUERY/INTERACTIVE tail latency from BATCH packing.
    s.reserve_frac_on_interactive_pool = {"cpu": 0.20, "ram": 0.20}

    # Simple tick counter (for future extensions like aging; not used heavily here)
    s.ticks = 0


def _priority_bucket(s, p):
    pr = getattr(p, "priority", None)
    if pr in s.waiting_queues:
        return pr
    return Priority.BATCH_PIPELINE


def _is_oom_error(err):
    if err is None:
        return False
    msg = str(err).lower()
    return ("oom" in msg) or ("out of memory" in msg) or ("memoryerror" in msg)


def _pipeline_is_done_or_dead(s, pipeline):
    status = pipeline.runtime_status()
    if status.is_pipeline_successful():
        return True

    # If there are FAILED ops and any is marked fatal, consider the pipeline dead (won't make progress).
    failed_ops = status.get_ops([OperatorState.FAILED], require_parents_complete=False) or []
    for op in failed_ops:
        if id(op) in s.fatal_ops:
            return True
    return False


def _next_assignable_nonfatal_op(s, pipeline):
    status = pipeline.runtime_status()
    ops = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True) or []
    for op in ops:
        if id(op) not in s.fatal_ops:
            return op
    return None


def _pool_ids_sorted_by_headroom(avail_cpu, avail_ram, pool_ids):
    # Sort pools by a simple headroom score so we place where it fits best.
    # Deterministic tie-breaker by pool_id.
    scored = []
    for pid in pool_ids:
        scored.append((avail_cpu[pid] + avail_ram[pid], avail_cpu[pid], avail_ram[pid], -pid))
    scored.sort(reverse=True)
    return [(-t[3]) for t in scored]


def _pool_order_for_priority(s, priority, avail_cpu, avail_ram, high_prio_backlog):
    n = s.executor.num_pools
    all_pools = list(range(n))
    if n <= 1:
        return all_pools

    ip = s.interactive_pool_id

    # For high-priority, try interactive pool first, then best headroom.
    if priority in (Priority.QUERY, Priority.INTERACTIVE):
        others = [pid for pid in all_pools if pid != ip]
        others = _pool_ids_sorted_by_headroom(avail_cpu, avail_ram, others)
        return [ip] + others

    # For batch, prefer non-interactive pools, but if there is no high-priority backlog,
    # allow interactive pool equally (it might be idle).
    others = [pid for pid in all_pools if pid != ip]
    others = _pool_ids_sorted_by_headroom(avail_cpu, avail_ram, others)
    if high_prio_backlog:
        return others + [ip]
    # No high-priority backlog -> treat all pools as candidates by headroom
    return _pool_ids_sorted_by_headroom(avail_cpu, avail_ram, all_pools)


def _target_request(s, pool, priority, op):
    fr = s.size_fracs.get(priority, {"cpu": 1.0, "ram": 1.0})
    cpu_t = max(s.min_cpu, pool.max_cpu_pool * fr["cpu"])
    ram_t = max(s.min_ram, pool.max_ram_pool * fr["ram"])

    op_id = id(op)
    if op_id in s.op_ram_hint:
        # Never go below learned hint
        ram_t = max(ram_t, float(s.op_ram_hint[op_id]))
    if op_id in s.op_cpu_hint:
        cpu_t = max(cpu_t, float(s.op_cpu_hint[op_id]))

    # Cap by pool max (defensive)
    cpu_t = min(cpu_t, pool.max_cpu_pool)
    ram_t = min(ram_t, pool.max_ram_pool)
    return cpu_t, ram_t


def _can_place_with_reserve(s, pool_id, priority, cpu_req, ram_req, avail_cpu, avail_ram, high_prio_backlog):
    # Only enforce reserve for BATCH on interactive pool when high-priority backlog exists.
    if (
        high_prio_backlog
        and pool_id == s.interactive_pool_id
        and priority == Priority.BATCH_PIPELINE
        and s.executor.num_pools > 1
    ):
        pool = s.executor.pools[pool_id]
        res_cpu = pool.max_cpu_pool * s.reserve_frac_on_interactive_pool["cpu"]
        res_ram = pool.max_ram_pool * s.reserve_frac_on_interactive_pool["ram"]
        return (avail_cpu[pool_id] - cpu_req >= res_cpu) and (avail_ram[pool_id] - ram_req >= res_ram)
    return True


@register_scheduler(key="scheduler_low_011_r10")
def scheduler_low_011_r10(s, results, pipelines):
    """
    Weighted RR across priorities + multi-assignment packing.

    Key differences vs previous attempt:
    - No "interactive only on pool0" deferral (removes interactive starvation).
    - Pack until pools are full each tick (boosts throughput, reduces queueing delays).
    - Weighted RR avoids strict priority starvation while still favoring QUERY/INTERACTIVE.
    - Reserve headroom on interactive pool when high-priority backlog exists (protects latency).
    """
    s.ticks += 1

    # Incorporate new pipelines into per-priority queues
    for p in pipelines:
        s.waiting_queues[_priority_bucket(s, p)].append(p)

    # Update per-op hints and fatal markers based on failures
    for r in results:
        failed = False
        try:
            failed = r.failed()
        except Exception:
            failed = getattr(r, "error", None) is not None

        if not failed:
            continue

        ops = getattr(r, "ops", None) or []
        is_oom = _is_oom_error(getattr(r, "error", None))

        for op in ops:
            op_id = id(op)
            if is_oom:
                attempts = int(s.op_attempts.get(op_id, 0)) + 1
                s.op_attempts[op_id] = attempts
                if attempts > s.max_oom_retries:
                    s.fatal_ops.add(op_id)
                    continue

                # Increase RAM hint aggressively to converge quickly.
                observed_ram = getattr(r, "ram", None)
                base = float(observed_ram) if observed_ram is not None else float(s.op_ram_hint.get(op_id, s.min_ram))
                base = max(base, s.min_ram)
                s.op_ram_hint[op_id] = max(float(s.op_ram_hint.get(op_id, 0.0)), base * 2.0)

                # Keep CPU hint at least the observed amount (usually not critical for OOM recovery).
                observed_cpu = getattr(r, "cpu", None)
                if observed_cpu is not None:
                    s.op_cpu_hint[op_id] = max(float(s.op_cpu_hint.get(op_id, 0.0)), float(observed_cpu), s.min_cpu)
            else:
                # Non-OOM failures are treated as fatal to avoid infinite retry loops.
                s.fatal_ops.add(op_id)

    # Fast exit if nothing changed (still allows ongoing executions to proceed without new decisions)
    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    # Local available resources while we build the schedule (avoid over-committing within a tick)
    n = s.executor.num_pools
    avail_cpu = [s.executor.pools[i].avail_cpu_pool for i in range(n)]
    avail_ram = [s.executor.pools[i].avail_ram_pool for i in range(n)]

    def any_capacity_left():
        for i in range(n):
            if avail_cpu[i] >= s.min_cpu and avail_ram[i] >= s.min_ram:
                return True
        return False

    # Track how many ops we assigned per pipeline in this tick to prevent domination
    scheduled_per_pipeline = {}

    # High-priority backlog signal for headroom reservation logic
    high_prio_backlog = (len(s.waiting_queues[Priority.QUERY]) + len(s.waiting_queues[Priority.INTERACTIVE])) > 0

    # Bounded scheduling loop:
    # - scan_budget scales with queue sizes to avoid infinite loops when nothing fits.
    scan_budget = (
        32
        + 2 * (len(s.waiting_queues[Priority.QUERY]) + len(s.waiting_queues[Priority.INTERACTIVE]) + len(s.waiting_queues[Priority.BATCH_PIPELINE]))
        + 4 * n
    )
    consecutive_no_place = 0
    max_consecutive_no_place = max(64, scan_budget // 2)

    for _ in range(scan_budget):
        if not any_capacity_left():
            break

        # Choose next priority by weighted cycle
        pr = s.weight_cycle[s.rr_idx]
        s.rr_idx = (s.rr_idx + 1) % len(s.weight_cycle)

        q = s.waiting_queues[pr]
        if not q:
            continue

        pipeline = q.pop(0)

        # Drop pipelines that are already completed or are dead due to fatal failures
        if _pipeline_is_done_or_dead(s, pipeline):
            consecutive_no_place = 0
            continue

        # Enforce per-pipeline cap for this tick
        cap = int(s.max_ops_per_pipeline_per_tick.get(pr, 1))
        pid = pipeline.pipeline_id
        if scheduled_per_pipeline.get(pid, 0) >= cap:
            q.append(pipeline)
            consecutive_no_place += 1
            if consecutive_no_place >= max_consecutive_no_place:
                break
            continue

        op = _next_assignable_nonfatal_op(s, pipeline)
        if op is None:
            # Not ready right now; requeue
            q.append(pipeline)
            consecutive_no_place += 1
            if consecutive_no_place >= max_consecutive_no_place:
                break
            continue

        # Try to place this op into some pool
        placed = False
        pool_order = _pool_order_for_priority(s, pr, avail_cpu, avail_ram, high_prio_backlog)

        for pool_id in pool_order:
            pool = s.executor.pools[pool_id]
            if avail_cpu[pool_id] < s.min_cpu or avail_ram[pool_id] < s.min_ram:
                continue

            cpu_t, ram_t = _target_request(s, pool, pr, op)

            # Downsize to currently available (packing), but never below minimums.
            cpu_req = min(cpu_t, avail_cpu[pool_id])
            ram_req = min(ram_t, avail_ram[pool_id])

            if cpu_req < s.min_cpu or ram_req < s.min_ram:
                continue

            # Optional reserve to protect interactive pool from batch contention
            if not _can_place_with_reserve(s, pool_id, pr, cpu_req, ram_req, avail_cpu, avail_ram, high_prio_backlog):
                continue

            # Place it
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
            avail_cpu[pool_id] -= cpu_req
            avail_ram[pool_id] -= ram_req

            scheduled_per_pipeline[pid] = scheduled_per_pipeline.get(pid, 0) + 1
            placed = True
            consecutive_no_place = 0
            break

        # Requeue pipeline for future ops / retries, regardless of placement outcome
        q.append(pipeline)

        if not placed:
            consecutive_no_place += 1
            if consecutive_no_place >= max_consecutive_no_place:
                break

    return suspensions, assignments