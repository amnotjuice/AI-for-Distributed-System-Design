# policy_key: scheduler_low_046
# reasoning_effort: low
# model: gpt-5.2-2025-12-11
# llm_cost: 0.055759
# generation_seconds: 40.78
# generated_at: 2026-04-09T21:39:31.561383
@register_scheduler_init(key="scheduler_low_046")
def scheduler_low_046_init(s):
    """
    Priority-first, failure-averse scheduler.

    Core ideas:
    - Strictly prioritize QUERY > INTERACTIVE > BATCH to minimize weighted latency.
    - Allocate "right-sized but generous" resources for high-priority work to avoid OOM-driven retries.
    - On failures, retry (instead of dropping) and increase RAM for that pipeline to reduce repeat OOMs.
    - Avoid starvation by applying simple aging to lower priorities (eventually they run).
    - Keep execution simple and stable: schedule at most one ready operator per pool per tick.
    """
    from collections import deque

    s.ticks = 0

    # Per-priority FIFO queues of pipeline_ids (we store ids and fetch latest pipeline objects from s.pipelines_by_id).
    s.q_query = deque()
    s.q_interactive = deque()
    s.q_batch = deque()

    # Latest pipeline object per id (so we can update status each tick without duplicating entries).
    s.pipelines_by_id = {}

    # Arrival tick for fairness/aging.
    s.arrival_tick = {}

    # RAM boost per pipeline after failures (starts at 1.0, grows on failures, capped).
    s.ram_boost = {}

    # Track per-operator attempts so we can keep retrying without infinite loops.
    # Keyed by (pipeline_id, op_key) -> attempts
    s.op_attempts = {}

    # Cap retries per operator to avoid endless churn on truly non-recoverable failures.
    s.max_attempts_per_op = 4

    # Heuristic resource fractions by priority (of pool max). These trade throughput for lower tail latency and fewer OOMs.
    s.cpu_frac = {
        Priority.QUERY: 0.85,
        Priority.INTERACTIVE: 0.70,
        Priority.BATCH_PIPELINE: 0.55,
    }
    s.ram_frac = {
        Priority.QUERY: 0.85,
        Priority.INTERACTIVE: 0.70,
        Priority.BATCH_PIPELINE: 0.60,
    }

    # Aging: each N ticks waiting increases effective urgency for lower priorities.
    s.aging_interval = 25  # ticks


def _low046_prio_queue(s, priority):
    if priority == Priority.QUERY:
        return s.q_query
    if priority == Priority.INTERACTIVE:
        return s.q_interactive
    return s.q_batch


def _low046_op_key(op):
    # Best-effort stable key across simulator objects.
    for attr in ("op_id", "operator_id", "id", "name"):
        if hasattr(op, attr):
            return str(getattr(op, attr))
    return str(op)


def _low046_is_oom_error(err):
    if err is None:
        return False
    try:
        msg = str(err).lower()
    except Exception:
        return False
    return ("oom" in msg) or ("out of memory" in msg) or ("memory" in msg and "exceed" in msg)


def _low046_effective_urgency(s, pipeline):
    """
    Compute an ordering key where smaller means more urgent.

    Base order: QUERY (0), INTERACTIVE (1), BATCH (2).
    Aging reduces the base order for long-waiting pipelines to prevent starvation.
    """
    base = 2
    if pipeline.priority == Priority.QUERY:
        base = 0
    elif pipeline.priority == Priority.INTERACTIVE:
        base = 1

    arrived = s.arrival_tick.get(pipeline.pipeline_id, s.ticks)
    waited = max(0, s.ticks - arrived)
    age_bonus = waited // max(1, s.aging_interval)

    # Only apply aging to lower priorities so queries still dominate.
    # Batch can climb, but not above interactive; interactive can climb, but not above query.
    if pipeline.priority == Priority.BATCH_PIPELINE:
        base = max(1, base - age_bonus)
    elif pipeline.priority == Priority.INTERACTIVE:
        base = max(0, base - age_bonus)

    # Tie-break by earlier arrival to keep FIFO-ish behavior.
    return (base, arrived)


def _low046_target_resources(s, pool, priority, pipeline_id):
    """
    Choose CPU/RAM targets based on pool max, priority, and pipeline failure history.
    We clamp to available pool resources at assignment time.
    """
    boost = float(s.ram_boost.get(pipeline_id, 1.0))
    # Keep boost bounded so we don't pin entire pools forever.
    boost = max(1.0, min(4.0, boost))

    cpu_target = max(1.0, float(pool.max_cpu_pool) * float(s.cpu_frac.get(priority, 0.60)))
    ram_target = max(1.0, float(pool.max_ram_pool) * float(s.ram_frac.get(priority, 0.65)) * boost)

    # Never request more than pool max.
    cpu_target = min(float(pool.max_cpu_pool), cpu_target)
    ram_target = min(float(pool.max_ram_pool), ram_target)
    return cpu_target, ram_target


@register_scheduler(key="scheduler_low_046")
def scheduler_low_046(s, results: List["ExecutionResult"], pipelines: List["Pipeline"]) -> Tuple[List["Suspend"], List["Assignment"]]:
    """
    Scheduling loop:
    - Ingest new pipelines into per-priority queues.
    - Use results to (a) detect failures, (b) increase RAM boost on OOM, (c) keep retrying failed ops.
    - For each pool, pick the most urgent pipeline that has a ready-to-run operator, and assign ONE operator.
    """
    s.ticks += 1

    # Ingest new pipelines.
    for p in pipelines:
        s.pipelines_by_id[p.pipeline_id] = p
        if p.pipeline_id not in s.arrival_tick:
            s.arrival_tick[p.pipeline_id] = s.ticks
        if p.pipeline_id not in s.ram_boost:
            s.ram_boost[p.pipeline_id] = 1.0
        _low046_prio_queue(s, p.priority).append(p.pipeline_id)

    # Use results to adapt.
    # If an operator failed, retry it (ASSIGNABLE_STATES includes FAILED) and increase RAM boost if likely OOM.
    for r in results:
        if r is None:
            continue
        if r.failed():
            # Increase RAM boost on likely OOM to reduce repeat failures.
            if _low046_is_oom_error(getattr(r, "error", None)):
                pid = getattr(r, "pipeline_id", None)
                # Some simulators may not provide pipeline_id on ExecutionResult; fall back to operator->pipeline tracking via queues.
                if pid is not None and pid in s.ram_boost:
                    s.ram_boost[pid] = min(4.0, float(s.ram_boost.get(pid, 1.0)) * 1.6)

    # Early exit if nothing changed.
    if not pipelines and not results:
        return [], []

    suspensions: List["Suspend"] = []
    assignments: List["Assignment"] = []

    # Build a unique candidate set from queues without losing FIFO order too aggressively.
    # We'll do bounded scanning per pool to keep runtime reasonable.
    def pop_next_candidate(max_scan=64):
        """
        Return next candidate pipeline_id by urgency with bounded scanning across queues.
        Does not permanently drop entries; callers should requeue the id if still active.
        """
        scanned = []
        # Pull up to max_scan ids across queues in round-robin-ish way (but queries first).
        for q in (s.q_query, s.q_interactive, s.q_batch):
            while q and len(scanned) < max_scan:
                scanned.append(q.popleft())

        # Deduplicate while preserving first occurrence.
        seen = set()
        uniq = []
        for pid in scanned:
            if pid in seen:
                continue
            seen.add(pid)
            uniq.append(pid)

        # Convert to pipelines and compute urgency.
        candidates = []
        for pid in uniq:
            p = s.pipelines_by_id.get(pid)
            if p is None:
                continue
            st = p.runtime_status()
            # Drop successful pipelines from further consideration.
            if st.is_pipeline_successful():
                continue
            candidates.append(p)

        # Sort by urgency (lower is better).
        candidates.sort(key=lambda p: _low046_effective_urgency(s, p))

        # Requeue all scanned ids to preserve fairness; active ones will be re-added below.
        for pid in scanned:
            p = s.pipelines_by_id.get(pid)
            if p is None:
                continue
            st = p.runtime_status()
            if not st.is_pipeline_successful():
                _low046_prio_queue(s, p.priority).append(pid)

        return candidates

    # For each pool, assign at most one operator to keep decisions stable and reduce interference.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)
        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        candidates = pop_next_candidate(max_scan=96)
        if not candidates:
            continue

        chosen_pipeline = None
        chosen_ops = None

        # Find first pipeline with a ready op and remaining retry budget.
        for p in candidates:
            st = p.runtime_status()

            # Only schedule if there is at least one assignable op whose parents are complete.
            ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
            if not ops:
                continue

            # Enforce retry cap per op.
            op = ops[0]
            opk = _low046_op_key(op)
            attempts = int(s.op_attempts.get((p.pipeline_id, opk), 0))
            if attempts >= int(s.max_attempts_per_op):
                # Skip this pipeline for now; it may have a poisoned op. We'll still keep the pipeline in queue.
                continue

            chosen_pipeline = p
            chosen_ops = ops
            break

        if chosen_pipeline is None or not chosen_ops:
            continue

        cpu_target, ram_target = _low046_target_resources(s, pool, chosen_pipeline.priority, chosen_pipeline.pipeline_id)

        # Clamp to currently available resources.
        cpu = min(avail_cpu, cpu_target)
        ram = min(avail_ram, ram_target)

        # If we can't provide at least some minimal CPU/RAM, skip this pool for now.
        if cpu <= 0 or ram <= 0:
            continue

        # Record attempt count for chosen op.
        opk = _low046_op_key(chosen_ops[0])
        s.op_attempts[(chosen_pipeline.pipeline_id, opk)] = int(s.op_attempts.get((chosen_pipeline.pipeline_id, opk), 0)) + 1

        assignments.append(
            Assignment(
                ops=chosen_ops,
                cpu=cpu,
                ram=ram,
                priority=chosen_pipeline.priority,
                pool_id=pool_id,
                pipeline_id=chosen_pipeline.pipeline_id,
            )
        )

    return suspensions, assignments
