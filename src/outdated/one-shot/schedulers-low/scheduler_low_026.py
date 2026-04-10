# policy_key: scheduler_low_026
# reasoning_effort: low
# model: gpt-5.2-2025-12-11
# llm_cost: 0.047039
# generation_seconds: 59.91
# generated_at: 2026-03-14T02:22:38.453206
@register_scheduler_init(key="scheduler_low_026")
def scheduler_low_026_init(s):
    """
    Priority-aware, reservation-based scheduler (small, incremental improvement over naive FIFO).

    Key improvements vs. naive:
      1) Separate queues per priority; always try QUERY -> INTERACTIVE -> BATCH.
      2) Prevent BATCH from consuming the entire pool via simple headroom reservations.
      3) Avoid allocating "all available" resources to a single op; use capped allocations per priority.
      4) OOM-aware retry: if an op fails with an OOM-like error, increase a per-pipeline RAM hint and retry.

    Notes:
      - No true preemption is attempted (container ids are not known at assignment time in the provided API).
      - The policy is intentionally conservative/simple to remain robust across unknown operator schemas.
    """
    from collections import deque

    # Per-priority pipeline queues
    s.q_query = deque()
    s.q_interactive = deque()
    s.q_batch = deque()

    # Per-pipeline resource hints (learned from OOM failures)
    s.pipeline_ram_hint = {}  # pipeline_id -> ram
    s.pipeline_cpu_hint = {}  # pipeline_id -> cpu
    s.pipeline_drop = set()   # pipeline_ids to drop (non-OOM failures)

    # Fairness: soft aging counter (ticks waited since last successful assignment attempt)
    s.pipeline_age = {}       # pipeline_id -> int

    # Conservative caps per op (fractions of pool max), by priority
    s.cpu_caps = {
        Priority.QUERY: 0.50,
        Priority.INTERACTIVE: 0.75,
        Priority.BATCH_PIPELINE: 1.00,
    }
    s.ram_caps = {
        Priority.QUERY: 0.50,
        Priority.INTERACTIVE: 0.75,
        Priority.BATCH_PIPELINE: 1.00,
    }

    # Keep headroom for latency-sensitive work (applied when considering BATCH placements)
    s.reserve_cpu_frac = 0.20
    s.reserve_ram_frac = 0.20

    # How aggressively to grow RAM on OOM
    s.oom_ram_growth = 1.6
    s.oom_ram_growth_min_add = 0.5  # in "RAM units" (whatever simulator uses)
    s.max_oom_retries = 3
    s.pipeline_oom_retries = {}  # pipeline_id -> count


def _is_oom_error(err) -> bool:
    if not err:
        return False
    e = str(err).lower()
    return ("oom" in e) or ("out of memory" in e) or ("memoryerror" in e) or ("cuda out of memory" in e)


def _enqueue_pipeline(s, p):
    # Initialize per-pipeline state if needed
    pid = p.pipeline_id
    if pid not in s.pipeline_age:
        s.pipeline_age[pid] = 0
    if pid not in s.pipeline_oom_retries:
        s.pipeline_oom_retries[pid] = 0

    if p.priority == Priority.QUERY:
        s.q_query.append(p)
    elif p.priority == Priority.INTERACTIVE:
        s.q_interactive.append(p)
    else:
        s.q_batch.append(p)


def _iter_queues_in_priority_order(s):
    # Latency-first ordering
    return [s.q_query, s.q_interactive, s.q_batch]


def _priority_reserve_amounts(s, pool):
    reserve_cpu = pool.max_cpu_pool * s.reserve_cpu_frac
    reserve_ram = pool.max_ram_pool * s.reserve_ram_frac
    return reserve_cpu, reserve_ram


def _cap_allocation(s, pool, priority, avail_cpu, avail_ram):
    # Cap allocations so we don't dump an entire pool into one op (reduces HoL blocking).
    cpu_cap = max(1.0, pool.max_cpu_pool * s.cpu_caps.get(priority, 1.0))
    ram_cap = max(1.0, pool.max_ram_pool * s.ram_caps.get(priority, 1.0))
    cpu = min(avail_cpu, cpu_cap)
    ram = min(avail_ram, ram_cap)
    return cpu, ram


def _hinted_allocation(s, pool, pipeline, avail_cpu, avail_ram):
    # Apply learned hints as minimums, then cap by per-priority limits.
    pid = pipeline.pipeline_id
    prio = pipeline.priority

    cpu, ram = _cap_allocation(s, pool, prio, avail_cpu, avail_ram)

    # Ensure we meet per-pipeline minimum hints if possible
    cpu_hint = s.pipeline_cpu_hint.get(pid, None)
    ram_hint = s.pipeline_ram_hint.get(pid, None)

    if cpu_hint is not None:
        cpu = min(avail_cpu, max(cpu, float(cpu_hint)))
    if ram_hint is not None:
        ram = min(avail_ram, max(ram, float(ram_hint)))

    # Ensure at least 1 unit (sim usually treats as vCPU/GB-like units)
    cpu = max(1.0, float(cpu))
    ram = max(1.0, float(ram))
    return cpu, ram


@register_scheduler(key="scheduler_low_026")
def scheduler_low_026_scheduler(s, results, pipelines):
    """
    Scheduler step:
      - Incorporate new arrivals into per-priority queues.
      - Learn from failures: OOM -> increase RAM hint & retry; non-OOM -> drop pipeline.
      - For each pool, try to assign one ready operator, prioritizing QUERY/INTERACTIVE.
      - Only schedule BATCH if pool has headroom beyond reserved CPU/RAM.
    """
    # Enqueue new pipelines
    for p in pipelines:
        _enqueue_pipeline(s, p)

    # Early exit if nothing changed
    if not pipelines and not results:
        return [], []

    # Learn from execution results
    for r in results:
        # Results include pipeline_id? Not listed; infer via r.ops if needed is not possible.
        # We therefore only use r.priority + error signal to adjust per-pipeline hints *when possible*.
        # Many simulators include r.ops with backrefs, but we can't rely on it, so keep this conservative.
        if r.failed():
            # If we can't attribute to a pipeline, we can still avoid catastrophic behavior by not changing global state.
            # However, ExecutionResult in many implementations carries ops with pipeline_id; try best-effort.
            pid = None
            try:
                # common patterns: r.ops is list of Operator objects with .pipeline_id or .pipeline.pipeline_id
                if getattr(r, "pipeline_id", None) is not None:
                    pid = r.pipeline_id
                elif r.ops:
                    op0 = r.ops[0]
                    if getattr(op0, "pipeline_id", None) is not None:
                        pid = op0.pipeline_id
                    elif getattr(op0, "pipeline", None) is not None and getattr(op0.pipeline, "pipeline_id", None) is not None:
                        pid = op0.pipeline.pipeline_id
            except Exception:
                pid = None

            if pid is not None:
                if _is_oom_error(r.error):
                    # Retry OOMs a few times with larger RAM; keep CPU unchanged.
                    if s.pipeline_oom_retries.get(pid, 0) < s.max_oom_retries:
                        s.pipeline_oom_retries[pid] = s.pipeline_oom_retries.get(pid, 0) + 1

                        # Grow RAM hint based on previous attempt, with a minimum additive bump.
                        prev_ram = s.pipeline_ram_hint.get(pid, None)
                        base = float(prev_ram) if prev_ram is not None else float(r.ram) if getattr(r, "ram", None) else 1.0
                        grown = max(base * s.oom_ram_growth, base + s.oom_ram_growth_min_add)
                        s.pipeline_ram_hint[pid] = grown
                    else:
                        s.pipeline_drop.add(pid)
                else:
                    # Non-OOM failure: don't churn retries indefinitely
                    s.pipeline_drop.add(pid)

    suspensions = []  # No preemption in this incremental version
    assignments = []

    # Helper: pull one assignable op from a queue, requeueing pipelines that aren't ready yet.
    def try_assign_from_queue(q, pool_id, pool, avail_cpu, avail_ram, allow_batch):
        # We'll scan up to current queue length to avoid infinite loops.
        scanned = 0
        qlen = len(q)

        while scanned < qlen and q:
            pipeline = q.popleft()
            scanned += 1

            pid = pipeline.pipeline_id
            if pid in s.pipeline_drop:
                continue

            status = pipeline.runtime_status()

            # Drop if complete
            if status.is_pipeline_successful():
                continue

            # If pipeline has failures and we didn't mark it for retry, drop it
            # (Note: some sims mark FAILED ops for retry; we only retry OOMs via hints)
            try:
                if status.state_counts[OperatorState.FAILED] > 0 and pid in s.pipeline_drop:
                    continue
            except Exception:
                pass

            # Find one ready op
            op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
            if not op_list:
                # Not ready; age it and requeue
                s.pipeline_age[pid] = s.pipeline_age.get(pid, 0) + 1
                q.append(pipeline)
                continue

            # For batch, enforce headroom reservations
            if (pipeline.priority == Priority.BATCH_PIPELINE) and (not allow_batch):
                # Put it back and give up for this pool
                s.pipeline_age[pid] = s.pipeline_age.get(pid, 0) + 1
                q.append(pipeline)
                return None

            # Allocate resources (capped + hinted)
            cpu, ram = _hinted_allocation(s, pool, pipeline, avail_cpu, avail_ram)

            # If even minimums don't fit, requeue and stop trying on this pool
            if cpu > avail_cpu or ram > avail_ram:
                s.pipeline_age[pid] = s.pipeline_age.get(pid, 0) + 1
                q.append(pipeline)
                return None

            # Reset age on successful admission attempt
            s.pipeline_age[pid] = 0

            return Assignment(
                ops=op_list,
                cpu=cpu,
                ram=ram,
                priority=pipeline.priority,
                pool_id=pool_id,
                pipeline_id=pid,
            )

        return None

    # For each pool, assign at most one op per tick (keeps interactive latency stable under contention).
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool
        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        reserve_cpu, reserve_ram = _priority_reserve_amounts(s, pool)
        allow_batch = (avail_cpu > reserve_cpu) and (avail_ram > reserve_ram)

        # Try queues in priority order
        assignment = None
        for q in _iter_queues_in_priority_order(s):
            assignment = try_assign_from_queue(q, pool_id, pool, avail_cpu, avail_ram, allow_batch=allow_batch)
            if assignment is not None:
                break

        if assignment is not None:
            assignments.append(assignment)

    return suspensions, assignments
