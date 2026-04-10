# policy_key: scheduler_low_035
# reasoning_effort: low
# model: gpt-5.2-2025-12-11
# llm_cost: 0.044547
# generation_seconds: 46.30
# generated_at: 2026-03-14T02:29:33.926682
@register_scheduler_init(key="scheduler_low_035")
def scheduler_low_035_init(s):
    """
    Priority-aware, low-risk improvement over naive FIFO.

    Changes vs naive:
      1) Maintain separate queues per priority and always consider higher priorities first.
      2) Avoid "monopolizing" a pool: cap per-assignment CPU/RAM so multiple ops can run concurrently.
      3) Soft reservations: when high-priority work is waiting, avoid consuming the last chunk of pool
         capacity with batch work (protects tail latency).
      4) Simple OOM-driven RAM backoff per pipeline: if an op fails with an OOM-like error, retry later
         with more RAM (bounded by pool max).
    """
    from collections import deque

    s.q_query = deque()
    s.q_interactive = deque()
    s.q_batch = deque()

    # Per-pipeline resource "hints" that evolve via feedback from results
    # Hints are in absolute units (cpu, ram) representing a target per-container allocation.
    s.pipeline_hints = {}  # pipeline_id -> {"cpu": float|None, "ram": float|None, "ram_mult": float}

    # Soft reservation fractions used only to prevent batch from consuming all remaining resources
    # when high priority work is queued.
    s.reserve_cpu_frac = 0.30
    s.reserve_ram_frac = 0.30

    # Caps to prevent a single assignment from consuming an entire pool by default.
    # This is intentionally conservative; it improves latency under contention by enabling concurrency.
    s.max_cpu_frac_per_assignment = 0.60
    s.max_ram_frac_per_assignment = 0.60

    # Batch gets smaller slices to reduce interference; interactive/query get larger slices.
    s.priority_cpu_boost = {
        Priority.QUERY: 1.00,
        Priority.INTERACTIVE: 0.85,
        Priority.BATCH_PIPELINE: 0.50,
    }
    s.priority_ram_boost = {
        Priority.QUERY: 1.00,
        Priority.INTERACTIVE: 0.90,
        Priority.BATCH_PIPELINE: 0.60,
    }


def _is_oom_error(err) -> bool:
    if err is None:
        return False
    try:
        msg = str(err).lower()
    except Exception:
        return False
    return ("oom" in msg) or ("out of memory" in msg) or ("memory" in msg and "alloc" in msg)


def _enqueue_pipeline(s, p):
    # Put into the appropriate priority queue
    if p.priority == Priority.QUERY:
        s.q_query.append(p)
    elif p.priority == Priority.INTERACTIVE:
        s.q_interactive.append(p)
    else:
        s.q_batch.append(p)

    # Initialize hint record if needed
    pid = p.pipeline_id
    if pid not in s.pipeline_hints:
        s.pipeline_hints[pid] = {"cpu": None, "ram": None, "ram_mult": 1.0}


def _pop_next_runnable_pipeline(s):
    # Strict priority order: QUERY -> INTERACTIVE -> BATCH
    if s.q_query:
        return s.q_query.popleft()
    if s.q_interactive:
        return s.q_interactive.popleft()
    if s.q_batch:
        return s.q_batch.popleft()
    return None


def _has_high_priority_waiting(s) -> bool:
    return bool(s.q_query) or bool(s.q_interactive)


def _desired_alloc_for_pipeline(s, pool, pipeline):
    """
    Choose a conservative per-assignment CPU/RAM.
    - If we have hints for this pipeline, use them (bounded).
    - Otherwise, use a fraction of pool size based on priority, also bounded by per-assignment caps.
    """
    pid = pipeline.pipeline_id
    hint = s.pipeline_hints.get(pid, {"cpu": None, "ram": None, "ram_mult": 1.0})

    # Base targets from pool capacity
    base_cpu = pool.max_cpu_pool * s.priority_cpu_boost.get(pipeline.priority, 0.6)
    base_ram = pool.max_ram_pool * s.priority_ram_boost.get(pipeline.priority, 0.6)

    # Apply OOM-driven multiplier (only affects RAM)
    base_ram = base_ram * max(1.0, float(hint.get("ram_mult", 1.0)))

    # Prefer explicit hints if present
    target_cpu = hint["cpu"] if hint.get("cpu") is not None else base_cpu
    target_ram = hint["ram"] if hint.get("ram") is not None else base_ram

    # Apply per-assignment caps (avoid monopolizing the pool)
    cpu_cap = pool.max_cpu_pool * s.max_cpu_frac_per_assignment
    ram_cap = pool.max_ram_pool * s.max_ram_frac_per_assignment

    target_cpu = min(float(target_cpu), float(cpu_cap))
    target_ram = min(float(target_ram), float(ram_cap))

    # Also ensure they never exceed pool maximums
    target_cpu = min(target_cpu, float(pool.max_cpu_pool))
    target_ram = min(target_ram, float(pool.max_ram_pool))

    # And never request non-positive resources
    target_cpu = max(1e-9, target_cpu)
    target_ram = max(1e-9, target_ram)

    return target_cpu, target_ram


def _fits_with_soft_reservation(s, pool, avail_cpu, avail_ram, req_cpu, req_ram) -> bool:
    """
    If high priority is waiting, don't let batch consume into reserved headroom.
    For high priority itself, always allow if it fits in current availability.
    """
    # For safety, only enforce reservation against scheduling batch work.
    if not _has_high_priority_waiting(s):
        return (req_cpu <= avail_cpu) and (req_ram <= avail_ram)

    reserved_cpu = pool.max_cpu_pool * s.reserve_cpu_frac
    reserved_ram = pool.max_ram_pool * s.reserve_ram_frac

    # If we spend resources, ensure post-allocation availability keeps some headroom.
    return (req_cpu <= avail_cpu) and (req_ram <= avail_ram) and ((avail_cpu - req_cpu) >= reserved_cpu) and (
        (avail_ram - req_ram) >= reserved_ram
    )


@register_scheduler(key="scheduler_low_035")
def scheduler_low_035(s, results: List["ExecutionResult"], pipelines: List["Pipeline"]) -> Tuple[List["Suspend"], List["Assignment"]]:
    """
    Scheduler step:
      - Ingest new pipelines into priority queues.
      - Use execution results to update OOM-based RAM backoff and (lightly) track observed allocations.
      - For each pool, repeatedly assign ready ops, prioritizing high priority and preventing batch from
        consuming reserved headroom when high priority is queued.
    """
    # Ingest arrivals
    for p in pipelines:
        _enqueue_pipeline(s, p)

    # If nothing changed, do nothing
    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    # Feedback: react to failures (especially OOM) by increasing RAM multiplier for that pipeline.
    # Note: We do NOT attempt preemption here because the template API does not expose all running containers.
    for r in results:
        try:
            pid = getattr(r, "pipeline_id", None)
        except Exception:
            pid = None

        # Some simulators may not carry pipeline_id in results; try to infer from ops if present.
        if pid is None:
            try:
                if getattr(r, "ops", None):
                    # Best-effort: operator may carry pipeline_id; otherwise give up.
                    op0 = r.ops[0]
                    pid = getattr(op0, "pipeline_id", None)
            except Exception:
                pid = None

        if pid is None:
            continue

        if pid not in s.pipeline_hints:
            s.pipeline_hints[pid] = {"cpu": None, "ram": None, "ram_mult": 1.0}

        if hasattr(r, "failed") and r.failed():
            if _is_oom_error(getattr(r, "error", None)):
                # Exponential-ish backoff, bounded; keep modest so we don't overreact.
                cur = float(s.pipeline_hints[pid].get("ram_mult", 1.0))
                s.pipeline_hints[pid]["ram_mult"] = min(8.0, max(1.25, cur * 1.8))
        else:
            # If it succeeded, we can gently "learn" that this allocation worked.
            # Keep the learned values modestly sticky but bounded.
            try:
                if getattr(r, "cpu", None) is not None:
                    s.pipeline_hints[pid]["cpu"] = float(r.cpu)
                if getattr(r, "ram", None) is not None:
                    s.pipeline_hints[pid]["ram"] = float(r.ram)
                # Successful completion suggests our RAM multiplier is not too small; relax slightly.
                cur = float(s.pipeline_hints[pid].get("ram_mult", 1.0))
                s.pipeline_hints[pid]["ram_mult"] = max(1.0, cur * 0.97)
            except Exception:
                pass

    # Scheduling loop per pool
    # Strategy: keep pulling next pipeline by priority; for each pipeline, if it has a runnable op, try to assign.
    # If it doesn't fit, requeue and move on (prevents head-of-line blocking).
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool
        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # Avoid infinite loops: bound how many pipelines we inspect in this tick per pool.
        # We allow scanning up to the total queued length at entry.
        q_len = len(s.q_query) + len(s.q_interactive) + len(s.q_batch)
        scans_left = max(1, q_len)

        # Try to pack multiple assignments into the pool while we have capacity.
        while scans_left > 0:
            scans_left -= 1
            pipeline = _pop_next_runnable_pipeline(s)
            if pipeline is None:
                break

            status = pipeline.runtime_status()
            has_failures = status.state_counts[OperatorState.FAILED] > 0

            # Drop completed/failed pipelines (we do not attempt full pipeline retries here)
            if status.is_pipeline_successful() or has_failures:
                continue

            # Get next runnable op (parents complete)
            op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
            if not op_list:
                # Not runnable now; keep it around
                _enqueue_pipeline(s, pipeline)
                continue

            # Decide allocation
            req_cpu, req_ram = _desired_alloc_for_pipeline(s, pool, pipeline)

            # Must fit current availability
            if req_cpu > avail_cpu or req_ram > avail_ram:
                # If this is high priority, we can still try a smaller allocation to avoid stalling.
                if pipeline.priority in (Priority.QUERY, Priority.INTERACTIVE):
                    req_cpu = min(req_cpu, avail_cpu)
                    req_ram = min(req_ram, avail_ram)
                    # If still not viable, requeue.
                    if req_cpu <= 0 or req_ram <= 0:
                        _enqueue_pipeline(s, pipeline)
                        continue
                else:
                    _enqueue_pipeline(s, pipeline)
                    continue

            # Enforce soft reservations only for batch when high priority is waiting
            if pipeline.priority == Priority.BATCH_PIPELINE and not _fits_with_soft_reservation(
                s, pool, avail_cpu, avail_ram, req_cpu, req_ram
            ):
                # Defer batch for later; keep scanning for other work (maybe high priority)
                _enqueue_pipeline(s, pipeline)
                continue

            # Make assignment
            assignments.append(
                Assignment(
                    ops=op_list,
                    cpu=req_cpu,
                    ram=req_ram,
                    priority=pipeline.priority,
                    pool_id=pool_id,
                    pipeline_id=pipeline.pipeline_id,
                )
            )

            # Update available resources (optimistic packing within this tick)
            avail_cpu -= req_cpu
            avail_ram -= req_ram

            # If little headroom remains, stop packing this pool this tick
            if avail_cpu <= 0 or avail_ram <= 0:
                break

            # Requeue pipeline for its next ops
            _enqueue_pipeline(s, pipeline)

    return suspensions, assignments
