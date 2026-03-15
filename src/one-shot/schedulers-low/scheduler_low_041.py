# policy_key: scheduler_low_041
# reasoning_effort: low
# model: gpt-5.2-2025-12-11
# llm_cost: 0.036945
# generation_seconds: 39.16
# generated_at: 2026-03-14T02:33:57.775187
@register_scheduler_init(key="scheduler_low_041")
def scheduler_low_041_init(s):
    """Priority-aware FIFO with small, safe improvements over naive FIFO.

    Improvements (kept intentionally simple and incremental):
    1) Priority queues: schedule QUERY/INTERACTIVE ahead of BATCH.
    2) Resource caps per assignment: avoid giving a single op the entire pool (reduces head-of-line blocking).
    3) OOM/failed retry with RAM backoff: if an op fails, retry with increased RAM (bounded), instead of dropping the pipeline.
    4) Bounded retries: prevent infinite retry loops on non-transient failures.

    Notes:
    - No preemption is used because the minimal public interface shown does not expose a reliable way to list/evict running containers.
    - We schedule at most one ready op per pipeline per tick to improve fairness and latency under contention.
    """
    # Separate FIFO queues by priority (higher priority drained first).
    s.queues = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }

    # RAM backoff multiplier per pipeline (increases after failures).
    s.ram_mult = {}  # pipeline_id -> float

    # Retry budget per pipeline.
    s.retries = {}  # pipeline_id -> int

    # Tunables
    s.max_retries = 3
    s.max_ram_mult = 8.0

    # Per-priority caps: fraction of currently-available pool resources to allocate to a single assignment.
    # This reduces tail latency by preventing one op from monopolizing all headroom.
    s.priority_caps = {
        Priority.QUERY: (0.75, 0.75),          # (cpu_frac, ram_frac)
        Priority.INTERACTIVE: (0.60, 0.60),
        Priority.BATCH_PIPELINE: (0.40, 0.40),
    }

    # Minimums (best-effort; units depend on simulator config).
    s.min_cpu = 1
    s.min_ram = 1


def _priority_order():
    return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]


def _ensure_pipeline_state(s, pipeline_id):
    if pipeline_id not in s.ram_mult:
        s.ram_mult[pipeline_id] = 1.0
    if pipeline_id not in s.retries:
        s.retries[pipeline_id] = 0


def _enqueue(s, pipeline):
    # Unknown priority falls back to batch behavior.
    pr = pipeline.priority if pipeline.priority in s.queues else Priority.BATCH_PIPELINE
    s.queues[pr].append(pipeline)


def _pick_next_pipeline(s):
    for pr in _priority_order():
        q = s.queues[pr]
        if q:
            return q.pop(0)
    return None


def _requeue(s, pipeline):
    _enqueue(s, pipeline)


def _cap_resources(s, priority, avail_cpu, avail_ram):
    cpu_frac, ram_frac = s.priority_caps.get(priority, (0.4, 0.4))
    cpu = int(avail_cpu * cpu_frac)
    ram = int(avail_ram * ram_frac)
    if cpu <= 0 and avail_cpu > 0:
        cpu = min(s.min_cpu, int(avail_cpu)) if int(avail_cpu) >= 1 else 0
    if ram <= 0 and avail_ram > 0:
        ram = min(s.min_ram, int(avail_ram)) if int(avail_ram) >= 1 else 0
    cpu = max(0, min(int(avail_cpu), cpu))
    ram = max(0, min(int(avail_ram), ram))
    return cpu, ram


def _apply_ram_backoff(s, pipeline_id, base_ram, avail_ram):
    mult = s.ram_mult.get(pipeline_id, 1.0)
    ram = int(base_ram * mult)
    ram = min(int(avail_ram), ram)
    return max(0, ram)


@register_scheduler(key="scheduler_low_041")
def scheduler_low_041(s, results, pipelines):
    """
    Priority-aware scheduler with conservative resource sharing and OOM-style backoff.

    Strategy per tick:
    - Ingest new pipelines into per-priority FIFO queues.
    - Process results: on failure, increment retry and increase RAM multiplier for that pipeline.
    - For each pool, repeatedly:
        - choose the next pipeline by priority FIFO,
        - pick exactly one ready op from that pipeline (parents complete),
        - allocate capped CPU/RAM (with RAM backoff multiplier),
        - emit an Assignment if feasible; otherwise requeue and stop for that pool.
    """
    # Enqueue incoming pipelines and initialize state.
    for p in pipelines:
        _ensure_pipeline_state(s, p.pipeline_id)
        _enqueue(s, p)

    # Update backoff state based on results (simple: any failure triggers RAM backoff).
    # We do not drop the whole pipeline on first failure; we allow retries.
    for r in results:
        if r is None:
            continue
        # If the execution failed, assume resource-related (e.g., OOM) and back off RAM.
        if hasattr(r, "failed") and r.failed():
            # Attempt to attribute to a pipeline via r.ops' first op's pipeline_id is not available,
            # so we conservatively use r.priority only for nothing; instead, rely on pipeline_id in queues.
            # However, ExecutionResult provides no pipeline_id in the given interface. Therefore:
            # - We cannot map failure to a specific pipeline reliably.
            # - Still, we can use observed container sizing signals only if we had mapping.
            # In absence of mapping, we do nothing here.
            pass

    # Early exit if nothing to do.
    if not pipelines and not results:
        # Even without new pipelines/results, there could be queued work; don't early exit then.
        any_waiting = any(len(s.queues[pr]) > 0 for pr in s.queues)
        if not any_waiting:
            return [], []

    suspensions = []
    assignments = []

    # Helper: clean up completed pipelines at dequeue-time.
    def pipeline_is_done_or_hopeless(p):
        st = p.runtime_status()
        if st.is_pipeline_successful():
            return True
        _ensure_pipeline_state(s, p.pipeline_id)
        # If too many retries (tracked heuristically by counting FAILED ops present), drop.
        # Because we cannot map ExecutionResult -> pipeline reliably, use current status as signal.
        failed_ops = st.state_counts.get(OperatorState.FAILED, 0) if hasattr(st, "state_counts") else 0
        if failed_ops > 0:
            # Count a "retry epoch" once per tick when we observe failures and still have assignable ops.
            # This is a conservative bound to avoid infinite loops.
            s.retries[p.pipeline_id] = max(s.retries[p.pipeline_id], 1)
        return s.retries.get(p.pipeline_id, 0) > s.max_retries

    # Another helper: if we see FAILED ops, we back off RAM multiplier once per scheduling attempt.
    def maybe_backoff_on_failed_ops(p):
        st = p.runtime_status()
        failed_ops = st.state_counts.get(OperatorState.FAILED, 0) if hasattr(st, "state_counts") else 0
        if failed_ops > 0:
            pid = p.pipeline_id
            _ensure_pipeline_state(s, pid)
            if s.retries[pid] < s.max_retries:
                s.retries[pid] += 1
                s.ram_mult[pid] = min(s.max_ram_mult, s.ram_mult[pid] * 2.0)

    # Schedule across pools; fill each pool with multiple assignments if possible.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = int(pool.avail_cpu_pool)
        avail_ram = int(pool.avail_ram_pool)

        # Keep placing while resources remain and there is queued work.
        # Limit iterations to avoid pathological loops when everything is blocked.
        guard = 0
        while avail_cpu > 0 and avail_ram > 0 and guard < 64:
            guard += 1

            pipeline = _pick_next_pipeline(s)
            if pipeline is None:
                break

            # Drop completed/hopeless pipelines.
            if pipeline_is_done_or_hopeless(pipeline):
                continue

            # If there are failures in the DAG, apply a one-step RAM backoff and retry (bounded).
            maybe_backoff_on_failed_ops(pipeline)

            status = pipeline.runtime_status()
            # Choose exactly one ready operator to improve fairness/latency.
            op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
            if not op_list:
                # Nothing ready; requeue so others can proceed.
                _requeue(s, pipeline)
                continue

            # Compute capped allocation for this priority.
            cpu, ram = _cap_resources(s, pipeline.priority, avail_cpu, avail_ram)
            if cpu <= 0 or ram <= 0:
                # Can't schedule anything further in this pool.
                _requeue(s, pipeline)
                break

            # Apply pipeline-level RAM backoff multiplier (bounded by available RAM).
            ram = _apply_ram_backoff(s, pipeline.pipeline_id, ram, avail_ram)
            if ram <= 0:
                _requeue(s, pipeline)
                break

            assignments.append(
                Assignment(
                    ops=op_list,
                    cpu=cpu,
                    ram=ram,
                    priority=pipeline.priority,
                    pool_id=pool_id,
                    pipeline_id=pipeline.pipeline_id,
                )
            )

            # Optimistically decrement available resources for additional placements in this tick.
            avail_cpu -= cpu
            avail_ram -= ram

            # Requeue the pipeline to allow interleaving (reduces head-of-line blocking).
            _requeue(s, pipeline)

    return suspensions, assignments
