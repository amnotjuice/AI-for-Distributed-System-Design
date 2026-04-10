# policy_key: scheduler_est_048
# reasoning_effort: low
# model: gpt-5.2-2025-12-11
# llm_cost: 0.042034
# generation_seconds: 39.71
# generated_at: 2026-04-01T00:16:46.324391
@register_scheduler_init(key="scheduler_est_048")
def scheduler_est_048_init(s):
    """Priority-aware, resource-slicing scheduler with simple OOM-aware retries.

    Incremental improvements over naive FIFO:
      1) Separate waiting queues by priority (serve high-priority first).
      2) Avoid "one op takes the whole pool": allocate CPU/RAM in reasonable slices to run more ops concurrently.
      3) Use op.estimate.mem_peak_gb as a RAM floor; on failures, retry with increased RAM (bounded retries).
      4) Basic aging for batch: ensure some progress even under constant interactive load.
    """
    # Priority queues: pipelines wait here until they complete.
    s.waiting_by_prio = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }

    # Retry bookkeeping: (pipeline_id, op_key) -> count
    s.retry_counts = {}

    # RAM inflation per op_key based on observed failures: op_key -> multiplier
    s.ram_mult = {}

    # Scheduler tick counter for aging decisions
    s.tick = 0

    # Tunables
    s.max_retries_per_op = 3
    s.default_ram_headroom_mult = 1.20  # apply to estimates to reduce near-floor OOMs
    s.oom_backoff_mult = 1.60           # multiply ram multiplier on failure
    s.max_ram_mult = 4.0

    # CPU slicing defaults (vCPU) by priority; these are "targets", bounded by pool availability
    s.cpu_slice = {
        Priority.QUERY: 4.0,
        Priority.INTERACTIVE: 3.0,
        Priority.BATCH_PIPELINE: 2.0,
    }

    # Minimum CPU to avoid extreme slowdown / thrash
    s.min_cpu_slice = 1.0

    # Batch aging: every N ticks, let one batch op through per pool even if interactive present
    s.batch_aging_period = 5


def _prio_order():
    # Highest to lowest. Treat QUERY as top, then INTERACTIVE, then BATCH.
    return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]


def _op_key(op):
    # Best-effort stable key for per-op learning without relying on unknown fields.
    for attr in ("op_id", "operator_id", "name", "uid"):
        if hasattr(op, attr):
            return str(getattr(op, attr))
    return str(op)


def _pick_ready_op(pipeline):
    status = pipeline.runtime_status()
    # Require parents complete to respect DAG.
    ops = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
    if not ops:
        return None
    return ops[0]


def _pipeline_done_or_hard_failed(s, pipeline):
    status = pipeline.runtime_status()
    if status.is_pipeline_successful():
        return True
    # Only drop a pipeline if it has failures that exceeded retry budget.
    failed_ops = status.get_ops([OperatorState.FAILED], require_parents_complete=False)
    for op in failed_ops:
        k = (pipeline.pipeline_id, _op_key(op))
        if s.retry_counts.get(k, 0) <= s.max_retries_per_op:
            return False
    # If there are failed ops and all exceeded retry, treat as terminal.
    return len(failed_ops) > 0


def _ram_floor_gb(op):
    est = None
    if hasattr(op, "estimate") and op.estimate is not None and hasattr(op.estimate, "mem_peak_gb"):
        est = op.estimate.mem_peak_gb
    # If estimate missing, use a small non-zero floor; allocator will cap by pool.
    if est is None:
        return 1.0
    try:
        est_f = float(est)
        return max(0.1, est_f)
    except Exception:
        return 1.0


def _compute_allocation(s, pool, pipeline_priority, op):
    # CPU: allocate in slices to allow concurrency.
    avail_cpu = pool.avail_cpu_pool
    avail_ram = pool.avail_ram_pool
    if avail_cpu <= 0 or avail_ram <= 0:
        return None

    target_cpu = s.cpu_slice.get(pipeline_priority, 2.0)
    cpu = min(avail_cpu, max(s.min_cpu_slice, target_cpu))

    # RAM: use estimate floor * (base headroom) * (learned multiplier).
    base_floor = _ram_floor_gb(op)
    learned = s.ram_mult.get(_op_key(op), 1.0)
    ram_need = base_floor * s.default_ram_headroom_mult * learned

    # Do not grab the entire pool RAM unless necessary; but also avoid tiny allocations.
    # A reasonable cap per op encourages concurrency under memory pressure.
    # Cap depends on priority: interactive can be greedier to reduce OOM risk.
    if pipeline_priority in (Priority.QUERY, Priority.INTERACTIVE):
        per_op_cap = max(2.0, 0.60 * pool.max_ram_pool)
    else:
        per_op_cap = max(2.0, 0.40 * pool.max_ram_pool)

    ram = min(avail_ram, min(per_op_cap, max(1.0, ram_need)))

    # If we can't meet even the base floor, don't schedule this op now.
    if ram + 1e-9 < min(avail_ram, base_floor):
        return None

    return cpu, ram


@register_scheduler(key="scheduler_est_048")
def scheduler_est_048(s, results, pipelines):
    """
    Priority-aware queueing + concurrent packing + OOM-aware retries.

    Decisions:
      - Admission: enqueue all incoming pipelines into priority queues.
      - Selection: schedule ready ops from highest priority first, with batch aging.
      - Placement: greedy per pool (no cross-pool search), pack multiple ops per tick until pool resources are low.
      - Sizing: allocate CPU/RAM slices; RAM uses estimate floor + headroom; failures inflate RAM for future retries.
      - Preemption: not implemented (insufficient visibility into running containers in the provided interface).
    """
    s.tick += 1

    # Enqueue new pipelines by priority.
    for p in pipelines:
        s.waiting_by_prio[p.priority].append(p)

    # Learn from execution results: if failure, increase RAM multiplier for those ops and bump retry count.
    for r in results:
        if not getattr(r, "failed", lambda: False)():
            continue
        # Any failure is treated as "possibly OOM" in this incremental policy;
        # we respond by increasing RAM to reduce repeated failures.
        for op in getattr(r, "ops", []) or []:
            ok = _op_key(op)
            s.ram_mult[ok] = min(s.max_ram_mult, s.ram_mult.get(ok, 1.0) * s.oom_backoff_mult)
            k = (getattr(r, "pipeline_id", None), ok)

            # If pipeline_id is not present on result, fall back to a best-effort placeholder key
            # to still limit retries (per op, across pipelines).
            if k[0] is None:
                k = ("_unknown_pipeline", ok)

            s.retry_counts[k] = s.retry_counts.get(k, 0) + 1

    # Early exit if no new info and no new pipelines.
    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    # Helper to pop the next runnable pipeline according to priority and aging.
    def pop_next_pipeline(allow_batch):
        # Serve high-priority first; optionally skip batch unless allowed.
        for pr in _prio_order():
            if pr == Priority.BATCH_PIPELINE and not allow_batch:
                continue
            q = s.waiting_by_prio.get(pr, [])
            while q:
                p = q.pop(0)
                # Drop finished or terminally failed pipelines.
                if _pipeline_done_or_hard_failed(s, p):
                    continue
                return p
        return None

    # Pack assignments per pool.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]

        # Batch aging: every N ticks, allow batch to flow; otherwise, keep batch behind.
        allow_batch = (s.tick % s.batch_aging_period == 0)

        # Try to fill the pool with multiple ops (bounded by resource availability).
        # Stop when remaining resources are too small to place another reasonable slice.
        while True:
            if pool.avail_cpu_pool < s.min_cpu_slice or pool.avail_ram_pool < 1.0:
                break

            pipeline = pop_next_pipeline(allow_batch=allow_batch)
            if pipeline is None:
                break

            op = _pick_ready_op(pipeline)
            if op is None:
                # No ready ops yet; requeue the pipeline and try another.
                s.waiting_by_prio[pipeline.priority].append(pipeline)
                continue

            alloc = _compute_allocation(s, pool, pipeline.priority, op)
            if alloc is None:
                # Not enough resources right now; requeue and stop trying to pack this pool.
                s.waiting_by_prio[pipeline.priority].append(pipeline)
                break

            cpu, ram = alloc

            # Enforce retry budget per (pipeline, op).
            k = (pipeline.pipeline_id, _op_key(op))
            if s.retry_counts.get(k, 0) > s.max_retries_per_op:
                # Give up on this pipeline (do not requeue); continue to next pipeline.
                continue

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

            # Requeue pipeline so its subsequent ops can be scheduled later.
            s.waiting_by_prio[pipeline.priority].append(pipeline)

            # Continue packing until pool is tight.

    return suspensions, assignments
