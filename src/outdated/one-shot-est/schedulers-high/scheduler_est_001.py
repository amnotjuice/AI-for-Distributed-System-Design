# policy_key: scheduler_est_001
# reasoning_effort: high
# model: gpt-5.2-2025-12-11
# llm_cost: 0.000000
# generation_seconds: 173.59
# generated_at: 2026-03-31T18:22:38.566462
@register_scheduler_init(key="scheduler_est_001")
def scheduler_est_001_init(s):
    """
    Priority-aware, estimate-driven scheduler (small step up from naive FIFO).

    Improvements over the naive example:
      1) Priority queues: always try QUERY, then INTERACTIVE, then BATCH.
      2) Right-size RAM using op.estimate.mem_peak_gb with a safety factor.
      3) OOM-aware retries: if an op OOMs, increase its RAM safety factor and retry (bounded).
      4) CPU capping: avoid giving a single op the entire pool CPU by default, so multiple ops
         (especially high-priority) can make progress concurrently.
      5) Soft isolation when multiple pools exist: prefer pool 0 for latency-sensitive work by
         not scheduling batch there unless no high-priority work is waiting.
    """
    s.queues = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }

    # Track which pipelines we've enqueued so we don't duplicate them.
    s.enqueued_pipeline_ids = set()

    # Ops are tracked by Python object identity (id(op)); in the simulator, result.ops should
    # reference the same op objects we schedule from pipeline.runtime_status().
    s.op_mem_mult = {}          # op_key -> RAM safety multiplier
    s.op_retries = {}           # op_key -> retry count (only OOM retries are attempted)
    s.op_to_pipeline_id = {}    # op_key -> pipeline_id (so we can "kill" a pipeline on non-OOM failures)

    s.dead_pipelines = set()    # pipeline_id -> do not schedule again (non-OOM failure or too many retries)

    # Tuning knobs (kept intentionally simple).
    s.base_mem_safety = 1.20
    s.max_mem_safety = 6.0
    s.max_oom_retries = 3

    s.min_ram_gb = 0.5
    s.min_cpu = 1.0

    # CPU caps per container by priority (fraction of pool max CPU).
    s.cpu_cap_frac = {
        Priority.QUERY: 0.75,
        Priority.INTERACTIVE: 0.50,
        Priority.BATCH_PIPELINE: 0.35,
    }

    # When high-priority work is waiting, keep some headroom before scheduling batch in a pool.
    s.reserve_frac_cpu_for_high = 0.25
    s.reserve_frac_ram_for_high = 0.25

    # Simple anti-starvation: after many high-priority placements in a row, allow a batch placement.
    s.high_streak = 0
    s.high_streak_limit = 8


def _is_oom_error(err) -> bool:
    if not err:
        return False
    msg = str(err).lower()
    return ("oom" in msg) or ("out of memory" in msg) or ("out_of_memory" in msg) or ("memoryerror" in msg)


def _op_estimated_mem_gb(op, default_gb: float) -> float:
    est = getattr(op, "estimate", None)
    if est is None:
        return default_gb
    v = getattr(est, "mem_peak_gb", None)
    if v is None:
        return default_gb
    try:
        v = float(v)
    except Exception:
        return default_gb
    return max(0.0, v)


def _dequeue_next_runnable(s, priority):
    """
    Returns (pipeline, op) for the next runnable operator of the given priority.
    Rotates the queue to avoid head-of-line blocking when pipelines have no runnable ops yet.
    """
    q = s.queues[priority]
    n = len(q)
    for _ in range(n):
        p = q.pop(0)

        # Drop dead pipelines eagerly.
        if p.pipeline_id in s.dead_pipelines:
            s.enqueued_pipeline_ids.discard(p.pipeline_id)
            continue

        status = p.runtime_status()
        if status.is_pipeline_successful():
            s.enqueued_pipeline_ids.discard(p.pipeline_id)
            continue

        # Only schedule ops whose parents are complete; allow retrying FAILED ops.
        op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
        if not op_list:
            # Not runnable yet; rotate to back.
            q.append(p)
            continue

        op = op_list[0]
        op_key = id(op)

        # If we've already exceeded retry budget, kill the pipeline.
        if s.op_retries.get(op_key, 0) > s.max_oom_retries:
            s.dead_pipelines.add(p.pipeline_id)
            s.enqueued_pipeline_ids.discard(p.pipeline_id)
            continue

        return p, op

    return None, None


def _compute_request(s, pool, priority, op):
    """
    Compute (cpu_req, ram_req) for this op under this pool.
    RAM: estimate-based with OOM-driven multiplier.
    CPU: capped by priority to avoid a single op monopolizing a pool.
    """
    op_key = id(op)

    est_mem = _op_estimated_mem_gb(op, default_gb=s.min_ram_gb)
    mult = s.op_mem_mult.get(op_key, s.base_mem_safety)
    ram_req = max(s.min_ram_gb, est_mem * mult)

    # Never request more than the pool can provide; if it's truly too big, it will OOM and we'll stop after retries.
    ram_req = min(ram_req, float(pool.max_ram_pool))

    cpu_cap = max(s.min_cpu, float(pool.max_cpu_pool) * float(s.cpu_cap_frac.get(priority, 0.35)))
    cpu_req = min(cpu_cap, float(pool.max_cpu_pool))

    return cpu_req, ram_req


@register_scheduler(key="scheduler_est_001")
def scheduler_est_001_scheduler(s, results: List["ExecutionResult"], pipelines: List["Pipeline"]) -> Tuple[List["Suspend"], List["Assignment"]]:
    # Admit new pipelines into their priority queues.
    for p in pipelines:
        if p.pipeline_id in s.dead_pipelines:
            continue
        if p.pipeline_id in s.enqueued_pipeline_ids:
            continue
        s.queues[p.priority].append(p)
        s.enqueued_pipeline_ids.add(p.pipeline_id)

    # If nothing changed, return quickly.
    if not pipelines and not results:
        return [], []

    # Process results to adapt memory sizing and decide whether to kill pipelines on non-OOM failures.
    for r in results:
        # Some results may contain multiple ops (e.g., if simulator batches); treat each op the same.
        for op in (r.ops or []):
            op_key = id(op)

            if r.failed():
                if _is_oom_error(r.error):
                    # Increase RAM multiplier and retry (bounded).
                    prev = s.op_mem_mult.get(op_key, s.base_mem_safety)
                    s.op_mem_mult[op_key] = min(s.max_mem_safety, prev * 1.5)
                    s.op_retries[op_key] = s.op_retries.get(op_key, 0) + 1

                    # If we now exceeded retries, kill the pipeline so we don't spin forever.
                    if s.op_retries[op_key] > s.max_oom_retries:
                        pid = s.op_to_pipeline_id.get(op_key, None)
                        if pid is not None:
                            s.dead_pipelines.add(pid)
                            s.enqueued_pipeline_ids.discard(pid)
                else:
                    # Non-OOM failure: mark the pipeline dead (no retries).
                    pid = s.op_to_pipeline_id.get(op_key, None)
                    if pid is not None:
                        s.dead_pipelines.add(pid)
                        s.enqueued_pipeline_ids.discard(pid)
            else:
                # Success: gently decay multiplier back toward base safety.
                if op_key in s.op_mem_mult:
                    s.op_mem_mult[op_key] = max(s.base_mem_safety, s.op_mem_mult[op_key] * 0.90)

    suspensions: List["Suspend"] = []
    assignments: List["Assignment"] = []

    # Helper: whether we have any high-priority work waiting.
    def high_waiting() -> bool:
        return (len(s.queues[Priority.QUERY]) > 0) or (len(s.queues[Priority.INTERACTIVE]) > 0)

    # Soft isolation: if multiple pools exist, prefer pool 0 for latency-sensitive work by default.
    latency_pool_id = 0 if s.executor.num_pools > 1 else None

    # Schedule across pools; within each pool, place multiple containers as long as resources permit.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)

        if avail_cpu < s.min_cpu or avail_ram < s.min_ram_gb:
            continue

        # If high-priority work is waiting, keep some headroom from being consumed by batch.
        reserve_cpu = float(pool.max_cpu_pool) * float(s.reserve_frac_cpu_for_high) if high_waiting() else 0.0
        reserve_ram = float(pool.max_ram_pool) * float(s.reserve_frac_ram_for_high) if high_waiting() else 0.0

        # Keep placing work while there is space.
        while True:
            if avail_cpu < s.min_cpu or avail_ram < s.min_ram_gb:
                break

            hw = high_waiting()

            # Anti-starvation: occasionally allow a batch placement even with high-priority waiting,
            # but only if this pool has headroom beyond the high-priority reserve.
            allow_batch_now = (not hw) or (s.high_streak >= s.high_streak_limit)

            # Soft isolation: in the latency pool, only schedule batch if no high-priority is waiting.
            if latency_pool_id is not None and pool_id == latency_pool_id and hw:
                allow_batch_now = False

            # Choose which priority to schedule next.
            chosen_priority = None
            if len(s.queues[Priority.QUERY]) > 0:
                chosen_priority = Priority.QUERY
            elif len(s.queues[Priority.INTERACTIVE]) > 0:
                chosen_priority = Priority.INTERACTIVE
            elif allow_batch_now and len(s.queues[Priority.BATCH_PIPELINE]) > 0:
                chosen_priority = Priority.BATCH_PIPELINE
            else:
                break  # nothing schedulable per our rules

            # If chosen is batch while high waiting, enforce reserve headroom.
            effective_avail_cpu = avail_cpu
            effective_avail_ram = avail_ram
            if chosen_priority == Priority.BATCH_PIPELINE and hw:
                effective_avail_cpu = max(0.0, avail_cpu - reserve_cpu)
                effective_avail_ram = max(0.0, avail_ram - reserve_ram)
                if effective_avail_cpu < s.min_cpu or effective_avail_ram < s.min_ram_gb:
                    # Not enough headroom to run batch without violating reserve.
                    break

            # Pick a runnable (pipeline, op) from that priority.
            pipeline, op = _dequeue_next_runnable(s, chosen_priority)
            if pipeline is None or op is None:
                # Nothing runnable at this priority; try falling through to lower priority.
                # We do a small, explicit fallback chain without getting too clever.
                if chosen_priority == Priority.QUERY:
                    pipeline, op = _dequeue_next_runnable(s, Priority.INTERACTIVE)
                    chosen_priority = Priority.INTERACTIVE if pipeline is not None else chosen_priority
                if pipeline is None and op is None:
                    if allow_batch_now:
                        pipeline, op = _dequeue_next_runnable(s, Priority.BATCH_PIPELINE)
                        chosen_priority = Priority.BATCH_PIPELINE if pipeline is not None else chosen_priority
                if pipeline is None or op is None:
                    break

            # Compute request for this op.
            cpu_req, ram_req = _compute_request(s, pool, chosen_priority, op)

            # Respect current availability (and reserve rule already applied for batch via effective_avail_*).
            if cpu_req > effective_avail_cpu or ram_req > effective_avail_ram:
                # Can't fit now; rotate pipeline back and stop trying in this pool for this tick to avoid spinning.
                s.queues[chosen_priority].append(pipeline)
                break

            # Finally, place the op.
            assignments.append(
                Assignment(
                    ops=[op],
                    cpu=cpu_req,
                    ram=ram_req,
                    priority=chosen_priority,
                    pool_id=pool_id,
                    pipeline_id=pipeline.pipeline_id,
                )
            )

            # Record mapping so we can attribute failures back to pipeline_id.
            s.op_to_pipeline_id[id(op)] = pipeline.pipeline_id

            # Put pipeline back (it may have more ops later).
            s.queues[chosen_priority].append(pipeline)

            # Update available resources.
            avail_cpu -= float(cpu_req)
            avail_ram -= float(ram_req)

            # Update streak for anti-starvation.
            if chosen_priority in (Priority.QUERY, Priority.INTERACTIVE):
                s.high_streak += 1
            else:
                s.high_streak = 0

            # If we just served batch due to the streak rule, reset it to give high-priority another run.
            if chosen_priority == Priority.BATCH_PIPELINE and hw:
                s.high_streak = 0

    return suspensions, assignments
