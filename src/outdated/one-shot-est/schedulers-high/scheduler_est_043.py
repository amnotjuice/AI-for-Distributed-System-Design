# policy_key: scheduler_est_043
# reasoning_effort: high
# model: gpt-5.2-2025-12-11
# llm_cost: 0.072137
# generation_seconds: 110.28
# generated_at: 2026-03-31T19:43:17.550970
@register_scheduler_init(key="scheduler_est_043")
def scheduler_est_043_init(s):
    """
    Priority-aware, estimate-aware scheduler (incremental improvement over naive FIFO).

    Key ideas (kept intentionally simple / "small improvements first"):
    1) Priority queues: always try to run QUERY/INTERACTIVE work before BATCH.
    2) Right-size RAM using op.estimate.mem_peak_gb with a small safety factor (avoid giving all RAM to one op).
    3) OOM-aware retries: if an op fails with an OOM-like error, retry it with increased RAM multiplier.
    4) Gentle fairness: round-robin within each priority queue so one pipeline can't dominate.
    """
    from collections import deque

    # Round-robin queues per priority
    s.q_by_prio = {
        Priority.QUERY: deque(),
        Priority.INTERACTIVE: deque(),
        Priority.BATCH_PIPELINE: deque(),
    }

    # Track which pipelines are already queued to avoid duplicates
    s.queued_pipeline_ids = set()

    # Per-op adaptive RAM multiplier and failure tracking (best-effort, tolerant to missing IDs)
    # key: (pipeline_id_or_none, id(op))
    s.op_ram_mult = {}
    s.op_retries = {}
    s.op_permanent_fail = set()

    # Per-pipeline metadata (e.g., enqueue time for basic aging)
    s.pipeline_enqueued_tick = {}

    # Scheduler tick counter for crude aging/backoff decisions
    s.tick = 0

    # Tuning knobs (kept conservative)
    s.min_cpu = 1.0
    s.min_ram_gb = 0.25

    # Default RAM safety factors by priority
    s.base_ram_mult = {
        Priority.QUERY: 1.25,
        Priority.INTERACTIVE: 1.20,
        Priority.BATCH_PIPELINE: 1.10,
    }
    s.max_ram_mult = 4.0

    # Retry policy
    s.max_oom_retries = 5
    s.max_non_oom_retries = 1  # keep low to avoid infinite retry loops for genuine errors

    # Prefer keeping pool 0 "friendly" for interactive if multiple pools exist
    s.interactive_pool_id = 0

    # When pool 0 is idle for high-priority work, allow batch to use it
    s.batch_spill_age_ticks = 25  # small, but non-zero to avoid instant batch takeover


@register_scheduler(key="scheduler_est_043")
def scheduler_est_043(s, results: List["ExecutionResult"], pipelines: List["Pipeline"]) -> Tuple[List["Suspend"], List["Assignment"]]:
    """
    See init docstring for overview.

    Implementation notes:
    - No preemption (yet): we focus on obvious latency wins via priority ordering and right-sizing.
    - We schedule multiple ops per pool per tick while capacity remains, decrementing a local "remaining"
      resource counter (so we don't over-assign).
    """
    s.tick += 1

    def _pipeline_done_or_invalid(p) -> bool:
        st = p.runtime_status()
        return st.is_pipeline_successful()

    def _enqueue_pipeline(p):
        # Avoid duplicates across ticks.
        pid = p.pipeline_id
        if pid in s.queued_pipeline_ids:
            return
        s.queued_pipeline_ids.add(pid)
        s.pipeline_enqueued_tick[pid] = s.tick
        s.q_by_prio[p.priority].append(p)

    def _dequeue_pipeline_tracking(p):
        pid = p.pipeline_id
        s.queued_pipeline_ids.discard(pid)
        s.pipeline_enqueued_tick.pop(pid, None)

    def _result_pipeline_id(res):
        # ExecutionResult likely has pipeline_id, but be defensive.
        return getattr(res, "pipeline_id", None)

    def _is_oom_error(err) -> bool:
        if err is None:
            return False
        try:
            msg = str(err).lower()
        except Exception:
            return False
        return ("oom" in msg) or ("out of memory" in msg) or ("cuda out of memory" in msg) or ("memoryerror" in msg)

    def _op_key(pipeline_id, op):
        # Use pipeline_id when possible to avoid cross-pipeline collisions; fallback to None.
        return (pipeline_id, id(op))

    def _op_est_mem_gb(op) -> float:
        # Estimator exists per problem statement as op.estimate.mem_peak_gb, but be defensive.
        est = None
        try:
            est_obj = getattr(op, "estimate", None)
            if est_obj is not None:
                est = getattr(est_obj, "mem_peak_gb", None)
        except Exception:
            est = None
        if est is None:
            return 0.5  # conservative small default
        try:
            est_f = float(est)
        except Exception:
            return 0.5
        return max(est_f, s.min_ram_gb)

    def _ram_request_gb(p, op, pool_max_ram_gb, remaining_ram_gb) -> float:
        est = _op_est_mem_gb(op)
        base_mult = s.base_ram_mult.get(p.priority, 1.15)
        pid = p.pipeline_id
        k = _op_key(pid, op)
        mult = s.op_ram_mult.get(k, base_mult)

        # Requested RAM is estimate * multiplier, clamped.
        req = est * mult
        req = max(req, s.min_ram_gb)
        req = min(req, pool_max_ram_gb)

        # Can't exceed remaining in this tick's local accounting.
        req = min(req, remaining_ram_gb)
        return req

    def _cpu_request(p, pool_max_cpu, remaining_cpu) -> float:
        # Simple priority CPU caps: give higher priority more CPU, but avoid monopolizing the pool.
        if p.priority == Priority.QUERY:
            cap = max(2.0, 0.60 * float(pool_max_cpu))
        elif p.priority == Priority.INTERACTIVE:
            cap = max(2.0, 0.45 * float(pool_max_cpu))
        else:
            cap = max(1.0, 0.30 * float(pool_max_cpu))

        cpu = min(float(remaining_cpu), float(cap))
        cpu = max(cpu, s.min_cpu)
        cpu = min(cpu, float(remaining_cpu))
        return cpu

    def _prio_iteration_for_pool(pool_id: int):
        # If we have multiple pools, keep pool 0 biased toward interactive work.
        if s.executor.num_pools >= 2 and pool_id == s.interactive_pool_id:
            # Batch can spill into pool 0 only if it has waited long enough and no high-priority is ready.
            return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]
        else:
            # Other pools prefer batch, but still allow spillover for latency.
            return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]

    def _high_prio_waiting() -> bool:
        return bool(s.q_by_prio[Priority.QUERY]) or bool(s.q_by_prio[Priority.INTERACTIVE])

    def _batch_can_use_interactive_pool() -> bool:
        if not s.q_by_prio[Priority.BATCH_PIPELINE]:
            return False
        # If there is any high-priority waiting, don't allow batch into interactive pool.
        if _high_prio_waiting():
            return False
        # Or if batch has been waiting a while, allow it (helps utilization).
        # We check the oldest batch pipeline.
        oldest = s.q_by_prio[Priority.BATCH_PIPELINE][0]
        enq = s.pipeline_enqueued_tick.get(oldest.pipeline_id, s.tick)
        return (s.tick - enq) >= s.batch_spill_age_ticks

    # Ingest new pipelines
    for p in pipelines:
        _enqueue_pipeline(p)

    # If nothing new and nothing queued, return early
    if not pipelines and not results and not any(s.q_by_prio.values()):
        return [], []

    # Process results to adapt RAM multipliers on failures/success.
    for res in results:
        pid = _result_pipeline_id(res)
        failed = False
        try:
            failed = res.failed()
        except Exception:
            failed = bool(getattr(res, "error", None))

        ops = getattr(res, "ops", None) or []
        if not isinstance(ops, list):
            ops = list(ops)

        if failed:
            oom = _is_oom_error(getattr(res, "error", None))
            for op in ops:
                k = _op_key(pid, op)
                s.op_retries[k] = s.op_retries.get(k, 0) + 1
                if oom:
                    # Increase RAM multiplier for this op and allow several retries.
                    cur = s.op_ram_mult.get(k, s.base_ram_mult.get(getattr(res, "priority", None), 1.15))
                    nxt = min(cur * 1.5, s.max_ram_mult)
                    s.op_ram_mult[k] = nxt
                    if s.op_retries[k] > s.max_oom_retries:
                        s.op_permanent_fail.add(k)
                else:
                    # Non-OOM failures: allow only minimal retry, then mark as permanently failing.
                    if s.op_retries[k] > s.max_non_oom_retries:
                        s.op_permanent_fail.add(k)
        else:
            # On success, gently decay multiplier back toward the base.
            pr = getattr(res, "priority", None)
            base = s.base_ram_mult.get(pr, 1.15)
            for op in ops:
                k = _op_key(pid, op)
                if k in s.op_ram_mult:
                    s.op_ram_mult[k] = max(base, s.op_ram_mult[k] * 0.90)

    suspensions: List["Suspend"] = []
    assignments: List["Assignment"] = []

    # Scheduling loop: for each pool, greedily pack assignments in priority order.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        remaining_cpu = float(pool.avail_cpu_pool)
        remaining_ram = float(pool.avail_ram_pool)
        pool_max_cpu = float(pool.max_cpu_pool)
        pool_max_ram = float(pool.max_ram_pool)

        if remaining_cpu < s.min_cpu or remaining_ram < s.min_ram_gb:
            continue

        # Special handling: if this is the "interactive pool", avoid batch unless allowed.
        allow_batch_here = True
        if s.executor.num_pools >= 2 and pool_id == s.interactive_pool_id:
            allow_batch_here = _batch_can_use_interactive_pool()

        # Try to assign multiple ops while we have room.
        # Bound the search to prevent worst-case spinning when nothing is runnable.
        max_attempts = 0
        for pr in [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]:
            max_attempts += len(s.q_by_prio[pr])
        max_attempts = max(8, 4 * max_attempts)

        attempts = 0
        while remaining_cpu >= s.min_cpu and remaining_ram >= s.min_ram_gb and attempts < max_attempts:
            attempts += 1

            picked = None
            picked_pr = None

            for pr in _prio_iteration_for_pool(pool_id):
                if pr == Priority.BATCH_PIPELINE and not allow_batch_here:
                    continue
                q = s.q_by_prio[pr]
                if not q:
                    continue
                picked = q.popleft()
                picked_pr = pr
                break

            if picked is None:
                break

            # Clean up completed pipelines lazily.
            if _pipeline_done_or_invalid(picked):
                _dequeue_pipeline_tracking(picked)
                continue

            status = picked.runtime_status()

            # If the pipeline has a FAILED op that we've deemed permanently failing, drop it to avoid spinning.
            failed_ops = status.get_ops([OperatorState.FAILED], require_parents_complete=False) if hasattr(status, "get_ops") else []
            drop_pipeline = False
            for op in failed_ops[:5]:  # cap scanning to keep it cheap
                k = _op_key(picked.pipeline_id, op)
                if k in s.op_permanent_fail:
                    drop_pipeline = True
                    break
            if drop_pipeline:
                _dequeue_pipeline_tracking(picked)
                continue

            # Get one assignable op (pending or failed) whose parents are complete.
            op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
            if not op_list:
                # Not runnable now; requeue to preserve round-robin fairness.
                s.q_by_prio[picked_pr].append(picked)
                continue

            op = op_list[0]

            # If this specific op is permanently failing, drop the pipeline.
            if _op_key(picked.pipeline_id, op) in s.op_permanent_fail:
                _dequeue_pipeline_tracking(picked)
                continue

            # Resource sizing
            ram_req = _ram_request_gb(picked, op, pool_max_ram, remaining_ram)

            # If we can't give a reasonable amount of RAM, don't schedule to avoid near-certain OOM.
            est = _op_est_mem_gb(op)
            # Require at least 90% of estimated memory unless estimate itself exceeds pool max (then best-effort).
            if est <= pool_max_ram and ram_req < 0.90 * est:
                # Requeue and try another pipeline/op; maybe resources free later.
                s.q_by_prio[picked_pr].append(picked)
                continue

            cpu_req = _cpu_request(picked, pool_max_cpu, remaining_cpu)

            if cpu_req < s.min_cpu or ram_req < s.min_ram_gb:
                s.q_by_prio[picked_pr].append(picked)
                continue

            assignments.append(
                Assignment(
                    ops=op_list,
                    cpu=cpu_req,
                    ram=ram_req,
                    priority=picked.priority,
                    pool_id=pool_id,
                    pipeline_id=picked.pipeline_id,
                )
            )

            remaining_cpu -= float(cpu_req)
            remaining_ram -= float(ram_req)

            # Requeue pipeline so it can make progress on later ops (or be picked again if still runnable).
            s.q_by_prio[picked_pr].append(picked)

    return suspensions, assignments
