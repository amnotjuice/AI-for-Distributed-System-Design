# policy_key: scheduler_high_034
# reasoning_effort: high
# model: gpt-5.2-2025-12-11
# llm_cost: 0.100631
# generation_seconds: 116.02
# generated_at: 2026-03-14T04:00:31.098730
@register_scheduler_init(key="scheduler_high_034")
def scheduler_high_034_init(s):
    """
    Priority-aware, latency-first scheduler (incremental improvement over naive FIFO).

    Key ideas:
      1) Maintain separate queues per priority and always schedule higher priorities first.
      2) Avoid letting a single batch op grab the entire pool by using per-priority CPU/RAM caps.
      3) Keep a small headroom reserve when there is high-priority backlog (to reduce tail latency).
      4) Basic OOM-adaptive RAM retry: if an op fails with an OOM-like error, increase its RAM estimate.
         (If a FAILED op was never seen OOM-ing, treat it as non-retryable and drop the pipeline.)
      5) If multiple pools exist, treat pool 0 as a "latency pool": avoid scheduling batch there when
         interactive/query backlog exists.
    """
    # Per-priority waiting queues (round-robin within each priority)
    s.queues = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }

    # Simple time/tick counter for optional future aging/debug; kept deterministic
    s.tick = 0

    # Track pipelines we've already enqueued (by pipeline_id)
    s.seen_pipeline_ids = set()

    # OOM-adaptive sizing per operator identity (id(op) as key)
    s.op_ram_est = {}          # op_key -> ram
    s.op_cpu_est = {}          # reserved for future; keep hook (not heavily used)
    s.op_oom_seen = set()      # op_key that has ever produced an OOM-like failure
    s.op_oom_retries = {}      # op_key -> count
    s.op_permafail = set()     # op_key -> do not retry anymore

    # Tuning knobs (keep conservative, small improvements first)
    s.max_oom_retries = 3
    s.max_assignments_per_pool = 4  # allow some concurrency; bounded to reduce churn

    # Per-priority "target" fractions of a pool (caps; we will not exceed available resources)
    s.cpu_frac = {
        Priority.QUERY: 0.75,
        Priority.INTERACTIVE: 0.50,
        Priority.BATCH_PIPELINE: 0.25,
    }
    s.ram_frac = {
        Priority.QUERY: 0.50,
        Priority.INTERACTIVE: 0.40,
        Priority.BATCH_PIPELINE: 0.30,
    }

    # When high-priority backlog exists, keep some headroom by limiting batch allocation
    s.reserve_frac_when_high_backlog = 0.20


def _is_oom_error(err) -> bool:
    if not err:
        return False
    try:
        msg = str(err).lower()
    except Exception:
        return False
    return ("oom" in msg) or ("out of memory" in msg) or ("out-of-memory" in msg) or ("memoryerror" in msg)


def _prio_order():
    # Strict priority ordering for latency protection
    return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]


def _get_first_assignable_op(pipeline):
    status = pipeline.runtime_status()
    op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
    if not op_list:
        return None
    return op_list[0]


def _pipeline_has_non_oom_failed_ops(s, pipeline) -> bool:
    """
    If a pipeline has FAILED ops, only treat them as retryable if we previously saw OOM for those ops.
    Otherwise, assume deterministic failure (e.g., code error) and drop to avoid infinite retries.
    """
    status = pipeline.runtime_status()
    try:
        failed_ops = status.get_ops([OperatorState.FAILED], require_parents_complete=False)
    except Exception:
        return False

    for op in failed_ops:
        op_key = id(op)
        if (op_key not in s.op_oom_seen) and (op_key not in s.op_permafail):
            return True
    return False


def _pop_next_runnable_pipeline(s, q):
    """
    Round-robin within a priority queue:
      - Skip completed pipelines.
      - Drop pipelines with non-OOM failures (to avoid infinite retries).
      - Return the first pipeline that has an assignable op.
    """
    n = len(q)
    for _ in range(n):
        p = q.pop(0)
        status = p.runtime_status()

        if status.is_pipeline_successful():
            continue

        if _pipeline_has_non_oom_failed_ops(s, p):
            # Drop: failed for reasons other than observed OOM
            continue

        op = _get_first_assignable_op(p)
        if op is None:
            # Not runnable yet; keep it in rotation
            q.append(p)
            continue

        # Runnable; caller will schedule or requeue
        return p

    return None


def _has_high_priority_backlog(s) -> bool:
    # Detect whether QUERY/INTERACTIVE have runnable work (cheap scan)
    for prio in (Priority.QUERY, Priority.INTERACTIVE):
        q = s.queues.get(prio, [])
        for p in q:
            status = p.runtime_status()
            if status.is_pipeline_successful():
                continue
            if _pipeline_has_non_oom_failed_ops(s, p):
                continue
            if _get_first_assignable_op(p) is not None:
                return True
    return False


def _clamp_int(x, lo, hi):
    try:
        xi = int(x)
    except Exception:
        xi = int(lo)
    if xi < lo:
        return lo
    if xi > hi:
        return hi
    return xi


@register_scheduler(key="scheduler_high_034")
def scheduler_high_034(s, results: List["ExecutionResult"], pipelines: List["Pipeline"]) -> Tuple[List["Suspend"], List["Assignment"]]:
    """
    Priority-aware scheduler loop.

    - Enqueue new pipelines by priority.
    - Update OOM-based RAM estimates from execution results.
    - For each pool, schedule up to N ops, prioritizing QUERY > INTERACTIVE > BATCH.
    - Use per-priority allocation caps to prevent a single batch op from monopolizing the pool.
    """
    s.tick += 1

    # 1) Enqueue new pipelines
    for p in pipelines:
        pid = getattr(p, "pipeline_id", None)
        if pid in s.seen_pipeline_ids:
            continue
        s.seen_pipeline_ids.add(pid)
        prio = getattr(p, "priority", Priority.BATCH_PIPELINE)
        if prio not in s.queues:
            s.queues[prio] = []
        s.queues[prio].append(p)

    # 2) Process results to learn from OOMs (RAM-only, conservative)
    if results:
        for r in results:
            if not r.failed():
                continue

            if not _is_oom_error(getattr(r, "error", None)):
                continue

            pool_id = getattr(r, "pool_id", 0)
            try:
                pool = s.executor.pools[pool_id]
                max_ram = pool.max_ram_pool
            except Exception:
                max_ram = getattr(r, "ram", 0)

            # Increase per-op RAM estimate for retry
            for op in getattr(r, "ops", []) or []:
                op_key = id(op)
                s.op_oom_seen.add(op_key)

                prev = s.op_ram_est.get(op_key, 0)
                base = getattr(r, "ram", 0) or prev or 1
                new_est = max(prev, base * 2)

                if max_ram:
                    new_est = min(new_est, max_ram)

                s.op_ram_est[op_key] = new_est

                # Retry bookkeeping
                s.op_oom_retries[op_key] = s.op_oom_retries.get(op_key, 0) + 1
                if s.op_oom_retries[op_key] > s.max_oom_retries:
                    # If we still OOM after ramping, stop retrying this op
                    s.op_permafail.add(op_key)

    # Early exit if nothing changed
    if not pipelines and not results:
        return [], []

    suspensions = []  # No preemption in this incremental version (keeps code safe/working)
    assignments = []

    high_backlog = _has_high_priority_backlog(s)

    # 3) Scheduling: fill each pool with bounded concurrency, strict priority order
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool

        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # Latency pool behavior: if there is high-priority backlog, avoid scheduling batch in pool 0
        allow_batch_in_this_pool = True
        if pool_id == 0 and high_backlog:
            allow_batch_in_this_pool = False

        num_assigned = 0
        while num_assigned < s.max_assignments_per_pool and avail_cpu > 0 and avail_ram > 0:
            scheduled_any = False

            for prio in _prio_order():
                if prio == Priority.BATCH_PIPELINE and not allow_batch_in_this_pool:
                    continue

                q = s.queues.get(prio, [])
                if not q:
                    continue

                pipeline = _pop_next_runnable_pipeline(s, q)
                if pipeline is None:
                    continue

                # Re-check runnable op now that we popped it
                op = _get_first_assignable_op(pipeline)
                if op is None:
                    # Put back; nothing runnable
                    q.append(pipeline)
                    continue

                op_key = id(op)
                if op_key in s.op_permafail:
                    # Drop this pipeline to avoid endless loops on a permanently failing op
                    # (We cannot easily mark the pipeline failed; we just stop scheduling it.)
                    continue

                # Compute per-op target sizes (cap to fraction of pool)
                cpu_target = max(1, int(pool.max_cpu_pool * s.cpu_frac.get(prio, 0.25)))
                ram_target = max(1, int(pool.max_ram_pool * s.ram_frac.get(prio, 0.30)))

                # Use learned RAM estimate if present (helps avoid repeated OOM)
                ram_est = s.op_ram_est.get(op_key, ram_target)

                # CPU estimate hook (not learned yet; keep simple)
                cpu_est = s.op_cpu_est.get(op_key, cpu_target)

                # Clamp to available resources
                cpu = _clamp_int(min(avail_cpu, cpu_est), 1, int(avail_cpu))
                ram = _clamp_int(min(avail_ram, ram_est), 1, int(avail_ram))

                # If we're about to schedule batch while high-priority backlog exists,
                # keep some headroom by limiting how much batch can take.
                if prio == Priority.BATCH_PIPELINE and high_backlog:
                    reserve_cpu = max(0, int(pool.max_cpu_pool * s.reserve_frac_when_high_backlog))
                    reserve_ram = max(0, int(pool.max_ram_pool * s.reserve_frac_when_high_backlog))

                    cpu_cap = max(0, int(avail_cpu) - reserve_cpu)
                    ram_cap = max(0, int(avail_ram) - reserve_ram)

                    if cpu_cap < 1 or ram_cap < 1:
                        # Can't schedule batch without violating reserve; put it back and stop trying batch.
                        q.append(pipeline)
                        continue

                    cpu = min(cpu, cpu_cap)
                    ram = min(ram, ram_cap)
                    cpu = _clamp_int(cpu, 1, int(avail_cpu))
                    ram = _clamp_int(ram, 1, int(avail_ram))

                assignment = Assignment(
                    ops=[op],
                    cpu=cpu,
                    ram=ram,
                    priority=pipeline.priority,
                    pool_id=pool_id,
                    pipeline_id=pipeline.pipeline_id,
                )
                assignments.append(assignment)

                # Round-robin: push pipeline back to allow other pipelines to run too
                q.append(pipeline)

                avail_cpu -= cpu
                avail_ram -= ram
                num_assigned += 1
                scheduled_any = True
                break  # re-start priority scan after successful schedule

            if not scheduled_any:
                break

    return suspensions, assignments
