# policy_key: scheduler_high_024
# reasoning_effort: high
# exp: reasoning
# model: gpt-5.2-2025-12-11
# llm_cost: 0.228956
# generation_seconds: 155.50
# generated_at: 2026-04-10T01:56:02.070385
@register_scheduler_init(key="scheduler_high_024")
def scheduler_high_024_init(s):
    """Priority-protecting FIFO with OOM-adaptive RAM hints.

    Goals:
      - Minimize weighted latency with strong protection for QUERY/INTERACTIVE.
      - Avoid failures (720s penalty) by retrying FAILED ops and increasing RAM on OOM.
      - Keep policy simple and robust: per-priority FIFO queues + conservative RAM for high-priority,
        with spillover to any pool when needed, and light aging to avoid total batch starvation.
    """
    from collections import deque

    # Per-priority FIFO queues of pipelines
    s.queues = {
        Priority.QUERY: deque(),
        Priority.INTERACTIVE: deque(),
        Priority.BATCH_PIPELINE: deque(),
    }

    # Pipeline bookkeeping
    s.pipeline_meta = {}  # pipeline_id -> dict(arrival_tick, last_scheduled_tick)
    s.dead_pipelines = set()

    # Per-(pipeline, op) attempt counts to avoid infinite retry loops
    s.op_attempts = {}  # (pipeline_id, op_key) -> int
    s.max_op_attempts = 4

    # Per-op RAM sizing hints (Arrow-native: RAM above minimum doesn't slow execution; OOM is catastrophic)
    s.op_ram_hint = {}  # op_key -> float (last "safe" ram)
    s.op_oom_multiplier = {}  # op_key -> float multiplier (bumps on OOM, decays on success)

    # Aging: allow some batch progress even under sustained high-priority load
    s.tick = 0
    s.batch_aging_threshold = 200  # ticks since last scheduled before batch is allowed to slip in
    s.batch_max_concurrent_when_high = 1  # cap batch issuance when high-priority is waiting


def _op_key(op):
    # Try several common operator identifiers; fall back to repr for stability across ticks.
    for attr in ("op_id", "operator_id", "id", "name"):
        if hasattr(op, attr):
            try:
                v = getattr(op, attr)
                if v is not None:
                    return (attr, str(v))
            except Exception:
                pass
    try:
        return ("repr", repr(op))
    except Exception:
        return ("type", str(type(op)))


def _is_oom_error(err):
    if err is None:
        return False
    try:
        msg = str(err).lower()
    except Exception:
        return False
    return ("oom" in msg) or ("out of memory" in msg) or ("memory" in msg and "exceed" in msg)


def _priority_ram_fraction(priority):
    # Conservative for high priority to reduce OOM risk and protect tail latency.
    if priority == Priority.QUERY:
        return 0.50
    if priority == Priority.INTERACTIVE:
        return 0.40
    return 0.25  # batch


def _priority_cpu_fraction(priority):
    # Give high priority more CPU for latency; batch gets less to preserve concurrency.
    if priority == Priority.QUERY:
        return 0.60
    if priority == Priority.INTERACTIVE:
        return 0.50
    return 0.40  # batch


def _get_ready_op(status):
    # Prefer rerunning FAILED ops (often OOM-related and now fixable via RAM bump),
    # otherwise run next PENDING op whose parents are complete.
    failed_ops = status.get_ops([OperatorState.FAILED], require_parents_complete=True)
    if failed_ops:
        return failed_ops[0]
    pending_ops = status.get_ops([OperatorState.PENDING], require_parents_complete=True)
    if pending_ops:
        return pending_ops[0]
    return None


def _queue_has_ready(q, limit=25):
    # Scan a prefix to avoid O(n) every tick; good enough for gating batch under contention.
    n = min(len(q), limit)
    for i in range(n):
        p = q[i]
        st = p.runtime_status()
        if st.is_pipeline_successful():
            continue
        op = _get_ready_op(st)
        if op is not None:
            return True
    return False


@register_scheduler(key="scheduler_high_024")
def scheduler_high_024(s, results, pipelines):
    s.tick += 1

    # Incorporate new pipeline arrivals into per-priority queues.
    for p in pipelines:
        if p.pipeline_id in s.dead_pipelines:
            continue
        if p.pipeline_id not in s.pipeline_meta:
            s.pipeline_meta[p.pipeline_id] = {
                "arrival_tick": s.tick,
                "last_scheduled_tick": s.tick,
            }
        s.queues[p.priority].append(p)

    # Update RAM hints based on execution results (OOM -> bump multiplier; success -> gentle decay).
    for r in results:
        try:
            ops = r.ops or []
        except Exception:
            ops = []
        for op in ops:
            k = _op_key(op)
            old_mult = s.op_oom_multiplier.get(k, 1.0)

            if hasattr(r, "failed") and r.failed():
                if _is_oom_error(getattr(r, "error", None)):
                    # Aggressive bump on OOM; cap to avoid runaway.
                    new_mult = min(4.0, max(old_mult, 1.0) * 1.6)
                    s.op_oom_multiplier[k] = new_mult

                    # Raise the RAM hint based on the RAM that just failed (next attempt must be higher).
                    try:
                        failed_ram = float(getattr(r, "ram", 0.0) or 0.0)
                    except Exception:
                        failed_ram = 0.0
                    if failed_ram > 0:
                        s.op_ram_hint[k] = max(s.op_ram_hint.get(k, 0.0), failed_ram * 1.5)
                else:
                    # Non-OOM failure: keep multiplier unchanged; still allow a few retries.
                    s.op_oom_multiplier[k] = old_mult
            else:
                # Success: decay multiplier toward 1 and remember RAM used as a safe hint.
                s.op_oom_multiplier[k] = max(1.0, old_mult * 0.9)
                try:
                    used_ram = float(getattr(r, "ram", 0.0) or 0.0)
                except Exception:
                    used_ram = 0.0
                if used_ram > 0:
                    # Keep the larger of existing hint and used_ram to remain conservative.
                    s.op_ram_hint[k] = max(s.op_ram_hint.get(k, 0.0), used_ram)

    # If no state changes that affect decisions, early exit.
    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    # Determine whether high-priority work is ready; used to throttle batch.
    high_ready = _queue_has_ready(s.queues[Priority.QUERY]) or _queue_has_ready(s.queues[Priority.INTERACTIVE])

    # Helper: decide if a batch pipeline has aged enough to run under contention.
    def batch_is_aged(p):
        meta = s.pipeline_meta.get(p.pipeline_id)
        if not meta:
            return False
        waited = s.tick - meta.get("last_scheduled_tick", meta.get("arrival_tick", s.tick))
        return waited >= s.batch_aging_threshold

    # Try to schedule work on each pool.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)

        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # Hard-reserve pool 0 preferentially for high priority when present.
        # This is a simple improvement over FIFO that typically lowers query/interactive tail latency.
        pool0_reserved_for_high = (pool_id == 0 and high_ready)

        # Cap batch issuance when high priority is waiting (spillover still allowed for high priority).
        batch_issued_here = 0
        max_batch_here = s.batch_max_concurrent_when_high if high_ready else 10

        # Limit the number of assignments per pool per tick for stability.
        max_assignments_here = 4
        made_progress = True

        while made_progress and len(assignments) < 10_000 and max_assignments_here > 0:
            made_progress = False

            # Always consider in strict priority order.
            for pri in (Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE):
                q = s.queues[pri]
                if not q:
                    continue

                # Throttle batch under high load, especially on pool 0.
                if pri == Priority.BATCH_PIPELINE:
                    if pool0_reserved_for_high:
                        continue
                    if high_ready and batch_issued_here >= max_batch_here:
                        continue

                # Rotate through the queue once to find a runnable pipeline/op that fits.
                q_len = len(q)
                if q_len == 0:
                    continue

                scheduled_one = False
                for _ in range(q_len):
                    p = q.popleft()
                    if p.pipeline_id in s.dead_pipelines:
                        continue

                    st = p.runtime_status()
                    if st.is_pipeline_successful():
                        # Drop completed pipelines from the queue.
                        s.pipeline_meta.pop(p.pipeline_id, None)
                        continue

                    op = _get_ready_op(st)
                    if op is None:
                        # Not ready yet; keep it in the queue.
                        q.append(p)
                        continue

                    # If this is batch under high pressure, only allow if aged.
                    if pri == Priority.BATCH_PIPELINE and high_ready and not batch_is_aged(p):
                        q.append(p)
                        continue

                    opk = _op_key(op)
                    attempt_key = (p.pipeline_id, opk)
                    attempts = s.op_attempts.get(attempt_key, 0)
                    if attempts >= s.max_op_attempts:
                        # Stop thrashing: mark pipeline dead and do not schedule further.
                        s.dead_pipelines.add(p.pipeline_id)
                        s.pipeline_meta.pop(p.pipeline_id, None)
                        continue

                    # Compute RAM request: base fraction of pool + OOM-adaptive multiplier + hint.
                    base_ram = _priority_ram_fraction(pri) * float(pool.max_ram_pool)
                    hint_ram = float(s.op_ram_hint.get(opk, 0.0) or 0.0)
                    mult = float(s.op_oom_multiplier.get(opk, 1.0) or 1.0)
                    req_ram = max(base_ram, hint_ram) * mult
                    req_ram = min(req_ram, float(pool.max_ram_pool))

                    # Compute CPU request: fraction of pool max, but never exceed what's available.
                    base_cpu = _priority_cpu_fraction(pri) * float(pool.max_cpu_pool)
                    req_cpu = min(float(pool.max_cpu_pool), base_cpu)
                    # Allow fractional CPU; but don't schedule if essentially none available.
                    if avail_cpu < 0.05 or avail_ram < 0.05:
                        q.appendleft(p)
                        continue
                    req_cpu = min(req_cpu, avail_cpu)

                    # If we can't fit the conservative RAM target, wait rather than risk OOM.
                    if req_ram > avail_ram:
                        q.append(p)
                        continue

                    # Ensure non-zero allocations.
                    if req_cpu <= 0 or req_ram <= 0:
                        q.append(p)
                        continue

                    assignments.append(
                        Assignment(
                            ops=[op],
                            cpu=req_cpu,
                            ram=req_ram,
                            priority=p.priority,
                            pool_id=pool_id,
                            pipeline_id=p.pipeline_id,
                        )
                    )

                    # Update local capacity.
                    avail_cpu -= req_cpu
                    avail_ram -= req_ram

                    # Update pipeline/op bookkeeping.
                    s.op_attempts[attempt_key] = attempts + 1
                    meta = s.pipeline_meta.get(p.pipeline_id)
                    if meta:
                        meta["last_scheduled_tick"] = s.tick

                    # Requeue pipeline (it likely has more ops later).
                    q.append(p)

                    if pri == Priority.BATCH_PIPELINE:
                        batch_issued_here += 1

                    scheduled_one = True
                    made_progress = True
                    max_assignments_here -= 1
                    break  # break rotation over pipelines for this priority

                if scheduled_one:
                    break  # restart from highest priority with updated avail resources

            # Stop if we can no longer place anything meaningful.
            if avail_cpu <= 0.05 or avail_ram <= 0.05:
                break

    return suspensions, assignments
