# policy_key: scheduler_high_037
# reasoning_effort: high
# exp: reasoning
# model: gpt-5.2-2025-12-11
# llm_cost: 0.129217
# generation_seconds: 216.36
# generated_at: 2026-04-10T02:58:45.790341
@register_scheduler_init(key="scheduler_high_037")
def scheduler_high_037_init(s):
    """
    Priority-aware, failure-averse scheduler.

    Core ideas:
      - Weighted fair scheduling across priorities using deficit round-robin quanta
        (query:10, interactive:5, batch:1) to match the scoring objective emphasis.
      - Conservative RAM allocation for high-priority work, plus per-operator RAM hint
        escalation on OOM to drive completion rate up (failures are heavily penalized).
      - Soft headroom reservation: when higher-priority queues are non-empty, batch (and
        sometimes interactive) assignments leave CPU/RAM slack to reduce tail latency.
      - Retry FAILED ops first (within a pipeline) to recover quickly and finish pipelines.
    """
    from collections import deque

    s.q_query = deque()
    s.q_interactive = deque()
    s.q_batch = deque()

    # Deficit round-robin across priority classes (scheduling opportunities, not time)
    s.pri_order = [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]
    s.quantum = {
        Priority.QUERY: 10,
        Priority.INTERACTIVE: 5,
        Priority.BATCH_PIPELINE: 1,
    }
    s.deficit = {p: 0 for p in s.pri_order}
    s.rr_idx = 0
    s.max_deficit = 200  # cap to avoid unbounded buildup

    # Per-operator adaptive RAM hints and retry tracking (keyed by (pipeline_id, id(op)))
    s.ram_hint_by_op = {}     # op_key -> ram
    s.retry_by_op = {}        # op_key -> int
    s.last_error_by_op = {}   # op_key -> str

    # Map running/completed results back to pipeline/operator context
    s.op_meta = {}  # id(op) -> dict(op_key=..., pipeline_id=..., priority=..., last_ram=..., last_cpu=...)

    # Avoid infinite thrashing on non-OOM failures (OOM gets aggressive RAM escalation instead)
    s.max_non_oom_retries = {
        Priority.QUERY: 2,
        Priority.INTERACTIVE: 2,
        Priority.BATCH_PIPELINE: 1,
    }
    s.max_oom_retries = {
        Priority.QUERY: 6,
        Priority.INTERACTIVE: 5,
        Priority.BATCH_PIPELINE: 4,
    }

    # Limit how many new assignments we emit per pool per tick (packing without too much churn)
    s.max_assignments_per_pool = 6


def _q_for_pri(s, pri):
    if pri == Priority.QUERY:
        return s.q_query
    if pri == Priority.INTERACTIVE:
        return s.q_interactive
    return s.q_batch


def _is_oom_error(err) -> bool:
    if not err:
        return False
    e = str(err).lower()
    return ("oom" in e) or ("out of memory" in e) or ("memoryerror" in e) or ("cuda out of memory" in e)


def _global_max_ram(s):
    m = 0.0
    for i in range(s.executor.num_pools):
        pool = s.executor.pools[i]
        if pool.max_ram_pool > m:
            m = pool.max_ram_pool
    return m


def _op_key(pipeline_id, op):
    # Use object identity for determinism within a single simulation run.
    return (pipeline_id, id(op))


def _select_priority(s):
    """
    Pick a priority class to serve.
    Prefer classes with positive deficit (weighted fairness), otherwise fall back to
    strict priority to avoid deadlock when deficits run out.
    Returns: (priority, consume_deficit_bool) or (None, False)
    """
    n = len(s.pri_order)

    # Deficit-aware round-robin
    for _ in range(n):
        pri = s.pri_order[s.rr_idx]
        s.rr_idx = (s.rr_idx + 1) % n
        if len(_q_for_pri(s, pri)) > 0 and s.deficit.get(pri, 0) > 0:
            return pri, True

    # Fallback: strict priority (query > interactive > batch)
    for pri in s.pri_order:
        if len(_q_for_pri(s, pri)) > 0:
            return pri, False

    return None, False


def _base_fracs(priority, has_other_waiting: bool):
    """
    Per-container target sizing (fractions of pool max) to allow some concurrency.
    If nothing else is waiting, be more aggressive for latency.
    """
    if priority == Priority.QUERY:
        return (0.70, 0.55) if not has_other_waiting else (0.55, 0.45)  # (cpu_frac, ram_frac)
    if priority == Priority.INTERACTIVE:
        return (0.55, 0.45) if not has_other_waiting else (0.45, 0.38)
    return (0.45, 0.35) if not has_other_waiting else (0.35, 0.30)


def _reserve_fracs(priority):
    """
    Headroom reservation fractions used when higher-priority queues are non-empty.
    """
    if priority == Priority.INTERACTIVE:
        return (0.15, 0.15)  # reserve for query
    if priority == Priority.BATCH_PIPELINE:
        return (0.25, 0.25)  # reserve for query+interactive
    return (0.0, 0.0)


def _compute_allocation(s, pool, avail_cpu, avail_ram, priority, op_key, higher_waiting: bool):
    """
    Compute (cpu, ram) for the container. Returns (cpu, ram) or (None, None) if it should not run now.
    """
    # Base fractions aim to keep concurrency while giving enough to finish quickly.
    has_other_waiting = (
        (len(s.q_query) + len(s.q_interactive) + len(s.q_batch)) > 1
    )
    cpu_frac, ram_frac = _base_fracs(priority, has_other_waiting=has_other_waiting)

    # Start from fractions of pool max (not current avail), then cap to current avail.
    cpu = min(avail_cpu, max(0.1, pool.max_cpu_pool * cpu_frac))
    ram = min(avail_ram, max(0.1, pool.max_ram_pool * ram_frac))

    # Apply per-op RAM hint (main mechanism to avoid repeated OOMs).
    hint = s.ram_hint_by_op.get(op_key, None)
    if hint is not None:
        # If we can't satisfy the hint with current availability, don't run it here.
        if hint > avail_ram:
            return None, None
        ram = max(ram, hint)

    # Soft reservations to protect higher-priority tail latency.
    if higher_waiting:
        reserve_cpu_frac, reserve_ram_frac = _reserve_fracs(priority)
        reserve_cpu = pool.max_cpu_pool * reserve_cpu_frac
        reserve_ram = pool.max_ram_pool * reserve_ram_frac

        # Try to shrink to leave reserves (especially for batch).
        cpu = min(cpu, max(0.1, avail_cpu - reserve_cpu))
        ram = min(ram, max(0.1, avail_ram - reserve_ram))

        # Never shrink below a learned hint.
        if hint is not None:
            ram = max(ram, hint)

        # If we still can't keep the reserve (due to low availability), skip.
        if (avail_cpu - cpu) < reserve_cpu or (avail_ram - ram) < reserve_ram:
            return None, None

    # Final caps.
    cpu = min(cpu, avail_cpu)
    ram = min(ram, avail_ram)
    if cpu <= 0 or ram <= 0:
        return None, None
    return cpu, ram


@register_scheduler(key="scheduler_high_037")
def scheduler_high_037_scheduler(s, results: List[ExecutionResult], pipelines: List[Pipeline]):
    """
    Scheduler step:
      1) Ingest arrivals into priority queues.
      2) Update RAM hints and retry counters from execution results (OOM-aware).
      3) Add deficit quanta (weighted fairness).
      4) For each pool, greedily pack assignments while leaving headroom for higher priority.
    """
    suspensions = []
    assignments = []

    # --- 1) Ingest new pipelines ---
    for p in pipelines:
        if p.priority == Priority.QUERY:
            s.q_query.append(p)
        elif p.priority == Priority.INTERACTIVE:
            s.q_interactive.append(p)
        else:
            s.q_batch.append(p)

    # --- 2) Process execution results: adapt RAM hints and retry budgets ---
    gmax_ram = _global_max_ram(s)

    for r in results:
        # Each result can include multiple ops in a container; update per-op metadata.
        for op in (r.ops or []):
            meta = s.op_meta.pop(id(op), None)
            if meta is None:
                continue

            op_key = meta["op_key"]
            pri = meta["priority"]

            if r.failed():
                err = r.error
                s.last_error_by_op[op_key] = str(err) if err is not None else ""

                if _is_oom_error(err):
                    # Escalate RAM hint aggressively to drive completion (OOM is the main avoidable failure).
                    prev = s.ram_hint_by_op.get(op_key, 0.0)
                    new_hint = max(prev, float(r.ram) * 2.0 if r.ram is not None else prev * 2.0)
                    if new_hint <= 0:
                        new_hint = max(prev, 1.0)
                    s.ram_hint_by_op[op_key] = min(new_hint, gmax_ram)

                    s.retry_by_op[op_key] = s.retry_by_op.get(op_key, 0) + 1
                else:
                    # Non-OOM failures: limited retries; don't overreact with RAM.
                    s.retry_by_op[op_key] = s.retry_by_op.get(op_key, 0) + 1
            else:
                # On success, keep hint (safe) but cap it to what worked (helps reduce oversizing).
                if r.ram is not None:
                    prev = s.ram_hint_by_op.get(op_key, None)
                    if prev is not None:
                        s.ram_hint_by_op[op_key] = min(prev, float(r.ram))

                # Reset retry counter on success to avoid over-penalizing later attempts.
                if op_key in s.retry_by_op:
                    s.retry_by_op[op_key] = 0

    # --- 3) Deficit update (weighted fairness across priorities) ---
    for pri, q in s.quantum.items():
        s.deficit[pri] = min(s.max_deficit, s.deficit.get(pri, 0) + q)

    # --- 4) Build assignments pool-by-pool, packing multiple per pool when possible ---
    # Prefer scheduling on pools with more headroom first (helps large/hinted ops place successfully).
    pool_ids = list(range(s.executor.num_pools))
    pool_ids.sort(
        key=lambda pid: (s.executor.pools[pid].avail_ram_pool, s.executor.pools[pid].avail_cpu_pool),
        reverse=True,
    )

    for pool_id in pool_ids:
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool
        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        made = 0
        # Attempt to pack multiple assignments into this pool this tick.
        while made < s.max_assignments_per_pool:
            total_q = len(s.q_query) + len(s.q_interactive) + len(s.q_batch)
            if total_q == 0:
                break
            if avail_cpu <= 0 or avail_ram <= 0:
                break

            # Limit scanning to avoid infinite loops if nothing is runnable right now.
            max_attempts = max(4, total_q)
            scheduled_one = False

            for _ in range(max_attempts):
                pri, consume_deficit = _select_priority(s)
                if pri is None:
                    break

                q = _q_for_pri(s, pri)
                if not q:
                    continue

                pipeline = q.popleft()
                status = pipeline.runtime_status()

                # Drop completed pipelines.
                if status.is_pipeline_successful():
                    continue

                # Prefer retrying FAILED ops first (faster recovery to completion).
                op_list = status.get_ops([OperatorState.FAILED], require_parents_complete=True)[:1]
                if not op_list:
                    op_list = status.get_ops([OperatorState.PENDING], require_parents_complete=True)[:1]
                if not op_list:
                    # Not runnable yet; requeue and try another pipeline.
                    q.append(pipeline)
                    continue

                op = op_list[0]
                op_key = _op_key(pipeline.pipeline_id, op)

                # Enforce retry limits; avoid burning resources on hopeless non-OOM failures.
                retries = s.retry_by_op.get(op_key, 0)
                last_err = s.last_error_by_op.get(op_key, "")
                is_oom = _is_oom_error(last_err)
                if is_oom and retries > s.max_oom_retries.get(pri, 4):
                    # Requeue but de-prioritize by putting it at the end; still allow eventual attempt.
                    q.append(pipeline)
                    continue
                if (not is_oom) and retries > s.max_non_oom_retries.get(pri, 1):
                    q.append(pipeline)
                    continue

                higher_waiting = False
                if pri == Priority.INTERACTIVE:
                    higher_waiting = len(s.q_query) > 0
                elif pri == Priority.BATCH_PIPELINE:
                    higher_waiting = (len(s.q_query) > 0) or (len(s.q_interactive) > 0)

                cpu, ram = _compute_allocation(
                    s=s,
                    pool=pool,
                    avail_cpu=avail_cpu,
                    avail_ram=avail_ram,
                    priority=pri,
                    op_key=op_key,
                    higher_waiting=higher_waiting,
                )
                if cpu is None or ram is None:
                    # Can't place with current headroom; requeue and try others / other pools.
                    q.append(pipeline)
                    continue

                # Emit assignment
                assignment = Assignment(
                    ops=[op],
                    cpu=cpu,
                    ram=ram,
                    priority=pipeline.priority,
                    pool_id=pool_id,
                    pipeline_id=pipeline.pipeline_id,
                )
                assignments.append(assignment)

                # Track metadata to learn from result
                s.op_meta[id(op)] = {
                    "op_key": op_key,
                    "pipeline_id": pipeline.pipeline_id,
                    "priority": pipeline.priority,
                    "last_cpu": cpu,
                    "last_ram": ram,
                    "pool_id": pool_id,
                }

                # Requeue pipeline to allow additional ready ops to be scheduled later.
                q.append(pipeline)

                # Update local availability (packing)
                avail_cpu -= cpu
                avail_ram -= ram

                # Consume deficit only when we actually scheduled work.
                if consume_deficit and s.deficit.get(pri, 0) > 0:
                    s.deficit[pri] -= 1

                made += 1
                scheduled_one = True
                break

            if not scheduled_one:
                break

    return suspensions, assignments
