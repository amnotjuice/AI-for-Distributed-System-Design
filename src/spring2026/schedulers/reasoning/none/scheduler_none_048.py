# policy_key: scheduler_none_048
# reasoning_effort: none
# model: gpt-5.2-2025-12-11
# llm_cost: 0.045891
# generation_seconds: 45.10
# generated_at: 2026-03-14T02:16:04.789184
@register_scheduler_init(key="scheduler_none_048")
def scheduler_none_048_init(s):
    """
    Priority-aware FIFO with minimal preemption + OOM-driven RAM backoff.

    Improvements over naive FIFO:
    1) Maintain separate queues per priority and always schedule higher priority first.
    2) Right-size CPU/RAM per op using simple heuristics (avoid grabbing entire pool).
    3) On OOM, increase RAM request for that op next time (bounded exponential backoff).
    4) If a high-priority pipeline arrives and no headroom exists, preempt a single
       low-priority running container (best-effort) to create room.

    Design goal: reduce tail latency for QUERY/INTERACTIVE without starving BATCH.
    """
    # Waiting queues per priority class
    s.waiting_queues = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }

    # Per-(pipeline_id, op_id) RAM override after OOMs; values are absolute RAM requests
    s.op_ram_override = {}  # (pipeline_id, op_id) -> ram

    # Track consecutive OOMs per op to scale RAM more aggressively
    s.op_oom_count = {}  # (pipeline_id, op_id) -> int

    # Basic aging: after N scheduler ticks, let lower priorities run even if higher queue non-empty
    s.tick = 0
    s.aging_every = 25  # periodically allow a batch op through (anti-starvation)

    # Limits
    s.max_ram_fraction = 0.9  # never request more than 90% of pool RAM for a single assignment
    s.max_cpu_fraction = 0.9  # never request more than 90% of pool CPU for a single assignment

    # Heuristic defaults (per-op sizing)
    s.default_cpu = {
        Priority.QUERY: 2.0,
        Priority.INTERACTIVE: 2.0,
        Priority.BATCH_PIPELINE: 4.0,
    }
    s.default_ram_frac = {
        Priority.QUERY: 0.25,
        Priority.INTERACTIVE: 0.33,
        Priority.BATCH_PIPELINE: 0.50,
    }


def _priority_order_with_aging(s):
    # Mostly strict priority, but occasionally allow batch first to prevent starvation.
    if s.tick % s.aging_every == 0:
        return [Priority.BATCH_PIPELINE, Priority.QUERY, Priority.INTERACTIVE]
    return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]


def _pick_op(pipeline):
    # Pick a single ready op (parents complete). Keep behavior close to naive.
    status = pipeline.runtime_status()
    op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
    return op_list


def _op_key(pipeline, op):
    # We rely on op having a stable identifier attribute; fall back to repr for safety.
    op_id = getattr(op, "op_id", None)
    if op_id is None:
        op_id = getattr(op, "operator_id", None)
    if op_id is None:
        op_id = getattr(op, "id", None)
    if op_id is None:
        op_id = repr(op)
    return (pipeline.pipeline_id, op_id)


def _is_oom_error(err):
    if err is None:
        return False
    s = str(err).lower()
    return ("oom" in s) or ("out of memory" in s) or ("killed" in s and "memory" in s)


def _compute_request(s, pool, pipeline, op):
    # CPU: small fixed default per priority, capped by pool availability.
    cpu_req = float(s.default_cpu.get(pipeline.priority, 2.0))
    cpu_cap = max(0.0, float(pool.avail_cpu_pool) * s.max_cpu_fraction)
    cpu_req = min(cpu_req, cpu_cap)
    if cpu_req <= 0:
        cpu_req = 0.0

    # RAM: fraction of pool availability, with OOM overrides.
    ram_req = float(pool.avail_ram_pool) * float(s.default_ram_frac.get(pipeline.priority, 0.33))
    ram_cap = max(0.0, float(pool.avail_ram_pool) * s.max_ram_fraction)
    ram_req = min(ram_req, ram_cap)

    k = _op_key(pipeline, op)
    if k in s.op_ram_override:
        ram_req = min(float(s.op_ram_override[k]), ram_cap)

    # Ensure non-zero requests when any resource exists.
    if ram_req <= 0 and pool.avail_ram_pool > 0:
        ram_req = min(1.0, ram_cap)  # 1 "unit" minimal
    if cpu_req <= 0 and pool.avail_cpu_pool > 0:
        cpu_req = min(1.0, cpu_cap)  # 1 vCPU minimal (if fractional supported, this is OK)

    return cpu_req, ram_req


def _collect_running_containers(results):
    # Build a best-effort map of currently running containers from the latest tick's results.
    # We only know about containers that emitted an event last tick, but it's enough to do
    # minimal "preempt one low-priority" when needed.
    running = []
    for r in results or []:
        # If a result has a container_id and did not fail, assume it was running/ran recently.
        if getattr(r, "container_id", None) is None:
            continue
        # We avoid preempting failed containers.
        if hasattr(r, "failed") and r.failed():
            continue
        running.append(r)
    return running


def _choose_preemption_candidate(running_results):
    # Prefer preempting lowest priority (BATCH first), then INTERACTIVE, never QUERY.
    # Among those, pick the one with largest RAM allocation to free headroom quickly.
    cand = None
    for r in running_results:
        pr = getattr(r, "priority", None)
        if pr == Priority.QUERY:
            continue
        if cand is None:
            cand = r
            continue

        def score(x):
            # lower priority => higher preemption score
            p = getattr(x, "priority", Priority.BATCH_PIPELINE)
            pri_score = 0
            if p == Priority.BATCH_PIPELINE:
                pri_score = 3
            elif p == Priority.INTERACTIVE:
                pri_score = 2
            elif p == Priority.QUERY:
                pri_score = 0
            ram = float(getattr(x, "ram", 0.0) or 0.0)
            return (pri_score, ram)

        if score(r) > score(cand):
            cand = r
    return cand


@register_scheduler(key="scheduler_none_048")
def scheduler_none_048(s, results, pipelines):
    """
    Step function:
    - Ingest new pipelines into per-priority queues.
    - Process results: on OOM failures, increase RAM override for that op next retry.
    - For each pool: schedule 1 ready op from the highest-priority non-empty queue.
      If a high-priority job is waiting but pool has no resources, preempt one
      low-priority container (best effort).
    """
    s.tick += 1

    # Enqueue new pipelines
    for p in pipelines or []:
        q = s.waiting_queues.get(p.priority)
        if q is None:
            # Unknown priority: treat as batch
            s.waiting_queues[Priority.BATCH_PIPELINE].append(p)
        else:
            q.append(p)

    # Update RAM overrides from failures (OOM backoff)
    for r in results or []:
        if not (hasattr(r, "failed") and r.failed()):
            continue
        if not _is_oom_error(getattr(r, "error", None)):
            continue

        # Attempt to identify the failed op
        ops = getattr(r, "ops", None) or []
        if not ops:
            continue
        op = ops[0]

        # We need pipeline_id; it exists in Assignment but may not in result. Try best-effort.
        pipeline_id = getattr(r, "pipeline_id", None)
        if pipeline_id is None:
            # If not provided, we cannot track per pipeline reliably; skip.
            continue

        # Compute a stable key
        op_id = getattr(op, "op_id", None)
        if op_id is None:
            op_id = getattr(op, "operator_id", None)
        if op_id is None:
            op_id = getattr(op, "id", None)
        if op_id is None:
            op_id = repr(op)
        k = (pipeline_id, op_id)

        # Increase RAM request: double previous request or start from last allocation.
        prev = s.op_ram_override.get(k, None)
        base = float(prev) if prev is not None else float(getattr(r, "ram", 0.0) or 0.0)
        if base <= 0:
            base = 1.0

        cnt = s.op_oom_count.get(k, 0) + 1
        s.op_oom_count[k] = cnt

        # Exponential with mild aggressiveness: 2x, but cap growth a bit for repeated OOMs
        new_ram = base * (2.0 if cnt <= 3 else 1.5)
        s.op_ram_override[k] = new_ram

    # Early exit if nothing happened
    if not (pipelines or results):
        return [], []

    suspensions = []
    assignments = []

    running = _collect_running_containers(results or [])

    # Helper: check if any high-priority work is waiting (ready or not)
    def has_waiting(priority):
        q = s.waiting_queues.get(priority, [])
        return len(q) > 0

    # Main scheduling loop: one assignment per pool per tick (simple, stable)
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)

        # If no resources, consider preemption only if high priority is queued
        if (avail_cpu <= 0 or avail_ram <= 0) and (has_waiting(Priority.QUERY) or has_waiting(Priority.INTERACTIVE)):
            cand = _choose_preemption_candidate([r for r in running if getattr(r, "pool_id", None) == pool_id])
            if cand is not None:
                suspensions.append(Suspend(container_id=cand.container_id, pool_id=pool_id))
                # After issuing a suspension, don't also schedule in same tick (avoid thrash).
                continue

        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # Choose next pipeline by priority (with aging)
        chosen_pipeline = None
        chosen_queue = None

        for pr in _priority_order_with_aging(s):
            q = s.waiting_queues.get(pr, [])
            # Scan a small prefix to find a pipeline with a ready op; keep FIFO-ish behavior.
            scan_n = min(8, len(q))
            for i in range(scan_n):
                p = q[i]
                status = p.runtime_status()

                # Drop completed pipelines
                if status.is_pipeline_successful():
                    q.pop(i)
                    break

                # Drop pipelines with failures that are not retryable (we only retry OOM via normal flow)
                has_failures = status.state_counts[OperatorState.FAILED] > 0
                if has_failures:
                    # Keep it in queue; failed ops may be retried depending on ASSIGNABLE_STATES
                    pass

                op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
                if op_list:
                    chosen_pipeline = p
                    chosen_queue = q
                    # Move chosen to front-pop behavior (preserve relative order among others)
                    q.pop(i)
                    break
            if chosen_pipeline is not None:
                break

        if chosen_pipeline is None:
            continue

        op_list = _pick_op(chosen_pipeline)
        if not op_list:
            # Not ready; push back
            chosen_queue.append(chosen_pipeline)
            continue

        # Size request
        cpu_req, ram_req = _compute_request(s, pool, chosen_pipeline, op_list[0])

        # If we couldn't request anything meaningful, push back
        if cpu_req <= 0 or ram_req <= 0:
            chosen_queue.append(chosen_pipeline)
            continue

        # Clamp to pool availability to avoid over-asking and getting rejected downstream
        cpu_req = min(cpu_req, avail_cpu)
        ram_req = min(ram_req, avail_ram)

        assignments.append(
            Assignment(
                ops=op_list,
                cpu=cpu_req,
                ram=ram_req,
                priority=chosen_pipeline.priority,
                pool_id=pool_id,
                pipeline_id=chosen_pipeline.pipeline_id,
            )
        )

        # Requeue pipeline to allow subsequent ops later
        chosen_queue.append(chosen_pipeline)

    return suspensions, assignments
