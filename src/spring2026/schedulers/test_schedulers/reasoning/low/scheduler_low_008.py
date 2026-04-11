# policy_key: scheduler_low_008
# reasoning_effort: low
# model: gpt-5.2-2025-12-11
# llm_cost: 0.048115
# generation_seconds: 49.17
# generated_at: 2026-04-09T21:08:15.394591
@register_scheduler_init(key="scheduler_low_008")
def scheduler_low_008_init(s):
    """
    Priority-aware, OOM-averse, non-preemptive scheduler.

    Main ideas:
      - Separate queues per priority (QUERY, INTERACTIVE, BATCH) and do weighted round-robin
        to strongly protect query/interactive latency while still guaranteeing batch progress.
      - Schedule only "ready" operators (parents complete) and only one operator per assignment
        to reduce blast radius when our resource guesses are wrong.
      - Use RAM hints with exponential backoff on failures (especially OOM-like failures),
        to minimize repeated failures (each failure risks turning into a 720s penalty).
      - Conservative initial RAM for high-priority work to reduce the chance of first-run OOM.
      - No preemption (safe default given limited visibility into running containers).
    """
    s.q_query = []
    s.q_interactive = []
    s.q_batch = []

    # (pipeline_id, op_key) -> ram_hint, cpu_hint
    s.ram_hint = {}
    s.cpu_hint = {}

    # (pipeline_id, op_key) -> retry_count
    s.retry_count = {}

    # Weighted RR tokens; replenished each tick.
    s.rr_weights = {
        Priority.QUERY: 10,
        Priority.INTERACTIVE: 5,
        Priority.BATCH_PIPELINE: 1,
    }
    s.rr_tokens = {
        Priority.QUERY: 0,
        Priority.INTERACTIVE: 0,
        Priority.BATCH_PIPELINE: 0,
    }

    # Hard caps to prevent pathological over-allocation.
    s.max_retries_per_op = 4

    # Base RAM fraction of pool max RAM used as a fallback when operator min RAM is unknown.
    s.base_ram_frac = {
        Priority.QUERY: 0.30,
        Priority.INTERACTIVE: 0.22,
        Priority.BATCH_PIPELINE: 0.12,
    }

    # CPU share caps (relative to pool max CPU) by priority.
    s.cpu_frac_cap = {
        Priority.QUERY: 0.75,
        Priority.INTERACTIVE: 0.60,
        Priority.BATCH_PIPELINE: 0.40,
    }


def _priority_of(pipeline):
    pr = getattr(pipeline, "priority", None)
    if pr is None:
        return Priority.BATCH_PIPELINE
    return pr


def _op_key(op):
    # Try a few common identifiers; fall back to object identity.
    return (
        getattr(op, "op_id", None)
        or getattr(op, "operator_id", None)
        or getattr(op, "id", None)
        or getattr(op, "name", None)
        or str(id(op))
    )


def _infer_min_ram(op):
    # Use best-effort hints if present; otherwise None.
    for attr in ("min_ram", "min_memory", "ram_min", "required_ram", "required_memory", "ram"):
        v = getattr(op, attr, None)
        if isinstance(v, (int, float)) and v > 0:
            return float(v)
    return None


def _is_oom_error(err):
    # Best-effort string match. If unknown, treat as generic failure (still increase RAM modestly).
    if err is None:
        return False
    s = str(err).lower()
    return ("oom" in s) or ("out of memory" in s) or ("out-of-memory" in s) or ("memory" in s and "exceed" in s)


def _enqueue_pipeline(s, pipeline):
    pr = _priority_of(pipeline)
    if pr == Priority.QUERY:
        s.q_query.append(pipeline)
    elif pr == Priority.INTERACTIVE:
        s.q_interactive.append(pipeline)
    else:
        s.q_batch.append(pipeline)


def _refill_tokens(s):
    for pr, w in s.rr_weights.items():
        s.rr_tokens[pr] += w


def _pick_queue_order(s):
    # Deterministic priority order with tokens as the main arbiter.
    # If tokens exist for multiple queues, prefer higher weight first to protect tail latency.
    order = [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]
    order.sort(key=lambda pr: (s.rr_tokens.get(pr, 0), s.rr_weights.get(pr, 0)), reverse=True)
    return order


def _queue_for_priority(s, pr):
    if pr == Priority.QUERY:
        return s.q_query
    if pr == Priority.INTERACTIVE:
        return s.q_interactive
    return s.q_batch


def _estimate_resources_for_op(s, pool, pipeline, op, avail_cpu, avail_ram):
    pr = _priority_of(pipeline)
    pid = pipeline.pipeline_id
    ok = _op_key(op)

    # RAM estimation:
    # 1) Use remembered hint if present
    # 2) Else use operator min RAM if known
    # 3) Else use conservative fraction of pool max RAM (more conservative for higher priority)
    hint_ram = s.ram_hint.get((pid, ok))
    min_ram = _infer_min_ram(op)
    base_frac = s.base_ram_frac.get(pr, 0.15)
    base_ram = base_frac * float(pool.max_ram_pool)

    ram_need = hint_ram if hint_ram is not None else (min_ram if min_ram is not None else base_ram)

    # Add a small safety margin above minimums to reduce borderline OOMs.
    if min_ram is not None and hint_ram is None:
        ram_need = max(ram_need, 1.10 * float(min_ram))

    # Cap to pool max and available.
    ram_need = min(float(pool.max_ram_pool), float(ram_need))
    ram_need = min(float(avail_ram), float(ram_need))

    # CPU estimation:
    # Favor giving more CPU to high priority, but don't monopolize the pool.
    hint_cpu = s.cpu_hint.get((pid, ok))
    cap_frac = s.cpu_frac_cap.get(pr, 0.5)
    cpu_cap = max(1.0, cap_frac * float(pool.max_cpu_pool))
    if hint_cpu is None:
        # Default CPU: 1 for batch-ish, 2 for interactive, up to 4 for query if available.
        default_cpu = 1.0
        if pr == Priority.INTERACTIVE:
            default_cpu = 2.0
        elif pr == Priority.QUERY:
            default_cpu = 4.0
        cpu_need = min(default_cpu, cpu_cap)
    else:
        cpu_need = min(float(hint_cpu), cpu_cap)

    # Must fit in available CPU.
    cpu_need = min(float(avail_cpu), float(cpu_need))
    cpu_need = max(0.0, float(cpu_need))

    return cpu_need, ram_need


@register_scheduler(key="scheduler_low_008")
def scheduler_low_008_scheduler(s, results, pipelines):
    """
    Scheduling step:
      - Incorporate new arrivals into priority queues.
      - Update RAM/CPU hints based on failures/successes.
      - For each pool, repeatedly try to place one ready operator at a time using weighted RR.
      - If an operator doesn't fit in current pool headroom, rotate it to the back and try others,
        preventing head-of-line blocking and starvation.
    """
    # Enqueue new pipelines
    for p in pipelines:
        _enqueue_pipeline(s, p)

    # Early exit if nothing happened
    if not pipelines and not results:
        return [], []

    # Update hints from results (success/failure)
    for r in results:
        try:
            pid = getattr(r, "pipeline_id", None)
        except Exception:
            pid = None

        # If pipeline_id isn't present on result, we cannot key hints per pipeline reliably.
        # Fall back to only-op identity in that case (still useful within one simulation run).
        for op in getattr(r, "ops", []) or []:
            ok = _op_key(op)
            key = (pid, ok)

            # On failure: increase RAM (OOM: aggressive; otherwise moderate) and slightly reduce CPU guess
            # to avoid overcommitting if the pool is tight.
            if hasattr(r, "failed") and r.failed():
                rc = s.retry_count.get(key, 0) + 1
                s.retry_count[key] = rc

                prev_ram = s.ram_hint.get(key, getattr(r, "ram", None))
                prev_ram = float(prev_ram) if isinstance(prev_ram, (int, float)) and prev_ram > 0 else None
                used_ram = getattr(r, "ram", None)
                used_ram = float(used_ram) if isinstance(used_ram, (int, float)) and used_ram > 0 else prev_ram

                oomish = _is_oom_error(getattr(r, "error", None))
                if used_ram is not None:
                    if oomish:
                        new_ram = used_ram * 2.0
                    else:
                        new_ram = used_ram * 1.35
                    s.ram_hint[key] = new_ram

                prev_cpu = s.cpu_hint.get(key, getattr(r, "cpu", None))
                prev_cpu = float(prev_cpu) if isinstance(prev_cpu, (int, float)) and prev_cpu > 0 else None
                if prev_cpu is not None:
                    s.cpu_hint[key] = max(1.0, prev_cpu * 0.9)

            else:
                # On success: remember what worked (lightly), enabling stable sizing next time.
                used_ram = getattr(r, "ram", None)
                used_cpu = getattr(r, "cpu", None)
                if isinstance(used_ram, (int, float)) and used_ram > 0:
                    # Keep a small buffer above observed to reduce repeat OOM due to variance.
                    s.ram_hint[key] = max(s.ram_hint.get(key, 0.0), float(used_ram) * 1.05)
                if isinstance(used_cpu, (int, float)) and used_cpu > 0:
                    s.cpu_hint[key] = max(1.0, float(used_cpu))

    suspensions = []
    assignments = []

    # Refill RR tokens each tick so batch still gets periodic service.
    _refill_tokens(s)

    # Scheduling per pool
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)

        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # Try to place multiple operators if resources permit.
        # Use bounded attempts to avoid infinite loops when nothing fits.
        max_attempts = 64
        attempts = 0
        made_any = True

        while made_any and attempts < max_attempts:
            made_any = False
            attempts += 1

            # Build queue order based on current tokens.
            order = _pick_queue_order(s)

            scheduled_this_round = False
            for pr in order:
                q = _queue_for_priority(s, pr)
                if not q or s.rr_tokens.get(pr, 0) <= 0:
                    continue

                # Scan up to N pipelines in this queue to find one runnable op that fits.
                scan_n = min(len(q), 16)
                rotated = 0
                while rotated < scan_n and q and s.rr_tokens.get(pr, 0) > 0:
                    pipeline = q.pop(0)
                    rotated += 1

                    status = pipeline.runtime_status()

                    # Drop completed pipelines from queues.
                    if status.is_pipeline_successful():
                        continue

                    # If there are failures, we still try to retry them (ASSIGNABLE_STATES includes FAILED).
                    # But we cap retries per operator using s.max_retries_per_op.
                    ops_ready = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
                    if not ops_ready:
                        # Not ready yet; keep pipeline in queue.
                        q.append(pipeline)
                        continue

                    op = ops_ready[0]
                    pid = pipeline.pipeline_id
                    ok = _op_key(op)
                    key = (pid, ok)

                    if s.retry_count.get(key, 0) > s.max_retries_per_op:
                        # Avoid infinite retry loops; keep pipeline in queue to allow other ops to proceed,
                        # but we won't keep slamming the same op.
                        q.append(pipeline)
                        continue

                    cpu_need, ram_need = _estimate_resources_for_op(
                        s, pool, pipeline, op, avail_cpu=avail_cpu, avail_ram=avail_ram
                    )

                    # If we can't allocate at least 1 CPU or any RAM, we can't schedule.
                    if cpu_need < 1.0 or ram_need <= 0:
                        q.append(pipeline)
                        continue

                    # If it doesn't fit, rotate to back and try another pipeline to prevent HOL blocking.
                    if cpu_need > avail_cpu or ram_need > avail_ram:
                        q.append(pipeline)
                        continue

                    # Create assignment (one operator per container)
                    assignment = Assignment(
                        ops=[op],
                        cpu=cpu_need,
                        ram=ram_need,
                        priority=pipeline.priority,
                        pool_id=pool_id,
                        pipeline_id=pipeline.pipeline_id,
                    )
                    assignments.append(assignment)

                    # Consume pool headroom
                    avail_cpu -= cpu_need
                    avail_ram -= ram_need

                    # Consume one RR token for this priority
                    s.rr_tokens[pr] = max(0, s.rr_tokens.get(pr, 0) - 1)

                    # Re-enqueue pipeline to allow subsequent operators to be scheduled later.
                    q.append(pipeline)

                    scheduled_this_round = True
                    made_any = True
                    break

                if scheduled_this_round:
                    break

            # If nothing scheduled, stop trying for this pool.
            if not scheduled_this_round:
                break

            if avail_cpu <= 0 or avail_ram <= 0:
                break

    return suspensions, assignments
