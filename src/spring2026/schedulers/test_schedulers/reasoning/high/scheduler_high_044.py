# policy_key: scheduler_high_044
# reasoning_effort: high
# exp: reasoning
# model: gpt-5.2-2025-12-11
# llm_cost: 0.185469
# generation_seconds: 15127.10
# generated_at: 2026-04-10T07:28:56.767395
@register_scheduler_init(key="scheduler_high_044")
def scheduler_high_044_init(s):
    """Priority-aware, OOM-adaptive, non-preemptive scheduler.

    Main ideas:
      - Strict priority ordering: QUERY > INTERACTIVE > BATCH (to minimize weighted latency).
      - Headroom reservation: avoid letting BATCH (and sometimes INTERACTIVE) consume all capacity,
        since we don't rely on preemption and want good tail latency for high priority arrivals.
      - OOM-adaptive RAM floors per operator: start with modest RAM, and on OOM failures, raise a
        per-operator RAM "floor" aggressively to converge quickly and avoid repeated failures.
      - Light SRPT bias for QUERY/INTERACTIVE: pick pipelines with fewer remaining operators first.
    """
    # Per-priority FIFO queues of Pipeline objects
    s.q_query = []
    s.q_interactive = []
    s.q_batch = []

    # Per-operator RAM floors learned from OOM failures (keyed by a stable-ish operator key)
    s.op_ram_floor = {}  # op_key -> ram_amount

    # Simple tick counters to adapt headroom when high-priority has been idle
    s.tick_count = 0
    s.hp_idle_ticks = 0


def _is_oom_error(err) -> bool:
    if not err:
        return False
    e = str(err).lower()
    return ("oom" in e) or ("out of memory" in e) or ("out-of-memory" in e) or ("memoryerror" in e)


def _op_key(op) -> str:
    # Try a few common attributes; fall back to object id (stable only within a run).
    for attr in ("op_id", "operator_id", "node_id", "task_id", "name"):
        if hasattr(op, attr):
            v = getattr(op, attr)
            # Avoid including huge structures; keep key short and stable.
            if isinstance(v, (int, str)):
                return f"{attr}:{v}"
    return f"obj:{id(op)}"


def _safe_len(x, default=0):
    try:
        return len(x)
    except Exception:
        return default


def _remaining_ops_estimate(pipeline, status) -> int:
    # Prefer total_ops - completed when we can; otherwise approximate with non-completed counts.
    total = _safe_len(getattr(pipeline, "values", None), default=0)
    completed = 0
    try:
        completed = status.state_counts.get(OperatorState.COMPLETED, 0)
    except Exception:
        completed = 0

    if total > 0:
        rem = total - completed
        return rem if rem >= 0 else 0

    # Fallback: count "not done yet" states
    rem = 0
    try:
        for st in (OperatorState.PENDING, OperatorState.FAILED, OperatorState.ASSIGNED, OperatorState.RUNNING, OperatorState.SUSPENDING):
            rem += status.state_counts.get(st, 0)
    except Exception:
        pass
    return rem


def _baseline_ram(pool, priority):
    # Conservative starting points to reduce OOM churn without killing concurrency.
    # Scales with pool size; never exceeds pool max; never below a tiny epsilon.
    max_ram = float(pool.max_ram_pool)
    if priority == Priority.QUERY:
        frac = 0.10
    elif priority == Priority.INTERACTIVE:
        frac = 0.08
    else:
        frac = 0.06

    # Ensure a small absolute floor relative to pool size; avoid hard-coding GB units.
    baseline = max(max_ram * frac, max_ram * 0.02, 0.1)
    return min(max_ram, baseline)


def _baseline_cpu(pool, priority):
    # Fixed small quanta encourage concurrency; interactive/query latency is mostly queueing-driven.
    max_cpu = float(pool.max_cpu_pool)
    if priority == Priority.QUERY:
        base = 4.0
    elif priority == Priority.INTERACTIVE:
        base = 2.0
    else:
        base = 1.0
    return max(1.0, min(base, max_cpu))


def _headroom(pool_id, pool, query_waiting, hp_waiting, hp_idle_ticks):
    """Compute reserved headroom for future high-priority arrivals (non-preemptive protection)."""
    max_cpu = float(pool.max_cpu_pool)
    max_ram = float(pool.max_ram_pool)

    # If we have multiple pools, pool 0 is treated as the "primary" high-priority pool.
    primary_hp_pool = (pool_id == 0)

    # When high-priority has been idle for a while, reduce headroom to avoid batch incompletion.
    if hp_idle_ticks >= 5 and not hp_waiting:
        cpu_frac = 0.05
        ram_frac = 0.05
    else:
        cpu_frac = 0.20 if primary_hp_pool else 0.10
        ram_frac = 0.20 if primary_hp_pool else 0.10

    # Ensure headroom can accommodate at least a baseline QUERY container when queries may arrive.
    # (We reserve more when queries are present/likely to avoid being blocked by long batch ops.)
    min_cpu = 1.0
    min_ram = max(max_ram * 0.02, 0.1)
    if query_waiting or primary_hp_pool:
        min_cpu = min(max_cpu, max(2.0, _baseline_cpu(pool, Priority.QUERY)))
        min_ram = min(max_ram, max(_baseline_ram(pool, Priority.QUERY), max_ram * 0.05, 0.1))

    cpu_hr = min(max_cpu, max(min_cpu, max_cpu * cpu_frac))
    ram_hr = min(max_ram, max(min_ram, max_ram * ram_frac))
    return cpu_hr, ram_hr


def _queue_has_runnable(queue, priority) -> bool:
    # Quick scan for any pipeline with at least one assignable op.
    scan = min(10, len(queue))
    for i in range(scan):
        p = queue[i]
        st = p.runtime_status()
        if st.is_pipeline_successful():
            continue
        ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        if ops:
            # Light cap on per-pipeline parallelism
            running = st.state_counts.get(OperatorState.RUNNING, 0) + st.state_counts.get(OperatorState.ASSIGNED, 0)
            limit = 2 if priority in (Priority.QUERY, Priority.INTERACTIVE) else 1
            if running < limit:
                return True
    return False


def _cleanup_completed(queue):
    # Keep ordering but remove completed pipelines.
    out = []
    for p in queue:
        try:
            if not p.runtime_status().is_pipeline_successful():
                out.append(p)
        except Exception:
            out.append(p)
    queue[:] = out


def _pick_candidate(s, queue, priority, pool, avail_cpu, avail_ram):
    """Pick (pipeline, op, cpu, ram) or (None, None, None, None)."""

    # For high priority: SRPT-like (fewest remaining ops) among a small window.
    # For batch: FIFO within a small window.
    scan_limit = min(20, len(queue))
    best = None  # (score, idx, pipeline, op, cpu, ram)

    for idx in range(scan_limit):
        p = queue[idx]
        st = p.runtime_status()
        if st.is_pipeline_successful():
            continue

        # Limit per-pipeline concurrency to reduce thrash and keep end-to-end predictable.
        running = st.state_counts.get(OperatorState.RUNNING, 0) + st.state_counts.get(OperatorState.ASSIGNED, 0)
        limit = 2 if priority in (Priority.QUERY, Priority.INTERACTIVE) else 1
        if running >= limit:
            continue

        ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        if not ops:
            continue
        op = ops[0]

        # Decide RAM request using learned OOM floors when available.
        key = _op_key(op)
        strict_floor = key in s.op_ram_floor
        ram_req = float(s.op_ram_floor.get(key, _baseline_ram(pool, priority)))
        cpu_req = float(_baseline_cpu(pool, priority))

        # Must fit in currently available resources.
        if avail_cpu < 1.0:
            continue
        cpu = min(cpu_req, float(avail_cpu))
        cpu = max(1.0, cpu)

        if strict_floor:
            # If we learned a floor from OOMs, don't schedule unless we can meet it.
            if avail_ram < ram_req:
                continue
            ram = ram_req
        else:
            # Otherwise, allow smaller-than-baseline RAM if the pool is tight.
            if avail_ram < 0.1:
                continue
            ram = min(ram_req, float(avail_ram))
            ram = max(0.1, ram)

        # Scoring: smaller remaining ops first for QUERY/INTERACTIVE; FIFO for BATCH.
        if priority in (Priority.QUERY, Priority.INTERACTIVE):
            rem = _remaining_ops_estimate(p, st)
            score = rem
        else:
            score = idx  # FIFO-ish

        cand = (score, idx, p, op, cpu, ram)
        if best is None or cand[0] < best[0]:
            best = cand

    if best is None:
        return None, None, None, None

    _, idx, p, op, cpu, ram = best

    # Round-robin within the queue: move scheduled pipeline to the back to avoid monopolization.
    queue.append(queue.pop(idx))
    return p, op, cpu, ram


@register_scheduler(key="scheduler_high_044")
def scheduler_high_044_scheduler(s, results, pipelines):
    """
    Scheduler step:
      - Enqueue arrivals by priority
      - Update RAM floors on OOM failures
      - For each pool: schedule as many ops as possible, honoring priority ordering and headroom
    """
    s.tick_count += 1

    # Enqueue new arrivals
    for p in pipelines:
        if p.priority == Priority.QUERY:
            s.q_query.append(p)
        elif p.priority == Priority.INTERACTIVE:
            s.q_interactive.append(p)
        else:
            s.q_batch.append(p)

    # Early exit if nothing changed
    if not pipelines and not results:
        return [], []

    # Update learned RAM floors based on OOM failures
    for r in results:
        try:
            if r.failed() and _is_oom_error(getattr(r, "error", None)):
                pool = s.executor.pools[r.pool_id]
                for op in getattr(r, "ops", []) or []:
                    key = _op_key(op)
                    baseline = _baseline_ram(pool, r.priority)
                    prev = float(s.op_ram_floor.get(key, baseline))
                    # Aggressive ramp to avoid repeated OOM loops; cap at pool max.
                    new_floor = max(prev, float(r.ram) * 1.8, baseline * 2.0)
                    new_floor = min(float(pool.max_ram_pool), new_floor)
                    s.op_ram_floor[key] = new_floor
        except Exception:
            # If the result object doesn't match assumptions, ignore and keep scheduling.
            pass

    # Remove completed pipelines from queues (avoid endless scanning)
    _cleanup_completed(s.q_query)
    _cleanup_completed(s.q_interactive)
    _cleanup_completed(s.q_batch)

    query_waiting = _queue_has_runnable(s.q_query, Priority.QUERY)
    interactive_waiting = _queue_has_runnable(s.q_interactive, Priority.INTERACTIVE)
    hp_waiting = query_waiting or interactive_waiting

    if hp_waiting:
        s.hp_idle_ticks = 0
    else:
        s.hp_idle_ticks += 1

    suspensions = []
    assignments = []

    # Schedule per pool
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)

        if avail_cpu < 1.0 or avail_ram < 0.1:
            continue

        # Headroom that BATCH must not consume (and INTERACTIVE must respect if queries waiting)
        hr_cpu, hr_ram = _headroom(pool_id, pool, query_waiting, hp_waiting, s.hp_idle_ticks)

        while avail_cpu >= 1.0 and avail_ram >= 0.1:
            made = False

            # 1) QUERY first (dominant weight)
            p, op, cpu, ram = _pick_candidate(s, s.q_query, Priority.QUERY, pool, avail_cpu, avail_ram)
            if p is not None:
                assignments.append(
                    Assignment(
                        ops=[op],
                        cpu=cpu,
                        ram=ram,
                        priority=p.priority,
                        pool_id=pool_id,
                        pipeline_id=p.pipeline_id,
                    )
                )
                avail_cpu -= cpu
                avail_ram -= ram
                made = True
                continue

            # 2) INTERACTIVE next; if queries are waiting somewhere, preserve headroom
            p, op, cpu, ram = _pick_candidate(s, s.q_interactive, Priority.INTERACTIVE, pool, avail_cpu, avail_ram)
            if p is not None:
                if query_waiting:
                    # Don't let interactive block an imminent/query backlog given no preemption.
                    if (avail_cpu - cpu) < hr_cpu or (avail_ram - ram) < hr_ram:
                        p = None  # treat as not schedulable here
                    else:
                        assignments.append(
                            Assignment(
                                ops=[op],
                                cpu=cpu,
                                ram=ram,
                                priority=p.priority,
                                pool_id=pool_id,
                                pipeline_id=p.pipeline_id,
                            )
                        )
                        avail_cpu -= cpu
                        avail_ram -= ram
                        made = True
                        continue
                else:
                    assignments.append(
                        Assignment(
                            ops=[op],
                            cpu=cpu,
                            ram=ram,
                            priority=p.priority,
                            pool_id=pool_id,
                            pipeline_id=p.pipeline_id,
                        )
                    )
                    avail_cpu -= cpu
                    avail_ram -= ram
                    made = True
                    continue

            # 3) BATCH last; preserve headroom when high priority exists or may arrive
            p, op, cpu, ram = _pick_candidate(s, s.q_batch, Priority.BATCH, pool, avail_cpu, avail_ram)
            if p is not None:
                # If high priority is waiting, be stricter about leaving headroom.
                # If not waiting but recently idle, allow more batch (headroom shrinks via hp_idle_ticks).
                if (avail_cpu - cpu) < hr_cpu or (avail_ram - ram) < hr_ram:
                    # Not enough safe headroom to start a long-running batch container in this pool.
                    p = None
                else:
                    assignments.append(
                        Assignment(
                            ops=[op],
                            cpu=cpu,
                            ram=ram,
                            priority=p.priority,
                            pool_id=pool_id,
                            pipeline_id=p.pipeline_id,
                        )
                    )
                    avail_cpu -= cpu
                    avail_ram -= ram
                    made = True
                    continue

            if not made:
                break

    return suspensions, assignments
