# policy_key: scheduler_est_025
# reasoning_effort: high
# model: gpt-5.2-2025-12-11
# llm_cost: 0.077454
# generation_seconds: 121.66
# generated_at: 2026-03-31T19:08:13.614699
@register_scheduler_init(key="scheduler_est_025")
def scheduler_est_025_init(s):
    """
    Priority-aware, estimator-driven scheduler (incremental improvement over naive FIFO).

    Key improvements over the example FIFO:
      1) Priority queues: always try QUERY, then INTERACTIVE, then BATCH.
      2) Right-size RAM per op using op.estimate.mem_peak_gb with a safety multiplier.
      3) Avoid "give everything to one op": per-op CPU caps by priority to improve concurrency.
      4) OOM-aware retries: if an op fails with OOM, retry with increased RAM (bounded retries).
      5) Soft reservation: when high-priority work is waiting, keep some pool headroom by
         throttling BATCH allocations so new high-priority ops can start quickly.
    """
    s.queues = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }
    s.known_pipeline_ids = set()

    # Per-operator adaptive memory multiplier driven by OOM feedback.
    s.op_mem_mult = {}          # op_key -> float
    s.op_oom_retries = {}       # op_key -> int
    s.op_to_pipeline_id = {}    # op_key -> pipeline_id

    # Terminally failed pipelines (non-OOM failures, or too many OOM retries).
    s.terminal_failed_pipelines = set()

    # Limit retries to avoid infinite loops.
    s.max_oom_retries = 3

    # Small fairness knob: allow some batch even under constant interactive load.
    s.batch_tokens = 0
    s.max_batch_tokens = 2


def _op_key(op):
    """Best-effort stable key for an operator across scheduler calls."""
    for attr in ("operator_id", "op_id", "id", "name"):
        v = getattr(op, attr, None)
        if v is not None:
            return (attr, v)
    # Fallback: Python identity (works if simulator uses persistent operator objects)
    return ("py_id", id(op))


def _get_mem_estimate_gb(op):
    """Fetch op.estimate.mem_peak_gb if present; otherwise return None."""
    est = getattr(op, "estimate", None)
    if est is None:
        return None
    v = getattr(est, "mem_peak_gb", None)
    try:
        if v is None:
            return None
        v = float(v)
        if v <= 0:
            return None
        return v
    except Exception:
        return None


def _is_oom_error(err):
    if not err:
        return False
    e = str(err).lower()
    return ("oom" in e) or ("out of memory" in e) or ("memory" in e and "killed" in e)


def _has_any_runnable(s, priorities):
    """Check whether there exists any runnable op among the given priority queues."""
    for pr in priorities:
        for p in s.queues.get(pr, []):
            if p.pipeline_id in s.terminal_failed_pipelines:
                continue
            st = p.runtime_status()
            if st.is_pipeline_successful():
                continue
            ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
            if ops:
                return True
    return False


def _cleanup_queues(s):
    """Remove completed and terminal-failed pipelines from queues."""
    for pr in list(s.queues.keys()):
        new_q = []
        for p in s.queues[pr]:
            if p.pipeline_id in s.terminal_failed_pipelines:
                s.known_pipeline_ids.discard(p.pipeline_id)
                continue
            st = p.runtime_status()
            if st.is_pipeline_successful():
                s.known_pipeline_ids.discard(p.pipeline_id)
                continue
            new_q.append(p)
        s.queues[pr] = new_q


def _cpu_target(pool, priority):
    """
    Per-op CPU cap to avoid monopolization while still favoring latency-sensitive work.
    The simulator accepts floats; keep a minimum of 1 vCPU.
    """
    max_cpu = float(pool.max_cpu_pool)
    if priority == Priority.QUERY:
        return max(1.0, min(max_cpu * 0.75, 16.0))
    if priority == Priority.INTERACTIVE:
        return max(1.0, min(max_cpu * 0.50, 12.0))
    # Batch: smaller slices for better overall concurrency + reserved headroom.
    return max(1.0, min(max_cpu * 0.25, 8.0))


def _ram_required_gb(s, op, priority):
    """
    RAM sizing using mem_peak_gb estimate * (priority-specific base) * (oom multiplier).
    """
    base_est = _get_mem_estimate_gb(op)
    mult = s.op_mem_mult.get(_op_key(op), 1.0)

    # Priority-specific safety margin (small and conservative to start).
    if priority == Priority.QUERY:
        safety = 1.20
    elif priority == Priority.INTERACTIVE:
        safety = 1.15
    else:
        safety = 1.10

    if base_est is None:
        # If no estimate exists, pick a small-ish default; OOM feedback will correct upward.
        # Keep it non-trivial to reduce immediate OOMs.
        base_est = 2.0

    # Add a small constant cushion to reduce "barely OOM" churn on estimates.
    req = base_est * safety * mult + 0.25
    # Hard floor.
    return max(0.5, float(req))


def _pop_next_runnable_pipeline_rr(queue, require_parents_complete=True):
    """
    Round-robin scan:
      - rotates the queue
      - returns (pipeline, op) if a runnable op exists
      - otherwise returns (None, None)
    """
    n = len(queue)
    for _ in range(n):
        p = queue.pop(0)
        queue.append(p)
        st = p.runtime_status()
        if st.is_pipeline_successful():
            continue
        ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=require_parents_complete)
        if ops:
            return p, ops[0]
    return None, None


@register_scheduler(key="scheduler_est_025")
def scheduler_est_025(s, results, pipelines):
    """
    Deterministic scheduling step:
      - ingest new pipelines into per-priority RR queues
      - update OOM multipliers from results
      - assign runnable ops to pools with:
          * priority ordering
          * CPU caps per op
          * estimator-based RAM sizing
          * soft batch throttling when high-priority waiting
    """
    # --- Ingest new pipelines ---
    for p in pipelines:
        if p.pipeline_id in s.known_pipeline_ids:
            continue
        s.known_pipeline_ids.add(p.pipeline_id)
        s.queues[p.priority].append(p)

    # --- Update feedback from previous executions (OOM-aware retries) ---
    for r in results:
        ops = getattr(r, "ops", None) or []
        for op in ops:
            k = _op_key(op)

            # Best-effort back-map op -> pipeline_id for terminal failure marking.
            pid = getattr(r, "pipeline_id", None)
            if pid is None:
                pid = s.op_to_pipeline_id.get(k, None)
            if pid is not None:
                s.op_to_pipeline_id[k] = pid

            if r.failed():
                if _is_oom_error(getattr(r, "error", None)):
                    # Increase multiplier and retry count.
                    s.op_oom_retries[k] = s.op_oom_retries.get(k, 0) + 1
                    prev = s.op_mem_mult.get(k, 1.0)
                    # Multiplicative bump (moderate) with a reasonable cap.
                    s.op_mem_mult[k] = min(prev * 1.6, 8.0)

                    # Too many retries => mark pipeline terminally failed (avoid infinite loops).
                    if s.op_oom_retries[k] > s.max_oom_retries and pid is not None:
                        s.terminal_failed_pipelines.add(pid)
                else:
                    # Non-OOM failures are treated as terminal.
                    if pid is not None:
                        s.terminal_failed_pipelines.add(pid)
            else:
                # Successful completion: gently decay multiplier toward 1.0 (reduces persistent over-allocation).
                if k in s.op_mem_mult:
                    s.op_mem_mult[k] = max(1.0, s.op_mem_mult[k] * 0.9)

    # Clean out completed / terminal pipelines.
    _cleanup_queues(s)

    # If nothing arrived and nothing finished/failed, decisions won't change.
    if not pipelines and not results:
        # Still increment tokens slowly to ensure batch can progress when scheduler is invoked periodically.
        s.batch_tokens = min(s.max_batch_tokens, s.batch_tokens + 1)
        return [], []

    # Soft batch fairness tokens.
    s.batch_tokens = min(s.max_batch_tokens, s.batch_tokens + 1)

    # Detect whether high priority work is waiting (used for soft reservation).
    high_waiting = _has_any_runnable(s, [Priority.QUERY, Priority.INTERACTIVE])

    suspensions = []
    assignments = []

    # --- Main scheduling loop over pools ---
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]

        local_avail_cpu = float(pool.avail_cpu_pool)
        local_avail_ram = float(pool.avail_ram_pool)

        if local_avail_cpu <= 0.0 or local_avail_ram <= 0.0:
            continue

        # Soft reservation: if high priority is waiting, keep some headroom by throttling BATCH.
        # This is a "small, obvious" latency improvement without requiring preemption APIs.
        reserved_cpu = float(pool.max_cpu_pool) * (0.25 if high_waiting else 0.0)
        reserved_ram = float(pool.max_ram_pool) * (0.25 if high_waiting else 0.0)

        # Try to place multiple ops per pool per scheduler call.
        # Bound iterations to avoid long scans on large queues.
        for _ in range(32):
            if local_avail_cpu < 1.0 or local_avail_ram <= 0.0:
                break

            chosen_prio = None
            # Strict priority for latency.
            if _has_any_runnable(s, [Priority.QUERY]):
                chosen_prio = Priority.QUERY
            elif _has_any_runnable(s, [Priority.INTERACTIVE]):
                chosen_prio = Priority.INTERACTIVE
            else:
                # Batch only if we have tokens (fairness) and not starving high-priority.
                if s.batch_tokens <= 0:
                    break
                chosen_prio = Priority.BATCH_PIPELINE

            # Enforce reservation by limiting what BATCH can consume from currently-free resources.
            if chosen_prio == Priority.BATCH_PIPELINE and high_waiting:
                allowed_cpu = max(0.0, local_avail_cpu - reserved_cpu)
                allowed_ram = max(0.0, local_avail_ram - reserved_ram)
            else:
                allowed_cpu = local_avail_cpu
                allowed_ram = local_avail_ram

            if allowed_cpu < 1.0 or allowed_ram <= 0.0:
                # Can't schedule this priority on this pool right now.
                if chosen_prio == Priority.BATCH_PIPELINE:
                    break
                # If this was high priority but we can't fit, no point continuing here.
                break

            q = s.queues.get(chosen_prio, [])
            if not q:
                if chosen_prio == Priority.BATCH_PIPELINE:
                    break
                continue

            # Find a runnable pipeline/op round-robin.
            p, op = _pop_next_runnable_pipeline_rr(q, require_parents_complete=True)
            if p is None or op is None:
                # Nothing runnable at this priority (despite earlier check); try again.
                if chosen_prio == Priority.BATCH_PIPELINE:
                    break
                continue

            # Skip terminal-failed pipelines if they are still lingering in the queue.
            if p.pipeline_id in s.terminal_failed_pipelines:
                continue

            # Determine resource request for this op.
            req_ram = _ram_required_gb(s, op, p.priority)
            cpu_cap = _cpu_target(pool, p.priority)

            # CPU for this assignment.
            # For latency-sensitive work, try to allocate at least 2 vCPU if possible.
            if p.priority in (Priority.QUERY, Priority.INTERACTIVE):
                min_cpu = 2.0
            else:
                min_cpu = 1.0

            cpu = min(allowed_cpu, cpu_cap)
            if cpu < min_cpu:
                # If we can't hit a minimum for high prio, try with what we have only if >=1.
                if allowed_cpu >= 1.0:
                    cpu = min(allowed_cpu, cpu_cap)
                else:
                    break

            # RAM must meet required RAM; otherwise, don't schedule this op yet.
            if req_ram > allowed_ram:
                # If the estimate is too large for the remaining headroom, try another op in the same queue
                # that might fit (bounded attempts). This avoids head-of-line blocking.
                found = False
                scan_n = min(len(q), 8)
                for _scan in range(scan_n):
                    p2, op2 = _pop_next_runnable_pipeline_rr(q, require_parents_complete=True)
                    if p2 is None or op2 is None:
                        break
                    if p2.pipeline_id in s.terminal_failed_pipelines:
                        continue
                    req_ram2 = _ram_required_gb(s, op2, p2.priority)
                    if req_ram2 <= allowed_ram:
                        p, op, req_ram = p2, op2, req_ram2
                        found = True
                        break
                if not found:
                    # Can't fit any op from this priority on this pool right now.
                    if chosen_prio == Priority.BATCH_PIPELINE:
                        break
                    # For high priority, if we can't fit, stop trying this pool.
                    break

            ram = min(allowed_ram, req_ram)

            # Create assignment (one op per container).
            assignment = Assignment(
                ops=[op],
                cpu=cpu,
                ram=ram,
                priority=p.priority,
                pool_id=pool_id,
                pipeline_id=p.pipeline_id,
            )
            assignments.append(assignment)

            # Remember mapping for later failure attribution.
            s.op_to_pipeline_id[_op_key(op)] = p.pipeline_id

            # Update local free resources for additional placements in the same pool this tick.
            local_avail_cpu -= cpu
            local_avail_ram -= ram

            # Consume a batch token if we just scheduled batch.
            if chosen_prio == Priority.BATCH_PIPELINE:
                s.batch_tokens = max(0, s.batch_tokens - 1)

    return suspensions, assignments
