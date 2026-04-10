# policy_key: scheduler_est_031
# reasoning_effort: high
# model: gpt-5.2-2025-12-11
# llm_cost: 0.099588
# generation_seconds: 154.75
# generated_at: 2026-03-31T19:20:20.483525
@register_scheduler_init(key="scheduler_est_031")
def scheduler_est_031_init(s):
    """Priority-aware, estimate-driven scheduler (incremental improvement over naive FIFO).

    Key ideas (kept intentionally simple):
      1) Priority queues: QUERY > INTERACTIVE > BATCH_PIPELINE.
      2) Use op.estimate.mem_peak_gb for RAM sizing with a small safety buffer.
      3) On suspected OOM, retry the same op with a larger RAM multiplier (bounded).
      4) Prevent batch from consuming the entire pool when high-priority work is waiting
         via a small per-pool CPU/RAM reservation.
      5) Avoid excessive per-pipeline parallelism by limiting in-flight ops per pipeline.
    """
    s.q_query = []
    s.q_interactive = []
    s.q_batch = []
    s._enqueued_pipeline_ids = set()

    # OOM-aware RAM sizing state (tracked per operator object identity).
    s._op_ram_mult = {}          # op_key -> multiplier
    s._op_oom_retries = {}       # op_key -> count

    # Terminal failures (non-OOM) or repeated OOM beyond max retries.
    s._terminal_failed_pipelines = set()

    # Fairness knobs: after N high-priority picks, allow one batch pick if batch is waiting.
    s._hp_streak = 0
    s._hp_streak_limit = 8

    # Reservations (only apply when higher-priority queues are non-empty).
    s._reserve_frac_for_hp_over_batch = 0.20
    s._reserve_frac_for_query_over_interactive = 0.10

    # Resource sizing knobs.
    s._min_cpu = 1.0
    s._cpu_cap = {
        Priority.QUERY: 4.0,
        Priority.INTERACTIVE: 2.0,
        Priority.BATCH_PIPELINE: 1.0,
    }

    # In-flight limits per pipeline (reduces contention / improves tail latency stability).
    s._max_inflight = {
        Priority.QUERY: 2,
        Priority.INTERACTIVE: 2,
        Priority.BATCH_PIPELINE: 4,
    }

    # OOM retry policy.
    s._max_oom_retries = 3
    s._max_ram_mult = 8.0

    # Scheduler loop caps (avoid spinning when nothing fits).
    s._max_attempts_per_pool = 64


def _prio_rank(priority):
    # Smaller rank means higher priority.
    if priority == Priority.QUERY:
        return 0
    if priority == Priority.INTERACTIVE:
        return 1
    if priority == Priority.BATCH_PIPELINE:
        return 2
    return 3


def _is_hp(priority):
    return priority in (Priority.QUERY, Priority.INTERACTIVE)


def _hp_waiting(s):
    return bool(s.q_query) or bool(s.q_interactive)


def _enqueue_pipeline(s, p):
    pid = p.pipeline_id
    if pid in s._enqueued_pipeline_ids:
        return
    if pid in s._terminal_failed_pipelines:
        return
    st = p.runtime_status()
    if st.is_pipeline_successful():
        return

    if p.priority == Priority.QUERY:
        s.q_query.append(p)
    elif p.priority == Priority.INTERACTIVE:
        s.q_interactive.append(p)
    else:
        s.q_batch.append(p)

    s._enqueued_pipeline_ids.add(pid)


def _dequeue_next_pipeline(s):
    # Fairness: if high-priority is continuously present, occasionally admit one batch.
    if (s.q_query or s.q_interactive) and s.q_batch and s._hp_streak >= s._hp_streak_limit:
        s._hp_streak = 0
        p = s.q_batch.pop(0)
        s._enqueued_pipeline_ids.discard(p.pipeline_id)
        return p

    if s.q_query:
        s._hp_streak += 1
        p = s.q_query.pop(0)
        s._enqueued_pipeline_ids.discard(p.pipeline_id)
        return p

    if s.q_interactive:
        s._hp_streak += 1
        p = s.q_interactive.pop(0)
        s._enqueued_pipeline_ids.discard(p.pipeline_id)
        return p

    s._hp_streak = 0
    if s.q_batch:
        p = s.q_batch.pop(0)
        s._enqueued_pipeline_ids.discard(p.pipeline_id)
        return p

    return None


def _requeue_pipeline(s, p):
    # Re-enqueue only if still active.
    pid = p.pipeline_id
    if pid in s._terminal_failed_pipelines:
        return
    st = p.runtime_status()
    if st.is_pipeline_successful():
        return
    _enqueue_pipeline(s, p)


def _op_key(op):
    # Prefer stable IDs if present; fall back to object identity.
    return getattr(op, "op_id", None) or getattr(op, "operator_id", None) or id(op)


def _mem_est_gb(op):
    est = getattr(op, "estimate", None)
    mem = getattr(est, "mem_peak_gb", None) if est is not None else None
    if mem is None:
        mem = getattr(op, "mem_peak_gb", None)
    try:
        mem = float(mem)
    except Exception:
        mem = 1.0
    # Guardrails: avoid 0 or negative estimates.
    return max(0.1, mem)


def _desired_ram_gb(s, op, pool):
    key = _op_key(op)
    mult = s._op_ram_mult.get(key, 1.0)

    base = _mem_est_gb(op)
    # Small safety margin + fixed cushion to reduce near-miss OOMs.
    req = base * mult * 1.10 + 0.25

    # If it can never fit in the pool, signal "unschedulable here".
    if req > pool.max_ram_pool + 1e-9:
        return None
    return req


def _cpu_target(s, priority, pool):
    cap = s._cpu_cap.get(priority, 1.0)
    # Don't ask for more than the pool can ever provide.
    return max(s._min_cpu, min(float(pool.max_cpu_pool), float(cap)))


def _infer_oom(error_str):
    if not error_str:
        return False
    e = str(error_str).lower()
    # Keep heuristics simple/robust.
    return ("oom" in e) or ("out of memory" in e) or ("memory" in e and "kill" in e) or ("killed process" in e)


@register_scheduler(key="scheduler_est_031")
def scheduler_est_031(s, results, pipelines):
    """
    Priority-aware scheduler using memory estimates and OOM-based RAM backoff.
    """
    suspensions = []
    assignments = []

    # 1) Ingest new pipelines.
    for p in pipelines:
        _enqueue_pipeline(s, p)

    # 2) Learn from results (OOM backoff; terminal failures).
    for r in results:
        # If ExecutionResult exposes pipeline_id, prefer it.
        r_pid = getattr(r, "pipeline_id", None)

        if r.failed():
            oom = _infer_oom(getattr(r, "error", None))

            # Update per-op RAM multiplier on suspected OOM.
            if oom:
                for op in getattr(r, "ops", []) or []:
                    k = _op_key(op)
                    s._op_oom_retries[k] = s._op_oom_retries.get(k, 0) + 1
                    prev = s._op_ram_mult.get(k, 1.0)
                    s._op_ram_mult[k] = min(s._max_ram_mult, max(prev, 1.0) * 1.5)

                    # Too many OOMs: fail the pipeline to avoid infinite churn.
                    if s._op_oom_retries[k] > s._max_oom_retries and r_pid is not None:
                        s._terminal_failed_pipelines.add(r_pid)
            else:
                # Non-OOM failures: mark pipeline as terminal if we can identify it.
                if r_pid is not None:
                    s._terminal_failed_pipelines.add(r_pid)
        else:
            # On success, gently decay the RAM multiplier (prevents permanent over-allocation).
            for op in getattr(r, "ops", []) or []:
                k = _op_key(op)
                if k in s._op_ram_mult:
                    s._op_ram_mult[k] = max(1.0, s._op_ram_mult[k] * 0.95)

    # If there's truly nothing to do, exit quickly.
    if not s.q_query and not s.q_interactive and not s.q_batch:
        return suspensions, assignments

    # 3) Schedule onto pools. Fill each pool with multiple assignments (not just one).
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)

        if avail_cpu < s._min_cpu or avail_ram <= 0:
            continue

        attempts = 0
        stalled = 0

        while attempts < s._max_attempts_per_pool:
            attempts += 1

            if avail_cpu < s._min_cpu or avail_ram <= 0:
                break

            p = _dequeue_next_pipeline(s)
            if p is None:
                break

            pid = p.pipeline_id
            if pid in s._terminal_failed_pipelines:
                continue

            status = p.runtime_status()
            if status.is_pipeline_successful():
                continue

            # Limit per-pipeline in-flight ops to reduce interference and tail latency.
            inflight = 0
            try:
                inflight = (
                    status.state_counts.get(OperatorState.RUNNING, 0)
                    + status.state_counts.get(OperatorState.ASSIGNED, 0)
                    + status.state_counts.get(OperatorState.SUSPENDING, 0)
                )
            except Exception:
                inflight = 0

            if inflight >= s._max_inflight.get(p.priority, 2):
                _requeue_pipeline(s, p)
                stalled += 1
                if stalled >= 8:
                    break
                continue

            # Pick an assignable op; choose smallest estimated memory first to reduce HOL blocking.
            ready_ops = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True) or []
            if not ready_ops:
                _requeue_pipeline(s, p)
                stalled += 1
                if stalled >= 8:
                    break
                continue

            try:
                ready_ops = sorted(ready_ops, key=_mem_est_gb)
            except Exception:
                pass
            op = ready_ops[0]

            # If this op has exceeded OOM retries, fail the pipeline (avoid endless retries).
            ok = _op_key(op)
            if s._op_oom_retries.get(ok, 0) > s._max_oom_retries:
                s._terminal_failed_pipelines.add(pid)
                continue

            req_ram = _desired_ram_gb(s, op, pool)
            if req_ram is None:
                # Cannot ever fit in this pool; keep it queued so another pool may run it.
                _requeue_pipeline(s, p)
                stalled += 1
                if stalled >= 16:
                    break
                continue

            # Apply reservations based on queue state (only constrain lower priorities).
            eff_cpu = avail_cpu
            eff_ram = avail_ram

            if p.priority == Priority.BATCH_PIPELINE and _hp_waiting(s):
                eff_cpu = max(0.0, avail_cpu - float(pool.max_cpu_pool) * s._reserve_frac_for_hp_over_batch)
                eff_ram = max(0.0, avail_ram - float(pool.max_ram_pool) * s._reserve_frac_for_hp_over_batch)
            elif p.priority == Priority.INTERACTIVE and bool(s.q_query):
                eff_cpu = max(0.0, avail_cpu - float(pool.max_cpu_pool) * s._reserve_frac_for_query_over_interactive)
                eff_ram = max(0.0, avail_ram - float(pool.max_ram_pool) * s._reserve_frac_for_query_over_interactive)

            if eff_cpu < s._min_cpu or eff_ram <= 0:
                _requeue_pipeline(s, p)
                stalled += 1
                if stalled >= 8:
                    break
                continue

            # CPU allocation: aim for a small cap; if constrained, take what's left (>= 1).
            cpu_tgt = _cpu_target(s, p.priority, pool)
            cpu_alloc = min(cpu_tgt, eff_cpu)
            if cpu_alloc < s._min_cpu:
                _requeue_pipeline(s, p)
                stalled += 1
                if stalled >= 8:
                    break
                continue

            # RAM allocation must meet requirement (don't under-allocate; it will just OOM).
            if req_ram > eff_ram + 1e-9:
                _requeue_pipeline(s, p)
                stalled += 1
                if stalled >= 16:
                    break
                continue

            assignments.append(
                Assignment(
                    ops=[op],
                    cpu=cpu_alloc,
                    ram=req_ram,
                    priority=p.priority,
                    pool_id=pool_id,
                    pipeline_id=pid,
                )
            )

            # Assume resources are reserved immediately for additional packing in this tick.
            avail_cpu -= cpu_alloc
            avail_ram -= req_ram

            stalled = 0
            _requeue_pipeline(s, p)

    return suspensions, assignments
