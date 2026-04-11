# policy_key: scheduler_high_001
# reasoning_effort: high
# exp: reasoning
# model: gpt-5.2-2025-12-11
# llm_cost: 0.000000
# generation_seconds: 153.04
# generated_at: 2026-04-10T00:15:22.398959
@register_scheduler_init(key="scheduler_high_001")
def scheduler_high_001_init(s):
    """
    Priority-first, OOM-avoiding, low-HOL scheduling.

    Core ideas:
    - Strictly protect QUERY/INTERACTIVE latency via (a) priority ordering and (b) reserving headroom from BATCH when high-priority work is waiting.
    - Improve completion rate by retrying FAILED operators with exponential RAM backoff on failures (especially OOM-like errors).
    - Avoid head-of-line blocking by not allocating an entire pool to a single operator; use moderate per-container CPU caps.
    - Prevent indefinite batch starvation via a small "batch drip" when high-priority work is continuously present.
    """
    from collections import deque

    s.tick = 0

    # Active pipelines by id (authoritative reference to objects).
    s.active = {}

    # Round-robin queues by priority (contain pipeline_ids; stale ids are tolerated and skipped).
    s.q_query = deque()
    s.q_interactive = deque()
    s.q_batch = deque()

    # Learned RAM multipliers per (pipeline_id, op_uid). Start at 1.0 and backoff on failures.
    s.op_ram_mult = {}

    # Pipeline last-scheduled tick (for simple aging / starvation checks).
    s.last_scheduled_tick = {}

    # Batch starvation guard: allow at least one batch assignment every N ticks when batch is waiting.
    s.last_batch_served_tick = -10**9
    s.batch_drip_interval_ticks = 10  # small but non-zero progress even under constant high-priority load

    # Reserve some headroom for high-priority arrivals (only enforced against BATCH when hi-priority is waiting).
    s.reserve_cpu_frac = 0.15
    s.reserve_ram_frac = 0.15

    # Per-priority in-flight limit per pipeline to avoid one pipeline dominating.
    s.inflight_limit = {
        Priority.QUERY: 2,
        Priority.INTERACTIVE: 1,
        Priority.BATCH_PIPELINE: 1,
    }

    # Per-priority CPU sizing caps (keep concurrency; CPU scaling is sublinear).
    s.cpu_cap = {
        Priority.QUERY: 8,
        Priority.INTERACTIVE: 4,
        Priority.BATCH_PIPELINE: 2,
    }


def _get_queue_for_priority(s, priority):
    if priority == Priority.QUERY:
        return s.q_query
    if priority == Priority.INTERACTIVE:
        return s.q_interactive
    return s.q_batch


def _safe_state_count(status, state):
    try:
        return status.state_counts.get(state, 0)
    except Exception:
        # Be robust if state_counts is not a dict-like.
        try:
            return status.state_counts[state]
        except Exception:
            return 0


def _pipeline_is_done_or_gone(s, pipeline):
    if pipeline is None:
        return True
    try:
        st = pipeline.runtime_status()
        return st.is_pipeline_successful()
    except Exception:
        return False


def _op_uid(op):
    # Prefer stable identifiers if present; otherwise fall back to object id.
    for attr in ("op_id", "operator_id", "task_id", "node_id", "name"):
        v = getattr(op, attr, None)
        if v is not None:
            return str(v)
    return str(id(op))


def _result_pipeline_id(result):
    # Some simulators attach pipeline_id to results; use it if present.
    pid = getattr(result, "pipeline_id", None)
    if pid is not None:
        return pid
    # Otherwise, try to infer from the first op.
    try:
        if result.ops:
            op0 = result.ops[0]
            for attr in ("pipeline_id", "dag_id", "workflow_id"):
                v = getattr(op0, attr, None)
                if v is not None:
                    return v
    except Exception:
        pass
    return None


def _get_op_ram_min(op):
    # Try common attribute names for minimum required RAM.
    for attr in ("ram_min", "min_ram", "memory_min", "mem_min", "ram", "memory"):
        v = getattr(op, attr, None)
        if v is None:
            continue
        try:
            fv = float(v)
            if fv > 0:
                return fv
        except Exception:
            continue
    return None


def _priority_base_ram_frac(priority):
    if priority == Priority.QUERY:
        return 0.25
    if priority == Priority.INTERACTIVE:
        return 0.20
    return 0.15


def _desired_cpu(s, priority, pool_max_cpu, avail_cpu):
    # Moderate CPU sizing to keep concurrency; cap per priority.
    try:
        max_cpu = float(pool_max_cpu)
    except Exception:
        max_cpu = pool_max_cpu

    if priority == Priority.QUERY:
        base = max(1, int(round(max_cpu * 0.25)))
    elif priority == Priority.INTERACTIVE:
        base = max(1, int(round(max_cpu * 0.20)))
    else:
        base = max(1, int(round(max_cpu * 0.10)))

    cap = s.cpu_cap.get(priority, 2)
    desired = min(cap, base)

    # Fit to current availability.
    try:
        if avail_cpu < 1:
            return 0
        return min(desired, int(avail_cpu))
    except Exception:
        return min(desired, avail_cpu)


def _desired_ram(s, priority, op, pool_max_ram, avail_ram, ram_mult):
    op_min = _get_op_ram_min(op)

    if op_min is not None:
        # Allocate just above the minimum, with learned multiplier and small safety margin.
        desired = float(op_min) * float(ram_mult) * 1.10
    else:
        # Fallback: allocate a conservative fraction of pool RAM by priority.
        try:
            desired = float(pool_max_ram) * _priority_base_ram_frac(priority) * float(ram_mult)
        except Exception:
            desired = pool_max_ram * _priority_base_ram_frac(priority) * ram_mult

        # Ensure at least something non-trivial.
        try:
            desired = max(desired, float(pool_max_ram) * 0.05)
        except Exception:
            pass

    # Don't request more than the pool can ever provide.
    try:
        desired = min(desired, float(pool_max_ram))
    except Exception:
        desired = min(desired, pool_max_ram)

    # If it doesn't fit in currently available RAM, don't schedule (avoid guaranteed OOM / thrash).
    try:
        if float(avail_ram) < desired:
            return None
    except Exception:
        if avail_ram < desired:
            return None

    return desired


def _has_ready_high_priority_work(s):
    # Fast check: scan queues and see if any query/interactive has an assignable op ready.
    # Keep it bounded to avoid O(n) blowups; we only need a hint for headroom reservation.
    max_checks = 32

    def check_queue(q):
        checked = 0
        for pid in list(q)[:max_checks]:
            checked += 1
            p = s.active.get(pid, None)
            if p is None:
                continue
            try:
                st = p.runtime_status()
                if st.is_pipeline_successful():
                    continue
                ready = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
                if ready:
                    return True
            except Exception:
                continue
        return False

    return check_queue(s.q_query) or check_queue(s.q_interactive)


def _pick_pipeline_and_op(s, allow_batch):
    # Strict priority order, with optional batch gating.
    prios = [Priority.QUERY, Priority.INTERACTIVE]
    if allow_batch:
        prios.append(Priority.BATCH_PIPELINE)

    for pr in prios:
        q = _get_queue_for_priority(s, pr)
        qlen = len(q)
        for _ in range(qlen):
            pid = q.popleft()
            p = s.active.get(pid, None)
            if p is None:
                continue

            try:
                st = p.runtime_status()
                if st.is_pipeline_successful():
                    # Pipeline completed: remove from active.
                    s.active.pop(pid, None)
                    s.last_scheduled_tick.pop(pid, None)
                    continue

                # Limit in-flight operators per pipeline to reduce dominance and churn.
                inflight = (
                    _safe_state_count(st, OperatorState.ASSIGNED)
                    + _safe_state_count(st, OperatorState.RUNNING)
                    + _safe_state_count(st, OperatorState.SUSPENDING)
                )
                limit = s.inflight_limit.get(p.priority, 1)
                if inflight >= limit:
                    q.append(pid)
                    continue

                ready_ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
                if not ready_ops:
                    q.append(pid)
                    continue

                # Round-robin: keep pipeline in queue, but return it for scheduling now.
                q.append(pid)
                return p, ready_ops[0]
            except Exception:
                # On any introspection issues, keep it in queue and move on.
                q.append(pid)
                continue

    return None, None


@register_scheduler(key="scheduler_high_001")
def scheduler_high_001(s, results, pipelines):
    """
    Scheduler step:
    - Incorporate arrivals and execution results.
    - Retry failed operators with RAM backoff (OOM-aware).
    - Assign as many operators as possible per pool with:
        * strict priority preference for QUERY > INTERACTIVE > BATCH
        * reserved headroom for high priority when it is waiting (applies to BATCH only)
        * batch drip to ensure progress under constant high-priority load
    """
    s.tick += 1

    # --- Ingest new pipelines ---
    for p in pipelines:
        pid = p.pipeline_id
        if pid in s.active:
            continue
        s.active[pid] = p
        s.last_scheduled_tick[pid] = s.tick
        _get_queue_for_priority(s, p.priority).append(pid)

    # --- Learn from results (RAM backoff on failure) ---
    for r in results:
        if not getattr(r, "failed", lambda: False)():
            continue

        pid = _result_pipeline_id(r)
        err = ""
        try:
            err = (str(r.error) if r.error is not None else "").lower()
        except Exception:
            err = ""

        # Conservative backoff: OOM-like => 2.0x, otherwise 1.5x
        if ("oom" in err) or ("out of memory" in err) or ("memory" in err and "exceed" in err):
            backoff = 2.0
        else:
            backoff = 1.5

        # Update multiplier for each op if we can identify it.
        try:
            ops = r.ops or []
        except Exception:
            ops = []

        for op in ops:
            if pid is None:
                # Try infer pipeline_id from op if present.
                pid = getattr(op, "pipeline_id", None)

            if pid is None:
                continue

            key = (pid, _op_uid(op))
            prev = s.op_ram_mult.get(key, 1.0)
            # Cap to avoid unbounded growth; if it still fails, it will wait for full pool.
            s.op_ram_mult[key] = min(prev * backoff, 16.0)

    # Even if there are no new pipelines/results, we may have free resources due to prior completions.
    suspensions = []
    assignments = []

    # Hint whether high-priority is waiting (to decide whether to reserve headroom from batch).
    hi_waiting = _has_ready_high_priority_work(s)

    # Batch gating: if hi work is present, only allow batch once every N ticks (but still allow if no hi waiting).
    allow_batch_now = (not hi_waiting) or ((s.tick - s.last_batch_served_tick) >= s.batch_drip_interval_ticks)

    # --- Main scheduling loop across pools ---
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool

        # Simple local headroom reservation (applies only against batch when hi work is waiting).
        reserve_cpu = pool.max_cpu_pool * s.reserve_cpu_frac
        reserve_ram = pool.max_ram_pool * s.reserve_ram_frac

        # Bound attempts to avoid spinning when nothing fits.
        max_attempts = max(8, len(s.active) * 2)
        attempts = 0

        while attempts < max_attempts:
            attempts += 1

            # Stop if no resources for a minimal container.
            try:
                if float(avail_cpu) < 1.0 or float(avail_ram) <= 0.0:
                    break
            except Exception:
                if avail_cpu < 1 or avail_ram <= 0:
                    break

            p, op = _pick_pipeline_and_op(s, allow_batch=allow_batch_now)
            if p is None or op is None:
                break

            # Compute per-op learned RAM multiplier.
            key = (p.pipeline_id, _op_uid(op))
            ram_mult = s.op_ram_mult.get(key, 1.0)

            # Size resources.
            cpu = _desired_cpu(s, p.priority, pool.max_cpu_pool, avail_cpu)
            if not cpu or cpu <= 0:
                break

            ram = _desired_ram(s, p.priority, op, pool.max_ram_pool, avail_ram, ram_mult)
            if ram is None:
                # Can't fit now; try a different pipeline/op (might fit in remaining slack).
                continue

            # Enforce reserved headroom only for BATCH when hi-priority is waiting.
            if hi_waiting and p.priority == Priority.BATCH_PIPELINE:
                try:
                    if (float(avail_cpu) - float(cpu)) < float(reserve_cpu) or (float(avail_ram) - float(ram)) < float(reserve_ram):
                        # If this tick is not the batch drip moment, keep batch gated.
                        # Otherwise, still avoid stealing the entire pool if it would eliminate headroom.
                        if not allow_batch_now:
                            continue
                        # If it's batch drip, allow it only when it doesn't violate *both* reserves too hard.
                        # (Heuristic: allow if at least one of CPU or RAM reserve remains.)
                        if (float(avail_cpu) - float(cpu)) < float(reserve_cpu) and (float(avail_ram) - float(ram)) < float(reserve_ram):
                            continue
                except Exception:
                    pass

            # Ensure final fit.
            try:
                if float(cpu) > float(avail_cpu) or float(ram) > float(avail_ram):
                    continue
            except Exception:
                if cpu > avail_cpu or ram > avail_ram:
                    continue

            # Create assignment (one op per container for flexibility and better tail latency).
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

            # Update local accounting for additional packing this tick.
            try:
                avail_cpu = float(avail_cpu) - float(cpu)
                avail_ram = float(avail_ram) - float(ram)
            except Exception:
                avail_cpu -= cpu
                avail_ram -= ram

            s.last_scheduled_tick[p.pipeline_id] = s.tick
            if p.priority == Priority.BATCH_PIPELINE:
                s.last_batch_served_tick = s.tick

    return suspensions, assignments
