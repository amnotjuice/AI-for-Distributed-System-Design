# policy_key: scheduler_high_018
# reasoning_effort: high
# model: gpt-5.2-2025-12-11
# llm_cost: 0.121715
# generation_seconds: 148.27
# generated_at: 2026-03-12T23:05:47.842503
@register_scheduler_init(key="scheduler_high_018")
def scheduler_high_018_init(s):
    """
    Priority-aware, latency-biased scheduler with small, safe improvements over naive FIFO:

    1) Priority queues (QUERY > INTERACTIVE > BATCH) to reduce queueing latency for high-priority work.
    2) Pack multiple operators per pool per tick (instead of one-op-per-pool), improving responsiveness under load.
    3) Conservative right-sizing (don't hand entire pool to a single op) with per-priority floors/caps.
    4) Simple OOM/failed retry handling via per-operator RAM backoff, plus decay on success.
    5) Optional multi-pool bias: if multiple pools exist, pool 0 is treated as a "latency pool"
       (BATCH is de-prioritized there while high-priority is waiting).
    6) Minimal fairness: batch "aging" allows old batch work to occasionally run even during contention.
    """
    from collections import deque

    # Core state
    s.tick = 0
    s.pipelines_by_id = {}  # pipeline_id -> Pipeline
    s.enqueue_tick = {}  # pipeline_id -> tick when first seen

    # Priority queues store pipeline_ids; we lazily skip finished/unknown pipelines while popping
    s.q_by_prio = {
        Priority.QUERY: deque(),
        Priority.INTERACTIVE: deque(),
        Priority.BATCH_PIPELINE: deque(),
    }

    # Per-operator resource hints (keyed by Python object identity to avoid assuming any op_id field)
    s.op_ram_mult = {}  # op_uid -> multiplier
    s.op_retries = {}  # op_uid -> retry count

    # Tuning knobs (keep simple / safe)
    s.max_retries = 5
    s.ram_backoff = 1.6   # on failure, multiply RAM hint
    s.ram_decay = 0.90    # on success, decay RAM hint toward 1.0
    s.batch_aging_ticks = 250  # after this, batch may be scheduled even under contention

    # Priority sizing floors/caps as fractions of pool capacity (avoids allocating *everything*)
    s.cpu_floor = {
        Priority.QUERY: 0.25,
        Priority.INTERACTIVE: 0.20,
        Priority.BATCH_PIPELINE: 0.15,
    }
    s.cpu_cap = {
        Priority.QUERY: 0.75,
        Priority.INTERACTIVE: 0.60,
        Priority.BATCH_PIPELINE: 0.50,
    }
    s.ram_floor = {
        Priority.QUERY: 0.12,
        Priority.INTERACTIVE: 0.08,
        Priority.BATCH_PIPELINE: 0.05,
    }
    s.ram_cap = {
        Priority.QUERY: 0.70,
        Priority.INTERACTIVE: 0.60,
        Priority.BATCH_PIPELINE: 0.50,
    }

    # Limit the number of in-flight ops per pipeline (prevents one pipeline from monopolizing a pool)
    s.inflight_cap = {
        Priority.QUERY: 2,
        Priority.INTERACTIVE: 2,
        Priority.BATCH_PIPELINE: 1,
    }


def _op_uid(op):
    # Prefer stable, explicit identifiers if they exist; otherwise fall back to Python object identity.
    for attr in ("op_id", "operator_id", "id", "name"):
        if hasattr(op, attr):
            try:
                v = getattr(op, attr)
                if isinstance(v, (int, str)) and v is not None:
                    return ("attr", attr, v)
            except Exception:
                pass
    return ("pyid", id(op))


def _safe_state_count(status, state):
    try:
        return status.state_counts.get(state, 0)
    except Exception:
        return 0


def _pipeline_done_or_dropped(s, pipeline):
    # Drop completed pipelines, and drop pipelines with a FAILED op that exceeded retry limit.
    status = pipeline.runtime_status()
    if status.is_pipeline_successful():
        return True

    try:
        failed_ops = status.get_ops([OperatorState.FAILED], require_parents_complete=False)
    except Exception:
        failed_ops = []

    for op in failed_ops:
        uid = _op_uid(op)
        if s.op_retries.get(uid, 0) >= s.max_retries:
            return True

    return False


def _get_assignable_ops(status, require_parents_complete=True):
    # Prefer simulator-provided ASSIGNABLE_STATES; otherwise fall back to PENDING+FAILED.
    try:
        states = ASSIGNABLE_STATES
    except Exception:
        states = [OperatorState.PENDING, OperatorState.FAILED]
    return status.get_ops(states, require_parents_complete=require_parents_complete)


def _inflight_count(status):
    # Count assigned/running/suspending as "in-flight"
    return (
        _safe_state_count(status, OperatorState.ASSIGNED)
        + _safe_state_count(status, OperatorState.RUNNING)
        + _safe_state_count(status, OperatorState.SUSPENDING)
    )


def _choose_resources(s, pool, pipeline_priority, op, avail_cpu, avail_ram):
    # Estimate minimum RAM if operator exposes it; otherwise choose a small baseline.
    min_ram = None
    for attr in ("ram_min", "min_ram", "min_ram_gb", "ram_requirement"):
        if hasattr(op, attr):
            try:
                v = getattr(op, attr)
                if isinstance(v, (int, float)) and v is not None and v > 0:
                    min_ram = float(v)
                    break
            except Exception:
                pass
    if min_ram is None:
        # 2% of pool RAM as a conservative default baseline
        min_ram = max(0.01, float(pool.max_ram_pool) * 0.02)

    # Apply per-op backoff multiplier (used for OOM/failed retries)
    uid = _op_uid(op)
    mult = float(s.op_ram_mult.get(uid, 1.0))
    ram_hint = min_ram * mult

    # Floors/caps by priority (avoid giving whole pool to one op; still give QUERY more)
    ram_floor = float(pool.max_ram_pool) * float(s.ram_floor[pipeline_priority])
    ram_cap = float(pool.max_ram_pool) * float(s.ram_cap[pipeline_priority])
    cpu_floor = float(pool.max_cpu_pool) * float(s.cpu_floor[pipeline_priority])
    cpu_cap = float(pool.max_cpu_pool) * float(s.cpu_cap[pipeline_priority])

    # Final request within available resources
    ram_req = max(ram_hint, ram_floor)
    ram_req = min(ram_req, ram_cap, float(avail_ram))

    cpu_req = max(1.0, cpu_floor)
    cpu_req = min(cpu_req, cpu_cap, float(avail_cpu))

    # Guard against pathological tiny allocations
    if cpu_req <= 0 or ram_req <= 0:
        return 0.0, 0.0

    return cpu_req, ram_req


def _queue_nonempty(s, prio):
    q = s.q_by_prio.get(prio)
    return bool(q) if q is not None else False


def _any_high_backlog(s):
    return _queue_nonempty(s, Priority.QUERY) or _queue_nonempty(s, Priority.INTERACTIVE)


def _pop_next_pipeline_id(s, prio, max_spins=64):
    """
    Pop/rotate the priority queue until we find a pipeline_id that still exists,
    otherwise return None. This keeps queue maintenance cheap and lazy.
    """
    q = s.q_by_prio[prio]
    spins = min(len(q), max_spins)
    for _ in range(spins):
        pid = q[0]
        q.rotate(-1)  # round-robin within a priority
        if pid in s.pipelines_by_id:
            return pid
        # If unknown, discard it
        try:
            q.remove(pid)
        except Exception:
            pass
    return None


@register_scheduler(key="scheduler_high_018")
def scheduler_high_018(s, results: List["ExecutionResult"], pipelines: List["Pipeline"]) -> Tuple[List["Suspend"], List["Assignment"]]:
    """
    Scheduling step:

    - Incorporate new pipelines.
    - Update per-op RAM hints using execution results (backoff on failure, decay on success).
    - Build assignments by filling each pool with multiple operators, prioritizing QUERY/INTERACTIVE.
    - Apply a small multi-pool bias: pool 0 is latency-sensitive when multiple pools exist.
    - Avoid over-allocating the entire pool to one operator to improve concurrency and reduce tail latency.
    """
    s.tick += 1

    suspensions: List["Suspend"] = []
    assignments: List["Assignment"] = []

    # 1) Ingest new pipelines into state + enqueue by priority
    for p in pipelines:
        pid = p.pipeline_id
        if pid not in s.pipelines_by_id:
            s.pipelines_by_id[pid] = p
            s.enqueue_tick[pid] = s.tick
            s.q_by_prio[p.priority].append(pid)

    # 2) Update resource hints based on results
    for r in results:
        # Update RAM backoff/decay for the specific ops that ran
        for op in getattr(r, "ops", []) or []:
            uid = _op_uid(op)
            if r.failed():
                s.op_retries[uid] = int(s.op_retries.get(uid, 0)) + 1
                s.op_ram_mult[uid] = float(s.op_ram_mult.get(uid, 1.0)) * float(s.ram_backoff)
            else:
                # Gentle decay toward 1.0 to avoid permanently oversized allocations
                cur = float(s.op_ram_mult.get(uid, 1.0))
                s.op_ram_mult[uid] = max(1.0, cur * float(s.ram_decay))

    # 3) Prune completed/dropped pipelines (lazy queue cleanup happens when popping)
    to_remove = []
    for pid, p in s.pipelines_by_id.items():
        if _pipeline_done_or_dropped(s, p):
            to_remove.append(pid)
    for pid in to_remove:
        s.pipelines_by_id.pop(pid, None)
        s.enqueue_tick.pop(pid, None)

    # Fast exit if no work and no updates
    if not s.pipelines_by_id:
        return suspensions, assignments

    # Track how many ops we schedule per pipeline in this tick (since runtime_status won't reflect new assignments yet)
    scheduled_this_tick = {}

    # Helper: decide whether batch should be allowed in this pool right now (latency protection)
    def batch_allowed(pool_id, pool, avail_cpu, avail_ram):
        if not _any_high_backlog(s):
            return True

        # If there is high priority backlog, keep pool 0 more latency-focused (when multiple pools exist)
        if s.executor.num_pools > 1 and pool_id == 0:
            return False

        # Otherwise, allow batch only if pool has ample headroom (so it won't block high-priority arrivals)
        return (float(avail_cpu) >= 0.50 * float(pool.max_cpu_pool)) and (float(avail_ram) >= 0.50 * float(pool.max_ram_pool))

    # Helper: pick next (pipeline, op) from queues with priority + simple aging for batch
    def pick_next_op(pool_id, pool, avail_cpu, avail_ram):
        # Determine if we should treat some batch as "aged" (fairness)
        aged_batch_exists = False
        now = s.tick
        # We don't scan all; we just use a cheap heuristic: if oldest batch item has aged enough, consider it.
        if s.q_by_prio[Priority.BATCH_PIPELINE]:
            oldest_pid = s.q_by_prio[Priority.BATCH_PIPELINE][0]
            enq = s.enqueue_tick.get(oldest_pid, now)
            aged_batch_exists = (now - enq) >= int(s.batch_aging_ticks)

        # Selection order:
        # - Always try QUERY first, then INTERACTIVE
        # - Try BATCH only if allowed OR if batch is aged (fairness escape hatch)
        prio_order = [Priority.QUERY, Priority.INTERACTIVE]
        if batch_allowed(pool_id, pool, avail_cpu, avail_ram) or aged_batch_exists:
            prio_order.append(Priority.BATCH_PIPELINE)

        for prio in prio_order:
            pid = _pop_next_pipeline_id(s, prio)
            if pid is None:
                continue
            p = s.pipelines_by_id.get(pid)
            if p is None:
                continue

            status = p.runtime_status()
            if status.is_pipeline_successful():
                continue

            # Enforce per-pipeline in-flight cap (including assignments we make in this tick)
            inflight = _inflight_count(status) + int(scheduled_this_tick.get(pid, 0))
            cap = int(s.inflight_cap.get(p.priority, 1))
            if inflight >= cap:
                continue

            # Choose one ready op (parents complete)
            try:
                ops = _get_assignable_ops(status, require_parents_complete=True)
            except Exception:
                ops = []
            if not ops:
                continue

            return p, ops[0]

        return None, None

    # 4) Build assignments by packing each pool with multiple ops
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)

        # If effectively no capacity, skip
        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # Keep making assignments until we can't fit more useful work
        # Safety bound prevents infinite loops in case of unexpected behaviors.
        safety = 0
        while avail_cpu > 0 and avail_ram > 0 and safety < 128:
            safety += 1

            pipeline, op = pick_next_op(pool_id, pool, avail_cpu, avail_ram)
            if pipeline is None or op is None:
                break

            cpu_req, ram_req = _choose_resources(s, pool, pipeline.priority, op, avail_cpu, avail_ram)
            if cpu_req <= 0 or ram_req <= 0:
                break

            assignments.append(
                Assignment(
                    ops=[op],
                    cpu=cpu_req,
                    ram=ram_req,
                    priority=pipeline.priority,
                    pool_id=pool_id,
                    pipeline_id=pipeline.pipeline_id,
                )
            )

            # Deduct local available resources
            avail_cpu -= float(cpu_req)
            avail_ram -= float(ram_req)

            # Track per-pipeline assignments we make this tick (for inflight cap)
            scheduled_this_tick[pipeline.pipeline_id] = int(scheduled_this_tick.get(pipeline.pipeline_id, 0)) + 1

            # If remaining capacity is tiny, stop packing to avoid lots of tiny allocations
            if avail_cpu < 1.0 or avail_ram < 0.01:
                break

    return suspensions, assignments
