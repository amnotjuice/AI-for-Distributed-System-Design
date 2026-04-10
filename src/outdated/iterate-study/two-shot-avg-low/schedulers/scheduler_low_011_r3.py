# source: scheduler_low_011.py
# model: gpt-5.2-2025-12-11
# effort: low
# feedback: minimal
# iteration: r3
@register_scheduler_init(key="scheduler_low_011_r3")
def scheduler_low_011_r3_init(s):
    """Priority-aware, latency-leaning scheduler (incremental step beyond naive FIFO).

    Key ideas (kept simple/robust):
    - Separate FIFO queues per priority; schedule QUERY > INTERACTIVE > BATCH.
    - Avoid head-of-line blocking within a priority via round-robin scan for a ready op.
    - Soft resource reservations: when high-priority work is waiting, limit BATCH to leave headroom.
    - Spillover placement: prefer a designated interactive pool for high priority, but allow spillover after a short wait.
    - OOM-aware retry: on OOM-like failures, increase RAM hint (exponential backoff) and retry up to a small cap.
      Non-OOM failures are treated as non-retriable for that operator to avoid infinite retry loops.
    - Multiple assignments per pool per tick (bounded) to reduce queueing delays.
    """
    from collections import deque

    s.tick = 0

    # Per-priority FIFO queues (store Pipeline objects)
    s.queues = {
        Priority.QUERY: deque(),
        Priority.INTERACTIVE: deque(),
        Priority.BATCH_PIPELINE: deque(),
    }

    # De-dup pipelines to avoid ballooning queues
    s.enqueued_pipeline_ids = set()
    s.pipeline_first_seen_tick = {}

    # Prefer this pool for QUERY/INTERACTIVE when possible
    s.interactive_pool_id = 0
    s.spillover_wait_ticks = 2  # after this, high-priority can run on any pool

    # Scheduling bounds
    s.max_assignments_per_pool_per_tick = 4
    s.max_preemptions_per_tick = 2  # best-effort; only used if we can introspect running containers

    # Minimum allocs (defensive; simulation typically uses continuous resources)
    s.min_cpu = 1.0
    s.min_ram = 1.0

    # Default sizing as fractions of pool max (tuned for latency: give high priority more CPU)
    s.size_fracs = {
        Priority.QUERY: {"cpu": 0.90, "ram": 0.65},
        Priority.INTERACTIVE: {"cpu": 0.80, "ram": 0.65},
        Priority.BATCH_PIPELINE: {"cpu": 0.70, "ram": 0.70},
    }

    # Soft reservation when high priority is waiting: keep this fraction of max free from NEW batch admits.
    s.reserve_fracs_when_high_waiting = {"cpu": 0.20, "ram": 0.20}

    # OOM retry / learning
    s.max_retries_per_op = 3
    s.op_hints = {}  # op_key -> {"ram": float, "cpu": float}
    s.op_attempts = {}  # op_key -> int (counts failures we observed)
    s.op_last_fail_oom = {}  # op_key -> bool


def _prio_order():
    return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]


def _is_oom_error(err):
    if err is None:
        return False
    msg = str(err).lower()
    return ("oom" in msg) or ("out of memory" in msg) or ("memoryerror" in msg)


def _op_key(op, pipeline_id=None, result=None):
    # Prefer (pipeline_id, id(op)) to avoid cross-pipeline contamination if pipeline_id is known.
    pid = pipeline_id
    if pid is None and result is not None:
        pid = getattr(result, "pipeline_id", None)
    if pid is None:
        pid = getattr(op, "pipeline_id", None)
    if pid is None:
        return ("op", id(op))
    return ("op", pid, id(op))


def _cleanup_pipeline_tracking(s, pipeline):
    pid = pipeline.pipeline_id
    s.enqueued_pipeline_ids.discard(pid)
    s.pipeline_first_seen_tick.pop(pid, None)


def _get_ready_op(pipeline):
    status = pipeline.runtime_status()
    ops = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
    if not ops:
        return None
    return ops[0]


def _default_request(pool, priority, min_cpu, min_ram, size_fracs):
    fr = size_fracs.get(priority, {"cpu": 1.0, "ram": 1.0})
    cpu = max(min_cpu, float(pool.max_cpu_pool) * float(fr["cpu"]))
    ram = max(min_ram, float(pool.max_ram_pool) * float(fr["ram"]))

    # Cap by pool max (defensive) and current availability (we'll also check again against local avail)
    cpu = min(cpu, float(pool.max_cpu_pool), float(pool.avail_cpu_pool))
    ram = min(ram, float(pool.max_ram_pool), float(pool.avail_ram_pool))
    return cpu, ram


def _apply_hints(s, pool, priority, pipeline_id, op):
    cpu, ram = _default_request(pool, priority, s.min_cpu, s.min_ram, s.size_fracs)

    k = _op_key(op, pipeline_id=pipeline_id)
    hint = s.op_hints.get(k)
    if hint:
        # Prefer higher of default/hint to reduce re-OOM and reduce tail via fewer retries.
        cpu = max(cpu, float(hint.get("cpu", cpu)))
        ram = max(ram, float(hint.get("ram", ram)))

    # Cap again
    cpu = min(cpu, float(pool.max_cpu_pool), float(pool.avail_cpu_pool))
    ram = min(ram, float(pool.max_ram_pool), float(pool.avail_ram_pool))

    # Ensure positive
    cpu = max(s.min_cpu, cpu)
    ram = max(s.min_ram, ram)
    return cpu, ram


def _iter_running_containers_best_effort(pool):
    # Try common shapes without assuming an exact schema.
    candidates = []
    for attr in ("running_containers", "containers", "active_containers"):
        obj = getattr(pool, attr, None)
        if obj is None:
            continue
        if isinstance(obj, dict):
            candidates.extend(list(obj.values()))
        elif isinstance(obj, (list, tuple, set)):
            candidates.extend(list(obj))
    # De-dup by container_id when possible
    seen = set()
    uniq = []
    for c in candidates:
        cid = getattr(c, "container_id", None)
        key = cid if cid is not None else id(c)
        if key in seen:
            continue
        seen.add(key)
        uniq.append(c)
    return uniq


def _container_priority_best_effort(container):
    # Return a Priority if present, else None.
    pr = getattr(container, "priority", None)
    if pr is not None:
        return pr
    pr = getattr(container, "pipeline_priority", None)
    if pr is not None:
        return pr
    return None


@register_scheduler(key="scheduler_low_011_r3")
def scheduler_low_011_r3(s, results, pipelines):
    """
    Scheduler step:
    - enqueue new pipelines by priority (de-duplicated)
    - update OOM hints from results
    - optional best-effort preemption of BATCH in the interactive pool when QUERY/INTERACTIVE is queued and pool is saturated
    - assign ready ops priority-first, with soft batch reservations while high-priority is waiting
    """
    s.tick += 1

    # Enqueue new arrivals (de-duped)
    for p in pipelines:
        pid = p.pipeline_id
        if pid in s.enqueued_pipeline_ids:
            continue
        s.enqueued_pipeline_ids.add(pid)
        s.pipeline_first_seen_tick.setdefault(pid, s.tick)

        pr = p.priority if p.priority in s.queues else Priority.BATCH_PIPELINE
        s.queues[pr].append(p)

    # Learn from results (OOM retry hints, avoid infinite retries on non-OOM failures)
    for r in results:
        failed = False
        try:
            failed = bool(r.failed())
        except Exception:
            failed = getattr(r, "error", None) is not None

        ops = getattr(r, "ops", None) or []

        if not failed:
            # On success, we can record that this RAM at least worked (conservative).
            for op in ops:
                k = _op_key(op, result=r)
                prev = s.op_hints.get(k, {})
                # Keep the max RAM we've seen succeed (can be slightly over-conservative but reduces tail retries).
                obs_ram = float(getattr(r, "ram", 0.0) or 0.0)
                obs_cpu = float(getattr(r, "cpu", 0.0) or 0.0)
                if obs_ram > 0:
                    prev_ram = float(prev.get("ram", 0.0) or 0.0)
                    prev["ram"] = max(prev_ram, obs_ram)
                if obs_cpu > 0:
                    prev_cpu = float(prev.get("cpu", 0.0) or 0.0)
                    prev["cpu"] = max(prev_cpu, obs_cpu)
                if prev:
                    s.op_hints[k] = prev
            continue

        # Failure path
        oom = _is_oom_error(getattr(r, "error", None))
        for op in ops:
            k = _op_key(op, result=r)

            # Count failures we observed for this op key
            s.op_attempts[k] = int(s.op_attempts.get(k, 0)) + 1
            s.op_last_fail_oom[k] = bool(oom)

            if oom:
                # Exponential backoff on RAM
                prev = s.op_hints.get(k, {})
                prev_ram = float(prev.get("ram", 0.0) or 0.0)
                obs_ram = float(getattr(r, "ram", 0.0) or 0.0)
                baseline = max(prev_ram, obs_ram, s.min_ram)
                new_ram = baseline * 2.0
                prev["ram"] = new_ram
                # Keep CPU hint at least 1; don't aggressively change CPU on OOM
                prev_cpu = float(prev.get("cpu", 0.0) or 0.0)
                obs_cpu = float(getattr(r, "cpu", 0.0) or 0.0)
                prev["cpu"] = max(prev_cpu, obs_cpu, s.min_cpu)
                s.op_hints[k] = prev
            else:
                # Mark as non-retriable by pushing attempts beyond cap.
                s.op_attempts[k] = max(int(s.op_attempts.get(k, 0)), s.max_retries_per_op + 10)

    # Early exit if nothing new happened
    if not pipelines and not results:
        return [], []

    # Determine if high priority is waiting (affects batch reservations)
    high_waiting = (len(s.queues[Priority.QUERY]) > 0) or (len(s.queues[Priority.INTERACTIVE]) > 0)

    suspensions = []
    assignments = []
    scheduled_pipelines_this_tick = set()

    # Best-effort preemption: if QUERY/INTERACTIVE is waiting and the interactive pool is saturated, suspend BATCH.
    # This is guarded heavily to avoid relying on executor internals; if we can't introspect, it becomes a no-op.
    preempt_budget = int(getattr(s, "max_preemptions_per_tick", 0) or 0)
    if high_waiting and s.executor.num_pools > 0 and preempt_budget > 0:
        ip = min(int(getattr(s, "interactive_pool_id", 0) or 0), s.executor.num_pools - 1)
        pool = s.executor.pools[ip]

        # Only consider preemption if we can't even start a minimal container right now.
        if float(pool.avail_cpu_pool) < s.min_cpu or float(pool.avail_ram_pool) < s.min_ram:
            running = _iter_running_containers_best_effort(pool)
            # Prefer suspending BATCH first; keep QUERY/INTERACTIVE intact if possible.
            batch_containers = []
            for c in running:
                pr = _container_priority_best_effort(c)
                if pr == Priority.BATCH_PIPELINE:
                    cid = getattr(c, "container_id", None)
                    if cid is not None:
                        batch_containers.append(cid)

            for cid in batch_containers[:preempt_budget]:
                suspensions.append(Suspend(container_id=cid, pool_id=ip))

    # Helper: pick next schedulable (pipeline, op, priority) from a queue without HoL blocking
    def try_pick_from_priority(pr, pool_id):
        q = s.queues[pr]
        if not q:
            return None

        # Scan each pipeline at most once per call (round-robin).
        n = len(q)
        for _ in range(n):
            p = q.popleft()
            status = p.runtime_status()

            # Drop completed pipelines
            if status.is_pipeline_successful():
                _cleanup_pipeline_tracking(s, p)
                continue

            # Don't schedule multiple ops from the same pipeline in the same tick
            if p.pipeline_id in scheduled_pipelines_this_tick:
                q.append(p)
                continue

            op = _get_ready_op(p)
            if op is None:
                # Not ready; keep it in queue
                q.append(p)
                continue

            # Retry safety: if we have evidence this op is non-retriable (non-OOM), drop pipeline.
            k = _op_key(op, pipeline_id=p.pipeline_id)
            attempts = int(s.op_attempts.get(k, 0) or 0)
            if attempts > s.max_retries_per_op:
                # If last failure was OOM, we still cap retries; drop to prevent thrash.
                # If last failure was non-OOM, also drop.
                _cleanup_pipeline_tracking(s, p)
                continue
            if attempts > 0 and not bool(s.op_last_fail_oom.get(k, True)):
                _cleanup_pipeline_tracking(s, p)
                continue

            # Pool preference for high priority:
            # Prefer interactive_pool_id, but allow spillover if it's been waiting a bit or interactive pool has no room.
            if pr in (Priority.QUERY, Priority.INTERACTIVE) and s.executor.num_pools > 1:
                ip = min(int(getattr(s, "interactive_pool_id", 0) or 0), s.executor.num_pools - 1)
                if pool_id != ip:
                    waited = s.tick - int(s.pipeline_first_seen_tick.get(p.pipeline_id, s.tick))
                    ip_pool = s.executor.pools[ip]
                    ip_has_room = (float(ip_pool.avail_cpu_pool) >= s.min_cpu) and (float(ip_pool.avail_ram_pool) >= s.min_ram)
                    if ip_has_room and waited < int(getattr(s, "spillover_wait_ticks", 0) or 0):
                        # Keep waiting for interactive pool
                        q.append(p)
                        continue

            # Keep pipeline in queue (we re-append after assignment) and return candidate
            q.append(p)
            return p, op

        return None

    # Main scheduling: for each pool, place up to a small number of ops, priority-first.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        local_avail_cpu = float(pool.avail_cpu_pool)
        local_avail_ram = float(pool.avail_ram_pool)

        if local_avail_cpu < s.min_cpu or local_avail_ram < s.min_ram:
            continue

        # Reservation thresholds applied only to NEW batch admits while high priority is waiting.
        reserve_cpu = 0.0
        reserve_ram = 0.0
        if high_waiting:
            reserve_cpu = float(pool.max_cpu_pool) * float(s.reserve_fracs_when_high_waiting["cpu"])
            reserve_ram = float(pool.max_ram_pool) * float(s.reserve_fracs_when_high_waiting["ram"])

        for _ in range(int(getattr(s, "max_assignments_per_pool_per_tick", 1) or 1)):
            if local_avail_cpu < s.min_cpu or local_avail_ram < s.min_ram:
                break

            chosen = None
            chosen_pr = None

            # Prefer high priority always
            for pr in _prio_order():
                cand = try_pick_from_priority(pr, pool_id)
                if cand is None:
                    continue
                p, op = cand

                # Determine request
                req_cpu, req_ram = _apply_hints(s, pool, pr, p.pipeline_id, op)

                # Soft reservations: if high is waiting, don't let NEW batch admits consume reserved headroom.
                eff_cpu = local_avail_cpu
                eff_ram = local_avail_ram
                if pr == Priority.BATCH_PIPELINE and high_waiting:
                    eff_cpu = max(0.0, local_avail_cpu - reserve_cpu)
                    eff_ram = max(0.0, local_avail_ram - reserve_ram)

                if req_cpu <= eff_cpu and req_ram <= eff_ram:
                    chosen = (p, op, req_cpu, req_ram)
                    chosen_pr = pr
                    break

            if chosen is None:
                break

            p, op, req_cpu, req_ram = chosen

            assignments.append(
                Assignment(
                    ops=[op],
                    cpu=req_cpu,
                    ram=req_ram,
                    priority=chosen_pr,
                    pool_id=pool_id,
                    pipeline_id=p.pipeline_id,
                )
            )
            scheduled_pipelines_this_tick.add(p.pipeline_id)

            # Decrement local available for additional placements in this pool this tick
            local_avail_cpu -= float(req_cpu)
            local_avail_ram -= float(req_ram)

    return suspensions, assignments