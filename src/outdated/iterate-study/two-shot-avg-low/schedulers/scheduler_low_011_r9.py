# source: scheduler_low_011.py
# model: gpt-5.2-2025-12-11
# effort: low
# feedback: minimal
# iteration: r9
@register_scheduler_init(key="scheduler_low_011_r9")
def scheduler_low_011_r9_init(s):
    """Priority-first, latency-oriented scheduler (incremental upgrade over naive FIFO).

    Key incremental improvements:
    1) Strict priority queues (QUERY > INTERACTIVE > BATCH) with head-of-line blocking avoidance.
    2) Multi-assignment per pool per tick (instead of at most 1), to reduce queueing delay for high priority.
    3) Pool isolation: keep an "interactive" pool mostly free of batch when high-priority work is present
       (plus a short holdoff window after high-priority arrivals).
    4) OOM-aware retries that actually retry: learn per-operator RAM hints, bump retryable pipelines to the
       front of their queue, and avoid dropping pipelines just because they have FAILED ops.
    5) Light SRPT bias within each priority: among the first K queued pipelines, prefer fewer remaining ops.
    """
    import collections

    # Time / hysteresis
    s.ticks = 0
    s.holdoff_window_ticks = 3
    s.holdoff_until_tick = 0

    # Queueing state: store pipeline_ids (avoid duplicates); keep pipeline objects in active map
    s.queues = {
        Priority.QUERY: collections.deque(),
        Priority.INTERACTIVE: collections.deque(),
        Priority.BATCH_PIPELINE: collections.deque(),
    }
    s.in_queue = set()  # pipeline_id currently present in some queue
    s.active = {}       # pipeline_id -> Pipeline
    s.pid_priority = {} # pipeline_id -> Priority

    # Failure handling / learning
    # op_key = (pipeline_id, id(op))
    s.op_hints = {}       # op_key -> {"ram": float, "cpu": float}
    s.op_attempts = {}    # op_key -> int
    s.retryable_ops = set()  # op_key marked retryable (OOM) within retry budget
    s.dead_pipelines = set() # pipeline_ids to ignore (non-OOM failure or exceeded retries)

    # Map op identity back to pipeline_id (since results may not include pipeline_id)
    s.op_to_pid = {}  # id(op) -> pipeline_id

    # Tuning knobs (kept simple)
    s.max_retries_per_op = 3
    s.scan_k = 8  # consider up to first K pipelines per priority for SRPT pick

    # Sizing fractions (conservative to allow concurrency; RAM beyond minimum doesn't speed up)
    s.size_fracs = {
        Priority.QUERY: {"cpu": 0.50, "ram": 0.25},
        Priority.INTERACTIVE: {"cpu": 0.50, "ram": 0.33},
        Priority.BATCH_PIPELINE: {"cpu": 1.00, "ram": 1.00},
    }

    # If multiple pools exist, treat pool 0 as interactive by default
    s.interactive_pool_id = 0


def _prio_order():
    return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]


def _is_oom_error(err):
    if err is None:
        return False
    msg = str(err).lower()
    return ("oom" in msg) or ("out of memory" in msg) or ("memoryerror" in msg)


def _op_key(pid, op):
    return (pid, id(op))


def _remaining_ops_count(status):
    # Small SRPT bias: count all non-completed ops (including failed/assigned/running/suspending/pending)
    return (
        status.state_counts.get(OperatorState.PENDING, 0)
        + status.state_counts.get(OperatorState.ASSIGNED, 0)
        + status.state_counts.get(OperatorState.RUNNING, 0)
        + status.state_counts.get(OperatorState.SUSPENDING, 0)
        + status.state_counts.get(OperatorState.FAILED, 0)
    )


def _enqueue_back(s, pipeline):
    pid = pipeline.pipeline_id
    if pid in s.dead_pipelines:
        return
    # Don't enqueue completed pipelines
    if pipeline.runtime_status().is_pipeline_successful():
        return
    pr = pipeline.priority
    s.active[pid] = pipeline
    s.pid_priority[pid] = pr
    if pid in s.in_queue:
        return
    s.queues[pr].append(pid)
    s.in_queue.add(pid)


def _enqueue_front(s, pipeline):
    pid = pipeline.pipeline_id
    if pid in s.dead_pipelines:
        return
    if pipeline.runtime_status().is_pipeline_successful():
        return
    pr = pipeline.priority
    s.active[pid] = pipeline
    s.pid_priority[pid] = pr
    if pid in s.in_queue:
        # If it's already queued, move-to-front by rebuilding that queue (rare path: on OOM retry).
        q = s.queues[pr]
        if len(q) <= 1:
            return
        newq = type(q)()
        newq.appendleft(pid)
        for x in q:
            if x != pid:
                newq.append(x)
        s.queues[pr] = newq
        return
    s.queues[pr].appendleft(pid)
    s.in_queue.add(pid)


def _drop_pipeline_if_done_or_dead(s, pid, status_cache):
    p = s.active.get(pid)
    if p is None:
        s.in_queue.discard(pid)
        return True
    if pid in s.dead_pipelines:
        return True
    st = status_cache.get(pid)
    if st is None:
        st = p.runtime_status()
        status_cache[pid] = st
    if st.is_pipeline_successful():
        return True
    return False


def _has_ready_high_priority(s, status_cache, limit=16):
    # Fast-ish probe to decide whether to reserve interactive pool for latency.
    for pr in (Priority.QUERY, Priority.INTERACTIVE):
        q = s.queues[pr]
        checked = 0
        for pid in q:
            if pid in s.dead_pipelines:
                continue
            p = s.active.get(pid)
            if p is None:
                continue
            st = status_cache.get(pid)
            if st is None:
                st = p.runtime_status()
                status_cache[pid] = st
            ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
            if ops:
                return True
            checked += 1
            if checked >= limit:
                break
    return False


def _default_request(s, pool, priority, has_high_prio_pressure):
    # Keep requests bounded and concurrency-friendly for high priority.
    fr = s.size_fracs.get(priority, {"cpu": 1.0, "ram": 1.0})

    cpu_frac = fr["cpu"]
    ram_frac = fr["ram"]

    # When high-priority is present, cap batch to reduce interference and preserve headroom.
    if priority == Priority.BATCH_PIPELINE and has_high_prio_pressure:
        cpu_frac = min(cpu_frac, 0.50)
        ram_frac = min(ram_frac, 0.80)

    cpu = max(1.0, pool.max_cpu_pool * cpu_frac)
    ram = max(1.0, pool.max_ram_pool * ram_frac)

    # Cap to current available resources
    cpu = min(cpu, pool.avail_cpu_pool)
    ram = min(ram, pool.avail_ram_pool)

    # Ensure positive (in case avail is tiny); caller will stop if avail < 1
    cpu = max(0.0, cpu)
    ram = max(0.0, ram)
    return cpu, ram


def _apply_hints(s, pid, op, cpu, ram):
    k = _op_key(pid, op)
    hint = s.op_hints.get(k)
    if not hint:
        return cpu, ram
    # Use max to avoid regressing on retries.
    hcpu = float(hint.get("cpu", 0.0) or 0.0)
    hram = float(hint.get("ram", 0.0) or 0.0)
    cpu = max(cpu, hcpu) if hcpu > 0 else cpu
    ram = max(ram, hram) if hram > 0 else ram
    return cpu, ram


def _select_pipeline_for_pool(s, pr, pool, pool_id, status_cache, scheduled_pids, has_high_prio_pressure):
    """Pick one (pid, op) from priority queue pr for this pool.

    Strategy:
    - Look at first K pipeline_ids in the queue.
    - Skip completed/dead pipelines; rotate blocked ones to back to reduce HoL blocking.
    - Among ready pipelines, pick smallest remaining-ops (SRPT-ish).
    - If op is marked retryable (OOM) and its hinted RAM cannot fit in this pool, prefer not to place it here.
    """
    q = s.queues[pr]
    if not q:
        return None, None

    pulled = []
    best = None  # (remaining_ops, pid, op, blocked_flag)
    # Pull up to K from the front for evaluation.
    for _ in range(min(s.scan_k, len(q))):
        pid = q.popleft()
        s.in_queue.discard(pid)
        pulled.append(pid)

        if pid in scheduled_pids:
            continue
        if pid in s.dead_pipelines:
            continue
        p = s.active.get(pid)
        if p is None:
            continue

        st = status_cache.get(pid)
        if st is None:
            st = p.runtime_status()
            status_cache[pid] = st

        if st.is_pipeline_successful():
            continue

        ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        if not ops:
            # blocked: we'll rotate it to back later
            continue

        op = ops[0]

        # If this op is an OOM-retry, avoid placing it on a pool that can't satisfy its RAM hint.
        k = _op_key(pid, op)
        if k in s.retryable_ops:
            hint = s.op_hints.get(k, {})
            need_ram = float(hint.get("ram", 0.0) or 0.0)
            if need_ram > 0.0 and need_ram > pool.avail_ram_pool:
                continue

        rem = _remaining_ops_count(st)
        cand = (rem, pid, op)
        if best is None or cand[0] < best[0]:
            best = cand

    # Rebuild queue: keep order, but rotate blocked pipelines to back (those pulled but not chosen/ready).
    chosen_pid = best[1] if best else None
    for pid in pulled:
        if pid == chosen_pid:
            continue
        # If pipeline became dead/completed, drop it.
        if pid in s.dead_pipelines:
            continue
        p = s.active.get(pid)
        if p is None:
            continue
        st = status_cache.get(pid)
        if st is None:
            st = p.runtime_status()
            status_cache[pid] = st
        if st.is_pipeline_successful():
            continue
        # Rotate to back to reduce HoL blocking and spread opportunities.
        q.append(pid)
        s.in_queue.add(pid)

    if not best:
        return None, None

    return best[1], best[2]


@register_scheduler(key="scheduler_low_011_r9")
def scheduler_low_011_r9(s, results, pipelines):
    """
    Latency-oriented priority scheduler with:
    - strict priority ordering
    - interactive-pool batch throttling (with short holdoff window)
    - multi-assignment per pool per tick
    - OOM-aware retries with learned RAM hints
    - small SRPT bias within each priority queue
    """
    s.ticks += 1

    # Cache runtime_status() calls within this tick
    status_cache = {}

    # Register new pipelines; extend holdoff after high-priority arrivals
    for p in pipelines:
        s.active[p.pipeline_id] = p
        s.pid_priority[p.pipeline_id] = p.priority
        if p.priority in (Priority.QUERY, Priority.INTERACTIVE):
            s.holdoff_until_tick = max(s.holdoff_until_tick, s.ticks + s.holdoff_window_ticks)
        _enqueue_back(s, p)

    # Process results: learn from OOM and mark dead on non-OOM failure
    for r in results:
        # Clear retryable markers on success for any ops returned
        if not r.failed():
            for op in (getattr(r, "ops", None) or []):
                pid = s.op_to_pid.get(id(op))
                if pid is None:
                    continue
                s.retryable_ops.discard(_op_key(pid, op))
            continue

        oom = _is_oom_error(getattr(r, "error", None))
        for op in (getattr(r, "ops", None) or []):
            pid = s.op_to_pid.get(id(op))
            if pid is None:
                continue

            if not oom:
                # Non-OOM failure: treat pipeline as dead to avoid wasting resources on deterministic failures.
                s.dead_pipelines.add(pid)
                continue

            # OOM: update RAM hint and allow retry up to budget.
            k = _op_key(pid, op)
            prev = s.op_hints.get(k, {})
            prev_ram = float(prev.get("ram", 0.0) or 0.0)
            prev_cpu = float(prev.get("cpu", 0.0) or 0.0)

            observed_ram = float(getattr(r, "ram", 0.0) or 0.0)
            observed_cpu = float(getattr(r, "cpu", 0.0) or 0.0)

            base_ram = max(1.0, prev_ram, observed_ram if observed_ram > 0 else 1.0)
            base_cpu = max(1.0, prev_cpu, observed_cpu if observed_cpu > 0 else 1.0)

            new_ram = base_ram * 2.0
            s.op_hints[k] = {"ram": new_ram, "cpu": base_cpu}

            att = int(s.op_attempts.get(k, 0)) + 1
            s.op_attempts[k] = att

            if att <= s.max_retries_per_op:
                s.retryable_ops.add(k)
                # Bump retryable pipeline to the front to reduce tail latency on transient under-allocation.
                p = s.active.get(pid)
                if p is not None:
                    _enqueue_front(s, p)
            else:
                s.dead_pipelines.add(pid)

    # If no new information, short-circuit
    if not pipelines and not results:
        return [], []

    # Decide whether to reserve interactive pool for high priority work
    has_high_prio_ready = _has_ready_high_priority(s, status_cache, limit=16)
    in_holdoff = s.ticks <= s.holdoff_until_tick

    # Choose pool iteration order:
    # - If high priority is ready, schedule interactive pool first.
    # - Otherwise, keep batch away from interactive by scheduling other pools first.
    num_pools = s.executor.num_pools
    if num_pools <= 0:
        return [], []

    ip = min(max(0, int(getattr(s, "interactive_pool_id", 0))), num_pools - 1)
    all_pools = list(range(num_pools))
    if has_high_prio_ready:
        pool_order = [ip] + [i for i in all_pools if i != ip]
    else:
        pool_order = [i for i in all_pools if i != ip] + [ip]

    suspensions = []
    assignments = []
    scheduled_pids = set()  # at most one op per pipeline per tick to avoid duplicate assignments

    # Assign as much as fits in each pool, priority-first.
    for pool_id in pool_order:
        pool = s.executor.pools[pool_id]

        # Stop if effectively no allocatable capacity.
        if pool.avail_cpu_pool < 1.0 or pool.avail_ram_pool < 1.0:
            continue

        # In interactive pool, avoid batch if high priority is present or within holdoff.
        allow_batch_here = not (pool_id == ip and (has_high_prio_ready or in_holdoff))

        # Keep issuing assignments while resources remain.
        while pool.avail_cpu_pool >= 1.0 and pool.avail_ram_pool >= 1.0:
            picked = False
            for pr in _prio_order():
                if pr == Priority.BATCH_PIPELINE and not allow_batch_here:
                    continue

                pid, op = _select_pipeline_for_pool(
                    s=s,
                    pr=pr,
                    pool=pool,
                    pool_id=pool_id,
                    status_cache=status_cache,
                    scheduled_pids=scheduled_pids,
                    has_high_prio_pressure=has_high_prio_ready,
                )
                if pid is None or op is None:
                    continue

                # Drop if dead/completed (race with state updates)
                if pid in s.dead_pipelines:
                    continue
                p = s.active.get(pid)
                if p is None:
                    continue
                st = status_cache.get(pid)
                if st is None:
                    st = p.runtime_status()
                    status_cache[pid] = st
                if st.is_pipeline_successful():
                    continue

                # Compute request and apply hints
                cpu, ram = _default_request(s, pool, pr, has_high_prio_ready)
                cpu, ram = _apply_hints(s, pid, op, cpu, ram)

                # Final cap to availability (avoid over-alloc); ensure minimally allocatable.
                cpu = min(cpu, pool.avail_cpu_pool)
                ram = min(ram, pool.avail_ram_pool)
                if cpu < 1.0 or ram < 1.0:
                    # Not enough room for even minimal allocation; stop filling this pool this tick.
                    picked = True  # prevents trying lower prio just to spin
                    break

                # Record mapping for result attribution
                s.op_to_pid[id(op)] = pid

                assignments.append(
                    Assignment(
                        ops=[op],
                        cpu=cpu,
                        ram=ram,
                        priority=pr,
                        pool_id=pool_id,
                        pipeline_id=pid,
                    )
                )

                # Re-enqueue pipeline to back for future ops (unless dead/completed)
                scheduled_pids.add(pid)
                if pid not in s.dead_pipelines:
                    _enqueue_back(s, p)

                picked = True
                break  # re-evaluate from highest priority with updated pool availability

            if not picked:
                break

    return suspensions, assignments