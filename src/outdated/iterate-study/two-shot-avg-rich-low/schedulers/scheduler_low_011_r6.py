# source: scheduler_low_011.py
# model: gpt-5.2-2025-12-11
# effort: low
# feedback: rich
# iteration: r6
@register_scheduler_init(key="scheduler_low_011_r6")
def scheduler_low_011_r6_init(s):
    """Priority-aware, latency-focused scheduler (incremental step from naive FIFO).

    Key fixes vs the previous iteration that achieved great median latency but starved INTERACTIVE/BATCH:
    - Avoid head-of-line blocking: rotate queues when a pipeline has no runnable operator yet.
    - Avoid underutilization: pack multiple assignments per pool per tick (track local remaining cpu/ram).
    - Prevent INTERACTIVE starvation: weighted round-robin across QUERY/INTERACTIVE (and only run BATCH when no high-priority can run).
    - Pool preference tweak: keep pool 0 biased for INTERACTIVE, push QUERY to non-0 pools when possible.

    Also keeps the low-risk OOM retry behavior with RAM backoff and a small retry cap.
    """
    # FIFO queues per priority + membership sets to avoid duplicate enqueues
    s.q = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }
    s.in_q = {
        Priority.QUERY: set(),
        Priority.INTERACTIVE: set(),
        Priority.BATCH_PIPELINE: set(),
    }

    # Learned per-operator hints (only RAM is really used; CPU hint kept for completeness)
    # key: (pipeline_id, op_identity) -> {"ram": float, "cpu": float}
    s.op_hints = {}
    s.op_attempts = {}
    s.op_blacklist = set()  # operators that failed non-OOM too many times (stop retrying)

    s.max_retries_per_op = 2

    # Pool preferences
    s.interactive_pool_id = 0

    # RR pattern: give INTERACTIVE and QUERY consistent service; BATCH only when possible
    # (We will still skip BATCH if any high-priority work can run.)
    s.rr_pattern = [
        Priority.INTERACTIVE,
        Priority.QUERY,
        Priority.INTERACTIVE,
        Priority.QUERY,
        Priority.BATCH_PIPELINE,
    ]
    s.rr_idx = 0

    # Bound per-tick work so we don't spin if queues contain mostly non-runnable pipelines
    s.max_global_assignments_per_tick = 64
    s.max_scan_per_pick = 32  # max pipelines to rotate per pick per priority

    # Conservative default sizing (fractions of pool max). We later adapt CPU down when backlog is large.
    s.base_fracs = {
        Priority.INTERACTIVE: {"cpu": 0.50, "ram": 0.40},
        Priority.QUERY: {"cpu": 0.45, "ram": 0.35},
        Priority.BATCH_PIPELINE: {"cpu": 0.90, "ram": 0.60},
    }


def _prio_of(p):
    pr = getattr(p, "priority", Priority.BATCH_PIPELINE)
    if pr in (Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE):
        return pr
    return Priority.BATCH_PIPELINE


def _is_oom_error(err):
    if err is None:
        return False
    msg = str(err).lower()
    return ("oom" in msg) or ("out of memory" in msg) or ("memoryerror" in msg)


def _op_identity(op):
    # Prefer a stable explicit id if present; fall back to object identity.
    oid = getattr(op, "op_id", None)
    return oid if oid is not None else id(op)


def _op_key(pipeline_id, op):
    return (pipeline_id, _op_identity(op))


def _pool_pref_rank(s, priority, pool_id):
    """Lower is better."""
    if s.executor.num_pools <= 1:
        return 0
    if priority == Priority.INTERACTIVE:
        return 0 if pool_id == s.interactive_pool_id else 1
    if priority == Priority.QUERY:
        # Push QUERY away from the interactive pool when possible.
        return 0 if pool_id != s.interactive_pool_id else 1
    # BATCH: also avoid pool 0.
    return 0 if pool_id != s.interactive_pool_id else 1


def _enqueue(s, pipeline, front=False):
    pr = _prio_of(pipeline)
    pid = pipeline.pipeline_id
    if pid in s.in_q[pr]:
        return
    if front:
        s.q[pr].insert(0, pipeline)
    else:
        s.q[pr].append(pipeline)
    s.in_q[pr].add(pid)


def _dequeue_head(s, pr):
    """Pop from head of pr queue, maintaining membership set."""
    q = s.q[pr]
    if not q:
        return None
    p = q.pop(0)
    s.in_q[pr].discard(p.pipeline_id)
    return p


def _rotate_back(s, pr, pipeline):
    """Append pipeline to tail (if not already queued) to avoid HoL blocking."""
    _enqueue(s, pipeline, front=False)


def _pick_runnable_op(s, pipeline):
    """Pick a runnable operator for this pipeline, skipping permanently blacklisted ops."""
    status = pipeline.runtime_status()
    if status.is_pipeline_successful():
        return None

    # Only run ops whose parents are complete.
    ops = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
    if not ops:
        return None

    pid = pipeline.pipeline_id
    for op in ops:
        k = _op_key(pid, op)
        if k in s.op_blacklist:
            # If the only runnable ops are blacklisted, treat pipeline as effectively dead.
            continue
        return op
    return None


def _high_priority_backlog(s):
    return len(s.q[Priority.INTERACTIVE]) + len(s.q[Priority.QUERY])


def _adaptive_request(s, pool, priority, pipeline_id, op, remaining_cpu, remaining_ram):
    """Compute (cpu, ram) request based on priority, backlog, and learned hints; cap to remaining resources."""
    fr = s.base_fracs.get(priority, {"cpu": 1.0, "ram": 1.0})
    backlog_hp = _high_priority_backlog(s)

    # CPU adaptation: when backlog is large, reduce per-op CPU to cut queueing delay.
    # (CPU scaling is sublinear; smaller slices often improve median latency under load.)
    cpu_frac = fr["cpu"]
    if priority in (Priority.INTERACTIVE, Priority.QUERY):
        if backlog_hp > 80:
            cpu_frac = min(cpu_frac, 0.20)
        elif backlog_hp > 20:
            cpu_frac = min(cpu_frac, 0.30)
    else:
        # Batch: be gentler when any high-priority exists
        if backlog_hp > 0:
            cpu_frac = min(cpu_frac, 0.20)

    # Baselines from pool max
    cpu = max(1.0, pool.max_cpu_pool * cpu_frac)
    ram = max(1.0, pool.max_ram_pool * fr["ram"])

    # Apply learned hints (primarily RAM from OOM retries)
    k = _op_key(pipeline_id, op)
    hint = s.op_hints.get(k)
    if hint:
        cpu = max(cpu, float(hint.get("cpu", cpu)))
        ram = max(ram, float(hint.get("ram", ram)))

    # Cap by remaining resources (local packing) and pool max
    cpu = min(cpu, remaining_cpu, pool.max_cpu_pool)
    ram = min(ram, remaining_ram, pool.max_ram_pool)

    # Ensure positive and feasible
    cpu = max(1.0, cpu)
    ram = max(1.0, ram)
    return cpu, ram


@register_scheduler(key="scheduler_low_011_r6")
def scheduler_low_011_r6(s, results, pipelines):
    """
    Scheduler step:
    - Enqueue new pipelines (front-load high-priority arrivals slightly).
    - Update OOM hints from failures and blacklist non-OOM repeated failures.
    - Build assignments with:
        * weighted round-robin across priorities,
        * packing multiple ops per pool per tick,
        * pool preference bias (pool 0 for INTERACTIVE),
        * batching only when no high-priority can run.
    """
    # Enqueue new arrivals; front-load high-priority to reduce admission delay
    for p in pipelines:
        pr = _prio_of(p)
        _enqueue(s, p, front=(pr in (Priority.INTERACTIVE, Priority.QUERY)))

    # Learn from failures: OOM => increase RAM and retry; non-OOM => eventually blacklist
    for r in results:
        failed = False
        try:
            failed = r.failed()
        except Exception:
            failed = getattr(r, "error", None) is not None

        if not failed:
            continue

        pid = getattr(r, "pipeline_id", None)
        ops = getattr(r, "ops", []) or []
        err = getattr(r, "error", None)

        # If we can't attribute to a pipeline/op, we can't learn; just skip.
        if pid is None or not ops:
            continue

        is_oom = _is_oom_error(err)
        for op in ops:
            k = _op_key(pid, op)
            prev_attempts = int(s.op_attempts.get(k, 0)) + 1
            s.op_attempts[k] = prev_attempts

            if is_oom and prev_attempts <= s.max_retries_per_op:
                # Exponential RAM backoff based on last known allocation.
                prev_hint = s.op_hints.get(k, {})
                base_ram = float(prev_hint.get("ram", 0.0) or 0.0)
                if base_ram <= 0.0:
                    base_ram = float(getattr(r, "ram", 1.0) or 1.0)
                new_ram = max(1.0, base_ram * 2.0)

                base_cpu = float(prev_hint.get("cpu", 0.0) or 0.0)
                if base_cpu <= 0.0:
                    base_cpu = float(getattr(r, "cpu", 1.0) or 1.0)

                s.op_hints[k] = {"ram": new_ram, "cpu": max(1.0, base_cpu)}
            else:
                # Non-OOM or too many retries => blacklist to avoid infinite rescheduling
                s.op_blacklist.add(k)

    # Nothing changed
    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    # Snapshot pool remaining resources (we pack within the tick)
    rem_cpu = {}
    rem_ram = {}
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        rem_cpu[pool_id] = float(pool.avail_cpu_pool)
        rem_ram[pool_id] = float(pool.avail_ram_pool)

    def any_high_priority_runnable():
        # Cheap-ish probe: try a bounded scan of the head elements without destructively consuming.
        for pr in (Priority.INTERACTIVE, Priority.QUERY):
            q = s.q[pr]
            lim = min(len(q), 8)
            for i in range(lim):
                p = q[i]
                op = _pick_runnable_op(s, p)
                if op is not None:
                    return True
        return False

    hp_runnable = any_high_priority_runnable()

    # Main loop: pick priority via RR pattern, then pick a runnable pipeline from that queue,
    # then pick a pool (by preference, then by remaining capacity), and assign if fits.
    made = 0
    max_iters = s.max_global_assignments_per_tick

    while made < max_iters:
        # Stop early if no capacity anywhere
        has_cap = False
        for pool_id in range(s.executor.num_pools):
            if rem_cpu[pool_id] >= 1.0 and rem_ram[pool_id] >= 1.0:
                has_cap = True
                break
        if not has_cap:
            break

        # Choose next priority (skip BATCH when high-priority is runnable)
        pr = s.rr_pattern[s.rr_idx % len(s.rr_pattern)]
        s.rr_idx += 1
        if pr == Priority.BATCH_PIPELINE and hp_runnable:
            # Don't even try batch while high-priority has runnable work
            continue

        # Pull a runnable pipeline from this priority queue (rotate to avoid HoL blocking)
        picked_pipeline = None
        picked_op = None

        scan = 0
        qlen = len(s.q[pr])
        # Bound scanning to avoid spinning on non-runnable queues
        max_scan = min(max(qlen, 1), s.max_scan_per_pick)
        while scan < max_scan and s.q[pr]:
            p = _dequeue_head(s, pr)
            if p is None:
                break

            # Drop completed pipelines
            if p.runtime_status().is_pipeline_successful():
                scan += 1
                continue

            op = _pick_runnable_op(s, p)
            if op is None:
                # Not runnable now; rotate to back
                _rotate_back(s, pr, p)
                scan += 1
                continue

            picked_pipeline = p
            picked_op = op
            break

        if picked_pipeline is None:
            # Nothing runnable in this class right now; try next RR slot
            # If we were probing HP runnable, refresh occasionally (cheaply) to let batch start once HP drains.
            if pr in (Priority.INTERACTIVE, Priority.QUERY) and hp_runnable:
                hp_runnable = any_high_priority_runnable()
            continue

        # Choose a pool for this op: prefer based on priority, then maximize remaining CPU (to reduce fragmentation).
        candidate_pools = list(range(s.executor.num_pools))
        candidate_pools.sort(
            key=lambda pid: (
                _pool_pref_rank(s, pr, pid),
                -rem_cpu[pid],
                -rem_ram[pid],
            )
        )

        assigned = False
        for pool_id in candidate_pools:
            if rem_cpu[pool_id] < 1.0 or rem_ram[pool_id] < 1.0:
                continue
            pool = s.executor.pools[pool_id]

            cpu, ram = _adaptive_request(
                s,
                pool,
                pr,
                picked_pipeline.pipeline_id,
                picked_op,
                remaining_cpu=rem_cpu[pool_id],
                remaining_ram=rem_ram[pool_id],
            )

            # Must fit the remaining resources in this tick's packing model
            if cpu <= rem_cpu[pool_id] + 1e-9 and ram <= rem_ram[pool_id] + 1e-9:
                assignments.append(
                    Assignment(
                        ops=[picked_op],
                        cpu=cpu,
                        ram=ram,
                        priority=pr,
                        pool_id=pool_id,
                        pipeline_id=picked_pipeline.pipeline_id,
                    )
                )
                rem_cpu[pool_id] -= float(cpu)
                rem_ram[pool_id] -= float(ram)
                made += 1
                assigned = True
                break

        # Requeue pipeline for future ops if it still has work; if we couldn't assign it, push back so it isn't lost.
        # (Even if it just got an op assigned, it may have other parallel-ready ops later; requeue keeps it visible.)
        if not picked_pipeline.runtime_status().is_pipeline_successful():
            _enqueue(s, picked_pipeline, front=False)

        if not assigned:
            # If we couldn't fit it anywhere, keep it around but don't spin trying forever this tick.
            # This also helps avoid repeated attempts when all pools are too tight.
            break

        # Refresh high-priority runnable flag occasionally
        if made % 8 == 0 and hp_runnable:
            hp_runnable = any_high_priority_runnable()

    return suspensions, assignments