# policy_key: scheduler_medium_033
# reasoning_effort: medium
# exp: reasoning
# model: gpt-5.2-2025-12-11
# llm_cost: 0.106481
# generation_seconds: 83.93
# generated_at: 2026-04-09T23:14:12.923165
@register_scheduler_init(key="scheduler_medium_033")
def scheduler_medium_033_init(s):
    """
    Priority-aware, failure-averse scheduler with conservative sizing + OOM backoff.

    Core ideas:
      - Strict preference for QUERY, then INTERACTIVE, then BATCH, but with simple aging to
        avoid indefinite batch starvation.
      - Conservative initial RAM sizing (to reduce OOM/failures which are heavily penalized),
        and exponential RAM backoff on failures.
      - Soft "headroom protection": when high-priority work is waiting, limit batch admission
        unless ample spare capacity exists.
      - Keep scheduling simple and robust: no reliance on executor internals for preemption.
    """
    # Tick counter used for FIFO/aging without needing simulator time.
    s._tick = 0

    # Per-priority FIFO queues of pipeline_ids
    s._q_query = []
    s._q_interactive = []
    s._q_batch = []

    # Track whether a pipeline_id is enqueued (avoid duplicates)
    s._in_queue = set()

    # Remember pipeline objects and basic metadata
    # meta: {pipeline_id: {"enqueue_tick": int, "last_scheduled_tick": int}}
    s._pipelines = {}
    s._meta = {}

    # Per-operator RAM estimate with exponential backoff on failures.
    # key: (pipeline_id, op_key) -> {"ram_est": float, "fails": int}
    s._op_ram = {}

    # If an operator's estimated RAM exceeds global max RAM, it is unschedulable.
    # Track pipelines we stop trying to schedule to avoid endless spinning.
    s._abandoned = set()

    # Tuning knobs (ticks are scheduler-calls, not seconds)
    s._batch_aging_threshold = 80          # after this, batch is treated more like interactive
    s._max_assignments_per_pool = 4        # modest packing while keeping interference manageable
    s._max_fail_backoff = 10               # prevent infinite backoff loops per-op
    s._min_cpu = 1.0


def _p_weight(priority):
    # Larger is higher priority for selection.
    if priority == Priority.QUERY:
        return 3
    if priority == Priority.INTERACTIVE:
        return 2
    return 1  # Priority.BATCH_PIPELINE


def _get_op_key(op):
    # Best-effort stable key for an operator within a pipeline.
    for attr in ("op_id", "operator_id", "id", "name"):
        if hasattr(op, attr):
            try:
                v = getattr(op, attr)
                if callable(v):
                    v = v()
                if v is not None:
                    return (attr, str(v))
            except Exception:
                pass
    # Fallback to object identity (stable for lifetime of objects in the simulation).
    return ("py_id", str(id(op)))


def _op_base_ram(op):
    # Best-effort minimum RAM hint from operator object; fall back to None.
    for attr in ("ram_min", "min_ram", "ram", "memory", "mem"):
        if hasattr(op, attr):
            try:
                v = getattr(op, attr)
                if callable(v):
                    v = v()
                if v is not None:
                    v = float(v)
                    if v > 0:
                        return v
            except Exception:
                pass
    return None


def _enqueue_pipeline(s, p):
    pid = p.pipeline_id
    if pid in s._abandoned:
        return
    if pid not in s._pipelines:
        s._pipelines[pid] = p
        s._meta[pid] = {"enqueue_tick": s._tick, "last_scheduled_tick": -10**9}

    if pid in s._in_queue:
        return

    if p.priority == Priority.QUERY:
        s._q_query.append(pid)
    elif p.priority == Priority.INTERACTIVE:
        s._q_interactive.append(pid)
    else:
        s._q_batch.append(pid)
    s._in_queue.add(pid)


def _dequeue_one(q):
    if not q:
        return None
    return q.pop(0)


def _requeue(s, pid):
    # Requeue based on original priority.
    if pid in s._abandoned:
        return
    p = s._pipelines.get(pid)
    if p is None:
        return
    if pid in s._in_queue:
        # It should have been removed before scheduling attempt; keep robust.
        return
    if p.priority == Priority.QUERY:
        s._q_query.append(pid)
    elif p.priority == Priority.INTERACTIVE:
        s._q_interactive.append(pid)
    else:
        s._q_batch.append(pid)
    s._in_queue.add(pid)


def _global_max_ram(s):
    mx = 0.0
    for i in range(s.executor.num_pools):
        try:
            mx = max(mx, float(s.executor.pools[i].max_ram_pool))
        except Exception:
            pass
    return mx


def _pick_next_pid_for_pool(s, allow_batch):
    """
    Choose next pipeline id to try scheduling from priority queues with simple aging.

    Returns:
      pid or None
    """
    # Aging: if a batch pipeline has waited long enough, treat it like interactive in selection.
    # Implementation: we still pop from the real queues, but we may look at batch earlier.
    def batch_is_aged(pid):
        meta = s._meta.get(pid)
        if not meta:
            return False
        return (s._tick - meta["enqueue_tick"]) >= s._batch_aging_threshold

    # Build a list of (queue_name, queue_obj) in the order we will attempt.
    # If any batch is aged, interleave batch between interactive and batch.
    aged_batch_exists = any(batch_is_aged(pid) for pid in s._q_batch[: min(len(s._q_batch), 32)])
    if allow_batch and aged_batch_exists:
        queues = [("query", s._q_query), ("interactive", s._q_interactive), ("batch", s._q_batch), ("batch", s._q_batch)]
    else:
        queues = [("query", s._q_query), ("interactive", s._q_interactive)]
        if allow_batch:
            queues.append(("batch", s._q_batch))

    # Try at most one full pass over each queue to find a viable candidate.
    for _, q in queues:
        n = len(q)
        for _ in range(n):
            pid = _dequeue_one(q)
            if pid is None:
                break
            s._in_queue.discard(pid)
            if pid in s._abandoned:
                continue
            p = s._pipelines.get(pid)
            if p is None:
                continue

            status = p.runtime_status()
            # Drop completed pipelines from scheduling state.
            if status.is_pipeline_successful():
                continue

            # Avoid rescheduling the same pipeline multiple times in the same tick (reduces burstiness).
            meta = s._meta.get(pid)
            if meta and meta["last_scheduled_tick"] == s._tick:
                _requeue(s, pid)
                continue

            return pid

    return None


def _estimate_ram_for_op(s, pipeline_id, op, pool):
    """
    Conservative RAM sizing:
      - Start from operator min RAM if available; otherwise from a pool-fraction default.
      - If we have a learned estimate from prior failures, use it.
      - Cap to pool.max_ram_pool, and never exceed available pool RAM at assignment time.
    """
    opk = _get_op_key(op)
    rec = s._op_ram.get((pipeline_id, opk))
    base = _op_base_ram(op)

    # Fallback defaults by pool size: start relatively conservatively to reduce OOM risk.
    # (More RAM than needed doesn't hurt performance; less can cause OOM and huge score penalty.)
    pool_max = float(pool.max_ram_pool)
    if base is None or base <= 0:
        base = max(1.0, 0.30 * pool_max)  # safe-ish initial guess

    ram_est = float(base)
    if rec and rec.get("ram_est"):
        ram_est = max(ram_est, float(rec["ram_est"]))

    # Ensure not exceeding pool's maximum.
    ram_est = min(ram_est, pool_max)
    return ram_est


def _choose_cpu_for_priority(priority, pool, avail_cpu):
    """
    CPU sizing heuristic:
      - Give queries more CPU to lower latency.
      - Keep some room for concurrency when possible (sublinear scaling).
    """
    pool_max = float(pool.max_cpu_pool)
    avail_cpu = float(avail_cpu)

    if priority == Priority.QUERY:
        target = 0.75 * pool_max
    elif priority == Priority.INTERACTIVE:
        target = 0.60 * pool_max
    else:
        target = 0.40 * pool_max

    # Use up to target, but not more than what's currently available.
    cpu = min(avail_cpu, max(1.0, target))

    # If pool has plenty of headroom, allow using more to finish faster.
    if priority in (Priority.QUERY, Priority.INTERACTIVE) and avail_cpu >= 0.90 * pool_max:
        cpu = min(avail_cpu, pool_max)

    return cpu


@register_scheduler(key="scheduler_medium_033")
def scheduler_medium_033(s, results, pipelines):
    """
    Priority-aware scheduler tuned for weighted-latency with heavy failure penalty.

    - Enqueue all new pipelines.
    - Update RAM estimates from failures (exponential backoff).
    - For each pool, schedule up to K ready operators from best available pipelines,
      respecting soft headroom rules for batch when high-priority is waiting.
    """
    s._tick += 1

    # Add new pipelines to state/queues.
    for p in pipelines:
        _enqueue_pipeline(s, p)

    # Update learned RAM estimates from execution results.
    # We treat failures as likely OOM and increase RAM estimate; this improves completion rate.
    # If failures aren't OOM-related, the backoff may over-allocate but still helps avoid retries.
    global_max_ram = _global_max_ram(s)

    for r in results:
        # If result doesn't map cleanly to a pipeline/op, skip safely.
        try:
            pid = r.ops[0].pipeline_id if hasattr(r.ops[0], "pipeline_id") else None
        except Exception:
            pid = None

        # We'll also try to find pipeline_id via r if it exists.
        if pid is None and hasattr(r, "pipeline_id"):
            try:
                pid = r.pipeline_id
            except Exception:
                pid = None

        if pid is None:
            continue

        p = s._pipelines.get(pid)
        if p is None:
            # Pipeline could have arrived earlier but not tracked (defensive).
            continue

        # Re-enqueue pipeline after any state change to ensure forward progress.
        _enqueue_pipeline(s, p)

        if not getattr(r, "ops", None):
            continue

        # Only update estimate for the first op in the container (we schedule 1 op per assignment).
        op = r.ops[0]
        opk = _get_op_key(op)
        rec = s._op_ram.get((pid, opk), {"ram_est": None, "fails": 0})

        if hasattr(r, "failed") and r.failed():
            rec["fails"] = int(rec.get("fails", 0)) + 1

            # Exponential RAM backoff, capped by global max RAM.
            prev_est = rec["ram_est"]
            if prev_est is None:
                try:
                    prev_est = float(r.ram) if hasattr(r, "ram") else None
                except Exception:
                    prev_est = None

            if prev_est is None:
                # If we don't know what we tried, start with a conservative chunk of the pool max.
                prev_est = max(1.0, 0.35 * global_max_ram)

            new_est = float(prev_est) * 1.8  # gentler than doubling to reduce over-allocation jumps
            new_est = min(new_est, float(global_max_ram))

            rec["ram_est"] = new_est
            s._op_ram[(pid, opk)] = rec

            # If we've hit the ceiling repeatedly, abandon to avoid endless reattempts that block others.
            if rec["fails"] >= s._max_fail_backoff and rec["ram_est"] >= float(global_max_ram) * 0.999:
                s._abandoned.add(pid)

        else:
            # On success, keep the estimate at least as high as what was used (stable; avoids regression).
            try:
                used = float(r.ram) if hasattr(r, "ram") and r.ram is not None else None
            except Exception:
                used = None
            if used is not None and used > 0:
                rec["ram_est"] = max(float(rec["ram_est"] or 0.0), used)
                s._op_ram[(pid, opk)] = rec

    # Early exit if nothing to do.
    if not pipelines and not results and not (s._q_query or s._q_interactive or s._q_batch):
        return [], []

    suspensions = []
    assignments = []

    # Soft headroom: if any high-priority is waiting, be cautious scheduling batch.
    high_waiting = bool(s._q_query or s._q_interactive)

    # Schedule per pool with limited packing.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)

        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # Decide whether to allow batch scheduling in this pool right now.
        # If high-priority exists, only allow batch when there is plenty of spare capacity.
        allow_batch = True
        if high_waiting:
            allow_batch = (avail_cpu >= 0.60 * float(pool.max_cpu_pool)) and (avail_ram >= 0.60 * float(pool.max_ram_pool))

        num_assigned = 0
        while num_assigned < s._max_assignments_per_pool and avail_cpu > 0 and avail_ram > 0:
            pid = _pick_next_pid_for_pool(s, allow_batch=allow_batch)
            if pid is None:
                break

            p = s._pipelines.get(pid)
            if p is None or pid in s._abandoned:
                continue

            status = p.runtime_status()

            # If already done, skip.
            if status.is_pipeline_successful():
                continue

            # Only schedule ready ops (parents complete).
            op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
            if not op_list:
                # Not ready; requeue to avoid dropping and keep fairness.
                _requeue(s, pid)
                continue

            op = op_list[0]

            # Compute resource request.
            ram_req = _estimate_ram_for_op(s, pid, op, pool)
            cpu_req = _choose_cpu_for_priority(p.priority, pool, avail_cpu)

            # If the op cannot possibly fit in this pool, requeue and try later (maybe another pool).
            if ram_req > float(pool.max_ram_pool) + 1e-9:
                _requeue(s, pid)
                continue

            # If not enough currently available, requeue and stop packing this pool (avoid spinning).
            if ram_req > avail_ram + 1e-9 or cpu_req > avail_cpu + 1e-9:
                _requeue(s, pid)
                break

            # Create assignment.
            assignments.append(
                Assignment(
                    ops=[op],
                    cpu=cpu_req,
                    ram=ram_req,
                    priority=p.priority,
                    pool_id=pool_id,
                    pipeline_id=p.pipeline_id,
                )
            )

            # Bookkeeping.
            s._meta[pid]["last_scheduled_tick"] = s._tick
            # Keep pipeline eligible for future ops by requeueing (it won't reschedule the same op).
            _requeue(s, pid)

            avail_cpu -= float(cpu_req)
            avail_ram -= float(ram_req)
            num_assigned += 1

            # If we just scheduled high priority, tighten batch admission further in this pool.
            if p.priority in (Priority.QUERY, Priority.INTERACTIVE):
                allow_batch = False

    return suspensions, assignments
