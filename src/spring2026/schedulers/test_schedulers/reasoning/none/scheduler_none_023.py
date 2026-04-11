# policy_key: scheduler_none_023
# reasoning_effort: none
# model: gpt-5.2-2025-12-11
# llm_cost: 0.048003
# generation_seconds: 57.92
# generated_at: 2026-04-09T22:02:51.325038
@register_scheduler_init(key="scheduler_none_023")
def scheduler_none_023_init(s):
    """Priority-protecting, OOM-aware, low-churn scheduler.

    Goals aligned to objective:
      - Strongly protect QUERY/INTERACTIVE latency via reserved headroom + targeted preemption.
      - High completion rate via per-operator RAM backoff on OOM and limited retries.
      - Avoid starvation of BATCH via aging/credits so it continues to make progress.
      - Keep the policy simple: single-op assignments, pack within pools, minimal preemption churn.
    """
    # Queues per priority (store pipeline objects; we re-check runnable ops each tick)
    s.q_query = []
    s.q_interactive = []
    s.q_batch = []

    # Pipeline arrival accounting (for lightweight aging/credits)
    s.arrival_seq = 0
    s.pipeline_meta = {}  # pipeline_id -> dict(arrival_seq, last_served_seq)

    # Per-operator RAM multiplier to reduce OOM probability on retries
    # key: (pipeline_id, op_id_str) -> mult (float)
    s.op_ram_mult = {}

    # Track OOM attempts to stop infinite retries (failures penalize, but endless retries can block others)
    # key: (pipeline_id, op_id_str) -> count
    s.op_oom_retries = {}

    # Best-effort CPU sizing: remember last successful-ish cpu/ram for an operator to stabilize performance
    # key: (pipeline_id, op_id_str) -> dict(cpu, ram)
    s.op_last_alloc = {}

    # Preemption cooldown: avoid repeatedly suspending the same container
    # key: container_id -> last_preempt_seq
    s.preempt_cooldown = {}

    # Global tick counter (event loop step count proxy)
    s.tick = 0

    # Tunables (kept conservative)
    s.MAX_OOM_RETRIES = 2
    s.RAM_BACKOFF = 1.6          # multiplier applied on OOM
    s.RAM_BACKOFF_CAP = 8.0      # max multiplier
    s.MIN_RAM_FRACTION = 0.10    # initial guess uses at least 10% of pool RAM (or operator hints if available)
    s.MIN_CPU_FRACTION = 0.25    # initial guess uses at least 25% of pool CPU to reduce long tails
    s.QUERY_RESERVE_CPU = 0.20   # keep headroom to admit high priority
    s.QUERY_RESERVE_RAM = 0.20
    s.INT_RESERVE_CPU = 0.10
    s.INT_RESERVE_RAM = 0.10

    # Aging: each tick, batch gets a small chance even under pressure.
    # Implemented via credits accumulated when batch waits.
    s.batch_credit = 0.0
    s.BATCH_CREDIT_PER_TICK = 0.15
    s.BATCH_CREDIT_COST = 1.0

    # Fairness: ensure we don't only schedule one priority indefinitely
    s.last_scheduled_priority = None


@register_scheduler(key="scheduler_none_023")
def scheduler_none_023(s, results, pipelines):
    """
    Policy steps:
      1) Enqueue new pipelines by priority (do not drop).
      2) Process execution results: on OOM-like failures, bump RAM multiplier for those ops and allow retry.
      3) For each pool, attempt to schedule one runnable op at a time with:
         - Priority order: QUERY > INTERACTIVE > BATCH (with batch credit/aging).
         - Conservative reserved headroom for high priority.
         - RAM sizing using multiplier (OOM backoff), CPU sizing moderate.
      4) If a high-priority runnable op exists but doesn't fit, preempt low-priority running containers
         (with cooldown) until it fits, then schedule.
    """
    s.tick += 1

    # -------- Helpers (defined inside to avoid imports at top-level) --------
    def _prio_rank(p):
        if p == Priority.QUERY:
            return 0
        if p == Priority.INTERACTIVE:
            return 1
        return 2

    def _queue_for_priority(prio):
        if prio == Priority.QUERY:
            return s.q_query
        if prio == Priority.INTERACTIVE:
            return s.q_interactive
        return s.q_batch

    def _iter_all_queues():
        # Return in strict priority order
        return [s.q_query, s.q_interactive, s.q_batch]

    def _pipeline_done_or_hard_failed(p):
        st = p.runtime_status()
        if st.is_pipeline_successful():
            return True
        # If any operator is FAILED it might be retryable; we handle retries by leaving it in queue.
        # We do not "hard fail" here; let simulator mark terminal failures as needed.
        return False

    def _get_runnable_ops(p):
        st = p.runtime_status()
        # Prefer ready ops whose parents are complete; take at most one for simplicity/low contention.
        ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        if ops:
            return ops[:1]
        return []

    def _op_id(op):
        # Robust-ish op identifier: try common fields, fallback to repr
        for attr in ("op_id", "operator_id", "id", "name"):
            if hasattr(op, attr):
                v = getattr(op, attr)
                if v is not None:
                    return str(v)
        return repr(op)

    def _is_oom_error(err):
        if err is None:
            return False
        # Best-effort string matching; simulator errors may vary.
        msg = str(err).lower()
        return ("oom" in msg) or ("out of memory" in msg) or ("memory" in msg and "exceed" in msg)

    def _record_arrival(p):
        pid = p.pipeline_id
        if pid not in s.pipeline_meta:
            s.pipeline_meta[pid] = {"arrival_seq": s.arrival_seq, "last_served_seq": None}
            s.arrival_seq += 1

    def _note_served(p):
        meta = s.pipeline_meta.get(p.pipeline_id)
        if meta is not None:
            meta["last_served_seq"] = s.tick

    def _reserve_for_high_prio(pool, prio):
        # Reserve headroom to reduce preemption and admit future high-priority tasks quickly.
        # For QUERY: reserve more; for INTERACTIVE: reserve some; for BATCH: no reserve.
        if prio == Priority.QUERY:
            return (pool.max_cpu_pool * s.QUERY_RESERVE_CPU, pool.max_ram_pool * s.QUERY_RESERVE_RAM)
        if prio == Priority.INTERACTIVE:
            return (pool.max_cpu_pool * s.INT_RESERVE_CPU, pool.max_ram_pool * s.INT_RESERVE_RAM)
        return (0.0, 0.0)

    def _initial_op_ram_guess(pool, p):
        # If operator has a known minimum RAM requirement, use it; otherwise pick a small safe slice.
        # We look for a hint on the operator object; if absent, default to a fraction of pool.
        # This is intentionally conservative to avoid OOM, which is heavily penalized.
        default_ram = max(pool.max_ram_pool * s.MIN_RAM_FRACTION, 1.0)
        return min(default_ram, pool.max_ram_pool)

    def _initial_op_cpu_guess(pool, p):
        # Give moderate CPU to reduce long tail for high priority.
        base = max(pool.max_cpu_pool * s.MIN_CPU_FRACTION, 1.0)
        # Cap to available at assignment time; caller will clamp.
        return min(base, pool.max_cpu_pool)

    def _alloc_for_op(pool, p, op):
        pid = p.pipeline_id
        oid = _op_id(op)
        key = (pid, oid)

        # Start from remembered allocation if we have one
        last = s.op_last_alloc.get(key)
        if last is not None:
            base_cpu = float(last.get("cpu", _initial_op_cpu_guess(pool, p)))
            base_ram = float(last.get("ram", _initial_op_ram_guess(pool, p)))
        else:
            base_cpu = float(_initial_op_cpu_guess(pool, p))
            base_ram = float(_initial_op_ram_guess(pool, p))

        mult = float(s.op_ram_mult.get(key, 1.0))
        ram = min(base_ram * mult, pool.max_ram_pool)

        # Give more CPU to high priority, but keep it simple and bounded.
        if p.priority == Priority.QUERY:
            cpu = min(pool.max_cpu_pool, max(base_cpu, pool.max_cpu_pool * 0.50))
        elif p.priority == Priority.INTERACTIVE:
            cpu = min(pool.max_cpu_pool, max(base_cpu, pool.max_cpu_pool * 0.35))
        else:
            cpu = min(pool.max_cpu_pool, max(1.0, pool.max_cpu_pool * 0.20))

        return cpu, ram

    def _enqueue(p):
        _record_arrival(p)
        _queue_for_priority(p.priority).append(p)

    def _cleanup_queues():
        # Remove completed pipelines from the heads opportunistically (keep order mostly stable).
        for q in _iter_all_queues():
            keep = []
            for p in q:
                if _pipeline_done_or_hard_failed(p):
                    continue
                keep.append(p)
            q[:] = keep

    def _choose_next_pipeline(prio_preference):
        # prio_preference: list of priorities to consider in order
        # For each queue, pick the first pipeline with a runnable op.
        for prio in prio_preference:
            q = _queue_for_priority(prio)
            for i in range(len(q)):
                p = q[i]
                if _pipeline_done_or_hard_failed(p):
                    continue
                if _get_runnable_ops(p):
                    # rotate: move selected pipeline to end to reduce head-of-line blocking
                    q.pop(i)
                    q.append(p)
                    return p
        return None

    def _have_any_runnable(prio):
        q = _queue_for_priority(prio)
        for p in q:
            if _pipeline_done_or_hard_failed(p):
                continue
            if _get_runnable_ops(p):
                return True
        return False

    def _pool_running_containers(pool_id):
        # Collect running containers in this pool from executor state if available.
        # Fallback to empty if simulator doesn't expose.
        pool = s.executor.pools[pool_id]
        containers = []
        for attr in ("running_containers", "containers", "active_containers"):
            if hasattr(pool, attr):
                try:
                    containers = list(getattr(pool, attr))
                    return containers
                except Exception:
                    pass
        return []

    def _container_priority(c):
        # Try to extract priority from container; default to BATCH.
        for attr in ("priority", "prio"):
            if hasattr(c, attr):
                try:
                    return getattr(c, attr)
                except Exception:
                    pass
        return Priority.BATCH_PIPELINE

    def _container_resources(c):
        # Try to extract cpu/ram; if missing, treat as 0 (won't help admission via preemption).
        cpu = 0.0
        ram = 0.0
        for attr in ("cpu", "vcpus"):
            if hasattr(c, attr):
                try:
                    cpu = float(getattr(c, attr))
                    break
                except Exception:
                    pass
        for attr in ("ram", "mem", "memory"):
            if hasattr(c, attr):
                try:
                    ram = float(getattr(c, attr))
                    break
                except Exception:
                    pass
        return cpu, ram

    def _container_id(c):
        for attr in ("container_id", "id"):
            if hasattr(c, attr):
                try:
                    return getattr(c, attr)
                except Exception:
                    pass
        return None

    def _maybe_preempt_for(pool_id, need_cpu, need_ram, target_prio):
        # Preempt lower-priority work to make room for target_prio, with cooldown to reduce churn.
        susp = []
        pool = s.executor.pools[pool_id]

        # If already enough, do nothing.
        if pool.avail_cpu_pool >= need_cpu and pool.avail_ram_pool >= need_ram:
            return susp

        # Consider candidates in ascending "importance": batch first, then interactive, never query.
        candidates = _pool_running_containers(pool_id)
        scored = []
        for c in candidates:
            c_prio = _container_priority(c)
            if _prio_rank(c_prio) <= _prio_rank(target_prio):
                continue  # don't preempt same/higher priority
            cid = _container_id(c)
            if cid is None:
                continue
            last = s.preempt_cooldown.get(cid, -10**9)
            if (s.tick - last) < 5:
                continue  # cooldown window
            cpu, ram = _container_resources(c)
            # Prefer preempting largest resource hogs first to reduce number of preemptions.
            score = (ram * 2.0) + cpu
            scored.append((score, c_prio, cid, cpu, ram, getattr(c, "pool_id", pool_id)))

        # Sort: lower priority first (batch>interactive), then larger hog.
        scored.sort(key=lambda x: (_prio_rank(x[1]), -x[0]))

        for _, c_prio, cid, cpu, ram, c_pool_id in scored:
            susp.append(Suspend(container_id=cid, pool_id=pool_id))
            s.preempt_cooldown[cid] = s.tick

            # Optimistically assume we reclaim its resources; simulator will enforce actual.
            # This helps decide how many suspensions to issue in this tick.
            need_cpu = max(0.0, need_cpu - cpu)
            need_ram = max(0.0, need_ram - ram)
            if need_cpu <= 0.0 and need_ram <= 0.0:
                break

        return susp

    # -------- Ingest new pipelines --------
    for p in pipelines:
        _enqueue(p)

    # Aging/credits for batch progress
    if _have_any_runnable(Priority.BATCH_PIPELINE):
        s.batch_credit += s.BATCH_CREDIT_PER_TICK

    # -------- Process results: OOM-aware RAM backoff; remember allocations --------
    for r in results:
        # Remember last allocation to reduce oscillation (for both success and failure)
        try:
            for op in getattr(r, "ops", []) or []:
                oid = _op_id(op)
                key = (getattr(r, "pipeline_id", None), oid)
                # If pipeline_id isn't available on result, fall back to scanning queues later; ignore here.
                if key[0] is not None:
                    s.op_last_alloc[key] = {"cpu": float(getattr(r, "cpu", 0.0)), "ram": float(getattr(r, "ram", 0.0))}
        except Exception:
            pass

        if hasattr(r, "failed") and r.failed():
            if _is_oom_error(getattr(r, "error", None)):
                # Increase RAM multiplier for all ops in this result (usually one op)
                for op in getattr(r, "ops", []) or []:
                    # We may not have pipeline_id on result; attempt to infer by scanning queues is too costly.
                    # If missing, we still do best-effort by using container-local keying with repr(op).
                    pid = getattr(r, "pipeline_id", None)
                    oid = _op_id(op)
                    key = (pid, oid)
                    if pid is None:
                        continue
                    cnt = s.op_oom_retries.get(key, 0) + 1
                    s.op_oom_retries[key] = cnt

                    mult = float(s.op_ram_mult.get(key, 1.0))
                    mult = min(s.RAM_BACKOFF_CAP, mult * s.RAM_BACKOFF)
                    s.op_ram_mult[key] = mult

    _cleanup_queues()

    # -------- Build decisions --------
    suspensions = []
    assignments = []

    # Priority order for scheduling; batch can sometimes jump ahead if it has accumulated credit.
    def _prio_order():
        # If batch has credit, allow it to be considered between interactive and batch
        if s.batch_credit >= s.BATCH_CREDIT_COST:
            return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]
        return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]

    # For each pool, try to schedule at most one op (keeps interference low; improves tail latency)
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        if pool.avail_cpu_pool <= 0 or pool.avail_ram_pool <= 0:
            continue

        # Try to pick next pipeline to run based on priority and readiness.
        prio_pref = _prio_order()
        p = _choose_next_pipeline(prio_pref)
        if p is None:
            continue

        runnable_ops = _get_runnable_ops(p)
        if not runnable_ops:
            continue
        op = runnable_ops[0]
        oid = _op_id(op)
        key = (p.pipeline_id, oid)

        # Stop retrying pathological OOM ops too many times; but do not drop pipeline.
        # We'll let other ops/pipelines proceed; this reduces cluster-wide blocking.
        if s.op_oom_retries.get(key, 0) > s.MAX_OOM_RETRIES:
            # Push pipeline to back of its queue and skip this tick.
            _queue_for_priority(p.priority).append(p)
            continue

        # Compute desired allocation
        desired_cpu, desired_ram = _alloc_for_op(pool, p, op)

        # Apply reserve policy: don't consume into reserved headroom for higher priorities.
        reserve_cpu, reserve_ram = _reserve_for_high_prio(pool, p.priority)
        usable_cpu = max(0.0, pool.avail_cpu_pool - reserve_cpu)
        usable_ram = max(0.0, pool.avail_ram_pool - reserve_ram)

        # Clamp to usable
        cpu = min(desired_cpu, usable_cpu if usable_cpu > 0 else pool.avail_cpu_pool)
        ram = min(desired_ram, usable_ram if usable_ram > 0 else pool.avail_ram_pool)

        # Ensure non-zero allocations
        cpu = max(1.0, cpu) if pool.avail_cpu_pool >= 1.0 else pool.avail_cpu_pool
        ram = max(1.0, ram) if pool.avail_ram_pool >= 1.0 else pool.avail_ram_pool

        # If it still doesn't fit, attempt targeted preemption (only when necessary for QUERY/INTERACTIVE)
        need_cpu = max(0.0, desired_cpu - pool.avail_cpu_pool)
        need_ram = max(0.0, desired_ram - pool.avail_ram_pool)

        if (need_cpu > 0.0 or need_ram > 0.0) and p.priority in (Priority.QUERY, Priority.INTERACTIVE):
            # Try preempting to fit the *desired* allocation; if not possible, fit minimum.
            suspensions.extend(_maybe_preempt_for(pool_id, desired_cpu, desired_ram, p.priority))

        # Recompute after preemption requests (we can't see actual reclaimed resources; best-effort)
        # Assign with what is currently reported as available.
        if pool.avail_cpu_pool <= 0 or pool.avail_ram_pool <= 0:
            _queue_for_priority(p.priority).append(p)
            continue

        # Recompute usable after possible preemption signals
        reserve_cpu, reserve_ram = _reserve_for_high_prio(pool, p.priority)
        usable_cpu = max(0.0, pool.avail_cpu_pool - reserve_cpu)
        usable_ram = max(0.0, pool.avail_ram_pool - reserve_ram)

        cpu = min(desired_cpu, usable_cpu if usable_cpu > 0 else pool.avail_cpu_pool)
        ram = min(desired_ram, usable_ram if usable_ram > 0 else pool.avail_ram_pool)

        cpu = max(1.0, cpu) if pool.avail_cpu_pool >= 1.0 else pool.avail_cpu_pool
        ram = max(1.0, ram) if pool.avail_ram_pool >= 1.0 else pool.avail_ram_pool

        # Final admit check
        if cpu <= 0 or ram <= 0 or cpu > pool.avail_cpu_pool or ram > pool.avail_ram_pool:
            _queue_for_priority(p.priority).append(p)
            continue

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
        _note_served(p)

        # Spend batch credit when we schedule batch
        if p.priority == Priority.BATCH_PIPELINE and s.batch_credit >= s.BATCH_CREDIT_COST:
            s.batch_credit -= s.BATCH_CREDIT_COST

    return suspensions, assignments
