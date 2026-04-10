# policy_key: scheduler_est_034
# reasoning_effort: high
# model: gpt-5.2-2025-12-11
# llm_cost: 0.127154
# generation_seconds: 132.52
# generated_at: 2026-03-31T19:26:09.463448
def _op_key(op):
    """Best-effort stable key for an operator across retries."""
    for attr in ("op_id", "operator_id", "id", "name"):
        if hasattr(op, attr):
            try:
                v = getattr(op, attr)
                # Avoid callable attributes or overly large structures
                if not callable(v):
                    return (attr, v)
            except Exception:
                pass
    return ("py_id", id(op))


def _is_oom_error(err):
    if err is None:
        return False
    try:
        s = str(err).lower()
    except Exception:
        return False
    return ("oom" in s) or ("out of memory" in s) or ("out-of-memory" in s) or ("cuda oom" in s)


def _get_est_mem_gb(op):
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


def _pipeline_should_drop(s, pipeline):
    """Drop completed pipelines, and also drop pipelines that contain any 'give-up' failed ops."""
    status = pipeline.runtime_status()
    if status.is_pipeline_successful():
        return True

    # If any FAILED op is marked as give-up, we drop the whole pipeline (avoid infinite retry loops).
    try:
        failed_ops = status.get_ops([OperatorState.FAILED], require_parents_complete=False)
    except Exception:
        failed_ops = []
    for op in failed_ops or []:
        k = _op_key(op)
        if s.op_give_up.get(k, False):
            return True

    return False


def _queue_has_live_work(s, q):
    """True if the queue contains any pipeline that isn't completed/dropped."""
    # We don't want to aggressively traverse deep DAG state; just ensure there exists a live pipeline.
    # We'll lazily clean as we pop/rotate elsewhere.
    for p in q:
        if not _pipeline_should_drop(s, p):
            return True
    return False


def _pick_next_runnable(s, priorities, scheduled_pipelines_this_tick):
    """
    Round-robin selection across given priorities.
    Returns (pipeline, op) or (None, None) if nothing runnable.
    """
    for pr in priorities:
        q = s.queues.get(pr)
        if not q:
            continue

        # Try at most len(q) rotations to find a runnable op.
        n = len(q)
        for _ in range(n):
            pipeline = q.popleft()

            # Pipeline may be repeated in queues due to external references; keep unique by pipeline_id.
            pid = getattr(pipeline, "pipeline_id", None)
            if pid in scheduled_pipelines_this_tick:
                q.append(pipeline)
                continue

            if _pipeline_should_drop(s, pipeline):
                # Remove from presence set so future arrivals can be queued.
                if pid is not None:
                    s.present.discard(pid)
                continue

            status = pipeline.runtime_status()

            # Choose a single runnable op whose parents are complete.
            try:
                ops = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
            except Exception:
                ops = []

            op_to_run = None
            for op in ops or []:
                k = _op_key(op)
                if s.op_give_up.get(k, False):
                    continue
                op_to_run = op
                break

            # Keep pipeline in queue for future ops regardless of runnable now.
            q.append(pipeline)

            if op_to_run is None:
                continue

            return pipeline, op_to_run

    return None, None


@register_scheduler_init(key="scheduler_est_034")
def scheduler_est_034_init(s):
    """Priority-aware, estimate-driven scheduling with headroom reservation.

    Small-but-impactful improvements over naive FIFO:
      - Priority queues: QUERY > INTERACTIVE > BATCH
      - Memory sizing from op.estimate.mem_peak_gb with safety margin
      - OOM-aware retries: bump RAM multiplier on OOM failures
      - Headroom reservation: prevent batch from consuming all resources when high-priority work exists
      - Optional multi-pool bias: pool 0 favors high-priority work; other pools favor batch
    """
    from collections import deque

    s.tick = 0

    # Per-priority FIFO queues (round-robin within each priority).
    s.queues = {
        Priority.QUERY: deque(),
        Priority.INTERACTIVE: deque(),
        Priority.BATCH_PIPELINE: deque(),
    }
    s.present = set()  # pipeline_id set (avoid enqueuing duplicates)

    # Retry / sizing state keyed by operator identity.
    s.op_ram_mult = {}       # key -> multiplier applied to estimated mem
    s.op_fail_count = {}     # key -> total failures observed
    s.op_give_up = {}        # key -> bool (non-retryable or exceeded retries)
    s.op_last_error = {}     # key -> str/err

    # Policy knobs (conservative defaults).
    s.max_retries = 3

    # RAM safety multipliers by priority (favor latency by avoiding OOM retries).
    s.base_ram_safety = {
        Priority.QUERY: 1.35,
        Priority.INTERACTIVE: 1.25,
        Priority.BATCH_PIPELINE: 1.15,
    }

    # CPU share heuristics (as a fraction of pool max, then capped by avail).
    s.cpu_frac = {
        Priority.QUERY: 0.75,
        Priority.INTERACTIVE: 0.55,
        Priority.BATCH_PIPELINE: 0.35,
    }

    # When estimate is missing, fall back to a fraction of pool RAM.
    s.default_ram_frac = {
        Priority.QUERY: 0.50,
        Priority.INTERACTIVE: 0.40,
        Priority.BATCH_PIPELINE: 0.25,
    }

    # Keep headroom free when high-priority work exists (single-pool and batch pools).
    s.reserved_hi_cpu_frac = 0.30
    s.reserved_hi_ram_frac = 0.30

    s.min_cpu = 1
    s.min_ram_gb = 0.1

    # Prefer scheduling high-priority work in pool 0 (if it exists).
    s.latency_pool_id = 0


@register_scheduler(key="scheduler_est_034")
def scheduler_est_034(s, results: List[ExecutionResult], pipelines: List[Pipeline]) -> Tuple[List[Suspend], List[Assignment]]:
    suspensions = []
    assignments = []

    s.tick += 1

    # --- Ingest new pipelines (de-dup by pipeline_id) ---
    for p in pipelines:
        pid = getattr(p, "pipeline_id", None)
        if pid is None:
            # If pipeline_id is missing, just enqueue it (can't dedup reliably).
            s.queues[p.priority].append(p)
            continue
        if pid in s.present:
            continue
        s.present.add(pid)
        s.queues[p.priority].append(p)

    # --- Update retry / sizing state from execution results ---
    for r in results or []:
        if not r.failed():
            continue

        err = getattr(r, "error", None)
        oom = _is_oom_error(err)

        for op in getattr(r, "ops", []) or []:
            k = _op_key(op)
            s.op_last_error[k] = err
            s.op_fail_count[k] = s.op_fail_count.get(k, 0) + 1

            if oom:
                # Increase RAM multiplier aggressively to avoid repeated OOMs.
                cur = float(s.op_ram_mult.get(k, 1.0))
                # First OOM: 1.5x, then 2x thereafter (capped).
                bump = 1.5 if s.op_fail_count[k] == 1 else 2.0
                s.op_ram_mult[k] = min(cur * bump, 16.0)
                # Give up only if too many retries.
                if s.op_fail_count[k] > s.max_retries:
                    s.op_give_up[k] = True
            else:
                # Non-OOM failures: treat as non-retryable (avoid churn).
                s.op_give_up[k] = True

    # If nothing changed, early exit.
    if not pipelines and not results:
        return suspensions, assignments

    # --- Helper: compute reservation (only relevant if high-priority backlog exists) ---
    hi_backlog = _queue_has_live_work(s, s.queues[Priority.QUERY]) or _queue_has_live_work(s, s.queues[Priority.INTERACTIVE])

    scheduled_pipelines_this_tick = set()

    def _desired_ram_cpu(pool, priority, op):
        # RAM from estimate if present, else fallback to fraction of pool RAM.
        est_mem = _get_est_mem_gb(op)
        base = float(s.base_ram_safety.get(priority, 1.2))
        mult = float(s.op_ram_mult.get(_op_key(op), 1.0))

        if est_mem is not None:
            desired_ram = est_mem * base * mult
            # Small additive slack to reduce edge OOMs (bounded).
            desired_ram += min(0.25, 0.02 * float(pool.max_ram_pool))
        else:
            desired_ram = float(pool.max_ram_pool) * float(s.default_ram_frac.get(priority, 0.30)) * mult

        desired_ram = max(s.min_ram_gb, min(float(pool.max_ram_pool), desired_ram))

        # CPU from pool max and priority fraction.
        cpu_frac = float(s.cpu_frac.get(priority, 0.35))
        desired_cpu = int(round(float(pool.max_cpu_pool) * cpu_frac))
        desired_cpu = max(s.min_cpu, min(int(pool.max_cpu_pool), desired_cpu))

        return desired_ram, desired_cpu, est_mem

    def _try_assign_one(pool_id, pool, priorities, reserve_for_hi):
        nonlocal assignments, scheduled_pipelines_this_tick

        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)
        if avail_cpu < s.min_cpu or avail_ram < s.min_ram_gb:
            return 0.0, 0.0, False

        # Apply reservation only for batch-like scheduling passes.
        eff_cpu = avail_cpu
        eff_ram = avail_ram
        if reserve_for_hi:
            eff_cpu = max(0.0, avail_cpu - float(pool.max_cpu_pool) * float(s.reserved_hi_cpu_frac))
            eff_ram = max(0.0, avail_ram - float(pool.max_ram_pool) * float(s.reserved_hi_ram_frac))

        if eff_cpu < s.min_cpu or eff_ram < s.min_ram_gb:
            return 0.0, 0.0, False

        pipeline, op = _pick_next_runnable(s, priorities, scheduled_pipelines_this_tick)
        if pipeline is None or op is None:
            return 0.0, 0.0, False

        priority = pipeline.priority
        desired_ram, desired_cpu, est_mem = _desired_ram_cpu(pool, priority, op)

        # If we can't meet desired resources, for estimated ops try a minimal-safe attempt;
        # otherwise, skip to avoid predictable OOM/retries.
        cpu = min(float(desired_cpu), eff_cpu)
        if cpu < s.min_cpu:
            return 0.0, 0.0, False

        if desired_ram <= eff_ram:
            ram = desired_ram
        else:
            # If we have an estimate, allow running with slightly-below-desired if still >= estimate*1.05
            # (better to progress than stall), else skip.
            if est_mem is not None and eff_ram >= max(s.min_ram_gb, est_mem * 1.05):
                ram = eff_ram
            else:
                return 0.0, 0.0, False

        assignments.append(
            Assignment(
                ops=[op],
                cpu=cpu,
                ram=ram,
                priority=priority,
                pool_id=pool_id,
                pipeline_id=pipeline.pipeline_id,
            )
        )
        scheduled_pipelines_this_tick.add(pipeline.pipeline_id)
        return cpu, ram, True

    # --- PASS 1: schedule high-priority work first (QUERY then INTERACTIVE) ---
    # Bias pool 0 for latency work if multiple pools exist, but allow using other pools too.
    pool_order = list(range(s.executor.num_pools))
    if s.executor.num_pools > 1 and s.latency_pool_id in pool_order:
        pool_order.remove(s.latency_pool_id)
        pool_order = [s.latency_pool_id] + pool_order

    for pool_id in pool_order:
        pool = s.executor.pools[pool_id]
        while True:
            used_cpu, used_ram, ok = _try_assign_one(
                pool_id=pool_id,
                pool=pool,
                priorities=[Priority.QUERY, Priority.INTERACTIVE],
                reserve_for_hi=False,
            )
            if not ok:
                break
            # Locally account to allow multiple assignments in same tick.
            pool.avail_cpu_pool -= used_cpu
            pool.avail_ram_pool -= used_ram

    # Recompute high-priority backlog after pass 1 (might still exist).
    hi_backlog_after = _queue_has_live_work(s, s.queues[Priority.QUERY]) or _queue_has_live_work(s, s.queues[Priority.INTERACTIVE])

    # --- PASS 2: schedule batch work with headroom reservation if high-priority backlog exists ---
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]

        # If multiple pools exist, keep pool 0 primarily for latency work; only run batch there if no hi backlog.
        if s.executor.num_pools > 1 and pool_id == s.latency_pool_id and hi_backlog_after:
            continue

        # If any high-priority backlog exists, reserve headroom when scheduling batch.
        reserve = bool(hi_backlog_after or hi_backlog)

        while True:
            used_cpu, used_ram, ok = _try_assign_one(
                pool_id=pool_id,
                pool=pool,
                priorities=[Priority.BATCH_PIPELINE],
                reserve_for_hi=reserve,
            )
            if not ok:
                break
            pool.avail_cpu_pool -= used_cpu
            pool.avail_ram_pool -= used_ram

    return suspensions, assignments
