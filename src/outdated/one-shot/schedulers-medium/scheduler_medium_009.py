# policy_key: scheduler_medium_009
# reasoning_effort: medium
# model: gpt-5.2-2025-12-11
# llm_cost: 0.055369
# generation_seconds: 91.54
# generated_at: 2026-03-12T22:04:33.895084
@register_scheduler_init(key="scheduler_medium_009")
def scheduler_medium_009_init(s):
    """
    Priority-aware, headroom-preserving round-robin scheduler with simple OOM backoff.

    Improvements over naive FIFO:
      1) Separate queues per priority; prefer QUERY/INTERACTIVE over BATCH.
      2) Prevent BATCH from consuming the entire pool when high-priority work exists
         (leave CPU/RAM headroom).
      3) On OOM-like failures, retry the same operator with increased RAM (bounded retries).
      4) Basic round-robin within each priority to avoid a single pipeline monopolizing.
      5) Light multi-pool bias: pool 0 is preferred for high-priority when available.
    """
    from collections import deque

    # Per-priority waiting queues of Pipeline objects
    s.q_query = deque()
    s.q_interactive = deque()
    s.q_batch = deque()

    # A simple logical clock for aging / bookkeeping (ticks = scheduler invocations)
    s.clock = 0

    # Operator "profiles" keyed by operator object id(op):
    #   ram: last good (or next attempt) RAM
    #   cpu: last good CPU
    #   oom_retries: number of OOM-like retries so far
    #   fatal: if True, stop attempting this operator
    s.op_profiles = {}

    # Config knobs (kept conservative to avoid breaking behavior)
    s.max_oom_retries = 3

    # Burst control: after too many high-priority picks, allow a batch pick if available.
    s.high_burst_limit = 6
    s.high_burst_count = 0


@register_scheduler(key="scheduler_medium_009")
def scheduler_medium_009_scheduler(s, results, pipelines):
    """
    Scheduling step:
      - Enqueue new pipelines into priority queues.
      - Update per-operator profiles from execution results (OOM backoff).
      - For each pool, pick one assignable operator (highest priority first),
        size it, and assign it while preserving headroom for high priority.
    """
    from collections import deque

    def _is_oom_error(err):
        if not err:
            return False
        msg = str(err).lower()
        return ("oom" in msg) or ("out of memory" in msg) or ("killed" in msg and "memory" in msg)

    def _op_id(op):
        # Prefer stable ids if present, otherwise fall back to object identity.
        for attr in ("op_id", "operator_id", "id", "uid", "name"):
            if hasattr(op, attr):
                try:
                    v = getattr(op, attr)
                    # If it's callable (rare), skip.
                    if callable(v):
                        continue
                    # Make dict keys stable-ish.
                    return (attr, str(v))
                except Exception:
                    pass
        return ("pyid", str(id(op)))

    def _get_min_ram(op):
        # Best-effort extraction of a minimum RAM hint from operator object.
        for attr in ("min_ram", "ram_min", "min_memory", "memory_min", "minimum_ram"):
            if hasattr(op, attr):
                try:
                    v = getattr(op, attr)
                    if v is None:
                        continue
                    v = float(v)
                    if v > 0:
                        return v
                except Exception:
                    pass
        return None

    def _get_profile(op):
        oid = _op_id(op)
        prof = s.op_profiles.get(oid)
        if prof is None:
            prof = {"ram": None, "cpu": None, "oom_retries": 0, "fatal": False}
            s.op_profiles[oid] = prof
        return prof

    def _suggest_ram(op, pool, priority, avail_ram):
        prof = _get_profile(op)
        if prof.get("fatal"):
            return None

        # If we already have a known good/next RAM for this op, start from it.
        if prof.get("ram") is not None:
            base = float(prof["ram"])
        else:
            min_ram = _get_min_ram(op)
            # Default fractions: keep small for batch, larger for interactive/query to reduce tail latency.
            frac = 0.25 if priority in (Priority.QUERY, Priority.INTERACTIVE) else 0.125
            base = float(pool.max_ram_pool) * frac
            if min_ram is not None:
                # Slight safety margin over the minimum.
                base = max(base, float(min_ram) * 1.2)

        # Ensure positive and not exceeding what is currently available/pool max.
        base = max(base, 0.0)
        base = min(base, float(pool.max_ram_pool))
        base = min(base, float(avail_ram))
        # If it's effectively zero, we can't schedule.
        if base <= 0:
            return None
        return base

    def _suggest_cpu(op, pool, priority, avail_cpu):
        prof = _get_profile(op)
        if prof.get("fatal"):
            return None

        if prof.get("cpu") is not None:
            base = float(prof["cpu"])
        else:
            # Conservative initial sizing:
            # - Give high-priority a bigger slice to reduce latency.
            # - Keep batch smaller to reduce interference and preserve headroom.
            frac = 0.60 if priority in (Priority.QUERY, Priority.INTERACTIVE) else 0.30
            base = float(pool.max_cpu_pool) * frac
            base = max(base, 1.0)

        base = min(base, float(pool.max_cpu_pool))
        base = min(base, float(avail_cpu))
        if base <= 0:
            return None
        return base

    def _enqueue_pipeline(p):
        if p.priority == Priority.QUERY:
            s.q_query.append(p)
        elif p.priority == Priority.INTERACTIVE:
            s.q_interactive.append(p)
        else:
            s.q_batch.append(p)

    def _queue_has_high_pri():
        return (len(s.q_query) > 0) or (len(s.q_interactive) > 0)

    def _pick_from_queue(q, pool_id):
        """
        Round-robin pick: rotate through the queue looking for an assignable op.
        Returns (pipeline, op_list) or (None, None).
        """
        if not q:
            return None, None

        # Try at most one full rotation to avoid infinite loops.
        for _ in range(len(q)):
            p = q.popleft()
            status = p.runtime_status()

            # Drop completed pipelines.
            if status.is_pipeline_successful():
                continue

            # Find one assignable operator whose parents are complete.
            op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
            if not op_list:
                # Not ready; keep it in the queue.
                q.append(p)
                continue

            # If the operator is marked fatal in our profile, don't schedule it; move on.
            prof = _get_profile(op_list[0])
            if prof.get("fatal"):
                # Keep pipeline in queue to allow other ops, but avoid this op.
                q.append(p)
                continue

            # Found work: requeue pipeline to the end (RR) and return its op.
            q.append(p)
            return p, op_list

        return None, None

    def _pick_pipeline_for_pool(pool_id):
        """
        Priority + fairness selection:
          - Prefer QUERY, then INTERACTIVE, then BATCH
          - After a burst of high-priority picks, allow a batch pick if any batch exists.
        """
        allow_batch_due_to_burst = (s.high_burst_count >= s.high_burst_limit) and (len(s.q_batch) > 0)

        # If burst reached, try batch first once.
        if allow_batch_due_to_burst:
            p, ops = _pick_from_queue(s.q_batch, pool_id)
            if p is not None:
                s.high_burst_count = 0
                return p, ops

        # Normal strict-ish priority order.
        p, ops = _pick_from_queue(s.q_query, pool_id)
        if p is not None:
            s.high_burst_count += 1
            return p, ops

        p, ops = _pick_from_queue(s.q_interactive, pool_id)
        if p is not None:
            s.high_burst_count += 1
            return p, ops

        p, ops = _pick_from_queue(s.q_batch, pool_id)
        if p is not None:
            # Batch does not contribute to high burst.
            return p, ops

        return None, None

    # ---- Update logical clock and enqueue arrivals ----
    s.clock += 1
    for p in pipelines:
        _enqueue_pipeline(p)

    # ---- Process results: learn resource sizes and apply OOM backoff ----
    for r in results:
        # r.ops is a list of operators that were executed in that container.
        for op in getattr(r, "ops", []) or []:
            prof = _get_profile(op)

            if r.failed():
                if _is_oom_error(getattr(r, "error", None)):
                    prof["oom_retries"] = int(prof.get("oom_retries", 0)) + 1
                    if prof["oom_retries"] > s.max_oom_retries:
                        # Stop retrying this op; it's unlikely to fit.
                        prof["fatal"] = True
                        continue

                    # Increase RAM aggressively to escape repeated OOM quickly.
                    # Use last attempt RAM if present; otherwise use the recorded allocation.
                    last_ram = prof.get("ram")
                    attempt_ram = float(last_ram) if last_ram is not None else float(getattr(r, "ram", 0.0) or 0.0)

                    # If attempt_ram is unknown/zero, use a conservative jump based on pool max if available.
                    pool_max_ram = None
                    try:
                        pool_max_ram = float(s.executor.pools[r.pool_id].max_ram_pool)
                    except Exception:
                        pool_max_ram = None

                    if attempt_ram <= 0 and pool_max_ram is not None:
                        next_ram = max(pool_max_ram * 0.25, pool_max_ram * 0.10)
                    else:
                        bump = (pool_max_ram * 0.10) if pool_max_ram is not None else 0.0
                        next_ram = max(attempt_ram * 2.0, attempt_ram + bump)

                    # Cap at pool max if we can.
                    if pool_max_ram is not None:
                        next_ram = min(next_ram, pool_max_ram)

                    prof["ram"] = next_ram

                    # Keep CPU the same as last attempt to reduce churn; if absent, keep recorded.
                    if prof.get("cpu") is None:
                        try:
                            prof["cpu"] = float(getattr(r, "cpu", None) or 1.0)
                        except Exception:
                            prof["cpu"] = 1.0
                else:
                    # Non-OOM failure: mark fatal to avoid infinite retries.
                    prof["fatal"] = True
            else:
                # Success: record allocations as "known good".
                try:
                    prof["ram"] = float(getattr(r, "ram", None)) if getattr(r, "ram", None) is not None else prof.get("ram")
                except Exception:
                    pass
                try:
                    prof["cpu"] = float(getattr(r, "cpu", None)) if getattr(r, "cpu", None) is not None else prof.get("cpu")
                except Exception:
                    pass
                prof["oom_retries"] = 0
                prof["fatal"] = False

    # Early exit if no new info and nothing waiting.
    if not pipelines and not results and (len(s.q_query) + len(s.q_interactive) + len(s.q_batch) == 0):
        return [], []

    suspensions = []  # No preemption in this version (keeps behavior safe/simple).
    assignments = []

    # Pool bias: prefer scheduling high-priority on pool 0 if multiple pools exist.
    # We'll iterate pools in a slightly biased order: [0, 1, 2, ...]
    pool_order = list(range(s.executor.num_pools))
    if s.executor.num_pools > 1:
        pool_order = [0] + [i for i in range(1, s.executor.num_pools)]

    # Headroom policy:
    # If high-priority work exists, restrict batch allocations so batch can't fully occupy the pool.
    # This improves latency by reducing queueing and waiting for capacity.
    for pool_id in pool_order:
        pool = s.executor.pools[pool_id]
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)
        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # If there is high-priority waiting and we are considering scheduling batch, keep headroom.
        reserve_cpu = 0.0
        reserve_ram = 0.0
        if _queue_has_high_pri():
            reserve_cpu = max(1.0, float(pool.max_cpu_pool) * 0.20)
            reserve_ram = float(pool.max_ram_pool) * 0.20

        # Pick a pipeline/op for this pool.
        p, op_list = _pick_pipeline_for_pool(pool_id)
        if p is None or not op_list:
            continue

        priority = p.priority
        op = op_list[0]

        # Compute sizing.
        cpu_req = _suggest_cpu(op, pool, priority, avail_cpu)
        ram_req = _suggest_ram(op, pool, priority, avail_ram)
        if cpu_req is None or ram_req is None:
            continue

        # Apply headroom constraints only to batch.
        if priority == Priority.BATCH_PIPELINE and _queue_has_high_pri():
            cpu_req = min(cpu_req, max(0.0, avail_cpu - reserve_cpu))
            ram_req = min(ram_req, max(0.0, avail_ram - reserve_ram))

            # If headroom leaves no capacity, skip this pool for batch right now.
            if cpu_req <= 0 or ram_req <= 0:
                continue

            # Also avoid batch taking too much even beyond headroom.
            cpu_req = min(cpu_req, max(1.0, float(pool.max_cpu_pool) * 0.50))
            ram_req = min(ram_req, float(pool.max_ram_pool) * 0.50)

        # Final caps to available.
        cpu_req = min(cpu_req, avail_cpu)
        ram_req = min(ram_req, avail_ram)
        if cpu_req <= 0 or ram_req <= 0:
            continue

        assignments.append(
            Assignment(
                ops=op_list,
                cpu=cpu_req,
                ram=ram_req,
                priority=priority,
                pool_id=pool_id,
                pipeline_id=p.pipeline_id,
            )
        )

    return suspensions, assignments
