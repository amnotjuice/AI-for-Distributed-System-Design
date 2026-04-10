# policy_key: scheduler_est_011
# reasoning_effort: low
# model: gpt-5.2-2025-12-11
# llm_cost: 0.049511
# generation_seconds: 40.59
# generated_at: 2026-04-01T13:43:35.823896
@register_scheduler_init(key="scheduler_est_011")
def scheduler_est_011_init(s):
    """Priority-aware FIFO with conservative RAM sizing and simple OOM backoff.

    Small, incremental improvements over naive FIFO:
      1) Maintain separate queues per priority and always pick highest priority first.
      2) Use a lightweight per-op RAM hint (op.estimate.mem_peak_gb) with headroom.
      3) If an op fails with an OOM-like error, retry it with increased RAM (exponential backoff).
      4) Avoid grabbing the entire pool for one op; cap CPU per op by priority to reduce head-of-line blocking.
      5) If multiple pools exist, bias high-priority work to pool 0 to reduce interference.
    """
    s.wait_q = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }
    # Remember last attempted RAM per (pipeline_id, op_key) to implement OOM backoff.
    s.op_ram_backoff_gb = {}
    # Track "recently OOMed" ops to re-attempt with higher RAM.
    s.op_oom_count = {}

    # Tunables (kept intentionally simple)
    s.min_ram_gb = 1.0
    s.est_headroom_mult = 1.35
    s.oom_backoff_mult = 1.6
    s.max_oom_retries = 4

    # CPU caps by priority (fraction of pool's max_cpu_pool, also bounded by avail)
    s.cpu_cap_frac = {
        Priority.QUERY: 0.75,
        Priority.INTERACTIVE: 0.75,
        Priority.BATCH_PIPELINE: 0.50,
    }


@register_scheduler(key="scheduler_est_011")
def scheduler_est_011_scheduler(s, results, pipelines):
    """
    Scheduler step.

    Strategy:
      - Enqueue arriving pipelines into per-priority queues.
      - Process results: on OOM-like failures, increase RAM target for that op and allow retry.
      - For each pool, schedule at most one ready op (like the naive baseline), but:
          * choose highest priority first
          * size RAM conservatively using op.estimate.mem_peak_gb (+headroom) and OOM backoff memory
          * size CPU using a priority-based cap to avoid monopolizing a pool
          * if multiple pools: prefer pool 0 for high-priority pipelines
    """
    def _prio_order():
        return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]

    def _op_key(op):
        # Best-effort stable key across simulation objects.
        for attr in ("operator_id", "op_id", "id", "name"):
            if hasattr(op, attr):
                try:
                    v = getattr(op, attr)
                    # If callable (rare), call it; else use value.
                    v = v() if callable(v) else v
                    return str(v)
                except Exception:
                    pass
        # Fallback: representation
        try:
            return repr(op)
        except Exception:
            return str(type(op))

    def _is_oom_error(err):
        if err is None:
            return False
        msg = str(err).lower()
        # Keep heuristics broad; simulator error strings may vary.
        return ("oom" in msg) or ("out of memory" in msg) or ("memory" in msg and "exceed" in msg)

    def _desired_ram_gb(pipeline_id, op, pool):
        # 1) Start from conservative estimate if present, else minimal baseline.
        est = None
        if hasattr(op, "estimate") and op.estimate is not None and hasattr(op.estimate, "mem_peak_gb"):
            try:
                est = op.estimate.mem_peak_gb
            except Exception:
                est = None

        base = s.min_ram_gb
        if est is not None:
            try:
                if float(est) > 0:
                    base = max(base, float(est) * s.est_headroom_mult)
            except Exception:
                pass

        # 2) Apply OOM backoff if we've seen failures for this (pipeline, op).
        k = (pipeline_id, _op_key(op))
        if k in s.op_ram_backoff_gb:
            base = max(base, s.op_ram_backoff_gb[k])

        # 3) Bound by what the pool can provide.
        return min(pool.avail_ram_pool, pool.max_ram_pool, base)

    def _desired_cpu(pipeline_prio, pool):
        # Avoid taking the entire pool by default to reduce HOL blocking.
        cap_frac = s.cpu_cap_frac.get(pipeline_prio, 0.5)
        cap = max(1.0, pool.max_cpu_pool * cap_frac)
        return min(pool.avail_cpu_pool, cap)

    def _enqueue_pipeline(p):
        pr = p.priority
        if pr not in s.wait_q:
            # Unknown priority: treat as lowest.
            pr = Priority.BATCH_PIPELINE
        s.wait_q[pr].append(p)

    # Enqueue new pipelines
    for p in pipelines:
        _enqueue_pipeline(p)

    # Early exit if nothing to do
    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    # Process results to implement OOM backoff
    for r in results:
        try:
            if r.failed() and _is_oom_error(r.error):
                # We only get ops in the execution result; apply backoff to each.
                for op in getattr(r, "ops", []) or []:
                    # We may not know pipeline_id from result; use a generic key component if absent.
                    # But Assignment includes pipeline_id; simulator often ties retries via pipeline status.
                    # Here, we conservatively record against "unknown" pipeline id if needed.
                    # If op belongs to a pipeline, the retry key will still be found via op_key match if pipeline_id aligns.
                    # Prefer pipeline_id if present on result (not guaranteed by provided interface).
                    pid = getattr(r, "pipeline_id", None)
                    pid = pid if pid is not None else "unknown"
                    k = (pid, _op_key(op))

                    cnt = s.op_oom_count.get(k, 0) + 1
                    s.op_oom_count[k] = cnt

                    # Increase memory target based on what we just tried, otherwise use current allocation.
                    prev = s.op_ram_backoff_gb.get(k, None)
                    tried = getattr(r, "ram", None)
                    tried = float(tried) if tried is not None else None
                    if prev is None and tried is not None:
                        prev = tried

                    if prev is None:
                        prev = s.min_ram_gb

                    if cnt <= s.max_oom_retries:
                        s.op_ram_backoff_gb[k] = prev * s.oom_backoff_mult
        except Exception:
            # Never let bookkeeping crash scheduling.
            pass

    # Helper: pick next runnable op from queues, optionally preferring certain priorities
    def _pick_next_op(require_prios=None):
        # Returns: (pipeline, op) or (None, None)
        prios = require_prios if require_prios is not None else _prio_order()
        for pr in prios:
            q = s.wait_q.get(pr, [])
            # Iterate in FIFO order, but skip completed/failed pipelines and non-runnable ones.
            i = 0
            while i < len(q):
                p = q.pop(0)
                status = p.runtime_status()

                # Drop completed pipelines
                if status.is_pipeline_successful():
                    i += 1
                    continue

                # If pipeline has a failure, we still allow retry (FAILED is in ASSIGNABLE_STATES).
                op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
                if op_list:
                    # Put pipeline back at end (round-robin within same priority).
                    q.append(p)
                    return p, op_list[0]

                # Not runnable yet (parents incomplete); keep it in the queue and move on.
                q.append(p)
                i += 1
        return None, None

    # Pool selection bias:
    # - If multiple pools: try to keep QUERY/INTERACTIVE on pool 0 when possible.
    # - Otherwise, schedule normally across pools.
    pool_order = list(range(s.executor.num_pools))
    if s.executor.num_pools > 1:
        # Keep pool 0 first; others next.
        pool_order = [0] + [i for i in range(1, s.executor.num_pools)]

    for pool_id in pool_order:
        pool = s.executor.pools[pool_id]
        if pool.avail_cpu_pool <= 0 or pool.avail_ram_pool <= 0:
            continue

        # Determine which priorities we prefer on this pool
        if s.executor.num_pools > 1 and pool_id == 0:
            preferred = [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]
        elif s.executor.num_pools > 1 and pool_id != 0:
            # Try to keep background off the interactive pool; but if only background exists, schedule it.
            preferred = [Priority.BATCH_PIPELINE, Priority.INTERACTIVE, Priority.QUERY]
        else:
            preferred = None

        p, op = _pick_next_op(require_prios=preferred)
        if p is None or op is None:
            continue

        # Size resources
        cpu = _desired_cpu(p.priority, pool)
        ram = _desired_ram_gb(p.pipeline_id, op, pool)

        if cpu <= 0 or ram <= 0:
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
        # Keep behavior close to naive baseline: at most one assignment per pool per tick.
        # This reduces complexity and makes improvements easier to interpret.

    return suspensions, assignments
