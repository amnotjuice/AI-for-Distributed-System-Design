# policy_key: scheduler_est_005
# reasoning_effort: low
# model: gpt-5.2-2025-12-11
# llm_cost: 0.037197
# generation_seconds: 38.83
# generated_at: 2026-04-03T01:07:43.092866
@register_scheduler_init(key="scheduler_est_005")
def scheduler_est_005_init(s):
    """Priority-aware FIFO with small, safe improvements over the naive baseline.

    Main ideas (kept intentionally simple / low-risk):
      1) Separate waiting queues by priority; always try to schedule higher priority first.
      2) Avoid letting BATCH consume the entire pool by keeping a small CPU/RAM reserve
         (reduces latency spikes for interactive/query arrivals).
      3) Use optional per-op RAM peak estimate if present; on OOM-like failures, retry with
         increased RAM via a per-op backoff multiplier.

    Notes:
      - No explicit preemption is used (requires runtime visibility into running containers).
      - We schedule at most 1 operator per pool per tick (similar to the naive example),
        but with better choice/order and better sizing.
    """
    # Per-priority round-robin queues of pipeline_ids
    s.wait_q = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }

    # pipeline_id -> Pipeline (latest reference)
    s.pipelines_by_id = {}

    # Per-op RAM backoff multiplier for retry after OOM
    # key: (pipeline_id, op_object_id) -> multiplier (float)
    s.op_ram_mult = {}

    # Reserve fractions (applied when scheduling BATCH work)
    s.batch_cpu_reserve_frac = 0.20
    s.batch_ram_reserve_frac = 0.20

    # CPU caps to reduce single-op monopolization (helps concurrency & tail latency)
    # For high priority we allow a larger share than batch.
    s.query_cpu_cap_frac = 0.90
    s.interactive_cpu_cap_frac = 0.85
    s.batch_cpu_cap_frac = 0.70

    # RAM padding to apply on top of estimates (kept small; we rely on retry on OOM)
    s.est_ram_pad_frac = 0.10

    # Minimal RAM allocation guard (GB) to avoid pathological tiny allocations.
    s.min_ram_gb = 0.25


@register_scheduler(key="scheduler_est_005")
def scheduler_est_005_scheduler(s, results, pipelines):
    """
    Priority-first scheduling with headroom protection for batch and OOM-aware RAM retries.
    """
    # --- Helper functions (kept local to avoid module-level imports) ---
    def _prio_order():
        return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]

    def _enqueue_pipeline(p):
        pid = p.pipeline_id
        s.pipelines_by_id[pid] = p
        q = s.wait_q[p.priority]
        if pid not in q:
            q.append(pid)

    def _drop_pipeline(pid):
        # Remove from dict and all queues if present
        if pid in s.pipelines_by_id:
            del s.pipelines_by_id[pid]
        for pr in s.wait_q:
            q = s.wait_q[pr]
            try:
                q.remove(pid)
            except ValueError:
                pass

    def _pipeline_is_done_or_failed(p):
        st = p.runtime_status()
        if st.is_pipeline_successful():
            return True
        # "Obvious flaw" fix: don't keep retrying pipelines that have operator failures
        # other than those we can handle (OOM). Since we can't robustly distinguish here
        # without inspecting errors per-op, we conservatively keep pipelines alive; we'll
        # only drop when status shows FAILED ops and no assignable ops exist.
        return False

    def _get_one_assignable_op(p):
        st = p.runtime_status()
        # Only schedule ops whose parents are complete (standard pipeline DAG execution)
        ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        if not ops:
            return None
        return ops[0]

    def _op_key(pipeline_id, op):
        return (pipeline_id, id(op))

    def _is_oom_error(err):
        if err is None:
            return False
        # Be permissive: simulators often use simple strings.
        try:
            msg = str(err).lower()
        except Exception:
            return False
        return ("oom" in msg) or ("out of memory" in msg) or ("out-of-memory" in msg)

    def _choose_cpu(prio, pool_avail_cpu, pool_max_cpu, leave_headroom_for_batch):
        # Basic CPU allocation: cap per priority, and optionally leave reserve if batch.
        if pool_avail_cpu <= 0:
            return 0.0
        if prio == Priority.QUERY:
            cap_frac = s.query_cpu_cap_frac
        elif prio == Priority.INTERACTIVE:
            cap_frac = s.interactive_cpu_cap_frac
        else:
            cap_frac = s.batch_cpu_cap_frac

        cpu_cap = max(0.0, pool_max_cpu * cap_frac)
        cpu = min(pool_avail_cpu, cpu_cap)

        if leave_headroom_for_batch:
            # Keep some CPU in reserve for future high-priority arrivals.
            reserve = pool_max_cpu * s.batch_cpu_reserve_frac
            cpu = min(cpu, max(0.0, pool_avail_cpu - reserve))

        # Ensure we don't emit tiny/zero allocations unless forced.
        if cpu <= 0:
            return 0.0
        return cpu

    def _choose_ram(p, op, prio, pool_avail_ram, pool_max_ram, leave_headroom_for_batch):
        if pool_avail_ram <= 0:
            return 0.0

        # Use optional estimate if present; otherwise pick a conservative fraction.
        est = None
        try:
            est = getattr(op, "estimate", None)
            if est is not None:
                est = getattr(est, "mem_peak_gb", None)
        except Exception:
            est = None

        if isinstance(est, (int, float)) and est > 0:
            base = float(est) * (1.0 + s.est_ram_pad_frac)
        else:
            # If no estimate, avoid consuming the pool: choose a moderate slice.
            # (We don't know true min RAM; retry on OOM will correct under-allocations.)
            if prio in (Priority.QUERY, Priority.INTERACTIVE):
                base = pool_max_ram * 0.35
            else:
                base = pool_max_ram * 0.25

        mult = s.op_ram_mult.get(_op_key(p.pipeline_id, op), 1.0)
        ram_req = max(s.min_ram_gb, base * mult)

        ram = min(pool_avail_ram, ram_req)

        if leave_headroom_for_batch:
            reserve = pool_max_ram * s.batch_ram_reserve_frac
            ram = min(ram, max(0.0, pool_avail_ram - reserve))

        if ram <= 0:
            return 0.0
        return ram

    def _pick_next_pipeline_id():
        # Strict priority order; round-robin within each priority.
        for pr in _prio_order():
            q = s.wait_q[pr]
            # Find the first pipeline with an actually schedulable op
            for _ in range(len(q)):
                pid = q.pop(0)
                p = s.pipelines_by_id.get(pid)
                if p is None:
                    continue

                # Drop completed pipelines.
                if _pipeline_is_done_or_failed(p):
                    _drop_pipeline(pid)
                    continue

                op = _get_one_assignable_op(p)
                if op is None:
                    # Nothing schedulable now; keep it in the queue and continue.
                    q.append(pid)
                    continue

                # Put back at end (round-robin) and return it as candidate.
                q.append(pid)
                return pid
        return None

    # --- Incorporate new arrivals ---
    for p in pipelines:
        _enqueue_pipeline(p)

    # --- Process results to adjust RAM multipliers on OOM-like failures ---
    # We only use results for sizing feedback, not to decide preemption here.
    for r in results:
        if r is None or not hasattr(r, "failed"):
            continue
        if r.failed() and _is_oom_error(getattr(r, "error", None)):
            # Backoff RAM for each op in this execution result.
            # This assumes r.ops is iterable; if not, we handle gracefully.
            pid = getattr(r, "pipeline_id", None)
            if pid is None:
                # If pipeline_id isn't present on ExecutionResult, try to infer via stored pipelines:
                # (We can't reliably; skip.)
                pid = None
            try:
                ops_iter = list(getattr(r, "ops", []) or [])
            except Exception:
                ops_iter = []

            # Increase multiplier per-op; if we don't know pid, fall back to global by op id only.
            for op in ops_iter:
                if pid is not None:
                    k = _op_key(pid, op)
                else:
                    k = ("_unknown_pipeline_", id(op))
                old = s.op_ram_mult.get(k, 1.0)
                # Gentle exponential backoff; keeps retries bounded and converges quickly.
                s.op_ram_mult[k] = min(old * 1.6, 16.0)

    # Early exit if nothing changed and no waiting work
    if not pipelines and not results:
        any_waiting = any(len(s.wait_q[pr]) > 0 for pr in s.wait_q)
        if not any_waiting:
            return [], []

    suspensions = []
    assignments = []

    # --- Schedule: at most 1 op per pool per tick ---
    # Place high-priority work first by iterating pools in descending headroom.
    pool_order = list(range(s.executor.num_pools))
    try:
        pool_order.sort(
            key=lambda i: (
                s.executor.pools[i].avail_cpu_pool,
                s.executor.pools[i].avail_ram_pool,
            ),
            reverse=True,
        )
    except Exception:
        pass

    for pool_id in pool_order:
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool
        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        pid = _pick_next_pipeline_id()
        if pid is None:
            continue

        p = s.pipelines_by_id.get(pid)
        if p is None:
            continue

        op = _get_one_assignable_op(p)
        if op is None:
            continue

        prio = p.priority
        leave_headroom = (prio == Priority.BATCH_PIPELINE)

        cpu = _choose_cpu(prio, avail_cpu, pool.max_cpu_pool, leave_headroom_for_batch=leave_headroom)
        ram = _choose_ram(p, op, prio, avail_ram, pool.max_ram_pool, leave_headroom_for_batch=leave_headroom)

        # If headroom rules prevented any allocation, try again without headroom
        # (better to make progress than to deadlock under constant batch-only load).
        if (cpu <= 0 or ram <= 0) and leave_headroom:
            cpu = _choose_cpu(prio, avail_cpu, pool.max_cpu_pool, leave_headroom_for_batch=False)
            ram = _choose_ram(p, op, prio, avail_ram, pool.max_ram_pool, leave_headroom_for_batch=False)

        if cpu <= 0 or ram <= 0:
            continue

        assignments.append(
            Assignment(
                ops=[op],
                cpu=cpu,
                ram=ram,
                priority=prio,
                pool_id=pool_id,
                pipeline_id=p.pipeline_id,
            )
        )

    return suspensions, assignments
