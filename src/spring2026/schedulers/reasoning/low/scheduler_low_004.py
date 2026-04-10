# policy_key: scheduler_low_004
# reasoning_effort: low
# model: gpt-5.2-2025-12-11
# llm_cost: 0.045637
# generation_seconds: 50.97
# generated_at: 2026-04-09T20:20:26.334017
@register_scheduler_init(key="scheduler_low_004")
def scheduler_low_004_init(s):
    """
    Priority-aware, OOM-adaptive, non-preemptive packing scheduler.

    Goals:
      - Protect QUERY/INTERACTIVE latency (dominant weights) via strict priority and
        preferential use of "fast" pool (pool 0 when available).
      - Avoid failures (720s penalty) by allocating generous RAM initially and
        adaptively increasing RAM on failed attempts (esp. OOM-like failures).
      - Avoid starvation by periodically admitting BATCH even under steady high-priority load
        (simple quota/aging mechanism).
      - Increase throughput vs naive FIFO by packing multiple assignments per pool per tick
        (until pool resources are exhausted).
    """
    # Per-priority waiting queues (pipelines)
    s.q_query = []
    s.q_interactive = []
    s.q_batch = []

    # Track pipelines we've seen to avoid double-enqueue in some simulator variants
    s.known_pipeline_ids = set()

    # Adaptive resource estimates keyed by (pipeline_id, op_obj_id)
    # Values are dicts with "ram" and "cpu" suggested sizes.
    s.op_estimates = {}

    # Attempts keyed by (pipeline_id, op_obj_id)
    s.op_attempts = {}

    # Fairness knobs: after dispatching this many high-priority ops, force-try a batch op
    s.hp_dispatch_budget = 6
    s.hp_dispatch_count = 0


@register_scheduler(key="scheduler_low_004")
def scheduler_low_004_scheduler(s, results, pipelines):
    """
    Scheduler step:
      1) Ingest new pipelines into per-priority queues.
      2) Update per-op estimates based on execution results (success/failure).
      3) For each pool, repeatedly assign ready ops (parents complete) while resources allow:
           - Prefer QUERY then INTERACTIVE.
           - Admit BATCH regularly (quota) to prevent starvation.
           - If multiple pools: pool 0 biased to high priority; other pools biased to batch.
      4) No preemption (suspensions empty) to avoid churn/wasted work.
    """
    import math

    suspensions = []
    assignments = []

    # -----------------------------
    # Helpers
    # -----------------------------
    def _enqueue_pipeline(p):
        if p.pipeline_id in s.known_pipeline_ids:
            return
        s.known_pipeline_ids.add(p.pipeline_id)
        if p.priority == Priority.QUERY:
            s.q_query.append(p)
        elif p.priority == Priority.INTERACTIVE:
            s.q_interactive.append(p)
        else:
            s.q_batch.append(p)

    def _pipeline_terminal(p):
        st = p.runtime_status()
        # Consider it terminal only if fully successful.
        # We do NOT drop on failures; we retry FAILED ops (ASSIGNABLE_STATES include FAILED).
        return st.is_pipeline_successful()

    def _get_ready_op(p):
        st = p.runtime_status()
        if st.is_pipeline_successful():
            return None
        ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        if not ops:
            return None
        return ops[0]

    def _op_key(pipeline_id, op):
        # Use object id for op identity within run; stable enough for the simulator process.
        return (pipeline_id, id(op))

    def _is_oom_like(err):
        if err is None:
            return False
        try:
            msg = str(err).lower()
        except Exception:
            return False
        return ("oom" in msg) or ("out of memory" in msg) or ("memory" in msg and "exceed" in msg)

    def _priority_base_fractions(priority):
        # RAM generosity is crucial to avoid the 720s penalty.
        # CPU is moderate because scaling is sublinear; avoid monopolizing all CPU.
        if priority == Priority.QUERY:
            return 0.75, 0.70  # ram_frac, cpu_frac
        if priority == Priority.INTERACTIVE:
            return 0.70, 0.65
        return 0.50, 0.55  # batch

    def _clamp(x, lo, hi):
        return max(lo, min(hi, x))

    def _suggest_resources(pool, p, op):
        # Use learned estimates if present; otherwise start with generous priority-based fractions.
        ram_frac, cpu_frac = _priority_base_fractions(p.priority)
        base_ram = max(0.0, pool.max_ram_pool * ram_frac)
        base_cpu = max(0.0, pool.max_cpu_pool * cpu_frac)

        k = _op_key(p.pipeline_id, op)
        est = s.op_estimates.get(k)
        if est is not None:
            # Keep the larger of base and estimate for high priorities to reduce failure risk.
            # For batch, allow using estimate more directly to improve packing.
            if p.priority in (Priority.QUERY, Priority.INTERACTIVE):
                want_ram = max(base_ram, est.get("ram", 0.0))
                want_cpu = max(base_cpu, est.get("cpu", 0.0))
            else:
                want_ram = max(0.0, est.get("ram", base_ram))
                want_cpu = max(0.0, est.get("cpu", base_cpu))
        else:
            want_ram, want_cpu = base_ram, base_cpu

        # Ensure strictly positive requests; if pool has tiny availability, we won't schedule anyway.
        want_cpu = max(0.25, want_cpu)
        want_ram = max(0.25, want_ram)

        # Cap to pool max
        want_cpu = _clamp(want_cpu, 0.25, pool.max_cpu_pool)
        want_ram = _clamp(want_ram, 0.25, pool.max_ram_pool)
        return want_cpu, want_ram

    def _pool_bias_allows(pool_id, priority, has_hp_waiting):
        # If multiple pools exist, bias pool 0 toward high priority; other pools toward batch.
        # Still allow "steal" when the preferred class is absent to prevent idleness.
        if s.executor.num_pools <= 1:
            return True
        if pool_id == 0:
            # Pool 0 always allowed; but batch should only run here if no high-priority is waiting.
            if priority == Priority.BATCH_PIPELINE and has_hp_waiting:
                return False
            return True
        else:
            # Non-zero pools prefer batch; allow high-priority if batch absent to prevent waiting.
            if priority in (Priority.QUERY, Priority.INTERACTIVE):
                # Allow only if there is no batch waiting (so we don't starve batch capacity).
                return (len(s.q_batch) == 0)
            return True

    # -----------------------------
    # Ingest new pipelines
    # -----------------------------
    for p in pipelines:
        _enqueue_pipeline(p)

    # -----------------------------
    # Update estimates from results
    # -----------------------------
    # On success: record the resources that worked as a baseline estimate.
    # On failure: increase RAM (and slightly CPU) to reduce repeated failure risk.
    for r in results:
        if not getattr(r, "ops", None):
            continue
        for op in r.ops:
            k = _op_key(getattr(r, "pipeline_id", None) or getattr(op, "pipeline_id", None) or "unknown", op)
            # If we cannot reliably key by pipeline_id from result/op, fall back to op id only.
            # This is best-effort; the simulator typically keeps op objects unique per pipeline.
            if k[0] == "unknown":
                k = ("unknown", id(op))

            attempts = s.op_attempts.get(k, 0)
            s.op_attempts[k] = attempts + (1 if r.failed() else 0)

            prev = s.op_estimates.get(k, {"ram": 0.0, "cpu": 0.0})
            if not r.failed():
                # Success: lock in a conservative estimate: at least what succeeded.
                s.op_estimates[k] = {
                    "ram": max(prev.get("ram", 0.0), float(getattr(r, "ram", 0.0) or 0.0)),
                    "cpu": max(prev.get("cpu", 0.0), float(getattr(r, "cpu", 0.0) or 0.0)),
                }
            else:
                # Failure: assume it's often under-provisioning; increase RAM aggressively if OOM-like.
                ram_now = float(getattr(r, "ram", 0.0) or prev.get("ram", 0.0) or 0.0)
                cpu_now = float(getattr(r, "cpu", 0.0) or prev.get("cpu", 0.0) or 0.0)

                oom_like = _is_oom_like(getattr(r, "error", None))
                ram_mult = 1.75 if oom_like else 1.35
                cpu_mult = 1.15

                # Ensure some growth even if ram_now/cpu_now is missing.
                if ram_now <= 0:
                    ram_now = 1.0
                if cpu_now <= 0:
                    cpu_now = 0.5

                s.op_estimates[k] = {
                    "ram": ram_now * ram_mult,
                    "cpu": cpu_now * cpu_mult,
                }

    # -----------------------------
    # Scheduling / packing
    # -----------------------------
    # Rotate pipelines by popping from front and appending back if still active, to avoid head-of-line blocking.
    def _schedule_from_queue(pool_id, pool, queue, max_iters, has_hp_waiting):
        nonlocal assignments
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool
        iters = 0

        while iters < max_iters and queue and avail_cpu > 0 and avail_ram > 0:
            iters += 1
            p = queue.pop(0)

            if _pipeline_terminal(p):
                # Done; do not requeue.
                continue

            op = _get_ready_op(p)
            if op is None:
                # Nothing ready; requeue for later.
                queue.append(p)
                continue

            # Avoid infinite retry loops for truly broken ops: after many failures, stop spending pool 0 cycles.
            k = _op_key(p.pipeline_id, op)
            attempts = s.op_attempts.get(k, 0)
            if attempts >= 6 and p.priority in (Priority.QUERY, Priority.INTERACTIVE) and pool_id == 0:
                # Push it back; maybe other work can proceed.
                queue.append(p)
                continue

            if not _pool_bias_allows(pool_id, p.priority, has_hp_waiting):
                # Requeue and let other priority run on this pool.
                queue.append(p)
                return

            want_cpu, want_ram = _suggest_resources(pool, p, op)

            # Fit to available
            cpu = min(float(avail_cpu), float(want_cpu))
            ram = min(float(avail_ram), float(want_ram))

            # Require meaningful resources; otherwise stop packing in this pool.
            if cpu < 0.25 or ram < 0.25:
                queue.insert(0, p)
                return

            # Create assignment
            a = Assignment(
                ops=[op],
                cpu=cpu,
                ram=ram,
                priority=p.priority,
                pool_id=pool_id,
                pipeline_id=p.pipeline_id,
            )
            assignments.append(a)

            # Update available for packing more into the pool this tick
            avail_cpu -= cpu
            avail_ram -= ram

            # Requeue pipeline (likely has more ops)
            queue.append(p)

    # Determine if any high priority is waiting (for pool bias)
    def _has_hp_waiting():
        # "Waiting" meaning present in queues and not terminal
        for q in (s.q_query, s.q_interactive):
            for p in q:
                if not _pipeline_terminal(p):
                    return True
        return False

    hp_waiting = _has_hp_waiting()

    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        if pool.avail_cpu_pool <= 0 or pool.avail_ram_pool <= 0:
            continue

        # Packing iteration budget per pool to keep step time bounded.
        # More iterations -> more packing -> better utilization.
        budget = 32

        # Priority order with starvation protection:
        # - Normally: QUERY -> INTERACTIVE -> BATCH
        # - Every hp_dispatch_budget high-priority dispatches (global), try BATCH first if available.
        force_batch = (len(s.q_batch) > 0) and (s.hp_dispatch_count >= s.hp_dispatch_budget)

        if force_batch:
            _schedule_from_queue(pool_id, pool, s.q_batch, budget, hp_waiting)
            # Reset after giving batch a chance (even if it didn't fit, avoid perma-blocking)
            s.hp_dispatch_count = 0

        # High priority scheduling
        before = len(assignments)
        _schedule_from_queue(pool_id, pool, s.q_query, budget, hp_waiting)
        _schedule_from_queue(pool_id, pool, s.q_interactive, budget, hp_waiting)
        dispatched_hp = max(0, len(assignments) - before)

        if dispatched_hp > 0:
            s.hp_dispatch_count += dispatched_hp

        # Fill remaining capacity with batch
        _schedule_from_queue(pool_id, pool, s.q_batch, budget, hp_waiting)

    return suspensions, assignments
