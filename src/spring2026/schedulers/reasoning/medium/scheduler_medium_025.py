# policy_key: scheduler_medium_025
# reasoning_effort: medium
# exp: reasoning
# model: gpt-5.2-2025-12-11
# llm_cost: 0.062955
# generation_seconds: 68.88
# generated_at: 2026-04-09T23:02:06.253736
@register_scheduler_init(key="scheduler_medium_025")
def scheduler_medium_025_init(s):
    """Priority-aware, failure-averse scheduler.

    Main ideas:
      - Strict preference for QUERY/INTERACTIVE over BATCH, with mild aging to prevent starvation.
      - Conservative (larger) RAM allocations for high-priority ops to reduce OOM retries.
      - Adaptive RAM backoff on failures (especially OOM): increase per-op RAM estimate and retry.
      - Multi-pool aware: prefer keeping pool 0 for high-priority work when multiple pools exist.
      - Avoid preemption churn: no suspensions by default; focus on smart admission/packing.
    """
    s.waiting_queue = []  # list[Pipeline]
    s.pipeline_meta = {}  # pipeline_id -> dict(enqueue_tick, last_scheduled_tick)
    s.op_ram_est = {}     # op_key -> float (RAM estimate)
    s.op_failures = {}    # op_key -> int
    s.tick = 0


def _priority_rank(priority):
    # Higher is more important for scheduling order.
    if priority == Priority.QUERY:
        return 3
    if priority == Priority.INTERACTIVE:
        return 2
    return 1  # Priority.BATCH_PIPELINE (or any other)


def _pool_allows_priority(num_pools, pool_id, priority, has_hi_waiting):
    # If we have multiple pools, reserve pool 0 for high-priority work when there's demand.
    if num_pools <= 1:
        return True
    if pool_id == 0:
        if has_hi_waiting:
            return priority in (Priority.QUERY, Priority.INTERACTIVE)
        return True
    return True


def _op_key(pipeline_id, op):
    # Build a stable-ish key for per-op estimates.
    oid = None
    for attr in ("op_id", "operator_id", "id", "name"):
        if hasattr(op, attr):
            try:
                oid = getattr(op, attr)
                break
            except Exception:
                pass
    if oid is None:
        oid = id(op)
    return (pipeline_id, oid)


def _cpu_target(pool, priority):
    # Sublinear CPU scaling: allocate moderately, not "all available".
    max_cpu = pool.max_cpu_pool
    if priority == Priority.QUERY:
        frac = 0.75
    elif priority == Priority.INTERACTIVE:
        frac = 0.50
    else:
        frac = 0.25
    tgt = int(max(1, round(max_cpu * frac)))
    return max(1, tgt)


def _ram_target(pool, priority):
    # RAM has no speed benefit beyond minimum; we bias higher for high priority to avoid OOM.
    max_ram = pool.max_ram_pool
    if priority == Priority.QUERY:
        frac = 0.55
    elif priority == Priority.INTERACTIVE:
        frac = 0.40
    else:
        frac = 0.25
    # Ensure at least some non-trivial allocation even on tiny pools.
    return max(1.0, max_ram * frac)


def _effective_score(s, pipeline):
    # Ordering score: base priority + mild aging to avoid starvation.
    pid = pipeline.pipeline_id
    meta = s.pipeline_meta.get(pid, {})
    enq = meta.get("enqueue_tick", s.tick)
    wait = max(0, s.tick - enq)

    base = _priority_rank(pipeline.priority)
    # Aging: slow ramp-up; enough to ensure batch progress under sustained load.
    aging = wait / 200.0
    return base + aging


def _dedupe_and_prune_waiting(s):
    # Keep only incomplete pipelines; dedupe by pipeline_id (keep latest object reference).
    latest = {}
    for p in s.waiting_queue:
        latest[p.pipeline_id] = p

    pruned = []
    for pid, p in latest.items():
        try:
            st = p.runtime_status()
            if st.is_pipeline_successful():
                continue
        except Exception:
            # If status is unavailable for any reason, keep it (failure-averse).
            pass
        pruned.append(p)

    s.waiting_queue = pruned


def _update_estimates_from_results(s, results):
    # On failures, especially OOM, increase RAM estimate for the failed op(s) and retry later.
    for r in results:
        if not hasattr(r, "ops") or r.ops is None:
            continue

        if not r.failed():
            # No change needed on success; we keep the last known "safe" estimate.
            continue

        err = ""
        try:
            err = (r.error or "")
        except Exception:
            err = ""
        err_l = err.lower()

        # Slightly different multipliers for OOM vs other failures.
        is_oom = ("oom" in err_l) or ("out of memory" in err_l) or ("cuda oom" in err_l)
        bump = 1.7 if is_oom else 1.3

        # Cap estimates to the pool maximum RAM; we can infer that from r.pool_id.
        pool_max_ram = None
        try:
            pool = s.executor.pools[r.pool_id]
            pool_max_ram = pool.max_ram_pool
        except Exception:
            pool_max_ram = None

        for op in r.ops:
            ok = _op_key(getattr(r, "pipeline_id", None), op)  # best effort
            # If pipeline_id isn't present on result, fall back to op identity only.
            if ok[0] is None:
                ok = ("_unknown_pipeline_", ok[1])

            s.op_failures[ok] = s.op_failures.get(ok, 0) + 1

            prev = s.op_ram_est.get(ok, None)
            assigned = None
            try:
                assigned = float(r.ram)
            except Exception:
                assigned = None

            if prev is None:
                prev = assigned if assigned is not None else 1.0

            # If the failed run already used a lot of RAM, bump from that floor.
            base = max(prev, assigned if assigned is not None else prev)
            new_est = base * bump

            if pool_max_ram is not None:
                new_est = min(new_est, float(pool_max_ram))

            # Avoid no-op updates that can cause repeated identical OOMs.
            if new_est <= prev:
                new_est = min(float(pool_max_ram), prev * 1.15) if pool_max_ram is not None else prev * 1.15

            s.op_ram_est[ok] = new_est


@register_scheduler(key="scheduler_medium_025")
def scheduler_medium_025(s, results, pipelines):
    """
    Scheduler step:
      - Add arrivals to waiting queue (track enqueue tick).
      - Update per-op RAM estimates on failures to improve completion rate.
      - For each pool, repeatedly assign runnable ops, prioritizing QUERY/INTERACTIVE.
      - Keep pool 0 (if exists) for high-priority work when there is demand.
    """
    s.tick += 1

    # Add new arrivals.
    for p in pipelines:
        s.waiting_queue.append(p)
        if p.pipeline_id not in s.pipeline_meta:
            s.pipeline_meta[p.pipeline_id] = {
                "enqueue_tick": s.tick,
                "last_scheduled_tick": -1,
            }

    # Process results to adapt sizing (primarily RAM) after failures.
    if results:
        _update_estimates_from_results(s, results)

    # Early exit only when we truly have nothing to do.
    if not s.waiting_queue and not pipelines and not results:
        return [], []

    # Remove completed pipelines; keep everything else (failure-averse).
    _dedupe_and_prune_waiting(s)

    suspensions = []
    assignments = []

    # Determine whether we have high-priority demand.
    has_hi_waiting = False
    for p in s.waiting_queue:
        if p.priority in (Priority.QUERY, Priority.INTERACTIVE):
            try:
                st = p.runtime_status()
                ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
                if ops:
                    has_hi_waiting = True
                    break
            except Exception:
                # If status query fails, conservatively treat as demand.
                has_hi_waiting = True
                break

    # Sort pipelines once per tick by effective score (priority + aging).
    # We'll still check per-pool constraints and op readiness dynamically.
    waiting_sorted = sorted(s.waiting_queue, key=lambda p: _effective_score(s, p), reverse=True)

    num_pools = s.executor.num_pools
    for pool_id in range(num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool

        # Try to fill the pool with multiple assignments if resources permit.
        # We decrement local avail_* as we add assignments.
        made_progress = True
        while made_progress and avail_cpu >= 1 and avail_ram > 0:
            made_progress = False

            # Find best pipeline for this pool with a runnable op.
            chosen_p = None
            chosen_op = None

            for p in waiting_sorted:
                if not _pool_allows_priority(num_pools, pool_id, p.priority, has_hi_waiting):
                    continue

                try:
                    st = p.runtime_status()
                    if st.is_pipeline_successful():
                        continue
                    op_list = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
                except Exception:
                    # If we can't inspect status, skip this pipeline this tick.
                    continue

                if not op_list:
                    continue

                op = op_list[0]

                # Compute tentative allocations and ensure they fit.
                cpu_tgt = _cpu_target(pool, p.priority)
                cpu_alloc = min(avail_cpu, cpu_tgt)
                if cpu_alloc < 1:
                    continue

                ok = _op_key(p.pipeline_id, op)
                ram_est = s.op_ram_est.get(ok, None)
                if ram_est is None:
                    ram_est = _ram_target(pool, p.priority)
                # Never allocate more RAM than currently available in the pool.
                ram_alloc = min(float(avail_ram), float(ram_est))
                if ram_alloc <= 0:
                    continue

                # Small guard: if high-priority demand exists, avoid filling non-zero pools with
                # huge batch RAM chunks that could block interactive spillover.
                if has_hi_waiting and p.priority == Priority.BATCH_PIPELINE and num_pools > 1:
                    # Keep some headroom if possible.
                    headroom = 0.15 * float(pool.max_ram_pool)
                    if float(avail_ram) - ram_alloc < headroom and float(avail_ram) > headroom:
                        ram_alloc = max(1.0, float(avail_ram) - headroom)

                chosen_p, chosen_op = p, op
                chosen_cpu, chosen_ram = cpu_alloc, ram_alloc
                break

            if chosen_p is None:
                break

            assignments.append(
                Assignment(
                    ops=[chosen_op],
                    cpu=chosen_cpu,
                    ram=chosen_ram,
                    priority=chosen_p.priority,
                    pool_id=pool_id,
                    pipeline_id=chosen_p.pipeline_id,
                )
            )

            # Update local available resources.
            avail_cpu -= chosen_cpu
            avail_ram -= chosen_ram
            made_progress = True

            # Track scheduling activity for potential future improvements.
            meta = s.pipeline_meta.get(chosen_p.pipeline_id)
            if meta is not None:
                meta["last_scheduled_tick"] = s.tick

    return suspensions, assignments
