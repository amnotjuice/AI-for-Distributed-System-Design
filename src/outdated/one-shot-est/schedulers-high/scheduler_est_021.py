# policy_key: scheduler_est_021
# reasoning_effort: high
# model: gpt-5.2-2025-12-11
# llm_cost: 0.077510
# generation_seconds: 109.98
# generated_at: 2026-03-31T19:00:31.158058
@register_scheduler_init(key="scheduler_est_021")
def scheduler_est_021_init(s):
    """Priority-aware, estimate-driven scheduler (small step up from naive FIFO).

    Improvements over the naive example:
      1) Prioritize QUERY/INTERACTIVE over BATCH to improve tail latency.
      2) Use op.estimate.mem_peak_gb to right-size RAM (with a safety factor).
      3) Retry OOM failures with increased RAM instead of dropping the pipeline.
      4) Light fairness via aging so BATCH eventually makes progress.

    Notes:
      - No preemption (we don't rely on any non-documented "list running containers" API).
      - Schedules multiple ops per pool per tick when resources allow, but at most one op
        per pipeline per tick to avoid a single pipeline hogging capacity.
    """
    s.tick = 0

    # Pipeline tracking: pipeline_id -> {"p": Pipeline, "arrival": int, "last_scheduled": int}
    s.waiting = {}

    # Per-operator adaptive sizing / retry bookkeeping (keyed by a stable op key).
    s.op_mem_mult = {}     # op_key -> multiplier applied to estimate (starts near 1.0-1.5)
    s.op_oom_retries = {}  # op_key -> count
    s.op_to_pipeline = {}  # op_key -> pipeline_id (populated when we schedule the op)

    # Give up on ops that OOM too many times (usually indicates underestimate vs pool capacity).
    s.max_oom_retries = 3

    # Pipelines we will no longer attempt (e.g., repeated OOM beyond retries or non-OOM failure).
    s.blacklisted_pipelines = set()


def _op_key(op):
    # Prefer a stable semantic id if present; fallback to object identity.
    k = getattr(op, "op_id", None)
    return k if k is not None else id(op)


def _is_oom_error(err):
    if not err:
        return False
    try:
        msg = str(err).lower()
    except Exception:
        return False
    return ("oom" in msg) or ("out of memory" in msg) or ("memory" in msg and "killed" in msg)


def _priority_base_score(priority):
    # Higher is more important.
    if priority == Priority.QUERY:
        return 300.0
    if priority == Priority.INTERACTIVE:
        return 200.0
    if priority == Priority.BATCH_PIPELINE:
        return 100.0
    return 100.0


def _aging_factor(priority):
    # Let batch age faster so it eventually gets scheduled under sustained high-priority load.
    if priority == Priority.BATCH_PIPELINE:
        return 2.0
    return 0.5


def _cpu_target(pool, priority, avail_cpu):
    # Bias more CPU to latency-sensitive work; keep a small minimum to allow progress.
    max_cpu = float(getattr(pool, "max_cpu_pool", avail_cpu))
    avail_cpu = float(avail_cpu)

    if priority == Priority.QUERY:
        target = 0.75 * max_cpu
    elif priority == Priority.INTERACTIVE:
        target = 0.50 * max_cpu
    else:
        target = 0.25 * max_cpu

    # Minimum CPU: try to use at least 1 vCPU when possible.
    min_cpu = 1.0 if avail_cpu >= 1.0 else max(0.25, avail_cpu)
    return max(min_cpu, min(target, avail_cpu))


def _ram_target(pool, op, mem_mult, avail_ram):
    # Use estimate.mem_peak_gb with headroom + learned multiplier from past OOMs.
    avail_ram = float(avail_ram)
    max_ram = float(getattr(pool, "max_ram_pool", avail_ram))

    est = None
    try:
        est = float(op.estimate.mem_peak_gb)
    except Exception:
        est = None

    # If we have no estimate, be conservative but not absurdly large.
    if est is None or est <= 0:
        est = 1.0

    # Safety headroom: base 20% plus adaptive multiplier after OOMs.
    # Also add a small constant to avoid razor-thin allocations.
    ram = (est * 1.20 * float(mem_mult)) + 0.25

    # Enforce small minimum; cap to pool max; do not exceed current pool availability.
    ram = max(0.25, ram)
    ram = min(ram, max_ram)
    ram = min(ram, avail_ram)
    return ram


@register_scheduler(key="scheduler_est_021")
def scheduler_est_021_scheduler(s, results, pipelines):
    """
    Priority + estimate aware scheduler:
      - Maintain a waiting set of pipelines.
      - Sort candidates by (priority + aging).
      - For each pool, schedule as many ready ops as fit.
      - On OOM, increase per-op memory multiplier and retry (up to a limit).
    """
    s.tick += 1

    # --- Ingest new pipelines ---
    for p in pipelines:
        pid = p.pipeline_id
        if pid in s.blacklisted_pipelines:
            continue
        if pid not in s.waiting:
            s.waiting[pid] = {"p": p, "arrival": s.tick, "last_scheduled": -1}
        else:
            # Refresh the pipeline object reference in case simulator provides updated instances.
            s.waiting[pid]["p"] = p

    # --- Process execution results (learn from OOM, optionally blacklist) ---
    for r in results:
        # We only learn about the ops we scheduled (op->pipeline mapping set at scheduling time).
        for op in getattr(r, "ops", []) or []:
            ok = not r.failed()
            k = _op_key(op)
            pid = s.op_to_pipeline.get(k, None)

            if ok:
                # Successful completion: gently decay multiplier toward 1.0 (keep a small headroom).
                old = float(s.op_mem_mult.get(k, 1.25))
                s.op_mem_mult[k] = max(1.10, old * 0.95)
                # Reset retry count on success.
                s.op_oom_retries[k] = 0
                continue

            # Failure case
            if _is_oom_error(getattr(r, "error", None)):
                retries = int(s.op_oom_retries.get(k, 0)) + 1
                s.op_oom_retries[k] = retries

                # Increase multiplier; prefer using observed allocated RAM as a guide if present.
                old_mult = float(s.op_mem_mult.get(k, 1.25))
                try:
                    used_ram = float(getattr(r, "ram", 0.0))
                except Exception:
                    used_ram = 0.0

                # Step up aggressively after OOM; ensures quick convergence in a few retries.
                # If we know used_ram, increase multiplier proportionally.
                if used_ram and used_ram > 0:
                    # If we OOM'ed even at used_ram, bump by 50%+ each retry.
                    s.op_mem_mult[k] = max(old_mult * 1.50, old_mult + 0.75)
                else:
                    s.op_mem_mult[k] = max(old_mult * 1.60, old_mult + 0.75)

                # Too many OOM retries: blacklist pipeline (prevents infinite churn).
                if pid is not None and retries > int(getattr(s, "max_oom_retries", 3)):
                    s.blacklisted_pipelines.add(pid)
            else:
                # Non-OOM failure: conservative choice is to stop retrying this pipeline.
                if pid is not None:
                    s.blacklisted_pipelines.add(pid)

    # --- Garbage-collect finished / blacklisted pipelines ---
    to_delete = []
    for pid, rec in s.waiting.items():
        if pid in s.blacklisted_pipelines:
            to_delete.append(pid)
            continue
        p = rec["p"]
        try:
            if p.runtime_status().is_pipeline_successful():
                to_delete.append(pid)
        except Exception:
            # If status is unavailable, keep it.
            pass
    for pid in to_delete:
        s.waiting.pop(pid, None)

    # Early exit if nothing to do.
    if not s.waiting and not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    # Helper: precompute a global pipeline ordering (we'll still check per-pool fit).
    def pipeline_score(rec):
        p = rec["p"]
        base = _priority_base_score(p.priority)
        age = max(0, s.tick - int(rec["arrival"]))
        since_sched = age if rec["last_scheduled"] < 0 else max(0, s.tick - int(rec["last_scheduled"]))
        # Aging: grows with time since arrival and since last scheduled.
        score = base + (_aging_factor(p.priority) * age) + (0.10 * since_sched)
        return score

    # We keep this list stable within a tick; selection still depends on current pool resources.
    waiting_recs = list(s.waiting.values())
    waiting_recs.sort(key=pipeline_score, reverse=True)

    scheduled_this_tick = set()

    # Whether we currently have any high-priority demand.
    has_high_demand = any(
        rec["p"].priority in (Priority.QUERY, Priority.INTERACTIVE) for rec in waiting_recs
    )

    # --- Main scheduling loop: fill each pool ---
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)

        if avail_cpu <= 0.0 or avail_ram <= 0.0:
            continue

        # Simple "headroom" guardrail:
        # If there is high-priority demand, keep some capacity in pool 0 to reduce queuing delay.
        # (We only apply this guardrail against batch.)
        reserve_cpu = 0.0
        reserve_ram = 0.0
        if pool_id == 0 and has_high_demand:
            reserve_cpu = 0.20 * float(getattr(pool, "max_cpu_pool", avail_cpu))
            reserve_ram = 0.20 * float(getattr(pool, "max_ram_pool", avail_ram))

        # Two-pass fill:
        #   1) schedule QUERY/INTERACTIVE
        #   2) schedule BATCH with reserved headroom considered (on pool 0)
        for pass_idx in (0, 1):
            while avail_cpu > 0.0 and avail_ram > 0.0:
                picked = None

                for rec in waiting_recs:
                    p = rec["p"]
                    pid = p.pipeline_id

                    if pid in s.blacklisted_pipelines:
                        continue
                    if pid in scheduled_this_tick:
                        continue

                    if pass_idx == 0:
                        # High priority pass
                        if p.priority not in (Priority.QUERY, Priority.INTERACTIVE):
                            continue
                    else:
                        # Batch pass
                        if p.priority != Priority.BATCH_PIPELINE:
                            continue

                    # Skip pipelines already completed (defensive).
                    st = p.runtime_status()
                    if st.is_pipeline_successful():
                        continue

                    # Find a ready op (parents complete).
                    op_list = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
                    if not op_list:
                        continue
                    op = op_list[0]

                    # Size resources.
                    k = _op_key(op)
                    mem_mult = float(s.op_mem_mult.get(k, 1.25))

                    # Compute effective available resources for this pass (apply reservation only to batch).
                    eff_avail_cpu = avail_cpu
                    eff_avail_ram = avail_ram
                    if pass_idx == 1 and pool_id == 0 and has_high_demand:
                        eff_avail_cpu = max(0.0, avail_cpu - reserve_cpu)
                        eff_avail_ram = max(0.0, avail_ram - reserve_ram)

                    if eff_avail_cpu <= 0.0 or eff_avail_ram <= 0.0:
                        continue

                    cpu = _cpu_target(pool, p.priority, eff_avail_cpu)
                    ram = _ram_target(pool, op, mem_mult, eff_avail_ram)

                    # Fit check.
                    if cpu <= 0.0 or ram <= 0.0:
                        continue
                    if cpu > eff_avail_cpu + 1e-9 or ram > eff_avail_ram + 1e-9:
                        continue

                    picked = (p, op, cpu, ram, pid, rec)
                    break

                if not picked:
                    break

                p, op, cpu, ram, pid, rec = picked

                assignments.append(
                    Assignment(
                        ops=[op],
                        cpu=cpu,
                        ram=ram,
                        priority=p.priority,
                        pool_id=pool_id,
                        pipeline_id=pid,
                    )
                )

                # Bookkeeping for learning + fairness.
                scheduled_this_tick.add(pid)
                rec["last_scheduled"] = s.tick
                s.op_to_pipeline[_op_key(op)] = pid

                # Update local available resource accounting for this pool.
                avail_cpu -= float(cpu)
                avail_ram -= float(ram)

                # Stop if we're effectively out.
                if avail_cpu <= 1e-9 or avail_ram <= 1e-9:
                    break

    return suspensions, assignments
