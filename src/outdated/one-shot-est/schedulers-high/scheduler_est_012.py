# policy_key: scheduler_est_012
# reasoning_effort: high
# model: gpt-5.2-2025-12-11
# llm_cost: 0.089452
# generation_seconds: 79.46
# generated_at: 2026-03-31T18:43:22.820193
@register_scheduler_init(key="scheduler_est_012")
def scheduler_est_012_init(s):
    """
    Priority-aware, estimate-based scheduler (incremental improvement over naive FIFO).

    Main ideas:
      - Maintain per-priority FIFO queues (QUERY > INTERACTIVE > BATCH_PIPELINE).
      - Right-size RAM per op using op.estimate.mem_peak_gb with a small safety factor.
      - Cap per-op CPU to avoid a single op grabbing an entire pool (helps latency under contention).
      - Keep a small reserved headroom for high-priority work when any high-priority pipelines are waiting.
      - Retry FAILED ops only when the last failure looked like OOM; otherwise mark pipeline as permanently failed.
    """
    # Pipeline bookkeeping
    s.pipelines_by_id = {}  # pipeline_id -> Pipeline
    s.q_query = []
    s.q_interactive = []
    s.q_batch = []

    # Failure/estimation feedback
    s.op_oom_multiplier = {}          # (pipeline_id, op_key) -> multiplier (>=1.0)
    s.pipeline_permanent_fail = set()  # pipeline_id

    # Tuning knobs (kept simple / conservative)
    s.max_assignments_per_pool = 2

    # CPU caps as fractions of pool max CPU (prevents one op from monopolizing a pool)
    s.cpu_cap_frac = {
        Priority.QUERY: 0.75,
        Priority.INTERACTIVE: 0.60,
        Priority.BATCH_PIPELINE: 0.40,
    }

    # RAM safety factor on top of op.estimate.mem_peak_gb
    s.ram_safety = {
        Priority.QUERY: 1.25,
        Priority.INTERACTIVE: 1.20,
        Priority.BATCH_PIPELINE: 1.15,
    }

    # Reserved headroom fractions (only applied to scheduling BATCH when hi-pri is waiting)
    s.reserve_cpu_frac_for_hi = 0.25
    s.reserve_ram_frac_for_hi = 0.25


@register_scheduler(key="scheduler_est_012")
def scheduler_est_012_scheduler(s, results, pipelines):
    """
    Scheduler step:
      1) Ingest new pipelines into priority queues.
      2) Process results to:
         - Detect OOM failures and increase RAM multiplier for that op on retry.
         - Mark pipelines with non-OOM failures as permanently failed (no retries).
      3) For each pool, schedule up to N assignments:
         - Always try QUERY, then INTERACTIVE, then BATCH.
         - For BATCH, subtract reserved headroom if any high-priority work is waiting.
         - Use op.estimate.mem_peak_gb * safety * oom_multiplier for RAM sizing; cap CPU.
    """
    def _queue_for_priority(pri):
        if pri == Priority.QUERY:
            return s.q_query
        if pri == Priority.INTERACTIVE:
            return s.q_interactive
        return s.q_batch

    def _is_oom_error(err):
        if not err:
            return False
        msg = str(err).lower()
        return ("oom" in msg) or ("out of memory" in msg) or ("killed" in msg and "memory" in msg)

    def _op_identity(op):
        # Try common stable identifiers; fall back to repr.
        for attr in ("op_id", "operator_id", "task_id", "id"):
            if hasattr(op, attr):
                try:
                    v = getattr(op, attr)
                    if v is not None:
                        return v
                except Exception:
                    pass
        return repr(op)

    def _pipeline_is_done_or_dead(p):
        if p is None:
            return True
        if p.pipeline_id in s.pipeline_permanent_fail:
            return True
        try:
            st = p.runtime_status()
            return st.is_pipeline_successful()
        except Exception:
            # If status inspection fails, be conservative and keep it.
            return False

    def _get_next_assignable_op(p):
        st = p.runtime_status()
        ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        if not ops:
            return None
        return ops[0]

    def _estimate_ram_gb(op, priority, pool_max_ram, pool_avail_ram, oom_mult):
        # Use estimator if present, otherwise choose a small default.
        est = None
        try:
            est_obj = getattr(op, "estimate", None)
            if est_obj is not None and hasattr(est_obj, "mem_peak_gb"):
                est = est_obj.mem_peak_gb
        except Exception:
            est = None

        if est is None:
            est = 1.0

        safety = s.ram_safety.get(priority, 1.2)
        ram = float(est) * float(safety) * float(oom_mult)

        # Clamp to reasonable bounds.
        if ram < 0.25:
            ram = 0.25
        if ram > pool_max_ram:
            ram = pool_max_ram
        if ram > pool_avail_ram:
            # Caller will treat as "doesn't fit"
            return None
        return ram

    def _choose_cpu(op_priority, pool_max_cpu, pool_avail_cpu):
        cap_frac = s.cpu_cap_frac.get(op_priority, 0.5)
        cpu_cap = max(1.0, float(pool_max_cpu) * float(cap_frac))
        cpu = min(float(pool_avail_cpu), cpu_cap)
        if cpu <= 0:
            return None
        return cpu

    # 1) Ingest new pipelines
    for p in pipelines:
        pid = p.pipeline_id
        if pid in s.pipelines_by_id:
            continue
        s.pipelines_by_id[pid] = p
        _queue_for_priority(p.priority).append(pid)

    # 2) Process results (OOM feedback, permanent failure)
    for r in results:
        if not getattr(r, "failed", lambda: False)():
            continue

        # Identify pipeline_id and op identity if possible.
        pid = getattr(r, "pipeline_id", None)
        if pid is None:
            # Best-effort: many simulators include pipeline_id on result; if not, we can't safely attribute.
            continue

        if _is_oom_error(getattr(r, "error", None)):
            # Increase multiplier for the failed op so the retry requests more RAM.
            try:
                ops = getattr(r, "ops", None) or []
                if ops:
                    op = ops[0]
                    op_key = (pid, _op_identity(op))
                    prev = s.op_oom_multiplier.get(op_key, 1.0)
                    # Geometric backoff, capped (avoid overshooting too wildly).
                    s.op_oom_multiplier[op_key] = min(prev * 1.6, 8.0)
            except Exception:
                pass
        else:
            # Treat non-OOM failures as permanent pipeline failure (incremental, conservative).
            s.pipeline_permanent_fail.add(pid)

    # Cleanup completed/permanently failed pipelines from registry lazily during queue pops.

    # 3) Schedule assignments
    suspensions = []
    assignments = []

    # Determine if high-priority work is waiting (non-empty queues ignoring dead pipelines).
    def _has_waiting_hi():
        # Quick check; we'll tolerate some dead entries.
        return bool(s.q_query) or bool(s.q_interactive)

    hi_waiting = _has_waiting_hi()

    scheduled_pipelines_this_tick = set()

    # Helper to pop-next-fitting candidate from a given priority queue for a given pool
    def _try_schedule_from_queue(pool_id, pool, q, eff_avail_cpu, eff_avail_ram):
        if not q or eff_avail_cpu <= 0 or eff_avail_ram <= 0:
            return None

        n = len(q)
        for _ in range(n):
            pid = q.pop(0)
            p = s.pipelines_by_id.get(pid)

            # Drop dead pipelines
            if _pipeline_is_done_or_dead(p):
                s.pipelines_by_id.pop(pid, None)
                continue

            # Avoid scheduling multiple ops from the same pipeline in the same tick (keeps behavior stable)
            if pid in scheduled_pipelines_this_tick:
                q.append(pid)
                continue

            # Get next op (if none, keep pipeline queued)
            op = _get_next_assignable_op(p)
            if op is None:
                q.append(pid)
                continue

            # Size request
            op_id = _op_identity(op)
            oom_mult = s.op_oom_multiplier.get((pid, op_id), 1.0)

            cpu = _choose_cpu(p.priority, pool.max_cpu_pool, eff_avail_cpu)
            if cpu is None or cpu <= 0:
                q.append(pid)
                continue

            ram = _estimate_ram_gb(op, p.priority, pool.max_ram_pool, eff_avail_ram, oom_mult)
            if ram is None:
                # Doesn't fit in this pool right now; keep it in queue.
                q.append(pid)
                continue

            # Commit: requeue pipeline to allow future ops to be scheduled later.
            q.append(pid)
            scheduled_pipelines_this_tick.add(pid)

            return Assignment(
                ops=[op],
                cpu=cpu,
                ram=ram,
                priority=p.priority,
                pool_id=pool_id,
                pipeline_id=pid,
            )

        return None

    # Iterate pools; for each pool, schedule up to s.max_assignments_per_pool
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]

        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)
        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        for _k in range(int(s.max_assignments_per_pool)):
            if avail_cpu <= 0 or avail_ram <= 0:
                break

            # For BATCH, optionally reserve headroom if any high-priority work is waiting.
            reserve_cpu = float(pool.max_cpu_pool) * float(s.reserve_cpu_frac_for_hi) if hi_waiting else 0.0
            reserve_ram = float(pool.max_ram_pool) * float(s.reserve_ram_frac_for_hi) if hi_waiting else 0.0

            # QUERY first (no reservation limits)
            a = _try_schedule_from_queue(pool_id, pool, s.q_query, avail_cpu, avail_ram)
            if a is None:
                # INTERACTIVE next (no reservation limits)
                a = _try_schedule_from_queue(pool_id, pool, s.q_interactive, avail_cpu, avail_ram)

            if a is None:
                # BATCH last (apply reservation)
                eff_cpu = max(0.0, avail_cpu - reserve_cpu)
                eff_ram = max(0.0, avail_ram - reserve_ram)
                a = _try_schedule_from_queue(pool_id, pool, s.q_batch, eff_cpu, eff_ram)

            if a is None:
                break

            assignments.append(a)
            avail_cpu -= float(a.cpu)
            avail_ram -= float(a.ram)

    return suspensions, assignments
