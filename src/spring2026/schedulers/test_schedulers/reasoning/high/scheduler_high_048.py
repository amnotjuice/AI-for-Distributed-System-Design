# policy_key: scheduler_high_048
# reasoning_effort: high
# exp: reasoning
# model: gpt-5.2-2025-12-11
# llm_cost: 0.158575
# generation_seconds: 113.64
# generated_at: 2026-04-10T07:41:32.875616
@register_scheduler_init(key="scheduler_high_048")
def scheduler_high_048_init(s):
    """
    Priority-aware, low-failure scheduler with:
      - Separate FIFO queues per priority (QUERY > INTERACTIVE > BATCH)
      - SRPT-like tie-break for QUERY/INTERACTIVE (fewest remaining ops first)
      - Conservative RAM sizing + OOM-driven RAM backoff (retry with more RAM)
      - Soft reservation in single-pool setups to protect high-priority latency
      - Multi-pool preference: reserve pool 0 for high-priority when possible
    """
    # Logical time (ticks) for simple aging / bookkeeping
    s._tick = 0

    # Per-priority waiting queues
    s._q_query = []
    s._q_interactive = []
    s._q_batch = []

    # Pipeline metadata: enqueue tick, drop flag, failure counters
    s._pipe_meta = {}  # pipeline_id -> dict

    # Operator-level learned RAM estimates (per pipeline op instance)
    # Keyed by (pipeline_id, op_key) to avoid cross-pipeline contamination.
    s._op_ram_est = {}       # (pid, opk) -> ram
    s._op_fail_count = {}    # (pid, opk) -> int

    # Pipeline-level failure budget (to avoid infinite churn on pathological failures)
    s._pipe_fail_count = {}  # pid -> int

    # Map operator instance to pipeline_id for interpreting ExecutionResult
    s._op_to_pid = {}        # op_key_global -> pid


@register_scheduler(key="scheduler_high_048")
def scheduler_high_048(s, results, pipelines):
    """
    Scheduling step:
      1) Ingest new pipelines into per-priority queues.
      2) Process execution results to update RAM estimates on failures (esp. OOM).
      3) For each pool, place ready operators prioritizing QUERY/INTERACTIVE.
         - If only one pool, keep a soft reserve to prevent BATCH from consuming
           all headroom when high-priority work is waiting.
         - If multiple pools, steer high-priority to pool 0 first; steer BATCH
           away from pool 0 unless no alternatives.
    """
    s._tick += 1

    def _prio_weight(priority):
        if priority == Priority.QUERY:
            return 10
        if priority == Priority.INTERACTIVE:
            return 5
        return 1

    def _prio_rank(priority):
        if priority == Priority.QUERY:
            return 0
        if priority == Priority.INTERACTIVE:
            return 1
        return 2

    def _is_oom_error(err):
        if not err:
            return False
        e = str(err).lower()
        return ("oom" in e) or ("out of memory" in e) or ("memoryerror" in e)

    def _op_key(op):
        # Prefer stable ids if present; fallback to object identity.
        for attr in ("op_id", "operator_id", "id", "name"):
            if hasattr(op, attr):
                try:
                    v = getattr(op, attr)
                    # If "name" is used, include id(op) to disambiguate duplicates
                    if attr == "name":
                        return ("name", str(v), id(op))
                    return (attr, v)
                except Exception:
                    pass
        return ("pyid", id(op))

    def _op_min_ram_hint(op):
        # Best-effort extraction of a "minimum RAM" hint if present.
        for attr in ("min_ram", "ram_min", "min_memory", "mem_min", "min_ram_gb", "min_mem_gb"):
            if hasattr(op, attr):
                try:
                    v = getattr(op, attr)
                    if v is None:
                        continue
                    # If expressed in GB, still treat as simulator RAM units (best-effort)
                    return float(v)
                except Exception:
                    pass
        return None

    def _remaining_ops(pipeline):
        # SRPT-like metric: fewer remaining ops first.
        try:
            total = len(pipeline.values)
        except Exception:
            total = None

        try:
            status = pipeline.runtime_status()
            done = status.state_counts.get(OperatorState.COMPLETED, 0)
            failed = status.state_counts.get(OperatorState.FAILED, 0)
        except Exception:
            done, failed = 0, 0

        if total is None:
            # Fallback: approximate by counting non-completed ops via status if possible
            try:
                status = pipeline.runtime_status()
                pending = status.state_counts.get(OperatorState.PENDING, 0)
                assigned = status.state_counts.get(OperatorState.ASSIGNED, 0)
                running = status.state_counts.get(OperatorState.RUNNING, 0)
                susp = status.state_counts.get(OperatorState.SUSPENDING, 0)
                return pending + assigned + running + susp + failed
            except Exception:
                return 10**9
        return max(0, int(total) - int(done))

    def _pipeline_dropped(pipeline):
        pid = pipeline.pipeline_id
        meta = s._pipe_meta.get(pid)
        return bool(meta and meta.get("drop", False))

    def _pipeline_failed_budget_exceeded(pipeline):
        pid = pipeline.pipeline_id
        pr = pipeline.priority
        # Larger budgets for high-priority to bias completion over early abandonment.
        budget = 6 if pr in (Priority.QUERY, Priority.INTERACTIVE) else 3
        return s._pipe_fail_count.get(pid, 0) >= budget

    def _ensure_pipeline_meta(pipeline):
        pid = pipeline.pipeline_id
        if pid not in s._pipe_meta:
            s._pipe_meta[pid] = {
                "enq": s._tick,
                "drop": False,
                "priority": pipeline.priority,
            }
            s._pipe_fail_count[pid] = 0

    def _enqueue_pipeline(pipeline):
        _ensure_pipeline_meta(pipeline)
        if pipeline.priority == Priority.QUERY:
            s._q_query.append(pipeline)
        elif pipeline.priority == Priority.INTERACTIVE:
            s._q_interactive.append(pipeline)
        else:
            s._q_batch.append(pipeline)

    def _cleanup_queue(q):
        # Remove completed pipelines and those marked dropped.
        out = []
        for p in q:
            if _pipeline_dropped(p):
                continue
            try:
                st = p.runtime_status()
                if st.is_pipeline_successful():
                    continue
            except Exception:
                pass
            out.append(p)
        return out

    def _ready_ops(pipeline, limit=1):
        try:
            st = pipeline.runtime_status()
        except Exception:
            return []
        try:
            ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        except Exception:
            return []
        if not ops:
            return []
        return ops[:limit]

    def _has_ready_work(pipeline):
        return bool(_ready_ops(pipeline, limit=1))

    def _ram_base_fraction(priority):
        # Conservative defaults to reduce OOM probability and avoid 720s penalties.
        if priority == Priority.QUERY:
            return 0.45
        if priority == Priority.INTERACTIVE:
            return 0.35
        return 0.20

    def _cpu_target_fraction(priority):
        # Favor latency for high-priority but keep some sharing.
        if priority == Priority.QUERY:
            return 0.75
        if priority == Priority.INTERACTIVE:
            return 0.55
        return 0.40

    def _pick_pipeline_from_queue(q, prefer_small_remaining=False):
        # Returns (index, pipeline) or (None, None)
        if not q:
            return None, None

        # For QUERY/INTERACTIVE: choose smallest remaining ops among those with ready work.
        if prefer_small_remaining:
            best_i, best_p = None, None
            best_rem = None
            for i, p in enumerate(q):
                if _pipeline_dropped(p):
                    continue
                try:
                    st = p.runtime_status()
                    if st.is_pipeline_successful():
                        continue
                except Exception:
                    pass
                if not _has_ready_work(p):
                    continue
                rem = _remaining_ops(p)
                if best_rem is None or rem < best_rem:
                    best_rem = rem
                    best_i, best_p = i, p
            return best_i, best_p

        # For BATCH: FIFO among those with ready work
        for i, p in enumerate(q):
            if _pipeline_dropped(p):
                continue
            try:
                st = p.runtime_status()
                if st.is_pipeline_successful():
                    continue
            except Exception:
                pass
            if _has_ready_work(p):
                return i, p
        return None, None

    def _estimate_ram(pipeline, op, pool, avail_ram):
        pid = pipeline.pipeline_id
        opk = _op_key(op)
        key = (pid, opk)

        # Start from learned estimate if any; else from hints / pool fraction.
        est = s._op_ram_est.get(key)
        if est is None:
            hint = _op_min_ram_hint(op)
            if hint is not None and hint > 0:
                est = hint
            else:
                est = _ram_base_fraction(pipeline.priority) * float(pool.max_ram_pool)

        # Add small safety margin to reduce repeated OOMs.
        est = float(est) * 1.10

        # Clamp to pool max, and also never exceed currently available RAM.
        est = min(est, float(pool.max_ram_pool), float(avail_ram))
        # Enforce a minimal positive allocation if any RAM is available.
        if est <= 0 and avail_ram > 0:
            est = min(1.0, float(avail_ram))
        return est

    def _estimate_cpu(pipeline, pool, avail_cpu):
        # Keep at least 1 vCPU if possible.
        target = _cpu_target_fraction(pipeline.priority) * float(pool.max_cpu_pool)
        cpu = min(float(avail_cpu), max(1.0, target))
        # Don't allocate fractional CPU if simulator expects ints (best-effort).
        try:
            # If avail_cpu looks int-like, use ints.
            if isinstance(pool.max_cpu_pool, int) or isinstance(avail_cpu, int):
                cpu = int(max(1, int(cpu)))
        except Exception:
            pass
        return cpu

    def _pool_preference_order(priority):
        # If multi-pool: prefer pool 0 for high priority; avoid pool 0 for batch.
        n = s.executor.num_pools
        if n <= 1:
            return [0]
        if priority in (Priority.QUERY, Priority.INTERACTIVE):
            return [0] + list(range(1, n))
        # Batch prefers non-zero pools first
        return list(range(1, n)) + [0]

    # 1) Ingest new pipelines
    for p in pipelines:
        _enqueue_pipeline(p)

    # 2) Process results: update RAM estimates on failures and track failure budgets
    #    Map results back to pipeline via operator keys recorded at scheduling time.
    for r in results:
        if not getattr(r, "ops", None):
            continue
        for op in r.ops:
            opk = _op_key(op)
            pid = None

            # Prefer explicit pipeline_id if present on result (not guaranteed)
            if hasattr(r, "pipeline_id"):
                try:
                    pid = r.pipeline_id
                except Exception:
                    pid = None

            if pid is None:
                pid = s._op_to_pid.get(opk)

            if pid is None:
                continue

            pipe_key = (pid, opk)

            if r.failed():
                # Track pipeline-level failures
                s._pipe_fail_count[pid] = s._pipe_fail_count.get(pid, 0) + 1
                s._op_fail_count[pipe_key] = s._op_fail_count.get(pipe_key, 0) + 1

                # If OOM-like, increase RAM estimate aggressively; else modestly.
                if _is_oom_error(getattr(r, "error", None)):
                    prev = s._op_ram_est.get(pipe_key, getattr(r, "ram", None))
                    if prev is None:
                        prev = 1.0
                    # Exponential backoff to quickly reach feasible RAM.
                    new_est = float(prev) * 2.0
                else:
                    prev = s._op_ram_est.get(pipe_key, getattr(r, "ram", None))
                    if prev is None:
                        prev = 1.0
                    new_est = float(prev) * 1.25

                # Cap at the pool maximum when possible (use result.pool_id if available).
                try:
                    pool = s.executor.pools[r.pool_id]
                    new_est = min(new_est, float(pool.max_ram_pool))
                except Exception:
                    pass

                s._op_ram_est[pipe_key] = new_est

                # Optionally drop hopeless batch pipelines after repeated failures.
                # (Prevents infinite churn that harms weighted latency for QUERY/INTERACTIVE.)
                if s._pipe_fail_count.get(pid, 0) >= 3:
                    # Only auto-drop batch; keep retrying high-priority longer.
                    # We need the pipeline object to check priority; fall back to result.priority.
                    pr = getattr(r, "priority", None)
                    if pr == Priority.BATCH_PIPELINE:
                        meta = s._pipe_meta.get(pid)
                        if meta is not None:
                            meta["drop"] = True
            else:
                # On success, keep learned RAM as-is; optionally decay failure counters.
                if pipe_key in s._op_fail_count and s._op_fail_count[pipe_key] > 0:
                    s._op_fail_count[pipe_key] -= 1

    # 3) Clean up queues (remove completed/dropped)
    s._q_query = _cleanup_queue(s._q_query)
    s._q_interactive = _cleanup_queue(s._q_interactive)
    s._q_batch = _cleanup_queue(s._q_batch)

    # Early exit if nothing new to do
    if not pipelines and not results and not (s._q_query or s._q_interactive or s._q_batch):
        return [], []

    suspensions = []
    assignments = []

    # Helper: determine if any high-priority work is waiting (ready to run)
    def _high_ready_waiting():
        for q in (s._q_query, s._q_interactive):
            for p in q:
                if _pipeline_dropped(p):
                    continue
                if _has_ready_work(p):
                    return True
        return False

    # Soft reserve when single pool: keep headroom for high priority if they are waiting.
    def _reserve_fractions_single_pool():
        # Conservative reservation to protect tail latency for weighted priorities.
        # Applied only when high priority has ready work.
        return 0.30, 0.30  # reserve_cpu_frac, reserve_ram_frac

    # Try to schedule across pools with priority-aware placement
    # Limit assignments per tick to reduce churn and avoid overcommitting in one call.
    max_assignments_total = 8
    max_assignments_per_pool = 3

    high_waiting = _high_ready_waiting()

    # We schedule by iterating pools, but within each pool, we always try higher priority first.
    for pool_id in range(s.executor.num_pools):
        if len(assignments) >= max_assignments_total:
            break

        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool
        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        made = 0
        while made < max_assignments_per_pool and len(assignments) < max_assignments_total:
            # Decide which priority to schedule next on this pool based on pool preference and availability.
            # We attempt QUERY then INTERACTIVE then BATCH, but allow BATCH only if policy permits.
            candidate = None  # (pipeline, op, cpu, ram, priority)

            # Determine effective budget for batch in single-pool when high-priority is waiting.
            reserve_cpu = 0
            reserve_ram = 0
            if s.executor.num_pools == 1 and high_waiting:
                rcf, rrf = _reserve_fractions_single_pool()
                reserve_cpu = rcf * float(pool.max_cpu_pool)
                reserve_ram = rrf * float(pool.max_ram_pool)

            # Try each priority in order, but only if this pool is in its preference order early enough.
            # For multi-pool, this naturally pushes high-priority to pool 0 and batch to others.
            for pr in (Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE):
                # Skip if this pool is a poor choice and there exist other pools; handled implicitly by ordering,
                # but we still allow spillover when no other work can be placed elsewhere.
                pref = _pool_preference_order(pr)
                if pref and pool_id != pref[0]:
                    # Allow spillover only if the preferred pool is not this one AND (a) it's single pool,
                    # or (b) the job is high priority and we need to reduce tail latency by using any headroom.
                    if s.executor.num_pools > 1:
                        if pr == Priority.BATCH_PIPELINE:
                            # Strongly avoid batch on non-preferred pools (esp. pool 0)
                            # unless no other pools exist (handled above).
                            continue

                # Pick from correct queue
                if pr == Priority.QUERY:
                    q = s._q_query
                    prefer_small = True
                elif pr == Priority.INTERACTIVE:
                    q = s._q_interactive
                    prefer_small = True
                else:
                    q = s._q_batch
                    prefer_small = False

                idx, p = _pick_pipeline_from_queue(q, prefer_small_remaining=prefer_small)
                if p is None:
                    continue

                if _pipeline_failed_budget_exceeded(p):
                    # Mark as dropped to prevent repeated churn; affects mostly pathological cases.
                    meta = s._pipe_meta.get(p.pipeline_id)
                    if meta is not None:
                        meta["drop"] = True
                    # Remove it from queue and continue
                    try:
                        q.pop(idx)
                    except Exception:
                        pass
                    continue

                # Identify next ready op
                ops = _ready_ops(p, limit=1)
                if not ops:
                    continue
                op = ops[0]

                # Compute alloc respecting reserve for batch
                eff_avail_cpu = avail_cpu
                eff_avail_ram = avail_ram
                if pr == Priority.BATCH_PIPELINE and s.executor.num_pools == 1 and high_waiting:
                    eff_avail_cpu = max(0, float(avail_cpu) - float(reserve_cpu))
                    eff_avail_ram = max(0, float(avail_ram) - float(reserve_ram))

                # Must have at least minimal capacity
                if eff_avail_cpu <= 0 or eff_avail_ram <= 0:
                    continue

                # Estimate RAM/CPU; avoid scheduling if we can't meet RAM estimate.
                ram = _estimate_ram(p, op, pool, eff_avail_ram)
                if ram <= 0 or ram > eff_avail_ram:
                    continue

                cpu = _estimate_cpu(p, pool, eff_avail_cpu)
                if cpu <= 0 or cpu > eff_avail_cpu:
                    continue

                candidate = (p, op, cpu, ram, pr, q, idx)
                break

            if candidate is None:
                break

            p, op, cpu, ram, pr, q, idx = candidate

            # Create assignment
            assignment = Assignment(
                ops=[op],
                cpu=cpu,
                ram=ram,
                priority=p.priority,
                pool_id=pool_id,
                pipeline_id=p.pipeline_id,
            )
            assignments.append(assignment)

            # Record mapping for result attribution
            opk = _op_key(op)
            s._op_to_pid[opk] = p.pipeline_id
            # Seed RAM estimate if not set, so non-OOM failures can still escalate from a base.
            key = (p.pipeline_id, opk)
            if key not in s._op_ram_est:
                s._op_ram_est[key] = float(ram)

            # Update available resources
            try:
                avail_cpu -= cpu
                avail_ram -= ram
            except Exception:
                # Fallback if types mismatch; cast to float then keep going.
                avail_cpu = float(avail_cpu) - float(cpu)
                avail_ram = float(avail_ram) - float(ram)

            made += 1

            # Rotate pipeline to end of its queue for fairness (even for QUERY/INTERACTIVE)
            # so that a single pipeline doesn't monopolize a pool across ticks.
            try:
                q.pop(idx)
                q.append(p)
            except Exception:
                pass

            if avail_cpu <= 0 or avail_ram <= 0:
                break

    return suspensions, assignments
