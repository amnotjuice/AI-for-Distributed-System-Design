# policy_key: scheduler_high_040
# reasoning_effort: high
# exp: reasoning
# model: gpt-5.2-2025-12-11
# llm_cost: 0.152891
# generation_seconds: 120.50
# generated_at: 2026-04-10T03:06:18.584037
@register_scheduler_init(key="scheduler_high_040")
def scheduler_high_040_init(s):
    """
    Priority-weighted, failure-averse scheduler.

    Core ideas:
      - Strictly prioritize QUERY > INTERACTIVE > BATCH, with anti-starvation for BATCH.
      - Allocate "generous" initial RAM (esp. for QUERY/INTERACTIVE) to avoid OOM/failures (720s penalty).
      - Learn per-operator RAM hints from failures (exponential backoff) and retry.
      - Pack multiple operators per pool per tick using per-op CPU/RAM sizing (instead of "use all resources").
      - Soft pool partitioning when multiple pools exist: a "high-priority" pool prefers QUERY/INTERACTIVE.
    """
    s._tick = 0

    # Track active pipelines by id
    s._pipelines = {}  # pipeline_id -> Pipeline
    s._meta = {}  # pipeline_id -> dict(first_seen, last_scheduled_tick)

    # Per-operator learned sizing hints (keyed by operator object identity when possible)
    s._op_ram_hint = {}     # op_key -> ram
    s._op_fail_count = {}   # op_key -> count

    # Anti-starvation for batch
    s._last_batch_scheduled_tick = -10**9
    s._batch_starvation_threshold = 25  # ticks without scheduling any batch => force one if possible

    # Failure handling
    s._max_op_retries = 4

    # Cache pool "roles" (recomputed lazily if pool count changes)
    s._pool_roles = None  # list[str] of length num_pools: "high" or "general"


@register_scheduler(key="scheduler_high_040")
def scheduler_high_040(s, results, pipelines):
    """
    Scheduling step.

    Returns:
      suspensions: (unused by default; we avoid preemption to minimize churn)
      assignments: list of Assignment(ops=[op], cpu=..., ram=..., priority=..., pool_id=..., pipeline_id=...)
    """
    def _ensure_pool_roles():
        n = s.executor.num_pools
        if s._pool_roles is not None and len(s._pool_roles) == n:
            return
        # If multiple pools, reserve pool 0 as "high" (query/interactive preferred); rest are "general".
        if n <= 1:
            s._pool_roles = ["general"]
        else:
            s._pool_roles = ["high"] + ["general"] * (n - 1)

    def _priority_weight(pri):
        if pri == Priority.QUERY:
            return 10.0
        if pri == Priority.INTERACTIVE:
            return 5.0
        return 1.0

    def _priority_class_rank(pri):
        # Lower is higher priority
        if pri == Priority.QUERY:
            return 0
        if pri == Priority.INTERACTIVE:
            return 1
        return 2

    def _assignable_states():
        # Prefer provided constant if present; otherwise fall back.
        try:
            return ASSIGNABLE_STATES
        except NameError:
            return [OperatorState.PENDING, OperatorState.FAILED]

    def _op_key(op):
        # Try stable identifiers if present; otherwise fall back to object identity.
        for attr in ("operator_id", "op_id", "id", "name"):
            if hasattr(op, attr):
                try:
                    v = getattr(op, attr)
                    if isinstance(v, (str, int)):
                        return (attr, v)
                except Exception:
                    pass
        return ("obj", id(op))

    def _status_counts(status, state):
        try:
            sc = getattr(status, "state_counts", None)
            if isinstance(sc, dict):
                return int(sc.get(state, 0))
        except Exception:
            pass
        return 0

    def _pipeline_remaining_ops(pipeline, status):
        # Approximate remaining ops by total - completed.
        total = 0
        try:
            total = len(getattr(pipeline, "values", []) or [])
        except Exception:
            total = 0
        completed = _status_counts(status, OperatorState.COMPLETED)
        rem = max(1, total - completed) if total > 0 else 1
        return rem

    def _is_oom_error(err):
        if err is None:
            return False
        try:
            msg = str(err).lower()
        except Exception:
            return False
        return ("oom" in msg) or ("out of memory" in msg) or ("killed" in msg and "mem" in msg)

    def _initial_ram_fraction(pri):
        # Conservative to avoid OOM failures for high-priority (heavy penalty).
        if pri == Priority.QUERY:
            return 0.55
        if pri == Priority.INTERACTIVE:
            return 0.40
        return 0.25

    def _initial_cpu_fraction(pri):
        # Give more CPU to reduce query/interactive latency; keep batch smaller for better packing.
        if pri == Priority.QUERY:
            return 0.75
        if pri == Priority.INTERACTIVE:
            return 0.55
        return 0.30

    def _clamp(x, lo, hi):
        if x < lo:
            return lo
        if x > hi:
            return hi
        return x

    def _compute_op_ram_hint(op, pri, pool):
        k = _op_key(op)
        if k in s._op_ram_hint:
            return float(s._op_ram_hint[k])
        return float(pool.max_ram_pool) * _initial_ram_fraction(pri)

    def _update_op_hints_from_failure(res):
        # Exponential RAM backoff on failure, with special care for OOM-like errors.
        oom = _is_oom_error(getattr(res, "error", None))
        for op in getattr(res, "ops", []) or []:
            k = _op_key(op)
            s._op_fail_count[k] = int(s._op_fail_count.get(k, 0)) + 1

            prev = float(s._op_ram_hint.get(k, max(1.0, float(getattr(res, "ram", 1.0)))))
            used = float(getattr(res, "ram", prev))
            # If OOM, more aggressive; otherwise still back off a bit (unknown failure type).
            factor = 2.0 if oom else 1.5
            bumped = max(prev * factor, used * factor, prev + 1.0)
            s._op_ram_hint[k] = bumped

    def _eligible_pipeline(p):
        # Keep pipelines in dict; exclude those fully successful.
        try:
            st = p.runtime_status()
            if st.is_pipeline_successful():
                return False
        except Exception:
            # If status unavailable, keep it eligible to avoid dropping.
            return True
        return True

    def _get_ready_op(p, status):
        ops = status.get_ops(_assignable_states(), require_parents_complete=True)
        if not ops:
            return None
        # Prefer PENDING over FAILED when both are ready, to reduce repeated retries.
        # (FAILED ops will still be retried with larger RAM hints.)
        try:
            pending_ops = status.get_ops([OperatorState.PENDING], require_parents_complete=True)
            if pending_ops:
                return pending_ops[0]
        except Exception:
            pass
        return ops[0]

    def _pipeline_urgency(p, status, pool_role):
        pri = getattr(p, "priority", Priority.BATCH_PIPELINE)
        w = _priority_weight(pri)
        rem = float(_pipeline_remaining_ops(p, status))

        meta = s._meta.get(p.pipeline_id, {})
        first_seen = int(meta.get("first_seen", s._tick))
        last_sched = int(meta.get("last_scheduled", first_seen))
        wait = max(0, s._tick - last_sched)

        # Base: weighted "short remaining" heuristic.
        score = (w / rem)

        # Aging: prevent indefinite starvation; stronger for batch to avoid 720s incomplete penalty.
        if pri == Priority.BATCH_PIPELINE:
            score += 0.02 * min(wait, 200)
        else:
            score += 0.01 * min(wait, 200)

        # Pool role bias: in "high" pool, suppress batch unless nothing else runnable.
        if pool_role == "high" and pri == Priority.BATCH_PIPELINE:
            score *= 0.35

        # If pipeline has FAILED ops ready, slight boost to get it unstuck (but avoid thrash via retry limit).
        failed_count = _status_counts(status, OperatorState.FAILED)
        if failed_count > 0:
            score *= 1.10

        return score

    def _op_retry_exhausted(op):
        k = _op_key(op)
        return int(s._op_fail_count.get(k, 0)) >= int(s._max_op_retries)

    def _pick_next_pipeline(candidates, pool_role, force_batch=False):
        best = None
        best_score = None

        for p in candidates:
            pri = getattr(p, "priority", Priority.BATCH_PIPELINE)
            if force_batch and pri != Priority.BATCH_PIPELINE:
                continue

            try:
                st = p.runtime_status()
            except Exception:
                continue

            op = _get_ready_op(p, st)
            if op is None:
                continue
            if _op_retry_exhausted(op):
                # Skip hopeless op retries to avoid churn.
                continue

            # In the "high" pool, if any query/interactive runnable exist, ignore batch.
            if pool_role == "high" and pri == Priority.BATCH_PIPELINE:
                # We'll handle "no high-pri runnable" by allowing batch later.
                pass

            score = _pipeline_urgency(p, st, pool_role)
            if best is None or score > best_score:
                best = p
                best_score = score

        return best

    # --- Tick bookkeeping ---
    s._tick += 1
    _ensure_pool_roles()

    # --- Ingest new pipelines ---
    for p in pipelines or []:
        s._pipelines[p.pipeline_id] = p
        if p.pipeline_id not in s._meta:
            s._meta[p.pipeline_id] = {
                "first_seen": s._tick,
                "last_scheduled": s._tick - 1,
            }

    # --- Process results: learn from failures to reduce future 720s penalties ---
    for res in results or []:
        try:
            if res.failed():
                _update_op_hints_from_failure(res)
        except Exception:
            pass

    # --- Cleanup successful pipelines from active set ---
    for pid, p in list(s._pipelines.items()):
        try:
            st = p.runtime_status()
            if st.is_pipeline_successful():
                del s._pipelines[pid]
        except Exception:
            # Keep unknown status pipelines active.
            pass

    suspensions = []  # We avoid preemption by default to reduce wasted work/churn.
    assignments = []

    # Candidates list for this tick
    active = [p for p in s._pipelines.values() if _eligible_pipeline(p)]

    # Track which pipelines were already scheduled this tick (avoid over-parallelizing a single pipeline).
    scheduled_this_tick = set()

    # Determine whether to force a batch op due to starvation.
    batch_starved = (s._tick - s._last_batch_scheduled_tick) >= int(s._batch_starvation_threshold)

    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        pool_role = s._pool_roles[pool_id] if s._pool_roles else "general"

        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)

        if avail_cpu <= 0.0 or avail_ram <= 0.0:
            continue

        # In "high" pool, first check if any query/interactive is runnable; if yes, don't run batch there.
        high_runnable_exists = False
        if pool_role == "high":
            for p in active:
                if p.pipeline_id in scheduled_this_tick:
                    continue
                pri = getattr(p, "priority", Priority.BATCH_PIPELINE)
                if pri == Priority.BATCH_PIPELINE:
                    continue
                try:
                    st = p.runtime_status()
                    op = _get_ready_op(p, st)
                    if op is not None and not _op_retry_exhausted(op):
                        high_runnable_exists = True
                        break
                except Exception:
                    continue

        # Pack as many ops as we can fit into this pool this tick.
        # Keep a small headroom to reduce fragmentation.
        max_ops_this_pool = 8
        ops_scheduled = 0

        while ops_scheduled < max_ops_this_pool and avail_cpu > 0.0 and avail_ram > 0.0:
            # Force one batch op if batch has starved (prefer general pools; allow high pool only if no high runnable).
            force_batch = batch_starved and (pool_role != "high" or not high_runnable_exists)

            # Only consider pipelines not yet scheduled this tick.
            candidates = [p for p in active if p.pipeline_id not in scheduled_this_tick]

            if not candidates:
                break

            # In high pool, if any high runnable exists, don't force/consider batch.
            if pool_role == "high" and high_runnable_exists and force_batch:
                force_batch = False

            chosen = _pick_next_pipeline(candidates, pool_role, force_batch=force_batch)

            # If none chosen under force_batch, relax (so we don't idle).
            if chosen is None and force_batch:
                chosen = _pick_next_pipeline(candidates, pool_role, force_batch=False)

            if chosen is None:
                break

            try:
                st = chosen.runtime_status()
            except Exception:
                # Can't schedule without status.
                scheduled_this_tick.add(chosen.pipeline_id)
                continue

            op = _get_ready_op(chosen, st)
            if op is None or _op_retry_exhausted(op):
                scheduled_this_tick.add(chosen.pipeline_id)
                continue

            pri = getattr(chosen, "priority", Priority.BATCH_PIPELINE)

            # Compute RAM/CPU request using learned hints and priority-based fractions.
            ram_hint = _compute_op_ram_hint(op, pri, pool)

            # If this op failed before, bias toward running it with larger RAM to avoid repeated 720-penalty failures.
            k = _op_key(op)
            fails = int(s._op_fail_count.get(k, 0))
            if fails > 0:
                ram_hint *= (1.0 + 0.15 * min(fails, 4))

            # Clamp to pool bounds.
            ram_req = _clamp(ram_hint, 1.0, float(pool.max_ram_pool))
            cpu_req = float(pool.max_cpu_pool) * _initial_cpu_fraction(pri)
            cpu_req = _clamp(cpu_req, 1.0, float(pool.max_cpu_pool))

            # Fit to available resources; if we can't fit, try smaller CPU first (RAM is more failure-sensitive).
            if ram_req > avail_ram:
                # If we can't meet hint, only run if we can still allocate a meaningful fraction,
                # otherwise defer to avoid likely OOM.
                if avail_ram < max(1.0, 0.6 * ram_req):
                    # Mark as not scheduled so another pipeline may fit; but avoid tight loops.
                    scheduled_this_tick.add(chosen.pipeline_id)
                    continue
                ram_req = avail_ram

            if cpu_req > avail_cpu:
                # Reduce CPU to fit; better to run with fewer CPU than to wait.
                cpu_req = avail_cpu
                if cpu_req < 1.0:
                    break

            # Create assignment
            assignments.append(
                Assignment(
                    ops=[op],
                    cpu=cpu_req,
                    ram=ram_req,
                    priority=pri,
                    pool_id=pool_id,
                    pipeline_id=chosen.pipeline_id,
                )
            )

            # Update accounting
            avail_cpu -= cpu_req
            avail_ram -= ram_req
            ops_scheduled += 1

            scheduled_this_tick.add(chosen.pipeline_id)
            if chosen.pipeline_id in s._meta:
                s._meta[chosen.pipeline_id]["last_scheduled"] = s._tick

            if pri == Priority.BATCH_PIPELINE:
                s._last_batch_scheduled_tick = s._tick
                batch_starved = False  # satisfied

            # Recompute high runnable existence after scheduling in high pool (optional; keep simple)
            if pool_role == "high":
                high_runnable_exists = False
                for p in active:
                    if p.pipeline_id in scheduled_this_tick:
                        continue
                    ppri = getattr(p, "priority", Priority.BATCH_PIPELINE)
                    if ppri == Priority.BATCH_PIPELINE:
                        continue
                    try:
                        pst = p.runtime_status()
                        pop = _get_ready_op(p, pst)
                        if pop is not None and not _op_retry_exhausted(pop):
                            high_runnable_exists = True
                            break
                    except Exception:
                        continue

    return suspensions, assignments
