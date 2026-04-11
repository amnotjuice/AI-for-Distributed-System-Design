# policy_key: scheduler_est_010
# reasoning_effort: low
# model: gpt-5.2-2025-12-11
# llm_cost: 0.043952
# generation_seconds: 46.18
# generated_at: 2026-03-31T23:51:49.026840
@register_scheduler_init(key="scheduler_est_010")
def scheduler_est_010_init(s):
    """Priority-aware, estimate-based scheduler with basic OOM retry.

    Incremental improvements over naive FIFO:
      1) Maintain separate waiting queues per priority and always schedule higher priority first.
      2) Use per-operator memory estimate as a *floor* to reduce OOMs (extra RAM is "free").
      3) Retry suspected OOM failures by increasing RAM (exponential backoff) instead of dropping the pipeline.
      4) Keep some headroom for high-priority work by soft-reserving resources in each pool.
      5) Fill a pool with multiple assignments per tick (not just one), improving latency/throughput under load.
    """
    # Per-priority FIFO queues of pipeline_ids
    s.wait_q = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }

    # Registry of live pipelines by id (so queues only store ids)
    s.pipelines_by_id = {}

    # Per-operator RAM override after OOM/backoff: key=(pipeline_id, op_key) -> ram_gb
    s.op_ram_override_gb = {}

    # Simple aging counter so batch eventually runs even under sustained interactive load
    s.batch_starve_ticks = 0

    # Logical ticks (best-effort)
    s.tick = 0


@register_scheduler(key="scheduler_est_010")
def scheduler_est_010_scheduler(s, results, pipelines):
    """
    Policy:
      - Admit new pipelines into a per-priority FIFO.
      - On each scheduling step, for each pool:
          - compute available CPU/RAM and enforce a soft reservation:
              keep some headroom unless we're scheduling high priority.
          - repeatedly pick next runnable operator from highest-priority pipeline available
            (with aging to prevent indefinite batch starvation).
          - allocate:
              RAM = min(avail_ram, max(pool_max, ...)) with floor=max(estimate, override, small_min)
              CPU = give more CPU to higher priority; cap batch to leave headroom
      - On suspected OOM failure: retry same op with increased RAM (exponential backoff).
    """
    # ---- helpers (kept inside to avoid imports/global deps) ----
    def _prio_rank(prio):
        # Lower is more important in our internal ordering
        if prio == Priority.QUERY:
            return 0
        if prio == Priority.INTERACTIVE:
            return 1
        return 2  # batch

    def _is_suspected_oom(err):
        if err is None:
            return False
        msg = str(err).lower()
        return ("oom" in msg) or ("out of memory" in msg) or ("memory" in msg and "exceed" in msg)

    def _op_key(op):
        # Best-effort stable key without assuming schema; fallback to object identity.
        for attr in ("op_id", "operator_id", "id", "name"):
            if hasattr(op, attr):
                try:
                    v = getattr(op, attr)
                    if v is not None:
                        return (attr, str(v))
                except Exception:
                    pass
        return ("pyid", str(id(op)))

    def _enqueue_pipeline(p):
        pid = p.pipeline_id
        s.pipelines_by_id[pid] = p
        # Avoid duplicate queue entries
        for q in s.wait_q.values():
            if pid in q:
                return
        s.wait_q[p.priority].append(pid)

    def _dequeue_existing(pid):
        # Remove from any queue if present
        for q in s.wait_q.values():
            try:
                q.remove(pid)
            except ValueError:
                pass

    def _pipeline_done_or_failed_hard(p):
        st = p.runtime_status()
        if st.is_pipeline_successful():
            return True
        # We treat failures as potentially retryable; we don't "hard fail" the pipeline here.
        return False

    def _get_next_runnable_op(p):
        st = p.runtime_status()
        ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        if not ops:
            return None
        # Keep it simple: first runnable op in the pipeline's ordering.
        return ops[0]

    def _ram_floor_for_op(p, op, pool_max_ram):
        # Base minimum so tiny estimates don't cause immediate OOM due to overhead.
        base_min = 0.5  # GB
        est = None
        try:
            est = getattr(getattr(op, "estimate", None), "mem_peak_gb", None)
        except Exception:
            est = None

        override = s.op_ram_override_gb.get((p.pipeline_id, _op_key(op)), None)

        floor = base_min
        if est is not None:
            try:
                floor = max(floor, float(est))
            except Exception:
                pass
        if override is not None:
            try:
                floor = max(floor, float(override))
            except Exception:
                pass

        # Never exceed pool max
        return min(floor, pool_max_ram)

    def _cpu_target_for_prio(prio, avail_cpu, pool_max_cpu):
        # Give high priority more CPU to minimize latency; cap batch to keep headroom.
        if avail_cpu <= 0:
            return 0
        if prio == Priority.QUERY:
            # Try to run fast: up to all available (but not above pool max)
            return min(avail_cpu, pool_max_cpu)
        if prio == Priority.INTERACTIVE:
            # Good chunk, but avoid monopolizing the pool if others need to run
            return min(avail_cpu, max(1.0, 0.75 * pool_max_cpu))
        # Batch: smaller chunks improve coexistence and reduce preemption need
        return min(avail_cpu, max(1.0, 0.50 * pool_max_cpu))

    def _resources_for_prio_in_pool(prio, pool):
        # Soft reservation: keep headroom for higher priority arrivals.
        # If we're scheduling high priority, allow it to use full available.
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool

        # Reservation fractions tuned conservatively for latency protection.
        # QUERY uses everything; INTERACTIVE leaves some; BATCH leaves more.
        if prio == Priority.QUERY:
            return avail_cpu, avail_ram

        # Reserve a slice for QUERY (and for INTERACTIVE if we're batch).
        reserve_cpu_for_query = 0.15 * pool.max_cpu_pool
        reserve_ram_for_query = 0.15 * pool.max_ram_pool

        if prio == Priority.INTERACTIVE:
            cpu_budget = max(0.0, avail_cpu - reserve_cpu_for_query)
            ram_budget = max(0.0, avail_ram - reserve_ram_for_query)
            return cpu_budget, ram_budget

        # Batch: reserve for both QUERY and INTERACTIVE
        reserve_cpu_total = 0.35 * pool.max_cpu_pool
        reserve_ram_total = 0.35 * pool.max_ram_pool
        cpu_budget = max(0.0, avail_cpu - reserve_cpu_total)
        ram_budget = max(0.0, avail_ram - reserve_ram_total)
        return cpu_budget, ram_budget

    def _pick_next_pipeline_id():
        # Aging: if batch has been starved for long, occasionally let it run.
        # We consider "starved" if there exists batch work but we keep scheduling higher prio.
        if s.wait_q[Priority.BATCH_PIPELINE] and s.batch_starve_ticks >= 8:
            # Permit one batch selection, then reset.
            pid = s.wait_q[Priority.BATCH_PIPELINE].pop(0)
            s.batch_starve_ticks = 0
            return pid

        # Normal: strict priority order
        for prio in (Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE):
            if s.wait_q[prio]:
                return s.wait_q[prio].pop(0)
        return None

    # ---- incorporate new arrivals ----
    for p in pipelines:
        _enqueue_pipeline(p)

    # ---- process results (OOM retry bookkeeping) ----
    # We rely on results to detect OOM-like failures and increase RAM override for that operator.
    for r in results:
        if not getattr(r, "failed", lambda: False)():
            continue

        # If we can't attribute ops, we can't set a per-op override reliably.
        if not getattr(r, "ops", None):
            continue

        if not _is_suspected_oom(getattr(r, "error", None)):
            # Unknown failure: don't keep retrying blindly; leave as-is (pipeline may remain failed).
            continue

        # Increase RAM for each failed op (best-effort). Exponential backoff with cap.
        for op in r.ops:
            # We need pipeline_id; ExecutionResult doesn't guarantee it, so infer from op if possible.
            # If op carries pipeline_id, use it; otherwise, skip.
            pid = None
            for attr in ("pipeline_id", "pipeline", "dag_id"):
                if hasattr(op, attr):
                    try:
                        pid = getattr(op, attr)
                    except Exception:
                        pid = None
                    if pid is not None:
                        break
            if pid is None:
                # Fall back: cannot map, skip
                continue

            # Backoff from last assigned RAM (or estimate) toward pool max (unknown here: use r.ram as baseline).
            key = (pid, _op_key(op))
            prev = s.op_ram_override_gb.get(key, None)
            baseline = prev if prev is not None else getattr(r, "ram", None)
            try:
                baseline = float(baseline) if baseline is not None else 1.0
            except Exception:
                baseline = 1.0
            s.op_ram_override_gb[key] = max(baseline * 2.0, baseline + 1.0)

            # Ensure pipeline is still enqueued for retry if we still track it
            p_obj = s.pipelines_by_id.get(pid, None)
            if p_obj is not None:
                _enqueue_pipeline(p_obj)

    # ---- scheduling ----
    # Early exit if nothing changed
    if not pipelines and not results:
        s.tick += 1
        return [], []

    suspensions = []
    assignments = []

    # Age starvation counter: if there is batch work waiting and we are likely to schedule higher prio, increment.
    if s.wait_q[Priority.BATCH_PIPELINE] and (s.wait_q[Priority.QUERY] or s.wait_q[Priority.INTERACTIVE]):
        s.batch_starve_ticks += 1
    else:
        s.batch_starve_ticks = max(0, s.batch_starve_ticks - 1)

    # Per-pool scheduling: fill capacity with multiple assignments
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]

        # We try several picks per pool, but avoid infinite loops if nothing runnable.
        picks_without_progress = 0
        max_picks = 64

        while picks_without_progress < max_picks:
            pid = _pick_next_pipeline_id()
            if pid is None:
                break

            p = s.pipelines_by_id.get(pid, None)
            if p is None:
                picks_without_progress += 1
                continue

            # Drop completed pipelines from tracking
            if _pipeline_done_or_failed_hard(p):
                _dequeue_existing(pid)
                s.pipelines_by_id.pop(pid, None)
                picks_without_progress += 1
                continue

            op = _get_next_runnable_op(p)
            if op is None:
                # Not runnable yet; requeue and move on.
                _enqueue_pipeline(p)
                picks_without_progress += 1
                continue

            # Compute prio-specific budgets in this pool
            cpu_budget, ram_budget = _resources_for_prio_in_pool(p.priority, pool)
            if cpu_budget <= 0 or ram_budget <= 0:
                # Can't schedule in this pool now; requeue and stop trying this pool.
                _enqueue_pipeline(p)
                break

            # Determine RAM and CPU for this op
            ram_floor = _ram_floor_for_op(p, op, pool.max_ram_pool)
            if ram_floor > ram_budget:
                # Not enough RAM budget in this pool; requeue and stop trying this pool.
                _enqueue_pipeline(p)
                break

            cpu_target = _cpu_target_for_prio(p.priority, cpu_budget, pool.max_cpu_pool)
            if cpu_target <= 0:
                _enqueue_pipeline(p)
                break

            # Allocate RAM generously: give the whole remaining RAM budget to reduce future OOM risk.
            # (Extra RAM is free; this reduces chance of under-allocation due to estimate error.)
            ram_alloc = min(ram_budget, pool.avail_ram_pool)
            ram_alloc = max(ram_floor, ram_alloc)

            # Keep CPU within actual available
            cpu_alloc = min(cpu_target, pool.avail_cpu_pool)

            # If after max/min we still can't fit, requeue and stop
            if cpu_alloc <= 0 or ram_alloc <= 0 or ram_alloc > pool.avail_ram_pool:
                _enqueue_pipeline(p)
                break

            assignment = Assignment(
                ops=[op],
                cpu=cpu_alloc,
                ram=ram_alloc,
                priority=p.priority,
                pool_id=pool_id,
                pipeline_id=p.pipeline_id,
            )
            assignments.append(assignment)

            # Requeue pipeline for its next op (or to wait on dependencies)
            _enqueue_pipeline(p)

            # Reset progress counter since we successfully scheduled something
            picks_without_progress = 0

        # end while picks
    # end pools

    s.tick += 1
    return suspensions, assignments
