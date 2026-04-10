# policy_key: scheduler_est_033
# reasoning_effort: low
# model: gpt-5.2-2025-12-11
# llm_cost: 0.035062
# generation_seconds: 45.37
# generated_at: 2026-04-01T00:06:47.899509
@register_scheduler_init(key="scheduler_est_033")
def scheduler_est_033_init(s):
    """Priority-aware, estimate-aware FIFO with small, incremental improvements over naive FIFO.

    Improvements (kept intentionally simple / low-risk):
      1) Priority ordering: QUERY > INTERACTIVE > BATCH_PIPELINE.
      2) Resource reservation: keep a small headroom slice for high-priority work by
         refusing to start batch when remaining resources are below a reserved threshold.
      3) Right-sized allocations: avoid giving the entire pool to a single op; allocate
         a capped CPU share (larger cap for high priority) to improve latency/parallelism.
      4) OOM-avoidance retries: if an op fails (likely OOM), increase its RAM floor using
         estimate + exponential backoff and retry (ASSIGNABLE includes FAILED).
    """
    from collections import deque

    # Per-priority queues of pipelines (FIFO within each class)
    s.q_query = deque()
    s.q_interactive = deque()
    s.q_batch = deque()

    # Track pipelines currently known to avoid enqueue duplicates
    s.known_pipelines = {}  # pipeline_id -> Pipeline

    # Track per-(pipeline, op) retry state (RAM multipliers)
    # key: (pipeline_id, op_key) -> {"retries": int, "ram_floor_gb": float}
    s.op_retry = {}

    # Minimal caps and defaults (conservative; can be tuned in later iterations)
    s.min_cpu = 1.0
    s.min_ram_gb = 0.5

    # CPU caps as fraction of pool capacity (avoid single-op monopolization)
    s.cpu_cap_frac_query = 0.75
    s.cpu_cap_frac_interactive = 0.60
    s.cpu_cap_frac_batch = 0.35

    # Reserved headroom thresholds (fraction of pool max) to protect latency bursts
    s.reserve_frac_cpu = 0.20
    s.reserve_frac_ram = 0.20

    # Lightweight anti-starvation: after N high-priority assignments, try one batch if possible
    s.hp_quota = 6
    s.hp_used = 0

    # A simple tick counter for potential future aging logic
    s.ticks = 0


@register_scheduler(key="scheduler_est_033")
def scheduler_est_033_scheduler(s, results, pipelines):
    """
    Priority-aware scheduling across pools with simple OOM-avoidance retries.

    Key behaviors:
      - Enqueue new pipelines by priority class.
      - On failures, increase RAM floor for the specific operator and retry.
      - For each pool, schedule at most one operator per tick (like the baseline),
        but:
          * choose higher-priority pipelines first,
          * avoid starting batch if that would consume reserved headroom,
          * allocate CPU/RAM with caps and memory estimate floors.
    """
    from collections import deque

    # --- Helper functions (defined locally to keep policy self-contained) ---
    def _prio_rank(prio):
        # Higher number => higher priority
        if prio == Priority.QUERY:
            return 3
        if prio == Priority.INTERACTIVE:
            return 2
        return 1  # Priority.BATCH_PIPELINE and any others

    def _queue_for_priority(prio):
        if prio == Priority.QUERY:
            return s.q_query
        if prio == Priority.INTERACTIVE:
            return s.q_interactive
        return s.q_batch

    def _op_key(op):
        # Best-effort stable key across retries; fall back to object id.
        return getattr(op, "op_id", getattr(op, "operator_id", getattr(op, "id", id(op))))

    def _est_mem_floor_gb(op):
        # Use estimate as a RAM floor; extra RAM is "free" (no perf cost).
        est = None
        try:
            est = getattr(getattr(op, "estimate", None), "mem_peak_gb", None)
        except Exception:
            est = None
        if est is None:
            return s.min_ram_gb
        try:
            # Guard against invalid estimates
            est = float(est)
            if est <= 0:
                return s.min_ram_gb
            return max(s.min_ram_gb, est)
        except Exception:
            return s.min_ram_gb

    def _cpu_cap_frac_for_priority(prio):
        if prio == Priority.QUERY:
            return s.cpu_cap_frac_query
        if prio == Priority.INTERACTIVE:
            return s.cpu_cap_frac_interactive
        return s.cpu_cap_frac_batch

    def _reserved_thresholds(pool):
        return (pool.max_cpu_pool * s.reserve_frac_cpu, pool.max_ram_pool * s.reserve_frac_ram)

    def _should_block_batch_due_to_reserve(pool, avail_cpu, avail_ram):
        # If we're below reserved headroom, don't start batch work.
        res_cpu, res_ram = _reserved_thresholds(pool)
        return (avail_cpu <= res_cpu) or (avail_ram <= res_ram)

    def _enqueue_pipeline(p):
        # Avoid duplicates; keep the most recent object reference.
        pid = p.pipeline_id
        if pid in s.known_pipelines:
            return
        s.known_pipelines[pid] = p
        _queue_for_priority(p.priority).append(p)

    def _drop_pipeline(pid):
        # Remove from known; queue removal is lazy (we skip stale entries).
        if pid in s.known_pipelines:
            del s.known_pipelines[pid]

    def _pipeline_is_done_or_dead(p):
        status = p.runtime_status()
        if status.is_pipeline_successful():
            return True
        # We do not permanently drop failures here; we allow retries because FAILED is assignable.
        return False

    def _next_candidate_pipeline(allow_batch=True):
        # High-priority first; optionally block batch.
        if s.q_query:
            return s.q_query
        if s.q_interactive:
            return s.q_interactive
        if allow_batch and s.q_batch:
            return s.q_batch
        return None

    def _pick_one_assignable_op(p):
        status = p.runtime_status()
        ops = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        if not ops:
            return None
        return ops[0]

    def _ram_floor_for_op(pipeline_id, op):
        base = _est_mem_floor_gb(op)
        key = (pipeline_id, _op_key(op))
        st = s.op_retry.get(key)
        if not st:
            return base
        # Ensure we never go below our learned floor.
        return max(base, st.get("ram_floor_gb", base))

    def _record_failure_and_bump_ram(pipeline_id, op, last_ram):
        key = (pipeline_id, _op_key(op))
        st = s.op_retry.get(key)
        if not st:
            st = {"retries": 0, "ram_floor_gb": _est_mem_floor_gb(op)}
        st["retries"] = int(st.get("retries", 0)) + 1

        # Exponential backoff on RAM floor, anchored at max(estimate, last_ram).
        # Use mild growth to avoid overreacting.
        anchor = max(float(last_ram or 0.0), _est_mem_floor_gb(op))
        bump = max(anchor * (1.5 ** min(st["retries"], 6)), anchor + 0.5)
        st["ram_floor_gb"] = bump
        s.op_retry[key] = st

    # --- Update tick ---
    if pipelines or results:
        s.ticks += 1

    # --- Ingest new pipelines ---
    for p in pipelines:
        _enqueue_pipeline(p)

    # --- Process results (learn from failures) ---
    for r in results:
        if getattr(r, "failed", None) and r.failed():
            # Best-effort: bump RAM for each failed op in this result.
            # If the failure isn't OOM, this is still a conservative hedge.
            try:
                for op in getattr(r, "ops", []) or []:
                    _record_failure_and_bump_ram(
                        pipeline_id=getattr(r, "pipeline_id", None),
                        op=op,
                        last_ram=getattr(r, "ram", None),
                    )
            except Exception:
                # If ops are missing/unavailable, do nothing.
                pass

    # Early exit if nothing to do
    if not pipelines and not results and not (s.q_query or s.q_interactive or s.q_batch):
        return [], []

    suspensions = []
    assignments = []

    # --- Main scheduling loop: at most 1 assignment per pool per tick ---
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)
        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # Decide whether to allow batch work in this pool given headroom.
        allow_batch = not _should_block_batch_due_to_reserve(pool, avail_cpu, avail_ram)

        # Anti-starvation: if we've used lots of high-priority recently, attempt a batch,
        # but only if headroom allows it.
        if allow_batch and s.hp_used >= s.hp_quota and s.q_batch:
            chosen_queue = s.q_batch
        else:
            chosen_queue = _next_candidate_pipeline(allow_batch=allow_batch)

        if chosen_queue is None:
            continue

        # Pop until we find a viable pipeline entry (lazy cleanup).
        # Keep bounded work per tick per pool.
        tries = 0
        max_tries = 20
        chosen_pipeline = None
        while chosen_queue and tries < max_tries:
            tries += 1
            p = chosen_queue.popleft()

            # Skip stale duplicates (pipeline removed from known set).
            if p.pipeline_id not in s.known_pipelines:
                continue

            # Drop successful pipelines.
            if _pipeline_is_done_or_dead(p):
                _drop_pipeline(p.pipeline_id)
                continue

            # Try to find an assignable op.
            op = _pick_one_assignable_op(p)
            if op is None:
                # Not ready; requeue to preserve FIFO-ish behavior.
                chosen_queue.append(p)
                continue

            chosen_pipeline = p
            chosen_op = op
            break

        if chosen_pipeline is None:
            continue

        # Compute right-sized CPU and RAM allocations.
        prio = chosen_pipeline.priority

        # CPU: cap by priority fraction of pool capacity, and by what's available.
        cpu_cap = max(s.min_cpu, float(pool.max_cpu_pool) * _cpu_cap_frac_for_priority(prio))
        cpu = min(avail_cpu, cpu_cap)
        cpu = max(s.min_cpu, cpu) if avail_cpu >= s.min_cpu else avail_cpu

        # RAM: use estimate (and learned floor) but don't exceed available.
        # Allocate a bit of slack to reduce OOM retries.
        ram_floor = _ram_floor_for_op(chosen_pipeline.pipeline_id, chosen_op)
        ram_target = ram_floor * 1.15  # small slack; extra RAM is "free"
        ram = min(avail_ram, max(s.min_ram_gb, ram_target))

        # If we can't satisfy the RAM floor, don't start it in this pool now; requeue.
        if avail_ram < min(avail_ram, ram_floor):
            _queue_for_priority(prio).append(chosen_pipeline)
            continue

        # Create assignment for this one operator.
        assignment = Assignment(
            ops=[chosen_op],
            cpu=cpu,
            ram=ram,
            priority=prio,
            pool_id=pool_id,
            pipeline_id=chosen_pipeline.pipeline_id,
        )
        assignments.append(assignment)

        # Requeue the pipeline to continue progressing later.
        _queue_for_priority(prio).append(chosen_pipeline)

        # Update anti-starvation counters.
        if prio in (Priority.QUERY, Priority.INTERACTIVE):
            s.hp_used += 1
        else:
            s.hp_used = 0

    return suspensions, assignments
