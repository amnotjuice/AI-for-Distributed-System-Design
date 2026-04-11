# policy_key: scheduler_est_007
# reasoning_effort: low
# model: gpt-5.2-2025-12-11
# llm_cost: 0.039346
# generation_seconds: 37.73
# generated_at: 2026-03-31T23:49:40.409533
@register_scheduler_init(key="scheduler_est_007")
def scheduler_est_007_init(s):
    """
    Priority-aware, estimate-aware FIFO with conservative reservations.

    Improvements over naive FIFO:
    - Maintain separate waiting queues by priority (QUERY/INTERACTIVE/BATCH).
    - Use operator memory estimates (op.estimate.mem_peak_gb) as a RAM floor.
    - Avoid "give the whole pool to one op": allocate bounded CPU/RAM per op to
      improve tail latency and increase concurrency.
    - Keep a small reserved headroom in each pool for high-priority work by
      limiting how much low-priority work can consume.
    - On suspected OOM failures, increase the remembered RAM floor for the op and retry.
    """
    # Priority queues (FIFO within each priority)
    s.q_query = []
    s.q_interactive = []
    s.q_batch = []

    # Remembered per-operator RAM floors (GB) learned from failures/attempts
    # Keyed by id(op) to avoid relying on schema details.
    s.op_ram_floor_gb = {}

    # Remember pipeline enqueue order for fairness among pipelines within same priority
    s._enqueue_seq = 0
    s.pipeline_seq = {}  # pipeline_id -> seq (lower is older)

    # Configuration knobs (kept intentionally simple)
    s.min_ram_gb_default = 2.0          # if no estimate
    s.extra_ram_pad_gb = 0.5            # small padding above estimate/floor
    s.oom_backoff_multiplier = 2.0      # on OOM, multiply last attempt RAM
    s.max_cpu_per_op_frac = 0.5         # don't allocate more than 50% of pool CPU per op
    s.min_cpu_per_op = 1.0              # at least 1 vCPU if possible

    # Reservations (soft): do not allow batch to consume beyond these fractions
    # when higher-priority queues are non-empty.
    s.reserve_cpu_frac_for_hp = 0.25    # keep at least 25% CPU for QUERY/INTERACTIVE
    s.reserve_ram_frac_for_hp = 0.25    # keep at least 25% RAM for QUERY/INTERACTIVE


@register_scheduler(key="scheduler_est_007")
def scheduler_est_007_scheduler(s, results, pipelines):
    """
    Policy step:
    - Ingest new pipelines into per-priority FIFO queues.
    - Process results to learn from failures (OOM -> raise RAM floor).
    - For each pool, schedule ready operators, preferring higher priority.
      Use bounded resource sizing per op and soft reservations to protect latency.
    """
    # --- Helpers (kept inside to avoid imports / external dependencies) ---
    def _is_high_priority(pri):
        return pri in (Priority.QUERY, Priority.INTERACTIVE)

    def _queue_for_priority(pri):
        if pri == Priority.QUERY:
            return s.q_query
        if pri == Priority.INTERACTIVE:
            return s.q_interactive
        return s.q_batch

    def _suspected_oom(err):
        if err is None:
            return False
        # Be robust to different error types (string/exception/structured)
        try:
            msg = str(err).lower()
        except Exception:
            return False
        return ("oom" in msg) or ("out of memory" in msg) or ("cuda out of memory" in msg) or ("memoryerror" in msg)

    def _op_mem_floor_gb(op):
        # Start from estimate if present, else a small default
        est = None
        try:
            est = getattr(getattr(op, "estimate", None), "mem_peak_gb", None)
        except Exception:
            est = None

        floor = s.min_ram_gb_default if (est is None) else float(est)
        learned = s.op_ram_floor_gb.get(id(op))
        if learned is not None:
            floor = max(floor, float(learned))
        return max(0.0, floor)

    def _pick_cpu_for_op(pool, pri, avail_cpu):
        # Bound per-op CPU to improve concurrency/latency.
        # High priority gets a larger share, but still bounded.
        max_cpu_per_op = max(s.min_cpu_per_op, pool.max_cpu_pool * s.max_cpu_per_op_frac)

        if pri == Priority.QUERY:
            target = min(max_cpu_per_op, max(s.min_cpu_per_op, pool.max_cpu_pool * 0.5))
        elif pri == Priority.INTERACTIVE:
            target = min(max_cpu_per_op, max(s.min_cpu_per_op, pool.max_cpu_pool * 0.4))
        else:
            target = min(max_cpu_per_op, max(s.min_cpu_per_op, pool.max_cpu_pool * 0.25))

        cpu = min(avail_cpu, target)
        # If fractional CPU is not supported by the simulator, this still works as float;
        # caller will naturally avoid scheduling if cpu <= 0.
        return cpu

    def _pick_ram_for_op(avail_ram, op):
        # Use estimate/learned floor + small padding; extra RAM is "free" for performance.
        need = _op_mem_floor_gb(op) + s.extra_ram_pad_gb
        return min(avail_ram, need)

    def _pipeline_is_done_or_failed(p):
        st = p.runtime_status()
        if st.is_pipeline_successful():
            return True
        # If any operator failed, we may still want to retry (OOM) so do not drop here.
        return False

    def _get_ready_ops_one(p):
        st = p.runtime_status()
        # Only schedule if parents complete for DAG correctness
        ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        if not ops:
            return []
        return ops[:1]  # single-op granularity; concurrency comes from multiple pipelines/ops

    def _requeue_pipeline(p):
        q = _queue_for_priority(p.priority)
        q.append(p)

    # --- Ingest new pipelines ---
    for p in pipelines:
        if p.pipeline_id not in s.pipeline_seq:
            s.pipeline_seq[p.pipeline_id] = s._enqueue_seq
            s._enqueue_seq += 1
        _requeue_pipeline(p)

    # --- Learn from results (OOM -> raise RAM floor, retry by leaving pipeline queued) ---
    # We avoid "hard failing" pipelines on OOM by simply increasing per-op floor; the
    # pipeline will become assignable again via FAILED -> ASSIGNABLE_STATES.
    for r in results:
        if r is None:
            continue
        if not getattr(r, "failed", lambda: False)():
            continue
        if not _suspected_oom(getattr(r, "error", None)):
            continue
        # Increase remembered floor for each op in this failed attempt.
        for op in getattr(r, "ops", []) or []:
            prev = s.op_ram_floor_gb.get(id(op), None)
            attempted = getattr(r, "ram", None)
            if attempted is None:
                attempted = _op_mem_floor_gb(op)
            new_floor = float(attempted) * s.oom_backoff_multiplier
            if prev is None:
                s.op_ram_floor_gb[id(op)] = new_floor
            else:
                s.op_ram_floor_gb[id(op)] = max(float(prev), new_floor)

    # Early exit if nothing changed
    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    # --- Scheduling loop per pool ---
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool
        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # Determine if we should keep headroom for high-priority work
        hp_waiting = bool(s.q_query) or bool(s.q_interactive)

        # Compute soft reservation amounts (only enforced for batch scheduling)
        reserve_cpu = pool.max_cpu_pool * s.reserve_cpu_frac_for_hp if hp_waiting else 0.0
        reserve_ram = pool.max_ram_pool * s.reserve_ram_frac_for_hp if hp_waiting else 0.0

        # Try to schedule multiple ops in a pool within this tick
        while True:
            # Stop if we can't schedule anything meaningful
            if avail_cpu <= 0 or avail_ram <= 0:
                break

            # Priority order: QUERY -> INTERACTIVE -> BATCH
            picked_queue = None
            if s.q_query:
                picked_queue = s.q_query
            elif s.q_interactive:
                picked_queue = s.q_interactive
            elif s.q_batch:
                picked_queue = s.q_batch
            else:
                break

            # For BATCH, enforce reservations if HP is waiting elsewhere
            if picked_queue is s.q_batch and hp_waiting:
                if (avail_cpu - reserve_cpu) < s.min_cpu_per_op or (avail_ram - reserve_ram) < s.min_ram_gb_default:
                    break  # keep headroom for high priority

            # Pop next pipeline, but requeue if not ready; avoid dropping
            pipeline = picked_queue.pop(0)

            # Drop completed pipelines from the queue
            if _pipeline_is_done_or_failed(pipeline):
                continue

            # Find a ready operator in this pipeline
            op_list = _get_ready_ops_one(pipeline)
            if not op_list:
                # Not ready now; requeue at end to allow other pipelines to run
                _requeue_pipeline(pipeline)
                # To avoid spinning on blocked pipelines, if we cycled too much,
                # we can stop when all queues are blocked; keep it simple: continue.
                # If nothing else is runnable, loop will naturally terminate.
                # However, we should avoid infinite loops if all pipelines are blocked:
                # detect by not making progress in an iteration.
                # We'll do a simple safeguard: if all queues are non-empty but we
                # didn't assign anything, break after one blocked encounter.
                # (Implemented by checking progress below.)
                # Mark as no-progress and break if we already saw a block.
                if not hasattr(s, "_saw_blocked"):
                    s._saw_blocked = False
                if s._saw_blocked:
                    s._saw_blocked = False
                    break
                s._saw_blocked = True
                continue

            # We can schedule this op; reset blocked safeguard
            if hasattr(s, "_saw_blocked"):
                s._saw_blocked = False

            op = op_list[0]

            # Decide resources
            cpu = _pick_cpu_for_op(pool, pipeline.priority, avail_cpu)
            if cpu <= 0:
                # Can't schedule due to CPU; requeue and stop for this pool
                _requeue_pipeline(pipeline)
                break

            ram_cap = avail_ram
            if picked_queue is s.q_batch and hp_waiting:
                ram_cap = max(0.0, avail_ram - reserve_ram)
            ram = _pick_ram_for_op(ram_cap, op)

            # Must meet the memory floor; otherwise, we can't run it in this pool right now.
            min_need = _op_mem_floor_gb(op)
            if ram < min_need:
                _requeue_pipeline(pipeline)
                break

            # Create assignment
            assignments.append(
                Assignment(
                    ops=op_list,
                    cpu=cpu,
                    ram=ram,
                    priority=pipeline.priority,
                    pool_id=pool_id,
                    pipeline_id=pipeline.pipeline_id,
                )
            )

            # Update remaining resources in this pool for this tick
            avail_cpu -= cpu
            avail_ram -= ram

            # Requeue pipeline for subsequent operators
            _requeue_pipeline(pipeline)

    return suspensions, assignments
