# policy_key: scheduler_est_026
# reasoning_effort: low
# model: gpt-5.2-2025-12-11
# llm_cost: 0.045310
# generation_seconds: 37.07
# generated_at: 2026-04-01T00:02:08.144029
@register_scheduler_init(key="scheduler_est_026")
def scheduler_est_026_init(s):
    """Priority-aware, estimate-aware scheduler (incremental improvement over naive FIFO).

    Improvements vs. naive:
      1) Strict priority ordering across all waiting pipelines (QUERY > INTERACTIVE > BATCH).
      2) Use op.estimate.mem_peak_gb as a RAM floor to reduce OOMs (extra RAM is "free" performance-wise).
      3) Avoid head-of-line blocking by not giving the entire pool CPU/RAM to one op; instead,
         pack multiple ready ops per pool with a conservative per-op CPU cap.
      4) Simple retry-on-failure with RAM backoff (bounded) to learn unknown memory peaks.
    """
    s.waiting_queue = []  # FIFO arrival order, but scheduling picks by priority each tick
    s.pipeline_arrival_seq = 0
    s.pipeline_seq = {}  # pipeline_id -> arrival index

    # Per-op learned floors/attempts. Keys are best-effort stable identifiers.
    s.op_mem_floor_gb = {}  # op_key -> float
    s.op_attempts = {}  # op_key -> int

    # Conservative defaults (small steps first; keep behavior predictable).
    s.default_mem_floor_gb = 2.0
    s.max_retries_per_op = 2

    # Packing parameters: keep caps modest to reduce interference and improve latency under contention.
    s.cpu_cap_query = 4.0
    s.cpu_cap_interactive = 4.0
    s.cpu_cap_batch = 2.0

    # Minimal RAM boost on retry (OOM-agnostic, but still helps many failures).
    s.retry_mem_multiplier = 1.5
    s.retry_mem_add_gb = 1.0


@register_scheduler(key="scheduler_est_026")
def scheduler_est_026_scheduler(s, results, pipelines):
    """
    Policy logic:
      - Add arriving pipelines to a waiting list with an arrival sequence.
      - Incorporate execution feedback: on failures, increase learned RAM floor (bounded retries).
      - For each pool, repeatedly pick the highest-priority pipeline that has a ready op and
        pack assignments until CPU/RAM is exhausted.
    """
    # ---- helpers (kept inside for portability with the simulator harness) ----
    def _prio_rank(prio):
        # Smaller is higher priority in sorting.
        if prio == Priority.QUERY:
            return 0
        if prio == Priority.INTERACTIVE:
            return 1
        return 2  # Priority.BATCH_PIPELINE and any others

    def _cpu_cap_for(prio):
        if prio == Priority.QUERY:
            return s.cpu_cap_query
        if prio == Priority.INTERACTIVE:
            return s.cpu_cap_interactive
        return s.cpu_cap_batch

    def _op_key(pipeline, op):
        # Best-effort stable identifier across ticks.
        # Prefer pipeline_id + op_id if present; else fallback to object id.
        op_id = getattr(op, "op_id", None)
        if op_id is None:
            op_id = getattr(op, "operator_id", None)
        if op_id is None:
            op_id = id(op)
        return (pipeline.pipeline_id, op_id)

    def _estimated_mem_floor_gb(op):
        est = None
        try:
            est = op.estimate.mem_peak_gb
        except Exception:
            est = None
        if est is None:
            return s.default_mem_floor_gb
        try:
            # Guard against weird values.
            est = float(est)
        except Exception:
            return s.default_mem_floor_gb
        if est <= 0:
            return s.default_mem_floor_gb
        return est

    # ---- ingest new pipelines ----
    for p in pipelines:
        s.pipeline_arrival_seq += 1
        s.pipeline_seq[p.pipeline_id] = s.pipeline_arrival_seq
        s.waiting_queue.append(p)

    # Early exit if nothing to do.
    if not pipelines and not results:
        return [], []

    # ---- incorporate feedback from completed/failed executions ----
    for r in results:
        # If it failed, increase memory floor for the ops we can identify.
        if getattr(r, "failed", None) and r.failed():
            # r.ops is expected to be a list
            for op in (r.ops or []):
                # We don't have pipeline_id directly on the op, but ExecutionResult includes pipeline_id?
                # Not listed in the prompt, so best effort: use (None, op_id) fallback if needed.
                op_id = getattr(op, "op_id", None)
                if op_id is None:
                    op_id = getattr(op, "operator_id", None)
                if op_id is None:
                    op_id = id(op)
                op_key = ("_unknown_pipeline", op_id)

                # Count attempts and apply RAM backoff.
                attempts = s.op_attempts.get(op_key, 0) + 1
                s.op_attempts[op_key] = attempts

                prev_floor = s.op_mem_floor_gb.get(op_key, None)
                if prev_floor is None:
                    prev_floor = max(s.default_mem_floor_gb, float(getattr(r, "ram", 0) or 0))
                new_floor = max(prev_floor * s.retry_mem_multiplier, prev_floor + s.retry_mem_add_gb)
                s.op_mem_floor_gb[op_key] = new_floor

    # ---- drop completed pipelines; keep only those still active ----
    still_waiting = []
    for p in s.waiting_queue:
        status = p.runtime_status()
        if status.is_pipeline_successful():
            continue
        # Unlike the naive example (which drops pipelines with any failure), we allow bounded retries.
        still_waiting.append(p)
    s.waiting_queue = still_waiting

    suspensions = []
    assignments = []

    # ---- scheduling loop per pool: pack multiple ops with strict priority ordering ----
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)

        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # Attempt to pack as many ops as possible.
        # Stop when we cannot fit any ready op (by RAM) or when CPU is exhausted.
        made_progress = True
        while made_progress and avail_cpu > 0 and avail_ram > 0:
            made_progress = False

            # Select best candidate pipeline for this pool right now.
            # Criteria: highest priority, then earliest arrival.
            best = None
            best_ready_ops = None

            for p in s.waiting_queue:
                status = p.runtime_status()

                # Identify ops that can be scheduled now (parents complete).
                ready_ops = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
                if not ready_ops:
                    continue

                # Keep only one op per pipeline per pass to avoid a single pipeline monopolizing packing.
                # (We will revisit pipelines on the next iteration.)
                candidate = ( _prio_rank(p.priority), s.pipeline_seq.get(p.pipeline_id, 0) )
                if best is None or candidate < best:
                    best = candidate
                    best_ready_ops = (p, ready_ops[:1])

            if best_ready_ops is None:
                break

            pipeline, op_list = best_ready_ops
            op = op_list[0]

            # Compute RAM floor: max(estimate, learned floor).
            est_floor = _estimated_mem_floor_gb(op)

            # Also check if we have a learned floor from failures for a stable key.
            k = _op_key(pipeline, op)
            learned = s.op_mem_floor_gb.get(k, None)
            if learned is None:
                # Try the fallback key used from ExecutionResult if we couldn't map pipeline_id there.
                op_id = getattr(op, "op_id", None)
                if op_id is None:
                    op_id = getattr(op, "operator_id", None)
                if op_id is None:
                    op_id = id(op)
                learned = s.op_mem_floor_gb.get(("_unknown_pipeline", op_id), None)

            mem_floor = max(est_floor, float(learned) if learned is not None else 0.0, s.default_mem_floor_gb)

            # If op already exceeded retry budget, don't keep hammering.
            attempts = s.op_attempts.get(k, 0)
            if attempts > s.max_retries_per_op:
                # Skip this pipeline for now; it will effectively stall, but avoids infinite retry churn.
                # (Future policies can mark pipeline failed; here we keep changes minimal.)
                # Prevent infinite loops: remove it from consideration by temporarily pretending no progress.
                break

            if mem_floor > avail_ram:
                # Can't fit this op right now in this pool; no point continuing packing for this pool
                # because we only have less RAM as we pack more. Exit.
                break

            # Allocate CPU conservatively to pack more concurrent work and reduce tail latency for high priority.
            cpu_cap = _cpu_cap_for(pipeline.priority)
            cpu = min(avail_cpu, cpu_cap)
            if cpu <= 0:
                break

            # Allocate RAM as the needed floor (extra doesn't speed up), limited by remaining pool RAM.
            ram = min(avail_ram, mem_floor)

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

            avail_cpu -= cpu
            avail_ram -= ram
            made_progress = True

    return suspensions, assignments
