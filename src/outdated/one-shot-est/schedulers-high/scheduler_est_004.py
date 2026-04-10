# policy_key: scheduler_est_004
# reasoning_effort: high
# model: gpt-5.2-2025-12-11
# llm_cost: 0.099182
# generation_seconds: 110.62
# generated_at: 2026-03-31T18:28:08.153322
@register_scheduler_init(key="scheduler_est_004")
def scheduler_est_004_init(s):
    """
    Priority-aware, estimate-driven scheduler (small improvements over naive FIFO).

    Main ideas:
      1) Maintain separate FIFO queues per priority; always try QUERY/INTERACTIVE before BATCH.
      2) Size RAM using op.estimate.mem_peak_gb with a safety factor; on OOM, bump the factor and retry (bounded).
      3) Prevent batch from consuming the entire pool by keeping a simple "reservation" headroom for high priority.
      4) Use small per-op CPU caps by priority to allow more concurrency (especially for interactive/query).
    """
    # Per-priority FIFO queues
    s.queues = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }
    s.in_queue = set()  # pipeline_id present in some queue

    # Per-pipeline metadata (kept even if it leaves the queue; safe to reuse on re-arrival)
    # Fields:
    #   mem_factor: multiplier for op.estimate.mem_peak_gb
    #   oom_retries: number of OOM retries seen (pipeline-level heuristic)
    #   retryable: whether we're allowed to retry FAILED ops (only after detecting OOM)
    #   enqueue_tick: tick when first enqueued (for simple batch aging)
    s.meta = {}

    # Map op object identity -> pipeline_id to attribute results back to pipelines
    s.op_to_pipeline_id = {}

    # Logical tick counter (increments each scheduler call)
    s.tick = 0

    # Tuning knobs (conservative, "small improvements first")
    s.mem_factor_init = 1.25
    s.mem_factor_max = 4.0
    s.mem_pad_gb = 0.25  # additive cushion even if estimate is small
    s.max_oom_retries = 3

    # Reserve some headroom so batch doesn't crowd out interactive/query arrivals
    s.reserve_frac_cpu = 0.20
    s.reserve_frac_ram = 0.20

    # Simple anti-starvation: allow batch to run even when high priority exists after waiting long enough
    s.batch_aging_ticks = 50

    # CPU caps by priority (favor latency for high priority while allowing concurrency)
    s.cpu_cap_query = 8.0
    s.cpu_cap_interactive = 4.0
    s.cpu_cap_batch = 2.0


def _prio_order():
    # Highest to lowest
    return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]


def _ensure_meta(s, pipeline):
    pid = pipeline.pipeline_id
    if pid not in s.meta:
        s.meta[pid] = {
            "mem_factor": s.mem_factor_init,
            "oom_retries": 0,
            "retryable": False,
            "enqueue_tick": s.tick,
        }


def _is_oom_error(err):
    # Defensive: error may be None or non-string.
    if err is None:
        return False
    text = str(err).lower()
    return ("oom" in text) or ("out of memory" in text) or ("out_of_memory" in text) or ("memory" in text and "exceed" in text)


def _any_ready_high_prio(s):
    # Check if there exists any QUERY/INTERACTIVE pipeline with an assignable op ready.
    for prio in (Priority.QUERY, Priority.INTERACTIVE):
        q = s.queues.get(prio, [])
        for p in q:
            status = p.runtime_status()
            if status.is_pipeline_successful():
                continue
            # If it has FAILED ops and we are not in a retryable mode, treat as not runnable.
            has_failures = status.state_counts[OperatorState.FAILED] > 0
            if has_failures and not s.meta.get(p.pipeline_id, {}).get("retryable", False):
                continue
            op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
            if op_list:
                return True
    return False


def _cpu_for_prio(s, prio, pool, avail_cpu):
    # Keep at least 1 vCPU if anything is runnable.
    if avail_cpu <= 0:
        return 0.0
    if prio == Priority.QUERY:
        cap = min(s.cpu_cap_query, pool.max_cpu_pool)
    elif prio == Priority.INTERACTIVE:
        cap = min(s.cpu_cap_interactive, pool.max_cpu_pool)
    else:
        cap = min(s.cpu_cap_batch, pool.max_cpu_pool)
    cpu = min(avail_cpu, max(1.0, cap))
    return cpu


def _ram_for_op(s, pipeline_id, op, pool, avail_ram):
    # Estimate-driven RAM sizing with safety factor and additive cushion.
    meta = s.meta.get(pipeline_id, None)
    mem_factor = meta["mem_factor"] if meta else s.mem_factor_init

    est = None
    try:
        est = op.estimate.mem_peak_gb
    except Exception:
        est = None

    if est is None:
        # Fallback: request a small slice, but not more than what's available.
        # (Conservative: better to under-allocate than consume the whole pool blindly.)
        ram = min(avail_ram, max(0.5, min(2.0, pool.max_ram_pool * 0.10)))
        return ram

    ram = est * mem_factor + s.mem_pad_gb
    # Never exceed pool capacity; if it doesn't fit in avail_ram, caller can decide to wait.
    ram = min(ram, pool.max_ram_pool)
    # Ensure a minimal non-trivial request if estimate is tiny.
    ram = max(0.5, ram)
    ram = min(ram, avail_ram)
    return ram


def _effective_avail_for_batch(s, pool):
    # Reserve headroom for future high-priority arrivals.
    reserve_cpu = max(0.0, pool.max_cpu_pool * s.reserve_frac_cpu)
    reserve_ram = max(0.0, pool.max_ram_pool * s.reserve_frac_ram)
    eff_cpu = max(0.0, pool.avail_cpu_pool - reserve_cpu)
    eff_ram = max(0.0, pool.avail_ram_pool - reserve_ram)
    return eff_cpu, eff_ram


def _pick_next_assignment_for_pool(s, pool_id, pool, high_prio_waiting_ready):
    """
    Choose the next (pipeline, op, cpu, ram, prio) to run on this pool, or None.

    Strategy:
      - If high-priority work exists and is ready, do not start new batch work unless it has "aged" enough.
      - Scan queues in strict priority order; preserve FIFO within each priority by rotating items.
      - Only pick an op if it fits current pool availability (or effective availability for batch).
    """
    # Determine if batch is allowed right now.
    allow_batch = True
    if high_prio_waiting_ready:
        allow_batch = False

    for prio in _prio_order():
        if prio == Priority.BATCH_PIPELINE and not allow_batch:
            # Batch can still run if the oldest batch has aged past threshold.
            # (We implement this by checking candidates' enqueue_tick below.)
            pass

        q = s.queues.get(prio, [])
        if not q:
            continue

        n = len(q)
        for _ in range(n):
            pipeline = q.pop(0)
            pid = pipeline.pipeline_id
            _ensure_meta(s, pipeline)

            status = pipeline.runtime_status()

            # Drop successful pipelines from consideration
            if status.is_pipeline_successful():
                s.in_queue.discard(pid)
                continue

            # If there are failures, only allow retry if we've flagged it as retryable (OOM path).
            has_failures = status.state_counts[OperatorState.FAILED] > 0
            if has_failures and not s.meta[pid]["retryable"]:
                s.in_queue.discard(pid)
                continue

            # Batch gating when high priority is waiting
            if prio == Priority.BATCH_PIPELINE and high_prio_waiting_ready:
                age = s.tick - s.meta[pid]["enqueue_tick"]
                if age < s.batch_aging_ticks:
                    # Not aged enough; rotate and skip.
                    q.append(pipeline)
                    continue

            op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
            if not op_list:
                # Not ready (parents not complete) or nothing assignable; rotate within same priority.
                q.append(pipeline)
                continue

            op = op_list[0]

            # For batch, only consider effective availability to preserve headroom.
            if prio == Priority.BATCH_PIPELINE:
                eff_cpu, eff_ram = _effective_avail_for_batch(s, pool)
                avail_cpu = eff_cpu
                avail_ram = eff_ram
            else:
                avail_cpu = pool.avail_cpu_pool
                avail_ram = pool.avail_ram_pool

            if avail_cpu <= 0 or avail_ram <= 0:
                # Can't run anything now on this pool.
                q.append(pipeline)
                return None

            cpu = _cpu_for_prio(s, prio, pool, avail_cpu)
            ram = _ram_for_op(s, pid, op, pool, avail_ram)

            # If we ended up clipping RAM to avail_ram, we should only proceed if it still looks safe:
            # - If estimate exists and clipped below est*factor, prefer to wait rather than risk OOM.
            # (If estimate missing, proceed with fallback RAM.)
            try:
                est = op.estimate.mem_peak_gb
            except Exception:
                est = None
            if est is not None:
                desired = min(pool.max_ram_pool, est * s.meta[pid]["mem_factor"] + s.mem_pad_gb)
                if ram + 1e-9 < min(desired, avail_ram) and desired <= pool.max_ram_pool:
                    # This check is mostly redundant because ram=min(desired, avail_ram),
                    # but keep it explicit: if desired doesn't fit, don't run now.
                    q.append(pipeline)
                    continue
                if desired > avail_ram + 1e-9:
                    # Not enough RAM right now for an estimate-backed safe run.
                    q.append(pipeline)
                    continue

            if cpu <= 0 or ram <= 0:
                q.append(pipeline)
                continue

            # Put the pipeline back in the queue so future ops can be scheduled later.
            # (This keeps FIFO-ish behavior while still allowing progress across many pipelines.)
            q.append(pipeline)

            return pipeline, op, cpu, ram, prio

    return None


@register_scheduler(key="scheduler_est_004")
def scheduler_est_004_scheduler(s, results: List["ExecutionResult"], pipelines: List["Pipeline"]) -> Tuple[List["Suspend"], List["Assignment"]]:
    """
    Scheduler step:
      - Ingest new pipelines into per-priority queues.
      - Update per-pipeline memory factors based on observed OOM failures.
      - Assign as many ops as fit per pool, prioritizing QUERY/INTERACTIVE.
    """
    s.tick += 1

    # Ingest new pipelines
    for p in pipelines:
        _ensure_meta(s, p)
        pid = p.pipeline_id
        if pid not in s.in_queue:
            s.queues[p.priority].append(p)
            s.in_queue.add(pid)
        else:
            # Already queued; no action.
            pass

    # Process results: detect OOM and adjust memory factor; allow retries only for OOM.
    for r in results:
        # Attribute result back to pipeline via op identity mapping.
        pid = None
        if getattr(r, "ops", None):
            for op in r.ops:
                pid = s.op_to_pipeline_id.get(id(op), None)
                if pid is not None:
                    break

        if pid is None:
            continue

        if pid not in s.meta:
            # Pipeline meta might have been GC'd logically; ignore.
            continue

        if r.failed():
            if _is_oom_error(getattr(r, "error", None)):
                # Bump memory factor and mark pipeline retryable.
                s.meta[pid]["retryable"] = True
                s.meta[pid]["oom_retries"] += 1
                s.meta[pid]["mem_factor"] = min(s.mem_factor_max, s.meta[pid]["mem_factor"] * 1.5)

                # If it keeps OOMing, stop retrying (avoid infinite loops).
                if s.meta[pid]["oom_retries"] > s.max_oom_retries:
                    s.meta[pid]["retryable"] = False
                    # Also remove from queue if present; we will drop on next scan.
            else:
                # Non-OOM failures are treated as terminal: do not retry FAILED ops.
                s.meta[pid]["retryable"] = False
        else:
            # On success, clear retryable mode and reset OOM counter (pipeline-level heuristic).
            s.meta[pid]["retryable"] = False
            s.meta[pid]["oom_retries"] = 0

    # Early exit if no new pipelines and no results (stable state)
    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    # Compute whether high priority is waiting with runnable work, to gate batch starts.
    high_prio_waiting_ready = _any_ready_high_prio(s)

    # Assign work per pool
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]

        # Fill the pool with as many runnable ops as possible.
        # (If the simulator allows only one container per pool, the second assignment will simply fail to fit.)
        while True:
            if pool.avail_cpu_pool <= 0 or pool.avail_ram_pool <= 0:
                break

            picked = _pick_next_assignment_for_pool(s, pool_id, pool, high_prio_waiting_ready)
            if picked is None:
                break

            pipeline, op, cpu, ram, prio = picked
            pid = pipeline.pipeline_id

            # Final safety fit check against actual current availability (not effective).
            # For batch, _pick_* already used effective availability; but actual availability is >= effective.
            if cpu > pool.avail_cpu_pool + 1e-9 or ram > pool.avail_ram_pool + 1e-9:
                break

            # Record mapping so we can attribute the result back to the pipeline later.
            s.op_to_pipeline_id[id(op)] = pid

            assignments.append(
                Assignment(
                    ops=[op],
                    cpu=cpu,
                    ram=ram,
                    priority=prio,
                    pool_id=pool_id,
                    pipeline_id=pid,
                )
            )

            # Optimistically assume resources are consumed by this assignment in this tick's view.
            # (Even if the simulator accounts for it later, this prevents us from over-assigning.)
            try:
                pool.avail_cpu_pool -= cpu
                pool.avail_ram_pool -= ram
            except Exception:
                # If pool availability is read-only, just issue one assignment per pool.
                break

    return suspensions, assignments
