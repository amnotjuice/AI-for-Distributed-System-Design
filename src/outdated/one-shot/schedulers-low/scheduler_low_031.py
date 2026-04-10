# policy_key: scheduler_low_031
# reasoning_effort: low
# model: gpt-5.2-2025-12-11
# llm_cost: 0.038513
# generation_seconds: 35.68
# generated_at: 2026-03-14T02:26:20.606706
@register_scheduler_init(key="scheduler_low_031")
def scheduler_low_031_init(s):
    """Priority-aware FIFO with simple right-sizing + bounded retries.

    Improvements over naive FIFO:
      - Separate FIFO queues per priority; schedule QUERY/INTERACTIVE before BATCH.
      - Avoid "give all resources to one op": allocate a capped slice of pool CPU/RAM per assignment.
      - On OOM failures, retry with increased RAM for that pipeline (exponential backoff, capped).
      - Bound retries to avoid infinite loops on persistent failures.
    """
    # FIFO queues per priority
    s.q_query = []
    s.q_interactive = []
    s.q_batch = []

    # Track presence to avoid accidental duplicates
    s.enqueued = set()  # pipeline_id

    # Simple per-pipeline RAM boost (applies to next scheduled op in that pipeline)
    s.pipeline_ram_boost = {}  # pipeline_id -> ram_to_request_next_time

    # Failure accounting / retry bounds
    s.pipeline_fail_count = {}  # pipeline_id -> count
    s.max_retries = 3

    # Tuning knobs (kept conservative)
    s.query_cpu_frac = 0.60
    s.query_ram_frac = 0.60
    s.interactive_cpu_frac = 0.50
    s.interactive_ram_frac = 0.50
    s.batch_cpu_frac = 0.35
    s.batch_ram_frac = 0.35

    # Small floor to avoid allocating 0 in tiny pools
    s.min_cpu_alloc = 0.1
    s.min_ram_alloc = 0.1


def _prio_queue(s, priority):
    if priority == Priority.QUERY:
        return s.q_query
    if priority == Priority.INTERACTIVE:
        return s.q_interactive
    return s.q_batch


def _oom_like(err):
    if not err:
        return False
    try:
        msg = str(err).upper()
    except Exception:
        return False
    return ("OOM" in msg) or ("OUT OF MEMORY" in msg) or ("MEMORY" in msg and "KILL" in msg)


def _alloc_fracs(s, priority):
    if priority == Priority.QUERY:
        return s.query_cpu_frac, s.query_ram_frac
    if priority == Priority.INTERACTIVE:
        return s.interactive_cpu_frac, s.interactive_ram_frac
    return s.batch_cpu_frac, s.batch_ram_frac


def _pipeline_is_done_or_abandoned(s, pipeline):
    st = pipeline.runtime_status()
    if st.is_pipeline_successful():
        return True
    # If we exceeded retry budget, abandon it.
    if s.pipeline_fail_count.get(pipeline.pipeline_id, 0) > s.max_retries:
        return True
    return False


def _next_ready_assignment_from_queue(s, q, pool_id, avail_cpu, avail_ram):
    """Pick the first pipeline in q that has a ready op; keep FIFO via rotation."""
    if not q:
        return None, None

    # We'll rotate through the queue once to find schedulable work.
    n = len(q)
    for _ in range(n):
        pipeline = q.pop(0)

        # If the pipeline is completed or abandoned, drop it.
        if _pipeline_is_done_or_abandoned(s, pipeline):
            s.enqueued.discard(pipeline.pipeline_id)
            continue

        st = pipeline.runtime_status()

        # Only schedule ops whose parents are complete.
        op_list = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
        if not op_list:
            # Not ready yet; keep it in FIFO order (rotate to back).
            q.append(pipeline)
            continue

        # Found ready work: keep pipeline in queue (round-robin at op granularity).
        q.append(pipeline)

        # Size resources conservatively: fraction of available, capped by pool max,
        # and optionally boosted by last OOM signal for this pipeline.
        cpu_frac, ram_frac = _alloc_fracs(s, pipeline.priority)
        cpu = max(s.min_cpu_alloc, min(avail_cpu, avail_cpu * cpu_frac))
        ram = max(s.min_ram_alloc, min(avail_ram, avail_ram * ram_frac))

        # Apply RAM boost if we previously saw OOM for this pipeline.
        boosted = s.pipeline_ram_boost.get(pipeline.pipeline_id)
        if boosted is not None:
            ram = max(ram, boosted)
            ram = min(ram, avail_ram)  # cannot exceed current availability

        assignment = Assignment(
            ops=op_list,
            cpu=cpu,
            ram=ram,
            priority=pipeline.priority,
            pool_id=pool_id,
            pipeline_id=pipeline.pipeline_id,
        )
        return pipeline, assignment

    return None, None


@register_scheduler(key="scheduler_low_031")
def scheduler_low_031(s, results: List[ExecutionResult], pipelines: List[Pipeline]) -> Tuple[List[Suspend], List[Assignment]]:
    """
    Priority-aware, low-risk improvements over naive FIFO.

    Decision order:
      1) Update per-pipeline sizing hints from failures (especially OOM).
      2) Enqueue new pipelines into per-priority FIFO queues.
      3) For each pool, schedule at most one ready operator, preferring:
         QUERY -> INTERACTIVE -> BATCH.
    """
    # 1) Incorporate execution feedback (OOM -> increase RAM, failures -> retry budget).
    for r in results or []:
        # Only act on failures; successes keep current hints.
        if getattr(r, "failed", None) and r.failed():
            pid = getattr(r, "pipeline_id", None)
            # Some simulators may not put pipeline_id on result; fall back to no-op.
            if pid is None:
                continue

            s.pipeline_fail_count[pid] = s.pipeline_fail_count.get(pid, 0) + 1

            if _oom_like(getattr(r, "error", None)):
                # Exponential backoff using last attempted RAM as baseline, capped by pool max.
                pool = s.executor.pools[r.pool_id]
                prev = float(getattr(r, "ram", 0.0) or 0.0)
                if prev <= 0:
                    # If unknown, start from a modest fraction of pool capacity.
                    prev = max(s.min_ram_alloc, pool.max_ram_pool * 0.25)
                boosted = min(pool.max_ram_pool, prev * 2.0)
                s.pipeline_ram_boost[pid] = boosted
            else:
                # Non-OOM failures: don't keep retrying forever; no special resizing.
                # If it later succeeds, we won't care; if it keeps failing, it will be abandoned.
                pass

    # 2) Enqueue arrivals (once).
    for p in pipelines or []:
        if p.pipeline_id in s.enqueued:
            continue
        s.enqueued.add(p.pipeline_id)
        _prio_queue(s, p.priority).append(p)

    # Early exit if nothing changed.
    if not (pipelines or results):
        return [], []

    suspensions = []  # No safe preemption without visibility into running containers.
    assignments = []

    # Helper: if high-priority work exists, try harder not to schedule batch unless
    # we have reasonable headroom. This is a soft guardrail for latency.
    have_hp_waiting = bool(s.q_query or s.q_interactive)

    # 3) One op per pool per tick, in priority order.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool
        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        chosen_assignment = None

        # Try QUERY first, then INTERACTIVE.
        _, a = _next_ready_assignment_from_queue(s, s.q_query, pool_id, avail_cpu, avail_ram)
        if a is not None:
            chosen_assignment = a
        else:
            _, a = _next_ready_assignment_from_queue(s, s.q_interactive, pool_id, avail_cpu, avail_ram)
            if a is not None:
                chosen_assignment = a

        # Only consider batch if no high-priority assignment was possible.
        if chosen_assignment is None:
            # If high-priority work exists elsewhere, require a bit of headroom to run batch.
            if have_hp_waiting:
                # Keep a small reserve to reduce interference when HP becomes schedulable.
                if avail_cpu < max(s.min_cpu_alloc, pool.max_cpu_pool * 0.25) or avail_ram < max(s.min_ram_alloc, pool.max_ram_pool * 0.25):
                    continue

            _, a = _next_ready_assignment_from_queue(s, s.q_batch, pool_id, avail_cpu, avail_ram)
            if a is not None:
                chosen_assignment = a

        if chosen_assignment is not None:
            assignments.append(chosen_assignment)

    # Opportunistic cleanup: if pipelines are done/abandoned, they will be removed lazily
    # when encountered in queue rotation; no extra pass needed here.

    return suspensions, assignments
