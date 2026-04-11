# policy_key: scheduler_none_011
# reasoning_effort: none
# model: gpt-5.2-2025-12-11
# llm_cost: 0.034873
# generation_seconds: 39.90
# generated_at: 2026-03-12T21:28:39.497084
@register_scheduler_init(key="scheduler_none_011")
def scheduler_none_011_init(s):
    """Priority-aware, conservative improvement over naive FIFO.

    Goals (small, obvious improvements first):
      1) Separate waiting queues by priority; always schedule higher priority first.
      2) Avoid head-of-line blocking: if the next pipeline can't make progress (no ready ops), skip it.
      3) Add simple OOM-aware RAM backoff per pipeline (retry with more RAM next time).
      4) Add minimal CPU slicing: don't give a single op the entire pool by default; cap per-assignment CPU
         to improve tail latency for concurrent interactive work.

    Non-goals (kept simple on purpose):
      - No sophisticated preemption logic (Suspend requires container IDs for running work, which we don't
        track robustly here).
      - No per-operator resource estimation beyond coarse retry/backoff.
    """
    from collections import deque

    # Per-priority queues
    s.q_query = deque()
    s.q_interactive = deque()
    s.q_batch = deque()

    # Pipeline-level RAM retry state
    # pipeline_id -> {"ram_hint": float, "oom_count": int}
    s.pipe_state = {}

    # Conservative caps to prevent one assignment from monopolizing a pool.
    s.max_cpu_per_assign = 8.0  # vCPU cap per assignment (soft cap)
    s.min_cpu_per_assign = 1.0  # always allocate at least 1 vCPU if possible

    # Safety factor on RAM when we see OOMs
    s.oom_ram_multiplier = 2.0
    s.max_oom_retries = 3

    # How many ops to bundle in one container assignment (keep 1 to stay close to baseline)
    s.ops_per_assignment = 1


def _priority_rank(pri):
    # Lower is higher priority
    if pri == Priority.QUERY:
        return 0
    if pri == Priority.INTERACTIVE:
        return 1
    return 2


def _enqueue_pipeline(s, p):
    if p.priority == Priority.QUERY:
        s.q_query.append(p)
    elif p.priority == Priority.INTERACTIVE:
        s.q_interactive.append(p)
    else:
        s.q_batch.append(p)


def _iter_priority_queues(s):
    # Iterate queues from highest to lowest priority
    return (s.q_query, s.q_interactive, s.q_batch)


def _get_or_init_pipe_state(s, pipeline_id, default_ram_hint):
    st = s.pipe_state.get(pipeline_id)
    if st is None:
        st = {"ram_hint": float(default_ram_hint), "oom_count": 0}
        s.pipe_state[pipeline_id] = st
    else:
        # Ensure ram_hint is at least some small positive value
        st["ram_hint"] = float(max(0.0, st.get("ram_hint", 0.0)))
        st["oom_count"] = int(st.get("oom_count", 0))
    return st


def _is_oom_error(err):
    if err is None:
        return False
    # Be robust to various error encodings
    try:
        s = str(err).lower()
    except Exception:
        return False
    return ("oom" in s) or ("out of memory" in s) or ("memory" in s and "exceed" in s)


def _drain_one_schedulable_pipeline(q):
    """Pop pipelines until we find one; return (pipeline, skipped_list)."""
    skipped = []
    while q:
        p = q.popleft()
        skipped.append(p)
        return p, skipped  # we return immediately; caller decides if it was schedulable
    return None, skipped


def _requeue_skipped(q, skipped):
    # Put skipped pipelines back in the same order they were seen (round-robin feel)
    for p in skipped:
        q.append(p)


@register_scheduler(key="scheduler_none_011")
def scheduler_none_011(s, results, pipelines):
    """
    Priority-aware scheduler with minimal RAM backoff on OOM and CPU capping.

    Strategy:
      - Ingest new pipelines into per-priority queues.
      - Process execution results:
          * If an op failed due to OOM, increase RAM hint for that pipeline (exponential backoff) and allow retry.
          * If a pipeline is complete, drop it (no requeue).
      - For each pool, fill available resources by repeatedly taking the highest-priority pipeline
        that has a ready operator. Assign a single ready op at a time.
      - Allocation:
          * RAM: min(avail_ram, max(ram_hint, small_floor))
          * CPU: min(avail_cpu, max_cpu_per_assign), but at least min_cpu_per_assign when possible.

    Returns:
      - suspensions: none (kept simple)
      - assignments: list of container assignments
    """
    # Ingest new pipelines
    for p in pipelines:
        _enqueue_pipeline(s, p)

    # Update state from results (especially OOM backoff)
    for r in results:
        if r is None:
            continue
        if not hasattr(r, "failed"):
            continue
        if r.failed():
            # Attempt to map failure back to pipeline via r.ops' pipeline_id if present,
            # otherwise use r.pipeline_id if the sim provides it. Fallback: do nothing.
            pipeline_id = getattr(r, "pipeline_id", None)
            if pipeline_id is None and getattr(r, "ops", None):
                # If ops objects carry pipeline_id, try to extract; use first op
                try:
                    pipeline_id = getattr(r.ops[0], "pipeline_id", None)
                except Exception:
                    pipeline_id = None

            if pipeline_id is not None and _is_oom_error(getattr(r, "error", None)):
                # Increase RAM hint based on observed allocation or existing hint
                current_hint = float(getattr(r, "ram", 0.0) or 0.0)
                st = _get_or_init_pipe_state(s, pipeline_id, default_ram_hint=max(0.0, current_hint))
                st["oom_count"] += 1
                # If we have an observed RAM allocation, backoff from that; else from hint
                base = max(st["ram_hint"], current_hint, 0.0)
                # Ensure growth even if base is 0
                if base <= 0.0:
                    base = 1.0
                st["ram_hint"] = base * s.oom_ram_multiplier

    # Early exit if nothing changed that would affect decisions
    if not pipelines and not results and not (s.q_query or s.q_interactive or s.q_batch):
        return [], []

    suspensions = []
    assignments = []

    # For each pool, attempt to schedule multiple assignments as resources allow
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)

        if avail_cpu <= 0.0 or avail_ram <= 0.0:
            continue

        # Keep assigning while we have meaningful resources
        made_progress = True
        while made_progress:
            made_progress = False
            # Stop if we can't even give minimal CPU/RAM
            if avail_cpu < s.min_cpu_per_assign or avail_ram <= 0.0:
                break

            chosen_pipeline = None
            chosen_queue = None
            chosen_skipped = None
            chosen_status = None
            chosen_ops = None

            # Pick highest-priority pipeline with at least one ready op
            for q in _iter_priority_queues(s):
                if not q:
                    continue

                # Round-robin: take one pipeline, see if it can schedule; if not, requeue and keep searching.
                # We cap the scan to len(q) to avoid infinite loops.
                scan_limit = len(q)
                local_skipped = []
                for _ in range(scan_limit):
                    p = q.popleft()
                    local_skipped.append(p)

                    status = p.runtime_status()
                    # Drop completed pipelines
                    if status.is_pipeline_successful():
                        continue

                    # If pipeline has any failures (non-OOM or exceeded retries), drop it (naive behavior).
                    # However, keep FAILED as assignable; the runtime uses FAILED state for retry.
                    # We only drop if there are failures AND we've exceeded our OOM retries heuristic.
                    # (If it's non-OOM failure, it will likely keep failing; we drop to prevent thrash.)
                    has_failed = status.state_counts[OperatorState.FAILED] > 0
                    if has_failed:
                        st = s.pipe_state.get(p.pipeline_id)
                        if st is not None and st.get("oom_count", 0) > s.max_oom_retries:
                            continue

                    # Find ready ops whose parents are complete
                    ops = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[: s.ops_per_assignment]
                    if not ops:
                        # Can't schedule now; keep it for later
                        continue

                    # Found a schedulable pipeline
                    chosen_pipeline = p
                    chosen_queue = q
                    chosen_skipped = local_skipped
                    chosen_status = status
                    chosen_ops = ops
                    break

                # Requeue anything we popped but did not select (including the selected one will be handled below)
                if chosen_pipeline is None:
                    # None selected from this queue; requeue all popped
                    for pp in local_skipped:
                        q.append(pp)
                    continue
                else:
                    # We selected chosen_pipeline, but local_skipped includes it; requeue the others.
                    for pp in chosen_skipped:
                        if pp is not chosen_pipeline:
                            chosen_queue.append(pp)
                    break

            if chosen_pipeline is None or not chosen_ops:
                break

            # Compute RAM/CPU allocation
            st = _get_or_init_pipe_state(s, chosen_pipeline.pipeline_id, default_ram_hint=min(avail_ram, pool.max_ram_pool))
            ram_hint = st.get("ram_hint", 0.0) or 0.0

            # Baseline RAM floor: allocate at least a small slice to reduce immediate OOM if hint not set
            ram_floor = max(1.0, 0.05 * float(pool.max_ram_pool))
            desired_ram = max(ram_hint, ram_floor)
            alloc_ram = min(avail_ram, desired_ram)

            # CPU cap to avoid monopolization; give more to higher priority if possible
            base_cpu_cap = float(s.max_cpu_per_assign)
            if chosen_pipeline.priority == Priority.QUERY:
                cpu_cap = base_cpu_cap
            elif chosen_pipeline.priority == Priority.INTERACTIVE:
                cpu_cap = max(2.0, base_cpu_cap * 0.75)
            else:
                cpu_cap = max(1.0, base_cpu_cap * 0.5)

            alloc_cpu = min(avail_cpu, cpu_cap)
            if alloc_cpu < s.min_cpu_per_assign:
                # Not enough CPU; put pipeline back and stop filling this pool
                _enqueue_pipeline(s, chosen_pipeline)
                break

            assignment = Assignment(
                ops=chosen_ops,
                cpu=alloc_cpu,
                ram=alloc_ram,
                priority=chosen_pipeline.priority,
                pool_id=pool_id,
                pipeline_id=chosen_pipeline.pipeline_id,
            )
            assignments.append(assignment)

            # Update remaining pool resources
            avail_cpu -= alloc_cpu
            avail_ram -= alloc_ram
            made_progress = True

            # Requeue pipeline for further ops (DAG may have more ready later)
            _enqueue_pipeline(s, chosen_pipeline)

    return suspensions, assignments
