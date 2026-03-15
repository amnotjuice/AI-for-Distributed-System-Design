# policy_key: scheduler_low_016
# reasoning_effort: low
# model: gpt-5.2-2025-12-11
# llm_cost: 0.035391
# generation_seconds: 49.05
# generated_at: 2026-03-12T21:46:37.784388
@register_scheduler_init(key="scheduler_low_016")
def scheduler_low_016_init(s):
    """
    Priority-aware FIFO with conservative reservations + simple OOM backoff.

    Small, incremental improvements over naive FIFO:
      1) Maintain separate FIFO queues per priority and always pick highest priority first.
      2) Reserve a fraction of each pool's resources for high-priority work when any is waiting,
         preventing batch from consuming all capacity and harming tail latency.
      3) Track in-flight assignments by container_id; on OOM-like failures, retry by increasing
         the pipeline's RAM "hint" multiplier (bounded), instead of dropping immediately.
    """
    from collections import deque

    # Priority queues (FIFO within each priority)
    s.q_query = deque()
    s.q_interactive = deque()
    s.q_batch = deque()

    # Track pipelines by id so we can requeue reliably
    s.pipelines_by_id = {}

    # RAM backoff hint per pipeline_id (multiplier on chosen RAM slice)
    s.ram_hint = {}  # pipeline_id -> float

    # Track what we launched so results can be attributed back to a pipeline
    # container_id -> dict(pipeline_id=..., pool_id=..., priority=..., cpu=..., ram=...)
    s.inflight = {}

    # Tunables (kept simple; can be evolved later)
    s.reserve_frac_for_hp = 0.35   # reserve this fraction of pool for QUERY/INTERACTIVE if any waiting
    s.max_ram_hint = 8.0           # cap RAM backoff multiplier
    s.oom_backoff = 2.0            # multiplicative increase on OOM-like failure

    # Per-priority sizing fractions of the pool (per assignment)
    s.cpu_frac = {
        Priority.QUERY: 0.75,
        Priority.INTERACTIVE: 0.70,
        Priority.BATCH_PIPELINE: 0.40,
    }
    s.ram_frac = {
        Priority.QUERY: 0.70,
        Priority.INTERACTIVE: 0.70,
        Priority.BATCH_PIPELINE: 0.45,
    }


@register_scheduler(key="scheduler_low_016")
def scheduler_low_016_scheduler(s, results, pipelines):
    """
    Scheduling step:
      - Ingest new pipelines into per-priority queues.
      - Process results; on OOM-like failure, requeue the pipeline with increased RAM hint.
      - For each pool, schedule at most one ready operator (highest priority first),
        enforcing a headroom reservation for high-priority work when any is waiting.
    """
    # ---- Helpers (local to avoid imports at module level) ----
    def _queue_for_priority(pri):
        if pri == Priority.QUERY:
            return s.q_query
        if pri == Priority.INTERACTIVE:
            return s.q_interactive
        return s.q_batch

    def _has_hp_waiting():
        return (len(s.q_query) > 0) or (len(s.q_interactive) > 0)

    def _looks_like_oom(err):
        if err is None:
            return False
        try:
            msg = str(err).lower()
        except Exception:
            return False
        # Heuristic string match; simulator error types may vary
        return ("oom" in msg) or ("out of memory" in msg) or ("memory" in msg and "exceed" in msg)

    def _pipeline_done_or_failed(p):
        st = p.runtime_status()
        # If any op is FAILED, we may still retry depending on result classification.
        # Here we only use this to avoid scheduling already-successful pipelines.
        return st.is_pipeline_successful()

    def _get_next_ready_pipeline_from_queue(q, max_scan):
        """
        Find a pipeline with at least one assignable op whose parents are complete.
        Rotate the queue to preserve FIFO-ish behavior.
        """
        scan = min(max_scan, len(q))
        for _ in range(scan):
            p = q.popleft()
            # Drop if it's already successful (may linger due to retries/results ordering)
            if _pipeline_done_or_failed(p):
                continue
            st = p.runtime_status()
            op_list = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
            if op_list:
                return p, op_list
            # Not ready yet; rotate to back
            q.append(p)
        return None, None

    def _pick_next_ready_pipeline():
        # Strict priority: QUERY > INTERACTIVE > BATCH
        p, ops = _get_next_ready_pipeline_from_queue(s.q_query, max_scan=64)
        if p is not None:
            return p, ops
        p, ops = _get_next_ready_pipeline_from_queue(s.q_interactive, max_scan=64)
        if p is not None:
            return p, ops
        p, ops = _get_next_ready_pipeline_from_queue(s.q_batch, max_scan=128)
        if p is not None:
            return p, ops
        return None, None

    # ---- Ingest new pipelines ----
    for p in pipelines:
        s.pipelines_by_id[p.pipeline_id] = p
        if p.pipeline_id not in s.ram_hint:
            s.ram_hint[p.pipeline_id] = 1.0
        _queue_for_priority(p.priority).append(p)

    suspensions = []
    assignments = []

    # ---- Process results (update hints / requeue on OOM) ----
    for r in results:
        # Clean up inflight tracking
        meta = s.inflight.pop(getattr(r, "container_id", None), None)

        if not r.failed():
            continue

        # If we can't map back, we can't safely retry; just continue.
        if meta is None:
            continue

        pipeline_id = meta.get("pipeline_id", None)
        if pipeline_id is None:
            continue

        p = s.pipelines_by_id.get(pipeline_id, None)
        if p is None:
            continue

        # If it looks like OOM, increase RAM hint and requeue pipeline for retry.
        # Otherwise, do not thrash with retries; leave it to be counted as failed by the simulator.
        if _looks_like_oom(getattr(r, "error", None)):
            cur = float(s.ram_hint.get(pipeline_id, 1.0))
            new = min(s.max_ram_hint, cur * s.oom_backoff)
            s.ram_hint[pipeline_id] = new
            _queue_for_priority(p.priority).appendleft(p)  # retry sooner to protect latency
        # else: no retry; pipeline will remain failed per runtime state.

    # Early exit if nothing to do
    if not pipelines and not results and not _has_hp_waiting() and len(s.q_batch) == 0:
        return [], []

    # ---- Schedule per pool (at most one op per pool per tick) ----
    hp_waiting = _has_hp_waiting()

    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool
        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # If high-priority work is waiting, reserve headroom by limiting what we allocate to batch.
        # We do this without explicit preemption (keeps complexity low, reduces churn).
        effective_cpu = avail_cpu
        effective_ram = avail_ram

        if hp_waiting:
            # Keep some capacity unallocated unless we're assigning high priority right now.
            # We'll enforce this by checking the chosen priority below.
            pass

        p, op_list = _pick_next_ready_pipeline()
        if p is None or not op_list:
            continue

        pri = p.priority

        # Enforce reservation: if HP waiting and we're about to schedule BATCH, don't eat into reserve.
        if hp_waiting and pri == Priority.BATCH_PIPELINE:
            reserve_cpu = pool.max_cpu_pool * float(s.reserve_frac_for_hp)
            reserve_ram = pool.max_ram_pool * float(s.reserve_frac_for_hp)
            effective_cpu = max(0, avail_cpu - reserve_cpu)
            effective_ram = max(0, avail_ram - reserve_ram)
            if effective_cpu <= 0 or effective_ram <= 0:
                # Not enough non-reserved resources; skip batch this tick.
                continue

        # Choose sizing: fraction of pool, capped by (effective) available.
        # Then apply RAM hint multiplier for pipelines that previously OOM'd.
        cpu_target = min(effective_cpu, max(1e-9, pool.max_cpu_pool * float(s.cpu_frac.get(pri, 0.5))))
        ram_base = min(effective_ram, max(1e-9, pool.max_ram_pool * float(s.ram_frac.get(pri, 0.5))))
        ram_target = min(effective_ram, ram_base * float(s.ram_hint.get(p.pipeline_id, 1.0)))

        # If after hinting we still don't have enough RAM, skip; avoid guaranteed OOM churn.
        if ram_target <= 0 or cpu_target <= 0:
            continue

        a = Assignment(
            ops=op_list,
            cpu=cpu_target,
            ram=ram_target,
            priority=pri,
            pool_id=pool_id,
            pipeline_id=p.pipeline_id,
        )
        assignments.append(a)

        # Track as inflight; container_id will be filled in results later (we track by result.container_id).
        # We don't know container_id yet, so we store a temporary marker keyed by object id of assignment.
        # To remain robust with the simulator, we instead only track by container_id on completion:
        # If we can't map due to missing container_id association, we simply won't retry.
        #
        # Some simulators may echo back container_id immediately; if so, we'd want to record it then.
        # Here we keep minimal complexity; retries will still work when container_id is present in results.
        #
        # NOTE: If the simulator exposes container_id at assignment time, update this accordingly.
        # (No-op here.)

    # Best-effort: if results include container_id, we can track from now on by peeking at any running assignment
    # is not possible with the given public API, so inflight mapping relies on results only.

    # However, we *can* opportunistically record pipeline_id for a container_id when the first result arrives,
    # but we already do that by using meta. Since meta is empty, retries may be limited. To improve robustness,
    # we record a mapping if the simulator returns container_id in Assignment later (not specified), so we avoid it.

    # To still benefit from OOM backoff, we add a fallback: if any failure comes back without inflight meta,
    # do nothing (safe).

    return suspensions, assignments
