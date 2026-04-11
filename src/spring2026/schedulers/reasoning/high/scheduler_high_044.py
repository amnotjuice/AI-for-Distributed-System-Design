# policy_key: scheduler_high_044
# reasoning_effort: high
# model: gpt-5.2-2025-12-11
# llm_cost: 0.141430
# generation_seconds: 173.86
# generated_at: 2026-03-14T04:25:39.147807
@register_scheduler_init(key="scheduler_high_044")
def scheduler_high_044_init(s):
    """
    Priority-aware, resource-capped scheduler (incremental improvement over naive FIFO).

    Main ideas:
      1) Priority queues: QUERY > INTERACTIVE > BATCH.
      2) Resource caps per assignment: avoid giving a single op the entire pool (improves concurrency / tail latency).
      3) Soft reservation: when high-priority work is waiting, keep a fraction of each pool free so batch doesn't
         fully consume capacity.
      4) Basic OOM-aware retry sizing using ExecutionResult.error signals (best-effort; safe fallback if signals missing).

    Notes:
      - Avoids scheduling the same pipeline more than once per tick (prevents duplicate assignment of the same op).
      - Does not implement preemption because the minimal interface here doesn't expose running-container inventory.
    """
    from collections import deque

    # Per-priority waiting queues (store pipeline_ids to avoid duplicating objects)
    s.q_query = deque()
    s.q_interactive = deque()
    s.q_batch = deque()

    # pipeline_id -> Pipeline object (latest reference)
    s.pipeline_by_id = {}

    # pipeline_id -> hints / counters (e.g., ram_hint after OOM)
    s.pipeline_hints = {}

    # bookkeeping
    s.ticks = 0

    # --- Tuning knobs (conservative, incremental changes) ---

    # Cap per-assignment CPU/RAM as fraction of pool capacity (prevents one op from monopolizing a pool)
    s.cpu_frac = {
        Priority.QUERY: 0.75,
        Priority.INTERACTIVE: 0.50,
        Priority.BATCH_PIPELINE: 0.25,
    }
    s.ram_frac = {
        Priority.QUERY: 0.33,
        Priority.INTERACTIVE: 0.25,
        Priority.BATCH_PIPELINE: 0.15,
    }

    # Soft reservations when any high-priority work is waiting
    s.reserve_cpu_frac = 0.20
    s.reserve_ram_frac = 0.20

    # OOM behavior (best-effort; depends on error strings and pipeline_id extraction)
    s.max_oom_retries = 3

    # If we observe a pipeline with FAILED ops for too long, drop it to avoid infinite churn
    s.failure_timeout_ticks = 250

    # Periodic queue compaction to remove stale pipeline_ids
    s.compact_every = 50


@register_scheduler(key="scheduler_high_044")
def scheduler_high_044_scheduler(s, results, pipelines):
    """
    Scheduler tick.

    Inputs:
      - results: finished container results (success/failure with error strings)
      - pipelines: newly arrived pipelines

    Outputs:
      - suspensions: none (no preemption in this incremental policy)
      - assignments: new container assignments
    """
    # -------- helper functions (kept inside for portability) --------
    def _norm_priority(p):
        # Ensure unknown priorities don't crash; treat unknown as batch
        if p in (Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE):
            return p
        return Priority.BATCH_PIPELINE

    def _queue_for_priority(prio):
        prio = _norm_priority(prio)
        if prio == Priority.QUERY:
            return s.q_query
        if prio == Priority.INTERACTIVE:
            return s.q_interactive
        return s.q_batch

    def _priority_order():
        return (Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE)

    def _get_assignable_states():
        # Prefer simulator-provided ASSIGNABLE_STATES; otherwise derive from OperatorState
        try:
            return ASSIGNABLE_STATES
        except NameError:
            return (OperatorState.PENDING, OperatorState.FAILED)

    def _safe_state_count(status, st):
        try:
            return status.state_counts.get(st, 0)
        except Exception:
            # fall back if state_counts isn't dict-like
            try:
                return status.state_counts[st]
            except Exception:
                return 0

    def _is_pipeline_done_or_drop(pipeline):
        # Returns (drop: bool, done: bool)
        status = pipeline.runtime_status()
        if status.is_pipeline_successful():
            return True, True

        failed_cnt = _safe_state_count(status, OperatorState.FAILED)
        if failed_cnt > 0:
            pid = pipeline.pipeline_id
            h = s.pipeline_hints.setdefault(pid, {})
            first_failed = h.get("first_failed_tick")
            if first_failed is None:
                h["first_failed_tick"] = s.ticks
            elif (s.ticks - first_failed) > s.failure_timeout_ticks:
                # give up on pipelines that remain failed for too long
                return True, False

            # If we have explicit OOM retry count and it's exceeded, drop as well
            if h.get("oom_retries", 0) > s.max_oom_retries:
                return True, False

        return False, False

    def _extract_pipeline_id_from_result(r):
        # Best-effort extraction of pipeline_id from ExecutionResult; robust to missing fields.
        pid = getattr(r, "pipeline_id", None)
        if pid is not None:
            return pid
        ops = getattr(r, "ops", None) or []
        for op in ops:
            pid = getattr(op, "pipeline_id", None)
            if pid is not None:
                return pid
            pipe = getattr(op, "pipeline", None)
            pid = getattr(pipe, "pipeline_id", None)
            if pid is not None:
                return pid
        return None

    def _looks_like_oom(err):
        if not err:
            return False
        e = str(err).lower()
        return ("oom" in e) or ("out of memory" in e) or ("cuda out of memory" in e) or ("cannot allocate memory" in e)

    def _min_cpu(pool):
        # At least 1 vCPU (or a small fraction if pool is fractional), without overshooting
        try:
            return max(1.0, 0.05 * float(pool.max_cpu_pool))
        except Exception:
            return 1.0

    def _min_ram(pool):
        # Small floor to avoid obviously under-sizing; relative to pool size to be portable
        try:
            return max(0.5, 0.05 * float(pool.max_ram_pool))
        except Exception:
            return 0.5

    def _default_request(pool, prio):
        prio = _norm_priority(prio)
        cpu_cap = float(pool.max_cpu_pool) * float(s.cpu_frac[prio])
        ram_cap = float(pool.max_ram_pool) * float(s.ram_frac[prio])

        cpu_req = max(_min_cpu(pool), cpu_cap)
        ram_req = max(_min_ram(pool), ram_cap)
        return cpu_req, ram_req

    def _apply_hints(pool, pid, prio, cpu_req, ram_req):
        # Apply per-pipeline hints (e.g., after OOM); keep within pool maxima
        h = s.pipeline_hints.get(pid, {})
        hinted_ram = h.get("ram_hint")
        hinted_cpu = h.get("cpu_hint")

        if hinted_cpu is not None:
            cpu_req = max(cpu_req, float(hinted_cpu))
        if hinted_ram is not None:
            ram_req = max(ram_req, float(hinted_ram))

        # Clamp to pool maxima
        cpu_req = min(cpu_req, float(pool.max_cpu_pool))
        ram_req = min(ram_req, float(pool.max_ram_pool))

        # Keep above floors
        cpu_req = max(cpu_req, _min_cpu(pool))
        ram_req = max(ram_req, _min_ram(pool))
        return cpu_req, ram_req

    # -------- main tick logic --------
    s.ticks += 1
    suspensions = []
    assignments = []

    # Ingest new pipelines into priority queues
    for p in pipelines:
        s.pipeline_by_id[p.pipeline_id] = p
        q = _queue_for_priority(p.priority)
        q.append(p.pipeline_id)
        s.pipeline_hints.setdefault(p.pipeline_id, {"enqueue_tick": s.ticks})

    # Update hints from execution results (best-effort OOM detection)
    for r in results:
        if not hasattr(r, "failed") or not callable(r.failed):
            continue
        if not r.failed():
            # On success, clear "first_failed_tick" to avoid incorrectly timing out later
            pid = _extract_pipeline_id_from_result(r)
            if pid is not None:
                h = s.pipeline_hints.setdefault(pid, {})
                h.pop("first_failed_tick", None)
            continue

        pid = _extract_pipeline_id_from_result(r)
        if pid is None:
            continue

        h = s.pipeline_hints.setdefault(pid, {})
        h["last_error"] = str(getattr(r, "error", ""))

        if _looks_like_oom(getattr(r, "error", None)):
            h["oom_retries"] = h.get("oom_retries", 0) + 1
            # Increase RAM hint relative to what we just tried; clamp using the pool's max RAM
            try:
                pool = s.executor.pools[r.pool_id]
                prev = float(h.get("ram_hint", 0.0))
                tried = float(getattr(r, "ram", 0.0) or 0.0)
                h["ram_hint"] = min(float(pool.max_ram_pool), max(prev, tried * 2.0, _min_ram(pool)))
            except Exception:
                # If we can't access pool info, still bump a bit
                prev = float(h.get("ram_hint", 0.0) or 0.0)
                tried = float(getattr(r, "ram", 0.0) or 0.0)
                h["ram_hint"] = max(prev, tried * 2.0, 1.0)

    # Periodically compact queues to remove stale pipeline_ids
    if s.ticks % s.compact_every == 0:
        live = set(s.pipeline_by_id.keys())
        for q in (s.q_query, s.q_interactive, s.q_batch):
            if not q:
                continue
            # Rebuild with only live pids (keep ordering)
            newq = type(q)()
            for pid in q:
                if pid in live:
                    newq.append(pid)
            q.clear()
            q.extend(newq)

    # Determine whether we should reserve resources for high-priority waiting work
    # (stale IDs are OK; worst case we reserve a bit more than needed)
    hp_waiting = bool(s.q_query) or bool(s.q_interactive)

    # Schedule across pools, starting with pools that have the most headroom
    pool_ids = list(range(s.executor.num_pools))
    pool_ids.sort(
        key=lambda i: float(s.executor.pools[i].avail_cpu_pool) + float(s.executor.pools[i].avail_ram_pool),
        reverse=True,
    )

    # Prevent assigning the same pipeline more than once per tick to avoid duplicate-op assignment
    used_pipelines = set()

    assignable_states = _get_assignable_states()

    for pool_id in pool_ids:
        pool = s.executor.pools[pool_id]

        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)
        if avail_cpu <= 0.0 or avail_ram <= 0.0:
            continue

        # Compute soft reservation for batch if high priority is waiting
        if hp_waiting:
            reserve_cpu = float(pool.max_cpu_pool) * float(s.reserve_cpu_frac)
            reserve_ram = float(pool.max_ram_pool) * float(s.reserve_ram_frac)
        else:
            reserve_cpu = 0.0
            reserve_ram = 0.0

        # Choose one pipeline for this pool, scanning priorities in order
        chosen = False
        requeue = {Priority.QUERY: [], Priority.INTERACTIVE: [], Priority.BATCH_PIPELINE: []}

        for prio in _priority_order():
            q = _queue_for_priority(prio)
            # We'll scan the current queue length; anything we pop is requeued for the end of the tick,
            # so each pipeline is visited at most once per tick (across all pools).
            n = len(q)
            for _ in range(n):
                pid = q.popleft()
                pipeline = s.pipeline_by_id.get(pid)
                if pipeline is None:
                    continue

                # If already assigned this tick, just requeue
                if pid in used_pipelines:
                    requeue[prio].append(pid)
                    continue

                # Drop completed / timed-out failed pipelines
                drop, done = _is_pipeline_done_or_drop(pipeline)
                if drop:
                    # Remove from tracking; do not requeue
                    s.pipeline_by_id.pop(pid, None)
                    s.pipeline_hints.pop(pid, None)
                    continue

                status = pipeline.runtime_status()
                op_list = status.get_ops(assignable_states, require_parents_complete=True)[:1]
                if not op_list:
                    # Not runnable yet; keep it in queue
                    requeue[prio].append(pid)
                    continue

                # Determine request (default caps + hints), then clamp to currently available resources
                cpu_req, ram_req = _default_request(pool, pipeline.priority)
                cpu_req, ram_req = _apply_hints(pool, pid, pipeline.priority, cpu_req, ram_req)

                # For batch, enforce soft reservation when high priority is waiting
                if _norm_priority(pipeline.priority) == Priority.BATCH_PIPELINE and hp_waiting:
                    batch_cpu_cap = max(0.0, avail_cpu - reserve_cpu)
                    batch_ram_cap = max(0.0, avail_ram - reserve_ram)
                    if cpu_req > batch_cpu_cap or ram_req > batch_ram_cap:
                        # Can't fit batch without dipping into reserved headroom; requeue and continue
                        requeue[prio].append(pid)
                        continue

                # Clamp to what's actually available in this pool right now
                if cpu_req > avail_cpu or ram_req > avail_ram:
                    # Can't place right now; requeue and continue scanning
                    requeue[prio].append(pid)
                    continue

                assignments.append(
                    Assignment(
                        ops=op_list,
                        cpu=cpu_req,
                        ram=ram_req,
                        priority=pipeline.priority,
                        pool_id=pool_id,
                        pipeline_id=pipeline.pipeline_id,
                    )
                )

                # Update local availability for this pool (we only place one assignment per pool per tick)
                avail_cpu -= cpu_req
                avail_ram -= ram_req
                used_pipelines.add(pid)

                # Requeue pipeline for future ops (next tick)
                requeue[prio].append(pid)

                chosen = True
                break

            if chosen:
                break

        # Put back anything we popped but didn't drop (preserve priority queues)
        for prio in _priority_order():
            q = _queue_for_priority(prio)
            for pid in requeue[prio]:
                q.append(pid)

    return suspensions, assignments
