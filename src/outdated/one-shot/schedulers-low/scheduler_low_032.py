# policy_key: scheduler_low_032
# reasoning_effort: low
# model: gpt-5.2-2025-12-11
# llm_cost: 0.038513
# generation_seconds: 47.61
# generated_at: 2026-03-14T02:27:08.213172
@register_scheduler_init(key="scheduler_low_032")
def scheduler_low_032_init(s):
    """Priority-aware, latency-first scheduler with conservative right-sizing.

    Small improvements over naive FIFO:
      - Maintain separate waiting queues by priority; always schedule higher priority first.
      - Avoid giving one operator the entire pool by default; use per-priority CPU/RAM caps.
      - Simple OOM-aware retry: if an operator fails due to OOM, increase RAM request and retry.
      - Leave a small headroom in pools when high-priority work is waiting (reduces tail latency).
    """
    # Queues per priority (higher first at scheduling time)
    s.waiting_by_prio = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }

    # Per-pipeline resource hints (learned from past attempts)
    # pipeline_id -> {"ram": float, "cpu": float}
    s.pipeline_hints = {}

    # Track pipelines we have seen to avoid pathological re-enqueue patterns
    s.seen_pipelines = set()

    # Tuning knobs (kept deliberately simple)
    s.oom_ram_growth = 1.6
    s.max_retries_per_op = 5  # best-effort cap to avoid infinite loops

    # Track retry counts per (pipeline_id, op_id)
    s.retry_counts = {}  # (pipeline_id, op_id) -> int


@register_scheduler(key="scheduler_low_032")
def scheduler_low_032_scheduler(s, results, pipelines):
    """
    Scheduler step:
      1) Ingest new pipelines into priority queues.
      2) Process results to update RAM hints on OOM failures.
      3) For each pool, schedule at most one ready operator from highest priority queues.
         - Use per-priority CPU/RAM fractions rather than "all available".
         - If high-priority waiting exists, keep headroom in each pool.
    """
    def _prio_order():
        return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]

    def _is_oom_error(err):
        if err is None:
            return False
        try:
            msg = str(err).lower()
        except Exception:
            return False
        return ("oom" in msg) or ("out of memory" in msg) or ("memoryerror" in msg)

    def _clamp(x, lo, hi):
        return max(lo, min(hi, x))

    def _get_one_assignable_op(p):
        status = p.runtime_status()
        # Only schedule ops whose parents are complete
        ops = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        if not ops:
            return None
        return ops[0]

    def _pipeline_done_or_failed(p):
        status = p.runtime_status()
        if status.is_pipeline_successful():
            return True
        # If any operator failed, we still might retry if we believe it was OOM.
        # We therefore do NOT drop pipelines solely because FAILED exists here.
        return False

    def _any_high_prio_waiting():
        return bool(s.waiting_by_prio[Priority.QUERY] or s.waiting_by_prio[Priority.INTERACTIVE])

    def _base_fractions(priority):
        # Conservative caps to reduce interference and improve latency for small/high-priority queries.
        # (cpu_frac, ram_frac)
        if priority == Priority.QUERY:
            return 0.5, 0.30
        if priority == Priority.INTERACTIVE:
            return 0.7, 0.45
        return 1.0, 0.85  # batch uses more by default, but not 100% RAM to reduce OOM cascade

    def _headroom_fractions(priority):
        # When high-priority work is waiting, keep headroom even while scheduling.
        # This helps reduce p95/p99 by preventing total saturation.
        if priority in (Priority.QUERY, Priority.INTERACTIVE):
            return 0.05, 0.05
        return 0.15, 0.15

    def _ensure_hint(pipeline_id):
        if pipeline_id not in s.pipeline_hints:
            s.pipeline_hints[pipeline_id] = {"ram": None, "cpu": None}

    def _op_key(pipeline_id, op):
        # Best-effort stable key for retry counters
        op_id = getattr(op, "op_id", None)
        if op_id is None:
            op_id = getattr(op, "operator_id", None)
        if op_id is None:
            op_id = id(op)
        return (pipeline_id, op_id)

    # Ingest new pipelines into queues
    for p in pipelines:
        # Only enqueue if not already complete
        if _pipeline_done_or_failed(p):
            continue
        if p.priority not in s.waiting_by_prio:
            # Unknown priority: treat as batch
            s.waiting_by_prio[Priority.BATCH_PIPELINE].append(p)
        else:
            s.waiting_by_prio[p.priority].append(p)
        s.seen_pipelines.add(p.pipeline_id)
        _ensure_hint(p.pipeline_id)

    # Early exit if nothing changed
    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    # Process results: update hints on OOM failures (RAM-first approach)
    for r in results:
        if not hasattr(r, "failed") or not callable(r.failed):
            continue
        if not r.failed():
            continue

        # Attempt to detect OOM; on OOM, increase RAM hint and requeue by leaving pipeline in queue.
        if _is_oom_error(getattr(r, "error", None)):
            pid = getattr(r, "pipeline_id", None)
            # pipeline_id may not be present on result in some implementations; best-effort fallback.
            # If missing, we can't update hints safely.
            if pid is not None:
                _ensure_hint(pid)
                prev_ram = s.pipeline_hints[pid].get("ram", None)
                used_ram = getattr(r, "ram", None)
                # Grow from observed allocation if available; else grow previous hint; else start at 25% pool.
                if used_ram is not None:
                    new_ram = float(used_ram) * s.oom_ram_growth
                elif prev_ram is not None:
                    new_ram = float(prev_ram) * s.oom_ram_growth
                else:
                    new_ram = None  # will be set when we see a pool to run on
                s.pipeline_hints[pid]["ram"] = new_ram

        # Non-OOM failures: no special handling here; the pipeline may still be retried depending on runtime state.

    # Helper to pop next runnable pipeline from a queue (round-robin-ish)
    def _pop_next_runnable_from_queue(q):
        # Rotate through queue to find a pipeline with a ready op; preserve order otherwise.
        for _ in range(len(q)):
            p = q.pop(0)
            if _pipeline_done_or_failed(p):
                continue
            op = _get_one_assignable_op(p)
            if op is None:
                # Not runnable now; keep it for later
                q.append(p)
                continue
            # Runnable; put pipeline back (it may have more ops later) and return op
            q.append(p)
            return p, op
        return None, None

    # Schedule per pool: one assignment per pool per tick (simple, stable, latency-friendly)
    high_waiting = _any_high_prio_waiting()

    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)

        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # Pick highest-priority runnable pipeline/op
        chosen = None
        chosen_prio = None
        chosen_op = None

        for pr in _prio_order():
            # If high-priority is waiting, optionally gate batch to reduce latency interference.
            if high_waiting and pr == Priority.BATCH_PIPELINE:
                continue
            p, op = _pop_next_runnable_from_queue(s.waiting_by_prio[pr])
            if p is not None:
                chosen = p
                chosen_prio = pr
                chosen_op = op
                break

        if chosen is None:
            continue

        # Determine CPU/RAM request using fractions + learned hints
        cpu_frac, ram_frac = _base_fractions(chosen_prio)
        hr_cpu, hr_ram = _headroom_fractions(chosen_prio) if high_waiting else (0.0, 0.0)

        # Keep headroom in the pool (soft), but never below a tiny minimum
        effective_cpu = max(0.0, avail_cpu - pool.max_cpu_pool * hr_cpu)
        effective_ram = max(0.0, avail_ram - pool.max_ram_pool * hr_ram)

        # If headroom makes effective resources zero, fall back to whatever is available
        if effective_cpu <= 0:
            effective_cpu = avail_cpu
        if effective_ram <= 0:
            effective_ram = avail_ram

        _ensure_hint(chosen.pipeline_id)
        hint_cpu = s.pipeline_hints[chosen.pipeline_id].get("cpu", None)
        hint_ram = s.pipeline_hints[chosen.pipeline_id].get("ram", None)

        # Requested sizes: use hint if present; else use fractions of pool capacity (not just current avail)
        req_cpu = hint_cpu if hint_cpu is not None else (pool.max_cpu_pool * cpu_frac)
        req_ram = hint_ram if hint_ram is not None else (pool.max_ram_pool * ram_frac)

        # Clamp to current effective availability
        req_cpu = _clamp(float(req_cpu), 1.0, effective_cpu) if effective_cpu >= 1.0 else effective_cpu
        req_ram = _clamp(float(req_ram), 0.25, effective_ram) if effective_ram >= 0.25 else effective_ram

        # Retry cap per op: if too many OOM retries, stop increasing and just try max we can
        opk = _op_key(chosen.pipeline_id, chosen_op)
        retries = s.retry_counts.get(opk, 0)
        if retries >= s.max_retries_per_op:
            req_ram = effective_ram

        # If we still can't allocate meaningful resources, skip this pool this tick
        if req_cpu <= 0 or req_ram <= 0:
            continue

        # If assignment fails due to OOM, we'll learn from results and grow.
        # Record that we are attempting this op (retry counter increments on observed OOM next time;
        # but we don't have that per-op linkage reliably, so we increment when we see FAILED+OOM via op key).
        assignment = Assignment(
            ops=[chosen_op],
            cpu=req_cpu,
            ram=req_ram,
            priority=chosen.priority,
            pool_id=pool_id,
            pipeline_id=chosen.pipeline_id,
        )
        assignments.append(assignment)

    # Update retry counters for OOM failures using result.op identity when possible
    for r in results:
        if not hasattr(r, "failed") or not callable(r.failed) or not r.failed():
            continue
        if not _is_oom_error(getattr(r, "error", None)):
            continue

        pid = getattr(r, "pipeline_id", None)
        # If result doesn't carry pipeline_id, we cannot safely track per-op retries
        if pid is None:
            continue

        for op in getattr(r, "ops", []) or []:
            opk = _op_key(pid, op)
            s.retry_counts[opk] = s.retry_counts.get(opk, 0) + 1

    return suspensions, assignments
