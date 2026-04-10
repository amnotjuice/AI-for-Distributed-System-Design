# policy_key: scheduler_high_045
# reasoning_effort: high
# model: gpt-5.2-2025-12-11
# llm_cost: 0.145459
# generation_seconds: 95.35
# generated_at: 2026-03-14T04:27:14.494525
@register_scheduler_init(key="scheduler_high_045")
def scheduler_high_045_init(s):
    """Priority-aware, low-risk improvement over naive FIFO.

    Main changes vs. naive:
      1) Priority queues: always prefer QUERY > INTERACTIVE > BATCH.
      2) Better fairness: round-robin within a priority; occasional anti-starvation for batch.
      3) OOM-aware retries: if an operator fails with an OOM-like error, retry it with higher RAM next time.
      4) Avoid head-of-line blocking: don't give an entire pool to a single operator by default; size per priority.

    Notes:
      - This policy deliberately avoids complex preemption because the minimal public interface
        doesn't expose a reliable list of running containers to suspend.
      - It still improves latency by ensuring high-priority work is admitted/scheduled first and
        by reducing the chance high-priority work sits behind a long batch assignment.
    """
    s.ticks = 0

    # Per-priority FIFO queues (we'll rotate items to get round-robin fairness).
    s.wait_q = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }

    # Track which pipeline_ids are currently enqueued (avoid duplicates).
    s.enqueued = set()

    # Metadata per pipeline_id (for aging / starvation prevention).
    # { pipeline_id: {"enqueued_tick": int} }
    s.pipeline_meta = {}

    # If an op fails with an OOM-like error, we store a higher RAM hint here:
    # key: (pipeline_id, op_key) -> ram_amount
    s.op_ram_hint = {}

    # Map op_key -> pipeline_id so we can attribute ExecutionResult back to a pipeline.
    s.op_to_pipeline = {}

    # Pipelines with non-OOM failures are blacklisted (don't retry forever).
    s.dead_pipelines = set()

    # Anti-starvation knobs
    s.batch_every = 7            # roughly, every N ticks let batch contend earlier
    s.batch_starve_ticks = 25    # if a batch pipeline waits longer than this, boost it


@register_scheduler(key="scheduler_high_045")
def scheduler_high_045(s, results, pipelines):
    """Scheduler step.

    Strategy:
      - Ingest new pipelines into per-priority queues.
      - Use results to detect OOM and increase RAM hint for the failing operator on retry.
      - For each pool, schedule as many single-op assignments as resources allow:
          * priority order (with slight batch anti-starvation)
          * at most 1 op per pipeline per tick (prevents one pipeline from hogging a tick)
          * conservative default sizing with per-op RAM bump on OOM
    """
    s.ticks += 1

    def _op_key(op):
        # Prefer a stable operator identifier if available; else fall back to object id.
        return getattr(op, "op_id", None) or getattr(op, "operator_id", None) or id(op)

    def _is_oom_error(err):
        if err is None:
            return False
        msg = str(err).lower()
        return ("oom" in msg) or ("out of memory" in msg) or ("memoryerror" in msg)

    def _queue_for_priority(pri):
        if pri == Priority.QUERY:
            return s.wait_q[Priority.QUERY]
        if pri == Priority.INTERACTIVE:
            return s.wait_q[Priority.INTERACTIVE]
        return s.wait_q[Priority.BATCH_PIPELINE]

    def _drop_pipeline(pipeline_id):
        # Remove bookkeeping; the actual object may still be present in lists (we'll skip it).
        s.enqueued.discard(pipeline_id)
        s.pipeline_meta.pop(pipeline_id, None)

    def _pipeline_wait_ticks(pipeline_id):
        meta = s.pipeline_meta.get(pipeline_id)
        if not meta:
            return 0
        return max(0, s.ticks - meta.get("enqueued_tick", s.ticks))

    def _priority_pick_order():
        # Default: QUERY > INTERACTIVE > BATCH
        # Anti-starvation: occasionally allow batch to be considered before interactive,
        # and always boost very old batch to the front of the line (after queries).
        batch_boost = False
        if s.ticks % s.batch_every == 0:
            batch_boost = True
        else:
            # If any batch has waited too long, boost.
            for bp in s.wait_q[Priority.BATCH_PIPELINE]:
                if _pipeline_wait_ticks(bp.pipeline_id) >= s.batch_starve_ticks:
                    batch_boost = True
                    break

        if batch_boost:
            return [Priority.QUERY, Priority.BATCH_PIPELINE, Priority.INTERACTIVE]
        return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]

    def _default_ram_frac(pri):
        # Start modest to reduce head-of-line blocking; OOM bumps will correct underestimates.
        if pri == Priority.QUERY:
            return 0.35
        if pri == Priority.INTERACTIVE:
            return 0.30
        return 0.20

    def _default_cpu_frac(pri):
        # Give higher priority more CPU to improve latency.
        if pri == Priority.QUERY:
            return 0.80
        if pri == Priority.INTERACTIVE:
            return 0.60
        return 0.40

    def _size_request(pool, pri, pipeline_id, op):
        # Determine cpu/ram request using pool size, priority defaults, and OOM-derived hints.
        opk = _op_key(op)

        # RAM
        hint = s.op_ram_hint.get((pipeline_id, opk), None)
        if hint is None:
            ram_req = pool.max_ram_pool * _default_ram_frac(pri)
        else:
            ram_req = hint

        # CPU
        cpu_req = pool.max_cpu_pool * _default_cpu_frac(pri)

        # Enforce minimums (avoid zero-sized allocations)
        if cpu_req < 1:
            cpu_req = 1
        if ram_req < 1:
            ram_req = 1

        # Clip to pool capacity (and later to availability)
        if cpu_req > pool.max_cpu_pool:
            cpu_req = pool.max_cpu_pool
        if ram_req > pool.max_ram_pool:
            ram_req = pool.max_ram_pool

        return cpu_req, ram_req

    # --- Ingest new pipelines ---
    for p in pipelines:
        if p.pipeline_id in s.dead_pipelines:
            continue
        if p.pipeline_id in s.enqueued:
            continue
        q = _queue_for_priority(p.priority)
        q.append(p)
        s.enqueued.add(p.pipeline_id)
        s.pipeline_meta[p.pipeline_id] = {"enqueued_tick": s.ticks}

    # --- Process results (OOM-aware RAM bumps; blacklist non-OOM failures) ---
    for r in results:
        # Attribute results to a pipeline using the op->pipeline mapping created at assignment time.
        for op in getattr(r, "ops", []) or []:
            opk = _op_key(op)
            pipeline_id = s.op_to_pipeline.get(opk, None)
            if pipeline_id is None:
                continue

            if hasattr(r, "failed") and r.failed():
                if _is_oom_error(getattr(r, "error", None)):
                    # Increase RAM hint aggressively to converge quickly.
                    # Use reported allocation if present, else fall back to previous hint.
                    prev = s.op_ram_hint.get((pipeline_id, opk), None)
                    alloc = getattr(r, "ram", None)
                    base = alloc if (alloc is not None and alloc > 0) else (prev if prev is not None else 1)
                    new_hint = max(base * 2, (prev * 2 if prev is not None else 0))
                    s.op_ram_hint[(pipeline_id, opk)] = new_hint
                else:
                    # Non-OOM failure: don't keep retrying forever.
                    s.dead_pipelines.add(pipeline_id)
                    _drop_pipeline(pipeline_id)

    # Early exit if nothing changed.
    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    # Avoid scheduling multiple ops from the same pipeline in the same tick.
    scheduled_pipeline_ids = set()

    # Helper to rotate through a priority queue and find a schedulable op.
    def _pick_next_op_from_queue(q, pool, avail_cpu, avail_ram):
        """Return (pipeline, op, cpu_req, ram_req) or (None, None, None, None).
        This rotates the queue for round-robin fairness.
        """
        if not q:
            return None, None, None, None

        n = len(q)
        for _ in range(n):
            p = q.pop(0)  # rotate
            pid = p.pipeline_id

            # Drop dead pipelines
            if pid in s.dead_pipelines:
                continue

            # One-op-per-pipeline-per-tick
            if pid in scheduled_pipeline_ids:
                q.append(p)
                continue

            status = p.runtime_status()

            # If completed, drop it
            if status.is_pipeline_successful():
                _drop_pipeline(pid)
                continue

            # If the pipeline has no runnable ops right now, keep it in queue
            op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
            if not op_list:
                q.append(p)
                continue

            op = op_list[0]

            # Size request and see if it can fit in this pool's current availability.
            cpu_req, ram_req = _size_request(pool, p.priority, pid, op)
            cpu_req = min(cpu_req, avail_cpu)
            ram_req = min(ram_req, avail_ram)

            # If we can't even give minimums, keep it queued and try another pipeline.
            if cpu_req < 1 or ram_req < 1:
                q.append(p)
                continue

            # If this op has a known RAM hint that's bigger than avail, it won't fit here now.
            # Keep it queued and try another pipeline; it may fit in another pool or later tick.
            hint = s.op_ram_hint.get((pid, _op_key(op)), None)
            if hint is not None and hint > avail_ram:
                q.append(p)
                continue

            # Success: keep pipeline in queue (it likely has more ops later), but mark scheduled.
            q.append(p)
            return p, op, cpu_req, ram_req

        return None, None, None, None

    # --- Main scheduling loop (per pool, fill with priority-aware small batches) ---
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool

        # Keep scheduling until the pool is effectively full.
        # (CPU/RAM minimum enforced in sizing.)
        while avail_cpu >= 1 and avail_ram >= 1:
            picked = None

            for pri in _priority_pick_order():
                q = _queue_for_priority(pri)
                p, op, cpu_req, ram_req = _pick_next_op_from_queue(q, pool, avail_cpu, avail_ram)
                if p is None:
                    continue

                # Create assignment of a single operator.
                assignment = Assignment(
                    ops=[op],
                    cpu=cpu_req,
                    ram=ram_req,
                    priority=p.priority,
                    pool_id=pool_id,
                    pipeline_id=p.pipeline_id,
                )
                assignments.append(assignment)

                # Track mapping so results can be attributed.
                s.op_to_pipeline[_op_key(op)] = p.pipeline_id

                # Update availability and tick-local state.
                avail_cpu -= cpu_req
                avail_ram -= ram_req
                scheduled_pipeline_ids.add(p.pipeline_id)

                picked = True
                break

            if not picked:
                break

    return suspensions, assignments
