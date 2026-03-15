# policy_key: scheduler_low_004
# reasoning_effort: low
# model: gpt-5.2-2025-12-11
# llm_cost: 0.050847
# generation_seconds: 52.12
# generated_at: 2026-03-12T21:37:09.772840
@register_scheduler_init(key="scheduler_low_004")
def scheduler_low_004_init(s):
    """
    Priority-aware FIFO with small, safe improvements over the naive baseline:

    1) Priority queues: always try QUERY/INTERACTIVE before BATCH.
    2) More conservative per-op sizing: don't hand an op the entire pool by default.
       This reduces head-of-line blocking and improves latency under contention.
    3) Simple OOM-aware retry: if we see an OOM failure in results, increase the
       RAM request for that pipeline on subsequent attempts (bounded by pool size).
    4) Mild aging: if batch waits long enough, allow it to occasionally jump ahead
       to avoid starvation (without hurting interactive tail too much).

    Notes:
    - No preemption yet (kept intentionally simple and robust to unknown executor APIs).
    - One ready operator per pipeline per assignment (keeps interactivity snappy).
    """
    from collections import deque

    s.q_query = deque()
    s.q_interactive = deque()
    s.q_batch = deque()

    # Per-pipeline adaptive hints.
    # ram_boost multiplies a baseline RAM request after OOM.
    s.ram_boost = {}  # pipeline_id -> float
    s.oom_count = {}  # pipeline_id -> int

    # Simple aging: record enqueue tick and occasionally promote batch.
    s.enqueue_tick = {}  # pipeline_id -> int
    s.tick = 0

    # Track pipelines we should drop (non-OOM failures), so we don't spin.
    s.dead_pipelines = set()

    # Tunables (kept conservative).
    s.MIN_CPU = 1.0
    s.MIN_RAM = 1.0

    # Caps as fractions of a pool for a single op; improves latency vs "take all".
    s.CPU_CAP_FRAC = {
        Priority.QUERY: 0.50,
        Priority.INTERACTIVE: 0.60,
        Priority.BATCH_PIPELINE: 0.75,
    }
    s.RAM_CAP_FRAC = {
        Priority.QUERY: 0.50,
        Priority.INTERACTIVE: 0.60,
        Priority.BATCH_PIPELINE: 0.80,
    }

    # Baseline request as fractions (before caps); small by default to pack better.
    s.CPU_BASE_FRAC = {
        Priority.QUERY: 0.25,
        Priority.INTERACTIVE: 0.30,
        Priority.BATCH_PIPELINE: 0.40,
    }
    s.RAM_BASE_FRAC = {
        Priority.QUERY: 0.25,
        Priority.INTERACTIVE: 0.30,
        Priority.BATCH_PIPELINE: 0.40,
    }

    # Aging: after this many ticks, batch can be promoted occasionally.
    s.BATCH_AGING_TICKS = 30
    s.BATCH_PROMOTE_EVERY = 3  # promote 1 batch every N scheduling ticks if aged


@register_scheduler(key="scheduler_low_004")
def scheduler_low_004_scheduler(s, results, pipelines):
    """
    Scheduler step:
    - Enqueue new pipelines into per-priority queues.
    - Process execution results: detect OOMs and adapt RAM requests for retries.
    - For each pool, repeatedly try to schedule one ready op at a time, prioritizing
      interactive work while allowing limited aging-based batch promotion.
    """
    from collections import deque

    def _prio_queue(priority):
        if priority == Priority.QUERY:
            return s.q_query
        if priority == Priority.INTERACTIVE:
            return s.q_interactive
        return s.q_batch

    def _is_oom_error(err):
        if err is None:
            return False
        try:
            msg = str(err).lower()
        except Exception:
            return False
        return ("oom" in msg) or ("out of memory" in msg) or ("memoryerror" in msg)

    def _enqueue_pipeline(p):
        if p.pipeline_id in s.dead_pipelines:
            return
        q = _prio_queue(p.priority)
        q.append(p)
        if p.pipeline_id not in s.ram_boost:
            s.ram_boost[p.pipeline_id] = 1.0
        if p.pipeline_id not in s.oom_count:
            s.oom_count[p.pipeline_id] = 0
        if p.pipeline_id not in s.enqueue_tick:
            s.enqueue_tick[p.pipeline_id] = s.tick

    def _pipeline_done_or_dead(p):
        if p.pipeline_id in s.dead_pipelines:
            return True
        st = p.runtime_status()
        if st.is_pipeline_successful():
            return True
        return False

    def _has_ready_op(p):
        st = p.runtime_status()
        ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        return ops[0:1]  # one op at a time

    def _compute_request(pool, priority, pipeline_id):
        # Baseline request scaled by pool size, then boosted after OOM, then capped.
        max_cpu = pool.max_cpu_pool
        max_ram = pool.max_ram_pool

        base_cpu = max_cpu * s.CPU_BASE_FRAC.get(priority, 0.30)
        base_ram = max_ram * s.RAM_BASE_FRAC.get(priority, 0.30)

        boost = s.ram_boost.get(pipeline_id, 1.0)
        req_ram = base_ram * boost

        cap_cpu = max_cpu * s.CPU_CAP_FRAC.get(priority, 0.60)
        cap_ram = max_ram * s.RAM_CAP_FRAC.get(priority, 0.70)

        req_cpu = min(max(base_cpu, s.MIN_CPU), cap_cpu)
        req_ram = min(max(req_ram, s.MIN_RAM), cap_ram)

        # Also never request more than currently available in pool.
        req_cpu = min(req_cpu, pool.avail_cpu_pool)
        req_ram = min(req_ram, pool.avail_ram_pool)
        return req_cpu, req_ram

    def _try_pop_next_pipeline():
        """
        Pop next pipeline according to:
        - QUERY then INTERACTIVE then BATCH
        - occasional aging-based promotion of BATCH
        Returns (pipeline, source_queue) or (None, None).
        """
        # Aging-based batch promotion (very mild)
        promote_batch = False
        if (s.tick % max(1, s.BATCH_PROMOTE_EVERY)) == 0 and len(s.q_batch) > 0:
            # If the oldest batch has waited long enough, promote one.
            oldest = s.q_batch[0]
            waited = s.tick - s.enqueue_tick.get(oldest.pipeline_id, s.tick)
            if waited >= s.BATCH_AGING_TICKS:
                promote_batch = True

        # If promoting, consider batch right after QUERY (still protecting latency).
        if len(s.q_query) > 0:
            return s.q_query.popleft(), s.q_query
        if promote_batch and len(s.q_batch) > 0:
            return s.q_batch.popleft(), s.q_batch
        if len(s.q_interactive) > 0:
            return s.q_interactive.popleft(), s.q_interactive
        if len(s.q_batch) > 0:
            return s.q_batch.popleft(), s.q_batch
        return None, None

    # --- ingest new pipelines ---
    for p in pipelines:
        _enqueue_pipeline(p)

    # --- process results (learn OOM + drop non-OOM failures) ---
    # Keep it conservative: only treat explicit OOM as retry-with-more-RAM.
    for r in results:
        if not r.failed():
            continue
        # Attempt to locate pipeline id: ExecutionResult doesn't expose it in the prompt;
        # we therefore fall back to updating by priority only if we cannot map precisely.
        # However, we *can* still avoid dropping everything by not mass-dropping.
        is_oom = _is_oom_error(getattr(r, "error", None))
        # If result includes ops, try to find owning pipeline via op metadata is not available here.
        # So: only apply adaptive RAM if we can infer pipeline from a single waiting instance is unsafe.
        # We keep a weak heuristic: if exactly one pipeline exists in that priority queue, boost it.
        if is_oom:
            boosted = False
            # Prefer boosting the most recently enqueued pipeline of that priority (often the one retrying).
            q = _prio_queue(r.priority)
            if len(q) == 1:
                pid = q[0].pipeline_id
                s.oom_count[pid] = s.oom_count.get(pid, 0) + 1
                # Exponential-ish backoff, bounded later by pool max via compute_request.
                s.ram_boost[pid] = min(s.ram_boost.get(pid, 1.0) * 2.0, 64.0)
                boosted = True
            if not boosted:
                # If we can't map safely, do nothing rather than corrupt state.
                pass
        else:
            # Non-OOM failure: do not infinitely retry. If we can identify a single candidate, drop it.
            q = _prio_queue(r.priority)
            if len(q) == 1:
                s.dead_pipelines.add(q[0].pipeline_id)

    # Fast path: no changes and nothing waiting
    if not pipelines and not results and not (s.q_query or s.q_interactive or s.q_batch):
        s.tick += 1
        return [], []

    suspensions = []
    assignments = []

    # --- scheduling loop across pools ---
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]

        # Keep scheduling until pool headroom is too small or queues empty.
        while True:
            if pool.avail_cpu_pool < s.MIN_CPU or pool.avail_ram_pool < s.MIN_RAM:
                break
            if not (s.q_query or s.q_interactive or s.q_batch):
                break

            p, src_q = _try_pop_next_pipeline()
            if p is None:
                break

            # Drop completed/dead pipelines
            if _pipeline_done_or_dead(p):
                continue

            # Fetch a single ready op (parents complete). If none, requeue to same queue tail.
            op_list = _has_ready_op(p)
            if not op_list:
                # Requeue to preserve FIFO-ish behavior within each priority.
                _prio_queue(p.priority).append(p)
                continue

            # Compute request for this pool.
            req_cpu, req_ram = _compute_request(pool, p.priority, p.pipeline_id)

            # If we still can't fit minimal request, requeue and stop trying in this pool.
            if req_cpu < s.MIN_CPU or req_ram < s.MIN_RAM:
                _prio_queue(p.priority).appendleft(p)
                break

            # Create assignment.
            assignments.append(
                Assignment(
                    ops=op_list,
                    cpu=req_cpu,
                    ram=req_ram,
                    priority=p.priority,
                    pool_id=pool_id,
                    pipeline_id=p.pipeline_id,
                )
            )

            # Requeue the pipeline so its next stage can be scheduled later (keeps concurrency).
            _prio_queue(p.priority).append(p)

            # Update pool availability pessimistically for this tick's planning loop.
            # (This mirrors the naive style that treats assignments as consuming headroom immediately.)
            try:
                pool.avail_cpu_pool -= req_cpu
                pool.avail_ram_pool -= req_ram
            except Exception:
                # If pool availability fields are read-only in some implementations, just stop.
                break

    s.tick += 1
    return suspensions, assignments
