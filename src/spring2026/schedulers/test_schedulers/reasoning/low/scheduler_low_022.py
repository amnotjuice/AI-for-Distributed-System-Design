# policy_key: scheduler_low_022
# reasoning_effort: low
# model: gpt-5.2-2025-12-11
# llm_cost: 0.054793
# generation_seconds: 37.97
# generated_at: 2026-04-09T21:19:55.309953
@register_scheduler_init(key="scheduler_low_022")
def scheduler_low_022_init(s):
    """Priority + anti-failure scheduler.

    Goals (tuned for objective):
      - Minimize weighted end-to-end latency (query > interactive > batch).
      - Strongly avoid failures (each failed/incomplete pipeline is extremely costly).
      - Simple, robust behavior: prioritize high-priority work, but still ensure batch progress.

    Strategy:
      - Maintain per-priority waiting queues with mild aging to avoid starvation.
      - Schedule at most one ready operator per pipeline per tick (reduces head-of-line blocking).
      - Conservative sizing to reduce OOM risk:
          * Start with a reasonable RAM share by priority.
          * On failure with OOM-like signal: boost RAM multiplicatively and retry (up to a cap).
          * Also modestly boost CPU after failures to reduce time spent near timeouts.
      - No preemption (API for enumerating running containers is not guaranteed here);
        instead, protect query/interactive by always selecting them first when capacity appears.
    """
    s.queues = {
        "query": [],
        "interactive": [],
        "batch": [],
    }
    s.pipeline_meta = {}  # pipeline_id -> dict(attempts, ram_factor, cpu_factor, enq_tick)
    s.tick = 0

    # Retry budget: enough to recover from OOM sizing errors but bounded to avoid infinite churn.
    s.max_attempts = 5

    # RAM growth on OOM; CPU growth on repeated failures/slowdowns (best-effort).
    s.oom_ram_growth = 1.6
    s.fail_cpu_growth = 1.25

    # Soft minimum RAM fractions by priority (of pool max RAM); then clamped by available RAM.
    s.base_ram_frac = {
        "query": 0.70,
        "interactive": 0.55,
        "batch": 0.40,
    }

    # Soft CPU fractions by priority (of pool max CPU); then clamped by available CPU.
    s.base_cpu_frac = {
        "query": 0.75,
        "interactive": 0.60,
        "batch": 0.50,
    }

    # Aging: after enough ticks waiting, allow lower priority to bubble up occasionally.
    s.aging_ticks = 25


@register_scheduler(key="scheduler_low_022")
def scheduler_low_022_scheduler(s, results, pipelines):
    """
    Scheduler step.

    Returns:
      (suspensions, assignments)

    Notes:
      - We only rely on the access patterns shown in the template.
      - We retry FAILED operators (ASSIGNABLE_STATES includes FAILED) when the failure
        is likely due to undersizing (OOM). Other errors are retried conservatively
        within the max_attempts budget because completing is heavily rewarded.
    """
    s.tick += 1

    def _prio_bucket(p):
        if p.priority == Priority.QUERY:
            return "query"
        if p.priority == Priority.INTERACTIVE:
            return "interactive"
        return "batch"

    def _ensure_meta(pipeline_id):
        m = s.pipeline_meta.get(pipeline_id)
        if m is None:
            m = {
                "attempts": 0,
                "ram_factor": 1.0,
                "cpu_factor": 1.0,
                "enq_tick": s.tick,
            }
            s.pipeline_meta[pipeline_id] = m
        return m

    def _looks_like_oom(err):
        if err is None:
            return False
        try:
            msg = str(err).lower()
        except Exception:
            return False
        return ("oom" in msg) or ("out of memory" in msg) or ("cuda out of memory" in msg) or ("memoryerror" in msg)

    # Incorporate new arrivals into queues.
    for p in pipelines:
        meta = _ensure_meta(p.pipeline_id)
        meta["enq_tick"] = s.tick
        s.queues[_prio_bucket(p)].append(p)

    # Update meta based on execution results.
    # - On failure, we boost RAM if OOM-like, otherwise modest CPU boost.
    # - We do not assume we can access detailed op stats; we just tune per-pipeline.
    for r in results:
        # Some runs may return results without a pipeline handle; we key by r.ops[0].pipeline_id
        # is not guaranteed. We instead adjust by matching container results only if possible.
        # Since API doesn't expose pipeline_id on ExecutionResult, we only apply generic signals
        # when we can infer from r.ops metadata, otherwise skip safely.
        pipeline_id = None
        try:
            if r.ops and hasattr(r.ops[0], "pipeline_id"):
                pipeline_id = r.ops[0].pipeline_id
        except Exception:
            pipeline_id = None

        if pipeline_id is None:
            continue

        meta = _ensure_meta(pipeline_id)

        if hasattr(r, "failed") and r.failed():
            meta["attempts"] += 1
            if _looks_like_oom(getattr(r, "error", None)):
                meta["ram_factor"] = min(meta["ram_factor"] * s.oom_ram_growth, 8.0)
            else:
                meta["cpu_factor"] = min(meta["cpu_factor"] * s.fail_cpu_growth, 3.0)

    # Early exit if nothing changed (keeps simulator fast), matching the starter template spirit.
    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    # Helper: pop next schedulable pipeline with aging.
    def _pick_next_pipeline():
        # Base priority order, but allow aging to occasionally lift batch/interactive.
        # Effective order:
        #   - Always prefer query if present.
        #   - Otherwise, if interactive is present and has waited long enough, prefer it.
        #   - Otherwise, allow oldest among interactive/batch to proceed.
        # This prevents indefinite starvation with minimal complexity.
        q = s.queues["query"]
        it = s.queues["interactive"]
        bt = s.queues["batch"]

        if q:
            return q.pop(0)

        # If any pipeline has waited beyond aging threshold, choose the oldest among it/bt.
        oldest = None
        oldest_bucket = None
        for bucket_name, bucket in (("interactive", it), ("batch", bt)):
            if not bucket:
                continue
            p0 = bucket[0]
            meta0 = s.pipeline_meta.get(p0.pipeline_id, {"enq_tick": s.tick})
            waited = s.tick - meta0.get("enq_tick", s.tick)
            if waited >= s.aging_ticks:
                if oldest is None or meta0.get("enq_tick", s.tick) < s.pipeline_meta.get(oldest.pipeline_id, {}).get("enq_tick", s.tick):
                    oldest = p0
                    oldest_bucket = bucket_name

        if oldest is not None:
            # Remove that exact pipeline (usually at head, but be safe).
            bucket = s.queues[oldest_bucket]
            for i, p in enumerate(bucket):
                if p.pipeline_id == oldest.pipeline_id:
                    return bucket.pop(i)

        # Default strict priority among remaining.
        if it:
            return it.pop(0)
        if bt:
            return bt.pop(0)
        return None

    # Helper: requeue pipeline if still active and retry budget not exceeded.
    def _requeue_if_needed(p):
        status = p.runtime_status()
        if status.is_pipeline_successful():
            return
        meta = _ensure_meta(p.pipeline_id)
        if meta["attempts"] >= s.max_attempts:
            # Stop scheduling this pipeline to avoid endless churn. It will count as failed/incomplete.
            return
        meta["enq_tick"] = s.tick
        s.queues[_prio_bucket(p)].append(p)

    # Attempt to schedule work across pools.
    # We create at most one assignment per pool per tick (simple & avoids overload),
    # but we may schedule multiple pools.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool
        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # Pull pipelines until we find one with a ready operator and resources.
        tried = 0
        max_try = 32  # prevent long loops if queues contain many blocked pipelines
        requeue = []

        while tried < max_try:
            p = _pick_next_pipeline()
            if p is None:
                break
            tried += 1

            status = p.runtime_status()

            # If successful, drop it.
            if status.is_pipeline_successful():
                continue

            meta = _ensure_meta(p.pipeline_id)

            # If out of retry budget, do not keep it in queue.
            if meta["attempts"] >= s.max_attempts:
                continue

            # Find one ready op (parents complete) to minimize concurrency/OOM risk.
            op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
            if not op_list:
                # Not ready; keep it around.
                requeue.append(p)
                continue

            bucket = _prio_bucket(p)

            # Choose sizing: conservative RAM to reduce OOM risk (failure is very costly).
            # Start from base fraction of pool max RAM, apply per-pipeline RAM factor,
            # clamp by available RAM.
            target_ram = pool.max_ram_pool * s.base_ram_frac[bucket] * meta["ram_factor"]
            # Ensure we never allocate more than available; also keep a small floor to avoid zero.
            ram = max(1.0, min(avail_ram, target_ram))

            # CPU: give a reasonable share; extra CPU can help reduce timeout/latency.
            target_cpu = pool.max_cpu_pool * s.base_cpu_frac[bucket] * meta["cpu_factor"]
            cpu = max(1.0, min(avail_cpu, target_cpu))

            # If we can't allocate meaningful resources, requeue and stop on this pool.
            if cpu <= 0 or ram <= 0:
                requeue.append(p)
                break

            assignments.append(
                Assignment(
                    ops=op_list,
                    cpu=cpu,
                    ram=ram,
                    priority=p.priority,
                    pool_id=pool_id,
                    pipeline_id=p.pipeline_id,
                )
            )

            # After assigning an op, requeue pipeline for its next steps (if any).
            requeue.append(p)
            break

        # Put back any pipelines we pulled but didn't permanently drop.
        for p in requeue:
            _requeue_if_needed(p)

    return suspensions, assignments
