# policy_key: scheduler_low_049
# reasoning_effort: low
# model: gpt-5.2-2025-12-11
# llm_cost: 0.050327
# generation_seconds: 41.44
# generated_at: 2026-04-09T21:41:33.845653
@register_scheduler_init(key="scheduler_low_049")
def scheduler_low_049_init(s):
    """
    Priority + aging + OOM-backoff scheduler.

    Goals aligned with score:
    - Strongly prioritize QUERY and INTERACTIVE to reduce weighted latency.
    - Avoid failures (720s penalty) by retrying FAILED operators with RAM backoff on suspected OOM.
    - Avoid starvation by adding simple aging to promote long-waiting pipelines.
    - Keep the design simple/stable: no aggressive preemption, one-op assignments.
    """
    s.waiting_pipelines = {}          # pipeline_id -> Pipeline
    s.enqueue_tick = {}               # pipeline_id -> tick when first seen (for aging)
    s.tick = 0                        # logical time
    s.op_attempts = {}                # (pipeline_id, op_key) -> attempts
    s.op_ram_hint = {}                # (pipeline_id, op_key) -> ram hint to request next time
    s.pipeline_giveup = set()         # pipeline_id marked as non-retriable failure
    s.max_attempts_per_op = 4         # cap retries to avoid infinite churn
    s.max_ram_backoff_steps = 4       # number of times we'll double RAM hint on OOM-like failures


def _p_weight(priority):
    # Higher = more important for ordering/admission decisions
    if priority == Priority.QUERY:
        return 3
    if priority == Priority.INTERACTIVE:
        return 2
    return 1


def _initial_cpu_share(priority, pool_max_cpu):
    # Conservative CPU sizing to allow some concurrency while still favoring high priority.
    # Clamp to at least 1 vCPU.
    if pool_max_cpu <= 0:
        return 0
    if priority == Priority.QUERY:
        return max(1.0, 0.75 * pool_max_cpu)
    if priority == Priority.INTERACTIVE:
        return max(1.0, 0.50 * pool_max_cpu)
    return max(1.0, 0.30 * pool_max_cpu)


def _initial_ram_share(priority, pool_max_ram):
    # Conservative RAM sizing; on OOM we back off (increase) per-op.
    if pool_max_ram <= 0:
        return 0
    if priority == Priority.QUERY:
        return max(0.0, 0.55 * pool_max_ram)
    if priority == Priority.INTERACTIVE:
        return max(0.0, 0.40 * pool_max_ram)
    return max(0.0, 0.30 * pool_max_ram)


def _looks_like_oom(err):
    if err is None:
        return False
    try:
        msg = str(err).lower()
    except Exception:
        return False
    return ("oom" in msg) or ("out of memory" in msg) or ("memory" in msg and "alloc" in msg)


def _op_key(op):
    # Stable-enough within a run; we avoid imports; id(op) is sufficient for simulator process lifetime.
    return id(op)


def _pipeline_is_done_or_failed(pipeline):
    status = pipeline.runtime_status()
    if status.is_pipeline_successful():
        return True
    # If it has failed ops but they are still retryable we keep it; "giveup" handled separately.
    return False


def _get_next_assignable_op(pipeline):
    status = pipeline.runtime_status()
    op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
    if not op_list:
        return None
    return op_list[0]


def _effective_rank(s, pipeline):
    # Higher rank is scheduled sooner.
    # Priority dominates; aging helps long-waiting pipelines make progress.
    base = _p_weight(pipeline.priority)
    enq = s.enqueue_tick.get(pipeline.pipeline_id, s.tick)
    age = max(0, s.tick - enq)

    # Aging rate: batch ages faster to prevent starvation, but doesn't overtake queries quickly.
    if pipeline.priority == Priority.BATCH_PIPELINE:
        age_bonus = age / 40.0
    elif pipeline.priority == Priority.INTERACTIVE:
        age_bonus = age / 80.0
    else:
        age_bonus = age / 120.0

    # Slight penalty if we've already had many retries in this pipeline (reduce churn ahead of fresh work).
    # We can't easily count all attempts per pipeline efficiently without more bookkeeping, so approximate:
    retry_penalty = 0.0
    # (Keep this tiny to avoid starving retried-but-important queries.)
    return base + age_bonus - retry_penalty


@register_scheduler(key="scheduler_low_049")
def scheduler_low_049_scheduler(s, results, pipelines):
    """
    Each tick:
    1) Ingest arrivals into a waiting set (no drops).
    2) Learn from execution results:
       - If failed with OOM-like error: increase RAM hint for that op and retry up to a cap.
       - If failed non-OOM repeatedly: eventually give up to avoid infinite retries.
    3) For each pool, schedule one operator at a time, selecting the highest-ranked pipeline that has an
       assignable op (parents complete). Use per-op RAM hints with conservative defaults.
    """
    s.tick += 1

    # Ingest new pipelines
    for p in pipelines:
        pid = p.pipeline_id
        s.waiting_pipelines[pid] = p
        if pid not in s.enqueue_tick:
            s.enqueue_tick[pid] = s.tick

    # Learn from results (resource hints + retry bookkeeping)
    # Note: We don't attempt preemption; suspensions remain empty.
    if results:
        for r in results:
            # r.ops is a list of operators executed in the container (typically length 1 in our usage)
            # If empty/unexpected, skip.
            ops = getattr(r, "ops", None) or []
            if not ops:
                continue

            # If pipeline already marked giveup, ignore.
            # We don't have pipeline_id on result directly; infer from op ownership is not available.
            # So we only keep per-op hints and attempt counters keyed by (unknown pipeline_id, op_key) is impossible.
            # Workaround: since Assignment includes pipeline_id, simulator likely embeds it in operator or result, but not guaranteed.
            # We'll use a best-effort: if result has pipeline_id attribute, use it; else skip attempt tracking.
            pid = getattr(r, "pipeline_id", None)

            if r.failed():
                # If we can attribute to a pipeline, update attempt counts / ram hints.
                if pid is not None:
                    for op in ops:
                        ok = _op_key(op)
                        key = (pid, ok)
                        prev_attempts = s.op_attempts.get(key, 0)
                        s.op_attempts[key] = prev_attempts + 1

                        # Update RAM hint on OOM-like failures (double, capped by pool max later)
                        if _looks_like_oom(getattr(r, "error", None)):
                            prev_hint = s.op_ram_hint.get(key, getattr(r, "ram", 0) or 0)
                            if prev_hint <= 0:
                                prev_hint = getattr(r, "ram", 0) or 0
                            # If still unknown, we'll fall back to pool share at scheduling time.
                            if prev_hint > 0:
                                backoff_steps = min(s.max_ram_backoff_steps, s.op_attempts[key])
                                # exponential-ish growth but bounded by using attempts as steps
                                new_hint = prev_hint * (2.0 if backoff_steps <= s.max_ram_backoff_steps else 1.0)
                                s.op_ram_hint[key] = max(prev_hint, new_hint)
                        else:
                            # Non-OOM failure: after enough attempts, mark pipeline as give-up if known.
                            if s.op_attempts[key] >= s.max_attempts_per_op:
                                s.pipeline_giveup.add(pid)
                else:
                    # Can't attribute; do nothing (scheduler remains conservative).
                    pass

    # Remove completed pipelines from waiting; keep retryable ones
    to_delete = []
    for pid, p in s.waiting_pipelines.items():
        if pid in s.pipeline_giveup:
            # Mark as done from scheduler perspective (avoid endless retries). This will score as failed/incomplete.
            to_delete.append(pid)
            continue
        if _pipeline_is_done_or_failed(p):
            to_delete.append(pid)
    for pid in to_delete:
        s.waiting_pipelines.pop(pid, None)

    # Early exit if nothing to do
    if not s.waiting_pipelines and not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    # Helper: produce a sorted list of pipeline_ids by effective rank
    def ranked_pipeline_ids():
        items = list(s.waiting_pipelines.values())
        items.sort(key=lambda p: _effective_rank(s, p), reverse=True)
        return [p.pipeline_id for p in items]

    # Schedule per pool; try to place high-priority work wherever resources exist
    ranked_ids = ranked_pipeline_ids()

    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)

        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # Keep packing one-op assignments while we still have resources.
        # Stop when no eligible pipeline can fit.
        made_progress = True
        while made_progress:
            made_progress = False

            if avail_cpu <= 0 or avail_ram <= 0:
                break

            # Iterate in priority/aging order to pick the first fit
            chosen_pid = None
            chosen_op = None
            chosen_cpu = None
            chosen_ram = None
            chosen_priority = None

            for pid in ranked_ids:
                p = s.waiting_pipelines.get(pid)
                if p is None:
                    continue
                if pid in s.pipeline_giveup:
                    continue

                op = _get_next_assignable_op(p)
                if op is None:
                    continue

                # Compute resource request
                prio = p.priority
                target_cpu = _initial_cpu_share(prio, float(pool.max_cpu_pool))
                target_ram = _initial_ram_share(prio, float(pool.max_ram_pool))

                # Apply per-op RAM hint (from previous OOM-like failures)
                ok = _op_key(op)
                hint_key = (pid, ok)
                if hint_key in s.op_ram_hint and s.op_ram_hint[hint_key] > 0:
                    target_ram = max(target_ram, float(s.op_ram_hint[hint_key]))

                # Bound by currently available resources
                req_cpu = min(avail_cpu, target_cpu)
                req_ram = min(avail_ram, target_ram)

                # Must request at least some CPU/RAM to run
                if req_cpu <= 0 or req_ram <= 0:
                    continue

                # Extra safety: for very small remaining fragments, avoid launching batch ops that might OOM
                # and consume retries; require a minimal RAM floor based on priority.
                if prio == Priority.BATCH_PIPELINE and req_ram < 0.10 * float(pool.max_ram_pool):
                    continue

                chosen_pid = pid
                chosen_op = op
                chosen_cpu = req_cpu
                chosen_ram = req_ram
                chosen_priority = prio
                break

            if chosen_pid is None:
                break

            # Create assignment (one op at a time to minimize blast radius and simplify retries)
            assignments.append(
                Assignment(
                    ops=[chosen_op],
                    cpu=chosen_cpu,
                    ram=chosen_ram,
                    priority=chosen_priority,
                    pool_id=pool_id,
                    pipeline_id=chosen_pid,
                )
            )

            # Update local available resources; note that actual executor accounting may differ,
            # but this avoids over-issuing in one scheduler tick.
            avail_cpu -= chosen_cpu
            avail_ram -= chosen_ram
            made_progress = True

    return suspensions, assignments
