# policy_key: scheduler_est_010
# reasoning_effort: low
# model: gpt-5.2-2025-12-11
# llm_cost: 0.035139
# generation_seconds: 39.94
# generated_at: 2026-04-03T01:11:03.996842
@register_scheduler_init(key="scheduler_est_010")
def scheduler_est_010_init(s):
    """Priority-aware FIFO with simple RAM estimation + OOM-aware retries.

    Improvements over naive FIFO:
      - Maintain separate queues per priority and schedule higher priority first.
      - Use op.estimate.mem_peak_gb (if present) to size RAM aggressively.
      - On OOM failures, retry the same op with increased RAM (exponential backoff).
      - Avoid dropping pipelines just because they have failures (OOM is recoverable).

    Intentionally kept simple:
      - No preemption (requires access to running containers beyond the provided API surface).
      - Conservative CPU caps per op to reduce head-of-line blocking and improve tail latency.
    """
    s.q_query = []
    s.q_interactive = []
    s.q_batch = []

    # Per-operator retry state keyed by (pipeline_id, op_key)
    s.op_retry_count = {}       # (pid, op_key) -> int
    s.op_ram_boost_gb = {}      # (pid, op_key) -> float additional RAM to add next time

    # Anti-starvation: after N high-priority assignments, try to schedule a batch op if possible.
    s.hp_streak = 0
    s.hp_streak_limit = 8

    # Max retries for OOM before we give up (still keep pipeline in queue but skip that op)
    s.max_oom_retries = 5


def _priority_rank(priority):
    # Lower is higher priority
    if priority == Priority.QUERY:
        return 0
    if priority == Priority.INTERACTIVE:
        return 1
    return 2  # Priority.BATCH_PIPELINE and any others


def _queue_for_priority(s, priority):
    if priority == Priority.QUERY:
        return s.q_query
    if priority == Priority.INTERACTIVE:
        return s.q_interactive
    return s.q_batch


def _op_key(op):
    # Best-effort stable key for retry tracking
    return getattr(op, "op_id", None) or getattr(op, "operator_id", None) or id(op)


def _is_oom_error(err):
    if err is None:
        return False
    msg = str(err).lower()
    return ("oom" in msg) or ("out of memory" in msg) or ("out-of-memory" in msg) or ("memoryerror" in msg)


def _desired_ram_gb(s, pool, pipeline_id, op):
    # If estimator attaches op.estimate.mem_peak_gb, use it aggressively with a small headroom.
    est = getattr(getattr(op, "estimate", None), "mem_peak_gb", None)
    if est is None:
        # Unknown: start small-ish but non-trivial to reduce immediate OOM loops.
        base = max(1.0, 0.10 * pool.max_ram_pool)
    else:
        # Aggressive sizing: 5% headroom.
        base = max(0.5, float(est) * 1.05)

    key = (pipeline_id, _op_key(op))
    boost = float(s.op_ram_boost_gb.get(key, 0.0))
    ram = base + boost

    # Clamp to pool capacity (can't exceed a pool's max RAM).
    ram = max(0.25, min(ram, float(pool.max_ram_pool)))
    return ram


def _desired_cpu(s, pool, priority, avail_cpu):
    # Bias CPU to higher priority without letting one op monopolize the entire pool.
    # Keep it simple and responsive: cap at a fraction of pool max, and also by currently available CPU.
    if priority == Priority.QUERY:
        cap_frac = 0.75
    elif priority == Priority.INTERACTIVE:
        cap_frac = 0.60
    else:
        cap_frac = 0.50

    cpu_cap = max(1.0, float(pool.max_cpu_pool) * cap_frac)
    cpu = min(float(avail_cpu), cpu_cap)

    # Ensure at least some CPU to make progress.
    if cpu < 0.1:
        return 0.0
    return cpu


def _pipeline_is_done_or_hard_failed(pipeline):
    status = pipeline.runtime_status()
    if status.is_pipeline_successful():
        return True

    # If pipeline has FAILED ops, we *do not* drop it here because OOM is recoverable.
    # Any irrecoverable failure handling is best-effort based on error signals; without them
    # on pipeline status, we keep it queued and let retry limits gate infinite loops.
    return False


def _pick_next_pipeline(s):
    # Strict priority with a small anti-starvation hook.
    if s.q_batch and s.hp_streak >= s.hp_streak_limit and (s.q_query or s.q_interactive):
        s.hp_streak = 0
        return s.q_batch.pop(0)

    if s.q_query:
        s.hp_streak += 1
        return s.q_query.pop(0)
    if s.q_interactive:
        s.hp_streak += 1
        return s.q_interactive.pop(0)
    if s.q_batch:
        s.hp_streak = 0
        return s.q_batch.pop(0)

    return None


@register_scheduler(key="scheduler_est_010")
def scheduler_est_010(s, results, pipelines):
    """
    Priority-aware scheduler:
      - Enqueue new pipelines by priority.
      - Process results to learn from OOM failures and increase RAM next retry.
      - For each pool, greedily assign ready ops from highest priority pipelines while resources last.
    """
    # Enqueue arrivals
    for p in pipelines:
        _queue_for_priority(s, p.priority).append(p)

    # Learn from failures (especially OOM)
    for r in results:
        if not getattr(r, "failed", lambda: False)():
            continue

        # If we can detect OOM, boost RAM for next attempt of each failed op.
        if _is_oom_error(getattr(r, "error", None)):
            for op in getattr(r, "ops", []) or []:
                key = (getattr(r, "pipeline_id", None), _op_key(op))
                # Fallback if pipeline_id is not present in result (use op object identity only)
                if key[0] is None:
                    key = ("unknown", _op_key(op))

                cnt = int(s.op_retry_count.get(key, 0)) + 1
                s.op_retry_count[key] = cnt

                # Exponential-ish backoff in GB. Start by adding +25% of last allocation (if known),
                # otherwise add a small absolute bump.
                last_ram = getattr(r, "ram", None)
                if last_ram is None:
                    bump = 1.0
                else:
                    bump = max(0.5, float(last_ram) * 0.25)

                # Increase bump with retries to converge faster on true peak.
                bump *= (1.0 + 0.35 * max(0, cnt - 1))

                s.op_ram_boost_gb[key] = float(s.op_ram_boost_gb.get(key, 0.0)) + bump

    # Early exit if nothing changed
    if not pipelines and not results and not (s.q_query or s.q_interactive or s.q_batch):
        return [], []

    suspensions = []
    assignments = []

    # We'll cycle pipelines through queues to preserve FIFO within each priority band.
    # Pipelines are requeued unless they're completed.
    def requeue_pipeline(p):
        if not _pipeline_is_done_or_hard_failed(p):
            _queue_for_priority(s, p.priority).append(p)

    # Try to schedule work on each pool.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)

        if avail_cpu <= 0.0 or avail_ram <= 0.0:
            continue

        # Greedily assign multiple ops while resources remain.
        # Keep a bounded loop to avoid pathological long ticks.
        iters = 0
        max_iters = 64

        # Temporary buffer to avoid losing pipelines we pop but can't schedule right now.
        popped = []

        while iters < max_iters:
            iters += 1

            pipeline = _pick_next_pipeline(s)
            if pipeline is None:
                break

            status = pipeline.runtime_status()
            if status.is_pipeline_successful():
                # Drop completed pipelines
                continue

            # Find an op that is ready to run (parents complete).
            op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
            if not op_list:
                # Nothing ready; keep pipeline around for later.
                popped.append(pipeline)
                continue

            op = op_list[0]

            # Retry gating: if we have exceeded OOM retries for this op, skip it (to avoid infinite loops).
            key = (pipeline.pipeline_id, _op_key(op))
            cnt = int(s.op_retry_count.get(key, 0))
            if cnt >= int(s.max_oom_retries):
                # Skip for now; keep pipeline queued so other ops (if any) could proceed.
                popped.append(pipeline)
                continue

            # Size resources
            want_ram = _desired_ram_gb(s, pool, pipeline.pipeline_id, op)
            want_cpu = _desired_cpu(s, pool, pipeline.priority, avail_cpu)

            # If we can't fit RAM, defer and try other pipelines (maybe smaller).
            if want_ram > avail_ram or want_cpu <= 0.0:
                popped.append(pipeline)
                # If RAM is tight, stop early to avoid churn.
                if want_ram > avail_ram and avail_ram < 0.5:
                    break
                continue

            # Make assignment
            assignments.append(
                Assignment(
                    ops=[op],
                    cpu=want_cpu,
                    ram=want_ram,
                    priority=pipeline.priority,
                    pool_id=pool_id,
                    pipeline_id=pipeline.pipeline_id,
                )
            )

            # Update available resources (local accounting)
            avail_cpu -= want_cpu
            avail_ram -= want_ram

            # Requeue pipeline for subsequent ops
            requeue_pipeline(pipeline)

            if avail_cpu <= 0.0 or avail_ram <= 0.0:
                break

        # Requeue pipelines we popped but didn't schedule
        for p in popped:
            requeue_pipeline(p)

    return suspensions, assignments
