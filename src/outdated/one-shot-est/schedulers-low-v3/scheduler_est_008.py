# policy_key: scheduler_est_008
# reasoning_effort: low
# model: gpt-5.2-2025-12-11
# llm_cost: 0.041901
# generation_seconds: 35.78
# generated_at: 2026-04-03T01:09:38.074636
@register_scheduler_init(key="scheduler_est_008")
def scheduler_est_008_init(s):
    """Priority-aware, small-step improvement over naive FIFO.

    Main ideas:
    - Maintain separate waiting queues per priority (QUERY > INTERACTIVE > BATCH).
    - Avoid head-of-line blocking by scanning for runnable ops and scheduling multiple ops per tick/pool.
    - Right-size CPU to enable concurrency (instead of giving the entire pool to one op).
    - Use optional op.estimate.mem_peak_gb; on OOM, retry with multiplicative RAM bump.
    - Add a tiny fairness knob: after several high-priority placements, allow a batch placement if available.
    """
    from collections import deque

    # Queues of pipelines by priority (pipelines re-enqueued until completion)
    s.q_query = deque()
    s.q_interactive = deque()
    s.q_batch = deque()

    # Per-operator RAM retry multiplier after OOMs: op_key -> multiplier (>= 1.0)
    s.op_ram_mult = {}

    # Simple fairness: after N high-priority assignments, permit one batch assignment
    s.hp_streak = 0


@register_scheduler(key="scheduler_est_008")
def scheduler_est_008(s, results: List[ExecutionResult], pipelines: List[Pipeline]) -> Tuple[List[Suspend], List[Assignment]]:
    from collections import deque

    def _prio_rank(prio):
        # Higher is more important
        if prio == Priority.QUERY:
            return 3
        if prio == Priority.INTERACTIVE:
            return 2
        return 1  # Priority.BATCH_PIPELINE and anything else

    def _get_queue(prio):
        if prio == Priority.QUERY:
            return s.q_query
        if prio == Priority.INTERACTIVE:
            return s.q_interactive
        return s.q_batch

    def _op_key(pipeline_id, op):
        # Best-effort stable key for retry tracking across ticks.
        # Prefer explicit IDs if present; otherwise fall back to repr(op).
        if hasattr(op, "op_id"):
            return (pipeline_id, "op_id", getattr(op, "op_id"))
        if hasattr(op, "operator_id"):
            return (pipeline_id, "operator_id", getattr(op, "operator_id"))
        if hasattr(op, "name"):
            return (pipeline_id, "name", getattr(op, "name"))
        return (pipeline_id, "repr", repr(op))

    def _looks_like_oom(err):
        if err is None:
            return False
        txt = str(err).lower()
        return ("oom" in txt) or ("out of memory" in txt) or ("cuda out of memory" in txt) or ("killed" in txt and "memory" in txt)

    def _cpu_target(pool, prio):
        # Small-step improvement: avoid consuming the entire pool for one op.
        # Still give more CPU to higher priority to improve latency.
        max_cpu = float(pool.max_cpu_pool)
        if prio == Priority.QUERY:
            return max(1.0, min(4.0, 0.5 * max_cpu))
        if prio == Priority.INTERACTIVE:
            return max(1.0, min(6.0, 0.6 * max_cpu))
        # Batch: smaller per-op share to allow opportunistic progress
        return max(1.0, min(2.0, 0.3 * max_cpu))

    def _ram_target_gb(pool, prio, pipeline_id, op):
        # Use estimator if present; otherwise pick a modest default.
        # On OOM, bump multiplicatively for that operator.
        est = None
        if hasattr(op, "estimate") and op.estimate is not None and hasattr(op.estimate, "mem_peak_gb"):
            est = op.estimate.mem_peak_gb

        mult = s.op_ram_mult.get(_op_key(pipeline_id, op), 1.0)

        # Base RAM selection
        if isinstance(est, (int, float)) and est is not None:
            base = float(est)
            # Use estimate fairly aggressively; small cushion to avoid trivial underestimates.
            base = max(base * 1.10, 0.25)
        else:
            # No estimate: choose a conservative small slice, giving a bit more to higher priority.
            max_ram = float(pool.max_ram_pool)
            if prio == Priority.QUERY:
                base = max(0.5, min(4.0, 0.25 * max_ram))
            elif prio == Priority.INTERACTIVE:
                base = max(0.5, min(6.0, 0.30 * max_ram))
            else:
                base = max(0.5, min(3.0, 0.20 * max_ram))

        want = base * float(mult)

        # Clamp within pool capacity (if this is still too low, OOM will bump and retry)
        want = min(want, float(pool.max_ram_pool))
        return max(0.1, want)

    def _enqueue_pipeline(p):
        _get_queue(p.priority).append(p)

    def _pipeline_done_or_failed(p):
        st = p.runtime_status()
        if st.is_pipeline_successful():
            return True
        # If there are FAILED ops we still allow retrying them (ASSIGNABLE_STATES includes FAILED),
        # so we do NOT drop on failures here.
        return False

    def _get_one_runnable_op(p):
        st = p.runtime_status()
        # Require parents complete to respect DAG dependencies.
        ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        if not ops:
            return None
        # Take one op at a time to enable fairness across pipelines.
        return ops[0]

    # Update RAM retry multipliers based on OOM-like failures.
    for r in results:
        if r is None:
            continue
        if hasattr(r, "failed") and r.failed():
            if _looks_like_oom(getattr(r, "error", None)):
                # Ramp RAM for each failed op
                for op in getattr(r, "ops", []) or []:
                    key = _op_key(getattr(op, "pipeline_id", None), op)
                    # Use (pipeline_id, ...) if present on op; if not, we still keep a best-effort key
                    s.op_ram_mult[key] = min(16.0, max(1.5, s.op_ram_mult.get(key, 1.0) * 2.0))

    # Enqueue newly arrived pipelines into per-priority queues.
    for p in pipelines:
        _enqueue_pipeline(p)

    if not pipelines and not results:
        return [], []

    suspensions: List[Suspend] = []
    assignments: List[Assignment] = []

    # Helper to rotate queues while trying to place work.
    def _pop_next_pipeline(prefer_batch_ok: bool):
        # Decide which queue to use next, with a small fairness knob.
        # If we've placed several high-priority ops in a row, allow batch.
        if prefer_batch_ok and s.q_batch:
            return s.q_batch.popleft()

        if s.q_query:
            return s.q_query.popleft()
        if s.q_interactive:
            return s.q_interactive.popleft()
        if s.q_batch:
            return s.q_batch.popleft()
        return None

    # Requeue list to preserve pipelines we inspected but didn't finish.
    requeue: List[Pipeline] = []

    # Schedule per pool, attempting multiple placements per pool to improve responsiveness.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]

        # Work-conserving: keep scheduling while resources remain.
        # Put a hard cap to avoid long loops in pathological cases.
        max_placements = 64
        placements = 0

        while placements < max_placements:
            avail_cpu = float(pool.avail_cpu_pool)
            avail_ram = float(pool.avail_ram_pool)
            if avail_cpu <= 0.0 or avail_ram <= 0.0:
                break

            # Fairness: after 6 high-priority placements, allow a batch placement if any.
            allow_batch_now = (s.hp_streak >= 6)

            p = _pop_next_pipeline(prefer_batch_ok=allow_batch_now)
            if p is None:
                break

            if _pipeline_done_or_failed(p):
                # Drop completed pipelines.
                continue

            op = _get_one_runnable_op(p)
            if op is None:
                # Not runnable yet (waiting on dependencies); keep it around.
                requeue.append(p)
                placements += 1
                continue

            # Compute targets and clamp to availability.
            cpu = min(avail_cpu, _cpu_target(pool, p.priority))
            ram = min(avail_ram, _ram_target_gb(pool, p.priority, p.pipeline_id, op))

            # If we can't fit even the minimal meaningful resources, requeue and stop for this pool.
            if cpu <= 0.0 or ram <= 0.0:
                requeue.append(p)
                break

            # Place exactly one op; this avoids single pipeline hogging.
            assignments.append(
                Assignment(
                    ops=[op],
                    cpu=cpu,
                    ram=ram,
                    priority=p.priority,
                    pool_id=pool_id,
                    pipeline_id=p.pipeline_id,
                )
            )

            # Update fairness streak.
            if p.priority in (Priority.QUERY, Priority.INTERACTIVE):
                s.hp_streak += 1
            else:
                s.hp_streak = 0

            # Requeue pipeline for future ops.
            requeue.append(p)

            placements += 1

    # Put pipelines back into their respective queues.
    for p in requeue:
        _enqueue_pipeline(p)

    return suspensions, assignments
