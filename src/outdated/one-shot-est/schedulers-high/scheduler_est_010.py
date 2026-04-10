# policy_key: scheduler_est_010
# reasoning_effort: high
# model: gpt-5.2-2025-12-11
# llm_cost: 0.120966
# generation_seconds: 164.08
# generated_at: 2026-03-31T18:40:26.498452
@register_scheduler_init(key="scheduler_est_010")
def scheduler_est_010_init(s):
    """
    Priority-aware, estimate-guided scheduler (small, safe improvements over naive FIFO).

    Improvements vs. naive baseline:
      1) Removes "early exit" idling: keeps scheduling from an existing waiting queue.
      2) Uses strict priority order (QUERY > INTERACTIVE > BATCH) to reduce tail latency.
      3) Right-sizes RAM using op.estimate.mem_peak_gb (+ safety margin) instead of "all available".
      4) Caps CPU per container by priority to avoid one container monopolizing a pool.
      5) Retries OOM failures with increased RAM (bounded, with a max retry cap).
      6) Adds simple starvation protection: aged BATCH pipelines can be promoted to INTERACTIVE.

    Design principle: keep it simple, deterministic, and robust to missing fields.
    """
    # Bookkeeping for active pipelines and their queue membership.
    s.tick = 0
    s.pipelines_by_id = {}          # pipeline_id -> Pipeline
    s.enqueued = set()              # pipeline_ids currently enqueued (in exactly one queue)
    s.enqueue_tick = {}             # pipeline_id -> tick enqueued
    s.dead_pipelines = set()        # pipeline_ids we won't schedule anymore (failed non-OOM or too many OOMs)

    # Per-priority queues (simple lists; we rotate by pop(0)/append()).
    s.queues = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }

    # OOM adaptation: track per-operator overrides keyed by id(op).
    s.op_to_pipeline_id = {}        # id(op) -> pipeline_id (filled when we first see/schedule the op)
    s.op_attempts = {}              # id(op) -> number of attempts (only counts failures we handle)
    s.op_ram_override_gb = {}       # id(op) -> min RAM (GB) to request next time, based on OOM history

    # Config knobs (conservative defaults).
    s.max_retries_oom = 3
    s.mem_safety = 1.30             # multiplier on estimated mem_peak_gb
    s.mem_pad_gb = 0.25             # additive pad in GB
    s.default_ram_gb = 1.0          # if estimate missing
    s.min_ram_gb = 0.5              # don't request tiny RAM containers

    # CPU caps by priority: keep latency-sensitive work faster, but avoid pool monopolization.
    s.cpu_frac_of_pool = {
        Priority.QUERY: 0.60,
        Priority.INTERACTIVE: 0.40,
        Priority.BATCH_PIPELINE: 0.25,
    }
    s.cpu_min = {
        Priority.QUERY: 1.0,
        Priority.INTERACTIVE: 0.5,
        Priority.BATCH_PIPELINE: 0.25,
    }

    # Limit per-pipeline parallelism to reduce the chance we "overschedule" a single pipeline in one tick.
    s.inflight_limit = {
        Priority.QUERY: 1,
        Priority.INTERACTIVE: 1,
        Priority.BATCH_PIPELINE: 2,
    }

    # Starvation protection: promote long-waiting batch pipelines.
    s.batch_age_boost_ticks = 50

    # Scheduling loop guardrails.
    s.max_assignments_per_pool_per_tick = 8

    # Cached cluster limits.
    s._max_pool_ram_gb = None


def _safe_state_count(status, state):
    try:
        return status.state_counts[state]
    except Exception:
        try:
            return status.state_counts.get(state, 0)
        except Exception:
            return 0


def _inflight_ops(status):
    # Count assigned/running/suspending as "in flight" to avoid overscheduling one pipeline.
    return (
        _safe_state_count(status, OperatorState.ASSIGNED)
        + _safe_state_count(status, OperatorState.RUNNING)
        + _safe_state_count(status, OperatorState.SUSPENDING)
    )


def _is_oom_error(err) -> bool:
    if err is None:
        return False
    msg = str(err).lower()
    return ("oom" in msg) or ("out of memory" in msg) or ("memory" in msg and "killed" in msg)


def _get_op_mem_est_gb(op):
    # Best-effort extraction of op.estimate.mem_peak_gb
    try:
        est = getattr(op, "estimate", None)
        if est is not None and hasattr(est, "mem_peak_gb"):
            v = est.mem_peak_gb
            if v is None:
                return None
            # Some generators may produce ints/floats; accept both.
            return float(v)
    except Exception:
        return None
    return None


def _drop_pipeline(s, pipeline_id):
    # Remove from our tracking. We lazily remove from queues by skipping when popped.
    s.pipelines_by_id.pop(pipeline_id, None)
    s.enqueued.discard(pipeline_id)
    s.enqueue_tick.pop(pipeline_id, None)
    s.dead_pipelines.discard(pipeline_id)


def _maybe_promote_aged_batch(s):
    # Promote sufficiently old BATCH pipelines into the INTERACTIVE queue.
    batch_q = s.queues[Priority.BATCH_PIPELINE]
    if not batch_q:
        return

    keep = []
    promote = []
    for p in batch_q:
        pid = p.pipeline_id
        if pid in s.dead_pipelines or pid not in s.pipelines_by_id:
            # Dropped pipeline; don't keep it.
            continue
        enq_t = s.enqueue_tick.get(pid, s.tick)
        if (s.tick - enq_t) >= s.batch_age_boost_ticks:
            promote.append(p)
        else:
            keep.append(p)

    if promote:
        s.queues[Priority.BATCH_PIPELINE] = keep
        # Keep original FIFO ordering among promoted batch jobs (append at end).
        s.queues[Priority.INTERACTIVE].extend(promote)


def _compute_ram_request_gb(s, pool, op):
    est = _get_op_mem_est_gb(op)
    base = (est * s.mem_safety + s.mem_pad_gb) if est is not None else s.default_ram_gb
    if base < s.min_ram_gb:
        base = s.min_ram_gb

    override = s.op_ram_override_gb.get(id(op), 0.0)
    req = base if base >= override else override

    # Never request more than the pool's maximum RAM.
    try:
        if req > pool.max_ram_pool:
            req = pool.max_ram_pool
    except Exception:
        pass

    return float(req)


def _compute_cpu_request(s, pool, priority, avail_cpu, have_other_waiting_work: bool):
    # Cap CPU to avoid monopolization; if no other waiting work, we can be more generous.
    try:
        pool_cap = float(pool.max_cpu_pool) * float(s.cpu_frac_of_pool.get(priority, 0.25))
    except Exception:
        pool_cap = float(avail_cpu)

    min_cpu = float(s.cpu_min.get(priority, 0.25))
    cap = pool_cap if pool_cap >= min_cpu else min_cpu

    # If nothing else is waiting, it's reasonable to give the op everything available.
    if not have_other_waiting_work:
        cap = float(avail_cpu)

    cpu = float(avail_cpu) if float(avail_cpu) <= cap else cap

    # Avoid zeros/negatives; allow fractional CPUs.
    if cpu <= 0.0:
        return 0.0
    return cpu


def _queue_nonempty_excluding_dead(s, prio):
    # Best-effort check used for "give more CPU if nothing else waiting".
    q = s.queues.get(prio, [])
    for p in q:
        pid = p.pipeline_id
        if pid in s.dead_pipelines or pid not in s.pipelines_by_id:
            continue
        # Consider it waiting if it still has any assignable op (parents complete).
        try:
            st = p.runtime_status()
            if st.is_pipeline_successful():
                continue
            ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
            if ops:
                return True
        except Exception:
            # If status is flaky, conservatively treat as waiting.
            return True
    return False


def _schedule_one_from_priority_queue(s, pool_id, pool, prio, avail_cpu, avail_ram):
    """
    Try to schedule exactly one operator from the given priority queue, rotating the queue.
    Returns: (Assignment or None, new_avail_cpu, new_avail_ram)
    """
    q = s.queues[prio]
    if not q:
        return None, avail_cpu, avail_ram

    # Decide whether we should cap CPU (i.e., there is other waiting work).
    # We only check other queues at a high level (cheap signal).
    have_other_waiting = (
        _queue_nonempty_excluding_dead(s, Priority.QUERY)
        or _queue_nonempty_excluding_dead(s, Priority.INTERACTIVE)
        or _queue_nonempty_excluding_dead(s, Priority.BATCH_PIPELINE)
    )

    n = len(q)
    for _ in range(n):
        pipeline = q.pop(0)
        pid = pipeline.pipeline_id

        # Skip pipelines we've dropped or no longer track.
        if pid in s.dead_pipelines or pid not in s.pipelines_by_id:
            continue

        status = pipeline.runtime_status()

        # Drop completed pipelines from our tracking.
        if status.is_pipeline_successful():
            _drop_pipeline(s, pid)
            continue

        # Enforce per-pipeline in-flight limit (and "one assignment per pipeline per tick").
        inflight = _inflight_ops(status)
        limit = s.inflight_limit.get(pipeline.priority, 1)
        if inflight >= limit or pid in s._scheduled_pipeline_ids_this_tick:
            q.append(pipeline)
            continue

        # Find a ready-to-run operator.
        op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
        if not op_list:
            q.append(pipeline)
            continue

        op = op_list[0]
        s.op_to_pipeline_id[id(op)] = pid  # allow mapping failures back to the owning pipeline

        ram_req = _compute_ram_request_gb(s, pool, op)

        # If we already know the op needs more RAM than any pool can offer, drop it to avoid infinite queueing.
        if s._max_pool_ram_gb is not None and ram_req > float(s._max_pool_ram_gb):
            s.dead_pipelines.add(pid)
            _drop_pipeline(s, pid)
            continue

        cpu_req = _compute_cpu_request(s, pool, pipeline.priority, avail_cpu, have_other_waiting)

        # Fit check.
        if cpu_req <= 0.0 or ram_req <= 0.0 or cpu_req > avail_cpu or ram_req > avail_ram:
            q.append(pipeline)
            continue

        assignment = Assignment(
            ops=op_list,
            cpu=cpu_req,
            ram=ram_req,
            priority=pipeline.priority,
            pool_id=pool_id,
            pipeline_id=pid,
        )

        # Rotate pipeline to the back: it may have more work later.
        q.append(pipeline)

        s._scheduled_pipeline_ids_this_tick.add(pid)
        return assignment, (avail_cpu - cpu_req), (avail_ram - ram_req)

    return None, avail_cpu, avail_ram


@register_scheduler(key="scheduler_est_010")
def scheduler_est_010(s, results: List[ExecutionResult], pipelines: List[Pipeline]) -> Tuple[List[Suspend], List[Assignment]]:
    """
    Scheduling step:
      - Add new pipelines to priority queues.
      - Process results, adapting RAM on OOM and dropping non-retryable failures.
      - Age/promote long-waiting batch pipelines.
      - For each pool, repeatedly schedule ready operators, preferring higher priority.
    """
    s.tick += 1
    suspensions: List[Suspend] = []
    assignments: List[Assignment] = []
    s._scheduled_pipeline_ids_this_tick = set()

    # Cache cluster max RAM (largest pool) once we have executor info.
    if s._max_pool_ram_gb is None:
        try:
            s._max_pool_ram_gb = max(p.max_ram_pool for p in s.executor.pools)
        except Exception:
            s._max_pool_ram_gb = None

    # Incorporate newly arrived pipelines.
    for p in pipelines:
        pid = p.pipeline_id
        if pid in s.pipelines_by_id or pid in s.dead_pipelines:
            continue
        s.pipelines_by_id[pid] = p
        s.enqueue_tick[pid] = s.tick
        s.enqueued.add(pid)
        s.queues[p.priority].append(p)

    # Process results (OOM retry, drop non-OOM failures).
    for r in results:
        if not r.failed():
            # On success, we could clear overrides, but keeping them is safe and can reduce re-OOM
            # if the same op shape reappears (rare). We'll leave them as-is.
            continue

        oom = _is_oom_error(getattr(r, "error", None))
        ops = getattr(r, "ops", None) or []

        # If we can identify the owning pipeline from any op, use it to drop after too many retries / non-OOM.
        owning_pipeline_id = None
        for op in ops:
            owning_pipeline_id = s.op_to_pipeline_id.get(id(op), None)
            if owning_pipeline_id is not None:
                break

        if not oom:
            # Non-OOM failures: do not retry; drop the whole pipeline from our control.
            if owning_pipeline_id is not None:
                s.dead_pipelines.add(owning_pipeline_id)
                _drop_pipeline(s, owning_pipeline_id)
            continue

        # OOM failures: increase RAM override for each op and allow retry (up to cap).
        for op in ops:
            opid = id(op)
            attempts = s.op_attempts.get(opid, 0) + 1
            s.op_attempts[opid] = attempts

            if attempts > s.max_retries_oom:
                # Too many OOMs => give up on the pipeline (avoid endless thrash).
                pid = s.op_to_pipeline_id.get(opid, owning_pipeline_id)
                if pid is not None:
                    s.dead_pipelines.add(pid)
                    _drop_pipeline(s, pid)
                continue

            # Use last allocation as a lower bound, then grow (doubling is simple and effective).
            try:
                last_ram = float(getattr(r, "ram", 0.0) or 0.0)
            except Exception:
                last_ram = 0.0

            prev_override = float(s.op_ram_override_gb.get(opid, 0.0))
            new_override = prev_override
            grow_target = last_ram * 2.0 if last_ram > 0.0 else (prev_override * 2.0 if prev_override > 0.0 else s.default_ram_gb * 2.0)
            if grow_target > new_override:
                new_override = grow_target

            # Bound by largest pool (if known) to prevent impossible requests.
            if s._max_pool_ram_gb is not None and new_override > float(s._max_pool_ram_gb):
                new_override = float(s._max_pool_ram_gb)

            s.op_ram_override_gb[opid] = new_override

    # Promote aged batch pipelines to reduce starvation.
    _maybe_promote_aged_batch(s)

    # If there's nothing to do, exit quickly (but don't idle if there are waiting pipelines).
    have_waiting = any(s.queues[p] for p in s.queues)
    if not have_waiting and not pipelines and not results:
        return suspensions, assignments

    # Schedule across pools.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool

        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # Fill the pool with multiple assignments if possible.
        for _ in range(s.max_assignments_per_pool_per_tick):
            if avail_cpu <= 0 or avail_ram <= 0:
                break

            made_progress = False

            # Strict priority: QUERY > INTERACTIVE > BATCH
            for prio in (Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE):
                a, avail_cpu, avail_ram = _schedule_one_from_priority_queue(
                    s, pool_id, pool, prio, avail_cpu, avail_ram
                )
                if a is not None:
                    assignments.append(a)
                    made_progress = True
                    break

            if not made_progress:
                break

    return suspensions, assignments
