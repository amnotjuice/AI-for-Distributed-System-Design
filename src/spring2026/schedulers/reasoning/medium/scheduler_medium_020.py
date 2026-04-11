# policy_key: scheduler_medium_020
# reasoning_effort: medium
# model: gpt-5.2-2025-12-11
# llm_cost: 0.064581
# generation_seconds: 96.90
# generated_at: 2026-03-12T22:21:30.556513
@register_scheduler_init(key="scheduler_medium_020")
def scheduler_medium_020_init(s):
    """Priority-aware, latency-oriented scheduler with simple fairness and OOM backoff.

    Key ideas (small, incremental improvements over naive FIFO):
      1) Priority queues: always consider QUERY/INTERACTIVE before BATCH.
      2) Avoid "give everything to one op": right-size CPU/RAM per op to enable concurrency.
      3) Simple reservation: don't let batch consume the last headroom when high-priority is waiting.
      4) OOM-aware RAM hints: if an op fails with OOM, retry later with more RAM (exponential-ish backoff).
      5) Bounded retries per pipeline to avoid infinite loops on non-OOM failures.
    """
    from collections import deque

    # Waiting queues per priority
    s.waiting = {
        Priority.QUERY: deque(),
        Priority.INTERACTIVE: deque(),
        Priority.BATCH_PIPELINE: deque(),
    }

    # Weighted round-robin sequence (QUERY favored most)
    s._prio_rr = (
        [Priority.QUERY] * 4
        + [Priority.INTERACTIVE] * 2
        + [Priority.BATCH_PIPELINE] * 1
    )

    # Per-pool RR cursor to spread decisions evenly across ticks
    s._rr_cursor_by_pool = {}

    # OOM-based RAM hints by operator "key"
    s.op_ram_hint = {}

    # Track pipeline failure transitions and cap retries
    s.pipeline_attempts = {}     # pipeline_id -> attempts
    s.pipeline_failed_seen = {}  # pipeline_id -> last seen FAILED count
    s.max_pipeline_attempts = 3


def _scheduler_medium_020_is_oom_error(err):
    if not err:
        return False
    e = str(err).lower()
    return ("oom" in e) or ("out of memory" in e) or ("memoryerror" in e)


def _scheduler_medium_020_op_key(op):
    # Try a few common fields; fall back to repr(). We avoid using pipeline_id here
    # so hints generalize across pipelines when operator identities are stable.
    for attr in ("op_id", "operator_id", "id", "name"):
        if hasattr(op, attr):
            v = getattr(op, attr)
            if v is not None:
                return f"{attr}:{v}"
    return f"repr:{repr(op)}"


def _scheduler_medium_020_prio_params(priority):
    # Conservative per-op sizing targets to increase concurrency while favoring latency.
    # These are fractions of pool max resources.
    if priority == Priority.QUERY:
        return 0.50, 0.50  # cpu_frac, ram_frac
    if priority == Priority.INTERACTIVE:
        return 0.33, 0.33
    return 0.20, 0.20  # batch


def _scheduler_medium_020_next_priority(s, pool_id):
    # Weighted RR over available queues.
    cursor = s._rr_cursor_by_pool.get(pool_id, 0)
    n = len(s._prio_rr)
    for i in range(n):
        pr = s._prio_rr[(cursor + i) % n]
        if s.waiting[pr]:
            s._rr_cursor_by_pool[pool_id] = (cursor + i + 1) % n
            return pr
    return None


def _scheduler_medium_020_has_high_prio_backlog(s):
    return bool(s.waiting[Priority.QUERY]) or bool(s.waiting[Priority.INTERACTIVE])


@register_scheduler(key="scheduler_medium_020")
def scheduler_medium_020(s, results, pipelines):
    """
    Scheduler step:
      - Enqueue arrivals by priority
      - Update OOM hints from results
      - For each pool, schedule multiple ops per tick using weighted RR:
          * QUERY > INTERACTIVE > BATCH
          * Reserve headroom if high-priority backlog exists
          * Requeue pipelines to allow progress across many pipelines
    """
    # Enqueue new pipelines
    for p in pipelines:
        s.waiting[p.priority].append(p)

    # Early exit if nothing to do
    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    # Update OOM-based RAM hints from execution results
    # Note: results don't reliably include pipeline_id in the provided interface, so hints are op-scoped.
    for r in results:
        try:
            failed = r.failed()
        except Exception:
            failed = False

        if not failed:
            continue

        if _scheduler_medium_020_is_oom_error(getattr(r, "error", None)):
            # Increase RAM hint for the involved ops based on attempted RAM
            pool = s.executor.pools[r.pool_id]
            attempted_ram = getattr(r, "ram", 0) or 0
            for op in getattr(r, "ops", []) or []:
                k = _scheduler_medium_020_op_key(op)
                prev = s.op_ram_hint.get(k, 0) or 0
                # If we know attempted RAM, double it; otherwise jump to a safe-ish fraction.
                if attempted_ram > 0:
                    new_hint = attempted_ram * 2
                else:
                    new_hint = pool.max_ram_pool * 0.25
                # Clamp to pool max and ensure monotonic increases
                new_hint = min(pool.max_ram_pool, max(prev, new_hint))
                s.op_ram_hint[k] = new_hint

    # Scheduling loop: fill each pool with multiple assignments if resources allow
    high_prio_backlog = _scheduler_medium_020_has_high_prio_backlog(s)

    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool

        # Simple reservation when high-priority backlog exists:
        # keep some headroom so batch can't take the last resources.
        reserve_cpu = (0.30 * pool.max_cpu_pool) if high_prio_backlog else 0.0
        reserve_ram = (0.30 * pool.max_ram_pool) if high_prio_backlog else 0.0

        # Avoid infinite loops when pipelines are blocked (no ready ops)
        no_progress_iters = 0
        max_no_progress_iters = 32

        # Cap number of assignments per pool per tick to keep step time bounded
        max_assignments_per_pool = 16
        num_assigned_here = 0

        while (
            avail_cpu > 0
            and avail_ram > 0
            and num_assigned_here < max_assignments_per_pool
            and no_progress_iters < max_no_progress_iters
        ):
            pr = _scheduler_medium_020_next_priority(s, pool_id)
            if pr is None:
                break

            pipeline = s.waiting[pr].popleft()
            status = pipeline.runtime_status()

            # Drop completed pipelines
            if status.is_pipeline_successful():
                continue

            # Track failure transitions (regardless of cause) and cap retries.
            # This prevents infinite resubmissions for non-OOM errors.
            try:
                failed_cnt = status.state_counts[OperatorState.FAILED]
            except Exception:
                failed_cnt = 0

            prev_failed_cnt = s.pipeline_failed_seen.get(pipeline.pipeline_id, 0)
            if failed_cnt > prev_failed_cnt:
                s.pipeline_failed_seen[pipeline.pipeline_id] = failed_cnt
                s.pipeline_attempts[pipeline.pipeline_id] = s.pipeline_attempts.get(pipeline.pipeline_id, 0) + 1

            if s.pipeline_attempts.get(pipeline.pipeline_id, 0) > s.max_pipeline_attempts:
                # Give up on this pipeline
                continue

            # Pick one ready op to keep fairness across pipelines
            op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
            if not op_list:
                # Blocked (waiting on parents/running elsewhere); requeue and move on
                s.waiting[pr].append(pipeline)
                no_progress_iters += 1
                continue

            op = op_list[0]
            opk = _scheduler_medium_020_op_key(op)

            cpu_frac, ram_frac = _scheduler_medium_020_prio_params(pr)

            # Default sizing based on pool capacity; clamp by current availability.
            cpu_req = max(1.0, float(int(max(1.0, round(pool.max_cpu_pool * cpu_frac)))))
            cpu_req = min(cpu_req, avail_cpu)

            ram_default = max(1.0, pool.max_ram_pool * ram_frac)
            ram_hint = s.op_ram_hint.get(opk, 0) or 0

            # If the pipeline has already failed before, bias toward more RAM (likely OOM)
            attempt = s.pipeline_attempts.get(pipeline.pipeline_id, 0)
            ram_req = max(ram_default, ram_hint)
            if attempt > 0:
                ram_req = ram_req * (1.0 + 0.5 * attempt)

            # Clamp to pool and to what is available right now
            ram_req = min(pool.max_ram_pool, ram_req)
            ram_req = min(ram_req, avail_ram)

            # If we can't allocate meaningful resources, stop trying to pack this pool.
            if cpu_req <= 0 or ram_req <= 0:
                s.waiting[pr].appendleft(pipeline)
                break

            # Reservation rule: if batch, don't consume into reserved headroom when high-priority is waiting.
            if pr == Priority.BATCH_PIPELINE and high_prio_backlog:
                if (avail_cpu - cpu_req) < reserve_cpu or (avail_ram - ram_req) < reserve_ram:
                    # Defer batch and try other work
                    s.waiting[pr].append(pipeline)
                    no_progress_iters += 1
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

            # Optimistically update remaining capacity for packing multiple assignments
            avail_cpu -= cpu_req
            avail_ram -= ram_req

            # Requeue pipeline so other pipelines also get a chance next selection
            s.waiting[pr].append(pipeline)

            # We made progress
            num_assigned_here += 1
            no_progress_iters = 0

    return suspensions, assignments
