# policy_key: scheduler_medium_004
# reasoning_effort: medium
# model: gpt-5.2-2025-12-11
# llm_cost: 0.103630
# generation_seconds: 138.64
# generated_at: 2026-03-12T21:57:41.120436
@register_scheduler_init(key="scheduler_medium_004")
def scheduler_medium_004_init(s):
    """Priority-aware, latency-biased scheduler (incremental improvement over naive FIFO).

    Key ideas (kept intentionally simple/robust):
      - Maintain separate queues per priority (QUERY > INTERACTIVE > BATCH_PIPELINE).
      - Prevent a single low-priority op from consuming an entire pool by capping per-assignment CPU/RAM.
      - If only one pool exists, reserve a fraction of capacity for high-priority work when any exists.
      - Retry OOM-like failures by increasing a per-pipeline RAM floor; treat other failures as non-retryable.
      - Avoid running multiple ops from the same pipeline concurrently (reduces memory pressure and latency variance).
      - If multiple pools exist, bias pool 0 toward high-priority work, but allow spillover when needed.
    """
    s.queues = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }
    s.active_pipelines = {}  # pipeline_id -> Pipeline (for de-dupe + bookkeeping)

    # Map scheduled operator objects back to pipeline_id (for interpreting ExecutionResult).
    s.op_to_pipeline_id = {}  # id(op) -> pipeline_id

    # Adaptive retry knobs (start unset; learned on OOM-like failures).
    s.pipeline_ram_floor = {}  # pipeline_id -> minimum RAM to request next time

    # If we see a failure that doesn't look like OOM, we stop retrying that pipeline.
    s.pipeline_nonretryable_fail = set()  # pipeline_id

    # Simple time base for optional backoff / future features.
    s.tick = 0


@register_scheduler(key="scheduler_medium_004")
def scheduler_medium_004(s, results, pipelines):
    """
    Scheduler step:
      1) Enqueue new pipelines by priority
      2) Process results (learn OOM -> bump RAM floor)
      3) Assign runnable ops in priority order with pool-aware sizing + reservations
    """
    s.tick += 1

    def _is_oom_error(err) -> bool:
        msg = "" if err is None else str(err)
        msg_u = msg.upper()
        # Keep heuristic broad; simulator error strings can vary.
        return ("OOM" in msg_u) or ("OUT OF MEMORY" in msg_u) or ("MEMORY" in msg_u and "KILL" in msg_u)

    def _high_priority_backlog_exists() -> bool:
        return (len(s.queues[Priority.QUERY]) + len(s.queues[Priority.INTERACTIVE])) > 0

    def _prio_order_for_pool(pool_id: int, high_backlog: bool) -> list:
        # If we have multiple pools, bias pool 0 toward latency-sensitive work.
        if s.executor.num_pools > 1 and pool_id == 0:
            return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]
        # Other pools: if high priority is waiting, let it spill; otherwise, focus batch.
        if high_backlog:
            return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]
        return [Priority.BATCH_PIPELINE, Priority.QUERY, Priority.INTERACTIVE]

    def _caps_for_priority(pool, prio):
        # Conservative caps to avoid head-of-line blocking caused by "take all available".
        # These are fractions of *pool max*, not current availability.
        if prio == Priority.QUERY:
            return 0.50, 0.50  # cpu_frac, ram_frac
        if prio == Priority.INTERACTIVE:
            return 0.50, 0.50
        # Batch: smaller cap encourages concurrency and protects latency.
        return 0.33, 0.40

    def _select_next_pipeline_and_op(prio_order: list):
        # Pick the next pipeline that has:
        #   - not completed
        #   - not marked non-retryable failed
        #   - no in-flight ops (avoid concurrent ops within same pipeline)
        #   - at least one assignable op with parents complete
        INFLIGHT_STATES = [OperatorState.ASSIGNED, OperatorState.RUNNING, OperatorState.SUSPENDING]

        for pr in prio_order:
            q = s.queues[pr]
            n = len(q)
            if n == 0:
                continue

            for _ in range(n):
                p = q.pop(0)

                # If it's no longer active (e.g., already dropped), skip.
                if p.pipeline_id not in s.active_pipelines:
                    continue

                status = p.runtime_status()

                if status.is_pipeline_successful():
                    # Pipeline done; retire it.
                    s.active_pipelines.pop(p.pipeline_id, None)
                    s.pipeline_ram_floor.pop(p.pipeline_id, None)
                    s.pipeline_nonretryable_fail.discard(p.pipeline_id)
                    continue

                if p.pipeline_id in s.pipeline_nonretryable_fail:
                    # Drop pipeline permanently on non-OOM failure.
                    s.active_pipelines.pop(p.pipeline_id, None)
                    s.pipeline_ram_floor.pop(p.pipeline_id, None)
                    continue

                # Avoid multiple concurrent ops from the same pipeline.
                inflight = status.get_ops(INFLIGHT_STATES, require_parents_complete=False)
                if inflight:
                    q.append(p)
                    continue

                op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
                if not op_list:
                    q.append(p)
                    continue

                # Found work.
                return p, op_list, pr

        return None, None, None

    # 1) Enqueue new pipelines
    for p in pipelines:
        # De-dupe: only enqueue the first time we see a pipeline_id.
        if p.pipeline_id in s.active_pipelines:
            continue
        s.active_pipelines[p.pipeline_id] = p
        # Ignore unknown priorities by treating them as batch-like.
        pr = p.priority if p.priority in s.queues else Priority.BATCH_PIPELINE
        s.queues[pr].append(p)

    # 2) Process results (learn RAM needs and mark non-retryable failures)
    for r in results:
        if not r.failed():
            continue

        # Try to determine pipeline_id from the op objects.
        pid = None
        for op in getattr(r, "ops", []) or []:
            pid = s.op_to_pipeline_id.get(id(op))
            if pid is not None:
                break

        # If we can't attribute the failure, we can't learn from it; skip safely.
        if pid is None:
            continue

        if _is_oom_error(getattr(r, "error", None)):
            # Increase RAM floor for future retries of this pipeline.
            # Use last attempted RAM as signal; bump multiplicatively.
            last_ram = float(getattr(r, "ram", 0.0) or 0.0)
            if last_ram <= 0:
                # Fallback: a small but non-zero bump
                bumped = 1.0
            else:
                bumped = last_ram * 1.8

            prev = float(s.pipeline_ram_floor.get(pid, 0.0) or 0.0)
            s.pipeline_ram_floor[pid] = max(prev, bumped, 1.0)
        else:
            # Non-OOM failure: don't keep retrying indefinitely.
            s.pipeline_nonretryable_fail.add(pid)

    # Early exit if no state change that would affect decisions
    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    # 3) Schedule across pools
    high_backlog = _high_priority_backlog_exists()

    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)

        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # If single pool, keep some headroom for high-priority work when it exists.
        reserve_cpu = 0.0
        reserve_ram = 0.0
        if s.executor.num_pools == 1 and high_backlog:
            reserve_cpu = float(pool.max_cpu_pool) * 0.25
            reserve_ram = float(pool.max_ram_pool) * 0.25

        prio_order = _prio_order_for_pool(pool_id, high_backlog)

        # Try multiple placements per pool (instead of the naive "one op per pool" behavior).
        # Guard against infinite loops when many pipelines are blocked.
        attempts = 0
        max_attempts = 64

        while attempts < max_attempts and avail_cpu > 0 and avail_ram > 0:
            attempts += 1

            p, op_list, pr = _select_next_pipeline_and_op(prio_order)
            if p is None:
                break

            # If pool 0 is "interactive-biased" and high priority exists, don't run batch there.
            if s.executor.num_pools > 1 and pool_id == 0 and high_backlog and pr == Priority.BATCH_PIPELINE:
                # Put it back and stop trying to place batch into this pool.
                s.queues[pr].append(p)
                break

            cpu_frac, ram_frac = _caps_for_priority(pool, pr)

            # Target sizes: capped by pool max and remaining availability.
            cpu_target = min(avail_cpu, max(1.0, float(pool.max_cpu_pool) * cpu_frac))
            ram_target = min(avail_ram, max(1.0, float(pool.max_ram_pool) * ram_frac))

            # Apply learned RAM floor on OOM retries (per pipeline).
            ram_floor = float(s.pipeline_ram_floor.get(p.pipeline_id, 0.0) or 0.0)
            if ram_floor > ram_target:
                ram_target = ram_floor

            # If we can't fit the floor, we can't schedule it now.
            if ram_target > avail_ram or cpu_target > avail_cpu:
                s.queues[pr].append(p)
                continue

            # Respect reservation for high priority when scheduling batch on a single shared pool.
            if pr == Priority.BATCH_PIPELINE and (avail_cpu - cpu_target < reserve_cpu or avail_ram - ram_target < reserve_ram):
                s.queues[pr].append(p)
                continue

            # Create assignment
            assignment = Assignment(
                ops=op_list,
                cpu=cpu_target,
                ram=ram_target,
                priority=p.priority,
                pool_id=pool_id,
                pipeline_id=p.pipeline_id,
            )
            assignments.append(assignment)

            # Bookkeeping for result attribution.
            for op in op_list:
                s.op_to_pipeline_id[id(op)] = p.pipeline_id

            # Requeue the pipeline for future ops (it now has in-flight work, so it won't be selected again until done).
            s.queues[pr].append(p)

            # Update remaining capacity in this pool for this tick.
            avail_cpu -= float(cpu_target)
            avail_ram -= float(ram_target)

            # Stop if remaining resources are too small to be useful.
            if avail_cpu < 1.0 or avail_ram < 1.0:
                break

    return suspensions, assignments
