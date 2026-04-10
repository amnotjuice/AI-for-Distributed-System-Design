# policy_key: scheduler_est_037
# reasoning_effort: high
# model: gpt-5.2-2025-12-11
# llm_cost: 0.141378
# generation_seconds: 73.71
# generated_at: 2026-03-31T19:31:58.026577
@register_scheduler_init(key="scheduler_est_037")
def scheduler_est_037_init(s):
    """Priority-aware, right-sized packing scheduler (incremental improvement over naive FIFO).

    Key changes vs naive:
      - Maintain separate waiting queues by priority (QUERY > INTERACTIVE > BATCH).
      - Avoid allocating the entire pool to a single op; instead right-size per-op CPU/RAM so
        multiple ops can run concurrently, improving latency under contention.
      - Use op.estimate.mem_peak_gb with a safety factor; on OOM-like failures, increase a
        per-op memory multiplier to reduce repeated OOMs and tail latency.

    Notes:
      - No preemption in this iteration (keeps policy simple and less churn-prone).
      - Fairness: round-robin within each priority queue.
    """
    s.waiting_by_prio = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }

    # Per-operator memory multiplier learned from OOM-like failures (keyed by op identity/name).
    s.op_mem_multiplier = {}

    # Simple knobs (kept conservative for "small improvements first").
    s.mem_safety = 1.25          # multiply estimate.mem_peak_gb by this factor
    s.mem_bump_on_oom = 1.6      # multiplicative bump after OOM-like failure
    s.mem_multiplier_cap = 8.0   # prevent unbounded growth

    # Minimum allocations to avoid pathological tiny containers.
    s.min_cpu_per_op = 0.25
    s.min_ram_gb_per_op = 0.25

    # Target CPU per op by priority (kept small to encourage packing & lower queuing latency).
    s.cpu_target_by_prio = {
        Priority.QUERY: 1.0,
        Priority.INTERACTIVE: 2.0,
        Priority.BATCH_PIPELINE: 4.0,
    }

    # When high-priority work is waiting, keep this fraction of each pool uncommitted from BATCH.
    s.reserve_frac_for_high = 0.20


def _est_mem_gb(s, op, pool):
    # Base estimate from the simulator (GB).
    base = None
    est = getattr(op, "estimate", None)
    if est is not None:
        base = getattr(est, "mem_peak_gb", None)
    if base is None:
        base = 1.0

    # Identify operator for learned multiplier (best-effort stable key).
    op_id = getattr(op, "op_id", None)
    if op_id is None:
        op_id = getattr(op, "operator_id", None)
    if op_id is None:
        op_id = getattr(op, "name", None)
    if op_id is None:
        op_id = str(type(op))

    mult = s.op_mem_multiplier.get(op_id, 1.0)

    # Apply safety + learned multiplier.
    need = float(base) * float(s.mem_safety) * float(mult)

    # Clamp to pool max (scheduler can't ask for more than exists).
    need = max(need, s.min_ram_gb_per_op)
    need = min(need, float(pool.max_ram_pool))
    return need, op_id


def _target_cpu(s, priority, pool, avail_cpu):
    # Small, priority-aware CPU allocation to reduce head-of-line blocking.
    tgt = float(s.cpu_target_by_prio.get(priority, 2.0))

    # Don't over-allocate on small pools.
    tgt = min(tgt, max(float(pool.max_cpu_pool) * 0.5, s.min_cpu_per_op))

    # Must fit available CPU.
    tgt = min(tgt, float(avail_cpu))
    tgt = max(tgt, 0.0)
    return tgt


def _looks_like_oom(err):
    if not err:
        return False
    e = str(err).lower()
    return ("oom" in e) or ("out of memory" in e) or ("cuda out of memory" in e) or ("killed" in e and "memory" in e)


def _pop_next_runnable_pipeline(queue):
    """Round-robin scan: returns (pipeline, op_list) or (None, None).

    Side effects:
      - Completed pipelines are dropped.
      - Non-runnable pipelines are rotated to the back.
      - Runnable pipeline is also rotated to the back (so we don't starve others).
    """
    if not queue:
        return None, None

    n = len(queue)
    for _ in range(n):
        pipeline = queue.pop(0)
        status = pipeline.runtime_status()
        if status.is_pipeline_successful():
            # drop completed pipeline
            continue

        # Find at most one runnable op (keep it simple to avoid over-parallelizing a single DAG).
        op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
        queue.append(pipeline)
        if op_list:
            return pipeline, op_list

    return None, None


@register_scheduler(key="scheduler_est_037")
def scheduler_est_037(s, results, pipelines):
    """
    Scheduler step:
      1) Enqueue arriving pipelines into per-priority queues.
      2) Learn from OOM-like failures: increase memory multiplier for the failed ops.
      3) For each pool, pack multiple assignments:
          - Always consider higher priority first.
          - Right-size CPU/RAM rather than consuming the full pool.
          - If high priority is waiting, prevent BATCH from consuming the last reserve fraction.
    """
    # Enqueue new pipelines by priority.
    for p in pipelines:
        pr = getattr(p, "priority", Priority.BATCH_PIPELINE)
        if pr not in s.waiting_by_prio:
            pr = Priority.BATCH_PIPELINE
        s.waiting_by_prio[pr].append(p)

    # Learn from execution results (OOM -> bump learned memory multiplier).
    for r in results:
        if not r.failed():
            continue
        if not _looks_like_oom(getattr(r, "error", None)):
            continue

        # For each failed op, bump multiplier based on our best-effort op key.
        for op in getattr(r, "ops", []) or []:
            op_id = getattr(op, "op_id", None)
            if op_id is None:
                op_id = getattr(op, "operator_id", None)
            if op_id is None:
                op_id = getattr(op, "name", None)
            if op_id is None:
                op_id = str(type(op))

            cur = float(s.op_mem_multiplier.get(op_id, 1.0))
            new = min(cur * float(s.mem_bump_on_oom), float(s.mem_multiplier_cap))
            s.op_mem_multiplier[op_id] = new

    # Early exit if nothing changed (keeps simulator fast).
    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    # Detect high-priority backlog (coarse signal).
    high_waiting = bool(s.waiting_by_prio[Priority.QUERY] or s.waiting_by_prio[Priority.INTERACTIVE])

    # Priority order: protect latency.
    prio_order = [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]

    # Attempt to pack assignments in each pool.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)
        if avail_cpu <= 0.0 or avail_ram <= 0.0:
            continue

        # Reserve some headroom from BATCH when high-priority work is waiting.
        reserve_cpu = float(pool.max_cpu_pool) * float(s.reserve_frac_for_high) if high_waiting else 0.0
        reserve_ram = float(pool.max_ram_pool) * float(s.reserve_frac_for_high) if high_waiting else 0.0

        # Keep assigning until we can't fit anything else reasonably.
        made_progress = True
        while made_progress:
            made_progress = False

            for pr in prio_order:
                q = s.waiting_by_prio[pr]
                if not q:
                    continue

                pipeline, op_list = _pop_next_runnable_pipeline(q)
                if pipeline is None or not op_list:
                    continue

                # Compute request sizes.
                mem_need, _ = _est_mem_gb(s, op_list[0], pool)
                cpu_need = _target_cpu(s, pr, pool, avail_cpu)

                # Enforce minimums.
                cpu_need = max(cpu_need, s.min_cpu_per_op)
                mem_need = max(mem_need, s.min_ram_gb_per_op)

                # If this is BATCH and high-priority is waiting, don't consume reserved headroom.
                if pr == Priority.BATCH_PIPELINE and high_waiting:
                    if (avail_cpu - cpu_need) < reserve_cpu or (avail_ram - mem_need) < reserve_ram:
                        # Can't schedule this batch op without violating reserve; try other work.
                        continue

                # Must fit current availability.
                if cpu_need > avail_cpu or mem_need > avail_ram:
                    continue

                assignments.append(
                    Assignment(
                        ops=op_list,
                        cpu=cpu_need,
                        ram=mem_need,
                        priority=pipeline.priority,
                        pool_id=pool_id,
                        pipeline_id=pipeline.pipeline_id,
                    )
                )

                avail_cpu -= cpu_need
                avail_ram -= mem_need
                made_progress = True

                # If we are extremely low on resources, stop trying to pack further.
                if avail_cpu <= 0.0 or avail_ram <= 0.0:
                    made_progress = False
                break  # restart prio loop with updated avail

    return suspensions, assignments
