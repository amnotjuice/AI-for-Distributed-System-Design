# policy_key: scheduler_medium_019
# reasoning_effort: medium
# model: gpt-5.2-2025-12-11
# llm_cost: 0.076649
# generation_seconds: 73.32
# generated_at: 2026-03-12T22:19:53.650253
@register_scheduler_init(key="scheduler_medium_019")
def scheduler_medium_019_init(s):
    """
    Priority-aware FIFO with simple OOM-adaptive right-sizing.

    Improvements over the naive baseline:
      - Maintain separate queues per priority and always schedule higher priority first.
      - Prevent batch from consuming the entire pool by reserving headroom when higher-priority
        work is waiting (reduces tail latency for interactive/query).
      - Retry OOM failures with exponentially increased RAM requests (best-effort inference).
      - Simple fairness via round-robin within each priority queue.
      - Avoid duplicate assignments within the same tick (one op per pipeline per tick).
    """
    from collections import deque

    s.queues = {
        Priority.INTERACTIVE: deque(),
        Priority.QUERY: deque(),
        Priority.BATCH_PIPELINE: deque(),
    }

    # Per-pipeline resource sizing hints learned from outcomes (primarily OOMs).
    # Values are multiplicative factors applied to the base per-priority request.
    s.pipeline_hints = {}  # pipeline_id -> dict(ram_mult, cpu_mult, oom_retries, dead)

    # Policy knobs (kept intentionally simple / conservative).
    s.max_oom_retries = 3

    # Base request shares (fraction of pool capacity) per assignment.
    # Interactive gets more to finish quickly; batch gets less to improve concurrency and protect latency.
    s.cpu_share = {
        Priority.INTERACTIVE: 0.50,
        Priority.QUERY: 0.35,
        Priority.BATCH_PIPELINE: 0.20,
    }
    s.ram_share = {
        Priority.INTERACTIVE: 0.50,
        Priority.QUERY: 0.35,
        Priority.BATCH_PIPELINE: 0.25,
    }

    # Reservations (fraction of pool capacity) held back from lower priorities when higher priorities wait.
    s.reserve_for_interactive = (0.25, 0.25)  # (cpu_frac, ram_frac)
    s.reserve_for_query = (0.15, 0.15)        # (cpu_frac, ram_frac)

    # Minimum allocation for any assignment (avoid zero/near-zero allocations).
    s.min_cpu = 1.0
    s.min_ram = 1.0


@register_scheduler(key="scheduler_medium_019")
def scheduler_medium_019_scheduler(s, results: list, pipelines: list):
    """
    Step function:
      1) Ingest new pipelines into per-priority queues.
      2) Update sizing hints from execution results (OOM => increase RAM).
      3) For each pool, schedule ready ops from highest to lowest priority, while:
           - reserving headroom if higher-priority queues are non-empty,
           - keeping round-robin fairness within each priority,
           - assigning at most one op per pipeline per tick to avoid duplicate scheduling.
    """
    def _is_oom(err) -> bool:
        if err is None:
            return False
        try:
            msg = str(err).lower()
        except Exception:
            return False
        return ("oom" in msg) or ("out of memory" in msg) or ("cuda out of memory" in msg) or ("memoryerror" in msg)

    def _get_or_init_hint(pipeline_id):
        h = s.pipeline_hints.get(pipeline_id)
        if h is None:
            h = {"ram_mult": 1.0, "cpu_mult": 1.0, "oom_retries": 0, "dead": False}
            s.pipeline_hints[pipeline_id] = h
        return h

    def _queue_for_priority(prio):
        # Defensive: unknown priorities go to batch queue.
        return s.queues.get(prio, s.queues[Priority.BATCH_PIPELINE])

    def _prio_order():
        # Highest to lowest
        return [Priority.INTERACTIVE, Priority.QUERY, Priority.BATCH_PIPELINE]

    def _reserved_capacity(pool, target_prio):
        """
        Compute (reserved_cpu, reserved_ram) that should be held back from target_prio
        because higher-priority work is waiting.
        """
        reserved_cpu = 0.0
        reserved_ram = 0.0

        if target_prio == Priority.BATCH_PIPELINE:
            # Reserve for interactive if any waiting
            if len(s.queues[Priority.INTERACTIVE]) > 0:
                reserved_cpu += pool.max_cpu_pool * s.reserve_for_interactive[0]
                reserved_ram += pool.max_ram_pool * s.reserve_for_interactive[1]
            # Reserve for query if any waiting
            if len(s.queues[Priority.QUERY]) > 0:
                reserved_cpu += pool.max_cpu_pool * s.reserve_for_query[0]
                reserved_ram += pool.max_ram_pool * s.reserve_for_query[1]
        elif target_prio == Priority.QUERY:
            # Reserve for interactive if any waiting
            if len(s.queues[Priority.INTERACTIVE]) > 0:
                reserved_cpu += pool.max_cpu_pool * s.reserve_for_interactive[0]
                reserved_ram += pool.max_ram_pool * s.reserve_for_interactive[1]

        return reserved_cpu, reserved_ram

    # 1) Ingest new pipelines
    for p in pipelines:
        _queue_for_priority(p.priority).append(p)
        _get_or_init_hint(p.pipeline_id)

    # 2) Update hints from results
    for r in results or []:
        # We infer only at the pipeline level to keep the policy simple and stable.
        if getattr(r, "failed", None) and r.failed():
            if _is_oom(getattr(r, "error", None)):
                hint = _get_or_init_hint(r.pipeline_id)
                hint["oom_retries"] += 1
                # Exponential backoff on RAM; cap indirectly by pool availability at assignment time.
                hint["ram_mult"] = min(hint["ram_mult"] * 2.0, 64.0)
            else:
                # Non-OOM failures are treated as terminal for scheduling to avoid hot-loop retries.
                hint = _get_or_init_hint(r.pipeline_id)
                hint["dead"] = True
        else:
            # Mild decay toward baseline after success to avoid permanently over-allocating.
            hint = _get_or_init_hint(r.pipeline_id)
            hint["ram_mult"] = max(1.0, hint["ram_mult"] * 0.95)
            hint["cpu_mult"] = max(1.0, hint["cpu_mult"] * 0.98)

    # Early exit if no state changes affecting decisions
    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    # Prevent duplicate assignment within this tick.
    scheduled_pipelines_this_tick = set()

    # Iterate pools and schedule greedily (highest priority first)
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]

        # Local view of remaining capacity for this tick (avoid over-issuing assignments).
        local_avail_cpu = float(pool.avail_cpu_pool)
        local_avail_ram = float(pool.avail_ram_pool)

        if local_avail_cpu <= 0 or local_avail_ram <= 0:
            continue

        made_progress = True
        # Continue filling the pool while we can place at least one more assignment.
        while made_progress:
            made_progress = False

            for prio in _prio_order():
                if local_avail_cpu <= 0 or local_avail_ram <= 0:
                    break

                q = s.queues[prio]
                if not q:
                    continue

                # Apply reservation if we're scheduling lower-priority work.
                reserved_cpu, reserved_ram = _reserved_capacity(pool, prio)
                eff_cpu = max(0.0, local_avail_cpu - reserved_cpu)
                eff_ram = max(0.0, local_avail_ram - reserved_ram)

                if eff_cpu < s.min_cpu or eff_ram < s.min_ram:
                    # Not enough capacity for this priority given reservations.
                    continue

                # Round-robin: scan at most the current queue length for a runnable pipeline.
                q_len = len(q)
                for _ in range(q_len):
                    pipeline = q.popleft()

                    hint = _get_or_init_hint(pipeline.pipeline_id)
                    if hint.get("dead", False):
                        # Drop terminal pipelines from our queue.
                        continue

                    status = pipeline.runtime_status()
                    if status.is_pipeline_successful():
                        # Completed pipelines should not be requeued.
                        continue

                    # Avoid retrying forever on OOM.
                    if hint.get("oom_retries", 0) > s.max_oom_retries:
                        hint["dead"] = True
                        continue

                    # Avoid scheduling the same pipeline multiple times in the same tick.
                    if pipeline.pipeline_id in scheduled_pipelines_this_tick:
                        q.append(pipeline)
                        continue

                    # Select one ready operator (parents complete).
                    op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
                    if not op_list:
                        # Not runnable yet; requeue.
                        q.append(pipeline)
                        continue

                    # Compute base request by priority share, then apply learned multipliers.
                    base_cpu = max(s.min_cpu, pool.max_cpu_pool * s.cpu_share.get(prio, 0.2))
                    base_ram = max(s.min_ram, pool.max_ram_pool * s.ram_share.get(prio, 0.25))

                    req_cpu = base_cpu * float(hint.get("cpu_mult", 1.0))
                    req_ram = base_ram * float(hint.get("ram_mult", 1.0))

                    # Clamp to effective and local availability.
                    cpu = min(req_cpu, eff_cpu, local_avail_cpu)
                    ram = min(req_ram, eff_ram, local_avail_ram)

                    if cpu < s.min_cpu or ram < s.min_ram:
                        # Can't place this op right now; requeue and keep looking.
                        q.append(pipeline)
                        continue

                    assignments.append(
                        Assignment(
                            ops=op_list,
                            cpu=cpu,
                            ram=ram,
                            priority=pipeline.priority,
                            pool_id=pool_id,
                            pipeline_id=pipeline.pipeline_id,
                        )
                    )

                    # Update local capacity and bookkeeping.
                    local_avail_cpu -= cpu
                    local_avail_ram -= ram
                    scheduled_pipelines_this_tick.add(pipeline.pipeline_id)

                    # Requeue pipeline for future ops.
                    q.append(pipeline)

                    made_progress = True
                    break  # move back to priority loop to re-evaluate reservations/capacity

                if made_progress:
                    break  # restart from highest priority after a successful placement

    return suspensions, assignments
