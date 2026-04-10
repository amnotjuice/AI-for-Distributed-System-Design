# policy_key: scheduler_low_044
# reasoning_effort: low
# model: gpt-5.2-2025-12-11
# llm_cost: 0.053860
# generation_seconds: 46.76
# generated_at: 2026-03-14T02:36:25.732235
@register_scheduler_init(key="scheduler_low_044")
def scheduler_low_044_init(s):
    """Priority-aware FIFO with conservative resource slicing + simple OOM-based RAM hinting.

    Improvements over naive FIFO:
      1) Priority queues: always try to schedule QUERY/INTERACTIVE before BATCH_PIPELINE.
      2) Soft reservations: when high-priority work is waiting, limit how much batch can consume
         so we keep headroom and reduce tail latency for interactive arrivals.
      3) Resource slicing: avoid giving a single assignment the entire pool by default; this
         enables more concurrency and reduces head-of-line blocking.
      4) OOM hinting: if an op fails with an OOM-like error, retry later with increased RAM.
    """
    from collections import deque

    # Separate waiting queues by priority (simple, stable behavior).
    s.q_query = deque()
    s.q_interactive = deque()
    s.q_batch = deque()

    # Per-(pipeline, op) RAM hint. We only increase on OOM-like failures.
    # Keying by (pipeline_id, repr(op)) is crude but deterministic and stable in the simulator.
    s.ram_hint = {}

    # Tunables (kept conservative; can be iterated later).
    s.reserve_cpu_frac_for_high = 0.20  # keep some CPU headroom when high-priority waiting
    s.reserve_ram_frac_for_high = 0.20  # keep some RAM headroom when high-priority waiting
    s.oom_ram_growth = 1.75            # multiplicative RAM increase on OOM-like failure
    s.oom_ram_min_bump = 1.0           # additive RAM bump (in case small numbers)
    s.max_assignments_per_pool_tick = 4  # prevent over-scheduling in one tick

    # Per-priority sizing caps as fractions of pool capacity (resource slicing).
    s.cpu_frac_cap = {
        Priority.QUERY: 0.75,
        Priority.INTERACTIVE: 0.60,
        Priority.BATCH_PIPELINE: 0.35,
    }
    s.ram_frac_cap = {
        Priority.QUERY: 0.75,
        Priority.INTERACTIVE: 0.60,
        Priority.BATCH_PIPELINE: 0.35,
    }


@register_scheduler(key="scheduler_low_044")
def scheduler_low_044_scheduler(s, results, pipelines):
    """Scheduler step: drain results -> update hints; enqueue new pipelines; assign ops by priority."""
    from collections import deque

    def _enqueue_pipeline(p):
        if p.priority == Priority.QUERY:
            s.q_query.append(p)
        elif p.priority == Priority.INTERACTIVE:
            s.q_interactive.append(p)
        else:
            s.q_batch.append(p)

    def _is_oom_error(err):
        if not err:
            return False
        msg = str(err).lower()
        return ("oom" in msg) or ("out of memory" in msg) or ("out-of-memory" in msg) or ("memory" in msg and "exceed" in msg)

    def _op_key(pipeline_id, op):
        # Use repr(op) to avoid relying on unknown attributes; deterministic in most sims.
        return (pipeline_id, repr(op))

    def _next_pipeline_with_ready_op(q):
        """Pop/rotate until finding a pipeline with at least one ready op. Returns (pipeline, [op]) or (None, None)."""
        n = len(q)
        for _ in range(n):
            p = q.popleft()
            status = p.runtime_status()

            # Drop completed pipelines.
            if status.is_pipeline_successful():
                continue

            # Only schedule ops whose parents are complete.
            ops = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
            if ops:
                return p, ops

            # Not schedulable yet; keep it in queue.
            q.append(p)
        return None, None

    # 1) Enqueue new arrivals.
    for p in pipelines:
        _enqueue_pipeline(p)

    # Early exit if nothing changed.
    if not pipelines and not results:
        return [], []

    # 2) Process execution results to update OOM hints.
    # Note: We do NOT automatically drop failed pipelines here; we let the runtime status reflect
    # failure and only "hint" RAM so the next retry (if simulator allows) can use more RAM.
    for r in results:
        if r is None:
            continue
        if r.failed() and _is_oom_error(r.error):
            # If the simulator returns ops as a list, bump hint for each op in the result.
            for op in (r.ops or []):
                # We don't have pipeline_id on result; best effort: use op repr alone if needed.
                # But we DO have pipeline_id in Assignment we create; so for now, we can only
                # apply hints when we can map via future pipeline scheduling.
                # Use a global key fallback; it is still helpful if repr(op) is stable.
                key = ("*", repr(op))
                prev = s.ram_hint.get(key, r.ram if r.ram else 0)
                bumped = max(prev * s.oom_ram_growth, prev + s.oom_ram_min_bump)
                s.ram_hint[key] = bumped

    # 3) Decide if high-priority work is waiting (to apply soft reservations).
    high_waiting = (len(s.q_query) + len(s.q_interactive)) > 0

    suspensions = []  # No preemption in this iteration (limited API visibility).
    assignments = []

    # 4) Assign work pool-by-pool, always preferring higher priorities.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool
        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # Soft reservations: limit what batch can take if high priority is waiting.
        reserve_cpu = pool.max_cpu_pool * s.reserve_cpu_frac_for_high if high_waiting else 0
        reserve_ram = pool.max_ram_pool * s.reserve_ram_frac_for_high if high_waiting else 0

        # Make a bounded number of assignments per pool per tick.
        for _ in range(s.max_assignments_per_pool_tick):
            # Refresh (we update local vars after each assignment).
            if avail_cpu <= 0 or avail_ram <= 0:
                break

            # Choose which queue to pull from.
            chosen_q = None
            chosen_pri = None

            # Always prioritize QUERY > INTERACTIVE > BATCH.
            if len(s.q_query) > 0:
                chosen_q = s.q_query
                chosen_pri = Priority.QUERY
            elif len(s.q_interactive) > 0:
                chosen_q = s.q_interactive
                chosen_pri = Priority.INTERACTIVE
            elif len(s.q_batch) > 0:
                chosen_q = s.q_batch
                chosen_pri = Priority.BATCH_PIPELINE
            else:
                break

            # For batch, enforce reservation (when high priority waiting).
            effective_avail_cpu = avail_cpu
            effective_avail_ram = avail_ram
            if chosen_pri == Priority.BATCH_PIPELINE and high_waiting:
                effective_avail_cpu = max(0, avail_cpu - reserve_cpu)
                effective_avail_ram = max(0, avail_ram - reserve_ram)
                if effective_avail_cpu <= 0 or effective_avail_ram <= 0:
                    # Can't schedule batch right now; stop scheduling batch on this pool this tick.
                    break

            p, op_list = _next_pipeline_with_ready_op(chosen_q)
            if not p or not op_list:
                # Nothing schedulable at this priority; try next priority within same tick.
                if chosen_pri == Priority.QUERY:
                    # Temporarily empty or blocked; attempt lower priority.
                    if len(s.q_interactive) > 0 or len(s.q_batch) > 0:
                        continue
                elif chosen_pri == Priority.INTERACTIVE:
                    if len(s.q_batch) > 0:
                        continue
                break

            # Determine per-assignment caps to prevent one task from monopolizing the pool.
            cpu_cap = max(0, min(effective_avail_cpu, pool.max_cpu_pool * s.cpu_frac_cap.get(p.priority, 0.5)))
            ram_cap = max(0, min(effective_avail_ram, pool.max_ram_pool * s.ram_frac_cap.get(p.priority, 0.5)))

            # Use OOM hint if present (try pipeline-specific first, then global fallback).
            op = op_list[0]
            hint = None
            k_specific = _op_key(p.pipeline_id, op)
            k_global = ("*", repr(op))
            if k_specific in s.ram_hint:
                hint = s.ram_hint[k_specific]
            elif k_global in s.ram_hint:
                hint = s.ram_hint[k_global]

            # RAM selection:
            # - If hinted, request at least the hint (bounded by current caps/availability).
            # - Else, request up to ram_cap (but not all remaining RAM).
            if hint is not None:
                ram_req = min(effective_avail_ram, max(1, min(hint, ram_cap)))
            else:
                ram_req = max(1, ram_cap)

            # CPU selection: similar slicing. Keep at least 1.
            cpu_req = max(1, cpu_cap)

            # If our computed request is infeasible, requeue the pipeline and stop on this pool.
            if cpu_req > effective_avail_cpu or ram_req > effective_avail_ram:
                # Put pipeline back to the front to preserve relative FIFO within priority.
                chosen_q.appendleft(p)
                break

            assignments.append(
                Assignment(
                    ops=op_list,
                    cpu=cpu_req,
                    ram=ram_req,
                    priority=p.priority,
                    pool_id=pool_id,
                    pipeline_id=p.pipeline_id,
                )
            )

            # Put pipeline back to the end to allow fairness among pipelines at same priority.
            chosen_q.append(p)

            # Update local availability.
            avail_cpu -= cpu_req
            avail_ram -= ram_req

    return suspensions, assignments
