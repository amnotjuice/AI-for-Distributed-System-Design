# policy_key: scheduler_high_012
# reasoning_effort: high
# model: gpt-5.2-2025-12-11
# llm_cost: 0.144283
# generation_seconds: 142.87
# generated_at: 2026-03-12T22:51:11.466963
@register_scheduler_init(key="scheduler_high_012")
def scheduler_high_012_init(s):
    """Priority-aware FIFO with simple headroom reservation.

    Small, low-risk improvements over naive FIFO:
      1) Maintain separate FIFO queues per priority and always consider higher priority first.
      2) Avoid letting batch consume the last slice of resources by reserving pool headroom for high-priority work.
      3) Fill a pool with multiple small/medium assignments (instead of at most one huge assignment per pool),
         which reduces queueing latency for interactive/query workloads.
      4) Keep the policy conservative: no preemption, no complex runtime prediction; just better ordering + headroom.
    """
    s.tick = 0

    # Per-priority waiting queues (FIFO within each priority).
    s.waiting_queues = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }

    # Per-pipeline lightweight metadata (used for basic retry budgeting / housekeeping).
    # Note: we cannot reliably attribute ExecutionResult failures to a pipeline without explicit pipeline_id in results,
    # so we keep this minimal and driven by pipeline.runtime_status() only.
    s.pipeline_meta = {}  # pipeline_id -> dict

    # Tuning knobs (kept simple / safe).
    s.max_assignments_per_pool = 8
    s.min_cpu = 1
    s.min_ram = 1

    # Reserve fractions (only enforced while there is any high-priority backlog).
    # Pool 0 is treated as "latency pool" if multiple pools exist: keep more headroom there.
    s.reserve_frac_pool0 = 0.50
    s.reserve_frac_other = 0.25

    # Per-op caps (to avoid single ops hoarding whole pools and inflating tail latency).
    s.cpu_cap = {
        Priority.QUERY: 4,
        Priority.INTERACTIVE: 8,
        Priority.BATCH_PIPELINE: 8,
    }
    s.ram_target_frac = {
        Priority.QUERY: 0.25,
        Priority.INTERACTIVE: 0.35,
        Priority.BATCH_PIPELINE: 0.25,
    }
    s.ram_cap_frac = {
        Priority.QUERY: 0.60,
        Priority.INTERACTIVE: 0.70,
        Priority.BATCH_PIPELINE: 0.60,
    }

    # Fairness: allow occasional batch starts on non-latency pools even under high load,
    # but only from resources beyond reserved headroom.
    s.high_quota_before_batch = 8
    s.pool_high_counter = {}  # pool_id -> int


@register_scheduler(key="scheduler_high_012")
def scheduler_high_012_scheduler(s, results, pipelines):
    """
    Priority-aware, headroom-reserving scheduler.

    Key behaviors:
      - Enqueue new pipelines by priority (FIFO).
      - For each pool, repeatedly try to assign ready operators:
          * Query first, then Interactive, then Batch.
          * Batch can only use resources beyond reserved headroom when high-priority backlog exists.
          * Pool 0 is treated as a "latency pool": if any high-priority backlog exists, don't schedule batch there.
      - Keep at most one operator per pipeline per tick (reduces over-parallelization surprises).
    """
    # Local fallback in case ASSIGNABLE_STATES isn't in the environment for some reason.
    try:
        assignable_states = ASSIGNABLE_STATES
    except NameError:
        assignable_states = [OperatorState.PENDING, OperatorState.FAILED]

    def _is_high(pri):
        return pri in (Priority.QUERY, Priority.INTERACTIVE)

    def _cleanup_pipeline(pipeline_id):
        # Remove meta; queue cleanup happens lazily as we pop entries.
        if pipeline_id in s.pipeline_meta:
            del s.pipeline_meta[pipeline_id]

    def _ensure_meta(p):
        m = s.pipeline_meta.get(p.pipeline_id)
        if m is None:
            m = {
                "arrival_tick": s.tick,
                "last_failed_count": 0,
                "drop": False,
            }
            s.pipeline_meta[p.pipeline_id] = m
        return m

    def _rotate_pick_ready(pri, scheduled_pipeline_ids):
        """
        Pop/rotate within a single priority queue until we find a pipeline with a ready operator.
        Always re-enqueues pipelines that are still alive so FIFO fairness is maintained within the class.
        Returns (pipeline, op_list) or (None, None).
        """
        q = s.waiting_queues.get(pri, [])
        if not q:
            return None, None

        tries = len(q)
        for _ in range(tries):
            p = q.pop(0)

            # Completed pipelines are dropped.
            status = p.runtime_status()
            if status.is_pipeline_successful():
                _cleanup_pipeline(p.pipeline_id)
                continue

            meta = _ensure_meta(p)

            # If we ever decide to drop (e.g., due to repeated non-OOM failures), do it here.
            # Currently conservative: we do not auto-drop, but we keep the hook.
            if meta.get("drop", False):
                _cleanup_pipeline(p.pipeline_id)
                continue

            # Avoid scheduling multiple operators from the same pipeline in one tick.
            if p.pipeline_id in scheduled_pipeline_ids:
                q.append(p)
                continue

            # Only schedule operators whose parents are complete.
            op_list = status.get_ops(assignable_states, require_parents_complete=True)[:1]
            if not op_list:
                q.append(p)
                continue

            # Keep pipeline queued for its remaining operators (or retries) after this op is scheduled.
            q.append(p)
            return p, op_list

        return None, None

    def _compute_reserve(pool, pool_id, high_backlog):
        # Only enforce reserves when there is high-priority backlog; otherwise let batch fully utilize.
        if not high_backlog:
            return 0, 0
        frac = s.reserve_frac_pool0 if pool_id == 0 else s.reserve_frac_other
        return pool.max_cpu_pool * frac, pool.max_ram_pool * frac

    def _size_for_priority(pri, pool, eff_cpu, eff_ram):
        if eff_cpu < s.min_cpu or eff_ram < s.min_ram:
            return None, None

        cpu = min(eff_cpu, max(s.min_cpu, s.cpu_cap.get(pri, s.min_cpu)))
        ram_target = pool.max_ram_pool * s.ram_target_frac.get(pri, 0.25)
        ram_cap = pool.max_ram_pool * s.ram_cap_frac.get(pri, 0.60)
        ram = min(eff_ram, max(s.min_ram, min(ram_target, ram_cap)))

        # Safety: if we can't meet minimums, don't schedule.
        if cpu < s.min_cpu or ram < s.min_ram:
            return None, None
        return cpu, ram

    # Advance scheduler time.
    s.tick += 1

    # Enqueue new pipelines into per-priority queues.
    for p in pipelines:
        _ensure_meta(p)
        # Default to batch if an unknown priority appears.
        pri = p.priority if p.priority in s.waiting_queues else Priority.BATCH_PIPELINE
        s.waiting_queues[pri].append(p)

    # If nothing new happened, do nothing.
    if not pipelines and not results:
        return [], []

    # Track whether we have any high-priority backlog (used for enforcing headroom reservations).
    high_backlog = len(s.waiting_queues[Priority.QUERY]) + len(s.waiting_queues[Priority.INTERACTIVE]) > 0

    suspensions = []
    assignments = []
    scheduled_pipeline_ids = set()

    # Prefer pool 0 for latency-sensitive work by iterating pools in order (0 first).
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool

        if avail_cpu < s.min_cpu or avail_ram < s.min_ram:
            continue

        if pool_id not in s.pool_high_counter:
            s.pool_high_counter[pool_id] = 0

        reserve_cpu, reserve_ram = _compute_reserve(pool, pool_id, high_backlog)

        # On pool 0 (latency pool), don't start batch when there is any high backlog.
        allow_batch_on_this_pool = not (pool_id == 0 and high_backlog)

        per_pool_assignments = 0
        while per_pool_assignments < s.max_assignments_per_pool:
            # Stop if we can't meet minimums for any class.
            if avail_cpu < s.min_cpu or avail_ram < s.min_ram:
                break

            # Batch fairness on non-latency pools: after a few high-priority starts,
            # allow one batch start if there is batch work and resources beyond reserve.
            batch_turn = False
            if allow_batch_on_this_pool and s.pool_high_counter[pool_id] >= s.high_quota_before_batch:
                eff_cpu_for_batch = max(0, avail_cpu - reserve_cpu)
                eff_ram_for_batch = max(0, avail_ram - reserve_ram)
                if (
                    s.waiting_queues[Priority.BATCH_PIPELINE]
                    and eff_cpu_for_batch >= s.min_cpu
                    and eff_ram_for_batch >= s.min_ram
                ):
                    batch_turn = True

            # Priority order (query > interactive > batch), with optional batch turn.
            if batch_turn:
                pri_order = [Priority.BATCH_PIPELINE, Priority.QUERY, Priority.INTERACTIVE]
            else:
                pri_order = [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]

            picked = False
            for pri in pri_order:
                if pri == Priority.BATCH_PIPELINE and not allow_batch_on_this_pool:
                    continue

                p, op_list = _rotate_pick_ready(pri, scheduled_pipeline_ids)
                if p is None:
                    continue

                # Enforce headroom reservation for batch when high backlog exists:
                # batch can only use resources beyond the reserve.
                if pri == Priority.BATCH_PIPELINE:
                    eff_cpu = max(0, avail_cpu - reserve_cpu)
                    eff_ram = max(0, avail_ram - reserve_ram)
                else:
                    eff_cpu = avail_cpu
                    eff_ram = avail_ram

                cpu, ram = _size_for_priority(pri, pool, eff_cpu, eff_ram)
                if cpu is None or ram is None:
                    # Can't fit this class right now.
                    continue

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

                # Update local pool availability for subsequent placements in this tick.
                avail_cpu -= cpu
                avail_ram -= ram
                per_pool_assignments += 1

                scheduled_pipeline_ids.add(p.pipeline_id)

                # Update fairness counter.
                if _is_high(pri):
                    s.pool_high_counter[pool_id] += 1
                else:
                    s.pool_high_counter[pool_id] = 0

                picked = True
                break

            if not picked:
                # Nothing schedulable in this pool given current constraints.
                break

    return suspensions, assignments
