# policy_key: scheduler_est_038
# reasoning_effort: low
# model: gpt-5.2-2025-12-11
# llm_cost: 0.041124
# generation_seconds: 36.04
# generated_at: 2026-04-01T00:10:05.838030
@register_scheduler_init(key="scheduler_est_038")
def scheduler_est_038_init(s):
    """
    Priority-aware, estimate-aware greedy packing scheduler.

    Small, obvious improvements over naive FIFO:
      - Use pipeline priority to order admissions (QUERY > INTERACTIVE > BATCH).
      - Avoid head-of-line blocking by scanning for any runnable op that fits.
      - Use op.estimate.mem_peak_gb as a RAM floor to reduce OOMs; retry with more RAM.
      - Limit batch "greed" when high-priority work is waiting (soft reservation),
        improving tail latency for interactive/query.
    """
    s.waiting_queue = []          # list[Pipeline]
    s._arrival_seq = 0            # monotonic counter for FIFO within same priority
    s._pipe_meta = {}             # pipeline_id -> dict(seq=int, retry=int)
    s._last_tick_had_work = False


@register_scheduler(key="scheduler_est_038")
def scheduler_est_038(s, results, pipelines):
    """
    Policy details:
      - Enqueue new pipelines with an arrival sequence.
      - Update per-pipeline retry counters on failures (assumed often OOM-related).
      - For each pool, greedily assign as many runnable ops as fit.
      - Select next pipeline by (priority_rank, arrival_seq) while skipping those with
        no runnable ops or whose next runnable op does not fit current headroom.
      - Allocate CPU with a priority bias (high priority gets more per-op CPU).
      - Allocate RAM using estimate floor * headroom * retry_multiplier.
      - Soft-reserve capacity for high-priority by restricting batch allocations when
        any high-priority pipeline is waiting.
    """
    # Enqueue new pipelines
    for p in pipelines:
        pid = p.pipeline_id
        if pid not in s._pipe_meta:
            s._pipe_meta[pid] = {"seq": s._arrival_seq, "retry": 0}
            s._arrival_seq += 1
        s.waiting_queue.append(p)

    # Update retry state from results (simple: any failure increases retry for that pipeline)
    # We do not have explicit OOM signals, so treat failures as "needs more headroom".
    for r in results:
        if getattr(r, "pipeline_id", None) is not None:
            pid = r.pipeline_id
        else:
            # Fall back: results don't expose pipeline_id in provided API; infer from ops if possible.
            # If we cannot infer, skip.
            pid = None
            try:
                if r.ops:
                    pid = getattr(r.ops[0], "pipeline_id", None)
            except Exception:
                pid = None

        if pid is not None and pid in s._pipe_meta and r.failed():
            s._pipe_meta[pid]["retry"] = min(s._pipe_meta[pid]["retry"] + 1, 6)

    # Early exit if nothing changed (helps simulator speed)
    if not pipelines and not results and not s._last_tick_had_work:
        return [], []

    # Helper: priority ranking (lower is better)
    def _prio_rank(prio):
        # Expect: Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE
        if prio == Priority.QUERY:
            return 0
        if prio == Priority.INTERACTIVE:
            return 1
        return 2

    # Prune completed/failed pipelines and refresh waiting list
    refreshed = []
    for p in s.waiting_queue:
        st = p.runtime_status()
        if st.is_pipeline_successful():
            continue
        # If any operator is FAILED, we keep the pipeline in queue to allow retries;
        # the simulator's ASSIGNABLE_STATES includes FAILED so it can be rescheduled.
        refreshed.append(p)
    s.waiting_queue = refreshed

    # Determine if any high-priority work is waiting (for soft reservation)
    any_high_waiting = False
    for p in s.waiting_queue:
        if _prio_rank(p.priority) <= 1:
            st = p.runtime_status()
            ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
            if ops:
                any_high_waiting = True
                break

    # Sort waiting pipelines by priority then arrival order (stable)
    def _pipe_sort_key(p):
        meta = s._pipe_meta.get(p.pipeline_id, {"seq": 0, "retry": 0})
        return (_prio_rank(p.priority), meta["seq"])

    s.waiting_queue.sort(key=_pipe_sort_key)

    suspensions = []
    assignments = []

    # Greedy packing per pool
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool

        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # Soft reservation when high-priority exists:
        # keep some fraction of CPU/RAM available by constraining batch allocations.
        # (We cannot truly reserve without preemption; this simply avoids consuming all headroom.)
        if any_high_waiting:
            batch_cpu_cap = max(1.0, pool.max_cpu_pool * 0.35)
            batch_ram_cap = max(0.5, pool.max_ram_pool * 0.50)
        else:
            batch_cpu_cap = pool.max_cpu_pool
            batch_ram_cap = pool.max_ram_pool

        # Iterate and place multiple ops while resources remain.
        # To avoid O(n^2) runaway, limit scans per pool per tick.
        max_scans = max(20, 4 * len(s.waiting_queue))
        scans = 0

        while avail_cpu > 0 and avail_ram > 0 and scans < max_scans:
            scans += 1
            placed = False

            # Scan pipelines in priority order; pick the first runnable op that fits.
            for p in s.waiting_queue:
                st = p.runtime_status()

                # Candidate ops that are ready to run now
                op_list = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
                if not op_list:
                    continue

                op = op_list[0]
                meta = s._pipe_meta.get(p.pipeline_id, {"seq": 0, "retry": 0})
                retry = meta.get("retry", 0)

                # RAM sizing:
                # - Use estimate as floor (if provided).
                # - Add headroom.
                # - Increase on retries.
                est = None
                try:
                    est = getattr(getattr(op, "estimate", None), "mem_peak_gb", None)
                except Exception:
                    est = None

                # Minimum non-zero RAM to avoid pathological 0 allocations
                base_floor = 0.25
                if est is None:
                    est_floor = base_floor
                else:
                    # guard against bad estimates
                    est_floor = max(base_floor, float(est))

                headroom = 1.20
                retry_mult = 1.0 + 0.60 * retry
                target_ram = est_floor * headroom * retry_mult

                # CPU sizing: bias high priority to reduce latency; batch is capped under contention.
                if p.priority == Priority.QUERY:
                    cpu_cap = max(1.0, pool.max_cpu_pool * 0.85)
                elif p.priority == Priority.INTERACTIVE:
                    cpu_cap = max(1.0, pool.max_cpu_pool * 0.60)
                else:
                    cpu_cap = max(1.0, batch_cpu_cap)

                # Apply per-class RAM cap when reserving for high priority
                if p.priority == Priority.BATCH_PIPELINE and any_high_waiting:
                    target_ram = min(target_ram, batch_ram_cap)

                # Fit to available resources
                cpu = min(avail_cpu, cpu_cap)
                # If CPU becomes too tiny, skip (prevents thrash with fractional crumbs)
                if cpu < 0.5:
                    continue

                ram = min(avail_ram, target_ram)

                # Must fit at least the estimated floor; otherwise skip to avoid likely OOM.
                if ram + 1e-9 < est_floor:
                    continue

                # Create assignment
                assignment = Assignment(
                    ops=op_list,
                    cpu=cpu,
                    ram=ram,
                    priority=p.priority,
                    pool_id=pool_id,
                    pipeline_id=p.pipeline_id,
                )
                assignments.append(assignment)

                # Update remaining headroom optimistically for packing this tick
                avail_cpu -= cpu
                avail_ram -= ram

                placed = True
                break  # rescan from highest priority with updated headroom

            if not placed:
                break

    s._last_tick_had_work = bool(assignments) or bool(s.waiting_queue)
    return suspensions, assignments
