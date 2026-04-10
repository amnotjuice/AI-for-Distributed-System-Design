# policy_key: scheduler_medium_022
# reasoning_effort: medium
# exp: reasoning
# model: gpt-5.2-2025-12-11
# llm_cost: 0.108455
# generation_seconds: 78.19
# generated_at: 2026-04-09T22:58:53.457839
@register_scheduler_init(key="scheduler_medium_022")
def scheduler_medium_022_init(s):
    """Priority-aware, failure-averse scheduler.

    Core ideas:
      - Strictly prioritize QUERY then INTERACTIVE then BATCH to minimize weighted latency.
      - Be conservative with RAM to reduce OOMs (OOMs are very costly via 720s penalty).
      - Adaptive per-pipeline RAM multiplier: on OOM, retry with more RAM; on success, decay slowly.
      - Simple fairness: batch still runs when capacity exists; avoid duplicate enqueues and drop "hopeless" pipelines after repeated non-OOM failures.
      - Multi-pool hint: if multiple pools, pool 0 is treated as "latency pool" (prefer high priority there).
    """
    from collections import deque

    s.q_query = deque()
    s.q_interactive = deque()
    s.q_batch = deque()

    # Pipeline metadata keyed by pipeline_id
    #   ram_mult: multiplicative safety factor for RAM allocation
    #   cpu_mult: multiplicative factor for CPU allocation (lightly used)
    #   oom_fails: count of OOM failures observed
    #   other_fails: count of non-OOM failures observed
    #   dead: stop scheduling this pipeline (avoid wasting cluster time)
    #   age: increments when pipeline is waiting (used for mild fairness / spillover)
    s.meta = {}

    # Track which pipeline_ids are currently enqueued to avoid duplicates.
    s.enqueued = set()

    # Logical tick counter for mild aging.
    s.tick = 0


@register_scheduler(key="scheduler_medium_022")
def scheduler_medium_022(s, results, pipelines):
    """
    Scheduler step:
      1) Ingest new pipelines into priority queues.
      2) Update per-pipeline metadata using execution results (OOM-aware retries).
      3) Assign ready operators to pools with priority-first ordering and conservative RAM.
    """
    from collections import deque

    def _prio_queue(priority):
        if priority == Priority.QUERY:
            return s.q_query
        if priority == Priority.INTERACTIVE:
            return s.q_interactive
        return s.q_batch

    def _is_oom_error(err):
        if err is None:
            return False
        msg = str(err).lower()
        return ("oom" in msg) or ("out of memory" in msg) or ("memory" in msg and "alloc" in msg)

    def _get_meta(pid):
        m = s.meta.get(pid)
        if m is None:
            m = {
                "ram_mult": 1.0,
                "cpu_mult": 1.0,
                "oom_fails": 0,
                "other_fails": 0,
                "dead": False,
                "age": 0,
            }
            s.meta[pid] = m
        return m

    def _pipeline_inflight(status):
        # Count work already in progress; limit batch concurrency to reduce OOM risk.
        counts = getattr(status, "state_counts", {}) or {}
        return (
            counts.get(OperatorState.RUNNING, 0)
            + counts.get(OperatorState.ASSIGNED, 0)
            + counts.get(OperatorState.SUSPENDING, 0)
        )

    def _pipeline_done_or_dead(p):
        status = p.runtime_status()
        if status.is_pipeline_successful():
            return True
        m = _get_meta(p.pipeline_id)
        if m["dead"]:
            return True
        return False

    def _maybe_mark_dead(p):
        # Avoid infinite retries on systemic errors. OOM is handled via RAM scaling.
        m = _get_meta(p.pipeline_id)
        if m["other_fails"] >= 3:
            m["dead"] = True

    def _has_ready_op(p):
        status = p.runtime_status()
        op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        return len(op_list) > 0

    def _pick_ready_pipeline(q, inflight_cap):
        # Rotate through the queue to find a pipeline with an assignable op.
        # Returns (pipeline, op_list) or (None, None)
        n = len(q)
        for _ in range(n):
            p = q.popleft()
            if _pipeline_done_or_dead(p):
                s.enqueued.discard(p.pipeline_id)
                continue

            status = p.runtime_status()
            if _pipeline_inflight(status) >= inflight_cap:
                # Keep waiting; don't over-parallelize within a pipeline (esp. batch).
                q.append(p)
                continue

            op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
            if op_list:
                return p, op_list[:1]

            # No ready ops right now; keep it in the queue.
            q.append(p)

        return None, None

    def _base_cpu_ram(pool, priority):
        # Conservative RAM to reduce OOMs; CPU biased toward high priority for latency.
        max_cpu = pool.max_cpu_pool
        max_ram = pool.max_ram_pool

        if priority == Priority.QUERY:
            cpu = max(1, int(round(max_cpu * 0.75)))
            ram = max(1, int(round(max_ram * 0.60)))
        elif priority == Priority.INTERACTIVE:
            cpu = max(1, int(round(max_cpu * 0.55)))
            ram = max(1, int(round(max_ram * 0.45)))
        else:
            cpu = max(1, int(round(max_cpu * 0.30)))
            ram = max(1, int(round(max_ram * 0.30)))

        return cpu, ram

    def _apply_reservation(avail_cpu, avail_ram, pool, priority, high_waiting):
        # If there is any high priority waiting, keep a small reserve to reduce
        # their queueing delay. This mainly throttles batch admission.
        if not high_waiting:
            return avail_cpu, avail_ram

        if priority == Priority.BATCH_PIPELINE:
            cpu_reserve = max(0, int(round(pool.max_cpu_pool * 0.20)))
            ram_reserve = max(0, int(round(pool.max_ram_pool * 0.20)))
            return max(0, avail_cpu - cpu_reserve), max(0, avail_ram - ram_reserve)

        return avail_cpu, avail_ram

    def _enqueue_pipeline(p):
        pid = p.pipeline_id
        if pid in s.enqueued:
            return
        _get_meta(pid)  # ensure meta exists
        _prio_queue(p.priority).append(p)
        s.enqueued.add(pid)

    # -----------------------------
    # 1) Ingest new pipelines
    # -----------------------------
    for p in pipelines:
        _enqueue_pipeline(p)

    # -----------------------------
    # 2) Update metadata from results
    # -----------------------------
    s.tick += 1
    if results:
        for r in results:
            # Results reference a pipeline via container/op metadata; we rely on pipeline_id via ops if present.
            # Eudoxia results provide r.ops, which are operators from a pipeline; extract pipeline_id if available.
            # If not available, fall back to doing nothing.
            pid = None
            try:
                # Common patterns: operator has .pipeline_id or .pipeline.pipeline_id
                if r.ops and len(r.ops) > 0:
                    op0 = r.ops[0]
                    if hasattr(op0, "pipeline_id"):
                        pid = op0.pipeline_id
                    elif hasattr(op0, "pipeline") and hasattr(op0.pipeline, "pipeline_id"):
                        pid = op0.pipeline.pipeline_id
            except Exception:
                pid = None

            if pid is None:
                continue

            m = _get_meta(pid)

            if r.failed():
                if _is_oom_error(getattr(r, "error", None)):
                    m["oom_fails"] += 1
                    # Aggressively scale RAM on OOM to protect completion rate.
                    m["ram_mult"] = min(4.0, max(m["ram_mult"] * 1.7, 1.5))
                    # Also slightly increase CPU: sometimes OOM correlates with slower operators holding memory longer.
                    m["cpu_mult"] = min(2.0, max(m["cpu_mult"] * 1.10, 1.0))
                else:
                    m["other_fails"] += 1
                    # Try a small CPU bump for generic failures (may help timeouts in some models).
                    m["cpu_mult"] = min(2.0, max(m["cpu_mult"] * 1.20, 1.0))
                    if m["other_fails"] >= 3:
                        m["dead"] = True
            else:
                # Successful execution: slowly decay RAM multiplier toward 1.0.
                m["ram_mult"] = max(1.0, m["ram_mult"] * 0.95)
                m["cpu_mult"] = max(1.0, m["cpu_mult"] * 0.98)

    # Periodic queue compaction (remove completed/dead pipelines).
    def _compact(q):
        new_q = deque()
        for _ in range(len(q)):
            p = q.popleft()
            if _pipeline_done_or_dead(p):
                s.enqueued.discard(p.pipeline_id)
                continue
            new_q.append(p)
        return new_q

    if pipelines or results:
        s.q_query = _compact(s.q_query)
        s.q_interactive = _compact(s.q_interactive)
        s.q_batch = _compact(s.q_batch)

    # Mild aging: increment age for enqueued pipelines.
    # This is used as a spillover hint, not a strict ordering.
    for q in (s.q_query, s.q_interactive, s.q_batch):
        for p in q:
            _get_meta(p.pipeline_id)["age"] += 1

    # -----------------------------
    # 3) Build assignments
    # -----------------------------
    suspensions = []  # no preemption without reliable container enumeration
    assignments = []

    # Determine whether high priority work is waiting (to reserve some capacity from batch).
    high_waiting = False
    for q in (s.q_query, s.q_interactive):
        for p in q:
            if not _pipeline_done_or_dead(p) and _has_ready_op(p):
                high_waiting = True
                break
        if high_waiting:
            break

    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool
        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # If multiple pools, bias pool 0 toward latency-sensitive work by allowing batch only
        # when there is no high-priority waiting.
        allow_batch_here = True
        if s.executor.num_pools > 1 and pool_id == 0 and high_waiting:
            allow_batch_here = False

        # Greedy packing: schedule as many ready ops as fit.
        # Priority order: QUERY -> INTERACTIVE -> (BATCH if allowed).
        while avail_cpu > 0 and avail_ram > 0:
            made_progress = False

            # (priority, inflight_cap)
            prio_plan = [
                (Priority.QUERY, 2),
                (Priority.INTERACTIVE, 2),
                (Priority.BATCH_PIPELINE, 1),
            ]

            for prio, inflight_cap in prio_plan:
                if prio == Priority.BATCH_PIPELINE and not allow_batch_here:
                    continue

                # Apply reservation for batch when high priority is waiting elsewhere.
                eff_cpu, eff_ram = _apply_reservation(avail_cpu, avail_ram, pool, prio, high_waiting)
                if eff_cpu <= 0 or eff_ram <= 0:
                    continue

                q = _prio_queue(prio)
                p, op_list = _pick_ready_pipeline(q, inflight_cap=inflight_cap)
                if p is None:
                    continue

                # Compute requested resources (conservative RAM; CPU biased to priority).
                m = _get_meta(p.pipeline_id)
                base_cpu, base_ram = _base_cpu_ram(pool, prio)

                cpu_req = int(max(1, round(base_cpu * m["cpu_mult"])))
                ram_req = int(max(1, round(base_ram * m["ram_mult"])))

                # Clamp to effective availability.
                cpu = min(eff_cpu, cpu_req)
                ram = min(eff_ram, ram_req)

                # Ensure at least 1 unit each (otherwise can't schedule).
                if cpu <= 0 or ram <= 0:
                    q.appendleft(p)
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

                # Update available resources (local accounting).
                avail_cpu -= cpu
                avail_ram -= ram

                # Reset age since it made progress.
                m["age"] = 0

                # Keep pipeline in queue for future ops.
                q.append(p)

                made_progress = True
                break  # re-evaluate from highest priority with updated resources

            if not made_progress:
                break

    return suspensions, assignments
