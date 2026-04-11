# policy_key: scheduler_est_040
# reasoning_effort: low
# exp: estimation
# model: gpt-5.2-2025-12-11
# llm_cost: 0.049812
# generation_seconds: 67.44
# generated_at: 2026-04-10T10:18:13.923573
@register_scheduler_init(key="scheduler_est_040")
def scheduler_est_040_init(s):
    """
    Memory-aware, priority-first scheduler with gentle anti-starvation and OOM backoff.

    Core ideas:
      - Prefer QUERY then INTERACTIVE then BATCH, but allow aging to promote long-waiting pipelines.
      - Use op.estimate.mem_peak_gb (when present) to:
          * pre-filter infeasible placements (avoid obvious OOMs),
          * right-size RAM requests with a safety factor.
      - On failures, apply per-operator RAM backoff (increase future RAM requests) to improve completion rate.
      - Avoid over-allocating CPU to a single op to keep concurrency and reduce head-of-line blocking.
    """
    # Queues store pipeline_ids; pipelines themselves are stored in s.pipelines_by_id.
    s.q_query = []
    s.q_interactive = []
    s.q_batch = []

    s.pipelines_by_id = {}          # pipeline_id -> Pipeline
    s.pipeline_wait_cycles = {}     # pipeline_id -> int scheduler cycles since arrival/enqueued

    # Per-operator backoff to reduce repeat failures (keyed by object identity).
    s.op_ram_backoff = {}           # id(op) -> float multiplier (>=1.0)

    # Light per-tick fairness: try not to starve batch indefinitely.
    s._tick = 0


@register_scheduler(key="scheduler_est_040")
def scheduler_est_040_scheduler(s, results, pipelines):
    def _prio_weight(prio):
        if prio == Priority.QUERY:
            return 10
        if prio == Priority.INTERACTIVE:
            return 5
        return 1

    def _base_rank(prio):
        # lower is better
        if prio == Priority.QUERY:
            return 0
        if prio == Priority.INTERACTIVE:
            return 1
        return 2

    def _queue_for_priority(prio):
        if prio == Priority.QUERY:
            return s.q_query
        if prio == Priority.INTERACTIVE:
            return s.q_interactive
        return s.q_batch

    def _is_pipeline_done_or_failed(p):
        st = p.runtime_status()
        # If any operator failed, we consider it failed; we do not retry whole pipeline here.
        # (The simulator will mark latency penalties; scheduler's job is to avoid failures.)
        has_failures = st.state_counts[OperatorState.FAILED] > 0
        return st.is_pipeline_successful() or has_failures

    def _pick_assignable_op(p):
        st = p.runtime_status()
        op_list = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        if not op_list:
            return None
        # Prefer smaller estimated-memory ops first to keep headroom and reduce OOM cascades.
        # Use 0.0 when estimate missing to avoid blocking; we'll still size conservatively later.
        def est_mem(op):
            em = None
            if getattr(op, "estimate", None) is not None:
                em = getattr(op.estimate, "mem_peak_gb", None)
            return float(em) if em is not None else 0.0

        op_list.sort(key=lambda op: (est_mem(op), id(op)))
        return op_list[0]

    def _effective_rank(p):
        # Aging-based promotion to avoid starvation (in cycles, not seconds).
        age = s.pipeline_wait_cycles.get(p.pipeline_id, 0)
        rank = _base_rank(p.priority)

        # Promote very old work gradually:
        # - INTERACTIVE can promote to QUERY if extremely old.
        # - BATCH can promote to INTERACTIVE, then QUERY if extremely old.
        if p.priority == Priority.INTERACTIVE and age >= 250:
            rank = 0
        if p.priority == Priority.BATCH_PIPELINE:
            if age >= 150:
                rank = min(rank, 1)
            if age >= 350:
                rank = 0
        return rank, -age  # older first within same effective rank

    def _get_est_mem_gb(op):
        if getattr(op, "estimate", None) is None:
            return None
        return getattr(op.estimate, "mem_peak_gb", None)

    def _ram_request_gb(op, pool, prio):
        # Start from estimate if available, otherwise be conservative.
        est = _get_est_mem_gb(op)
        # Safety factors by priority: higher priority gets a bit more headroom to avoid costly failures.
        if prio == Priority.QUERY:
            safety = 1.35
        elif prio == Priority.INTERACTIVE:
            safety = 1.40
        else:
            safety = 1.25

        # Apply per-operator backoff after failures (assume likely OOM).
        backoff = s.op_ram_backoff.get(id(op), 1.0)

        if est is None:
            # With no estimate, request a moderate fraction of pool to avoid tiny allocations that OOM.
            base = max(0.25 * pool.max_ram_pool, 1.0)
        else:
            base = max(est * safety, 0.5)

        req = base * backoff

        # Clamp to pool bounds.
        req = max(0.5, min(req, pool.max_ram_pool))
        return req

    def _cpu_request_units(pool, prio):
        avail = pool.avail_cpu_pool
        if avail <= 0:
            return 0

        # Avoid taking the entire pool for one op; keep concurrency to reduce queueing latency.
        if prio == Priority.QUERY:
            frac = 0.75
        elif prio == Priority.INTERACTIVE:
            frac = 0.65
        else:
            frac = 0.50

        cpu = max(1, int(avail * frac))
        cpu = min(cpu, int(pool.max_cpu_pool), int(avail))
        return cpu

    def _fits_pool(op, p, pool):
        # Quick memory feasibility using estimate when available.
        est = _get_est_mem_gb(op)
        if est is not None:
            # Add a minimal cushion to account for estimator noise (but don't be overly strict).
            if est * 1.10 > pool.avail_ram_pool:
                return False

        req_ram = _ram_request_gb(op, pool, p.priority)
        if req_ram > pool.avail_ram_pool:
            return False

        req_cpu = _cpu_request_units(pool, p.priority)
        if req_cpu <= 0:
            return False

        return True

    # --- Ingest new pipelines into queues and state ---
    for p in pipelines:
        s.pipelines_by_id[p.pipeline_id] = p
        s.pipeline_wait_cycles[p.pipeline_id] = 0
        _queue_for_priority(p.priority).append(p.pipeline_id)

    # --- Process results to adjust backoff on failures ---
    for r in results:
        if hasattr(r, "failed") and r.failed():
            # Treat all failures as potential memory under-provisioning; increase RAM backoff.
            # Use stronger backoff for higher priority to avoid repeated 720s penalties.
            prio = getattr(r, "priority", Priority.BATCH_PIPELINE)
            if prio == Priority.QUERY:
                mult = 1.6
            elif prio == Priority.INTERACTIVE:
                mult = 1.7
            else:
                mult = 1.5

            for op in getattr(r, "ops", []) or []:
                k = id(op)
                prev = s.op_ram_backoff.get(k, 1.0)
                s.op_ram_backoff[k] = min(prev * mult, 8.0)

    # Early exit if nothing changed and no new arrivals
    if not pipelines and not results:
        return [], []

    s._tick += 1

    # Increment waiting age for all queued pipelines.
    for q in (s.q_query, s.q_interactive, s.q_batch):
        for pid in q:
            s.pipeline_wait_cycles[pid] = s.pipeline_wait_cycles.get(pid, 0) + 1

    suspensions = []
    assignments = []

    # A tiny fairness knob: ensure we *try* to schedule at least one batch op per tick (if runnable and fits).
    batch_budget = 1

    def _pop_candidate_from_queue(q, pool):
        """
        Rotate through a queue once, pick first pipeline with an assignable op that fits this pool.
        Returns (pipeline, op) or (None, None). Keeps queue order mostly stable.
        """
        n = len(q)
        if n == 0:
            return None, None

        requeue = []
        picked = None
        picked_op = None

        for _ in range(n):
            pid = q.pop(0)
            p = s.pipelines_by_id.get(pid, None)
            if p is None:
                continue
            if _is_pipeline_done_or_failed(p):
                # Drop from queues/state (no retries here).
                s.pipeline_wait_cycles.pop(pid, None)
                continue

            op = _pick_assignable_op(p)
            if op is not None and _fits_pool(op, p, pool) and picked is None:
                picked = p
                picked_op = op
                # Do not requeue picked pipeline immediately; it will be requeued after assignment attempt.
                continue

            requeue.append(pid)

        # Restore the queue with those not picked, preserving their relative order.
        q.extend(requeue)
        return picked, picked_op

    # Pool-by-pool packing: assign multiple ops per pool as resources allow.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]

        # Try multiple placements per pool in the same tick to improve throughput, without over-packing.
        # Cap attempts to avoid long loops when nothing fits.
        max_attempts = 6
        attempts = 0

        while attempts < max_attempts:
            attempts += 1
            if pool.avail_cpu_pool <= 0 or pool.avail_ram_pool <= 0:
                break

            # Build a small ordered list of queues to consult:
            # Prefer higher effective priority, but allow one batch per tick if possible.
            queue_order = []

            # Compute which priority class should be considered first using effective rank + aging.
            # We approximate by peeking the oldest item from each queue.
            candidates = []
            for q in (s.q_query, s.q_interactive, s.q_batch):
                if not q:
                    continue
                pid = q[0]
                p = s.pipelines_by_id.get(pid, None)
                if p is None:
                    continue
                candidates.append((*_effective_rank(p), _prio_weight(p.priority), q))

            # Default order: by effective rank, then by weight (higher weight first).
            candidates.sort(key=lambda t: (t[0], t[1] * -1, t[2]))

            # If batch budget remains, insert batch queue early but not ahead of query.
            if batch_budget > 0 and s.q_batch:
                # Place batch after query (if any), otherwise at front.
                qlist = [c[-1] for c in candidates]
                if s.q_query in qlist:
                    idx = qlist.index(s.q_query) + 1
                    queue_order = qlist[:idx] + ([s.q_batch] if s.q_batch not in qlist else []) + qlist[idx:]
                else:
                    queue_order = ([s.q_batch] if s.q_batch not in qlist else []) + qlist
            else:
                queue_order = [c[-1] for c in candidates]

            if not queue_order:
                break

            chosen_p = None
            chosen_op = None
            chosen_q = None

            # Search queues in order for an op that fits this pool.
            for q in queue_order:
                p, op = _pop_candidate_from_queue(q, pool)
                if p is not None:
                    chosen_p, chosen_op, chosen_q = p, op, q
                    break

            if chosen_p is None:
                break  # nothing fits this pool right now

            # Create assignment.
            cpu = _cpu_request_units(pool, chosen_p.priority)
            ram = _ram_request_gb(chosen_op, pool, chosen_p.priority)

            # Final check (resources may have changed slightly due to earlier assignments in this loop).
            if cpu <= 0 or ram > pool.avail_ram_pool or cpu > pool.avail_cpu_pool:
                # Requeue pipeline to its original queue and stop trying in this pool.
                _queue_for_priority(chosen_p.priority).append(chosen_p.pipeline_id)
                break

            assignments.append(
                Assignment(
                    ops=[chosen_op],
                    cpu=cpu,
                    ram=ram,
                    priority=chosen_p.priority,
                    pool_id=pool_id,
                    pipeline_id=chosen_p.pipeline_id,
                )
            )

            # Requeue the pipeline so its next operators can be scheduled later.
            _queue_for_priority(chosen_p.priority).append(chosen_p.pipeline_id)

            if chosen_p.priority == Priority.BATCH_PIPELINE and batch_budget > 0:
                batch_budget -= 1

    return suspensions, assignments
