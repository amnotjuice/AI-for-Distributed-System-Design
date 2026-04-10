# policy_key: scheduler_est_036
# reasoning_effort: high
# model: gpt-5.2-2025-12-11
# llm_cost: 0.099602
# generation_seconds: 159.52
# generated_at: 2026-03-31T19:30:44.310668
@register_scheduler_init(key="scheduler_est_036")
def scheduler_est_036_init(s):
    """
    Priority + estimate-aware scheduler (small, safe improvements over naive FIFO).

    Improvements vs. naive:
      - Separate queues by priority; strict priority order (QUERY > INTERACTIVE > BATCH).
      - Avoid head-of-line blocking by scanning within each priority queue for ready ops.
      - Right-size RAM using op.estimate.mem_peak_gb with an OOM-driven backoff.
      - Avoid letting BATCH consume the last shared resources when high-priority backlog exists
        (soft reservation on non-dedicated pools).
      - Allow multiple assignments per pool per tick (until resources are exhausted).
    """
    # Persistent queues (round-robin) per priority
    s.queues = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }
    s.known_pipeline_ids = set()

    # Failure/oom tracking per operator instance (best-effort keys)
    s.fail_counts = {}        # op_key -> int
    s.oom_counts = {}         # op_key -> int
    s.cooldown_until = {}     # op_key -> tick when we may retry

    # Configuration knobs (kept intentionally simple)
    s.tick = 0
    s.max_assignments_per_pool = 32
    s.queue_scan_limit = 32

    s.min_cpu = 1.0
    s.min_ram_gb = 0.5

    # Memory sizing: requested = estimate * (base_safety + oom_count * oom_backoff)
    s.mem_base_safety = 1.25
    s.mem_oom_backoff = 0.75

    # Retry policy
    s.max_fail_retries = 3       # for non-OOM-ish failures
    s.max_oom_retries = 6        # allow more retries for memory tuning

    # Soft reservation on shared pools when high-priority backlog exists
    s.shared_pool_reserve_frac_cpu = 0.25
    s.shared_pool_reserve_frac_ram = 0.25

    # CPU fractions by priority (cap per-op CPU to encourage concurrency)
    s.cpu_frac_by_prio = {
        Priority.QUERY: 0.50,
        Priority.INTERACTIVE: 0.33,
        Priority.BATCH_PIPELINE: 0.25,
    }

    # Soft RAM caps by priority (do not enforce if op's estimate is larger)
    s.ram_cap_frac_by_prio = {
        Priority.QUERY: 0.90,
        Priority.INTERACTIVE: 0.75,
        Priority.BATCH_PIPELINE: 0.60,
    }


@register_scheduler(key="scheduler_est_036")
def scheduler_est_036(s, results: List[ExecutionResult], pipelines: List[Pipeline]) -> Tuple[List[Suspend], List[Assignment]]:
    # -----------------------------
    # Helpers (local, no imports)
    # -----------------------------
    def _prio(p: Pipeline):
        # Default unknown priority to batch-like behavior
        return getattr(p, "priority", Priority.BATCH_PIPELINE)

    def _op_key(op):
        # Best-effort stable key for an operator instance
        oid = getattr(op, "op_id", None)
        if oid is not None:
            return ("op_id", oid)
        return ("py_id", id(op))

    def _is_oom_error(err):
        if not err:
            return False
        e = str(err).lower()
        return ("oom" in e) or ("out of memory" in e) or ("out-of-memory" in e) or ("memory" in e and "alloc" in e)

    def _mem_estimate_gb(op):
        est = getattr(op, "estimate", None)
        v = getattr(est, "mem_peak_gb", None) if est is not None else None
        try:
            if v is None:
                return None
            v = float(v)
            if v <= 0:
                return None
            return v
        except Exception:
            return None

    def _pipeline_should_drop(p: Pipeline):
        # Drop pipelines that have FAILED ops exceeding retry limits to prevent infinite churn.
        st = p.runtime_status()
        failed_ops = st.get_ops([OperatorState.FAILED], require_parents_complete=False)
        for op in failed_ops:
            k = _op_key(op)
            fails = s.fail_counts.get(k, 0)
            ooms = s.oom_counts.get(k, 0)
            limit = s.max_oom_retries if ooms > 0 else s.max_fail_retries
            if fails >= limit:
                return True
        return False

    def _queue_has_ready_work(queue, scan=8):
        # Check if any pipeline in the queue has an assignable op ready (bounded scan).
        n = min(len(queue), scan)
        for i in range(n):
            p = queue[i]
            st = p.runtime_status()
            if st.is_pipeline_successful():
                continue
            if _pipeline_should_drop(p):
                continue
            ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
            if not ops:
                continue
            op = ops[0]
            k = _op_key(op)
            # If cooling down after a failure, consider it not ready for now.
            if st is not None and getattr(op, "state", None) == OperatorState.FAILED:
                if s.cooldown_until.get(k, -1) > s.tick:
                    continue
            # Also block retry if cooldown is active even if state is FAILED but op object doesn't expose .state
            if s.cooldown_until.get(k, -1) > s.tick and op in st.get_ops([OperatorState.FAILED], require_parents_complete=False):
                continue
            return True
        return False

    def _dequeue_next_ready(queue, prio):
        """
        Round-robin scan: rotate queue until we find a pipeline with a ready assignable op.
        Returns (pipeline, op) or (None, None).
        """
        scans = min(len(queue), s.queue_scan_limit)
        for _ in range(scans):
            p = queue.pop(0)
            st = p.runtime_status()

            # Drop completed / doomed pipelines
            if st.is_pipeline_successful() or _pipeline_should_drop(p):
                continue

            ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
            if not ops:
                # Not ready; keep it in the queue
                queue.append(p)
                continue

            op = ops[0]
            k = _op_key(op)

            # Simple cooldown to avoid immediate retry thrash after failures.
            if s.cooldown_until.get(k, -1) > s.tick:
                queue.append(p)
                continue

            # Keep pipeline in queue for future ops (end of queue round-robin)
            queue.append(p)
            return p, op

        return None, None

    def _cpu_request(prio, pool, avail_cpu):
        if avail_cpu < s.min_cpu:
            return None
        frac = s.cpu_frac_by_prio.get(prio, 0.25)
        target = max(s.min_cpu, pool.max_cpu_pool * frac)
        return min(avail_cpu, target)

    def _ram_request(prio, pool, avail_ram, op):
        if avail_ram < s.min_ram_gb:
            return None

        est = _mem_estimate_gb(op)

        # If no estimate, pick a conservative-but-not-tiny default:
        # - Let high priority take more (reduce latency), batch takes less (reduce interference).
        if est is None:
            base = 1.0 if prio == Priority.BATCH_PIPELINE else 2.0
            est = min(max(base, s.min_ram_gb), pool.max_ram_pool)

        k = _op_key(op)
        ooms = s.oom_counts.get(k, 0)
        safety = s.mem_base_safety + (ooms * s.mem_oom_backoff)

        desired = max(s.min_ram_gb, est * safety)

        # Soft cap per priority to preserve concurrency, but never below the base estimate.
        cap_frac = s.ram_cap_frac_by_prio.get(prio, 0.7)
        soft_cap = pool.max_ram_pool * cap_frac

        if est <= soft_cap:
            desired = min(desired, soft_cap)

        # If we can't meet a reasonable fraction of the estimate, it's likely to OOM -> wait.
        # (More strict for batch; more permissive for high priority).
        min_frac = 0.90 if prio == Priority.BATCH_PIPELINE else 0.80
        if avail_ram < est * min_frac:
            return None

        return min(avail_ram, desired)

    # -----------------------------
    # Update state with new arrivals
    # -----------------------------
    for p in pipelines:
        pid = getattr(p, "pipeline_id", None)
        # Avoid double-enqueue if generator repeats references
        if pid is not None and pid in s.known_pipeline_ids:
            continue
        if pid is not None:
            s.known_pipeline_ids.add(pid)
        s.queues.setdefault(_prio(p), s.queues[Priority.BATCH_PIPELINE]).append(p)

    # -----------------------------
    # Consume results (failure tracking)
    # -----------------------------
    s.tick += 1
    if results:
        for r in results:
            if not r.failed():
                continue
            is_oom = _is_oom_error(getattr(r, "error", None))
            for op in getattr(r, "ops", []) or []:
                k = _op_key(op)
                s.fail_counts[k] = s.fail_counts.get(k, 0) + 1
                if is_oom:
                    s.oom_counts[k] = s.oom_counts.get(k, 0) + 1
                # Cooldown one tick to avoid immediate reschedule thrash
                s.cooldown_until[k] = s.tick + 1

    # Early exit if nothing changed (but keep deterministic tick increment above)
    if not pipelines and not results:
        return [], []

    # -----------------------------
    # Scheduling decisions
    # -----------------------------
    suspensions: List[Suspend] = []
    assignments: List[Assignment] = []

    # Determine if there is high-priority backlog with *ready* work (bounded scan).
    high_ready = _queue_has_ready_work(s.queues[Priority.QUERY]) or _queue_has_ready_work(s.queues[Priority.INTERACTIVE])

    # If multiple pools exist, treat pool 0 as "preferred" for high-priority (not exclusive).
    num_pools = s.executor.num_pools

    # Always consider priorities in strict order for latency.
    priority_order = [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]

    for pool_id in range(num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool
        if avail_cpu < s.min_cpu or avail_ram < s.min_ram_gb:
            continue

        is_high_preferred_pool = (num_pools >= 2 and pool_id == 0)

        # On non-preferred pools, keep some headroom when high-priority work is waiting.
        reserve_cpu = 0.0
        reserve_ram = 0.0
        if high_ready and (not is_high_preferred_pool):
            reserve_cpu = pool.max_cpu_pool * s.shared_pool_reserve_frac_cpu
            reserve_ram = pool.max_ram_pool * s.shared_pool_reserve_frac_ram

        made = 0
        while made < s.max_assignments_per_pool:
            if avail_cpu < s.min_cpu or avail_ram < s.min_ram_gb:
                break

            picked = False

            for pr in priority_order:
                q = s.queues.get(pr, [])

                # If high-priority work is ready, don't let batch chew into reserved headroom.
                if pr == Priority.BATCH_PIPELINE and high_ready and (not is_high_preferred_pool):
                    if avail_cpu <= reserve_cpu or avail_ram <= reserve_ram:
                        continue

                p, op = _dequeue_next_ready(q, pr)
                if p is None or op is None:
                    continue

                # Compute effective availability for batch so it cannot allocate into the reserve.
                eff_avail_cpu = avail_cpu
                eff_avail_ram = avail_ram
                if pr == Priority.BATCH_PIPELINE and high_ready and (not is_high_preferred_pool):
                    eff_avail_cpu = max(0.0, avail_cpu - reserve_cpu)
                    eff_avail_ram = max(0.0, avail_ram - reserve_ram)

                cpu_req = _cpu_request(pr, pool, eff_avail_cpu)
                ram_req = _ram_request(pr, pool, eff_avail_ram, op)

                # If this op doesn't fit right now, skip and try other pipelines/priors.
                if cpu_req is None or ram_req is None:
                    continue

                # Final guard (should be redundant)
                if cpu_req > avail_cpu or ram_req > avail_ram:
                    continue

                assignments.append(
                    Assignment(
                        ops=[op],
                        cpu=cpu_req,
                        ram=ram_req,
                        priority=p.priority,
                        pool_id=pool_id,
                        pipeline_id=p.pipeline_id,
                    )
                )
                avail_cpu -= cpu_req
                avail_ram -= ram_req
                made += 1
                picked = True
                break  # re-evaluate priorities after each placement

            if not picked:
                break

    return suspensions, assignments
