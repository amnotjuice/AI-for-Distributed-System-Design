# policy_key: scheduler_est_050
# reasoning_effort: low
# exp: estimation
# model: gpt-5.2-2025-12-11
# llm_cost: 0.047866
# generation_seconds: 35.82
# generated_at: 2026-04-10T10:25:43.872573
@register_scheduler_init(key="scheduler_est_050")
def scheduler_est_050_init(s):
    """Priority + memory-estimate-aware greedy packing with conservative OOM retries.

    Core ideas:
      - Separate FIFO queues per priority (QUERY > INTERACTIVE > BATCH).
      - Prefer assigning ops that (estimated) fit in a pool to reduce OOM failures.
      - Greedy packing within each pool: highest priority first; within same priority,
        pick smallest estimated memory first to improve utilization and reduce blocking.
      - On failures, retry a limited number of times with increased RAM "boost" to
        improve completion rate (avoids 720s penalty from failed/incomplete pipelines).
      - Soft reservation: avoid consuming the last headroom with batch when higher
        priorities are waiting.
    """
    s.wait_q = []
    s.wait_i = []
    s.wait_b = []

    # Per-pipeline retry state (RAM boost + retry count)
    s.ram_boost = {}   # pipeline_id -> float multiplier
    s.retry_cnt = {}   # pipeline_id -> int

    # Tunables (kept simple / conservative)
    s.max_retries = 3
    s.base_ram_slack_gb = 0.5         # additive slack above estimate
    s.base_ram_mult = 1.20            # multiplicative slack above estimate
    s.oom_ram_boost_mult = 1.60       # multiplier applied after each failure
    s.max_ram_boost = 4.0

    # Soft reservation for high priorities when they are waiting
    s.reserve_cpu_frac = 0.25
    s.reserve_ram_frac = 0.25


def _priority_rank(p):
    # Higher is more important
    if p == Priority.QUERY:
        return 3
    if p == Priority.INTERACTIVE:
        return 2
    return 1  # BATCH_PIPELINE and any others


def _target_cpu_for_priority(pool, priority):
    # Simple right-sizing: query gets more CPU to reduce tail latency.
    # Constrained by pool availability at assignment time.
    max_cpu = getattr(pool, "max_cpu_pool", None)
    if max_cpu is None or max_cpu <= 0:
        return 1

    if priority == Priority.QUERY:
        return max(1, int(round(max_cpu * 1.00)))
    if priority == Priority.INTERACTIVE:
        return max(1, int(round(max_cpu * 0.75)))
    return max(1, int(round(max_cpu * 0.50)))


def _estimated_mem_gb(op):
    est = getattr(op, "estimate", None)
    if est is None:
        return None
    return getattr(est, "mem_peak_gb", None)


def _compute_ram_request_gb(s, pipeline_id, op, pool):
    # Convert estimate into a RAM request with slack + per-pipeline boost.
    est_mem = _estimated_mem_gb(op)
    boost = s.ram_boost.get(pipeline_id, 1.0)

    if est_mem is None:
        # Conservative fallback when no estimate: allocate a moderate slice of the pool.
        # This aims to reduce OOM while still allowing concurrency.
        return max(1.0, float(getattr(pool, "max_ram_pool", 1.0)) * 0.60)

    req = max(0.0, float(est_mem))
    req = req * s.base_ram_mult + s.base_ram_slack_gb
    req = req * boost
    return max(0.5, req)


def _pipeline_done_or_dead(pipeline):
    st = pipeline.runtime_status()
    if st.is_pipeline_successful():
        return True
    # If any operator is FAILED, we consider it "dead" for now, but we may retry
    # by leaving it in queues (scheduler will pick ASSIGNABLE ops again).
    # We do NOT drop it here.
    return False


def _queue_for_priority(s, priority):
    if priority == Priority.QUERY:
        return s.wait_q
    if priority == Priority.INTERACTIVE:
        return s.wait_i
    return s.wait_b


def _enqueue_pipeline(s, pipeline):
    q = _queue_for_priority(s, pipeline.priority)
    q.append(pipeline)
    pid = pipeline.pipeline_id
    if pid not in s.ram_boost:
        s.ram_boost[pid] = 1.0
    if pid not in s.retry_cnt:
        s.retry_cnt[pid] = 0


def _prune_and_rotate_queue(q):
    # Keep queue order stable while removing already-successful pipelines.
    kept = []
    for p in q:
        if not _pipeline_done_or_dead(p):
            kept.append(p)
    q[:] = kept


def _collect_candidates_from_queue(s, q, max_scan=64):
    # Return a list of (pipeline, op) candidates (at most one per pipeline) in FIFO order.
    cands = []
    scanned = 0
    for p in q:
        if scanned >= max_scan:
            break
        scanned += 1
        st = p.runtime_status()
        if st.is_pipeline_successful():
            continue

        # Only consider assignable ops whose parents are complete.
        op_list = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
        if not op_list:
            continue
        cands.append((p, op_list[0]))
    return cands


def _has_high_priority_waiting(s):
    # If any query or interactive has an assignable op ready, treat as waiting.
    for q in (s.wait_q, s.wait_i):
        for p in q:
            st = p.runtime_status()
            if st.is_pipeline_successful():
                continue
            op_list = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
            if op_list:
                return True
    return False


@register_scheduler(key="scheduler_est_050")
def scheduler_est_050(s, results, pipelines):
    suspensions = []
    assignments = []

    # Ingest new pipelines
    for p in pipelines:
        _enqueue_pipeline(s, p)

    # Process results to adjust RAM boost / retry count on failures.
    # Heuristic: any failure triggers RAM boost and a limited retry budget.
    for r in results:
        if hasattr(r, "failed") and r.failed():
            pid = getattr(r, "pipeline_id", None)
            # If pipeline_id is not available on result, fall back to ops' pipeline attribute if present.
            if pid is None:
                pid = getattr(getattr(r, "ops", [None])[0], "pipeline_id", None)

            if pid is not None:
                s.retry_cnt[pid] = s.retry_cnt.get(pid, 0) + 1
                if s.retry_cnt[pid] <= s.max_retries:
                    s.ram_boost[pid] = min(s.max_ram_boost, s.ram_boost.get(pid, 1.0) * s.oom_ram_boost_mult)

    # Early exit if nothing changed
    if not pipelines and not results:
        return [], []

    # Prune completed pipelines from queues (keep potentially retryable ones).
    _prune_and_rotate_queue(s.wait_q)
    _prune_and_rotate_queue(s.wait_i)
    _prune_and_rotate_queue(s.wait_b)

    # Determine if we should reserve capacity against batch packing.
    high_waiting = _has_high_priority_waiting(s)

    # For each pool, greedily pack assignments.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool
        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # Soft reservations for batch only (keep some headroom if high priorities are waiting).
        reserve_cpu = int(round(getattr(pool, "max_cpu_pool", avail_cpu) * s.reserve_cpu_frac)) if high_waiting else 0
        reserve_ram = float(getattr(pool, "max_ram_pool", avail_ram) * s.reserve_ram_frac) if high_waiting else 0.0

        # Collect candidates from queues (bounded scan to keep per-tick cost small/deterministic).
        c_q = _collect_candidates_from_queue(s, s.wait_q, max_scan=64)
        c_i = _collect_candidates_from_queue(s, s.wait_i, max_scan=64)
        c_b = _collect_candidates_from_queue(s, s.wait_b, max_scan=128)

        # Selection loop: keep assigning while there is room.
        # We choose across all candidates each iteration based on priority then estimated mem (smallest first).
        already_picked_pids = set()  # avoid assigning multiple ops from same pipeline in same pool tick

        while avail_cpu > 0 and avail_ram > 0:
            best = None  # (priority_rank, est_mem_sortkey, fifo_idx, pipeline, op)
            fifo_idx = 0

            for cand_list in (c_q, c_i, c_b):
                for (p, op) in cand_list:
                    fifo_idx += 1
                    pid = p.pipeline_id
                    if pid in already_picked_pids:
                        continue

                    # Stop retrying after max_retries: leaving it queued will likely fail again;
                    # but we still don't "drop" it explicitly—simulation scoring will penalize if it never completes.
                    if s.retry_cnt.get(pid, 0) > s.max_retries:
                        continue

                    # Compute required RAM request based on estimate/boost.
                    ram_req = _compute_ram_request_gb(s, pid, op, pool)

                    # Batch reservation enforcement
                    is_batch = (p.priority == Priority.BATCH_PIPELINE)
                    eff_avail_cpu = avail_cpu
                    eff_avail_ram = avail_ram
                    if is_batch and high_waiting:
                        eff_avail_cpu = max(0, avail_cpu - reserve_cpu)
                        eff_avail_ram = max(0.0, avail_ram - reserve_ram)

                    # Must fit current pool availability (after reservation if batch)
                    if ram_req > eff_avail_ram:
                        continue
                    # CPU will be sized below; require at least 1 CPU available.
                    if eff_avail_cpu <= 0:
                        continue

                    est_mem = _estimated_mem_gb(op)
                    # Sorting: higher priority first; within priority prefer smaller estimated mem.
                    # If estimate is None, treat as "large" to avoid blocking tighter fits.
                    mem_sort = float(est_mem) if est_mem is not None else 1e18
                    cand_key = (_priority_rank(p.priority), -mem_sort, -fifo_idx)  # build for max() selection

                    if best is None or cand_key > best[0]:
                        best = (cand_key, p, op, ram_req)

            if best is None:
                break

            _, p, op, ram_req = best
            pid = p.pipeline_id

            # CPU sizing
            target_cpu = _target_cpu_for_priority(pool, p.priority)
            cpu_req = min(avail_cpu, target_cpu)
            if cpu_req <= 0:
                break

            # Final RAM allocation caps to available RAM
            ram_alloc = min(avail_ram, ram_req)

            # Create assignment
            assignment = Assignment(
                ops=[op],
                cpu=cpu_req,
                ram=ram_alloc,
                priority=p.priority,
                pool_id=pool_id,
                pipeline_id=pid,
            )
            assignments.append(assignment)

            # Update local availability and bookkeeping
            avail_cpu -= cpu_req
            avail_ram -= ram_alloc
            already_picked_pids.add(pid)

    return suspensions, assignments
