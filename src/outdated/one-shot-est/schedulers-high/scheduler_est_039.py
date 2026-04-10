# policy_key: scheduler_est_039
# reasoning_effort: high
# model: gpt-5.2-2025-12-11
# llm_cost: 0.100372
# generation_seconds: 143.15
# generated_at: 2026-03-31T19:36:14.043853
@register_scheduler_init(key="scheduler_est_039")
def scheduler_est_039_init(s):
    """Priority-aware, estimate-based scheduler with simple headroom reservation.

    Incremental improvements over naive FIFO:
      1) Fill each pool with multiple assignments per tick (avoid artificial 1-op-per-pool bottleneck).
      2) Use strict priority ordering (QUERY > INTERACTIVE > BATCH) to improve tail latency.
      3) Reserve a small fraction of each pool for high-priority work when there's backlog.
      4) Size RAM from op.estimate.mem_peak_gb with a safety margin; on OOM, retry with larger RAM.
      5) Keep fairness via round-robin within each priority class + optional aging for batch.
    """
    s.tick = 0

    # Per-priority FIFO queues of pipeline_ids (round-robin via cursor index).
    s.queues = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }
    s.rr_cursor = {
        Priority.QUERY: 0,
        Priority.INTERACTIVE: 0,
        Priority.BATCH_PIPELINE: 0,
    }

    # Tracking pipelines and metadata
    s.pipeline_by_id = {}          # pipeline_id -> Pipeline
    s.pipeline_prio = {}           # pipeline_id -> Priority
    s.enqueue_tick = {}            # pipeline_id -> tick when first seen
    s.in_queue = set()             # pipeline_ids currently queued

    # Failure handling / OOM backoff
    s.last_failure_kind = {}       # pipeline_id -> "oom" | "other"
    s.oom_mem_factor = {}          # (pipeline_id, op_obj) OR op_obj -> factor
    s.oom_retry_count = {}         # (pipeline_id, op_obj) OR op_obj -> count
    s.dropped = set()              # pipeline_ids we will no longer schedule

    # Tunables (kept conservative; can be adjusted iteratively)
    s.reserve_frac_cpu = 0.20      # keep this fraction of CPU for high-priority backlog
    s.reserve_frac_ram = 0.20      # keep this fraction of RAM for high-priority backlog

    s.mem_safety = 1.20            # multiply estimates by this on first attempt
    s.min_ram_gb = 0.5             # minimum RAM to ever assign (GB)
    s.max_mem_factor = 16.0        # cap exponential RAM backoff on repeated OOMs
    s.max_oom_retries = 4          # cap OOM retries per operator before dropping pipeline

    s.batch_aging_ticks = 250      # after this, batch can consume reserved headroom


@register_scheduler(key="scheduler_est_039")
def scheduler_est_039_scheduler(s, results: List["ExecutionResult"],
                                pipelines: List["Pipeline"]) -> Tuple[List["Suspend"], List["Assignment"]]:
    # ---------------------------
    # Helpers (local, no imports)
    # ---------------------------
    def _prio_order():
        # Highest to lowest.
        return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]

    def _is_high_prio(prio):
        return prio in (Priority.QUERY, Priority.INTERACTIVE)

    def _is_oom_error(err):
        if err is None:
            return False
        try:
            e = str(err).lower()
        except Exception:
            return False
        return ("oom" in e) or ("out of memory" in e) or ("out-of-memory" in e) or ("killed" in e and "memory" in e)

    def _infer_pipeline_id_from_result(r):
        # ExecutionResult does not guarantee pipeline_id in the prompt; infer from ops when possible.
        pid = getattr(r, "pipeline_id", None)
        if pid is not None:
            return pid
        ops = getattr(r, "ops", None) or []
        for op in ops:
            pid = getattr(op, "pipeline_id", None)
            if pid is not None:
                return pid
            p = getattr(op, "pipeline", None)
            if p is not None:
                pid = getattr(p, "pipeline_id", None)
                if pid is not None:
                    return pid
        return None

    def _queue_for_prio(prio):
        if prio not in s.queues:
            # Defensive: unknown priorities go to lowest class.
            return s.queues[Priority.BATCH_PIPELINE]
        return s.queues[prio]

    def _remove_pipeline(pid):
        if pid in s.in_queue:
            pr = s.pipeline_prio.get(pid, None)
            if pr in s.queues:
                q = s.queues[pr]
                try:
                    idx = q.index(pid)
                    q.pop(idx)
                    # Keep cursor stable after removal.
                    if s.rr_cursor[pr] > idx:
                        s.rr_cursor[pr] -= 1
                    if s.rr_cursor[pr] >= len(q):
                        s.rr_cursor[pr] = 0
                except ValueError:
                    pass
            s.in_queue.discard(pid)

        s.pipeline_by_id.pop(pid, None)
        s.pipeline_prio.pop(pid, None)
        s.enqueue_tick.pop(pid, None)
        s.last_failure_kind.pop(pid, None)
        s.dropped.discard(pid)

    def _get_est_mem_gb(op, default_gb):
        est = getattr(op, "estimate", None)
        mem = getattr(est, "mem_peak_gb", None)
        try:
            if mem is None:
                return float(default_gb)
            memf = float(mem)
            if memf <= 0:
                return float(default_gb)
            return memf
        except Exception:
            return float(default_gb)

    def _oom_key(pid, op):
        # Use (pid, op) when possible; fallback to op identity.
        return (pid, op) if pid is not None else op

    def _desired_ram_gb(pid, op, pool):
        default_gb = max(s.min_ram_gb, 0.10 * float(pool.max_ram_pool))
        est_gb = _get_est_mem_gb(op, default_gb)

        key = _oom_key(pid, op)
        factor = float(s.oom_mem_factor.get(key, 1.0))
        ram = max(s.min_ram_gb, est_gb * s.mem_safety * factor)
        # Never ask beyond pool maximum.
        if ram > float(pool.max_ram_pool):
            ram = float(pool.max_ram_pool)
        return ram

    def _desired_cpu(prio, pool, avail_cpu):
        # Conservative CPU sizing to improve latency for high-priority without destroying parallelism.
        max_cpu = float(pool.max_cpu_pool)
        if prio == Priority.QUERY:
            target = max(1.0, 0.50 * max_cpu)
        elif prio == Priority.INTERACTIVE:
            target = max(1.0, 0.33 * max_cpu)
        else:
            target = max(1.0, 0.25 * max_cpu)

        # Cap at available.
        if target > float(avail_cpu):
            target = float(avail_cpu)
        # Minimum 1 CPU when anything is available.
        if float(avail_cpu) >= 1.0 and target < 1.0:
            target = 1.0
        return target

    def _has_inflight_ops(status):
        running_like = [OperatorState.RUNNING, OperatorState.ASSIGNED, OperatorState.SUSPENDING]
        inflight = status.get_ops(running_like, require_parents_complete=False)
        return len(inflight) > 0

    def _next_candidate_pid(prio, attempts_limit=None):
        q = s.queues[prio]
        if not q:
            return None
        if attempts_limit is None:
            attempts_limit = len(q)
        if attempts_limit <= 0:
            return None

        # Round-robin scan.
        start = s.rr_cursor[prio] % len(q)
        for step in range(min(attempts_limit, len(q))):
            idx = (start + step) % len(q)
            pid = q[idx]
            # Cursor moves past the examined pid for fairness.
            s.rr_cursor[prio] = (idx + 1) % len(q)
            return pid
        return None

    def _rotate_pid_to_back(prio, pid):
        # Optional; keep simple fairness by moving scheduled/checked items to back.
        q = s.queues[prio]
        try:
            idx = q.index(pid)
        except ValueError:
            return
        q.pop(idx)
        q.append(pid)
        # Cursor stays within bounds.
        if s.rr_cursor[prio] >= len(q):
            s.rr_cursor[prio] = 0

    def _has_high_prio_backlog():
        # A cheap heuristic: if any high-priority queue contains any not-dropped pipeline.
        for pr in (Priority.QUERY, Priority.INTERACTIVE):
            for pid in s.queues.get(pr, []):
                if pid not in s.dropped and pid in s.pipeline_by_id:
                    return True
        return False

    # ---------------------------
    # Tick bookkeeping
    # ---------------------------
    s.tick += 1

    # Ingest new pipelines
    for p in pipelines:
        pid = p.pipeline_id
        s.pipeline_by_id[pid] = p
        s.pipeline_prio[pid] = p.priority
        if pid not in s.enqueue_tick:
            s.enqueue_tick[pid] = s.tick
        if pid not in s.in_queue and pid not in s.dropped:
            _queue_for_prio(p.priority).append(pid)
            s.in_queue.add(pid)

    # Process execution results (OOM backoff + failure classification)
    for r in results:
        if not hasattr(r, "failed") or not r.failed():
            continue

        pid = _infer_pipeline_id_from_result(r)
        oom = _is_oom_error(getattr(r, "error", None))
        if pid is not None:
            s.last_failure_kind[pid] = "oom" if oom else "other"

        # If OOM, increase memory factor for the failed ops and allow retries.
        if oom:
            ops = getattr(r, "ops", None) or []
            for op in ops:
                key = _oom_key(pid, op)
                prev_factor = float(s.oom_mem_factor.get(key, 1.0))
                new_factor = min(s.max_mem_factor, max(1.0, prev_factor * 2.0))
                s.oom_mem_factor[key] = new_factor

                prev_cnt = int(s.oom_retry_count.get(key, 0))
                s.oom_retry_count[key] = prev_cnt + 1

                # If we keep OOMing the same op, drop the pipeline to avoid infinite churn.
                if (prev_cnt + 1) > int(s.max_oom_retries) and pid is not None:
                    s.dropped.add(pid)
        else:
            # Non-OOM failures: drop pipeline (conservative).
            if pid is not None:
                s.dropped.add(pid)

    # ---------------------------
    # Scheduling
    # ---------------------------
    suspensions = []
    assignments = []

    high_prio_backlog = _has_high_prio_backlog()

    # Prefer to iterate pools with most headroom first to reduce "can't-fit" scans.
    pool_ids = list(range(s.executor.num_pools))
    try:
        pool_ids.sort(
            key=lambda i: (
                float(s.executor.pools[i].avail_cpu_pool) + 1e-9,
                float(s.executor.pools[i].avail_ram_pool) + 1e-9,
            ),
            reverse=True,
        )
    except Exception:
        pass

    for pool_id in pool_ids:
        pool = s.executor.pools[pool_id]
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)
        if avail_cpu <= 0.0 or avail_ram <= 0.0:
            continue

        reserve_cpu = float(pool.max_cpu_pool) * float(s.reserve_frac_cpu) if high_prio_backlog else 0.0
        reserve_ram = float(pool.max_ram_pool) * float(s.reserve_frac_ram) if high_prio_backlog else 0.0

        # Hard stop thresholds: must have at least 1 CPU and minimal RAM to run anything.
        while avail_cpu >= 1.0 and avail_ram >= float(s.min_ram_gb):
            made_assignment = False

            for prio in _prio_order():
                q = s.queues.get(prio, [])
                if not q:
                    continue

                # Enforce headroom reservation against batch while high-priority backlog exists,
                # unless a batch pipeline has aged enough.
                if prio == Priority.BATCH_PIPELINE and high_prio_backlog:
                    # If only reserved headroom remains, skip batch.
                    usable_cpu = avail_cpu - reserve_cpu
                    usable_ram = avail_ram - reserve_ram
                    if usable_cpu < 1.0 or usable_ram < float(s.min_ram_gb):
                        continue

                # Try a bounded number of candidates for this priority, to avoid long scans.
                scan_budget = min(len(q), 8)
                for _ in range(scan_budget):
                    pid = _next_candidate_pid(prio)
                    if pid is None:
                        break
                    if pid in s.dropped:
                        _remove_pipeline(pid)
                        continue

                    pipeline = s.pipeline_by_id.get(pid, None)
                    if pipeline is None:
                        _remove_pipeline(pid)
                        continue

                    status = pipeline.runtime_status()
                    if status.is_pipeline_successful():
                        _remove_pipeline(pid)
                        continue

                    # Conservative failure policy:
                    # - If any FAILED exists and we did not classify it as OOM, drop.
                    has_failures = status.state_counts.get(OperatorState.FAILED, 0) > 0
                    if has_failures and s.last_failure_kind.get(pid, "other") != "oom":
                        s.dropped.add(pid)
                        _remove_pipeline(pid)
                        continue

                    # Keep per-pipeline concurrency at 1 to reduce interference and improve predictability.
                    if _has_inflight_ops(status):
                        _rotate_pid_to_back(prio, pid)
                        continue

                    # Get ready ops (PENDING/FAILED with parents complete); pick smallest RAM first for fit.
                    ready_ops = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
                    if not ready_ops:
                        _rotate_pid_to_back(prio, pid)
                        continue

                    # Choose op that is easiest to fit given remaining RAM.
                    def _op_sort_key(op):
                        return _desired_ram_gb(pid, op, pool)

                    ready_ops_sorted = sorted(ready_ops, key=_op_sort_key)
                    chosen_op = None
                    chosen_ram = None

                    # Try a few smallest ops from this pipeline; helps packing and reduces head-of-line blocking.
                    for op in ready_ops_sorted[:3]:
                        req_ram = float(_desired_ram_gb(pid, op, pool))
                        if req_ram <= avail_ram:
                            chosen_op = op
                            chosen_ram = req_ram
                            break

                    if chosen_op is None:
                        # Can't fit anything from this pipeline right now; try other pipelines.
                        _rotate_pid_to_back(prio, pid)
                        continue

                    # Apply reservation constraints for batch (compute usable headroom).
                    if prio == Priority.BATCH_PIPELINE and high_prio_backlog:
                        age = int(s.tick - s.enqueue_tick.get(pid, s.tick))
                        if age < int(s.batch_aging_ticks):
                            usable_cpu = max(0.0, avail_cpu - reserve_cpu)
                            usable_ram = max(0.0, avail_ram - reserve_ram)
                            if usable_cpu < 1.0 or usable_ram < float(s.min_ram_gb):
                                _rotate_pid_to_back(prio, pid)
                                continue
                            if chosen_ram > usable_ram:
                                _rotate_pid_to_back(prio, pid)
                                continue
                            req_cpu = float(_desired_cpu(prio, pool, usable_cpu))
                        else:
                            # Aged batch may use full remaining resources.
                            req_cpu = float(_desired_cpu(prio, pool, avail_cpu))
                    else:
                        req_cpu = float(_desired_cpu(prio, pool, avail_cpu))

                    if req_cpu < 1.0 or req_cpu > avail_cpu or chosen_ram > avail_ram:
                        _rotate_pid_to_back(prio, pid)
                        continue

                    assignments.append(
                        Assignment(
                            ops=[chosen_op],
                            cpu=req_cpu,
                            ram=float(chosen_ram),
                            priority=pipeline.priority,
                            pool_id=pool_id,
                            pipeline_id=pipeline.pipeline_id,
                        )
                    )

                    avail_cpu -= req_cpu
                    avail_ram -= float(chosen_ram)

                    # Move this pipeline to the back to be fair within its priority.
                    _rotate_pid_to_back(prio, pid)
                    made_assignment = True
                    break  # stop scanning this priority; continue filling pool from highest priorities again

                if made_assignment:
                    break  # restart priority loop with updated avail resources

            if not made_assignment:
                break  # can't make progress in this pool right now

    return suspensions, assignments
