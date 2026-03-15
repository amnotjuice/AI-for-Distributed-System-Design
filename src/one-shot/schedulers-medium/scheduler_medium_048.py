# policy_key: scheduler_medium_048
# reasoning_effort: medium
# model: gpt-5.2-2025-12-11
# llm_cost: 0.057119
# generation_seconds: 59.11
# generated_at: 2026-03-14T03:22:54.002317
@register_scheduler_init(key="scheduler_medium_048")
def scheduler_medium_048_init(s):
    """
    Priority-aware, right-sized FIFO with simple OOM-aware retries.

    Improvements over naive FIFO:
      1) Separate waiting queues per priority (QUERY > INTERACTIVE > BATCH) using weighted RR
         so high-priority work is admitted first under contention.
      2) Avoid letting a single low-priority op monopolize the whole pool by capping per-op
         CPU/RAM allocations based on priority (better latency under mixed workloads).
      3) If an op fails with OOM, retry it with increased RAM next time (bounded growth).
    """
    # Waiting queues per priority (FIFO within each class)
    s.wait_q = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }

    # Weighted round-robin across priority classes (higher weight => more picks)
    s.rr_order = (
        [Priority.QUERY] * 6
        + [Priority.INTERACTIVE] * 3
        + [Priority.BATCH_PIPELINE] * 1
    )
    s.rr_idx = 0

    # Track which pipeline an op belongs to (ExecutionResult doesn't necessarily include pipeline_id)
    # Keyed by (pipeline_id, op_key) and/or op_key.
    s.opkey_to_pipeline = {}          # op_key -> pipeline_id
    s.op_attempts = {}                # (pipeline_id, op_key) -> int
    s.op_ram_mult = {}                # (pipeline_id, op_key) -> float (OOM backoff multiplier)
    s.op_non_oom_failed = set()        # (pipeline_id, op_key) -> mark as do-not-retry

    # Tunables
    s.max_attempts = 4
    s.ram_backoff = 1.8               # multiply RAM on each OOM
    s.max_ram_mult = 12.0             # cap multiplier to avoid runaway

    # Per-priority caps (fractions of pool max). These prevent batch from grabbing the full pool.
    s.cpu_cap_frac = {
        Priority.QUERY: 0.85,
        Priority.INTERACTIVE: 0.60,
        Priority.BATCH_PIPELINE: 0.35,
    }
    s.ram_cap_frac = {
        Priority.QUERY: 0.85,
        Priority.INTERACTIVE: 0.60,
        Priority.BATCH_PIPELINE: 0.35,
    }

    # Minimal viable allocation (avoid scheduling tiny allocations that only add overhead)
    s.min_cpu = 0.25
    s.min_ram = 0.25


@register_scheduler(key="scheduler_medium_048")
def scheduler_medium_048(s, results, pipelines):
    """
    Scheduler step: update state from results, enqueue new pipelines, and assign ready ops
    across pools using priority-aware selection and right-sized resource requests.
    """
    def _op_key(op):
        # Use object identity to avoid relying on optional fields (e.g., op_id).
        return id(op)

    def _is_oom_error(err):
        if not err:
            return False
        msg = str(err).lower()
        return ("oom" in msg) or ("out of memory" in msg) or ("memory" in msg and "exceed" in msg)

    def _enqueue_pipeline(p):
        pr = p.priority
        if pr not in s.wait_q:
            # Fallback: treat unknown priority as batch-like
            pr = Priority.BATCH_PIPELINE
        s.wait_q[pr].append(p)

    # Add new pipelines to priority queues
    for p in pipelines:
        _enqueue_pipeline(p)

    # If nothing changed, do nothing
    if not pipelines and not results:
        return [], []

    # Update OOM/non-OOM failure state using returned results
    # We map op -> pipeline_id using s.opkey_to_pipeline (populated at assignment time).
    for r in results:
        if not getattr(r, "failed", None):
            continue
        if not r.failed():
            continue

        is_oom = _is_oom_error(getattr(r, "error", None))
        for op in getattr(r, "ops", []) or []:
            ok = _op_key(op)
            pid = s.opkey_to_pipeline.get(ok, None)
            if pid is None:
                continue

            k = (pid, ok)
            s.op_attempts[k] = s.op_attempts.get(k, 0) + 1

            if is_oom:
                # Increase RAM multiplier for next attempt
                cur = s.op_ram_mult.get(k, 1.0)
                nxt = min(s.max_ram_mult, cur * s.ram_backoff)
                s.op_ram_mult[k] = nxt
            else:
                # Non-OOM failures are treated as non-retryable to avoid thrashing.
                s.op_non_oom_failed.add(k)

    suspensions = []
    assignments = []

    # Helper: choose next pipeline using weighted round-robin across priority classes.
    def _pick_next_pipeline():
        n = len(s.rr_order)
        for _ in range(n):
            pr = s.rr_order[s.rr_idx]
            s.rr_idx = (s.rr_idx + 1) % n
            q = s.wait_q.get(pr, [])
            while q:
                p = q.pop(0)
                st = p.runtime_status()
                if st.is_pipeline_successful():
                    continue

                # If any FAILED ops exist, only continue if we believe they are retryable (OOM).
                failed_ops = st.get_ops([OperatorState.FAILED], require_parents_complete=False) or []
                if failed_ops:
                    # If any failed op is marked non-retryable, drop the whole pipeline.
                    drop = False
                    for op in failed_ops:
                        ok = _op_key(op)
                        k = (p.pipeline_id, ok)
                        if k in s.op_non_oom_failed:
                            drop = True
                            break
                        if s.op_attempts.get(k, 0) >= s.max_attempts:
                            # Too many retries: treat as failed to prevent infinite loops.
                            drop = True
                            break
                    if drop:
                        continue

                return p
        return None

    # Track which pipelines we scheduled in this tick (avoid scheduling the same pipeline twice).
    scheduled_pids = set()

    # Precompute whether high-priority queues are empty to allow batch to expand when idle.
    def _has_high_prio_waiting():
        return bool(s.wait_q[Priority.QUERY]) or bool(s.wait_q[Priority.INTERACTIVE])

    # Schedule across pools
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool

        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # Keep placing as long as we can fit something meaningful
        while avail_cpu >= s.min_cpu and avail_ram >= s.min_ram:
            p = _pick_next_pipeline()
            if p is None:
                break
            if p.pipeline_id in scheduled_pids:
                # Put it back to preserve FIFO-ish behavior within its priority class.
                _enqueue_pipeline(p)
                break

            st = p.runtime_status()
            # Only schedule ops whose parents are complete and are in assignable states.
            op_list = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True) or []
            if not op_list:
                # Nothing ready yet; keep pipeline around.
                _enqueue_pipeline(p)
                continue

            op = op_list[0]
            ok = _op_key(op)
            pid = p.pipeline_id
            k = (pid, ok)

            # If we previously marked this op as non-retryable, skip pipeline.
            if k in s.op_non_oom_failed:
                # Drop by not re-enqueueing.
                continue

            # Determine caps for this priority; if no higher-priority waiting, let batch expand.
            pr = p.priority
            cpu_cap = s.cpu_cap_frac.get(pr, 0.35)
            ram_cap = s.ram_cap_frac.get(pr, 0.35)

            if pr == Priority.BATCH_PIPELINE and not _has_high_prio_waiting():
                cpu_cap = 0.90
                ram_cap = 0.90

            # Base request is a capped fraction of pool max, but also cannot exceed current availability.
            req_cpu = min(avail_cpu, max(s.min_cpu, pool.max_cpu_pool * cpu_cap))
            req_ram = min(avail_ram, max(s.min_ram, pool.max_ram_pool * ram_cap))

            # OOM-aware RAM multiplier (only affects RAM)
            mult = s.op_ram_mult.get(k, 1.0)
            req_ram = min(avail_ram, pool.max_ram_pool, max(s.min_ram, req_ram * mult))

            # If request can't fit even the minimum, stop trying to pack this pool this tick.
            if req_cpu < s.min_cpu or req_ram < s.min_ram:
                _enqueue_pipeline(p)
                break

            # Record mapping for result attribution
            s.opkey_to_pipeline[ok] = pid

            assignments.append(
                Assignment(
                    ops=[op],
                    cpu=req_cpu,
                    ram=req_ram,
                    priority=p.priority,
                    pool_id=pool_id,
                    pipeline_id=pid,
                )
            )
            scheduled_pids.add(pid)

            # Update local availability for additional placements in this pool
            avail_cpu -= req_cpu
            avail_ram -= req_ram

            # Re-enqueue the pipeline for future ready ops (or retries)
            _enqueue_pipeline(p)

    return suspensions, assignments
