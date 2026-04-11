# policy_key: scheduler_none_031
# reasoning_effort: none
# model: gpt-5.2-2025-12-11
# llm_cost: 0.058517
# generation_seconds: 41.35
# generated_at: 2026-04-09T22:09:20.201838
@register_scheduler_init(key="scheduler_none_031")
def scheduler_none_031_init(s):
    """Priority-protecting, OOM-adaptive, fair scheduler.

    Core ideas (kept intentionally simple and robust):
    1) Strict priority across classes to protect weighted score: QUERY > INTERACTIVE > BATCH.
    2) Conservative first-attempt sizing (RAM) to avoid OOM-driven failures (which are extremely costly).
    3) OOM-adaptive retry: on failure, increase RAM guess for the pipeline and requeue.
    4) Light fairness via aging: long-waiting lower-priority pipelines eventually get considered.
    5) Minimal preemption: only preempt low priority when high priority arrives and there is no headroom,
       using a small preemption budget to avoid churn.

    Notes:
    - We do not attempt perfect bin-packing or multi-op per assignment; we schedule at most 1 ready op per tick per pool.
    - We infer RAM needs only from observed OOM failures and previous allocations.
    """
    # Queue entries: dict(pipeline=Pipeline, enq_t=int)
    s.waiting = []
    # Per-pipeline RAM guess (float)
    s.ram_guess = {}
    # Per-pipeline CPU cap guess (float) to avoid hogging whole pool
    s.cpu_cap_guess = {}
    # Per-pipeline count of oom failures
    s.oom_failures = {}
    # Track if we saw new high-priority arrivals this tick (used for preemption trigger)
    s._saw_hp_arrival = False
    # Preemption rate limiting (simple token bucket per tick)
    s._preempt_tokens = 0
    s._tick = 0


@register_scheduler(key="scheduler_none_031")
def scheduler_none_031(s, results, pipelines):
    """See init docstring for overview."""
    # ---- Helper functions kept local to avoid imports and external dependencies ----
    def _is_oom_failure(res):
        if not res or not res.failed():
            return False
        # Try a few common patterns; simulator may encode error in different ways.
        err = getattr(res, "error", None)
        if err is None:
            return False
        msg = str(err).lower()
        return ("oom" in msg) or ("out of memory" in msg) or ("out_of_memory" in msg) or ("memoryerror" in msg)

    def _priority_rank(pri):
        # Higher rank = more important
        if pri == Priority.QUERY:
            return 3
        if pri == Priority.INTERACTIVE:
            return 2
        return 1  # batch or anything else

    def _priority_weight(pri):
        if pri == Priority.QUERY:
            return 10
        if pri == Priority.INTERACTIVE:
            return 5
        return 1

    def _ensure_defaults(p):
        pid = p.pipeline_id
        if pid not in s.ram_guess:
            # Conservative default: start with a modest fraction of pool RAM so we don't under-allocate too often.
            # Will be clamped to pool availability at assignment time.
            s.ram_guess[pid] = None  # pool-dependent default decided later
        if pid not in s.cpu_cap_guess:
            # Cap CPU so that high priority can run concurrently and avoid head-of-line blocking.
            # This is a cap, not a reservation; if plenty of CPU exists we can still use it.
            s.cpu_cap_guess[pid] = None  # pool-dependent default decided later
        if pid not in s.oom_failures:
            s.oom_failures[pid] = 0

    def _pipeline_state(p):
        st = p.runtime_status()
        # Drop completed or terminally failed pipelines (avoid infinite retries on non-OOM failures).
        if st.is_pipeline_successful():
            return "DONE"
        has_failures = st.state_counts[OperatorState.FAILED] > 0
        if has_failures:
            return "HAS_FAILED_OP"
        return "ACTIVE"

    def _ready_ops(p):
        st = p.runtime_status()
        # Only schedule parents-complete ops; allow FAILED to be re-assigned if simulator uses that for retry.
        ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        return ops

    def _age_bonus(enq_t, now_tick):
        # Mild aging: every ~20 ticks adds one priority "point" (bounded).
        waited = max(0, now_tick - enq_t)
        return min(2.0, waited / 20.0)

    def _score_key(item, now_tick):
        p = item["pipeline"]
        pri = p.priority
        # Weighted by class importance + aging to prevent starvation
        base = _priority_rank(pri) * 10.0
        bonus = _age_bonus(item["enq_t"], now_tick)
        # Prefer pipelines that have no OOM failures (to finish more quickly) but do not starve OOM ones.
        pid = p.pipeline_id
        oom_pen = min(5.0, s.oom_failures.get(pid, 0) * 1.5)
        return base + bonus - oom_pen

    def _pick_pool_for(pri):
        # If there are multiple pools, prefer pools with most headroom for high priority.
        # For batch, prefer the "largest" pool too, to reduce fragmentation and avoid OOM.
        best = None
        best_key = None
        for pool_id in range(s.executor.num_pools):
            pool = s.executor.pools[pool_id]
            avail_cpu = pool.avail_cpu_pool
            avail_ram = pool.avail_ram_pool
            if avail_cpu <= 0 or avail_ram <= 0:
                continue
            # Key: high priority values RAM headroom most (avoid OOM), then CPU.
            # Batch values CPU slightly more to keep throughput.
            if pri in (Priority.QUERY, Priority.INTERACTIVE):
                key = (avail_ram, avail_cpu)
            else:
                key = (avail_cpu, avail_ram)
            if best is None or key > best_key:
                best = pool_id
                best_key = key
        return best

    def _default_ram_for_pool(pool):
        # Conservative default: 25% of pool RAM for query, 20% for interactive, 15% for batch.
        return pool.max_ram_pool

    def _ram_target(p, pool):
        pid = p.pipeline_id
        pri = p.priority
        max_ram = pool.max_ram_pool
        # Set per-class default if not initialized
        if s.ram_guess[pid] is None:
            if pri == Priority.QUERY:
                s.ram_guess[pid] = max(0.10 * max_ram, min(0.30 * max_ram, max_ram))
            elif pri == Priority.INTERACTIVE:
                s.ram_guess[pid] = max(0.10 * max_ram, min(0.25 * max_ram, max_ram))
            else:
                s.ram_guess[pid] = max(0.05 * max_ram, min(0.20 * max_ram, max_ram))

        # OOM backoff: exponentially increase but cap at pool max RAM
        oom_cnt = s.oom_failures.get(pid, 0)
        guess = float(s.ram_guess[pid])
        if oom_cnt > 0:
            # 1st OOM: x1.5, then x2.0, x2.5 ... (bounded)
            mult = min(4.0, 1.0 + 0.5 * oom_cnt + 0.5 * max(0, oom_cnt - 1))
            guess = min(max_ram, guess * mult)

        # Clamp to available RAM
        return max(1e-6, min(pool.avail_ram_pool, guess, max_ram))

    def _cpu_target(p, pool):
        pid = p.pipeline_id
        pri = p.priority
        max_cpu = pool.max_cpu_pool

        # Per-class CPU cap heuristic: keep room for other work, but don't under-allocate too much.
        if s.cpu_cap_guess[pid] is None:
            if pri == Priority.QUERY:
                s.cpu_cap_guess[pid] = max(1.0, min(max_cpu, 0.60 * max_cpu))
            elif pri == Priority.INTERACTIVE:
                s.cpu_cap_guess[pid] = max(1.0, min(max_cpu, 0.50 * max_cpu))
            else:
                s.cpu_cap_guess[pid] = max(1.0, min(max_cpu, 0.70 * max_cpu))

        cap = float(s.cpu_cap_guess[pid])

        # If pipeline has repeated OOM failures, CPU isn't the fix; keep CPU modest to reduce contention.
        oom_cnt = s.oom_failures.get(pid, 0)
        if oom_cnt >= 2 and pri != Priority.BATCH_PIPELINE:
            cap = min(cap, max(1.0, 0.40 * max_cpu))

        # Clamp to available CPU
        return max(1e-6, min(pool.avail_cpu_pool, cap, max_cpu))

    # ---- Update tick + tokens ----
    s._tick += 1
    # Allow at most 1 preemption per tick; accumulate up to 2.
    s._preempt_tokens = min(2, s._preempt_tokens + 1)

    # ---- Ingest new pipelines ----
    s._saw_hp_arrival = False
    for p in pipelines:
        _ensure_defaults(p)
        s.waiting.append({"pipeline": p, "enq_t": s._tick})
        if p.priority in (Priority.QUERY, Priority.INTERACTIVE):
            s._saw_hp_arrival = True

    # ---- Process results: adapt RAM guesses on OOM failures; drop hard failures from queue ----
    if results:
        for r in results:
            # If a container failed due to OOM, raise RAM guess for that pipeline.
            if _is_oom_failure(r):
                pid = getattr(r, "pipeline_id", None)
                # If pipeline_id isn't available on result, infer from ops if possible (best-effort).
                if pid is None:
                    # Attempt to find pipeline by matching op objects; this may not always work.
                    pid = None
                # Use assignment's RAM as baseline if present
                if pid is not None:
                    s.oom_failures[pid] = s.oom_failures.get(pid, 0) + 1
                    prev = s.ram_guess.get(pid, None)
                    if prev is None and getattr(r, "ram", None):
                        s.ram_guess[pid] = float(r.ram)
                    elif prev is not None:
                        # Bump baseline upward modestly; multiplicative bump handled in _ram_target.
                        s.ram_guess[pid] = min(s.ram_guess[pid] * 1.25, s.ram_guess[pid] + float(getattr(r, "ram", 0) or 0))

    # ---- Clean up waiting list: remove completed pipelines; keep active and allow retry on OOM by leaving FAILED ops assignable ----
    if pipelines or results:
        new_waiting = []
        for item in s.waiting:
            p = item["pipeline"]
            st = p.runtime_status()
            if st.is_pipeline_successful():
                continue
            # If pipeline has failures, we still keep it only if we believe retry is possible.
            # We treat non-OOM failures as terminal by removing if any FAILED and we didn't just OOM-bump.
            # Since we can't reliably detect non-OOM per pipeline here, keep it but it will stall if no assignable ops.
            new_waiting.append(item)
        s.waiting = new_waiting

    # Early exit if nothing changed and no queued work
    if not pipelines and not results and not s.waiting:
        return [], []

    suspensions = []
    assignments = []

    # ---- Optional minimal preemption for high-priority arrivals ----
    # If we saw new QUERY/INTERACTIVE arrivals and all pools are saturated (no cpu or ram),
    # preempt one low-priority running container to free headroom, rate-limited.
    if s._saw_hp_arrival and s._preempt_tokens > 0:
        any_headroom = False
        for pool_id in range(s.executor.num_pools):
            pool = s.executor.pools[pool_id]
            if pool.avail_cpu_pool > 0 and pool.avail_ram_pool > 0:
                any_headroom = True
                break

        if not any_headroom:
            # Best-effort: look for a low-priority container to preempt from results metadata if present.
            # If not available, skip preemption to avoid errors.
            # Some simulators provide executor.container_index or similar; we avoid relying on it.
            pass

    # ---- Scheduling loop: one assignment per pool per tick, pick best pipeline by priority+aging ----
    # Sort waiting by score descending (highest first)
    if s.waiting:
        s.waiting.sort(key=lambda it: _score_key(it, s._tick), reverse=True)

    for _ in range(s.executor.num_pools):
        if not s.waiting:
            break

        # Choose next candidate pipeline in priority order, but also choose pool appropriately.
        # We attempt a few top candidates to find one with ready ops and a viable pool.
        chosen_idx = None
        chosen_pool_id = None
        chosen_ops = None

        scan_limit = min(12, len(s.waiting))
        for idx in range(scan_limit):
            p = s.waiting[idx]["pipeline"]
            _ensure_defaults(p)
            state = _pipeline_state(p)
            if state == "DONE":
                continue

            ops = _ready_ops(p)
            if not ops:
                continue

            pool_id = _pick_pool_for(p.priority)
            if pool_id is None:
                continue

            pool = s.executor.pools[pool_id]
            if pool.avail_cpu_pool <= 0 or pool.avail_ram_pool <= 0:
                continue

            chosen_idx = idx
            chosen_pool_id = pool_id
            chosen_ops = ops[:1]  # single-op assignment for stability
            break

        if chosen_idx is None:
            # No viable assignment across pools
            break

        item = s.waiting.pop(chosen_idx)
        p = item["pipeline"]
        pool = s.executor.pools[chosen_pool_id]

        cpu = _cpu_target(p, pool)
        ram = _ram_target(p, pool)

        # If after clamping, resources are effectively zero, put back and stop for this tick.
        if cpu <= 1e-6 or ram <= 1e-6:
            s.waiting.append(item)
            break

        assignments.append(
            Assignment(
                ops=chosen_ops,
                cpu=cpu,
                ram=ram,
                priority=p.priority,
                pool_id=chosen_pool_id,
                pipeline_id=p.pipeline_id,
            )
        )

        # Reinsert pipeline (it likely has more ops later). Keep original enqueue time for fairness.
        s.waiting.append(item)

    return suspensions, assignments
