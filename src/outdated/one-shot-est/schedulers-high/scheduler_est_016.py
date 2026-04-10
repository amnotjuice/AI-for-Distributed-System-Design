# policy_key: scheduler_est_016
# reasoning_effort: high
# model: gpt-5.2-2025-12-11
# llm_cost: 0.112188
# generation_seconds: 152.75
# generated_at: 2026-03-31T18:51:13.629004
@register_scheduler_init(key="scheduler_est_016")
def scheduler_est_016_init(s):
    """
    Priority-aware, estimate-guided greedy packing scheduler.

    Improvements over naive FIFO:
      1) Strict priority ordering (QUERY > INTERACTIVE > BATCH_PIPELINE) to reduce tail latency.
      2) Right-size RAM per op using op.estimate.mem_peak_gb with OOM-driven backoff.
      3) Cap per-op CPU by priority to allow more concurrency (avoid "one op eats the pool").
      4) Simple reservation: when high-priority work exists, keep a small CPU/RAM buffer so batch
         does not consume the entire pool and block interactive/query admission.
    """
    from collections import deque

    s.tick = 0

    # One queue per priority (pipelines rotate for fairness within a class)
    s.queues = {
        Priority.QUERY: deque(),
        Priority.INTERACTIVE: deque(),
        Priority.BATCH_PIPELINE: deque(),
    }
    s.in_queue = set()  # pipeline_id currently enqueued (avoid duplicates)

    # Per-operator retry sizing state (keyed by operator identity)
    s.op_mem_factor = {}       # op_key -> multiplicative factor on estimated memory
    s.op_last_ram = {}         # op_key -> last attempted ram (GB)
    s.op_nonretryable = set()  # op_key -> failed with non-OOM error; do not retry


@register_scheduler(key="scheduler_est_016")
def scheduler_est_016_scheduler(s, results, pipelines):
    """
    Main scheduling step.

    Strategy:
      - Enqueue new pipelines into per-priority queues.
      - Update per-op memory factor on OOM failures.
      - Greedily schedule at most 1 op per pipeline per tick (fairness), globally across pools:
          * High priority chooses the pool with MOST headroom (reduces queueing/placement failures).
          * Batch chooses BEST-FIT by RAM (packs tightly, preserves big headroom for latency work).
      - When any high-priority pipeline appears to have ready work, reserve a small portion of each
        pool for it; batch can only consume resources beyond the reservation.
    """
    s.tick += 1

    # -------- Helper functions (kept local to avoid imports at module scope) --------
    def _prio_order():
        return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]

    def _is_high_prio(prio):
        return prio in (Priority.QUERY, Priority.INTERACTIVE)

    def _op_key(op):
        # Prefer stable ids if present; fall back to object identity.
        return getattr(op, "op_id", getattr(op, "operator_id", id(op)))

    def _is_oom_error(err):
        if err is None:
            return False
        msg = str(err).lower()
        return ("oom" in msg) or ("out of memory" in msg) or ("killed" in msg and "memory" in msg)

    def _pipeline_done_or_hard_failed(p):
        status = p.runtime_status()
        if status.is_pipeline_successful():
            return True

        # If any FAILED op is marked non-retryable, consider the whole pipeline terminally failed.
        if status.state_counts.get(OperatorState.FAILED, 0) > 0:
            failed_ops = status.get_ops([OperatorState.FAILED], require_parents_complete=False)
            for op in failed_ops:
                if _op_key(op) in s.op_nonretryable:
                    return True

        return False

    def _pick_ready_op(p):
        status = p.runtime_status()

        # If the pipeline is already in a terminal state, don't schedule anything.
        if status.is_pipeline_successful():
            return None

        # Get assignable ops (PENDING or FAILED) whose parents are complete.
        # Note: FAILED ops are only safe to retry if they failed due to OOM (we track via factors).
        op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        if not op_list:
            return None

        # Prefer the first ready op returned by runtime_status (assumed topological).
        # Skip non-retryable failed ops (if they appear assignable, pipeline is effectively stuck).
        for op in op_list:
            if _op_key(op) in s.op_nonretryable:
                return None
            return op
        return None

    def _estimate_ram_gb(op):
        est = None
        est_obj = getattr(op, "estimate", None)
        if est_obj is not None:
            est = getattr(est_obj, "mem_peak_gb", None)
        if est is None:
            est = 1.0  # conservative default if estimates are absent
        try:
            est = float(est)
        except Exception:
            est = 1.0
        return max(0.25, est)

    def _ram_request_gb(op):
        opk = _op_key(op)
        base = _estimate_ram_gb(op)

        # Safety margin + adaptive backoff after OOMs.
        factor = s.op_mem_factor.get(opk, 1.15)

        # Small fixed overhead for runtime/container + Arrow buffers, etc.
        ram = (base + 0.25) * factor

        # If we previously tried a larger RAM and still failed/ran, do not shrink too aggressively.
        if opk in s.op_last_ram:
            ram = max(ram, s.op_last_ram[opk] * 1.02)

        return max(0.25, ram)

    def _cpu_cap_for_priority(prio):
        # Caps to avoid single-op monopoly; tuned for latency vs. concurrency.
        if prio == Priority.QUERY:
            return 8.0
        if prio == Priority.INTERACTIVE:
            return 4.0
        return 2.0  # batch

    def _has_high_prio_pressure():
        # Lightweight readiness check: if any high-priority pipeline has a ready op, apply reservation.
        for pr in (Priority.QUERY, Priority.INTERACTIVE):
            q = s.queues[pr]
            # Scan a small prefix to avoid O(N) cost on huge queues.
            scanned = 0
            for p in list(q)[:8]:
                scanned += 1
                if _pipeline_done_or_hard_failed(p):
                    continue
                if _pick_ready_op(p) is not None:
                    return True
        return False

    def _reserve_for_high_prio(pool):
        # Reserve a modest share to reduce head-of-line blocking for interactive/query.
        # Keep it small to avoid starving batch under light interactive load.
        r_cpu = max(1.0, 0.15 * float(pool.max_cpu_pool))
        r_ram = max(0.5, 0.15 * float(pool.max_ram_pool))
        return r_cpu, r_ram

    def _select_pool_for_op(prio, ram_need, cpu_cap, avail_cpu, avail_ram, pools, reserve_active):
        best_pool = None
        best_score = None
        best_cpu = None

        for i in range(len(pools)):
            pool = pools[i]

            # Compute usable resources (batch respects reservation; high-prio can dip into it).
            if reserve_active and (prio == Priority.BATCH_PIPELINE):
                r_cpu, r_ram = _reserve_for_high_prio(pool)
                usable_cpu = max(0.0, avail_cpu[i] - r_cpu)
                usable_ram = max(0.0, avail_ram[i] - r_ram)
            else:
                usable_cpu = avail_cpu[i]
                usable_ram = avail_ram[i]

            if usable_cpu < 1e-6 or usable_ram < 1e-6:
                continue
            if usable_ram + 1e-9 < ram_need:
                continue

            cpu = min(cpu_cap, usable_cpu)
            if cpu < 1e-6:
                continue
            cpu = max(1.0, cpu)

            if _is_high_prio(prio):
                # For latency-sensitive work: pick MOST headroom.
                # Score: (usable_cpu, usable_ram) lexicographically.
                score = (usable_cpu, usable_ram)
                if best_score is None or score > best_score:
                    best_score = score
                    best_pool = i
                    best_cpu = cpu
            else:
                # For batch: pick BEST-FIT by RAM (pack tightly), tie-breaker by leftover CPU.
                leftover_ram = usable_ram - ram_need
                leftover_cpu = usable_cpu - cpu
                score = (leftover_ram, -leftover_cpu)  # minimize leftover_ram, then maximize leftover_cpu
                if best_score is None or score < best_score:
                    best_score = score
                    best_pool = i
                    best_cpu = cpu

        return best_pool, best_cpu

    def _dequeue_next_candidate(scheduled_pipelines):
        # Rotate within each priority queue; return (pipeline, op) for the first schedulable candidate.
        for pr in _prio_order():
            q = s.queues[pr]
            n = len(q)
            for _ in range(n):
                p = q.popleft()

                # Drop completed / hard-failed pipelines permanently.
                if _pipeline_done_or_hard_failed(p):
                    s.in_queue.discard(p.pipeline_id)
                    continue

                # Avoid scheduling multiple ops from the same pipeline in a single tick (fairness).
                if p.pipeline_id in scheduled_pipelines:
                    q.append(p)
                    continue

                op = _pick_ready_op(p)
                q.append(p)  # always rotate to maintain fairness
                if op is None:
                    continue

                return p, op
        return None, None

    # -------- Ingest new pipelines --------
    for p in pipelines:
        if p.pipeline_id in s.in_queue:
            continue
        # Enqueue by priority; will be dropped later if already successful/failed.
        s.queues[p.priority].append(p)
        s.in_queue.add(p.pipeline_id)

    # -------- Process results (update memory backoff and non-retryable failures) --------
    for r in results:
        if not r.failed():
            continue

        # Some simulators include pipeline_id on results; use it if present (optional).
        err_is_oom = _is_oom_error(getattr(r, "error", None))

        # Mark op(s) and update memory backoff on OOM
        for op in getattr(r, "ops", []) or []:
            opk = _op_key(op)

            if err_is_oom:
                # Increase memory factor for next retry; keep bounded.
                prev = s.op_mem_factor.get(opk, 1.15)
                s.op_mem_factor[opk] = min(8.0, max(prev * 1.5, prev + 0.25))

                # Remember last attempted RAM; next time, don't go lower.
                try:
                    s.op_last_ram[opk] = float(getattr(r, "ram", 0.0) or 0.0)
                except Exception:
                    pass
            else:
                # Non-OOM failure: do not retry this op; pipeline will be dropped when detected.
                s.op_nonretryable.add(opk)

    # Early exit if nothing changed
    if not pipelines and not results:
        return [], []

    # -------- Scheduling (no preemption in this incremental policy) --------
    suspensions = []
    assignments = []

    pools = s.executor.pools
    avail_cpu = [float(p.avail_cpu_pool) for p in pools]
    avail_ram = [float(p.avail_ram_pool) for p in pools]

    reserve_active = _has_high_prio_pressure()

    # At most 1 op per pipeline per tick => total assignments bounded by number of enqueued pipelines.
    scheduled_pipelines = set()
    max_iters = max(20, len(s.in_queue) * 3)

    for _ in range(max_iters):
        # Stop if no pool has any meaningful resources left.
        if max(avail_cpu) < 1.0 or max(avail_ram) < 0.25:
            break

        p, op = _dequeue_next_candidate(scheduled_pipelines)
        if p is None or op is None:
            break

        prio = p.priority
        ram_need = _ram_request_gb(op)
        cpu_cap = _cpu_cap_for_priority(prio)

        pool_id, cpu = _select_pool_for_op(
            prio=prio,
            ram_need=ram_need,
            cpu_cap=cpu_cap,
            avail_cpu=avail_cpu,
            avail_ram=avail_ram,
            pools=pools,
            reserve_active=reserve_active,
        )

        if pool_id is None:
            # Can't place now (insufficient headroom); keep pipeline in queue for later ticks.
            continue

        # Final clamp to physical pool maxima (defensive)
        cpu = min(cpu, float(pools[pool_id].max_cpu_pool))
        ram_need = min(ram_need, float(pools[pool_id].max_ram_pool))

        # If our local accounting disagrees, skip (should be rare).
        if avail_cpu[pool_id] + 1e-9 < cpu or avail_ram[pool_id] + 1e-9 < ram_need:
            continue

        assignment = Assignment(
            ops=[op],
            cpu=cpu,
            ram=ram_need,
            priority=prio,
            pool_id=pool_id,
            pipeline_id=p.pipeline_id,
        )
        assignments.append(assignment)

        # Local resource accounting for multi-assignment packing per tick.
        avail_cpu[pool_id] -= cpu
        avail_ram[pool_id] -= ram_need

        scheduled_pipelines.add(p.pipeline_id)

    return suspensions, assignments
