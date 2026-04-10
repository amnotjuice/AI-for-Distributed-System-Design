# policy_key: scheduler_low_001
# reasoning_effort: low
# model: gpt-5.2-2025-12-11
# llm_cost: 0.000000
# generation_seconds: 45.60
# generated_at: 2026-04-09T20:18:01.997630
@register_scheduler_init(key="scheduler_low_001")
def scheduler_low_001_init(s):
    """
    Priority-weighted, OOM-adaptive FIFO scheduler.

    Goals aligned with objective:
      - Strongly protect QUERY and INTERACTIVE latency (they dominate score).
      - Avoid failures (each failure/incomplete pipeline costs 720s) by:
          * starting with conservative RAM for high priority
          * retrying failed ops with RAM backoff (OOM-aware)
      - Avoid starvation by aging batch slightly (soft boost after waiting).

    Policy summary:
      1) Maintain per-priority FIFO queues of pipelines.
      2) Always schedule ready ops from highest effective priority first.
      3) Allocate CPU shares by priority (query > interactive > batch) with caps.
      4) Allocate RAM from an adaptive per-op estimate; on OOM, increase estimate.
      5) Never drop pipelines proactively; retry failures up to a safe limit.
    """
    from collections import deque

    s.q_query = deque()
    s.q_interactive = deque()
    s.q_batch = deque()

    # Per-operator resource estimates and retry counters.
    # Keys are stable strings derived from (pipeline_id, op identity).
    s.op_ram_est = {}        # op_key -> ram_est
    s.op_fail_count = {}     # op_key -> count
    s.op_last_fail_was_oom = {}  # op_key -> bool

    # Per-pipeline arrival tick (for simple aging).
    s.pipeline_arrival_tick = {}  # pipeline_id -> tick
    s._tick = 0

    # Retry limits: keep high completion rate, but avoid infinite loops.
    s.max_retries_oom = 4
    s.max_retries_other = 2

    # RAM sizing knobs (fractions of pool max for initial guesses).
    s.init_ram_frac = {
        Priority.QUERY: 0.50,
        Priority.INTERACTIVE: 0.40,
        Priority.BATCH_PIPELINE: 0.25,
    }

    # CPU caps as fraction of pool max (avoid one op taking the entire pool).
    s.cpu_cap_frac = {
        Priority.QUERY: 0.80,
        Priority.INTERACTIVE: 0.65,
        Priority.BATCH_PIPELINE: 0.50,
    }

    # Minimum CPU to give any op (if available).
    s.min_cpu = 1.0

    # Aging: after this many ticks in queue, batch can be treated like interactive.
    s.batch_aging_ticks = 25


@register_scheduler(key="scheduler_low_001")
def scheduler_low_001_scheduler(s, results, pipelines):
    """
    Scheduler step:
      - Ingest new pipelines into priority queues.
      - Update per-op estimates based on failures (especially OOM).
      - For each pool, greedily assign one ready op at a time, preferring
        higher effective priority and respecting available CPU/RAM.
    """
    from collections import deque

    def _priority_queue(priority):
        if priority == Priority.QUERY:
            return s.q_query
        if priority == Priority.INTERACTIVE:
            return s.q_interactive
        return s.q_batch

    def _op_identity(op):
        # Try to create a stable identity for the operator.
        # Fallbacks keep this code robust to differing operator object shapes.
        if hasattr(op, "op_id"):
            return str(op.op_id)
        if hasattr(op, "operator_id"):
            return str(op.operator_id)
        if hasattr(op, "id"):
            return str(op.id)
        # Last resort: repr (may include memory addr in some implementations)
        return repr(op)

    def _op_key(pipeline_id, op):
        return f"{pipeline_id}:{_op_identity(op)}"

    def _is_oom_error(err):
        if not err:
            return False
        msg = str(err).lower()
        # Common OOM patterns; keep broad to avoid missing OOM-like signals.
        return ("oom" in msg) or ("out of memory" in msg) or ("out_of_memory" in msg) or ("memoryerror" in msg)

    def _effective_priority(pipeline):
        # Soft aging to avoid indefinite starvation of batch.
        pr = pipeline.priority
        if pr == Priority.BATCH_PIPELINE:
            at = s.pipeline_arrival_tick.get(pipeline.pipeline_id, s._tick)
            if (s._tick - at) >= s.batch_aging_ticks:
                return Priority.INTERACTIVE
        return pr

    def _pick_next_pipeline(pool_id):
        # Select next runnable pipeline by effective priority.
        # We do a bounded scan to maintain FIFO behavior while skipping blocked pipelines.
        scan_limit = 64

        # Construct an ordered list of queues by priority.
        # Note: effective priority can promote batch to interactive via aging.
        # We handle that by scanning in a fixed order but using effective priority checks.
        queues = [s.q_query, s.q_interactive, s.q_batch]

        best = None
        best_q = None
        best_idx = None
        best_rank = None  # smaller is better

        def rank(pr):
            if pr == Priority.QUERY:
                return 0
            if pr == Priority.INTERACTIVE:
                return 1
            return 2

        for q in queues:
            n = min(len(q), scan_limit)
            for i in range(n):
                p = q[i]
                st = p.runtime_status()

                # Drop successful pipelines from queues (they're done).
                if st.is_pipeline_successful():
                    continue

                # Must have at least one assignable op with parents complete.
                op_list = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
                if not op_list:
                    continue

                r = rank(_effective_priority(p))
                if best is None or r < best_rank:
                    best = p
                    best_q = q
                    best_idx = i
                    best_rank = r
                    if best_rank == 0:
                        # Can't beat QUERY; still might want to find another QUERY earlier,
                        # but FIFO within query queue is already preserved by index scan.
                        pass

        if best is None:
            return None, None
        # Remove chosen pipeline from its queue while preserving order.
        # deque doesn't support pop(i), so rotate.
        # We'll implement removal by rebuilding if needed; but to keep it light,
        # do rotate method for deques.
        if isinstance(best_q, deque):
            # Rotate left by best_idx, popleft, rotate back.
            best_q.rotate(-best_idx)
            chosen = best_q.popleft()
            best_q.rotate(best_idx)
            return chosen, best_q
        else:
            # Fallback for list-like
            chosen = best_q.pop(best_idx)
            return chosen, best_q

    def _initial_ram_guess(pool, priority):
        frac = s.init_ram_frac.get(priority, 0.30)
        return max(1.0, pool.max_ram_pool * frac)

    def _cpu_cap(pool, priority):
        frac = s.cpu_cap_frac.get(priority, 0.50)
        return max(s.min_cpu, pool.max_cpu_pool * frac)

    # Advance tick.
    s._tick += 1

    # Ingest new pipelines.
    for p in pipelines:
        if p.pipeline_id not in s.pipeline_arrival_tick:
            s.pipeline_arrival_tick[p.pipeline_id] = s._tick
        _priority_queue(p.priority).append(p)

    # Update estimates from results.
    # If an op fails with OOM, increase its RAM estimate for the next retry.
    for r in results:
        if not r or not getattr(r, "ops", None):
            continue

        # In this simulator, an ExecutionResult can include multiple ops;
        # update each op key with the observed failure signal.
        for op in r.ops:
            ok = _op_key(r.pipeline_id, op) if hasattr(r, "pipeline_id") else None
            if ok is None:
                # If pipeline_id isn't available on result, we still can key by container+op.
                ok = f"{getattr(r, 'container_id', 'c?')}:{_op_identity(op)}"

            if r.failed():
                s.op_fail_count[ok] = s.op_fail_count.get(ok, 0) + 1
                was_oom = _is_oom_error(getattr(r, "error", None))
                s.op_last_fail_was_oom[ok] = was_oom

                # Ensure we have a starting estimate; then back off.
                # Use the RAM that was attempted if available, else pool-based fallback later.
                prev = s.op_ram_est.get(ok, getattr(r, "ram", None))
                if prev is None:
                    prev = 1.0
                # OOM: aggressively increase; other failures: modest increase.
                mult = 2.0 if was_oom else 1.25
                s.op_ram_est[ok] = float(prev) * mult
            else:
                # On success, keep estimate as max of previous and used (helps future similar ops).
                used = getattr(r, "ram", None)
                if used is not None:
                    prev = s.op_ram_est.get(ok, 0.0)
                    s.op_ram_est[ok] = float(max(prev, used))

    # Early exit if nothing changed.
    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    # Helper: prune completed pipelines occasionally to avoid queue bloat.
    def _prune_queue(q):
        if not q:
            return
        # Bounded prune: scan a few elements from the front.
        for _ in range(min(len(q), 32)):
            p = q[0]
            st = p.runtime_status()
            if st.is_pipeline_successful():
                q.popleft()
            else:
                # Stop at first non-completed to preserve FIFO-ish ordering.
                break

    _prune_queue(s.q_query)
    _prune_queue(s.q_interactive)
    _prune_queue(s.q_batch)

    # Assign work pool-by-pool.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool

        # Keep filling while resources remain; place at most a few per tick per pool
        # to avoid pathological long loops.
        placed = 0
        place_limit = 16

        while placed < place_limit and avail_cpu >= s.min_cpu and avail_ram > 0:
            pipeline, _ = _pick_next_pipeline(pool_id)
            if pipeline is None:
                break

            st = pipeline.runtime_status()
            if st.is_pipeline_successful():
                # Already done; don't requeue.
                continue

            # Get one ready op (sequential within pipeline helps reduce contention and risk).
            op_list = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
            if not op_list:
                # Blocked; requeue to same priority queue.
                _priority_queue(pipeline.priority).append(pipeline)
                continue

            op = op_list[0]
            ok = _op_key(pipeline.pipeline_id, op)

            # Check retry budget.
            fails = s.op_fail_count.get(ok, 0)
            was_oom = s.op_last_fail_was_oom.get(ok, False)
            if was_oom and fails > s.max_retries_oom:
                # Too many OOM retries; requeue pipeline (do not drop),
                # but deprioritize implicitly by placing it back and moving on.
                _priority_queue(pipeline.priority).append(pipeline)
                break
            if (not was_oom) and fails > s.max_retries_other:
                _priority_queue(pipeline.priority).append(pipeline)
                break

            eff_pr = _effective_priority(pipeline)

            # Decide CPU.
            cpu_cap = _cpu_cap(pool, eff_pr)
            cpu = min(avail_cpu, cpu_cap)
            if cpu < s.min_cpu:
                # Not enough CPU headroom; requeue.
                _priority_queue(pipeline.priority).append(pipeline)
                break

            # Decide RAM (estimate -> clamp to pool/availability).
            ram_est = s.op_ram_est.get(ok, None)
            if ram_est is None:
                ram_est = _initial_ram_guess(pool, eff_pr)

            # Clamp to pool max and available RAM; ensure at least 1.0.
            ram = max(1.0, min(float(ram_est), float(pool.max_ram_pool), float(avail_ram)))

            # If RAM is too tiny relative to estimate (because pool is tight),
            # avoid launching likely-to-OOM high priority ops; instead, requeue and try other work.
            # This reduces failure penalty (720s) which dominates the score.
            if eff_pr in (Priority.QUERY, Priority.INTERACTIVE):
                if ram < min(float(ram_est), float(pool.max_ram_pool)) * 0.60 and avail_ram < float(ram_est):
                    _priority_queue(pipeline.priority).append(pipeline)
                    # Can't fit this op safely now; don't spin forever.
                    break

            assignment = Assignment(
                ops=op_list,
                cpu=cpu,
                ram=ram,
                priority=pipeline.priority,
                pool_id=pool_id,
                pipeline_id=pipeline.pipeline_id,
            )
            assignments.append(assignment)

            # Update local available resources for further packing in this pool.
            avail_cpu -= cpu
            avail_ram -= ram
            placed += 1

            # Requeue pipeline so subsequent ops can be scheduled later (keeps throughput).
            _priority_queue(pipeline.priority).append(pipeline)

    return suspensions, assignments
