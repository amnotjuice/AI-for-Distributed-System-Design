# policy_key: scheduler_est_042
# reasoning_effort: low
# exp: estimation
# model: gpt-5.2-2025-12-11
# llm_cost: 0.051497
# generation_seconds: 41.02
# generated_at: 2026-04-10T10:19:38.501433
@register_scheduler_init(key="scheduler_est_042")
def scheduler_est_042_init(s):
    """Memory-aware, priority-biased scheduler with OOM-avoidance + bounded retries.

    Goals (aligned to weighted-latency score):
      - Strongly protect QUERY and INTERACTIVE latency via strict priority ordering.
      - Reduce failures (720s penalty) by using mem_peak estimates to avoid obvious OOMs
        and by retrying failed operators with increased RAM (backoff) a bounded number of times.
      - Avoid starvation by allowing limited aging for BATCH.

    High-level behavior each tick:
      1) Enqueue new pipelines by priority.
      2) Process results; on failure, bump per-operator RAM factor and retry (bounded).
      3) For each pool, greedily pack one operator per container while resources remain:
           - choose next pipeline by priority (QUERY > INTERACTIVE > BATCH w/ aging)
           - pick first ready operator (parents complete)
           - select pool only if estimated memory plausibly fits
           - size RAM using estimate * safety * retry_backoff (capped by pool max/avail)
           - size CPU with a priority-aware share (query gets more)
    """
    import collections

    s.q_query = collections.deque()
    s.q_interactive = collections.deque()
    s.q_batch = collections.deque()

    # Simple logical time for aging decisions (not simulator time; deterministic per scheduler call)
    s._tick = 0

    # Retry controls (kept conservative to avoid infinite churn)
    s.max_retries_per_op = 5

    # Backoff knobs
    s.mem_safety = 1.20          # guard against estimator under-shoot
    s.mem_backoff_mult = 1.60    # multiply RAM request after each failure

    # Aging knobs (helps ensure batch progress without hurting query too much)
    s.batch_aging_ticks = 40     # after this many ticks in queue, batch can be considered earlier


@register_scheduler(key="scheduler_est_042")
def scheduler_est_042_scheduler(s, results, pipelines):
    import math
    import collections

    s._tick += 1

    # ---- helpers (defined inside to respect "no imports at top-level") ----
    def _get_queue(priority):
        if priority == Priority.QUERY:
            return s.q_query
        if priority == Priority.INTERACTIVE:
            return s.q_interactive
        return s.q_batch

    def _pipeline_done_or_dead(p):
        st = p.runtime_status()
        # If successful, drop from our queues.
        if st.is_pipeline_successful():
            return True
        # If it has failures, we still keep it around because FAILED operators are retryable;
        # but if an operator exceeded max retries, we stop trying (pipeline likely stays failed).
        return False

    def _first_ready_op(p):
        st = p.runtime_status()
        ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        if not ops:
            return None
        # Simple heuristic: within a pipeline, prefer smaller estimated memory first
        # to reduce head-of-line blocking under tight RAM.
        def _op_key(op):
            est = None
            try:
                est = op.estimate.mem_peak_gb
            except Exception:
                est = None
            if est is None:
                est = float("inf")
            return est
        ops_sorted = sorted(ops, key=_op_key)
        return ops_sorted[0]

    def _op_retry(op):
        return getattr(op, "_sched_retry", 0)

    def _op_ram_factor(op):
        return getattr(op, "_sched_ram_factor", 1.0)

    def _bump_on_failure(op):
        # Treat any failure as potentially memory-related; this reduces repeated failures,
        # which are heavily penalized by the objective.
        r = _op_retry(op) + 1
        setattr(op, "_sched_retry", r)
        # Exponential-ish backoff on RAM sizing
        setattr(op, "_sched_ram_factor", _op_ram_factor(op) * s.mem_backoff_mult)

    def _est_mem(op):
        try:
            return op.estimate.mem_peak_gb
        except Exception:
            return None

    def _plausibly_fits_pool(op, pool):
        """Filter pools that are extremely unlikely to work, based on estimated mem."""
        est = _est_mem(op)
        if est is None:
            return True  # no estimator; don't over-filter
        # If estimate exceeds max pool RAM by a lot, likely impossible.
        # Allow some slack since estimator is noisy and may over-shoot.
        if est > pool.max_ram_pool * 1.35:
            return False
        return True

    def _choose_ram(op, pool, avail_ram):
        est = _est_mem(op)
        factor = _op_ram_factor(op)
        if est is None:
            # Conservative fallback: request a moderate chunk, but don't monopolize the pool.
            base = min(pool.max_ram_pool * 0.50, max(1.0, avail_ram * 0.60))
        else:
            base = est * s.mem_safety
        req = base * factor
        # Cap by pool and current availability
        req = min(req, pool.max_ram_pool, avail_ram)
        # Avoid pathological tiny allocations
        req = max(0.5, req)
        return req

    def _choose_cpu(priority, pool, avail_cpu):
        # Keep it simple: give more CPU to higher priority to reduce their latency.
        # Also avoid consuming the entire pool so we can pack multiple small ops.
        if avail_cpu <= 0:
            return 0
        if priority == Priority.QUERY:
            share = 0.75
        elif priority == Priority.INTERACTIVE:
            share = 0.60
        else:
            share = 0.45
        desired = max(1.0, math.floor(avail_cpu * share))
        return min(avail_cpu, desired)

    def _enqueue_pipeline(p):
        q = _get_queue(p.priority)
        if getattr(p, "_sched_enqueued", False):
            return
        setattr(p, "_sched_enqueued", True)
        setattr(p, "_sched_enqueue_tick", s._tick)
        q.append(p)

    def _aging_tick(p):
        return getattr(p, "_sched_enqueue_tick", s._tick)

    def _iter_queues_with_aging():
        """Priority order with limited batch aging."""
        # Strict priority for query/interactive, but allow aged batch to compete with interactive.
        yield s.q_query
        # If the head of batch is old enough, consider it before interactive (rare).
        if s.q_batch:
            head = s.q_batch[0]
            if (s._tick - _aging_tick(head)) >= s.batch_aging_ticks:
                yield s.q_batch
                yield s.q_interactive
                return
        yield s.q_interactive
        yield s.q_batch

    # ---- incorporate new pipelines ----
    for p in pipelines:
        _enqueue_pipeline(p)

    # ---- process results: failures -> bump RAM factor on involved ops ----
    for r in results:
        if r is None:
            continue
        if hasattr(r, "failed") and r.failed():
            # Bump backoff for each op in this failed container
            for op in getattr(r, "ops", []) or []:
                _bump_on_failure(op)

    # Early exit when nothing changed and no queued work
    if not pipelines and not results and (not s.q_query and not s.q_interactive and not s.q_batch):
        return [], []

    suspensions = []
    assignments = []

    # ---- scheduling loop: greedy pack per pool ----
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool
        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # Try to place multiple ops per pool per tick (one op per assignment/container).
        # This tends to improve throughput without creating giant containers that risk OOM.
        placed_something = True
        while placed_something:
            placed_something = False
            if avail_cpu <= 0 or avail_ram <= 0:
                break

            candidate = None
            candidate_op = None
            candidate_q = None

            # Pick next runnable pipeline by (priority + aging), keeping queue order.
            for q in _iter_queues_with_aging():
                if not q:
                    continue

                # Scan a small window to avoid being stuck behind a pipeline with no ready ops yet.
                # Keep window small to preserve FIFO-ish behavior and avoid scheduler overhead.
                scan_n = min(6, len(q))
                chosen_idx = None
                for i in range(scan_n):
                    p = q[i]
                    if _pipeline_done_or_dead(p):
                        continue
                    op = _first_ready_op(p)
                    if op is None:
                        continue
                    # Stop trying if this op exceeded retries; likely unrecoverable.
                    if _op_retry(op) > s.max_retries_per_op:
                        continue
                    # Pool feasibility pre-filter (based on estimate, not availability)
                    if not _plausibly_fits_pool(op, pool):
                        continue
                    # Availability check using estimated RAM request
                    ram_req = _choose_ram(op, pool, avail_ram)
                    if ram_req > avail_ram:
                        continue
                    # If estimator says it's bigger than current avail by a lot, skip to avoid OOM cascades.
                    est = _est_mem(op)
                    if est is not None and est * 0.90 > avail_ram:
                        continue

                    chosen_idx = i
                    candidate = p
                    candidate_op = op
                    candidate_q = q
                    break

                if candidate is not None:
                    # Rotate queue: move pipelines before chosen_idx to the back, then pop chosen
                    for _ in range(chosen_idx):
                        candidate_q.append(candidate_q.popleft())
                    candidate = candidate_q.popleft()
                    break

            if candidate is None or candidate_op is None:
                break

            # Size resources
            ram = _choose_ram(candidate_op, pool, avail_ram)
            cpu = _choose_cpu(candidate.priority, pool, avail_cpu)
            if cpu <= 0 or ram <= 0 or ram > avail_ram:
                # Couldn't place after all; requeue and stop attempting in this pool
                _get_queue(candidate.priority).appendleft(candidate)
                break

            # Create assignment for a single operator (more predictable RAM behavior)
            assignments.append(
                Assignment(
                    ops=[candidate_op],
                    cpu=cpu,
                    ram=ram,
                    priority=candidate.priority,
                    pool_id=pool_id,
                    pipeline_id=candidate.pipeline_id,
                )
            )

            avail_cpu -= cpu
            avail_ram -= ram
            placed_something = True

            # Requeue pipeline for subsequent operators
            _get_queue(candidate.priority).append(candidate)

    return suspensions, assignments
