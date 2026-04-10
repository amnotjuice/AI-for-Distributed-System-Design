# policy_key: scheduler_est_025
# reasoning_effort: low
# exp: estimation
# model: gpt-5.2-2025-12-11
# llm_cost: 0.043652
# generation_seconds: 56.02
# generated_at: 2026-04-10T10:04:47.670257
@register_scheduler_init(key="scheduler_est_025")
def scheduler_est_025_init(s):
    """Memory-aware, priority-weighted scheduler.

    Design goals aligned to score:
      - Strongly protect QUERY/INTERACTIVE latency via strict priority ordering.
      - Reduce 720s penalties by avoiding OOM cascades using mem_peak_gb hints and
        by retrying failed operators with increased RAM (bounded, with retry caps).
      - Avoid starving BATCH by allowing it to use leftover capacity, but keep a
        small reserve in each pool to absorb sudden high-priority arrivals.

    Key ideas:
      1) Three FIFO queues by priority (QUERY > INTERACTIVE > BATCH).
      2) Within each priority, pick the next runnable op with the smallest
         estimated memory that fits the pool (best-fit-ish).
      3) RAM sizing uses estimator * safety_multiplier; on failures, bump multiplier.
      4) Conservative reservation when scheduling batch: keep headroom for spikes.
    """
    s.q_query = []
    s.q_interactive = []
    s.q_batch = []

    # Track pipelines by id for quick lookup from ExecutionResult.
    s.pipelines_by_id = {}

    # Per-operator RAM multiplier and retry counts (keyed by operator object).
    s.op_ram_mult = {}     # op -> float
    s.op_retries = {}      # op -> int

    # Per-pipeline failure counters to avoid infinite loops.
    s.pipeline_failures = {}  # pipeline_id -> int

    # Tunables (kept small/simple for incremental improvement over FIFO).
    s.base_ram_mult = 1.20          # estimator safety factor
    s.fail_ram_mult_bump = 1.50     # multiply on each failure (OOM or unknown)
    s.max_op_retries = 3            # cap per operator
    s.max_pipeline_failures = 8     # cap per pipeline (across ops)

    # Reserve for sudden high-priority arrivals (applied only when scheduling batch).
    s.batch_reserve_ram_frac = 0.20
    s.batch_reserve_cpu_frac = 0.20


def _prio_rank(priority):
    # Smaller is higher priority.
    if priority == Priority.QUERY:
        return 0
    if priority == Priority.INTERACTIVE:
        return 1
    return 2


def _enqueue_pipeline(s, p, front=False):
    if p.priority == Priority.QUERY:
        (s.q_query.insert(0, p) if front else s.q_query.append(p))
    elif p.priority == Priority.INTERACTIVE:
        (s.q_interactive.insert(0, p) if front else s.q_interactive.append(p))
    else:
        (s.q_batch.insert(0, p) if front else s.q_batch.append(p))


def _all_queues(s):
    return [s.q_query, s.q_interactive, s.q_batch]


def _pipeline_done_or_hopeless(s, pipeline):
    status = pipeline.runtime_status()
    if status.is_pipeline_successful():
        return True
    # If a pipeline has accumulated too many failures, stop trying.
    pid = pipeline.pipeline_id
    if s.pipeline_failures.get(pid, 0) >= s.max_pipeline_failures:
        return True
    return False


def _next_runnable_op(pipeline):
    status = pipeline.runtime_status()
    # Only schedule ops whose parents are complete.
    op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
    if not op_list:
        return None
    return op_list[0]


def _est_mem_gb(op):
    est = getattr(op, "estimate", None)
    if est is None:
        return None
    return getattr(est, "mem_peak_gb", None)


def _ram_request_for_op(s, op, pool):
    # Estimate may be None; treat as "unknown" and be conservative.
    est = _est_mem_gb(op)
    mult = s.op_ram_mult.get(op, s.base_ram_mult)

    # Always bound to pool max RAM.
    pool_max = pool.max_ram_pool

    if est is None:
        # Unknown memory: pick a moderate chunk to reduce OOM likelihood
        # without monopolizing the pool.
        req = 0.60 * pool_max
    else:
        # Ensure non-trivial allocation; estimator can be noisy/near-zero.
        req = max(0.25, est * mult)

    # Hard bounds.
    if req > pool_max:
        req = pool_max
    if req < 0.25:
        req = 0.25
    return req


def _cpu_request_for_prio(priority, pool, avail_cpu):
    # CPU sizing: give more to high-priority for latency, but avoid full monopolization.
    if priority == Priority.QUERY:
        target = 0.75 * pool.max_cpu_pool
    elif priority == Priority.INTERACTIVE:
        target = 0.50 * pool.max_cpu_pool
    else:
        target = 0.33 * pool.max_cpu_pool

    cpu = min(avail_cpu, max(1.0, target))
    # Avoid allocating fractional CPUs below 1 if the simulator expects integers;
    # but keep as float if it supports it.
    if cpu < 1.0:
        cpu = 1.0
    return cpu


def _pick_best_candidate(s, pool, candidates):
    """Pick candidate op that fits RAM in this pool; prefer higher priority and smaller est mem."""
    best = None
    best_key = None

    for (pipeline, op) in candidates:
        if op is None:
            continue

        ram_req = _ram_request_for_op(s, op, pool)
        if ram_req > pool.avail_ram_pool:
            continue

        # If we have an estimate, also avoid assignments that are obviously impossible.
        est = _est_mem_gb(op)
        if est is not None and est > pool.max_ram_pool:
            continue

        # Prefer smaller estimated memory within the same priority to pack better.
        # Unknown estimate is treated as large to avoid blocking small known ops.
        est_for_sort = est if est is not None else (pool.max_ram_pool * 10.0)

        key = (_prio_rank(pipeline.priority), est_for_sort, pipeline.pipeline_id)
        if best is None or key < best_key:
            best = (pipeline, op, ram_req)
            best_key = key

    return best  # (pipeline, op, ram_req) or None


@register_scheduler(key="scheduler_est_025")
def scheduler_est_025(s, results, pipelines):
    """
    Scheduler tick:
      - Ingest new pipelines into priority queues.
      - Process results: on failures, bump RAM multiplier and retry (with caps).
      - For each pool, greedily assign as many runnable ops as resources allow:
          * always serve QUERY then INTERACTIVE then BATCH
          * for BATCH, keep a small resource reserve (headroom)
          * choose op that fits and has smallest estimated memory (packing)
    """
    # Add arrivals.
    for p in pipelines:
        s.pipelines_by_id[p.pipeline_id] = p
        _enqueue_pipeline(s, p, front=False)

    # Process results: handle failures by retrying with more RAM.
    for r in results:
        if getattr(r, "failed", None) is not None and r.failed():
            pid = r.pipeline_id if hasattr(r, "pipeline_id") else None
            # Try to locate pipeline.
            pipeline = None
            if pid is not None:
                pipeline = s.pipelines_by_id.get(pid, None)
            if pipeline is None:
                # Fallback: try to find by matching stored pipeline ids in queues.
                for q in _all_queues(s):
                    for pp in q:
                        if getattr(pp, "pipeline_id", None) == pid:
                            pipeline = pp
                            break
                    if pipeline is not None:
                        break

            # Update per-pipeline failure count (even if we couldn't find it, be safe).
            if pipeline is not None:
                s.pipeline_failures[pipeline.pipeline_id] = s.pipeline_failures.get(pipeline.pipeline_id, 0) + 1

            # Bump RAM multiplier for the failed ops and increment retries.
            for op in getattr(r, "ops", []) or []:
                s.op_retries[op] = s.op_retries.get(op, 0) + 1
                curr = s.op_ram_mult.get(op, s.base_ram_mult)
                s.op_ram_mult[op] = min(curr * s.fail_ram_mult_bump, 8.0)

            # Re-enqueue pipeline near the front for high priority to reduce tail latency.
            if pipeline is not None and not _pipeline_done_or_hopeless(s, pipeline):
                front = pipeline.priority in (Priority.QUERY, Priority.INTERACTIVE)
                _enqueue_pipeline(s, pipeline, front=front)

    # Early exit if nothing to do.
    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    # Garbage-collect completed/hopeless pipelines from queues by filtering each tick.
    def _filter_queue(q):
        kept = []
        for p in q:
            if p is None:
                continue
            if _pipeline_done_or_hopeless(s, p):
                continue
            kept.append(p)
        return kept

    s.q_query = _filter_queue(s.q_query)
    s.q_interactive = _filter_queue(s.q_interactive)
    s.q_batch = _filter_queue(s.q_batch)

    # Scheduling: per pool, greedily assign.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]

        # We will recompute availability as we assign.
        while True:
            avail_cpu = pool.avail_cpu_pool
            avail_ram = pool.avail_ram_pool
            if avail_cpu <= 0 or avail_ram <= 0:
                break

            # Build candidate list from the heads of queues (small lookahead).
            # Lookahead reduces head-of-line blocking when the first pipeline isn't runnable.
            candidates = []
            lookahead = 6

            for q in (s.q_query, s.q_interactive, s.q_batch):
                for i in range(min(lookahead, len(q))):
                    p = q[i]
                    if p is None or _pipeline_done_or_hopeless(s, p):
                        continue
                    op = _next_runnable_op(p)
                    if op is None:
                        continue

                    # If operator exceeded retry cap, stop trying this pipeline (avoid infinite churn).
                    if s.op_retries.get(op, 0) >= s.max_op_retries:
                        s.pipeline_failures[p.pipeline_id] = s.pipeline_failures.get(p.pipeline_id, 0) + 1
                        continue

                    candidates.append((p, op))

            if not candidates:
                break

            # If the best candidate is batch, enforce reserve headroom.
            # Implement by temporarily shrinking "effective" available resources for batch selection.
            # (We still allow batch if there is plenty beyond the reserve.)
            effective_pool = pool
            best = _pick_best_candidate(s, effective_pool, candidates)

            if best is None:
                break

            pipeline, op, ram_req = best
            prio = pipeline.priority

            # Apply batch headroom reservation.
            if prio == Priority.BATCH_PIPELINE:
                reserve_ram = s.batch_reserve_ram_frac * pool.max_ram_pool
                reserve_cpu = s.batch_reserve_cpu_frac * pool.max_cpu_pool
                if pool.avail_ram_pool - ram_req < reserve_ram or pool.avail_cpu_pool - 1.0 < reserve_cpu:
                    # If batch would eat into reserve, only schedule it if no higher-priority runnable ops fit.
                    higher_candidates = [(p, o) for (p, o) in candidates if p.priority != Priority.BATCH_PIPELINE]
                    higher_best = _pick_best_candidate(s, pool, higher_candidates)
                    if higher_best is not None:
                        pipeline, op, ram_req = higher_best
                        prio = pipeline.priority
                    else:
                        # No higher-priority op fits; allow batch anyway to avoid starvation.
                        pass

            cpu_req = _cpu_request_for_prio(prio, pool, pool.avail_cpu_pool)
            if cpu_req > pool.avail_cpu_pool or ram_req > pool.avail_ram_pool:
                break

            assignment = Assignment(
                ops=[op],
                cpu=cpu_req,
                ram=ram_req,
                priority=prio,
                pool_id=pool_id,
                pipeline_id=pipeline.pipeline_id,
            )
            assignments.append(assignment)

            # Remove the pipeline from its queue once we scheduled one op from it,
            # to provide fairness among pipelines of the same priority.
            q = s.q_query if prio == Priority.QUERY else (s.q_interactive if prio == Priority.INTERACTIVE else s.q_batch)
            removed = False
            for idx, pp in enumerate(q):
                if pp.pipeline_id == pipeline.pipeline_id:
                    q.pop(idx)
                    removed = True
                    break
            if removed and not _pipeline_done_or_hopeless(s, pipeline):
                # Reappend to tail for round-robin fairness within priority.
                _enqueue_pipeline(s, pipeline, front=False)

            # Continue loop: greedily fill remaining capacity.

    return suspensions, assignments
