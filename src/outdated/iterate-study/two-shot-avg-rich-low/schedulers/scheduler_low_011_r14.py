# source: scheduler_low_011.py
# model: gpt-5.2-2025-12-11
# effort: low
# feedback: rich
# iteration: r14
@register_scheduler_init(key="scheduler_low_011_r14")
def scheduler_low_011_r14_init(s):
    """Iteration 2: Latency-focused priority scheduling with basic fairness + packing.

    Changes vs the previous attempt (small, targeted fixes):
    - Remove the "interactive must run only on pool 0" behavior that caused starvation.
    - Pack multiple operators per pool per tick (instead of max 1) to reduce queueing latency.
    - Keep strict preference for high priority (query/interactive) but add aging-based escape hatches
      so interactive/batch can make progress under continuous query arrivals.
    - Add a simple high-priority reserve: batch cannot consume the last slice of resources if high-priority
      work is ready, protecting tail latency.
    - Keep OOM-aware RAM backoff, but make it actually retryable by not dropping pipelines just because
      they have FAILED ops (we only drop on non-retriable failures).
    """
    # FIFO queues per priority, with de-dup so each pipeline appears at most once.
    s.waiting_queues = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }
    s.queued_ids = {
        Priority.QUERY: set(),
        Priority.INTERACTIVE: set(),
        Priority.BATCH_PIPELINE: set(),
    }

    # Simple tick counter for aging decisions.
    s.tick = 0
    s.first_seen_tick = {}        # pipeline_id -> tick
    s.last_scheduled_tick = {}    # pipeline_id -> tick

    # Resource hints per operator (for OOM retry).
    # key: (pipeline_id, op_id) -> {"ram": float, "cpu": float}
    s.op_hints = {}
    s.op_attempts = {}
    s.max_retries_per_op = 3

    # Track whether a failed operator is retriable (OOM-like) vs fatal.
    s.retriable_ops = set()  # set of (pipeline_id, op_id)
    s.fatal_ops = set()      # set of (pipeline_id, op_id)

    # Map operator object id back to pipeline_id (best-effort correlation for ExecutionResult).
    s.opid_to_pid = {}

    # Packing / scanning knobs
    s.max_assignments_per_pool_per_tick = 8
    s.max_scan_per_queue = 32

    # Aging thresholds (in scheduler ticks) to prevent starvation under heavy high-priority load.
    s.interactive_aging_boost = 20
    s.batch_min_age_to_run_when_high_ready = 30

    # Reserve a slice of each pool for high-priority work (only enforced against batch).
    s.reserve_frac_cpu = 0.20
    s.reserve_frac_ram = 0.20

    # Target per-op allocations (small allocations -> more concurrency -> less queueing)
    # Caps keep single operators from grabbing a whole pool when not necessary.
    s.targets = {
        Priority.QUERY: {"cpu_frac": 0.25, "ram_frac": 0.12, "cpu_cap": 4.0},
        Priority.INTERACTIVE: {"cpu_frac": 0.25, "ram_frac": 0.15, "cpu_cap": 4.0},
        Priority.BATCH_PIPELINE: {"cpu_frac": 0.50, "ram_frac": 0.25, "cpu_cap": 8.0},
    }


def _is_oom_error(err):
    if err is None:
        return False
    msg = str(err).lower()
    return ("oom" in msg) or ("out of memory" in msg) or ("memoryerror" in msg)


def _op_key(pipeline_id, op):
    return (pipeline_id, id(op))


def _priority_order():
    # Highest to lowest base priority
    return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]


def _priority_base_score(pr):
    # Keep query slightly above interactive; batch much lower.
    if pr == Priority.QUERY:
        return 100
    if pr == Priority.INTERACTIVE:
        return 90
    return 10


def _pipeline_drop_or_keep(s, pipeline):
    """Return True if pipeline should be dropped from queues (completed or fatal), else False."""
    status = pipeline.runtime_status()
    if status.is_pipeline_successful():
        return True

    # If there are FAILED ops, only keep the pipeline if those FAILED ops are retriable (OOM) and within retry budget.
    failed_count = status.state_counts.get(OperatorState.FAILED, 0)
    if failed_count <= 0:
        return False

    failed_ops = status.get_ops([OperatorState.FAILED], require_parents_complete=False) or []
    if not failed_ops:
        # Conservative: if status says FAILED but we can't enumerate, drop to avoid infinite loops.
        return True

    for op in failed_ops:
        ok = _op_key(pipeline.pipeline_id, op)
        if ok in s.fatal_ops:
            return True
        if ok not in s.retriable_ops:
            return True
        if int(s.op_attempts.get(ok, 0)) > int(s.max_retries_per_op):
            return True

    # All failed ops appear retriable within budget.
    return False


def _get_first_assignable_op(pipeline):
    status = pipeline.runtime_status()
    ops = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True) or []
    if not ops:
        return None
    return ops[0]


def _queue_remove_at(q, idx):
    # list pop at index
    return q.pop(idx)


def _dequeue_best_candidate(s, pr, scan_limit):
    """Pick and remove the best (highest score) pipeline from a priority queue that has a ready op.

    Returns: (pipeline, op) or (None, None)
    """
    q = s.waiting_queues[pr]
    if not q:
        return None, None

    best_idx = None
    best_score = None
    best_op = None

    n = min(len(q), scan_limit)
    for i in range(n):
        p = q[i]

        # Drop completed/fatal pipelines eagerly.
        if _pipeline_drop_or_keep(s, p):
            # Remove from queue and queued_ids
            s.queued_ids[pr].discard(p.pipeline_id)
            _queue_remove_at(q, i)
            return _dequeue_best_candidate(s, pr, scan_limit)  # restart after mutation

        op = _get_first_assignable_op(p)
        if op is None:
            continue

        pid = p.pipeline_id
        first = int(s.first_seen_tick.get(pid, s.tick))
        last = int(s.last_scheduled_tick.get(pid, first))
        age = max(0, int(s.tick) - last)

        score = _priority_base_score(pr) + age
        # Give interactive extra help to prevent starvation under continuous query load.
        if pr == Priority.INTERACTIVE:
            score += int(s.interactive_aging_boost)

        if (best_score is None) or (score > best_score):
            best_score = score
            best_idx = i
            best_op = op

    if best_idx is None:
        return None, None

    p = _queue_remove_at(q, best_idx)
    s.queued_ids[pr].discard(p.pipeline_id)
    return p, best_op


def _high_priority_ready_exists(s):
    """Best-effort: check if there's any ready op in query or interactive queues (limited scan)."""
    for pr in (Priority.QUERY, Priority.INTERACTIVE):
        q = s.waiting_queues[pr]
        n = min(len(q), 8)
        for i in range(n):
            p = q[i]
            if _pipeline_drop_or_keep(s, p):
                continue
            if _get_first_assignable_op(p) is not None:
                return True
    return False


def _compute_request(s, pr, pool, avail_cpu, avail_ram, pipeline_id, op):
    """Compute cpu/ram request using small targets + OOM hints, capped by availability."""
    t = s.targets.get(pr, {"cpu_frac": 1.0, "ram_frac": 1.0, "cpu_cap": 9999.0})
    # Targets derived from pool max, then capped.
    cpu_t = max(1.0, float(pool.max_cpu_pool) * float(t["cpu_frac"]))
    cpu_t = min(cpu_t, float(t["cpu_cap"]))

    ram_t = max(1.0, float(pool.max_ram_pool) * float(t["ram_frac"]))

    ok = _op_key(pipeline_id, op)
    hint = s.op_hints.get(ok)
    if hint:
        cpu_t = max(cpu_t, float(hint.get("cpu", cpu_t)))
        ram_t = max(ram_t, float(hint.get("ram", ram_t)))

    # Cap by currently available.
    cpu = min(cpu_t, float(avail_cpu))
    ram = min(ram_t, float(avail_ram))

    # Ensure positive.
    cpu = max(1.0, cpu)
    ram = max(1.0, ram)

    # If we capped to availability below the hinted requirement, the caller will treat it as "doesn't fit".
    return cpu, ram


@register_scheduler(key="scheduler_low_011_r14")
def scheduler_low_011_r14(s, results, pipelines):
    """
    Priority-first, aging-aware, work-conserving packer.

    Key behavior:
    - Always try to keep pools busy by packing multiple ops per tick.
    - Protect query/interactive latency by reserving headroom against batch when high-priority work is ready.
    - Prevent starvation by allowing batch to run once it has aged enough, and by boosting interactive scores.
    - Retry OOM-like failures with exponential RAM backoff (bounded).
    """
    s.tick += 1

    # Enqueue new pipelines with de-dup.
    for p in pipelines:
        pr = p.priority if p.priority in s.waiting_queues else Priority.BATCH_PIPELINE
        pid = p.pipeline_id
        if pid not in s.first_seen_tick:
            s.first_seen_tick[pid] = int(s.tick)
        if pid not in s.last_scheduled_tick:
            s.last_scheduled_tick[pid] = int(s.first_seen_tick[pid])

        if pid not in s.queued_ids[pr]:
            s.waiting_queues[pr].append(p)
            s.queued_ids[pr].add(pid)

    # Process results: learn OOM hints and mark retriable/fatal ops.
    for r in results:
        # Determine failure.
        failed = False
        try:
            failed = bool(r.failed())
        except Exception:
            failed = getattr(r, "error", None) is not None

        if not failed:
            continue

        ops = getattr(r, "ops", None) or []
        err = getattr(r, "error", None)

        for op in ops:
            pid = getattr(r, "pipeline_id", None)
            if pid is None:
                pid = s.opid_to_pid.get(id(op))
            if pid is None:
                continue

            ok = _op_key(pid, op)

            if _is_oom_error(err):
                # Mark retriable and increase RAM hint (exponential backoff).
                s.retriable_ops.add(ok)
                s.fatal_ops.discard(ok)

                prev_hint = s.op_hints.get(ok, {})
                prev_ram = float(prev_hint.get("ram", 0.0) or 0.0)
                prev_cpu = float(prev_hint.get("cpu", 0.0) or 0.0)

                # Use observed allocation as baseline if available.
                observed_ram = float(getattr(r, "ram", 0.0) or 0.0)
                observed_cpu = float(getattr(r, "cpu", 0.0) or 0.0)

                baseline_ram = max(prev_ram, observed_ram, 1.0)
                new_ram = baseline_ram * 2.0

                baseline_cpu = max(prev_cpu, observed_cpu, 1.0)

                s.op_hints[ok] = {"ram": new_ram, "cpu": baseline_cpu}
                s.op_attempts[ok] = int(s.op_attempts.get(ok, 0)) + 1
            else:
                # Non-OOM failures are treated as fatal for this operator.
                s.fatal_ops.add(ok)
                s.retriable_ops.discard(ok)

    # Early exit if nothing changed.
    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    # For each pool, pack multiple assignments while respecting local availability.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)
        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        reserve_cpu = float(pool.max_cpu_pool) * float(s.reserve_frac_cpu)
        reserve_ram = float(pool.max_ram_pool) * float(s.reserve_frac_ram)

        made = 0
        while made < int(s.max_assignments_per_pool_per_tick) and avail_cpu > 0 and avail_ram > 0:
            high_ready = _high_priority_ready_exists(s)

            chosen_p = None
            chosen_op = None
            chosen_pr = None

            # Try to pick best candidate across priorities using scores + aging.
            # Order matters only as a tie-breaker; we primarily score by base+age.
            best = None  # (score, pr, p, op)
            for pr in _priority_order():
                # If high work is ready, avoid selecting batch unless it has aged sufficiently.
                if high_ready and pr == Priority.BATCH_PIPELINE:
                    # Look at an approximate batch age from the head items; if too young, skip trying batch.
                    q = s.waiting_queues[pr]
                    approx_old_enough = False
                    n = min(len(q), 8)
                    for i in range(n):
                        p = q[i]
                        if _pipeline_drop_or_keep(s, p):
                            continue
                        pid = p.pipeline_id
                        first = int(s.first_seen_tick.get(pid, s.tick))
                        last = int(s.last_scheduled_tick.get(pid, first))
                        age = max(0, int(s.tick) - last)
                        if age >= int(s.batch_min_age_to_run_when_high_ready):
                            approx_old_enough = True
                            break
                    if not approx_old_enough:
                        continue

                p, op = _dequeue_best_candidate(s, pr, int(s.max_scan_per_queue))
                if p is None:
                    continue

                # Compute score of this candidate (same logic used in selection, recomputed here).
                pid = p.pipeline_id
                first = int(s.first_seen_tick.get(pid, s.tick))
                last = int(s.last_scheduled_tick.get(pid, first))
                age = max(0, int(s.tick) - last)
                score = _priority_base_score(pr) + age
                if pr == Priority.INTERACTIVE:
                    score += int(s.interactive_aging_boost)

                # Keep best; losers must be put back (since we dequeued them).
                if (best is None) or (score > best[0]):
                    # Put back previous best (if any)
                    if best is not None:
                        _, bpr, bp, _ = best
                        if bp.pipeline_id not in s.queued_ids[bpr]:
                            s.waiting_queues[bpr].append(bp)
                            s.queued_ids[bpr].add(bp.pipeline_id)
                    best = (score, pr, p, op)
                else:
                    # Put back this candidate
                    if p.pipeline_id not in s.queued_ids[pr]:
                        s.waiting_queues[pr].append(p)
                        s.queued_ids[pr].add(p.pipeline_id)

            if best is None:
                break

            _, chosen_pr, chosen_p, chosen_op = best
            pid = chosen_p.pipeline_id

            # Correlate op->pipeline for future result processing.
            s.opid_to_pid[id(chosen_op)] = pid

            # Compute request (targets + hints), and verify it fits.
            cpu, ram = _compute_request(s, chosen_pr, pool, avail_cpu, avail_ram, pid, chosen_op)

            # If this doesn't fit, put pipeline back and stop trying to pack further in this pool this tick.
            # (We avoid complex binpacking here to keep policy robust and deterministic.)
            if cpu > avail_cpu or ram > avail_ram:
                if pid not in s.queued_ids[chosen_pr]:
                    s.waiting_queues[chosen_pr].append(chosen_p)
                    s.queued_ids[chosen_pr].add(pid)
                break

            # Enforce reserve against batch when high-priority is ready.
            if chosen_pr == Priority.BATCH_PIPELINE and high_ready:
                if (avail_cpu - cpu) < reserve_cpu or (avail_ram - ram) < reserve_ram:
                    # Put batch back and try again but without batch (by breaking this packing loop for simplicity).
                    if pid not in s.queued_ids[chosen_pr]:
                        s.waiting_queues[chosen_pr].append(chosen_p)
                        s.queued_ids[chosen_pr].add(pid)
                    break

            # Make assignment (one op per container).
            assignments.append(
                Assignment(
                    ops=[chosen_op],
                    cpu=cpu,
                    ram=ram,
                    priority=chosen_pr,
                    pool_id=pool_id,
                    pipeline_id=pid,
                )
            )

            # Update local availability.
            avail_cpu -= float(cpu)
            avail_ram -= float(ram)
            made += 1

            # Update scheduling age.
            s.last_scheduled_tick[pid] = int(s.tick)

            # Re-enqueue pipeline (single entry) so later ops can run.
            if not _pipeline_drop_or_keep(s, chosen_p):
                if pid not in s.queued_ids[chosen_pr]:
                    s.waiting_queues[chosen_pr].append(chosen_p)
                    s.queued_ids[chosen_pr].add(pid)

    return suspensions, assignments