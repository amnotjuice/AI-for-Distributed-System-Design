# policy_key: scheduler_est_009
# reasoning_effort: low
# model: gpt-5.2-2025-12-11
# llm_cost: 0.038583
# generation_seconds: 45.98
# generated_at: 2026-04-03T01:10:24.053316
@register_scheduler_init(key="scheduler_est_009")
def scheduler_est_009_init(s):
    """Priority-aware, resource-sane scheduler (small step up from naive FIFO).

    Improvements over naive:
      - Priority ordering: QUERY > INTERACTIVE > BATCH_PIPELINE
      - Avoids "give all resources to one op" by right-sizing CPU/RAM per op
      - Packs multiple ops per pool per tick when resources allow
      - Simple OOM-reactive RAM bumping using operator memory estimate hints
    """
    # Per-priority pipeline queues (round-robin within each priority).
    s.waiting = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }

    # Per-operator RAM bump factor after failures (esp. OOM). Keyed by (pipeline_id, op_key).
    s.op_ram_bump = {}

    # Small constant to avoid zero-resource assignments.
    s.min_cpu = 0.5
    s.min_ram_gb = 0.25

    # Controls for estimate usage and backoff on failure.
    s.est_safety = 1.05        # allocate close to estimate (aggressive), rely on retries
    s.fail_bump = 1.5          # multiply RAM after a failure/OOM
    s.max_bump = 16.0          # cap runaway bumps


def _priority_order():
    return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]


def _op_key(op):
    # Best-effort stable identifier across retries.
    for attr in ("op_id", "operator_id", "id", "name"):
        if hasattr(op, attr):
            return getattr(op, attr)
    return id(op)


def _is_oom_error(err):
    if err is None:
        return False
    try:
        msg = str(err).lower()
    except Exception:
        return False
    return ("oom" in msg) or ("out of memory" in msg) or ("cuda out of memory" in msg)


def _estimate_peak_gb(op):
    # Estimator interface: op.estimate.mem_peak_gb may exist or be None.
    est = None
    if hasattr(op, "estimate") and getattr(op, "estimate") is not None:
        est = getattr(getattr(op, "estimate"), "mem_peak_gb", None)
    if est is None:
        return None
    try:
        est = float(est)
    except Exception:
        return None
    if est <= 0:
        return None
    return est


def _target_cpu_for_priority(pool, priority):
    # Keep it simple: cap per-op CPU to improve latency for high-priority without
    # letting a single op monopolize the pool.
    max_cpu = pool.max_cpu_pool
    if priority == Priority.QUERY:
        return max(s.min_cpu, 0.5 * max_cpu)
    if priority == Priority.INTERACTIVE:
        return max(s.min_cpu, 0.35 * max_cpu)
    return max(s.min_cpu, 0.25 * max_cpu)


def _target_ram_for_op(s, pool, pipeline_id, op, priority):
    # Use estimate if present; otherwise choose a small conservative default.
    est = _estimate_peak_gb(op)
    if est is None:
        # Unknown: pick a modest default that avoids excessive fragmentation.
        base = 1.0 if priority in (Priority.QUERY, Priority.INTERACTIVE) else 2.0
    else:
        base = est * s.est_safety

    bump = s.op_ram_bump.get((pipeline_id, _op_key(op)), 1.0)
    need = base * bump

    # Clamp to pool max; also enforce small minimum.
    need = max(s.min_ram_gb, min(need, pool.max_ram_pool))
    return need


def _enqueue_pipeline(s, p):
    # Unknown priorities fall back to batch queue.
    pr = p.priority if p.priority in s.waiting else Priority.BATCH_PIPELINE
    s.waiting[pr].append(p)


@register_scheduler(key="scheduler_est_009")
def scheduler_est_009(s, results, pipelines):
    """
    Scheduler step:
      - Ingest new pipelines into per-priority queues.
      - On failures, bump RAM for the failed op(s) (OOM bumps more aggressively).
      - For each pool, greedily assign ready operators from highest priority queues,
        packing multiple operators while pool resources remain.
    """
    # Ingest new pipelines.
    for p in pipelines:
        _enqueue_pipeline(s, p)

    # Process results: bump RAM on failure so retries can succeed.
    # Note: We don't attempt preemption here (small improvement step).
    for r in results:
        if hasattr(r, "failed") and r.failed():
            # If ops list isn't available, we can't target; skip.
            ops = getattr(r, "ops", None) or []
            for op in ops:
                k = (getattr(r, "pipeline_id", None), _op_key(op))
                # If pipeline_id isn't in result, fall back to op-scoped key only
                if k[0] is None:
                    k = ("_unknown_pipeline_", _op_key(op))
                cur = s.op_ram_bump.get(k, 1.0)
                bump = s.fail_bump * (1.25 if _is_oom_error(getattr(r, "error", None)) else 1.0)
                s.op_ram_bump[k] = min(s.max_bump, max(cur, cur * bump))

    # Early exit if no changes that could affect scheduling decisions.
    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    # Greedily pack each pool with ready work, prioritizing latency-sensitive ops.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool

        # Attempt to schedule multiple ops per pool in this tick.
        # Limit iterations to avoid pathological scanning loops.
        scan_budget = 64

        while scan_budget > 0 and avail_cpu > s.min_cpu and avail_ram > s.min_ram_gb:
            scan_budget -= 1

            scheduled_any = False

            # Always attempt higher priorities first.
            for pr in _priority_order():
                q = s.waiting.get(pr, [])
                if not q:
                    continue

                # Round-robin: rotate through the queue until we find a schedulable op or exhaust.
                tried = 0
                qlen = len(q)
                while tried < qlen and avail_cpu > s.min_cpu and avail_ram > s.min_ram_gb:
                    tried += 1
                    pipeline = q.pop(0)

                    status = pipeline.runtime_status()

                    # Drop completed pipelines.
                    if status.is_pipeline_successful():
                        continue

                    # Pick one ready operator (respecting parent completion).
                    op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
                    if not op_list:
                        # Not runnable yet; keep it in queue.
                        q.append(pipeline)
                        continue

                    op = op_list[0]

                    # Determine per-op targets.
                    cpu_need = _target_cpu_for_priority(pool, pipeline.priority)
                    ram_need = _target_ram_for_op(s, pool, pipeline.pipeline_id, op, pipeline.priority)

                    # Cap to what's currently available.
                    cpu_alloc = min(avail_cpu, cpu_need)
                    ram_alloc = min(avail_ram, ram_need)

                    # If we can't give at least minimal resources, defer (but keep order fair).
                    if cpu_alloc < s.min_cpu or ram_alloc < s.min_ram_gb:
                        q.append(pipeline)
                        continue

                    # Assign just this operator.
                    assignments.append(
                        Assignment(
                            ops=[op],
                            cpu=cpu_alloc,
                            ram=ram_alloc,
                            priority=pipeline.priority,
                            pool_id=pool_id,
                            pipeline_id=pipeline.pipeline_id,
                        )
                    )

                    # Update available resources for packing.
                    avail_cpu -= cpu_alloc
                    avail_ram -= ram_alloc

                    # Put pipeline back for further ops later.
                    q.append(pipeline)

                    scheduled_any = True
                    break  # Move back to highest priority after any successful schedule.

                if scheduled_any:
                    break

            if not scheduled_any:
                # Nothing runnable fits in this pool right now.
                break

    return suspensions, assignments
