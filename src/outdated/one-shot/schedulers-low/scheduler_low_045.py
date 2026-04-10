# policy_key: scheduler_low_045
# reasoning_effort: low
# model: gpt-5.2-2025-12-11
# llm_cost: 0.043987
# generation_seconds: 53.03
# generated_at: 2026-03-14T02:37:18.761116
@register_scheduler_init(key="scheduler_low_045")
def scheduler_low_045_init(s):
    """
    Priority-aware FIFO with simple, safe improvements over the naive baseline:

    1) Maintain separate FIFO queues per priority and always schedule higher priority first.
    2) Cap per-assignment CPU/RAM to avoid a single function monopolizing an entire pool.
    3) Retry on OOM by increasing a per-pipeline RAM scale factor (exponential backoff).
    4) Best-fit-ish placement: for each runnable op, pick the pool with the most headroom.

    Notes:
    - This policy intentionally avoids preemption (needs reliable visibility into running containers).
    - Complexity is kept low: one-op-at-a-time per pipeline, FIFO within each priority class.
    """
    # Per-priority FIFO queues of pipelines
    s.queues = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }

    # Pipeline-level RAM scale (used to increase RAM after OOM failures)
    s.ram_scale_by_pipeline = {}  # pipeline_id -> float

    # Pipelines we will no longer schedule (non-OOM failures)
    s.drop_pipeline_ids = set()


def _is_oom_error(err) -> bool:
    if err is None:
        return False
    try:
        msg = str(err).lower()
    except Exception:
        return False
    return ("oom" in msg) or ("out of memory" in msg) or ("out-of-memory" in msg) or ("memoryerror" in msg)


def _prio_order():
    # Highest urgency first
    return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]


def _pool_score(av_cpu, av_ram, max_cpu, max_ram) -> float:
    # Simple headroom score in [0, 2]
    cpu_frac = (av_cpu / max_cpu) if max_cpu else 0.0
    ram_frac = (av_ram / max_ram) if max_ram else 0.0
    return cpu_frac + ram_frac


def _caps_for_priority(pool, priority):
    # Small, obvious improvement vs naive "take everything":
    # cap allocations so one op doesn't monopolize the pool.
    if priority in (Priority.QUERY, Priority.INTERACTIVE):
        cpu_cap = max(1.0, 0.75 * pool.max_cpu_pool)
        ram_cap = max(1.0, 0.75 * pool.max_ram_pool)
    else:
        cpu_cap = max(1.0, 0.50 * pool.max_cpu_pool)
        ram_cap = max(1.0, 0.50 * pool.max_ram_pool)
    return cpu_cap, ram_cap


def _clean_and_enqueue(s, pipelines):
    # Enqueue new pipelines into the appropriate priority queue
    for p in pipelines:
        if p.pipeline_id in s.drop_pipeline_ids:
            continue
        s.queues[p.priority].append(p)

    # Remove completed/failed pipelines from queues (best-effort)
    for pr in list(s.queues.keys()):
        new_q = []
        for p in s.queues[pr]:
            if p.pipeline_id in s.drop_pipeline_ids:
                continue
            status = p.runtime_status()
            if status.is_pipeline_successful():
                continue
            # If pipeline has any FAILED operators, we keep it ONLY if we intend to retry (OOM path).
            # Otherwise, it will be dropped when we observe the failure in results.
            new_q.append(p)
        s.queues[pr] = new_q


@register_scheduler(key="scheduler_low_045")
def scheduler_low_045(s, results, pipelines):
    """
    Scheduler step:
    - Process results:
        * On OOM failure: increase per-pipeline RAM scale factor and keep pipeline queued.
        * On other failure: drop pipeline from future scheduling.
    - Enqueue new pipelines.
    - Schedule runnable ops by priority across pools:
        * choose pool with most headroom
        * allocate with per-priority caps and per-pipeline RAM scaling
        * FIFO within each priority class, round-robin across pipelines of the same class
    """
    # Fast path: nothing to do
    if not results and not pipelines:
        return [], []

    # --- Process results to learn from failures (especially OOM) ---
    # We only have explicit access to ExecutionResult fields; use that to adapt RAM.
    for r in results:
        if not getattr(r, "failed", None):
            continue
        try:
            did_fail = r.failed()
        except Exception:
            did_fail = False

        if not did_fail:
            continue

        # Identify pipeline_id if present on ops; fall back to None if we can't infer it.
        # In Eudoxia, the scheduler has pipeline objects; we use results only to guide RAM scale.
        pipeline_id = None
        try:
            if r.ops and hasattr(r.ops[0], "pipeline_id"):
                pipeline_id = r.ops[0].pipeline_id
        except Exception:
            pipeline_id = None

        # If we can't infer pipeline_id, we cannot adapt per pipeline; skip.
        if pipeline_id is None:
            continue

        if _is_oom_error(getattr(r, "error", None)):
            # Exponential backoff on RAM, bounded to avoid runaway
            prev = s.ram_scale_by_pipeline.get(pipeline_id, 1.0)
            s.ram_scale_by_pipeline[pipeline_id] = min(8.0, max(1.0, prev * 1.5))
        else:
            # Non-OOM failure: drop pipeline to avoid thrashing
            s.drop_pipeline_ids.add(pipeline_id)

    # --- Enqueue new pipelines and clean queues ---
    _clean_and_enqueue(s, pipelines)

    suspensions = []
    assignments = []

    # Local view of pool availability so multiple assignments in one tick don't oversubscribe.
    pool_avail = []
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        pool_avail.append(
            {
                "pool_id": pool_id,
                "avail_cpu": float(pool.avail_cpu_pool),
                "avail_ram": float(pool.avail_ram_pool),
                "max_cpu": float(pool.max_cpu_pool),
                "max_ram": float(pool.max_ram_pool),
            }
        )

    def pick_best_pool_for(priority):
        # Pick the pool with the most headroom that still has some resources.
        best_idx = None
        best_score = -1.0
        for idx, pa in enumerate(pool_avail):
            if pa["avail_cpu"] <= 0.0 or pa["avail_ram"] <= 0.0:
                continue
            score = _pool_score(pa["avail_cpu"], pa["avail_ram"], pa["max_cpu"], pa["max_ram"])
            if score > best_score:
                best_score = score
                best_idx = idx
        return best_idx

    # Try to place as many ops as possible, priority-first.
    made_progress = True
    while made_progress:
        made_progress = False

        for pr in _prio_order():
            q = s.queues.get(pr, [])
            if not q:
                continue

            # Round-robin within priority: scan up to current queue length for a runnable pipeline.
            n = len(q)
            for _ in range(n):
                pipeline = q.pop(0)

                # Skip dropped pipelines
                if pipeline.pipeline_id in s.drop_pipeline_ids:
                    continue

                status = pipeline.runtime_status()
                if status.is_pipeline_successful():
                    continue

                # Only schedule one runnable operator (parents complete).
                op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
                if not op_list:
                    # No runnable op right now; keep pipeline in FIFO order.
                    q.append(pipeline)
                    continue

                # Choose a pool with most headroom (simple best-fit-ish)
                best_pool_idx = pick_best_pool_for(pr)
                if best_pool_idx is None:
                    # No resources anywhere; requeue and stop trying this priority for now.
                    q.append(pipeline)
                    break

                pa = pool_avail[best_pool_idx]
                pool = s.executor.pools[pa["pool_id"]]

                cpu_cap, ram_cap = _caps_for_priority(pool, pr)
                scale = s.ram_scale_by_pipeline.get(pipeline.pipeline_id, 1.0)

                # Request capped resources, then apply RAM scaling (bounded by available RAM).
                cpu_req = min(pa["avail_cpu"], cpu_cap)
                ram_req = min(pa["avail_ram"], min(ram_cap * scale, pa["avail_ram"]))

                # Must request >0 to make sense
                if cpu_req <= 0.0 or ram_req <= 0.0:
                    q.append(pipeline)
                    break

                assignments.append(
                    Assignment(
                        ops=op_list,
                        cpu=cpu_req,
                        ram=ram_req,
                        priority=pipeline.priority,
                        pool_id=pa["pool_id"],
                        pipeline_id=pipeline.pipeline_id,
                    )
                )

                # Update local availability and requeue pipeline for potential later ops.
                pa["avail_cpu"] -= cpu_req
                pa["avail_ram"] -= ram_req

                q.append(pipeline)
                made_progress = True
                break  # move back to highest priority immediately

            s.queues[pr] = q

            if made_progress:
                break  # restart outer loop to honor strict priority

    return suspensions, assignments
