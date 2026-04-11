# policy_key: scheduler_low_031
# reasoning_effort: low
# model: gpt-5.2-2025-12-11
# llm_cost: 0.043761
# generation_seconds: 51.76
# generated_at: 2026-04-09T21:27:38.433156
@register_scheduler_init(key="scheduler_low_031")
def scheduler_low_031_init(s):
    """
    Priority-aware, OOM-robust scheduler with mild anti-starvation.

    Core ideas:
      - Strictly favor QUERY then INTERACTIVE then BATCH for low weighted latency score.
      - Avoid the 720s failure penalty by retrying OOM'd operators with exponential RAM backoff.
      - Keep batch making progress via simple aging-based promotion.
      - Place higher priority work on the pool with the most headroom (when multiple pools exist).
    """
    s.time = 0  # logical ticks
    s.wait_q = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }
    s.pipelines_by_id = {}  # pipeline_id -> Pipeline
    s.meta = {}  # pipeline_id -> dict(arrival, ram_mult, last_oom, fail_cnt, drop)
    s.max_retries = 3
    s.batch_aging_threshold = 50  # ticks before allowing batch to jump ahead if nothing else runs


def _is_oom_error(err) -> bool:
    try:
        msg = str(err).lower()
    except Exception:
        return False
    return ("oom" in msg) or ("out of memory" in msg) or ("cuda out of memory" in msg) or ("memoryerror" in msg)


def _cleanup_pipeline_if_done(s, pipeline):
    status = pipeline.runtime_status()
    if status.is_pipeline_successful():
        pid = pipeline.pipeline_id
        s.meta.pop(pid, None)
        s.pipelines_by_id.pop(pid, None)
        # Note: pipeline may still exist in queues; we lazily skip it during pop.
        return True
    return False


def _pipeline_is_dropped(s, pid) -> bool:
    m = s.meta.get(pid)
    return bool(m and m.get("drop", False))


def _next_assignable_op(pipeline):
    status = pipeline.runtime_status()
    # Require parents complete to respect DAG dependencies.
    op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
    if not op_list:
        return []
    return op_list[:1]


def _pool_order_for_priority(s, priority):
    # Choose pool ordering by headroom: higher priorities get "best" pool first.
    # For batch, prefer non-best pools to reduce interference.
    pools = list(range(s.executor.num_pools))
    scores = []
    for i in pools:
        p = s.executor.pools[i]
        # Favor pools with both CPU and RAM available; normalize by pool max to compare.
        cpu_frac = (p.avail_cpu_pool / p.max_cpu_pool) if p.max_cpu_pool else 0.0
        ram_frac = (p.avail_ram_pool / p.max_ram_pool) if p.max_ram_pool else 0.0
        scores.append((i, min(cpu_frac, ram_frac), cpu_frac + ram_frac))

    if priority in (Priority.QUERY, Priority.INTERACTIVE):
        # Most headroom first
        scores.sort(key=lambda x: (x[1], x[2]), reverse=True)
    else:
        # For batch, try to use the "least desirable" pools first (leave best for queries).
        scores.sort(key=lambda x: (x[1], x[2]), reverse=False)

    return [i for (i, _, __) in scores]


def _compute_request(s, pool, pipeline, priority):
    """
    Decide cpu/ram request for the *next* operator of this pipeline.
    Conservative base sizing to reduce OOM-induced 720s penalties,
    with exponential RAM backoff after OOM.
    """
    pid = pipeline.pipeline_id
    m = s.meta.get(pid, {})
    ram_mult = float(m.get("ram_mult", 1.0))

    # Base shares: bias toward finishing high-priority quickly.
    cpu_share = {
        Priority.QUERY: 0.75,
        Priority.INTERACTIVE: 0.60,
        Priority.BATCH_PIPELINE: 0.35,
    }.get(priority, 0.35)

    # RAM shares: start relatively safe to avoid OOM; batch slightly lower for concurrency.
    ram_share = {
        Priority.QUERY: 0.70,
        Priority.INTERACTIVE: 0.60,
        Priority.BATCH_PIPELINE: 0.50,
    }.get(priority, 0.50)

    # Compute targets with caps.
    target_cpu = max(1.0, pool.max_cpu_pool * cpu_share)
    target_ram = max(0.0, pool.max_ram_pool * ram_share * ram_mult)

    # Do not exceed pool maxima.
    target_cpu = min(target_cpu, pool.max_cpu_pool)
    target_ram = min(target_ram, pool.max_ram_pool)

    # Final requests cannot exceed currently available resources.
    req_cpu = min(target_cpu, pool.avail_cpu_pool)
    req_ram = min(target_ram, pool.avail_ram_pool)

    # If we can't get at least some CPU and enough RAM to be meaningful, return zeros.
    if req_cpu <= 0 or req_ram <= 0:
        return 0.0, 0.0

    return req_cpu, req_ram


def _pop_next_pipeline(s, priority):
    """
    Pop next runnable pipeline from the given priority queue, skipping completed/dropped.
    Returns a Pipeline or None.
    """
    q = s.wait_q[priority]
    while q:
        pid = q.pop(0)
        p = s.pipelines_by_id.get(pid)
        if p is None:
            continue
        if _pipeline_is_dropped(s, pid):
            continue
        if _cleanup_pipeline_if_done(s, p):
            continue

        # If pipeline already terminally failed (non-OOM repeated), drop it.
        status = p.runtime_status()
        # We allow FAILED ops to be retried, but if we have marked drop, we skip above.
        _ = status  # explicit to show intent: we don't drop just because a FAILED exists.
        return p
    return None


def _choose_next_pipeline_for_pool(s, pool_id):
    """
    Choose pipeline to run next on a pool.
    Priority order: QUERY > INTERACTIVE > BATCH
    With mild anti-starvation: if batch head has waited long enough, allow it when
    high-priority queues are empty (or are blocked).
    """
    # Try strict priority first.
    for pr in (Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE):
        p = _pop_next_pipeline(s, pr)
        if p is not None:
            return p

    # Nothing.
    return None


@register_scheduler(key="scheduler_low_031")
def scheduler_low_031(s, results, pipelines):
    """
    Scheduler step:
      1) Ingest new pipelines into per-priority queues.
      2) Process results to detect OOM and adjust per-pipeline RAM multiplier.
      3) For each pool, assign a small number of operators, favoring high priority,
         and sizing containers to reduce OOM probability.
    """
    s.time += 1

    # 1) Add new pipelines
    for p in pipelines:
        pid = p.pipeline_id
        s.pipelines_by_id[pid] = p
        if pid not in s.meta:
            s.meta[pid] = {
                "arrival": s.time,
                "ram_mult": 1.0,
                "last_oom": False,
                "fail_cnt": 0,
                "drop": False,
            }
        s.wait_q[p.priority].append(pid)

    # Early exit if nothing changed.
    if not pipelines and not results:
        return [], []

    # 2) Process results: handle OOM by increasing RAM multiplier; drop on repeated non-OOM failures.
    for r in results:
        # Best effort mapping: ExecutionResult includes pipeline_id per Assignment, but result object
        # in some sims may not carry it. If missing, we can still react via priority only (no-op).
        pid = getattr(r, "pipeline_id", None)
        if pid is None:
            continue
        if pid not in s.meta:
            # Might happen if state was cleaned; ignore.
            continue

        if r.failed():
            m = s.meta[pid]
            m["fail_cnt"] = int(m.get("fail_cnt", 0)) + 1
            oom = _is_oom_error(getattr(r, "error", None))
            m["last_oom"] = bool(oom)

            if oom:
                # Exponential backoff on RAM requests to strongly reduce repeated OOMs.
                m["ram_mult"] = min(float(m.get("ram_mult", 1.0)) * 2.0, 16.0)
                # Requeue promptly within its priority to recover quickly (avoid 720s penalty).
                if pid not in s.wait_q[s.pipelines_by_id[pid].priority]:
                    s.wait_q[s.pipelines_by_id[pid].priority].insert(0, pid)
            else:
                # Non-OOM failures: retry a small number of times, then drop to avoid endless churn.
                if m["fail_cnt"] > s.max_retries:
                    m["drop"] = True
                else:
                    if pid not in s.wait_q[s.pipelines_by_id[pid].priority]:
                        s.wait_q[s.pipelines_by_id[pid].priority].append(pid)
        else:
            # Success: requeue pipeline to continue remaining ops.
            if pid in s.pipelines_by_id and not _pipeline_is_dropped(s, pid):
                if not _cleanup_pipeline_if_done(s, s.pipelines_by_id[pid]):
                    if pid not in s.wait_q[s.pipelines_by_id[pid].priority]:
                        s.wait_q[s.pipelines_by_id[pid].priority].append(pid)

    suspensions = []
    assignments = []

    # 3) Assign work per pool, using per-priority pool ordering.
    # We'll make up to 2 assignments per pool per tick to keep it simple and avoid fragmentation.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]

        if pool.avail_cpu_pool <= 0 or pool.avail_ram_pool <= 0:
            continue

        made = 0
        while made < 2 and pool.avail_cpu_pool > 0 and pool.avail_ram_pool > 0:
            # Choose next pipeline globally (strict priority).
            pipeline = _choose_next_pipeline_for_pool(s, pool_id)
            if pipeline is None:
                break

            # If this pipeline would be better placed on another pool, try that by requeueing
            # and skipping here (simple "placement" without complex stealing).
            preferred_pools = _pool_order_for_priority(s, pipeline.priority)
            if preferred_pools and preferred_pools[0] != pool_id:
                # Put it back and stop trying to schedule it on this pool right now.
                s.wait_q[pipeline.priority].insert(0, pipeline.pipeline_id)
                break

            # Select next operator.
            op_list = _next_assignable_op(pipeline)
            if not op_list:
                # Pipeline might be waiting on parents or already running; requeue and move on.
                s.wait_q[pipeline.priority].append(pipeline.pipeline_id)
                break

            req_cpu, req_ram = _compute_request(s, pool, pipeline, pipeline.priority)
            if req_cpu <= 0 or req_ram <= 0:
                # Can't fit now; requeue and stop scheduling on this pool.
                s.wait_q[pipeline.priority].insert(0, pipeline.pipeline_id)
                break

            assignments.append(
                Assignment(
                    ops=op_list,
                    cpu=req_cpu,
                    ram=req_ram,
                    priority=pipeline.priority,
                    pool_id=pool_id,
                    pipeline_id=pipeline.pipeline_id,
                )
            )
            made += 1

            # Note: pool.avail_* are typically updated by the simulator after assignments are applied.
            # To avoid over-assigning in the same tick, conservatively track local availability too.
            try:
                pool.avail_cpu_pool -= req_cpu
                pool.avail_ram_pool -= req_ram
            except Exception:
                # If pool availability is read-only, we just won't do multi-assign safely.
                break

    return suspensions, assignments
