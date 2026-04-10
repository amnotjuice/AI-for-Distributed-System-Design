# policy_key: scheduler_est_012
# reasoning_effort: low
# model: gpt-5.2-2025-12-11
# llm_cost: 0.050119
# generation_seconds: 35.09
# generated_at: 2026-04-03T01:12:26.187775
@register_scheduler_init(key="scheduler_est_012")
def scheduler_est_012_init(s):
    """Priority-aware FIFO with simple RAM estimation + OOM-driven retries.

    Improvements over naive FIFO:
      - Maintain separate queues by priority and always prefer high priority.
      - Prefer placing high-priority work in the "best" pool (more headroom).
      - Use per-operator memory estimates (if present) to size RAM.
      - On OOM failure, retry the same operator with increased RAM (exponential backoff).
      - Keep the policy simple: no preemption and at most one op assigned per pool per tick.
    """
    from collections import deque

    # Separate waiting queues by priority (lower latency for high priority).
    s.waiting_by_prio = {
        Priority.QUERY: deque(),
        Priority.INTERACTIVE: deque(),
        Priority.BATCH_PIPELINE: deque(),
    }

    # Track per-(pipeline, op) RAM inflation factor after OOMs.
    # Keyed by (pipeline_id, id(op)) to avoid relying on unknown operator identifiers.
    s.ram_factor = {}

    # Track recently seen pipeline ids to avoid pathological duplicates (best-effort).
    s._seen_pipeline_ids = set()

    # Tunables: small safety margin around estimate; and exponential bump on OOM.
    s.est_safety = 1.05
    s.oom_bump = 2.0

    # Default RAM request as fraction of pool RAM when no estimate exists.
    # Keep conservative for batch to avoid hogging pools; allow more for query/interactive to reduce retries.
    s.default_ram_frac = {
        Priority.QUERY: 0.45,
        Priority.INTERACTIVE: 0.35,
        Priority.BATCH_PIPELINE: 0.25,
    }

    # Default CPU caps as fraction of *available* CPU (simple latency bias).
    s.default_cpu_frac = {
        Priority.QUERY: 0.90,
        Priority.INTERACTIVE: 0.75,
        Priority.BATCH_PIPELINE: 0.50,
    }


def _priority_order_for_pool(s, pool_id: int):
    """Pool-local priority order.
    If multiple pools exist, bias pool 0 toward low-latency work.
    """
    if s.executor.num_pools <= 1:
        return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]
    if pool_id == 0:
        return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]
    return [Priority.INTERACTIVE, Priority.QUERY, Priority.BATCH_PIPELINE]


def _pool_sort_key_headroom(s, pool_id: int):
    """Sort pools by a combined headroom score to place latency-sensitive work first."""
    pool = s.executor.pools[pool_id]
    # Normalize by max to avoid preferring larger pools purely due to scale.
    cpu_score = (pool.avail_cpu_pool / pool.max_cpu_pool) if pool.max_cpu_pool else 0.0
    ram_score = (pool.avail_ram_pool / pool.max_ram_pool) if pool.max_ram_pool else 0.0
    return (cpu_score + ram_score) / 2.0


def _pipeline_is_droppable(pipeline) -> bool:
    """Return True if pipeline should be removed from queues (completed or irrecoverably failed)."""
    status = pipeline.runtime_status()
    if status.is_pipeline_successful():
        return True
    # NOTE: We still allow FAILED ops to be retried (ASSIGNABLE_STATES includes FAILED in this simulator context).
    return False


def _next_ready_op_from_pipeline(pipeline):
    """Get the next assignable operator whose parents are complete (single-op scheduling)."""
    status = pipeline.runtime_status()
    op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
    return op_list


def _estimate_ram_request_gb(s, op, pipeline, pool, avail_ram: float) -> float:
    """RAM sizing using estimator hint + OOM backoff factor; bounded by pool/availability."""
    # Base estimate: prefer estimator if present.
    est = None
    try:
        est = getattr(op, "estimate", None)
        if est is not None:
            est = getattr(est, "mem_peak_gb", None)
    except Exception:
        est = None

    if est is None:
        base = pool.max_ram_pool * s.default_ram_frac.get(pipeline.priority, 0.25)
    else:
        # Small safety margin; we can retry on OOM.
        base = float(est) * s.est_safety

    factor = s.ram_factor.get((pipeline.pipeline_id, id(op)), 1.0)
    req = base * factor

    # Bound request: cannot exceed availability; also keep at least a small floor if anything is available.
    if avail_ram <= 0:
        return 0.0
    req = max(min(req, avail_ram), min(0.25, avail_ram))  # 256MB floor-ish in GB units.
    return req


def _estimate_cpu_request(s, pipeline, avail_cpu: float) -> float:
    """CPU sizing: bias more CPU to high priority, but never exceed available."""
    if avail_cpu <= 0:
        return 0.0
    frac = s.default_cpu_frac.get(pipeline.priority, 0.5)
    req = avail_cpu * frac
    # Keep a minimal floor of 1 vCPU if possible.
    req = max(min(req, avail_cpu), min(1.0, avail_cpu))
    return req


@register_scheduler(key="scheduler_est_012")
def scheduler_est_012(s, results: list, pipelines: list):
    """
    Priority-aware scheduler:
      1) Ingest new pipelines into per-priority queues.
      2) Process results:
         - On OOM failures, increase RAM factor for the failed op so retries request more RAM.
      3) For each pool (chosen by headroom for low-latency work), assign at most one ready op,
         preferring QUERY > INTERACTIVE > BATCH (with slight pool-specific bias).
    """
    # Ingest new pipelines.
    for p in pipelines:
        # Best-effort dedup by id to avoid queue blowups if generator replays objects.
        if p.pipeline_id in s._seen_pipeline_ids:
            s.waiting_by_prio[p.priority].append(p)
        else:
            s._seen_pipeline_ids.add(p.pipeline_id)
            s.waiting_by_prio[p.priority].append(p)

    # Process execution results: learn from OOM failures.
    for r in results:
        if r is None:
            continue
        if r.failed():
            # Heuristic: treat any failure as possibly OOM if error mentions it,
            # otherwise don't aggressively change behavior.
            err = ""
            try:
                err = (r.error or "")
            except Exception:
                err = ""
            is_oom = ("oom" in err.lower()) or ("out of memory" in err.lower())

            if is_oom:
                # Inflate RAM factor for each op in this result (usually one).
                for op in getattr(r, "ops", []) or []:
                    # We rely on pipeline_id to be available in results; if not, skip learning.
                    # The template indicates Assignment includes pipeline_id but ExecutionResult may not;
                    # so we guard heavily.
                    pid = getattr(r, "pipeline_id", None)
                    if pid is None:
                        # Fall back: cannot attribute; skip.
                        continue
                    key = (pid, id(op))
                    prev = s.ram_factor.get(key, 1.0)
                    s.ram_factor[key] = min(prev * s.oom_bump, 64.0)  # cap runaway growth

    # Early exit if nothing changed.
    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    # Choose pool iteration order: for responsiveness, schedule on highest-headroom pools first.
    pool_ids = list(range(s.executor.num_pools))
    pool_ids.sort(key=lambda i: _pool_sort_key_headroom(s, i), reverse=True)

    # Try to schedule one op per pool per tick.
    for pool_id in pool_ids:
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool
        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # Local priority order (pool 0 biased for latency if multiple pools exist).
        prio_order = _priority_order_for_pool(s, pool_id)

        assigned = False

        # We will rotate through each priority queue with a small bounded scan
        # to find a pipeline that has a ready operator.
        for prio in prio_order:
            q = s.waiting_by_prio[prio]
            if not q:
                continue

            # Bounded scan prevents O(n) worst-case every tick.
            scan = min(len(q), 32)
            requeue = []
            chosen_pipeline = None
            chosen_ops = None

            for _ in range(scan):
                p = q.popleft()

                # Drop completed pipelines.
                if _pipeline_is_droppable(p):
                    continue

                op_list = _next_ready_op_from_pipeline(p)
                if op_list:
                    chosen_pipeline = p
                    chosen_ops = op_list
                    break
                else:
                    requeue.append(p)

            # Put back scanned-but-not-chosen pipelines.
            for p in requeue:
                q.append(p)

            if chosen_pipeline is None:
                continue

            op = chosen_ops[0]

            # Compute resource requests.
            ram_req = _estimate_ram_request_gb(s, op, chosen_pipeline, pool, avail_ram)
            cpu_req = _estimate_cpu_request(s, chosen_pipeline, avail_cpu)

            # If we can't allocate meaningful resources, requeue and continue.
            if ram_req <= 0 or cpu_req <= 0:
                q.append(chosen_pipeline)
                continue

            # Create assignment (single-op).
            assignments.append(
                Assignment(
                    ops=chosen_ops,
                    cpu=cpu_req,
                    ram=ram_req,
                    priority=chosen_pipeline.priority,
                    pool_id=pool_id,
                    pipeline_id=chosen_pipeline.pipeline_id,
                )
            )

            # Requeue the pipeline to allow next stages to be scheduled later.
            q.append(chosen_pipeline)
            assigned = True
            break

        if assigned:
            continue

    return suspensions, assignments
