# policy_key: scheduler_low_025
# reasoning_effort: low
# model: gpt-5.2-2025-12-11
# llm_cost: 0.039871
# generation_seconds: 38.75
# generated_at: 2026-03-14T02:21:38.543561
@register_scheduler_init(key="scheduler_low_025")
def scheduler_low_025_init(s):
    """
    Priority-aware FIFO with small, safe improvements over the naive baseline:

    1) Maintain separate FIFO queues per priority and always dispatch higher priority first.
    2) Apply simple resource "reservation" in a shared pool so batch cannot consume the last
       capacity needed for interactive/query latency.
    3) On suspected OOM failures, retry the pipeline with a higher RAM request (bounded).

    Notes:
    - This policy intentionally avoids aggressive preemption to keep churn low and code robust.
    - It assigns at most one operator per pool per tick (like the baseline) to reduce interference.
    """
    s.waiting = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }
    # Per-pipeline RAM inflation factor used after OOM-like failures
    s.ram_inflation = {}  # pipeline_id -> factor (>= 1.0)
    # Track whether we've already enqueued a pipeline (avoid pathological duplicates)
    s.enqueued = set()

    # Tunables (kept conservative for "small improvements first")
    s.base_ram_frac = 0.25   # start with 25% of pool max RAM (bounded by availability)
    s.base_cpu_frac = 0.25   # start with 25% of pool max CPU (bounded by availability)
    s.max_ram_frac = 0.95    # never ask for more than 95% of pool max RAM
    s.max_cpu_frac = 0.95    # never ask for more than 95% of pool max CPU
    s.oom_backoff_mult = 2.0 # double RAM request on OOM-like failures
    s.max_inflation = 8.0    # cap RAM inflation factor

    # Reservations in a shared pool (used only when num_pools == 1)
    s.reserve_cpu_frac = 0.30
    s.reserve_ram_frac = 0.30


def _is_oom_error(err) -> bool:
    if not err:
        return False
    msg = str(err).lower()
    return ("oom" in msg) or ("out of memory" in msg) or ("memoryerror" in msg)


def _prio_order():
    return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]


def _pick_next_pipeline(s, pool_id: int):
    """
    Return (pipeline, op_list) for the next schedulable work item using strict priority FIFO.
    Skips completed/failed pipelines and those without runnable operators.
    """
    for pr in _prio_order():
        q = s.waiting[pr]
        # Scan FIFO but allow skipping pipelines with no runnable ops.
        # Keep scan bounded by queue size to avoid infinite loops.
        n = len(q)
        for _ in range(n):
            p = q.pop(0)
            status = p.runtime_status()
            # Drop fully successful pipelines
            if status.is_pipeline_successful():
                s.enqueued.discard(p.pipeline_id)
                continue
            # If any operator is FAILED, we treat it as "has failures".
            # We allow retry if it was an OOM-like failure (handled via ram_inflation + requeue).
            # If failures are non-OOM, we drop the pipeline (simple, conservative).
            has_failures = status.state_counts[OperatorState.FAILED] > 0
            if has_failures:
                # Keep it around; whether it progresses depends on ASSIGNABLE_STATES and retry sizing.
                pass

            op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
            if op_list:
                # Requeue pipeline at tail to preserve FIFO among same priority after dispatch attempt
                q.append(p)
                return p, op_list
            else:
                # Not runnable now; keep it in FIFO order (tail)
                q.append(p)
        # next priority
    return None, []


def _compute_request(s, pool, pipeline_id, priority):
    """
    Compute conservative CPU/RAM request with optional RAM inflation after OOM-like failures.
    """
    infl = s.ram_inflation.get(pipeline_id, 1.0)

    # Base request sized as fraction of pool max, then bounded by availability.
    req_cpu = pool.max_cpu_pool * s.base_cpu_frac
    req_ram = pool.max_ram_pool * s.base_ram_frac * infl

    # Priority boost: give more CPU (not RAM) to high-priority work to improve latency.
    if priority in (Priority.QUERY, Priority.INTERACTIVE):
        req_cpu = max(req_cpu, pool.max_cpu_pool * 0.50)

    # Cap by pool maximum fractions
    req_cpu = min(req_cpu, pool.max_cpu_pool * s.max_cpu_frac)
    req_ram = min(req_ram, pool.max_ram_pool * s.max_ram_frac)

    # Finally bound by instantaneous availability
    req_cpu = min(req_cpu, pool.avail_cpu_pool)
    req_ram = min(req_ram, pool.avail_ram_pool)

    # Avoid zero allocations
    if req_cpu <= 0 or req_ram <= 0:
        return 0, 0
    return req_cpu, req_ram


def _can_admit_batch_in_shared_pool(s, pool, req_cpu, req_ram):
    """
    Enforce reservations in a single-pool setup:
    keep some headroom for interactive/query arrivals to reduce tail latency.
    """
    reserve_cpu = pool.max_cpu_pool * s.reserve_cpu_frac
    reserve_ram = pool.max_ram_pool * s.reserve_ram_frac

    # Remaining after this assignment
    rem_cpu = pool.avail_cpu_pool - req_cpu
    rem_ram = pool.avail_ram_pool - req_ram

    return (rem_cpu >= reserve_cpu) and (rem_ram >= reserve_ram)


@register_scheduler(key="scheduler_low_025")
def scheduler_low_025(s, results, pipelines):
    """
    Priority-aware FIFO scheduler with conservative resource reservations and OOM RAM backoff.
    """
    # Ingest new pipelines into per-priority FIFO queues (avoid accidental duplicates)
    for p in pipelines:
        if p.pipeline_id in s.enqueued:
            continue
        s.waiting[p.priority].append(p)
        s.enqueued.add(p.pipeline_id)
        s.ram_inflation.setdefault(p.pipeline_id, 1.0)

    # Process results to adjust RAM inflation on OOM-like failures
    for r in results:
        if getattr(r, "failed", None) and r.failed():
            if _is_oom_error(getattr(r, "error", None)):
                # Inflate RAM request for the owning pipeline on next dispatch attempt
                # (we don't know the operator min-RAM; this is a simple safe heuristic)
                # Prefer pipeline_id if present; otherwise infer from r.ops[0].pipeline_id not available here.
                # The simulator provides r.ops; Assignment provides pipeline_id, but results may not.
                # Best-effort: if ops exist and have pipeline_id attribute, use it; else do nothing.
                pid = None
                if getattr(r, "ops", None):
                    op0 = r.ops[0]
                    pid = getattr(op0, "pipeline_id", None) or getattr(op0, "pipeline", None) or None
                    # If op0.pipeline is an object, try pipeline_id
                    if pid is not None and not isinstance(pid, (str, int)):
                        pid = getattr(pid, "pipeline_id", None)
                if pid is not None:
                    cur = s.ram_inflation.get(pid, 1.0)
                    s.ram_inflation[pid] = min(cur * s.oom_backoff_mult, s.max_inflation)

    # Early exit if nothing changed
    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    # Pool selection strategy:
    # - If multiple pools: prefer dedicating pool 0 to QUERY/INTERACTIVE by filling it with high priority first.
    # - If single pool: use reservations to protect latency.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        if pool.avail_cpu_pool <= 0 or pool.avail_ram_pool <= 0:
            continue

        # Decide which priority classes are eligible for this pool this tick
        eligible_priorities = _prio_order()
        if s.executor.num_pools > 1:
            if pool_id == 0:
                eligible_priorities = [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]
            else:
                eligible_priorities = [Priority.BATCH_PIPELINE, Priority.INTERACTIVE, Priority.QUERY]

        # Pick work: strict priority, but constrained to eligible order for this pool
        chosen_pipeline = None
        chosen_ops = []
        for pr in eligible_priorities:
            q = s.waiting[pr]
            n = len(q)
            for _ in range(n):
                p = q.pop(0)
                status = p.runtime_status()
                if status.is_pipeline_successful():
                    s.enqueued.discard(p.pipeline_id)
                    continue

                # If pipeline has failures, only keep retrying; non-OOM failures will remain FAILED and thus
                # are still "assignable" per ASSIGNABLE_STATES, but may loop. To avoid infinite churn, drop
                # pipelines that have FAILED ops but did not just get an OOM inflation (i.e., still at 1.0).
                has_failures = status.state_counts[OperatorState.FAILED] > 0
                if has_failures and s.ram_inflation.get(p.pipeline_id, 1.0) <= 1.0:
                    # Conservative: treat as non-retriable
                    s.enqueued.discard(p.pipeline_id)
                    s.ram_inflation.pop(p.pipeline_id, None)
                    continue

                op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
                if op_list:
                    q.append(p)  # keep FIFO rotation
                    chosen_pipeline = p
                    chosen_ops = op_list
                    break
                else:
                    q.append(p)
            if chosen_pipeline:
                break

        if not chosen_pipeline or not chosen_ops:
            continue

        # Size request
        req_cpu, req_ram = _compute_request(s, pool, chosen_pipeline.pipeline_id, chosen_pipeline.priority)
        if req_cpu <= 0 or req_ram <= 0:
            continue

        # If single pool, protect latency by reserving headroom from batch allocations
        if s.executor.num_pools == 1 and chosen_pipeline.priority == Priority.BATCH_PIPELINE:
            if not _can_admit_batch_in_shared_pool(s, pool, req_cpu, req_ram):
                # Skip batch this tick; try to schedule higher priority instead (if any)
                # We already picked the best available by strict priority, so just don't schedule here.
                continue

        assignments.append(
            Assignment(
                ops=chosen_ops,
                cpu=req_cpu,
                ram=req_ram,
                priority=chosen_pipeline.priority,
                pool_id=pool_id,
                pipeline_id=chosen_pipeline.pipeline_id,
            )
        )

        # Like the baseline: at most one assignment per pool per tick for predictability
        # (small improvement phase; later iterations can increase parallelism carefully).
        continue

    return suspensions, assignments
