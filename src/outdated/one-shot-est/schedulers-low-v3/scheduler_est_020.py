# policy_key: scheduler_est_020
# reasoning_effort: low
# model: gpt-5.2-2025-12-11
# llm_cost: 0.041621
# generation_seconds: 43.17
# generated_at: 2026-04-03T01:17:20.640816
@register_scheduler_init(key="scheduler_est_020")
def scheduler_est_020_init(s):
    """Priority-aware, estimate-guided scheduler (incremental improvement over naive FIFO).

    Main ideas (kept intentionally simple / low-risk):
      1) Priority queues: always try QUERY first, then INTERACTIVE, then BATCH.
      2) Headroom protection: cap how much of each pool BATCH can consume to reduce tail latency
         for higher priorities (no preemption needed).
      3) RAM sizing using per-op estimate (if present) + conservative retry-on-OOM ramp:
         - allocate close to estimate (small buffer)
         - if op fails, increase RAM for that op on retry (exponential backoff), up to pool max
      4) CPU sizing: avoid giving all CPUs to one op by default; give more to high priority,
         but keep per-op caps to improve responsiveness under contention.
    """
    # Waiting queues split by priority
    s.wait_q = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }

    # Track per-operator retry RAM multipliers after failures (e.g., OOM)
    # key: (pipeline_id, op_key) -> multiplier (float)
    s.op_ram_mult = {}

    # Track failure counts to avoid infinite loops
    # key: (pipeline_id, op_key) -> int
    s.op_fail_count = {}

    # Config knobs (small, safe improvements first)
    s.max_retries_per_op = 4

    # Batch headroom protection: BATCH can only consume up to this fraction of each pool
    # (leave the rest for higher priority arrivals)
    s.batch_cpu_frac_cap = 0.70
    s.batch_ram_frac_cap = 0.70

    # Per-op CPU caps by priority (avoid single op monopolizing a pool)
    s.cpu_cap = {
        Priority.QUERY: 8.0,
        Priority.INTERACTIVE: 6.0,
        Priority.BATCH_PIPELINE: 4.0,
    }

    # If estimate missing, use a small default allocation (GB) and rely on retry-on-failure
    s.default_ram_gb = {
        Priority.QUERY: 2.0,
        Priority.INTERACTIVE: 2.0,
        Priority.BATCH_PIPELINE: 3.0,
    }

    # Estimate buffer and retry backoff
    s.est_buffer_frac = 0.10          # allocate ~10% above estimate
    s.retry_backoff = 1.8             # multiplier per failure


def _priority_order():
    return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]


def _op_key(op):
    # Best-effort stable key across simulation objects
    k = getattr(op, "operator_id", None)
    if k is None:
        k = getattr(op, "op_id", None)
    if k is None:
        k = getattr(op, "name", None)
    if k is None:
        k = repr(op)
    return str(k)


def _get_est_mem_gb(op):
    # Estimator may attach op.estimate.mem_peak_gb; accept missing/None
    est = None
    est_obj = getattr(op, "estimate", None)
    if est_obj is not None:
        est = getattr(est_obj, "mem_peak_gb", None)
    if est is None:
        est = getattr(op, "est_mem_peak_gb", None)
    try:
        if est is None:
            return None
        est_f = float(est)
        if est_f <= 0:
            return None
        return est_f
    except Exception:
        return None


def _get_min_mem_gb(op):
    # Not guaranteed to exist; best-effort
    for attr in ("mem_min_gb", "min_mem_gb", "mem_gb_min", "ram_min_gb"):
        v = getattr(op, attr, None)
        if v is not None:
            try:
                v = float(v)
                if v > 0:
                    return v
            except Exception:
                pass
    return None


def _oom_like(error_obj):
    if error_obj is None:
        return False
    s = str(error_obj).lower()
    # Heuristic: treat anything that looks like OOM/memory as RAM-related
    return ("oom" in s) or ("out of memory" in s) or ("memory" in s) or ("cuda out of memory" in s)


def _pop_next_runnable_pipeline(q_list):
    # Pop the next pipeline that still has runnable ops (parents satisfied) and is not complete.
    # We cycle pipelines by moving them to the back when not runnable right now.
    n = len(q_list)
    for _ in range(n):
        p = q_list.pop(0)
        status = p.runtime_status()
        if status.is_pipeline_successful():
            continue
        # Keep it in rotation; actual op selection happens later
        q_list.append(p)
        return p
    return None


def _select_one_op(pipeline):
    status = pipeline.runtime_status()
    # Only schedule ops whose parents are complete; pick one op at a time for stability.
    op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
    return op_list


def _compute_batch_caps(s, pool):
    # Hard caps on how much BATCH we allow to consume in a pool at any moment.
    # We don't have exact "current usage by priority" so we use "availability" as a proxy:
    # if avail is low, we effectively stop admitting batch.
    cpu_cap = max(0.0, pool.max_cpu_pool * s.batch_cpu_frac_cap)
    ram_cap = max(0.0, pool.max_ram_pool * s.batch_ram_frac_cap)
    return cpu_cap, ram_cap


def _effective_avail_for_priority(s, pool, priority):
    # For high priorities, use full available.
    # For batch, apply caps by reducing effective availability if the pool is already "tight".
    avail_cpu = pool.avail_cpu_pool
    avail_ram = pool.avail_ram_pool

    if priority != Priority.BATCH_PIPELINE:
        return avail_cpu, avail_ram

    # If remaining availability is already below headroom, block batch by shrinking effective avail to 0.
    batch_cpu_cap, batch_ram_cap = _compute_batch_caps(s, pool)

    # If pool is highly utilized (avail is small), don't admit more batch.
    # We treat "protected headroom" as (1 - cap_frac) * max.
    protected_cpu = max(0.0, pool.max_cpu_pool - batch_cpu_cap)
    protected_ram = max(0.0, pool.max_ram_pool - batch_ram_cap)

    if avail_cpu <= protected_cpu or avail_ram <= protected_ram:
        return 0.0, 0.0

    return avail_cpu - protected_cpu, avail_ram - protected_ram


def _size_ram_gb(s, pipeline, op, pool, priority):
    # Start from estimate if present; otherwise default.
    est = _get_est_mem_gb(op)
    base = est * (1.0 + s.est_buffer_frac) if est is not None else s.default_ram_gb.get(priority, 2.0)

    # Respect any known min if available (best-effort)
    min_mem = _get_min_mem_gb(op)
    if min_mem is not None:
        base = max(base, min_mem)

    # Apply retry multiplier if this op previously failed
    k = (pipeline.pipeline_id, _op_key(op))
    mult = s.op_ram_mult.get(k, 1.0)
    req = base * mult

    # Clamp to what's feasible
    req = max(0.1, req)
    req = min(req, pool.avail_ram_pool, pool.max_ram_pool)
    return req


def _size_cpu(s, pool, priority):
    # Give more CPU to higher priority, but cap per op to avoid monopolization.
    cap = float(s.cpu_cap.get(priority, 4.0))
    cpu = min(pool.avail_cpu_pool, cap)
    # Avoid zero/negative CPU
    return max(0.0, cpu)


@register_scheduler(key="scheduler_est_020")
def scheduler_est_020(s, results, pipelines):
    """
    Priority-aware scheduler loop.

    - Enqueue incoming pipelines by priority.
    - Process results: on failure, increase RAM multiplier for the failed op(s) and keep pipeline queued.
    - For each pool, repeatedly try to assign 1 runnable op at a time, prioritizing QUERY > INTERACTIVE > BATCH,
      while ensuring BATCH doesn't eat into protected headroom.
    """
    # Enqueue new pipelines
    for p in pipelines:
        pr = p.priority
        if pr not in s.wait_q:
            # Unknown priority; treat as batch-like
            pr = Priority.BATCH_PIPELINE
        s.wait_q[pr].append(p)

    # Update retry sizing based on failures
    for r in results:
        if r is None:
            continue
        if not r.failed():
            continue

        # If we can detect OOM-like failures, ramp RAM aggressively; otherwise ramp mildly.
        oomish = _oom_like(getattr(r, "error", None))
        bump = s.retry_backoff if oomish else 1.3

        for op in getattr(r, "ops", []) or []:
            # Try to attribute failure to specific ops; if none, we can't do much.
            pid = getattr(op, "pipeline_id", None)
            # We don't reliably have pipeline_id on op; fall back to result container/pool context only.
            # Use a best-effort key without pipeline_id if needed.
            opk = _op_key(op)
            if pid is None:
                # Use a sentinel; still allows per-op ramp within this run.
                pid = "__unknown_pipeline__"

            k = (pid, opk)
            s.op_fail_count[k] = s.op_fail_count.get(k, 0) + 1
            if s.op_fail_count[k] <= s.max_retries_per_op:
                s.op_ram_mult[k] = min(64.0, s.op_ram_mult.get(k, 1.0) * bump)

    # Early exit if nothing changed
    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    # Schedule per pool
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]

        # Keep assigning while we can fit at least something.
        # (We purposely assign one op per assignment for stability; can emit multiple assignments per tick.)
        made_progress = True
        while made_progress:
            made_progress = False

            # Try priorities in order
            for pr in _priority_order():
                eff_cpu, eff_ram = _effective_avail_for_priority(s, pool, pr)
                if eff_cpu <= 0.0 or eff_ram <= 0.0:
                    continue

                pipeline = _pop_next_runnable_pipeline(s.wait_q[pr])
                if pipeline is None:
                    continue

                status = pipeline.runtime_status()
                if status.is_pipeline_successful():
                    continue

                # If pipeline has failed ops, we still allow retries up to a limit (via ASSIGNABLE_STATES=...FAILED)
                op_list = _select_one_op(pipeline)
                if not op_list:
                    # Not runnable now; keep it in queue (it was already appended back)
                    continue

                op0 = op_list[0]

                # Enforce per-op retry cap if we can resolve pipeline_id; otherwise allow.
                k_known = (pipeline.pipeline_id, _op_key(op0))
                if s.op_fail_count.get(k_known, 0) > s.max_retries_per_op:
                    # Drop on repeated failures by not scheduling further; effectively stalls this pipeline.
                    # (We don't have a "mark failed" API here.)
                    continue

                cpu = min(eff_cpu, _size_cpu(s, pool, pr))
                if cpu <= 0.0:
                    continue

                ram = _size_ram_gb(s, pipeline, op0, pool, pr)
                # Also respect effective RAM for this priority
                ram = min(ram, eff_ram)
                if ram <= 0.0:
                    continue

                assignments.append(
                    Assignment(
                        ops=op_list,
                        cpu=cpu,
                        ram=ram,
                        priority=pipeline.priority,
                        pool_id=pool_id,
                        pipeline_id=pipeline.pipeline_id,
                    )
                )

                made_progress = True
                break  # restart priority loop after each assignment to re-check pool availability

    return suspensions, assignments
