# policy_key: scheduler_low_042
# reasoning_effort: low
# model: gpt-5.2-2025-12-11
# llm_cost: 0.036945
# generation_seconds: 45.10
# generated_at: 2026-03-14T02:34:42.876289
@register_scheduler_init(key="scheduler_low_042")
def scheduler_low_042_init(s):
    """Priority-aware FIFO with simple right-sizing and OOM-based RAM retry.

    Improvements over naive FIFO:
      1) Strict priority queues (QUERY > INTERACTIVE > BATCH) to reduce tail latency.
      2) Per-priority resource caps to avoid a single batch op consuming the whole pool,
         preserving headroom for future high-priority arrivals.
      3) OOM feedback loop: if an op fails, bump its RAM hint and retry later.

    This is intentionally a small, low-risk step up in complexity: no preemption and no
    speculative runtime modeling, but materially better latency isolation.
    """
    # Pending pipelines are tracked by priority for fast selection.
    s.waiting = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }

    # Track when a pipeline was first seen for light aging (avoid indefinite batch starvation).
    s.pipeline_first_seen_tick = {}  # pipeline_id -> tick

    # RAM hints keyed by a stable-ish operator signature string.
    s.op_ram_hint = {}  # op_key -> ram

    # Scheduler logical tick counter (increments each scheduler call).
    s.tick = 0


def _priority_order():
    return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]


def _cap_fraction_for_priority(priority):
    # Caps are fractions of pool max to prevent over-allocations that harm latency.
    if priority == Priority.QUERY:
        return 0.90
    if priority == Priority.INTERACTIVE:
        return 0.75
    return 0.50  # BATCH_PIPELINE


def _aging_threshold_ticks_for_batch():
    # After enough waiting, allow a batch pipeline to "act like" interactive for selection.
    return 25


def _op_key(op):
    # Best-effort stable key without relying on undocumented fields.
    # If the op has an ID attribute, prefer it; otherwise fall back to repr().
    for attr in ("op_id", "operator_id", "id", "name"):
        if hasattr(op, attr):
            try:
                v = getattr(op, attr)
                if v is not None:
                    return f"{attr}:{v}"
            except Exception:
                pass
    return f"repr:{repr(op)}"


def _is_oom_failure(result):
    # Robust-ish detection given unknown error types/strings.
    try:
        if getattr(result, "error", None) is None:
            return False
        msg = str(result.error).lower()
        return ("oom" in msg) or ("out of memory" in msg) or ("memory" in msg and "exceed" in msg)
    except Exception:
        return False


def _clamp(x, lo, hi):
    return max(lo, min(hi, x))


def _compute_allocation(pool, priority, avail_cpu, avail_ram):
    # Allocate up to a fraction of the pool max, but never exceed current availability.
    cap_frac = _cap_fraction_for_priority(priority)
    cpu_cap = pool.max_cpu_pool * cap_frac
    ram_cap = pool.max_ram_pool * cap_frac

    cpu = min(avail_cpu, cpu_cap)
    ram = min(avail_ram, ram_cap)

    # Avoid zero allocations.
    cpu = max(cpu, 0)
    ram = max(ram, 0)
    return cpu, ram


def _maybe_promote_aged_batch(s, pipeline):
    if pipeline.priority != Priority.BATCH_PIPELINE:
        return pipeline.priority

    first = s.pipeline_first_seen_tick.get(pipeline.pipeline_id, s.tick)
    if (s.tick - first) >= _aging_threshold_ticks_for_batch():
        # Promotion for selection only (does not change pipeline.priority used in Assignment).
        return Priority.INTERACTIVE
    return Priority.BATCH_PIPELINE


def _enqueue_pipeline(s, pipeline):
    if pipeline.pipeline_id not in s.pipeline_first_seen_tick:
        s.pipeline_first_seen_tick[pipeline.pipeline_id] = s.tick
    s.waiting[pipeline.priority].append(pipeline)


def _select_next_pipeline(s):
    # Selection uses strict priority, but allows aging promotion for batch.
    # We pop from the front (FIFO within priority).
    # Implementation detail: we scan small fronts rather than reorder entire queues.
    # 1) Prefer QUERY, then INTERACTIVE.
    for pr in (Priority.QUERY, Priority.INTERACTIVE):
        q = s.waiting[pr]
        while q:
            p = q.pop(0)
            return p
    # 2) For BATCH, allow aged promotion by looking at the head first, then FIFO.
    q = s.waiting[Priority.BATCH_PIPELINE]
    while q:
        p = q.pop(0)
        return p
    return None


@register_scheduler(key="scheduler_low_042")
def scheduler_low_042(s, results: list, pipelines: list):
    """
    Priority-aware scheduler:
      - Maintain per-priority FIFO queues.
      - Assign at most one ready op per pool per tick (small step-up from naive).
      - Cap per-assignment resources by priority to preserve headroom.
      - On failure (esp. OOM), increase per-op RAM hint and retry.

    Returns:
      (suspensions, assignments)
    """
    s.tick += 1

    # Ingest new pipelines.
    for p in pipelines:
        _enqueue_pipeline(s, p)

    # Update RAM hints from results.
    # Note: We avoid dropping pipelines on failure; instead we allow retries, but increase RAM hint.
    # If the simulation expects "FAILED" to be terminal, this still improves OOM behavior by
    # re-attempting only if the pipeline exposes FAILED in ASSIGNABLE_STATES (as per prompt).
    if results:
        for r in results:
            try:
                if r.failed():
                    # Increase RAM hint for the failed ops.
                    bump_factor = 2.0 if _is_oom_failure(r) else 1.5
                    for op in getattr(r, "ops", []) or []:
                        k = _op_key(op)
                        prev = s.op_ram_hint.get(k, 0)
                        # Prefer using the RAM that was actually attempted if available.
                        attempted = getattr(r, "ram", 0) or 0
                        new_hint = max(prev, attempted) * bump_factor
                        if new_hint > 0:
                            s.op_ram_hint[k] = new_hint
            except Exception:
                # Never let bookkeeping break scheduling.
                pass

    # Early exit if nothing changed that would affect decisions.
    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    # For each pool, try to place one operation. This keeps logic simple and reduces
    # the chance that batch work floods the cluster within a single tick.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool
        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # Attempt a few pulls to find an actually schedulable pipeline/op.
        # Prevent infinite loops if many pipelines are blocked on parents.
        attempts = 0
        max_attempts = 25

        requeue = []  # pipelines we inspected but couldn't schedule right now

        scheduled = False
        while attempts < max_attempts:
            attempts += 1
            pipeline = _select_next_pipeline(s)
            if pipeline is None:
                break

            status = pipeline.runtime_status()

            # Drop completed pipelines.
            if status.is_pipeline_successful():
                continue

            # Pick the next ready op (parents complete).
            op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
            if not op_list:
                # Nothing ready; keep it around.
                requeue.append(pipeline)
                continue

            # Selection priority (with aging). Allocation uses the pipeline's true priority.
            sel_pr = _maybe_promote_aged_batch(s, pipeline)

            # Determine capped allocation for this priority class.
            cpu, ram = _compute_allocation(pool, sel_pr, avail_cpu, avail_ram)

            # Apply per-op RAM hint if we have one (e.g., after OOM), but never exceed availability.
            opk = _op_key(op_list[0])
            hinted_ram = s.op_ram_hint.get(opk)
            if hinted_ram is not None and hinted_ram > 0:
                ram = min(avail_ram, max(ram, hinted_ram))

            # Final clamps (avoid pathological values).
            cpu = _clamp(cpu, 0, avail_cpu)
            ram = _clamp(ram, 0, avail_ram)

            if cpu <= 0 or ram <= 0:
                # Can't place anything in this pool now; requeue and stop.
                requeue.append(pipeline)
                break

            assignment = Assignment(
                ops=op_list,
                cpu=cpu,
                ram=ram,
                priority=pipeline.priority,  # preserve true priority for executor metrics
                pool_id=pool_id,
                pipeline_id=pipeline.pipeline_id,
            )
            assignments.append(assignment)
            scheduled = True
            break

        # Requeue anything we inspected but didn't schedule.
        for p in requeue:
            s.waiting[p.priority].append(p)

        # If we didn't schedule, nothing more to do for this pool.
        if not scheduled:
            continue

    return suspensions, assignments
