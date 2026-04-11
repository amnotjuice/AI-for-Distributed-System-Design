# policy_key: scheduler_low_016
# reasoning_effort: low
# model: gpt-5.2-2025-12-11
# llm_cost: 0.058097
# generation_seconds: 49.72
# generated_at: 2026-04-09T21:14:51.609615
@register_scheduler_init(key="scheduler_low_016")
def scheduler_low_016_init(s):
    """Priority-aware, failure-averse scheduler with OOM-driven RAM backoff + aging fairness.

    Goals:
      - Minimize weighted latency (query/interact dominate) while avoiding failures (720s penalty).
      - Prefer high-priority work (QUERY > INTERACTIVE > BATCH), but ensure batch makes progress via aging.
      - Reduce OOM failures by retrying failed ops with exponential RAM backoff (per-op, per-pipeline).
      - Keep CPU sizing conservative to increase concurrency; scale CPU up for high priority when headroom exists.

    Notes:
      - We avoid aggressive preemption because the simulator interface here does not expose a safe way
        to enumerate running containers beyond completion/failure results.
      - We do NOT drop pipelines on first failure; we retry failed ops up to a cap with increasing RAM.
    """
    s.waiting = []  # list of dict entries: {"pipeline": p, "enq": tick}
    s.tick = 0

    # Per (pipeline_id, op_key) RAM multiplier and retry counts.
    s.op_ram_mult = {}      # (pid, op_key) -> float
    s.op_retry_count = {}   # (pid, op_key) -> int

    # Pipeline arrival bookkeeping (optional; helps stable aging).
    s.pipeline_first_seen = {}  # pid -> tick

    # Tuning knobs
    s.max_retries = 4
    s.aging_batch_threshold = 25       # ticks waited before batch can jump ahead of interactive
    s.aging_interactive_threshold = 15 # ticks waited before interactive can contend more strongly
    s.base_ram_floor = {
        Priority.QUERY: 2.0,
        Priority.INTERACTIVE: 1.5,
        Priority.BATCH_PIPELINE: 1.0,
    }
    s.base_cpu_floor = {
        Priority.QUERY: 1.0,
        Priority.INTERACTIVE: 1.0,
        Priority.BATCH_PIPELINE: 0.5,
    }


@register_scheduler(key="scheduler_low_016")
def scheduler_low_016(s, results, pipelines):
    """
    Scheduler step:
      1) Ingest new pipelines into a single waiting list with enqueue tick.
      2) Process execution results: on failure, increase per-op RAM multiplier and track retries.
      3) For each pool, repeatedly pick the "best" next runnable op among waiting pipelines:
           - Priority order: QUERY > INTERACTIVE > BATCH
           - Aging: old INTERACTIVE/BATCH can jump the line to prevent starvation
      4) Assign ops with conservative CPU and RAM sized to avoid OOM (using learned backoff).
    """
    import math

    s.tick += 1

    # --- Helpers (local) ---
    def _priority_rank(pri):
        # Lower rank is better.
        if pri == Priority.QUERY:
            return 0
        if pri == Priority.INTERACTIVE:
            return 1
        return 2

    def _op_key(op):
        # Try stable identifiers if they exist; fallback to object id.
        for attr in ("op_id", "operator_id", "id", "name"):
            if hasattr(op, attr):
                v = getattr(op, attr)
                # For callables or None, skip.
                if v is not None and not callable(v):
                    return (attr, str(v))
        return ("py_id", str(id(op)))

    def _get_op_min_ram(op):
        # Probe common attribute names; default to 1.0 (units should match executor).
        for attr in ("min_ram", "ram_min", "ram_requirement", "ram", "memory", "mem"):
            if hasattr(op, attr):
                v = getattr(op, attr)
                if isinstance(v, (int, float)) and v > 0:
                    return float(v)
        return 1.0

    def _get_op_min_cpu(op):
        for attr in ("min_cpu", "cpu_min", "cpu_requirement", "cpu", "vcpus", "cores"):
            if hasattr(op, attr):
                v = getattr(op, attr)
                if isinstance(v, (int, float)) and v > 0:
                    return float(v)
        return 1.0

    def _effective_class(pri, waited):
        # Aging rules to avoid starvation while still protecting query latency:
        # - QUERY always first.
        # - INTERACTIVE can outrank BATCH immediately.
        # - BATCH can contend earlier once it has waited long enough.
        if pri == Priority.QUERY:
            return 0
        if pri == Priority.INTERACTIVE:
            # Interactive can behave closer to query after a bit of waiting.
            return 1 if waited < s.aging_interactive_threshold else 0.5
        # Batch: only improves after enough waiting.
        return 2 if waited < s.aging_batch_threshold else 1.5

    def _pick_cpu(pri, avail_cpu, pool_max_cpu, op_min_cpu):
        # Conservative CPU to allow concurrency; scale up for query when plenty of headroom.
        cpu_floor = s.base_cpu_floor.get(pri, 1.0)
        cpu = max(cpu_floor, op_min_cpu)

        if pri == Priority.QUERY:
            # Give queries more CPU if available, up to ~1/2 pool.
            cpu = max(cpu, min(avail_cpu, max(2.0, 0.5 * pool_max_cpu)))
        elif pri == Priority.INTERACTIVE:
            # Moderate CPU, up to ~1/3 pool.
            cpu = max(cpu, min(avail_cpu, max(1.0, (1.0 / 3.0) * pool_max_cpu)))
        else:
            # Batch small slices; don't exceed 1/4 pool.
            cpu = max(cpu, min(avail_cpu, max(0.5, 0.25 * pool_max_cpu)))

        # Avoid tiny fractional CPU if the simulator expects positive.
        cpu = max(0.1, float(cpu))
        cpu = min(float(avail_cpu), cpu)
        return cpu

    def _pick_ram(pri, avail_ram, pool_max_ram, op_min_ram, mult):
        ram_floor = s.base_ram_floor.get(pri, 1.0)
        target = max(ram_floor, op_min_ram * mult)

        # If we have lots of headroom for high priority, add a small safety margin.
        if pri in (Priority.QUERY, Priority.INTERACTIVE) and avail_ram > 0.5 * pool_max_ram:
            target = max(target, op_min_ram * mult * 1.25)

        # Clamp to what we can allocate right now.
        target = min(float(avail_ram), float(target))
        return target

    # --- Ingest new pipelines ---
    for p in pipelines:
        if p.pipeline_id not in s.pipeline_first_seen:
            s.pipeline_first_seen[p.pipeline_id] = s.tick
        s.waiting.append({"pipeline": p, "enq": s.tick})

    # Early exit if nothing changed.
    if not pipelines and not results:
        return [], []

    # --- Process results (learn from failures) ---
    for r in results:
        if not hasattr(r, "ops") or not r.ops:
            continue
        if not hasattr(r, "failed") or not callable(r.failed):
            continue

        if r.failed():
            # Treat any failure as a reason to increase RAM next time (OOM-safe bias).
            # This is intentionally conservative to avoid 720s penalties.
            pid = getattr(r, "pipeline_id", None)
            # pipeline_id isn't available on ExecutionResult per the template; fall back to None-safe.
            # We'll key only by op if pid missing by using container_id/pool_id to at least separate a bit.
            if pid is None:
                pid = ("unknown_pid", getattr(r, "container_id", None), getattr(r, "pool_id", None))

            for op in r.ops:
                ok = _op_key(op)
                k = (pid, ok)
                prev = s.op_ram_mult.get(k, 1.0)
                # Exponential backoff, capped.
                s.op_ram_mult[k] = min(prev * 2.0, 64.0)
                s.op_retry_count[k] = s.op_retry_count.get(k, 0) + 1

    # --- Clean waiting list: remove completed pipelines; keep failed ones for retry unless over cap ---
    cleaned = []
    for entry in s.waiting:
        p = entry["pipeline"]
        st = p.runtime_status()
        if st.is_pipeline_successful():
            continue
        # If there are FAILED ops, we still keep the pipeline to retry.
        cleaned.append(entry)
    s.waiting = cleaned

    suspensions = []
    assignments = []

    # --- Scheduling loop per pool ---
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)
        max_cpu = float(pool.max_cpu_pool)
        max_ram = float(pool.max_ram_pool)

        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # Try to fill the pool with as many single-op assignments as possible.
        # (Each Assignment can contain multiple ops, but we keep it 1-op for safety and fairness.)
        made_progress = True
        while made_progress:
            made_progress = False
            if avail_cpu <= 0 or avail_ram <= 0:
                break
            if not s.waiting:
                break

            # Select the best candidate pipeline entry with a runnable op.
            best_idx = None
            best_tuple = None
            best_op = None

            for i, entry in enumerate(s.waiting):
                p = entry["pipeline"]
                st = p.runtime_status()

                # Skip pipelines that have no runnable ops yet.
                ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
                if not ops:
                    continue

                # Find the first runnable op.
                op = ops[0]

                waited = max(0, s.tick - entry["enq"])
                pri = p.priority
                eff_class = _effective_class(pri, waited)

                # RAM backoff keying: prefer (pipeline_id, op_key). If mismatch with result pid,
                # we still use pipeline_id here which is stable.
                pid = p.pipeline_id
                ok = _op_key(op)
                k = (pid, ok)
                mult = s.op_ram_mult.get(k, 1.0)
                retries = s.op_retry_count.get(k, 0)

                # If we've already retried too much, don't keep burning cycles; deprioritize heavily.
                # (Still keep the pipeline in queue so it can run if nothing else exists.)
                over_retry_penalty = 1000 if retries >= s.max_retries else 0

                op_min_ram = _get_op_min_ram(op)
                op_min_cpu = _get_op_min_cpu(op)

                # Quick feasibility check: if even the minimum (with current multiplier) can't fit,
                # deprioritize and let other work run; it may fit in another pool or later.
                needed_ram_est = max(s.base_ram_floor.get(pri, 1.0), op_min_ram * mult)
                if needed_ram_est > max_ram + 1e-9:
                    # Can't ever fit in this pool; skip for this pool.
                    continue

                # Prefer higher priority / aged, then older enqueue, then smaller estimated RAM to pack.
                tup = (
                    eff_class,                 # primary: priority class with aging (lower is better)
                    over_retry_penalty,        # huge penalty if exceeding retries
                    -waited,                   # secondary: older waited first
                    _priority_rank(pri),       # stable tie-breaker
                    needed_ram_est,            # pack smaller ops first to increase completion rate
                )

                if best_tuple is None or tup < best_tuple:
                    best_tuple = tup
                    best_idx = i
                    best_op = op

            if best_idx is None or best_op is None:
                break

            # Pop the selected pipeline entry; we will requeue it after assignment attempt.
            entry = s.waiting.pop(best_idx)
            p = entry["pipeline"]
            pri = p.priority
            pid = p.pipeline_id
            ok = _op_key(best_op)
            k = (pid, ok)

            mult = s.op_ram_mult.get(k, 1.0)
            retries = s.op_retry_count.get(k, 0)

            op_min_ram = _get_op_min_ram(best_op)
            op_min_cpu = _get_op_min_cpu(best_op)

            # If beyond retry cap, still attempt once in a while but only if there's ample headroom.
            if retries >= s.max_retries and (avail_ram < 0.75 * max_ram):
                # Requeue without scheduling to avoid thrashing.
                s.waiting.append(entry)
                continue

            ram = _pick_ram(pri, avail_ram, max_ram, op_min_ram, mult)
            cpu = _pick_cpu(pri, avail_cpu, max_cpu, op_min_cpu)

            # If we cannot give at least a minimal amount, requeue.
            if ram <= 0 or cpu <= 0:
                s.waiting.append(entry)
                continue

            # If the chosen RAM is below the estimated needed floor, skip for now.
            needed_floor = max(s.base_ram_floor.get(pri, 1.0), op_min_ram * mult)
            if ram + 1e-9 < needed_floor:
                s.waiting.append(entry)
                continue

            assignment = Assignment(
                ops=[best_op],
                cpu=cpu,
                ram=ram,
                priority=pri,
                pool_id=pool_id,
                pipeline_id=pid,
            )
            assignments.append(assignment)

            # Update local available resources (best-effort accounting).
            avail_cpu = max(0.0, avail_cpu - float(cpu))
            avail_ram = max(0.0, avail_ram - float(ram))

            # Requeue pipeline for subsequent ops.
            s.waiting.append(entry)
            made_progress = True

    return suspensions, assignments
