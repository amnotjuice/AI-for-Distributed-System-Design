# policy_key: scheduler_low_005
# reasoning_effort: low
# model: gpt-5.2-2025-12-11
# llm_cost: 0.046339
# generation_seconds: 54.07
# generated_at: 2026-03-12T21:38:03.844644
@register_scheduler_init(key="scheduler_low_005")
def scheduler_low_005_init(s):
    """
    Priority-aware, latency-improving scheduler (incremental upgrade over naive FIFO).

    Key changes vs naive:
      - Maintain separate FIFO queues per priority; always consider higher priority first.
      - Do NOT allocate the entire pool to one op; right-size CPU/RAM to allow concurrency.
      - Simple OOM-aware retry: if an op fails with OOM, bump RAM (exponential backoff) and retry.
      - Light anti-starvation: after a streak of high-priority placements, allow a batch placement.
    """
    # Per-priority FIFO queues of pipelines
    s.queues = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }

    # Resource hints per operator instance (keyed by pipeline_id + python object identity)
    # Values are "next attempt" sizes.
    s.op_ram_hint = {}   # (pipeline_id, op_key) -> ram
    s.op_cpu_hint = {}   # (pipeline_id, op_key) -> cpu

    # Retry tracking for failures
    s.op_retries = {}    # (pipeline_id, op_key) -> int
    s.pipeline_hard_failed = set()  # pipeline_id set for non-OOM failures beyond tolerance

    # Anti-starvation knobs
    s.high_streak = 0
    s.max_high_streak = 6  # after this many high-priority placements, try to place one batch if possible

    # OOM/backoff knobs
    s.max_oom_retries = 4
    s.max_non_oom_retries = 1

    # Default sizing fractions (of pool max), capped by available resources.
    s.fraction_cpu = {
        Priority.QUERY: 0.50,
        Priority.INTERACTIVE: 0.50,
        Priority.BATCH_PIPELINE: 0.25,
    }
    s.fraction_ram = {
        Priority.QUERY: 0.25,
        Priority.INTERACTIVE: 0.25,
        Priority.BATCH_PIPELINE: 0.15,
    }

    # Minimums to avoid pathological tiny allocations (still may OOM; we learn via backoff).
    s.min_cpu = {
        Priority.QUERY: 1,
        Priority.INTERACTIVE: 1,
        Priority.BATCH_PIPELINE: 1,
    }
    s.min_ram = {
        Priority.QUERY: 1,
        Priority.INTERACTIVE: 1,
        Priority.BATCH_PIPELINE: 1,
    }


@register_scheduler(key="scheduler_low_005")
def scheduler_low_005_scheduler(s, results, pipelines):
    """
    Scheduler step:
      1) Ingest new pipelines into per-priority queues.
      2) Process execution results, updating OOM-based RAM hints and retry counters.
      3) For each pool, pack runnable operators while resources remain:
           - Prefer QUERY/INTERACTIVE for latency.
           - Occasionally schedule BATCH to prevent starvation.
           - Use per-op hints if present; otherwise use conservative fractions of pool capacity.
    """
    # ---- helpers (kept local; no imports) ----
    def _op_key(op):
        # Use python identity as a stable key within a simulation run.
        return id(op)

    def _is_oom_error(err):
        if not err:
            return False
        e = str(err).lower()
        return ("oom" in e) or ("out of memory" in e) or ("memory" in e and "exceed" in e)

    def _clamp(x, lo, hi):
        if x < lo:
            return lo
        if x > hi:
            return hi
        return x

    def _default_cpu_for(priority, pool, avail_cpu):
        # Allocate a fraction of pool max, but never exceed what's currently available.
        target = int(pool.max_cpu_pool * s.fraction_cpu.get(priority, 0.25))
        target = max(s.min_cpu.get(priority, 1), target)
        return min(avail_cpu, max(1, target))

    def _default_ram_for(priority, pool, avail_ram):
        target = int(pool.max_ram_pool * s.fraction_ram.get(priority, 0.15))
        target = max(s.min_ram.get(priority, 1), target)
        return min(avail_ram, max(1, target))

    def _get_candidate_ops(pipeline):
        status = pipeline.runtime_status()
        # Only schedule ops whose parents are complete.
        return status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)

    def _should_drop_pipeline(pipeline):
        if pipeline.pipeline_id in s.pipeline_hard_failed:
            return True
        status = pipeline.runtime_status()
        return status.is_pipeline_successful()

    def _priority_order_with_aging():
        # Primarily prioritize latency-sensitive work, but prevent batch starvation
        # by letting one batch op through after a streak of high-priority placements.
        if s.high_streak >= s.max_high_streak:
            return [Priority.BATCH_PIPELINE, Priority.QUERY, Priority.INTERACTIVE]
        return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]

    def _pick_next_pipeline_for_pool(pool_id, avail_cpu, avail_ram):
        # Select the next pipeline (and its next runnable op) to schedule given pool headroom.
        # Returns (pipeline, op) or (None, None).
        prios = _priority_order_with_aging()
        for pr in prios:
            q = s.queues.get(pr, [])
            if not q:
                continue

            # Rotate through queue to find a pipeline with a runnable op.
            # Keep FIFO behavior among pipelines that have runnable work.
            attempts = len(q)
            for _ in range(attempts):
                p = q.pop(0)

                if _should_drop_pipeline(p):
                    # Drop completed or hard-failed pipelines.
                    continue

                # If pipeline has no runnable ops right now, keep it in queue.
                ops = _get_candidate_ops(p)
                if not ops:
                    q.append(p)
                    continue

                op = ops[0]
                # Check if hinted resources could fit at all; if not, defer (do not block).
                ok = True
                k = (p.pipeline_id, _op_key(op))
                hinted_ram = s.op_ram_hint.get(k, None)
                hinted_cpu = s.op_cpu_hint.get(k, None)
                if hinted_ram is not None and hinted_ram > avail_ram:
                    ok = False
                if hinted_cpu is not None and hinted_cpu > avail_cpu:
                    ok = False

                if ok:
                    # Put pipeline back to tail (round-robin among pipelines at same priority).
                    q.append(p)
                    return p, op
                else:
                    q.append(p)
                    continue

        return None, None

    # ---- ingest new pipelines ----
    for p in pipelines:
        s.queues[p.priority].append(p)

    # ---- process results: update OOM hints / retry counters ----
    for r in results:
        if r is None:
            continue

        # Attempt to map result to the operator(s) it ran.
        # r.ops is expected to be a list of operator objects.
        ran_ops = getattr(r, "ops", None) or []

        if r.failed():
            oom = _is_oom_error(getattr(r, "error", None))
            for op in ran_ops:
                # Pipeline id isn't present on ExecutionResult in the provided API,
                # so we can only update hints if we can infer it via op identity across pipelines.
                # Best-effort approach: update a generic key without pipeline_id is unsafe.
                # Instead, we update all matching (pipeline_id, op_key) entries we may have seen.
                opk = _op_key(op)
                matching_keys = [k for k in s.op_retries.keys() if k[1] == opk]
                if not matching_keys:
                    # If we've never seen this op before, we can't safely attribute it to a pipeline_id
                    # for hinting; skip hint update.
                    continue

                for k in matching_keys:
                    retries = s.op_retries.get(k, 0) + 1
                    s.op_retries[k] = retries

                    if oom:
                        # Exponential backoff for RAM on OOM, capped later by pool max when assigning.
                        prev_ram = s.op_ram_hint.get(k, getattr(r, "ram", None))
                        if prev_ram is None:
                            prev_ram = max(1, getattr(r, "ram", 1))
                        s.op_ram_hint[k] = max(prev_ram + 1, int(prev_ram * 2))

                        # Optionally bump CPU slightly too, but keep conservative.
                        prev_cpu = s.op_cpu_hint.get(k, getattr(r, "cpu", None))
                        if prev_cpu is None:
                            prev_cpu = max(1, getattr(r, "cpu", 1))
                        s.op_cpu_hint[k] = max(prev_cpu, int(prev_cpu * 1.25))

                        if retries > s.max_oom_retries:
                            # Mark the owning pipeline as hard-failed if we can.
                            # We only know (pipeline_id, op_key); mark by pipeline_id.
                            s.pipeline_hard_failed.add(k[0])
                    else:
                        # Non-OOM failure: allow very limited retry; otherwise mark pipeline hard-failed.
                        if retries > s.max_non_oom_retries:
                            s.pipeline_hard_failed.add(k[0])
        else:
            # On success, clear per-op hints and retry counts to avoid runaway allocations.
            for op in ran_ops:
                opk = _op_key(op)
                matching_keys = [k for k in list(s.op_retries.keys()) if k[1] == opk]
                for k in matching_keys:
                    s.op_retries.pop(k, None)
                    s.op_ram_hint.pop(k, None)
                    s.op_cpu_hint.pop(k, None)

    # Early exit if no changes
    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    # ---- schedule: pack ops into each pool while resources remain ----
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool
        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # Greedy packing loop: stop when we can't fit anything else.
        made_progress = True
        while made_progress:
            made_progress = False
            if avail_cpu <= 0 or avail_ram <= 0:
                break

            pipeline, op = _pick_next_pipeline_for_pool(pool_id, avail_cpu, avail_ram)
            if pipeline is None or op is None:
                break

            # Determine sizing: use hint if available; else use default fractions.
            k = (pipeline.pipeline_id, _op_key(op))

            # Seed retry tracking for this op the first time we consider it.
            if k not in s.op_retries:
                s.op_retries[k] = 0

            cpu = s.op_cpu_hint.get(k, None)
            ram = s.op_ram_hint.get(k, None)

            if cpu is None:
                cpu = _default_cpu_for(pipeline.priority, pool, avail_cpu)
            if ram is None:
                ram = _default_ram_for(pipeline.priority, pool, avail_ram)

            # Clamp to pool and availability
            cpu = _clamp(int(cpu), 1, int(min(pool.max_cpu_pool, avail_cpu)))
            ram = _clamp(int(ram), 1, int(min(pool.max_ram_pool, avail_ram)))

            # If we still can't fit, stop trying in this pool for now.
            if cpu <= 0 or ram <= 0 or cpu > avail_cpu or ram > avail_ram:
                break

            assignments.append(
                Assignment(
                    ops=[op],
                    cpu=cpu,
                    ram=ram,
                    priority=pipeline.priority,
                    pool_id=pool_id,
                    pipeline_id=pipeline.pipeline_id,
                )
            )

            # Update anti-starvation streak
            if pipeline.priority in (Priority.QUERY, Priority.INTERACTIVE):
                s.high_streak += 1
            else:
                s.high_streak = 0

            # Update remaining capacity in this pool for this tick's packing.
            avail_cpu -= cpu
            avail_ram -= ram
            made_progress = True

    return suspensions, assignments
