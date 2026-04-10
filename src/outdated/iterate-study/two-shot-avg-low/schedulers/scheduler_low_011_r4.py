# source: scheduler_low_011.py
# model: gpt-5.2-2025-12-11
# effort: low
# feedback: minimal
# iteration: r4
@register_scheduler_init(key="scheduler_low_011_r4")
def scheduler_low_011_r4_init(s):
    """Priority-aware FIFO scheduler with small, latency-focused improvements.

    Iteration goals (incremental, low-risk):
    1) Strict priority ordering (QUERY > INTERACTIVE > BATCH) with reduced head-of-line blocking by scanning queues.
    2) "Interactive pool" reservation: avoid filling the interactive pool with BATCH so high-priority arrivals don't wait.
    3) High-priority spillover: if QUERY/INTERACTIVE waits for a few ticks, allow running on any pool to cut queueing.
    4) Actually retry OOM failures: FAILED ops are assignable; keep retryable OOM failures in the system and increase RAM.
    """
    # Per-priority FIFO queues of pipelines
    s.waiting_queues = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }

    # Simple logical clock for waiting/aging decisions
    s.tick = 0
    s.pipeline_first_enqueue_tick = {}  # pipeline_id -> first tick seen

    # Learned per-op resource hints (from OOM failures)
    # Keys are either (pipeline_id, id(op)) when known, or ("*", id(op)) as a fallback.
    s.op_hints = {}          # op_key -> {"ram": float, "cpu": float}
    s.op_attempts = {}       # op_key -> int
    s.op_retryable = {}      # op_key -> bool (true only for OOM-like failures)

    # Policy knobs (kept simple)
    s.max_retries_per_op = 3
    s.queue_scan_limit = 24

    # Prefer pool 0 for interactive/query if multiple pools exist
    s.interactive_pool_id = 0

    # Reserve headroom on the interactive pool to protect latency (no preemption needed)
    s.reserved_frac_cpu_interactive = 0.25
    s.reserved_frac_ram_interactive = 0.25

    # Allow high-priority spillover to non-interactive pools after waiting a bit
    s.spillover_wait_ticks = 3

    # Default sizing fractions of pool max (dynamic adjustments happen at runtime)
    s.base_fracs = {
        Priority.QUERY: {"cpu": 0.9, "ram": 0.6},
        Priority.INTERACTIVE: {"cpu": 0.9, "ram": 0.6},
        Priority.BATCH_PIPELINE: {"cpu": 1.0, "ram": 1.0},
    }


def _prio_order():
    return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]


def _is_oom_error(err):
    if err is None:
        return False
    msg = str(err).lower()
    return ("oom" in msg) or ("out of memory" in msg) or ("memoryerror" in msg)


def _op_key(pipeline_id, op):
    # Primary key includes pipeline_id (best), fallback uses "*" if pipeline_id is not available.
    if pipeline_id is None:
        return ("*", id(op))
    return (pipeline_id, id(op))


def _get_hint(s, pipeline_id, op):
    # Try pipeline-specific key first, then fallback wildcard key.
    k1 = _op_key(pipeline_id, op)
    if k1 in s.op_hints:
        return s.op_hints[k1], k1
    k2 = ("*", id(op))
    if k2 in s.op_hints:
        return s.op_hints[k2], k2
    return None, k1


def _pipeline_wait_ticks(s, pipeline):
    first = s.pipeline_first_enqueue_tick.get(pipeline.pipeline_id, s.tick)
    return max(0, s.tick - first)


def _pipeline_is_hard_failed(s, pipeline):
    """Return True if the pipeline has FAILED ops that we should not retry."""
    status = pipeline.runtime_status()
    failed_ops = status.get_ops([OperatorState.FAILED], require_parents_complete=False) or []
    if not failed_ops:
        return False

    # If any failed op is not marked retryable (or exceeded retry budget), treat as hard failure.
    for op in failed_ops:
        _, k = _get_hint(s, pipeline.pipeline_id, op)
        retryable = bool(s.op_retryable.get(k, False))
        attempts = int(s.op_attempts.get(k, 0))
        if (not retryable) or (attempts > s.max_retries_per_op):
            return True
    return False


def _get_next_assignable_op(pipeline):
    status = pipeline.runtime_status()
    ops = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True) or []
    if not ops:
        return None
    return ops[0]


def _pool_is_interactive(s, pool_id):
    return (s.executor.num_pools > 1) and (pool_id == s.interactive_pool_id)


def _hp_backlog(s):
    return (len(s.waiting_queues[Priority.QUERY]) + len(s.waiting_queues[Priority.INTERACTIVE])) > 0


def _dynamic_fracs(s, priority, hp_backlog_count):
    """Slightly adjust CPU fraction: when HP backlog is small, give more CPU to finish faster (latency)."""
    base = s.base_fracs.get(priority, {"cpu": 1.0, "ram": 1.0})
    cpu_frac = float(base["cpu"])
    ram_frac = float(base["ram"])

    if priority in (Priority.QUERY, Priority.INTERACTIVE):
        # If few HP pipelines are waiting, prefer "scale-up" (faster completion).
        # If many are waiting, reduce a bit to allow some parallelism.
        if hp_backlog_count <= max(1, s.executor.num_pools):
            cpu_frac = min(1.0, max(cpu_frac, 1.0))
            ram_frac = min(1.0, max(ram_frac, 0.7))
        else:
            cpu_frac = min(cpu_frac, 0.6)
            ram_frac = min(ram_frac, 0.6)

    return cpu_frac, ram_frac


def _request_resources(s, pool, pool_id, priority, pipeline, op, rem_cpu, rem_ram, hp_backlog_present):
    """Compute a safe cpu/ram request for this op in this pool, applying OOM hints and pool reservations."""
    # Start from fractions of pool max (then cap by remaining)
    hp_backlog_count = len(s.waiting_queues[Priority.QUERY]) + len(s.waiting_queues[Priority.INTERACTIVE])
    cpu_frac, ram_frac = _dynamic_fracs(s, priority, hp_backlog_count)

    req_cpu = max(1.0, pool.max_cpu_pool * cpu_frac)
    req_ram = max(1.0, pool.max_ram_pool * ram_frac)

    # Apply learned hints (OOM -> higher RAM)
    hint, hint_key = _get_hint(s, pipeline.pipeline_id, op)
    if hint:
        if "cpu" in hint:
            req_cpu = max(req_cpu, float(hint["cpu"]))
        if "ram" in hint:
            req_ram = max(req_ram, float(hint["ram"]))

    # Interactive pool reservation: don't let BATCH consume reserved headroom
    if _pool_is_interactive(s, pool_id) and priority == Priority.BATCH_PIPELINE:
        reserved_cpu = pool.max_cpu_pool * float(s.reserved_frac_cpu_interactive)
        reserved_ram = pool.max_ram_pool * float(s.reserved_frac_ram_interactive)

        # If HP backlog exists, we avoid batch on the interactive pool entirely (handled by caller).
        # If not, cap batch request to leave reserved headroom.
        rem_cpu_for_batch = max(0.0, rem_cpu - reserved_cpu)
        rem_ram_for_batch = max(0.0, rem_ram - reserved_ram)
        req_cpu = min(req_cpu, rem_cpu_for_batch)
        req_ram = min(req_ram, rem_ram_for_batch)

    # Final caps by remaining and pool max
    req_cpu = min(req_cpu, rem_cpu, pool.max_cpu_pool)
    req_ram = min(req_ram, rem_ram, pool.max_ram_pool)

    # Ensure positivity
    if req_cpu < 1.0 or req_ram < 1.0:
        return None, None

    return req_cpu, req_ram


def _placement_allowed(s, pool_id, priority, pipeline):
    """Placement rules to reduce latency: keep HP on interactive pool unless it has waited."""
    if s.executor.num_pools <= 1:
        return True

    wait = _pipeline_wait_ticks(s, pipeline)
    on_interactive_pool = _pool_is_interactive(s, pool_id)

    if priority in (Priority.QUERY, Priority.INTERACTIVE):
        # Prefer interactive pool, but allow spillover after some waiting to avoid queueing.
        if on_interactive_pool:
            return True
        return wait >= int(s.spillover_wait_ticks)

    # Batch: prefer non-interactive pools
    if priority == Priority.BATCH_PIPELINE:
        return not on_interactive_pool

    return True


def _select_from_queue(s, pool, pool_id, priority, rem_cpu, rem_ram, hp_backlog_present):
    """Scan a bounded prefix of the queue to find a schedulable pipeline/op, reducing head-of-line blocking.
    Returns (pipeline, op, cpu, ram) and removes pipeline from the queue if selected; may drop terminal pipelines.
    """
    q = s.waiting_queues.get(priority, [])
    if not q:
        return None

    scan = min(len(q), int(s.queue_scan_limit))
    i = 0
    while i < scan and i < len(q):
        pipeline = q[i]
        status = pipeline.runtime_status()

        # Drop successful pipelines
        if status.is_pipeline_successful():
            q.pop(i)
            scan -= 1
            continue

        # Drop hard-failed pipelines (non-retryable failures)
        if _pipeline_is_hard_failed(s, pipeline):
            q.pop(i)
            scan -= 1
            continue

        # Enforce placement rules (spillover for HP; batch stays off interactive pool)
        if not _placement_allowed(s, pool_id, priority, pipeline):
            i += 1
            continue

        # Extra rule: don't run batch on interactive pool when HP backlog exists (protect latency)
        if _pool_is_interactive(s, pool_id) and priority == Priority.BATCH_PIPELINE and hp_backlog_present:
            i += 1
            continue

        op = _get_next_assignable_op(pipeline)
        if op is None:
            # Not ready yet; skip it for now to avoid head-of-line blocking.
            i += 1
            continue

        cpu, ram = _request_resources(s, pool, pool_id, priority, pipeline, op, rem_cpu, rem_ram, hp_backlog_present)
        if cpu is None or ram is None:
            i += 1
            continue

        # Ensure fits in remaining pool resources
        if cpu <= rem_cpu and ram <= rem_ram:
            q.pop(i)
            return pipeline, op, cpu, ram

        i += 1

    return None


@register_scheduler(key="scheduler_low_011_r4")
def scheduler_low_011_r4(s, results, pipelines):
    """
    Scheduler step:
    - Update clock and enqueue arrivals.
    - Update retryability and RAM hints from OOM failures (and only retry OOM).
    - For each pool, assign multiple ops per tick (bounded) with strict priority ordering.
    - Protect interactive pool headroom from batch to improve tail latency without preemption.
    """
    s.tick += 1

    # Enqueue new pipelines
    for p in pipelines:
        pr = p.priority if p.priority in s.waiting_queues else Priority.BATCH_PIPELINE
        s.waiting_queues[pr].append(p)
        if p.pipeline_id not in s.pipeline_first_enqueue_tick:
            s.pipeline_first_enqueue_tick[p.pipeline_id] = s.tick

    # Process results: mark OOM failures as retryable and increase RAM hints
    for r in results:
        # Determine failure
        failed = False
        try:
            failed = bool(r.failed())
        except Exception:
            failed = getattr(r, "error", None) is not None

        if not failed:
            continue

        is_oom = _is_oom_error(getattr(r, "error", None))
        ops = getattr(r, "ops", []) or []
        # ExecutionResult may or may not carry pipeline_id; use fallback if missing
        res_pipeline_id = getattr(r, "pipeline_id", None)

        for op in ops:
            k = _op_key(res_pipeline_id, op)

            # Non-OOM failures are treated as non-retryable
            if not is_oom:
                s.op_retryable[k] = False
                continue

            # OOM: retry with higher RAM (exponential-ish), capped by pool max at scheduling time.
            prev_attempts = int(s.op_attempts.get(k, 0)) + 1
            s.op_attempts[k] = prev_attempts
            s.op_retryable[k] = prev_attempts <= int(s.max_retries_per_op)

            prev_hint = s.op_hints.get(k, {})
            prev_ram = float(prev_hint.get("ram", 0.0))
            # Use observed allocation as baseline if available, else grow from existing hint.
            observed_ram = float(getattr(r, "ram", 0.0) or 0.0)
            baseline_ram = max(1.0, observed_ram, prev_ram)

            # Double RAM on OOM (simple and robust)
            new_ram = baseline_ram * 2.0
            # Keep CPU hint stable (OOM isn't CPU-related)
            observed_cpu = float(getattr(r, "cpu", 1.0) or 1.0)
            prev_cpu = float(prev_hint.get("cpu", observed_cpu))

            s.op_hints[k] = {"ram": max(1.0, new_ram), "cpu": max(1.0, prev_cpu)}

    # Early exit if nothing new to decide
    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    hp_backlog_present = _hp_backlog(s)

    # Schedule per pool. Allow a small number of assignments per pool per tick to reduce HP queueing.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        rem_cpu = float(pool.avail_cpu_pool)
        rem_ram = float(pool.avail_ram_pool)
        if rem_cpu <= 0 or rem_ram <= 0:
            continue

        # Bound concurrency per pool: more for HP, less for batch to reduce interference
        max_slots = 2
        if _pool_is_interactive(s, pool_id):
            max_slots = 3  # interactive pool: run a bit more HP concurrently if resources allow

        slots = 0
        while slots < max_slots and rem_cpu > 0 and rem_ram > 0:
            picked = None

            # Strict priority: try QUERY, then INTERACTIVE, then BATCH
            for pr in _prio_order():
                cand = _select_from_queue(
                    s=s,
                    pool=pool,
                    pool_id=pool_id,
                    priority=pr,
                    rem_cpu=rem_cpu,
                    rem_ram=rem_ram,
                    hp_backlog_present=hp_backlog_present,
                )
                if cand is not None:
                    picked = (pr,) + cand  # (priority, pipeline, op, cpu, ram)
                    break

            if picked is None:
                break

            pr, pipeline, op, cpu, ram = picked

            # Create assignment
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

            # Update local remaining resources so we don't oversubscribe within this tick
            rem_cpu -= float(cpu)
            rem_ram -= float(ram)
            slots += 1

            # Re-enqueue the pipeline to allow subsequent operators to be scheduled later
            pr_q = pipeline.priority if pipeline.priority in s.waiting_queues else pr
            s.waiting_queues[pr_q].append(pipeline)

            # Recompute HP backlog presence after scheduling (can change decisions for batch)
            hp_backlog_present = _hp_backlog(s)

    return suspensions, assignments