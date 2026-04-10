# source: scheduler_low_011.py
# model: gpt-5.2-2025-12-11
# effort: low
# feedback: rich
# iteration: r16
@register_scheduler_init(key="scheduler_low_011_r16")
def scheduler_low_011_r16_init(s):
    """Iteration 2: Priority-aware, packed scheduling to reduce queueing latency.

    Small, obvious fixes over the previous attempt:
    - Keep strict per-priority FIFO queues but avoid duplicate enqueues (prevents queue blow-up / wasted scans).
    - Fill each pool with *multiple* assignments per tick using local (simulated) available CPU/RAM.
      This increases throughput and reduces queueing delay, improving median/p99 latency.
    - Strongly prioritize QUERY + INTERACTIVE; only schedule BATCH when no runnable high-priority work exists
      (or when high-priority DAGs are blocked), to protect tail latency.
    - Add a simple QUERY/INTERACTIVE mix pattern to prevent INTERACTIVE starvation while still favoring QUERY.
    - Keep OOM-aware RAM backoff retries (robustness), keyed via operator object identity.
    """
    # Per-priority FIFO queues of pipeline_ids
    s.queues = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }
    # Track which pipeline_ids are currently enqueued per priority to prevent duplicates
    s.in_queue = {
        Priority.QUERY: set(),
        Priority.INTERACTIVE: set(),
        Priority.BATCH_PIPELINE: set(),
    }

    # Stable pipeline lookup
    s.pipeline_by_id = {}

    # Operator->pipeline mapping (operator objects appear in ExecutionResult.ops)
    s.op_to_pipeline = {}  # key: id(op) -> pipeline_id

    # Resource hints learned from OOM retries:
    # key: (pipeline_id, op_id) -> {"ram": float, "cpu": float}
    s.op_hints = {}
    s.op_attempts = {}  # key: (pipeline_id, op_id) -> int
    s.max_retries_per_op = 4

    # Non-retriable failed ops (based on error strings in results)
    s.nonretriable_failed_ops = set()  # contains (pipeline_id, op_id)

    # Priority mixing: allow some INTERACTIVE even under heavy QUERY load
    s.hp_pattern = [Priority.QUERY, Priority.QUERY, Priority.INTERACTIVE, Priority.QUERY]
    s.hp_pattern_idx = 0

    # Per-priority default sizing caps (keep small to allow packing; hints can increase RAM on OOM)
    s.cpu_caps = {
        Priority.QUERY: 4.0,
        Priority.INTERACTIVE: 6.0,      # give interactive a bit more CPU to reduce its tail
        Priority.BATCH_PIPELINE: 8.0,
    }
    # RAM as fraction of pool max (small defaults -> more concurrency; OOM backoff grows as needed)
    s.ram_fracs = {
        Priority.QUERY: 0.12,
        Priority.INTERACTIVE: 0.16,
        Priority.BATCH_PIPELINE: 0.20,
    }

    # Safety / runtime limits
    s.max_assignments_per_pool_per_tick = 24
    s.min_cpu = 1.0
    s.min_ram = 1.0


def _is_oom_error(err):
    if err is None:
        return False
    msg = str(err).lower()
    return ("oom" in msg) or ("out of memory" in msg) or ("memoryerror" in msg)


def _enqueue_pipeline(s, pipeline):
    pr = pipeline.priority
    if pr not in s.queues:
        pr = Priority.BATCH_PIPELINE
    pid = pipeline.pipeline_id
    s.pipeline_by_id[pid] = pipeline
    if pid not in s.in_queue[pr]:
        s.queues[pr].append(pid)
        s.in_queue[pr].add(pid)


def _dequeue_runnable_from_priority(s, pr, scheduled_this_tick):
    """Pop/rotate within a priority queue to find a runnable (pipeline, op).
    Returns (pipeline, op) or (None, None). Preserves FIFO-ish order by rotating blocked pipelines.
    """
    q = s.queues[pr]
    if not q:
        return None, None

    # We will scan at most current queue length to avoid infinite loops.
    n = len(q)
    for _ in range(n):
        pid = q.pop(0)
        s.in_queue[pr].discard(pid)

        # Avoid scheduling same pipeline multiple times in one tick (reduces wasted scans)
        if pid in scheduled_this_tick:
            # Put it back at the end
            if pid not in s.in_queue[pr]:
                q.append(pid)
                s.in_queue[pr].add(pid)
            continue

        pipeline = s.pipeline_by_id.get(pid)
        if pipeline is None:
            continue

        status = pipeline.runtime_status()

        # Drop completed pipelines
        if status.is_pipeline_successful():
            continue

        # If there are failures, only continue if they look retriable (OOM backoff).
        if status.state_counts.get(OperatorState.FAILED, 0) > 0:
            failed_ops = status.get_ops([OperatorState.FAILED], require_parents_complete=False) or []
            nonretriable = False
            for op in failed_ops:
                k = (pid, id(op))
                if k in s.nonretriable_failed_ops:
                    nonretriable = True
                    break
                # If we exceeded retry budget, treat as non-retriable to avoid infinite loops
                if s.op_attempts.get(k, 0) > s.max_retries_per_op:
                    nonretriable = True
                    break
            if nonretriable:
                continue  # drop pipeline

        # Find one runnable operator whose parents are complete
        ops = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True) or []
        if not ops:
            # Not runnable yet; rotate to end
            if pid not in s.in_queue[pr]:
                q.append(pid)
                s.in_queue[pr].add(pid)
            continue

        op = ops[0]
        return pipeline, op

    return None, None


def _hp_backlog_exists(s):
    return bool(s.queues[Priority.QUERY] or s.queues[Priority.INTERACTIVE])


def _next_hp_priority(s):
    # Rotate a small pattern so INTERACTIVE isn't starved by QUERY.
    pr = s.hp_pattern[s.hp_pattern_idx % len(s.hp_pattern)]
    s.hp_pattern_idx = (s.hp_pattern_idx + 1) % len(s.hp_pattern)
    return pr


def _compute_request(s, pool, pid, op, pr, local_avail_cpu, local_avail_ram):
    # Default per-priority requests (small to allow packing)
    cpu_cap = min(float(s.cpu_caps.get(pr, 4.0)), float(pool.max_cpu_pool))
    # Use at most a modest share of the remaining CPU to avoid one op monopolizing the pool
    cpu_req = min(cpu_cap, max(s.min_cpu, local_avail_cpu))

    ram_frac = float(s.ram_fracs.get(pr, 0.15))
    ram_cap = max(s.min_ram, float(pool.max_ram_pool) * ram_frac)
    ram_req = min(ram_cap, max(s.min_ram, local_avail_ram))

    # Apply learned hints (from OOM backoff) to avoid repeated failures
    k = (pid, id(op))
    hint = s.op_hints.get(k)
    if hint:
        cpu_req = max(cpu_req, float(hint.get("cpu", cpu_req)))
        ram_req = max(ram_req, float(hint.get("ram", ram_req)))

    # Final caps by local availability and pool maximums
    cpu_req = min(max(s.min_cpu, cpu_req), local_avail_cpu, float(pool.max_cpu_pool))
    ram_req = min(max(s.min_ram, ram_req), local_avail_ram, float(pool.max_ram_pool))

    return cpu_req, ram_req


@register_scheduler(key="scheduler_low_011_r16")
def scheduler_low_011_r16(s, results, pipelines):
    """
    Priority-first packed scheduler.

    Key behavior aimed at lowering latency:
    - Fill each pool with multiple small high-priority ops (reduces queueing delay).
    - Prevent interactive starvation with a simple QUERY/INTERACTIVE pattern.
    - Avoid scheduling batch while high-priority runnable work exists (protects tail latency).
    - OOM retry: double RAM hint on OOM and retry up to a small cap.
    """
    # Ingest new pipelines
    for p in pipelines:
        _enqueue_pipeline(s, p)

    # Process results: learn from failures (especially OOM), and mark non-retriable failures
    for r in results:
        # Update per-op mappings using result ops (best-effort)
        ops = getattr(r, "ops", None) or []
        # Determine failure
        try:
            failed = bool(r.failed())
        except Exception:
            failed = getattr(r, "error", None) is not None

        if not failed:
            # On success we can forget op->pipeline mappings (optional cleanup)
            for op in ops:
                s.op_to_pipeline.pop(id(op), None)
            continue

        # Failure path: decide if retriable OOM
        is_oom = _is_oom_error(getattr(r, "error", None))
        for op in ops:
            op_id = id(op)
            pid = s.op_to_pipeline.get(op_id)
            if pid is None:
                # If we haven't seen this op assigned (should be rare), we can't key hints safely.
                continue

            k = (pid, op_id)
            if is_oom:
                # Increase RAM hint exponentially and retry (bounded)
                prev = s.op_hints.get(k, {})
                baseline_ram = float(prev.get("ram", getattr(r, "ram", 0.0) or 0.0))
                if baseline_ram <= 0.0:
                    baseline_ram = float(getattr(r, "ram", s.min_ram) or s.min_ram)
                new_ram = max(s.min_ram, baseline_ram * 2.0)

                # Keep CPU hint at least what we had (CPU doesn't fix OOM, but avoid shrinking unexpectedly)
                baseline_cpu = float(prev.get("cpu", getattr(r, "cpu", s.min_cpu) or s.min_cpu))
                baseline_cpu = max(s.min_cpu, baseline_cpu)

                s.op_hints[k] = {"ram": new_ram, "cpu": baseline_cpu}
                s.op_attempts[k] = int(s.op_attempts.get(k, 0)) + 1

                # Re-enqueue the pipeline for retry if we still allow retries
                if s.op_attempts[k] <= s.max_retries_per_op:
                    pipeline = s.pipeline_by_id.get(pid)
                    if pipeline is not None:
                        _enqueue_pipeline(s, pipeline)
                else:
                    # Too many retries => treat as non-retriable so we stop looping forever
                    s.nonretriable_failed_ops.add(k)
            else:
                # Non-OOM failures are treated as non-retriable
                s.nonretriable_failed_ops.add(k)

            # Cleanup op mapping for finished attempt
            s.op_to_pipeline.pop(op_id, None)

    # Early exit: if nothing changed, do nothing.
    # (If the simulator calls us only on events, this is safe and faster.)
    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    scheduled_this_tick = set()

    # For each pool, pack as many assignments as fit.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        local_avail_cpu = float(pool.avail_cpu_pool)
        local_avail_ram = float(pool.avail_ram_pool)

        if local_avail_cpu < s.min_cpu or local_avail_ram < s.min_ram:
            continue

        made = 0
        while made < s.max_assignments_per_pool_per_tick:
            if local_avail_cpu < s.min_cpu or local_avail_ram < s.min_ram:
                break

            # First try to schedule high priority. Only backfill with batch if no runnable HP exists.
            pipeline = None
            op = None
            pr = None

            # Try a couple of HP picks per loop to respect the pattern and still make progress
            if _hp_backlog_exists(s):
                # Attempt up to 2 pattern steps to find a runnable HP op
                for _ in range(2):
                    hp_pr = _next_hp_priority(s)
                    if hp_pr not in (Priority.QUERY, Priority.INTERACTIVE):
                        continue
                    pipeline, op = _dequeue_runnable_from_priority(s, hp_pr, scheduled_this_tick)
                    if pipeline is not None:
                        pr = hp_pr
                        break

                # If pattern-selected priority has no runnable op, try the other HP priority as fallback
                if pipeline is None:
                    fallback = Priority.INTERACTIVE if (pr == Priority.QUERY) else Priority.QUERY
                    pipeline, op = _dequeue_runnable_from_priority(s, fallback, scheduled_this_tick)
                    if pipeline is not None:
                        pr = fallback

            # If no runnable HP, try batch as backfill (only to avoid idling when HP DAGs are blocked)
            if pipeline is None:
                pipeline, op = _dequeue_runnable_from_priority(s, Priority.BATCH_PIPELINE, scheduled_this_tick)
                pr = Priority.BATCH_PIPELINE if pipeline is not None else None

            if pipeline is None or op is None or pr is None:
                break

            pid = pipeline.pipeline_id

            # Compute request based on local availability
            cpu_req, ram_req = _compute_request(s, pool, pid, op, pr, local_avail_cpu, local_avail_ram)

            # If we can't fit minimal resources, stop scheduling in this pool for this tick
            if cpu_req < s.min_cpu or ram_req < s.min_ram:
                # Put pipeline back so it isn't lost
                _enqueue_pipeline(s, pipeline)
                break

            # Record operator->pipeline mapping so failures can be attributed later
            s.op_to_pipeline[id(op)] = pid

            # Assign exactly one op per container
            assignments.append(
                Assignment(
                    ops=[op],
                    cpu=cpu_req,
                    ram=ram_req,
                    priority=pr,
                    pool_id=pool_id,
                    pipeline_id=pid,
                )
            )

            # Mark as scheduled this tick to avoid duplicate scheduling across pools in the same tick
            scheduled_this_tick.add(pid)

            # Re-enqueue pipeline for future operators
            _enqueue_pipeline(s, pipeline)

            # Update local availability for packing within this tick
            local_avail_cpu -= float(cpu_req)
            local_avail_ram -= float(ram_req)

            made += 1

    return suspensions, assignments