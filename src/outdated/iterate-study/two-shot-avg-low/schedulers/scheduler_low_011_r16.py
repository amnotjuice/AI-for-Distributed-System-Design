# source: scheduler_low_011.py
# model: gpt-5.2-2025-12-11
# effort: low
# feedback: minimal
# iteration: r16
@register_scheduler_init(key="scheduler_low_011_r16")
def scheduler_low_011_r16_init(s):
    """Priority-aware FIFO++ tuned for lower latency (iteration r16).

    Small-but-meaningful improvements over the naive FIFO example:
    1) Priority queues: always consider QUERY/INTERACTIVE before BATCH.
    2) Multi-assign per pool per tick: avoid leaving resources idle (reduces queueing delay).
    3) Pool isolation when multiple pools exist: keep pool 0 "interactive" (no batch there).
    4) Admission shaping: when only one pool exists (or contention is high), keep a small
       CPU/RAM reserve so batch cannot fully crowd out interactive work.
    5) OOM-aware retries: if an operator fails with an OOM-like error, retry it with
       increased RAM (exponential backoff), rather than dropping the whole pipeline.
    """
    # FIFO waiting queues per priority (pipelines are re-enqueued until completed/failed)
    s.waiting_queues = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }
    # Track membership to avoid duplicates in queues
    s.in_queue = {
        Priority.QUERY: set(),
        Priority.INTERACTIVE: set(),
        Priority.BATCH_PIPELINE: set(),
    }

    # Learned per-operator RAM hints (to avoid repeated OOMs). Key includes pipeline_id when known.
    s.op_ram_hint = {}          # op_key -> ram
    s.op_retry_count = {}       # op_key -> int
    s.op_retryable_oom = set()  # op_key
    s.op_nonretry_fail = set()  # op_key (non-OOM failures)

    # Config knobs (kept simple)
    s.max_retries_per_op = 3

    # Pool preference: pool 0 is treated as "interactive" when multiple pools exist
    s.interactive_pool_id = 0

    # Per-priority target chunk sizes (fractions of pool max). Smaller chunks -> more concurrency -> less queueing.
    s.cpu_frac = {
        Priority.QUERY: 0.50,
        Priority.INTERACTIVE: 0.50,
        Priority.BATCH_PIPELINE: 0.25,
    }
    s.ram_frac = {
        Priority.QUERY: 0.40,
        Priority.INTERACTIVE: 0.40,
        Priority.BATCH_PIPELINE: 0.60,
    }

    # Reserve a portion of resources to protect latency when high-priority is waiting
    s.reserve_cpu_frac = 0.25
    s.reserve_ram_frac = 0.25

    # Soft cap: avoid scheduling too many ops per pool per tick (prevents extreme bursts)
    s.max_assignments_per_pool_per_tick = 8

    s._tick = 0


def _prio_order():
    return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]


def _op_identity(op):
    # Prefer a stable op_id if present; otherwise use object identity.
    return getattr(op, "op_id", id(op))


def _op_key_variants(pipeline_id, op):
    # We store some failure learning without pipeline_id (if ExecutionResult lacks it),
    # so check both (pipeline-specific) and (None, op_id) variants.
    oid = _op_identity(op)
    return ((pipeline_id, oid), (None, oid))


def _is_oom_error(err):
    if err is None:
        return False
    msg = str(err).lower()
    return ("oom" in msg) or ("out of memory" in msg) or ("memoryerror" in msg) or ("killed process" in msg)


def _queue_len(s, pr):
    return len(s.waiting_queues[pr])


def _enqueue_pipeline(s, pr, pipeline):
    pid = pipeline.pipeline_id
    if pid in s.in_queue[pr]:
        return
    s.waiting_queues[pr].append(pipeline)
    s.in_queue[pr].add(pid)


def _dequeue_pipeline(s, pr):
    q = s.waiting_queues[pr]
    if not q:
        return None
    p = q.pop(0)
    s.in_queue[pr].discard(p.pipeline_id)
    return p


def _pipeline_has_nonretry_failure(s, pipeline):
    status = pipeline.runtime_status()
    # If there are FAILED ops, only allow retry if they are known OOM failures and within retry budget.
    failed_ops = status.get_ops([OperatorState.FAILED], require_parents_complete=False) or []
    if not failed_ops:
        return False

    for op in failed_ops:
        variants = _op_key_variants(pipeline.pipeline_id, op)
        # Any known non-retry failure => drop pipeline
        if any(k in s.op_nonretry_fail for k in variants):
            return True

        # If we have no signal that this failure is retryable OOM, treat as non-retry (conservative)
        if not any(k in s.op_retryable_oom for k in variants):
            return True

        # If it is retryable OOM, ensure retry budget remains (check any variant with a count)
        retry_counts = [s.op_retry_count.get(k, 0) for k in variants]
        if max(retry_counts) > s.max_retries_per_op:
            return True

    return False


def _next_ready_op(pipeline):
    status = pipeline.runtime_status()
    ops = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True) or []
    if not ops:
        return None
    return ops[0]


def _can_run_batch_in_pool(s, pool_id):
    # With multiple pools, keep the interactive pool batch-free to protect latency.
    if s.executor.num_pools > 1 and pool_id == s.interactive_pool_id:
        return False
    return True


def _requested_resources(s, pool, pr, pipeline_id, op, local_avail_cpu, local_avail_ram):
    # Base chunk request (fractions of max), capped to local availability.
    cpu = pool.max_cpu_pool * float(s.cpu_frac.get(pr, 1.0))
    ram = pool.max_ram_pool * float(s.ram_frac.get(pr, 1.0))

    cpu = max(1.0, cpu)
    ram = max(1.0, ram)

    # Apply learned OOM RAM hints (take max to avoid repeated OOMs)
    for k in _op_key_variants(pipeline_id, op):
        if k in s.op_ram_hint:
            ram = max(ram, float(s.op_ram_hint[k]))

    # Cap to what we can allocate now
    cpu = min(cpu, local_avail_cpu)
    ram = min(ram, local_avail_ram)

    # Keep allocations positive
    cpu = max(1.0, cpu)
    ram = max(1.0, ram)
    return cpu, ram


@register_scheduler(key="scheduler_low_011_r16")
def scheduler_low_011_r16(s, results, pipelines):
    """
    Priority-first, latency-protecting scheduler:
    - Enqueue new pipelines into per-priority FIFO queues (de-duplicated).
    - Learn from failures: OOM failures trigger RAM backoff + retry; other failures are not retried.
    - Schedule multiple ops per pool per tick using conservative per-op "chunk" sizing to reduce queueing.
    - If multiple pools: reserve pool 0 for QUERY/INTERACTIVE (no batch there).
    - If single pool: keep a small CPU/RAM reserve whenever high-priority is waiting to avoid crowd-out.
    """
    s._tick += 1

    # Enqueue new arrivals
    for p in pipelines:
        pr = p.priority if p.priority in s.waiting_queues else Priority.BATCH_PIPELINE
        _enqueue_pipeline(s, pr, p)

    # Process results to learn OOM vs non-OOM failures and update RAM hints
    for r in results:
        failed = False
        try:
            failed = r.failed()
        except Exception:
            failed = getattr(r, "error", None) is not None

        if not failed:
            continue

        ops = getattr(r, "ops", None) or []
        pipeline_id = getattr(r, "pipeline_id", None)  # may not exist in some simulator versions
        is_oom = _is_oom_error(getattr(r, "error", None))

        for op in ops:
            # Record keys (with and without pipeline id)
            for k in _op_key_variants(pipeline_id, op):
                if is_oom:
                    s.op_retryable_oom.add(k)
                    s.op_retry_count[k] = int(s.op_retry_count.get(k, 0)) + 1

                    # Exponential RAM backoff from the last attempted allocation (if available)
                    baseline = float(getattr(r, "ram", 1.0) or 1.0)
                    prev = float(s.op_ram_hint.get(k, 0.0) or 0.0)
                    # Ensure we make progress: double the max of (prev hint, last allocation)
                    new_hint = max(1.0, max(prev, baseline) * 2.0)
                    s.op_ram_hint[k] = new_hint
                else:
                    s.op_nonretry_fail.add(k)

    # Nothing to do
    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    # Global signal: if any high-priority work is waiting, protect it (reservations & ordering)
    high_waiting = (_queue_len(s, Priority.QUERY) + _queue_len(s, Priority.INTERACTIVE)) > 0

    # Prevent one pipeline from being scheduled multiple times in the same tick (fairness)
    scheduled_pipeline_ids = set()

    # Schedule per pool: multi-assign until resources are consumed or no eligible work
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        local_avail_cpu = float(pool.avail_cpu_pool)
        local_avail_ram = float(pool.avail_ram_pool)

        if local_avail_cpu <= 0 or local_avail_ram <= 0:
            continue

        # If single pool and high-priority is waiting, keep a reserve (batch cannot consume it)
        reserve_cpu = float(pool.max_cpu_pool) * float(s.reserve_cpu_frac) if (s.executor.num_pools == 1 and high_waiting) else 0.0
        reserve_ram = float(pool.max_ram_pool) * float(s.reserve_ram_frac) if (s.executor.num_pools == 1 and high_waiting) else 0.0

        made = 0
        while made < s.max_assignments_per_pool_per_tick and local_avail_cpu > 0 and local_avail_ram > 0:
            chosen = None  # (pipeline, pr, op, cpu, ram)

            # Priority order always; batch might be disallowed in interactive pool
            for pr in _prio_order():
                if pr == Priority.BATCH_PIPELINE and not _can_run_batch_in_pool(s, pool_id):
                    continue

                q = s.waiting_queues[pr]
                if not q:
                    continue

                # Scan at most current queue length to find a runnable pipeline (avoid infinite loops)
                scan = len(q)
                for _ in range(scan):
                    p = _dequeue_pipeline(s, pr)
                    if p is None:
                        break

                    status = p.runtime_status()

                    # Drop completed pipelines
                    if status.is_pipeline_successful():
                        continue

                    # Drop pipelines with known non-retry failures (but allow OOM retries)
                    if _pipeline_has_nonretry_failure(s, p):
                        continue

                    # Avoid scheduling same pipeline multiple times in one tick
                    if p.pipeline_id in scheduled_pipeline_ids:
                        _enqueue_pipeline(s, pr, p)
                        continue

                    op = _next_ready_op(p)
                    if op is None:
                        # Not ready yet; keep it queued
                        _enqueue_pipeline(s, pr, p)
                        continue

                    # Enforce reservation: when high-priority is waiting on a single pool,
                    # batch must leave reserve headroom.
                    effective_cpu = local_avail_cpu
                    effective_ram = local_avail_ram
                    if pr == Priority.BATCH_PIPELINE and (reserve_cpu > 0.0 or reserve_ram > 0.0):
                        effective_cpu = max(0.0, local_avail_cpu - reserve_cpu)
                        effective_ram = max(0.0, local_avail_ram - reserve_ram)
                        if effective_cpu < 1.0 or effective_ram < 1.0:
                            # Can't run batch without consuming reserve; requeue and try other priorities
                            _enqueue_pipeline(s, pr, p)
                            continue

                    cpu, ram = _requested_resources(s, pool, pr, p.pipeline_id, op, effective_cpu, effective_ram)

                    # If still doesn't fit, requeue and try others
                    if cpu > effective_cpu or ram > effective_ram:
                        _enqueue_pipeline(s, pr, p)
                        continue

                    chosen = (p, pr, op, cpu, ram)
                    break

                if chosen is not None:
                    break

            if chosen is None:
                break

            p, pr, op, cpu, ram = chosen

            assignments.append(
                Assignment(
                    ops=[op],
                    cpu=cpu,
                    ram=ram,
                    priority=pr,
                    pool_id=pool_id,
                    pipeline_id=p.pipeline_id,
                )
            )

            made += 1
            scheduled_pipeline_ids.add(p.pipeline_id)

            # Update local availability as if the assignment is accepted (simulator applies later)
            local_avail_cpu -= float(cpu)
            local_avail_ram -= float(ram)

            # Re-enqueue pipeline so subsequent operators can be scheduled in future ticks
            _enqueue_pipeline(s, pr, p)

    return suspensions, assignments