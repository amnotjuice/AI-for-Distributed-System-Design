# source: scheduler_low_011.py
# model: gpt-5.2-2025-12-11
# effort: low
# feedback: rich
# iteration: r19
@register_scheduler_init(key="scheduler_low_011_r19")
def scheduler_low_011_r19_init(s):
    """Priority-aware, throughput-friendly scheduler focused on reducing end-to-end latency under load.

    Key fixes vs. the previous naive-ish attempt:
    - Never "early exit" when no new events arrive; keep scheduling from queued work (major throughput/latency bug fix).
    - Schedule multiple operators per pool per tick (pack until pool resources are exhausted).
    - Weighted round-robin across priorities to avoid starving interactive/batch while still favoring query latency.
    - Lightweight OOM-aware RAM hinting per operator (keyed by operator object id) with retry limits.
    - Soft isolation: keep pool 0 biased toward high-priority work when multiple pools exist, but don't hard-block others.
    """
    from collections import deque

    s.queues = {
        Priority.QUERY: deque(),
        Priority.INTERACTIVE: deque(),
        Priority.BATCH_PIPELINE: deque(),
    }

    # Weighted RR: favor query, then interactive, then batch; still guarantees progress for lower priorities.
    s.rr_seq = (
        [Priority.QUERY] * 6
        + [Priority.INTERACTIVE] * 2
        + [Priority.BATCH_PIPELINE] * 1
    )
    s.rr_idx = 0

    # Failure-driven hints keyed by operator identity (ExecutionResult does not guarantee pipeline_id).
    s.op_hint_ram = {}        # op_id -> suggested RAM
    s.op_hint_cpu = {}        # op_id -> suggested CPU (rarely used; kept for completeness)
    s.op_attempts = {}        # op_id -> failure count
    s.op_nonretryable = set() # op_id -> don't retry (non-OOM failure or exceeded retry budget)

    # Conservative bounds to prevent infinite thrash and excessive scanning.
    s.max_retries_per_op = 3
    s.max_assignments_per_pool_per_tick = 32
    s.max_pipeline_scan_per_pick = 64

    # If multiple pools exist, bias pool 0 toward high priority to reduce interference.
    s.interactive_pool_id = 0


def _norm_priority(p):
    if p in (Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE):
        return p
    return Priority.BATCH_PIPELINE


def _is_oom_error(err):
    if err is None:
        return False
    msg = str(err).lower()
    return ("oom" in msg) or ("out of memory" in msg) or ("memoryerror" in msg)


def _next_assignable_op(pipeline):
    status = pipeline.runtime_status()
    ops = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
    if not ops:
        return None
    return ops[0]


def _targets_for(pool, prio, qlens, high_pending):
    """Compute default per-op CPU/RAM targets (before hints), shaped to reduce queueing latency."""
    max_cpu = float(pool.max_cpu_pool)
    max_ram = float(pool.max_ram_pool)

    # CPU: choose a "slice" to increase parallelism (reduces queueing), with small boosts when backlog is tiny.
    if prio == Priority.QUERY:
        q = qlens.get(Priority.QUERY, 0)
        if q <= 2:
            cpu_frac, cpu_cap = 0.50, 8.0
        elif q <= 10:
            cpu_frac, cpu_cap = 0.33, 6.0
        else:
            cpu_frac, cpu_cap = 0.25, 4.0
        ram_frac, ram_cap = 0.20, max_ram  # cap left as pool max; fraction is the main limiter
    elif prio == Priority.INTERACTIVE:
        q = qlens.get(Priority.INTERACTIVE, 0)
        if q <= 2:
            cpu_frac, cpu_cap = 0.50, 8.0
        elif q <= 10:
            cpu_frac, cpu_cap = 0.33, 6.0
        else:
            cpu_frac, cpu_cap = 0.25, 4.0
        ram_frac, ram_cap = 0.25, max_ram
    else:
        # Batch should yield when high-priority backlog exists, but still make progress.
        if high_pending:
            cpu_frac, cpu_cap = 0.25, 4.0
            ram_frac, ram_cap = 0.35, max_ram
        else:
            cpu_frac, cpu_cap = 0.50, 8.0
            ram_frac, ram_cap = 0.50, max_ram

    cpu_t = max(1.0, min(cpu_cap, max_cpu * cpu_frac))
    ram_t = max(1.0, min(ram_cap, max_ram * ram_frac))
    return cpu_t, ram_t


def _request_with_hints(s, pool, prio, op_id, qlens, high_pending, avail_cpu, avail_ram):
    cpu_t, ram_t = _targets_for(pool, prio, qlens, high_pending)

    # Apply hints (primarily RAM after OOM).
    if op_id in s.op_hint_cpu:
        try:
            cpu_t = max(cpu_t, float(s.op_hint_cpu[op_id]))
        except Exception:
            pass
    if op_id in s.op_hint_ram:
        try:
            ram_t = max(ram_t, float(s.op_hint_ram[op_id]))
        except Exception:
            pass

    # Cap to what's currently available in this pool (packing loop tracks local availability).
    cpu = min(cpu_t, float(avail_cpu), float(pool.max_cpu_pool))
    ram = min(ram_t, float(avail_ram), float(pool.max_ram_pool))

    # Ensure minimum viable allocation.
    if cpu < 1.0 or ram < 1.0:
        return 0.0, 0.0
    return cpu, ram


@register_scheduler(key="scheduler_low_011_r19")
def scheduler_low_011_r19(s, results, pipelines):
    """
    Priority-aware weighted RR scheduler with multi-assignment packing per pool.

    Main goals:
    - Reduce overall latency by improving throughput (avoid idle pools, schedule even without new arrivals/results).
    - Preserve tail latency for high priorities via weighted selection and pool-0 bias.
    - Prevent starvation of interactive/batch (guaranteed RR turns) while still prioritizing query.
    """
    suspensions = []
    assignments = []

    # Enqueue new arrivals.
    for p in pipelines or []:
        s.queues[_norm_priority(p.priority)].append(p)

    # Update failure-driven hints.
    for r in results or []:
        failed = False
        try:
            failed = r.failed()
        except Exception:
            failed = getattr(r, "error", None) is not None

        if not failed:
            continue

        oom = _is_oom_error(getattr(r, "error", None))
        ops = getattr(r, "ops", None) or []
        for op in ops:
            op_id = id(op)
            s.op_attempts[op_id] = int(s.op_attempts.get(op_id, 0)) + 1

            if not oom:
                # Non-OOM failures are treated as non-retryable to avoid wasting capacity.
                s.op_nonretryable.add(op_id)
                continue

            # OOM: increase RAM hint (multiplicative backoff), retry until budget is exceeded.
            baseline = s.op_hint_ram.get(op_id, None)
            if baseline is None:
                baseline = getattr(r, "ram", None)
            try:
                baseline = float(baseline) if baseline is not None else 1.0
            except Exception:
                baseline = 1.0

            new_hint = max(1.0, baseline * 1.8)
            prev_hint = s.op_hint_ram.get(op_id, 0.0)
            try:
                prev_hint = float(prev_hint)
            except Exception:
                prev_hint = 0.0
            s.op_hint_ram[op_id] = max(prev_hint, new_hint)

            # If we keep failing, stop retrying this op to avoid infinite loops.
            if s.op_attempts[op_id] > s.max_retries_per_op:
                s.op_nonretryable.add(op_id)

    # Snapshot queue lengths for sizing heuristics.
    qlens = {
        Priority.QUERY: len(s.queues[Priority.QUERY]),
        Priority.INTERACTIVE: len(s.queues[Priority.INTERACTIVE]),
        Priority.BATCH_PIPELINE: len(s.queues[Priority.BATCH_PIPELINE]),
    }

    # "High pending" is used to bias batch down when interactive/query backlog exists.
    high_pending = (qlens[Priority.QUERY] + qlens[Priority.INTERACTIVE]) > 0

    # Pack assignments into each pool until resources (or runnable work) are exhausted.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)
        if avail_cpu < 1.0 or avail_ram < 1.0:
            continue

        # Bias pool 0 to high priority: only run batch there if high-priority queues are empty.
        batch_allowed_here = True
        if s.executor.num_pools > 1 and pool_id == s.interactive_pool_id and high_pending:
            batch_allowed_here = False

        for _ in range(s.max_assignments_per_pool_per_tick):
            if avail_cpu < 1.0 or avail_ram < 1.0:
                break

            scheduled = False

            # Try priorities in weighted RR order.
            for _ in range(len(s.rr_seq)):
                pr = s.rr_seq[s.rr_idx]
                s.rr_idx = (s.rr_idx + 1) % len(s.rr_seq)

                if pr == Priority.BATCH_PIPELINE and not batch_allowed_here:
                    continue

                q = s.queues[pr]
                if not q:
                    continue

                # Scan a bounded number of pipelines to find a runnable op.
                scan_limit = min(len(q), s.max_pipeline_scan_per_pick)
                for _scan in range(scan_limit):
                    pipeline = q.popleft()

                    status = pipeline.runtime_status()
                    if status.is_pipeline_successful():
                        # Drop completed pipelines.
                        continue

                    op = _next_assignable_op(pipeline)
                    if op is None:
                        # Not runnable yet; keep it moving to avoid head-of-line blocking.
                        q.append(pipeline)
                        continue

                    op_id = id(op)
                    if op_id in s.op_nonretryable:
                        # Drop pipelines blocked on a non-retryable failed op (avoid wasting cycles).
                        # (If the pipeline had other runnable ops, they'd be returned by get_ops; we only see one here.)
                        continue

                    cpu, ram = _request_with_hints(
                        s=s,
                        pool=pool,
                        prio=pr,
                        op_id=op_id,
                        qlens=qlens,
                        high_pending=high_pending,
                        avail_cpu=avail_cpu,
                        avail_ram=avail_ram,
                    )

                    if cpu <= 0.0 or ram <= 0.0:
                        # Can't fit anything meaningful right now.
                        q.appendleft(pipeline)
                        break

                    if cpu > avail_cpu or ram > avail_ram:
                        # Doesn't fit: requeue and keep scanning in case another pipeline/op fits.
                        q.append(pipeline)
                        continue

                    # Schedule it.
                    assignments.append(
                        Assignment(
                            ops=[op],
                            cpu=cpu,
                            ram=ram,
                            priority=pr,
                            pool_id=pool_id,
                            pipeline_id=pipeline.pipeline_id,
                        )
                    )
                    avail_cpu -= cpu
                    avail_ram -= ram

                    # Requeue pipeline for future ops.
                    q.append(pipeline)

                    scheduled = True
                    break  # stop scanning pipelines for this priority

                if scheduled:
                    break  # move on to place another assignment

            if not scheduled:
                break  # no runnable work that fits in this pool right now

    return suspensions, assignments