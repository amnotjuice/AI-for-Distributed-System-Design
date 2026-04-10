# source: scheduler_low_011.py
# model: gpt-5.2-2025-12-11
# effort: low
# feedback: rich
# iteration: r7
@register_scheduler_init(key="scheduler_low_011_r7")
def scheduler_low_011_r7_init(s):
    """Priority-aware, throughput-friendly scheduler to reduce queueing latency.

    Small, obvious fixes over the prior version:
    1) Avoid starvation: weighted round-robin between QUERY and INTERACTIVE (batch only when headroom exists).
    2) Increase throughput: schedule MULTIPLE operators per pool per scheduler call (not just one).
    3) Right-size by default: start with small CPU/RAM slices to improve concurrency; rely on OOM backoff to learn RAM.
    4) Soft pool preference: pool 0 slightly favors INTERACTIVE, but INTERACTIVE can run anywhere if needed.
    """
    from collections import deque

    s.queues = {
        Priority.QUERY: deque(),
        Priority.INTERACTIVE: deque(),
        Priority.BATCH_PIPELINE: deque(),
    }

    # OOM-aware retry state keyed by operator object identity (good enough within a sim run).
    s.op_ram_hint = {}          # op_id -> suggested_ram
    s.op_cpu_hint = {}          # op_id -> suggested_cpu (mostly stable, but kept for extensibility)
    s.op_attempts = {}          # op_id -> retry attempts
    s.op_retryable_oom = set()  # op_id set: FAILED ops allowed to be retried
    s.op_terminal_fail = set()  # op_id set: FAILED ops that should not be retried

    # Retry policy (keep modest to avoid thrash)
    s.max_oom_retries = 3

    # Weighted RR sequences (pool 0 favors interactive a bit to help tail latency there)
    s.rr_seq_default = (
        [Priority.QUERY] * 3
        + [Priority.INTERACTIVE] * 2
        + [Priority.BATCH_PIPELINE] * 1
    )
    s.rr_seq_pool0 = (
        [Priority.INTERACTIVE] * 3
        + [Priority.QUERY] * 2
        + [Priority.BATCH_PIPELINE] * 1
    )
    s.rr_idx = 0

    # Concurrency-friendly default sizing:
    # - cap CPU to small numbers (sublinear scaling; better to run more in parallel)
    # - RAM as a small fraction of pool max; OOM will trigger backoff hints
    s.cpu_cap = {
        Priority.QUERY: 2.0,
        Priority.INTERACTIVE: 2.0,
        Priority.BATCH_PIPELINE: 4.0,
    }
    s.ram_frac = {
        Priority.QUERY: 0.10,
        Priority.INTERACTIVE: 0.12,
        Priority.BATCH_PIPELINE: 0.25,
    }

    # Keep headroom for high priority by limiting batch admission when pool is tight
    s.batch_headroom_frac = 0.15

    # Safety limits per call
    s.max_assignments_per_call = 128
    s.scan_limit_per_pick = 32

    # If multiple pools exist, pool 0 is treated as "interactive-preferred"
    s.interactive_pool_id = 0


def _r7_is_oom_error(err):
    if err is None:
        return False
    msg = str(err).lower()
    return ("oom" in msg) or ("out of memory" in msg) or ("memoryerror" in msg)


def _r7_pipeline_has_nonretryable_failure(s, pipeline):
    """Drop pipelines that have FAILED ops we cannot/should not retry."""
    status = pipeline.runtime_status()
    failed_ops = status.get_ops([OperatorState.FAILED], require_parents_complete=False) or []
    if not failed_ops:
        return False
    # If any failed op is terminal (or not marked retryable), treat pipeline as failed for our purposes
    for op in failed_ops:
        op_id = id(op)
        if op_id in s.op_terminal_fail:
            return True
        if op_id not in s.op_retryable_oom:
            return True
        if s.op_attempts.get(op_id, 0) > s.max_oom_retries:
            return True
    return False


def _r7_pick_ready_op(s, pipeline):
    """Pick a single ready operator to run next, preferring PENDING over FAILED retries."""
    status = pipeline.runtime_status()

    pending = status.get_ops([OperatorState.PENDING], require_parents_complete=True) or []
    if pending:
        return pending[0]

    # No pending ready op; allow retrying FAILED ops if they are marked retryable (e.g., OOM backoff)
    failed_ready = status.get_ops([OperatorState.FAILED], require_parents_complete=True) or []
    for op in failed_ready:
        op_id = id(op)
        if op_id in s.op_retryable_oom and s.op_attempts.get(op_id, 0) <= s.max_oom_retries:
            return op

    return None


def _r7_compute_request(s, pool, priority, op):
    """Compute (cpu, ram) request for this op in this pool using small defaults + hints."""
    # Default small slices for concurrency
    cpu = min(s.cpu_cap.get(priority, 2.0), float(pool.avail_cpu_pool), float(pool.max_cpu_pool))
    cpu = max(1.0, cpu)

    # RAM default is a fraction of pool max (also concurrency-friendly)
    ram_default = float(pool.max_ram_pool) * float(s.ram_frac.get(priority, 0.15))
    ram = min(ram_default, float(pool.avail_ram_pool), float(pool.max_ram_pool))
    ram = max(1.0, ram)

    # Apply per-op hints (primarily from OOM retries)
    op_id = id(op)
    if op_id in s.op_cpu_hint:
        cpu = max(cpu, float(s.op_cpu_hint[op_id]))
    if op_id in s.op_ram_hint:
        ram = max(ram, float(s.op_ram_hint[op_id]))

    # Cap to pool limits and current availability
    cpu = min(cpu, float(pool.avail_cpu_pool), float(pool.max_cpu_pool))
    ram = min(ram, float(pool.avail_ram_pool), float(pool.max_ram_pool))

    # Still ensure positive requests
    cpu = max(1.0, cpu)
    ram = max(1.0, ram)
    return cpu, ram


def _r7_rr_sequence_for_pool(s, pool_id):
    return s.rr_seq_pool0 if pool_id == getattr(s, "interactive_pool_id", 0) else s.rr_seq_default


@register_scheduler(key="scheduler_low_011_r7")
def scheduler_low_011_r7(s, results, pipelines):
    """
    Weighted RR + multi-assign per pool + OOM backoff.

    Key intended latency win: reduce queueing delays by (a) avoiding starvation between QUERY/INTERACTIVE,
    and (b) increasing concurrency via smaller default container sizes and multiple assignments per pool call.
    """
    from collections import deque  # noqa: F401  (kept local per instructions)

    # Enqueue new arrivals
    for p in pipelines:
        pr = p.priority
        if pr not in s.queues:
            pr = Priority.BATCH_PIPELINE
        s.queues[pr].append(p)

    # Process results to learn OOM backoff hints
    for r in results:
        # Determine failure
        failed = False
        try:
            failed = r.failed()
        except Exception:
            failed = getattr(r, "error", None) is not None

        if not failed:
            continue

        ops = getattr(r, "ops", None) or []
        err = getattr(r, "error", None)

        if _r7_is_oom_error(err):
            # Mark retryable and increase RAM hint (exponential backoff)
            for op in ops:
                op_id = id(op)
                prev_attempts = int(s.op_attempts.get(op_id, 0))
                next_attempts = prev_attempts + 1
                s.op_attempts[op_id] = next_attempts

                if next_attempts <= s.max_oom_retries:
                    s.op_retryable_oom.add(op_id)
                    # Baseline from current allocation if provided; else from previous hint; else small
                    base_ram = float(getattr(r, "ram", 0.0) or 0.0)
                    if base_ram <= 0.0:
                        base_ram = float(s.op_ram_hint.get(op_id, 1.0))
                    new_ram = max(1.0, base_ram * 2.0)
                    s.op_ram_hint[op_id] = new_ram

                    # Keep CPU stable but remember last allocation (can be useful if caller changes defaults)
                    base_cpu = float(getattr(r, "cpu", 0.0) or 0.0)
                    if base_cpu > 0.0:
                        s.op_cpu_hint[op_id] = max(1.0, base_cpu)
                else:
                    # Too many retries: treat as terminal to avoid infinite cycling
                    s.op_retryable_oom.discard(op_id)
                    s.op_terminal_fail.add(op_id)
        else:
            # Non-OOM failure: never retry
            for op in ops:
                op_id = id(op)
                s.op_retryable_oom.discard(op_id)
                s.op_terminal_fail.add(op_id)

    # Early exit if no decisions needed
    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []
    total_assignments = 0

    # Per-pool scheduling: fill each pool with as many ops as it can take
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]

        # Track local availability to avoid over-assigning within this single scheduler call
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)
        if avail_cpu <= 0.0 or avail_ram <= 0.0:
            continue

        rr_seq = _r7_rr_sequence_for_pool(s, pool_id)
        if not rr_seq:
            continue

        # Precompute batch headroom threshold for this pool
        batch_reserved_cpu = float(pool.max_cpu_pool) * float(s.batch_headroom_frac)
        batch_reserved_ram = float(pool.max_ram_pool) * float(s.batch_headroom_frac)

        # Inner loop: keep assigning while resources remain and we haven't hit call cap
        # Use a bounded number of attempts to prevent infinite loops when nothing fits.
        local_no_progress = 0
        while (
            avail_cpu >= 1.0
            and avail_ram >= 1.0
            and total_assignments < s.max_assignments_per_call
            and local_no_progress < (len(rr_seq) * 4)
        ):
            picked = False

            # Try a handful of RR steps to find schedulable work for this pool
            for _ in range(len(rr_seq)):
                pr = rr_seq[s.rr_idx % len(rr_seq)]
                s.rr_idx = (s.rr_idx + 1) % len(rr_seq)

                q = s.queues.get(pr)
                if not q:
                    continue
                if len(q) == 0:
                    continue

                # Batch headroom gate: only schedule batch if pool has room beyond reserved headroom
                if pr == Priority.BATCH_PIPELINE:
                    if (avail_cpu <= batch_reserved_cpu) or (avail_ram <= batch_reserved_ram):
                        continue

                # Scan/rotate within this priority queue for a runnable op that fits
                scan = min(len(q), int(s.scan_limit_per_pick))
                chosen_pipeline = None
                chosen_op = None
                chosen_cpu = None
                chosen_ram = None

                for _scan_i in range(scan):
                    p = q.popleft()

                    status = p.runtime_status()
                    if status.is_pipeline_successful():
                        # Drop completed pipeline
                        continue
                    if _r7_pipeline_has_nonretryable_failure(s, p):
                        # Drop failed pipeline
                        continue

                    op = _r7_pick_ready_op(s, p)
                    if op is None:
                        # Not ready yet; keep it circulating
                        q.append(p)
                        continue

                    # Compute request and check fit against LOCAL availability (not global pool state)
                    cpu_req, ram_req = _r7_compute_request(s, pool, pr, op)

                    # If batch, also cap to "excess" above reserved headroom to protect latency
                    if pr == Priority.BATCH_PIPELINE:
                        cpu_cap = max(0.0, avail_cpu - batch_reserved_cpu)
                        ram_cap = max(0.0, avail_ram - batch_reserved_ram)
                        cpu_req = min(cpu_req, cpu_cap) if cpu_cap > 0 else 0.0
                        ram_req = min(ram_req, ram_cap) if ram_cap > 0 else 0.0

                    if cpu_req >= 1.0 and ram_req >= 1.0 and cpu_req <= avail_cpu and ram_req <= avail_ram:
                        chosen_pipeline = p
                        chosen_op = op
                        chosen_cpu = cpu_req
                        chosen_ram = ram_req
                        break
                    else:
                        # Doesn't fit right now; keep pipeline in queue
                        q.append(p)

                if chosen_pipeline is None:
                    # Try next priority in RR sequence
                    continue

                # Make assignment (one op per container)
                assignments.append(
                    Assignment(
                        ops=[chosen_op],
                        cpu=chosen_cpu,
                        ram=chosen_ram,
                        priority=pr,
                        pool_id=pool_id,
                        pipeline_id=chosen_pipeline.pipeline_id,
                    )
                )
                total_assignments += 1

                # Update local availability
                avail_cpu -= float(chosen_cpu)
                avail_ram -= float(chosen_ram)

                # Re-enqueue pipeline for subsequent ops
                s.queues[pr].append(chosen_pipeline)

                picked = True
                break  # move to next assignment attempt

            if not picked:
                local_no_progress += 1
            else:
                local_no_progress = 0

    return suspensions, assignments