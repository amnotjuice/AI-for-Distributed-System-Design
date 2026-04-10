# source: scheduler_low_011.py
# model: gpt-5.2-2025-12-11
# effort: low
# feedback: minimal
# iteration: r1
@register_scheduler_init(key="scheduler_low_011_r1")
def scheduler_low_011_r1_init(s):
    """Priority-aware, latency-focused scheduler (incremental improvement over naive FIFO).

    Key changes vs prior iteration:
    - Fix obvious flaw: do NOT drop pipelines just because they have FAILED ops; allow OOM retries (ASSIGNABLE_STATES includes FAILED).
    - Strict priority ordering (QUERY > INTERACTIVE > BATCH) with an "interactive pool" preference when multiple pools exist.
    - Reservation on the interactive pool: when any high-priority work is waiting, batch is only allowed to consume non-reserved headroom.
    - Allow multiple assignments per pool per tick (bounded), improving start latency under bursty arrivals.
    - Controlled spillover: if high-priority waits too long, allow it to run on non-interactive pools.
    - OOM-aware RAM backoff per (pipeline, op) with capped retries.

    This policy stays intentionally simple: no preemption and no sophisticated runtime prediction.
    """
    s.tick = 0

    # FIFO queues per priority
    s.waiting_queues = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }

    # Track whether a pipeline_id is already enqueued (prevents duplicates)
    s.in_queue = {}  # pipeline_id -> Priority

    # For spillover decisions (when did this pipeline arrive)
    s.enqueue_tick = {}  # pipeline_id -> tick

    # Per-operator hints & retry bookkeeping (keyed by (pipeline_id, id(op)))
    s.op_hints = {}  # (pid, op_id) -> {"ram": float, "cpu": float}
    s.op_attempts = {}  # (pid, op_id) -> int
    s.op_last_failure_oom = {}  # (pid, op_id) -> bool

    # Retry budget
    s.max_retries_per_op = 3

    # Pool preference
    s.interactive_pool_id = 0

    # Spillover for high-priority to non-interactive pools (in scheduler ticks)
    s.spillover_ticks = 3

    # Reservations (only enforced on interactive pool when any high-priority is waiting)
    s.reserve_cpu_frac = 0.30
    s.reserve_ram_frac = 0.30

    # Default sizing by priority (fractions of pool max; capped by pool available)
    # Latency bias: give high-priority enough CPU to finish quickly, but not so much that only 1 can run.
    s.cpu_frac = {
        Priority.QUERY: 0.70,
        Priority.INTERACTIVE: 0.60,
        Priority.BATCH_PIPELINE: 1.00,
    }
    # RAM doesn't speed up beyond minimum; keep moderate defaults and rely on OOM backoff to learn minima.
    s.ram_frac = {
        Priority.QUERY: 0.35,
        Priority.INTERACTIVE: 0.35,
        Priority.BATCH_PIPELINE: 0.80,
    }

    # Bounds to avoid pathological tiny allocations
    s.min_cpu = 1.0
    s.min_ram = 1.0

    # Bounded work per pool per tick (prevents long loops)
    s.max_assignments_per_pool = 8

    # Avoid one pipeline dominating within a tick
    s.max_ops_per_pipeline_per_tick = {
        Priority.QUERY: 2,
        Priority.INTERACTIVE: 2,
        Priority.BATCH_PIPELINE: 1,
    }

    # Scanning bounds to avoid O(n^2) when queues are large
    s.scan_limit = 32


def _priority_order():
    return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]


def _is_oom_error(err):
    if err is None:
        return False
    msg = str(err).lower()
    return ("oom" in msg) or ("out of memory" in msg) or ("memoryerror" in msg)


def _op_key(pipeline_id, op):
    # pipeline_id is expected to be stable and unique; op identity is stable within the simulation process.
    return (pipeline_id, id(op))


def _get_state_count(status, state):
    # status.state_counts may be a Counter-like mapping
    try:
        return status.state_counts.get(state, 0)
    except Exception:
        try:
            return status.state_counts[state]
        except Exception:
            return 0


def _pipeline_terminal_or_success(s, pipeline):
    status = pipeline.runtime_status()
    if status.is_pipeline_successful():
        return True

    # If there are failed ops, only keep the pipeline if all FAILED ops are OOM-retriable and within retry budget.
    failed_cnt = _get_state_count(status, OperatorState.FAILED)
    if failed_cnt <= 0:
        return False

    failed_ops = status.get_ops([OperatorState.FAILED], require_parents_complete=False) or []
    for op in failed_ops:
        k = _op_key(pipeline.pipeline_id, op)
        attempts = int(s.op_attempts.get(k, 0))
        last_oom = bool(s.op_last_failure_oom.get(k, False))
        if (not last_oom) or (attempts > s.max_retries_per_op):
            return True  # terminal failure (unretriable or exceeded retries)
    return False  # keep retrying


def _any_active_high_priority_waiting(s):
    # Lightweight check: scan a bounded prefix of high-priority queues and see if any pipeline is still active.
    for pr in (Priority.QUERY, Priority.INTERACTIVE):
        q = s.waiting_queues.get(pr, [])
        for i in range(min(len(q), s.scan_limit)):
            p = q[i]
            if not _pipeline_terminal_or_success(s, p):
                return True
    return False


def _eligible_for_pool(s, pipeline, pool_id):
    pr = pipeline.priority
    if s.executor.num_pools <= 1:
        return True

    if pr in (Priority.QUERY, Priority.INTERACTIVE):
        if pool_id == s.interactive_pool_id:
            return True
        # Spillover after waiting long enough
        enq = int(s.enqueue_tick.get(pipeline.pipeline_id, s.tick))
        return (s.tick - enq) >= int(s.spillover_ticks)

    # Batch: prefer non-interactive pools but allow anywhere
    return True


def _compute_request(s, pool, pipeline, op, pr, rem_cpu, rem_ram, reserve_cpu, reserve_ram, high_prio_waiting, pool_id):
    # For batch on interactive pool when high-priority is waiting: enforce reservation
    avail_cpu_for_task = rem_cpu
    avail_ram_for_task = rem_ram
    if high_prio_waiting and pool_id == s.interactive_pool_id and pr == Priority.BATCH_PIPELINE:
        avail_cpu_for_task = max(0.0, rem_cpu - reserve_cpu)
        avail_ram_for_task = max(0.0, rem_ram - reserve_ram)

    if avail_cpu_for_task < s.min_cpu or avail_ram_for_task < s.min_ram:
        return None

    # Defaults from fractions of pool MAX (not remaining) to avoid oscillating allocations
    base_cpu = max(s.min_cpu, pool.max_cpu_pool * float(s.cpu_frac.get(pr, 1.0)))
    base_ram = max(s.min_ram, pool.max_ram_pool * float(s.ram_frac.get(pr, 1.0)))

    # Apply OOM-learned hints for this specific op
    k = _op_key(pipeline.pipeline_id, op)
    hint = s.op_hints.get(k)
    if hint:
        try:
            base_cpu = max(base_cpu, float(hint.get("cpu", base_cpu)))
        except Exception:
            pass
        try:
            base_ram = max(base_ram, float(hint.get("ram", base_ram)))
        except Exception:
            pass

    # Cap by task-available headroom and pool max
    cpu = min(base_cpu, avail_cpu_for_task, pool.max_cpu_pool)
    ram = min(base_ram, avail_ram_for_task, pool.max_ram_pool)

    # Ensure minimal positive allocations
    cpu = max(s.min_cpu, cpu)
    ram = max(s.min_ram, ram)

    # Final feasibility check
    if cpu > avail_cpu_for_task or ram > avail_ram_for_task:
        return None
    return cpu, ram


def _next_assignable_op(pipeline):
    status = pipeline.runtime_status()
    ops = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True) or []
    if not ops:
        return None
    return ops[0]


@register_scheduler(key="scheduler_low_011_r1")
def scheduler_low_011_r1(s, results, pipelines):
    s.tick += 1

    # Ingest new pipelines (deduplicate by pipeline_id)
    for p in pipelines:
        pr = p.priority if p.priority in s.waiting_queues else Priority.BATCH_PIPELINE
        pid = p.pipeline_id
        if pid not in s.in_queue:
            s.in_queue[pid] = pr
            s.enqueue_tick[pid] = s.tick
            s.waiting_queues[pr].append(p)

    # Learn from execution results (OOM -> increase RAM hint, retry up to budget)
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
        ops = getattr(r, "ops", None) or []
        pid = getattr(r, "pipeline_id", None)

        # If pipeline_id is not present on results, we can't reliably key hints per pipeline.
        # Use a sentinel pid to at least avoid crashes; effectiveness depends on simulator providing pipeline_id.
        if pid is None:
            pid = -1

        for op in ops:
            k = (pid, id(op))
            s.op_last_failure_oom[k] = bool(is_oom)
            if not is_oom:
                continue

            # Count retries and grow RAM hint exponentially
            prev_attempts = int(s.op_attempts.get(k, 0))
            s.op_attempts[k] = prev_attempts + 1

            prev_hint = s.op_hints.get(k, {})
            prev_ram = float(prev_hint.get("ram", 0.0) or 0.0)
            prev_cpu = float(prev_hint.get("cpu", 0.0) or 0.0)

            observed_ram = float(getattr(r, "ram", 0.0) or 0.0)
            observed_cpu = float(getattr(r, "cpu", 0.0) or 0.0)

            baseline_ram = max(prev_ram, observed_ram, s.min_ram)
            new_ram = max(s.min_ram, baseline_ram * 2.0)

            # Keep CPU hint as at least what we observed (rarely critical for OOM, but stable)
            new_cpu = max(s.min_cpu, prev_cpu, observed_cpu, s.min_cpu)

            s.op_hints[k] = {"ram": new_ram, "cpu": new_cpu}

    # If no changes, exit quickly
    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    # Track per-tick assignment counts to prevent a single pipeline from dominating within one scheduler call
    assigned_ops_this_tick = set()  # (pipeline_id, op_id)
    assigned_count_by_pid = {}

    high_prio_waiting = _any_active_high_priority_waiting(s)

    # Pool iteration order: try interactive pool first if it exists
    pool_order = list(range(s.executor.num_pools))
    if s.executor.num_pools > 1 and s.interactive_pool_id in pool_order:
        pool_order = [s.interactive_pool_id] + [i for i in pool_order if i != s.interactive_pool_id]

    # Core scheduling loop: schedule up to N per pool, strict priority, reservations on interactive pool
    for pool_id in pool_order:
        pool = s.executor.pools[pool_id]
        rem_cpu = float(pool.avail_cpu_pool)
        rem_ram = float(pool.avail_ram_pool)
        if rem_cpu < s.min_cpu or rem_ram < s.min_ram:
            continue

        reserve_cpu = float(pool.max_cpu_pool) * float(s.reserve_cpu_frac) if high_prio_waiting else 0.0
        reserve_ram = float(pool.max_ram_pool) * float(s.reserve_ram_frac) if high_prio_waiting else 0.0

        made = 0
        # Bounded attempts to find work that fits and is eligible (prevents spinning)
        attempts = 0
        max_attempts = s.max_assignments_per_pool * len(_priority_order()) * s.scan_limit

        while made < s.max_assignments_per_pool and attempts < max_attempts:
            attempts += 1
            scheduled_one = False

            for pr in _priority_order():
                q = s.waiting_queues.get(pr, [])
                if not q:
                    continue

                # Scan a bounded number of queue items to find an eligible, active pipeline
                chosen_pipeline = None
                scan_n = min(len(q), s.scan_limit)
                for _ in range(scan_n):
                    p = q.pop(0)

                    # Drop completed / terminally failed pipelines
                    if _pipeline_terminal_or_success(s, p):
                        s.in_queue.pop(p.pipeline_id, None)
                        continue

                    # Pool eligibility (spillover for high priority)
                    if not _eligible_for_pool(s, p, pool_id):
                        q.append(p)
                        continue

                    # Enforce per-pipeline per-tick cap
                    pid = p.pipeline_id
                    cap = int(s.max_ops_per_pipeline_per_tick.get(pr, 1))
                    if int(assigned_count_by_pid.get(pid, 0)) >= cap:
                        q.append(p)
                        continue

                    chosen_pipeline = p
                    break

                if chosen_pipeline is None:
                    continue

                # Pick an operator (respecting parent completion)
                op = _next_assignable_op(chosen_pipeline)
                if op is None:
                    # Not ready; rotate to back
                    q.append(chosen_pipeline)
                    continue

                k = _op_key(chosen_pipeline.pipeline_id, op)

                # Avoid double-assigning the same op within one tick
                if k in assigned_ops_this_tick:
                    q.append(chosen_pipeline)
                    continue

                # If this op has exceeded retry budget (only meaningful if it previously OOM-failed), drop pipeline as terminal
                if int(s.op_attempts.get(k, 0)) > int(s.max_retries_per_op):
                    s.in_queue.pop(chosen_pipeline.pipeline_id, None)
                    # Do not requeue
                    continue

                # Compute resources; enforce reservation for batch on interactive pool
                req = _compute_request(
                    s, pool, chosen_pipeline, op, pr,
                    rem_cpu, rem_ram, reserve_cpu, reserve_ram,
                    high_prio_waiting, pool_id
                )
                if req is None:
                    # Can't fit; rotate and continue (maybe another pool/tick will work)
                    q.append(chosen_pipeline)
                    continue

                cpu, ram = req

                # Commit assignment
                assignments.append(
                    Assignment(
                        ops=[op],
                        cpu=cpu,
                        ram=ram,
                        priority=pr,
                        pool_id=pool_id,
                        pipeline_id=chosen_pipeline.pipeline_id,
                    )
                )

                # Update local remaining capacity (scheduler must do this; executor won't reflect until next tick)
                rem_cpu -= float(cpu)
                rem_ram -= float(ram)

                assigned_ops_this_tick.add(k)
                pid = chosen_pipeline.pipeline_id
                assigned_count_by_pid[pid] = int(assigned_count_by_pid.get(pid, 0)) + 1

                # Requeue pipeline for future ops
                q.append(chosen_pipeline)

                made += 1
                scheduled_one = True
                break  # restart from highest priority

            if not scheduled_one:
                break

            if rem_cpu < s.min_cpu or rem_ram < s.min_ram:
                break

    return suspensions, assignments