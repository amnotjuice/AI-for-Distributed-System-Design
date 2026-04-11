# policy_key: scheduler_high_020
# reasoning_effort: high
# model: gpt-5.2-2025-12-11
# llm_cost: 0.103921
# generation_seconds: 174.56
# generated_at: 2026-03-12T23:10:42.271980
@register_scheduler_init(key="scheduler_high_020")
def scheduler_high_020_init(s):
    """Priority-aware, latency-oriented scheduler.

    Incremental improvements over naive FIFO:
      1) Separate per-priority queues (QUERY > INTERACTIVE > BATCH).
      2) Avoid monopolizing a whole pool with a single operator by using
         priority-specific per-op CPU/RAM caps (improves concurrency and tail latency).
      3) Soft-reserve capacity for high-priority work when it is waiting.
      4) Basic OOM-aware RAM backoff using recent ExecutionResult failures (per-operator key).

    Notes:
      - No preemption (keeps policy simple and avoids relying on unexposed runtime APIs).
      - Retries FAILED operators only when the failure looks like OOM and within limits.
    """
    # Queues by priority
    s.q_query = []
    s.q_interactive = []
    s.q_batch = []

    # "Time" for simple aging / debugging
    s._tick = 0
    s._enqueued_at = {}  # pipeline_id -> tick enqueued (first seen)

    # Per-operator adaptive state keyed by a stable op key
    s._op_oom_retries = {}       # op_key -> int
    s._op_nonoom_failures = {}   # op_key -> int
    s._op_last_req_ram = {}      # op_key -> last requested ram (best-effort)
    s._op_last_req_cpu = {}      # op_key -> last requested cpu (best-effort)

    # Policy knobs (kept conservative)
    s.max_assignments_per_pool = 16

    # Soft reservation fractions (only enforced when high-priority work is waiting)
    s.reserve_cpu_frac = 0.30
    s.reserve_ram_frac = 0.30

    # Per-op sizing caps by priority (fractions of pool max)
    s.query_cpu_frac = 0.50
    s.query_ram_frac = 0.45

    s.interactive_cpu_frac = 0.35
    s.interactive_ram_frac = 0.40

    s.batch_cpu_frac = 0.25
    s.batch_ram_frac = 0.30

    # Retry limits
    s.max_oom_retries = 4
    s.max_nonoom_retries = 1


@register_scheduler(key="scheduler_high_020")
def scheduler_high_020(s, results, pipelines):
    """See init docstring for the policy overview."""
    # -------- Helpers (defined inside to keep this file self-contained) --------
    def _op_key(op):
        """Best-effort stable key for an operator across scheduling/results."""
        for attr in ("op_id", "operator_id", "id", "uid"):
            if hasattr(op, attr):
                try:
                    v = getattr(op, attr)
                    # v might be a method in some implementations
                    if callable(v):
                        v = v()
                    return (attr, v)
                except Exception:
                    pass
        # Fallback: object identity (works if simulator returns the same op objects)
        return ("pyid", id(op))

    def _is_oom_error(err):
        if not err:
            return False
        try:
            e = str(err).upper()
        except Exception:
            return False
        return ("OOM" in e) or ("OUT OF MEMORY" in e) or ("OUT_OF_MEMORY" in e) or ("MEMORY" in e and "KILL" in e)

    def _clamp(x, lo, hi):
        if x < lo:
            return lo
        if x > hi:
            return hi
        return x

    def _queue_for_priority(prio):
        if prio == Priority.QUERY:
            return s.q_query
        if prio == Priority.INTERACTIVE:
            return s.q_interactive
        return s.q_batch  # Priority.BATCH_PIPELINE and any unknowns

    def _base_fracs_for_priority(prio):
        if prio == Priority.QUERY:
            return s.query_cpu_frac, s.query_ram_frac
        if prio == Priority.INTERACTIVE:
            return s.interactive_cpu_frac, s.interactive_ram_frac
        return s.batch_cpu_frac, s.batch_ram_frac

    def _pipeline_drop_or_keep(p):
        """Return (keep: bool). Drop completed pipelines and clearly non-retryable failures."""
        status = p.runtime_status()
        if status.is_pipeline_successful():
            return False

        # If there are failures, only keep if we still want to retry them.
        failed_ops = status.get_ops([OperatorState.FAILED], require_parents_complete=False)
        if not failed_ops:
            return True

        for op in failed_ops:
            k = _op_key(op)
            # If we have non-OOM failures beyond limit, drop.
            if s._op_nonoom_failures.get(k, 0) > s.max_nonoom_retries:
                return False
            # If we have OOM retries beyond limit, drop.
            if s._op_oom_retries.get(k, 0) > s.max_oom_retries:
                return False

        # If we haven't observed the failure reason yet, be conservative and allow one retry.
        return True

    def _next_ready_op(p):
        """Return a single ready operator (or None)."""
        status = p.runtime_status()
        ops = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        if not ops:
            return None
        return ops[0]

    def _is_failed_ready_op(p, op):
        """Check if op is in FAILED state and retryable context matters."""
        status = p.runtime_status()
        failed_ready = status.get_ops([OperatorState.FAILED], require_parents_complete=True)
        # Avoid hashing op directly; compare via op_key.
        ok = _op_key(op)
        for fo in failed_ready:
            if _op_key(fo) == ok:
                return True
        return False

    def _desired_resources(pool, prio, op):
        """Return (cpu, ram) desired for this op under this priority and pool, after backoff."""
        cpu_frac, ram_frac = _base_fracs_for_priority(prio)

        # Base request caps (do not grab the whole pool by default).
        base_cpu = pool.max_cpu_pool * cpu_frac
        base_ram = pool.max_ram_pool * ram_frac

        # Enforce small floors so we don't request 0.
        base_cpu = max(1.0, base_cpu)
        base_ram = max(1.0, base_ram)

        k = _op_key(op)
        oom_r = s._op_oom_retries.get(k, 0)

        # Exponential RAM backoff on OOM (fast convergence to RAM minimum).
        ram = base_ram * (2 ** oom_r)

        # CPU: keep stable; slightly bump for high-priority retried ops to reduce repeated queueing.
        if prio in (Priority.QUERY, Priority.INTERACTIVE) and oom_r > 0:
            cpu = base_cpu * (1.0 + 0.25 * oom_r)
        else:
            cpu = base_cpu

        # Clamp to pool maxima.
        cpu = _clamp(cpu, 1.0, pool.max_cpu_pool)
        ram = _clamp(ram, 1.0, pool.max_ram_pool)

        return cpu, ram

    def _rotate_find_and_maybe_assign(queue, prio, pool_id, avail_cpu, avail_ram):
        """Try to find a pipeline in 'queue' that has a ready op and fits. Returns Assignment or None."""
        if not queue:
            return None

        pool = s.executor.pools[pool_id]
        n = len(queue)

        for _ in range(n):
            p = queue.pop(0)

            # Drop completed / non-retryable pipelines
            if not _pipeline_drop_or_keep(p):
                continue

            op = _next_ready_op(p)
            if op is None:
                # Not ready; keep in the same priority queue.
                queue.append(p)
                continue

            # Enforce retry limits for FAILED ops.
            if _is_failed_ready_op(p, op):
                k = _op_key(op)
                if s._op_oom_retries.get(k, 0) > s.max_oom_retries:
                    # Drop pipeline (do not requeue)
                    continue
                if s._op_nonoom_failures.get(k, 0) > s.max_nonoom_retries:
                    continue

            # Compute desired size and check fit.
            want_cpu, want_ram = _desired_resources(pool, prio, op)

            # Allow CPU to shrink to fit (it only affects runtime, not correctness).
            req_cpu = min(want_cpu, avail_cpu)

            # RAM must fit our chosen backoff level; otherwise, skip and try other work.
            req_ram = want_ram
            if req_cpu < 1.0 or req_ram > avail_ram:
                # Can't place on this pool right now; rotate and keep pipeline queued.
                queue.append(p)
                continue

            # Create assignment for a single op (atomic scheduling unit).
            a = Assignment(
                ops=[op],
                cpu=req_cpu,
                ram=req_ram,
                priority=p.priority,
                pool_id=pool_id,
                pipeline_id=p.pipeline_id,
            )

            # Keep pipeline in queue for future ops.
            queue.append(p)

            # Record last requested resources (useful for debugging/backoff logic).
            k = _op_key(op)
            s._op_last_req_cpu[k] = req_cpu
            s._op_last_req_ram[k] = req_ram

            return a

        return None

    def _high_priority_waiting():
        """True if there exists any ready high-priority operator waiting in queues."""
        for q in (s.q_query, s.q_interactive):
            # Check a bounded number to avoid scanning huge queues every tick.
            # Rotation-safe because we only peek.
            limit = min(8, len(q))
            for i in range(limit):
                p = q[i]
                if not _pipeline_drop_or_keep(p):
                    continue
                if _next_ready_op(p) is not None:
                    return True
        return False

    # -------- Update state from inputs --------
    s._tick += 1

    # Enqueue new pipelines by priority
    for p in pipelines:
        q = _queue_for_priority(p.priority)
        q.append(p)
        if p.pipeline_id not in s._enqueued_at:
            s._enqueued_at[p.pipeline_id] = s._tick

    # Update per-op retry state from recent results
    for r in results:
        if not r.failed():
            continue
        oom = _is_oom_error(getattr(r, "error", None))
        for op in getattr(r, "ops", []) or []:
            k = _op_key(op)
            if oom:
                s._op_oom_retries[k] = s._op_oom_retries.get(k, 0) + 1
            else:
                s._op_nonoom_failures[k] = s._op_nonoom_failures.get(k, 0) + 1

    # Early exit if nothing happened (no new pipelines, no results), but keep working if queues non-empty.
    if not pipelines and not results and not (s.q_query or s.q_interactive or s.q_batch):
        return [], []

    # -------- Scheduling --------
    suspensions = []  # no preemption in this version
    assignments = []

    # Track planned usage per pool so we can pack multiple assignments per tick.
    used_cpu = {i: 0.0 for i in range(s.executor.num_pools)}
    used_ram = {i: 0.0 for i in range(s.executor.num_pools)}

    def _avail(pool_id):
        pool = s.executor.pools[pool_id]
        return (
            max(0.0, pool.avail_cpu_pool - used_cpu[pool_id]),
            max(0.0, pool.avail_ram_pool - used_ram[pool_id]),
        )

    # Order pools by headroom (helps fit large interactive ops earlier).
    pool_order = list(range(s.executor.num_pools))
    pool_order.sort(
        key=lambda i: (s.executor.pools[i].avail_cpu_pool, s.executor.pools[i].avail_ram_pool),
        reverse=True,
    )

    high_waiting = _high_priority_waiting()

    # Pass 1: schedule high-priority work aggressively.
    for pool_id in pool_order:
        pool = s.executor.pools[pool_id]
        for _ in range(s.max_assignments_per_pool):
            avail_cpu, avail_ram = _avail(pool_id)
            if avail_cpu <= 0.0 or avail_ram <= 0.0:
                break

            a = _rotate_find_and_maybe_assign(s.q_query, Priority.QUERY, pool_id, avail_cpu, avail_ram)
            if a is None:
                a = _rotate_find_and_maybe_assign(
                    s.q_interactive, Priority.INTERACTIVE, pool_id, avail_cpu, avail_ram
                )
            if a is None:
                break

            assignments.append(a)
            used_cpu[pool_id] += a.cpu
            used_ram[pool_id] += a.ram

    # Pass 2: schedule batch work, but keep a soft reservation if high-priority is waiting.
    for pool_id in pool_order:
        pool = s.executor.pools[pool_id]
        reserve_cpu = pool.max_cpu_pool * s.reserve_cpu_frac if high_waiting else 0.0
        reserve_ram = pool.max_ram_pool * s.reserve_ram_frac if high_waiting else 0.0

        for _ in range(s.max_assignments_per_pool):
            avail_cpu, avail_ram = _avail(pool_id)
            if avail_cpu <= 0.0 or avail_ram <= 0.0:
                break

            # Enforce reservation by reducing what batch is allowed to consume.
            # (If the pool is mostly idle, this may leave some capacity unused, trading throughput for latency.)
            eff_cpu = max(0.0, avail_cpu - reserve_cpu)
            eff_ram = max(0.0, avail_ram - reserve_ram)
            if eff_cpu <= 0.0 or eff_ram <= 0.0:
                break

            a = _rotate_find_and_maybe_assign(s.q_batch, Priority.BATCH_PIPELINE, pool_id, eff_cpu, eff_ram)
            if a is None:
                break

            assignments.append(a)
            used_cpu[pool_id] += a.cpu
            used_ram[pool_id] += a.ram

    return suspensions, assignments
