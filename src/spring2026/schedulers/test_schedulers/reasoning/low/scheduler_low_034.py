# policy_key: scheduler_low_034
# reasoning_effort: low
# model: gpt-5.2-2025-12-11
# llm_cost: 0.059833
# generation_seconds: 44.22
# generated_at: 2026-04-09T21:30:03.164138
@register_scheduler_init(key="scheduler_low_034")
def scheduler_low_034_init(s):
    """Priority-weighted, failure-averse, starvation-safe scheduler.

    Core ideas:
      1) Separate FIFO queues per priority (QUERY > INTERACTIVE > BATCH).
      2) Assign at most one ready operator per pipeline per tick to reduce HoL blocking.
      3) Conservative "batch budgets" when higher priorities are waiting (protect tail latency).
      4) OOM/failure-aware RAM backoff per (pipeline, operator) with bounded retries to
         maximize completion rate and avoid repeated 720s penalties.
      5) Gentle fairness: if only batch is runnable it will consume the pool; if higher
         priorities exist, batch is throttled but not starved.
    """
    s.wait_q = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }

    # Per-(pipeline,op) RAM hint that grows on failures (especially OOM-like failures).
    s.op_ram_hint = {}  # (pipeline_id, op_key) -> ram
    s.op_retries = {}   # (pipeline_id, op_key) -> int

    # Soft knobs (kept simple / robust).
    s.max_retries_per_op = 4

    # When high-priority is waiting, throttle batch to protect query/interactive latency.
    s.batch_cpu_budget_frac_when_hp_waiting = 0.25
    s.batch_ram_budget_frac_when_hp_waiting = 0.25

    # Per-op CPU fractions by priority (cap to available).
    s.cpu_frac = {
        Priority.QUERY: 0.75,
        Priority.INTERACTIVE: 0.60,
        Priority.BATCH_PIPELINE: 0.50,  # can still go big when no HP waiting (handled below)
    }

    # Default initial RAM guess as fraction of pool (small, but not tiny).
    s.default_ram_frac = {
        Priority.QUERY: 0.25,
        Priority.INTERACTIVE: 0.20,
        Priority.BATCH_PIPELINE: 0.20,
    }

    # Minimum allocation guards (avoid pathological tiny allocations).
    s.min_cpu = 1
    s.min_ram = 1


def _priority_weight(priority):
    if priority == Priority.QUERY:
        return 3
    if priority == Priority.INTERACTIVE:
        return 2
    return 1


def _op_key(op):
    # Best-effort stable key without importing / relying on unknown fields.
    for attr in ("op_id", "operator_id", "id", "name"):
        if hasattr(op, attr):
            return str(getattr(op, attr))
    return str(op)


def _is_oom_like(error_obj):
    if error_obj is None:
        return False
    msg = str(error_obj).lower()
    return ("oom" in msg) or ("out of memory" in msg) or ("killed" in msg) or ("memory" in msg)


def _clamp(x, lo, hi):
    if x < lo:
        return lo
    if x > hi:
        return hi
    return x


def _enqueue(s, pipeline):
    s.wait_q[pipeline.priority].append(pipeline)


def _dequeue_next_pipeline(s, hp_waiting):
    """
    Weighted priority selection with simple round-robin inside each priority FIFO:
      QUERY first, then INTERACTIVE, then BATCH.
    """
    if s.wait_q[Priority.QUERY]:
        return s.wait_q[Priority.QUERY].pop(0)
    if s.wait_q[Priority.INTERACTIVE]:
        return s.wait_q[Priority.INTERACTIVE].pop(0)
    if s.wait_q[Priority.BATCH_PIPELINE]:
        return s.wait_q[Priority.BATCH_PIPELINE].pop(0)
    return None


def _requeue(s, pipeline):
    # Keep FIFO within each priority class.
    s.wait_q[pipeline.priority].append(pipeline)


def _pick_one_ready_op(pipeline):
    status = pipeline.runtime_status()
    has_failures = status.state_counts[OperatorState.FAILED] > 0

    # If pipeline is done, or has failures but no retry mechanism, skip.
    if status.is_pipeline_successful():
        return None

    # Prefer ready ops requiring parents complete; take just one to avoid HoL and improve interactivity.
    ops = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
    if ops:
        return ops[0]

    # If nothing is runnable (e.g., all running/assigned), return None.
    # (We do NOT relax parent requirement; that would violate DAG correctness.)
    return None


def _ram_for_op(s, pool, pipeline, op):
    p = pipeline.priority
    pid = pipeline.pipeline_id
    key = (pid, _op_key(op))

    # If we have a learned hint, use it (bounded).
    if key in s.op_ram_hint:
        return _clamp(s.op_ram_hint[key], s.min_ram, pool.max_ram_pool)

    # Otherwise start with a modest default fraction of the pool.
    guess = int(pool.max_ram_pool * s.default_ram_frac.get(p, 0.20))
    return _clamp(guess, s.min_ram, pool.max_ram_pool)


def _cpu_for_op(s, pool, pipeline, hp_waiting):
    p = pipeline.priority

    # If batch and HP is waiting, throttle hard.
    if p == Priority.BATCH_PIPELINE and hp_waiting:
        cap = int(pool.max_cpu_pool * s.batch_cpu_budget_frac_when_hp_waiting)
        cap = _clamp(cap, s.min_cpu, pool.max_cpu_pool)
        return cap

    frac = s.cpu_frac.get(p, 0.5)
    cap = int(pool.max_cpu_pool * frac)
    cap = _clamp(cap, s.min_cpu, pool.max_cpu_pool)
    return cap


def _apply_failure_backoff(s, result):
    """
    Increase RAM hint on failures, aggressively for OOM-like signals.
    """
    if not result.failed():
        return
    if not result.ops:
        return

    pid = result.ops[0].pipeline_id if hasattr(result.ops[0], "pipeline_id") else None
    # We still can use assignment.pipeline_id via result metadata in most simulators; if absent, skip.
    # The starter template uses pipeline_id in Assignment, but ExecutionResult fields are limited;
    # so we fall back to parsing from ops if present.
    if pid is None:
        # Best-effort: can't key without pipeline id.
        return

    oom_like = _is_oom_like(result.error)

    for op in result.ops:
        key = (pid, _op_key(op))
        s.op_retries[key] = s.op_retries.get(key, 0) + 1

        # Base on what we attempted.
        prev = result.ram if getattr(result, "ram", None) is not None else None
        if prev is None:
            prev = s.op_ram_hint.get(key, s.min_ram)

        # Backoff policy:
        #   - If OOM-like: double.
        #   - Else: +50% (still helps completion but avoids over-allocating too quickly).
        if oom_like:
            new = int(prev * 2)
        else:
            new = int(prev * 1.5) + 1

        # Store hint; actual clamp happens at assignment time with pool.max_ram_pool.
        s.op_ram_hint[key] = max(new, s.min_ram)


@register_scheduler(key="scheduler_low_034")
def scheduler_low_034(s, results, pipelines):
    """
    Scheduler step:
      - Ingest arrivals into per-priority queues.
      - Learn from failures (RAM backoff).
      - For each pool, repeatedly assign single ready ops while resources remain:
          QUERY first, then INTERACTIVE, then BATCH.
        Batch is throttled when any HP is waiting anywhere.
      - Never drop pipelines; bounded per-op retries to avoid infinite churn.
    """
    # Ingest new pipelines.
    for p in pipelines:
        _enqueue(s, p)

    # Update RAM hints from failures (especially OOM) to improve completion rate.
    for r in results:
        _apply_failure_backoff(s, r)

    # Early exit if nothing changed.
    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    # Determine if any high-priority work is waiting (affects batch throttling).
    hp_waiting = bool(s.wait_q[Priority.QUERY] or s.wait_q[Priority.INTERACTIVE])

    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]

        # We'll compute availability fresh per assignment to avoid over-allocating.
        while True:
            avail_cpu = pool.avail_cpu_pool
            avail_ram = pool.avail_ram_pool
            if avail_cpu <= 0 or avail_ram <= 0:
                break

            pipeline = _dequeue_next_pipeline(s, hp_waiting)
            if pipeline is None:
                break

            status = pipeline.runtime_status()
            if status.is_pipeline_successful():
                # Completed pipelines should not be requeued.
                continue

            op = _pick_one_ready_op(pipeline)
            if op is None:
                # Not runnable now; requeue for later.
                _requeue(s, pipeline)
                # Prevent tight spinning on only blocked pipelines: if this was HP, try others.
                # Continue loop to search other pipelines.
                continue

            # Bound retries: if this op has failed too many times, we still keep pipeline alive,
            # but avoid immediate re-attempt churn by requeueing (lets other pipelines progress).
            key = (pipeline.pipeline_id, _op_key(op))
            if s.op_retries.get(key, 0) >= s.max_retries_per_op:
                _requeue(s, pipeline)
                # Try another pipeline this tick.
                continue

            # Decide resource sizes.
            cpu_cap = _cpu_for_op(s, pool, pipeline, hp_waiting)
            ram_need = _ram_for_op(s, pool, pipeline, op)

            # Fit to availability.
            cpu = _clamp(min(avail_cpu, cpu_cap), s.min_cpu, avail_cpu)
            ram = _clamp(min(avail_ram, ram_need), s.min_ram, avail_ram)

            # If we can't allocate at least minimal resources, requeue and stop on this pool.
            if cpu <= 0 or ram <= 0:
                _requeue(s, pipeline)
                break

            # Assign exactly one operator.
            assignment = Assignment(
                ops=[op],
                cpu=cpu,
                ram=ram,
                priority=pipeline.priority,
                pool_id=pool_id,
                pipeline_id=pipeline.pipeline_id,
            )
            assignments.append(assignment)

            # Requeue pipeline if it likely has more work; keeps fairness and supports DAG progression.
            _requeue(s, pipeline)

            # Refresh global HP waiting signal after potentially draining HP queues.
            hp_waiting = bool(s.wait_q[Priority.QUERY] or s.wait_q[Priority.INTERACTIVE])

    return suspensions, assignments
