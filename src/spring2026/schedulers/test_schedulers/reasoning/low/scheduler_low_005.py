# policy_key: scheduler_low_005
# reasoning_effort: low
# model: gpt-5.2-2025-12-11
# llm_cost: 0.000000
# generation_seconds: 43.94
# generated_at: 2026-04-09T21:01:17.067274
@register_scheduler_init(key="scheduler_low_005")
def scheduler_low_005_init(s):
    """Priority-first, completion-oriented scheduler.

    Goals:
    - Strongly protect QUERY and INTERACTIVE latency (dominant score weights).
    - Avoid failures by retrying FAILED ops with progressively larger RAM (OOM-aware).
    - Avoid indefinite starvation of BATCH by injecting periodic batch service (simple aging).
    - Keep implementation simple and robust vs. missing/unknown operator metadata.
    """
    # Per-priority FIFO queues of pipelines awaiting scheduling consideration
    s.queues = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }

    # Remember which pipelines are already enqueued to avoid duplicates
    s.enqueued = set()

    # Per-(pipeline, op) resource learning for retries:
    # ram_mult increases after OOM-ish failures to reduce future failures (720s penalty avoidance).
    s.op_ram_mult = {}   # (pipeline_id, op_key) -> float
    s.op_retry_cnt = {}  # (pipeline_id, op_key) -> int

    # Fairness knobs: occasionally serve batch even if interactive traffic is ongoing
    s.high_served_since_batch = 0

    # Pipeline ordering stability
    s.pipeline_seq = 0
    s.pipeline_order = {}  # pipeline_id -> seq for consistent FIFO across requeues


def _op_key_local(op):
    """Best-effort stable key for an operator without relying on specific schema."""
    for attr in ("op_id", "operator_id", "id", "name", "key"):
        if hasattr(op, attr):
            try:
                v = getattr(op, attr)
                if v is not None:
                    return (attr, str(v))
            except Exception:
                pass
    # Fallback: object identity (only stable during simulation lifetime)
    return ("py_id", str(id(op)))


def _is_oom_error(err):
    if err is None:
        return False
    try:
        s = str(err).lower()
    except Exception:
        return False
    return ("oom" in s) or ("out of memory" in s) or ("memoryerror" in s) or ("killed" in s and "memory" in s)


def _base_fraction_for_priority(priority):
    # Conservative defaults to reduce OOM while leaving some headroom for concurrency.
    # More RAM beyond minimum doesn't speed up but avoids costly OOM retries/failures.
    if priority == Priority.QUERY:
        return 0.55
    if priority == Priority.INTERACTIVE:
        return 0.45
    return 0.35  # batch


def _cpu_fraction_for_priority(priority):
    # Queries get more CPU to reduce latency; batch less to preserve responsiveness.
    if priority == Priority.QUERY:
        return 0.90
    if priority == Priority.INTERACTIVE:
        return 0.70
    return 0.50


def _choose_next_priority(s):
    """Strict priority with simple aging: every few high-priority dispatches, allow a batch dispatch."""
    has_q = len(s.queues[Priority.QUERY]) > 0
    has_i = len(s.queues[Priority.INTERACTIVE]) > 0
    has_b = len(s.queues[Priority.BATCH_PIPELINE]) > 0

    if has_q:
        return Priority.QUERY

    if has_i:
        # Periodically inject batch to avoid starvation when interactive stream is steady.
        if has_b and s.high_served_since_batch >= 5:
            return Priority.BATCH_PIPELINE
        return Priority.INTERACTIVE

    if has_b:
        return Priority.BATCH_PIPELINE

    return None


def _queue_push(s, pipeline):
    pr = pipeline.priority
    if pipeline.pipeline_id not in s.pipeline_order:
        s.pipeline_seq += 1
        s.pipeline_order[pipeline.pipeline_id] = s.pipeline_seq
    if pipeline.pipeline_id in s.enqueued:
        return
    s.queues[pr].append(pipeline)
    s.enqueued.add(pipeline.pipeline_id)


def _queue_pop(s, priority):
    if not s.queues[priority]:
        return None
    p = s.queues[priority].pop(0)
    # Allow it to be re-enqueued later if still active
    s.enqueued.discard(p.pipeline_id)
    return p


@register_scheduler(key="scheduler_low_005")
def scheduler_low_005(s, results, pipelines):
    """
    Scheduler step:
    - Enqueue new pipelines by priority.
    - Update per-op RAM multiplier when failures occur (OOM-aware).
    - Dispatch ready operators with conservative RAM sizing and priority-biased CPU.
    - Retry FAILED operators with exponentially increased RAM (bounded).
    """
    # Enqueue new arrivals
    for p in pipelines:
        _queue_push(s, p)

    # Learn from results (especially OOM-like failures)
    for r in results:
        if not getattr(r, "failed", lambda: False)():
            continue
        # Associate result ops with retry learning; r.ops is list-like per examples
        try:
            rops = list(getattr(r, "ops", []) or [])
        except Exception:
            rops = []
        for op in rops:
            opk = _op_key_local(op)
            key = (getattr(r, "pipeline_id", None), opk)
            # If pipeline_id isn't present on result, fall back to op-key-only within a shared namespace
            if key[0] is None:
                key = ("_unknown_pipeline", opk)

            cnt = s.op_retry_cnt.get(key, 0) + 1
            s.op_retry_cnt[key] = cnt

            # Increase RAM multiplier faster on OOM signals; otherwise gentle increase.
            mult = s.op_ram_mult.get(key, 1.0)
            if _is_oom_error(getattr(r, "error", None)):
                # Exponential increase to avoid repeated OOMs (very costly via 720s penalties)
                mult = max(mult, 1.0) * 1.8
            else:
                mult = max(mult, 1.0) * 1.2

            # Cap multiplier to prevent absurd allocations; we'll also cap by pool max at assign-time.
            if mult > 8.0:
                mult = 8.0
            s.op_ram_mult[key] = mult

    # Early exit if no new work and no results to react to
    if not pipelines and not results:
        return [], []

    suspensions = []  # We avoid preemption because running container IDs/resources aren't exposed reliably.
    assignments = []

    # Helper to (re)queue pipeline if still active
    def requeue_if_active(p):
        st = p.runtime_status()
        has_failures = st.state_counts[OperatorState.FAILED] > 0
        if st.is_pipeline_successful():
            return
        # Do not drop on failures: retry logic relies on FAILED being assignable.
        # Only stop requeueing if it is truly irrecoverable; simulator usually marks failed ops as FAILED.
        _queue_push(s, p)

    # Schedule per pool to respect pool-local resource limits
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool

        # Local packing within the pool for this tick
        # Stop if we can't place even a minimal container
        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # Try to place multiple ops in the same pool in one tick (packing)
        # Use a modest cap to avoid spending too long in the loop.
        for _ in range(32):
            if avail_cpu <= 0 or avail_ram <= 0:
                break

            pr = _choose_next_priority(s)
            if pr is None:
                break

            p = _queue_pop(s, pr)
            if p is None:
                break

            status = p.runtime_status()
            if status.is_pipeline_successful():
                continue

            # Select one ready operator to keep behavior predictable and reduce failure amplification
            op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
            if not op_list:
                # Not ready right now; requeue to be checked later
                requeue_if_active(p)
                continue

            op = op_list[0]
            opk = _op_key_local(op)

            # Determine RAM target: conservative baseline * retry multiplier
            # Use per-pipeline key when possible for better learning locality
            key = (p.pipeline_id, opk)
            mult = s.op_ram_mult.get(key, 1.0)

            base_frac = _base_fraction_for_priority(p.priority)
            target_ram = pool.max_ram_pool * base_frac * mult

            # If repeated retries, bias more strongly toward high RAM to avoid 720s penalties
            retry_cnt = s.op_retry_cnt.get(key, 0)
            if retry_cnt >= 2:
                target_ram = max(target_ram, pool.max_ram_pool * min(0.85, base_frac + 0.25))

            # Clamp to what's available and pool limits
            if target_ram > pool.max_ram_pool:
                target_ram = pool.max_ram_pool
            if target_ram > avail_ram:
                # If we can't fit our target, try a smaller (but still non-trivial) allocation.
                # Avoid allocating extremely small RAM that is likely to OOM and waste time.
                min_reasonable = pool.max_ram_pool * (0.25 if p.priority != Priority.BATCH_PIPELINE else 0.20)
                target_ram = min(avail_ram, max(min_reasonable, avail_ram))

            # Determine CPU target
            cpu_frac = _cpu_fraction_for_priority(p.priority)
            target_cpu = pool.max_cpu_pool * cpu_frac
            if target_cpu > avail_cpu:
                target_cpu = avail_cpu
            # Ensure some CPU if any is available
            if target_cpu <= 0 and avail_cpu > 0:
                target_cpu = avail_cpu

            # If still can't allocate meaningful resources, requeue and stop trying this pool
            if target_cpu <= 0 or target_ram <= 0:
                requeue_if_active(p)
                break

            # Commit assignment
            assignments.append(
                Assignment(
                    ops=op_list,
                    cpu=target_cpu,
                    ram=target_ram,
                    priority=p.priority,
                    pool_id=pool_id,
                    pipeline_id=p.pipeline_id,
                )
            )

            # Update local available resources (best-effort packing)
            avail_cpu -= target_cpu
            avail_ram -= target_ram

            # Update fairness counter
            if p.priority in (Priority.QUERY, Priority.INTERACTIVE):
                s.high_served_since_batch += 1
            else:
                s.high_served_since_batch = 0

            # Requeue pipeline for subsequent operators (if any remain)
            requeue_if_active(p)

    return suspensions, assignments
