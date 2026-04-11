# policy_key: scheduler_high_036
# reasoning_effort: high
# model: gpt-5.2-2025-12-11
# llm_cost: 0.098307
# generation_seconds: 111.41
# generated_at: 2026-03-14T04:04:45.374165
@register_scheduler_init(key="scheduler_high_036")
def scheduler_high_036_init(s):
    """Priority-aware, right-sized scheduling with simple OOM backoff.

    Improvements over naive FIFO:
      1) Priority queues: serve QUERY/INTERACTIVE ahead of BATCH.
      2) Right-sizing: avoid giving a single op the entire pool; run more ops concurrently.
      3) OOM handling: if an op OOMs, retry it with increased RAM (per-op hint).
      4) Simple batch protection: allow very old batch work to make progress (aging threshold).
    """
    # Per-priority FIFO queues of Pipeline objects
    s.q_query = []
    s.q_interactive = []
    s.q_batch = []

    # Tick counter for aging/fairness decisions
    s.now = 0

    # Track pipeline arrival tick (for batch aging)
    s.arrival_tick = {}  # pipeline_id -> tick

    # Track which pipelines are safe to retry (OOM) vs. fatal failure
    s.pipeline_oom_retryable = set()  # pipeline_id
    s.pipeline_fatal = set()          # pipeline_id

    # Per-operator resource hints (learned from OOMs)
    # Keyed by a best-effort operator key (see _op_key)
    s.op_ram_hint = {}  # op_key -> ram to request next time
    s.op_cpu_hint = {}  # op_key -> cpu (kept simple; rarely updated)

    # Tunables (kept conservative / simple)
    s.batch_aging_threshold_ticks = 50
    s.oom_ram_backoff = 1.6
    s.min_cpu_unit = 1.0


def _op_key(op):
    """Best-effort stable-ish key for an operator object across scheduler ticks."""
    for attr in ("op_id", "operator_id", "id", "name", "task_id"):
        if hasattr(op, attr):
            val = getattr(op, attr)
            if val is not None:
                return (attr, val)
    # Fallback: repr is usually stable enough within a simulation run
    return ("repr", repr(op))


def _is_oom_error(err) -> bool:
    if err is None:
        return False
    msg = str(err).lower()
    return ("oom" in msg) or ("out of memory" in msg) or ("memoryerror" in msg)


def _enqueue_pipeline(s, p):
    """Enqueue pipeline into the appropriate priority queue (once)."""
    pid = p.pipeline_id
    if pid not in s.arrival_tick:
        s.arrival_tick[pid] = s.now

    if p.priority == Priority.QUERY:
        s.q_query.append(p)
    elif p.priority == Priority.INTERACTIVE:
        s.q_interactive.append(p)
    else:
        s.q_batch.append(p)


def _pipeline_should_drop(s, pipeline) -> bool:
    """Drop successful pipelines or pipelines marked fatal."""
    pid = pipeline.pipeline_id
    if pid in s.pipeline_fatal:
        return True
    status = pipeline.runtime_status()
    if status.is_pipeline_successful():
        return True
    return False


def _has_failed_ops(status) -> bool:
    # Guard: state_counts exists in the example; keep defensive.
    try:
        return status.state_counts[OperatorState.FAILED] > 0
    except Exception:
        return False


def _pick_next_schedulable(s, q, allow_failed_retry: bool):
    """Pop/rotate within a single queue until a schedulable op is found."""
    n = len(q)
    for _ in range(n):
        p = q.pop(0)

        # Fast drop of known-complete/fatal pipelines
        if _pipeline_should_drop(s, p):
            continue

        status = p.runtime_status()

        # If it has failed ops and we don't know it's OOM-retryable, treat as fatal
        # (prevents infinite retry loops on logic/IO errors).
        pid = p.pipeline_id
        if _has_failed_ops(status) and (pid not in s.pipeline_oom_retryable) and (not allow_failed_retry):
            s.pipeline_fatal.add(pid)
            continue

        # Find a single ready operator (require parents complete)
        op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
        if op_list:
            return p, op_list

        # Not ready yet: rotate to back
        q.append(p)

    return None, None


def _compute_request(s, pool, pipeline_priority, op_list, avail_cpu, avail_ram, high_waiting: bool):
    """Compute a right-sized cpu/ram request, using OOM-learned hints when available."""
    op = op_list[0]
    k = _op_key(op)

    # Base CPU policy (small improvements first; keep it simple)
    if pipeline_priority == Priority.QUERY:
        base_cpu = min(4.0, avail_cpu)
        base_ram = min(avail_ram, pool.max_ram_pool * 0.50)
    elif pipeline_priority == Priority.INTERACTIVE:
        base_cpu = min(2.0, avail_cpu)
        base_ram = min(avail_ram, pool.max_ram_pool * 0.40)
    else:
        base_cpu = min(1.0, avail_cpu)
        base_ram = min(avail_ram, pool.max_ram_pool * 0.25)

    # If high-priority work is waiting, keep some headroom by not over-allocating batch.
    # (No preemption API is assumed; headroom reduces future blocking.)
    if high_waiting and pipeline_priority == Priority.BATCH_PIPELINE:
        reserve_cpu = pool.max_cpu_pool * 0.20
        reserve_ram = pool.max_ram_pool * 0.20
        base_cpu = min(base_cpu, max(0.0, avail_cpu - reserve_cpu))
        base_ram = min(base_ram, max(0.0, avail_ram - reserve_ram))

    # Apply hints (OOM backoff)
    cpu = s.op_cpu_hint.get(k, base_cpu)
    ram = s.op_ram_hint.get(k, base_ram)

    # Ensure minimum sensible allocations
    cpu = max(s.min_cpu_unit, min(cpu, avail_cpu))
    ram = max(0.0, min(ram, avail_ram))

    return cpu, ram


@register_scheduler(key="scheduler_high_036")
def scheduler_high_036_scheduler(s, results, pipelines):
    """
    Priority queues + right-sizing + OOM-aware retries.

    Scheduling behavior:
      - Always try to schedule QUERY, then INTERACTIVE, then BATCH.
      - Run multiple operators per pool per tick (until CPU/RAM exhausted).
      - On OOM: retry the same operator with increased RAM next time.
      - On non-OOM failure: mark pipeline as fatal (stop retrying).
      - Batch aging: if a batch pipeline waits too long, allow it to be retried even if it has FAILED ops.
    """
    s.now += 1

    # Ingest new pipelines
    for p in pipelines:
        _enqueue_pipeline(s, p)

    # Learn from results (especially OOM)
    for r in results:
        if not getattr(r, "failed", lambda: False)():
            continue

        pid = getattr(r, "pipeline_id", None)
        oom = _is_oom_error(getattr(r, "error", None))

        # Update per-op RAM hint on OOM
        if oom:
            if pid is not None:
                s.pipeline_oom_retryable.add(pid)
            ops = getattr(r, "ops", []) or []
            for op in ops:
                k = _op_key(op)
                prev = s.op_ram_hint.get(k, getattr(r, "ram", 0.0) or 0.0)
                last = getattr(r, "ram", prev) or prev
                bumped = max(prev, last) * s.oom_ram_backoff
                # Cap at pool max if we can infer it later; for now, just store bumped.
                s.op_ram_hint[k] = bumped
        else:
            if pid is not None:
                s.pipeline_fatal.add(pid)

    # Early exit if nothing changed
    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    high_waiting = (len(s.q_query) > 0) or (len(s.q_interactive) > 0)

    # Schedule across pools; fill each pool with multiple assignments if possible
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)

        if avail_cpu < s.min_cpu_unit or avail_ram <= 0:
            continue

        # While we can still fit at least a minimal container, keep scheduling.
        while avail_cpu >= s.min_cpu_unit and avail_ram > 0:
            # 1) Prefer QUERY, then INTERACTIVE
            p, op_list = _pick_next_schedulable(s, s.q_query, allow_failed_retry=True)
            chosen_prio = Priority.QUERY if op_list else None

            if not op_list:
                p, op_list = _pick_next_schedulable(s, s.q_interactive, allow_failed_retry=True)
                chosen_prio = Priority.INTERACTIVE if op_list else None

            # 2) Batch: allow if no high-priority schedulable OR if batch is very old (anti-starvation)
            if not op_list:
                allow_failed_retry = False
                if s.q_batch:
                    # If oldest batch is very old, allow failed-op retry even without known OOM flag.
                    oldest = s.q_batch[0]
                    waited = s.now - s.arrival_tick.get(oldest.pipeline_id, s.now)
                    allow_failed_retry = waited >= s.batch_aging_threshold_ticks

                p, op_list = _pick_next_schedulable(s, s.q_batch, allow_failed_retry=allow_failed_retry)
                chosen_prio = Priority.BATCH_PIPELINE if op_list else None

            if not op_list:
                break  # nothing schedulable for this pool right now

            # Compute request and ensure it fits
            req_cpu, req_ram = _compute_request(
                s, pool, chosen_prio, op_list, avail_cpu, avail_ram, high_waiting=high_waiting
            )

            # If request doesn't fit meaningfully, stop trying to pack this pool
            if req_cpu < s.min_cpu_unit or req_ram <= 0:
                # Put pipeline back to its queue to avoid losing it
                if chosen_prio == Priority.QUERY:
                    s.q_query.append(p)
                elif chosen_prio == Priority.INTERACTIVE:
                    s.q_interactive.append(p)
                else:
                    s.q_batch.append(p)
                break

            assignment = Assignment(
                ops=op_list,
                cpu=req_cpu,
                ram=req_ram,
                priority=p.priority,
                pool_id=pool_id,
                pipeline_id=p.pipeline_id,
            )
            assignments.append(assignment)

            # Re-queue pipeline for its next operator(s)
            if chosen_prio == Priority.QUERY:
                s.q_query.append(p)
            elif chosen_prio == Priority.INTERACTIVE:
                s.q_interactive.append(p)
            else:
                s.q_batch.append(p)

            # Update remaining pool capacity for additional packing this tick
            avail_cpu -= req_cpu
            avail_ram -= req_ram

    return suspensions, assignments
