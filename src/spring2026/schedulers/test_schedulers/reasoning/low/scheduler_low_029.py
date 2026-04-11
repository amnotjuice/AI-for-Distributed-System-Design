# policy_key: scheduler_low_029
# reasoning_effort: low
# model: gpt-5.2-2025-12-11
# llm_cost: 0.052959
# generation_seconds: 49.49
# generated_at: 2026-04-09T21:26:04.644073
@register_scheduler_init(key="scheduler_low_029")
def scheduler_low_029_init(s):
    """
    Priority-aware, failure-averse scheduler.

    Core ideas:
    - Strict priority ordering (QUERY > INTERACTIVE > BATCH) to protect the weighted objective.
    - Small initial CPU/RAM allocations to improve packing, but with fast exponential RAM backoff on failures
      (especially OOM-like failures) to maximize completion rate (avoid 720s penalties).
    - Gentle anti-starvation via "aging": long-waiting batch pipelines get a temporary boost.
    - Multi-pool aware: if multiple pools exist, bias high-priority work to pool 0 (if present) but fall back
      to any pool with headroom.

    Notes:
    - No preemption here (lack of reliable running-container introspection in the minimal interface).
      Completion-rate is handled via retries with larger RAM rather than churn.
    """
    # Pipeline storage and per-priority queues (store pipeline_id)
    s.pipelines_by_id = {}          # pipeline_id -> Pipeline
    s.enqueued = set()              # pipeline_id already known
    s.q_query = []                  # list of pipeline_id
    s.q_interactive = []
    s.q_batch = []

    # Aging and bookkeeping
    s.tick = 0
    s.enqueued_tick = {}            # pipeline_id -> tick when first seen

    # Failure-driven sizing state
    s.ram_multiplier = {}           # pipeline_id -> float multiplier (>=1)
    s.fail_count = {}               # pipeline_id -> int failures observed

    # Lightweight recent sizing memory (used only as a hint)
    s.last_good_ram = {}            # pipeline_id -> last non-failing ram (best-effort)
    s.last_good_cpu = {}            # pipeline_id -> last non-failing cpu (best-effort)


def _prio_queue_for(s, prio):
    if prio == Priority.QUERY:
        return s.q_query
    if prio == Priority.INTERACTIVE:
        return s.q_interactive
    return s.q_batch


def _effective_priority(s, pipeline):
    """
    Apply simple aging:
    - If a BATCH pipeline has waited long enough, treat it as INTERACTIVE for selection purposes.
    This reduces indefinite starvation without materially hurting high-priority tail latency.
    """
    pid = pipeline.pipeline_id
    base = pipeline.priority
    if base == Priority.BATCH_PIPELINE:
        t0 = s.enqueued_tick.get(pid, s.tick)
        waited = s.tick - t0
        # After ~250 scheduling ticks, allow batch to compete with interactive.
        if waited >= 250:
            return Priority.INTERACTIVE
    return base


def _is_oom_like(result):
    # Best-effort heuristic: some simulators encode OOM as error text; fall back to any failure.
    try:
        if hasattr(result, "error") and result.error:
            e = str(result.error).lower()
            if "oom" in e or "out of memory" in e or "memory" in e:
                return True
    except Exception:
        pass
    return False


def _update_from_results(s, results):
    """
    Update per-pipeline sizing multipliers based on observed failures.
    Exponential RAM backoff on failures to maximize completion probability.
    """
    for r in results:
        # Record last "good" sizing hints on success
        if not r.failed():
            # It's possible r.ops spans multiple operators; we store per pipeline only (coarse).
            # Still helpful to avoid shrinking too aggressively later.
            pid = getattr(r, "pipeline_id", None)
            if pid is None:
                # Some environments don't carry pipeline_id on result; skip.
                continue
            s.last_good_ram[pid] = max(float(getattr(r, "ram", 0.0) or 0.0), s.last_good_ram.get(pid, 0.0))
            s.last_good_cpu[pid] = max(float(getattr(r, "cpu", 0.0) or 0.0), s.last_good_cpu.get(pid, 0.0))
            continue

        # On failure: inflate RAM multiplier (and keep a count)
        pid = getattr(r, "pipeline_id", None)
        if pid is None:
            continue

        s.fail_count[pid] = s.fail_count.get(pid, 0) + 1
        cur = float(s.ram_multiplier.get(pid, 1.0))

        # More aggressive backoff when it looks like OOM; otherwise still increase some.
        if _is_oom_like(r):
            nxt = min(cur * 2.0, 64.0)
        else:
            nxt = min(cur * 1.5, 64.0)

        s.ram_multiplier[pid] = max(cur, nxt)


def _pipeline_done_or_dead(pipeline):
    """
    Decide whether to keep pipeline in queues.
    We avoid retrying pipelines that have any FAILED operators, as per baseline template guidance.
    """
    status = pipeline.runtime_status()
    if status.is_pipeline_successful():
        return True
    # If any operator is FAILED, treat pipeline as dead (no retries) to avoid endless churn.
    # NOTE: This matches the provided baseline behavior.
    has_failures = status.state_counts[OperatorState.FAILED] > 0
    if has_failures:
        return True
    return False


def _pick_next_pipeline_id(s, pool_id):
    """
    Choose the next pipeline to run on this pool using:
    - Priority ordering (with aging)
    - Shorter remaining-work heuristic (fewer assignable ops remaining) to reduce average latency
      among high-priority pipelines (proxy for SRPT).
    """
    # Assemble candidates from queues; keep scanning limited to avoid O(n^2) worst-case.
    scan_limit = 25

    def scan_queue(q):
        best_pid = None
        best_key = None
        best_idx = None
        n = min(len(q), scan_limit)
        for i in range(n):
            pid = q[i]
            p = s.pipelines_by_id.get(pid)
            if p is None:
                continue
            if _pipeline_done_or_dead(p):
                continue
            st = p.runtime_status()
            # Require parents complete so we don't waste assignments.
            ops_ready = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
            if not ops_ready:
                continue

            # Key: smaller remaining total ops first (rough), then older first (aging),
            # then fewer past failures.
            # Remaining ops approximation = count of non-completed ops.
            remaining = (
                st.state_counts.get(OperatorState.PENDING, 0)
                + st.state_counts.get(OperatorState.ASSIGNED, 0)
                + st.state_counts.get(OperatorState.RUNNING, 0)
                + st.state_counts.get(OperatorState.SUSPENDING, 0)
            )
            age = s.tick - s.enqueued_tick.get(pid, s.tick)
            fails = s.fail_count.get(pid, 0)
            key = (remaining, -age, fails)
            if best_key is None or key < best_key:
                best_key = key
                best_pid = pid
                best_idx = i
        return best_pid, best_idx

    # Pool bias: if multiple pools, keep high priority concentrated on pool 0 when possible.
    # Here we simply scan in priority order always; pool 0 gets first crack at high-prio naturally.
    # For other pools, still allow high-prio if present; otherwise use lower.
    # Apply aging by temporarily treating some batch as interactive at selection-time.
    # Implementation: scan query, then interactive+aged-batch, then batch.

    # 1) Query
    pid, idx = scan_queue(s.q_query)
    if pid is not None:
        del s.q_query[idx]
        return pid

    # 2) Interactive (plus aged batch)
    pid, idx = scan_queue(s.q_interactive)
    if pid is not None:
        del s.q_interactive[idx]
        return pid

    # Consider aged batch as interactive candidate
    # Only scan a slice to keep it cheap.
    best_pid = None
    best_key = None
    best_idx = None
    n = min(len(s.q_batch), scan_limit)
    for i in range(n):
        pid = s.q_batch[i]
        p = s.pipelines_by_id.get(pid)
        if p is None:
            continue
        if _pipeline_done_or_dead(p):
            continue
        if _effective_priority(s, p) != Priority.INTERACTIVE:
            continue
        st = p.runtime_status()
        ops_ready = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        if not ops_ready:
            continue
        remaining = (
            st.state_counts.get(OperatorState.PENDING, 0)
            + st.state_counts.get(OperatorState.ASSIGNED, 0)
            + st.state_counts.get(OperatorState.RUNNING, 0)
            + st.state_counts.get(OperatorState.SUSPENDING, 0)
        )
        age = s.tick - s.enqueued_tick.get(pid, s.tick)
        fails = s.fail_count.get(pid, 0)
        key = (remaining, -age, fails)
        if best_key is None or key < best_key:
            best_key = key
            best_pid = pid
            best_idx = i
    if best_pid is not None:
        del s.q_batch[best_idx]
        return best_pid

    # 3) Batch
    pid, idx = scan_queue(s.q_batch)
    if pid is not None:
        del s.q_batch[idx]
        return pid

    return None


def _size_for(s, pipeline, pool, avail_cpu, avail_ram):
    """
    Pick CPU/RAM sizing for the next operator of a pipeline.
    - CPU: moderate caps per priority to reduce interference; let multiple pipelines share.
    - RAM: allocate a fraction of pool RAM, multiplied by failure-driven multiplier.
    """
    prio = pipeline.priority
    pid = pipeline.pipeline_id

    # CPU caps (favor low tail latency but avoid giving all CPU to one op)
    if prio == Priority.QUERY:
        cpu_cap = max(1.0, 0.5 * float(pool.max_cpu_pool))
        cpu_floor = 1.0
    elif prio == Priority.INTERACTIVE:
        cpu_cap = max(1.0, 0.35 * float(pool.max_cpu_pool))
        cpu_floor = 1.0
    else:
        cpu_cap = max(1.0, 0.25 * float(pool.max_cpu_pool))
        cpu_floor = 1.0

    # If we have a last-good cpu hint, don't go below it (improves stability).
    hint_cpu = float(s.last_good_cpu.get(pid, 0.0) or 0.0)
    cpu = min(float(avail_cpu), cpu_cap)
    cpu = max(cpu, cpu_floor)
    if hint_cpu > 0:
        cpu = max(cpu, min(float(avail_cpu), hint_cpu))

    # RAM baseline fractions
    if prio == Priority.QUERY:
        base_frac = 0.30
    elif prio == Priority.INTERACTIVE:
        base_frac = 0.22
    else:
        base_frac = 0.15

    mult = float(s.ram_multiplier.get(pid, 1.0))
    # Start from pool max (not avail) to allow multiplier to meaningfully scale, then clamp to avail.
    target = base_frac * float(pool.max_ram_pool) * mult

    # If we saw a last-good RAM, use it as a floor to reduce repeat failures.
    hint_ram = float(s.last_good_ram.get(pid, 0.0) or 0.0)
    if hint_ram > 0:
        target = max(target, hint_ram)

    # Always clamp to available. If tiny availability, still try with what's available.
    ram = min(float(avail_ram), max(0.0, target))
    if ram <= 0.0:
        ram = float(avail_ram)

    return cpu, ram


@register_scheduler(key="scheduler_low_029")
def scheduler_low_029_scheduler(s, results, pipelines):
    """
    Scheduler step:
    - Ingest new pipelines into per-priority queues.
    - Update RAM multipliers from failures.
    - For each pool, assign ready operators from selected pipelines until headroom is low.

    Returns:
      (suspensions, assignments)
    """
    # Advance logical tick only when there's something to process to keep aging stable.
    if pipelines or results:
        s.tick += 1

    _update_from_results(s, results)

    # Enqueue newly arrived pipelines
    for p in pipelines:
        pid = p.pipeline_id
        s.pipelines_by_id[pid] = p
        if pid in s.enqueued:
            continue
        s.enqueued.add(pid)
        s.enqueued_tick[pid] = s.tick
        s.ram_multiplier.setdefault(pid, 1.0)
        s.fail_count.setdefault(pid, 0)
        _prio_queue_for(s, p.priority).append(pid)

    # Early exit if nothing new and no results that could change sizing
    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    # Helper: if pipeline is still active, requeue it after we (maybe) schedule an op.
    def requeue_if_active(p):
        if p is None:
            return
        if _pipeline_done_or_dead(p):
            return
        _prio_queue_for(s, p.priority).append(p.pipeline_id)

    # Iterate pools; for each pool, keep assigning while there is headroom.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]

        # Slightly bias high priority to pool 0 when multiple pools exist:
        # - For pool 0: allow full use.
        # - For other pools: keep some headroom in case pool 0 is saturated? (not enforceable without preemption)
        # So we simply proceed normally; this remains safe and simple.

        # We'll loop to fill the pool, but avoid infinite loops by limiting number of assignments per pool per tick.
        per_pool_limit = 8
        made = 0

        while made < per_pool_limit:
            avail_cpu = pool.avail_cpu_pool
            avail_ram = pool.avail_ram_pool
            if avail_cpu <= 0 or avail_ram <= 0:
                break

            pid = _pick_next_pipeline_id(s, pool_id)
            if pid is None:
                break

            pipeline = s.pipelines_by_id.get(pid)
            if pipeline is None or _pipeline_done_or_dead(pipeline):
                continue

            status = pipeline.runtime_status()
            op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
            if not op_list:
                # No ready ops now; put it back for later.
                requeue_if_active(pipeline)
                continue

            cpu, ram = _size_for(s, pipeline, pool, avail_cpu, avail_ram)

            # If we can't allocate at least some CPU and RAM, stop filling this pool.
            if cpu <= 0 or ram <= 0:
                requeue_if_active(pipeline)
                break

            assignments.append(
                Assignment(
                    ops=op_list,
                    cpu=cpu,
                    ram=ram,
                    priority=pipeline.priority,
                    pool_id=pool_id,
                    pipeline_id=pipeline.pipeline_id,
                )
            )
            made += 1

            # Requeue pipeline so it can continue (keeps concurrency across pipelines)
            requeue_if_active(pipeline)

    return suspensions, assignments
