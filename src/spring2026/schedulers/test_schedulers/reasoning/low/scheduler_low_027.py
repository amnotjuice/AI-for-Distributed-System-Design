# policy_key: scheduler_low_027
# reasoning_effort: low
# model: gpt-5.2-2025-12-11
# llm_cost: 0.054037
# generation_seconds: 69.23
# generated_at: 2026-04-09T21:24:26.582746
@register_scheduler_init(key="scheduler_low_027")
def scheduler_low_027_init(s):
    """Priority-aware, OOM-adaptive, starvation-safe scheduler.

    Goals aligned to the weighted-latency objective:
      - Strongly protect QUERY and INTERACTIVE latency via strict priority + reserved headroom.
      - Reduce failures (720s penalties) by learning per-operator RAM hints from failures and retrying with larger RAM.
      - Avoid starvation by adding simple aging so long-waiting lower-priority pipelines eventually run.
      - Keep it simple/robust: no preemption (often requires visibility into running containers not exposed here).

    High-level behavior:
      1) Maintain per-priority queues of pipelines (QUERY > INTERACTIVE > BATCH).
      2) For each pool, admit work while resources exist:
           - Prefer higher priority, but add aging to prevent starvation.
           - Do not let BATCH consume reserved headroom intended for high-priority work.
      3) Size resources per operator:
           - CPU: moderate per-op caps to preserve concurrency; higher caps for higher priority.
           - RAM: use learned per-operator hint; otherwise allocate a conservative per-priority fraction of pool RAM.
      4) On failures: treat as likely under-provisioning; increase RAM hint and retry up to a cap.
    """
    # Logical time for aging (ticks; deterministic and monotonic)
    s._t = 0

    # Pipeline registry and queues
    s._pipelines = {}  # pipeline_id -> Pipeline
    s._meta = {}       # pipeline_id -> dict(enqueue_t, retries, quarantined)
    s._q_query = []
    s._q_interactive = []
    s._q_batch = []

    # Learned RAM hint by "operator key" (best-effort stable identifier)
    s._op_ram_hint = {}  # op_key -> ram_amount

    # Retry controls: after too many failures, quarantine to avoid wasting cluster time
    s._max_retries = 4


def _prio_rank(priority):
    # Higher is more important
    if priority == Priority.QUERY:
        return 3
    if priority == Priority.INTERACTIVE:
        return 2
    return 1  # Priority.BATCH_PIPELINE and any other


def _queue_for_prio(s, priority):
    if priority == Priority.QUERY:
        return s._q_query
    if priority == Priority.INTERACTIVE:
        return s._q_interactive
    return s._q_batch


def _op_key(op):
    # Best-effort stable key across runs/pipelines to generalize RAM hints.
    # Falls back to repr(op) if no better attribute exists.
    k = getattr(op, "op_id", None)
    if k is not None:
        return ("op_id", k)
    k = getattr(op, "name", None)
    if k is not None:
        return ("name", k)
    k = getattr(op, "fn_name", None)
    if k is not None:
        return ("fn_name", k)
    return ("repr", repr(op))


def _reserve_headroom(pool, priority):
    """Return (avail_cpu_effective, avail_ram_effective) after headroom reservation.

    We reserve resources so batch doesn't crowd out query/interactive and cause long waits.
    Query/interactive can use full available resources.
    """
    avail_cpu = pool.avail_cpu_pool
    avail_ram = pool.avail_ram_pool
    if avail_cpu <= 0 or avail_ram <= 0:
        return 0, 0

    if priority in (Priority.QUERY, Priority.INTERACTIVE):
        return avail_cpu, avail_ram

    # For batch: keep headroom for future high-priority arrivals.
    # Conservative to reduce p95/p99 for query/interactive.
    reserved_cpu = max(1.0, 0.25 * pool.max_cpu_pool)
    reserved_ram = max(1.0, 0.25 * pool.max_ram_pool)

    eff_cpu = max(0.0, avail_cpu - reserved_cpu)
    eff_ram = max(0.0, avail_ram - reserved_ram)
    return eff_cpu, eff_ram


def _pick_next_pipeline(s, pool, pool_id):
    """Pick next pipeline to schedule from queues using priority + aging.

    Returns a Pipeline or None.
    """
    # Consider a bounded number from each queue to keep runtime small and deterministic.
    # We compute an urgency score = base_priority + aging_boost.
    def score(pid):
        p = s._pipelines.get(pid)
        if p is None:
            return -1e18
        m = s._meta.get(pid, {})
        if m.get("quarantined", False):
            return -1e18
        base = float(_prio_rank(p.priority)) * 1000.0
        age = max(0, s._t - m.get("enqueue_t", s._t))
        # Aging: slow ramp, but enough to prevent indefinite starvation.
        # Batch will eventually get a boost if it waits long enough.
        aging_boost = min(800.0, age * 2.0)
        return base + aging_boost

    # Build candidates from the head of each queue (up to K)
    K = 12
    candidates = []
    for q in (s._q_query, s._q_interactive, s._q_batch):
        for pid in q[:K]:
            candidates.append(pid)

    if not candidates:
        return None

    # Prefer the best score, but also ensure the chosen pipeline has a ready operator.
    # We may need to try a few in descending score order.
    # Deterministic sorting: (-score, pid)
    candidates = sorted(set(candidates), key=lambda pid: (-score(pid), pid))

    for pid in candidates[:24]:
        p = s._pipelines.get(pid)
        if p is None:
            continue
        m = s._meta.get(pid, {})
        if m.get("quarantined", False):
            continue

        status = p.runtime_status()
        if status.is_pipeline_successful():
            continue

        # Only schedule ops whose parents are complete
        op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
        if op_list:
            return p

    return None


def _size_resources(s, pool, priority, op):
    """Choose (cpu, ram) for this operator given pool availability and learned hints."""
    # CPU: prioritize query/interactive latency but avoid monopolizing the pool.
    avail_cpu = pool.avail_cpu_pool
    avail_ram = pool.avail_ram_pool
    if avail_cpu <= 0 or avail_ram <= 0:
        return 0.0, 0.0

    # Target CPU caps (favor concurrency; interactive dev should feel snappy; batch smaller)
    if priority == Priority.QUERY:
        cpu_cap = max(1.0, 0.50 * pool.max_cpu_pool)
    elif priority == Priority.INTERACTIVE:
        cpu_cap = max(1.0, 0.35 * pool.max_cpu_pool)
    else:
        cpu_cap = max(1.0, 0.20 * pool.max_cpu_pool)

    cpu = min(avail_cpu, cpu_cap)

    # RAM: use learned per-op hint, else allocate a conservative slice of pool RAM.
    key = _op_key(op)
    hint = s._op_ram_hint.get(key, None)
    if hint is not None:
        # Add small safety margin to reduce repeated failures.
        ram = min(avail_ram, max(1.0, hint * 1.10))
    else:
        if priority == Priority.QUERY:
            ram = min(avail_ram, max(1.0, 0.40 * pool.max_ram_pool))
        elif priority == Priority.INTERACTIVE:
            ram = min(avail_ram, max(1.0, 0.30 * pool.max_ram_pool))
        else:
            ram = min(avail_ram, max(1.0, 0.22 * pool.max_ram_pool))

    return cpu, ram


@register_scheduler(key="scheduler_low_027")
def scheduler_low_027(s, results, pipelines):
    """
    Scheduler step.

    Returns:
      suspensions: [] (no preemption used in this version)
      assignments: list of Assignment(ops=[op], cpu=..., ram=..., priority=..., pool_id=..., pipeline_id=...)
    """
    s._t += 1

    # Register new pipelines
    for p in pipelines:
        pid = p.pipeline_id
        s._pipelines[pid] = p
        if pid not in s._meta:
            s._meta[pid] = {"enqueue_t": s._t, "retries": 0, "quarantined": False}
        else:
            # Keep original enqueue time for aging, unless pipeline was previously removed.
            s._meta[pid].setdefault("enqueue_t", s._t)
            s._meta[pid].setdefault("retries", 0)
            s._meta[pid].setdefault("quarantined", False)

        _queue_for_prio(s, p.priority).append(pid)

    # Process results: learn from failures, clean up completed pipelines, and (re)queue active ones.
    # Note: We don't have direct "pipeline completed" results, so we re-check pipeline status below.
    for r in results:
        # If failure, assume under-provisioned memory most of the time; bump learned RAM and retry counter.
        if hasattr(r, "failed") and r.failed():
            # Update per-op RAM hint based on the failing operator(s)
            try:
                ops = getattr(r, "ops", []) or []
                for op in ops:
                    k = _op_key(op)
                    prev = s._op_ram_hint.get(k, None)
                    # Increase aggressively to prevent repeated 720s penalties.
                    new_hint = max(1.0, float(getattr(r, "ram", 1.0)) * 1.60)
                    if prev is None:
                        s._op_ram_hint[k] = new_hint
                    else:
                        s._op_ram_hint[k] = max(prev, new_hint)
            except Exception:
                # Best-effort only; never crash the scheduler.
                pass

            # Increase retry count and potentially quarantine
            pid = getattr(r, "pipeline_id", None)
            # ExecutionResult doesn't advertise pipeline_id in the provided list; fall back to no-op.
            # We'll still rely on Pipeline runtime_status() to expose FAILED ops for retries.
            if pid is not None and pid in s._meta:
                s._meta[pid]["retries"] = s._meta[pid].get("retries", 0) + 1
                if s._meta[pid]["retries"] >= s._max_retries:
                    s._meta[pid]["quarantined"] = True

    # Early exit only if there's truly nothing to do
    if not pipelines and not results and not (s._q_query or s._q_interactive or s._q_batch):
        return [], []

    suspensions = []
    assignments = []

    # Garbage-collect finished pipelines from queues (lazy cleanup)
    def is_done_or_quarantined(pid):
        p = s._pipelines.get(pid)
        if p is None:
            return True
        m = s._meta.get(pid, {})
        if m.get("quarantined", False):
            return True
        try:
            st = p.runtime_status()
            # If pipeline successful, remove it from scheduling consideration
            if st.is_pipeline_successful():
                return True
            # If pipeline has any FAILED ops and we've exceeded retry cap, quarantine
            if m.get("retries", 0) >= s._max_retries and st.state_counts.get(OperatorState.FAILED, 0) > 0:
                s._meta[pid]["quarantined"] = True
                return True
        except Exception:
            # If status fails, keep it to avoid accidental dropping
            return False
        return False

    s._q_query = [pid for pid in s._q_query if not is_done_or_quarantined(pid)]
    s._q_interactive = [pid for pid in s._q_interactive if not is_done_or_quarantined(pid)]
    s._q_batch = [pid for pid in s._q_batch if not is_done_or_quarantined(pid)]

    # For each pool, greedily schedule multiple operators while resources allow.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]

        # Avoid infinite loops: bound scheduling attempts per pool per tick
        attempts = 0
        max_attempts = 64

        while attempts < max_attempts:
            attempts += 1

            # If no resources at all, stop
            if pool.avail_cpu_pool <= 0 or pool.avail_ram_pool <= 0:
                break

            # Choose a pipeline with a ready operator (priority + aging)
            p = _pick_next_pipeline(s, pool, pool_id)
            if p is None:
                break

            pid = p.pipeline_id
            m = s._meta.get(pid, {})
            if m.get("quarantined", False):
                # Remove from queue heads lazily; continue
                break

            # Determine the next assignable operator
            status = p.runtime_status()
            if status.is_pipeline_successful():
                break
            op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
            if not op_list:
                # Nothing ready; try again after rotating queues a bit
                # Move this pipeline to the end of its queue to reduce head-of-line blocking.
                q = _queue_for_prio(s, p.priority)
                if pid in q:
                    try:
                        q.remove(pid)
                    except Exception:
                        pass
                    q.append(pid)
                continue

            op = op_list[0]

            # Apply headroom reservation (batch only)
            eff_cpu, eff_ram = _reserve_headroom(pool, p.priority)
            if eff_cpu <= 0 or eff_ram <= 0:
                # If we picked batch and headroom blocks it, try to schedule a higher-priority pipeline instead.
                # Rotate batch queue to avoid repeatedly selecting blocked batch work.
                if p.priority == Priority.BATCH_PIPELINE:
                    q = s._q_batch
                    if pid in q:
                        try:
                            q.remove(pid)
                        except Exception:
                            pass
                        q.append(pid)
                    # Try again: maybe query/interactive exists and can use resources.
                    # If not, we'll break due to repeated selection. Limit attempts handles it.
                    continue
                break

            # Temporarily use effective avail for sizing, but assignment consumes real pool resources.
            # Size with caps and learned hints.
            cpu, ram = _size_resources(s, pool, p.priority, op)
            cpu = min(cpu, eff_cpu)
            ram = min(ram, eff_ram)

            # Minimal safety: require positive allocations
            if cpu <= 0 or ram <= 0:
                # Can't schedule anything meaningful in this pool right now.
                break

            assignments.append(
                Assignment(
                    ops=[op],
                    cpu=cpu,
                    ram=ram,
                    priority=p.priority,
                    pool_id=pool_id,
                    pipeline_id=pid,
                )
            )

            # Move scheduled pipeline to back of its priority queue to enable round-robin within same priority
            q = _queue_for_prio(s, p.priority)
            if pid in q:
                try:
                    q.remove(pid)
                except Exception:
                    pass
                q.append(pid)

            # Note: pool.avail_* will be updated by the simulator after applying assignments,
            # so we keep scheduling optimistically within this tick; the simulator should handle feasibility.

    return suspensions, assignments
