# policy_key: scheduler_low_015
# reasoning_effort: low
# model: gpt-5.2-2025-12-11
# llm_cost: 0.049517
# generation_seconds: 33.89
# generated_at: 2026-03-12T21:45:48.734897
@register_scheduler_init(key="scheduler_low_015")
def scheduler_low_015_init(s):
    """Priority-aware FIFO with minimal adaptive sizing.

    Improvements over naive FIFO:
      1) Maintain separate FIFO queues per priority (QUERY > INTERACTIVE > BATCH).
      2) Reserve a small fraction of each pool for high-priority work so batch doesn't
         fully consume capacity and spike tail latency.
      3) On OOM (or failure), remember a higher RAM hint for that operator and retry
         later with increased RAM (simple multiplicative backoff).
      4) Avoid assigning more than one op per pool per tick to reduce head-of-line
         thrash and keep behavior predictable.
    """
    # Per-priority FIFO queues of pipelines
    s.q_query = []
    s.q_interactive = []
    s.q_batch = []

    # FIFO tie-breaker
    s._enqueue_counter = 0
    s._enqueued_at = {}  # pipeline_id -> counter

    # Simple per-operator RAM hint table: op_key -> ram_hint
    s._op_ram_hint = {}

    # Config knobs (kept intentionally small / conservative)
    s._reserve_query_frac = 0.20         # reserve 20% pool capacity for QUERY
    s._reserve_interactive_frac = 0.10   # reserve 10% for INTERACTIVE (in addition to QUERY)
    s._oom_ram_backoff = 1.6             # multiply RAM hint on OOM
    s._min_ram_retry_bump = 0.25         # +25% if we have no prior hint
    s._max_assignments_per_pool = 1      # keep it simple: one assignment per pool per tick


def _pclass(priority):
    # Highest first
    if priority == Priority.QUERY:
        return 0
    if priority == Priority.INTERACTIVE:
        return 1
    return 2  # Priority.BATCH_PIPELINE and anything else


def _op_key(op):
    # Best-effort stable key for RAM hinting across retries.
    # We avoid relying on unknown fields; fall back to repr(op).
    for attr in ("op_id", "operator_id", "id", "name"):
        if hasattr(op, attr):
            v = getattr(op, attr)
            if v is not None:
                return f"{attr}:{v}"
    return f"repr:{repr(op)}"


def _queue_for(s, pipeline):
    if pipeline.priority == Priority.QUERY:
        return s.q_query
    if pipeline.priority == Priority.INTERACTIVE:
        return s.q_interactive
    return s.q_batch


def _enqueue_pipeline(s, pipeline):
    pid = pipeline.pipeline_id
    if pid not in s._enqueued_at:
        s._enqueue_counter += 1
        s._enqueued_at[pid] = s._enqueue_counter
    _queue_for(s, pipeline).append(pipeline)


def _pop_next_runnable_pipeline(s):
    # FIFO within priority. We will rotate non-runnable pipelines to preserve ordering
    # without scanning the entire queue too aggressively.
    for q in (s.q_query, s.q_interactive, s.q_batch):
        # Try a bounded rotation: at most len(q) pops.
        n = len(q)
        for _ in range(n):
            p = q.pop(0)
            status = p.runtime_status()

            # Drop completed or permanently failed pipelines
            if status.is_pipeline_successful():
                continue
            if status.state_counts[OperatorState.FAILED] > 0:
                # The starter template drops failures; we do the same for now.
                continue

            # If no runnable ops yet, rotate to end
            ops = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
            if not ops:
                q.append(p)
                continue

            return p, ops
    return None, None


def _reserved_capacity(pool, prio):
    # Compute reserved CPU/RAM that lower priorities should not consume.
    max_cpu = pool.max_cpu_pool
    max_ram = pool.max_ram_pool

    if prio == Priority.QUERY:
        return 0.0, 0.0

    # Reserve a slice for QUERY and INTERACTIVE so batch doesn't starve them.
    reserve_cpu = max_cpu * s._reserve_query_frac
    reserve_ram = max_ram * s._reserve_query_frac

    if prio == Priority.BATCH_PIPELINE:
        reserve_cpu += max_cpu * s._reserve_interactive_frac
        reserve_ram += max_ram * s._reserve_interactive_frac

    return reserve_cpu, reserve_ram


@register_scheduler(key="scheduler_low_015")
def scheduler_low_015_scheduler(s, results, pipelines):
    """
    Priority-aware scheduling policy.

    Admission/queueing:
      - New pipelines go into per-priority FIFO queues.
      - Scheduling always attempts QUERY first, then INTERACTIVE, then BATCH.

    Placement:
      - Iterate pools; for each, schedule up to one runnable operator from the best
        available priority queue, respecting reserved headroom for higher priorities.

    Resizing:
      - Allocate RAM using a remembered per-operator hint (increased after OOM).
      - Allocate CPU more aggressively for high priority, but still bounded.

    Preemption:
      - Not implemented (API for enumerating/suspending running low-priority containers
        is not exposed in the template). Reservations help reduce the need.
    """
    # 1) Ingest new pipelines
    for p in pipelines:
        _enqueue_pipeline(s, p)

    # 2) Learn from results: bump RAM hints on OOM/failure
    for r in results:
        if r is None or not hasattr(r, "failed") or not r.failed():
            continue

        # If we can detect OOM specifically, treat it as a RAM signal; otherwise, be cautious.
        is_oom = False
        if hasattr(r, "error") and r.error is not None:
            err_str = str(r.error).lower()
            if "oom" in err_str or "out of memory" in err_str or "memory" in err_str:
                is_oom = True

        if not hasattr(r, "ops") or not r.ops:
            continue

        for op in r.ops:
            k = _op_key(op)
            prev = s._op_ram_hint.get(k, None)
            if prev is None:
                # Start from last attempted RAM (if present), otherwise do nothing.
                base = getattr(r, "ram", None)
                if base is None:
                    continue
                # Conservative bump to start; stronger if we think it was OOM.
                bump = (1.0 + s._min_ram_retry_bump) if is_oom else 1.15
                s._op_ram_hint[k] = float(base) * bump
            else:
                # Multiplicative backoff; stronger for OOM
                mult = s._oom_ram_backoff if is_oom else 1.2
                s._op_ram_hint[k] = float(prev) * mult

    # Early exit if nothing changed
    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    # 3) For each pool, attempt to schedule a single op from the highest priority queue
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)
        if avail_cpu <= 0.0 or avail_ram <= 0.0:
            continue

        made = 0
        while made < s._max_assignments_per_pool:
            pipeline, ops = _pop_next_runnable_pipeline(s)
            if pipeline is None or not ops:
                break

            # Only assign one op at a time for now
            op_list = ops[:1]
            op = op_list[0]

            # Respect reserved headroom for higher priorities
            reserve_cpu, reserve_ram = _reserved_capacity(pool, pipeline.priority)
            eff_cpu = max(0.0, avail_cpu - reserve_cpu)
            eff_ram = max(0.0, avail_ram - reserve_ram)

            # If this priority cannot use any headroom, put pipeline back and stop for this pool
            if eff_cpu <= 0.0 or eff_ram <= 0.0:
                _queue_for(s, pipeline).append(pipeline)
                break

            # CPU sizing: give more CPU to higher priority, but avoid grabbing the entire pool
            # so multiple pools can still serve other work.
            if pipeline.priority == Priority.QUERY:
                cpu_target = min(eff_cpu, max(1.0, pool.max_cpu_pool * 0.75))
            elif pipeline.priority == Priority.INTERACTIVE:
                cpu_target = min(eff_cpu, max(1.0, pool.max_cpu_pool * 0.50))
            else:
                cpu_target = min(eff_cpu, max(1.0, pool.max_cpu_pool * 0.35))

            # RAM sizing: use hint if any; otherwise a conservative slice based on priority.
            k = _op_key(op)
            hinted = s._op_ram_hint.get(k, None)
            if hinted is not None:
                ram_target = min(eff_ram, float(hinted))
            else:
                if pipeline.priority == Priority.QUERY:
                    ram_target = min(eff_ram, max(0.5, pool.max_ram_pool * 0.25))
                elif pipeline.priority == Priority.INTERACTIVE:
                    ram_target = min(eff_ram, max(0.5, pool.max_ram_pool * 0.20))
                else:
                    ram_target = min(eff_ram, max(0.5, pool.max_ram_pool * 0.15))

            # If targets are too small to be meaningful, requeue and stop for this pool
            if cpu_target <= 0.0 or ram_target <= 0.0:
                _queue_for(s, pipeline).append(pipeline)
                break

            assignments.append(
                Assignment(
                    ops=op_list,
                    cpu=cpu_target,
                    ram=ram_target,
                    priority=pipeline.priority,
                    pool_id=pool_id,
                    pipeline_id=pipeline.pipeline_id,
                )
            )

            # Put pipeline back; it may have more runnable ops later
            _queue_for(s, pipeline).append(pipeline)

            # Update local view of available resources
            avail_cpu -= cpu_target
            avail_ram -= ram_target
            made += 1

    return suspensions, assignments
