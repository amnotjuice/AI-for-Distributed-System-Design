# policy_key: scheduler_high_005
# reasoning_effort: high
# model: gpt-5.2-2025-12-11
# llm_cost: 0.167971
# generation_seconds: 167.94
# generated_at: 2026-03-12T22:34:28.033995
@register_scheduler_init(key="scheduler_high_005")
def scheduler_high_005_init(s):
    """Priority-aware, latency-first FIFO with small, safe improvements over naive FIFO.

    Improvements (kept intentionally incremental):
      1) Separate FIFO queues by priority; always schedule higher priority first.
      2) Light admission control for latency: keep a reserved CPU/RAM slice so batch can't
         consume everything (helps tail latency when interactive/query arrives).
      3) Basic OOM-aware RAM retry: on detected OOM, increase RAM request for that operator
         on subsequent retries (no guesswork about exact minima, just multiplicative backoff).
      4) Optional "latency pool": if multiple pools exist, pool 0 prefers high priority work.
      5) Mild starvation prevention: very old batch pipelines get promoted into the interactive
         queue (still run with batch limits, just earlier consideration).
    """
    s.tick = 0

    # FIFO queues per "scheduling class"
    s.queues = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }

    # Track whether a pipeline is currently present in any queue (avoid duplicates)
    s.in_queues = set()

    # Track pipeline metadata for aging and terminal failures
    s.pipeline_arrival_tick = {}   # pipeline_id -> tick
    s.pipeline_hard_failed = {}    # pipeline_id -> bool (non-OOM failure seen)

    # Map executed ops back to pipeline_id (because ExecutionResult doesn't carry pipeline_id)
    s.op_to_pipeline = {}          # id(op) -> pipeline_id

    # Per-operator RAM scaling factor; increases on OOM and stays until process ends
    s.op_ram_scale = {}            # id(op) -> float
    s.max_ram_scale = 16.0

    # Latency protection knobs (small, conservative defaults)
    s.reserve_frac_cpu = 0.20
    s.reserve_frac_ram = 0.20

    # Batch caps (prevents batch from grabbing an entire pool, reducing interference)
    s.batch_cpu_cap_frac = 0.50
    s.batch_ram_cap_frac = 0.60

    # Interactive is allowed to be large but slightly capped to reduce over-allocation
    s.interactive_cpu_cap_frac = 0.85
    s.interactive_ram_cap_frac = 0.90

    # Minimum allocations to avoid degenerate 0 allocations
    s.min_cpu = 0.1
    s.min_ram = 0.1

    # Starvation prevention: promote batch after enough scheduler ticks
    s.batch_aging_threshold_ticks = 250
    s.promoted_batch = set()  # pipeline_id promoted into interactive queue

    # If multiple pools exist, pool 0 prefers latency-sensitive work first
    s.latency_pool_id = 0


def _is_oom_error(err):
    if not err:
        return False
    e = str(err).lower()
    return ("oom" in e) or ("out of memory" in e) or ("out-of-memory" in e) or ("memoryerror" in e)


def _priority_order():
    # Highest to lowest
    return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]


def _enqueue_pipeline(s, p, target_priority=None):
    pid = p.pipeline_id
    if pid in s.in_queues:
        return
    if pid not in s.pipeline_arrival_tick:
        s.pipeline_arrival_tick[pid] = s.tick
    pr = target_priority if target_priority is not None else p.priority
    s.queues[pr].append(p)
    s.in_queues.add(pid)


def _drop_pipeline(s, p):
    pid = p.pipeline_id
    if pid in s.in_queues:
        s.in_queues.remove(pid)
    # Keep arrival tick for potential debugging; safe to leave as-is.


def _maybe_promote_aged_batch(s):
    # Move very old batch pipelines into the interactive queue for earlier consideration.
    # NOTE: pipeline.priority remains BATCH; this only changes scheduling queue order.
    batch_q = s.queues[Priority.BATCH_PIPELINE]
    if not batch_q:
        return

    kept = []
    moved = []
    for p in batch_q:
        pid = p.pipeline_id
        if pid in s.promoted_batch:
            kept.append(p)
            continue
        arrival = s.pipeline_arrival_tick.get(pid, s.tick)
        if (s.tick - arrival) >= s.batch_aging_threshold_ticks:
            s.promoted_batch.add(pid)
            moved.append(p)
        else:
            kept.append(p)

    if moved:
        s.queues[Priority.BATCH_PIPELINE] = kept
        # Insert promoted batch at the end of interactive FIFO
        s.queues[Priority.INTERACTIVE].extend(moved)


def _pipeline_is_terminal(s, p):
    pid = p.pipeline_id
    if s.pipeline_hard_failed.get(pid, False):
        return True
    st = p.runtime_status()
    return st.is_pipeline_successful()


def _pick_next_ready(s, prio_list, scheduled_pids):
    """Round-robin within each priority queue; return (pipeline, op_list) or (None, None)."""
    for pr in prio_list:
        q = s.queues.get(pr, [])
        if not q:
            continue

        n = len(q)
        for _ in range(n):
            p = q.pop(0)
            pid = p.pipeline_id

            # Drop terminal pipelines eagerly
            if _pipeline_is_terminal(s, p):
                _drop_pipeline(s, p)
                continue

            # Avoid scheduling two ops from the same pipeline in the same tick
            if pid in scheduled_pids:
                q.append(p)
                continue

            st = p.runtime_status()
            op_list = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]

            if not op_list:
                # Not ready yet; keep it in FIFO
                q.append(p)
                continue

            # Found a runnable op; keep pipeline in FIFO by appending to end
            q.append(p)
            return p, op_list

    return None, None


def _compute_allocation(s, pool, pipeline_priority, avail_cpu, avail_ram, op):
    """Compute CPU/RAM request with small latency protections and OOM backoff."""
    max_cpu = pool.max_cpu_pool
    max_ram = pool.max_ram_pool

    op_scale = s.op_ram_scale.get(id(op), 1.0)

    # Reserve headroom so batch can't take everything (helps tail latency).
    reserve_cpu = s.reserve_frac_cpu * max_cpu
    reserve_ram = s.reserve_frac_ram * max_ram

    if pipeline_priority == Priority.BATCH_PIPELINE:
        eff_cpu = avail_cpu - reserve_cpu
        eff_ram = avail_ram - reserve_ram
        if eff_cpu <= 0 or eff_ram <= 0:
            return 0.0, 0.0

        cpu_cap = min(eff_cpu, s.batch_cpu_cap_frac * max_cpu)
        ram_cap = min(eff_ram, s.batch_ram_cap_frac * max_ram)

        cpu = max(s.min_cpu, cpu_cap)
        ram = min(eff_ram, max(s.min_ram, ram_cap * op_scale))
        return cpu, ram

    if pipeline_priority == Priority.INTERACTIVE:
        cpu_cap = min(avail_cpu, s.interactive_cpu_cap_frac * max_cpu)
        ram_cap = min(avail_ram, s.interactive_ram_cap_frac * max_ram)
        cpu = max(s.min_cpu, cpu_cap)
        ram = min(avail_ram, max(s.min_ram, ram_cap * op_scale))
        return cpu, ram

    # Priority.QUERY (or any other unexpected high-priority): be aggressive for latency.
    cpu = max(s.min_cpu, min(avail_cpu, max_cpu))
    ram = min(avail_ram, max(s.min_ram, min(avail_ram, max_ram) * op_scale))
    return cpu, ram


@register_scheduler(key="scheduler_high_005")
def scheduler_high_005_scheduler(s, results, pipelines):
    """
    Priority-aware scheduler:
      - Enqueue new pipelines into per-priority FIFO queues (no duplicates).
      - Update per-operator RAM backoff on OOM failures; hard-fail pipelines on non-OOM errors.
      - For each pool, pick the highest priority runnable op.
      - If multiple pools exist, pool 0 prefers query/interactive; falls back to batch if none ready.
    """
    s.tick += 1

    # Enqueue new pipelines
    for p in pipelines:
        _enqueue_pipeline(s, p)

    # Process results (OOM backoff / hard fail)
    for r in results:
        if not r.failed():
            continue

        is_oom = _is_oom_error(r.error)
        for op in (r.ops or []):
            op_id = id(op)
            pid = s.op_to_pipeline.get(op_id)

            if is_oom:
                prev = s.op_ram_scale.get(op_id, 1.0)
                s.op_ram_scale[op_id] = min(prev * 2.0, s.max_ram_scale)
            else:
                if pid is not None:
                    s.pipeline_hard_failed[pid] = True

    # Early exit if nothing could have changed decisions
    if not pipelines and not results:
        return [], []

    # Promote very old batch to reduce starvation (doesn't change batch resource limits)
    _maybe_promote_aged_batch(s)

    suspensions = []
    assignments = []
    scheduled_pids = set()

    num_pools = s.executor.num_pools

    for pool_id in range(num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool
        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # Latency pool behavior: prefer high priority first if pool 0 and multiple pools exist.
        if num_pools > 1 and pool_id == s.latency_pool_id:
            p, op_list = _pick_next_ready(s, [Priority.QUERY, Priority.INTERACTIVE], scheduled_pids)
            if p is None:
                p, op_list = _pick_next_ready(s, _priority_order(), scheduled_pids)
        else:
            p, op_list = _pick_next_ready(s, _priority_order(), scheduled_pids)

        if p is None or not op_list:
            continue

        # Compute allocation and submit assignment
        op0 = op_list[0]
        cpu, ram = _compute_allocation(s, pool, p.priority, avail_cpu, avail_ram, op0)

        # If batch can't fit due to reservation, skip this pool this tick.
        if cpu <= 0 or ram <= 0:
            continue

        # Map op -> pipeline for future result handling
        for op in op_list:
            s.op_to_pipeline[id(op)] = p.pipeline_id

        assignments.append(
            Assignment(
                ops=op_list,
                cpu=cpu,
                ram=ram,
                priority=p.priority,
                pool_id=pool_id,
                pipeline_id=p.pipeline_id,
            )
        )
        scheduled_pids.add(p.pipeline_id)

    return suspensions, assignments
