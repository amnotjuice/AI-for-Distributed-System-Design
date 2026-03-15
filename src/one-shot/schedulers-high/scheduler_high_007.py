# policy_key: scheduler_high_007
# reasoning_effort: high
# model: gpt-5.2-2025-12-11
# llm_cost: 0.127315
# generation_seconds: 125.24
# generated_at: 2026-03-12T22:39:18.234145
@register_scheduler_init(key="scheduler_high_007")
def scheduler_high_007_init(s):
    """
    Priority-aware round-robin scheduler with small, incremental improvements over naive FIFO:
      1) Maintain per-priority queues (QUERY > INTERACTIVE > BATCH) to reduce tail latency.
      2) Right-size per-assignment CPU/RAM (caps by priority) to improve concurrency vs. "give all".
      3) Reserve a small fraction of each pool for high-priority work when high-priority is waiting.
      4) OOM-aware retries: if an op OOMs, retry with exponentially more RAM (capped), otherwise mark hard-failed.
    """
    from collections import deque, defaultdict

    # Priority queues (round-robin within each priority)
    s.queues = {
        Priority.QUERY: deque(),
        Priority.INTERACTIVE: deque(),
        Priority.BATCH_PIPELINE: deque(),
    }

    # Track pipeline IDs currently enqueued to avoid duplicates
    s.in_queue = {}  # pipeline_id -> Priority

    # Failure tracking
    s.hard_failed_pipelines = set()  # pipeline_ids that should be dropped (non-OOM failures or too many retries)
    s.op_oom_retries = defaultdict(int)  # (pipeline_id, op_key) -> retry count

    # Tuning knobs (kept simple on purpose)
    s.max_oom_retries = 4

    # Resource sizing heuristics (fractions of pool max; clamped by current available/budget)
    s.cpu_share = {
        Priority.QUERY: 0.75,
        Priority.INTERACTIVE: 0.50,
        Priority.BATCH_PIPELINE: 0.25,
    }
    s.ram_share = {
        Priority.QUERY: 0.60,
        Priority.INTERACTIVE: 0.45,
        Priority.BATCH_PIPELINE: 0.25,
    }

    # Reservation to protect latency: only applied when high-priority work is waiting
    s.reserve_cpu_frac_for_hp = 0.25
    s.reserve_ram_frac_for_hp = 0.25


def _priority_of(pipeline):
    pr = getattr(pipeline, "priority", Priority.BATCH_PIPELINE)
    if pr in (Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE):
        return pr
    return Priority.BATCH_PIPELINE


def _is_oom_error(err):
    if not err:
        return False
    msg = str(err).lower()
    return ("oom" in msg) or ("out of memory" in msg) or ("memory" in msg and "exceed" in msg)


def _op_key(op):
    # Try stable identifiers first; fall back to object id
    for attr in ("op_id", "operator_id", "id", "name"):
        if hasattr(op, attr):
            v = getattr(op, attr)
            if v is not None:
                return (attr, str(v))
    return ("pyid", str(id(op)))


def _cleanup_queue(s, q):
    """Remove completed/hard-failed pipelines and keep in-queue map consistent."""
    kept = []
    while q:
        p = q.popleft()
        pid = p.pipeline_id
        if pid in s.hard_failed_pipelines:
            # drop
            s.in_queue.pop(pid, None)
            continue
        status = p.runtime_status()
        if status.is_pipeline_successful():
            # drop completed
            s.in_queue.pop(pid, None)
            continue
        kept.append(p)
    for p in kept:
        q.append(p)


def _alloc_for(pool, priority, avail_cpu, avail_ram, oom_retries):
    """
    Compute a CPU/RAM allocation that:
      - caps by priority share of pool max
      - stays within current available/budget
      - increases RAM exponentially after OOM (bounded by availability)
    """
    # CPU cap by priority share
    cpu_cap = pool.max_cpu_pool * float(safe_get(priority, s_cpu_share=None))
    # (Work around: we'll pass shares directly from caller; avoid global access issues.)


def safe_get(key, mapping, default=None):
    try:
        return mapping.get(key, default)
    except Exception:
        return default


def _compute_alloc(s, pool, pr, budget_cpu, budget_ram, oom_retries):
    # Caps based on pool size and priority
    cpu_cap = float(pool.max_cpu_pool) * float(safe_get(pr, s.cpu_share, 0.25))
    ram_cap = float(pool.max_ram_pool) * float(safe_get(pr, s.ram_share, 0.25))

    # Base allocation: up to cap, but not exceeding budget
    cpu = min(float(budget_cpu), float(cpu_cap))
    ram = min(float(budget_ram), float(ram_cap))

    # Ensure we request something positive if any budget exists
    if cpu <= 0 and budget_cpu > 0:
        cpu = float(budget_cpu)
    if ram <= 0 and budget_ram > 0:
        ram = float(budget_ram)

    # OOM backoff: exponential RAM growth, clamped by remaining budget
    if oom_retries > 0 and budget_ram > 0:
        ram = min(float(budget_ram), max(ram, float(ram) * (2.0 ** float(oom_retries))))

    # Final clamp
    cpu = max(0.0, min(float(budget_cpu), float(cpu)))
    ram = max(0.0, min(float(budget_ram), float(ram)))
    return cpu, ram


def _schedule_from_queue(
    s,
    q,
    pool_id,
    pool,
    budget_cpu,
    budget_ram,
    scheduled_this_tick,
    max_attempts,
):
    """
    Round-robin: try up to `max_attempts` pipelines from the queue, scheduling at most one op
    per pipeline per tick. Returns (assignments, new_budget_cpu, new_budget_ram).
    """
    assignments = []
    attempts = 0

    while budget_cpu > 0 and budget_ram > 0 and q and attempts < max_attempts:
        attempts += 1
        p = q.popleft()
        pid = p.pipeline_id

        # Drop hard-failed pipelines eagerly
        if pid in s.hard_failed_pipelines:
            s.in_queue.pop(pid, None)
            continue

        status = p.runtime_status()
        if status.is_pipeline_successful():
            s.in_queue.pop(pid, None)
            continue

        # Enforce "at most one op per pipeline per tick" for fairness and latency predictability
        if pid in scheduled_this_tick:
            q.append(p)
            continue

        # Find a single ready op
        op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
        if not op_list:
            q.append(p)
            continue

        op = op_list[0]
        ok = (pid, _op_key(op))
        oom_retries = int(s.op_oom_retries.get(ok, 0))

        # Compute allocation
        pr = _priority_of(p)
        cpu, ram = _compute_alloc(s, pool, pr, budget_cpu, budget_ram, oom_retries)

        if cpu <= 0 or ram <= 0:
            # Can't fit anything right now
            q.appendleft(p)
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

        scheduled_this_tick.add(pid)
        budget_cpu -= cpu
        budget_ram -= ram

        # Requeue pipeline for future ops
        q.append(p)

    return assignments, budget_cpu, budget_ram


@register_scheduler(key="scheduler_high_007")
def scheduler_high_007_scheduler(s, results, pipelines):
    """
    Priority-aware scheduler:
      - Enqueue new pipelines by priority (QUERY > INTERACTIVE > BATCH)
      - Process results to detect OOM vs non-OOM failures
      - For each pool (most headroom first):
          1) Schedule high-priority queues using full available resources.
          2) If high-priority is waiting, reserve a fraction of the pool and only let batch use the remainder.
    """
    # Enqueue new pipelines
    for p in pipelines:
        pid = p.pipeline_id
        if pid in s.hard_failed_pipelines:
            continue
        if pid in s.in_queue:
            continue
        pr = _priority_of(p)
        s.queues[pr].append(p)
        s.in_queue[pid] = pr

    # Incorporate execution results (failures / OOM backoff)
    for r in results:
        if not r.failed():
            continue

        pid = getattr(r, "pipeline_id", None)
        # If pipeline_id isn't present on results, rely on the op object association only.
        # But Assignment includes pipeline_id; most sims propagate it. We'll still handle None gracefully.
        is_oom = _is_oom_error(getattr(r, "error", None))

        # Update per-op backoff when possible
        ops = getattr(r, "ops", None) or []
        for op in ops:
            if pid is None:
                # Can't key properly; treat as hard failure unless OOM (still can't retry well)
                if not is_oom:
                    continue
                else:
                    continue
            key = (pid, _op_key(op))
            if is_oom:
                s.op_oom_retries[key] += 1
                if s.op_oom_retries[key] > s.max_oom_retries:
                    s.hard_failed_pipelines.add(pid)
            else:
                s.hard_failed_pipelines.add(pid)

        # If we have pipeline_id but no ops list, still decide fate conservatively
        if pid is not None and not ops:
            if not is_oom:
                s.hard_failed_pipelines.add(pid)

    # If nothing changed, no need to act
    if not pipelines and not results:
        return [], []

    # Clean up queues (remove completed/hard-failed)
    _cleanup_queue(s, s.queues[Priority.QUERY])
    _cleanup_queue(s, s.queues[Priority.INTERACTIVE])
    _cleanup_queue(s, s.queues[Priority.BATCH_PIPELINE])

    # Are we currently latency-sensitive? (any high-priority waiting)
    hp_waiting = bool(s.queues[Priority.QUERY] or s.queues[Priority.INTERACTIVE])

    # Sort pools by current headroom to place latency-sensitive work first where it fits best
    pool_ids = list(range(s.executor.num_pools))
    pool_ids.sort(
        key=lambda i: (
            float(s.executor.pools[i].avail_cpu_pool) + 1e-9
        ) * (
            float(s.executor.pools[i].avail_ram_pool) + 1e-9
        ),
        reverse=True,
    )

    suspensions = []  # no preemption in this incremental version (keeps code robust)
    assignments = []
    scheduled_this_tick = set()

    for pool_id in pool_ids:
        pool = s.executor.pools[pool_id]
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)
        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # Stage 1: schedule QUERY then INTERACTIVE with full availability
        # (Try up to queue length attempts each time to keep round-robin progress.)
        q_query = s.queues[Priority.QUERY]
        q_inter = s.queues[Priority.INTERACTIVE]
        q_batch = s.queues[Priority.BATCH_PIPELINE]

        a, avail_cpu, avail_ram = _schedule_from_queue(
            s, q_query, pool_id, pool, avail_cpu, avail_ram, scheduled_this_tick, max_attempts=len(q_query) + 1
        )
        assignments.extend(a)

        a, avail_cpu, avail_ram = _schedule_from_queue(
            s, q_inter, pool_id, pool, avail_cpu, avail_ram, scheduled_this_tick, max_attempts=len(q_inter) + 1
        )
        assignments.extend(a)

        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # Stage 2: schedule batch, possibly leaving a reservation for future high-priority arrivals
        if hp_waiting:
            reserve_cpu = float(pool.max_cpu_pool) * float(s.reserve_cpu_frac_for_hp)
            reserve_ram = float(pool.max_ram_pool) * float(s.reserve_ram_frac_for_hp)
            batch_budget_cpu = max(0.0, avail_cpu - reserve_cpu)
            batch_budget_ram = max(0.0, avail_ram - reserve_ram)
        else:
            batch_budget_cpu = avail_cpu
            batch_budget_ram = avail_ram

        if batch_budget_cpu > 0 and batch_budget_ram > 0:
            a, batch_budget_cpu, batch_budget_ram = _schedule_from_queue(
                s, q_batch, pool_id, pool, batch_budget_cpu, batch_budget_ram, scheduled_this_tick, max_attempts=len(q_batch) + 1
            )
            assignments.extend(a)

            # Reflect batch consumption in actual available resources
            used_cpu = (avail_cpu - (batch_budget_cpu + (float(pool.max_cpu_pool) * float(s.reserve_cpu_frac_for_hp) if hp_waiting else 0.0))) if hp_waiting else (avail_cpu - batch_budget_cpu)
            used_ram = (avail_ram - (batch_budget_ram + (float(pool.max_ram_pool) * float(s.reserve_ram_frac_for_hp) if hp_waiting else 0.0))) if hp_waiting else (avail_ram - batch_budget_ram)
            # The above "used_*" calc can be distorted by reservations; instead, just recompute conservatively:
            # Reduce avail by the sum of CPU/RAM in the batch assignments for this pool.
            # (Robust and simple; avoids double-counting.)
            if a:
                cpu_spent = 0.0
                ram_spent = 0.0
                for asn in a:
                    cpu_spent += float(asn.cpu)
                    ram_spent += float(asn.ram)
                avail_cpu = max(0.0, avail_cpu - cpu_spent)
                avail_ram = max(0.0, avail_ram - ram_spent)

    return suspensions, assignments
