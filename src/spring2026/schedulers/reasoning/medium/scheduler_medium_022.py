# policy_key: scheduler_medium_022
# reasoning_effort: medium
# model: gpt-5.2-2025-12-11
# llm_cost: 0.069050
# generation_seconds: 118.71
# generated_at: 2026-03-14T02:44:05.940978
@register_scheduler_init(key="scheduler_medium_022")
def scheduler_medium_022_init(s):
    """
    Priority-aware, low-risk improvement over naive FIFO.

    Changes vs naive:
      1) Separate queues per priority; schedule in strict priority order (QUERY > INTERACTIVE > BATCH).
      2) Soft reservations: when higher-priority work is waiting, leave some headroom by limiting what
         lower priorities may consume (reduces tail latency under contention).
      3) OOM-aware retries: if an op fails with an OOM-like error, retry it later with increased RAM.
      4) Simple round-robin within each priority queue to avoid starvation within the same class.

    Notes:
      - This policy intentionally avoids preemption (needs richer runtime visibility); it focuses on
        "obvious flaws" and should be robust with the minimal API shown in the template.
    """
    s.tick = 0

    # Per-priority pipeline queues (round-robin using pop(0)/append()).
    s.queues = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }

    # Track when a pipeline first arrived (pipeline_id -> tick) for potential future aging.
    s.pipeline_arrival_tick = {}

    # Per-operator hints keyed by Python object identity (works because ops persist in Pipeline DAG).
    s.op_ram_hint = {}       # op_id -> last known "safe" RAM
    s.op_fail_count = {}     # op_id -> consecutive-ish failure count (bounded by dropping after max)

    # Retry / sizing knobs.
    s.max_retries_per_op = 3

    # Default request sizes as fractions of pool capacity (per priority).
    s.default_ram_frac = {
        Priority.QUERY: 0.25,
        Priority.INTERACTIVE: 0.20,
        Priority.BATCH_PIPELINE: 0.15,
    }
    s.default_cpu_frac = {
        Priority.QUERY: 0.50,
        Priority.INTERACTIVE: 0.33,
        Priority.BATCH_PIPELINE: 0.25,
    }

    # Soft reservations to protect higher priorities when they are waiting.
    # (Applied only if there is runnable higher-priority work in the queues.)
    s.reserve_for_query_frac = (0.10, 0.10)   # (cpu_frac, ram_frac) reserved for QUERY
    s.reserve_for_qi_frac = (0.20, 0.20)      # reserved for QUERY+INTERACTIVE (when scheduling BATCH)

    # Ensure BATCH makes some progress even under constant higher-priority load.
    s.min_batch_share_frac = (0.05, 0.05)     # minimum effective cpu/ram for BATCH if anything is free

    # Limit assignments per pool per tick to reduce overscheduling bursts.
    s.max_assignments_per_pool_per_tick = 4


def _is_oom_error(err) -> bool:
    if err is None:
        return False
    msg = str(err).lower()
    return ("oom" in msg) or ("out of memory" in msg) or ("memoryerror" in msg)


def _op_id(op) -> int:
    # Use object identity; stable for the life of the Pipeline object in the simulator.
    return id(op)


def _has_runnable_work(queue) -> bool:
    # Returns True if any pipeline in this queue has at least one assignable op
    # whose parents are complete.
    for p in queue:
        status = p.runtime_status()
        if status.is_pipeline_successful():
            continue
        op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        if op_list:
            return True
    return False


def _next_runnable_from_queue(s, queue):
    """
    Round-robin selection: scan at most len(queue) pipelines by rotating the queue.
    Returns (pipeline, op) or (None, None) if nothing runnable.
    Pipelines that are completed or hopelessly failed are dropped.
    """
    if not queue:
        return None, None

    n = len(queue)
    for _ in range(n):
        p = queue.pop(0)
        status = p.runtime_status()

        # Drop completed pipelines.
        if status.is_pipeline_successful():
            continue

        # Find runnable ops (includes FAILED if ASSIGNABLE_STATES includes FAILED).
        op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        if not op_list:
            # Nothing runnable yet; keep it in the queue.
            queue.append(p)
            continue

        # Prefer retrying ops that have failed (if any) by sorting by fail count desc.
        # This is a heuristic: it tends to clear "stuck" pipelines sooner once memory is right-sized.
        def failcnt(op):
            return s.op_fail_count.get(_op_id(op), 0)

        op_list = sorted(op_list, key=failcnt, reverse=True)

        # Pick the first op that hasn't exceeded retry budget.
        chosen = None
        for op in op_list:
            if s.op_fail_count.get(_op_id(op), 0) < s.max_retries_per_op:
                chosen = op
                break

        if chosen is None:
            # Pipeline has only over-budget failed ops; drop it to avoid infinite loops.
            continue

        # Keep pipeline in queue for future scheduling (round-robin).
        queue.append(p)
        return p, chosen

    return None, None


def _compute_request(s, pool, priority, op, avail_cpu, avail_ram):
    """
    Compute (cpu, ram) request for an operator, bounded by local available resources.
    """
    # CPU sizing: give higher priority a larger slice for lower latency, but keep concurrency.
    cpu_req = pool.max_cpu_pool * s.default_cpu_frac.get(priority, 0.25)
    cpu_req = max(1.0, float(cpu_req))
    cpu_req = min(float(avail_cpu), float(cpu_req))

    # RAM sizing: use hint if available; otherwise a conservative fraction of pool RAM.
    opid = _op_id(op)
    if opid in s.op_ram_hint:
        ram_req = float(s.op_ram_hint[opid])
    else:
        ram_req = pool.max_ram_pool * s.default_ram_frac.get(priority, 0.15)
        ram_req = max(1.0, float(ram_req))

    ram_req = min(float(avail_ram), float(ram_req))

    # If we can't allocate meaningful resources, signal inability to schedule.
    if cpu_req < 1.0 or ram_req < 1.0:
        return 0.0, 0.0
    return cpu_req, ram_req


@register_scheduler(key="scheduler_medium_022")
def scheduler_medium_022(s, results: List["ExecutionResult"], pipelines: List["Pipeline"]) -> Tuple[List["Suspend"], List["Assignment"]]:
    """
    See init() docstring. This function:
      - Enqueues new pipelines by priority
      - Updates per-op RAM hints based on execution results (OOM backoff)
      - Schedules runnable ops in priority order with soft reservations
    """
    s.tick += 1

    # Enqueue new pipelines.
    for p in pipelines:
        s.queues[p.priority].append(p)
        if p.pipeline_id not in s.pipeline_arrival_tick:
            s.pipeline_arrival_tick[p.pipeline_id] = s.tick

    # Update hints from execution results.
    # Note: ExecutionResult does not expose pipeline_id in the provided API list, so we key by op identity.
    if results:
        for r in results:
            if not getattr(r, "ops", None):
                continue

            failed = r.failed()
            is_oom = failed and _is_oom_error(getattr(r, "error", None))

            for op in r.ops:
                opid = _op_id(op)

                if failed:
                    s.op_fail_count[opid] = s.op_fail_count.get(opid, 0) + 1
                    if is_oom:
                        # Backoff RAM quickly on OOM-like failures.
                        # Use last allocation as baseline if available.
                        prev = s.op_ram_hint.get(opid, float(getattr(r, "ram", 0.0)) or 1.0)
                        base = float(getattr(r, "ram", 0.0)) or float(prev) or 1.0
                        s.op_ram_hint[opid] = max(prev, base * 2.0)
                else:
                    # On success, clear failure streak and record a "safe" RAM hint (do not downsize aggressively).
                    s.op_fail_count[opid] = 0
                    if getattr(r, "ram", None) is not None:
                        s.op_ram_hint[opid] = max(float(getattr(r, "ram", 0.0)), float(s.op_ram_hint.get(opid, 0.0)))

    # Early exit: if nothing changed and no new pipelines, do nothing.
    if not pipelines and not results:
        return [], []

    suspensions: List["Suspend"] = []
    assignments: List["Assignment"] = []

    # Determine if higher-priority work is runnable (used for enabling reservations).
    query_runnable = _has_runnable_work(s.queues[Priority.QUERY])
    interactive_runnable = _has_runnable_work(s.queues[Priority.INTERACTIVE])

    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        local_avail_cpu = float(pool.avail_cpu_pool)
        local_avail_ram = float(pool.avail_ram_pool)

        if local_avail_cpu <= 0.0 or local_avail_ram <= 0.0:
            continue

        assigned_this_pool = 0

        # Helper to compute effective available resources for a given priority with soft reservations.
        def effective_avail(priority):
            cpu = local_avail_cpu
            ram = local_avail_ram

            if priority == Priority.QUERY:
                return cpu, ram

            if priority == Priority.INTERACTIVE:
                if query_runnable:
                    cpu_res = float(pool.max_cpu_pool) * float(s.reserve_for_query_frac[0])
                    ram_res = float(pool.max_ram_pool) * float(s.reserve_for_query_frac[1])
                    cpu = max(0.0, cpu - cpu_res)
                    ram = max(0.0, ram - ram_res)
                return cpu, ram

            # BATCH
            if query_runnable or interactive_runnable:
                cpu_res = float(pool.max_cpu_pool) * float(s.reserve_for_qi_frac[0])
                ram_res = float(pool.max_ram_pool) * float(s.reserve_for_qi_frac[1])
                cpu = max(0.0, cpu - cpu_res)
                ram = max(0.0, ram - ram_res)

                # Ensure some minimal progress for batch if anything is free.
                cpu_min = float(pool.max_cpu_pool) * float(s.min_batch_share_frac[0])
                ram_min = float(pool.max_ram_pool) * float(s.min_batch_share_frac[1])
                cpu = max(cpu, min(cpu_min, local_avail_cpu))
                ram = max(ram, min(ram_min, local_avail_ram))
            return cpu, ram

        # Schedule in strict priority order.
        for priority in (Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE):
            # Keep scheduling while (a) we have room, (b) we have runnable work, (c) we haven't overscheduled this pool.
            while assigned_this_pool < s.max_assignments_per_pool_per_tick:
                eff_cpu, eff_ram = effective_avail(priority)
                if eff_cpu < 1.0 or eff_ram < 1.0:
                    break

                p, op = _next_runnable_from_queue(s, s.queues[priority])
                if p is None or op is None:
                    break

                cpu_req, ram_req = _compute_request(s, pool, priority, op, eff_cpu, eff_ram)
                if cpu_req <= 0.0 or ram_req <= 0.0:
                    # Can't fit anything meaningful in this pool right now.
                    break

                # Bound hints to pool max to avoid runaway after repeated OOM backoffs.
                opid = _op_id(op)
                if opid in s.op_ram_hint:
                    s.op_ram_hint[opid] = min(float(s.op_ram_hint[opid]), float(pool.max_ram_pool))

                assignments.append(
                    Assignment(
                        ops=[op],
                        cpu=cpu_req,
                        ram=ram_req,
                        priority=p.priority,
                        pool_id=pool_id,
                        pipeline_id=p.pipeline_id,
                    )
                )

                # Update local accounting so we don't overcommit within this tick.
                local_avail_cpu -= cpu_req
                local_avail_ram -= ram_req
                assigned_this_pool += 1

                if local_avail_cpu < 1.0 or local_avail_ram < 1.0:
                    break

    return suspensions, assignments
