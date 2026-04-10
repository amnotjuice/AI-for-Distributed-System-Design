# source: scheduler_low_011.py
# model: gpt-5.2-2025-12-11
# effort: low
# feedback: minimal
# iteration: r7
@register_scheduler_init(key="scheduler_low_011_r7")
def scheduler_low_011_r7_init(s):
    """Priority-first packing with headroom reservations + real OOM-aware retries.

    Small, incremental improvements vs naive/previous:
    - Always prioritize QUERY/INTERACTIVE over BATCH (separate FIFO queues).
    - Pack multiple assignments per pool per tick (avoid leaving resources idle).
    - Reserve CPU/RAM headroom for high-priority work (no preemption available here).
    - Track per-operator retry hints using operator identity captured at assignment time,
      so OOM retries actually increase RAM on the next attempt.
    """
    s.waiting_queues = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }

    # Pipeline index (best-effort de-dupe / reference)
    s.pipelines_by_id = {}

    # Tick counter for light bookkeeping (optional future aging); kept for determinism/debugging
    s.tick = 0

    # OOM retry state keyed by operator object identity.
    # NOTE: ExecutionResult does not expose pipeline_id, so we must key by op id(op).
    s.op_hints = {}          # op_id -> {"ram": float, "cpu": float}
    s.op_attempts = {}       # op_id -> int (OOM attempts)
    s.op_last_req = {}       # op_id -> {"ram": float, "cpu": float, "pool_id": int, "priority": Priority, "pipeline_id": str/int}
    s.op_give_up = set()     # op_ids that should not be retried (non-OOM failure or too many OOMs)

    # Knobs: keep conservative and simple
    s.max_oom_retries = 4
    s.max_assignments_per_pool = 8
    s.scan_limit_per_pick = 24

    # Pool preferences
    s.interactive_pool_id = 0

    # Headroom reservation to protect latency without preemption:
    # - global reserve (all pools) when high-priority backlog exists
    # - extra reserve on the interactive pool
    s.reserve_global = {"cpu_frac": 0.10, "ram_frac": 0.10}
    s.reserve_interactive_extra = {"cpu_frac": 0.25, "ram_frac": 0.25}

    # Sizing fractions (of pool max); hints can override upwards
    s.fracs_hp = {"cpu": 0.95, "ram": 0.70}          # QUERY/INTERACTIVE: run fast, avoid OOM
    s.fracs_batch_busy = {"cpu": 0.25, "ram": 0.30}  # when HP backlog exists: keep batch "thin"
    s.fracs_batch_idle = {"cpu": 0.80, "ram": 0.60}  # when no HP backlog: let batch use more


def _priority_order():
    # Highest to lowest
    return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]


def _is_oom_error(err) -> bool:
    if err is None:
        return False
    msg = str(err).lower()
    return ("oom" in msg) or ("out of memory" in msg) or ("memoryerror" in msg)


def _safe_failed(res) -> bool:
    try:
        return bool(res.failed())
    except Exception:
        return getattr(res, "error", None) is not None


def _prune_pipeline_if_terminal_or_hopeless(s, p) -> bool:
    """Return True if pipeline should be dropped from queues."""
    status = p.runtime_status()
    if status.is_pipeline_successful():
        return True

    # If pipeline has failed ops, only keep it if those ops are retriable (i.e., not in give_up).
    failed_ops = status.get_ops([OperatorState.FAILED], require_parents_complete=False) or []
    for op in failed_ops:
        if id(op) in s.op_give_up:
            return True

    return False


def _next_assignable_op(s, p):
    """Prefer retrying FAILED ops first (if retriable), else schedule PENDING."""
    status = p.runtime_status()

    failed_ready = status.get_ops([OperatorState.FAILED], require_parents_complete=True) or []
    for op in failed_ready:
        if id(op) not in s.op_give_up:
            return op

    pending_ready = status.get_ops([OperatorState.PENDING], require_parents_complete=True) or []
    if pending_ready:
        return pending_ready[0]

    return None


def _cap_positive(x, lo=1.0):
    try:
        if x is None:
            return lo
        x = float(x)
        return lo if x < lo else x
    except Exception:
        return lo


def _compute_reserve(s, pool, pool_id, hp_backlog: bool):
    """Return absolute (cpu, ram) to keep free for future HP admission."""
    if not hp_backlog:
        return 0.0, 0.0

    cpu_r = pool.max_cpu_pool * s.reserve_global["cpu_frac"]
    ram_r = pool.max_ram_pool * s.reserve_global["ram_frac"]

    if pool_id == s.interactive_pool_id:
        cpu_r += pool.max_cpu_pool * s.reserve_interactive_extra["cpu_frac"]
        ram_r += pool.max_ram_pool * s.reserve_interactive_extra["ram_frac"]

    return max(0.0, cpu_r), max(0.0, ram_r)


def _request_for_op(s, pool, pool_id, priority, op, cpu_budget, ram_budget, hp_backlog: bool):
    """Choose a CPU/RAM request within the provided budgets, applying OOM hints."""
    op_id = id(op)

    if priority in (Priority.QUERY, Priority.INTERACTIVE):
        fr = s.fracs_hp
    else:
        fr = s.fracs_batch_busy if hp_backlog else s.fracs_batch_idle

    # Start from fraction of pool max, then cap to budgets.
    cpu = min(cpu_budget, pool.max_cpu_pool * fr["cpu"])
    ram = min(ram_budget, pool.max_ram_pool * fr["ram"])

    cpu = _cap_positive(cpu, lo=1.0)
    ram = _cap_positive(ram, lo=1.0)

    # Apply hints from previous OOM retries (or last request) to avoid repeating OOM.
    hint = s.op_hints.get(op_id)
    if hint:
        # CPU hint is soft; we can shrink CPU to fit budget.
        cpu = max(cpu, _cap_positive(hint.get("cpu", 1.0), lo=1.0))
        # RAM hint is hard for avoiding repeated OOM; if it doesn't fit, we must skip this pool.
        ram = max(ram, _cap_positive(hint.get("ram", 1.0), lo=1.0))

    # Final cap to budgets (CPU can shrink; RAM cannot go below chosen value)
    if ram > ram_budget:
        return None

    cpu = min(cpu, cpu_budget)
    cpu = _cap_positive(cpu, lo=1.0)

    # Also cap to pool max (defensive)
    cpu = min(cpu, pool.max_cpu_pool)
    ram = min(ram, pool.max_ram_pool)

    if cpu <= 0 or ram <= 0:
        return None
    return cpu, ram


def _try_schedule_one_from_queue(s, pool, pool_id, q, priority, cpu_budget, ram_budget, hp_backlog, scheduled_pipelines):
    """Scan FIFO queue for a pipeline with a ready op that fits; schedule at most one assignment."""
    if cpu_budget <= 0 or ram_budget <= 0 or not q:
        return None

    # Scan a limited window to avoid O(n) full scans every time.
    scan_n = min(len(q), s.scan_limit_per_pick)
    for _ in range(scan_n):
        p = q.pop(0)

        # Best-effort de-dupe per tick: avoid one pipeline dominating all pools this tick
        if p.pipeline_id in scheduled_pipelines:
            q.append(p)
            continue

        if _prune_pipeline_if_terminal_or_hopeless(s, p):
            # Drop terminal/hopeless pipeline
            continue

        op = _next_assignable_op(s, p)
        if op is None:
            # Not schedulable yet; keep it around
            q.append(p)
            continue

        # If we have explicitly given up on this failed op, drop the pipeline (it will not progress).
        if id(op) in s.op_give_up:
            continue

        req = _request_for_op(s, pool, pool_id, priority, op, cpu_budget, ram_budget, hp_backlog)
        if req is None:
            # Doesn't fit in this pool budget; requeue and keep scanning others
            q.append(p)
            continue

        cpu, ram = req
        assignment = Assignment(
            ops=[op],
            cpu=cpu,
            ram=ram,
            priority=priority,
            pool_id=pool_id,
            pipeline_id=p.pipeline_id,
        )

        # Requeue pipeline for subsequent ops
        q.append(p)

        # Record request metadata so OOM retries can increase RAM meaningfully next time.
        op_id = id(op)
        s.op_last_req[op_id] = {
            "cpu": float(cpu),
            "ram": float(ram),
            "pool_id": int(pool_id),
            "priority": priority,
            "pipeline_id": p.pipeline_id,
        }

        scheduled_pipelines.add(p.pipeline_id)
        return assignment

    return None


@register_scheduler(key="scheduler_low_011_r7")
def scheduler_low_011_r7(s, results, pipelines):
    """
    Scheduler step:
    1) Enqueue new pipelines into per-priority FIFO queues.
    2) Process results: on OOM failures, increase RAM hint; on non-OOM failures, give up retrying that op.
    3) Schedule:
       - First admit high-priority ops (QUERY, INTERACTIVE), packing pools (one HP op per pool per tick).
       - Then pack BATCH ops using remaining resources, while keeping reserved headroom if HP backlog exists.
    """
    s.tick += 1

    # Enqueue new arrivals
    for p in pipelines:
        s.pipelines_by_id[p.pipeline_id] = p
        pr = p.priority if p.priority in s.waiting_queues else Priority.BATCH_PIPELINE
        s.waiting_queues[pr].append(p)

    # Process execution results to adjust OOM hints / retry behavior
    for r in results:
        failed = _safe_failed(r)
        ops = getattr(r, "ops", None) or []
        if not isinstance(ops, list):
            ops = list(ops)

        if not ops:
            continue

        if failed:
            is_oom = _is_oom_error(getattr(r, "error", None))
            for op in ops:
                op_id = id(op)
                if is_oom:
                    prev_attempts = int(s.op_attempts.get(op_id, 0)) + 1
                    s.op_attempts[op_id] = prev_attempts

                    if prev_attempts > s.max_oom_retries:
                        # Stop retrying: continuing will only churn.
                        s.op_give_up.add(op_id)
                        continue

                    # Baseline from last request, then from prior hint, then from result.
                    last = s.op_last_req.get(op_id, {})
                    prev_hint = s.op_hints.get(op_id, {})

                    baseline_ram = max(
                        _cap_positive(last.get("ram", 1.0), lo=1.0),
                        _cap_positive(prev_hint.get("ram", 1.0), lo=1.0),
                        _cap_positive(getattr(r, "ram", 1.0), lo=1.0),
                    )
                    baseline_cpu = max(
                        _cap_positive(last.get("cpu", 1.0), lo=1.0),
                        _cap_positive(prev_hint.get("cpu", 1.0), lo=1.0),
                        _cap_positive(getattr(r, "cpu", 1.0), lo=1.0),
                    )

                    # Exponential backoff on RAM; CPU stays as-is (CPU isn't the cause of OOM).
                    s.op_hints[op_id] = {"ram": float(baseline_ram * 2.0), "cpu": float(baseline_cpu)}
                else:
                    # Non-OOM failures are treated as non-retriable for this simple policy.
                    s.op_give_up.add(op_id)
        else:
            # Success: clear per-op retry state to avoid memory growth and stale hints.
            for op in ops:
                op_id = id(op)
                if op_id in s.op_hints:
                    del s.op_hints[op_id]
                if op_id in s.op_attempts:
                    del s.op_attempts[op_id]
                if op_id in s.op_last_req:
                    del s.op_last_req[op_id]
                if op_id in s.op_give_up:
                    s.op_give_up.remove(op_id)

    # Early exit if nothing changed
    if not pipelines and not results:
        return [], []

    suspensions = []  # No preemption in this iteration (no safe container inventory access in the template)
    assignments = []

    # Approximate HP backlog: if queues are non-empty. (We prune while scanning anyway.)
    hp_backlog = bool(s.waiting_queues[Priority.QUERY] or s.waiting_queues[Priority.INTERACTIVE])

    # Track remaining budget per pool locally so we can pack multiple assignments safely.
    pool_cpu_budget = {}
    pool_ram_budget = {}
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        pool_cpu_budget[pool_id] = float(pool.avail_cpu_pool)
        pool_ram_budget[pool_id] = float(pool.avail_ram_pool)

    scheduled_pipelines = set()

    # ---- Phase 1: schedule high-priority (one per pool per tick, but across all pools) ----
    if s.executor.num_pools > 0:
        pool_order_hp = [s.interactive_pool_id] + [i for i in range(s.executor.num_pools) if i != s.interactive_pool_id]
    else:
        pool_order_hp = []

    for pool_id in pool_order_hp:
        pool = s.executor.pools[pool_id]
        cpu_b = pool_cpu_budget[pool_id]
        ram_b = pool_ram_budget[pool_id]
        if cpu_b <= 0 or ram_b <= 0:
            continue

        # One HP op per pool per tick: reduce contention and improve tail latency.
        for pr in (Priority.QUERY, Priority.INTERACTIVE):
            a = _try_schedule_one_from_queue(
                s, pool, pool_id, s.waiting_queues[pr], pr, cpu_b, ram_b, hp_backlog, scheduled_pipelines
            )
            if a is not None:
                assignments.append(a)
                cpu_b -= float(a.cpu)
                ram_b -= float(a.ram)
                pool_cpu_budget[pool_id] = cpu_b
                pool_ram_budget[pool_id] = ram_b
                break

    # ---- Phase 2: schedule batch, packing while keeping headroom if HP backlog exists ----
    if s.executor.num_pools > 1:
        pool_order_batch = [i for i in range(s.executor.num_pools) if i != s.interactive_pool_id] + [s.interactive_pool_id]
    else:
        pool_order_batch = list(range(s.executor.num_pools))

    for pool_id in pool_order_batch:
        pool = s.executor.pools[pool_id]
        cpu_b = pool_cpu_budget[pool_id]
        ram_b = pool_ram_budget[pool_id]
        if cpu_b <= 0 or ram_b <= 0:
            continue

        # Keep some headroom for future HP admission (no preemption).
        res_cpu, res_ram = _compute_reserve(s, pool, pool_id, hp_backlog=hp_backlog)
        cpu_for_batch = max(0.0, cpu_b - res_cpu)
        ram_for_batch = max(0.0, ram_b - res_ram)

        # Pack multiple small batch ops to improve overall utilization without blocking HP next tick.
        slots = s.max_assignments_per_pool
        while slots > 0 and cpu_for_batch > 0 and ram_for_batch > 0:
            a = _try_schedule_one_from_queue(
                s,
                pool,
                pool_id,
                s.waiting_queues[Priority.BATCH_PIPELINE],
                Priority.BATCH_PIPELINE,
                cpu_for_batch,
                ram_for_batch,
                hp_backlog,
                scheduled_pipelines,
            )
            if a is None:
                break

            assignments.append(a)
            cpu_b -= float(a.cpu)
            ram_b -= float(a.ram)
            pool_cpu_budget[pool_id] = cpu_b
            pool_ram_budget[pool_id] = ram_b

            # Recompute remaining batch-usable budget after the assignment.
            cpu_for_batch = max(0.0, cpu_b - res_cpu)
            ram_for_batch = max(0.0, ram_b - res_ram)
            slots -= 1

    return suspensions, assignments