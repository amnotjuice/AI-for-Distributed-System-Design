# policy_key: scheduler_none_010
# reasoning_effort: none
# model: gpt-5.2-2025-12-11
# llm_cost: 0.030603
# generation_seconds: 39.54
# generated_at: 2026-03-12T21:27:59.593411
@register_scheduler_init(key="scheduler_none_010")
def scheduler_none_010_init(s):
    """Priority-aware FIFO with small, safe improvements over the naive baseline.

    Goals / incremental improvements:
    1) Priority ordering: always consider higher-priority pipelines first.
    2) Avoid head-of-line blocking: keep per-priority queues and requeue pipelines that can't run yet.
    3) Basic OOM-aware RAM bumping: if an op failed, retry with more RAM (bounded), instead of dropping the whole pipeline.
    4) Gentle CPU sizing: don't give an op the entire pool; cap per-assignment CPU to reduce interference and improve tail latency.

    Notes:
    - This policy stays conservative: no preemption (yet), no complex runtime prediction.
    - It tries to improve latency primarily by making sure interactive/query work gets first access to resources.
    """
    # Per-priority waiting queues (FIFO within each class)
    s.waiting_by_prio = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }

    # Remember last known resource "hint" per (pipeline_id, op_id) to avoid repeated OOMs.
    # Key is a tuple; value is dict(cpu=..., ram=...)
    s.op_hints = {}

    # Simple knobs (kept small/obvious)
    s.max_assignments_per_tick_per_pool = 2  # don't flood a pool with many new containers in one tick
    s.cpu_cap_fraction = 0.5               # at most 50% of a pool's available CPU for a single op
    s.min_cpu = 1                          # minimum CPU to allocate if available
    s.ram_bump_factor = 1.5                # multiplicative RAM bump on OOM
    s.ram_bump_additive = 256              # additive RAM bump (MB/units) to ensure progress for small values
    s.max_ram_fraction = 0.9               # never allocate more than 90% of pool RAM to one op


def _prio_order():
    # Higher priority first: QUERY > INTERACTIVE > BATCH_PIPELINE
    return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]


def _op_key(pipeline, op):
    # Use operator_id if present; fallback to object id for safety in simulator
    op_id = getattr(op, "operator_id", None)
    if op_id is None:
        op_id = getattr(op, "op_id", None)
    if op_id is None:
        op_id = id(op)
    return (pipeline.pipeline_id, op_id)


def _bump_ram(prev_ram, avail_ram, max_ram):
    # Conservative bump bounded by availability and a fraction of pool capacity
    bumped = int(prev_ram * 1.5) + 256
    cap = int(max_ram * 0.9)
    bumped = min(bumped, cap)
    bumped = min(bumped, int(avail_ram))
    return max(1, bumped)


def _choose_cpu(avail_cpu, max_cpu, cap_fraction, min_cpu):
    if avail_cpu <= 0:
        return 0
    cap = max(min_cpu, int(avail_cpu * cap_fraction))
    cap = min(cap, int(avail_cpu))
    cap = min(cap, int(max_cpu))
    # If cap becomes 0 due to int truncation but we have some CPU, request at least 1
    if cap <= 0 and avail_cpu >= 1:
        cap = 1
    return cap


@register_scheduler(key="scheduler_none_010")
def scheduler_none_010(s, results, pipelines):
    """
    Priority-aware, OOM-adaptive scheduler.

    Mechanics:
    - Enqueue new pipelines by priority.
    - Process execution results to update per-op resource hints (especially after failures).
    - For each pool, schedule up to N ops this tick, always pulling from the highest-priority
      queue first. Allocate modest CPU slices and RAM based on hints (or a small default).
    """
    # Enqueue new pipelines
    for p in pipelines:
        # Unknown priorities still get enqueued; default to batch behavior
        pr = p.priority
        if pr not in s.waiting_by_prio:
            s.waiting_by_prio[pr] = []
        s.waiting_by_prio[pr].append(p)

    # Update hints from results (learn from OOMs / failures)
    for r in results:
        # If we get results without ops (shouldn't happen), ignore safely
        if not getattr(r, "ops", None):
            continue
        op = r.ops[0]
        # We need a pipeline_id to key; r may not carry it. We store by (pipeline_id, op_id),
        # so we must recover pipeline_id from the op if possible.
        pipeline_id = getattr(op, "pipeline_id", None)
        if pipeline_id is None:
            # Can't reliably store hint; skip
            continue

        op_id = getattr(op, "operator_id", None)
        if op_id is None:
            op_id = getattr(op, "op_id", None)
        if op_id is None:
            op_id = id(op)
        key = (pipeline_id, op_id)

        # Record last attempted resources (helps avoid oscillation)
        s.op_hints.setdefault(key, {})
        s.op_hints[key]["cpu"] = getattr(r, "cpu", None)
        s.op_hints[key]["ram"] = getattr(r, "ram", None)

        # On failure: bump RAM for next attempt (assume OOM-like; safe even if other failure)
        if r.failed():
            prev_ram = getattr(r, "ram", None)
            if prev_ram is not None:
                # We don't know which pool will run next; store "desired" ram as bumped from prev
                # and let pool-specific bounds apply at scheduling time.
                s.op_hints[key]["desired_ram_bump_from"] = prev_ram

    # Early exit if nothing to do
    if not pipelines and not results:
        # Still might have waiting queues, so do not early-exit solely on empty inputs.
        has_waiting = any(len(q) > 0 for q in s.waiting_by_prio.values())
        if not has_waiting:
            return [], []

    suspensions = []
    assignments = []

    # Helper to pop next runnable pipeline in priority order, with requeue to avoid HOL blocking.
    def pop_next_pipeline():
        for pr in _prio_order():
            q = s.waiting_by_prio.get(pr, [])
            while q:
                p = q.pop(0)
                status = p.runtime_status()
                # Drop completed pipelines
                if status.is_pipeline_successful():
                    continue
                # Keep pipelines even if some op failed; we will retry failed ops (ASSIGNABLE_STATES includes FAILED)
                return p
        return None

    # Requeue list to preserve ordering after inspection
    requeue = {pr: [] for pr in s.waiting_by_prio.keys()}

    # For each pool, schedule a small number of ops, prioritizing high-priority work
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool

        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        scheduled_here = 0
        # We'll attempt multiple picks; if a pipeline can't produce an assignable op, we requeue it.
        while scheduled_here < s.max_assignments_per_tick_per_pool:
            if avail_cpu <= 0 or avail_ram <= 0:
                break

            p = pop_next_pipeline()
            if p is None:
                break

            status = p.runtime_status()

            # Choose the next assignable op whose parents are complete
            op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
            if not op_list:
                # Nothing ready now; requeue to its priority queue tail
                requeue.setdefault(p.priority, []).append(p)
                continue

            op = op_list[0]

            # Determine resources using hints (best-effort)
            key = _op_key(p, op)
            hint = s.op_hints.get(key, {})

            # CPU: cap to reduce interference and improve latency for interactive work
            # Give QUERY a slightly higher cap than others (still bounded)
            cap_fraction = s.cpu_cap_fraction
            if p.priority == Priority.QUERY:
                cap_fraction = min(0.7, max(cap_fraction, 0.6))
            elif p.priority == Priority.INTERACTIVE:
                cap_fraction = min(0.6, max(cap_fraction, 0.5))
            else:
                cap_fraction = min(0.5, max(cap_fraction, 0.4))

            cpu = _choose_cpu(avail_cpu, pool.max_cpu_pool, cap_fraction, s.min_cpu)
            if cpu <= 0:
                # Can't run anything on this pool this tick
                requeue.setdefault(p.priority, []).append(p)
                break

            # RAM: start from a conservative slice; bump on failures when we have history
            # Default: allocate up to 50% of avail RAM for QUERY/INTERACTIVE, 33% for BATCH (bounded)
            if p.priority in (Priority.QUERY, Priority.INTERACTIVE):
                default_ram = max(1, int(min(avail_ram, pool.max_ram_pool) * 0.5))
            else:
                default_ram = max(1, int(min(avail_ram, pool.max_ram_pool) * 0.33))

            ram = default_ram

            # If we have a previous attempt RAM, prefer that (it likely worked if it completed; if it failed, we'll bump)
            prev_ram = hint.get("ram", None)
            if prev_ram is not None:
                ram = max(ram, int(prev_ram))

            # If a failure was observed, bump from the failing allocation (bounded per pool availability)
            bump_from = hint.get("desired_ram_bump_from", None)
            if bump_from is not None:
                ram = max(ram, _bump_ram(int(bump_from), avail_ram, pool.max_ram_pool))

            # Ensure we never request more than available and max fraction of pool
            ram_cap = int(pool.max_ram_pool * s.max_ram_fraction)
            ram = min(int(ram), int(avail_ram), ram_cap)
            if ram <= 0:
                requeue.setdefault(p.priority, []).append(p)
                break

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

            # Reduce local estimates so we can pack a couple of ops per tick
            avail_cpu -= cpu
            avail_ram -= ram
            scheduled_here += 1

            # Requeue pipeline for subsequent ops
            requeue.setdefault(p.priority, []).append(p)

    # Put requeued pipelines back preserving priority queues
    for pr, items in requeue.items():
        if pr not in s.waiting_by_prio:
            s.waiting_by_prio[pr] = []
        s.waiting_by_prio[pr].extend(items)

    return suspensions, assignments
