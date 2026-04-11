# policy_key: scheduler_medium_009
# reasoning_effort: medium
# exp: reasoning
# model: gpt-5.2-2025-12-11
# llm_cost: 0.091809
# generation_seconds: 85.47
# generated_at: 2026-04-09T22:40:32.465301
@register_scheduler_init(key="scheduler_medium_009")
def scheduler_medium_009_init(s):
    """
    Priority-first, OOM-averse scheduler with simple learning.

    Design goals for the objective:
      - Protect QUERY/INTERACTIVE latency via strict priority ordering + reservations.
      - Avoid 720s penalties by reducing OOM likelihood using per-operator RAM learning:
          * start with conservative RAM defaults (by priority)
          * if an operator fails with OOM, increase its next RAM request quickly (x2)
      - Preserve fairness / avoid starvation via round-robin within each priority queue.
      - Avoid risky preemption (churn) unless the simulator exposes safe container selection
        primitives (not assumed here).
    """
    s.tick = 0

    # Pipeline bookkeeping (store ids in queues to keep stable order)
    s.pipelines_by_id = {}  # pipeline_id -> Pipeline
    s.first_seen_tick = {}  # pipeline_id -> tick

    # Round-robin queues per priority class
    s.queues = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }

    # Per-operator learned resource requests (keyed by operator identity heuristic)
    s.learned_ram = {}  # op_key -> ram
    s.learned_cpu = {}  # op_key -> cpu (lightweight; mostly defaults)

    # A small knob: after this many ticks without any high-priority waiting,
    # allow batch to spill into pool 0 even when multiple pools exist.
    s.batch_spillover_idle_ticks = 10
    s.last_high_pri_waiting_tick = 0


def _op_key(op):
    """Best-effort stable key for an operator object across callbacks."""
    for attr in ("op_id", "operator_id", "id", "name", "key"):
        v = getattr(op, attr, None)
        if v is not None:
            return f"{attr}:{v}"
    # Fallback: repr is not guaranteed stable, but better than nothing.
    return repr(op)


def _priority_rank(priority):
    # Higher is more important.
    if priority == Priority.QUERY:
        return 3
    if priority == Priority.INTERACTIVE:
        return 2
    return 1


def _default_cpu(max_cpu, priority):
    # CPU scaling is sublinear; avoid huge allocations that block concurrency.
    # Keep query a bit "snappier" than interactive; batch minimal.
    if max_cpu <= 0:
        return 0
    if priority == Priority.QUERY:
        return max(1, min(int(max_cpu), 4))
    if priority == Priority.INTERACTIVE:
        return max(1, min(int(max_cpu), 3))
    return 1


def _default_ram(max_ram, priority):
    # RAM beyond minimum gives no speed benefit, but under-allocating risks OOM.
    # Start conservative but not wasteful; learn up on OOM.
    if max_ram <= 0:
        return 0
    if priority == Priority.QUERY:
        frac = 0.22
    elif priority == Priority.INTERACTIVE:
        frac = 0.18
    else:
        frac = 0.12
    return max(1, int(max_ram * frac))


def _pool_is_latency_pool(pool_id, num_pools):
    # If multiple pools exist, treat pool 0 as "latency pool" primarily for high-priority.
    return num_pools > 1 and pool_id == 0


def _cleanup_finished(s, pid):
    """Remove finished pipelines from queues/state."""
    p = s.pipelines_by_id.get(pid)
    if p is None:
        return True
    status = p.runtime_status()
    if status.is_pipeline_successful():
        # Remove from global map; queue cleanup is handled lazily by skipping missing IDs.
        s.pipelines_by_id.pop(pid, None)
        return True
    return False


def _queue_has_ready_high_pri(s):
    """Conservative check: any high-priority pipeline still present in queues."""
    for pri in (Priority.QUERY, Priority.INTERACTIVE):
        q = s.queues.get(pri, [])
        for pid in q:
            if pid in s.pipelines_by_id and not _cleanup_finished(s, pid):
                return True
    return False


def _pick_next_pipeline_id(s, pool_id, num_pools, scheduled_this_tick):
    """
    Pick the next pipeline to schedule for this pool.
    Policy:
      - Always prefer QUERY over INTERACTIVE over BATCH.
      - For latency pool (pool 0 when multi-pool): avoid running BATCH unless no high-pri waiting.
      - Round-robin within each priority queue; skip already-scheduled pipelines this tick.
    """
    latency_pool = _pool_is_latency_pool(pool_id, num_pools)

    high_pri_waiting = _queue_has_ready_high_pri(s)
    if high_pri_waiting:
        s.last_high_pri_waiting_tick = s.tick

    # Batch spillover into latency pool only if high-priority has been idle for a while.
    allow_batch_in_latency_pool = True
    if latency_pool:
        idle_for = s.tick - s.last_high_pri_waiting_tick
        allow_batch_in_latency_pool = (idle_for >= s.batch_spillover_idle_ticks)

    priority_order = [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]
    for pri in priority_order:
        if latency_pool and pri == Priority.BATCH_PIPELINE and not allow_batch_in_latency_pool:
            continue

        q = s.queues.get(pri, [])
        if not q:
            continue

        # Try up to len(q) items to find a usable one.
        for _ in range(len(q)):
            pid = q.pop(0)
            # Keep RR order by appending back; if finished/unknown, drop it.
            if pid not in s.pipelines_by_id:
                continue
            if _cleanup_finished(s, pid):
                continue
            if pid in scheduled_this_tick:
                q.append(pid)
                continue
            q.append(pid)
            return pid
    return None


@register_scheduler(key="scheduler_medium_009")
def scheduler_medium_009_scheduler(s, results, pipelines):
    """
    Scheduler step:
      1) Ingest new pipelines into per-priority RR queues.
      2) Learn from execution results (OOM -> bump per-op RAM quickly).
      3) Assign ready operators, prioritizing QUERY/INTERACTIVE, and reserving headroom
         against batch fill-up when needed.
    """
    s.tick += 1

    # Ingest new arrivals
    for p in pipelines:
        s.pipelines_by_id[p.pipeline_id] = p
        if p.pipeline_id not in s.first_seen_tick:
            s.first_seen_tick[p.pipeline_id] = s.tick
        # Enqueue once; duplicates are avoided by checking membership (O(n), but ok for sim).
        q = s.queues[p.priority]
        if p.pipeline_id not in q:
            q.append(p.pipeline_id)

    # Early exit if nothing changed
    if not pipelines and not results:
        return [], []

    # Learn from results: focus on preventing OOM (completion rate matters a lot).
    for r in results:
        pool = s.executor.pools[r.pool_id]
        max_ram = pool.max_ram_pool
        max_cpu = pool.max_cpu_pool

        ops = getattr(r, "ops", None) or []
        op = ops[0] if ops else None
        if op is None:
            continue
        key = _op_key(op)

        if r.failed():
            err = str(getattr(r, "error", "")).lower()
            # Treat anything that looks like OOM as RAM insufficiency.
            is_oom = ("oom" in err) or ("out of memory" in err) or ("memory" in err and "alloc" in err)
            if is_oom:
                # Exponential backoff, capped at pool max RAM.
                prev = s.learned_ram.get(key, 0)
                base = max(int(getattr(r, "ram", 0) or 0), prev, 1)
                bumped = min(int(max_ram), max(base + 1, base * 2))
                s.learned_ram[key] = max(prev, bumped)
            else:
                # Non-OOM failure: modestly increase CPU (bounded) to avoid pathological slowdowns.
                prev_cpu = s.learned_cpu.get(key, 0)
                base_cpu = int(getattr(r, "cpu", 1) or 1)
                bumped_cpu = min(int(max_cpu), max(prev_cpu, base_cpu, 1))
                s.learned_cpu[key] = bumped_cpu
        else:
            # On success, we can at least set an upper bound that this RAM works.
            # We do NOT proactively reduce, to avoid reintroducing OOMs.
            ran_ram = int(getattr(r, "ram", 0) or 0)
            if ran_ram > 0 and key not in s.learned_ram:
                s.learned_ram[key] = ran_ram
            ran_cpu = int(getattr(r, "cpu", 0) or 0)
            if ran_cpu > 0 and key not in s.learned_cpu:
                s.learned_cpu[key] = ran_cpu

    suspensions = []
    assignments = []

    num_pools = s.executor.num_pools
    scheduled_this_tick = set()

    # Determine if we should reserve headroom (single pool scenario) to protect latency.
    high_pri_waiting = _queue_has_ready_high_pri(s)

    for pool_id in range(num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool
        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # Reservation only matters in single-pool mode; in multi-pool mode we already
        # bias pool 0 away from batch.
        reserve_cpu = 0
        reserve_ram = 0
        if num_pools == 1 and high_pri_waiting:
            # Keep some headroom so a newly-arrived query/interactive can start quickly.
            reserve_cpu = max(1, int(pool.max_cpu_pool * 0.20))
            reserve_ram = max(1, int(pool.max_ram_pool * 0.20))

        # Greedily fill the pool with ready work, respecting reservations for batch.
        # Also limit the number of assignments per pool per tick to avoid overscheduling bursts.
        max_assignments_this_pool = 8
        made = 0

        while made < max_assignments_this_pool:
            if avail_cpu <= 0 or avail_ram <= 0:
                break

            pid = _pick_next_pipeline_id(s, pool_id, num_pools, scheduled_this_tick)
            if pid is None:
                break

            p = s.pipelines_by_id.get(pid)
            if p is None:
                continue

            status = p.runtime_status()
            if status.is_pipeline_successful():
                s.pipelines_by_id.pop(pid, None)
                continue

            # Pick one ready operator whose parents are complete.
            op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
            if not op_list:
                # Not ready yet; try other pipelines.
                continue
            op = op_list[0]
            key = _op_key(op)

            # Compute desired resources (RAM is the main completion risk).
            desired_ram = s.learned_ram.get(key, 0)
            if desired_ram <= 0:
                desired_ram = _default_ram(pool.max_ram_pool, p.priority)

            # Cap overly large learned values to avoid monopolizing an entire pool.
            # Still allows up to 60% of pool RAM for a single op.
            desired_ram = min(int(pool.max_ram_pool), max(1, int(desired_ram)))
            desired_ram = min(desired_ram, max(1, int(pool.max_ram_pool * 0.60)))

            desired_cpu = s.learned_cpu.get(key, 0)
            if desired_cpu <= 0:
                desired_cpu = _default_cpu(pool.max_cpu_pool, p.priority)
            desired_cpu = min(int(pool.max_cpu_pool), max(1, int(desired_cpu)))
            desired_cpu = min(desired_cpu, 4)  # limit to reduce head-of-line blocking

            # If this is batch in single-pool mode, enforce reservations.
            if num_pools == 1 and p.priority == Priority.BATCH_PIPELINE and high_pri_waiting:
                if avail_cpu - desired_cpu < reserve_cpu or avail_ram - desired_ram < reserve_ram:
                    # Don't let batch consume the reserved headroom.
                    break

            # Must have enough RAM to meet our (learned/default) request; avoid knowingly under-allocating.
            if avail_ram < desired_ram:
                # Can't place now; try other pools (next loop) rather than spinning.
                break

            # CPU can be flexed down if needed, but keep at least 1.
            if avail_cpu < 1:
                break
            cpu = min(desired_cpu, int(avail_cpu))
            if cpu < 1:
                break

            assignment = Assignment(
                ops=op_list,
                cpu=cpu,
                ram=int(desired_ram),
                priority=p.priority,
                pool_id=pool_id,
                pipeline_id=p.pipeline_id,
            )
            assignments.append(assignment)
            scheduled_this_tick.add(pid)

            avail_cpu -= cpu
            avail_ram -= int(desired_ram)
            made += 1

    return suspensions, assignments
