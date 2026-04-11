# policy_key: scheduler_high_045
# reasoning_effort: high
# exp: reasoning
# model: gpt-5.2-2025-12-11
# llm_cost: 0.181328
# generation_seconds: 263.18
# generated_at: 2026-04-10T07:33:19.946632
@register_scheduler_init(key="scheduler_high_045")
def scheduler_high_045_init(s):
    """Priority-first, failure-avoiding scheduler with light SRPT and RAM backoff.

    Goals for the weighted-latency objective:
    - Strongly protect QUERY / INTERACTIVE by (a) prioritization, (b) keeping headroom, and (c) reducing OOM retries.
    - Avoid turning pipelines into the 720s penalty case by *retrying* FAILED operators with increased RAM.
    - Keep BATCH making progress (anti-starvation) while still yielding resources to high priority.

    Key ideas:
    - Per-priority FIFO queues, but pick among the first few candidates using a small SRPT-like heuristic
      (prefer fewer remaining ops) plus aging (prefer longer-waiting pipelines).
    - Adaptive RAM: start with conservative defaults; on failure, double RAM up to pool max and retry.
    - Multi-pool: if >=2 pools, pool 0 is treated as "high-priority preferred" (no batch unless nothing else).
    - Reservations: in single-pool mode, keep a small fraction of CPU/RAM unallocated to reduce head-of-line
      blocking when a query arrives (no preemption available).
    """
    s.tick = 0

    # Per-priority queues store pipeline_id in arrival-ish order.
    s.queues = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }

    # Active pipeline objects
    s.pipelines = {}  # pipeline_id -> Pipeline
    s.arrival_tick = {}  # pipeline_id -> tick when first seen
    s.last_scheduled_tick = {}  # pipeline_id -> last tick we scheduled any op from it

    # Urgent pipelines: bumped after failures to get retried quickly (especially for QUERY/INTERACTIVE).
    s.urgent = set()  # pipeline_id

    # Map operator object identity to owning pipeline_id (so we can learn from ExecutionResult without pipeline_id).
    s.op_owner = {}  # id(op) -> pipeline_id

    # Adaptive per-op RAM estimate keyed by operator object identity.
    s.op_ram_est = {}  # id(op) -> ram
    s.op_fail_count = {}  # id(op) -> fail attempts

    # Pipelines deemed "doomed" (e.g., repeated failures at max RAM). We deprioritize to protect others.
    s.doomed = set()  # pipeline_id
    s.max_fail_attempts = 6  # after this at max RAM, deprioritize


def _op_oid(op):
    # Use object identity; result.ops should reference the same objects used in scheduling.
    return id(op)


def _state_counts_total(state_counts):
    try:
        return sum(state_counts.values())
    except Exception:
        return 0


def _pipeline_remaining_ops(pipeline):
    # Best-effort: estimate remaining ops from runtime status counts.
    try:
        st = pipeline.runtime_status()
        total = _state_counts_total(st.state_counts)
        done = st.state_counts.get(OperatorState.COMPLETED, 0)
        if total > 0:
            return max(0, total - done)
    except Exception:
        pass
    # Fallback: unknown; treat as medium size.
    return 100


def _first_ready_op(pipeline, planned_op_oids):
    st = pipeline.runtime_status()
    ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
    if not ops:
        return None
    # Avoid double-assigning the same operator within the same scheduler call.
    for op in ops:
        if _op_oid(op) not in planned_op_oids:
            return op
    return None


def _priority_defaults(pool, priority):
    # Conservative RAM to reduce OOM retries for high priority; moderate CPU caps to allow concurrency.
    max_cpu = pool.max_cpu_pool
    max_ram = pool.max_ram_pool

    # CPU fractions (cap per container)
    if priority == Priority.QUERY:
        cpu_frac = 0.50
        cpu_min = 2
        ram_frac = 0.60
    elif priority == Priority.INTERACTIVE:
        cpu_frac = 0.40
        cpu_min = 2
        ram_frac = 0.50
    else:  # BATCH_PIPELINE
        cpu_frac = 0.25
        cpu_min = 1
        ram_frac = 0.35

    cpu_default = max(cpu_min, max_cpu * cpu_frac)
    ram_default = max(1, max_ram * ram_frac)

    return cpu_default, ram_default


def _compute_request(s, op, pipeline_priority, pool, avail_cpu, avail_ram):
    """Compute (cpu, ram) request for a single-op container."""
    cpu_default, ram_default = _priority_defaults(pool, pipeline_priority)

    oid = _op_oid(op)
    est_ram = s.op_ram_est.get(oid, 0)

    # If we've failed before, treat est_ram as a minimum we should respect (to avoid repeated OOM).
    min_ram = est_ram if est_ram > 0 else 0

    # Choose a target RAM: default unless we learned it needs more.
    target_ram = ram_default
    if est_ram > target_ram:
        target_ram = est_ram

    # If we cannot meet the learned minimum, do not schedule now (likely to fail again).
    if min_ram > 0 and avail_ram < min_ram:
        return None, None

    # RAM request: bounded by availability; if no estimate exists, allow "whatever fits" (fast start).
    if avail_ram <= 0:
        return None, None
    ram_req = min(avail_ram, target_ram)
    if min_ram > 0:
        ram_req = max(ram_req, min_ram)
        if ram_req > avail_ram:
            return None, None

    # CPU request: cap to leave room for concurrency; but always at least 1 if possible.
    if avail_cpu <= 0:
        return None, None
    cpu_req = min(avail_cpu, cpu_default)
    if cpu_req < 1:
        cpu_req = 1

    return cpu_req, ram_req


def _pick_best_pipeline_for_pool(
    s,
    pool_id,
    allowed_priorities,
    avail_cpu,
    avail_ram,
    planned_op_oids,
    scheduled_count,
    reserve_cpu_for_batch,
    reserve_ram_for_batch,
    max_scan=10,
):
    """Pick the best (pipeline_id, op, cpu, ram, queue_priority_key) that fits in this pool."""
    best = None  # (score, prio, idx_in_queue, pid, op, cpu, ram)

    for prio in allowed_priorities:
        q = s.queues.get(prio, [])
        if not q:
            continue

        scan_n = min(max_scan, len(q))
        for i in range(scan_n):
            pid = q[i]
            pipeline = s.pipelines.get(pid)
            if pipeline is None:
                continue

            # Don't let doomed pipelines take precedence unless nothing else fits.
            doomed = pid in s.doomed

            # Limit per-pipeline burstiness within a tick to avoid hogging (especially batch).
            limit = 2 if pipeline.priority in (Priority.QUERY, Priority.INTERACTIVE) else 1
            if scheduled_count.get(pid, 0) >= limit:
                continue

            st = pipeline.runtime_status()
            if st.is_pipeline_successful():
                continue

            op = _first_ready_op(pipeline, planned_op_oids)
            if op is None:
                continue

            cpu_req, ram_req = _compute_request(s, op, pipeline.priority, s.executor.pools[pool_id], avail_cpu, avail_ram)
            if cpu_req is None or ram_req is None:
                continue

            # Enforce batch reservations when scheduling batch in this pool.
            if pipeline.priority == Priority.BATCH_PIPELINE:
                if (avail_cpu - cpu_req) < reserve_cpu_for_batch or (avail_ram - ram_req) < reserve_ram_for_batch:
                    continue

            # Score: small remaining ops first (SRPT-like), then older pipelines; failures get urgency.
            remaining = _pipeline_remaining_ops(pipeline)
            last = s.last_scheduled_tick.get(pid, s.arrival_tick.get(pid, s.tick))
            wait = max(0, s.tick - last)

            urgent_bonus = -2000 if pid in s.urgent else 0
            if doomed:
                urgent_bonus += 500000  # huge penalty; only run if nothing else

            # Stronger aging for batch to prevent starvation / 720s incompletes.
            if pipeline.priority == Priority.QUERY:
                age_weight = 2
            elif pipeline.priority == Priority.INTERACTIVE:
                age_weight = 4
            else:
                age_weight = 8

            # Penalize repeated failures a bit (to reduce churn), but do not starve.
            oid = _op_oid(op)
            fail_cnt = s.op_fail_count.get(oid, 0)
            fail_penalty = min(20000, fail_cnt * 1000)

            score = remaining * 1000 - min(wait, 200) * age_weight * 10 + urgent_bonus + fail_penalty

            if best is None or score < best[0]:
                best = (score, prio, i, pid, op, cpu_req, ram_req)

    if best is None:
        return None

    _, prio, idx, pid, op, cpu_req, ram_req = best
    return prio, idx, pid, op, cpu_req, ram_req


@register_scheduler(key="scheduler_high_045")
def scheduler_high_045(s, results, pipelines):
    """
    Main scheduling step.

    - Updates adaptive RAM estimates from results (doubling on failure).
    - Rebuilds queues to remove completed/stale pipelines.
    - Greedily packs each pool with assignments:
        * If >=2 pools: pool 0 is reserved for QUERY/INTERACTIVE (no batch).
        * Otherwise: keep a small headroom reservation so queries can start quickly.
    """
    s.tick += 1

    # 1) Admit new pipelines
    for p in pipelines:
        pid = p.pipeline_id
        if pid in s.pipelines:
            continue
        s.pipelines[pid] = p
        s.arrival_tick[pid] = s.tick
        s.last_scheduled_tick.setdefault(pid, s.tick)
        s.queues[p.priority].append(pid)

    # 2) Learn from execution results (OOM/backoff) and mark failures urgent
    for r in results:
        if not getattr(r, "ops", None):
            continue

        # Identify owning pipeline via operator identity mapping
        pid = None
        for op in r.ops:
            pid = s.op_owner.get(_op_oid(op))
            if pid is not None:
                break
        if pid is None:
            continue

        if r.failed():
            pool = s.executor.pools[r.pool_id]
            max_ram = pool.max_ram_pool

            for op in r.ops:
                oid = _op_oid(op)
                prev = s.op_ram_est.get(oid, r.ram if getattr(r, "ram", None) is not None else 0)
                if prev <= 0:
                    prev = r.ram if getattr(r, "ram", None) is not None else 1

                # Double RAM on failure, with a small additive bump, capped at pool max.
                new_ram = max(prev * 2, (r.ram * 2) if getattr(r, "ram", None) is not None else 0, prev + 1)
                if new_ram > max_ram:
                    new_ram = max_ram

                s.op_ram_est[oid] = new_ram
                s.op_fail_count[oid] = s.op_fail_count.get(oid, 0) + 1

                # If we're already at max RAM and keep failing, deprioritize to protect overall score.
                if new_ram >= max_ram and s.op_fail_count[oid] >= s.max_fail_attempts:
                    s.doomed.add(pid)

            # Urgent retry, especially for high-priority pipelines
            s.urgent.add(pid)
        else:
            # On success, keep (or increase) estimate at least to the successful RAM allocation.
            for op in r.ops:
                oid = _op_oid(op)
                prev = s.op_ram_est.get(oid, 0)
                if getattr(r, "ram", None) is not None and r.ram > prev:
                    s.op_ram_est[oid] = r.ram
                # Lightly decay failure counter on success.
                if oid in s.op_fail_count and s.op_fail_count[oid] > 0:
                    s.op_fail_count[oid] -= 1

    # 3) Cleanup: remove completed pipelines from active maps and queues
    completed = set()
    for pid, p in list(s.pipelines.items()):
        try:
            if p.runtime_status().is_pipeline_successful():
                completed.add(pid)
        except Exception:
            # If status is unavailable for some reason, keep it.
            pass

    if completed:
        for prio in list(s.queues.keys()):
            s.queues[prio] = [pid for pid in s.queues[prio] if pid not in completed]
        for pid in completed:
            s.pipelines.pop(pid, None)
            s.arrival_tick.pop(pid, None)
            s.last_scheduled_tick.pop(pid, None)
            s.urgent.discard(pid)
            s.doomed.discard(pid)

    # Also purge unknown pipeline_ids (stale) from queues.
    for prio in list(s.queues.keys()):
        s.queues[prio] = [pid for pid in s.queues[prio] if pid in s.pipelines]

    # 4) Scheduling: greedily pack pools with assignments
    suspensions = []
    assignments = []

    planned_op_oids = set()
    scheduled_count = {}  # pipeline_id -> count assigned in this call

    num_pools = s.executor.num_pools

    # Determine whether any high-priority work is pending/ready, to decide reservations.
    def _has_any_hi_pending():
        for pid, p in s.pipelines.items():
            if p.priority in (Priority.QUERY, Priority.INTERACTIVE):
                try:
                    if not p.runtime_status().is_pipeline_successful():
                        return True
                except Exception:
                    return True
        return False

    def _has_any_hi_ready():
        for prio in (Priority.QUERY, Priority.INTERACTIVE):
            q = s.queues.get(prio, [])
            scan_n = min(10, len(q))
            for i in range(scan_n):
                pid = q[i]
                p = s.pipelines.get(pid)
                if p is None:
                    continue
                try:
                    if _first_ready_op(p, planned_op_oids) is not None:
                        return True
                except Exception:
                    continue
        return False

    hi_pending = _has_any_hi_pending()
    hi_ready = _has_any_hi_ready()

    for pool_id in range(num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool

        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # Pool policy:
        # - If multiple pools: pool 0 is preferred for high priority and does not run batch.
        # - Otherwise: keep a small reservation so queries can start quickly without preemption.
        if num_pools >= 2 and pool_id == 0:
            allowed_priorities = [Priority.QUERY, Priority.INTERACTIVE]
        else:
            allowed_priorities = [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]

        # Batch reservation logic (only relevant when considering batch).
        if num_pools == 1:
            # Single pool: keep some headroom always; more if high-priority is pending/ready.
            reserve_frac = 0.30 if (hi_pending or hi_ready) else 0.15
            reserve_cpu_for_batch = pool.max_cpu_pool * reserve_frac
            reserve_ram_for_batch = pool.max_ram_pool * reserve_frac
        else:
            # With a dedicated high-priority pool, only reserve in other pools when HI backlog exists.
            reserve_frac = 0.15 if hi_ready else 0.0
            reserve_cpu_for_batch = pool.max_cpu_pool * reserve_frac
            reserve_ram_for_batch = pool.max_ram_pool * reserve_frac

        # Greedy packing loop
        while True:
            # Stop if nothing meaningful fits.
            if avail_cpu <= 0 or avail_ram <= 0:
                break

            pick = _pick_best_pipeline_for_pool(
                s,
                pool_id=pool_id,
                allowed_priorities=allowed_priorities,
                avail_cpu=avail_cpu,
                avail_ram=avail_ram,
                planned_op_oids=planned_op_oids,
                scheduled_count=scheduled_count,
                reserve_cpu_for_batch=reserve_cpu_for_batch,
                reserve_ram_for_batch=reserve_ram_for_batch,
                max_scan=12,
            )
            if pick is None:
                break

            queue_prio, idx_in_queue, pid, op, cpu_req, ram_req = pick
            pipeline = s.pipelines.get(pid)
            if pipeline is None:
                # Stale; remove from queue and continue.
                q = s.queues.get(queue_prio, [])
                if 0 <= idx_in_queue < len(q) and q[idx_in_queue] == pid:
                    q.pop(idx_in_queue)
                continue

            # Ensure we do not exceed available resources.
            if cpu_req > avail_cpu:
                cpu_req = avail_cpu
            if ram_req > avail_ram:
                ram_req = avail_ram
            if cpu_req <= 0 or ram_req <= 0:
                break

            # Assign single operator per container (safe default).
            assignments.append(
                Assignment(
                    ops=[op],
                    cpu=cpu_req,
                    ram=ram_req,
                    priority=pipeline.priority,
                    pool_id=pool_id,
                    pipeline_id=pid,
                )
            )

            # Track ownership for learning from future ExecutionResult.
            s.op_owner[_op_oid(op)] = pid

            # Update local accounting for greedy packing.
            planned_op_oids.add(_op_oid(op))
            avail_cpu -= cpu_req
            avail_ram -= ram_req

            scheduled_count[pid] = scheduled_count.get(pid, 0) + 1
            s.last_scheduled_tick[pid] = s.tick
            s.urgent.discard(pid)

            # Rotate pipeline within its queue (round-robin fairness within the priority class).
            q = s.queues.get(queue_prio, [])
            # Remove the chosen pipeline id at idx, then append to end.
            if 0 <= idx_in_queue < len(q) and q[idx_in_queue] == pid:
                q.pop(idx_in_queue)
                q.append(pid)

            # If pool 0 (multi-pool) and no more high-priority ready work, we intentionally do NOT
            # backfill with batch to keep low tail latency for sudden query arrivals.
            if num_pools >= 2 and pool_id == 0:
                # Soft stop if both query and interactive look empty/blocked.
                if not _has_any_hi_ready():
                    break

    return suspensions, assignments
