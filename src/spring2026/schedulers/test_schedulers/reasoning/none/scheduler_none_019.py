# policy_key: scheduler_none_019
# reasoning_effort: none
# model: gpt-5.2-2025-12-11
# llm_cost: 0.047219
# generation_seconds: 49.47
# generated_at: 2026-04-09T21:59:29.577491
@register_scheduler_init(key="scheduler_none_019")
def scheduler_none_019_init(s):
    """Priority-aware, failure-averse scheduler.

    Design goals for the weighted latency objective:
      - Keep QUERY/INTERACTIVE tail latency low by (a) prioritizing them in dispatch and
        (b) reserving pool headroom so a new high-priority arrival can start quickly.
      - Avoid failures (720s penalty) by learning per-operator RAM needs from OOM failures
        and retrying with increased RAM (bounded), rather than abandoning pipelines.
      - Avoid starvation by adding simple aging (time-in-queue) that gradually boosts
        lower priorities when they wait too long.

    Key mechanisms:
      1) Central waiting queue of pipelines (not per-pool), scored each tick.
      2) Per-(pipeline, op) RAM "hint" table updated on failure; used on retries.
      3) Per-pool soft reservations: keep some CPU/RAM free for high-priority work.
      4) Very light preemption: if QUERY cannot fit anywhere, suspend one BATCH container
         (then INTERACTIVE) from the same pool to free headroom. Avoid churn by cooldown.
    """
    # Waiting pipelines and lightweight metadata
    s.waiting_queue = []          # FIFO append; selection uses scoring
    s.enqueue_ts = {}             # pipeline_id -> first seen scheduler clock
    s.pipeline_obj = {}           # pipeline_id -> Pipeline (latest reference)

    # Failure-avoidance: learned RAM needs per operator instance
    # Key: (pipeline_id, op_id) -> ram_hint
    s.op_ram_hint = {}

    # Keep track of recent preemptions to avoid thrashing
    s.last_preempt_ts = {}        # (pool_id) -> last preempt clock
    s.preempt_cooldown_s = 5.0

    # A monotonically increasing "scheduler clock" (ticks) used for aging; simulator is deterministic
    s.t = 0

    # Tuning knobs (conservative to start)
    s.max_parallel_ops_per_pool = 2  # don't overpack; reduce interference/latency spikes
    s.max_ops_per_assignment = 1     # one op per assignment (keeps sizing specific)
    s.ram_backoff_mult = 1.6         # on failure, increase RAM hint
    s.ram_backoff_add = 0.0          # additive bump (kept 0 to avoid overshooting small ops)
    s.ram_cap_frac_of_pool = 0.9     # don't allocate > 90% pool RAM to one op by default
    s.cpu_cap_frac_of_pool = 0.9     # don't allocate > 90% pool CPU to one op by default
    s.cpu_small_frac = 0.25          # default CPU fraction when contention
    s.cpu_large_frac = 0.6           # when plenty headroom, give more CPU to finish quickly

    # Reservations to keep latency low for high priority arrivals (soft targets)
    s.reserve = {
        Priority.QUERY: {"cpu_frac": 0.20, "ram_frac": 0.20},
        Priority.INTERACTIVE: {"cpu_frac": 0.10, "ram_frac": 0.10},
        Priority.BATCH_PIPELINE: {"cpu_frac": 0.00, "ram_frac": 0.00},
    }

    # Aging parameters: after this many ticks, boost lower priorities
    s.aging_start = 20
    s.aging_slope = 0.02


@register_scheduler(key="scheduler_none_019")
def scheduler_none_019_scheduler(s, results, pipelines):
    """
    Scheduler step: returns (suspensions, assignments).
    """
    s.t += 1

    def _prio_weight(prio):
        if prio == Priority.QUERY:
            return 10.0
        if prio == Priority.INTERACTIVE:
            return 5.0
        return 1.0

    def _base_prio_score(prio):
        # Larger is better
        if prio == Priority.QUERY:
            return 3.0
        if prio == Priority.INTERACTIVE:
            return 2.0
        return 1.0

    def _now():
        return float(s.t)

    def _pipeline_status(p):
        return p.runtime_status()

    def _is_done_or_hard_failed(p):
        st = _pipeline_status(p)
        # We keep retrying failed ops (ASSIGNABLE includes FAILED), so only drop when success.
        # If the simulator marks unrecoverable failures differently, the policy is still safe:
        # it will keep trying; worst case is queue growth, but avoids 720s from "dropped".
        return st.is_pipeline_successful()

    def _next_assignable_ops(p, limit=1):
        st = _pipeline_status(p)
        # Prefer ops whose parents are complete to progress pipeline critical path.
        ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        if not ops:
            return []
        return ops[:limit]

    def _op_id(op):
        # Try common id attributes; fallback to Python object id for stability within run.
        for attr in ("op_id", "operator_id", "id", "name"):
            if hasattr(op, attr):
                try:
                    v = getattr(op, attr)
                    if v is not None:
                        return v
                except Exception:
                    pass
        return id(op)

    def _pool_effective_avail(pool_id):
        pool = s.executor.pools[pool_id]
        return pool.avail_cpu_pool, pool.avail_ram_pool, pool.max_cpu_pool, pool.max_ram_pool

    def _reserved_headroom(pool_id, incoming_prio):
        # Keep headroom for at least QUERY; then INTERACTIVE.
        # For batch, still respect QUERY/INTERACTIVE reservations.
        pool = s.executor.pools[pool_id]
        max_cpu = pool.max_cpu_pool
        max_ram = pool.max_ram_pool

        # Always reserve for QUERY and INTERACTIVE, because they dominate score.
        res_cpu = max_cpu * (s.reserve[Priority.QUERY]["cpu_frac"] + s.reserve[Priority.INTERACTIVE]["cpu_frac"])
        res_ram = max_ram * (s.reserve[Priority.QUERY]["ram_frac"] + s.reserve[Priority.INTERACTIVE]["ram_frac"])

        # If scheduling a QUERY, allow it to dip into INTERACTIVE reserve (but still keep QUERY reserve minimal).
        if incoming_prio == Priority.QUERY:
            res_cpu = max_cpu * s.reserve[Priority.QUERY]["cpu_frac"]
            res_ram = max_ram * s.reserve[Priority.QUERY]["ram_frac"]

        # If scheduling INTERACTIVE, keep QUERY reserve intact.
        if incoming_prio == Priority.INTERACTIVE:
            res_cpu = max_cpu * (s.reserve[Priority.QUERY]["cpu_frac"] + 0.0)
            res_ram = max_ram * (s.reserve[Priority.QUERY]["ram_frac"] + 0.0)

        return max(0.0, res_cpu), max(0.0, res_ram)

    def _can_fit(pool_id, cpu_need, ram_need, prio):
        avail_cpu, avail_ram, _, _ = _pool_effective_avail(pool_id)
        res_cpu, res_ram = _reserved_headroom(pool_id, prio)
        return (avail_cpu - res_cpu) >= cpu_need and (avail_ram - res_ram) >= ram_need

    def _choose_cpu(pool_id, prio):
        avail_cpu, _, max_cpu, _ = _pool_effective_avail(pool_id)
        cap = max_cpu * s.cpu_cap_frac_of_pool
        # Give queries more CPU to reduce end-to-end latency; keep conservative when pool is tight.
        if prio == Priority.QUERY:
            target = max_cpu * s.cpu_large_frac
        elif prio == Priority.INTERACTIVE:
            target = max_cpu * (0.45)
        else:
            target = max_cpu * (0.35)

        # If little available, take smaller slice to reduce blocking.
        if avail_cpu < max_cpu * 0.35:
            target = max_cpu * s.cpu_small_frac

        cpu = min(cap, max(1.0, min(avail_cpu, target)))
        return cpu

    def _choose_ram(pool_id, p, op, prio):
        # We don't know true min; use learned hint if present; otherwise allocate a conservative slice.
        pool = s.executor.pools[pool_id]
        max_ram = pool.max_ram_pool
        cap = max_ram * s.ram_cap_frac_of_pool

        key = (p.pipeline_id, _op_id(op))
        hint = s.op_ram_hint.get(key, None)

        if hint is not None:
            ram = hint
        else:
            # Conservative default: allocate a fraction of pool RAM by priority.
            if prio == Priority.QUERY:
                ram = max_ram * 0.45
            elif prio == Priority.INTERACTIVE:
                ram = max_ram * 0.35
            else:
                ram = max_ram * 0.25

        # Ensure <= available and within cap; also >= tiny epsilon
        avail_ram = pool.avail_ram_pool
        ram = min(cap, max(0.5, min(avail_ram, ram)))
        return ram

    def _num_running_in_pool(pool_id):
        # Best-effort: infer from results? If executor exposes containers, use it; otherwise skip.
        # We keep it simple: allow up to max_parallel_ops_per_pool assignments per tick.
        return 0

    def _pipeline_score(p):
        # Higher score = schedule sooner.
        prio = p.priority
        base = _base_prio_score(prio)

        # Aging: after waiting long enough, boost score to avoid starvation.
        enq = s.enqueue_ts.get(p.pipeline_id, _now())
        waited = _now() - enq
        aging = 0.0
        if waited > s.aging_start:
            aging = (waited - s.aging_start) * s.aging_slope

        # If pipeline has ready-to-run ops, boost; otherwise keep but lower (prevents spinning).
        ready_ops = _next_assignable_ops(p, limit=1)
        ready_boost = 0.5 if ready_ops else 0.0

        # Penalize pipelines that are currently blocked (no assignable ops) slightly
        return base + aging + ready_boost

    def _pick_victim_for_preempt(pool_id, min_prio_to_preempt):
        # Prefer suspending a batch container; then interactive; never preempt query.
        # Since we may not have direct access to running containers list, we use `results`
        # to build a recent view of active container ids per pool (best-effort).
        # If unavailable, return None and skip preemption.
        candidates = []
        for r in results or []:
            try:
                if r.pool_id != pool_id:
                    continue
                if r.container_id is None:
                    continue
                # Only consider non-failed results as "still running" signal is weak; but it's best-effort.
                pr = getattr(r, "priority", None)
                if pr is None:
                    continue
                if pr == Priority.QUERY:
                    continue
                if min_prio_to_preempt == Priority.INTERACTIVE and pr == Priority.INTERACTIVE:
                    candidates.append((2, r.container_id, pr))
                elif min_prio_to_preempt == Priority.BATCH_PIPELINE:
                    # Preempt batch first, then interactive if needed.
                    rank = 1 if pr == Priority.BATCH_PIPELINE else 2
                    candidates.append((rank, r.container_id, pr))
            except Exception:
                continue
        if not candidates:
            return None
        candidates.sort(key=lambda x: x[0])
        return candidates[0][1]

    # Ingest new pipelines
    for p in pipelines:
        s.waiting_queue.append(p)
        s.pipeline_obj[p.pipeline_id] = p
        if p.pipeline_id not in s.enqueue_ts:
            s.enqueue_ts[p.pipeline_id] = _now()

    # Process results: learn RAM hints on failures (OOM assumed) and requeue pipelines
    # We do NOT drop failed pipelines; we retry with increased RAM to avoid 720s penalties.
    for r in results:
        try:
            if r.failed():
                pid = getattr(r, "pipeline_id", None)
                # If pipeline_id isn't on result, try to infer from op metadata; best effort.
                if pid is None:
                    pid = None
                # Learn RAM hint keyed by operator if possible.
                op = r.ops[0] if getattr(r, "ops", None) else None
                if pid is not None and op is not None:
                    key = (pid, _op_id(op))
                    prev = s.op_ram_hint.get(key, r.ram if getattr(r, "ram", None) else None)
                    if prev is None:
                        prev = 1.0
                    new_hint = prev * s.ram_backoff_mult + s.ram_backoff_add
                    # Cap to a sensible fraction of pool max RAM to avoid pathological oversizing.
                    pool = s.executor.pools[r.pool_id]
                    new_hint = min(new_hint, pool.max_ram_pool * s.ram_cap_frac_of_pool)
                    s.op_ram_hint[key] = new_hint

                # Ensure pipeline remains in queue for retry if we can find it.
                if pid is not None and pid in s.pipeline_obj:
                    # Keep enqueue time unchanged to preserve aging.
                    if s.pipeline_obj[pid] not in s.waiting_queue:
                        s.waiting_queue.append(s.pipeline_obj[pid])
        except Exception:
            # Keep scheduler robust to schema differences
            pass

    # Early exit if nothing changed
    if not pipelines and not results:
        return [], []

    # Remove completed pipelines from queue
    new_q = []
    seen = set()
    for p in s.waiting_queue:
        if p.pipeline_id in seen:
            continue
        seen.add(p.pipeline_id)
        if _is_done_or_hard_failed(p):
            continue
        new_q.append(p)
    s.waiting_queue = new_q

    # Sort pipelines by score (desc); stable tie-breaker by enqueue time
    scored = []
    for p in s.waiting_queue:
        scored.append((-_pipeline_score(p), s.enqueue_ts.get(p.pipeline_id, _now()), p.pipeline_id))
    scored.sort()
    ordered_pipelines = [s.pipeline_obj[pid] for _, _, pid in scored if pid in s.pipeline_obj]

    suspensions = []
    assignments = []

    # Try to schedule high-priority first, but still allow batch progress via aging.
    # We'll iterate pools and pick best-fitting pipeline for each.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool
        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        assigned_here = 0
        while assigned_here < s.max_parallel_ops_per_pool:
            picked = None
            picked_ops = None
            picked_cpu = None
            picked_ram = None

            for p in ordered_pipelines:
                prio = p.priority
                ops = _next_assignable_ops(p, limit=s.max_ops_per_assignment)
                if not ops:
                    continue
                op = ops[0]

                cpu_need = _choose_cpu(pool_id, prio)
                ram_need = _choose_ram(pool_id, p, op, prio)

                if _can_fit(pool_id, cpu_need, ram_need, prio):
                    picked = p
                    picked_ops = ops
                    picked_cpu = cpu_need
                    picked_ram = ram_need
                    break

            if picked is None:
                # If we couldn't fit anything, consider minimal preemption only for QUERY/INTERACTIVE
                # to protect weighted latency objective.
                # Find if there's a QUERY waiting that is ready but cannot fit due to reservations.
                query_candidate = None
                for p in ordered_pipelines:
                    if p.priority != Priority.QUERY:
                        continue
                    ops = _next_assignable_ops(p, limit=1)
                    if not ops:
                        continue
                    op = ops[0]
                    cpu_need = _choose_cpu(pool_id, p.priority)
                    ram_need = _choose_ram(pool_id, p, op, p.priority)
                    # For preemption decision, ignore reservations: we want to make room.
                    if pool.avail_cpu_pool >= cpu_need and pool.avail_ram_pool >= ram_need:
                        continue
                    query_candidate = (p, ops, cpu_need, ram_need)
                    break

                if query_candidate is not None:
                    lastp = s.last_preempt_ts.get(pool_id, -1e9)
                    if (_now() - lastp) >= s.preempt_cooldown_s:
                        victim = _pick_victim_for_preempt(pool_id, min_prio_to_preempt=Priority.BATCH_PIPELINE)
                        if victim is not None:
                            suspensions.append(Suspend(victim, pool_id))
                            s.last_preempt_ts[pool_id] = _now()
                break  # no assignment this pool this tick

            assignment = Assignment(
                ops=picked_ops,
                cpu=picked_cpu,
                ram=picked_ram,
                priority=picked.priority,
                pool_id=pool_id,
                pipeline_id=picked.pipeline_id,
            )
            assignments.append(assignment)
            assigned_here += 1

            # Keep it simple: do not remove pipeline from ordered list; next loop will see updated status next tick.
            # Avoid repeated scheduling same pipeline within same tick if it only has one ready op.
            # We'll break after one assignment per tick per pool to reduce conflicts.
            break

    return suspensions, assignments
