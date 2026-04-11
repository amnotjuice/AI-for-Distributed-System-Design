# policy_key: scheduler_none_046
# reasoning_effort: none
# model: gpt-5.2-2025-12-11
# llm_cost: 0.064901
# generation_seconds: 56.29
# generated_at: 2026-04-09T22:21:17.814523
@register_scheduler_init(key="scheduler_none_046")
def scheduler_none_046_init(s):
    """Priority-aware, OOM-adaptive, low-churn scheduler.

    Goals aligned to weighted latency objective:
    - Strongly protect QUERY and INTERACTIVE latency via (a) reserved headroom and (b) selective preemption.
    - Reduce 720s penalties by aggressively learning per-op RAM needs on OOM and retrying quickly.
    - Avoid starvation by aging batch pipelines gradually into higher effective priority.
    - Keep churn low: preempt only when a high-priority op is ready and cannot fit otherwise.

    High-level behavior:
    - Maintain per-priority FIFO queues of pipelines.
    - On each tick, ingest arrivals, process results (record OOM learning + clear inflight markers),
      then schedule across pools with reserved capacity and a small number of assignments per pool.
    """
    # Per-priority queues (FIFO)
    s.wait_q = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }

    # Arrival time for aging/fairness
    s.arrival_ts = {}  # pipeline_id -> first_seen_time (ticks)

    # Track pipelines currently having an op assigned/running to avoid oversubscribing a single pipeline
    s.inflight_pipelines = set()  # pipeline_id

    # Learned RAM requirement multipliers (per operator object) from OOMs
    # Keyed by operator identity (id(op)) because operators can be objects in the DAG.
    s.op_ram_mult = {}  # id(op) -> multiplier (float), starts at 1.0

    # Remember last assigned sizing to help on retries
    s.op_last_ram = {}  # (pipeline_id, id(op)) -> ram
    s.op_last_cpu = {}  # (pipeline_id, id(op)) -> cpu

    # Track "current time" in scheduler ticks (deterministic step count)
    s.ticks = 0

    # Policy knobs
    s.max_assignments_per_pool_per_tick = 2  # small to reduce churn and keep interactive responsive
    s.max_assignments_total_per_tick = 6     # cap global scheduling work

    # Headroom reservation: keep some fraction for high priority to reduce tail latency
    s.reserve_frac = {
        Priority.QUERY: (0.15, 0.15),         # (cpu, ram)
        Priority.INTERACTIVE: (0.10, 0.10),
        Priority.BATCH_PIPELINE: (0.00, 0.00),
    }

    # Aging: after this many ticks, batch gets treated closer to interactive to avoid 720s penalties
    s.batch_age_boost_start = 60
    s.batch_age_boost_full = 240  # by this tick age, batch reaches near interactive effective score

    # Preemption controls
    s.preempt_enabled = True
    s.preempt_min_gain_cpu_frac = 0.10  # require meaningful gain to preempt
    s.preempt_min_gain_ram_frac = 0.10

    # Track observed running containers by pool so we can preempt low-priority when needed
    # We update this from results only (best-effort). If simulator doesn't provide running-state results,
    # preemption will naturally become a no-op.
    s.running_by_pool = {}  # pool_id -> list of dict(container_id, priority, cpu, ram)


def _priority_rank(p):
    # Lower is higher priority for sorting
    if p == Priority.QUERY:
        return 0
    if p == Priority.INTERACTIVE:
        return 1
    return 2


def _effective_priority(s, pipeline):
    # Aging for batch to prevent starvation and heavy penalties.
    p = pipeline.priority
    if p != Priority.BATCH_PIPELINE:
        return p

    pid = pipeline.pipeline_id
    first = s.arrival_ts.get(pid, s.ticks)
    age = max(0, s.ticks - first)
    if age < s.batch_age_boost_start:
        return Priority.BATCH_PIPELINE

    # Once old enough, treat as INTERACTIVE for scheduling order,
    # but only for selection order (does not change Assignment.priority field).
    if age >= s.batch_age_boost_full:
        return Priority.INTERACTIVE

    # Partial boost: still batch, but will be ranked between interactive and batch
    # via fractional rank using a custom comparator in selection.
    return Priority.BATCH_PIPELINE


def _batch_boost_factor(s, pipeline):
    # Returns 0..1 where 1 means "treat like interactive" in ordering.
    if pipeline.priority != Priority.BATCH_PIPELINE:
        return 0.0
    pid = pipeline.pipeline_id
    first = s.arrival_ts.get(pid, s.ticks)
    age = max(0, s.ticks - first)
    if age <= s.batch_age_boost_start:
        return 0.0
    if age >= s.batch_age_boost_full:
        return 1.0
    return float(age - s.batch_age_boost_start) / float(s.batch_age_boost_full - s.batch_age_boost_start)


def _pipeline_is_done_or_dead(status):
    # "Dead" means has any FAILED op; we still may want retries depending on simulator semantics.
    # Here, we do NOT drop failed pipelines automatically; we allow retries via ASSIGNABLE_STATES.
    return status.is_pipeline_successful()


def _get_next_ready_op(status):
    # Prefer parent-complete ops only; schedule at most one op per pipeline concurrently.
    op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
    if op_list:
        return op_list[0]
    return None


def _ram_guess_for_op(s, pipeline, op, pool_ram_cap):
    # Heuristic RAM sizing:
    # - Start with a small fraction of pool RAM to limit waste and allow more concurrency.
    # - Multiply up based on learned OOM multiplier.
    # - Cap to pool max RAM.
    mult = s.op_ram_mult.get(id(op), 1.0)
    base = max(1.0, 0.25 * pool_ram_cap)  # conservative baseline
    ram = base * mult
    # If we have a last known attempt, bias upward slightly to reduce repeat OOM loops
    key = (pipeline.pipeline_id, id(op))
    if key in s.op_last_ram:
        ram = max(ram, s.op_last_ram[key] * 1.15)
    # Hard cap
    return min(ram, pool_ram_cap)


def _cpu_guess_for_op(s, pipeline, op, pool_cpu_cap):
    # Heuristic CPU sizing:
    # - Give more CPU to higher priority, but keep it bounded to avoid monopolization.
    # - Because scaling is sublinear, aim for "enough" not "max".
    p = pipeline.priority
    if p == Priority.QUERY:
        frac = 0.70
    elif p == Priority.INTERACTIVE:
        frac = 0.55
    else:
        frac = 0.35
    cpu = max(1.0, frac * pool_cpu_cap)
    key = (pipeline.pipeline_id, id(op))
    if key in s.op_last_cpu:
        # If previously assigned, keep similar to reduce variability.
        cpu = max(1.0, min(pool_cpu_cap, 0.5 * cpu + 0.5 * s.op_last_cpu[key]))
    return min(cpu, pool_cpu_cap)


def _can_fit(avail_cpu, avail_ram, cpu_need, ram_need):
    return (cpu_need <= avail_cpu) and (ram_need <= avail_ram)


def _select_candidate_pipelines(s):
    # Produce an ordered list of pipelines to consider for scheduling:
    # QUERY first, INTERACTIVE second, then BATCH; but with aging boost for BATCH.
    # Maintain FIFO within each base class.
    # We do a lightweight merge by repeatedly picking the best head-of-queue.
    candidates = []

    # Work on shallow copies of queues by index pointers (don't mutate here).
    idx = {Priority.QUERY: 0, Priority.INTERACTIVE: 0, Priority.BATCH_PIPELINE: 0}
    q = s.wait_q

    while True:
        heads = []
        for pr in (Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE):
            if idx[pr] < len(q[pr]):
                heads.append(q[pr][idx[pr]])
        if not heads:
            break

        # Compute a composite order key
        def key_fn(pipeline):
            pr = pipeline.priority
            base_rank = _priority_rank(pr)
            if pr == Priority.BATCH_PIPELINE:
                boost = _batch_boost_factor(s, pipeline)
                # Move batch closer to interactive: base_rank 2 -> 1 + (1-boost)
                # boost=0 => 2.0, boost=1 => 1.0
                eff = 1.0 + (1.0 - boost)
            else:
                eff = float(base_rank)
            # Older first within same effective priority to reduce tail and failure penalties
            pid = pipeline.pipeline_id
            age = s.ticks - s.arrival_ts.get(pid, s.ticks)
            return (eff, -age)

        best = min(heads, key=key_fn)
        candidates.append(best)
        idx[best.priority] += 1

        # Avoid unbounded work per tick; we only need a limited view
        if len(candidates) >= 64:
            break

    return candidates


def _rebuild_queues_from_candidates(s, candidates, keep_set):
    # keep_set contains pipeline_ids to keep queued; others have been popped/removed.
    newq = {Priority.QUERY: [], Priority.INTERACTIVE: [], Priority.BATCH_PIPELINE: []}
    seen = set()
    for p in candidates:
        if p.pipeline_id in keep_set and p.pipeline_id not in seen:
            newq[p.priority].append(p)
            seen.add(p.pipeline_id)
    # There may be pipelines not present in candidates due to the 64 cap; append them preserving order
    for pr in (Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE):
        for p in s.wait_q[pr]:
            if p.pipeline_id in keep_set and p.pipeline_id not in seen:
                newq[pr].append(p)
                seen.add(p.pipeline_id)
    s.wait_q = newq


def _reserve_available(s, pool, prio):
    # Return (cpu_avail_effective, ram_avail_effective) after reserving headroom for high priorities.
    cpu = pool.avail_cpu_pool
    ram = pool.avail_ram_pool
    r_cpu, r_ram = s.reserve_frac.get(prio, (0.0, 0.0))
    cpu_eff = max(0.0, cpu - r_cpu * pool.max_cpu_pool)
    ram_eff = max(0.0, ram - r_ram * pool.max_ram_pool)
    return cpu_eff, ram_eff


def _try_preempt_for(s, pool_id, need_cpu, need_ram):
    # Best-effort: preempt one low-priority container to make room.
    # Preempt order: BATCH > INTERACTIVE; never preempt QUERY.
    running = s.running_by_pool.get(pool_id, [])
    if not running:
        return []

    # Sort by priority (lowest importance first) then by largest resource footprint for effectiveness.
    def prio_value(pr):
        if pr == Priority.BATCH_PIPELINE:
            return 2
        if pr == Priority.INTERACTIVE:
            return 1
        return 0

    victims = sorted(
        [c for c in running if c.get("priority") != Priority.QUERY],
        key=lambda c: (prio_value(c.get("priority")), -(c.get("cpu", 0.0) + c.get("ram", 0.0))),
        reverse=True,
    )
    if not victims:
        return []

    victim = victims[0]
    # Require that preempting makes a meaningful dent in the deficit
    if victim.get("cpu", 0.0) < need_cpu * s.preempt_min_gain_cpu_frac and victim.get("ram", 0.0) < need_ram * s.preempt_min_gain_ram_frac:
        return []

    return [Suspend(victim["container_id"], pool_id)]


@register_scheduler(key="scheduler_none_046")
def scheduler_none_046(s, results, pipelines):
    """
    Scheduler step:
    - Update state from new arrivals and execution results (learn OOM, clear inflight, update running list).
    - Attempt to assign ready ops across pools, prioritizing QUERY/INTERACTIVE, then aged BATCH.
    - Apply headroom reservation and selective preemption to protect high-priority latency.
    """
    s.ticks += 1

    # Ingest new pipelines
    for p in pipelines:
        if p.pipeline_id not in s.arrival_ts:
            s.arrival_ts[p.pipeline_id] = s.ticks
        s.wait_q[p.priority].append(p)

    # Process results: learn OOMs, clear inflight markers, maintain approximate running list for preemption.
    if results:
        # Reset running list each tick; we only know about containers through results.
        # If simulator emits periodic RUNNING results, this will track; otherwise preemption becomes conservative.
        s.running_by_pool = {}

        for r in results:
            # Clear inflight: any result indicates container finished (success or fail) for those ops.
            # We mark the pipeline as no longer inflight so it can be scheduled again.
            # NOTE: r.ops may include ops from one pipeline in this simulator.
            pid = getattr(r, "pipeline_id", None)  # might not exist
            if pid is not None:
                if pid in s.inflight_pipelines:
                    s.inflight_pipelines.discard(pid)

            # Track running containers (if simulator uses such results); otherwise keep best-effort.
            # We add container info regardless; if a container just finished, preemption won't matter.
            pool_id = r.pool_id
            if pool_id not in s.running_by_pool:
                s.running_by_pool[pool_id] = []
            s.running_by_pool[pool_id].append({
                "container_id": r.container_id,
                "priority": r.priority,
                "cpu": getattr(r, "cpu", 0.0),
                "ram": getattr(r, "ram", 0.0),
            })

            # Learn from failures
            if r.failed():
                # Treat any error as potential OOM signal; if error is structured, prefer OOM classification.
                err = str(r.error) if getattr(r, "error", None) is not None else ""
                is_oom = ("oom" in err.lower()) or ("out of memory" in err.lower()) or ("memory" in err.lower())

                if is_oom:
                    # Increase learned RAM multiplier for the involved ops.
                    # If r.ops not provided, nothing to learn.
                    for op in getattr(r, "ops", []) or []:
                        k = id(op)
                        prev = s.op_ram_mult.get(k, 1.0)
                        # Exponential backoff-ish: multiply by 1.6, cap to avoid runaway
                        s.op_ram_mult[k] = min(prev * 1.6, 16.0)

    # Early exit: no new info
    if not pipelines and not results and all(not s.wait_q[pr] for pr in s.wait_q):
        return [], []

    suspensions = []
    assignments = []
    assignments_budget = s.max_assignments_total_per_tick

    # Build candidate ordering once for this tick (approximate global order)
    candidates = _select_candidate_pipelines(s)
    keep_ids = set(p.pipeline_id for p in candidates)
    # We'll pop from a working list; later rebuild queues.
    worklist = candidates[:]

    for pool_id in range(s.executor.num_pools):
        if assignments_budget <= 0:
            break

        pool = s.executor.pools[pool_id]
        if pool.avail_cpu_pool <= 0 or pool.avail_ram_pool <= 0:
            continue

        per_pool_budget = s.max_assignments_per_pool_per_tick

        # We'll iterate over worklist and try to schedule some ops that fit.
        i = 0
        while i < len(worklist) and per_pool_budget > 0 and assignments_budget > 0:
            pipeline = worklist[i]
            status = pipeline.runtime_status()

            # Drop completed pipelines from queues
            if _pipeline_is_done_or_dead(status):
                keep_ids.discard(pipeline.pipeline_id)
                worklist.pop(i)
                continue

            # Avoid multiple concurrent ops per pipeline (reduces resource spikes and failure risk)
            if pipeline.pipeline_id in s.inflight_pipelines:
                i += 1
                continue

            op = _get_next_ready_op(status)
            if op is None:
                i += 1
                continue

            # Compute resource guesses
            cpu_need = _cpu_guess_for_op(s, pipeline, op, pool.max_cpu_pool)
            ram_need = _ram_guess_for_op(s, pipeline, op, pool.max_ram_pool)

            # Apply reservation (for the pipeline's priority) to protect tail latency
            cpu_eff, ram_eff = _reserve_available(s, pool, pipeline.priority)

            if not _can_fit(cpu_eff, ram_eff, cpu_need, ram_need):
                # If high priority, attempt a single preemption to make room
                if s.preempt_enabled and pipeline.priority in (Priority.QUERY, Priority.INTERACTIVE):
                    # Deficit relative to effective availability
                    need_cpu = max(0.0, cpu_need - cpu_eff)
                    need_ram = max(0.0, ram_need - ram_eff)
                    susp = _try_preempt_for(s, pool_id, need_cpu, need_ram)
                    if susp:
                        suspensions.extend(susp)
                        # After preempting, don't immediately assume resources; wait next tick
                        i += 1
                        continue

                # Can't fit here, try next pipeline or pool
                i += 1
                continue

            # Clamp to actual available (not just effective)
            cpu_assign = min(cpu_need, pool.avail_cpu_pool)
            ram_assign = min(ram_need, pool.avail_ram_pool)

            assignment = Assignment(
                ops=[op],
                cpu=cpu_assign,
                ram=ram_assign,
                priority=pipeline.priority,
                pool_id=pool_id,
                pipeline_id=pipeline.pipeline_id,
            )
            assignments.append(assignment)

            # Remember last attempt sizing for this op (for smoother retries)
            key = (pipeline.pipeline_id, id(op))
            s.op_last_ram[key] = ram_assign
            s.op_last_cpu[key] = cpu_assign

            # Mark inflight
            s.inflight_pipelines.add(pipeline.pipeline_id)

            # Update budgets and move on; keep pipeline in queue for subsequent ops
            per_pool_budget -= 1
            assignments_budget -= 1
            i += 1

        # end while worklist

    # Rebuild queues (remove completed only; keep others)
    _rebuild_queues_from_candidates(s, candidates, keep_ids)

    return suspensions, assignments
