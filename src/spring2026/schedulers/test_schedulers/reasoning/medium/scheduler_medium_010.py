# policy_key: scheduler_medium_010
# reasoning_effort: medium
# exp: reasoning
# model: gpt-5.2-2025-12-11
# llm_cost: 0.091697
# generation_seconds: 74.37
# generated_at: 2026-04-09T22:41:46.840829
@register_scheduler_init(key="scheduler_medium_010")
def scheduler_medium_010_init(s):
    """Priority-aware round-robin with conservative RAM sizing and gentle batch progress.

    Main ideas:
    - Strict priority for admission/order: QUERY > INTERACTIVE > BATCH.
    - Within each priority class, round-robin across pipelines to avoid starvation.
    - Conservative RAM sizing:
        * Try to read operator RAM minimum from common attribute names.
        * On any observed pipeline operator failure, increase a per-pipeline RAM multiplier.
        * Never intentionally schedule an op if estimated RAM-min cannot fit in the pool.
    - Protect high priority by keeping a small "reserve" in each pool that batch cannot consume
      when any higher-priority ready work exists.
    - Ensure batch completion progress via a small token bucket even under constant high-priority load.
    """
    s.tick = 0

    # Per-priority FIFO of pipeline_ids (we round-robin by pop/append).
    s.queues = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }

    # pipeline_id -> meta
    # meta fields: pipeline, priority, enqueue_tick, ram_mult, fail_count, last_fail_seen_tick
    s.meta = {}

    # Batch progress tokens: accrue each tick; spend when scheduling batch while higher-priority ready work exists.
    s.batch_tokens = 0.0
    s.batch_token_rate = 0.25  # tokens per tick
    s.batch_token_cap = 2.0

    # Pool protection: when high-priority ready work exists, batch can only use (1-reserve_frac) of free resources.
    s.reserve_frac_cpu = 0.20
    s.reserve_frac_ram = 0.20

    # Scheduling limits to reduce churn / overly aggressive packing.
    s.max_assignments_per_pool = 4
    s.max_ops_per_pipeline_per_tick = 1

    # RAM backoff tuning on failures
    s.ram_backoff = 1.5
    s.ram_mult_cap = 32.0

    # If we can't infer RAM min for an op, allocate this fraction of pool max RAM (capped by available).
    s.unknown_ram_frac = 0.50


def _get_op_ram_min(op):
    """Best-effort extraction of operator minimum RAM requirement from common attribute names."""
    candidates = [
        "ram_min", "min_ram", "min_memory", "memory_min",
        "ram", "memory", "mem", "mem_min", "peak_ram", "ram_requirement",
        "required_ram", "required_memory",
    ]
    for name in candidates:
        if hasattr(op, name):
            v = getattr(op, name)
            # Accept ints/floats; ignore None/strings/objects.
            if isinstance(v, (int, float)) and v > 0:
                return float(v)
    return None


def _pipeline_has_ready_ops(pipeline):
    st = pipeline.runtime_status()
    ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
    return bool(ops)


def _priority_order():
    return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]


def _cpu_target(pool, priority):
    # Give more CPU to higher priority; keep it simple and stable.
    max_cpu = getattr(pool, "max_cpu_pool", None)
    if not isinstance(max_cpu, (int, float)) or max_cpu <= 0:
        # Fallback if max not available: try to use a small-ish default
        max_cpu = max(1.0, float(getattr(pool, "avail_cpu_pool", 1.0)))

    if priority == Priority.QUERY:
        frac = 0.60
    elif priority == Priority.INTERACTIVE:
        frac = 0.40
    else:
        frac = 0.20

    target = max(1.0, max_cpu * frac)
    return target


def _ram_target(pool, op, ram_mult, priority, unknown_ram_frac):
    max_ram = getattr(pool, "max_ram_pool", None)
    if not isinstance(max_ram, (int, float)) or max_ram <= 0:
        max_ram = max(1.0, float(getattr(pool, "avail_ram_pool", 1.0)))

    ram_min = _get_op_ram_min(op)
    if ram_min is None:
        # Unknown: allocate a conservative chunk to reduce OOM risk.
        base = max_ram * unknown_ram_frac
    else:
        # Small priority bump for tail latency protection (still primarily driven by min requirement).
        if priority == Priority.QUERY:
            prio_bump = 1.10
        elif priority == Priority.INTERACTIVE:
            prio_bump = 1.05
        else:
            prio_bump = 1.00
        base = ram_min * ram_mult * prio_bump

    return float(base)


def _cleanup_finished_and_update_failures(s):
    """Remove successful pipelines; if any operator has FAILED, increase RAM multiplier once per tick."""
    for prio in list(s.queues.keys()):
        new_q = []
        for pid in s.queues[prio]:
            meta = s.meta.get(pid)
            if meta is None:
                continue
            p = meta["pipeline"]
            st = p.runtime_status()

            if st.is_pipeline_successful():
                # Drop completed pipelines.
                s.meta.pop(pid, None)
                continue

            # If any failures appear, bump RAM multiplier once per tick to reduce repeated OOMs.
            has_failures = st.state_counts[OperatorState.FAILED] > 0
            if has_failures and meta.get("last_fail_seen_tick") != s.tick:
                meta["last_fail_seen_tick"] = s.tick
                meta["fail_count"] = int(meta.get("fail_count", 0)) + 1
                meta["ram_mult"] = min(float(meta.get("ram_mult", 1.0)) * s.ram_backoff, s.ram_mult_cap)

            new_q.append(pid)

        s.queues[prio] = new_q


def _has_high_prio_ready_work(s):
    for prio in (Priority.QUERY, Priority.INTERACTIVE):
        for pid in s.queues.get(prio, []):
            meta = s.meta.get(pid)
            if meta is None:
                continue
            if _pipeline_has_ready_ops(meta["pipeline"]):
                return True
    return False


@register_scheduler(key="scheduler_medium_010")
def scheduler_medium_010_scheduler(s, results, pipelines):
    """
    Scheduler step:
    - Incorporate new pipelines into per-priority RR queues.
    - Update pipeline meta on failures/completions.
    - For each pool, repeatedly assign ready operators:
        * Query first, then interactive, then batch.
        * Round-robin across pipelines within same class.
        * Apply pool reserve to keep headroom for high-priority work.
        * Use tokens to allow occasional batch progress even under high-priority pressure.
    """
    s.tick += 1
    s.batch_tokens = min(s.batch_token_cap, s.batch_tokens + s.batch_token_rate)

    # Enqueue new pipelines.
    for p in pipelines:
        pid = p.pipeline_id
        if pid in s.meta:
            continue
        prio = p.priority
        s.meta[pid] = {
            "pipeline": p,
            "priority": prio,
            "enqueue_tick": s.tick,
            "ram_mult": 1.0,
            "fail_count": 0,
            "last_fail_seen_tick": None,
        }
        if prio not in s.queues:
            # Defensive: unknown priority goes to batch-like queue.
            prio = Priority.BATCH_PIPELINE
            s.meta[pid]["priority"] = prio
        s.queues[prio].append(pid)

    # Early exit if nothing changed.
    if not pipelines and not results:
        return [], []

    # Update failures/completions by inspecting pipeline runtime status (robust even without result->pipeline mapping).
    _cleanup_finished_and_update_failures(s)

    suspensions = []
    assignments = []

    high_waiting = _has_high_prio_ready_work(s)

    # Track per-tick pipeline assignment count (avoid over-scheduling a single pipeline).
    per_pipeline_assigned = {}

    # Iterate pools and pack a few assignments per pool.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)
        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # When high-priority ready work exists, keep a reserve that batch cannot consume.
        cpu_reserve = (float(pool.max_cpu_pool) * s.reserve_frac_cpu) if high_waiting else 0.0
        ram_reserve = (float(pool.max_ram_pool) * s.reserve_frac_ram) if high_waiting else 0.0

        # Schedule up to N assignments per pool per tick.
        for _ in range(s.max_assignments_per_pool):
            if avail_cpu <= 0 or avail_ram <= 0:
                break

            # Select the next eligible pipeline in strict priority order.
            chosen_pid = None
            chosen_prio = None
            chosen_op = None
            must_spend_batch_token = False

            # We iterate in priority order; within each prio we RR by pop(0)/append.
            for prio in _priority_order():
                q = s.queues.get(prio, [])
                if not q:
                    continue

                # Batch gating when higher-priority ready work exists.
                if prio == Priority.BATCH_PIPELINE and high_waiting:
                    if s.batch_tokens < 1.0:
                        continue
                    must_spend_batch_token = True

                # Pool reserve: for batch under high pressure, restrict usable resources.
                eff_avail_cpu = avail_cpu
                eff_avail_ram = avail_ram
                if prio == Priority.BATCH_PIPELINE and high_waiting:
                    eff_avail_cpu = max(0.0, avail_cpu - cpu_reserve)
                    eff_avail_ram = max(0.0, avail_ram - ram_reserve)
                    if eff_avail_cpu < 1.0 or eff_avail_ram <= 0.0:
                        continue

                # RR scan: try up to len(q) pipelines to find one with ready ops and within per-tick cap.
                scanned = 0
                qlen = len(q)
                while scanned < qlen:
                    pid = q.pop(0)
                    q.append(pid)
                    scanned += 1

                    meta = s.meta.get(pid)
                    if meta is None:
                        continue
                    p = meta["pipeline"]

                    # Per-pipeline per-tick cap.
                    if per_pipeline_assigned.get(pid, 0) >= s.max_ops_per_pipeline_per_tick:
                        continue

                    st = p.runtime_status()
                    if st.is_pipeline_successful():
                        # Clean up lazily.
                        s.meta.pop(pid, None)
                        continue

                    op_list = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
                    if not op_list:
                        continue

                    op = op_list[0]

                    # Compute resource targets.
                    ram_mult = float(meta.get("ram_mult", 1.0))
                    target_ram = _ram_target(pool, op, ram_mult, prio, s.unknown_ram_frac)

                    # If we can infer a minimum and it's above effective availability, skip to avoid OOM thrash.
                    op_ram_min = _get_op_ram_min(op)
                    if op_ram_min is not None and eff_avail_ram < op_ram_min:
                        continue

                    # Final RAM is capped by effective availability.
                    assign_ram = min(eff_avail_ram, target_ram)
                    if assign_ram <= 0:
                        continue

                    target_cpu = _cpu_target(pool, prio)
                    assign_cpu = min(eff_avail_cpu, target_cpu)
                    if assign_cpu < 1.0:
                        continue

                    chosen_pid = pid
                    chosen_prio = prio
                    chosen_op = op
                    break

                if chosen_pid is not None:
                    break

            if chosen_pid is None:
                break

            # Spend batch token if needed.
            if must_spend_batch_token and chosen_prio == Priority.BATCH_PIPELINE and high_waiting:
                if s.batch_tokens >= 1.0:
                    s.batch_tokens -= 1.0
                else:
                    # Shouldn't happen due to gating; just stop scheduling batch.
                    break

            # Recompute with actual current pool availability (avoid over-alloc after multiple assignments).
            # Apply same reserve rule for batch.
            if chosen_prio == Priority.BATCH_PIPELINE and high_waiting:
                eff_avail_cpu = max(0.0, avail_cpu - cpu_reserve)
                eff_avail_ram = max(0.0, avail_ram - ram_reserve)
            else:
                eff_avail_cpu = avail_cpu
                eff_avail_ram = avail_ram

            if eff_avail_cpu < 1.0 or eff_avail_ram <= 0.0:
                break

            meta = s.meta.get(chosen_pid)
            if meta is None:
                continue

            # Finalize sizes.
            target_cpu = _cpu_target(pool, chosen_prio)
            assign_cpu = min(eff_avail_cpu, target_cpu)

            ram_mult = float(meta.get("ram_mult", 1.0))
            target_ram = _ram_target(pool, chosen_op, ram_mult, chosen_prio, s.unknown_ram_frac)
            assign_ram = min(eff_avail_ram, target_ram)

            # Defensive minimums.
            if assign_cpu < 1.0 or assign_ram <= 0.0:
                break

            assignment = Assignment(
                ops=[chosen_op],
                cpu=assign_cpu,
                ram=assign_ram,
                priority=chosen_prio,
                pool_id=pool_id,
                pipeline_id=chosen_pid,
            )
            assignments.append(assignment)

            # Account for resources consumed this tick.
            avail_cpu -= float(assign_cpu)
            avail_ram -= float(assign_ram)
            per_pipeline_assigned[chosen_pid] = per_pipeline_assigned.get(chosen_pid, 0) + 1

    return suspensions, assignments
