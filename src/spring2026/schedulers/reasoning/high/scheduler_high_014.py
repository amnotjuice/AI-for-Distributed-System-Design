# policy_key: scheduler_high_014
# reasoning_effort: high
# exp: reasoning
# model: gpt-5.2-2025-12-11
# llm_cost: 0.192805
# generation_seconds: 238.17
# generated_at: 2026-04-10T00:57:46.336181
@register_scheduler_init(key="scheduler_high_014")
def scheduler_high_014_init(s):
    """
    Priority-aware, retry-safe scheduler focused on minimizing weighted end-to-end latency
    while avoiding pipeline failures/incompletes.

    Core ideas:
      - Strict priority order: QUERY > INTERACTIVE > BATCH (with light batch "credit" to avoid starvation).
      - Conservative-but-not-tiny initial RAM (per priority) to reduce OOM-induced failures.
      - OOM-aware retries: on OOM, increase RAM exponentially for that operator; apply a mild pipeline-level
        RAM multiplier after OOMs to reduce repeated failures later in the same pipeline.
      - Cooldown after repeated failures to avoid tight failure loops that waste time/resources.
      - Pool preference: reserve pool 0 (if present) primarily for QUERY/INTERACTIVE; push BATCH to other pools.
    """
    # Queues store pipeline_id (lazy cleanup).
    s.queues = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }

    # Optional "urgent" queue for retrying recently-failed operators sooner.
    s.urgent = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }
    s.urgent_set = {
        Priority.QUERY: set(),
        Priority.INTERACTIVE: set(),
        Priority.BATCH_PIPELINE: set(),
    }

    # Pipeline registry.
    s.pipeline_by_id = {}
    s.enqueue_tick = {}
    s.last_scheduled_tick = {}

    # Operator tuning keyed by (pipeline_id, op_id/name/id(op)).
    # Fields: ram, cpu, retries_oom, retries_other, last_fail_tick, cooldown_until
    s.op_tuning = {}

    # Pipeline-level RAM multiplier (increases after OOMs).
    s.pipeline_ram_factor = {}

    # Scheduler tick counter (logical time, used only for aging/cooldowns).
    s.tick = 0

    # Batch anti-starvation credit (spent to occasionally schedule batch ahead of interactive when no queries).
    s.batch_credit = 0.0

    # Tuning knobs.
    s.scan_limit_per_pick = 64
    s.aging_div = 50  # smaller -> faster aging

    s.max_oom_retries = {
        Priority.QUERY: 8,
        Priority.INTERACTIVE: 7,
        Priority.BATCH_PIPELINE: 6,
    }
    s.max_other_retries = {
        Priority.QUERY: 2,
        Priority.INTERACTIVE: 2,
        Priority.BATCH_PIPELINE: 1,
    }

    # Failure cooldowns (ticks) to prevent tight fail loops.
    s.cooldown_ticks = {
        Priority.QUERY: 1,
        Priority.INTERACTIVE: 2,
        Priority.BATCH_PIPELINE: 4,
    }


def _is_oom_error(err):
    if not err:
        return False
    e = str(err).lower()
    return ("oom" in e) or ("out of memory" in e) or ("memory" in e and "alloc" in e)


def _infer_pipeline_id_from_op(op):
    pid = getattr(op, "pipeline_id", None)
    if pid is not None:
        return pid
    p = getattr(op, "pipeline", None)
    if p is not None:
        return getattr(p, "pipeline_id", None)
    return None


def _op_key(pipeline_id, op):
    # Prefer stable identifiers if they exist; otherwise fall back to object identity.
    op_id = getattr(op, "op_id", None)
    if op_id is None:
        op_id = getattr(op, "operator_id", None)
    if op_id is None:
        op_id = getattr(op, "name", None)
    if op_id is None:
        op_id = id(op)
    return (pipeline_id, op_id)


def _cleanup_pipeline_state(s, pipeline_id):
    s.pipeline_by_id.pop(pipeline_id, None)
    s.enqueue_tick.pop(pipeline_id, None)
    s.last_scheduled_tick.pop(pipeline_id, None)
    s.pipeline_ram_factor.pop(pipeline_id, None)

    # Remove from urgent sets (lazy removal from lists).
    for pr in (Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE):
        if pipeline_id in s.urgent_set[pr]:
            s.urgent_set[pr].discard(pipeline_id)


def _mark_urgent(s, pipeline_id, priority):
    if pipeline_id is None:
        return
    if pipeline_id not in s.pipeline_by_id:
        return
    if pipeline_id in s.urgent_set[priority]:
        return
    s.urgent_set[priority].add(pipeline_id)
    s.urgent[priority].append(pipeline_id)


def _initial_ram(pool, priority):
    # Start reasonably sized to reduce OOM churn (OOM -> heavy penalties via incompletes).
    # Fractions are conservative enough to allow concurrency but large enough to avoid obvious OOMs.
    max_ram = pool.max_ram_pool
    if priority == Priority.QUERY:
        frac = 0.45
        floor_frac = 0.15
    elif priority == Priority.INTERACTIVE:
        frac = 0.35
        floor_frac = 0.12
    else:
        frac = 0.25
        floor_frac = 0.10
    ram = max(max_ram * floor_frac, max_ram * frac)
    # Ensure strictly positive.
    return max(1e-9, ram)


def _initial_cpu(pool, priority):
    max_cpu = pool.max_cpu_pool
    # Cap absolute CPU per op to prevent single-op monopolization.
    if priority == Priority.QUERY:
        frac = 0.60
        cap = 8.0
        floor = 1.0
    elif priority == Priority.INTERACTIVE:
        frac = 0.45
        cap = 6.0
        floor = 1.0
    else:
        frac = 0.35
        cap = 4.0
        floor = 1.0
    cpu = max(max_cpu * frac, floor)
    return min(cpu, cap)


def _choose_ready_op_with_cooldown(s, pipeline_id, ops):
    # Pick the first assignable op that is not in cooldown.
    for op in ops:
        opk = _op_key(pipeline_id, op)
        ent = s.op_tuning.get(opk)
        if not ent:
            return op
        if ent.get("cooldown_until", 0) <= s.tick:
            return op
    return None


def _get_desired_resources(s, pool, pipeline, op):
    pid = pipeline.pipeline_id
    pr = pipeline.priority
    opk = _op_key(pid, op)

    base_ram = _initial_ram(pool, pr)
    base_cpu = _initial_cpu(pool, pr)

    factor = s.pipeline_ram_factor.get(pid, 1.0)
    base_ram *= factor

    ent = s.op_tuning.get(opk)
    if ent:
        # Use max() so pipeline-level factor can still lift RAM after earlier OOMs.
        desired_ram = max(ent.get("ram", base_ram), base_ram)
        desired_cpu = ent.get("cpu", base_cpu)
    else:
        desired_ram = base_ram
        desired_cpu = base_cpu

    # Hard caps to pool maxima.
    desired_ram = min(desired_ram, pool.max_ram_pool)
    desired_cpu = min(desired_cpu, pool.max_cpu_pool)

    # Ensure positive.
    desired_ram = max(1e-9, desired_ram)
    desired_cpu = max(1.0, desired_cpu)

    return desired_cpu, desired_ram


def _pick_candidate_from_urgent(s, priority):
    # Urgent is best-effort; pop until we find a pipeline with a ready (assignable) op.
    q = s.urgent[priority]
    while q:
        pid = q.pop(0)
        s.urgent_set[priority].discard(pid)

        p = s.pipeline_by_id.get(pid)
        if p is None:
            continue

        st = p.runtime_status()
        if st.is_pipeline_successful():
            _cleanup_pipeline_state(s, pid)
            continue

        ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        if not ops:
            continue

        op = _choose_ready_op_with_cooldown(s, pid, ops)
        if op is None:
            continue
        return p, op

    return None, None


def _pick_candidate_from_normal(s, priority):
    # Round-robin scan of normal queue for a ready op.
    q = s.queues[priority]
    n = min(len(q), s.scan_limit_per_pick)
    for _ in range(n):
        pid = q.pop(0)
        q.append(pid)

        p = s.pipeline_by_id.get(pid)
        if p is None:
            continue

        st = p.runtime_status()
        if st.is_pipeline_successful():
            _cleanup_pipeline_state(s, pid)
            continue

        ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        if not ops:
            continue

        op = _choose_ready_op_with_cooldown(s, pid, ops)
        if op is None:
            continue
        return p, op

    return None, None


def _pick_next_candidate(s, allow_batch_ahead_of_interactive=False):
    # First try urgent in priority order.
    p, op = _pick_candidate_from_urgent(s, Priority.QUERY)
    if p:
        return p, op
    p, op = _pick_candidate_from_urgent(s, Priority.INTERACTIVE)
    if p:
        return p, op
    if allow_batch_ahead_of_interactive:
        p, op = _pick_candidate_from_urgent(s, Priority.BATCH_PIPELINE)
        if p:
            return p, op

    # Then normal queues, with optional batch ahead of interactive.
    p, op = _pick_candidate_from_normal(s, Priority.QUERY)
    if p:
        return p, op
    if allow_batch_ahead_of_interactive:
        p, op = _pick_candidate_from_normal(s, Priority.BATCH_PIPELINE)
        if p:
            return p, op
    p, op = _pick_candidate_from_normal(s, Priority.INTERACTIVE)
    if p:
        return p, op
    p, op = _pick_candidate_from_normal(s, Priority.BATCH_PIPELINE)
    if p:
        return p, op

    return None, None


@register_scheduler(key="scheduler_high_014")
def scheduler_high_014(s, results, pipelines):
    """
    Policy step:
      - Ingest new pipelines.
      - Update retry tuning based on execution results (OOM => raise RAM aggressively).
      - Fill pools with assignments:
          * Prefer QUERY/INTERACTIVE on pool 0 (if present).
          * Use other pools for BATCH to prevent interference.
          * Occasionally allow a BATCH op ahead of INTERACTIVE when no queries are waiting
            (controlled by a small token/credit bucket) to avoid end-of-sim incompletes.
    """
    s.tick += 1

    suspensions = []
    assignments = []

    # 1) Ingest new pipelines.
    for p in pipelines:
        pid = p.pipeline_id
        s.pipeline_by_id[pid] = p
        s.enqueue_tick.setdefault(pid, s.tick)
        s.last_scheduled_tick.setdefault(pid, -1)
        s.pipeline_ram_factor.setdefault(pid, 1.0)
        s.queues[p.priority].append(pid)

    # 2) Update batch credit (anti-starvation).
    #    If queries are absent, accumulate credit faster so some batch progress happens.
    has_query_waiting = len(s.queues[Priority.QUERY]) > 0
    has_batch_waiting = len(s.queues[Priority.BATCH_PIPELINE]) > 0
    if has_batch_waiting:
        s.batch_credit += 0.25 if not has_query_waiting else 0.05
    else:
        # Decay slowly so credit doesn't persist forever.
        s.batch_credit *= 0.90
    if s.batch_credit > 2.0:
        s.batch_credit = 2.0

    # 3) Process results to adjust per-operator resource requests.
    for r in results:
        ops = getattr(r, "ops", None) or []
        failed = r.failed()
        pool = s.executor.pools[r.pool_id]

        for op in ops:
            pid = _infer_pipeline_id_from_op(op)
            # pid can be None; still tune operator with pid=None to match later only if inferred likewise.
            # Prefer using actual known pid where possible.
            if pid is None or pid not in s.pipeline_by_id:
                # Best effort: if op lacks pid, keep tuning keyed by None; may not match later.
                pid_for_key = pid
            else:
                pid_for_key = pid

            opk = _op_key(pid_for_key, op)
            ent = s.op_tuning.get(opk, {
                "ram": _initial_ram(pool, r.priority),
                "cpu": _initial_cpu(pool, r.priority),
                "retries_oom": 0,
                "retries_other": 0,
                "last_fail_tick": -1,
                "cooldown_until": 0,
            })

            if failed:
                is_oom = _is_oom_error(getattr(r, "error", None))

                # Update retry counts.
                if is_oom:
                    ent["retries_oom"] = ent.get("retries_oom", 0) + 1
                else:
                    ent["retries_other"] = ent.get("retries_other", 0) + 1

                ent["last_fail_tick"] = s.tick
                ent["cooldown_until"] = s.tick + s.cooldown_ticks.get(r.priority, 2)

                # RAM escalation: OOM => exponential growth; otherwise mild growth.
                prev_ram = float(getattr(r, "ram", None) or ent.get("ram") or _initial_ram(pool, r.priority))
                prev_cpu = float(getattr(r, "cpu", None) or ent.get("cpu") or _initial_cpu(pool, r.priority))

                if is_oom:
                    # Increase aggressively; last-ditch attempt approaches pool max.
                    oom_tries = ent["retries_oom"]
                    cap = s.max_oom_retries.get(r.priority, 6)
                    if oom_tries >= cap:
                        new_ram = pool.max_ram_pool
                    else:
                        mult = 2.0 if r.priority != Priority.QUERY else 2.2
                        new_ram = min(pool.max_ram_pool, max(prev_ram * mult, prev_ram + 0.10 * pool.max_ram_pool))
                    new_cpu = prev_cpu  # usually memory-bound failure; keep CPU steady.
                else:
                    # Non-OOM failure: slightly increase both; avoid runaway.
                    other_tries = ent["retries_other"]
                    cap_other = s.max_other_retries.get(r.priority, 1)
                    if other_tries >= cap_other:
                        # Try one stronger attempt but don't lock the pool max.
                        new_ram = min(pool.max_ram_pool, max(prev_ram, 0.60 * pool.max_ram_pool))
                        new_cpu = min(pool.max_cpu_pool, max(prev_cpu, 0.60 * pool.max_cpu_pool))
                    else:
                        new_ram = min(pool.max_ram_pool, max(prev_ram * 1.25, prev_ram + 0.05 * pool.max_ram_pool))
                        new_cpu = min(pool.max_cpu_pool, max(prev_cpu * 1.10, prev_cpu + 1.0))

                ent["ram"] = float(new_ram)
                ent["cpu"] = float(new_cpu)

                # Pipeline-level RAM factor to reduce repeated OOMs across operators.
                if is_oom and pid is not None and pid in s.pipeline_by_id:
                    bump = 1.35 if r.priority == Priority.QUERY else (1.25 if r.priority == Priority.INTERACTIVE else 1.15)
                    s.pipeline_ram_factor[pid] = min(2.5, s.pipeline_ram_factor.get(pid, 1.0) * bump)
                    _mark_urgent(s, pid, r.priority)

            else:
                # Success: clear cooldown (keep tuned resources as "known good" to avoid regressions).
                ent["cooldown_until"] = 0

            s.op_tuning[opk] = ent

    # 4) Decide pool ordering and batch placement preference.
    num_pools = s.executor.num_pools
    pool_ids = list(range(num_pools))
    if num_pools > 1:
        # Prefer pool 0 for high priority; push batch to other pools first.
        hi_order = [0] + [i for i in pool_ids if i != 0]
        batch_order = [i for i in pool_ids if i != 0] + [0]
    else:
        hi_order = pool_ids
        batch_order = pool_ids

    # Helper to fill a pool with assignments using local accounting.
    def fill_pool(pool_id, batch_allowed, batch_ahead_of_interactive):
        pool = s.executor.pools[pool_id]
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)

        # Keep packing until we can't fit the next op.
        while avail_cpu >= 1.0 and avail_ram > 0.0:
            # Decide whether to allow a batch op to jump ahead of interactive.
            allow_batch_ahead = bool(batch_allowed and batch_ahead_of_interactive)

            pipeline, op = _pick_next_candidate(s, allow_batch_ahead_of_interactive=allow_batch_ahead)
            if pipeline is None:
                break

            # Enforce per-pool batch restrictions (to protect pool 0).
            if (not batch_allowed) and pipeline.priority == Priority.BATCH_PIPELINE:
                # No batch here; try another pool later.
                # Mark it urgent for its own priority only if it was chosen due to scanning.
                # (Avoid hammering this pool with batch picks.)
                _mark_urgent(s, pipeline.pipeline_id, pipeline.priority)
                break

            desired_cpu, desired_ram = _get_desired_resources(s, pool, pipeline, op)

            # Fit CPU by downsizing; fit RAM only if fully available (to avoid intentional OOMs).
            cpu = min(desired_cpu, avail_cpu)
            if cpu < 1.0:
                break

            ram = desired_ram
            if ram > avail_ram:
                # Not enough RAM now; don't force undersized allocations.
                # Make it urgent so it gets retried promptly when memory frees up.
                _mark_urgent(s, pipeline.pipeline_id, pipeline.priority)
                break

            # Commit assignment.
            assignments.append(
                Assignment(
                    ops=[op],
                    cpu=cpu,
                    ram=ram,
                    priority=pipeline.priority,
                    pool_id=pool_id,
                    pipeline_id=pipeline.pipeline_id,
                )
            )

            s.last_scheduled_tick[pipeline.pipeline_id] = s.tick

            # Spend batch credit if batch jumped ahead.
            if allow_batch_ahead and pipeline.priority == Priority.BATCH_PIPELINE:
                s.batch_credit = max(0.0, s.batch_credit - 1.0)

            # Update local available resources.
            avail_cpu -= cpu
            avail_ram -= ram

    # 5) Fill pools: first prioritize query/interactive everywhere (especially pool 0).
    #    Determine if we should let batch jump ahead of interactive (only when no query waiting and credit is available).
    allow_batch_jump = (not has_query_waiting) and (s.batch_credit >= 1.0)

    # Stage A: protect pool 0 for high priority when there is any high-priority backlog.
    has_interactive_waiting = len(s.queues[Priority.INTERACTIVE]) > 0
    has_high_backlog = has_query_waiting or has_interactive_waiting

    for pid in hi_order:
        # On pool 0, block batch if there's any high-priority backlog.
        batch_allowed = True
        if num_pools > 1 and pid == 0 and has_high_backlog:
            batch_allowed = False

        fill_pool(
            pool_id=pid,
            batch_allowed=batch_allowed,
            batch_ahead_of_interactive=allow_batch_jump,
        )

    # Stage B: if still capacity and batch remains, fill remaining with batch (prefer non-zero pools).
    # This stage mostly matters when high priority is blocked on dependencies and pools have headroom.
    for pid in batch_order:
        # Only schedule batch on pool 0 if no high backlog, or if there is just idle capacity and no progress otherwise.
        if num_pools > 1 and pid == 0 and has_high_backlog:
            continue
        fill_pool(
            pool_id=pid,
            batch_allowed=True,
            batch_ahead_of_interactive=False,
        )

    return suspensions, assignments

