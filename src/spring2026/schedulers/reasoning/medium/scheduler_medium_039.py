# policy_key: scheduler_medium_039
# reasoning_effort: medium
# exp: reasoning
# model: gpt-5.2-2025-12-11
# llm_cost: 0.081211
# generation_seconds: 81.92
# generated_at: 2026-04-09T23:22:48.018904
@register_scheduler_init(key="scheduler_medium_039")
def scheduler_medium_039_init(s):
    """Priority-aware, completion-focused scheduler.

    Goals:
      - Protect query and interactive latency via strict priority + soft resource reservation.
      - Minimize failures (720s penalty) using conservative initial RAM sizing and OOM-driven RAM backoff.
      - Avoid starvation using simple aging-based promotion for long-waiting pipelines.
      - Keep implementation robust: no reliance on optional executor/container introspection; no preemption.
    """
    # Logical time (ticks of scheduler invocations)
    s.tick = 0

    # Active pipelines by id (arrived but not yet successful)
    s.active = {}  # pipeline_id -> Pipeline

    # Pipeline metadata for aging and throttling
    s.pmeta = {}  # pipeline_id -> dict(arrival_tick=int, last_assign_tick=int, fail_count=int)

    # Per-operator learned resource hints (best-effort; keys are stable strings)
    s.op_ram_hint = {}  # op_key -> float (GB)
    s.op_cpu_hint = {}  # op_key -> float (vCPU)

    # Failure accounting to detect repeated issues
    s.op_fail_counts = {}   # op_key -> int
    s.op_oom_counts = {}    # op_key -> int
    s.op_last_error = {}    # op_key -> str

    # Tuning knobs (kept simple and conservative)
    s.hp_reserve_frac_cpu = 0.20  # reserve capacity for QUERY/INTERACTIVE when such work is pending
    s.hp_reserve_frac_ram = 0.20
    s.aging_promote_ticks = 250   # every N ticks, a pipeline is promoted by one level (batch -> interactive)
    s.per_pipeline_cooldown = 1   # don't schedule the same pipeline multiple times in the same tick


def _prio_level(priority):
    # Lower is higher priority
    if priority == Priority.QUERY:
        return 0
    if priority == Priority.INTERACTIVE:
        return 1
    return 2  # Priority.BATCH_PIPELINE and others


def _op_key(op):
    # Try common identifiers, fall back to a stable repr-based key
    for attr in ("op_id", "operator_id", "id", "name"):
        if hasattr(op, attr):
            try:
                v = getattr(op, attr)
                if v is not None:
                    return f"{attr}:{v}"
            except Exception:
                pass
    return f"repr:{repr(op)}"


def _is_oom(err):
    if not err:
        return False
    e = str(err).lower()
    return ("oom" in e) or ("out of memory" in e) or ("memoryerror" in e) or ("cuda out of memory" in e)


def _clamp(x, lo, hi):
    if x < lo:
        return lo
    if x > hi:
        return hi
    return x


def _initial_frac_for_priority(priority):
    # Conservative RAM first to avoid OOM; CPU kept moderate to allow concurrency.
    if priority == Priority.QUERY:
        return 0.85, 0.70  # (ram_frac, cpu_frac)
    if priority == Priority.INTERACTIVE:
        return 0.75, 0.55
    return 0.65, 0.35


def _desired_resources(s, op, priority, pool, avail_cpu, avail_ram):
    """Choose cpu/ram for a single-op assignment.

    Strategy:
      - RAM: use learned hint if available; else allocate a conservative fraction of pool max.
             If the op has OOM history, bias upward.
      - CPU: use learned hint if available; else allocate a priority-based fraction of pool max.
    """
    opk = _op_key(op)

    # RAM sizing
    ram_frac, cpu_frac = _initial_frac_for_priority(priority)
    base_ram = pool.max_ram_pool * ram_frac

    # If we have a hint, use it; otherwise use base. Apply OOM-history multiplier.
    hint_ram = s.op_ram_hint.get(opk, None)
    oom_ct = s.op_oom_counts.get(opk, 0)
    oom_mult = 1.0 + min(1.5, 0.35 * oom_ct)  # up to 2.5x for repeated OOMs

    target_ram = (hint_ram if hint_ram is not None else base_ram) * oom_mult

    # Never request more than the pool max; never request less than a small minimum if available.
    # (We don't know operator minima; better to be generous but bounded.)
    min_reasonable_ram = min(1.0, pool.max_ram_pool)  # 1GB if possible, else max available in tiny pools
    target_ram = _clamp(target_ram, min_reasonable_ram, pool.max_ram_pool)
    target_ram = min(target_ram, avail_ram)

    # CPU sizing
    base_cpu = max(1.0, pool.max_cpu_pool * cpu_frac)
    hint_cpu = s.op_cpu_hint.get(opk, None)
    target_cpu = hint_cpu if hint_cpu is not None else base_cpu
    target_cpu = _clamp(target_cpu, 1.0, pool.max_cpu_pool)
    target_cpu = min(target_cpu, avail_cpu)

    # Ensure we don't create empty assignments
    if target_cpu <= 0 or target_ram <= 0:
        return 0, 0
    return target_cpu, target_ram


def _effective_level(s, pipeline):
    """Aging-based promotion to avoid starvation.

    Base: QUERY(0), INTERACTIVE(1), BATCH(2)
    Every aging_promote_ticks, reduce level by 1 (down to 0).
    """
    pid = pipeline.pipeline_id
    meta = s.pmeta.get(pid)
    base = _prio_level(pipeline.priority)
    if not meta:
        return base
    age = max(0, s.tick - meta["arrival_tick"])
    promote = age // max(1, s.aging_promote_ticks)
    return max(0, base - min(base, promote))


def _pick_pool_order(priority, num_pools):
    # If multiple pools exist, steer batch away from pool 0 to reduce interference.
    if num_pools <= 1:
        return [0]
    if priority in (Priority.QUERY, Priority.INTERACTIVE):
        return list(range(num_pools))  # try pool 0 first
    # batch: try non-zero pools first
    return list(range(1, num_pools)) + [0]


def _has_pending_hp(candidates):
    for _, p in candidates:
        if p.priority in (Priority.QUERY, Priority.INTERACTIVE):
            return True
    return False


@register_scheduler(key="scheduler_medium_039")
def scheduler_medium_039(s, results: List[ExecutionResult], pipelines: List[Pipeline]) -> Tuple[List[Suspend], List[Assignment]]:
    """
    Scheduling step.

    - Update learned hints from execution results (especially OOM backoff).
    - Maintain active pipelines (do not drop on failures; retry with increased RAM).
    - Build a list of runnable (ready) operators across pipelines.
    - Assign operations to pools using:
        (1) strict priority + aging promotion,
        (2) soft reservation of capacity for high-priority work,
        (3) conservative RAM to reduce failure risk.
    """
    s.tick += 1

    suspensions: List[Suspend] = []
    assignments: List[Assignment] = []

    # 1) Ingest new pipelines
    for p in pipelines:
        pid = p.pipeline_id
        s.active[pid] = p
        if pid not in s.pmeta:
            s.pmeta[pid] = {
                "arrival_tick": s.tick,
                "last_assign_tick": -10**9,
                "fail_count": 0,
            }

    # 2) Learn from results
    for r in results:
        # Results may include multiple ops; in this simulator we typically assign 1 op at a time,
        # but handle lists defensively.
        rops = []
        try:
            rops = list(r.ops) if r.ops is not None else []
        except Exception:
            rops = []

        if not rops:
            continue

        # Update each op independently (hints keyed by op signature)
        for op in rops:
            opk = _op_key(op)

            if r.failed():
                s.op_fail_counts[opk] = s.op_fail_counts.get(opk, 0) + 1
                s.op_last_error[opk] = str(r.error) if r.error is not None else ""

                if _is_oom(r.error):
                    s.op_oom_counts[opk] = s.op_oom_counts.get(opk, 0) + 1
                    # Increase RAM hint aggressively on OOM to drive completion
                    # Prefer doubling what was used; cap at pool max when applying at scheduling time.
                    prev = s.op_ram_hint.get(opk, None)
                    bumped = max(float(r.ram) * 2.0, float(r.ram) + 0.10 * float(r.ram))
                    s.op_ram_hint[opk] = max(prev if prev is not None else 0.0, bumped)
                else:
                    # For non-OOM failures, don't shrink; keep RAM hint at least what we tried.
                    prev = s.op_ram_hint.get(opk, None)
                    tried = float(r.ram)
                    s.op_ram_hint[opk] = max(prev if prev is not None else 0.0, tried)
            else:
                # On success, keep RAM/CPU hints near what worked (avoid shrinking too much).
                # This favors completion and reduces the chance of future OOMs.
                prev_ram = s.op_ram_hint.get(opk, None)
                prev_cpu = s.op_cpu_hint.get(opk, None)
                used_ram = float(r.ram)
                used_cpu = float(r.cpu)

                if prev_ram is None:
                    s.op_ram_hint[opk] = used_ram
                else:
                    # EMA biased toward the larger value
                    s.op_ram_hint[opk] = 0.7 * max(prev_ram, used_ram) + 0.3 * min(prev_ram, used_ram)

                if prev_cpu is None:
                    s.op_cpu_hint[opk] = used_cpu
                else:
                    s.op_cpu_hint[opk] = 0.6 * prev_cpu + 0.4 * used_cpu

    # 3) Refresh active set: remove completed pipelines
    #    (Do not drop failed/incomplete pipelines; retrying is handled by ASSIGNABLE_STATES including FAILED.)
    to_remove = []
    for pid, p in s.active.items():
        try:
            st = p.runtime_status()
            if st.is_pipeline_successful():
                to_remove.append(pid)
        except Exception:
            # If runtime_status is temporarily unavailable, keep pipeline.
            pass
    for pid in to_remove:
        s.active.pop(pid, None)
        s.pmeta.pop(pid, None)

    # Early exit if no decision points
    if not s.active and not pipelines and not results:
        return [], []

    # 4) Build candidate list of (op, pipeline) that are runnable now
    candidates = []
    for pid, p in s.active.items():
        try:
            st = p.runtime_status()
        except Exception:
            continue

        # Skip if no runnable ops (parents not complete, etc.)
        try:
            ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        except Exception:
            ops = []

        if not ops:
            continue

        # Simple: pick the first runnable op for the pipeline
        op = ops[0]
        candidates.append((op, p))

    if not candidates:
        return [], []

    # 5) Sort candidates by effective priority (with aging) and then by age (older first within level)
    def _cand_sort_key(item):
        op, p = item
        pid = p.pipeline_id
        meta = s.pmeta.get(pid, {"arrival_tick": s.tick})
        age = s.tick - meta["arrival_tick"]
        lvl = _effective_level(s, p)
        # Prefer higher priority (lower lvl), then older, then stable by id
        return (lvl, -age, pid)

    candidates.sort(key=_cand_sort_key)

    # Determine if we should reserve resources for high priority work
    pending_hp = _has_pending_hp(candidates)

    # 6) Assign across pools; avoid assigning multiple times to the same pipeline in one tick
    scheduled_pipelines = set()

    # Per-pool local availability tracking (since we may add multiple assignments per pool)
    local_avail = []
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        local_avail.append([float(pool.avail_cpu_pool), float(pool.avail_ram_pool)])

    # Helper: try to place a single candidate into the "best" pool for its priority
    def _try_place(op, p):
        pid = p.pipeline_id
        if pid in scheduled_pipelines:
            return False

        meta = s.pmeta.get(pid)
        if meta is not None and (s.tick - meta["last_assign_tick"]) < s.per_pipeline_cooldown:
            return False

        pool_order = _pick_pool_order(p.priority, s.executor.num_pools)

        for pool_id in pool_order:
            pool = s.executor.pools[pool_id]
            avail_cpu, avail_ram = local_avail[pool_id]

            if avail_cpu <= 0 or avail_ram <= 0:
                continue

            # Soft reservation: when high-priority work is pending, ensure batch doesn't consume all headroom
            if pending_hp and p.priority == Priority.BATCH_PIPELINE:
                reserve_cpu = float(pool.max_cpu_pool) * float(s.hp_reserve_frac_cpu)
                reserve_ram = float(pool.max_ram_pool) * float(s.hp_reserve_frac_ram)
            else:
                reserve_cpu = 0.0
                reserve_ram = 0.0

            cpu_req, ram_req = _desired_resources(s, op, p.priority, pool, avail_cpu, avail_ram)
            if cpu_req <= 0 or ram_req <= 0:
                continue

            # Enforce reservation for batch
            if p.priority == Priority.BATCH_PIPELINE and pending_hp:
                if (avail_cpu - cpu_req) < reserve_cpu or (avail_ram - ram_req) < reserve_ram:
                    continue

            # Make assignment
            assignments.append(
                Assignment(
                    ops=[op],
                    cpu=cpu_req,
                    ram=ram_req,
                    priority=p.priority,
                    pool_id=pool_id,
                    pipeline_id=pid,
                )
            )

            # Update local availability
            local_avail[pool_id][0] = avail_cpu - cpu_req
            local_avail[pool_id][1] = avail_ram - ram_req

            scheduled_pipelines.add(pid)
            if meta is not None:
                meta["last_assign_tick"] = s.tick
            return True

        return False

    # 7) Two-pass scheduling:
    #    Pass A: place all QUERY/INTERACTIVE first (including aged-promoted ones via effective level)
    #    Pass B: place remaining (mostly batch) respecting reservations
    #
    # We implement this by iterating candidates multiple times with simple filters.
    # This keeps the logic robust and avoids overfitting to unknown simulator details.

    # Pass A: effective levels 0 and 1
    for op, p in candidates:
        lvl = _effective_level(s, p)
        if lvl <= 1:
            _try_place(op, p)

    # Pass B: everything else
    for op, p in candidates:
        if p.pipeline_id in scheduled_pipelines:
            continue
        _try_place(op, p)

    return suspensions, assignments
