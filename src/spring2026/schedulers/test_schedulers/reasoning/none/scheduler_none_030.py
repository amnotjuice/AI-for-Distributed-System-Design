# policy_key: scheduler_none_030
# reasoning_effort: none
# model: gpt-5.2-2025-12-11
# llm_cost: 0.040905
# generation_seconds: 51.38
# generated_at: 2026-04-09T22:08:38.849466
@register_scheduler_init(key="scheduler_none_030")
def scheduler_none_030_init(s):
    """Priority-aware, OOM-adaptive, anti-starvation scheduler.

    Core ideas:
      - Protect QUERY and INTERACTIVE latency via strict priority ordering and (limited) preemption.
      - Reduce 720s penalties by aggressively learning per-op RAM needs from OOM failures and retrying.
      - Keep throughput decent by right-sizing CPU (cap per-op CPU share) so we don't block the pool with one task.
      - Avoid starvation via mild aging for BATCH pipelines that wait "too long" without progress.

    Notes/assumptions:
      - RAM beyond operator minimum doesn't speed up; allocate enough to avoid OOM, then use remaining RAM opportunistically.
      - CPU has sublinear scaling; allocate a moderate slice rather than the entire pool.
    """
    # Pipeline queue and metadata
    s.waiting = []  # list[Pipeline]
    s.arrival_ts = {}  # pipeline_id -> sim time when first seen
    s.last_scheduled_ts = {}  # pipeline_id -> sim time when last scheduled an op
    s.pipeline_priority = {}  # pipeline_id -> Priority

    # Learned RAM requirements (by operator "signature"); updated on OOM failures
    # signature -> {"ram": learned_required_ram, "attempts": int}
    s.op_ram_hint = {}

    # Bookkeeping for retry backoff (avoid thrashing on repeated failures)
    # (pipeline_id, op_sig) -> {"fails": int, "cooldown_until": float}
    s.fail_state = {}

    # Config knobs (small improvements over FIFO without being too complex)
    s.max_preempt_per_tick = 2
    s.preempt_min_remaining = 1  # keep at least 1 running container if possible
    s.batch_aging_seconds = 60.0  # after this, batch gets a small boost
    s.cooldown_base = 5.0  # seconds
    s.cooldown_max = 60.0  # seconds
    s.ram_growth_factor = 1.6  # multiply RAM on OOM
    s.ram_growth_add = 0.0     # additive growth (kept 0 for simplicity)
    s.ram_safety_mult = 1.15   # small extra RAM to reduce repeated OOMs
    s.max_cpu_share = 0.50     # don't allocate more than 50% of pool CPU to a single assignment
    s.min_cpu = 1.0            # minimum cpu to assign (if available)
    s.max_ops_per_assignment = 1  # schedule one op at a time for better interactivity


@register_scheduler(key="scheduler_none_030")
def scheduler_none_030(s, results, pipelines):
    """
    Scheduler step:
      1) Ingest new pipelines; track arrival times.
      2) Update RAM hints and cooldowns from execution results (especially OOM).
      3) Optionally preempt low-priority running work if high-priority is blocked.
      4) Assign ready operators in priority order with anti-starvation aging.
    """
    # Helper functions are defined inside to avoid imports / external dependencies.
    def _now():
        # Prefer a simulator clock if present; fall back to 0.
        for attr in ("now", "time", "t", "clock"):
            if hasattr(s, attr):
                v = getattr(s, attr)
                if isinstance(v, (int, float)):
                    return float(v)
        if hasattr(s, "executor"):
            for attr in ("now", "time", "t", "clock"):
                if hasattr(s.executor, attr):
                    v = getattr(s.executor, attr)
                    if isinstance(v, (int, float)):
                        return float(v)
        return 0.0

    def _priority_rank(pri):
        # Smaller is higher priority.
        if pri == Priority.QUERY:
            return 0
        if pri == Priority.INTERACTIVE:
            return 1
        return 2  # Priority.BATCH_PIPELINE and anything else

    def _priority_weight(pri):
        if pri == Priority.QUERY:
            return 10
        if pri == Priority.INTERACTIVE:
            return 5
        return 1

    def _pipeline_done_or_failed(p):
        st = p.runtime_status()
        if st.is_pipeline_successful():
            return True
        # If any op is FAILED, we still want to retry (ASSIGNABLE_STATES includes FAILED in this environment),
        # so we do NOT drop failed pipelines here.
        return False

    def _get_ready_ops(p):
        st = p.runtime_status()
        return st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[: s.max_ops_per_assignment]

    def _op_signature(pipeline, op):
        # Best-effort stable signature for learning RAM across retries.
        # Prefer explicit operator attributes if present; otherwise fall back to repr.
        # Include pipeline_id to avoid cross-pipeline leakage if operator identity is too fuzzy.
        name = None
        for attr in ("op_id", "operator_id", "id", "name", "key"):
            if hasattr(op, attr):
                v = getattr(op, attr)
                if v is not None:
                    name = str(v)
                    break
        if name is None:
            name = str(op)
        return f"{pipeline.pipeline_id}:{name}"

    def _extract_error_string(res):
        # res.error may be string or exception-like.
        if not hasattr(res, "error"):
            return ""
        e = res.error
        if e is None:
            return ""
        try:
            return str(e).lower()
        except Exception:
            return ""

    def _is_oom(res):
        # Heuristic OOM detection from error string.
        es = _extract_error_string(res)
        if not es:
            return False
        tokens = ("oom", "out of memory", "memoryerror", "killed", "oomkilled", "oom killed")
        return any(t in es for t in tokens)

    def _record_failure_and_hint(res):
        # On OOM: increase RAM hint for each failed op signature.
        if not res.failed():
            return
        if not _is_oom(res):
            # Non-OOM failures: set a short cooldown to avoid immediate thrash.
            for op in getattr(res, "ops", []) or []:
                sig = _op_signature(_pipeline_by_id.get(res.pipeline_id, None), op) if hasattr(res, "pipeline_id") else str(op)
                key = (getattr(res, "pipeline_id", None), sig)
                fs = s.fail_state.get(key, {"fails": 0, "cooldown_until": 0.0})
                fs["fails"] += 1
                cd = min(s.cooldown_max, s.cooldown_base * (2 ** (fs["fails"] - 1)))
                fs["cooldown_until"] = max(fs["cooldown_until"], now + cd)
                s.fail_state[key] = fs
            return

        # OOM: grow hint based on last assigned RAM
        used_ram = getattr(res, "ram", None)
        if used_ram is None:
            return

        for op in getattr(res, "ops", []) or []:
            # Try to map to its pipeline for signature; fall back safely.
            pid = getattr(res, "pipeline_id", None)
            pip = _pipeline_by_id.get(pid, None)
            sig = _op_signature(pip, op) if pip is not None else str(op)

            hint = s.op_ram_hint.get(sig, {"ram": 0.0, "attempts": 0})
            hint["attempts"] += 1
            # Increase from at least current used_ram; apply growth factor + safety.
            new_ram = max(float(used_ram) * s.ram_growth_factor + s.ram_growth_add, hint["ram"])
            hint["ram"] = new_ram
            s.op_ram_hint[sig] = hint

            # Also apply cooldown to avoid re-OOM thrash
            key = (pid, sig)
            fs = s.fail_state.get(key, {"fails": 0, "cooldown_until": 0.0})
            fs["fails"] += 1
            cd = min(s.cooldown_max, s.cooldown_base * (2 ** (fs["fails"] - 1)))
            fs["cooldown_until"] = max(fs["cooldown_until"], now + cd)
            s.fail_state[key] = fs

    def _op_min_ram(op):
        # Best-effort extraction; may not exist in all environments.
        for attr in ("min_ram", "ram_min", "min_memory", "memory_min", "required_ram"):
            if hasattr(op, attr):
                v = getattr(op, attr)
                if isinstance(v, (int, float)) and v > 0:
                    return float(v)
        # Fall back: unknown; assume tiny.
        return 0.0

    def _compute_ram_request(p, op, avail_ram):
        sig = _op_signature(p, op)
        base = _op_min_ram(op)
        hint = s.op_ram_hint.get(sig, None)
        target = base
        if hint and hint.get("ram", 0.0) > target:
            target = hint["ram"]

        # Safety multiplier to reduce repeated OOMs; clamp to available.
        target = target * s.ram_safety_mult
        # If we know nothing, don't grab all RAM; be conservative to allow concurrency.
        if target <= 0.0:
            target = min(avail_ram, 0.25 * avail_ram) if avail_ram > 0 else 0.0

        # Clamp to available RAM (may still OOM; will learn).
        return max(0.0, min(float(avail_ram), float(target)))

    def _compute_cpu_request(avail_cpu, pri):
        # Give higher priority slightly more CPU, but cap per-task share.
        if avail_cpu <= 0:
            return 0.0
        cap = float(avail_cpu) * s.max_cpu_share
        if pri == Priority.QUERY:
            req = min(float(avail_cpu), max(s.min_cpu, cap))
        elif pri == Priority.INTERACTIVE:
            req = min(float(avail_cpu), max(s.min_cpu, cap * 0.8))
        else:
            req = min(float(avail_cpu), max(s.min_cpu, cap * 0.6))
        return max(0.0, min(float(avail_cpu), float(req)))

    def _cooldown_active(pipeline_id, op_sig):
        key = (pipeline_id, op_sig)
        fs = s.fail_state.get(key, None)
        if not fs:
            return False
        return fs.get("cooldown_until", 0.0) > now

    def _effective_rank(p):
        # Mild aging for batch: after waiting, slightly boost but never above INTERACTIVE.
        pri = p.priority
        base_rank = _priority_rank(pri)
        if pri != Priority.BATCH_PIPELINE:
            return base_rank

        arr = s.arrival_ts.get(p.pipeline_id, now)
        waited = max(0.0, now - arr)
        if waited >= s.batch_aging_seconds:
            # Move batch closer to interactive, but not past it.
            return 1.5
        return base_rank

    def _select_victim_container():
        # Best-effort: find a low-priority running container to preempt.
        # This relies on optional executor/container introspection. If not available, returns None.
        # Expected return: (container_id, pool_id, victim_priority_rank)
        ex = getattr(s, "executor", None)
        if ex is None:
            return None

        candidates = []
        # Try common patterns: ex.pools[pool_id].containers or .running_containers etc.
        for pool_id in range(ex.num_pools):
            pool = ex.pools[pool_id]
            for attr in ("containers", "running_containers", "active_containers"):
                if not hasattr(pool, attr):
                    continue
                conts = getattr(pool, attr)
                if conts is None:
                    continue
                try:
                    it = list(conts.values()) if isinstance(conts, dict) else list(conts)
                except Exception:
                    continue
                for c in it:
                    # container object should have .priority and .container_id
                    cid = getattr(c, "container_id", None)
                    cpri = getattr(c, "priority", None)
                    if cid is None or cpri is None:
                        continue
                    candidates.append((cid, pool_id, _priority_rank(cpri)))
        if not candidates:
            return None
        # Prefer preempting lowest priority (largest rank).
        candidates.sort(key=lambda x: x[2], reverse=True)
        return candidates[0]

    # --- Main logic starts here ---
    now = _now()

    # Track pipelines by id for hinting logic
    _pipeline_by_id = {}

    # Ingest new pipelines
    for p in pipelines:
        _pipeline_by_id[p.pipeline_id] = p
        if p.pipeline_id not in s.arrival_ts:
            s.arrival_ts[p.pipeline_id] = now
            s.last_scheduled_ts[p.pipeline_id] = now
            s.pipeline_priority[p.pipeline_id] = p.priority
        s.waiting.append(p)

    # Update known pipeline objects in queue map as well
    for p in s.waiting:
        _pipeline_by_id[p.pipeline_id] = p

    # Incorporate results: update RAM hints and cooldowns
    for res in results:
        _record_failure_and_hint(res)

    # Early exit if nothing changed
    if not pipelines and not results and not s.waiting:
        return [], []

    # Clean queue: remove completed pipelines; keep failed for retry (with larger RAM) unless simulator marks otherwise
    new_waiting = []
    for p in s.waiting:
        if _pipeline_done_or_failed(p):
            continue
        new_waiting.append(p)
    s.waiting = new_waiting

    suspensions = []
    assignments = []

    # Preemption: if there exists any ready QUERY/INTERACTIVE op but no pool can fit it, try to preempt low-priority.
    # This is conservative and limited per tick.
    def _exists_blocked_high_pri():
        high = []
        for p in s.waiting:
            if p.priority not in (Priority.QUERY, Priority.INTERACTIVE):
                continue
            ops = _get_ready_ops(p)
            if not ops:
                continue
            op = ops[0]
            sig = _op_signature(p, op)
            if _cooldown_active(p.pipeline_id, sig):
                continue
            high.append((p, op))
        if not high:
            return False

        # Check if any pool has enough headroom for at least one high-priority op (roughly).
        ex = s.executor
        for (p, op) in high:
            for pool_id in range(ex.num_pools):
                pool = ex.pools[pool_id]
                if pool.avail_cpu_pool <= 0 or pool.avail_ram_pool <= 0:
                    continue
                cpu_req = _compute_cpu_request(pool.avail_cpu_pool, p.priority)
                ram_req = _compute_ram_request(p, op, pool.avail_ram_pool)
                if cpu_req > 0 and ram_req > 0:
                    return False
        return True

    # Only attempt preemption if we appear blocked and we can introspect containers.
    if _exists_blocked_high_pri():
        for _ in range(s.max_preempt_per_tick):
            victim = _select_victim_container()
            if victim is None:
                break
            cid, pool_id, prank = victim
            # Only preempt if victim is lower priority than interactive.
            if prank <= 1:
                break
            # Also ensure we don't churn too hard: keep some work running if possible.
            # Best-effort: check pool has some available after preempt; if not introspectable, still suspend.
            suspensions.append(Suspend(container_id=cid, pool_id=pool_id))

    # Sort pipelines by effective rank, then by last scheduled (older first), then by weight (higher first)
    # to protect high-priority tail latency while allowing some fairness.
    def _sort_key(p):
        return (
            _effective_rank(p),
            s.last_scheduled_ts.get(p.pipeline_id, now),
            -_priority_weight(p.priority),
            s.arrival_ts.get(p.pipeline_id, now),
        )

    s.waiting.sort(key=_sort_key)

    # Try to place work across pools; for each pool, pick the best pipeline/op that fits.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)
        if avail_cpu <= 0.0 or avail_ram <= 0.0:
            continue

        # Find first fit in priority order.
        chosen = None  # (pipeline, op, cpu_req, ram_req)
        for p in s.waiting:
            st = p.runtime_status()
            if st.is_pipeline_successful():
                continue

            ops = _get_ready_ops(p)
            if not ops:
                continue
            op = ops[0]
            sig = _op_signature(p, op)
            if _cooldown_active(p.pipeline_id, sig):
                continue

            cpu_req = _compute_cpu_request(avail_cpu, p.priority)
            if cpu_req <= 0.0:
                continue

            ram_req = _compute_ram_request(p, op, avail_ram)
            if ram_req <= 0.0:
                continue

            # Require at least the operator's known min RAM if we can detect it; otherwise accept.
            min_ram = _op_min_ram(op)
            if min_ram > 0.0 and ram_req + 1e-9 < min_ram and avail_ram >= min_ram:
                # If we can satisfy min_ram, bump to min_ram rather than knowingly under-allocating.
                ram_req = min(avail_ram, min_ram)

            # If we still can't meet min_ram, skip; otherwise try.
            if min_ram > 0.0 and ram_req + 1e-9 < min_ram:
                continue

            chosen = (p, op, cpu_req, ram_req)
            break

        if chosen is None:
            continue

        p, op, cpu_req, ram_req = chosen

        assignment = Assignment(
            ops=[op],
            cpu=cpu_req,
            ram=ram_req,
            priority=p.priority,
            pool_id=pool_id,
            pipeline_id=p.pipeline_id,
        )
        assignments.append(assignment)
        s.last_scheduled_ts[p.pipeline_id] = now

        # Update local available resources to avoid oversubscription within the same tick.
        # (One assignment per pool per tick is conservative and aligns with "small improvements" guideline.)
        # If you want more packing later, extend with a while-loop and update avail_* accordingly.
        continue

    return suspensions, assignments
