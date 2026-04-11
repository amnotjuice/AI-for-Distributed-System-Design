# policy_key: scheduler_none_003
# reasoning_effort: none
# model: gpt-5.2-2025-12-11
# llm_cost: 0.051265
# generation_seconds: 46.51
# generated_at: 2026-04-09T21:47:13.338448
@register_scheduler_init(key="scheduler_none_003")
def scheduler_none_003_init(s):
    """Priority-aware, failure-averse scheduler with conservative sizing and bounded preemption.

    Core ideas (incremental over FIFO):
      1) Priority + aging: always prefer QUERY > INTERACTIVE > BATCH, but age waiting pipelines to avoid starvation.
      2) Failure-averse RAM sizing: start with modest RAM; on OOM (or failure), increase RAM for that operator next time.
      3) Small, safe CPU sizing: give reasonable CPU shares to reduce tail latency, but avoid monopolizing pools.
      4) Bounded preemption: if a high-priority pipeline is blocked and no pool has headroom, preempt at most one
         lower-priority running container (prefer the one with largest allocation) to free resources.

    Notes:
      - We never drop pipelines ourselves. Failed/incomplete is very costly (720s), so we bias toward completion.
      - We only use information accessible via Pipeline/ExecutionResult and pool headroom.
    """
    s.waiting = []  # list of dict entries: {"p": Pipeline, "t0": float, "seq": int}
    s.seq = 0

    # Per-operator RAM "hint" learned from failures (keyed by pipeline_id, op_id)
    # Value is a multiplier on the op's minimum RAM requirement (if available), else absolute RAM guess.
    s.op_ram_mult = {}  # (pipeline_id, op_key) -> float
    s.op_ram_abs = {}   # (pipeline_id, op_key) -> float

    # Remember last successful/failing sizes for containers (keyed by container_id)
    s.container_last = {}  # container_id -> {"cpu": x, "ram": y, "pool_id": i, "priority": Priority, "pipeline_id": id, "op_keys":[...]}

    # Track waiting start time for aging (uses simulation time if available; else monotonic tick counter)
    s.tick = 0

    # Conservative defaults; chosen to be simple and robust across pool sizes
    s.base_ram_frac = {
        Priority.QUERY: 0.35,
        Priority.INTERACTIVE: 0.30,
        Priority.BATCH_PIPELINE: 0.20,
    }
    s.base_cpu_frac = {
        Priority.QUERY: 0.60,
        Priority.INTERACTIVE: 0.50,
        Priority.BATCH_PIPELINE: 0.35,
    }

    # How aggressively to increase RAM after a failure
    s.ram_backoff = 1.6
    s.ram_backoff_cap_mult = 8.0  # cap multiplier vs min to avoid runaway; if min unknown, cap vs pool max separately

    # Preemption guardrails
    s.enable_preemption = True
    s.max_preempt_per_tick = 1
    s.min_high_pri_wait_ticks_for_preempt = 2  # avoid churn on transient contention


@register_scheduler(key="scheduler_none_003")
def scheduler_none_003(s, results, pipelines):
    """
    Scheduler loop. Returns (suspensions, assignments).

    Steps:
      - Ingest arrivals into a waiting list.
      - Learn from execution results (failures -> increase RAM hints for those ops).
      - Build a ranked list of runnable ops (parents complete) with priority + aging.
      - For each pool, place one small "batch" of ops (1 per assignment) using available headroom.
      - If high-priority work is blocked everywhere, preempt one lower-priority running container.
    """
    # Local helpers kept inside to respect "no imports" at top-level.
    def _now_tick():
        # Prefer simulator time if present; else fall back to our own tick counter.
        t = getattr(s.executor, "time", None)
        if t is None:
            return s.tick
        try:
            return float(t)
        except Exception:
            return s.tick

    def _prio_rank(prio):
        # Higher is better
        if prio == Priority.QUERY:
            return 3
        if prio == Priority.INTERACTIVE:
            return 2
        return 1

    def _aging_bonus(wait_ticks):
        # Soft aging: every ~10 ticks adds a small bump; keeps starvation low without overriding priority.
        # Capped to prevent a batch job from leapfrogging many queries.
        try:
            wt = float(wait_ticks)
        except Exception:
            wt = 0.0
        return min(0.75, wt / 20.0)

    def _op_key(op):
        # Best-effort stable key; different implementations may expose different identifiers.
        for attr in ("op_id", "operator_id", "id", "name"):
            if hasattr(op, attr):
                try:
                    v = getattr(op, attr)
                    if callable(v):
                        v = v()
                    return str(v)
                except Exception:
                    pass
        return str(id(op))

    def _op_min_ram(op):
        # Best-effort: use typical attribute names; if unknown, return None.
        for attr in ("min_ram", "min_ram_gb", "ram_min", "min_memory", "min_memory_gb", "memory_min"):
            if hasattr(op, attr):
                try:
                    v = getattr(op, attr)
                    if callable(v):
                        v = v()
                    if v is None:
                        continue
                    return float(v)
                except Exception:
                    continue
        # Some models store requirements in a dict-like "resources"
        if hasattr(op, "resources"):
            try:
                r = op.resources
                if isinstance(r, dict):
                    for k in ("min_ram", "ram", "memory", "mem"):
                        if k in r and r[k] is not None:
                            return float(r[k])
            except Exception:
                pass
        return None

    def _pool_caps(pool):
        return float(pool.max_cpu_pool), float(pool.max_ram_pool)

    def _choose_pool_for(prio):
        # Prefer putting queries/interactive into pools with most headroom to reduce queueing.
        # For batch, prefer fullest packing (least remaining) to leave headroom in other pools.
        best = None
        for pid in range(s.executor.num_pools):
            pool = s.executor.pools[pid]
            avail_cpu = float(pool.avail_cpu_pool)
            avail_ram = float(pool.avail_ram_pool)
            max_cpu, max_ram = _pool_caps(pool)
            if max_cpu <= 0 or max_ram <= 0:
                continue
            cpu_head = avail_cpu / max_cpu
            ram_head = avail_ram / max_ram
            head = min(cpu_head, ram_head)
            if best is None:
                best = (pid, head)
                continue
            if prio in (Priority.QUERY, Priority.INTERACTIVE):
                if head > best[1]:
                    best = (pid, head)
            else:
                # batch: pick pool with smaller headroom (pack), but still > 0
                if head < best[1]:
                    best = (pid, head)
        return None if best is None else best[0]

    def _size_for(pool, prio, op, pipeline_id):
        max_cpu, max_ram = _pool_caps(pool)
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)

        # Base sizing fractions per priority.
        cpu = max(1.0, max_cpu * s.base_cpu_frac.get(prio, 0.35))
        ram = max(1.0, max_ram * s.base_ram_frac.get(prio, 0.20))

        # Bound by current availability.
        cpu = min(cpu, avail_cpu)
        ram = min(ram, avail_ram)

        # Use minimum RAM requirement if known; ensure we meet it (avoid OOM).
        min_ram = _op_min_ram(op)
        opk = _op_key(op)
        mult = s.op_ram_mult.get((pipeline_id, opk))
        abs_hint = s.op_ram_abs.get((pipeline_id, opk))

        if min_ram is not None and min_ram > 0:
            # Apply learned multiplier if present, else start at 1.0.
            m = 1.0 if mult is None else float(mult)
            # Clamp multiplier.
            m = max(1.0, min(m, s.ram_backoff_cap_mult))
            hinted = min_ram * m
            ram = max(ram, hinted)
        elif abs_hint is not None:
            # No min known; use learned absolute hint.
            ram = max(ram, float(abs_hint))

        # Cap by pool max and current availability.
        ram = min(ram, float(pool.avail_ram_pool), max_ram)
        cpu = min(cpu, float(pool.avail_cpu_pool), max_cpu)

        # Ensure positive.
        cpu = max(1.0, cpu)
        ram = max(1.0, ram)
        return cpu, ram

    def _record_result(res):
        # Update failure-based hints.
        if res is None:
            return
        try:
            failed = res.failed()
        except Exception:
            failed = bool(getattr(res, "error", None))
        if not failed:
            return

        # If we can map container -> pipeline/op(s), update their RAM hints.
        meta = s.container_last.get(getattr(res, "container_id", None))
        if not meta:
            return
        pipeline_id = meta.get("pipeline_id")
        op_keys = meta.get("op_keys", [])
        used_ram = float(getattr(res, "ram", meta.get("ram", 0.0)) or 0.0)

        # Heuristic: treat any failure as potential OOM and increase RAM. (Simpler, safer for completion)
        for opk in op_keys:
            k = (pipeline_id, opk)
            cur_mult = s.op_ram_mult.get(k, 1.0)
            new_mult = min(float(cur_mult) * s.ram_backoff, s.ram_backoff_cap_mult)
            s.op_ram_mult[k] = new_mult

            # Also keep an absolute floor if min unknown; increase to 1.2x last used ram.
            cur_abs = s.op_ram_abs.get(k, 0.0)
            s.op_ram_abs[k] = max(float(cur_abs), used_ram * 1.2, 1.0)

    def _pipeline_state(p):
        try:
            return p.runtime_status()
        except Exception:
            return None

    def _is_done_or_failed(pstatus):
        if pstatus is None:
            return False
        try:
            if pstatus.is_pipeline_successful():
                return True
        except Exception:
            pass
        # If any operator failed, keep it retryable (ASSIGNABLE_STATES includes FAILED).
        # We do not drop pipelines on failure because failing is heavily penalized.
        return False

    def _runnable_ops(pstatus):
        if pstatus is None:
            return []
        try:
            return pstatus.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        except Exception:
            return []

    def _find_preempt_candidate():
        # Best-effort: look for running containers via executor/pool structures if exposed.
        # We prefer preempting lowest priority and largest allocation to free headroom quickly.
        candidates = []
        for pid in range(s.executor.num_pools):
            pool = s.executor.pools[pid]
            for attr in ("containers", "running_containers", "active_containers"):
                if hasattr(pool, attr):
                    try:
                        conts = getattr(pool, attr)
                        if callable(conts):
                            conts = conts()
                        # conts could be dict or list
                        if isinstance(conts, dict):
                            cont_iter = conts.values()
                        else:
                            cont_iter = conts
                        for c in cont_iter:
                            # Extract basics
                            cid = getattr(c, "container_id", None) or getattr(c, "id", None)
                            if cid is None:
                                continue
                            pr = getattr(c, "priority", None)
                            cpu = float(getattr(c, "cpu", 0.0) or 0.0)
                            ram = float(getattr(c, "ram", 0.0) or 0.0)
                            candidates.append((pid, cid, pr, cpu, ram))
                    except Exception:
                        pass

        # If we couldn't see live containers, fall back to last-seen containers from results metadata.
        # This is less accurate but can still work if container ids remain valid.
        if not candidates:
            for cid, meta in s.container_last.items():
                candidates.append((meta.get("pool_id"), cid, meta.get("priority"), float(meta.get("cpu", 0.0)), float(meta.get("ram", 0.0))))

        if not candidates:
            return None

        # Choose lowest priority, then largest ram+cpu
        def key_fn(x):
            _, _, pr, cpu, ram = x
            return (_prio_rank(pr), -(ram + cpu))

        # We want smallest _prio_rank => lowest priority; so sort ascending by prio_rank, then descending by size.
        candidates.sort(key=key_fn)
        return candidates[0]  # (pool_id, container_id, prio, cpu, ram)

    # Advance local tick
    s.tick += 1
    now = _now_tick()

    # Ingest new pipelines
    for p in pipelines:
        s.waiting.append({"p": p, "t0": now, "seq": s.seq})
        s.seq += 1

    # Learn from results
    for r in results:
        _record_result(r)

    # Early exit if nothing to do
    if not pipelines and not results and not s.waiting:
        return [], []

    # Clean waiting list (remove completed pipelines)
    new_waiting = []
    for entry in s.waiting:
        p = entry["p"]
        st = _pipeline_state(p)
        if _is_done_or_failed(st):
            continue
        new_waiting.append(entry)
    s.waiting = new_waiting

    suspensions = []
    assignments = []

    # Build list of candidate (score, entry, op)
    candidates = []
    for entry in s.waiting:
        p = entry["p"]
        st = _pipeline_state(p)
        if st is None:
            continue
        if _is_done_or_failed(st):
            continue
        ops = _runnable_ops(st)
        if not ops:
            continue

        pr = p.priority
        wait = max(0.0, now - entry["t0"])
        score = _prio_rank(pr) + _aging_bonus(wait)
        # Small FIFO tie-breaker (earlier seq first => higher score)
        score += max(0.0, 0.001 * (1.0 / (1.0 + entry["seq"])))
        # We'll pick best op per pipeline first (just take the first runnable op)
        candidates.append((score, entry, ops[0]))

    # Sort best-first
    candidates.sort(key=lambda x: x[0], reverse=True)

    # Attempt to place as many as possible, one op per assignment, choosing pools adaptively.
    used_candidates = set()
    for _ in range(len(candidates)):
        # refresh per-iteration: headroom changes as we assign
        picked = None
        for idx, (score, entry, op) in enumerate(candidates):
            if idx in used_candidates:
                continue
            p = entry["p"]
            pr = p.priority
            pool_id = _choose_pool_for(pr)
            if pool_id is None:
                continue
            pool = s.executor.pools[pool_id]
            if float(pool.avail_cpu_pool) <= 0 or float(pool.avail_ram_pool) <= 0:
                continue

            cpu, ram = _size_for(pool, pr, op, p.pipeline_id)
            # Check feasibility
            if cpu <= float(pool.avail_cpu_pool) + 1e-9 and ram <= float(pool.avail_ram_pool) + 1e-9:
                picked = (idx, pool_id, cpu, ram, p, op)
                break

        if picked is None:
            break

        idx, pool_id, cpu, ram, p, op = picked
        used_candidates.add(idx)

        op_list = [op]
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

        # Record meta to update hints on failure.
        # (If the simulator returns container_id only after launch, this still helps by best-effort mapping later.)
        # We also store by a synthetic key in case container_id isn't available; updated on results when known.
        # Here we can only store "pending" under None; harmless.
        s.container_last[None] = {
            "cpu": cpu,
            "ram": ram,
            "pool_id": pool_id,
            "priority": p.priority,
            "pipeline_id": p.pipeline_id,
            "op_keys": [_op_key(op)],
        }

    # Preemption: if we made no assignments and there exists waiting high-priority runnable ops,
    # try to preempt one low-priority container to unblock.
    if s.enable_preemption and not assignments:
        # detect blocked high-priority work
        has_hi = False
        hi_wait_long_enough = False
        for score, entry, op in candidates[:5]:
            pr = entry["p"].priority
            if pr in (Priority.QUERY, Priority.INTERACTIVE):
                has_hi = True
                wait = max(0.0, now - entry["t0"])
                if wait >= s.min_high_pri_wait_ticks_for_preempt:
                    hi_wait_long_enough = True
                break

        if has_hi and hi_wait_long_enough:
            preempted = 0
            cand = _find_preempt_candidate()
            if cand is not None and preempted < s.max_preempt_per_tick:
                pool_id, container_id, pr, cpu, ram = cand
                # Only preempt lower priority than interactive/query
                if pr == Priority.BATCH_PIPELINE:
                    try:
                        suspensions.append(Suspend(container_id=container_id, pool_id=pool_id))
                        preempted += 1
                    except Exception:
                        pass

    return suspensions, assignments
