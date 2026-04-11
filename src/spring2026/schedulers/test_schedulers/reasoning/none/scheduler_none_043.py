# policy_key: scheduler_none_043
# reasoning_effort: none
# model: gpt-5.2-2025-12-11
# llm_cost: 0.046393
# generation_seconds: 50.09
# generated_at: 2026-04-09T22:18:36.706847
@register_scheduler_init(key="scheduler_none_043")
def scheduler_none_043_init(s):
    """Priority-aware, OOM-adaptive, low-churn scheduler.

    Goals aligned to weighted latency score:
      - Prioritize QUERY and INTERACTIVE to reduce dominant weighted latency.
      - Avoid failures by learning per-operator RAM needs from OOMs and never under-allocating again.
      - Minimize preemption churn: only preempt when high-priority work cannot be admitted otherwise.
      - Maintain progress for BATCH via aging (soft fairness) to avoid 720s penalties from incompletion.

    High-level behavior:
      1) Maintain per-priority queues of pipelines and a simple aging mechanism.
      2) For each pool, try to admit one "best" ready operator at a time with right-sized RAM/CPU.
      3) On OOM failure, bump the operator's RAM request (exponential backoff + floor) and requeue.
      4) If no capacity for high-priority ready op, preempt one lowest-priority container in that pool.
    """
    # Pipelines waiting to be scheduled, grouped by priority.
    s.q_query = []
    s.q_interactive = []
    s.q_batch = []

    # Track arrivals to implement mild aging for fairness.
    s.tick = 0
    s.pipeline_arrival_tick = {}  # pipeline_id -> first seen tick

    # Learned RAM needs per (pipeline_id, op_id) based on OOM failures.
    s.op_ram_hint = {}  # (pipeline_id, op_id) -> ram

    # Track last assigned CPU per (pipeline_id, op_id) for small incremental scaling.
    s.op_cpu_hint = {}  # (pipeline_id, op_id) -> cpu

    # Track running containers by pool so we can preempt when needed.
    # container_id -> dict(priority, pool_id)
    s.running = {}

    # Conservative defaults: reserve headroom to reduce cascading OOM/queueing.
    s.ram_headroom_frac = 0.10  # keep 10% RAM free if possible
    s.cpu_headroom_frac = 0.05  # keep 5% CPU free if possible

    # RAM backoff parameters for OOMs.
    s.oom_backoff_mult = 1.6
    s.oom_backoff_add = 0.5  # additive bump in "RAM units" used by simulator
    s.max_ram_frac = 0.95  # never request more than 95% of pool RAM to keep room for others

    # CPU sizing: prefer modest CPU to reduce contention; boost on repeats.
    s.cpu_min_frac = 0.25  # of pool
    s.cpu_max_frac = 1.00  # of pool
    s.cpu_step_mult = 1.35

    # Aging: periodically allow batch to compete if it has waited too long.
    s.batch_aging_ticks = 25
    s.interactive_aging_ticks = 40  # let interactive occasionally outrank query only if very old (rare)


@register_scheduler(key="scheduler_none_043")
def scheduler_none_043(s, results, pipelines):
    """
    Returns (suspensions, assignments).

    Core scheduling loop:
      - Ingest new pipelines into priority queues.
      - Update learned RAM/CPU hints from execution results (especially OOM failures).
      - For each pool: attempt to schedule one ready operator (or a few if ample headroom),
        using priority + aging, with RAM sized to avoid OOM and CPU sized modestly.
      - If a high-priority ready op can't fit, preempt a low-priority running container
        in that same pool (at most one per tick per pool) to make room.
    """
    s.tick += 1

    # ---------- Helpers (no imports) ----------
    def _push_pipeline(p):
        pid = p.pipeline_id
        if pid not in s.pipeline_arrival_tick:
            s.pipeline_arrival_tick[pid] = s.tick
        if p.priority == Priority.QUERY:
            s.q_query.append(p)
        elif p.priority == Priority.INTERACTIVE:
            s.q_interactive.append(p)
        else:
            s.q_batch.append(p)

    def _is_done_or_failed(p):
        st = p.runtime_status()
        if st.is_pipeline_successful():
            return True
        # If any operator is FAILED, we still want to retry; don't drop here.
        return False

    def _get_ready_ops(p):
        st = p.runtime_status()
        # Require parents complete; pull at most 1 op (atomic scheduling).
        ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        if not ops:
            return []
        return ops[:1]

    def _op_id(op):
        # Operators may have different identifier field names across versions.
        # Prefer stable ids if present.
        if hasattr(op, "op_id"):
            return op.op_id
        if hasattr(op, "operator_id"):
            return op.operator_id
        if hasattr(op, "id"):
            return op.id
        # Fallback: object identity
        return str(id(op))

    def _pool_headroomed(avail_cpu, avail_ram, pool):
        # Keep some headroom to avoid saturation spirals.
        cpu_limit = max(0.0, pool.max_cpu_pool * (1.0 - s.cpu_headroom_frac))
        ram_limit = max(0.0, pool.max_ram_pool * (1.0 - s.ram_headroom_frac))
        # We treat "available" as current free; headroom means don't consume past limit.
        # Convert to an effective available.
        eff_avail_cpu = min(avail_cpu, cpu_limit)
        eff_avail_ram = min(avail_ram, ram_limit)
        return eff_avail_cpu, eff_avail_ram

    def _clamp(x, lo, hi):
        if x < lo:
            return lo
        if x > hi:
            return hi
        return x

    def _choose_cpu(pool, pid, oid):
        # Modest default CPU to keep more concurrency for high-priority latency,
        # but allow some scale-up on repeated scheduling of same op.
        base = max(1.0, pool.max_cpu_pool * s.cpu_min_frac)
        prev = s.op_cpu_hint.get((pid, oid), None)
        if prev is None:
            cpu = base
        else:
            cpu = min(pool.max_cpu_pool * s.cpu_max_frac, prev * s.cpu_step_mult)
        # Also don't request more than currently available (handled later), but cap to pool max here.
        cpu = _clamp(cpu, 1.0, pool.max_cpu_pool * s.cpu_max_frac)
        s.op_cpu_hint[(pid, oid)] = cpu
        return cpu

    def _choose_ram(pool, pid, oid, avail_ram):
        # Use learned hint if present, else start with a conservative fraction of pool RAM,
        # which helps avoid OOM penalties (720s) by giving more than bare minimum.
        hint = s.op_ram_hint.get((pid, oid), None)
        if hint is None:
            ram = max(1.0, pool.max_ram_pool * 0.35)
        else:
            ram = hint

        # Never exceed a safe fraction of pool RAM and don't exceed available RAM.
        ram = min(ram, pool.max_ram_pool * s.max_ram_frac)
        ram = min(ram, max(0.0, avail_ram))
        # Ensure at least 1.
        ram = max(1.0, ram)
        return ram

    def _effective_priority(p):
        # Base weights reflect objective weights: QUERY > INTERACTIVE > BATCH.
        # Aging boosts older pipelines to prevent starvation/incompletion penalties.
        age = s.tick - s.pipeline_arrival_tick.get(p.pipeline_id, s.tick)
        if p.priority == Priority.QUERY:
            base = 1000
            # Very mild aging; queries already dominate.
            return base + min(50, age // max(1, s.interactive_aging_ticks))
        if p.priority == Priority.INTERACTIVE:
            base = 600
            return base + min(100, age // max(1, s.batch_aging_ticks))
        base = 100
        return base + min(300, age // max(1, s.batch_aging_ticks))

    def _pick_next_pipeline():
        # Choose pipeline with highest effective priority that has a ready op.
        # To reduce per-tick cost, scan limited number from each queue.
        best = None
        best_score = None

        def scan(queue, max_scan=20):
            nonlocal best, best_score
            # We rotate by popping and appending to preserve FIFO-ish order within same class.
            n = min(len(queue), max_scan)
            for _ in range(n):
                p = queue.pop(0)
                if _is_done_or_failed(p):
                    continue
                if _get_ready_ops(p):
                    score = _effective_priority(p)
                    if best is None or score > best_score:
                        best = p
                        best_score = score
                # Keep it around regardless; it may become ready later.
                queue.append(p)

        scan(s.q_query, 30)
        scan(s.q_interactive, 25)
        scan(s.q_batch, 20)
        return best

    def _register_running_from_assignment(asg):
        # If container_id isn't known until results, we can't fully register here.
        # Still, we can note that something is scheduled by op hints; running is tracked on results.
        return

    def _select_preemption_candidate(pool_id):
        # Preempt a single lowest-priority running container in this pool.
        # Prefer BATCH > INTERACTIVE > QUERY (never preempt QUERY unless nothing else exists).
        candidate = None
        candidate_rank = None  # higher means more preemptable

        def rank(pri):
            if pri == Priority.BATCH_PIPELINE:
                return 3
            if pri == Priority.INTERACTIVE:
                return 2
            return 1  # QUERY

        for cid, meta in s.running.items():
            if meta.get("pool_id") != pool_id:
                continue
            r = rank(meta.get("priority"))
            if candidate is None or r > candidate_rank:
                candidate = cid
                candidate_rank = r

        # Never preempt query if any other exists; if only queries running, return None.
        if candidate is None:
            return None
        if candidate_rank <= 1:
            return None
        return candidate

    # ---------- Ingest new pipelines ----------
    for p in pipelines:
        _push_pipeline(p)

    # ---------- Process results: learn from OOMs and update running map ----------
    for r in results:
        # Track running containers from results (best-effort).
        if getattr(r, "container_id", None) is not None and getattr(r, "pool_id", None) is not None:
            # If it failed or completed, remove it from running.
            if r.failed() or (getattr(r, "error", None) is None and hasattr(r, "ops") and r.ops):
                # We don't know completion vs intermediate, but results are per-op execution.
                # Treat result event as end of that container's run.
                if r.container_id in s.running:
                    del s.running[r.container_id]
            else:
                # Mark as running if there's an update (rarely used).
                s.running[r.container_id] = {"priority": r.priority, "pool_id": r.pool_id}

        # Learn RAM needs on OOM (or any failure with "oom" in error string).
        if r.failed():
            err = str(getattr(r, "error", "") or "").lower()
            is_oom = ("oom" in err) or ("out of memory" in err) or ("memory" in err and "exceed" in err)
            if is_oom and hasattr(r, "ops") and r.ops:
                # We assume one op per assignment in our scheduler.
                op = r.ops[0]
                oid = _op_id(op)
                pid = getattr(r, "pipeline_id", None)
                # pipeline_id isn't guaranteed on result; infer via op if possible is not.
                # If pipeline_id isn't present, we can't key; fall back to container-based global hint.
                if pid is None:
                    # Use a coarse key; better than nothing.
                    pid = "unknown"

                prev = s.op_ram_hint.get((pid, oid), None)
                cur = float(getattr(r, "ram", 0.0) or 0.0)
                if cur <= 0.0:
                    continue
                bumped = cur * s.oom_backoff_mult + s.oom_backoff_add
                if prev is None:
                    s.op_ram_hint[(pid, oid)] = bumped
                else:
                    s.op_ram_hint[(pid, oid)] = max(prev, bumped)

    # Early exit
    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    # ---------- Scheduling per pool ----------
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool
        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # Try to place a few ops if resources are abundant; otherwise 1.
        # This helps throughput without hurting latency too much given headrooming and modest CPU.
        max_place = 3 if (avail_cpu > 0.6 * pool.max_cpu_pool and avail_ram > 0.6 * pool.max_ram_pool) else 1
        placed = 0

        while placed < max_place:
            eff_avail_cpu, eff_avail_ram = _pool_headroomed(avail_cpu, avail_ram, pool)
            if eff_avail_cpu <= 0 or eff_avail_ram <= 0:
                break

            p = _pick_next_pipeline()
            if p is None:
                break

            op_list = _get_ready_ops(p)
            if not op_list:
                break
            op = op_list[0]
            pid = p.pipeline_id
            oid = _op_id(op)

            # Choose resources.
            req_cpu = _choose_cpu(pool, pid, oid)
            req_ram = _choose_ram(pool, pid, oid, eff_avail_ram)

            # Clamp to what we can currently give (post-headroom).
            req_cpu = min(req_cpu, eff_avail_cpu)
            req_ram = min(req_ram, eff_avail_ram)

            # If it still doesn't fit, consider preemption only for high priorities.
            if req_cpu <= 0 or req_ram <= 0:
                break

            if req_cpu > avail_cpu or req_ram > avail_ram:
                # Can't fit in this pool right now (even without headroom).
                # Preempt only if this is QUERY/INTERACTIVE.
                if p.priority in (Priority.QUERY, Priority.INTERACTIVE):
                    cid = _select_preemption_candidate(pool_id)
                    if cid is not None:
                        suspensions.append(Suspend(cid, pool_id))
                        # Assume resources will free next tick; don't over-assign in same tick.
                        # This reduces churn and avoids optimistic overcommit.
                        break
                break

            # Make assignment.
            assignment = Assignment(
                ops=[op],
                cpu=req_cpu,
                ram=req_ram,
                priority=p.priority,
                pool_id=pool_id,
                pipeline_id=pipeline_id
            )
            assignments.append(assignment)
            _register_running_from_assignment(assignment)

            # Update local available resources pessimistically for this tick.
            avail_cpu -= req_cpu
            avail_ram -= req_ram
            placed += 1

    return suspensions, assignments
