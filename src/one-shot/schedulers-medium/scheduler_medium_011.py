# policy_key: scheduler_medium_011
# reasoning_effort: medium
# model: gpt-5.2-2025-12-11
# llm_cost: 0.071399
# generation_seconds: 134.64
# generated_at: 2026-03-12T22:08:16.717341
@register_scheduler_init(key="scheduler_medium_011")
def scheduler_medium_011_init(s):
    """Priority-aware, OOM-adaptive round-robin scheduler.

    Incremental improvements over naive FIFO:
      1) Fix head-of-line blocking / stalling by always attempting scheduling if queues are non-empty.
      2) Use explicit priority queues (QUERY/INTERACTIVE ahead of BATCH).
      3) Right-size allocations (don't hand entire pool to one op) to improve tail latency under contention.
      4) React to OOM by learning per-operator RAM estimates and retrying with larger RAM.
      5) Add simple anti-starvation for batch via aging-based "batch escape hatch".
      6) Best-effort preemption hook (only if the simulator exposes running container metadata).
    """
    # Per-priority FIFO queues of pipelines (round-robin within each priority)
    s.queues = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }

    # Arrival time for aging / fairness
    s.arrival_tick_by_pipeline = {}

    # Learned RAM estimates per operator "key" (based on OOM + successes)
    s.ram_est_by_opkey = {}          # opkey -> ram
    s.retries_by_opkey = {}          # opkey -> count
    s.oom_recent_until_by_opkey = {} # opkey -> tick_until (TTL)

    # Timekeeping
    s.tick = 0

    # Tuning knobs (kept conservative / simple)
    s.oom_ttl_ticks = 50
    s.max_oom_retries_per_op = 3
    s.max_assignments_per_pool = 2

    # Resource shaping by priority (fractions of pool max)
    s.cpu_frac = {
        Priority.QUERY: 0.60,
        Priority.INTERACTIVE: 0.50,
        Priority.BATCH_PIPELINE: 0.35,
    }
    s.ram_frac = {
        Priority.QUERY: 0.35,
        Priority.INTERACTIVE: 0.30,
        Priority.BATCH_PIPELINE: 0.45,
    }

    # Batch anti-starvation
    s.batch_starvation_ticks = 200
    s.batch_min_interval_ticks = 10
    s.last_batch_scheduled_tick = -10**9


@register_scheduler(key="scheduler_medium_011")
def scheduler_medium_011(s, results: List["ExecutionResult"], pipelines: List["Pipeline"]) -> Tuple[List["Suspend"], List["Assignment"]]:
    """Run one deterministic scheduling step."""
    def _prio_order():
        # Treat QUERY + INTERACTIVE as latency-sensitive; keep stable ordering.
        return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]

    def _is_oom_error(err) -> bool:
        if not err:
            return False
        try:
            msg = str(err).lower()
        except Exception:
            return False
        return ("oom" in msg) or ("out of memory" in msg) or ("out-of-memory" in msg) or ("memoryerror" in msg)

    def _op_key(op) -> str:
        # Best-effort stable identifier across ticks.
        for attr in ("op_id", "operator_id", "id", "name"):
            try:
                v = getattr(op, attr, None)
                if v is not None:
                    return f"{attr}:{v}"
            except Exception:
                pass
        try:
            return f"repr:{repr(op)}"
        except Exception:
            return f"pyid:{id(op)}"

    def _get_op_min_ram(op):
        # If simulator exposes a minimum RAM requirement on operators, use it.
        for attr in ("min_ram", "ram_min", "min_memory", "memory_min", "ram_requirement"):
            try:
                v = getattr(op, attr, None)
                if v is not None:
                    return float(v)
            except Exception:
                pass
        return None

    def _clamp(x, lo, hi):
        if x < lo:
            return lo
        if x > hi:
            return hi
        return x

    def _clean_oom_ttls():
        # Drop expired TTL markers to avoid unbounded growth.
        expired = []
        for k, until in s.oom_recent_until_by_opkey.items():
            if until <= s.tick:
                expired.append(k)
        for k in expired:
            try:
                del s.oom_recent_until_by_opkey[k]
            except Exception:
                pass

    def _pipeline_done_or_hard_failed(p) -> bool:
        st = p.runtime_status()
        if st.is_pipeline_successful():
            return True

        # If there are FAILED ops, only keep the pipeline if those failures look OOM-retryable.
        try:
            failed_ops = st.get_ops([OperatorState.FAILED], require_parents_complete=False)
        except Exception:
            failed_ops = []

        if not failed_ops:
            return False

        # Retry only if at least one FAILED op is within our recent OOM TTL marker
        # AND it has not exceeded retry budget.
        retryable = False
        for op in failed_ops:
            k = _op_key(op)
            if k in s.oom_recent_until_by_opkey:
                if s.retries_by_opkey.get(k, 0) <= s.max_oom_retries_per_op:
                    retryable = True
                    break

        return not retryable

    def _get_first_ready_op(p):
        st = p.runtime_status()
        try:
            ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        except Exception:
            return None
        if not ops:
            return None
        return ops[0]

    def _batch_is_starving() -> bool:
        # Find the oldest batch pipeline (by arrival) that isn't done/hard-failed.
        oldest = None
        for p in s.queues[Priority.BATCH_PIPELINE]:
            if _pipeline_done_or_hard_failed(p):
                continue
            t0 = s.arrival_tick_by_pipeline.get(p.pipeline_id, s.tick)
            if oldest is None or t0 < oldest:
                oldest = t0
        if oldest is None:
            return False
        return (s.tick - oldest) >= s.batch_starvation_ticks

    def _should_allow_batch_even_if_high_waiting() -> bool:
        if _batch_is_starving():
            return True
        # Also allow a periodic batch "escape" to maintain progress.
        return (s.tick - s.last_batch_scheduled_tick) >= s.batch_min_interval_ticks

    def _pick_pipeline_for_priority(prio):
        # Round-robin scan: pick the first pipeline that has a ready op.
        q = s.queues[prio]
        if not q:
            return None, None

        n = len(q)
        for _ in range(n):
            p = q.pop(0)

            if _pipeline_done_or_hard_failed(p):
                # drop completed or non-retryable failed pipelines
                continue

            op = _get_first_ready_op(p)
            q.append(p)  # keep it in RR set
            if op is not None:
                return p, op

        return None, None

    def _choose_request(pool, pprio, op, avail_cpu, avail_ram, high_waiting: bool):
        # Determine CPU request
        cpu_target = max(1.0, float(getattr(pool, "max_cpu_pool", avail_cpu)) * float(s.cpu_frac.get(pprio, 0.4)))
        cpu_req = _clamp(cpu_target, 1.0, float(avail_cpu))

        # If we're placing batch while high-priority is waiting, be more conservative with CPU share.
        if pprio == Priority.BATCH_PIPELINE and high_waiting:
            cpu_req = _clamp(cpu_req, 1.0, max(1.0, float(avail_cpu) * 0.50))

        # Determine RAM request: learned estimate > operator min > fraction of pool max
        opk = _op_key(op)
        learned = s.ram_est_by_opkey.get(opk, None)
        op_min = _get_op_min_ram(op)
        pool_max_ram = float(getattr(pool, "max_ram_pool", avail_ram))

        base = pool_max_ram * float(s.ram_frac.get(pprio, 0.35))
        ram_target = base
        if op_min is not None:
            ram_target = max(ram_target, float(op_min) * 1.10)  # small headroom
        if learned is not None:
            ram_target = max(ram_target, float(learned))

        ram_req = _clamp(ram_target, 1.0, float(avail_ram))

        # If we're placing batch while high-priority is waiting, avoid grabbing almost all RAM.
        if pprio == Priority.BATCH_PIPELINE and high_waiting:
            ram_req = _clamp(ram_req, 1.0, max(1.0, float(avail_ram) * 0.70))

        return cpu_req, ram_req

    def _get_pool_running_containers(pool):
        # Best-effort introspection: different simulator versions may expose different attributes.
        for attr in ("containers", "running_containers", "active_containers", "live_containers"):
            try:
                v = getattr(pool, attr, None)
                if v:
                    return list(v)
            except Exception:
                pass
        return []

    def _container_priority(c):
        try:
            return getattr(c, "priority", None)
        except Exception:
            return None

    def _container_id(c):
        for attr in ("container_id", "id"):
            try:
                v = getattr(c, attr, None)
                if v is not None:
                    return v
            except Exception:
                pass
        return None

    def _maybe_preempt_for_high_prio(pool_id, incoming_prio):
        # Preempt a single lowest-priority container if metadata is available.
        pool = s.executor.pools[pool_id]
        containers = _get_pool_running_containers(pool)
        if not containers:
            return None

        # Only preempt when incoming is latency-sensitive.
        if incoming_prio not in (Priority.QUERY, Priority.INTERACTIVE):
            return None

        # Choose a batch container first; if none, choose interactive when incoming is QUERY.
        def _rank(c):
            pr = _container_priority(c)
            # Lower value = more preemptable
            if pr == Priority.BATCH_PIPELINE:
                return 0
            if pr == Priority.INTERACTIVE and incoming_prio == Priority.QUERY:
                return 1
            return 2

        containers_sorted = sorted(containers, key=_rank)
        victim = containers_sorted[0]
        if _rank(victim) >= 2:
            return None

        cid = _container_id(victim)
        if cid is None:
            return None
        return Suspend(container_id=cid, pool_id=pool_id)

    # ---- Tick bookkeeping ----
    s.tick += 1

    # Ingest new pipelines
    for p in pipelines:
        try:
            pr = p.priority
        except Exception:
            pr = Priority.BATCH_PIPELINE
        if pr not in s.queues:
            # Unknown priority -> treat as batch
            pr = Priority.BATCH_PIPELINE
        s.queues[pr].append(p)
        if getattr(p, "pipeline_id", None) is not None:
            s.arrival_tick_by_pipeline.setdefault(p.pipeline_id, s.tick)

    # Process results to learn RAM + mark OOM retryability
    for r in results:
        if not getattr(r, "failed", lambda: False)():
            # Success: softly update RAM estimate downwards (keep conservative floor).
            try:
                for op in getattr(r, "ops", []) or []:
                    k = _op_key(op)
                    prev = s.ram_est_by_opkey.get(k, None)
                    if prev is None:
                        s.ram_est_by_opkey[k] = float(getattr(r, "ram", 0.0) or 0.0)
                    else:
                        # EMA with bias towards previous (avoid shrinking too aggressively).
                        s.ram_est_by_opkey[k] = 0.85 * float(prev) + 0.15 * float(getattr(r, "ram", prev) or prev)
            except Exception:
                pass
            continue

        # Failure path
        if _is_oom_error(getattr(r, "error", None)):
            try:
                alloc_ram = float(getattr(r, "ram", 0.0) or 0.0)
                for op in getattr(r, "ops", []) or []:
                    k = _op_key(op)
                    s.oom_recent_until_by_opkey[k] = s.tick + s.oom_ttl_ticks
                    s.retries_by_opkey[k] = s.retries_by_opkey.get(k, 0) + 1
                    # Exponential backoff on RAM estimate, bounded by "something sensible"
                    prev = s.ram_est_by_opkey.get(k, None)
                    bumped = max(alloc_ram * 2.0, (prev * 1.7) if prev is not None else 0.0, 1.0)
                    s.ram_est_by_opkey[k] = bumped
            except Exception:
                pass

    _clean_oom_ttls()

    suspensions: List["Suspend"] = []
    assignments: List["Assignment"] = []

    # Determine if high priority work exists (that is not done/hard-failed)
    def _has_waiting(prio):
        for p in s.queues.get(prio, []):
            if not _pipeline_done_or_hard_failed(p) and _get_first_ready_op(p) is not None:
                return True
        return False

    high_waiting = _has_waiting(Priority.QUERY) or _has_waiting(Priority.INTERACTIVE)

    # If high priority is waiting, only schedule batch if starving or periodic escape hatch.
    allow_batch = (not high_waiting) or _should_allow_batch_even_if_high_waiting()

    # Schedule per pool; attempt a small number of assignments per pool
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = float(getattr(pool, "avail_cpu_pool", 0.0) or 0.0)
        avail_ram = float(getattr(pool, "avail_ram_pool", 0.0) or 0.0)

        if avail_cpu <= 0.0 or avail_ram <= 0.0:
            continue

        made = 0
        while made < s.max_assignments_per_pool:
            # Recompute availability pessimistically based on what we assign in this tick.
            if avail_cpu <= 0.0 or avail_ram <= 0.0:
                break

            picked_p = None
            picked_op = None
            picked_prio = None

            # Priority-first selection
            for prio in _prio_order():
                if prio == Priority.BATCH_PIPELINE and not allow_batch:
                    continue
                p, op = _pick_pipeline_for_priority(prio)
                if p is not None and op is not None:
                    picked_p, picked_op, picked_prio = p, op, prio
                    break

            if picked_p is None:
                break

            cpu_req, ram_req = _choose_request(pool, picked_prio, picked_op, avail_cpu, avail_ram, high_waiting=high_waiting)

            # If we can't allocate even minimal resources, consider preemption only for high priority.
            if cpu_req <= 0.0 or ram_req <= 0.0:
                break
            if cpu_req > avail_cpu or ram_req > avail_ram:
                pre = _maybe_preempt_for_high_prio(pool_id, picked_prio)
                if pre is not None:
                    suspensions.append(pre)
                break

            # Issue assignment (one op per container to reduce interference + improve latency predictability)
            assignments.append(
                Assignment(
                    ops=[picked_op],
                    cpu=cpu_req,
                    ram=ram_req,
                    priority=picked_prio,
                    pool_id=pool_id,
                    pipeline_id=picked_p.pipeline_id,
                )
            )

            # Bookkeeping for fairness tracking
            if picked_prio == Priority.BATCH_PIPELINE:
                s.last_batch_scheduled_tick = s.tick

            # Decrement local avail; simulator will compute actual pool availability next tick
            avail_cpu -= float(cpu_req)
            avail_ram -= float(ram_req)
            made += 1

            # Re-evaluate high_waiting after scheduling something, but keep simple:
            # if we scheduled high prio, keep prioritizing it in the same tick by leaving allow_batch as-is.

    return suspensions, assignments
