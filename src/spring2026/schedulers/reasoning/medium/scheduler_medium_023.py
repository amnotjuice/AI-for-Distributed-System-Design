# policy_key: scheduler_medium_023
# reasoning_effort: medium
# model: gpt-5.2-2025-12-11
# llm_cost: 0.087950
# generation_seconds: 89.08
# generated_at: 2026-03-14T02:45:35.023451
@register_scheduler_init(key="scheduler_medium_023")
def scheduler_medium_023_init(s):
    """Priority-aware, right-sized, multi-assignment scheduler.

    Incremental improvements over naive FIFO:
      1) Strict priority ordering (QUERY > INTERACTIVE > BATCH_PIPELINE) to reduce tail latency.
      2) Round-robin within each priority to avoid starvation within a class.
      3) Avoid allocating the entire pool to one op by using per-priority CPU/RAM caps,
         enabling multiple concurrent ops per pool when possible.
      4) Simple OOM-aware retries: if an op fails with an OOM-like error, increase its RAM hint
         and retry (instead of dropping the whole pipeline).
      5) Soft reservation: when high-priority work is waiting, prevent batch from consuming
         the last fraction of pool resources.
    """
    # Per-priority queues of pipelines (RR within each priority).
    s.queues = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }
    s.rr_index = {
        Priority.QUERY: 0,
        Priority.INTERACTIVE: 0,
        Priority.BATCH_PIPELINE: 0,
    }

    # Track pipelines that should be dropped due to non-OOM failures.
    s.dead_pipelines = set()

    # Per-op sizing hints learned from failures (primarily OOM).
    # key -> {"ram": float, "cpu": float, "oom_retries": int}
    s.op_hints = {}

    # Configuration knobs (kept simple and conservative).
    s.max_oom_retries = 3

    # Caps used to avoid giving one op the entire pool by default.
    # Hints can override these (e.g., after OOM we may exceed cap for RAM).
    s.cpu_cap_frac = {
        Priority.QUERY: 0.75,
        Priority.INTERACTIVE: 0.60,
        Priority.BATCH_PIPELINE: 0.40,
    }
    s.ram_cap_frac = {
        Priority.QUERY: 0.75,
        Priority.INTERACTIVE: 0.60,
        Priority.BATCH_PIPELINE: 0.40,
    }

    # Soft reservation for high-priority arrivals: batch can only use resources above this.
    s.reserve_for_high_cpu_frac = 0.20
    s.reserve_for_high_ram_frac = 0.20


@register_scheduler(key="scheduler_medium_023")
def scheduler_medium_023_scheduler(s, results, pipelines):
    """Scheduler step: assign ready operators to pools with priority-aware RR + OOM-aware RAM growth."""
    # ---- helpers (kept local to avoid top-level imports) ----
    def _is_oom_error(err):
        if not err:
            return False
        try:
            msg = str(err).lower()
        except Exception:
            return False
        return ("oom" in msg) or ("out of memory" in msg) or ("out-of-memory" in msg) or ("memoryerror" in msg)

    def _prio_order():
        # Highest to lowest.
        return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]

    def _op_key(pipeline_id, op):
        # Try to use stable identifiers when present; fall back to repr(op).
        for attr in ("op_id", "operator_id", "id", "name"):
            try:
                if hasattr(op, attr):
                    v = getattr(op, attr)
                    # Avoid including callables or huge objects.
                    if isinstance(v, (int, float, str)):
                        return (pipeline_id, attr, v)
            except Exception:
                pass
        try:
            return (pipeline_id, "repr", repr(op))
        except Exception:
            return (pipeline_id, "pyid", id(op))

    def _get_hint(pipeline_id, op):
        return s.op_hints.get(_op_key(pipeline_id, op))

    def _set_hint(pipeline_id, op, cpu=None, ram=None, oom_retries=None):
        k = _op_key(pipeline_id, op)
        cur = s.op_hints.get(k, {"cpu": None, "ram": None, "oom_retries": 0})
        if cpu is not None:
            cur["cpu"] = cpu
        if ram is not None:
            cur["ram"] = ram
        if oom_retries is not None:
            cur["oom_retries"] = oom_retries
        s.op_hints[k] = cur

    def _effective_cap(val, frac):
        # Preserve type (int/float) as much as possible.
        try:
            return val * frac
        except Exception:
            return val

    def _min_positive(x, fallback=1):
        try:
            if x is None:
                return fallback
            if x <= 0:
                return fallback
            return x
        except Exception:
            return fallback

    def _pop_rr(queue, rr_idx):
        # Pop one element using round-robin index.
        if not queue:
            return None, rr_idx
        rr_idx = rr_idx % len(queue)
        p = queue.pop(rr_idx)
        # Next time, continue from same index (since list shrank).
        rr_idx = rr_idx % max(1, len(queue))
        return p, rr_idx

    def _append_rr(queue, pipeline):
        queue.append(pipeline)

    def _pipeline_done_or_dead(pipeline):
        if pipeline.pipeline_id in s.dead_pipelines:
            return True
        st = pipeline.runtime_status()
        if st.is_pipeline_successful():
            return True
        return False

    def _first_ready_op(pipeline):
        st = pipeline.runtime_status()
        # Allow retry of FAILED ops (ASSIGNABLE_STATES includes FAILED), but only if we didn't mark pipeline dead.
        op_list = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
        if not op_list:
            return None
        return op_list[0]

    # ---- ingest new pipelines ----
    for p in pipelines:
        # Unknown priorities are treated as lowest.
        pr = getattr(p, "priority", Priority.BATCH_PIPELINE)
        if pr not in s.queues:
            pr = Priority.BATCH_PIPELINE
        s.queues[pr].append(p)

    # Early exit if nothing could change our decisions.
    if not pipelines and not results:
        return [], []

    # ---- learn from results (OOM-aware RAM increase, drop non-OOM failures) ----
    for r in results:
        try:
            if not r.failed():
                continue
        except Exception:
            # If we cannot determine, ignore.
            continue

        # If any op in this result OOMs, treat as OOM; otherwise mark pipeline dead.
        oom = _is_oom_error(getattr(r, "error", None))

        # Best-effort pipeline_id extraction: ExecutionResult may not include it.
        # We can still grow hint keyed by op + "unknown pipeline id" if needed,
        # but prefer to key by actual pipeline id when available.
        pipeline_id = getattr(r, "pipeline_id", None)

        # If pipeline_id isn't available, fall back to using op-based keys with pipeline_id=None.
        if pipeline_id is None:
            pipeline_id = None

        if oom:
            # Increase RAM hint for each op in the result.
            for op in getattr(r, "ops", []) or []:
                hint = _get_hint(pipeline_id, op) or {"cpu": None, "ram": None, "oom_retries": 0}
                if hint.get("oom_retries", 0) >= s.max_oom_retries:
                    # Too many OOM retries -> mark dead to prevent infinite loops.
                    # If we don't have pipeline_id, we cannot mark a specific pipeline dead.
                    if pipeline_id is not None:
                        s.dead_pipelines.add(pipeline_id)
                    continue

                prev_ram = hint.get("ram")
                # Use last allocated RAM as baseline if present; otherwise use result.ram; otherwise leave None.
                base_ram = prev_ram
                if base_ram is None:
                    base_ram = getattr(r, "ram", None)

                # Grow RAM aggressively on OOM, but keep it as a numeric type.
                try:
                    new_ram = base_ram * 2 if base_ram is not None else None
                except Exception:
                    new_ram = base_ram

                _set_hint(
                    pipeline_id,
                    op,
                    cpu=hint.get("cpu", getattr(r, "cpu", None)),
                    ram=new_ram,
                    oom_retries=hint.get("oom_retries", 0) + 1,
                )
        else:
            # Non-OOM failure: drop pipeline to avoid thrashing.
            if pipeline_id is not None:
                s.dead_pipelines.add(pipeline_id)

    # ---- scheduling ----
    suspensions = []
    assignments = []

    high_waiting = (len(s.queues[Priority.QUERY]) > 0) or (len(s.queues[Priority.INTERACTIVE]) > 0)

    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = getattr(pool, "avail_cpu_pool", 0)
        avail_ram = getattr(pool, "avail_ram_pool", 0)
        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # For batch scheduling, keep a soft reserve if high-priority work is waiting.
        reserve_cpu = _effective_cap(getattr(pool, "max_cpu_pool", avail_cpu), s.reserve_for_high_cpu_frac) if high_waiting else 0
        reserve_ram = _effective_cap(getattr(pool, "max_ram_pool", avail_ram), s.reserve_for_high_ram_frac) if high_waiting else 0

        # Try to place multiple ops in a pool in one tick using local accounting.
        while avail_cpu > 0 and avail_ram > 0:
            made_assignment = False

            for pr in _prio_order():
                q = s.queues.get(pr, [])
                if not q:
                    continue

                # Batch should not consume the soft reserve when high-priority is waiting.
                eff_avail_cpu = avail_cpu
                eff_avail_ram = avail_ram
                if pr == Priority.BATCH_PIPELINE and high_waiting:
                    try:
                        eff_avail_cpu = max(0, avail_cpu - reserve_cpu)
                    except Exception:
                        pass
                    try:
                        eff_avail_ram = max(0, avail_ram - reserve_ram)
                    except Exception:
                        pass
                    if eff_avail_cpu <= 0 or eff_avail_ram <= 0:
                        continue

                # Scan up to len(q) pipelines (RR) to find a ready op that fits.
                scans = len(q)
                while scans > 0 and q and eff_avail_cpu > 0 and eff_avail_ram > 0:
                    pipeline, s.rr_index[pr] = _pop_rr(q, s.rr_index[pr])
                    scans -= 1
                    if pipeline is None:
                        break

                    # Drop completed/dead pipelines.
                    if pipeline.pipeline_id in s.dead_pipelines:
                        continue
                    st = pipeline.runtime_status()
                    if st.is_pipeline_successful():
                        continue

                    op = _first_ready_op(pipeline)
                    if op is None:
                        # Not ready yet; put it back for later.
                        _append_rr(q, pipeline)
                        continue

                    # Compute default per-priority caps to avoid giving one op the entire pool.
                    max_cpu = getattr(pool, "max_cpu_pool", avail_cpu)
                    max_ram = getattr(pool, "max_ram_pool", avail_ram)

                    cap_cpu = _effective_cap(max_cpu, s.cpu_cap_frac.get(pr, 0.4))
                    cap_ram = _effective_cap(max_ram, s.ram_cap_frac.get(pr, 0.4))

                    # Apply hints (primarily RAM after OOM). Allow hint to exceed cap (up to pool max).
                    hint = _get_hint(pipeline.pipeline_id, op) or _get_hint(None, op)  # best effort
                    hint_cpu = hint.get("cpu") if hint else None
                    hint_ram = hint.get("ram") if hint else None

                    # Choose request sizes:
                    # - CPU: cap by default; use hint if it's smaller (conservative) or if cap is tiny.
                    # - RAM: take max(cap, hint) because OOM hints indicate minimum needed.
                    try:
                        req_cpu = cap_cpu
                        if hint_cpu is not None:
                            # Do not blindly increase CPU from hints; keep it conservative.
                            req_cpu = min(req_cpu, hint_cpu) if hint_cpu > 0 else req_cpu
                    except Exception:
                        req_cpu = cap_cpu

                    try:
                        req_ram = cap_ram
                        if hint_ram is not None:
                            # If we learned we need more RAM, honor it (up to pool max).
                            req_ram = max(req_ram, hint_ram)
                    except Exception:
                        req_ram = cap_ram

                    # Clamp to what this pool can provide now.
                    try:
                        req_cpu = min(req_cpu, eff_avail_cpu)
                    except Exception:
                        pass
                    try:
                        req_ram = min(req_ram, eff_avail_ram)
                    except Exception:
                        pass

                    # Enforce minimal positive allocations.
                    req_cpu = _min_positive(req_cpu, fallback=1)
                    req_ram = _min_positive(req_ram, fallback=1)

                    # Ensure the request fits; otherwise, requeue and try next pipeline.
                    if req_cpu > eff_avail_cpu or req_ram > eff_avail_ram:
                        _append_rr(q, pipeline)
                        continue

                    assignments.append(
                        Assignment(
                            ops=[op],
                            cpu=req_cpu,
                            ram=req_ram,
                            priority=pipeline.priority,
                            pool_id=pool_id,
                            pipeline_id=pipeline.pipeline_id,
                        )
                    )

                    # Local accounting so we can place more ops in this pool this tick.
                    try:
                        avail_cpu -= req_cpu
                    except Exception:
                        avail_cpu = 0
                    try:
                        avail_ram -= req_ram
                    except Exception:
                        avail_ram = 0

                    # Keep pipeline in the queue for subsequent ops (RR fairness).
                    _append_rr(q, pipeline)

                    made_assignment = True
                    break  # stop scanning this priority; go back to outer while to re-evaluate avail + priorities

                if made_assignment:
                    break

            if not made_assignment:
                break

    return suspensions, assignments
