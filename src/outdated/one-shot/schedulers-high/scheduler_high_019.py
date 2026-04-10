# policy_key: scheduler_high_019
# reasoning_effort: high
# model: gpt-5.2-2025-12-11
# llm_cost: 0.123423
# generation_seconds: 119.87
# generated_at: 2026-03-12T23:07:47.713255
@register_scheduler_init(key="scheduler_high_019")
def scheduler_high_019_init(s):
    """Priority-aware, latency-improving scheduler (incremental upgrade over naive FIFO).

    Main changes vs. naive:
      1) Priority-aware selection (QUERY > INTERACTIVE > BATCH), with simple aging to avoid starvation.
      2) Avoid "give the whole pool to one op": allocate modest per-op CPU/RAM slices so multiple ops
         can run concurrently, improving tail latency for high-priority arrivals.
      3) Simple OOM retry logic: if an op fails with an OOM-like error, double its RAM hint next time.
      4) Light pool isolation (if multiple pools exist): prefer pool 0 for QUERY/INTERACTIVE and other
         pools for BATCH, while still allowing stealing when idle.
    """
    # Logical time for aging. Incremented once per scheduler() call.
    s.now = 0

    # Pipelines we know about (by id), and their first-seen time (for aging).
    s.pipeline_by_id = {}          # pipeline_id -> Pipeline
    s.pipeline_arrival = {}        # pipeline_id -> int tick first seen
    s.active_pipeline_ids = []     # stable iteration order (arrival order)

    # Drop set for pipelines we decide to stop scheduling (e.g., repeated non-OOM failures).
    s.dropped_pipelines = set()

    # Per-operator adaptive hints (keyed by a stable-ish operator key).
    s.op_ram_hint = {}             # op_key -> ram to request next time
    s.op_cpu_hint = {}             # op_key -> cpu to request next time (not heavily used yet)
    s.op_attempts = {}             # op_key -> number of attempts observed (via results)

    # Retry / safety knobs.
    s.max_op_attempts = 4          # after this, we stop trying that operator (and thus its pipeline)

    # Default sizing knobs (small, conservative slices to increase parallelism and reduce HoL blocking).
    # These are *targets*; actual allocation is clamped by available pool resources.
    s.default_cpu_by_pri = {
        Priority.QUERY: 4.0,
        Priority.INTERACTIVE: 2.0,
        Priority.BATCH_PIPELINE: 1.0,
    }
    # RAM fractions of pool capacity by priority (helps avoid "one op grabs all RAM").
    s.default_ram_frac_by_pri = {
        Priority.QUERY: 0.25,           # start reasonably sized for fast p95
        Priority.INTERACTIVE: 0.18,
        Priority.BATCH_PIPELINE: 0.10,
    }

    # If multiple pools exist, treat pool 0 as "latency pool" by preference (soft isolation).
    s.latency_pool_id = 0


@register_scheduler(key="scheduler_high_019")
def scheduler_high_019_scheduler(s, results: List["ExecutionResult"],
                                pipelines: List["Pipeline"]) -> Tuple[List["Suspend"], List["Assignment"]]:
    # -------------------------
    # Helpers (kept local to avoid imports and keep policy self-contained)
    # -------------------------
    def _pri_rank(pri) -> int:
        # Higher number means higher priority.
        if pri == Priority.QUERY:
            return 3
        if pri == Priority.INTERACTIVE:
            return 2
        if pri == Priority.BATCH_PIPELINE:
            return 1
        return 0

    def _is_high(pri) -> bool:
        return pri in (Priority.QUERY, Priority.INTERACTIVE)

    def _op_key(op) -> str:
        # Best-effort stable key: try common attributes first; fall back to object id.
        for attr in ("op_id", "operator_id", "id", "name"):
            if hasattr(op, attr):
                try:
                    v = getattr(op, attr)
                    if v is not None:
                        return f"{attr}:{v}"
                except Exception:
                    pass
        return f"pyid:{id(op)}"

    def _is_oom_error(err) -> bool:
        if err is None:
            return False
        msg = str(err).upper()
        return ("OOM" in msg) or ("OUT OF MEMORY" in msg) or ("MEMORY" in msg and "ALLOC" in msg)

    def _clamp_positive(x, lo=1e-9):
        try:
            if x is None:
                return lo
            return x if x > lo else lo
        except Exception:
            return lo

    def _default_cpu_for(pri, pool_max_cpu, avail_cpu) -> float:
        # Small CPU slices improve concurrency; high priority gets more.
        target = float(s.default_cpu_by_pri.get(pri, 1.0))
        # Do not exceed a modest fraction of pool by default (keeps room for others).
        cap = max(1.0, 0.50 * float(pool_max_cpu))
        cpu = min(target, cap, float(avail_cpu))
        return _clamp_positive(cpu, lo=1.0)

    def _default_ram_for(pri, pool_max_ram, avail_ram) -> float:
        frac = float(s.default_ram_frac_by_pri.get(pri, 0.10))
        target = frac * float(pool_max_ram)
        # Keep it at least a small positive amount; exact units are simulator-defined.
        ram = min(target, float(avail_ram))
        return _clamp_positive(ram, lo=1.0)

    # -------------------------
    # Time / intake
    # -------------------------
    s.now += 1

    # Add new pipelines to our known set.
    for p in pipelines:
        pid = p.pipeline_id
        if pid in s.dropped_pipelines:
            continue
        if pid not in s.pipeline_by_id:
            s.pipeline_by_id[pid] = p
            s.pipeline_arrival[pid] = s.now
            s.active_pipeline_ids.append(pid)
        else:
            # Refresh reference in case the simulator provides updated objects.
            s.pipeline_by_id[pid] = p

    # -------------------------
    # Learn from recent execution results (OOM-driven RAM doubling; cap retries)
    # -------------------------
    if results:
        for r in results:
            # Update attempt counters for involved ops.
            try:
                ops = list(r.ops) if r.ops is not None else []
            except Exception:
                ops = []

            for op in ops:
                ok = _op_key(op)
                s.op_attempts[ok] = s.op_attempts.get(ok, 0) + (1 if getattr(r, "failed")() else 0)

                # On OOM, increase RAM hint aggressively (doubling).
                if getattr(r, "failed")() and _is_oom_error(getattr(r, "error", None)):
                    try:
                        pool = s.executor.pools[r.pool_id]
                        pool_max_ram = pool.max_ram_pool
                    except Exception:
                        pool_max_ram = None

                    prev = s.op_ram_hint.get(ok, None)
                    base = getattr(r, "ram", None)
                    # If we have neither previous hint nor previous allocation, start from a small floor.
                    start = 1.0
                    if prev is not None:
                        start = float(prev)
                    elif base is not None:
                        start = float(base)

                    new_hint = 2.0 * max(start, 1.0)
                    if pool_max_ram is not None:
                        new_hint = min(float(pool_max_ram), float(new_hint))
                    s.op_ram_hint[ok] = new_hint

                # Optionally: if we see repeated failures, consider nudging CPU down/up.
                # For now, keep CPU hint simple and conservative; RAM is the primary OOM lever.

    # -------------------------
    # Garbage collect completed (or "hopeless") pipelines from our active set
    # -------------------------
    # We cannot mutate the simulator pipeline state, so we simply stop considering them.
    still_active = []
    for pid in s.active_pipeline_ids:
        if pid in s.dropped_pipelines:
            continue
        p = s.pipeline_by_id.get(pid)
        if p is None:
            continue
        try:
            st = p.runtime_status()
            if st.is_pipeline_successful():
                continue
        except Exception:
            # If status can't be read, keep it for now.
            pass
        still_active.append(pid)
    s.active_pipeline_ids = still_active

    # -------------------------
    # Scheduling: priority + aging + modest resource slicing
    # -------------------------
    suspensions: List["Suspend"] = []
    assignments: List["Assignment"] = []

    if not pipelines and not results:
        # Nothing changed; do not recompute aggressively.
        # (Still safe to return empty; the simulator will call us again.)
        return suspensions, assignments

    # Determine if there exists any *ready* high-priority work (to bias pool 0 and limit batch there).
    high_ready_exists = False
    for pid in s.active_pipeline_ids:
        p = s.pipeline_by_id.get(pid)
        if p is None or not _is_high(getattr(p, "priority", None)):
            continue
        try:
            st = p.runtime_status()
            op_list = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
            if op_list:
                high_ready_exists = True
                break
        except Exception:
            continue

    # Avoid scheduling the same pipeline multiple times in the same tick (reduces burstiness).
    scheduled_this_tick = set()

    # Per-pool scheduling loop
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)

        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # Soft isolation logic:
        # - If multiple pools: pool 0 prefers high-priority; other pools prefer batch.
        # - If a pool has no preferred work, it may "steal" other work to avoid idling.
        multi_pool = s.executor.num_pools > 1
        prefer_high = (not multi_pool) or (pool_id == s.latency_pool_id)
        prefer_batch = multi_pool and (pool_id != s.latency_pool_id)

        # If high-priority ready exists, avoid scheduling batch on the latency pool
        # (simple latency protection without preemption).
        disallow_batch_here = multi_pool and (pool_id == s.latency_pool_id) and high_ready_exists

        # Cap the number of new assignments per pool per tick to limit churn/overhead.
        # Still allows filling large pools via multiple iterations.
        max_assignments_this_pool = max(1, int(pool.max_cpu_pool))  # heuristic bound

        made = 0
        while made < max_assignments_this_pool and avail_cpu > 0 and avail_ram > 0:
            best = None  # (score, pid, op)
            best_req = None  # (cpu, ram)

            # Two-pass selection: first try preferred class, then allow stealing.
            for pass_idx in (0, 1):
                for pid in s.active_pipeline_ids:
                    if pid in s.dropped_pipelines or pid in scheduled_this_tick:
                        continue
                    p = s.pipeline_by_id.get(pid)
                    if p is None:
                        continue

                    pri = getattr(p, "priority", None)

                    # Class filtering per pool and pass.
                    if disallow_batch_here and pri == Priority.BATCH_PIPELINE:
                        continue
                    if multi_pool:
                        if pass_idx == 0:
                            if prefer_high and not _is_high(pri):
                                continue
                            if prefer_batch and pri != Priority.BATCH_PIPELINE:
                                continue
                        # pass_idx == 1: allow stealing (no class filter)

                    # Find one assignable op.
                    try:
                        st = p.runtime_status()
                        if st.is_pipeline_successful():
                            continue
                        op_list = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
                    except Exception:
                        continue
                    if not op_list:
                        continue
                    op = op_list[0]
                    ok = _op_key(op)

                    # If this op keeps failing, drop the pipeline to avoid infinite looping.
                    if s.op_attempts.get(ok, 0) >= s.max_op_attempts:
                        s.dropped_pipelines.add(pid)
                        continue

                    # Decide resource request for this op.
                    cpu_req = s.op_cpu_hint.get(ok, None)
                    if cpu_req is None:
                        cpu_req = _default_cpu_for(pri, pool.max_cpu_pool, avail_cpu)
                    else:
                        cpu_req = min(float(cpu_req), float(avail_cpu))
                        cpu_req = _clamp_positive(cpu_req, lo=1.0)

                    ram_req = s.op_ram_hint.get(ok, None)
                    if ram_req is None:
                        ram_req = _default_ram_for(pri, pool.max_ram_pool, avail_ram)
                    else:
                        ram_req = float(ram_req)
                        ram_req = _clamp_positive(ram_req, lo=1.0)

                    # Fit check. We can always clamp CPU down, but RAM must fit as-is to be meaningful.
                    if ram_req > avail_ram:
                        continue
                    if cpu_req > avail_cpu:
                        cpu_req = float(avail_cpu)
                        if cpu_req < 1.0:
                            continue

                    # Score: strict priority dominates, then aging.
                    arrived = s.pipeline_arrival.get(pid, s.now)
                    wait = max(0, s.now - arrived)
                    score = (_pri_rank(pri) * 1_000_000) + wait

                    # Slightly prefer QUERY over INTERACTIVE when equal rank isn't enough:
                    # (already handled by rank; keep it simple)
                    if best is None or score > best[0]:
                        best = (score, pid, op)
                        best_req = (cpu_req, ram_req)

                if best is not None:
                    break  # found something this pass; schedule it

            if best is None:
                break

            _, pid, op = best
            cpu_req, ram_req = best_req
            p = s.pipeline_by_id[pid]
            pri = p.priority

            # Create the assignment (single-op containers; avoids needing per-op packing in one container).
            assignment = Assignment(
                ops=[op],
                cpu=cpu_req,
                ram=ram_req,
                priority=pri,
                pool_id=pool_id,
                pipeline_id=pid,
            )
            assignments.append(assignment)

            # Update local available resources for further scheduling in this pool this tick.
            avail_cpu -= float(cpu_req)
            avail_ram -= float(ram_req)
            made += 1
            scheduled_this_tick.add(pid)

    return suspensions, assignments
