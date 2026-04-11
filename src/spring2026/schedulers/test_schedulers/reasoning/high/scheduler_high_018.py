# policy_key: scheduler_high_018
# reasoning_effort: high
# exp: reasoning
# model: gpt-5.2-2025-12-11
# llm_cost: 0.110303
# generation_seconds: 192.00
# generated_at: 2026-04-10T01:09:52.872235
@register_scheduler_init(key="scheduler_high_018")
def scheduler_high_018_init(s):
    """Priority-aware weighted fair scheduler with RAM-safe retries.

    Goals:
    - Keep QUERY/INTERACTIVE latency low (dominant score weights) via priority + reservations.
    - Avoid permanent failures by retrying OOM with increased RAM hints (per-operator).
    - Prevent batch starvation using weighted-fair credits and limited backfill.
    """
    # Global tick counter (sim time steps in the simulator callback cadence)
    s._tick = 0

    # Per-priority round-robin queues of live pipelines
    s._queues = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }
    s._known_pipeline_ids = set()
    s._pipeline_arrival_tick = {}
    s._pipeline_last_pool = {}  # for soft stickiness

    # Weighted-fair credits (higher weight => more frequent service)
    s._credits = {
        Priority.QUERY: 0.0,
        Priority.INTERACTIVE: 0.0,
        Priority.BATCH_PIPELINE: 0.0,
    }

    # Per-(pipeline, op) resource hints and failure tracking
    # op key is derived from op identity; stable within a simulation run.
    s._op_ram_hint = {}   # (pid, op_key) -> ram
    s._op_cpu_hint = {}   # (pid, op_key) -> cpu
    s._op_fail = {}       # (pid, op_key) -> dict(total=int, oom=int, non_oom=int)

    # Map operator object identity to pipeline id (filled on assignment)
    s._op_to_pid = {}     # id(op) -> pid

    # Pipelines we intentionally stop retrying (unrecoverable / too many failures)
    s._abandoned_pipelines = set()

    # Tunables (conservative, incremental improvements over FIFO)
    s._weights = {
        Priority.QUERY: 10.0,
        Priority.INTERACTIVE: 5.0,
        Priority.BATCH_PIPELINE: 1.0,
    }
    s._total_weight = sum(s._weights.values())

    # Credits added per scheduler call; scaling > 1 lets us place multiple ops per tick without going negative too fast.
    s._credit_scale = 4.0
    s._credit_cap_mult = 6.0  # cap credits to avoid huge bursts after idling

    # Protect headroom for high-priority arrivals: avoid filling pools with batch when high-priority is queued.
    s._reserve_cpu_frac = 0.20
    s._reserve_ram_frac = 0.20

    # Default per-priority "probe" sizing (RAM is a hint; allocating below true peak risks OOM)
    s._base_ram_frac = {
        Priority.QUERY: 0.30,
        Priority.INTERACTIVE: 0.25,
        Priority.BATCH_PIPELINE: 0.18,
    }
    s._max_ram_frac = {
        Priority.QUERY: 0.60,
        Priority.INTERACTIVE: 0.55,
        Priority.BATCH_PIPELINE: 0.45,
    }

    # Default CPU sizing: sublinear scaling implies modest caps are often best for latency/throughput balance.
    s._cpu_cap = {
        Priority.QUERY: 8.0,
        Priority.INTERACTIVE: 4.0,
        Priority.BATCH_PIPELINE: 2.0,
    }
    s._cpu_frac = {
        Priority.QUERY: 0.50,
        Priority.INTERACTIVE: 0.33,
        Priority.BATCH_PIPELINE: 0.25,
    }

    # Retry policy
    s._max_total_retries = {
        Priority.QUERY: 5,
        Priority.INTERACTIVE: 5,
        Priority.BATCH_PIPELINE: 6,
    }
    s._max_non_oom_retries = {
        Priority.QUERY: 2,
        Priority.INTERACTIVE: 2,
        Priority.BATCH_PIPELINE: 2,
    }
    s._oom_growth = 1.75  # multiply RAM hint on OOM
    s._oom_min_mult = 2.0  # ensure at least 2x last allocated RAM on OOM
    s._fit_frac_required = {
        Priority.QUERY: 0.85,
        Priority.INTERACTIVE: 0.80,
        Priority.BATCH_PIPELINE: 0.60,
    }

    # Limit per-pool scheduling burst each tick to reduce thrash
    s._max_assignments_per_pool_per_tick = 6


@register_scheduler(key="scheduler_high_018")
def scheduler_high_018(s, results: List[ExecutionResult], pipelines: List[Pipeline]) -> Tuple[List[Suspend], List[Assignment]]:
    """Weighted fair, priority-first scheduling with OOM-aware RAM hinting and batch backfill."""
    s._tick += 1
    suspensions: List[Suspend] = []
    assignments: List[Assignment] = []

    def _is_oom_error(err) -> bool:
        if err is None:
            return False
        try:
            msg = str(err).lower()
        except Exception:
            return False
        return ("oom" in msg) or ("out of memory" in msg) or ("out-of-memory" in msg) or ("memoryerror" in msg)

    def _op_key(op) -> int:
        # Prefer stable intrinsic IDs if present; otherwise fall back to Python object id.
        for attr in ("op_id", "operator_id", "id"):
            try:
                v = getattr(op, attr)
                if v is not None:
                    # If it's a method/property, attempt to call only if callable with no args.
                    if callable(v):
                        try:
                            v = v()
                        except Exception:
                            v = None
                    if v is not None:
                        return hash((attr, v))
            except Exception:
                pass
        return id(op)

    def _queue_has_live_work(pr: Priority) -> bool:
        q = s._queues[pr]
        # Fast path: non-empty; deeper checks happen during selection.
        return len(q) > 0

    def _high_priority_waiting() -> bool:
        return _queue_has_live_work(Priority.QUERY) or _queue_has_live_work(Priority.INTERACTIVE)

    def _cleanup_if_done_or_abandoned(p: Pipeline) -> bool:
        pid = p.pipeline_id
        if pid in s._abandoned_pipelines:
            return True
        st = p.runtime_status()
        if st.is_pipeline_successful():
            # Remove from tracking; hints are left in dicts (bounded by #ops) but pipeline won't be scheduled again.
            s._known_pipeline_ids.discard(pid)
            s._pipeline_arrival_tick.pop(pid, None)
            s._pipeline_last_pool.pop(pid, None)
            return True
        return False

    def _base_hints_for(pr: Priority, pool) -> Tuple[float, float]:
        # Base RAM hint is a fraction of pool size, capped, with a small floor.
        base_ram = max(1.0, pool.max_ram_pool * s._base_ram_frac[pr])
        base_ram = min(base_ram, pool.max_ram_pool * s._max_ram_frac[pr])
        # Base CPU hint uses fraction + cap.
        base_cpu = max(1.0, pool.max_cpu_pool * s._cpu_frac[pr])
        base_cpu = min(base_cpu, s._cpu_cap[pr], pool.max_cpu_pool)
        return base_cpu, base_ram

    def _get_hints(pid: int, op, pr: Priority, pool) -> Tuple[float, float]:
        ok = _op_key(op)
        base_cpu, base_ram = _base_hints_for(pr, pool)
        cpu_hint = float(s._op_cpu_hint.get((pid, ok), base_cpu))
        ram_hint = float(s._op_ram_hint.get((pid, ok), base_ram))
        # Clamp to pool maxima
        cpu_hint = min(max(1.0, cpu_hint), float(pool.max_cpu_pool))
        ram_hint = min(max(1.0, ram_hint), float(pool.max_ram_pool))
        return cpu_hint, ram_hint

    def _op_retry_exhausted(pid: int, op, pr: Priority) -> bool:
        ok = _op_key(op)
        fc = s._op_fail.get((pid, ok), None)
        if not fc:
            return False
        if fc.get("total", 0) > s._max_total_retries[pr]:
            return True
        if fc.get("non_oom", 0) > s._max_non_oom_retries[pr]:
            return True
        return False

    def _eligible_to_fit(pid: int, op, pr: Priority, pool, avail_cpu: float, avail_ram: float) -> bool:
        cpu_hint, ram_hint = _get_hints(pid, op, pr, pool)
        # Require a minimum fraction of hinted RAM to reduce churn from repeated OOM at tail capacity.
        need_ram = ram_hint * s._fit_frac_required[pr]
        need_cpu = 1.0
        return (avail_cpu >= need_cpu) and (avail_ram >= need_ram)

    def _pick_priority_for_pool(pool, avail_cpu: float, avail_ram: float) -> Priority | None:
        candidates = []
        for pr in (Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE):
            if not _queue_has_live_work(pr):
                continue
            # If high-priority is waiting, only run batch when we still keep a headroom reserve.
            if pr == Priority.BATCH_PIPELINE and _high_priority_waiting():
                reserve_cpu = max(1.0, pool.max_cpu_pool * s._reserve_cpu_frac)
                reserve_ram = max(1.0, pool.max_ram_pool * s._reserve_ram_frac)
                if (avail_cpu - reserve_cpu) < 1.0 or (avail_ram - reserve_ram) < 1.0:
                    continue
            candidates.append(pr)

        if not candidates:
            return None

        # Primary: pick max credits; fallback: strict priority if all credits are low/negative.
        best_pr = None
        best_credit = None
        for pr in candidates:
            c = float(s._credits.get(pr, 0.0))
            if best_pr is None or c > best_credit or (c == best_credit and s._weights[pr] > s._weights[best_pr]):
                best_pr, best_credit = pr, c

        if best_credit is not None and best_credit <= 0.0:
            # Strict priority fallback to avoid idling when credits are depleted.
            for pr in (Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE):
                if pr in candidates:
                    return pr
        return best_pr

    def _pick_pipeline_and_op(pr: Priority, pool_id: int, pool, avail_cpu: float, avail_ram: float):
        q = s._queues[pr]
        if not q:
            return None, None

        # Two-pass: first try pipelines with stickiness to this pool, then any pipeline.
        for pass_idx in (0, 1):
            n = len(q)
            for _ in range(n):
                p = q.pop(0)
                pid = p.pipeline_id

                # Drop finished/abandoned from queue permanently.
                if _cleanup_if_done_or_abandoned(p):
                    continue

                # Soft stickiness: prefer running follow-up ops on same pool that last produced a result.
                if pass_idx == 0:
                    lp = s._pipeline_last_pool.get(pid, None)
                    if lp is not None and lp != pool_id:
                        q.append(p)
                        continue

                st = p.runtime_status()
                # Avoid hard-dropping pipelines with failures; instead retry failed ops (ASSIGNABLE includes FAILED).
                op_list = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
                if not op_list:
                    q.append(p)
                    continue

                op = op_list[0]

                # If an op looks unrecoverable (too many non-OOM failures), abandon the whole pipeline to protect SLOs.
                if _op_retry_exhausted(pid, op, pr):
                    s._abandoned_pipelines.add(pid)
                    continue

                # Only pick it if it fits well enough in this pool right now (reduce OOM / tail churn).
                if not _eligible_to_fit(pid, op, pr, pool, avail_cpu, avail_ram):
                    q.append(p)
                    continue

                # Keep pipeline in the queue (round-robin) for future parallelism / later ops.
                q.append(p)
                return p, op

        return None, None

    def _size_resources(pid: int, op, pr: Priority, pool, avail_cpu: float, avail_ram: float) -> Tuple[float, float]:
        cpu_hint, ram_hint = _get_hints(pid, op, pr, pool)

        # Allocate close to hint but never exceed available.
        cpu = min(cpu_hint, float(avail_cpu))
        ram = min(ram_hint, float(avail_ram))

        # Ensure minimums
        cpu = max(1.0, cpu)
        ram = max(1.0, ram)

        # If we had to clip RAM too much, don't run (risk OOM). Caller should have pre-checked, but be safe.
        if ram < ram_hint * s._fit_frac_required[pr]:
            return 0.0, 0.0

        return cpu, ram

    # Enqueue new pipelines (one-time)
    for p in pipelines:
        pid = p.pipeline_id
        if pid in s._known_pipeline_ids:
            continue
        s._known_pipeline_ids.add(pid)
        s._pipeline_arrival_tick[pid] = s._tick
        s._queues[p.priority].append(p)

    # Process execution results to adjust hints and stickiness
    for r in results:
        # Update hints per-op based on observed failures / successes.
        pr = r.priority
        pool_id = r.pool_id
        pool = s.executor.pools[pool_id]
        oom = r.failed() and _is_oom_error(r.error)

        for op in getattr(r, "ops", []) or []:
            pid = s._op_to_pid.get(id(op), None)
            if pid is None:
                continue

            s._pipeline_last_pool[pid] = pool_id
            ok = _op_key(op)

            fc = s._op_fail.get((pid, ok), {"total": 0, "oom": 0, "non_oom": 0})
            if r.failed():
                fc["total"] = int(fc.get("total", 0) + 1)
                if oom:
                    fc["oom"] = int(fc.get("oom", 0) + 1)
                else:
                    fc["non_oom"] = int(fc.get("non_oom", 0) + 1)
                s._op_fail[(pid, ok)] = fc

                # RAM-first: OOM => increase RAM hint aggressively (up to pool max).
                if oom:
                    prev_hint = float(s._op_ram_hint.get((pid, ok), max(1.0, r.ram)))
                    new_hint = max(prev_hint * s._oom_growth, float(r.ram) * s._oom_min_mult)
                    new_hint = min(float(pool.max_ram_pool), new_hint)
                    s._op_ram_hint[(pid, ok)] = new_hint

                    # Keep CPU hint reasonable; don't explode CPU on OOM.
                    prev_cpu = float(s._op_cpu_hint.get((pid, ok), max(1.0, r.cpu)))
                    s._op_cpu_hint[(pid, ok)] = min(float(pool.max_cpu_pool), max(1.0, prev_cpu))
                else:
                    # Non-OOM failure: small CPU bump (might help if timeouts modeled as failures), but cap tightly.
                    prev_cpu = float(s._op_cpu_hint.get((pid, ok), max(1.0, r.cpu)))
                    bumped = min(float(pool.max_cpu_pool), max(1.0, prev_cpu * 1.35))
                    s._op_cpu_hint[(pid, ok)] = bumped

                    # Also keep RAM at least what we tried last time to avoid oscillation.
                    prev_ram = float(s._op_ram_hint.get((pid, ok), max(1.0, r.ram)))
                    s._op_ram_hint[(pid, ok)] = min(float(pool.max_ram_pool), max(prev_ram, float(r.ram)))
            else:
                # Success: record that this RAM/CPU worked; keep hints at least this large to reduce later OOM.
                prev_ram = float(s._op_ram_hint.get((pid, ok), 0.0))
                s._op_ram_hint[(pid, ok)] = min(float(pool.max_ram_pool), max(prev_ram, float(r.ram)))
                prev_cpu = float(s._op_cpu_hint.get((pid, ok), 0.0))
                s._op_cpu_hint[(pid, ok)] = min(float(pool.max_cpu_pool), max(prev_cpu, float(r.cpu), 1.0))

    # Add weighted credits (drives fairness without starving batch)
    for pr, w in s._weights.items():
        s._credits[pr] += float(w) * float(s._credit_scale)
        cap = float(w) * float(s._credit_scale) * float(s._credit_cap_mult)
        if s._credits[pr] > cap:
            s._credits[pr] = cap

    # Order pools by available RAM then CPU (place bigger/higher-risk ops on roomier pools)
    pool_ids = list(range(s.executor.num_pools))
    pool_ids.sort(
        key=lambda i: (s.executor.pools[i].avail_ram_pool, s.executor.pools[i].avail_cpu_pool),
        reverse=True,
    )

    # Main scheduling loop
    for pool_id in pool_ids:
        pool = s.executor.pools[pool_id]
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)
        if avail_cpu < 1.0 or avail_ram < 1.0:
            continue

        for _ in range(int(s._max_assignments_per_pool_per_tick)):
            if avail_cpu < 1.0 or avail_ram < 1.0:
                break

            pr = _pick_priority_for_pool(pool, avail_cpu, avail_ram)
            if pr is None:
                break

            p, op = _pick_pipeline_and_op(pr, pool_id, pool, avail_cpu, avail_ram)
            if p is None or op is None:
                # If chosen priority can't place anything in this pool right now, slightly penalize its credits
                # to encourage trying other classes and avoid spin.
                s._credits[pr] -= 0.25 * s._total_weight
                continue

            cpu, ram = _size_resources(p.pipeline_id, op, pr, pool, avail_cpu, avail_ram)
            if cpu <= 0.0 or ram <= 0.0:
                # Can't fit safely; try others.
                s._credits[pr] -= 0.10 * s._total_weight
                continue

            assignment = Assignment(
                ops=[op],
                cpu=cpu,
                ram=ram,
                priority=p.priority,
                pool_id=pool_id,
                pipeline_id=p.pipeline_id,
            )
            assignments.append(assignment)

            # Remember mapping for subsequent result processing.
            s._op_to_pid[id(op)] = p.pipeline_id

            # Consume credits for this scheduled op (approximate weighted fair queuing across classes).
            s._credits[pr] -= float(s._total_weight)

            # Update local available resources for additional placements in this pool this tick.
            avail_cpu -= float(cpu)
            avail_ram -= float(ram)

    return suspensions, assignments
