# policy_key: scheduler_high_019
# reasoning_effort: high
# exp: reasoning
# model: gpt-5.2-2025-12-11
# llm_cost: 0.159597
# generation_seconds: 173.45
# generated_at: 2026-04-10T01:12:46.318463
@register_scheduler_init(key="scheduler_high_019")
def scheduler_high_019_init(s):
    """Priority-aware, OOM-averse scheduler with conservative sizing + bounded retries.

    Design goals aligned to the score:
    - Keep QUERY/INTERACTIVE latency low via strict priority ordering + (soft) batch headroom reservation.
    - Avoid OOM failures (each can cascade into long/incomplete pipelines) by over-allocating RAM for high-priority,
      and retrying failures with exponentially increased RAM (up to pool max).
    - Avoid indefinite starvation by allowing batch to consume non-reserved capacity and by round-robin within priority.
    """
    s.tick = 0

    # Pipeline tracking
    s.pipeline_map = {}          # pipeline_id -> Pipeline
    s.arrival_tick = {}          # pipeline_id -> tick when first seen
    s.dead_pipelines = set()     # pipeline_ids we consider hopeless (e.g., repeated OOM at max RAM)

    # Per-priority RR queues of pipeline_ids
    s.queues = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }
    s.rr_idx = {
        Priority.QUERY: 0,
        Priority.INTERACTIVE: 0,
        Priority.BATCH_PIPELINE: 0,
    }

    # Per-operator (keyed by id(op)) learning for RAM sizing & retries
    s.ram_mult = {}              # op_key -> multiplier applied to op's min_ram (if known)
    s.ram_last_success = {}      # op_key -> last successful RAM allocation
    s.retry_count = {}           # op_key -> number of observed failures (all causes)
    s.oom_count = {}             # op_key -> number of observed OOM failures
    s.maxram_oom_count = {}      # op_key -> OOM failures that occurred while already at (near) pool max RAM
    s.unrunnable_ops = set()     # op_key -> give up scheduling attempts

    # Tunables (kept conservative to avoid OOM penalties)
    s.max_total_retries = 12           # after this many failures, mark op unrunnable to avoid wasting the cluster
    s.max_maxram_oom_retries = 3       # repeated OOM at max RAM strongly suggests unrunnable on any pool
    s.max_ram_multiplier = 64.0        # cap exponential growth
    s.decay_on_success = 0.85          # slowly reduce multiplier after success to regain concurrency


@register_scheduler(key="scheduler_high_019")
def scheduler_high_019(s, results, pipelines):
    """
    Scheduler step:
    - Incorporate new pipelines.
    - Update RAM estimates based on results (OOM -> increase RAM multiplier).
    - Remove completed pipelines from queues.
    - Assign ready operators in priority order, respecting soft reservations for batch.
    """
    s.tick += 1

    def _assignable_states():
        # Prefer global ASSIGNABLE_STATES if present; otherwise use the documented definition.
        return globals().get("ASSIGNABLE_STATES", (OperatorState.PENDING, OperatorState.FAILED))

    def _op_key(op):
        # Operator objects are stable in the simulator; id(op) works across ticks.
        return id(op)

    def _is_oom_error(err):
        if err is None:
            return False
        try:
            msg = str(err).lower()
        except Exception:
            return False
        return ("oom" in msg) or ("out of memory" in msg) or ("out-of-memory" in msg) or ("memoryerror" in msg)

    def _get_op_min_ram(op):
        # Try common attribute names; if unavailable, return None.
        candidates = [
            "min_ram", "min_ram_gb", "ram_min", "ram_required", "memory_required",
            "mem_required", "min_memory", "min_memory_gb",
        ]
        for name in candidates:
            v = getattr(op, name, None)
            if v is None:
                continue
            try:
                v = float(v)
            except Exception:
                continue
            if v > 0:
                return v
        return None

    def _pool_reservations(num_pools, high_backlog):
        # Soft reservations: only restrict BATCH; QUERY/INTERACTIVE can use full capacity.
        # Rationale: keep headroom for high-priority arrivals to reduce queueing/tail latency.
        reserves = []
        for pool_id in range(num_pools):
            pool = s.executor.pools[pool_id]
            if not high_backlog:
                reserves.append((0.0, 0.0))
                continue

            # If multiple pools exist, bias pool 0 towards high-priority by reserving more there.
            if num_pools > 1 and pool_id == 0:
                cpu_r = 0.50 * pool.max_cpu_pool
                ram_r = 0.50 * pool.max_ram_pool
            else:
                cpu_r = 0.25 * pool.max_cpu_pool
                ram_r = 0.25 * pool.max_ram_pool
            reserves.append((cpu_r, ram_r))
        return reserves

    def _prio_order():
        return (Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE)

    def _prio_cpu_frac(prio):
        if prio == Priority.QUERY:
            return 0.75
        if prio == Priority.INTERACTIVE:
            return 0.60
        return 0.30

    def _prio_ram_frac(prio):
        # High-priority gets a big RAM slice to reduce OOM probability (OOM is extremely costly).
        if prio == Priority.QUERY:
            return 0.80
        if prio == Priority.INTERACTIVE:
            return 0.70
        return 0.35

    def _batch_cpu_cap():
        # Avoid letting batch consume entire pools; keep it bounded for responsiveness.
        return 8.0

    def _pick_candidate_pools(prio, num_pools, high_backlog):
        if num_pools <= 1:
            return [0]

        if prio in (Priority.QUERY, Priority.INTERACTIVE):
            # Prefer pool 0 for high-priority (less contention if we also steer batch elsewhere).
            return [0] + list(range(1, num_pools))

        # Batch: prefer non-0 pools, spill into 0 only if no high-priority backlog.
        pools = list(range(1, num_pools))
        if not pools:
            pools = [0]
        if not high_backlog:
            pools = pools + [0]
        return pools

    def _pipeline_is_done(p):
        try:
            return p.runtime_status().is_pipeline_successful()
        except Exception:
            return False

    def _pipeline_has_runnable_ready_op(p):
        # True if there exists at least one ready op not marked unrunnable.
        st = p.runtime_status()
        ops = st.get_ops(_assignable_states(), require_parents_complete=True)
        for op in ops:
            if _op_key(op) not in s.unrunnable_ops:
                return True
        return False

    def _select_next_ready_pipeline(prio, scheduled_this_tick):
        q = s.queues[prio]
        n = len(q)
        if n == 0:
            return None, None

        start = s.rr_idx[prio] % n
        for offset in range(n):
            idx = (start + offset) % n
            pid = q[idx]
            p = s.pipeline_map.get(pid, None)
            if p is None:
                continue
            if pid in s.dead_pipelines:
                continue
            if _pipeline_is_done(p):
                continue
            if pid in scheduled_this_tick:
                continue

            st = p.runtime_status()

            # For query/interactive, reduce parallelism within a pipeline to stabilize latency.
            try:
                running = st.state_counts.get(OperatorState.RUNNING, 0)
            except Exception:
                running = 0
            if prio in (Priority.QUERY, Priority.INTERACTIVE) and running > 0:
                continue

            ops = st.get_ops(_assignable_states(), require_parents_complete=True)
            if not ops:
                continue

            # Pick the first runnable op (skip those we deemed unrunnable).
            picked = None
            for op in ops:
                if _op_key(op) not in s.unrunnable_ops:
                    picked = op
                    break
            if picked is None:
                continue

            # Advance RR pointer to after this pipeline
            s.rr_idx[prio] = (idx + 1) % n
            return p, picked

        return None, None

    def _score_pool_for_assignment(prio, pool_id, cpu_req, ram_req, avail_cpu, avail_ram, reserve_cpu, reserve_ram):
        # Lower score is better.
        if prio in (Priority.QUERY, Priority.INTERACTIVE):
            # Prefer the pool with the most remaining headroom after placement (reduce future contention).
            headroom_cpu = (avail_cpu - cpu_req)
            headroom_ram = (avail_ram - ram_req)
            return -(headroom_cpu + 0.5 * headroom_ram)
        # Batch: best-fit packing to leave contiguous space for high-priority bursts.
        eff_cpu = max(0.0, avail_cpu - reserve_cpu)
        eff_ram = max(0.0, avail_ram - reserve_ram)
        leftover_cpu = eff_cpu - cpu_req
        leftover_ram = eff_ram - ram_req
        return (leftover_cpu + 0.5 * leftover_ram)

    def _compute_req_for_pool(prio, op, pool_id, avail_cpu, avail_ram, reserve_cpu, reserve_ram, high_backlog):
        pool = s.executor.pools[pool_id]
        opk = _op_key(op)

        # Effective available for BATCH respects reservation; high-priority can use everything.
        if prio == Priority.BATCH_PIPELINE and high_backlog:
            eff_avail_cpu = max(0.0, avail_cpu - reserve_cpu)
            eff_avail_ram = max(0.0, avail_ram - reserve_ram)
        else:
            eff_avail_cpu = avail_cpu
            eff_avail_ram = avail_ram

        if eff_avail_cpu <= 0 or eff_avail_ram <= 0:
            return None

        # CPU request
        base_cpu = max(1.0, pool.max_cpu_pool * _prio_cpu_frac(prio))
        if prio == Priority.BATCH_PIPELINE:
            base_cpu = min(base_cpu, _batch_cpu_cap())
        cpu_req = min(base_cpu, eff_avail_cpu)
        if cpu_req < 1.0:
            return None

        # RAM request: use min_ram when available; otherwise take a conservative slice of pool max.
        min_ram = _get_op_min_ram(op)
        mult = float(s.ram_mult.get(opk, 1.0))
        mult = max(1.0, min(mult, s.max_ram_multiplier))

        base_ram = pool.max_ram_pool * _prio_ram_frac(prio)
        if min_ram is not None:
            base_ram = max(base_ram, min_ram * mult)

        # If we've succeeded before, bias toward that RAM to reduce regression.
        last_ok = s.ram_last_success.get(opk, None)
        if last_ok is not None:
            try:
                last_ok = float(last_ok)
                if last_ok > 0:
                    # For high-priority, keep closer to prior success; for batch, allow more decay.
                    base_ram = max(base_ram, last_ok * (0.95 if prio != Priority.BATCH_PIPELINE else 0.85))
            except Exception:
                pass

        # Never exceed pool max; must fit in effective available.
        ram_req = min(base_ram, pool.max_ram_pool, eff_avail_ram)
        if min_ram is not None and ram_req + 1e-9 < min_ram:
            return None
        if ram_req <= 0:
            return None

        # If we had many OOMs, bias to larger RAM (already encoded via mult) and avoid too-small CPU throttling
        # (but keep within availability).
        return float(cpu_req), float(ram_req)

    # --- Incorporate new pipelines ---
    for p in pipelines:
        pid = p.pipeline_id
        if pid not in s.pipeline_map:
            s.pipeline_map[pid] = p
            s.arrival_tick[pid] = s.tick
            s.queues[p.priority].append(pid)
        else:
            # Keep latest object reference if simulator reuses ids (defensive).
            s.pipeline_map[pid] = p

    # --- Update learning from results ---
    for r in results:
        # Results may include multiple ops; update each.
        ops = getattr(r, "ops", []) or []
        failed = False
        try:
            failed = r.failed()
        except Exception:
            failed = False

        is_oom = failed and _is_oom_error(getattr(r, "error", None))
        pool_id = getattr(r, "pool_id", None)

        for op in ops:
            opk = _op_key(op)
            s.retry_count[opk] = int(s.retry_count.get(opk, 0)) + (1 if failed else 0)

            if failed:
                if is_oom:
                    s.oom_count[opk] = int(s.oom_count.get(opk, 0)) + 1
                    cur = float(s.ram_mult.get(opk, 1.0))
                    s.ram_mult[opk] = min(s.max_ram_multiplier, max(1.0, cur * 2.0))

                    # Detect OOM at/near pool max RAM; repeated suggests unrunnable.
                    if pool_id is not None and 0 <= int(pool_id) < s.executor.num_pools:
                        pool = s.executor.pools[int(pool_id)]
                        try:
                            used_ram = float(getattr(r, "ram", 0.0) or 0.0)
                        except Exception:
                            used_ram = 0.0
                        if pool.max_ram_pool > 0 and used_ram >= 0.99 * pool.max_ram_pool:
                            s.maxram_oom_count[opk] = int(s.maxram_oom_count.get(opk, 0)) + 1
                else:
                    # Unknown failure: slightly increase RAM as a hedge, but less aggressively than OOM.
                    cur = float(s.ram_mult.get(opk, 1.0))
                    s.ram_mult[opk] = min(s.max_ram_multiplier, max(1.0, cur * 1.25))
            else:
                # Success: record RAM and decay multiplier to regain concurrency over time.
                try:
                    used_ram = float(getattr(r, "ram", 0.0) or 0.0)
                    if used_ram > 0:
                        s.ram_last_success[opk] = used_ram
                except Exception:
                    pass
                cur = float(s.ram_mult.get(opk, 1.0))
                s.ram_mult[opk] = max(1.0, cur * float(s.decay_on_success))
                # Reset some failure signals on success.
                s.maxram_oom_count.pop(opk, None)

            # Mark op unrunnable if it repeatedly fails (especially OOM at max RAM).
            if int(s.retry_count.get(opk, 0)) >= int(s.max_total_retries):
                s.unrunnable_ops.add(opk)
            if int(s.maxram_oom_count.get(opk, 0)) >= int(s.max_maxram_oom_retries):
                s.unrunnable_ops.add(opk)

    # --- Cleanup completed / dead pipelines from queues ---
    for prio in _prio_order():
        newq = []
        for pid in s.queues[prio]:
            p = s.pipeline_map.get(pid, None)
            if p is None:
                continue
            if _pipeline_is_done(p):
                continue

            # If there are no runnable ready ops and the pipeline has failures, treat as dead to avoid
            # permanently reserving capacity for hopeless high-priority jobs.
            try:
                st = p.runtime_status()
                has_failures = st.state_counts.get(OperatorState.FAILED, 0) > 0
            except Exception:
                has_failures = False

            if has_failures and (not _pipeline_has_runnable_ready_op(p)):
                s.dead_pipelines.add(pid)
                continue

            newq.append(pid)
        s.queues[prio] = newq
        if newq:
            s.rr_idx[prio] %= len(newq)
        else:
            s.rr_idx[prio] = 0

    # --- Determine high-priority backlog for reservations ---
    high_backlog = False
    for prio in (Priority.QUERY, Priority.INTERACTIVE):
        for pid in s.queues[prio]:
            if pid in s.dead_pipelines:
                continue
            p = s.pipeline_map.get(pid, None)
            if p is None or _pipeline_is_done(p):
                continue
            if _pipeline_has_runnable_ready_op(p):
                high_backlog = True
                break
        if high_backlog:
            break

    # --- Build local available resources snapshot (so we don't over-assign within the tick) ---
    num_pools = s.executor.num_pools
    avail_cpu = [0.0] * num_pools
    avail_ram = [0.0] * num_pools
    for i in range(num_pools):
        pool = s.executor.pools[i]
        avail_cpu[i] = float(getattr(pool, "avail_cpu_pool", 0.0) or 0.0)
        avail_ram[i] = float(getattr(pool, "avail_ram_pool", 0.0) or 0.0)

    reserves = _pool_reservations(num_pools, high_backlog)  # list of (reserve_cpu, reserve_ram)

    # --- Scheduling loop ---
    assignments = []
    suspensions = []  # preemption not used (container_id not reliably known at scheduling time)
    scheduled_this_tick = set()

    # Keep trying to place work until we can't make progress.
    for _ in range(1024):  # hard safety cap
        made_any = False

        for prio in _prio_order():
            p, op = _select_next_ready_pipeline(prio, scheduled_this_tick)
            if p is None or op is None:
                continue

            # If batch and we have high backlog, we still schedule batch but only into non-reserved headroom.
            candidates = _pick_candidate_pools(prio, num_pools, high_backlog)

            best = None  # (score, pool_id, cpu_req, ram_req)
            for pool_id in candidates:
                r_cpu, r_ram = reserves[pool_id]
                req = _compute_req_for_pool(
                    prio=prio,
                    op=op,
                    pool_id=pool_id,
                    avail_cpu=avail_cpu[pool_id],
                    avail_ram=avail_ram[pool_id],
                    reserve_cpu=r_cpu,
                    reserve_ram=r_ram,
                    high_backlog=high_backlog,
                )
                if req is None:
                    continue
                cpu_req, ram_req = req

                # Final fit check
                if prio == Priority.BATCH_PIPELINE and high_backlog:
                    if cpu_req > max(0.0, avail_cpu[pool_id] - r_cpu) + 1e-9:
                        continue
                    if ram_req > max(0.0, avail_ram[pool_id] - r_ram) + 1e-9:
                        continue
                else:
                    if cpu_req > avail_cpu[pool_id] + 1e-9:
                        continue
                    if ram_req > avail_ram[pool_id] + 1e-9:
                        continue

                score = _score_pool_for_assignment(
                    prio=prio,
                    pool_id=pool_id,
                    cpu_req=cpu_req,
                    ram_req=ram_req,
                    avail_cpu=avail_cpu[pool_id],
                    avail_ram=avail_ram[pool_id],
                    reserve_cpu=r_cpu,
                    reserve_ram=r_ram,
                )
                cand = (score, pool_id, cpu_req, ram_req)
                if best is None or cand[0] < best[0]:
                    best = cand

            if best is None:
                continue

            _, pool_id, cpu_req, ram_req = best

            assignments.append(
                Assignment(
                    ops=[op],
                    cpu=cpu_req,
                    ram=ram_req,
                    priority=p.priority,
                    pool_id=pool_id,
                    pipeline_id=p.pipeline_id,
                )
            )

            avail_cpu[pool_id] -= cpu_req
            avail_ram[pool_id] -= ram_req
            scheduled_this_tick.add(p.pipeline_id)
            made_any = True

        if not made_any:
            break

    return suspensions, assignments
