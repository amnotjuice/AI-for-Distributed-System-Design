from collections import deque

@register_scheduler_init(key="scheduler_low_001")
def scheduler_low_001_init(s):
    # Per-priority pipeline queues (store pipeline_ids; latest Pipeline object stored separately).
    s.q = {
        Priority.QUERY: deque(),
        Priority.INTERACTIVE: deque(),
        Priority.BATCH_PIPELINE: deque(),
    }
    s.in_queue = {}  # pipeline_id -> Priority (which queue), absent/None if not enqueued

    # Latest pipeline references and arrival/aging.
    s.pipelines_by_id = {}       # pipeline_id -> Pipeline
    s.arrival_tick = {}          # pipeline_id -> tick first seen
    s.last_scheduled_tick = {}   # pipeline_id -> tick last time we scheduled any op

    # Operator resource learning (by signature).
    s.op_ram_lb = {}             # op_sig -> max RAM that OOM-failed (lower bound for true min)
    s.op_ram_ub = {}             # op_sig -> min RAM that succeeded (upper bound for true min)
    s.op_ooms = {}               # op_sig -> count
    s.op_fails = {}              # op_sig -> count
    s.op_success = {}            # op_sig -> count

    # Tick counter.
    s._tick = 0

    # Batch aging to avoid starvation penalties.
    s.batch_aging_ticks = 24

    # Initial RAM guess as small slice of a pool (to promote packing), with quick OOM convergence.
    s.init_ram_frac = {
        Priority.QUERY: 0.022,
        Priority.INTERACTIVE: 0.018,
        Priority.BATCH_PIPELINE: 0.015,
    }
    s.init_ram_min = 1.0  # simulator units (GB-ish)

    # Safety margin applied at allocation time (slightly higher for QUERY).
    s.base_safety = {
        Priority.QUERY: 1.10,
        Priority.INTERACTIVE: 1.07,
        Priority.BATCH_PIPELINE: 1.05,
    }

    # OOM/failure backoff to drive OOMs to near-zero after first miss.
    s.oom_backoff = 1.85
    s.fail_backoff = 1.25

    # CPU sizing: moderate parallelism; opportunistic boost when pool is mostly idle.
    s.min_cpu = 1.0
    s.cpu_target_frac = {
        Priority.QUERY: 0.075,
        Priority.INTERACTIVE: 0.060,
        Priority.BATCH_PIPELINE: 0.050,
    }
    s.cpu_cap_frac = {
        Priority.QUERY: 0.22,
        Priority.INTERACTIVE: 0.18,
        Priority.BATCH_PIPELINE: 0.14,
    }
    s.idle_boost_threshold = 0.62
    s.idle_boost_mult = 1.6

    # Lightweight reservation when QUERY work is ready (avoid tail-lat spikes without idling too much).
    s.reserve_cpu_frac = 0.08
    s.reserve_ram_frac = 0.10

    # Weighted round-robin across priorities for admission (prevents starvation).
    s.rr_seq = (
        [Priority.QUERY] * 10 +
        [Priority.INTERACTIVE] * 5 +
        [Priority.BATCH_PIPELINE] * 1
    )
    s.rr_idx = 0


@register_scheduler(key="scheduler_low_001")
def scheduler_low_001_scheduler(s, results, pipelines):
    s._tick += 1

    def _safe_str(x, max_len=200):
        try:
            v = str(x)
        except Exception:
            v = repr(x)
        if len(v) > max_len:
            return v[:max_len]
        return v

    def _op_sig(op):
        parts = [type(op).__name__]
        for attr in ("name", "op_name", "fn_name", "kind", "type", "sql", "query", "udf_name", "callable_name"):
            if hasattr(op, attr):
                val = getattr(op, attr)
                if val is None:
                    continue
                parts.append(f"{attr}={_safe_str(val, 160)}")
        for attr in ("op_id", "operator_id", "id"):
            if hasattr(op, attr):
                val = getattr(op, attr)
                if val is None:
                    continue
                parts.append(f"{attr}={_safe_str(val, 80)}")
                break
        return "|".join(parts)

    def _is_oom_error(err):
        if not err:
            return False
        msg = _safe_str(err, 400).lower()
        return ("oom" in msg) or ("out of memory" in msg) or ("out_of_memory" in msg) or ("memoryerror" in msg)

    def _effective_priority(pipeline):
        pr = pipeline.priority
        if pr == Priority.BATCH_PIPELINE:
            at = s.arrival_tick.get(pipeline.pipeline_id, s._tick)
            if (s._tick - at) >= s.batch_aging_ticks:
                return Priority.INTERACTIVE
        return pr

    def _rank(pr):
        if pr == Priority.QUERY:
            return 0
        if pr == Priority.INTERACTIVE:
            return 1
        return 2

    def _remove_deque_at(q, idx):
        q.rotate(-idx)
        item = q.popleft()
        q.rotate(idx)
        return item

    def _enqueue(pid, pr):
        cur = s.in_queue.get(pid)
        if cur is not None:
            return
        s.q[pr].append(pid)
        s.in_queue[pid] = pr

    def _dequeue(q, idx, pid):
        _remove_deque_at(q, idx)
        s.in_queue[pid] = None

    def _initial_ram_guess(pool, pr):
        frac = s.init_ram_frac.get(pr, 0.015)
        guess = float(pool.max_ram_pool) * float(frac)
        return max(float(s.init_ram_min), float(guess))

    def _ram_estimate(pool, sig, pr):
        # Use UB (smallest success) if known to maximize packing with near-zero OOM;
        # else use LB (largest OOM) scaled up; else use initial guess.
        ub = s.op_ram_ub.get(sig)
        lb = s.op_ram_lb.get(sig)

        if ub is not None and ub > 0:
            base = float(ub)
        elif lb is not None and lb > 0:
            base = float(lb) * float(s.oom_backoff)
        else:
            base = _initial_ram_guess(pool, pr)

        base = min(float(pool.max_ram_pool), max(float(s.init_ram_min), float(base)))

        # Slightly higher safety after repeated OOMs for this op signature.
        ooms = s.op_ooms.get(sig, 0)
        safety = float(s.base_safety.get(pr, 1.06)) * (1.0 + 0.10 * min(5, int(ooms)))
        need = min(float(pool.max_ram_pool), base * safety)
        return need

    def _cpu_need(pool, pr, avail_cpu):
        max_cpu = float(pool.max_cpu_pool)
        avail = float(avail_cpu)

        base = max_cpu * float(s.cpu_target_frac.get(pr, 0.05))
        cap = max_cpu * float(s.cpu_cap_frac.get(pr, 0.14))

        cpu = max(float(s.min_cpu), float(base))
        if max_cpu > 0 and (avail / max_cpu) >= float(s.idle_boost_threshold):
            cpu = min(float(cap), cpu * float(s.idle_boost_mult))

        cpu = min(float(cap), float(avail), max(float(s.min_cpu), cpu))
        return cpu

    def _any_query_ready():
        # Small bounded scan to decide whether to reserve headroom.
        q = s.q[Priority.QUERY]
        scan = min(len(q), 48)
        for i in range(scan):
            pid = q[i]
            p = s.pipelines_by_id.get(pid)
            if not p:
                continue
            st = p.runtime_status()
            if st.is_pipeline_successful():
                continue
            if st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True):
                return True
        return False

    # --- Learn from results ---
    for r in results:
        if not r or not getattr(r, "ops", None):
            continue

        attempted_ram = getattr(r, "ram", None)
        attempted_cpu = getattr(r, "cpu", None)
        was_fail = False
        try:
            was_fail = r.failed()
        except Exception:
            was_fail = False
        was_oom = was_fail and _is_oom_error(getattr(r, "error", None))

        for op in r.ops:
            sig = _op_sig(op)

            if was_fail:
                s.op_fails[sig] = s.op_fails.get(sig, 0) + 1
                if was_oom:
                    s.op_ooms[sig] = s.op_ooms.get(sig, 0) + 1

                if attempted_ram is not None:
                    ar = float(attempted_ram)
                    if was_oom:
                        prev_lb = s.op_ram_lb.get(sig)
                        s.op_ram_lb[sig] = ar if prev_lb is None else max(float(prev_lb), ar)
                        # If we previously marked an UB <= this failed cap, discard UB (it is inconsistent).
                        prev_ub = s.op_ram_ub.get(sig)
                        if prev_ub is not None and float(prev_ub) <= ar + 1e-9:
                            s.op_ram_ub.pop(sig, None)
                    else:
                        # Non-OOM failure: keep RAM bounds, but nudge UB away from too-small guesses if any.
                        prev_lb = s.op_ram_lb.get(sig)
                        if prev_lb is None:
                            s.op_ram_lb[sig] = max(float(s.init_ram_min), ar * 0.75)
                        else:
                            s.op_ram_lb[sig] = max(float(prev_lb), ar * 0.75)

            else:
                s.op_success[sig] = s.op_success.get(sig, 0) + 1
                if attempted_ram is not None:
                    ar = float(attempted_ram)
                    prev_ub = s.op_ram_ub.get(sig)
                    s.op_ram_ub[sig] = ar if prev_ub is None else min(float(prev_ub), ar)
                    # If LB >= UB, prefer UB and clear LB (we have a safe cap).
                    prev_lb = s.op_ram_lb.get(sig)
                    if prev_lb is not None and float(prev_lb) >= float(s.op_ram_ub[sig]) - 1e-9:
                        s.op_ram_lb.pop(sig, None)

    # --- Ingest pipelines (dedup + keep in queues) ---
    for p in pipelines:
        pid = p.pipeline_id
        s.pipelines_by_id[pid] = p
        if pid not in s.arrival_tick:
            s.arrival_tick[pid] = s._tick
            s.last_scheduled_tick[pid] = s._tick

        st = p.runtime_status()
        if st.is_pipeline_successful():
            # Ensure removed from queue membership.
            if s.in_queue.get(pid) is not None:
                s.in_queue[pid] = None
            continue

        # Enqueue if there is any remaining assignable work (even if not parent-ready yet).
        remaining = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=False)
        if remaining:
            _enqueue(pid, p.priority)

    suspensions = []
    assignments = []

    query_ready = _any_query_ready()

    # Snapshot pool availability in local mutable arrays.
    num_pools = int(s.executor.num_pools)
    pool_avail_cpu = [float(s.executor.pools[i].avail_cpu_pool) for i in range(num_pools)]
    pool_avail_ram = [float(s.executor.pools[i].avail_ram_pool) for i in range(num_pools)]

    def _pool_reserve(pool, eff_pr):
        if not query_ready:
            return (0.0, 0.0)
        # QUERY itself ignores reservations.
        if eff_pr == Priority.QUERY:
            return (0.0, 0.0)
        # INTERACTIVE is allowed to use some headroom but keep a small reserve for QUERY.
        if eff_pr == Priority.INTERACTIVE:
            return (float(pool.max_cpu_pool) * (s.reserve_cpu_frac * 0.6),
                    float(pool.max_ram_pool) * (s.reserve_ram_frac * 0.6))
        # BATCH must respect full reserve when QUERY is waiting.
        return (float(pool.max_cpu_pool) * float(s.reserve_cpu_frac),
                float(pool.max_ram_pool) * float(s.reserve_ram_frac))

    def _choose_best_placement(op, eff_pr):
        sig = _op_sig(op)

        # Consider a small set of candidate pools: top-K by available RAM (helps big ops).
        # K chosen small for speed; include any pool with enough CPU for min.
        candidates = []
        for i in range(num_pools):
            if pool_avail_cpu[i] >= float(s.min_cpu) and pool_avail_ram[i] >= float(s.init_ram_min):
                candidates.append(i)
        if not candidates:
            return None

        candidates.sort(key=lambda i: pool_avail_ram[i], reverse=True)
        candidates = candidates[:6]

        best = None
        best_score = None

        for pool_id in candidates:
            pool = s.executor.pools[pool_id]
            avail_cpu = float(pool_avail_cpu[pool_id])
            avail_ram = float(pool_avail_ram[pool_id])

            reserve_cpu, reserve_ram = _pool_reserve(pool, eff_pr)

            cpu_need = _cpu_need(pool, eff_pr, avail_cpu)
            if cpu_need < float(s.min_cpu) - 1e-9:
                continue

            ram_need = _ram_estimate(pool, sig, eff_pr)
            # If we can't fit the (conservative) need, don't start it; prefer waiting to avoid OOM thrash.
            if ram_need > avail_ram + 1e-9:
                continue

            # Reservations: ensure we keep some headroom when queries are ready.
            if (avail_cpu - cpu_need) < reserve_cpu - 1e-9 or (avail_ram - ram_need) < reserve_ram - 1e-9:
                continue

            # Best-fit-ish score: minimize leftover RAM first (RAM is the common bottleneck), then CPU.
            post_ram = avail_ram - ram_need
            post_cpu = avail_cpu - cpu_need
            waste_score = (post_ram / max(1.0, float(pool.max_ram_pool)),
                           post_cpu / max(1.0, float(pool.max_cpu_pool)))

            if best is None or waste_score < best_score:
                best = (pool_id, cpu_need, ram_need)
                best_score = waste_score

        return best

    def _find_candidate_for_priority(pr):
        # Scan queue(s) for this priority; for INTERACTIVE also consider aged batch.
        scan_limit = 72

        queue_list = []
        if pr == Priority.INTERACTIVE:
            queue_list = [(Priority.INTERACTIVE, s.q[Priority.INTERACTIVE]), (Priority.BATCH_PIPELINE, s.q[Priority.BATCH_PIPELINE])]
        else:
            queue_list = [(pr, s.q[pr])]

        best = None
        best_key = None

        for base_pr, q in queue_list:
            if not q:
                continue
            scan = min(len(q), scan_limit)
            i = 0
            while i < scan and i < len(q):
                pid = q[i]
                p = s.pipelines_by_id.get(pid)
                if not p:
                    # Drop stale id.
                    _dequeue(q, i, pid)
                    scan -= 1
                    continue

                st = p.runtime_status()
                if st.is_pipeline_successful():
                    _dequeue(q, i, pid)
                    scan -= 1
                    continue

                eff_pr = _effective_priority(p)
                # Only consider aged batch in INTERACTIVE searches; otherwise enforce base.
                if pr == Priority.INTERACTIVE and base_pr == Priority.BATCH_PIPELINE and eff_pr != Priority.INTERACTIVE:
                    i += 1
                    continue
                if pr != Priority.INTERACTIVE and base_pr != pr:
                    i += 1
                    continue

                ready_ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
                if not ready_ops:
                    i += 1
                    continue

                # Try a few ready ops; pick smallest-RAM feasible placement to improve packing.
                chosen = None  # (pool_id, cpu, ram, op)
                chosen_ram = None

                for op in ready_ops[:8]:
                    placement = _choose_best_placement(op, eff_pr)
                    if placement is None:
                        continue
                    pool_id, cpu_need, ram_need = placement
                    if chosen is None or float(ram_need) < float(chosen_ram):
                        chosen = (pool_id, cpu_need, ram_need, op, eff_pr)
                        chosen_ram = ram_need

                if chosen is None:
                    i += 1
                    continue

                age = s._tick - s.arrival_tick.get(pid, s._tick)
                since = s._tick - s.last_scheduled_tick.get(pid, s._tick)
                # Key: prioritize effective priority, then pipelines that haven't been scheduled recently,
                # then older arrivals, then smaller RAM (packing).
                key = (
                    _rank(chosen[4]),
                    -min(since, 10_000),
                    -min(age, 10_000),
                    float(chosen[2]),
                )

                if best is None or key < best_key:
                    best = (q, i, pid, p, chosen[0], chosen[1], chosen[2], chosen[3], chosen[4])
                    best_key = key

                i += 1

        return best

    # Global scheduling: repeatedly pick by weighted RR, placing best-fit across pools.
    max_assignments = 256
    placed = 0

    while placed < max_assignments:
        # Quick stop if no pool can run anything.
        any_pool_has_room = False
        for i in range(num_pools):
            if pool_avail_cpu[i] >= float(s.min_cpu) and pool_avail_ram[i] >= float(s.init_ram_min):
                any_pool_has_room = True
                break
        if not any_pool_has_room:
            break

        picked = None
        tried = 0
        seq_len = len(s.rr_seq)

        while tried < seq_len and picked is None:
            pr = s.rr_seq[s.rr_idx]
            s.rr_idx = (s.rr_idx + 1) % seq_len
            tried += 1
            picked = _find_candidate_for_priority(pr)

        if picked is None:
            break

        q, idx, pid, pipeline, pool_id, cpu_need, ram_need, op, eff_pr = picked

        # Remove pipeline id from its queue, then re-enqueue to the end if still incomplete.
        _dequeue(q, idx, pid)

        # Final availability check (might have changed since placement was computed).
        if cpu_need > pool_avail_cpu[pool_id] + 1e-9 or ram_need > pool_avail_ram[pool_id] + 1e-9:
            # Put back and stop this round (avoid tight loops).
            _enqueue(pid, pipeline.priority)
            break

        assignments.append(
            Assignment(
                ops=[op],
                cpu=float(cpu_need),
                ram=float(ram_need),
                priority=pipeline.priority,
                pool_id=int(pool_id),
                pipeline_id=pipeline.pipeline_id,
            )
        )

        pool_avail_cpu[pool_id] -= float(cpu_need)
        pool_avail_ram[pool_id] -= float(ram_need)
        s.last_scheduled_tick[pid] = s._tick
        placed += 1

        # Re-enqueue if still has remaining assignable work (even if not parent-ready yet).
        st = pipeline.runtime_status()
        if not st.is_pipeline_successful():
            remaining = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=False)
            if remaining:
                _enqueue(pid, pipeline.priority)

    return suspensions, assignments
