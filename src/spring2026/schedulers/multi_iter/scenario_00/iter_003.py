from typing import List, Tuple

@register_scheduler_init(key="scheduler_low_001")
def scheduler_low_001_init(s):
    # Per-priority pipeline queues (store pipeline_ids; latest Pipeline object stored separately).
    s.q = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }
    s.in_queue = {}  # pipeline_id -> Priority (which queue), None if not enqueued

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
    s.op_probe = {}              # op_sig -> multiplicative probe factor (<1 packs harder; >1 more conservative)
    s.op_streak = {}             # op_sig -> consecutive successes since last failure

    # Tick counter.
    s._tick = 0

    # Batch aging to avoid starvation penalties.
    s.batch_aging_ticks = 14

    # Initial RAM guess (more realistic to avoid first-try OOM thrash on small clusters).
    s.init_ram_frac = {
        Priority.QUERY: 0.030,
        Priority.INTERACTIVE: 0.024,
        Priority.BATCH_PIPELINE: 0.020,
    }
    s.init_ram_min = 1.0  # simulator units (GB-ish)

    # Safety margin applied at allocation time.
    s.base_safety = {
        Priority.QUERY: 1.08,
        Priority.INTERACTIVE: 1.06,
        Priority.BATCH_PIPELINE: 1.05,
    }

    # OOM/failure backoff.
    s.oom_backoff = 1.95
    s.fail_backoff = 1.20

    # Probe dynamics (drives packing while quickly converging to zero/near-zero OOM).
    s.probe_init = 1.00
    s.probe_min = 0.84
    s.probe_max = 1.40
    s.probe_decay_on_success = 0.985   # smaller -> more aggressive packing after repeated successes
    s.probe_bump_on_oom = 1.20
    s.probe_bump_on_fail = 1.08
    s.probe_success_streak_for_decay = 2

    # CPU sizing: keep integers to avoid "Overallocated CPU in assignment" from rounding.
    s.min_cpu_by_pr = {
        Priority.QUERY: 2,
        Priority.INTERACTIVE: 1,
        Priority.BATCH_PIPELINE: 1,
    }
    s.cpu_target_frac = {
        Priority.QUERY: 0.070,
        Priority.INTERACTIVE: 0.055,
        Priority.BATCH_PIPELINE: 0.045,
    }
    s.cpu_cap_frac = {
        Priority.QUERY: 0.20,
        Priority.INTERACTIVE: 0.16,
        Priority.BATCH_PIPELINE: 0.12,
    }
    s.idle_boost_threshold = 0.68
    s.idle_boost_mult = 1.5

    # Soft reservation when QUERY work is ready (protect tail latency), but allow aged/stalled work to borrow.
    s.reserve_cpu_frac = 0.08
    s.reserve_ram_frac = 0.10
    s.reserve_disable_after_ticks = {
        Priority.INTERACTIVE: 40,
        Priority.BATCH_PIPELINE: 70,
    }

    # Weighted round-robin across priorities (prevents starvation).
    s.rr_seq = (
        [Priority.QUERY] * 10 +
        [Priority.INTERACTIVE] * 5 +
        [Priority.BATCH_PIPELINE] * 1
    )
    s.rr_idx = 0

    # Preemption (best-effort): only used if QUERY is blocked and executor exposes running containers.
    s.preempt_cooldown_ticks = 10
    s.container_preempts = {}     # container_id -> count
    s.container_last_preempt = {} # container_id -> tick


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

    def _enqueue(pid, pr):
        cur = s.in_queue.get(pid)
        if cur is not None:
            return
        s.q[pr].append(pid)
        s.in_queue[pid] = pr

    def _dequeue(q, idx, pid):
        try:
            del q[idx]
        except Exception:
            # Fallback: remove first occurrence.
            try:
                q.remove(pid)
            except Exception:
                pass
        s.in_queue[pid] = None

    def _initial_ram_guess(pool, pr):
        frac = s.init_ram_frac.get(pr, 0.020)
        guess = float(pool.max_ram_pool) * float(frac)
        return max(float(s.init_ram_min), float(guess))

    def _container_cpu_cap(pool):
        # Try to respect any per-container caps if exposed by executor; fall back to pool max.
        for attr in ("max_cpu_container", "max_container_cpu", "max_cpu_per_container", "container_max_cpu", "vm_cpu"):
            if hasattr(pool, attr):
                try:
                    v = float(getattr(pool, attr))
                    if v > 0:
                        return min(float(pool.max_cpu_pool), v)
                except Exception:
                    pass
        return float(pool.max_cpu_pool)

    def _container_ram_cap(pool):
        for attr in ("max_ram_container", "max_container_ram", "max_ram_per_container", "container_max_ram", "vm_ram"):
            if hasattr(pool, attr):
                try:
                    v = float(getattr(pool, attr))
                    if v > 0:
                        return min(float(pool.max_ram_pool), v)
                except Exception:
                    pass
        return float(pool.max_ram_pool)

    def _ram_estimate(pool, sig, pr):
        ub = s.op_ram_ub.get(sig)
        lb = s.op_ram_lb.get(sig)

        if sig not in s.op_probe:
            s.op_probe[sig] = float(s.probe_init)
        probe = float(s.op_probe.get(sig, 1.0))

        if ub is not None and ub > 0:
            base = float(ub)
        elif lb is not None and lb > 0:
            base = float(lb) * float(s.oom_backoff)
        else:
            base = _initial_ram_guess(pool, pr)

        # Apply probe (packs down slowly on repeated successes; bumps up fast on OOM).
        base = float(base) * float(probe)

        # Bound within pool/container.
        ram_cap = _container_ram_cap(pool)
        base = min(float(ram_cap), max(float(s.init_ram_min), float(base)))

        # Safety
        ooms = s.op_ooms.get(sig, 0)
        safety = float(s.base_safety.get(pr, 1.06)) * (1.0 + 0.06 * min(6, int(ooms)))
        need = min(float(ram_cap), base * safety)

        # Never go below the known OOM lower-bound margin (if any).
        if lb is not None and lb > 0:
            need = max(float(need), float(lb) * 1.03)

        return float(need)

    def _quantize_cpu(desired, avail):
        # Avoid float rounding issues; use integer CPU units.
        a = int(float(avail) + 1e-9)
        if a <= 0:
            return 0.0
        d = float(desired)
        # Ceil desired but never exceed available integer units.
        di = int(d)
        if d - float(di) > 1e-9:
            di += 1
        if di <= 0:
            di = 1
        return float(min(di, a))

    def _cpu_need(pool, pr, avail_cpu, reserve_cpu, backlog):
        max_cpu = _container_cpu_cap(pool)
        avail = float(avail_cpu) - float(reserve_cpu)
        if avail < 1.0 - 1e-9:
            return 0.0

        target = float(max_cpu) * float(s.cpu_target_frac.get(pr, 0.05))
        cap = float(max_cpu) * float(s.cpu_cap_frac.get(pr, 0.14))

        # Backlog-adaptive: when backlog is small, give more CPU to finish pipelines;
        # when backlog is large, prefer parallelism.
        if backlog <= 3:
            target *= 1.30
        elif backlog <= 8:
            target *= 1.10
        elif backlog >= 30:
            target *= 0.92

        desired = max(float(s.min_cpu_by_pr.get(pr, 1)), float(target))
        desired = min(float(cap), float(desired))

        # Idle boost.
        if max_cpu > 0 and (float(avail_cpu) / float(max_cpu)) >= float(s.idle_boost_threshold):
            desired = min(float(cap), desired * float(s.idle_boost_mult))

        return _quantize_cpu(desired, avail)

    def _any_query_ready():
        q = s.q[Priority.QUERY]
        scan = min(len(q), 64)
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

    def _pipeline_age(pid):
        return s._tick - s.arrival_tick.get(pid, s._tick)

    def _pipeline_stall(pid):
        return s._tick - s.last_scheduled_tick.get(pid, s._tick)

    # --- Learn from results ---
    for r in results:
        if not r or not getattr(r, "ops", None):
            continue

        attempted_ram = getattr(r, "ram", None)
        was_fail = False
        try:
            was_fail = r.failed()
        except Exception:
            was_fail = False
        was_oom = was_fail and _is_oom_error(getattr(r, "error", None))

        for op in r.ops:
            sig = _op_sig(op)
            if sig not in s.op_probe:
                s.op_probe[sig] = float(s.probe_init)

            if was_fail:
                s.op_fails[sig] = s.op_fails.get(sig, 0) + 1
                s.op_streak[sig] = 0

                if was_oom:
                    s.op_ooms[sig] = s.op_ooms.get(sig, 0) + 1
                    s.op_probe[sig] = min(float(s.probe_max), float(s.op_probe[sig]) * float(s.probe_bump_on_oom))
                else:
                    s.op_probe[sig] = min(float(s.probe_max), float(s.op_probe[sig]) * float(s.probe_bump_on_fail))

                if attempted_ram is not None:
                    ar = float(attempted_ram)
                    if was_oom:
                        prev_lb = s.op_ram_lb.get(sig)
                        s.op_ram_lb[sig] = ar if prev_lb is None else max(float(prev_lb), ar)
                        prev_ub = s.op_ram_ub.get(sig)
                        if prev_ub is not None and float(prev_ub) <= ar + 1e-9:
                            s.op_ram_ub.pop(sig, None)
                    else:
                        prev_lb = s.op_ram_lb.get(sig)
                        if prev_lb is None:
                            s.op_ram_lb[sig] = max(float(s.init_ram_min), ar * 0.70)
                        else:
                            s.op_ram_lb[sig] = max(float(prev_lb), ar * 0.70)

            else:
                s.op_success[sig] = s.op_success.get(sig, 0) + 1
                s.op_streak[sig] = s.op_streak.get(sig, 0) + 1

                # Decay probe slowly after a few consecutive successes (packs tighter).
                if s.op_streak.get(sig, 0) >= int(s.probe_success_streak_for_decay):
                    s.op_probe[sig] = max(float(s.probe_min), float(s.op_probe[sig]) * float(s.probe_decay_on_success))

                if attempted_ram is not None:
                    ar = float(attempted_ram)
                    prev_ub = s.op_ram_ub.get(sig)
                    s.op_ram_ub[sig] = ar if prev_ub is None else min(float(prev_ub), ar)

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
            if s.in_queue.get(pid) is not None:
                s.in_queue[pid] = None
            continue

        remaining = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=False)
        if remaining:
            _enqueue(pid, p.priority)

    suspensions = []
    assignments = []

    query_ready = _any_query_ready()

    num_pools = int(s.executor.num_pools)
    pool_avail_cpu = [float(s.executor.pools[i].avail_cpu_pool) for i in range(num_pools)]
    pool_avail_ram = [float(s.executor.pools[i].avail_ram_pool) for i in range(num_pools)]

    backlog = len(s.q[Priority.QUERY]) + len(s.q[Priority.INTERACTIVE]) + len(s.q[Priority.BATCH_PIPELINE])

    def _pool_reserve(pool, eff_pr, pid):
        if not query_ready:
            return (0.0, 0.0)
        if eff_pr == Priority.QUERY:
            return (0.0, 0.0)

        # Allow stalled/aged pipelines to borrow reserved capacity to avoid incomplete penalties.
        disable_after = s.reserve_disable_after_ticks.get(eff_pr, None)
        if disable_after is not None:
            if _pipeline_stall(pid) >= int(disable_after) or _pipeline_age(pid) >= int(disable_after):
                return (0.0, 0.0)

        if eff_pr == Priority.INTERACTIVE:
            return (float(pool.max_cpu_pool) * (float(s.reserve_cpu_frac) * 0.60),
                    float(pool.max_ram_pool) * (float(s.reserve_ram_frac) * 0.60))

        return (float(pool.max_cpu_pool) * float(s.reserve_cpu_frac),
                float(pool.max_ram_pool) * float(s.reserve_ram_frac))

    def _choose_best_placement(op, eff_pr, pid):
        sig = _op_sig(op)

        candidates = []
        for i in range(num_pools):
            if pool_avail_ram[i] >= float(s.init_ram_min) and pool_avail_cpu[i] >= 1.0:
                candidates.append(i)
        if not candidates:
            return None

        # Prefer RAM-headroom pools first (helps big ops; reduces dead-ends).
        candidates.sort(key=lambda i: pool_avail_ram[i], reverse=True)
        candidates = candidates[:8]

        best = None
        best_score = None

        for pool_id in candidates:
            pool = s.executor.pools[pool_id]
            avail_cpu = float(pool_avail_cpu[pool_id])
            avail_ram = float(pool_avail_ram[pool_id])

            reserve_cpu, reserve_ram = _pool_reserve(pool, eff_pr, pid)

            cpu_need = _cpu_need(pool, eff_pr, avail_cpu, reserve_cpu, backlog)
            if cpu_need < 1.0 - 1e-9:
                continue

            ram_need = _ram_estimate(pool, sig, eff_pr)

            # If we can't fit our estimate, for QUERY/INTERACTIVE try a best-effort fit to avoid stalling.
            if ram_need > (avail_ram - reserve_ram) + 1e-9:
                if eff_pr != Priority.BATCH_PIPELINE:
                    fit = max(float(s.init_ram_min), float(avail_ram - reserve_ram))
                    if fit >= float(s.init_ram_min) and fit <= float(avail_ram) + 1e-9:
                        # Only do this if we have no UB yet (still learning) or the pipeline is quite stalled.
                        if (s.op_ram_ub.get(sig) is None) or (_pipeline_stall(pid) >= 25):
                            ram_need = float(fit)
                        else:
                            continue
                    else:
                        continue
                else:
                    continue

            if (avail_cpu - cpu_need) < reserve_cpu - 1e-9:
                continue
            if (avail_ram - ram_need) < reserve_ram - 1e-9:
                continue

            # Best-fit-ish: minimize normalized leftover RAM, then CPU.
            post_ram = avail_ram - ram_need
            post_cpu = avail_cpu - cpu_need
            score = (
                post_ram / max(1.0, float(pool.max_ram_pool)),
                post_cpu / max(1.0, float(pool.max_cpu_pool)),
            )

            if best is None or score < best_score:
                best = (pool_id, float(cpu_need), float(ram_need))
                best_score = score

        return best

    def _find_candidate_for_priority(pr):
        scan_limit = 96

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
                    _dequeue(q, i, pid)
                    scan -= 1
                    continue

                st = p.runtime_status()
                if st.is_pipeline_successful():
                    _dequeue(q, i, pid)
                    scan -= 1
                    continue

                eff_pr = _effective_priority(p)
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

                chosen = None  # (pool_id, cpu, ram, op, eff_pr)
                chosen_ram = None

                # Try a few ops, prefer smaller RAM to improve packing.
                for op in ready_ops[:10]:
                    placement = _choose_best_placement(op, eff_pr, pid)
                    if placement is None:
                        continue
                    pool_id, cpu_need, ram_need = placement
                    if chosen is None or float(ram_need) < float(chosen_ram):
                        chosen = (pool_id, cpu_need, ram_need, op, eff_pr)
                        chosen_ram = ram_need

                if chosen is None:
                    i += 1
                    continue

                age = _pipeline_age(pid)
                since = _pipeline_stall(pid)
                # Prioritize: effective priority, then stalled, then older, then smaller RAM.
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

    def _iter_running_containers():
        # Best-effort introspection; if executor doesn't expose containers, returns empty.
        out = []
        for pool_id in range(num_pools):
            pool = s.executor.pools[pool_id]
            candidates = None
            for attr in ("containers", "running_containers", "running", "active_containers"):
                if hasattr(pool, attr):
                    candidates = getattr(pool, attr)
                    break
            if candidates is None:
                continue
            try:
                if isinstance(candidates, dict):
                    it = list(candidates.values())
                else:
                    it = list(candidates)
            except Exception:
                continue
            for c in it:
                try:
                    cid = getattr(c, "container_id", None)
                    if cid is None and hasattr(c, "id"):
                        cid = getattr(c, "id")
                    pr = getattr(c, "priority", None)
                    cpu = getattr(c, "cpu", None)
                    ram = getattr(c, "ram", None)
                    if cid is None or pr is None or cpu is None or ram is None:
                        continue
                    out.append((cid, pool_id, pr, float(cpu), float(ram)))
                except Exception:
                    continue
        return out

    def _maybe_preempt_for_query():
        # Only preempt if there is ready QUERY work but none can be placed now.
        if not query_ready:
            return False

        # Fast check: can we place at least one QUERY op without preemption?
        picked = _find_candidate_for_priority(Priority.QUERY)
        if picked is not None:
            return False

        running = _iter_running_containers()
        if not running:
            return False

        # Choose victims: prefer BATCH, then INTERACTIVE; avoid repeated churn via cooldown.
        victims = []
        for cid, pool_id, pr, cpu, ram in running:
            if pr == Priority.QUERY:
                continue
            last = s.container_last_preempt.get(cid, -10**9)
            if (s._tick - last) < int(s.preempt_cooldown_ticks):
                continue
            cnt = s.container_preempts.get(cid, 0)
            if cnt >= 3:
                continue
            victims.append((cid, pool_id, pr, cpu, ram))

        if not victims:
            return False

        def _victim_key(v):
            _, _, pr, cpu, ram = v
            # Preempt batch first; then larger RAM; then larger CPU.
            return (_rank(pr), -ram, -cpu)

        victims.sort(key=_victim_key, reverse=True)

        # Preempt a small number to limit churn.
        did = False
        for cid, pool_id, pr, cpu, ram in victims[:4]:
            suspensions.append(Suspend(container_id=cid, pool_id=int(pool_id)))
            s.container_preempts[cid] = s.container_preempts.get(cid, 0) + 1
            s.container_last_preempt[cid] = s._tick
            did = True

        return did

    # If QUERY is blocked, attempt limited preemption and don't over-schedule this tick.
    preempted = _maybe_preempt_for_query()

    # Global scheduling: repeatedly pick by weighted RR.
    max_assignments = 256
    placed = 0

    while placed < max_assignments:
        any_pool_has_room = False
        for i in range(num_pools):
            if pool_avail_cpu[i] >= 1.0 and pool_avail_ram[i] >= float(s.init_ram_min):
                any_pool_has_room = True
                break
        if not any_pool_has_room:
            break

        picked = None
        tried = 0
        seq_len = len(s.rr_seq)

        # If we preempted for QUERY, bias strongly toward QUERY this tick.
        if preempted:
            picked = _find_candidate_for_priority(Priority.QUERY)
            tried = seq_len

        while tried < seq_len and picked is None:
            pr = s.rr_seq[s.rr_idx]
            s.rr_idx = (s.rr_idx + 1) % seq_len
            tried += 1
            picked = _find_candidate_for_priority(pr)

        if picked is None:
            break

        q, idx, pid, pipeline, pool_id, cpu_need, ram_need, op, eff_pr = picked

        _dequeue(q, idx, pid)

        # Final availability check.
        if cpu_need > pool_avail_cpu[pool_id] + 1e-9 or ram_need > pool_avail_ram[pool_id] + 1e-9:
            _enqueue(pid, pipeline.priority)
            break

        # Ensure CPU is a safe integer that cannot exceed availability.
        cpu_need = _quantize_cpu(cpu_need, pool_avail_cpu[pool_id])
        if cpu_need < 1.0 - 1e-9:
            _enqueue(pid, pipeline.priority)
            break

        # Cap RAM to available (avoid any float epsilon over-allocation).
        ram_need = float(min(float(ram_need), float(pool_avail_ram[pool_id])))

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

        st = pipeline.runtime_status()
        if not st.is_pipeline_successful():
            remaining = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=False)
            if remaining:
                _enqueue(pid, pipeline.priority)

    return suspensions, assignments
