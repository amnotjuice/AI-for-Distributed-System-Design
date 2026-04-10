from collections import deque

@register_scheduler_init(key="scheduler_low_001")
def scheduler_low_001_init(s):
    # Per-priority pipeline queues (single membership enforced via s.in_queues).
    s.q_query = deque()
    s.q_interactive = deque()
    s.q_batch = deque()
    s.in_queues = set()  # pipeline_id currently present in any queue

    # Bookkeeping.
    s._tick = 0
    s.pipeline_arrival_tick = {}   # pipeline_id -> tick first seen
    s.pipeline_inflight = {}       # pipeline_id -> number of ops currently assigned/running
    s.pipeline_ooms = {}           # pipeline_id -> oom count

    # Resource learning (shared across pipelines by op type).
    s.op_ram_est = {}              # op_type_key -> RAM estimate (GB)
    s.op_ram_lb = {}               # op_type_key -> known lower bound (GB) from OOMs
    s.op_ram_succ_min = {}         # op_type_key -> minimum successful RAM observed (GB)
    s.op_oom_count = {}            # op_type_key -> count of OOM failures
    s.op_fail_count = {}           # op_type_key -> count of any failures

    # Policy knobs.
    s.min_cpu = 1.0

    # Dynamic scheduling limits (derived per pool each tick; these are minimums).
    s.place_limit_per_pool_min = 192
    s.scan_limit_min = 96

    # Aging / escalation (ticks in scheduler steps).
    # Slightly faster promotion reduces "incomplete => 720s" penalties under sustained load.
    s.batch_to_interactive_ticks = 24
    s.batch_to_query_ticks = 190
    s.interactive_to_query_ticks = 140

    # Per-pipeline parallelism limits (base; actual is scaled with cluster size).
    s.max_inflight_per_pipeline_base = {
        Priority.QUERY: 8,
        Priority.INTERACTIVE: 6,
        Priority.BATCH_PIPELINE: 4,
    }
    s.burst_per_pick_base = {
        Priority.QUERY: 3,
        Priority.INTERACTIVE: 2,
        Priority.BATCH_PIPELINE: 2,
    }

    # Initial RAM guesses: avoid scaling linearly with cluster RAM (prevents massive over-allocation on big clusters).
    s.ram_base = {
        Priority.QUERY: 12.0,
        Priority.INTERACTIVE: 10.0,
        Priority.BATCH_PIPELINE: 8.0,
    }
    # Gentle pool-based fallback for unknown ops; hard-capped via ram_abs_cap below.
    s.ram_frac = {
        Priority.QUERY: 0.012,
        Priority.INTERACTIVE: 0.010,
        Priority.BATCH_PIPELINE: 0.008,
    }
    s.ram_abs_cap = {
        Priority.QUERY: 64.0,
        Priority.INTERACTIVE: 48.0,
        Priority.BATCH_PIPELINE: 40.0,
    }
    s.ram_safety = {
        Priority.QUERY: 1.12,
        Priority.INTERACTIVE: 1.12,
        Priority.BATCH_PIPELINE: 1.14,
    }
    s.ram_op_cap_frac = 0.90  # allow big ops to fit while avoiding extreme single-op monopolization

    # OOM backoff behavior.
    s.oom_backoff_mult = 1.75
    s.oom_backoff_add = 4.0

    # CPU sizing: prefer parallelism (sublinear scaling) and enforce integer vCPU to avoid over-allocation errors.
    s.cpu_base = {
        Priority.QUERY: 8.0,
        Priority.INTERACTIVE: 6.0,
        Priority.BATCH_PIPELINE: 2.0,
    }
    s.cpu_frac = {
        Priority.QUERY: 0.035,
        Priority.INTERACTIVE: 0.025,
        Priority.BATCH_PIPELINE: 0.010,
    }
    # Per-container absolute caps (kept moderate; helps robustness across executor/VM models).
    s.cpu_cap_abs = {
        Priority.QUERY: 32.0,
        Priority.INTERACTIVE: 24.0,
        Priority.BATCH_PIPELINE: 16.0,
    }
    s.cpu_cap_frac = {
        Priority.QUERY: 0.18,
        Priority.INTERACTIVE: 0.15,
        Priority.BATCH_PIPELINE: 0.12,
    }

    # Soft reservations for high priority on the "hi" pool.
    s.reserve_cpu_frac_for_hi = 0.14
    s.reserve_ram_frac_for_hi = 0.14
    s.reserve_cpu_abs_min = 6.0
    s.reserve_ram_abs_min = 24.0

    # Multi-pool bias: prefer (but do not force) running QUERY/INTERACTIVE on pool 0.
    s.hi_pool_id = 0


@register_scheduler(key="scheduler_low_001")
def scheduler_low_001_scheduler(s, results, pipelines):
    def _op_identity(op):
        for attr in ("op_id", "operator_id", "id", "name", "kind", "op_type", "type"):
            if hasattr(op, attr):
                try:
                    v = getattr(op, attr)
                    if v is not None:
                        return str(v)
                except Exception:
                    pass
        try:
            return op.__class__.__name__
        except Exception:
            return repr(op)

    def _op_type_key(op):
        for attr in ("kind", "op_type", "name", "type"):
            if hasattr(op, attr):
                try:
                    v = getattr(op, attr)
                    if v:
                        return str(v)
                except Exception:
                    pass
        return _op_identity(op)

    def _op_instance_key(pipeline_id, op):
        return f"{pipeline_id}:{_op_identity(op)}"

    def _is_oom_error(err):
        if not err:
            return False
        msg = str(err).lower()
        return ("oom" in msg) or ("out of memory" in msg) or ("out_of_memory" in msg) or ("memoryerror" in msg)

    def _remove_at_deque(q, idx):
        q.rotate(-idx)
        item = q.popleft()
        q.rotate(idx)
        return item

    def _prune_front(q, limit=256):
        for _ in range(min(len(q), int(limit))):
            p = q[0]
            try:
                if p.runtime_status().is_pipeline_successful():
                    pid = p.pipeline_id
                    q.popleft()
                    s.in_queues.discard(pid)
                    continue
            except Exception:
                pass
            break

    def _waited_ticks(pid):
        return int(s._tick - s.pipeline_arrival_tick.get(pid, s._tick))

    def _effective_priority(p):
        pr = p.priority
        pid = p.pipeline_id
        waited = _waited_ticks(pid)

        if pr == Priority.QUERY:
            return Priority.QUERY

        if pr == Priority.INTERACTIVE:
            if waited >= int(s.interactive_to_query_ticks):
                return Priority.QUERY
            return Priority.INTERACTIVE

        if waited >= int(s.batch_to_query_ticks):
            return Priority.QUERY
        if waited >= int(s.batch_to_interactive_ticks):
            return Priority.INTERACTIVE
        return Priority.BATCH_PIPELINE

    def _queue_for_effective_priority(epr):
        if epr == Priority.QUERY:
            return s.q_query
        if epr == Priority.INTERACTIVE:
            return s.q_interactive
        return s.q_batch

    def _enqueue(p):
        pid = p.pipeline_id
        if pid in s.in_queues:
            return
        st = None
        try:
            st = p.runtime_status()
        except Exception:
            st = None
        if st is not None:
            try:
                if st.is_pipeline_successful():
                    return
            except Exception:
                pass
        epr = _effective_priority(p)
        _queue_for_effective_priority(epr).append(p)
        s.in_queues.add(pid)

    def _reclassify_queue(q, scan_limit=256):
        limit = min(len(q), int(scan_limit))
        for _ in range(limit):
            p = q.popleft()
            pid = p.pipeline_id
            s.in_queues.discard(pid)

            st = None
            try:
                st = p.runtime_status()
            except Exception:
                st = None
            if st is not None:
                try:
                    if st.is_pipeline_successful():
                        continue
                except Exception:
                    pass
            _enqueue(p)

    def _cluster_total_max_cpu():
        tot = 0.0
        try:
            for i in range(int(s.executor.num_pools)):
                try:
                    tot += float(s.executor.pools[int(i)].max_cpu_pool)
                except Exception:
                    pass
        except Exception:
            tot = 64.0
        return max(1.0, tot)

    def _cluster_scale_factor():
        # ~1.0 for 64 CPUs total, grows as sqrt to avoid runaway parallelism.
        tot = _cluster_total_max_cpu()
        sf = (float(tot) / 64.0) ** 0.5
        if sf < 1.0:
            sf = 1.0
        if sf > 6.0:
            sf = 6.0
        return sf

    def _inflight_limit(epr):
        base = int(s.max_inflight_per_pipeline_base.get(epr, 3))
        sf = _cluster_scale_factor()
        # Mild scaling: improves makespan/latency on large clusters without letting single pipelines dominate.
        mult = int(sf + 0.00001)
        if mult < 1:
            mult = 1
        cap = base * mult
        # Upper bounds to prevent pathologically large DAGs from flooding the cluster.
        if epr == Priority.QUERY:
            return min(cap, 48)
        if epr == Priority.INTERACTIVE:
            return min(cap, 36)
        return min(cap, 24)

    def _burst_cap(epr):
        base = int(s.burst_per_pick_base.get(epr, 1))
        sf = _cluster_scale_factor()
        mult = int((sf * 0.9) + 1.00001)
        if mult < 1:
            mult = 1
        burst = base * mult
        if epr == Priority.QUERY:
            return min(burst, 10)
        if epr == Priority.INTERACTIVE:
            return min(burst, 8)
        return min(burst, 6)

    def _ram_guess(pool, epr, op, pid):
        opk = _op_type_key(op)

        lb = float(s.op_ram_lb.get(opk, 0.0) or 0.0)
        est = s.op_ram_est.get(opk)

        if est is None:
            base = float(s.ram_base.get(epr, 8.0))
            frac = float(s.ram_frac.get(epr, 0.008))
            abs_cap = float(s.ram_abs_cap.get(epr, 48.0))

            pool_based = float(pool.max_ram_pool) * frac
            # Cap pool-based fallback so it doesn't explode on huge clusters.
            fallback = max(base, min(pool_based, abs_cap))
            est = fallback
        est = float(est)

        # Per-pipeline extra safety after repeated OOMs to converge quickly.
        po = int(s.pipeline_ooms.get(pid, 0) or 0)
        pipeline_safety = 1.0
        if po > 0:
            pipeline_safety = min(1.35, 1.0 + 0.10 * float(min(3, po)))

        safety = float(s.ram_safety.get(epr, 1.12)) * pipeline_safety

        target = max(est, lb) * safety

        # Cap to avoid pathological single-op allocations, but still allow large ops to fit.
        cap = float(pool.max_ram_pool) * float(s.ram_op_cap_frac)
        if cap > 1.0:
            target = min(target, cap)

        target = max(1.0, target)
        return float(target)

    def _quantize_cpu(cpu, avail_cpu):
        # Enforce integer CPU allocations and strict <= available to avoid "Overallocated CPU" failures.
        if cpu is None:
            return 0
        try:
            cpu_f = float(cpu)
        except Exception:
            return 0
        try:
            avail_f = float(avail_cpu)
        except Exception:
            avail_f = 0.0

        if avail_f <= 0.0:
            return 0

        # Cap to available.
        if cpu_f > avail_f:
            cpu_f = avail_f

        # Integer vCPU (floor).
        cpu_i = int(cpu_f + 1e-9)
        if cpu_i < 1:
            # If we have at least 1 CPU available, take 1.
            if avail_f >= 1.0:
                cpu_i = 1
            else:
                return 0
        # Final guard.
        if float(cpu_i) > avail_f + 1e-9:
            cpu_i = int(avail_f + 1e-9)
        if cpu_i < 1:
            return 0
        return cpu_i

    def _cpu_guess(pool, epr, avail_cpu):
        base = float(s.cpu_base.get(epr, 2.0))
        frac = float(s.cpu_frac.get(epr, 0.01))
        cap_frac = float(s.cpu_cap_frac.get(epr, 0.12))
        cap_abs = float(s.cpu_cap_abs.get(epr, 16.0))

        target = max(base, float(pool.max_cpu_pool) * frac)
        cap = min(float(pool.max_cpu_pool) * cap_frac, cap_abs)
        cpu = min(float(avail_cpu), float(target), float(cap))
        cpu = max(float(s.min_cpu), cpu)
        return _quantize_cpu(cpu, avail_cpu)

    def _dynamic_limits(pool):
        # More aggressive fill on larger pools but bounded to avoid overhead.
        try:
            mc = float(pool.max_cpu_pool)
        except Exception:
            mc = 64.0
        place_limit = int(max(s.place_limit_per_pool_min, min(4096, mc * 3.0)))
        scan_limit = int(max(s.scan_limit_min, min(2048, mc * 2.0)))
        return place_limit, scan_limit

    def _hi_backlog_exists():
        return (len(s.q_query) > 0) or (len(s.q_interactive) > 0)

    def _select_pipeline_index_with_fit(q, pool, avail_cpu, avail_ram, scheduled_this_tick, scan_limit, prefer_wait):
        best_idx = None
        best_score = None  # higher is better

        scan = min(len(q), int(scan_limit))
        for i in range(scan):
            p = q[i]
            pid = p.pipeline_id

            st = None
            try:
                st = p.runtime_status()
            except Exception:
                st = None
            if st is None:
                continue
            try:
                if st.is_pipeline_successful():
                    continue
            except Exception:
                pass

            epr = _effective_priority(p)

            inflight = int(s.pipeline_inflight.get(pid, 0))
            if inflight >= _inflight_limit(epr):
                continue

            try:
                ready = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
            except Exception:
                ready = []
            if not ready:
                continue

            fit_ram = None
            checked = 0
            for op in ready:
                if checked >= 12:
                    break
                checked += 1
                instk = _op_instance_key(pid, op)
                if instk in scheduled_this_tick:
                    continue
                ram_need = _ram_guess(pool, epr, op, pid)
                if float(ram_need) <= float(avail_ram) and float(avail_cpu) >= float(s.min_cpu):
                    if fit_ram is None:
                        fit_ram = float(ram_need)
                    else:
                        # For hi-priority, prefer smaller RAM ops (proxy for shorter runtime);
                        # for batch packing, prefer larger RAM ops.
                        if prefer_wait:
                            if float(ram_need) < float(fit_ram):
                                fit_ram = float(ram_need)
                        else:
                            if float(ram_need) > float(fit_ram):
                                fit_ram = float(ram_need)

            if fit_ram is None:
                continue

            waited = _waited_ticks(pid)
            if prefer_wait:
                # Tail protection + SRPT-ish: more waited is better; smaller op is better.
                score = waited * 1000.0 - (fit_ram * 0.25) - inflight * 8.0
            else:
                # Packing: fill RAM efficiently; still consider age.
                score = (fit_ram / max(1.0, float(avail_ram))) * 1000.0 + waited * 6.0 - inflight * 4.0

            if (best_score is None) or (score > best_score):
                best_score = score
                best_idx = i

        return best_idx

    def _schedule_from_queue(q, pool, pool_id, avail_cpu, avail_ram, scheduled_this_tick, scan_limit, reserve_cpu, reserve_ram, prefer_wait):
        if not q:
            return avail_cpu, avail_ram, []

        idx = _select_pipeline_index_with_fit(
            q, pool, avail_cpu, avail_ram, scheduled_this_tick, scan_limit=int(scan_limit), prefer_wait=bool(prefer_wait)
        )
        if idx is None:
            return avail_cpu, avail_ram, []

        p = _remove_at_deque(q, idx)
        pid = p.pipeline_id
        s.in_queues.discard(pid)

        st = None
        try:
            st = p.runtime_status()
        except Exception:
            st = None

        if st is None:
            _enqueue(p)
            return avail_cpu, avail_ram, []

        try:
            if st.is_pipeline_successful():
                return avail_cpu, avail_ram, []
        except Exception:
            pass

        epr = _effective_priority(p)
        inflight = int(s.pipeline_inflight.get(pid, 0))
        inflight_cap = _inflight_limit(epr)
        burst_cap = _burst_cap(epr)
        to_launch_cap = max(0, min(int(burst_cap), int(inflight_cap - inflight)))

        try:
            ready_ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        except Exception:
            ready_ops = []

        if (not ready_ops) or (to_launch_cap <= 0):
            _enqueue(p)
            return avail_cpu, avail_ram, []

        cand = []
        for op in ready_ops:
            instk = _op_instance_key(pid, op)
            if instk in scheduled_this_tick:
                continue
            ram_need = _ram_guess(pool, epr, op, pid)
            cand.append((float(ram_need), op, instk))

        # Hi-priority: smaller RAM first (proxy for shorter jobs).
        # Batch: larger RAM first (packing).
        cand.sort(key=lambda x: x[0], reverse=(not prefer_wait))

        new_assignments = []
        launched = 0

        for ram_need, op, instk in cand:
            if launched >= to_launch_cap:
                break

            # Respect soft reservations.
            if float(avail_cpu) < float(s.min_cpu):
                break
            if float(avail_ram) < 1.0:
                break
            if float(avail_cpu) - float(s.min_cpu) < float(reserve_cpu):
                break
            if float(avail_ram) - float(max(1.0, ram_need)) < float(reserve_ram):
                break

            if float(avail_ram) < float(ram_need):
                continue

            cpu_i = _cpu_guess(pool, epr, avail_cpu)
            # Ensure we don't dip into reserved headroom.
            max_cpu_allowed = float(avail_cpu) - float(reserve_cpu)
            cpu_i = _quantize_cpu(min(float(cpu_i), max_cpu_allowed), avail_cpu)
            if cpu_i < 1:
                continue

            ram = min(float(ram_need), float(avail_ram) - float(reserve_ram), float(pool.max_ram_pool))
            ram = max(1.0, ram)

            # Strict safety checks.
            if float(ram) > float(avail_ram) + 1e-9:
                continue
            if float(cpu_i) > float(avail_cpu) + 1e-9:
                continue

            new_assignments.append(
                Assignment(
                    ops=[op],
                    cpu=float(cpu_i),
                    ram=float(ram),
                    priority=p.priority,  # preserve original label for metrics
                    pool_id=pool_id,
                    pipeline_id=pid,
                )
            )
            scheduled_this_tick.add(instk)
            avail_cpu -= float(cpu_i)
            avail_ram -= float(ram)
            launched += 1
            s.pipeline_inflight[pid] = int(s.pipeline_inflight.get(pid, 0)) + 1

        _enqueue(p)
        return avail_cpu, avail_ram, new_assignments

    # Advance time tick.
    s._tick += 1

    # Update from results: reduce inflight counts and learn RAM on failures/successes.
    for r in results:
        if not r or not getattr(r, "ops", None):
            continue

        pid = getattr(r, "pipeline_id", None)

        # Decrement inflight.
        if pid is not None:
            finished_ops = 1
            try:
                finished_ops = max(1, len(r.ops))
            except Exception:
                finished_ops = 1
            s.pipeline_inflight[pid] = max(0, int(s.pipeline_inflight.get(pid, 0)) - int(finished_ops))

        failed = False
        try:
            failed = r.failed()
        except Exception:
            failed = bool(getattr(r, "error", None))

        attempted_ram = float(getattr(r, "ram", 0.0) or 0.0)
        if attempted_ram <= 0.0:
            attempted_ram = 0.0

        if failed:
            was_oom = _is_oom_error(getattr(r, "error", None))
            if was_oom and (pid is not None):
                s.pipeline_ooms[pid] = int(s.pipeline_ooms.get(pid, 0)) + 1

            pool_id = getattr(r, "pool_id", None)
            pool_max_ram = None
            if pool_id is not None and 0 <= int(pool_id) < int(s.executor.num_pools):
                try:
                    pool_max_ram = float(s.executor.pools[int(pool_id)].max_ram_pool)
                except Exception:
                    pool_max_ram = None

            for op in r.ops:
                opk = _op_type_key(op)
                s.op_fail_count[opk] = int(s.op_fail_count.get(opk, 0)) + 1

                if was_oom:
                    s.op_oom_count[opk] = int(s.op_oom_count.get(opk, 0)) + 1
                    oc = int(s.op_oom_count.get(opk, 1))

                    prev_est = float(s.op_ram_est.get(opk, max(1.0, attempted_ram if attempted_ram > 0 else 1.0)))
                    prev_lb = float(s.op_ram_lb.get(opk, 0.0) or 0.0)
                    base_attempt = attempted_ram if attempted_ram > 0 else prev_est

                    # Increase lower bound and estimate to converge quickly (few retries).
                    mult = float(s.oom_backoff_mult) + min(0.25, 0.05 * float(max(0, oc - 1)))
                    new_lb = max(prev_lb, base_attempt + 1.0, base_attempt * (1.18 + 0.06 * min(5, oc)))
                    new_est = max(prev_est, base_attempt * mult, base_attempt + float(s.oom_backoff_add), new_lb * 1.10)

                    if pool_max_ram is not None:
                        new_lb = min(new_lb, pool_max_ram)
                        new_est = min(new_est, pool_max_ram)

                    s.op_ram_lb[opk] = max(1.0, float(new_lb))
                    s.op_ram_est[opk] = max(1.0, float(new_est))
                else:
                    # For non-OOM failures: don't reduce; keep at least attempted.
                    if attempted_ram > 0:
                        prev = float(s.op_ram_est.get(opk, 0.0) or 0.0)
                        s.op_ram_est[opk] = max(prev, attempted_ram, float(s.op_ram_lb.get(opk, 0.0) or 0.0))
        else:
            # On success: record minimal successful RAM and gently tighten estimate, never below lb.
            if attempted_ram > 0:
                for op in r.ops:
                    opk = _op_type_key(op)
                    lb = float(s.op_ram_lb.get(opk, 0.0) or 0.0)

                    succ_min = s.op_ram_succ_min.get(opk)
                    if succ_min is None:
                        s.op_ram_succ_min[opk] = attempted_ram
                    else:
                        try:
                            s.op_ram_succ_min[opk] = min(float(succ_min), attempted_ram)
                        except Exception:
                            s.op_ram_succ_min[opk] = attempted_ram

                    prev = s.op_ram_est.get(opk)
                    if prev is None:
                        s.op_ram_est[opk] = max(1.0, max(lb, attempted_ram))
                    else:
                        prevf = float(prev)
                        target = max(lb, float(s.op_ram_succ_min.get(opk, attempted_ram)) * 1.05)
                        if target >= prevf:
                            new_est = target
                        else:
                            # Tighten slowly to avoid oscillation.
                            new_est = max(lb, prevf * 0.975 + target * 0.025)
                        s.op_ram_est[opk] = max(1.0, float(new_est))

    # Ingest pipelines (avoid duplicating queue membership).
    for p in pipelines:
        pid = p.pipeline_id
        if pid not in s.pipeline_arrival_tick:
            s.pipeline_arrival_tick[pid] = s._tick
        _enqueue(p)

    # Prune and reclassify (aging promotions).
    _prune_front(s.q_query, limit=384)
    _prune_front(s.q_interactive, limit=384)
    _prune_front(s.q_batch, limit=384)

    _reclassify_queue(s.q_batch, scan_limit=512)
    _reclassify_queue(s.q_interactive, scan_limit=384)

    scheduled_this_tick = set()
    assignments = []
    suspensions = []

    # Pool scheduling order: prefer pool 0 first, but allow all pools to serve all priorities.
    pool_order = list(range(int(s.executor.num_pools)))
    if int(s.executor.num_pools) > 1 and s.hi_pool_id in pool_order:
        pool_order.remove(s.hi_pool_id)
        pool_order = [s.hi_pool_id] + pool_order

    hi_backlog = _hi_backlog_exists()

    for pool_id in pool_order:
        pool = s.executor.pools[int(pool_id)]
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)

        # Work with integer-ish CPU budget to avoid float accumulation issues.
        avail_cpu = float(int(avail_cpu + 1e-9))

        place_limit, scan_limit = _dynamic_limits(pool)

        reserve_cpu_hi = max(float(s.reserve_cpu_abs_min), float(pool.max_cpu_pool) * float(s.reserve_cpu_frac_for_hi))
        reserve_ram_hi = max(float(s.reserve_ram_abs_min), float(pool.max_ram_pool) * float(s.reserve_ram_frac_for_hi))

        placed = 0
        while placed < int(place_limit) and avail_cpu >= float(s.min_cpu) and avail_ram >= 1.0:
            before = len(assignments)

            # QUERY first.
            avail_cpu, avail_ram, new_asg = _schedule_from_queue(
                s.q_query,
                pool,
                pool_id,
                avail_cpu,
                avail_ram,
                scheduled_this_tick,
                scan_limit=scan_limit,
                reserve_cpu=0.0,
                reserve_ram=0.0,
                prefer_wait=True,
            )
            if new_asg:
                assignments.extend(new_asg)
                placed += len(new_asg)
                continue

            # INTERACTIVE next.
            avail_cpu, avail_ram, new_asg = _schedule_from_queue(
                s.q_interactive,
                pool,
                pool_id,
                avail_cpu,
                avail_ram,
                scheduled_this_tick,
                scan_limit=scan_limit,
                reserve_cpu=0.0,
                reserve_ram=0.0,
                prefer_wait=True,
            )
            if new_asg:
                assignments.extend(new_asg)
                placed += len(new_asg)
                continue

            # BATCH: packing, with reservations primarily on the hi pool.
            reserve_cpu = 0.0
            reserve_ram = 0.0
            if hi_backlog:
                if int(s.executor.num_pools) > 1 and int(pool_id) == int(s.hi_pool_id):
                    reserve_cpu = reserve_cpu_hi
                    reserve_ram = reserve_ram_hi
                else:
                    # Light reservation on other pools to keep some headroom but still make batch progress.
                    reserve_cpu = max(0.0, reserve_cpu_hi * 0.35)
                    reserve_ram = max(0.0, reserve_ram_hi * 0.35)

            avail_cpu, avail_ram, new_asg = _schedule_from_queue(
                s.q_batch,
                pool,
                pool_id,
                avail_cpu,
                avail_ram,
                scheduled_this_tick,
                scan_limit=scan_limit,
                reserve_cpu=reserve_cpu,
                reserve_ram=reserve_ram,
                prefer_wait=False,
            )
            if new_asg:
                assignments.extend(new_asg)
                placed += len(new_asg)
                continue

            # Nothing fits.
            if len(assignments) == before:
                break

    return suspensions, assignments
