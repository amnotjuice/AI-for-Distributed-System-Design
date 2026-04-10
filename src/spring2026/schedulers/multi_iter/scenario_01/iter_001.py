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

    # Resource learning (keyed by a (mostly) stable operator identity; shared across pipelines).
    s.op_ram_est = {}              # op_type_key -> ram estimate
    s.op_oom_count = {}            # op_type_key -> count of OOM failures
    s.op_fail_count = {}           # op_type_key -> count of any failures

    # Policy knobs.
    s.min_cpu = 1.0
    s.place_limit_per_pool = 64
    s.scan_limit = 48

    # Batch aging to prevent starvation (promote to interactive after wait).
    s.batch_promote_ticks = 40

    # Per-pipeline parallelism limits (avoid one pipeline monopolizing the cluster).
    s.max_inflight_per_pipeline = {
        Priority.QUERY: 6,
        Priority.INTERACTIVE: 4,
        Priority.BATCH_PIPELINE: 3,
    }
    s.burst_per_pick = {
        Priority.QUERY: 2,
        Priority.INTERACTIVE: 2,
        Priority.BATCH_PIPELINE: 1,
    }

    # Initial RAM guesses: start fairly aggressive (low) to maximize utilization; rely on fast OOM backoff.
    s.ram_base = {
        Priority.QUERY: 8.0,
        Priority.INTERACTIVE: 6.0,
        Priority.BATCH_PIPELINE: 4.0,
    }
    s.ram_frac = {
        Priority.QUERY: 0.030,
        Priority.INTERACTIVE: 0.025,
        Priority.BATCH_PIPELINE: 0.020,
    }
    s.ram_op_cap_frac = 0.65  # never allocate > this fraction of a pool to a single op by default

    # OOM backoff behavior.
    s.oom_backoff_mult = 1.8
    s.oom_backoff_add = 2.0

    # CPU sizing: keep moderate to allow concurrency; boost on a dedicated "high-priority" pool when present.
    s.cpu_base = {
        Priority.QUERY: 8.0,
        Priority.INTERACTIVE: 6.0,
        Priority.BATCH_PIPELINE: 2.0,
    }
    s.cpu_hi_pool_frac = {
        Priority.QUERY: 0.30,
        Priority.INTERACTIVE: 0.20,
        Priority.BATCH_PIPELINE: 0.00,
    }
    s.cpu_cap_frac = {
        Priority.QUERY: 0.75,
        Priority.INTERACTIVE: 0.60,
        Priority.BATCH_PIPELINE: 0.50,
    }

    # If we have multiple pools, prefer to run QUERY/INTERACTIVE on pool 0.
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
        # Shared across pipelines to make OOM learning transferable.
        # Prefer stable "kind/name/type" if available; otherwise fall back.
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
        # Used only for "already scheduled this tick" de-duplication.
        return f"{pipeline_id}:{_op_identity(op)}"

    def _is_oom_error(err):
        if not err:
            return False
        msg = str(err).lower()
        return ("oom" in msg) or ("out of memory" in msg) or ("out_of_memory" in msg) or ("memoryerror" in msg)

    def _queue_for_priority(pr):
        if pr == Priority.QUERY:
            return s.q_query
        if pr == Priority.INTERACTIVE:
            return s.q_interactive
        return s.q_batch

    def _enqueue(p):
        pid = p.pipeline_id
        if pid in s.in_queues:
            return
        st = p.runtime_status()
        if st.is_pipeline_successful():
            return
        _queue_for_priority(p.priority).append(p)
        s.in_queues.add(pid)

    def _remove_at_deque(q, idx):
        # Remove and return element at idx from deque q.
        q.rotate(-idx)
        item = q.popleft()
        q.rotate(idx)
        return item

    def _promote_aged_batch():
        # Bounded promotion to prevent batch starvation and reduce scan overhead.
        limit = min(len(s.q_batch), 64)
        for _ in range(limit):
            p = s.q_batch.popleft()
            pid = p.pipeline_id
            s.in_queues.discard(pid)

            st = p.runtime_status()
            if st.is_pipeline_successful():
                continue

            at = s.pipeline_arrival_tick.get(pid, s._tick)
            if (s._tick - at) >= s.batch_promote_ticks:
                s.q_interactive.append(p)
                s.in_queues.add(pid)
            else:
                s.q_batch.append(p)
                s.in_queues.add(pid)

    def _prune_front(q, limit=48):
        for _ in range(min(len(q), limit)):
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

    def _inflight_limit(priority):
        return int(s.max_inflight_per_pipeline.get(priority, 2))

    def _ram_guess(pool, pr, op):
        opk = _op_type_key(op)
        est = s.op_ram_est.get(opk)
        if est is None:
            base = float(s.ram_base.get(pr, 4.0))
            frac = float(s.ram_frac.get(pr, 0.02))
            est = max(base, float(pool.max_ram_pool) * frac)
        est = float(est)
        cap = float(pool.max_ram_pool) * float(s.ram_op_cap_frac)
        est = min(est, cap if cap > 0 else est)
        est = max(1.0, est)
        return est

    def _cpu_guess(pool, pr, pool_id, avail_cpu):
        base = float(s.cpu_base.get(pr, 2.0))
        if (s.executor.num_pools > 1) and (pool_id == s.hi_pool_id):
            frac = float(s.cpu_hi_pool_frac.get(pr, 0.0))
            base = max(base, float(pool.max_cpu_pool) * frac)
        cap = float(pool.max_cpu_pool) * float(s.cpu_cap_frac.get(pr, 0.5))
        cpu = min(float(avail_cpu), float(cap), float(base))
        cpu = max(float(s.min_cpu), cpu)
        return cpu

    def _select_pipeline_index_with_fit(q, pool, pool_id, avail_cpu, avail_ram, pr, scheduled_this_tick):
        # Find a pipeline (by index in deque) that has at least one READY op that fits,
        # preferring larger RAM ops (best utilization, less fragmentation).
        best = None  # (ram_need, -wait, idx)
        best_ram = None
        best_wait = None
        best_idx = None

        scan = min(len(q), int(s.scan_limit))
        for i in range(scan):
            p = q[i]
            pid = p.pipeline_id
            try:
                st = p.runtime_status()
            except Exception:
                continue

            if st.is_pipeline_successful():
                continue

            inflight = int(s.pipeline_inflight.get(pid, 0))
            if inflight >= _inflight_limit(p.priority):
                continue

            try:
                ready = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
            except Exception:
                ready = []

            if not ready:
                continue

            # Check if any ready op fits; prefer the largest RAM that fits.
            waited = int(s._tick - s.pipeline_arrival_tick.get(pid, s._tick))
            best_fit_ram_for_p = None
            for op in ready[:8]:
                instk = _op_instance_key(pid, op)
                if instk in scheduled_this_tick:
                    continue
                ram_need = _ram_guess(pool, pr, op)
                if ram_need <= float(avail_ram) and float(s.min_cpu) <= float(avail_cpu):
                    if (best_fit_ram_for_p is None) or (ram_need > best_fit_ram_for_p):
                        best_fit_ram_for_p = ram_need

            if best_fit_ram_for_p is None:
                continue

            if (best is None) or (best_fit_ram_for_p > best_ram) or (best_fit_ram_for_p == best_ram and waited > best_wait):
                best = (best_fit_ram_for_p, waited, i)
                best_ram = best_fit_ram_for_p
                best_wait = waited
                best_idx = i

        return best_idx

    def _schedule_from_queue(q, pool, pool_id, avail_cpu, avail_ram, scheduled_this_tick, allow_batch_on_hi_pool):
        if not q:
            return avail_cpu, avail_ram, []

        # If this is the dedicated high-priority pool, optionally disallow batch unless nothing else to do.
        if (s.executor.num_pools > 1) and (pool_id == s.hi_pool_id) and (not allow_batch_on_hi_pool):
            # We'll enforce this by simply not calling this function for batch on hi pool,
            # except when explicitly allowed.
            pass

        # Determine priority class for sizing decisions:
        # use the pipeline's true priority (promotion already moved aged batch).
        pr_for_sizing = None
        if len(q) > 0:
            pr_for_sizing = q[0].priority
        else:
            return avail_cpu, avail_ram, []

        idx = _select_pipeline_index_with_fit(q, pool, pool_id, avail_cpu, avail_ram, pr_for_sizing, scheduled_this_tick)
        if idx is None:
            return avail_cpu, avail_ram, []

        p = _remove_at_deque(q, idx)
        pid = p.pipeline_id
        s.in_queues.discard(pid)

        new_assignments = []
        try:
            st = p.runtime_status()
        except Exception:
            st = None

        if st is None or (hasattr(st, "is_pipeline_successful") and st.is_pipeline_successful()):
            return avail_cpu, avail_ram, []

        inflight = int(s.pipeline_inflight.get(pid, 0))
        inflight_cap = _inflight_limit(p.priority)
        burst_cap = int(s.burst_per_pick.get(p.priority, 1))
        to_launch_cap = max(0, min(burst_cap, inflight_cap - inflight))

        try:
            ready_ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        except Exception:
            ready_ops = []

        if not ready_ops or to_launch_cap <= 0:
            # Requeue and move on.
            _enqueue(p)
            return avail_cpu, avail_ram, []

        # Choose up to to_launch_cap ready ops, preferring larger RAM first.
        cand = []
        for op in ready_ops:
            instk = _op_instance_key(pid, op)
            if instk in scheduled_this_tick:
                continue
            ram_need = _ram_guess(pool, p.priority, op)
            cand.append((ram_need, op, instk))
        cand.sort(key=lambda x: x[0], reverse=True)

        launched = 0
        for ram_need, op, instk in cand:
            if launched >= to_launch_cap:
                break
            if float(avail_ram) < float(ram_need) or float(avail_cpu) < float(s.min_cpu):
                continue

            cpu = _cpu_guess(pool, p.priority, pool_id, avail_cpu)
            if float(cpu) < float(s.min_cpu):
                continue
            if float(cpu) > float(avail_cpu):
                cpu = float(avail_cpu)

            # Clamp RAM to available and pool max; keep at least 1.0.
            ram = max(1.0, min(float(ram_need), float(avail_ram), float(pool.max_ram_pool)))

            # Final fit check.
            if float(ram) > float(avail_ram) or float(cpu) > float(avail_cpu):
                continue

            new_assignments.append(
                Assignment(
                    ops=[op],
                    cpu=cpu,
                    ram=ram,
                    priority=p.priority,
                    pool_id=pool_id,
                    pipeline_id=pid,
                )
            )
            scheduled_this_tick.add(instk)
            avail_cpu -= cpu
            avail_ram -= ram
            launched += 1
            s.pipeline_inflight[pid] = int(s.pipeline_inflight.get(pid, 0)) + 1

        # Requeue pipeline if not done (keep progress); if we launched nothing, requeue anyway.
        _enqueue(p)
        return avail_cpu, avail_ram, new_assignments

    # Advance time tick.
    s._tick += 1

    # Update from results: reduce inflight counts and learn RAM on failures.
    for r in results:
        if not r or not getattr(r, "ops", None):
            continue

        pid = getattr(r, "pipeline_id", None)
        if pid is not None:
            finished_ops = 0
            try:
                finished_ops = len(r.ops)
            except Exception:
                finished_ops = 1
            if finished_ops > 0:
                s.pipeline_inflight[pid] = max(0, int(s.pipeline_inflight.get(pid, 0)) - int(finished_ops))

        failed = False
        try:
            failed = r.failed()
        except Exception:
            failed = bool(getattr(r, "error", None))

        if failed:
            was_oom = _is_oom_error(getattr(r, "error", None))
            attempted_ram = float(getattr(r, "ram", 0.0) or 0.0)
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
                    prev = float(s.op_ram_est.get(opk, attempted_ram if attempted_ram > 0 else 1.0))
                    base_attempt = attempted_ram if attempted_ram > 0 else prev
                    new_est = max(prev * float(s.oom_backoff_mult), base_attempt * float(s.oom_backoff_mult), base_attempt + float(s.oom_backoff_add))
                    if pool_max_ram is not None:
                        new_est = min(new_est, pool_max_ram)
                    s.op_ram_est[opk] = max(1.0, float(new_est))
                else:
                    # For non-OOM failures, keep estimate at least as large as what we attempted.
                    if attempted_ram > 0:
                        prev = float(s.op_ram_est.get(opk, 0.0))
                        s.op_ram_est[opk] = max(prev, attempted_ram)
        else:
            # On success, keep estimate as the minimum of previous and attempted ram, but don't shrink too aggressively.
            attempted_ram = float(getattr(r, "ram", 0.0) or 0.0)
            if attempted_ram > 0:
                for op in r.ops:
                    opk = _op_type_key(op)
                    prev = s.op_ram_est.get(opk)
                    if prev is None:
                        s.op_ram_est[opk] = attempted_ram
                    else:
                        prevf = float(prev)
                        # Gentle decay to recover from over-allocation without causing oscillation.
                        s.op_ram_est[opk] = max(1.0, min(prevf, prevf * 0.90 + attempted_ram * 0.10))

    # Ingest pipelines (avoid duplicating queue membership).
    for p in pipelines:
        pid = p.pipeline_id
        if pid not in s.pipeline_arrival_tick:
            s.pipeline_arrival_tick[pid] = s._tick
        _enqueue(p)

    # Aging promotion and pruning.
    _promote_aged_batch()
    _prune_front(s.q_query)
    _prune_front(s.q_interactive)
    _prune_front(s.q_batch)

    # Scheduling.
    scheduled_this_tick = set()
    assignments = []
    suspensions = []

    # Pool scheduling order: prefer running QUERY/INTERACTIVE on hi pool (0) if multiple pools.
    pool_order = list(range(int(s.executor.num_pools)))
    if int(s.executor.num_pools) > 1 and s.hi_pool_id in pool_order:
        pool_order.remove(s.hi_pool_id)
        pool_order = [s.hi_pool_id] + pool_order

    for pool_id in pool_order:
        pool = s.executor.pools[int(pool_id)]
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)

        placed = 0
        while placed < int(s.place_limit_per_pool) and avail_cpu >= float(s.min_cpu) and avail_ram >= 1.0:
            before = len(assignments)

            # If we have multiple pools, keep pool 0 biased toward QUERY/INTERACTIVE.
            allow_batch_on_hi_pool = True
            if int(s.executor.num_pools) > 1 and int(pool_id) == int(s.hi_pool_id):
                # Only allow batch if no query/interactive progress is possible on this pool right now.
                allow_batch_on_hi_pool = False

            # Try QUERY then INTERACTIVE.
            avail_cpu, avail_ram, new_asg = _schedule_from_queue(
                s.q_query, pool, pool_id, avail_cpu, avail_ram, scheduled_this_tick, allow_batch_on_hi_pool=True
            )
            if new_asg:
                assignments.extend(new_asg)
                placed += len(new_asg)
                continue

            avail_cpu, avail_ram, new_asg = _schedule_from_queue(
                s.q_interactive, pool, pool_id, avail_cpu, avail_ram, scheduled_this_tick, allow_batch_on_hi_pool=True
            )
            if new_asg:
                assignments.extend(new_asg)
                placed += len(new_asg)
                continue

            # Batch: on the hi pool, only if nothing else can be scheduled there.
            if (int(s.executor.num_pools) == 1) or (int(pool_id) != int(s.hi_pool_id)) or allow_batch_on_hi_pool:
                avail_cpu, avail_ram, new_asg = _schedule_from_queue(
                    s.q_batch, pool, pool_id, avail_cpu, avail_ram, scheduled_this_tick, allow_batch_on_hi_pool=True
                )
                if new_asg:
                    assignments.extend(new_asg)
                    placed += len(new_asg)
                    continue

            # If hi pool and we disallowed batch, but the hi queues have no fit, backfill with batch to avoid idling.
            if int(s.executor.num_pools) > 1 and int(pool_id) == int(s.hi_pool_id):
                avail_cpu, avail_ram, new_asg = _schedule_from_queue(
                    s.q_batch, pool, pool_id, avail_cpu, avail_ram, scheduled_this_tick, allow_batch_on_hi_pool=True
                )
                if new_asg:
                    assignments.extend(new_asg)
                    placed += len(new_asg)
                    continue

            # Nothing fits.
            if len(assignments) == before:
                break

    return suspensions, assignments
