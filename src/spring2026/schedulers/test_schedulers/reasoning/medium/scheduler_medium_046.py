# policy_key: scheduler_medium_046
# reasoning_effort: medium
# exp: reasoning
# model: gpt-5.2-2025-12-11
# llm_cost: 0.082737
# generation_seconds: 107.00
# generated_at: 2026-04-09T23:32:42.271426
@register_scheduler_init(key="scheduler_medium_046")
def scheduler_medium_046_init(s):
    """
    Priority-aware, reservation-based scheduler with conservative RAM sizing and OOM backoff.

    Goals:
      - Protect QUERY / INTERACTIVE tail latency via (a) strict scheduling order and (b) capacity reservations against BATCH.
      - Avoid pipeline failures (720s penalty) by allocating relatively generous RAM and using OOM-triggered RAM backoff retries.
      - Avoid starvation with round-robin within each priority class + simple aging for long-waiting batch pipelines.
      - Optional best-effort preemption: if pool exposes running containers, suspend low-priority work only when needed.
    """
    # Scheduler "time" in ticks (monotonic counter; simulator tick duration is opaque but deterministic)
    s.tick = 0

    # Per-priority round-robin queues of pipeline_ids
    s.q_query = []
    s.q_interactive = []
    s.q_batch = []

    # pipeline_id -> Pipeline (latest object reference)
    s.pipelines_by_id = {}

    # pipeline_id -> tick when first seen
    s.arrival_tick = {}

    # pipeline_id -> total failure count observed (from results)
    s.pipeline_failures = {}

    # (pipeline_id, op_key) -> guessed RAM to request next time
    s.op_ram_guess = {}

    # pipeline_id -> bool; if True, pipeline gets a one-time "front of queue" bump after OOM
    s.boost_next = {}

    # Config knobs (kept simple and robust)
    s.reserve_frac_cpu = 0.25   # reserve this fraction of pool capacity against BATCH
    s.reserve_frac_ram = 0.25
    s.batch_aging_ticks = 200   # after this, batch gets scheduled ahead of interactive (but still behind query)
    s.max_non_oom_retries = 1   # avoid infinite retries for deterministic non-OOM failures

    # Resource sizing fractions (relative to pool max)
    s.cpu_frac = {
        Priority.QUERY: 0.70,
        Priority.INTERACTIVE: 0.55,
        Priority.BATCH_PIPELINE: 0.35,
    }
    s.ram_frac = {
        Priority.QUERY: 0.60,
        Priority.INTERACTIVE: 0.50,
        Priority.BATCH_PIPELINE: 0.35,
    }


@register_scheduler(key="scheduler_medium_046")
def scheduler_medium_046_scheduler(s, results, pipelines):
    def _prio_rank(p):
        if p == Priority.QUERY:
            return 3
        if p == Priority.INTERACTIVE:
            return 2
        return 1  # batch

    def _queue_for_priority(priority):
        if priority == Priority.QUERY:
            return s.q_query
        if priority == Priority.INTERACTIVE:
            return s.q_interactive
        return s.q_batch

    def _remove_from_all_queues(pipeline_id):
        # Small lists: linear removal is OK and robust.
        for q in (s.q_query, s.q_interactive, s.q_batch):
            i = 0
            while i < len(q):
                if q[i] == pipeline_id:
                    q.pop(i)
                else:
                    i += 1

    def _op_key(pipeline_id, op):
        # Prefer stable identifiers if available; fallback to id(op) (stable within a run).
        op_id = getattr(op, "op_id", None)
        if op_id is not None:
            return (pipeline_id, ("op_id", op_id))
        name = getattr(op, "name", None)
        if name is not None:
            return (pipeline_id, ("name", name))
        return (pipeline_id, ("py_id", id(op)))

    def _is_oom_error(err):
        if err is None:
            return False
        try:
            msg = str(err).lower()
        except Exception:
            return False
        return ("oom" in msg) or ("out of memory" in msg) or ("memory" in msg and "out" in msg)

    def _get_ready_op(pipeline):
        status = pipeline.runtime_status()
        if status.is_pipeline_successful():
            return None
        # Don't schedule pipelines that are clearly beyond a reasonable retry budget.
        # Note: FAILED ops are assignable; we still allow OOM backoff retries.
        op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        if not op_list:
            return None
        return op_list[0]

    def _pool_reserve(pool):
        reserve_cpu = int(s.reserve_frac_cpu * pool.max_cpu_pool)
        reserve_ram = int(s.reserve_frac_ram * pool.max_ram_pool)
        return reserve_cpu, reserve_ram

    def _default_ram_guess(pool, priority):
        # Conservative baseline guesses: higher for interactive/query to reduce OOM risk.
        frac = s.ram_frac.get(priority, 0.35)
        g = int(max(1, frac * pool.max_ram_pool))
        return min(g, int(pool.max_ram_pool))

    def _compute_request(pool, priority, pipeline_id, op, avail_cpu, avail_ram, is_batch_limited):
        # CPU request
        target_cpu = int(max(1, s.cpu_frac.get(priority, 0.35) * pool.max_cpu_pool))
        cpu_req = min(avail_cpu, max(1, target_cpu))

        # RAM request: from guess (learned) or conservative default; never exceed available.
        k = _op_key(pipeline_id, op)
        guess = s.op_ram_guess.get(k, None)
        if guess is None:
            guess = _default_ram_guess(pool, priority)

        # If we're limiting batch by reservations, keep request under the limited availability.
        ram_req = min(avail_ram, max(1, int(guess)))

        if is_batch_limited:
            # For batch, prefer smaller allocations to increase concurrency and reduce head-of-line blocking.
            # Still keep a reasonable floor to avoid trivial OOM loops.
            ram_req = min(ram_req, int(max(1, 0.50 * avail_ram)))

        return cpu_req, ram_req

    def _iter_running_containers(pool):
        # Best-effort introspection: different simulator versions may expose different names.
        for attr in ("running_containers", "containers", "active_containers"):
            conts = getattr(pool, attr, None)
            if conts is None:
                continue
            try:
                return list(conts)
            except Exception:
                continue
        return []

    def _container_priority(c):
        return getattr(c, "priority", None)

    def _container_resources(c):
        cpu = getattr(c, "cpu", None)
        ram = getattr(c, "ram", None)
        if cpu is None or ram is None:
            return None, None
        try:
            return int(cpu), int(ram)
        except Exception:
            return None, None

    def _container_id(c):
        return getattr(c, "container_id", None)

    def _maybe_preempt_for_query(local_avail_cpu, local_avail_ram, need_cpu, need_ram):
        # Suspend lowest-priority containers until we can fit a query assignment in SOME pool.
        # We only do this for QUERY, because preemption churn can hurt overall throughput.
        susp = []
        for pool_id in range(s.executor.num_pools):
            pool = s.executor.pools[pool_id]
            avail_cpu = local_avail_cpu[pool_id]
            avail_ram = local_avail_ram[pool_id]
            if avail_cpu >= need_cpu and avail_ram >= need_ram:
                return susp  # already feasible somewhere

        # Try preempting batch first (prefer non-latency pool=0 to remain for interactive/query if used).
        pool_order = list(range(s.executor.num_pools))
        if len(pool_order) > 1:
            pool_order = pool_order[1:] + pool_order[:1]

        for pool_id in pool_order:
            pool = s.executor.pools[pool_id]
            conts = _iter_running_containers(pool)
            if not conts:
                continue

            # Consider only containers with known resources and lower priority than query.
            candidates = []
            for c in conts:
                cid = _container_id(c)
                pr = _container_priority(c)
                ccpu, cram = _container_resources(c)
                if cid is None or pr is None or ccpu is None or cram is None:
                    continue
                if _prio_rank(pr) < _prio_rank(Priority.QUERY):
                    candidates.append((pr, cid, ccpu, cram))

            # Sort by priority ascending (batch first), then by larger resource holders first to reduce number of suspends.
            def _sort_key(x):
                pr, _, ccpu, cram = x
                return (_prio_rank(pr), -(ccpu + cram))

            candidates.sort(key=_sort_key)

            for pr, cid, ccpu, cram in candidates:
                # Suspend one-by-one until any pool becomes feasible.
                susp.append(Suspend(container_id=cid, pool_id=pool_id))
                local_avail_cpu[pool_id] += ccpu
                local_avail_ram[pool_id] += cram

                for pid2 in range(s.executor.num_pools):
                    if local_avail_cpu[pid2] >= need_cpu and local_avail_ram[pid2] >= need_ram:
                        return susp
        return susp

    # --------------------------
    # Update scheduler time/state
    # --------------------------
    s.tick += 1

    # Incorporate new pipelines (arrivals)
    for p in pipelines:
        s.pipelines_by_id[p.pipeline_id] = p
        if p.pipeline_id not in s.arrival_tick:
            s.arrival_tick[p.pipeline_id] = s.tick
            s.pipeline_failures[p.pipeline_id] = 0
            s.boost_next[p.pipeline_id] = False
            _queue_for_priority(p.priority).append(p.pipeline_id)
        else:
            # If pipeline object is re-sent, keep the latest reference.
            pass

    # Process execution results: update OOM guesses + failure counters
    for r in results:
        if r is None:
            continue
        pid = getattr(r, "pipeline_id", None)
        # Not all simulators include pipeline_id in ExecutionResult; fallback by searching ops membership is expensive.
        # If missing, still update OOM guesses keyed only by op id(op) from r.ops when possible.
        if r.failed():
            # Failure accounting (best-effort)
            if pid is not None:
                s.pipeline_failures[pid] = s.pipeline_failures.get(pid, 0) + 1

            # OOM backoff: double RAM guess for the failed ops
            if _is_oom_error(getattr(r, "error", None)):
                # If we know the op(s), increase their RAM guess.
                for op in getattr(r, "ops", []) or []:
                    if pid is None:
                        # We cannot key by pipeline reliably; fall back to op-only key.
                        k = ("unknown_pid", ("py_id", id(op)))
                    else:
                        k = _op_key(pid, op)

                    prev = s.op_ram_guess.get(k, None)
                    if prev is None:
                        prev = int(max(1, getattr(r, "ram", 1)))
                    new_guess = int(max(prev + 1, prev * 2))

                    # Cap at pool max RAM if we can infer it; otherwise just store.
                    pool_id = getattr(r, "pool_id", None)
                    if pool_id is not None and 0 <= pool_id < s.executor.num_pools:
                        new_guess = min(new_guess, int(s.executor.pools[pool_id].max_ram_pool))
                    s.op_ram_guess[k] = new_guess

                # Give pipeline a one-time queue boost to recover quickly and reduce end-to-end latency
                if pid is not None:
                    s.boost_next[pid] = True

    # Early exit if nothing new happened (but still need to react to running completions sometimes);
    # we only return early if there are no new pipelines/results.
    if not pipelines and not results:
        return [], []

    # Cleanup: remove completed pipelines from queues/state to keep scheduling tight
    completed_ids = []
    for pid, p in list(s.pipelines_by_id.items()):
        try:
            if p.runtime_status().is_pipeline_successful():
                completed_ids.append(pid)
        except Exception:
            # If status can't be read, keep it.
            continue
    for pid in completed_ids:
        _remove_from_all_queues(pid)
        s.pipelines_by_id.pop(pid, None)
        s.boost_next.pop(pid, None)

    # --------------------------
    # Scheduling
    # --------------------------
    suspensions = []
    assignments = []

    # Local view of remaining capacity this tick, updated as we create assignments/suspensions.
    local_avail_cpu = [int(s.executor.pools[i].avail_cpu_pool) for i in range(s.executor.num_pools)]
    local_avail_ram = [int(s.executor.pools[i].avail_ram_pool) for i in range(s.executor.num_pools)]

    # Determine whether we should treat pool 0 as a "latency pool" if multiple pools exist.
    has_dedicated_latency_pool = s.executor.num_pools > 1

    def _priority_order_with_aging():
        # Aging: batch that waited long becomes more urgent than interactive (but still behind query).
        # This reduces starvation/incompletion risk (720s penalty) without harming query too much.
        oldest_batch_wait = 0
        if s.q_batch:
            for pid in s.q_batch:
                at = s.arrival_tick.get(pid, s.tick)
                oldest_batch_wait = max(oldest_batch_wait, s.tick - at)
        if oldest_batch_wait >= s.batch_aging_ticks:
            return [Priority.QUERY, Priority.BATCH_PIPELINE, Priority.INTERACTIVE]
        return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]

    def _choose_pool_for(pipeline, op, cpu_req, ram_req):
        # For batch, enforce reservation headroom; for others, use all available.
        best = None
        best_score = None
        for pool_id in range(s.executor.num_pools):
            pool = s.executor.pools[pool_id]
            avail_cpu = local_avail_cpu[pool_id]
            avail_ram = local_avail_ram[pool_id]

            is_batch = (pipeline.priority == Priority.BATCH_PIPELINE)
            reserve_cpu, reserve_ram = _pool_reserve(pool)

            if is_batch:
                # If dedicated latency pool exists, keep batch off pool 0 unless necessary.
                if has_dedicated_latency_pool and pool_id == 0:
                    # Still allow if other pools are completely blocked for batch
                    pass

                avail_cpu_eff = max(0, avail_cpu - reserve_cpu)
                avail_ram_eff = max(0, avail_ram - reserve_ram)
            else:
                avail_cpu_eff = avail_cpu
                avail_ram_eff = avail_ram

            if avail_cpu_eff < cpu_req or avail_ram_eff < ram_req:
                continue

            # Prefer pools with more headroom after placement (reduce future blocking).
            cpu_headroom = avail_cpu_eff - cpu_req
            ram_headroom = avail_ram_eff - ram_req

            # For query/interactive, prefer pool 0 (latency pool) if it fits.
            latency_bonus = 0
            if has_dedicated_latency_pool and pool_id == 0 and pipeline.priority in (Priority.QUERY, Priority.INTERACTIVE):
                latency_bonus = 1_000_000  # dominates tie-breaks safely

            score = latency_bonus + (cpu_headroom * 1000) + ram_headroom
            if best is None or score > best_score:
                best = pool_id
                best_score = score

        # If batch and we have a dedicated latency pool, avoid pool 0 if other pool fits.
        if pipeline.priority == Priority.BATCH_PIPELINE and has_dedicated_latency_pool and best == 0:
            # Try again excluding pool 0
            best2 = None
            best2_score = None
            for pool_id in range(1, s.executor.num_pools):
                pool = s.executor.pools[pool_id]
                avail_cpu = local_avail_cpu[pool_id]
                avail_ram = local_avail_ram[pool_id]
                reserve_cpu, reserve_ram = _pool_reserve(pool)
                avail_cpu_eff = max(0, avail_cpu - reserve_cpu)
                avail_ram_eff = max(0, avail_ram - reserve_ram)
                if avail_cpu_eff < cpu_req or avail_ram_eff < ram_req:
                    continue
                cpu_headroom = avail_cpu_eff - cpu_req
                ram_headroom = avail_ram_eff - ram_req
                score = (cpu_headroom * 1000) + ram_headroom
                if best2 is None or score > best2_score:
                    best2 = pool_id
                    best2_score = score
            if best2 is not None:
                return best2

        return best

    # One-time boosts (after OOM) to the front of their queue to reduce end-to-end latency.
    # Do it before scheduling to ensure immediate retry.
    for q in (s.q_query, s.q_interactive, s.q_batch):
        i = 0
        while i < len(q):
            pid = q[i]
            if s.boost_next.get(pid, False):
                q.pop(i)
                q.insert(0, pid)
                s.boost_next[pid] = False
                i += 1
            else:
                i += 1

    # If there are pending query ops and no pool can accommodate a reasonable request, try preempting batch.
    # We estimate a "reasonable request" using pool 0 max capacity (or any pool if only one exists).
    if s.q_query:
        # Find the first query pipeline that has a ready op.
        qp = None
        qop = None
        for qpid in s.q_query:
            p = s.pipelines_by_id.get(qpid, None)
            if p is None:
                continue
            op = _get_ready_op(p)
            if op is not None:
                qp, qop = p, op
                break

        if qp is not None and qop is not None:
            # Compute an estimated need based on the largest pool max (more permissive).
            max_pool = s.executor.pools[0]
            for i in range(s.executor.num_pools):
                if (s.executor.pools[i].max_cpu_pool + s.executor.pools[i].max_ram_pool) > (
                    max_pool.max_cpu_pool + max_pool.max_ram_pool
                ):
                    max_pool = s.executor.pools[i]

            # Use conservative-ish query request (likely to avoid OOM)
            est_cpu = int(max(1, s.cpu_frac[Priority.QUERY] * max_pool.max_cpu_pool))
            # Use known guess if present; else default for the chosen max pool.
            k = _op_key(qp.pipeline_id, qop)
            est_ram = s.op_ram_guess.get(k, _default_ram_guess(max_pool, Priority.QUERY))

            # Cap by some reasonable portion to reduce aggressive preempting
            est_cpu = min(est_cpu, int(max_pool.max_cpu_pool))
            est_ram = min(int(est_ram), int(max_pool.max_ram_pool))

            suspensions.extend(_maybe_preempt_for_query(local_avail_cpu, local_avail_ram, est_cpu, int(est_ram)))

    # Main placement loop: round-robin per priority, fill as long as we can place anything.
    total_waiting = len(s.q_query) + len(s.q_interactive) + len(s.q_batch)
    max_attempts = max(10, total_waiting * 4)

    attempts = 0
    made_progress = True
    while made_progress and attempts < max_attempts:
        made_progress = False
        attempts += 1

        for pr in _priority_order_with_aging():
            q = _queue_for_priority(pr)
            if not q:
                continue

            # Round-robin: rotate by taking from front and putting to back if not placed
            # but only iterate up to queue length per outer attempt.
            qlen = len(q)
            for _ in range(qlen):
                pid = q.pop(0)
                p = s.pipelines_by_id.get(pid, None)
                if p is None:
                    continue

                # Skip completed pipelines
                try:
                    if p.runtime_status().is_pipeline_successful():
                        s.pipelines_by_id.pop(pid, None)
                        s.boost_next.pop(pid, None)
                        continue
                except Exception:
                    pass

                # Enforce retry budget for non-OOM failures: if too many failures and no progress, deprioritize by moving back.
                fails = s.pipeline_failures.get(pid, 0)
                if fails > (s.max_non_oom_retries + 3) and p.priority != Priority.BATCH_PIPELINE:
                    # For high-priority, still allow; for batch, keep going (completion matters).
                    pass

                op = _get_ready_op(p)
                if op is None:
                    # Not runnable now; keep it in queue.
                    q.append(pid)
                    continue

                # Decide which pool and how much to request, respecting reservations for batch.
                # We'll compute requests against each pool's effective availability during selection.
                chosen_pool = None
                chosen_cpu = None
                chosen_ram = None

                for pool_id in range(s.executor.num_pools):
                    pool = s.executor.pools[pool_id]
                    avail_cpu = local_avail_cpu[pool_id]
                    avail_ram = local_avail_ram[pool_id]

                    is_batch = (p.priority == Priority.BATCH_PIPELINE)
                    reserve_cpu, reserve_ram = _pool_reserve(pool)
                    if is_batch:
                        avail_cpu_eff = max(0, avail_cpu - reserve_cpu)
                        avail_ram_eff = max(0, avail_ram - reserve_ram)
                    else:
                        avail_cpu_eff = avail_cpu
                        avail_ram_eff = avail_ram

                    if avail_cpu_eff <= 0 or avail_ram_eff <= 0:
                        continue

                    cpu_req, ram_req = _compute_request(
                        pool=pool,
                        priority=p.priority,
                        pipeline_id=p.pipeline_id,
                        op=op,
                        avail_cpu=avail_cpu_eff,
                        avail_ram=avail_ram_eff,
                        is_batch_limited=is_batch,
                    )

                    # Ensure we never request more than actual physical availability.
                    cpu_req = min(cpu_req, avail_cpu_eff)
                    ram_req = min(ram_req, avail_ram_eff)

                    if cpu_req <= 0 or ram_req <= 0:
                        continue

                    # Record "best pool" by calling chooser with these reqs.
                    # (We re-check below with _choose_pool_for for consistent scoring.)
                    pass

                # Choose pool based on scoring; compute request for that pool.
                # We recompute req for each candidate and select via _choose_pool_for.
                best_pool = None
                best_cpu = None
                best_ram = None
                for pool_id in range(s.executor.num_pools):
                    pool = s.executor.pools[pool_id]
                    avail_cpu = local_avail_cpu[pool_id]
                    avail_ram = local_avail_ram[pool_id]

                    is_batch = (p.priority == Priority.BATCH_PIPELINE)
                    reserve_cpu, reserve_ram = _pool_reserve(pool)
                    if is_batch:
                        avail_cpu_eff = max(0, avail_cpu - reserve_cpu)
                        avail_ram_eff = max(0, avail_ram - reserve_ram)
                    else:
                        avail_cpu_eff = avail_cpu
                        avail_ram_eff = avail_ram

                    if avail_cpu_eff <= 0 or avail_ram_eff <= 0:
                        continue

                    cpu_req, ram_req = _compute_request(
                        pool=pool,
                        priority=p.priority,
                        pipeline_id=p.pipeline_id,
                        op=op,
                        avail_cpu=avail_cpu_eff,
                        avail_ram=avail_ram_eff,
                        is_batch_limited=is_batch,
                    )

                    if cpu_req <= 0 or ram_req <= 0:
                        continue

                    # Check feasibility on that pool specifically.
                    if avail_cpu_eff < cpu_req or avail_ram_eff < ram_req:
                        continue

                    # Use chooser scoring to break ties.
                    chosen = _choose_pool_for(p, op, cpu_req, ram_req)
                    if chosen == pool_id:
                        best_pool = pool_id
                        best_cpu = cpu_req
                        best_ram = ram_req
                        break

                chosen_pool = best_pool
                chosen_cpu = best_cpu
                chosen_ram = best_ram

                if chosen_pool is None:
                    # Can't place now; keep pipeline in queue and move on.
                    q.append(pid)
                    continue

                # Additional guardrails:
                # - If batch and we have a dedicated latency pool, avoid placing on pool 0 unless it's the only feasible option.
                if (
                    p.priority == Priority.BATCH_PIPELINE
                    and has_dedicated_latency_pool
                    and chosen_pool == 0
                ):
                    # Check if any non-zero pool could fit with current local availability.
                    found_elsewhere = False
                    for pool_id in range(1, s.executor.num_pools):
                        pool = s.executor.pools[pool_id]
                        reserve_cpu, reserve_ram = _pool_reserve(pool)
                        if (local_avail_cpu[pool_id] - reserve_cpu) >= chosen_cpu and (local_avail_ram[pool_id] - reserve_ram) >= chosen_ram:
                            found_elsewhere = True
                            break
                    if found_elsewhere:
                        q.append(pid)
                        continue

                # Create the assignment
                assignment = Assignment(
                    ops=[op],
                    cpu=int(max(1, chosen_cpu)),
                    ram=int(max(1, chosen_ram)),
                    priority=p.priority,
                    pool_id=int(chosen_pool),
                    pipeline_id=p.pipeline_id,
                )
                assignments.append(assignment)

                # Update local availability. For batch, we already ensured it fits reservation-adjusted avail;
                # consume from raw availability to reflect actual capacity usage.
                local_avail_cpu[chosen_pool] -= int(max(1, chosen_cpu))
                local_avail_ram[chosen_pool] -= int(max(1, chosen_ram))

                # Round-robin: put pipeline at back for fairness (it may have more ready ops later).
                q.append(pid)

                made_progress = True
                break  # move to next priority level after one placement to keep latency-sensitive fairness
            if made_progress:
                break  # restart priority loop to always reconsider query first

    return suspensions, assignments
