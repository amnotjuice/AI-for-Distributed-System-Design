# policy_key: scheduler_iter_best_rich_013
# context: rich
# model: gpt-5.2-2025-12-11
# llm_cost: 0.055833
# generation_seconds: 46.66
# generated_at: 2026-04-12T00:29:04.027501
@register_scheduler_init(key="scheduler_iter_best_rich_013")
def scheduler_iter_best_rich_013_init(s):
    """Priority-aware scheduler with learned RAM sizing (OOM backoff + success upper-bounds) and
    modest CPU caps to improve weighted latency via higher safe parallelism.

    Incremental improvements vs previous priority FIFO:
      - Learn per-(pipeline,op) RAM bounds:
          * On OOM: raise lower bound based on attempted RAM.
          * On success: lower the upper bound to the successful RAM.
        Then pick RAM using a bounded search between lb/ub to avoid chronic OOMs while reducing
        chronic over-allocation (which was high in prior run).
      - Keep strict priority order (QUERY > INTERACTIVE > BATCH) but with light aging to prevent
        indefinite starvation under sustained higher-priority load.
      - CPU sizing: small, priority-based caps, with slight scale-up when system is not backlogged.

    Intentional omissions (until we have better runtime/container visibility in the simulator API):
      - No preemption/suspension (cannot robustly choose victims without a running set).
    """
    # Per-priority FIFO queues (simple lists to match template access patterns)
    s.q_query = []
    s.q_interactive = []
    s.q_batch = []

    # Logical tick counter for basic aging (incremented each scheduler call)
    s.tick = 0

    # Pipeline metadata for aging
    s.pipeline_enq_tick = {}  # pipeline_id -> tick when last enqueued

    # Per-operator RAM bounds and last observations (keyed by (pipeline_id, op_id))
    s.op_ram_lb = {}      # minimum RAM believed necessary (after OOMs)
    s.op_ram_ub = {}      # maximum RAM believed sufficient (after successes)
    s.op_ram_good = {}    # last successful RAM allocation

    # Some pipelines may hard-fail; we stop re-enqueuing them if we can identify them.
    s.dead_pipelines = set()

    def _op_key(op, pipeline_id):
        # Prefer stable IDs if present; else use object identity (stable within a run).
        oid = getattr(op, "op_id", None)
        if oid is None:
            oid = getattr(op, "operator_id", None)
        if oid is None:
            oid = getattr(op, "name", None)
        if oid is None:
            oid = id(op)
        return (pipeline_id, oid)

    s._op_key = _op_key


@register_scheduler(key="scheduler_iter_best_rich_013")
def scheduler_iter_best_rich_013_scheduler(s, results, pipelines):
    """Scheduler step: ingest arrivals, update RAM model from results, then place ops by priority with aging."""
    s.tick += 1

    # -----------------------------
    # Helpers
    # -----------------------------
    def _is_oom_error(err) -> bool:
        if err is None:
            return False
        msg = str(err).lower()
        return ("oom" in msg) or ("out of memory" in msg)

    def _is_timeout_error(err) -> bool:
        if err is None:
            return False
        msg = str(err).lower()
        return "timeout" in msg

    def _enqueue_pipeline(p):
        if p.pipeline_id in s.dead_pipelines:
            return
        # Record/update enqueue tick for aging.
        s.pipeline_enq_tick[p.pipeline_id] = s.tick
        if p.priority == Priority.QUERY:
            s.q_query.append(p)
        elif p.priority == Priority.INTERACTIVE:
            s.q_interactive.append(p)
        else:
            s.q_batch.append(p)

    def _queue_for_priority(priority):
        if priority == Priority.QUERY:
            return s.q_query
        if priority == Priority.INTERACTIVE:
            return s.q_interactive
        return s.q_batch

    def _effective_score(p):
        # Higher score scheduled first.
        # Strict base priority plus aging to keep batch making progress (reduces long-tail timeouts).
        if p.priority == Priority.QUERY:
            base = 3.0
        elif p.priority == Priority.INTERACTIVE:
            base = 2.0
        else:
            base = 1.0

        enq = s.pipeline_enq_tick.get(p.pipeline_id, s.tick)
        age = max(0, s.tick - enq)

        # Aging rate: faster for lower priority to avoid starvation, slow for high priority.
        if p.priority == Priority.QUERY:
            aging = 0.0005 * age
        elif p.priority == Priority.INTERACTIVE:
            aging = 0.0020 * age
        else:
            aging = 0.0040 * age

        return base + aging

    def _pick_next_pipeline_and_op():
        # Choose among the heads of each queue based on effective score.
        # We do bounded rotations for each queue to find runnable ops.
        candidates = []

        for q in (s.q_query, s.q_interactive, s.q_batch):
            if not q:
                continue

            n = len(q)
            chosen = None
            chosen_ops = None

            # Rotate through queue to find the first runnable pipeline
            for _ in range(n):
                p = q.pop(0)

                if p.pipeline_id in s.dead_pipelines:
                    continue

                st = p.runtime_status()
                if st.is_pipeline_successful():
                    continue

                op_list = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
                if op_list:
                    chosen = p
                    chosen_ops = op_list
                    break

                # Not runnable yet, rotate to back.
                q.append(p)

            if chosen is not None:
                candidates.append((chosen, chosen_ops))

        if not candidates:
            return None, None

        # Pick candidate with max score; if tie, prefer higher priority implicitly via score base.
        best_p, best_ops = None, None
        best_score = None
        for p, op_list in candidates:
            sc = _effective_score(p)
            if best_score is None or sc > best_score:
                best_score = sc
                best_p, best_ops = p, op_list

        return best_p, best_ops

    def _clamp(x, lo, hi):
        if x < lo:
            return lo
        if x > hi:
            return hi
        return x

    def _initial_ram_guess(priority, pool):
        # Start smaller than before to reduce mean allocated %, then learn upwards only if needed.
        # Fractions tuned to be conservative for QUERY, moderate for INTERACTIVE, higher for BATCH.
        if priority == Priority.QUERY:
            frac = 0.04
        elif priority == Priority.INTERACTIVE:
            frac = 0.06
        else:
            frac = 0.10
        return pool.max_ram_pool * frac

    def _choose_ram_for_op(p, op, pool, avail_ram):
        opk = s._op_key(op, p.pipeline_id)

        lb = s.op_ram_lb.get(opk, 0.0)
        ub = s.op_ram_ub.get(opk, None)

        # Base guess depends on priority (smaller to increase parallelism, rely on learning to avoid OOM).
        guess = _initial_ram_guess(p.priority, pool)
        if lb > 0:
            guess = max(guess, lb)

        # If we have an upper bound, aim between lb and ub (biased towards lower to reduce over-allocation).
        if ub is not None:
            # If bounds are inconsistent (lb > ub), trust lb (we saw OOM at smaller sizes).
            if lb > ub:
                target = lb
            else:
                # Geometric-ish midpoint without math import: sqrt(lb*ub) approximated via exponent by 0.5 not allowed
                # Use weighted linear interpolation towards lb.
                target = lb + 0.35 * (ub - lb)
            guess = max(guess, target)

        # If we have a known-good successful RAM, we can try slightly below it to reduce allocation,
        # but not below lb.
        good = s.op_ram_good.get(opk, None)
        if good is not None:
            guess = min(guess, max(lb, good * 0.90))

        # Clamp to available and pool max; also ensure non-trivial allocation.
        ram = _clamp(guess, 0.0, min(avail_ram, pool.max_ram_pool))
        if ram < 0.01:
            ram = 0.0
        return ram

    def _choose_cpu(priority, pool, avail_cpu, backlog_factor):
        # Keep CPU small to increase concurrency (reduces queueing latency), but allow modest scale-up
        # when not backlogged to reduce per-op runtime (helps timeouts).
        max_cpu = pool.max_cpu_pool

        if priority == Priority.QUERY:
            base = min(2.0, max_cpu * 0.25)
        elif priority == Priority.INTERACTIVE:
            base = min(4.0, max_cpu * 0.35)
        else:
            base = min(8.0, max_cpu * 0.60)

        # backlog_factor in [0,1]: higher backlog -> smaller cpu to pack more ops.
        scale = 1.0 - 0.35 * backlog_factor
        cpu = base * scale
        cpu = _clamp(cpu, 0.5, base)  # don't go too tiny, avoid pathological slowdowns/timeouts
        cpu = min(cpu, avail_cpu)
        if cpu < 0.01:
            cpu = 0.0
        return cpu

    # -----------------------------
    # Ingest arrivals
    # -----------------------------
    for p in pipelines:
        _enqueue_pipeline(p)

    # Early exit when nothing changes; keep deterministic and cheap.
    if not pipelines and not results:
        return [], []

    # -----------------------------
    # Update RAM model based on results
    # -----------------------------
    for r in results:
        # Update bounds only if we can associate ops. If r.ops missing, skip.
        ops = r.ops or []
        if not ops:
            continue

        # Pipeline id is not guaranteed on ExecutionResult per the given interface.
        # If absent, we can only key by (None, op_id); still helps within repeated retries if consistent.
        pid = getattr(r, "pipeline_id", None)

        if r.failed():
            if _is_oom_error(r.error):
                # OOM means attempted RAM is below required: raise lower bound.
                # Use a multiplier to jump quickly and reduce repeated OOMs (and wasted attempts).
                for op in ops:
                    opk = s._op_key(op, pid)
                    cur_lb = s.op_ram_lb.get(opk, 0.0)

                    attempted = float(getattr(r, "ram", 0.0) or 0.0)
                    # Jump factor: 1.6 tends to reduce repeated OOM cycles without huge overshoot.
                    new_lb = attempted * 1.6 if attempted > 0 else cur_lb
                    if new_lb < cur_lb:
                        new_lb = cur_lb

                    s.op_ram_lb[opk] = new_lb

                    # If we had an upper bound lower than new lb, drop it (inconsistent).
                    ub = s.op_ram_ub.get(opk, None)
                    if ub is not None and ub < new_lb:
                        s.op_ram_ub.pop(opk, None)

            else:
                # For non-OOM failures we avoid aggressive retries by optionally marking pipeline dead.
                # However, keep going on timeouts: better scheduling may still help later.
                if not _is_timeout_error(r.error):
                    if pid is not None:
                        s.dead_pipelines.add(pid)

        else:
            # Success: attempted RAM is sufficient => tighten upper bound and record good size.
            for op in ops:
                opk = s._op_key(op, pid)
                attempted = float(getattr(r, "ram", 0.0) or 0.0)
                if attempted <= 0:
                    continue

                s.op_ram_good[opk] = attempted
                cur_ub = s.op_ram_ub.get(opk, None)
                if cur_ub is None or attempted < cur_ub:
                    s.op_ram_ub[opk] = attempted

                # If upper bound falls below lower bound due to noise, reconcile by setting lb = ub.
                lb = s.op_ram_lb.get(opk, 0.0)
                if lb > 0 and attempted < lb:
                    s.op_ram_lb[opk] = attempted

    suspensions = []  # No preemption in this iteration.
    assignments = []

    # -----------------------------
    # Scheduling / placement
    # -----------------------------
    # Backlog estimate to adapt CPU sizing slightly:
    # normalize by a soft constant so it's roughly in [0,1] for typical queue sizes.
    backlog = len(s.q_query) * 3 + len(s.q_interactive) * 2 + len(s.q_batch) * 1
    backlog_factor = backlog / float(backlog + 200.0)  # smooth saturation

    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool

        # Avoid spinning on tiny headroom.
        while avail_cpu > 0.05 and avail_ram > 0.05:
            p, op_list = _pick_next_pipeline_and_op()
            if p is None:
                break

            op = op_list[0]

            cpu = _choose_cpu(p.priority, pool, avail_cpu, backlog_factor)
            ram = _choose_ram_for_op(p, op, pool, avail_ram)

            # If we can't allocate meaningful resources, put the pipeline back and stop scheduling this pool.
            if cpu <= 0.0 or ram <= 0.0:
                _enqueue_pipeline(p)
                break

            assignments.append(
                Assignment(
                    ops=op_list,
                    cpu=cpu,
                    ram=ram,
                    priority=p.priority,
                    pool_id=pool_id,
                    pipeline_id=p.pipeline_id,
                )
            )

            avail_cpu -= cpu
            avail_ram -= ram

            # Round-robin fairness within queues: re-enqueue after scheduling one op.
            _enqueue_pipeline(p)

    return suspensions, assignments
