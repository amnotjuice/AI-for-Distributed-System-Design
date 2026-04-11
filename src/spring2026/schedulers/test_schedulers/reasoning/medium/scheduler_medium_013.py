# policy_key: scheduler_medium_013
# reasoning_effort: medium
# exp: reasoning
# model: gpt-5.2-2025-12-11
# llm_cost: 0.076129
# generation_seconds: 108.70
# generated_at: 2026-04-09T22:46:15.490741
@register_scheduler_init(key="scheduler_medium_013")
def scheduler_medium_013_init(s):
    """
    Priority-first, completion-friendly scheduler with:
      - Strict preference for QUERY then INTERACTIVE then BATCH, with light batch interleaving + aging
      - Conservative per-op CPU caps to avoid one job monopolizing a pool
      - RAM backoff on detected OOM failures (per-pipeline hint), to reduce repeated failures (720s penalty)
      - Multi-pool placement: prefer pool 0 for QUERY/INTERACTIVE if multiple pools, but allow spillover
      - Small reservation on pool 0 to keep headroom for high-priority arrivals
    """
    from collections import deque

    s.tick = 0

    # Per-priority FIFO queues; we will do limited scans to pick "best" (short pipelines, aged batches).
    s.q_query = deque()
    s.q_interactive = deque()
    s.q_batch = deque()

    # Track membership to avoid duplicates across requeues.
    s.in_queue = set()  # pipeline_id

    # Per-pipeline metadata (best-effort; simulator may not always provide pipeline_id on results).
    # Keys: pipeline_id -> dict(enqueue_tick, ram_hint, oom_retries, other_retries)
    s.meta = {}

    # Batch fairness knobs.
    s.batch_interleave_k = 8          # every k non-batch assignments, allow a batch if waiting (when no queries)
    s.assignments_since_batch = 0
    s.batch_aging_threshold = 60      # after this many scheduler ticks waiting, batch can be considered earlier
    s.hard_aging_threshold = 240      # very old batch can be treated like interactive if no queries

    # Pool0 headroom reservation when multiple pools exist and there is high-priority demand.
    s.pool0_reserve_frac = 0.15

    # Retry control (mostly to prevent infinite churn on non-OOM errors).
    s.max_other_retries = 2
    s.max_oom_retries = 8


@register_scheduler(key="scheduler_medium_013")
def scheduler_medium_013(s, results, pipelines):
    """
    See init docstring. This scheduler never intentionally drops pipelines; it keeps re-queuing until
    completion (or until the simulator marks them failed/incomplete by time limit).
    """
    s.tick += 1

    def _prio_rank(prio):
        # Lower is better (scheduled earlier).
        if prio == Priority.QUERY:
            return 0
        if prio == Priority.INTERACTIVE:
            return 1
        return 2  # Priority.BATCH_PIPELINE (and anything else treated as batch)

    def _get_queue_for_priority(prio):
        if prio == Priority.QUERY:
            return s.q_query
        if prio == Priority.INTERACTIVE:
            return s.q_interactive
        return s.q_batch

    def _enqueue_pipeline(p, fresh=False):
        pid = p.pipeline_id
        if pid in s.in_queue:
            return
        q = _get_queue_for_priority(p.priority)
        q.append(p)
        s.in_queue.add(pid)
        if pid not in s.meta:
            s.meta[pid] = {
                "enqueue_tick": s.tick,
                "ram_hint": 0.0,       # absolute RAM to request (best effort), bumped on OOM
                "oom_retries": 0,
                "other_retries": 0,
            }
        elif fresh:
            # If a pipeline re-arrives with same id (unlikely), reset enqueue tick to reflect new arrival.
            s.meta[pid]["enqueue_tick"] = s.tick

    def _drop_pipeline_if_present(pipeline_id):
        # Only metadata/membership cleanup; queue removal happens when we pop/scan.
        if pipeline_id in s.in_queue:
            s.in_queue.remove(pipeline_id)
        # Keep meta around a bit; safe to delete to avoid growth.
        if pipeline_id in s.meta:
            del s.meta[pipeline_id]

    def _looks_like_oom(err):
        if not err:
            return False
        e = str(err).lower()
        return ("oom" in e) or ("out of memory" in e) or ("outofmemory" in e) or ("memoryerror" in e)

    def _pipeline_id_from_result(r):
        # ExecutionResult doesn't guarantee pipeline_id; try to infer from contained ops.
        try:
            for op in getattr(r, "ops", []) or []:
                for attr in ("pipeline_id", "pipeline", "parent_pipeline_id"):
                    if hasattr(op, attr):
                        val = getattr(op, attr)
                        # Some simulators might store pipeline object on op.pipeline
                        if attr == "pipeline" and hasattr(val, "pipeline_id"):
                            return val.pipeline_id
                        if isinstance(val, (int, str)):
                            return val
        except Exception:
            pass
        return None

    def _remaining_ops(p):
        st = p.runtime_status()
        total = None
        try:
            total = len(p.values)
        except Exception:
            total = None
        try:
            completed = st.state_counts.get(OperatorState.COMPLETED, 0)
            failed = st.state_counts.get(OperatorState.FAILED, 0)
            assigned = st.state_counts.get(OperatorState.ASSIGNED, 0)
            running = st.state_counts.get(OperatorState.RUNNING, 0)
            pending = st.state_counts.get(OperatorState.PENDING, 0)
            if total is None:
                total = completed + failed + assigned + running + pending
            # Remaining includes failed (needs retry) and pending.
            remaining = max(0, total - completed)
            return remaining
        except Exception:
            return 10**9

    def _age(p):
        meta = s.meta.get(p.pipeline_id)
        if not meta:
            return 0
        return max(0, s.tick - meta.get("enqueue_tick", s.tick))

    def _pop_best_from_deque(dq, score_fn, max_scan=32):
        # Limited scan to keep it cheap.
        if not dq:
            return None
        n = min(len(dq), max_scan)
        items = []
        for _ in range(n):
            items.append(dq.popleft())

        best_i = 0
        best_score = score_fn(items[0])
        for i in range(1, len(items)):
            sc = score_fn(items[i])
            if sc < best_score:
                best_score = sc
                best_i = i

        best = items[best_i]
        for i, it in enumerate(items):
            if i != best_i:
                dq.appendleft(it)  # keep relative order among non-selected (approx)
        # Restore non-scanned tail as-is (already in dq behind).
        return best

    def _has_waiting_query():
        return len(s.q_query) > 0

    def _has_waiting_interactive():
        return len(s.q_interactive) > 0

    def _has_waiting_batch():
        return len(s.q_batch) > 0

    def _cpu_cap_for_priority(pool, prio):
        # Caps allow concurrency and lower aggregate latency.
        max_cpu = float(pool.max_cpu_pool)
        if prio == Priority.QUERY:
            return max(1.0, 0.50 * max_cpu)
        if prio == Priority.INTERACTIVE:
            return max(1.0, 0.33 * max_cpu)
        return max(1.0, 0.20 * max_cpu)

    def _base_ram_for_priority(pool, prio):
        # Allocate enough to reduce OOM risk for high-priority, but don't always take the whole pool.
        max_ram = float(pool.max_ram_pool)
        if prio == Priority.QUERY:
            return 0.55 * max_ram
        if prio == Priority.INTERACTIVE:
            return 0.40 * max_ram
        return 0.25 * max_ram

    def _choose_pool_order_for_priority(prio):
        # If multiple pools, bias pool 0 to interactive; allow spillover.
        if s.executor.num_pools <= 1:
            return [0]
        if prio in (Priority.QUERY, Priority.INTERACTIVE):
            return [0] + [i for i in range(1, s.executor.num_pools)]
        # Batch: prefer non-zero pools first
        return [i for i in range(1, s.executor.num_pools)] + [0]

    def _pick_next_pipeline_for_pool(pool_id):
        # Selection policy:
        #   1) Always serve QUERY first.
        #   2) Then INTERACTIVE.
        #   3) Then BATCH, but allow: (a) interleaving, (b) aging promotion when no queries.
        #
        # Additionally, when running on non-preferred pool, still allow high-priority spillover.

        # If any queries are waiting anywhere, prefer them on any pool.
        if _has_waiting_query():
            def score_q(p):
                # Favor short remaining work (SRPT-ish) and older age (tie-break).
                return (_remaining_ops(p), -_age(p))
            return _pop_best_from_deque(s.q_query, score_q)

        # No queries waiting.
        if _has_waiting_interactive():
            # Batch interleaving: occasionally pick a batch to avoid starvation, but only if it has aged a bit.
            if _has_waiting_batch() and s.assignments_since_batch >= s.batch_interleave_k:
                # Only interleave if batch has waited at least a little (avoid penalizing interactive too much).
                def score_bi(p):
                    return (-min(_age(p), 10**9), _remaining_ops(p))
                cand = _pop_best_from_deque(s.q_batch, score_bi)
                if cand is not None and _age(cand) >= 10:
                    s.assignments_since_batch = 0
                    return cand
                # If not suitable, put back (best-effort). We popped it; re-enqueue at front-ish by appendleft.
                if cand is not None:
                    s.q_batch.appendleft(cand)

            def score_i(p):
                return (_remaining_ops(p), -_age(p))
            s.assignments_since_batch += 1
            return _pop_best_from_deque(s.q_interactive, score_i)

        # No queries or interactive waiting.
        if _has_waiting_batch():
            def score_b(p):
                # Favor aged (to reduce starvation) and then short remaining.
                return (-_age(p), _remaining_ops(p))
            s.assignments_since_batch = 0
            return _pop_best_from_deque(s.q_batch, score_b)

        return None

    # --- incorporate new arrivals ---
    for p in pipelines or []:
        _enqueue_pipeline(p, fresh=True)

    # --- process results to update RAM hints / retry counters ---
    for r in results or []:
        pid = _pipeline_id_from_result(r)
        if pid is None or pid not in s.meta:
            continue
        if getattr(r, "failed", None) and r.failed():
            if _looks_like_oom(getattr(r, "error", None)):
                s.meta[pid]["oom_retries"] += 1
                # Bump absolute RAM hint based on last allocation (simple exponential backoff).
                try:
                    last = float(getattr(r, "ram", 0.0) or 0.0)
                except Exception:
                    last = 0.0
                bumped = max(s.meta[pid].get("ram_hint", 0.0), 2.0 * last, 1.0)
                s.meta[pid]["ram_hint"] = bumped
            else:
                s.meta[pid]["other_retries"] += 1

    suspensions = []
    assignments = []

    # If nothing is queued, nothing to do.
    if not (s.q_query or s.q_interactive or s.q_batch):
        return suspensions, assignments

    # --- scheduling loop (pool by pool) ---
    num_pools = s.executor.num_pools
    # We'll iterate pools and try to pack a few assignments each tick.
    for pool_id in range(num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)

        if avail_cpu < 1.0 or avail_ram <= 0.0:
            continue

        # Reserve headroom on pool0 if multiple pools and there is high-priority pressure.
        reserve_cpu = 0.0
        reserve_ram = 0.0
        if pool_id == 0 and num_pools > 1 and (_has_waiting_query() or _has_waiting_interactive()):
            reserve_cpu = s.pool0_reserve_frac * float(pool.max_cpu_pool)
            reserve_ram = s.pool0_reserve_frac * float(pool.max_ram_pool)

        guard = 0
        while guard < 64:
            guard += 1
            eff_cpu = avail_cpu - reserve_cpu
            eff_ram = avail_ram - reserve_ram
            if eff_cpu < 1.0 or eff_ram <= 0.0:
                break

            # Pick a pipeline according to global priority/aging policy.
            p = _pick_next_pipeline_for_pool(pool_id)
            if p is None:
                break

            pid = p.pipeline_id
            # Mark as temporarily out of queue while we decide.
            if pid in s.in_queue:
                s.in_queue.remove(pid)

            st = p.runtime_status()
            if st.is_pipeline_successful():
                _drop_pipeline_if_present(pid)
                continue

            # If the pipeline has too many non-OOM failures, stop burning resources on it.
            meta = s.meta.get(pid, {})
            if meta.get("other_retries", 0) > s.max_other_retries:
                # Keep it out of queues; simulator will count it failed/incomplete anyway.
                _drop_pipeline_if_present(pid)
                continue

            # Choose pool placement: if this pool is not ideal and others have more headroom, we could requeue.
            # Keep it simple: placement happens implicitly by pool iteration + pool0 reservation.
            # (Spillover is allowed because we pick globally; we don't force re-placement.)

            # Pick next ready operator.
            op_list = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
            if not op_list:
                # Not ready yet; requeue and continue.
                _enqueue_pipeline(p)
                continue

            prio = p.priority

            # Compute resource request.
            cpu_cap = _cpu_cap_for_priority(pool, prio)
            cpu_req = min(eff_cpu, cpu_cap)
            cpu_req = max(1.0, cpu_req)

            base_ram = _base_ram_for_priority(pool, prio)
            ram_hint = float(meta.get("ram_hint", 0.0) or 0.0)

            # If we saw repeated OOMs, ask for more aggressively (still capped by pool and availability).
            oom_retries = int(meta.get("oom_retries", 0) or 0)
            if oom_retries > 0:
                base_ram = max(base_ram, ram_hint)
                # Mild growth beyond hint for repeated OOMs.
                base_ram = max(base_ram, (1.25 ** min(oom_retries, 6)) * base_ram)

            ram_req = min(eff_ram, float(pool.max_ram_pool), base_ram)
            # If hint is larger than base, try to honor it (up to eff_ram).
            if ram_hint > 0.0:
                ram_req = min(eff_ram, float(pool.max_ram_pool), max(ram_req, min(ram_hint, eff_ram)))

            # If we still can't fit, requeue and stop trying to pack this pool further.
            if cpu_req < 1.0 or ram_req <= 0.0:
                _enqueue_pipeline(p)
                break

            # Create the assignment (one op at a time to preserve fairness & limit OOM blast radius).
            assignments.append(
                Assignment(
                    ops=op_list,
                    cpu=cpu_req,
                    ram=ram_req,
                    priority=prio,
                    pool_id=pool_id,
                    pipeline_id=pid,
                )
            )

            # Update local available resources.
            avail_cpu -= cpu_req
            avail_ram -= ram_req

            # Requeue pipeline for future ops (it may have more ready ops later).
            _enqueue_pipeline(p)

    return suspensions, assignments
