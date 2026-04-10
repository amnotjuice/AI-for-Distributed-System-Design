# policy_key: scheduler_medium_028
# reasoning_effort: medium
# exp: reasoning
# model: gpt-5.2-2025-12-11
# llm_cost: 0.086041
# generation_seconds: 98.15
# generated_at: 2026-04-09T23:06:57.134910
@register_scheduler_init(key="scheduler_medium_028")
def scheduler_medium_028_init(s):
    """
    Priority- and failure-aware scheduler focused on minimizing weighted latency while
    strongly avoiding incomplete/failed pipelines (720s penalty).

    Core ideas:
      - Strict protection for QUERY, strong preference for INTERACTIVE, but prevent BATCH starvation via aging + burst limiting.
      - Conservative initial RAM sizing with adaptive multiplicative backoff on OOM-like failures (per-operator).
      - Mild CPU right-sizing by priority to reduce tail latency for high-priority work without monopolizing pools.
      - Round-robin within priority queues; avoid scheduling the same pipeline multiple times in the same tick.
    """
    # Logical time for aging decisions
    s.tick = 0

    # Pipeline tracking
    s.pipelines_by_id = {}          # pipeline_id -> Pipeline
    s.pipeline_arrival_tick = {}    # pipeline_id -> tick when first seen

    # Queues by priority (store pipeline_id)
    try:
        from collections import deque
    except Exception:
        deque = list  # fallback (should not happen)
    s.q_query = deque()
    s.q_interactive = deque()
    s.q_batch = deque()

    # Map operator object -> pipeline_id for decoding ExecutionResult (which lacks pipeline_id)
    s.op_to_pipeline = {}  # id(op) -> pipeline_id

    # Per-operator resource adaptation
    s.op_ram_guess = {}    # (pipeline_id, op_key) -> ram guess
    s.op_fail_count = {}   # (pipeline_id, op_key) -> total failures
    s.op_oom_count = {}    # (pipeline_id, op_key) -> oom-like failures

    # If a pipeline repeatedly fails non-OOM, we can abandon it to protect overall completion/latency
    s.abandoned_pipelines = set()

    # Burst control to ensure some batch progress under sustained high-priority load
    s.hp_streak = 0  # counts consecutive QUERY/INTERACTIVE assignments


@register_scheduler(key="scheduler_medium_028")
def scheduler_medium_028(s, results, pipelines):
    """
    Scheduling step.

    Returns:
      (suspensions, assignments)

    Notes:
      - We do not aggressively preempt because the simulator API does not expose a stable
        way to enumerate currently-running containers here; instead we rely on:
          * priority-aware admission,
          * RAM backoff on OOM failures,
          * anti-starvation for batch.
    """
    # -----------------------------
    # Helper functions (local)
    # -----------------------------
    def _priority_class(p):
        # Normalize priority into 3 classes expected by this policy.
        if p == Priority.QUERY:
            return "query"
        if p == Priority.INTERACTIVE:
            return "interactive"
        return "batch"

    def _get_queue_for_priority(pr):
        pc = _priority_class(pr)
        if pc == "query":
            return s.q_query
        if pc == "interactive":
            return s.q_interactive
        return s.q_batch

    def _iter_ops_in_pipeline(p):
        # Pipeline.values is described as DAG of operators (iterable).
        try:
            for op in p.values:
                yield op
        except Exception:
            return

    def _op_key(op):
        # Prefer stable operator identifier if present; fall back to object id.
        for attr in ("operator_id", "op_id", "id", "name"):
            if hasattr(op, attr):
                try:
                    v = getattr(op, attr)
                    # Ensure hashable and not a huge object
                    if isinstance(v, (int, str, tuple)):
                        return v
                except Exception:
                    pass
        return id(op)

    def _is_oom_error(err):
        if not err:
            return False
        try:
            msg = str(err).lower()
        except Exception:
            return False
        return ("oom" in msg) or ("out of memory" in msg) or ("memoryerror" in msg)

    def _op_min_ram(op, pool):
        # Try common attribute names; else default to a conservative fraction of pool RAM.
        for attr in ("min_ram", "ram_min", "ram_req", "ram", "memory", "mem"):
            if hasattr(op, attr):
                try:
                    v = float(getattr(op, attr))
                    if v > 0:
                        return v
                except Exception:
                    pass
        # Default: 15% of pool RAM (conservative to reduce OOM rate for unknown curves).
        try:
            return float(pool.max_ram_pool) * 0.15
        except Exception:
            return 1.0

    def _initial_ram_guess(op, pool, prio):
        base = _op_min_ram(op, pool)
        # Slight cushion; bigger for higher priority to reduce OOM-induced retries/latency.
        if prio == Priority.QUERY:
            return base * 1.6
        if prio == Priority.INTERACTIVE:
            return base * 1.45
        return base * 1.3

    def _choose_ram(pool, pipeline_id, op, prio, avail_ram):
        k = (pipeline_id, _op_key(op))
        guess = s.op_ram_guess.get(k)
        if guess is None:
            guess = _initial_ram_guess(op, pool, prio)

        # Backoff factor based on observed OOMs for this operator.
        oom_ct = s.op_oom_count.get(k, 0)
        if oom_ct > 0:
            # Each OOM increases guess multiplicatively.
            guess = guess * (1.6 ** min(oom_ct, 6))

        # Cap to keep headroom for other tasks and reduce fragmentation.
        try:
            hard_cap = float(pool.max_ram_pool) * 0.9
        except Exception:
            hard_cap = guess
        ram = min(guess, hard_cap, float(avail_ram))

        # Ensure we never go below minimum requirement estimate.
        min_ram = _op_min_ram(op, pool)
        if ram < min_ram:
            # If we can't satisfy minimum, signal "doesn't fit".
            if min_ram > avail_ram:
                return None
            ram = min(min_ram, float(avail_ram))

        # Persist chosen guess (helps stabilize once it works).
        s.op_ram_guess[k] = max(s.op_ram_guess.get(k, 0.0), ram)
        return ram

    def _choose_cpu(pool, prio, avail_cpu):
        try:
            max_cpu = float(pool.max_cpu_pool)
        except Exception:
            max_cpu = float(avail_cpu) if avail_cpu else 0.0

        if avail_cpu <= 0:
            return 0.0

        # Right-size by priority; leave room for concurrency, but give high-priority enough CPU.
        if prio == Priority.QUERY:
            target = max(1.0, max_cpu * 0.55)
        elif prio == Priority.INTERACTIVE:
            target = max(1.0, max_cpu * 0.40)
        else:
            target = max(1.0, max_cpu * 0.30)

        cpu = min(float(avail_cpu), target)

        # Avoid tiny fractional CPU that might stall; if only fractional remains, take it.
        if cpu < 1.0 and avail_cpu >= 1.0:
            cpu = 1.0
        return cpu

    def _effective_priority_score(pipeline_id, p):
        # Higher is better. Queries always dominate; batch can age to beat interactive occasionally.
        age = s.tick - s.pipeline_arrival_tick.get(pipeline_id, s.tick)
        if p.priority == Priority.QUERY:
            base = 300
            age_cap = 40
        elif p.priority == Priority.INTERACTIVE:
            base = 200
            age_cap = 90
        else:
            base = 100
            # Allow batch to eventually compete with interactive, but never beat queries.
            age_cap = 180
        return base + min(age, age_cap)

    def _pipeline_done_or_abandoned(p):
        if p.pipeline_id in s.abandoned_pipelines:
            return True
        st = p.runtime_status()
        return st.is_pipeline_successful()

    def _dequeue_dead_front(q):
        # Rotate away completed/abandoned pipelines at the front.
        # Keep bounded work to avoid O(n^2) in large queues.
        tries = min(len(q), 16) if hasattr(q, "__len__") else 0
        for _ in range(tries):
            if not q:
                break
            pid = q[0]
            p = s.pipelines_by_id.get(pid)
            if p is None or _pipeline_done_or_abandoned(p):
                q.popleft()
            else:
                break

    def _pop_best_fitting_pipeline(avail_cpu, avail_ram, pool):
        """
        Choose a pipeline to schedule next on a given pool, considering:
          - strict preference for query
          - interactive next
          - batch with aging and a burst limiter (serve batch after long hp streak)
          - resource fit (RAM)
        Returns pipeline_id or None.
        """
        # Clean dead items at queue fronts.
        _dequeue_dead_front(s.q_query)
        _dequeue_dead_front(s.q_interactive)
        _dequeue_dead_front(s.q_batch)

        has_query = bool(s.q_query)
        has_interactive = bool(s.q_interactive)
        has_batch = bool(s.q_batch)

        if not (has_query or has_interactive or has_batch):
            return None

        # Burst limiter: after enough high-priority assignments, force a batch if any is pending.
        force_batch = (s.hp_streak >= 6) and has_batch and (not has_query)

        # Candidate selection: scan a few from each queue to incorporate aging + fit.
        candidates = []
        scan_k = 8

        def add_candidates_from_queue(q):
            # Peek/scan without permanently reordering.
            n = min(len(q), scan_k)
            for i in range(n):
                pid = q[i]
                p = s.pipelines_by_id.get(pid)
                if p is None or _pipeline_done_or_abandoned(p):
                    continue
                st = p.runtime_status()
                ops = st.get_ops([OperatorState.PENDING, OperatorState.FAILED], require_parents_complete=True)
                if not ops:
                    continue
                op0 = ops[0]
                # Quick fit check on RAM.
                ram_need = _choose_ram(pool, pid, op0, p.priority, avail_ram)
                if ram_need is None or ram_need <= 0:
                    continue
                candidates.append((pid, p, op0, ram_need))

        if force_batch:
            add_candidates_from_queue(s.q_batch)
        else:
            # Always consider query first; interactive second; batch last (but batch can age up).
            if has_query:
                add_candidates_from_queue(s.q_query)
            if has_interactive:
                add_candidates_from_queue(s.q_interactive)
            if has_batch:
                add_candidates_from_queue(s.q_batch)

        if not candidates:
            return None

        # Choose by effective priority score (aging) and then best-fit RAM packing.
        # For query protection: query always wins ties by base score.
        best = None
        best_key = None
        for (pid, p, op0, ram_need) in candidates:
            score = _effective_priority_score(pid, p)
            # Best-fit: prefer larger RAM need (packs pool, reduces fragmentation) but only after score.
            key = (score, ram_need)
            if best is None or key > best_key:
                best = pid
                best_key = key
        return best

    def _rotate_queue_after_scheduling(pid, prio):
        q = _get_queue_for_priority(prio)
        # Move pid from front to back if it's at/near the front; keep stable ordering otherwise.
        try:
            if q and q[0] == pid:
                q.popleft()
                q.append(pid)
            else:
                # If not at front, remove one occurrence and append (bounded cost).
                # This helps round-robin fairness when we selected a non-front item due to aging/fit.
                try:
                    q.remove(pid)
                    q.append(pid)
                except Exception:
                    pass
        except Exception:
            pass

    # -----------------------------
    # Advance logical time
    # -----------------------------
    s.tick += 1

    suspensions = []
    assignments = []

    # -----------------------------
    # Ingest new pipelines
    # -----------------------------
    for p in pipelines:
        pid = p.pipeline_id
        if pid in s.pipelines_by_id:
            continue
        s.pipelines_by_id[pid] = p
        s.pipeline_arrival_tick[pid] = s.tick
        _get_queue_for_priority(p.priority).append(pid)

        # Build operator -> pipeline mapping to interpret ExecutionResult
        for op in _iter_ops_in_pipeline(p):
            s.op_to_pipeline[id(op)] = pid

    # -----------------------------
    # Process execution results to adapt RAM guesses and track repeated failures
    # -----------------------------
    for r in results:
        if not getattr(r, "failed", lambda: False)():
            continue

        oom_like = _is_oom_error(getattr(r, "error", None))
        for op in getattr(r, "ops", []) or []:
            pid = s.op_to_pipeline.get(id(op))
            if pid is None:
                continue
            k = (pid, _op_key(op))

            s.op_fail_count[k] = s.op_fail_count.get(k, 0) + 1
            if oom_like:
                s.op_oom_count[k] = s.op_oom_count.get(k, 0) + 1
                # Increase RAM guess aggressively (OOMs are catastrophic for latency/score).
                prev = s.op_ram_guess.get(k)
                if prev is None:
                    # If we never set it, start from assigned RAM and bump.
                    try:
                        prev = float(getattr(r, "ram", 0.0)) or 0.0
                    except Exception:
                        prev = 0.0
                s.op_ram_guess[k] = max(s.op_ram_guess.get(k, 0.0), prev * 1.6 if prev else 0.0)
            else:
                # Non-OOM failure: allow a small number of retries but then abandon pipeline to
                # prevent thrashing that can cause more 720s elsewhere.
                if s.op_fail_count[k] >= 3:
                    s.abandoned_pipelines.add(pid)

    # -----------------------------
    # Garbage collect completed/abandoned pipelines from tracking maps (bounded work)
    # -----------------------------
    # Do not iterate all pipelines every tick if huge; check fronts + a small sample.
    for q in (s.q_query, s.q_interactive, s.q_batch):
        _dequeue_dead_front(q)

    # -----------------------------
    # Schedule across pools
    # -----------------------------
    scheduled_this_tick = set()

    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool

        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # Keep assigning while pool has resources; bound attempts to avoid infinite loops.
        attempts = 0
        max_attempts = 24

        while attempts < max_attempts and avail_cpu > 0 and avail_ram > 0:
            attempts += 1

            pid = _pop_best_fitting_pipeline(avail_cpu, avail_ram, pool)
            if pid is None:
                break

            if pid in scheduled_this_tick:
                # Avoid scheduling same pipeline multiple times per tick to reduce interference.
                # Rotate and try again.
                p = s.pipelines_by_id.get(pid)
                if p is not None:
                    _rotate_queue_after_scheduling(pid, p.priority)
                continue

            p = s.pipelines_by_id.get(pid)
            if p is None or _pipeline_done_or_abandoned(p):
                # Remove stale entries as we encounter them.
                try:
                    _get_queue_for_priority(p.priority if p else Priority.BATCH_PIPELINE).remove(pid)
                except Exception:
                    pass
                continue

            st = p.runtime_status()
            op_list = st.get_ops([OperatorState.PENDING, OperatorState.FAILED], require_parents_complete=True)[:1]
            if not op_list:
                # Not runnable now; rotate for fairness and try others.
                _rotate_queue_after_scheduling(pid, p.priority)
                continue

            op0 = op_list[0]
            ram = _choose_ram(pool, pid, op0, p.priority, avail_ram)
            if ram is None or ram <= 0:
                # Can't fit in this pool right now; try next candidate.
                _rotate_queue_after_scheduling(pid, p.priority)
                continue

            cpu = _choose_cpu(pool, p.priority, avail_cpu)
            if cpu <= 0:
                break

            assignments.append(
                Assignment(
                    ops=op_list,
                    cpu=cpu,
                    ram=ram,
                    priority=p.priority,
                    pool_id=pool_id,
                    pipeline_id=pid,
                )
            )

            scheduled_this_tick.add(pid)
            avail_cpu -= cpu
            avail_ram -= ram

            # Update burst counter to ensure batch progress under sustained high-priority.
            if p.priority in (Priority.QUERY, Priority.INTERACTIVE):
                s.hp_streak += 1
            else:
                s.hp_streak = 0

            # Round-robin the pipeline within its priority class.
            _rotate_queue_after_scheduling(pid, p.priority)

    return suspensions, assignments
