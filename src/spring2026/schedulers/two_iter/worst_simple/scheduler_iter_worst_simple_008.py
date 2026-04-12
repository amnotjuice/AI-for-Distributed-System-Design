# policy_key: scheduler_iter_worst_simple_008
# context: simple
# model: gpt-5.2-2025-12-11
# llm_cost: 0.052480
# generation_seconds: 38.85
# generated_at: 2026-04-12T00:39:50.707678
@register_scheduler_init(key="scheduler_iter_worst_simple_008")
def scheduler_iter_worst_simple_008_init(s):
    """
    Priority-first, work-conserving scheduler aimed at reducing weighted latency.

    Key fixes / improvements over the previous attempt:
      - Do NOT drop pipelines just because they have FAILED ops; FAILED is retryable (ASSIGNABLE_STATES).
      - Work-conserving packing: schedule multiple ops per pool per tick (not just one) to cut queueing delay.
      - Strict priority ordering: always try QUERY, then INTERACTIVE, then BATCH.
      - Headroom protection: only schedule BATCH if there is no runnable high-priority work.
      - Smarter default sizing:
          * QUERY/INTERACTIVE get more RAM by default to reduce OOM retries.
          * CPU is sliced to allow multiple concurrent high-priority ops (better weighted latency).
      - OOM backoff: on OOM-like failures, increase RAM hint aggressively for that operator.
      - Light anti-starvation: periodically allow a small amount of BATCH progress even if
        interactive workload is heavy (bounded, so latency SLOs remain protected).
    """
    from collections import deque

    # Waiting queues by base priority
    s.q_query = deque()
    s.q_interactive = deque()
    s.q_batch = deque()

    # Bookkeeping to avoid duplicate enqueues and enable aging/round-robin
    s.enqueued_at = {}          # pipeline_id -> tick first seen
    s.last_popped = {}          # pipeline_id -> tick last scheduled attempt (optional)

    # Per-operator learned resource hints (mostly RAM) from failures
    s.op_ram_hint = {}          # (pipeline_id, op_key) -> ram
    s.op_cpu_hint = {}          # (pipeline_id, op_key) -> cpu

    # Scheduler logical time
    s.tick = 0

    # Limits / knobs
    s.max_assignments_per_pool = 8   # pack multiple ops per pool per tick

    # Default slices (avoid pathological tiny allocations)
    s.min_cpu = 1
    s.min_ram = 1

    # Default RAM targets (units match simulator; used as relative sizing)
    s.query_default_ram = 8
    s.interactive_default_ram = 6
    s.batch_default_ram = 4

    # Default CPU slices (favor concurrency for latency)
    s.query_cpu_slice = 2
    s.interactive_cpu_slice = 2
    s.batch_cpu_slice = 2

    # Anti-starvation: every N ticks, allow at most one batch op per pool even if high-priority exists
    s.batch_leak_period = 25


@register_scheduler(key="scheduler_iter_worst_simple_008")
def scheduler_iter_worst_simple_008_scheduler(s, results, pipelines):
    """
    Scheduling step:
      1) Ingest new pipelines into priority queues (QUERY > INTERACTIVE > BATCH).
      2) Learn from failures: OOM-like -> double RAM hint; other failures -> small bump.
      3) For each pool, repeatedly dispatch ready ops up to a per-tick cap:
           - Drain QUERY then INTERACTIVE first.
           - Run BATCH only when no runnable high-priority exists (except periodic "leak").
           - Size CPU/RAM conservatively to reduce OOM retries and improve weighted latency.
    """
    s.tick += 1

    # ---------------- Helpers ----------------
    def _queue_for_priority(prio):
        if prio == Priority.QUERY:
            return s.q_query
        if prio == Priority.INTERACTIVE:
            return s.q_interactive
        return s.q_batch

    def _enqueue_pipeline(p):
        pid = p.pipeline_id
        if pid not in s.enqueued_at:
            s.enqueued_at[pid] = s.tick
            _queue_for_priority(p.priority).append(p)

    def _op_key(op):
        # Best-effort stable identity
        if hasattr(op, "op_id"):
            return ("op_id", getattr(op, "op_id"))
        if hasattr(op, "operator_id"):
            return ("operator_id", getattr(op, "operator_id"))
        if hasattr(op, "name"):
            return ("name", getattr(op, "name"))
        return ("py_id", id(op))

    def _is_done(p):
        st = p.runtime_status()
        return st.is_pipeline_successful()

    def _ready_ops(p):
        # Retryable FAILED ops are included in ASSIGNABLE_STATES
        st = p.runtime_status()
        ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        if not ops:
            return []
        return ops[:1]  # schedule one op at a time per pipeline

    def _pipeline_remaining_ops_est(p):
        # Crude SRPT-ish tie-breaker: prefer pipelines closer to completion within same priority
        st = p.runtime_status()
        completed = st.state_counts.get(OperatorState.COMPLETED, 0)
        total = len(getattr(p, "values", []) or [])
        if total <= 0:
            return 10**9
        return max(0, total - completed)

    def _has_runnable_in_queue(q):
        # Bounded scan: detect if any pipeline in q has a ready op; rotate to preserve fairness
        for _ in range(len(q)):
            p = q.popleft()
            if _is_done(p):
                # drop permanently
                s.enqueued_at.pop(p.pipeline_id, None)
                continue
            ops = _ready_ops(p)
            q.append(p)
            if ops:
                return True
        return False

    def _select_pipeline_from_queue(q):
        """
        Pick one runnable pipeline from queue q.
        Strategy:
          - bounded scan across queue once
          - choose among runnable candidates the one with smallest remaining ops (SRPT-ish)
          - rotate queue to avoid starvation within same priority
        Returns (pipeline, ops) or (None, None)
        """
        best = None
        best_ops = None
        best_score = None

        n = len(q)
        for _ in range(n):
            p = q.popleft()

            if _is_done(p):
                s.enqueued_at.pop(p.pipeline_id, None)
                continue

            ops = _ready_ops(p)
            q.append(p)

            if not ops:
                continue

            score = _pipeline_remaining_ops_est(p)
            # Tie-breaker: older first
            age = s.tick - s.enqueued_at.get(p.pipeline_id, s.tick)
            key = (score, -age)

            if best is None or key < best_score:
                best = p
                best_ops = ops
                best_score = key

        return best, best_ops

    def _size_for(pool_id, pipeline, op, effective_priority):
        pool = s.executor.pools[pool_id]

        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool
        if avail_cpu <= 0 or avail_ram <= 0:
            return 0, 0

        # Default targets (RAM up for high-priority to avoid OOM retries)
        if effective_priority == Priority.QUERY:
            base_cpu = s.query_cpu_slice
            base_ram = s.query_default_ram
            cpu_cap_frac, ram_cap_frac = 0.9, 0.9
        elif effective_priority == Priority.INTERACTIVE:
            base_cpu = s.interactive_cpu_slice
            base_ram = s.interactive_default_ram
            cpu_cap_frac, ram_cap_frac = 0.85, 0.9
        else:
            base_cpu = s.batch_cpu_slice
            base_ram = s.batch_default_ram
            # Keep batch small to preserve headroom
            cpu_cap_frac, ram_cap_frac = 0.4, 0.5

        # Apply learned hints
        k = (pipeline.pipeline_id, _op_key(op))
        hinted_ram = s.op_ram_hint.get(k, base_ram)
        hinted_cpu = s.op_cpu_hint.get(k, base_cpu)

        req_cpu = max(s.min_cpu, int(hinted_cpu))
        req_ram = max(s.min_ram, int(hinted_ram))

        # Hard caps relative to pool max
        cpu_cap = max(s.min_cpu, int(pool.max_cpu_pool * cpu_cap_frac))
        ram_cap = max(s.min_ram, int(pool.max_ram_pool * ram_cap_frac))
        req_cpu = min(req_cpu, cpu_cap)
        req_ram = min(req_ram, ram_cap)

        # Fit to availability (work-conserving)
        req_cpu = min(req_cpu, avail_cpu)
        req_ram = min(req_ram, avail_ram)

        # Must be positive
        if req_cpu < s.min_cpu or req_ram < s.min_ram:
            return 0, 0

        return req_cpu, req_ram

    # ---------------- Ingest pipelines ----------------
    for p in pipelines:
        _enqueue_pipeline(p)

    # Early exit when nothing changed
    if not pipelines and not results:
        return [], []

    # ---------------- Learn from results ----------------
    for r in results:
        if not (hasattr(r, "failed") and r.failed()):
            continue

        pid = getattr(r, "pipeline_id", None)
        if pid is None:
            continue

        err = str(getattr(r, "error", "") or "")
        err_l = err.lower()
        oom_like = ("oom" in err_l) or ("out of memory" in err_l) or ("memory" in err_l)

        ops = getattr(r, "ops", []) or []
        for op in ops:
            k = (pid, _op_key(op))
            prev_ram = int(s.op_ram_hint.get(k, max(s.min_ram, int(getattr(r, "ram", 1) or 1))))
            prev_cpu = int(s.op_cpu_hint.get(k, max(s.min_cpu, int(getattr(r, "cpu", 1) or 1))))

            if oom_like:
                # Aggressive RAM backoff; CPU unchanged
                new_ram = max(prev_ram + 1, prev_ram * 2)
                s.op_ram_hint[k] = new_ram
                s.op_cpu_hint[k] = prev_cpu
            else:
                # Mild bump for other failures (often under-provisioning / transient)
                s.op_ram_hint[k] = max(prev_ram, prev_ram + 2)
                s.op_cpu_hint[k] = max(prev_cpu, prev_cpu + 1)

    # ---------------- Dispatch ----------------
    suspensions = []
    assignments = []

    # Determine whether we allow a small amount of batch "leak" this tick
    allow_batch_leak = (s.tick % s.batch_leak_period) == 0

    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]

        # Pack multiple assignments per pool per tick
        made = 0
        while made < s.max_assignments_per_pool:
            if pool.avail_cpu_pool <= 0 or pool.avail_ram_pool <= 0:
                break

            # Check if any high-priority runnable work exists (global, not just this pool)
            runnable_query = _has_runnable_in_queue(s.q_query)
            runnable_interactive = _has_runnable_in_queue(s.q_interactive)
            high_runnable = runnable_query or runnable_interactive

            # Select priority class to schedule
            if runnable_query:
                chosen_queue = s.q_query
                effective_prio = Priority.QUERY
            elif runnable_interactive:
                chosen_queue = s.q_interactive
                effective_prio = Priority.INTERACTIVE
            else:
                # Only schedule batch when there's no high-priority runnable work,
                # except for periodic bounded "leak" to prevent total starvation.
                if high_runnable and not allow_batch_leak:
                    break
                chosen_queue = s.q_batch
                effective_prio = Priority.BATCH_PIPELINE

            p, ops = _select_pipeline_from_queue(chosen_queue)
            if p is None or not ops:
                # If we couldn't find runnable work in the chosen queue, stop packing.
                # (We might still have other queues; try them once more before stopping.)
                if effective_prio != Priority.QUERY and _has_runnable_in_queue(s.q_query):
                    continue
                if effective_prio != Priority.INTERACTIVE and _has_runnable_in_queue(s.q_interactive):
                    continue
                break

            op = ops[0]
            cpu, ram = _size_for(pool_id, p, op, effective_prio)
            if cpu <= 0 or ram <= 0:
                # Can't fit; stop packing this pool this tick (avoid tight loops)
                break

            assignments.append(
                Assignment(
                    ops=ops,
                    cpu=cpu,
                    ram=ram,
                    priority=p.priority,  # preserve original pipeline priority
                    pool_id=pool_id,
                    pipeline_id=p.pipeline_id,
                )
            )
            made += 1

    return suspensions, assignments
