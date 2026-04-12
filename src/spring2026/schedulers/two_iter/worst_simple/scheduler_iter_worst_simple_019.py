# policy_key: scheduler_iter_worst_simple_019
# context: simple
# model: gpt-5.2-2025-12-11
# llm_cost: 0.059088
# generation_seconds: 49.34
# generated_at: 2026-04-12T00:47:41.012874
@register_scheduler_init(key="scheduler_iter_worst_simple_019")
def scheduler_iter_worst_simple_019_init(s):
    """
    Iteration 2: Priority-first, multi-dispatch, with batch headroom reservation + bounded retries.

    Changes vs naive FIFO (and the previous attempt):
      - Strict priority ordering: QUERY > INTERACTIVE > BATCH for runnable ops.
      - Dispatch multiple assignments per pool per tick (not just one), improving queueing delay.
      - Reserve CPU/RAM headroom for latency-sensitive work so BATCH can't consume all capacity.
      - Prefer pool 0 for QUERY/INTERACTIVE when multiple pools exist; keep BATCH off pool 0 if any
        high-priority work exists (or likely to arrive).
      - OOM-aware RAM backoff per operator with bounded retries; do NOT drop whole pipelines just
        because one op failed once (FAILED is retryable via ASSIGNABLE_STATES).
      - Very light aging: long-waiting BATCH becomes INTERACTIVE for dispatch decisions.
    """
    from collections import deque

    s.q_query = deque()
    s.q_interactive = deque()
    s.q_batch = deque()

    s.enqueued_at = {}  # pipeline_id -> tick first seen
    s.tick = 0

    # Learned resource hints keyed by (pipeline_id, op_key)
    s.op_ram_hint = {}
    s.op_cpu_hint = {}

    # Retry bookkeeping
    s.op_fail_count = {}  # (pipeline_id, op_key) -> int
    s.max_retries_per_op = 3

    # Aging / anti-starvation (kept mild to protect weighted latency)
    s.batch_promotion_ticks = 400

    # Headroom reservations (fractions of pool max). Batch can only use beyond these when any
    # high-priority work is waiting/runnable.
    s.reserve_cpu_frac = 0.25
    s.reserve_ram_frac = 0.25

    # Target per-op CPU share (fractions of pool max) to reduce wall-clock without monopolizing.
    s.cpu_share_query = 0.50
    s.cpu_share_interactive = 0.35
    s.cpu_share_batch = 0.20

    # RAM provisioning defaults (RAM doesn't speed up; allocate small unless hints say otherwise)
    s.default_ram_query = 4
    s.default_ram_interactive = 4
    s.default_ram_batch = 4

    # Minimum slices
    s.min_cpu = 1
    s.min_ram = 1


@register_scheduler(key="scheduler_iter_worst_simple_019")
def scheduler_iter_worst_simple_019_scheduler(s, results, pipelines):
    from collections import deque

    s.tick += 1
    suspensions = []
    assignments = []

    # ---------------- Helpers ----------------
    def _queue_for_prio(prio):
        if prio == Priority.QUERY:
            return s.q_query
        if prio == Priority.INTERACTIVE:
            return s.q_interactive
        return s.q_batch

    def _effective_prio(p):
        # Promote old batch to interactive to avoid indefinite starvation.
        if p.priority != Priority.BATCH_PIPELINE:
            return p.priority
        waited = s.tick - s.enqueued_at.get(p.pipeline_id, s.tick)
        if waited >= s.batch_promotion_ticks:
            return Priority.INTERACTIVE
        return p.priority

    def _prio_rank(prio):
        if prio == Priority.QUERY:
            return 3
        if prio == Priority.INTERACTIVE:
            return 2
        return 1

    def _op_key(op):
        # Best-effort stable identity; prefer explicit IDs if present.
        if hasattr(op, "op_id"):
            return ("op_id", getattr(op, "op_id"))
        if hasattr(op, "operator_id"):
            return ("operator_id", getattr(op, "operator_id"))
        if hasattr(op, "name"):
            return ("name", getattr(op, "name"))
        return ("py_id", id(op))

    def _enqueue_pipeline(p):
        pid = p.pipeline_id
        if pid not in s.enqueued_at:
            s.enqueued_at[pid] = s.tick
        _queue_for_prio(p.priority).append(p)

    def _cleanup_if_done(p):
        st = p.runtime_status()
        if st.is_pipeline_successful():
            # Leave lazy duplicates in queues; they will be skipped later.
            s.enqueued_at.pop(p.pipeline_id, None)
            return True
        return False

    def _get_ready_op(p):
        st = p.runtime_status()
        ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        if not ops:
            return None
        return ops[0]

    def _has_any_high_priority_waiting():
        # Conservative: consider "waiting" if any pipeline in q_query/q_interactive has a runnable op.
        # We bound scans to avoid O(n^2) on big queues.
        def _scan_has_ready(q, budget=32):
            n = min(len(q), budget)
            for i in range(n):
                p = q[i]
                if _cleanup_if_done(p):
                    continue
                if _get_ready_op(p) is not None:
                    return True
            return False

        return _scan_has_ready(s.q_query) or _scan_has_ready(s.q_interactive)

    def _pool_preference_ok(pool_id, eff_prio, any_hi_waiting):
        # If multiple pools exist, prefer pool 0 for query/interactive.
        # Keep batch off pool 0 while any high-priority work exists.
        if s.executor.num_pools <= 1:
            return True
        if pool_id == 0:
            if eff_prio == Priority.BATCH_PIPELINE and any_hi_waiting:
                return False
            return True
        # Non-zero pools are fine for everything, but are preferred for batch
        return True

    def _size_for(pool, eff_prio, pid, op):
        # CPU: give a fractional share to reduce runtime but still allow concurrency.
        if eff_prio == Priority.QUERY:
            share = s.cpu_share_query
            base_ram = s.default_ram_query
        elif eff_prio == Priority.INTERACTIVE:
            share = s.cpu_share_interactive
            base_ram = s.default_ram_interactive
        else:
            share = s.cpu_share_batch
            base_ram = s.default_ram_batch

        # Derived targets, bounded by availability
        target_cpu = max(s.min_cpu, int(pool.max_cpu_pool * share))
        # Ensure at least 1 and not more than what's available
        cpu = max(s.min_cpu, min(target_cpu, pool.avail_cpu_pool))

        # RAM: use hint if exists, else small default; bounded by availability
        key = (pid, _op_key(op))
        hinted_ram = s.op_ram_hint.get(key, base_ram)
        ram = max(s.min_ram, min(int(hinted_ram), pool.avail_ram_pool))

        # If we have a CPU hint (rarely used), respect it but don't exceed availability.
        hinted_cpu = s.op_cpu_hint.get(key, cpu)
        cpu = max(s.min_cpu, min(int(hinted_cpu), cpu, pool.avail_cpu_pool))

        return cpu, ram

    def _pop_next_runnable_from_queue(q, allow_failed_ops=True, scan_budget=64):
        """
        Pop and return a runnable pipeline+op from queue, else None.
        Maintains queue order by rotating scanned items to the back.
        """
        n = min(len(q), scan_budget)
        for _ in range(n):
            p = q.popleft()

            # Drop completed pipelines
            if _cleanup_if_done(p):
                continue

            op = _get_ready_op(p)
            if op is None:
                q.append(p)
                continue

            # Enforce bounded retries: if op has failed too many times, effectively give up on it.
            # (We don't have a "mark pipeline failed" API; we just stop scheduling this op.)
            key = (p.pipeline_id, _op_key(op))
            if s.op_fail_count.get(key, 0) > s.max_retries_per_op:
                q.append(p)
                continue

            return p, op

        return None, None

    # ---------------- Ingest new pipelines ----------------
    for p in pipelines:
        _enqueue_pipeline(p)

    # Early exit if nothing to do
    if not pipelines and not results:
        return [], []

    # ---------------- Learn from results ----------------
    for r in results:
        if not (hasattr(r, "failed") and r.failed()):
            continue

        pid = getattr(r, "pipeline_id", None)
        ops = getattr(r, "ops", []) or []
        err = str(getattr(r, "error", "") or "").lower()

        # Heuristic OOM detection: prioritize RAM backoff.
        oom_like = ("oom" in err) or ("out of memory" in err) or ("memory" in err)

        if pid is None:
            # Can't learn per-op without a pipeline id.
            continue

        for op in ops:
            k = (pid, _op_key(op))
            s.op_fail_count[k] = s.op_fail_count.get(k, 0) + 1

            prev_ram = int(s.op_ram_hint.get(k, int(getattr(r, "ram", 1) or 1)))
            prev_cpu = int(s.op_cpu_hint.get(k, int(getattr(r, "cpu", 1) or 1)))

            if oom_like:
                # Exponential RAM backoff; keep CPU unchanged.
                s.op_ram_hint[k] = max(prev_ram + 1, prev_ram * 2)
                s.op_cpu_hint[k] = max(s.min_cpu, prev_cpu)
            else:
                # Mild bump for unknown failures (may reflect under-provisioning).
                s.op_ram_hint[k] = max(prev_ram + 1, int(prev_ram * 1.25) + 1)
                s.op_cpu_hint[k] = max(prev_cpu + 1, int(prev_cpu * 1.20) + 1)

    # ---------------- Dispatch ----------------
    any_hi_waiting = _has_any_high_priority_waiting()

    # Pool iteration order: if multiple pools, try to schedule high-priority on pool 0 early.
    pool_order = list(range(s.executor.num_pools))
    if s.executor.num_pools > 1:
        pool_order = [0] + [i for i in range(1, s.executor.num_pools)]

    for pool_id in pool_order:
        pool = s.executor.pools[pool_id]
        if pool.avail_cpu_pool <= 0 or pool.avail_ram_pool <= 0:
            continue

        # Compute reserved headroom for high-priority work (only affects BATCH).
        reserve_cpu = int(pool.max_cpu_pool * s.reserve_cpu_frac)
        reserve_ram = int(pool.max_ram_pool * s.reserve_ram_frac)

        # Greedily pack the pool with multiple assignments this tick, but always prioritize.
        # We do not split an operator; we just keep picking runnable ops.
        while pool.avail_cpu_pool > 0 and pool.avail_ram_pool > 0:
            picked = None

            # Strict priority order: QUERY -> INTERACTIVE -> BATCH
            for q in (s.q_query, s.q_interactive, s.q_batch):
                p, op = _pop_next_runnable_from_queue(q)
                if p is None:
                    continue

                eff_prio = _effective_prio(p)
                if not _pool_preference_ok(pool_id, eff_prio, any_hi_waiting):
                    # Put it back and try other queues.
                    q.appendleft(p)
                    continue

                # If this is batch and high-priority exists, enforce reservation:
                if eff_prio == Priority.BATCH_PIPELINE and any_hi_waiting:
                    if pool.avail_cpu_pool <= reserve_cpu or pool.avail_ram_pool <= reserve_ram:
                        # Not enough headroom beyond reserve; don't place more batch here now.
                        q.appendleft(p)
                        continue

                picked = (p, op, eff_prio, q)
                break

            if picked is None:
                break

            p, op, eff_prio, src_q = picked

            cpu, ram = _size_for(pool, eff_prio, p.pipeline_id, op)

            # For BATCH with headroom reservation, additionally cap by "beyond reserve" space.
            if eff_prio == Priority.BATCH_PIPELINE and any_hi_waiting:
                cpu = min(cpu, max(s.min_cpu, pool.avail_cpu_pool - reserve_cpu))
                ram = min(ram, max(s.min_ram, pool.avail_ram_pool - reserve_ram))

            # If we can't allocate even minimal slices now, requeue and stop trying this pool.
            if cpu < s.min_cpu or ram < s.min_ram:
                src_q.append(p)
                break

            assignments.append(
                Assignment(
                    ops=[op],
                    cpu=cpu,
                    ram=ram,
                    priority=p.priority,  # preserve original pipeline priority
                    pool_id=pool_id,
                    pipeline_id=p.pipeline_id,
                )
            )

            # Requeue pipeline for subsequent operators.
            src_q.append(p)

            # NOTE: We assume the simulator will account for resource consumption after assignments;
            # within this tick loop, pool.avail_* may not reflect deductions. To stay safe, we stop
            # after one assignment if avail doesn't change. Detect that and break to avoid infinite loop.
            # (Some Eudoxia versions update avail immediately; others apply after scheduler returns.)
            # We'll use a simple guard by manually decrementing a local view if needed.
            if not hasattr(s, "_local_pool_avail"):
                s._local_pool_avail = {}
            # Initialize local view per tick/pool
            key = (s.tick, pool_id)
            if key not in s._local_pool_avail:
                s._local_pool_avail[key] = [pool.avail_cpu_pool, pool.avail_ram_pool]

            prev_cpu_av, prev_ram_av = s._local_pool_avail[key]
            # If pool.avail_* did not change, decrement our local view and break when depleted.
            if pool.avail_cpu_pool == prev_cpu_av and pool.avail_ram_pool == prev_ram_av:
                prev_cpu_av -= cpu
                prev_ram_av -= ram
                s._local_pool_avail[key] = [prev_cpu_av, prev_ram_av]
                if prev_cpu_av <= 0 or prev_ram_av <= 0:
                    break
            else:
                s._local_pool_avail[key] = [pool.avail_cpu_pool, pool.avail_ram_pool]

    return suspensions, assignments
