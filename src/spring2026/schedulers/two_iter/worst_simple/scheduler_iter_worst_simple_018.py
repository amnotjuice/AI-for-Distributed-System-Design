# policy_key: scheduler_iter_worst_simple_018
# context: simple
# model: gpt-5.2-2025-12-11
# llm_cost: 0.061846
# generation_seconds: 42.51
# generated_at: 2026-04-12T00:46:51.670677
@register_scheduler_init(key="scheduler_iter_worst_simple_018")
def scheduler_iter_worst_simple_018_init(s):
    """
    Iteration 2: Strong priority separation + multi-assign per tick + aggressive sizing for latency.

    Changes vs prior attempt (aim: reduce weighted latency):
      - Strict priority ordering: never schedule BATCH if any QUERY/INTERACTIVE runnable exists anywhere.
      - Pool preference: pool 0 is "latency pool" (QUERY first, then INTERACTIVE). BATCH uses other pools first.
      - Fill pools: schedule multiple assignments per pool per tick while resources remain (not just 1).
      - Faster QUERY: give QUERY nearly all remaining CPU in chosen pool to cut tail/median latency.
      - OOM backoff: per-(pipeline,op) RAM hints learned from failures; retry FAILED ops rather than dropping pipelines.
      - Light anti-starvation: if BATCH waits long and no high-priority runnable exists, it can run on pool 0.
    """
    from collections import deque

    s.q_query = deque()
    s.q_interactive = deque()
    s.q_batch = deque()

    s.enqueued_at = {}  # pipeline_id -> tick first seen (best-effort)
    s.tick = 0

    # Learned per operator hints (best-effort identity)
    s.op_ram_hint = {}  # (pipeline_id, op_key) -> ram
    s.op_cpu_hint = {}  # (pipeline_id, op_key) -> cpu

    # Retry tracking to avoid infinite failure loops
    s.op_fail_count = {}  # (pipeline_id, op_key) -> count
    s.max_retries_per_op = 6

    # Aging knobs
    s.batch_allow_pool0_after_ticks = 400

    # Minimum slices to avoid zero allocations
    s.min_cpu = 1
    s.min_ram = 1


@register_scheduler(key="scheduler_iter_worst_simple_018")
def scheduler_iter_worst_simple_018_scheduler(s, results, pipelines):
    """
    Priority-first scheduler with pool affinity and multi-assignment packing.

    Core rule:
      - If any QUERY/INTERACTIVE runnable op exists, do not schedule BATCH anywhere this tick.
    """
    from collections import deque

    s.tick += 1

    # ---------------- Helpers ----------------
    def _op_key(op):
        # Best-effort stable identity within a run.
        if hasattr(op, "op_id"):
            return ("op_id", getattr(op, "op_id"))
        if hasattr(op, "operator_id"):
            return ("operator_id", getattr(op, "operator_id"))
        if hasattr(op, "name"):
            return ("name", getattr(op, "name"))
        return ("py_id", id(op))

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

    def _is_done(p):
        st = p.runtime_status()
        return st.is_pipeline_successful()

    def _ready_ops(p):
        st = p.runtime_status()
        # Only schedule ops whose parents are complete
        ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        return ops or []

    def _has_any_runnable_in_queue(q):
        # Bounded scan: rotate through current elements once.
        for _ in range(len(q)):
            p = q.popleft()
            if _is_done(p):
                # Drop completed pipelines from bookkeeping
                s.enqueued_at.pop(p.pipeline_id, None)
                continue
            ops = _ready_ops(p)
            q.append(p)
            if ops:
                return True
        return False

    def _oom_like(err_str):
        e = (err_str or "").lower()
        return ("oom" in e) or ("out of memory" in e) or ("memory" in e)

    def _cap(x, lo, hi):
        return max(lo, min(x, hi))

    def _pool_order_for_priority(prio):
        # pool 0 is latency pool if multiple pools exist
        n = s.executor.num_pools
        if n <= 1:
            return [0] if n == 1 else []
        if prio in (Priority.QUERY, Priority.INTERACTIVE):
            return [0] + list(range(1, n))
        # batch: avoid pool 0
        return list(range(1, n)) + [0]

    def _size_for(p, op, pool_id, allow_batch):
        pool = s.executor.pools[pool_id]
        avail_cpu = int(pool.avail_cpu_pool)
        avail_ram = int(pool.avail_ram_pool)
        if avail_cpu <= 0 or avail_ram <= 0:
            return 0, 0

        key = (p.pipeline_id, _op_key(op))
        hint_ram = int(s.op_ram_hint.get(key, 0) or 0)
        hint_cpu = int(s.op_cpu_hint.get(key, 0) or 0)

        # Default baselines (kept small; RAM hint grows on OOM)
        base_ram = 2
        base_cpu = 1

        if p.priority == Priority.QUERY:
            # Latency: use almost all available CPU, bounded by pool max.
            req_cpu = max(base_cpu, hint_cpu, avail_cpu)
            req_cpu = _cap(req_cpu, s.min_cpu, int(pool.max_cpu_pool))
            req_cpu = min(req_cpu, avail_cpu)

            # RAM: prefer hint, otherwise modest; never exceed available.
            req_ram = max(base_ram, hint_ram, s.min_ram)
            req_ram = _cap(req_ram, s.min_ram, int(pool.max_ram_pool))
            req_ram = min(req_ram, avail_ram)
            return req_cpu, req_ram

        if p.priority == Priority.INTERACTIVE:
            # Give interactive a decent share but leave headroom in pool 0.
            if pool_id == 0:
                cpu_target = max(2, hint_cpu, int(pool.max_cpu_pool * 0.6))
                ram_target = max(3, hint_ram, int(pool.max_ram_pool * 0.7))
            else:
                cpu_target = max(2, hint_cpu, int(pool.max_cpu_pool * 0.75))
                ram_target = max(3, hint_ram, int(pool.max_ram_pool * 0.8))

            req_cpu = _cap(cpu_target, s.min_cpu, int(pool.max_cpu_pool))
            req_ram = _cap(ram_target, s.min_ram, int(pool.max_ram_pool))
            req_cpu = min(req_cpu, avail_cpu)
            req_ram = min(req_ram, avail_ram)
            return req_cpu, req_ram

        # Batch
        if not allow_batch:
            return 0, 0

        # Conservative batch slices to keep latency headroom and reduce interference.
        # Still respect hints to avoid repeated OOM.
        cpu_cap = int(pool.max_cpu_pool * (0.35 if pool_id == 0 else 0.55))
        ram_cap = int(pool.max_ram_pool * (0.45 if pool_id == 0 else 0.65))

        req_cpu = max(base_cpu, hint_cpu, 1)
        req_ram = max(base_ram, hint_ram, 2)

        req_cpu = _cap(req_cpu, s.min_cpu, max(s.min_cpu, cpu_cap))
        req_ram = _cap(req_ram, s.min_ram, max(s.min_ram, ram_cap))

        req_cpu = min(req_cpu, avail_cpu)
        req_ram = min(req_ram, avail_ram)
        return req_cpu, req_ram

    def _pick_next_runnable(q, pool_id, allow_batch):
        """
        Pop/rotate until we find a runnable pipeline. Returns (pipeline, [op]) or (None, None).
        Keeps pipeline in queue (round-robin fairness within priority).
        """
        for _ in range(len(q)):
            p = q.popleft()
            if _is_done(p):
                s.enqueued_at.pop(p.pipeline_id, None)
                continue

            ops = _ready_ops(p)
            if not ops:
                q.append(p)
                continue

            # Filter out ops that exceeded retry budget (mark them as "skip"; pipeline may still finish if other ops exist)
            chosen_op = None
            for op in ops:
                key = (p.pipeline_id, _op_key(op))
                if s.op_fail_count.get(key, 0) <= s.max_retries_per_op:
                    chosen_op = op
                    break
            if chosen_op is None:
                q.append(p)
                continue

            # Put pipeline back for RR fairness; we schedule one op at a time per pipeline.
            q.append(p)
            return p, [chosen_op]

        return None, None

    # ---------------- Ingest new pipelines ----------------
    for p in pipelines:
        _enqueue_pipeline(p)

    # Early exit if nothing changed
    if not pipelines and not results:
        return [], []

    # ---------------- Learn from results (OOM backoff + retry counts) ----------------
    for r in results:
        if not (hasattr(r, "failed") and r.failed()):
            continue

        pid = getattr(r, "pipeline_id", None)
        if pid is None:
            continue

        ops = getattr(r, "ops", []) or []
        err = str(getattr(r, "error", "") or "")
        oom = _oom_like(err)

        used_ram = int(getattr(r, "ram", 1) or 1)
        used_cpu = int(getattr(r, "cpu", 1) or 1)

        for op in ops:
            key = (pid, _op_key(op))
            s.op_fail_count[key] = s.op_fail_count.get(key, 0) + 1

            prev_ram = int(s.op_ram_hint.get(key, max(s.min_ram, used_ram)) or max(s.min_ram, used_ram))
            prev_cpu = int(s.op_cpu_hint.get(key, max(s.min_cpu, used_cpu)) or max(s.min_cpu, used_cpu))

            if oom:
                # RAM is the typical fix: double, plus a small additive bump.
                s.op_ram_hint[key] = max(prev_ram, used_ram, prev_ram * 2 + 1)
                # Keep CPU steady on OOM to avoid wasting more resources.
                s.op_cpu_hint[key] = max(prev_cpu, used_cpu)
            else:
                # Generic failure: mild bumps to try alternative sizing.
                s.op_ram_hint[key] = max(prev_ram, used_ram, int(prev_ram * 1.4) + 1)
                s.op_cpu_hint[key] = max(prev_cpu, used_cpu, int(prev_cpu * 1.25) + 1)

    # ---------------- Global gating: avoid BATCH when high-priority runnable exists ----------------
    any_query_runnable = _has_any_runnable_in_queue(s.q_query)
    any_interactive_runnable = _has_any_runnable_in_queue(s.q_interactive)
    highprio_runnable = any_query_runnable or any_interactive_runnable

    # Allow batch only if no high-priority runnable; additionally, allow batch on pool 0 only if very old.
    allow_batch_globally = not highprio_runnable

    # ---------------- Create assignments (pack multiple per pool) ----------------
    suspensions = []
    assignments = []

    # Iterate pools; within each pool, keep assigning while resources allow.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]

        # If multiple pools: keep pool 0 for latency unless no high-priority runnable
        if s.executor.num_pools > 1 and pool_id == 0 and not highprio_runnable:
            # Still prefer interactive over batch on pool 0 if both exist (interactive helps weighted latency)
            pass

        # Attempt to fill this pool with multiple assignments.
        # Bounded loop prevents infinite scheduling when avail doesn't change in the same tick.
        for _ in range(64):
            avail_cpu = int(pool.avail_cpu_pool)
            avail_ram = int(pool.avail_ram_pool)
            if avail_cpu <= 0 or avail_ram <= 0:
                break

            # Choose which priority to schedule next for this pool
            chosen_p = None
            chosen_ops = None

            # Pool-specific ordering: pool 0 is QUERY -> INTERACTIVE -> (BATCH rarely)
            # Other pools: QUERY -> INTERACTIVE -> BATCH (but QUERY should still win globally)
            priority_order = [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]

            for prio in priority_order:
                q = _queue_for_priority(prio)

                # Batch admission control:
                if prio == Priority.BATCH_PIPELINE:
                    if not allow_batch_globally:
                        continue
                    if s.executor.num_pools > 1 and pool_id == 0:
                        # Only allow batch on pool0 if it's been waiting a long time (anti-starvation)
                        # and no high-priority runnable exists (already true here).
                        # Check oldest batch pipeline age quickly.
                        oldest_age = None
                        for i in range(min(len(q), 16)):
                            bp = q[i]
                            age = s.tick - s.enqueued_at.get(bp.pipeline_id, s.tick)
                            oldest_age = age if oldest_age is None else min(oldest_age, age)
                        if oldest_age is None or oldest_age < s.batch_allow_pool0_after_ticks:
                            continue

                # If multiple pools, route QUERY/INTERACTIVE to preferred pools first
                if s.executor.num_pools > 1 and prio in (Priority.QUERY, Priority.INTERACTIVE):
                    if pool_id != 0 and any_query_runnable:
                        # If any query runnable exists, avoid stealing resources from pool 0 unless pool 0 is absent.
                        # (This reduces interference and keeps query latency low.)
                        if prio == Priority.INTERACTIVE:
                            # Interactive can still run on non-zero pools while queries run on pool 0.
                            pass

                p, ops = _pick_next_runnable(q, pool_id, allow_batch_globally)
                if p is None:
                    continue

                # Size and ensure we can allocate
                req_cpu, req_ram = _size_for(p, ops[0], pool_id, allow_batch_globally)
                if req_cpu <= 0 or req_ram <= 0:
                    continue
                if req_cpu > pool.avail_cpu_pool or req_ram > pool.avail_ram_pool:
                    # Can't fit right now; try next candidate/priority.
                    continue

                chosen_p, chosen_ops = p, ops
                break

            if chosen_p is None:
                break

            assignments.append(
                Assignment(
                    ops=chosen_ops,
                    cpu=int(req_cpu),
                    ram=int(req_ram),
                    priority=chosen_p.priority,
                    pool_id=pool_id,
                    pipeline_id=chosen_p.pipeline_id,
                )
            )

    return suspensions, assignments
