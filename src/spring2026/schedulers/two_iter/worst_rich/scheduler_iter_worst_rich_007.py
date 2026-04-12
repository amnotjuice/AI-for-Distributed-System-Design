# policy_key: scheduler_iter_worst_rich_007
# context: rich
# model: gpt-5.2-2025-12-11
# llm_cost: 0.051444
# generation_seconds: 35.50
# generated_at: 2026-04-12T00:52:46.032411
@register_scheduler_init(key="scheduler_iter_worst_rich_007")
def scheduler_iter_worst_rich_007_init(s):
    """
    Iteration 2: Fix catastrophic OOM behavior first, then apply simple priority handling.

    Key changes vs previous attempt:
      - Never drop a pipeline just because it has FAILED ops; FAILED ops are retryable.
      - Allocate *much more RAM by default* (close to naive FIFO: use most/all available RAM).
      - On OOM, exponentially back off RAM for the specific operator and retry (bounded by pool max).
      - Priority-aware admission:
          * Always consider QUERY > INTERACTIVE > BATCH.
          * If any high-priority work is waiting, cap BATCH allocations to preserve headroom.
          * With multiple pools: prefer pool 0 for QUERY/INTERACTIVE; prefer non-0 pools for BATCH.
      - Keep it simple: no preemption/suspension yet.
    """
    from collections import deque

    s.q_query = deque()
    s.q_interactive = deque()
    s.q_batch = deque()

    # Avoid enqueuing the same pipeline repeatedly
    s.enqueued = set()  # pipeline_id

    # Per-(pipeline, op) learned minimum RAM (based on OOM retries)
    s.op_ram_hint = {}  # (pipeline_id, op_key) -> ram

    # Keep a small tick counter for bounded scans/debug-style behavior
    s.tick = 0

    # If high-priority work exists, batch gets at most these fractions of pool resources
    s.batch_cpu_cap_if_hp_waiting = 0.50
    s.batch_ram_cap_if_hp_waiting = 0.60

    # For QUERY/INTERACTIVE, be close to naive: grab most of what's free
    s.hp_cpu_frac = 1.00
    s.hp_ram_frac = 1.00

    # For BATCH when no HP waiting, still be fairly aggressive to avoid underprovision OOM
    s.batch_cpu_frac = 1.00
    s.batch_ram_frac = 1.00


@register_scheduler(key="scheduler_iter_worst_rich_007")
def scheduler_iter_worst_rich_007_scheduler(s, results, pipelines):
    """
    Priority-aware, OOM-resilient FIFO-like scheduler.

    Strategy per tick:
      1) Enqueue new pipelines by priority.
      2) Learn from OOM failures: increase RAM hint for the failed op (per pipeline).
      3) For each pool, schedule at most one operator:
          - Choose highest-priority runnable pipeline.
          - Use pool placement preference: pool 0 for HP, others for batch if possible.
          - Size RAM aggressively (near all available) to prevent OOM storms.
          - If any HP is waiting, cap batch to preserve latency headroom.
    """
    s.tick += 1

    # ---- Helpers (local to keep module scope clean) ----
    def _q_for_priority(prio):
        if prio == Priority.QUERY:
            return s.q_query
        if prio == Priority.INTERACTIVE:
            return s.q_interactive
        return s.q_batch

    def _enqueue(p):
        pid = p.pipeline_id
        if pid in s.enqueued:
            return
        s.enqueued.add(pid)
        _q_for_priority(p.priority).append(p)

    def _op_key(op):
        # Best-effort stable op identity.
        if hasattr(op, "op_id"):
            return ("op_id", getattr(op, "op_id"))
        if hasattr(op, "operator_id"):
            return ("operator_id", getattr(op, "operator_id"))
        if hasattr(op, "name"):
            return ("name", getattr(op, "name"))
        return ("py_id", id(op))

    def _pipeline_done(p):
        st = p.runtime_status()
        return st.is_pipeline_successful()

    def _first_ready_op(p):
        st = p.runtime_status()
        ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        if not ops:
            return None
        return ops[0]

    def _hp_waiting():
        # Consider "waiting" if queues contain any pipelines that are not already complete.
        # (We keep lazy removals; so scan a few items to avoid heavy work.)
        for q in (s.q_query, s.q_interactive):
            for _ in range(min(len(q), 8)):
                p = q[0]
                if _pipeline_done(p):
                    q.popleft()
                    s.enqueued.discard(p.pipeline_id)
                    continue
                return True
        return False

    def _pool_order_for_priority(prio):
        # Prefer pool 0 for HP and non-0 for batch when multiple pools exist.
        n = s.executor.num_pools
        if n <= 1:
            return [0]
        if prio in (Priority.QUERY, Priority.INTERACTIVE):
            return [0] + [i for i in range(1, n)]
        return [i for i in range(1, n)] + [0]

    def _size_for(p, op, pool_id, hp_waiting_flag):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool
        if avail_cpu <= 0 or avail_ram <= 0:
            return 0, 0

        # Choose aggressive defaults to avoid OOM storms.
        if p.priority in (Priority.QUERY, Priority.INTERACTIVE):
            cpu = int(avail_cpu * s.hp_cpu_frac)
            ram = int(avail_ram * s.hp_ram_frac)
        else:
            cpu = int(avail_cpu * s.batch_cpu_frac)
            ram = int(avail_ram * s.batch_ram_frac)

            # If any high-priority is waiting, cap batch allocations to preserve headroom.
            if hp_waiting_flag:
                cpu = min(cpu, max(1, int(pool.max_cpu_pool * s.batch_cpu_cap_if_hp_waiting)))
                ram = min(ram, max(1, int(pool.max_ram_pool * s.batch_ram_cap_if_hp_waiting)))

        # Apply learned RAM hint for this op (dominates if larger).
        hint = s.op_ram_hint.get((p.pipeline_id, _op_key(op)))
        if hint is not None:
            ram = max(ram, int(hint))

        # Bound and ensure minimums
        cpu = max(1, min(cpu, avail_cpu))
        ram = max(1, min(ram, avail_ram))
        return cpu, ram

    # ---- Ingest new pipelines ----
    for p in pipelines:
        _enqueue(p)

    # Early exit if nothing changed
    if not pipelines and not results:
        return [], []

    # ---- Learn from results: OOM => bump RAM hint for failed ops ----
    for r in results:
        if hasattr(r, "failed") and r.failed():
            # If we don't have pipeline_id in the result, we can't attach a hint reliably.
            pid = getattr(r, "pipeline_id", None)
            if pid is None:
                continue

            err = str(getattr(r, "error", "") or "").lower()
            oom_like = ("oom" in err) or ("out of memory" in err) or ("memory" in err)

            ops = getattr(r, "ops", None) or []
            if not ops:
                continue

            for op in ops:
                k = (pid, _op_key(op))
                prev = s.op_ram_hint.get(k, int(getattr(r, "ram", 1) or 1))
                # Exponential backoff on OOM; otherwise small bump.
                if oom_like:
                    s.op_ram_hint[k] = max(prev + 1, prev * 2)
                else:
                    s.op_ram_hint[k] = max(prev + 1, int(prev * 1.25) + 1)

    # ---- Dispatch (one assignment per pool) ----
    suspensions = []
    assignments = []

    hp_waiting_flag = _hp_waiting()

    # We'll decide assignments per pool. For each pool, try to pick a pipeline that matches
    # the pool preference and has a runnable op.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        if pool.avail_cpu_pool <= 0 or pool.avail_ram_pool <= 0:
            continue

        chosen_p = None
        chosen_op = None

        # Determine which priority we should try first for this pool:
        # pool 0: HP first; others: batch first (but still fall back to HP).
        if s.executor.num_pools > 1 and pool_id != 0:
            priority_scan = (s.q_batch, s.q_query, s.q_interactive)
        else:
            priority_scan = (s.q_query, s.q_interactive, s.q_batch)

        # Bounded scan over each queue to find the first runnable pipeline
        for q in priority_scan:
            for _ in range(min(len(q), 64)):
                p = q.popleft()

                # Drop completed pipelines lazily
                if _pipeline_done(p):
                    s.enqueued.discard(p.pipeline_id)
                    continue

                op = _first_ready_op(p)
                if op is None:
                    # Not ready; keep it in queue
                    q.append(p)
                    continue

                # Pool preference check: if multiple pools exist, try to avoid running batch on pool 0
                # when there are other pools with capacity (simple heuristic).
                if s.executor.num_pools > 1:
                    pref = _pool_order_for_priority(p.priority)
                    if pool_id != pref[0]:
                        # Put it back; we'll still allow it if nothing else fits.
                        q.append(p)
                        continue

                chosen_p, chosen_op = p, op
                # Put it back after scheduling attempt so pipeline remains eligible for future ops.
                q.append(p)
                break

            if chosen_p is not None:
                break

        # If nothing matched pool preference, do a second pass without preference constraints.
        if chosen_p is None:
            for q in (s.q_query, s.q_interactive, s.q_batch):
                for _ in range(min(len(q), 64)):
                    p = q.popleft()
                    if _pipeline_done(p):
                        s.enqueued.discard(p.pipeline_id)
                        continue
                    op = _first_ready_op(p)
                    if op is None:
                        q.append(p)
                        continue
                    chosen_p, chosen_op = p, op
                    q.append(p)
                    break
                if chosen_p is not None:
                    break

        if chosen_p is None:
            continue

        cpu, ram = _size_for(chosen_p, chosen_op, pool_id, hp_waiting_flag)
        if cpu <= 0 or ram <= 0:
            continue

        assignments.append(
            Assignment(
                ops=[chosen_op],
                cpu=cpu,
                ram=ram,
                priority=chosen_p.priority,
                pool_id=pool_id,
                pipeline_id=chosen_p.pipeline_id,
            )
        )

    return suspensions, assignments
