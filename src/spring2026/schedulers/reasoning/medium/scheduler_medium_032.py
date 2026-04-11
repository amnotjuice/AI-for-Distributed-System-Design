# policy_key: scheduler_medium_032
# reasoning_effort: medium
# model: gpt-5.2-2025-12-11
# llm_cost: 0.098797
# generation_seconds: 110.58
# generated_at: 2026-03-14T03:00:10.649014
@register_scheduler_init(key="scheduler_medium_032")
def scheduler_medium_032_init(s):
    """
    Priority-aware, low-risk improvement over naive FIFO.

    Key ideas (incremental vs. the baseline):
      1) Maintain separate FIFO queues per priority; always try higher priorities first.
      2) Add simple "headroom reservation": when high-priority work is waiting, limit how much
         CPU/RAM batch work can consume in a pool so interactive/query latency stays low.
      3) Retry OOM-failed operators with increased RAM (bounded exponential backoff).
         (Non-OOM failures are treated as terminal when we can attribute them to a pipeline.)
    """
    s.tick = 0

    # Per-priority FIFO queues (pipelines stored once; we rotate for scan).
    s.queues = {}

    # First-enqueue tick for aging / fairness.
    s.enqueue_tick = {}

    # Per-(pipeline, op) hints learned from OOMs.
    s.ram_hint = {}          # (pipeline_id, op_key) -> ram
    s.oom_retries = {}       # (pipeline_id, op_key) -> count

    # Pipelines we decided to stop scheduling (e.g., repeated OOM or non-OOM failures).
    s.dead_pipelines = set()

    # Conservative knobs: keep the policy simple and stable.
    s.max_assignments_per_pool = 2

    # When high-priority is waiting, batch can only use (max - reserve) in each pool.
    s.reserve_frac_cpu = 0.25
    s.reserve_frac_ram = 0.25

    # Let batch "age" into reserved headroom after waiting long enough.
    s.batch_aging_ticks = 50

    # OOM retry budget per operator.
    s.max_oom_retries = 3


@register_scheduler(key="scheduler_medium_032")
def scheduler_medium_032(s, results, pipelines):
    """
    See init() docstring. This scheduler is intentionally simple:
      - No preemption (keeps churn low, avoids relying on unknown executor internals).
      - No complex operator size inference beyond OOM-based RAM backoff.
      - Pool selection: place high-priority ops onto the pool with the most usable headroom.
    """
    # ---------- helpers (kept local to avoid global imports) ----------
    def _priority_order():
        # Highest -> lowest
        return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]

    def _init_queues_if_needed():
        if s.queues:
            return
        for pr in _priority_order():
            s.queues[pr] = []

    def _is_oom(err):
        if err is None:
            return False
        txt = str(err).lower()
        return ("oom" in txt) or ("out of memory" in txt) or ("memory" in txt and "alloc" in txt)

    def _op_key(op):
        # Best-effort stable identifier for hints; fallback to object id.
        for attr in ("op_id", "operator_id", "task_id", "id", "name"):
            v = getattr(op, attr, None)
            if v is not None:
                return v
        return id(op)

    def _extract_pipeline_id_from_ops(op_list):
        # Try to locate a pipeline_id from operator objects.
        for op in op_list or []:
            pid = getattr(op, "pipeline_id", None)
            if pid is not None:
                return pid
            pip = getattr(op, "pipeline", None)
            if pip is not None:
                pid2 = getattr(pip, "pipeline_id", None)
                if pid2 is not None:
                    return pid2
        return None

    def _has_high_pri_waiting():
        # Quick check (queue non-empty after pruning pass).
        return (len(s.queues.get(Priority.QUERY, [])) > 0) or (len(s.queues.get(Priority.INTERACTIVE, [])) > 0)

    def _prune_queue(pr):
        # Drop completed/dead pipelines from the queue while preserving FIFO order of the rest.
        q = s.queues[pr]
        if not q:
            return
        kept = []
        for p in q:
            if p is None:
                continue
            if p.pipeline_id in s.dead_pipelines:
                continue
            st = p.runtime_status()
            if st.is_pipeline_successful():
                continue
            kept.append(p)
        s.queues[pr] = kept

    def _next_assignable_op(p):
        st = p.runtime_status()
        # Require parents complete for correctness.
        op_list = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
        return op_list

    def _batch_is_aged(p):
        t0 = s.enqueue_tick.get(p.pipeline_id, s.tick)
        return (s.tick - t0) >= s.batch_aging_ticks

    def _pool_snapshot():
        snaps = []
        for pool_id in range(s.executor.num_pools):
            pool = s.executor.pools[pool_id]
            snaps.append(
                {
                    "pool_id": pool_id,
                    "avail_cpu": pool.avail_cpu_pool,
                    "avail_ram": pool.avail_ram_pool,
                    "max_cpu": pool.max_cpu_pool,
                    "max_ram": pool.max_ram_pool,
                    "assigned_count": 0,
                }
            )
        return snaps

    def _usable_capacity(snap, pr, high_pri_waiting, allow_batch_into_reserve):
        # Compute how much of this pool we are willing to allocate *for this assignment*.
        avail_cpu = snap["avail_cpu"]
        avail_ram = snap["avail_ram"]
        if avail_cpu <= 0 or avail_ram <= 0:
            return 0, 0

        if pr == Priority.BATCH_PIPELINE and high_pri_waiting and not allow_batch_into_reserve:
            reserve_cpu = snap["max_cpu"] * s.reserve_frac_cpu
            reserve_ram = snap["max_ram"] * s.reserve_frac_ram
            usable_cpu = max(0, avail_cpu - reserve_cpu)
            usable_ram = max(0, avail_ram - reserve_ram)
            return usable_cpu, usable_ram

        return avail_cpu, avail_ram

    def _default_request_fraction(pr):
        # Conservative: give more to latency-sensitive ops, but avoid always taking 100%.
        if pr == Priority.QUERY:
            return 0.90, 0.90
        if pr == Priority.INTERACTIVE:
            return 0.75, 0.75
        return 0.50, 0.50  # batch

    def _choose_best_pool_for(pr, op_list, pid, snaps, high_pri_waiting, allow_batch_into_reserve):
        # Pick pool with maximum usable CPU (tie-breaker: usable RAM).
        best = None
        best_score = None
        for snap in snaps:
            if snap["assigned_count"] >= s.max_assignments_per_pool:
                continue

            usable_cpu, usable_ram = _usable_capacity(
                snap, pr, high_pri_waiting=high_pri_waiting, allow_batch_into_reserve=allow_batch_into_reserve
            )
            if usable_cpu <= 0 or usable_ram <= 0:
                continue

            # Verify we can meet RAM hint if present.
            op = op_list[0]
            hk = (pid, _op_key(op))
            hint_ram = s.ram_hint.get(hk, None)
            if hint_ram is not None and hint_ram > usable_ram:
                continue

            score = (usable_cpu, usable_ram)
            if best is None or score > best_score:
                best = snap
                best_score = score

        return best

    def _compute_request(pr, pid, op_list, snap, high_pri_waiting, allow_batch_into_reserve):
        usable_cpu, usable_ram = _usable_capacity(
            snap, pr, high_pri_waiting=high_pri_waiting, allow_batch_into_reserve=allow_batch_into_reserve
        )
        if usable_cpu <= 0 or usable_ram <= 0:
            return None, None

        cpu_frac, ram_frac = _default_request_fraction(pr)

        # Start with a fraction of pool max (bounded by usable and availability).
        req_cpu = min(usable_cpu, snap["max_cpu"] * cpu_frac)
        req_ram = min(usable_ram, snap["max_ram"] * ram_frac)

        # Apply learned RAM hint for this operator (OOM backoff), if any.
        op = op_list[0]
        hk = (pid, _op_key(op))
        hint_ram = s.ram_hint.get(hk, None)
        if hint_ram is not None:
            req_ram = max(req_ram, hint_ram)

        # Final clamp to what's usable now.
        req_cpu = min(req_cpu, usable_cpu)
        req_ram = min(req_ram, usable_ram)

        # Avoid zero allocations.
        if req_cpu <= 0:
            req_cpu = min(usable_cpu, 1)
        if req_ram <= 0:
            req_ram = min(usable_ram, 1)

        # If hints force too-large requests, caller should have filtered; still guard.
        if req_cpu <= 0 or req_ram <= 0:
            return None, None
        return req_cpu, req_ram

    def _rotate_pop_runnable(pr):
        """
        Scan the priority queue in FIFO order to find a pipeline with an assignable op.
        Returns (pipeline, op_list) or (None, None). Queue is rotated to preserve fairness.
        """
        q = s.queues[pr]
        if not q:
            return None, None

        n = len(q)
        for _ in range(n):
            p = q.pop(0)

            if p is None:
                continue
            if p.pipeline_id in s.dead_pipelines:
                continue

            st = p.runtime_status()
            if st.is_pipeline_successful():
                continue

            op_list = _next_assignable_op(p)
            if op_list:
                # Put back at tail (round-robin within priority).
                q.append(p)
                return p, op_list

            # Not runnable now (waiting for parents, already running, etc.) -> rotate to tail.
            q.append(p)

        return None, None

    # ---------- main ----------
    _init_queues_if_needed()
    s.tick += 1

    # Ingest new pipelines
    for p in pipelines:
        if p is None:
            continue
        if p.pipeline_id in s.dead_pipelines:
            continue
        if p.pipeline_id not in s.enqueue_tick:
            s.enqueue_tick[p.pipeline_id] = s.tick
        s.queues[p.priority].append(p)

    # Update hints from results (OOM backoff) and optionally mark pipelines dead.
    # Note: ExecutionResult doesn't guarantee pipeline_id, so we infer from ops when possible.
    if results:
        for r in results:
            if not r.failed():
                continue

            # Only act on failures we can reasonably treat.
            if _is_oom(getattr(r, "error", None)):
                pid = _extract_pipeline_id_from_ops(getattr(r, "ops", None))
                if pid is None:
                    # Can't attribute -> skip learning (safe fallback).
                    continue

                pool = s.executor.pools[r.pool_id]
                for op in (r.ops or []):
                    hk = (pid, _op_key(op))
                    cnt = s.oom_retries.get(hk, 0) + 1
                    s.oom_retries[hk] = cnt
                    if cnt > s.max_oom_retries:
                        s.dead_pipelines.add(pid)
                        break

                    # Exponential backoff with a small additive bump; clamp to pool max RAM.
                    prev = s.ram_hint.get(hk, 0)
                    # r.ram is what we tried; if absent, just bump previous.
                    tried = getattr(r, "ram", None)
                    if tried is None:
                        new_hint = max(prev + 1, 1)
                    else:
                        new_hint = max(prev, tried * 2, tried + 1)
                    s.ram_hint[hk] = min(pool.max_ram_pool, new_hint)

            else:
                # Non-OOM failure: if we can identify the pipeline, stop retrying to avoid loops.
                pid = _extract_pipeline_id_from_ops(getattr(r, "ops", None))
                if pid is not None:
                    s.dead_pipelines.add(pid)

    # If nothing changed, we can early-exit (no new resources without results; no new work without pipelines).
    if not pipelines and not results:
        return [], []

    # Prune queues to remove completed/dead pipelines (keeps the policy from wasting scans).
    for pr in _priority_order():
        _prune_queue(pr)

    high_pri_waiting = _has_high_pri_waiting()
    snaps = _pool_snapshot()

    suspensions = []  # no preemption in this policy (keeps it stable)
    assignments = []

    # Schedule in strict priority order.
    for pr in _priority_order():
        # For batch: only allow dipping into reserved headroom if aged.
        # For high priority: always allowed to use full available.
        while True:
            p, op_list = _rotate_pop_runnable(pr)
            if p is None:
                break

            allow_batch_into_reserve = True
            if pr == Priority.BATCH_PIPELINE and high_pri_waiting:
                allow_batch_into_reserve = _batch_is_aged(p)

            best_snap = _choose_best_pool_for(
                pr, op_list, p.pipeline_id, snaps, high_pri_waiting=high_pri_waiting, allow_batch_into_reserve=allow_batch_into_reserve
            )
            if best_snap is None:
                # No pool can fit this op right now; stop trying this priority this tick.
                # (Avoid spinning; next results tick may free resources.)
                break

            req_cpu, req_ram = _compute_request(
                pr, p.pipeline_id, op_list, best_snap, high_pri_waiting=high_pri_waiting, allow_batch_into_reserve=allow_batch_into_reserve
            )
            if req_cpu is None or req_ram is None:
                break

            # Final feasibility check vs. true snapshot availability.
            if req_cpu > best_snap["avail_cpu"] or req_ram > best_snap["avail_ram"]:
                break

            assignments.append(
                Assignment(
                    ops=op_list,
                    cpu=req_cpu,
                    ram=req_ram,
                    priority=p.priority,
                    pool_id=best_snap["pool_id"],
                    pipeline_id=p.pipeline_id,
                )
            )

            # Update snapshot resources after this assignment.
            best_snap["avail_cpu"] -= req_cpu
            best_snap["avail_ram"] -= req_ram
            best_snap["assigned_count"] += 1

            # Keep scheduling until we can't place more (per pool caps / resource exhaustion).
            # This intentionally favors getting more high-priority work running quickly.

    return suspensions, assignments
