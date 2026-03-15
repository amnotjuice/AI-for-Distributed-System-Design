# policy_key: scheduler_medium_003
# reasoning_effort: medium
# model: gpt-5.2-2025-12-11
# llm_cost: 0.084324
# generation_seconds: 112.02
# generated_at: 2026-03-12T21:55:22.480487
@register_scheduler_init(key="scheduler_medium_003")
def scheduler_medium_003_init(s):
    """Priority-aware, latency-improving scheduler (incremental upgrade over naive FIFO).

    Key ideas (kept intentionally simple / low-risk):
      1) Separate FIFO queues per priority; always try QUERY + INTERACTIVE before BATCH.
      2) Avoid "one op consumes whole pool" by using per-op resource targets (CPU/RAM slices).
      3) Keep headroom for high-priority work by reserving a fraction of each pool from BATCH
         when high-priority queues are non-empty (no preemption required).
      4) Basic OOM-aware retries: on OOM failures, increase RAM request for that operator.

    Notes:
      - No explicit preemption (Suspend) is used to avoid relying on undocumented executor APIs.
      - FAILED ops are treated as retryable; non-terminating failures are bounded by a simple
        per-pipeline failure-increase budget inferred from runtime_status().
    """
    from collections import deque

    # Waiting pipelines per priority (FIFO within each priority class)
    s.q_by_prio = {
        Priority.QUERY: deque(),
        Priority.INTERACTIVE: deque(),
        Priority.BATCH_PIPELINE: deque(),
    }

    # Map op identity -> pipeline_id (best-effort, used for failure attribution)
    s.op_to_pipeline_id = {}

    # Per-operator resource hints (learned primarily from OOM feedback)
    # Keys are (pipeline_id, op_uid) when pipeline_id is known, else just op_uid.
    s.op_ram_hint = {}
    s.op_cpu_hint = {}

    # Track observed failed-op counts per pipeline to bound endless retries
    s.pipeline_failed_seen = {}   # pipeline_id -> last seen FAILED count
    s.pipeline_fail_increases = {}  # pipeline_id -> number of times FAILED count increased

    # Tunables (kept conservative)
    s.max_fail_increases = 6  # after this many new failed ops, drop pipeline from queues

    # Default per-op slice targets by priority (fractions of pool capacity)
    s.cpu_frac = {
        Priority.QUERY: 0.35,
        Priority.INTERACTIVE: 0.35,
        Priority.BATCH_PIPELINE: 0.20,
    }
    s.ram_frac = {
        Priority.QUERY: 0.25,
        Priority.INTERACTIVE: 0.25,
        Priority.BATCH_PIPELINE: 0.15,
    }

    # Reserve headroom when high-priority work is waiting (to reduce latency without preemption)
    s.reserve_cpu_frac = 0.20
    s.reserve_ram_frac = 0.20


@register_scheduler(key="scheduler_medium_003")
def scheduler_medium_003(s, results, pipelines):
    """See init docstring for policy overview."""
    from collections import deque

    def _pool_score(pool):
        # Prefer pools with more headroom (helps large-RAM ops and reduces contention)
        return float(getattr(pool, "avail_cpu_pool", 0)) + float(getattr(pool, "avail_ram_pool", 0))

    def _iter_ops_best_effort(pipeline):
        # Best-effort op enumeration to build op->pipeline_id mapping.
        # The simulator's Pipeline.values is described as "DAG of operators"; we try to iterate it.
        vals = getattr(pipeline, "values", None)
        if vals is None:
            return []
        try:
            # If it's dict-like, iterate over values; if list-like, iterate directly.
            if hasattr(vals, "values"):
                return list(vals.values())
            return list(vals)
        except Exception:
            return []

    def _op_uid(op):
        # Use stable-in-process identity; simulator reuses operator objects within a run.
        # If an explicit id exists, prefer it.
        return getattr(op, "op_id", None) or getattr(op, "operator_id", None) or id(op)

    def _hint_key(pipeline_id, op):
        uid = _op_uid(op)
        if pipeline_id is None:
            return uid
        return (pipeline_id, uid)

    def _clamp(x, lo, hi):
        if x < lo:
            return lo
        if x > hi:
            return hi
        return x

    def _default_cpu(pool, prio):
        max_cpu = float(getattr(pool, "max_cpu_pool", 0) or 0)
        # Ensure a meaningful minimum if pool reports small/zero max (defensive).
        base = max(1.0, max_cpu * float(s.cpu_frac.get(prio, 0.25))) if max_cpu > 0 else 1.0
        return base

    def _default_ram(pool, prio):
        max_ram = float(getattr(pool, "max_ram_pool", 0) or 0)
        base = max(1.0, max_ram * float(s.ram_frac.get(prio, 0.15))) if max_ram > 0 else 1.0
        return base

    def _is_oom(err):
        if not err:
            return False
        try:
            e = str(err).lower()
        except Exception:
            return False
        return ("oom" in e) or ("out of memory" in e) or ("out-of-memory" in e) or ("memoryerror" in e)

    def _any_high_prio_waiting():
        return (len(s.q_by_prio[Priority.QUERY]) > 0) or (len(s.q_by_prio[Priority.INTERACTIVE]) > 0)

    # Enqueue new pipelines by priority and build best-effort op->pipeline mapping
    for p in pipelines:
        pr = getattr(p, "priority", Priority.BATCH_PIPELINE)
        if pr not in s.q_by_prio:
            # Unknown priority -> treat as batch
            pr = Priority.BATCH_PIPELINE
        s.q_by_prio[pr].append(p)

        pid = getattr(p, "pipeline_id", None)
        if pid is not None:
            # Best-effort index of ops -> pipeline id, used to attribute failures
            for op in _iter_ops_best_effort(p):
                s.op_to_pipeline_id[_op_uid(op)] = pid

    # Update resource hints based on execution results (especially OOM)
    for r in results or []:
        try:
            failed = r.failed()
        except Exception:
            failed = False

        if not failed:
            continue

        pool_id = getattr(r, "pool_id", None)
        pool = None
        if pool_id is not None and 0 <= int(pool_id) < int(getattr(s.executor, "num_pools", 0) or 0):
            pool = s.executor.pools[int(pool_id)]

        err = getattr(r, "error", None)
        oom = _is_oom(err)

        # Try to update RAM hint(s) for the failing op(s)
        ops = list(getattr(r, "ops", []) or [])
        for op in ops:
            uid = _op_uid(op)
            pid = s.op_to_pipeline_id.get(uid, None)
            hk = _hint_key(pid, op)

            if oom:
                # Increase RAM hint aggressively but clamp to pool max when known.
                prev = s.op_ram_hint.get(hk, None)
                observed = getattr(r, "ram", None)
                # Prefer observed container RAM if present; else fall back to previous/default.
                base = None
                if observed is not None:
                    try:
                        base = float(observed)
                    except Exception:
                        base = None
                if base is None:
                    base = float(prev) if prev is not None else None

                if base is None:
                    # If we can't infer, bump to a conservative fraction of pool max if available
                    if pool is not None:
                        base = max(1.0, float(getattr(pool, "max_ram_pool", 0) or 1.0) * 0.25)
                    else:
                        base = 2.0

                new_ram = max(base * 2.0, base + 1.0)
                if pool is not None:
                    max_ram = float(getattr(pool, "max_ram_pool", 0) or 0)
                    if max_ram > 0:
                        new_ram = min(new_ram, max_ram)
                s.op_ram_hint[hk] = new_ram

                # Optional: slightly reduce CPU hint on repeated OOM to avoid cache/mem pressure
                prev_cpu = s.op_cpu_hint.get(hk, None)
                if prev_cpu is not None:
                    try:
                        s.op_cpu_hint[hk] = max(1.0, float(prev_cpu) * 0.9)
                    except Exception:
                        pass

    # If no new info, early exit (keeps determinism + reduces overhead)
    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    # Order pools by available headroom
    num_pools = int(getattr(s.executor, "num_pools", 0) or 0)
    pool_order = list(range(num_pools))
    try:
        pool_order.sort(key=lambda i: _pool_score(s.executor.pools[i]), reverse=True)
    except Exception:
        pass

    # Priority order: prioritize low tail latency
    prio_order = [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]

    # Main scheduling loop: pack multiple assignments per pool using per-op slices.
    for pool_id in pool_order:
        pool = s.executor.pools[pool_id]
        avail_cpu = float(getattr(pool, "avail_cpu_pool", 0) or 0)
        avail_ram = float(getattr(pool, "avail_ram_pool", 0) or 0)
        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # Reserve headroom from batch if high-priority work is waiting
        reserve_cpu = float(getattr(pool, "max_cpu_pool", 0) or 0) * float(getattr(s, "reserve_cpu_frac", 0.0))
        reserve_ram = float(getattr(pool, "max_ram_pool", 0) or 0) * float(getattr(s, "reserve_ram_frac", 0.0))
        high_waiting = _any_high_prio_waiting()

        # Prevent issuing multiple ops from the same pipeline on the same pool in one tick
        assigned_pipelines_this_pool = set()

        made_progress = True
        while made_progress:
            made_progress = False

            for pr in prio_order:
                q = s.q_by_prio.get(pr, deque())
                if not q:
                    continue

                # Compute effective availability for this priority (batch respects reservation)
                eff_cpu = avail_cpu
                eff_ram = avail_ram
                if pr == Priority.BATCH_PIPELINE and high_waiting:
                    eff_cpu = max(0.0, avail_cpu - reserve_cpu)
                    eff_ram = max(0.0, avail_ram - reserve_ram)

                if eff_cpu <= 0 or eff_ram <= 0:
                    continue

                # Try to find a pipeline in this priority queue that has an assignable op
                qlen = len(q)
                if qlen == 0:
                    continue

                picked = None  # (pipeline, op)
                for _ in range(qlen):
                    pipeline = q[0]
                    q.rotate(-1)  # round-robin within priority

                    pid = getattr(pipeline, "pipeline_id", None)
                    if pid is not None and pid in assigned_pipelines_this_pool:
                        continue

                    status = pipeline.runtime_status()

                    # Drop successful pipelines
                    if status.is_pipeline_successful():
                        # Remove from queue by rotating back and popping one instance:
                        # Since we've rotated, easiest is to filter later; do an in-place mark.
                        # We'll just skip scheduling it and let the periodic cleanup below remove it.
                        continue

                    # Bound pathological repeated failures (inferred by FAILED count changes)
                    failed_cnt = int(getattr(status, "state_counts", {}).get(OperatorState.FAILED, 0) or 0)
                    if pid is not None:
                        prev_failed = int(s.pipeline_failed_seen.get(pid, 0) or 0)
                        if failed_cnt > prev_failed:
                            s.pipeline_failed_seen[pid] = failed_cnt
                            s.pipeline_fail_increases[pid] = int(s.pipeline_fail_increases.get(pid, 0) or 0) + 1
                        if int(s.pipeline_fail_increases.get(pid, 0) or 0) > int(getattr(s, "max_fail_increases", 6) or 6):
                            # Stop wasting resources; pipeline considered irrecoverable
                            continue

                    # Find one assignable op whose parents are complete
                    op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
                    if not op_list:
                        continue

                    op = op_list[0]
                    picked = (pipeline, op_list)
                    break

                if picked is None:
                    continue

                pipeline, op_list = picked
                pid = getattr(pipeline, "pipeline_id", None)

                # Determine per-op CPU/RAM request using hints (OOM backoff) or defaults
                op = op_list[0]
                hk = _hint_key(pid, op)

                cpu_req = s.op_cpu_hint.get(hk, None)
                if cpu_req is None:
                    cpu_req = _default_cpu(pool, pr)

                ram_req = s.op_ram_hint.get(hk, None)
                if ram_req is None:
                    ram_req = _default_ram(pool, pr)

                # If only batch is waiting (no high prio), allow "fill remaining" to improve throughput
                if pr == Priority.BATCH_PIPELINE and (not high_waiting) and (len(s.q_by_prio[Priority.QUERY]) == 0) and (len(s.q_by_prio[Priority.INTERACTIVE]) == 0):
                    cpu_req = max(cpu_req, eff_cpu)
                    ram_req = max(ram_req, eff_ram)

                # Enforce effective availability constraints
                if cpu_req <= 0 or ram_req <= 0:
                    continue
                if cpu_req > eff_cpu or ram_req > eff_ram:
                    continue

                # Also don't exceed true pool availability (eff_* already derived from avail_*)
                cpu_req = min(cpu_req, avail_cpu)
                ram_req = min(ram_req, avail_ram)
                if cpu_req <= 0 or ram_req <= 0:
                    continue

                assignments.append(
                    Assignment(
                        ops=op_list,
                        cpu=cpu_req,
                        ram=ram_req,
                        priority=getattr(pipeline, "priority", pr),
                        pool_id=pool_id,
                        pipeline_id=pid,
                    )
                )

                # Update available resources and flags
                avail_cpu -= float(cpu_req)
                avail_ram -= float(ram_req)
                if pid is not None:
                    assigned_pipelines_this_pool.add(pid)

                made_progress = True

                # If pool is exhausted, stop scheduling on this pool
                if avail_cpu <= 0 or avail_ram <= 0:
                    break

            # End for priorities
        # End while made_progress

    # Opportunistic cleanup: remove completed pipelines from queues to keep them small
    for pr, q in list(s.q_by_prio.items()):
        if not q:
            continue
        kept = deque()
        while q:
            p = q.popleft()
            try:
                if p.runtime_status().is_pipeline_successful():
                    continue
            except Exception:
                pass
            kept.append(p)
        s.q_by_prio[pr] = kept

    return suspensions, assignments
