# policy_key: scheduler_high_009
# reasoning_effort: high
# exp: reasoning
# model: gpt-5.2-2025-12-11
# llm_cost: 0.166751
# generation_seconds: 158.67
# generated_at: 2026-04-10T00:38:21.307410
@register_scheduler_init(key="scheduler_high_009")
def scheduler_high_009_init(s):
    """Priority-first, OOM-aware, no-preemption scheduler.

    Goals:
      - Minimize weighted end-to-end latency with strong protection for QUERY and INTERACTIVE.
      - Avoid failures (720s penalty) by retrying FAILED ops with RAM backoff on suspected OOM.
      - Reduce head-of-line blocking vs. naive FIFO by:
          * using per-priority queues,
          * reserving headroom for high-priority work (soft reservation, no preemption),
          * avoiding running BATCH on the "latency pool" (pool 0) while high-priority is waiting,
          * limiting per-op CPU so multiple queries can run concurrently when resources allow.
    """
    # Per-priority FIFO queues of Pipeline objects.
    s.queues = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }

    # Discrete tick counter (increments each scheduler call).
    s.tick = 0

    # Track when pipelines were enqueued (optional aging hooks / debugging).
    s.enqueue_tick = {}  # pipeline_id -> tick

    # Prevent repeatedly scanning pipelines that already have an in-flight op.
    s.inflight_pipelines = set()  # pipeline_id
    s.op_to_pipeline = {}         # op_key -> pipeline_id

    # Per-operator adaptive state (keyed by op identity).
    # Note: Eudoxia passes operator objects around; we assume identity is stable across results/status.
    s.op_state = {}  # op_key -> dict(retry=int, next_ram=float|None, last_ram=float|None, last_cpu=float|None, hard_fail=int)

    # Conservative retry limits: keep completion rate high but avoid infinite churn on non-OOM failures.
    s.max_retries_oom = 6
    s.max_retries_hard = 2

    # Limit how many new assignments we try to launch per pool per tick (avoid over-scheduling bursts).
    s.max_assignments_per_pool = 4

    # Soft reservation when any high-priority is waiting: don't let batch consume the last slice.
    s.reserve_frac_cpu = 0.20
    s.reserve_frac_ram = 0.20


def _op_key(op):
    """Best-effort stable key for an operator."""
    for attr in ("op_id", "operator_id", "id", "name"):
        if hasattr(op, attr):
            try:
                v = getattr(op, attr)
                # Avoid returning bound methods or complex objects.
                if isinstance(v, (int, str)):
                    return (attr, v)
            except Exception:
                pass
    return ("py_id", id(op))


def _is_oom_error(err):
    """Heuristic: detect OOM-like failures from error string."""
    if err is None:
        return False
    try:
        msg = str(err).lower()
    except Exception:
        return False
    return ("oom" in msg) or ("out of memory" in msg) or ("memory" in msg and "alloc" in msg)


def _active_pipeline(p):
    """Whether the pipeline should still be considered."""
    try:
        st = p.runtime_status()
        if st.is_pipeline_successful():
            return False
    except Exception:
        # If status is unavailable, treat as active.
        return True
    return True


def _has_assignable_ready_op(p):
    """Returns (op or None). Only considers ops whose parents are complete."""
    try:
        st = p.runtime_status()
        ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        if ops:
            return ops[0]
    except Exception:
        return None
    return None


def _count_waiting_pipelines(q):
    """Count pipelines that are still active (best effort)."""
    c = 0
    for p in q:
        if _active_pipeline(p):
            c += 1
    return c


def _compute_request(s, pool, priority, opk, avail_cpu, avail_ram, q_counts, allow_batch, high_waiting):
    """Compute (cpu, ram) request for a single operator in this pool."""
    max_cpu = float(pool.max_cpu_pool)
    max_ram = float(pool.max_ram_pool)

    # Base fractions adapt to queue pressure:
    # - if multiple waiting at that priority, keep allocations smaller to allow concurrency
    # - otherwise, allocate more to reduce tail latency / OOM risk
    if priority == Priority.QUERY:
        many = q_counts.get(Priority.QUERY, 0) >= 2
        cpu_frac = 0.55 if many else 0.85
        ram_frac = 0.55 if many else 0.80
        cpu_cap = 8.0
    elif priority == Priority.INTERACTIVE:
        many = q_counts.get(Priority.INTERACTIVE, 0) >= 2
        cpu_frac = 0.50 if many else 0.75
        ram_frac = 0.55 if many else 0.70
        cpu_cap = 8.0
    else:  # BATCH_PIPELINE
        # Batch is throughput-oriented; avoid monopolizing when high-priority is waiting.
        cpu_frac = 0.90 if not high_waiting else 0.60
        ram_frac = 0.70 if not high_waiting else 0.55
        cpu_cap = max_cpu

    # Convert fractions into requests, with lower bounds.
    req_cpu = min(float(avail_cpu), max(1.0, min(cpu_cap, max_cpu * cpu_frac)))
    req_ram = min(float(avail_ram), max(1e-6, max_ram * ram_frac))

    # Apply OOM backoff if present.
    st = s.op_state.get(opk)
    if st and st.get("next_ram") is not None:
        req_ram = max(req_ram, float(st["next_ram"]))

    # If we cannot satisfy required RAM for an OOM-retrying op, don't schedule it now.
    if st and st.get("next_ram") is not None and float(avail_ram) + 1e-9 < float(st["next_ram"]):
        return None

    # Soft reservation: when high-priority is waiting, avoid consuming the last slice with batch.
    if priority == Priority.BATCH_PIPELINE and high_waiting:
        if not allow_batch:
            return None
        reserve_cpu = max_cpu * float(getattr(s, "reserve_frac_cpu", 0.0))
        reserve_ram = max_ram * float(getattr(s, "reserve_frac_ram", 0.0))

        # Try to shrink request to preserve headroom; if can't, skip.
        max_cpu_for_batch = float(avail_cpu) - reserve_cpu
        max_ram_for_batch = float(avail_ram) - reserve_ram
        if max_cpu_for_batch < 1.0 or max_ram_for_batch <= 0:
            return None
        req_cpu = min(req_cpu, max_cpu_for_batch)
        req_ram = min(req_ram, max_ram_for_batch)
        if req_cpu < 1.0 or req_ram <= 0:
            return None

    # Final clamps.
    req_cpu = max(1.0, min(req_cpu, float(avail_cpu)))
    req_ram = max(1e-6, min(req_ram, float(avail_ram)))
    return req_cpu, req_ram


def _pick_candidate_from_queue(s, pool, priority, avail_cpu, avail_ram, q_counts, allow_batch, high_waiting):
    """Pop/rotate within a priority queue to find a schedulable (pipeline, op, cpu, ram)."""
    q = s.queues[priority]
    if not q:
        return None

    n = len(q)
    for _ in range(n):
        p = q.pop(0)

        # Drop completed pipelines.
        if not _active_pipeline(p):
            continue

        pid = getattr(p, "pipeline_id", None)

        # If pipeline already has an in-flight op, rotate it to the back.
        if pid is not None and pid in s.inflight_pipelines:
            q.append(p)
            continue

        op = _has_assignable_ready_op(p)
        if op is None:
            # Not ready yet; keep it queued.
            q.append(p)
            continue

        opk = _op_key(op)
        st = s.op_state.get(opk, None)
        if st:
            # Avoid infinite churn on non-OOM failures.
            hard_fail = int(st.get("hard_fail", 0))
            retry = int(st.get("retry", 0))
            if hard_fail >= s.max_retries_hard:
                # Give up this operator; pipeline will remain incomplete but we avoid wasting resources.
                # Don't requeue; it's effectively dead weight.
                continue
            if st.get("next_ram") is not None and retry >= s.max_retries_oom:
                # Too many OOM retries.
                continue

        req = _compute_request(
            s=s,
            pool=pool,
            priority=priority,
            opk=opk,
            avail_cpu=avail_cpu,
            avail_ram=avail_ram,
            q_counts=q_counts,
            allow_batch=allow_batch,
            high_waiting=high_waiting,
        )
        if req is None:
            q.append(p)
            continue

        cpu, ram = req
        # Put pipeline back to track future stages; inflight prevents re-selection until result arrives.
        q.append(p)
        return p, op, cpu, ram

    return None


@register_scheduler(key="scheduler_high_009")
def scheduler_high_009(s, results: List["ExecutionResult"], pipelines: List["Pipeline"]) -> Tuple[List["Suspend"], List["Assignment"]]:
    """
    Priority-first, OOM-aware scheduling loop.

    - Enqueue arrivals by priority (QUERY > INTERACTIVE > BATCH).
    - Update per-op retry/backoff using execution results:
        * On suspected OOM: increase next_ram (exponential backoff).
        * On other failure: limited hard-fail retries, then give up that op.
      Always clears in-flight flags for ops that produced a result.
    - For each pool, greedily launch up to K ops, favoring:
        * pool 0 for latency-sensitive work (no batch while high-priority waiting),
        * soft reservation to keep headroom for high-priority arrivals,
        * moderate per-op CPU to allow concurrency for queries/interactives.
    """
    s.tick += 1

    # ---- Ingest new pipelines ----
    for p in pipelines:
        pr = getattr(p, "priority", Priority.BATCH_PIPELINE)
        if pr not in s.queues:
            pr = Priority.BATCH_PIPELINE
        s.queues[pr].append(p)
        try:
            s.enqueue_tick[p.pipeline_id] = s.tick
        except Exception:
            pass

    # ---- Process results (update backoff + clear inflight) ----
    for r in results:
        # Clear inflight markers for any ops in the result.
        try:
            for op in (r.ops or []):
                opk = _op_key(op)
                pid = s.op_to_pipeline.pop(opk, None)
                if pid is not None and pid in s.inflight_pipelines:
                    s.inflight_pipelines.discard(pid)
        except Exception:
            pass

        # Update per-op state based on success/failure.
        failed = False
        try:
            failed = r.failed()
        except Exception:
            failed = False

        if not failed:
            continue

        oom = _is_oom_error(getattr(r, "error", None))

        # We can use pool max RAM from the pool that ran the op, as a cap for next_ram.
        try:
            pool = s.executor.pools[r.pool_id]
            cap_ram = float(pool.max_ram_pool)
        except Exception:
            cap_ram = None

        try:
            for op in (r.ops or []):
                opk = _op_key(op)
                st = s.op_state.get(opk)
                if st is None:
                    st = {"retry": 0, "next_ram": None, "last_ram": None, "last_cpu": None, "hard_fail": 0}
                    s.op_state[opk] = st

                st["retry"] = int(st.get("retry", 0)) + 1
                st["last_ram"] = float(getattr(r, "ram", 0.0) or 0.0)
                st["last_cpu"] = float(getattr(r, "cpu", 0.0) or 0.0)

                if oom:
                    prev = float(getattr(r, "ram", 0.0) or 0.0)
                    # Exponential RAM backoff; if prev is 0 for some reason, start with a small positive.
                    proposed = (prev * 1.6) if prev > 0 else 1.0
                    # Ensure monotonic increase if we already had a target.
                    if st.get("next_ram") is not None:
                        proposed = max(proposed, float(st["next_ram"]))
                    if cap_ram is not None:
                        proposed = min(proposed, cap_ram)
                    st["next_ram"] = proposed
                else:
                    # Non-OOM failures: allow limited retries, but do not endlessly churn.
                    st["hard_fail"] = int(st.get("hard_fail", 0)) + 1
        except Exception:
            pass

    # If nothing changed, exit early.
    if not pipelines and not results:
        return [], []

    suspensions: List["Suspend"] = []
    assignments: List["Assignment"] = []

    # Precompute rough queue pressure (counts of active pipelines).
    q_counts = {
        Priority.QUERY: _count_waiting_pipelines(s.queues[Priority.QUERY]),
        Priority.INTERACTIVE: _count_waiting_pipelines(s.queues[Priority.INTERACTIVE]),
        Priority.BATCH_PIPELINE: _count_waiting_pipelines(s.queues[Priority.BATCH_PIPELINE]),
    }
    high_waiting = (q_counts[Priority.QUERY] > 0) or (q_counts[Priority.INTERACTIVE] > 0)

    # ---- Pool-by-pool greedy scheduling ----
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)

        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # Pool role:
        # - pool 0: "latency pool" (protect QUERY/INTERACTIVE)
        # - other pools: throughput pools (can run batch, and can spill high priority if needed)
        if pool_id == 0:
            prio_order = [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]
            allow_batch = not high_waiting
        else:
            # If high priority is waiting, still try to run it early to reduce latency (spillover).
            prio_order = [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE] if high_waiting else [
                Priority.BATCH_PIPELINE, Priority.INTERACTIVE, Priority.QUERY
            ]
            allow_batch = True

        launches = 0
        while launches < int(getattr(s, "max_assignments_per_pool", 4)):
            if avail_cpu <= 0 or avail_ram <= 0:
                break

            picked = None
            for pr in prio_order:
                cand = _pick_candidate_from_queue(
                    s=s,
                    pool=pool,
                    priority=pr,
                    avail_cpu=avail_cpu,
                    avail_ram=avail_ram,
                    q_counts=q_counts,
                    allow_batch=allow_batch,
                    high_waiting=high_waiting,
                )
                if cand is not None:
                    picked = (pr, cand)
                    break

            if picked is None:
                break

            pr, (pipeline, op, cpu, ram) = picked

            # Record inflight mapping to avoid re-scheduling the same pipeline until we get a result.
            pid = getattr(pipeline, "pipeline_id", None)
            opk = _op_key(op)
            if pid is not None:
                s.inflight_pipelines.add(pid)
                s.op_to_pipeline[opk] = pid

            # Persist last requested resources (useful for next backoff step).
            st = s.op_state.get(opk)
            if st is None:
                st = {"retry": 0, "next_ram": None, "last_ram": None, "last_cpu": None, "hard_fail": 0}
                s.op_state[opk] = st
            st["last_ram"] = float(ram)
            st["last_cpu"] = float(cpu)

            assignments.append(
                Assignment(
                    ops=[op],
                    cpu=cpu,
                    ram=ram,
                    priority=pipeline.priority,
                    pool_id=pool_id,
                    pipeline_id=pipeline.pipeline_id,
                )
            )

            avail_cpu -= float(cpu)
            avail_ram -= float(ram)
            launches += 1

    return suspensions, assignments
