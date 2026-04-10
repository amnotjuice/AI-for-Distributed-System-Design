# policy_key: scheduler_est_040
# reasoning_effort: low
# model: gpt-5.2-2025-12-11
# llm_cost: 0.042300
# generation_seconds: 31.65
# generated_at: 2026-04-01T00:11:18.178167
@register_scheduler_init(key="scheduler_est_040")
def scheduler_est_040_init(s):
    """Priority-aware FIFO with RAM-flooring using estimates and simple OOM backoff.

    Small, safe improvements over naive FIFO:
      1) Separate waiting queues by priority (QUERY > INTERACTIVE > BATCH) to reduce tail latency.
      2) Use op.estimate.mem_peak_gb (when present) as a RAM floor to reduce OOM risk.
      3) On failure that looks like OOM, remember a higher RAM floor for that operator and retry later.

    Non-goals (kept intentionally simple for correctness):
      - No preemption (requires reliable access to running containers).
      - No sophisticated CPU sizing; just small caps by priority to avoid batch hogging.
    """
    s.waiting_by_prio = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }
    # Per-operator RAM floor learned from failures (keyed by object id(op)).
    s.op_mem_floor_gb = {}
    # Track pipelines we've seen to avoid duplicate enqueues if generator replays references.
    s._seen_pipeline_ids = set()


def _prio_order():
    return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]


def _safe_float(x):
    try:
        if x is None:
            return None
        v = float(x)
        if v != v:  # NaN
            return None
        return v
    except Exception:
        return None


def _looks_like_oom(err) -> bool:
    if err is None:
        return False
    s = str(err).lower()
    return ("oom" in s) or ("out of memory" in s) or ("cuda out of memory" in s) or ("killed" in s and "memory" in s)


def _op_mem_floor(s, op) -> float:
    # Use learned floor first, then estimator (as a floor), else 0.
    learned = s.op_mem_floor_gb.get(id(op))
    if learned is not None:
        return max(0.0, float(learned))

    est = None
    try:
        est = _safe_float(getattr(getattr(op, "estimate", None), "mem_peak_gb", None))
    except Exception:
        est = None

    if est is None:
        return 0.0
    return max(0.0, float(est))


def _cpu_cap_for_priority(pool, prio):
    # Simple caps: reserve some headroom for high priority by not letting batch take everything.
    # Still bounded by pool.avail_cpu_pool when assigning.
    max_cpu = getattr(pool, "max_cpu_pool", None)
    if max_cpu is None:
        # Fallback: if max not available, don't cap.
        return None

    if prio == Priority.QUERY:
        return float(max_cpu)  # full
    if prio == Priority.INTERACTIVE:
        return max(1.0, 0.75 * float(max_cpu))
    return max(1.0, 0.50 * float(max_cpu))  # batch


def _enqueue_pipeline(s, p: "Pipeline"):
    # Avoid duplicate enqueues for the same pipeline_id.
    pid = getattr(p, "pipeline_id", None)
    if pid is not None and pid in s._seen_pipeline_ids:
        return
    if pid is not None:
        s._seen_pipeline_ids.add(pid)
    s.waiting_by_prio[p.priority].append(p)


def _pipeline_done_or_failed(p: "Pipeline") -> bool:
    st = p.runtime_status()
    if st.is_pipeline_successful():
        return True
    # If any op is FAILED, we still want to retry (since OOM/backoff can fix).
    # So we do NOT drop failed pipelines here.
    return False


def _next_assignable_op(p: "Pipeline"):
    st = p.runtime_status()
    ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
    if not ops:
        return None
    return ops[0]


def _requeue(s, p: "Pipeline"):
    s.waiting_by_prio[p.priority].append(p)


@register_scheduler(key="scheduler_est_040")
def scheduler_est_040_scheduler(s, results: "List[ExecutionResult]",
                                pipelines: "List[Pipeline]") -> "Tuple[List[Suspend], List[Assignment]]":
    # Incorporate new arrivals.
    for p in pipelines:
        _enqueue_pipeline(s, p)

    # Learn from results (OOM backoff) and allow pipelines to continue.
    if results:
        for r in results:
            if getattr(r, "failed", None) and r.failed() and _looks_like_oom(getattr(r, "error", None)):
                # Increase floor for each op in this failed container.
                # Use last allocated RAM + a safety margin, but cap later by pool capacity.
                try:
                    last_ram = float(getattr(r, "ram", 0.0) or 0.0)
                except Exception:
                    last_ram = 0.0
                bumped = max(last_ram * 1.5, last_ram + 1.0, 1.0)
                for op in getattr(r, "ops", []) or []:
                    cur = s.op_mem_floor_gb.get(id(op), 0.0)
                    s.op_mem_floor_gb[id(op)] = max(float(cur), float(bumped))

    # Early exit if nothing changed (keeps sim fast/deterministic).
    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    # Choose pool iteration order: for latency, try to place work where it starts soonest.
    # We'll evaluate pools each time; but keep it simple: sort pools by (avail_cpu desc, avail_ram desc).
    pool_ids = list(range(s.executor.num_pools))
    pool_ids.sort(
        key=lambda i: (s.executor.pools[i].avail_cpu_pool, s.executor.pools[i].avail_ram_pool),
        reverse=True,
    )

    # Fill each pool with at most one op per tick (small improvement over naive: strict priority selection).
    # This avoids one pool monopolizing scheduling decisions and helps spread interactive work.
    for pool_id in pool_ids:
        pool = s.executor.pools[pool_id]
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)
        if avail_cpu <= 0.0 or avail_ram <= 0.0:
            continue

        chosen_pipeline = None
        chosen_op = None

        # Pick the highest-priority pipeline that has an assignable op and can fit minimal RAM.
        for prio in _prio_order():
            q = s.waiting_by_prio[prio]
            if not q:
                continue

            # Scan a small prefix to avoid O(n) behavior under huge queues.
            # We requeue non-ready pipelines to preserve approximate FIFO within priority.
            scan = min(len(q), 32)
            picked_idx = None
            for idx in range(scan):
                p = q[idx]
                if _pipeline_done_or_failed(p):
                    picked_idx = idx  # remove it
                    chosen_pipeline = None
                    chosen_op = None
                    break  # remove done pipeline first
                op = _next_assignable_op(p)
                if op is None:
                    continue
                mem_floor = _op_mem_floor(s, op)
                if mem_floor <= avail_ram:
                    chosen_pipeline = p
                    chosen_op = op
                    picked_idx = idx
                    break

            if picked_idx is not None:
                # Remove the picked pipeline (either done or chosen for assignment).
                q.pop(picked_idx)
                # If we removed a done pipeline, continue searching at same priority.
                if chosen_pipeline is None:
                    # Continue trying to find a runnable pipeline in this pool.
                    # (We'll re-evaluate priorities afresh.)
                    pass
                else:
                    break

        if chosen_pipeline is None or chosen_op is None:
            continue

        # Compute RAM: allocate at least floor, but be generous if available (RAM doesn't speed up, but avoids OOM).
        mem_floor = _op_mem_floor(s, chosen_op)
        # Extra RAM is "free" per model; allocate up to 80% of available to increase success probability,
        # but never below floor.
        target_ram = max(mem_floor, 0.80 * avail_ram)
        ram = min(avail_ram, target_ram)
        if ram < mem_floor:
            # Shouldn't happen due to earlier check, but keep safe.
            _requeue(s, chosen_pipeline)
            continue

        # Compute CPU: high priority gets more CPU to reduce latency; batch is capped.
        cap = _cpu_cap_for_priority(pool, chosen_pipeline.priority)
        if cap is None:
            cpu = avail_cpu
        else:
            cpu = min(avail_cpu, float(cap))
        # Always give at least 1 vCPU if possible.
        if cpu <= 0.0:
            _requeue(s, chosen_pipeline)
            continue
        cpu = max(1.0, cpu)

        # Create assignment for exactly one operator.
        assignments.append(
            Assignment(
                ops=[chosen_op],
                cpu=cpu,
                ram=ram,
                priority=chosen_pipeline.priority,
                pool_id=pool_id,
                pipeline_id=chosen_pipeline.pipeline_id,
            )
        )

        # Requeue the pipeline so subsequent operators can be scheduled in future ticks.
        _requeue(s, chosen_pipeline)

    return suspensions, assignments
