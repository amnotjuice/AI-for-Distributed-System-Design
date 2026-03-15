# policy_key: scheduler_none_018
# reasoning_effort: none
# model: gpt-5.2-2025-12-11
# llm_cost: 0.037659
# generation_seconds: 33.38
# generated_at: 2026-03-12T21:33:02.474301
@register_scheduler_init(key="scheduler_none_018")
def scheduler_none_018_init(s):
    """Priority-aware FIFO with cautious sizing + simple OOM backoff.

    Small improvements over naive FIFO:
      1) Keep separate queues per priority and always serve higher priority first.
      2) Avoid "give everything" sizing; use a conservative default slice to reduce interference.
      3) If an operator OOMs, bump its RAM request (exponential backoff) on retry.
      4) If an operator is repeatedly OOMing, eventually allow it to take most of the pool to finish.
    """
    # Per-priority waiting queues (pipelines). We'll round-robin within a priority via FIFO.
    s.waiting_queues = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }

    # Remember per-(pipeline, op) resource hints learned from OOMs/observations.
    # key: (pipeline_id, op_id) -> {"ram": float, "cpu": float, "ooms": int}
    s.op_hints = {}

    # Some conservative defaults for initial assignment sizing.
    s.default_cpu_frac = 0.5      # default to half of currently available CPU in pool
    s.default_ram_frac = 0.5      # default to half of currently available RAM in pool
    s.min_cpu = 1.0               # never request below this CPU (if available)
    s.min_ram = 1.0               # never request below this RAM (if available)

    # OOM backoff parameters.
    s.oom_ram_multiplier = 2.0    # double RAM hint on OOM
    s.max_oom_before_monopoly = 2 # after this many OOMs, allow near-monopoly to finish


def _prio_order():
    # Highest to lowest.
    return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]


def _first_assignable_op(pipeline):
    status = pipeline.runtime_status()
    # Only schedule ops whose parents are complete.
    op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
    if not op_list:
        return None
    return op_list[0]


def _op_id(op):
    # Try several common fields; fall back to stable string.
    for attr in ("op_id", "operator_id", "id", "name"):
        if hasattr(op, attr):
            try:
                v = getattr(op, attr)
                if v is not None:
                    return v
            except Exception:
                pass
    return str(op)


def _clamp(v, lo, hi):
    if v < lo:
        return lo
    if v > hi:
        return hi
    return v


def _choose_sizing(s, pool, pipeline_id, op):
    """Choose cpu/ram for op based on pool availability and learned hints."""
    avail_cpu = pool.avail_cpu_pool
    avail_ram = pool.avail_ram_pool
    max_cpu = pool.max_cpu_pool
    max_ram = pool.max_ram_pool

    # Baseline conservative slice: avoid saturating pool with a single op.
    base_cpu = max(s.min_cpu, avail_cpu * s.default_cpu_frac)
    base_ram = max(s.min_ram, avail_ram * s.default_ram_frac)

    # If we have a hint, use it (bounded by available and pool limits).
    key = (pipeline_id, _op_id(op))
    hint = s.op_hints.get(key)
    if hint:
        # Prefer hinted RAM strongly (OOM-avoidance), CPU moderately.
        base_ram = max(base_ram, hint.get("ram", 0.0))
        base_cpu = max(base_cpu, hint.get("cpu", 0.0))

        # If we have repeated OOMs, let this op take most of the pool to ensure progress.
        if hint.get("ooms", 0) >= s.max_oom_before_monopoly:
            base_ram = max(base_ram, min(avail_ram, max_ram) * 0.95)
            base_cpu = max(base_cpu, min(avail_cpu, max_cpu) * 0.95)

    cpu = _clamp(base_cpu, 0.0, min(avail_cpu, max_cpu))
    ram = _clamp(base_ram, 0.0, min(avail_ram, max_ram))

    # Ensure if we can schedule at all, we request at least some resources.
    if cpu <= 0 or ram <= 0:
        return 0.0, 0.0
    return cpu, ram


@register_scheduler(key="scheduler_none_018")
def scheduler_none_018(s, results, pipelines):
    """
    Priority-aware FIFO scheduler with simple OOM backoff.

    Policy steps:
      - Enqueue new pipelines into per-priority FIFO queues.
      - Process execution results:
          * On OOM-like failure, increase RAM hint for the failed op.
          * On success, record last used (cpu, ram) as a weak hint (optional, conservative).
      - For each pool, try to schedule at most one op (like naive), but:
          * pick from highest priority non-empty queue
          * use conservative sizing + hints
      - No preemption initially (small incremental improvement).
    """
    # Enqueue new pipelines.
    for p in pipelines:
        if p.priority not in s.waiting_queues:
            # In case new priority values appear, treat as lowest.
            s.waiting_queues.setdefault(p.priority, [])
        s.waiting_queues[p.priority].append(p)

    # If no new signals, do nothing (fast path).
    if not pipelines and not results:
        return [], []

    # Update hints based on results.
    for r in results:
        # We only learn from single-op assignments; if multiple, apply same sizing to each.
        for op in (r.ops or []):
            key = (r.pipeline_id if hasattr(r, "pipeline_id") else None, _op_id(op))
            # Some simulators may not expose pipeline_id on result; fall back to None-scoped hints.
            hint = s.op_hints.get(key, {"ram": 0.0, "cpu": 0.0, "ooms": 0})

            if r.failed():
                # Detect OOM-ish errors via string match (best-effort).
                err = (str(r.error).lower() if r.error is not None else "")
                if "oom" in err or "out of memory" in err or "memory" in err:
                    hint["ooms"] = hint.get("ooms", 0) + 1
                    # Exponential RAM backoff; start from the attempted ram if present.
                    attempted_ram = getattr(r, "ram", 0.0) or 0.0
                    if hint.get("ram", 0.0) <= 0 and attempted_ram > 0:
                        hint["ram"] = attempted_ram
                    hint["ram"] = max(hint.get("ram", 0.0), attempted_ram) * s.oom_ram_multiplier
                    # Keep CPU at least what was attempted (do not reduce).
                    attempted_cpu = getattr(r, "cpu", 0.0) or 0.0
                    hint["cpu"] = max(hint.get("cpu", 0.0), attempted_cpu)
                else:
                    # Non-OOM failure: don't aggressively change sizing; keep hints as-is.
                    pass
            else:
                # Success: weakly remember last used sizing to avoid undersizing repeats.
                attempted_ram = getattr(r, "ram", 0.0) or 0.0
                attempted_cpu = getattr(r, "cpu", 0.0) or 0.0
                hint["ram"] = max(hint.get("ram", 0.0), attempted_ram * 0.75)
                hint["cpu"] = max(hint.get("cpu", 0.0), attempted_cpu * 0.75)

            s.op_hints[key] = hint

    suspensions = []
    assignments = []

    # Helper: pop next runnable pipeline in priority order (but requeue if not runnable yet).
    def pop_next_pipeline_for_prio(prio):
        q = s.waiting_queues.get(prio, [])
        while q:
            p = q.pop(0)
            st = p.runtime_status()

            # Drop completed pipelines.
            if st.is_pipeline_successful():
                continue

            # If pipeline has failed ops, we still allow retry because ASSIGNABLE_STATES includes FAILED.
            # But if the runtime marks irrecoverable failures differently, this will naturally stop producing ops.
            op = _first_assignable_op(p)
            if op is None:
                # Not runnable yet; keep it in queue for later.
                q.append(p)
                return None, None
            return p, op
        return None, None

    # For each pool, attempt to schedule one operator (keeps change small vs naive).
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        if pool.avail_cpu_pool <= 0 or pool.avail_ram_pool <= 0:
            continue

        chosen = None
        chosen_op = None

        for prio in _prio_order():
            p, op = pop_next_pipeline_for_prio(prio)
            if p is not None and op is not None:
                chosen = p
                chosen_op = op
                break

        if chosen is None:
            continue

        cpu, ram = _choose_sizing(s, pool, chosen.pipeline_id, chosen_op)
        if cpu <= 0 or ram <= 0:
            # Can't place now; requeue and move on.
            s.waiting_queues[chosen.priority].append(chosen)
            continue

        assignment = Assignment(
            ops=[chosen_op],
            cpu=cpu,
            ram=ram,
            priority=chosen.priority,
            pool_id=pool_id,
            pipeline_id=chosen.pipeline_id,
        )
        assignments.append(assignment)

        # Requeue pipeline for future ops.
        s.waiting_queues[chosen.priority].append(chosen)

    return suspensions, assignments
