# policy_key: scheduler_high_005
# reasoning_effort: high
# exp: reasoning
# model: gpt-5.2-2025-12-11
# llm_cost: 0.134453
# generation_seconds: 150.22
# generated_at: 2026-04-10T00:26:24.527407
@register_scheduler_init(key="scheduler_high_005")
def scheduler_high_005_init(s):
    """
    Priority-first, completion-oriented scheduler with conservative batch sizing.

    Goals (aligned to weighted-latency objective):
      - Minimize query & interactive latency by always serving them first and (when possible)
        keeping one pool lightly contended.
      - Reduce failure/incomplete penalties by retrying FAILED operators (especially OOM)
        with increased RAM, learned per-operator.
      - Prevent batch starvation via a simple quota/aging mechanism, but keep batch containers small
        while high-priority work exists to preserve headroom without preemption.
    """
    from collections import deque

    s.tick = 0

    # Round-robin queues per priority (store pipeline_id).
    s.queues = {
        Priority.QUERY: deque(),
        Priority.INTERACTIVE: deque(),
        Priority.BATCH_PIPELINE: deque(),
    }

    # Active pipelines by id (latest object reference).
    s.active = {}

    # First time we saw a pipeline id (for simple aging / starvation control).
    s.first_seen_tick = {}

    # Learned per-operator hints (keyed by a stable-ish op key).
    s.op_ram_hint = {}      # op_key -> recommended RAM
    s.op_cpu_hint = {}      # op_key -> recommended CPU
    s.op_fail_count = {}    # op_key -> failures observed
    s.op_oom_count = {}     # op_key -> OOM failures observed

    # Fairness knobs: allow a batch to run occasionally even under high-priority load.
    s.consecutive_query = 0
    s.consecutive_hi = 0
    s.batch_quota_hi = 14      # after this many hi-priority dispatches, allow one batch
    s.interactive_quota_q = 7  # after this many consecutive queries, allow one interactive if waiting

    # Track which pipelines were dispatched recently (avoid double-dispatch of same pipeline in a tick).
    s._last_dispatch_tick = {}  # pipeline_id -> tick


def _op_key(op):
    # Prefer explicit identifiers if they exist; fall back to repr (deterministic enough in-sim).
    for attr in ("op_id", "operator_id", "task_id", "name", "id"):
        if hasattr(op, attr):
            try:
                v = getattr(op, attr)
                if v is not None:
                    return (attr, str(v))
            except Exception:
                pass
    return ("repr", repr(op))


def _is_oom_error(err):
    if err is None:
        return False
    try:
        s = str(err).lower()
    except Exception:
        return False
    return ("oom" in s) or ("out of memory" in s) or ("memory" in s and "out" in s)


def _base_caps(priority, pool, hi_waiting):
    """
    Return (cpu_cap, ram_cap) for initial sizing.
    While high-priority exists, cap batch small to preserve headroom (no preemption).
    When no high-priority exists, allow batch to expand for throughput/completion.
    """
    max_cpu = pool.max_cpu_pool
    max_ram = pool.max_ram_pool

    if priority == Priority.QUERY:
        return max(1, int(0.75 * max_cpu)), max(1, int(0.65 * max_ram))
    if priority == Priority.INTERACTIVE:
        return max(1, int(0.60 * max_cpu)), max(1, int(0.55 * max_ram))

    # Batch
    if hi_waiting:
        return max(1, int(0.35 * max_cpu)), max(1, int(0.35 * max_ram))
    return max(1, int(0.90 * max_cpu)), max(1, int(0.90 * max_ram))


def _reserve_for_hi(pool):
    # Keep enough headroom to start at least one query-ish container quickly.
    return max(1, int(0.55 * pool.max_cpu_pool)), max(1, int(0.55 * pool.max_ram_pool))


def _queue_has_ready(s, priority, limit_scan=8):
    """
    Conservative readiness check (bounded scan) to decide whether to protect headroom.
    """
    q = s.queues.get(priority)
    if not q:
        return False
    n = min(len(q), limit_scan)
    for i in range(n):
        pid = q[i]
        p = s.active.get(pid)
        if not p:
            continue
        status = p.runtime_status()
        if status.is_pipeline_successful():
            continue
        ops = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        if ops:
            return True
    return False


def _pick_pipeline_with_ready_op(s, priority, scheduled_pipelines):
    """
    Round-robin pick of a pipeline that has at least one ready (assignable) op.
    Returns (pipeline, op) or (None, None).
    """
    q = s.queues[priority]
    if not q:
        return None, None

    tries = len(q)
    while tries > 0:
        tries -= 1
        pid = q.popleft()

        p = s.active.get(pid)
        if p is None:
            continue

        # Avoid dispatching the same pipeline multiple times in the same scheduler tick.
        if scheduled_pipelines is not None and pid in scheduled_pipelines:
            q.append(pid)
            continue

        status = p.runtime_status()
        if status.is_pipeline_successful():
            # Drop silently.
            continue

        ops = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        if not ops:
            # Not ready; keep it in rotation.
            q.append(pid)
            continue

        # Found a runnable op; keep pipeline in queue for future ops (RR).
        q.append(pid)
        return p, ops[0]

    return None, None


def _choose_priority_for_pool(s, pool_id, any_query_ready, any_interactive_ready, any_batch_ready):
    """
    Choose which priority to try scheduling on this pool given fairness + pool role.
    If multiple pools exist, keep pool 0 biased toward high priority to protect tail latency.
    """
    # Hard bias: dedicate pool 0 to QUERY/INTERACTIVE when possible.
    if s.executor.num_pools >= 2 and pool_id == 0:
        if any_query_ready:
            return Priority.QUERY
        if any_interactive_ready:
            return Priority.INTERACTIVE
        if any_batch_ready:
            return Priority.BATCH_PIPELINE
        return None

    # General pools: priority-first with small fairness.
    if any_query_ready:
        # Give interactive a chance if queries have dominated for too long.
        if any_interactive_ready and s.consecutive_query >= s.interactive_quota_q:
            return Priority.INTERACTIVE
        return Priority.QUERY

    if any_interactive_ready:
        return Priority.INTERACTIVE

    if any_batch_ready:
        return Priority.BATCH_PIPELINE

    return None


@register_scheduler(key="scheduler_high_005")
def scheduler_high_005_scheduler(s, results: List[ExecutionResult], pipelines: List[Pipeline]) -> Tuple[List[Suspend], List[Assignment]]:
    s.tick += 1

    # In this policy we avoid preemption (no reliable container inventory exposed),
    # and instead preserve headroom by sizing batch small under high-priority load.
    suspensions: List[Suspend] = []
    assignments: List[Assignment] = []

    # --- Ingest new pipelines ---
    for p in pipelines:
        s.active[p.pipeline_id] = p
        if p.pipeline_id not in s.first_seen_tick:
            s.first_seen_tick[p.pipeline_id] = s.tick
        # Enqueue once; duplicates are okay but wasteful—guard lightly.
        s.queues[p.priority].append(p.pipeline_id)

    # --- Learn from execution results (OOM-aware retries) ---
    for r in results:
        if not getattr(r, "ops", None):
            continue
        pool = s.executor.pools[r.pool_id] if r.pool_id is not None else None
        max_ram = pool.max_ram_pool if pool is not None else None
        max_cpu = pool.max_cpu_pool if pool is not None else None

        failed = False
        try:
            failed = r.failed()
        except Exception:
            failed = bool(getattr(r, "error", None))

        for op in r.ops:
            k = _op_key(op)

            if failed:
                s.op_fail_count[k] = s.op_fail_count.get(k, 0) + 1

                # On OOM: aggressively increase RAM hint to avoid repeated 720s penalties.
                if _is_oom_error(getattr(r, "error", None)):
                    s.op_oom_count[k] = s.op_oom_count.get(k, 0) + 1
                    prev = s.op_ram_hint.get(k, int(getattr(r, "ram", 1)) if getattr(r, "ram", None) else 1)
                    bump = max(prev * 2, int(getattr(r, "ram", 1) * 2) if getattr(r, "ram", None) else prev * 2)
                    if max_ram is not None:
                        bump = min(bump, max_ram)
                    s.op_ram_hint[k] = max(1, int(bump))

                # If it's not clearly OOM, still nudge CPU/RAM slightly (kept conservative).
                else:
                    if getattr(r, "ram", None):
                        prev = s.op_ram_hint.get(k, int(r.ram))
                        bump = int(max(prev, int(r.ram * 1.25)))
                        if max_ram is not None:
                            bump = min(bump, max_ram)
                        s.op_ram_hint[k] = max(1, bump)

                    if getattr(r, "cpu", None):
                        prevc = s.op_cpu_hint.get(k, int(r.cpu))
                        bumpc = int(max(prevc, int(r.cpu * 1.25)))
                        if max_cpu is not None:
                            bumpc = min(bumpc, max_cpu)
                        s.op_cpu_hint[k] = max(1, bumpc)

            else:
                # Success: lock in at least the successful size as a safe baseline.
                if getattr(r, "ram", None):
                    s.op_ram_hint[k] = max(int(r.ram), int(s.op_ram_hint.get(k, 0)))
                if getattr(r, "cpu", None):
                    s.op_cpu_hint[k] = max(int(r.cpu), int(s.op_cpu_hint.get(k, 0)))

                # Reset fail counter on success to avoid runaway allocations on later retries.
                if k in s.op_fail_count:
                    s.op_fail_count[k] = 0
                if k in s.op_oom_count:
                    s.op_oom_count[k] = 0

    # --- Early exit if nothing changed ---
    if not pipelines and not results:
        return suspensions, assignments

    # --- Determine whether high-priority work is waiting (ready) ---
    any_query_ready = _queue_has_ready(s, Priority.QUERY)
    any_interactive_ready = _queue_has_ready(s, Priority.INTERACTIVE)
    any_batch_ready = _queue_has_ready(s, Priority.BATCH_PIPELINE)
    hi_waiting = any_query_ready or any_interactive_ready

    # Fairness gate: allow occasional batch even under hi-priority load.
    allow_batch_under_hi = False
    if hi_waiting and any_batch_ready and s.consecutive_hi >= s.batch_quota_hi:
        allow_batch_under_hi = True

    # Track per-tick dispatch to avoid multi-dispatch of same pipeline.
    scheduled_pipelines = set()

    # --- Schedule at most one assignment per pool per tick (simple, predictable) ---
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool
        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # Decide which priority to attempt on this pool first.
        pr = _choose_priority_for_pool(s, pool_id, any_query_ready, any_interactive_ready, any_batch_ready)
        if pr is None:
            continue

        # Under high-priority waiting, normally avoid starting new batch work except by fairness gate.
        if pr == Priority.BATCH_PIPELINE and hi_waiting and not allow_batch_under_hi:
            # Try to schedule high-priority instead (spillover to nonzero pools).
            if any_query_ready:
                pr = Priority.QUERY
            elif any_interactive_ready:
                pr = Priority.INTERACTIVE
            else:
                continue

        pipeline, op = _pick_pipeline_with_ready_op(s, pr, scheduled_pipelines)
        if pipeline is None or op is None:
            # If chosen priority had nothing runnable, attempt next lower priority.
            fallbacks = []
            if pr == Priority.QUERY:
                fallbacks = [Priority.INTERACTIVE, Priority.BATCH_PIPELINE]
            elif pr == Priority.INTERACTIVE:
                fallbacks = [Priority.QUERY, Priority.BATCH_PIPELINE]
            else:
                fallbacks = [Priority.QUERY, Priority.INTERACTIVE]

            picked = False
            for alt in fallbacks:
                if alt == Priority.BATCH_PIPELINE and hi_waiting and not allow_batch_under_hi:
                    continue
                pipeline, op = _pick_pipeline_with_ready_op(s, alt, scheduled_pipelines)
                if pipeline is not None and op is not None:
                    pr = alt
                    picked = True
                    break
            if not picked:
                continue

        # Compute caps and learned hints for this operator.
        cpu_cap, ram_cap = _base_caps(pr, pool, hi_waiting)
        k = _op_key(op)

        # If OOM repeated, favor RAM even more (bounded by pool max). This reduces repeated 720 penalties.
        oom_cnt = s.op_oom_count.get(k, 0)
        learned_ram = int(s.op_ram_hint.get(k, 0))
        learned_cpu = int(s.op_cpu_hint.get(k, 0))

        # Base desired sizes.
        desired_ram = max(ram_cap, learned_ram) if learned_ram > 0 else ram_cap
        desired_cpu = max(cpu_cap, learned_cpu) if learned_cpu > 0 else cpu_cap

        # If batch is under hi-waiting, keep it small unless it has failed OOM multiple times.
        if pr == Priority.BATCH_PIPELINE and hi_waiting and not allow_batch_under_hi:
            desired_ram = min(desired_ram, ram_cap)
            desired_cpu = min(desired_cpu, cpu_cap)

        # If batch is allowed under hi pressure (fairness), still preserve a minimal reserve on the pool.
        if pr == Priority.BATCH_PIPELINE and hi_waiting and allow_batch_under_hi:
            res_cpu, res_ram = _reserve_for_hi(pool)
            # If scheduling this batch would erase reserve, keep it smaller; if impossible, skip batch.
            if avail_cpu - 1 < res_cpu or avail_ram - 1 < res_ram:
                # No safe room; skip batch on this pool right now.
                continue
            desired_cpu = min(desired_cpu, max(1, avail_cpu - res_cpu))
            desired_ram = min(desired_ram, max(1, avail_ram - res_ram))

        # If we have repeated OOMs and no hi-priority is waiting, allow more aggressive RAM to complete.
        if pr == Priority.BATCH_PIPELINE and (not hi_waiting) and oom_cnt >= 1:
            desired_ram = max(desired_ram, int(0.75 * pool.max_ram_pool))

        # Finally clamp to available resources (can't exceed).
        cpu = int(max(1, min(avail_cpu, desired_cpu)))
        ram = int(max(1, min(avail_ram, desired_ram)))

        # If we can’t even give a minimally meaningful slice, skip.
        if cpu <= 0 or ram <= 0:
            continue

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

        scheduled_pipelines.add(pipeline.pipeline_id)
        s._last_dispatch_tick[pipeline.pipeline_id] = s.tick

        # Update fairness counters.
        if pr == Priority.QUERY:
            s.consecutive_query += 1
            s.consecutive_hi += 1
        elif pr == Priority.INTERACTIVE:
            s.consecutive_query = 0
            s.consecutive_hi += 1
        else:
            # Batch served: reset hi streak so we don't over-serve batch under contention.
            s.consecutive_query = 0
            s.consecutive_hi = 0
            allow_batch_under_hi = False  # consume the fairness slot

    return suspensions, assignments
