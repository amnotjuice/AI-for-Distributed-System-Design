# policy_key: scheduler_high_029
# reasoning_effort: high
# model: gpt-5.2-2025-12-11
# llm_cost: 0.170687
# generation_seconds: 188.74
# generated_at: 2026-03-14T03:49:07.472211
@register_scheduler_init(key="scheduler_high_029")
def scheduler_high_029_init(s):
    """Priority-aware, OOM-adaptive scheduler (incremental improvement over naive FIFO).

    Improvements vs naive FIFO:
      1) Priority queues: always consider QUERY/INTERACTIVE before BATCH.
      2) Avoid dropping failed pipelines: retry FAILED ops; adapt RAM upward on OOM.
      3) Soft reservation: keep some CPU/RAM headroom so batch doesn't consume everything,
         improving latency for late-arriving interactive/query work (without preemption).
      4) Basic packing: issue multiple assignments per tick, decrementing local pool headroom.

    Notes:
      - No preemption, because the minimal public interface does not expose a safe way to
        enumerate running containers to suspend.
      - RAM hints are tracked per-operator (best-effort keying), increasing on OOM failures.
    """
    from collections import deque

    s.queues = {
        Priority.QUERY: deque(),
        Priority.INTERACTIVE: deque(),
        Priority.BATCH_PIPELINE: deque(),
    }

    # Per-operator hints (best-effort op keying; see _op_key()).
    s.op_ram_hint = {}     # op_key -> required RAM hint (absolute units)
    s.op_cpu_hint = {}     # op_key -> previously used CPU (absolute units)
    s.op_failures = {}     # op_key -> failure count

    # Conservative soft-reservation used when batch is present alongside higher priorities.
    s.reserve_frac_cpu = 0.20
    s.reserve_frac_ram = 0.20

    # Retry policy
    s.max_oom_retries = 4
    s.max_other_retries = 1


@register_scheduler(key="scheduler_high_029")
def scheduler_high_029(s, results, pipelines):
    """See init docstring for policy overview."""
    # Local helpers are defined inside to avoid top-level imports.
    def _op_key(op):
        # Prefer stable IDs if present; otherwise fall back to Python object identity.
        for attr in ("op_id", "operator_id", "id"):
            if hasattr(op, attr):
                try:
                    v = getattr(op, attr)
                    # Skip bound methods or callables accidentally named "id"
                    if not callable(v):
                        return (attr, v)
                except Exception:
                    pass
        return ("pyid", id(op))

    def _is_oom(err):
        if err is None:
            return False
        try:
            msg = str(err).upper()
        except Exception:
            return False
        return ("OOM" in msg) or ("OUT OF MEMORY" in msg) or ("OUT_OF_MEMORY" in msg)

    def _has_high_backlog():
        # Best-effort: queues may contain completed pipelines, but those are pruned during scan.
        return (len(s.queues[Priority.QUERY]) > 0) or (len(s.queues[Priority.INTERACTIVE]) > 0)

    def _cpu_target_frac(priority, high_backlog):
        if priority == Priority.QUERY:
            return 0.75
        if priority == Priority.INTERACTIVE:
            return 0.60
        # Batch: smaller slices when high-priority backlog exists; larger slices otherwise.
        return 0.30 if high_backlog else 0.60

    def _ram_target_frac(priority, high_backlog):
        if priority == Priority.QUERY:
            return 0.50
        if priority == Priority.INTERACTIVE:
            return 0.40
        return 0.25 if high_backlog else 0.40

    def _ram_headroom_mul(priority):
        # Slightly over-allocate RAM for high priority to reduce OOM-induced tail latency.
        if priority == Priority.QUERY:
            return 1.15
        if priority == Priority.INTERACTIVE:
            return 1.10
        return 1.00

    # Enqueue new pipelines by priority.
    for p in pipelines:
        s.queues[p.priority].append(p)

    # If nothing changed, decisions won't change (discrete-event simulator).
    if not pipelines and not results:
        return [], []

    # Update hints from execution results (especially OOM-driven RAM adaptation).
    for r in results:
        try:
            pool = s.executor.pools[r.pool_id]
        except Exception:
            pool = None

        # Results may contain multiple ops (typically one).
        for op in getattr(r, "ops", []) or []:
            k = _op_key(op)

            # Record last known CPU; can be used as a minimum target next time.
            try:
                if getattr(r, "cpu", None) is not None:
                    s.op_cpu_hint[k] = r.cpu
            except Exception:
                pass

            if hasattr(r, "failed") and r.failed():
                s.op_failures[k] = s.op_failures.get(k, 0) + 1

                if _is_oom(getattr(r, "error", None)):
                    # Increase RAM hint aggressively on OOM to converge quickly.
                    prev = s.op_ram_hint.get(k, None)
                    base = None
                    try:
                        base = float(getattr(r, "ram", 0) or 0)
                    except Exception:
                        base = 0.0
                    if prev is None:
                        prev = base

                    # Double, capped by pool max RAM if available.
                    new_hint = max(prev, base) * 2.0
                    if pool is not None:
                        try:
                            new_hint = min(new_hint, float(pool.max_ram_pool))
                        except Exception:
                            pass
                    s.op_ram_hint[k] = new_hint
            else:
                # On success, keep a conservative hint: what worked last time.
                try:
                    used_ram = getattr(r, "ram", None)
                    if used_ram is not None:
                        prev = s.op_ram_hint.get(k, None)
                        if prev is None:
                            s.op_ram_hint[k] = used_ram
                        else:
                            # Never increase on success; can slowly decrease if we were over-hinting.
                            s.op_ram_hint[k] = min(prev, used_ram)
                except Exception:
                    pass

    # Snapshot pool headroom and maintain local decremented headroom as we add assignments.
    local_avail_cpu = []
    local_avail_ram = []
    max_cpu = []
    max_ram = []
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        local_avail_cpu.append(pool.avail_cpu_pool)
        local_avail_ram.append(pool.avail_ram_pool)
        max_cpu.append(pool.max_cpu_pool)
        max_ram.append(pool.max_ram_pool)

    high_backlog = _has_high_backlog()
    reserved_cpu = [mc * s.reserve_frac_cpu for mc in max_cpu]
    reserved_ram = [mr * s.reserve_frac_ram for mr in max_ram]

    def _pool_choice_for(op, priority):
        """Pick a pool that can fit this op given current local headroom.

        Returns:
          (pool_id, cpu_alloc, ram_alloc) or (None, None, None)
        """
        k = _op_key(op)

        best = None  # (score, pool_id, cpu_alloc, ram_alloc)
        for pool_id in range(s.executor.num_pools):
            a_cpu = local_avail_cpu[pool_id]
            a_ram = local_avail_ram[pool_id]
            if a_cpu <= 0 or a_ram <= 0:
                continue

            # Minimum "unit" CPU; if the platform allows fractional CPU, this is safe too.
            min_cpu_unit = min(1.0, float(max_cpu[pool_id])) if float(max_cpu[pool_id]) > 0 else 0.0
            if a_cpu < min_cpu_unit or min_cpu_unit <= 0:
                continue

            # CPU target (absolute)
            cpu_target = float(max_cpu[pool_id]) * _cpu_target_frac(priority, high_backlog)
            cpu_hint = s.op_cpu_hint.get(k, None)
            if cpu_hint is not None:
                try:
                    cpu_target = max(cpu_target, float(cpu_hint))
                except Exception:
                    pass
            cpu_target = max(cpu_target, min_cpu_unit)
            cpu_alloc = min(a_cpu, cpu_target)

            # RAM requirement/target (absolute)
            ram_hint = s.op_ram_hint.get(k, None)
            if ram_hint is None:
                ram_req = float(max_ram[pool_id]) * _ram_target_frac(priority, high_backlog)
            else:
                ram_req = float(ram_hint)

            ram_req = min(ram_req * _ram_headroom_mul(priority), float(max_ram[pool_id]))
            if a_ram < ram_req:
                continue
            ram_alloc = ram_req  # allocate what we believe we need (+ headroom)

            # Soft reservation: when high-priority backlog exists, don't let batch consume the last headroom.
            if priority == Priority.BATCH_PIPELINE and high_backlog:
                if (a_cpu - cpu_alloc) < reserved_cpu[pool_id]:
                    continue
                if (a_ram - ram_alloc) < reserved_ram[pool_id]:
                    continue

            # Scoring: high priority prefers more headroom; batch prefers tighter packing.
            rem_cpu = max(0.0, a_cpu - cpu_alloc)
            rem_ram = max(0.0, a_ram - ram_alloc)
            # Normalize to avoid RAM dominating score when units differ wildly.
            cpu_norm = rem_cpu / float(max_cpu[pool_id]) if float(max_cpu[pool_id]) > 0 else rem_cpu
            ram_norm = rem_ram / float(max_ram[pool_id]) if float(max_ram[pool_id]) > 0 else rem_ram

            if priority in (Priority.QUERY, Priority.INTERACTIVE):
                # Prefer leaving more remaining headroom after placement.
                score = cpu_norm + ram_norm
                candidate = (score, pool_id, cpu_alloc, ram_alloc)
                if best is None or candidate[0] > best[0]:
                    best = candidate
            else:
                # Prefer using up headroom (pack), but keep feasibility.
                score = -(cpu_norm + ram_norm)
                candidate = (score, pool_id, cpu_alloc, ram_alloc)
                if best is None or candidate[0] > best[0]:
                    best = candidate

        if best is None:
            return None, None, None
        _, pool_id, cpu_alloc, ram_alloc = best
        return pool_id, cpu_alloc, ram_alloc

    assignments = []
    suspensions = []

    # Prevent duplicate scheduling within the same tick (status won't reflect new assignments yet).
    assigned_pipelines_this_tick = set()
    assigned_ops_this_tick = set()

    def _can_retry(op):
        k = _op_key(op)
        fails = s.op_failures.get(k, 0)
        # If we've never observed failure, it's definitely retryable.
        if fails <= 0:
            return True
        # If we have an explicit RAM hint, assume OOM-related; allow more retries.
        if k in s.op_ram_hint:
            return fails <= s.max_oom_retries
        return fails <= s.max_other_retries

    # Greedy scheduling by priority class.
    for priority in (Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE):
        q = s.queues[priority]

        made_progress = True
        while made_progress:
            made_progress = False
            if len(q) == 0:
                break

            # Try up to one full rotation to find a schedulable pipeline/op for this priority.
            n = len(q)
            scheduled_idx = -1
            scheduled_item = None  # (pipeline, op, pool_id, cpu, ram)

            for i in range(n):
                pipeline = q.popleft()

                # Skip pipelines we've already scheduled this tick (avoid duplicates).
                if pipeline.pipeline_id in assigned_pipelines_this_tick:
                    q.append(pipeline)
                    continue

                status = pipeline.runtime_status()
                if status.is_pipeline_successful():
                    # Drop completed pipelines.
                    continue

                # Pick the first ready op (parents complete) that is retryable and not already assigned this tick.
                ops = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
                chosen_op = None
                for op in ops:
                    k = _op_key(op)
                    if k in assigned_ops_this_tick:
                        continue
                    if not _can_retry(op):
                        continue
                    chosen_op = op
                    break

                if chosen_op is None:
                    # No ready ops or all exceed retry policy; keep in queue (unless it's stuck forever).
                    q.append(pipeline)
                    continue

                # Find a pool for this op.
                pool_id, cpu_alloc, ram_alloc = _pool_choice_for(chosen_op, priority)
                if pool_id is None:
                    # Can't fit anywhere right now.
                    q.append(pipeline)
                    continue

                scheduled_idx = i
                scheduled_item = (pipeline, chosen_op, pool_id, cpu_alloc, ram_alloc)
                break

            if scheduled_item is None:
                # No schedulable work at this priority given current headroom.
                break

            pipeline, op, pool_id, cpu_alloc, ram_alloc = scheduled_item

            # Emit assignment and decrement local headroom.
            assignments.append(
                Assignment(
                    ops=[op],
                    cpu=cpu_alloc,
                    ram=ram_alloc,
                    priority=pipeline.priority,
                    pool_id=pool_id,
                    pipeline_id=pipeline.pipeline_id,
                )
            )

            local_avail_cpu[pool_id] = max(0.0, local_avail_cpu[pool_id] - cpu_alloc)
            local_avail_ram[pool_id] = max(0.0, local_avail_ram[pool_id] - ram_alloc)

            assigned_pipelines_this_tick.add(pipeline.pipeline_id)
            assigned_ops_this_tick.add(_op_key(op))

            # Round-robin: put pipeline at end to avoid monopolizing the queue.
            q.append(pipeline)
            made_progress = True

    return suspensions, assignments
