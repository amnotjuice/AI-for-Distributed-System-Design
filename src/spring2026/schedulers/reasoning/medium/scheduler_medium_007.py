# policy_key: scheduler_medium_007
# reasoning_effort: medium
# model: gpt-5.2-2025-12-11
# llm_cost: 0.067412
# generation_seconds: 96.87
# generated_at: 2026-03-12T22:01:53.741997
@register_scheduler_init(key="scheduler_medium_007")
def scheduler_medium_007_init(s):
    """Priority-aware FIFO with simple reservations + OOM-aware RAM retries.

    Improvements over naive FIFO:
      1) Separate queues by priority; always schedule higher priority first.
      2) Reserve a fraction of each pool for high-priority work when any high-priority
         pipeline is waiting, to protect tail latency from batch saturation.
      3) On OOM-like failures, retry the failed operator with increased RAM (exponential backoff),
         instead of dropping the whole pipeline immediately.
      4) Size CPU/RAM with conservative per-priority defaults and per-operator hints learned
         from previous executions (mainly RAM on OOM).

    Notes:
      - No preemption is attempted (API for enumerating running containers is not guaranteed).
      - Scheduling atom is one operator per assignment.
    """
    from collections import deque

    # Per-priority waiting queues (store pipeline_ids; actual objects in s.active)
    s.q = {
        Priority.QUERY: deque(),
        Priority.INTERACTIVE: deque(),
        Priority.BATCH_PIPELINE: deque(),
    }

    # Active pipelines by id
    s.active = {}  # pipeline_id -> Pipeline

    # Simple time for aging/debug (not used heavily)
    s.tick = 0
    s.enqueue_tick = {}  # pipeline_id -> tick

    # Per-operator resource hints (keyed by (pipeline_id, op_idish))
    s.op_ram_hint = {}  # op_key -> ram
    s.op_cpu_hint = {}  # op_key -> cpu

    # OOM retry bookkeeping
    s.oom_retryable_ops = set()  # op_key
    s.oom_retries = {}  # op_key -> count
    s.max_oom_retries = 3
    s.ram_backoff = 2.0

    # Pool reservation for high-priority latency protection
    s.reserve_frac_cpu = 0.25
    s.reserve_frac_ram = 0.25

    # Safety bounds / policy knobs
    s.max_assignments_per_pool = 8  # avoid long loops per tick
    s.min_cpu_alloc = 1  # assume vCPU is integer-like
    s.min_ram_alloc = 1  # assume RAM unit is integer-like


@register_scheduler(key="scheduler_medium_007")
def scheduler_medium_007(s, results: List["ExecutionResult"], pipelines: List["Pipeline"]) -> Tuple[List["Suspend"], List["Assignment"]]:
    """Main scheduling loop for scheduler_medium_007."""
    from collections import deque

    def _is_high(prio):
        return prio in (Priority.QUERY, Priority.INTERACTIVE)

    def _prio_order():
        # Strict priority: QUERY > INTERACTIVE > BATCH
        return (Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE)

    def _default_cpu(prio, pool):
        # Give higher priority more CPU to reduce latency, but avoid monopolizing the pool.
        max_cpu = getattr(pool, "max_cpu_pool", 1) or 1
        if prio == Priority.QUERY:
            return max(s.min_cpu_alloc, int(max_cpu * 0.50))
        if prio == Priority.INTERACTIVE:
            return max(s.min_cpu_alloc, int(max_cpu * 0.40))
        return max(s.min_cpu_alloc, int(max_cpu * 0.25))

    def _default_ram(prio, pool):
        # RAM is the most dangerous dimension (OOM). Defaults are more generous for high priority.
        max_ram = getattr(pool, "max_ram_pool", 1) or 1
        if prio == Priority.QUERY:
            return max(s.min_ram_alloc, int(max_ram * 0.50))
        if prio == Priority.INTERACTIVE:
            return max(s.min_ram_alloc, int(max_ram * 0.45))
        return max(s.min_ram_alloc, int(max_ram * 0.30))

    def _looks_like_oom(err):
        if not err:
            return False
        msg = str(err).lower()
        return ("oom" in msg) or ("out of memory" in msg) or ("memoryerror" in msg)

    def _op_stable_id(op):
        # Try common attribute names; fallback to repr (stable enough within a run).
        for name in ("op_id", "operator_id", "node_id", "id", "name"):
            if hasattr(op, name):
                try:
                    v = getattr(op, name)
                    if callable(v):
                        v = v()
                    if v is not None:
                        return str(v)
                except Exception:
                    pass
        try:
            return repr(op)
        except Exception:
            return str(type(op))

    def _op_key(pipeline_id, op):
        return (pipeline_id, _op_stable_id(op))

    def _queue_nonempty(prio):
        # Clean front-of-queue dead entries lazily
        q = s.q[prio]
        while q and q[0] not in s.active:
            q.popleft()
        return len(q) > 0

    def _any_high_waiting():
        return _queue_nonempty(Priority.QUERY) or _queue_nonempty(Priority.INTERACTIVE)

    def _pop_next_pipeline_id(prio):
        q = s.q[prio]
        while q:
            pid = q.popleft()
            if pid in s.active:
                return pid
        return None

    def _requeue_pipeline_id(prio, pid):
        if pid in s.active:
            s.q[prio].append(pid)

    def _pipeline_terminal_failure(pipeline):
        # If there are FAILED ops that are not marked retryable due to OOM, treat as terminal.
        st = pipeline.runtime_status()
        failed_ops = st.get_ops([OperatorState.FAILED], require_parents_complete=False) or []
        if not failed_ops:
            return False
        for op in failed_ops:
            ok = _op_key(pipeline.pipeline_id, op)
            if ok not in s.oom_retryable_ops:
                return True
        return False

    # Advance logical time
    s.tick += 1

    # Ingest new pipelines (dedupe by pipeline_id)
    for p in pipelines:
        pid = p.pipeline_id
        if pid not in s.active:
            s.active[pid] = p
            s.enqueue_tick[pid] = s.tick
            s.q[p.priority].append(pid)

    # Update per-op hints based on execution results (mainly OOM -> RAM backoff)
    for r in results:
        try:
            ops = getattr(r, "ops", None) or []
            # Best-effort: infer OOM and apply to each op in the result
            if r.failed() and _looks_like_oom(getattr(r, "error", None)):
                for op in ops:
                    # ExecutionResult may not carry pipeline_id; fall back to using op's own id only.
                    # If pipeline_id isn't available, we still store a hint keyed by a pseudo id.
                    pid = getattr(r, "pipeline_id", None)
                    if pid is None:
                        pid = "unknown_pipeline"
                    ok = _op_key(pid, op)

                    # Exponential backoff RAM hint
                    prev = s.op_ram_hint.get(ok, getattr(r, "ram", None))
                    if prev is None:
                        prev = 1
                    new_ram = max(s.min_ram_alloc, int(float(prev) * s.ram_backoff))
                    s.op_ram_hint[ok] = new_ram

                    s.oom_retryable_ops.add(ok)
                    s.oom_retries[ok] = s.oom_retries.get(ok, 0) + 1
                    if s.oom_retries[ok] > s.max_oom_retries:
                        # Stop retrying forever: remove retryability so pipeline can be dropped.
                        if ok in s.oom_retryable_ops:
                            s.oom_retryable_ops.remove(ok)
            else:
                # On success, we can record observed allocations as hints (very conservative).
                for op in ops:
                    pid = getattr(r, "pipeline_id", None)
                    if pid is None:
                        pid = "unknown_pipeline"
                    ok = _op_key(pid, op)
                    if getattr(r, "ram", None) is not None:
                        s.op_ram_hint.setdefault(ok, r.ram)
                    if getattr(r, "cpu", None) is not None:
                        s.op_cpu_hint.setdefault(ok, r.cpu)
        except Exception:
            # Never let hinting crash the scheduler.
            pass

    # Early exit if nothing to do
    if not pipelines and not results and not any(_queue_nonempty(pr) for pr in _prio_order()):
        return [], []

    suspensions: List["Suspend"] = []
    assignments: List["Assignment"] = []

    # Avoid scheduling multiple ops from the same pipeline in one tick across pools
    inflight_this_tick = set()

    # Schedule per pool, prioritizing high-priority work
    high_waiting = _any_high_waiting()

    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = getattr(pool, "avail_cpu_pool", 0) or 0
        avail_ram = getattr(pool, "avail_ram_pool", 0) or 0

        if avail_cpu < s.min_cpu_alloc or avail_ram < s.min_ram_alloc:
            continue

        reserve_cpu = 0
        reserve_ram = 0
        if high_waiting:
            max_cpu = getattr(pool, "max_cpu_pool", avail_cpu) or avail_cpu
            max_ram = getattr(pool, "max_ram_pool", avail_ram) or avail_ram
            reserve_cpu = int(max_cpu * s.reserve_frac_cpu)
            reserve_ram = int(max_ram * s.reserve_frac_ram)

        made = 0
        # Try multiple placements per pool per tick
        while made < s.max_assignments_per_pool:
            # If no resources left, stop
            if avail_cpu < s.min_cpu_alloc or avail_ram < s.min_ram_alloc:
                break

            scheduled_one = False

            for prio in _prio_order():
                # Determine effective available resources for this priority
                if prio == Priority.BATCH_PIPELINE and high_waiting:
                    eff_cpu = avail_cpu - reserve_cpu
                    eff_ram = avail_ram - reserve_ram
                else:
                    eff_cpu = avail_cpu
                    eff_ram = avail_ram

                if eff_cpu < s.min_cpu_alloc or eff_ram < s.min_ram_alloc:
                    continue

                pid = _pop_next_pipeline_id(prio)
                if pid is None:
                    continue

                pipeline = s.active.get(pid)
                if pipeline is None:
                    continue

                # Skip if we already scheduled this pipeline this tick
                if pid in inflight_this_tick:
                    _requeue_pipeline_id(prio, pid)
                    continue

                st = pipeline.runtime_status()

                # Drop successful pipelines
                if st.is_pipeline_successful():
                    s.active.pop(pid, None)
                    s.enqueue_tick.pop(pid, None)
                    continue

                # Drop terminal failures; keep retryable OOM-failures
                if _pipeline_terminal_failure(pipeline):
                    s.active.pop(pid, None)
                    s.enqueue_tick.pop(pid, None)
                    continue

                # Find one runnable op whose parents are complete
                op_list = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True) or []
                if not op_list:
                    # Blocked; requeue for later
                    _requeue_pipeline_id(prio, pid)
                    continue

                op = op_list[0]
                ok = _op_key(pid, op)

                # Determine allocation using hints or defaults
                cpu_hint = s.op_cpu_hint.get(ok, _default_cpu(prio, pool))
                ram_hint = s.op_ram_hint.get(ok, _default_ram(prio, pool))

                # Clamp to effective availability
                cpu_alloc = min(eff_cpu, cpu_hint)
                ram_alloc = min(eff_ram, ram_hint)

                # Enforce minimum allocs
                if cpu_alloc < s.min_cpu_alloc or ram_alloc < s.min_ram_alloc:
                    # Can't place this op now; requeue and try other pipelines/priorities
                    _requeue_pipeline_id(prio, pid)
                    continue

                assignments.append(
                    Assignment(
                        ops=[op],
                        cpu=cpu_alloc,
                        ram=ram_alloc,
                        priority=pipeline.priority,
                        pool_id=pool_id,
                        pipeline_id=pid,
                    )
                )

                # Update pool availability locally
                avail_cpu -= cpu_alloc
                avail_ram -= ram_alloc

                # Requeue pipeline (more ops may become runnable later)
                _requeue_pipeline_id(prio, pid)

                inflight_this_tick.add(pid)
                made += 1
                scheduled_one = True
                break  # re-evaluate priorities from top

            if not scheduled_one:
                # No eligible placements given current effective headroom and queues
                break

    return suspensions, assignments
