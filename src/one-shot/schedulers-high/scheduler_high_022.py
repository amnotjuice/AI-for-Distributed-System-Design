# policy_key: scheduler_high_022
# reasoning_effort: high
# model: gpt-5.2-2025-12-11
# llm_cost: 0.080121
# generation_seconds: 94.19
# generated_at: 2026-03-14T03:29:00.675523
@register_scheduler_init(key="scheduler_high_022")
def scheduler_high_022_init(s):
    """Priority-aware FIFO with simple headroom reservation + OOM-aware RAM hinting.

    Improvements over naive FIFO:
      1) Separate waiting queues per priority; always schedule higher priority first.
      2) Reserve a small amount of free CPU/RAM for high-priority arrivals; avoid filling
         the pool with batch work when interactive/query work is pending.
      3) Avoid "grab all resources for one op" by using per-priority sizing targets,
         increasing concurrency and reducing tail latency.
      4) If an op fails with OOM, retry it with higher RAM next time (per-op RAM hints).
    """
    # Per-priority round-robin queues of pipelines
    s.waiting_queues = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }

    # Per-(pipeline, op) resource hints learned from past executions
    # Keys are (pipeline_id, op_key)
    s.op_ram_hint = {}
    s.op_cpu_hint = {}

    # Failure tracking to avoid infinite retry loops
    s.op_failures = {}       # (pipeline_id, op_key) -> int
    s.pipeline_failures = {} # pipeline_id -> int

    # Tunables (kept intentionally simple)
    s.max_op_retries = 2
    s.max_pipeline_failures = 6

    # Reservation fraction per pool when any high-priority work is waiting.
    # (We reserve headroom for query/interactive; batch should not consume all.)
    s.reserve_frac_cpu = 0.25
    s.reserve_frac_ram = 0.25

    # Baseline sizing fractions of pool capacity for each priority.
    # (Smaller than "all resources" to increase parallelism and reduce HOL blocking.)
    s.base_cpu_frac = {
        Priority.QUERY: 0.50,
        Priority.INTERACTIVE: 0.50,
        Priority.BATCH_PIPELINE: 0.25,
    }
    s.base_ram_frac = {
        Priority.QUERY: 0.60,
        Priority.INTERACTIVE: 0.60,
        Priority.BATCH_PIPELINE: 0.35,
    }


def _op_key(op):
    """Best-effort stable key for an operator object across scheduler ticks."""
    # Try common names first, fall back to Python identity.
    for attr in ("op_id", "operator_id", "id", "name"):
        if hasattr(op, attr):
            try:
                v = getattr(op, attr)
                # Avoid storing huge structures as keys
                if isinstance(v, (int, str)):
                    return v
            except Exception:
                pass
    return id(op)


def _is_oom_error(err):
    """Heuristic: treat error strings containing 'oom' as out-of-memory."""
    if err is None:
        return False
    try:
        return "oom" in str(err).lower() or "out of memory" in str(err).lower()
    except Exception:
        return False


def _priority_order():
    # Highest to lowest
    return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]


def _has_waiting_high_pri(s):
    return bool(s.waiting_queues[Priority.QUERY]) or bool(s.waiting_queues[Priority.INTERACTIVE])


def _pool_reservation(s, pool, reserve_for_high_pri):
    """Compute how much free capacity we keep unallocated for high-priority arrivals."""
    if not reserve_for_high_pri:
        return 0, 0
    # Keep it as integers if pool capacities are ints; otherwise, numeric is fine.
    reserve_cpu = pool.max_cpu_pool * s.reserve_frac_cpu
    reserve_ram = pool.max_ram_pool * s.reserve_frac_ram
    # Avoid reserving all capacity in small pools
    if reserve_cpu >= pool.max_cpu_pool:
        reserve_cpu = max(0, pool.max_cpu_pool - 1)
    if reserve_ram >= pool.max_ram_pool:
        reserve_ram = max(0, pool.max_ram_pool - 1)
    return reserve_cpu, reserve_ram


def _target_alloc_for(s, pool, priority, avail_cpu, avail_ram, ram_hint=None, cpu_hint=None):
    """Choose CPU/RAM allocation for one op under a priority class."""
    # Baseline targets based on pool size (not current availability), then cap to availability.
    base_cpu = pool.max_cpu_pool * s.base_cpu_frac.get(priority, 0.25)
    base_ram = pool.max_ram_pool * s.base_ram_frac.get(priority, 0.35)

    # Ensure we allocate at least 1 unit when possible (common in discrete CPU/RAM models).
    # If the simulator uses fractional resources, this still behaves reasonably.
    cpu = base_cpu
    ram = base_ram

    if cpu_hint is not None:
        cpu = max(cpu, cpu_hint)
    if ram_hint is not None:
        ram = max(ram, ram_hint)

    # Cap to available resources
    if avail_cpu < cpu:
        cpu = avail_cpu
    if avail_ram < ram:
        ram = avail_ram

    # Avoid zero allocations
    if cpu <= 0 or ram <= 0:
        return 0, 0

    # If resources are likely integer-sized, enforce minimum 1 if we have at least 1 available.
    if cpu < 1 and avail_cpu >= 1:
        cpu = 1
    if ram < 1 and avail_ram >= 1:
        ram = 1

    # Final cap after min bump
    cpu = min(cpu, avail_cpu)
    ram = min(ram, avail_ram)
    return cpu, ram


def _pick_next_pipeline_with_ready_op(s, priority):
    """Round-robin: find a pipeline in this priority queue with a ready op."""
    q = s.waiting_queues[priority]
    if not q:
        return None, None

    # At most one full rotation to avoid infinite loops
    n = len(q)
    for _ in range(n):
        p = q.pop(0)

        status = p.runtime_status()

        # Drop completed pipelines
        if status.is_pipeline_successful():
            continue

        # Drop pipelines that exceeded failure budget
        pf = s.pipeline_failures.get(p.pipeline_id, 0)
        if pf >= s.max_pipeline_failures:
            continue

        # Find one ready-to-assign operator (including FAILED, to allow retry)
        ops = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
        if ops:
            # Requeue the pipeline for future ops after we schedule one
            q.append(p)
            return p, ops[0]

        # No op ready yet; keep it around
        q.append(p)

    return None, None


@register_scheduler(key="scheduler_high_022")
def scheduler_high_022(s, results: List[ExecutionResult], pipelines: List[Pipeline]) -> Tuple[List[Suspend], List[Assignment]]:
    """
    Priority-aware scheduler:
      - Enqueue new pipelines by priority.
      - Learn from results (OOM -> increase RAM hint; repeated failures -> eventually drop).
      - For each pool, schedule as many ops as possible:
          * Always prefer QUERY > INTERACTIVE > BATCH.
          * If any high-priority work exists, keep some pool headroom by not scheduling batch
            when free resources are below reservation.
          * Allocate moderate per-op CPU/RAM (per-priority baseline), rather than all available.
    """
    # Enqueue new pipelines by priority
    for p in pipelines:
        pr = p.priority
        if pr not in s.waiting_queues:
            # Defensive: unknown priority -> treat as batch
            pr = Priority.BATCH_PIPELINE
        s.waiting_queues[pr].append(p)

    # Update hints/failure counters from execution results
    for r in results:
        # Results may include multiple ops; treat each independently for hints
        try:
            ops = list(r.ops) if r.ops is not None else []
        except Exception:
            ops = []

        for op in ops:
            k = (getattr(r, "pipeline_id", None), _op_key(op))
            # If pipeline_id isn't present on the result object, fall back to None-keying
            # (still helps within a single pipeline execution if consistent).
            if k[0] is None:
                k = (None, _op_key(op))

            if r.failed():
                # Count failures
                s.op_failures[k] = s.op_failures.get(k, 0) + 1

                # Also count per-pipeline failures if we can
                if getattr(r, "pipeline_id", None) is not None:
                    pid = r.pipeline_id
                    s.pipeline_failures[pid] = s.pipeline_failures.get(pid, 0) + 1

                # If OOM, increase RAM hint aggressively
                if _is_oom_error(getattr(r, "error", None)):
                    prev = s.op_ram_hint.get(k, 0)
                    # If r.ram is available, double it; else bump by a small constant.
                    try:
                        new_hint = max(prev, (r.ram * 2) if r.ram is not None else (prev + 1))
                    except Exception:
                        new_hint = prev + 1
                    s.op_ram_hint[k] = new_hint

                    # Optionally also bump CPU slightly on repeated OOMs (helps if OOM is time-related)
                    if s.op_failures.get(k, 0) >= 2:
                        prevc = s.op_cpu_hint.get(k, 0)
                        try:
                            s.op_cpu_hint[k] = max(prevc, (r.cpu + 1) if r.cpu is not None else (prevc + 1))
                        except Exception:
                            s.op_cpu_hint[k] = prevc + 1
            else:
                # On success, keep hints at least as large as what worked (monotonic, safe).
                # Note: we do NOT decrease hints because we don't know the true minimum.
                prev = s.op_ram_hint.get(k, 0)
                try:
                    if r.ram is not None:
                        s.op_ram_hint[k] = max(prev, r.ram)
                except Exception:
                    pass

                prevc = s.op_cpu_hint.get(k, 0)
                try:
                    if r.cpu is not None:
                        s.op_cpu_hint[k] = max(prevc, r.cpu)
                except Exception:
                    pass

    # Early exit when nothing changes that affects decisions
    if not pipelines and not results:
        # Still schedule if queues already contain work; do not early-exit in that case.
        if not _has_waiting_high_pri(s) and not s.waiting_queues[Priority.BATCH_PIPELINE]:
            return [], []

    suspensions: List[Suspend] = []
    assignments: List[Assignment] = []

    reserve_for_high = _has_waiting_high_pri(s)

    # For each pool, schedule multiple ops if resources allow
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool

        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        reserve_cpu, reserve_ram = _pool_reservation(s, pool, reserve_for_high)

        # Try to fill the pool, but keep reservation when high-priority work exists
        # Loop guard to avoid pathological infinite loops
        loop_guard = 0
        while avail_cpu > 0 and avail_ram > 0:
            loop_guard += 1
            if loop_guard > 128:
                break

            # Choose next priority to schedule:
            # Always try higher priorities first.
            chosen_p = None
            chosen_op = None
            chosen_pr = None

            for pr in _priority_order():
                # If batch, enforce reservation when high-priority is waiting
                if pr == Priority.BATCH_PIPELINE and reserve_for_high:
                    if avail_cpu <= reserve_cpu or avail_ram <= reserve_ram:
                        continue

                p, op = _pick_next_pipeline_with_ready_op(s, pr)
                if p is None or op is None:
                    continue

                # Enforce retry budget: if an op is failing repeatedly, eventually stop retrying it.
                k = (p.pipeline_id, _op_key(op))
                if s.op_failures.get(k, 0) > s.max_op_retries:
                    # Count towards pipeline failures and skip this op for now by not scheduling it.
                    s.pipeline_failures[p.pipeline_id] = s.pipeline_failures.get(p.pipeline_id, 0) + 1
                    continue

                chosen_p, chosen_op, chosen_pr = p, op, pr
                break

            if chosen_p is None:
                break

            # Fetch learned hints if any
            opk = (chosen_p.pipeline_id, _op_key(chosen_op))
            ram_hint = s.op_ram_hint.get(opk)
            cpu_hint = s.op_cpu_hint.get(opk)

            cpu, ram = _target_alloc_for(
                s=s,
                pool=pool,
                priority=chosen_pr,
                avail_cpu=avail_cpu,
                avail_ram=avail_ram,
                ram_hint=ram_hint,
                cpu_hint=cpu_hint,
            )

            if cpu <= 0 or ram <= 0:
                break

            # Create an assignment for exactly one op (keeps decisions simple and latency-friendly)
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

            # Account for allocated resources locally
            try:
                avail_cpu -= cpu
            except Exception:
                avail_cpu = 0
            try:
                avail_ram -= ram
            except Exception:
                avail_ram = 0

            # If we reserved headroom, stop scheduling batch once we dip near reservation.
            # (Interactive/query can still consume remaining.)
            if reserve_for_high and (avail_cpu <= reserve_cpu or avail_ram <= reserve_ram):
                # Only stop early if no more high-priority ops are available right now in queues
                # (otherwise continue, as they are latency critical).
                hi_available = False
                for pr in (Priority.QUERY, Priority.INTERACTIVE):
                    p2, op2 = _pick_next_pipeline_with_ready_op(s, pr)
                    if p2 is not None and op2 is not None:
                        hi_available = True
                        break
                if not hi_available:
                    break

    return suspensions, assignments
