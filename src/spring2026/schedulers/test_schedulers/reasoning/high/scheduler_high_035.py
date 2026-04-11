# policy_key: scheduler_high_035
# reasoning_effort: high
# exp: reasoning
# model: gpt-5.2-2025-12-11
# llm_cost: 0.170671
# generation_seconds: 220.24
# generated_at: 2026-04-10T02:52:57.435256
@register_scheduler_init(key="scheduler_high_035")
def scheduler_high_035_init(s):
    """Priority-aware, failure-averse scheduler.

    Core ideas:
      - Strict priority for QUERY and INTERACTIVE to reduce weighted score.
      - Keep headroom for high-priority work (especially in single-pool setups).
      - Treat failures as likely under-provisioning: retry failed operators with increased RAM/CPU hints.
      - Avoid duplicate scheduling within the same tick by limiting to <=1 op per pipeline per scheduler call.
      - Never drop pipelines: keep retrying with capped exponential backoff (failures are heavily penalized).
    """
    # Priority queues hold pipeline_ids (not Pipeline objects) to avoid stale references.
    s.queues = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }
    s.pipeline_registry = {}          # pipeline_id -> Pipeline
    s.pipeline_in_queue = set()       # pipeline_id currently enqueued
    s.enqueue_step = {}              # pipeline_id -> first enqueue step (for aging)

    # Per-operator hints keyed by (pipeline_id, op_local_key) to reduce OOM/fail loops.
    s.op_ram_hint = {}               # op_global_key -> suggested RAM
    s.op_cpu_hint = {}               # op_global_key -> suggested CPU
    s.op_retry_count = {}            # op_global_key -> retries

    # Map operator object identity to pipeline_id for result attribution (best-effort).
    s.op_obj_to_pipeline = {}         # id(op_obj) -> pipeline_id

    # Scheduler time (tick counter).
    s.step_count = 0

    # Tuning knobs (conservative; small improvements over FIFO).
    s.reserve_frac_cpu_single_pool = 0.25
    s.reserve_frac_ram_single_pool = 0.25

    # Batch aging escape hatch for single-pool contention.
    s.batch_aging_steps = 250
    s.batch_escape_hatch_every = 25
    s._since_last_batch_escape = 0

    # Resource targeting. Keep it simple and bounded to avoid over-allocating on large pools.
    s.cpu_abs_cap = {
        Priority.QUERY: 16.0,
        Priority.INTERACTIVE: 8.0,
        Priority.BATCH_PIPELINE: 4.0,
    }
    s.cpu_target_frac = {
        Priority.QUERY: 0.60,
        Priority.INTERACTIVE: 0.45,
        Priority.BATCH_PIPELINE: 0.30,
    }
    s.cpu_base = {
        Priority.QUERY: 4.0,
        Priority.INTERACTIVE: 2.0,
        Priority.BATCH_PIPELINE: 1.0,
    }
    s.ram_target_frac = {
        Priority.QUERY: 0.50,
        Priority.INTERACTIVE: 0.35,
        Priority.BATCH_PIPELINE: 0.25,
    }

    # Retry backoff (bias to increase RAM more than CPU to prevent OOM loops).
    s.ram_retry_growth = 1.8
    s.cpu_retry_growth = 1.25
    s.retry_growth_cap_steps = 4


def _op_local_key(op):
    for attr in ("op_id", "operator_id", "uid", "name"):
        if hasattr(op, attr):
            try:
                v = getattr(op, attr)
                if v is not None:
                    return str(v)
            except Exception:
                pass
    return str(id(op))


def _op_global_key(pipeline_id, op):
    return f"{pipeline_id}:{_op_local_key(op)}"


def _is_oom_error(err):
    try:
        s = str(err).lower()
    except Exception:
        return False
    return ("oom" in s) or ("out of memory" in s) or ("out-of-memory" in s) or ("memoryerror" in s)


def _requeue(s, pipeline_id, priority, to_front=False):
    if pipeline_id in s.pipeline_in_queue:
        return
    if to_front:
        s.queues[priority].insert(0, pipeline_id)
    else:
        s.queues[priority].append(pipeline_id)
    s.pipeline_in_queue.add(pipeline_id)


def _promote_pipeline(s, pipeline_id):
    # Best-effort: move pipeline_id to the front of whichever queue currently contains it.
    if pipeline_id not in s.pipeline_in_queue:
        return
    for prio, q in s.queues.items():
        try:
            idx = q.index(pipeline_id)
        except ValueError:
            continue
        # Move to front.
        q.pop(idx)
        q.insert(0, pipeline_id)
        return


def _has_any_waiting(s, priorities):
    # Approximate check (queues may contain already-completed pipelines); kept simple.
    for pr in priorities:
        if s.queues.get(pr):
            return True
    return False


def _oldest_age(s, priority):
    q = s.queues.get(priority, [])
    if not q:
        return 0
    oldest = None
    for pid in q:
        st = s.enqueue_step.get(pid)
        if st is None:
            continue
        if oldest is None or st < oldest:
            oldest = st
    if oldest is None:
        return 0
    return max(0, s.step_count - oldest)


def _pool_prio_order(s, pool_id, high_waiting):
    num_pools = s.executor.num_pools
    if num_pools >= 2:
        # Pool 0 is "interactive-ish": keep it focused on high-priority when backlog exists.
        if pool_id == 0:
            if high_waiting:
                return [Priority.QUERY, Priority.INTERACTIVE]
            return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]

        # Other pools: allow spillover for high-priority, otherwise favor batch throughput.
        if high_waiting:
            return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]
        return [Priority.BATCH_PIPELINE, Priority.INTERACTIVE, Priority.QUERY]

    # Single pool: strict priority, but batch may run under reservation/aging escape hatch.
    return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]


def _pop_next_pid(s, priority, assigned_this_tick):
    q = s.queues[priority]
    if not q:
        return None

    # Bound scanning to avoid infinite loops.
    scan_budget = len(q)
    while q and scan_budget > 0:
        scan_budget -= 1
        pid = q.pop(0)
        s.pipeline_in_queue.discard(pid)

        if pid in assigned_this_tick:
            _requeue(s, pid, priority, to_front=False)
            continue

        p = s.pipeline_registry.get(pid)
        if p is None:
            continue

        try:
            st = p.runtime_status()
        except Exception:
            # If status is unavailable, put it back and try later.
            _requeue(s, pid, priority, to_front=False)
            continue

        if st.is_pipeline_successful():
            # Drop completed pipelines from queues (no requeue).
            continue

        return pid

    return None


def _size_for_op(s, pipeline_id, priority, op, pool, avail_cpu, avail_ram):
    max_cpu = float(pool.max_cpu_pool)
    max_ram = float(pool.max_ram_pool)

    gk = _op_global_key(pipeline_id, op)
    retries = int(s.op_retry_count.get(gk, 0))
    rstep = min(retries, int(s.retry_growth_cap_steps))

    # CPU target: fraction of pool, with absolute cap and a small base.
    cpu_target = max(s.cpu_base.get(priority, 1.0), max_cpu * s.cpu_target_frac.get(priority, 0.3))
    cpu_target = min(cpu_target, s.cpu_abs_cap.get(priority, cpu_target), max_cpu)

    # RAM target: conservative fraction of pool.
    ram_target = max_ram * s.ram_target_frac.get(priority, 0.25)
    ram_target = min(ram_target, max_ram)

    # Apply learned hints (never allocate below hint if possible).
    if gk in s.op_cpu_hint:
        try:
            cpu_target = max(cpu_target, float(s.op_cpu_hint[gk]))
        except Exception:
            pass
    if gk in s.op_ram_hint:
        try:
            ram_target = max(ram_target, float(s.op_ram_hint[gk]))
        except Exception:
            pass

    # Retry-based growth (favor RAM to avoid repeated OOM).
    cpu_target = min(max_cpu, cpu_target * (s.cpu_retry_growth ** rstep))
    ram_target = min(max_ram, ram_target * (s.ram_retry_growth ** rstep))

    # Minimum RAM gating: for high-priority, prefer waiting over likely-OOM execution.
    hint_ram = None
    if gk in s.op_ram_hint:
        try:
            hint_ram = float(s.op_ram_hint[gk])
        except Exception:
            hint_ram = None

    if hint_ram is not None:
        min_required_ram = min(max_ram, hint_ram)
    else:
        # If we don't have a hint yet, still avoid running too small for high priority.
        if priority == Priority.BATCH_PIPELINE:
            min_required_ram = 0.60 * ram_target
        else:
            min_required_ram = 0.80 * ram_target

    if avail_ram < max(0.0, min_required_ram):
        return 0.0, 0.0

    # CPU is never correctness-critical; allow smaller allocations if needed.
    if avail_cpu <= 0.0:
        return 0.0, 0.0

    cpu = min(avail_cpu, cpu_target)
    ram = min(avail_ram, ram_target)

    # Avoid returning zeros due to numeric edge cases.
    if cpu <= 0.0 or ram <= 0.0:
        return 0.0, 0.0

    return cpu, ram


@register_scheduler(key="scheduler_high_035")
def scheduler_high_035(s, results: List["ExecutionResult"], pipelines: List["Pipeline"]) -> Tuple[List["Suspend"], List["Assignment"]]:
    assignable_states = globals().get("ASSIGNABLE_STATES", [OperatorState.PENDING, OperatorState.FAILED])

    s.step_count += 1
    suspensions: List[Suspend] = []
    assignments: List[Assignment] = []

    # Ingest new pipelines.
    for p in pipelines:
        s.pipeline_registry[p.pipeline_id] = p
        if p.pipeline_id not in s.enqueue_step:
            s.enqueue_step[p.pipeline_id] = s.step_count
        if p.pipeline_id not in s.pipeline_in_queue:
            _requeue(s, p.pipeline_id, p.priority, to_front=False)

    # Learn from results: increase RAM/CPU hints on failure, and promote failed pipelines.
    for r in results:
        try:
            failed = r.failed()
        except Exception:
            failed = False

        ops = getattr(r, "ops", None) or []
        for op in ops:
            # Best-effort pipeline attribution.
            pid = None
            try:
                pid = s.op_obj_to_pipeline.get(id(op))
            except Exception:
                pid = None
            if pid is None and hasattr(op, "pipeline_id"):
                try:
                    pid = getattr(op, "pipeline_id")
                except Exception:
                    pid = None

            # Fall back to "unknown" bucket if we cannot attribute.
            pid_key = pid if pid is not None else "unknown"
            gk = f"{pid_key}:{_op_local_key(op)}"

            if failed:
                s.op_retry_count[gk] = int(s.op_retry_count.get(gk, 0)) + 1

                err = getattr(r, "error", None)
                is_oom = _is_oom_error(err)

                # Update RAM hint aggressively on OOM; otherwise modestly grow both.
                try:
                    alloc_ram = float(getattr(r, "ram", 0.0) or 0.0)
                except Exception:
                    alloc_ram = 0.0
                try:
                    alloc_cpu = float(getattr(r, "cpu", 0.0) or 0.0)
                except Exception:
                    alloc_cpu = 0.0

                if is_oom:
                    prev = float(s.op_ram_hint.get(gk, alloc_ram or 0.0) or 0.0)
                    base = max(prev, alloc_ram, 1e-6)
                    s.op_ram_hint[gk] = base * 2.0
                else:
                    if alloc_ram > 0:
                        prev = float(s.op_ram_hint.get(gk, alloc_ram) or alloc_ram)
                        s.op_ram_hint[gk] = prev * 1.5
                    if alloc_cpu > 0:
                        prev = float(s.op_cpu_hint.get(gk, alloc_cpu) or alloc_cpu)
                        s.op_cpu_hint[gk] = prev * 1.25

                # Promote pipeline to reduce end-to-end latency after a failure.
                if pid is not None:
                    _promote_pipeline(s, pid)
            else:
                # On success, keep "safe" hints (do not reduce aggressively).
                try:
                    alloc_ram = float(getattr(r, "ram", 0.0) or 0.0)
                except Exception:
                    alloc_ram = 0.0
                try:
                    alloc_cpu = float(getattr(r, "cpu", 0.0) or 0.0)
                except Exception:
                    alloc_cpu = 0.0

                if alloc_ram > 0 and gk not in s.op_ram_hint:
                    s.op_ram_hint[gk] = alloc_ram
                if alloc_cpu > 0 and gk not in s.op_cpu_hint:
                    s.op_cpu_hint[gk] = alloc_cpu

    # Single-tick guard: avoid scheduling multiple ops for the same pipeline per call,
    # because runtime_status() won't reflect assignments made in this same scheduler step.
    assigned_this_tick = set()

    # Backlog and batch aging signals.
    high_waiting = _has_any_waiting(s, [Priority.QUERY, Priority.INTERACTIVE])
    oldest_batch_age = _oldest_age(s, Priority.BATCH_PIPELINE)
    allow_batch_escape = False
    if s.executor.num_pools == 1 and high_waiting and oldest_batch_age >= int(s.batch_aging_steps):
        s._since_last_batch_escape += 1
        if s._since_last_batch_escape >= int(s.batch_escape_hatch_every):
            allow_batch_escape = True
            s._since_last_batch_escape = 0
    elif s.executor.num_pools == 1 and not high_waiting:
        s._since_last_batch_escape = 0

    # Schedule across pools.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        try:
            avail_cpu = float(pool.avail_cpu_pool)
            avail_ram = float(pool.avail_ram_pool)
        except Exception:
            continue

        if avail_cpu <= 0.0 or avail_ram <= 0.0:
            continue

        # Keep the policy conservative: limited number of assignments per pool per tick.
        max_assignments = 2 if (s.executor.num_pools >= 2 and pool_id == 0) else 3
        made = 0

        while made < max_assignments and avail_cpu > 0.0 and avail_ram > 0.0:
            prio_order = _pool_prio_order(s, pool_id, high_waiting)

            scheduled_any = False
            for pr in prio_order:
                # Single-pool headroom reservation: keep resources for high-priority.
                if s.executor.num_pools == 1 and pr == Priority.BATCH_PIPELINE and high_waiting and not allow_batch_escape:
                    reserve_cpu = float(pool.max_cpu_pool) * float(s.reserve_frac_cpu_single_pool)
                    reserve_ram = float(pool.max_ram_pool) * float(s.reserve_frac_ram_single_pool)
                    if avail_cpu <= reserve_cpu or avail_ram <= reserve_ram:
                        continue

                pid = _pop_next_pid(s, pr, assigned_this_tick)
                if pid is None:
                    continue

                p = s.pipeline_registry.get(pid)
                if p is None:
                    continue

                # Identify a runnable operator.
                try:
                    st = p.runtime_status()
                    op_list = st.get_ops(assignable_states, require_parents_complete=True)[:1]
                except Exception:
                    # Put it back and move on.
                    _requeue(s, pid, pr, to_front=False)
                    continue

                if not op_list:
                    # Not ready (likely blocked on running parents); requeue and try others.
                    _requeue(s, pid, pr, to_front=False)
                    continue

                op = op_list[0]

                cpu, ram = _size_for_op(s, pid, p.priority, op, pool, avail_cpu, avail_ram)
                if cpu <= 0.0 or ram <= 0.0:
                    # Can't fit right now; put it back near the front to retry soon.
                    _requeue(s, pid, pr, to_front=True)
                    # If we can't fit a high-priority op in this pool, don't waste time looping.
                    if p.priority in (Priority.QUERY, Priority.INTERACTIVE):
                        made = max_assignments
                    scheduled_any = True
                    break

                assignments.append(
                    Assignment(
                        ops=op_list,
                        cpu=cpu,
                        ram=ram,
                        priority=p.priority,
                        pool_id=pool_id,
                        pipeline_id=pid,
                    )
                )

                # Result attribution helper.
                try:
                    s.op_obj_to_pipeline[id(op)] = pid
                except Exception:
                    pass

                # Update local availability for additional packing.
                avail_cpu -= cpu
                avail_ram -= ram

                assigned_this_tick.add(pid)

                # Requeue pipeline for later steps (it likely has more operators).
                _requeue(s, pid, pr, to_front=False)

                made += 1
                scheduled_any = True
                break

            if not scheduled_any:
                break

    return suspensions, assignments
