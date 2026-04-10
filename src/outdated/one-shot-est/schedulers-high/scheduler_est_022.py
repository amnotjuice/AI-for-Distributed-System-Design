# policy_key: scheduler_est_022
# reasoning_effort: high
# model: gpt-5.2-2025-12-11
# llm_cost: 0.103242
# generation_seconds: 145.72
# generated_at: 2026-03-31T19:02:56.881287
@register_scheduler_init(key="scheduler_est_022")
def scheduler_est_022_init(s):
    """Priority-aware, estimate-guided scheduler (incremental improvement over naive FIFO).

    Key improvements (kept intentionally simple/robust):
      1) Priority ordering: always prefer QUERY/INTERACTIVE over BATCH.
      2) Right-sizing: do not hand an entire pool to a single op; size CPU/RAM per-op.
      3) Soft reservation: keep headroom in pool 0 for high-priority work when it exists.
      4) OOM-aware retries: if an op failed with an OOM-like error, retry with higher RAM.
    """
    # Track active pipelines by id to avoid duplicates and keep stable arrival ordering.
    s.pipeline_by_id = {}
    s.pipeline_arrival_seq = {}
    s._arrival_seq_counter = 0

    # Per-operator adaptive memory factor (bumped on OOM).
    s.op_mem_factor = {}
    s.op_fail_count = {}
    s.op_last_fail_is_oom = {}
    s.op_last_error = {}

    # Tunables (kept modest; simulator-friendly).
    s.max_retries_per_op = 3
    s.max_mem_factor = 6.0

    # Safety and sizing knobs.
    s.mem_safety = 1.25          # multiplicative safety over estimate
    s.mem_additive_gb = 0.10     # small overhead for runtime/Arrow/fragmentation
    s.min_ram_gb = 0.25          # avoid tiny allocations
    s.cpu_min = 1.0

    # Soft reservation to protect tail latency for high priority.
    s.reserve_frac_cpu_pool0 = 0.20
    s.reserve_frac_ram_pool0 = 0.20

    # Absolute CPU caps by priority to prevent a single op from monopolizing a pool.
    # (Also improves concurrency for multiple interactive queries.)
    s.abs_cpu_cap = {
        Priority.QUERY: 8.0,
        Priority.INTERACTIVE: 4.0,
        Priority.BATCH_PIPELINE: 2.0,
    }

    # Fractional CPU caps relative to pool size (a second guardrail).
    s.frac_cpu_cap = {
        Priority.QUERY: 0.75,
        Priority.INTERACTIVE: 0.50,
        Priority.BATCH_PIPELINE: 0.25,
    }


def _priority_rank(pri):
    # Lower is "more important"
    if pri == Priority.QUERY:
        return 0
    if pri == Priority.INTERACTIVE:
        return 1
    return 2  # Priority.BATCH_PIPELINE (and any unknowns)


def _op_key(op):
    # Prefer stable ids if present; fallback to Python object identity.
    for attr in ("op_id", "operator_id", "id", "name"):
        if hasattr(op, attr):
            v = getattr(op, attr)
            if v is not None:
                return (attr, v)
    return ("py_id", id(op))


def _is_oom_error(err):
    if err is None:
        return False
    s = str(err).lower()
    return ("oom" in s) or ("out of memory" in s) or ("memory" in s and "alloc" in s)


def _get_estimated_mem_gb(op, default_gb=1.0):
    try:
        est = getattr(getattr(op, "estimate", None), "mem_peak_gb", None)
        if est is None:
            return float(default_gb)
        est = float(est)
        if est <= 0:
            return float(default_gb)
        return est
    except Exception:
        return float(default_gb)


def _round_up(x, quantum):
    # Avoid importing math; do a small stable rounding-up.
    if quantum <= 0:
        return x
    q = float(quantum)
    v = float(x)
    n = int(v / q)
    if n * q < v:
        n += 1
    return n * q


def _ram_request_gb(s, op, pool, priority):
    # Estimate-guided RAM request with safety, additive overhead, and OOM-based multiplier.
    k = _op_key(op)
    factor = float(s.op_mem_factor.get(k, 1.0))

    # If estimate is missing, be slightly more conservative for batch (often heavier ops).
    default_est = 1.0 if priority in (Priority.QUERY, Priority.INTERACTIVE) else 2.0
    est = _get_estimated_mem_gb(op, default_gb=default_est)

    ram = est * s.mem_safety * factor + s.mem_additive_gb
    ram = max(ram, s.min_ram_gb)

    # Round to 0.25GB quanta to reduce thrash and make decisions more stable.
    ram = _round_up(ram, 0.25)

    # Clamp to pool max (if we can't fit, we just won't schedule it).
    try:
        ram = min(float(pool.max_ram_pool), float(ram))
    except Exception:
        pass
    return float(ram)


def _cpu_request(s, pool, priority, avail_cpu):
    # CPU request with both absolute and fractional caps.
    try:
        max_cpu = float(pool.max_cpu_pool)
    except Exception:
        max_cpu = float(avail_cpu)

    frac_cap = s.frac_cpu_cap.get(priority, 0.25)
    abs_cap = s.abs_cpu_cap.get(priority, 2.0)

    cap = min(abs_cap, max(s.cpu_min, max_cpu * float(frac_cap)))
    cpu = min(float(avail_cpu), float(cap))
    cpu = max(float(s.cpu_min), float(cpu))
    return float(cpu)


def _retry_allowed(s, op):
    k = _op_key(op)
    fails = int(s.op_fail_count.get(k, 0))
    if fails <= 0:
        return False
    if not bool(s.op_last_fail_is_oom.get(k, False)):
        return False
    return fails <= int(s.max_retries_per_op)


def _pipeline_ready_op(s, pipeline):
    # Prefer PENDING ops first; if none, allow FAILED ops only if retryable.
    status = pipeline.runtime_status()

    pending = status.get_ops([OperatorState.PENDING], require_parents_complete=True)
    if pending:
        return pending[0]

    failed = status.get_ops([OperatorState.FAILED], require_parents_complete=True)
    for op in failed:
        if _retry_allowed(s, op):
            return op

    return None


def _has_any_ready_high_priority_work(s, pipelines):
    for p in pipelines:
        if p.priority not in (Priority.QUERY, Priority.INTERACTIVE):
            continue
        if p.runtime_status().is_pipeline_successful():
            continue
        if _pipeline_ready_op(s, p) is not None:
            return True
    return False


def _drop_if_unretryable_failures(s, pipeline):
    # If pipeline has FAILED ops that are not retryable (or retried too many times), drop it.
    status = pipeline.runtime_status()
    failed_ops = status.get_ops([OperatorState.FAILED], require_parents_complete=False)
    if not failed_ops:
        return False

    for op in failed_ops:
        k = _op_key(op)
        fails = int(s.op_fail_count.get(k, 0))
        is_oom = bool(s.op_last_fail_is_oom.get(k, False))

        # If we don't have a recorded failure type, assume unretryable (conservative).
        if k not in s.op_last_fail_is_oom:
            return True

        # Non-OOM failures: treat as unretryable (keeps policy simple and avoids infinite loops).
        if not is_oom:
            return True

        # OOM failures beyond retry cap: drop.
        if fails > int(s.max_retries_per_op):
            return True

    return False


@register_scheduler(key="scheduler_est_022")
def scheduler_est_022(s, results, pipelines):
    """
    Priority-aware, right-sized scheduling loop with OOM-aware retries.

    Scheduling strategy:
      - Maintain active pipelines with stable FIFO order within each priority.
      - For each pool:
          * Prefer high-priority ops in pool 0 when any exist.
          * Otherwise, fill capacity with best available ops, sized per-op.
          * Apply soft reservation in pool 0 so batch doesn't crowd out interactive/query.
      - No preemption (kept robust given limited exposed executor state).
    """
    # Incorporate new pipelines (stable arrival order).
    for p in pipelines:
        pid = p.pipeline_id
        if pid not in s.pipeline_by_id:
            s.pipeline_by_id[pid] = p
            s.pipeline_arrival_seq[pid] = s._arrival_seq_counter
            s._arrival_seq_counter += 1
        else:
            # Refresh reference in case simulator passes updated object.
            s.pipeline_by_id[pid] = p

    # Update OOM/Success signals from execution results.
    for r in results:
        # r.ops is a list of ops executed in this container.
        ops = getattr(r, "ops", None) or []
        if getattr(r, "failed", None) and r.failed():
            is_oom = _is_oom_error(getattr(r, "error", None))
            for op in ops:
                k = _op_key(op)
                s.op_fail_count[k] = int(s.op_fail_count.get(k, 0)) + 1
                s.op_last_fail_is_oom[k] = bool(is_oom)
                s.op_last_error[k] = getattr(r, "error", None)

                # If OOM-like, increase memory multiplier aggressively but cap it.
                if is_oom:
                    cur = float(s.op_mem_factor.get(k, 1.0))
                    s.op_mem_factor[k] = min(float(s.max_mem_factor), cur * 1.5)
        else:
            # On success, slightly relax any previously inflated memory factor.
            for op in ops:
                k = _op_key(op)
                if k in s.op_mem_factor:
                    s.op_mem_factor[k] = max(1.0, float(s.op_mem_factor[k]) * 0.9)

    # Rebuild active pipeline list, dropping completed or unretryable failed pipelines.
    active = []
    for pid, p in list(s.pipeline_by_id.items()):
        status = p.runtime_status()
        if status.is_pipeline_successful():
            del s.pipeline_by_id[pid]
            continue
        if _drop_if_unretryable_failures(s, p):
            del s.pipeline_by_id[pid]
            continue
        active.append(p)

    # If nothing changed and no new pipelines/results, avoid churning.
    if not pipelines and not results:
        return [], []

    # Stable ordering: priority then arrival seq.
    def _sort_key(p):
        return (_priority_rank(p.priority), int(s.pipeline_arrival_seq.get(p.pipeline_id, 0)))

    active.sort(key=_sort_key)

    hp_ready = _has_any_ready_high_priority_work(s, active)

    suspensions = []
    assignments = []

    # Track pipelines we've already scheduled in this tick to avoid assigning multiple ops per pipeline per tick.
    scheduled_pids = set()

    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]

        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)
        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # Soft reservation in pool 0: keep headroom for high priority if any is waiting/ready.
        reserve_cpu = 0.0
        reserve_ram = 0.0
        if pool_id == 0 and hp_ready:
            reserve_cpu = float(pool.max_cpu_pool) * float(s.reserve_frac_cpu_pool0)
            reserve_ram = float(pool.max_ram_pool) * float(s.reserve_frac_ram_pool0)

        # Pool preference:
        #   - pool 0: if HP exists, schedule HP first, then batch if still room.
        #   - other pools: schedule batch first, then HP (keeps some isolation while still allowing spillover).
        if pool_id == 0 and hp_ready:
            prio_order = [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]
        elif pool_id == 0:
            prio_order = [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]
        else:
            prio_order = [Priority.BATCH_PIPELINE, Priority.INTERACTIVE, Priority.QUERY]

        made_progress = True
        while made_progress:
            made_progress = False
            if avail_cpu <= 0 or avail_ram <= 0:
                break

            # Find the best pipeline for this pool based on priority order and arrival seq.
            chosen_pipeline = None
            chosen_op = None

            for pri in prio_order:
                for p in active:
                    if p.pipeline_id in scheduled_pids:
                        continue
                    if p.priority != pri:
                        continue
                    op = _pipeline_ready_op(s, p)
                    if op is None:
                        continue
                    # Candidate found; we'll check fit after sizing.
                    chosen_pipeline = p
                    chosen_op = op
                    break
                if chosen_pipeline is not None:
                    break

            if chosen_pipeline is None:
                break

            # Size request.
            ram_req = _ram_request_gb(s, chosen_op, pool, chosen_pipeline.priority)
            cpu_req = _cpu_request(s, pool, chosen_pipeline.priority, avail_cpu)

            # Enforce "no batch crowd-out" in pool 0 when HP exists by keeping reserves.
            if pool_id == 0 and hp_ready and chosen_pipeline.priority == Priority.BATCH_PIPELINE:
                if (avail_cpu - cpu_req) < reserve_cpu or (avail_ram - ram_req) < reserve_ram:
                    # Try to find a non-batch op instead; if none fit, we stop filling pool 0.
                    alt_found = False
                    for alt_pri in (Priority.QUERY, Priority.INTERACTIVE):
                        for p in active:
                            if p.pipeline_id in scheduled_pids:
                                continue
                            if p.priority != alt_pri:
                                continue
                            op = _pipeline_ready_op(s, p)
                            if op is None:
                                continue
                            alt_ram = _ram_request_gb(s, op, pool, p.priority)
                            alt_cpu = _cpu_request(s, pool, p.priority, avail_cpu)
                            if alt_ram <= avail_ram and alt_cpu <= avail_cpu:
                                chosen_pipeline, chosen_op = p, op
                                ram_req, cpu_req = alt_ram, alt_cpu
                                alt_found = True
                                break
                        if alt_found:
                            break
                    if not alt_found:
                        break

            # Fit check.
            if ram_req > avail_ram or cpu_req > avail_cpu:
                # Can't fit the chosen op; to avoid O(N^2) searching, do a light fallback search
                # within the same priority order for any op that fits.
                fit_found = False
                for pri in prio_order:
                    for p in active:
                        if p.pipeline_id in scheduled_pids:
                            continue
                        if p.priority != pri:
                            continue
                        op = _pipeline_ready_op(s, p)
                        if op is None:
                            continue
                        rr = _ram_request_gb(s, op, pool, p.priority)
                        cr = _cpu_request(s, pool, p.priority, avail_cpu)
                        if rr <= avail_ram and cr <= avail_cpu:
                            chosen_pipeline, chosen_op = p, op
                            ram_req, cpu_req = rr, cr
                            fit_found = True
                            break
                    if fit_found:
                        break
                if not fit_found:
                    break

            assignments.append(
                Assignment(
                    ops=[chosen_op],
                    cpu=float(cpu_req),
                    ram=float(ram_req),
                    priority=chosen_pipeline.priority,
                    pool_id=pool_id,
                    pipeline_id=chosen_pipeline.pipeline_id,
                )
            )

            scheduled_pids.add(chosen_pipeline.pipeline_id)
            avail_cpu -= float(cpu_req)
            avail_ram -= float(ram_req)
            made_progress = True

    return suspensions, assignments
