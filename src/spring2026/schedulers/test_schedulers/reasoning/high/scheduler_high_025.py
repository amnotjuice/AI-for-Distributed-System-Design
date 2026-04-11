# policy_key: scheduler_high_025
# reasoning_effort: high
# exp: reasoning
# model: gpt-5.2-2025-12-11
# llm_cost: 0.129511
# generation_seconds: 188.81
# generated_at: 2026-04-10T01:59:10.879274
@register_scheduler_init(key="scheduler_high_025")
def scheduler_high_025_init(s):
    """
    Priority + reservation + OOM-adaptive retry scheduler.

    Goals (aligned with objective):
      - Keep query/interactive latency low via strict priority ordering and reserved headroom.
      - Avoid permanent failures by treating FAILED as retryable and increasing RAM on OOM feedback.
      - Avoid starvation (and 720s penalties for incomplete pipelines) by ensuring batch progress
        on non-latency pools (or via a weak anti-starvation knob in single-pool setups).

    Key ideas:
      - Multi-queue by priority (QUERY > INTERACTIVE > BATCH).
      - Headroom reservation: when HP work is waiting, do not let batch consume the last X% CPU/RAM.
      - OOM learning: if an op fails with OOM, bump its RAM estimate and retry later.
      - Placement bias: pool 0 is the "latency pool" (if multiple pools); other pools bias to batch.
    """
    from collections import deque, defaultdict

    s.queues = {
        Priority.QUERY: deque(),
        Priority.INTERACTIVE: deque(),
        Priority.BATCH_PIPELINE: deque(),
    }

    # Pipeline bookkeeping (avoid leaks / optionally mark "give up" on repeated irrecoverable failures).
    s.pipeline_meta = {}  # pipeline_id -> {"give_up": bool}

    # Per-operator learned RAM estimates (keyed by a stable-ish op key).
    s.op_ram_est = {}  # op_key -> ram
    s.op_oom_counts = defaultdict(int)   # op_key -> count
    s.op_fail_counts = defaultdict(int)  # op_key -> count (any failure)

    # Policy knobs
    s.reserved_cpu_frac = 0.30  # reserved for high priority when HP is waiting
    s.reserved_ram_frac = 0.30
    s.oom_backoff = 1.8         # multiply last RAM on OOM
    s.safety_ram = 1.10         # small safety margin even for learned estimates
    s.max_fail_retries = 6      # after too many failures for an op, stop spending resources

    # Single-pool anti-starvation: after too many ticks with batch waiting and no batch scheduled,
    # slightly relax reservations to ensure batch completes within horizon.
    s.batch_starve_ticks = 0
    s.batch_starve_threshold = 12

    # Minimal allocations (avoid zero-sized assignments)
    s.min_cpu = 0.1
    s.min_ram = 0.1


def _is_oom_error(err) -> bool:
    if not err:
        return False
    s_err = str(err).lower()
    return ("oom" in s_err) or ("out of memory" in s_err) or ("out_of_memory" in s_err) or ("memoryerror" in s_err)


def _op_key(op):
    """Best-effort stable key for learning RAM across retries/results."""
    for attr in ("operator_id", "op_id", "id", "name", "key"):
        if hasattr(op, attr):
            try:
                v = getattr(op, attr)
                if v is not None:
                    return (attr, v)
            except Exception:
                pass
    # Fallback: repr may include memory addr; id(op) is stable for object lifetime (good enough here).
    try:
        return ("id", id(op))
    except Exception:
        return ("repr", repr(op))


def _priority_order_for_pool(num_pools: int, pool_id: int):
    # If multiple pools, bias pool 0 to latency-sensitive work.
    if num_pools > 1 and pool_id == 0:
        return (Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE)
    # Throughput pools bias to batch, but can absorb HP if idle.
    return (Priority.BATCH_PIPELINE, Priority.INTERACTIVE, Priority.QUERY)


def _estimate_cpu(s, pool, priority, num_pools: int, pool_id: int, avail_cpu: float) -> float:
    max_cpu = pool.max_cpu_pool

    # Slightly higher CPU allocations on the latency pool.
    latency_pool = (num_pools > 1 and pool_id == 0)

    if priority == Priority.QUERY:
        frac = 0.75 if latency_pool else 0.65
    elif priority == Priority.INTERACTIVE:
        frac = 0.50 if latency_pool else 0.45
    else:
        frac = 0.25 if not latency_pool else 0.20

    target = max_cpu * frac

    # Avoid pathological over-allocation when pool is nearly empty (sublinear scaling + fairness).
    # Still allow HP to grab more if it's the only thing running.
    target = min(target, max_cpu)

    cpu = min(avail_cpu, target)
    if cpu < s.min_cpu:
        cpu = min(avail_cpu, s.min_cpu)
    return cpu


def _estimate_ram(s, pool, priority, op_key, avail_ram: float) -> float:
    max_ram = pool.max_ram_pool

    learned = s.op_ram_est.get(op_key, None)
    if learned is None:
        # Conservative initial sizing to reduce OOMs (failures are expensive in the objective).
        if priority == Priority.QUERY:
            frac = 0.40
        elif priority == Priority.INTERACTIVE:
            frac = 0.32
        else:
            frac = 0.24
        est = max_ram * frac
    else:
        est = learned

    # Safety margin + additional boost for repeated OOM history.
    est *= s.safety_ram
    oom_n = s.op_oom_counts.get(op_key, 0)
    if oom_n > 0:
        est *= min(4.0, 1.35 ** oom_n)

    # Cap to avoid fully pinning a pool (unless it's truly the only option).
    est = min(est, max_ram * 0.95)
    est = max(est, s.min_ram)

    # If we can't fit even the estimate, do not shrink below estimate (better to wait than OOM).
    return min(est, avail_ram)


def _can_schedule_batch_with_reservation(s, pool, remaining_cpu_after: float, remaining_ram_after: float, hp_waiting: bool) -> bool:
    if not hp_waiting:
        return True
    reserved_cpu = pool.max_cpu_pool * s.reserved_cpu_frac
    reserved_ram = pool.max_ram_pool * s.reserved_ram_frac
    return (remaining_cpu_after >= reserved_cpu) and (remaining_ram_after >= reserved_ram)


def _pick_pipeline_op_fit(s, pool, pool_id: int, num_pools: int,
                         avail_cpu: float, avail_ram: float,
                         scheduled_counts, per_pipeline_limit,
                         hp_waiting: bool):
    """
    Greedy scan within each priority queue (rotation-preserving) to find a READY op that fits.
    Returns (pipeline, op_list, cpu, ram, priority) or None.
    """
    priority_order = _priority_order_for_pool(num_pools, pool_id)

    for prio in priority_order:
        q = s.queues[prio]
        n = len(q)
        if n == 0:
            continue

        for _ in range(n):
            p = q.popleft()

            # Drop finished pipelines from queues.
            try:
                status = p.runtime_status()
            except Exception:
                # If status access fails for any reason, requeue and skip.
                q.append(p)
                continue

            if status.is_pipeline_successful():
                s.pipeline_meta.pop(p.pipeline_id, None)
                continue

            meta = s.pipeline_meta.setdefault(p.pipeline_id, {})
            if meta.get("give_up", False):
                continue

            # Limit parallelism per pipeline per tick (avoid one pipeline hogging everything).
            if scheduled_counts.get(p.pipeline_id, 0) >= per_pipeline_limit.get(prio, 1):
                q.append(p)
                continue

            # Only schedule ops whose parents are complete.
            op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
            if not op_list:
                q.append(p)
                continue

            op = op_list[0]
            ok = _op_key(op)

            # If this op has failed too many times, stop spending resources on the pipeline.
            if s.op_fail_counts.get(ok, 0) >= s.max_fail_retries:
                meta["give_up"] = True
                continue

            cpu = _estimate_cpu(s, pool, prio, num_pools, pool_id, avail_cpu)
            if cpu <= 0:
                q.append(p)
                continue

            ram = _estimate_ram(s, pool, prio, ok, avail_ram)
            if ram <= 0:
                q.append(p)
                continue

            # Must fit.
            if cpu > avail_cpu + 1e-9 or ram > avail_ram + 1e-9:
                q.append(p)
                continue

            # Enforce reservation for batch if HP work exists, unless we're in severe starvation mode.
            if prio == Priority.BATCH_PIPELINE:
                allow = _can_schedule_batch_with_reservation(
                    s, pool, avail_cpu - cpu, avail_ram - ram, hp_waiting
                )
                if not allow:
                    q.append(p)
                    continue

            # Rotation-preserving: put it back after selecting, so future scans are fair.
            q.append(p)
            return p, op_list, cpu, ram, prio

    return None


@register_scheduler(key="scheduler_high_025")
def scheduler_high_025(s, results: List["ExecutionResult"],
                       pipelines: List["Pipeline"]) -> Tuple[List["Suspend"], List["Assignment"]]:
    suspensions = []
    assignments = []

    # Enqueue new arrivals.
    for p in pipelines:
        s.pipeline_meta.setdefault(p.pipeline_id, {})
        s.queues[p.priority].append(p)

    # Update RAM estimates based on execution feedback (especially OOM).
    for r in results:
        try:
            ops = r.ops or []
        except Exception:
            ops = []
        if not ops:
            continue

        ok = _op_key(ops[0])

        if r.failed():
            s.op_fail_counts[ok] += 1
            if _is_oom_error(getattr(r, "error", None)):
                s.op_oom_counts[ok] += 1
                # Increase estimate based on what we just tried.
                # Prefer ramping quickly (avoid repeated OOMs that extend latency / risk incompletion).
                last = float(getattr(r, "ram", 0) or 0)
                bumped = max(last * s.oom_backoff, last + s.min_ram)
                # Clamp to pool max RAM.
                try:
                    pool_max = s.executor.pools[r.pool_id].max_ram_pool
                    bumped = min(bumped, pool_max * 0.98)
                except Exception:
                    pass
                prev = s.op_ram_est.get(ok, 0)
                s.op_ram_est[ok] = max(prev, bumped)
        else:
            # On success, record a baseline if we have none (do not aggressively shrink; failures are costly).
            try:
                used = float(getattr(r, "ram", 0) or 0)
            except Exception:
                used = 0
            if used > 0 and ok not in s.op_ram_est:
                s.op_ram_est[ok] = used

    # If nothing changed, early exit.
    if not pipelines and not results:
        return suspensions, assignments

    # Determine whether HP work is waiting (used to enforce batch reservations).
    hp_waiting = (len(s.queues[Priority.QUERY]) > 0) or (len(s.queues[Priority.INTERACTIVE]) > 0)

    # Single-pool anti-starvation: if batch is waiting for many ticks, relax reservations a bit.
    # This is a safety valve to reduce the chance batch pipelines remain incomplete (720s penalty).
    if s.executor.num_pools == 1:
        if len(s.queues[Priority.BATCH_PIPELINE]) > 0 and len(assignments) == 0:
            s.batch_starve_ticks += 1
        else:
            s.batch_starve_ticks = 0

        if s.batch_starve_ticks >= s.batch_starve_threshold:
            # Temporarily relax reservations (still keep some headroom for HP).
            s.reserved_cpu_frac = 0.18
            s.reserved_ram_frac = 0.18
        else:
            s.reserved_cpu_frac = 0.30
            s.reserved_ram_frac = 0.30
    else:
        # Reset to defaults in multi-pool mode.
        s.reserved_cpu_frac = 0.30
        s.reserved_ram_frac = 0.30

    # Per-tick per-pipeline parallelism caps: let QUERY run a bit more parallel to cut tail latency.
    per_pipeline_limit = {
        Priority.QUERY: 2,
        Priority.INTERACTIVE: 1,
        Priority.BATCH_PIPELINE: 1,
    }
    scheduled_counts = {}  # pipeline_id -> how many ops scheduled this tick

    # Greedily fill each pool with fitting work.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool

        # If essentially no resources, skip.
        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # Fill loop with a conservative bound to avoid infinite loops on unready queues.
        max_iters = 64
        iters = 0
        while iters < max_iters and avail_cpu > 0 and avail_ram > 0:
            iters += 1
            picked = _pick_pipeline_op_fit(
                s, pool, pool_id, s.executor.num_pools,
                avail_cpu, avail_ram,
                scheduled_counts, per_pipeline_limit,
                hp_waiting=hp_waiting
            )
            if not picked:
                break

            pipeline, op_list, cpu, ram, prio = picked

            # Final sanity clamp (never exceed current availability).
            cpu = min(cpu, avail_cpu)
            ram = min(ram, avail_ram)
            if cpu <= 0 or ram <= 0:
                break

            assignments.append(
                Assignment(
                    ops=op_list,
                    cpu=cpu,
                    ram=ram,
                    priority=prio,
                    pool_id=pool_id,
                    pipeline_id=pipeline.pipeline_id,
                )
            )

            scheduled_counts[pipeline.pipeline_id] = scheduled_counts.get(pipeline.pipeline_id, 0) + 1
            avail_cpu -= cpu
            avail_ram -= ram

    return suspensions, assignments
