# policy_key: scheduler_high_038
# reasoning_effort: high
# exp: reasoning
# model: gpt-5.2-2025-12-11
# llm_cost: 0.193309
# generation_seconds: 158.84
# generated_at: 2026-04-10T03:01:24.626717
@register_scheduler_init(key="scheduler_high_038")
def scheduler_high_038_init(s):
    """Priority-aware, OOM-averse scheduler.

    Core ideas:
      1) Strict priority ordering (QUERY > INTERACTIVE > BATCH) with a small anti-starvation escape hatch for batch.
      2) Conservative RAM sizing + adaptive RAM bumps on OOM to maximize completion rate (failures are heavily penalized).
      3) Moderate CPU sizing to improve overall concurrency while still favoring high-priority latency.
      4) Optional, best-effort preemption (only if the executor exposes running container metadata).
    """
    s.tick = 0

    # Round-robin queues per priority; keep pipelines in queues even while running.
    s.queues = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }
    s.in_queue = set()  # pipeline_id set to avoid duplicates

    # Per-operator adaptive hints (keyed by id(op) for stability within a simulation run).
    s.op_ram_mult = {}       # op_id -> multiplier applied to estimated minimum RAM
    s.op_ram_hint = {}       # op_id -> absolute RAM floor learned from prior runs
    s.op_cpu_hint = {}       # op_id -> absolute CPU floor learned from prior runs
    s.op_fail_count = {}     # op_id -> number of failures
    s.op_next_tick = {}      # op_id -> earliest tick allowed to retry (backoff for non-OOM)

    # Track which pipeline was scheduled this tick to avoid multi-pool double-dispatch.
    s.scheduled_this_tick = set()

    # Anti-starvation: if batch has been skipped for too long, allow limited progress.
    s.batch_starve_ticks = {}   # pipeline_id -> ticks since last successful assignment
    s.batch_starve_threshold = 30

    # Guardrails for RAM growth.
    s.max_ram_mult = 16.0

    # Best-effort preemption cooldown.
    s.container_last_preempt_tick = {}  # container_id -> tick


def _is_number(x):
    return isinstance(x, (int, float)) and x == x  # NaN-safe


def _get_attr_number(obj, names):
    for n in names:
        if hasattr(obj, n):
            v = getattr(obj, n)
            if _is_number(v):
                return float(v)
    return None


def _estimate_min_ram(op, pool):
    # Prefer explicit operator requirements if present; otherwise choose a small, safe floor.
    v = _get_attr_number(op, ["min_ram", "ram_min", "ram", "mem", "memory", "peak_ram", "ram_req"])
    if v is not None and v > 0:
        return v
    # Fallback: small fraction of pool RAM, but not too tiny (OOMs are expensive).
    return max(0.5, 0.02 * float(pool.max_ram_pool))


def _estimate_min_cpu(op, pool):
    v = _get_attr_number(op, ["min_cpu", "cpu_min", "cpu", "cpu_req", "vcpus"])
    if v is not None and v > 0:
        return v
    return 1.0


def _default_ram_mult(priority):
    if priority == Priority.QUERY:
        return 1.8
    if priority == Priority.INTERACTIVE:
        return 1.6
    return 1.3


def _cpu_target(priority, pool):
    # Moderate CPU targets to preserve concurrency, while still favoring high-priority latency.
    max_cpu = float(pool.max_cpu_pool)
    if priority == Priority.QUERY:
        return min(8.0, max(2.0, 0.50 * max_cpu))
    if priority == Priority.INTERACTIVE:
        return min(6.0, max(2.0, 0.35 * max_cpu))
    return min(4.0, max(1.0, 0.25 * max_cpu))


def _is_oom_error(err):
    if err is None:
        return False
    try:
        s = str(err).lower()
    except Exception:
        return False
    return ("oom" in s) or ("out of memory" in s) or ("cuda out of memory" in s) or ("memoryerror" in s)


def _op_id(op):
    # Use id(op) as a stable-per-run identifier.
    return id(op)


def _pipeline_ready_op(pipeline):
    st = pipeline.runtime_status()
    if st.is_pipeline_successful():
        return None
    ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
    if not ops:
        return None
    return ops[0]


def _best_effort_running_containers(pool):
    # Try a few common attribute names; if none exist, return [].
    for name in ["running_containers", "containers", "active_containers", "live_containers"]:
        if hasattr(pool, name):
            try:
                v = getattr(pool, name)
                if isinstance(v, list):
                    return v
                # Some implementations may expose dict-like containers.
                if hasattr(v, "values"):
                    return list(v.values())
            except Exception:
                return []
    return []


def _container_fields(container):
    # Return (container_id, priority, cpu, ram) if present, else None.
    try:
        cid = getattr(container, "container_id", None)
        pr = getattr(container, "priority", None)
        cpu = getattr(container, "cpu", None)
        ram = getattr(container, "ram", None)
        if cid is None or pr is None or not _is_number(cpu) or not _is_number(ram):
            return None
        return cid, pr, float(cpu), float(ram)
    except Exception:
        return None


@register_scheduler(key="scheduler_high_038")
def scheduler_high_038_scheduler(s, results, pipelines):
    """
    Scheduler step:
      - Enqueue new pipelines.
      - Update adaptive RAM/CPU hints from execution results (OOM-aware).
      - Assign ready operators using strict priority + limited batch anti-starvation.
      - Optionally preempt batch containers when query work is blocked and metadata is available.
    """
    s.tick += 1
    s.scheduled_this_tick = set()

    # Enqueue arrivals (no duplicates).
    for p in pipelines:
        pid = p.pipeline_id
        if pid not in s.in_queue:
            s.queues[p.priority].append(p)
            s.in_queue.add(pid)
            if p.priority == Priority.BATCH_PIPELINE:
                s.batch_starve_ticks[pid] = 0

    # Early exit if nothing changed.
    if not pipelines and not results:
        return [], []

    # Compute global capacity bounds (used to detect truly unschedulable OOM escalations).
    max_ram_any = 0.0
    max_cpu_any = 0.0
    for i in range(s.executor.num_pools):
        pool = s.executor.pools[i]
        try:
            max_ram_any = max(max_ram_any, float(pool.max_ram_pool))
            max_cpu_any = max(max_cpu_any, float(pool.max_cpu_pool))
        except Exception:
            pass

    # Update adaptive hints from results.
    for r in results:
        try:
            failed = r.failed()
        except Exception:
            failed = False

        oom = failed and _is_oom_error(getattr(r, "error", None))

        # Learn from each op in the result.
        for op in getattr(r, "ops", []) or []:
            oid = _op_id(op)

            if failed:
                s.op_fail_count[oid] = s.op_fail_count.get(oid, 0) + 1

                if oom:
                    # Aggressively increase RAM; OOM is the most damaging failure mode for completion rate.
                    prev_mult = s.op_ram_mult.get(oid, _default_ram_mult(getattr(r, "priority", Priority.BATCH_PIPELINE)))
                    new_mult = min(s.max_ram_mult, max(prev_mult, prev_mult * 1.6))
                    s.op_ram_mult[oid] = new_mult

                    # Raise absolute RAM floor based on what we tried last time.
                    tried_ram = getattr(r, "ram", None)
                    if _is_number(tried_ram) and tried_ram > 0:
                        prev_hint = s.op_ram_hint.get(oid, 0.0)
                        s.op_ram_hint[oid] = max(prev_hint, float(tried_ram) * 1.6)

                    # No backoff for OOM; retry quickly with more RAM.
                    if oid in s.op_next_tick:
                        s.op_next_tick[oid] = min(s.op_next_tick[oid], s.tick)
                else:
                    # Non-OOM: apply exponential backoff to reduce churn, but keep retrying.
                    c = s.op_fail_count.get(oid, 1)
                    delay = 2 ** min(c, 6)
                    s.op_next_tick[oid] = max(s.op_next_tick.get(oid, 0), s.tick + delay)
            else:
                # Success: store floors so we don't regress to too-small allocations.
                tried_ram = getattr(r, "ram", None)
                if _is_number(tried_ram) and tried_ram > 0:
                    s.op_ram_hint[oid] = max(s.op_ram_hint.get(oid, 0.0), float(tried_ram))

                tried_cpu = getattr(r, "cpu", None)
                if _is_number(tried_cpu) and tried_cpu > 0:
                    s.op_cpu_hint[oid] = max(s.op_cpu_hint.get(oid, 0.0), float(tried_cpu))

                # Clear retry backoff after success.
                if oid in s.op_next_tick:
                    del s.op_next_tick[oid]

    suspensions = []
    assignments = []

    def _queue_has_ready(priority):
        q = s.queues[priority]
        # Bounded scan; queue may contain blocked/running pipelines.
        for p in q[: min(len(q), 16)]:
            if p.pipeline_id in s.scheduled_this_tick:
                continue
            op = _pipeline_ready_op(p)
            if op is None:
                continue
            oid = _op_id(op)
            if s.op_next_tick.get(oid, 0) > s.tick:
                continue
            return True
        return False

    query_waiting = _queue_has_ready(Priority.QUERY)
    interactive_waiting = _queue_has_ready(Priority.INTERACTIVE)

    def _pick_fit_from_queue(priority, pool, avail_cpu, avail_ram):
        q = s.queues[priority]
        n = len(q)
        if n == 0:
            return None, None, None, None

        # Round-robin scan: rotate as we inspect.
        for _ in range(n):
            p = q.pop(0)
            pid = p.pipeline_id

            # Drop completed pipelines lazily.
            st = p.runtime_status()
            if st.is_pipeline_successful():
                s.in_queue.discard(pid)
                if pid in s.batch_starve_ticks:
                    del s.batch_starve_ticks[pid]
                continue

            # Keep pipeline in queue regardless; we will append back unless removed.
            q.append(p)

            if pid in s.scheduled_this_tick:
                continue

            op = _pipeline_ready_op(p)
            if op is None:
                continue

            oid = _op_id(op)
            if s.op_next_tick.get(oid, 0) > s.tick:
                continue

            base_ram = _estimate_min_ram(op, pool)
            base_cpu = _estimate_min_cpu(op, pool)

            mult = s.op_ram_mult.get(oid, _default_ram_mult(priority))
            ram = base_ram * mult
            ram = max(ram, s.op_ram_hint.get(oid, 0.0))

            # Priority-specific RAM floors to reduce OOM risk for score-dominant classes.
            if priority == Priority.QUERY:
                ram = max(ram, 1.0)
            elif priority == Priority.INTERACTIVE:
                ram = max(ram, 1.0)
            else:
                ram = max(ram, 0.5)

            # CPU sizing: use hints and targets, but avoid consuming the entire pool for one op.
            cpu = max(base_cpu, s.op_cpu_hint.get(oid, 0.0), _cpu_target(priority, pool))
            cpu = min(cpu, float(pool.max_cpu_pool))
            cpu = min(cpu, avail_cpu)
            ram = min(ram, avail_ram)

            # Detect impossible RAM requirement: if our current learned floor already exceeds all pools, skip permanently.
            # (Avoids wasting resources on doomed work; penalty remains 720 either way.)
            if s.op_ram_hint.get(oid, 0.0) > 0 and s.op_ram_hint[oid] > max_ram_any:
                continue
            if base_ram > max_ram_any:
                continue

            # Must fit at least minimal viable CPU/RAM.
            if cpu >= 1.0 and ram >= min(base_ram, max_ram_any) and ram > 0:
                return p, op, cpu, ram

        return None, None, None, None

    def _maybe_preempt_for_query(pool_id, pool, avail_cpu, avail_ram):
        # Only if query is waiting and we can see running containers.
        if not query_waiting:
            return avail_cpu, avail_ram

        # Estimate what we need for *some* query op (best-effort): look for the first query that could run here.
        p, op, need_cpu, need_ram = _pick_fit_from_queue(Priority.QUERY, pool, float(pool.max_cpu_pool), float(pool.max_ram_pool))
        if p is None:
            return avail_cpu, avail_ram

        # If already enough headroom, no preemption.
        if avail_cpu >= need_cpu and avail_ram >= need_ram:
            return avail_cpu, avail_ram

        # Try to suspend a small number of batch containers to create headroom.
        containers = _best_effort_running_containers(pool)
        candidates = []
        for c in containers:
            f = _container_fields(c)
            if f is None:
                continue
            cid, pr, ccpu, cram = f
            if pr != Priority.BATCH_PIPELINE:
                continue
            last = s.container_last_preempt_tick.get(cid, -10**9)
            if s.tick - last < 10:
                continue
            candidates.append((cram, ccpu, cid))  # prefer suspending largest RAM first

        candidates.sort(reverse=True)

        suspended = 0
        for _, _, cid in candidates:
            if avail_cpu >= need_cpu and avail_ram >= need_ram:
                break
            if suspended >= 2:
                break
            suspensions.append(Suspend(cid, pool_id))
            s.container_last_preempt_tick[cid] = s.tick
            # Optimistically account resources; simulator will reconcile.
            # If values were unknown, we won't be here due to _container_fields.
            for c in containers:
                f = _container_fields(c)
                if f is not None and f[0] == cid:
                    avail_cpu += f[2]
                    avail_ram += f[3]
                    break
            suspended += 1

        return avail_cpu, avail_ram

    # Fill pools greedily, favoring high priority. Limit assignments per pool to keep decisions stable.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        try:
            avail_cpu = float(pool.avail_cpu_pool)
            avail_ram = float(pool.avail_ram_pool)
        except Exception:
            continue

        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # Best-effort preemption to unblock query latency if possible.
        avail_cpu, avail_ram = _maybe_preempt_for_query(pool_id, pool, avail_cpu, avail_ram)

        max_assignments = 4
        made = 0

        # Reserve headroom for query/interactive when they are waiting, to avoid batch blocking.
        reserve_cpu = 0.0
        reserve_ram = 0.0
        if query_waiting or interactive_waiting:
            reserve_cpu = 0.20 * float(pool.max_cpu_pool)
            reserve_ram = 0.20 * float(pool.max_ram_pool)

        while made < max_assignments and avail_cpu >= 1.0 and avail_ram > 0:
            # Batch anti-starvation: if any batch pipeline has waited too long, allow one batch op opportunistically.
            allow_batch = True
            if query_waiting or interactive_waiting:
                # Only allow batch if we still have meaningful headroom.
                allow_batch = (avail_cpu - reserve_cpu) >= 1.0 and (avail_ram - reserve_ram) >= 0.5

            force_one_batch = False
            if (query_waiting or interactive_waiting) and allow_batch:
                # If some batch pipeline is starving, allow exactly one batch op per pool per tick.
                for pid, t in list(s.batch_starve_ticks.items()):
                    if t >= s.batch_starve_threshold:
                        force_one_batch = True
                        break

            chosen = None

            # Priority order with optional "one batch escape hatch".
            if query_waiting:
                chosen = Priority.QUERY
            elif interactive_waiting:
                chosen = Priority.INTERACTIVE
            elif allow_batch:
                chosen = Priority.BATCH_PIPELINE

            if force_one_batch:
                chosen = Priority.BATCH_PIPELINE

            if chosen is None:
                break

            p, op, cpu, ram = _pick_fit_from_queue(chosen, pool, avail_cpu, avail_ram)

            # If chosen priority can't fit, try the next best option.
            if p is None:
                if chosen == Priority.QUERY:
                    p, op, cpu, ram = _pick_fit_from_queue(Priority.INTERACTIVE, pool, avail_cpu, avail_ram)
                    if p is None and allow_batch:
                        p, op, cpu, ram = _pick_fit_from_queue(Priority.BATCH_PIPELINE, pool, avail_cpu, avail_ram)
                elif chosen == Priority.INTERACTIVE:
                    if allow_batch:
                        p, op, cpu, ram = _pick_fit_from_queue(Priority.BATCH_PIPELINE, pool, avail_cpu, avail_ram)
                    if p is None and query_waiting:
                        p, op, cpu, ram = _pick_fit_from_queue(Priority.QUERY, pool, avail_cpu, avail_ram)
                else:
                    # Batch chosen but doesn't fit; don't steal reserved space by forcing a high-priority oversize.
                    p = None

            if p is None:
                break

            # Create the assignment (single op per assignment for better fairness/packing).
            assignment = Assignment(
                ops=[op],
                cpu=cpu,
                ram=ram,
                priority=p.priority,
                pool_id=pool_id,
                pipeline_id=p.pipeline_id,
            )
            assignments.append(assignment)

            s.scheduled_this_tick.add(p.pipeline_id)
            avail_cpu -= float(cpu)
            avail_ram -= float(ram)
            made += 1

            # Reset starvation counter on batch progress; increment otherwise below.
            if p.priority == Priority.BATCH_PIPELINE:
                s.batch_starve_ticks[p.pipeline_id] = 0

        # Increment batch starvation ticks for queued batch pipelines not scheduled this tick.
        for p in s.queues[Priority.BATCH_PIPELINE]:
            pid = p.pipeline_id
            if pid in s.in_queue and pid not in s.scheduled_this_tick:
                s.batch_starve_ticks[pid] = s.batch_starve_ticks.get(pid, 0) + 1

    return suspensions, assignments
