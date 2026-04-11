# policy_key: scheduler_high_032
# reasoning_effort: high
# model: gpt-5.2-2025-12-11
# llm_cost: 0.120553
# generation_seconds: 120.34
# generated_at: 2026-03-14T03:55:13.742164
@register_scheduler_init(key="scheduler_high_032")
def scheduler_high_032_init(s):
    """Priority-aware, latency-improving scheduler (incremental upgrade over naive FIFO).

    Main improvements (kept intentionally simple and robust):
      1) Priority queues: always try QUERY first, then INTERACTIVE, then BATCH.
      2) Avoid head-of-line blocking: place multiple small/medium assignments per tick per pool,
         instead of giving the whole pool to a single operator.
      3) Basic OOM-driven RAM auto-sizing: on OOM, retry the failed operator with more RAM.
      4) Gentle reservation: when high-priority backlog exists, keep some headroom before
         scheduling batch work to protect tail latency.
    """
    s.t = 0

    # Per-priority FIFO queues of Pipeline objects
    s.q = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }

    # Track pipeline arrivals (for optional anti-starvation decisions / bookkeeping)
    s.known_pipeline_ids = set()
    s.pipeline_enqueue_ts = {}

    # Per-operator adaptive hints (keyed by a stable operator key)
    s.op_ram_hint = {}           # last known good RAM (or increased after OOM)
    s.op_cpu_hint = {}           # last observed CPU (lightly capped on use)
    s.op_retries = {}            # retries for OOM-like failures
    s.op_last_fail_reason = {}   # "oom" / "fatal" / absent


def _prio_norm(p):
    # Default unknown priorities to batch-like behavior.
    if p == Priority.QUERY:
        return Priority.QUERY
    if p == Priority.INTERACTIVE:
        return Priority.INTERACTIVE
    if p == Priority.BATCH_PIPELINE:
        return Priority.BATCH_PIPELINE
    return Priority.BATCH_PIPELINE


def _op_key(op):
    # Prefer an explicit op_id if present; otherwise use Python object identity.
    # (Eudoxia typically keeps operator objects stable within a simulation run.)
    return getattr(op, "op_id", id(op))


def _is_oom_error(err):
    if err is None:
        return False
    s = str(err).lower()
    # Keep the predicate broad; simulator error strings can vary.
    return ("oom" in s) or ("out of memory" in s) or ("memory" in s and "alloc" in s)


def _should_drop_pipeline(s, pipeline):
    """Decide if pipeline should be dropped (terminal failure)."""
    status = pipeline.runtime_status()
    if status.is_pipeline_successful():
        return True

    # If there are failed ops, only allow retry when we believe it was OOM and retries remain.
    failed_ops = status.get_ops([OperatorState.FAILED], require_parents_complete=False)
    if not failed_ops:
        return False

    MAX_OOM_RETRIES = 3
    for op in failed_ops:
        k = _op_key(op)
        reason = s.op_last_fail_reason.get(k, None)
        if reason != "oom":
            return True
        if s.op_retries.get(k, 0) > MAX_OOM_RETRIES:
            return True
    return False


def _default_fracs(priority):
    """Return (cpu_frac, cpu_cap_frac, ram_frac, ram_cap_frac) for initial sizing."""
    # These are intentionally conservative for latency:
    # - High priority gets medium slices (enables concurrency + decent single-op speed).
    # - Batch gets smaller slices (keeps headroom and reduces interference).
    if priority == Priority.QUERY:
        return 0.50, 0.75, 0.35, 0.75
    if priority == Priority.INTERACTIVE:
        return 0.50, 0.75, 0.35, 0.75
    return 0.25, 0.50, 0.25, 0.60


def _calc_request(s, pool, priority, op, avail_cpu, avail_ram, reserve_cpu, reserve_ram):
    """Compute cpu/ram request for a single op in a pool, respecting hints and headroom."""
    k = _op_key(op)

    cpu_frac, cpu_cap_frac, ram_frac, ram_cap_frac = _default_fracs(priority)
    cpu_cap = max(1.0, pool.max_cpu_pool * cpu_cap_frac)
    ram_cap = max(1e-9, pool.max_ram_pool * ram_cap_frac)

    eff_avail_cpu = max(0.0, avail_cpu - reserve_cpu)
    eff_avail_ram = max(0.0, avail_ram - reserve_ram)

    # Cannot schedule anything meaningful without headroom.
    if eff_avail_cpu < 1.0 or eff_avail_ram <= 0.0:
        return None

    cpu_hint = s.op_cpu_hint.get(k, None)
    ram_hint = s.op_ram_hint.get(k, None)

    # If we have a hint (from previous success or an OOM-driven increase),
    # do NOT shrink below it; if it doesn't fit, we wait rather than likely re-OOM.
    if ram_hint is not None:
        req_ram = min(ram_hint, ram_cap)
        if req_ram > eff_avail_ram:
            return None
    else:
        req_ram = min(pool.max_ram_pool * ram_frac, eff_avail_ram, ram_cap)

    if cpu_hint is not None:
        req_cpu = min(max(1.0, cpu_hint), cpu_cap)
        if req_cpu > eff_avail_cpu:
            # CPU can be shrunk safely; keep at least 1 vCPU.
            req_cpu = eff_avail_cpu
    else:
        req_cpu = min(max(1.0, pool.max_cpu_pool * cpu_frac), eff_avail_cpu, cpu_cap)

    if req_cpu < 1.0 or req_ram <= 0.0:
        return None

    return req_cpu, req_ram


def _pick_next_for_pool(s, pool, pool_id, prio_list, avail_cpu, avail_ram, reserve_cpu, reserve_ram):
    """Pick the next (pipeline, op, cpu, ram) that fits on this pool, else None."""
    for prio in prio_list:
        q = s.q.get(prio, [])
        if not q:
            continue

        # Scan at most one full rotation of this queue to find something runnable that fits.
        n = len(q)
        for _ in range(n):
            pipeline = q.pop(0)

            # Drop completed or terminally failed pipelines.
            if _should_drop_pipeline(s, pipeline):
                pid = pipeline.pipeline_id
                s.known_pipeline_ids.discard(pid)
                s.pipeline_enqueue_ts.pop(pid, None)
                continue

            status = pipeline.runtime_status()
            op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
            if not op_list:
                # Not runnable yet; keep it in the queue.
                q.append(pipeline)
                continue

            op = op_list[0]
            req = _calc_request(
                s, pool, _prio_norm(pipeline.priority), op,
                avail_cpu, avail_ram, reserve_cpu, reserve_ram
            )
            if req is None:
                # Doesn't fit here; keep it for later / other pools.
                q.append(pipeline)
                continue

            cpu, ram = req
            # Requeue pipeline behind others for fairness (even after scheduling one op).
            q.append(pipeline)
            return pipeline, op, cpu, ram

    return None


@register_scheduler(key="scheduler_high_032")
def scheduler_high_032(s, results, pipelines):
    """
    Priority-first, multi-assignment per pool, OOM-adaptive sizing.

    Scheduling phases per tick:
      A) Schedule QUERY + INTERACTIVE across pools (sorted by available headroom).
      B) Schedule BATCH from remaining resources, but keep some headroom if high backlog exists.
    """
    s.t += 1

    # Incorporate new pipelines (dedupe by pipeline_id).
    for p in pipelines:
        pid = p.pipeline_id
        if pid in s.known_pipeline_ids:
            continue
        s.known_pipeline_ids.add(pid)
        s.pipeline_enqueue_ts[pid] = s.t
        s.q[_prio_norm(p.priority)].append(p)

    # Learn from results: detect OOM and increase RAM hints for retries.
    for r in results:
        if not getattr(r, "ops", None):
            continue
        for op in r.ops:
            k = _op_key(op)
            if r.failed():
                if _is_oom_error(getattr(r, "error", None)):
                    s.op_last_fail_reason[k] = "oom"
                    s.op_retries[k] = s.op_retries.get(k, 0) + 1
                    # Increase RAM hint multiplicatively from the last allocation or prior hint.
                    base = s.op_ram_hint.get(k, None)
                    if base is None:
                        base = getattr(r, "ram", None) or 1.0
                    else:
                        # If the last attempt used more than our hint, respect it as the base.
                        if getattr(r, "ram", None) is not None:
                            base = max(base, r.ram)
                    s.op_ram_hint[k] = float(base) * 2.0
                else:
                    s.op_last_fail_reason[k] = "fatal"
            else:
                # On success, remember that the allocated resources worked.
                s.op_last_fail_reason.pop(k, None)
                s.op_retries.pop(k, None)
                if getattr(r, "ram", None) is not None:
                    prev = s.op_ram_hint.get(k, 0.0)
                    s.op_ram_hint[k] = max(prev, float(r.ram))
                if getattr(r, "cpu", None) is not None:
                    prev = s.op_cpu_hint.get(k, 0.0)
                    s.op_cpu_hint[k] = max(prev, float(r.cpu))

    # Early exit only if nothing is waiting and nothing changed.
    has_waiting = any(len(q) > 0 for q in s.q.values())
    if not has_waiting and not pipelines and not results:
        return [], []

    suspensions = []  # no preemption yet (kept simple for correctness)
    assignments = []

    # Snapshot available resources; update locally as we assign to avoid oversubscription.
    avail_cpu = {}
    avail_ram = {}
    pools = s.executor.pools
    for pid in range(s.executor.num_pools):
        avail_cpu[pid] = float(pools[pid].avail_cpu_pool)
        avail_ram[pid] = float(pools[pid].avail_ram_pool)

    # Determine if we should reserve headroom for high-priority backlog.
    high_backlog = len(s.q[Priority.QUERY]) + len(s.q[Priority.INTERACTIVE])

    # Process pools with most headroom first (helps high priority find a fitting pool).
    pool_order = list(range(s.executor.num_pools))
    pool_order.sort(key=lambda i: (avail_ram[i], avail_cpu[i]), reverse=True)

    # Phase A: schedule high priority first.
    for pool_id in pool_order:
        pool = pools[pool_id]
        # High priority uses the full currently available pool headroom.
        reserve_cpu = 0.0
        reserve_ram = 0.0

        while True:
            if avail_cpu[pool_id] < 1.0 or avail_ram[pool_id] <= 0.0:
                break

            picked = _pick_next_for_pool(
                s, pool, pool_id,
                [Priority.QUERY, Priority.INTERACTIVE],
                avail_cpu[pool_id], avail_ram[pool_id],
                reserve_cpu, reserve_ram
            )
            if picked is None:
                break

            pipeline, op, cpu, ram = picked
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
            avail_cpu[pool_id] -= cpu
            avail_ram[pool_id] -= ram

    # Phase B: schedule batch with remaining resources.
    # If there is high backlog, keep a small reservation to improve latency / tail behavior.
    for pool_id in pool_order:
        pool = pools[pool_id]
        if high_backlog > 0:
            reserve_cpu = max(0.0, pool.max_cpu_pool * 0.15)   # keep some vCPU headroom
            reserve_ram = max(0.0, pool.max_ram_pool * 0.15)   # keep some RAM headroom
        else:
            reserve_cpu = 0.0
            reserve_ram = 0.0

        while True:
            if avail_cpu[pool_id] < 1.0 or avail_ram[pool_id] <= 0.0:
                break

            picked = _pick_next_for_pool(
                s, pool, pool_id,
                [Priority.BATCH_PIPELINE],
                avail_cpu[pool_id], avail_ram[pool_id],
                reserve_cpu, reserve_ram
            )
            if picked is None:
                break

            pipeline, op, cpu, ram = picked
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
            avail_cpu[pool_id] -= cpu
            avail_ram[pool_id] -= ram

    return suspensions, assignments
