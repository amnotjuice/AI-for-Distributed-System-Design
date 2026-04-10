# policy_key: scheduler_est_007
# reasoning_effort: low
# model: gpt-5.2-2025-12-11
# llm_cost: 0.043908
# generation_seconds: 45.26
# generated_at: 2026-04-01T13:40:33.197894
@register_scheduler_init(key="scheduler_est_007")
def scheduler_est_007_init(s):
    """
    Priority-aware, conservative-sizing scheduler (incremental improvement over naive FIFO).

    Main changes vs. naive:
      - Maintain separate FIFO queues per priority; always try to schedule higher priority first.
      - Reserve a small fraction of each pool's capacity for future high-priority arrivals
        (prevents batch from fully occupying a pool and harming interactive latency).
      - Use memory estimates (op.estimate.mem_peak_gb) when available; on OOM, increase RAM
        for that specific operator on retry (warm-start sizing).
      - Avoid "give everything to one op": cap CPU allocation per op to reduce head-of-line blocking.

    Notes:
      - No preemption yet (kept intentionally simple); this is a "small improvement first" policy.
      - We schedule at most one operator per pool per tick (stable and easy to reason about).
    """
    # Priority queues (FIFO) per class
    s.waiting_by_prio = {}

    # Per-operator RAM sizing hint (GB), learned from OOMs / prior attempts
    # Keyed by (pipeline_id, op_id-like)
    s.op_ram_hint_gb = {}

    # Basic knobs (kept modest)
    s.reserve_cpu_frac = 0.20  # reserve for QUERY/INTERACTIVE by preventing BATCH from using last 20%
    s.reserve_ram_frac = 0.20

    # Per-op CPU caps by priority (fraction of pool max)
    s.cpu_cap_frac_by_prio = {
        Priority.QUERY: 0.60,
        Priority.INTERACTIVE: 0.50,
        Priority.BATCH_PIPELINE: 0.35,
    }

    # When using estimates, pad them to reduce OOM risk
    s.mem_est_pad = 1.25

    # On OOM, multiply last request by this factor for the next attempt
    s.oom_backoff = 1.6

    # Minimum allocations to avoid pathological tiny containers (units are simulator's CPU/RAM)
    s.min_cpu = 1.0
    s.min_ram = 1.0


@register_scheduler(key="scheduler_est_007")
def scheduler_est_007_scheduler(s, results, pipelines):
    """
    Scheduler step.

    Inputs:
      - results: execution results from previous tick
      - pipelines: new pipelines arriving at this tick

    Returns:
      - suspensions: none (no preemption in this version)
      - assignments: up to one op per pool per tick, chosen by priority order and FIFO within class
    """
    # ---- Helpers (defined inside to keep policy self-contained) ----
    def _prio_key(p):
        # Smaller is higher priority in our ordering
        if p == Priority.QUERY:
            return 0
        if p == Priority.INTERACTIVE:
            return 1
        return 2  # Priority.BATCH_PIPELINE and anything else

    def _get_op_id(op):
        # Try common attributes; fall back to stable string
        for attr in ("operator_id", "op_id", "id", "name"):
            if hasattr(op, attr):
                v = getattr(op, attr)
                # Avoid using large objects as keys; coerce to str for safety
                try:
                    return str(v)
                except Exception:
                    pass
        try:
            return str(repr(op))
        except Exception:
            return str(id(op))

    def _op_key(pipeline_id, op):
        return (pipeline_id, _get_op_id(op))

    def _is_oom_error(err):
        if err is None:
            return False
        try:
            msg = str(err).lower()
        except Exception:
            return False
        return ("oom" in msg) or ("out of memory" in msg) or ("memory" in msg)

    def _ensure_prio_queue_exists(prio):
        if prio not in s.waiting_by_prio:
            s.waiting_by_prio[prio] = []

    def _pipeline_done_or_failed(p):
        st = p.runtime_status()
        if st.is_pipeline_successful():
            return True
        # Consider any failure as terminal for now except the operator-level OOM learning
        # (operators may be retried by the simulator/pipeline state machine; we only avoid
        # scheduling fully-failed pipelines here).
        try:
            return st.state_counts[OperatorState.FAILED] > 0
        except Exception:
            return False

    def _get_assignable_ops(p):
        st = p.runtime_status()
        # Prefer ops whose parents are complete; keep to a single op to avoid over-committing
        ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        return ops if ops else []

    def _reserve_for_interactive(pool):
        # Amount we try to keep free so that high-priority arrivals have headroom.
        reserve_cpu = pool.max_cpu_pool * s.reserve_cpu_frac
        reserve_ram = pool.max_ram_pool * s.reserve_ram_frac
        return reserve_cpu, reserve_ram

    def _cpu_cap_for_prio(pool, prio):
        frac = s.cpu_cap_frac_by_prio.get(prio, 0.35)
        return max(s.min_cpu, pool.max_cpu_pool * frac)

    def _ram_request_for_op(pool, pipeline, op):
        # Base on:
        #  - learned hint from past OOM
        #  - estimate if present (padded)
        #  - otherwise small default
        key = _op_key(pipeline.pipeline_id, op)
        learned = s.op_ram_hint_gb.get(key)

        est = None
        if hasattr(op, "estimate") and op.estimate is not None and hasattr(op.estimate, "mem_peak_gb"):
            try:
                est = op.estimate.mem_peak_gb
            except Exception:
                est = None

        candidates = []
        if learned is not None:
            candidates.append(float(learned))
        if est is not None:
            try:
                candidates.append(float(est) * s.mem_est_pad)
            except Exception:
                pass

        if candidates:
            req = max(candidates)
        else:
            # Default: small-but-not-tiny RAM request, biased by priority
            if pipeline.priority in (Priority.QUERY, Priority.INTERACTIVE):
                req = max(s.min_ram, 0.15 * pool.max_ram_pool)
            else:
                req = max(s.min_ram, 0.10 * pool.max_ram_pool)

        # Never request more than the pool currently has available
        return min(req, pool.avail_ram_pool)

    # ---- Ingest new pipelines ----
    for p in pipelines:
        _ensure_prio_queue_exists(p.priority)
        s.waiting_by_prio[p.priority].append(p)

    # ---- Learn from results (OOM backoff) ----
    # If an op failed with OOM, increase its future RAM request hint.
    for r in results:
        if not hasattr(r, "ops") or not r.ops:
            continue
        if not r.failed():
            continue
        if not _is_oom_error(getattr(r, "error", None)):
            continue

        # Results contain the ops in that container. Learn per-op within that assignment.
        for op in r.ops:
            # Pipeline id is not on ExecutionResult in the provided interface; best effort:
            # - if op has pipeline_id, use it
            # - else we cannot key precisely; fall back to a global key using "unknown"
            pipeline_id = getattr(op, "pipeline_id", "unknown")
            key = (str(pipeline_id), _get_op_id(op))

            # If we know what RAM was used, back off from that; else use a minimal bump.
            used_ram = getattr(r, "ram", None)
            try:
                used_ram = float(used_ram) if used_ram is not None else None
            except Exception:
                used_ram = None

            prev = s.op_ram_hint_gb.get(key, used_ram if used_ram is not None else s.min_ram)
            try:
                prev = float(prev)
            except Exception:
                prev = s.min_ram

            new_hint = max(prev * s.oom_backoff, prev + s.min_ram)
            s.op_ram_hint_gb[key] = new_hint

    # Early exit if nothing changed
    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    # ---- Scheduling loop: one op per pool per tick ----
    # Highest to lowest priority; FIFO within each class.
    prio_order = sorted(list(s.waiting_by_prio.keys()), key=_prio_key)

    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool
        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        reserve_cpu, reserve_ram = _reserve_for_interactive(pool)

        chosen_pipeline = None
        chosen_op = None

        # Try to find the best (highest priority) pipeline that has an assignable op.
        # Also apply a simple reservation rule: do not schedule BATCH if it would consume
        # into reserved headroom (i.e., only schedule BATCH when there's ample free capacity).
        for prio in prio_order:
            q = s.waiting_by_prio.get(prio, [])
            if not q:
                continue

            # Scan FIFO, but allow skipping completed/failed ones (and keep the rest in order)
            scanned = 0
            q_len = len(q)
            while scanned < q_len:
                p = q.pop(0)
                scanned += 1

                # Drop completed/failed pipelines
                if _pipeline_done_or_failed(p):
                    continue

                ops = _get_assignable_ops(p)
                if not ops:
                    # Not ready yet; keep it in the queue
                    q.append(p)
                    continue

                # Reservation rule for batch: keep headroom for interactive/query
                if p.priority == Priority.BATCH_PIPELINE:
                    if avail_cpu <= reserve_cpu or avail_ram <= reserve_ram:
                        # Not enough headroom; keep batch waiting
                        q.append(p)
                        continue

                chosen_pipeline = p
                chosen_op = ops[0]
                # Put pipeline back to the end so it can progress fairly over time
                q.append(p)
                break

            if chosen_pipeline is not None:
                break

        if chosen_pipeline is None or chosen_op is None:
            continue

        # Determine resource request (conservative, capped CPU)
        cpu_cap = _cpu_cap_for_prio(pool, chosen_pipeline.priority)
        cpu_req = min(avail_cpu, cpu_cap)
        cpu_req = max(s.min_cpu, cpu_req)

        ram_req = _ram_request_for_op(pool, chosen_pipeline, chosen_op)
        ram_req = max(s.min_ram, ram_req)

        # If, after computing our request, we can't fit even minimums, skip
        if cpu_req <= 0 or ram_req <= 0 or cpu_req > avail_cpu or ram_req > avail_ram:
            continue

        assignments.append(
            Assignment(
                ops=[chosen_op],
                cpu=cpu_req,
                ram=ram_req,
                priority=chosen_pipeline.priority,
                pool_id=pool_id,
                pipeline_id=chosen_pipeline.pipeline_id,
            )
        )

    return suspensions, assignments
