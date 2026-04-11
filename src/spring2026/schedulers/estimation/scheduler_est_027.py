# policy_key: scheduler_est_027
# reasoning_effort: low
# model: gpt-5.2-2025-12-11
# llm_cost: 0.037806
# generation_seconds: 41.98
# generated_at: 2026-04-01T00:02:50.128414
@register_scheduler_init(key="scheduler_est_027")
def scheduler_est_027_init(s):
    """
    Priority-aware FIFO with small, incremental improvements over naive FIFO:
      1) Maintain separate waiting queues per priority (QUERY/INTERACTIVE/BATCH).
      2) Use simple aging to avoid starvation (older items get promoted within their class).
      3) Size RAM using op.estimate.mem_peak_gb as a floor (+ retry-based multiplier on OOM).
      4) Reserve headroom for high-priority work by limiting batch from consuming the last slice.
      5) Assign at most one operator per pipeline at a time (keeps fairness, reduces HoL blocking).
    """
    s.tick = 0

    # Waiting entries: dict(pipeline=Pipeline, enq_tick=int, last_scheduled_tick=int|None)
    s.wait_q = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }

    # Per-operator memory multiplier bumped on OOM (keyed by best-effort operator identity)
    s.mem_mult = {}  # op_key -> float

    # Pipelines we consider terminally failed (non-OOM) to avoid infinite retries
    s.terminal_failed = set()  # pipeline_id

    # Config knobs (kept conservative; incremental over naive FIFO)
    s.cfg = {
        # Batch cannot consume below this fraction of pool resources (keeps headroom for latency work)
        "batch_reserve_cpu_frac": 0.20,
        "batch_reserve_ram_frac": 0.20,
        # Aging: every N ticks waiting increases scheduling score
        "aging_ticks": 10,
        # Memory sizing
        "default_mem_gb": 1.0,
        "mem_headroom_mult": 1.20,  # extra RAM is "free" in the model; allocate a bit above estimate
        "oom_bump_mult": 1.60,       # bump on OOM
        "oom_bump_mult_cap": 8.0,    # cap to avoid runaway
        # CPU sizing
        "hp_cpu_target_frac": 0.75,  # high-priority tries to take most of available CPU (latency)
        "batch_cpu_target_frac": 0.35,
        "min_cpu": 1.0,
        # Limits
        "max_assignments_per_pool_per_tick": 4,  # allow a few ops per tick if capacity exists
    }


@register_scheduler(key="scheduler_est_027")
def scheduler_est_027_scheduler(s, results, pipelines):
    """
    Priority-aware scheduler:
      - Enqueue incoming pipelines into per-priority queues.
      - Process execution results to detect OOM and bump per-op RAM multiplier, allowing retry.
      - For each pool, repeatedly pick the "best" runnable op across queues (priority + aging),
        and assign it with RAM floor based on estimate and CPU sized by priority class.
    """
    s.tick += 1

    suspensions = []
    assignments = []

    # --- Helpers (kept local to avoid imports/global deps) ---
    def _is_oom_error(err):
        if err is None:
            return False
        try:
            msg = str(err).lower()
        except Exception:
            return False
        return ("oom" in msg) or ("out of memory" in msg) or ("killed" in msg and "memory" in msg)

    def _op_key(pipeline_id, op):
        # Best-effort stable key across ticks; fall back to id(op)
        for attr in ("op_id", "operator_id", "id", "name"):
            if hasattr(op, attr):
                try:
                    v = getattr(op, attr)
                    # If attribute is callable (e.g., id()), call it; else use directly
                    v = v() if callable(v) else v
                    return (pipeline_id, attr, v)
                except Exception:
                    pass
        return (pipeline_id, "pyid", id(op))

    def _mem_floor_gb(op, pipeline_id):
        est = None
        try:
            est = getattr(getattr(op, "estimate", None), "mem_peak_gb", None)
        except Exception:
            est = None
        if est is None:
            est = s.cfg["default_mem_gb"]
        try:
            est = float(est)
        except Exception:
            est = s.cfg["default_mem_gb"]
        est = max(est, 0.1)

        k = _op_key(pipeline_id, op)
        mult = s.mem_mult.get(k, 1.0)
        mult = max(1.0, min(mult, s.cfg["oom_bump_mult_cap"]))
        return est * s.cfg["mem_headroom_mult"] * mult

    def _cpu_request(avail_cpu, pool_max_cpu, prio):
        min_cpu = s.cfg["min_cpu"]
        if avail_cpu <= 0:
            return 0.0
        if prio in (Priority.QUERY, Priority.INTERACTIVE):
            # Favor latency: grab a large slice of currently available CPU
            target = max(min_cpu, avail_cpu * s.cfg["hp_cpu_target_frac"])
        else:
            # Favor sharing: smaller slices for batch
            target = max(min_cpu, avail_cpu * s.cfg["batch_cpu_target_frac"])
        # Never exceed what's available; also keep within pool maxima for sanity
        return max(0.0, min(float(avail_cpu), float(pool_max_cpu), float(target)))

    def _get_assignable_op(pipeline):
        status = pipeline.runtime_status()
        # Skip completed or terminal-failed pipelines
        if status.is_pipeline_successful():
            return None
        if pipeline.pipeline_id in s.terminal_failed:
            return None

        # Require parents complete: standard DAG semantics
        ops = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        if not ops:
            return None
        # Only schedule one op per pipeline at a time for fairness
        return ops[0]

    def _queue_for_priority(prio):
        # Robustness if unknown priority shows up
        if prio in s.wait_q:
            return s.wait_q[prio]
        return s.wait_q[Priority.BATCH_PIPELINE]

    def _effective_score(entry, prio):
        # Lower is better: (priority rank, -age buckets, enq_tick)
        # Priority rank: QUERY (0) < INTERACTIVE (1) < BATCH (2)
        prio_rank = {
            Priority.QUERY: 0,
            Priority.INTERACTIVE: 1,
            Priority.BATCH_PIPELINE: 2,
        }.get(prio, 3)

        age = max(0, s.tick - entry["enq_tick"])
        age_buckets = age // max(1, s.cfg["aging_ticks"])
        # More age => earlier scheduling (negative bucket)
        return (prio_rank, -age_buckets, entry["enq_tick"])

    # --- Ingest new pipelines ---
    for p in pipelines:
        q = _queue_for_priority(p.priority)
        q.append({"pipeline": p, "enq_tick": s.tick, "last_scheduled_tick": None})

    # --- Process results (track OOM and terminal failures) ---
    if results:
        for r in results:
            if not r.failed():
                continue

            # Associate the failure to its ops; bump mem multiplier on OOM to allow retry.
            oom = _is_oom_error(getattr(r, "error", None))
            ops = getattr(r, "ops", None) or []
            # Try to find a pipeline_id; results may not include it, so we can only bump by op object
            # using a generic key if pipeline_id is unavailable.
            # In practice, ops are tied to a pipeline object in queues/status; fallback still helps.
            for op in ops:
                # Use unknown pipeline_id marker if not available
                k = _op_key(getattr(op, "pipeline_id", "unknown"), op)
                if oom:
                    cur = s.mem_mult.get(k, 1.0)
                    nxt = min(cur * s.cfg["oom_bump_mult"], s.cfg["oom_bump_mult_cap"])
                    s.mem_mult[k] = nxt

            # If not OOM, mark as terminal failed to avoid infinite retries.
            if not oom:
                # Best effort: attempt to find pipeline_id on ops, else skip marking.
                marked = False
                for op in ops:
                    pid = getattr(op, "pipeline_id", None)
                    if pid is not None:
                        s.terminal_failed.add(pid)
                        marked = True
                # If we can't attribute, we still proceed without terminal marking.

    # --- Scheduling loop per pool ---
    # We will attempt multiple assignments per pool, respecting headroom and availability.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]

        made = 0
        while made < s.cfg["max_assignments_per_pool_per_tick"]:
            avail_cpu = float(pool.avail_cpu_pool)
            avail_ram = float(pool.avail_ram_pool)
            if avail_cpu <= 0.0 or avail_ram <= 0.0:
                break

            # Compute batch headroom limits (do not consume the last reserve slice)
            batch_cpu_limit = max(0.0, float(pool.max_cpu_pool) * (1.0 - s.cfg["batch_reserve_cpu_frac"]))
            batch_ram_limit = max(0.0, float(pool.max_ram_pool) * (1.0 - s.cfg["batch_reserve_ram_frac"]))

            # Choose best candidate across queues by priority+aging, but only if runnable and fits.
            best = None  # (score_tuple, entry, op, prio, cpu_req, ram_req)
            for prio, q in s.wait_q.items():
                # Fast skip: no candidates
                if not q:
                    continue

                # Consider a small prefix to keep it cheap (incremental improvement)
                # We will scan up to 8 entries per queue to find a runnable op.
                scan_n = min(len(q), 8)
                for idx in range(scan_n):
                    entry = q[idx]
                    pipeline = entry["pipeline"]

                    op = _get_assignable_op(pipeline)
                    if op is None:
                        continue

                    ram_req = _mem_floor_gb(op, pipeline.pipeline_id)
                    if ram_req > avail_ram:
                        continue

                    # Apply batch headroom constraints
                    if prio == Priority.BATCH_PIPELINE:
                        # If we're already below headroom, do not schedule more batch.
                        # (Using "available" resources as a proxy for "remaining headroom".)
                        # Equivalent: only schedule batch if we still have enough slack left.
                        if avail_cpu < (float(pool.max_cpu_pool) - batch_cpu_limit) + s.cfg["min_cpu"]:
                            continue
                        if avail_ram < (float(pool.max_ram_pool) - batch_ram_limit) + ram_req * 0.1:
                            continue

                    cpu_req = _cpu_request(avail_cpu, float(pool.max_cpu_pool), prio)
                    if cpu_req <= 0.0:
                        continue

                    score = _effective_score(entry, prio)
                    cand = (score, idx, entry, op, prio, cpu_req, ram_req)
                    if best is None or cand[0] < best[0]:
                        best = cand

            if best is None:
                break

            _, idx, entry, op, prio, cpu_req, ram_req = best
            pipeline = entry["pipeline"]

            # Remove chosen entry from its queue (we'll requeue it after attempting an assignment)
            q = _queue_for_priority(prio)
            try:
                q.pop(idx)
            except Exception:
                # If the queue mutated unexpectedly, just stop scheduling this pool this tick.
                break

            # Create assignment for the single operator
            assignments.append(
                Assignment(
                    ops=[op],
                    cpu=cpu_req,
                    ram=ram_req,
                    priority=pipeline.priority,
                    pool_id=pool_id,
                    pipeline_id=pipeline.pipeline_id,
                )
            )

            # Requeue the pipeline entry so it can run subsequent operators later
            entry["last_scheduled_tick"] = s.tick
            q.append(entry)

            made += 1

    return suspensions, assignments
