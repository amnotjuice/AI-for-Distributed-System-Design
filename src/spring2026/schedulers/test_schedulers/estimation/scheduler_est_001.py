# policy_key: scheduler_est_001
# reasoning_effort: low
# exp: estimation
# model: gpt-5.2-2025-12-11
# llm_cost: 0.000000
# generation_seconds: 38.58
# generated_at: 2026-04-10T07:46:40.418180
@register_scheduler_init(key="scheduler_est_001")
def scheduler_est_001_init(s):
    """Priority + memory-aware, failure-avoiding scheduler.

    Core ideas:
      - Protect QUERY and INTERACTIVE latency via strict priority ordering.
      - Use estimator hint (op.estimate.mem_peak_gb) to avoid obvious OOM placements.
      - On failures, retry with increased RAM request (bounded by pool max RAM).
      - Keep fairness by limiting to 1 concurrent assignment per pipeline per tick.
      - Pack more work when possible by choosing "fits-best" ops per pool.
    """
    s.waiting_queue = []  # FIFO of pipelines (arrival order); we will prioritize at dispatch time
    s.pipeline_info = {}  # pipeline_id -> dict with retry/backoff info
    s.max_assignments_per_tick_per_pool = 8  # keep bounded work per decision tick
    s.safety_ram_gb = 0.25  # additive safety margin (GB)
    s.est_overcommit_factor = 1.25  # multiplicative safety on estimated peak memory
    s.fail_ram_multiplier_step = 1.6  # how much to increase RAM request after a failure
    s.fail_ram_multiplier_cap = 16.0  # cap multiplier to avoid runaway
    s.default_ram_fraction_of_pool = {  # fallback when estimate is missing
        Priority.QUERY: 0.60,
        Priority.INTERACTIVE: 0.50,
        Priority.BATCH_PIPELINE: 0.35,
    }
    s.cpu_cap = {  # keep some sharing while still protecting high priority
        Priority.QUERY: 4.0,
        Priority.INTERACTIVE: 2.0,
        Priority.BATCH_PIPELINE: 1.0,
    }
    s.cpu_floor = {
        Priority.QUERY: 1.0,
        Priority.INTERACTIVE: 1.0,
        Priority.BATCH_PIPELINE: 0.5,
    }


@register_scheduler(key="scheduler_est_001")
def scheduler_est_001_scheduler(s, results: list, pipelines: list):
    """
    Scheduler loop:
      1) Ingest arrivals into a persistent waiting list.
      2) Update per-pipeline failure-driven RAM multipliers from execution results.
      3) For each pool, greedily assign ready operators by descending priority, choosing
         ops that "fit" the pool RAM (using estimator when present) and that request
         minimal RAM among candidates to increase throughput.
    """
    # -------- helpers --------
    def _prio_rank(prio):
        if prio == Priority.QUERY:
            return 0
        if prio == Priority.INTERACTIVE:
            return 1
        return 2  # batch

    def _get_info(pipeline_id):
        info = s.pipeline_info.get(pipeline_id)
        if info is None:
            info = {
                "ram_mult": 1.0,          # escalates on failures to reduce repeated OOM
                "recent_failures": 0,     # counts consecutive failures (any type)
            }
            s.pipeline_info[pipeline_id] = info
        return info

    def _op_est_mem(op):
        est = getattr(op, "estimate", None)
        if est is None:
            return None
        mem = getattr(est, "mem_peak_gb", None)
        try:
            if mem is None:
                return None
            mem = float(mem)
            if mem < 0:
                return None
            return mem
        except Exception:
            return None

    def _pipeline_done_or_failed(pipeline):
        status = pipeline.runtime_status()
        if status.is_pipeline_successful():
            return True
        # If any operator is FAILED, we still want to retry; do not drop the pipeline here.
        return False

    def _pipeline_has_assignable_op(pipeline):
        status = pipeline.runtime_status()
        op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        return op_list[0] if op_list else None

    def _requested_ram_gb(pipeline, op, pool):
        """Compute RAM request for this op on this pool. Return None if impossible on this pool."""
        info = _get_info(pipeline.pipeline_id)
        ram_mult = info["ram_mult"]

        est_mem = _op_est_mem(op)
        if est_mem is not None:
            # Conservative request around estimated peak; add headroom and failure backoff.
            req = (est_mem * s.est_overcommit_factor + s.safety_ram_gb) * ram_mult
            # If estimate suggests it cannot ever fit in this pool, skip it for this pool.
            if est_mem > (pool.max_ram_pool * 0.98):
                return None
        else:
            # Fallback: take a fraction of pool max RAM (not avail), then scale with failure backoff.
            frac = s.default_ram_fraction_of_pool.get(pipeline.priority, 0.40)
            req = (pool.max_ram_pool * frac + s.safety_ram_gb) * ram_mult

        # Clamp to pool max; if clamped too low vs estimate, prefer skipping to avoid likely OOM.
        req = min(req, pool.max_ram_pool)

        # Must fit current availability in the pool.
        if req > pool.avail_ram_pool:
            return None

        # Ensure some minimum > 0 to avoid degenerate allocations.
        if req <= 0:
            return None

        return req

    def _requested_cpu(pipeline, pool):
        # Keep CPU modest to allow packing; prioritize latency with higher caps for queries.
        cap = s.cpu_cap.get(pipeline.priority, 1.0)
        floor = s.cpu_floor.get(pipeline.priority, 0.5)
        cpu = min(pool.avail_cpu_pool, cap)
        if cpu < floor:
            return None
        return cpu

    # -------- ingest arrivals --------
    for p in pipelines:
        s.waiting_queue.append(p)
        _get_info(p.pipeline_id)  # ensure info exists for backoff bookkeeping

    # Early exit if nothing changed.
    if not pipelines and not results:
        return [], []

    # -------- incorporate results / failure backoff --------
    # If a pipeline experiences failures, increase RAM multiplier to avoid repeated OOM.
    for r in results:
        try:
            if not r.failed():
                # Success: gently decay failure streak; do not aggressively reduce ram_mult to avoid flapping.
                info = _get_info(getattr(r, "pipeline_id", None)) if hasattr(r, "pipeline_id") else None
                if info is not None:
                    info["recent_failures"] = max(0, info["recent_failures"] - 1)
                continue
        except Exception:
            pass

        # We do not always have pipeline_id in ExecutionResult; try to infer via attached ops if possible.
        pid = None
        if hasattr(r, "pipeline_id"):
            pid = r.pipeline_id
        else:
            # Best-effort: if ops exist and carry pipeline_id
            try:
                if r.ops and hasattr(r.ops[0], "pipeline_id"):
                    pid = r.ops[0].pipeline_id
            except Exception:
                pid = None

        if pid is None:
            continue

        info = _get_info(pid)
        info["recent_failures"] += 1

        # Escalate RAM multiplier; cap to avoid runaway.
        info["ram_mult"] = min(info["ram_mult"] * s.fail_ram_multiplier_step, s.fail_ram_multiplier_cap)

    # -------- build candidate set (do not drop pipelines; keep them in queue) --------
    # We'll process a snapshot of waiting_queue and re-append survivors to preserve FIFO within same priority.
    active = []
    for p in s.waiting_queue:
        if _pipeline_done_or_failed(p):
            # If completed, keep it out of queue; if failed operators exist, we still keep pipeline to retry.
            # Note: _pipeline_done_or_failed returns True only for successful completion here.
            continue
        active.append(p)
    s.waiting_queue = active

    suspensions = []
    assignments = []

    # Track whether we've already assigned an op from a pipeline this tick (fairness).
    assigned_pipeline_ids = set()

    # -------- dispatch per pool --------
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        if pool.avail_cpu_pool <= 0 or pool.avail_ram_pool <= 0:
            continue

        # Greedy loop with bounded assignments per pool per tick.
        for _ in range(s.max_assignments_per_tick_per_pool):
            if pool.avail_cpu_pool <= 0 or pool.avail_ram_pool <= 0:
                break

            # Build candidate list of (priority_rank, est_mem_sort_key, req_ram, req_cpu, pipeline, op)
            candidates = []
            for p in s.waiting_queue:
                if p.pipeline_id in assigned_pipeline_ids:
                    continue

                op = _pipeline_has_assignable_op(p)
                if op is None:
                    continue

                req_cpu = _requested_cpu(p, pool)
                if req_cpu is None:
                    continue

                req_ram = _requested_ram_gb(p, op, pool)
                if req_ram is None:
                    continue

                est_mem = _op_est_mem(op)
                # Sort key: smaller estimated footprint first (helps packing), but still strict by priority.
                # If estimate missing, treat as moderately large to avoid blocking many small ops.
                est_key = est_mem if est_mem is not None else (pool.max_ram_pool * 0.65)
                candidates.append((_prio_rank(p.priority), est_key, req_ram, req_cpu, p, op))

            if not candidates:
                break

            # Choose best candidate: highest priority (lowest rank), then smallest estimated mem, then smallest req_ram.
            candidates.sort(key=lambda x: (x[0], x[1], x[2]))
            _, _, req_ram, req_cpu, pipeline, op = candidates[0]

            # Final safety check vs current availability (may have changed in loop).
            if req_cpu > pool.avail_cpu_pool or req_ram > pool.avail_ram_pool:
                break

            # Assign exactly one operator at a time for the chosen pipeline.
            assignments.append(
                Assignment(
                    ops=[op],
                    cpu=req_cpu,
                    ram=req_ram,
                    priority=pipeline.priority,
                    pool_id=pool_id,
                    pipeline_id=pipeline.pipeline_id,
                )
            )

            assigned_pipeline_ids.add(pipeline.pipeline_id)

            # Optimistically update local view to pack more in this tick (simulator will enforce real limits).
            try:
                pool.avail_cpu_pool -= req_cpu
                pool.avail_ram_pool -= req_ram
            except Exception:
                # If pool fields are read-only, ignore; next loop iteration will be conservative.
                pass

    return suspensions, assignments
