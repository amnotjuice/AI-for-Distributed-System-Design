# policy_key: scheduler_est_023
# reasoning_effort: low
# model: gpt-5.2-2025-12-11
# llm_cost: 0.039738
# generation_seconds: 35.98
# generated_at: 2026-04-01T00:00:05.463936
@register_scheduler_init(key="scheduler_est_023")
def scheduler_est_023_init(s):
    """
    Priority-aware, right-sized scheduling (small step up from naive FIFO).

    Key improvements vs naive:
    - Maintain separate waiting queues by priority and always schedule highest priority first.
    - Avoid head-of-line blocking by scanning for runnable (parents-complete) ops.
    - Right-size CPU/RAM per op rather than giving the entire pool to one op.
    - Use op.estimate.mem_peak_gb as a RAM floor; on suspected OOM, increase RAM floor on retry.
    """
    # Pipelines waiting to run (per priority).
    s.waiting = {
        Priority.INTERACTIVE: [],
        Priority.QUERY: [],
        Priority.BATCH_PIPELINE: [],
    }

    # Per-operator memory floor overrides learned from failures/success.
    # Keyed by (pipeline_id, id(op)) -> mem_gb_floor
    s.mem_floor_gb = {}

    # Simple attempt counter per op to dampen repeated OOMs.
    s.attempts = {}

    # Tunables (kept intentionally simple)
    s.defaults = {
        Priority.INTERACTIVE: {"cpu": 4.0, "ram_gb": 8.0},
        Priority.QUERY: {"cpu": 2.0, "ram_gb": 6.0},
        Priority.BATCH_PIPELINE: {"cpu": 1.0, "ram_gb": 4.0},
    }

    # How aggressively to bump RAM on OOM-like failures.
    s.oom_bump_mult = 1.5
    s.oom_bump_add_gb = 2.0

    # Keep some headroom to reduce fragmentation / leave room for small interactive bursts.
    # (We only enforce this on lower priorities.)
    s.batch_reserved_cpu = 0.0
    s.batch_reserved_ram_gb = 0.0

    # Minimum allocs (avoid tiny containers).
    s.min_cpu = 0.5
    s.min_ram_gb = 1.0


@register_scheduler(key="scheduler_est_023")
def scheduler_est_023_scheduler(s, results, pipelines):
    """
    Priority-first scheduling with simple resource sizing and OOM-aware RAM floors.

    Strategy per tick:
    1) Ingest new pipelines into priority queues.
    2) Update per-op memory floors based on execution results (OOM bumps).
    3) For each pool, greedily place runnable ops from highest to lowest priority,
       allocating modest CPU and enough RAM to avoid OOM (estimate/learned floor).
    """
    # Helper: normalize mem estimate
    def _est_mem_gb(op):
        est = None
        try:
            est = getattr(getattr(op, "estimate", None), "mem_peak_gb", None)
        except Exception:
            est = None
        if est is None:
            return None
        try:
            est = float(est)
        except Exception:
            return None
        if est <= 0:
            return None
        return est

    def _is_oom_error(err):
        if err is None:
            return False
        try:
            msg = str(err).lower()
        except Exception:
            return False
        return ("oom" in msg) or ("out of memory" in msg) or ("memoryerror" in msg)

    def _op_key(pipeline, op):
        return (pipeline.pipeline_id, id(op))

    # 1) Enqueue new pipelines by priority
    for p in pipelines:
        pr = p.priority
        if pr not in s.waiting:
            # Unknown priority: treat as batch-like
            pr = Priority.BATCH_PIPELINE
        s.waiting[pr].append(p)

    # Early exit if nothing changed
    if not pipelines and not results:
        return [], []

    # 2) Learn from results
    for r in results:
        # We only get ops and pipeline priority, but not the pipeline_id directly.
        # The op objects in r.ops should be the same objects we see via runtime_status().get_ops().
        if not getattr(r, "failed", lambda: False)():
            # Success: if scheduler allocated some RAM, keep as a (soft) floor for next time.
            # (This is conservative; success doesn't prove peak <= ram, but helps stabilize.)
            try:
                ram_gb = float(getattr(r, "ram", 0.0) or 0.0)
            except Exception:
                ram_gb = 0.0
            if ram_gb > 0:
                for op in getattr(r, "ops", []) or []:
                    # We cannot map to pipeline_id reliably from ExecutionResult, so store by id(op) only.
                    # This still helps within the simulation because op objects are stable.
                    k = ("__any_pipeline__", id(op))
                    prev = s.mem_floor_gb.get(k, 0.0)
                    if ram_gb > prev:
                        s.mem_floor_gb[k] = ram_gb
            continue

        # Failure: if likely OOM, bump RAM floor for those ops
        if _is_oom_error(getattr(r, "error", None)):
            try:
                prev_ram = float(getattr(r, "ram", 0.0) or 0.0)
            except Exception:
                prev_ram = 0.0
            bump = max(prev_ram * s.oom_bump_mult, prev_ram + s.oom_bump_add_gb, s.min_ram_gb)
            for op in getattr(r, "ops", []) or []:
                k = ("__any_pipeline__", id(op))
                cur = s.mem_floor_gb.get(k, 0.0)
                if bump > cur:
                    s.mem_floor_gb[k] = bump

                akey = ("__any_pipeline__", id(op))
                s.attempts[akey] = s.attempts.get(akey, 0) + 1

    suspensions = []
    assignments = []

    # 3) Schedule per pool
    pr_order = [Priority.INTERACTIVE, Priority.QUERY, Priority.BATCH_PIPELINE]

    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = float(getattr(pool, "avail_cpu_pool", 0.0) or 0.0)
        avail_ram = float(getattr(pool, "avail_ram_pool", 0.0) or 0.0)

        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # Greedily pack multiple ops into the pool this tick.
        # Stop when we can't fit even a minimal container.
        while avail_cpu >= s.min_cpu and avail_ram >= s.min_ram_gb:
            picked = None  # (priority, pipeline, op)

            # Pick the highest priority runnable op across queued pipelines.
            for pr in pr_order:
                q = s.waiting.get(pr, [])
                if not q:
                    continue

                # Scan a small window to reduce head-of-line blocking without O(n) each tick.
                scan_n = min(16, len(q))
                found_idx = None
                found_op = None
                found_pipe = None

                for i in range(scan_n):
                    pipe = q[i]
                    status = pipe.runtime_status()

                    # Drop finished pipelines
                    if status.is_pipeline_successful():
                        continue

                    # If pipeline has failures, allow retry (FAILED is in ASSIGNABLE_STATES),
                    # but only for ops whose parents are complete.
                    op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
                    if not op_list:
                        continue

                    # Take just one op at a time to keep allocations granular.
                    found_idx = i
                    found_pipe = pipe
                    found_op = op_list[0]
                    break

                if found_pipe is not None:
                    # Rotate chosen pipeline to the back (round-robin within priority)
                    q.pop(found_idx)
                    q.append(found_pipe)
                    picked = (pr, found_pipe, found_op)
                    break

            if picked is None:
                break

            pr, pipe, op = picked

            # Compute RAM floor: max(default, estimate, learned).
            default_ram = float(s.defaults.get(pr, {}).get("ram_gb", 4.0))
            est = _est_mem_gb(op)
            learned_any = s.mem_floor_gb.get(("__any_pipeline__", id(op)), 0.0)

            ram_floor = default_ram
            if est is not None:
                ram_floor = max(ram_floor, est)
            if learned_any:
                ram_floor = max(ram_floor, learned_any)

            # If we have repeated OOM attempts, add a small extra cushion.
            attempts = s.attempts.get(("__any_pipeline__", id(op)), 0)
            if attempts >= 2:
                ram_floor = ram_floor + 1.0

            # Don't allocate more RAM than available; if we can't satisfy floor, skip for now
            # (leave it queued; maybe another pool/tick has more headroom).
            if ram_floor > avail_ram:
                # Push pipeline back (already rotated); stop packing this pool this tick.
                # This avoids spinning on an unschedulable op while resources are low.
                break

            # Compute CPU request: modest per priority, but no more than what's left.
            default_cpu = float(s.defaults.get(pr, {}).get("cpu", 1.0))
            cpu_req = max(s.min_cpu, min(default_cpu, avail_cpu))

            # For lower priority work, keep a little headroom if interactive/query work exists.
            if pr == Priority.BATCH_PIPELINE:
                hi_backlog = (len(s.waiting.get(Priority.INTERACTIVE, [])) + len(s.waiting.get(Priority.QUERY, []))) > 0
                if hi_backlog:
                    # Keep at least 1 CPU and 2GB RAM unallocated when possible.
                    cpu_req = min(cpu_req, max(s.min_cpu, avail_cpu - 1.0))
                    if cpu_req < s.min_cpu:
                        break

            # Make the assignment (single-op container)
            assignment = Assignment(
                ops=[op],
                cpu=cpu_req,
                ram=ram_floor,
                priority=pipe.priority,
                pool_id=pool_id,
                pipeline_id=pipe.pipeline_id,
            )
            assignments.append(assignment)

            # Update available resources for further packing
            avail_cpu -= cpu_req
            avail_ram -= ram_floor

    return suspensions, assignments
