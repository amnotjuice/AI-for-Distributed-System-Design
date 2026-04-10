# policy_key: scheduler_high_050
# reasoning_effort: high
# model: gpt-5.2-2025-12-11
# llm_cost: 0.124991
# generation_seconds: 139.87
# generated_at: 2026-03-14T04:39:46.903459
@register_scheduler_init(key="scheduler_high_050")
def scheduler_high_050_init(s):
    """
    Priority-aware, latency-oriented scheduler (incremental improvement over naive FIFO).

    Main ideas:
      1) Priority queues: always try QUERY/INTERACTIVE before BATCH.
      2) Avoid "give everything to one op": cap CPU/RAM per assignment to reduce head-of-line blocking.
      3) Simple OOM learning: if an op fails with OOM, increase the pipeline's RAM factor for retries.
      4) If multiple pools exist, prefer pool 0 for high-priority work (soft "interactive pool").
      5) Keep a small headroom ("reserve") in shared pools when high-priority work is waiting, so batch
         doesn't consume all resources and spike tail latency.
    """
    from collections import deque

    # Pipeline registry so we can de-dupe arrivals and clean up completed pipelines.
    s.pipelines_by_id = {}  # pipeline_id -> Pipeline

    # Per-priority round-robin queues of pipeline_ids.
    s.q_query = deque()
    s.q_interactive = deque()
    s.q_batch = deque()

    # Lightweight per-pipeline learned hints and failure tracking.
    # pipeline_id -> {
    #   "ram_factor": float,        # multiplicative factor applied to base RAM fraction on assignment
    #   "oom_retries": int,         # count of OOM-based retries
    #   "last_failure_oom": bool,   # last failure was OOM (so retrying FAILED ops is allowed)
    #   "drop": bool               # permanently drop pipeline from scheduling
    # }
    s.meta = {}

    s._tick = 0


@register_scheduler(key="scheduler_high_050")
def scheduler_high_050_scheduler(s, results, pipelines):
    """
    Scheduling step.

    - Enqueue new pipelines into priority-specific RR queues.
    - Process results to learn from OOM failures (increase RAM factor).
    - Schedule in two phases:
        Phase A: schedule high-priority (QUERY, INTERACTIVE) across pools (pool 0 preferred).
        Phase B: schedule BATCH across pools, keeping headroom if high-priority work is waiting.
    """
    s._tick += 1

    def _prio_queue(priority):
        if priority == Priority.QUERY:
            return s.q_query
        if priority == Priority.INTERACTIVE:
            return s.q_interactive
        return s.q_batch

    def _ensure_meta(pipeline_id):
        if pipeline_id not in s.meta:
            s.meta[pipeline_id] = {
                "ram_factor": 1.0,
                "oom_retries": 0,
                "last_failure_oom": False,
                "drop": False,
            }
        return s.meta[pipeline_id]

    def _is_oom_error(err):
        if err is None:
            return False
        try:
            return "oom" in str(err).lower() or "out of memory" in str(err).lower()
        except Exception:
            return False

    def _cleanup_pipeline(pipeline_id):
        # Remove from registry/meta; queue entries are lazily skipped.
        s.pipelines_by_id.pop(pipeline_id, None)
        s.meta.pop(pipeline_id, None)

    def _pipeline_done_or_dropped(pipeline):
        pid = pipeline.pipeline_id
        meta = s.meta.get(pid)
        if meta and meta.get("drop", False):
            return True
        status = pipeline.runtime_status()
        if status.is_pipeline_successful():
            return True
        # If something failed and it wasn't an OOM we decided to retry, drop it to avoid infinite loops.
        has_failures = status.state_counts[OperatorState.FAILED] > 0
        if has_failures and not (meta and meta.get("last_failure_oom", False)):
            return True
        return False

    def _base_fracs(priority):
        # Conservative caps to improve latency vs. the naive "take all resources".
        # (CPU, RAM) expressed as fractions of pool max.
        if priority == Priority.QUERY:
            return 0.60, 0.30
        if priority == Priority.INTERACTIVE:
            return 0.60, 0.35
        # Batch starts smaller for better concurrency; can still fill the pool via multiple assignments.
        return 0.70, 0.25

    def _compute_request(priority, pool, rem_cpu, rem_ram, meta, reserve_cpu=0, reserve_ram=0, is_batch=False):
        max_cpu = pool.max_cpu_pool
        max_ram = pool.max_ram_pool
        cpu_frac, ram_frac = _base_fracs(priority)

        # Base request sizes from pool capacity.
        req_cpu = int(round(max_cpu * cpu_frac))
        req_ram = int(round(max_ram * ram_frac * float(meta.get("ram_factor", 1.0))))

        # Enforce a minimum of 1 if any resource exists.
        if rem_cpu > 0:
            req_cpu = max(1, req_cpu)
        if rem_ram > 0:
            req_ram = max(1, req_ram)

        # Respect headroom for batch if high-priority is waiting.
        cap_cpu = rem_cpu
        cap_ram = rem_ram
        if is_batch:
            cap_cpu = max(0, rem_cpu - reserve_cpu)
            cap_ram = max(0, rem_ram - reserve_ram)

        # Clamp to what we can give right now.
        req_cpu = min(req_cpu, cap_cpu)
        req_ram = min(req_ram, cap_ram)

        if req_cpu <= 0 or req_ram <= 0:
            return 0, 0

        # Final clamp to remaining.
        req_cpu = min(req_cpu, rem_cpu)
        req_ram = min(req_ram, rem_ram)
        return req_cpu, req_ram

    def _any_ready_in_queues(priorities, assigned_pipelines):
        # Conservative check: if any pipeline in these queues has a ready ASSIGNABLE op, return True.
        # This helps decide whether to reserve headroom for interactive latency protection.
        queues = []
        for pr in priorities:
            queues.append(_prio_queue(pr))

        for q in queues:
            # Scan up to current queue length; RR order isn't important for this existence check.
            n = len(q)
            for _ in range(n):
                pid = q[0]
                q.rotate(-1)
                if pid in assigned_pipelines:
                    continue
                pipeline = s.pipelines_by_id.get(pid)
                if not pipeline:
                    continue
                meta = s.meta.get(pid)
                if meta and meta.get("drop", False):
                    continue
                status = pipeline.runtime_status()
                if status.is_pipeline_successful():
                    continue
                has_failures = status.state_counts[OperatorState.FAILED] > 0
                if has_failures and not (meta and meta.get("last_failure_oom", False)):
                    continue
                ops = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
                if ops:
                    return True
        return False

    def _pick_next_ready(priorities, assigned_pipelines):
        # Round-robin across queues in priority order; pick the first pipeline with a ready op.
        for pr in priorities:
            q = _prio_queue(pr)
            n = len(q)
            for _ in range(n):
                pid = q.popleft()

                # Skip if already assigned this tick (prevents duplicate scheduling when we do multi-pool).
                if pid in assigned_pipelines:
                    q.append(pid)
                    continue

                pipeline = s.pipelines_by_id.get(pid)
                if pipeline is None:
                    # stale queue entry
                    continue

                meta = _ensure_meta(pid)

                # Drop if completed or irrecoverably failed.
                if _pipeline_done_or_dropped(pipeline):
                    meta["drop"] = True
                    _cleanup_pipeline(pid)
                    continue

                status = pipeline.runtime_status()
                op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
                if not op_list:
                    # Not ready yet; keep it in RR.
                    q.append(pid)
                    continue

                # Keep in RR for future ops; mark this pipeline as assigned for this tick.
                q.append(pid)
                assigned_pipelines.add(pid)
                return pipeline, op_list, pr

        return None, None, None

    # ---- Incorporate new pipelines (de-dupe by pipeline_id) ----
    for p in pipelines:
        pid = p.pipeline_id
        if pid in s.pipelines_by_id:
            continue
        s.pipelines_by_id[pid] = p
        _ensure_meta(pid)
        _prio_queue(p.priority).append(pid)

    # ---- Process results (OOM learning + cleanup) ----
    for r in results:
        # We don't have a direct pipeline_id in the result; use r.ops[0].pipeline_id if present, otherwise skip.
        pid = None
        try:
            if r.ops and hasattr(r.ops[0], "pipeline_id"):
                pid = r.ops[0].pipeline_id
        except Exception:
            pid = None

        if pid is None:
            continue

        meta = _ensure_meta(pid)

        if r.failed():
            if _is_oom_error(getattr(r, "error", None)):
                meta["last_failure_oom"] = True
                meta["oom_retries"] += 1

                # Exponential backoff on RAM factor; cap to avoid runaway.
                # (This is deliberately simple; Eudoxia will let us test and tune caps/factors.)
                meta["ram_factor"] = min(16.0, float(meta.get("ram_factor", 1.0)) * 2.0)

                # If we keep OOMing, eventually drop to avoid infinite retry loops.
                if meta["oom_retries"] >= 6:
                    meta["drop"] = True
                    _cleanup_pipeline(pid)
            else:
                # Non-OOM failures are treated as terminal in this simple policy.
                meta["last_failure_oom"] = False
                meta["drop"] = True
                _cleanup_pipeline(pid)
        else:
            # On success, clear the "retry FAILED ops" flag.
            meta["last_failure_oom"] = False

    # If nothing arrived and no completions/failures, nothing changes (safe early return).
    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    # Local remaining resources per pool so we don't oversubscribe within this tick.
    rem_cpu = []
    rem_ram = []
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        rem_cpu.append(pool.avail_cpu_pool)
        rem_ram.append(pool.avail_ram_pool)

    assigned_pipelines = set()

    # Pool ordering: prefer pool 0 for high-priority if multiple pools exist.
    all_pools = list(range(s.executor.num_pools))
    high_pool_order = all_pools[:]
    if s.executor.num_pools >= 2 and 0 in high_pool_order:
        high_pool_order.remove(0)
        high_pool_order = [0] + high_pool_order

    batch_pool_order = all_pools[:]
    if s.executor.num_pools >= 2 and 0 in batch_pool_order:
        batch_pool_order.remove(0)
        batch_pool_order = batch_pool_order + [0]  # pool 0 last for batch

    high_prios = [Priority.QUERY, Priority.INTERACTIVE]
    batch_prios = [Priority.BATCH_PIPELINE]

    # Phase A: schedule high-priority work first.
    for pool_id in high_pool_order:
        pool = s.executor.pools[pool_id]
        # Keep scheduling while resources remain and we can find ready high-priority ops.
        while rem_cpu[pool_id] > 0 and rem_ram[pool_id] > 0:
            pipeline, op_list, pr = _pick_next_ready(high_prios, assigned_pipelines)
            if pipeline is None:
                break

            pid = pipeline.pipeline_id
            meta = _ensure_meta(pid)

            cpu, ram = _compute_request(
                priority=pr,
                pool=pool,
                rem_cpu=rem_cpu[pool_id],
                rem_ram=rem_ram[pool_id],
                meta=meta,
                reserve_cpu=0,
                reserve_ram=0,
                is_batch=False,
            )
            if cpu <= 0 or ram <= 0:
                # Not enough resources in this pool; allow other pools to try.
                break

            assignments.append(
                Assignment(
                    ops=op_list,
                    cpu=cpu,
                    ram=ram,
                    priority=pipeline.priority,
                    pool_id=pool_id,
                    pipeline_id=pipeline.pipeline_id,
                )
            )
            rem_cpu[pool_id] -= cpu
            rem_ram[pool_id] -= ram

    # Decide if we should reserve headroom for high-priority latency protection.
    # If high-priority work is waiting, batch should not eat the entire pool.
    high_waiting = _any_ready_in_queues(high_prios, assigned_pipelines)

    # Phase B: schedule batch, respecting a reserve if high-priority is waiting.
    for pool_id in batch_pool_order:
        pool = s.executor.pools[pool_id]

        # Soft interactive pool: if we have multiple pools, avoid batch on pool 0 when high-priority is waiting.
        if s.executor.num_pools >= 2 and pool_id == 0 and high_waiting:
            continue

        # Reserve some headroom in shared pools when high-priority is waiting.
        reserve_cpu = 0
        reserve_ram = 0
        if high_waiting:
            reserve_cpu = int(round(pool.max_cpu_pool * 0.25))
            reserve_ram = int(round(pool.max_ram_pool * 0.25))

        while rem_cpu[pool_id] > 0 and rem_ram[pool_id] > 0:
            pipeline, op_list, pr = _pick_next_ready(batch_prios, assigned_pipelines)
            if pipeline is None:
                break

            pid = pipeline.pipeline_id
            meta = _ensure_meta(pid)

            cpu, ram = _compute_request(
                priority=pr,
                pool=pool,
                rem_cpu=rem_cpu[pool_id],
                rem_ram=rem_ram[pool_id],
                meta=meta,
                reserve_cpu=reserve_cpu,
                reserve_ram=reserve_ram,
                is_batch=True,
            )
            if cpu <= 0 or ram <= 0:
                # If batch can't fit due to reserve, stop filling this pool.
                break

            assignments.append(
                Assignment(
                    ops=op_list,
                    cpu=cpu,
                    ram=ram,
                    priority=pipeline.priority,
                    pool_id=pool_id,
                    pipeline_id=pipeline.pipeline_id,
                )
            )
            rem_cpu[pool_id] -= cpu
            rem_ram[pool_id] -= ram

    return suspensions, assignments
