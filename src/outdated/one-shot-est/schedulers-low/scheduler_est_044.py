# policy_key: scheduler_est_044
# reasoning_effort: low
# model: gpt-5.2-2025-12-11
# llm_cost: 0.041264
# generation_seconds: 35.43
# generated_at: 2026-04-01T00:13:59.412852
@register_scheduler_init(key="scheduler_est_044")
def scheduler_est_044_init(s):
    """Priority-aware FIFO with simple right-sizing.

    Small, incremental improvements over the naive FIFO:
      1) Maintain separate queues per priority (QUERY > INTERACTIVE > BATCH_PIPELINE).
      2) Allocate RAM using op.estimate.mem_peak_gb as a floor with a safety factor to reduce OOMs.
      3) Avoid "one assignment per pool" bottleneck: pack multiple single-op assignments per pool per tick.
      4) Cap per-op CPU for lower priorities so interactive/query latency isn't dominated by one batch op.
      5) On failures, assume OOM-like and increase the per-op RAM safety factor, then allow retry.
    """
    # Priority-ordered queues of pipelines (round-robin within each priority)
    s.queues = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }

    # Per-operator RAM safety factor and failure counters
    # Keyed by (pipeline_id, op_object_id) to be stable within the simulation process.
    s.op_ram_factor = {}   # (pipeline_id, op_key) -> float
    s.op_fail_count = {}   # (pipeline_id, op_key) -> int

    # Mild defaults: extra RAM is "free" in the simulator; prioritize avoiding OOMs.
    s.default_estimate_gb = 1.0
    s.base_ram_safety = 1.20

    # Failure handling: exponential backoff on RAM
    s.max_op_failures = 4
    s.ram_bump_multiplier = 1.50

    # CPU caps per op by priority (fraction of pool max CPU)
    # Rationale: keep batch from grabbing all CPU when higher priority work may arrive/queue.
    s.cpu_cap_frac = {
        Priority.QUERY: 1.00,
        Priority.INTERACTIVE: 0.85,
        Priority.BATCH_PIPELINE: 0.60,
    }

    # Minimum CPU slice per assignment; keep small to allow packing.
    s.min_cpu_slice = 1.0

    # Small guardrail to avoid scheduling extremely tiny RAM when estimate is missing.
    s.min_ram_gb = 0.5


@register_scheduler(key="scheduler_est_044")
def scheduler_est_044(s, results: List[ExecutionResult],
                      pipelines: List[Pipeline]) -> Tuple[List[Suspend], List[Assignment]]:
    """Priority-aware packing scheduler with RAM estimation floor and simple retry on failure."""
    def _prio_order():
        return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]

    def _enqueue_pipeline(p: Pipeline):
        # Avoid duplicates: do a light check by pipeline_id in queues.
        # (O(n) but queues are expected to be small in the simulator.)
        for pr in _prio_order():
            for existing in s.queues[pr]:
                if existing.pipeline_id == p.pipeline_id:
                    return
        s.queues[p.priority].append(p)

    def _drop_pipeline_from_queues(pipeline_id):
        for pr in _prio_order():
            q = s.queues[pr]
            s.queues[pr] = [p for p in q if p.pipeline_id != pipeline_id]

    def _pop_next_pipeline_with_ready_op():
        # Highest-priority first; round-robin within the chosen priority.
        for pr in _prio_order():
            q = s.queues[pr]
            if not q:
                continue

            # Search up to len(q) to find one with an assignable ready op.
            for _ in range(len(q)):
                p = q.pop(0)
                status = p.runtime_status()

                # Drop fully successful pipelines
                if status.is_pipeline_successful():
                    continue

                # Find ready ops (parents complete)
                op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
                if op_list:
                    # Put back for round-robin fairness (still has future ops)
                    q.append(p)
                    return p, op_list[:1]  # schedule one op at a time for packing

                # Not ready now; keep it in the queue and continue searching
                q.append(p)

        return None, None

    def _op_key(p: Pipeline, op):
        return (p.pipeline_id, id(op))

    def _est_mem_gb(op):
        # Use estimator if present; otherwise fallback.
        est = None
        try:
            est = op.estimate.mem_peak_gb
        except Exception:
            est = None
        if est is None:
            est = s.default_estimate_gb
        try:
            est = float(est)
        except Exception:
            est = s.default_estimate_gb
        return max(est, s.min_ram_gb)

    def _ram_request_gb(p: Pipeline, op):
        key = _op_key(p, op)
        factor = s.op_ram_factor.get(key, s.base_ram_safety)
        return _est_mem_gb(op) * factor

    def _cpu_cap_for_priority(pool, prio):
        frac = s.cpu_cap_frac.get(prio, 0.75)
        return max(s.min_cpu_slice, pool.max_cpu_pool * frac)

    # Ingest new pipelines
    for p in pipelines:
        _enqueue_pipeline(p)

    # Update RAM retry factors based on results
    for r in results:
        # If a pipeline finished successfully, we might still see results; ignore.
        if not hasattr(r, "ops") or not r.ops:
            continue

        # On failure, bump RAM for the failed op and allow retry by keeping pipeline queued.
        if r.failed():
            for op in r.ops:
                # We may not have the pipeline object here; key only by pipeline_id and op object id.
                key = (r.pipeline_id if hasattr(r, "pipeline_id") else None, id(op))
                # If pipeline_id isn't available in result, fall back to searching by op identity in our queues.
                if key[0] is None:
                    # Best-effort: try to find the pipeline by matching priority + queued pipelines
                    found_pid = None
                    for pr in _prio_order():
                        for p in s.queues[pr]:
                            if p.pipeline_id == getattr(r, "pipeline_id", p.pipeline_id):
                                found_pid = p.pipeline_id
                                break
                        if found_pid is not None:
                            break
                    key = (found_pid, id(op))

                # Increment fail count and bump RAM factor
                s.op_fail_count[key] = s.op_fail_count.get(key, 0) + 1
                prev = s.op_ram_factor.get(key, s.base_ram_safety)
                s.op_ram_factor[key] = prev * s.ram_bump_multiplier

    # Early exit if nothing to do
    if not pipelines and not results:
        any_waiting = any(len(s.queues[pr]) > 0 for pr in _prio_order())
        if not any_waiting:
            return [], []

    suspensions: List[Suspend] = []
    assignments: List[Assignment] = []

    # Schedule per pool: pack as many single-op assignments as resources allow.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool

        # Keep assigning while we have meaningful resources
        while avail_cpu >= s.min_cpu_slice and avail_ram > 0:
            p, op_list = _pop_next_pipeline_with_ready_op()
            if not p or not op_list:
                break

            op = op_list[0]
            ram_need = _ram_request_gb(p, op)

            # If an op has failed too many times, drop the entire pipeline to avoid infinite loops.
            key_local = _op_key(p, op)
            if s.op_fail_count.get(key_local, 0) >= s.max_op_failures:
                _drop_pipeline_from_queues(p.pipeline_id)
                continue

            # If we can't fit RAM, stop packing this pool (likely RAM constrained).
            # (We keep the pipeline in the queue via round-robin already.)
            if ram_need > avail_ram:
                break

            cpu_cap = _cpu_cap_for_priority(pool, p.priority)
            cpu_need = min(avail_cpu, cpu_cap)

            # Ensure we don't schedule a zero/negative CPU assignment.
            if cpu_need < s.min_cpu_slice:
                break

            assignments.append(
                Assignment(
                    ops=[op],
                    cpu=cpu_need,
                    ram=ram_need,
                    priority=p.priority,
                    pool_id=pool_id,
                    pipeline_id=p.pipeline_id,
                )
            )

            avail_cpu -= cpu_need
            avail_ram -= ram_need

    return suspensions, assignments
