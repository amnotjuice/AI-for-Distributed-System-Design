# policy_key: scheduler_medium_026
# reasoning_effort: medium
# model: gpt-5.2-2025-12-11
# llm_cost: 0.099245
# generation_seconds: 81.79
# generated_at: 2026-03-14T02:50:05.756180
@register_scheduler_init(key="scheduler_medium_026")
def scheduler_medium_026_init(s):
    """Priority-aware, latency-oriented improvement over naive FIFO.

    Key incremental improvements vs. baseline:
      1) Priority-first global queueing (QUERY > INTERACTIVE > BATCH), with round-robin within each priority.
      2) Avoid "give everything to one op" (which hurts latency under contention):
         - allocate capped CPU/RAM quanta per operator, enabling more concurrency and better tail latency.
      3) OOM-adaptive retries:
         - if an operator fails with OOM, retry it with exponentially increased RAM (per-operator).
         - if an operator fails with non-OOM error, stop retrying that pipeline (to avoid churn).
      4) Fragment-fitting:
         - if the next high-priority op doesn't fit current pool headroom, try other pipelines that might fit
           (prevents headroom fragmentation from stalling progress).

    Notes:
      - This policy intentionally avoids preemption because the minimal public interface in the prompt
        doesn't expose a safe way to enumerate/evict running containers across pools.
      - The design keeps complexity modest while targeting obvious latency flaws in the naive scheduler.
    """
    # Separate FIFO queues per priority, but we will round-robin within each queue.
    s.queues = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }
    s.rr_index = {
        Priority.QUERY: 0,
        Priority.INTERACTIVE: 0,
        Priority.BATCH_PIPELINE: 0,
    }

    # Map operator identity -> pipeline id for interpreting ExecutionResult (which doesn't include pipeline_id).
    s.op_to_pipeline = {}  # op_key -> pipeline_id

    # Per-operator retry/adaptation state keyed by (pipeline_id, op_key)
    s.op_ram_mult = {}     # (pid, op_key) -> multiplier (float)
    s.op_fail_count = {}   # (pid, op_key) -> int
    s.op_last_was_oom = {} # (pid, op_key) -> bool

    # Pipeline-level hard failure marker (non-OOM errors)
    s.pipeline_hard_failed = set()

    # Limits to avoid infinite churn
    s.max_oom_retries = 4
    s.max_non_oom_failures = 1

    # Minimum allocs (avoid zeros)
    s.min_cpu = 1
    s.min_ram = 1

    # Caps as fractions of pool capacity by priority (tuned to improve latency vs. "monopolize pool")
    # QUERY should be responsive; INTERACTIVE similar; BATCH smaller to reduce interference.
    s.cpu_frac_cap = {
        Priority.QUERY: 0.50,
        Priority.INTERACTIVE: 0.60,
        Priority.BATCH_PIPELINE: 0.35,
    }
    s.ram_frac_cap = {
        Priority.QUERY: 0.50,
        Priority.INTERACTIVE: 0.60,
        Priority.BATCH_PIPELINE: 0.40,
    }


@register_scheduler(key="scheduler_medium_026")
def scheduler_medium_026_scheduler(s, results, pipelines):
    """
    Priority-aware, round-robin, OOM-adaptive scheduler.

    Returns:
        (suspensions, assignments)
    """
    def _op_key(op):
        # Use Python object identity for a stable key across scheduling/results callbacks.
        return id(op)

    def _is_oom_error(err):
        if err is None:
            return False
        msg = str(err).lower()
        return ("oom" in msg) or ("out of memory" in msg) or ("cuda out of memory" in msg) or ("killed process" in msg)

    def _clamp_int(x, lo, hi):
        if x < lo:
            return lo
        if x > hi:
            return hi
        return x

    def _pool_order():
        # Place work into pools with the most headroom first to reduce fragmentation and help latency.
        scored = []
        for pool_id in range(s.executor.num_pools):
            pool = s.executor.pools[pool_id]
            max_cpu = pool.max_cpu_pool if pool.max_cpu_pool > 0 else 1
            max_ram = pool.max_ram_pool if pool.max_ram_pool > 0 else 1
            headroom = (pool.avail_cpu_pool / max_cpu) + (pool.avail_ram_pool / max_ram)
            scored.append((headroom, pool_id))
        scored.sort(reverse=True)
        return [pid for _, pid in scored]

    def _cleanup_pipeline_state(pipeline_id):
        # Remove per-op state for a pipeline to prevent unbounded growth.
        to_del = []
        for (pid, ok) in s.op_ram_mult.keys():
            if pid == pipeline_id:
                to_del.append((pid, ok))
        for k in to_del:
            s.op_ram_mult.pop(k, None)
            s.op_fail_count.pop(k, None)
            s.op_last_was_oom.pop(k, None)
        # Also remove any op->pipeline mappings that point to this pipeline.
        to_del2 = [ok for ok, pid in s.op_to_pipeline.items() if pid == pipeline_id]
        for ok in to_del2:
            s.op_to_pipeline.pop(ok, None)
        s.pipeline_hard_failed.discard(pipeline_id)

    def _pick_next_pipeline(priority):
        q = s.queues[priority]
        if not q:
            return None
        # Round-robin pick to reduce head-of-line blocking within a priority.
        idx = s.rr_index[priority] % len(q)
        p = q.pop(idx)
        # Next start index continues after the removed element.
        s.rr_index[priority] = idx % (len(q) + 1)  # +1 because we just popped from old length
        return p

    def _requeue_pipeline(p):
        s.queues[p.priority].append(p)

    # Enqueue new pipelines by their priority.
    for p in pipelines:
        s.queues[p.priority].append(p)

    # Update adaptation state based on execution results.
    # NOTE: ExecutionResult doesn't include pipeline_id; we infer it from op object identity.
    for r in results:
        if not hasattr(r, "ops") or not r.ops:
            continue

        # Infer pipeline_id from first op that we can map.
        inferred_pid = None
        for op in r.ops:
            ok = _op_key(op)
            pid = s.op_to_pipeline.get(ok)
            if pid is not None:
                inferred_pid = pid
                break
        if inferred_pid is None:
            continue

        if r.failed():
            oom = _is_oom_error(r.error)
            for op in r.ops:
                ok = _op_key(op)
                key = (inferred_pid, ok)
                s.op_fail_count[key] = s.op_fail_count.get(key, 0) + 1
                s.op_last_was_oom[key] = oom

                if oom:
                    # Exponential RAM backoff for this specific operator.
                    cur = s.op_ram_mult.get(key, 1.0)
                    s.op_ram_mult[key] = min(cur * 2.0, 64.0)  # large cap to avoid runaway
                else:
                    # Non-OOM failures are treated as hard failures; stop retrying pipeline.
                    s.pipeline_hard_failed.add(inferred_pid)

    # Early exit: nothing to do.
    if not pipelines and not results and all(not q for q in s.queues.values()):
        return [], []

    suspensions = []
    assignments = []

    # Priority order: protect latency for query/interactive first.
    priority_order = [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]

    # For each pool, try to pack multiple single-op assignments to improve throughput and reduce queueing latency.
    for pool_id in _pool_order():
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool

        if avail_cpu < s.min_cpu or avail_ram < s.min_ram:
            continue

        # Bound the amount of searching per pool to avoid O(N^2) churn when many pipelines are blocked.
        # We try at most (total queued pipelines * 2) picks in this pool.
        total_queued = sum(len(s.queues[p]) for p in priority_order)
        max_picks = max(4, total_queued * 2)
        picks = 0

        while picks < max_picks:
            picks += 1

            if avail_cpu < s.min_cpu or avail_ram < s.min_ram:
                break

            selected_pipeline = None

            # Find the next pipeline to run (priority-first). If a pipeline isn't runnable, requeue it.
            for pr in priority_order:
                p = _pick_next_pipeline(pr)
                if p is None:
                    continue

                status = p.runtime_status()

                # Drop successful pipelines.
                if status.is_pipeline_successful():
                    _cleanup_pipeline_state(p.pipeline_id)
                    continue

                # Drop pipelines with hard failures (non-OOM) to avoid churn.
                if p.pipeline_id in s.pipeline_hard_failed:
                    _cleanup_pipeline_state(p.pipeline_id)
                    continue

                # Find one runnable operator (parents complete).
                op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
                if not op_list:
                    # Not runnable now; keep it for later.
                    _requeue_pipeline(p)
                    continue

                # Candidate found.
                selected_pipeline = p
                selected_ops = op_list
                break

            if selected_pipeline is None:
                # Nothing runnable in this pool at the moment.
                break

            op0 = selected_ops[0]
            ok0 = _op_key(op0)
            s.op_to_pipeline[ok0] = selected_pipeline.pipeline_id
            key0 = (selected_pipeline.pipeline_id, ok0)

            # Enforce retry limits on OOM to prevent infinite retries.
            fail_cnt = s.op_fail_count.get(key0, 0)
            last_was_oom = s.op_last_was_oom.get(key0, False)
            if last_was_oom and fail_cnt > s.max_oom_retries:
                s.pipeline_hard_failed.add(selected_pipeline.pipeline_id)
                _cleanup_pipeline_state(selected_pipeline.pipeline_id)
                continue
            if (not last_was_oom) and fail_cnt > s.max_non_oom_failures:
                s.pipeline_hard_failed.add(selected_pipeline.pipeline_id)
                _cleanup_pipeline_state(selected_pipeline.pipeline_id)
                continue

            # Compute capped CPU/RAM targets for this op.
            # Goal: avoid monopolizing pool (better latency under concurrency) while still giving
            # higher priority more resources.
            pr = selected_pipeline.priority
            cpu_cap = max(s.min_cpu, int(pool.max_cpu_pool * s.cpu_frac_cap.get(pr, 0.5)))
            ram_cap = max(s.min_ram, int(pool.max_ram_pool * s.ram_frac_cap.get(pr, 0.5)))

            # Apply per-op OOM backoff multiplier to RAM request.
            ram_mult = s.op_ram_mult.get(key0, 1.0)
            req_cpu = cpu_cap
            req_ram = int(ram_cap * ram_mult)

            # If request cannot fit, try to find another pipeline that might fit in this pool to reduce fragmentation.
            # We do NOT downsize below our chosen cap heuristics because that tends to cause avoidable OOM churn.
            if req_cpu > avail_cpu or req_ram > avail_ram:
                _requeue_pipeline(selected_pipeline)
                continue

            # Make assignment (1 op at a time per pipeline).
            assignments.append(
                Assignment(
                    ops=selected_ops,
                    cpu=req_cpu,
                    ram=req_ram,
                    priority=selected_pipeline.priority,
                    pool_id=pool_id,
                    pipeline_id=selected_pipeline.pipeline_id,
                )
            )

            # Update local headroom for additional packing in this pool.
            avail_cpu -= req_cpu
            avail_ram -= req_ram

            # Requeue pipeline; it may have more runnable work later.
            _requeue_pipeline(selected_pipeline)

    return suspensions, assignments
