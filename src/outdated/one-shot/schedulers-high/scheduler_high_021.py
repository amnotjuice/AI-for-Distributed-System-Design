# policy_key: scheduler_high_021
# reasoning_effort: high
# model: gpt-5.2-2025-12-11
# llm_cost: 0.000000
# generation_seconds: 92.98
# generated_at: 2026-03-14T03:27:26.480416
@register_scheduler_init(key="scheduler_high_021")
def scheduler_high_021_init(s):
    """
    Priority-aware, OOM-adaptive FIFO.

    Improvements over naive FIFO:
      1) Separate queues per priority and always schedule higher priority first.
      2) Avoid "give entire pool to one op" by using per-priority CPU/RAM caps so we can run multiple ops concurrently.
      3) If an op fails with OOM, retry it with increased RAM next time (bounded by pool capacity).
      4) If an op fails with non-OOM repeatedly, blacklist its pipeline to avoid infinite retries that harm latency.
    """
    # Per-priority FIFO queues
    s.q_query = []
    s.q_interactive = []
    s.q_batch = []

    # Op-level hints keyed by a stable key derived from the op object
    s.op_ram_hint = {}          # op_key -> ram to request next time
    s.op_cpu_hint = {}          # op_key -> cpu to request next time (lightly used)
    s.op_fail_count = {}        # op_key -> number of failures observed
    s.op_last_error_kind = {}   # op_key -> "oom" | "other" | None

    # Map op_key -> pipeline_id so we can attribute results to pipelines
    s.op_to_pipeline = {}

    # Pipelines we won't schedule anymore due to repeated non-OOM failures
    s.blacklisted_pipelines = set()

    # Basic tunables (kept conservative for "small improvements first")
    s.max_assignments_per_tick = 32
    s.max_assignments_per_pool = 8
    s.oom_backoff_factor = 1.6
    s.non_oom_fail_blacklist_threshold = 1


@register_scheduler(key="scheduler_high_021")
def scheduler_high_021_scheduler(s, results: List["ExecutionResult"],
                                pipelines: List["Pipeline"]) -> Tuple[List["Suspend"], List["Assignment"]]:
    """
    Priority-aware scheduler with bounded per-op sizing and OOM-driven RAM growth.

    Scheduling approach:
      - Enqueue new pipelines into priority-specific FIFO queues.
      - Process execution results to update per-op RAM hints (grow on OOM) and detect repeated non-OOM failures.
      - Build a "shadow" view of available resources per pool; schedule in strict priority order:
            QUERY -> INTERACTIVE -> BATCH_PIPELINE
        choosing a pool with best headroom (with a mild preference to keep batch off pool 0 when possible).
      - Assign at most one ready operator per pipeline per tick to avoid over-committing a single pipeline.
    """
    def _op_key(op) -> int:
        # Use object identity; in this simulator, op objects should remain stable across ticks.
        return id(op)

    def _is_oom_error(err) -> bool:
        if err is None:
            return False
        msg = str(err).lower()
        return ("oom" in msg) or ("out of memory" in msg) or ("memory" in msg and "out" in msg)

    def _queue_for_priority(pri):
        if pri == Priority.QUERY:
            return s.q_query
        if pri == Priority.INTERACTIVE:
            return s.q_interactive
        return s.q_batch

    def _priority_order():
        return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]

    def _cpu_cap(pool, pri) -> float:
        # Cap cpu per op to reduce head-of-line blocking and improve latency for small interactive bursts.
        # Still large enough to benefit from scale-up bias.
        max_cpu = pool.max_cpu_pool
        if pri == Priority.QUERY:
            return max(1.0, 0.50 * max_cpu)
        if pri == Priority.INTERACTIVE:
            return max(1.0, 0.45 * max_cpu)
        return max(1.0, 0.35 * max_cpu)

    def _ram_cap(pool, pri) -> float:
        # Cap ram per op; keep enough headroom to run multiple ops concurrently.
        max_ram = pool.max_ram_pool
        if pri == Priority.QUERY:
            return max(1.0, 0.45 * max_ram)
        if pri == Priority.INTERACTIVE:
            return max(1.0, 0.50 * max_ram)
        return max(1.0, 0.40 * max_ram)

    def _pick_pool(pri, cpu_need, ram_need, cpu_free, ram_free) -> int:
        # Choose the pool with the most headroom that can fit the request.
        # Mild isolation: try to keep batch off pool 0 if multiple pools exist.
        candidates = []
        for i in range(s.executor.num_pools):
            if cpu_free[i] >= cpu_need and ram_free[i] >= ram_need:
                # Score: combine headroom; prioritize cpu headroom a bit for latency.
                score = (cpu_free[i] * 2.0) + ram_free[i]
                # For batch, de-prefer pool 0 if we have alternatives.
                if pri == Priority.BATCH_PIPELINE and s.executor.num_pools > 1 and i == 0:
                    score -= 1e9
                candidates.append((score, i))
        if not candidates:
            return -1
        candidates.sort(reverse=True)
        return candidates[0][1]

    # Enqueue new pipelines by priority (FIFO within each class)
    for p in pipelines:
        if p.pipeline_id in s.blacklisted_pipelines:
            continue
        _queue_for_priority(p.priority).append(p)

    # Process results: update hints and blacklist repeated non-OOM failures
    for r in (results or []):
        # Update every op in the result; we attribute it to the pipeline we mapped at assignment time.
        for op in (r.ops or []):
            k = _op_key(op)
            if r.failed():
                s.op_fail_count[k] = s.op_fail_count.get(k, 0) + 1
                if _is_oom_error(r.error):
                    s.op_last_error_kind[k] = "oom"
                    # Grow RAM hint from what we just tried; bounded later by pool capacity.
                    if r.ram is not None:
                        grown = float(r.ram) * float(s.oom_backoff_factor)
                        prev = s.op_ram_hint.get(k, 0.0)
                        s.op_ram_hint[k] = max(prev, grown)
                else:
                    s.op_last_error_kind[k] = "other"
                    # After a small number of non-OOM failures, blacklist the pipeline to protect latency.
                    if s.op_fail_count[k] >= s.non_oom_fail_blacklist_threshold:
                        pid = s.op_to_pipeline.get(k, None)
                        if pid is not None:
                            s.blacklisted_pipelines.add(pid)
            else:
                s.op_last_error_kind[k] = None
                # Keep successful allocations as a soft hint (can help avoid under-allocation).
                if r.ram is not None:
                    prev = s.op_ram_hint.get(k, 0.0)
                    s.op_ram_hint[k] = max(prev, float(r.ram))
                if r.cpu is not None:
                    prevc = s.op_cpu_hint.get(k, 0.0)
                    s.op_cpu_hint[k] = max(prevc, float(r.cpu))

    # Early exit if no state changes that affect our decisions
    if not pipelines and not results:
        return [], []

    suspensions: List["Suspend"] = []
    assignments: List["Assignment"] = []

    # Shadow resource tracking so we can emit multiple assignments safely.
    cpu_free = []
    ram_free = []
    for i in range(s.executor.num_pools):
        pool = s.executor.pools[i]
        cpu_free.append(float(pool.avail_cpu_pool))
        ram_free.append(float(pool.avail_ram_pool))

    # Helper to pop next schedulable pipeline from a given queue.
    # We rotate FIFO: pop front, possibly push back.
    def _next_pipeline_from_queue(q):
        while q:
            p = q.pop(0)
            if p.pipeline_id in s.blacklisted_pipelines:
                continue
            status = p.runtime_status()
            if status.is_pipeline_successful():
                continue

            # If there are failed ops and any was marked as non-OOM, stop scheduling this pipeline.
            try:
                failed_ops = status.get_ops([OperatorState.FAILED], require_parents_complete=False)
            except Exception:
                failed_ops = []
            non_oom_seen = False
            for op in failed_ops:
                k = _op_key(op)
                if s.op_last_error_kind.get(k, None) == "other":
                    non_oom_seen = True
                    break
            if non_oom_seen:
                s.blacklisted_pipelines.add(p.pipeline_id)
                continue

            return p
        return None

    # Main scheduling loop: strict priority order to improve tail latency.
    total_assignments = 0
    per_pool_assignments = [0 for _ in range(s.executor.num_pools)]

    for pri in _priority_order():
        q = _queue_for_priority(pri)

        # We'll attempt to schedule until we can't make progress for this priority.
        # Bound work to keep runtime predictable.
        no_progress_iters = 0
        while q and total_assignments < s.max_assignments_per_tick:
            p = _next_pipeline_from_queue(q)
            if p is None:
                break

            status = p.runtime_status()
            # Only schedule ops whose parents are complete to respect DAG dependencies.
            op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
            if not op_list:
                # Nothing runnable now; requeue and move on.
                q.append(p)
                no_progress_iters += 1
                if no_progress_iters > len(q) + 1:
                    break
                continue

            op = op_list[0]
            ok = _op_key(op)
            s.op_to_pipeline[ok] = p.pipeline_id

            # Compute request sizes with caps; prefer hints for RAM if we have them.
            # Note: we must fit within pool capacity; pool pick is based on this initial request.
            # We'll keep request "reasonable" to enable concurrency.
            # CPU request: cap by priority and remaining pool capacity.
            # RAM request: use hint if present, else start with a cap-based fraction.
            # (We don't know true minima; OOM will grow the hint.)
            # We'll first decide a conservative default request using pool0 max as a reference.
            # The final cap is applied per chosen pool.
            ref_pool = s.executor.pools[0]
            default_cpu = max(1.0, 0.25 * float(ref_pool.max_cpu_pool))
            default_ram = max(1.0, 0.20 * float(ref_pool.max_ram_pool))
            if pri == Priority.QUERY:
                default_cpu = max(1.0, 0.35 * float(ref_pool.max_cpu_pool))
                default_ram = max(1.0, 0.30 * float(ref_pool.max_ram_pool))
            elif pri == Priority.INTERACTIVE:
                default_cpu = max(1.0, 0.30 * float(ref_pool.max_cpu_pool))
                default_ram = max(1.0, 0.35 * float(ref_pool.max_ram_pool))

            cpu_req = float(s.op_cpu_hint.get(ok, default_cpu))
            ram_req = float(s.op_ram_hint.get(ok, default_ram))

            # Try to place the op. We choose a pool first, then apply pool-specific caps.
            # To improve admission for high priority, we allow them to grab smaller slices if needed.
            placed = False
            for shrink in [1.0, 0.75, 0.5]:
                cpu_try = max(1.0, cpu_req * shrink)
                ram_try = max(1.0, ram_req * shrink)

                # Select a pool that fits this request.
                pool_id = _pick_pool(pri, cpu_try, ram_try, cpu_free, ram_free)
                if pool_id < 0:
                    continue

                pool = s.executor.pools[pool_id]
                if per_pool_assignments[pool_id] >= s.max_assignments_per_pool:
                    continue

                # Apply per-pool caps and available resources.
                cpu_cap = _cpu_cap(pool, pri)
                ram_cap = _ram_cap(pool, pri)

                cpu_final = min(cpu_try, cpu_cap, cpu_free[pool_id])
                ram_final = min(ram_try, ram_cap, ram_free[pool_id])

                if cpu_final <= 0.0 or ram_final <= 0.0:
                    continue

                # Ensure we still fit after caps.
                if cpu_final > cpu_free[pool_id] or ram_final > ram_free[pool_id]:
                    continue

                assignments.append(
                    Assignment(
                        ops=op_list,
                        cpu=cpu_final,
                        ram=ram_final,
                        priority=p.priority,
                        pool_id=pool_id,
                        pipeline_id=p.pipeline_id,
                    )
                )
                cpu_free[pool_id] -= cpu_final
                ram_free[pool_id] -= ram_final
                per_pool_assignments[pool_id] += 1
                total_assignments += 1

                # Requeue the pipeline so it can make progress in future ticks (or even this tick).
                q.append(p)
                placed = True
                break

            if not placed:
                # Couldn't place now; requeue and stop trying aggressively for this priority
                # to avoid O(n^2) thrashing when resources are tight.
                q.append(p)
                break

    return suspensions, assignments
