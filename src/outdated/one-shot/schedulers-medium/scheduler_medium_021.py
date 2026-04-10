# policy_key: scheduler_medium_021
# reasoning_effort: medium
# model: gpt-5.2-2025-12-11
# llm_cost: 0.069050
# generation_seconds: 76.30
# generated_at: 2026-03-14T02:42:07.232105
@register_scheduler_init(key="scheduler_medium_021")
def scheduler_medium_021_init(s):
    """Priority-aware, headroom-preserving scheduler.

    Incremental improvements over naive FIFO:
      1) Maintain separate queues per priority and always schedule higher priority first.
      2) Avoid saturating the entire pool with low-priority work by reserving headroom
         when high-priority work is waiting (reduces latency spikes).
      3) Simple OOM backoff: if an operator fails with OOM, retry with more RAM next time.
         If repeated OOMs exceed a limit, stop retrying that pipeline.
    """
    # Per-priority waiting queues (round-robin within each priority)
    s.queues = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }

    # Track pipelines we've seen to avoid enqueuing duplicates aggressively
    s.seen_pipeline_ids = set()

    # Map operator object identity -> pipeline_id, for attributing results back to pipelines
    s.op_to_pipeline_id = {}

    # Last requested resources per pipeline (used as a hint for future sizing)
    s.pipeline_last_cpu = {}  # pipeline_id -> cpu
    s.pipeline_last_ram = {}  # pipeline_id -> ram

    # RAM backoff state for OOM retries
    s.pipeline_ram_factor = {}     # pipeline_id -> multiplier
    s.pipeline_oom_retries = {}    # pipeline_id -> count
    s.pipeline_non_retryable = set()

    # Tuning knobs (kept simple and conservative)
    s.max_oom_retries = 3
    s.max_ram_factor = 16.0


@register_scheduler(key="scheduler_medium_021")
def scheduler_medium_021(s, results: List[ExecutionResult], pipelines: List[Pipeline]) -> Tuple[List[Suspend], List[Assignment]]:
    """Priority queues + headroom reservation + OOM RAM backoff."""
    # Fast-path: no new info
    if not pipelines and not results:
        return [], []

    suspensions: List[Suspend] = []
    assignments: List[Assignment] = []

    def _is_oom_error(err) -> bool:
        if err is None:
            return False
        msg = str(err).lower()
        return ("oom" in msg) or ("out of memory" in msg) or ("out-of-memory" in msg) or ("memoryerror" in msg)

    def _enqueue_pipeline(p: Pipeline):
        # Avoid repeated enqueues of the exact same pipeline_id within a short window.
        # We still keep it in the queue across ticks.
        if p.pipeline_id not in s.seen_pipeline_ids:
            s.seen_pipeline_ids.add(p.pipeline_id)
            s.queues[p.priority].append(p)
        else:
            # If we see it again, still ensure it's present if it was dropped by accident.
            # (Lightweight check: do nothing; queue already holds it in normal operation.)
            pass

        # Initialize backoff defaults
        s.pipeline_ram_factor.setdefault(p.pipeline_id, 1.0)
        s.pipeline_oom_retries.setdefault(p.pipeline_id, 0)

    # Ingest new pipelines
    for p in pipelines:
        _enqueue_pipeline(p)

    # Process results to update OOM backoff and resource hints
    for r in results:
        # Attribute result to a pipeline via operator identity mapping
        pid = None
        for op in getattr(r, "ops", []) or []:
            pid = s.op_to_pipeline_id.pop(id(op), pid)

        # Update last requested resources as hints (even on failure, they're informative)
        if pid is not None:
            if getattr(r, "cpu", None) is not None:
                s.pipeline_last_cpu[pid] = r.cpu
            if getattr(r, "ram", None) is not None:
                s.pipeline_last_ram[pid] = r.ram

        if r.failed():
            if pid is None:
                continue

            if _is_oom_error(getattr(r, "error", None)):
                # Exponential RAM backoff on OOM
                s.pipeline_oom_retries[pid] = s.pipeline_oom_retries.get(pid, 0) + 1
                s.pipeline_ram_factor[pid] = min(
                    s.max_ram_factor,
                    max(s.pipeline_ram_factor.get(pid, 1.0) * 2.0, 2.0),
                )
                if s.pipeline_oom_retries[pid] > s.max_oom_retries:
                    s.pipeline_non_retryable.add(pid)
            else:
                # Non-OOM failures are treated as non-retryable (avoid infinite loops)
                s.pipeline_non_retryable.add(pid)
        else:
            # On success, gently decay RAM factor back toward 1 to avoid permanent over-allocation
            if pid is not None and pid in s.pipeline_ram_factor:
                f = s.pipeline_ram_factor[pid]
                if f > 1.0:
                    s.pipeline_ram_factor[pid] = max(1.0, f * 0.9)
                # Reset OOM retries after a success
                s.pipeline_oom_retries[pid] = 0

    # Determine if any high-priority work is waiting (used for headroom reservation)
    def _has_waiting_high_priority() -> bool:
        for pr in (Priority.QUERY, Priority.INTERACTIVE):
            q = s.queues.get(pr, [])
            # Consider "waiting" if there exists a pipeline with at least one ready op
            for p in q:
                if p.pipeline_id in s.pipeline_non_retryable:
                    continue
                st = p.runtime_status()
                if st.is_pipeline_successful():
                    continue
                ready = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
                if ready:
                    return True
        return False

    high_waiting = _has_waiting_high_priority()

    # Pool iteration orders: bias high priority to earlier pools; batch to later pools
    pool_ids_for_high = list(range(s.executor.num_pools))
    pool_ids_for_batch = list(reversed(range(s.executor.num_pools)))

    # Priority order: strict, to improve latency
    priority_order = [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]

    # Base sizing fractions per priority (simple and stable)
    base_cpu_frac = {
        Priority.QUERY: 0.55,
        Priority.INTERACTIVE: 0.45,
        Priority.BATCH_PIPELINE: 0.30,
    }
    base_ram_frac = {
        Priority.QUERY: 0.45,
        Priority.INTERACTIVE: 0.40,
        Priority.BATCH_PIPELINE: 0.30,
    }

    # Headroom reservation when high-priority work exists:
    # keep some resources available so new queries don't queue behind long batch steps.
    reserve_cpu_frac = 0.35 if high_waiting else 0.0
    reserve_ram_frac = 0.35 if high_waiting else 0.0

    def _pick_request(pid: int, pool, prio):
        # Start from base fractions of pool capacity
        cpu = pool.max_cpu_pool * base_cpu_frac[prio]
        ram = pool.max_ram_pool * base_ram_frac[prio]

        # If we have prior successful/attempted sizes, use them as hints:
        cpu = max(cpu, s.pipeline_last_cpu.get(pid, 0) or 0)
        ram = max(ram, s.pipeline_last_ram.get(pid, 0) or 0)

        # Apply OOM backoff multiplier to RAM
        ram *= s.pipeline_ram_factor.get(pid, 1.0)

        # Ensure minimum non-zero requests
        min_cpu = max(1.0, pool.max_cpu_pool * 0.05)
        min_ram = max(1.0, pool.max_ram_pool * 0.05)
        cpu = max(cpu, min_cpu)
        ram = max(ram, min_ram)

        return cpu, ram

    def _try_schedule_from_queue(pool_id: int, prio, max_iters: int) -> None:
        nonlocal assignments
        pool = s.executor.pools[pool_id]

        # Calculate effective availability; for batch, respect headroom reservations
        def _effective_avail():
            avail_cpu = pool.avail_cpu_pool
            avail_ram = pool.avail_ram_pool
            if prio == Priority.BATCH_PIPELINE and (reserve_cpu_frac > 0.0 or reserve_ram_frac > 0.0):
                avail_cpu = max(0.0, avail_cpu - pool.max_cpu_pool * reserve_cpu_frac)
                avail_ram = max(0.0, avail_ram - pool.max_ram_pool * reserve_ram_frac)
            return avail_cpu, avail_ram

        q = s.queues.get(prio, [])
        if not q:
            return

        # Round-robin: rotate through pipelines, scheduling at most one op per pipeline per pass
        iters = 0
        while iters < max_iters and q:
            iters += 1
            avail_cpu_eff, avail_ram_eff = _effective_avail()
            if avail_cpu_eff <= 0 or avail_ram_eff <= 0:
                break

            p = q.pop(0)

            # Skip non-retryable or completed pipelines
            if p.pipeline_id in s.pipeline_non_retryable:
                continue
            st = p.runtime_status()
            if st.is_pipeline_successful():
                continue

            # Find a ready operator (parents complete)
            op_list = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
            if not op_list:
                # Not ready now; keep it in rotation
                q.append(p)
                continue

            # Decide resource request
            req_cpu, req_ram = _pick_request(p.pipeline_id, pool, prio)

            # Clamp to effective availability (never request more than we can immediately place)
            avail_cpu_eff, avail_ram_eff = _effective_avail()
            cpu = min(req_cpu, avail_cpu_eff)
            ram = min(req_ram, avail_ram_eff)

            # If clamped too low, skip for now to reduce predictable OOM churn
            # (Especially for batch: better to wait than to thrash)
            min_cpu = max(1.0, pool.max_cpu_pool * 0.05)
            min_ram = max(1.0, pool.max_ram_pool * 0.05)
            if cpu < min_cpu or ram < min_ram:
                q.append(p)
                continue

            # Create assignment
            assignment = Assignment(
                ops=op_list,
                cpu=cpu,
                ram=ram,
                priority=p.priority,
                pool_id=pool_id,
                pipeline_id=p.pipeline_id,
            )
            assignments.append(assignment)

            # Record mapping for attributing results back to this pipeline
            for op in op_list:
                s.op_to_pipeline_id[id(op)] = p.pipeline_id

            # Keep pipeline in queue for subsequent ops
            q.append(p)

    # Schedule work: for each priority, attempt to fill pools while respecting headroom
    for prio in priority_order:
        pool_ids = pool_ids_for_high if prio in (Priority.QUERY, Priority.INTERACTIVE) else pool_ids_for_batch
        # Limit iterations to avoid pathological O(n^2) scans when many pipelines are blocked
        max_iters = max(10, len(s.queues.get(prio, [])) * 2)
        for pool_id in pool_ids:
            _try_schedule_from_queue(pool_id, prio, max_iters=max_iters)

    return suspensions, assignments
