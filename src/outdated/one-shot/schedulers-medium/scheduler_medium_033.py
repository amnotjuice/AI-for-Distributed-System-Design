# policy_key: scheduler_medium_033
# reasoning_effort: medium
# model: gpt-5.2-2025-12-11
# llm_cost: 0.066331
# generation_seconds: 75.07
# generated_at: 2026-03-14T03:01:25.724359
@register_scheduler_init(key="scheduler_medium_033")
def scheduler_medium_033_init(s):
    """
    Priority-aware FIFO with small, safe improvements over naive:
      - Separate per-priority queues (QUERY > INTERACTIVE > BATCH)
      - Gentle resource reservation to protect high-priority tail latency
      - Simple OOM-aware RAM backoff (retry by increasing RAM on OOM-like failures)
      - Round-robin within a priority class to avoid head-of-line blocking
    """
    from collections import deque

    # Per-priority queues (round-robin inside each).
    s.queues = {
        Priority.QUERY: deque(),
        Priority.INTERACTIVE: deque(),
        Priority.BATCH_PIPELINE: deque(),
    }

    # Per-pipeline resource hints learned from failures.
    s.pipeline_ram_hint = {}      # pipeline_id -> ram to request next time
    s.pipeline_cpu_hint = {}      # pipeline_id -> cpu to request next time (lightly used)
    s.pipeline_oom_count = {}     # pipeline_id -> consecutive oom-ish failures

    # Pipelines we should not retry (non-OOM failures).
    s.dead_pipelines = set()


@register_scheduler(key="scheduler_medium_033")
def scheduler_medium_033_scheduler(s, results: List["ExecutionResult"], pipelines: List["Pipeline"]) -> Tuple[List["Suspend"], List["Assignment"]]:
    # ---- Helper functions (kept local to avoid top-level imports) ----
    def _prio_order():
        # Highest first
        return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]

    def _is_oom_error(err) -> bool:
        if err is None:
            return False
        msg = str(err).lower()
        return ("oom" in msg) or ("out of memory" in msg) or ("cuda out of memory" in msg)

    def _pool_order_by_headroom():
        # Schedule more valuable work on "bigger" (more available) pools first.
        pool_ids = list(range(s.executor.num_pools))
        def headroom(pid):
            p = s.executor.pools[pid]
            # Weight RAM slightly more (OOMs are expensive), but both matter.
            return (p.avail_cpu_pool, p.avail_ram_pool)
        pool_ids.sort(key=headroom, reverse=True)
        return pool_ids

    def _pipeline_done_or_dead(p):
        if p.pipeline_id in s.dead_pipelines:
            return True
        st = p.runtime_status()
        if st.is_pipeline_successful():
            return True
        return False

    def _pipeline_has_nonretryable_failures(p):
        # If there are FAILED ops, we only keep going if we've observed an OOM for this pipeline.
        st = p.runtime_status()
        failed_cnt = st.state_counts.get(OperatorState.FAILED, 0)
        if failed_cnt <= 0:
            return False
        # Retry only if we have some OOM evidence; otherwise treat as dead.
        return s.pipeline_oom_count.get(p.pipeline_id, 0) <= 0

    def _next_assignable_op(p):
        st = p.runtime_status()
        ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        if not ops:
            return None
        return ops[0]

    def _clamp(v, lo, hi):
        if v < lo:
            return lo
        if v > hi:
            return hi
        return v

    def _alloc_for(priority, pool, pipeline_id):
        """
        Choose a conservative allocation:
          - High priority: use most of available to minimize latency.
          - Batch: cap usage and keep reserved headroom for future interactive/query arrivals.
        """
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool
        max_cpu = pool.max_cpu_pool
        max_ram = pool.max_ram_pool

        # Small floors to avoid pathological tiny containers.
        cpu_floor = max(1.0, 0.10 * max_cpu)
        ram_floor = max(0.5, 0.10 * max_ram)

        # Defaults: allocate large for latency-sensitive work.
        cpu_cap = avail_cpu
        ram_cap = avail_ram

        # Reservation logic: batch should not consume the entire pool.
        high_waiting = (len(s.queues[Priority.QUERY]) + len(s.queues[Priority.INTERACTIVE])) > 0

        if priority == Priority.BATCH_PIPELINE:
            # Keep some headroom for bursty interactive/query.
            reserve_frac = 0.40 if high_waiting else 0.20
            reserve_cpu = reserve_frac * max_cpu
            reserve_ram = reserve_frac * max_ram
            cpu_cap = max(0.0, avail_cpu - reserve_cpu)
            ram_cap = max(0.0, avail_ram - reserve_ram)

            # Also avoid "one batch op grabs everything" even when empty.
            cpu_cap = min(cpu_cap, 0.60 * max_cpu)
            ram_cap = min(ram_cap, 0.60 * max_ram)

        elif priority == Priority.INTERACTIVE:
            # Interactive: allow large, but avoid always grabbing 100% if multiple pools exist.
            # (Keeps some elasticity; still latency-first.)
            cpu_cap = min(cpu_cap, 0.90 * max_cpu) if s.executor.num_pools > 1 else cpu_cap
            ram_cap = min(ram_cap, 0.90 * max_ram) if s.executor.num_pools > 1 else ram_cap

        else:
            # QUERY: most latency sensitive; use everything available.
            pass

        # Apply learned RAM hint (OOM backoff), but never exceed what we can give.
        ram_hint = s.pipeline_ram_hint.get(pipeline_id, None)
        if ram_hint is not None:
            ram_cap = min(ram_cap, ram_hint)

        # Apply a light CPU hint (optional future extensibility).
        cpu_hint = s.pipeline_cpu_hint.get(pipeline_id, None)
        if cpu_hint is not None:
            cpu_cap = min(cpu_cap, cpu_hint)

        cpu = _clamp(cpu_cap, 0.0, avail_cpu)
        ram = _clamp(ram_cap, 0.0, avail_ram)

        # Enforce floors if possible; if not possible, return zeros to skip scheduling now.
        if cpu > 0 and cpu < cpu_floor:
            if avail_cpu >= cpu_floor:
                cpu = cpu_floor
            else:
                return 0.0, 0.0
        if ram > 0 and ram < ram_floor:
            if avail_ram >= ram_floor:
                ram = ram_floor
            else:
                return 0.0, 0.0

        return cpu, ram

    # ---- Ingest new pipelines ----
    for p in pipelines:
        # Unknown priority types fall back to BATCH for safety.
        pr = getattr(p, "priority", Priority.BATCH_PIPELINE)
        if pr not in s.queues:
            pr = Priority.BATCH_PIPELINE
        s.queues[pr].append(p)

    # ---- Process results (learn from failures) ----
    for r in results:
        # If a container failed, decide whether to retry with more RAM or kill the pipeline.
        if r.failed():
            pid = getattr(r, "pipeline_id", None)
            # Some simulators may not expose pipeline_id on result; fall back to "do nothing".
            if pid is None:
                continue

            if _is_oom_error(getattr(r, "error", None)):
                # Exponential backoff on RAM for this pipeline.
                prev_hint = s.pipeline_ram_hint.get(pid, getattr(r, "ram", None))
                if prev_hint is None:
                    # If we don't know what we asked for, start from a conservative fraction of the pool max.
                    pool = s.executor.pools[r.pool_id]
                    prev_hint = max(0.25 * pool.max_ram_pool, 0.10 * pool.max_ram_pool)

                pool = s.executor.pools[r.pool_id]
                new_hint = min(prev_hint * 2.0, pool.max_ram_pool)
                s.pipeline_ram_hint[pid] = new_hint
                s.pipeline_oom_count[pid] = s.pipeline_oom_count.get(pid, 0) + 1

                # Also nudge CPU down a bit after repeated OOMs (often helps by reducing memory pressure).
                if s.pipeline_oom_count[pid] >= 2:
                    prev_cpu = s.pipeline_cpu_hint.get(pid, getattr(r, "cpu", None))
                    if prev_cpu is not None:
                        s.pipeline_cpu_hint[pid] = max(1.0, 0.75 * prev_cpu)
            else:
                # Non-OOM failure: treat as non-retryable for this simple policy.
                s.dead_pipelines.add(pid)
        else:
            # On success, clear OOM streak (but keep hints to stay safe).
            pid = getattr(r, "pipeline_id", None)
            if pid is not None and pid in s.pipeline_oom_count:
                s.pipeline_oom_count[pid] = 0

    # If nothing changed, exit early.
    if not pipelines and not results:
        return [], []

    suspensions: List["Suspend"] = []
    assignments: List["Assignment"] = []

    # ---- Scheduling loop ----
    # Try to fill each pool with highest-priority ready work, with gentle reservations for batch.
    for pool_id in _pool_order_by_headroom():
        pool = s.executor.pools[pool_id]

        # Keep placing until no more headroom or no runnable ops.
        # (This is still conservative because each op grabs a chunk of resources, and batch is capped.)
        while pool.avail_cpu_pool > 0 and pool.avail_ram_pool > 0:
            chosen_p = None
            chosen_pr = None

            # Pick next runnable pipeline in priority order.
            for pr in _prio_order():
                q = s.queues[pr]
                if not q:
                    continue

                # Round-robin scan within the queue to avoid a stuck head blocking others.
                n = len(q)
                for _ in range(n):
                    cand = q.popleft()

                    if _pipeline_done_or_dead(cand):
                        continue

                    if _pipeline_has_nonretryable_failures(cand):
                        # Mark dead to avoid spinning forever.
                        s.dead_pipelines.add(cand.pipeline_id)
                        continue

                    op = _next_assignable_op(cand)
                    if op is None:
                        # Not runnable yet; put it back and keep scanning.
                        q.append(cand)
                        continue

                    # Found runnable pipeline; keep it (we'll requeue after assignment).
                    chosen_p = cand
                    chosen_pr = pr
                    chosen_op = op
                    break

                if chosen_p is not None:
                    break

            if chosen_p is None:
                # No runnable work for this pool right now.
                break

            cpu, ram = _alloc_for(chosen_pr, pool, chosen_p.pipeline_id)
            if cpu <= 0 or ram <= 0:
                # Can't fit this priority/pipeline in this pool right now; requeue and stop for this pool.
                s.queues[chosen_pr].appendleft(chosen_p)
                break

            assignments.append(
                Assignment(
                    ops=[chosen_op],
                    cpu=cpu,
                    ram=ram,
                    priority=chosen_pr,
                    pool_id=pool_id,
                    pipeline_id=chosen_p.pipeline_id,
                )
            )

            # Requeue for round-robin fairness (pipeline still has more ops).
            s.queues[chosen_pr].append(chosen_p)

            # Note: pool.avail_* is expected to be updated by the simulator after applying assignments.
            # We conservatively break here to avoid assuming immediate in-function mutation.
            break

    return suspensions, assignments
