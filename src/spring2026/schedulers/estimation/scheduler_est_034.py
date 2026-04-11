# policy_key: scheduler_est_034
# reasoning_effort: low
# model: gpt-5.2-2025-12-11
# llm_cost: 0.046766
# generation_seconds: 41.75
# generated_at: 2026-04-01T00:07:29.651986
@register_scheduler_init(key="scheduler_est_034")
def scheduler_est_034_init(s):
    """
    Priority-aware FIFO with conservative RAM sizing + OOM backoff.

    Incremental improvements over naive FIFO:
      1) Maintain separate queues per priority and schedule high priority first
      2) Prefer a "fast lane" pool (pool 0) for QUERY/INTERACTIVE when multiple pools exist
      3) Use op.estimate.mem_peak_gb as a RAM floor (extra RAM is "free")
      4) On failures (likely OOM in this simplified model), retry with increased RAM multiplier
      5) Assign more than one op per tick/pool when resources allow (improves latency/throughput)
    """
    s.queues = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }
    # Per-pipeline backoff state to reduce repeated OOMs.
    # ram_mult increases allocated RAM floor; retries caps infinite loops.
    s.pipe_state = {}  # pipeline_id -> {"ram_mult": float, "retries": int}
    s.max_retries = 4
    s.base_ram_gb = 0.5  # small but non-zero default when estimate is missing
    s.ram_mult_step = 1.7


@register_scheduler(key="scheduler_est_034")
def scheduler_est_034_scheduler(s, results, pipelines):
    """
    Scheduler step:
      - Ingest new pipelines into per-priority FIFO queues
      - Update backoff state from execution results (increase RAM multiplier on failure)
      - For each pool, repeatedly assign one ready op at a time, preferring higher priority
    """
    # --- ingest new arrivals ---
    for p in pipelines:
        if p.priority not in s.queues:
            # Unknown priority: treat as lowest priority.
            s.queues[Priority.BATCH_PIPELINE].append(p)
        else:
            s.queues[p.priority].append(p)

        if p.pipeline_id not in s.pipe_state:
            s.pipe_state[p.pipeline_id] = {"ram_mult": 1.0, "retries": 0}

    # Early exit if nothing changed.
    if not pipelines and not results:
        return [], []

    # --- update state based on results ---
    # We assume failures are often RAM-related; we react by increasing RAM multiplier for the pipeline.
    for r in results:
        if hasattr(r, "failed") and r.failed():
            # Best-effort: try to locate pipeline_id if present; otherwise, backoff by priority is too coarse.
            # In this simulator interface, we reliably have container_id/pool_id, but not pipeline_id.
            # We therefore only apply RAM backoff when we can infer pipeline_id from attached ops (if present).
            pipe_id = None
            # Some implementations attach pipeline_id to results; use it if available.
            if hasattr(r, "pipeline_id"):
                pipe_id = r.pipeline_id
            else:
                # Try to infer from op metadata if it carries pipeline_id.
                if getattr(r, "ops", None):
                    op0 = r.ops[0]
                    if hasattr(op0, "pipeline_id"):
                        pipe_id = op0.pipeline_id

            if pipe_id is not None:
                st = s.pipe_state.get(pipe_id)
                if st is None:
                    st = {"ram_mult": 1.0, "retries": 0}
                    s.pipe_state[pipe_id] = st
                st["retries"] += 1
                st["ram_mult"] *= s.ram_mult_step

    suspensions = []
    assignments = []

    def _is_done_or_terminal(pipeline):
        status = pipeline.runtime_status()
        if status.is_pipeline_successful():
            return True
        # If the pipeline has failures and we've exhausted retries, drop it (terminal).
        st = s.pipe_state.get(pipeline.pipeline_id, {"retries": 0})
        has_failures = status.state_counts[OperatorState.FAILED] > 0
        if has_failures and st["retries"] >= s.max_retries:
            return True
        return False

    def _next_ready_op(pipeline):
        status = pipeline.runtime_status()
        # If parents complete, we can retry FAILED ops as well (ASSIGNABLE_STATES includes FAILED).
        ops = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        if not ops:
            return None
        return ops[0]

    def _ram_floor_for_op(pipeline, op, pool_max_ram):
        # Use estimator as a RAM floor; apply pipeline-level multiplier after failures.
        est = None
        if hasattr(op, "estimate") and op.estimate is not None and hasattr(op.estimate, "mem_peak_gb"):
            est = op.estimate.mem_peak_gb
        est_floor = float(est) if (est is not None) else float(s.base_ram_gb)

        st = s.pipe_state.get(pipeline.pipeline_id, {"ram_mult": 1.0})
        floor = est_floor * float(st.get("ram_mult", 1.0))

        # Clamp to pool max; if still too small, the op may OOM, but we will backoff on failure.
        if floor > pool_max_ram:
            floor = pool_max_ram
        if floor <= 0:
            floor = min(float(s.base_ram_gb), pool_max_ram)
        return floor

    def _pick_pool_for_priority(priority):
        # Small, obvious multi-pool improvement:
        # - If multiple pools exist, reserve pool 0 as a "fast lane" for QUERY/INTERACTIVE.
        # - Batch prefers the last pool.
        if s.executor.num_pools <= 1:
            return [0]
        if priority in (Priority.QUERY, Priority.INTERACTIVE):
            # Try fast lane first, then others.
            return [0] + [i for i in range(1, s.executor.num_pools)]
        # Batch: try non-fast-lane first to reduce interference with interactive latency.
        return [i for i in range(1, s.executor.num_pools)] + [0]

    # We schedule pools in order but choose candidates depending on their priority affinity.
    # For each pool, repeatedly assign the highest priority ready op that fits.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool

        # If no resources, nothing to do in this pool.
        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # Keep assigning while we still can do useful work.
        # We enforce at least 1 CPU per op to avoid 0-CPU assignments.
        made_progress = True
        while made_progress:
            made_progress = False

            # Global priority order (latency-first):
            # QUERY > INTERACTIVE > BATCH_PIPELINE
            for prio in (Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE):
                # If this pool is not ideal for this priority, we still allow it,
                # but we bias the search so high priority first consumes pool 0.
                preferred_pools = _pick_pool_for_priority(prio)
                if pool_id not in preferred_pools:
                    continue

                q = s.queues.get(prio, [])
                if not q:
                    continue

                # Scan FIFO queue until we find something runnable; keep order stable by rotating.
                n = len(q)
                for _ in range(n):
                    pipeline = q.pop(0)

                    if _is_done_or_terminal(pipeline):
                        # Drop completed/terminal pipelines.
                        continue

                    op = _next_ready_op(pipeline)
                    if op is None:
                        # Not ready yet (waiting on parents); keep it for later.
                        q.append(pipeline)
                        continue

                    # Compute RAM floor.
                    ram_need = _ram_floor_for_op(pipeline, op, pool.max_ram_pool)

                    # If it can't fit now, requeue and try others.
                    # Note: We only consider avail_* in this pool; another pool may have headroom.
                    if ram_need > avail_ram or avail_cpu < 1:
                        q.append(pipeline)
                        continue

                    # CPU sizing:
                    # - Give interactive/query as much as possible to reduce latency.
                    # - Give batch a smaller chunk to avoid monopolizing a pool.
                    if prio in (Priority.QUERY, Priority.INTERACTIVE):
                        cpu_need = avail_cpu  # grab remaining CPU to finish quickly
                    else:
                        # Cap batch per-assignment CPU to encourage sharing and reduce head-of-line blocking.
                        cpu_need = min(avail_cpu, max(1, pool.max_cpu_pool // 2))

                    if cpu_need < 1:
                        q.append(pipeline)
                        continue

                    assignments.append(
                        Assignment(
                            ops=[op],
                            cpu=cpu_need,
                            ram=ram_need,
                            priority=pipeline.priority,
                            pool_id=pool_id,
                            pipeline_id=pipeline.pipeline_id,
                        )
                    )

                    # Update local available resources (so we can pack multiple ops per tick).
                    avail_cpu -= cpu_need
                    avail_ram -= ram_need

                    # Put pipeline back for subsequent operators (DAG will gate readiness).
                    q.append(pipeline)

                    made_progress = True
                    break  # re-evaluate from highest priority with updated avail_*

                if made_progress:
                    break  # restart priority loop

            # Stop if pool is exhausted.
            if avail_cpu < 1 or avail_ram <= 0:
                break

    return suspensions, assignments
