# policy_key: scheduler_low_001
# reasoning_effort: low
# model: gpt-5.2-2025-12-11
# llm_cost: 0.040980
# generation_seconds: 40.39
# generated_at: 2026-03-12T15:39:49.867107
@register_scheduler_init(key="scheduler_low_001")
def scheduler_low_001_init(s):
    """Priority-aware, latency-oriented incremental improvement over naive FIFO.

    Key ideas (kept intentionally simple and robust):
      1) Priority queues: always schedule higher-priority pipelines first (QUERY > INTERACTIVE > BATCH).
      2) Avoid head-of-line blocking: don't hand an entire pool to a single operator; use small per-op slices
         so multiple short/interactive ops can run concurrently.
      3) Gentle OOM adaptation: if an op fails with an OOM-like error, retry the pipeline with a larger RAM
         "boost factor" next time it gets scheduled.
      4) Fairness within a priority: round-robin across pipelines by rotating the queue after scheduling an op.

    Notes:
      - We intentionally avoid preemption because we don't have a reliable way (from the provided interface)
        to map RUNNING ops to container_ids for suspension.
    """
    # Separate waiting queues per priority (lists of Pipeline)
    s.waiting_by_prio = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }

    # Track known pipeline ids to prevent accidental duplicate enqueues (best-effort).
    s.known_pipeline_ids = set()

    # Per-pipeline multiplicative RAM boost on OOM-like failures.
    s.pipeline_ram_boost = {}

    # Per-pipeline multiplicative CPU boost on repeated slowdowns (not measured here yet, kept for future use).
    s.pipeline_cpu_boost = {}

    # Config knobs (small, safe defaults)
    s.max_assignments_per_pool_per_tick = 8  # keep scheduling loop bounded
    s.base_cpu_slice_frac = 0.25            # default CPU slice = 25% of pool max (capped by avail)
    s.base_ram_slice_frac = 0.25            # default RAM slice = 25% of pool max (capped by avail)
    s.min_cpu = 1.0
    s.min_ram = 1.0
    s.max_ram_boost = 8.0
    s.max_cpu_boost = 4.0


def _is_oom_error(err) -> bool:
    if not err:
        return False
    msg = str(err).lower()
    # Common variants: "oom", "out of memory", "cuda oom" (even if irrelevant), "killed"
    return ("oom" in msg) or ("out of memory" in msg) or ("killed" in msg and "memory" in msg)


def _prio_order():
    # Higher priority first; QUERY is typically most latency sensitive.
    return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]


def _enqueue_pipeline(s, p):
    # Best-effort dedupe; pipelines may re-arrive in the 'pipelines' argument in some generators.
    if getattr(p, "pipeline_id", None) in s.known_pipeline_ids:
        return
    s.known_pipeline_ids.add(p.pipeline_id)
    s.waiting_by_prio[p.priority].append(p)


def _drop_pipeline(s, p):
    # Remove from known set; queue cleanup is done lazily during scheduling.
    if getattr(p, "pipeline_id", None) in s.known_pipeline_ids:
        s.known_pipeline_ids.remove(p.pipeline_id)
    # Also cleanup adaptive state to avoid unbounded growth.
    s.pipeline_ram_boost.pop(p.pipeline_id, None)
    s.pipeline_cpu_boost.pop(p.pipeline_id, None)


def _compute_slices(s, pool, pipeline_id):
    # RAM/CPU slicing: reserve only a fraction so we can co-schedule multiple ops (latency friendly).
    ram_boost = s.pipeline_ram_boost.get(pipeline_id, 1.0)
    cpu_boost = s.pipeline_cpu_boost.get(pipeline_id, 1.0)

    # Base slices based on pool max, then boosted, then capped by currently available resources.
    cpu = max(s.min_cpu, pool.max_cpu_pool * s.base_cpu_slice_frac * cpu_boost)
    ram = max(s.min_ram, pool.max_ram_pool * s.base_ram_slice_frac * ram_boost)

    cpu = min(cpu, pool.avail_cpu_pool)
    ram = min(ram, pool.avail_ram_pool)

    return cpu, ram


@register_scheduler(key="scheduler_low_001")
def scheduler_low_001_scheduler(s, results, pipelines):
    """
    Priority-aware scheduler with small-slice packing and OOM-adaptive retries.

    Returns:
      suspensions: [] (no preemption in this version)
      assignments: list of Assignment objects
    """
    # Ingest new pipelines
    for p in pipelines:
        _enqueue_pipeline(s, p)

    # If nothing changed, exit early (matches the example style)
    if not pipelines and not results:
        return [], []

    # Adaptation based on failures observed in results
    for r in results:
        if getattr(r, "failed", None) and r.failed():
            # Best-effort: bump RAM on OOM-like errors; otherwise do not retry indefinitely.
            if _is_oom_error(getattr(r, "error", None)):
                # We may not have direct pipeline_id on result, but we do have it in assignments.
                # When that's not available, we cannot safely attribute. Here, rely on r.ops to map
                # back to a pipeline is not possible with the given interface, so we apply a conservative
                # global bump keyed by container_id if pipeline_id is absent.
                #
                # However, the ExecutionResult interface in the prompt doesn't include pipeline_id.
                # So we use a fallback "last known" approach: no-op if we can't attribute.
                #
                # If your simulator attaches pipeline_id to result, this will work automatically.
                pid = getattr(r, "pipeline_id", None)
                if pid is not None:
                    cur = s.pipeline_ram_boost.get(pid, 1.0)
                    s.pipeline_ram_boost[pid] = min(s.max_ram_boost, max(cur * 2.0, cur + 0.5))
            else:
                # Non-OOM failures are treated as fatal at pipeline level by dropping once detected
                # in runtime_status below (has_failures check). We don't need to do anything here.
                pass

    suspensions = []
    assignments = []

    # Schedule per pool; pack multiple small ops for better latency (esp. QUERY/INTERACTIVE).
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]

        made = 0
        # Try to fill this pool with several assignments
        while made < s.max_assignments_per_pool_per_tick:
            if pool.avail_cpu_pool < s.min_cpu or pool.avail_ram_pool < s.min_ram:
                break

            scheduled_any = False

            # Always try higher priorities first
            for prio in _prio_order():
                q = s.waiting_by_prio[prio]
                if not q:
                    continue

                # Find one runnable op from some pipeline in this priority queue
                # We rotate the queue to provide round-robin fairness within the priority.
                selected_idx = None
                selected_pipeline = None
                selected_ops = None

                # Bound scan to queue length to avoid infinite loops
                for i in range(len(q)):
                    p = q[i]
                    status = p.runtime_status()

                    # Drop completed pipelines
                    if status.is_pipeline_successful():
                        selected_idx = i
                        selected_pipeline = p
                        selected_ops = None
                        break

                    # If pipeline has failures, only keep it if we believe it's OOM-retriable.
                    # With limited signals, treat any FAILED op as fatal unless we have a RAM boost set.
                    # (This is conservative; it avoids endless retry loops.)
                    has_failed_ops = status.state_counts[OperatorState.FAILED] > 0
                    if has_failed_ops and p.pipeline_id not in s.pipeline_ram_boost:
                        selected_idx = i
                        selected_pipeline = p
                        selected_ops = "DROP"
                        break

                    op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
                    if not op_list:
                        continue

                    selected_idx = i
                    selected_pipeline = p
                    selected_ops = op_list
                    break

                if selected_pipeline is None:
                    continue

                # Remove from queue for handling
                q.pop(selected_idx)

                # Handle cleanup/drop cases
                if selected_ops is None:
                    # Completed
                    _drop_pipeline(s, selected_pipeline)
                    scheduled_any = True  # we made progress by cleaning up
                    break

                if selected_ops == "DROP":
                    # Fatal pipeline
                    _drop_pipeline(s, selected_pipeline)
                    scheduled_any = True
                    break

                # Compute slices and assign
                cpu, ram = _compute_slices(s, pool, selected_pipeline.pipeline_id)

                # If slices are too small due to near-empty pool, stop trying to schedule here.
                if cpu < s.min_cpu or ram < s.min_ram:
                    # Put back at end and stop; pool has no capacity.
                    q.append(selected_pipeline)
                    scheduled_any = False
                    break

                assignments.append(
                    Assignment(
                        ops=selected_ops,
                        cpu=cpu,
                        ram=ram,
                        priority=selected_pipeline.priority,
                        pool_id=pool_id,
                        pipeline_id=selected_pipeline.pipeline_id,
                    )
                )

                # Rotate pipeline to end of its queue for fairness
                q.append(selected_pipeline)

                scheduled_any = True
                made += 1
                break  # go back to while loop to possibly schedule more work

            if not scheduled_any:
                break

    return suspensions, assignments
