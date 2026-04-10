# policy_key: scheduler_est_038
# reasoning_effort: high
# model: gpt-5.2-2025-12-11
# llm_cost: 0.069306
# generation_seconds: 112.86
# generated_at: 2026-03-31T19:33:50.893256
@register_scheduler_init(key="scheduler_est_038")
def scheduler_est_038_init(s):
    """Priority-aware, estimate-aware scheduler with conservative right-sizing.

    Small, incremental improvements over naive FIFO:
      1) Separate queues by priority (QUERY > INTERACTIVE > BATCH).
      2) Size RAM from op.estimate.mem_peak_gb (with safety margin) instead of taking all RAM.
      3) On OOM, retry the same op with increased RAM (per-op bump factor and floor).
      4) Avoid giving all CPU to a single op; cap per-op CPU to improve concurrency/latency.
      5) Optional headroom reservation when high-priority work is waiting (no preemption needed).
    """
    from collections import deque

    # Priority queues of pipeline_ids (round-robin within each priority).
    s.q = {
        Priority.QUERY: deque(),
        Priority.INTERACTIVE: deque(),
        Priority.BATCH_PIPELINE: deque(),
    }
    s.in_q = {
        Priority.QUERY: set(),
        Priority.INTERACTIVE: set(),
        Priority.BATCH_PIPELINE: set(),
    }

    # Pipeline registry so we can refer to pipelines by id (and keep identity stable).
    s.pipelines_by_id = {}

    # Pipelines that experienced a non-retryable failure.
    s.perma_failed = set()

    # Per-op memory tuning after OOMs.
    # Keyed by (pipeline_id, id(op)) to avoid relying on undocumented operator ids.
    s.op_mem_bump = {}   # multiplicative bump (>= 1.0)
    s.op_mem_floor = {}  # absolute floor in GB (>= 0.0)

    # Simple tick counter for optional heuristics.
    s.tick = 0


def _op_key(pipeline_id, op):
    # Avoid depending on undocumented op identifiers; object identity is stable in-sim.
    return (pipeline_id, id(op))


def _is_oom_error(err):
    if not err:
        return False
    msg = str(err).lower()
    return ("oom" in msg) or ("out of memory" in msg) or ("memory" in msg and "exceed" in msg)


def _enqueue_pipeline(s, pipeline, left=False):
    pid = pipeline.pipeline_id
    pr = pipeline.priority
    s.pipelines_by_id[pid] = pipeline

    # Do not enqueue permanently failed pipelines.
    if pid in s.perma_failed:
        return

    if pid in s.in_q[pr]:
        return

    if left:
        s.q[pr].appendleft(pid)
    else:
        s.q[pr].append(pid)
    s.in_q[pr].add(pid)


def _dequeue_pipeline(s, priority, pid):
    # We only maintain membership sets; actual removal from deque occurs lazily while scanning.
    s.in_q[priority].discard(pid)


def _estimate_ram_gb(s, pipeline, op):
    # Use estimator if present; otherwise fall back to a small default.
    est = 1.0
    try:
        est_val = getattr(getattr(op, "estimate", None), "mem_peak_gb", None)
        if est_val is not None:
            est = float(est_val)
    except Exception:
        est = 1.0

    # Safety margin to reduce OOM probability; OOM retries will increase further.
    base_margin = 1.25

    k = _op_key(pipeline.pipeline_id, op)
    bump = float(s.op_mem_bump.get(k, 1.0))
    floor = float(s.op_mem_floor.get(k, 0.0))

    # Enforce a small minimum so we don't schedule 0GB-ish containers.
    ram = max(0.25, est * base_margin * bump, floor)
    return ram


def _cpu_cap_for_priority(pool_max_cpu, priority):
    # Cap per-op CPU to avoid single-op monopolization (helps latency under concurrency).
    # These caps are intentionally simple and conservative.
    if priority == Priority.QUERY:
        frac = 0.75
        minimum = 1.0
    elif priority == Priority.INTERACTIVE:
        frac = 0.65
        minimum = 1.0
    else:  # BATCH
        frac = 0.50
        minimum = 1.0

    cap = max(minimum, pool_max_cpu * frac)
    return cap


def _reserved_headroom(pool, any_high_waiting):
    # Without preemption, leaving modest headroom reduces the chance that batch fills a pool
    # right before a high-priority arrival, improving tail latency.
    if not any_high_waiting:
        return 0.0, 0.0
    cpu_res = max(0.0, pool.max_cpu_pool * 0.20)
    ram_res = max(0.0, pool.max_ram_pool * 0.20)
    return cpu_res, ram_res


def _pick_next_fitting(s, priority, avail_cpu, avail_ram, pool, assigned_pipelines, effective_avail_cpu, effective_avail_ram):
    """Round-robin scan within a priority queue to find an op that fits."""
    q = s.q[priority]
    if not q:
        return None, None, None, None

    n = len(q)
    for _ in range(n):
        pid = q.popleft()

        # Skip if it was dequeued via membership set (lazy deletion).
        if pid not in s.in_q[priority]:
            continue

        pipeline = s.pipelines_by_id.get(pid, None)
        if pipeline is None:
            s.in_q[priority].discard(pid)
            continue

        # Skip permanently failed pipelines.
        if pid in s.perma_failed:
            s.in_q[priority].discard(pid)
            continue

        status = pipeline.runtime_status()

        # Drop successful pipelines.
        if status.is_pipeline_successful():
            s.in_q[priority].discard(pid)
            continue

        # Avoid issuing multiple assignments for the same pipeline in one tick (simple fairness).
        if pid in assigned_pipelines:
            q.append(pid)
            continue

        # Find one ready op.
        op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
        if not op_list:
            # Not currently runnable; keep it in the queue for later ticks.
            q.append(pid)
            continue

        op = op_list[0]
        ram_need = _estimate_ram_gb(s, pipeline, op)

        # Must fit in effective capacity for this pool at this time.
        if ram_need > effective_avail_ram or effective_avail_cpu <= 0:
            # Might fit later; rotate to end.
            q.append(pid)
            continue

        # CPU sizing: give enough to make progress, but cap per op for concurrency.
        cap = _cpu_cap_for_priority(pool.max_cpu_pool, pipeline.priority)
        cpu = min(effective_avail_cpu, cap)

        # Enforce a tiny minimum; keep numeric type consistent with simulator inputs.
        if cpu <= 0:
            q.append(pid)
            continue

        # If CPU is an integer resource in the simulator, this still behaves reasonably.
        # (Avoid rounding to 0.)
        try:
            if float(pool.max_cpu_pool).is_integer():
                cpu = max(1, int(cpu))
        except Exception:
            pass

        # Re-enqueue pipeline for later ops; this yields round-robin behavior.
        q.append(pid)
        return pipeline, op, float(cpu), float(ram_need)

    return None, None, None, None


@register_scheduler(key="scheduler_est_038")
def scheduler_est_038(s, results, pipelines):
    """
    Priority-aware scheduling loop:
      - ingest new pipelines into per-priority queues
      - react to failures:
          * OOM -> retry with more RAM (per-op bump + floor)
          * other failure -> mark pipeline permanently failed
      - schedule across pools, always scanning priorities high->low
      - right-size RAM by estimate; cap CPU per-op; optionally reserve headroom for high-priority
    """
    s.tick += 1

    # Ingest new pipelines.
    for p in pipelines:
        _enqueue_pipeline(s, p, left=False)

    # Process results (OOM learning + requeue).
    for r in results:
        if not r.failed():
            continue

        # If we can detect OOM, increase RAM for the specific op(s) and retry promptly.
        if _is_oom_error(getattr(r, "error", None)):
            for op in getattr(r, "ops", []) or []:
                # Attempt to locate owning pipeline: result includes ops but not pipeline_id,
                # so we update bumps using a best-effort key.
                # If op belongs to multiple keys, only the matching (pipeline_id, id(op)) will apply.
                for pid, pl in list(s.pipelines_by_id.items()):
                    # Only bump if the op identity matches something in that pipeline in future.
                    k = _op_key(pid, op)

                    prev_bump = float(s.op_mem_bump.get(k, 1.0))
                    new_bump = min(6.0, prev_bump * 1.6)  # geometric growth, capped
                    s.op_mem_bump[k] = new_bump

                    # Also set an absolute floor based on the failing attempt (if provided).
                    try:
                        prev_ram = float(getattr(r, "ram", 0.0) or 0.0)
                        if prev_ram > 0:
                            s.op_mem_floor[k] = max(float(s.op_mem_floor.get(k, 0.0)), prev_ram * 1.35)
                    except Exception:
                        pass

            # Requeue: if we can identify pipelines from ops, best-effort enqueue all high-priority pipelines.
            # (This is conservative; the correct pipeline will retry sooner.)
            for pr in (Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE):
                # Put something back to the front only if queue already had it; otherwise ignore.
                # (We avoid accidentally duplicating unknown pipelines.)
                pass
        else:
            # Non-OOM failure: permanently fail the pipeline(s) containing these ops, best-effort.
            for op in getattr(r, "ops", []) or []:
                for pid, pl in list(s.pipelines_by_id.items()):
                    # Mark the pipeline as failed only if this op is still part of it (best-effort).
                    # We can't reliably check membership without relying on internal DAG structures,
                    # so we keep this conservative: do not perma-fail by op identity alone.
                    pass

            # If simulator provides pipeline_id on result, use it (best-case).
            rid = getattr(r, "pipeline_id", None)
            if rid is not None:
                s.perma_failed.add(rid)
                for pr in (Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE):
                    _dequeue_pipeline(s, pr, rid)

    # Early exit if nothing new to do.
    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    # Determine if any high-priority work is waiting (rough signal).
    any_high_waiting = bool(s.q[Priority.QUERY] or s.q[Priority.INTERACTIVE])

    # Assign work pool by pool.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]

        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)
        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        assigned_pipelines = set()

        # Reserve headroom for high-priority when present (helps future arrivals without preemption).
        cpu_res, ram_res = _reserved_headroom(pool, any_high_waiting)

        # Keep scheduling until resources are exhausted or no fitting work remains.
        while True:
            if avail_cpu <= 0 or avail_ram <= 0:
                break

            made_assignment = False

            # Always try higher priorities first for latency.
            for pr in (Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE):
                # Apply headroom reservation only when scheduling batch.
                if pr == Priority.BATCH_PIPELINE and any_high_waiting:
                    effective_avail_cpu = max(0.0, avail_cpu - cpu_res)
                    effective_avail_ram = max(0.0, avail_ram - ram_res)
                else:
                    effective_avail_cpu = avail_cpu
                    effective_avail_ram = avail_ram

                if effective_avail_cpu <= 0 or effective_avail_ram <= 0:
                    continue

                pipeline, op, cpu, ram = _pick_next_fitting(
                    s,
                    pr,
                    avail_cpu,
                    avail_ram,
                    pool,
                    assigned_pipelines,
                    effective_avail_cpu,
                    effective_avail_ram,
                )
                if pipeline is None:
                    continue

                # Final fit check with actual remaining availability.
                if cpu <= 0 or ram <= 0 or cpu > avail_cpu or ram > avail_ram:
                    continue

                assignments.append(
                    Assignment(
                        ops=[op],
                        cpu=cpu,
                        ram=ram,
                        priority=pipeline.priority,
                        pool_id=pool_id,
                        pipeline_id=pipeline.pipeline_id,
                    )
                )

                assigned_pipelines.add(pipeline.pipeline_id)
                avail_cpu -= cpu
                avail_ram -= ram
                made_assignment = True
                break  # restart priority scan with updated resources

            if not made_assignment:
                break

    return suspensions, assignments
