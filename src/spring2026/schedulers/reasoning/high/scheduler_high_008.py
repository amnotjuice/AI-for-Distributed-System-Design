# policy_key: scheduler_high_008
# reasoning_effort: high
# model: gpt-5.2-2025-12-11
# llm_cost: 0.109118
# generation_seconds: 160.25
# generated_at: 2026-03-12T22:41:58.482367
@register_scheduler_init(key="scheduler_high_008")
def scheduler_high_008_init(s):
    """Priority-aware, OOM-adaptive scheduler (incremental improvement over naive FIFO).

    Main improvements over the naive example:
      1) Priority queues: always try QUERY/INTERACTIVE before BATCH.
      2) Batch gating + small reservation: avoid launching new batch work when high-priority work is waiting.
      3) Fill the pool: issue multiple assignments per tick (not just one per pool).
      4) Retry FAILED operators, but only keep retrying on likely-OOM; otherwise blacklist the pipeline.
      5) Simple OOM-driven RAM backoff per (pipeline, op) to reduce repeated fast-fail OOM churn.
    """
    import collections

    # Per-priority FIFO queues
    s.q_by_prio = {
        Priority.QUERY: collections.deque(),
        Priority.INTERACTIVE: collections.deque(),
        Priority.BATCH_PIPELINE: collections.deque(),
    }

    # Track which pipeline_ids are currently enqueued (prevents unbounded duplication)
    s.enqueued_pipeline_ids = set()

    # Per-(pipeline_id, op_key) RAM multiplier; increases on OOM failures
    s.ram_mult = {}

    # Per-(pipeline_id, op_key) retry counters (we mainly care about OOM retries)
    s.retry_count = {}

    # Pipelines we will no longer retry (e.g., non-OOM failures)
    s.blacklisted_pipelines = set()

    # Heuristic knobs (kept deliberately simple)
    s.target_concurrency = 4  # how many "slots" we try to create per pool
    s.max_oom_retries_per_op = 3
    s.max_ram_multiplier = 16.0

    # Fairness knob: after many consecutive high-priority launches, allow a batch launch
    s.max_high_streak = 8
    s.high_streak = 0


def _sched008_norm_prio(p):
    # Default unknown priorities to batch-like behavior
    if p in (Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE):
        return p
    return Priority.BATCH_PIPELINE


def _sched008_is_oom_error(err) -> bool:
    if not err:
        return False
    msg = str(err).lower()
    return ("oom" in msg) or ("out of memory" in msg) or ("cuda out of memory" in msg) or ("memoryerror" in msg)


def _sched008_pool_order(s):
    # Prefer scheduling into pools with most headroom (helps latency under fragmentation)
    pools = s.executor.pools
    def score(i):
        p = pools[i]
        return float(p.avail_cpu_pool) * float(p.avail_ram_pool)
    return sorted(range(s.executor.num_pools), key=score, reverse=True)


def _sched008_has_high_waiting(s) -> bool:
    return bool(s.q_by_prio[Priority.QUERY]) or bool(s.q_by_prio[Priority.INTERACTIVE])


def _sched008_pick_pipeline_from_queue(q, scheduled_pipeline_ids, blacklisted_pipelines):
    """Pop left until we find a pipeline not already scheduled this tick.
    Returns (pipeline, None) if picked, or (None, requeued_list) if none found.
    """
    requeued = []
    picked = None
    scanned = 0
    qlen = len(q)
    while q and scanned < qlen:
        scanned += 1
        p = q.popleft()
        if p.pipeline_id in blacklisted_pipelines:
            # Drop blacklisted pipelines on sight
            continue
        if p.pipeline_id in scheduled_pipeline_ids:
            requeued.append(p)
            continue
        picked = p
        break
    return picked, requeued


def _sched008_base_slot_resources(pool, target_concurrency: int):
    # Create a "slot" sized resource chunk, then scale by priority.
    # Use floats to avoid integer truncation surprises.
    slot_cpu = float(pool.max_cpu_pool) / float(max(1, target_concurrency))
    slot_ram = float(pool.max_ram_pool) / float(max(1, target_concurrency))
    # Avoid zeros in tiny pools
    slot_cpu = max(slot_cpu, 0.1)
    slot_ram = max(slot_ram, 0.1)
    return slot_cpu, slot_ram


@register_scheduler(key="scheduler_high_008")
def scheduler_high_008_scheduler(s, results: List["ExecutionResult"],
                                 pipelines: List["Pipeline"]) -> Tuple[List["Suspend"], List["Assignment"]]:
    suspensions: List["Suspend"] = []
    assignments: List["Assignment"] = []

    # Use simulator-provided ASSIGNABLE_STATES if available; otherwise define it.
    assignable_states = globals().get("ASSIGNABLE_STATES", [OperatorState.PENDING, OperatorState.FAILED])

    # --------------------------
    # 1) Ingest new pipelines
    # --------------------------
    for p in pipelines:
        if p.pipeline_id in s.blacklisted_pipelines:
            continue
        if p.pipeline_id not in s.enqueued_pipeline_ids:
            pr = _sched008_norm_prio(p.priority)
            s.q_by_prio[pr].append(p)
            s.enqueued_pipeline_ids.add(p.pipeline_id)

    # --------------------------
    # 2) Learn from results (OOM backoff, blacklist non-OOM)
    # --------------------------
    for r in results:
        if not r.failed():
            continue

        # Update per-op stats
        is_oom = _sched008_is_oom_error(r.error)
        for op in (r.ops or []):
            op_key = (getattr(r, "pipeline_id", None), id(op))  # pipeline_id may not exist on result
            # If result doesn't carry pipeline_id, we still key by (None, id(op)) as best effort.
            s.retry_count[op_key] = s.retry_count.get(op_key, 0) + 1

            if is_oom:
                cur = s.ram_mult.get(op_key, 1.0)
                nxt = min(s.max_ram_multiplier, max(cur * 2.0, cur + 1.0))
                s.ram_mult[op_key] = nxt

        # If it doesn't look like OOM, treat as "real failure" and stop retrying that pipeline
        # (keeps the scheduler from burning cycles on deterministic failures).
        if not is_oom:
            # Best effort: infer pipeline_id from ops' attached pipeline, otherwise can't blacklist precisely.
            # Many simulators will preserve pipeline_id on the ExecutionResult; if not, we do nothing here.
            pid = getattr(r, "pipeline_id", None)
            if pid is not None:
                s.blacklisted_pipelines.add(pid)

    # If there's nothing to do, exit quickly
    if (
        not results
        and not pipelines
        and not s.q_by_prio[Priority.QUERY]
        and not s.q_by_prio[Priority.INTERACTIVE]
        and not s.q_by_prio[Priority.BATCH_PIPELINE]
    ):
        return suspensions, assignments

    # --------------------------
    # 3) Schedule: priority-aware, fill pools, batch gating
    # --------------------------
    scheduled_pipeline_ids = set()
    high_waiting = _sched008_has_high_waiting(s)

    # A small per-pool reservation to avoid consuming *all* headroom with new batch launches.
    # We don't preempt (no safe access to running container inventory), so this is the simplest
    # latency protection that still works with the exposed interface.
    for pool_id in _sched008_pool_order(s):
        pool = s.executor.pools[pool_id]

        # We will try to pack multiple assignments per pool per tick.
        # Stop when resources are too low to be useful.
        safety_iter = 0
        while safety_iter < 1000:
            safety_iter += 1

            avail_cpu = float(pool.avail_cpu_pool)
            avail_ram = float(pool.avail_ram_pool)
            if avail_cpu <= 0.0 or avail_ram <= 0.0:
                break

            slot_cpu, slot_ram = _sched008_base_slot_resources(pool, s.target_concurrency)

            # Reserve ~1 slot if high-priority work is waiting; batch must not eat into this.
            reserve_cpu = slot_cpu if high_waiting else 0.0
            reserve_ram = slot_ram if high_waiting else 0.0

            # Decide which priority to pick next
            # Priority order: QUERY > INTERACTIVE > BATCH
            chosen_prio = None

            # High-priority selection always wins if present
            if s.q_by_prio[Priority.QUERY]:
                chosen_prio = Priority.QUERY
            elif s.q_by_prio[Priority.INTERACTIVE]:
                chosen_prio = Priority.INTERACTIVE
            elif s.q_by_prio[Priority.BATCH_PIPELINE]:
                # Batch gating: if high is waiting, only allow occasional batch
                # and only from non-reserved headroom.
                if high_waiting:
                    can_spare = (avail_cpu - reserve_cpu) > 0.0 and (avail_ram - reserve_ram) > 0.0
                    if can_spare and s.high_streak >= s.max_high_streak:
                        chosen_prio = Priority.BATCH_PIPELINE
                    else:
                        # Don't launch new batch work while high is waiting
                        break
                else:
                    chosen_prio = Priority.BATCH_PIPELINE
            else:
                break  # no queued work at all

            q = s.q_by_prio[chosen_prio]

            # Pick a pipeline that we haven't already scheduled this tick
            pipeline, requeued = _sched008_pick_pipeline_from_queue(
                q, scheduled_pipeline_ids, s.blacklisted_pipelines
            )
            # Put back the ones we skipped this time
            for rp in requeued:
                q.append(rp)

            if pipeline is None:
                # No eligible pipeline of this priority right now.
                # Try lower priority in same pool iteration.
                if chosen_prio == Priority.QUERY:
                    if s.q_by_prio[Priority.INTERACTIVE]:
                        continue
                    if s.q_by_prio[Priority.BATCH_PIPELINE] and not high_waiting:
                        continue
                elif chosen_prio == Priority.INTERACTIVE:
                    if s.q_by_prio[Priority.BATCH_PIPELINE] and not high_waiting:
                        continue
                break

            # Drop completed pipelines; keep pipeline_ids consistent
            status = pipeline.runtime_status()
            if status.is_pipeline_successful():
                s.enqueued_pipeline_ids.discard(pipeline.pipeline_id)
                scheduled_pipeline_ids.discard(pipeline.pipeline_id)
                continue
            if pipeline.pipeline_id in s.blacklisted_pipelines:
                s.enqueued_pipeline_ids.discard(pipeline.pipeline_id)
                continue

            # Find an assignable op (prefer retrying FAILED ops first)
            failed_ops = status.get_ops([OperatorState.FAILED], require_parents_complete=True)[:1]
            if failed_ops:
                op_list = failed_ops
            else:
                op_list = status.get_ops(assignable_states, require_parents_complete=True)[:1]

            if not op_list:
                # Not ready yet; requeue and move on.
                q.append(pipeline)
                continue

            op = op_list[0]

            # Compute resource request based on priority and available headroom
            # (small, incremental heuristic: give more to high priority, but keep concurrency).
            if chosen_prio == Priority.QUERY:
                cpu_factor, ram_factor = 2.0, 2.0
            elif chosen_prio == Priority.INTERACTIVE:
                cpu_factor, ram_factor = 1.5, 1.5
            else:
                cpu_factor, ram_factor = 1.0, 1.0

            # For batch while high is waiting, never allocate from reserved headroom.
            effective_avail_cpu = avail_cpu
            effective_avail_ram = avail_ram
            if chosen_prio == Priority.BATCH_PIPELINE and high_waiting:
                effective_avail_cpu = max(0.0, avail_cpu - reserve_cpu)
                effective_avail_ram = max(0.0, avail_ram - reserve_ram)
                if effective_avail_cpu <= 0.0 or effective_avail_ram <= 0.0:
                    q.append(pipeline)
                    break

            # Apply OOM backoff if we have history for this op.
            op_key = (pipeline.pipeline_id, id(op))
            ram_mult = float(s.ram_mult.get(op_key, 1.0))

            # Base request from slot sizing, then capped by (effective) availability.
            req_cpu = min(effective_avail_cpu, max(0.1, slot_cpu * cpu_factor))
            req_ram = min(effective_avail_ram, max(0.1, slot_ram * ram_factor * ram_mult))

            # If the op has repeatedly OOMed, bias strongly towards RAM (helps avoid infinite loops).
            # Keep CPU non-zero.
            retries = s.retry_count.get(op_key, 0)
            if retries > 0 and ram_mult > 1.0:
                req_cpu = min(req_cpu, max(0.1, slot_cpu))  # don't overspend CPU when memory is the bottleneck

            # If we can't allocate meaningful resources, requeue and stop trying in this pool.
            if req_cpu <= 0.0 or req_ram <= 0.0:
                q.append(pipeline)
                break

            # Enforce a retry cap for OOM-flapping operators: blacklist pipeline once exceeded.
            # (We can only do best-effort here without direct access to op failure reasons.)
            if retries >= s.max_oom_retries_per_op and ram_mult >= s.max_ram_multiplier:
                s.blacklisted_pipelines.add(pipeline.pipeline_id)
                s.enqueued_pipeline_ids.discard(pipeline.pipeline_id)
                continue

            assignments.append(
                Assignment(
                    ops=[op],
                    cpu=req_cpu,
                    ram=req_ram,
                    priority=pipeline.priority,
                    pool_id=pool_id,
                    pipeline_id=pipeline.pipeline_id,
                )
            )

            # Update streak tracking for fairness gating
            if chosen_prio in (Priority.QUERY, Priority.INTERACTIVE):
                s.high_streak += 1
            else:
                s.high_streak = 0

            scheduled_pipeline_ids.add(pipeline.pipeline_id)

            # Requeue pipeline for future ops (we keep it enqueued until fully completed/blacklisted)
            q.append(pipeline)

    return suspensions, assignments
