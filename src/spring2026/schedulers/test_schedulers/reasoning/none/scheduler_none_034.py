# policy_key: scheduler_none_034
# reasoning_effort: none
# model: gpt-5.2-2025-12-11
# llm_cost: 0.046463
# generation_seconds: 50.42
# generated_at: 2026-04-09T22:11:32.444862
@register_scheduler_init(key="scheduler_none_034")
def scheduler_none_034_init(s):
    """Priority-aware, OOM-adaptive, low-churn scheduler.

    Goals (aligned to weighted-latency objective with heavy failure penalty):
      - Protect QUERY/INTERACTIVE latency via strict priority ordering + optional preemption.
      - Reduce failures by learning per-operator RAM requirements from OOMs and retrying.
      - Avoid starvation by aging lower-priority pipelines over time.
      - Keep churn low: only preempt when a high-priority op is ready and cannot fit.

    Core ideas:
      1) Maintain per-pipeline FIFO queues by priority.
      2) For each ready operator, choose a conservative RAM allocation:
           max(observed_success_ram, last_oom_ram * backoff, min_pool_share)
         and allocate CPU as a capped share to avoid monopolizing a pool.
      3) If a QUERY/INTERACTIVE op cannot be admitted, try to preempt a BATCH container.
      4) Age waiting pipelines so BATCH eventually runs even under steady high-priority load.
    """
    # Queues by priority (store pipeline_ids to avoid duplicates)
    s.q_query = []
    s.q_interactive = []
    s.q_batch = []

    # Track which pipelines we've seen to avoid repeated enqueue spam
    s.known_pipelines = set()

    # Retry / learning state
    # Keyed by (pipeline_id, op_id) -> learned RAM floor based on OOMs and successes
    s.op_ram_floor = {}          # learned minimal safe RAM (grows on OOM)
    s.op_ram_success = {}        # last known successful RAM (may be smaller than floor if no OOM)
    s.op_oom_count = {}          # OOM counters

    # Pipeline metadata for aging
    s.pipeline_meta = {}         # pipeline_id -> dict(priority=..., first_seen=..., last_enqueued=..., age_boost=...)

    # Soft timekeeping (ticks). Eudoxia is discrete; we just need monotonic increments.
    s.tick = 0

    # Policy knobs (conservative defaults)
    s.ram_backoff = 1.6          # multiply RAM on each OOM (capped by pool max)
    s.ram_min_share = 0.15       # minimal RAM share per assignment as fraction of pool max
    s.cpu_max_share = 0.60       # cap CPU per assignment as fraction of pool max (improves concurrency)
    s.cpu_min_share = 0.20       # minimal CPU share to prevent tiny slow allocations
    s.preempt_for_high = True
    s.max_preempt_per_tick = 2   # bound churn
    s.max_assignments_per_pool = 1  # simple: one new assignment per pool per tick

    # Aging knobs: after enough ticks, batch can "bubble up" slightly
    s.aging_start = 30           # ticks after which batch begins to gain boost
    s.aging_step = 10            # every this many ticks, increase boost
    s.aging_boost_cap = 3        # cap of priority boost levels


def _priority_rank(priority):
    # Lower is better
    if priority == Priority.QUERY:
        return 0
    if priority == Priority.INTERACTIVE:
        return 1
    return 2


def _get_queue_for_priority(s, priority):
    if priority == Priority.QUERY:
        return s.q_query
    if priority == Priority.INTERACTIVE:
        return s.q_interactive
    return s.q_batch


def _enqueue_pipeline(s, p):
    pid = p.pipeline_id
    if pid not in s.known_pipelines:
        s.known_pipelines.add(pid)
        s.pipeline_meta[pid] = {
            "priority": p.priority,
            "first_seen": s.tick,
            "last_enqueued": s.tick,
            "age_boost": 0,
        }
        _get_queue_for_priority(s, p.priority).append(pid)
    else:
        # Pipeline already known; only (re)enqueue if it is not currently in any queue.
        # This helps when we removed it earlier but it became runnable again.
        if pid not in s.q_query and pid not in s.q_interactive and pid not in s.q_batch:
            meta = s.pipeline_meta.get(pid)
            if meta is not None:
                meta["last_enqueued"] = s.tick
            _get_queue_for_priority(s, p.priority).append(pid)


def _pipeline_done_or_failed(p):
    st = p.runtime_status()
    if st.is_pipeline_successful():
        return True
    # If any failed operators exist, pipeline has failures (but we may retry FAILED ops)
    # We avoid dropping pipelines here; we only treat it as terminal if no retry is possible.
    # Terminal decision is handled by "no assignable ops and failed exists" logic in selection.
    return False


def _select_ready_op(pipeline):
    st = pipeline.runtime_status()

    # Prefer ready ops whose parents are complete
    ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
    if ops:
        return ops[0:1]

    # If nothing is parent-ready, allow non-parent-ready to avoid deadlock in odd DAG states
    ops2 = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=False)
    if ops2:
        return ops2[0:1]

    return []


def _op_key(pipeline_id, op):
    # Best-effort operator id extraction.
    # Eudoxia Operator objects typically have stable identity; fall back to repr.
    oid = getattr(op, "op_id", None)
    if oid is None:
        oid = getattr(op, "operator_id", None)
    if oid is None:
        oid = getattr(op, "id", None)
    if oid is None:
        oid = repr(op)
    return (pipeline_id, oid)


def _compute_age_boost(s, pipeline_id):
    meta = s.pipeline_meta.get(pipeline_id)
    if not meta:
        return 0
    waited = max(0, s.tick - meta["first_seen"])
    if waited < s.aging_start:
        return 0
    boost = min(s.aging_boost_cap, 1 + (waited - s.aging_start) // max(1, s.aging_step))
    return int(boost)


def _effective_rank(s, pipeline):
    base = _priority_rank(pipeline.priority)
    pid = pipeline.pipeline_id
    boost = _compute_age_boost(s, pid)
    # Only boost batch; keep query/interactive protected
    if pipeline.priority == Priority.BATCH_PIPELINE:
        # Reduce rank (better) by boost, but never outrank INTERACTIVE
        return max(1, base - boost)
    return base


def _learn_from_results(s, results):
    # Update RAM floors from failures (OOM) and successes.
    for r in results:
        if not getattr(r, "ops", None):
            continue
        # Some results may include multiple ops, but assignments typically pass one op.
        op0 = r.ops[0]
        # Pipeline id is not on ExecutionResult per the provided list, so infer from op if possible.
        # If unavailable, we can only keep coarse learning by op identity alone; but we prefer pipeline+op.
        pid = getattr(op0, "pipeline_id", None)
        if pid is None:
            # Can't safely attribute; skip learning to avoid cross-pipeline contamination.
            continue

        k = _op_key(pid, op0)
        used_ram = getattr(r, "ram", None)
        if used_ram is None:
            continue

        if r.failed():
            # Treat any failure with "oom" substring as OOM; otherwise ignore (could be user code).
            err = getattr(r, "error", "") or ""
            if isinstance(err, str) and ("oom" in err.lower() or "out of memory" in err.lower()):
                s.op_oom_count[k] = s.op_oom_count.get(k, 0) + 1
                prev_floor = s.op_ram_floor.get(k, 0.0)
                # Increase floor beyond what we tried.
                new_floor = max(prev_floor, float(used_ram) * s.ram_backoff)
                s.op_ram_floor[k] = new_floor
        else:
            # Success: record a successful RAM point and floor at least that.
            s.op_ram_success[k] = float(used_ram)
            prev_floor = s.op_ram_floor.get(k, 0.0)
            s.op_ram_floor[k] = max(prev_floor, float(used_ram))


def _current_running_containers_by_priority(s):
    # Best-effort: scan executor pools for running containers if exposed.
    # If not available, return empty lists -> preemption will be a no-op.
    running = {Priority.QUERY: [], Priority.INTERACTIVE: [], Priority.BATCH_PIPELINE: []}
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        containers = getattr(pool, "containers", None)
        if not containers:
            continue
        for c in containers:
            # Container objects likely have .priority, .container_id, .pool_id, .state
            pr = getattr(c, "priority", None)
            cid = getattr(c, "container_id", None)
            state = getattr(c, "state", None)
            if cid is None or pr is None:
                continue
            # Only preempt running/assigned containers (avoid double-suspending)
            if state is None or state in (OperatorState.ASSIGNED, OperatorState.RUNNING):
                running.setdefault(pr, []).append((pool_id, cid))
    return running


def _maybe_preempt_for_admission(s, needed_cpu, needed_ram, preferred_pool_id=None):
    # Try to free resources by suspending BATCH first, then INTERACTIVE if absolutely necessary.
    # Returns list of suspensions (bounded).
    suspensions = []
    if not s.preempt_for_high:
        return suspensions

    running = _current_running_containers_by_priority(s)
    victims = []
    # Prefer batch victims
    victims.extend(running.get(Priority.BATCH_PIPELINE, []))
    # As a last resort, allow interactive victims (but never preempt QUERY)
    victims.extend(running.get(Priority.INTERACTIVE, []))

    if not victims:
        return suspensions

    # Preempt from the preferred pool first to maximize local headroom
    if preferred_pool_id is not None:
        victims = sorted(victims, key=lambda x: 0 if x[0] == preferred_pool_id else 1)

    # Suspend a limited number; resources freed are not directly modeled here,
    # but simulator will reflect it on subsequent ticks.
    for pool_id, cid in victims[: s.max_preempt_per_tick]:
        suspensions.append(Suspend(container_id=cid, pool_id=pool_id))
    return suspensions


@register_scheduler(key="scheduler_none_034")
def scheduler_none_034(s, results, pipelines):
    """
    Deterministic scheduling step.

    Process:
      - Learn from results (OOM -> raise RAM floor; success -> remember RAM).
      - Enqueue new pipelines.
      - For each pool, pick the best available ready op among queued pipelines by:
          effective priority rank (with limited aging for batch)
      - Size resources conservatively:
          RAM = max(learned_floor, min_share*pool_max, <= pool_avail)
          CPU = clamp(pool_avail within [min_share, max_share] of pool_max)
      - If a high priority op cannot fit, preempt lower priority (best-effort).
    """
    s.tick += 1

    # Learn and update state from execution results
    if results:
        _learn_from_results(s, results)

    # Enqueue new pipelines
    for p in pipelines:
        _enqueue_pipeline(s, p)

    # Early exit if nothing could change our decision
    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    # We'll need pipeline objects by id (from new pipelines only we have objects; for queued ones we may not)
    # The simulator typically passes only new arrivals; thus, to keep things functional, we store
    # pipeline objects when we see them and reuse thereafter.
    if not hasattr(s, "pipelines_by_id"):
        s.pipelines_by_id = {}
    for p in pipelines:
        s.pipelines_by_id[p.pipeline_id] = p

    # Cleanup: remove completed pipelines from queues
    def prune_queue(q):
        kept = []
        for pid in q:
            p = s.pipelines_by_id.get(pid)
            if p is None:
                # If we don't have the object, keep it (can't inspect).
                kept.append(pid)
                continue
            st = p.runtime_status()
            if st.is_pipeline_successful():
                continue
            kept.append(pid)
        return kept

    s.q_query = prune_queue(s.q_query)
    s.q_interactive = prune_queue(s.q_interactive)
    s.q_batch = prune_queue(s.q_batch)

    # Helper to iterate candidate pipeline ids in desired order
    def candidate_pids_in_order():
        # Build list of (effective_rank, first_seen, pid) across all queues
        items = []
        for q in (s.q_query, s.q_interactive, s.q_batch):
            for pid in q:
                p = s.pipelines_by_id.get(pid)
                if p is None:
                    continue
                meta = s.pipeline_meta.get(pid, {})
                items.append((_effective_rank(s, p), meta.get("first_seen", 0), pid))
        items.sort(key=lambda t: (t[0], t[1], t[2]))
        for _, _, pid in items:
            yield pid

    # Track which pids we attempted but couldn't place, to avoid spinning in same tick
    attempted = set()

    for pool_id in range(s.executor.num_pools):
        if s.max_assignments_per_pool <= 0:
            break

        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool
        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        made = 0
        while made < s.max_assignments_per_pool:
            # Pick best next runnable op across all queues
            selected_pid = None
            selected_pipeline = None
            selected_ops = None

            for pid in candidate_pids_in_order():
                if pid in attempted:
                    continue
                p = s.pipelines_by_id.get(pid)
                if p is None:
                    attempted.add(pid)
                    continue

                st = p.runtime_status()
                # If pipeline has failures but can retry FAILED ops, allow it; otherwise, keep it queued.
                # We'll consider assignable ops regardless of past failures.
                if st.is_pipeline_successful():
                    attempted.add(pid)
                    continue

                ops = _select_ready_op(p)
                if not ops:
                    # No runnable op now
                    attempted.add(pid)
                    continue

                selected_pid = pid
                selected_pipeline = p
                selected_ops = ops
                break

            if selected_pipeline is None:
                break

            # Size resources
            op0 = selected_ops[0]
            k = _op_key(selected_pid, op0)
            learned_floor = float(s.op_ram_floor.get(k, 0.0))

            # Minimal share to avoid tiny allocations; but do not exceed available
            ram_min = float(pool.max_ram_pool) * float(s.ram_min_share)
            target_ram = max(learned_floor, ram_min)
            target_ram = min(float(avail_ram), float(pool.max_ram_pool), target_ram)

            cpu_min = float(pool.max_cpu_pool) * float(s.cpu_min_share)
            cpu_cap = float(pool.max_cpu_pool) * float(s.cpu_max_share)
            target_cpu = min(float(avail_cpu), cpu_cap)
            if target_cpu < cpu_min:
                target_cpu = min(float(avail_cpu), cpu_min)

            # If this is high priority and doesn't fit RAM, consider preemption
            pr = selected_pipeline.priority
            is_high = pr in (Priority.QUERY, Priority.INTERACTIVE)

            if target_ram <= 0 or target_cpu <= 0:
                attempted.add(selected_pid)
                break

            if target_ram > avail_ram or target_cpu > avail_cpu:
                if is_high:
                    # Ask for preemption; actual avail won't change until later ticks
                    suspensions.extend(_maybe_preempt_for_admission(
                        s, target_cpu, target_ram, preferred_pool_id=pool_id
                    ))
                attempted.add(selected_pid)
                break

            # Create assignment
            assignments.append(
                Assignment(
                    ops=selected_ops,
                    cpu=target_cpu,
                    ram=target_ram,
                    priority=pr,
                    pool_id=pool_id,
                    pipeline_id=selected_pid,
                )
            )

            # Optimistically reduce available resources for additional assignments in this tick
            avail_cpu -= target_cpu
            avail_ram -= target_ram
            made += 1

            # Rotate the selected pipeline within its queue (round-robin within priority)
            q = _get_queue_for_priority(s, pr)
            if selected_pid in q:
                q.remove(selected_pid)
                q.append(selected_pid)

    return suspensions, assignments
