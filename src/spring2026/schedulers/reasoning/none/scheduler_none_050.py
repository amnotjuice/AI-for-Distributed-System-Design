# policy_key: scheduler_none_050
# reasoning_effort: none
# model: gpt-5.2-2025-12-11
# llm_cost: 0.061681
# generation_seconds: 51.04
# generated_at: 2026-04-09T22:24:50.045012
@register_scheduler_init(key="scheduler_none_050")
def scheduler_none_050_init(s):
    """Priority-aware, OOM-adaptive, starvation-avoiding scheduler.

    Goals aligned to objective:
      - Strongly protect QUERY and INTERACTIVE latency via priority queues + light preemption.
      - Avoid failures (720s penalty) by learning per-operator RAM needs from OOMs and retrying.
      - Avoid starvation by aging (promote long-waiting pipelines).
      - Keep implementation simple: one-op-at-a-time scheduling, conservative sizing, minimal churn.
    """
    # Queues by priority (store pipeline objects)
    s.q_query = []
    s.q_interactive = []
    s.q_batch = []

    # Track arrivals and first-seen time to implement aging
    s.now = 0.0
    s.first_seen_time = {}  # pipeline_id -> time first observed

    # OOM learning:
    # Keyed by (pipeline_id, op_id) -> minimum RAM that should succeed next time.
    # We only increase on OOM; never decrease.
    s.op_ram_floor = {}

    # Backoff to avoid immediately retrying the same failing operator with insufficient resources
    # pipeline_id -> next_time_eligible
    s.retry_after = {}

    # Track last time we attempted to schedule a pipeline (to avoid thrashing)
    s.last_scheduled_time = {}  # pipeline_id -> time

    # Preemption knobs (conservative)
    s.enable_preemption = True
    s.preempt_cooldown_s = 5.0  # don't repeatedly preempt the same pool too frequently
    s.last_preempt_time_by_pool = {}  # pool_id -> time

    # Aging knobs: promote long-waiting pipelines upward
    s.age_to_interactive_s = 60.0
    s.age_to_query_s = 120.0

    # Sizing knobs
    s.min_cpu_per_op = 1.0
    # For high priority, avoid tiny CPU assignments that elongate tail latency
    s.query_cpu_target_frac = 0.75
    s.interactive_cpu_target_frac = 0.60
    s.batch_cpu_target_frac = 0.40

    # RAM safety margins for retries after OOM
    s.oom_ram_multiplier = 1.5
    s.oom_ram_additive = 0.25  # add 25% of pool max RAM as extra cushion in addition to multiplier
    s.max_retry_multiplier_cap = 0.95  # never request > 95% of pool RAM for an op

    # Attempt to keep high priority off congested pools by biasing placement
    s.interactive_pool_preference = 0  # prefer pool 0 if exists; fallback to best-headroom


@register_scheduler(key="scheduler_none_050")
def scheduler_none_050(s, results, pipelines):
    """Main scheduling loop: priority queues + OOM-adaptive retries + conservative preemption.

    - Enqueue new pipelines by priority.
    - Process results: on OOM failure, bump learned RAM floor and requeue for retry.
    - Apply aging promotions to reduce starvation risk.
    - For each pool, pick best candidate (QUERY > INTERACTIVE > BATCH), schedule 1 ready op.
    - If a QUERY arrives and pools are full, preempt a low-priority running container to make room.
    """
    # Helper functions kept inside to avoid imports and keep policy self-contained
    def _get_time_incremented():
        # Eudoxia "tick" timing isn't explicitly provided; we maintain a logical clock that
        # increases by 1 per scheduler invocation with any event or arrivals.
        # This is sufficient for relative aging/backoff decisions.
        if pipelines or results:
            s.now = float(s.now) + 1.0

    def _enqueue_pipeline(p):
        pid = p.pipeline_id
        if pid not in s.first_seen_time:
            s.first_seen_time[pid] = float(s.now)
        if p.priority == Priority.QUERY:
            s.q_query.append(p)
        elif p.priority == Priority.INTERACTIVE:
            s.q_interactive.append(p)
        else:
            s.q_batch.append(p)

    def _pipeline_done_or_dead(p):
        status = p.runtime_status()
        if status.is_pipeline_successful():
            return True
        # If any operator FAILED, we still want to retry because failures are heavily penalized.
        # However, if the simulator marks irrecoverable errors, we can detect via results.error
        # when available; otherwise, we keep retrying with increased RAM for OOM-like failures.
        return False

    def _eligible_now(p):
        pid = p.pipeline_id
        t = s.retry_after.get(pid, -1.0)
        return float(s.now) >= float(t)

    def _maybe_promote_aging():
        # Promote long-waiting batch -> interactive, interactive -> query.
        # This avoids starvation and reduces 720s penalties for unfinished pipelines.
        promoted_to_interactive = []
        kept_batch = []
        for p in s.q_batch:
            waited = float(s.now) - float(s.first_seen_time.get(p.pipeline_id, s.now))
            if waited >= s.age_to_interactive_s:
                promoted_to_interactive.append(p)
            else:
                kept_batch.append(p)
        s.q_batch = kept_batch
        s.q_interactive.extend(promoted_to_interactive)

        promoted_to_query = []
        kept_interactive = []
        for p in s.q_interactive:
            waited = float(s.now) - float(s.first_seen_time.get(p.pipeline_id, s.now))
            if waited >= s.age_to_query_s:
                promoted_to_query.append(p)
            else:
                kept_interactive.append(p)
        s.q_interactive = kept_interactive
        s.q_query.extend(promoted_to_query)

    def _get_ready_ops(p, limit=1):
        status = p.runtime_status()
        # Only schedule ops whose parents are complete
        op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        if not op_list:
            return []
        return op_list[:limit]

    def _op_key(op):
        # Attempt to derive a stable identifier; fall back to repr for dict keys.
        # Works across many simple operator representations.
        if hasattr(op, "op_id"):
            return str(op.op_id)
        if hasattr(op, "operator_id"):
            return str(op.operator_id)
        if hasattr(op, "id"):
            return str(op.id)
        return repr(op)

    def _learn_from_result(r):
        # Update RAM floor on failures that look like OOM.
        # We don't have structured error codes; check string content when present.
        if not r.failed():
            return
        err = ""
        try:
            err = (r.error or "")
        except Exception:
            err = ""
        err_l = str(err).lower()
        is_oom = ("oom" in err_l) or ("out of memory" in err_l) or ("memory" in err_l and "kill" in err_l)

        # If we can't tell, still backoff slightly to avoid hot-loop retry.
        pid = None
        try:
            pid = r.ops[0].pipeline_id  # may not exist
        except Exception:
            pid = None

        # We cannot reliably get pipeline_id from result in this template, so we infer from ops list
        # only when ops carry it; otherwise, we skip per-pipeline backoff updates.
        # However, we *can* learn RAM floor for the operator(s) we see, if we can key them.
        if is_oom:
            for op in (r.ops or []):
                # Best-effort pipeline id from op
                op_pid = getattr(op, "pipeline_id", None)
                if op_pid is None:
                    continue
                key = (str(op_pid), _op_key(op))
                prev = float(s.op_ram_floor.get(key, 0.0))
                # Next RAM floor: at least what we tried, multiplied + cushion (bounded)
                tried_ram = float(getattr(r, "ram", 0.0) or 0.0)
                # If tried_ram missing, just nudge upward from prev
                base = max(prev, tried_ram)
                # Add cushion based on pool max RAM when we can infer it
                pool_id = getattr(r, "pool_id", None)
                pool_max = None
                if pool_id is not None and 0 <= int(pool_id) < s.executor.num_pools:
                    pool_max = float(s.executor.pools[int(pool_id)].max_ram_pool)
                add = float(s.oom_ram_additive) * (pool_max if pool_max is not None else base)
                new_floor = max(prev, base * float(s.oom_ram_multiplier) + add)
                if pool_max is not None:
                    new_floor = min(new_floor, float(s.max_retry_multiplier_cap) * pool_max)
                s.op_ram_floor[key] = new_floor

                # Backoff retry for that pipeline a bit
                s.retry_after[str(op_pid)] = float(s.now) + 2.0

        else:
            # Non-OOM failure: backoff a bit to reduce churn; still retry to avoid 720s penalties.
            for op in (r.ops or []):
                op_pid = getattr(op, "pipeline_id", None)
                if op_pid is None:
                    continue
                s.retry_after[str(op_pid)] = float(s.now) + 3.0

    def _pool_headroom_score(pool):
        # Prefer pools with more balanced headroom
        cpu = float(pool.avail_cpu_pool)
        ram = float(pool.avail_ram_pool)
        # Slightly emphasize RAM to reduce OOM risk
        return (cpu + 1e-6) * ((ram + 1e-6) ** 1.2)

    def _pick_pool_for_priority(priority):
        # For INTERACTIVE, prefer a designated pool if it has some headroom.
        if priority == Priority.INTERACTIVE and s.executor.num_pools > 0:
            pref = int(s.interactive_pool_preference)
            if 0 <= pref < s.executor.num_pools:
                p = s.executor.pools[pref]
                if float(p.avail_cpu_pool) >= s.min_cpu_per_op and float(p.avail_ram_pool) > 0:
                    return pref
        # Otherwise choose best headroom pool
        best = None
        best_score = -1.0
        for i in range(s.executor.num_pools):
            pool = s.executor.pools[i]
            if float(pool.avail_cpu_pool) < s.min_cpu_per_op or float(pool.avail_ram_pool) <= 0:
                continue
            sc = _pool_headroom_score(pool)
            if sc > best_score:
                best_score = sc
                best = i
        return best

    def _compute_request(pool, pipeline, op):
        # CPU sizing based on priority with pool fraction targets, bounded by available.
        max_cpu = float(pool.max_cpu_pool)
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)
        if pipeline.priority == Priority.QUERY:
            cpu_target = max(s.min_cpu_per_op, max_cpu * float(s.query_cpu_target_frac))
        elif pipeline.priority == Priority.INTERACTIVE:
            cpu_target = max(s.min_cpu_per_op, max_cpu * float(s.interactive_cpu_target_frac))
        else:
            cpu_target = max(s.min_cpu_per_op, max_cpu * float(s.batch_cpu_target_frac))
        cpu_req = min(avail_cpu, cpu_target)

        # RAM sizing:
        # - Use learned floor if we have one from OOM
        # - Otherwise, be conservative: give a reasonable fraction of available RAM
        key = (str(pipeline.pipeline_id), _op_key(op))
        learned = float(s.op_ram_floor.get(key, 0.0))
        if learned > 0.0:
            ram_req = min(avail_ram, learned)
        else:
            # Initial attempt: avoid tiny RAM to reduce chance of OOM and retries.
            # But do not monopolize: use 50% of available for query/interactive, 35% for batch.
            frac = 0.50 if pipeline.priority in (Priority.QUERY, Priority.INTERACTIVE) else 0.35
            ram_req = max(0.0, min(avail_ram, avail_ram * frac))
            # If pool has very little available RAM, just take what's available (may OOM; we'll learn).
            if ram_req <= 0.0:
                ram_req = min(avail_ram, float(pool.max_ram_pool) * 0.10)

        # Ensure RAM request does not exceed pool max cap for retried ops
        ram_req = min(ram_req, float(pool.max_ram_pool) * float(s.max_retry_multiplier_cap))
        return cpu_req, ram_req

    def _requeue(p):
        # Re-enqueue based on current (possibly promoted) priority.
        _enqueue_pipeline(p)

    def _pop_next_pipeline_from_queue(q):
        # Pop first eligible pipeline that still has assignable ready ops.
        kept = []
        chosen = None
        for p in q:
            if chosen is None and _eligible_now(p) and not _pipeline_done_or_dead(p):
                ops = _get_ready_ops(p, limit=1)
                if ops:
                    chosen = p
                    continue
            kept.append(p)
        # mutate queue in place to kept + (others excluding chosen)
        q[:] = kept
        return chosen

    def _choose_next_pipeline():
        # Strict priority ordering with aging already applied.
        p = _pop_next_pipeline_from_queue(s.q_query)
        if p is not None:
            return p
        p = _pop_next_pipeline_from_queue(s.q_interactive)
        if p is not None:
            return p
        p = _pop_next_pipeline_from_queue(s.q_batch)
        return p

    def _find_preempt_candidate():
        # Best-effort preemption: suspend one low-priority container if we need to schedule a QUERY.
        # We rely on executor exposing "containers" per pool; if absent, we skip preemption.
        candidates = []
        for pool_id in range(s.executor.num_pools):
            pool = s.executor.pools[pool_id]
            containers = getattr(pool, "containers", None)
            if not containers:
                continue
            for c in containers:
                # c should have priority and container_id
                cprio = getattr(c, "priority", None)
                if cprio in (Priority.BATCH_PIPELINE, Priority.INTERACTIVE):
                    candidates.append((pool_id, c))
        # Prefer preempting BATCH first, then INTERACTIVE; and from pools with low headroom.
        def _cand_key(item):
            pool_id, c = item
            pool = s.executor.pools[pool_id]
            head = _pool_headroom_score(pool)
            cprio = getattr(c, "priority", Priority.BATCH_PIPELINE)
            pr = 0 if cprio == Priority.BATCH_PIPELINE else 1
            return (pr, head)

        if not candidates:
            return None
        candidates.sort(key=_cand_key)
        return candidates[0]

    _get_time_incremented()

    # Enqueue new pipelines
    for p in pipelines:
        _enqueue_pipeline(p)

    # Learn from execution results (OOM adaptive) and requeue affected pipelines if needed
    for r in results:
        _learn_from_result(r)

    # Promote aging to avoid starvation
    _maybe_promote_aging()

    # Early exit if nothing changed
    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    # Attempt to schedule at most one op per pool per tick (keeps behavior stable)
    # Use placement bias: choose pool per picked pipeline priority, not just linear scan.
    scheduled_pools = set()
    attempts = 0
    max_attempts = max(1, s.executor.num_pools) * 3  # limit loop in case of non-progress

    while attempts < max_attempts and len(scheduled_pools) < s.executor.num_pools:
        attempts += 1
        pipeline = _choose_next_pipeline()
        if pipeline is None:
            break

        # Pick a pool for this pipeline
        pool_id = _pick_pool_for_priority(pipeline.priority)
        if pool_id is None or pool_id in scheduled_pools:
            # Can't place now; requeue and stop trying to avoid spin
            _requeue(pipeline)
            break

        pool = s.executor.pools[pool_id]
        if float(pool.avail_cpu_pool) < s.min_cpu_per_op or float(pool.avail_ram_pool) <= 0:
            _requeue(pipeline)
            scheduled_pools.add(pool_id)
            continue

        op_list = _get_ready_ops(pipeline, limit=1)
        if not op_list:
            # Nothing ready; keep it around
            _requeue(pipeline)
            continue

        op = op_list[0]
        cpu_req, ram_req = _compute_request(pool, pipeline, op)

        # If we can't allocate anything meaningful, requeue
        if cpu_req < s.min_cpu_per_op or ram_req <= 0:
            _requeue(pipeline)
            scheduled_pools.add(pool_id)
            continue

        assignment = Assignment(
            ops=op_list,
            cpu=cpu_req,
            ram=ram_req,
            priority=pipeline.priority,
            pool_id=pool_id,
            pipeline_id=pipeline.pipeline_id
        )
        assignments.append(assignment)
        s.last_scheduled_time[pipeline.pipeline_id] = float(s.now)
        scheduled_pools.add(pool_id)

    # Conservative preemption: if we failed to schedule any QUERY and query queue has eligible work,
    # and pools appear saturated, try preempting one low priority container once per cooldown.
    if s.enable_preemption:
        # Check if there is eligible QUERY work waiting
        has_query_waiting = False
        for p in s.q_query:
            if _eligible_now(p) and _get_ready_ops(p, limit=1):
                has_query_waiting = True
                break

        if has_query_waiting and not any(a.priority == Priority.QUERY for a in assignments):
            # Check if all pools are effectively saturated
            saturated = True
            for i in range(s.executor.num_pools):
                pool = s.executor.pools[i]
                if float(pool.avail_cpu_pool) >= s.min_cpu_per_op and float(pool.avail_ram_pool) > 0:
                    saturated = False
                    break

            if saturated:
                cand = _find_preempt_candidate()
                if cand is not None:
                    pool_id, c = cand
                    lastp = float(s.last_preempt_time_by_pool.get(pool_id, -1e9))
                    if float(s.now) - lastp >= float(s.preempt_cooldown_s):
                        container_id = getattr(c, "container_id", None)
                        if container_id is not None:
                            suspensions.append(Suspend(container_id=container_id, pool_id=pool_id))
                            s.last_preempt_time_by_pool[pool_id] = float(s.now)

    return suspensions, assignments
