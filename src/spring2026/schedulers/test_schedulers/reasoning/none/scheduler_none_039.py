# policy_key: scheduler_none_039
# reasoning_effort: none
# model: gpt-5.2-2025-12-11
# llm_cost: 0.061611
# generation_seconds: 57.20
# generated_at: 2026-04-09T22:15:28.229297
@register_scheduler_init(key="scheduler_none_039")
def scheduler_none_039_init(s):
    """Priority-aware, OOM-adaptive, low-churn scheduler.

    Goals aligned to weighted latency score with heavy failure penalty:
      - Strong preference for QUERY and INTERACTIVE latency (and completion).
      - Reduce OOM failures via per-op RAM floor learning and cautious RAM sizing.
      - Avoid starvation via simple aging credits for lower priorities.
      - Limit preemption churn: only preempt when high-priority work cannot be admitted.
    """
    # Queues per priority (store pipeline_ids; pipelines stored in dict for freshness)
    s._pipelines_by_id = {}
    s._q_query = []
    s._q_interactive = []
    s._q_batch = []

    # Per-(pipeline, op) learned RAM floor (in absolute units matching pool.ram)
    # Updated on OOM failures; used to reduce repeat failures (720s penalty).
    s._op_ram_floor = {}  # (pipeline_id, op_id) -> ram_floor

    # Per-pipeline starvation/aging credits (increase when waiting, decrease when scheduled)
    s._credits = {}  # pipeline_id -> float

    # Track currently running containers by pool to optionally preempt
    s._running_by_pool = {}  # pool_id -> list of dicts {container_id, priority}

    # Config knobs (kept conservative to avoid overfitting)
    s._max_assignments_per_tick_per_pool = 2
    s._max_ops_per_assignment = 1  # keep atomic to reduce head-of-line blocking & risk
    s._credit_inc = {
        Priority.QUERY: 0.2,
        Priority.INTERACTIVE: 0.4,
        Priority.BATCH_PIPELINE: 0.8,
    }
    s._credit_cost_on_schedule = 1.0

    # RAM growth factor after OOM (multiplicative) and additive safety margin.
    s._oom_ram_mult = 1.5
    s._oom_ram_add = 0.05  # as fraction of pool max RAM; applied as absolute later

    # Minimum share of pool resources reserved for high-priority work (soft, via admission).
    s._reserve_frac = {
        Priority.QUERY: 0.25,
        Priority.INTERACTIVE: 0.15,
        Priority.BATCH_PIPELINE: 0.0,
    }

    # CPU sizing: avoid grabbing whole pool unless needed; keep concurrency up.
    # Use fractions of available CPU with caps.
    s._cpu_target_frac = {
        Priority.QUERY: 0.75,
        Priority.INTERACTIVE: 0.6,
        Priority.BATCH_PIPELINE: 0.5,
    }
    s._cpu_min = 1.0  # assume CPU units allow fractional/float; if not, simulation will coerce
    s._cpu_max_frac_of_pool = 0.85

    # RAM sizing: default to a conservative fraction of pool to reduce OOM,
    # but avoid consuming entire pool to keep some concurrency.
    s._ram_target_frac = {
        Priority.QUERY: 0.60,
        Priority.INTERACTIVE: 0.55,
        Priority.BATCH_PIPELINE: 0.50,
    }
    s._ram_max_frac_of_pool = 0.85


def _priority_rank(priority):
    # Higher rank means more important
    if priority == Priority.QUERY:
        return 3
    if priority == Priority.INTERACTIVE:
        return 2
    return 1


def _queue_for_priority(s, priority):
    if priority == Priority.QUERY:
        return s._q_query
    if priority == Priority.INTERACTIVE:
        return s._q_interactive
    return s._q_batch


def _enqueue_pipeline_if_new(s, p):
    pid = p.pipeline_id
    if pid not in s._pipelines_by_id:
        s._pipelines_by_id[pid] = p
        s._credits.setdefault(pid, 0.0)
        q = _queue_for_priority(s, p.priority)
        q.append(pid)
    else:
        # Refresh pipeline object reference (runtime status is dynamic)
        s._pipelines_by_id[pid] = p


def _is_pipeline_done_or_failed(p):
    st = p.runtime_status()
    if st.is_pipeline_successful():
        return True
    # If any failed op exists, we still might retry (FAILED is assignable), so don't drop here.
    # We only drop pipelines that are fully successful; otherwise keep them.
    return False


def _get_next_assignable_op(p):
    st = p.runtime_status()
    # Prioritize ops whose parents are complete; take one op to reduce risk/churn.
    ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
    if ops:
        return ops[0]
    return None


def _op_identifier(op):
    # Best-effort stable id across simulation objects.
    # Try common attributes; fall back to object id.
    for attr in ("op_id", "operator_id", "id", "name"):
        if hasattr(op, attr):
            return getattr(op, attr)
    return str(id(op))


def _update_running_index(s, results):
    # Maintain a lightweight view of currently running containers per pool.
    # We only reliably learn about containers when results arrive. We'll update based on
    # completion/failure events and rely on executor state minimally.
    if not hasattr(s, "_running_by_pool") or s._running_by_pool is None:
        s._running_by_pool = {}

    for r in results:
        # When a result arrives, that container finished (success or failure).
        pool_list = s._running_by_pool.get(r.pool_id, [])
        if pool_list:
            s._running_by_pool[r.pool_id] = [x for x in pool_list if x.get("container_id") != r.container_id]


def _record_oom_and_learn_ram(s, r):
    # Learn from OOM-like failures: raise per-op RAM floor to avoid repeated failures.
    if not r.failed():
        return
    if not getattr(r, "error", None):
        return

    err = str(r.error).lower()
    oomish = ("oom" in err) or ("out of memory" in err) or ("memory" in err and "killed" in err)
    if not oomish:
        return

    # For each op in this container, raise floor.
    for op in getattr(r, "ops", []) or []:
        op_id = _op_identifier(op)
        key = (getattr(r, "pipeline_id", None), op_id)
        # Some traces might not include pipeline_id in result; attempt to infer via op if possible.
        if key[0] is None:
            # Try to find pipeline id by searching s._pipelines_by_id for this op object
            # (best-effort; if not found, skip learning).
            inferred_pid = None
            for pid, p in s._pipelines_by_id.items():
                # Avoid deep search; accept that inference may fail.
                if hasattr(p, "values") and op in getattr(p, "values", []):
                    inferred_pid = pid
                    break
            if inferred_pid is None:
                continue
            key = (inferred_pid, op_id)

        prev = s._op_ram_floor.get(key, 0.0)
        new_floor = max(prev, float(getattr(r, "ram", 0.0)) * s._oom_ram_mult)
        s._op_ram_floor[key] = new_floor


def _age_credits(s):
    # Increase credits for all queued pipelines to avoid starvation;
    # batch ages faster to guarantee eventual progress without harming query latency too much.
    for pid, p in list(s._pipelines_by_id.items()):
        if pid not in s._credits:
            s._credits[pid] = 0.0
        # Only age if still not completed
        if _is_pipeline_done_or_failed(p):
            continue
        s._credits[pid] += s._credit_inc.get(p.priority, 0.4)


def _pop_next_candidate(s):
    # Choose next pipeline to schedule using strict priority with aging boost.
    # We keep queues as FIFO but may rotate if head isn't schedulable.
    # Scoring: base by priority rank, plus credits scaled so long-waiting batch can run.
    best_pid = None
    best_score = None

    # Consider a bounded window from each queue to keep runtime low and preserve FIFO-ish behavior.
    def consider_queue(q, window=8):
        nonlocal best_pid, best_score
        for i in range(min(window, len(q))):
            pid = q[i]
            p = s._pipelines_by_id.get(pid)
            if p is None:
                continue
            if _is_pipeline_done_or_failed(p):
                continue
            op = _get_next_assignable_op(p)
            if op is None:
                continue
            score = _priority_rank(p.priority) * 1000.0 + float(s._credits.get(pid, 0.0))
            if best_score is None or score > best_score:
                best_score = score
                best_pid = pid

    consider_queue(s._q_query)
    consider_queue(s._q_interactive)
    consider_queue(s._q_batch)

    return best_pid


def _remove_pid_from_queues(s, pid):
    # Remove one occurrence from any queue (should be unique).
    for q in (s._q_query, s._q_interactive, s._q_batch):
        try:
            q.remove(pid)
        except ValueError:
            pass


def _reappend_pid(s, pid):
    p = s._pipelines_by_id.get(pid)
    if p is None:
        return
    q = _queue_for_priority(s, p.priority)
    q.append(pid)


def _compute_request_sizes(s, pool, p, op, avail_cpu, avail_ram):
    # Determine CPU/RAM request for assignment, balancing:
    #  - avoid OOM via learned RAM floor,
    #  - avoid grabbing entire pool (improve concurrency),
    #  - keep query fast via higher CPU fraction.
    pool_max_cpu = float(pool.max_cpu_pool)
    pool_max_ram = float(pool.max_ram_pool)

    # CPU
    cpu = max(s._cpu_min, avail_cpu * s._cpu_target_frac.get(p.priority, 0.6))
    cpu = min(cpu, pool_max_cpu * s._cpu_max_frac_of_pool)
    cpu = min(cpu, avail_cpu)

    # RAM
    op_id = _op_identifier(op)
    learned_floor = s._op_ram_floor.get((p.pipeline_id, op_id), 0.0)
    # Safety: add a small absolute margin based on pool size.
    safety_add = pool_max_ram * s._oom_ram_add
    target = max(learned_floor + safety_add, avail_ram * s._ram_target_frac.get(p.priority, 0.55))
    ram = min(target, pool_max_ram * s._ram_max_frac_of_pool)
    ram = min(ram, avail_ram)

    # If the learned floor itself exceeds available RAM, request all available RAM
    # to maximize chance of success; if still insufficient, caller may decide to preempt.
    if learned_floor > 0 and ram < min(learned_floor, avail_ram):
        ram = avail_ram

    # Avoid zero/negative sizing
    if cpu <= 0 or ram <= 0:
        return None, None

    return cpu, ram


def _need_reserve(s, pool, priority):
    # Soft reserve: ensure some headroom remains for higher priority classes.
    # For batch, we require leaving reserve for query+interactive.
    if priority == Priority.QUERY:
        return 0.0, 0.0  # no reserve needed above query
    pool_max_cpu = float(pool.max_cpu_pool)
    pool_max_ram = float(pool.max_ram_pool)
    # Reserve for QUERY always; and for INTERACTIVE if scheduling BATCH.
    reserve_cpu = pool_max_cpu * s._reserve_frac.get(Priority.QUERY, 0.0)
    reserve_ram = pool_max_ram * s._reserve_frac.get(Priority.QUERY, 0.0)
    if priority == Priority.BATCH_PIPELINE:
        reserve_cpu += pool_max_cpu * s._reserve_frac.get(Priority.INTERACTIVE, 0.0)
        reserve_ram += pool_max_ram * s._reserve_frac.get(Priority.INTERACTIVE, 0.0)
    return reserve_cpu, reserve_ram


def _select_pool_order(s, priority):
    # Prefer pool with most headroom for high priority; for batch, prefer most free too.
    # Return pool_ids sorted by available CPU+RAM (normalized).
    scores = []
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        # Normalize by max to compare across pool sizes
        cpu_score = float(pool.avail_cpu_pool) / max(1.0, float(pool.max_cpu_pool))
        ram_score = float(pool.avail_ram_pool) / max(1.0, float(pool.max_ram_pool))
        # Weight RAM slightly higher because OOM failures are very costly
        score = cpu_score + 1.3 * ram_score
        # Small bias: try to place high priority in the "best" pool
        scores.append((score, pool_id))
    scores.sort(reverse=True)
    return [pid for _, pid in scores]


def _maybe_preempt_for_high_priority(s, pool_id, need_cpu, need_ram):
    # Preempt lowest priority running containers in this pool until enough headroom.
    # Conservative: only preempt BATCH for QUERY/INTERACTIVE, and only if necessary.
    suspensions = []
    pool = s.executor.pools[pool_id]

    deficit_cpu = max(0.0, need_cpu - float(pool.avail_cpu_pool))
    deficit_ram = max(0.0, need_ram - float(pool.avail_ram_pool))
    if deficit_cpu <= 0 and deficit_ram <= 0:
        return suspensions

    running = list(s._running_by_pool.get(pool_id, []))
    if not running:
        return suspensions

    # Sort by ascending priority (preempt batch first), then FIFO-ish
    running.sort(key=lambda x: _priority_rank(x.get("priority")))

    freed_cpu = 0.0
    freed_ram = 0.0
    for rc in running:
        pr = rc.get("priority")
        # Only preempt BATCH work to protect higher priorities.
        if pr != Priority.BATCH_PIPELINE:
            continue
        cid = rc.get("container_id")
        if cid is None:
            continue
        suspensions.append(Suspend(container_id=cid, pool_id=pool_id))
        # We don't know exact freed resources here; assume it will help and stop early
        # once we issued enough preemptions (churn control).
        # Preempt at most 2 containers per tick per pool.
        if len(suspensions) >= 2:
            break

    return suspensions


@register_scheduler(key="scheduler_none_039")
def scheduler_none_039_scheduler(s, results, pipelines):
    """
    Each tick:
      1) Ingest new pipelines into per-priority queues.
      2) Learn RAM floors from OOM failures.
      3) Age credits to prevent starvation.
      4) For each pool, try a small number of assignments:
         - choose best candidate pipeline (priority + aging),
         - size CPU/RAM conservatively with learned RAM,
         - enforce soft reserves (don't let batch consume all headroom),
         - optionally preempt batch if needed for query/interactive admission.
    """
    # Ingest new pipelines
    for p in pipelines:
        _enqueue_pipeline_if_new(s, p)

    # Update running index and learn from failures
    _update_running_index(s, results)
    for r in results:
        _record_oom_and_learn_ram(s, r)

    # Early exit if nothing changed materially
    if not pipelines and not results:
        return [], []

    # Age credits for queued pipelines
    _age_credits(s)

    suspensions = []
    assignments = []

    # Attempt scheduling by iterating pools in a headroom-based order per priority.
    # We'll schedule high priority first globally by repeatedly selecting candidates.
    # Limit total work to avoid overly aggressive behavior.
    global_budget = max(1, s.executor.num_pools * s._max_assignments_per_tick_per_pool)

    while global_budget > 0:
        pid = _pop_next_candidate(s)
        if pid is None:
            break

        p = s._pipelines_by_id.get(pid)
        if p is None:
            _remove_pid_from_queues(s, pid)
            continue

        if _is_pipeline_done_or_failed(p):
            _remove_pid_from_queues(s, pid)
            continue

        op = _get_next_assignable_op(p)
        if op is None:
            # Not ready; rotate to back to avoid blocking.
            _remove_pid_from_queues(s, pid)
            _reappend_pid(s, pid)
            # Reduce credit slightly to avoid tight loops
            s._credits[pid] = max(0.0, float(s._credits.get(pid, 0.0)) - 0.25)
            global_budget -= 1
            continue

        placed = False
        pool_order = _select_pool_order(s, p.priority)

        for pool_id in pool_order:
            pool = s.executor.pools[pool_id]
            avail_cpu = float(pool.avail_cpu_pool)
            avail_ram = float(pool.avail_ram_pool)
            if avail_cpu <= 0 or avail_ram <= 0:
                continue

            # Soft reserves to protect query/interactive latency (dominant score terms)
            reserve_cpu, reserve_ram = _need_reserve(s, pool, p.priority)
            effective_avail_cpu = max(0.0, avail_cpu - reserve_cpu)
            effective_avail_ram = max(0.0, avail_ram - reserve_ram)
            if p.priority == Priority.BATCH_PIPELINE:
                # For batch, respect reserves strongly.
                if effective_avail_cpu <= 0 or effective_avail_ram <= 0:
                    continue
            else:
                # For high priority, we can use full availability.
                effective_avail_cpu = avail_cpu
                effective_avail_ram = avail_ram

            cpu_req, ram_req = _compute_request_sizes(s, pool, p, op, effective_avail_cpu, effective_avail_ram)
            if cpu_req is None or ram_req is None:
                continue

            # If not enough effective availability for batch, skip
            if cpu_req > effective_avail_cpu + 1e-9 or ram_req > effective_avail_ram + 1e-9:
                continue

            # For high priority, if effective_avail wasn't enough due to reserves, try with full
            if p.priority != Priority.BATCH_PIPELINE:
                cpu_req, ram_req = _compute_request_sizes(s, pool, p, op, avail_cpu, avail_ram)
                if cpu_req is None or ram_req is None:
                    continue

                if cpu_req > avail_cpu + 1e-9 or ram_req > avail_ram + 1e-9:
                    # Consider preempting batch to make room
                    need_cpu = min(float(pool.max_cpu_pool), cpu_req)
                    need_ram = min(float(pool.max_ram_pool), ram_req)
                    susp = _maybe_preempt_for_high_priority(s, pool_id, need_cpu, need_ram)
                    if susp:
                        suspensions.extend(susp)
                        # After preemption, try placing next tick to reduce churn and
                        # avoid assuming immediate resource reclamation.
                        placed = True  # treat as "made progress" to avoid re-looping
                        break
                    continue

            # Place exactly one op per assignment for better latency predictability and fewer OOM cascades
            assignment = Assignment(
                ops=[op][: s._max_ops_per_assignment],
                cpu=cpu_req,
                ram=ram_req,
                priority=p.priority,
                pool_id=pool_id,
                pipeline_id=p.pipeline_id,
            )
            assignments.append(assignment)

            # Bookkeeping: mark pipeline as recently scheduled, reduce credits
            s._credits[pid] = max(0.0, float(s._credits.get(pid, 0.0)) - s._credit_cost_on_schedule)

            # Rotate pipeline to back for fairness among same priority
            _remove_pid_from_queues(s, pid)
            _reappend_pid(s, pid)

            # Track container as running once executor assigns it (best-effort; container_id not yet known)
            # We'll rely on results to clear; preemption uses running_by_pool best-effort.
            s._running_by_pool.setdefault(pool_id, [])

            placed = True
            global_budget -= 1
            break

        if not placed:
            # Couldn't place now; rotate and keep credits so it will be retried soon.
            _remove_pid_from_queues(s, pid)
            _reappend_pid(s, pid)
            # Avoid infinite loop within same tick
            global_budget -= 1

        # Per-pool assignment cap: enforce by counting current tick assignments per pool
        if assignments:
            per_pool_counts = {}
            for a in assignments:
                per_pool_counts[a.pool_id] = per_pool_counts.get(a.pool_id, 0) + 1
            # If any pool exceeded cap, stop scheduling further to reduce bursts
            if any(c >= s._max_assignments_per_tick_per_pool for c in per_pool_counts.values()):
                break

    return suspensions, assignments
