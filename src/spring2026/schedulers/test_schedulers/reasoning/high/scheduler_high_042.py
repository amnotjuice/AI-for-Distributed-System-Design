# policy_key: scheduler_high_042
# reasoning_effort: high
# exp: reasoning
# model: gpt-5.2-2025-12-11
# llm_cost: 0.167297
# generation_seconds: 211.49
# generated_at: 2026-04-10T03:13:18.420371
@register_scheduler_init(key="scheduler_high_042")
def scheduler_high_042_init(s):
    """
    Priority-aware, OOM-avoiding, headroom-preserving scheduler.

    Main ideas (small, safe improvements over FIFO):
      1) Strict priority order: QUERY > INTERACTIVE > BATCH, but with anti-starvation aging for BATCH.
      2) Right-size CPU (sublinear scaling) to avoid one op monopolizing a pool.
      3) Allocate RAM conservatively and learn from OOMs: if an op OOMs at ram=X, next time give it more.
      4) Preserve headroom for future high-priority arrivals by preventing batch from fully packing the pool.
      5) Avoid duplicate scheduling: at most one op per pipeline per scheduler tick.
    """
    s.tick = 0

    # Per-priority round-robin queues of pipeline_ids
    s.waiting = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }

    # Active pipelines by id (object references kept so runtime_status() evolves)
    s.pipeline_by_id = {}
    s.enqueue_tick = {}

    # Global per-op RAM hints (learned primarily from OOM failures)
    # Keyed by a "best-effort stable" operator key derived from the op object.
    s.op_ram_hint = {}

    # Track per-(pipeline_id, op_key) failure counts to cap thrash.
    s.fail_counts = {}

    # If we decide a pipeline is not worth retrying (too many failures), stop scheduling it.
    s.pipeline_blacklist = set()

    # Remember whether we've ever seen high-priority arrivals; used for preserving headroom.
    s.seen_high_priority_arrival = False

    # Tuning knobs
    s.max_assignments_per_pool = 8  # avoid excessive fragmentation

    # CPU sizing: cap fraction of pool CPU per operator by priority (sublinear scaling).
    s.cpu_frac_cap = {
        Priority.QUERY: 0.75,
        Priority.INTERACTIVE: 0.60,
        Priority.BATCH_PIPELINE: 0.50,
    }
    s.min_cpu = 1.0

    # RAM sizing: base fraction of pool RAM per operator by priority (reduce OOM risk).
    s.base_ram_frac = {
        Priority.QUERY: 0.60,
        Priority.INTERACTIVE: 0.50,
        Priority.BATCH_PIPELINE: 0.40,
    }

    # Preserve headroom so a new query/interactive can start even if batch is present.
    s.reserve_high_frac = 0.15  # keep at least this fraction of pool resources free from batch packing

    # Batch anti-starvation aging: after enough waiting, allow batch to ignore headroom restrictions.
    s.batch_aging_ticks = 80
    s.batch_escape_ticks = 220

    # Retry caps (OOMs typically need a retry with more RAM; repeated non-OOM failures are likely futile).
    s.max_retries = {
        Priority.QUERY: 4,
        Priority.INTERACTIVE: 3,
        Priority.BATCH_PIPELINE: 2,
    }


def _sched_high_042_is_oom(err) -> bool:
    if not err:
        return False
    e = str(err).lower()
    return ("oom" in e) or ("out of memory" in e) or ("cuda out of memory" in e)


def _sched_high_042_op_key(op):
    """
    Best-effort stable key. Prefer explicit ids/names if present; fall back to Python object id.
    """
    k = getattr(op, "op_id", None)
    if k is None:
        k = getattr(op, "operator_id", None)
    if k is None:
        k = getattr(op, "name", None)
    if k is None:
        # Object identity is stable within a simulation run; good enough for retrying the same failed op.
        k = ("obj", id(op))
    return k


def _sched_high_042_assignable_states():
    # Be robust if ASSIGNABLE_STATES is not defined in the environment.
    try:
        return ASSIGNABLE_STATES
    except NameError:
        return [OperatorState.PENDING, OperatorState.FAILED]


def _sched_high_042_has_ready_ops(s, prio, limit=20) -> bool:
    """
    Check whether there exists at least one pipeline in the given priority queue with an assignable op.
    Limit scan to avoid O(n) on huge queues each tick.
    """
    q = s.waiting.get(prio, [])
    n = len(q)
    if n == 0:
        return False
    states = _sched_high_042_assignable_states()
    scans = min(limit, n)
    for i in range(scans):
        pid = q[i]
        if pid in s.pipeline_blacklist:
            continue
        p = s.pipeline_by_id.get(pid)
        if p is None:
            continue
        st = p.runtime_status()
        if st.is_pipeline_successful():
            continue
        ops = st.get_ops(states, require_parents_complete=True)
        if ops:
            return True
    return False


def _sched_high_042_cpu_request(s, prio, pool, avail_cpu):
    cap = s.cpu_frac_cap.get(prio, 0.50) * pool.max_cpu_pool
    cpu = min(avail_cpu, cap)
    if cpu < s.min_cpu:
        return 0.0
    return cpu


def _sched_high_042_ram_request(s, prio, pool, op_key, avail_ram):
    # Base allocation: fraction of total pool RAM, but never below a small minimum.
    base = s.base_ram_frac.get(prio, 0.40) * pool.max_ram_pool
    base = max(base, 0.05 * pool.max_ram_pool)

    hint = s.op_ram_hint.get(op_key, 0.0)
    ram = max(base, hint)
    ram = min(ram, pool.max_ram_pool, avail_ram)
    if ram <= 0:
        return 0.0
    return ram


def _sched_high_042_batch_is_aged(s, pipeline_id) -> bool:
    t0 = s.enqueue_tick.get(pipeline_id, s.tick)
    waited = s.tick - t0
    return waited >= s.batch_aging_ticks


def _sched_high_042_batch_can_escape(s, pipeline_id) -> bool:
    t0 = s.enqueue_tick.get(pipeline_id, s.tick)
    waited = s.tick - t0
    return waited >= s.batch_escape_ticks


@register_scheduler(key="scheduler_high_042")
def scheduler_high_042_scheduler(s, results, pipelines):
    """
    Scheduler step:
      - ingest arrivals
      - update RAM hints from OOM results
      - pack pools using strict priority + headroom reservation + batch aging escape hatch
    """
    s.tick += 1

    # 1) Add newly arrived pipelines
    for p in pipelines:
        pid = p.pipeline_id
        s.pipeline_by_id[pid] = p
        if pid not in s.enqueue_tick:
            s.enqueue_tick[pid] = s.tick
        s.waiting[p.priority].append(pid)
        if p.priority in (Priority.QUERY, Priority.INTERACTIVE):
            s.seen_high_priority_arrival = True

    # 2) Learn from execution results (mostly to mitigate OOMs)
    for r in results:
        # If pipeline_id exists on the result in the simulator implementation, use it; otherwise keep global hints.
        rid = getattr(r, "pipeline_id", None)

        # Update per-op RAM hints on OOM and cap retries on repeated failures.
        for op in getattr(r, "ops", []) or []:
            opk = _sched_high_042_op_key(op)

            if hasattr(r, "failed") and r.failed():
                # Track failures for thrash control if we can associate to a pipeline_id
                if rid is not None:
                    key = (rid, opk)
                    s.fail_counts[key] = s.fail_counts.get(key, 0) + 1

                # If OOM, increase RAM hint aggressively (this is the main completion-rate safeguard).
                if _sched_high_042_is_oom(getattr(r, "error", None)):
                    pool_id = getattr(r, "pool_id", None)
                    pool_max_ram = None
                    if pool_id is not None and 0 <= pool_id < s.executor.num_pools:
                        pool_max_ram = s.executor.pools[pool_id].max_ram_pool
                    # Next attempt: at least 2x the RAM that just OOM'd (bounded by pool max if known).
                    new_hint = float(getattr(r, "ram", 0.0)) * 2.0
                    if pool_max_ram is not None:
                        new_hint = min(new_hint, pool_max_ram)
                    s.op_ram_hint[opk] = max(s.op_ram_hint.get(opk, 0.0), new_hint)
            else:
                # Success doesn't reveal true RAM need (we only know what we allocated), so we keep hints as-is.
                pass

        # If we can map failures to a pipeline, consider blacklisting after repeated failures to avoid burning time.
        if hasattr(r, "failed") and r.failed() and rid is not None:
            p = s.pipeline_by_id.get(rid)
            prio = getattr(p, "priority", None) if p is not None else getattr(r, "priority", None)
            if prio is None:
                prio = Priority.BATCH_PIPELINE
            # If any op in this result has exceeded retry budget, stop scheduling this pipeline (especially for batch).
            # (Failing/incomplete is equally penalized, so this avoids cascading latency to other pipelines.)
            budget = s.max_retries.get(prio, 2)
            exceeded = False
            for op in getattr(r, "ops", []) or []:
                opk = _sched_high_042_op_key(op)
                if s.fail_counts.get((rid, opk), 0) > budget:
                    exceeded = True
                    break
            if exceeded and prio == Priority.BATCH_PIPELINE:
                s.pipeline_blacklist.add(rid)

    # 3) Schedule: per tick, at most one op per pipeline to avoid duplicate assignment and reduce interference.
    scheduled_pipelines = set()
    suspensions = []
    assignments = []

    states = _sched_high_042_assignable_states()

    # Precompute whether high-priority has ready work; used to gate batch packing.
    high_ready = _sched_high_042_has_ready_ops(s, Priority.QUERY) or _sched_high_042_has_ready_ops(s, Priority.INTERACTIVE)

    # Helper to try scheduling one op of a given priority on a given pool (RR over that priority queue).
    def try_schedule_from_priority(pool_id, prio, avail_cpu, avail_ram, allow_batch_override=False):
        q = s.waiting.get(prio, [])
        if not q:
            return None  # no change

        # Round-robin scan: pop-front and append-back for fairness within priority class.
        n = len(q)
        for _ in range(n):
            pid = q.pop(0)

            if pid in s.pipeline_blacklist:
                # Drop it from queue by not re-appending (keeps scan costs down over time).
                continue

            p = s.pipeline_by_id.get(pid)
            if p is None:
                continue

            if pid in scheduled_pipelines:
                q.append(pid)
                continue

            st = p.runtime_status()
            if st.is_pipeline_successful():
                # Retire: do not reappend.
                s.pipeline_by_id.pop(pid, None)
                s.enqueue_tick.pop(pid, None)
                continue

            # Thrash control: if we can observe failures in status, prefer to limit retries for batch.
            # (We still allow high-priority retries to avoid 720s penalties.)
            if prio == Priority.BATCH_PIPELINE:
                # If batch is heavily failing (tracked via results when pipeline_id exists), consider blacklisting.
                # This is a soft check; if we cannot track, it has no effect.
                budget = s.max_retries.get(prio, 2)
                # We don't know which op is failing; if any tracked op failures exceed budget, blacklist.
                for (rid, opk), cnt in list(s.fail_counts.items()):
                    if rid == pid and cnt > budget:
                        s.pipeline_blacklist.add(pid)
                        p = None
                        break
                if p is None:
                    continue

            ops = st.get_ops(states, require_parents_complete=True)
            if not ops:
                q.append(pid)
                continue

            # Only schedule one operator per pipeline per tick.
            op = ops[0]
            opk = _sched_high_042_op_key(op)

            cpu_req = _sched_high_042_cpu_request(s, prio, s.executor.pools[pool_id], avail_cpu)
            if cpu_req <= 0:
                q.append(pid)
                continue

            ram_req = _sched_high_042_ram_request(s, prio, s.executor.pools[pool_id], opk, avail_ram)
            if ram_req <= 0:
                q.append(pid)
                continue

            # Enforce headroom reservation against batch packing (unless overridden by aging escape).
            if prio == Priority.BATCH_PIPELINE and not allow_batch_override and s.seen_high_priority_arrival:
                pool = s.executor.pools[pool_id]
                reserve_cpu = s.reserve_high_frac * pool.max_cpu_pool
                reserve_ram = s.reserve_high_frac * pool.max_ram_pool
                # Only apply when there is (or has been) high-priority demand.
                if (avail_cpu - cpu_req) < reserve_cpu or (avail_ram - ram_req) < reserve_ram:
                    q.append(pid)
                    continue

            if cpu_req > avail_cpu or ram_req > avail_ram:
                q.append(pid)
                continue

            # Commit assignment
            assignments.append(
                Assignment(
                    ops=[op],
                    cpu=cpu_req,
                    ram=ram_req,
                    priority=p.priority,
                    pool_id=pool_id,
                    pipeline_id=pid,
                )
            )
            scheduled_pipelines.add(pid)
            q.append(pid)  # keep pipeline in queue for subsequent ops
            return (cpu_req, ram_req)

        return None

    # Pool-specific policy:
    # - If multiple pools: pool 0 is "high-preferred" (batch only when no high ready or batch has aged a lot).
    # - Other pools: shared, but still reserve headroom against batch.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool
        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        is_high_preferred_pool = (s.executor.num_pools > 1 and pool_id == 0)

        # Limit how many assignments we pack into a single pool per tick.
        remaining_slots = s.max_assignments_per_pool
        while remaining_slots > 0 and avail_cpu > 0 and avail_ram > 0:
            made_progress = False

            # Always try query first.
            r = try_schedule_from_priority(pool_id, Priority.QUERY, avail_cpu, avail_ram)
            if r is not None:
                cpu_used, ram_used = r
                avail_cpu -= cpu_used
                avail_ram -= ram_used
                remaining_slots -= 1
                made_progress = True
                continue

            # Then interactive.
            r = try_schedule_from_priority(pool_id, Priority.INTERACTIVE, avail_cpu, avail_ram)
            if r is not None:
                cpu_used, ram_used = r
                avail_cpu -= cpu_used
                avail_ram -= ram_used
                remaining_slots -= 1
                made_progress = True
                continue

            # Finally batch, but gate it to protect high-priority latency.
            # If batch has been waiting long enough, allow it to bypass headroom constraints.
            allow_batch = True
            allow_batch_override = False

            if is_high_preferred_pool:
                # Keep pool 0 mostly for high priority. Only schedule batch if:
                #   - no high work is ready, OR
                #   - some batch is extremely aged (escape hatch)
                if high_ready:
                    allow_batch = False
                    # But if any batch is "escape-aged", allow.
                    for pid in s.waiting.get(Priority.BATCH_PIPELINE, [])[:30]:
                        if _sched_high_042_batch_can_escape(s, pid):
                            allow_batch = True
                            allow_batch_override = True
                            break
                else:
                    allow_batch = True
            else:
                # Shared pools: allow batch, but if high is ready and batch is aged, let it override headroom.
                allow_batch = True
                if high_ready:
                    for pid in s.waiting.get(Priority.BATCH_PIPELINE, [])[:30]:
                        if _sched_high_042_batch_is_aged(s, pid):
                            allow_batch_override = True
                            break

            if allow_batch:
                r = try_schedule_from_priority(
                    pool_id,
                    Priority.BATCH_PIPELINE,
                    avail_cpu,
                    avail_ram,
                    allow_batch_override=allow_batch_override,
                )
                if r is not None:
                    cpu_used, ram_used = r
                    avail_cpu -= cpu_used
                    avail_ram -= ram_used
                    remaining_slots -= 1
                    made_progress = True
                    continue

            if not made_progress:
                break

    return suspensions, assignments
