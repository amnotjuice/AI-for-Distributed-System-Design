# policy_key: scheduler_high_015
# reasoning_effort: high
# exp: reasoning
# model: gpt-5.2-2025-12-11
# llm_cost: 0.211145
# generation_seconds: 209.44
# generated_at: 2026-04-10T01:01:15.779843
@register_scheduler_init(key="scheduler_high_015")
def scheduler_high_015_init(s):
    """
    Priority + completion-focused scheduler.

    Goals:
      - Minimize weighted latency dominated by QUERY/INTERACTIVE.
      - Avoid OOM-driven failures by learning per-operator RAM hints and retrying with backoff.
      - Preserve headroom for high-priority arrivals (especially when only one pool) by limiting BATCH
        consumption unless the system is otherwise idle or the batch has aged.
      - Keep simple: no preemption (requires runtime visibility into active containers), one-op per pipeline per tick.

    Core ideas:
      1) Three priority queues (QUERY, INTERACTIVE, BATCH) with SRPT-like tie-break (fewest remaining ops).
      2) RAM hint learning from ExecutionResult; on OOM, increase RAM and retry (bounded).
      3) Batch throttling via per-pool "reserve" headroom while high-priority work is waiting.
      4) Batch aging: after enough waiting, batches can borrow beyond reserves to avoid indefinite 720s penalties.
    """
    from collections import deque

    s.tick = 0

    # Pipeline tracking
    s.pipeline_by_id = {}
    s.enqueue_tick = {}  # pipeline_id -> first seen tick

    # Priority queues hold pipeline_ids (no duplicates)
    s.q = {
        Priority.QUERY: deque(),
        Priority.INTERACTIVE: deque(),
        Priority.BATCH_PIPELINE: deque(),
    }
    s.qset = {
        Priority.QUERY: set(),
        Priority.INTERACTIVE: set(),
        Priority.BATCH_PIPELINE: set(),
    }

    # Operator-level resource hints learned from outcomes
    # Keyed by operator object (unique per pipeline DAG in typical simulators)
    s.op_ram_hint = {}       # op -> last known-good RAM (or next-try RAM after failure)
    s.op_fail_count = {}     # op -> count of failures observed
    s.op_give_up = set()     # ops deemed impossible (likely > max pool RAM)

    # Pool resource caps used for hint bounding
    s.pool_max_ram = {}
    s.pool_max_cpu = {}
    max_ram = 0
    for i in range(s.executor.num_pools):
        p = s.executor.pools[i]
        s.pool_max_ram[i] = p.max_ram_pool
        s.pool_max_cpu[i] = p.max_cpu_pool
        if p.max_ram_pool > max_ram:
            max_ram = p.max_ram_pool
    s.global_max_ram = max_ram

    # Tuning knobs (kept conservative to reduce 720s penalties)
    s.max_retries = 5

    # Default sizing fractions (RAM is safety-first; CPU is moderate to allow concurrency)
    s.ram_frac = {
        Priority.QUERY: 0.45,
        Priority.INTERACTIVE: 0.35,
        Priority.BATCH_PIPELINE: 0.25,
    }
    s.cpu_frac = {
        Priority.QUERY: 0.60,
        Priority.INTERACTIVE: 0.50,
        Priority.BATCH_PIPELINE: 0.40,
    }

    # Headroom preservation (only constrains BATCH when high-priority is waiting)
    # Latency pool is pool 0 when multiple pools exist.
    s.reserve_frac_latency_pool = 0.35
    s.reserve_frac_other_pool = 0.20

    # Batch aging: after this many ticks, batch can borrow beyond reserves to avoid indefinite starvation
    s.batch_promote_after = 250


def _is_oom_error(err) -> bool:
    try:
        if err is None:
            return False
        s = str(err).upper()
        return ("OOM" in s) or ("OUT OF MEMORY" in s) or ("MEMORY" in s and "KILL" in s)
    except Exception:
        return False


def _safe_min_cpu(avail_cpu):
    # CPU quantities may be float; treat 1.0 as the minimum useful allocation.
    try:
        return 1.0 if avail_cpu >= 1.0 else 0.0
    except Exception:
        return 0.0


def _default_ram_for(s, priority, pool_id):
    max_ram = s.pool_max_ram.get(pool_id, s.global_max_ram)
    frac = s.ram_frac.get(priority, 0.25)
    # Keep a meaningful floor to reduce early OOM retries; still allow concurrency.
    floor = 0.10 * max_ram
    ram = frac * max_ram
    return max(floor, ram)


def _default_cpu_for(s, priority, pool_id, avail_cpu):
    max_cpu = s.pool_max_cpu.get(pool_id, avail_cpu)
    frac = s.cpu_frac.get(priority, 0.4)
    cpu = frac * max_cpu
    # Don't exceed availability; ensure at least 1 if possible.
    cpu = min(cpu, avail_cpu)
    min_cpu = _safe_min_cpu(avail_cpu)
    if cpu < min_cpu:
        cpu = min_cpu
    return cpu


def _remaining_ops(status):
    # SRPT-ish tie-breaker: fewer remaining ops => prefer (reduces mean/weighted latency).
    try:
        total = 0
        for v in status.state_counts.values():
            total += v
        completed = status.state_counts.get(OperatorState.COMPLETED, 0)
        rem = max(0, total - completed)
        return rem
    except Exception:
        return 10**9


def _cleanup_queue(s, prio):
    # Remove completed pipelines and keep queues free of dead entries.
    from collections import deque
    q = s.q[prio]
    new_q = deque()
    new_set = set()

    while q:
        pid = q.popleft()
        p = s.pipeline_by_id.get(pid)
        if p is None:
            continue
        try:
            st = p.runtime_status()
            if st.is_pipeline_successful():
                continue
        except Exception:
            # If we can't read status, keep it to avoid dropping.
            pass
        new_q.append(pid)
        new_set.add(pid)

    s.q[prio] = new_q
    s.qset[prio] = new_set


def _enqueue_pipeline(s, p):
    pid = p.pipeline_id
    s.pipeline_by_id[pid] = p
    if pid not in s.enqueue_tick:
        s.enqueue_tick[pid] = s.tick
    pr = p.priority
    if pid not in s.qset[pr]:
        s.q[pr].append(pid)
        s.qset[pr].add(pid)


def _has_ready(s, prio, assigned_pipelines, assigned_ops, scan=12):
    # Fast check whether there exists runnable work of a given priority not already assigned this tick.
    q = s.q[prio]
    n = min(len(q), scan)
    for i in range(n):
        pid = q[i]
        if pid in assigned_pipelines:
            continue
        p = s.pipeline_by_id.get(pid)
        if p is None:
            continue
        try:
            st = p.runtime_status()
            if st.is_pipeline_successful():
                continue
            ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
            for op in ops[:2]:
                if op in s.op_give_up:
                    continue
                if op in assigned_ops:
                    continue
                return True
        except Exception:
            continue
    return False


def _find_candidate(s, prio, pool_id, assigned_pipelines, assigned_ops, blocked_pipelines, max_scan=30):
    """
    Pick one (pipeline, op) from priority queue with SRPT-like tie-break (fewest remaining ops),
    skipping already-assigned pipelines/ops and pool-blocked pipelines.
    """
    q = s.q[prio]
    if not q:
        return None, None

    best_pid = None
    best_op = None
    best_rem = None
    best_age = None

    # Iterate by rotating the queue (fairness) while choosing best among scanned.
    scanned = 0
    for _ in range(min(len(q), max_scan)):
        pid = q.popleft()
        q.append(pid)
        scanned += 1

        if pid in assigned_pipelines or pid in blocked_pipelines:
            continue

        p = s.pipeline_by_id.get(pid)
        if p is None:
            continue

        try:
            st = p.runtime_status()
            if st.is_pipeline_successful():
                continue

            ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:3]
            if not ops:
                continue

            # Choose the first viable op (they are usually presented in topological order).
            chosen = None
            for op in ops:
                if op in assigned_ops:
                    continue
                if op in s.op_give_up:
                    continue
                chosen = op
                break
            if chosen is None:
                continue

            rem = _remaining_ops(st)
            age = s.tick - s.enqueue_tick.get(pid, s.tick)

            if (best_rem is None) or (rem < best_rem) or (rem == best_rem and age > best_age):
                best_pid = pid
                best_op = chosen
                best_rem = rem
                best_age = age
        except Exception:
            continue

    if best_pid is None:
        return None, None
    return s.pipeline_by_id.get(best_pid), best_op


@register_scheduler(key="scheduler_high_015")
def scheduler_high_015_scheduler(s, results, pipelines):
    """
    See init docstring for policy overview.
    """
    s.tick += 1

    # Ensure ASSIGNABLE_STATES exists even if the environment doesn't provide it as a constant.
    global ASSIGNABLE_STATES
    try:
        ASSIGNABLE_STATES  # noqa: B018 (intentional existence check)
    except Exception:
        ASSIGNABLE_STATES = [OperatorState.PENDING, OperatorState.FAILED]

    # 1) Ingest new pipelines
    for p in pipelines:
        _enqueue_pipeline(s, p)

    # 2) Update operator RAM hints based on execution results (retry-on-failure with RAM backoff)
    for r in results:
        # Bound hints by the pool where it ran (most relevant max RAM); fall back to global max.
        max_ram = s.pool_max_ram.get(getattr(r, "pool_id", None), s.global_max_ram)

        try:
            failed = r.failed()
        except Exception:
            failed = False

        for op in getattr(r, "ops", []) or []:
            if failed:
                cnt = s.op_fail_count.get(op, 0) + 1
                s.op_fail_count[op] = cnt

                prev = s.op_ram_hint.get(op, getattr(r, "ram", 0) or 0)
                last_ram = getattr(r, "ram", prev) or prev

                if _is_oom_error(getattr(r, "error", None)):
                    # Aggressive ramp for OOM: doubling converges quickly.
                    new_ram = max(last_ram * 2.0, last_ram + 0.10 * max_ram)
                else:
                    # Unknown failure: moderate ramp, but don't spin too long.
                    new_ram = max(last_ram * 1.5, last_ram + 0.05 * max_ram)

                new_ram = min(max_ram, new_ram)
                # Keep a small floor so "0" doesn't propagate.
                new_ram = max(new_ram, 0.10 * max_ram)

                s.op_ram_hint[op] = new_ram

                # Give up only if we've repeatedly failed and we're already at (near) max RAM.
                if cnt >= s.max_retries and new_ram >= 0.95 * max_ram:
                    s.op_give_up.add(op)
            else:
                # Success: record RAM as known-good lower bound.
                ram_used = getattr(r, "ram", None)
                if ram_used is not None:
                    prev = s.op_ram_hint.get(op, 0)
                    s.op_ram_hint[op] = max(prev, ram_used)
                # Reset failure count slowly (optional); keep it simple: clear on success.
                if op in s.op_fail_count:
                    s.op_fail_count[op] = 0

    # 3) Queue cleanup for completed pipelines (prevents unbounded scan cost)
    _cleanup_queue(s, Priority.QUERY)
    _cleanup_queue(s, Priority.INTERACTIVE)
    _cleanup_queue(s, Priority.BATCH_PIPELINE)

    # Early exit: if nothing changed and no queued work, do nothing
    if not pipelines and not results:
        if not s.q[Priority.QUERY] and not s.q[Priority.INTERACTIVE] and not s.q[Priority.BATCH_PIPELINE]:
            return [], []

    suspensions = []  # no preemption in this policy
    assignments = []

    # Local guards to avoid double-assigning the same pipeline/op within one scheduler call
    assigned_pipelines = set()
    assigned_ops = set()

    # 4) Schedule per pool, preserving headroom for high-priority work
    num_pools = s.executor.num_pools
    for pool_id in range(num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool

        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        is_latency_pool = (num_pools > 1 and pool_id == 0)
        reserve_frac = s.reserve_frac_latency_pool if is_latency_pool else s.reserve_frac_other_pool
        reserve_cpu = reserve_frac * pool.max_cpu_pool
        reserve_ram = reserve_frac * pool.max_ram_pool

        blocked_pipelines = set()

        # Limit assignments per pool per tick to reduce planner churn and accidental oversubscription.
        max_assignments_this_pool = 6
        made = 0

        while made < max_assignments_this_pool and avail_cpu > 0 and avail_ram > 0:
            # Determine if we should throttle batch to keep headroom for high-priority arrivals.
            # We treat "ready" as "has an assignable op that we haven't already scheduled this tick".
            hi_ready = _has_ready(s, Priority.QUERY, assigned_pipelines, assigned_ops) or _has_ready(
                s, Priority.INTERACTIVE, assigned_pipelines, assigned_ops
            )

            # Candidate priorities: always attempt QUERY then INTERACTIVE, then BATCH.
            # Latency pool strongly prefers high priority, but can run batch if none are ready.
            prio_order = [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]
            if is_latency_pool and hi_ready:
                prio_order = [Priority.QUERY, Priority.INTERACTIVE]

            chosen_pipeline = None
            chosen_op = None
            chosen_prio = None

            for prio in prio_order:
                p, op = _find_candidate(
                    s,
                    prio=prio,
                    pool_id=pool_id,
                    assigned_pipelines=assigned_pipelines,
                    assigned_ops=assigned_ops,
                    blocked_pipelines=blocked_pipelines,
                    max_scan=40 if prio != Priority.BATCH_PIPELINE else 25,
                )
                if p is None or op is None:
                    continue
                chosen_pipeline, chosen_op, chosen_prio = p, op, prio
                break

            if chosen_pipeline is None:
                break

            # If batch has been waiting long enough, allow it to borrow beyond reserves.
            pid = chosen_pipeline.pipeline_id
            age = s.tick - s.enqueue_tick.get(pid, s.tick)
            batch_aged = (chosen_prio == Priority.BATCH_PIPELINE and age >= s.batch_promote_after)

            # Determine RAM request: use learned hint if available, else conservative default.
            ram_req = s.op_ram_hint.get(chosen_op, None)
            if ram_req is None:
                ram_req = _default_ram_for(s, chosen_pipeline.priority, pool_id)
            else:
                # Keep at least a small floor of pool max (stability across pools)
                ram_req = max(ram_req, 0.10 * pool.max_ram_pool)
                ram_req = min(ram_req, pool.max_ram_pool)

            # Determine CPU request: default fraction, then shrink to fit.
            cpu_req = _default_cpu_for(s, chosen_pipeline.priority, pool_id, avail_cpu)
            cpu_req = min(cpu_req, avail_cpu)
            min_cpu = _safe_min_cpu(avail_cpu)
            if cpu_req < min_cpu:
                # Can't fit even the minimum; stop on this pool.
                break

            # If RAM doesn't fit in this pool, try other pipelines; if nothing fits, we'll stop.
            if ram_req > avail_ram:
                blocked_pipelines.add(pid)
                # If we keep hitting blocks, bail out to avoid tight loops.
                if len(blocked_pipelines) > 25:
                    break
                continue

            # Apply batch throttling when high-priority is pending (unless batch is aged).
            if chosen_prio == Priority.BATCH_PIPELINE and hi_ready and (not batch_aged):
                # Don't consume into reserved headroom.
                if (avail_cpu - cpu_req) < reserve_cpu or (avail_ram - ram_req) < reserve_ram:
                    # If high priority is waiting, stop scheduling additional batch in this pool now.
                    break

            # Create assignment (one op at a time to avoid duplicates and keep scheduling simple)
            assignment = Assignment(
                ops=[chosen_op],
                cpu=cpu_req,
                ram=ram_req,
                priority=chosen_pipeline.priority,
                pool_id=pool_id,
                pipeline_id=chosen_pipeline.pipeline_id,
            )
            assignments.append(assignment)

            # Local bookkeeping
            assigned_pipelines.add(chosen_pipeline.pipeline_id)
            assigned_ops.add(chosen_op)

            # Update local pool availability
            avail_cpu -= cpu_req
            avail_ram -= ram_req
            made += 1

            # If nearly out of RAM/CPU, stop.
            if avail_cpu < 1.0 or avail_ram <= 0:
                break

    return suspensions, assignments
