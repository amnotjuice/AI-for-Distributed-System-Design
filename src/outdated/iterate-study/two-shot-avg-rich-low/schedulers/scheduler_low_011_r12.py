# source: scheduler_low_011.py
# model: gpt-5.2-2025-12-11
# effort: low
# feedback: rich
# iteration: r12
@register_scheduler_init(key="scheduler_low_011_r12")
def scheduler_low_011_r12_init(s):
    """Iteration r12: priority-aware, throughput-friendly scheduler to reduce queueing latency.

    Key changes vs prior attempt (based on feedback):
    - Fix obvious throughput flaw: pack MULTIPLE assignments per pool per tick (prior version did ~1/pool/tick).
    - Keep strict preference for high-priority, but add lightweight fairness so INTERACTIVE is not starved by QUERY.
    - Right-size CPU (caps per priority) to increase parallelism and reduce queueing delays.
    - Make OOM retries actually work without relying on missing ExecutionResult.pipeline_id:
        * Learn RAM hints keyed by operator object identity (id(op)).
        * Allow rescheduling FAILED ops only when we observed an OOM for that op (retryable) and under retry budget.
    - Avoid scheduling BATCH on the "interactive pool" when QUERY/INTERACTIVE backlog exists (simple isolation).
    """
    from collections import deque

    s.tick = 0

    # Per-priority FIFO queues of pipelines.
    s.q = {
        Priority.QUERY: deque(),
        Priority.INTERACTIVE: deque(),
        Priority.BATCH_PIPELINE: deque(),
    }

    # Arrival time (in scheduler ticks) for aging/boosting.
    s.arrival_tick = {}

    # Deficit-based fairness across priorities (low complexity; prevents starvation).
    s.deficit = {
        Priority.QUERY: 0,
        Priority.INTERACTIVE: 0,
        Priority.BATCH_PIPELINE: 0,
    }
    # Larger quantum => more scheduling share.
    # Keep QUERY highest to protect latency, but give INTERACTIVE enough share to complete.
    s.quantum = {
        Priority.QUERY: 8,
        Priority.INTERACTIVE: 5,
        Priority.BATCH_PIPELINE: 1,
    }

    # Pool preference / isolation.
    s.interactive_pool_id = 0

    # Packing/scheduling limits (avoid pathological loops).
    s.max_assignments_per_pool_per_tick = 12
    s.scan_limit_per_pick = 16  # max pipelines scanned within a chosen priority before trying another

    # Baseline sizing by priority: small CPU caps to increase concurrency; RAM as fraction of pool max.
    # (RAM is the main OOM risk; CPU is mostly for latency vs parallelism tradeoff.)
    s.cpu_cap = {
        Priority.QUERY: 4.0,
        Priority.INTERACTIVE: 4.0,
        Priority.BATCH_PIPELINE: 2.0,
    }
    s.cpu_min = {
        Priority.QUERY: 1.0,
        Priority.INTERACTIVE: 1.0,
        Priority.BATCH_PIPELINE: 1.0,
    }
    s.ram_frac = {
        Priority.QUERY: 0.30,        # keep higher to avoid OOM retries on latency-sensitive work
        Priority.INTERACTIVE: 0.25,  # still fairly safe
        Priority.BATCH_PIPELINE: 0.15,
    }
    s.ram_min = 1.0

    # OOM learning keyed by operator identity (ExecutionResult may not carry pipeline_id).
    s.op_ram_hint = {}       # op_key -> ram
    s.op_retryable = set()   # op_key observed to OOM (so retrying FAILED is allowed)
    s.op_attempts = {}       # op_key -> count
    s.op_blacklist = set()   # op_key exceeded retry budget or non-retryable failure
    s.max_retries_per_op = 3

    # Aging thresholds (in ticks): boost older work to prevent indefinite starvation.
    s.boost_interactive_after = 50
    s.boost_batch_after = 200


def _prio_order():
    return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]


def _prio_rank(pr):
    if pr == Priority.QUERY:
        return 3
    if pr == Priority.INTERACTIVE:
        return 2
    return 1


def _is_oom_error(err):
    if err is None:
        return False
    msg = str(err).lower()
    return ("oom" in msg) or ("out of memory" in msg) or ("memoryerror" in msg)


def _op_key(op):
    # Operator objects are generally unique per pipeline instance in the sim.
    return id(op)


def _pipeline_priority(p):
    pr = getattr(p, "priority", Priority.BATCH_PIPELINE)
    if pr in (Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE):
        return pr
    return Priority.BATCH_PIPELINE


def _pipeline_done_or_permafailed(s, pipeline):
    st = pipeline.runtime_status()
    if st.is_pipeline_successful():
        return True

    # If there are FAILED ops, only keep the pipeline if at least one failed op is retryable and not blacklisted.
    if st.state_counts.get(OperatorState.FAILED, 0) > 0:
        # Inspect assignable ops; FAILED ops are included in ASSIGNABLE_STATES in this simulator.
        ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True) or []
        for op in ops:
            if getattr(op, "state", None) == OperatorState.FAILED:
                k = _op_key(op)
                if (k in s.op_retryable) and (k not in s.op_blacklist) and (s.op_attempts.get(k, 0) <= s.max_retries_per_op):
                    return False
        # No retryable failed ops found => treat as permanently failed (drop).
        return True

    return False


def _get_one_assignable_op(pipeline):
    st = pipeline.runtime_status()
    ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
    if not ops:
        return None
    return ops[0]


def _size_request(s, pool, pr, op, avail_cpu, avail_ram):
    # Baseline RAM: fraction of pool max, but not exceeding available.
    ram = max(s.ram_min, pool.max_ram_pool * s.ram_frac.get(pr, 0.2))
    # Apply learned OOM RAM hint (monotonic non-decreasing).
    k = _op_key(op)
    hint = s.op_ram_hint.get(k)
    if hint is not None:
        ram = max(ram, float(hint))

    # Cap RAM to pool max and current availability.
    ram = min(ram, pool.max_ram_pool, avail_ram)

    # CPU: capped small to increase parallelism; never exceed availability.
    cpu = min(s.cpu_cap.get(pr, 2.0), pool.max_cpu_pool, avail_cpu)
    cpu = max(s.cpu_min.get(pr, 1.0), cpu)

    # If we still can't fit RAM minimally, signal can't fit.
    if ram < s.ram_min or ram > avail_ram:
        return None, None

    # If CPU can't fit minimally, also can't run.
    if cpu < s.cpu_min.get(pr, 1.0) or cpu > avail_cpu:
        return None, None

    return cpu, ram


def _effective_deficit(s, pr):
    # Base deficit plus aging boosts (prevents starvation without fully breaking priority).
    d = s.deficit.get(pr, 0)

    q = s.q.get(pr)
    if not q:
        return d

    # Oldest pipeline in that priority queue (approximate).
    p0 = q[0]
    pid = getattr(p0, "pipeline_id", None)
    if pid is None:
        return d

    age = s.tick - s.arrival_tick.get(pid, s.tick)

    if pr == Priority.INTERACTIVE and age >= s.boost_interactive_after:
        d += 10
    if pr == Priority.BATCH_PIPELINE and age >= s.boost_batch_after:
        d += 8

    return d


@register_scheduler(key="scheduler_low_011_r12")
def scheduler_low_011_r12(s, results: list, pipelines: list):
    """
    Priority + deficit packing scheduler.

    Strategy:
    - Enqueue arrivals into per-priority FIFO.
    - Add per-tick scheduling quanta (deficit round-robin with strict-ish priority).
    - For each pool, pack as many single-op assignments as resources allow.
    - Prefer QUERY, but ensure INTERACTIVE progresses (fixes starvation seen previously).
    - Learn from OOM failures and retry failed ops with increased RAM, without relying on pipeline_id in results.
    """
    s.tick += 1

    # Enqueue new pipelines.
    for p in pipelines:
        pr = _pipeline_priority(p)
        s.q[pr].append(p)
        pid = getattr(p, "pipeline_id", None)
        if pid is not None and pid not in s.arrival_tick:
            s.arrival_tick[pid] = s.tick

    # Process results: update OOM hints and retryability.
    for r in results:
        # Mark non-failed ops as not retryable anymore.
        try:
            failed = r.failed()
        except Exception:
            failed = getattr(r, "error", None) is not None

        ops = getattr(r, "ops", None) or []

        if not failed:
            for op in ops:
                k = _op_key(op)
                # Successful completion clears retryable/blacklist status for that op instance.
                if k in s.op_retryable:
                    s.op_retryable.discard(k)
                if k in s.op_blacklist:
                    s.op_blacklist.discard(k)
            continue

        # Failed:
        if _is_oom_error(getattr(r, "error", None)):
            for op in ops:
                k = _op_key(op)
                s.op_retryable.add(k)
                prev = s.op_ram_hint.get(k)
                base = float(getattr(r, "ram", 0.0) or 0.0)
                if prev is None:
                    prev = base if base > 0 else 1.0
                # Exponential backoff for RAM on OOM.
                new_hint = max(1.0, float(prev) * 2.0)
                s.op_ram_hint[k] = new_hint
                s.op_attempts[k] = int(s.op_attempts.get(k, 0)) + 1
                if s.op_attempts[k] > s.max_retries_per_op:
                    s.op_blacklist.add(k)
        else:
            # Non-OOM failures are treated as permanent for that op instance.
            for op in ops:
                k = _op_key(op)
                s.op_blacklist.add(k)

    # Add scheduling quantum each tick.
    for pr in _prio_order():
        s.deficit[pr] = int(s.deficit.get(pr, 0)) + int(s.quantum.get(pr, 0))

    # Nothing to do.
    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    # Helper: try to take one runnable pipeline+op from a given priority queue that fits in the pool.
    def try_pick_from_priority(pr, pool, avail_cpu, avail_ram):
        q = s.q[pr]
        scans = 0
        while q and scans < s.scan_limit_per_pick:
            scans += 1
            p = q.popleft()

            # Drop completed/permafailed pipelines early.
            if _pipeline_done_or_permafailed(s, p):
                continue

            op = _get_one_assignable_op(p)
            if op is None:
                # Not ready yet; keep it in rotation.
                q.append(p)
                continue

            # If op is FAILED, only retry if we know it's retryable and within budget.
            if getattr(op, "state", None) == OperatorState.FAILED:
                k = _op_key(op)
                if (k in s.op_blacklist) or (k not in s.op_retryable) or (s.op_attempts.get(k, 0) > s.max_retries_per_op):
                    # Permanent failure -> drop the pipeline.
                    continue

            cpu, ram = _size_request(s, pool, pr, op, avail_cpu, avail_ram)
            if cpu is None or ram is None:
                # Doesn't fit right now; rotate pipeline to avoid head-of-line blocking.
                q.append(p)
                continue

            return p, op, cpu, ram

        return None, None, None, None

    # Pack each pool.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]

        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)

        if avail_cpu < 1.0 or avail_ram < s.ram_min:
            continue

        made = 0
        while made < s.max_assignments_per_pool_per_tick and avail_cpu >= 1.0 and avail_ram >= s.ram_min:
            # Candidate priorities for this pool.
            # Simple isolation: avoid BATCH in interactive pool when QUERY/INTERACTIVE backlog exists.
            has_hi_backlog = (len(s.q[Priority.QUERY]) > 0) or (len(s.q[Priority.INTERACTIVE]) > 0)
            candidates = []
            for pr in _prio_order():
                if not s.q[pr]:
                    continue
                if pool_id == s.interactive_pool_id and pr == Priority.BATCH_PIPELINE and has_hi_backlog:
                    continue
                candidates.append(pr)

            if not candidates:
                break

            # Pick priority by effective deficit; tie-break by strict priority.
            # If all deficits are <= 0 (rare), still pick the highest priority with backlog.
            best_pr = None
            best_score = None
            for pr in candidates:
                score = _effective_deficit(s, pr)
                if best_pr is None or score > best_score or (score == best_score and _prio_rank(pr) > _prio_rank(best_pr)):
                    best_pr = pr
                    best_score = score

            if best_pr is None:
                break

            # Try to schedule from chosen priority; if can't fit, fall back to other candidates (in score order).
            # This avoids stalls when head items don't fit current resources.
            ordered = sorted(
                candidates,
                key=lambda pr: (_effective_deficit(s, pr), _prio_rank(pr)),
                reverse=True,
            )

            picked = False
            for pr in ordered:
                # Deficit gating: if deficit is negative, skip unless it's QUERY (always allowed) to protect latency.
                if pr != Priority.QUERY and s.deficit.get(pr, 0) <= 0:
                    continue

                p, op, cpu, ram = try_pick_from_priority(pr, pool, avail_cpu, avail_ram)
                if p is None:
                    continue

                assignments.append(
                    Assignment(
                        ops=[op],
                        cpu=cpu,
                        ram=ram,
                        priority=pr,
                        pool_id=pool_id,
                        pipeline_id=p.pipeline_id,
                    )
                )

                # Consume local pool capacity (executor won't reflect it until after this tick).
                avail_cpu -= float(cpu)
                avail_ram -= float(ram)

                # Consume deficit (one "unit" per assignment).
                s.deficit[pr] = int(s.deficit.get(pr, 0)) - 1

                # Requeue pipeline for future ops.
                s.q[pr].append(p)

                made += 1
                picked = True
                break

            if not picked:
                # Nothing fits in this pool right now.
                break

    return suspensions, assignments