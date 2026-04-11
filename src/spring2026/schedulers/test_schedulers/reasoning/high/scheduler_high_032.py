# policy_key: scheduler_high_032
# reasoning_effort: high
# exp: reasoning
# model: gpt-5.2-2025-12-11
# llm_cost: 0.164819
# generation_seconds: 178.82
# generated_at: 2026-04-10T02:44:21.536034
@register_scheduler_init(key="scheduler_high_032")
def scheduler_high_032_init(s):
    """Priority-aware, OOM-resilient, work-conserving scheduler.

    Core ideas:
      - Maintain per-priority round-robin queues (query > interactive > batch).
      - Use deficit round-robin tokens (10/5/1) to guarantee some batch progress
        without sacrificing high-priority latency.
      - Conservative default RAM sizing + adaptive RAM hints on OOM to drive high
        completion rate (failures are very costly in the objective).
      - Avoid oversubscribing a single pipeline: limit inflight ops per pipeline.
      - No preemption (keeps churn low and avoids relying on non-guaranteed APIs).
    """
    from collections import deque, defaultdict

    s.queues = {
        Priority.QUERY: deque(),
        Priority.INTERACTIVE: deque(),
        Priority.BATCH_PIPELINE: deque(),
    }
    s.active = {}  # pipeline_id -> Pipeline
    s.enqueued = set()  # pipeline_id currently present in its priority queue

    # Track inflight ops per pipeline to avoid over-parallelizing one pipeline.
    s.inflight_per_pipeline = defaultdict(int)
    s.max_inflight_per_pipeline = 1

    # Map op -> pipeline_id for accounting when results come back.
    s.op_to_pipeline = {}

    # Adaptive RAM hints per op. Keyed by a stable-ish op key.
    s.ram_hint = {}  # op_key -> suggested RAM
    s.retry_count = defaultdict(int)  # op_key -> retries observed

    # Deficit round robin tokens to ensure low-priority progress (and avoid 720s).
    s.tokens = {
        Priority.QUERY: 0,
        Priority.INTERACTIVE: 0,
        Priority.BATCH_PIPELINE: 0,
    }
    s.quantum = {Priority.QUERY: 10, Priority.INTERACTIVE: 5, Priority.BATCH_PIPELINE: 1}
    # Prevent low-priority token burst from suddenly harming high-priority latency.
    s.token_cap = {Priority.QUERY: 80, Priority.INTERACTIVE: 40, Priority.BATCH_PIPELINE: 6}

    # Default sizing fractions (conservative on RAM to avoid OOM; queries dominate objective).
    s.base_cpu_frac = {Priority.QUERY: 0.60, Priority.INTERACTIVE: 0.50, Priority.BATCH_PIPELINE: 0.35}
    s.base_ram_frac = {Priority.QUERY: 0.50, Priority.INTERACTIVE: 0.42, Priority.BATCH_PIPELINE: 0.32}

    # Retry policy
    s.max_retries_non_oom = 2
    s.max_retries_oom = 6

    # Per-tick assignment bounds to avoid pathological overscheduling loops.
    s.max_assignments_per_pool = 6


@register_scheduler(key="scheduler_high_032")
def scheduler_high_032(s, results, pipelines):
    """
    Scheduler step:
      1) Ingest arrivals into per-priority queues (deduplicated).
      2) Process execution results:
           - decrement inflight counters
           - update RAM hints on OOM (exponential increase)
           - gently decay hints on success
      3) Add DRR tokens.
      4) For each pool (most headroom first), greedily schedule fittable ready ops:
           - choose priority by token availability (batch needs tokens; query/interactive may run even if tokens low)
           - pick next pipeline in RR order with a ready op whose RAM hint fits
           - allocate CPU/RAM sized by priority + per-op hint, bounded by pool availability
    """
    suspensions = []
    assignments = []

    def _op_key(op):
        # Try common stable identifiers; fallback to id(op) (stable for object lifetime in sim).
        for attr in ("op_id", "operator_id", "id", "name", "uid"):
            if hasattr(op, attr):
                v = getattr(op, attr)
                if v is not None:
                    return (attr, v)
        return ("pyid", id(op))

    def _is_oom_error(err):
        if not err:
            return False
        msg = str(err).lower()
        return ("oom" in msg) or ("out of memory" in msg) or ("memoryerror" in msg)

    def _pool_max_ram_all():
        mr = 0
        for i in range(s.executor.num_pools):
            mr = max(mr, s.executor.pools[i].max_ram_pool)
        return mr

    def _desired_sizes(priority, pool, opk):
        # CPU: priority-based fraction of pool capacity, with a minimum of 1.
        cpu = pool.max_cpu_pool * s.base_cpu_frac.get(priority, 0.35)
        if cpu < 1:
            cpu = 1

        # RAM: max(base fraction of pool, per-op hint).
        base_ram = pool.max_ram_pool * s.base_ram_frac.get(priority, 0.32)
        if base_ram <= 0:
            base_ram = pool.max_ram_pool * 0.25

        hint = s.ram_hint.get(opk, None)
        if hint is None:
            # Initialize hint at base RAM to reduce first-try OOMs.
            s.ram_hint[opk] = base_ram
            hint = base_ram

        ram = max(base_ram, hint)
        # Keep a tiny margin below pool max to reduce "exact max" edge behavior.
        cap = pool.max_ram_pool * 0.98
        if ram > cap:
            ram = cap

        return cpu, ram

    def _pipeline_done_or_invalid(p):
        try:
            st = p.runtime_status()
        except Exception:
            return True
        return st.is_pipeline_successful()

    def _remove_pipeline(p):
        pid = p.pipeline_id
        s.active.pop(pid, None)
        s.enqueued.discard(pid)
        # inflight bookkeeping is handled on results; keep counts if still running.

    def _choose_priority_with_ready_work():
        # Prefer priorities with tokens available; allow QUERY/INTERACTIVE even without tokens
        # to protect their latency (they dominate the objective).
        order = [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]
        for pr in order:
            if len(s.queues[pr]) == 0:
                continue
            if pr == Priority.BATCH_PIPELINE and s.tokens[pr] <= 0:
                continue
            return pr

        # If batch has no tokens but only batch exists, allow it to be work-conserving.
        if len(s.queues[Priority.QUERY]) == 0 and len(s.queues[Priority.INTERACTIVE]) == 0:
            if len(s.queues[Priority.BATCH_PIPELINE]) > 0:
                return Priority.BATCH_PIPELINE
        return None

    def _pop_next_fittable(priority, pool, avail_cpu, avail_ram):
        q = s.queues[priority]
        if not q:
            return None

        max_scan = len(q)
        for _ in range(max_scan):
            p = q.popleft()
            pid = p.pipeline_id

            if pid not in s.active:
                # Stale entry.
                continue

            if _pipeline_done_or_invalid(p):
                _remove_pipeline(p)
                continue

            if s.inflight_per_pipeline[pid] >= s.max_inflight_per_pipeline:
                # Keep RR position; pipeline already has inflight work.
                q.append(p)
                continue

            status = p.runtime_status()
            ops = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
            if not ops:
                q.append(p)
                continue

            op = ops[0]
            opk = _op_key(op)

            # If we've retried too much with no signal of OOM, stop wasting cycles:
            # (Still keep pipeline queued; maybe another op becomes ready.)
            # For OOM we keep trying up to a higher bound.
            # NOTE: We do not "drop" pipelines; we just may skip scheduling a hopeless op for now.
            rc = s.retry_count[opk]
            # No special skip here; rely on fitting + caps.

            desired_cpu, desired_ram = _desired_sizes(p.priority, pool, opk)

            # Bound by current availability.
            cpu = min(avail_cpu, desired_cpu)
            ram = min(avail_ram, desired_ram)

            # Require at least 1 CPU and enough RAM to satisfy our hint.
            if cpu < 1 or ram < desired_ram:
                q.append(p)
                continue

            # Found a fit; re-append pipeline for RR fairness.
            q.append(p)
            return p, op, cpu, ram

        return None

    # 1) Ingest arrivals
    for p in pipelines:
        s.active[p.pipeline_id] = p
        if p.pipeline_id not in s.enqueued:
            s.queues[p.priority].append(p)
            s.enqueued.add(p.pipeline_id)

    # 2) Process results: update inflight and RAM hints
    if results:
        max_pool_ram_all = _pool_max_ram_all()

        for r in results:
            # Decrement inflight per pipeline once per pipeline represented in this result.
            pids_touched = set()
            for op in getattr(r, "ops", []) or []:
                opk = _op_key(op)
                pid = s.op_to_pipeline.get(opk, None)
                if pid is not None:
                    pids_touched.add(pid)

            for pid in pids_touched:
                cur = s.inflight_per_pipeline[pid]
                if cur > 0:
                    s.inflight_per_pipeline[pid] = cur - 1

            # Update hints per op.
            failed = r.failed()
            oom = _is_oom_error(getattr(r, "error", None))

            for op in getattr(r, "ops", []) or []:
                opk = _op_key(op)

                if failed:
                    s.retry_count[opk] += 1

                    if oom:
                        # Aggressively raise RAM hint on OOM to avoid repeated failures.
                        prev = s.ram_hint.get(opk, max(getattr(r, "ram", 0), 1))
                        tried = getattr(r, "ram", prev)
                        bumped = max(prev, tried) * 1.7
                        # Cap at the maximum RAM any pool could possibly provide (minus margin).
                        cap = max_pool_ram_all * 0.98
                        if bumped > cap:
                            bumped = cap
                        if bumped < 1:
                            bumped = 1
                        s.ram_hint[opk] = bumped
                    else:
                        # Non-OOM failure: don't keep increasing RAM; allow a couple retries.
                        if s.retry_count[opk] > s.max_retries_non_oom:
                            # Mildly increase RAM anyway in case error is underprovisioning-related.
                            prev = s.ram_hint.get(opk, max(getattr(r, "ram", 0), 1))
                            tried = getattr(r, "ram", prev)
                            bumped = max(prev, tried) * 1.15
                            cap = max_pool_ram_all * 0.98
                            if bumped > cap:
                                bumped = cap
                            s.ram_hint[opk] = bumped
                else:
                    # Success: gently decay RAM hint to improve concurrency over time.
                    prev = s.ram_hint.get(opk, None)
                    if prev is not None:
                        decayed = prev * 0.92
                        if decayed < 1:
                            decayed = 1
                        s.ram_hint[opk] = decayed
                    s.retry_count[opk] = 0

    # If nothing is happening and no queued work, return quickly.
    if not pipelines and not results and not any(len(q) for q in s.queues.values()):
        return [], []

    # 3) Add DRR tokens (cap to prevent bursts)
    for pr in (Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE):
        s.tokens[pr] = min(s.token_cap[pr], s.tokens[pr] + s.quantum[pr])

    # 4) Schedule work: process pools with most available headroom first.
    pool_ids = list(range(s.executor.num_pools))
    pool_ids.sort(
        key=lambda i: (
            s.executor.pools[i].avail_ram_pool,
            s.executor.pools[i].avail_cpu_pool,
            s.executor.pools[i].max_ram_pool,
            s.executor.pools[i].max_cpu_pool,
        ),
        reverse=True,
    )

    for pool_id in pool_ids:
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool
        if avail_cpu < 1 or avail_ram <= 0:
            continue

        local_assignments = 0
        while local_assignments < s.max_assignments_per_pool and avail_cpu >= 1 and avail_ram > 0:
            pr = _choose_priority_with_ready_work()
            if pr is None:
                break

            picked = _pop_next_fittable(pr, pool, avail_cpu, avail_ram)

            # If nothing fits at this priority, try lower priorities (work-conserving),
            # but do not let batch run without tokens if there is higher priority waiting.
            if picked is None:
                if pr == Priority.QUERY:
                    # Try interactive, then batch.
                    picked = _pop_next_fittable(Priority.INTERACTIVE, pool, avail_cpu, avail_ram)
                    if picked is None and (s.tokens[Priority.BATCH_PIPELINE] > 0 or (len(s.queues[Priority.QUERY]) == 0 and len(s.queues[Priority.INTERACTIVE]) == 0)):
                        picked = _pop_next_fittable(Priority.BATCH_PIPELINE, pool, avail_cpu, avail_ram)
                elif pr == Priority.INTERACTIVE:
                    # Try query, then batch.
                    picked = _pop_next_fittable(Priority.QUERY, pool, avail_cpu, avail_ram)
                    if picked is None and (s.tokens[Priority.BATCH_PIPELINE] > 0 or (len(s.queues[Priority.QUERY]) == 0 and len(s.queues[Priority.INTERACTIVE]) == 0)):
                        picked = _pop_next_fittable(Priority.BATCH_PIPELINE, pool, avail_cpu, avail_ram)
                else:
                    # Batch selected but nothing fits.
                    picked = None

            if picked is None:
                break

            p, op, cpu, ram = picked
            opk = _op_key(op)

            # Consume tokens (batch requires tokens; query/interactive optionally consume to keep ratios sane).
            if pr == Priority.BATCH_PIPELINE:
                if s.tokens[pr] > 0:
                    s.tokens[pr] -= 1
                else:
                    # Shouldn't happen due to selection logic; be safe.
                    break
            else:
                if s.tokens[pr] > 0:
                    s.tokens[pr] -= 1

            assignments.append(
                Assignment(
                    ops=[op],
                    cpu=cpu,
                    ram=ram,
                    priority=p.priority,
                    pool_id=pool_id,
                    pipeline_id=p.pipeline_id,
                )
            )

            # Bookkeeping
            s.op_to_pipeline[opk] = p.pipeline_id
            s.inflight_per_pipeline[p.pipeline_id] += 1

            # Update local available resources to keep scheduling consistent within this tick.
            avail_cpu -= cpu
            avail_ram -= ram
            local_assignments += 1

    return suspensions, assignments
