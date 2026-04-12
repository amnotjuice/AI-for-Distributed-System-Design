# policy_key: scheduler_iter_worst_rich_018
# context: rich
# model: gpt-5.2-2025-12-11
# llm_cost: 0.051080
# generation_seconds: 47.15
# generated_at: 2026-04-12T00:59:14.916385
@register_scheduler_init(key="scheduler_iter_worst_rich_018")
def scheduler_iter_worst_rich_018_init(s):
    """
    Iteration 2: Fix obvious flaws causing OOM storms and zero completion.

    Key changes vs prior attempt:
      - Never drop a pipeline just because it has FAILED ops; FAILED is retryable (ASSIGNABLE).
      - Allocate RAM much more aggressively (near-available) to avoid repeated OOMs.
      - Still prioritize QUERY > INTERACTIVE > BATCH, with mild batch CPU capping to protect latency.
      - Learn per-(pipeline,op) RAM hints from failures when pipeline_id is available on results.
    """
    from collections import deque

    s.q_query = deque()
    s.q_interactive = deque()
    s.q_batch = deque()

    # Prevent duplicate enqueue
    s.enqueued = set()  # pipeline_id

    # Per op resource hints (best-effort; only if pipeline_id is available on results)
    s.op_ram_hint = {}  # (pipeline_id, op_key) -> ram
    s.op_cpu_hint = {}  # (pipeline_id, op_key) -> cpu

    # Per pipeline failure counts (to avoid infinite loops on pathological cases)
    s.pipeline_failures = {}  # pipeline_id -> count
    s.max_failures_before_drop = 50

    # Aging to reduce starvation
    s.tick = 0
    s.pipeline_enqueued_at = {}  # pipeline_id -> tick
    s.batch_promotion_ticks = 400


@register_scheduler(key="scheduler_iter_worst_rich_018")
def scheduler_iter_worst_rich_018_scheduler(s, results, pipelines):
    from collections import deque

    s.tick += 1

    def _queue_for_priority(prio):
        if prio == Priority.QUERY:
            return s.q_query
        if prio == Priority.INTERACTIVE:
            return s.q_interactive
        return s.q_batch

    def _enqueue_pipeline(p):
        pid = p.pipeline_id
        if pid in s.enqueued:
            return
        s.enqueued.add(pid)
        s.pipeline_enqueued_at[pid] = s.tick
        _queue_for_priority(p.priority).append(p)

    def _drop_pipeline(p):
        pid = p.pipeline_id
        s.enqueued.discard(pid)
        s.pipeline_enqueued_at.pop(pid, None)
        s.pipeline_failures.pop(pid, None)

    def _effective_priority(p):
        # Promote long-waiting batch to interactive to avoid starvation.
        if p.priority != Priority.BATCH_PIPELINE:
            return p.priority
        waited = s.tick - s.pipeline_enqueued_at.get(p.pipeline_id, s.tick)
        if waited >= s.batch_promotion_ticks:
            return Priority.INTERACTIVE
        return Priority.BATCH_PIPELINE

    def _op_key(op):
        # Best-effort stable identity within a run.
        for attr in ("op_id", "operator_id", "id", "name"):
            if hasattr(op, attr):
                return (attr, getattr(op, attr))
        return ("py_id", id(op))

    def _extract_min_ram(op):
        # Try common attribute names; otherwise return None.
        for attr in (
            "min_ram", "ram_min", "min_memory", "memory_min",
            "required_ram", "required_memory", "peak_ram", "peak_memory",
            "ram", "memory"
        ):
            if hasattr(op, attr):
                try:
                    v = getattr(op, attr)
                    if v is None:
                        continue
                    v = int(v)
                    if v > 0:
                        return v
                except Exception:
                    continue
        return None

    def _get_ready_ops(p):
        st = p.runtime_status()
        ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        if not ops:
            return []
        return ops[:1]  # one op at a time per pipeline

    def _pool_order_for_priority(eprio):
        # If multiple pools exist, bias pool 0 toward latency-sensitive work.
        if s.executor.num_pools <= 1:
            return list(range(s.executor.num_pools))
        if eprio in (Priority.QUERY, Priority.INTERACTIVE):
            return [0] + [i for i in range(1, s.executor.num_pools)]
        return [i for i in range(1, s.executor.num_pools)] + [0]

    def _size_for(p, op, pool_id):
        pool = s.executor.pools[pool_id]
        avail_cpu = int(pool.avail_cpu_pool)
        avail_ram = int(pool.avail_ram_pool)
        if avail_cpu <= 0 or avail_ram <= 0:
            return 0, 0

        eprio = _effective_priority(p)

        # CPU policy:
        # - QUERY: take most available to finish fast.
        # - INTERACTIVE: take most available (but at least 1).
        # - BATCH: cap CPU to preserve responsiveness.
        if eprio == Priority.BATCH_PIPELINE:
            cpu_cap = max(1, int(pool.max_cpu_pool * 0.5))
            req_cpu = min(avail_cpu, cpu_cap)
        else:
            req_cpu = avail_cpu

        # RAM policy (critical to avoid OOM):
        # - Aim to allocate most available RAM (OOM dominates).
        # - Respect known operator minimums and learned hints.
        op_min = _extract_min_ram(op)
        key = (p.pipeline_id, _op_key(op))

        hinted_ram = s.op_ram_hint.get(key, None)
        target_floor = 1
        if op_min is not None:
            target_floor = max(target_floor, op_min)
        if hinted_ram is not None:
            target_floor = max(target_floor, int(hinted_ram))

        # Aggressive default: grab a large fraction of what's available in the pool.
        # Batch slightly less aggressive than interactive/query.
        frac = 0.95 if eprio == Priority.BATCH_PIPELINE else 0.98
        greedy = max(target_floor, int(avail_ram * frac))

        req_ram = min(avail_ram, max(1, greedy))

        # If we still can't meet a known floor, fail fast by taking all available RAM.
        # (Better chance than under-allocating.)
        if target_floor > req_ram:
            req_ram = avail_ram

        # Also allow small CPU hints (rarely needed); keep bounded by availability.
        hinted_cpu = s.op_cpu_hint.get(key, None)
        if hinted_cpu is not None:
            try:
                req_cpu = min(avail_cpu, max(1, int(hinted_cpu)))
            except Exception:
                pass

        return max(1, req_cpu), max(1, req_ram)

    # Ingest new pipelines
    for p in pipelines:
        _enqueue_pipeline(p)

    # Learn from results: bump RAM hints on OOM-like errors
    for r in results:
        if not (hasattr(r, "failed") and r.failed()):
            continue

        pid = getattr(r, "pipeline_id", None)
        # Track per-pipeline failure counts if possible
        if pid is not None:
            s.pipeline_failures[pid] = s.pipeline_failures.get(pid, 0) + 1

        err = str(getattr(r, "error", "") or "")
        oom_like = ("oom" in err.lower()) or ("out of memory" in err.lower()) or ("memory" in err.lower())

        # If we don't know pid, we can't key hints precisely; skip.
        if pid is None:
            continue

        ran_cpu = int(getattr(r, "cpu", 1) or 1)
        ran_ram = int(getattr(r, "ram", 1) or 1)
        ops = getattr(r, "ops", []) or []
        for op in ops:
            key = (pid, _op_key(op))
            prev = s.op_ram_hint.get(key, 0)

            if oom_like:
                # Exponential RAM increase on OOM
                s.op_ram_hint[key] = max(prev, max(ran_ram * 2, ran_ram + 1))
            else:
                # Mild RAM increase on other failures (may still be underprovision)
                s.op_ram_hint[key] = max(prev, max(int(ran_ram * 1.25) + 1, ran_ram + 1))

            # CPU rarely fixes OOM; only bump a little for non-OOM failures
            if not oom_like:
                pc = s.op_cpu_hint.get(key, 0)
                s.op_cpu_hint[key] = max(pc, int(ran_cpu * 1.25) + 1)

    # Early exit if nothing changed
    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    # We assign at most one op per pool per tick (like the naive baseline),
    # but choose the highest-priority runnable pipeline.
    # We do a bounded scan of each queue to find a runnable pipeline.
    queues_in_order = (s.q_query, s.q_interactive, s.q_batch)

    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        if int(pool.avail_cpu_pool) <= 0 or int(pool.avail_ram_pool) <= 0:
            continue

        chosen = None
        chosen_ops = None

        # Search for a runnable pipeline; do not permanently pop items (rotate).
        for q in queues_in_order:
            for _ in range(len(q)):
                p = q.popleft()

                st = p.runtime_status()
                if st.is_pipeline_successful():
                    _drop_pipeline(p)
                    continue

                # Drop pathological pipelines that are failing endlessly (best-effort).
                if s.pipeline_failures.get(p.pipeline_id, 0) >= s.max_failures_before_drop:
                    _drop_pipeline(p)
                    continue

                # If this pipeline prefers other pools strongly, lightly bias away (but don't block).
                eprio = _effective_priority(p)
                preferred = _pool_order_for_priority(eprio)
                if s.executor.num_pools > 1:
                    # If pool 0 and batch, try to keep batch off pool 0 if other pools exist.
                    if pool_id == 0 and eprio == Priority.BATCH_PIPELINE and 1 in preferred:
                        q.append(p)
                        continue

                ops = _get_ready_ops(p)
                if not ops:
                    q.append(p)
                    continue

                chosen = p
                chosen_ops = ops
                # Requeue now to preserve round-robin among same-priority pipelines.
                q.append(p)
                break

            if chosen is not None:
                break

        if chosen is None:
            continue

        cpu, ram = _size_for(chosen, chosen_ops[0], pool_id)
        if cpu <= 0 or ram <= 0:
            continue

        assignments.append(
            Assignment(
                ops=chosen_ops,
                cpu=cpu,
                ram=ram,
                priority=chosen.priority,
                pool_id=pool_id,
                pipeline_id=chosen.pipeline_id,
            )
        )

    return suspensions, assignments
