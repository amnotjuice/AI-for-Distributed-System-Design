# policy_key: scheduler_iter_worst_rich_016
# context: rich
# model: gpt-5.2-2025-12-11
# llm_cost: 0.043618
# generation_seconds: 31.84
# generated_at: 2026-04-12T00:57:45.775200
@register_scheduler_init(key="scheduler_iter_worst_rich_016")
def scheduler_iter_worst_rich_016_init(s):
    """
    Priority-aware single-op-per-pool scheduler with OOM-safe RAM sizing.

    Key fixes vs the previous attempt (which OOM'd almost everything):
      - Allocate RAM aggressively (near all available pool RAM) because extra RAM has no perf penalty,
        and under-allocating causes catastrophic OOM + zero throughput.
      - Do NOT drop pipelines just because some operators are FAILED; instead retry FAILED ops.
      - Learn per-(pipeline, op) RAM requirement on OOM and immediately raise RAM hint (up to pool max).
      - Keep simple priority queues (QUERY > INTERACTIVE > BATCH) to improve weighted latency.

    Intentionally small step in complexity: avoids preemption and fancy CPU curves until we get completions.
    """
    from collections import deque

    s.q_query = deque()
    s.q_interactive = deque()
    s.q_batch = deque()

    # Keep a set of pipeline_ids we believe are already enqueued to avoid duplicates.
    s.enqueued = set()

    # Per-operator RAM hint learned from OOMs.
    # key: (pipeline_id, op_key) -> ram_hint
    s.op_ram_hint = {}

    # Track retries per op to prevent infinite thrash if pool max still can't satisfy.
    # key: (pipeline_id, op_key) -> retries
    s.op_retries = {}

    # Hard retry cap per operator before we give up scheduling it (leave it queued but effectively skipped).
    s.max_retries_per_op = 6

    # CPU sizing defaults (keep modest to allow potential sharing if simulator supports it later).
    s.query_cpu = 4
    s.interactive_cpu = 2
    s.batch_cpu = 2

    # Allocate RAM as a fraction of available pool RAM (very high to avoid OOM).
    s.ram_frac_query = 0.95
    s.ram_frac_interactive = 0.92
    s.ram_frac_batch = 0.90


@register_scheduler(key="scheduler_iter_worst_rich_016")
def scheduler_iter_worst_rich_016_scheduler(s, results, pipelines):
    """
    Each tick:
      1) Ingest pipelines into per-priority queues (dedup by pipeline_id).
      2) Process results:
           - On OOM-like failure, raise RAM hint for those ops aggressively.
      3) For each pool, dispatch at most one ready operator, always choosing highest priority first.
         RAM request is max(hint, frac * avail_ram), bounded by avail/max.
    """
    def _q_for_priority(prio):
        if prio == Priority.QUERY:
            return s.q_query
        if prio == Priority.INTERACTIVE:
            return s.q_interactive
        return s.q_batch

    def _op_key(op):
        # Best-effort stable identity within a run.
        if hasattr(op, "op_id"):
            return ("op_id", getattr(op, "op_id"))
        if hasattr(op, "operator_id"):
            return ("operator_id", getattr(op, "operator_id"))
        if hasattr(op, "name"):
            return ("name", getattr(op, "name"))
        return ("py_id", id(op))

    def _is_pipeline_done(p):
        st = p.runtime_status()
        return st.is_pipeline_successful()

    def _get_ready_op_list(p):
        st = p.runtime_status()
        ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        if not ops:
            return []
        return ops[:1]  # one op at a time per pipeline

    def _oom_like(err_str):
        e = (err_str or "")
        e = e.lower()
        return ("oom" in e) or ("out of memory" in e) or ("memory" in e)

    # 1) Ingest new pipelines
    for p in pipelines:
        pid = p.pipeline_id
        if pid in s.enqueued:
            continue
        s.enqueued.add(pid)
        _q_for_priority(p.priority).append(p)

    # Fast-path
    if not pipelines and not results:
        return [], []

    # 2) Learn from results (OOM -> higher RAM hint)
    for r in results:
        if not (hasattr(r, "failed") and r.failed()):
            continue

        pid = getattr(r, "pipeline_id", None)
        if pid is None:
            # Can't safely key hints without pipeline_id.
            continue

        ops = getattr(r, "ops", None) or []
        err = str(getattr(r, "error", "") or "")
        is_oom = _oom_like(err)

        # Use observed allocation as a baseline bump target.
        prev_alloc_ram = int(getattr(r, "ram", 1) or 1)

        for op in ops:
            k = (pid, _op_key(op))
            s.op_retries[k] = s.op_retries.get(k, 0) + 1

            # Aggressive bump on OOM: double requested RAM, minimum 4, and at least previous allocation + 1.
            # If non-OOM failure, still bump a bit (may be underprovisioning).
            old_hint = int(s.op_ram_hint.get(k, 0) or 0)
            if is_oom:
                new_hint = max(old_hint, prev_alloc_ram + 1, max(4, prev_alloc_ram * 2))
            else:
                new_hint = max(old_hint, prev_alloc_ram + 1, max(4, int(prev_alloc_ram * 1.25) + 1))
            s.op_ram_hint[k] = new_hint

    suspensions = []
    assignments = []

    # 3) Dispatch: per pool, pick highest-priority runnable pipeline (bounded scan)
    queues = (s.q_query, s.q_interactive, s.q_batch)

    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = int(pool.avail_cpu_pool)
        avail_ram = int(pool.avail_ram_pool)
        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        chosen_pipeline = None
        chosen_ops = None
        chosen_queue = None

        # Find a runnable pipeline in priority order; rotate to preserve FIFO within each queue.
        for q in queues:
            qlen = len(q)
            if qlen == 0:
                continue

            for _ in range(qlen):
                p = q.popleft()

                # Drop finished pipelines from bookkeeping.
                if _is_pipeline_done(p):
                    if p.pipeline_id in s.enqueued:
                        s.enqueued.remove(p.pipeline_id)
                    continue

                ops = _get_ready_op_list(p)
                if not ops:
                    q.append(p)
                    continue

                # Skip ops that exceeded retry cap (avoids thrash if truly unschedulable).
                opk = (p.pipeline_id, _op_key(ops[0]))
                if s.op_retries.get(opk, 0) > s.max_retries_per_op:
                    q.append(p)
                    continue

                chosen_pipeline = p
                chosen_ops = ops
                chosen_queue = q
                break

            if chosen_pipeline is not None:
                break

        if chosen_pipeline is None:
            continue

        # Compute CPU request (small, priority-biased, bounded by availability)
        if chosen_pipeline.priority == Priority.QUERY:
            req_cpu = min(avail_cpu, max(1, int(s.query_cpu)))
            ram_frac = s.ram_frac_query
        elif chosen_pipeline.priority == Priority.INTERACTIVE:
            req_cpu = min(avail_cpu, max(1, int(s.interactive_cpu)))
            ram_frac = s.ram_frac_interactive
        else:
            req_cpu = min(avail_cpu, max(1, int(s.batch_cpu)))
            ram_frac = s.ram_frac_batch

        # Compute RAM request: extremely OOM-averse.
        op0 = chosen_ops[0]
        hint = int(s.op_ram_hint.get((chosen_pipeline.pipeline_id, _op_key(op0)), 0) or 0)

        # Ask for a large fraction of *available* RAM (not max), because only avail can be allocated now.
        # Also respect hint when higher than fraction-of-avail (bounded to avail).
        req_ram = max(1, int(avail_ram * float(ram_frac)))
        req_ram = max(req_ram, min(hint, avail_ram))
        req_ram = min(req_ram, avail_ram)

        # Sanity: never request more than pool max (should already hold, but keep it safe).
        req_cpu = min(req_cpu, int(pool.max_cpu_pool))
        req_ram = min(req_ram, int(pool.max_ram_pool))

        assignments.append(
            Assignment(
                ops=chosen_ops,
                cpu=req_cpu,
                ram=req_ram,
                priority=chosen_pipeline.priority,
                pool_id=pool_id,
                pipeline_id=chosen_pipeline.pipeline_id,
            )
        )

        # Requeue pipeline for subsequent operators.
        if chosen_queue is not None:
            chosen_queue.append(chosen_pipeline)

    return suspensions, assignments
