# policy_key: scheduler_iter_worst_rich_015
# context: rich
# model: gpt-5.2-2025-12-11
# llm_cost: 0.039656
# generation_seconds: 35.48
# generated_at: 2026-04-12T00:57:13.927216
@register_scheduler_init(key="scheduler_iter_worst_rich_015")
def scheduler_iter_worst_rich_015_init(s):
    """
    Priority-aware FIFO with "RAM-safety-first" sizing + OOM backoff.

    Changes vs naive FIFO (small, obvious fixes first):
      - Never drop pipelines just because an operator FAILED; FAILED ops are retryable in Eudoxia.
      - Always schedule higher priority first (QUERY > INTERACTIVE > BATCH).
      - Allocate RAM generously (RAM above minimum has no perf penalty, but under-alloc causes OOM).
      - Learn per-(pipeline, op) RAM hints from OOM failures and retry with higher RAM.
      - Mild CPU shaping by priority to reduce interference while keeping throughput reasonable.

    Goal: dramatically reduce OOM churn (which was driving 0% completion) and improve weighted latency
    by protecting high-priority work through queue ordering + resource sizing.
    """
    from collections import deque

    s.q_query = deque()
    s.q_interactive = deque()
    s.q_batch = deque()

    # Best-effort de-dup / aging bookkeeping
    s.enqueued_at = {}  # pipeline_id -> tick
    s.tick = 0

    # Per-op learned minimum-ish RAM from OOMs; key: (pipeline_id, op_key) -> ram
    s.op_ram_hint = {}

    # Policy knobs (kept simple)
    s.batch_aging_ticks = 300  # promote old batch to interactive to avoid starvation
    s.min_cpu_slice = 1
    s.min_ram_slice = 1

    # RAM "safety" fractions (fraction of pool max we are willing to hand to a single op)
    # Note: giving more RAM is safe; this mainly avoids completely monopolizing a pool.
    s.ram_frac_query = 0.95
    s.ram_frac_interactive = 0.90
    s.ram_frac_batch = 0.85

    # CPU shaping fractions
    s.cpu_frac_query = 1.00
    s.cpu_frac_interactive = 0.80
    s.cpu_frac_batch = 0.60


@register_scheduler(key="scheduler_iter_worst_rich_015")
def scheduler_iter_worst_rich_015_scheduler(s, results, pipelines):
    """
    Step function: ingest arrivals, update RAM hints from OOM failures, then assign at most
    one ready operator per pool, prioritizing higher priority queues.

    The key fix: do NOT treat "has failures" as terminal; FAILED ops are retryable.
    """
    s.tick += 1

    # ---- helpers ----
    def _op_key(op):
        # Best-effort stable identity.
        if hasattr(op, "op_id"):
            return ("op_id", getattr(op, "op_id"))
        if hasattr(op, "operator_id"):
            return ("operator_id", getattr(op, "operator_id"))
        if hasattr(op, "name"):
            return ("name", getattr(op, "name"))
        return ("py_id", id(op))

    def _queue_for_priority(prio):
        if prio == Priority.QUERY:
            return s.q_query
        if prio == Priority.INTERACTIVE:
            return s.q_interactive
        return s.q_batch

    def _effective_priority(p):
        # Simple aging: promote long-waiting batch to interactive.
        if p.priority != Priority.BATCH_PIPELINE:
            return p.priority
        enq = s.enqueued_at.get(p.pipeline_id, s.tick)
        if (s.tick - enq) >= s.batch_aging_ticks:
            return Priority.INTERACTIVE
        return p.priority

    def _iter_queues_by_importance():
        # Base order; batch aging handled via _effective_priority when selecting.
        return (s.q_query, s.q_interactive, s.q_batch)

    def _ram_frac_for(prio):
        if prio == Priority.QUERY:
            return s.ram_frac_query
        if prio == Priority.INTERACTIVE:
            return s.ram_frac_interactive
        return s.ram_frac_batch

    def _cpu_frac_for(prio):
        if prio == Priority.QUERY:
            return s.cpu_frac_query
        if prio == Priority.INTERACTIVE:
            return s.cpu_frac_interactive
        return s.cpu_frac_batch

    def _learn_from_results():
        # Increase RAM hint on OOM-like failures for the involved ops.
        for r in results:
            if not (hasattr(r, "failed") and r.failed()):
                continue

            err = str(getattr(r, "error", "") or "")
            oom_like = ("oom" in err.lower()) or ("out of memory" in err.lower())

            # If we cannot key to a pipeline, we can't store per-op hints.
            pid = getattr(r, "pipeline_id", None)
            if pid is None:
                continue

            if not oom_like:
                continue

            ran_ram = int(getattr(r, "ram", 0) or 0)
            if ran_ram <= 0:
                continue

            for op in (getattr(r, "ops", []) or []):
                k = (pid, _op_key(op))
                prev = int(s.op_ram_hint.get(k, 0) or 0)

                # Double RAM on OOM (bounded later by pool max). Add +1 to ensure growth from 1.
                bumped = max(prev, ran_ram * 2 + 1)
                s.op_ram_hint[k] = bumped

    def _size_for(pool_id, p, op):
        pool = s.executor.pools[pool_id]
        avail_cpu = int(pool.avail_cpu_pool)
        avail_ram = int(pool.avail_ram_pool)
        if avail_cpu <= 0 or avail_ram <= 0:
            return 0, 0

        eff_prio = _effective_priority(p)

        # CPU: shape by priority but always request at least 1 if any is available.
        cpu_cap = max(s.min_cpu_slice, int(pool.max_cpu_pool * _cpu_frac_for(eff_prio)))
        req_cpu = max(s.min_cpu_slice, min(avail_cpu, cpu_cap))

        # RAM: be generous to avoid OOM; take a large slice of pool max and availability.
        ram_cap = max(s.min_ram_slice, int(pool.max_ram_pool * _ram_frac_for(eff_prio)))
        req_ram = max(s.min_ram_slice, min(avail_ram, ram_cap))

        # Apply learned hint (from OOM) if higher than current request; still bounded by availability.
        hint = int(s.op_ram_hint.get((p.pipeline_id, _op_key(op)), 0) or 0)
        if hint > req_ram:
            req_ram = min(avail_ram, max(req_ram, hint))

        return req_cpu, req_ram

    # ---- ingest arrivals ----
    for p in pipelines:
        pid = p.pipeline_id
        if pid not in s.enqueued_at:
            s.enqueued_at[pid] = s.tick
        _queue_for_priority(p.priority).append(p)

    if not pipelines and not results:
        return [], []

    # ---- learn from last tick ----
    _learn_from_results()

    suspensions = []
    assignments = []

    # ---- dispatch: one assignment per pool ----
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        if int(pool.avail_cpu_pool) <= 0 or int(pool.avail_ram_pool) <= 0:
            continue

        chosen = None
        chosen_ops = None

        # Scan queues in priority order; within each queue, rotate until we find a runnable op.
        # Additionally, allow aged batch to compete by treating it as interactive during selection.
        best_eff_rank = -1
        best_candidate = None
        best_candidate_ops = None
        best_candidate_queue = None

        # Bounded scan: examine at most current queue lengths (keeps O(n) per tick).
        for q in _iter_queues_by_importance():
            qlen = len(q)
            for _ in range(qlen):
                p = q.popleft()

                st = p.runtime_status()
                if st.is_pipeline_successful():
                    # Completed pipeline: drop bookkeeping and do not requeue.
                    s.enqueued_at.pop(p.pipeline_id, None)
                    continue

                # Find one ready operator (parents complete). FAILED ops are retryable via ASSIGNABLE_STATES.
                ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
                if not ops:
                    q.append(p)
                    continue

                eff = _effective_priority(p)
                # Rank: QUERY(3) > INTERACTIVE(2) > BATCH(1)
                rank = 3 if eff == Priority.QUERY else (2 if eff == Priority.INTERACTIVE else 1)

                # Prefer the best effective priority among runnable candidates.
                if rank > best_eff_rank:
                    best_eff_rank = rank
                    best_candidate = p
                    best_candidate_ops = ops[:1]
                    best_candidate_queue = q

                # Keep scanning to see if there's an even higher effective priority in this tick.
                q.append(p)

            # Early stop if we already found a QUERY candidate.
            if best_eff_rank == 3:
                break

        chosen = best_candidate
        chosen_ops = best_candidate_ops

        if chosen is None or not chosen_ops:
            continue

        op = chosen_ops[0]
        req_cpu, req_ram = _size_for(pool_id, chosen, op)
        if req_cpu <= 0 or req_ram <= 0:
            continue

        assignments.append(
            Assignment(
                ops=chosen_ops,
                cpu=req_cpu,
                ram=req_ram,
                priority=chosen.priority,  # keep original priority
                pool_id=pool_id,
                pipeline_id=chosen.pipeline_id,
            )
        )

    return suspensions, assignments
