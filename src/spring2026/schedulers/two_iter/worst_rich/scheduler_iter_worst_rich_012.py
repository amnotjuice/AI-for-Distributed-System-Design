# policy_key: scheduler_iter_worst_rich_012
# context: rich
# model: gpt-5.2-2025-12-11
# llm_cost: 0.044318
# generation_seconds: 36.80
# generated_at: 2026-04-12T00:55:38.351557
@register_scheduler_init(key="scheduler_iter_worst_rich_012")
def scheduler_iter_worst_rich_012_init(s):
    """
    Priority-first FIFO with "finish work" sizing + OOM-aware retries.

    Fixes obvious flaws from prior iteration:
      - Never drop a pipeline just because it has FAILED ops; FAILED is retryable in Eudoxia.
      - Allocate enough RAM by default (use most/all available pool RAM) to avoid OOM storms.
      - Use strict priority ordering (QUERY > INTERACTIVE > BATCH) to reduce weighted latency.
      - Keep pool 0 preferentially for latency-sensitive work when multiple pools exist.
      - Learn per-(pipeline,op) RAM hints from OOM failures; retry with larger RAM (up to pool max).
      - Cap retries per op to avoid infinite loops on pathological operators.
    """
    from collections import deque

    s.q_query = deque()
    s.q_interactive = deque()
    s.q_batch = deque()

    s.enqueued_at = {}  # pipeline_id -> tick
    s.tick = 0

    # Learned resource hints for a specific operator within a pipeline
    # key: (pipeline_id, op_key) -> int
    s.op_ram_hint = {}
    s.op_cpu_hint = {}

    # Retry accounting
    # key: (pipeline_id, op_key) -> int attempts
    s.op_attempts = {}
    s.max_attempts_per_op = 6

    # Anti-starvation aging for batch
    s.batch_promotion_ticks = 400


@register_scheduler(key="scheduler_iter_worst_rich_012")
def scheduler_iter_worst_rich_012_scheduler(s, results, pipelines):
    """
    Each tick:
      1) Enqueue new pipelines into priority queues.
      2) Update RAM/CPU hints on failures (OOM -> aggressive RAM bump).
      3) For each pool, pick the highest-effective-priority pipeline with a ready op.
      4) Assign one op per pool, sized to avoid OOMs:
          - QUERY/INTERACTIVE: give it the pool (or most of it) to finish quickly.
          - BATCH: still give substantial RAM (OOMs are disastrous), but prefer non-0 pools.
    """
    s.tick += 1

    # ---------- helpers ----------
    def _queue_for_priority(prio):
        if prio == Priority.QUERY:
            return s.q_query
        if prio == Priority.INTERACTIVE:
            return s.q_interactive
        return s.q_batch

    def _enqueue_pipeline(p):
        pid = p.pipeline_id
        if pid not in s.enqueued_at:
            s.enqueued_at[pid] = s.tick
            _queue_for_priority(p.priority).append(p)

    def _drop_pipeline_bookkeeping(pid):
        if pid in s.enqueued_at:
            del s.enqueued_at[pid]

    def _op_key(op):
        # Best-effort stable op identity
        if hasattr(op, "op_id"):
            return ("op_id", getattr(op, "op_id"))
        if hasattr(op, "operator_id"):
            return ("operator_id", getattr(op, "operator_id"))
        if hasattr(op, "name"):
            return ("name", getattr(op, "name"))
        return ("py_id", id(op))

    def _effective_priority(p):
        # Promote long-waiting batch into INTERACTIVE to guarantee eventual progress
        if p.priority == Priority.BATCH_PIPELINE:
            waited = s.tick - s.enqueued_at.get(p.pipeline_id, s.tick)
            if waited >= s.batch_promotion_ticks:
                return Priority.INTERACTIVE
        return p.priority

    def _priority_rank(prio):
        if prio == Priority.QUERY:
            return 3
        if prio == Priority.INTERACTIVE:
            return 2
        return 1

    def _pool_preference_ok(eff_prio, pool_id):
        # If multiple pools: reserve pool 0 for QUERY/INTERACTIVE when possible
        if s.executor.num_pools <= 1:
            return True
        if pool_id == 0 and eff_prio == Priority.BATCH_PIPELINE:
            return False
        return True

    def _ready_ops(p):
        st = p.runtime_status()
        return st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True) or []

    def _pipeline_done(p):
        return p.runtime_status().is_pipeline_successful()

    def _size_for_op(p, op, pool_id):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool
        if avail_cpu <= 0 or avail_ram <= 0:
            return 0, 0

        eff_prio = _effective_priority(p)

        # Default strategy: avoid OOM storms by giving substantial RAM.
        # - QUERY/INTERACTIVE: basically "take the pool" to minimize latency and preemption needs.
        # - BATCH: still give most RAM to avoid OOM; CPU can be all available since we schedule 1 op/pool.
        if eff_prio in (Priority.QUERY, Priority.INTERACTIVE):
            base_ram = avail_ram
            base_cpu = avail_cpu
        else:
            # Leave a small sliver of RAM headroom in case a high-priority op arrives on same pool
            # (only relevant in single-pool or if batch landed on pool 0 due to lack of others).
            base_ram = max(1, int(avail_ram * 0.90))
            base_cpu = avail_cpu

        key = (p.pipeline_id, _op_key(op))
        hinted_ram = s.op_ram_hint.get(key, 0)
        hinted_cpu = s.op_cpu_hint.get(key, 0)

        req_ram = max(base_ram, hinted_ram)
        req_cpu = max(base_cpu, hinted_cpu)

        # Bound by availability and pool max (defensive)
        req_ram = max(1, min(req_ram, avail_ram, pool.max_ram_pool))
        req_cpu = max(1, min(req_cpu, avail_cpu, pool.max_cpu_pool))
        return req_cpu, req_ram

    # ---------- ingest pipelines ----------
    for p in pipelines:
        _enqueue_pipeline(p)

    if not pipelines and not results:
        return [], []

    # ---------- learn from results ----------
    for r in results:
        if hasattr(r, "failed") and r.failed():
            pid = getattr(r, "pipeline_id", None)
            if pid is None:
                continue

            err = str(getattr(r, "error", "") or "")
            oom_like = ("oom" in err.lower()) or ("out of memory" in err.lower())

            ops = getattr(r, "ops", []) or []
            for op in ops:
                k = (pid, _op_key(op))
                s.op_attempts[k] = s.op_attempts.get(k, 0) + 1

                prev_ram = int(s.op_ram_hint.get(k, int(getattr(r, "ram", 1) or 1)))
                prev_cpu = int(s.op_cpu_hint.get(k, int(getattr(r, "cpu", 1) or 1)))

                if oom_like:
                    # Aggressive RAM backoff: 2x, with a minimum bump to avoid stagnation.
                    new_ram = max(prev_ram + 1, int((getattr(r, "ram", prev_ram) or prev_ram) * 2))
                    s.op_ram_hint[k] = new_ram
                    # Keep CPU stable on OOM; RAM is the main issue.
                    s.op_cpu_hint[k] = prev_cpu
                else:
                    # For non-OOM failures, small increase (may represent mild under-provisioning).
                    s.op_ram_hint[k] = max(prev_ram, int((getattr(r, "ram", prev_ram) or prev_ram) * 1.25) + 1)
                    s.op_cpu_hint[k] = max(prev_cpu, int((getattr(r, "cpu", prev_cpu) or prev_cpu) * 1.10) + 1)

    # ---------- scheduling ----------
    suspensions = []
    assignments = []

    # For each pool, assign one operator (simple, stable; avoids fragmentation).
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        if pool.avail_cpu_pool <= 0 or pool.avail_ram_pool <= 0:
            continue

        chosen = None
        chosen_ops = None
        chosen_eff = None
        chosen_queue = None

        # Scan queues in priority order, but avoid placing batch on pool 0 if other pools exist.
        queues_in_order = (s.q_query, s.q_interactive, s.q_batch)

        # First pass: honor pool preference (batch avoids pool 0)
        for q in queues_in_order:
            for _ in range(len(q)):
                p = q.popleft()

                # Drop completed pipelines (bookkeeping only; lazy queue cleanup)
                if _pipeline_done(p):
                    _drop_pipeline_bookkeeping(p.pipeline_id)
                    continue

                eff = _effective_priority(p)

                ops = _ready_ops(p)
                if not ops:
                    q.append(p)
                    continue

                # Respect preference in this first pass
                if not _pool_preference_ok(eff, pool_id):
                    q.append(p)
                    continue

                chosen = p
                chosen_ops = ops[:1]
                chosen_eff = eff
                chosen_queue = q
                break
            if chosen is not None:
                break

        # Second pass: if nothing found, allow anything runnable
        if chosen is None:
            for q in queues_in_order:
                for _ in range(len(q)):
                    p = q.popleft()

                    if _pipeline_done(p):
                        _drop_pipeline_bookkeeping(p.pipeline_id)
                        continue

                    ops = _ready_ops(p)
                    if not ops:
                        q.append(p)
                        continue

                    chosen = p
                    chosen_ops = ops[:1]
                    chosen_eff = _effective_priority(p)
                    chosen_queue = q
                    break
                if chosen is not None:
                    break

        if chosen is None:
            continue

        op0 = chosen_ops[0]
        opk = (chosen.pipeline_id, _op_key(op0))

        # Give up on pathological ops after too many failures (prevents infinite OOM loops).
        if s.op_attempts.get(opk, 0) >= s.max_attempts_per_op:
            # Mark bookkeeping dropped; queue cleanup remains lazy.
            _drop_pipeline_bookkeeping(chosen.pipeline_id)
            continue

        req_cpu, req_ram = _size_for_op(chosen, op0, pool_id)
        if req_cpu <= 0 or req_ram <= 0:
            # Can't schedule now; requeue
            chosen_queue.append(chosen)
            continue

        assignments.append(
            Assignment(
                ops=chosen_ops,
                cpu=req_cpu,
                ram=req_ram,
                priority=chosen.priority,  # preserve original priority for metrics/accounting
                pool_id=pool_id,
                pipeline_id=chosen.pipeline_id,
            )
        )

        # Requeue pipeline for future ops
        chosen_queue.append(chosen)

    return suspensions, assignments
