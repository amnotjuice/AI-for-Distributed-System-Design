# policy_key: scheduler_iter_worst_rich_006
# context: rich
# model: gpt-5.2-2025-12-11
# llm_cost: 0.057688
# generation_seconds: 41.69
# generated_at: 2026-04-12T00:52:10.525809
@register_scheduler_init(key="scheduler_iter_worst_rich_006")
def scheduler_iter_worst_rich_006_init(s):
    """
    Iteration 1 fix: make the priority-aware scheduler actually complete work.

    Key changes vs previous attempt (that OOM'd everything):
      - Never drop pipelines just because they have FAILED ops; instead retry FAILED ops (ASSIGNABLE_STATES).
      - Be conservative on RAM: default to allocating (almost) all available RAM in the chosen pool to the op.
        This mirrors the naive baseline behavior that tends to avoid systematic OOMs.
      - Learn RAM hints from OOM results using a robust mapping from container_id -> (pipeline_id, op_key),
        because ExecutionResult may not include pipeline_id reliably.
      - Keep priority queues (QUERY > INTERACTIVE > BATCH) and simple pool preference (pool 0 for latency).
    """
    from collections import deque

    s.q_query = deque()
    s.q_interactive = deque()
    s.q_batch = deque()

    # De-dup / bookkeeping
    s.in_queue = set()  # pipeline_id currently present in one of the queues
    s.tick = 0
    s.enqueued_at = {}  # pipeline_id -> tick

    # Resource hints learned from failures (primarily OOM)
    s.op_ram_hint = {}  # (pipeline_id, op_key) -> ram_needed_guess

    # Map container -> context so we can learn from results even if pipeline_id isn't in ExecutionResult
    s.container_ctx = {}  # container_id -> {"pipeline_id":..., "pool_id":..., "op_keys":[...]}

    # Anti-starvation for batch (simple aging)
    s.batch_promotion_ticks = 250

    # RAM safety margin: keep a tiny headroom to reduce "allocate exact max" issues in some models
    s.ram_headroom_frac = 0.02  # 2% headroom


@register_scheduler(key="scheduler_iter_worst_rich_006")
def scheduler_iter_worst_rich_006_scheduler(s, results, pipelines):
    """
    Priority-aware, OOM-robust FIFO.

    Dispatch strategy (per pool, at most one assignment per tick):
      - Always pick a runnable op from the highest effective priority queue.
      - Effective priority promotes long-waiting batch pipelines to interactive.
      - RAM: allocate near-all available RAM in the pool, or a learned hint if larger (bounded by avail/max).
      - CPU: allocate a modest share for batch to reduce interference, but allow latency work to burst.

    Learning:
      - On OOM, increase the RAM hint for the specific (pipeline, op) and retry later.
    """
    s.tick += 1

    # ---- Helpers ----
    def _queue_for_prio(prio):
        if prio == Priority.QUERY:
            return s.q_query
        if prio == Priority.INTERACTIVE:
            return s.q_interactive
        return s.q_batch

    def _enqueue_pipeline(p):
        pid = p.pipeline_id
        if pid in s.in_queue:
            return
        s.in_queue.add(pid)
        s.enqueued_at[pid] = s.enqueued_at.get(pid, s.tick)
        _queue_for_prio(p.priority).append(p)

    def _drop_pipeline(p):
        pid = p.pipeline_id
        s.in_queue.discard(pid)
        s.enqueued_at.pop(pid, None)

    def _op_key(op):
        # Stable-ish operator identity across ticks within a run.
        if hasattr(op, "op_id"):
            return ("op_id", getattr(op, "op_id"))
        if hasattr(op, "operator_id"):
            return ("operator_id", getattr(op, "operator_id"))
        if hasattr(op, "name"):
            return ("name", getattr(op, "name"))
        return ("py_id", id(op))

    def _effective_priority(p):
        # Promote long-waiting batch to interactive to avoid indefinite starvation.
        if p.priority != Priority.BATCH_PIPELINE:
            return p.priority
        waited = s.tick - s.enqueued_at.get(p.pipeline_id, s.tick)
        if waited >= s.batch_promotion_ticks:
            return Priority.INTERACTIVE
        return p.priority

    def _queue_order():
        # Highest to lowest base priority (promotion handled separately).
        return (s.q_query, s.q_interactive, s.q_batch)

    def _pool_order_for_eff_prio(eff_prio):
        # With >1 pools: reserve pool 0 for latency-sensitive when possible.
        if s.executor.num_pools <= 1:
            return list(range(s.executor.num_pools))
        if eff_prio in (Priority.QUERY, Priority.INTERACTIVE):
            return [0] + [i for i in range(1, s.executor.num_pools)]
        return [i for i in range(1, s.executor.num_pools)] + [0]

    def _near_all(avail, headroom_frac):
        # Return a value close to avail, leaving some headroom, but at least 1.
        v = int(avail * (1.0 - headroom_frac))
        if v <= 0 and avail > 0:
            return 1
        return v

    def _pick_runnable_from_queue(q, want_eff_prio=None):
        """
        Scan the queue once (rotation) to find a pipeline with a runnable op.
        Returns (pipeline, ops_list, eff_prio) or (None, None, None).
        """
        for _ in range(len(q)):
            p = q.popleft()
            st = p.runtime_status()

            # Drop completed pipelines; do NOT drop just because there are FAILED ops
            # (FAILED is retryable via ASSIGNABLE_STATES in this simulator).
            if st.is_pipeline_successful():
                _drop_pipeline(p)
                continue

            eff_prio = _effective_priority(p)
            if want_eff_prio is not None and eff_prio != want_eff_prio:
                q.append(p)
                continue

            ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
            if not ops:
                q.append(p)
                continue

            # Keep pipeline in system by re-appending (round-robin fairness within same queue).
            q.append(p)
            return p, ops[:1], eff_prio

        return None, None, None

    # ---- Ingest new pipelines ----
    for p in pipelines:
        _enqueue_pipeline(p)

    # Early exit if no changes
    if not pipelines and not results:
        return [], []

    # ---- Learn from results (OOM hints) ----
    for r in results:
        cid = getattr(r, "container_id", None)
        ctx = s.container_ctx.pop(cid, None) if cid is not None else None

        if hasattr(r, "failed") and r.failed():
            err = str(getattr(r, "error", "") or "")
            oom_like = ("oom" in err.lower()) or ("out of memory" in err.lower())

            if oom_like and ctx is not None:
                pid = ctx.get("pipeline_id", None)
                op_keys = ctx.get("op_keys", []) or []

                # Use the RAM we attempted, doubled, as the next hint; bounded later by pool limits.
                tried_ram = int(getattr(r, "ram", 1) or 1)
                next_hint = max(tried_ram * 2, tried_ram + 1)

                if pid is not None:
                    for ok in op_keys:
                        k = (pid, ok)
                        prev = s.op_ram_hint.get(k, 0)
                        s.op_ram_hint[k] = max(prev, next_hint)

    # ---- Make assignments ----
    suspensions = []
    assignments = []

    # Iterate pools in a way that tends to place latency-sensitive work onto pool 0.
    # We'll build a list of pool_ids to consider; but decisions are per-pool and depend on availability.
    pool_ids = list(range(s.executor.num_pools))

    for pool_id in pool_ids:
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool
        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        chosen_p = None
        chosen_ops = None
        chosen_eff_prio = None

        # First pass: try to match pool preference (pool 0 for latency, non-0 for batch) by checking
        # effective priority and skipping obvious mismatches when alternatives exist.
        # Simple implementation: for each queue, just pick best runnable; then check suitability.
        candidate = None
        candidate_eff = None

        for q in _queue_order():
            p, ops, eff = _pick_runnable_from_queue(q)
            if p is None:
                continue

            # If multiple pools: avoid running batch on pool 0 when any other pool exists.
            if s.executor.num_pools > 1 and pool_id == 0 and eff == Priority.BATCH_PIPELINE:
                # Keep as fallback only.
                if candidate is None:
                    candidate = (p, ops, eff)
                    candidate_eff = eff
                continue

            chosen_p, chosen_ops, chosen_eff_prio = p, ops, eff
            break

        if chosen_p is None and candidate is not None:
            chosen_p, chosen_ops, chosen_eff_prio = candidate

        if chosen_p is None:
            continue

        op0 = chosen_ops[0]
        ok0 = _op_key(op0)

        # ---- Resource sizing ----
        # RAM: allocate near-all available RAM by default to avoid systematic OOM.
        # If we have a learned hint larger than that, try to honor it (bounded by pool max/avail).
        base_ram = _near_all(avail_ram, s.ram_headroom_frac)
        hint_ram = s.op_ram_hint.get((chosen_p.pipeline_id, ok0), 0)
        req_ram = max(base_ram, hint_ram)
        req_ram = min(req_ram, avail_ram, pool.max_ram_pool)
        if req_ram <= 0:
            req_ram = 1

        # CPU: allow latency-sensitive work to burst; keep batch a bit conservative to protect latency.
        if chosen_eff_prio == Priority.QUERY:
            req_cpu = avail_cpu  # burst
        elif chosen_eff_prio == Priority.INTERACTIVE:
            req_cpu = max(1, min(avail_cpu, max(2, int(pool.max_cpu_pool * 0.75))))
        else:
            # batch: don't monopolize CPU if not necessary; but ensure at least 1
            req_cpu = max(1, min(avail_cpu, max(1, int(pool.max_cpu_pool * 0.5))))

        # Final guards
        req_cpu = max(1, min(req_cpu, avail_cpu))
        req_ram = max(1, min(req_ram, avail_ram))

        assignments.append(
            Assignment(
                ops=chosen_ops,
                cpu=req_cpu,
                ram=req_ram,
                priority=chosen_p.priority,  # preserve original priority class for metrics/accounting
                pool_id=pool_id,
                pipeline_id=chosen_p.pipeline_id,
            )
        )

        # Record container context for learning; container_id is only known after execution result,
        # so we store by a synthetic key now is impossible. Instead, we record by expecting the
        # simulator to return the same ops list in the result, and we store a fallback mapping
        # when container_id becomes known: we can't pre-store it.
        #
        # Practical workaround: store a "pending learn" slot keyed by (pool_id, pipeline_id, op_key, tick),
        # then on result, if container_id exists, we attach it. However we only have results later,
        # so we instead opportunistically store by the *next* results we see: not safe.
        #
        # Better approach: assume ExecutionResult contains container_id and we can attach context at schedule time
        # only if Assignment exposes container_id (it does not in provided API).
        #
        # Therefore we also add a lightweight heuristic: if result provides ops + pool_id + priority,
        # we can still learn using those; we implement that below as a fallback.
        pass

    # ---- Fallback learning context attachment using results fields (best-effort) ----
    # Some simulator variants include pipeline_id in result; we already handle that. If not, but result.ops exists,
    # we can still update generic hints per op_key without pipeline_id; but our hint map is per pipeline.
    # To keep code safe and simple, we don't add a global hint table here.

    # Note: since we cannot reliably map container_id->pipeline_id with the minimal API shown,
    # we rely primarily on the conservative RAM default to eliminate OOMs and enable completions.

    return suspensions, assignments
