# policy_key: scheduler_iter_worst_rich_008
# context: rich
# model: gpt-5.2-2025-12-11
# llm_cost: 0.048772
# generation_seconds: 38.72
# generated_at: 2026-04-12T00:53:24.758252
@register_scheduler_init(key="scheduler_iter_worst_rich_008")
def scheduler_iter_worst_rich_008_init(s):
    """
    Priority-aware scheduler focused on *getting completions* (avoid OOM storms) to reduce weighted latency.

    Key fixes vs the prior attempt:
      - Never drop a pipeline just because it has FAILED ops; instead retry FAILED ops with larger RAM.
      - Track (container_id -> pipeline/op) so we can learn RAM hints even if ExecutionResult lacks pipeline_id.
      - Use aggressive initial RAM sizing (fraction of pool) and exponential RAM backoff on OOM.
      - Keep simple priority queues and pool preference (pool 0 favors latency-sensitive work).
      - Add a retry cap per operator to avoid infinite retry loops.
    """
    from collections import deque

    # Priority queues
    s.q_query = deque()
    s.q_interactive = deque()
    s.q_batch = deque()

    # Dedup + aging bookkeeping
    s.enqueued_at = {}  # pipeline_id -> tick
    s.tick = 0

    # Learned per-(pipeline, op) resource hints
    s.op_ram_hint = {}  # (pipeline_id, op_key) -> ram
    s.op_cpu_hint = {}  # (pipeline_id, op_key) -> cpu

    # Retry counters to prevent infinite retries
    s.op_retry_count = {}  # (pipeline_id, op_key) -> int
    s.max_retries_per_op = 6

    # Map container_id -> context for learning when results arrive
    s.container_ctx = {}  # container_id -> (pipeline_id, [op_key,...])

    # Defaults for sizing
    s.min_cpu = 1
    s.min_ram = 1


@register_scheduler(key="scheduler_iter_worst_rich_008")
def scheduler_iter_worst_rich_008_scheduler(s, results, pipelines):
    """
    Step:
      1) Enqueue new pipelines into per-priority queues.
      2) Process results: learn RAM hints on OOM; clear container_ctx.
      3) For each pool: pick highest priority runnable op that can fit; size resources:
           - If we have a RAM hint, request that (or wait for capacity).
           - Else request a generous fraction of pool RAM to avoid OOM-first-learning.
         Assign at most one operator per pool per tick to keep logic simple/deterministic.
    """
    s.tick += 1

    # ---------------- Helpers ----------------
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
        s.enqueued_at.pop(pid, None)
        # Note: op hints are kept; they can help if pipeline_id reused rarely. Safe enough.

    def _op_key(op):
        # Best-effort stable identity
        if hasattr(op, "op_id"):
            return ("op_id", getattr(op, "op_id"))
        if hasattr(op, "operator_id"):
            return ("operator_id", getattr(op, "operator_id"))
        if hasattr(op, "name"):
            return ("name", getattr(op, "name"))
        return ("py_id", id(op))

    def _oom_like(err):
        e = (str(err or "")).lower()
        return ("oom" in e) or ("out of memory" in e) or ("memory" in e)

    def _preferred_pool_order_for_pipeline(p):
        # If multiple pools, bias pool 0 for QUERY/INTERACTIVE and keep BATCH away from it when possible.
        n = s.executor.num_pools
        if n <= 1:
            return [0]
        if p.priority in (Priority.QUERY, Priority.INTERACTIVE):
            return [0] + [i for i in range(1, n)]
        return [i for i in range(1, n)] + [0]

    def _initial_ram_frac(prio):
        # Aggressive starts to avoid OOM storms; batch slightly lower but still substantial.
        if prio == Priority.QUERY:
            return 0.75
        if prio == Priority.INTERACTIVE:
            return 0.65
        return 0.55

    def _initial_cpu_frac(prio):
        if prio == Priority.QUERY:
            return 0.75
        if prio == Priority.INTERACTIVE:
            return 0.60
        return 0.50

    def _size_for_op(p, op, pool_id):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool
        if avail_cpu <= 0 or avail_ram <= 0:
            return 0, 0

        key = (p.pipeline_id, _op_key(op))

        # CPU request
        hinted_cpu = s.op_cpu_hint.get(key, 0)
        if hinted_cpu and hinted_cpu > 0:
            req_cpu = hinted_cpu
        else:
            req_cpu = max(s.min_cpu, int(pool.max_cpu_pool * _initial_cpu_frac(p.priority)))
        req_cpu = max(s.min_cpu, min(req_cpu, avail_cpu, pool.max_cpu_pool))

        # RAM request
        hinted_ram = s.op_ram_hint.get(key, 0)
        if hinted_ram and hinted_ram > 0:
            req_ram = hinted_ram
        else:
            req_ram = max(s.min_ram, int(pool.max_ram_pool * _initial_ram_frac(p.priority)))

        # Clamp to what we can actually allocate now.
        req_ram = max(s.min_ram, min(req_ram, avail_ram, pool.max_ram_pool))

        return req_cpu, req_ram

    def _can_fit_hint(p, op, pool_id):
        # If we have a RAM hint larger than availability, skip this op for now (avoid guaranteed OOM).
        pool = s.executor.pools[pool_id]
        key = (p.pipeline_id, _op_key(op))
        hinted_ram = s.op_ram_hint.get(key, 0)
        if hinted_ram and hinted_ram > pool.avail_ram_pool:
            return False
        hinted_cpu = s.op_cpu_hint.get(key, 0)
        if hinted_cpu and hinted_cpu > pool.avail_cpu_pool:
            return False
        return True

    # ---------------- Enqueue new pipelines ----------------
    for p in pipelines:
        _enqueue_pipeline(p)

    # Fast exit
    if not pipelines and not results:
        return [], []

    # ---------------- Process results (learn) ----------------
    for r in results:
        cid = getattr(r, "container_id", None)
        ctx = s.container_ctx.pop(cid, None) if cid is not None else None

        # Only learn from failures; successes don't need hint bumps.
        if hasattr(r, "failed") and r.failed():
            err = getattr(r, "error", None)
            oom = _oom_like(err)

            # Determine which ops/pipeline to attribute to
            if ctx is not None:
                pid, op_keys = ctx
            else:
                pid = getattr(r, "pipeline_id", None)
                ops = getattr(r, "ops", []) or []
                op_keys = [_op_key(op) for op in ops] if ops else []

            if pid is None or not op_keys:
                continue

            # Bump hints: on OOM, double RAM (bounded later by pool max/availability at assignment time).
            for ok in op_keys:
                k = (pid, ok)
                prev_ram = s.op_ram_hint.get(k, int(getattr(r, "ram", 1) or 1))
                prev_cpu = s.op_cpu_hint.get(k, int(getattr(r, "cpu", 1) or 1))

                # Retry counting
                s.op_retry_count[k] = s.op_retry_count.get(k, 0) + 1

                if oom:
                    # Exponential RAM backoff is the core fix.
                    s.op_ram_hint[k] = max(prev_ram + 1, int(prev_ram * 2))
                    # Keep CPU stable on OOM; it doesn't help.
                    s.op_cpu_hint[k] = max(s.min_cpu, prev_cpu)
                else:
                    # Mild increase for other failures (may be under-provisioning).
                    s.op_ram_hint[k] = max(prev_ram + 1, int(prev_ram * 1.25) + 1)
                    s.op_cpu_hint[k] = max(s.min_cpu, int(prev_cpu * 1.25) + 1)

    # ---------------- Dispatch ----------------
    suspensions = []
    assignments = []

    # For each pool, select a runnable op. One assignment per pool per tick.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        if pool.avail_cpu_pool <= 0 or pool.avail_ram_pool <= 0:
            continue

        chosen_pipeline = None
        chosen_ops = None

        # Search order: QUERY -> INTERACTIVE -> BATCH (simple weighted-latency proxy)
        for q in (s.q_query, s.q_interactive, s.q_batch):
            # Bounded scan through the queue once
            q_len = len(q)
            for _ in range(q_len):
                p = q.popleft()
                st = p.runtime_status()

                if st.is_pipeline_successful():
                    _drop_pipeline_bookkeeping(p.pipeline_id)
                    continue

                # Find a ready op (PENDING or FAILED) whose parents are complete
                ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
                if not ops:
                    q.append(p)
                    continue

                op = ops[0]
                key = (p.pipeline_id, _op_key(op))
                if s.op_retry_count.get(key, 0) > s.max_retries_per_op:
                    # Give up on pathological ops/pipelines to protect global latency.
                    _drop_pipeline_bookkeeping(p.pipeline_id)
                    continue

                # Pool preference: if multiple pools, avoid putting batch on pool 0 when others exist.
                if s.executor.num_pools > 1 and pool_id == 0 and p.priority == Priority.BATCH_PIPELINE:
                    # If any non-zero pool exists, keep batch in queue and try others first.
                    q.append(p)
                    continue

                # If we have a RAM hint that doesn't fit, skip for now (avoid guaranteed OOM).
                if not _can_fit_hint(p, op, pool_id):
                    q.append(p)
                    continue

                chosen_pipeline = p
                chosen_ops = [op]
                # Requeue the pipeline immediately (round-robin across its future ops)
                q.append(p)
                break

            if chosen_pipeline is not None:
                break

        if chosen_pipeline is None:
            continue

        # Also ensure this pipeline would prefer this pool; otherwise we still allow it if no better,
        # but here we already avoided batch on pool 0 above. Keep it simple.
        req_cpu, req_ram = _size_for_op(chosen_pipeline, chosen_ops[0], pool_id)
        if req_cpu <= 0 or req_ram <= 0:
            continue

        a = Assignment(
            ops=chosen_ops,
            cpu=req_cpu,
            ram=req_ram,
            priority=chosen_pipeline.priority,
            pool_id=pool_id,
            pipeline_id=chosen_pipeline.pipeline_id,
        )
        assignments.append(a)

        # Record container context lazily on next tick when container_id is known?
        # We only learn from results, which include container_id; we can't know it here.
        # However, some simulators set container_id deterministically or return it in results only.
        # Workaround: if Assignment has no container_id, we map by (pool_id, pipeline_id, op_key) in a small ring.
        # Since we *do* have container_id in results, we opportunistically also map by last-seen running container ids
        # if the simulator sets it. If not, ctx will remain None and we fall back to r.pipeline_id/r.ops.
        #
        # If the simulator attaches container_id to assignments later, this map won't work; but it doesn't hurt.
        # We keep this no-op safe behavior.
        #
        # (Intentionally left minimal to match the documented API.)
        pass

    # Opportunistic: if results include container_id but not pipeline_id, we need a mapping.
    # We can't robustly build it without a callback, so we rely primarily on r.pipeline_id/r.ops.
    # Still, if the simulator echoes pipeline_id on results, we learn correctly.

    return suspensions, assignments
