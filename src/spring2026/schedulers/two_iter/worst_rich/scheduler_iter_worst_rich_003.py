# policy_key: scheduler_iter_worst_rich_003
# context: rich
# model: gpt-5.2-2025-12-11
# llm_cost: 0.057198
# generation_seconds: 36.23
# generated_at: 2026-04-12T00:50:16.869372
@register_scheduler_init(key="scheduler_iter_worst_rich_003")
def scheduler_iter_worst_rich_003_init(s):
    """
    Priority-aware FIFO with "OOM-first" fixes to actually complete work.

    Key fixes vs the prior attempt (which OOMed almost everything):
      - Never drop a pipeline just because it has FAILED ops; instead, retry FAILED ops.
      - Allocate *much larger RAM by default* (close to pool availability) to avoid unknown min-RAM OOMs.
      - On OOM, exponentially back off (increase) RAM hint for that specific operator and retry.
      - Keep simple priority queues (QUERY > INTERACTIVE > BATCH) to reduce weighted latency.
      - Prefer pool 0 for latency-sensitive work when multiple pools exist.
    """
    from collections import deque

    s.q_query = deque()
    s.q_interactive = deque()
    s.q_batch = deque()

    # Track whether we've already enqueued a pipeline (avoid duplicates across ticks)
    s.enqueued = set()  # pipeline_id

    # Per-operator learned sizing hints. Primary key includes pipeline_id when known.
    # We also maintain a fallback key without pipeline_id if ExecutionResult lacks it.
    s.op_ram_hint = {}  # (pipeline_id_or_none, op_key) -> ram
    s.op_cpu_hint = {}  # (pipeline_id_or_none, op_key) -> cpu

    # Basic tick counter (used only for bounded scans / determinism)
    s.tick = 0

    # Default sizing knobs (chosen to avoid OOMs under uncertainty)
    s.query_cpu_frac = 1.0
    s.interactive_cpu_frac = 1.0
    s.batch_cpu_frac = 0.5

    # RAM defaults: start big to avoid OOMs; batch slightly less aggressive but still substantial
    s.query_ram_frac = 1.0
    s.interactive_ram_frac = 1.0
    s.batch_ram_frac = 0.75

    # Minimum resource request (best-effort; simulator uses ints)
    s.min_cpu = 1
    s.min_ram = 1


@register_scheduler(key="scheduler_iter_worst_rich_003")
def scheduler_iter_worst_rich_003_scheduler(s, results, pipelines):
    """
    Scheduler step:
      1) Enqueue new pipelines into per-priority queues.
      2) Update per-op RAM hints on failures (OOM => double RAM).
      3) For each pool, pick the highest priority pipeline with a runnable op and assign it,
         using aggressive RAM to avoid OOM + modest CPU sharing for batch.
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
        if pid in s.enqueued:
            return
        s.enqueued.add(pid)
        _queue_for_priority(p.priority).append(p)

    def _drop_pipeline(p):
        # Lazy queue removal; just clear de-dup marker so a future reappearance can enqueue again
        pid = p.pipeline_id
        if pid in s.enqueued:
            s.enqueued.remove(pid)

    def _op_key(op):
        # Stable-ish operator identity (best effort across simulator objects)
        if hasattr(op, "op_id"):
            return ("op_id", getattr(op, "op_id"))
        if hasattr(op, "operator_id"):
            return ("operator_id", getattr(op, "operator_id"))
        if hasattr(op, "name"):
            return ("name", getattr(op, "name"))
        return ("py_id", id(op))

    def _pool_order_for_prio(prio):
        # If multiple pools exist, keep pool 0 biased towards latency-sensitive work.
        n = s.executor.num_pools
        if n <= 1:
            return list(range(n))
        if prio in (Priority.QUERY, Priority.INTERACTIVE):
            return [0] + [i for i in range(1, n)]
        # Batch prefers non-zero pools first
        return [i for i in range(1, n)] + [0]

    def _pick_cpu_frac(prio):
        if prio == Priority.QUERY:
            return s.query_cpu_frac
        if prio == Priority.INTERACTIVE:
            return s.interactive_cpu_frac
        return s.batch_cpu_frac

    def _pick_ram_frac(prio):
        if prio == Priority.QUERY:
            return s.query_ram_frac
        if prio == Priority.INTERACTIVE:
            return s.interactive_ram_frac
        return s.batch_ram_frac

    def _size_for_op(p, op, pool_id):
        pool = s.executor.pools[pool_id]
        avail_cpu = int(pool.avail_cpu_pool)
        avail_ram = int(pool.avail_ram_pool)
        if avail_cpu <= 0 or avail_ram <= 0:
            return 0, 0

        prio = p.priority
        cpu_target = max(s.min_cpu, int(avail_cpu * _pick_cpu_frac(prio)))
        ram_target = max(s.min_ram, int(avail_ram * _pick_ram_frac(prio)))

        # Apply learned hints (prefer pipeline-specific hints, fallback to global op hints).
        ok = _op_key(op)
        pid = p.pipeline_id

        hinted_ram = s.op_ram_hint.get((pid, ok), s.op_ram_hint.get((None, ok), 0))
        hinted_cpu = s.op_cpu_hint.get((pid, ok), s.op_cpu_hint.get((None, ok), 0))

        if hinted_ram:
            ram_target = max(ram_target, int(hinted_ram))
        if hinted_cpu:
            cpu_target = max(cpu_target, int(hinted_cpu))

        # Bound by availability and pool maxima
        cpu = max(s.min_cpu, min(int(cpu_target), int(pool.max_cpu_pool), avail_cpu))
        ram = max(s.min_ram, min(int(ram_target), int(pool.max_ram_pool), avail_ram))
        return cpu, ram

    # ---------------- Ingest ----------------
    for p in pipelines:
        _enqueue_pipeline(p)

    if not pipelines and not results:
        return [], []

    # ---------------- Learn from failures ----------------
    for r in results:
        if not (hasattr(r, "failed") and r.failed()):
            continue

        ops = getattr(r, "ops", []) or []
        err = str(getattr(r, "error", "") or "")
        oom_like = ("oom" in err.lower()) or ("out of memory" in err.lower())

        pid = getattr(r, "pipeline_id", None)
        prev_ram = int(getattr(r, "ram", 0) or 0)
        prev_cpu = int(getattr(r, "cpu", 0) or 0)

        for op in ops:
            ok = _op_key(op)

            # Use pipeline-specific key when possible; always also update global fallback.
            keys = []
            if pid is not None:
                keys.append((pid, ok))
            keys.append((None, ok))

            for k in keys:
                cur_ram = int(s.op_ram_hint.get(k, 0) or 0)
                cur_cpu = int(s.op_cpu_hint.get(k, 0) or 0)

                if oom_like:
                    # Exponential RAM backoff is the most important fix.
                    base = max(cur_ram, prev_ram, s.min_ram)
                    s.op_ram_hint[k] = int(base * 2)
                    # CPU doesn't help OOM; keep at least what we had.
                    s.op_cpu_hint[k] = max(cur_cpu, prev_cpu, s.min_cpu)
                else:
                    # Generic failure: small bump both ways (conservative).
                    s.op_ram_hint[k] = max(cur_ram, prev_ram, s.min_ram) + 1
                    s.op_cpu_hint[k] = max(cur_cpu, prev_cpu, s.min_cpu)

    # ---------------- Dispatch ----------------
    suspensions = []
    assignments = []

    # We assign at most one container per pool per tick (simple, stable behavior).
    # Within a pool, always pick QUERY > INTERACTIVE > BATCH.
    # For each queue we do a bounded scan (rotate items) to find a runnable op.
    queues_in_priority_order = (s.q_query, s.q_interactive, s.q_batch)

    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        if int(pool.avail_cpu_pool) <= 0 or int(pool.avail_ram_pool) <= 0:
            continue

        chosen_pipeline = None
        chosen_ops = None

        # For this pool, try higher priority work first.
        for q in queues_in_priority_order:
            # Rotate through queue once to find something runnable.
            for _ in range(len(q)):
                p = q.popleft()

                st = p.runtime_status()
                if st.is_pipeline_successful():
                    _drop_pipeline(p)
                    continue

                # Only schedule ops whose parents are complete; allow retry of FAILED ops.
                ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
                if not ops:
                    q.append(p)
                    continue

                # Pool preference: if multiple pools exist, bias pool 0 away from batch when possible.
                if s.executor.num_pools > 1 and pool_id == 0 and p.priority == Priority.BATCH_PIPELINE:
                    # If there are other pools with resources, keep batch circulating.
                    # (We can't see other pools' future, but this small bias helps weighted latency.)
                    q.append(p)
                    continue

                chosen_pipeline = p
                chosen_ops = ops[:1]
                break

            if chosen_pipeline is not None:
                break

        if chosen_pipeline is None:
            continue

        cpu, ram = _size_for_op(chosen_pipeline, chosen_ops[0], pool_id)
        if cpu <= 0 or ram <= 0:
            # Can't place now; requeue and move on.
            _queue_for_priority(chosen_pipeline.priority).append(chosen_pipeline)
            continue

        assignments.append(
            Assignment(
                ops=chosen_ops,
                cpu=cpu,
                ram=ram,
                priority=chosen_pipeline.priority,
                pool_id=pool_id,
                pipeline_id=chosen_pipeline.pipeline_id,
            )
        )

        # Requeue pipeline for subsequent ops.
        _queue_for_priority(chosen_pipeline.priority).append(chosen_pipeline)

    return suspensions, assignments
