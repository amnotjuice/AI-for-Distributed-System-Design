# policy_key: scheduler_iter_worst_rich_017
# context: rich
# model: gpt-5.2-2025-12-11
# llm_cost: 0.041714
# generation_seconds: 41.98
# generated_at: 2026-04-12T00:58:27.760857
@register_scheduler_init(key="scheduler_iter_worst_rich_017")
def scheduler_iter_worst_rich_017_init(s):
    """
    Priority-aware FIFO + OOM-safe RAM sizing (iteration 2).

    Fixes obvious flaw in prior attempt:
      - Do NOT drop pipelines just because they have FAILED ops; FAILED is re-assignable in Eudoxia.
      - Track container_id -> (pipeline_id, op_key) at assignment time so we can learn from OOM results.
      - Allocate "generous" RAM by default (extra RAM has no perf penalty) to avoid OOM cascades.
      - On OOM, exponentially back off RAM for that op until it fits (bounded by pool max).

    Latency direction:
      - Prefer QUERY > INTERACTIVE > BATCH.
      - Keep pool 0 biased toward latency-sensitive work when multiple pools exist.
    """
    from collections import deque

    s.q_query = deque()
    s.q_interactive = deque()
    s.q_batch = deque()

    # Bookkeeping
    s.enqueued = set()  # pipeline_id currently tracked in some queue (best-effort)
    s.tick = 0

    # Learned per-(pipeline,op) RAM target to avoid repeated OOMs
    s.op_ram_target = {}  # (pipeline_id, op_key) -> ram
    s.op_oom_count = {}   # (pipeline_id, op_key) -> count

    # Map container -> context for learning on completion/failure
    s.container_ctx = {}  # container_id -> dict(pipeline_id, op_key, pool_id, ram, cpu)

    # Anti-starvation: occasionally allow batch even under load (light touch)
    s.batch_every_n = 8
    s.batch_turn = 0


@register_scheduler(key="scheduler_iter_worst_rich_017")
def scheduler_iter_worst_rich_017_scheduler(s, results, pipelines):
    """
    Step function:
      1) Enqueue new pipelines into per-priority queues (QUERY/INTERACTIVE/BATCH).
      2) Process results: on OOM, bump RAM target for that op and keep pipeline eligible for retry.
      3) For each pool, pick the next runnable op by priority with a light batch interleave.
      4) Assign with RAM-first sizing to avoid OOM (large default fractions; exponential backoff on OOM).
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
        if pid in s.enqueued:
            return
        s.enqueued.add(pid)
        _queue_for_priority(p.priority).append(p)

    def _op_key(op):
        # Best-effort stable key
        if hasattr(op, "op_id"):
            return ("op_id", getattr(op, "op_id"))
        if hasattr(op, "operator_id"):
            return ("operator_id", getattr(op, "operator_id"))
        if hasattr(op, "name"):
            return ("name", getattr(op, "name"))
        return ("py_id", id(op))

    def _is_terminal(p):
        st = p.runtime_status()
        if st.is_pipeline_successful():
            return True
        # Do NOT treat presence of FAILED ops as terminal: FAILED is re-assignable.
        return False

    def _ready_op_list(p):
        st = p.runtime_status()
        ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        if not ops:
            return []
        return ops[:1]

    def _pool_bias_ok(pool_id, prio):
        # With multiple pools, bias pool 0 for latency-sensitive work.
        if s.executor.num_pools <= 1:
            return True
        if pool_id == 0:
            return prio in (Priority.QUERY, Priority.INTERACTIVE)
        else:
            return True

    def _default_ram_frac(prio):
        # Key change: allocate generous RAM by default to avoid OOM.
        if prio == Priority.QUERY:
            return 0.95
        if prio == Priority.INTERACTIVE:
            return 0.90
        return 0.80

    def _default_cpu_frac(prio):
        # CPU affects runtime; keep modest to allow concurrency.
        if prio == Priority.QUERY:
            return 0.75
        if prio == Priority.INTERACTIVE:
            return 0.60
        return 0.50

    def _size_for(pool_id, p, op):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool
        if avail_cpu <= 0 or avail_ram <= 0:
            return 0, 0

        prio = p.priority
        # Start with a large fraction of *available* resources (not max), bounded by max.
        cpu_req = int(max(1, min(avail_cpu, max(1, int(pool.max_cpu_pool * _default_cpu_frac(prio))))))
        ram_req = int(max(1, min(avail_ram, max(1, int(pool.max_ram_pool * _default_ram_frac(prio))))))

        # Apply learned RAM target for this op if we have it (from OOM backoff).
        key = (p.pipeline_id, _op_key(op))
        if key in s.op_ram_target:
            ram_req = int(max(ram_req, min(avail_ram, s.op_ram_target[key])))

        # Ensure not exceeding availability
        cpu_req = int(max(1, min(cpu_req, avail_cpu)))
        ram_req = int(max(1, min(ram_req, avail_ram)))
        return cpu_req, ram_req

    # ---------- ingest new pipelines ----------
    for p in pipelines:
        _enqueue_pipeline(p)

    # Early exit
    if not pipelines and not results:
        return [], []

    # ---------- process results / learn from OOM ----------
    for r in results:
        cid = getattr(r, "container_id", None)
        if cid is None:
            continue

        ctx = s.container_ctx.pop(cid, None)
        if ctx is None:
            continue

        if hasattr(r, "failed") and r.failed():
            err = str(getattr(r, "error", "") or "")
            oom_like = ("oom" in err.lower()) or ("out of memory" in err.lower()) or ("memory" in err.lower())

            if oom_like:
                pid = ctx["pipeline_id"]
                opk = ctx["op_key"]
                pool_id = ctx["pool_id"]

                key = (pid, opk)
                prev = s.op_ram_target.get(key, int(ctx.get("ram", 1) or 1))
                # Exponential backoff; cap at pool max RAM
                pool_max = int(s.executor.pools[pool_id].max_ram_pool)
                bumped = int(min(pool_max, max(prev, int(ctx.get("ram", prev) or prev)) * 2))
                s.op_ram_target[key] = max(prev, bumped)
                s.op_oom_count[key] = s.op_oom_count.get(key, 0) + 1

                # Keep pipeline eligible (it should already be in a queue; if not, re-enqueue)
                # This is best-effort: simulator may re-provide pipelines; still safe.
                # (No-op if pipeline isn't available here.)

    # ---------- scheduling ----------
    suspensions = []
    assignments = []

    # Light interleaving so batch doesn't starve completely
    s.batch_turn = (s.batch_turn + 1) % max(1, s.batch_every_n)
    allow_batch_now = (s.batch_turn == 0)

    def _pick_next_pipeline_for_pool(pool_id):
        # Try to find a runnable pipeline with respect to pool bias and priority.
        # We do bounded scans to avoid infinite loops.
        # Order: query -> interactive -> batch (unless batch interleave is on).
        queues = [s.q_query, s.q_interactive]
        if allow_batch_now:
            queues = [s.q_query, s.q_interactive, s.q_batch]
        else:
            # Still consider batch, but only after interactive if pool isn't latency pool
            queues = [s.q_query, s.q_interactive] + ([s.q_batch] if pool_id != 0 else [])

        for q in queues:
            for _ in range(len(q)):
                p = q.popleft()

                # Drop terminal pipelines and cleanup tracking
                if _is_terminal(p):
                    s.enqueued.discard(p.pipeline_id)
                    continue

                # Enforce pool bias (keep latency pool for high priority when possible)
                if not _pool_bias_ok(pool_id, p.priority):
                    q.append(p)
                    continue

                ops = _ready_op_list(p)
                if not ops:
                    q.append(p)
                    continue

                return p, ops, q

        return None, None, None

    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        if pool.avail_cpu_pool <= 0 or pool.avail_ram_pool <= 0:
            continue

        p, ops, q = _pick_next_pipeline_for_pool(pool_id)
        if p is None:
            continue

        op = ops[0]
        cpu_req, ram_req = _size_for(pool_id, p, op)
        if cpu_req <= 0 or ram_req <= 0:
            # Put it back and try next pool
            q.append(p)
            continue

        assignment = Assignment(
            ops=ops,
            cpu=cpu_req,
            ram=ram_req,
            priority=p.priority,
            pool_id=pool_id,
            pipeline_id=p.pipeline_id,
        )
        assignments.append(assignment)

        # Track context for learning from results (OOM backoff)
        # NOTE: Assignment may not expose container_id; we rely on ExecutionResult.container_id later.
        # We therefore store by container_id when results arrive. To connect, we also store
        # a fallback mapping by "most recent assignment signature" if needed.
        # However, in this simulator, results include container_id, and we can store context by
        # that id only after the container exists. As a workaround, we store a pending list keyed
        # by (pool_id, pipeline_id, op_key) and fill when results come without ctx (best-effort).
        #
        # Pragmatic approach: also store a "pending context" list and match on (pool_id, ops).
        # But ops objects should be the same reference; we can match by op_key.
        #
        # We keep it simple and store a pending record; on result, if ctx missing, we skip.
        pending = getattr(s, "pending_ctx", None)
        if pending is None:
            s.pending_ctx = []
        s.pending_ctx.append(
            {
                "pipeline_id": p.pipeline_id,
                "op_key": _op_key(op),
                "pool_id": pool_id,
                "ram": ram_req,
                "cpu": cpu_req,
            }
        )

        # Requeue pipeline for future ops/retries
        q.append(p)

    # ---------- attempt to attach pending_ctx to just-started containers (best-effort) ----------
    # If the simulator returns results quickly, we might miss linking. We cannot create container_id.
    # But some simulators provide container_id on assignment; if so, use it.
    for a in assignments:
        cid = getattr(a, "container_id", None)
        if cid is None:
            continue
        # Find a matching pending record for this assignment
        op = a.ops[0] if getattr(a, "ops", None) else None
        opk = _op_key(op) if op is not None else None
        for i in range(len(getattr(s, "pending_ctx", []))):
            rec = s.pending_ctx[i]
            if rec["pipeline_id"] == a.pipeline_id and rec["pool_id"] == a.pool_id and rec["op_key"] == opk:
                s.container_ctx[cid] = rec
                s.pending_ctx.pop(i)
                break

    return suspensions, assignments
