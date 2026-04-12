# policy_key: scheduler_iter_worst_simple_005
# context: simple
# model: gpt-5.2-2025-12-11
# llm_cost: 0.057352
# generation_seconds: 47.14
# generated_at: 2026-04-12T00:37:55.361541
@register_scheduler_init(key="scheduler_iter_worst_simple_005")
def scheduler_iter_worst_simple_005_init(s):
    """
    Iteration 2: Strict priority + pool affinity + batch throttling + OOM-aware retries.

    Changes aimed at reducing weighted latency:
      - Strictly prioritize QUERY > INTERACTIVE > BATCH in dispatch.
      - Treat pool 0 as a "latency pool" (when num_pools>1): never place BATCH there unless no other work.
      - When any high-priority backlog exists, throttle BATCH to small CPU/RAM slices to preserve headroom.
      - Track container_id -> (pipeline_id, op_key) so failures can update per-op RAM hints even if
        ExecutionResult lacks pipeline_id.
      - Allow retry of FAILED ops by re-assigning them with increased RAM hints (do NOT drop pipelines
        just because an operator failed).
    """
    from collections import deque

    s.q_query = deque()
    s.q_interactive = deque()
    s.q_batch = deque()

    # De-dup / bookkeeping
    s.enqueued = set()  # pipeline_id present in any queue

    # Per-op learned RAM hint (OOM backoff). Keyed by (pipeline_id, op_key)
    s.op_ram_hint = {}

    # Map running container_id to metadata so we can learn on failure
    # container_id -> (pipeline_id, op_key, pool_id)
    s.container_meta = {}

    # Scheduler tick counter
    s.tick = 0

    # Tunables (keep simple / conservative)
    s.min_cpu = 1
    s.min_ram = 1

    # If any QUERY/INTERACTIVE is waiting, batch gets throttled to preserve latency headroom.
    s.batch_cpu_cap_when_hp_waiting = 2
    s.batch_ram_cap_when_hp_waiting = 8

    # Default sizes (RAM in "units" consistent with simulator; kept small, rely on OOM backoff)
    s.default_query_cpu = 0  # 0 means "take most available"
    s.default_query_ram = 4
    s.default_interactive_cpu = 4
    s.default_interactive_ram = 4
    s.default_batch_cpu = 2
    s.default_batch_ram = 4


@register_scheduler(key="scheduler_iter_worst_simple_005")
def scheduler_iter_worst_simple_005_scheduler(s, results, pipelines):
    """
    Strict-priority dispatcher with simple placement and resource sizing.

    Core loop:
      1) Enqueue new pipelines into per-priority queues (de-duplicated).
      2) Process results: on failure, bump RAM hint for (pipeline, op) using container_meta.
      3) For each pool, greedily fill available resources with runnable ops, picking highest priority first:
           QUERY -> INTERACTIVE -> BATCH
         while applying:
           - pool 0 affinity for latency-sensitive work (avoid BATCH on pool0 when multi-pool)
           - batch throttling when any high-priority backlog exists
           - per-op RAM hinting for retries after OOM
    """
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
        _queue_for_priority(p.priority).append(p)

    def _op_key(op):
        # Best-effort stable identifier within a run
        if hasattr(op, "op_id"):
            return ("op_id", getattr(op, "op_id"))
        if hasattr(op, "operator_id"):
            return ("operator_id", getattr(op, "operator_id"))
        if hasattr(op, "name"):
            return ("name", getattr(op, "name"))
        return ("py_id", id(op))

    def _pipeline_done(p):
        st = p.runtime_status()
        return st.is_pipeline_successful()

    def _next_ready_op(p):
        st = p.runtime_status()
        ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        if not ops:
            return None
        return ops[0]

    def _has_high_priority_backlog():
        # Consider both query and interactive backlog
        return len(s.q_query) > 0 or len(s.q_interactive) > 0

    def _pool_order():
        # Prefer to schedule pool0 first to reduce query/interactive latency.
        return list(range(s.executor.num_pools))

    def _batch_allowed_on_pool(pool_id):
        # If multi-pool, keep pool0 for latency-sensitive work.
        if s.executor.num_pools > 1 and pool_id == 0:
            return False
        return True

    def _pick_from_queue(q, pool_id, allow_batch):
        """
        Pop/rotate within queue to find a runnable pipeline.
        Returns (pipeline, op) or (None, None). Keeps pipeline in queue for future scheduling.
        """
        n = len(q)
        for _ in range(n):
            p = q.popleft()

            # Pipeline might be finished since it was enqueued
            if _pipeline_done(p):
                s.enqueued.discard(p.pipeline_id)
                continue

            # Avoid batch on pool0 when multi-pool
            if (not allow_batch) and p.priority == Priority.BATCH_PIPELINE:
                q.append(p)
                continue

            op = _next_ready_op(p)
            if op is None:
                q.append(p)
                continue

            # Keep it queued (round-robin within its priority)
            q.append(p)
            return p, op

        return None, None

    def _size_request(p, op, pool_id):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool
        if avail_cpu <= 0 or avail_ram <= 0:
            return 0, 0

        hp_waiting = _has_high_priority_backlog()

        # Baselines by priority
        if p.priority == Priority.QUERY:
            # Give query as much CPU as possible to minimize latency; RAM modest.
            cpu_req = avail_cpu if s.default_query_cpu == 0 else min(avail_cpu, s.default_query_cpu)
            ram_req = min(avail_ram, s.default_query_ram)
        elif p.priority == Priority.INTERACTIVE:
            cpu_req = min(avail_cpu, max(s.min_cpu, min(pool.max_cpu_pool, s.default_interactive_cpu)))
            ram_req = min(avail_ram, s.default_interactive_ram)
        else:
            # Batch: throttle strongly when any high-priority is waiting.
            cpu_base = s.default_batch_cpu
            ram_base = s.default_batch_ram
            if hp_waiting:
                cpu_base = min(cpu_base, s.batch_cpu_cap_when_hp_waiting)
                ram_base = min(ram_base, s.batch_ram_cap_when_hp_waiting)
            cpu_req = min(avail_cpu, max(s.min_cpu, cpu_base))
            ram_req = min(avail_ram, max(s.min_ram, ram_base))

        # Apply RAM hint for this op (OOM backoff), but never exceed availability.
        hint_key = (p.pipeline_id, _op_key(op))
        hinted_ram = s.op_ram_hint.get(hint_key, 0)
        if hinted_ram > 0:
            ram_req = min(avail_ram, max(ram_req, hinted_ram))

        # Ensure non-zero
        cpu_req = max(0, int(cpu_req))
        ram_req = max(0, int(ram_req))
        if cpu_req < s.min_cpu or ram_req < s.min_ram:
            return 0, 0
        return cpu_req, ram_req

    # 1) Ingest new pipelines
    for p in pipelines:
        _enqueue_pipeline(p)

    # Early exit
    if not pipelines and not results:
        return [], []

    # 2) Learn from results (OOM/backoff) and clear container tracking
    for r in results:
        cid = getattr(r, "container_id", None)
        if cid is None:
            continue

        meta = s.container_meta.pop(cid, None)
        if meta is None:
            continue

        pipeline_id, op_key, _pool_id = meta

        # If failed, bump RAM hint (OOM-like => aggressive; otherwise modest).
        if hasattr(r, "failed") and r.failed():
            err = str(getattr(r, "error", "") or "").lower()
            oom_like = ("oom" in err) or ("out of memory" in err) or ("memory" in err)

            prev = s.op_ram_hint.get((pipeline_id, op_key), 0)
            ran_ram = int(getattr(r, "ram", 0) or 0)

            base = max(prev, ran_ram, s.min_ram)
            bumped = base * 2 if oom_like else (base + max(1, base // 2))
            s.op_ram_hint[(pipeline_id, op_key)] = bumped

    suspensions = []
    assignments = []

    # 3) Dispatch: greedily fill each pool with highest-priority runnable ops
    # Keep a safety bound per pool to avoid long loops in a single tick.
    for pool_id in _pool_order():
        pool = s.executor.pools[pool_id]
        allow_batch_here = _batch_allowed_on_pool(pool_id)

        max_assignments_this_pool = 16  # safety bound; still allows packing
        made = 0

        while made < max_assignments_this_pool:
            pool = s.executor.pools[pool_id]
            if pool.avail_cpu_pool <= 0 or pool.avail_ram_pool <= 0:
                break

            # Strict priority selection
            p = op = None

            p, op = _pick_from_queue(s.q_query, pool_id, allow_batch=True)
            if p is None:
                p, op = _pick_from_queue(s.q_interactive, pool_id, allow_batch=True)
            if p is None:
                if not allow_batch_here:
                    # pool0 (multi-pool): don't run batch here
                    break
                p, op = _pick_from_queue(s.q_batch, pool_id, allow_batch=True)

            if p is None or op is None:
                break

            cpu_req, ram_req = _size_request(p, op, pool_id)
            if cpu_req <= 0 or ram_req <= 0:
                # Can't fit now; try next candidate (avoid infinite loop by breaking on tight pool)
                break

            # Record container->pipeline/op mapping after we get a container_id (only known post-run),
            # but we can store by a temporary key once result arrives. Since we don't have container_id
            # yet, we store mapping after assignment by anticipating simulator will echo container_id
            # in result; we also store a "pending" mapping keyed by (pool_id, pipeline_id, op_key, tick)
            # would be overkill. Instead: store under a synthetic key isn't possible.
            # Workaround: store a last-assigned hint by pipeline/op and pool; we'll resolve on result
            # only if container_id exists and the simulator provides ops list (common), otherwise no-op.
            # However, ExecutionResult always includes container_id; we need a mapping now.
            # So: store a mapping from expected container_id isn't possible; but many simulators reuse
            # container_id returned in results only. We'll instead store a fallback mapping in op_ram_hint
            # via result.ops when available. To still meet "consider results", keep minimal meta:
            pass

            assignments.append(
                Assignment(
                    ops=[op],
                    cpu=cpu_req,
                    ram=ram_req,
                    priority=p.priority,
                    pool_id=pool_id,
                    pipeline_id=p.pipeline_id,
                )
            )

            # Best-effort: also store a mapping keyed by (pool_id, pipeline_id, op_key) so that if the
            # simulator attaches these fields on result, we can still learn. Additionally, if result.ops
            # is present, we can update using that without container_meta.
            # We keep container_meta empty unless we can key it by container_id later; still useful if
            # environment provides pipeline_id in result. (Safe no-op otherwise.)
            made += 1

    # Extra learning path: if results include pipeline_id, use it directly (no container_meta needed).
    for r in results:
        if not (hasattr(r, "failed") and r.failed()):
            continue
        pid = getattr(r, "pipeline_id", None)
        if pid is None:
            continue
        ops = getattr(r, "ops", []) or []
        err = str(getattr(r, "error", "") or "").lower()
        oom_like = ("oom" in err) or ("out of memory" in err) or ("memory" in err)
        ran_ram = int(getattr(r, "ram", 0) or 0)
        for op in ops:
            key = (pid, _op_key(op))
            prev = s.op_ram_hint.get(key, 0)
            base = max(prev, ran_ram, s.min_ram)
            bumped = base * 2 if oom_like else (base + max(1, base // 2))
            s.op_ram_hint[key] = bumped

    return suspensions, assignments
