# policy_key: scheduler_iter_worst_rich_005
# context: rich
# model: gpt-5.2-2025-12-11
# llm_cost: 0.049568
# generation_seconds: 37.07
# generated_at: 2026-04-12T00:51:28.833821
@register_scheduler_init(key="scheduler_iter_worst_rich_005")
def scheduler_iter_worst_rich_005_init(s):
    """
    Priority-aware FIFO with "OOM-avoidance first" sizing and safe retries.

    Key fixes vs prior attempt:
      - Never drop pipelines just because an operator FAILED; FAILED ops are retryable.
      - Size RAM aggressively on first attempt (avoid universal OOMs), then learn per-op RAM hints.
      - Track container->(pipeline, op) metadata so we can learn from failures even if results
        don't include pipeline_id/op identity.
      - Keep pool 0 biased toward QUERY/INTERACTIVE when multiple pools exist.

    This is intentionally a small, robust improvement step: it aims to restore completion
    (and therefore meaningful latency) before adding more complex packing/preemption.
    """
    from collections import deque

    # Simple per-priority FIFO queues
    s.q_query = deque()
    s.q_interactive = deque()
    s.q_batch = deque()

    # bookkeeping
    s.enqueued = set()  # pipeline_id
    s.tick = 0

    # Learning: per (pipeline_id, op_key) RAM/CPU hints
    s.op_ram_hint = {}  # (pid, op_key) -> ram
    s.op_cpu_hint = {}  # (pid, op_key) -> cpu

    # Map container_id -> metadata to learn from results robustly
    s.container_meta = {}  # container_id -> dict(pid, op_key, pool_id, cpu, ram, priority)

    # Default first-try sizing fractions (high to avoid OOM storm)
    s.default_ram_frac = {
        Priority.QUERY: 0.90,
        Priority.INTERACTIVE: 0.85,
        Priority.BATCH_PIPELINE: 0.80,
    }
    s.default_cpu_frac = {
        Priority.QUERY: 0.90,
        Priority.INTERACTIVE: 0.75,
        Priority.BATCH_PIPELINE: 0.60,
    }

    # Minimum allocs (avoid zeros)
    s.min_cpu = 1
    s.min_ram = 1

    # How aggressively to grow RAM after OOM
    s.oom_ram_multiplier = 2.0

    # If multiple pools, treat pool 0 as latency pool
    s.latency_pool_id = 0


@register_scheduler(key="scheduler_iter_worst_rich_005")
def scheduler_iter_worst_rich_005_scheduler(s, results, pipelines):
    """
    Step algorithm:
      1) Enqueue new pipelines by priority.
      2) Learn from results:
           - on OOM-like failures: bump per-op RAM hint (x2) capped by pool max.
      3) For each pool:
           - pick highest-priority runnable pipeline (FIFO within class),
           - assign exactly one ready op per pool per tick (conservative but stable),
           - request RAM/CPU based on learned hint else a large fraction of pool max
             (bounded by current availability).
    """
    s.tick += 1

    # ---------------- Helpers ----------------
    def _prio_queue(prio):
        if prio == Priority.QUERY:
            return s.q_query
        if prio == Priority.INTERACTIVE:
            return s.q_interactive
        return s.q_batch

    def _enqueue(p):
        pid = p.pipeline_id
        if pid in s.enqueued:
            return
        s.enqueued.add(pid)
        _prio_queue(p.priority).append(p)

    def _op_key(op):
        # Try stable identity; fall back to python object id.
        if hasattr(op, "op_id"):
            return ("op_id", getattr(op, "op_id"))
        if hasattr(op, "operator_id"):
            return ("operator_id", getattr(op, "operator_id"))
        if hasattr(op, "name"):
            return ("name", getattr(op, "name"))
        return ("py_id", id(op))

    def _oom_like(err_str):
        e = (err_str or "")
        e = e.lower()
        return ("oom" in e) or ("out of memory" in e) or ("memory" in e)

    def _get_ready_op(p):
        st = p.runtime_status()
        if st.is_pipeline_successful():
            return None
        # Retryable: FAILED ops are in ASSIGNABLE_STATES, so don't drop the pipeline.
        ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        if not ops:
            return None
        return ops[0]

    def _pool_order_for_pipeline(p):
        # If we have >1 pool, bias pool 0 for QUERY/INTERACTIVE.
        if s.executor.num_pools <= 1:
            return list(range(s.executor.num_pools))
        if p.priority in (Priority.QUERY, Priority.INTERACTIVE):
            return [s.latency_pool_id] + [i for i in range(s.executor.num_pools) if i != s.latency_pool_id]
        # Batch prefers non-latency pools
        return [i for i in range(s.executor.num_pools) if i != s.latency_pool_id] + [s.latency_pool_id]

    def _size_for(p, op, pool_id):
        pool = s.executor.pools[pool_id]

        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool
        if avail_cpu <= 0 or avail_ram <= 0:
            return 0, 0

        prio = p.priority
        ram_frac = s.default_ram_frac.get(prio, 0.85)
        cpu_frac = s.default_cpu_frac.get(prio, 0.70)

        # Large, safe first-try requests to avoid universal OOM.
        # Use pool.max_* as target, then clamp to current availability.
        default_ram = max(s.min_ram, int(pool.max_ram_pool * ram_frac))
        default_cpu = max(s.min_cpu, int(pool.max_cpu_pool * cpu_frac))

        key = (p.pipeline_id, _op_key(op))
        hinted_ram = s.op_ram_hint.get(key, default_ram)
        hinted_cpu = s.op_cpu_hint.get(key, default_cpu)

        req_ram = max(default_ram, hinted_ram)
        req_cpu = max(default_cpu, hinted_cpu)

        # Clamp to feasible bounds.
        req_ram = max(s.min_ram, min(req_ram, pool.max_ram_pool, avail_ram))
        req_cpu = max(s.min_cpu, min(req_cpu, pool.max_cpu_pool, avail_cpu))

        return req_cpu, req_ram

    # ---------------- Ingest ----------------
    for p in pipelines:
        _enqueue(p)

    if not pipelines and not results:
        return [], []

    # ---------------- Learn from results ----------------
    for r in results:
        if not (hasattr(r, "failed") and r.failed()):
            # Clean up container metadata for completed containers to prevent growth.
            cid = getattr(r, "container_id", None)
            if cid is not None and cid in s.container_meta:
                del s.container_meta[cid]
            continue

        cid = getattr(r, "container_id", None)
        meta = s.container_meta.get(cid, None)

        # Identify (pid, op_key) for learning: prefer our meta; fallback to result fields if present.
        pid = None
        opk = None
        pool_id = getattr(r, "pool_id", None)

        if meta is not None:
            pid = meta.get("pid", None)
            opk = meta.get("op_key", None)
            if pool_id is None:
                pool_id = meta.get("pool_id", None)

        # If we can't key it, we can't learn precisely.
        if pid is None or opk is None:
            continue

        # Cap bumps by pool max RAM if we can determine pool.
        pool_max_ram = None
        if pool_id is not None and 0 <= pool_id < s.executor.num_pools:
            pool_max_ram = s.executor.pools[pool_id].max_ram_pool

        err = str(getattr(r, "error", "") or "")
        is_oom = _oom_like(err)

        key = (pid, opk)
        prev_hint = s.op_ram_hint.get(key, None)

        # Base off the RAM we just tried (prefer meta), else result.ram, else existing hint.
        tried_ram = None
        if meta is not None:
            tried_ram = meta.get("ram", None)
        if tried_ram is None:
            tried_ram = getattr(r, "ram", None)
        if tried_ram is None:
            tried_ram = prev_hint if prev_hint is not None else s.min_ram

        tried_ram = max(s.min_ram, int(tried_ram))

        if is_oom:
            bumped = int(tried_ram * s.oom_ram_multiplier)
        else:
            # Non-OOM failures: small bump; avoids runaway RAM growth for logic errors.
            bumped = tried_ram + max(1, int(tried_ram * 0.10))

        if pool_max_ram is not None:
            bumped = min(bumped, int(pool_max_ram))

        if prev_hint is None:
            s.op_ram_hint[key] = bumped
        else:
            s.op_ram_hint[key] = max(int(prev_hint), bumped)

        # CPU hint: keep what we used; don't increase CPU on OOM.
        prev_cpu = s.op_cpu_hint.get(key, None)
        tried_cpu = None
        if meta is not None:
            tried_cpu = meta.get("cpu", None)
        if tried_cpu is None:
            tried_cpu = getattr(r, "cpu", None)
        if tried_cpu is None:
            tried_cpu = prev_cpu if prev_cpu is not None else s.min_cpu
        tried_cpu = max(s.min_cpu, int(tried_cpu))
        if prev_cpu is None:
            s.op_cpu_hint[key] = tried_cpu
        else:
            s.op_cpu_hint[key] = max(int(prev_cpu), tried_cpu)

        # Keep container meta until completion to continue learning if it fails repeatedly.

    # ---------------- Dispatch ----------------
    suspensions = []
    assignments = []

    # Make at most one assignment per pool per tick (stable; avoids overcommitting with unknown RAM mins).
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        if pool.avail_cpu_pool <= 0 or pool.avail_ram_pool <= 0:
            continue

        chosen = None
        chosen_op = None

        # Try highest priority first; rotate queue to preserve FIFO among runnable pipelines.
        for q in (s.q_query, s.q_interactive, s.q_batch):
            n = len(q)
            for _ in range(n):
                p = q.popleft()
                st = p.runtime_status()
                if st.is_pipeline_successful():
                    # Done: stop tracking.
                    s.enqueued.discard(p.pipeline_id)
                    continue

                # If multiple pools, skip pool mismatch for latency protection:
                # - keep batch off latency pool if there are other pools with capacity.
                if s.executor.num_pools > 1 and pool_id == s.latency_pool_id and p.priority == Priority.BATCH_PIPELINE:
                    # Put it back and look for a better fit.
                    q.append(p)
                    continue

                op = _get_ready_op(p)
                if op is None:
                    q.append(p)
                    continue

                chosen = p
                chosen_op = op
                # Keep pipeline in queue for future ops.
                q.append(p)
                break

            if chosen is not None:
                break

        if chosen is None:
            continue

        req_cpu, req_ram = _size_for(chosen, chosen_op, pool_id)
        if req_cpu <= 0 or req_ram <= 0:
            continue

        assignment = Assignment(
            ops=[chosen_op],
            cpu=req_cpu,
            ram=req_ram,
            priority=chosen.priority,
            pool_id=pool_id,
            pipeline_id=chosen.pipeline_id,
        )
        assignments.append(assignment)

        # Record metadata for learning from results
        # Note: container_id isn't known at assignment time in some systems; if Eudoxia fills it later,
        # this won't help. But in many simulators, ExecutionResult includes container_id we can map
        # by also storing last assignment per (pool, pipeline, op). To remain robust, we also store
        # a fallback mapping keyed by a synthetic token when container_id isn't available.
        #
        # Here we assume ExecutionResult.container_id corresponds to the container that ran this op.
        # We can only attach after the fact; so store a "pending" map keyed by (pool_id, pipeline_id, op_key).
        if not hasattr(s, "pending_meta"):
            s.pending_meta = {}
        tok = (pool_id, chosen.pipeline_id, _op_key(chosen_op))
        s.pending_meta[tok] = {
            "pid": chosen.pipeline_id,
            "op_key": _op_key(chosen_op),
            "pool_id": pool_id,
            "cpu": req_cpu,
            "ram": req_ram,
            "priority": chosen.priority,
        }

    # Second pass: if results contain container_id but we only have pending_meta, try to bind them.
    # This handles simulators that don't provide container_id at assignment time.
    for r in results:
        cid = getattr(r, "container_id", None)
        if cid is None or cid in s.container_meta:
            continue
        pid = getattr(r, "pipeline_id", None)
        pool_id = getattr(r, "pool_id", None)
        ops = getattr(r, "ops", None) or []
        if pid is None or pool_id is None or not ops:
            continue
        opk = _op_key(ops[0])
        tok = (pool_id, pid, opk)
        meta = getattr(s, "pending_meta", {}).pop(tok, None)
        if meta is not None:
            s.container_meta[cid] = meta

    return suspensions, assignments
