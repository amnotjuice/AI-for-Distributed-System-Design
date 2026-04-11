# policy_key: scheduler_est_002
# reasoning_effort: low
# exp: estimation
# model: gpt-5.2-2025-12-11
# llm_cost: 0.044189
# generation_seconds: 53.03
# generated_at: 2026-04-10T07:47:33.447397
@register_scheduler_init(key="scheduler_est_002")
def scheduler_est_002_init(s):
    """Priority-first, memory-aware scheduler with OOM backoff.

    Key ideas:
    - Strictly protect QUERY and INTERACTIVE latency (dominates objective) using priority-first selection.
    - Use op.estimate.mem_peak_gb as a noisy hint to avoid obviously infeasible placements (reduces OOM => avoids 720s penalties).
    - On operator failure, apply per-pipeline RAM backoff multiplier and retry (instead of dropping), improving completion rate.
    - Use light weighted round-robin across priorities to avoid batch starvation.
    """
    s.tick = 0

    # Per-priority FIFO queues of pipeline_ids
    s.q_query = []
    s.q_interactive = []
    s.q_batch = []

    # pipeline_id -> Pipeline (latest reference)
    s.pipelines_by_id = {}

    # pipeline_id -> metadata
    # {
    #   "ram_boost": float,  # multiplicative boost after failures
    #   "fail_count": int,
    #   "last_pool_id": Optional[int],
    # }
    s.meta = {}

    # Deficit-style counters to prevent starvation while still favoring high priority
    s.deficit = {"query": 0, "interactive": 0, "batch": 0}
    s.quantum = {"query": 3, "interactive": 2, "batch": 1}

    # Conservative estimator safety factor (noisy; avoid under-alloc more than over-alloc)
    s.est_safety = 1.25

    # Minimum RAM allocations (GB) to avoid trivially tiny containers
    s.min_ram_gb = 0.25

    # CPU shares per priority when carving up a pool's remaining CPU
    s.cpu_share = {
        "query": 0.75,
        "interactive": 0.50,
        "batch": 0.30,
    }


@register_scheduler(key="scheduler_est_002")
def scheduler_est_002_scheduler(s, results, pipelines):
    def _pkey(priority):
        # Map Priority enum to our internal key strings (robust to unknowns)
        try:
            if priority == Priority.QUERY:
                return "query"
            if priority == Priority.INTERACTIVE:
                return "interactive"
            if priority == Priority.BATCH_PIPELINE:
                return "batch"
        except Exception:
            pass
        return "batch"

    def _enqueue(p, front=False):
        pid = p.pipeline_id
        s.pipelines_by_id[pid] = p
        if pid not in s.meta:
            s.meta[pid] = {"ram_boost": 1.0, "fail_count": 0, "last_pool_id": None}
        key = _pkey(p.priority)
        q = s.q_batch
        if key == "query":
            q = s.q_query
        elif key == "interactive":
            q = s.q_interactive
        if pid in q:
            return
        if front:
            q.insert(0, pid)
        else:
            q.append(pid)

    def _dequeue_pid(pid):
        # Remove pid from all queues
        for q in (s.q_query, s.q_interactive, s.q_batch):
            try:
                q.remove(pid)
            except ValueError:
                pass

    def _pipeline_done_or_failed(p):
        st = p.runtime_status()
        if st.is_pipeline_successful():
            return True
        # Consider any FAILED op as pipeline failure signal (we will retry by re-enqueueing)
        # but do not "drop" from system permanently here.
        return False

    def _get_next_assignable_op(p):
        st = p.runtime_status()
        # Only assign ops whose parents are complete
        ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        if not ops:
            return None
        return ops[0]

    def _est_mem_gb(op):
        # Return estimated peak memory (GB) or None
        try:
            est = op.estimate.mem_peak_gb
            if est is None:
                return None
            # Guard against weird values
            if est < 0:
                return None
            return float(est)
        except Exception:
            return None

    def _fits_any_pool(req_ram):
        for pool_id in range(s.executor.num_pools):
            if req_ram <= s.executor.pools[pool_id].max_ram_pool:
                return True
        return False

    def _choose_pool_for_op(p, op):
        """Choose a pool that can fit the op's estimated RAM with highest headroom; bias to last_pool_id."""
        est = _est_mem_gb(op)
        meta = s.meta.get(p.pipeline_id, {"ram_boost": 1.0})
        boost = meta.get("ram_boost", 1.0)

        # If we have an estimate, require it to fit into pool.max_ram_pool at least
        # (otherwise it will never run; keep queued to avoid futile OOM loops).
        if est is not None:
            req = max(s.min_ram_gb, est * s.est_safety * boost)
            if not _fits_any_pool(req):
                return None  # cannot fit anywhere right now (or ever); wait (avoid OOM churn)

        best = None
        best_score = None
        last_pool = meta.get("last_pool_id", None)

        for pool_id in range(s.executor.num_pools):
            pool = s.executor.pools[pool_id]
            if pool.avail_cpu_pool <= 0 or pool.avail_ram_pool <= 0:
                continue

            # Compute required RAM to consider this pool feasible
            if est is None:
                # Unknown: require at least minimum RAM now; we may still OOM, but backoff handles it
                req_ram = s.min_ram_gb * boost
            else:
                req_ram = max(s.min_ram_gb, est * s.est_safety * boost)

            if req_ram > pool.avail_ram_pool:
                continue

            # Score by (more available RAM, more CPU), with slight bias to last_pool_id for cache locality
            score = (pool.avail_ram_pool, pool.avail_cpu_pool)
            if last_pool is not None and pool_id == last_pool:
                score = (score[0] * 1.10, score[1] * 1.05)

            if best is None or score > best_score:
                best = pool_id
                best_score = score

        return best

    def _next_pipeline_id():
        """Weighted round-robin across priorities with strict bias to query/interactive."""
        # Add deficit each call; then pick from highest priority that has positive deficit and non-empty queue.
        for k in ("query", "interactive", "batch"):
            s.deficit[k] += s.quantum[k]

        # Try to spend deficits in priority order, but allow batch when accumulated.
        for _ in range(6):  # bounded search
            for k, q in (("query", s.q_query), ("interactive", s.q_interactive), ("batch", s.q_batch)):
                if s.deficit[k] <= 0 or not q:
                    continue
                pid = q.pop(0)
                s.deficit[k] -= 1
                return pid
            # If we couldn't pick due to empty queues, break early
            if not (s.q_query or s.q_interactive or s.q_batch):
                break
            # If deficits are blocked (e.g., all <=0), reset a bit
            if all(s.deficit[k] <= 0 for k in ("query", "interactive", "batch")):
                for k in ("query", "interactive", "batch"):
                    s.deficit[k] += s.quantum[k]
        return None

    def _cpu_for(priority_key, pool_avail_cpu):
        # Carve a share of remaining CPU, but ensure at least 1 vCPU when possible
        share = s.cpu_share.get(priority_key, 0.30)
        cpu = int(pool_avail_cpu * share)
        if pool_avail_cpu > 0 and cpu <= 0:
            cpu = 1
        if cpu > pool_avail_cpu:
            cpu = pool_avail_cpu
        return cpu

    def _ram_for(p, op, pool_avail_ram):
        meta = s.meta.get(p.pipeline_id, {"ram_boost": 1.0})
        boost = meta.get("ram_boost", 1.0)
        est = _est_mem_gb(op)
        if est is None:
            req = s.min_ram_gb * boost
        else:
            req = max(s.min_ram_gb, est * s.est_safety * boost)
        if req > pool_avail_ram:
            return None
        return req

    s.tick += 1

    # Ingest new pipelines
    for p in pipelines:
        _enqueue(p, front=False)

    # Process execution results: detect failures and apply RAM backoff + requeue
    for r in results:
        # Attempt to locate pipeline
        pid = getattr(r, "pipeline_id", None)
        # Some simulators may not put pipeline_id on results; fall back by searching current known pipelines.
        p = s.pipelines_by_id.get(pid) if pid is not None else None

        if r.failed():
            # If we can identify the pipeline, backoff RAM and retry.
            if p is not None:
                meta = s.meta.setdefault(p.pipeline_id, {"ram_boost": 1.0, "fail_count": 0, "last_pool_id": None})
                meta["fail_count"] = meta.get("fail_count", 0) + 1
                meta["last_pool_id"] = r.pool_id

                # Increase RAM boost aggressively early to avoid repeated OOM penalties.
                # Cap to avoid requesting absurd RAM; if it can't fit any pool, it'll just wait.
                if meta["fail_count"] <= 2:
                    meta["ram_boost"] = min(meta.get("ram_boost", 1.0) * 1.8, 16.0)
                else:
                    meta["ram_boost"] = min(meta.get("ram_boost", 1.0) * 1.4, 16.0)

                # Re-enqueue at front for query/interactive, else back
                key = _pkey(p.priority)
                _enqueue(p, front=(key in ("query", "interactive")))
            # If pipeline unknown, do nothing.

    # Early exit if no decision-relevant updates
    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    # Scheduling: greedily fill pools with runnable ops, prioritizing high-priority pipelines.
    # We schedule at most a handful per pool per tick to avoid over-commit and churn.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]

        # Soft limit per pool per tick: more for query-heavy to reduce queueing latency
        per_pool_limit = 6
        made = 0

        while made < per_pool_limit:
            avail_cpu = pool.avail_cpu_pool
            avail_ram = pool.avail_ram_pool
            if avail_cpu <= 0 or avail_ram <= 0:
                break

            pid = _next_pipeline_id()
            if pid is None:
                break

            p = s.pipelines_by_id.get(pid)
            if p is None:
                continue

            st = p.runtime_status()
            # Drop completed pipelines from queues/state
            if st.is_pipeline_successful():
                _dequeue_pid(pid)
                s.meta.pop(pid, None)
                s.pipelines_by_id.pop(pid, None)
                continue

            op = _get_next_assignable_op(p)
            if op is None:
                # Not runnable yet; requeue to preserve order
                _enqueue(p, front=False)
                continue

            # Choose best pool for this op; if not this pool, requeue and try later.
            chosen_pool = _choose_pool_for_op(p, op)
            if chosen_pool is None:
                # Cannot fit anywhere (based on estimate) or no resources; keep queued.
                _enqueue(p, front=False)
                continue
            if chosen_pool != pool_id:
                # Put back and let the appropriate pool pick it up.
                _enqueue(p, front=False)
                continue

            priority_key = _pkey(p.priority)

            cpu = _cpu_for(priority_key, pool.avail_cpu_pool)
            if cpu <= 0:
                _enqueue(p, front=True)
                break

            ram = _ram_for(p, op, pool.avail_ram_pool)
            if ram is None or ram <= 0:
                # Can't fit in this pool now; requeue.
                _enqueue(p, front=False)
                break

            # Record last pool affinity
            s.meta.setdefault(pid, {"ram_boost": 1.0, "fail_count": 0, "last_pool_id": None})
            s.meta[pid]["last_pool_id"] = pool_id

            # Assign exactly one operator at a time to avoid one pipeline hogging resources
            assignments.append(
                Assignment(
                    ops=[op],
                    cpu=cpu,
                    ram=ram,
                    priority=p.priority,
                    pool_id=pool_id,
                    pipeline_id=p.pipeline_id,
                )
            )
            made += 1

            # Requeue pipeline to allow other pipelines to interleave (reduces tail latency)
            _enqueue(p, front=False)

    return suspensions, assignments
