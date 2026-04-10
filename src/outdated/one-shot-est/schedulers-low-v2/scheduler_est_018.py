# policy_key: scheduler_est_018
# reasoning_effort: low
# model: gpt-5.2-2025-12-11
# llm_cost: 0.044832
# generation_seconds: 61.49
# generated_at: 2026-04-01T13:48:59.905850
@register_scheduler_init(key="scheduler_est_018")
def scheduler_est_018_init(s):
    """Priority-aware, concurrency-friendly scheduler with conservative memory sizing.

    Improvements over naive FIFO:
      - Maintain per-priority pipeline queues (QUERY > INTERACTIVE > BATCH_PIPELINE).
      - Avoid "allocate entire pool to one op" by slicing CPU/RAM to run multiple ops concurrently.
      - Use (optional) op.estimate.mem_peak_gb as a conservative hint; on failures, exponentially backoff RAM.
      - Simple fairness: round-robin within each priority and mild aging for batch to avoid starvation.
    """
    # Pipelines waiting by priority.
    s.q_query = []
    s.q_interactive = []
    s.q_batch = []

    # Track per-operator memory backoff after failures/OOM-like events.
    # key: (pipeline_id, op_key) -> {'ram_gb': float, 'fails': int}
    s.op_mem_backoff = {}

    # Round-robin cursor per priority to avoid re-scanning from the front each tick.
    s.rr_cursor = {"query": 0, "interactive": 0, "batch": 0}

    # Soft aging counter for batch pipelines to avoid indefinite starvation.
    # pipeline_id -> accumulated "waiting ticks"
    s.batch_age = {}

    # Config knobs (kept intentionally simple for first iteration).
    s.cfg = {
        # CPU slicing: allow multiple concurrent ops per pool.
        "max_concurrent_ops_per_pool": 8,
        "min_cpu_per_op": 1.0,

        # Memory headroom fraction to reduce OOM risk when estimates are missing/noisy.
        "mem_headroom_frac": 0.15,  # add 15% headroom over estimate/backoff

        # Backoff on failure
        "mem_backoff_mult": 2.0,
        "mem_backoff_add_gb": 1.0,

        # Priority ordering
        "priority_order": ["query", "interactive", "batch"],

        # Batch aging: each tick not scheduled adds weight to selection.
        "batch_age_gain_per_tick": 1.0,
        "batch_age_cap": 50.0,
    }


@register_scheduler(key="scheduler_est_018")
def scheduler_est_018_scheduler(s, results, pipelines):
    """
    Priority-aware scheduler:
      1) Ingest new pipelines into per-priority queues.
      2) Update memory backoff hints based on failed execution results.
      3) For each pool, place multiple ops per tick, prioritizing QUERY then INTERACTIVE then BATCH.
      4) Size resources per op: small CPU slices, conservative RAM using estimates and backoff.
    """
    # ----------------------------
    # Helpers (kept local/no imports)
    # ----------------------------
    def _prio_bucket(p):
        if p == Priority.QUERY:
            return "query"
        if p == Priority.INTERACTIVE:
            return "interactive"
        return "batch"

    def _get_op_key(op):
        # Try stable ids first; fallback to object identity.
        if hasattr(op, "op_id"):
            return ("op_id", getattr(op, "op_id"))
        if hasattr(op, "operator_id"):
            return ("operator_id", getattr(op, "operator_id"))
        if hasattr(op, "name"):
            return ("name", getattr(op, "name"))
        return ("pyid", id(op))

    def _estimate_mem_gb(op):
        # op.estimate.mem_peak_gb may exist and be None/float.
        est = None
        if hasattr(op, "estimate"):
            est = getattr(op.estimate, "mem_peak_gb", None)
        if est is None:
            return None
        try:
            estf = float(est)
            if estf > 0:
                return estf
        except Exception:
            return None
        return None

    def _clamp(x, lo, hi):
        if x < lo:
            return lo
        if x > hi:
            return hi
        return x

    def _is_retryable_failure(exec_result):
        # Conservative: treat any failure as retryable for memory backoff purposes.
        # If there are non-memory failures in the simulator, this is still safe (may over-allocate).
        try:
            return exec_result.failed()
        except Exception:
            return False

    def _get_assignable_op(pipeline):
        status = pipeline.runtime_status()
        # Drop successful pipelines.
        if status.is_pipeline_successful():
            return None

        # We only assign ops whose parents are complete.
        op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        if not op_list:
            return None
        return op_list[0]

    def _pick_pool_for_priority(priority, pools_avail):
        # pools_avail: list of dicts with cpu/ram avail and pool_id.
        # Prefer the pool with most "fit" for interactive work; batch can use anything.
        # Simple heuristic:
        #   - QUERY/INTERACTIVE: maximize min(avail_cpu, avail_ram) to reduce queueing.
        #   - BATCH: maximize avail_cpu (throughput) with ram tie-break.
        if not pools_avail:
            return None

        if priority in (Priority.QUERY, Priority.INTERACTIVE):
            best = None
            best_score = None
            for pa in pools_avail:
                score = min(pa["cpu"], pa["ram"])
                if best is None or score > best_score:
                    best = pa
                    best_score = score
            return best["pool_id"]
        else:
            best = None
            best_score = None
            for pa in pools_avail:
                score = pa["cpu"] * 1.0 + pa["ram"] * 0.01
                if best is None or score > best_score:
                    best = pa
                    best_score = score
            return best["pool_id"]

    def _cpu_target(priority, pool_avail_cpu):
        # Keep this simple: interactive gets slightly more CPU, but still sliced.
        # Ensure at least min_cpu_per_op when possible.
        min_cpu = s.cfg["min_cpu_per_op"]
        if pool_avail_cpu < min_cpu:
            return 0.0
        if priority == Priority.QUERY:
            return _clamp(4.0, min_cpu, pool_avail_cpu)
        if priority == Priority.INTERACTIVE:
            return _clamp(2.0, min_cpu, pool_avail_cpu)
        return _clamp(1.0, min_cpu, pool_avail_cpu)

    def _ram_target_gb(pipeline_id, op, priority, pool_max_ram, pool_avail_ram):
        # Use (1) backoff hint (2) op estimate (3) fallback floor based on priority.
        opk = _get_op_key(op)
        bk = s.op_mem_backoff.get((pipeline_id, opk), None)
        backoff_ram = bk["ram_gb"] if bk else None

        est = _estimate_mem_gb(op)

        # Priority-specific floors to reduce churn when estimates are missing.
        if priority == Priority.QUERY:
            floor = 2.0
        elif priority == Priority.INTERACTIVE:
            floor = 1.5
        else:
            floor = 1.0

        base = floor
        if est is not None:
            base = max(base, est)
        if backoff_ram is not None:
            base = max(base, backoff_ram)

        # Add headroom for safety.
        base = base * (1.0 + s.cfg["mem_headroom_frac"])

        # Don't request more than a reasonable fraction of the pool max (avoid one op blocking everything).
        # For QUERY allow bigger chunk; for batch keep smaller.
        if priority == Priority.QUERY:
            cap_frac = 0.80
        elif priority == Priority.INTERACTIVE:
            cap_frac = 0.60
        else:
            cap_frac = 0.50

        cap = max(1.0, pool_max_ram * cap_frac)

        # Also must fit in current available RAM.
        ram = _clamp(base, 0.0, min(cap, pool_avail_ram))
        return ram

    def _enqueue_pipeline(p):
        b = _prio_bucket(p.priority)
        if b == "query":
            s.q_query.append(p)
        elif b == "interactive":
            s.q_interactive.append(p)
        else:
            s.q_batch.append(p)
            # Initialize age
            if p.pipeline_id not in s.batch_age:
                s.batch_age[p.pipeline_id] = 0.0

    def _age_batch():
        # Mild aging each tick for batch pipelines currently waiting (not completed).
        for p in s.q_batch:
            try:
                st = p.runtime_status()
                if st.is_pipeline_successful():
                    continue
            except Exception:
                pass
            pid = p.pipeline_id
            s.batch_age[pid] = min(
                s.cfg["batch_age_cap"],
                s.batch_age.get(pid, 0.0) + s.cfg["batch_age_gain_per_tick"],
            )

    def _clean_queue(queue):
        # Remove completed pipelines to prevent unbounded growth.
        out = []
        for p in queue:
            try:
                st = p.runtime_status()
                if st.is_pipeline_successful():
                    continue
                # If pipeline has failures, we still keep it (simulator may allow retries).
                out.append(p)
            except Exception:
                out.append(p)
        return out

    # ----------------------------
    # Ingest new pipelines
    # ----------------------------
    for p in pipelines:
        _enqueue_pipeline(p)

    # ----------------------------
    # Process results: update memory backoff on failures
    # ----------------------------
    if results:
        for r in results:
            if not _is_retryable_failure(r):
                continue
            # Increase memory for each op in the failed container.
            try:
                ops = r.ops or []
            except Exception:
                ops = []
            for op in ops:
                # We do not have pipeline_id on ExecutionResult in the provided API.
                # Best-effort: backoff by op identity alone by using pipeline_id=None bucket.
                # But we *do* have pipeline_id when scheduling; prefer to store with None here
                # and later merge by op key regardless of pipeline.
                opk = _get_op_key(op)
                key_none = (None, opk)
                prev = s.op_mem_backoff.get(key_none, None)
                prev_ram = prev["ram_gb"] if prev else None
                current = None
                try:
                    current = float(r.ram) if r.ram is not None else None
                except Exception:
                    current = None

                # Next RAM guess: max(current, prev_ram, est) * mult + add
                est = _estimate_mem_gb(op)
                base = 1.0
                if current is not None and current > 0:
                    base = max(base, current)
                if prev_ram is not None and prev_ram > 0:
                    base = max(base, prev_ram)
                if est is not None and est > 0:
                    base = max(base, est)

                new_ram = base * s.cfg["mem_backoff_mult"] + s.cfg["mem_backoff_add_gb"]
                s.op_mem_backoff[key_none] = {
                    "ram_gb": new_ram,
                    "fails": (prev["fails"] + 1) if prev else 1,
                }

    # Age batch a bit each tick to improve fairness under constant interactive load.
    _age_batch()

    # Clean completed pipelines from queues.
    s.q_query = _clean_queue(s.q_query)
    s.q_interactive = _clean_queue(s.q_interactive)
    s.q_batch = _clean_queue(s.q_batch)

    # Early exit if nothing to do.
    if not pipelines and not results and not (s.q_query or s.q_interactive or s.q_batch):
        return [], []

    suspensions = []
    assignments = []

    # Snapshot pool availability.
    pools_avail = []
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        pools_avail.append({
            "pool_id": pool_id,
            "cpu": float(pool.avail_cpu_pool),
            "ram": float(pool.avail_ram_pool),
            "max_cpu": float(pool.max_cpu_pool),
            "max_ram": float(pool.max_ram_pool),
        })

    # ----------------------------
    # Main placement loop: iterate pools, pack multiple ops per pool
    # ----------------------------
    for pa in pools_avail:
        pool_id = pa["pool_id"]
        pool_cpu = pa["cpu"]
        pool_ram = pa["ram"]
        pool_max_ram = pa["max_ram"]

        if pool_cpu <= 0 or pool_ram <= 0:
            continue

        # We will place up to N ops per pool per tick (packing).
        placed = 0
        max_place = s.cfg["max_concurrent_ops_per_pool"]

        # Within a pool tick, we always try higher priorities first.
        # For batch, we also consider age by doing a simple "pick oldest" fallback.
        while placed < max_place and pool_cpu > 0 and pool_ram > 0:
            # Choose which priority bucket to schedule next from.
            # If any query ops exist -> schedule them; else interactive; else batch.
            chosen_bucket = None
            if s.q_query:
                chosen_bucket = "query"
            elif s.q_interactive:
                chosen_bucket = "interactive"
            elif s.q_batch:
                chosen_bucket = "batch"
            else:
                break

            # Select a pipeline in round-robin (batch uses age-biased selection).
            if chosen_bucket == "query":
                q = s.q_query
                prio = Priority.QUERY
            elif chosen_bucket == "interactive":
                q = s.q_interactive
                prio = Priority.INTERACTIVE
            else:
                q = s.q_batch
                prio = Priority.BATCH_PIPELINE

            if not q:
                break

            # Batch: pick highest age among a small window to keep it cheap.
            # Query/Interactive: round-robin.
            if chosen_bucket == "batch":
                best_idx = None
                best_age = -1.0
                # Look at first K pipelines to avoid O(n) scans.
                K = min(10, len(q))
                for i in range(K):
                    p = q[i]
                    age = s.batch_age.get(p.pipeline_id, 0.0)
                    if age > best_age:
                        best_age = age
                        best_idx = i
                idx = 0 if best_idx is None else best_idx
            else:
                cur = s.rr_cursor[chosen_bucket] % max(1, len(q))
                idx = cur

            pipeline = q.pop(idx)
            if chosen_bucket != "batch":
                s.rr_cursor[chosen_bucket] = idx  # next tick continues from here-ish

            # Find an assignable op.
            op = _get_assignable_op(pipeline)
            if op is None:
                # Not ready or completed; do not requeue if completed.
                try:
                    if not pipeline.runtime_status().is_pipeline_successful():
                        q.append(pipeline)
                except Exception:
                    q.append(pipeline)
                continue

            # Decide placement pool (if multiple pools). If this pool isn't best, requeue and continue.
            # This keeps logic simple and avoids cross-pool overcommit. If only 1 pool, it will match.
            best_pool = _pick_pool_for_priority(
                pipeline.priority,
                [{"pool_id": x["pool_id"], "cpu": x["cpu"], "ram": x["ram"]} for x in pools_avail],
            )
            if best_pool is not None and best_pool != pool_id:
                # Requeue and try next candidate.
                q.append(pipeline)
                # To avoid infinite looping in a single pool, break if nothing else to do here.
                # We'll let the outer loop reach the best pool.
                break

            # Compute resources per op.
            cpu_req = _cpu_target(pipeline.priority, pool_cpu)
            if cpu_req <= 0:
                q.append(pipeline)
                break

            # Merge global backoff keyed by (None, op_key) into per-pipeline key.
            opk = _get_op_key(op)
            none_bk = s.op_mem_backoff.get((None, opk), None)
            if none_bk is not None:
                cur_bk = s.op_mem_backoff.get((pipeline.pipeline_id, opk), None)
                if cur_bk is None or (none_bk["ram_gb"] > cur_bk["ram_gb"]):
                    s.op_mem_backoff[(pipeline.pipeline_id, opk)] = dict(none_bk)

            ram_req = _ram_target_gb(pipeline.pipeline_id, op, pipeline.priority, pool_max_ram, pool_ram)
            # If we cannot fit a minimum usable RAM, requeue and stop packing this pool.
            if ram_req <= 0.0:
                q.append(pipeline)
                break

            # Create assignment for exactly one op (atomic unit) with sliced resources.
            assignment = Assignment(
                ops=[op],
                cpu=cpu_req,
                ram=ram_req,
                priority=pipeline.priority,
                pool_id=pool_id,
                pipeline_id=pipeline.pipeline_id,
            )
            assignments.append(assignment)

            # Update pool remaining capacity for further packing in this same tick.
            pool_cpu -= cpu_req
            pool_ram -= ram_req
            placed += 1

            # Requeue pipeline for additional ops later (once dependencies are satisfied).
            q.append(pipeline)

            # If we placed a batch op, reduce its age slightly (it got service).
            if pipeline.priority == Priority.BATCH_PIPELINE:
                s.batch_age[pipeline.pipeline_id] = max(0.0, s.batch_age.get(pipeline.pipeline_id, 0.0) - 5.0)

    return suspensions, assignments
