# policy_key: scheduler_none_025
# reasoning_effort: none
# model: gpt-5.2-2025-12-11
# llm_cost: 0.062255
# generation_seconds: 50.15
# generated_at: 2026-04-09T22:04:37.893913
@register_scheduler_init(key="scheduler_none_025")
def scheduler_none_025_init(s):
    """Priority-aware, failure-averse scheduler.

    Core ideas (kept simple, incremental vs FIFO):
    - Weighted priority queues (query > interactive > batch) to protect high-weight latency.
    - Conservative, right-sized initial CPU/RAM allocations to reduce OOM risk and improve completion rate.
    - On OOM/failure signal, retry the same pipeline with increased RAM (exponential backoff) and modest CPU bump.
    - Gentle fairness via aging credit for waiting batch/interactive so they still make progress.
    - Avoid aggressive preemption (churn can waste work and increase failures); only preempt as a last resort
      and only for QUERY if we detect clear starvation of query work with available low-priority running containers.
    """
    # Per-pipeline learned sizing hints
    s.pipe_hints = {}  # pipeline_id -> dict(ram_mult, cpu_mult, last_pool_id)
    s.arrival_order = 0
    s.enqueue_meta = {}  # pipeline_id -> dict(arrival_seq, last_enqueued_t)

    # Waiting queues by priority
    s.wait_q = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }

    # Aging/fairness credits (accumulate when not scheduled)
    s.fair_credit = {
        Priority.QUERY: 0.0,
        Priority.INTERACTIVE: 0.0,
        Priority.BATCH_PIPELINE: 0.0,
    }

    # Track recently seen running containers by pool to enable limited preemption if necessary
    s.running_index = {}  # pool_id -> list of (priority, container_id)

    # Tunables (safe defaults)
    s.base_ram_frac = 0.60  # start with a majority of pool RAM to avoid OOM (functions scale-up bias)
    s.base_cpu_frac = 0.50  # keep headroom for concurrent arrivals, improve responsiveness
    s.max_assignments_per_tick_per_pool = 1  # keep simple; reduces overcommit/fragmentation risk
    s.max_ops_per_assignment = 1  # atomic step: one ready operator per assignment

    # Retry sizing multipliers
    s.ram_backoff = 1.6  # on OOM, increase RAM noticeably
    s.cpu_backoff = 1.25  # mild CPU bump on failure
    s.max_ram_mult = 4.0
    s.max_cpu_mult = 2.0

    # Limited preemption configuration
    s.enable_preemption = True
    s.preempt_only_for_query = True


@register_scheduler(key="scheduler_none_025")
def scheduler_none_025_scheduler(s, results, pipelines):
    """
    Scheduler step.

    Produces:
    - suspensions: optional preemption to protect QUERY tail latency
    - assignments: place one ready operator at a time, with conservative sizing and OOM-aware retries
    """
    # Helper functions defined inside to avoid imports at module level (per instructions)
    def _prio_weight(prio):
        if prio == Priority.QUERY:
            return 10
        if prio == Priority.INTERACTIVE:
            return 5
        return 1

    def _ensure_hint(pipeline_id):
        if pipeline_id not in s.pipe_hints:
            s.pipe_hints[pipeline_id] = {
                "ram_mult": 1.0,
                "cpu_mult": 1.0,
                "last_pool_id": None,
            }
        return s.pipe_hints[pipeline_id]

    def _enqueue_pipeline(p):
        pid = p.pipeline_id
        if pid not in s.enqueue_meta:
            s.arrival_order += 1
            s.enqueue_meta[pid] = {"arrival_seq": s.arrival_order, "last_enqueued_t": s.arrival_order}
        else:
            s.enqueue_meta[pid]["last_enqueued_t"] = s.arrival_order

        s.wait_q[p.priority].append(p)
        _ensure_hint(pid)

    def _pipeline_is_done_or_failed(p):
        st = p.runtime_status()
        if st.is_pipeline_successful():
            return True
        # If there are FAILED ops, we will retry them (ASSIGNABLE_STATES includes FAILED in this simulator),
        # so we should NOT drop the pipeline here.
        return False

    def _get_ready_ops(p):
        st = p.runtime_status()
        # Prefer only ops whose parents are complete to reduce wasted work
        return st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)

    def _choose_pool_for_priority(priority):
        # Simple: try to place QUERY/INTERACTIVE on pool with most free CPU (responsiveness),
        # and BATCH on pool with most free RAM (OOM avoidance, larger steps).
        best_pool = None
        best_score = None
        for pool_id in range(s.executor.num_pools):
            pool = s.executor.pools[pool_id]
            avail_cpu = pool.avail_cpu_pool
            avail_ram = pool.avail_ram_pool
            if avail_cpu <= 0 or avail_ram <= 0:
                continue

            if priority in (Priority.QUERY, Priority.INTERACTIVE):
                score = (avail_cpu / max(pool.max_cpu_pool, 1e-9)) * 0.7 + (avail_ram / max(pool.max_ram_pool, 1e-9)) * 0.3
            else:
                score = (avail_ram / max(pool.max_ram_pool, 1e-9)) * 0.7 + (avail_cpu / max(pool.max_cpu_pool, 1e-9)) * 0.3

            if best_score is None or score > best_score:
                best_score = score
                best_pool = pool_id
        return best_pool

    def _compute_request(pool_id, pipeline_id, priority):
        pool = s.executor.pools[pool_id]
        hint = _ensure_hint(pipeline_id)

        # Base request is a fraction of *pool capacity* (not just available), then bounded by available.
        base_ram = pool.max_ram_pool * s.base_ram_frac
        base_cpu = pool.max_cpu_pool * s.base_cpu_frac

        # Priority tilt: queries get slightly more CPU to reduce latency; batch gets slightly more RAM to avoid OOM.
        if priority == Priority.QUERY:
            base_cpu *= 1.25
            base_ram *= 1.00
        elif priority == Priority.INTERACTIVE:
            base_cpu *= 1.10
            base_ram *= 1.00
        else:
            base_cpu *= 0.90
            base_ram *= 1.15

        # Apply learned multipliers after failures
        req_ram = base_ram * hint["ram_mult"]
        req_cpu = base_cpu * hint["cpu_mult"]

        # Clamp to [small positive, available]
        # Ensure at least some resources; simulator likely expects >0
        req_ram = max(0.1, min(req_ram, pool.avail_ram_pool))
        req_cpu = max(0.1, min(req_cpu, pool.avail_cpu_pool))
        return req_cpu, req_ram

    def _bump_hints_on_failure(res):
        # Conservative: only bump if result indicates failure. If error suggests OOM, prioritize RAM.
        # We don't have structured error codes; use string heuristics.
        if not res.failed():
            return
        pid = res.ops[0].pipeline_id if getattr(res.ops[0], "pipeline_id", None) is not None else None
        # If pipeline_id isn't on op, fall back to res context; if unavailable, skip.
        # (Eudoxia's op objects usually carry pipeline_id; keep robust.)
        if pid is None:
            return

        hint = _ensure_hint(pid)
        err = str(res.error).lower() if res.error is not None else ""
        is_oom = ("oom" in err) or ("out of memory" in err) or ("memory" in err)

        if is_oom:
            hint["ram_mult"] = min(s.max_ram_mult, max(hint["ram_mult"], 1.0) * s.ram_backoff)
            # Tiny cpu bump can help if failure was due to timeouts modeled as failures
            hint["cpu_mult"] = min(s.max_cpu_mult, max(hint["cpu_mult"], 1.0) * 1.05)
        else:
            # Unknown failure: bump both a bit, but keep RAM preference (failures are costly)
            hint["ram_mult"] = min(s.max_ram_mult, max(hint["ram_mult"], 1.0) * 1.25)
            hint["cpu_mult"] = min(s.max_cpu_mult, max(hint["cpu_mult"], 1.0) * s.cpu_backoff)

    def _update_running_index_from_results():
        # Rebuild a light index opportunistically; results include container_id and pool_id.
        # We only track containers mentioned in recent results to avoid needing executor introspection.
        # This makes preemption "best-effort" and limited.
        for res in results:
            if getattr(res, "pool_id", None) is None or getattr(res, "container_id", None) is None:
                continue
            if res.pool_id not in s.running_index:
                s.running_index[res.pool_id] = []
            # If a container produced a result, it likely finished/failed; don't add as running.
            # We keep the list only for containers we might have assigned this tick; handled below.

    def _age_fairness():
        # Credits accumulate for lower priorities to prevent starvation.
        # Higher weight classes age slower since they're already favored by selection.
        s.fair_credit[Priority.QUERY] += 0.2
        s.fair_credit[Priority.INTERACTIVE] += 0.4
        s.fair_credit[Priority.BATCH_PIPELINE] += 0.8

        # Cap credits to avoid runaway
        for k in s.fair_credit:
            if s.fair_credit[k] > 5.0:
                s.fair_credit[k] = 5.0

    def _pick_next_pipeline():
        # Weighted priority with aging: compute a score per queue head
        # (simple, avoids sorting entire queue).
        candidates = []
        for prio, q in s.wait_q.items():
            while q and _pipeline_is_done_or_failed(q[0]):
                q.pop(0)
            if not q:
                continue
            p = q[0]
            ready = _get_ready_ops(p)
            if not ready:
                # If no ready ops, keep it in queue but allow others behind it by rotating once
                # (prevents head-of-line blocking in a simple way).
                q.append(q.pop(0))
                # Re-check new head next iteration via recursion avoidance; just skip now.
                continue
            # Base preference by priority weight plus fairness credit; smaller arrival_seq preferred within same class
            pid = p.pipeline_id
            arrival_seq = s.enqueue_meta.get(pid, {}).get("arrival_seq", 0)
            score = _prio_weight(prio) + s.fair_credit.get(prio, 0.0) - (arrival_seq * 1e-6)
            candidates.append((score, prio, p))
        if not candidates:
            return None
        candidates.sort(key=lambda x: x[0], reverse=True)
        _, prio, p = candidates[0]
        # Consume some fairness credit when chosen (more for lower priorities to keep balance)
        if prio == Priority.BATCH_PIPELINE:
            s.fair_credit[prio] = max(0.0, s.fair_credit[prio] - 1.0)
        elif prio == Priority.INTERACTIVE:
            s.fair_credit[prio] = max(0.0, s.fair_credit[prio] - 0.6)
        else:
            s.fair_credit[prio] = max(0.0, s.fair_credit[prio] - 0.3)
        return p

    # Enqueue new pipelines
    for p in pipelines:
        _enqueue_pipeline(p)

    # Early exit if nothing to do
    if not pipelines and not results:
        return [], []

    # Update hints based on execution outcomes (OOM avoidance boosts completion rate)
    for res in results:
        _bump_hints_on_failure(res)

    _update_running_index_from_results()
    _age_fairness()

    suspensions = []
    assignments = []

    # Optional limited preemption: only if QUERY exists waiting with ready ops and no pool can fit it.
    # Preempt one low-priority container in a pool with no headroom to create space.
    def _query_waiting_ready():
        q = s.wait_q[Priority.QUERY]
        for _ in range(min(len(q), 3)):
            if not q:
                break
            p = q[0]
            if _pipeline_is_done_or_failed(p):
                q.pop(0)
                continue
            if _get_ready_ops(p):
                return True
            q.append(q.pop(0))
        return False

    def _any_pool_can_admit_query():
        # Check if any pool has enough resources for a reasonable query request
        for pool_id in range(s.executor.num_pools):
            pool = s.executor.pools[pool_id]
            if pool.avail_cpu_pool <= 0 or pool.avail_ram_pool <= 0:
                continue
            # Use conservative minimum: at least 10% of pool CPU and 20% of pool RAM available
            if pool.avail_cpu_pool >= max(0.1, 0.10 * pool.max_cpu_pool) and pool.avail_ram_pool >= max(0.1, 0.20 * pool.max_ram_pool):
                return True
        return False

    if s.enable_preemption and _query_waiting_ready() and not _any_pool_can_admit_query():
        # Best-effort: preempt one known low-priority running container if we have any in index.
        # Since we can't inspect all running containers, this is intentionally conservative.
        for pool_id, lst in list(s.running_index.items()):
            # Prefer preempting BATCH first, then INTERACTIVE (never preempt QUERY)
            victim = None
            for prio, cid in lst:
                if prio == Priority.BATCH_PIPELINE:
                    victim = (cid, prio)
                    break
            if victim is None:
                for prio, cid in lst:
                    if prio == Priority.INTERACTIVE and not s.preempt_only_for_query:
                        victim = (cid, prio)
                        break
            if victim is not None:
                cid, _ = victim
                suspensions.append(Suspend(cid, pool_id))
                # Remove it from index
                s.running_index[pool_id] = [(p, c) for (p, c) in lst if c != cid]
                break

    # Build assignments: per pool, make up to N assignments.
    # We do not pack many ops at once to avoid a single pipeline monopolizing and to reduce OOM probability.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        for _ in range(s.max_assignments_per_tick_per_pool):
            if pool.avail_cpu_pool <= 0 or pool.avail_ram_pool <= 0:
                break

            p = _pick_next_pipeline()
            if p is None:
                break

            # Prefer placing on a "best" pool; if chosen pool differs, we keep pipeline in queue and skip here.
            best_pool = _choose_pool_for_priority(p.priority)
            if best_pool is not None and best_pool != pool_id:
                # Put it back at the end of its priority queue and try another pipeline for this pool
                s.wait_q[p.priority].append(s.wait_q[p.priority].pop(0))
                continue

            # Remove from head (it should be at head for its queue)
            # Because _pick_next_pipeline returns head, we pop head now.
            if s.wait_q[p.priority] and s.wait_q[p.priority][0].pipeline_id == p.pipeline_id:
                s.wait_q[p.priority].pop(0)
            else:
                # Fallback: remove by id (should be rare due to rotation logic)
                q = s.wait_q[p.priority]
                for i in range(len(q)):
                    if q[i].pipeline_id == p.pipeline_id:
                        q.pop(i)
                        break

            if _pipeline_is_done_or_failed(p):
                continue

            ready_ops = _get_ready_ops(p)
            if not ready_ops:
                # Not ready now; requeue and move on
                s.wait_q[p.priority].append(p)
                continue

            op_list = ready_ops[: s.max_ops_per_assignment]
            req_cpu, req_ram = _compute_request(pool_id, p.pipeline_id, p.priority)

            assignment = Assignment(
                ops=op_list,
                cpu=req_cpu,
                ram=req_ram,
                priority=p.priority,
                pool_id=pool_id,
                pipeline_id=p.pipeline_id,
            )
            assignments.append(assignment)

            # Track as "running" candidate for potential preemption decisions next tick (best-effort)
            if pool_id not in s.running_index:
                s.running_index[pool_id] = []
            # We don't know container_id until result; store placeholder None? skip; rely on results for real ids.

            # Requeue pipeline to allow next operators later (keeps throughput and avoids dropping)
            s.wait_q[p.priority].append(p)

    return suspensions, assignments
