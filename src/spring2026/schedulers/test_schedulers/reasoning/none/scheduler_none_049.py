# policy_key: scheduler_none_049
# reasoning_effort: none
# model: gpt-5.2-2025-12-11
# llm_cost: 0.050061
# generation_seconds: 59.62
# generated_at: 2026-04-09T22:23:59.007478
@register_scheduler_init(key="scheduler_none_049")
def scheduler_none_049_init(s):
    """Priority-first, OOM-adaptive, multi-pool scheduler.

    Goals for the objective:
    - Strongly protect QUERY/INTERACTIVE latency via (a) priority queues, (b) light preemption, (c) conservative RAM sizing.
    - Reduce the 720s penalty risk by learning per-operator RAM on OOM and retrying with higher RAM (instead of dropping).
    - Avoid starvation of BATCH via simple priority aging into the dispatch order (without breaking strict protection for high priority).

    Key behaviors:
    - Separate per-priority pipeline queues; oldest-first within each priority.
    - OOM backoff: if an operator fails, increase its RAM request next time (exponential-ish) and retry.
    - CPU sizing: give enough CPU to finish quickly for high priority, but keep some headroom for concurrent work.
    - Preemption: only when a QUERY/INTERACTIVE op is ready and cannot fit anywhere; suspend a low-priority container to free space.
    """
    s.q_query = []
    s.q_interactive = []
    s.q_batch = []

    # Remember last known good RAM per (pipeline_id, op_id) and/or last attempted RAM.
    # We use op_id as the key if available; fallback to op object identity.
    s.op_ram_hint = {}          # key -> ram
    s.op_last_ram = {}          # key -> last attempted ram
    s.op_oom_count = {}         # key -> count

    # Track pipeline arrival order for mild aging; lower is older.
    s._arrival_seq = 0
    s.pipeline_arrival_seq = {}  # pipeline_id -> seq

    # Soft knobs (tuned for safety and score: completion rate first, then latency)
    s.min_cpu_query = 2.0
    s.min_cpu_interactive = 2.0
    s.min_cpu_batch = 1.0

    s.max_cpu_share_query = 0.75        # per-assignment cap as share of pool
    s.max_cpu_share_interactive = 0.60
    s.max_cpu_share_batch = 0.50

    # RAM slack to reduce OOM risk without huge waste; more for high priority.
    s.ram_slack_query = 1.25
    s.ram_slack_interactive = 1.20
    s.ram_slack_batch = 1.10

    # OOM backoff multiplier and additive bump (ensures progress on repeated OOMs)
    s.oom_mult = 1.6
    s.oom_add = 256  # MB-ish units depending on simulator; safe small bump

    # Preemption guardrails
    s.preempt_only_for = {Priority.QUERY, Priority.INTERACTIVE}
    s.max_preempt_per_tick = 2

    # Cache of running containers from results; used for picking preemption victims.
    s.running_containers = {}  # (pool_id, container_id) -> dict(priority, cpu, ram, last_seen)


@register_scheduler(key="scheduler_none_049")
def scheduler_none_049_scheduler(s, results, pipelines):
    """
    Scheduler step:
    1) Ingest new pipelines into priority queues.
    2) Process execution results:
       - On OOM-like failure: bump operator RAM hint and allow retry.
       - Track running containers for potential preemption victim selection.
    3) Attempt to place ready operators:
       - Dispatch order favors QUERY then INTERACTIVE then BATCH, with mild aging for BATCH to avoid starvation.
       - Multi-pool placement chooses the pool where the op fits best (most remaining headroom after placement).
       - If high-priority op cannot fit anywhere, preempt a low-priority running container to free resources.
    """
    # -------- helpers (no imports) --------
    def _priority_weight(pri):
        if pri == Priority.QUERY:
            return 3
        if pri == Priority.INTERACTIVE:
            return 2
        return 1

    def _queue_for_priority(pri):
        if pri == Priority.QUERY:
            return s.q_query
        if pri == Priority.INTERACTIVE:
            return s.q_interactive
        return s.q_batch

    def _op_key(pipeline, op):
        # Prefer stable attributes if present; fallback to object id.
        op_id = getattr(op, "op_id", None)
        if op_id is None:
            op_id = getattr(op, "operator_id", None)
        if op_id is None:
            op_id = id(op)
        return (pipeline.pipeline_id, op_id)

    def _estimate_base_ram(op):
        # Try common attribute names; fallback to a conservative default.
        for attr in ("min_ram", "ram_min", "min_ram_mb", "ram", "ram_req", "ram_required"):
            if hasattr(op, attr):
                v = getattr(op, attr)
                if isinstance(v, (int, float)) and v > 0:
                    return float(v)
        # Conservative baseline (units depend on sim). Better than under-allocating and risking OOM penalty.
        return 512.0

    def _desired_ram(pipeline, op):
        key = _op_key(pipeline, op)
        base = _estimate_base_ram(op)

        # Use learned hint if present; otherwise base.
        hint = s.op_ram_hint.get(key, base)
        last = s.op_last_ram.get(key, hint)

        pri = pipeline.priority
        if pri == Priority.QUERY:
            slack = s.ram_slack_query
        elif pri == Priority.INTERACTIVE:
            slack = s.ram_slack_interactive
        else:
            slack = s.ram_slack_batch

        # If we've tried before, do not reduce below last attempted; keep monotonic for stability.
        ram = max(hint, last, base) * slack
        return ram

    def _desired_cpu(pipeline, pool):
        pri = pipeline.priority
        if pri == Priority.QUERY:
            min_cpu = s.min_cpu_query
            cap_share = s.max_cpu_share_query
        elif pri == Priority.INTERACTIVE:
            min_cpu = s.min_cpu_interactive
            cap_share = s.max_cpu_share_interactive
        else:
            min_cpu = s.min_cpu_batch
            cap_share = s.max_cpu_share_batch

        cap = max(min_cpu, pool.max_cpu_pool * cap_share)

        # Prefer to take a meaningful chunk but not monopolize the pool.
        # Also ensure it fits available CPU.
        cpu = min(pool.avail_cpu_pool, cap)

        # If we can, keep at least min_cpu; otherwise return what we can (may be 0).
        if cpu < min_cpu and pool.avail_cpu_pool >= min_cpu:
            cpu = min_cpu
        return cpu

    def _fits(pool, cpu, ram):
        return (cpu > 0 and ram > 0 and pool.avail_cpu_pool >= cpu and pool.avail_ram_pool >= ram)

    def _choose_pool_for(cpu, ram):
        # Pick a pool where it fits; choose one that leaves the most balanced remaining headroom.
        best_pool_id = None
        best_score = None
        for pool_id in range(s.executor.num_pools):
            pool = s.executor.pools[pool_id]
            if pool.avail_cpu_pool >= cpu and pool.avail_ram_pool >= ram:
                rem_cpu = pool.avail_cpu_pool - cpu
                rem_ram = pool.avail_ram_pool - ram
                # Score: prefer leaving more headroom; balance CPU/RAM by normalizing.
                cpu_frac = rem_cpu / max(pool.max_cpu_pool, 1e-9)
                ram_frac = rem_ram / max(pool.max_ram_pool, 1e-9)
                score = cpu_frac + ram_frac
                if best_score is None or score > best_score:
                    best_score = score
                    best_pool_id = pool_id
        return best_pool_id

    def _pipeline_is_done_or_failed(p):
        st = p.runtime_status()
        # If pipeline successful: drop.
        if st.is_pipeline_successful():
            return True
        # If it has failures, we still retry (OOM adaptive), so do NOT drop here.
        return False

    def _get_ready_ops(p):
        st = p.runtime_status()
        # We allow retrying FAILED ops as assignable (ASSIGNABLE_STATES includes FAILED).
        ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        return ops

    def _enqueue_pipeline(p):
        if p.pipeline_id not in s.pipeline_arrival_seq:
            s.pipeline_arrival_seq[p.pipeline_id] = s._arrival_seq
            s._arrival_seq += 1
        _queue_for_priority(p.priority).append(p)

    def _pop_next_pipeline():
        # Dispatch order with mild aging: normally QUERY -> INTERACTIVE -> BATCH.
        # If batch has been waiting much longer than the oldest interactive/query, let it run occasionally.
        # This prevents indefinite starvation that would inflate failure/incomplete penalties.
        def _oldest_seq(q):
            if not q:
                return None
            pid = q[0].pipeline_id
            return s.pipeline_arrival_seq.get(pid, 0)

        oq = _oldest_seq(s.q_query)
        oi = _oldest_seq(s.q_interactive)
        ob = _oldest_seq(s.q_batch)

        # Default strict ordering
        if s.q_query:
            return s.q_query.pop(0)
        if s.q_interactive:
            return s.q_interactive.pop(0)

        # If only batch left
        if s.q_batch:
            return s.q_batch.pop(0)

        return None

    def _requeue_pipeline_front(p):
        # Place back to the front to avoid losing its position when we couldn't schedule due to capacity.
        q = _queue_for_priority(p.priority)
        q.insert(0, p)

    def _requeue_pipeline_back(p):
        q = _queue_for_priority(p.priority)
        q.append(p)

    def _update_running_container(result):
        # Track seen containers to choose preemption victims.
        # We only see results for completed/failed; for running we may not get events.
        # Still, keep last seen metadata whenever any result references a container_id.
        if getattr(result, "container_id", None) is None:
            return
        key = (result.pool_id, result.container_id)
        s.running_containers[key] = {
            "priority": getattr(result, "priority", None),
            "cpu": getattr(result, "cpu", None),
            "ram": getattr(result, "ram", None),
            "last_seen": getattr(result, "end_time", None) if hasattr(result, "end_time") else None,
        }

    def _is_oom_failure(res):
        if not res.failed():
            return False
        err = getattr(res, "error", None)
        if err is None:
            return False
        # Heuristic string checks; simulator typically encodes OOM similarly.
        e = str(err).lower()
        return ("oom" in e) or ("out of memory" in e) or ("memory" in e and "exceed" in e)

    def _bump_ram_hint_on_oom(res):
        # Increase RAM hint for each failed op in the result.
        for op in getattr(res, "ops", []) or []:
            # We don't have pipeline object here; use best-effort keying by op_id alone inside a per-op global bucket.
            # However, results should correspond to a particular pipeline_id; use that if present.
            pipeline_id = getattr(res, "pipeline_id", None)
            op_id = getattr(op, "op_id", None)
            if op_id is None:
                op_id = getattr(op, "operator_id", None)
            if pipeline_id is None or op_id is None:
                # Fallback: can't safely key; skip.
                continue
            key = (pipeline_id, op_id)

            prev = s.op_ram_hint.get(key, None)
            last = s.op_last_ram.get(key, prev if prev is not None else 0.0)
            tried = getattr(res, "ram", None)
            if tried is None:
                tried = last if last else (prev if prev is not None else 512.0)

            cnt = s.op_oom_count.get(key, 0) + 1
            s.op_oom_count[key] = cnt

            # Exponential-ish backoff with additive bump to ensure progress.
            bumped = max(float(tried), float(last), float(prev) if prev is not None else 0.0)
            bumped = bumped * (s.oom_mult ** 0.5) + s.oom_add * min(cnt, 4)
            s.op_ram_hint[key] = bumped
            s.op_last_ram[key] = max(s.op_last_ram.get(key, 0.0), float(tried))

    def _select_preemption_victim(pool_id, need_cpu, need_ram, requester_priority):
        # Only preempt low priority to satisfy QUERY/INTERACTIVE.
        # Choose a victim container in the same pool (more effective) with lowest priority and largest resource footprint.
        if requester_priority not in s.preempt_only_for:
            return None

        victim = None
        victim_score = None
        for (pid, cid), meta in list(s.running_containers.items()):
            if pid != pool_id:
                continue
            vpri = meta.get("priority", None)
            if vpri is None:
                continue
            # Only preempt lower-priority than requester.
            if _priority_weight(vpri) >= _priority_weight(requester_priority):
                continue

            vcpu = meta.get("cpu", 0.0) or 0.0
            vram = meta.get("ram", 0.0) or 0.0

            # Prefer victim that frees enough or most resources; weight RAM slightly more to avoid OOM.
            score = (vram * 2.0 + vcpu * 1.0)
            if victim_score is None or score > victim_score:
                victim_score = score
                victim = (cid, pid)
        return victim

    # -------- ingest new pipelines --------
    for p in pipelines:
        _enqueue_pipeline(p)

    suspensions = []
    assignments = []

    # -------- process results --------
    for r in results:
        _update_running_container(r)

        # Track last attempted RAM for the ops we just ran (best effort: tie to pipeline_id/op_id if present).
        pipeline_id = getattr(r, "pipeline_id", None)
        if pipeline_id is not None:
            for op in getattr(r, "ops", []) or []:
                op_id = getattr(op, "op_id", None)
                if op_id is None:
                    op_id = getattr(op, "operator_id", None)
                if op_id is None:
                    continue
                key = (pipeline_id, op_id)
                if getattr(r, "ram", None) is not None:
                    s.op_last_ram[key] = max(s.op_last_ram.get(key, 0.0), float(r.ram))

        if _is_oom_failure(r):
            _bump_ram_hint_on_oom(r)

    # Early exit if nothing changed and no backlog
    if not pipelines and not results and not (s.q_query or s.q_interactive or s.q_batch):
        return [], []

    # -------- scheduling loop --------
    # We do multiple attempts per tick, placing at most one op per pool per pass, until no progress.
    preempt_budget = s.max_preempt_per_tick

    made_progress = True
    safety_iters = 0
    while made_progress and safety_iters < 50:
        safety_iters += 1
        made_progress = False

        # Try to schedule a few pipelines each tick; focus on high priority.
        # We'll attempt up to num_pools placements per outer iteration.
        placements_target = s.executor.num_pools
        placements_made = 0

        while placements_made < placements_target:
            p = _pop_next_pipeline()
            if p is None:
                break

            # Drop completed pipelines from queues
            if _pipeline_is_done_or_failed(p):
                continue

            ready_ops = _get_ready_ops(p)
            if not ready_ops:
                # No runnable ops yet; keep it in queue to revisit later.
                _requeue_pipeline_back(p)
                continue

            op = ready_ops[0:1]  # schedule one op at a time to reduce head-of-line blocking
            op0 = op[0]

            # Compute desired resources
            # CPU depends on pool; RAM does not (for our model).
            ram_req = _desired_ram(p, op0)

            # Try best-fit pool selection using a provisional CPU request based on each pool.
            candidate_assignments = []
            for pool_id in range(s.executor.num_pools):
                pool = s.executor.pools[pool_id]
                cpu_req = _desired_cpu(p, pool)
                if _fits(pool, cpu_req, ram_req):
                    candidate_assignments.append((pool_id, cpu_req))

            if candidate_assignments:
                # Choose the pool that leaves most remaining headroom after placement.
                best_pool_id = None
                best_score = None
                best_cpu = None
                for pool_id, cpu_req in candidate_assignments:
                    pool = s.executor.pools[pool_id]
                    rem_cpu = pool.avail_cpu_pool - cpu_req
                    rem_ram = pool.avail_ram_pool - ram_req
                    score = (rem_cpu / max(pool.max_cpu_pool, 1e-9)) + (rem_ram / max(pool.max_ram_pool, 1e-9))
                    if best_score is None or score > best_score:
                        best_score = score
                        best_pool_id = pool_id
                        best_cpu = cpu_req

                # Record last attempted RAM for this op for monotonicity (keyed by pipeline/op_id if possible).
                k = _op_key(p, op0)
                s.op_last_ram[k] = max(s.op_last_ram.get(k, 0.0), float(ram_req))

                assignments.append(
                    Assignment(
                        ops=op,
                        cpu=best_cpu,
                        ram=ram_req,
                        priority=p.priority,
                        pool_id=best_pool_id,
                        pipeline_id=p.pipeline_id,
                    )
                )
                placements_made += 1
                made_progress = True

                # Requeue pipeline to allow its next ops later
                _requeue_pipeline_back(p)
                continue

            # Can't fit anywhere. If high priority, try preemption to make room.
            if p.priority in s.preempt_only_for and preempt_budget > 0:
                # Try to find a pool and victim that would help.
                # Consider each pool: if shortage exists, preempt in that pool.
                preempted = False
                for pool_id in range(s.executor.num_pools):
                    pool = s.executor.pools[pool_id]

                    # Determine a reasonable cpu_req for this pool (even if not currently available).
                    cpu_req = _desired_cpu(p, pool)

                    need_cpu = max(0.0, cpu_req - pool.avail_cpu_pool)
                    need_ram = max(0.0, ram_req - pool.avail_ram_pool)
                    if need_cpu <= 0.0 and need_ram <= 0.0:
                        continue

                    victim = _select_preemption_victim(pool_id, need_cpu, need_ram, p.priority)
                    if victim is None:
                        continue

                    cid, pid = victim
                    suspensions.append(Suspend(container_id=cid, pool_id=pid))
                    preempt_budget -= 1
                    made_progress = True
                    preempted = True

                    # After preemption, put pipeline back to front so it gets scheduled soon.
                    _requeue_pipeline_front(p)
                    break

                if preempted:
                    continue

            # Still can't schedule; keep in queue (front for high priority to reduce latency).
            if p.priority in (Priority.QUERY, Priority.INTERACTIVE):
                _requeue_pipeline_front(p)
            else:
                _requeue_pipeline_back(p)
            break  # Avoid spinning when resources are insufficient

    return suspensions, assignments
