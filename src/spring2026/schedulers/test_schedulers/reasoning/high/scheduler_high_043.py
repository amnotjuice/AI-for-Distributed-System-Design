# policy_key: scheduler_high_043
# reasoning_effort: high
# exp: reasoning
# model: gpt-5.2-2025-12-11
# llm_cost: 0.175599
# generation_seconds: 211.24
# generated_at: 2026-04-10T03:16:49.665963
@register_scheduler_init(key="scheduler_high_043")
def scheduler_high_043_init(s):
    """
    Priority-first, failure-averse scheduler.

    Goals (tuned for the objective):
      - Strongly protect QUERY/INTERACTIVE latency (dominant weights) via strict priority + reserved headroom.
      - Avoid 720s penalties by reacting to OOM with per-operator RAM backoff and retries.
      - Prevent indefinite starvation with simple aging-based "force service" for INTERACTIVE/BATCH.
      - Keep design simple/robust: no preemption (only admission/placement/sizing).
    """
    from collections import deque

    s.tick = 0

    # Per-priority pipeline queues (store pipeline_id).
    s.queues = {
        Priority.QUERY: deque(),
        Priority.INTERACTIVE: deque(),
        Priority.BATCH_PIPELINE: deque(),
    }

    # Track known pipelines and basic metadata.
    s.pipelines = {}  # pipeline_id -> Pipeline
    s.pipeline_enqueue_tick = {}  # pipeline_id -> tick when first seen
    s.pipeline_last_service_tick = {}  # pipeline_id -> last tick we assigned any op
    s.in_queue = set()  # pipeline_ids present in some queue

    # Mark pipelines we consider unrecoverable (e.g., non-OOM failure, or repeated OOM at max RAM).
    s.pipeline_dead = set()

    # Per-operator (by object identity) resource hints to reduce OOM risk.
    s.op_to_pipeline = {}         # op_obj_id -> pipeline_id
    s.op_ram_hint = {}            # op_obj_id -> suggested RAM for next attempt
    s.op_oom_failures = {}        # op_obj_id -> count of OOM failures
    s.op_non_oom_failure = set()  # op_obj_id that failed for non-OOM reasons (don't retry)

    # Tunables (conservative for high completion rate).
    s.scan_limit = 32  # how many items to scan per priority queue to find a ready/fit candidate

    # OOM backoff behavior.
    s.max_oom_retries = 4
    s.oom_ram_growth = 2.0  # multiplicative increase on OOM

    # Headroom reservation to avoid filling pools with batch when high-priority is pending.
    s.reserve_frac_cpu_for_hi = 0.30
    s.reserve_frac_ram_for_hi = 0.30

    # Limit how many new assignments per pool per scheduler step (reduce fragmentation).
    s.max_assign_pool0 = 1
    s.max_assign_other = 2

    # Aging thresholds (in scheduler steps) to prevent starvation.
    s.force_interactive_after = 25
    s.force_batch_after = 120

    # Resource sizing fractions by priority (relative to pool max). RAM is conservative to avoid OOM.
    s.cpu_frac = {
        Priority.QUERY: 0.80,
        Priority.INTERACTIVE: 0.60,
        Priority.BATCH_PIPELINE: 0.40,
    }
    s.ram_frac = {
        Priority.QUERY: 0.70,
        Priority.INTERACTIVE: 0.60,
        Priority.BATCH_PIPELINE: 0.50,
    }


@register_scheduler(key="scheduler_high_043")
def scheduler_high_043(s, results: List["ExecutionResult"],
                       pipelines: List["Pipeline"]) -> Tuple[List["Suspend"], List["Assignment"]]:
    def _is_oom(err) -> bool:
        if err is None:
            return False
        msg = str(err).lower()
        return ("oom" in msg) or ("out of memory" in msg) or ("out-of-memory" in msg) or ("memory" in msg and "out" in msg)

    def _remaining_ops(status) -> int:
        # Approximate remaining work using state counts we know exist.
        return (
            status.state_counts.get(OperatorState.PENDING, 0)
            + status.state_counts.get(OperatorState.ASSIGNED, 0)
            + status.state_counts.get(OperatorState.RUNNING, 0)
            + status.state_counts.get(OperatorState.SUSPENDING, 0)
            + status.state_counts.get(OperatorState.FAILED, 0)
        )

    def _service_age(pid: str) -> int:
        last = s.pipeline_last_service_tick.get(pid, s.pipeline_enqueue_tick.get(pid, s.tick))
        return max(0, s.tick - last)

    def _has_pending_hi() -> bool:
        return (len(s.queues[Priority.QUERY]) > 0) or (len(s.queues[Priority.INTERACTIVE]) > 0)

    def _calc_request(priority, pool, avail_cpu, avail_ram, op_obj_id, reserve_cpu=0.0, reserve_ram=0.0):
        # Compute desired resources, then clamp to what's allocatable (optionally keeping headroom).
        max_cpu = float(pool.max_cpu_pool)
        max_ram = float(pool.max_ram_pool)

        alloc_cpu_cap = max(0.0, float(avail_cpu) - float(reserve_cpu))
        alloc_ram_cap = max(0.0, float(avail_ram) - float(reserve_ram))
        if alloc_cpu_cap <= 0.0 or alloc_ram_cap <= 0.0:
            return 0.0, 0.0

        desired_cpu = max_cpu * float(s.cpu_frac.get(priority, 0.5))
        desired_ram = max_ram * float(s.ram_frac.get(priority, 0.5))

        # If we have an OOM-driven hint, honor it (strongly reduces 720s penalties).
        hint_ram = float(s.op_ram_hint.get(op_obj_id, 0.0))
        if hint_ram > 0.0:
            desired_ram = max(desired_ram, hint_ram)

        # Clamp to allocatable capacity.
        cpu_req = min(alloc_cpu_cap, desired_cpu)
        ram_req = min(alloc_ram_cap, desired_ram)

        # Avoid microscopic allocations.
        if cpu_req <= 0.0 or ram_req <= 0.0:
            return 0.0, 0.0
        return cpu_req, ram_req

    def _try_pick_and_assign(priority, pool_id, pool, avail_cpu, avail_ram, inflight_pids, reserve_cpu=0.0, reserve_ram=0.0):
        """
        Scan a bounded number of pipelines in the priority queue, select the best ready+fit candidate,
        and return (Assignment, cpu_used, ram_used) or (None, 0, 0).
        """
        q = s.queues[priority]
        if not q:
            return None, 0.0, 0.0

        scanned = []
        kept = []
        candidates = []  # (metric, pid, op_list, cpu_req, ram_req)

        limit = min(len(q), int(s.scan_limit))
        for _ in range(limit):
            pid = q.popleft()
            scanned.append(pid)

            pipeline = s.pipelines.get(pid)
            if pipeline is None:
                continue
            if pid in s.pipeline_dead:
                continue

            status = pipeline.runtime_status()
            if status.is_pipeline_successful():
                # Drop completed pipelines eagerly.
                s.in_queue.discard(pid)
                s.pipelines.pop(pid, None)
                s.pipeline_enqueue_tick.pop(pid, None)
                s.pipeline_last_service_tick.pop(pid, None)
                continue

            # Keep in queue unless we later decide to drop it.
            kept.append(pid)

            if pid in inflight_pids:
                continue

            # If there are FAILED ops due to non-OOM, stop spending resources.
            # (We detect via per-op marks; pipeline-level marking happens in result processing.)
            if pid in s.pipeline_dead:
                continue

            op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
            if not op_list:
                continue

            op = op_list[0]
            op_obj_id = id(op)

            # If this op previously failed for non-OOM reasons, don't retry it.
            if op_obj_id in s.op_non_oom_failure:
                s.pipeline_dead.add(pid)
                continue

            cpu_req, ram_req = _calc_request(priority, pool, avail_cpu, avail_ram, op_obj_id,
                                             reserve_cpu=reserve_cpu, reserve_ram=reserve_ram)
            if cpu_req <= 0.0 or ram_req <= 0.0:
                continue
            if cpu_req > float(avail_cpu) or ram_req > float(avail_ram):
                continue

            # Metric:
            # - QUERY/INTERACTIVE: prefer short remaining (reduce mean/p95 latency), tie-break by older service age.
            # - BATCH: prefer older service age (avoid starvation), tie-break by shorter remaining.
            rem = _remaining_ops(status)
            age = _service_age(pid)
            if priority == Priority.BATCH_PIPELINE:
                metric = (-age, rem)
            else:
                metric = (rem, -age)

            candidates.append((metric, pid, op_list, cpu_req, ram_req))

        if not candidates:
            # Rebuild queue as-is (drop nothing besides completed/dead handled above).
            for pid in kept:
                q.append(pid)
            return None, 0.0, 0.0

        # Choose the best candidate.
        candidates.sort(key=lambda x: x[0])
        _, best_pid, best_ops, best_cpu, best_ram = candidates[0]

        # Rebuild queue, moving chosen pid to the end for mild fairness.
        for pid in kept:
            if pid != best_pid and pid not in s.pipeline_dead and pid in s.pipelines:
                q.append(pid)
        if best_pid not in s.pipeline_dead and best_pid in s.pipelines:
            q.append(best_pid)

        # Create assignment.
        inflight_pids.add(best_pid)
        s.pipeline_last_service_tick[best_pid] = s.tick

        # Remember which pipeline these ops belong to (for interpreting results).
        for op in best_ops:
            s.op_to_pipeline[id(op)] = best_pid

        assignment = Assignment(
            ops=best_ops,
            cpu=best_cpu,
            ram=best_ram,
            priority=priority,
            pool_id=pool_id,
            pipeline_id=best_pid,
        )
        return assignment, float(best_cpu), float(best_ram)

    # ----------------------------
    # Advance scheduler "time"
    # ----------------------------
    s.tick += 1

    # ----------------------------
    # Process execution results (learn from OOMs, mark unrecoverable pipelines)
    # ----------------------------
    for r in results:
        if not r.failed():
            continue

        oom = _is_oom(getattr(r, "error", None))
        pool = s.executor.pools[r.pool_id]
        max_ram = float(pool.max_ram_pool)

        for op in getattr(r, "ops", []) or []:
            op_id = id(op)
            pid = s.op_to_pipeline.get(op_id)

            if oom:
                s.op_oom_failures[op_id] = int(s.op_oom_failures.get(op_id, 0)) + 1

                # Increase RAM hint aggressively; objective heavily penalizes failures.
                prev_hint = float(s.op_ram_hint.get(op_id, 0.0))
                last_ram = float(getattr(r, "ram", 0.0) or 0.0)
                new_hint = max(prev_hint, last_ram * float(s.oom_ram_growth), 0.50 * max_ram)

                # Cap at pool max; if we still OOM near max, mark pipeline dead.
                new_hint = min(new_hint, max_ram)
                s.op_ram_hint[op_id] = new_hint

                # If repeated OOMs or already at (near) max RAM, consider unrecoverable.
                if pid is not None:
                    if s.op_oom_failures[op_id] >= int(s.max_oom_retries) or last_ram >= 0.95 * max_ram:
                        s.pipeline_dead.add(pid)
            else:
                # Non-OOM failures are likely deterministic; don't burn time retrying.
                s.op_non_oom_failure.add(op_id)
                if pid is not None:
                    s.pipeline_dead.add(pid)

    # ----------------------------
    # Admit new pipelines
    # ----------------------------
    for p in pipelines:
        pid = p.pipeline_id
        if pid in s.pipelines:
            # Already known (defensive).
            continue

        s.pipelines[pid] = p
        s.pipeline_enqueue_tick[pid] = s.tick
        s.pipeline_last_service_tick[pid] = s.tick  # starts "fresh"
        if pid not in s.in_queue:
            s.queues[p.priority].append(pid)
            s.in_queue.add(pid)

    # Early exit if no state changes that could unlock new work.
    # (If nothing arrived and nothing finished/failed, available resources are unchanged.)
    if not pipelines and not results:
        return [], []

    # ----------------------------
    # Build assignments across pools
    # ----------------------------
    suspensions: List["Suspend"] = []
    assignments: List["Assignment"] = []

    pending_hi = _has_pending_hi()

    # Precompute oldest ages for force-service logic.
    def _oldest_age_in_queue(priority) -> int:
        q = s.queues[priority]
        if not q:
            return 0
        # Scan a few to approximate "oldest" without O(n).
        oldest = 0
        cnt = 0
        for pid in list(q)[:min(len(q), 64)]:
            if pid in s.pipeline_dead:
                continue
            oldest = max(oldest, _service_age(pid))
            cnt += 1
            if cnt >= 64:
                break
        return oldest

    oldest_interactive = _oldest_age_in_queue(Priority.INTERACTIVE)
    oldest_batch = _oldest_age_in_queue(Priority.BATCH_PIPELINE)

    # Per-tick soft cap: don't let batch take too many "first chances" when high-priority exists.
    batch_assignments_this_step = 0

    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)
        if avail_cpu <= 0.0 or avail_ram <= 0.0:
            continue

        max_assign = int(s.max_assign_pool0 if pool_id == 0 else s.max_assign_other)
        inflight_pids = set()  # within this scheduler step, avoid double-assigning same pipeline across pools

        for _ in range(max_assign):
            if avail_cpu <= 0.0 or avail_ram <= 0.0:
                break

            # Reserve headroom whenever high priority is waiting, to avoid batch clogging.
            reserve_cpu = float(pool.max_cpu_pool) * float(s.reserve_frac_cpu_for_hi) if pending_hi else 0.0
            reserve_ram = float(pool.max_ram_pool) * float(s.reserve_frac_ram_for_hi) if pending_hi else 0.0

            # Force-service logic to prevent starvation:
            force_interactive = (oldest_interactive >= int(s.force_interactive_after)) and len(s.queues[Priority.INTERACTIVE]) > 0
            force_batch = (oldest_batch >= int(s.force_batch_after)) and len(s.queues[Priority.BATCH_PIPELINE]) > 0

            # Pool 0 is a "fast lane": avoid batch there unless no high priority is waiting.
            allow_batch_here = (pool_id != 0) or (not pending_hi)

            # Priority choice:
            chosen_prio = None
            if force_interactive:
                chosen_prio = Priority.INTERACTIVE
            elif len(s.queues[Priority.QUERY]) > 0:
                chosen_prio = Priority.QUERY
            elif len(s.queues[Priority.INTERACTIVE]) > 0:
                chosen_prio = Priority.INTERACTIVE
            elif allow_batch_here and len(s.queues[Priority.BATCH_PIPELINE]) > 0:
                chosen_prio = Priority.BATCH_PIPELINE

            # If batch is starving, allow one batch even while interactive/query exist, but keep headroom.
            if chosen_prio is None and allow_batch_here and force_batch:
                chosen_prio = Priority.BATCH_PIPELINE

            if chosen_prio is None:
                break

            # Apply headroom reservation only to batch; give hi-priority full access to available resources.
            use_reserve_cpu = reserve_cpu if (chosen_prio == Priority.BATCH_PIPELINE and pending_hi) else 0.0
            use_reserve_ram = reserve_ram if (chosen_prio == Priority.BATCH_PIPELINE and pending_hi) else 0.0

            # Additional throttle: when high priority exists, limit batch assignments per scheduler step.
            if chosen_prio == Priority.BATCH_PIPELINE and pending_hi and batch_assignments_this_step >= 1:
                break

            a, cpu_used, ram_used = _try_pick_and_assign(
                chosen_prio, pool_id, pool, avail_cpu, avail_ram, inflight_pids,
                reserve_cpu=use_reserve_cpu, reserve_ram=use_reserve_ram
            )
            if a is None:
                # If chosen priority couldn't find a ready+fit op, try next best once (avoid stalling pool).
                fallback_order = []
                if chosen_prio == Priority.QUERY:
                    fallback_order = [Priority.INTERACTIVE, Priority.BATCH_PIPELINE]
                elif chosen_prio == Priority.INTERACTIVE:
                    fallback_order = [Priority.QUERY, Priority.BATCH_PIPELINE]
                else:
                    fallback_order = [Priority.QUERY, Priority.INTERACTIVE]

                tried = False
                for fp in fallback_order:
                    if fp == Priority.BATCH_PIPELINE and (not allow_batch_here):
                        continue
                    if fp == Priority.BATCH_PIPELINE and pending_hi and batch_assignments_this_step >= 1:
                        continue

                    use_res_cpu2 = reserve_cpu if (fp == Priority.BATCH_PIPELINE and pending_hi) else 0.0
                    use_res_ram2 = reserve_ram if (fp == Priority.BATCH_PIPELINE and pending_hi) else 0.0

                    a, cpu_used, ram_used = _try_pick_and_assign(
                        fp, pool_id, pool, avail_cpu, avail_ram, inflight_pids,
                        reserve_cpu=use_res_cpu2, reserve_ram=use_res_ram2
                    )
                    tried = True
                    if a is not None:
                        chosen_prio = fp
                        break

                if a is None:
                    # Nothing ready/fit for this pool right now.
                    break

            assignments.append(a)
            avail_cpu -= float(cpu_used)
            avail_ram -= float(ram_used)

            if chosen_prio == Priority.BATCH_PIPELINE:
                batch_assignments_this_step += 1

    return suspensions, assignments
