# policy_key: scheduler_est_047
# reasoning_effort: low
# exp: estimation
# model: gpt-5.2-2025-12-11
# llm_cost: 0.059150
# generation_seconds: 50.24
# generated_at: 2026-04-10T10:23:41.890600
@register_scheduler_init(key="scheduler_est_047")
def scheduler_est_047_init(s):
    """Priority- and memory-aware scheduler with OOM-avoidance and bounded retries.

    Key ideas:
    - Maintain separate FIFO queues per priority (QUERY > INTERACTIVE > BATCH).
    - Prefer scheduling "ready" operators that (estimated) fit into pool RAM to reduce OOMs.
    - For noisy/None memory estimates, be conservative for high priorities (allocate more headroom).
    - On failures, retry a limited number of times with increased RAM (assume many failures are OOM-like).
    - Avoid head-of-line blocking by scanning within each priority queue for a feasible candidate.
    """
    from collections import deque

    s.queues = {
        Priority.QUERY: deque(),
        Priority.INTERACTIVE: deque(),
        Priority.BATCH_PIPELINE: deque(),
    }

    # Metadata for lightweight "aging" (oldest-first within priority).
    s._time = 0
    s.pipeline_enqueued_at = {}  # pipeline_id -> time

    # Retry state (keyed by (pipeline_id, op_obj_id)).
    s.op_ram_override_gb = {}  # (pipeline_id, id(op)) -> ram_gb
    s.pipeline_fail_count = {}  # pipeline_id -> int


@register_scheduler(key="scheduler_est_047")
def scheduler_est_047_scheduler(s, results, pipelines):
    from collections import deque

    # ---- Helpers ----
    def _priority_weight(pri):
        # Used only for internal heuristics.
        if pri == Priority.QUERY:
            return 3
        if pri == Priority.INTERACTIVE:
            return 2
        return 1

    def _safety_factor(pri):
        # RAM headroom multiplier applied to noisy memory estimates.
        # Larger for higher priorities to reduce catastrophic OOM/fail penalties.
        if pri == Priority.QUERY:
            return 1.35
        if pri == Priority.INTERACTIVE:
            return 1.25
        return 1.15

    def _cpu_share(pri):
        # Favor high priority latency without starving batch.
        if pri == Priority.QUERY:
            return 0.80
        if pri == Priority.INTERACTIVE:
            return 0.65
        return 0.50

    def _min_cpu(pri):
        if pri == Priority.QUERY:
            return 2
        if pri == Priority.INTERACTIVE:
            return 1
        return 1

    def _get_ready_ops(pipeline):
        status = pipeline.runtime_status()
        # Only schedule ops whose parents are complete (keeps DAG correctness).
        return status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)

    def _pipeline_done_or_dead(pipeline):
        status = pipeline.runtime_status()
        if status.is_pipeline_successful():
            return True
        # If any ops are FAILED, we still may want to retry (OOM) unless too many failures.
        # So this helper does not treat FAILED as terminal by itself.
        return False

    def _op_est_mem_gb(op):
        est = getattr(op, "estimate", None)
        if est is None:
            return None
        return getattr(est, "mem_peak_gb", None)

    def _desired_ram_gb(pool, pipeline, op):
        """Compute RAM request for this op in this pool (<= pool.max_ram_pool), before clamping to availability."""
        pri = pipeline.priority
        key = (pipeline.pipeline_id, id(op))
        if key in s.op_ram_override_gb:
            return min(pool.max_ram_pool, float(s.op_ram_override_gb[key]))

        est_mem = _op_est_mem_gb(op)
        sf = _safety_factor(pri)

        if est_mem is None:
            # Unknown memory: allocate conservatively (more for high priority).
            # We avoid "tiny" allocations that increase failure risk.
            frac = 0.70 if pri == Priority.QUERY else (0.60 if pri == Priority.INTERACTIVE else 0.50)
            return min(pool.max_ram_pool, max(1.0, pool.max_ram_pool * frac))

        # Estimated memory known: add headroom, and also avoid too-small allocations.
        req = max(1.0, float(est_mem) * sf)
        # For batch, allow slightly tighter packing; still keep at least 1GB.
        return min(pool.max_ram_pool, req)

    def _feasible_in_pool(pool, pipeline, op):
        """Feasibility check against pool availability (not max)."""
        need_ram = _desired_ram_gb(pool, pipeline, op)
        # Need at least some CPU too.
        need_cpu = _min_cpu(pipeline.priority)
        return (pool.avail_ram_pool >= need_ram) and (pool.avail_cpu_pool >= need_cpu)

    def _score_candidate(pool, pipeline, op):
        """Lower is better. Prefer high-priority, older, smaller memory ops that fit."""
        pri = pipeline.priority
        age = s._time - s.pipeline_enqueued_at.get(pipeline.pipeline_id, s._time)
        est_mem = _op_est_mem_gb(op)
        # Treat None as "large-ish" to avoid risky placements when others available.
        mem_term = float(est_mem) if est_mem is not None else (0.90 * pool.max_ram_pool)
        # Prefer older within same priority; prefer smaller memory to pack more and avoid OOM.
        # Strongly prioritize priority class.
        return (-1000 * _priority_weight(pri), -age, mem_term)

    def _enqueue_pipeline(p):
        # Avoid duplicating a pipeline in the queue.
        # (We don't maintain a full membership set to keep it simple; tolerate rare duplicates by filtering later.)
        if p.pipeline_id not in s.pipeline_enqueued_at:
            s.pipeline_enqueued_at[p.pipeline_id] = s._time
        s.queues[p.priority].append(p)

    def _pick_candidate_for_pool(pool):
        """Pick a single (pipeline, op) to schedule in this pool, scanning queues to avoid HoL blocking."""
        best = None  # (score_tuple, pri, idx, pipeline, op)
        # Search in priority order; scan only a bounded number to keep scheduler light.
        for pri in (Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE):
            q = s.queues[pri]
            if not q:
                continue
            scan_limit = min(len(q), 32)  # small bounded scan to avoid O(n) blowups
            # We do not pop here; we only find the best index, then pop that specific element.
            for i in range(scan_limit):
                pipeline = q[i]
                if _pipeline_done_or_dead(pipeline):
                    continue
                # Bound retries at pipeline-level (non-transient errors would otherwise churn).
                if s.pipeline_fail_count.get(pipeline.pipeline_id, 0) >= 3:
                    continue
                ready_ops = _get_ready_ops(pipeline)
                if not ready_ops:
                    continue

                # Choose one op per pipeline (greedy): smallest estimated memory among ready ops.
                chosen_op = None
                chosen_mem = None
                for op in ready_ops:
                    mem = _op_est_mem_gb(op)
                    # None treated as large for selection (but still schedulable).
                    mem_val = float(mem) if mem is not None else float("inf")
                    if chosen_op is None or mem_val < chosen_mem:
                        chosen_op, chosen_mem = op, mem_val

                if chosen_op is None:
                    continue
                if not _feasible_in_pool(pool, pipeline, chosen_op):
                    continue

                score = _score_candidate(pool, pipeline, chosen_op)
                if best is None or score > best[0]:
                    best = (score, pri, i, pipeline, chosen_op)

            # If we found a QUERY candidate, don't consider lower priorities for this pool allocation round.
            if best is not None and best[1] == Priority.QUERY:
                break

        if best is None:
            return None
        _, pri, idx, pipeline, op = best
        return pri, idx, pipeline, op

    def _remove_from_queue_by_index(pri, idx):
        q = s.queues[pri]
        # deque does not support efficient middle removal; rebuild small deques.
        if idx == 0:
            return q.popleft()
        tmp = deque()
        chosen = None
        for i in range(len(q)):
            item = q.popleft()
            if i == idx:
                chosen = item
            else:
                tmp.append(item)
        s.queues[pri] = tmp
        return chosen

    # ---- Update time and ingest arrivals ----
    s._time += 1
    for p in pipelines:
        _enqueue_pipeline(p)

    # ---- Process results: bump RAM on failures and re-enqueue pipelines ----
    # Many simulation setups fail ops mainly due to OOM when under-provisioned; we treat failures as retryable
    # up to a small bound, increasing RAM aggressively to reduce repeat failures (which are heavily penalized).
    for r in results:
        if not r.failed():
            continue

        # Increment pipeline-level failure count.
        pid = r.pipeline_id if hasattr(r, "pipeline_id") else None
        if pid is not None:
            s.pipeline_fail_count[pid] = s.pipeline_fail_count.get(pid, 0) + 1

        # Increase RAM override for failed ops (if we can map to pipeline_id).
        # If pipeline_id is not present on result, we still attempt to set overrides by scanning r.ops.
        for op in getattr(r, "ops", []) or []:
            # Best-effort pipeline_id resolution: prefer r.pipeline_id; else try op.pipeline_id if exists.
            op_pid = pid
            if op_pid is None and hasattr(op, "pipeline_id"):
                op_pid = op.pipeline_id
            if op_pid is None:
                continue

            # Increase requested RAM sharply; clamp later to pool max and availability.
            est_mem = _op_est_mem_gb(op)
            base = float(r.ram) if getattr(r, "ram", None) is not None else 1.0
            # If estimate exists and is larger than what we gave, jump toward it; otherwise double.
            target = max(base * 2.0, (float(est_mem) * 1.35) if est_mem is not None else 0.0, 1.0)
            s.op_ram_override_gb[(op_pid, id(op))] = target

        # Re-enqueue the pipeline to make progress unless it exceeded retry bound.
        if pid is not None:
            # Try to find the pipeline object in queues or in arrivals list; if not found, do nothing.
            # This is best-effort; the simulator typically keeps the same Pipeline objects flowing.
            found = None
            for pri in (Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE):
                for p in s.queues[pri]:
                    if p.pipeline_id == pid:
                        found = p
                        break
                if found is not None:
                    break
            if found is not None and s.pipeline_fail_count.get(pid, 0) < 3:
                # Move it toward the front of its priority queue to reduce repeated penalty.
                # (Rebuild queue without duplicates of this pipeline_id within scanned window.)
                pri = found.priority
                q = s.queues[pri]
                newq = deque()
                newq.appendleft(found)
                # Keep others, skipping one instance of this pipeline if it appears again.
                skipped = False
                for p in q:
                    if p.pipeline_id == pid and not skipped:
                        skipped = True
                        continue
                    newq.append(p)
                s.queues[pri] = newq

    # Early exit if nothing changed that would affect decisions.
    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    # ---- Scheduling loop: fill each pool with as many single-op assignments as resources allow ----
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]

        # Keep placing ops while we have meaningful resources.
        # Use small loop bounds to prevent pathological scheduling time.
        for _ in range(64):
            if pool.avail_cpu_pool <= 0 or pool.avail_ram_pool <= 0:
                break

            picked = _pick_candidate_for_pool(pool)
            if picked is None:
                break

            pri, idx, pipeline, op = picked

            # Pop the selected pipeline from its queue (we'll requeue it if it still has remaining work later).
            chosen_pipeline = _remove_from_queue_by_index(pri, idx)

            # Re-check completion (pipeline might have finished since it was queued).
            if _pipeline_done_or_dead(chosen_pipeline):
                continue
            if s.pipeline_fail_count.get(chosen_pipeline.pipeline_id, 0) >= 3:
                continue

            # Compute requested resources and clamp to availability.
            need_ram = _desired_ram_gb(pool, chosen_pipeline, op)
            # Clamp to availability; if we can't meet minimum CPU, give up for this pool this round.
            min_cpu = _min_cpu(chosen_pipeline.priority)
            if pool.avail_cpu_pool < min_cpu:
                # Put pipeline back (unchanged) and stop scheduling in this pool.
                s.queues[pri].appendleft(chosen_pipeline)
                break

            # If RAM doesn't fit now, requeue and stop (avoid spinning).
            if pool.avail_ram_pool < need_ram:
                s.queues[pri].appendleft(chosen_pipeline)
                break

            cpu_req = min(
                pool.avail_cpu_pool,
                max(min_cpu, int(round(pool.max_cpu_pool * _cpu_share(chosen_pipeline.priority))))
            )
            # Ensure cpu_req does not exceed available and is >= 1.
            cpu_req = max(1, min(cpu_req, pool.avail_cpu_pool))

            ram_req = min(pool.avail_ram_pool, need_ram)

            # Create assignment with a single op per container to reduce interference and OOM risk.
            assignments.append(
                Assignment(
                    ops=[op],
                    cpu=cpu_req,
                    ram=ram_req,
                    priority=chosen_pipeline.priority,
                    pool_id=pool_id,
                    pipeline_id=chosen_pipeline.pipeline_id,
                )
            )

            # Re-enqueue the pipeline to allow subsequent operators to be scheduled later.
            # We append to the right to preserve FIFO/aging within priority.
            s.queues[chosen_pipeline.priority].append(chosen_pipeline)

    return suspensions, assignments
