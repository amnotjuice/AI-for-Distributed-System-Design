# policy_key: scheduler_iter_worst_simple_017
# context: simple
# model: gpt-5.2-2025-12-11
# llm_cost: 0.051598
# generation_seconds: 47.84
# generated_at: 2026-04-12T00:46:09.161566
@register_scheduler_init(key="scheduler_iter_worst_simple_017")
def scheduler_iter_worst_simple_017_init(s):
    """
    Iteration 2: priority-first + latency pool + SRPT-ish selection + retry-on-failure.

    Changes vs prior attempt (to reduce weighted latency):
      - Hard-reserve pool 0 for QUERY/INTERACTIVE when multiple pools exist (batch only if empty).
      - Do NOT drop pipelines just because they have FAILED ops; instead retry FAILED with RAM backoff.
      - Track container_id -> (pipeline_id, op_keys) so we can learn from failures even if results
        don't include pipeline_id.
      - For QUERY/INTERACTIVE, pick "shortest remaining" pipeline (fewest unfinished operators) to
        reduce weighted completion time under contention (SRPT-like heuristic).
      - For BATCH, keep conservative per-assignment caps and avoid pool 0.
      - Slightly more aggressive CPU for QUERY to cut tail latency.
    """
    from collections import deque

    s.q_query = deque()
    s.q_interactive = deque()
    s.q_batch = deque()

    # pipeline_id -> first time seen tick (for light aging / debugging)
    s.pipeline_enqueued_at = {}

    # (pipeline_id, op_key) -> hint
    s.op_ram_hint = {}
    s.op_cpu_hint = {}

    # container_id -> {"pipeline_id": ..., "pool_id": ..., "op_keys": [...], "priority": ...}
    s.container_map = {}

    s.tick = 0

    # Anti-starvation: after this many ticks, allow batch to run in pool 0 if nothing else fits
    s.batch_pool0_after_ticks = 400

    # Minimum slices (best effort; bounded by availability)
    s.min_cpu_slice = 1
    s.min_ram_slice = 1


@register_scheduler(key="scheduler_iter_worst_simple_017")
def scheduler_iter_worst_simple_017_scheduler(s, results, pipelines):
    """
    Priority-aware scheduler optimizing weighted latency:
      - QUERY/INTERACTIVE always precede BATCH for admission and placement.
      - Dedicated latency pool (pool 0) for QUERY/INTERACTIVE when multiple pools exist.
      - SRPT-ish within QUERY/INTERACTIVE by choosing pipeline with fewest unfinished ops.
      - Retry FAILED ops with RAM backoff; keep CPU modest on OOM to avoid wasting cores.
    """
    s.tick += 1

    from collections import deque

    def _queue_for_priority(prio):
        if prio == Priority.QUERY:
            return s.q_query
        if prio == Priority.INTERACTIVE:
            return s.q_interactive
        return s.q_batch

    def _enqueue_pipeline(p):
        pid = p.pipeline_id
        if pid not in s.pipeline_enqueued_at:
            s.pipeline_enqueued_at[pid] = s.tick
            _queue_for_priority(p.priority).append(p)

    def _drop_pipeline(pid):
        if pid in s.pipeline_enqueued_at:
            del s.pipeline_enqueued_at[pid]
        # We lazily let stale pipeline objects fall out of queues during scanning.

    def _op_key(op):
        if hasattr(op, "op_id"):
            return ("op_id", getattr(op, "op_id"))
        if hasattr(op, "operator_id"):
            return ("operator_id", getattr(op, "operator_id"))
        if hasattr(op, "name"):
            return ("name", getattr(op, "name"))
        return ("py_id", id(op))

    def _pipeline_total_ops(p):
        # Best-effort total operators
        vals = getattr(p, "values", None)
        try:
            return len(vals) if vals is not None else 0
        except Exception:
            return 0

    def _pipeline_unfinished_ops(p):
        st = p.runtime_status()
        total = _pipeline_total_ops(p)
        completed = st.state_counts.get(OperatorState.COMPLETED, 0)
        # If total isn't available, fall back to counting non-completed known states.
        if total <= 0:
            # Sum of all but completed is a rough proxy
            pending = st.state_counts.get(OperatorState.PENDING, 0)
            assigned = st.state_counts.get(OperatorState.ASSIGNED, 0)
            running = st.state_counts.get(OperatorState.RUNNING, 0)
            susp = st.state_counts.get(OperatorState.SUSPENDING, 0)
            failed = st.state_counts.get(OperatorState.FAILED, 0)
            return pending + assigned + running + susp + failed
        rem = total - completed
        return rem if rem > 0 else 0

    def _has_any_high_prio_waiting():
        return len(s.q_query) > 0 or len(s.q_interactive) > 0

    def _get_ready_ops(p):
        st = p.runtime_status()
        # FAILED is re-assignable in Eudoxia (ASSIGNABLE_STATES). Require parents complete.
        ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        if not ops:
            return []
        return ops[:1]

    def _cap_fracs(prio):
        # Caps expressed as fraction of pool max; lower batch caps preserve headroom.
        if prio == Priority.QUERY:
            return 0.95, 0.95
        if prio == Priority.INTERACTIVE:
            return 0.85, 0.90
        return 0.45, 0.55  # BATCH_PIPELINE

    def _default_base(prio, pool):
        # Small but meaningful defaults; QUERY gets more CPU to cut latency.
        if prio == Priority.QUERY:
            base_cpu = min(6, max(1, pool.max_cpu_pool))
            base_ram = min(6, max(1, pool.max_ram_pool))
        elif prio == Priority.INTERACTIVE:
            base_cpu = min(3, max(1, pool.max_cpu_pool))
            base_ram = min(5, max(1, pool.max_ram_pool))
        else:
            base_cpu = min(2, max(1, pool.max_cpu_pool))
            base_ram = min(4, max(1, pool.max_ram_pool))
        return base_cpu, base_ram

    def _size_for_op(p, op, pool_id):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool
        if avail_cpu <= 0 or avail_ram <= 0:
            return 0, 0

        cpu_frac, ram_frac = _cap_fracs(p.priority)
        cpu_cap = max(s.min_cpu_slice, int(pool.max_cpu_pool * cpu_frac))
        ram_cap = max(s.min_ram_slice, int(pool.max_ram_pool * ram_frac))

        base_cpu, base_ram = _default_base(p.priority, pool)

        k = (p.pipeline_id, _op_key(op))
        hinted_cpu = s.op_cpu_hint.get(k, base_cpu)
        hinted_ram = s.op_ram_hint.get(k, base_ram)

        req_cpu = max(base_cpu, hinted_cpu)
        req_ram = max(base_ram, hinted_ram)

        # Clamp to caps and current availability.
        req_cpu = max(s.min_cpu_slice, min(req_cpu, cpu_cap, avail_cpu))
        req_ram = max(s.min_ram_slice, min(req_ram, ram_cap, avail_ram))
        return req_cpu, req_ram

    def _learn_from_result(r):
        # Prefer pipeline_id from our container map; fall back if present on result.
        cid = getattr(r, "container_id", None)
        mapped = s.container_map.get(cid, None) if cid is not None else None
        pid = None
        op_keys = None
        if mapped is not None:
            pid = mapped.get("pipeline_id")
            op_keys = mapped.get("op_keys")
        if pid is None:
            pid = getattr(r, "pipeline_id", None)

        if not (hasattr(r, "failed") and r.failed()):
            # Clear mapping on success to prevent leaks.
            if cid is not None and cid in s.container_map:
                del s.container_map[cid]
            return

        # Determine if error looks like OOM.
        err = str(getattr(r, "error", "") or "")
        e = err.lower()
        oom_like = ("oom" in e) or ("out of memory" in e) or ("memoryerror" in e) or ("killed" in e and "memory" in e)

        # If we cannot attribute to a pipeline/op, still clear container map and stop.
        if pid is None:
            if cid is not None and cid in s.container_map:
                del s.container_map[cid]
            return

        # If we have op_keys from our own assignment map, use them; else derive from r.ops.
        if op_keys is None:
            ops = getattr(r, "ops", []) or []
            op_keys = [_op_key(op) for op in ops]

        prev_ram = int(getattr(r, "ram", 1) or 1)
        prev_cpu = int(getattr(r, "cpu", 1) or 1)

        for ok in op_keys:
            k = (pid, ok)
            cur_ram = s.op_ram_hint.get(k, max(s.min_ram_slice, prev_ram))
            cur_cpu = s.op_cpu_hint.get(k, max(s.min_cpu_slice, prev_cpu))

            if oom_like:
                # Aggressive RAM backoff, CPU stays modest.
                s.op_ram_hint[k] = max(cur_ram, int(max(prev_ram, cur_ram) * 2.5) + 1)
                s.op_cpu_hint[k] = max(s.min_cpu_slice, min(cur_cpu, 2))
            else:
                # Mild bump for generic failures (may still be resource-related).
                s.op_ram_hint[k] = max(cur_ram, int(max(prev_ram, cur_ram) * 1.4) + 1)
                s.op_cpu_hint[k] = max(cur_cpu, int(max(prev_cpu, cur_cpu) * 1.2) + 1)

        # Clear container map entry; future retries will be mapped anew.
        if cid is not None and cid in s.container_map:
            del s.container_map[cid]

    # Ingest new pipelines
    for p in pipelines:
        _enqueue_pipeline(p)

    # Learn from last tick results
    for r in results:
        _learn_from_result(r)

    # Early exit if nothing to do
    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    # Pool selection policy:
    # - If multiple pools: pool 0 is latency pool. Pools 1..n-1 are throughput pools.
    # - In pool 0: schedule QUERY then INTERACTIVE; schedule BATCH only if no high-priority runnable
    #   (or batch has waited a long time and nothing else fits).
    # - In other pools: schedule INTERACTIVE/QUERY first if available, else BATCH.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        if pool.avail_cpu_pool <= 0 or pool.avail_ram_pool <= 0:
            continue

        # Choose candidate queues in order for this pool.
        if s.executor.num_pools > 1 and pool_id == 0:
            candidate_queues = [s.q_query, s.q_interactive]
            # Potentially allow batch in pool0 only if:
            #   (a) no high prio waiting, OR
            #   (b) batch has waited long enough and no runnable high-prio fits.
            allow_batch = (not _has_any_high_prio_waiting())
            if not allow_batch:
                # If batch has waited long, we may allow it later as a last resort.
                # We'll decide after failing to find runnable high-prio.
                pass
        else:
            candidate_queues = [s.q_query, s.q_interactive, s.q_batch]

        chosen = None
        chosen_ops = None

        def _scan_and_pick(q, pick_srpt=False):
            # Bounded scan: traverse queue once, requeue everything.
            best = None
            best_ops = None
            best_score = None  # smaller is better
            n = len(q)
            for _ in range(n):
                p = q.popleft()
                st = p.runtime_status()

                if st.is_pipeline_successful():
                    _drop_pipeline(p.pipeline_id)
                    continue

                ops = _get_ready_ops(p)
                if not ops:
                    q.append(p)
                    continue

                if not pick_srpt:
                    # FIFO: take first runnable
                    q.append(p)
                    return p, ops

                # SRPT-ish: choose minimal unfinished ops, tie-breaker by older enqueue time
                rem = _pipeline_unfinished_ops(p)
                age = s.tick - s.pipeline_enqueued_at.get(p.pipeline_id, s.tick)
                score = (rem, -age)  # prefer smaller rem; if tied, prefer older (larger age)
                if best is None or score < best_score:
                    best = p
                    best_ops = ops
                    best_score = score

                q.append(p)

            return best, best_ops

        # First attempt: pick from candidate queues
        for q in candidate_queues:
            # Use SRPT-ish selection for QUERY/INTERACTIVE to reduce weighted completion time.
            pick_srpt = (q is s.q_query) or (q is s.q_interactive)
            p, ops = _scan_and_pick(q, pick_srpt=pick_srpt)
            if p is not None and ops is not None:
                chosen, chosen_ops = p, ops
                break

        # Special case: pool0 and we didn't find high-prio runnable: allow batch if appropriate.
        if chosen is None and s.executor.num_pools > 1 and pool_id == 0:
            # Check whether batch has waited long enough to be allowed in pool0.
            oldest_batch_age = 0
            for p in list(s.q_batch)[: min(len(s.q_batch), 32)]:
                age = s.tick - s.pipeline_enqueued_at.get(p.pipeline_id, s.tick)
                if age > oldest_batch_age:
                    oldest_batch_age = age
            allow_batch_in_pool0 = (not _has_any_high_prio_waiting()) or (oldest_batch_age >= s.batch_pool0_after_ticks)

            if allow_batch_in_pool0:
                p, ops = _scan_and_pick(s.q_batch, pick_srpt=False)
                if p is not None and ops is not None:
                    chosen, chosen_ops = p, ops

        if chosen is None:
            continue

        # Size resources and emit assignment
        op = chosen_ops[0]
        req_cpu, req_ram = _size_for_op(chosen, op, pool_id)
        if req_cpu <= 0 or req_ram <= 0:
            continue

        assignments.append(
            Assignment(
                ops=chosen_ops,
                cpu=req_cpu,
                ram=req_ram,
                priority=chosen.priority,
                pool_id=pool_id,
                pipeline_id=chosen.pipeline_id,
            )
        )

        # Record mapping for learning from results if pipeline_id isn't present there.
        # We don't know container_id yet; Eudoxia will return it in results. If results
        # include container_id only after launch, we cannot map it now. However, some
        # simulators set container_id deterministically; to be safe, also map by a
        # speculative "pending" entry keyed by (pool_id, pipeline_id, op_key, tick),
        # and reconcile later if container_id appears with ops.
        #
        # Practically, we still benefit when results provide pipeline_id; this is a best-effort.
        # We'll also populate container_map opportunistically if the Assignment gains container_id.
        cid = getattr(assignments[-1], "container_id", None)
        if cid is not None:
            s.container_map[cid] = {
                "pipeline_id": chosen.pipeline_id,
                "pool_id": pool_id,
                "op_keys": [_op_key(o) for o in chosen_ops],
                "priority": chosen.priority,
            }

    return suspensions, assignments
