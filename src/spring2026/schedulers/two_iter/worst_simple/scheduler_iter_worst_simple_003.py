# policy_key: scheduler_iter_worst_simple_003
# context: simple
# model: gpt-5.2-2025-12-11
# llm_cost: 0.057828
# generation_seconds: 38.98
# generated_at: 2026-04-12T00:36:28.125308
@register_scheduler_init(key="scheduler_iter_worst_simple_003")
def scheduler_iter_worst_simple_003_init(s):
    """
    Priority-aware multi-assign packing with conservative batch throttling.

    Changes vs naive FIFO / prior iteration:
      - Always schedule highest priority first (QUERY > INTERACTIVE > BATCH).
      - Pack multiple assignments per pool per tick (not just one) to reduce queueing delay.
      - Keep pool 0 biased toward latency-sensitive work when multiple pools exist.
      - Batch gets smaller CPU/RAM slices to preserve headroom for interactive/query.
      - Simple OOM-ish backoff: if an operator fails with memory-like error, bump its RAM hint.
      - Anti-starvation: very old BATCH pipelines get treated as INTERACTIVE for dispatch order.
    """
    from collections import deque

    s.q_query = deque()
    s.q_interactive = deque()
    s.q_batch = deque()

    # pipeline_id -> tick enqueued (first seen)
    s.enq_tick = {}

    # Per-operator resource hints (best-effort, keyed by op identity; may generalize across pipelines)
    s.op_ram_hint = {}  # op_key -> ram units
    s.op_cpu_hint = {}  # op_key -> cpu units

    s.tick = 0

    # Aging threshold in scheduler ticks
    s.batch_promotion_ticks = 150

    # Minimum slices to avoid pathological tiny allocations
    s.min_cpu = 1
    s.min_ram = 1


@register_scheduler(key="scheduler_iter_worst_simple_003")
def scheduler_iter_worst_simple_003_scheduler(s, results, pipelines):
    """
    Step logic:
      1) Enqueue new pipelines into per-priority queues.
      2) Update per-op resource hints on failures (RAM-first on OOM-like errors).
      3) For each pool, repeatedly pick the best runnable pipeline/op and assign until resources are exhausted:
           - choose highest effective priority (with aging)
           - prefer keeping pool 0 for QUERY/INTERACTIVE when possible
           - size resources by priority + hints:
                QUERY: grab as much CPU as available (finish fast), RAM modest but >= hint
                INTERACTIVE: medium CPU, sufficient RAM
                BATCH: small CPU/RAM slice (throttle), enough for hint, to preserve headroom
    """
    from collections import deque

    s.tick += 1

    # -----------------------
    # Helpers
    # -----------------------
    def op_key(op):
        # Stable-ish identity if available; otherwise Python object identity.
        if hasattr(op, "op_id"):
            return ("op_id", getattr(op, "op_id"))
        if hasattr(op, "operator_id"):
            return ("operator_id", getattr(op, "operator_id"))
        if hasattr(op, "name"):
            return ("name", getattr(op, "name"))
        return ("py_id", id(op))

    def base_queue_for_priority(prio):
        if prio == Priority.QUERY:
            return s.q_query
        if prio == Priority.INTERACTIVE:
            return s.q_interactive
        return s.q_batch

    def ensure_enqueued(p):
        pid = p.pipeline_id
        if pid not in s.enq_tick:
            s.enq_tick[pid] = s.tick
            base_queue_for_priority(p.priority).append(p)

    def drop_pipeline_bookkeeping(pid):
        if pid in s.enq_tick:
            del s.enq_tick[pid]

    def effective_priority(p):
        # Promote old batch to interactive to prevent starvation.
        if p.priority == Priority.BATCH_PIPELINE:
            waited = s.tick - s.enq_tick.get(p.pipeline_id, s.tick)
            if waited >= s.batch_promotion_ticks:
                return Priority.INTERACTIVE
        return p.priority

    def prio_rank(prio):
        if prio == Priority.QUERY:
            return 3
        if prio == Priority.INTERACTIVE:
            return 2
        return 1

    def is_terminal(p):
        st = p.runtime_status()
        done = st.is_pipeline_successful()
        has_failures = st.state_counts.get(OperatorState.FAILED, 0) > 0
        # Keep "obvious first" behavior: drop pipelines that look terminal failed.
        # (Operator-level retries still happen because FAILED is assignable, but if a pipeline
        # keeps failing permanently this prevents infinite churn.)
        return done or False, has_failures

    def ready_ops_one(p):
        st = p.runtime_status()
        ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        if not ops:
            return []
        return ops[:1]

    def pool_preference_ok(pool_id, eff_prio):
        # If multiple pools exist, bias pool 0 to latency-sensitive work.
        if s.executor.num_pools <= 1:
            return True
        if pool_id == 0 and eff_prio == Priority.BATCH_PIPELINE:
            # Prefer batch off pool 0 when other pools exist.
            return False
        return True

    def size_for(pool_id, eff_prio, op):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool
        if avail_cpu <= 0 or avail_ram <= 0:
            return 0, 0

        ok = op_key(op)
        hint_ram = int(s.op_ram_hint.get(ok, s.min_ram))
        hint_cpu = int(s.op_cpu_hint.get(ok, s.min_cpu))

        # Priority-based sizing rules:
        # - For weighted latency: spend CPU on QUERY/INTERACTIVE to finish them quickly.
        # - Throttle BATCH with small slices to preserve headroom and reduce interference.
        if eff_prio == Priority.QUERY:
            # Grab most available CPU; RAM is just enough to run (hint) but don't hog the pool.
            req_cpu = max(s.min_cpu, avail_cpu)
            req_ram = max(s.min_ram, min(avail_ram, max(hint_ram, int(0.25 * pool.max_ram_pool), 1)))
        elif eff_prio == Priority.INTERACTIVE:
            # Medium CPU; modest RAM.
            target_cpu = max(s.min_cpu, int(0.6 * pool.max_cpu_pool))
            req_cpu = max(s.min_cpu, min(avail_cpu, max(hint_cpu, target_cpu)))
            target_ram = max(s.min_ram, int(0.3 * pool.max_ram_pool))
            req_ram = max(s.min_ram, min(avail_ram, max(hint_ram, target_ram)))
        else:
            # Batch: small slice, but at least hints; never more than a small cap.
            cpu_cap = max(s.min_cpu, int(0.25 * pool.max_cpu_pool))
            ram_cap = max(s.min_ram, int(0.35 * pool.max_ram_pool))
            req_cpu = max(s.min_cpu, min(avail_cpu, cpu_cap, max(hint_cpu, s.min_cpu)))
            req_ram = max(s.min_ram, min(avail_ram, ram_cap, max(hint_ram, s.min_ram)))

        return int(req_cpu), int(req_ram)

    # -----------------------
    # Ingest new pipelines
    # -----------------------
    for p in pipelines:
        ensure_enqueued(p)

    if not pipelines and not results:
        return [], []

    # -----------------------
    # Learn from results (RAM-first backoff on OOM-like failures)
    # -----------------------
    for r in results:
        if hasattr(r, "failed") and r.failed():
            err = str(getattr(r, "error", "") or "")
            oom_like = ("oom" in err.lower()) or ("out of memory" in err.lower()) or ("memory" in err.lower())
            ran_ops = getattr(r, "ops", []) or []
            used_ram = int(getattr(r, "ram", s.min_ram) or s.min_ram)
            used_cpu = int(getattr(r, "cpu", s.min_cpu) or s.min_cpu)

            for op in ran_ops:
                ok = op_key(op)
                prev_ram = int(s.op_ram_hint.get(ok, s.min_ram))
                prev_cpu = int(s.op_cpu_hint.get(ok, s.min_cpu))

                if oom_like:
                    # Double RAM hint, keep CPU stable.
                    s.op_ram_hint[ok] = max(prev_ram, max(used_ram * 2, prev_ram + 1))
                    s.op_cpu_hint[ok] = max(prev_cpu, used_cpu)
                else:
                    # Small bump to both for generic under-provisioning.
                    s.op_ram_hint[ok] = max(prev_ram, int(used_ram * 1.25) + 1)
                    s.op_cpu_hint[ok] = max(prev_cpu, int(used_cpu * 1.25) + 1)

    # -----------------------
    # Dispatch: pack multiple assignments per pool
    # -----------------------
    suspensions = []
    assignments = []

    # Iterate pools; in multi-pool, pool 0 will naturally take QUERY/INTERACTIVE first.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool
        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # Keep assigning while we still have resources.
        # We scan queues in priority order; we also do bounded rotations to avoid head-of-line blocking.
        made_progress = True
        while made_progress:
            made_progress = False

            # Recompute availability each loop (we subtract locally as we pack).
            if avail_cpu <= 0 or avail_ram <= 0:
                break

            chosen = None
            chosen_ops = None
            chosen_eff_prio = None
            chosen_queue = None

            # Scan queues in strict effective priority order.
            queues_in_order = (s.q_query, s.q_interactive, s.q_batch)
            for q in queues_in_order:
                # Bounded scan through current queue length.
                qlen = len(q)
                for _ in range(qlen):
                    p = q.popleft()

                    done, failed = is_terminal(p)
                    if done or failed:
                        drop_pipeline_bookkeeping(p.pipeline_id)
                        continue

                    eff_prio = effective_priority(p)

                    # Pool preference: keep pool 0 for non-batch if possible.
                    if not pool_preference_ok(pool_id, eff_prio):
                        # Put it back; maybe another pool will pick it up.
                        q.append(p)
                        continue

                    ops = ready_ops_one(p)
                    if not ops:
                        q.append(p)
                        continue

                    # Select immediately: strict priority order already respected by queue order,
                    # and within queue FIFO-ish due to rotation.
                    chosen = p
                    chosen_ops = ops
                    chosen_eff_prio = eff_prio
                    chosen_queue = q
                    break

                if chosen is not None:
                    break

            if chosen is None:
                break

            # Size request and ensure it fits remaining local availability.
            op = chosen_ops[0]
            req_cpu, req_ram = size_for(pool_id, chosen_eff_prio, op)

            # If request doesn't fit, try to downsize (only for batch/interactive), else give up this pool loop.
            if req_cpu > avail_cpu or req_ram > avail_ram:
                if chosen_eff_prio == Priority.BATCH_PIPELINE:
                    req_cpu = max(s.min_cpu, min(avail_cpu, req_cpu))
                    req_ram = max(s.min_ram, min(avail_ram, req_ram))
                elif chosen_eff_prio == Priority.INTERACTIVE:
                    # Allow interactive to shrink slightly to fit; queries prefer to wait for a bigger slot.
                    req_cpu = max(s.min_cpu, min(avail_cpu, req_cpu))
                    req_ram = max(s.min_ram, min(avail_ram, req_ram))
                else:
                    # Put it back and stop packing this pool for now.
                    base_queue_for_priority(chosen.priority).append(chosen)
                    break

            if req_cpu <= 0 or req_ram <= 0 or req_cpu > avail_cpu or req_ram > avail_ram:
                base_queue_for_priority(chosen.priority).append(chosen)
                break

            assignments.append(
                Assignment(
                    ops=chosen_ops,
                    cpu=req_cpu,
                    ram=req_ram,
                    priority=chosen.priority,  # preserve original priority for accounting
                    pool_id=pool_id,
                    pipeline_id=chosen.pipeline_id,
                )
            )

            # Requeue pipeline for future ops.
            base_queue_for_priority(chosen.priority).append(chosen)

            # Update local availability for packing more work into this pool this tick.
            avail_cpu -= req_cpu
            avail_ram -= req_ram
            made_progress = True

    return suspensions, assignments
