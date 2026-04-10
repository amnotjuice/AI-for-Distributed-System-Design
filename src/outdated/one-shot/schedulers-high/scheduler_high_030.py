# policy_key: scheduler_high_030
# reasoning_effort: high
# model: gpt-5.2-2025-12-11
# llm_cost: 0.093995
# generation_seconds: 113.87
# generated_at: 2026-03-14T03:51:01.339870
@register_scheduler_init(key="scheduler_high_030")
def scheduler_high_030_init(s):
    """
    Priority-aware FIFO with small, low-risk improvements over naive FIFO:

    1) Queueing: maintain separate round-robin queues per priority (QUERY > INTERACTIVE > BATCH).
       This fixes the obvious flaw where batch work can sit in front of interactive/query work.

    2) Headroom for latency: when high-priority work is waiting, cap per-assignment resources for
       batch so it doesn't monopolize an entire pool, leaving capacity to start high-priority ops.

    3) OOM-aware retry sizing: on OOM failures, increase a per-(pipeline,op) RAM hint and retry;
       avoid the naive behavior of dropping pipelines on any failure.

    Notes:
    - No preemption is used (keeps churn low and avoids relying on unknown executor introspection).
    - Schedules at most one new op per pipeline per tick (reduces bursty hogging and improves fairness).
    """
    from collections import deque

    s.queues = {
        Priority.QUERY: deque(),
        Priority.INTERACTIVE: deque(),
        Priority.BATCH_PIPELINE: deque(),
    }

    # Track all pipelines we've seen, so we can keep scheduling them even when not re-submitted.
    s.known_pipelines = {}

    # Per-op RAM sizing hints (keyed by (pipeline_id, op_key_string)).
    s.op_ram_hint = {}

    # Per-op failure counts to stop infinite retry on non-OOM errors.
    s.op_fail_count = {}

    # Drop-list for pipelines that have exceeded retry budgets on non-OOM errors.
    s.dropped_pipeline_ids = set()

    # Tunables (kept conservative; incrementally better than naive)
    s.max_non_oom_retries = 2
    s.oom_ram_growth = 1.6          # multiply RAM hint on OOM
    s.initial_ram_share = 0.45      # initial hint as share of pool max RAM if unknown
    s.min_high_ram_share = 0.30     # ensure high-priority ops get a decent starting hint
    s.batch_cap_share_when_high_waiting = 0.55  # cap batch CPU/RAM share if high priority is waiting
    s.high_cpu_share = 0.60         # typical CPU share for high-priority ops per assignment
    s.high_ram_share = 0.55         # typical RAM share for high-priority ops per assignment

    # To reduce head-of-line blocking, scan up to N items when searching for schedulable work.
    s.max_queue_scan = 32


@register_scheduler(key="scheduler_high_030")
def scheduler_high_030_scheduler(s, results, pipelines):
    """
    See init docstring for policy overview.

    Implementation details:
    - New pipelines are appended to the queue for their priority.
    - Execution results update OOM RAM hints and non-OOM retry counts.
    - For each pool, we greedily pack multiple assignments while resources remain.
    - Selection is priority-first with round-robin fairness within priority.
    - Each pipeline receives at most one new assignment per scheduler tick.
    """
    def _is_oom_error(err):
        if not err:
            return False
        try:
            u = str(err).lower()
        except Exception:
            return False
        return ("oom" in u) or ("out of memory" in u) or ("memoryerror" in u)

    def _op_key(op):
        # Use the most stable identifier available; fall back to stringification.
        oid = getattr(op, "op_id", None)
        if oid is not None:
            return f"op_id:{oid}"
        name = getattr(op, "name", None)
        if name is not None:
            return f"name:{name}"
        return f"repr:{str(op)}"

    def _high_waiting():
        return (len(s.queues[Priority.QUERY]) + len(s.queues[Priority.INTERACTIVE])) > 0

    def _priority_order():
        return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]

    def _pool_priority_order(pool_id):
        # If multiple pools exist, gently bias pool 0 toward low-latency work.
        # Still allow spillover so we don't idle capacity.
        if s.executor.num_pools <= 1:
            return _priority_order()
        if pool_id == 0:
            return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]
        # Background pools prefer batch, but will take high-priority if present.
        if _high_waiting():
            return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]
        return [Priority.BATCH_PIPELINE, Priority.INTERACTIVE, Priority.QUERY]

    def _target_shares(priority, has_high_waiting_now):
        # Shares are per-assignment caps, not reservations.
        if priority in (Priority.QUERY, Priority.INTERACTIVE):
            return s.high_cpu_share, s.high_ram_share

        # Batch: allow full-pool usage if no high-priority work is queued.
        if has_high_waiting_now:
            return s.batch_cap_share_when_high_waiting, s.batch_cap_share_when_high_waiting
        return 1.0, 1.0

    def _required_ram_for_op(pool, pipeline_id, op):
        k = (pipeline_id, _op_key(op))
        hint = s.op_ram_hint.get(k, None)
        if hint is None:
            # Initial hint: start moderately sized; slightly smaller floor for high-priority
            # is handled by choosing higher typical shares for high-priority allocations.
            hint = max(0.0, pool.max_ram_pool * s.initial_ram_share)
            s.op_ram_hint[k] = hint
        # Always cap at pool max.
        return min(hint, pool.max_ram_pool)

    def _update_hints_from_result(r):
        # Update per-op sizing hints and failure counters.
        if not getattr(r, "ops", None):
            return

        pool = s.executor.pools[r.pool_id]
        for op in r.ops:
            k = (getattr(r, "pipeline_id", None), _op_key(op))  # pipeline_id may not exist on result
            # If pipeline_id isn't on result, we can't safely key per pipeline; fall back to op-only key.
            if k[0] is None:
                k = ("<unknown_pipeline>", _op_key(op))

            if r.failed():
                if _is_oom_error(r.error):
                    # On OOM, increase RAM hint aggressively to converge in few retries.
                    prev = s.op_ram_hint.get(k, max(0.0, pool.max_ram_pool * s.initial_ram_share))
                    basis = prev
                    # If we know what we tried, grow from that too.
                    tried = getattr(r, "ram", None)
                    if tried is not None:
                        try:
                            basis = max(basis, float(tried))
                        except Exception:
                            pass
                    s.op_ram_hint[k] = min(pool.max_ram_pool, max(prev, basis) * s.oom_ram_growth)
                else:
                    s.op_fail_count[k] = s.op_fail_count.get(k, 0) + 1
            else:
                # On success, remember at least what worked (avoid shrinking).
                tried = getattr(r, "ram", None)
                if tried is not None:
                    try:
                        s.op_ram_hint[k] = max(s.op_ram_hint.get(k, 0.0), float(tried))
                    except Exception:
                        pass

    def _pipeline_done_or_dropped(pipeline):
        if pipeline.pipeline_id in s.dropped_pipeline_ids:
            return True
        st = pipeline.runtime_status()
        if st.is_pipeline_successful():
            return True
        return False

    def _should_drop_pipeline_due_to_failures(pipeline):
        # If an op has failed too many times with non-OOM errors, stop retrying this pipeline.
        # We conservatively only drop when we can identify failed ops and match our counters.
        st = pipeline.runtime_status()
        failed_ops = st.get_ops([OperatorState.FAILED], require_parents_complete=False) or []
        for op in failed_ops:
            k1 = (pipeline.pipeline_id, _op_key(op))
            k2 = ("<unknown_pipeline>", _op_key(op))
            if s.op_fail_count.get(k1, 0) > s.max_non_oom_retries or s.op_fail_count.get(k2, 0) > s.max_non_oom_retries:
                return True
        return False

    def _pop_next_schedulable(priority, pool, avail_cpu, avail_ram, scheduled_pipeline_ids):
        """
        Round-robin scan within a priority queue to find the next pipeline with a ready op
        that can fit (RAM) in this pool. Returns (pipeline, op_list) or (None, None).
        """
        q = s.queues[priority]
        if not q:
            return None, None

        scans = min(len(q), s.max_queue_scan)
        for _ in range(scans):
            pipeline = q.popleft()

            # Skip duplicates for this tick to prevent one pipeline grabbing multiple slots.
            if pipeline.pipeline_id in scheduled_pipeline_ids:
                q.append(pipeline)
                continue

            # Cleanup completed/dropped pipelines.
            if _pipeline_done_or_dropped(pipeline):
                continue
            if _should_drop_pipeline_due_to_failures(pipeline):
                s.dropped_pipeline_ids.add(pipeline.pipeline_id)
                continue

            st = pipeline.runtime_status()

            # Get a single runnable operator to keep fairness simple and predictable.
            op_list = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
            if not op_list:
                # Not runnable yet (parents not complete); keep it in RR queue.
                q.append(pipeline)
                continue

            op = op_list[0]
            req_ram = _required_ram_for_op(pool, pipeline.pipeline_id, op)

            # Admission: if we can't meet the RAM hint, keep scanning for something that fits.
            # This avoids repeated OOM churn and avoids blocking on a too-large op.
            if avail_ram < max(0.0, req_ram):
                q.append(pipeline)
                continue

            # Found something schedulable.
            q.append(pipeline)  # keep it in the queue for later stages/ops
            return pipeline, op_list

        return None, None

    # Enqueue newly arrived pipelines.
    for p in pipelines:
        s.known_pipelines[p.pipeline_id] = p
        # Avoid re-enqueueing a completed/dropped pipeline.
        if p.pipeline_id in s.dropped_pipeline_ids:
            continue
        if p.runtime_status().is_pipeline_successful():
            continue
        s.queues[p.priority].append(p)

    # Process results to update RAM hints / failure counters.
    for r in results:
        _update_hints_from_result(r)

    # Early exit if nothing changed and no backlog.
    if not pipelines and not results:
        if not _high_waiting() and len(s.queues[Priority.BATCH_PIPELINE]) == 0:
            return [], []

    suspensions = []
    assignments = []
    scheduled_pipeline_ids = set()
    has_high_waiting_now = _high_waiting()

    # Greedily schedule across pools.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool

        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # Pack multiple assignments per pool per tick while resources remain.
        # Stop when no further schedulable work exists.
        made_progress = True
        while made_progress and avail_cpu > 0 and avail_ram > 0:
            made_progress = False

            for prio in _pool_priority_order(pool_id):
                pipeline, op_list = _pop_next_schedulable(
                    prio, pool, avail_cpu, avail_ram, scheduled_pipeline_ids
                )
                if pipeline is None:
                    continue

                cpu_share, ram_share = _target_shares(prio, has_high_waiting_now)

                # Choose allocation capped by shares; always allocate at least 1 CPU if possible.
                target_cpu = max(1, int(pool.max_cpu_pool * cpu_share))
                cpu = min(avail_cpu, target_cpu)

                # RAM allocation: must satisfy per-op hint, but otherwise capped by share.
                op = op_list[0]
                req_ram = _required_ram_for_op(pool, pipeline.pipeline_id, op)

                target_ram = pool.max_ram_pool * ram_share
                ram = min(avail_ram, max(req_ram, target_ram if ram_share < 1.0 else req_ram))

                # Final safety: if we somehow still don't satisfy req_ram, skip.
                if ram < req_ram:
                    continue

                assignments.append(
                    Assignment(
                        ops=op_list,
                        cpu=cpu,
                        ram=ram,
                        priority=pipeline.priority,
                        pool_id=pool_id,
                        pipeline_id=pipeline.pipeline_id,
                    )
                )

                scheduled_pipeline_ids.add(pipeline.pipeline_id)
                avail_cpu -= cpu
                avail_ram -= ram
                made_progress = True
                break  # re-evaluate from highest priority with updated availability

    return suspensions, assignments
