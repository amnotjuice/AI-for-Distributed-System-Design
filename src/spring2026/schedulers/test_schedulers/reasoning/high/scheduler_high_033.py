# policy_key: scheduler_high_033
# reasoning_effort: high
# exp: reasoning
# model: gpt-5.2-2025-12-11
# llm_cost: 0.161641
# generation_seconds: 111.74
# generated_at: 2026-04-10T02:46:13.280354
@register_scheduler_init(key="scheduler_high_033")
def scheduler_high_033_init(s):
    """Priority-aware, OOM-averse scheduler with gentle fairness.

    Core ideas:
      - Strictly protect QUERY first, then INTERACTIVE; BATCH still makes progress via weighted RR.
      - Allocate conservative (generous) RAM to reduce OOMs; on OOM, exponentially increase RAM hint + add retry backoff.
      - Allocate more CPU to higher priorities (sublinear scaling assumed, but helps latency).
      - Avoid duplicate assignments in the same tick by limiting to 1 op/pipeline/tick.
    """
    s.tick = 0

    # Per-priority FIFO queues of pipelines
    s.queues = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }

    # Op-level hints (keyed by (pipeline_id, op_key))
    s.op_ram_hint = {}        # last "safe" ram to avoid OOM
    s.op_cpu_hint = {}        # optional cpu preference (lightweight)
    s.op_retry_count = {}     # consecutive failures
    s.op_next_eligible_tick = {}  # backoff scheduling to avoid thrash

    # Weighted round-robin pattern for fairness (still overridden by strict query protection)
    s.rr_pattern = (
        [Priority.QUERY] * 10 +
        [Priority.INTERACTIVE] * 5 +
        [Priority.BATCH_PIPELINE] * 1
    )
    s.rr_idx = 0

    # Assignable states: prefer using a locally-defined list to avoid relying on a global constant
    s.assignable_states = [OperatorState.PENDING, OperatorState.FAILED]

    # Limit how much we scan queues per decision to avoid pathological O(n^2) behavior
    s.max_scan_factor = 2  # scan up to factor * len(queue)


def _err_is_oom(err) -> bool:
    if err is None:
        return False
    msg = str(err).lower()
    # Keep this intentionally broad; simulator errors are often stringly-typed.
    return ("oom" in msg) or ("out of memory" in msg) or ("out_of_memory" in msg) or ("memory" in msg and "alloc" in msg)


def _get_op_key(op):
    # Prefer stable logical identifiers when present; fall back to object identity.
    k = getattr(op, "op_id", None)
    if k is not None:
        return ("op_id", k)
    k = getattr(op, "operator_id", None)
    if k is not None:
        return ("operator_id", k)
    return ("id", id(op))


def _op_hint_key(pipeline_id, op):
    return (pipeline_id, _get_op_key(op))


def _base_cpu_for_priority(pool, priority):
    mc = float(pool.max_cpu_pool)
    if priority == Priority.QUERY:
        return max(1.0, 0.60 * mc)
    if priority == Priority.INTERACTIVE:
        return max(1.0, 0.40 * mc)
    return max(1.0, 0.25 * mc)


def _base_ram_for_priority(pool, priority):
    mr = float(pool.max_ram_pool)
    if priority == Priority.QUERY:
        return max(1.0, 0.40 * mr)
    if priority == Priority.INTERACTIVE:
        return max(1.0, 0.30 * mr)
    return max(1.0, 0.20 * mr)


def _reserve_for_query(pool):
    # Keep a small headroom for sudden query arrivals to avoid needing preemption.
    return max(1.0, 0.10 * float(pool.max_cpu_pool)), max(1.0, 0.10 * float(pool.max_ram_pool))


def _reserve_for_interactive(pool):
    # Smaller headroom for interactive arrivals when batch is running.
    return max(1.0, 0.05 * float(pool.max_cpu_pool)), max(1.0, 0.05 * float(pool.max_ram_pool))


def _queue_nonempty(s, priority):
    q = s.queues.get(priority, [])
    return len(q) > 0


def _pick_priority(s):
    # If any queries exist, strongly bias to query to protect tail latency.
    if _queue_nonempty(s, Priority.QUERY):
        return Priority.QUERY

    # If no query backlog, use weighted RR between interactive/batch (and queries if present later).
    for _ in range(len(s.rr_pattern)):
        pr = s.rr_pattern[s.rr_idx]
        s.rr_idx = (s.rr_idx + 1) % len(s.rr_pattern)
        if _queue_nonempty(s, pr):
            return pr

    return None


def _pop_runnable_pipeline_and_op(s, priority, scheduled_pipelines_this_tick):
    q = s.queues.get(priority, [])
    if not q:
        return None, None

    # Scan a bounded amount: rotate queue to preserve FIFO-ish ordering.
    scan_budget = max(1, s.max_scan_factor * len(q))
    while q and scan_budget > 0:
        scan_budget -= 1
        pipeline = q.pop(0)

        # Avoid double-scheduling within the same tick (keeps state coherent).
        if pipeline.pipeline_id in scheduled_pipelines_this_tick:
            q.append(pipeline)
            continue

        status = pipeline.runtime_status()

        # Drop fully successful pipelines from the queue.
        if status.is_pipeline_successful():
            continue

        # Find runnable ops (parents complete)
        op_list = status.get_ops(s.assignable_states, require_parents_complete=True)
        if not op_list:
            # Not ready yet; keep it in the queue.
            q.append(pipeline)
            continue

        # Choose one op (keep it simple + avoid overscheduling the same pipeline).
        op = op_list[0]
        hk = _op_hint_key(pipeline.pipeline_id, op)
        next_tick = s.op_next_eligible_tick.get(hk, 0)
        if next_tick > s.tick:
            q.append(pipeline)
            continue

        # Runnable
        q.append(pipeline)  # round-robin within same priority
        return pipeline, op

    return None, None


def _clamp(x, lo, hi):
    return max(lo, min(hi, x))


@register_scheduler(key="scheduler_high_033")
def scheduler_high_033(s, results: List[ExecutionResult], pipelines: List[Pipeline]) -> Tuple[List[Suspend], List[Assignment]]:
    """
    Priority-first scheduler with OOM-aware RAM backoff and gentle fairness.

    - Updates per-op RAM hints on OOM failures to avoid repeated failures (which are heavily penalized).
    - Schedules queries aggressively (CPU + headroom preservation), then interactive, then batch.
    - Uses weighted RR when no query backlog exists to avoid batch starvation.
    """
    s.tick += 1

    # Enqueue newly arrived pipelines by priority
    for p in pipelines:
        s.queues[p.priority].append(p)

    # Update hints based on execution results
    for r in results:
        # Update per-op stats for each op in the container result
        for op in (r.ops or []):
            hk = _op_hint_key(getattr(r, "pipeline_id", None) or getattr(op, "pipeline_id", None) or -1, op)
            # If we can't recover a proper pipeline_id from the result/op, fall back to keying by op identity only.
            # This is a best-effort safety valve; the scheduler still works without perfect attribution.
            if hk[0] == -1:
                hk = ("unknown_pipeline", _get_op_key(op))

            if r.failed():
                prev = s.op_retry_count.get(hk, 0)
                s.op_retry_count[hk] = prev + 1

                # RAM backoff: be more aggressive on OOM-like errors, but still increase slightly on unknown failures.
                if _err_is_oom(getattr(r, "error", None)):
                    bump = 1.75
                else:
                    bump = 1.25

                # Use last allocation as a baseline, bump it; keep monotonic.
                last_ram = float(getattr(r, "ram", 1.0) or 1.0)
                cur_hint = float(s.op_ram_hint.get(hk, 0.0) or 0.0)
                s.op_ram_hint[hk] = max(cur_hint, last_ram * bump)

                # Backoff to avoid rapid re-fail thrash; cap to keep progress.
                # (Backoff in ticks, not seconds; simulator is event-driven.)
                backoff = min(12, 1 + (2 ** min(3, s.op_retry_count[hk] - 1)))
                s.op_next_eligible_tick[hk] = s.tick + backoff
            else:
                # Successful run: stabilize hints
                last_ram = float(getattr(r, "ram", 1.0) or 1.0)
                last_cpu = float(getattr(r, "cpu", 1.0) or 1.0)
                s.op_ram_hint[hk] = max(float(s.op_ram_hint.get(hk, 0.0) or 0.0), last_ram)
                s.op_cpu_hint[hk] = max(float(s.op_cpu_hint.get(hk, 0.0) or 0.0), last_cpu)
                s.op_retry_count[hk] = 0
                s.op_next_eligible_tick.pop(hk, None)

    # If nothing changed, do nothing (but still allow tick-based backoff to elapse).
    if not pipelines and not results:
        return [], []

    suspensions: List[Suspend] = []
    assignments: List[Assignment] = []

    # Schedule work across pools
    scheduled_pipelines_this_tick = set()

    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)

        # If pool has no resources, skip
        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # Fill the pool with as many single-op assignments as we can
        # while protecting headroom when higher priority work exists.
        while avail_cpu >= 1.0 and avail_ram >= 1.0:
            # Decide which priority to attempt next
            pr = _pick_priority(s)
            if pr is None:
                break

            pipeline, op = _pop_runnable_pipeline_and_op(s, pr, scheduled_pipelines_this_tick)
            if pipeline is None or op is None:
                # No runnable op at this priority; try again (RR will advance)
                # but if queues exist yet nothing runnable, break to avoid spinning.
                # Heuristic: if all priorities fail to provide runnable work, stop.
                tried = 0
                got_any = False
                while tried < 3:
                    pr2 = _pick_priority(s)
                    if pr2 is None:
                        break
                    p2, o2 = _pop_runnable_pipeline_and_op(s, pr2, scheduled_pipelines_this_tick)
                    if p2 is not None:
                        pipeline, op, pr = p2, o2, pr2
                        got_any = True
                        break
                    tried += 1
                if not got_any:
                    break

            # Compute resource request using base fractions + learned hints
            hk = _op_hint_key(pipeline.pipeline_id, op)

            base_cpu = _base_cpu_for_priority(pool, pipeline.priority)
            base_ram = _base_ram_for_priority(pool, pipeline.priority)

            # Start from hints if present; otherwise base.
            cpu_hint = float(s.op_cpu_hint.get(hk, 0.0) or 0.0)
            ram_hint = float(s.op_ram_hint.get(hk, 0.0) or 0.0)

            # Use a conservative default CPU; don't let hints explode beyond pool.
            target_cpu = base_cpu if cpu_hint <= 0 else _clamp(cpu_hint, 1.0, float(pool.max_cpu_pool))
            target_ram = base_ram if ram_hint <= 0 else _clamp(ram_hint, 1.0, float(pool.max_ram_pool))

            # If we've seen repeated failures, increase RAM further (OOM risk dominates objective).
            retries = int(s.op_retry_count.get(hk, 0) or 0)
            if retries > 0:
                target_ram = max(target_ram, base_ram * (1.50 ** min(4, retries)))

            # Keep headroom for higher priorities to reduce their queueing latency.
            # - If queries exist, don't let interactive/batch consume the last resources.
            # - If interactive exists, don't let batch consume the last resources.
            reserve_cpu_q, reserve_ram_q = _reserve_for_query(pool)
            reserve_cpu_i, reserve_ram_i = _reserve_for_interactive(pool)

            if pipeline.priority != Priority.QUERY and _queue_nonempty(s, Priority.QUERY):
                # Enforce query reserve
                max_cpu_for_this = max(0.0, avail_cpu - reserve_cpu_q)
                max_ram_for_this = max(0.0, avail_ram - reserve_ram_q)
            elif pipeline.priority == Priority.BATCH_PIPELINE and _queue_nonempty(s, Priority.INTERACTIVE):
                # Enforce interactive reserve (only constrains batch)
                max_cpu_for_this = max(0.0, avail_cpu - reserve_cpu_i)
                max_ram_for_this = max(0.0, avail_ram - reserve_ram_i)
            else:
                max_cpu_for_this = avail_cpu
                max_ram_for_this = avail_ram

            # If we can't fit even minimal resources due to reserves, stop scheduling lower priority in this pool.
            if max_cpu_for_this < 1.0 or max_ram_for_this < 1.0:
                break

            # Final request clamped to available headroom
            req_cpu = _clamp(target_cpu, 1.0, max_cpu_for_this)
            req_ram = _clamp(target_ram, 1.0, max_ram_for_this)

            # Create assignment (one op at a time)
            assignments.append(
                Assignment(
                    ops=[op],
                    cpu=req_cpu,
                    ram=req_ram,
                    priority=pipeline.priority,
                    pool_id=pool_id,
                    pipeline_id=pipeline.pipeline_id,
                )
            )

            scheduled_pipelines_this_tick.add(pipeline.pipeline_id)

            # Update local available resources
            avail_cpu -= req_cpu
            avail_ram -= req_ram

    return suspensions, assignments
