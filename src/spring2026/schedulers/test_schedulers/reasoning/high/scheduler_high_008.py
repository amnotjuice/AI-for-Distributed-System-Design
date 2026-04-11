# policy_key: scheduler_high_008
# reasoning_effort: high
# exp: reasoning
# model: gpt-5.2-2025-12-11
# llm_cost: 0.184615
# generation_seconds: 182.42
# generated_at: 2026-04-10T00:35:42.637834
@register_scheduler_init(key="scheduler_high_008")
def scheduler_high_008_init(s):
    """
    Priority-aware, failure-avoiding scheduler with gentle fairness.

    Goals:
      - Strongly protect QUERY/INTERACTIVE latency (dominant in weighted score)
      - Avoid OOM-driven failures by adaptive per-operator RAM backoff
      - Ensure BATCH makes progress via aging promotion + limited batch forcing
      - Keep some headroom for high-priority arrivals (no preemption required)

    High-level behavior:
      - Maintain per-priority FIFO queues of pipelines.
      - Prefer QUERY > INTERACTIVE > BATCH, but promote old BATCH to INTERACTIVE.
      - When high-priority is waiting, reserve a fraction of each pool so BATCH
        doesn't consume all resources and later block high-priority work.
      - On operator failure, increase the next RAM request for that operator.
    """
    s.tick = 0

    # Per-priority FIFO queues (store Pipeline objects).
    s.queues = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }

    # Track which pipeline_ids are currently enqueued to avoid duplicates.
    s.in_queue = set()

    # Pipeline metadata for aging / scheduling decisions.
    s.pipeline_first_seen_tick = {}   # pipeline_id -> tick
    s.pipeline_last_enqueued_tick = {}  # pipeline_id -> tick

    # Per-operator adaptive resource hints (keyed by op object identity/signature).
    # Fields:
    #   retries: number of observed failures
    #   need_ram: minimum RAM to attempt next (increased on failure)
    s.op_profile = {}

    # Cap retries to avoid pathological infinite loops; after this, we stop trying.
    s.max_op_retries = 6

    # Per-pool assignment limits per scheduler invocation (controls burstiness).
    s.max_assignments_per_pool = 4

    # Headroom reservation in each pool when high-priority is waiting.
    s.reserve_high_cpu_frac = 0.25
    s.reserve_high_ram_frac = 0.25

    # Promote long-waiting batch pipelines to interactive to prevent starvation.
    s.batch_promote_after_ticks = 150

    # If we schedule several high-priority ops in a pool and batch exists,
    # try to schedule one batch op (if it fits beyond reserved headroom).
    s.force_one_batch_after_high = 3

    # Limit per-pipeline concurrent inflight ops (ASSIGNED/RUNNING/SUSPENDING).
    s.max_inflight_by_priority = {
        Priority.QUERY: 2,
        Priority.INTERACTIVE: 2,
        Priority.BATCH_PIPELINE: 1,
    }


@register_scheduler(key="scheduler_high_008")
def scheduler_high_008(s, results: List[ExecutionResult],
                       pipelines: List[Pipeline]) -> Tuple[List[Suspend], List[Assignment]]:
    """
    Scheduler step:
      - Ingest new pipelines into per-priority queues (dedup by pipeline_id)
      - Update per-operator RAM backoff on failures
      - Promote old batch pipelines to interactive
      - For each pool:
          * schedule up to max_assignments_per_pool assignments
          * prioritize QUERY > INTERACTIVE > (aged/promoted) > BATCH
          * reserve headroom for high-priority when high-priority is waiting
    """
    s.tick += 1
    suspensions: List[Suspend] = []
    assignments: List[Assignment] = []

    def _op_key(op):
        # Try a few likely stable identifiers; fall back to object id.
        for attr in ("op_id", "operator_id", "id", "name"):
            if hasattr(op, attr):
                try:
                    v = getattr(op, attr)
                    if v is not None:
                        return (attr, v)
                except Exception:
                    pass
        return ("py_id", id(op))

    def _profile_for_op(op):
        k = _op_key(op)
        prof = s.op_profile.get(k)
        if prof is None:
            prof = {"retries": 0, "need_ram": 0.0}
            s.op_profile[k] = prof
        return prof

    def _enqueue_pipeline(p: Pipeline):
        pid = p.pipeline_id
        if pid not in s.pipeline_first_seen_tick:
            s.pipeline_first_seen_tick[pid] = s.tick
        s.pipeline_last_enqueued_tick[pid] = s.tick

        if pid in s.in_queue:
            return
        s.in_queue.add(pid)
        s.queues[p.priority].append(p)

    def _pipeline_age(p: Pipeline) -> int:
        return s.tick - s.pipeline_first_seen_tick.get(p.pipeline_id, s.tick)

    def _effective_priority(p: Pipeline):
        # Only batch is eligible for promotion (prevents starvation / 720s penalties).
        if p.priority == Priority.BATCH_PIPELINE and _pipeline_age(p) >= s.batch_promote_after_ticks:
            return Priority.INTERACTIVE
        return p.priority

    def _inflight_count(status) -> int:
        # Counts ops currently consuming resources (or about to).
        sc = getattr(status, "state_counts", {}) or {}
        return (
            sc.get(OperatorState.ASSIGNED, 0) +
            sc.get(OperatorState.RUNNING, 0) +
            sc.get(OperatorState.SUSPENDING, 0)
        )

    def _can_schedule_more_for_pipeline(p: Pipeline, status) -> bool:
        limit = s.max_inflight_by_priority.get(p.priority, 1)
        return _inflight_count(status) < limit

    def _base_fractions(priority):
        # Conservative RAM to reduce OOM retries (failures are extremely costly).
        if priority == Priority.QUERY:
            return 0.75, 0.45  # cpu_frac, ram_frac
        if priority == Priority.INTERACTIVE:
            return 0.60, 0.35
        return 0.45, 0.28  # batch

    def _clamp(v, lo, hi):
        if v < lo:
            return lo
        if v > hi:
            return hi
        return v

    # Ingest new pipelines.
    for p in pipelines:
        _enqueue_pipeline(p)

    # Update operator RAM hints based on failures/successes.
    # We treat any failure as requiring more RAM next time (sim is often OOM-driven).
    for r in results:
        if not getattr(r, "ops", None):
            continue
        if r.failed():
            for op in r.ops:
                prof = _profile_for_op(op)
                prof["retries"] += 1
                # Backoff RAM: next attempt needs at least 2x the RAM we just tried,
                # with a small floor to avoid zero/None edge cases.
                tried_ram = float(getattr(r, "ram", 0.0) or 0.0)
                prof["need_ram"] = max(prof["need_ram"], max(0.1, tried_ram * 2.0))
        else:
            # On success: do not aggressively reduce need_ram (risk OOM),
            # but ensure it's at least a tiny positive number.
            for op in r.ops:
                prof = _profile_for_op(op)
                prof["need_ram"] = max(0.0, float(prof.get("need_ram", 0.0) or 0.0))

    # Early exit only if there's truly nothing to do.
    if not any(s.queues.values()):
        return suspensions, assignments

    # One-time promotion pass: move aged batch pipelines into interactive queue.
    # This is linear in batch queue length; bounded enough for simulation.
    if s.queues[Priority.BATCH_PIPELINE]:
        new_batch = []
        for p in s.queues[Priority.BATCH_PIPELINE]:
            if _pipeline_age(p) >= s.batch_promote_after_ticks:
                # Promote by re-enqueueing as interactive (without duplicating in_queue).
                s.queues[Priority.INTERACTIVE].append(p)
            else:
                new_batch.append(p)
        s.queues[Priority.BATCH_PIPELINE] = new_batch

    def _has_high_waiting() -> bool:
        return bool(s.queues[Priority.QUERY] or s.queues[Priority.INTERACTIVE])

    def _pop_next_ready(pri: Priority, scheduled_this_call: set):
        """
        Pop the next pipeline from queue[pri] that:
          - is not completed
          - is not abandoned (excessive op retries)
          - has at least one ASSIGNABLE op with parents complete
          - has inflight capacity (per-priority limit)
          - was not already scheduled in this scheduler call
        Returns (pipeline, op_list) or (None, None).
        """
        q = s.queues[pri]
        if not q:
            return None, None

        # Rotate through queue at most its current length.
        n = len(q)
        kept = []
        chosen = None
        chosen_ops = None

        for _ in range(n):
            p = q.pop(0)
            pid = p.pipeline_id

            # Skip if already scheduled in this call (avoid over-focusing one pipeline).
            if pid in scheduled_this_call:
                kept.append(p)
                continue

            status = p.runtime_status()
            if status.is_pipeline_successful():
                # Done: remove from bookkeeping.
                s.in_queue.discard(pid)
                continue

            # If pipeline currently has too many inflight ops, don't schedule more now.
            if not _can_schedule_more_for_pipeline(p, status):
                kept.append(p)
                continue

            op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
            if not op_list:
                kept.append(p)
                continue

            op = op_list[0]
            prof = _profile_for_op(op)
            if prof.get("retries", 0) >= s.max_op_retries:
                # Give up scheduling this operator/pipeline further to avoid wasting time.
                # This will likely be penalized as incomplete/failed by the benchmark.
                s.in_queue.discard(pid)
                continue

            chosen = p
            chosen_ops = op_list
            break

        # Put back unchosen pipelines to preserve FIFO-ish behavior.
        if kept:
            q.extend(kept)
        if chosen is not None:
            # Requeue chosen pipeline to allow future ops to be scheduled later.
            q.append(chosen)

        return chosen, chosen_ops

    scheduled_this_call = set()

    # Pool placement: if multiple pools exist, treat pool 0 as "high-priority-friendly":
    # avoid starting new batch there while high-priority is queued.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)

        if avail_cpu <= 0.0 or avail_ram <= 0.0:
            continue

        high_waiting = _has_high_waiting()
        pool_reserved_for_high = (s.executor.num_pools > 1 and pool_id == 0)

        # Reserve headroom in each pool when high-priority is waiting (except pool 0 which
        # we keep almost exclusively for high anyway).
        reserve_cpu = 0.0
        reserve_ram = 0.0
        if high_waiting and not pool_reserved_for_high:
            reserve_cpu = float(pool.max_cpu_pool) * float(s.reserve_high_cpu_frac)
            reserve_ram = float(pool.max_ram_pool) * float(s.reserve_high_ram_frac)

        # Per-pool loop to create multiple assignments if resources allow.
        made = 0
        high_scheduled_in_pool = 0

        while made < s.max_assignments_per_pool and avail_cpu >= 1.0 and avail_ram > 0.0:
            # Choose which priority to attempt next.
            # Mostly strict priority, with occasional batch forcing to prevent starvation.
            try_batch_now = (
                (not pool_reserved_for_high) and
                (high_scheduled_in_pool >= s.force_one_batch_after_high) and
                bool(s.queues[Priority.BATCH_PIPELINE])
            )

            candidates = []
            if pool_reserved_for_high and high_waiting:
                # Keep pool 0 for high-priority only while high waits.
                candidates = [Priority.QUERY, Priority.INTERACTIVE]
            else:
                if try_batch_now:
                    candidates = [Priority.BATCH_PIPELINE, Priority.QUERY, Priority.INTERACTIVE]
                else:
                    candidates = [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]

            chosen_p = None
            chosen_ops = None
            chosen_pri = None

            # Find the first priority class with a runnable pipeline/op.
            for pri in candidates:
                p, op_list = _pop_next_ready(pri, scheduled_this_call)
                if p is not None and op_list:
                    chosen_p, chosen_ops, chosen_pri = p, op_list, pri
                    break

            if chosen_p is None:
                break

            # Compute desired resources for this op.
            op = chosen_ops[0]
            prof = _profile_for_op(op)

            cpu_frac, ram_frac = _base_fractions(chosen_p.priority)

            desired_cpu = max(1.0, float(pool.max_cpu_pool) * float(cpu_frac))
            desired_ram = max(0.1, float(pool.max_ram_pool) * float(ram_frac))

            # Apply RAM backoff if we've seen failures for this operator.
            desired_ram = max(desired_ram, float(prof.get("need_ram", 0.0) or 0.0))

            # Clamp desired resources to pool maximums.
            desired_cpu = _clamp(desired_cpu, 1.0, float(pool.max_cpu_pool))
            desired_ram = _clamp(desired_ram, 0.1, float(pool.max_ram_pool))

            # Enforce headroom reservation for batch when high is waiting.
            eff_avail_cpu = avail_cpu
            eff_avail_ram = avail_ram
            if chosen_p.priority == Priority.BATCH_PIPELINE and high_waiting and not pool_reserved_for_high:
                eff_avail_cpu = max(0.0, avail_cpu - reserve_cpu)
                eff_avail_ram = max(0.0, avail_ram - reserve_ram)

            # Fit CPU first (we can reduce CPU without risking OOM); keep RAM conservative.
            cpu = min(desired_cpu, eff_avail_cpu)
            ram = min(desired_ram, eff_avail_ram)

            # If we can't fit the RAM we believe we need, don't schedule this op now.
            # This avoids OOM retries that are costly in the objective.
            if ram + 1e-9 < desired_ram:
                # Can't fit; stop in this pool if it's due to headroom reservation,
                # otherwise give other pools/ops a chance next call.
                break

            if cpu < 1.0:
                break

            # Issue assignment.
            assignment = Assignment(
                ops=chosen_ops,
                cpu=cpu,
                ram=ram,
                priority=chosen_p.priority,
                pool_id=pool_id,
                pipeline_id=chosen_p.pipeline_id,
            )
            assignments.append(assignment)

            # Update local pool availability (optimistic; matches template pattern).
            avail_cpu -= cpu
            avail_ram -= ram
            made += 1

            scheduled_this_call.add(chosen_p.pipeline_id)
            if chosen_p.priority in (Priority.QUERY, Priority.INTERACTIVE):
                high_scheduled_in_pool += 1
            else:
                high_scheduled_in_pool = 0

    return suspensions, assignments
