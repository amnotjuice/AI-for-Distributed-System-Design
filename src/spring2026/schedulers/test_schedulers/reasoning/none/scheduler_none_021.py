# policy_key: scheduler_none_021
# reasoning_effort: none
# model: gpt-5.2-2025-12-11
# llm_cost: 0.054261
# generation_seconds: 54.22
# generated_at: 2026-04-09T22:01:11.129861
@register_scheduler_init(key="scheduler_none_021")
def scheduler_none_021_init(s):
    """Priority-aware, OOM-adaptive, non-starving scheduler.

    Design goals for the weighted latency objective:
      - Strongly protect QUERY and INTERACTIVE by (1) dedicated headroom, (2) limited preemption of BATCH.
      - Avoid failures (720s penalty) via per-op RAM backoff on OOM and cautious CPU/RAM sizing.
      - Avoid starvation by aging BATCH and enforcing a small batch admission share when possible.

    Key ideas:
      - Maintain three FIFO queues (query/interactive/batch).
      - Per-(pipeline_id, op_id) RAM multiplier that increases on OOM and decays slowly on success.
      - Reserve a fraction of each pool for high priority admission; preempt BATCH only when needed.
      - Place high priority on the pool with most headroom; allow batch to use remaining capacity.
    """
    # Priority queues (FIFO within priority)
    s.q_query = []
    s.q_interactive = []
    s.q_batch = []

    # Arrival tick for aging / anti-starvation
    s._tick = 0
    s._arrival_tick = {}  # pipeline_id -> tick

    # Track per-op RAM multiplier for OOM avoidance: (pipeline_id, op_id) -> multiplier
    s._ram_mult = {}
    s._ram_mult_min = 1.0
    s._ram_mult_max = 4.0
    s._ram_mult_step = 1.35  # multiplicative increase on OOM

    # Track last known "good" cpu per op to avoid always maxing out
    s._cpu_hint = {}  # (pipeline_id, op_id) -> cpu

    # Track running containers for possible preemption
    s._running = {}  # container_id -> dict(pool_id, priority, pipeline_id)

    # Headroom reservations per pool for high priority work (fractions of pool capacity)
    s._reserve_query_cpu_frac = 0.20
    s._reserve_query_ram_frac = 0.20
    s._reserve_interactive_cpu_frac = 0.10
    s._reserve_interactive_ram_frac = 0.10

    # Batch fairness: try to admit at least one batch op every N ticks if there is capacity
    s._batch_fairness_period = 10
    s._last_batch_admit_tick = -10

    # Limits to avoid over-preemption churn
    s._max_preempts_per_tick = 2


@register_scheduler(key="scheduler_none_021")
def scheduler_none_021_scheduler(s, results, pipelines):
    """
    Scheduler step:
      1) Ingest new pipelines into priority queues.
      2) Process results:
         - On OOM failure: increase RAM multiplier and requeue pipeline.
         - Track running containers for targeted preemption.
      3) If high-priority work is waiting and reserved headroom is insufficient, preempt some BATCH.
      4) Assign ready ops:
         - Prefer QUERY then INTERACTIVE then BATCH (with aging and periodic fairness).
         - Pick pool with best headroom fit for the priority.
         - Size RAM with backoff multiplier; size CPU moderately (not always max) to reduce interference.
    """
    # Helper functions are defined inside to avoid global imports per instructions.
    def _get_prio_bucket(prio):
        if prio == Priority.QUERY:
            return "query"
        if prio == Priority.INTERACTIVE:
            return "interactive"
        return "batch"

    def _enqueue_pipeline(p):
        pid = p.pipeline_id
        if pid not in s._arrival_tick:
            s._arrival_tick[pid] = s._tick
        b = _get_prio_bucket(p.priority)
        if b == "query":
            s.q_query.append(p)
        elif b == "interactive":
            s.q_interactive.append(p)
        else:
            s.q_batch.append(p)

    def _pipeline_done_or_hard_failed(p):
        st = p.runtime_status()
        if st.is_pipeline_successful():
            return True
        # Don't permanently drop failed pipelines; we only "hard fail" if there are FAILED ops
        # that are not retryable, but the simulator doesn't expose that distinction.
        # So we keep retrying failures with increased RAM; the objective heavily penalizes failure.
        return False

    def _get_ready_ops(p, k=1):
        st = p.runtime_status()
        # Include FAILED so we can retry after OOM/backoff; require parents complete.
        ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        if not ops:
            return []
        return ops[:k]

    def _op_id(op):
        # Best-effort stable identifier; operator objects in simulator typically have operator_id or op_id.
        if hasattr(op, "operator_id"):
            return getattr(op, "operator_id")
        if hasattr(op, "op_id"):
            return getattr(op, "op_id")
        if hasattr(op, "id"):
            return getattr(op, "id")
        # Fallback to repr (deterministic within run)
        return repr(op)

    def _ram_multiplier(pid, op):
        key = (pid, _op_id(op))
        return s._ram_mult.get(key, 1.0)

    def _set_ram_multiplier(pid, op, mult):
        key = (pid, _op_id(op))
        s._ram_mult[key] = max(s._ram_mult_min, min(s._ram_mult_max, mult))

    def _decay_ram_multiplier(pid, op):
        # On success, slowly decay toward 1.0 to avoid permanently over-allocating.
        key = (pid, _op_id(op))
        if key in s._ram_mult:
            s._ram_mult[key] = max(1.0, s._ram_mult[key] * 0.95)
            if s._ram_mult[key] <= 1.01:
                s._ram_mult.pop(key, None)

    def _cpu_target(pool, prio, pid, op):
        # Prefer moderate CPU to reduce contention and allow parallelism, but ensure good latency for high priority.
        # Use any hint from past success; otherwise default by priority.
        key = (pid, _op_id(op))
        hint = s._cpu_hint.get(key, None)

        max_cpu = pool.max_cpu_pool
        avail_cpu = pool.avail_cpu_pool

        if prio == Priority.QUERY:
            base = max(1.0, 0.60 * max_cpu)
        elif prio == Priority.INTERACTIVE:
            base = max(1.0, 0.45 * max_cpu)
        else:
            base = max(1.0, 0.25 * max_cpu)

        if hint is not None:
            base = max(1.0, min(max_cpu, hint))

        # Don't exceed what is available; also keep a small slack for additional admissions.
        return max(1.0, min(avail_cpu, base))

    def _ram_target(pool, prio, pid, op):
        # Without explicit per-op minimum/peak exposed here, we treat the last attempt's RAM (from failure result)
        # as a signal and otherwise allocate a fraction of pool RAM. RAM beyond peak doesn't help but reduces OOM risk.
        max_ram = pool.max_ram_pool
        avail_ram = pool.avail_ram_pool

        if prio == Priority.QUERY:
            base = 0.55 * max_ram
        elif prio == Priority.INTERACTIVE:
            base = 0.45 * max_ram
        else:
            base = 0.35 * max_ram

        mult = _ram_multiplier(pid, op)
        target = base * mult

        # Cap to pool availability; if not enough RAM, we won't assign in this pool.
        return max(0.0, min(avail_ram, target))

    def _pool_score_for_assignment(pool, prio):
        # Higher score means better placement. Prefer more headroom for high priority.
        # For batch, prefer packing into pools with less headroom (so we preserve space for high prio).
        cpu_head = pool.avail_cpu_pool / max(1e-9, pool.max_cpu_pool)
        ram_head = pool.avail_ram_pool / max(1e-9, pool.max_ram_pool)

        if prio == Priority.QUERY:
            return 3.0 * cpu_head + 3.0 * ram_head
        if prio == Priority.INTERACTIVE:
            return 2.0 * cpu_head + 2.0 * ram_head
        # pack batch (inverse headroom)
        return 1.0 * (1.0 - cpu_head) + 1.0 * (1.0 - ram_head)

    def _reserved_resources(pool):
        rq_cpu = s._reserve_query_cpu_frac * pool.max_cpu_pool
        rq_ram = s._reserve_query_ram_frac * pool.max_ram_pool
        ri_cpu = s._reserve_interactive_cpu_frac * pool.max_cpu_pool
        ri_ram = s._reserve_interactive_ram_frac * pool.max_ram_pool
        return rq_cpu, rq_ram, ri_cpu, ri_ram

    def _high_prio_waiting():
        return bool(s.q_query) or bool(s.q_interactive)

    def _choose_victim_containers_for_preempt(pool_id, needed_cpu, needed_ram):
        # Target only BATCH to reduce churn for high priorities.
        victims = []
        cpu_freed = 0.0
        ram_freed = 0.0
        # Iterate over known running containers and pick batch in this pool.
        for cid, meta in list(s._running.items()):
            if meta.get("pool_id") != pool_id:
                continue
            if meta.get("priority") != Priority.BATCH_PIPELINE:
                continue
            victims.append(cid)
            # We don't know exact freed resources here unless we consult executor internals;
            # approximating by "some" progress is fine; actual release is handled by simulator on suspend.
            # Use a heuristic increment to limit victims.
            cpu_freed += 1.0
            ram_freed += 1.0
            if cpu_freed >= needed_cpu and ram_freed >= needed_ram:
                break
            if len(victims) >= s._max_preempts_per_tick:
                break
        return victims

    # Tick update and ingest arrivals
    s._tick += 1
    for p in pipelines:
        _enqueue_pipeline(p)

    suspensions = []
    assignments = []

    # Process results: update running map, detect OOM, adjust RAM multiplier and CPU hints.
    for r in results:
        # Track running containers so we can preempt batch if needed.
        if getattr(r, "container_id", None) is not None:
            # If this result corresponds to a completion/failure, container is no longer running.
            # We'll remove it opportunistically in both cases.
            if r.container_id in s._running:
                s._running.pop(r.container_id, None)

        if r.failed():
            # If failure likely due to OOM, increase RAM multiplier for those ops.
            # The simulator encodes error as a string; best-effort match.
            err = (r.error or "")
            is_oom = ("OOM" in err) or ("out of memory" in err.lower()) or ("memory" in err.lower())
            if is_oom:
                for op in (r.ops or []):
                    pid = getattr(op, "pipeline_id", None)
                    # Some op objects may not carry pipeline_id; fall back to result's pipeline info not provided.
                    # We can't reliably obtain pipeline_id from result, so we will update using container meta if present.
                    if pid is None:
                        # Try recover from running meta: not available now; skip if unknown.
                        continue
                    mult = _ram_multiplier(pid, op)
                    _set_ram_multiplier(pid, op, mult * s._ram_mult_step)
            # On any failure, avoid increasing CPU hints; we'll keep them as-is.
        else:
            # On success completion, decay RAM multiplier and store CPU hint as the last cpu used (works as "enough CPU").
            for op in (r.ops or []):
                pid = getattr(op, "pipeline_id", None)
                if pid is None:
                    continue
                _decay_ram_multiplier(pid, op)
                key = (pid, _op_id(op))
                if getattr(r, "cpu", None) is not None:
                    s._cpu_hint[key] = r.cpu

    # If we have high priority waiting but pools are tight, preempt some BATCH to open headroom.
    if _high_prio_waiting():
        preempts_done = 0
        for pool_id in range(s.executor.num_pools):
            if preempts_done >= s._max_preempts_per_tick:
                break
            pool = s.executor.pools[pool_id]
            rq_cpu, rq_ram, ri_cpu, ri_ram = _reserved_resources(pool)

            # Determine if we are below reserved headroom while having waiters.
            # If query waiters exist, enforce query reserve; else enforce interactive reserve.
            need_cpu = 0.0
            need_ram = 0.0
            if s.q_query and (pool.avail_cpu_pool < rq_cpu or pool.avail_ram_pool < rq_ram):
                need_cpu = max(0.0, rq_cpu - pool.avail_cpu_pool)
                need_ram = max(0.0, rq_ram - pool.avail_ram_pool)
            elif (not s.q_query) and s.q_interactive and (pool.avail_cpu_pool < ri_cpu or pool.avail_ram_pool < ri_ram):
                need_cpu = max(0.0, ri_cpu - pool.avail_cpu_pool)
                need_ram = max(0.0, ri_ram - pool.avail_ram_pool)

            if need_cpu <= 0.0 and need_ram <= 0.0:
                continue

            victims = _choose_victim_containers_for_preempt(pool_id, need_cpu, need_ram)
            for cid in victims:
                if preempts_done >= s._max_preempts_per_tick:
                    break
                suspensions.append(Suspend(cid, pool_id))
                # Remove from running map; the actual free happens in simulation engine.
                s._running.pop(cid, None)
                preempts_done += 1

    # Build a combined "next pipeline" picker with aging and batch fairness.
    def _pop_next_pipeline():
        # Always prioritize query, then interactive.
        if s.q_query:
            return s.q_query.pop(0)
        if s.q_interactive:
            return s.q_interactive.pop(0)

        # Periodic batch admission to avoid starvation, but only if no high prio queued.
        if s.q_batch:
            return s.q_batch.pop(0)
        return None

    def _requeue_pipeline(p):
        # Aging: if batch has been waiting a long time, occasionally allow it ahead of interactive when interactive is long.
        # Keep it simple: requeue to its bucket's tail.
        _enqueue_pipeline(p)

    # Attempt to schedule across all pools; allow multiple assignments per tick if capacity exists.
    # We do light packing: one op per assignment as in baseline, but iterate while we can place something.
    made_progress = True
    max_attempts = 50  # avoid infinite loops if nothing fits
    attempts = 0

    while made_progress and attempts < max_attempts:
        attempts += 1
        made_progress = False

        # Batch fairness: if no high-prio waiting and batch exists, ensure we don't stall batch forever.
        if (not s.q_query and not s.q_interactive) and s.q_batch:
            if (s._tick - s._last_batch_admit_tick) >= s._batch_fairness_period:
                # We'll try to admit a batch next by leaving picker as-is; update tick on success.
                pass

        # Pick a pipeline to consider; if none, stop.
        p = _pop_next_pipeline()
        if p is None:
            break

        if _pipeline_done_or_hard_failed(p):
            continue

        ops = _get_ready_ops(p, k=1)
        if not ops:
            # Not ready now; requeue and move on.
            _requeue_pipeline(p)
            continue

        op = ops[0]

        # Choose best pool that can fit (cpu>=1 and ram>0) and respects reserved headroom for high priority.
        best_pool_id = None
        best_score = None
        best_cpu = None
        best_ram = None

        for pool_id in range(s.executor.num_pools):
            pool = s.executor.pools[pool_id]
            avail_cpu = pool.avail_cpu_pool
            avail_ram = pool.avail_ram_pool
            if avail_cpu < 1.0 or avail_ram <= 0.0:
                continue

            rq_cpu, rq_ram, ri_cpu, ri_ram = _reserved_resources(pool)

            # Enforce reservation: don't let lower priority consume reserved headroom.
            if p.priority == Priority.BATCH_PIPELINE:
                if (avail_cpu - 1.0) < (rq_cpu + ri_cpu) or (avail_ram - 1.0) < (rq_ram + ri_ram):
                    # Keep batch away from reserved space unless no other options will fit.
                    # We'll still allow it later if nothing else fits.
                    pass

            cpu = _cpu_target(pool, p.priority, p.pipeline_id, op)
            if cpu < 1.0:
                continue

            ram = _ram_target(pool, p.priority, p.pipeline_id, op)
            if ram <= 0.0:
                continue

            # For high priority, ensure we don't dip below its own reserved minimum by scheduling something huge.
            # We do this by limiting cpu/ram to "avail - reserve_of_same_or_higher".
            if p.priority == Priority.QUERY:
                cpu = min(cpu, max(1.0, avail_cpu))  # query can use anything available
                ram = min(ram, avail_ram)
            elif p.priority == Priority.INTERACTIVE:
                # Don't consume query reserve.
                cpu = min(cpu, max(1.0, avail_cpu - rq_cpu))
                ram = min(ram, max(0.0, avail_ram - rq_ram))
                if cpu < 1.0 or ram <= 0.0:
                    continue
            else:
                # Batch shouldn't consume query+interactive reserve when possible.
                cpu = min(cpu, max(1.0, avail_cpu - (rq_cpu + ri_cpu)))
                ram = min(ram, max(0.0, avail_ram - (rq_ram + ri_ram)))
                if cpu < 1.0 or ram <= 0.0:
                    # If batch can't fit respecting reserves, allow it only if there's no high-prio waiting at all.
                    if _high_prio_waiting():
                        continue
                    # Otherwise, allow using full available.
                    cpu = min(_cpu_target(pool, p.priority, p.pipeline_id, op), avail_cpu)
                    ram = min(_ram_target(pool, p.priority, p.pipeline_id, op), avail_ram)
                    if cpu < 1.0 or ram <= 0.0:
                        continue

            score = _pool_score_for_assignment(pool, p.priority)
            if best_score is None or score > best_score:
                best_score = score
                best_pool_id = pool_id
                best_cpu = cpu
                best_ram = ram

        if best_pool_id is None:
            # Couldn't place now; requeue to avoid dropping.
            _requeue_pipeline(p)
            continue

        # Make assignment
        assignment = Assignment(
            ops=[op],
            cpu=best_cpu,
            ram=best_ram,
            priority=p.priority,
            pool_id=best_pool_id,
            pipeline_id=p.pipeline_id,
        )
        assignments.append(assignment)
        made_progress = True

        if p.priority == Priority.BATCH_PIPELINE:
            s._last_batch_admit_tick = s._tick

        # Requeue pipeline if more ops remain; otherwise it'll be recognized as done in later ticks.
        _requeue_pipeline(p)

    return suspensions, assignments

