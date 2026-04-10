# policy_key: scheduler_high_024
# reasoning_effort: high
# model: gpt-5.2-2025-12-11
# llm_cost: 0.148987
# generation_seconds: 162.28
# generated_at: 2026-03-14T03:34:14.822732
@register_scheduler_init(key="scheduler_high_024")
def scheduler_high_024_init(s):
    """
    Priority-aware multi-assign scheduler (small, obvious improvements over naive FIFO).

    Key changes vs. naive example:
      1) Priority queues: always try QUERY, then INTERACTIVE, then BATCH.
      2) Multiple assignments per pool per tick (instead of at most one).
      3) Avoid giving one op the entire pool by default (caps per-op CPU/RAM by priority).
      4) Basic OOM retry: if an op fails with an OOM-like error, retry later with more RAM.
      5) Basic anti-starvation: occasionally allow a batch op even when interactive work exists
         (but keep pool 0 biased towards interactive).
    """
    from collections import deque

    # Tick counter for simple fairness / quotas
    s.tick = 0

    # Pipeline registry and de-duplication
    s.pipeline_by_id = {}          # pipeline_id -> Pipeline
    s.known_pipelines = set()      # pipeline_id

    # Waiting queues by priority (store pipeline_id)
    s.waiting = {
        Priority.QUERY: deque(),
        Priority.INTERACTIVE: deque(),
        Priority.BATCH_PIPELINE: deque(),
    }
    s.in_queue = set()  # pipeline_id currently enqueued somewhere

    # If we observe failures that look non-transient, stop retrying that pipeline
    s.pipeline_fatal = set()  # pipeline_id

    # Map operator identity -> pipeline_id (learned when we schedule it)
    s.op_to_pipeline = {}  # id(op) -> pipeline_id

    # OOM sizing hints per operator (keyed by id(op))
    s.op_ram_mult = {}   # id(op) -> multiplier (>= 1.0)
    s.op_ram_floor = {}  # id(op) -> absolute minimum RAM to try next time (based on previous OOM alloc)

    # Scheduling knobs (kept intentionally simple)
    s.max_assignments_per_pool = 4
    s.min_cpu = 1.0
    s.min_ram = 1e-6

    # Anti-starvation: after N non-batch assignments, allow one batch assignment
    # (but keep pool 0 more protected for latency-sensitive work).
    s.nonbatch_assignments = 0
    s.batch_every = 10

    # Caps on how aggressively we keep multiplying RAM after repeated OOMs
    s.max_ram_mult = 16.0


def _scheduler_high_024_is_oom_error(err):
    if not err:
        return False
    e = str(err).lower()
    return ("oom" in e) or ("out of memory" in e) or ("cuda out of memory" in e)


def _scheduler_high_024_norm_priority(pri):
    # Ensure unknown priorities don't crash and behave like batch.
    if pri == Priority.QUERY:
        return Priority.QUERY
    if pri == Priority.INTERACTIVE:
        return Priority.INTERACTIVE
    if pri == Priority.BATCH_PIPELINE:
        return Priority.BATCH_PIPELINE
    return Priority.BATCH_PIPELINE


def _scheduler_high_024_enqueue_pid(s, pipeline_id):
    if pipeline_id in s.pipeline_fatal:
        return
    if pipeline_id in s.in_queue:
        return
    p = s.pipeline_by_id.get(pipeline_id)
    if p is None:
        return
    pri = _scheduler_high_024_norm_priority(p.priority)
    s.waiting[pri].append(pipeline_id)
    s.in_queue.add(pipeline_id)


def _scheduler_high_024_cleanup_pipeline_if_done(s, pipeline_id, pipeline):
    # Remove from queues lazily (we de-dup by in_queue; we just avoid re-enqueueing).
    try:
        status = pipeline.runtime_status()
        if status.is_pipeline_successful():
            # Keep registry small
            s.pipeline_by_id.pop(pipeline_id, None)
            s.known_pipelines.discard(pipeline_id)
            s.in_queue.discard(pipeline_id)
            return True
    except Exception:
        # If status probing fails for any reason, don't delete state.
        return False
    return False


def _scheduler_high_024_pick_next_pipeline(s, pool_id, allow_batch, assigned_counts, scan_limit=32):
    """
    Pick the next pipeline to schedule from queues, prioritizing latency-sensitive work.
    Returns (pipeline_id, pipeline, op_list) or (None, None, None).
    """
    # Pool 0 is slightly protected for interactive/queries.
    protect_pool0 = (pool_id == 0)

    pri_order = [Priority.QUERY, Priority.INTERACTIVE]
    if allow_batch and not protect_pool0:
        pri_order.append(Priority.BATCH_PIPELINE)

    scanned = []  # pipeline_ids we pulled but couldn't schedule now; will re-enqueue

    for pri in pri_order:
        q = s.waiting[pri]
        # Limit scanning to avoid O(n) behavior when many pipelines have no runnable ops yet.
        n = min(len(q), scan_limit)
        for _ in range(n):
            pid = q.popleft()
            s.in_queue.discard(pid)

            p = s.pipeline_by_id.get(pid)
            if p is None:
                continue
            if pid in s.pipeline_fatal:
                continue
            if _scheduler_high_024_cleanup_pipeline_if_done(s, pid, p):
                continue

            # Avoid scheduling too many ops from the same pipeline in a single tick.
            # Allow QUERY pipelines to get up to 2 ops per tick (slightly better latency).
            cap = 2 if _scheduler_high_024_norm_priority(p.priority) == Priority.QUERY else 1
            if assigned_counts.get(pid, 0) >= cap:
                scanned.append(pid)
                continue

            status = p.runtime_status()

            # If the pipeline has failed ops, we only want to retry those that are retryable.
            # We do not have rich failure classification in status; we rely on result-based
            # pipeline_fatal marking + OOM-based retry sizing.
            op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
            if not op_list:
                scanned.append(pid)
                continue

            # Put back what we scanned before returning a selection
            for spid in scanned:
                _scheduler_high_024_enqueue_pid(s, spid)

            return pid, p, op_list

    # Re-enqueue scanned pipelines at the end (preserving their original priority)
    for spid in scanned:
        _scheduler_high_024_enqueue_pid(s, spid)

    return None, None, None


def _scheduler_high_024_resource_targets(s, pool, priority, op):
    """
    Compute a reasonable CPU/RAM target for an operator based on its priority,
    plus any learned OOM-based RAM inflation for this op.
    """
    pri = _scheduler_high_024_norm_priority(priority)

    max_cpu = float(pool.max_cpu_pool)
    max_ram = float(pool.max_ram_pool)

    # Per-op caps; aim to avoid one op taking the whole pool by default.
    if pri == Priority.QUERY:
        cpu_cap = max(s.min_cpu, 0.50 * max_cpu)
        ram_cap = max(s.min_ram, 0.50 * max_ram)
    elif pri == Priority.INTERACTIVE:
        cpu_cap = max(s.min_cpu, 0.33 * max_cpu)
        ram_cap = max(s.min_ram, 0.40 * max_ram)
    else:
        cpu_cap = max(s.min_cpu, 0.25 * max_cpu)
        ram_cap = max(s.min_ram, 0.30 * max_ram)

    op_key = id(op)
    mult = float(s.op_ram_mult.get(op_key, 1.0))
    floor = float(s.op_ram_floor.get(op_key, 0.0))

    ram_target = max(ram_cap * mult, floor)
    ram_target = min(ram_target, max_ram)

    cpu_target = min(cpu_cap, max_cpu)

    return cpu_target, ram_target, floor


@register_scheduler(key="scheduler_high_024")
def scheduler_high_024(s, results, pipelines):
    """
    Priority-aware scheduler:
      - Enqueue new pipelines by priority
      - Learn from OOM failures and increase RAM on retry
      - Assign multiple runnable ops per pool each tick with per-op resource caps
    """
    s.tick += 1

    suspensions = []
    assignments = []

    # ---- Ingest new pipelines (de-dup) ----
    for p in pipelines:
        pid = p.pipeline_id
        if pid not in s.known_pipelines:
            s.known_pipelines.add(pid)
            s.pipeline_by_id[pid] = p
            _scheduler_high_024_enqueue_pid(s, pid)
        else:
            # Refresh pointer in case simulator provides an updated object reference.
            s.pipeline_by_id[pid] = p
            _scheduler_high_024_enqueue_pid(s, pid)

    # ---- Process results (OOM learning, fatal failures) ----
    # NOTE: ExecutionResult doesn't explicitly include pipeline_id in the provided API list,
    # so we join via operator identity (id(op)) recorded at assignment time.
    for r in results:
        failed = False
        try:
            failed = r.failed()
        except Exception:
            failed = False

        for op in getattr(r, "ops", []) or []:
            op_key = id(op)
            pid = s.op_to_pipeline.get(op_key)

            # Best-effort cleanup of the join map to avoid unbounded growth.
            # If an op runs again later, we'll recreate the mapping when we re-assign it.
            s.op_to_pipeline.pop(op_key, None)

            if pid is None:
                continue

            if failed:
                if _scheduler_high_024_is_oom_error(getattr(r, "error", None)):
                    # Exponential backoff on RAM for this specific operator.
                    s.op_ram_mult[op_key] = min(float(s.op_ram_mult.get(op_key, 1.0)) * 2.0, s.max_ram_mult)

                    # Also set an absolute floor based on what we tried before.
                    tried_ram = float(getattr(r, "ram", 0.0) or 0.0)
                    if tried_ram > 0:
                        s.op_ram_floor[op_key] = max(float(s.op_ram_floor.get(op_key, 0.0)), tried_ram * 2.0)

                    # Ensure the pipeline is back in the queue for retry.
                    _scheduler_high_024_enqueue_pid(s, pid)
                else:
                    # Non-OOM failures are treated as fatal for now to avoid infinite retries.
                    s.pipeline_fatal.add(pid)
                    s.in_queue.discard(pid)
                    s.pipeline_by_id.pop(pid, None)
                    s.known_pipelines.discard(pid)

    # Early exit if nothing changed and no queued work (cheap check)
    if not pipelines and not results:
        any_waiting = any(len(q) > 0 for q in s.waiting.values())
        if not any_waiting:
            return [], []

    # ---- Scheduling loop: try to fill each pool with multiple assignments ----
    assigned_counts = {}  # pipeline_id -> how many ops we assigned this tick

    # Determine whether we should allow batch this tick, to provide some fairness.
    # If we recently scheduled many non-batch ops, allow a batch op (on non-protected pools).
    allow_batch_globally = True
    if s.nonbatch_assignments < s.batch_every:
        # Still within non-batch streak: only allow batch if no interactive/query is waiting.
        allow_batch_globally = (len(s.waiting[Priority.QUERY]) == 0 and len(s.waiting[Priority.INTERACTIVE]) == 0)

    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)

        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        made = 0
        # A small scan limit per assignment to avoid quadratic scans under heavy load.
        while made < s.max_assignments_per_pool:
            if avail_cpu < s.min_cpu or avail_ram < s.min_ram:
                break

            # Pool 0 is biased toward latency work; only allow batch there if nothing else exists.
            protect_pool0 = (pool_id == 0)
            allow_batch = allow_batch_globally
            if protect_pool0:
                allow_batch = (len(s.waiting[Priority.QUERY]) == 0 and len(s.waiting[Priority.INTERACTIVE]) == 0)

            pid, pipeline, op_list = _scheduler_high_024_pick_next_pipeline(
                s,
                pool_id=pool_id,
                allow_batch=allow_batch,
                assigned_counts=assigned_counts,
                scan_limit=32,
            )
            if pipeline is None or not op_list:
                break

            op = op_list[0]
            cpu_target, ram_target, ram_floor = _scheduler_high_024_resource_targets(s, pool, pipeline.priority, op)

            # If we learned a hard floor for this op (after OOM), do not run it under that.
            if ram_floor > 0 and avail_ram < ram_floor:
                # Put it back; try others.
                _scheduler_high_024_enqueue_pid(s, pid)
                break

            # Clamp to available resources; allow running under target for first-try ops,
            # but respect learned floor after OOM.
            cpu = min(avail_cpu, cpu_target)
            ram = min(avail_ram, ram_target)

            if cpu < s.min_cpu or ram < s.min_ram:
                _scheduler_high_024_enqueue_pid(s, pid)
                break
            if ram_floor > 0 and ram < ram_floor:
                _scheduler_high_024_enqueue_pid(s, pid)
                break

            # Record op->pipeline join for result processing.
            s.op_to_pipeline[id(op)] = pid

            assignments.append(
                Assignment(
                    ops=op_list,
                    cpu=cpu,
                    ram=ram,
                    priority=pipeline.priority,
                    pool_id=pool_id,
                    pipeline_id=pid,
                )
            )

            # Update local accounting (so we can pack multiple ops per pool this tick).
            avail_cpu -= float(cpu)
            avail_ram -= float(ram)

            made += 1
            assigned_counts[pid] = assigned_counts.get(pid, 0) + 1

            # Re-enqueue the pipeline so it can make progress on later ops in future ticks.
            _scheduler_high_024_enqueue_pid(s, pid)

            # Update non-batch streak counter for fairness.
            pri = _scheduler_high_024_norm_priority(pipeline.priority)
            if pri in (Priority.QUERY, Priority.INTERACTIVE):
                s.nonbatch_assignments += 1
            else:
                s.nonbatch_assignments = 0

    return suspensions, assignments
