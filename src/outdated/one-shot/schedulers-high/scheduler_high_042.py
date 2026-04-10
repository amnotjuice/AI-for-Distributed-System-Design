# policy_key: scheduler_high_042
# reasoning_effort: high
# model: gpt-5.2-2025-12-11
# llm_cost: 0.150950
# generation_seconds: 140.80
# generated_at: 2026-03-14T04:20:22.739827
@register_scheduler_init(key="scheduler_high_042")
def scheduler_high_042_init(s):
    """
    Priority-aware FIFO with conservative latency improvements over the naive baseline.

    Small, obvious upgrades (kept intentionally simple/robust):
      1) Maintain separate waiting queues per priority and always prefer higher priority.
      2) Retry OOM-failed operators with increased RAM (exponential backoff), but do not
         retry non-OOM failures (avoid infinite churn).
      3) Right-size CPU/RAM by priority (high priority gets a larger share to reduce latency),
         while capping low-priority allocations to preserve headroom.

    Notes:
      - This policy avoids preemption because we do not assume visibility into all running
        containers (container IDs for arbitrary running work are not exposed in the template).
      - If your simulator exposes running containers via executor/pool state, preemption
        can be added later without changing the core queueing logic.
    """
    # Discrete scheduler "time" for simple aging/fairness.
    s._tick = 0

    # Per-priority waiting queues of Pipeline objects.
    s._queues = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }

    # Track pipeline enqueue tick to add minimal fairness (aging) for long-waiting batch.
    s._enqueue_tick_by_pipeline_id = {}

    # Track seen pipelines to avoid duplicate enqueues (defensive).
    s._known_pipeline_ids = set()

    # Per-operator hints (keyed by id(op) to remain stable across results/assignments).
    s._ram_hint_by_op_id = {}         # suggested RAM for next try
    s._oom_retries_by_op_id = {}      # number of OOM retries so far
    s._retryable_failed_ops = set()   # ops that failed with OOM (retryable)
    s._nonretryable_failed_ops = set()  # ops that failed for other reasons

    # Policy knobs (kept simple and conservative).
    s._max_oom_retries = 4
    s._oom_ram_multiplier = 2.0

    # Minimum "unit" to avoid allocating 0; simulator may allow fractional.
    s._min_cpu = 0.1
    s._min_ram = 0.1

    # Aging: slowly increases batch priority to avoid starvation.
    s._batch_aging_rate = 0.5  # score per tick waiting


@register_scheduler(key="scheduler_high_042")
def scheduler_high_042(s, results: List[ExecutionResult], pipelines: List[Pipeline]) -> Tuple[List[Suspend], List[Assignment]]:
    # ----------------------------
    # Helpers (local, no imports)
    # ----------------------------
    def _prio_base_score(prio):
        if prio == Priority.QUERY:
            return 1000.0
        if prio == Priority.INTERACTIVE:
            return 500.0
        return 0.0  # batch baseline; aging adds fairness

    def _is_oom_error(err) -> bool:
        if not err:
            return False
        msg = str(err).lower()
        return ("oom" in msg) or ("out of memory" in msg) or ("out-of-memory" in msg) or ("memory" in msg and "alloc" in msg)

    def _cleanup_pipeline(p: Pipeline) -> bool:
        """Return True if pipeline should be kept in queues, False if it should be dropped."""
        status = p.runtime_status()
        if status.is_pipeline_successful():
            return False

        # If any operator failed and is non-retryable, drop pipeline.
        failed_ops = status.get_ops([OperatorState.FAILED], require_parents_complete=False) or []
        if failed_ops:
            # If we don't have error classification for a failed op, be conservative and drop
            # (keeps the policy predictable and avoids endless retries on logic errors).
            for op in failed_ops:
                op_id = id(op)
                if op_id in s._nonretryable_failed_ops:
                    return False
                if op_id not in s._retryable_failed_ops:
                    return False
                if s._oom_retries_by_op_id.get(op_id, 0) > s._max_oom_retries:
                    return False
        return True

    def _pipeline_ready_ops(p: Pipeline):
        """Return list of assignable ops whose parents are complete."""
        status = p.runtime_status()
        return status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True) or []

    def _effective_pipeline_score(p: Pipeline) -> float:
        base = _prio_base_score(p.priority)
        enq = s._enqueue_tick_by_pipeline_id.get(p.pipeline_id, s._tick)
        age = max(0, s._tick - enq)
        # Only batch gets meaningful aging; high priorities already dominate.
        if p.priority == Priority.BATCH_PIPELINE:
            return base + age * s._batch_aging_rate
        return base + age * 0.05  # tiny nudge to prevent pathological ordering

    def _pop_best_pipeline(allowed_prios, scan_k=8):
        """
        Pick a pipeline to schedule next.
        We scan only the first k entries in each allowed queue to keep it cheap/deterministic.
        """
        best = None
        best_prio = None
        best_idx = None
        best_score = None

        for pr in allowed_prios:
            q = s._queues.get(pr, [])
            if not q:
                continue
            # Scan head-of-line plus a small window to reduce HOL blocking.
            upto = min(len(q), scan_k)
            for i in range(upto):
                p = q[i]
                if not _cleanup_pipeline(p):
                    continue
                score = _effective_pipeline_score(p)
                if (best is None) or (score > best_score):
                    best = p
                    best_prio = pr
                    best_idx = i
                    best_score = score

        if best is None:
            # Also perform a light cleanup pass on queue heads (drop completed/failed).
            for pr in allowed_prios:
                q = s._queues.get(pr, [])
                while q and (not _cleanup_pipeline(q[0])):
                    q.pop(0)
            return None

        # Remove chosen pipeline from its queue.
        s._queues[best_prio].pop(best_idx)
        return best

    def _priority_resource_fractions(prio, pool_id: int, num_pools: int):
        """
        Return (cpu_frac, ram_frac) *of pool max* for a single op.
        High priority gets bigger slices to reduce latency.
        Low priority is capped to preserve headroom.
        """
        if prio == Priority.QUERY:
            return 0.75, 0.50
        if prio == Priority.INTERACTIVE:
            return 0.50, 0.40
        # Batch:
        # If we have multiple pools, prefer batch to use non-zero pools more aggressively.
        if num_pools > 1 and pool_id != 0:
            return 0.50, 0.50
        return 0.25, 0.25

    def _compute_allocation(p: Pipeline, op, pool, pool_id: int):
        """Compute (cpu, ram) for an op using hints + priority fractions."""
        cpu_frac, ram_frac = _priority_resource_fractions(p.priority, pool_id, s.executor.num_pools)

        # Start with priority-based sizing, bounded by what's available.
        desired_cpu = min(pool.avail_cpu_pool, pool.max_cpu_pool * cpu_frac)
        desired_ram = min(pool.avail_ram_pool, pool.max_ram_pool * ram_frac)

        # Apply OOM-backed RAM hint if we have one.
        op_id = id(op)
        hint_ram = s._ram_hint_by_op_id.get(op_id, None)
        if hint_ram is not None:
            desired_ram = max(desired_ram, hint_ram)
            desired_ram = min(desired_ram, pool.avail_ram_pool)

        # Guardrails against zero allocations.
        desired_cpu = max(s._min_cpu, desired_cpu)
        desired_ram = max(s._min_ram, desired_ram)

        # If we can't actually fit even the minimum, signal not schedulable.
        if pool.avail_cpu_pool < s._min_cpu or pool.avail_ram_pool < s._min_ram:
            return None, None
        if desired_cpu > pool.avail_cpu_pool or desired_ram > pool.avail_ram_pool:
            desired_cpu = min(desired_cpu, pool.avail_cpu_pool)
            desired_ram = min(desired_ram, pool.avail_ram_pool)
        return desired_cpu, desired_ram

    # ----------------------------
    # Tick + ingest new pipelines
    # ----------------------------
    s._tick += 1

    for p in pipelines:
        if p.pipeline_id in s._known_pipeline_ids:
            continue
        s._known_pipeline_ids.add(p.pipeline_id)
        s._enqueue_tick_by_pipeline_id[p.pipeline_id] = s._tick
        if p.priority not in s._queues:
            # Defensive: if an unknown priority appears, treat as batch.
            s._queues[Priority.BATCH_PIPELINE].append(p)
        else:
            s._queues[p.priority].append(p)

    # ----------------------------
    # Process results (OOM retry hints)
    # ----------------------------
    for r in results:
        if not r.failed():
            # On success, we could record "works at this size" hints, but avoid
            # shrinking aggressively (shrinking can cause oscillation).
            for op in (r.ops or []):
                op_id = id(op)
                if r.ram is not None:
                    prev = s._ram_hint_by_op_id.get(op_id, None)
                    if prev is None:
                        s._ram_hint_by_op_id[op_id] = max(s._min_ram, r.ram)
                    else:
                        s._ram_hint_by_op_id[op_id] = max(prev, r.ram)
            continue

        oom = _is_oom_error(r.error)
        for op in (r.ops or []):
            op_id = id(op)
            if oom:
                s._retryable_failed_ops.add(op_id)
                # Exponential RAM backoff based on the attempted allocation.
                prev_hint = s._ram_hint_by_op_id.get(op_id, max(s._min_ram, r.ram if r.ram else s._min_ram))
                attempted = max(s._min_ram, r.ram if r.ram else prev_hint)
                new_hint = max(prev_hint, attempted * s._oom_ram_multiplier)
                s._ram_hint_by_op_id[op_id] = new_hint
                s._oom_retries_by_op_id[op_id] = s._oom_retries_by_op_id.get(op_id, 0) + 1
            else:
                # Non-OOM failures are treated as non-retryable in this "small improvement" policy.
                s._nonretryable_failed_ops.add(op_id)

    # Early exit if nothing to do.
    if not pipelines and not results:
        return [], []

    suspensions: List[Suspend] = []
    assignments: List[Assignment] = []

    # ----------------------------
    # Scheduling
    # ----------------------------
    # Per-tick cap: avoid scheduling too many ops from the same pipeline in one tick
    # (keeps behavior closer to FIFO and avoids one DAG flooding assignments).
    scheduled_ops_by_pipeline_id = {}

    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        if pool.avail_cpu_pool <= 0 or pool.avail_ram_pool <= 0:
            continue

        # If multiple pools exist, treat pool 0 as "latency pool" (prefer high priority).
        if s.executor.num_pools > 1 and pool_id == 0:
            allowed_prios = [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]
            # If any high-priority work exists, avoid placing batch on pool 0 to preserve headroom.
            high_exists = bool(s._queues[Priority.QUERY] or s._queues[Priority.INTERACTIVE])
            if high_exists:
                allowed_prios = [Priority.QUERY, Priority.INTERACTIVE]
        else:
            # Non-zero pools: allow everything, but batch likely dominates here.
            allowed_prios = [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]

        # Fill the pool with one-op assignments while resources remain.
        # Use a bounded number of iterations to avoid spinning when pipelines have no ready ops.
        max_iters = 64
        iters = 0
        while iters < max_iters and pool.avail_cpu_pool > 0 and pool.avail_ram_pool > 0:
            iters += 1

            pipeline = _pop_best_pipeline(allowed_prios, scan_k=8)
            if pipeline is None:
                break

            # Drop completed/failed pipelines.
            if not _cleanup_pipeline(pipeline):
                continue

            # Per-pipeline per-tick limit (slightly higher for high priority).
            per_tick_limit = 2 if pipeline.priority in (Priority.QUERY, Priority.INTERACTIVE) else 1
            pid = pipeline.pipeline_id
            if scheduled_ops_by_pipeline_id.get(pid, 0) >= per_tick_limit:
                # Requeue and move on to other pipelines.
                s._queues[pipeline.priority].append(pipeline)
                continue

            ready_ops = _pipeline_ready_ops(pipeline)
            if not ready_ops:
                # Nothing ready now (parents not complete / currently running); requeue.
                s._queues[pipeline.priority].append(pipeline)
                continue

            # Assign a single op (keeps changes small vs naive baseline).
            op = ready_ops[0]
            cpu, ram = _compute_allocation(pipeline, op, pool, pool_id)
            if cpu is None or ram is None:
                # Can't fit; requeue and stop trying to pack this pool further.
                s._queues[pipeline.priority].append(pipeline)
                break

            # If this op exceeded max OOM retries, do not keep thrashing.
            op_id = id(op)
            if s._oom_retries_by_op_id.get(op_id, 0) > s._max_oom_retries:
                # Mark as non-retryable to allow pipeline cleanup to drop it deterministically.
                s._nonretryable_failed_ops.add(op_id)
                s._queues[pipeline.priority].append(pipeline)
                continue

            assignment = Assignment(
                ops=[op],
                cpu=cpu,
                ram=ram,
                priority=pipeline.priority,
                pool_id=pool_id,
                pipeline_id=pipeline.pipeline_id,
            )
            assignments.append(assignment)

            # Account for this pipeline's usage this tick.
            scheduled_ops_by_pipeline_id[pid] = scheduled_ops_by_pipeline_id.get(pid, 0) + 1

            # Requeue pipeline for subsequent ops (FIFO within priority).
            s._queues[pipeline.priority].append(pipeline)

            # NOTE: We do not manually subtract from pool.avail_* because the simulator's
            # executor/pool model typically applies assignments after this step. We rely on
            # the pool.avail_* values only as a snapshot for this tick.

            # To avoid issuing multiple assignments that exceed the same snapshot, we conservatively
            # stop after one assignment if resources are tight.
            if pool.avail_cpu_pool < (2 * s._min_cpu) or pool.avail_ram_pool < (2 * s._min_ram):
                break

    return suspensions, assignments
