# policy_key: scheduler_medium_015
# reasoning_effort: medium
# model: gpt-5.2-2025-12-11
# llm_cost: 0.078903
# generation_seconds: 69.19
# generated_at: 2026-03-12T22:14:36.172253
@register_scheduler_init(key="scheduler_medium_015")
def scheduler_medium_015_init(s):
    """Priority-aware FIFO with small, safe improvements over naive FIFO.

    Improvements (kept intentionally modest / low-risk):
      1) Separate waiting queues per priority; always consider higher priority first.
      2) Avoid head-of-line blocking by scanning queues for runnable ops (parents complete).
      3) Allow limited parallelism per pipeline (more for higher priority).
      4) Right-size per-op CPU/RAM by priority using simple pool-fraction caps (prevents one op
         from grabbing the entire pool and blocking latency-sensitive work).
      5) Best-effort OOM adaptation: if we observe OOM-like failures for a pipeline, increase
         its RAM boost factor for subsequent retries (when the simulator provides pipeline id).
    """
    # Per-priority FIFO queues of Pipeline objects
    s.waiting = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }

    # Round-robin cursor per priority (implemented by rotating list)
    s.tick = 0

    # Track per-pipeline RAM boost after suspected OOMs; conservative default is 1.0
    # Keyed by pipeline_id when available.
    s.ram_boost = {}  # pipeline_id -> float

    # Recent OOM signals (pipeline_id -> expiry_tick) to decide whether to keep retrying failures
    s.oom_recent_until = {}  # pipeline_id -> int

    # Starvation protection: after too many high-priority picks, force a batch pick if possible
    s.hp_streak = 0


@register_scheduler(key="scheduler_medium_015")
def scheduler_medium_015_scheduler(s, results: List["ExecutionResult"], pipelines: List["Pipeline"]):
    """
    Scheduling loop:
      - Enqueue new pipelines into priority queues.
      - Process results to detect OOM-like failures and increase per-pipeline RAM boost.
      - For each pool, greedily assign ready operators while resources remain, choosing
        pipelines in priority order with a small anti-starvation rule for batch.
    """
    s.tick += 1

    def _priority_rank(pri):
        # Higher comes first
        if pri == Priority.QUERY:
            return 0
        if pri == Priority.INTERACTIVE:
            return 1
        return 2

    def _is_oom_error(err):
        if err is None:
            return False
        try:
            msg = str(err).lower()
        except Exception:
            return False
        # Common substrings across runtimes/simulators
        return ("oom" in msg) or ("out of memory" in msg) or ("memoryerror" in msg)

    def _get_pipeline_id_from_result(r):
        # The simulator may or may not expose pipeline_id on results; try a few options.
        pid = getattr(r, "pipeline_id", None)
        if pid is not None:
            return pid
        ops = getattr(r, "ops", None) or []
        if ops:
            pid = getattr(ops[0], "pipeline_id", None)
            if pid is not None:
                return pid
            # Sometimes nested metadata exists
            meta = getattr(ops[0], "meta", None)
            if meta is not None and isinstance(meta, dict):
                pid = meta.get("pipeline_id")
                if pid is not None:
                    return pid
        return None

    def _cleanup_expired_oom_marks():
        expired = [pid for pid, until in s.oom_recent_until.items() if until <= s.tick]
        for pid in expired:
            s.oom_recent_until.pop(pid, None)

    def _enqueue_pipeline(p):
        # Ensure the priority key exists; default unknowns to batch-like behavior.
        pri = getattr(p, "priority", Priority.BATCH_PIPELINE)
        if pri not in s.waiting:
            pri = Priority.BATCH_PIPELINE
        s.waiting[pri].append(p)

    def _pipeline_is_retriable(p):
        # Default: if any failures exist, we only keep it if we recently saw OOM for this pipeline.
        # This keeps behavior close to the naive baseline (drop failures) but allows OOM recovery.
        status = p.runtime_status()
        has_failures = status.state_counts.get(OperatorState.FAILED, 0) > 0
        if not has_failures:
            return True
        pid = getattr(p, "pipeline_id", None)
        if pid is None:
            return False
        until = s.oom_recent_until.get(pid, 0)
        return until > s.tick

    def _inflight_ops_count(p):
        status = p.runtime_status()
        return status.state_counts.get(OperatorState.RUNNING, 0) + status.state_counts.get(OperatorState.ASSIGNED, 0)

    def _max_inflight_for_priority(pri):
        # Higher priority can use a bit more parallelism to reduce tail latency.
        if pri == Priority.QUERY:
            return 2
        if pri == Priority.INTERACTIVE:
            return 2
        return 1

    def _caps_for_priority(pool, pri):
        # Simple fractions of the pool to prevent single-op monopolization.
        # Still allows scale-up bias (we allocate a "large slice", just not everything).
        if pri == Priority.QUERY:
            cpu_cap = max(1.0, 0.75 * pool.max_cpu_pool)
            ram_cap = max(0.0, 0.75 * pool.max_ram_pool)
        elif pri == Priority.INTERACTIVE:
            cpu_cap = max(1.0, 0.50 * pool.max_cpu_pool)
            ram_cap = max(0.0, 0.50 * pool.max_ram_pool)
        else:
            cpu_cap = max(1.0, 0.25 * pool.max_cpu_pool)
            ram_cap = max(0.0, 0.25 * pool.max_ram_pool)
        return cpu_cap, ram_cap

    def _pick_next_priority():
        # Anti-starvation: after enough high-priority picks, try to schedule a batch item.
        if s.hp_streak >= 8 and s.waiting[Priority.BATCH_PIPELINE]:
            return Priority.BATCH_PIPELINE
        # Otherwise, pick highest priority that has anything waiting.
        for pri in (Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE):
            if s.waiting[pri]:
                return pri
        return None

    def _rotate_and_find_runnable(pri):
        """Rotate queue to find a pipeline with a ready op and acceptable inflight count.
        Returns (pipeline, op_list) or (None, None).
        """
        q = s.waiting[pri]
        if not q:
            return None, None

        n = len(q)
        for _ in range(n):
            p = q.pop(0)

            status = p.runtime_status()

            # Drop completed pipelines
            if status.is_pipeline_successful():
                continue

            # Drop non-retriable failures
            if not _pipeline_is_retriable(p):
                continue

            # Respect max inflight per pipeline
            if _inflight_ops_count(p) >= _max_inflight_for_priority(p.priority):
                q.append(p)
                continue

            # Find a single ready operator (parents complete)
            op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
            if not op_list:
                q.append(p)
                continue

            # Put it back at end (round-robin fairness within priority) and return runnable op.
            q.append(p)
            return p, op_list

        return None, None

    # Enqueue new pipelines
    for p in pipelines:
        _enqueue_pipeline(p)

    # Process results for OOM hints (best-effort)
    for r in results:
        try:
            failed = r.failed()
        except Exception:
            failed = False

        if failed and _is_oom_error(getattr(r, "error", None)):
            pid = _get_pipeline_id_from_result(r)
            if pid is not None:
                # Mark "recent OOM" so FAILED ops can be retried.
                s.oom_recent_until[pid] = max(s.oom_recent_until.get(pid, 0), s.tick + 5)

                # Increase RAM boost for subsequent attempts (capped to avoid runaway).
                prev = float(s.ram_boost.get(pid, 1.0))
                s.ram_boost[pid] = min(prev * 2.0, 8.0)

    _cleanup_expired_oom_marks()

    # Early exit if nothing to do
    if not pipelines and not results and all(len(q) == 0 for q in s.waiting.values()):
        return [], []

    suspensions = []
    assignments = []

    # Greedily schedule within each pool using local accounting (since pool avail won't reflect our decisions yet)
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)

        # If essentially no room, skip
        if avail_cpu <= 0.0 or avail_ram <= 0.0:
            continue

        # Keep assigning while we have meaningful resources and runnable work exists
        # Guard with a max-iterations bound to avoid infinite loops on weird states.
        max_iters = 128
        iters = 0
        while iters < max_iters:
            iters += 1

            pri = _pick_next_priority()
            if pri is None:
                break

            p, op_list = _rotate_and_find_runnable(pri)
            if p is None or not op_list:
                # No runnable items in that priority; temporarily pretend it's empty and try others
                # by checking the next priorities in order.
                found = False
                for pri2 in sorted(s.waiting.keys(), key=_priority_rank):
                    if pri2 == pri:
                        continue
                    if not s.waiting[pri2]:
                        continue
                    p2, op_list2 = _rotate_and_find_runnable(pri2)
                    if p2 is not None and op_list2:
                        p, op_list, pri = p2, op_list2, pri2
                        found = True
                        break
                if not found:
                    break

            # Compute per-priority caps and apply RAM boost if known
            cpu_cap, ram_cap = _caps_for_priority(pool, pri)

            pid = getattr(p, "pipeline_id", None)
            boost = float(s.ram_boost.get(pid, 1.0)) if pid is not None else 1.0

            # Allocate a "large slice" but not the whole pool; bounded by what's left.
            cpu_alloc = min(avail_cpu, cpu_cap)
            ram_alloc = min(avail_ram, ram_cap * boost)

            # Minimum CPU to avoid degenerate allocations
            if cpu_alloc < 1.0:
                break
            # If RAM alloc is too tiny, stop for this pool
            if ram_alloc <= 0.0:
                break

            assignments.append(
                Assignment(
                    ops=op_list,
                    cpu=cpu_alloc,
                    ram=ram_alloc,
                    priority=p.priority,
                    pool_id=pool_id,
                    pipeline_id=p.pipeline_id,
                )
            )

            # Local accounting
            avail_cpu -= cpu_alloc
            avail_ram -= ram_alloc

            # Update anti-starvation streak
            if pri in (Priority.QUERY, Priority.INTERACTIVE):
                s.hp_streak += 1
            else:
                s.hp_streak = 0

            # If we can't fit at least a minimal next task, stop
            if avail_cpu < 1.0 or avail_ram <= 0.0:
                break

    return suspensions, assignments
