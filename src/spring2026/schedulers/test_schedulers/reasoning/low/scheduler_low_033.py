# policy_key: scheduler_low_033
# reasoning_effort: low
# model: gpt-5.2-2025-12-11
# llm_cost: 0.046435
# generation_seconds: 53.98
# generated_at: 2026-04-09T21:29:18.940924
@register_scheduler_init(key="scheduler_low_033")
def scheduler_low_033_init(s):
    """
    Priority + fairness + OOM-adaptive sizing scheduler.

    Goals aligned with weighted-latency objective:
      - Strongly protect QUERY and INTERACTIVE by always considering them first,
        but avoid starving BATCH via token-based fairness + aging.
      - Avoid failures (720s penalty) by retrying FAILED operators (ASSIGNABLE_STATES)
        with increasing RAM "hints" when failures look like OOM.
      - Keep concurrency reasonable by giving small/moderate CPU slices per op
        rather than always maxing out a pool on a single op.
    """
    # Local imports allowed inside functions per instructions
    from collections import deque

    # Book-keeping for all pipelines that have arrived but aren't yet finished
    s._pipelines_by_id = {}           # pipeline_id -> Pipeline
    s._done_pipeline_ids = set()      # pipeline_ids already completed successfully

    # Priority queues (store pipeline_id; the canonical Pipeline is in _pipelines_by_id)
    s._q_query = deque()
    s._q_interactive = deque()
    s._q_batch = deque()

    # Fairness / aging
    s._tick = 0
    s._enqueue_tick = {}              # pipeline_id -> tick when (re)enqueued
    s._tokens = {
        Priority.QUERY: 0,
        Priority.INTERACTIVE: 0,
        Priority.BATCH_PIPELINE: 0,
    }

    # Adaptive sizing and failure handling
    s._ram_mult = {}                  # pipeline_id -> multiplier applied to base RAM slice
    s._oom_failures = {}              # pipeline_id -> count of suspected OOM failures
    s._nonoom_failures = {}           # pipeline_id -> count of non-OOM failures (guardrail)


@register_scheduler(key="scheduler_low_033")
def scheduler_low_033_scheduler(s, results, pipelines):
    """
    Scheduling logic per tick:
      1) Ingest new pipelines; keep them in per-priority queues.
      2) Process results to adjust RAM hints (OOM -> increase RAM multiplier).
      3) For each pool, assign runnable operators using:
           - priority-first (QUERY > INTERACTIVE > BATCH),
           - token-based fairness (weights 10/5/1) to prevent starvation,
           - simple aging boost (long-waiting pipelines bubble up).
      4) Retry FAILED operators (ASSIGNABLE_STATES includes FAILED) rather than dropping.
    """
    from collections import deque

    s._tick += 1

    # -----------------------------
    # Helpers (kept inside for simplicity / no global imports)
    # -----------------------------
    def _prio_weight(prio):
        if prio == Priority.QUERY:
            return 10
        if prio == Priority.INTERACTIVE:
            return 5
        return 1

    def _get_queue(prio):
        if prio == Priority.QUERY:
            return s._q_query
        if prio == Priority.INTERACTIVE:
            return s._q_interactive
        return s._q_batch

    def _suspected_oom(err):
        if err is None:
            return False
        try:
            msg = str(err).lower()
        except Exception:
            return False
        return ("oom" in msg) or ("out of memory" in msg) or ("cuda oom" in msg) or ("memoryerror" in msg)

    def _ensure_pipeline_tracked(p):
        pid = p.pipeline_id
        if pid in s._done_pipeline_ids:
            return
        if pid not in s._pipelines_by_id:
            s._pipelines_by_id[pid] = p
        # Initialize sizing / failure state if missing
        s._ram_mult.setdefault(pid, 1.0)
        s._oom_failures.setdefault(pid, 0)
        s._nonoom_failures.setdefault(pid, 0)

    def _enqueue_pipeline(pid):
        p = s._pipelines_by_id.get(pid)
        if p is None or pid in s._done_pipeline_ids:
            return
        q = _get_queue(p.priority)
        # Avoid duplicates in queue: cheap check by scan (queues are typically small in sim)
        if pid in q:
            return
        q.append(pid)
        s._enqueue_tick[pid] = s._tick

    def _pipeline_finished_or_gone(p):
        if p is None:
            return True
        status = p.runtime_status()
        return status.is_pipeline_successful()

    def _runnable_ops(p):
        status = p.runtime_status()
        # Runnable means pending or failed, and parents completed
        return status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)

    def _pick_next_pipeline_id(candidates):
        """
        Choose among candidate pipeline_ids using:
          - priority tokens (weighted by 10/5/1),
          - aging boost to prevent long-waiting starvation across priorities.
        """
        best_pid = None
        best_score = None

        for pid in candidates:
            p = s._pipelines_by_id.get(pid)
            if p is None or pid in s._done_pipeline_ids:
                continue

            prio = p.priority
            tokens = s._tokens.get(prio, 0)
            age = max(0, s._tick - s._enqueue_tick.get(pid, s._tick))

            # Score: tokens dominate; age provides gradual boost.
            # This keeps QUERY/INTERACTIVE preferred but allows BATCH to move eventually.
            score = (tokens * 1000) + age

            if best_score is None or score > best_score:
                best_score = score
                best_pid = pid

        return best_pid

    def _cpu_slice_for(prio, avail_cpu):
        # Keep slices modest to increase concurrency and reduce tail latency under contention.
        # QUERY gets more CPU by default; BATCH gets minimal CPU.
        if avail_cpu <= 0:
            return 0
        if prio == Priority.QUERY:
            return max(1, min(4, avail_cpu))
        if prio == Priority.INTERACTIVE:
            return max(1, min(2, avail_cpu))
        return 1

    def _base_ram_slice_for(prio, pool, avail_ram):
        # Provide a moderate RAM share to reduce OOM risk (expensive objective penalty),
        # but not so large that only one op can run.
        if avail_ram <= 0:
            return 0

        max_ram = getattr(pool, "max_ram_pool", avail_ram)

        if prio == Priority.QUERY:
            frac = 0.30
        elif prio == Priority.INTERACTIVE:
            frac = 0.22
        else:
            frac = 0.16

        # At least 1 unit, at most what's available
        target = int(max(1, min(avail_ram, max_ram * frac)))
        return target

    # -----------------------------
    # Ingest new pipelines
    # -----------------------------
    for p in pipelines:
        _ensure_pipeline_tracked(p)
        _enqueue_pipeline(p.pipeline_id)

    # -----------------------------
    # Update sizing hints from results
    # -----------------------------
    for r in results:
        # We don't have pipeline_id directly on ExecutionResult in the provided API,
        # so we adjust based on the priority class globally only if we can't map.
        # If the simulator includes r.ops with operator objects containing pipeline_id,
        # we try to infer it.
        inferred_pid = None

        # Attempt inference from r.ops (best-effort; safe if fields don't exist)
        try:
            if getattr(r, "ops", None):
                op0 = r.ops[0]
                inferred_pid = getattr(op0, "pipeline_id", None) or getattr(op0, "pipeline", None)
        except Exception:
            inferred_pid = None

        if inferred_pid is not None:
            pid = inferred_pid
            s._ram_mult.setdefault(pid, 1.0)
            s._oom_failures.setdefault(pid, 0)
            s._nonoom_failures.setdefault(pid, 0)
        else:
            pid = None

        if hasattr(r, "failed") and r.failed():
            if _suspected_oom(getattr(r, "error", None)):
                # Increase RAM multiplier aggressively (OOM failures are extremely costly in objective).
                if pid is not None:
                    s._oom_failures[pid] = s._oom_failures.get(pid, 0) + 1
                    cur = s._ram_mult.get(pid, 1.0)
                    # Multiplicative bump with cap to avoid consuming whole pool forever.
                    s._ram_mult[pid] = min(4.0, cur * 1.5)
                    _enqueue_pipeline(pid)
            else:
                # Non-OOM failures: retry a few times but avoid infinite churn.
                if pid is not None:
                    s._nonoom_failures[pid] = s._nonoom_failures.get(pid, 0) + 1
                    # Small RAM bump sometimes helps borderline failures without hogging.
                    cur = s._ram_mult.get(pid, 1.0)
                    s._ram_mult[pid] = min(2.0, cur * 1.15)
                    _enqueue_pipeline(pid)

    # -----------------------------
    # Re-enqueue pipelines that are still active but not currently queued
    # (e.g., after partial progress, or if queues got out of sync).
    # -----------------------------
    # Keep it light: only touch pipelines we already know about.
    for pid, p in list(s._pipelines_by_id.items()):
        if pid in s._done_pipeline_ids:
            continue
        if p is None:
            continue
        if _pipeline_finished_or_gone(p):
            s._done_pipeline_ids.add(pid)
            continue

        # Ensure it remains queued if it has runnable work.
        try:
            if _runnable_ops(p):
                _enqueue_pipeline(pid)
        except Exception:
            # If runtime_status fails unexpectedly, don't break scheduling loop.
            pass

    # -----------------------------
    # Fairness tokens per tick (weighted like the objective)
    # -----------------------------
    s._tokens[Priority.QUERY] += 10
    s._tokens[Priority.INTERACTIVE] += 5
    s._tokens[Priority.BATCH_PIPELINE] += 1

    # -----------------------------
    # Scheduling / placement
    # -----------------------------
    suspensions = []
    assignments = []

    # No preemption by default: preempting can waste work and increase failures;
    # we rely on admission + priority ordering to protect tail latency.
    # (If the simulator provides running container inventory, this can be extended.)
    #
    # For each pool, schedule multiple ops if resources allow.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool

        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # Build a local candidate set from the head portions of each queue
        # to keep selection efficient but still responsive.
        candidates = []

        def _take_head(q, k):
            out = []
            i = 0
            for pid in q:
                out.append(pid)
                i += 1
                if i >= k:
                    break
            return out

        # Prefer higher-priority queues but still include some batch candidates.
        candidates.extend(_take_head(s._q_query, 16))
        candidates.extend(_take_head(s._q_interactive, 16))
        candidates.extend(_take_head(s._q_batch, 16))

        # If no candidates, skip pool.
        if not candidates:
            continue

        # Try to fill the pool with several assignments (one operator per assignment),
        # while respecting available CPU/RAM.
        # Upper-bound per tick to avoid huge bursts.
        max_assignments_this_pool = 32
        made = 0

        while made < max_assignments_this_pool and avail_cpu > 0 and avail_ram > 0:
            pid = _pick_next_pipeline_id(candidates)
            if pid is None:
                break

            p = s._pipelines_by_id.get(pid)
            if p is None or pid in s._done_pipeline_ids:
                # Remove from all queues if present
                try:
                    if pid in s._q_query:
                        s._q_query.remove(pid)
                    if pid in s._q_interactive:
                        s._q_interactive.remove(pid)
                    if pid in s._q_batch:
                        s._q_batch.remove(pid)
                except Exception:
                    pass
                # Refresh candidates
                candidates = [c for c in candidates if c != pid]
                continue

            status = p.runtime_status()
            if status.is_pipeline_successful():
                s._done_pipeline_ids.add(pid)
                # remove from its queue if present
                q = _get_queue(p.priority)
                try:
                    if pid in q:
                        q.remove(pid)
                except Exception:
                    pass
                candidates = [c for c in candidates if c != pid]
                continue

            # Guardrail: if repeated non-OOM failures, stop spending resources endlessly.
            # (This effectively "drops" by not scheduling further, but avoids infinite churn.)
            if s._nonoom_failures.get(pid, 0) >= 5:
                # Remove from queue and candidate list
                q = _get_queue(p.priority)
                try:
                    if pid in q:
                        q.remove(pid)
                except Exception:
                    pass
                candidates = [c for c in candidates if c != pid]
                continue

            op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
            if not op_list:
                # Nothing runnable now; leave it queued but don't pick again immediately
                candidates = [c for c in candidates if c != pid]
                continue

            prio = p.priority

            cpu = _cpu_slice_for(prio, avail_cpu)
            if cpu <= 0:
                break

            base_ram = _base_ram_slice_for(prio, pool, avail_ram)
            mult = s._ram_mult.get(pid, 1.0)
            ram = int(max(1, min(avail_ram, base_ram * mult)))

            # If RAM becomes too tiny relative to pool, prefer not to schedule to avoid likely OOM churn
            if ram <= 0:
                break

            assignments.append(
                Assignment(
                    ops=op_list,
                    cpu=cpu,
                    ram=ram,
                    priority=prio,
                    pool_id=pool_id,
                    pipeline_id=p.pipeline_id,
                )
            )

            # Consume resources locally for this tick's decisions
            avail_cpu -= cpu
            avail_ram -= ram
            made += 1

            # Consume tokens to enforce fairness:
            # cost = weight, so QUERY burns tokens faster but also accrues more each tick.
            s._tokens[prio] = max(0, s._tokens.get(prio, 0) - _prio_weight(prio))

            # After assigning, remove pid from queue to prevent duplicate scheduling this tick.
            q = _get_queue(prio)
            try:
                if pid in q:
                    q.remove(pid)
            except Exception:
                pass

            # Re-enqueue later (next tick) if it still has work; this keeps round-robin behavior.
            # We do NOT re-enqueue immediately to avoid repeatedly selecting the same pipeline
            # within the same tick/pool.
            s._enqueue_tick[pid] = s._tick

            # Refresh candidates (remove this pid for this loop iteration)
            candidates = [c for c in candidates if c != pid]

    return suspensions, assignments
