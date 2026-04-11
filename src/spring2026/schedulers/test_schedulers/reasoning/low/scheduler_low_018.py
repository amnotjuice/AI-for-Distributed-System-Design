# policy_key: scheduler_low_018
# reasoning_effort: low
# model: gpt-5.2-2025-12-11
# llm_cost: 0.040891
# generation_seconds: 56.72
# generated_at: 2026-04-09T21:16:31.516228
@register_scheduler_init(key="scheduler_low_018")
def scheduler_low_018_init(s):
    """Priority-weighted, OOM-aware, non-dropping scheduler.

    Goals aligned to the score:
      - Strongly protect QUERY and INTERACTIVE latency (dominant weights)
      - Avoid permanent failures (each failure/incomplete costs 720s)
      - Keep BATCH making progress via aging (no starvation)

    Key ideas:
      1) Three FIFO queues (per priority) with aging-based selection.
      2) Conservative-but-fast default sizing per priority (scale-up bias).
      3) OOM retry: if an op fails with OOM, requeue and increase RAM guess.
      4) Non-OOM failures are treated as terminal (do not spin forever).
    """
    s.time = 0  # logical tick counter for aging
    s.q_query = []
    s.q_interactive = []
    s.q_batch = []

    # Pipeline metadata for aging / retry handling
    s.arrival_time = {}  # pipeline_id -> tick
    s.nonretryable_failed = set()  # pipeline_ids that had terminal failures

    # Per-operator RAM guess (keyed by (pipeline_id, op_identity))
    s.ram_guess = {}

    # Track last used pool sizing bounds (used to cap RAM increases sanely)
    s.pool_max_ram_seen = 0


@register_scheduler(key="scheduler_low_018")
def scheduler_low_018(s, results, pipelines):
    """
    Scheduler step:
      - Enqueue new pipelines
      - Process results to learn from OOM failures (increase RAM guess)
      - For each pool, repeatedly assign one runnable op at a time, selecting
        the next pipeline by (priority weight + aging), while ensuring parents
        are complete.
    """
    s.time += 1

    # ---- helpers (kept inside for portability with the simulator environment) ----
    def _prio_weight(p):
        if p == Priority.QUERY:
            return 10
        if p == Priority.INTERACTIVE:
            return 5
        return 1

    def _enqueue(pipeline, front=False):
        pid = pipeline.pipeline_id
        if pid not in s.arrival_time:
            s.arrival_time[pid] = s.time

        if pipeline.priority == Priority.QUERY:
            q = s.q_query
        elif pipeline.priority == Priority.INTERACTIVE:
            q = s.q_interactive
        else:
            q = s.q_batch

        if front:
            q.insert(0, pipeline)
        else:
            q.append(pipeline)

    def _peek_queues():
        # Return tuple list: (queue_ref, pipeline_or_None)
        return [
            (s.q_query, s.q_query[0] if s.q_query else None),
            (s.q_interactive, s.q_interactive[0] if s.q_interactive else None),
            (s.q_batch, s.q_batch[0] if s.q_batch else None),
        ]

    def _pop_front(q):
        if q:
            return q.pop(0)
        return None

    def _is_oom_error(err):
        if not err:
            return False
        msg = str(err).lower()
        return ("oom" in msg) or ("out of memory" in msg) or ("memoryerror" in msg)

    def _op_key(pipeline_id, op):
        # Use a stable-ish identity for the op across retries within this run.
        # Prefer common attributes when present; otherwise fall back to Python object id.
        op_id = None
        for attr in ("op_id", "operator_id", "task_id", "node_id", "name", "id"):
            if hasattr(op, attr):
                try:
                    v = getattr(op, attr)
                    if v is not None:
                        op_id = v
                        break
                except Exception:
                    pass
        if op_id is None:
            op_id = id(op)
        return (pipeline_id, op_id)

    def _default_caps(pool):
        # Caps are fractions of pool maxima; tuned to favor scale-up for interactive work
        # while still leaving room for some concurrency.
        return {
            Priority.QUERY: (0.85, 0.75),        # (cpu_frac, ram_frac)
            Priority.INTERACTIVE: (0.70, 0.60),
            Priority.BATCH_PIPELINE: (0.55, 0.50),
        }

    def _choose_resources(pool, pipeline, op):
        # CPU: allocate up to a priority cap but not more than available.
        # RAM: allocate max(default cap, learned guess), but not more than available.
        caps = _default_caps(pool)
        cpu_frac, ram_frac = caps.get(pipeline.priority, (0.55, 0.50))

        # Base targets from pool maxima (scale-up bias)
        target_cpu = max(1.0, pool.max_cpu_pool * cpu_frac)
        target_ram = max(1.0, pool.max_ram_pool * ram_frac)

        # Learned RAM guess overrides upward to avoid repeated OOMs
        k = _op_key(pipeline.pipeline_id, op)
        if k in s.ram_guess:
            if s.ram_guess[k] > target_ram:
                target_ram = s.ram_guess[k]

        # Clamp to availability
        cpu = min(pool.avail_cpu_pool, target_cpu)
        ram = min(pool.avail_ram_pool, target_ram)

        # Ensure at least some meaningful allocation
        if cpu < 1.0 or ram < 1.0:
            return None, None

        return cpu, ram

    def _pipeline_done_or_terminal(pipeline):
        pid = pipeline.pipeline_id
        if pid in s.nonretryable_failed:
            return True
        st = pipeline.runtime_status()
        if st.is_pipeline_successful():
            return True
        return False

    def _best_queue_index_for_pool(pool):
        # Choose among the front of each queue via (priority_weight * 1000 + age),
        # but only if the pipeline has a runnable op. We keep this cheap by only peeking
        # at queue fronts; we rotate blocked pipelines to the back.
        best_idx = None
        best_score = None
        queues = [s.q_query, s.q_interactive, s.q_batch]

        for i, q in enumerate(queues):
            if not q:
                continue
            p = q[0]
            if _pipeline_done_or_terminal(p):
                # We'll drop it lazily at pop time.
                pass

            pid = p.pipeline_id
            age = max(0, s.time - s.arrival_time.get(pid, s.time))
            score = _prio_weight(p.priority) * 1000 + age

            if best_score is None or score > best_score:
                best_score = score
                best_idx = i

        return best_idx

    # ---- ingest new pipelines ----
    for p in pipelines:
        _enqueue(p, front=False)

    # ---- process results (learn from failures; do not drop on OOM) ----
    for r in results:
        # Update observed pool maxima (used to cap RAM increases)
        try:
            pool = s.executor.pools[r.pool_id]
            if pool.max_ram_pool > s.pool_max_ram_seen:
                s.pool_max_ram_seen = pool.max_ram_pool
        except Exception:
            pass

        if not r.failed():
            continue

        # Learn only from OOM-like failures; otherwise mark pipeline as terminal failed.
        if _is_oom_error(getattr(r, "error", None)):
            # Increase RAM guess for each op in the failed container.
            # Also re-queue the pipeline at the front to retry quickly (protect latency).
            for op in getattr(r, "ops", []) or []:
                k = _op_key(getattr(r, "pipeline_id", None) or "unknown", op)
                prev = s.ram_guess.get(k, max(1.0, float(getattr(r, "ram", 1.0))))
                # Exponential backoff; cap to a seen max, with a safe fallback.
                cap = max(1.0, float(s.pool_max_ram_seen or (getattr(r, "ram", 1.0) * 8.0)))
                new_guess = min(cap, max(prev * 2.0, float(getattr(r, "ram", prev)) * 1.5))
                s.ram_guess[k] = new_guess
        else:
            # Non-retryable failure: avoid infinite retries/spin.
            # We can't reliably map result -> pipeline_id here; when available, use it.
            pid = getattr(r, "pipeline_id", None)
            if pid is not None:
                s.nonretryable_failed.add(pid)

    # ---- scheduling ----
    suspensions = []
    assignments = []

    # Early exit if nothing changed and no work queued
    if not pipelines and not results and not (s.q_query or s.q_interactive or s.q_batch):
        return suspensions, assignments

    # For each pool, greedily pack 1-op assignments while resources remain.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]

        # If pool has no headroom, skip
        if pool.avail_cpu_pool < 1.0 or pool.avail_ram_pool < 1.0:
            continue

        # Avoid infinite loops when pipelines are blocked on parents
        guard = 0
        max_iters = (len(s.q_query) + len(s.q_interactive) + len(s.q_batch) + 5) * 3

        while pool.avail_cpu_pool >= 1.0 and pool.avail_ram_pool >= 1.0 and guard < max_iters:
            guard += 1

            idx = _best_queue_index_for_pool(pool)
            if idx is None:
                break

            q = [s.q_query, s.q_interactive, s.q_batch][idx]
            pipeline = _pop_front(q)
            if pipeline is None:
                continue

            # Drop completed/terminal pipelines lazily
            if _pipeline_done_or_terminal(pipeline):
                continue

            st = pipeline.runtime_status()

            # If there are failures, only keep retrying if they are likely OOM.
            # (We don't have per-op failure type here; rely on nonretryable_failed set.)
            if pipeline.pipeline_id in s.nonretryable_failed:
                continue

            # Choose one runnable operator (parents must be complete)
            op_list = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
            if not op_list:
                # Blocked (parents running or none assignable yet): rotate to back to avoid head-of-line blocking.
                _enqueue(pipeline, front=False)
                continue

            op = op_list[0]
            cpu, ram = _choose_resources(pool, pipeline, op)
            if cpu is None or ram is None:
                # Not enough resources right now; put it back at the front (to be picked when resources free),
                # and stop scheduling this pool this tick.
                _enqueue(pipeline, front=True)
                break

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

            # We assume the simulator deducts pool resources after assignments are applied.
            # To prevent over-assigning within this same tick, pessimistically decrement locally too.
            try:
                pool.avail_cpu_pool -= cpu
                pool.avail_ram_pool -= ram
            except Exception:
                # If pool fields are read-only, rely on the simulator's enforcement.
                pass

            # Requeue the pipeline to continue once this op completes (keeps fairness across pipelines).
            _enqueue(pipeline, front=False)

    return suspensions, assignments
