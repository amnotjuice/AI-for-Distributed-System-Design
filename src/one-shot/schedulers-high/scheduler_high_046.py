# policy_key: scheduler_high_046
# reasoning_effort: high
# model: gpt-5.2-2025-12-11
# llm_cost: 0.086407
# generation_seconds: 129.63
# generated_at: 2026-03-14T04:29:24.125288
@register_scheduler_init(key="scheduler_high_046")
def scheduler_high_046_init(s):
    """
    Priority-aware, latency-focused evolution of the naive FIFO baseline.

    Small, "obvious" improvements over naive:
      1) Priority queues: schedule QUERY/INTERACTIVE before BATCH.
      2) Better packing: schedule multiple ops per pool per tick (not just one).
      3) OOM-aware retries: on OOM, bump a per-operator RAM hint (exponential backoff).
      4) Headroom protection: when high-priority work exists, don't let batch consume
         the last slice of resources (since we do not implement preemption here).

    Notes:
      - This policy keeps complexity low and avoids relying on executor internals
        (e.g., enumerating running containers).
      - Operators are retried on failure up to s.max_retries; persistent failures
        mark the whole pipeline as "fatal" to avoid infinite loops.
    """
    s.queues = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }
    s.known_pipeline_ids = set()

    # Per-operator hints learned from past executions
    s.ram_hint = {}          # op_key -> minimum RAM to try next time
    s.last_alloc = {}        # op_key -> (cpu, ram) last used allocation
    s.op_fail_count = {}     # op_key -> number of failed attempts
    s.op_to_pipeline = {}    # op_key -> pipeline_id (learned when scheduled)

    # Pipelines we stop scheduling due to repeated failures
    s.fatal_pipelines = set()

    # Retry budget per operator
    s.max_retries = 5


def _op_key(op):
    """Best-effort stable key for an operator across scheduler ticks."""
    # Prefer explicit identifiers if present; otherwise fall back to object identity.
    for attr in ("op_id", "operator_id", "task_id", "node_id", "id"):
        v = getattr(op, attr, None)
        if v is not None:
            return ("attr", attr, v)
    return ("pyid", id(op))


def _is_oom_error(err):
    if not err:
        return False
    try:
        msg = str(err).lower()
    except Exception:
        return False
    return ("oom" in msg) or ("out of memory" in msg) or ("out-of-memory" in msg)


def _pool_targets(pool, priority):
    """
    Heuristic per-op sizing targets by priority.
    Chosen to improve latency for high priority while still allowing some parallelism.
    """
    max_cpu = int(getattr(pool, "max_cpu_pool", 0) or 0)
    max_ram = int(getattr(pool, "max_ram_pool", 0) or 0)

    # Fallbacks in case max_* are unset (shouldn't happen, but keep safe).
    if max_cpu <= 0:
        max_cpu = 1
    if max_ram <= 0:
        max_ram = 1

    if priority in (Priority.QUERY, Priority.INTERACTIVE):
        cpu_t = max(1, int(max_cpu * 0.50))
        ram_t = max(1, int(max_ram * 0.25))
    else:
        cpu_t = max(1, int(max_cpu * 0.25))
        ram_t = max(1, int(max_ram * 0.15))

    return cpu_t, ram_t


def _headroom(pool):
    """Resources to keep free for potential high-priority arrivals (no preemption)."""
    max_cpu = int(getattr(pool, "max_cpu_pool", 0) or 0)
    max_ram = int(getattr(pool, "max_ram_pool", 0) or 0)
    if max_cpu <= 0:
        max_cpu = 1
    if max_ram <= 0:
        max_ram = 1
    return max(1, int(max_cpu * 0.20)), max(1, int(max_ram * 0.20))


def _cleanup_queues(s):
    """Drop completed/fatal pipelines from queues."""
    for prio, q in s.queues.items():
        new_q = []
        for p in q:
            if p.pipeline_id in s.fatal_pipelines:
                continue
            st = p.runtime_status()
            if st.is_pipeline_successful():
                continue
            new_q.append(p)
        s.queues[prio] = new_q


def _select_candidate(s, prio, avail_cpu, avail_ram, pool, high_prio_waiting):
    """
    Find (pipeline, op, cpu_req, ram_req) for this priority that fits in current avail.
    Implements a round-robin scan of the priority queue (simple fairness within prio).
    """
    q = s.queues[prio]
    if not q:
        return None

    # Protect headroom when high priority exists, by restricting batch allocations.
    head_cpu, head_ram = _headroom(pool)
    enforce_headroom = (prio == Priority.BATCH_PIPELINE) and high_prio_waiting

    # Scan up to len(q) entries; rotate to preserve FIFO-ish fairness.
    n = len(q)
    for _ in range(n):
        p = q.pop(0)

        # Skip pipelines we should not run.
        if p.pipeline_id in s.fatal_pipelines:
            continue

        st = p.runtime_status()
        if st.is_pipeline_successful():
            continue

        # Only schedule ops whose parents are complete.
        op_list = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
        if not op_list:
            # Not ready right now; rotate to end and keep it.
            q.append(p)
            continue

        op = op_list[0]
        ok = _op_key(op)

        # Compute a reasonable CPU/RAM request.
        cpu_target, ram_target = _pool_targets(pool, p.priority)

        # RAM hint logic: if we saw OOM before, do not go below hint.
        hint = int(s.ram_hint.get(ok, 0) or 0)

        # CPU: allow scaling down to what's available (but never below 1).
        cpu_req = min(int(avail_cpu), int(cpu_target))
        cpu_req = max(1, cpu_req)

        # RAM: prefer target, but allow shrinking if no hint is known.
        desired_ram = max(int(ram_target), int(hint))
        if desired_ram <= avail_ram:
            ram_req = desired_ram
        else:
            if hint > 0:
                # Can't satisfy known minimum -> skip this op for now.
                q.append(p)
                continue
            # No hint: try with whatever remains (still at least 1).
            ram_req = max(1, int(avail_ram))

        # Must fit current available resources.
        if cpu_req > avail_cpu or ram_req > avail_ram:
            q.append(p)
            continue

        # If enforcing headroom for batch, don't consume the last slice.
        if enforce_headroom:
            if (avail_cpu - cpu_req) < head_cpu or (avail_ram - ram_req) < head_ram:
                q.append(p)
                continue

        # Candidate accepted; rotate pipeline to end (so next time others get a chance).
        q.append(p)
        return (p, op, cpu_req, ram_req)

    return None


@register_scheduler(key="scheduler_high_046")
def scheduler_high_046(s, results: List[ExecutionResult], pipelines: List[Pipeline]) -> Tuple[List[Suspend], List[Assignment]]:
    """
    Priority-aware multi-assignment scheduler with OOM-based RAM backoff.

    The algorithm:
      - Enqueue new pipelines by priority (QUERY/INTERACTIVE/BATCH).
      - Update RAM hints and retry counters from execution results.
      - For each pool, greedily pack assignments:
          * Always try high priority first (QUERY, INTERACTIVE), then BATCH.
          * When any high-priority is waiting, keep a headroom buffer by limiting
            batch allocations (no preemption).
    """
    # Enqueue new pipelines (avoid duplicates).
    for p in pipelines:
        if p.pipeline_id in s.known_pipeline_ids:
            continue
        s.known_pipeline_ids.add(p.pipeline_id)
        s.queues[p.priority].append(p)

    # If nothing changed, avoid work (mirrors baseline pattern).
    if not pipelines and not results:
        return [], []

    # Learn from results.
    for r in results:
        if not getattr(r, "ops", None):
            continue

        failed = False
        try:
            failed = r.failed()
        except Exception:
            failed = False

        if not failed:
            continue

        is_oom = _is_oom_error(getattr(r, "error", None))

        # Update per-op tracking for all ops in this container.
        for op in r.ops:
            ok = _op_key(op)

            # Count failures and cap retries.
            s.op_fail_count[ok] = int(s.op_fail_count.get(ok, 0) or 0) + 1

            # Record/refresh mapping (if known from scheduling) to allow pipeline kill.
            pid = s.op_to_pipeline.get(ok, None)

            # OOM -> bump RAM hint exponentially from observed allocation.
            if is_oom:
                prev = int(s.ram_hint.get(ok, 0) or 0)
                obs_ram = int(getattr(r, "ram", 0) or 0)
                if obs_ram <= 0:
                    obs_ram = int(s.last_alloc.get(ok, (0, 1))[1] or 1)
                new_hint = max(prev, max(obs_ram + 1, obs_ram * 2))
                s.ram_hint[ok] = new_hint
            else:
                # Non-OOM failures: mark fatal quickly to prevent infinite churn.
                if pid is not None:
                    s.fatal_pipelines.add(pid)

            # Too many retries -> mark pipeline fatal.
            if s.op_fail_count[ok] > int(s.max_retries):
                if pid is not None:
                    s.fatal_pipelines.add(pid)

    # Drop completed/fatal pipelines from queues.
    _cleanup_queues(s)

    suspensions = []  # No preemption implemented in this iteration.
    assignments = []

    # Approximate whether any high-priority work exists (queue non-empty).
    # (We keep it simple: even if not-ready, presence in queue still signals demand.)
    high_prio_waiting_global = bool(s.queues[Priority.QUERY] or s.queues[Priority.INTERACTIVE])

    # Greedily pack each pool.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = int(pool.avail_cpu_pool or 0)
        avail_ram = int(pool.avail_ram_pool or 0)
        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # Pack multiple assignments as long as resources remain.
        # Keep a small "stop" guard to avoid infinite looping in odd edge cases.
        pack_guard = 0
        while avail_cpu > 0 and avail_ram > 0 and pack_guard < 128:
            pack_guard += 1

            # Priority order: prioritize QUERY/INTERACTIVE for latency.
            # (QUERY first, then INTERACTIVE; both before BATCH)
            candidate = None
            for prio in (Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE):
                candidate = _select_candidate(
                    s=s,
                    prio=prio,
                    avail_cpu=avail_cpu,
                    avail_ram=avail_ram,
                    pool=pool,
                    high_prio_waiting=high_prio_waiting_global,
                )
                if candidate is not None:
                    break

            if candidate is None:
                break

            p, op, cpu_req, ram_req = candidate

            # Create an assignment for a single op (atomic scheduling unit).
            a = Assignment(
                ops=[op],
                cpu=cpu_req,
                ram=ram_req,
                priority=p.priority,
                pool_id=pool_id,
                pipeline_id=p.pipeline_id,
            )
            assignments.append(a)

            # Track for learning/retries.
            ok = _op_key(op)
            s.last_alloc[ok] = (cpu_req, ram_req)
            s.op_to_pipeline[ok] = p.pipeline_id

            # Decrement available resources for further packing.
            avail_cpu -= int(cpu_req)
            avail_ram -= int(ram_req)

    return suspensions, assignments
