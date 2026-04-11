# policy_key: scheduler_high_033
# reasoning_effort: high
# model: gpt-5.2-2025-12-11
# llm_cost: 0.182531
# generation_seconds: 201.33
# generated_at: 2026-03-14T03:58:35.077217
@register_scheduler_init(key="scheduler_high_033")
def scheduler_high_033_init(s):
    """Priority-aware, multi-assignment scheduler (incremental improvement over naive FIFO).

    Key ideas (kept intentionally simple / low-risk):
      1) Maintain separate waiting queues per priority (QUERY > INTERACTIVE > BATCH_PIPELINE).
      2) Pack multiple single-op assignments per pool per tick (instead of "one op per pool"),
         using conservative default CPU/RAM slices to reduce queueing latency for high priority.
      3) Basic OOM-aware retry: if an op fails with an OOM-like error, remember a higher RAM hint
         for that specific op and retry later (up to a cap). Non-OOM repeated failures are dropped.
      4) Soft headroom reservation: when high-priority runnable work exists, restrict batch from
         consuming the last part of pool resources to reduce tail latency for interactive/query.
    """
    # Waiting queues keyed by pipeline priority
    s.queues = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }

    # Track pipelines we've seen so we don't enqueue duplicates
    s.known_pipeline_ids = set()
    s.dead_pipeline_ids = set()

    # Per-operator resource hints (learned from previous attempts/results)
    # Keys are derived from operator identity; hints are "next attempt" sizes.
    s.op_ram_hint = {}
    s.op_cpu_hint = {}

    # Retry counters (used to stop infinite replays)
    s.op_oom_retries = {}
    s.op_other_retries = {}
    s.blacklisted_ops = set()

    # Retry policy caps
    s.max_oom_retries = 4
    s.max_other_retries = 1

    # Default per-op sizing fractions by priority (conservative packing for latency)
    s.cpu_frac = {
        Priority.QUERY: 0.60,
        Priority.INTERACTIVE: 0.50,
        Priority.BATCH_PIPELINE: 0.25,
    }
    s.ram_frac = {
        Priority.QUERY: 0.50,
        Priority.INTERACTIVE: 0.40,
        Priority.BATCH_PIPELINE: 0.25,
    }

    # Hard caps per op so one op doesn't consume an entire pool by default
    s.cpu_cap = {
        Priority.QUERY: 0.90,
        Priority.INTERACTIVE: 0.75,
        Priority.BATCH_PIPELINE: 0.60,
    }
    s.ram_cap = {
        Priority.QUERY: 0.90,
        Priority.INTERACTIVE: 0.80,
        Priority.BATCH_PIPELINE: 0.70,
    }


def _op_key(op):
    """Best-effort stable key for an operator object."""
    # Prefer explicit IDs if present; fall back to object identity.
    for attr in ("op_id", "operator_id", "id", "uuid"):
        if hasattr(op, attr):
            try:
                v = getattr(op, attr)
                if v is not None:
                    return (attr, v)
            except Exception:
                pass
    return ("py_id", id(op))


def _is_oom_error(err):
    """Heuristic OOM detection from an error string/obj."""
    if err is None:
        return False
    try:
        msg = str(err).lower()
    except Exception:
        return False
    return ("oom" in msg) or ("out of memory" in msg) or ("out-of-memory" in msg) or ("memoryerror" in msg)


def _prune_queue(s, prio):
    """Remove completed/dead pipelines from a priority queue."""
    q = s.queues[prio]
    if not q:
        return
    new_q = []
    for p in q:
        if p.pipeline_id in s.dead_pipeline_ids:
            continue
        try:
            st = p.runtime_status()
        except Exception:
            # If status is temporarily unavailable, keep it.
            new_q.append(p)
            continue
        if st.is_pipeline_successful():
            continue
        new_q.append(p)
    s.queues[prio] = new_q


def _exists_runnable(s, prio):
    """True if any pipeline at this priority has at least one runnable (assignable) op."""
    q = s.queues.get(prio, [])
    if not q:
        return False
    assignable_states = [OperatorState.PENDING, OperatorState.FAILED]
    for p in q:
        if p.pipeline_id in s.dead_pipeline_ids:
            continue
        try:
            st = p.runtime_status()
        except Exception:
            continue
        if st.is_pipeline_successful():
            continue
        ops = st.get_ops(assignable_states, require_parents_complete=True)
        if not ops:
            continue
        # If the first runnable is blacklisted we treat the pipeline as dead-ish, but do not
        # mutate state here; the selector will.
        return True
    return False


def _next_runnable_candidate(s, prio, pool, skip_pipeline_ids, skip_op_keys):
    """Round-robin within a priority queue to find a runnable op candidate.

    This rotates the queue by popping from the front and appending to the end to preserve fairness.
    Returns (pipeline, op) or None if no runnable candidate exists.
    """
    q = s.queues.get(prio, [])
    if not q:
        return None

    assignable_states = [OperatorState.PENDING, OperatorState.FAILED]
    n = len(q)
    for _ in range(n):
        p = q.pop(0)

        # Drop dead pipelines immediately
        if p.pipeline_id in s.dead_pipeline_ids:
            continue

        # Enforce "at most one op per pipeline per tick"
        if p.pipeline_id in skip_pipeline_ids:
            q.append(p)
            continue

        try:
            st = p.runtime_status()
        except Exception:
            # Status unavailable; keep in queue
            q.append(p)
            continue

        # Drop completed pipelines
        if st.is_pipeline_successful():
            continue

        ops = st.get_ops(assignable_states, require_parents_complete=True)
        if not ops:
            q.append(p)
            continue

        op = ops[0]
        k = _op_key(op)

        # If we have decided this op is hopeless, drop the whole pipeline to prevent spinning
        if k in s.blacklisted_ops:
            s.dead_pipeline_ids.add(p.pipeline_id)
            continue

        # Avoid double-scheduling same operator in a single tick
        if k in skip_op_keys:
            q.append(p)
            continue

        # Candidate found; rotate pipeline to end for RR fairness
        q.append(p)
        return (p, op)

    return None


def _size_for_op(s, pool, prio, op_k, avail_cpu, avail_ram):
    """Compute CPU/RAM for an operator using hints + conservative defaults.

    Returns (cpu, ram). Returns (0, 0) if the op cannot fit (e.g., RAM hint exceeds available RAM).
    """
    # Defaults are fractions of pool maxima (helps latency by packing multiple high-priority ops)
    default_cpu = int(pool.max_cpu_pool * s.cpu_frac[prio])
    default_ram = int(pool.max_ram_pool * s.ram_frac[prio])
    if default_cpu < 1:
        default_cpu = 1
    if default_ram < 1:
        default_ram = 1

    # Apply per-op learned hints if present
    cpu = int(s.op_cpu_hint.get(op_k, default_cpu))
    ram = int(s.op_ram_hint.get(op_k, default_ram))

    # Cap per-op size so one op doesn't monopolize a pool by default
    cap_cpu = int(pool.max_cpu_pool * s.cpu_cap[prio])
    cap_ram = int(pool.max_ram_pool * s.ram_cap[prio])
    if cap_cpu < 1:
        cap_cpu = 1
    if cap_ram < 1:
        cap_ram = 1

    cpu = min(cpu, cap_cpu)
    ram = min(ram, cap_ram)

    # If we have a RAM hint and it doesn't fit in current availability, we shouldn't run it here
    if op_k in s.op_ram_hint and s.op_ram_hint[op_k] > avail_ram:
        return 0, 0

    # Final clamp to what's available in this pool
    if avail_cpu < 1 or avail_ram < 1:
        return 0, 0
    cpu = min(cpu, int(avail_cpu))
    ram = min(ram, int(avail_ram))

    if cpu < 1 or ram < 1:
        return 0, 0
    return int(cpu), int(ram)


@register_scheduler(key="scheduler_high_033")
def scheduler_high_033(s, results, pipelines):
    """Priority queues + packing + OOM-aware RAM growth + soft headroom for latency."""
    # Enqueue new pipelines into the right priority queue (avoid duplicates)
    for p in pipelines:
        if p.pipeline_id in s.known_pipeline_ids or p.pipeline_id in s.dead_pipeline_ids:
            continue
        s.known_pipeline_ids.add(p.pipeline_id)
        # If a new/unknown priority appears, treat it as lowest priority
        if p.priority not in s.queues:
            s.queues[p.priority] = []
        s.queues[p.priority].append(p)

    # Early exit if nothing changed (keeps simulator fast)
    if not pipelines and not results:
        return [], []

    # Learn from results (primarily to respond to OOM by increasing RAM next attempt)
    for r in results:
        pool = None
        try:
            pool = s.executor.pools[r.pool_id]
        except Exception:
            pool = None

        ops = getattr(r, "ops", None) or []
        for op in ops:
            k = _op_key(op)

            # Ignore further learning for blacklisted ops
            if k in s.blacklisted_ops:
                continue

            if r.failed():
                if _is_oom_error(getattr(r, "error", None)):
                    s.op_oom_retries[k] = s.op_oom_retries.get(k, 0) + 1

                    # Increase RAM aggressively (exponential-ish) to converge faster
                    prev = s.op_ram_hint.get(k, int(getattr(r, "ram", 1) or 1))
                    base = int(getattr(r, "ram", prev) or prev)
                    prev = max(prev, base, 1)

                    # Ensure we jump to a meaningful size relative to pool, if known
                    if pool is not None:
                        floor_jump = int(pool.max_ram_pool * 0.25)
                        new_ram = max(prev + 1, prev * 2, floor_jump)
                        new_ram = min(int(pool.max_ram_pool), int(new_ram))
                    else:
                        new_ram = max(prev + 1, prev * 2)

                    s.op_ram_hint[k] = int(max(1, new_ram))

                    # Keep CPU hint if available (not critical for correctness)
                    try:
                        if getattr(r, "cpu", None):
                            s.op_cpu_hint.setdefault(k, int(r.cpu))
                    except Exception:
                        pass

                    # Give up after too many OOM retries
                    if s.op_oom_retries[k] > s.max_oom_retries:
                        s.blacklisted_ops.add(k)
                else:
                    s.op_other_retries[k] = s.op_other_retries.get(k, 0) + 1

                    # Non-OOM failures are likely deterministic; don't spin forever
                    if s.op_other_retries[k] > s.max_other_retries:
                        s.blacklisted_ops.add(k)
                    else:
                        # Keep last attempted sizes as hints
                        try:
                            if getattr(r, "ram", None):
                                s.op_ram_hint[k] = int(r.ram)
                        except Exception:
                            pass
                        try:
                            if getattr(r, "cpu", None):
                                s.op_cpu_hint[k] = int(r.cpu)
                        except Exception:
                            pass
            else:
                # Success: record "known good" sizes (conservative: don't shrink automatically yet)
                try:
                    if getattr(r, "ram", None):
                        s.op_ram_hint.setdefault(k, int(r.ram))
                except Exception:
                    pass
                try:
                    if getattr(r, "cpu", None):
                        s.op_cpu_hint.setdefault(k, int(r.cpu))
                except Exception:
                    pass

    # Prune completed/dead pipelines to keep queues tidy
    _prune_queue(s, Priority.QUERY)
    _prune_queue(s, Priority.INTERACTIVE)
    _prune_queue(s, Priority.BATCH_PIPELINE)

    suspensions = []
    assignments = []

    # Precompute whether high-priority runnable work exists (used for soft headroom reservations)
    # Note: This is intentionally simple and may be slightly stale during the tick.
    query_runnable = _exists_runnable(s, Priority.QUERY)
    interactive_runnable = _exists_runnable(s, Priority.INTERACTIVE)

    # Schedule on pools with most available RAM first (helps avoid RAM-hint starvation)
    pool_ids = list(range(s.executor.num_pools))
    pool_ids.sort(
        key=lambda i: (s.executor.pools[i].avail_ram_pool, s.executor.pools[i].avail_cpu_pool),
        reverse=True,
    )

    scheduled_pipeline_ids = set()
    scheduled_op_keys = set()

    for pool_id in pool_ids:
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool
        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # Keep scheduling until this pool is effectively full or no candidates fit
        guard = 0
        while avail_cpu > 0 and avail_ram > 0:
            guard += 1
            if guard > 100:
                break

            made_assignment = False

            # Strict priority order (small improvement with obvious latency gains)
            for prio in (Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE):
                # Soft headroom:
                # - If QUERY runnable exists, avoid letting INTERACTIVE/BATCH consume the last chunk.
                # - If any high runnable exists, avoid letting BATCH consume the last chunk.
                if prio == Priority.BATCH_PIPELINE and (query_runnable or interactive_runnable):
                    reserve_cpu = int(pool.max_cpu_pool * 0.25)
                    reserve_ram = int(pool.max_ram_pool * 0.25)
                elif prio == Priority.INTERACTIVE and query_runnable:
                    reserve_cpu = int(pool.max_cpu_pool * 0.15)
                    reserve_ram = int(pool.max_ram_pool * 0.15)
                else:
                    reserve_cpu = 0
                    reserve_ram = 0

                # Try multiple pipelines within this priority to find something that fits
                qlen = len(s.queues.get(prio, []))
                for _ in range(qlen):
                    cand = _next_runnable_candidate(
                        s,
                        prio,
                        pool,
                        scheduled_pipeline_ids,
                        scheduled_op_keys,
                    )
                    if cand is None:
                        break

                    pipeline, op = cand
                    op_k = _op_key(op)

                    cpu, ram = _size_for_op(s, pool, prio, op_k, avail_cpu, avail_ram)
                    if cpu <= 0 or ram <= 0:
                        # Doesn't fit currently; try next candidate within same priority
                        continue

                    # Enforce soft headroom reservations for lower priorities
                    if (avail_cpu - cpu) < reserve_cpu or (avail_ram - ram) < reserve_ram:
                        # Can't take more without sacrificing headroom; try another candidate (maybe smaller via hint)
                        continue

                    assignments.append(
                        Assignment(
                            ops=[op],
                            cpu=cpu,
                            ram=ram,
                            priority=pipeline.priority,
                            pool_id=pool_id,
                            pipeline_id=pipeline.pipeline_id,
                        )
                    )
                    avail_cpu -= cpu
                    avail_ram -= ram

                    scheduled_pipeline_ids.add(pipeline.pipeline_id)
                    scheduled_op_keys.add(op_k)

                    made_assignment = True
                    break  # schedule one op at a time, then re-evaluate priorities

                if made_assignment:
                    break

            if not made_assignment:
                break

    return suspensions, assignments
