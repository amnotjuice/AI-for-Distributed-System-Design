# source: scheduler_low_011.py
# model: gpt-5.2-2025-12-11
# effort: low
# feedback: minimal
# iteration: r18
@register_scheduler_init(key="scheduler_low_011_r18")
def scheduler_low_011_r18_init(s):
    """Priority-first, latency-oriented scheduler with small but meaningful fixes.

    Key improvements over the prior iteration:
    - Fix obvious bug: do NOT drop pipelines just because they contain FAILED ops; allow OOM-retries.
    - Fill pools better: schedule multiple operators per tick until pool headroom is used.
    - Stronger priority isolation: keep batch off the interactive pool when high-priority backlog exists.
    - Smarter initial sizing: if operator exposes a RAM minimum-like attribute, size RAM near it with a safety factor
      (reduces OOM retries without over-allocating entire pools).
    - Learn from outcomes:
        * On OOM: exponential RAM backoff per (pipeline, op) and mark as retriable up to a cap.
        * On non-OOM failure: mark op as non-retriable to avoid endless replays.
        * On success: keep a conservative "last-known-good" RAM hint (helps avoid over-allocation after OOM inflation).
    """
    # Priority FIFO queues
    s.waiting_queues = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }

    # Learned per-operator hints keyed by (pipeline_id, op_identity)
    # {"ram": float, "cpu": float}
    s.op_hints = {}

    # Failure classification per op key
    s.op_is_nonretriable = set()  # non-OOM failures
    s.op_oom_retries = {}  # op_key -> int

    # Best-effort mapping to infer pipeline_id from op object when results lack pipeline_id
    s.op_to_pipeline = {}  # id(op) -> pipeline_id

    # Policy knobs (kept small / robust)
    s.max_oom_retries_per_op = 3
    s.interactive_pool_id = 0

    # Default sizing when op-level RAM minimum is unknown (fractions of pool MAX)
    s.default_ram_frac = {
        Priority.QUERY: 0.30,
        Priority.INTERACTIVE: 0.30,
        Priority.BATCH_PIPELINE: 0.60,
    }

    # CPU sizing baseline (fractions of pool MAX); adjusted dynamically under backlog
    s.default_cpu_frac = {
        Priority.QUERY: 0.70,
        Priority.INTERACTIVE: 0.65,
        Priority.BATCH_PIPELINE: 1.00,
    }

    # Safety factor on operator RAM minima (if discoverable)
    s.ram_safety = {
        Priority.QUERY: 1.50,
        Priority.INTERACTIVE: 1.40,
        Priority.BATCH_PIPELINE: 1.20,
    }

    # Interactive pool headroom reservation when high-priority backlog exists
    s.reserve_interactive_frac_cpu = 0.20
    s.reserve_interactive_frac_ram = 0.20

    # Limit per-tick work to keep the simulator stable and avoid pathological loops
    s.max_assignments_per_tick_per_pool = 4


def _priority_order():
    return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]


def _is_oom_error(err):
    if err is None:
        return False
    msg = str(err).lower()
    return ("oom" in msg) or ("out of memory" in msg) or ("memoryerror" in msg)


def _iter_pipeline_ops(pipeline):
    vals = getattr(pipeline, "values", None)
    if vals is None:
        return
    try:
        if isinstance(vals, dict):
            iterable = vals.values()
        else:
            iterable = vals
        for op in iterable:
            yield op
    except Exception:
        return


def _op_key(pipeline_id, op):
    return (pipeline_id, id(op))


def _get_op_ram_min(op):
    # Best-effort introspection: Eudoxia workloads often attach a RAM minimum.
    # We keep this defensive to avoid relying on a specific schema.
    candidates = []
    for attr in (
        "ram_min",
        "min_ram",
        "min_memory",
        "memory_min",
        "mem_min",
        "required_ram",
        "required_memory",
    ):
        try:
            v = getattr(op, attr, None)
        except Exception:
            v = None
        if v is None:
            continue
        try:
            fv = float(v)
            if fv > 0:
                candidates.append(fv)
        except Exception:
            continue

    if not candidates:
        return None
    return min(candidates)


def _pipeline_remaining_ops_estimate(pipeline):
    # Coarse "remaining work" heuristic for QUERY lookahead: fewer remaining ops may finish sooner.
    try:
        st = pipeline.runtime_status()
        sc = getattr(st, "state_counts", {}) or {}
        rem = 0
        rem += int(sc.get(OperatorState.PENDING, 0))
        rem += int(sc.get(OperatorState.ASSIGNED, 0))
        rem += int(sc.get(OperatorState.RUNNING, 0))
        rem += int(sc.get(OperatorState.SUSPENDING, 0))
        rem += int(sc.get(OperatorState.FAILED, 0))
        return rem
    except Exception:
        return 10**9


def _pool_preference_penalty(num_pools, interactive_pool_id, priority, pool_id):
    # Lower is better. We strongly prefer keeping batch off the interactive pool.
    if num_pools <= 1:
        return 0
    if priority in (Priority.QUERY, Priority.INTERACTIVE):
        return 0 if pool_id == interactive_pool_id else 1
    # batch
    return 1 if pool_id == interactive_pool_id else 0


def _compute_request(s, pool, rem_cpu, rem_ram, priority, pipeline_id, op, high_pr_backlog_count):
    # CPU: reduce per-op CPU under high-pr backlog to increase concurrency and reduce queueing latency.
    base_cpu_frac = float(s.default_cpu_frac.get(priority, 1.0))
    if priority in (Priority.QUERY, Priority.INTERACTIVE) and high_pr_backlog_count > 1:
        base_cpu_frac = min(base_cpu_frac, 0.50)
    if priority == Priority.BATCH_PIPELINE and high_pr_backlog_count > 0:
        base_cpu_frac = min(base_cpu_frac, 0.60)

    cpu = max(1.0, pool.max_cpu_pool * base_cpu_frac)

    # RAM: if we can infer an op minimum, size near it with a safety factor; else use a pool fraction.
    op_min = _get_op_ram_min(op)
    if op_min is not None:
        target_ram = op_min * float(s.ram_safety.get(priority, 1.2))
    else:
        target_ram = pool.max_ram_pool * float(s.default_ram_frac.get(priority, 0.6))
    ram = max(1.0, target_ram)

    # Apply learned hints (OOM backoff or last-known-good)
    k = _op_key(pipeline_id, op)
    hint = s.op_hints.get(k)
    if hint:
        try:
            cpu = max(cpu, float(hint.get("cpu", cpu)))
        except Exception:
            pass
        try:
            ram = max(ram, float(hint.get("ram", ram)))
        except Exception:
            pass

    # Cap by pool remaining & max
    cpu = min(cpu, rem_cpu, pool.max_cpu_pool)
    ram = min(ram, rem_ram, pool.max_ram_pool)

    # Ensure still positive
    cpu = max(1.0, cpu)
    ram = max(1.0, ram)
    return cpu, ram


def _can_place_with_reservation(s, pool, pool_id, priority, rem_cpu, rem_ram, req_cpu, req_ram, high_pr_backlog_count):
    # Basic fit
    if req_cpu > rem_cpu or req_ram > rem_ram:
        return False

    # If there are multiple pools, reserve interactive pool headroom when there is high-priority backlog.
    if s.executor.num_pools > 1 and pool_id == s.interactive_pool_id and high_pr_backlog_count > 0:
        # Keep batch away from interactive pool under backlog; avoid interference and preserve tail latency.
        if priority == Priority.BATCH_PIPELINE:
            return False

        reserve_cpu = pool.max_cpu_pool * float(s.reserve_interactive_frac_cpu)
        reserve_ram = pool.max_ram_pool * float(s.reserve_interactive_frac_ram)

        # Reservation is "post placement": ensure we don't consume reserved headroom.
        if (rem_cpu - req_cpu) < reserve_cpu:
            return False
        if (rem_ram - req_ram) < reserve_ram:
            return False

    return True


def _choose_ready_pipeline_fifo(s, prio_queue, priority, scheduled_pipelines):
    # FIFO scan with rotation (bounded) to find a pipeline with at least one assignable op ready.
    n = len(prio_queue)
    for _ in range(n):
        p = prio_queue.pop(0)
        st = p.runtime_status()

        # Drop completed pipelines immediately
        if st.is_pipeline_successful():
            continue

        # Avoid scheduling multiple ops from the same pipeline in the same tick (prevents duplicates)
        if p.pipeline_id in scheduled_pipelines:
            prio_queue.append(p)
            continue

        # If pipeline has failures but no assignable ops, it cannot make progress; drop it.
        assignable = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        if not assignable:
            has_failed = int(getattr(st, "state_counts", {}).get(OperatorState.FAILED, 0)) > 0
            if has_failed:
                continue
            prio_queue.append(p)
            continue

        # Choose first ready op; if it's non-retriable, drop pipeline (prevents dead queue churn).
        op = assignable[0]
        k = _op_key(p.pipeline_id, op)
        if k in s.op_is_nonretriable:
            continue
        if int(s.op_oom_retries.get(k, 0)) > int(s.max_oom_retries_per_op):
            continue

        return p, op

    return None, None


def _choose_ready_pipeline_query_lookahead(s, prio_queue, scheduled_pipelines, lookahead=8):
    # For QUERY: small lookahead to prefer pipelines likely to complete sooner (reduces tail latency).
    n = len(prio_queue)
    if n == 0:
        return None, None
    k = min(int(lookahead), n)

    # Pop first k to inspect; reinsert later preserving order (minus chosen).
    inspected = []
    best = None  # (remaining_ops, idx, pipeline, op)
    for i in range(k):
        p = prio_queue.pop(0)
        inspected.append(p)

        st = p.runtime_status()
        if st.is_pipeline_successful():
            continue
        if p.pipeline_id in scheduled_pipelines:
            continue

        assignable = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        if not assignable:
            has_failed = int(getattr(st, "state_counts", {}).get(OperatorState.FAILED, 0)) > 0
            # If failed but not assignable, drop it by not requeueing
            if has_failed:
                inspected[-1] = None
            continue

        op = assignable[0]
        opk = _op_key(p.pipeline_id, op)
        if opk in s.op_is_nonretriable:
            continue
        if int(s.op_oom_retries.get(opk, 0)) > int(s.max_oom_retries_per_op):
            continue

        rem = _pipeline_remaining_ops_estimate(p)
        cand = (rem, i, p, op)
        if best is None or cand < best:
            best = cand

    # Requeue inspected pipelines, skipping those we decided to drop and skipping chosen
    chosen_p = best[2] if best is not None else None
    chosen_op = best[3] if best is not None else None
    for p in inspected:
        if p is None:
            continue
        if chosen_p is not None and p.pipeline_id == chosen_p.pipeline_id:
            continue
        prio_queue.append(p)

    return chosen_p, chosen_op


@register_scheduler(key="scheduler_low_011_r18")
def scheduler_low_011_r18(s, results, pipelines):
    """
    Priority-first scheduler with:
    - OOM-aware retries (do not drop pipelines on FAILED; retry with higher RAM).
    - Non-OOM failure suppression (mark non-retriable ops to avoid wasting cycles).
    - Better utilization (multiple assignments per pool per tick).
    - Latency isolation (reserve interactive pool and keep batch off it under high-pr backlog).
    """
    # Enqueue new arrivals and populate op->pipeline mapping (best-effort)
    for p in pipelines:
        pr = p.priority if p.priority in s.waiting_queues else Priority.BATCH_PIPELINE
        s.waiting_queues[pr].append(p)
        for op in _iter_pipeline_ops(p):
            s.op_to_pipeline[id(op)] = p.pipeline_id

    # Update learning from execution results
    for r in results:
        # Determine failure vs success
        failed = False
        try:
            failed = bool(r.failed())
        except Exception:
            failed = getattr(r, "error", None) is not None

        ops = getattr(r, "ops", []) or []

        if failed:
            is_oom = _is_oom_error(getattr(r, "error", None))
            for op in ops:
                pipeline_id = getattr(r, "pipeline_id", None)
                if pipeline_id is None:
                    pipeline_id = s.op_to_pipeline.get(id(op))
                if pipeline_id is None:
                    continue

                k = _op_key(pipeline_id, op)

                if is_oom:
                    # Exponential backoff on RAM, capped later per-pool.
                    prev_hint = s.op_hints.get(k, {})
                    prev_ram = None
                    try:
                        prev_ram = float(prev_hint.get("ram", None))
                    except Exception:
                        prev_ram = None

                    observed_ram = getattr(r, "ram", None)
                    try:
                        observed_ram = float(observed_ram) if observed_ram is not None else None
                    except Exception:
                        observed_ram = None

                    baseline = None
                    if prev_ram is not None and prev_ram > 0:
                        baseline = prev_ram
                    elif observed_ram is not None and observed_ram > 0:
                        baseline = observed_ram
                    else:
                        baseline = 1.0

                    new_ram = max(1.0, baseline * 2.0)

                    # Keep CPU hint conservative (do not inflate on OOM)
                    observed_cpu = getattr(r, "cpu", None)
                    try:
                        observed_cpu = float(observed_cpu) if observed_cpu is not None else 1.0
                    except Exception:
                        observed_cpu = 1.0

                    s.op_hints[k] = {
                        "ram": new_ram,
                        "cpu": max(1.0, float(prev_hint.get("cpu", observed_cpu)) if isinstance(prev_hint, dict) else 1.0),
                    }
                    s.op_oom_retries[k] = int(s.op_oom_retries.get(k, 0)) + 1
                else:
                    # Non-OOM failures are treated as non-retriable to avoid endless repeats.
                    s.op_is_nonretriable.add(k)
        else:
            # On success, store "last-known-good" allocation to avoid staying over-inflated after OOM retries.
            for op in ops:
                pipeline_id = getattr(r, "pipeline_id", None)
                if pipeline_id is None:
                    pipeline_id = s.op_to_pipeline.get(id(op))
                if pipeline_id is None:
                    continue
                k = _op_key(pipeline_id, op)

                try:
                    obs_ram = float(getattr(r, "ram", None))
                except Exception:
                    obs_ram = None
                try:
                    obs_cpu = float(getattr(r, "cpu", None))
                except Exception:
                    obs_cpu = None

                if obs_ram is None and obs_cpu is None:
                    continue

                prev = s.op_hints.get(k, {})
                new_hint = dict(prev) if isinstance(prev, dict) else {}
                if obs_ram is not None and obs_ram > 0:
                    prev_ram = new_hint.get("ram", obs_ram)
                    try:
                        prev_ram = float(prev_ram)
                    except Exception:
                        prev_ram = obs_ram
                    # Shrink only toward observed (conservative). Prevents retaining huge OOM-inflated RAM.
                    new_hint["ram"] = min(prev_ram, obs_ram) if prev_ram > 0 else obs_ram
                if obs_cpu is not None and obs_cpu > 0:
                    prev_cpu = new_hint.get("cpu", obs_cpu)
                    try:
                        prev_cpu = float(prev_cpu)
                    except Exception:
                        prev_cpu = obs_cpu
                    new_hint["cpu"] = min(prev_cpu, obs_cpu) if prev_cpu > 0 else obs_cpu

                s.op_hints[k] = new_hint
                # If it succeeded, it's not non-retriable.
                if k in s.op_is_nonretriable:
                    s.op_is_nonretriable.discard(k)

    # Early exit if no state changes that affect decisions
    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    # Compute a coarse high-priority backlog count (used for reservation and CPU sizing)
    high_pr_backlog_count = len(s.waiting_queues.get(Priority.QUERY, [])) + len(s.waiting_queues.get(Priority.INTERACTIVE, []))

    # Local remaining resources per pool (simulate consumption across multiple assignments this tick)
    num_pools = s.executor.num_pools
    pools = s.executor.pools
    rem_cpu = [float(pools[i].avail_cpu_pool) for i in range(num_pools)]
    rem_ram = [float(pools[i].avail_ram_pool) for i in range(num_pools)]
    per_pool_assigned = [0 for _ in range(num_pools)]

    # Track pipelines scheduled this tick to avoid duplicate-assigning multiple ops from the same pipeline
    scheduled_pipelines = set()

    # Main loop: always try to schedule highest priority next, repeatedly, until no progress
    while True:
        made_progress = False

        for pr in _priority_order():
            q = s.waiting_queues.get(pr, [])
            if not q:
                continue

            # Select a ready pipeline/op (QUERY uses small lookahead to reduce tail latency)
            if pr == Priority.QUERY:
                pipeline, op = _choose_ready_pipeline_query_lookahead(s, q, scheduled_pipelines, lookahead=8)
            else:
                pipeline, op = _choose_ready_pipeline_fifo(s, q, pr, scheduled_pipelines)

            if pipeline is None or op is None:
                continue

            # Decide pool placement: evaluate all pools that still have capacity and haven't hit per-tick cap
            best = None  # (score, pool_id, req_cpu, req_ram)
            for pool_id in range(num_pools):
                if per_pool_assigned[pool_id] >= int(s.max_assignments_per_tick_per_pool):
                    continue
                if rem_cpu[pool_id] < 1.0 or rem_ram[pool_id] < 1.0:
                    continue

                pool = pools[pool_id]

                # Compute request for this pool
                req_cpu, req_ram = _compute_request(
                    s=s,
                    pool=pool,
                    rem_cpu=rem_cpu[pool_id],
                    rem_ram=rem_ram[pool_id],
                    priority=pr,
                    pipeline_id=pipeline.pipeline_id,
                    op=op,
                    high_pr_backlog_count=high_pr_backlog_count,
                )

                if not _can_place_with_reservation(
                    s=s,
                    pool=pool,
                    pool_id=pool_id,
                    priority=pr,
                    rem_cpu=rem_cpu[pool_id],
                    rem_ram=rem_ram[pool_id],
                    req_cpu=req_cpu,
                    req_ram=req_ram,
                    high_pr_backlog_count=high_pr_backlog_count,
                ):
                    continue

                # Score: prefer correct pool, then "best fit" packing (leave larger holes for later).
                penalty = _pool_preference_penalty(num_pools, s.interactive_pool_id, pr, pool_id)
                # normalized leftover (lower is better)
                left_cpu = (rem_cpu[pool_id] - req_cpu) / max(1.0, float(pool.max_cpu_pool))
                left_ram = (rem_ram[pool_id] - req_ram) / max(1.0, float(pool.max_ram_pool))
                pack_score = left_cpu + left_ram
                score = penalty * 10.0 + pack_score

                cand = (score, pool_id, req_cpu, req_ram)
                if best is None or cand < best:
                    best = cand

            if best is None:
                # Could not place this op anywhere right now; re-enqueue and try lower priorities.
                q.append(pipeline)
                continue

            _, pool_id, req_cpu, req_ram = best

            # Record assignment
            assignments.append(
                Assignment(
                    ops=[op],
                    cpu=req_cpu,
                    ram=req_ram,
                    priority=pr,
                    pool_id=pool_id,
                    pipeline_id=pipeline.pipeline_id,
                )
            )

            # Consume local headroom
            rem_cpu[pool_id] -= req_cpu
            rem_ram[pool_id] -= req_ram
            per_pool_assigned[pool_id] += 1
            scheduled_pipelines.add(pipeline.pipeline_id)

            # Re-enqueue pipeline for future ops
            q.append(pipeline)

            made_progress = True
            break  # restart from highest priority

        if not made_progress:
            break

    return suspensions, assignments