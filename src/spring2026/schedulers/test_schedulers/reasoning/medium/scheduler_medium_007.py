# policy_key: scheduler_medium_007
# reasoning_effort: medium
# exp: reasoning
# model: gpt-5.2-2025-12-11
# llm_cost: 0.075401
# generation_seconds: 110.28
# generated_at: 2026-04-09T22:37:38.938099
@register_scheduler_init(key="scheduler_medium_007")
def scheduler_medium_007_init(s):
    """
    Priority-aware, OOM-adaptive, headroom-preserving scheduler.

    Core ideas:
      - Use weighted deficit round-robin across priorities (query:10, interactive:5, batch:1)
        to strongly favor high-priority latency while still guaranteeing batch progress.
      - Preserve a small fixed headroom in each pool so future query/interactive arrivals can start
        quickly (prevents batch from fully clogging pools).
      - Learn per-operator RAM needs from failures/successes:
          * On failure (assumed OOM-like), increase next RAM hint (doubling) up to pool max.
          * On success, remember RAM as a good future target.
      - Avoid dropping pipelines: keep retrying failed operators with increased RAM to maximize
        completion rate (failed/incomplete is heavily penalized).
    """
    from collections import deque

    s.tick = 0

    # One FIFO queue per priority
    s.queues = {
        Priority.QUERY: deque(),
        Priority.INTERACTIVE: deque(),
        Priority.BATCH_PIPELINE: deque(),
    }
    s.pipeline_ids_in_queues = set()

    # Weighted deficit round-robin state
    s.deficit = {
        Priority.QUERY: 0,
        Priority.INTERACTIVE: 0,
        Priority.BATCH_PIPELINE: 0,
    }
    s.quantum = {
        Priority.QUERY: 10,
        Priority.INTERACTIVE: 5,
        Priority.BATCH_PIPELINE: 1,
    }

    # RAM sizing heuristics (fractions of pool max RAM) for first attempt
    s.base_ram_frac = {
        Priority.QUERY: 0.30,
        Priority.INTERACTIVE: 0.24,
        Priority.BATCH_PIPELINE: 0.18,
    }
    s.max_ram_frac = 0.90

    # CPU sizing heuristics (fractions of pool max CPU), with caps to avoid monopolizing large pools
    s.base_cpu_frac = {
        Priority.QUERY: 0.50,
        Priority.INTERACTIVE: 0.35,
        Priority.BATCH_PIPELINE: 0.25,
    }
    s.cpu_cap = {
        Priority.QUERY: 8,
        Priority.INTERACTIVE: 6,
        Priority.BATCH_PIPELINE: 4,
    }

    # Headroom preserved in each pool to reduce high-priority tail latency under contention
    s.headroom_frac_cpu = 0.20
    s.headroom_frac_ram = 0.20

    # Per-operator learned sizing state:
    #   key -> {"failures": int, "hint_ram": int, "last_success_ram": int, "last_success_cpu": int}
    s.op_state = {}

    # Safety bounds for scanning queues
    s.max_queue_scan = 128


def _op_key(op):
    """Best-effort stable key for an operator object across retries within a simulation."""
    for attr in ("op_id", "operator_id", "task_id", "name", "op_name"):
        if hasattr(op, attr):
            try:
                v = getattr(op, attr)
                if v is not None:
                    return (attr, str(v))
            except Exception:
                pass
    # Fallback: repr is usually stable enough within a single run
    try:
        return ("repr", repr(op))
    except Exception:
        return ("id", str(id(op)))


def _is_pipeline_done_or_drop(s, pipeline):
    """Remove completed pipelines from tracking; never drop failed/incomplete ones."""
    try:
        status = pipeline.runtime_status()
    except Exception:
        return False

    if status.is_pipeline_successful():
        pid = pipeline.pipeline_id
        if pid in s.pipeline_ids_in_queues:
            s.pipeline_ids_in_queues.discard(pid)
        return True
    return False


def _get_ready_ops(pipeline):
    """Return a single ready op (as a list for Assignment), or None if nothing is runnable."""
    status = pipeline.runtime_status()
    if status.is_pipeline_successful():
        return None
    ops = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
    if not ops:
        return None
    return ops[:1]


def _pop_next_pipeline_with_ready_op(s, prio):
    """
    Rotate through the priority queue to find a pipeline with a runnable op.
    Keeps non-runnable pipelines in the queue to preserve fairness.
    """
    q = s.queues[prio]
    if not q:
        return None, None

    scans = min(len(q), s.max_queue_scan)
    for _ in range(scans):
        p = q.popleft()

        # Drop completed pipelines from queues/index
        if _is_pipeline_done_or_drop(s, p):
            continue

        ready_ops = _get_ready_ops(p)
        # Keep pipeline in rotation
        q.append(p)

        if ready_ops:
            return p, ready_ops

    return None, None


def _compute_cpu(s, pool, prio, op):
    """Compute CPU to allocate, balancing latency vs. concurrency."""
    max_cpu = int(getattr(pool, "max_cpu_pool", 0) or 0)
    if max_cpu <= 0:
        return 0

    base = max(1, int(max_cpu * s.base_cpu_frac.get(prio, 0.25)))
    cap = int(s.cpu_cap.get(prio, 4))
    cpu = min(base, cap, max_cpu)

    # If we learned a good CPU size, reuse it (bounded by caps)
    key = _op_key(op)
    st = s.op_state.get(key)
    if st and st.get("last_success_cpu"):
        try:
            learned = int(st["last_success_cpu"])
            if learned > 0:
                cpu = min(max(1, learned), cap, max_cpu)
        except Exception:
            pass

    return max(1, cpu)


def _compute_ram(s, pool, prio, op):
    """Compute RAM to allocate; prioritize completion by increasing RAM on failures."""
    max_ram = int(getattr(pool, "max_ram_pool", 0) or 0)
    if max_ram <= 0:
        return 0

    key = _op_key(op)
    st = s.op_state.get(key, {})

    # Strongest signal: if we have a specific hint RAM (e.g., doubled after failure), use it.
    hint = st.get("hint_ram")
    if hint is not None:
        try:
            hint = int(hint)
            if hint > 0:
                return max(1, min(hint, max_ram))
        except Exception:
            pass

    # Next best: last successful RAM size is usually safe and avoids repeated OOMs.
    last_ok = st.get("last_success_ram")
    if last_ok is not None:
        try:
            last_ok = int(last_ok)
            if last_ok > 0:
                return max(1, min(last_ok, max_ram))
        except Exception:
            pass

    failures = int(st.get("failures", 0) or 0)
    base_frac = float(s.base_ram_frac.get(prio, 0.18))
    frac = min(s.max_ram_frac, base_frac * (2 ** failures))
    ram = int(max_ram * frac)
    return max(1, min(ram, max_ram))


def _has_any_ready_work(s, prio):
    """Fast-ish check: does this priority have at least one runnable operator?"""
    q = s.queues[prio]
    if not q:
        return False
    scans = min(len(q), 16)  # small bound to keep scheduler overhead low
    for i in range(scans):
        p = q[i]
        if _is_pipeline_done_or_drop(s, p):
            continue
        try:
            if _get_ready_ops(p):
                return True
        except Exception:
            continue
    return False


@register_scheduler(key="scheduler_medium_007")
def scheduler_medium_007(s, results: list, pipelines: list):
    """
    Scheduler step:
      - Incorporate new arrivals.
      - Learn from execution results (success/failure) for per-operator RAM sizing.
      - Assign work to pools using weighted deficit round-robin + headroom preservation.
    """
    s.tick += 1

    # Enqueue new pipelines (one-time) into priority queues
    for p in pipelines:
        pid = p.pipeline_id
        if pid in s.pipeline_ids_in_queues:
            continue
        s.pipeline_ids_in_queues.add(pid)
        s.queues[p.priority].append(p)

    # Update learned sizing from execution results
    for r in results:
        # r.ops is a list; we assume one op per assignment in this policy.
        ops = getattr(r, "ops", None) or []
        for op in ops:
            key = _op_key(op)
            st = s.op_state.get(key)
            if st is None:
                st = {"failures": 0}
                s.op_state[key] = st

            if r.failed():
                st["failures"] = int(st.get("failures", 0) or 0) + 1

                # If this was an OOM-like failure, increase RAM aggressively to avoid repeated failures.
                # We treat all failures as potentially RAM-related to maximize completion rate.
                try:
                    pool = s.executor.pools[r.pool_id]
                    max_ram = int(getattr(pool, "max_ram_pool", 0) or 0)
                except Exception:
                    max_ram = 0

                try:
                    prev_ram = int(getattr(r, "ram", 0) or 0)
                except Exception:
                    prev_ram = 0

                if max_ram > 0:
                    # Next hint: double previous, at least a small fraction of max, cap at max.
                    next_hint = prev_ram * 2 if prev_ram > 0 else int(max_ram * 0.25)
                    next_hint = max(1, min(int(next_hint), max_ram))
                    st["hint_ram"] = next_hint
            else:
                # Success: remember the last successful RAM/CPU; clear hint to avoid over-allocation.
                try:
                    st["last_success_ram"] = int(getattr(r, "ram", 0) or 0)
                except Exception:
                    pass
                try:
                    st["last_success_cpu"] = int(getattr(r, "cpu", 0) or 0)
                except Exception:
                    pass
                if "hint_ram" in st:
                    st.pop("hint_ram", None)
                # Keep failures count (it influences future base sizing only if no success data exists)

    # Add deficit quantum each tick (weighted by objective importance)
    for prio, q in s.quantum.items():
        s.deficit[prio] = int(s.deficit.get(prio, 0) or 0) + int(q)

    # If nothing changed, avoid scanning pools/queues
    if not pipelines and not results:
        return [], []

    suspensions = []  # no preemption in this version (headroom is used instead)
    assignments = []

    # Precompute whether we have high-priority ready work (used to be more conservative with batch)
    query_ready = _has_any_ready_work(s, Priority.QUERY)
    interactive_ready = _has_any_ready_work(s, Priority.INTERACTIVE)

    # Helper for picking next priority using deficit DRR, without starving batch.
    prios = [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]

    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = int(pool.avail_cpu_pool)
        avail_ram = int(pool.avail_ram_pool)

        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        max_cpu = int(getattr(pool, "max_cpu_pool", 0) or 0)
        max_ram = int(getattr(pool, "max_ram_pool", 0) or 0)
        headroom_cpu = max(1, int(max_cpu * s.headroom_frac_cpu)) if max_cpu > 0 else 1
        headroom_ram = max(1, int(max_ram * s.headroom_frac_ram)) if max_ram > 0 else 1

        # Fill the pool with multiple assignments, honoring headroom and deficits
        # Hard bound to avoid infinite loops in corner cases
        for _ in range(256):
            if avail_cpu <= 0 or avail_ram <= 0:
                break

            # Choose a priority: highest deficit among priorities with runnable work.
            chosen_prio = None
            chosen_pipeline = None
            chosen_ops = None

            # Sort priorities by current deficit (descending), then by objective importance
            # to reduce tie-driven jitter.
            cand = sorted(
                prios,
                key=lambda pr: (int(s.deficit.get(pr, 0) or 0), int(s.quantum.get(pr, 0) or 0)),
                reverse=True,
            )

            # If deficits are all <= 0, still schedule highest importance work available
            # (deficits will replenish next tick).
            tried = 0
            for pr in cand:
                tried += 1
                p, ops = _pop_next_pipeline_with_ready_op(s, pr)
                if p is None or not ops:
                    continue

                # If high-priority work exists, be extra conservative about scheduling batch.
                if pr == Priority.BATCH_PIPELINE and (query_ready or interactive_ready):
                    # Only schedule batch if we can preserve headroom after doing so.
                    # This prevents batch from consuming the last resources and hurting p95/p99.
                    op0 = ops[0]
                    cpu_need = _compute_cpu(s, pool, pr, op0)
                    ram_need = _compute_ram(s, pool, pr, op0)

                    cpu_need = min(cpu_need, avail_cpu)
                    ram_need = min(ram_need, avail_ram)

                    if (avail_cpu - cpu_need) < headroom_cpu or (avail_ram - ram_need) < headroom_ram:
                        continue  # try another priority (interactive/query)

                chosen_prio, chosen_pipeline, chosen_ops = pr, p, ops
                break

            if chosen_pipeline is None:
                break

            op0 = chosen_ops[0]
            cpu_need = _compute_cpu(s, pool, chosen_prio, op0)
            ram_need = _compute_ram(s, pool, chosen_prio, op0)

            # Fit to available; keep at least 1 CPU and 1 RAM unit if possible
            if ram_need > avail_ram:
                # Can't place due to RAM; stop scheduling in this pool this tick
                break
            cpu_need = min(cpu_need, avail_cpu)
            if cpu_need <= 0:
                break

            # For batch (even when no high-priority ready work), preserve some headroom
            # to protect sudden arrivals; if not possible, skip batch for now.
            if chosen_prio == Priority.BATCH_PIPELINE:
                if (avail_cpu - cpu_need) < headroom_cpu or (avail_ram - ram_need) < headroom_ram:
                    # Try to reduce CPU first (RAM is the usual OOM culprit).
                    reduced_cpu = max(1, min(cpu_need, avail_cpu - headroom_cpu))
                    if reduced_cpu < cpu_need:
                        cpu_need = reduced_cpu
                    # If still can't keep headroom, don't schedule batch here.
                    if (avail_cpu - cpu_need) < headroom_cpu or (avail_ram - ram_need) < headroom_ram:
                        # Reduce RAM only if it doesn't go to zero; but reducing RAM risks OOM,
                        # so we prefer skipping over risking failures.
                        break

            assignments.append(
                Assignment(
                    ops=chosen_ops,
                    cpu=int(cpu_need),
                    ram=int(ram_need),
                    priority=chosen_pipeline.priority,
                    pool_id=pool_id,
                    pipeline_id=chosen_pipeline.pipeline_id,
                )
            )

            avail_cpu -= int(cpu_need)
            avail_ram -= int(ram_need)

            # Consume deficit for the chosen priority
            s.deficit[chosen_prio] = int(s.deficit.get(chosen_prio, 0) or 0) - 1

    return suspensions, assignments
