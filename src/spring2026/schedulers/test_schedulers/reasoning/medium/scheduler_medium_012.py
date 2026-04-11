# policy_key: scheduler_medium_012
# reasoning_effort: medium
# exp: reasoning
# model: gpt-5.2-2025-12-11
# llm_cost: 0.093237
# generation_seconds: 70.69
# generated_at: 2026-04-09T22:44:26.791146
@register_scheduler_init(key="scheduler_medium_012")
def scheduler_medium_012_init(s):
    """
    Priority-first, OOM-robust scheduler with light SRPT and aging.

    Core ideas:
      - Always prefer QUERY > INTERACTIVE > BATCH for runnable operators.
      - Reduce end-to-end latency by choosing the pipeline with the fewest remaining pending/failed ops (SRPT-like).
      - Avoid expensive failures by being conservative with RAM for high priority, and adaptively increasing RAM on failures.
      - Preserve fairness with simple aging so background work still makes progress.
    """
    # Global step counter for deterministic aging.
    s._step = 0

    # Active pipelines (by id) and their bookkeeping.
    s._pipelines = {}  # pipeline_id -> Pipeline

    # Per-pipeline metadata.
    s._pmeta = {}  # pipeline_id -> dict(arrival_step, last_pick_step, fail_count)

    # Per-operator resource hints: (pipeline_id, op_key) -> dict(ram, cpu, fails)
    s._op_hints = {}

    # Track arrivals by priority for optional debugging/metrics (not required by simulator).
    s._arrivals = {Priority.QUERY: 0, Priority.INTERACTIVE: 0, Priority.BATCH_PIPELINE: 0}


def _op_key(op):
    """Best-effort stable key for an operator across ticks."""
    for attr in ("operator_id", "op_id", "id", "name"):
        if hasattr(op, attr):
            try:
                v = getattr(op, attr)
                # Some attrs may be methods; avoid calling.
                if callable(v):
                    continue
                return (attr, str(v))
            except Exception:
                pass
    # Fallback: repr is usually stable enough within a run; include type to reduce collisions.
    try:
        return ("repr", repr(op))
    except Exception:
        return ("pyid", str(id(op)))


def _priority_rank(p):
    """Lower is better."""
    if p == Priority.QUERY:
        return 0
    if p == Priority.INTERACTIVE:
        return 1
    return 2


def _default_ram_fraction(priority):
    # Conservative RAM for high priority to reduce OOM retries (which hurt latency and completion probability).
    if priority == Priority.QUERY:
        return 0.55
    if priority == Priority.INTERACTIVE:
        return 0.40
    return 0.22


def _default_cpu_cap(priority):
    # CPU caps to encourage concurrency; queries get more CPU, but not the whole pool.
    if priority == Priority.QUERY:
        return 4.0
    if priority == Priority.INTERACTIVE:
        return 2.0
    return 1.0


def _ensure_pipeline_tracked(s, p):
    if p.pipeline_id not in s._pipelines:
        s._pipelines[p.pipeline_id] = p
        s._pmeta[p.pipeline_id] = {
            "arrival_step": s._step,
            "last_pick_step": -1,
            "fail_count": 0,
        }
        if p.priority in s._arrivals:
            s._arrivals[p.priority] += 1


def _pipeline_remaining_ops(p):
    """Approximate remaining work with count of pending/failed ops (ignores running)."""
    status = p.runtime_status()
    try:
        rem = len(status.get_ops(ASSIGNABLE_STATES, require_parents_complete=False))
        # If nothing assignable, still might have assigned/running; treat as non-zero to avoid SRPT bias to "stuck".
        return rem if rem > 0 else 1
    except Exception:
        return 1


def _pipeline_has_runnable_op(p):
    status = p.runtime_status()
    try:
        op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        return len(op_list) > 0
    except Exception:
        return False


def _get_runnable_op(p):
    """Prefer retrying FAILED ops first (with larger hints), then PENDING."""
    status = p.runtime_status()
    failed_first = []
    try:
        failed_first = status.get_ops([OperatorState.FAILED], require_parents_complete=True)
    except Exception:
        failed_first = []
    if failed_first:
        return failed_first[0]
    try:
        ops = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        return ops[0] if ops else None
    except Exception:
        return None


def _effective_selection_score(s, p):
    """
    Lower is better. Combine:
      - priority rank (dominant)
      - SRPT proxy (few remaining ops)
      - aging bonus (older pipelines get pulled forward)
      - trouble penalty for repeatedly failing low priority pipelines
    """
    meta = s._pmeta.get(p.pipeline_id, {"arrival_step": s._step, "fail_count": 0})
    wait = max(0, s._step - meta["arrival_step"])
    rem = _pipeline_remaining_ops(p)

    # Aging: small per-step benefit, stronger for low priority to avoid starvation.
    if p.priority == Priority.QUERY:
        aging = 0.015 * wait
    elif p.priority == Priority.INTERACTIVE:
        aging = 0.030 * wait
    else:
        aging = 0.060 * wait

    # Penalize repeatedly failing batch pipelines to avoid harming the whole system after they're "likely-broken".
    trouble = meta.get("fail_count", 0)
    trouble_penalty = 0.0
    if p.priority == Priority.BATCH_PIPELINE and trouble >= 3:
        trouble_penalty = 1000.0 + 50.0 * (trouble - 3)

    # Priority dominates by large constant offsets.
    return 10000.0 * _priority_rank(p.priority) + (rem - aging) + trouble_penalty


def _compute_request(s, p, op, pool, avail_cpu, avail_ram):
    """
    Decide CPU/RAM for an operator:
      - Start conservative with RAM for high priorities.
      - On failures, double RAM (capped to pool max) and modestly increase CPU.
    """
    opk = (p.pipeline_id, _op_key(op))
    hint = s._op_hints.get(opk, None)

    # Default RAM based on pool size and priority.
    base_ram = pool.max_ram_pool * _default_ram_fraction(p.priority)
    base_ram = max(1.0, base_ram)

    # Default CPU based on priority cap and pool capacity.
    base_cpu = min(_default_cpu_cap(p.priority), pool.max_cpu_pool)
    base_cpu = max(1.0, base_cpu)

    if hint is not None:
        # Respect learned hints, but keep within pool limits.
        ram = max(base_ram, float(hint.get("ram", base_ram)))
        cpu = max(base_cpu, float(hint.get("cpu", base_cpu)))
    else:
        ram, cpu = base_ram, base_cpu

    # If this pipeline has been failing, be more aggressive for high priorities to increase completion probability.
    p_fail = s._pmeta.get(p.pipeline_id, {}).get("fail_count", 0)
    if p_fail >= 2 and p.priority in (Priority.QUERY, Priority.INTERACTIVE):
        ram = max(ram, 0.80 * pool.max_ram_pool)
        cpu = max(cpu, min(4.0, pool.max_cpu_pool))

    # Final caps to what's available right now and pool maxima.
    ram = min(ram, pool.max_ram_pool, avail_ram)
    cpu = min(cpu, pool.max_cpu_pool, avail_cpu)

    # If we cannot allocate at least 1 CPU and some RAM, we can't schedule.
    if cpu < 1.0 or ram <= 0:
        return None, None

    # Ensure we don't allocate fractional "almost 1" due to float arithmetic.
    cpu = max(1.0, cpu)
    ram = max(1.0, ram)
    return cpu, ram


def _update_hints_from_result(s, r):
    """Update RAM/CPU hints on failures to reduce repeat OOMs and speed up retries."""
    if not r.failed():
        return

    # Count per-pipeline failures to inform "trouble" handling.
    pid = getattr(r, "pipeline_id", None)
    if pid is not None and pid in s._pmeta:
        s._pmeta[pid]["fail_count"] = s._pmeta[pid].get("fail_count", 0) + 1

    # Update per-operator hints based on the attempted allocation.
    # Note: We may not always have pipeline_id in result; op object is available via r.ops.
    try:
        ops = r.ops or []
    except Exception:
        ops = []

    # Conservative: bump RAM aggressively (doubling) and CPU mildly for retries.
    for op in ops:
        # If result lacks pipeline_id, we can't namespace the hint reliably; skip.
        if pid is None:
            continue
        opk = (pid, _op_key(op))
        prev = s._op_hints.get(opk, {"ram": 0.0, "cpu": 1.0, "fails": 0})

        prev["fails"] = int(prev.get("fails", 0)) + 1

        attempted_ram = float(getattr(r, "ram", 0.0) or 0.0)
        attempted_cpu = float(getattr(r, "cpu", 0.0) or 0.0)

        # If simulator reports 0, still increase from previous.
        new_ram = max(float(prev.get("ram", 0.0)), attempted_ram)
        if new_ram <= 0.0:
            new_ram = 1.0

        # After each failure, double RAM up to a reasonable ceiling; later capped by pool at scheduling time.
        # Add a small additive bump to avoid being stuck on tiny sizes.
        new_ram = new_ram * 2.0 + 1.0

        new_cpu = max(float(prev.get("cpu", 1.0)), attempted_cpu, 1.0)
        # Mild CPU bump every other failure (keeps concurrency).
        if prev["fails"] % 2 == 0:
            new_cpu = min(new_cpu + 1.0, 4.0)

        prev["ram"] = new_ram
        prev["cpu"] = new_cpu
        s._op_hints[opk] = prev


@register_scheduler(key="scheduler_medium_012")
def scheduler_medium_012_scheduler(s, results, pipelines):
    """
    Scheduling loop:
      1) Track arrivals and update failure-driven hints.
      2) For each pool, repeatedly pack runnable ops, prioritizing QUERY/INTERACTIVE,
         using SRPT-like choice among runnable pipelines with aging.
      3) No preemption (sim interface may not expose running container lists reliably);
         instead, strong priority ordering and conservative RAM reduce tail latency and failures.
    """
    s._step += 1

    # Track new pipelines.
    for p in pipelines:
        _ensure_pipeline_tracked(s, p)

    # Learn from execution results (especially failures).
    for r in results:
        _update_hints_from_result(s, r)

    # Early exit if nothing changed (keeps simulator fast).
    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    # Clean up completed pipelines from active set.
    # (Do not drop pipelines just because they have FAILED ops; we want to retry to avoid 720s penalties.)
    to_delete = []
    for pid, p in s._pipelines.items():
        try:
            if p.runtime_status().is_pipeline_successful():
                to_delete.append(pid)
        except Exception:
            pass
    for pid in to_delete:
        s._pipelines.pop(pid, None)
        s._pmeta.pop(pid, None)
        # Keep op hints around (they're namespaced by pipeline_id, but harmless); could be cleared if desired.

    # Build a quick list of runnable pipelines each tick (those with at least one op whose parents are complete).
    runnable = []
    for p in s._pipelines.values():
        if _pipeline_has_runnable_op(p):
            runnable.append(p)

    # For each pool, pack multiple assignments until resources are exhausted or no fitting job remains.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)

        if avail_cpu < 1.0 or avail_ram <= 0.0:
            continue

        # Greedy packing: keep scheduling while we can fit at least one runnable op.
        # We re-evaluate choices after each placement to adapt to remaining resources.
        while avail_cpu >= 1.0 and avail_ram > 0.0:
            # Choose best runnable pipeline by (priority, SRPT, aging).
            # We also require that its current runnable op can fit in remaining resources (after request calc).
            best = None
            best_op = None
            best_score = None
            best_req = None

            # Consider only pipelines with runnable ops; order by score, then try to fit.
            # To limit overhead, do a single pass and pick the best fitting candidate.
            for p in runnable:
                # Skip if already completed between loops.
                try:
                    if p.runtime_status().is_pipeline_successful():
                        continue
                except Exception:
                    pass

                op = _get_runnable_op(p)
                if op is None:
                    continue

                cpu_req, ram_req = _compute_request(s, p, op, pool, avail_cpu, avail_ram)
                if cpu_req is None:
                    continue

                # If the request is too large, we could try downsizing CPU (not RAM) to fit.
                # Downsizing RAM below hint increases OOM risk; for high priority, avoid it.
                if cpu_req > avail_cpu:
                    cpu_req = avail_cpu
                if ram_req > avail_ram:
                    # For QUERY/INTERACTIVE, don't squeeze RAM; try another pipeline.
                    # For BATCH, allow squeezing a bit to keep throughput.
                    if p.priority == Priority.BATCH_PIPELINE:
                        ram_req = avail_ram
                    else:
                        continue

                if cpu_req < 1.0 or ram_req <= 0.0:
                    continue

                score = _effective_selection_score(s, p)
                if best is None or score < best_score:
                    best, best_op, best_score = p, op, score
                    best_req = (cpu_req, ram_req)

            if best is None:
                break

            cpu_req, ram_req = best_req

            # Create assignment for a single operator to reduce blast radius and keep interactive latency low.
            assignments.append(
                Assignment(
                    ops=[best_op],
                    cpu=cpu_req,
                    ram=ram_req,
                    priority=best.priority,
                    pool_id=pool_id,
                    pipeline_id=best.pipeline_id,
                )
            )

            # Update available resources for further packing.
            avail_cpu -= float(cpu_req)
            avail_ram -= float(ram_req)

            # Update pipeline meta to support anti-starvation among same priority.
            meta = s._pmeta.get(best.pipeline_id)
            if meta is not None:
                meta["last_pick_step"] = s._step

            # Remove from runnable if it has no more runnable ops (optional micro-optimization).
            # It may still have more runnable ops after this assignment; keep it to allow parallelism across pools.
            # Within the same pool loop, leaving it is fine since parents may not be complete for next op anyway.

    return suspensions, assignments
