# policy_key: scheduler_medium_008
# reasoning_effort: medium
# exp: reasoning
# model: gpt-5.2-2025-12-11
# llm_cost: 0.112893
# generation_seconds: 88.05
# generated_at: 2026-04-09T22:39:06.993094
@register_scheduler_init(key="scheduler_medium_008")
def scheduler_medium_008_init(s):
    """
    Priority-first, OOM-aware, non-preemptive scheduler.

    Main ideas:
      - Always try to place QUERY, then INTERACTIVE, then BATCH to protect weighted latency.
      - Use conservative (but not maximal) default RAM sizing to reduce OOM/fail penalties.
      - Learn per-operator RAM hints from OOM failures and quickly ramp RAM on retry.
      - Add mild aging so BATCH eventually progresses without starving.
      - One op per pipeline per tick to avoid head-of-line monopolization.
      - Placement: high-priority prefers the pool with most headroom; batch prefers best-fit packing.
    """
    s.tick = 0

    # pipeline_id -> {"enqueue_tick": int, "priority": Priority}
    s.pipeline_meta = {}

    # pipeline_id -> Pipeline (kept until success)
    s.pipelines_by_id = {}

    # op_key -> learned minimal "safe" RAM allocation (absolute units)
    s.ram_hint_by_op = {}

    # Track recent OOMs (for light diagnostics / optional heuristics)
    s.oom_events = 0


@register_scheduler(key="scheduler_medium_008")
def scheduler_medium_008(s, results, pipelines):
    """
    Scheduler step:
      - Incorporate new pipelines.
      - Update RAM hints from OOM failures in results.
      - Build ready candidates (one ready op per pipeline).
      - Iteratively place assignments by priority class across pools until no feasible placement.
    """
    s.tick += 1

    # -------- helpers --------
    def _prio_base(p):
        # Larger means "schedule sooner"
        if p == Priority.QUERY:
            return 1000.0
        if p == Priority.INTERACTIVE:
            return 600.0
        return 200.0  # Priority.BATCH_PIPELINE

    def _prio_age_mult(p):
        # Mild aging; batch ages slightly faster to prevent starvation.
        if p == Priority.QUERY:
            return 1.0
        if p == Priority.INTERACTIVE:
            return 1.2
        return 2.0

    def _effective_score(pipeline_id, priority):
        meta = s.pipeline_meta.get(pipeline_id, None)
        if not meta:
            return _prio_base(priority)
        age = max(0, s.tick - meta["enqueue_tick"])
        return _prio_base(priority) + _prio_age_mult(priority) * float(age)

    def _op_key(op):
        # Best-effort stable key for learning RAM requirements.
        for attr in ("op_id", "operator_id", "id", "name"):
            if hasattr(op, attr):
                try:
                    v = getattr(op, attr)
                    # attribute could be a method or value
                    if callable(v):
                        v = v()
                    if v is not None:
                        return (attr, str(v))
                except Exception:
                    pass
        # Fallback: object identity (stable within one simulation run)
        return ("py_id", int(id(op)))

    def _is_oom_error(err):
        if err is None:
            return False
        try:
            msg = str(err).lower()
        except Exception:
            return False
        return ("oom" in msg) or ("out of memory" in msg) or ("cuda out of memory" in msg)

    def _default_ram_fraction(priority):
        # Conservative defaults to reduce OOM-induced 720s penalties.
        if priority == Priority.QUERY:
            return 0.25
        if priority == Priority.INTERACTIVE:
            return 0.20
        return 0.15

    def _target_cpu(priority, pool_max_cpu):
        # Give more CPU to high priority (reduce latency), but cap to keep concurrency.
        if pool_max_cpu <= 0:
            return 0.0
        if priority == Priority.QUERY:
            return min(8.0, max(1.0, 0.50 * float(pool_max_cpu)))
        if priority == Priority.INTERACTIVE:
            return min(6.0, max(1.0, 0.33 * float(pool_max_cpu)))
        return min(4.0, max(1.0, 0.25 * float(pool_max_cpu)))

    def _required_ram_for_op(op, priority, pool_max_ram):
        # Learned RAM hint takes precedence; otherwise conservative fraction of pool size.
        if pool_max_ram <= 0:
            return 0.0
        k = _op_key(op)
        learned = s.ram_hint_by_op.get(k, None)
        base = _default_ram_fraction(priority) * float(pool_max_ram)
        req = max(base, float(learned) if learned is not None else 0.0)
        # Never request more than pool capacity.
        return min(req, float(pool_max_ram))

    def _choose_pool_for_request(priority, req_cpu, req_ram, pool_avail_cpu, pool_avail_ram, pool_max_cpu, pool_max_ram):
        # Returns best pool_id or None.
        best_pool = None
        best_score = None

        for pid in range(s.executor.num_pools):
            a_cpu = pool_avail_cpu[pid]
            a_ram = pool_avail_ram[pid]
            if req_cpu <= 0 or req_ram <= 0:
                continue
            if a_cpu + 1e-9 < req_cpu or a_ram + 1e-9 < req_ram:
                continue

            max_cpu = float(pool_max_cpu[pid]) if pool_max_cpu[pid] > 0 else 1.0
            max_ram = float(pool_max_ram[pid]) if pool_max_ram[pid] > 0 else 1.0

            if priority in (Priority.QUERY, Priority.INTERACTIVE):
                # Headroom-seeking: prefer pools with more free resources (reduce queueing/fragmentation risk).
                score = 2.0 * (a_cpu / max_cpu) + 1.0 * (a_ram / max_ram)
                if best_score is None or score > best_score:
                    best_score = score
                    best_pool = pid
            else:
                # Best-fit: preserve big contiguous headroom for future high-priority bursts.
                leftover_cpu = (a_cpu - req_cpu) / max_cpu
                leftover_ram = (a_ram - req_ram) / max_ram
                score = leftover_cpu + leftover_ram  # smaller is better
                if best_score is None or score < best_score:
                    best_score = score
                    best_pool = pid

        return best_pool

    # -------- incorporate new pipelines --------
    for p in pipelines:
        s.pipelines_by_id[p.pipeline_id] = p
        if p.pipeline_id not in s.pipeline_meta:
            s.pipeline_meta[p.pipeline_id] = {"enqueue_tick": s.tick, "priority": p.priority}

    # -------- learn from execution results (OOM -> bump RAM hint) --------
    for r in results:
        if not hasattr(r, "failed") or not r.failed():
            continue
        if not _is_oom_error(getattr(r, "error", None)):
            continue

        s.oom_events += 1

        # Ramp factor: quick convergence to avoid repeated 720s penalties.
        bump = 1.7
        alloc_ram = float(getattr(r, "ram", 0.0) or 0.0)
        if alloc_ram <= 0:
            continue
        new_hint = alloc_ram * bump

        for op in getattr(r, "ops", []) or []:
            k = _op_key(op)
            prev = s.ram_hint_by_op.get(k, 0.0) or 0.0
            if new_hint > prev:
                s.ram_hint_by_op[k] = new_hint

    # -------- cleanup completed pipelines --------
    # Remove successful pipelines to keep the candidate set small and prevent rescheduling.
    to_delete = []
    for pid, p in s.pipelines_by_id.items():
        try:
            status = p.runtime_status()
            if status.is_pipeline_successful():
                to_delete.append(pid)
        except Exception:
            # If status can't be read, keep it (avoid dropping).
            pass

    for pid in to_delete:
        s.pipelines_by_id.pop(pid, None)
        s.pipeline_meta.pop(pid, None)

    # Early exit if nothing new happened and no results arrived.
    if not pipelines and not results:
        return [], []

    # -------- build local pool availability snapshots --------
    pool_avail_cpu = []
    pool_avail_ram = []
    pool_max_cpu = []
    pool_max_ram = []
    for pid in range(s.executor.num_pools):
        pool = s.executor.pools[pid]
        pool_avail_cpu.append(float(pool.avail_cpu_pool))
        pool_avail_ram.append(float(pool.avail_ram_pool))
        pool_max_cpu.append(float(pool.max_cpu_pool))
        pool_max_ram.append(float(pool.max_ram_pool))

    # -------- build ready candidates by priority --------
    # candidate: (score, pipeline_id, pipeline, op)
    candidates = {Priority.QUERY: [], Priority.INTERACTIVE: [], Priority.BATCH_PIPELINE: []}

    for pid, p in s.pipelines_by_id.items():
        try:
            status = p.runtime_status()
        except Exception:
            continue

        # Skip if already complete.
        if status.is_pipeline_successful():
            continue

        # One ready op per pipeline (reduces monopolization and keeps behavior simple/deterministic).
        op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
        if not op_list:
            continue

        op = op_list[0]
        pr = p.priority
        sc = _effective_score(pid, pr)
        candidates[pr].append((sc, pid, p, op))

    # Sort within each priority by score (desc), then pipeline_id for determinism.
    for pr in candidates:
        candidates[pr].sort(key=lambda t: (-t[0], str(t[1])))

    suspensions = []  # non-preemptive policy for simplicity/robustness
    assignments = []

    scheduled_this_tick = set()

    def _try_place_from_list(pr):
        # Try to place the best feasible candidate from this priority list.
        lst = candidates[pr]
        i = 0
        while i < len(lst):
            _, pid, p, op = lst[i]
            if pid in scheduled_this_tick:
                i += 1
                continue

            # Choose pool-aware resource sizes.
            # CPU chosen by pool; RAM chosen by op hint and pool size.
            # We try pools in priority-specific "best pool" order via _choose_pool_for_request.
            # Since req depends on pool max, we will search pools by evaluating per pool.
            best_choice = None  # (pool_id, cpu, ram, pool_select_score)
            for pool_id in range(s.executor.num_pools):
                if pool_avail_cpu[pool_id] <= 0 or pool_avail_ram[pool_id] <= 0:
                    continue

                cpu = _target_cpu(pr, pool_max_cpu[pool_id])
                cpu = min(cpu, pool_avail_cpu[pool_id])
                if cpu < 1e-6:
                    continue

                ram = _required_ram_for_op(op, pr, pool_max_ram[pool_id])
                # Don't schedule if we can't fit the (learned/default) safe RAM.
                if ram < 1e-6 or pool_avail_ram[pool_id] + 1e-9 < ram:
                    continue

                # Choose pool using the same heuristic as global selection.
                chosen = _choose_pool_for_request(
                    pr, cpu, ram, pool_avail_cpu, pool_avail_ram, pool_max_cpu, pool_max_ram
                )
                if chosen is None:
                    continue
                if chosen != pool_id:
                    continue

                # Pool selection score (only used to break ties among feasible "chosen" pools).
                # Favor more headroom for high-priority; tighter fit for batch.
                max_cpu = pool_max_cpu[pool_id] if pool_max_cpu[pool_id] > 0 else 1.0
                max_ram = pool_max_ram[pool_id] if pool_max_ram[pool_id] > 0 else 1.0
                if pr in (Priority.QUERY, Priority.INTERACTIVE):
                    pool_score = 2.0 * (pool_avail_cpu[pool_id] / max_cpu) + 1.0 * (pool_avail_ram[pool_id] / max_ram)
                    better = (best_choice is None) or (pool_score > best_choice[3])
                else:
                    pool_score = ((pool_avail_cpu[pool_id] - cpu) / max_cpu) + ((pool_avail_ram[pool_id] - ram) / max_ram)
                    better = (best_choice is None) or (pool_score < best_choice[3])

                if better:
                    best_choice = (pool_id, cpu, ram, pool_score)

            if best_choice is None:
                # Can't place this candidate now; try next one (maybe smaller / different pool possible).
                i += 1
                continue

            pool_id, cpu, ram, _ = best_choice
            assignment = Assignment(
                ops=[op],
                cpu=cpu,
                ram=ram,
                priority=pr,
                pool_id=pool_id,
                pipeline_id=p.pipeline_id,
            )
            assignments.append(assignment)

            pool_avail_cpu[pool_id] -= float(cpu)
            pool_avail_ram[pool_id] -= float(ram)
            scheduled_this_tick.add(pid)

            # Remove from list to avoid reconsidering.
            lst.pop(i)
            return True

        return False

    # -------- iterative placement: QUERY -> INTERACTIVE -> BATCH --------
    # Keep placing until we fail to place from all classes in one full round.
    while True:
        placed = False

        # Strictly protect high priority: try to place all feasible QUERY ops first.
        if _try_place_from_list(Priority.QUERY):
            placed = True
        elif _try_place_from_list(Priority.INTERACTIVE):
            placed = True
        elif _try_place_from_list(Priority.BATCH_PIPELINE):
            placed = True

        if not placed:
            break

        # Stop if all pools are exhausted.
        any_cpu = any(c > 1e-6 for c in pool_avail_cpu)
        any_ram = any(r > 1e-6 for r in pool_avail_ram)
        if not (any_cpu and any_ram):
            break

    return suspensions, assignments
