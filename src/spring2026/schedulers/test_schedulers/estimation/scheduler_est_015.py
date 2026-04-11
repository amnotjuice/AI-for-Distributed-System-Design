# policy_key: scheduler_est_015
# reasoning_effort: low
# exp: estimation
# model: gpt-5.2-2025-12-11
# llm_cost: 0.049980
# generation_seconds: 3940.51
# generated_at: 2026-04-10T09:03:40.953742
@register_scheduler_init(key="scheduler_est_015")
def scheduler_est_015_init(s):
    """Priority-first, memory-aware, OOM-averse packing scheduler.

    Goals:
      - Protect QUERY/INTERACTIVE latency by always scheduling them first and giving them
        slightly more CPU.
      - Reduce OOM failures by using op.estimate.mem_peak_gb as a placement filter and
        by retrying failed ops with increasing RAM (bounded retries).
      - Avoid starving BATCH by allowing it to run when there is headroom, but keep
        a RAM/CPU reserve for high-priority arrivals.
    """
    # Active pipelines we still consider for scheduling (by id)
    s.active_pipelines = {}

    # Simple round-robin pointer to avoid always picking the same pipeline within a class
    s.rr_index = {
        "QUERY": 0,
        "INTERACTIVE": 0,
        "BATCH_PIPELINE": 0,
    }

    # Failure tracking to adapt RAM on retries
    # Keyed by a stable-ish operator key (best-effort).
    s.op_fail_counts = {}          # op_key -> int
    s.op_last_ram_gb = {}          # op_key -> float
    s.op_last_pool_id = {}         # op_key -> int

    # Max retries per operator before we stop scheduling it (to avoid infinite churn)
    s.max_retries_per_op = 4

    # Safety multiplier applied to estimated memory when sizing RAM
    s.base_mem_safety = 1.35

    # Minimum RAM (GB) to allocate when estimate is missing
    s.default_min_ram_gb = 1.0


@register_scheduler(key="scheduler_est_015")
def scheduler_est_015_scheduler(s, results, pipelines):
    """
    Scheduling loop:
      1) Ingest new pipelines into active set.
      2) Process results: on failures, increase per-op retry counters (assume OOM-prone).
      3) Build one-op-per-pipeline candidates (parents complete), grouped by priority.
      4) For each pool, pack assignments in priority order with best-fit RAM placement
         using the (noisy) memory estimate as a hint and with a reserve for high-priority.
    """
    # Local helper functions to keep policy readable.
    def _pkey(priority):
        # Priority ordering: QUERY > INTERACTIVE > BATCH_PIPELINE
        # We compare via string names to avoid assuming numeric ordering.
        name = getattr(priority, "name", str(priority))
        if "QUERY" in name:
            return 0
        if "INTERACTIVE" in name:
            return 1
        return 2

    def _priority_name(priority):
        name = getattr(priority, "name", str(priority))
        if "QUERY" in name:
            return "QUERY"
        if "INTERACTIVE" in name:
            return "INTERACTIVE"
        return "BATCH_PIPELINE"

    def _op_key(op):
        # Best-effort stable key across ticks.
        for attr in ("operator_id", "op_id", "id", "name"):
            if hasattr(op, attr):
                try:
                    v = getattr(op, attr)
                    if callable(v):
                        v = v()
                    if v is not None:
                        return (attr, v)
                except Exception:
                    pass
        return ("py_id", id(op))

    def _est_mem_gb(op):
        try:
            est = getattr(op, "estimate", None)
            if est is None:
                return None
            v = getattr(est, "mem_peak_gb", None)
            if v is None:
                return None
            # Guard against weird values
            if v < 0:
                return None
            return float(v)
        except Exception:
            return None

    def _should_reserve_for_hp(hp_waiting):
        # If there is any high-priority work waiting, keep a reserve.
        return hp_waiting > 0

    def _reserve_targets(pool, hp_waiting):
        # Keep a small reserve to quickly admit query/interactive arrivals.
        # This prevents batch from consuming the last RAM and causing HP to wait.
        if not _should_reserve_for_hp(hp_waiting):
            return 0.0, 0.0
        # Reserve at least 1 CPU and 15% RAM, capped reasonably.
        cpu_res = min(1.0, float(pool.max_cpu_pool))
        ram_res = min(max(1.0, 0.15 * float(pool.max_ram_pool)), float(pool.max_ram_pool))
        return cpu_res, ram_res

    def _cpu_target(priority, avail_cpu):
        # Give higher priority slightly more CPU, but keep it bounded to allow packing.
        pname = _priority_name(priority)
        if pname == "QUERY":
            target = 4.0
        elif pname == "INTERACTIVE":
            target = 3.0
        else:
            target = 2.0
        return max(1.0, min(float(avail_cpu), target))

    def _ram_target(pool, op, priority):
        # Use estimator when present; otherwise allocate conservatively.
        est = _est_mem_gb(op)
        ok = _op_key(op)
        fails = s.op_fail_counts.get(ok, 0)

        # Exponential backoff on retries (assumes failure likely memory-related).
        # 0 fails => 1.0, 1 fail => 1.5, 2 fails => 2.25, ...
        retry_mult = 1.0 * (1.5 ** max(0, fails))

        # Priority-dependent extra safety: queries/interactive get slightly more headroom.
        pname = _priority_name(priority)
        prio_mult = 1.10 if pname in ("QUERY", "INTERACTIVE") else 1.00

        if est is None:
            # When no estimate: allocate a modest chunk, but not too large.
            # Make it large enough to reduce random OOM while still allowing packing.
            base = max(s.default_min_ram_gb, 0.20 * float(pool.max_ram_pool))
        else:
            base = est * s.base_mem_safety

        ram = base * retry_mult * prio_mult

        # If we have last attempted RAM and it failed, ensure we increase at least somewhat.
        last = s.op_last_ram_gb.get(ok, None)
        if last is not None and fails > 0:
            ram = max(ram, last * 1.25)

        # Cap to pool max (can't allocate more than pool has overall).
        ram = min(ram, float(pool.max_ram_pool))
        # Always at least a small minimum.
        ram = max(ram, s.default_min_ram_gb)
        return float(ram)

    def _fits_with_hint(pool, op, avail_ram):
        # Use estimate to avoid obviously impossible placements.
        est = _est_mem_gb(op)
        if est is None:
            return True
        # Be slightly conservative: require estimate * 1.05 <= available.
        return (est * 1.05) <= float(avail_ram)

    def _best_pool_for_op(op, priority, pool_states):
        # Pick a pool where the op likely fits and leaves the least leftover RAM (best fit),
        # while also having CPU available.
        best = None
        best_score = None

        for ps in pool_states:
            pool_id, pool, avail_cpu, avail_ram, hp_waiting = ps
            if avail_cpu <= 0 or avail_ram <= 0:
                continue
            if not _fits_with_hint(pool, op, avail_ram):
                continue

            cpu_res, ram_res = _reserve_targets(pool, hp_waiting)
            # For batch, enforce reserve; for HP, ignore reserve (they *are* the reserve users).
            pname = _priority_name(priority)
            if pname == "BATCH_PIPELINE":
                if avail_cpu - cpu_res < 1.0 or avail_ram - ram_res < s.default_min_ram_gb:
                    continue

            # Score by leftover RAM after allocating target, with mild preference for more CPU available.
            ram_need = _ram_target(pool, op, priority)
            if ram_need > avail_ram:
                continue

            leftover = float(avail_ram) - float(ram_need)
            score = leftover  # lower leftover preferred (tighter packing)
            # Tie-breaker: prefer pool with more available CPU (less likely to queue inside pool)
            score = (score, -float(avail_cpu))

            if best_score is None or score < best_score:
                best_score = score
                best = pool_id

        return best

    # Ingest new pipelines
    for p in pipelines:
        s.active_pipelines[p.pipeline_id] = p

    # Early exit when nothing changes
    if not pipelines and not results:
        return [], []

    # Process results: update failure counters and last used resources
    for r in results:
        try:
            for op in getattr(r, "ops", []) or []:
                ok = _op_key(op)
                if hasattr(r, "ram") and r.ram is not None:
                    try:
                        s.op_last_ram_gb[ok] = float(r.ram)
                    except Exception:
                        pass
                if hasattr(r, "pool_id"):
                    s.op_last_pool_id[ok] = r.pool_id
                if hasattr(r, "cpu") and r.cpu is not None:
                    # We currently don't use cpu history, but could later.
                    pass

                if hasattr(r, "failed") and callable(r.failed) and r.failed():
                    s.op_fail_counts[ok] = s.op_fail_counts.get(ok, 0) + 1
        except Exception:
            # Keep scheduler robust to unexpected result shapes
            pass

    # Prune successful pipelines from active set (keep failed/incomplete to avoid 720s penalty)
    # If a pipeline has ops exceeding retry cap, we stop scheduling it to avoid infinite churn.
    to_delete = []
    for pid, p in list(s.active_pipelines.items()):
        status = p.runtime_status()
        if status.is_pipeline_successful():
            to_delete.append(pid)
            continue

        # If pipeline has any assignable failed ops with too many retries, treat as doomed.
        failed_ops = status.get_ops([OperatorState.FAILED], require_parents_complete=False) or []
        doomed = False
        for op in failed_ops:
            if s.op_fail_counts.get(_op_key(op), 0) >= s.max_retries_per_op:
                doomed = True
                break
        if doomed:
            # Removing prevents consuming resources on repeated failures; still likely penalized,
            # but avoids dragging down overall weighted score by harming HP latency.
            to_delete.append(pid)

    for pid in to_delete:
        if pid in s.active_pipelines:
            del s.active_pipelines[pid]

    # Build one assignable op per pipeline (parents complete), grouped by priority.
    candidates_by_prio = {"QUERY": [], "INTERACTIVE": [], "BATCH_PIPELINE": []}
    hp_waiting_count = 0

    for p in s.active_pipelines.values():
        status = p.runtime_status()
        # Skip if already successful (should have been removed) or if irrecoverably failed.
        if status.is_pipeline_successful():
            continue

        op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True) or []
        if not op_list:
            continue

        # Prefer PENDING ops over FAILED ops when both are assignable, to make forward progress.
        pending = [op for op in op_list if getattr(op, "state", None) == OperatorState.PENDING]
        pick_from = pending if pending else op_list

        # Pick the "smallest" op by estimated memory to reduce tail latency and increase packing.
        def _cand_sort_key(op):
            est = _est_mem_gb(op)
            est_key = est if est is not None else 1e18
            fails = s.op_fail_counts.get(_op_key(op), 0)
            # Slightly deprioritize high-retry ops to reduce churn while still allowing progress.
            return (est_key, fails)

        op = sorted(pick_from, key=_cand_sort_key)[0]
        pname = _priority_name(p.priority)
        candidates_by_prio[pname].append((p, op))

        if pname in ("QUERY", "INTERACTIVE"):
            hp_waiting_count += 1

    # Apply simple RR within each class to avoid always scheduling the same first pipeline.
    def _rr_rotate(lst, idx):
        if not lst:
            return lst
        i = idx % len(lst)
        return lst[i:] + lst[:i]

    for pname in ("QUERY", "INTERACTIVE", "BATCH_PIPELINE"):
        candidates_by_prio[pname] = _rr_rotate(candidates_by_prio[pname], s.rr_index.get(pname, 0))

    suspensions = []
    assignments = []

    # Snapshot per-pool state we can mutate as we assign within this tick.
    pool_states = []
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        pool_states.append([pool_id, pool, float(pool.avail_cpu_pool), float(pool.avail_ram_pool), hp_waiting_count])

    # Main packing loop:
    # Try to assign multiple containers per tick per pool as long as there is capacity.
    # Priority order: QUERY -> INTERACTIVE -> BATCH
    prio_order = ("QUERY", "INTERACTIVE", "BATCH_PIPELINE")

    # Limit batch starts per tick to avoid spikes that delay HP (soft limit).
    batch_starts_remaining = max(1, s.executor.num_pools)

    made_progress = True
    while made_progress:
        made_progress = False

        # Stop if no candidates left at all.
        if not (candidates_by_prio["QUERY"] or candidates_by_prio["INTERACTIVE"] or candidates_by_prio["BATCH_PIPELINE"]):
            break

        # Always attempt to schedule highest priority available first.
        for pname in prio_order:
            if pname == "BATCH_PIPELINE" and batch_starts_remaining <= 0:
                continue
            if not candidates_by_prio[pname]:
                continue

            # Choose next candidate in this priority class:
            # Sort by estimated memory ascending each iteration to pack tightly.
            def _prio_cand_key(item):
                _p, _op = item
                est = _est_mem_gb(_op)
                est_key = est if est is not None else 1e18
                fails = s.op_fail_counts.get(_op_key(_op), 0)
                return (est_key, fails)

            candidates_by_prio[pname].sort(key=_prio_cand_key)
            pipeline, op = candidates_by_prio[pname][0]

            # If op has exceeded retry cap, drop it from consideration.
            if s.op_fail_counts.get(_op_key(op), 0) >= s.max_retries_per_op:
                candidates_by_prio[pname].pop(0)
                made_progress = True
                continue

            # Select best pool for this op given current availability.
            pool_id = _best_pool_for_op(op, pipeline.priority, pool_states)
            if pool_id is None:
                # Can't place now (likely memory). Defer and try other candidates.
                # Rotate list to avoid getting stuck on one huge op.
                candidates_by_prio[pname] = candidates_by_prio[pname][1:] + candidates_by_prio[pname][:1]
                continue

            # Fetch and update chosen pool state.
            ps = None
            for x in pool_states:
                if x[0] == pool_id:
                    ps = x
                    break
            if ps is None:
                continue
            _, pool, avail_cpu, avail_ram, _ = ps

            # Compute container sizing
            cpu = _cpu_target(pipeline.priority, avail_cpu)
            ram = _ram_target(pool, op, pipeline.priority)

            # Enforce reserves for batch if HP waiting.
            cpu_res, ram_res = _reserve_targets(pool, hp_waiting_count)
            if pname == "BATCH_PIPELINE" and _should_reserve_for_hp(hp_waiting_count):
                if (avail_cpu - cpu) < cpu_res or (avail_ram - ram) < ram_res:
                    # Defer batch to avoid starving HP.
                    candidates_by_prio[pname] = candidates_by_prio[pname][1:] + candidates_by_prio[pname][:1]
                    continue

            if cpu <= 0 or ram <= 0 or cpu > avail_cpu or ram > avail_ram:
                # Should not happen due to checks, but keep safe.
                candidates_by_prio[pname] = candidates_by_prio[pname][1:] + candidates_by_prio[pname][:1]
                continue

            # Create assignment (single op per container for better packing and less blast radius).
            assignment = Assignment(
                ops=[op],
                cpu=cpu,
                ram=ram,
                priority=pipeline.priority,
                pool_id=pool_id,
                pipeline_id=pipeline.pipeline_id,
            )
            assignments.append(assignment)

            # Update pool availability snapshot.
            ps[2] = avail_cpu - cpu
            ps[3] = avail_ram - ram

            # Remove candidate and mark progress.
            candidates_by_prio[pname].pop(0)
            made_progress = True

            if pname == "BATCH_PIPELINE":
                batch_starts_remaining -= 1

            # Update RR pointer for that priority class (so next tick shifts).
            s.rr_index[pname] = (s.rr_index.get(pname, 0) + 1) % max(1, len(candidates_by_prio[pname]) + 1)

            # After a successful placement of HP, decrease hp_waiting_count for reserve computation.
            if pname in ("QUERY", "INTERACTIVE") and hp_waiting_count > 0:
                hp_waiting_count -= 1
                # Update hp_waiting in pool_states
                for st in pool_states:
                    st[4] = hp_waiting_count

            # Restart from highest priority after any successful placement.
            break

    return suspensions, assignments
