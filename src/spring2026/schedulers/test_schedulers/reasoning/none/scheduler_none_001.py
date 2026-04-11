# policy_key: scheduler_none_001
# reasoning_effort: none
# model: gpt-5.2-2025-12-11
# llm_cost: 0.000000
# generation_seconds: 43.74
# generated_at: 2026-04-09T21:45:43.518202
@register_scheduler_init(key="scheduler_none_001")
def scheduler_none_001_init(s):
    """Priority-aware, failure-averse scheduler.

    Core ideas (small step up from FIFO):
    - Maintain three FIFO queues by priority: QUERY > INTERACTIVE > BATCH.
    - Admit only "ready" operators (parents completed).
    - Right-size RAM conservatively using a per-op memory multiplier learned from OOMs.
      * Start at a safe default fraction of pool RAM (bounded).
      * On OOM, increase the multiplier for that operator signature and retry later.
    - Avoid heavy preemption churn: only preempt when a high-priority op is ready but no pool
      has enough headroom, and we can free enough by suspending low-priority RUNNING containers.
    - Allocate CPU with a scale-up bias: give each assignment a decent chunk of pool CPU
      (bounded) to reduce latency for high-priority work without starving batch.
    """
    # Priority FIFO queues
    s.q_query = []
    s.q_interactive = []
    s.q_batch = []

    # Track per-pipeline start time for end-to-end latency accounting (if simulator uses it elsewhere)
    s.pipeline_first_seen_ts = {}

    # Memory learning: map from "op signature" -> multiplier for requested RAM (to avoid repeated OOMs)
    s.op_mem_mult = {}

    # Conservative defaults
    s.default_mem_mult = 1.35  # start slightly above "guess"
    s.max_mem_mult = 8.0       # cap to avoid consuming whole pool forever
    s.mem_mult_backoff = 1.6   # multiply on OOM

    # CPU sizing knobs
    s.min_cpu_frac = 0.25      # at least this fraction of available CPU when we decide to schedule
    s.max_cpu_frac = 1.0       # can take all available in a pool if it helps
    s.batch_cpu_cap_frac = 0.5 # batch ops get at most this fraction of pool max CPU (scale-up bias, but not monopolize)

    # RAM sizing knobs
    s.min_ram_frac = 0.15      # request at least this fraction of pool max RAM to reduce OOM risk
    s.max_ram_frac = 0.85      # leave headroom to reduce contention/OOM cascades

    # Preemption controls
    s.enable_preemption = True
    s.preempt_only_if_no_fit = True
    s.max_preemptions_per_tick = 2  # keep churn low

    # Small anti-starvation aging for batch: after N ticks of not being scheduled, bump ahead
    s.batch_age = {}           # pipeline_id -> age counter
    s.batch_boost_after = 25   # ticks
    s.tick = 0


@register_scheduler(key="scheduler_none_001")
def scheduler_none_001_scheduler(s, results, pipelines):
    """
    Scheduler step:
    1) Ingest new pipelines into per-priority queues.
    2) Process results to learn from OOMs (increase per-op RAM multiplier).
    3) Try to place one ready operator per pool per tick (keeps behavior close to FIFO baseline).
       - Choose highest priority ready op across queues.
       - Size RAM conservatively; size CPU aggressively for high priority.
    4) If no placement is possible for high priority due to headroom, optionally preempt
       lower-priority RUNNING containers to make room.
    """
    # Helper functions kept inside to avoid imports; use duck typing and safe attribute access.
    def _prio_rank(p):
        # Lower is higher priority
        if p == Priority.QUERY:
            return 0
        if p == Priority.INTERACTIVE:
            return 1
        return 2  # Priority.BATCH_PIPELINE or others

    def _enqueue(p):
        if p.pipeline_id not in s.pipeline_first_seen_ts:
            # If simulator has time in executor/clock, store it; else just mark.
            now = getattr(getattr(s, "executor", None), "now", None)
            s.pipeline_first_seen_ts[p.pipeline_id] = now if now is not None else s.tick
        if p.priority == Priority.QUERY:
            s.q_query.append(p)
        elif p.priority == Priority.INTERACTIVE:
            s.q_interactive.append(p)
        else:
            s.q_batch.append(p)
            s.batch_age.setdefault(p.pipeline_id, 0)

    def _iter_queues_in_priority_order():
        # Return actual queue lists in descending priority
        return [s.q_query, s.q_interactive, s.q_batch]

    def _pipeline_done_or_failed(p):
        st = p.runtime_status()
        if st.is_pipeline_successful():
            return True
        # If any op is FAILED we don't necessarily drop the pipeline; failures could be retried.
        # But if the simulator marks a pipeline as failed terminally, it'll be reflected by no assignable ops.
        return False

    def _get_ready_ops(p, limit=1):
        st = p.runtime_status()
        # Require parents complete to keep correctness; also avoid scheduling multiple ops from same pipeline at once.
        ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        if not ops:
            return []
        return ops[:limit]

    def _op_signature(pipeline, op):
        # Robust "signature" for learning: combine pipeline_id-independent operator identity where possible.
        # Use common attributes if present, else fall back to repr().
        # This is intentionally conservative: worse case, learning doesn't generalize.
        try:
            op_id = getattr(op, "op_id", None) or getattr(op, "operator_id", None) or getattr(op, "name", None)
            op_kind = getattr(op, "kind", None) or getattr(op, "type", None) or getattr(op, "__class__", type(op)).__name__
            return (op_kind, op_id)
        except Exception:
            return ("op", repr(op)[:80])

    def _pool_headroom(pool_id):
        pool = s.executor.pools[pool_id]
        return pool.avail_cpu_pool, pool.avail_ram_pool, pool.max_cpu_pool, pool.max_ram_pool

    def _clamp(x, lo, hi):
        if x < lo:
            return lo
        if x > hi:
            return hi
        return x

    def _size_ram(pool_max_ram, pool_avail_ram, priority, mult):
        # Start with a safe fraction of pool max RAM, bounded by available RAM and headroom limits.
        # High priority gets a bit more RAM to reduce OOM risk (failures are very costly).
        base_frac = s.min_ram_frac
        if priority == Priority.QUERY:
            base_frac = max(base_frac, 0.22)
        elif priority == Priority.INTERACTIVE:
            base_frac = max(base_frac, 0.18)
        else:
            base_frac = max(base_frac, 0.15)

        target = pool_max_ram * base_frac
        target *= mult
        target = _clamp(target, pool_max_ram * s.min_ram_frac, pool_max_ram * s.max_ram_frac)
        target = min(target, pool_avail_ram)
        return target

    def _size_cpu(pool_max_cpu, pool_avail_cpu, priority):
        # Give high priority more CPU to cut latency; batch is capped to avoid monopolization.
        if pool_avail_cpu <= 0:
            return 0
        if priority == Priority.QUERY:
            frac = 1.0
        elif priority == Priority.INTERACTIVE:
            frac = 0.75
        else:
            frac = min(s.batch_cpu_cap_frac, 0.5)
        cpu = pool_max_cpu * frac
        cpu = min(cpu, pool_avail_cpu)
        # Ensure we don't allocate a tiny slice once we decided to schedule.
        cpu = max(cpu, pool_avail_cpu * s.min_cpu_frac)
        cpu = _clamp(cpu, 0, pool_avail_cpu)
        return cpu

    def _collect_running_containers_by_priority():
        # Best-effort: support multiple possible executor shapes.
        running = []  # (priority_rank, priority, pool_id, container_id, cpu, ram)
        ex = s.executor
        # Try common locations for containers; fall back to empty.
        container_sources = []
        for attr in ["containers", "running_containers", "active_containers"]:
            if hasattr(ex, attr):
                container_sources.append(getattr(ex, attr))
        for pool in getattr(ex, "pools", []):
            for attr in ["containers", "running_containers", "active_containers"]:
                if hasattr(pool, attr):
                    container_sources.append(getattr(pool, attr))
        # Normalize: if dict, take values; if list, iterate.
        seen = set()
        for src in container_sources:
            try:
                it = src.values() if hasattr(src, "values") else src
                for c in it:
                    try:
                        cid = getattr(c, "container_id", None) or getattr(c, "id", None)
                        pid = getattr(c, "pool_id", None)
                        if cid is None or pid is None:
                            continue
                        key = (pid, cid)
                        if key in seen:
                            continue
                        seen.add(key)
                        pr = getattr(c, "priority", None)
                        cpu = getattr(c, "cpu", 0)
                        ram = getattr(c, "ram", 0)
                        running.append((_prio_rank(pr), pr, pid, cid, cpu, ram))
                    except Exception:
                        continue
            except Exception:
                continue
        # Lower priority first for preemption (i.e., higher rank value)
        running.sort(key=lambda x: (-x[0], x[2]))
        return running

    # --- ingest new pipelines ---
    for p in pipelines:
        _enqueue(p)

    # --- process results and learn from OOMs / failures ---
    if results:
        for r in results:
            if not getattr(r, "failed", lambda: False)():
                continue
            err = getattr(r, "error", "") or ""
            is_oom = "oom" in str(err).lower() or "out of memory" in str(err).lower() or "memory" in str(err).lower()
            if not is_oom:
                continue
            # Learn per-op signature; if we can't, fall back to priority-wide bump by using a coarse key.
            ops = getattr(r, "ops", None) or []
            if not ops:
                sigs = [("unknown", "unknown")]
            else:
                # If multiple ops in one container, bump all.
                sigs = []
                for op in ops:
                    try:
                        sigs.append(_op_signature(None, op))
                    except Exception:
                        sigs.append(("op", repr(op)[:80]))

            for sig in sigs:
                cur = s.op_mem_mult.get(sig, s.default_mem_mult)
                nxt = _clamp(cur * s.mem_mult_backoff, s.default_mem_mult, s.max_mem_mult)
                s.op_mem_mult[sig] = nxt

    # --- update aging for batch (anti-starvation) ---
    s.tick += 1
    for q in [s.q_batch]:
        for p in q:
            s.batch_age[p.pipeline_id] = s.batch_age.get(p.pipeline_id, 0) + 1

    # If nothing changed, early exit
    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    # --- per-pool assignment: at most one operator per pool per tick (simple + stable) ---
    for pool_id in range(s.executor.num_pools):
        avail_cpu, avail_ram, max_cpu, max_ram = _pool_headroom(pool_id)
        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # Select next pipeline to consider: strict priority, with a small batch boost via aging.
        selected = None
        selected_q = None

        # Batch boost: if oldest batch waited long enough, allow it to be considered after interactive
        # (but still not before query).
        oldest_batch = None
        oldest_age = -1
        for p in s.q_batch:
            age = s.batch_age.get(p.pipeline_id, 0)
            if age > oldest_age:
                oldest_age = age
                oldest_batch = p
        batch_boost = oldest_batch is not None and oldest_age >= s.batch_boost_after

        # Priority order: QUERY -> INTERACTIVE -> (BATCH boosted?) -> BATCH
        queues = [s.q_query, s.q_interactive]
        if batch_boost:
            # Place the oldest batch pipeline ahead of the rest of batch for this selection attempt.
            # We'll still verify it has a ready op.
            pass
        queues.append(s.q_batch)

        # Find first pipeline with a ready op; for batch boost, check oldest batch once early.
        candidate_list = []
        if s.q_query:
            candidate_list.append(("query", s.q_query))
        if s.q_interactive:
            candidate_list.append(("interactive", s.q_interactive))
        if batch_boost and oldest_batch is not None:
            candidate_list.append(("batch_boost", [oldest_batch]))
        candidate_list.append(("batch", s.q_batch))

        chosen_pipeline = None
        chosen_op_list = None

        for _, q in candidate_list:
            # FIFO scan but cheap: try first few only to avoid O(n) every tick
            scan = q[:8] if len(q) > 8 else q
            for p in scan:
                if _pipeline_done_or_failed(p):
                    continue
                op_list = _get_ready_ops(p, limit=1)
                if not op_list:
                    continue
                chosen_pipeline = p
                chosen_op_list = op_list
                break
            if chosen_pipeline is not None:
                break

        if chosen_pipeline is None:
            continue

        # RAM multiplier learned from OOMs
        sig = _op_signature(chosen_pipeline, chosen_op_list[0])
        mult = s.op_mem_mult.get(sig, s.default_mem_mult)

        # Size request
        req_ram = _size_ram(max_ram, avail_ram, chosen_pipeline.priority, mult)
        req_cpu = _size_cpu(max_cpu, avail_cpu, chosen_pipeline.priority)

        # If we can't fit with current headroom, optionally try preemption for high-priority only.
        if (req_ram <= 0 or req_cpu <= 0) or (req_ram > avail_ram or req_cpu > avail_cpu):
            need_preempt = s.enable_preemption and chosen_pipeline.priority in (Priority.QUERY, Priority.INTERACTIVE)
            if need_preempt and s.preempt_only_if_no_fit:
                # Attempt to free resources in this pool by suspending lower-priority running containers.
                to_suspend = []
                freed_cpu = 0.0
                freed_ram = 0.0
                running = _collect_running_containers_by_priority()
                # Only consider containers in this pool and strictly lower priority
                target_rank = _prio_rank(chosen_pipeline.priority)
                for rank, pr, pid, cid, cpu, ram in running:
                    if pid != pool_id:
                        continue
                    if rank <= target_rank:
                        continue  # don't preempt same/higher priority
                    to_suspend.append((cid, pid, cpu, ram))
                    freed_cpu += cpu
                    freed_ram += ram
                    if (avail_cpu + freed_cpu) >= req_cpu and (avail_ram + freed_ram) >= req_ram:
                        break

                if to_suspend and (avail_cpu + freed_cpu) >= req_cpu and (avail_ram + freed_ram) >= req_ram:
                    # Limit churn
                    for cid, pid, _, _ in to_suspend[: s.max_preemptions_per_tick]:
                        suspensions.append(Suspend(cid, pid))
                    # After suspending, assume headroom is sufficient for this tick's assignment decision.
                    # The simulator will reflect actual resources next tick; still issue assignment now if plausible.
                    # Clamp to current available to avoid requesting impossible amounts.
                    req_cpu = min(req_cpu, max(avail_cpu, avail_cpu + freed_cpu))
                    req_ram = min(req_ram, max(avail_ram, avail_ram + freed_ram))
                else:
                    continue
            else:
                continue

        # Issue assignment
        assignments.append(
            Assignment(
                ops=chosen_op_list,
                cpu=req_cpu,
                ram=req_ram,
                priority=chosen_pipeline.priority,
                pool_id=pool_id,
                pipeline_id=chosen_pipeline.pipeline_id,
            )
        )

        # Reset batch age if we scheduled from batch
        if chosen_pipeline.priority == Priority.BATCH_PIPELINE:
            s.batch_age[chosen_pipeline.pipeline_id] = 0

        # Move the chosen pipeline to the back of its queue (round-robin within same priority)
        # This keeps fairness and avoids one pipeline monopolizing.
        if chosen_pipeline.priority == Priority.QUERY:
            if chosen_pipeline in s.q_query:
                s.q_query.remove(chosen_pipeline)
                s.q_query.append(chosen_pipeline)
        elif chosen_pipeline.priority == Priority.INTERACTIVE:
            if chosen_pipeline in s.q_interactive:
                s.q_interactive.remove(chosen_pipeline)
                s.q_interactive.append(chosen_pipeline)
        else:
            if chosen_pipeline in s.q_batch:
                s.q_batch.remove(chosen_pipeline)
                s.q_batch.append(chosen_pipeline)

    return suspensions, assignments
