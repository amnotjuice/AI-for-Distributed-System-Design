# policy_key: scheduler_none_048
# reasoning_effort: none
# model: gpt-5.2-2025-12-11
# llm_cost: 0.058377
# generation_seconds: 46.63
# generated_at: 2026-04-09T22:22:59.387720
@register_scheduler_init(key="scheduler_none_048")
def scheduler_none_048_init(s):
    """
    Priority-aware, OOM-resilient, low-churn scheduler.

    Core ideas:
    - Two-level selection: always try to schedule ready ops from higher priority pipelines first.
    - Conservative sizing for high-priority work: give them enough RAM headroom to avoid OOM retries.
    - Adaptive RAM on OOM: if an op/container fails with OOM, increase the RAM estimate for that pipeline.
    - Gentle fairness: small aging bonus for long-waiting pipelines so batch doesn't starve (avoids 720s penalties).
    - Minimal preemption: only preempt when high-priority is waiting and pools are fully blocked.
    """
    # Queues store pipeline objects; we keep them in a dict for dedupe + stable bookkeeping.
    s._p_by_id = {}  # pipeline_id -> Pipeline
    s._arrived_at = {}  # pipeline_id -> first seen time tick (logical)
    s._last_scheduled_at = {}  # pipeline_id -> last tick when we scheduled something
    s._tick = 0

    # RAM estimate multiplier per pipeline, increased upon OOM-ish failures.
    s._ram_mult = {}  # pipeline_id -> multiplier >= 1.0

    # Track per-priority "default" RAM fractions (of pool max) for initial sizing.
    # We bias toward completion for query/interactive to avoid 720s penalties.
    s._prio_ram_frac = {
        Priority.QUERY: 0.60,
        Priority.INTERACTIVE: 0.50,
        Priority.BATCH_PIPELINE: 0.35,
    }
    # CPU fractions (sublinear scaling + co-scheduling): avoid giving everything all CPU.
    s._prio_cpu_frac = {
        Priority.QUERY: 0.70,
        Priority.INTERACTIVE: 0.60,
        Priority.BATCH_PIPELINE: 0.45,
    }

    # Cap multipliers to prevent one pipeline from hoarding an entire pool forever.
    s._ram_mult_cap = 3.0

    # Preemption knobs: keep very conservative to reduce churn/wasted work.
    s._enable_preemption = True
    s._preempt_only_if_hp_waiting = True


def _scheduler_none_048_prio_rank(priority):
    # Lower rank value = higher priority
    if priority == Priority.QUERY:
        return 0
    if priority == Priority.INTERACTIVE:
        return 1
    return 2


def _scheduler_none_048_is_oom_error(err):
    if not err:
        return False
    try:
        msg = str(err).lower()
    except Exception:
        return False
    # Heuristics: simulator error strings often include "oom" / "out of memory"
    return ("oom" in msg) or ("out of memory" in msg) or ("memory" in msg and "alloc" in msg)


def _scheduler_none_048_pipeline_done_or_failed(pipeline):
    status = pipeline.runtime_status()
    if status.is_pipeline_successful():
        return True
    # If any failures exist, we still keep the pipeline because we can retry FAILED ops.
    # But if all remaining ops are blocked and failures persist, we'd want to keep retrying with more RAM.
    # So we do NOT drop failed pipelines here.
    return False


def _scheduler_none_048_ready_ops(pipeline):
    status = pipeline.runtime_status()
    # Allow retry of FAILED ops; require parents complete to preserve DAG semantics.
    return status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)


def _scheduler_none_048_score_pipeline(s, pipeline_id, pipeline):
    """
    Compute an ordering score; lower is better.
    Components:
      - priority rank (dominant)
      - aging: older pipelines get a small boost to prevent starvation
      - recency: deprioritize pipelines we just scheduled (reduce thrash)
    """
    pr = _scheduler_none_048_prio_rank(pipeline.priority)
    arrived = s._arrived_at.get(pipeline_id, s._tick)
    age = max(0, s._tick - arrived)

    last = s._last_scheduled_at.get(pipeline_id, -10**9)
    since_last = max(0, s._tick - last)

    # Aging helps batch eventually run; keep small so queries remain protected.
    # Recency penalty: if we just scheduled this pipeline very recently, let others in same priority class proceed.
    aging_bonus = -min(50, age) * 0.001
    recency_penalty = 0.02 if since_last <= 1 else 0.0

    return pr + recency_penalty + aging_bonus


def _scheduler_none_048_pick_pool_for_priority(s, priority):
    """
    Pick a pool that best fits high priority:
    - For queries: prefer pool with most available CPU (reduce queueing).
    - For interactive: prefer balanced headroom.
    - For batch: pack into pool with least available CPU that can still run (leave headroom elsewhere).
    """
    best_pool = None
    best_score = None
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool
        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        if priority == Priority.QUERY:
            score = (-avail_cpu, -avail_ram)
        elif priority == Priority.INTERACTIVE:
            score = (-(min(avail_cpu, pool.max_cpu_pool) / max(1e-9, pool.max_cpu_pool)),
                     -(min(avail_ram, pool.max_ram_pool) / max(1e-9, pool.max_ram_pool)))
        else:
            # Batch packing: choose pool with smaller remaining CPU to keep other pool(s) open for queries.
            score = (avail_cpu, avail_ram)

        if best_score is None or score < best_score:
            best_score = score
            best_pool = pool_id

    return best_pool


def _scheduler_none_048_compute_allocation(s, pool, pipeline):
    """
    Compute CPU/RAM allocation for an assignment:
    - Start from per-priority fraction of pool max.
    - Never exceed available pool resources.
    - Apply pipeline-specific RAM multiplier (learned from OOMs).
    - Keep a small RAM reserve in pool to avoid filling to 100% (reduces deadlock risk).
    """
    prio = pipeline.priority
    ram_frac = s._prio_ram_frac.get(prio, 0.35)
    cpu_frac = s._prio_cpu_frac.get(prio, 0.45)

    # Reserve a little headroom to reduce all-or-nothing fragmentation.
    ram_reserve = 0.05 * pool.max_ram_pool
    cpu_reserve = 0.05 * pool.max_cpu_pool

    avail_ram = max(0.0, pool.avail_ram_pool - ram_reserve)
    avail_cpu = max(0.0, pool.avail_cpu_pool - cpu_reserve)

    # Baseline request proportional to pool size; clamp to available.
    base_ram = ram_frac * pool.max_ram_pool
    base_cpu = cpu_frac * pool.max_cpu_pool

    mult = s._ram_mult.get(pipeline.pipeline_id, 1.0)
    ram_req = base_ram * mult

    # Ensure at least some minimal CPU if possible, otherwise skip scheduling.
    cpu = min(avail_cpu, base_cpu)
    ram = min(avail_ram, ram_req)

    # If we still have lots of available RAM and we're scheduling high prio, add a bit more headroom.
    if prio in (Priority.QUERY, Priority.INTERACTIVE):
        if pool.avail_ram_pool > 0.70 * pool.max_ram_pool:
            ram = min(pool.avail_ram_pool, max(ram, 0.70 * pool.max_ram_pool * mult))

    # Clamp lower bounds: if allocation becomes too tiny, we effectively can't schedule here.
    if cpu <= 0 or ram <= 0:
        return 0.0, 0.0
    return cpu, ram


@register_scheduler(key="scheduler_none_048")
def scheduler_none_048_scheduler(s, results, pipelines):
    """
    Scheduler step:
    - Ingest new pipelines.
    - Update RAM multipliers from failures (OOM -> increase).
    - Create assignments by repeatedly selecting the best (priority+aging) pipeline with ready ops,
      placing it into the best pool for its priority, and allocating resources conservatively.
    - Optional preemption: if a query/interactive is waiting and no pool has headroom, suspend
      one lowest-priority running container to free a pool slot.
    """
    s._tick += 1

    # Add new pipelines; keep stable pipeline object in dict.
    for p in pipelines:
        pid = p.pipeline_id
        s._p_by_id[pid] = p
        if pid not in s._arrived_at:
            s._arrived_at[pid] = s._tick
        if pid not in s._ram_mult:
            s._ram_mult[pid] = 1.0

    # Update RAM multipliers on failures.
    for r in results or []:
        if hasattr(r, "failed") and r.failed():
            # We retry by allowing FAILED ops; boost RAM if it looks like OOM.
            if _scheduler_none_048_is_oom_error(getattr(r, "error", None)):
                pid = getattr(r, "pipeline_id", None)
                # Some simulators don't put pipeline_id on result; fall back by scanning ops' pipeline if needed.
                # If not available, still try to boost based on container's priority? We can't map reliably then.
                if pid is not None and pid in s._ram_mult:
                    s._ram_mult[pid] = min(s._ram_mult_cap, s._ram_mult[pid] * 1.5)
            else:
                # Non-OOM failures: small bump as a hedge; avoid runaway.
                pid = getattr(r, "pipeline_id", None)
                if pid is not None and pid in s._ram_mult:
                    s._ram_mult[pid] = min(s._ram_mult_cap, s._ram_mult[pid] * 1.15)

    # Early exit if nothing changed.
    if not pipelines and not results:
        return [], []

    # Remove completed pipelines to reduce work.
    drop_pids = []
    for pid, p in s._p_by_id.items():
        if _scheduler_none_048_pipeline_done_or_failed(p):
            drop_pids.append(pid)
    for pid in drop_pids:
        s._p_by_id.pop(pid, None)

    suspensions = []
    assignments = []

    # Helper: detect if any high-priority pipeline is waiting with ready ops.
    def hp_waiting():
        for pid, p in s._p_by_id.items():
            if p.priority in (Priority.QUERY, Priority.INTERACTIVE):
                if _scheduler_none_048_ready_ops(p):
                    return True
        return False

    # Optional preemption if high-priority is waiting and all pools have no allocatable headroom.
    if s._enable_preemption and (not s._preempt_only_if_hp_waiting or hp_waiting()):
        # If there is no pool with both cpu and ram available, consider preempting.
        any_headroom = False
        for pool_id in range(s.executor.num_pools):
            pool = s.executor.pools[pool_id]
            if pool.avail_cpu_pool > 0 and pool.avail_ram_pool > 0:
                any_headroom = True
                break

        if not any_headroom:
            # Preempt a single lowest-priority running container (batch preferred).
            # We only have execution results for completed ops; assume executor tracks running containers on pool.
            # If not accessible, skip preemption rather than crashing.
            try:
                # Heuristic: choose a pool and suspend one container if pool exposes running_containers.
                chosen = None
                for pool_id in range(s.executor.num_pools):
                    pool = s.executor.pools[pool_id]
                    running = getattr(pool, "running_containers", None)
                    if not running:
                        continue
                    # running may be a dict container_id -> container-like with priority
                    for cid, c in (running.items() if hasattr(running, "items") else []):
                        cprio = getattr(c, "priority", Priority.BATCH_PIPELINE)
                        rank = _scheduler_none_048_prio_rank(cprio)
                        # prefer suspending worst (highest rank number)
                        cand = (rank, pool_id, cid)
                        if chosen is None or cand > chosen:
                            chosen = cand
                if chosen is not None:
                    _, pool_id, cid = chosen
                    suspensions.append(Suspend(cid, pool_id))
            except Exception:
                # If we can't introspect running containers, don't preempt.
                pass

    # Main assignment loop: keep making assignments while possible.
    # Limit number of assignments per tick to avoid over-scheduling.
    max_new_assignments = 8
    made_progress = True

    while made_progress and len(assignments) < max_new_assignments:
        made_progress = False

        # Build candidate list: pipelines with ready ops.
        candidates = []
        for pid, p in s._p_by_id.items():
            ops = _scheduler_none_048_ready_ops(p)
            if not ops:
                continue
            candidates.append((pid, p))

        if not candidates:
            break

        # Sort by priority + aging.
        candidates.sort(key=lambda x: _scheduler_none_048_score_pipeline(s, x[0], x[1]))

        # Take best candidate and try to place it.
        pid, pipeline = candidates[0]
        pool_id = _scheduler_none_048_pick_pool_for_priority(s, pipeline.priority)
        if pool_id is None:
            break

        pool = s.executor.pools[pool_id]
        cpu, ram = _scheduler_none_048_compute_allocation(s, pool, pipeline)
        if cpu <= 0 or ram <= 0:
            # Can't allocate on selected pool; try other pools (fallback).
            tried = {pool_id}
            placed = False
            for _ in range(s.executor.num_pools - 1):
                # Choose next best pool by brute force scan.
                alt_pool_id = None
                alt_score = None
                for pid2 in range(s.executor.num_pools):
                    if pid2 in tried:
                        continue
                    p2 = s.executor.pools[pid2]
                    if p2.avail_cpu_pool <= 0 or p2.avail_ram_pool <= 0:
                        continue
                    # Use same selection logic: for non-batch, prefer more headroom; for batch, prefer packing.
                    if pipeline.priority == Priority.BATCH_PIPELINE:
                        score = (p2.avail_cpu_pool, p2.avail_ram_pool)
                    else:
                        score = (-p2.avail_cpu_pool, -p2.avail_ram_pool)
                    if alt_score is None or score < alt_score:
                        alt_score = score
                        alt_pool_id = pid2
                if alt_pool_id is None:
                    break
                tried.add(alt_pool_id)
                p_alt = s.executor.pools[alt_pool_id]
                cpu2, ram2 = _scheduler_none_048_compute_allocation(s, p_alt, pipeline)
                if cpu2 > 0 and ram2 > 0:
                    pool_id = alt_pool_id
                    pool = p_alt
                    cpu, ram = cpu2, ram2
                    placed = True
                    break
            if not placed and (cpu <= 0 or ram <= 0):
                break

        # Assign only 1 op at a time per pipeline to reduce blast radius and improve responsiveness.
        op_list = _scheduler_none_048_ready_ops(pipeline)[:1]
        if not op_list:
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
        s._last_scheduled_at[pid] = s._tick
        made_progress = True

    return suspensions, assignments
