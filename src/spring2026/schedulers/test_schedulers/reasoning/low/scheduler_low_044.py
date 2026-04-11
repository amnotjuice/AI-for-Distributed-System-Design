# policy_key: scheduler_low_044
# reasoning_effort: low
# model: gpt-5.2-2025-12-11
# llm_cost: 0.054079
# generation_seconds: 52.94
# generated_at: 2026-04-09T21:38:00.485948
@register_scheduler_init(key="scheduler_low_044")
def scheduler_low_044_init(s):
    """
    Priority + aging + failure-aware right-sizing scheduler.

    Goals:
      - Minimize weighted latency (query > interactive > batch) without starving batch.
      - Reduce 720s penalties by aggressively retrying failures with higher RAM.
      - Improve throughput over naive FIFO by packing multiple operators per pool tick.

    Core ideas:
      - Maintain per-priority FIFO queues of pipelines, but select next pipeline via
        (base_priority + aging) to prevent starvation.
      - Size resources by priority fractions of pool capacity, then apply per-pipeline
        "required minimums" learned from past failures (e.g., OOM -> increase RAM).
      - Never drop pipelines; keep retrying with exponential RAM backoff (capped).
    """
    from collections import deque

    s.time = 0

    # Separate waiting queues by priority
    s.q_query = deque()
    s.q_interactive = deque()
    s.q_batch = deque()

    # Track which pipelines are currently enqueued (avoid duplicates)
    s.enqueued = set()

    # Enqueue time for aging (pipeline_id -> time)
    s.enqueue_time = {}

    # Failure-aware sizing hints (pipeline_id -> required min resources)
    s.req_ram = {}   # absolute RAM amount to request (best-effort)
    s.req_cpu = {}   # absolute CPU amount to request (best-effort)

    # Retry counts (pipeline_id -> int). We do not cap retries, but it is useful for tuning.
    s.retries = {}


def _q_for_priority(s, prio):
    if prio == Priority.QUERY:
        return s.q_query
    if prio == Priority.INTERACTIVE:
        return s.q_interactive
    return s.q_batch


def _base_score(prio):
    # Larger means "more urgent" for selection.
    if prio == Priority.QUERY:
        return 100.0
    if prio == Priority.INTERACTIVE:
        return 50.0
    return 10.0


def _target_fracs(prio):
    # Fractions of pool max resources to request per assignment attempt.
    # Queries get more to reduce tail latency; batch gets less to improve concurrency.
    if prio == Priority.QUERY:
        return 0.75, 0.75
    if prio == Priority.INTERACTIVE:
        return 0.55, 0.55
    return 0.30, 0.35


def _choose_pool_for_request(s, need_cpu, need_ram, prio):
    """
    Choose a pool that can satisfy (need_cpu, need_ram).
    Prefer the pool with the largest feasible headroom ratio.
    """
    best_pool = None
    best_score = -1.0

    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool
        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # Hard feasibility check
        if avail_cpu + 1e-12 < need_cpu or avail_ram + 1e-12 < need_ram:
            continue

        # Favor pools that keep more spare capacity after placement
        # (to reduce future head-of-line blocking for high priority).
        cpu_ratio = (avail_cpu - need_cpu) / (pool.max_cpu_pool + 1e-12)
        ram_ratio = (avail_ram - need_ram) / (pool.max_ram_pool + 1e-12)

        # Bias queries to CPU headroom; batch to RAM headroom (slight heuristic)
        if prio == Priority.QUERY:
            score = 0.7 * cpu_ratio + 0.3 * ram_ratio
        elif prio == Priority.INTERACTIVE:
            score = 0.5 * cpu_ratio + 0.5 * ram_ratio
        else:
            score = 0.3 * cpu_ratio + 0.7 * ram_ratio

        if score > best_score:
            best_score = score
            best_pool = pool_id

    return best_pool


def _prune_finished_and_failed(s, pipeline):
    """
    Return True if the pipeline should be removed from consideration (done or terminal fail).
    We never treat failures as terminal here; we keep retrying to avoid 720s penalties.
    """
    status = pipeline.runtime_status()
    if status.is_pipeline_successful():
        return True
    # If the simulator marks a pipeline as irrecoverably failed, it may still show FAILED ops.
    # We do not have a reliable terminal-failure signal other than "successful", so keep it.
    return False


def _pop_best_pipeline(s):
    """
    Choose next pipeline across queues using (base_priority + aging).
    Pops and returns one pipeline, or None.
    """
    best = None
    best_queue = None
    best_val = -1.0

    # Consider the head of each queue (keeps per-priority FIFO behavior).
    for q in (s.q_query, s.q_interactive, s.q_batch):
        if not q:
            continue
        p = q[0]
        enq_t = s.enqueue_time.get(p.pipeline_id, s.time)
        age = max(0, s.time - enq_t)

        val = _base_score(p.priority) + 0.12 * age  # aging prevents starvation
        if val > best_val:
            best_val = val
            best = p
            best_queue = q

    if best is None:
        return None

    best_queue.popleft()
    s.enqueued.discard(best.pipeline_id)
    # Keep enqueue_time around; if we re-enqueue, we reset to current time.
    return best


def _enqueue_pipeline(s, pipeline, front=False):
    """
    Enqueue pipeline once. Optionally push to front (used after failure to retry sooner).
    """
    pid = pipeline.pipeline_id
    if pid in s.enqueued:
        return
    q = _q_for_priority(s, pipeline.priority)
    if front:
        q.appendleft(pipeline)
    else:
        q.append(pipeline)
    s.enqueued.add(pid)
    s.enqueue_time[pid] = s.time


@register_scheduler(key="scheduler_low_044")
def scheduler_low_044(s, results, pipelines):
    """
    Scheduler step:
      1) Incorporate new arrivals into priority queues.
      2) Learn from execution results:
           - On failure: increase required RAM (exponential backoff) and re-enqueue quickly.
      3) Pack assignments across pools while resources remain:
           - Pick next pipeline by (priority + aging).
           - Assign one ready operator at a time.
           - Size requests by priority fractions, then apply learned per-pipeline minima.
    """
    s.time += 1

    # 1) New arrivals
    for p in pipelines:
        _enqueue_pipeline(s, p, front=False)

    # 2) Learn from results (failure-aware resizing)
    #    Also re-enqueue pipelines that still have work, especially on failure.
    for r in results:
        # We only get pool_id, cpu, ram, ops, error, etc. Use these as signals.
        if r.failed():
            # Exponential RAM backoff to avoid repeated OOM-like failures.
            # Use observed RAM request as baseline; double it.
            prev_req = s.req_ram.get(r.pipeline_id, 0.0)
            next_req = max(prev_req, float(getattr(r, "ram", 0.0)) * 2.0)

            # If we can see the pool, cap at its max.
            try:
                pool = s.executor.pools[r.pool_id]
                next_req = min(next_req, pool.max_ram_pool)
            except Exception:
                pass

            s.req_ram[r.pipeline_id] = next_req
            s.retries[r.pipeline_id] = s.retries.get(r.pipeline_id, 0) + 1

        # Re-enqueue the pipeline to continue progress if it's known to us.
        # We cannot reliably reconstruct pipeline objects from results, so we only
        # re-enqueue via the waiting queues when pipelines are already present or arrive.
        # (The simulator typically keeps pipeline objects alive and may reappear in pipelines.)

    # Early exit if nothing to do
    if not pipelines and not results and not (s.q_query or s.q_interactive or s.q_batch):
        return [], []

    suspensions = []
    assignments = []

    # 3) Create assignments by packing across pools
    # Track pool availability locally so we can produce multiple assignments per pool in one tick.
    avail_cpu = []
    avail_ram = []
    max_cpu = []
    max_ram = []
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu.append(pool.avail_cpu_pool)
        avail_ram.append(pool.avail_ram_pool)
        max_cpu.append(pool.max_cpu_pool)
        max_ram.append(pool.max_ram_pool)

    # Safety: bound loop iterations (prevents pathological infinite loops)
    max_total_assignments = 4 * max(1, s.executor.num_pools) * 8
    made_progress = True
    iters = 0

    while made_progress and iters < max_total_assignments:
        iters += 1
        made_progress = False

        pipeline = _pop_best_pipeline(s)
        if pipeline is None:
            break

        # Drop completed pipelines, keep everything else.
        if _prune_finished_and_failed(s, pipeline):
            continue

        status = pipeline.runtime_status()

        # One ready op at a time (reduces risk of over-admission and helps latency)
        op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
        if not op_list:
            # Not ready yet; re-enqueue with aging preserved (use same enqueue_time)
            _enqueue_pipeline(s, pipeline, front=False)
            continue

        prio = pipeline.priority
        cpu_frac, ram_frac = _target_fracs(prio)

        # Choose a candidate pool by trying feasibility across pools.
        # Compute a per-pool request and take the first feasible by best headroom.
        # We incorporate per-pipeline learned RAM/CPU minima.
        pid = pipeline.pipeline_id
        learned_ram = float(s.req_ram.get(pid, 0.0))
        learned_cpu = float(s.req_cpu.get(pid, 0.0))

        # Determine "typical" request sizes based on a representative pool maximum.
        # We'll later clamp to the chosen pool's current availability.
        # Use the largest pool max as a stable sizing basis.
        rep_max_cpu = max(max_cpu) if max_cpu else 0.0
        rep_max_ram = max(max_ram) if max_ram else 0.0

        base_need_cpu = max(learned_cpu, cpu_frac * rep_max_cpu)
        base_need_ram = max(learned_ram, ram_frac * rep_max_ram)

        # Ensure strictly positive requests when resources exist.
        base_need_cpu = max(base_need_cpu, 0.01)
        base_need_ram = max(base_need_ram, 0.01)

        pool_id = _choose_pool_for_request(s, base_need_cpu, base_need_ram, prio)

        # If no pool can satisfy the "ideal" request, degrade gracefully:
        # try smaller CPU first (RAM is more failure-critical), then smaller RAM.
        if pool_id is None:
            # Try RAM-first feasibility: keep RAM at learned/base and reduce CPU.
            reduced_cpu = max(0.01, min(base_need_cpu, 0.35 * rep_max_cpu))
            pool_id = _choose_pool_for_request(s, reduced_cpu, base_need_ram, prio)
            if pool_id is not None:
                base_need_cpu = reduced_cpu

        if pool_id is None:
            # As a last resort, try to fit into any pool with any non-zero slice.
            # This helps avoid deadlock when pools are fragmented.
            for pid_pool in range(s.executor.num_pools):
                if avail_cpu[pid_pool] > 0 and avail_ram[pid_pool] > 0:
                    pool_id = pid_pool
                    break

        if pool_id is None:
            # No resources anywhere: re-enqueue and stop.
            _enqueue_pipeline(s, pipeline, front=True)
            break

        # Clamp to chosen pool's current availability and maximums.
        need_cpu = min(base_need_cpu, avail_cpu[pool_id], max_cpu[pool_id])
        need_ram = min(base_need_ram, avail_ram[pool_id], max_ram[pool_id])

        # If still not feasible (due to clamping to tiny leftovers), re-enqueue.
        if need_cpu <= 0 or need_ram <= 0:
            _enqueue_pipeline(s, pipeline, front=True)
            continue

        # Make the assignment
        assignments.append(
            Assignment(
                ops=op_list,
                cpu=need_cpu,
                ram=need_ram,
                priority=prio,
                pool_id=pool_id,
                pipeline_id=pipeline.pipeline_id,
            )
        )

        # Update local availability (packing multiple assignments per tick)
        avail_cpu[pool_id] -= need_cpu
        avail_ram[pool_id] -= need_ram
        made_progress = True

        # Re-enqueue pipeline if it likely has more work.
        # Put queries/interactives slightly closer to front to reduce end-to-end latency.
        _enqueue_pipeline(s, pipeline, front=(prio != Priority.BATCH_PIPELINE))

    return suspensions, assignments
