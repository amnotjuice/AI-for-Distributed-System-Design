# policy_key: scheduler_none_028
# reasoning_effort: none
# model: gpt-5.2-2025-12-11
# llm_cost: 0.043147
# generation_seconds: 41.11
# generated_at: 2026-03-14T02:01:47.902390
@register_scheduler_init(key="scheduler_none_028")
def scheduler_none_028_init(s):
    """Priority-aware, latency-oriented scheduler (incremental improvement over naive FIFO).

    Main ideas (kept intentionally simple and robust):
      1) Maintain separate FIFO queues per priority (QUERY > INTERACTIVE > BATCH_PIPELINE).
      2) Reserve a small fraction of each pool for high-priority work (soft reservation).
      3) When high-priority work arrives and can't fit, preempt (suspend) low-priority running containers
         in that pool until enough headroom exists.
      4) Basic OOM learning: if an op OOMs at RAM=r, retry the pipeline with a larger RAM request next time.
      5) Avoid assigning "all available resources" to a single op; cap CPU and RAM per assignment to reduce
         head-of-line blocking and improve concurrent responsiveness.

    Notes:
      - This policy only uses signals available via results + pool availability.
      - It is conservative: it never attempts to pack multiple ops into one Assignment (keeps 1-op assignments).
    """
    # Per-priority waiting queues
    s.q_query = []
    s.q_interactive = []
    s.q_batch = []

    # Track last-seen runnable pipelines to avoid duplicates
    s.enqueued = set()

    # RAM retry factor for OOMs (pipeline-level heuristic)
    s.oom_ram_multiplier = 1.5
    s.max_ram_cap_fraction = 0.90  # never request more than this fraction of pool RAM
    s.max_cpu_cap_fraction = 0.75  # never request more than this fraction of pool CPU

    # Per-pipeline RAM "hint" learned from failures; default None => use base sizing heuristic
    s.pipeline_ram_hint = {}  # pipeline_id -> ram

    # Per-pool reserved headroom fractions for top priorities (soft reservations)
    # These protect tail latency by keeping slack for bursts.
    s.reserve_query = 0.15
    s.reserve_interactive = 0.10

    # Keep a light cache of running containers observed (for preemption decisions)
    # container_id -> dict(pool_id, priority, cpu, ram)
    s.running = {}

    # Prefer placing high-priority work in emptiest pool (by RAM headroom)
    s.prefer_most_headroom = True


def _priority_rank(priority):
    # Higher number means higher priority
    if priority == Priority.QUERY:
        return 3
    if priority == Priority.INTERACTIVE:
        return 2
    return 1  # Priority.BATCH_PIPELINE and others


def _enqueue_pipeline(s, p):
    if p.pipeline_id in s.enqueued:
        return
    s.enqueued.add(p.pipeline_id)
    if p.priority == Priority.QUERY:
        s.q_query.append(p)
    elif p.priority == Priority.INTERACTIVE:
        s.q_interactive.append(p)
    else:
        s.q_batch.append(p)


def _dequeue_next_pipeline(s):
    # Strict priority order to reduce latency for interactive/query.
    if s.q_query:
        return s.q_query.pop(0)
    if s.q_interactive:
        return s.q_interactive.pop(0)
    if s.q_batch:
        return s.q_batch.pop(0)
    return None


def _requeue_pipeline_front(s, p):
    # Place back at front of its priority queue (for quick retry after preemption headroom changes).
    if p.priority == Priority.QUERY:
        s.q_query.insert(0, p)
    elif p.priority == Priority.INTERACTIVE:
        s.q_interactive.insert(0, p)
    else:
        s.q_batch.insert(0, p)


def _pool_reserved(pool, priority, s):
    # Soft reservation: higher priority can use everything; lower priority must leave some headroom.
    if priority == Priority.QUERY:
        return 0.0
    if priority == Priority.INTERACTIVE:
        # interactive should leave a bit for queries
        return max(0.0, s.reserve_query)
    # batch should leave for interactive + query
    return max(0.0, s.reserve_query + s.reserve_interactive)


def _cap_resources(pool, cpu_req, ram_req, s):
    # Cap per-assignment to avoid one op grabbing entire pool and harming latency.
    cpu_cap = max(1.0, pool.max_cpu_pool * s.max_cpu_cap_fraction)
    ram_cap = max(1.0, pool.max_ram_pool * s.max_ram_cap_fraction)
    return min(cpu_req, cpu_cap), min(ram_req, ram_cap)


def _base_request_for_pool(pool, priority):
    # Conservative baseline sizing:
    # - Queries: moderate CPU+RAM (favor quick completion, but leave room for concurrency)
    # - Interactive: slightly larger than query
    # - Batch: smaller to allow more concurrency and reduce interference
    if priority == Priority.QUERY:
        cpu = max(1.0, pool.max_cpu_pool * 0.40)
        ram = max(1.0, pool.max_ram_pool * 0.35)
    elif priority == Priority.INTERACTIVE:
        cpu = max(1.0, pool.max_cpu_pool * 0.50)
        ram = max(1.0, pool.max_ram_pool * 0.40)
    else:
        cpu = max(1.0, pool.max_cpu_pool * 0.25)
        ram = max(1.0, pool.max_ram_pool * 0.25)
    return cpu, ram


def _choose_pool_order(s):
    # Prefer pools with more available RAM headroom for high priority to reduce OOM/preemption.
    pool_ids = list(range(s.executor.num_pools))
    if not s.prefer_most_headroom:
        return pool_ids
    pool_ids.sort(key=lambda i: (s.executor.pools[i].avail_ram_pool, s.executor.pools[i].avail_cpu_pool), reverse=True)
    return pool_ids


def _collect_preemptable(s, pool_id, min_rank):
    # Return list of running containers in pool that are lower priority than min_rank.
    victims = []
    for cid, info in s.running.items():
        if info["pool_id"] != pool_id:
            continue
        if _priority_rank(info["priority"]) < min_rank:
            victims.append((cid, info))
    # Preempt lowest priority first; within same priority, preempt biggest RAM first to free headroom quickly.
    victims.sort(key=lambda x: (_priority_rank(x[1]["priority"]), -x[1]["ram"]))
    return victims


@register_scheduler(key="scheduler_none_028")
def scheduler_none_028(s, results, pipelines):
    """
    Priority-aware scheduler with soft reservations + targeted preemption + basic OOM backoff.

    Behavior:
      - Enqueue new pipelines into per-priority FIFO queues.
      - Process results:
          * If a container finished/failed, update running-cache.
          * If OOM-like failure, increase pipeline RAM hint for future retries.
      - For each pool (preferring those with headroom):
          * Try to schedule one runnable op from the highest-priority pipeline.
          * Enforce soft reservations for lower priorities.
          * If top-priority can't fit, preempt lower-priority running containers in that pool.

    Returns:
      - suspensions: list of Suspend actions
      - assignments: list of Assignment actions (1 op per assignment)
    """
    # Add new arrivals
    for p in pipelines:
        _enqueue_pipeline(s, p)

    # Process results to learn and to keep running cache fresh
    for r in results:
        # If we see a result for a container, it is no longer running afterwards (completed/failed/suspended outcome)
        if getattr(r, "container_id", None) is not None and r.container_id in s.running:
            del s.running[r.container_id]

        # Learn from failures: if it looks like OOM, increase RAM hint for that pipeline
        if hasattr(r, "failed") and r.failed():
            err = str(getattr(r, "error", "") or "").lower()
            is_oom = ("oom" in err) or ("out of memory" in err) or ("memory" in err and "kill" in err)
            if is_oom:
                pid = getattr(r, "pipeline_id", None)
                # pipeline_id might not be on result; fall back to None (no update)
                if pid is not None:
                    prev = s.pipeline_ram_hint.get(pid, None)
                    base = float(getattr(r, "ram", 0.0) or 0.0)
                    if base <= 0.0:
                        # if unknown, choose a conservative bump later using pool-based sizing
                        continue
                    bumped = base * s.oom_ram_multiplier
                    if prev is None:
                        s.pipeline_ram_hint[pid] = bumped
                    else:
                        s.pipeline_ram_hint[pid] = max(prev, bumped)

    # Early exit if nothing changed
    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    # Helper to drop completed/failed pipelines from queue and allow re-enqueue if needed
    def pipeline_is_droppable(p):
        st = p.runtime_status()
        # If successful, drop. If it has failures, drop (simple policy: do not retry non-OOM failures here).
        has_failures = st.state_counts[OperatorState.FAILED] > 0
        return st.is_pipeline_successful() or has_failures

    # Scheduling pass: attempt up to one assignment per pool per tick (simple, reduces churn)
    pool_order = _choose_pool_order(s)
    for pool_id in pool_order:
        pool = s.executor.pools[pool_id]
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)
        if avail_cpu <= 0.0 or avail_ram <= 0.0:
            continue

        # Try multiple pipelines until we either schedule one assignment or run out
        tried = 0
        max_tries = len(s.q_query) + len(s.q_interactive) + len(s.q_batch)
        while tried < max_tries:
            tried += 1
            p = _dequeue_next_pipeline(s)
            if p is None:
                break

            # Ensure we don't keep dead pipelines around
            if pipeline_is_droppable(p):
                s.enqueued.discard(p.pipeline_id)
                continue

            st = p.runtime_status()
            op_list = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
            if not op_list:
                # Not runnable now; put it back and try next pipeline (avoid HOL blocking)
                _enqueue_pipeline(s, p)
                continue

            # Determine requested resources
            base_cpu, base_ram = _base_request_for_pool(pool, p.priority)
            hinted_ram = s.pipeline_ram_hint.get(p.pipeline_id, None)
            cpu_req = base_cpu
            ram_req = hinted_ram if hinted_ram is not None else base_ram
            cpu_req, ram_req = _cap_resources(pool, cpu_req, ram_req, s)

            # Enforce soft reservation for lower priorities
            reserved_frac = _pool_reserved(pool, p.priority, s)
            effective_avail_cpu = max(0.0, avail_cpu - pool.max_cpu_pool * reserved_frac)
            effective_avail_ram = max(0.0, avail_ram - pool.max_ram_pool * reserved_frac)

            can_fit = (cpu_req <= effective_avail_cpu) and (ram_req <= effective_avail_ram)

            # If can't fit and this is high priority, attempt preemption in this pool
            if not can_fit and _priority_rank(p.priority) >= _priority_rank(Priority.INTERACTIVE):
                # Preempt lower-priority work until it fits (or no victims)
                need_cpu = max(0.0, cpu_req - avail_cpu)
                need_ram = max(0.0, ram_req - avail_ram)

                victims = _collect_preemptable(s, pool_id, _priority_rank(p.priority))
                freed_cpu = 0.0
                freed_ram = 0.0
                for cid, info in victims:
                    # Suspend victim
                    suspensions.append(Suspend(container_id=cid, pool_id=pool_id))
                    freed_cpu += float(info["cpu"])
                    freed_ram += float(info["ram"])
                    # Remove from running cache immediately (optimistic)
                    if cid in s.running:
                        del s.running[cid]
                    if (freed_cpu >= need_cpu) and (freed_ram >= need_ram):
                        break

                # Update local availability estimates optimistically
                avail_cpu += freed_cpu
                avail_ram += freed_ram
                effective_avail_cpu = max(0.0, avail_cpu - pool.max_cpu_pool * reserved_frac)
                effective_avail_ram = max(0.0, avail_ram - pool.max_ram_pool * reserved_frac)
                can_fit = (cpu_req <= effective_avail_cpu) and (ram_req <= effective_avail_ram)

            if not can_fit:
                # Couldn't schedule in this pool; requeue and stop trying in this pool to avoid churn
                _requeue_pipeline_front(s, p)
                break

            # Place assignment
            assignment = Assignment(
                ops=op_list,
                cpu=min(cpu_req, avail_cpu),
                ram=min(ram_req, avail_ram),
                priority=p.priority,
                pool_id=pool_id,
                pipeline_id=p.pipeline_id,
            )
            assignments.append(assignment)

            # Optimistically track as running; container_id will be known later, but we can still track intent.
            # We'll update/removal based on results when they come in.
            # Use a synthetic key to avoid collision; if container_id is required, results will correct state anyway.
            synthetic_cid = f"pending:{p.pipeline_id}:{pool_id}:{len(assignments)}"
            s.running[synthetic_cid] = {
                "pool_id": pool_id,
                "priority": p.priority,
                "cpu": float(assignment.cpu),
                "ram": float(assignment.ram),
            }

            # Re-enqueue pipeline for its subsequent operators
            _enqueue_pipeline(s, p)

            # Only one assignment per pool per tick (simple, reduces contention and improves responsiveness)
            break

    return suspensions, assignments
