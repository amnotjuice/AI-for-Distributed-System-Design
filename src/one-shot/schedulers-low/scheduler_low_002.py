# policy_key: scheduler_low_002
# reasoning_effort: low
# model: gpt-5.2-2025-12-11
# llm_cost: 0.000000
# generation_seconds: 50.38
# generated_at: 2026-03-12T21:35:22.478276
@register_scheduler_init(key="scheduler_low_002")
def scheduler_low_002_init(s):
    """Priority-aware, latency-biased scheduler (small incremental improvement over naive FIFO).

    Main changes vs naive:
      - Maintain separate FIFO queues per priority and always schedule higher priority first.
      - Basic admission control via "reservation": keep some headroom in each pool for high-priority work.
      - Allow multiple assignments per pool per tick (instead of at most 1).
      - Simple OOM retry: if an op fails with an OOM-like error, retry it with increased RAM next time.
      - Starvation guard: periodically allow a batch op through even if interactive traffic exists.
    """
    # Per-priority FIFO queues of pipelines
    s.waiting_by_prio = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }

    # Track RAM boost factors for pipelines that experienced OOM-like failures.
    # Keyed by pipeline_id; value is multiplicative boost applied to baseline RAM request.
    s.pipeline_ram_boost = {}

    # Track failure counts to avoid infinite retry loops on persistent failures.
    s.pipeline_fail_count = {}

    # Starvation guard: every N high-priority dispatches, allow one batch dispatch.
    s.hp_dispatch_counter = 0
    s.batch_every_n_hp = 8

    # Round-robin pointer across pools to avoid always filling pool 0 first (minor fairness).
    s.pool_rr = 0


def _is_oom_error(err) -> bool:
    try:
        if err is None:
            return False
        msg = str(err).lower()
        return ("oom" in msg) or ("out of memory" in msg) or ("cuda out of memory" in msg)
    except Exception:
        return False


def _enqueue_pipeline(s, p):
    # Ensure dict entries exist even if new priority values appear (defensive).
    if p.priority not in s.waiting_by_prio:
        s.waiting_by_prio[p.priority] = []
    s.waiting_by_prio[p.priority].append(p)


def _dequeue_next_pipeline(s, allow_batch: bool):
    """Pick next pipeline to schedule, honoring priority and starvation guard."""
    # Highest priority first: QUERY > INTERACTIVE > BATCH_PIPELINE
    # (If your enum order differs, this explicit ordering keeps behavior stable.)
    prio_order = [Priority.QUERY, Priority.INTERACTIVE]
    if allow_batch:
        prio_order.append(Priority.BATCH_PIPELINE)

    for pr in prio_order:
        q = s.waiting_by_prio.get(pr, [])
        while q:
            p = q.pop(0)
            status = p.runtime_status()

            # Drop completed pipelines
            if status.is_pipeline_successful():
                continue

            # Drop pipelines with too many failures (prevent endless churn)
            pid = p.pipeline_id
            if s.pipeline_fail_count.get(pid, 0) >= 6:
                continue

            return p

    return None


def _baseline_share(priority):
    """Return baseline CPU/RAM share of a pool to allocate per op, before OOM boosts."""
    # Bias latency: give high-priority more per-op resources to finish faster.
    if priority in (Priority.QUERY, Priority.INTERACTIVE):
        return 0.50  # up to half a pool for a single interactive op
    return 0.25      # batch gets smaller slices to improve concurrency


def _reserved_headroom_fraction():
    """Fraction of each pool reserved (kept free) for high-priority work."""
    # Small, obvious improvement: prevent batch from saturating the pool so interactive can start quickly.
    return 0.20


@register_scheduler(key="scheduler_low_002")
def scheduler_low_002_scheduler(s, results, pipelines):
    """
    Priority-aware scheduler with reservations + OOM-aware RAM backoff.

    Decisions:
      - Admission/ordering: strict priority with FIFO within each class, plus a light starvation guard.
      - Placement: fill pools in round-robin; no cross-pool affinity yet.
      - Sizing: allocate a per-op slice of pool resources; boost RAM on observed OOM failures.
      - Preemption: not used here (kept simple; API visibility into running set may vary).
    """
    # Enqueue newly arrived pipelines
    for p in pipelines:
        _enqueue_pipeline(s, p)

    # Process execution results to learn from failures (especially OOM)
    for r in results:
        try:
            pid = getattr(r, "pipeline_id", None)
        except Exception:
            pid = None

        # We can still infer the pipeline_id from ops in some implementations; keep this conservative.
        # If pipeline_id isn't available on result, we won't track boost for it.
        if pid is None:
            continue

        if r.failed():
            s.pipeline_fail_count[pid] = s.pipeline_fail_count.get(pid, 0) + 1
            if _is_oom_error(getattr(r, "error", None)):
                # Increase RAM boost multiplicatively; cap growth to avoid instantly monopolizing a pool.
                prev = s.pipeline_ram_boost.get(pid, 1.0)
                s.pipeline_ram_boost[pid] = min(prev * 2.0, 8.0)

    # If nothing changed, early exit
    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    num_pools = s.executor.num_pools
    if num_pools <= 0:
        return suspensions, assignments

    # Try to place work in round-robin across pools each tick
    start_pool = s.pool_rr % num_pools
    s.pool_rr = (s.pool_rr + 1) % num_pools

    # We'll temporarily hold pipelines we popped but couldn't schedule this tick
    deferred = {Priority.QUERY: [], Priority.INTERACTIVE: [], Priority.BATCH_PIPELINE: []}

    # Iterate pools in RR order
    for k in range(num_pools):
        pool_id = (start_pool + k) % num_pools
        pool = s.executor.pools[pool_id]

        # Snapshot available resources
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)
        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # Reservation: keep headroom for high priority by not letting low priority consume the last slice
        reserve_frac = _reserved_headroom_fraction()
        reserve_cpu = float(pool.max_cpu_pool) * reserve_frac
        reserve_ram = float(pool.max_ram_pool) * reserve_frac

        # Allow multiple assignments per pool to improve utilization
        # Stop when remaining resources are too low to be useful.
        max_assignments_this_pool = 8
        made = 0

        while made < max_assignments_this_pool and avail_cpu > 0 and avail_ram > 0:
            # Starvation guard: occasionally allow a batch op even under continuous HP load.
            allow_batch = True
            if s.hp_dispatch_counter % (s.batch_every_n_hp + 1) != s.batch_every_n_hp:
                # Prefer HP most of the time; batch allowed only when no HP is available
                allow_batch = True

            # Decide whether we can schedule batch given reservations:
            # If we have interactive/query queued, disallow batch when we'd dip into reserved headroom.
            hp_queued = bool(s.waiting_by_prio.get(Priority.QUERY)) or bool(s.waiting_by_prio.get(Priority.INTERACTIVE))
            if hp_queued:
                # If we'd fall below reserved headroom, don't schedule batch.
                batch_ok = (avail_cpu - reserve_cpu) > 0 and (avail_ram - reserve_ram) > 0
            else:
                batch_ok = True

            # For picking pipeline: if batch isn't OK, temporarily disallow it.
            p = _dequeue_next_pipeline(s, allow_batch=batch_ok)
            if p is None:
                break

            status = p.runtime_status()

            # Get one ready op whose parents are complete
            op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
            if not op_list:
                # Not runnable yet; defer to preserve FIFO-ish behavior
                deferred.setdefault(p.priority, []).append(p)
                continue

            # Determine baseline slice; cap to remaining availability
            share = _baseline_share(p.priority)
            target_cpu = min(avail_cpu, max(1.0, float(pool.max_cpu_pool) * share))
            target_ram = min(avail_ram, max(1.0, float(pool.max_ram_pool) * share))

            # Apply OOM-derived RAM boost (pipeline-level)
            pid = p.pipeline_id
            boost = float(s.pipeline_ram_boost.get(pid, 1.0))
            target_ram = min(avail_ram, target_ram * boost)

            # Avoid assigning batch if it would violate reserved headroom while HP queued
            if p.priority == Priority.BATCH_PIPELINE and hp_queued:
                if (avail_cpu - target_cpu) < reserve_cpu or (avail_ram - target_ram) < reserve_ram:
                    # Can't place this batch op now; defer and stop trying batch in this pool iteration
                    deferred.setdefault(p.priority, []).append(p)
                    # Try again for HP work only
                    p2 = _dequeue_next_pipeline(s, allow_batch=False)
                    if p2 is None:
                        break
                    status2 = p2.runtime_status()
                    op_list2 = status2.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
                    if not op_list2:
                        deferred.setdefault(p2.priority, []).append(p2)
                        continue
                    share2 = _baseline_share(p2.priority)
                    target_cpu = min(avail_cpu, max(1.0, float(pool.max_cpu_pool) * share2))
                    target_ram = min(avail_ram, max(1.0, float(pool.max_ram_pool) * share2))
                    pid2 = p2.pipeline_id
                    boost2 = float(s.pipeline_ram_boost.get(pid2, 1.0))
                    target_ram = min(avail_ram, target_ram * boost2)
                    p = p2
                    op_list = op_list2
                    pid = pid2

            # Final feasibility check
            if target_cpu <= 0 or target_ram <= 0:
                deferred.setdefault(p.priority, []).append(p)
                break

            assignments.append(
                Assignment(
                    ops=op_list,
                    cpu=target_cpu,
                    ram=target_ram,
                    priority=p.priority,
                    pool_id=pool_id,
                    pipeline_id=p.pipeline_id,
                )
            )

            # Update accounting
            avail_cpu -= target_cpu
            avail_ram -= target_ram
            made += 1

            # Update starvation guard counter
            if p.priority in (Priority.QUERY, Priority.INTERACTIVE):
                s.hp_dispatch_counter += 1

            # Re-enqueue pipeline to continue later (other ops may become ready after this op completes)
            _enqueue_pipeline(s, p)

    # Put deferred pipelines back at the front (to preserve rough FIFO within priority)
    for pr, items in deferred.items():
        if not items:
            continue
        # Prepend by rebuilding queue: deferred first, then existing queue
        existing = s.waiting_by_prio.get(pr, [])
        s.waiting_by_prio[pr] = items + existing

    return suspensions, assignments
