# policy_key: scheduler_high_035
# reasoning_effort: high
# model: gpt-5.2-2025-12-11
# llm_cost: 0.100631
# generation_seconds: 142.87
# generated_at: 2026-03-14T04:02:53.967694
@register_scheduler_init(key="scheduler_high_035")
def scheduler_high_035_init(s):
    """
    Priority-aware, latency-oriented scheduler (incremental improvement over naive FIFO).

    Key ideas (kept intentionally simple):
      1) Maintain per-priority round-robin queues (QUERY > INTERACTIVE > BATCH).
      2) Avoid head-of-line blocking by NOT giving an entire pool to a single op;
         instead, size each op to a small per-priority "slice" and pack multiple ops.
      3) Prefer to keep pool 0 for latency-sensitive work when there is high-priority backlog.
      4) On failure, assume memory pressure/OOM-like behavior and increase the next RAM request
         for the failing operator (bounded by pool max), enabling fast retry convergence.

    Notes:
      - We avoid preemption here because the minimal public API in the template does not expose
        a reliable way to enumerate currently-running containers for suspension.
      - This policy is meant as a "small, obvious fixes first" step toward better latency.
    """
    # Per-priority pipeline queues (round-robin within each priority).
    s.queues = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }
    s.in_queue = set()  # pipeline_id membership guard

    # Pipeline registry so queue holds ids (stable) instead of objects.
    s.pipeline_by_id = {}

    # Operator-level resource hints learned from failures (assumed OOM-ish).
    # Keys use a best-effort "operator identity" that should be stable within a simulation.
    s.op_ram_request = {}     # op_key -> last_known_good_or_bumped_ram
    s.op_oom_attempts = {}    # op_key -> count
    s.max_oom_retries = 4

    # A simple counter used for gentle batch aging in non-reserved pools.
    s.total_assignments = 0
    s.batch_force_every = 10  # every N assignments, allow one batch pick even if high backlog


def _priority_order():
    # Highest to lowest.
    return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]


def _is_high_priority(prio):
    return prio in (Priority.QUERY, Priority.INTERACTIVE)


def _op_key(op):
    # Best-effort stable identifier for an operator object.
    # Prefer explicit ids if present, otherwise fall back to repr (deterministic in many sims).
    if hasattr(op, "op_id"):
        return ("op_id", getattr(op, "op_id"))
    if hasattr(op, "operator_id"):
        return ("operator_id", getattr(op, "operator_id"))
    if hasattr(op, "name"):
        return ("name", getattr(op, "name"))
    return ("repr", repr(op))


def _ensure_enqueued(s, p):
    pid = p.pipeline_id
    if pid in s.in_queue:
        return
    s.pipeline_by_id[pid] = p
    s.queues[p.priority].append(pid)
    s.in_queue.add(pid)


def _cleanup_pipeline_if_done(s, pid, pipeline):
    # Remove completed pipelines from bookkeeping.
    status = pipeline.runtime_status()
    if status.is_pipeline_successful():
        if pid in s.in_queue:
            s.in_queue.remove(pid)
        if pid in s.pipeline_by_id:
            del s.pipeline_by_id[pid]
        return True
    return False


def _base_cpu_cap(pool, prio):
    # Small slices for latency-sensitive work to increase concurrency and reduce HOL blocking.
    max_cpu = pool.max_cpu_pool
    if prio in (Priority.QUERY, Priority.INTERACTIVE):
        cap = int(max_cpu * 0.125)  # ~1/8 of pool
        cap = max(1, min(4, cap if cap > 0 else 1))
        return cap
    # Batch: give a bit more per op (but still not the whole pool).
    cap = int(max_cpu * 0.25)  # ~1/4 of pool
    cap = max(1, min(8, cap if cap > 0 else 1))
    return cap


def _base_ram_cap(pool, prio):
    max_ram = pool.max_ram_pool
    if prio in (Priority.QUERY, Priority.INTERACTIVE):
        return max_ram * 0.125  # ~1/8 of pool RAM
    return max_ram * 0.25      # ~1/4 of pool RAM


def _reserve_for_latency(pool, global_high_backlog):
    # When high-priority backlog exists, keep headroom so newly-arriving queries
    # are less likely to queue behind batch filling the pool.
    if not global_high_backlog:
        return 0, 0
    reserve_cpu = int(pool.max_cpu_pool * 0.15)
    reserve_cpu = max(1, reserve_cpu) if pool.max_cpu_pool > 1 else 0
    reserve_ram = pool.max_ram_pool * 0.15
    return reserve_cpu, reserve_ram


def _pick_next_op_for_pool(s, pool_id, avail_cpu, avail_ram, allow_batch, force_batch_pick):
    """
    Round-robin scan of per-priority queues; returns a feasible (pipeline, op, cpu, ram, prio)
    or (None, None, None, None, None) if nothing fits.
    """
    pool = s.executor.pools[pool_id]

    prios = _priority_order()
    if not allow_batch:
        prios = [Priority.QUERY, Priority.INTERACTIVE]

    # Optional gentle batch aging: in non-reserved pools, occasionally prefer batch.
    if force_batch_pick and allow_batch and s.queues[Priority.BATCH_PIPELINE]:
        prios = [Priority.BATCH_PIPELINE, Priority.QUERY, Priority.INTERACTIVE]

    for prio in prios:
        q = s.queues[prio]
        qlen = len(q)
        for _ in range(qlen):
            pid = q.pop(0)
            pipeline = s.pipeline_by_id.get(pid)
            if pipeline is None:
                # Stale id (should be rare).
                if pid in s.in_queue:
                    s.in_queue.remove(pid)
                continue

            # Drop successful pipelines early.
            if _cleanup_pipeline_if_done(s, pid, pipeline):
                continue

            status = pipeline.runtime_status()
            op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
            if not op_list:
                # Not ready; keep round-robin position.
                q.append(pid)
                continue

            op = op_list[0]
            ok = _op_key(op)

            # Compute requested resources (base slice, plus learned RAM bumps).
            cpu_cap = _base_cpu_cap(pool, prio)
            cpu_req = min(avail_cpu, cpu_cap)
            if cpu_req < 1:
                # Can't fit CPU now; keep position.
                q.append(pid)
                continue

            base_ram = _base_ram_cap(pool, prio)
            bumped_ram = s.op_ram_request.get(ok, 0)
            ram_req = max(base_ram, bumped_ram)

            # Must fit current availability.
            if ram_req <= 0 or ram_req > avail_ram:
                # Can't fit RAM now; keep position.
                q.append(pid)
                continue

            # Keep pipeline in queue for further ops (round-robin fairness).
            q.append(pid)
            return pipeline, [op], cpu_req, ram_req, prio

    return None, None, None, None, None


@register_scheduler(key="scheduler_high_035")
def scheduler_high_035(s, results: List[ExecutionResult], pipelines: List[Pipeline]) -> Tuple[List[Suspend], List[Assignment]]:
    """
    Priority-aware packing scheduler.

    Improvements vs the naive example:
      - Per-priority queues + round-robin pipeline selection.
      - Pack multiple ops per pool by assigning "slices" rather than monopolizing a pool.
      - Reserve pool 0 for high-priority work when there is high-priority backlog.
      - Increase RAM on operator failures to converge away from repeated OOMs.
    """
    # Ingest new pipelines.
    for p in pipelines:
        _ensure_enqueued(s, p)

    # Update operator RAM hints from results (assume failure is OOM/resource-related).
    for r in results:
        if not r.failed():
            continue

        pool = s.executor.pools[r.pool_id]
        pool_max_ram = pool.max_ram_pool

        for op in (r.ops or []):
            ok = _op_key(op)
            s.op_oom_attempts[ok] = s.op_oom_attempts.get(ok, 0) + 1

            # Exponential backoff on RAM, bounded by pool max.
            prev_req = s.op_ram_request.get(ok, 0)
            observed = r.ram if (r.ram is not None) else prev_req
            candidate = max(prev_req, observed) * 2 if max(prev_req, observed) > 0 else (pool_max_ram * 0.25)
            if candidate > pool_max_ram:
                candidate = pool_max_ram
            s.op_ram_request[ok] = candidate

            # If we keep failing at max RAM, stop increasing (but we still allow retries
            # because the simulator may have transient contention); cap attempts anyway.
            if s.op_oom_attempts[ok] > s.max_oom_retries:
                s.op_ram_request[ok] = pool_max_ram

    # Early exit if nothing to do.
    if not pipelines and not results:
        return [], []

    suspensions: List[Suspend] = []
    assignments: List[Assignment] = []

    global_high_backlog = bool(s.queues[Priority.QUERY] or s.queues[Priority.INTERACTIVE])

    # Schedule per pool, attempting to pack multiple operators.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool

        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # Pool selection / isolation:
        # - If we have high-priority backlog, keep pool 0 exclusively for high-priority
        #   work to reduce latency/tail.
        allow_batch = True
        if pool_id == 0 and global_high_backlog:
            allow_batch = False

        # When backlog exists, reserve headroom (mainly to prevent batch from filling pools).
        reserve_cpu, reserve_ram = _reserve_for_latency(pool, global_high_backlog)

        # Pack multiple ops in this pool until resources are too low or nothing fits.
        while True:
            eff_avail_cpu = avail_cpu
            eff_avail_ram = avail_ram

            # Apply reservation only when batch is allowed and high backlog exists.
            # (High-priority tasks may consume reserved headroom.)
            force_batch_pick = False
            if allow_batch and global_high_backlog:
                # If we're about to pick batch, ensure we don't eat into headroom.
                # We implement this by reducing effective availability used for feasibility.
                eff_avail_cpu = max(0, avail_cpu - reserve_cpu)
                eff_avail_ram = max(0, avail_ram - reserve_ram)

                # Gentle batch aging in non-reserved pools (never in pool 0).
                if pool_id != 0 and s.batch_force_every > 0:
                    force_batch_pick = (s.total_assignments > 0 and (s.total_assignments % s.batch_force_every == 0))

            # If reservation removes everything, we can still schedule high-priority,
            # but not batch. In that case, temporarily disallow batch for this pick.
            allow_batch_this_pick = allow_batch
            if eff_avail_cpu < 1 or eff_avail_ram <= 0:
                allow_batch_this_pick = False
                eff_avail_cpu = avail_cpu
                eff_avail_ram = avail_ram

            pipeline, ops, cpu_req, ram_req, prio = _pick_next_op_for_pool(
                s,
                pool_id=pool_id,
                avail_cpu=eff_avail_cpu,
                avail_ram=eff_avail_ram,
                allow_batch=allow_batch_this_pick,
                force_batch_pick=force_batch_pick,
            )

            if pipeline is None:
                break  # nothing feasible for this pool right now

            # Final sanity bounds.
            if cpu_req is None or ram_req is None or cpu_req < 1 or ram_req <= 0:
                break
            if cpu_req > avail_cpu or ram_req > avail_ram:
                break

            assignments.append(
                Assignment(
                    ops=ops,
                    cpu=cpu_req,
                    ram=ram_req,
                    priority=pipeline.priority,
                    pool_id=pool_id,
                    pipeline_id=pipeline.pipeline_id,
                )
            )

            s.total_assignments += 1
            avail_cpu -= cpu_req
            avail_ram -= ram_req

            if avail_cpu < 1 or avail_ram <= 0:
                break

    return suspensions, assignments
