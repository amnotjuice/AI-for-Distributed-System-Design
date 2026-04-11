# policy_key: scheduler_high_003
# reasoning_effort: high
# model: gpt-5.2-2025-12-11
# llm_cost: 0.120402
# generation_seconds: 136.80
# generated_at: 2026-03-12T22:28:13.858940
@register_scheduler_init(key="scheduler_high_003")
def scheduler_high_003_init(s):
    """Priority-aware, concurrency-friendly scheduler.

    Incremental improvements over naive FIFO:
      1) Separate waiting queues per priority (QUERY > INTERACTIVE > BATCH_PIPELINE).
      2) Avoid allocating the entire pool to a single operator (cap CPU/RAM per assignment),
         enabling multiple small operators to run concurrently -> better latency under contention.
      3) Basic OOM adaptation: if an operator fails with an OOM-like error, retry with more RAM.
      4) Light "reserve" behavior: when any high-priority work is waiting, prevent batch from
         consuming the last slice of a pool (no preemption; just conservative batch admission).
    """
    from collections import deque

    s.tick = 0

    # Per-priority FIFO queues of pipeline_ids
    s.q = {
        Priority.QUERY: deque(),
        Priority.INTERACTIVE: deque(),
        Priority.BATCH_PIPELINE: deque(),
    }

    # Canonical pipeline objects by id
    s.pipelines_by_id = {}

    # Enqueue tick for basic aging / debugging (not heavily used)
    s.enq_tick = {}

    # Operator-level RAM hints learned from OOMs and (lightly) decayed on success
    # Keyed by a best-effort stable op key.
    s.op_ram_request = {}  # absolute RAM request floor inferred from failures
    s.op_ram_mult = {}     # multiplicative backoff on top of a baseline

    # Operators that failed with a non-OOM error (avoid infinite retries)
    s.op_blacklist = set()

    # Tuning knobs (kept intentionally simple and conservative)
    s.max_assignments_per_pool_per_tick = 8

    # Minimum granularity we try to allocate (if the pool has less, we won't schedule)
    s.min_cpu = 0.25
    s.min_ram = 0.25

    # When high-priority work exists, keep at least this fraction of each pool free from batch.
    s.batch_reserve_frac_cpu = 0.20
    s.batch_reserve_frac_ram = 0.20

    # Per-priority target sizing as fractions of a pool, with caps to prevent monopolization.
    # These are "targets"; we always clamp to availability.
    s.size_profile = {
        Priority.QUERY: {"cpu_frac": 0.50, "cpu_cap": 8.0, "ram_frac": 0.12, "ram_cap_frac": 0.60},
        Priority.INTERACTIVE: {"cpu_frac": 0.40, "cpu_cap": 6.0, "ram_frac": 0.10, "ram_cap_frac": 0.55},
        Priority.BATCH_PIPELINE: {"cpu_frac": 0.25, "cpu_cap": 4.0, "ram_frac": 0.08, "ram_cap_frac": 0.50},
    }


@register_scheduler(key="scheduler_high_003")
def scheduler_high_003(s, results, pipelines):
    """See init() docstring for policy overview."""
    def _op_key(op):
        # Best-effort stable key across simulation run
        for attr in ("op_id", "operator_id", "id", "name"):
            v = getattr(op, attr, None)
            if v is not None:
                return (attr, v)
        # Fall back to object identity (stable within the same run)
        return ("pyid", id(op))

    def _is_oom_error(err):
        if not err:
            return False
        txt = str(err).lower()
        return ("oom" in txt) or ("out of memory" in txt) or ("memory" in txt and "alloc" in txt)

    # --- Intake new pipelines (FIFO within each priority) ---
    s.tick += 1
    for p in pipelines:
        pid = p.pipeline_id
        # Update canonical pipeline reference
        s.pipelines_by_id[pid] = p
        if pid not in s.enq_tick:
            s.enq_tick[pid] = s.tick
            # Enqueue once
            s.q[p.priority].append(pid)

    # --- Process execution results (learn from failures/successes) ---
    for r in results:
        # Results are per-container; may include multiple ops
        ops = getattr(r, "ops", None) or []
        if r.failed():
            if _is_oom_error(getattr(r, "error", None)):
                for op in ops:
                    k = _op_key(op)
                    # Exponential-ish backoff; also record absolute floor from the last attempt.
                    s.op_ram_mult[k] = max(2.0, s.op_ram_mult.get(k, 1.0) * 1.7)
                    tried_ram = getattr(r, "ram", None)
                    if tried_ram is not None:
                        s.op_ram_request[k] = max(s.op_ram_request.get(k, 0.0), float(tried_ram) * 1.8)
                    # If previously blacklisted, un-blacklist on OOM signals (OOM is retriable)
                    if k in s.op_blacklist:
                        s.op_blacklist.discard(k)
            else:
                # Non-OOM failures: do not retry those ops to avoid infinite churn.
                for op in ops:
                    s.op_blacklist.add(_op_key(op))
        else:
            # Lightly decay RAM backoff on success to recover utilization over time.
            for op in ops:
                k = _op_key(op)
                if k in s.op_ram_mult:
                    s.op_ram_mult[k] = max(1.0, s.op_ram_mult[k] * 0.92)

    # Early exit if nothing changed
    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    # A coarse signal: if any high-priority pipelines are queued, protect headroom from batch.
    high_waiting = (len(s.q[Priority.QUERY]) > 0) or (len(s.q[Priority.INTERACTIVE]) > 0)

    def _next_ready_op_from_queue(prio):
        """Pop/rotate within a priority queue until we find a pipeline with a ready op.
        Returns (pipeline, op_list) or (None, None) if none found right now.
        """
        q = s.q[prio]
        # Bound rotations so we don't loop forever on blocked DAGs
        rotations = len(q)
        while rotations > 0 and q:
            rotations -= 1
            pid = q.popleft()
            p = s.pipelines_by_id.get(pid, None)
            if p is None:
                continue

            status = p.runtime_status()
            if status.is_pipeline_successful():
                # Drop completed pipelines
                s.pipelines_by_id.pop(pid, None)
                s.enq_tick.pop(pid, None)
                continue

            # If any FAILED ops are blacklisted, drop the whole pipeline (simple, safe behavior)
            failed_ops = status.get_ops([OperatorState.FAILED], require_parents_complete=False)
            if any((_op_key(op) in s.op_blacklist) for op in failed_ops):
                s.pipelines_by_id.pop(pid, None)
                s.enq_tick.pop(pid, None)
                continue

            op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
            if not op_list:
                # Not ready yet; keep it queued
                q.append(pid)
                continue

            # If the chosen op itself is blacklisted, drop pipeline (avoids repeated selection)
            if _op_key(op_list[0]) in s.op_blacklist:
                s.pipelines_by_id.pop(pid, None)
                s.enq_tick.pop(pid, None)
                continue

            # Keep pipeline in circulation so it can make progress across many steps
            q.append(pid)
            return p, op_list

        return None, None

    # We try to fill each pool with small-ish assignments, prioritizing latency-sensitive work.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)

        if avail_cpu < s.min_cpu or avail_ram < s.min_ram:
            continue

        # Prevent a single tick from spawning too many containers per pool
        pool_assignments = 0

        # Round-robin passes: schedule at most one op per priority per pass, repeat while progress.
        while pool_assignments < s.max_assignments_per_pool_per_tick:
            made_progress = False

            for prio in (Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE):
                if pool_assignments >= s.max_assignments_per_pool_per_tick:
                    break

                # Batch admission control when high priority is waiting: keep some headroom.
                if prio == Priority.BATCH_PIPELINE and high_waiting:
                    reserve_cpu = float(pool.max_cpu_pool) * float(s.batch_reserve_frac_cpu)
                    reserve_ram = float(pool.max_ram_pool) * float(s.batch_reserve_frac_ram)
                    if avail_cpu <= max(s.min_cpu, reserve_cpu) or avail_ram <= max(s.min_ram, reserve_ram):
                        continue
                    # Clamp what batch can consume right now
                    batch_cpu_ceiling = max(s.min_cpu, avail_cpu - reserve_cpu)
                    batch_ram_ceiling = max(s.min_ram, avail_ram - reserve_ram)
                else:
                    batch_cpu_ceiling = avail_cpu
                    batch_ram_ceiling = avail_ram

                if avail_cpu < s.min_cpu or avail_ram < s.min_ram:
                    break

                p, op_list = _next_ready_op_from_queue(prio)
                if p is None:
                    continue

                op = op_list[0]
                k = _op_key(op)

                prof = s.size_profile.get(prio, s.size_profile[Priority.BATCH_PIPELINE])

                # Baseline targets
                cpu_target = min(float(prof["cpu_cap"]), float(pool.max_cpu_pool) * float(prof["cpu_frac"]))
                ram_target = float(pool.max_ram_pool) * float(prof["ram_frac"])
                ram_cap = float(pool.max_ram_pool) * float(prof["ram_cap_frac"])

                # OOM-aware scaling
                ram_mult = float(s.op_ram_mult.get(k, 1.0))
                ram_floor = float(s.op_ram_request.get(k, 0.0))
                ram_target = max(ram_target * ram_mult, ram_floor)

                # Clamp to caps and current availability (and batch ceilings if applicable)
                cpu = min(avail_cpu, batch_cpu_ceiling, max(s.min_cpu, cpu_target))
                ram = min(avail_ram, batch_ram_ceiling, max(s.min_ram, min(ram_target, ram_cap)))

                # Final feasibility check
                if cpu < s.min_cpu or ram < s.min_ram:
                    continue

                assignments.append(
                    Assignment(
                        ops=op_list,
                        cpu=cpu,
                        ram=ram,
                        priority=p.priority,
                        pool_id=pool_id,
                        pipeline_id=p.pipeline_id,
                    )
                )

                avail_cpu -= cpu
                avail_ram -= ram
                pool_assignments += 1
                made_progress = True

                if avail_cpu < s.min_cpu or avail_ram < s.min_ram:
                    break

            if not made_progress:
                break

    return suspensions, assignments
