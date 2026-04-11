# policy_key: scheduler_medium_041
# reasoning_effort: medium
# exp: reasoning
# model: gpt-5.2-2025-12-11
# llm_cost: 0.080259
# generation_seconds: 64.34
# generated_at: 2026-04-09T23:25:06.407324
@register_scheduler_init(key="scheduler_medium_041")
def scheduler_medium_041_init(s):
    """
    Priority-weighted fair scheduler with OOM-aware retries.

    Main ideas:
      - Weighted fair queueing across priority classes (query:10, interactive:5, batch:1) to
        protect high-priority latency while avoiding starvation.
      - Conservative initial sizing (RAM/CPU shares) to improve concurrency vs. "take all resources".
      - Adaptive per-operator RAM hints: on OOM-like failures, retry the same operator with more RAM
        (exponential backoff up to pool max) to drive completion rate up and avoid 720s penalties.
      - Avoid dropping pipelines just because an operator failed; treat FAILED ops as retryable.
    """
    # Per-priority FIFO queues of pipelines
    s.queues = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }

    # Token-bucket for weighted fair sharing across priorities
    s.tokens = {
        Priority.QUERY: 0.0,
        Priority.INTERACTIVE: 0.0,
        Priority.BATCH_PIPELINE: 0.0,
    }
    s.token_cap_mult = 3.0  # cap tokens to a few "ticks" worth to prevent runaway bursts

    # Per (pipeline_id, op_key) state for retries and sizing hints
    s.op_attempts = {}          # (pipeline_id, op_key) -> int
    s.op_ram_hint = {}          # (pipeline_id, op_key) -> float
    s.op_cpu_hint = {}          # (pipeline_id, op_key) -> float
    s.poison_ops = set()        # ops that repeatedly fail with non-OOM errors; deprioritize

    # Assignment throttles to avoid monopolizing a pool in a single scheduler step
    s.max_assignments_per_pool = 4

    # Small floors so we don't request 0
    s.min_cpu_floor = 0.25
    s.min_ram_floor = 0.25


def _prio_weight(p):
    if p == Priority.QUERY:
        return 10.0
    if p == Priority.INTERACTIVE:
        return 5.0
    return 1.0


def _is_oom_error(err) -> bool:
    if not err:
        return False
    msg = str(err).lower()
    return ("oom" in msg) or ("out of memory" in msg) or ("memory" in msg and "alloc" in msg)


def _op_key(op) -> int:
    # Use object identity as a stable key within a simulation run
    return id(op)


def _base_share(priority):
    # Conservative default shares to increase concurrency while still giving high priority more
    if priority == Priority.QUERY:
        return 0.45, 0.50  # (cpu_share, ram_share)
    if priority == Priority.INTERACTIVE:
        return 0.35, 0.40
    return 0.25, 0.30


def _clamp(x, lo, hi):
    if x < lo:
        return lo
    if x > hi:
        return hi
    return x


def _choose_next_priority_with_tokens(s):
    # Highest urgency first, but only if it has tokens and work
    for pr in (Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE):
        if s.tokens.get(pr, 0.0) >= 1.0 and s.queues[pr]:
            return pr
    return None


def _get_ready_op(pipeline):
    status = pipeline.runtime_status()
    # Get a single ready op whose parents are complete; retry FAILED ops too
    ops = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
    return ops[0] if ops else None


@register_scheduler(key="scheduler_medium_041")
def scheduler_medium_041(s, results, pipelines):
    """
    Scheduler step:
      1) Ingest new pipelines into per-priority queues.
      2) Process execution results: update retry counts; on OOM, increase RAM hint for that op.
      3) Refill tokens by priority weights.
      4) For each pool, schedule up to N assignments using weighted fair selection and
         OOM-aware sizing hints.
    """
    # Enqueue new arrivals
    for p in pipelines:
        s.queues[p.priority].append(p)

    # Process results to learn per-op sizing; do not drop pipelines on failures
    for r in results:
        if not getattr(r, "ops", None):
            continue
        # We assign one op per container in this policy, but handle list safely
        for op in r.ops:
            k = (getattr(r, "pipeline_id", None), _op_key(op))
            # If pipeline_id is not carried on result, fall back to op identity alone
            if k[0] is None:
                k = ("_unknown_pipeline_", _op_key(op))

            if r.failed():
                prev = s.op_attempts.get(k, 0)
                s.op_attempts[k] = prev + 1

                if _is_oom_error(getattr(r, "error", None)):
                    # Increase RAM aggressively to avoid repeated OOMs (which create huge penalties)
                    cur = float(getattr(r, "ram", 0.0) or 0.0)
                    # If reported RAM is missing/0, start from a small baseline
                    if cur <= 0:
                        cur = 1.0
                    bumped = max(cur * 1.7, cur + 1.0)
                    # Cap to pool max when we schedule (we don't know it here), but store bumped hint
                    s.op_ram_hint[k] = max(s.op_ram_hint.get(k, 0.0), bumped)
                else:
                    # Non-OOM errors may be non-retriable; after a couple tries, deprioritize
                    if s.op_attempts[k] >= 3:
                        s.poison_ops.add(k)

    # Refill priority tokens
    # We refill by weights each tick; cap to prevent unlimited burstiness.
    # Cap scaled by total weight so bursts are proportional.
    total_weight = _prio_weight(Priority.QUERY) + _prio_weight(Priority.INTERACTIVE) + _prio_weight(Priority.BATCH_PIPELINE)
    cap_base = total_weight * s.token_cap_mult
    for pr in (Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE):
        s.tokens[pr] = _clamp(s.tokens.get(pr, 0.0) + _prio_weight(pr), 0.0, cap_base)

    # Early exit if nothing changed
    if not pipelines and not results:
        # Still could schedule from existing queues, so don't exit early
        pass

    suspensions = []
    assignments = []

    # Pools: prefer placing high priority on larger pools to reduce risk of insufficient headroom
    pool_ids_by_capacity = list(range(s.executor.num_pools))
    pool_ids_by_capacity.sort(
        key=lambda i: (s.executor.pools[i].max_ram_pool, s.executor.pools[i].max_cpu_pool),
        reverse=True,
    )

    # For each pool, schedule a handful of ops
    for pool_id in pool_ids_by_capacity:
        pool = s.executor.pools[pool_id]
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)
        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        num_assigned_here = 0

        # Keep some headroom for interactive bursts: don't consume last slice of RAM/CPU with batch
        # (soft reservation via sizing, not hard admission control).
        while num_assigned_here < s.max_assignments_per_pool:
            pr = _choose_next_priority_with_tokens(s)
            if pr is None:
                break

            # Find a schedulable pipeline in that priority queue (round-robin scan)
            q = s.queues[pr]
            scheduled = False

            scan_len = len(q)
            for _ in range(scan_len):
                pipeline = q.pop(0)
                status = pipeline.runtime_status()

                # If completed, drop from queue
                if status.is_pipeline_successful():
                    continue

                op = _get_ready_op(pipeline)
                if op is None:
                    # Not ready; keep in queue
                    q.append(pipeline)
                    continue

                opk = (pipeline.pipeline_id, _op_key(op))

                # If op appears "poison", only run it when system is otherwise idle-ish
                # to avoid harming query/interactive tail latency.
                if opk in s.poison_ops:
                    # Require plenty of slack to attempt poison ops
                    if pr != Priority.BATCH_PIPELINE and (avail_cpu < pool.max_cpu_pool * 0.5 or avail_ram < pool.max_ram_pool * 0.5):
                        q.append(pipeline)
                        continue

                # Decide CPU/RAM request:
                cpu_share, ram_share = _base_share(pr)

                # Use hints if available (especially for RAM after OOM)
                hinted_ram = s.op_ram_hint.get(opk, None)
                hinted_cpu = s.op_cpu_hint.get(opk, None)

                # Soft headroom: for batch, don't take the pool too close to empty
                # (reduces chance a subsequent query waits and gets penalized heavily)
                if pr == Priority.BATCH_PIPELINE:
                    max_cpu_take = max(avail_cpu - pool.max_cpu_pool * 0.10, 0.0)
                    max_ram_take = max(avail_ram - pool.max_ram_pool * 0.10, 0.0)
                else:
                    max_cpu_take = avail_cpu
                    max_ram_take = avail_ram

                # Base sizing from shares
                cpu_req = max(pool.max_cpu_pool * cpu_share, s.min_cpu_floor)
                ram_req = max(pool.max_ram_pool * ram_share, s.min_ram_floor)

                # Apply hints (RAM hints are treated as minimum requested to avoid repeated OOM)
                if hinted_ram is not None:
                    ram_req = max(ram_req, float(hinted_ram))
                if hinted_cpu is not None:
                    cpu_req = max(cpu_req, float(hinted_cpu))

                # Clamp to what we are willing/able to allocate right now
                cpu_req = _clamp(cpu_req, s.min_cpu_floor, max_cpu_take)
                ram_req = _clamp(ram_req, s.min_ram_floor, max_ram_take)

                # If we can't allocate a minimally useful amount, stop scheduling in this pool
                if cpu_req <= 0 or ram_req <= 0:
                    q.append(pipeline)
                    break

                # If we have a strong RAM hint that doesn't fit yet, prefer waiting over likely OOM
                if hinted_ram is not None and float(hinted_ram) > max_ram_take:
                    q.append(pipeline)
                    continue

                # Final fit check
                if cpu_req > avail_cpu or ram_req > avail_ram:
                    q.append(pipeline)
                    continue

                # Make the assignment (one op per assignment)
                assignments.append(
                    Assignment(
                        ops=[op],
                        cpu=cpu_req,
                        ram=ram_req,
                        priority=pipeline.priority,
                        pool_id=pool_id,
                        pipeline_id=pipeline.pipeline_id,
                    )
                )

                # Consume token + resources, requeue pipeline for subsequent ops
                s.tokens[pr] = max(0.0, s.tokens.get(pr, 0.0) - 1.0)
                avail_cpu -= cpu_req
                avail_ram -= ram_req

                q.append(pipeline)
                num_assigned_here += 1
                scheduled = True
                break

            if not scheduled:
                # No schedulable pipeline found for this priority; burn a token? No.
                # Instead, try another priority if possible.
                # To avoid infinite loops, temporarily set tokens to 0 for this priority if queue exists but blocked.
                if s.queues[pr]:
                    s.tokens[pr] = 0.0
                else:
                    # Nothing to do for this priority
                    pass

                # If all priorities are blocked, exit
                if _choose_next_priority_with_tokens(s) is None:
                    break

    return suspensions, assignments
