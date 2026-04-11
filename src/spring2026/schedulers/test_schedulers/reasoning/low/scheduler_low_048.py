# policy_key: scheduler_low_048
# reasoning_effort: low
# model: gpt-5.2-2025-12-11
# llm_cost: 0.040625
# generation_seconds: 43.70
# generated_at: 2026-04-09T21:40:52.405943
@register_scheduler_init(key="scheduler_low_048")
def scheduler_low_048_init(s):
    """Priority-aware DRR (deficit round-robin) with OOM-safe RAM backoff.

    Goals vs FIFO:
      - Protect QUERY/INTERACTIVE latency (dominant weights in objective)
      - Avoid failures/incompletes by retrying suspected OOM with exponential RAM backoff
      - Avoid starvation via deficit round-robin across priority classes

    High-level:
      - Maintain per-priority queues of pipelines
      - Each tick, add deficit proportional to objective weights (Q=10, I=5, B=1)
      - Schedule one ready operator at a time, cycling through priorities while deficit allows
      - Size CPU aggressively for QUERY, moderately for INTERACTIVE, conservatively for BATCH
      - Size RAM using a hint table; on failure, increase RAM hint and retry (bounded)
    """
    # Pipelines waiting by priority
    s.q_query = []
    s.q_interactive = []
    s.q_batch = []

    # Pipeline bookkeeping
    s.pipeline_by_id = {}

    # Deficit round-robin state (bigger quantum -> more service)
    s.deficit = {
        Priority.QUERY: 0.0,
        Priority.INTERACTIVE: 0.0,
        Priority.BATCH_PIPELINE: 0.0,
    }
    s.quantum = {
        Priority.QUERY: 10.0,
        Priority.INTERACTIVE: 5.0,
        Priority.BATCH_PIPELINE: 1.0,
    }
    s.prio_order = [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]
    s.prio_rr_idx = 0

    # RAM hinting/backoff for retries
    # Keyed by (pipeline_id, op_key) -> {"ram": float, "retries": int}
    s.op_hints = {}

    # Retry policy (avoid infinite loops on non-OOM failures)
    s.max_retries = 4

    # "Small but safe" initial RAM fraction by priority (of pool max RAM),
    # further capped by pool available RAM at scheduling time.
    s.init_ram_frac = {
        Priority.QUERY: 0.55,
        Priority.INTERACTIVE: 0.45,
        Priority.BATCH_PIPELINE: 0.30,
    }

    # CPU sizing fractions (of pool max CPU), capped by available CPU.
    s.cpu_frac = {
        Priority.QUERY: 1.00,
        Priority.INTERACTIVE: 0.70,
        Priority.BATCH_PIPELINE: 0.40,
    }


def _queue_for_priority(s, prio):
    if prio == Priority.QUERY:
        return s.q_query
    if prio == Priority.INTERACTIVE:
        return s.q_interactive
    return s.q_batch


def _op_key(op):
    # Best-effort stable key without relying on imports or unknown attributes.
    # Prefer explicit id/name fields if present; otherwise fall back to object id.
    for attr in ("operator_id", "op_id", "id", "name"):
        if hasattr(op, attr):
            try:
                v = getattr(op, attr)
                if v is not None:
                    return (attr, str(v))
            except Exception:
                pass
    return ("pyid", str(id(op)))


def _is_probable_oom(err):
    if err is None:
        return False
    try:
        msg = str(err).lower()
    except Exception:
        return False
    # Heuristic: treat any memory-ish failure as OOM-like.
    return ("oom" in msg) or ("out of memory" in msg) or ("memory" in msg) or ("killed" in msg)


def _pipeline_done_or_hard_failed(pipeline):
    status = pipeline.runtime_status()
    if status.is_pipeline_successful():
        return True
    # If there are failed operators, we may still want to retry (OOM/backoff),
    # so do not treat FAILED as terminal here.
    return False


def _next_ready_op(pipeline):
    status = pipeline.runtime_status()
    # Include FAILED so we can retry with bigger RAM if needed, but only once parents complete.
    op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
    if not op_list:
        return None
    return op_list[0]


def _choose_pool(s, prio):
    # Prefer pools with the most headroom. For high priority, bias more heavily to RAM headroom.
    best_pool = None
    best_score = None
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool
        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # Simple headroom score: normalized available resources.
        # For QUERY/INTERACTIVE, emphasize RAM to reduce OOM risk and avoid retries.
        cpu_norm = avail_cpu / max(pool.max_cpu_pool, 1e-9)
        ram_norm = avail_ram / max(pool.max_ram_pool, 1e-9)
        if prio == Priority.QUERY:
            score = (0.35 * cpu_norm) + (0.65 * ram_norm)
        elif prio == Priority.INTERACTIVE:
            score = (0.45 * cpu_norm) + (0.55 * ram_norm)
        else:
            score = (0.60 * cpu_norm) + (0.40 * ram_norm)

        if best_score is None or score > best_score:
            best_score = score
            best_pool = pool_id
    return best_pool


def _size_cpu(s, pool, prio, avail_cpu):
    # Allocate a priority-dependent slice of pool max, capped by availability.
    target = pool.max_cpu_pool * s.cpu_frac.get(prio, 0.5)
    cpu = min(avail_cpu, max(1.0, target))
    # Ensure we don't request more than pool max (defensive).
    cpu = min(cpu, pool.max_cpu_pool)
    return cpu


def _size_ram(s, pool, prio, avail_ram, pipeline_id, op):
    ok = _op_key(op)
    hk = (pipeline_id, ok)
    hint = s.op_hints.get(hk)
    if hint and hint.get("ram", 0) > 0:
        base = hint["ram"]
    else:
        base = pool.max_ram_pool * s.init_ram_frac.get(prio, 0.35)

    # Cap by availability and pool max; enforce a minimum > 0.
    ram = min(avail_ram, min(pool.max_ram_pool, max(0.1, base)))
    return ram


@register_scheduler(key="scheduler_low_048")
def scheduler_low_048(s, results, pipelines):
    """
    Tick-based policy:
      1) Ingest new pipelines into per-priority queues.
      2) Process results:
         - On failure: if probable OOM and retries remain, increase RAM hint and keep pipeline queued.
         - Otherwise: keep pipeline queued (the simulator will reflect FAILED ops; we won't drop).
      3) Add DRR deficits and issue assignments until pools fill or deficits exhausted.
    """
    # Enqueue new pipelines
    for p in pipelines:
        s.pipeline_by_id[p.pipeline_id] = p
        _queue_for_priority(s, p.priority).append(p)

    # If nothing changed, avoid work
    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    # Process execution results to update RAM hints for retries
    for r in results:
        if not hasattr(r, "ops") or not r.ops:
            continue

        # Update hint for each op involved in the container (typically one op)
        # If failed and looks like OOM -> increase RAM hint.
        if hasattr(r, "failed") and r.failed():
            probable_oom = _is_probable_oom(getattr(r, "error", None))
            for op in r.ops:
                hk = (getattr(r, "pipeline_id", None), _op_key(op))
                # If pipeline_id isn't included in result, best-effort: skip hinting.
                if hk[0] is None:
                    continue

                prev = s.op_hints.get(hk, {"ram": getattr(r, "ram", 0.0) or 0.0, "retries": 0})
                retries = prev.get("retries", 0) + 1
                if probable_oom and retries <= s.max_retries:
                    prev_ram = prev.get("ram", 0.0) or (getattr(r, "ram", 0.0) or 0.1)
                    # Exponential backoff, but don't exceed pool max at schedule-time.
                    new_ram = max(prev_ram * 1.75, prev_ram + 0.5)
                    s.op_hints[hk] = {"ram": new_ram, "retries": retries}
                else:
                    # Non-OOM or too many retries: still record retries to avoid endless growth.
                    s.op_hints[hk] = {"ram": prev.get("ram", getattr(r, "ram", 0.0) or 0.1), "retries": retries}

    # Clean queues of completed pipelines (keep FAILED/incomplete for retry)
    def _prune_queue(q):
        kept = []
        for p in q:
            if _pipeline_done_or_hard_failed(p):
                continue
            kept.append(p)
        return kept

    s.q_query = _prune_queue(s.q_query)
    s.q_interactive = _prune_queue(s.q_interactive)
    s.q_batch = _prune_queue(s.q_batch)

    # Add deficit each tick to ensure fairness while strongly favoring higher weights.
    for pr in s.prio_order:
        s.deficit[pr] += s.quantum[pr]

    # Track local available resources per pool as we build assignments (avoid oversubscription).
    pool_avail = {}
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        pool_avail[pool_id] = [pool.avail_cpu_pool, pool.avail_ram_pool]

    # Helper: attempt one assignment for a given priority if deficit allows.
    def _try_schedule_one(prio):
        # Need at least "1 unit" deficit per op scheduled.
        if s.deficit.get(prio, 0.0) < 1.0:
            return False

        q = _queue_for_priority(s, prio)
        if not q:
            return False

        # Round-robin within the priority queue to avoid head-of-line blocking.
        # Pop front, inspect, and either schedule it or move to back.
        attempts = len(q)
        while attempts > 0:
            attempts -= 1
            p = q.pop(0)

            if _pipeline_done_or_hard_failed(p):
                continue

            op = _next_ready_op(p)
            if op is None:
                # Not ready (waiting on parents or already running); rotate to back.
                q.append(p)
                continue

            pool_id = _choose_pool(s, prio)
            if pool_id is None:
                # No capacity anywhere; put it back and stop trying this prio this tick.
                q.append(p)
                return False

            pool = s.executor.pools[pool_id]
            avail_cpu, avail_ram = pool_avail[pool_id]
            if avail_cpu <= 0 or avail_ram <= 0:
                q.append(p)
                return False

            cpu = _size_cpu(s, pool, prio, avail_cpu)
            ram = _size_ram(s, pool, prio, avail_ram, p.pipeline_id, op)

            # If we can't give at least minimal resources, skip for now.
            if cpu <= 0 or ram <= 0:
                q.append(p)
                return False

            # Reserve locally and emit assignment
            pool_avail[pool_id][0] -= cpu
            pool_avail[pool_id][1] -= ram

            assignments.append(
                Assignment(
                    ops=[op],
                    cpu=cpu,
                    ram=ram,
                    priority=p.priority,
                    pool_id=pool_id,
                    pipeline_id=p.pipeline_id,
                )
            )

            # Spend deficit and rotate pipeline to back (more ops may remain).
            s.deficit[prio] -= 1.0
            q.append(p)
            return True

        return False

    # Main scheduling loop: keep placing while any priority can make progress.
    # Start from current RR pointer to prevent always scanning from QUERY.
    made_progress = True
    safety = 0
    while made_progress and safety < 10_000:
        safety += 1
        made_progress = False
        for i in range(len(s.prio_order)):
            prio = s.prio_order[(s.prio_rr_idx + i) % len(s.prio_order)]
            if _try_schedule_one(prio):
                made_progress = True
                s.prio_rr_idx = (s.prio_order.index(prio) + 1) % len(s.prio_order)
                break

        # Stop if all pools are effectively full.
        any_capacity = False
        for pool_id in range(s.executor.num_pools):
            cpu_left, ram_left = pool_avail[pool_id]
            if cpu_left > 0 and ram_left > 0:
                any_capacity = True
                break
        if not any_capacity:
            break

    return suspensions, assignments
