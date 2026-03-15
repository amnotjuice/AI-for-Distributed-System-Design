# policy_key: scheduler_medium_006
# reasoning_effort: medium
# model: gpt-5.2-2025-12-11
# llm_cost: 0.070489
# generation_seconds: 72.09
# generated_at: 2026-03-12T22:00:16.870161
@register_scheduler_init(key="scheduler_medium_006")
def scheduler_medium_006_init(s):
    """Priority-aware, right-sized FIFO with OOM-aware retries.

    Improvements over naive FIFO:
      1) Do not allocate *all* pool CPU/RAM to a single op; right-size to allow concurrency (lower tail latency).
      2) Priority queues: serve QUERY/INTERACTIVE ahead of BATCH.
      3) Keep headroom when high-priority work is waiting (soft reservations).
      4) Retry FAILED ops when the failure looks like OOM by increasing RAM hints; avoid infinite retries.
    """
    # Per-priority FIFO queues of Pipeline objects
    s.queues = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }

    # Round-robin cursor per priority to avoid one pipeline monopolizing the queue
    s.rr_cursor = {
        Priority.QUERY: 0,
        Priority.INTERACTIVE: 0,
        Priority.BATCH_PIPELINE: 0,
    }

    # Per-operator RAM hint (bytes/units as used by simulator). Keyed by (pipeline_id, op_key).
    s.op_ram_hint = {}

    # Retry counters per operator (pipeline_id, op_key) -> int
    s.op_retries = {}

    # Pipelines that we consider permanently failed (non-OOM or too many OOM retries)
    s.dead_pipelines = set()

    # Some light accounting
    s.iteration = 0


def _prio_order():
    # Keep the order explicit and stable.
    # (If INTERACTIVE is meant to be "higher" than QUERY in your workload, swap them here.)
    return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]


def _op_key(op):
    # Prefer a stable operator id if present; else fall back to Python object identity.
    k = getattr(op, "op_id", None)
    if k is None:
        k = getattr(op, "operator_id", None)
    if k is None:
        k = id(op)
    return k


def _is_oom_error(err) -> bool:
    if err is None:
        return False
    msg = str(err).upper()
    # Keep this broad; simulator errors often stringify.
    return ("OOM" in msg) or ("OUT OF MEMORY" in msg) or ("OUT_OF_MEMORY" in msg) or ("MEMORY" in msg and "OUT" in msg)


def _queue_len(s, prio):
    q = s.queues.get(prio, [])
    return len(q) if q is not None else 0


def _has_high_prio_waiting(s) -> bool:
    return (_queue_len(s, Priority.QUERY) + _queue_len(s, Priority.INTERACTIVE)) > 0


def _clean_and_requeue_pipeline(s, pipeline, prio):
    """Return True if pipeline should remain eligible (queued), else False."""
    if pipeline is None:
        return False
    pid = pipeline.pipeline_id
    if pid in s.dead_pipelines:
        return False

    st = pipeline.runtime_status()
    if st.is_pipeline_successful():
        return False

    # Keep it around even if it has FAILED ops; we may retry (especially for OOM).
    s.queues[prio].append(pipeline)
    return True


def _pop_next_ready_pipeline_and_ops(s, prio):
    """Round-robin scan within a priority queue to find a pipeline with a ready op.

    Returns (pipeline, op_list) or (None, None).
    """
    q = s.queues[prio]
    if not q:
        return None, None

    n = len(q)
    # rr_cursor points to the next index we *try* first
    start = s.rr_cursor.get(prio, 0) % n

    for off in range(n):
        idx = (start + off) % n
        pipeline = q[idx]
        if pipeline is None:
            continue
        if pipeline.pipeline_id in s.dead_pipelines:
            continue

        st = pipeline.runtime_status()
        if st.is_pipeline_successful():
            continue

        # Find one ready operator (parents complete)
        ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
        if not ops:
            continue

        # Remove pipeline from queue and advance cursor to the next position
        # (cursor points to "next element after removed idx" in the *old* list; close enough)
        q.pop(idx)
        if q:
            s.rr_cursor[prio] = idx % len(q)
        else:
            s.rr_cursor[prio] = 0
        return pipeline, ops

    # No ready ops found; leave queue as-is
    s.rr_cursor[prio] = start
    return None, None


def _default_cpu_target(priority):
    # Small, latency-friendly sizing (lets multiple ops run concurrently).
    if priority == Priority.BATCH_PIPELINE:
        return 2.0
    return 4.0


def _default_ram_fraction(priority):
    # Default RAM sizing as a fraction of pool capacity. OOMs will bump via hints.
    if priority == Priority.BATCH_PIPELINE:
        return 0.20
    return 0.25


def _oom_max_retries(priority):
    # Be more forgiving for interactive work; still bounded.
    if priority == Priority.BATCH_PIPELINE:
        return 3
    return 5


def _compute_request(s, pool, pipeline_id, ops, priority, avail_cpu, avail_ram):
    """Compute (cpu_req, ram_req). Returns (None, None) if cannot fit now."""
    # CPU target
    cpu_target = _default_cpu_target(priority)
    cpu_req = min(avail_cpu, cpu_target)
    if cpu_req <= 0:
        return None, None

    # RAM target: max(default fraction, learned hint)
    op = ops[0]
    ok = (pipeline_id, _op_key(op))
    hint = s.op_ram_hint.get(ok, 0.0)

    default_ram = _default_ram_fraction(priority) * float(pool.max_ram_pool)
    ram_req = max(default_ram, float(hint))

    # If still not positive, make a minimal request
    if ram_req <= 0:
        ram_req = min(avail_ram, max(1.0, 0.10 * float(pool.max_ram_pool)))

    # Must fit
    if ram_req > avail_ram:
        return None, None

    return cpu_req, ram_req


@register_scheduler(key="scheduler_medium_006")
def scheduler_medium_006_scheduler(s, results, pipelines):
    """
    Priority-aware, right-sized, OOM-adaptive scheduler.

    Core behavior:
      - Enqueue arriving pipelines by priority.
      - Update per-op RAM hints from OOM failures; retry OOM-failed ops (bounded).
      - For each pool, schedule multiple ops in one tick, right-sized, by priority.
      - Soft-reserve a share of resources for high-priority work by limiting batch usage
        when QUERY/INTERACTIVE items are waiting.
    """
    s.iteration += 1

    # --- 1) Ingest new pipelines into priority queues ---
    for p in pipelines:
        if p is None:
            continue
        pr = p.priority
        if pr not in s.queues:
            # Unknown priority: treat as batch-like.
            pr = Priority.BATCH_PIPELINE
        s.queues[pr].append(p)

    # --- 2) Process execution results: learn OOM hints and mark hard failures ---
    for r in results:
        if r is None:
            continue
        if not getattr(r, "ops", None):
            continue

        failed = r.failed()
        oom = failed and _is_oom_error(getattr(r, "error", None))

        for op in r.ops:
            ok = (getattr(r, "pipeline_id", None), _op_key(op))  # best-effort; may be None
            # If pipeline_id isn't present on result, fall back to searching by op's pipeline isn't possible;
            # store only when pipeline_id is known.
            if ok[0] is None:
                continue

            if failed:
                if oom:
                    # Increase RAM hint aggressively to reduce repeat OOMs.
                    prev = float(s.op_ram_hint.get(ok, 0.0))
                    # r.ram is what we attempted; double it on OOM.
                    attempted = float(getattr(r, "ram", 0.0))
                    new_hint = max(prev, attempted * 2.0)
                    # Add a small floor relative to pool attempt if attempted is 0.
                    if new_hint <= 0:
                        new_hint = max(prev, 1.0)

                    s.op_ram_hint[ok] = new_hint
                    s.op_retries[ok] = int(s.op_retries.get(ok, 0)) + 1
                else:
                    # Non-OOM failures are treated as permanent (avoid infinite loops).
                    # We mark the pipeline as dead if we can resolve pipeline_id.
                    s.dead_pipelines.add(ok[0])
            else:
                # On success, optionally keep hint (no harm); could decay but keep simple/stable.
                pass

    # --- 3) Clean queues of completed/dead pipelines ---
    for pr in list(s.queues.keys()):
        old = s.queues[pr]
        s.queues[pr] = []
        for p in old:
            _clean_and_requeue_pipeline(s, p, pr)

    # Early exit if nothing to do
    if not any(s.queues[pr] for pr in s.queues) and not pipelines and not results:
        return [], []

    suspensions = []  # No preemption in this version (lack of portable "list running containers" API)
    assignments = []

    # --- 4) Schedule per pool with soft reservations for high priority ---
    high_waiting = _has_high_prio_waiting(s)

    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]

        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)
        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # Soft reservations: if high-priority is waiting, don't let batch consume the last chunk.
        # (This reduces head-of-line blocking / tail latency for interactive arrivals.)
        reserve_cpu = 0.0
        reserve_ram = 0.0
        if high_waiting:
            reserve_cpu = 0.25 * float(pool.max_cpu_pool)
            reserve_ram = 0.25 * float(pool.max_ram_pool)

        made_progress = True
        # Try to fill the pool with multiple small assignments
        while made_progress:
            made_progress = False

            for pr in _prio_order():
                # Batch is limited to "non-reserved" resources when high-priority exists.
                eff_avail_cpu = avail_cpu
                eff_avail_ram = avail_ram
                if pr == Priority.BATCH_PIPELINE and high_waiting:
                    eff_avail_cpu = max(0.0, avail_cpu - reserve_cpu)
                    eff_avail_ram = max(0.0, avail_ram - reserve_ram)

                if eff_avail_cpu <= 0 or eff_avail_ram <= 0:
                    continue

                pipeline, ops = _pop_next_ready_pipeline_and_ops(s, pr)
                if pipeline is None:
                    continue

                pid = pipeline.pipeline_id
                # Bound OOM retries per operator; if exceeded, mark pipeline dead to avoid churn.
                ok = (pid, _op_key(ops[0]))
                if int(s.op_retries.get(ok, 0)) > _oom_max_retries(pr):
                    s.dead_pipelines.add(pid)
                    continue

                cpu_req, ram_req = _compute_request(
                    s=s,
                    pool=pool,
                    pipeline_id=pid,
                    ops=ops,
                    priority=pr,
                    avail_cpu=eff_avail_cpu,
                    avail_ram=eff_avail_ram,
                )

                if cpu_req is None or ram_req is None:
                    # Not enough room right now; put pipeline back for later.
                    s.queues[pr].append(pipeline)
                    continue

                assignments.append(
                    Assignment(
                        ops=ops,
                        cpu=cpu_req,
                        ram=ram_req,
                        priority=pr,
                        pool_id=pool_id,
                        pipeline_id=pid,
                    )
                )

                # Deduct from actual pool availability (not the effective values).
                avail_cpu -= float(cpu_req)
                avail_ram -= float(ram_req)

                made_progress = True
                break  # re-evaluate priorities with updated availability

            if avail_cpu <= 0 or avail_ram <= 0:
                break

    return suspensions, assignments
