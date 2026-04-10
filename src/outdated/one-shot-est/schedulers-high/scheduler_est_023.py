# policy_key: scheduler_est_023
# reasoning_effort: high
# model: gpt-5.2-2025-12-11
# llm_cost: 0.127129
# generation_seconds: 109.05
# generated_at: 2026-03-31T19:04:45.933095
@register_scheduler_init(key="scheduler_est_023")
def scheduler_est_023_init(s):
    """Priority-aware, estimate-driven scheduler (incremental improvements over naive FIFO).

    Key ideas (kept intentionally simple and robust):
      1) Split the single FIFO queue into per-priority queues to reduce tail latency for high priority.
      2) Right-size RAM using op.estimate.mem_peak_gb with a small safety margin to reduce OOM churn.
      3) Avoid head-of-line blocking by scanning within a priority queue for an op that *fits* current pool headroom.
      4) Basic fairness: occasionally allow batch to run even if high-priority work keeps arriving (credit-based).
      5) Retry OOM failures by increasing a per-operator RAM hint derived from the last failed attempt.
    """
    from collections import deque

    # Per-priority round-robin queues of pipelines.
    s.queues = {
        Priority.QUERY: deque(),
        Priority.INTERACTIVE: deque(),
        Priority.BATCH_PIPELINE: deque(),
    }

    # Per-operator memory hints (GB). Keyed by id(op) to avoid relying on un-guaranteed identifiers.
    s.op_ram_hint_gb = {}

    # Pipelines that experienced a non-OOM failure: do not keep retrying indefinitely.
    s.hard_failed_pipelines = set()

    # Fairness knob: allow batch work to run once after enough high-priority assignments.
    s.batch_credit = 0
    s.batch_credit_threshold = 6


def _is_oom_error(err) -> bool:
    if not err:
        return False
    s = str(err).lower()
    return ("oom" in s) or ("out of memory" in s) or ("memory" in s and "exceed" in s)


def _get_estimated_mem_gb(op) -> float:
    # Best-effort extraction of estimate; default small (but non-zero) to avoid allocating 0 RAM.
    est = None
    try:
        est_obj = getattr(op, "estimate", None)
        est = getattr(est_obj, "mem_peak_gb", None)
    except Exception:
        est = None
    if est is None:
        return 1.0
    try:
        est_f = float(est)
        return est_f if est_f > 0 else 1.0
    except Exception:
        return 1.0


def _priority_params(priority):
    # Conservative safety factors: larger for higher priority to avoid OOM requeues that hurt latency.
    if priority == Priority.QUERY:
        return {
            "ram_safety": 1.25,
            "cpu_cap_frac": 0.60,
            "cpu_min": 1.0,
            "ram_min": 1.0,
        }
    if priority == Priority.INTERACTIVE:
        return {
            "ram_safety": 1.20,
            "cpu_cap_frac": 0.45,
            "cpu_min": 1.0,
            "ram_min": 1.0,
        }
    # Batch
    return {
        "ram_safety": 1.15,
        "cpu_cap_frac": 0.30,
        "cpu_min": 1.0,
        "ram_min": 1.0,
    }


def _size_request_gb_cpu(s, op, priority, pool, avail_cpu, avail_ram, reserve_for_high=False):
    """Compute a feasible (cpu, ram) request for this op in this pool given current availability.

    If reserve_for_high is True (i.e., high-priority backlog exists), batch work is constrained to
    avoid consuming the last headroom that high priority may need soon.
    """
    params = _priority_params(priority)

    # Reservation only applies to batch when high-priority backlog exists.
    if reserve_for_high:
        # Keep a small fraction of pool headroom for high priority.
        reserve_cpu = 0.20 * float(pool.max_cpu_pool)
        reserve_ram = 0.20 * float(pool.max_ram_pool)
        avail_cpu = max(0.0, float(avail_cpu) - reserve_cpu)
        avail_ram = max(0.0, float(avail_ram) - reserve_ram)

    if avail_cpu <= 0.0 or avail_ram <= 0.0:
        return 0.0, 0.0

    est_mem = _get_estimated_mem_gb(op)
    hint = float(s.op_ram_hint_gb.get(id(op), 0.0))

    # RAM request is driven by estimate (with safety) and any accumulated hint from past OOMs.
    ram_req = max(params["ram_min"], est_mem * params["ram_safety"], hint)

    # CPU request: cap to a fraction of pool size to avoid single-op monopolization.
    cpu_cap = max(params["cpu_min"], float(pool.max_cpu_pool) * params["cpu_cap_frac"])
    cpu_req = min(float(avail_cpu), cpu_cap)
    cpu_req = max(0.0, cpu_req)

    # Must fit.
    if ram_req > float(avail_ram) or cpu_req < params["cpu_min"]:
        return 0.0, 0.0

    return cpu_req, ram_req


def _enqueue_pipeline(s, p):
    q = s.queues.get(p.priority, None)
    if q is None:
        # Unknown priority; treat as lowest.
        s.queues[Priority.BATCH_PIPELINE].append(p)
    else:
        q.append(p)


def _cleanup_or_keep_pipeline(s, pipeline):
    """Return True if pipeline should remain enqueued, False if it should be dropped."""
    if pipeline.pipeline_id in s.hard_failed_pipelines:
        return False

    status = pipeline.runtime_status()
    if status.is_pipeline_successful():
        return False

    # If there are failures, we only keep it if we believe it's retriable (OOM).
    # We cannot inspect per-op errors from status, so we rely on result processing to mark hard failures.
    return True


def _find_fittable_op_in_queue(s, queue, priority, pool, avail_cpu, avail_ram, reserve_for_high=False):
    """Round-robin scan for a pipeline with an assignable op that fits current availability."""
    n = len(queue)
    if n == 0:
        return None, None, 0.0, 0.0

    for _ in range(n):
        pipeline = queue.popleft()

        if not _cleanup_or_keep_pipeline(s, pipeline):
            continue

        status = pipeline.runtime_status()
        op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]

        # Always re-enqueue active pipelines to preserve round-robin fairness.
        queue.append(pipeline)

        if not op_list:
            continue

        op = op_list[0]
        cpu_req, ram_req = _size_request_gb_cpu(
            s, op, priority, pool, avail_cpu, avail_ram, reserve_for_high=reserve_for_high
        )
        if cpu_req > 0.0 and ram_req > 0.0:
            return pipeline, op, cpu_req, ram_req

    return None, None, 0.0, 0.0


@register_scheduler(key="scheduler_est_023")
def scheduler_est_023(s, results, pipelines):
    """
    Priority-aware scheduler with estimate-driven sizing and OOM-aware retries.

    Differences vs naive FIFO:
      - Separate queues by Priority and schedule strict-priority first.
      - Pack multiple ops per pool per tick (within available CPU/RAM) instead of 1-op-per-pool.
      - Use op.estimate.mem_peak_gb (+ safety) and learned OOM hints to size RAM.
      - Avoid head-of-line blocking: scan within a priority queue for an op that fits current headroom.
      - Batch fairness via a simple credit mechanism.
    """
    suspensions = []
    assignments = []

    # Ingest new pipelines.
    for p in pipelines:
        _enqueue_pipeline(s, p)

    # Learn from recent execution results (OOM-aware retries, hard-fail detection).
    for r in results:
        if r.failed():
            if _is_oom_error(r.error):
                # Increase RAM hint for any ops in this container so retry has a better chance.
                # Use max(existing, 1.5x last attempted RAM) as a simple exponential backoff.
                try:
                    attempted_ram = float(r.ram) if r.ram is not None else 0.0
                except Exception:
                    attempted_ram = 0.0
                new_hint = max(1.0, attempted_ram * 1.5) if attempted_ram > 0.0 else 2.0

                for op in (r.ops or []):
                    k = id(op)
                    prev = float(s.op_ram_hint_gb.get(k, 0.0))
                    s.op_ram_hint_gb[k] = max(prev, new_hint)
            else:
                # Non-OOM failures are treated as terminal for the pipeline to avoid infinite retries.
                # We may not always be able to recover pipeline_id from the result; best-effort.
                for op in (r.ops or []):
                    pid = getattr(op, "pipeline_id", None)
                    if pid is not None:
                        s.hard_failed_pipelines.add(pid)
        else:
            # On success, we can optionally record a lower-bound RAM hint to avoid regression;
            # keep it conservative: never decrease hints.
            try:
                used_ram = float(r.ram) if r.ram is not None else 0.0
            except Exception:
                used_ram = 0.0
            if used_ram > 0.0:
                for op in (r.ops or []):
                    k = id(op)
                    prev = float(s.op_ram_hint_gb.get(k, 0.0))
                    s.op_ram_hint_gb[k] = max(prev, min(used_ram, used_ram * 1.05))

    # If no changes that affect decisions, exit early.
    if not pipelines and not results:
        return suspensions, assignments

    # Determine whether we should reserve headroom for high priority when scheduling batch.
    # Approximation: if queues have any high-priority pipelines, we treat it as high backlog.
    high_backlog = (len(s.queues[Priority.QUERY]) > 0) or (len(s.queues[Priority.INTERACTIVE]) > 0)

    # Strict priority order.
    prio_order = [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]

    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)

        if avail_cpu <= 0.0 or avail_ram <= 0.0:
            continue

        # Pack as many assignments as fit in this pool this tick.
        while avail_cpu > 0.0 and avail_ram > 0.0:
            made = False

            # Decide whether batch is allowed this round (fairness).
            allow_batch = (not high_backlog) or (s.batch_credit >= s.batch_credit_threshold)

            for prio in prio_order:
                if prio == Priority.BATCH_PIPELINE and not allow_batch:
                    continue

                queue = s.queues[prio]
                if not queue:
                    continue

                reserve_for_high = (prio == Priority.BATCH_PIPELINE and high_backlog)
                pipeline, op, cpu_req, ram_req = _find_fittable_op_in_queue(
                    s, queue, prio, pool, avail_cpu, avail_ram, reserve_for_high=reserve_for_high
                )
                if pipeline is None:
                    continue

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

                avail_cpu -= float(cpu_req)
                avail_ram -= float(ram_req)

                # Fairness credit update: if we schedule high-priority, increase credit;
                # if we schedule batch, reset credit (we "spent" it).
                if prio in (Priority.QUERY, Priority.INTERACTIVE):
                    s.batch_credit += 1
                else:
                    s.batch_credit = 0

                made = True
                break

            if not made:
                break

    return suspensions, assignments
