# policy_key: scheduler_est_003
# reasoning_effort: low
# model: gpt-5.2-2025-12-11
# llm_cost: 0.041948
# generation_seconds: 44.24
# generated_at: 2026-04-01T13:37:45.294629
@register_scheduler_init(key="scheduler_est_003")
def scheduler_est_003_init(s):
    """
    Priority-aware, conservative sizing scheduler (small improvements over naive FIFO).

    Key changes vs. naive:
      - Maintain per-priority FIFO queues (QUERY > INTERACTIVE > BATCH_PIPELINE).
      - Avoid "give one op all resources": right-size CPU and RAM to reduce head-of-line blocking.
      - Use op.estimate.mem_peak_gb (if present) + safety factor for RAM sizing.
      - On failures, remember the last RAM and increase RAM on retry (simple warm-start).
      - Keep a small "headroom" reserve in each pool so batch work doesn't consume everything.
    """
    s.t = 0

    # Per-priority FIFO queues of pipelines.
    s.queues = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }

    # Track arrival time for mild aging (prevents indefinite starvation).
    # key: pipeline_id -> arrival tick
    s.pipeline_arrival_t = {}

    # Track per-operator RAM history to increase RAM after failures.
    # key: (pipeline_id, op_key) -> {"last_ram": float, "failures": int}
    s.op_ram_hist = {}

    # Config knobs (kept intentionally simple and safe).
    s.min_cpu = 1.0
    s.min_ram = 0.5  # GB
    s.max_ops_per_pool_per_tick = 2

    # Headroom reservation per pool (fraction of pool left unused when only batch is runnable).
    s.headroom_cpu_frac = 0.15
    s.headroom_ram_frac = 0.15

    # RAM sizing factors.
    s.est_ram_safety = 1.25  # multiply estimator by this
    s.fail_ram_growth = 1.6  # multiply previous RAM by this after failure

    # CPU sizing caps per priority (fraction of pool max CPU).
    s.cpu_cap_frac = {
        Priority.QUERY: 0.75,
        Priority.INTERACTIVE: 0.60,
        Priority.BATCH_PIPELINE: 0.40,
    }

    # Mild aging: after this many ticks, allow a batch pipeline to be considered earlier.
    s.aging_ticks = 100


def _op_key(op):
    # Best-effort stable key across retries without relying on specific simulator internals.
    for attr in ("op_id", "operator_id", "id", "name"):
        if hasattr(op, attr):
            v = getattr(op, attr)
            if callable(v):
                try:
                    v = v()
                except Exception:
                    v = None
            if v is not None:
                return str(v)
    # Fallback (not stable across serialization, but better than nothing).
    return str(id(op))


def _is_failed_oom(res):
    # Conservative: only treat as OOM if the error message indicates it.
    try:
        msg = str(res.error).lower()
    except Exception:
        return False
    return ("oom" in msg) or ("out of memory" in msg) or ("cuda oom" in msg) or ("memoryerror" in msg)


def _desired_ram_gb(s, pipeline, op, pool_avail_ram):
    # Base: at least a small minimum.
    ram = s.min_ram

    # Use estimator as a hint (safe upper bound to avoid OOM).
    est = None
    try:
        est = getattr(op, "estimate", None)
        if est is not None:
            est = getattr(est, "mem_peak_gb", None)
    except Exception:
        est = None
    if isinstance(est, (int, float)) and est > 0:
        ram = max(ram, float(est) * s.est_ram_safety)

    # Warm-start after failures: increase from previously attempted RAM.
    hist_key = (pipeline.pipeline_id, _op_key(op))
    h = s.op_ram_hist.get(hist_key)
    if h and h.get("failures", 0) > 0:
        ram = max(ram, float(h.get("last_ram", ram)) * s.fail_ram_growth)

    # Never request more RAM than what's currently available in the pool.
    ram = min(ram, max(s.min_ram, float(pool_avail_ram)))
    return ram


def _desired_cpu(s, priority, pool, pool_avail_cpu):
    cap = float(pool.max_cpu_pool) * float(s.cpu_cap_frac.get(priority, 0.4))
    cpu = min(float(pool_avail_cpu), max(s.min_cpu, cap))
    return cpu


def _pipeline_is_done_or_irrecoverable(pipeline):
    st = pipeline.runtime_status()
    if st.is_pipeline_successful():
        return True
    # If there are failures, we still allow retries (ASSIGNABLE_STATES includes FAILED).
    # So we do NOT treat "has failures" as irrecoverable.
    return False


def _pick_next_pipeline(s):
    # Priority order with mild aging for batch.
    # If batch has waited long enough, treat it as interactive for selection.
    best_prio_order = [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]

    # Check if any batch pipeline is "aged".
    aged_batch = None
    qb = s.queues[Priority.BATCH_PIPELINE]
    for p in qb:
        t0 = s.pipeline_arrival_t.get(p.pipeline_id, s.t)
        if (s.t - t0) >= s.aging_ticks:
            aged_batch = p
            break

    for prio in best_prio_order:
        q = s.queues[prio]
        # If there's an aged batch, allow it to be considered at INTERACTIVE level.
        if prio == Priority.INTERACTIVE and aged_batch is not None:
            return aged_batch, Priority.BATCH_PIPELINE

        while q:
            p = q[0]
            if _pipeline_is_done_or_irrecoverable(p):
                q.pop(0)
                continue
            return p, prio
    return None, None


def _rotate_queue_front(q):
    if q:
        q.append(q.pop(0))


@register_scheduler(key="scheduler_est_003")
def scheduler_est_003_scheduler(s, results, pipelines):
    """
    Step function implementing a simple priority-aware, right-sizing scheduler.

    Strategy:
      - Enqueue new pipelines into per-priority FIFO queues.
      - Process results to update RAM retry history on OOM failures.
      - For each pool, schedule up to N runnable ops, choosing from highest priority queues first.
      - Apply headroom: if only batch is runnable, keep a small reserve in the pool.
    """
    s.t += 1

    # Enqueue new pipelines.
    for p in pipelines:
        s.pipeline_arrival_t.setdefault(p.pipeline_id, s.t)
        s.queues[p.priority].append(p)

    # Process execution results: update RAM history on failures.
    for r in results:
        if not hasattr(r, "ops") or not r.ops:
            continue
        op = r.ops[0]
        # Record what we tried.
        try:
            pid = getattr(r, "pipeline_id", None)
        except Exception:
            pid = None

        # If pipeline_id isn't on the result, fall back to op being within some pipeline;
        # we can only reliably key by op id then.
        # However, the Assignment provides pipeline_id; results often carry it in simulators,
        # but we keep this defensive.
        if pid is None:
            pid = "unknown"

        hist_key = (pid, _op_key(op))
        h = s.op_ram_hist.get(hist_key, {"last_ram": None, "failures": 0})
        try:
            h["last_ram"] = float(getattr(r, "ram", h["last_ram"] or 0.0))
        except Exception:
            pass

        if hasattr(r, "failed") and callable(r.failed) and r.failed():
            # Only treat OOM as a signal to increase RAM aggressively.
            if _is_failed_oom(r):
                h["failures"] = int(h.get("failures", 0)) + 1
            else:
                # Non-OOM failures are not necessarily fixed by RAM; still count lightly.
                h["failures"] = int(h.get("failures", 0)) + 1
        s.op_ram_hist[hist_key] = h

    # Early exit if nothing changed.
    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    # Helper: check if any high-priority runnable work exists (used for headroom).
    def any_high_prio_runnable():
        for prio in (Priority.QUERY, Priority.INTERACTIVE):
            for p in s.queues[prio]:
                if _pipeline_is_done_or_irrecoverable(p):
                    continue
                st = p.runtime_status()
                ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
                if ops:
                    return True
        return False

    high_runnable = any_high_prio_runnable()

    # Schedule per pool.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)
        if avail_cpu < s.min_cpu or avail_ram < s.min_ram:
            continue

        # Schedule up to a small number of ops per tick per pool.
        for _ in range(s.max_ops_per_pool_per_tick):
            if avail_cpu < s.min_cpu or avail_ram < s.min_ram:
                break

            pipeline, src_prio = _pick_next_pipeline(s)
            if pipeline is None:
                break

            # Get runnable operators (parents complete).
            st = pipeline.runtime_status()
            op_list = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
            if not op_list:
                # Not runnable now; rotate it behind others in its queue.
                q = s.queues[src_prio]
                if q and q[0].pipeline_id == pipeline.pipeline_id:
                    _rotate_queue_front(q)
                else:
                    # If it's the aged-batch selection, rotate within batch queue.
                    qb = s.queues[Priority.BATCH_PIPELINE]
                    if qb and qb[0].pipeline_id == pipeline.pipeline_id:
                        _rotate_queue_front(qb)
                continue

            op = op_list[0]

            # Headroom policy: if we're about to schedule batch while there is (or may soon be)
            # high-priority work, keep some reserve to reduce latency spikes.
            is_batch = (pipeline.priority == Priority.BATCH_PIPELINE)
            reserve_cpu = 0.0
            reserve_ram = 0.0
            if is_batch and high_runnable:
                reserve_cpu = float(pool.max_cpu_pool) * s.headroom_cpu_frac
                reserve_ram = float(pool.max_ram_pool) * s.headroom_ram_frac

            eff_avail_cpu = max(0.0, avail_cpu - reserve_cpu)
            eff_avail_ram = max(0.0, avail_ram - reserve_ram)
            if eff_avail_cpu < s.min_cpu or eff_avail_ram < s.min_ram:
                # Can't schedule batch without breaking headroom; rotate and try later.
                qb = s.queues[Priority.BATCH_PIPELINE]
                if qb and qb[0].pipeline_id == pipeline.pipeline_id:
                    _rotate_queue_front(qb)
                break

            cpu = _desired_cpu(s, pipeline.priority, pool, eff_avail_cpu)
            ram = _desired_ram_gb(s, pipeline, op, eff_avail_ram)

            if cpu < s.min_cpu or ram < s.min_ram:
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

            # Update available resources pessimistically for this tick's decisions.
            avail_cpu -= cpu
            avail_ram -= ram

            # Rotate pipeline to allow other pipelines of same priority to make progress.
            q = s.queues[src_prio]
            if q and q[0].pipeline_id == pipeline.pipeline_id:
                _rotate_queue_front(q)
            else:
                # Aged-batch selection path.
                qb = s.queues[Priority.BATCH_PIPELINE]
                if qb and qb[0].pipeline_id == pipeline.pipeline_id:
                    _rotate_queue_front(qb)

    return suspensions, assignments
