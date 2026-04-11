# policy_key: scheduler_low_032
# reasoning_effort: low
# model: gpt-5.2-2025-12-11
# llm_cost: 0.052889
# generation_seconds: 46.52
# generated_at: 2026-04-09T21:28:24.958255
@register_scheduler_init(key="scheduler_low_032")
def scheduler_low_032_init(s):
    """Priority-aware, failure-averse scheduler with light aging and OOM-driven RAM retry.

    Core ideas:
      - Protect QUERY/INTERACTIVE latency by always selecting from higher-priority queues first,
        but add gentle aging so BATCH makes steady progress (avoids starvation and 720s penalties).
      - Be conservative about failures: if an operator fails (esp. OOM), remember a higher RAM hint
        for that operator and retry with increased RAM next time (bounded by pool capacity).
      - Avoid over-packing: cap per-op CPU/RAM shares by priority to keep concurrency and reduce
        head-of-line blocking, while still giving high priority ops enough resources.
    """
    # Logical time for aging (ticks / scheduling invocations)
    s.timestep = 0

    # Per-priority FIFO queues of (enqueue_ts, pipeline)
    s.queues = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }

    # Track pipelines currently known to queues (avoid duplicates if re-added)
    s.known_pipeline_ids = set()

    # Operator resource hints learned from failures:
    # key: (pipeline_id, op_key) -> ram_hint
    s.op_ram_hint = {}

    # Count failures per operator to progressively increase RAM
    s.op_fail_count = {}

    # Soft caps per priority (fractions of a pool's *maximum* capacity)
    s.cpu_cap_frac = {
        Priority.QUERY: 0.75,
        Priority.INTERACTIVE: 0.55,
        Priority.BATCH_PIPELINE: 0.35,
    }
    s.ram_cap_frac = {
        Priority.QUERY: 0.75,
        Priority.INTERACTIVE: 0.60,
        Priority.BATCH_PIPELINE: 0.50,
    }

    # Aging rates: how fast a waiting pipeline gains urgency (per tick)
    s.aging_rate = {
        Priority.QUERY: 0.00,
        Priority.INTERACTIVE: 0.02,
        Priority.BATCH_PIPELINE: 0.06,
    }


def _pipeline_done_or_failed(pipeline):
    status = pipeline.runtime_status()
    if status.is_pipeline_successful():
        return True
    # Treat any FAILED operators as terminal to avoid endless retries for non-OOM errors.
    # (We still react to failures to learn resource hints.)
    if status.state_counts.get(OperatorState.FAILED, 0) > 0:
        return True
    return False


def _op_key(op):
    # Best-effort stable key across retries; fall back to repr.
    for attr in ("op_id", "operator_id", "id", "name"):
        if hasattr(op, attr):
            try:
                v = getattr(op, attr)
                if v is not None:
                    return str(v)
            except Exception:
                pass
    return repr(op)


def _extract_min_ram(op):
    # Try common attribute names; otherwise 0 (unknown).
    for attr in ("min_ram", "ram_min", "min_memory", "memory_min", "min_mem", "peak_ram", "required_ram"):
        if hasattr(op, attr):
            try:
                v = getattr(op, attr)
                if v is not None:
                    return float(v)
            except Exception:
                pass
    return 0.0


def _is_oom_error(err):
    if err is None:
        return False
    try:
        s = str(err).lower()
    except Exception:
        return False
    return ("oom" in s) or ("out of memory" in s) or ("killed" in s and "memory" in s)


def _base_priority_score(priority):
    # Higher means scheduled earlier.
    if priority == Priority.QUERY:
        return 100.0
    if priority == Priority.INTERACTIVE:
        return 60.0
    return 20.0


def _effective_pipeline_score(priority, wait_ticks, aging_rate):
    return _base_priority_score(priority) + (wait_ticks * aging_rate.get(priority, 0.0))


def _pop_best_pipeline(s):
    # Choose the best head-of-queue pipeline across priorities, based on priority + aging.
    best_pr = None
    best_score = None

    for pr, q in s.queues.items():
        if not q:
            continue
        enqueue_ts, _ = q[0]
        wait_ticks = max(0, s.timestep - enqueue_ts)
        score = _effective_pipeline_score(pr, wait_ticks, s.aging_rate)
        if best_score is None or score > best_score:
            best_score = score
            best_pr = pr

    if best_pr is None:
        return None

    _, pipeline = s.queues[best_pr].pop(0)
    return pipeline


def _requeue_pipeline_front(s, pipeline, original_enqueue_ts=None):
    pr = pipeline.priority
    ts = original_enqueue_ts if original_enqueue_ts is not None else s.timestep
    s.queues[pr].insert(0, (ts, pipeline))


def _requeue_pipeline_back(s, pipeline, original_enqueue_ts=None):
    pr = pipeline.priority
    ts = original_enqueue_ts if original_enqueue_ts is not None else s.timestep
    s.queues[pr].append((ts, pipeline))


@register_scheduler(key="scheduler_low_032")
def scheduler_low_032(s, results, pipelines):
    """
    Scheduler step:
      1) Ingest new pipelines into per-priority queues.
      2) Learn from execution results: on failure, raise RAM hints for the involved ops.
      3) For each pool, greedily place ready operators by selecting pipelines using
         priority + aging, and sizing resources using caps + learned RAM hints.
    """
    s.timestep += 1

    # 1) Enqueue new arrivals
    for p in pipelines:
        if p.pipeline_id not in s.known_pipeline_ids:
            s.known_pipeline_ids.add(p.pipeline_id)
            s.queues[p.priority].append((s.timestep, p))

    # 2) Learn from results (especially OOM)
    if results:
        for r in results:
            if not r.failed():
                continue

            oom = _is_oom_error(getattr(r, "error", None))
            pool = s.executor.pools[r.pool_id]
            prev_ram = float(getattr(r, "ram", 0.0) or 0.0)

            # Increase factor: OOM -> more aggressive; other failure -> mild bump
            bump_factor = 2.0 if oom else 1.25
            floor_ram = max(prev_ram * bump_factor, prev_ram + (0.10 * pool.max_ram_pool))

            # Cap at pool max; avoid zero
            new_hint = max(0.0, min(float(pool.max_ram_pool), floor_ram))

            try:
                ops = list(getattr(r, "ops", []) or [])
            except Exception:
                ops = []

            # If ops list missing, we can't learn per-op; just skip.
            for op in ops:
                k = (getattr(r, "pipeline_id", None), _op_key(op))
                # Some ExecutionResult may not carry pipeline_id; fall back to op_key only.
                if k[0] is None:
                    k = ("_", _op_key(op))

                s.op_fail_count[k] = s.op_fail_count.get(k, 0) + 1

                # Progressive increase on repeated failures
                progressive = 1.0 + min(0.50, 0.10 * s.op_fail_count[k])
                hinted = min(float(pool.max_ram_pool), new_hint * progressive)

                old = s.op_ram_hint.get(k, 0.0)
                s.op_ram_hint[k] = max(old, hinted)

    # Early exit if nothing changed
    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    # 3) Per-pool placement
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)

        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # Greedily schedule multiple ops into this pool as long as resources remain.
        # Use a bounded loop to avoid thrashing if nothing fits.
        attempts = 0
        max_attempts = 64

        while attempts < max_attempts and avail_cpu > 0 and avail_ram > 0:
            attempts += 1

            pipeline = _pop_best_pipeline(s)
            if pipeline is None:
                break

            # If completed or already has terminal failures, drop it from consideration.
            if _pipeline_done_or_failed(pipeline):
                continue

            status = pipeline.runtime_status()
            op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
            if not op_list:
                # Not ready; requeue to the back without resetting its original arrival time too much.
                _requeue_pipeline_back(s, pipeline, original_enqueue_ts=s.timestep - 1)
                continue

            op = op_list[0]

            # Determine RAM requirement from:
            #   - learned hint for (pipeline_id, op)
            #   - operator's declared minimum RAM (if present)
            opk = (pipeline.pipeline_id, _op_key(op))
            hint_ram = float(s.op_ram_hint.get(opk, 0.0))
            min_ram = float(_extract_min_ram(op))
            required_ram = max(min_ram, hint_ram, 0.0)

            # Apply per-priority caps (to preserve concurrency), but never below required.
            cap_ram = float(pool.max_ram_pool) * float(s.ram_cap_frac.get(pipeline.priority, 0.50))
            target_ram = max(required_ram, min(avail_ram, cap_ram))
            target_ram = min(target_ram, avail_ram)

            # If we can't satisfy required RAM in this pool right now, requeue and stop packing this pool.
            if required_ram > avail_ram + 1e-9:
                _requeue_pipeline_front(s, pipeline, original_enqueue_ts=s.timestep - 1)
                break

            # CPU sizing: cap by priority, but never allocate > available.
            cap_cpu = float(pool.max_cpu_pool) * float(s.cpu_cap_frac.get(pipeline.priority, 0.35))
            target_cpu = min(avail_cpu, max(1.0, cap_cpu))

            # If we have RAM but essentially no CPU, stop.
            if target_cpu <= 0:
                _requeue_pipeline_front(s, pipeline, original_enqueue_ts=s.timestep - 1)
                break

            assignments.append(
                Assignment(
                    ops=op_list,
                    cpu=target_cpu,
                    ram=target_ram,
                    priority=pipeline.priority,
                    pool_id=pool_id,
                    pipeline_id=pipeline.pipeline_id,
                )
            )

            # Locally decrement available resources to allow multi-assign packing per pool.
            avail_cpu -= target_cpu
            avail_ram -= target_ram

            # Requeue pipeline so its next operators can be picked up later.
            _requeue_pipeline_back(s, pipeline, original_enqueue_ts=s.timestep - 1)

    return suspensions, assignments
