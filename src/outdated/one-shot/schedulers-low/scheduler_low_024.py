# policy_key: scheduler_low_024
# reasoning_effort: low
# model: gpt-5.2-2025-12-11
# llm_cost: 0.043248
# generation_seconds: 64.11
# generated_at: 2026-03-14T02:20:59.788272
@register_scheduler_init(key="scheduler_low_024")
def scheduler_low_024_init(s):
    """
    Priority-aware, low-risk improvement over naive FIFO.

    Key ideas (kept intentionally simple):
    - Maintain separate FIFO queues per priority (QUERY, INTERACTIVE, BATCH_PIPELINE).
    - Prefer scheduling higher priority first to improve tail latency.
    - Fill each pool with multiple assignments per tick (instead of 1 op per pool).
    - Conservative, pipeline-scoped RAM "guess" with OOM-based backoff:
        if an op OOMs, retry later with higher RAM (up to pool max).
    - Mild fairness: periodically allow a batch op through even if higher priority exists.
    """
    # Per-priority FIFO queues of pipelines
    s.queues = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }

    # Pipeline resource guesses and bookkeeping
    # s.pipe_state[pipeline_id] = {
    #   "ram_guess": float,
    #   "cpu_guess": float,
    #   "non_oom_failures": int,
    # }
    s.pipe_state = {}

    # Soft fairness knobs
    s._tick = 0
    s._batch_every = 8  # every N assignments, let one batch through if available
    s._assignments_since_batch = 0

    # RAM backoff knobs
    s._ram_init_frac = 0.25
    s._ram_backoff_mult = 2.0
    s._ram_max_frac = 0.95

    # CPU sizing knobs (fractions of pool max; also capped by availability)
    s._cpu_frac_by_pri = {
        Priority.QUERY: 0.75,
        Priority.INTERACTIVE: 0.60,
        Priority.BATCH_PIPELINE: 0.40,
    }


def _is_oom_error(err) -> bool:
    if not err:
        return False
    e = str(err).lower()
    return ("oom" in e) or ("out of memory" in e) or ("memory" in e and "killed" in e)


def _ensure_pipe_state(s, pipeline, pool):
    st = s.pipe_state.get(pipeline.pipeline_id)
    if st is None:
        # Start with a conservative RAM guess; CPU guess will be per-pool at assignment time.
        ram_guess = max(1.0, pool.max_ram_pool * s._ram_init_frac)
        s.pipe_state[pipeline.pipeline_id] = {
            "ram_guess": ram_guess,
            "cpu_guess": None,
            "non_oom_failures": 0,
        }
    else:
        # Keep within pool bounds if pool sizes vary.
        st["ram_guess"] = min(st["ram_guess"], pool.max_ram_pool * s._ram_max_frac)


def _pick_queue_order(s):
    # Base order: strict priority
    base = [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]

    # Mild fairness: if we've made many assignments without giving batch a chance,
    # temporarily allow batch to be considered earlier.
    if s._assignments_since_batch >= s._batch_every:
        return [Priority.BATCH_PIPELINE, Priority.QUERY, Priority.INTERACTIVE]
    return base


def _pop_next_runnable_pipeline(s, pri, require_parents_complete=True):
    """
    Pop and return the next pipeline from s.queues[pri] that currently has at least one
    assignable op (parents complete). Pipelines that aren't ready are rotated to back.
    Returns None if no runnable pipeline exists.
    """
    q = s.queues[pri]
    if not q:
        return None

    # Scan bounded by queue length to avoid infinite loops.
    for _ in range(len(q)):
        p = q.pop(0)
        status = p.runtime_status()

        # Drop terminal pipelines
        if status.is_pipeline_successful():
            continue
        if status.state_counts[OperatorState.FAILED] > 0:
            # Failed ops exist; we only retry if failures are due to OOM (handled via results).
            # If there are FAILED ops and we didn't mark it as retryable, treat as terminal.
            st = s.pipe_state.get(p.pipeline_id, {})
            if st.get("non_oom_failures", 0) > 0:
                continue

        ops = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=require_parents_complete)
        if ops:
            return p

        # Not runnable now; rotate
        q.append(p)

    return None


@register_scheduler(key="scheduler_low_024")
def scheduler_low_024(s, results: List["ExecutionResult"], pipelines: List["Pipeline"]) -> Tuple[List["Suspend"], List["Assignment"]]:
    """
    Scheduler step:
    - Ingest new pipelines into per-priority queues.
    - Process execution results to adjust RAM guesses on OOM and to stop retrying on non-OOM failures.
    - For each pool, repeatedly assign ready ops while resources remain:
        prioritize QUERY > INTERACTIVE > BATCH (with mild batch fairness).
        choose CPU/RAM sizes based on pool capacity and per-pipeline RAM guess.
    """
    s._tick += 1

    # Enqueue new pipelines
    for p in pipelines:
        pri = p.priority
        if pri not in s.queues:
            # Unknown priorities treated as batch-like
            pri = Priority.BATCH_PIPELINE
        s.queues[pri].append(p)

    # Process results to update backoff state
    # Note: results may include successes and failures; we only need failures to tune guesses.
    for r in results:
        # Attempt to find pipeline state by pipeline id is not available in result,
        # so we infer based on ops' pipeline_id when possible. If not possible, ignore.
        # The simulator's ExecutionResult in the provided context doesn't expose pipeline_id,
        # so we conservatively update based on last-known running pipeline via our queues only.
        # However, we can still use OOM signals to generally avoid repeated small RAM by
        # matching on the op's owning pipeline if the op has pipeline_id attribute.
        if not getattr(r, "failed", None):
            continue
        if not r.failed():
            continue

        # Derive pipeline_id from the first op, if available
        pipeline_id = None
        if getattr(r, "ops", None):
            try:
                op0 = r.ops[0]
                pipeline_id = getattr(op0, "pipeline_id", None)
            except Exception:
                pipeline_id = None

        if pipeline_id is None:
            continue

        st = s.pipe_state.get(pipeline_id)
        if st is None:
            continue

        if _is_oom_error(getattr(r, "error", None)):
            # Increase RAM guess for future attempts
            pool = s.executor.pools[r.pool_id]
            new_guess = max(st["ram_guess"] * s._ram_backoff_mult, (r.ram or st["ram_guess"]) * s._ram_backoff_mult)
            st["ram_guess"] = min(new_guess, pool.max_ram_pool * s._ram_max_frac)
        else:
            # Non-OOM failure: stop retrying (treat pipeline as terminal going forward).
            st["non_oom_failures"] = st.get("non_oom_failures", 0) + 1

    # If nothing changed, avoid work
    if not pipelines and not results:
        return [], []

    suspensions: List["Suspend"] = []
    assignments: List["Assignment"] = []

    # For each pool, fill resources with multiple ops per tick.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool

        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # Continue assigning while we can fit at least a small container
        # (we assume cpu/ram are continuous in the simulator; keep a minimal threshold).
        min_cpu = 1.0
        min_ram = 1.0

        # Use global priority order with mild fairness
        pri_order = _pick_queue_order(s)

        made_progress = True
        while made_progress and avail_cpu >= min_cpu and avail_ram >= min_ram:
            made_progress = False

            # Find a runnable pipeline among priorities
            chosen_p = None
            chosen_pri = None
            for pri in pri_order:
                p = _pop_next_runnable_pipeline(s, pri, require_parents_complete=True)
                if p is not None:
                    chosen_p = p
                    chosen_pri = pri
                    break

            if chosen_p is None:
                break

            # Ensure state exists and compute resource request
            _ensure_pipe_state(s, chosen_p, pool)
            st = s.pipe_state[chosen_p.pipeline_id]

            # If pipeline has recorded non-OOM failures, don't schedule it anymore.
            if st.get("non_oom_failures", 0) > 0:
                continue

            status = chosen_p.runtime_status()
            op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
            if not op_list:
                # Not actually runnable anymore; requeue and continue
                s.queues[chosen_pri].append(chosen_p)
                continue

            # CPU sizing: give higher priority more CPU but avoid grabbing all resources.
            cpu_target = pool.max_cpu_pool * s._cpu_frac_by_pri.get(chosen_pri, 0.40)
            cpu_req = min(avail_cpu, max(min_cpu, cpu_target))

            # RAM sizing: use backoff guess; cap to available and pool max fraction.
            ram_cap = pool.max_ram_pool * s._ram_max_frac
            ram_req = min(avail_ram, max(min_ram, min(st["ram_guess"], ram_cap)))

            # If we can't fit even the guessed RAM, try a smaller (but still >0) slice;
            # this is best-effort to utilize leftovers. Risk: might OOM; backoff will correct.
            if ram_req < min_ram or cpu_req < min_cpu:
                # Not enough room; put pipeline back and stop filling this pool
                s.queues[chosen_pri].append(chosen_p)
                break

            # Issue assignment
            assignments.append(
                Assignment(
                    ops=op_list,
                    cpu=cpu_req,
                    ram=ram_req,
                    priority=chosen_p.priority,
                    pool_id=pool_id,
                    pipeline_id=chosen_p.pipeline_id,
                )
            )

            # Update accounting and requeue pipeline for future ops
            avail_cpu -= cpu_req
            avail_ram -= ram_req
            made_progress = True

            if chosen_pri == Priority.BATCH_PIPELINE:
                s._assignments_since_batch = 0
            else:
                s._assignments_since_batch += 1

            # Keep pipeline in its queue if it still has work (most do).
            # If it completed synchronously (unlikely), runtime_status() will drop it next scan.
            s.queues[chosen_pri].append(chosen_p)

    return suspensions, assignments
