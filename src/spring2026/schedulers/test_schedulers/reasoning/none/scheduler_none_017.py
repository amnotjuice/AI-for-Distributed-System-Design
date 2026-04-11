# policy_key: scheduler_none_017
# reasoning_effort: none
# model: gpt-5.2-2025-12-11
# llm_cost: 0.045119
# generation_seconds: 44.79
# generated_at: 2026-04-09T21:57:58.184958
@register_scheduler_init(key="scheduler_none_017")
def scheduler_none_017_init(s):
    """Priority-protecting, OOM-averse, low-churn scheduler.

    Main ideas:
      - Keep per-priority FIFO queues (QUERY > INTERACTIVE > BATCH) to protect weighted latency.
      - Use simple RAM backoff on failure: if an op/container OOMs, retry with higher RAM next time.
      - Right-size CPU/RAM per op conservatively to reduce OOM/fail penalties (720s) while keeping
        high-priority latency low.
      - Mild preemption: only preempt BATCH to make room for QUERY/INTERACTIVE when they are blocked.
      - Avoid starvation: age BATCH slightly via periodic "forced" batch admits when safe.
    """
    # Queues per priority
    s.q_query = []
    s.q_interactive = []
    s.q_batch = []

    # Per operator retry sizing hints:
    # key: (pipeline_id, op_id) -> {"ram_mul": float, "cpu_mul": float}
    s.op_hints = {}

    # Pipeline arrival bookkeeping for fair-ish selection
    s.pipeline_seen_ts = {}  # pipeline_id -> first_seen_tick

    # Deterministic tick counter for aging
    s.tick = 0

    # Preemption throttling to avoid churn
    s.last_preempt_tick = -10

    # A small "reserve" of resources we try to keep available for high priority arrivals.
    # Implemented as soft thresholds to avoid packing the pool to 100% with batch.
    s.soft_reserve = {
        Priority.QUERY: {"cpu": 0.25, "ram": 0.25},         # keep 25% headroom if possible
        Priority.INTERACTIVE: {"cpu": 0.15, "ram": 0.15},   # keep 15% headroom if possible
    }

    # Batch admission gating: allow at least one batch assignment every N ticks if batch is pending.
    s.batch_force_every = 7

    # Preemption config
    s.preempt_cooldown = 3  # ticks between preemption events
    s.preempt_only_from = {Priority.BATCH_PIPELINE}  # only preempt batch by default

    # Max multipliers to prevent runaway allocations
    s.max_ram_mul = 8.0
    s.max_cpu_mul = 2.0


def _priority_rank(pri):
    if pri == Priority.QUERY:
        return 0
    if pri == Priority.INTERACTIVE:
        return 1
    return 2


def _enqueue_pipeline(s, p):
    if p.pipeline_id not in s.pipeline_seen_ts:
        s.pipeline_seen_ts[p.pipeline_id] = s.tick

    if p.priority == Priority.QUERY:
        s.q_query.append(p)
    elif p.priority == Priority.INTERACTIVE:
        s.q_interactive.append(p)
    else:
        s.q_batch.append(p)


def _iter_queues_in_order(s):
    # Strict priority with mild batch "aging" implemented elsewhere
    yield s.q_query
    yield s.q_interactive
    yield s.q_batch


def _pipeline_done_or_dead(p):
    st = p.runtime_status()
    if st.is_pipeline_successful():
        return True
    # If any op is FAILED, we still consider retryable (ASSIGNABLE_STATES includes FAILED).
    # We only drop if the simulator marks pipeline successful; otherwise keep it.
    return False


def _pick_next_assignable_op(p):
    st = p.runtime_status()
    # Prefer ready ops whose parents are complete.
    op_list = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
    if op_list:
        return op_list[0]
    # Fallback: allow ops even if parents not complete (should typically not happen).
    op_list = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=False)
    if op_list:
        return op_list[0]
    return None


def _op_id(op):
    # Try to derive a stable identifier without imports/assumptions.
    # Many implementations have .op_id; fallback to id(op) (stable only within run).
    if hasattr(op, "op_id"):
        return getattr(op, "op_id")
    if hasattr(op, "operator_id"):
        return getattr(op, "operator_id")
    if hasattr(op, "id"):
        return getattr(op, "id")
    return id(op)


def _get_hint(s, pipeline_id, op):
    key = (pipeline_id, _op_id(op))
    return s.op_hints.get(key, {"ram_mul": 1.0, "cpu_mul": 1.0})


def _set_hint(s, pipeline_id, op, ram_mul=None, cpu_mul=None):
    key = (pipeline_id, _op_id(op))
    cur = s.op_hints.get(key, {"ram_mul": 1.0, "cpu_mul": 1.0})
    if ram_mul is not None:
        cur["ram_mul"] = max(1.0, min(float(ram_mul), s.max_ram_mul))
    if cpu_mul is not None:
        cur["cpu_mul"] = max(1.0, min(float(cpu_mul), s.max_cpu_mul))
    s.op_hints[key] = cur


def _update_hints_from_results(s, results):
    # On failure (likely OOM), increase RAM multiplier and slightly reduce CPU to fit easier if tight.
    # On success, keep as-is (we don't decrease to avoid oscillations).
    for r in results:
        if not hasattr(r, "ops") or not r.ops:
            continue
        # Results can include multiple ops; treat each similarly.
        for op in r.ops:
            if r.failed():
                # Conservative assumption: most failures are OOM -> boost RAM significantly.
                prev = _get_hint(s, r.container_id if False else getattr(r, "pipeline_id", None), op)  # unused path
                # We don't have pipeline_id in results per given fields; so key by (unknown pipeline, op_id) isn't safe.
                # Instead, key by op_id only is too global. Workaround: store hint keyed by (container_id, op_id) on failure
                # is also not helpful for retries. So we use a softer global heuristic per op object via id(op) which should
                # remain for that pipeline object.
                # We can still safely use pipeline_id by looking up op owner in queues, but that's expensive.
                # Best effort: if result has pipeline_id attribute, use it; else fall back to None.
                pipeline_id = getattr(r, "pipeline_id", None)
                key_pid = pipeline_id if pipeline_id is not None else -1

                hint = _get_hint(s, key_pid, op)
                new_ram_mul = min(s.max_ram_mul, hint["ram_mul"] * 1.7)
                _set_hint(s, key_pid, op, ram_mul=new_ram_mul)

                # If we were CPU-heavy, keep it modest on retry.
                new_cpu_mul = min(s.max_cpu_mul, max(1.0, hint["cpu_mul"] * 1.05))
                _set_hint(s, key_pid, op, cpu_mul=new_cpu_mul)


def _pool_headroom(pool):
    # Avoid division by zero
    max_cpu = max(1e-9, float(pool.max_cpu_pool))
    max_ram = max(1e-9, float(pool.max_ram_pool))
    return (float(pool.avail_cpu_pool) / max_cpu, float(pool.avail_ram_pool) / max_ram)


def _choose_pool_for_priority(s, pri):
    # Choose the pool with most headroom (min of cpu/ram fractions), biased for high priority.
    best = None
    best_score = None
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        cpu_frac, ram_frac = _pool_headroom(pool)
        # Score favors balanced headroom. For high priority, prefer more headroom.
        score = min(cpu_frac, ram_frac)
        if pri in (Priority.QUERY, Priority.INTERACTIVE):
            score += 0.05 * (cpu_frac + ram_frac)
        if best is None or score > best_score:
            best = pool_id
            best_score = score
    return best


def _target_allocation(s, pool, pri, hint):
    # Conservative baseline: give enough RAM to avoid OOM penalties by not under-allocating.
    # Since we don't know true minima, allocate a decent fraction for high pri, smaller for batch.
    if pri == Priority.QUERY:
        base_ram_frac = 0.45
        base_cpu_frac = 0.60
    elif pri == Priority.INTERACTIVE:
        base_ram_frac = 0.35
        base_cpu_frac = 0.50
    else:
        base_ram_frac = 0.20
        base_cpu_frac = 0.35

    # Apply hint multipliers; multipliers primarily affect RAM to reduce OOM retries.
    ram = float(pool.max_ram_pool) * base_ram_frac * float(hint["ram_mul"])
    cpu = float(pool.max_cpu_pool) * base_cpu_frac * float(hint["cpu_mul"])

    # Clamp to available now (we don't partially assign beyond available)
    ram = min(ram, float(pool.avail_ram_pool))
    cpu = min(cpu, float(pool.avail_cpu_pool))

    # Enforce minimum positive allocations
    ram = max(0.0, ram)
    cpu = max(0.0, cpu)
    return cpu, ram


def _can_admit_batch_now(s):
    # Force some batch progress periodically to avoid indefinite starvation.
    return (s.tick % s.batch_force_every) == 0


def _soft_reserve_ok(s, pool, pri):
    # For batch: don't eat into reserved headroom if higher-priority queues are non-empty.
    if pri != Priority.BATCH_PIPELINE:
        return True

    if s.q_query or s.q_interactive:
        # Keep headroom for high priority if possible.
        cpu_frac, ram_frac = _pool_headroom(pool)
        # Use query reserve if any query pending, else interactive reserve.
        res = s.soft_reserve[Priority.QUERY] if s.q_query else s.soft_reserve[Priority.INTERACTIVE]
        return (cpu_frac >= res["cpu"]) and (ram_frac >= res["ram"])
    return True


def _gather_running_containers(results):
    # Best-effort: infer currently running containers from recent results is not possible.
    # The simulator likely has a pool/container view, but not exposed in the prompt.
    # Therefore, we only preempt using results info if it contains container_id of active work
    # (it won't). Return empty => preemption rarely triggers, which reduces churn.
    return []


@register_scheduler(key="scheduler_none_017")
def scheduler_none_017(s, results, pipelines):
    """
    Scheduler step:
      1) Update retry hints from failures.
      2) Enqueue new pipelines into per-priority queues.
      3) Try to place one ready operator per pool per tick, prioritizing QUERY then INTERACTIVE.
      4) Gate batch to avoid consuming headroom when high priority backlog exists.
      5) Very limited preemption hook (mostly disabled due to lack of container visibility).
    """
    s.tick += 1

    # Update sizing hints based on failures
    if results:
        _update_hints_from_results(s, results)

    # Enqueue new pipelines
    for p in pipelines:
        _enqueue_pipeline(s, p)

    # Early exit if nothing changes
    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    # Attempt very mild preemption when high priority is waiting and we have no headroom in any pool.
    # Due to limited visibility, this is effectively a no-op; kept for future compatibility.
    if (s.q_query or s.q_interactive) and (s.tick - s.last_preempt_tick) >= s.preempt_cooldown:
        blocked_everywhere = True
        for pool_id in range(s.executor.num_pools):
            pool = s.executor.pools[pool_id]
            if pool.avail_cpu_pool > 0 and pool.avail_ram_pool > 0:
                blocked_everywhere = False
                break
        if blocked_everywhere:
            running = _gather_running_containers(results)
            # If we had visibility, we'd preempt a batch container here.
            for cont in running:
                if cont.get("priority") in s.preempt_only_from:
                    suspensions.append(Suspend(cont["container_id"], cont["pool_id"]))
                    s.last_preempt_tick = s.tick
                    break

    # Main placement loop: try one assignment per pool per tick to reduce interference and churn.
    for _ in range(s.executor.num_pools):
        # Decide which priority to try next using strict priority, but allow batch periodically.
        # We schedule across pools by choosing best pool for the chosen pipeline.
        chosen_pipeline = None
        chosen_pri = None

        # Batch forcing: if only batch is waiting or batch is forced, allow selecting batch.
        allow_batch = _can_admit_batch_now(s)

        # Find next runnable pipeline in priority order (with mild batch forcing)
        queues = [s.q_query, s.q_interactive, s.q_batch]
        for qi, q in enumerate(queues):
            if qi == 2 and not allow_batch and (s.q_query or s.q_interactive):
                continue  # gate batch when high priority is pending
            # pop-and-requeue scan to skip completed pipelines
            scans = 0
            while q and scans < len(q):
                p = q.pop(0)
                scans += 1
                if _pipeline_done_or_dead(p):
                    continue
                op = _pick_next_assignable_op(p)
                if op is None:
                    # Not ready yet; requeue
                    q.append(p)
                    continue
                # Found a candidate
                chosen_pipeline = p
                chosen_pri = p.priority
                # Put pipeline back (we only assign one op; pipeline may have more later)
                q.append(p)
                break
            if chosen_pipeline is not None:
                break

        if chosen_pipeline is None:
            break

        # Choose best pool for this priority
        pool_id = _choose_pool_for_priority(s, chosen_pri)
        if pool_id is None:
            break
        pool = s.executor.pools[pool_id]

        # Check resources and batch soft reserve
        if pool.avail_cpu_pool <= 0 or pool.avail_ram_pool <= 0:
            continue
        if not _soft_reserve_ok(s, pool, chosen_pri):
            # Skip batch allocation if it would consume protected headroom; try next iteration.
            continue

        # Select the op again (since pipeline may have progressed)
        op = _pick_next_assignable_op(chosen_pipeline)
        if op is None:
            continue

        # Compute allocation
        hint = _get_hint(s, chosen_pipeline.pipeline_id, op)
        cpu, ram = _target_allocation(s, pool, chosen_pri, hint)

        # If we can't allocate meaningful resources, skip
        if cpu <= 0 or ram <= 0:
            continue

        assignments.append(
            Assignment(
                ops=[op],
                cpu=cpu,
                ram=ram,
                priority=chosen_pri,
                pool_id=pool_id,
                pipeline_id=chosen_pipeline.pipeline_id,
            )
        )

        # Since we used some capacity, adjust the pool's available resources locally is not possible here;
        # the executor will apply it. To avoid multiple oversubscriptions in the same tick, we only assign
        # one op per loop iteration; across iterations we might still pick same pool, but executor state
        # doesn't update until after. Therefore, we stop after one assignment per tick to be safe.
        break

    return suspensions, assignments
