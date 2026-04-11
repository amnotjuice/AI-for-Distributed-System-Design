# policy_key: scheduler_high_012
# reasoning_effort: high
# exp: reasoning
# model: gpt-5.2-2025-12-11
# llm_cost: 0.138709
# generation_seconds: 163.39
# generated_at: 2026-04-10T00:50:17.659958
@register_scheduler_init(key="scheduler_high_012")
def scheduler_high_012_init(s):
    """Priority-aware, failure-averse scheduler.

    Main ideas:
    - Strictly prioritize QUERY then INTERACTIVE then BATCH to minimize weighted latency.
    - Avoid OOM-driven failures by using conservative default RAM fractions and fast RAM backoff on suspected OOM.
    - Keep throughput/fairness via (a) limited per-pipeline parallelism, (b) aging for long-waiting batch,
      and (c) small headroom reservation so high-priority arrivals don't queue behind batch.
    - Optional best-effort preemption (if the simulator exposes running container metadata on pools).
    """
    s.tick = 0

    # Active pipelines and arrival ticks (for aging / fairness).
    s.active = {}  # pipeline_id -> Pipeline
    s.arrival_tick = {}  # pipeline_id -> tick

    # Per-operator resource hints and retry tracking (to reduce failures and retries).
    # Keys are best-effort fingerprints derived from operator objects.
    s.op_ram_hint = {}   # fp -> last_known_good_or_needed_ram
    s.op_cpu_hint = {}   # fp -> last_known_good_or_needed_cpu
    s.op_retries = {}    # fp -> consecutive failures (or total failures)

    # Preemption throttling to reduce churn.
    s.last_preempt_tick = -10**9
    s.preempt_cooldown_ticks = 5

    # Safety knobs.
    s.max_retries_per_op = 4

    # Per-priority inflight limits (ops assigned/running per pipeline).
    s.inflight_limit = {
        Priority.QUERY: 3,
        Priority.INTERACTIVE: 2,
        Priority.BATCH_PIPELINE: 1,
    }


def _is_oom_error(err) -> bool:
    if not err:
        return False
    msg = str(err).lower()
    return ("oom" in msg) or ("out of memory" in msg) or ("memoryerror" in msg) or ("killed" in msg and "memory" in msg)


def _op_fingerprint(op) -> str:
    # Try stable identifiers first; fall back to string form.
    for attr in ("op_id", "operator_id", "id", "node_id", "name"):
        v = getattr(op, attr, None)
        if v is not None:
            return f"{attr}:{v}"
    # If the operator is a dataclass-like object, repr may include fields; otherwise it's still usable within a run.
    try:
        return f"repr:{repr(op)}"
    except Exception:
        return f"str:{str(op)}"


def _remaining_ops_estimate(status) -> int:
    # Use state_counts if available; otherwise default to 1.
    sc = getattr(status, "state_counts", None)
    if not sc:
        return 1
    total = 0
    for st in (OperatorState.PENDING, OperatorState.ASSIGNED, OperatorState.RUNNING, OperatorState.SUSPENDING, OperatorState.FAILED):
        try:
            total += sc.get(st, 0)
        except Exception:
            pass
    return max(1, total)


def _inflight_count(status) -> int:
    sc = getattr(status, "state_counts", None)
    if not sc:
        return 0
    try:
        return int(sc.get(OperatorState.ASSIGNED, 0)) + int(sc.get(OperatorState.RUNNING, 0)) + int(sc.get(OperatorState.SUSPENDING, 0))
    except Exception:
        return 0


def _priority_base(p: Priority) -> float:
    if p == Priority.QUERY:
        return 3.0
    if p == Priority.INTERACTIVE:
        return 2.0
    return 1.0


def _aged_priority(base: float, prio: Priority, age_ticks: int) -> float:
    # Aging helps avoid 720s penalties for long-waiting background work without letting batch dominate.
    if prio == Priority.BATCH_PIPELINE:
        # After ~200 ticks, batch can compete with interactive; cap at +1.6.
        return base + min(1.6, age_ticks / 200.0)
    if prio == Priority.INTERACTIVE:
        # Mild aging; cap at +0.6.
        return base + min(0.6, age_ticks / 500.0)
    # Queries already dominate; no extra aging needed.
    return base


def _default_fracs(prio: Priority):
    # Conservative defaults: avoid OOMs (especially for high priority) but keep some concurrency.
    if prio == Priority.QUERY:
        return (0.55, 0.30)  # (cpu_frac, ram_frac)
    if prio == Priority.INTERACTIVE:
        return (0.45, 0.25)
    return (0.30, 0.20)


def _clamp(x, lo, hi):
    return max(lo, min(hi, x))


def _choose_pool_order(num_pools: int, prio: Priority):
    if num_pools <= 1:
        return [0]
    # Prefer pool 0 for query/interactive to reduce interference; steer batch to other pools first.
    if prio in (Priority.QUERY, Priority.INTERACTIVE):
        return [0] + [i for i in range(1, num_pools)]
    return [i for i in range(1, num_pools)] + [0]


def _iter_pool_containers(pool):
    """
    Best-effort container enumeration for optional preemption.
    Returns iterable of container-like objects with (container_id, priority, cpu, ram) if present.
    """
    # Common patterns we might encounter:
    # - pool.containers: dict[container_id] -> container
    # - pool.containers: list[container]
    # - pool.running_containers: list/dict
    for attr in ("containers", "running_containers", "assigned_containers"):
        obj = getattr(pool, attr, None)
        if obj is None:
            continue
        if isinstance(obj, dict):
            for k, v in obj.items():
                # If container_id isn't on the object, we can use the dict key.
                if getattr(v, "container_id", None) is None:
                    try:
                        setattr(v, "container_id", k)
                    except Exception:
                        pass
                yield v
        elif isinstance(obj, (list, tuple, set)):
            for v in obj:
                yield v


def _container_priority(c):
    return getattr(c, "priority", getattr(c, "prio", None))


def _container_id(c):
    return getattr(c, "container_id", getattr(c, "id", None))


def _container_cpu(c):
    return getattr(c, "cpu", getattr(c, "vcpus", None))


def _container_ram(c):
    return getattr(c, "ram", getattr(c, "memory", None))


@register_scheduler(key="scheduler_high_012")
def scheduler_high_012(s, results: list, pipelines: list):
    """
    Scheduler step:
    - Update hints from results (esp. OOM backoff).
    - Build ready candidates with aging + SRPT-ish tiebreaking.
    - Greedily assign ops pool-by-pool in strict priority order,
      with small headroom reservation to protect tail latency.
    - Optional: preempt one large batch container if a query can't be admitted.
    """
    s.tick += 1

    # Admit new pipelines.
    for p in pipelines:
        pid = p.pipeline_id
        if pid not in s.active:
            s.active[pid] = p
            s.arrival_tick[pid] = s.tick
        else:
            # If re-sent by generator, refresh stored object.
            s.active[pid] = p

    # Update operator hints from execution results.
    for r in results:
        # Update per-op hints based on success/failure.
        try:
            ops = r.ops or []
        except Exception:
            ops = []
        failed = False
        try:
            failed = r.failed()
        except Exception:
            failed = bool(getattr(r, "error", None))

        for op in ops:
            fp = _op_fingerprint(op)
            if failed:
                s.op_retries[fp] = int(s.op_retries.get(fp, 0)) + 1
                if _is_oom_error(getattr(r, "error", None)):
                    # Fast RAM backoff for suspected OOM.
                    prev = float(s.op_ram_hint.get(fp, 0.0) or 0.0)
                    try:
                        new_hint = float(r.ram) * 1.6
                    except Exception:
                        new_hint = prev * 1.6 if prev > 0 else 0.0
                    s.op_ram_hint[fp] = max(prev, new_hint)
                else:
                    # Non-OOM: try a small CPU bump (won't prevent failure, but may reduce long runtimes).
                    prev = float(s.op_cpu_hint.get(fp, 0.0) or 0.0)
                    try:
                        new_hint = float(r.cpu) * 1.2
                    except Exception:
                        new_hint = prev * 1.2 if prev > 0 else 0.0
                    s.op_cpu_hint[fp] = max(prev, new_hint)
            else:
                # Success: keep a "known good" lower bound.
                s.op_retries[fp] = 0
                try:
                    s.op_ram_hint[fp] = max(float(s.op_ram_hint.get(fp, 0.0) or 0.0), float(r.ram))
                except Exception:
                    pass
                try:
                    s.op_cpu_hint[fp] = max(float(s.op_cpu_hint.get(fp, 0.0) or 0.0), float(r.cpu))
                except Exception:
                    pass

    # Clean up completed pipelines and build ready candidate lists.
    to_delete = []
    candidates_by_prio = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }

    for pid, p in list(s.active.items()):
        st = p.runtime_status()
        if st.is_pipeline_successful():
            to_delete.append(pid)
            continue

        prio = p.priority
        age = s.tick - int(s.arrival_tick.get(pid, s.tick))

        # Limit per-pipeline parallelism to reduce contention and prevent one pipeline from hogging resources.
        inflight = _inflight_count(st)
        inflight_limit = int(s.inflight_limit.get(prio, 1))
        if inflight >= inflight_limit:
            continue

        # Ready-to-assign ops with parents satisfied.
        ready_ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        if not ready_ops:
            continue

        # Pick a "best" ready op for this pipeline:
        # - prefer fewer retries (avoid repeated failures),
        # - then smaller estimated RAM hint (more likely to fit quickly),
        # - otherwise first.
        best_op = None
        best_tuple = None
        for op in ready_ops:
            fp = _op_fingerprint(op)
            retries = int(s.op_retries.get(fp, 0))
            if retries > s.max_retries_per_op:
                continue
            ram_hint = float(s.op_ram_hint.get(fp, 0.0) or 0.0)
            t = (retries, ram_hint)
            if best_tuple is None or t < best_tuple:
                best_tuple = t
                best_op = op
        if best_op is None:
            continue

        remaining = _remaining_ops_estimate(st)
        base = _priority_base(prio)
        eff = _aged_priority(base, prio, age)

        # Sort key: higher effective priority first, then SRPT-ish (fewer remaining ops),
        # then older pipelines, then fewer retries.
        fp = _op_fingerprint(best_op)
        retries = int(s.op_retries.get(fp, 0))
        sort_key = (-eff, remaining, -age, retries, pid)

        candidates_by_prio[prio].append((sort_key, p, best_op))

    for pid in to_delete:
        s.active.pop(pid, None)
        s.arrival_tick.pop(pid, None)

    # Determine whether we should reserve headroom against batch (if any HP work is pending).
    hp_waiting = (len(candidates_by_prio[Priority.QUERY]) + len(candidates_by_prio[Priority.INTERACTIVE])) > 0

    # Sort each priority bucket.
    for pr in candidates_by_prio:
        candidates_by_prio[pr].sort(key=lambda x: x[0])

    suspensions = []
    assignments = []

    # Track additional assignments per pipeline this tick to respect inflight limits more tightly.
    assigned_count_this_tick = {}

    def can_assign_more(pipeline):
        pr = pipeline.priority
        limit = int(s.inflight_limit.get(pr, 1))
        pid_ = pipeline.pipeline_id
        return int(assigned_count_this_tick.get(pid_, 0)) < max(0, limit)

    def record_assigned(pipeline):
        pid_ = pipeline.pipeline_id
        assigned_count_this_tick[pid_] = int(assigned_count_this_tick.get(pid_, 0)) + 1

    def resource_request(pool, pipeline, op):
        # Returns (cpu_req, ram_req) in absolute units for this pool.
        pr = pipeline.priority
        cpu_frac, ram_frac = _default_fracs(pr)

        max_cpu = float(pool.max_cpu_pool)
        max_ram = float(pool.max_ram_pool)

        fp = _op_fingerprint(op)
        retries = int(s.op_retries.get(fp, 0))

        # Base requests by fraction of pool size.
        cpu_req = cpu_frac * max_cpu
        ram_req = ram_frac * max_ram

        # Apply hints as lower bounds where available.
        cpu_hint = float(s.op_cpu_hint.get(fp, 0.0) or 0.0)
        ram_hint = float(s.op_ram_hint.get(fp, 0.0) or 0.0)

        if cpu_hint > 0:
            cpu_req = max(cpu_req, cpu_hint)
        if ram_hint > 0:
            ram_req = max(ram_req, ram_hint)

        # Backoff after failures: strongly for RAM on OOM-like histories, mildly for CPU otherwise.
        # We don't know failure type history; retries alone increases RAM somewhat to avoid repeated failures.
        # Cap multiplier to avoid turning one op into a full-pool hog indefinitely.
        ram_mult = 1.0 + 0.35 * min(retries, 4)
        cpu_mult = 1.0 + 0.15 * min(retries, 3)
        ram_req *= ram_mult
        cpu_req *= cpu_mult

        # Enforce small minimums and pool maximums.
        cpu_req = _clamp(cpu_req, 1.0, max_cpu)
        ram_req = _clamp(ram_req, 1.0, max_ram)

        return cpu_req, ram_req

    def try_preempt_for_hp(pool_id, need_cpu, need_ram):
        # Preempt at most once per cooldown interval and only for QUERY admission.
        if (s.tick - int(s.last_preempt_tick)) < int(s.preempt_cooldown_ticks):
            return False
        pool = s.executor.pools[pool_id]

        # If we cannot enumerate containers, we can't preempt safely.
        containers = list(_iter_pool_containers(pool))
        if not containers:
            return False

        # Choose a BATCH container (lowest priority) with largest RAM footprint to free headroom quickly.
        batch_containers = []
        for c in containers:
            pr = _container_priority(c)
            cid = _container_id(c)
            if cid is None:
                continue
            if pr == Priority.BATCH_PIPELINE:
                cr = _container_ram(c)
                cc = _container_cpu(c)
                try:
                    cr = float(cr) if cr is not None else 0.0
                except Exception:
                    cr = 0.0
                try:
                    cc = float(cc) if cc is not None else 0.0
                except Exception:
                    cc = 0.0
                batch_containers.append((cr, cc, cid))
        if not batch_containers:
            return False

        # Biggest RAM first.
        batch_containers.sort(key=lambda t: (t[0], t[1]), reverse=True)
        _, _, cid = batch_containers[0]
        suspensions.append(Suspend(container_id=cid, pool_id=pool_id))
        s.last_preempt_tick = s.tick
        return True

    # Greedy scheduling in strict priority order.
    priority_order = [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]

    # Pool headroom reservation against batch, only when HP is waiting.
    # Keep modest so we don't starve batch and inflate makespan.
    def reserved_for_hp(pool, prio):
        if not hp_waiting:
            return (0.0, 0.0)
        if prio != Priority.BATCH_PIPELINE:
            return (0.0, 0.0)
        return (0.15 * float(pool.max_cpu_pool), 0.15 * float(pool.max_ram_pool))

    # For each priority class, fill pools in preferred order.
    for prio in priority_order:
        cand = candidates_by_prio[prio]
        if not cand:
            continue

        pool_ids = _choose_pool_order(s.executor.num_pools, prio)
        # For batch: if HP waiting, avoid using pool 0 unless others are saturated.
        if prio == Priority.BATCH_PIPELINE and hp_waiting and s.executor.num_pools > 1:
            pool_ids = [i for i in pool_ids if i != 0] + [0]

        # Greedy packing within each pool.
        for pool_id in pool_ids:
            pool = s.executor.pools[pool_id]
            avail_cpu = float(pool.avail_cpu_pool)
            avail_ram = float(pool.avail_ram_pool)

            if avail_cpu <= 0.0 or avail_ram <= 0.0:
                continue

            res_cpu, res_ram = reserved_for_hp(pool, prio)
            eff_avail_cpu = max(0.0, avail_cpu - res_cpu)
            eff_avail_ram = max(0.0, avail_ram - res_ram)

            # Keep trying to place candidates that fit; remove placed ones.
            i = 0
            while i < len(cand):
                _, p, op = cand[i]

                if not can_assign_more(p):
                    i += 1
                    continue

                cpu_req, ram_req = resource_request(pool, p, op)

                # For batch, enforce reservation; for HP, use full pool availability.
                cap_cpu = avail_cpu if prio != Priority.BATCH_PIPELINE else eff_avail_cpu
                cap_ram = avail_ram if prio != Priority.BATCH_PIPELINE else eff_avail_ram

                if cap_cpu < 1.0 or cap_ram < 1.0:
                    break

                if ram_req <= cap_ram and cpu_req <= cap_cpu:
                    # Fit; allocate.
                    assignments.append(
                        Assignment(
                            ops=[op],
                            cpu=min(cpu_req, cap_cpu),
                            ram=min(ram_req, cap_ram),
                            priority=p.priority,
                            pool_id=pool_id,
                            pipeline_id=p.pipeline_id,
                        )
                    )
                    record_assigned(p)

                    # Update local availability assuming assignments are applied immediately in this tick.
                    if prio != Priority.BATCH_PIPELINE:
                        avail_cpu -= cpu_req
                        avail_ram -= ram_req
                    else:
                        # Reservation-aware reduction.
                        eff_avail_cpu -= cpu_req
                        eff_avail_ram -= ram_req
                        avail_cpu = eff_avail_cpu + res_cpu
                        avail_ram = eff_avail_ram + res_ram

                    # Remove placed candidate.
                    cand.pop(i)
                    continue

                i += 1

    # Optional last-mile: if a QUERY is waiting but got no assignment and pools are tight, preempt one batch container.
    # This is deliberately conservative to avoid churn and wasted work.
    if candidates_by_prio[Priority.QUERY] and not any(a.priority == Priority.QUERY for a in assignments):
        # Try to preempt in the pool where we want queries to run first (pool 0 preferred).
        _, p0, op0 = candidates_by_prio[Priority.QUERY][0]
        preferred_pools = _choose_pool_order(s.executor.num_pools, Priority.QUERY)
        for pool_id in preferred_pools:
            pool = s.executor.pools[pool_id]
            cpu_req, ram_req = resource_request(pool, p0, op0)
            if float(pool.avail_cpu_pool) >= 1.0 and float(pool.avail_ram_pool) >= 1.0:
                # There is some capacity; if not enough for this query, try a single preemption.
                if cpu_req > float(pool.avail_cpu_pool) or ram_req > float(pool.avail_ram_pool):
                    if try_preempt_for_hp(pool_id, cpu_req, ram_req):
                        # After preemption, attempt to schedule this query immediately (optimistic).
                        assignments.append(
                            Assignment(
                                ops=[op0],
                                cpu=min(cpu_req, float(pool.max_cpu_pool)),
                                ram=min(ram_req, float(pool.max_ram_pool)),
                                priority=p0.priority,
                                pool_id=pool_id,
                                pipeline_id=p0.pipeline_id,
                            )
                        )
                break

    return suspensions, assignments
