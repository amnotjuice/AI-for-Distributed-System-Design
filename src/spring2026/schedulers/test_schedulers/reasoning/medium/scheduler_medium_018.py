# policy_key: scheduler_medium_018
# reasoning_effort: medium
# exp: reasoning
# model: gpt-5.2-2025-12-11
# llm_cost: 0.093895
# generation_seconds: 68.11
# generated_at: 2026-04-09T22:53:28.458980
@register_scheduler_init(key="scheduler_medium_018")
def scheduler_medium_018_init(s):
    """
    Priority-aware, OOM-adaptive, starvation-avoiding scheduler.

    Core ideas:
      - Strictly protect QUERY then INTERACTIVE latency by scheduling them first and reserving headroom.
      - Avoid 720s penalties by never dropping pipelines and by adapting RAM sizing upward on failures.
      - Maintain fairness via aging so BATCH makes progress when it has waited long enough.
      - Conservative batch packing to reduce interference with high-priority work.
    """
    s.tick = 0

    # Per-priority FIFO queues of pipeline_ids
    s.queues = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }

    # Canonical pipeline object by id
    s.pipelines_by_id = {}

    # Metadata per pipeline for adaptive sizing and aging
    # meta[pipeline_id] = {
    #   "priority": Priority,
    #   "enqueue_tick": int,
    #   "ram_frac": float in (0,1],   # fraction of pool max_ram to request
    #   "cpu_frac": float in (0,1],   # fraction of pool max_cpu to request
    #   "failures": int,
    #   "last_failure_tick": int,
    # }
    s.meta = {}

    # Round-robin cursor per priority to avoid rescanning from the head forever
    s.cursor = {
        Priority.QUERY: 0,
        Priority.INTERACTIVE: 0,
        Priority.BATCH_PIPELINE: 0,
    }

    # Tuning knobs (kept simple and safe)
    s.hp_reserve_frac_cpu = 0.20  # keep this fraction free when HP is waiting
    s.hp_reserve_frac_ram = 0.20
    s.batch_max_frac_cpu = 0.35   # batch never takes more than this fraction per op
    s.batch_max_frac_ram = 0.40
    s.aging_threshold_ticks = 40  # after this, batch can bypass strict priority if resources exist

    # Failure backoff multipliers (assume failures are mostly OOM)
    s.ram_backoff_mult = 1.6
    s.cpu_backoff_mult = 1.15

    # Initial sizing defaults per priority (fractions of pool capacity)
    s.init_fracs = {
        Priority.QUERY: {"cpu": 0.75, "ram": 0.55},
        Priority.INTERACTIVE: {"cpu": 0.55, "ram": 0.45},
        Priority.BATCH_PIPELINE: {"cpu": 0.25, "ram": 0.25},
    }


def _clamp(x, lo, hi):
    return lo if x < lo else hi if x > hi else x


def _queue_nonempty(s, prio):
    q = s.queues.get(prio, [])
    return len(q) > 0


def _any_high_priority_waiting(s):
    return _queue_nonempty(s, Priority.QUERY) or _queue_nonempty(s, Priority.INTERACTIVE)


def _remove_from_queue(s, prio, pipeline_id):
    q = s.queues[prio]
    # keep order stable; cost is fine at simulator scale
    s.queues[prio] = [pid for pid in q if pid != pipeline_id]
    s.cursor[prio] = 0 if not s.queues[prio] else min(s.cursor[prio], len(s.queues[prio]) - 1)


def _ensure_pipeline_registered(s, p):
    pid = p.pipeline_id
    if pid in s.meta:
        # Update stored object reference (in case simulator passes updated object instance)
        s.pipelines_by_id[pid] = p
        return

    prio = p.priority
    s.pipelines_by_id[pid] = p
    s.meta[pid] = {
        "priority": prio,
        "enqueue_tick": s.tick,
        "ram_frac": float(s.init_fracs[prio]["ram"]),
        "cpu_frac": float(s.init_fracs[prio]["cpu"]),
        "failures": 0,
        "last_failure_tick": -10**9,
    }
    s.queues[prio].append(pid)


def _pipeline_ready_op(p):
    status = p.runtime_status()
    if status.is_pipeline_successful():
        return None
    op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
    if not op_list:
        return None
    return op_list


def _pop_next_runnable_pipeline_id(s, prio):
    """
    Round-robin scan within a priority queue to find a pipeline with a runnable op.
    Returns (pipeline_id, op_list) or (None, None).
    """
    q = s.queues[prio]
    n = len(q)
    if n == 0:
        return None, None

    start = s.cursor[prio] % n
    for i in range(n):
        idx = (start + i) % n
        pid = q[idx]
        p = s.pipelines_by_id.get(pid)
        if p is None:
            continue
        op_list = _pipeline_ready_op(p)
        if op_list:
            # Next scan starts after this one (fairness within same priority)
            s.cursor[prio] = (idx + 1) % max(1, len(s.queues[prio]))
            return pid, op_list

    # Nothing runnable in this queue right now
    return None, None


def _oldest_wait_ticks(s, prio):
    oldest = None
    for pid in s.queues[prio]:
        m = s.meta.get(pid)
        if not m:
            continue
        wt = s.tick - m["enqueue_tick"]
        if oldest is None or wt > oldest:
            oldest = wt
    return 0 if oldest is None else oldest


def _desired_resources_for(s, pool, pid, prio, avail_cpu, avail_ram):
    """
    Choose cpu/ram for an op given priority, pool capacity, adaptive hints, and availability.
    """
    m = s.meta[pid]
    # Start with adaptive fractions; cap batch to be conservative
    cpu_frac = float(m["cpu_frac"])
    ram_frac = float(m["ram_frac"])

    if prio == Priority.BATCH_PIPELINE:
        cpu_frac = min(cpu_frac, s.batch_max_frac_cpu)
        ram_frac = min(ram_frac, s.batch_max_frac_ram)

    # Convert fractions to absolute requests
    # Keep at least 1 CPU if possible; RAM at least a small positive amount.
    cpu_req = max(1, int(pool.max_cpu_pool * cpu_frac))
    ram_req = pool.max_ram_pool * ram_frac

    # Never request more than available
    cpu_req = min(int(avail_cpu), int(cpu_req))
    ram_req = min(float(avail_ram), float(ram_req))

    # If we can't allocate minimally, indicate infeasible
    if cpu_req <= 0 or ram_req <= 0:
        return None, None

    return cpu_req, ram_req


def _pool_order(s):
    # Prefer pools with more headroom for placing high-priority work
    scored = []
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        # Use a simple headroom score
        score = float(pool.avail_cpu_pool) * float(pool.avail_ram_pool)
        scored.append((score, pool_id))
    scored.sort(reverse=True)
    return [pid for _, pid in scored]


@register_scheduler(key="scheduler_medium_018")
def scheduler_medium_018(s, results, pipelines):
    """
    Scheduling loop:
      1) Register new pipelines.
      2) Adapt RAM/CPU hints upward on failures (assume mostly OOM).
      3) Remove completed pipelines from queues.
      4) For each pool (most headroom first), fill with assignments:
           - Prefer QUERY then INTERACTIVE.
           - Keep headroom reserved for HP while HP is waiting.
           - Allow BATCH when no HP runnable, or when batch has aged enough and headroom exists.
    """
    s.tick += 1

    # 1) Ingest new pipelines
    for p in pipelines:
        _ensure_pipeline_registered(s, p)

    # 2) Process results for adaptive sizing
    for r in results:
        if not hasattr(r, "ops") or not r.ops:
            continue
        # We rely on ExecutionResult having pipeline_id indirectly; use op->pipeline not available.
        # Instead, infer pipeline_id by scanning ops' owning pipeline is not exposed; so we adapt
        # conservatively by priority only if we cannot map. However, Assignment includes pipeline_id,
        # and most simulators attach it to result; use it when present.
        pid = getattr(r, "pipeline_id", None)
        if pid is None:
            continue
        if pid not in s.meta:
            continue

        if hasattr(r, "failed") and r.failed():
            m = s.meta[pid]
            m["failures"] += 1
            m["last_failure_tick"] = s.tick

            # Increase RAM fraction aggressively to avoid repeated OOM -> 720s penalties.
            m["ram_frac"] = _clamp(m["ram_frac"] * s.ram_backoff_mult + 0.02, 0.10, 1.00)

            # Slight CPU bump to reduce tail latency once OOM risk is mitigated.
            m["cpu_frac"] = _clamp(m["cpu_frac"] * s.cpu_backoff_mult, 0.10, 1.00)

    # 3) Remove completed pipelines from queues/meta
    #    Keep failed/incomplete pipelines to avoid 720s penalties (we retry with bigger RAM).
    for prio in list(s.queues.keys()):
        for pid in list(s.queues[prio]):
            p = s.pipelines_by_id.get(pid)
            if p is None:
                _remove_from_queue(s, prio, pid)
                s.meta.pop(pid, None)
                s.pipelines_by_id.pop(pid, None)
                continue
            status = p.runtime_status()
            if status.is_pipeline_successful():
                _remove_from_queue(s, prio, pid)
                s.meta.pop(pid, None)
                s.pipelines_by_id.pop(pid, None)

    # Early exit if nothing to do
    if not pipelines and not results and not (_any_high_priority_waiting(s) or _queue_nonempty(s, Priority.BATCH_PIPELINE)):
        return [], []

    suspensions = []  # no preemption: simulator API doesn't reliably expose active containers to evict
    assignments = []

    hp_waiting = _any_high_priority_waiting(s)
    batch_old_wait = _oldest_wait_ticks(s, Priority.BATCH_PIPELINE)

    # 4) Schedule: place on pools with most headroom first
    for pool_id in _pool_order(s):
        pool = s.executor.pools[pool_id]
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)
        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # Dynamic reservations: if any HP is queued, keep some headroom in each pool.
        reserve_cpu = float(pool.max_cpu_pool) * (s.hp_reserve_frac_cpu if hp_waiting else 0.0)
        reserve_ram = float(pool.max_ram_pool) * (s.hp_reserve_frac_ram if hp_waiting else 0.0)

        # Fill the pool with as many ops as possible
        # (one op per assignment to keep placement flexible and reduce head-of-line blocking).
        while avail_cpu > 0 and avail_ram > 0:
            # Decide which priority to pull from next.
            # Strictly prefer HP, but allow batch if it has waited long and doesn't eat reserved headroom.
            chosen_prio = None

            # Try QUERY then INTERACTIVE first
            for pr in (Priority.QUERY, Priority.INTERACTIVE):
                pid, op_list = _pop_next_runnable_pipeline_id(s, pr)
                if pid is not None:
                    chosen_prio = pr
                    break
            else:
                pid, op_list = (None, None)

            # If no HP runnable, consider batch
            if chosen_prio is None:
                pid_b, op_list_b = _pop_next_runnable_pipeline_id(s, Priority.BATCH_PIPELINE)
                if pid_b is not None:
                    pid, op_list = pid_b, op_list_b
                    chosen_prio = Priority.BATCH_PIPELINE
                else:
                    break  # nothing runnable in this pool right now

            # Enforce headroom reservation against batch when HP waiting
            if chosen_prio == Priority.BATCH_PIPELINE and hp_waiting:
                # Only schedule batch if enough remains after reserving headroom,
                # unless batch has aged significantly (still respect reservation somewhat).
                aged = batch_old_wait >= s.aging_threshold_ticks
                if not aged:
                    if (avail_cpu - 1) < reserve_cpu or (avail_ram - (0.01 * pool.max_ram_pool)) < reserve_ram:
                        break  # keep headroom for HP arrivals / queued HP
                else:
                    # Relax reservation modestly when batch is old, but don't fully exhaust.
                    reserve_cpu *= 0.5
                    reserve_ram *= 0.5

            # Compute desired resources; if infeasible, stop scheduling in this pool.
            cpu_req, ram_req = _desired_resources_for(s, pool, pid, chosen_prio, avail_cpu, avail_ram)
            if cpu_req is None:
                break

            # If this would violate reservations (mainly for batch), skip/break
            if chosen_prio == Priority.BATCH_PIPELINE and hp_waiting:
                if (avail_cpu - cpu_req) < reserve_cpu or (avail_ram - ram_req) < reserve_ram:
                    # Try shrinking batch request once (conservative fallback)
                    cpu_req = max(1, min(cpu_req, int(max(1, pool.max_cpu_pool * 0.15))))
                    ram_req = min(ram_req, float(pool.max_ram_pool) * 0.20)
                    cpu_req = min(int(avail_cpu), int(cpu_req))
                    ram_req = min(float(avail_ram), float(ram_req))
                    if cpu_req <= 0 or ram_req <= 0:
                        break
                    if (avail_cpu - cpu_req) < reserve_cpu or (avail_ram - ram_req) < reserve_ram:
                        break

            p = s.pipelines_by_id.get(pid)
            if p is None:
                continue

            assignment = Assignment(
                ops=op_list,
                cpu=cpu_req,
                ram=ram_req,
                priority=p.priority,
                pool_id=pool_id,
                pipeline_id=p.pipeline_id,
            )
            assignments.append(assignment)

            avail_cpu -= float(cpu_req)
            avail_ram -= float(ram_req)

            # If we just scheduled HP, keep hp_waiting accurate for later iterations
            hp_waiting = _any_high_priority_waiting(s)

    return suspensions, assignments
