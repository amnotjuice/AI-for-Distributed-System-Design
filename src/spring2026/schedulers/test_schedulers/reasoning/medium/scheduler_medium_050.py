# policy_key: scheduler_medium_050
# reasoning_effort: medium
# exp: reasoning
# model: gpt-5.2-2025-12-11
# llm_cost: 0.069283
# generation_seconds: 82.33
# generated_at: 2026-04-09T23:37:49.822759
@register_scheduler_init(key="scheduler_medium_050")
def scheduler_medium_050_init(s):
    """Priority-aware, failure-averse scheduler.

    Goals for the weighted-latency objective:
      - Keep QUERY and INTERACTIVE tail latency low via priority-first dispatch and (optional) pool reservation.
      - Avoid pipeline failures caused by OOM by learning per-operator RAM hints and ramping on OOM.
      - Avoid starvation of BATCH via light aging/credit so they still make progress under sustained foreground load.
      - When resources are tight, optionally preempt BATCH to make room for foreground work (best-effort; depends on pool container visibility).
    """
    from collections import deque, defaultdict

    # Per-priority waiting queues
    s.q_query = deque()
    s.q_interactive = deque()
    s.q_batch = deque()

    # Track all known pipelines by id (avoid duplicates in queues; still robust if duplicates happen)
    s.pipelines_by_id = {}

    # Operator-level learned sizing hints (keyed per pipeline+op to keep identity stable)
    s.op_ram_hint = {}  # (pipeline_id, op_key) -> ram
    s.op_cpu_hint = {}  # (pipeline_id, op_key) -> cpu

    # Failure bookkeeping to decide when to stop wasting cycles on likely-nonrecoverable ops
    s.op_fail_count_non_oom = defaultdict(int)  # (pipeline_id, op_key) -> count
    s.op_fail_count_oom = defaultdict(int)      # (pipeline_id, op_key) -> count

    # Pipelines we deem "blocked" (likely non-recoverable errors). We stop scheduling them to protect others.
    s.blocked_pipelines = set()

    # Batch aging: increase "credit" over time so batch still runs occasionally.
    s.batch_age = defaultdict(int)  # pipeline_id -> age ticks while waiting

    # Preemption cooldown (avoid thrash)
    s.ticks = 0
    s.last_preempt_tick_by_pool = defaultdict(lambda: -10)


def _sm050_now_tick(s):
    return getattr(s, "ticks", 0)


def _sm050_is_oom_error(err) -> bool:
    if not err:
        return False
    msg = str(err).lower()
    return ("oom" in msg) or ("out of memory" in msg) or ("cuda out of memory" in msg) or ("killed" in msg and "memory" in msg)


def _sm050_op_key(op):
    # Best-effort stable identifier for an operator object across scheduling steps.
    for attr in ("op_id", "operator_id", "id", "name"):
        if hasattr(op, attr):
            try:
                v = getattr(op, attr)
                if callable(v):
                    v = v()
                if v is not None:
                    return (attr, v)
            except Exception:
                pass
    # Fallback: python identity (stable within a run)
    return ("pyid", id(op))


def _sm050_pipeline_done_or_blocked(s, pipeline) -> bool:
    if pipeline is None:
        return True
    if pipeline.pipeline_id in s.blocked_pipelines:
        return True
    st = pipeline.runtime_status()
    return st.is_pipeline_successful()


def _sm050_enqueue_pipeline(s, pipeline):
    # Insert into our canonical map and appropriate queue.
    s.pipelines_by_id[pipeline.pipeline_id] = pipeline
    if pipeline.pipeline_id in s.blocked_pipelines:
        return
    if pipeline.priority == Priority.QUERY:
        s.q_query.append(pipeline.pipeline_id)
    elif pipeline.priority == Priority.INTERACTIVE:
        s.q_interactive.append(pipeline.pipeline_id)
    else:
        s.q_batch.append(pipeline.pipeline_id)


def _sm050_pop_next_ready_pipeline_id(s, which: str, require_parents_complete=True, max_scan=64):
    """Round-robin scan within a queue to find a pipeline with at least one ready operator."""
    from collections import deque

    q = s.q_query if which == "query" else (s.q_interactive if which == "interactive" else s.q_batch)
    scans = min(len(q), max_scan)
    for _ in range(scans):
        pid = q.popleft()
        p = s.pipelines_by_id.get(pid)
        if p is None:
            continue
        if _sm050_pipeline_done_or_blocked(s, p):
            continue
        st = p.runtime_status()
        op_list = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=require_parents_complete)
        if op_list:
            # Put it back at tail (RR fairness) and return pid.
            q.append(pid)
            return pid
        # No ready ops now; keep it in queue for later.
        q.append(pid)
    return None


def _sm050_pick_next_pipeline_for_pool(s, pool_id: int):
    """Choose next pipeline class for this pool.

    If multiple pools exist:
      - Pool 0 prefers QUERY/INTERACTIVE (foreground) to protect latency.
      - Other pools prefer BATCH but can steal foreground if batch empty.
    If only one pool:
      - Always prefer QUERY then INTERACTIVE then BATCH, with light batch aging.
    """
    num_pools = getattr(s.executor, "num_pools", 1)

    # Light batch aging: if batch waiting a long time, allow occasional promotion.
    batch_waiting = len(s.q_batch) > 0
    batch_oldest_age = 0
    if batch_waiting:
        # Approximate oldest by scanning a few elements (cheap).
        scan = min(8, len(s.q_batch))
        for i in range(scan):
            pid = s.q_batch[i]
            batch_oldest_age = max(batch_oldest_age, s.batch_age.get(pid, 0))

    # Pool preference
    if num_pools > 1:
        if pool_id == 0:
            # Foreground pool: query first, then interactive, then batch if nothing else.
            for cls in ("query", "interactive", "batch"):
                pid = _sm050_pop_next_ready_pipeline_id(s, cls, require_parents_complete=True)
                if pid is not None:
                    return pid
            return None
        else:
            # Background pools: batch first, but steal foreground if batch empty.
            if batch_waiting:
                pid = _sm050_pop_next_ready_pipeline_id(s, "batch", require_parents_complete=True)
                if pid is not None:
                    return pid
            for cls in ("query", "interactive"):
                pid = _sm050_pop_next_ready_pipeline_id(s, cls, require_parents_complete=True)
                if pid is not None:
                    return pid
            return None
    else:
        # Single pool: always protect foreground; occasionally allow old batch to jump ahead.
        if batch_waiting and batch_oldest_age >= 25 and (len(s.q_query) + len(s.q_interactive)) > 0:
            pid = _sm050_pop_next_ready_pipeline_id(s, "batch", require_parents_complete=True)
            if pid is not None:
                return pid
        for cls in ("query", "interactive", "batch"):
            pid = _sm050_pop_next_ready_pipeline_id(s, cls, require_parents_complete=True)
            if pid is not None:
                return pid
        return None


def _sm050_target_resources(s, pool, pipeline, op):
    """Pick (cpu, ram) for an op based on priority, pool size, and learned hints."""
    import math

    max_cpu = getattr(pool, "max_cpu_pool", 1)
    max_ram = getattr(pool, "max_ram_pool", 1)
    avail_cpu = getattr(pool, "avail_cpu_pool", 0)
    avail_ram = getattr(pool, "avail_ram_pool", 0)

    # Base targets by priority (favor completion + low latency for foreground; avoid OOM by giving more RAM)
    if pipeline.priority == Priority.QUERY:
        base_cpu = max(1, int(math.ceil(0.70 * max_cpu)))
        base_ram = max(1, int(math.ceil(0.55 * max_ram)))
    elif pipeline.priority == Priority.INTERACTIVE:
        base_cpu = max(1, int(math.ceil(0.50 * max_cpu)))
        base_ram = max(1, int(math.ceil(0.40 * max_ram)))
    else:
        base_cpu = max(1, int(math.ceil(0.30 * max_cpu)))
        base_ram = max(1, int(math.ceil(0.25 * max_ram)))

    # Learned hints for this specific (pipeline, op)
    opk = (pipeline.pipeline_id, _sm050_op_key(op))
    hinted_ram = s.op_ram_hint.get(opk)
    hinted_cpu = s.op_cpu_hint.get(opk)

    # Prefer hints if present; otherwise base.
    tgt_cpu = hinted_cpu if hinted_cpu is not None else base_cpu
    tgt_ram = hinted_ram if hinted_ram is not None else base_ram

    # Clamp to available resources (we can only allocate what is free now)
    tgt_cpu = max(1, min(int(tgt_cpu), int(avail_cpu)))
    tgt_ram = max(1, min(int(tgt_ram), int(avail_ram)))

    # If we have plenty available and this is foreground, slightly boost CPU to reduce latency.
    if pipeline.priority in (Priority.QUERY, Priority.INTERACTIVE):
        if avail_cpu >= tgt_cpu + 1:
            tgt_cpu = min(int(avail_cpu), max(tgt_cpu, min(int(max_cpu), tgt_cpu + 1)))

    return tgt_cpu, tgt_ram


def _sm050_maybe_preempt_for_foreground(s, pool_id: int, pool, need_ram: int, need_cpu: int):
    """Best-effort: preempt one BATCH container in this pool if foreground is waiting and headroom is low.

    This depends on the simulator exposing running containers via pool.containers or similar.
    If no visibility, this is a no-op (safe).
    """
    # Cooldown to avoid repeated suspends each tick.
    now = _sm050_now_tick(s)
    if now - s.last_preempt_tick_by_pool[pool_id] < 3:
        return []

    avail_cpu = getattr(pool, "avail_cpu_pool", 0)
    avail_ram = getattr(pool, "avail_ram_pool", 0)
    if avail_cpu >= need_cpu and avail_ram >= need_ram:
        return []

    # Only bother if we have pending foreground.
    if len(s.q_query) == 0 and len(s.q_interactive) == 0:
        return []

    # Discover containers (best-effort)
    containers = None
    for attr in ("containers", "running_containers", "active_containers"):
        if hasattr(pool, attr):
            try:
                containers = getattr(pool, attr)
                break
            except Exception:
                containers = None
    if not containers:
        return []

    # Pick a lowest-priority container to suspend (prefer BATCH)
    victim = None
    for c in containers:
        try:
            cprio = getattr(c, "priority", None)
            cstate = getattr(c, "state", None)
            cid = getattr(c, "container_id", None)
            if cid is None:
                continue
            if cstate is not None and str(cstate) in ("SUSPENDING", "COMPLETED", "FAILED"):
                continue
            if cprio == Priority.BATCH_PIPELINE:
                victim = c
                break
        except Exception:
            continue

    if victim is None:
        return []

    cid = getattr(victim, "container_id", None)
    if cid is None:
        return []

    s.last_preempt_tick_by_pool[pool_id] = now
    return [Suspend(container_id=cid, pool_id=pool_id)]


@register_scheduler(key="scheduler_medium_050")
def scheduler_medium_050(s, results: list, pipelines: list):
    """
    Scheduler step:
      1) Ingest new pipelines into priority queues.
      2) Update RAM/CPU hints from execution results; ramp RAM on OOM; optionally stop retrying persistent non-OOM failures.
      3) Age batch pipelines while waiting.
      4) Assign ready operators to pools with priority awareness and OOM-avoidant sizing.
      5) Best-effort preemption of batch when foreground is blocked (if container visibility exists).
    """
    s.ticks = getattr(s, "ticks", 0) + 1

    # 1) Enqueue new pipelines
    for p in pipelines:
        _sm050_enqueue_pipeline(s, p)

    # 2) Process results: learn hints; ramp on OOM; track persistent non-OOM failures
    for r in results or []:
        # Update hints from what we actually ran (or tried to run).
        try:
            ops = getattr(r, "ops", []) or []
            pid = getattr(r, "pipeline_id", None)
            # Some simulators don't put pipeline_id in result; infer via op ownership if available (best-effort).
        except Exception:
            ops = []
            pid = None

        for op in ops:
            # Best effort pipeline_id retrieval
            if pid is None:
                try:
                    pid = getattr(op, "pipeline_id", None)
                except Exception:
                    pid = None
            if pid is None:
                continue

            opk = (pid, _sm050_op_key(op))

            # Track last-known working sizes for faster convergence.
            try:
                ran_ram = int(getattr(r, "ram", 0) or 0)
                ran_cpu = int(getattr(r, "cpu", 0) or 0)
            except Exception:
                ran_ram, ran_cpu = 0, 0

            failed = False
            try:
                failed = r.failed()
            except Exception:
                failed = False

            if not failed:
                # Successful: keep hints (but don't inflate unnecessarily).
                if ran_ram > 0:
                    prev = s.op_ram_hint.get(opk)
                    s.op_ram_hint[opk] = ran_ram if prev is None else max(1, min(prev, ran_ram))
                if ran_cpu > 0:
                    prevc = s.op_cpu_hint.get(opk)
                    s.op_cpu_hint[opk] = ran_cpu if prevc is None else max(1, min(prevc, ran_cpu))
                s.op_fail_count_non_oom[opk] = 0
                # OOM fail count decays naturally by reset on success
                s.op_fail_count_oom[opk] = 0
            else:
                err = getattr(r, "error", None)
                is_oom = _sm050_is_oom_error(err)

                if is_oom:
                    s.op_fail_count_oom[opk] += 1
                    # Ramp RAM aggressively: double (or add a floor) to converge quickly and avoid repeated OOM latency.
                    if ran_ram > 0:
                        s.op_ram_hint[opk] = max(ran_ram * 2, ran_ram + 1)
                else:
                    s.op_fail_count_non_oom[opk] += 1
                    # For non-OOM failures, retry a limited number of times then stop scheduling that pipeline
                    # to protect overall latency (since an eventual failure is a fixed 720s penalty anyway).
                    # Determine pipeline priority (best-effort via result, else lookup by pid).
                    prio = getattr(r, "priority", None)
                    if prio is None and pid in s.pipelines_by_id:
                        prio = s.pipelines_by_id[pid].priority

                    # Thresholds: more forgiving for foreground; stricter for batch.
                    if prio == Priority.QUERY:
                        limit = 4
                    elif prio == Priority.INTERACTIVE:
                        limit = 3
                    else:
                        limit = 2

                    if s.op_fail_count_non_oom[opk] >= limit:
                        s.blocked_pipelines.add(pid)

    # 3) Age batch pipelines (only those still pending and not done/blocked)
    # Increment age for items currently in the queue (bounded scan to keep it cheap).
    batch_scan = min(len(s.q_batch), 256)
    for i in range(batch_scan):
        pid = s.q_batch[i]
        p = s.pipelines_by_id.get(pid)
        if p is None or _sm050_pipeline_done_or_blocked(s, p):
            continue
        s.batch_age[pid] = s.batch_age.get(pid, 0) + 1

    # Early exit if nothing changed (but still allow aging to take effect; we already aged above)
    if (not pipelines) and (not results):
        # We can still schedule based on existing queues, so no early exit.
        pass

    suspensions = []
    assignments = []

    # 4) Schedule across pools
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = int(getattr(pool, "avail_cpu_pool", 0) or 0)
        avail_ram = int(getattr(pool, "avail_ram_pool", 0) or 0)
        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # Best-effort preemption if foreground is waiting and we have very low headroom.
        # Use conservative "need" to create at least one foreground slot.
        max_ram = int(getattr(pool, "max_ram_pool", 1) or 1)
        max_cpu = int(getattr(pool, "max_cpu_pool", 1) or 1)
        need_ram = max(1, int(0.35 * max_ram))
        need_cpu = max(1, int(0.25 * max_cpu))
        suspensions.extend(_sm050_maybe_preempt_for_foreground(s, pool_id, pool, need_ram=need_ram, need_cpu=need_cpu))

        # Pack multiple assignments per pool per tick while resources remain.
        # Stop after some cap to avoid infinite loops if queues contain many non-ready pipelines.
        pack_cap = 16
        packed = 0

        while avail_cpu > 0 and avail_ram > 0 and packed < pack_cap:
            pid = _sm050_pick_next_pipeline_for_pool(s, pool_id)
            if pid is None:
                break

            pipeline = s.pipelines_by_id.get(pid)
            if pipeline is None or _sm050_pipeline_done_or_blocked(s, pipeline):
                continue

            st = pipeline.runtime_status()
            op_list = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
            if not op_list:
                # No ready op right now
                break

            op = op_list[0]
            cpu, ram = _sm050_target_resources(s, pool, pipeline, op)

            # If we can't allocate at least 1/1, stop for this pool.
            if cpu <= 0 or ram <= 0:
                break
            if cpu > avail_cpu or ram > avail_ram:
                # Can't fit; stop packing (we didn't modify avail_* yet).
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

            avail_cpu -= cpu
            avail_ram -= ram
            packed += 1

    return suspensions, assignments
