# policy_key: scheduler_high_023
# reasoning_effort: high
# exp: reasoning
# model: gpt-5.2-2025-12-11
# llm_cost: 0.000000
# generation_seconds: 287.49
# generated_at: 2026-04-10T01:53:26.567913
@register_scheduler_init(key="scheduler_high_023")
def scheduler_high_023_init(s):
    """Priority-aware, failure-avoiding scheduler.

    Main ideas:
      - Maintain separate FIFO queues per priority (QUERY > INTERACTIVE > BATCH).
      - Allocate moderate CPU shares to enable concurrency among high-priority pipelines.
      - Allocate conservative RAM (relative to pool size + operator minimum if available).
      - On failure (assumed OOM-dominant), increase per-operator RAM hint and retry quickly.
      - Prevent batch from consuming the last headroom when high-priority work exists (soft reservation),
        while still guaranteeing batch makes progress (streak breaker + aging).
    """
    import collections

    s.tick = 0
    s.queues = {
        Priority.QUERY: collections.deque(),
        Priority.INTERACTIVE: collections.deque(),
        Priority.BATCH_PIPELINE: collections.deque(),
    }
    s.queued_ids = set()            # pipeline_id currently tracked in queues
    s.pipelines_by_id = {}          # pipeline_id -> Pipeline
    s.arrival_tick = {}             # pipeline_id -> tick when first seen

    # Map operator object identity back to its pipeline for result handling.
    s.opid_to_pipe = {}             # id(op) -> pipeline_id
    s.opid_to_opkey = {}            # id(op) -> stable-ish key tuple

    # Per-operator RAM escalation after failures: (pipeline_id, opkey) -> next_ram_request
    s.ram_hint = {}

    # Pipelines that just failed (retry should be prioritized within their class)
    s.retry_front = set()

    # Fairness knobs
    s.hi_streak = 0                 # count of consecutive (query/interactive) assignments across ticks


def _op_min_ram_023(op) -> float:
    """Best-effort extraction of an operator's minimum RAM requirement, if exposed by the model."""
    # Common attribute names across simulators / DAG node types
    for attr in ("min_ram", "ram_min", "min_memory", "memory_min", "mem_min", "ram", "memory", "mem"):
        v = getattr(op, attr, None)
        if isinstance(v, (int, float)) and v > 0:
            return float(v)

    res = getattr(op, "resources", None)
    if res is not None:
        for attr in ("min_ram", "ram", "memory", "mem"):
            v = getattr(res, attr, None)
            if isinstance(v, (int, float)) and v > 0:
                return float(v)

    return 0.0


def _make_opkey_023(pipeline_id, op):
    """Create a stable-ish identifier for an operator within a pipeline."""
    for attr in ("op_id", "operator_id", "task_id", "node_id", "name"):
        v = getattr(op, attr, None)
        if isinstance(v, (int, str)) and v != "":
            return (pipeline_id, attr, v)
    # Fallback: python object identity (stable for the operator object lifetime)
    return (pipeline_id, "pyid", id(op))


def _promote_retries_023(q, retry_front_set):
    """Move retrying pipelines to the front of a deque (stable partition)."""
    if not q or not retry_front_set:
        return
    n = len(q)
    front = []
    for _ in range(n):
        p = q.popleft()
        if p.pipeline_id in retry_front_set:
            front.append(p)
        else:
            q.append(p)
    for p in reversed(front):
        q.appendleft(p)


def _alloc_cpu_023(avail_cpu, target_cpu):
    if avail_cpu <= 0:
        return 0
    cpu = target_cpu if target_cpu < avail_cpu else avail_cpu
    if cpu < 1 and avail_cpu >= 1:
        cpu = 1
    return cpu


def _alloc_ram_023(avail_ram, target_ram):
    if avail_ram <= 0:
        return 0
    ram = target_ram if target_ram < avail_ram else avail_ram
    if ram < 1 and avail_ram >= 1:
        ram = 1
    return ram


@register_scheduler(key="scheduler_high_023")
def scheduler_high_023(s, results, pipelines):
    """
    Priority-aware scheduler with:
      - Separate queues per priority
      - Conservative RAM defaults + OOM-driven RAM backoff
      - Soft headroom reservation to keep latency low for queries/interactive
      - Anti-starvation for batch via streak breaker + aging
    """
    # If nothing new happened, decisions won't change (executor state only changes on arrivals/results).
    if not pipelines and not results:
        return [], []

    s.tick += 1
    assignments = []
    suspensions = []

    # --- Intake new pipelines ---
    for p in pipelines:
        pid = p.pipeline_id
        if pid in s.pipelines_by_id:
            continue
        s.pipelines_by_id[pid] = p
        s.arrival_tick[pid] = s.tick
        pr = p.priority
        if pr not in s.queues:
            pr = Priority.BATCH_PIPELINE
        s.queues[pr].append(p)
        s.queued_ids.add(pid)

    # --- Process execution results (OOM/backoff + prioritize retries) ---
    for r in results:
        if not r.failed():
            continue

        # Treat failures as primarily OOM-driven; escalate RAM for the failing ops.
        err = ""
        try:
            err = (str(r.error).lower() if r.error is not None else "")
        except Exception:
            err = ""
        is_oomish = ("oom" in err) or ("out of memory" in err) or ("memory" in err) or ("ram" in err)

        for op in (getattr(r, "ops", None) or []):
            op_id = id(op)
            pid = s.opid_to_pipe.get(op_id)
            if pid is None:
                continue

            opkey = s.opid_to_opkey.get(op_id)
            if opkey is None:
                opkey = _make_opkey_023(pid, op)
                s.opid_to_opkey[op_id] = opkey

            prev_hint = s.ram_hint.get((pid, opkey), 0.0)
            min_ram = _op_min_ram_023(op)

            # Base escalation: double the previous allocation, then grow further if repeated failures.
            # If it's not clearly OOM, still bump but more conservatively (to avoid runaway).
            base = float(getattr(r, "ram", 0.0) or 0.0)
            if base <= 0 and prev_hint > 0:
                base = prev_hint

            if is_oomish:
                bumped = max(prev_hint * 1.5, base * 2.0, min_ram * 1.5, 1.0)
            else:
                bumped = max(prev_hint * 1.25, base * 1.5, min_ram * 1.25, 1.0)

            s.ram_hint[(pid, opkey)] = bumped
            s.retry_front.add(pid)

    # --- Promote recently-failed pipelines to reduce time-to-retry ---
    _promote_retries_023(s.queues[Priority.QUERY], s.retry_front)
    _promote_retries_023(s.queues[Priority.INTERACTIVE], s.retry_front)
    _promote_retries_023(s.queues[Priority.BATCH_PIPELINE], s.retry_front)

    # --- Snapshot pool capacities (we do our own accounting while building assignments) ---
    num_pools = s.executor.num_pools
    pools = s.executor.pools
    avail_cpu = [pools[i].avail_cpu_pool for i in range(num_pools)]
    avail_ram = [pools[i].avail_ram_pool for i in range(num_pools)]
    max_cpu = [pools[i].max_cpu_pool for i in range(num_pools)]
    max_ram = [pools[i].max_ram_pool for i in range(num_pools)]

    q_wait = len(s.queues[Priority.QUERY])
    i_wait = len(s.queues[Priority.INTERACTIVE])
    b_wait = len(s.queues[Priority.BATCH_PIPELINE])
    hi_waiting = (q_wait + i_wait) > 0

    scheduled_this_tick = set()   # pipeline_id -> scheduled at most once per tick (limits bursty hogging)
    blocked_this_tick = set()     # pipeline_id that couldn't fit anywhere this tick

    def dequeue_ready_from(priority):
        """Round-robin scan a priority queue to find one ready op."""
        q = s.queues[priority]
        if not q:
            return None, None

        n = len(q)
        for _ in range(n):
            p = q.popleft()
            pid = p.pipeline_id

            if pid in blocked_this_tick or pid in scheduled_this_tick:
                q.append(p)
                continue

            st = p.runtime_status()
            if st.is_pipeline_successful():
                # Cleanup tracking for completed pipelines
                s.queued_ids.discard(pid)
                s.pipelines_by_id.pop(pid, None)
                s.arrival_tick.pop(pid, None)
                s.retry_front.discard(pid)
                q.append(p)  # keep queue rotation stable; it will be skipped/cleaned next time if re-encountered
                continue

            op_list = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
            if not op_list:
                q.append(p)
                continue

            op = op_list[0]
            s.opid_to_pipe[id(op)] = pid
            s.opid_to_opkey[id(op)] = _make_opkey_023(pid, op)

            # Keep pipeline in the queue (FIFO-ish); we only schedule one op per pipeline per tick.
            q.append(p)
            return p, op

        return None, None

    def pick_pool_and_size(p, op):
        """Choose a pool and size (cpu, ram) for this (pipeline, op) pair."""
        pid = p.pipeline_id
        pr = p.priority
        if pr not in s.queues:
            pr = Priority.BATCH_PIPELINE

        # Aging: allow old batch jobs to use pool0 even under contention.
        age_ticks = s.tick - s.arrival_tick.get(pid, s.tick)
        batch_aged = (pr == Priority.BATCH_PIPELINE and age_ticks >= 250)

        # CPU shares tuned for concurrency among high-priority work.
        total_hi = q_wait + i_wait
        if pr == Priority.QUERY:
            share = 0.60 if q_wait <= 1 else (0.45 if q_wait == 2 else 0.35)
        elif pr == Priority.INTERACTIVE:
            share = 0.50 if total_hi <= 2 else (0.33 if total_hi <= 4 else 0.25)
        else:
            share = 0.25 if b_wait <= 4 else 0.20

        min_ram_req = _op_min_ram_023(op)
        opkey = s.opid_to_opkey.get(id(op), _make_opkey_023(pid, op))
        hinted_ram = float(s.ram_hint.get((pid, opkey), 0.0) or 0.0)

        best = None  # (score, pool_id, cpu_req, ram_req)
        for pool_id in range(num_pools):
            if avail_cpu[pool_id] <= 0 or avail_ram[pool_id] <= 0:
                continue

            # For batch under high-priority contention: avoid pool0 unless aged or only one pool.
            if pr == Priority.BATCH_PIPELINE and hi_waiting and num_pools > 1 and pool_id == 0 and not batch_aged:
                continue

            # Soft reservations: don't let batch consume the last headroom while high-priority exists.
            if pr == Priority.BATCH_PIPELINE and hi_waiting and not batch_aged:
                reserve_frac = 0.35 if (num_pools > 1 and pool_id == 0) else 0.25
                reserve_cpu = max_cpu[pool_id] * reserve_frac
                reserve_ram = max_ram[pool_id] * reserve_frac
            else:
                reserve_cpu = 0.0
                reserve_ram = 0.0

            # Candidate CPU request
            target_cpu = max_cpu[pool_id] * share
            # Slightly bias queries to get more CPU on pool0 when available
            if pr == Priority.QUERY and num_pools > 1 and pool_id == 0:
                target_cpu = max_cpu[pool_id] * min(0.75, share + 0.10)

            cpu_req = _alloc_cpu_023(avail_cpu[pool_id], target_cpu)
            if cpu_req <= 0:
                continue

            # Candidate RAM request (base fraction of pool + op minimum + any hint)
            if pr == Priority.QUERY:
                base_frac = 0.12
                cap_frac = 0.90
                min_scale = 1.25
            elif pr == Priority.INTERACTIVE:
                base_frac = 0.10
                cap_frac = 0.85
                min_scale = 1.20
            else:
                base_frac = 0.08
                cap_frac = 0.75
                min_scale = 1.10

            base_ram = max(max_ram[pool_id] * base_frac, min_ram_req * min_scale, 1.0)
            target_ram = max(base_ram, hinted_ram, min_ram_req, 1.0)
            target_ram = min(target_ram, max_ram[pool_id] * cap_frac)

            # If hint/base exceeds current availability, try with what's available as long as >= min.
            ram_req = _alloc_ram_023(avail_ram[pool_id], target_ram)
            if ram_req < max(min_ram_req, 1.0):
                continue

            # Enforce headroom reservation only for batch.
            if pr == Priority.BATCH_PIPELINE and (avail_cpu[pool_id] - cpu_req) < reserve_cpu:
                continue
            if pr == Priority.BATCH_PIPELINE and (avail_ram[pool_id] - ram_req) < reserve_ram:
                continue

            # Score pool choice:
            #  - For QUERY/INTERACTIVE: prefer pools with more free resources (lower queueing)
            #  - For BATCH: prefer packing (less leftover), and avoid pool0 when possible
            cpu_free = avail_cpu[pool_id] / max(1e-9, float(max_cpu[pool_id]))
            ram_free = avail_ram[pool_id] / max(1e-9, float(max_ram[pool_id]))

            if pr in (Priority.QUERY, Priority.INTERACTIVE):
                score = -(2.0 * cpu_free + 1.0 * ram_free)
                if pool_id == 0:
                    score -= 0.10
            else:
                cpu_left = (avail_cpu[pool_id] - cpu_req) / max(1e-9, float(max_cpu[pool_id]))
                ram_left = (avail_ram[pool_id] - ram_req) / max(1e-9, float(max_ram[pool_id]))
                score = (1.0 * cpu_left + 0.7 * ram_left)
                if num_pools > 1 and pool_id == 0:
                    score += 0.20

            cand = (score, pool_id, cpu_req, ram_req)
            if best is None or cand[0] < best[0]:
                best = cand

        if best is None:
            return None, 0, 0
        _, pool_id, cpu_req, ram_req = best
        return pool_id, cpu_req, ram_req

    # --- Main scheduling loop ---
    # Bound iterations: scan a few times over queued pipelines.
    max_iter = (q_wait + i_wait + b_wait) * 4 + 20
    for _ in range(max_iter):
        # Simple streak-breaker: after enough high-priority assignments, try to place a batch op (if any).
        if b_wait > 0 and (s.hi_streak >= 8):
            priority_order = (Priority.BATCH_PIPELINE, Priority.QUERY, Priority.INTERACTIVE)
        else:
            priority_order = (Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE)

        chosen = None
        for pr in priority_order:
            p, op = dequeue_ready_from(pr)
            if p is not None:
                chosen = (p, op)
                break

        if chosen is None:
            break

        p, op = chosen
        pid = p.pipeline_id

        pool_id, cpu_req, ram_req = pick_pool_and_size(p, op)
        if pool_id is None:
            blocked_this_tick.add(pid)
            continue

        # Commit assignment + update local accounting
        assignment = Assignment(
            ops=[op],
            cpu=cpu_req,
            ram=ram_req,
            priority=p.priority,
            pool_id=pool_id,
            pipeline_id=pid,
        )
        assignments.append(assignment)

        avail_cpu[pool_id] -= cpu_req
        avail_ram[pool_id] -= ram_req

        scheduled_this_tick.add(pid)
        s.retry_front.discard(pid)

        if p.priority in (Priority.QUERY, Priority.INTERACTIVE):
            s.hi_streak += 1
        else:
            s.hi_streak = 0

    return suspensions, assignments
