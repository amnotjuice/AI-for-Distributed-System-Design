# policy_key: scheduler_high_010
# reasoning_effort: high
# exp: reasoning
# model: gpt-5.2-2025-12-11
# llm_cost: 0.152877
# generation_seconds: 164.97
# generated_at: 2026-04-10T00:41:06.275035
@register_scheduler_init(key="scheduler_high_010")
def scheduler_high_010_init(s):
    """
    Priority-aware, OOM-adaptive, SRPT-ish scheduler.

    Core ideas:
      - Strongly prioritize QUERY, then INTERACTIVE, then BATCH to minimize weighted latency.
      - Avoid 720s penalties by retrying OOM failures with increased RAM (per-op lower bounds).
      - Use light SRPT: within each priority, prefer pipelines with fewer remaining ops.
      - Basic fairness: age-aware tie-breaking + a soft batch reservation on the "batch pool" (if multi-pool).
      - No preemption (keeps churn low and avoids needing extra executor introspection).
    """
    s.now = 0

    # Known pipelines and bookkeeping
    s.pipelines = {}  # pipeline_id -> Pipeline
    s.enq_time = {}  # pipeline_id -> tick when first seen
    s.last_scheduled = {}  # pipeline_id -> last tick scheduled

    # Priority queues store pipeline_ids (lazy deletion/compaction)
    s.queues = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }
    s.in_queue = set()  # pipeline_id currently present in some queue list

    # Terminal pipelines we will not schedule further (non-OOM failures)
    s.dead_pipelines = set()

    # Per-(pipeline, op) attempt counters and RAM lower bounds after OOMs
    # key: (pipeline_id, op_sig)
    s.op_attempts = {}
    s.op_ram_lb = {}
    s.op_last_error = {}

    # Cross-pipeline operator signature hints (best-effort)
    # key: op_sig (without pipeline_id)
    s.sig_ram_success_min = {}  # smallest RAM that has succeeded for this op signature
    s.sig_ram_oom_lb = {}  # observed RAM lower bound after OOMs for this op signature


def _is_oom_error(err) -> bool:
    if err is None:
        return False
    try:
        msg = str(err).lower()
    except Exception:
        return False
    # Heuristic keywords; simulator errors typically contain one of these for OOM-like failures.
    return ("oom" in msg) or ("out of memory" in msg) or ("memory" in msg and "alloc" in msg) or ("killed" in msg and "memory" in msg)


def _get_numeric_attr(obj, names):
    for n in names:
        if hasattr(obj, n):
            v = getattr(obj, n)
            if isinstance(v, (int, float)) and v > 0:
                return float(v)
    return None


def _op_base_sig(op):
    # Stable-ish signature to generalize RAM hints across pipelines where possible.
    for attr in ("operator_id", "op_id", "name", "key"):
        if hasattr(op, attr):
            v = getattr(op, attr)
            if isinstance(v, (int, str)) and v != "":
                return (attr, v)
    # Fall back to type name; less precise but still useful.
    return ("type", type(op).__name__)


def _op_sig(pipeline_id, op):
    # Per-pipeline signature (for attempt counts / lb) + base signature for cross-pipeline hints.
    return (pipeline_id, _op_base_sig(op))


def _pipeline_total_ops(pipeline):
    vals = getattr(pipeline, "values", None)
    if vals is None:
        return None
    try:
        return len(vals)
    except Exception:
        return None


def _pipeline_remaining_ops(pipeline, status):
    total = _pipeline_total_ops(pipeline)
    try:
        completed = status.state_counts.get(OperatorState.COMPLETED, 0)
    except Exception:
        completed = 0
    if isinstance(total, int) and total >= 0:
        return max(0, total - int(completed))
    # If unknown, approximate with "not completed" count when possible.
    try:
        # Count as remaining everything that isn't completed.
        remaining = 0
        for st, cnt in status.state_counts.items():
            if st != OperatorState.COMPLETED:
                remaining += int(cnt)
        return max(1, remaining)
    except Exception:
        return 999


def _pipeline_in_flight_ops(status):
    # Limit per-pipeline concurrency to reduce resource spikes and OOM risk.
    try:
        return (
            int(status.state_counts.get(OperatorState.ASSIGNED, 0))
            + int(status.state_counts.get(OperatorState.RUNNING, 0))
            + int(status.state_counts.get(OperatorState.SUSPENDING, 0))
        )
    except Exception:
        return 0


def _compact_queues(s):
    # Remove completed/dead pipelines from queues periodically.
    if s.now % 50 != 0:
        return
    for prio, q in s.queues.items():
        new_q = []
        for pid in q:
            p = s.pipelines.get(pid)
            if p is None:
                continue
            if pid in s.dead_pipelines:
                continue
            try:
                if p.runtime_status().is_pipeline_successful():
                    continue
            except Exception:
                pass
            new_q.append(pid)
        s.queues[prio] = new_q
    # Rebuild in_queue
    s.in_queue = set()
    for q in s.queues.values():
        for pid in q:
            s.in_queue.add(pid)


def _estimate_resources(s, pipeline, op, pool, rem_cpu, rem_ram):
    # Priority-dependent CPU fraction (sublinear scaling; give more CPU to high priority, but not all).
    cpu_frac = {
        Priority.QUERY: 0.50,
        Priority.INTERACTIVE: 0.40,
        Priority.BATCH_PIPELINE: 0.25,
    }.get(pipeline.priority, 0.30)

    # Priority-dependent RAM fallback if operator requirement isn't introspectable.
    ram_frac = {
        Priority.QUERY: 0.25,
        Priority.INTERACTIVE: 0.20,
        Priority.BATCH_PIPELINE: 0.15,
    }.get(pipeline.priority, 0.18)

    # Operator-minimum RAM if available
    op_min_ram = _get_numeric_attr(
        op,
        (
            "min_ram",
            "ram_min",
            "min_memory",
            "min_mem",
            "ram_required",
            "ram_requirement",
            "memory_required",
            "memory_requirement",
            "ram",
            "memory",
            "mem",
        ),
    )

    # Operator-minimum CPU if available
    op_min_cpu = _get_numeric_attr(
        op,
        (
            "min_cpu",
            "cpu_min",
            "vcpus_min",
            "min_vcpus",
            "cpu",
            "vcpus",
            "num_cpus",
        ),
    )

    # Base targets
    base_cpu = max(1.0, float(pool.max_cpu_pool) * float(cpu_frac))
    base_ram = float(pool.max_ram_pool) * float(ram_frac)
    if op_min_ram is not None:
        base_ram = max(base_ram, float(op_min_ram))
    if op_min_cpu is not None:
        base_cpu = max(1.0, float(op_min_cpu))

    # OOM-adaptive lower bounds
    pid = pipeline.pipeline_id
    sig = _op_sig(pid, op)
    base_sig = _op_base_sig(op)

    attempts = int(s.op_attempts.get(sig, 0))
    # Exponential RAM backoff per-op-within-pipeline after OOMs.
    backoff_ram = base_ram * (2.0 ** max(0, attempts))
    lb_ram = max(
        float(s.op_ram_lb.get(sig, 0.0)),
        float(s.sig_ram_oom_lb.get(base_sig, 0.0)),
    )
    # If we've seen a successful minimum RAM for this operator signature, prefer it (reduces OOM risk).
    succ_min = float(s.sig_ram_success_min.get(base_sig, 0.0))
    target_ram = max(backoff_ram, lb_ram, succ_min, base_ram)

    # Fit to remaining pool resources; CPU can shrink (slower but avoids head-of-line blocking).
    cpu = min(float(rem_cpu), base_cpu)
    cpu = max(1.0, cpu) if rem_cpu >= 1.0 else 0.0
    ram = min(float(rem_ram), target_ram)

    # Must meet RAM target to avoid OOM; do not schedule if we can't allocate enough.
    if ram + 1e-9 < target_ram:
        return None
    if cpu < 1.0:
        return None
    return float(cpu), float(ram)


def _pick_best_pipeline_for_priority(s, prio, pool, rem_cpu, rem_ram, global_hi_waiting, pool_role):
    q = s.queues.get(prio, [])
    if not q:
        return None

    # Per-pipeline concurrency caps
    max_in_flight = {
        Priority.QUERY: 2,
        Priority.INTERACTIVE: 1,
        Priority.BATCH_PIPELINE: 1,
    }.get(prio, 1)

    # Soft reservations: when high-priority work is waiting, avoid filling shared pools with batch.
    reserve_cpu = 1.0
    reserve_ram = float(pool.max_ram_pool) * 0.10

    best = None
    best_metric = None
    best_idx = None

    for idx, pid in enumerate(q):
        p = s.pipelines.get(pid)
        if p is None:
            continue
        if pid in s.dead_pipelines:
            continue

        try:
            status = p.runtime_status()
        except Exception:
            continue

        try:
            if status.is_pipeline_successful():
                continue
        except Exception:
            pass

        if _pipeline_in_flight_ops(status) >= max_in_flight:
            continue

        # Only schedule ops whose parents are complete
        try:
            ready_ops = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
        except Exception:
            ready_ops = []
        if not ready_ops:
            continue

        op = ready_ops[0]
        est = _estimate_resources(s, p, op, pool, rem_cpu, rem_ram)
        if est is None:
            continue
        cpu, ram = est

        # If scheduling batch in a shared pool while high priority is waiting, keep headroom.
        if prio == Priority.BATCH_PIPELINE and global_hi_waiting and pool_role != "batch":
            if (rem_cpu - cpu) < reserve_cpu or (rem_ram - ram) < reserve_ram:
                continue

        age = int(s.now - s.enq_time.get(pid, s.now))
        remaining = _pipeline_remaining_ops(p, status)

        # SRPT-ish metric with age tie-breaker; batch gets stronger aging to avoid starvation.
        age_weight = {
            Priority.QUERY: 1,
            Priority.INTERACTIVE: 2,
            Priority.BATCH_PIPELINE: 5,
        }.get(prio, 2)

        metric = (int(remaining) * 1000) - (age_weight * age)

        if best is None or metric < best_metric:
            best = (p, op, cpu, ram)
            best_metric = metric
            best_idx = idx

    if best is None:
        return None

    # Move selected pipeline_id to back for mild fairness among similar candidates.
    try:
        pid = q[best_idx]
        q.pop(best_idx)
        q.append(pid)
        s.queues[prio] = q
    except Exception:
        pass

    return best


@register_scheduler(key="scheduler_high_010")
def scheduler_high_010(s, results, pipelines):
    s.now += 1

    # Add newly arrived pipelines
    for p in pipelines:
        pid = p.pipeline_id
        s.pipelines[pid] = p
        if pid not in s.enq_time:
            s.enq_time[pid] = s.now
        if pid not in s.in_queue and pid not in s.dead_pipelines:
            s.queues[p.priority].append(pid)
            s.in_queue.add(pid)

    # Process execution results (successes update RAM hints; failures trigger retries or terminal marking)
    for r in results:
        # Best-effort pipeline id extraction
        pid = getattr(r, "pipeline_id", None)
        if pid is None:
            # Some simulators attach pipeline_id to ops; try that.
            try:
                if getattr(r, "ops", None):
                    for attr in ("pipeline_id", "pipeline", "dag_id"):
                        if hasattr(r.ops[0], attr):
                            pid = getattr(r.ops[0], attr)
                            break
            except Exception:
                pid = None

        err = getattr(r, "error", None)
        is_fail = False
        try:
            is_fail = r.failed()
        except Exception:
            is_fail = False

        if is_fail:
            oom = _is_oom_error(err)
            # Update per-op and per-signature OOM lower bounds and attempts
            for op in getattr(r, "ops", []) or []:
                if pid is None:
                    continue
                sig = _op_sig(pid, op)
                base_sig = _op_base_sig(op)
                prev_attempts = int(s.op_attempts.get(sig, 0))
                s.op_attempts[sig] = prev_attempts + 1
                s.op_last_error[sig] = str(err) if err is not None else "failed"

                if oom:
                    # Increase RAM lower bound using the attempted RAM (if known); otherwise double prior lb.
                    tried_ram = getattr(r, "ram", None)
                    prev_lb = float(s.op_ram_lb.get(sig, 0.0))
                    if isinstance(tried_ram, (int, float)) and tried_ram > 0:
                        new_lb = max(prev_lb, float(tried_ram) * 2.0)
                    else:
                        new_lb = max(prev_lb * 2.0, prev_lb + 1.0)
                    s.op_ram_lb[sig] = new_lb
                    s.sig_ram_oom_lb[base_sig] = max(float(s.sig_ram_oom_lb.get(base_sig, 0.0)), new_lb)
                else:
                    # Non-OOM failures are treated as terminal to avoid wasting resources.
                    if pid is not None:
                        s.dead_pipelines.add(pid)
        else:
            # Success: record minimal successful RAM for operator signature
            used_ram = getattr(r, "ram", None)
            if isinstance(used_ram, (int, float)) and used_ram > 0:
                for op in getattr(r, "ops", []) or []:
                    base_sig = _op_base_sig(op)
                    prev = s.sig_ram_success_min.get(base_sig, None)
                    if prev is None:
                        s.sig_ram_success_min[base_sig] = float(used_ram)
                    else:
                        s.sig_ram_success_min[base_sig] = min(float(prev), float(used_ram))

    # Periodic queue cleanup
    _compact_queues(s)

    # Determine whether high-priority work is waiting (ready-to-run) to protect latency.
    def _has_ready(prio):
        for pid in s.queues.get(prio, []):
            if pid in s.dead_pipelines:
                continue
            p = s.pipelines.get(pid)
            if p is None:
                continue
            try:
                st = p.runtime_status()
                if st.is_pipeline_successful():
                    continue
                ready = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
                if ready:
                    return True
            except Exception:
                continue
        return False

    query_ready = _has_ready(Priority.QUERY)
    interactive_ready = _has_ready(Priority.INTERACTIVE)
    global_hi_waiting = bool(query_ready or interactive_ready)

    suspensions = []
    assignments = []

    num_pools = int(s.executor.num_pools)

    # Helper: "soft batch reservation" in batch pool when batch is old.
    def _oldest_batch_age():
        oldest = None
        for pid in s.queues.get(Priority.BATCH_PIPELINE, []):
            if pid in s.dead_pipelines:
                continue
            t = s.enq_time.get(pid, None)
            if t is None:
                continue
            if oldest is None or t < oldest:
                oldest = t
        if oldest is None:
            return 0
        return int(s.now - oldest)

    oldest_batch_age = _oldest_batch_age()

    for pool_id in range(num_pools):
        pool = s.executor.pools[pool_id]
        rem_cpu = float(pool.avail_cpu_pool)
        rem_ram = float(pool.avail_ram_pool)
        if rem_cpu < 1.0 or rem_ram <= 0.0:
            continue

        # Pool roles if multiple pools:
        #   - pool 0: "hi" (prefer QUERY/INTERACTIVE)
        #   - last pool: "batch" (ensure batch progress)
        #   - others: shared
        if num_pools >= 2 and pool_id == 0:
            pool_role = "hi"
        elif num_pools >= 2 and pool_id == (num_pools - 1):
            pool_role = "batch"
        else:
            pool_role = "shared"

        # Limit how many new containers we launch per pool per tick (keeps behavior stable).
        launches = 0
        max_launches = 4

        while rem_cpu >= 1.0 and rem_ram > 0.0 and launches < max_launches:
            # Priority order per pool role
            if pool_role == "hi":
                prio_order = [Priority.QUERY, Priority.INTERACTIVE]
                # Only allow batch on hi pool if no hi work is ready at all.
                if not global_hi_waiting:
                    prio_order.append(Priority.BATCH_PIPELINE)
            elif pool_role == "batch":
                # If batch has waited "too long", give it a turn even if queries exist.
                if oldest_batch_age >= 80:
                    prio_order = [Priority.BATCH_PIPELINE, Priority.QUERY, Priority.INTERACTIVE]
                else:
                    prio_order = [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]
            else:
                prio_order = [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]

            chosen = None
            for prio in prio_order:
                picked = _pick_best_pipeline_for_priority(
                    s=s,
                    prio=prio,
                    pool=pool,
                    rem_cpu=rem_cpu,
                    rem_ram=rem_ram,
                    global_hi_waiting=global_hi_waiting,
                    pool_role=pool_role,
                )
                if picked is not None:
                    chosen = (prio, picked)
                    break

            if chosen is None:
                break

            prio, (pipeline, op, cpu, ram) = chosen

            assignment = Assignment(
                ops=[op],
                cpu=float(cpu),
                ram=float(ram),
                priority=pipeline.priority,
                pool_id=pool_id,
                pipeline_id=pipeline.pipeline_id,
            )
            assignments.append(assignment)

            rem_cpu -= float(cpu)
            rem_ram -= float(ram)
            launches += 1
            s.last_scheduled[pipeline.pipeline_id] = s.now

    return suspensions, assignments
