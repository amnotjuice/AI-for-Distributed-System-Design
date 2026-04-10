# policy_key: scheduler_est_029
# reasoning_effort: high
# model: gpt-5.2-2025-12-11
# llm_cost: 0.064294
# generation_seconds: 135.53
# generated_at: 2026-03-31T19:16:05.487694
@register_scheduler_init(key="scheduler_est_029")
def scheduler_est_029_init(s):
    """Priority-aware, estimate-based packing scheduler (incremental improvement over naive FIFO).

    Key improvements vs naive:
      1) Priority ordering: schedule QUERY/INTERACTIVE ahead of BATCH.
      2) Avoid over-allocation: size RAM from op.estimate.mem_peak_gb (with safety factor).
      3) Better concurrency: pack multiple ops per pool per tick instead of "one op gets everything".
      4) OOM retry: if an op fails with OOM, retry with increased RAM (bounded retries).
      5) Simple fairness: batch aging can promote long-waiting batch to run earlier.
      6) Optional low-churn preemption hook (best-effort, only if pool exposes container metadata).
    """
    s.ticks = 0

    # Store active pipelines by id (avoids duplicate queueing across ticks).
    s.pipelines_by_id = {}
    s.arrival_tick_by_pid = {}

    # Per-operator adaptive memory multiplier (learned from OOM failures).
    s.op_mem_mult = {}          # op_key -> float multiplier
    s.op_retries = {}           # op_key -> int retry count
    s.op_error_kind = {}        # op_key -> "oom" | "other" | "ok"

    # Tunables (kept conservative; complexity can be increased later).
    s.mem_safety_factor = 1.25
    s.max_mem_mult = 4.0
    s.max_oom_retries = 3

    # CPU sizing (fractions of pool capacity) by priority; used as a target, then capped by availability.
    s.cpu_frac_by_priority = {
        Priority.QUERY: 0.75,
        Priority.INTERACTIVE: 0.60,
        Priority.BATCH_PIPELINE: 0.30,
    }
    s.min_cpu = 1.0
    s.min_ram_gb = 0.5

    # When high-priority work is waiting, keep some headroom by not letting batch consume the last slice.
    s.reserve_frac_cpu_for_hp = 0.15
    s.reserve_frac_ram_for_hp = 0.15

    # Batch aging to reduce starvation under sustained interactive load.
    s.batch_aging_threshold_ticks = 200  # after this, treat batch as interactive for ordering

    # Optional preemption control (best-effort; only used if we can find running containers).
    s.enable_best_effort_preemption = True
    s.preempt_cooldown_ticks = 25
    s.last_preempt_tick = -10**9


def _priority_rank(p):
    # Lower rank = higher priority (for comparisons).
    if p == Priority.QUERY:
        return 0
    if p == Priority.INTERACTIVE:
        return 1
    return 2  # Priority.BATCH_PIPELINE (and any unknown treated as lowest)


def _is_oom_error(err):
    if err is None:
        return False
    msg = str(err).lower()
    return ("oom" in msg) or ("out of memory" in msg) or ("out-of-memory" in msg) or ("memory" in msg and "alloc" in msg)


def _op_key(op):
    # Prefer stable ids if present; fallback to object identity.
    if hasattr(op, "op_id"):
        return ("op_id", getattr(op, "op_id"))
    if hasattr(op, "operator_id"):
        return ("operator_id", getattr(op, "operator_id"))
    if hasattr(op, "name"):
        return ("name_id", getattr(op, "name"), id(op))
    return ("py_id", id(op))


def _op_est_mem_gb(op, default_gb=1.0):
    est = getattr(op, "estimate", None)
    if est is None:
        return float(default_gb)
    val = getattr(est, "mem_peak_gb", None)
    if val is None:
        return float(default_gb)
    try:
        v = float(val)
    except Exception:
        v = float(default_gb)
    # Guard against pathological zeros/negatives.
    return max(0.01, v)


def _compute_ram_request_gb(s, op, pool):
    est_gb = _op_est_mem_gb(op, default_gb=1.0)
    mult = s.op_mem_mult.get(_op_key(op), 1.0)
    req = est_gb * s.mem_safety_factor * mult
    req = max(s.min_ram_gb, req)
    # Never request more than the pool can provide.
    try:
        req = min(req, float(pool.max_ram_pool))
    except Exception:
        pass
    return req


def _compute_cpu_request(s, priority, pool, avail_cpu):
    try:
        target = float(pool.max_cpu_pool) * float(s.cpu_frac_by_priority.get(priority, 0.30))
    except Exception:
        target = float(avail_cpu)
    req = max(s.min_cpu, target)
    # Cap by currently available CPU.
    req = min(float(avail_cpu), float(req))
    # Avoid tiny fractional CPU requests that may not make sense in some simulators.
    return max(0.0, req)


def _effective_priority_for_ordering(s, pipeline):
    # Simple starvation protection: batch pipelines that wait "too long" are promoted in ordering only.
    pr = pipeline.priority
    if pr != Priority.BATCH_PIPELINE:
        return pr
    pid = pipeline.pipeline_id
    waited = s.ticks - s.arrival_tick_by_pid.get(pid, s.ticks)
    if waited >= s.batch_aging_threshold_ticks:
        return Priority.INTERACTIVE
    return pr


def _iter_pool_containers_best_effort(pool):
    # Try common attribute names; if none exist, preemption becomes a no-op.
    for attr in ("containers", "running_containers", "active_containers"):
        conts = getattr(pool, attr, None)
        if conts is None:
            continue
        if isinstance(conts, dict):
            for v in conts.values():
                yield v
            return
        if isinstance(conts, (list, tuple, set)):
            for v in conts:
                yield v
            return


def _container_fields_best_effort(c):
    # Return (container_id, priority, cpu, ram) or (None, None, None, None) if unavailable.
    if c is None:
        return None, None, None, None
    cid = getattr(c, "container_id", None)
    if cid is None:
        cid = getattr(c, "id", None)
    pr = getattr(c, "priority", None)
    cpu = getattr(c, "cpu", None)
    ram = getattr(c, "ram", None)
    return cid, pr, cpu, ram


@register_scheduler(key="scheduler_est_029")
def scheduler_est_029(s, results: List[ExecutionResult], pipelines: List[Pipeline]) -> Tuple[List[Suspend], List[Assignment]]:
    s.ticks += 1

    # Ingest newly arrived pipelines.
    for p in pipelines:
        s.pipelines_by_id[p.pipeline_id] = p
        if p.pipeline_id not in s.arrival_tick_by_pid:
            s.arrival_tick_by_pid[p.pipeline_id] = s.ticks

    # Learn from execution results (primarily OOM -> increase RAM multiplier).
    for r in results:
        # r.ops is a list of operators executed in that container.
        for op in getattr(r, "ops", []) or []:
            k = _op_key(op)
            if r.failed():
                if _is_oom_error(getattr(r, "error", None)):
                    s.op_error_kind[k] = "oom"
                    s.op_retries[k] = s.op_retries.get(k, 0) + 1
                    prev = s.op_mem_mult.get(k, 1.0)
                    s.op_mem_mult[k] = min(s.max_mem_mult, prev * 1.8)
                else:
                    s.op_error_kind[k] = "other"
            else:
                # Success: gently decay any previous multiplier back toward 1.0.
                s.op_error_kind[k] = "ok"
                s.op_retries[k] = 0
                if k in s.op_mem_mult:
                    s.op_mem_mult[k] = max(1.0, s.op_mem_mult[k] * 0.90)

    # Drop completed pipelines and permanently-failed pipelines (non-OOM or too many OOM retries).
    active_pipelines = []
    for pid, p in list(s.pipelines_by_id.items()):
        st = p.runtime_status()
        if st.is_pipeline_successful():
            del s.pipelines_by_id[pid]
            continue

        # If there are FAILED ops, only keep pipeline if failures look OOM-related and retries remain.
        if st.state_counts.get(OperatorState.FAILED, 0) > 0:
            failed_ops = st.get_ops([OperatorState.FAILED], require_parents_complete=False) or []
            drop = False
            for op in failed_ops:
                k = _op_key(op)
                if s.op_error_kind.get(k, None) != "oom":
                    drop = True
                    break
                if s.op_retries.get(k, 0) > s.max_oom_retries:
                    drop = True
                    break
            if drop:
                del s.pipelines_by_id[pid]
                continue

        active_pipelines.append(p)

    # If nothing changed and no new arrivals, no new decisions to make.
    if not pipelines and not results:
        return [], []

    # Precompute per-pipeline scheduling info once (avoid repeated runtime_status() calls).
    pipeline_infos = []
    any_hp_waiting = False
    for p in active_pipelines:
        st = p.runtime_status()

        # Keep "one op per pipeline at a time" to reduce intra-pipeline contention/latency spikes.
        busy = (st.state_counts.get(OperatorState.ASSIGNED, 0) + st.state_counts.get(OperatorState.RUNNING, 0)) > 0
        if busy:
            continue

        op_list = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
        if not op_list:
            continue

        eff_pr = _effective_priority_for_ordering(s, p)
        if eff_pr in (Priority.QUERY, Priority.INTERACTIVE):
            any_hp_waiting = True

        pipeline_infos.append(
            {
                "pipeline": p,
                "op_list": op_list,  # length 1
                "eff_priority": eff_pr,
                "priority": p.priority,
                "pid": p.pipeline_id,
            }
        )

    suspensions: List[Suspend] = []
    assignments: List[Assignment] = []
    scheduled_pids = set()

    # Pack assignments per pool, always trying higher priority first.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = float(pool.avail_cpu_pool)
        avail_ram = float(pool.avail_ram_pool)
        if avail_cpu <= 0.0 or avail_ram <= 0.0:
            continue

        # Reserve a small slice for high-priority work if it is waiting, to reduce latency under load.
        reserve_cpu = 0.0
        reserve_ram = 0.0
        if any_hp_waiting:
            try:
                reserve_cpu = float(pool.max_cpu_pool) * float(s.reserve_frac_cpu_for_hp)
                reserve_ram = float(pool.max_ram_pool) * float(s.reserve_frac_ram_for_hp)
            except Exception:
                reserve_cpu = 0.0
                reserve_ram = 0.0

        # Greedy best-fit within each priority class: pick the smallest RAM request that fits.
        while True:
            picked = None

            for prio in (Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE):
                effective_avail_cpu = avail_cpu
                effective_avail_ram = avail_ram

                # Apply reservation only when considering batch placements.
                if prio == Priority.BATCH_PIPELINE and any_hp_waiting:
                    effective_avail_cpu = max(0.0, avail_cpu - reserve_cpu)
                    effective_avail_ram = max(0.0, avail_ram - reserve_ram)

                if effective_avail_cpu <= 0.0 or effective_avail_ram <= 0.0:
                    continue

                best = None  # (ram_req, cpu_req, info)
                for info in pipeline_infos:
                    if info["pid"] in scheduled_pids:
                        continue
                    if info["eff_priority"] != prio:
                        continue

                    op = info["op_list"][0]
                    ram_req = _compute_ram_request_gb(s, op, pool)
                    cpu_req = _compute_cpu_request(s, info["priority"], pool, effective_avail_cpu)

                    # Must fit in (effective) remaining capacity.
                    if cpu_req <= 0.0:
                        continue
                    if cpu_req <= effective_avail_cpu and ram_req <= effective_avail_ram:
                        cand = (ram_req, cpu_req, info)
                        if best is None or cand[0] < best[0]:
                            best = cand

                if best is not None:
                    picked = best
                    break

            if picked is None:
                break

            ram_req, cpu_req, info = picked
            p = info["pipeline"]

            assignments.append(
                Assignment(
                    ops=info["op_list"],
                    cpu=cpu_req,
                    ram=ram_req,
                    priority=p.priority,
                    pool_id=pool_id,
                    pipeline_id=p.pipeline_id,
                )
            )
            scheduled_pids.add(p.pipeline_id)
            avail_cpu -= float(cpu_req)
            avail_ram -= float(ram_req)

            if avail_cpu <= 0.0 or avail_ram <= 0.0:
                break

    # Best-effort preemption (low churn): if high-priority work is waiting but none got scheduled,
    # try suspending one batch container to create headroom. This is intentionally conservative.
    if (
        s.enable_best_effort_preemption
        and any_hp_waiting
        and not any(a.priority in (Priority.QUERY, Priority.INTERACTIVE) for a in assignments)
        and (s.ticks - s.last_preempt_tick) >= s.preempt_cooldown_ticks
    ):
        victim = None  # (priority_rank, -ram, pool_id, container_id)
        for pool_id in range(s.executor.num_pools):
            pool = s.executor.pools[pool_id]
            for c in _iter_pool_containers_best_effort(pool):
                cid, pr, cpu, ram = _container_fields_best_effort(c)
                if cid is None or pr is None:
                    continue
                # Only preempt strictly lower-priority work (typically batch).
                if _priority_rank(pr) <= _priority_rank(Priority.INTERACTIVE):
                    continue
                try:
                    ram_v = float(ram) if ram is not None else 0.0
                except Exception:
                    ram_v = 0.0
                key = (_priority_rank(pr), -ram_v, pool_id, cid)
                if victim is None or key > victim:
                    victim = key

        if victim is not None:
            _, _, v_pool_id, v_cid = victim
            suspensions.append(Suspend(v_cid, v_pool_id))
            s.last_preempt_tick = s.ticks

    return suspensions, assignments
