# policy_key: scheduler_medium_016
# reasoning_effort: medium
# exp: reasoning
# model: gpt-5.2-2025-12-11
# llm_cost: 0.116505
# generation_seconds: 91.85
# generated_at: 2026-04-09T22:50:56.513722
@register_scheduler_init(key="scheduler_medium_016")
def scheduler_medium_016_init(s):
    """
    Priority-weighted, OOM-aware, non-preemptive scheduler.

    Core ideas:
      - Strong priority ordering (QUERY > INTERACTIVE > BATCH), with a small weighted-fair component
        to prevent batch starvation (avoid 720s penalties for incomplete jobs).
      - OOM-aware retries: learn per-operator RAM needs from failures and rapidly escalate RAM to succeed.
      - Conservative CPU sizing for concurrency, with higher CPU targets for higher priorities.

    This policy intentionally avoids preemption to reduce wasted work/churn and relies on good admission
    + sizing decisions to protect high-priority latency while keeping batch making progress.
    """
    # Per-priority FIFO queues of pipelines
    s.queues = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }

    # Track pipelines we have seen to avoid double-enqueue
    s.known_pipelines = set()

    # Pipeline wait counters (in scheduler decision "turns") for aging
    s.wait_turns = {}  # pipeline_id -> int

    # Lightweight weighted-fair scheduling via deficits (prevents starvation)
    s.deficit = {
        Priority.QUERY: 0,
        Priority.INTERACTIVE: 0,
        Priority.BATCH_PIPELINE: 0,
    }
    s.quantum = {
        Priority.QUERY: 10,
        Priority.INTERACTIVE: 5,
        Priority.BATCH_PIPELINE: 1,
    }

    # Learned per-op resource profile keyed by a robust op signature
    # op_profile[key] = {"ram_suggest": float, "cpu_suggest": float, "ooms": int, "fails": int}
    s.op_profile = {}

    # Tuning knobs (kept simple; simulator may have different scales)
    s.aging_to_interactive = 40   # promote batch -> interactive after enough turns waiting
    s.aging_to_query = 120        # promote interactive -> query after long waits
    s.max_cpu_cap = 8.0           # don't request silly CPU for a single op
    s.min_cpu = 1.0


@register_scheduler(key="scheduler_medium_016")
def scheduler_medium_016(s, results, pipelines):
    def _prio_order():
        return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]

    def _ensure_pipeline(p):
        pid = p.pipeline_id
        if pid in s.known_pipelines:
            return
        s.known_pipelines.add(pid)
        s.wait_turns[pid] = 0
        s.queues[p.priority].append(p)

    def _is_oom_error(err):
        if err is None:
            return False
        msg = str(err).lower()
        return ("oom" in msg) or ("out of memory" in msg) or ("cuda oom" in msg) or ("memoryerror" in msg)

    def _op_id_like(op):
        # Best-effort stable identifier across runs
        for attr in ("op_id", "operator_id", "id", "name"):
            if hasattr(op, attr):
                try:
                    v = getattr(op, attr)
                    return str(v)
                except Exception:
                    pass
        try:
            return str(op)
        except Exception:
            return f"op@{id(op)}"

    def _infer_pipeline_id_from_result(r):
        # ExecutionResult may or may not carry pipeline_id; try common fields
        for attr in ("pipeline_id", "pipeline", "pid"):
            if hasattr(r, attr):
                try:
                    v = getattr(r, attr)
                    if v is None:
                        continue
                    return v.pipeline_id if hasattr(v, "pipeline_id") else v
                except Exception:
                    pass
        # Fall back: try op fields
        if hasattr(r, "ops") and r.ops:
            op0 = r.ops[0]
            for attr in ("pipeline_id", "pipeline", "pid"):
                if hasattr(op0, attr):
                    try:
                        v = getattr(op0, attr)
                        if v is None:
                            continue
                        return v.pipeline_id if hasattr(v, "pipeline_id") else v
                    except Exception:
                        pass
        return None

    def _op_key(pipeline_id, op):
        # Include pipeline_id when available to avoid cross-pipeline pollution
        base = _op_id_like(op)
        return f"{pipeline_id}:{base}" if pipeline_id is not None else base

    def _baseline_ram(pool, prio):
        # Baseline RAM as a fraction of pool capacity: protects against under-allocation OOM.
        # Kept moderate to preserve concurrency.
        frac = {
            Priority.QUERY: 0.22,
            Priority.INTERACTIVE: 0.18,
            Priority.BATCH_PIPELINE: 0.12,
        }[prio]
        return max(1.0, pool.max_ram_pool * frac)

    def _baseline_cpu(pool, prio):
        # CPU: higher for higher priority but capped to encourage concurrency.
        frac = {
            Priority.QUERY: 0.50,
            Priority.INTERACTIVE: 0.35,
            Priority.BATCH_PIPELINE: 0.25,
        }[prio]
        return max(s.min_cpu, min(s.max_cpu_cap, max(1.0, pool.max_cpu_pool * frac)))

    def _size_for_op(pool, prio, pipeline_id, op, avail_cpu, avail_ram):
        key = _op_key(pipeline_id, op)
        prof = s.op_profile.get(key, None)

        ram_req = _baseline_ram(pool, prio)
        cpu_req = _baseline_cpu(pool, prio)

        if prof is not None:
            # Trust learned suggestions; they come from observed failures/successes.
            if prof.get("ram_suggest") is not None:
                ram_req = max(ram_req, float(prof["ram_suggest"]))
            if prof.get("cpu_suggest") is not None:
                cpu_req = max(s.min_cpu, min(float(prof["cpu_suggest"]), s.max_cpu_cap))

            # If we have seen OOMs, ramp more aggressively to converge to success quickly.
            ooms = int(prof.get("ooms", 0))
            if ooms >= 1:
                ram_req = max(ram_req, _baseline_ram(pool, prio) * (1.5 ** min(3, ooms)))
            if ooms >= 2:
                # After multiple OOMs, jump towards a larger share of the machine to avoid repeated failures.
                ram_req = max(ram_req, 0.60 * pool.max_ram_pool)
            if ooms >= 3:
                ram_req = max(ram_req, 0.80 * pool.max_ram_pool)

        # Fit CPU to what's available (CPU under-provisioning slows but does not fail).
        cpu_req = min(cpu_req, max(s.min_cpu, avail_cpu))

        # RAM must be >= requirement to avoid OOM; if not enough available now, caller should defer.
        ram_req = min(ram_req, pool.max_ram_pool)
        return cpu_req, ram_req

    def _effective_priority(p):
        # Aging: promote long-waiting pipelines to avoid indefinite starvation (720s penalty).
        pid = p.pipeline_id
        w = s.wait_turns.get(pid, 0)
        if p.priority == Priority.BATCH_PIPELINE and w >= s.aging_to_interactive:
            return Priority.INTERACTIVE
        if p.priority == Priority.INTERACTIVE and w >= s.aging_to_query:
            return Priority.QUERY
        return p.priority

    def _refill_deficits_if_needed():
        if (s.deficit[Priority.QUERY] <= 0 and
            s.deficit[Priority.INTERACTIVE] <= 0 and
            s.deficit[Priority.BATCH_PIPELINE] <= 0):
            for pr in _prio_order():
                s.deficit[pr] += s.quantum[pr]

    def _choose_from_queues(pool, avail_cpu, avail_ram):
        """
        Pick a pipeline that (a) has an assignable op ready, and (b) likely fits in this pool now.

        We try priorities in order, but with deficits and aging adjustments to avoid starvation.
        """
        _refill_deficits_if_needed()

        # Build candidate order: primarily strict priority, but only if deficit allows.
        # If a priority is empty, skip it.
        prios = _prio_order()

        for pr in prios:
            # If deficit is empty, deprioritize, but still allow if nothing else fits later.
            if not s.queues[pr]:
                continue

        # Two-pass selection:
        #  - Pass 1: only priorities with positive deficit
        #  - Pass 2: any priority (prevents deadlock when deficits are exhausted)
        for pass_id in (1, 2):
            for pr in prios:
                if not s.queues[pr]:
                    continue
                if pass_id == 1 and s.deficit[pr] <= 0:
                    continue

                q = s.queues[pr]
                qlen = len(q)
                # Rotate through queue to find something runnable and that fits.
                for _ in range(qlen):
                    p = q.pop(0)

                    status = p.runtime_status()
                    if status.is_pipeline_successful():
                        # Drop completed pipelines from state.
                        pid = p.pipeline_id
                        s.known_pipelines.discard(pid)
                        s.wait_turns.pop(pid, None)
                        continue

                    # Use effective priority (aging) to possibly "upgrade" where it competes.
                    eff_pr = _effective_priority(p)
                    # If it was promoted, move it to the right queue and keep searching.
                    if eff_pr != p.priority:
                        s.queues[eff_pr].append(p)
                        continue

                    op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
                    if not op_list:
                        # Nothing ready; keep it in its queue for later.
                        q.append(p)
                        continue

                    # Check if the op likely fits this pool right now (RAM gating).
                    pipeline_id = p.pipeline_id
                    op = op_list[0]
                    cpu_req, ram_req = _size_for_op(pool, p.priority, pipeline_id, op, avail_cpu, avail_ram)

                    if ram_req <= avail_ram and cpu_req >= s.min_cpu and avail_cpu >= s.min_cpu:
                        # Put back at end; caller will actually schedule and we want round-robin within prio.
                        q.append(p)
                        # Consume one unit of deficit for its (original) priority.
                        s.deficit[p.priority] -= 1
                        return p, op_list, cpu_req, ram_req

                    # Doesn't fit now; rotate to end and keep searching others.
                    q.append(p)

        return None, None, None, None

    # Enqueue new pipelines
    for p in pipelines:
        _ensure_pipeline(p)

    # Update learned profiles from execution results
    for r in results:
        pipeline_id = _infer_pipeline_id_from_result(r)
        ops = getattr(r, "ops", []) or []

        for op in ops:
            key = _op_key(pipeline_id, op)
            prof = s.op_profile.get(key)
            if prof is None:
                prof = {"ram_suggest": None, "cpu_suggest": None, "ooms": 0, "fails": 0}
                s.op_profile[key] = prof

            if r.failed():
                prof["fails"] = int(prof.get("fails", 0)) + 1

                if _is_oom_error(getattr(r, "error", None)):
                    prof["ooms"] = int(prof.get("ooms", 0)) + 1
                    # Escalate RAM quickly to converge to a successful run.
                    prev_ram = float(getattr(r, "ram", 0.0) or 0.0)
                    if prev_ram > 0:
                        next_ram = prev_ram * 2.0
                        if prof.get("ram_suggest") is None:
                            prof["ram_suggest"] = next_ram
                        else:
                            prof["ram_suggest"] = max(float(prof["ram_suggest"]), next_ram)
                else:
                    # For non-OOM failures, slightly increase CPU to reduce timeout-like failures.
                    prev_cpu = float(getattr(r, "cpu", 0.0) or 0.0)
                    if prev_cpu > 0:
                        next_cpu = min(s.max_cpu_cap, max(s.min_cpu, prev_cpu * 1.25))
                        if prof.get("cpu_suggest") is None:
                            prof["cpu_suggest"] = next_cpu
                        else:
                            prof["cpu_suggest"] = max(float(prof["cpu_suggest"]), next_cpu)
            else:
                # Success: record a conservative "known good" RAM suggestion (don't reduce).
                used_ram = float(getattr(r, "ram", 0.0) or 0.0)
                used_cpu = float(getattr(r, "cpu", 0.0) or 0.0)
                if used_ram > 0:
                    prof["ram_suggest"] = max(float(prof.get("ram_suggest") or 0.0), used_ram)
                if used_cpu > 0:
                    prof["cpu_suggest"] = max(float(prof.get("cpu_suggest") or 0.0), min(used_cpu, s.max_cpu_cap))

    # If nothing changed, avoid doing work
    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    # Age all queued pipelines (turn-based)
    for pr in list(s.queues.keys()):
        for p in s.queues[pr]:
            pid = p.pipeline_id
            s.wait_turns[pid] = s.wait_turns.get(pid, 0) + 1

    # Schedule across pools, filling each pool as much as possible
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool

        # Try to place multiple ops into this pool until resources are tight
        # (CPU can be packed; RAM is the main admission gate to avoid OOM).
        safety_iters = 0
        while avail_cpu >= s.min_cpu and avail_ram > 0:
            safety_iters += 1
            if safety_iters > 2000:
                # Hard stop in case of unexpected queue/rotation behavior
                break

            p, op_list, cpu_req, ram_req = _choose_from_queues(pool, avail_cpu, avail_ram)
            if p is None:
                break

            # Re-check status (it may have changed due to previous assignments in this tick)
            status = p.runtime_status()
            if status.is_pipeline_successful():
                pid = p.pipeline_id
                s.known_pipelines.discard(pid)
                s.wait_turns.pop(pid, None)
                continue

            # Ensure op is still assignable and parents complete
            fresh_ops = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
            if not fresh_ops:
                continue

            # If the chosen op changed, re-size quickly
            op_list = fresh_ops
            op = op_list[0]
            cpu_req, ram_req = _size_for_op(pool, p.priority, p.pipeline_id, op, avail_cpu, avail_ram)

            if ram_req > avail_ram or cpu_req < s.min_cpu or avail_cpu < s.min_cpu:
                # Can't fit right now; stop trying to pack this pool further.
                break

            assignments.append(
                Assignment(
                    ops=op_list,
                    cpu=cpu_req,
                    ram=ram_req,
                    priority=p.priority,
                    pool_id=pool_id,
                    pipeline_id=p.pipeline_id,
                )
            )

            # Reset wait counter on actual scheduling to reduce over-promotion.
            s.wait_turns[p.pipeline_id] = 0

            avail_cpu -= cpu_req
            avail_ram -= ram_req

    return suspensions, assignments
