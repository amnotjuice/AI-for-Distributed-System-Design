# policy_key: scheduler_medium_008
# reasoning_effort: medium
# model: gpt-5.2-2025-12-11
# llm_cost: 0.086522
# generation_seconds: 68.61
# generated_at: 2026-03-12T22:03:02.350013
@register_scheduler_init(key="scheduler_medium_008")
def scheduler_medium_008_init(s):
    """Priority-aware, multi-assignment packing scheduler (small step up from naive FIFO).

    Main improvements vs. naive example:
      1) Strict priority queues (QUERY > INTERACTIVE > BATCH_PIPELINE) to improve tail latency.
      2) Avoid assigning *all* pool resources to a single operator; instead pack multiple ops per tick
         using per-priority resource caps (keeps room for high-priority work, improves concurrency).
      3) Lightweight OOM-aware RAM backoff: if a pipeline's op fails with an OOM-like error, retry
         future ops from that pipeline with a higher RAM multiplier (bounded by pool capacity).
      4) Round-robin within each priority to reduce head-of-line blocking among same-priority pipelines.
    """
    # Per-priority waiting queues of pipelines (append on arrival, rotate for RR fairness).
    s.waiting_queues = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }

    # Per-pipeline hints learned from prior results.
    #  - ram_mult: multiplicative bump to default RAM request (for OOM backoff)
    #  - last_failure_oom: whether the last observed failure looked like OOM
    s.pipeline_hints = {}

    # Optional mild anti-starvation knob: after N high-priority assignments, allow a batch assignment
    # if there is leftover capacity.
    s.max_consecutive_high = 4


@register_scheduler(key="scheduler_medium_008")
def scheduler_medium_008_scheduler(s, results, pipelines):
    """
    Priority scheduler with RR fairness, resource capping, and OOM RAM backoff.

    Returns:
        (suspensions, assignments)
    """
    # Fast path if nothing changed.
    if not results and not pipelines:
        return [], []

    suspensions = []
    assignments = []

    def _prio(p):
        # Defensive: unknown priority treated as batch.
        return p if p in s.waiting_queues else Priority.BATCH_PIPELINE

    def _get_hint(pipeline_id):
        hint = s.pipeline_hints.get(pipeline_id)
        if hint is None:
            hint = {"ram_mult": 1.0, "last_failure_oom": False}
            s.pipeline_hints[pipeline_id] = hint
        return hint

    def _is_oom_error(err):
        if not err:
            return False
        try:
            msg = str(err).lower()
        except Exception:
            return False
        return ("oom" in msg) or ("out of memory" in msg) or ("memoryerror" in msg)

    # Enqueue new pipelines.
    for p in pipelines:
        pr = _prio(p.priority)
        s.waiting_queues[pr].append(p)
        _get_hint(p.pipeline_id)  # ensure hint exists

    # Learn from execution results (only lightweight signal handling).
    for r in results:
        # Try to key hints by pipeline_id. If it's not present on result, skip learning.
        pipeline_id = getattr(r, "pipeline_id", None)
        if pipeline_id is None:
            continue

        hint = _get_hint(pipeline_id)
        if hasattr(r, "failed") and r.failed():
            if _is_oom_error(getattr(r, "error", None)):
                hint["last_failure_oom"] = True
                # Exponential-ish backoff, bounded.
                hint["ram_mult"] = min(hint.get("ram_mult", 1.0) * 1.6, 16.0)
            else:
                hint["last_failure_oom"] = False

    # Priority order: schedule highest priority first for latency.
    prio_order = [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]

    # Default resource "caps" per assignment, expressed as fractions of pool max.
    # NOTE: These are caps (upper bounds), not guarantees; we also bound by available capacity.
    # When high-priority work exists, cap batch smaller to preserve headroom.
    caps_when_high_waiting = {
        Priority.QUERY: {"cpu_frac": 0.75, "ram_frac": 0.70},
        Priority.INTERACTIVE: {"cpu_frac": 0.65, "ram_frac": 0.65},
        Priority.BATCH_PIPELINE: {"cpu_frac": 0.35, "ram_frac": 0.45},
    }
    caps_when_no_high_waiting = {
        Priority.QUERY: {"cpu_frac": 0.80, "ram_frac": 0.75},
        Priority.INTERACTIVE: {"cpu_frac": 0.70, "ram_frac": 0.70},
        Priority.BATCH_PIPELINE: {"cpu_frac": 0.70, "ram_frac": 0.75},
    }

    # Avoid assigning multiple ops from the same pipeline in the same tick (keeps planning simple).
    scheduled_pipeline_ids = set()

    def _pipeline_done_or_drop(p):
        """Return True if pipeline should be removed (completed or definitively failed)."""
        status = p.runtime_status()
        if status.is_pipeline_successful():
            return True

        # If pipeline has failures, only keep it if last observed failure looked like OOM
        # (we'll retry with more RAM). Otherwise drop to avoid infinite retries.
        has_failures = status.state_counts[OperatorState.FAILED] > 0
        if has_failures:
            hint = _get_hint(p.pipeline_id)
            return not bool(hint.get("last_failure_oom", False))

        return False

    def _next_assignable_op(p):
        status = p.runtime_status()
        # Only schedule ops whose parents are complete.
        op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
        return op_list

    def _request_for(p, pool, caps):
        """Compute cpu/ram request caps for a pipeline on a given pool."""
        pr = _prio(p.priority)
        hint = _get_hint(p.pipeline_id)

        # Base caps.
        cpu_cap = max(1.0, pool.max_cpu_pool * float(caps[pr]["cpu_frac"]))
        ram_cap = max(1.0, pool.max_ram_pool * float(caps[pr]["ram_frac"]))

        # OOM backoff increases RAM cap (still bounded later by availability).
        ram_cap = min(pool.max_ram_pool, ram_cap * float(hint.get("ram_mult", 1.0)))

        return cpu_cap, ram_cap

    # Schedule per pool, packing multiple assignments without exhausting on a single op.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool

        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # Determine if we should be conservative to protect high-priority latency.
        high_waiting = (len(s.waiting_queues[Priority.QUERY]) + len(s.waiting_queues[Priority.INTERACTIVE])) > 0
        caps = caps_when_high_waiting if high_waiting else caps_when_no_high_waiting

        consecutive_high = 0

        # Bound the number of assignments per pool per tick to avoid excessive looping.
        # (Keeps behavior stable/deterministic while still enabling packing.)
        max_assignments_this_pool = 8

        made_progress = True
        assignments_this_pool = 0

        while made_progress and assignments_this_pool < max_assignments_this_pool:
            made_progress = False

            # Mild anti-starvation: after a few high-priority placements, allow one batch if possible.
            if consecutive_high >= s.max_consecutive_high and s.waiting_queues[Priority.BATCH_PIPELINE]:
                candidate_prios = [Priority.BATCH_PIPELINE] + [Priority.QUERY, Priority.INTERACTIVE]
            else:
                candidate_prios = prio_order

            for pr in candidate_prios:
                q = s.waiting_queues[pr]
                if not q:
                    continue

                # Round-robin: rotate through queue until we either schedule something or exhaust attempts.
                attempts = len(q)
                while attempts > 0 and avail_cpu > 0 and avail_ram > 0:
                    attempts -= 1
                    p = q.pop(0)

                    # Skip if already scheduled this tick.
                    if p.pipeline_id in scheduled_pipeline_ids:
                        q.append(p)
                        continue

                    # Drop completed / definitively failed pipelines.
                    if _pipeline_done_or_drop(p):
                        continue

                    op_list = _next_assignable_op(p)
                    if not op_list:
                        # Not ready yet; keep it.
                        q.append(p)
                        continue

                    # Compute capped request and then bound by available capacity.
                    cpu_cap, ram_cap = _request_for(p, pool, caps)

                    cpu_req = min(avail_cpu, cpu_cap)
                    ram_req = min(avail_ram, ram_cap)

                    # If we can't give at least some meaningful resources, stop for this pool.
                    if cpu_req <= 0 or ram_req <= 0:
                        q.insert(0, p)
                        attempts = 0
                        break

                    # Make assignment.
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

                    # Update local pool availability to pack more assignments.
                    avail_cpu -= cpu_req
                    avail_ram -= ram_req

                    scheduled_pipeline_ids.add(p.pipeline_id)
                    q.append(p)  # keep pipeline for future ops

                    # Update high-priority streak.
                    if pr in (Priority.QUERY, Priority.INTERACTIVE):
                        consecutive_high += 1
                    else:
                        consecutive_high = 0

                    made_progress = True
                    assignments_this_pool += 1
                    break  # move to next priority / next placement

                if made_progress:
                    break  # restart outer loop with updated availability

    return suspensions, assignments
