# policy_key: scheduler_none_038
# reasoning_effort: none
# model: gpt-5.2-2025-12-11
# llm_cost: 0.039941
# generation_seconds: 42.50
# generated_at: 2026-03-14T02:08:43.595791
@register_scheduler_init(key="scheduler_none_038")
def scheduler_none_038_init(s):
    """Priority-aware, low-churn scheduler (incremental upgrade over naive FIFO).

    Improvements over the naive example:
      1) Priority queues: always try to schedule QUERY/INTERACTIVE before BATCH.
      2) Per-pool headroom reservation: keep some CPU/RAM available for high priority.
      3) Conservative sizing: don't blindly give an op the entire pool; cap per-assignment
         to reduce interference and enable concurrency.
      4) Simple OOM adaptation: if an op OOMs, retry with larger RAM next time.

    Design goals:
      - Reduce tail latency for interactive work (admission + reserved headroom).
      - Avoid preemption churn (no preemption in this first step; only admission control).
      - Maintain throughput by allowing some batch to run when headroom exists.
    """
    # Separate waiting queues per priority (pipelines are re-enqueued until completion).
    s.wait_q = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }

    # Track per-(pipeline, op) memory multipliers to adapt after OOM.
    # Key: (pipeline_id, op_id) -> multiplier (>= 1.0)
    s.op_ram_mult = {}

    # A small round-robin cursor to avoid always starting from pool 0.
    s.pool_rr = 0

    # Configuration knobs (kept simple and conservative).
    s.cfg = {
        # Keep headroom in each pool for higher priority work.
        "reserve_high_pri_cpu_frac": 0.25,
        "reserve_high_pri_ram_frac": 0.25,
        # Cap single assignment so we can run multiple ops concurrently.
        "max_assign_cpu_frac": 0.60,
        "max_assign_ram_frac": 0.60,
        # Ensure interactive work gets a reasonable minimum slice when possible.
        "min_interactive_cpu_frac": 0.15,
        "min_interactive_ram_frac": 0.15,
        # OOM backoff multiplier.
        "oom_ram_bump": 1.5,
        "oom_ram_cap_frac": 0.95,  # never request more than this fraction of pool RAM
    }


def _prio_order():
    return [Priority.QUERY, Priority.INTERACTIVE, Priority.BATCH_PIPELINE]


def _safe_op_id(op):
    # Best-effort stable identifier for an operator across ticks.
    # Many simulators expose op_id; if not, fall back to Python object id (stable during run).
    return getattr(op, "op_id", None) or getattr(op, "operator_id", None) or id(op)


def _pipeline_is_done_or_failed(p):
    st = p.runtime_status()
    has_failures = st.state_counts[OperatorState.FAILED] > 0
    return st.is_pipeline_successful() or has_failures


def _enqueue_pipeline(s, p):
    # Ensure unknown priorities don't break scheduling.
    if p.priority not in s.wait_q:
        s.wait_q[p.priority] = []
    s.wait_q[p.priority].append(p)


def _next_assignable_ops(pipeline):
    st = pipeline.runtime_status()
    ops = st.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)
    if not ops:
        return []
    # Only schedule one op at a time for now (small improvement step, lower risk).
    return ops[:1]


def _compute_headroom(s, pool, priority):
    # For high priority work, allow using almost everything.
    if priority in (Priority.QUERY, Priority.INTERACTIVE):
        return 0.0, 0.0
    # For batch, preserve some resources as headroom for potential high-priority arrivals.
    return (
        pool.max_cpu_pool * s.cfg["reserve_high_pri_cpu_frac"],
        pool.max_ram_pool * s.cfg["reserve_high_pri_ram_frac"],
    )


def _cap_request(s, pool, cpu_req, ram_req):
    # Cap at a fraction of pool to avoid hogging, but never exceed available either.
    cpu_cap = max(0.0, pool.max_cpu_pool * s.cfg["max_assign_cpu_frac"])
    ram_cap = max(0.0, pool.max_ram_pool * s.cfg["max_assign_ram_frac"])
    cpu_req = min(cpu_req, cpu_cap)
    ram_req = min(ram_req, ram_cap)
    cpu_req = min(cpu_req, pool.avail_cpu_pool)
    ram_req = min(ram_req, pool.avail_ram_pool)
    return cpu_req, ram_req


def _min_slice_for_interactive(s, pool, cpu_req, ram_req, priority):
    if priority not in (Priority.QUERY, Priority.INTERACTIVE):
        return cpu_req, ram_req

    min_cpu = pool.max_cpu_pool * s.cfg["min_interactive_cpu_frac"]
    min_ram = pool.max_ram_pool * s.cfg["min_interactive_ram_frac"]
    # If we have enough available, bump up to minimum slice to reduce latency.
    if pool.avail_cpu_pool >= min_cpu:
        cpu_req = max(cpu_req, min_cpu)
    if pool.avail_ram_pool >= min_ram:
        ram_req = max(ram_req, min_ram)
    # Still respect availability.
    cpu_req = min(cpu_req, pool.avail_cpu_pool)
    ram_req = min(ram_req, pool.avail_ram_pool)
    return cpu_req, ram_req


def _ram_multiplier(s, pipeline_id, op):
    k = (pipeline_id, _safe_op_id(op))
    return s.op_ram_mult.get(k, 1.0)


def _maybe_update_oom_model(s, results):
    # If an op failed due to OOM, increase its RAM multiplier for retries.
    for r in results:
        if not r.failed():
            continue
        # Heuristic: treat any error containing "oom" (case-insensitive) as OOM.
        err = str(r.error).lower() if r.error is not None else ""
        if "oom" in err or "out of memory" in err:
            for op in getattr(r, "ops", []) or []:
                k = (getattr(r, "pipeline_id", None), _safe_op_id(op))
                if k[0] is None:
                    # If pipeline_id not present on result, we can't key reliably.
                    # Fall back to using container_id to at least prevent repeated tiny retries.
                    k = (("container", r.container_id), _safe_op_id(op))
                cur = s.op_ram_mult.get(k, 1.0)
                s.op_ram_mult[k] = min(cur * s.cfg["oom_ram_bump"], 64.0)


@register_scheduler(key="scheduler_none_038")
def scheduler_none_038(s, results, pipelines):
    """
    Priority-aware admission + headroom reservation + conservative sizing.

    Strategy each tick:
      - Ingest new pipelines into per-priority queues.
      - Learn from failures: bump RAM for OOMing ops.
      - For each pool (round-robin start), try to fill it with:
          1) QUERY ops
          2) INTERACTIVE ops
          3) BATCH ops (only if above reserved headroom)
      - Schedule at most one ready op per pipeline per tick (low risk, stable).
    """
    for p in pipelines:
        _enqueue_pipeline(s, p)

    if not pipelines and not results:
        return [], []

    _maybe_update_oom_model(s, results)

    suspensions = []  # no preemption yet (small, safe improvement)
    assignments = []

    # Round-robin start pool index to avoid systematic bias.
    start_pool = s.pool_rr % max(1, s.executor.num_pools)
    s.pool_rr = (s.pool_rr + 1) % max(1, s.executor.num_pools)

    pool_indices = list(range(s.executor.num_pools))
    pool_indices = pool_indices[start_pool:] + pool_indices[:start_pool]

    # We'll requeue pipelines we touched; others remain in their queues.
    touched = {Priority.QUERY: [], Priority.INTERACTIVE: [], Priority.BATCH_PIPELINE: []}

    for pool_id in pool_indices:
        pool = s.executor.pools[pool_id]

        # If nothing available, skip.
        if pool.avail_cpu_pool <= 0 or pool.avail_ram_pool <= 0:
            continue

        # Try to place multiple assignments in a pool if resources allow.
        made_progress = True
        while made_progress:
            made_progress = False

            # Try priorities in order.
            for prio in _prio_order():
                q = s.wait_q.get(prio, [])
                if not q:
                    continue

                # Compute headroom only when scheduling this priority.
                head_cpu, head_ram = _compute_headroom(s, pool, prio)

                # If batch and we're below headroom, don't schedule batch here.
                if prio == Priority.BATCH_PIPELINE:
                    if pool.avail_cpu_pool <= head_cpu or pool.avail_ram_pool <= head_ram:
                        continue

                # Find the first pipeline in this priority queue with a ready op.
                picked_idx = None
                picked_pipeline = None
                picked_ops = None

                # Small scan limit to avoid O(n^2) behavior on large queues.
                scan_limit = min(len(q), 32)
                for i in range(scan_limit):
                    p = q[i]
                    if _pipeline_is_done_or_failed(p):
                        touched.setdefault(prio, []).append(p)
                        continue
                    ops = _next_assignable_ops(p)
                    if ops:
                        picked_idx = i
                        picked_pipeline = p
                        picked_ops = ops
                        break

                if picked_pipeline is None:
                    # Clean out any done/failed pipelines we might have encountered at front.
                    # (We don't aggressively purge entire queues to keep this cheap.)
                    # Also avoid infinite loops.
                    continue

                # Remove picked pipeline from queue for this tick (will be requeued).
                q.pop(picked_idx)
                touched.setdefault(prio, []).append(picked_pipeline)

                op0 = picked_ops[0]

                # Request sizing:
                # - Start with a fair share, then cap, then apply interactive minimums.
                # - Apply RAM multiplier if we've seen OOM for this op before.
                # Baseline share: for high priority, be more generous; for batch, more conservative.
                if prio in (Priority.QUERY, Priority.INTERACTIVE):
                    cpu_req = max(1.0, pool.avail_cpu_pool * 0.5)
                    ram_req = pool.avail_ram_pool * 0.5
                else:
                    cpu_req = max(1.0, pool.avail_cpu_pool * 0.33)
                    ram_req = pool.avail_ram_pool * 0.33

                # Respect reserved headroom for batch by reducing effective availability.
                if prio == Priority.BATCH_PIPELINE:
                    cpu_req = min(cpu_req, max(0.0, pool.avail_cpu_pool - head_cpu))
                    ram_req = min(ram_req, max(0.0, pool.avail_ram_pool - head_ram))

                # Apply OOM learning for RAM.
                mult = _ram_multiplier(s, picked_pipeline.pipeline_id, op0)
                ram_req = ram_req * mult
                ram_req = min(ram_req, pool.max_ram_pool * s.cfg["oom_ram_cap_frac"])

                # Cap per assignment to prevent hogging.
                cpu_req, ram_req = _cap_request(s, pool, cpu_req, ram_req)

                # Ensure interactive minimum slice where possible.
                cpu_req, ram_req = _min_slice_for_interactive(s, pool, cpu_req, ram_req, prio)

                # Final feasibility.
                if cpu_req <= 0 or ram_req <= 0:
                    # Can't run now; put it back and try next pool/priority.
                    _enqueue_pipeline(s, picked_pipeline)
                    continue

                assignment = Assignment(
                    ops=picked_ops,
                    cpu=cpu_req,
                    ram=ram_req,
                    priority=picked_pipeline.priority,
                    pool_id=pool_id,
                    pipeline_id=picked_pipeline.pipeline_id,
                )
                assignments.append(assignment)

                # Assume resources will be consumed; we can't mutate pool, but we can stop
                # trying if it looks like we're exhausted to reduce over-assignment risk.
                # (Executor should reject if overcommitted; we remain conservative.)
                if pool.avail_cpu_pool - cpu_req <= 0.5 or pool.avail_ram_pool - ram_req <= (0.05 * pool.max_ram_pool):
                    made_progress = False
                else:
                    made_progress = True

                # After scheduling one op, restart priority loop to keep strict priority.
                break

    # Re-enqueue pipelines we touched (unless they are done/failed now).
    for prio, lst in touched.items():
        for p in lst:
            if not _pipeline_is_done_or_failed(p):
                _enqueue_pipeline(s, p)

    return suspensions, assignments
