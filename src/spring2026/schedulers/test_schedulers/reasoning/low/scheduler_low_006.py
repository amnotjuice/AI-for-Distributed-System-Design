# policy_key: scheduler_low_006
# reasoning_effort: low
# model: gpt-5.2-2025-12-11
# llm_cost: 0.046589
# generation_seconds: 47.61
# generated_at: 2026-04-09T21:06:42.028717
@register_scheduler_init(key="scheduler_low_006")
def scheduler_low_006_init(s):
    """Priority-first, failure-averse scheduler with conservative parallelism.

    Goals (aligned to weighted latency objective):
      - Strongly protect QUERY and INTERACTIVE latency via strict priority order.
      - Avoid pipeline failures by learning per-operator RAM from failures and retrying with RAM bumps.
      - Prevent starvation by aging BATCH pipelines into service if they wait too long.
      - Improve throughput without increasing OOM risk by using moderate CPU per op (more for high priority),
        allowing multiple concurrent ops per pool when possible.

    Key ideas:
      - Maintain per-priority waiting queues.
      - For each tick/pool, repeatedly place at most one ready op per pipeline until pool resources are exhausted.
      - RAM sizing: start with a priority-based fraction of pool RAM; on failure, bump learned RAM for that op.
      - CPU sizing: priority-based caps (QUERY > INTERACTIVE > BATCH) to reduce tail latency while keeping concurrency.
      - Aging: if BATCH has been waiting beyond a threshold, allow it to be scheduled even if high-priority exists.
    """
    from collections import deque

    # Per-priority FIFO queues of pipelines awaiting scheduling
    s.q_query = deque()
    s.q_interactive = deque()
    s.q_batch = deque()

    # Pipeline bookkeeping
    s.pipeline_enqueued_at = {}   # pipeline_id -> first enqueue "time"
    s.pipeline_last_seen = {}     # pipeline_id -> last time we saw it (avoid duplicates)

    # Learned resource hints
    # op_key -> ram_required (float), inferred from last successful/failed run
    s.op_ram_hint = {}

    # pipeline_id -> number of failures observed (to cap runaway retries)
    s.pipeline_failures = {}

    # A simple monotonically increasing tick counter (simulator is deterministic)
    s.tick = 0

    # Tuning knobs
    s.max_failures_per_pipeline = 6          # retry budget before we stop inflating hints aggressively
    s.batch_aging_threshold_ticks = 30       # after this, batch can "jump the line" occasionally
    s.max_assignments_per_pool_per_tick = 8  # avoid too much churn in a single tick


def _priority_rank(priority):
    # Smaller is higher priority
    if priority == Priority.QUERY:
        return 0
    if priority == Priority.INTERACTIVE:
        return 1
    return 2


def _queue_for_priority(s, priority):
    if priority == Priority.QUERY:
        return s.q_query
    if priority == Priority.INTERACTIVE:
        return s.q_interactive
    return s.q_batch


def _op_key(op):
    # Best-effort stable operator key across ticks
    if hasattr(op, "op_id"):
        return ("op_id", op.op_id)
    if hasattr(op, "operator_id"):
        return ("operator_id", op.operator_id)
    if hasattr(op, "name"):
        return ("name", op.name, id(op))
    return ("pyid", id(op))


def _infer_is_oom(error):
    if not error:
        return False
    try:
        msg = str(error).lower()
    except Exception:
        return False
    return ("oom" in msg) or ("out of memory" in msg) or ("memoryerror" in msg) or ("killed" in msg)


def _base_ram_fraction(priority):
    # Start with a meaningful chunk to reduce OOM probability (failures are very expensive in the objective).
    if priority == Priority.QUERY:
        return 0.55
    if priority == Priority.INTERACTIVE:
        return 0.40
    return 0.30


def _cpu_cap(priority):
    # Cap CPU to allow concurrency; queries get more CPU to reduce tail latency.
    if priority == Priority.QUERY:
        return 8.0
    if priority == Priority.INTERACTIVE:
        return 4.0
    return 2.0


def _choose_next_pipeline(s):
    """Pick next pipeline with strict priority, but allow aging batch to prevent starvation."""
    # Aging: if oldest batch has waited long enough, allow it to be picked even if others exist.
    if s.q_batch:
        oldest_batch = s.q_batch[0]
        enq = s.pipeline_enqueued_at.get(oldest_batch.pipeline_id, s.tick)
        if (s.tick - enq) >= s.batch_aging_threshold_ticks:
            return s.q_batch.popleft()

    if s.q_query:
        return s.q_query.popleft()
    if s.q_interactive:
        return s.q_interactive.popleft()
    if s.q_batch:
        return s.q_batch.popleft()
    return None


def _enqueue_pipeline(s, p):
    """Enqueue pipeline once, tracking its first-seen time."""
    q = _queue_for_priority(s, p.priority)
    pid = p.pipeline_id

    # Avoid uncontrolled duplicates: only enqueue if it isn't already present in any queue.
    # (Linear scan is OK for simulation sizes; keeps logic robust.)
    for qq in (s.q_query, s.q_interactive, s.q_batch):
        for existing in qq:
            if existing.pipeline_id == pid:
                return

    q.append(p)
    if pid not in s.pipeline_enqueued_at:
        s.pipeline_enqueued_at[pid] = s.tick
    s.pipeline_last_seen[pid] = s.tick


def _size_resources_for_op(s, pool, priority, op):
    """Return (cpu, ram) for an op in the given pool, using hints and safe defaults."""
    # RAM: start from priority-based fraction of pool max RAM, then apply learned hint if larger.
    base_ram = max(0.0, _base_ram_fraction(priority) * float(pool.max_ram_pool))
    hint = s.op_ram_hint.get(_op_key(op))
    if hint is not None:
        # Add small headroom to reduce repeated OOM bounces
        base_ram = max(base_ram, float(hint) * 1.10)

    # Respect available RAM
    ram = min(float(pool.avail_ram_pool), base_ram)
    # If we can't even allocate a minimal meaningful amount, leave it to admission control (caller checks).
    # CPU: cap by priority, but never exceed available.
    cpu = min(float(pool.avail_cpu_pool), _cpu_cap(priority))
    # Give at least 1 CPU if possible
    if cpu < 1.0 and float(pool.avail_cpu_pool) >= 1.0:
        cpu = 1.0

    return cpu, ram


@register_scheduler(key="scheduler_low_006")
def scheduler_low_006(s, results: list, pipelines: list):
    """
    Priority-first + OOM-avoidant retry scheduler.

    Mechanics per tick:
      1) Ingest new pipelines into per-priority queues.
      2) Update RAM hints from execution results:
         - On OOM-like failure: bump per-op RAM hint aggressively.
         - On success: record observed RAM allocation as a lower-bound-ish hint.
      3) For each pool, schedule ready operators:
         - Select next pipeline by priority with batch aging.
         - Assign at most 1 ready operator per pipeline per iteration.
         - Continue until pool resources are too low or max assignments reached.
      4) Requeue pipelines that are not completed.

    Preemption is intentionally avoided here (no reliable introspection of running containers in the provided API);
    instead we focus on getting high completion rate and low tail latency via sizing and priority admission.
    """
    s.tick += 1

    # Ingest newly arrived pipelines
    for p in pipelines:
        _enqueue_pipeline(s, p)

    # Update hints from results (learn RAM needs; treat OOM specially)
    if results:
        for r in results:
            # Update per-pipeline failure counts
            try:
                pid = getattr(r, "pipeline_id", None)
            except Exception:
                pid = None

            failed = False
            try:
                failed = r.failed()
            except Exception:
                failed = bool(getattr(r, "error", None))

            # Learn from operator execution. r.ops is expected to be a list of ops.
            ops = getattr(r, "ops", None) or []
            for op in ops:
                k = _op_key(op)
                # If OOM, bump hint beyond what we tried; otherwise keep a conservative lower bound.
                if failed and _infer_is_oom(getattr(r, "error", None)):
                    prev = s.op_ram_hint.get(k, 0.0)
                    tried = float(getattr(r, "ram", 0.0) or 0.0)
                    # Aggressive bump to reduce repeated OOMs (which risk 720s penalties).
                    bumped = max(prev, tried) * 1.50
                    # If tried is zero/unknown, still bump to something meaningful on next run.
                    if bumped <= 0.0:
                        bumped = 0.35 * float(s.executor.pools[getattr(r, "pool_id", 0)].max_ram_pool)
                    s.op_ram_hint[k] = bumped
                else:
                    # On success (or non-OOM failure), treat allocated RAM as a weak signal.
                    tried = float(getattr(r, "ram", 0.0) or 0.0)
                    if tried > 0.0:
                        prev = s.op_ram_hint.get(k, 0.0)
                        s.op_ram_hint[k] = max(prev, tried * 0.90)

            # Track pipeline-level failures to avoid infinite escalation
            if failed and pid is not None:
                s.pipeline_failures[pid] = s.pipeline_failures.get(pid, 0) + 1

    # Early exit if nothing changed
    if not pipelines and not results:
        return [], []

    suspensions = []  # no preemption in this policy
    assignments = []

    # Helper: requeue pipelines that still have work
    def requeue_if_needed(p):
        st = p.runtime_status()
        if st.is_pipeline_successful():
            return
        # Keep retrying failed ops (ASSIGNABLE_STATES includes FAILED) but don't blow up RAM forever.
        _enqueue_pipeline(s, p)

    # Schedule per pool
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        made = 0

        while made < s.max_assignments_per_pool_per_tick:
            avail_cpu = float(pool.avail_cpu_pool)
            avail_ram = float(pool.avail_ram_pool)
            if avail_cpu < 1.0 or avail_ram <= 0.0:
                break

            pipeline = _choose_next_pipeline(s)
            if pipeline is None:
                break

            status = pipeline.runtime_status()

            # If successful, don't requeue
            if status.is_pipeline_successful():
                continue

            # If the pipeline has failed too many times, still keep trying (dropping is expensive),
            # but stop aggressive bumping; scheduling continues as normal.
            pid = pipeline.pipeline_id
            failures = s.pipeline_failures.get(pid, 0)

            # Get one ready operator (parents complete)
            op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
            if not op_list:
                # Nothing ready; requeue and move on
                requeue_if_needed(pipeline)
                continue

            op = op_list[0]
            cpu, ram = _size_resources_for_op(s, pool, pipeline.priority, op)

            # Admission control: if our computed RAM is tiny, avoid scheduling to prevent guaranteed OOM loops.
            # For very failure-prone pipelines, allow scheduling even with smaller RAM if that's all we have.
            min_meaningful_ram = 0.10 * float(pool.max_ram_pool)
            if ram < min_meaningful_ram and avail_ram < min_meaningful_ram and failures < s.max_failures_per_pipeline:
                # Wait for more RAM to free rather than risk an OOM and 720s penalty.
                requeue_if_needed(pipeline)
                break

            # If we can't allocate at least 1 CPU, wait.
            if cpu < 1.0:
                requeue_if_needed(pipeline)
                break

            assignment = Assignment(
                ops=op_list,
                cpu=cpu,
                ram=ram,
                priority=pipeline.priority,
                pool_id=pool_id,
                pipeline_id=pipeline.pipeline_id,
            )
            assignments.append(assignment)
            made += 1

            # Requeue the pipeline to make forward progress later (DAG likely has more ops)
            requeue_if_needed(pipeline)

    return suspensions, assignments
