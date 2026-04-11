# policy_key: scheduler_low_040
# reasoning_effort: low
# model: gpt-5.2-2025-12-11
# llm_cost: 0.060421
# generation_seconds: 54.53
# generated_at: 2026-04-09T21:34:48.856586
@register_scheduler_init(key="scheduler_low_040")
def scheduler_low_040_init(s):
    """Priority-aware, failure-avoiding scheduler with gentle fairness.

    Core ideas:
      - Strict preference for QUERY then INTERACTIVE, with aging to avoid BATCH starvation.
      - Conservative initial sizing + adaptive RAM backoff on failures to reduce repeated OOMs.
      - Avoid preemption (churn/waste) and instead rely on priority-biased admission decisions.
      - Keep retry limits to prevent infinite thrash; be more patient for high-priority pipelines.
    """
    # Canonical storage for active pipelines (so we keep scheduling across ticks)
    s.pipeline_map = {}          # pipeline_id -> Pipeline
    s.arrival_tick = {}          # pipeline_id -> tick when first seen
    s.last_scheduled_tick = {}   # pipeline_id -> last tick when we issued an assignment
    s.tick = 0

    # Per-pipeline adaptive sizing hints (learned from failures)
    s.ram_hint = {}              # pipeline_id -> last known-good/attempted RAM
    s.cpu_hint = {}              # pipeline_id -> last attempted CPU
    s.fail_count = {}            # pipeline_id -> count of failed execution results observed

    # Track per-pipeline "cooldown" after failure to avoid immediate retry thrash
    s.cooldown_until = {}        # pipeline_id -> tick; don't schedule before this

    # Simple queues per priority (store pipeline_id; we resolve to pipeline via map)
    s.queues = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }
    s.in_queue = set()  # pipeline_ids currently enqueued (in any priority queue)


def _prio_weight(p):
    if p == Priority.QUERY:
        return 10
    if p == Priority.INTERACTIVE:
        return 5
    return 1


def _max_failures_allowed(priority):
    # Be more patient for higher priority to maximize completion rate.
    if priority == Priority.QUERY:
        return 8
    if priority == Priority.INTERACTIVE:
        return 6
    return 4


def _base_share(priority):
    # Baseline fraction of pool to allocate for a single operator attempt.
    # We intentionally avoid allocating the entire pool to reduce interference and
    # keep headroom for higher priority arrivals in the next ticks.
    if priority == Priority.QUERY:
        return 0.65
    if priority == Priority.INTERACTIVE:
        return 0.50
    return 0.35


def _min_cpu(priority):
    if priority == Priority.QUERY:
        return 2
    if priority == Priority.INTERACTIVE:
        return 1
    return 1


def _pick_next_pipeline_id(s, pool_id):
    """Pick next pipeline to run for a pool, prioritizing QUERY/INTERACTIVE.

    Adds aging so BATCH pipelines make progress if they wait long enough.
    """
    # Aging threshold: after this many ticks waiting, batch can compete.
    # Keep modest: protect p95/p99 for query/interactive, but avoid starvation penalties.
    batch_aging_ticks = 25

    # Helper to score a candidate pipeline.
    def score(pid):
        p = s.pipeline_map[pid]
        age = s.tick - s.arrival_tick.get(pid, s.tick)
        # Strongly prefer higher priority; add age to prevent starvation.
        return (_prio_weight(p.priority) * 1000) + min(age, 500)

    # Filter out pipelines still in cooldown or no longer active
    def eligible(pid):
        if pid not in s.pipeline_map:
            return False
        if s.cooldown_until.get(pid, 0) > s.tick:
            return False
        st = s.pipeline_map[pid].runtime_status()
        if st.is_pipeline_successful():
            return False
        # If we exceeded retries, stop spending resources on it (it will be penalized anyway).
        if s.fail_count.get(pid, 0) > _max_failures_allowed(s.pipeline_map[pid].priority):
            return False
        return True

    # If there are any QUERY pipelines eligible, pick best by score
    q = [pid for pid in s.queues[Priority.QUERY] if eligible(pid)]
    if q:
        return max(q, key=score)

    # Then INTERACTIVE
    inter = [pid for pid in s.queues[Priority.INTERACTIVE] if eligible(pid)]
    if inter:
        return max(inter, key=score)

    # For batch: allow if no higher priority runnable OR if it has aged enough
    batch = [pid for pid in s.queues[Priority.BATCH_PIPELINE] if eligible(pid)]
    if not batch:
        return None

    # If any batch is old enough, run the oldest/scored; else run it only if no higher priority exists at all.
    oldest_age = -1
    oldest_pid = None
    for pid in batch:
        age = s.tick - s.arrival_tick.get(pid, s.tick)
        if age > oldest_age:
            oldest_age = age
            oldest_pid = pid

    if oldest_pid is not None and oldest_age >= batch_aging_ticks:
        return oldest_pid

    # If nothing else runnable (or all higher prio in cooldown), allow batch.
    return max(batch, key=score) if batch else None


def _remove_from_queues(s, pid):
    """Remove a pipeline id from all queues and in_queue set (best-effort)."""
    if pid in s.in_queue:
        s.in_queue.remove(pid)
    for pr in list(s.queues.keys()):
        if pid in s.queues[pr]:
            # Remove all occurrences defensively
            s.queues[pr] = [x for x in s.queues[pr] if x != pid]


def _enqueue_if_needed(s, pipeline):
    pid = pipeline.pipeline_id
    s.pipeline_map[pid] = pipeline
    if pid not in s.arrival_tick:
        s.arrival_tick[pid] = s.tick
    if pid not in s.in_queue:
        s.queues[pipeline.priority].append(pid)
        s.in_queue.add(pid)


@register_scheduler(key="scheduler_low_040")
def scheduler_low_040(s, results, pipelines):
    """
    Scheduler loop.

    - Incorporate new pipelines into priority queues.
    - Update RAM hints and cooldown on failures to reduce repeated OOM and thrash.
    - For each pool, schedule up to a small number of operators (usually 1-2) with
      conservative resource sizing that adapts with observed failures.
    """
    s.tick += 1

    # Add newly arrived pipelines to our state/queues
    for p in pipelines:
        _enqueue_if_needed(s, p)

    # Process execution results: update failure counts and sizing hints
    for r in results:
        # r.ops: list of operators that were executed in that container
        # We only maintain hints at pipeline granularity to keep the policy robust to API differences.
        # We identify pipeline via the pipeline id attached to the executed ops if possible; if not,
        # we fall back to scanning active pipelines (best effort).
        pid = None

        # Best effort: many implementations keep operator objects identical within pipeline DAG,
        # so we can find it by identity. If not found, we still use pool-wide heuristics.
        if hasattr(r, "ops") and r.ops:
            op0 = r.ops[0]
            for _pid, pl in list(s.pipeline_map.items()):
                try:
                    # If the operator is present in pipeline.values (DAG nodes), match it.
                    if hasattr(pl, "values") and op0 in pl.values:
                        pid = _pid
                        break
                except Exception:
                    continue

        if pid is None:
            # If we can't resolve, skip hint update (don't corrupt state).
            continue

        pl = s.pipeline_map.get(pid)
        if pl is None:
            continue

        # If pipeline finished successfully, clean it up from queues/state
        st = pl.runtime_status()
        if st.is_pipeline_successful():
            _remove_from_queues(s, pid)
            # Keep some hints around in case of re-arrival with same id (rare); safe to delete too.
            continue

        if r.failed():
            s.fail_count[pid] = s.fail_count.get(pid, 0) + 1

            # Increase RAM hint aggressively on failure to reduce repeated OOM and 720s penalties.
            # Cap will be enforced per-pool at scheduling time.
            prev_ram = s.ram_hint.get(pid, r.ram if getattr(r, "ram", None) is not None else 0)
            attempted_ram = r.ram if getattr(r, "ram", None) is not None else prev_ram
            new_ram = attempted_ram * 2 if attempted_ram and attempted_ram > 0 else attempted_ram

            # If we have no useful number, bump later via base share; keep hint unset.
            if new_ram and new_ram > 0:
                s.ram_hint[pid] = max(prev_ram, new_ram)

            # Add a short cooldown to avoid immediately retrying the same failing operator
            # (helps throughput and protects interactive/query tail latency).
            cooldown = 2 if pl.priority in (Priority.QUERY, Priority.INTERACTIVE) else 4
            s.cooldown_until[pid] = max(s.cooldown_until.get(pid, 0), s.tick + cooldown)

            # Optionally reduce CPU on repeated failures (some failures can be memory-related; CPU doesn't help)
            prev_cpu = s.cpu_hint.get(pid, r.cpu if getattr(r, "cpu", None) is not None else 0)
            if prev_cpu and prev_cpu > 1:
                s.cpu_hint[pid] = max(1, int(prev_cpu * 0.75))

    # Clean up any pipelines that finished since last tick
    for pid, pl in list(s.pipeline_map.items()):
        try:
            if pl.runtime_status().is_pipeline_successful():
                _remove_from_queues(s, pid)
                del s.pipeline_map[pid]
        except Exception:
            # If status call fails, keep it; simulator should be stable, but we don't crash the scheduler.
            pass

    # Early exit if nothing changed and no new arrivals
    if not pipelines and not results:
        return [], []

    suspensions = []
    assignments = []

    # Scheduling per pool:
    # - Prefer high priority by queue selection.
    # - Issue up to 2 assignments per pool per tick to improve throughput, but keep headroom.
    for pool_id in range(s.executor.num_pools):
        pool = s.executor.pools[pool_id]
        avail_cpu = pool.avail_cpu_pool
        avail_ram = pool.avail_ram_pool

        if avail_cpu <= 0 or avail_ram <= 0:
            continue

        # Keep a small reserve in each pool to improve responsiveness (especially for QUERY).
        # Reserve more if there exist queued high-priority pipelines.
        queued_query = any(pid in s.pipeline_map for pid in s.queues[Priority.QUERY])
        queued_inter = any(pid in s.pipeline_map for pid in s.queues[Priority.INTERACTIVE])

        reserve_cpu = 0
        reserve_ram = 0
        if queued_query:
            reserve_cpu = max(reserve_cpu, int(0.15 * pool.max_cpu_pool))
            reserve_ram = max(reserve_ram, 0.10 * pool.max_ram_pool)
        if queued_inter:
            reserve_cpu = max(reserve_cpu, int(0.10 * pool.max_cpu_pool))
            reserve_ram = max(reserve_ram, 0.05 * pool.max_ram_pool)

        # Issue limited number of assignments per pool to avoid over-committing.
        max_assignments_this_pool = 2
        issued = 0

        while issued < max_assignments_this_pool:
            # Recompute available headroom each iteration
            avail_cpu = pool.avail_cpu_pool
            avail_ram = pool.avail_ram_pool
            if avail_cpu <= 0 or avail_ram <= 0:
                break

            # Enforce reserve if there are higher priority queues
            effective_cpu = max(0, avail_cpu - reserve_cpu)
            effective_ram = max(0.0, avail_ram - reserve_ram)

            # If reserve consumes everything, still allow QUERY/INTERACTIVE to use it (but not batch).
            pid = _pick_next_pipeline_id(s, pool_id)
            if pid is None:
                break
            pl = s.pipeline_map.get(pid)
            if pl is None:
                _remove_from_queues(s, pid)
                continue

            # If only reserved resources remain, don't run batch.
            if (effective_cpu <= 0 or effective_ram <= 0) and pl.priority == Priority.BATCH_PIPELINE:
                break

            status = pl.runtime_status()

            # Stop scheduling pipelines that exceeded retry budget
            if s.fail_count.get(pid, 0) > _max_failures_allowed(pl.priority):
                _remove_from_queues(s, pid)
                # Keep in map (still "arrived"); simulator will mark it incomplete/failed.
                issued += 0
                continue

            # Get next ready operator (parents complete)
            op_list = status.get_ops(ASSIGNABLE_STATES, require_parents_complete=True)[:1]
            if not op_list:
                # Nothing ready; rotate by moving to back to avoid head-of-line blocking
                if pid in s.queues[pl.priority]:
                    q = s.queues[pl.priority]
                    try:
                        q.remove(pid)
                        q.append(pid)
                    except Exception:
                        pass
                break

            # Decide sizing:
            # - Start with base share of pool max (not avail), but clamp to avail.
            # - If we have a RAM hint from past failures, honor it (clamped).
            base = _base_share(pl.priority)

            target_cpu = int(max(_min_cpu(pl.priority), base * pool.max_cpu_pool))
            target_ram = base * pool.max_ram_pool

            # Apply per-pipeline hints if present
            if pid in s.cpu_hint and s.cpu_hint[pid]:
                target_cpu = max(_min_cpu(pl.priority), int(s.cpu_hint[pid]))
            if pid in s.ram_hint and s.ram_hint[pid]:
                target_ram = max(target_ram, float(s.ram_hint[pid]))

            # Clamp to what we can actually allocate now
            # Prefer using effective_* but allow QUERY/INTERACTIVE to dip into reserve.
            cpu_cap = avail_cpu if pl.priority in (Priority.QUERY, Priority.INTERACTIVE) else effective_cpu
            ram_cap = avail_ram if pl.priority in (Priority.QUERY, Priority.INTERACTIVE) else effective_ram

            cpu = int(min(max(1, cpu_cap), max(1, target_cpu)))
            ram = float(min(max(0.0, ram_cap), max(0.0, target_ram)))

            if cpu <= 0 or ram <= 0:
                break

            assignments.append(
                Assignment(
                    ops=op_list,
                    cpu=cpu,
                    ram=ram,
                    priority=pl.priority,
                    pool_id=pool_id,
                    pipeline_id=pl.pipeline_id,
                )
            )

            s.last_scheduled_tick[pid] = s.tick
            s.cpu_hint[pid] = cpu
            s.ram_hint[pid] = ram

            # Rotate within the same priority queue to improve fairness within class
            if pid in s.queues[pl.priority]:
                q = s.queues[pl.priority]
                try:
                    q.remove(pid)
                    q.append(pid)
                except Exception:
                    pass

            issued += 1

    return suspensions, assignments
