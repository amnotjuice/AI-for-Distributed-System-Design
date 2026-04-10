"""Extended simulator with additional experiment controls.

This is a local fork of eudoxia's run_simulator, adding two optional parameters
that are not supported by the upstream eudoxia package:

  max_outstanding (int | None):
      Maximum number of pipelines allowed in the outstanding queue at once.
      New arrivals beyond this limit are dropped (recorded as failed).
      None = no limit (default eudoxia behaviour).

  tick_timeout_ms (float | None):
      Maximum wall-clock milliseconds allowed for a single scheduler.run_one_tick()
      call. If exceeded, raises SchedulerTickTimeoutError.
      None = no limit (default eudoxia behaviour).

When neither parameter is set, behaviour is identical to run_simulator().
"""

import logging
import signal
import time
from collections import defaultdict
from typing import Dict, List, Union

import numpy as np

from eudoxia.executor import Executor
from eudoxia.scheduler import Scheduler
from eudoxia.simulator import (
    SimulatedTimeFormatter,
    SimulatorStats,
    compute_pipeline_stats,
    parse_args_with_defaults,
)
from eudoxia.utils.utils import Priority
from eudoxia.workload import Pipeline, Workload, WorkloadGenerator

logger = logging.getLogger(__name__)


class SchedulerTickTimeoutError(Exception):
    """Raised when a single scheduler tick exceeds tick_timeout_ms."""
    pass


from typing import NamedTuple, Optional
from eudoxia.simulator import PipelineStats

class SimulatorStatsExt(NamedTuple):
    """SimulatorStats extended with RAM utilization tracking."""
    pipelines_created: int
    containers_completed: int
    throughput: float
    p99_latency: float
    assignments: int
    suspensions: int
    failures: int
    failure_error_counts: dict
    pipelines_all: PipelineStats
    pipelines_query: PipelineStats
    pipelines_interactive: PipelineStats
    pipelines_batch: PipelineStats
    ram_utilization_mean: Optional[float] = None
    ram_utilization_median: Optional[float] = None
    ram_utilization_p5: Optional[float] = None
    ram_utilization_n: int = 0

    def adjusted_latency(self, weights=None, divide_by_completion_rate=True) -> float:
        """Weighted mean latency across pipeline categories, penalized for incompletions."""
        from eudoxia.utils.utils import Priority
        if self.pipelines_all.completion_count == 0:
            return float('inf')
        if weights is None:
            weights = {Priority.QUERY: 10, Priority.INTERACTIVE: 5, Priority.BATCH_PIPELINE: 1}
        categories = [
            (Priority.QUERY, self.pipelines_query),
            (Priority.INTERACTIVE, self.pipelines_interactive),
            (Priority.BATCH_PIPELINE, self.pipelines_batch),
        ]
        weighted_latency_sum = 0.0
        weighted_count_sum = 0.0
        total_arrivals = 0
        total_completions = 0
        for priority, stats in categories:
            weight = weights.get(priority, 1)
            if stats.completion_count > 0:
                weighted_count = weight * stats.completion_count
                weighted_latency_sum += weighted_count * stats.mean_latency_seconds
                weighted_count_sum += weighted_count
            total_arrivals += stats.arrival_count
            total_completions += stats.completion_count
        adjusted = weighted_latency_sum / weighted_count_sum
        if divide_by_completion_rate:
            completion_rate = total_completions / total_arrivals
            adjusted = adjusted / completion_rate
        return adjusted

    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization."""
        d = {
            'pipelines_created': self.pipelines_created,
            'containers_completed': self.containers_completed,
            'throughput': self.throughput,
            'p99_latency': self.p99_latency,
            'assignments': self.assignments,
            'suspensions': self.suspensions,
            'failures': self.failures,
            'failure_error_counts': self.failure_error_counts,
            'pipelines_all': self.pipelines_all.to_dict(),
            'pipelines_query': self.pipelines_query.to_dict(),
            'pipelines_interactive': self.pipelines_interactive.to_dict(),
            'pipelines_batch': self.pipelines_batch.to_dict(),
            'ram_utilization_mean': self.ram_utilization_mean,
            'ram_utilization_median': self.ram_utilization_median,
            'ram_utilization_p5': self.ram_utilization_p5,
            'ram_utilization_n': self.ram_utilization_n,
        }
        return d


def run_simulator_ext(
    param_input: Union[str, Dict],
    workload: Workload = None,
) -> SimulatorStats:
    """Drop-in replacement for eudoxia's run_simulator with extra experiment controls.

    Extra params (read from param_input dict):
        max_outstanding (int | None): max pipelines in queue; extras are dropped.
        tick_timeout_ms (float | None): max ms per scheduler tick; raises SchedulerTickTimeoutError.

    All other params are identical to run_simulator().
    """
    if isinstance(param_input, dict):
        params = param_input.copy()
    else:
        raise TypeError(f"run_simulator_ext expects a dict, got {type(param_input)}")
    params = parse_args_with_defaults(params)

    max_outstanding = params.get("max_outstanding", None)
    tick_timeout_ms = params.get("tick_timeout_ms", None)

    if workload is None:
        workload = WorkloadGenerator(**params)

    executor = Executor(**params)
    scheduler = Scheduler(executor, **params)
    ticks_per_second = params["ticks_per_second"]
    max_ticks = int(params["duration"] * ticks_per_second)

    sim_formatter = SimulatedTimeFormatter()
    for handler in logging.getLogger().handlers:
        handler.setFormatter(sim_formatter)

    logger.info(f"Running for {params['duration']}s or {max_ticks} ticks")
    logger.info(f"max_outstanding={max_outstanding}, tick_timeout_ms={tick_timeout_ms}")

    num_pipelines_created = 0
    num_assignments = 0
    num_suspenions = 0
    num_failures = 0
    num_dropped = 0
    failure_error_counts = defaultdict(int)
    executor_results = []
    ram_utilization_samples = []  # actual_peak / allocated_ram for completed containers
    outstanding_pipelines: Dict[str, Pipeline] = {}
    pipeline_arrivals_by_priority: Dict[Priority, int] = {
        Priority.QUERY: 0,
        Priority.INTERACTIVE: 0,
        Priority.BATCH_PIPELINE: 0,
    }
    pipeline_latencies_by_priority: Dict[Priority, List[int]] = {
        Priority.QUERY: [],
        Priority.INTERACTIVE: [],
        Priority.BATCH_PIPELINE: [],
    }

    # Set up signal-based tick timeout (interrupts slow ticks mid-execution)
    if tick_timeout_ms is not None:
        _tick_timeout_sec = tick_timeout_ms / 1000.0
        _tick_start = [0.0]  # mutable for closure access

        def _tick_alarm_handler(signum, frame):
            elapsed_ms = (time.perf_counter() - _tick_start[0]) * 1000
            raise SchedulerTickTimeoutError(
                f"Scheduler tick took {elapsed_ms:.1f}ms > {tick_timeout_ms}ms limit"
            )

        _old_alrm_handler = signal.signal(signal.SIGALRM, _tick_alarm_handler)

    try:
        for tick_number in range(max_ticks):
            sim_formatter.set_simulated_elapsed_seconds(tick_number / ticks_per_second)

            new_pipelines: List[Pipeline] = workload.run_one_tick()
            admitted_pipelines: List[Pipeline] = []
            for p in new_pipelines:
                num_pipelines_created += 1
                pipeline_arrivals_by_priority[p.priority] += 1
                if max_outstanding is not None and len(outstanding_pipelines) >= max_outstanding:
                    # Drop: count as failure, do not pass to scheduler
                    num_dropped += 1
                    num_failures += 1
                    failure_error_counts["dropped_max_outstanding"] += 1
                    continue
                p.runtime_status().record_arrival(tick_number)
                outstanding_pipelines[p.pipeline_id] = p
                admitted_pipelines.append(p)
                logger.debug(f"Pipeline arrived with Priority {p.priority} and {len(p.values)} op(s)")

            # Scheduler tick — optionally enforce wall-clock timeout via signal
            if tick_timeout_ms is not None:
                _tick_start[0] = time.perf_counter()
                signal.setitimer(signal.ITIMER_REAL, _tick_timeout_sec)
                suspensions, assignments = scheduler.run_one_tick(executor_results, admitted_pipelines)
                signal.setitimer(signal.ITIMER_REAL, 0)  # cancel timer
            else:
                suspensions, assignments = scheduler.run_one_tick(executor_results, admitted_pipelines)

            executor_results = executor.run_one_tick(suspensions, assignments)

            num_assignments += len(assignments)
            num_suspenions += len(suspensions)
            failures = [r for r in executor_results if r.failed()]
            num_failures += len(failures)
            for failure in failures:
                failure_error_counts[failure.error] += 1

            # Track RAM utilization for completed containers
            for r in executor_results:
                if not r.failed() and r.ram and r.ram > 0 and r.ops:
                    try:
                        actual_peak = max(
                            seg.get_peak_memory_gb()
                            for op in r.ops
                            for seg in op.get_segments()
                        )
                        if actual_peak is not None and actual_peak > 0:
                            ram_utilization_samples.append(actual_peak / r.ram)
                    except Exception:
                        pass

            if executor_results:
                for pipeline_id in list(outstanding_pipelines.keys()):
                    pipeline = outstanding_pipelines[pipeline_id]
                    if pipeline.runtime_status().is_pipeline_successful():
                        pipeline.runtime_status().record_finish(tick_number)
                        latency_ticks = pipeline.runtime_status().get_latency_ticks()
                        pipeline_latencies_by_priority[pipeline.priority].append(latency_ticks)
                        del outstanding_pipelines[pipeline_id]

            if tick_number % ticks_per_second == 0:
                logger.info(f"tick {tick_number}/{max_ticks} outstanding={len(outstanding_pipelines)}")
    finally:
        # Always clean up signal handler, even if SchedulerTickTimeoutError is raised
        if tick_timeout_ms is not None:
            signal.setitimer(signal.ITIMER_REAL, 0)
            signal.signal(signal.SIGALRM, _old_alrm_handler)

    throughput = executor.num_completed() / params["duration"]
    p99 = np.percentile(executor.container_tick_times(), 99) / ticks_per_second

    all_arrivals = sum(pipeline_arrivals_by_priority.values())
    all_latencies = sum(pipeline_latencies_by_priority.values(), [])
    pipelines_all = compute_pipeline_stats(all_arrivals, all_latencies, ticks_per_second)
    pipelines_query = compute_pipeline_stats(
        pipeline_arrivals_by_priority[Priority.QUERY],
        pipeline_latencies_by_priority[Priority.QUERY],
        ticks_per_second)
    pipelines_interactive = compute_pipeline_stats(
        pipeline_arrivals_by_priority[Priority.INTERACTIVE],
        pipeline_latencies_by_priority[Priority.INTERACTIVE],
        ticks_per_second)
    pipelines_batch = compute_pipeline_stats(
        pipeline_arrivals_by_priority[Priority.BATCH_PIPELINE],
        pipeline_latencies_by_priority[Priority.BATCH_PIPELINE],
        ticks_per_second)

    ram_util_mean = float(np.mean(ram_utilization_samples)) if ram_utilization_samples else None
    ram_util_median = float(np.median(ram_utilization_samples)) if ram_utilization_samples else None
    ram_util_p5 = float(np.percentile(ram_utilization_samples, 5)) if ram_utilization_samples else None

    stats = SimulatorStatsExt(
        pipelines_created=num_pipelines_created,
        containers_completed=executor.num_completed(),
        throughput=throughput,
        p99_latency=p99,
        assignments=num_assignments,
        suspensions=num_suspenions,
        failures=num_failures,
        failure_error_counts=dict(failure_error_counts),
        pipelines_all=pipelines_all,
        pipelines_query=pipelines_query,
        pipelines_interactive=pipelines_interactive,
        pipelines_batch=pipelines_batch,
        ram_utilization_mean=ram_util_mean,
        ram_utilization_median=ram_util_median,
        ram_utilization_p5=ram_util_p5,
        ram_utilization_n=len(ram_utilization_samples),
    )
    return stats
