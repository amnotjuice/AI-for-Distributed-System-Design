"""Generate scaled workload traces for WoAIS one-shot experiments.

Produces trace_scale{N}x_600s_{idx}.csv for each scale/variant combination,
written to this directory. Traces match the naming convention of the original
traces/ directory but use the full resource profile spectrum instead of a
single io_heavy profile, and include DAG shape variety.

Usage:
    python spring2026/traces/generate.py
    python spring2026/traces/generate.py --dry-run
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Callable

TRACES_DIR = Path(__file__).resolve().parent
REPO_ROOT = TRACES_DIR.parent.parent.parent
sys.path.insert(0, str(REPO_ROOT / "src/spring2026/tool"))

from trace_generator import (
    TraceConfig,
    TraceGenerator,
    ArrivalPattern,
    PipelineSpec,
)

# ---------------------------------------------------------------------------
# Scale traces
# Each scale multiplies num_pipelines_per_batch from the 1x baseline of 10,
# matching the structure of the original traces.
# ---------------------------------------------------------------------------

SCALES = {
    "1x":  10,
    "2x":  20,
    "4x":  40,
    "8x":  80,
    "16x": 160,
}

# Two variants per scale, differentiated by seed.
SCALE_VARIANTS = [
    (0, 42),
    (1, 43),
]


def make_scale_config(batch_size: int, seed: int) -> TraceConfig:
    """Canonical parameters at a given arrival batch size."""
    dag_shapes = ["linear", "branch_in", "branch_out"]
    dag_weights = [0.70, 0.15, 0.15]

    return TraceConfig(
        duration_seconds=DURATION,
        ticks_per_second=1000,
        pipeline_specs=[
            PipelineSpec.query(weight=0.1),
            PipelineSpec.interactive(
                weight=0.3,
                num_ops_mean=8,
                num_ops_min=1,
                cpu_io_ratio=0.3,
                dag_shapes=dag_shapes,
                dag_shape_weights=dag_weights,
            ),
            PipelineSpec.batch(
                weight=0.6,
                num_ops_mean=8,
                num_ops_min=1,
                cpu_io_ratio=0.1,
                dag_shapes=dag_shapes,
                dag_shape_weights=dag_weights,
            ),
        ],
        arrival_pattern=ArrivalPattern(
            waiting_seconds_mean=9.0,
            waiting_seconds_stdev=2.25,
            num_pipelines_per_batch=batch_size,
        ),
        random_seed=seed,
    )


# ---------------------------------------------------------------------------
# Benchmark suite
# 10 named workloads, each with a train trace (seed=42) and test trace
# (seed=137). Each stresses a distinct scheduler behavior axis.
# ---------------------------------------------------------------------------

TRAIN_SEED = 42
TEST_SEED  = 137
DURATION   = 3600

_MIXED_DAGS  = ["linear", "branch_in", "branch_out"]
_MIXED_W     = [0.70, 0.15, 0.15]
_BRANCH_DAGS = ["branch_in", "branch_out"]
_BRANCH_W    = [0.50, 0.50]


def _bench_canonical(seed: int) -> TraceConfig:
    """Baseline canonical mix. Reference point for all comparisons."""
    return TraceConfig(
        duration_seconds=DURATION, ticks_per_second=1000,
        pipeline_specs=[
            PipelineSpec.query(weight=0.1),
            PipelineSpec.interactive(weight=0.3, num_ops_mean=8, num_ops_min=1,
                                     cpu_io_ratio=0.3, dag_shapes=_MIXED_DAGS, dag_shape_weights=_MIXED_W),
            PipelineSpec.batch(weight=0.6, num_ops_mean=8, num_ops_min=1,
                               cpu_io_ratio=0.1, dag_shapes=_MIXED_DAGS, dag_shape_weights=_MIXED_W),
        ],
        arrival_pattern=ArrivalPattern(waiting_seconds_mean=9.0, num_pipelines_per_batch=10),
        random_seed=seed,
    )


def _bench_priority_heavy(seed: int) -> TraceConfig:
    """High QUERY + INTERACTIVE ratio. Stresses priority ordering under contention."""
    return TraceConfig(
        duration_seconds=DURATION, ticks_per_second=1000,
        pipeline_specs=[
            PipelineSpec.query(weight=0.4),
            PipelineSpec.interactive(weight=0.4, num_ops_mean=6, num_ops_min=1,
                                     cpu_io_ratio=0.3, dag_shapes=_MIXED_DAGS, dag_shape_weights=_MIXED_W),
            PipelineSpec.batch(weight=0.2, num_ops_mean=8, num_ops_min=1,
                               cpu_io_ratio=0.1, dag_shapes=_MIXED_DAGS, dag_shape_weights=_MIXED_W),
        ],
        arrival_pattern=ArrivalPattern(waiting_seconds_mean=9.0, num_pipelines_per_batch=10),
        random_seed=seed,
    )


def _bench_batch_heavy(seed: int) -> TraceConfig:
    """Mostly BATCH traffic. Stresses throughput and batch starvation avoidance."""
    return TraceConfig(
        duration_seconds=DURATION, ticks_per_second=1000,
        pipeline_specs=[
            PipelineSpec.query(weight=0.05),
            PipelineSpec.interactive(weight=0.10, num_ops_mean=8, num_ops_min=1,
                                     cpu_io_ratio=0.3, dag_shapes=_MIXED_DAGS, dag_shape_weights=_MIXED_W),
            PipelineSpec.batch(weight=0.85, num_ops_mean=8, num_ops_min=1,
                               cpu_io_ratio=0.1, dag_shapes=_MIXED_DAGS, dag_shape_weights=_MIXED_W),
        ],
        arrival_pattern=ArrivalPattern(waiting_seconds_mean=9.0, num_pipelines_per_batch=10),
        random_seed=seed,
    )


def _bench_high_load(seed: int) -> TraceConfig:
    """4x arrival rate. Stresses scheduling speed under sustained backlog."""
    return TraceConfig(
        duration_seconds=DURATION, ticks_per_second=1000,
        pipeline_specs=[
            PipelineSpec.query(weight=0.1),
            PipelineSpec.interactive(weight=0.3, num_ops_mean=8, num_ops_min=1,
                                     cpu_io_ratio=0.3, dag_shapes=_MIXED_DAGS, dag_shape_weights=_MIXED_W),
            PipelineSpec.batch(weight=0.6, num_ops_mean=8, num_ops_min=1,
                               cpu_io_ratio=0.1, dag_shapes=_MIXED_DAGS, dag_shape_weights=_MIXED_W),
        ],
        arrival_pattern=ArrivalPattern(waiting_seconds_mean=9.0, num_pipelines_per_batch=40),
        random_seed=seed,
    )


def _bench_low_load(seed: int) -> TraceConfig:
    """Sparse arrivals. Stresses resource utilization when the system is underloaded."""
    return TraceConfig(
        duration_seconds=DURATION, ticks_per_second=1000,
        pipeline_specs=[
            PipelineSpec.query(weight=0.1),
            PipelineSpec.interactive(weight=0.3, num_ops_mean=8, num_ops_min=1,
                                     cpu_io_ratio=0.3, dag_shapes=_MIXED_DAGS, dag_shape_weights=_MIXED_W),
            PipelineSpec.batch(weight=0.6, num_ops_mean=8, num_ops_min=1,
                               cpu_io_ratio=0.1, dag_shapes=_MIXED_DAGS, dag_shape_weights=_MIXED_W),
        ],
        arrival_pattern=ArrivalPattern(waiting_seconds_mean=30.0, num_pipelines_per_batch=3),
        random_seed=seed,
    )


def _bench_cpu_bound(seed: int) -> TraceConfig:
    """CPU-heavy operator profiles. Stresses CPU allocation and contention."""
    return TraceConfig(
        duration_seconds=DURATION, ticks_per_second=1000,
        pipeline_specs=[
            PipelineSpec.query(weight=0.1),
            PipelineSpec.interactive(weight=0.3, num_ops_mean=8, num_ops_min=1,
                                     cpu_io_ratio=1.5, dag_shapes=_MIXED_DAGS, dag_shape_weights=_MIXED_W),
            PipelineSpec.batch(weight=0.6, num_ops_mean=8, num_ops_min=1,
                               cpu_io_ratio=1.5, dag_shapes=_MIXED_DAGS, dag_shape_weights=_MIXED_W),
        ],
        arrival_pattern=ArrivalPattern(waiting_seconds_mean=9.0, num_pipelines_per_batch=10),
        random_seed=seed,
    )


def _bench_io_bound(seed: int) -> TraceConfig:
    """IO-heavy operator profiles. Stresses memory pressure and OOM recovery."""
    return TraceConfig(
        duration_seconds=DURATION, ticks_per_second=1000,
        pipeline_specs=[
            PipelineSpec.query(weight=0.1),
            PipelineSpec.interactive(weight=0.3, num_ops_mean=8, num_ops_min=1,
                                     cpu_io_ratio=0.0, dag_shapes=_MIXED_DAGS, dag_shape_weights=_MIXED_W),
            PipelineSpec.batch(weight=0.6, num_ops_mean=8, num_ops_min=1,
                               cpu_io_ratio=0.0, dag_shapes=_MIXED_DAGS, dag_shape_weights=_MIXED_W),
        ],
        arrival_pattern=ArrivalPattern(waiting_seconds_mean=9.0, num_pipelines_per_batch=10),
        random_seed=seed,
    )


def _bench_deep_pipelines(seed: int) -> TraceConfig:
    """Long linear pipelines. Stresses dependency tracking over many sequential operators."""
    return TraceConfig(
        duration_seconds=DURATION, ticks_per_second=1000,
        pipeline_specs=[
            PipelineSpec.query(weight=0.1),
            PipelineSpec.interactive(weight=0.3, num_ops_mean=15, num_ops_min=10,
                                     cpu_io_ratio=0.3, dag_shapes=["linear"]),
            PipelineSpec.batch(weight=0.6, num_ops_mean=15, num_ops_min=10,
                               cpu_io_ratio=0.1, dag_shapes=["linear"]),
        ],
        arrival_pattern=ArrivalPattern(waiting_seconds_mean=9.0, num_pipelines_per_batch=10),
        random_seed=seed,
    )


def _bench_shallow_pipelines(seed: int) -> TraceConfig:
    """Very short pipelines. Stresses scheduling overhead and high job turnover."""
    return TraceConfig(
        duration_seconds=DURATION, ticks_per_second=1000,
        pipeline_specs=[
            PipelineSpec.query(weight=0.1),
            PipelineSpec.interactive(weight=0.3, num_ops_mean=2, num_ops_min=1, num_ops_max=3,
                                     cpu_io_ratio=0.3, dag_shapes=_MIXED_DAGS, dag_shape_weights=_MIXED_W),
            PipelineSpec.batch(weight=0.6, num_ops_mean=2, num_ops_min=1, num_ops_max=3,
                               cpu_io_ratio=0.1, dag_shapes=_MIXED_DAGS, dag_shape_weights=_MIXED_W),
        ],
        arrival_pattern=ArrivalPattern(waiting_seconds_mean=9.0, num_pipelines_per_batch=10),
        random_seed=seed,
    )


def _bench_dag_variety(seed: int) -> TraceConfig:
    """Pure fan-in / fan-out DAGs, no linear chains. Stresses parallel scheduling and join sync."""
    return TraceConfig(
        duration_seconds=DURATION, ticks_per_second=1000,
        pipeline_specs=[
            PipelineSpec.query(weight=0.1),
            PipelineSpec.interactive(weight=0.3, num_ops_mean=8, num_ops_min=3,
                                     cpu_io_ratio=0.3, dag_shapes=_BRANCH_DAGS, dag_shape_weights=_BRANCH_W),
            PipelineSpec.batch(weight=0.6, num_ops_mean=8, num_ops_min=3,
                               cpu_io_ratio=0.1, dag_shapes=_BRANCH_DAGS, dag_shape_weights=_BRANCH_W),
        ],
        arrival_pattern=ArrivalPattern(waiting_seconds_mean=9.0, num_pipelines_per_batch=10),
        random_seed=seed,
    )


BENCHMARKS: dict[str, Callable[[int], TraceConfig]] = {
    "canonical":        _bench_canonical,
    "priority_heavy":   _bench_priority_heavy,
    "batch_heavy":      _bench_batch_heavy,
    "high_load":        _bench_high_load,
    "low_load":         _bench_low_load,
    "cpu_bound":        _bench_cpu_bound,
    "io_bound":         _bench_io_bound,
    "deep_pipelines":   _bench_deep_pipelines,
    "shallow_pipelines": _bench_shallow_pipelines,
    "dag_variety":      _bench_dag_variety,
}


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--dry-run", action="store_true",
                        help="Print what would be generated without writing files.")
    parser.add_argument("--only", choices=["scales", "benchmarks"], default=None,
                        help="Generate only scale traces or only benchmark traces (default: both).")
    args = parser.parse_args()

    do_scales     = args.only in (None, "scales")
    do_benchmarks = args.only in (None, "benchmarks")

    if do_scales:
        targets = [
            (scale_name, batch_size, idx, seed)
            for scale_name, batch_size in SCALES.items()
            for idx, seed in SCALE_VARIANTS
        ]
        print(f"Generating {len(targets)} scale traces...\n")
        for scale_name, batch_size, idx, seed in targets:
            filename = f"trace_scale{scale_name}_600s_{idx}.csv"
            out_path = TRACES_DIR / filename
            if args.dry_run:
                print(f"  [dry-run] {filename}  (batch_size={batch_size}, seed={seed})")
            else:
                make_scale_config(batch_size, seed)
                TraceGenerator(make_scale_config(batch_size, seed)).write_csv(out_path)

    if do_benchmarks:
        print(f"\nGenerating {len(BENCHMARKS) * 2} benchmark traces...\n")
        for name, config_fn in BENCHMARKS.items():
            for split, seed in [("train", TRAIN_SEED), ("test", TEST_SEED)]:
                filename = f"bench_{name}_{split}.csv"
                out_path = TRACES_DIR / filename
                if args.dry_run:
                    print(f"  [dry-run] {filename}  (seed={seed})")
                else:
                    TraceGenerator(config_fn(seed)).write_csv(out_path)

    if not args.dry_run:
        print(f"\nDone.")


if __name__ == "__main__":
    main()
