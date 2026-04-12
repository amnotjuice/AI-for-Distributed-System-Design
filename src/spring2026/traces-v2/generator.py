#!/usr/bin/env python3
"""Generate train/test trace pairs for the cross-product experiment.

Dimensions (3 × 2 × 3 = 18 combinations, 2 seeds each = 36 traces):
  priority : canonical | priority-heavy | batch-heavy
  load     : steady    | bursty
  resource : cpu-bound | balanced       | memory-bound

Usage:
    python generator.py
    python generator.py --out-dir /path/to/dir
    python generator.py --dry-run
"""
from __future__ import annotations

import argparse
import sys
from itertools import product
from pathlib import Path

TRACES_DIR = Path(__file__).resolve().parent
REPO_ROOT   = TRACES_DIR.parent.parent.parent
sys.path.insert(0, str(REPO_ROOT / "src/spring2026/tool"))

from trace_generator import (
    ArrivalPattern, BurstSpec, PipelineSpec, TraceConfig, TraceGenerator,
)

DURATION         = 600
TICKS_PER_SECOND = 1000
NUM_OPS_MEAN     = 5
DAG_SHAPES       = ["linear", "branch_in", "branch_out"]
DAG_WEIGHTS      = [0.70, 0.15, 0.15]

TRAIN_SEED = 42
TEST_SEED  = 137

# ---------------------------------------------------------------------------
# Dimension tables
# ---------------------------------------------------------------------------

# Priority mix: {query, interactive, batch} weights
PRIORITY_MIXES = {
    "canonical":      dict(query=0.1,  interactive=0.3,  batch=0.6),
    "priority-heavy": dict(query=0.6,  interactive=0.3,  batch=0.1),
    "batch-heavy":    dict(query=0.05, interactive=0.15, batch=0.8),
}

# Load level: arrival cadence + optional burst injections.
# Bursty uses longer gaps with 4 spikes spread across the 600 s window,
# keeping total pipeline count in the same rough range as steady.
LOAD_LEVELS = {
    "steady": dict(
        arrival=ArrivalPattern(waiting_seconds_mean=10, num_pipelines_per_batch=4),
        bursts=[],
    ),
    "bursty": dict(
        arrival=ArrivalPattern(waiting_seconds_mean=30, num_pipelines_per_batch=2),
        bursts=[
            BurstSpec(time_seconds=75,  num_pipelines=30),
            BurstSpec(time_seconds=225, num_pipelines=29),
            BurstSpec(time_seconds=375, num_pipelines=30),
            BurstSpec(time_seconds=525, num_pipelines=29),
        ],
    ),
}

# Resource profile: cpu_io_ratio controls operator profile distribution.
# High ratio  → cpu_heavy / cpu_intensive skew.
# Low ratio   → io_heavy / io_med skew.
RESOURCE_PROFILES = {
    "cpu-bound":    1.5,
    "balanced":     0.5,
    "memory-bound": 0.0,
}


# ---------------------------------------------------------------------------
# Config builder
# ---------------------------------------------------------------------------

def make_config(priority: str, load: str, resource: str, seed: int) -> TraceConfig:
    mix          = PRIORITY_MIXES[priority]
    load_cfg     = LOAD_LEVELS[load]
    cpu_io_ratio = RESOURCE_PROFILES[resource]

    return TraceConfig(
        duration_seconds=DURATION,
        ticks_per_second=TICKS_PER_SECOND,
        pipeline_specs=[
            PipelineSpec.query(weight=mix["query"]),
            PipelineSpec.interactive(
                weight=mix["interactive"],
                num_ops_mean=NUM_OPS_MEAN,
                num_ops_min=1,
                cpu_io_ratio=cpu_io_ratio,
                dag_shapes=DAG_SHAPES,
                dag_shape_weights=DAG_WEIGHTS,
            ),
            PipelineSpec.batch(
                weight=mix["batch"],
                num_ops_mean=NUM_OPS_MEAN,
                num_ops_min=1,
                cpu_io_ratio=cpu_io_ratio,
                dag_shapes=DAG_SHAPES,
                dag_shape_weights=DAG_WEIGHTS,
            ),
        ],
        arrival_pattern=load_cfg["arrival"],
        bursts=load_cfg["bursts"],
        random_seed=seed,
    )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--out-dir", type=Path, default=TRACES_DIR,
                        help="Output directory (default: same directory as this script).")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print what would be generated without writing files.")
    args = parser.parse_args()

    combos = list(product(PRIORITY_MIXES, LOAD_LEVELS, RESOURCE_PROFILES))
    seeds  = [("train", TRAIN_SEED), ("test", TEST_SEED)]
    total  = len(combos) * len(seeds)

    prefix = "[dry-run] " if args.dry_run else ""
    print(f"{prefix}Generating {total} traces "
          f"({len(combos)} combinations × 2 seeds) → {args.out_dir}\n")

    if not args.dry_run:
        args.out_dir.mkdir(parents=True, exist_ok=True)

    for priority, load, resource in combos:
        for split, seed in seeds:
            filename = f"{priority}_{load}_{resource}_{split}.csv"
            out_path = args.out_dir / filename
            if args.dry_run:
                print(f"  {filename}")
            else:
                TraceGenerator(make_config(priority, load, resource, seed)).write_csv(out_path)  # type: ignore[arg-type]

    if not args.dry_run:
        print(f"\nDone. {total} traces written to {args.out_dir}")


if __name__ == "__main__":
    main()
