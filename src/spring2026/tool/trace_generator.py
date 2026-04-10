"""
Trace generator for Eudoxia CSV workload traces.

Produces traces in the format accepted by:
    eudoxia run params.toml -w trace.csv

The format has nine fixed columns followed by zero or more label columns.
Label columns are extra per-operator metadata that schedulers and estimators
can optionally use (e.g. op_type, file_size_mb, noisy resource estimates).

Quick start
-----------
Run the built-in demo to write a sample trace to stdout:

    python tools/trace_generator.py

Or import and use programmatically:

    from tools.trace_generator import TraceConfig, TraceGenerator, PipelineSpec, ArrivalPattern
    from tools.trace_generator import op_type_label, file_size_mb_label

    config = TraceConfig(
        duration_seconds=600,
        ticks_per_second=1000,
        pipeline_specs=[
            PipelineSpec.query(label_specs=[op_type_label()]),
            PipelineSpec.interactive(num_ops_mean=5, label_specs=[op_type_label(), file_size_mb_label()]),
            PipelineSpec.batch(num_ops_mean=5, label_specs=[op_type_label(), file_size_mb_label()]),
        ],
        arrival_pattern=ArrivalPattern(waiting_seconds_mean=10.0, num_pipelines_per_batch=4),
        random_seed=42,
    )
    gen = TraceGenerator(config)
    gen.write_csv("my_trace.csv")
"""

from __future__ import annotations

import csv
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Optional

import numpy as np


# ---------------------------------------------------------------------------
# SegmentProfile — resource values for one operator
# ---------------------------------------------------------------------------

@dataclass
class SegmentProfile:
    """Resource requirements for a single operator (maps to one CSV row)."""
    name: str
    baseline_cpu_seconds: float
    cpu_scaling: str          # const | log | sqrt | linear3 | linear7 | squared | exp
    storage_read_gb: float
    memory_gb: Optional[float] = None   # None → linear growth from storage reads


# Default profiles mirroring Eudoxia's built-in segment prototypes.
DEFAULT_PROFILES: dict[str, SegmentProfile] = {
    "io_heavy":      SegmentProfile("io_heavy",      1,  "const",   55,   None),
    "io_med_a":      SegmentProfile("io_med_a",       2,  "sqrt",    55,   None),
    "io_med_b":      SegmentProfile("io_med_b",       5,  "linear3", 45,   None),
    "balanced":      SegmentProfile("balanced",       15, "linear3", 37.5, None),
    "cpu_med":       SegmentProfile("cpu_med",        20, "linear7", 30,   None),
    "cpu_heavy":     SegmentProfile("cpu_heavy",      40, "linear7", 20,   None),
    "cpu_intensive": SegmentProfile("cpu_intensive",  80, "squared", 10,   None),
    "query":         SegmentProfile("query",          15, "linear3", 35,   None),
}

# Profiles in cpu_io_ratio order (least IO → most IO), used by the random selector.
_PROFILE_SEQUENCE = [
    "io_heavy", "io_med_a", "io_med_b", "balanced", "cpu_med", "cpu_heavy", "cpu_intensive",
]


# ---------------------------------------------------------------------------
# OperatorContext — passed to label generators so they can be context-aware
# ---------------------------------------------------------------------------

@dataclass
class OperatorContext:
    """Read-only view of an operator's position and properties."""
    op_index: int              # 0-based position within the pipeline
    num_ops: int               # total operators in this pipeline
    dag_shape: str             # "linear" | "branch_in" | "branch_out"
    is_root: bool              # True if this operator has no parents
    is_leaf: bool              # True if this operator has no children
    priority: str              # "QUERY" | "INTERACTIVE" | "BATCH_PIPELINE"
    profile: SegmentProfile
    pipeline_id: str


# ---------------------------------------------------------------------------
# LabelSpec — describes how to generate one label column per operator
# ---------------------------------------------------------------------------

@dataclass
class LabelSpec:
    """
    Specifies a single label column.

    The generator callable receives the operator's context and the shared rng,
    and returns the value to write (or None to leave the cell empty).
    """
    column_name: str
    generator: Callable[[OperatorContext, np.random.Generator], Optional[str | int | float]]


# ---------------------------------------------------------------------------
# Built-in label generators
# ---------------------------------------------------------------------------

def op_type_label() -> LabelSpec:
    """Emit 'read', 'write', or 'transform' based on DAG position."""
    def _gen(ctx: OperatorContext, rng: np.random.Generator) -> str:
        if ctx.dag_shape == "linear":
            if ctx.is_root:
                return "read"
            elif ctx.is_leaf:
                return "write"
            else:
                return "transform"
        elif ctx.dag_shape == "branch_in":
            if ctx.is_leaf:
                return "transform"
            else:
                return "read"
        elif ctx.dag_shape == "branch_out":
            if ctx.is_root:
                return "read"
            else:
                return "write"
        return "transform"
    return LabelSpec("op_type", _gen)


def file_size_mb_label(lo: int = 50, hi: int = 500) -> LabelSpec:
    """Emit a random file size (MB) for read/write ops; empty for transforms."""
    def _gen(ctx: OperatorContext, rng: np.random.Generator) -> Optional[int]:
        op_type = _infer_op_type(ctx)
        if op_type in ("read", "write"):
            return int(rng.integers(lo, hi + 1))
        return None
    return LabelSpec("file_size_mb", _gen)


def fixed_label(column_name: str, value) -> LabelSpec:
    """Always emit the same value — useful for tagging experiment conditions."""
    return LabelSpec(column_name, lambda ctx, rng: value)


def choice_label(column_name: str, options: list, weights: Optional[list[float]] = None) -> LabelSpec:
    """Sample from a list of options, optionally weighted."""
    def _gen(ctx: OperatorContext, rng: np.random.Generator):
        if weights is not None:
            w = np.array(weights, dtype=float)
            w = w / w.sum()
            idx = rng.choice(len(options), p=w)
        else:
            idx = rng.integers(0, len(options))
        return options[idx]
    return LabelSpec(column_name, _gen)


def noisy_label(
    column_name: str,
    true_value_fn: Callable[[OperatorContext, np.random.Generator], float],
    noise_fn: Callable[[float, np.random.Generator], float],
) -> LabelSpec:
    """
    Generate a value then apply noise — the key primitive for noisy oracle experiments.

    true_value_fn(ctx, rng)  → ground truth float
    noise_fn(true_val, rng)  → noisy observation

    Example — multiplicative log-normal noise with 20% std:
        noisy_label(
            "noisy_cpu_seconds",
            true_value_fn=lambda ctx, rng: ctx.profile.baseline_cpu_seconds,
            noise_fn=lambda v, rng: v * rng.lognormal(0, 0.2),
        )
    """
    def _gen(ctx: OperatorContext, rng: np.random.Generator) -> float:
        true_val = true_value_fn(ctx, rng)
        return noise_fn(true_val, rng)
    return LabelSpec(column_name, _gen)


def _infer_op_type(ctx: OperatorContext) -> str:
    """Helper: infer read/write/transform from context (same logic as op_type_label)."""
    if ctx.dag_shape == "linear":
        if ctx.is_root:
            return "read"
        elif ctx.is_leaf:
            return "write"
        return "transform"
    elif ctx.dag_shape == "branch_in":
        return "transform" if ctx.is_leaf else "read"
    elif ctx.dag_shape == "branch_out":
        return "read" if ctx.is_root else "write"
    return "transform"


# ---------------------------------------------------------------------------
# BurstSpec — a one-time spike of pipelines at a fixed simulation time
# ---------------------------------------------------------------------------

@dataclass
class BurstSpec:
    """
    Injects a fixed number of pipelines at a specific simulation time,
    independent of the regular ArrivalPattern cadence.

    Parameters
    ----------
    time_seconds : float
        Simulation time at which the burst arrives.
    num_pipelines : int
        Number of pipelines to inject at that time.
    spec_index : int | None
        Index into TraceConfig.pipeline_specs to use for all burst pipelines.
        None (default) samples proportionally to each spec's weight, the same
        way regular arrivals do.

    Example — 20 QUERY pipelines spike at t=60s:
        bursts=[BurstSpec(time_seconds=60, num_pipelines=20, spec_index=0)]
    """
    time_seconds: float
    num_pipelines: int
    spec_index: Optional[int] = None


# ---------------------------------------------------------------------------
# ArrivalPattern — controls when batches of pipelines arrive
# ---------------------------------------------------------------------------

@dataclass
class ArrivalPattern:
    """
    Controls the arrival cadence of pipeline batches.

    Arrival times are drawn from a normal distribution (mean ± stdev).
    Negative samples are clamped to the mean.
    """
    waiting_seconds_mean: float
    waiting_seconds_stdev: Optional[float] = None   # defaults to mean / 4
    num_pipelines_per_batch: int = 4

    def __post_init__(self):
        if self.waiting_seconds_stdev is None:
            self.waiting_seconds_stdev = self.waiting_seconds_mean / 4


# ---------------------------------------------------------------------------
# PipelineSpec — distribution over one category of pipelines
# ---------------------------------------------------------------------------

@dataclass
class PipelineSpec:
    """
    Describes the distribution for one class of pipelines.

    Multiple PipelineSpecs make up the full workload mix; each arrival batch
    samples a spec proportionally to its weight.

    Parameters
    ----------
    priority : str
        "QUERY", "INTERACTIVE", or "BATCH_PIPELINE"
    weight : float
        Relative probability of this spec being chosen for any given pipeline.
    num_ops_dist : Callable[[rng], int]
        Returns the number of operators for a new pipeline.
    dag_shape_dist : Callable[[rng], str]
        Returns "linear", "branch_in", or "branch_out".
    op_profile_selector : Callable[[OperatorContext, rng], SegmentProfile]
        Returns the resource profile for a given operator.
    label_specs : list[LabelSpec]
        Label columns to emit for every operator in this pipeline type.
    """
    priority: str
    weight: float
    num_ops_dist: Callable[[np.random.Generator], int]
    dag_shape_dist: Callable[[np.random.Generator], str]
    op_profile_selector: Callable[[OperatorContext, np.random.Generator], SegmentProfile]
    label_specs: list[LabelSpec] = field(default_factory=list)

    # ------------------------------------------------------------------
    # Convenience constructors
    # ------------------------------------------------------------------

    @classmethod
    def query(
        cls,
        weight: float = 0.1,
        label_specs: Optional[list[LabelSpec]] = None,
        profiles: Optional[dict[str, SegmentProfile]] = None,
    ) -> PipelineSpec:
        """Single-operator query pipeline using the 'query' profile."""
        p = {**DEFAULT_PROFILES, **(profiles or {})}
        return cls(
            priority="QUERY",
            weight=weight,
            num_ops_dist=lambda rng: 1,
            dag_shape_dist=lambda rng: "linear",
            op_profile_selector=lambda ctx, rng: p["query"],
            label_specs=label_specs or [],
        )

    @classmethod
    def interactive(
        cls,
        num_ops_mean: float = 5,
        num_ops_min: int = 1,
        num_ops_max: Optional[int] = None,
        weight: float = 0.3,
        dag_shapes: Optional[list[str]] = None,
        dag_shape_weights: Optional[list[float]] = None,
        cpu_io_ratio: float = 0.5,
        label_specs: Optional[list[LabelSpec]] = None,
        profiles: Optional[dict[str, SegmentProfile]] = None,
    ) -> PipelineSpec:
        """Interactive pipeline with a configurable operator count and DAG shape."""
        return cls._make_non_query(
            priority="INTERACTIVE",
            weight=weight,
            num_ops_mean=num_ops_mean,
            num_ops_min=num_ops_min,
            num_ops_max=num_ops_max,
            dag_shapes=dag_shapes,
            dag_shape_weights=dag_shape_weights,
            cpu_io_ratio=cpu_io_ratio,
            label_specs=label_specs or [],
            profiles=profiles,
        )

    @classmethod
    def batch(
        cls,
        num_ops_mean: float = 5,
        num_ops_min: int = 1,
        num_ops_max: Optional[int] = None,
        weight: float = 0.6,
        dag_shapes: Optional[list[str]] = None,
        dag_shape_weights: Optional[list[float]] = None,
        cpu_io_ratio: float = 0.5,
        label_specs: Optional[list[LabelSpec]] = None,
        profiles: Optional[dict[str, SegmentProfile]] = None,
    ) -> PipelineSpec:
        """Batch pipeline with a configurable operator count and DAG shape."""
        return cls._make_non_query(
            priority="BATCH_PIPELINE",
            weight=weight,
            num_ops_mean=num_ops_mean,
            num_ops_min=num_ops_min,
            num_ops_max=num_ops_max,
            dag_shapes=dag_shapes,
            dag_shape_weights=dag_shape_weights,
            cpu_io_ratio=cpu_io_ratio,
            label_specs=label_specs or [],
            profiles=profiles,
        )

    @classmethod
    def _make_non_query(
        cls,
        priority: str,
        weight: float,
        num_ops_mean: float,
        num_ops_min: int,
        num_ops_max: Optional[int],
        dag_shapes: Optional[list[str]],
        dag_shape_weights: Optional[list[float]],
        cpu_io_ratio: float,
        label_specs: list[LabelSpec],
        profiles: Optional[dict[str, SegmentProfile]],
    ) -> PipelineSpec:
        p = {**DEFAULT_PROFILES, **(profiles or {})}
        shapes = dag_shapes or ["linear"]
        shape_w = np.array(dag_shape_weights or ([1.0] * len(shapes)), dtype=float)
        shape_w = shape_w / shape_w.sum()
        stdev = max(1.0, num_ops_mean / 4)

        def _num_ops(rng: np.random.Generator) -> int:
            n = int(rng.normal(num_ops_mean, stdev))
            n = max(num_ops_min, n)
            if num_ops_max is not None:
                n = min(num_ops_max, n)
            return n

        def _dag_shape(rng: np.random.Generator) -> str:
            idx = rng.choice(len(shapes), p=shape_w)
            return shapes[idx]

        def _profile(ctx: OperatorContext, rng: np.random.Generator) -> SegmentProfile:
            # First operator is always IO-heavy (matches Eudoxia's generate_segment_from_val(-2))
            if ctx.op_index == 0:
                return p["io_heavy"]
            # Subsequent operators: sample from the profile sequence using cpu_io_ratio
            val = rng.normal(cpu_io_ratio)
            val = max(-2.0, val)
            idx = _val_to_profile_idx(val)
            return p[_PROFILE_SEQUENCE[min(idx, len(_PROFILE_SEQUENCE) - 1)]]

        return cls(
            priority=priority,
            weight=weight,
            num_ops_dist=_num_ops,
            dag_shape_dist=_dag_shape,
            op_profile_selector=_profile,
            label_specs=label_specs,
        )


def _val_to_profile_idx(val: float) -> int:
    """Map a cpu_io_ratio-centered normal sample to a profile index."""
    if val < -1:    return 0
    elif val < -0.5: return 1
    elif val < 0:   return 2
    elif val < 0.5: return 3
    elif val < 1:   return 4
    elif val < 1.5: return 5
    else:           return 6


# ---------------------------------------------------------------------------
# TraceConfig — top-level configuration
# ---------------------------------------------------------------------------

@dataclass
class TraceConfig:
    """
    Complete configuration for a trace generation run.

    Parameters
    ----------
    duration_seconds : float
        Length of the simulated trace in seconds.
    ticks_per_second : int
        Tick resolution (1000 = 1 ms per tick, matching Eudoxia's default).
    pipeline_specs : list[PipelineSpec]
        One or more pipeline distributions. Pipelines are sampled proportionally
        to each spec's weight.
    arrival_pattern : ArrivalPattern
        Controls inter-arrival timing and batch size.
    random_seed : int
        Seed for all random number generation.
    """
    duration_seconds: float
    ticks_per_second: int
    pipeline_specs: list[PipelineSpec]
    arrival_pattern: ArrivalPattern
    random_seed: int = 42
    bursts: list[BurstSpec] = field(default_factory=list)


# ---------------------------------------------------------------------------
# TraceRow — one row in the output CSV
# ---------------------------------------------------------------------------

@dataclass
class TraceRow:
    pipeline_id: str
    arrival_seconds: Optional[float]   # set only for first operator of each pipeline
    priority: str                       # set only for first operator of each pipeline
    operator_id: str
    parents: str                        # semicolon-separated parent operator_ids
    baseline_cpu_seconds: float
    cpu_scaling: str
    memory_gb: Optional[float]
    storage_read_gb: float
    labels: dict                        # extra label columns


# ---------------------------------------------------------------------------
# TraceGenerator
# ---------------------------------------------------------------------------

class TraceGenerator:
    """
    Generates Eudoxia-compatible CSV workload traces from a TraceConfig.

    Usage:
        gen = TraceGenerator(config)
        rows = gen.generate()
        gen.write_csv("trace.csv", rows)

    Or in one call:
        gen.write_csv("trace.csv")
    """

    def __init__(self, config: TraceConfig):
        self.config = config

    def generate(self) -> list[TraceRow]:
        """Run the full arrival simulation and return all TraceRow objects."""
        cfg = self.config
        rng = np.random.default_rng(cfg.random_seed)
        ap = cfg.arrival_pattern

        tick_length = 1.0 / cfg.ticks_per_second
        max_ticks = int(cfg.duration_seconds * cfg.ticks_per_second)

        # Normalise pipeline spec weights
        weights = np.array([s.weight for s in cfg.pipeline_specs], dtype=float)
        weights = weights / weights.sum()

        pipeline_counter = 0
        rows: list[TraceRow] = []

        # Pre-sort bursts by time so we can consume them in order
        pending_bursts = sorted(cfg.bursts, key=lambda b: b.time_seconds)
        burst_idx = 0

        # Arrival tick tracking — mirrors Eudoxia's WorkloadGenerator
        ticks_since_last = 0
        waiting_ticks = 0   # first batch arrives at tick 0

        for tick in range(max_ticks):
            current_seconds = tick * tick_length

            # Emit any bursts whose time has arrived, before regular arrivals
            while burst_idx < len(pending_bursts) and pending_bursts[burst_idx].time_seconds <= current_seconds:
                burst = pending_bursts[burst_idx]
                for _ in range(burst.num_pipelines):
                    pipeline_counter += 1
                    if burst.spec_index is not None:
                        spec = cfg.pipeline_specs[burst.spec_index]
                    else:
                        spec = cfg.pipeline_specs[rng.choice(len(cfg.pipeline_specs), p=weights)]
                    pipeline_rows = self._generate_pipeline(
                        f"p{pipeline_counter}", burst.time_seconds, spec, rng
                    )
                    rows.extend(pipeline_rows)
                burst_idx += 1

            if ticks_since_last < waiting_ticks:
                ticks_since_last += 1
                continue

            # Emit a regular batch of pipelines at this tick
            for _ in range(ap.num_pipelines_per_batch):
                pipeline_counter += 1
                spec_idx = rng.choice(len(cfg.pipeline_specs), p=weights)
                spec = cfg.pipeline_specs[spec_idx]
                pipeline_rows = self._generate_pipeline(
                    f"p{pipeline_counter}", current_seconds, spec, rng
                )
                rows.extend(pipeline_rows)

            # Schedule next batch
            mean_t = ap.waiting_seconds_mean * cfg.ticks_per_second
            std_t = ap.waiting_seconds_stdev * cfg.ticks_per_second
            next_wait = int(rng.normal(mean_t, std_t))
            if next_wait <= 0:
                next_wait = int(mean_t)
            waiting_ticks = next_wait
            ticks_since_last = 0

        return rows

    def _generate_pipeline(
        self,
        pipeline_id: str,
        arrival_seconds: float,
        spec: PipelineSpec,
        rng: np.random.Generator,
    ) -> list[TraceRow]:
        num_ops = spec.num_ops_dist(rng)
        dag_shape = spec.dag_shape_dist(rng)
        rows = []

        # Build parent relationships based on DAG shape
        # parent_map[op_index] = list of parent op_indices
        parent_map = _build_parent_map(num_ops, dag_shape)

        for op_idx in range(num_ops):
            op_id = f"op{op_idx + 1}"
            parent_ids = [f"op{p + 1}" for p in parent_map[op_idx]]
            parents_str = ";".join(parent_ids)

            is_root = len(parent_map[op_idx]) == 0
            is_leaf = not any(op_idx in parent_map[j] for j in range(num_ops))

            ctx = OperatorContext(
                op_index=op_idx,
                num_ops=num_ops,
                dag_shape=dag_shape,
                is_root=is_root,
                is_leaf=is_leaf,
                priority=spec.priority,
                profile=None,   # filled in below
                pipeline_id=pipeline_id,
            )

            profile = spec.op_profile_selector(ctx, rng)
            ctx.profile = profile

            # Evaluate label generators
            labels = {}
            for ls in spec.label_specs:
                val = ls.generator(ctx, rng)
                if val is not None:
                    labels[ls.column_name] = val

            rows.append(TraceRow(
                pipeline_id=pipeline_id,
                arrival_seconds=arrival_seconds if op_idx == 0 else None,
                priority=spec.priority if op_idx == 0 else "",
                operator_id=op_id,
                parents=parents_str,
                baseline_cpu_seconds=profile.baseline_cpu_seconds,
                cpu_scaling=profile.cpu_scaling,
                memory_gb=profile.memory_gb,
                storage_read_gb=profile.storage_read_gb,
                labels=labels,
            ))

        return rows

    def write_csv(self, file_path: str | Path, rows: Optional[list[TraceRow]] = None):
        """
        Write trace rows to a CSV file.

        Discovers all label columns from the rows and writes them in sorted order
        after the fixed columns, so the output is always a valid Eudoxia trace.
        """
        if rows is None:
            rows = self.generate()

        # Collect all label column names (preserve insertion order, sort for stability)
        label_columns = sorted({k for row in rows for k in row.labels})

        fixed_columns = [
            "pipeline_id", "arrival_seconds", "priority", "operator_id",
            "parents", "baseline_cpu_seconds", "cpu_scaling", "memory_gb",
            "storage_read_gb",
        ]
        all_columns = fixed_columns + label_columns

        with open(file_path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=all_columns)
            writer.writeheader()
            for row in rows:
                d = {
                    "pipeline_id": row.pipeline_id,
                    "arrival_seconds": "" if row.arrival_seconds is None else row.arrival_seconds,
                    "priority": row.priority,
                    "operator_id": row.operator_id,
                    "parents": row.parents,
                    "baseline_cpu_seconds": row.baseline_cpu_seconds,
                    "cpu_scaling": row.cpu_scaling,
                    "memory_gb": "" if row.memory_gb is None else row.memory_gb,
                    "storage_read_gb": row.storage_read_gb,
                }
                for col in label_columns:
                    d[col] = row.labels.get(col, "")
                writer.writerow(d)

        print(f"Wrote {len(rows)} rows ({_count_pipelines(rows)} pipelines) to {file_path}")


# ---------------------------------------------------------------------------
# DAG shape helpers
# ---------------------------------------------------------------------------

def _build_parent_map(num_ops: int, dag_shape: str) -> dict[int, list[int]]:
    """
    Return a dict mapping op_index → list of parent op_indices.

    linear:    0 → 1 → 2 → ... → n-1
    branch_in: 0,1,...,n-2 are all roots; n-1 depends on all of them
    branch_out: 0 is the root; 1,2,...,n-1 all depend only on 0
    """
    parent_map: dict[int, list[int]] = {i: [] for i in range(num_ops)}
    if num_ops == 1:
        return parent_map

    if dag_shape == "linear":
        for i in range(1, num_ops):
            parent_map[i] = [i - 1]

    elif dag_shape == "branch_in":
        # All early ops are roots; final op depends on all of them
        parent_map[num_ops - 1] = list(range(num_ops - 1))

    elif dag_shape == "branch_out":
        # Root is op 0; all others depend on op 0
        for i in range(1, num_ops):
            parent_map[i] = [0]

    else:
        raise ValueError(f"Unknown dag_shape: {dag_shape!r}. Use 'linear', 'branch_in', or 'branch_out'.")

    return parent_map


def _count_pipelines(rows: list[TraceRow]) -> int:
    return len({r.pipeline_id for r in rows})


# ---------------------------------------------------------------------------
# Demo / CLI
# ---------------------------------------------------------------------------

def _demo_config() -> TraceConfig:
    """
    A representative config close to the WoAIS one-shot canonical parameters.
    Includes op_type and file_size_mb labels, plus a noisy CPU estimate column
    (multiplicative log-normal noise, ~20% std) to illustrate oracle experiments.
    """
    noisy_cpu = noisy_label(
        "noisy_cpu_seconds",
        true_value_fn=lambda ctx, rng: ctx.profile.baseline_cpu_seconds,
        noise_fn=lambda v, rng: round(v * float(rng.lognormal(0.0, 0.2)), 3),
    )
    common_labels = [op_type_label(), file_size_mb_label(), noisy_cpu]

    return TraceConfig(
        duration_seconds=600,
        ticks_per_second=1000,
        pipeline_specs=[
            PipelineSpec.query(weight=0.1, label_specs=common_labels),
            PipelineSpec.interactive(
                num_ops_mean=5, weight=0.3,
                dag_shapes=["linear", "branch_in", "branch_out"],
                dag_shape_weights=[0.7, 0.15, 0.15],
                label_specs=common_labels,
            ),
            PipelineSpec.batch(
                num_ops_mean=5, weight=0.6,
                dag_shapes=["linear", "branch_in", "branch_out"],
                dag_shape_weights=[0.7, 0.15, 0.15],
                label_specs=common_labels,
            ),
        ],
        arrival_pattern=ArrivalPattern(
            waiting_seconds_mean=10.0,
            num_pipelines_per_batch=4,
        ),
        random_seed=42,
    )


if __name__ == "__main__":
    output = sys.argv[1] if len(sys.argv) > 1 else "demo_trace.csv"
    config = _demo_config()
    gen = TraceGenerator(config)
    gen.write_csv(output)
