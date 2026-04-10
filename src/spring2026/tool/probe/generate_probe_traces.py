import csv
import argparse
from dataclasses import dataclass


@dataclass
class PipelineGroup:
    priority: str
    num_pipelines: int
    num_ops: int
    memory_gb: float
    baseline_cpu_seconds: float
    cpu_scaling: str
    arrival_seconds: float
    storage_read_gb: float = 1.0  # small default — minimises disk I/O time


@dataclass
class TracePreset:
    groups: list[PipelineGroup]
    description: str


PRESETS = {
    "basic": TracePreset(
        description="Small, low-memory pipelines any scheduler should complete trivially.",
        groups=[
            PipelineGroup(
                priority="BATCH_PIPELINE",
                num_pipelines=3,
                num_ops=3,
                memory_gb=5.0,
                baseline_cpu_seconds=1.0,
                cpu_scaling="const",
                arrival_seconds=0.0,
            ),
        ],
    ),
    "oom_retry": TracePreset(
        description="High-memory operators (50 GB) that OOM at typical assignment sizes but recover at 64 GB pool.",
        groups=[
            PipelineGroup(
                priority="BATCH_PIPELINE",
                num_pipelines=4,
                num_ops=5,
                memory_gb=50.0,
                baseline_cpu_seconds=1.0,
                cpu_scaling="const",
                arrival_seconds=0.0,
            ),
        ],
    ),
    "suspend": TracePreset(
        description="BATCH pipelines arrive first with high memory; QUERY pipelines arrive mid-simulation and cannot coexist.",
        groups=[
            PipelineGroup(
                priority="BATCH_PIPELINE",
                num_pipelines=3,
                num_ops=6,
                memory_gb=55.0,
                baseline_cpu_seconds=5.0,
                cpu_scaling="linear3",
                arrival_seconds=0.0,
            ),
            PipelineGroup(
                priority="QUERY",
                num_pipelines=2,
                num_ops=2,
                memory_gb=55.0,
                baseline_cpu_seconds=1.0,
                cpu_scaling="const",
                arrival_seconds=30.0,
            ),
        ],
    ),
    "priority_ordering": TracePreset(
        description="Equal mix of QUERY and BATCH pipelines arriving simultaneously; pool RAM forces sequential execution.",
        groups=[
            PipelineGroup(
                priority="QUERY",
                num_pipelines=3,
                num_ops=3,
                memory_gb=40.0,
                baseline_cpu_seconds=5.0,
                cpu_scaling="const",
                arrival_seconds=0.0,
            ),
            PipelineGroup(
                priority="BATCH_PIPELINE",
                num_pipelines=3,
                num_ops=3,
                memory_gb=40.0,
                baseline_cpu_seconds=5.0,
                cpu_scaling="const",
                arrival_seconds=0.0,
            ),
        ],
    ),
    "starvation": TracePreset(
        description="QUERY workload exceeds simulation duration; BATCH pipelines should still receive some scheduling time.",
        groups=[
            PipelineGroup(
                priority="QUERY",
                num_pipelines=10,
                num_ops=3,
                memory_gb=30.0,
                baseline_cpu_seconds=5.0,
                cpu_scaling="const",
                arrival_seconds=0.0,
            ),
            PipelineGroup(
                priority="BATCH_PIPELINE",
                num_pipelines=2,
                num_ops=1,
                memory_gb=30.0,
                baseline_cpu_seconds=5.0,
                cpu_scaling="const",
                arrival_seconds=0.0,
            ),
        ],
    ),
    "no_deadlock": TracePreset(
        description="Many pipelines competing for a tight pool; any scheduler making forward progress passes.",
        groups=[
            PipelineGroup(
                priority="BATCH_PIPELINE",
                num_pipelines=8,
                num_ops=4,
                memory_gb=10.0,
                baseline_cpu_seconds=3.0,
                cpu_scaling="const",
                arrival_seconds=0.0,
            ),
        ],
    ),
}


def generate_probe_trace(
    preset: TracePreset,
    trace_file: str,
    num_pipelines: int | None = None,
) -> None:
    """Write a probe trace CSV for the given preset.

    Args:
        preset: TracePreset defining pipeline groups and their properties.
        trace_file: Output path for the CSV file.
        num_pipelines: If provided, overrides the num_pipelines of the first group.
    """
    fields = [
        "pipeline_id", "arrival_seconds", "priority",
        "operator_id", "parents",
        "baseline_cpu_seconds", "cpu_scaling",
        "memory_gb", "storage_read_gb",
    ]

    groups = preset.groups
    if num_pipelines is not None:
        first = groups[0]
        groups = [
            PipelineGroup(
                priority=first.priority,
                num_pipelines=num_pipelines,
                num_ops=first.num_ops,
                memory_gb=first.memory_gb,
                baseline_cpu_seconds=first.baseline_cpu_seconds,
                cpu_scaling=first.cpu_scaling,
                arrival_seconds=first.arrival_seconds,
                storage_read_gb=first.storage_read_gb,
            ),
            *groups[1:],
        ]

    with open(trace_file, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(fields)

        pipeline_counter = 1
        for group in groups:
            for _ in range(group.num_pipelines):
                pid = f"p{pipeline_counter}"
                pipeline_counter += 1

                for op_idx in range(group.num_ops):
                    oid = f"op{op_idx + 1}"
                    parent = f"op{op_idx}" if op_idx > 0 else ""

                    if op_idx == 0:
                        arrival = group.arrival_seconds
                        priority = group.priority
                    else:
                        arrival = ""
                        priority = ""

                    writer.writerow([
                        pid,
                        arrival,
                        priority,
                        oid,
                        parent,
                        group.baseline_cpu_seconds,
                        group.cpu_scaling,
                        group.memory_gb,
                        group.storage_read_gb,
                    ])


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate deterministic probe trace CSV files.")
    parser.add_argument("preset", choices=PRESETS.keys(), help="Trace preset to generate.")
    parser.add_argument("--output", required=True, help="Output CSV file path.")
    parser.add_argument(
        "--num_pipelines", type=int, default=None,
        help="Override number of pipelines in the primary group.",
    )
    args = parser.parse_args()

    preset = PRESETS[args.preset]
    print(f"Generating '{args.preset}' trace: {preset.description}")
    generate_probe_trace(preset, args.output, args.num_pipelines)
    print(f"Written to {args.output}")


if __name__ == "__main__":
    main()
