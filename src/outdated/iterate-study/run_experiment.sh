#!/usr/bin/env bash
# Run a full iterate-study experiment: generate + evaluate.
#
# Usage:
#   ./run_experiment.sh --feedback minimal --subdir two-shot-val
#   ./run_experiment.sh --feedback rich --subdir two-shot-rich-val

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
VENV="$PROJECT_ROOT/.venv/bin/python"
ONE_SHOT_DIR="$SCRIPT_DIR/../one-shot"

# Defaults
FEEDBACK="minimal"
SUBDIR=""
N=20
EFFORT="high"
MODEL="gpt-5.2-2025-12-11"
SELECTION="best"

while [[ $# -gt 0 ]]; do
    case $1 in
        --feedback)   FEEDBACK="$2"; shift 2 ;;
        --subdir)     SUBDIR="$2"; shift 2 ;;
        --n)          N="$2"; shift 2 ;;
        --effort)     EFFORT="$2"; shift 2 ;;
        --model)      MODEL="$2"; shift 2 ;;
        --selection)  SELECTION="$2"; shift 2 ;;
        *) echo "Unknown arg: $1"; exit 1 ;;
    esac
done

# Default subdir if not set
if [[ -z "$SUBDIR" ]]; then
    SUBDIR="two-shot"
    [[ "$FEEDBACK" == "rich" ]] && SUBDIR="two-shot-rich"
    [[ "$SELECTION" == "avg" ]] && SUBDIR="two-shot-avg"
    [[ "$SELECTION" == "avg" && "$FEEDBACK" == "rich" ]] && SUBDIR="two-shot-avg-rich"
fi

SCHEDULER_DIR="$SCRIPT_DIR/$SUBDIR/schedulers"
OUTPUT_DIR="$SCRIPT_DIR/$SUBDIR/output"

echo "=== Iterate Study ==="
echo "  feedback:  $FEEDBACK"
echo "  subdir:    $SUBDIR"
echo "  n:         $N"
echo "  effort:    $EFFORT"
echo "  model:     $MODEL"
echo "  selection: $SELECTION"
echo "  output:    $SCHEDULER_DIR"
echo ""

# Step 1: Generate
echo ">>> Step 1: Generating schedulers..."
"$VENV" "$SCRIPT_DIR/generate.py" \
    --effort "$EFFORT" \
    --n "$N" \
    --feedback "$FEEDBACK" \
    --model "$MODEL" \
    --selection "$SELECTION" \
    --subdir "$SUBDIR"

# Step 2: Evaluate
echo ""
echo ">>> Step 2: Evaluating schedulers..."
cd "$ONE_SHOT_DIR"
"$VENV" analyze.py "$SCHEDULER_DIR" --output-dir "$OUTPUT_DIR"

echo ""
echo "=== Done ==="
echo "Results: $OUTPUT_DIR/analysis.jsonl"
