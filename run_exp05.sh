#!/bin/bash
# Full exp05 workflow: worst_rich, 20 schedulers per fidelity condition
#
# Usage: bash run_exp05.sh
#
# Steps:
#   1. analyze source  — run worst scheduler under 4 cheap sim conditions
#   2. generate        — generate 20 schedulers per condition using rich feedback
#   3. analyze eval    — evaluate generated schedulers under full sim
#   4. plot            — produce fig5.pdf

set -e

ROOT="$(cd "$(dirname "$0")" && pwd)"
SRC="$ROOT/src/spring2026/tool"

PYTHON="uv run python"

SOURCE="best"
CONTEXT="simple"
N=20
SIM_LABELS="dur1800_ticks100 dur900_ticks100 dur3600_ticks50 dur3600_ticks25"

echo "========================================"
echo "EXP05  source=${SOURCE}  context=${CONTEXT}  n=${N}"
echo "========================================"

echo ""
echo "==> [1/4] Analyze source under cheap sim conditions"
$PYTHON "$SRC/analyze.py" 05_two_shot_perf source --source "$SOURCE"

echo ""
echo "==> [2/4] Generate schedulers (${N} per condition)"
for label in $SIM_LABELS; do
    echo ""
    echo "--- $label ---"
    $PYTHON "$SRC/generate.py" \
        --exp two_shot_perf \
        --source "$SOURCE" \
        --context "$CONTEXT" \
        --sim_label "$label" \
        --n "$N"
done

echo ""
echo "==> [3/4] Evaluate generated schedulers (full sim)"
$PYTHON "$SRC/analyze.py" 05_two_shot_perf eval \
    --source "$SOURCE" \
    --context "$CONTEXT" \
    --workers 4

echo ""
echo "==> [4/4] Plot"
$PYTHON "$SRC/plot.py" 05_two_shot_perf \
    --source "$SOURCE" \
    --context "$CONTEXT"

echo ""
echo "========================================"
echo "DONE. Results in src/spring2026/results/05_two_shot_perf/${SOURCE}_${CONTEXT}/"
echo "Plot in src/spring2026/plots/05_two_shot_perf/fig5.pdf"
echo "========================================"
