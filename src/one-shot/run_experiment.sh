#!/bin/bash
set -e

cd "$(dirname "$0")"
source ../../.venv/bin/activate
export PYTHONUNBUFFERED=1

EFFORTS="none low medium high"
N=50

echo "======================================================"
echo "PHASE 1: Generate schedulers (${N} per effort)"
echo "======================================================"
for effort in $EFFORTS; do
    echo ""
    echo ">>> experiment.py --effort $effort --n $N"
    python experiment.py --effort $effort --n $N
done

echo ""
echo "======================================================"
echo "PHASE 2: Analyze schedulers"
echo "======================================================"
for effort in $EFFORTS; do
    echo ""
    echo ">>> analyze.py schedulers-${effort}/"
    python analyze.py schedulers-${effort}/
done

echo ""
echo "======================================================"
echo "PHASE 3: Plot all results together"
echo "======================================================"
python plot.py output/

echo ""
echo "======================================================"
echo "ALL DONE. Plots in plots/"
echo "======================================================"
