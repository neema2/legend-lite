#!/usr/bin/env bash
# Re-derive every number in ../README.md.
#
# Usage: run.sh <legend checkouts dir> <work dir>
#   <legend checkouts dir> holds legend-engine/ and legend-pure/ (the EMIT models, emit.md, the PCT sources)
#   <work dir>             receives every intermediate file; created if missing
#
# Needs bazel and Python 3 (set PYTHON if it isn't python3), and a gate 10 run first for the pass list:
#   bazel test //core:stress_suites
# (Windows: run from Git Bash with BAZEL_SH set to Git's bash.exe and MSYS_NO_PATHCONV=1.)
set -euo pipefail
PY=${PYTHON:-python3}

LEGEND=$(cd "$1" && pwd)
mkdir -p "$2"
WORK=$(cd "$2" && pwd)
HERE=$(cd "$(dirname "$0")" && pwd)
REPO=$(cd "$HERE/../../.." && pwd)
STRESS="$REPO/core/src/test/resources/stress"
PASS="$REPO/bazel-testlogs/core/stress_suites/test.outputs/stress-suites-pass.txt"
[ -f "$PASS" ] || { echo "no gate 10 pass list at $PASS; run: bazel test //core:stress_suites" >&2; exit 1; }
cp "$PASS" "$WORK/stress-suites-pass.txt"
cd "$REPO"

# 1. The corpus's protocol document: the linked projects first (dependencies before dependents,
#    StressCorpus.LINKED_PROJECTS), each in model/store/mapping order, then every stress file.
files=()
for p in core-types core-tenor core-fx core-ratings core-instrument core-calendar core-units core-account core-geo fee-core index-core; do
  for s in model store mapping; do
    [ -f "projects/$p/$s.pure" ] && files+=("$REPO/projects/$p/$s.pure")
  done
done
for f in "$STRESS"/*.pure; do files+=("$f"); done
bazel run //tools/engine-runner:testable -- "${files[@]}" --pmcd-json="$WORK/stress-pmcd.json"

# 2. Denominators: the EMIT descriptors, the PCT function names.
"$PY" "$HERE/emit_census.py" "$LEGEND/legend-engine" "$WORK/emit_models.json"
"$PY" "$HERE/pct_inventory.py" "$LEGEND" "$WORK/pct_functions.tsv"

# 3. The EMIT models as one protocol document. Compilation fails on names the models share; the
#    runner writes the document after parsing and before compiling, so the failure is expected.
"$PY" "$HERE/emit_concat.py" "$WORK/emit_models.json" "$LEGEND/legend-engine" "$WORK/emit_all.pure" persistence compile-failure
bazel run //tools/engine-runner:testable -- "$WORK/emit_all.pure" --pmcd-json="$WORK/emit_all.json" || true
[ -f "$WORK/emit_all.json" ] || { echo "EMIT parse failed; see the output above" >&2; exit 1; }

# 4. The measures.
cd "$HERE"
"$PY" expectations.py "$STRESS" "$WORK/expectations.tsv"
"$PY" deep_populated.py "$STRESS" "$WORK/stress-suites-pass.txt"
export PMCD_JSON="$WORK/stress-pmcd.json"
"$PY" analyze.py "$WORK/expectations.tsv" "$WORK/stress-suites-pass.txt" "$WORK/analysis.json"
"$PY" profile.py "$WORK/expectations.tsv" "$WORK/stress-suites-pass.txt" "$WORK/profile.json"
"$PY" coverage.py "$WORK/pct_functions.tsv" "$WORK/coverage.json"
"$PY" complexity.py "$WORK/analysis.json" "$WORK/profile.json" "$WORK/complexity.json"
"$PY" literal_sites.py
"$PY" unused_joins.py
"$PY" matrix.py "$LEGEND/legend-engine/docs/emit/emit.md" "$WORK/emit_models.json" "$WORK/profile.json" "$WORK/matrix.json"
PMCD_JSON="$WORK/emit_all.json" "$PY" emit_analyze.py "$WORK/emit_all.pure.ranges.json" "$WORK/emit_analysis.json"
