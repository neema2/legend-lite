#!/usr/bin/env bash
# Re-derive every number in ../README.md.
#
# Usage: run.sh <work dir> [<engine pass list>]
#   <work dir>           receives every intermediate file; created if missing
#   <engine pass list>   legend-engine's passing tests: `bazel run //scripts/corpus:run -- --out <dir>` writes
#                        <dir>/engine-pass.txt. profile.py and matrix.py then count a second "tested" column, judged
#                        on the engine (docs/STRESS_CORPUS_PLAN_2026_10_19.md, Phase 0)
#
# legend-engine and legend-pure (the EMIT models, emit.md, the PCT sources) are read at the release
# release.MODULE.bazel pins, from the trees Bazel fetches (@legend_engine_src, @legend_pure_src): one engine version
# for every test, never a hand checkout (the plan's D3).
#
# Needs bazel and Python 3 (set PYTHON if it isn't python3), and a gate 10 run first for the pass list:
#   bazel test //core:stress_suites
# (Windows: run from Git Bash with BAZEL_SH set to Git's bash.exe and MSYS_NO_PATHCONV=1. The paths below are then
# spelled D:/..., because the engine runner is a Windows JVM and reads /d/... as D:\d\....)
set -euo pipefail
PY=${PYTHON:-python3}

# a directory's absolute path, in the form a native program on this host reads (Git Bash's pwd -W; pwd elsewhere)
abs() { (cd "$1" && { pwd -W 2>/dev/null || pwd; }); }
mkdir -p "$1"
WORK=$(abs "$1")
HERE=$(abs "$(dirname "$0")")
REPO=$(abs "$HERE/../../..")
STRESS="$REPO/core/src/test/resources/stress"
PASS="$REPO/bazel-testlogs/core/stress_suites/test.outputs/stress-suites-pass.txt"
[ -f "$PASS" ] || { echo "no gate 10 pass list at $PASS; run: bazel test //core:stress_suites" >&2; exit 1; }
# -f and u+w: Bazel's test outputs are read-only, and cp gives the copy the same mode, so a second run must replace it
cp -f "$PASS" "$WORK/stress-suites-pass.txt" && chmod u+w "$WORK/stress-suites-pass.txt"
ENGINE_PASS=()
if [ -n "${2:-}" ]; then
  cp -f "$2" "$WORK/engine-pass.txt" && chmod u+w "$WORK/engine-pass.txt"
  ENGINE_PASS=("$WORK/engine-pass.txt")
fi
cd "$REPO"

# the pinned trees' roots, where Bazel fetched them
tree() { bazel query --output=location "@$1//:README.md" 2>/dev/null | sed -n 's#/README.md:1:1: source file .*##p'; }
ENGINE=$(tree legend_engine_src)
PURE=$(tree legend_pure_src)
[ -d "$ENGINE/docs/emit" ] && [ -d "$PURE" ] || { echo "the pinned trees were not found: '$ENGINE', '$PURE'" >&2; exit 1; }

# 1. The corpus's protocol document: the linked projects first (dependencies before dependents,
#    core/stress.bzl's LINKED_PROJECTS, the list StressCorpus loads), each in model/store/mapping order, then every
#    stress file.
linked=$(sed -n '/^LINKED_PROJECTS = \[/,/^\]/p' core/stress.bzl | grep -o '"[a-z0-9-]*"' | tr -d '"')
[ -n "$linked" ] || { echo "no LINKED_PROJECTS list in core/stress.bzl" >&2; exit 1; }
files=()
for p in $linked; do
  for s in model store mapping; do
    [ -f "projects/$p/$s.pure" ] && files+=("$REPO/projects/$p/$s.pure")
  done
done
for f in "$STRESS"/*.pure; do files+=("$f"); done
bazel run //tools/engine-runner:testable -- "${files[@]}" --pmcd-json="$WORK/stress-pmcd.json"

# 2. Denominators: the EMIT descriptors, the PCT function names.
"$PY" "$HERE/emit_census.py" "$ENGINE" "$WORK/emit_models.json"
"$PY" "$HERE/pct_inventory.py" "$PURE" "$ENGINE" "$WORK/pct_functions.tsv"

# 3. The EMIT models as one protocol document. Compilation fails on names the models share; the
#    runner writes the document after parsing and before compiling, so the failure is expected.
"$PY" "$HERE/emit_concat.py" "$WORK/emit_models.json" "$ENGINE" "$WORK/emit_all.pure" persistence compile-failure
bazel run //tools/engine-runner:testable -- "$WORK/emit_all.pure" --pmcd-json="$WORK/emit_all.json" || true
[ -f "$WORK/emit_all.json" ] || { echo "EMIT parse failed; see the output above" >&2; exit 1; }

# 4. The measures.
cd "$HERE"
"$PY" expectations.py "$STRESS" "$WORK/expectations.tsv"
"$PY" deep_populated.py "$STRESS" "$WORK/stress-suites-pass.txt" ${ENGINE_PASS[@]+"${ENGINE_PASS[@]}"}
export PMCD_JSON="$WORK/stress-pmcd.json"
"$PY" analyze.py "$WORK/expectations.tsv" "$WORK/stress-suites-pass.txt" "$WORK/analysis.json"
"$PY" profile.py "$WORK/expectations.tsv" "$WORK/stress-suites-pass.txt" "$WORK/profile.json" ${ENGINE_PASS[@]+"${ENGINE_PASS[@]}"}
"$PY" coverage.py "$WORK/pct_functions.tsv" "$WORK/coverage.json"
"$PY" complexity.py "$WORK/analysis.json" "$WORK/profile.json" "$WORK/complexity.json"
"$PY" literal_sites.py
"$PY" filter_values.py "$WORK/expectations.tsv" "$WORK/stress-suites-pass.txt" ${ENGINE_PASS[@]+"${ENGINE_PASS[@]}"}
"$PY" milestoned_time.py "$WORK/expectations.tsv" "$WORK/stress-suites-pass.txt" ${ENGINE_PASS[@]+"${ENGINE_PASS[@]}"}
"$PY" unused_joins.py
"$PY" matrix.py "$ENGINE/docs/emit/emit.md" "$WORK/emit_models.json" "$WORK/profile.json" "$WORK/matrix.json"
PMCD_JSON="$WORK/emit_all.json" "$PY" emit_analyze.py "$WORK/emit_all.pure.ranges.json" "$WORK/emit_analysis.json"
