#!/usr/bin/env bash
# The round a session runs before a push to the pull request: CI's own lanes (the suites of gates/BUILD.bazel), so
# that CI finds nothing a local run would have (2026-11-27: a hand-kept list of targets missed three of them).
#   tools/ci/pre-push-round.sh              the lanes a change to the product's Java can move
#   tools/ci/pre-push-round.sh <lane> ...   those lanes (names as in gates/BUILD.bazel: core, checks, corpus_h2, ...)
# Not here: pct_postgres (a container that runs as root cannot start Postgres: its lane, or
# tools/postgres/as_unprivileged.sh), and the lanes a Java change does not move (warehouse, datacube, ui, sdlc, the
# product build), which CI holds. One Bazel at a time; about 1.5 to 2 hours cold. Fourteen targets fail only because
# the container runs as root (AGENTS.md, "Traps"): rerun them unprivileged before calling one a regression.
set -euo pipefail
cd "$(git rev-parse --show-toplevel)"
lanes=("$@")
if [ "${#lanes[@]}" -eq 0 ]; then
  lanes=(core checks corpus_duckdb corpus_h2 pct_duckdb pct_h2 pct_channel_b parser_equivalence stress)
fi
labels=()
for l in "${lanes[@]}"; do
  grep -q "name = \"$l\"" gates/BUILD.bazel || { echo "no lane named $l in gates/BUILD.bazel"; exit 2; }
  labels+=("//gates:$l")
done
log="${TMPDIR:-/tmp}/pre-push-round.$$.log"
status=0
bazel test "${labels[@]}" --lockfile_mode=error --keep_going --jobs=3 --local_test_jobs=2 --test_output=errors \
  > "$log" 2>&1 || status=$?
# (a target that does not load or build is no failed test: Bazel says ERROR and, under --keep_going, runs the rest --
# 2026-11-28: a new package without its guard targets kept the guards from loading, and a round that printed failed
# tests alone read as "only the known ones")
grep -aE '^//.*(FAILED|TIMEOUT|NO STATUS|FLAKY)|^Executed [0-9]+ out of|^ERROR:|other errors during the build' "$log" \
  | cut -c1-300 || true
echo "exit $status; the log: $log"
exit $status
