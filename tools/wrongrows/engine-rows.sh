#!/bin/bash
# The whole stress corpus through legend-engine in rows mode (D23): every test's actual rows into <rows-dir>. The run
# is scripts/corpus/run.py's (Bazel workplan P3-25): one JVM per batch, a testable that hangs costing itself, the
# corpus's files and linked projects from core/stress.bzl's one list.
#   tools/wrongrows/engine-rows.sh <rows-dir> [--data=<damaged.pure>] [batch-size] [limit-seconds-per-testable]
# JOBS (default 2) JVMs run at once, each with a 6 GB heap. Build nothing while it runs: `bazel run` reads the
# runner's jars from bazel-bin, which a build replaces.
set -eu
cd "$(dirname "$0")/../.."
OUT="$1"; shift
DATA=(); if [[ "${1:-}" == --data=* ]]; then DATA=(--data "${1#--data=}"); shift; fi
exec bazel run //scripts/corpus:run -- --rows "$OUT" --out "$OUT/run" "${DATA[@]}" \
  --batch "${1:-40}" --limit "${2:-600}" --jobs "${JOBS:-2}"
