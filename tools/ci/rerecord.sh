#!/usr/bin/env bash
# Re-run every target that re-records a checked-in file, so a push carries no stale recording
# (2026-11-27: three CI runs went red in one evening on recordings nobody had re-run -- the edge corpus goldens, the
# gs-quant ledgers, the parser corpus).
#   tools/ci/rerecord.sh --list            the targets, as Bazel knows them now
#   tools/ci/rerecord.sh [<regex> ...]     run those whose label matches a regex (default: all), `git status` after each
# The list is asked of Bazel each time (every `update_*` target that writes a source file or is a recording binary),
# so a new recording joins it by existing. One Bazel at a time: run it where no other Bazel runs. Read every line a
# run changes before keeping it (AGENTS.md: a regenerated file can carry a line that is not this change's).
set -euo pipefail
cd "$(git rev-parse --show-toplevel)"
targets=$(bazel query 'filter(":update_[^:]*$", //...)' --output=label_kind 2>/dev/null \
  | awk '$1 == "_write_source_file" || $1 == "java_binary" || $1 == "sh_binary" || $1 == "py_binary" { print $3 }' \
  | grep -Ev '_[0-9]+$' | sort)
if [ "${1:-}" = "--list" ]; then
  printf '%s\n' "$targets"
  exit 0
fi
failed=0
for t in $targets; do
  if [ "$#" -gt 0 ]; then
    keep=0
    for re in "$@"; do if printf '%s' "$t" | grep -Eq -- "$re"; then keep=1; fi; done
    [ "$keep" = 1 ] || continue
  fi
  echo "=== $t"
  if ! bazel run "$t" --jobs=3 > /dev/null 2> "/tmp/rerecord.$$.log"; then
    echo "FAILED: $t"; tail -5 "/tmp/rerecord.$$.log"; failed=1
  fi
  git status --short
done
rm -f "/tmp/rerecord.$$.log"
exit $failed
