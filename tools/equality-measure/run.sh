#!/usr/bin/env bash
# The whole equality measurement in one command (tools/equality-measure/README.md):
#   tools/equality-measure/run.sh <out dir> [<id regex>]
# Clones the pinned legend-pure and legend-engine sources (.pure files only) into <out dir>/upstream once, collects the
# bodies, runs them (bazel run //core:equality_measure), then writes <out dir>/summary.md.
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
repo="$(cd "$here/../.." && pwd)"
out="$(mkdir -p "$1" && cd "$1" && pwd)"
only="${2:-}"
pure_tag="legend-pure-$(sed -n 's/^LEGEND_PURE_RELEASE = "\(.*\)"/\1/p' "$repo/release.MODULE.bazel")"
engine_tag="legend-engine-$(sed -n 's/^LEGEND_ENGINE_RELEASE = "\(.*\)"/\1/p' "$repo/release.MODULE.bazel")"
up="$out/upstream"
if [ ! -d "$up/pure" ]; then
  git clone -q --depth 1 --branch "$pure_tag" https://github.com/finos/legend-pure "$up/pure"
fi
if [ ! -d "$up/engine" ]; then
  git clone -q --depth 1 --no-checkout --branch "$engine_tag" https://github.com/finos/legend-engine "$up/engine"
  git -C "$up/engine" sparse-checkout set --no-cone '*.pure'
  git -C "$up/engine" checkout -q
fi
python3 "$here/collect.py" "$repo" "$out" "$up/pure" "$up/engine"
python3 "$here/helpers.py" "$out" "$up/pure" "$up/engine" > /dev/null
(cd "$repo" && bazel run //core:equality_measure -- "$out/bodies.tsv" "$out" ${only:+"$only"})
python3 "$here/report.py" "$out"
