#!/bin/zsh
# THE RENDER CENSUS (E-0, docs/EXECUTION_PLAN_BOUNDARY_2026_10_05.md §10): every SQL statement the JVM suites render,
# recorded by the probe (probe.py), so two trees can be compared byte for byte (compare.py).
#
#   docs/execution-plan-boundary-2026-10-05/render-census/run.sh <outdir>
#
# Run from the repository root. It applies the probe (probe.py, by signature: any stage of E), runs every lane, and
# removes the probe again; uncommitted work is measured as it stands.
# The browser lanes are not run: the probe uses JVM-only APIs the WebAssembly planner cannot compile
# (//pure-protocol:twins_test needs that planner, so it fails to build here, by design; --keep_going).
set -u
OUT=$1
HERE=${0:a:h}
P=core/src/main/java/com/legend/sql/dialect/RenderCensus.java
mkdir -p "$OUT"
python3 -I "$HERE/probe.py" apply || { echo "the probe does not apply to this tree"; exit 2; }
trap 'python3 -I "$HERE/probe.py" remove' EXIT
# a run id stamped into the probe: the renderer's jar changes (its interface does not), so every test and corpus judge
# action that renders runs again instead of answering from Bazel's cache
echo "// census run $(date +%s)" >> $P
LANES=(//gates:core //gates:stress //pct:pct_duckdb //pct:pct_h2 //pct:pct_postgres //pct:pct_channel_b)
for lane in $LANES; do
  name=${lane//[\/:]/_}
  echo "== $lane $(date +%H:%M:%S)"
  bazel test --keep_going --cache_test_results=no "$lane" > "$OUT/$name.log" 2>&1
  echo "exit=$? $(grep -E 'Executed' "$OUT/$name.log" | tail -1)"
  for t in $(bazel query "tests($lane)" 2>/dev/null); do
    d="bazel-testlogs/${${t#//}/://}/test.outputs"
    if [ -d "$d" ]; then
      mkdir -p "$OUT/$name/${${t#//}//[\/:]/_}"
      cp "$d"/census-*.tsv "$d"/texts-*.tsv "$OUT/$name/${${t#//}//[\/:]/_}/" 2>/dev/null || true
    fi
  done
done
# the relational corpus: its host and database passes are build actions; unsandboxed, the probe writes beside each
# pass's ledger (bazel-bin/spec/<pass>/census)
for lane in //spec:corpus_duckdb //spec:corpus_h2; do
  name=${lane//[\/:]/_}
  echo "== $lane $(date +%H:%M:%S)"
  find bazel-bin/spec -type d -name census -prune -exec rm -rf {} + 2>/dev/null
  bazel test --keep_going --cache_test_results=no --spawn_strategy=local "$lane" > "$OUT/$name.log" 2>&1
  echo "exit=$? $(grep -E 'Executed' "$OUT/$name.log" | tail -1)"
  for c in $(find bazel-bin/spec -type d -name census 2>/dev/null); do
    pass=${${c%/census}##*/}
    mkdir -p "$OUT/$name/$pass" && cp "$c"/*.tsv "$OUT/$name/$pass/" 2>/dev/null || true
  done
done
echo "== done $(date +%H:%M:%S)"
